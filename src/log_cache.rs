//! `LogCache` wraps any [`LogStore`] implementation with an in-memory ring
//! buffer that caches recently written entries. The wrap is transparent:
//! every method of [`LogStore`] is forwarded to the underlying store, but
//! `GetLog` and `StoreLogs` short-circuit on the cache when possible.
//!
//! Mirrors `log_cache.go` of the Go implementation.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::log::{Log, LogStore};
use crate::{RaftError, Result};

/// A [`LogStore`] wrapper that caches the most recent log entries in a
/// fixed-size ring buffer. Mirrors the Go `LogCache`.
pub struct LogCache {
    store: Arc<dyn LogStore>,
    /// Ring buffer indexed by `index % cache.len()`. Slots are empty
    /// until they are populated by [`LogCache::store_logs`].
    cache: RwLock<Vec<Option<Log>>>,
}

impl LogCache {
    /// Wraps `store` with a ring buffer of `capacity` entries. Returns an
    /// error if `capacity` is zero, mirroring `NewLogCache`.
    pub fn new(capacity: usize, store: Arc<dyn LogStore>) -> Result<Self> {
        if capacity == 0 {
            return Err(RaftError::Other("capacity must be positive".into()));
        }
        Ok(LogCache {
            store,
            cache: RwLock::new(vec![None; capacity]),
        })
    }

    /// Returns the configured ring-buffer capacity.
    pub fn capacity(&self) -> usize {
        self.cache.read().len()
    }
}

#[async_trait]
impl LogStore for LogCache {
    async fn first_index(&self) -> Result<u64> {
        self.store.first_index().await
    }

    async fn last_index(&self) -> Result<u64> {
        self.store.last_index().await
    }

    async fn get_log(&self, index: u64) -> Result<Log> {
        // Snapshot the slot under the lock, then drop the guard before
        // awaiting the underlying store.
        let cached = {
            let cache = self.cache.read();
            cache
                .get((index as usize) % cache.len())
                .and_then(|slot| slot.as_ref())
                .filter(|cached| cached.index == index)
                .cloned()
        };
        match cached {
            Some(log) => Ok(log),
            None => self.store.get_log(index).await,
        }
    }

    async fn store_log(&self, log: &Log) -> Result<()> {
        self.store_logs(std::slice::from_ref(log)).await
    }

    async fn store_logs(&self, logs: &[Log]) -> Result<()> {
        // Persist first; only populate the cache on success so a failed
        // store does not poison the cache.
        self.store.store_logs(logs).await?;
        {
            let mut cache = self.cache.write();
            for log in logs {
                let idx = (log.index as usize) % cache.len();
                cache[idx] = Some(log.clone());
            }
        }
        Ok(())
    }

    async fn delete_range(&self, min: u64, max: u64) -> Result<()> {
        // Invalidate the ring buffer first, then drop the guard before
        // awaiting the underlying store.
        let capacity = self.capacity();
        {
            let mut cache = self.cache.write();
            cache.clear();
            cache.resize(capacity, None);
        }
        self.store.delete_range(min, max).await
    }
}

// `CommitTrackingLogStore` is intentionally not implemented here: the
// underlying `Arc<dyn LogStore>` cannot be downcast without a concrete
// type. Wrap a `CommitTrackingLogStore` with `LogCache` only if you also
// need the cache; the in-memory `InmemCommitTrackingStore` is the
// canonical example and does not go through this wrapper.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inmem_store::InmemStore;
    use crate::log::LogType;

    fn log_at(index: u64) -> Log {
        Log {
            index,
            term: 1,
            log_type: LogType::Command,
            data: format!("log-{}", index).into_bytes(),
            ..Default::default()
        }
    }

    /// Mirrors `TestLogCache`: a small cache wraps an in-memory store and
    /// returns logs on hit, falling through to the store on miss.
    #[tokio::test]
    async fn log_cache_basic() {
        let store = Arc::new(InmemStore::new());
        let cache = LogCache::new(16, store.clone()).expect("cache");
        assert_eq!(cache.capacity(), 16);

        // Insert 32 logs into the store directly. Only the last 16 fit
        // in the cache.
        for i in 1..=32 {
            store.store_log(&log_at(i)).await.unwrap();
        }
        assert_eq!(cache.first_index().await.unwrap(), 1);
        assert_eq!(cache.last_index().await.unwrap(), 32);

        // Index 1 is no longer in the cache (it fell out of the ring),
        // but the underlying store still has it.
        assert_eq!(cache.get_log(1).await.unwrap().index, 1);

        // Indices 17..=32 should be cache hits.
        for i in 17..=32 {
            let log = cache.get_log(i).await.unwrap();
            assert_eq!(log.index, i);
            assert_eq!(log.data, format!("log-{}", i).into_bytes());
        }
    }

    /// `StoreLog` / `StoreLogs` propagate to the cache.
    #[tokio::test]
    async fn log_cache_store_propagates() {
        let store = Arc::new(InmemStore::new());
        let cache = LogCache::new(4, store.clone()).expect("cache");

        cache.store_log(&log_at(1)).await.unwrap();
        cache.store_logs(&[log_at(2), log_at(3)]).await.unwrap();

        for i in 1..=3 {
            assert_eq!(cache.get_log(i).await.unwrap().index, i);
        }
        assert_eq!(cache.last_index().await.unwrap(), 3);
    }

    /// `DeleteRange` invalidates the cache so reads go back to the store.
    #[tokio::test]
    async fn log_cache_delete_range_invalidates() {
        let store = Arc::new(InmemStore::new());
        let cache = LogCache::new(8, store.clone()).expect("cache");

        cache
            .store_logs(&(1..=10).map(log_at).collect::<Vec<_>>())
            .await
            .unwrap();
        assert_eq!(cache.get_log(5).await.unwrap().index, 5);

        cache.delete_range(3, 7).await.unwrap();
        // The cache is cleared, but the underlying store still has 8..=10.
        assert!(cache.get_log(5).await.is_err());
        assert_eq!(cache.get_log(10).await.unwrap().index, 10);
    }

    /// Constructing with capacity 0 is rejected.
    #[tokio::test]
    async fn log_cache_zero_capacity_rejected() {
        let store = Arc::new(InmemStore::new());
        let err = LogCache::new(0, store)
            .err()
            .expect("expected error for zero capacity");
        match err {
            RaftError::Other(msg) => assert!(msg.contains("positive")),
            _ => panic!("expected Other error"),
        }
    }
}
