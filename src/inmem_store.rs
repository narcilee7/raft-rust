use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::log::{Log, LogStore};
use crate::stable::StableStore;
use crate::{RaftError, Result};

/// A log store that also tracks the commit index, for log stores that
/// support restoring committed logs. Mirrors the Go
/// `CommitTrackingLogStore` interface.
pub trait CommitTrackingLogStore: LogStore {
    /// Stages the commit index, to be made durable by the store.
    fn stage_commit_index(&self, index: u64) -> Result<()>;

    /// Returns the staged commit index, 0 if none was staged.
    fn get_commit_index(&self) -> Result<u64>;
}

#[derive(Default)]
struct InmemState {
    low_index: u64,
    high_index: u64,
    logs: HashMap<u64, Log>,
    kv: HashMap<Vec<u8>, Vec<u8>>,
    kv_int: HashMap<Vec<u8>, u64>,
}

/// Implements both the [`LogStore`] and [`StableStore`] interfaces in
/// memory. Mirrors the Go `InmemStore`. Do NOT use for production; only for
/// unit tests.
#[derive(Default)]
pub struct InmemStore {
    state: RwLock<InmemState>,
}

impl InmemStore {
    pub fn new() -> Self {
        InmemStore::default()
    }
}

#[async_trait]
impl LogStore for InmemStore {
    async fn first_index(&self) -> Result<u64> {
        Ok(self.state.read().low_index)
    }

    async fn last_index(&self) -> Result<u64> {
        Ok(self.state.read().high_index)
    }

    async fn get_log(&self, index: u64) -> Result<Log> {
        self.state
            .read()
            .logs
            .get(&index)
            .cloned()
            .ok_or(RaftError::LogNotFound)
    }

    async fn store_log(&self, log: &Log) -> Result<()> {
        self.store_logs(std::slice::from_ref(log)).await
    }

    async fn store_logs(&self, logs: &[Log]) -> Result<()> {
        let mut state = self.state.write();
        for log in logs {
            if state.low_index == 0 {
                state.low_index = log.index;
            }
            if log.index > state.high_index {
                state.high_index = log.index;
            }
            state.logs.insert(log.index, log.clone());
        }
        Ok(())
    }

    async fn delete_range(&self, min: u64, max: u64) -> Result<()> {
        let mut state = self.state.write();
        for index in min..=max {
            state.logs.remove(&index);
        }
        if min <= state.low_index {
            state.low_index = max.saturating_add(1);
        }
        if max >= state.high_index {
            state.high_index = min.saturating_sub(1);
        }
        if state.low_index > state.high_index {
            state.low_index = 0;
            state.high_index = 0;
        }
        Ok(())
    }
}

#[async_trait]
impl StableStore for InmemStore {
    async fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.state.write().kv.insert(key.to_vec(), val.to_vec());
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.state.read().kv.get(key).cloned())
    }

    async fn set_u64(&self, key: &[u8], val: u64) -> Result<()> {
        self.state.write().kv_int.insert(key.to_vec(), val);
        Ok(())
    }

    async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>> {
        Ok(self.state.read().kv_int.get(key).copied())
    }
}

/// An in-memory store that additionally tracks the commit index. Mirrors
/// the Go `InmemCommitTrackingStore`. Only for testing.
#[derive(Default)]
pub struct InmemCommitTrackingStore {
    store: InmemStore,
    commit_index: AtomicU64,
}

impl InmemCommitTrackingStore {
    pub fn new() -> Self {
        InmemCommitTrackingStore::default()
    }
}

#[async_trait]
impl LogStore for InmemCommitTrackingStore {
    async fn first_index(&self) -> Result<u64> {
        self.store.first_index().await
    }

    async fn last_index(&self) -> Result<u64> {
        self.store.last_index().await
    }

    async fn get_log(&self, index: u64) -> Result<Log> {
        self.store.get_log(index).await
    }

    async fn store_log(&self, log: &Log) -> Result<()> {
        self.store.store_log(log).await
    }

    async fn store_logs(&self, logs: &[Log]) -> Result<()> {
        self.store.store_logs(logs).await
    }

    async fn delete_range(&self, min: u64, max: u64) -> Result<()> {
        self.store.delete_range(min, max).await
    }
}

#[async_trait]
impl StableStore for InmemCommitTrackingStore {
    async fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.store.set(key, val).await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.get(key).await
    }

    async fn set_u64(&self, key: &[u8], val: u64) -> Result<()> {
        self.store.set_u64(key, val).await
    }

    async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>> {
        self.store.get_u64(key).await
    }
}

impl CommitTrackingLogStore for InmemCommitTrackingStore {
    fn stage_commit_index(&self, index: u64) -> Result<()> {
        self.commit_index.store(index, Ordering::Release);
        Ok(())
    }

    fn get_commit_index(&self) -> Result<u64> {
        Ok(self.commit_index.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn log_store_roundtrip() {
        let store = InmemStore::new();
        assert_eq!(store.first_index().await.unwrap(), 0);
        assert_eq!(store.last_index().await.unwrap(), 0);

        store.store_log(&log_at(1)).await.unwrap();
        store.store_logs(&[log_at(2), log_at(3)]).await.unwrap();

        assert_eq!(store.first_index().await.unwrap(), 1);
        assert_eq!(store.last_index().await.unwrap(), 3);

        let got = store.get_log(2).await.unwrap();
        assert_eq!(got.data, b"log-2");
        assert!(matches!(
            store.get_log(42).await.unwrap_err(),
            RaftError::LogNotFound
        ));
    }

    #[tokio::test]
    async fn delete_range_updates_bounds() {
        let store = InmemStore::new();
        store
            .store_logs(&(1..=10).map(log_at).collect::<Vec<_>>())
            .await
            .unwrap();

        // Delete from the middle: bounds unchanged.
        store.delete_range(3, 5).await.unwrap();
        assert_eq!(store.first_index().await.unwrap(), 1);
        assert_eq!(store.last_index().await.unwrap(), 10);
        assert!(store.get_log(4).await.is_err());

        // Delete a prefix: low bound moves up.
        store.delete_range(1, 2).await.unwrap();
        assert_eq!(store.first_index().await.unwrap(), 3);

        // Delete everything: bounds reset.
        store.delete_range(3, 10).await.unwrap();
        assert_eq!(store.first_index().await.unwrap(), 0);
        assert_eq!(store.last_index().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn stable_store_roundtrip() {
        let store = InmemStore::new();
        assert_eq!(store.get(b"missing").await.unwrap(), None);
        assert_eq!(store.get_u64(b"missing").await.unwrap(), None);

        store.set(b"key", b"value").await.unwrap();
        assert_eq!(
            store.get(b"key").await.unwrap().as_deref(),
            Some(&b"value"[..])
        );

        store.set_u64(b"num", 42).await.unwrap();
        assert_eq!(store.get_u64(b"num").await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn commit_tracking() {
        let store = InmemCommitTrackingStore::new();
        assert_eq!(store.get_commit_index().unwrap(), 0);
        store.stage_commit_index(17).unwrap();
        assert_eq!(store.get_commit_index().unwrap(), 17);

        // Delegates log storage to the inner store.
        store.store_log(&log_at(1)).await.unwrap();
        assert_eq!(store.last_index().await.unwrap(), 1);
    }
}
