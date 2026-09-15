use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::config::SNAPSHOT_VERSION_MAX;
use crate::configuration::Configuration;
use crate::snapshot::{snapshot_name, SnapshotMeta, SnapshotReader, SnapshotSink, SnapshotStore};
use crate::{RaftError, Result};

#[derive(Default)]
struct InmemSnapshotData {
    meta: SnapshotMeta,
    contents: Vec<u8>,
}

/// Implements the [`SnapshotStore`] interface in memory, retaining only the
/// most recent snapshot. Mirrors the Go `InmemSnapshotStore`; only suitable
/// for testing.
#[derive(Default)]
pub struct InmemSnapshotStore {
    latest: Arc<RwLock<Option<Arc<RwLock<InmemSnapshotData>>>>>,
}

impl InmemSnapshotStore {
    pub fn new() -> Self {
        InmemSnapshotStore::default()
    }
}

#[async_trait]
impl SnapshotStore for InmemSnapshotStore {
    async fn create(
        &self,
        version: u8,
        index: u64,
        term: u64,
        configuration: &Configuration,
        configuration_index: u64,
    ) -> Result<Box<dyn SnapshotSink>> {
        if version != SNAPSHOT_VERSION_MAX {
            return Err(RaftError::Snapshot(format!(
                "unsupported snapshot version {}",
                version
            )));
        }

        let data = Arc::new(RwLock::new(InmemSnapshotData {
            meta: SnapshotMeta {
                version,
                id: snapshot_name(term, index),
                index,
                term,
                configuration: configuration.clone(),
                configuration_index,
                size: 0,
            },
            contents: Vec::new(),
        }));
        *self.latest.write() = Some(Arc::clone(&data));
        Ok(Box::new(InmemSnapshotSink { data }))
    }

    async fn list(&self) -> Result<Vec<SnapshotMeta>> {
        let latest = self.latest.read();
        match latest.as_ref() {
            Some(data) => Ok(vec![data.read().meta.clone()]),
            None => Ok(Vec::new()),
        }
    }

    async fn open(&self, id: &str) -> Result<(SnapshotMeta, SnapshotReader)> {
        let latest = self.latest.read();
        let data = latest
            .as_ref()
            .filter(|data| data.read().meta.id == id)
            .ok_or_else(|| RaftError::Snapshot(format!("failed to open snapshot id: {}", id)))?;
        let data = data.read();
        // Copy the contents, since the stored buffer must remain intact for
        // further opens.
        Ok((
            data.meta.clone(),
            Box::new(Cursor::new(data.contents.clone())),
        ))
    }
}

/// Implements [`SnapshotSink`] in memory. Mirrors the Go
/// `InmemSnapshotSink`.
struct InmemSnapshotSink {
    data: Arc<RwLock<InmemSnapshotData>>,
}

#[async_trait]
impl SnapshotSink for InmemSnapshotSink {
    async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut data = self.data.write();
        data.contents.extend_from_slice(buf);
        data.meta.size += buf.len() as u64;
        Ok(buf.len())
    }

    fn id(&self) -> String {
        self.data.read().meta.id.clone()
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn cancel(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Successfully snapshots while always discarding the result, for when the
/// log should be truncated but no snapshot retained. Mirrors the Go
/// `DiscardSnapshotStore`. Never for production use; only suitable for
/// testing.
pub struct DiscardSnapshotStore;

impl DiscardSnapshotStore {
    pub fn new() -> Self {
        DiscardSnapshotStore
    }
}

impl Default for DiscardSnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SnapshotStore for DiscardSnapshotStore {
    async fn create(
        &self,
        _version: u8,
        _index: u64,
        _term: u64,
        _configuration: &Configuration,
        _configuration_index: u64,
    ) -> Result<Box<dyn SnapshotSink>> {
        Ok(Box::new(DiscardSnapshotSink))
    }

    async fn list(&self) -> Result<Vec<SnapshotMeta>> {
        Ok(Vec::new())
    }

    async fn open(&self, _id: &str) -> Result<(SnapshotMeta, SnapshotReader)> {
        Err(RaftError::Snapshot("open is not supported".into()))
    }
}

/// Implements [`SnapshotSink`] while always discarding. Mirrors the Go
/// `DiscardSnapshotSink`.
struct DiscardSnapshotSink;

#[async_trait]
impl SnapshotSink for DiscardSnapshotSink {
    async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        Ok(buf.len())
    }

    fn id(&self) -> String {
        "discard".into()
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn cancel(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn test_configuration() -> Configuration {
        Configuration {
            servers: vec![crate::configuration::Server {
                suffrage: crate::configuration::ServerSuffrage::Voter,
                id: "a".into(),
                address: "addr-a".into(),
            }],
        }
    }

    // Mirrors TestInmemSS_CreateSnapshot: write through the sink, then list
    // and open the snapshot.
    #[tokio::test]
    async fn create_write_list_open() {
        let store = InmemSnapshotStore::new();
        let mut sink = store
            .create(SNAPSHOT_VERSION_MAX, 10, 3, &test_configuration(), 2)
            .await
            .unwrap();

        let id = sink.id();
        sink.write(b"first ").await.unwrap();
        sink.write(b"second").await.unwrap();
        sink.close().await.unwrap();

        let snaps = store.list().await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].id, id);
        assert_eq!(snaps[0].index, 10);
        assert_eq!(snaps[0].term, 3);
        assert_eq!(snaps[0].configuration_index, 2);
        assert_eq!(snaps[0].size, 12);

        let (meta, mut reader) = store.open(&id).await.unwrap();
        assert_eq!(meta.index, 10);
        let mut contents = String::new();
        reader.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "first second");
    }

    // Mirrors TestInmemSS_OpenSnapshotTwice: contents survive repeated opens.
    #[tokio::test]
    async fn open_snapshot_twice() {
        let store = InmemSnapshotStore::new();
        let mut sink = store
            .create(SNAPSHOT_VERSION_MAX, 10, 3, &test_configuration(), 2)
            .await
            .unwrap();
        sink.write(b"data").await.unwrap();
        sink.close().await.unwrap();
        let id = sink.id();

        for _ in 0..2 {
            let (_, mut reader) = store.open(&id).await.unwrap();
            let mut contents = String::new();
            reader.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, "data");
        }
    }

    #[tokio::test]
    async fn create_rejects_unsupported_version() {
        let store = InmemSnapshotStore::new();
        let result = store.create(0, 10, 3, &test_configuration(), 2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_replaces_latest() {
        let store = InmemSnapshotStore::new();
        let mut first = store
            .create(SNAPSHOT_VERSION_MAX, 10, 3, &test_configuration(), 2)
            .await
            .unwrap();
        first.write(b"old").await.unwrap();
        first.close().await.unwrap();

        let mut second = store
            .create(SNAPSHOT_VERSION_MAX, 20, 4, &test_configuration(), 2)
            .await
            .unwrap();
        second.write(b"new").await.unwrap();
        second.close().await.unwrap();

        let snaps = store.list().await.unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].index, 20);
        assert!(store.open(&first.id()).await.is_err());
    }

    #[tokio::test]
    async fn discard_store() {
        let store = DiscardSnapshotStore::new();
        let mut sink = store
            .create(SNAPSHOT_VERSION_MAX, 10, 3, &test_configuration(), 2)
            .await
            .unwrap();
        assert_eq!(sink.id(), "discard");
        assert_eq!(sink.write(b"abc").await.unwrap(), 3);
        sink.close().await.unwrap();

        assert!(store.list().await.unwrap().is_empty());
        assert!(store.open("discard").await.is_err());
    }
}
