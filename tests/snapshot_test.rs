//! Integration tests for the snapshot pipeline: `Raft::snapshot`, the
//! periodic snapshot task, and restart-time restoration. Mirrors the
//! `TestRaft_SnapshotRestore` family in `raft_test.go`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{inmem_config, inmem_snapshots, make_cluster_with};
use raft::{
    ApplyResponse, FSMSnapshot, Log, RaftFuture, SnapshotMeta, SnapshotReader, SnapshotSink,
    SnapshotStore, FSM,
};

/// Mirrors `TestRaft_SnapshotRestore`: a single-node cluster takes a
/// snapshot, logs are trimmed, the node shuts down, and a fresh node
/// started on top of the same stores restores from the snapshot.
#[tokio::test]
async fn snapshot_restore_single_node() {
    let mut conf = inmem_config();
    conf.trailing_logs = 10;
    let mut c = make_cluster_with(1, true, Some(conf.clone()), inmem_snapshots()).await;

    // Commit a lot of things.
    let leader = c.leader().await.clone();
    let mut last = None;
    for i in 0..100 {
        let cmd = format!("test{}", i);
        last = Some(leader.apply(cmd.as_bytes(), Duration::ZERO).await);
    }
    last.expect("at least one apply").wait().await.unwrap();

    // Take a snapshot.
    leader
        .snapshot()
        .await
        .error()
        .await
        .expect("snapshot failed");

    // Exactly one snapshot should exist.
    let store = c.snaps[0].clone();
    let snaps = store.list().await.expect("list snapshots");
    assert_eq!(snaps.len(), 1, "expected one snapshot");
    let snap = snaps[0].clone();

    // Logs should be trimmed, keeping `trailing_logs` entries behind the
    // snapshot index.
    let logs: Arc<dyn raft::LogStore> = c.stores[0].clone();
    let first_idx = logs.first_index().await.unwrap();
    let trailing = conf.trailing_logs;
    let expected_first = snap.index - trailing + 1;
    assert_eq!(
        first_idx, expected_first,
        "expected first_index={}, got {}",
        expected_first, first_idx
    );

    // Restart the node and verify it restores from the snapshot.
    c.restart_node(0).await;

    // After restart the FSM should hold all the logs (from the snapshot).
    let restored_logs = c.fsms[0].logs();
    assert_eq!(
        restored_logs.len(),
        100,
        "FSM should hold all 100 logs after restore"
    );

    // The new node should know the applied index is the snapshot index.
    assert_eq!(c.rafts[0].applied_index(), snap.index);

    c.close().await;
}

/// Mirrors the snapshot part of `TestRaft_SnapshotRestore`: take a
/// snapshot and check the metadata is correct.
#[tokio::test]
async fn snapshot_basic() {
    let c = make_cluster_with(1, true, Some(inmem_config()), inmem_snapshots()).await;

    let leader = c.leader().await.clone();
    let mut last = None;
    for i in 0..5 {
        let cmd = format!("test{}", i);
        last = Some(leader.apply(cmd.as_bytes(), Duration::ZERO).await);
    }
    last.expect("at least one apply").wait().await.unwrap();

    leader
        .snapshot()
        .await
        .error()
        .await
        .expect("snapshot failed");

    let store = c.snaps[0].clone();
    let snaps = store.list().await.expect("list snapshots");
    assert_eq!(snaps.len(), 1);
    let snap = &snaps[0];

    // The snapshot covers the latest applied log entry.
    assert_eq!(snap.term, leader.current_term());
    assert_eq!(snap.index, leader.applied_index());

    // The snapshot data can be opened and has size > 0.
    let (meta, mut reader) = store.open(&snap.id).await.expect("open snapshot");
    assert_eq!(meta.id, snap.id);
    let mut data = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut data).unwrap();
    assert!(!data.is_empty(), "snapshot data should not be empty");

    c.close().await;
}

/// A follower rejects user-triggered restore, mirroring the gating in
/// `runLeader`/`runFollower`.
#[tokio::test]
async fn restore_rejected_on_follower() {
    let c = make_cluster_with(3, true, Some(inmem_config()), inmem_snapshots()).await;

    let followers = c.followers().await;
    let follower = followers[0];

    let meta = SnapshotMeta {
        version: 1,
        id: "fake".into(),
        index: 1,
        term: 1,
        configuration: Default::default(),
        configuration_index: 1,
        size: 0,
    };
    let reader: SnapshotReader = Box::new(std::io::Cursor::new(Vec::<u8>::new()));
    let err = follower
        .restore(meta, reader, Duration::ZERO)
        .await
        .unwrap_err();
    assert!(
        matches!(err, raft::RaftError::NotLeader),
        "follower restore should fail with NotLeader: {}",
        err
    );

    c.close().await;
}

/// Mirrors the FSM-only portion of `TestRaftFSM_SnapshotRestore`: take a
/// snapshot via the FSM directly, restore it, and confirm the FSM state
/// is what we expect.
#[tokio::test]
async fn fsm_snapshot_restore() {
    // Take a snapshot via the FSM and persist it through an in-memory store.
    let fsm = SnapshotFSM::new();
    let snap = fsm.snapshot().await.unwrap();
    let store = c_snap_store();
    let sink = store.create(1, 7, 3, &Default::default(), 1).await.unwrap();
    snap.persist(sink).await.unwrap();

    // Read it back and restore into a fresh FSM.
    let snaps = store.list().await.unwrap();
    assert_eq!(snaps.len(), 1);
    let (meta, reader) = store.open(&snaps[0].id).await.unwrap();
    assert_eq!(meta.index, 7);
    assert_eq!(meta.term, 3);

    let restored = SnapshotFSM::new();
    restored.restore(reader).await.unwrap();
    assert_eq!(
        restored.logs(),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
}

// --- Test helpers -------------------------------------------------------

/// FSM that simply returns three pre-baked log entries on snapshot.
struct SnapshotFSM {
    inner: parking_lot::Mutex<Vec<Vec<u8>>>,
}

impl SnapshotFSM {
    fn new() -> Self {
        SnapshotFSM {
            inner: parking_lot::Mutex::new(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]),
        }
    }

    fn logs(&self) -> Vec<Vec<u8>> {
        self.inner.lock().clone()
    }
}

#[async_trait::async_trait]
impl raft::FSM for SnapshotFSM {
    async fn apply(&self, _log: &Log) -> raft::Result<ApplyResponse> {
        Ok(Box::new(0u64))
    }

    async fn snapshot(&self) -> raft::Result<Box<dyn FSMSnapshot>> {
        let logs = self.inner.lock().clone();
        Ok(Box::new(SnapshotFsmSnap { logs }))
    }

    async fn restore(&self, mut reader: SnapshotReader) -> raft::Result<()> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut buf)?;
        let logs: Vec<Vec<u8>> = rmp_serde::from_slice(&buf)?;
        *self.inner.lock() = logs;
        Ok(())
    }
}

struct SnapshotFsmSnap {
    logs: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl FSMSnapshot for SnapshotFsmSnap {
    async fn persist(&self, mut sink: Box<dyn SnapshotSink>) -> raft::Result<()> {
        let buf = rmp_serde::to_vec(&self.logs)?;
        sink.write(&buf).await?;
        sink.close().await?;
        Ok(())
    }

    fn release(&self) {}
}

/// Helper to obtain a fresh in-memory snapshot store.
fn c_snap_store() -> std::sync::Arc<raft::InmemSnapshotStore> {
    std::sync::Arc::new(raft::InmemSnapshotStore::new())
}
