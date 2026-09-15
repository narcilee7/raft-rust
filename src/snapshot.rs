use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tracing::{error, info};

use crate::config::SNAPSHOT_VERSION_MAX;
use crate::configuration::Configuration;
use crate::future::{ConfigurationsFuture, ReqSnapshotFuture, UserSnapshotFutureState};
use crate::raft::{random_duration, wait_flag, RaftCore};
use crate::{RaftError, Result};

/// A reader over snapshot contents. Mirrors the `io.ReadCloser` returned by
/// the Go `SnapshotStore.Open`.
pub type SnapshotReader = Box<dyn std::io::Read + Send>;

/// Metadata of a snapshot. Mirrors `SnapshotMeta` in snapshot.go; the
/// deprecated `Peers` field is not ported. Serde-derived so it can be
/// persisted alongside the snapshot contents (Go writes it as meta.json).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Version of the snapshot metadata format. Only version 1 is supported.
    pub version: u8,
    /// Opaque ID, used for opening.
    pub id: String,
    /// Index and term of the last log entry covered by the snapshot.
    pub index: u64,
    pub term: u64,
    /// Cluster membership as of `configuration_index`.
    pub configuration: Configuration,
    /// Log index where `configuration` was originally written.
    pub configuration_index: u64,
    /// Size of the snapshot contents in bytes.
    pub size: u64,
}

/// Generates a snapshot ID from the term, index and current time in
/// milliseconds. Mirrors `snapshotName` in file_snapshot.go.
pub fn snapshot_name(term: u64, index: u64) -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{}-{}-{}", term, index, millis)
}

/// Flexible snapshot storage and retrieval. Mirrors the Go `SnapshotStore`
/// interface in snapshot.go. Unlike Go, `create` does not take a transport
/// (the legacy peers encoding it was used for is not ported).
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Begins a snapshot at the given index and term, with the given
    /// committed configuration. The version controls the snapshot format;
    /// only version 1 is supported.
    async fn create(
        &self,
        version: u8,
        index: u64,
        term: u64,
        configuration: &Configuration,
        configuration_index: u64,
    ) -> Result<Box<dyn SnapshotSink>>;

    /// Lists the available snapshots in descending order (highest index
    /// first).
    async fn list(&self) -> Result<Vec<SnapshotMeta>>;

    /// Opens the snapshot with the given ID for reading. Once the reader is
    /// dropped the snapshot is assumed to be no longer needed.
    async fn open(&self, id: &str) -> Result<(SnapshotMeta, SnapshotReader)>;
}

/// Returned by [`SnapshotStore::create`]. The FSM writes state to the sink
/// and calls `close` on completion; on error `cancel` is invoked. Mirrors the
/// Go `SnapshotSink` interface.
#[async_trait]
pub trait SnapshotSink: Send {
    /// Appends bytes to the snapshot contents, returning the number of bytes
    /// written.
    async fn write(&mut self, buf: &[u8]) -> Result<usize>;

    /// The ID of the snapshot being written.
    fn id(&self) -> String;

    /// Finishes the snapshot, making it durable and visible.
    async fn close(&mut self) -> Result<()>;

    /// Aborts the snapshot, discarding any written state.
    async fn cancel(&mut self) -> Result<()>;
}

/// Copies everything from the reader into the sink, returning the number of
/// bytes written. Mirrors the counting-reader `io.Copy` in the Go snapshot
/// paths.
pub(crate) async fn copy_to_sink(
    reader: &mut SnapshotReader,
    sink: &mut Box<dyn SnapshotSink>,
) -> Result<u64> {
    let mut buf = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let n = std::io::Read::read(reader, &mut buf)?;
        if n == 0 {
            break;
        }
        sink.write(&buf[..n]).await?;
        total += n as u64;
    }
    Ok(total)
}

/// Drains and discards the remaining snapshot data, mirroring the Go
/// `io.Copy(io.Discard, rpc.Reader)` defer in `installSnapshot` that ensures
/// the stream is always fully consumed.
pub(crate) fn drain_reader(reader: &mut SnapshotReader) {
    let _ = std::io::copy(reader, &mut std::io::sink());
}

/// Releases an FSMSnapshot on drop, mirroring the Go
/// `defer snapReq.snapshot.Release()`.
struct ReleaseOnDrop(Option<Box<dyn crate::fsm::FSMSnapshot>>);

impl ReleaseOnDrop {
    fn new(snapshot: Box<dyn crate::fsm::FSMSnapshot>) -> Self {
        ReleaseOnDrop(Some(snapshot))
    }
}

impl std::ops::Deref for ReleaseOnDrop {
    type Target = dyn crate::fsm::FSMSnapshot;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("snapshot already released").as_ref()
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(snapshot) = self.0.take() {
            snapshot.release();
        }
    }
}

/// Long-running task that manages taking new snapshots of the FSM, in
/// parallel to the FSM and main tasks so snapshots do not block normal
/// operation. Mirrors `Raft.runSnapshots` in snapshot.go.
pub(crate) async fn run_snapshots(
    core: Arc<RaftCore>,
    mut user_snapshot_rx: mpsc::Receiver<UserSnapshotFutureState>,
    shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        let interval = core.config().snapshot_interval;
        tokio::select! {
            _ = tokio::time::sleep(random_duration(interval)) => {
                // Check if we should snapshot.
                match should_snapshot(&core).await {
                    Ok(true) => {
                        if let Err(e) = take_snapshot(&core).await {
                            if !matches!(e, RaftError::NothingNewToSnapshot) {
                                error!("failed to take snapshot: {}", e);
                            }
                        }
                    }
                    Ok(false) => {}
                    Err(e) => error!("failed to check snapshot condition: {}", e),
                }
            }
            future = user_snapshot_rx.recv() => {
                // User-triggered, run immediately.
                let Some(mut future) = future else { return };
                match take_snapshot(&core).await {
                    Ok(id) => {
                        let snapshots = Arc::clone(&core.snapshots);
                        future.opener = Some(Box::new(move || {
                            Box::pin(async move { snapshots.open(&id).await })
                        }));
                        future.respond(Ok(()));
                    }
                    Err(e) => {
                        if !matches!(e, RaftError::NothingNewToSnapshot) {
                            error!("failed to take snapshot: {}", e);
                        }
                        future.respond(Err(e));
                    }
                }
            }
            _ = wait_flag(&shutdown_rx) => return,
        }
    }
}

/// Checks if we meet the conditions to take a new snapshot: enough new logs
/// since the last snapshot. Mirrors `shouldSnapshot`.
async fn should_snapshot(core: &Arc<RaftCore>) -> Result<bool> {
    // Check the last snapshot index.
    let last_snap = core.shared.last_snapshot_index();

    // Check the last log index.
    let last_idx = core.logs.last_index().await?;

    // Compare the delta to the threshold.
    let delta = last_idx.saturating_sub(last_snap);
    Ok(delta >= core.config().snapshot_threshold)
}

/// Takes a new snapshot, returning the ID of the new snapshot. Must only be
/// called from the snapshot task, never the main task. Mirrors
/// `takeSnapshot`.
pub(crate) async fn take_snapshot(core: &Arc<RaftCore>) -> Result<String> {
    // Create a request for the FSM to perform a snapshot.
    let (req, rx) = ReqSnapshotFuture::new();
    tokio::select! {
        r = core.fsm_snapshot_tx.send(req) => r.map_err(|_| RaftError::RaftShutdown)?,
        _ = core.shutdown_wait() => return Err(RaftError::RaftShutdown),
    }

    // Wait until we get a response.
    let outcome = match rx.await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(RaftError::NothingNewToSnapshot)) => return Err(RaftError::NothingNewToSnapshot),
        Ok(Err(e)) => return Err(RaftError::Other(format!("failed to start snapshot: {}", e))),
        Err(_) => return Err(RaftError::RaftShutdown),
    };
    let snapshot = outcome
        .snapshot
        .expect("the FSM task provides a snapshot on success");
    let snapshot_guard = ReleaseOnDrop::new(snapshot);

    // Make a request for the configurations and extract the committed info.
    // We have to use the future here to safely get this information since it
    // is owned by the main task.
    let (config_req, config_rx) = ConfigurationsFuture::new();
    tokio::select! {
        r = core.configurations_tx.send(config_req) => r.map_err(|_| RaftError::RaftShutdown)?,
        _ = core.shutdown_wait() => return Err(RaftError::RaftShutdown),
    }
    let configurations = config_rx.wait_full().await?;
    let committed = configurations.committed;
    let committed_index = configurations.committed_index;

    // We don't support snapshots while there's a config change outstanding
    // since the snapshot doesn't have a means to represent this state.
    if outcome.index < committed_index {
        return Err(RaftError::Other(format!(
            "cannot take snapshot now, wait until the configuration entry at {} has been applied (have applied {})",
            committed_index, outcome.index
        )));
    }

    // Create a new snapshot.
    info!(index = outcome.index, "starting snapshot up to index");
    let sink = core
        .snapshots
        .create(
            SNAPSHOT_VERSION_MAX,
            outcome.index,
            outcome.term,
            &committed,
            committed_index,
        )
        .await
        .map_err(|e| RaftError::Other(format!("failed to create snapshot: {}", e)))?;
    let sink_id = sink.id();

    // Try to persist the snapshot. On error the FSM cancels the sink, per
    // the FSMSnapshot::persist contract.
    if let Err(e) = snapshot_guard.persist(sink).await {
        return Err(RaftError::Other(format!(
            "failed to persist snapshot: {}",
            e
        )));
    }

    // Update the last stable snapshot info.
    core.shared.set_last_snapshot(outcome.index, outcome.term);

    // Compact the logs.
    compact_logs(core, outcome.index).await?;

    info!(index = outcome.index, "snapshot complete up to index");
    Ok(sink_id)
}

/// Takes the last inclusive index of a snapshot and trims the logs that are
/// no longer needed, honoring the trailing-logs configuration. Mirrors
/// `compactLogs`/`compactLogsWithTrailing`.
pub(crate) async fn compact_logs(core: &Arc<RaftCore>, snap_idx: u64) -> Result<()> {
    // Determine the log range to compact.
    let min_log = core
        .logs
        .first_index()
        .await
        .map_err(|e| RaftError::Other(format!("failed to get first log index: {}", e)))?;

    let last_log_idx = core.shared.last_log_index();
    let trailing_logs = core.config().trailing_logs;

    // Check if we have enough logs to truncate.
    if last_log_idx <= trailing_logs {
        return Ok(());
    }

    // Truncate up to the end of the snapshot, or `trailing_logs` back from
    // the head, whichever is further back.
    let max_log = snap_idx.min(last_log_idx - trailing_logs);
    if min_log > max_log {
        info!("no logs to truncate");
        return Ok(());
    }

    info!(from = min_log, to = max_log, "compacting logs");
    core.logs
        .delete_range(min_log, max_log)
        .await
        .map_err(|e| RaftError::Other(format!("log compaction failed: {}", e)))
}
