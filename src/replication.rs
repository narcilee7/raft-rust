//! Log replication from the leader to a single follower: the replicate task,
//! the heartbeat task, and the pipelined fast path. Mirrors replication.go
//! of the Go implementation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::commitment::Commitment;
use crate::configuration::{encode_configuration, Server};
use crate::future::VerifyState;
use crate::log::Log;
use crate::raft::{backoff, capped_exponential_backoff, random_duration, RaftCore};
use crate::transport::{AppendEntriesRequest, AppendPipeline, InstallSnapshotRequest};
use crate::{RaftError, Result};

/// Maximum backoff scale factor, matching `maxFailureScale`.
const MAX_FAILURE_SCALE: u64 = 12;
/// Base backoff wait between failed RPCs, matching `failureWait`.
const FAILURE_WAIT: Duration = Duration::from_millis(10);

/// State for replicating to a single follower during a leader's term,
/// shared between the leader loop and the replication/heartbeat tasks.
/// Mirrors the Go `followerReplication` struct; the stop/trigger/notify
/// receivers are owned by the replicate task itself.
pub(crate) struct FollowerReplication {
    /// Network address and ID of the remote follower.
    pub(crate) peer: RwLock<Server>,
    /// Tracks entries acknowledged by followers so the leader's commit
    /// index can advance.
    pub(crate) commitment: Arc<Commitment>,
    /// The term of this leader, included in AppendEntries requests.
    pub(crate) current_term: u64,
    /// Index of the next log entry to send to the follower, which may fall
    /// past the end of the log.
    pub(crate) next_index: AtomicU64,
    /// Updated whenever any response is received from the follower
    /// (successful or not); used by the leader lease check.
    pub(crate) last_contact: Mutex<Instant>,
    /// Futures to resolve upon receipt of an acknowledgement.
    pub(crate) notify: Mutex<Vec<Arc<VerifyState>>>,
    /// Notified to send out a heartbeat, used to verify leadership.
    pub(crate) notify_tx: mpsc::Sender<()>,
    /// Signals the leader that we should step down based on information
    /// from this follower.
    pub(crate) step_down_tx: mpsc::Sender<()>,
    /// Notified every time new entries are appended to the log.
    pub(crate) trigger_tx: mpsc::Sender<()>,
    /// Number of failed RPCs since the last success, used for backoff.
    pub(crate) failures: AtomicU64,
}

impl FollowerReplication {
    /// Notifies all waiting verify futures whether this follower believes we
    /// are still the leader. Mirrors `notifyAll`.
    pub(crate) fn notify_all(&self, leader: bool) {
        let waiting: Vec<Arc<VerifyState>> = std::mem::take(&mut *self.notify.lock());
        for v in waiting {
            v.vote(leader);
        }
    }

    /// Removes a verify future from the notify set. Mirrors `cleanNotify`.
    pub(crate) fn clean_notify(&self, v: &Arc<VerifyState>) {
        self.notify.lock().retain(|x| !Arc::ptr_eq(x, v));
    }

    /// The time of last contact. Mirrors `LastContact`.
    pub(crate) fn last_contact(&self) -> Instant {
        *self.last_contact.lock()
    }

    /// Sets the last contact to now. Mirrors `setLastContact`.
    pub(crate) fn set_last_contact(&self) {
        *self.last_contact.lock() = Instant::now();
    }

    /// The current peer, cloned out of the lock. Mirrors the peerLock
    /// read pattern in replication.go.
    fn peer(&self) -> Server {
        self.peer.read().clone()
    }
}

/// The receivers a replicate task owns, mirroring the channel ends the Go
/// replicate goroutine selects on.
pub(crate) struct ReplicationChannels {
    pub(crate) stop_rx: mpsc::Receiver<u64>,
    pub(crate) trigger_rx: mpsc::Receiver<()>,
}

/// Long-running task replicating log entries to a single follower. Mirrors
/// `replicate`. Returns when the stop channel closes (leader stepped down
/// or the follower was removed), or on shutdown.
pub(crate) async fn replicate(
    core: Arc<RaftCore>,
    s: Arc<FollowerReplication>,
    stop_rx: mpsc::Receiver<u64>,
    trigger_rx: mpsc::Receiver<()>,
    notify_rx: mpsc::Receiver<()>,
) {
    let mut chans = ReplicationChannels {
        stop_rx,
        trigger_rx,
    };

    // Start an async heartbeating task.
    let (heartbeat_stop_tx, heartbeat_stop_rx) = watch::channel(false);
    core.go_func(heartbeat(
        Arc::clone(&core),
        Arc::clone(&s),
        notify_rx,
        heartbeat_stop_rx,
    ))
    .await;

    let mut allow_pipeline = false;
    'rpc: loop {
        // Standard RPC mode.
        let mut should_stop = false;
        while !should_stop {
            tokio::select! {
                max_index = chans.stop_rx.recv() => {
                    match max_index {
                        // Make a best effort to replicate up to this index.
                        Some(max_index) if max_index > 0 => {
                            replicate_to(&core, &s, max_index, &mut allow_pipeline).await;
                        }
                        // Channel closed: the leader stepped down.
                        _ => {}
                    }
                    break 'rpc;
                }
                _ = chans.trigger_rx.recv() => {
                    let last_log_idx = core.shared.last_log_index();
                    should_stop =
                        replicate_to(&core, &s, last_log_idx, &mut allow_pipeline).await;
                }
                // This is _not_ our heartbeat mechanism but ensures
                // followers quickly learn the leader's commit index when
                // raft commits stop flowing naturally.
                _ = tokio::time::sleep(random_duration(core.config().commit_timeout)) => {
                    let last_log_idx = core.shared.last_log_index();
                    should_stop =
                        replicate_to(&core, &s, last_log_idx, &mut allow_pipeline).await;
                }
            }

            // If things look healthy, switch to pipeline mode.
            if !should_stop && allow_pipeline {
                // Disable until re-enabled by a successful RPC.
                allow_pipeline = false;
                if let Err(e) = pipeline_replicate(&core, &s, &mut chans).await {
                    if !matches!(e, RaftError::PipelineReplicationNotSupported) {
                        error!(peer = s.peer().id, error = %e, "failed to start pipeline replication");
                    }
                }
                // Fall back to standard mode.
                continue 'rpc;
            }
        }
        break;
    }

    // Stop the heartbeat task.
    heartbeat_stop_tx.send_modify(|stopped| *stopped = true);
}

/// Replicates the logs up to the given last index, bringing the follower up
/// to date. Returns whether replication should stop. Mirrors `replicateTo`.
async fn replicate_to(
    core: &Arc<RaftCore>,
    s: &Arc<FollowerReplication>,
    last_index: u64,
    allow_pipeline: &mut bool,
) -> bool {
    // START in the Go implementation.
    loop {
        // Prevent an excessive retry rate on errors.
        let failures = s.failures.load(Ordering::Acquire);
        if failures > 0 {
            tokio::select! {
                _ = tokio::time::sleep(backoff(FAILURE_WAIT, failures, MAX_FAILURE_SCALE)) => {}
                _ = core.shutdown_wait() => {}
            }
        }

        let peer = s.peer();

        // Setup the request.
        let next_index = s.next_index.load(Ordering::Acquire);
        let req = match setup_append_entries(core, s, next_index, last_index).await {
            Ok(req) => req,
            Err(RaftError::LogNotFound) => {
                // The follower is too far behind; ship a snapshot instead.
                match send_latest_snapshot(core, s).await {
                    Ok(true) => return true,
                    Ok(false) => {}
                    Err(e) => {
                        error!(peer = peer.id, error = %e, "failed to send snapshot");
                        return false;
                    }
                }
                // CHECK_MORE: poll the stop flag, then check for more logs.
                if s.next_index.load(Ordering::Acquire) <= last_index {
                    continue;
                }
                return false;
            }
            Err(_) => return false,
        };

        // Make the RPC call.
        let resp = match core
            .trans
            .append_entries(&peer.id, &peer.address, &req)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                error!(peer = peer.id, error = %e, "failed to appendEntries");
                s.failures.fetch_add(1, Ordering::Release);
                return false;
            }
        };

        // Check for a newer term, stop running.
        if resp.term > req.term {
            handle_stale_term(s);
            return true;
        }

        // Update the last contact.
        s.set_last_contact();

        // Update s based on success.
        if resp.success {
            // Update our replication state.
            update_last_appended(s, &req);

            // Clear any failures, allow pipelining.
            s.failures.store(0, Ordering::Release);
            *allow_pipeline = true;
        } else {
            let next = s
                .next_index
                .load(Ordering::Acquire)
                .saturating_sub(1)
                .min(resp.last_log + 1)
                .max(1);
            s.next_index.store(next, Ordering::Release);
            if resp.no_retry_backoff {
                s.failures.store(0, Ordering::Release);
            } else {
                s.failures.fetch_add(1, Ordering::Release);
            }
            warn!(
                peer = peer.id,
                next, "appendEntries rejected, sending older logs"
            );
        }

        // CHECK_MORE: check if there are more logs to replicate.
        if s.next_index.load(Ordering::Acquire) <= last_index {
            continue;
        }
        return false;
    }
}

/// Sends the latest snapshot we have down to the follower. Returns
/// (should_stop, ()) on success paths, mirroring `sendLatestSnapshot`.
async fn send_latest_snapshot(core: &Arc<RaftCore>, s: &Arc<FollowerReplication>) -> Result<bool> {
    // Get the snapshots.
    let snapshots = core.snapshots.list().await?;
    if snapshots.is_empty() {
        return Err(RaftError::Snapshot("no snapshots found".into()));
    }

    // Open the most recent snapshot.
    let snap_id = snapshots[0].id.clone();
    info!(id = snap_id, "opening snapshot");
    let (meta, snapshot) = core.snapshots.open(&snap_id).await?;

    // Setup the request.
    let req = InstallSnapshotRequest {
        header: core.rpc_header(),
        snapshot_version: meta.version,
        term: s.current_term,
        last_log_index: meta.index,
        last_log_term: meta.term,
        configuration: encode_configuration(&meta.configuration)?,
        configuration_index: meta.configuration_index,
        size: meta.size,
    };

    let peer = s.peer();
    info!(
        peer = peer.id,
        id = snap_id,
        size = req.size,
        "installing snapshot"
    );
    let resp = core
        .trans
        .install_snapshot(&peer.id, &peer.address, &req, snapshot)
        .await
        .map_err(|e| {
            error!(peer = peer.id, id = snap_id, error = %e, "failed to install snapshot");
            s.failures.fetch_add(1, Ordering::Release);
            e
        })?;

    // Check for a newer term, stop running.
    if resp.term > req.term {
        handle_stale_term(s);
        return Ok(true);
    }

    // Update the last contact.
    s.set_last_contact();

    // Check for success.
    if resp.success {
        // Update the indexes.
        s.next_index.store(meta.index + 1, Ordering::Release);
        s.commitment.match_index(&peer.id, meta.index);
        // Clear any failures.
        s.failures.store(0, Ordering::Release);
        // Notify we are still leader.
        s.notify_all(true);
    } else {
        s.failures.fetch_add(1, Ordering::Release);
        warn!(peer = peer.id, id = snap_id, "installSnapshot rejected");
    }
    Ok(false)
}

/// Periodically invokes AppendEntries on a peer to ensure it doesn't time
/// out. Runs async of `replicate`, which could be blocked on disk IO.
/// Mirrors `heartbeat`.
async fn heartbeat(
    core: Arc<RaftCore>,
    s: Arc<FollowerReplication>,
    mut notify_rx: mpsc::Receiver<()>,
    stop_rx: watch::Receiver<bool>,
) {
    let mut failures = 0u64;

    loop {
        // Wait for the next heartbeat interval or forced notify.
        tokio::select! {
            _ = notify_rx.recv() => {}
            _ = tokio::time::sleep(random_duration(core.config().heartbeat_timeout / 10)) => {}
            _ = crate::raft::wait_flag(&stop_rx) => return,
        }

        let peer = s.peer();
        let req = AppendEntriesRequest {
            header: core.rpc_header(),
            term: s.current_term,
            prev_log_entry: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit_index: 0,
        };

        match core
            .trans
            .append_entries(&peer.id, &peer.address, &req)
            .await
        {
            Err(e) => {
                let next_backoff = capped_exponential_backoff(
                    FAILURE_WAIT,
                    failures,
                    MAX_FAILURE_SCALE,
                    core.config().heartbeat_timeout / 2,
                );
                error!(peer = peer.address, ?next_backoff, error = %e, "failed to heartbeat");
                failures += 1;
                tokio::select! {
                    _ = tokio::time::sleep(next_backoff) => {}
                    _ = crate::raft::wait_flag(&stop_rx) => return,
                }
            }
            Ok(resp) => {
                s.set_last_contact();
                failures = 0;
                s.notify_all(resp.success);
            }
        }
    }
}

/// Replicates using a pipeline for high performance. Cannot gracefully
/// recover from errors; the caller falls back to standard mode on failure.
/// Mirrors `pipelineReplicate`.
async fn pipeline_replicate(
    core: &Arc<RaftCore>,
    s: &Arc<FollowerReplication>,
    chans: &mut ReplicationChannels,
) -> Result<()> {
    let peer = s.peer();

    // Create a new pipeline.
    let pipeline = core
        .trans
        .append_entries_pipeline(&peer.id, &peer.address)
        .await?;

    info!(peer = peer.id, "pipelining replication");

    // Start a dedicated decoder.
    let (decode_stop_tx, decode_stop_rx) = watch::channel(false);
    let mut decode_done = tokio::spawn(pipeline_decode(
        Arc::clone(s),
        pipeline.consumer(),
        decode_stop_rx,
    ));

    // Start pipeline sends at the last good next_index.
    let mut next_index = s.next_index.load(Ordering::Acquire);

    let result: Result<()> = async {
        let mut should_stop = false;
        while !should_stop {
            tokio::select! {
                max_index = chans.stop_rx.recv() => {
                    // Make a best effort to replicate up to this index.
                    if let Some(max_index) = max_index {
                        if max_index > 0 {
                            pipeline_send(core, s, &*pipeline, &mut next_index, max_index).await?;
                        }
                    }
                    break;
                }
                _ = chans.trigger_rx.recv() => {
                    let last_log_idx = core.shared.last_log_index();
                    should_stop =
                        pipeline_send(core, s, &*pipeline, &mut next_index, last_log_idx).await.is_err();
                }
                _ = tokio::time::sleep(random_duration(core.config().commit_timeout)) => {
                    let last_log_idx = core.shared.last_log_index();
                    should_stop =
                        pipeline_send(core, s, &*pipeline, &mut next_index, last_log_idx).await.is_err();
                }
                done = &mut decode_done => {
                    // The decoder finished (newer term or rejected append).
                    match done {
                        Ok(()) => {}
                        Err(e) => error!(error = %e, "pipeline decoder panicked"),
                    }
                    break;
                }
            }
        }
        Ok(())
    }
    .await;

    // Stop the decoder and close the pipeline.
    decode_stop_tx.send_modify(|stopped| *stopped = true);
    let _ = decode_done.await;
    let _ = pipeline.close().await;

    info!(peer = peer.id, "aborting pipeline replication");
    result
}

/// Sends data over a pipeline. Mirrors `pipelineSend`.
async fn pipeline_send(
    core: &Arc<RaftCore>,
    s: &Arc<FollowerReplication>,
    pipeline: &dyn AppendPipeline,
    next_index: &mut u64,
    last_index: u64,
) -> Result<()> {
    // Create a new append request.
    let req = setup_append_entries(core, s, *next_index, last_index).await?;

    // Pipeline the append entries.
    pipeline.append_entries(req.clone()).await.map_err(|e| {
        error!(peer = s.peer().id, error = %e, "failed to pipeline appendEntries");
        e
    })?;

    // Increase the next send log to avoid re-sending old logs.
    if let Some(last) = req.entries.last() {
        *next_index = last.index + 1;
    }
    Ok(())
}

/// Decodes the responses of pipelined requests. Mirrors `pipelineDecode`.
async fn pipeline_decode(
    s: Arc<FollowerReplication>,
    mut consumer: mpsc::Receiver<crate::future::AppendFuture>,
    stop_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            ready = consumer.recv() => {
                let Some(ready) = ready else { return };
                let resp = match ready.wait().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(error = %e, "pipeline appendEntries failed");
                        return;
                    }
                };
                let req = ready.request();

                // Check for a newer term, stop running.
                if resp.term > req.term {
                    handle_stale_term(&s);
                    return;
                }

                // Update the last contact.
                s.set_last_contact();

                // Abort pipeline if not successful.
                if !resp.success {
                    return;
                }

                // Update our replication state.
                update_last_appended(&s, req);
            }
            _ = crate::raft::wait_flag(&stop_rx) => return,
        }
    }
}

/// Sets up an AppendEntries request. Mirrors `setupAppendEntries`.
async fn setup_append_entries(
    core: &Arc<RaftCore>,
    s: &Arc<FollowerReplication>,
    next_index: u64,
    last_index: u64,
) -> Result<AppendEntriesRequest> {
    let mut req = AppendEntriesRequest {
        header: core.rpc_header(),
        term: s.current_term,
        prev_log_entry: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit_index: core.shared.commit_index(),
    };
    set_previous_log(core, &mut req, next_index).await?;
    set_new_logs(core, &mut req, next_index, last_index).await?;
    Ok(req)
}

/// Sets up the PrevLogEntry and PrevLogTerm for an AppendEntriesRequest
/// given the next index to replicate. Mirrors `setPreviousLog`.
async fn set_previous_log(
    core: &Arc<RaftCore>,
    req: &mut AppendEntriesRequest,
    next_index: u64,
) -> Result<()> {
    // Guard for the first index, since there is no 0 log entry. Guard
    // against the previous index being a snapshot as well.
    let (last_snap_idx, last_snap_term) = core.shared.last_snapshot();
    if next_index == 1 {
        req.prev_log_entry = 0;
        req.prev_log_term = 0;
    } else if (next_index - 1) == last_snap_idx {
        req.prev_log_entry = last_snap_idx;
        req.prev_log_term = last_snap_term;
    } else {
        let l = core.logs.get_log(next_index - 1).await.map_err(|e| {
            error!(index = next_index - 1, error = %e, "failed to get log");
            e
        })?;
        req.prev_log_entry = l.index;
        req.prev_log_term = l.term;
    }
    Ok(())
}

/// Sets up the logs which should be appended for a request. Mirrors
/// `setNewLogs`.
async fn set_new_logs(
    core: &Arc<RaftCore>,
    req: &mut AppendEntriesRequest,
    next_index: u64,
    last_index: u64,
) -> Result<()> {
    let max_append_entries = core.config().max_append_entries as u64;
    let max_index = (next_index + max_append_entries - 1).min(last_index);
    let mut entries: Vec<Log> = Vec::new();
    for i in next_index..=max_index {
        let log = core.logs.get_log(i).await.map_err(|e| {
            error!(index = i, error = %e, "failed to get log");
            e
        })?;
        entries.push(log);
    }
    req.entries = entries;
    Ok(())
}

/// Handles a follower indicating that we have a stale term. Mirrors
/// `handleStaleTerm`.
fn handle_stale_term(s: &Arc<FollowerReplication>) {
    error!(
        peer = s.peer().id,
        "peer has newer term, stopping replication"
    );
    // No longer leader.
    s.notify_all(false);
    let _ = s.step_down_tx.try_send(());
}

/// Updates follower replication state after a successful AppendEntries RPC.
/// Mirrors `updateLastAppended`.
fn update_last_appended(s: &Arc<FollowerReplication>, req: &AppendEntriesRequest) {
    // Mark any inflight logs as committed.
    if let Some(last) = req.entries.last() {
        s.next_index.store(last.index + 1, Ordering::Release);
        s.commitment.match_index(&s.peer().id, last.index);
    }

    // Notify still leader.
    s.notify_all(true);
}
