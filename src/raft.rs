//! The raft main loop: state dispatch between follower/candidate/leader, RPC
//! handling, and the leader's commit bookkeeping. Mirrors raft.go of the Go
//! implementation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::commitment::Commitment;
use crate::config::{Config, PROTOCOL_VERSION_MAX, SNAPSHOT_VERSION_MAX};
use crate::configuration::{
    decode_configuration, encode_configuration, next_configuration, Configuration, ServerAddress,
    ServerID, ServerSuffrage,
};
use crate::fsm::{FsmMutate, FSM};
use crate::future::{
    BootstrapFutureState, ConfigurationChangeFuture, Configurations, ConfigurationsFuture,
    LeadershipTransferFutureState, LogFuture, ReqSnapshotFuture, RestoreFuture, ShutdownFuture,
    UserRestoreFutureState, UserSnapshotFutureState, VerifyState,
};
use crate::log::{Log, LogStore, LogType};
use crate::observer::{
    dispatch, LeaderObservation, ObservationData, ObserverHandle, PeerObservation,
};
use crate::replication::{replicate, FollowerReplication};
use crate::snapshot::{
    compact_logs, copy_to_sink, drain_reader, SnapshotMeta, SnapshotReader, SnapshotStore,
};
use crate::stable::{StableStore, CURRENT_TERM_KEY, LAST_VOTE_CAND_KEY, LAST_VOTE_TERM_KEY};
use crate::state::{RaftSharedState, RaftState};
use crate::transport::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RPCCommand, RPCHeader, RPCResponder, RPCResponse, RequestPreVoteRequest,
    RequestPreVoteResponse, RequestVoteRequest, RequestVoteResponse, TimeoutNowResponse, Transport,
    RPC,
};
use crate::{RaftError, Result};

/// Minimum interval between leader lease checks, matching the Go
/// implementation.
pub(crate) const MIN_CHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Returns a random duration in `[min, 2*min)`, mirroring `randomTimeout` in
/// util.go (used to randomize election and heartbeat timers).
pub(crate) fn random_duration(min: Duration) -> Duration {
    if min.is_zero() {
        return Duration::ZERO;
    }
    let extra = rand::random::<u64>() % min.as_nanos() as u64;
    min + Duration::from_nanos(extra)
}

/// Exponential backoff: base scaled by 2^(round-2), capped at scale `limit`.
/// Mirrors `backoff` in util.go.
pub(crate) fn backoff(mut base: Duration, round: u64, limit: u64) -> Duration {
    let mut power = round.min(limit);
    while power > 2 {
        base *= 2;
        power -= 1;
    }
    base
}

/// Exponential backoff with an adjustable cap. Mirrors
/// `cappedExponentialBackoff` in util.go.
pub(crate) fn capped_exponential_backoff(
    mut base: Duration,
    round: u64,
    limit: u64,
    cap: Duration,
) -> Duration {
    let mut power = round.min(limit);
    while power > 2 {
        if base > cap {
            return cap;
        }
        base *= 2;
        power -= 1;
    }
    base.min(cap)
}

/// State shared between the raft API handle, the main loop, and the
/// replication/heartbeat tasks. Everything here is safe to touch from any
/// task; mutable core state (configurations, leader state) lives in
/// [`MainLoop`] instead. Mirrors the shareable parts of the Go `Raft`
/// struct.
pub(crate) struct RaftCore {
    pub(crate) conf: RwLock<Config>,
    pub(crate) shared: RaftSharedState,
    #[allow(dead_code)]
    pub(crate) fsm: Arc<dyn FSM>,
    pub(crate) logs: Arc<dyn LogStore>,
    pub(crate) stable: Arc<dyn StableStore>,
    pub(crate) snapshots: Arc<dyn SnapshotStore>,
    pub(crate) trans: Arc<dyn Transport>,
    pub(crate) local_id: ServerID,
    pub(crate) local_addr: ServerAddress,
    pub(crate) leader: RwLock<(ServerAddress, ServerID)>,
    pub(crate) leader_tx: watch::Sender<bool>,
    /// Keeps `leader_tx` sendable even with no subscribers.
    #[allow(dead_code)]
    pub(crate) leader_rx: watch::Receiver<bool>,
    pub(crate) candidate_from_leadership_transfer: AtomicBool,
    pub(crate) pre_vote_disabled: bool,
    /// Copy of the latest configuration and its index, readable outside the
    /// main loop. Mirrors the Go `latestConfiguration` atomic value.
    pub(crate) latest_configuration: RwLock<(Configuration, u64)>,

    pub(crate) shutdown_tx: watch::Sender<bool>,
    pub(crate) shutdown_initiated: Mutex<bool>,
    pub(crate) tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,

    // Channel senders; the matching receivers live in the main loop or the
    // FSM/snapshot tasks.
    pub(crate) apply_tx: mpsc::Sender<LogFuture>,
    pub(crate) verify_tx: mpsc::UnboundedSender<Arc<VerifyState>>,
    pub(crate) config_change_tx: mpsc::Sender<ConfigurationChangeFuture>,
    #[allow(dead_code)]
    pub(crate) configurations_tx: mpsc::Sender<ConfigurationsFuture>,
    pub(crate) bootstrap_tx: mpsc::Sender<BootstrapFutureState>,
    pub(crate) leadership_transfer_tx: mpsc::Sender<LeadershipTransferFutureState>,
    pub(crate) user_snapshot_tx: mpsc::Sender<UserSnapshotFutureState>,
    pub(crate) user_restore_tx: mpsc::Sender<UserRestoreFutureState>,
    pub(crate) fsm_mutate_tx: mpsc::Sender<FsmMutate>,
    #[allow(dead_code)]
    pub(crate) fsm_snapshot_tx: mpsc::Sender<ReqSnapshotFuture>,
    #[allow(dead_code)]
    pub(crate) leader_notify_tx: mpsc::Sender<()>,
    #[allow(dead_code)]
    pub(crate) follower_notify_tx: mpsc::Sender<()>,

    /// Registered observers and the RW lock that protects the list.
    /// Mirrors `observers` + `observersLock` in the Go Raft struct.
    pub(crate) observers: parking_lot::RwLock<Vec<Arc<ObserverHandle>>>,
}

impl RaftCore {
    /// The current configuration. Mirrors the Go `config()` helper reading
    /// the atomically stored Config.
    pub(crate) fn config(&self) -> Config {
        self.conf.read().clone()
    }

    /// An initialized RPC header for outgoing requests and responses.
    /// Mirrors `getRPCHeader`.
    pub(crate) fn rpc_header(&self) -> RPCHeader {
        RPCHeader {
            protocol_version: PROTOCOL_VERSION_MAX,
            id: self.local_id.clone(),
            addr: self.local_addr.clone(),
        }
    }

    /// Houses logic about whether this instance can process the given RPC
    /// message. Mirrors `checkRPCHeader`; only protocol versions 2 and 3 are
    /// accepted (one back from the implemented version 3).
    pub(crate) fn check_rpc_header(&self, header: &RPCHeader) -> Result<()> {
        if header.protocol_version > PROTOCOL_VERSION_MAX
            || header.protocol_version < PROTOCOL_VERSION_MAX - 1
        {
            return Err(RaftError::UnsupportedProtocol);
        }
        Ok(())
    }

    /// Updates the current leader address and ID. Mirrors `setLeader`.
    pub(crate) fn set_leader(&self, addr: ServerAddress, id: ServerID) {
        *self.leader.write() = (addr.clone(), id.clone());
        // Emit a leader observation only when we transition to a real
        // leader (matches Go's `setLeader`, which calls `observe` only
        // when a non-empty address is set).
        if !addr.is_empty() {
            dispatch(
                &self.observers,
                ObservationData::Leader(LeaderObservation {
                    leader_addr: addr,
                    leader_id: id,
                    leader: String::new(),
                }),
            );
        }
    }

    /// The current leader address and ID, empty strings if unknown. Mirrors
    /// `LeaderWithID`.
    pub(crate) fn leader_with_id(&self) -> (ServerAddress, ServerID) {
        self.leader.read().clone()
    }

    /// Updates the current state. Any state transition clears the known
    /// leader, so the leader must be set only after updating the state.
    /// Mirrors `setState`.
    pub(crate) fn set_state(&self, state: RaftState) {
        self.set_leader(String::new(), String::new());
        self.shared.set_state(state);
        dispatch(&self.observers, ObservationData::State(state));
    }

    /// Sets the current term in a durable manner: persisted to the stable
    /// store before the in-memory update. Mirrors `setCurrentTerm`, which
    /// panics on persistence failure.
    pub(crate) async fn set_current_term(&self, term: u64) {
        if let Err(e) = self.stable.set_u64(CURRENT_TERM_KEY, term).await {
            panic!("failed to save current term: {}", e);
        }
        self.shared.set_current_term(term);
    }

    /// Persists our vote for safety. Mirrors `persistVote`.
    pub(crate) async fn persist_vote(&self, term: u64, candidate: &[u8]) -> Result<()> {
        self.stable.set_u64(LAST_VOTE_TERM_KEY, term).await?;
        self.stable.set(LAST_VOTE_CAND_KEY, candidate).await?;
        Ok(())
    }

    /// Sets the last-contact time to now. Mirrors `setLastContact`.
    pub(crate) fn set_last_contact_now(&self) {
        self.shared.set_last_contact(Instant::now());
    }

    /// Stores a copy of the latest configuration for off-main-thread reads.
    /// Mirrors the `latestConfiguration` atomic in `setLatestConfiguration`.
    pub(crate) fn store_latest_configuration(&self, conf: Configuration, index: u64) {
        *self.latest_configuration.write() = (conf, index);
    }

    /// Registers a freshly-created observer handle. The `self_ref` is
    /// stored so [`ObserverChannel::deregister`] can clean up after
    /// itself. Mirrors `Raft.RegisterObserver`.
    pub(crate) fn register_observer(
        &self,
        handle: Arc<ObserverHandle>,
        self_ref: std::sync::Weak<RaftCore>,
    ) {
        *handle.core.write() = Some(self_ref);
        self.observers.write().push(handle);
    }

    /// Removes the observer with the given ID, if registered. Mirrors
    /// `Raft.DeregisterObserver`.
    pub(crate) fn deregister_observer(&self, id: u64) {
        self.observers.write().retain(|o| o.id != id);
    }

    /// The latest configuration copy and its index.
    pub(crate) fn latest_configuration(&self) -> (Configuration, u64) {
        self.latest_configuration.read().clone()
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    /// Resolves once shutdown has been initiated. Race-free even if shutdown
    /// already happened.
    pub(crate) async fn shutdown_wait(&self) {
        let _ = self
            .shutdown_tx
            .subscribe()
            .wait_for(|shutdown| *shutdown)
            .await;
    }

    /// Spawns a background task tracked for shutdown, mirroring the Go
    /// `goFunc`/`routinesGroup` pair.
    pub(crate) async fn go_func<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.tasks.lock().await.spawn(fut);
    }

    /// Initiates shutdown: closes the shutdown signal, marks the state
    /// Shutdown, and returns a future that resolves once all tracked tasks
    /// have exited and the transport has been closed. Mirrors `Shutdown`.
    pub(crate) fn shutdown(&self) -> ShutdownFuture {
        {
            let mut initiated = self.shutdown_initiated.lock();
            if *initiated {
                // Already shut down; avoid closing the transport twice.
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(());
                return ShutdownFuture::new(rx, None);
            }
            *initiated = true;
        }
        self.shared.set_state(RaftState::Shutdown);
        self.shutdown_tx.send_modify(|shutdown| *shutdown = true);

        // Resolve the future once every tracked task has exited. This waiter
        // is deliberately not itself tracked.
        let (tx, rx) = oneshot::channel();
        let tasks = Arc::clone(&self.tasks);
        tokio::spawn(async move {
            let mut tasks = tasks.lock().await;
            while tasks.join_next().await.is_some() {}
            let _ = tx.send(());
        });
        ShutdownFuture::new(rx, Some(Arc::clone(&self.trans)))
    }

    /// Special handler for heartbeat requests, fast-pathed by transports
    /// that support it to avoid head-of-line blocking behind disk IO.
    /// Mirrors `processHeartbeat`; only heartbeat-shaped AppendEntries (no
    /// entries, no previous log, no commit index) are dispatched here, so it
    /// only touches state guarded for concurrent access. Must not be called
    /// from the main loop.
    pub(crate) async fn process_heartbeat(core: Arc<RaftCore>, rpc: RPC) {
        if core.is_shutdown() {
            return;
        }
        let (command, _reader, responder) = rpc.split();
        let RPCCommand::AppendEntries(req) = command else {
            tracing::error!("expected heartbeat, got {:?}", command);
            responder.respond_error(RaftError::Other("unexpected command".into()));
            return;
        };

        let mut resp = AppendEntriesResponse {
            header: core.rpc_header(),
            term: core.shared.current_term(),
            last_log: core.shared.last_index(),
            success: false,
            no_retry_backoff: false,
        };

        // Ignore an older term.
        if req.term < core.shared.current_term() {
            responder.respond(RPCResponse::AppendEntries(resp));
            return;
        }

        // Increase the term if we see a newer one, and transition to
        // follower if we ever get an AppendEntries call.
        if req.term > core.shared.current_term()
            || (core.shared.state() != RaftState::Follower
                && !core
                    .candidate_from_leadership_transfer
                    .load(Ordering::Acquire))
        {
            core.shared.set_state(RaftState::Follower);
            core.set_current_term(req.term).await;
            resp.term = req.term;
        }

        // Save the current leader.
        core.set_leader(req.header.addr, req.header.id);

        resp.success = true;
        core.set_last_contact_now();
        responder.respond(RPCResponse::AppendEntries(resp));
    }
}

/// State used only while leader. Mirrors the Go `leaderState` struct.
pub(crate) struct LeaderState {
    pub(crate) commit_rx: mpsc::Receiver<()>,
    pub(crate) commitment: Arc<Commitment>,
    /// Inflight log futures, in log index order.
    pub(crate) inflight: VecDeque<LogFuture>,
    pub(crate) repl_state: HashMap<ServerID, Arc<FollowerReplication>>,
    /// Senders for the replication stop channels; dropping them closes the
    /// channels, like `close(stopCh)` in Go.
    pub(crate) stop_txs: HashMap<ServerID, mpsc::Sender<u64>>,
    /// Pending verify-leader requests.
    pub(crate) notify: Vec<Arc<VerifyState>>,
    pub(crate) step_down_tx: mpsc::Sender<()>,
    pub(crate) step_down_rx: mpsc::Receiver<()>,
    pub(crate) leadership_transfer_in_progress: Arc<AtomicBool>,
}

/// The result of a single (pre-)vote RPC, mirroring the Go `voteResult` and
/// `preVoteResult` structs.
pub(crate) struct VoteResult {
    pub(crate) term: u64,
    pub(crate) granted: bool,
    #[allow(dead_code)]
    pub(crate) voter_id: ServerID,
}

/// The raft main loop. Owns the mutable core state; only this task mutates
/// the configurations and leader state. Mirrors the main-thread parts of
/// the Go `Raft`.
pub(crate) struct MainLoop {
    pub(crate) core: Arc<RaftCore>,
    /// Tracks the latest and committed configurations from the log/snapshot.
    pub(crate) configurations: Configurations,

    pub(crate) rpc_rx: mpsc::Receiver<RPC>,
    pub(crate) apply_rx: mpsc::Receiver<LogFuture>,
    pub(crate) verify_rx: mpsc::UnboundedReceiver<Arc<VerifyState>>,
    pub(crate) config_change_rx: mpsc::Receiver<ConfigurationChangeFuture>,
    pub(crate) configurations_rx: mpsc::Receiver<ConfigurationsFuture>,
    pub(crate) bootstrap_rx: mpsc::Receiver<BootstrapFutureState>,
    pub(crate) leadership_transfer_rx: mpsc::Receiver<LeadershipTransferFutureState>,
    pub(crate) user_restore_rx: mpsc::Receiver<UserRestoreFutureState>,
    pub(crate) leader_notify_rx: mpsc::Receiver<()>,
    pub(crate) follower_notify_rx: mpsc::Receiver<()>,
}

impl MainLoop {
    /// The main task: dispatches to the per-state loops. Mirrors `Raft.run`.
    pub(crate) async fn run(mut self) {
        loop {
            if self.core.is_shutdown() {
                // Clear the leader to prevent forwarding.
                self.core.set_leader(String::new(), String::new());
                return;
            }
            match self.core.shared.state() {
                RaftState::Follower => self.run_follower().await,
                RaftState::Candidate => self.run_candidate().await,
                RaftState::Leader => self.run_leader().await,
                RaftState::Shutdown => return,
            }
        }
    }

    /// Size of a quorum of the latest configuration. Must only be called on
    /// the main task. Mirrors `quorumSize`.
    fn quorum_size(&self) -> usize {
        let voters = self
            .configurations
            .latest
            .servers
            .iter()
            .filter(|s| s.suffrage == ServerSuffrage::Voter)
            .count();
        voters / 2 + 1
    }

    /// Stores the latest configuration and updates the readable copy.
    /// Mirrors `setLatestConfiguration`.
    pub(crate) fn set_latest_configuration(&mut self, conf: Configuration, index: u64) {
        self.core.store_latest_configuration(conf.clone(), index);
        self.configurations.latest = conf;
        self.configurations.latest_index = index;
    }

    /// Stores the committed configuration. Mirrors
    /// `setCommittedConfiguration`.
    pub(crate) fn set_committed_configuration(&mut self, conf: Configuration, index: u64) {
        self.configurations.committed = conf;
        self.configurations.committed_index = index;
    }

    /// Updates the configurations if the entry is a configuration entry.
    /// Mirrors `processConfigurationLogEntry` (protocol v3 branch only).
    pub(crate) fn process_configuration_log_entry(&mut self, entry: &Log) {
        if entry.log_type == LogType::Configuration {
            let latest = std::mem::take(&mut self.configurations.latest);
            self.set_committed_configuration(latest, self.configurations.latest_index);
            match decode_configuration(&entry.data) {
                Ok(conf) => self.set_latest_configuration(conf, entry.index),
                Err(e) => {
                    tracing::error!(
                        "failed to decode configuration entry at index {}: {}",
                        entry.index,
                        e
                    );
                }
            }
        }
    }

    /// Runs the main loop while in the follower state. Mirrors
    /// `runFollower`.
    async fn run_follower(&mut self) {
        let mut did_warn = false;
        let (_, leader_id) = self.core.leader_with_id();
        info!(leader_id, "entering follower state");
        let hb_timeout = self.core.config().heartbeat_timeout;
        let heartbeat_timer = tokio::time::sleep(random_duration(hb_timeout));
        tokio::pin!(heartbeat_timer);

        while self.core.shared.state() == RaftState::Follower {
            tokio::select! {
                rpc = self.rpc_rx.recv() => {
                    let Some(rpc) = rpc else { return };
                    self.process_rpc(rpc).await;
                }
                c = self.config_change_rx.recv() => {
                    // Reject any operations since we are not the leader.
                    let Some(mut c) = c else { return };
                    c.respond_error(RaftError::NotLeader);
                }
                a = self.apply_rx.recv() => {
                    let Some(mut a) = a else { return };
                    a.respond_error(RaftError::NotLeader);
                }
                v = self.verify_rx.recv() => {
                    let Some(v) = v else { return };
                    v.respond(Err(RaftError::NotLeader));
                }
                ur = self.user_restore_rx.recv() => {
                    let Some(mut ur) = ur else { return };
                    ur.respond(Err(RaftError::NotLeader));
                }
                l = self.leadership_transfer_rx.recv() => {
                    let Some(mut l) = l else { return };
                    l.respond(Err(RaftError::NotLeader));
                }
                c = self.configurations_rx.recv() => {
                    let Some(c) = c else { return };
                    self.answer_configurations(c);
                }
                b = self.bootstrap_rx.recv() => {
                    // Live bootstrap is implemented with the membership
                    // phase; a bootstrapped or running cluster always
                    // refuses.
                    let Some(mut b) = b else { return };
                    b.respond(Err(RaftError::CantBootstrap));
                }
                _ = self.leader_notify_rx.recv() => {
                    // Ignore since we are not the leader.
                }
                _ = self.follower_notify_rx.recv() => {
                    // Config changed; check for leader contact immediately.
                    heartbeat_timer.as_mut().reset(tokio::time::Instant::now());
                }
                _ = &mut heartbeat_timer => {
                    // Restart the heartbeat timer.
                    heartbeat_timer
                        .as_mut()
                        .reset(tokio::time::Instant::now() + random_duration(hb_timeout));

                    // Check if we have had a successful contact.
                    if let Some(last_contact) = self.core.shared.last_contact_time() {
                        if last_contact.elapsed() < hb_timeout {
                            continue;
                        }
                    }

                    // Heartbeat failed! Transition to the candidate state.
                    let (last_leader_addr, last_leader_id) = self.core.leader_with_id();
                    self.core.set_leader(String::new(), String::new());

                    if self.configurations.latest_index == 0 {
                        if !did_warn {
                            warn!("no known peers, aborting election");
                            did_warn = true;
                        }
                    } else if self.configurations.latest_index
                        == self.configurations.committed_index
                        && !self.configurations.latest.has_vote(&self.core.local_id)
                    {
                        if !did_warn {
                            warn!("not part of stable configuration, aborting election");
                            did_warn = true;
                        }
                    } else if self.configurations.latest.has_vote(&self.core.local_id) {
                        warn!(
                            last_leader_addr,
                            last_leader_id, "heartbeat timeout reached, starting election"
                        );
                        self.core.set_state(RaftState::Candidate);
                        return;
                    } else if !did_warn {
                        warn!("heartbeat timeout reached, not part of a stable configuration or a non-voter, not triggering a leader election");
                        did_warn = true;
                    }
                }
                _ = self.core.shutdown_wait() => return,
            }
        }
    }

    /// Runs the main loop while in the candidate state. Mirrors
    /// `runCandidate`.
    async fn run_candidate(&mut self) {
        let term = self.core.shared.current_term() + 1;
        info!(term, "entering candidate state");

        // Start a vote for ourselves; pre-vote first unless disabled or this
        // candidacy comes from a leadership transfer (which skips pre-vote
        // by design).
        let mut prevote_rx: Option<mpsc::Receiver<VoteResult>> = None;
        let mut vote_rx: Option<mpsc::Receiver<VoteResult>> = None;
        if !self.core.pre_vote_disabled
            && !self
                .core
                .candidate_from_leadership_transfer
                .load(Ordering::Acquire)
        {
            prevote_rx = Some(self.pre_elect_self().await);
        } else {
            vote_rx = Some(self.elect_self().await);
        }

        // Reset the leadership-transfer flag after each run, so the
        // privilege of the LeadershipTransfer vote flag cannot be abused.
        let _reset_transfer_flag = ResetTransferFlag(Arc::clone(&self.core));

        let election_timeout = self.core.config().election_timeout;
        let election_timer = tokio::time::sleep(random_duration(election_timeout));
        tokio::pin!(election_timer);

        // Tally the votes; a simple majority is needed.
        let mut prevote_granted = 0usize;
        let mut prevote_refused = 0usize;
        let mut granted = 0usize;
        let votes_needed = self.quorum_size();
        debug!(votes_needed, term, "calculated votes needed");

        while self.core.shared.state() == RaftState::Candidate {
            tokio::select! {
                rpc = self.rpc_rx.recv() => {
                    let Some(rpc) = rpc else { return };
                    self.process_rpc(rpc).await;
                }
                pre_vote = recv_optional(&mut prevote_rx) => {
                    let Some(pre_vote) = pre_vote else { continue };
                    // Check if the term is greater than ours, bail.
                    if pre_vote.term > term {
                        debug!(term = pre_vote.term, "pre-vote denied: found newer term, falling back to follower");
                        self.core.set_state(RaftState::Follower);
                        self.core.set_current_term(pre_vote.term).await;
                        return;
                    }
                    if pre_vote.granted {
                        prevote_granted += 1;
                    } else {
                        prevote_refused += 1;
                    }
                    // Won the pre-vote: proceed to a real election.
                    if prevote_granted >= votes_needed {
                        info!(term = pre_vote.term, tally = prevote_granted, "pre-vote successful, starting election");
                        prevote_granted = 0;
                        prevote_refused = 0;
                        election_timer
                            .as_mut()
                            .reset(tokio::time::Instant::now() + random_duration(election_timeout));
                        prevote_rx = None;
                        vote_rx = Some(self.elect_self().await);
                    }
                    if prevote_refused >= votes_needed {
                        info!(term = pre_vote.term, "pre-vote campaign failed, waiting for election timeout");
                    }
                }
                vote = recv_optional(&mut vote_rx) => {
                    let Some(vote) = vote else { continue };
                    // Check if the term is greater than ours, bail.
                    if vote.term > self.core.shared.current_term() {
                        debug!(term = vote.term, "newer term discovered, fallback to follower");
                        self.core.set_state(RaftState::Follower);
                        self.core.set_current_term(vote.term).await;
                        return;
                    }
                    if vote.granted {
                        granted += 1;
                        debug!(from = vote.voter_id, tally = granted, "vote granted");
                    }
                    if granted >= votes_needed {
                        info!(term = vote.term, tally = granted, "election won");
                        self.core.set_state(RaftState::Leader);
                        self.core
                            .set_leader(self.core.local_addr.clone(), self.core.local_id.clone());
                        return;
                    }
                }
                c = self.config_change_rx.recv() => {
                    let Some(mut c) = c else { return };
                    c.respond_error(RaftError::NotLeader);
                }
                a = self.apply_rx.recv() => {
                    let Some(mut a) = a else { return };
                    a.respond_error(RaftError::NotLeader);
                }
                v = self.verify_rx.recv() => {
                    let Some(v) = v else { return };
                    v.respond(Err(RaftError::NotLeader));
                }
                ur = self.user_restore_rx.recv() => {
                    let Some(mut ur) = ur else { return };
                    ur.respond(Err(RaftError::NotLeader));
                }
                l = self.leadership_transfer_rx.recv() => {
                    let Some(mut l) = l else { return };
                    l.respond(Err(RaftError::NotLeader));
                }
                c = self.configurations_rx.recv() => {
                    let Some(c) = c else { return };
                    self.answer_configurations(c);
                }
                b = self.bootstrap_rx.recv() => {
                    let Some(mut b) = b else { return };
                    b.respond(Err(RaftError::CantBootstrap));
                }
                _ = self.leader_notify_rx.recv() => {
                    // Ignore since we are not the leader.
                }
                _ = self.follower_notify_rx.recv() => {
                    // Ignore; config reloads land with the membership phase.
                }
                _ = &mut election_timer => {
                    // Election failed! Restart the election by returning,
                    // which kicks us back into run_candidate.
                    warn!("election timeout reached, restarting election");
                    return;
                }
                _ = self.core.shutdown_wait() => return,
            }
        }
    }

    /// Runs the main loop while in the leader state. Mirrors `runLeader`
    /// including its cleanup defer.
    async fn run_leader(&mut self) {
        info!("entering leader state");

        // Notify that we are the leader.
        let _ = self.core.leader_tx.send(true);
        let notify = self.core.config().notify_ch;
        if let Some(notify) = &notify {
            tokio::select! {
                r = notify.send(true) => { let _ = r; }
                _ = self.core.shutdown_wait() => { let _ = notify.try_send(true); }
            }
        }

        // Setup leader state. This is only accessed within the leader loop.
        let (commit_tx, commit_rx) = mpsc::channel(1);
        let (step_down_tx, step_down_rx) = mpsc::channel(1);
        let commitment = Arc::new(Commitment::new(
            &self.configurations.latest,
            // The first index that may be committed in this term.
            self.core.shared.last_index() + 1,
            commit_tx,
        ));
        let mut ls = LeaderState {
            commit_rx,
            commitment,
            inflight: VecDeque::new(),
            repl_state: HashMap::new(),
            stop_txs: HashMap::new(),
            notify: Vec::new(),
            step_down_tx,
            step_down_rx,
            leadership_transfer_in_progress: Arc::new(AtomicBool::new(false)),
        };

        // Start a replication routine for each peer.
        self.start_stop_replication(&mut ls).await;

        // Dispatch a no-op log entry first. This gets this leader up to the
        // latest possible commit index, even in the absence of client
        // commands.
        let (noop, _rx) = LogFuture::new(Log {
            log_type: LogType::Noop,
            ..Default::default()
        });
        self.dispatch_logs(&mut ls, vec![noop]).await;

        // Sit in the leader loop until we step down.
        self.leader_loop(&mut ls).await;

        // Cleanup on step down (mirrors the Go defer):
        //
        // Update our last contact time, so that to a client our data does
        // not look extremely stale from before we were the leader.
        self.core.set_last_contact_now();

        // Stop replication: dropping the senders closes the stop channels.
        ls.stop_txs.clear();

        // Respond to all inflight operations and pending verify requests.
        for mut future in ls.inflight.drain(..) {
            future.respond_error(RaftError::LeadershipLost);
        }
        for future in ls.notify.drain(..) {
            future.respond(Err(RaftError::LeadershipLost));
        }

        // If we are stepping down for some reason, there is no known leader.
        // We may have stepped down due to an RPC call, which would provide
        // the leader, so we cannot always blank this out.
        {
            let mut leader = self.core.leader.write();
            if leader.0 == self.core.local_addr && leader.1 == self.core.local_id {
                *leader = (String::new(), String::new());
            }
        }

        // Notify that we are not the leader.
        let _ = self.core.leader_tx.send(false);
        if let Some(notify) = &notify {
            tokio::select! {
                r = notify.send(false) => { let _ = r; }
                _ = self.core.shutdown_wait() => { let _ = notify.try_send(false); }
            }
        }
    }

    /// Sets up state and starts replication to new peers, and stops
    /// replication to removed peers (after a best-effort catch-up to the
    /// current index). Must only be called from the main task. Mirrors
    /// `startStopReplication`.
    async fn start_stop_replication(&mut self, ls: &mut LeaderState) {
        let last_idx = self.core.shared.last_index();
        let mut in_config: HashSet<&ServerID> = HashSet::new();

        // Start replication tasks that need starting.
        for server in &self.configurations.latest.servers {
            if server.id == self.core.local_id {
                continue;
            }
            in_config.insert(&server.id);

            match ls.repl_state.get(&server.id) {
                Some(repl) => {
                    let mut peer = repl.peer.write();
                    if peer.address != server.address {
                        info!(peer = server.id, "updating peer");
                        *peer = server.clone();
                    }
                }
                None => {
                    info!(peer = server.id, "added peer, starting replication");
                    dispatch(
                        &self.core.observers,
                        ObservationData::Peer(PeerObservation {
                            peer: server.clone(),
                            removed: false,
                        }),
                    );
                    let (stop_tx, stop_rx) = mpsc::channel(1);
                    let (trigger_tx, trigger_rx) = mpsc::channel(1);
                    let (notify_tx, notify_rx) = mpsc::channel(1);
                    let repl = Arc::new(FollowerReplication {
                        peer: RwLock::new(server.clone()),
                        commitment: Arc::clone(&ls.commitment),
                        current_term: self.core.shared.current_term(),
                        next_index: AtomicU64::new(last_idx + 1),
                        last_contact: Mutex::new(Instant::now()),
                        notify: Mutex::new(Vec::new()),
                        notify_tx,
                        step_down_tx: ls.step_down_tx.clone(),
                        trigger_tx,
                        failures: AtomicU64::new(0),
                    });
                    ls.repl_state.insert(server.id.clone(), Arc::clone(&repl));
                    ls.stop_txs.insert(server.id.clone(), stop_tx);
                    self.core
                        .go_func(replicate(
                            Arc::clone(&self.core),
                            repl,
                            stop_rx,
                            trigger_rx,
                            notify_rx,
                        ))
                        .await;
                }
            }
        }

        // Stop replication tasks that need stopping.
        let ids: Vec<ServerID> = ls.repl_state.keys().cloned().collect();
        for id in ids {
            if in_config.contains(&id) {
                continue;
            }
            // Replicate up to last_idx and stop.
            info!(
                peer = id,
                last_index = last_idx,
                "removed peer, stopping replication"
            );
            if let Some(repl) = ls.repl_state.remove(&id) {
                dispatch(
                    &self.core.observers,
                    ObservationData::Peer(PeerObservation {
                        peer: repl.peer.read().clone(),
                        removed: true,
                    }),
                );
            }
            if let Some(stop_tx) = ls.stop_txs.remove(&id) {
                let _ = stop_tx.try_send(last_idx);
                drop(stop_tx);
            }
        }
    }

    /// True if it is safe to process configuration changes: the latest
    /// configuration is committed, and this leader has committed an entry
    /// (the noop) in this term. Mirrors `configurationChangeChIfStable`.
    fn configuration_change_stable(&self, ls: &LeaderState) -> bool {
        self.configurations.latest_index == self.configurations.committed_index
            && self.core.shared.commit_index() >= ls.commitment.start_index()
    }

    /// The hot loop for a leader. Mirrors `leaderLoop`.
    async fn leader_loop(&mut self, ls: &mut LeaderState) {
        // Tracks if there is an inflight log that would cause us to lose
        // leadership (a removal of ourselves). If so, no new logs may be
        // processed.
        let mut step_down = false;
        let lease = tokio::time::sleep(self.core.config().leader_lease_timeout);
        tokio::pin!(lease);

        while self.core.shared.state() == RaftState::Leader {
            tokio::select! {
                rpc = self.rpc_rx.recv() => {
                    let Some(rpc) = rpc else { return };
                    self.process_rpc(rpc).await;
                }
                _ = ls.step_down_rx.recv() => {
                    self.core.set_state(RaftState::Follower);
                }
                future = self.leadership_transfer_rx.recv() => {
                    let Some(mut future) = future else { return };
                    if ls.leadership_transfer_in_progress.load(Ordering::Acquire) {
                        future.respond(Err(RaftError::LeadershipTransferInProgress));
                        continue;
                    }

                    // Pick the target server (specified or any voter other
                    // than self), or fail the request.
                    let (id, address) = match (future.id.clone(), future.address.clone()) {
                        (Some(id), Some(address)) => (id, address),
                        _ => match self.pick_server_for_transfer() {
                            Some(server) => (server.id.clone(), server.address.clone()),
                            None => {
                                future.respond(Err(RaftError::Other(
                                    "cannot find peer for leadership transfer".into(),
                                )));
                                continue;
                            }
                        },
                    };

                    let Some(repl) = ls.repl_state.get(&id).cloned() else {
                        future.respond(Err(RaftError::Other(format!(
                            "cannot find replication state for {}",
                            id
                        ))));
                        continue;
                    };

                    // Mark in progress and spawn the actual transfer.
                    ls.leadership_transfer_in_progress.store(true, Ordering::Release);
                    let election_timeout = self.core.config().election_timeout;
                    let core = Arc::clone(&self.core);
                    let step_down_tx = ls.step_down_tx.clone();
                    let ltip = Arc::clone(&ls.leadership_transfer_in_progress);

                    tokio::spawn(async move {
                        let outcome = leadership_transfer(
                            core.clone(),
                            id.clone(),
                            address.clone(),
                            repl.clone(),
                            step_down_tx,
                            election_timeout,
                        )
                        .await;
                        ltip.store(false, Ordering::Release);
                        future.respond(outcome);
                    });
                }
                _ = ls.commit_rx.recv() => {
                    // Process the newly committed entries.
                    let old_commit_index = self.core.shared.commit_index();
                    let commit_index = ls.commitment.commit_index();
                    self.core.shared.set_commit_index(commit_index);

                    // New configuration has been committed, set it as the
                    // committed value.
                    if self.configurations.latest_index > old_commit_index
                        && self.configurations.latest_index <= commit_index
                    {
                        let latest = self.configurations.latest.clone();
                        let latest_index = self.configurations.latest_index;
                        self.set_committed_configuration(latest, latest_index);
                        if !self.configurations.committed.has_vote(&self.core.local_id) {
                            step_down = true;
                        }
                    }

                    // Pull all inflight logs that are committed off the
                    // queue.
                    let mut group_futures: HashMap<u64, LogFuture> = HashMap::new();
                    let mut last_idx_in_group = 0;
                    while let Some(front) = ls.inflight.front() {
                        if front.log.index > commit_index {
                            break;
                        }
                        let future = ls.inflight.pop_front().expect("front checked");
                        last_idx_in_group = future.log.index;
                        group_futures.insert(future.log.index, future);
                    }

                    // Process the group.
                    if !group_futures.is_empty() {
                        self.process_logs(last_idx_in_group, group_futures).await;
                    }

                    if step_down {
                        if self.core.config().shutdown_on_remove {
                            info!("removed ourself, shutting down");
                            let _ = self.core.shutdown();
                        } else {
                            info!("removed ourself, transitioning to follower");
                            self.core.set_state(RaftState::Follower);
                        }
                    }
                }
                v = self.verify_rx.recv() => {
                    let Some(v) = v else { return };
                    if v.quorum_size() == 0 {
                        // Just dispatched, start the verification.
                        self.verify_leader(ls, v);
                    } else if v.votes() < v.quorum_size() {
                        // Early return, means there must be a new leader.
                        warn!("new leader elected, stepping down");
                        self.core.set_state(RaftState::Follower);
                        ls.notify.retain(|x| !Arc::ptr_eq(x, &v));
                        for repl in ls.repl_state.values() {
                            repl.clean_notify(&v);
                        }
                        v.respond(Err(RaftError::NotLeader));
                    } else {
                        // Quorum of members agree, we are still leader.
                        ls.notify.retain(|x| !Arc::ptr_eq(x, &v));
                        for repl in ls.repl_state.values() {
                            repl.clean_notify(&v);
                        }
                        v.respond(Ok(()));
                    }
                }
                future = self.user_restore_rx.recv() => {
                    let Some(mut future) = future else { return };
                    if ls.leadership_transfer_in_progress.load(Ordering::Acquire) {
                        debug!("leadership transfer in progress");
                        future.respond(Err(RaftError::LeadershipTransferInProgress));
                        continue;
                    }
                    let meta = std::mem::take(&mut future.meta);
                    let reader = std::mem::replace(&mut future.reader, Box::new(std::io::empty()));
                    let result = self.restore_user_snapshot(ls, &meta, reader).await;
                    future.respond(result);
                }
                future = self.configurations_rx.recv() => {
                    let Some(future) = future else { return };
                    self.answer_configurations(future);
                }
                future = self.config_change_rx.recv(), if self.configuration_change_stable(ls) => {
                    let Some(future) = future else { return };
                    self.append_configuration_entry(ls, future).await;
                }
                b = self.bootstrap_rx.recv() => {
                    let Some(mut b) = b else { return };
                    b.respond(Err(RaftError::CantBootstrap));
                }
                new_log = self.apply_rx.recv() => {
                    let Some(new_log) = new_log else { return };
                    if ls.leadership_transfer_in_progress.load(Ordering::Acquire) {
                        let mut new_log = new_log;
                        new_log.respond_error(RaftError::LeadershipTransferInProgress);
                        continue;
                    }
                    // Group commit: gather all the ready commits.
                    let mut ready = vec![new_log];
                    for _ in 0..self.core.config().max_append_entries {
                        match self.apply_rx.try_recv() {
                            Ok(new_log) => ready.push(new_log),
                            Err(_) => break,
                        }
                    }
                    // Dispatch the logs.
                    if step_down {
                        // We're in the process of stepping down as leader;
                        // don't process anything new.
                        for mut future in ready {
                            future.respond_error(RaftError::NotLeader);
                        }
                    } else {
                        self.dispatch_logs(ls, ready).await;
                    }
                }
                _ = &mut lease => {
                    // Check if we've exceeded the lease, potentially
                    // stepping down.
                    let max_diff = self.check_leader_lease(ls);

                    // Next check interval should adjust for the last node
                    // we've contacted, without going negative.
                    let mut check_interval = self
                        .core
                        .config()
                        .leader_lease_timeout
                        .saturating_sub(max_diff);
                    if check_interval < MIN_CHECK_INTERVAL {
                        check_interval = MIN_CHECK_INTERVAL;
                    }
                    lease.as_mut().reset(tokio::time::Instant::now() + check_interval);
                }
                _ = self.leader_notify_rx.recv() => {
                    for repl in ls.repl_state.values() {
                        let _ = repl.notify_tx.try_send(());
                    }
                }
                _ = self.follower_notify_rx.recv() => {
                    // Ignore since we are not a follower.
                }
                _ = self.core.shutdown_wait() => return,
            }
        }
    }

    /// Starts a leader verification: records our own vote, registers the
    /// future, and triggers immediate heartbeats. Must be called from the
    /// main task. Mirrors `verifyLeader`.
    fn verify_leader(&mut self, ls: &mut LeaderState, v: Arc<VerifyState>) {
        let quorum_size = self.quorum_size();
        // Hot path for a single-node cluster.
        if quorum_size == 1 {
            v.respond(Ok(()));
            return;
        }

        // Track this request (records the leader's own vote).
        v.start(quorum_size, self.core.verify_tx.clone());
        ls.notify.push(Arc::clone(&v));

        // Trigger immediate heartbeats.
        for repl in ls.repl_state.values() {
            repl.notify.lock().push(Arc::clone(&v));
            let _ = repl.notify_tx.try_send(());
        }
    }

    /// Picks any voter in the latest configuration other than this node,
    /// for use as the default target of a leadership transfer. Mirrors
    /// the `pickServer` helper in Go's `raft.go`.
    fn pick_server_for_transfer(&self) -> Option<crate::configuration::Server> {
        for server in &self.configurations.latest.servers {
            if server.suffrage == ServerSuffrage::Voter && server.id != self.core.local_id {
                return Some(server.clone());
            }
        }
        None
    }

    /// Checks if we can contact a quorum of nodes within the last leader
    /// lease interval; steps down if not. Returns the maximum duration
    /// without contact. Must only be called from the main task. Mirrors
    /// `checkLeaderLease`.
    fn check_leader_lease(&mut self, ls: &LeaderState) -> Duration {
        // We can always contact ourself.
        let mut contacted = 0usize;
        let lease_timeout = self.core.config().leader_lease_timeout;
        let mut max_diff = Duration::ZERO;
        let now = Instant::now();

        for server in &self.configurations.latest.servers {
            if server.suffrage != ServerSuffrage::Voter {
                continue;
            }
            if server.id == self.core.local_id {
                contacted += 1;
                continue;
            }
            let Some(repl) = ls.repl_state.get(&server.id) else {
                continue;
            };
            let diff = now.saturating_duration_since(repl.last_contact());
            if diff <= lease_timeout {
                contacted += 1;
                max_diff = max_diff.max(diff);
            } else if diff <= 3 * lease_timeout {
                warn!(server_id = server.id, ?diff, "failed to contact");
            } else {
                debug!(server_id = server.id, ?diff, "failed to contact");
            }
        }

        // Verify we can contact a quorum.
        if contacted < self.quorum_size() {
            warn!("failed to contact quorum of nodes, stepping down");
            self.core.set_state(RaftState::Follower);
        }
        max_diff
    }

    /// Changes the configuration and adds a new configuration entry to the
    /// log. Must only be called from the main task. Mirrors
    /// `appendConfigurationEntry`.
    async fn append_configuration_entry(
        &mut self,
        ls: &mut LeaderState,
        mut future: ConfigurationChangeFuture,
    ) {
        if future.req.prev_index > 0 && future.req.prev_index != self.configurations.latest_index {
            future.respond_error(RaftError::Other(format!(
                "configuration changed since {} (latest is {})",
                future.req.prev_index, self.configurations.latest_index
            )));
            return;
        }
        let configuration = match next_configuration(&self.configurations.latest, &future.req) {
            Ok(conf) => conf,
            Err(e) => {
                future.respond_error(e);
                return;
            }
        };

        info!(server_id = future.req.server_id, "updating configuration");

        future.log_future.log = Log {
            log_type: LogType::Configuration,
            data: match encode_configuration(&configuration) {
                Ok(data) => data,
                Err(e) => {
                    future.respond_error(e);
                    return;
                }
            },
            ..Default::default()
        };

        let ConfigurationChangeFuture { log_future, .. } = future;
        self.dispatch_logs(ls, vec![log_future]).await;
        // The dispatched entry was the last one written.
        let index = self.core.shared.last_index();
        self.set_latest_configuration(configuration.clone(), index);
        ls.commitment.set_configuration(&configuration);
        self.start_stop_replication(ls).await;
    }

    /// Pushes logs to disk, marks them inflight and begins replication.
    /// Must only be called on the leader, from the main task. Mirrors
    /// `dispatchLogs`.
    async fn dispatch_logs(&mut self, ls: &mut LeaderState, apply_logs: Vec<LogFuture>) {
        let now = Instant::now();
        let term = self.core.shared.current_term();
        let mut last_index = self.core.shared.last_index();
        let n = apply_logs.len();

        let mut logs = Vec::with_capacity(n);
        let mut apply_logs = apply_logs;
        for apply_log in apply_logs.iter_mut() {
            apply_log.dispatch = now;
            last_index += 1;
            apply_log.log.index = last_index;
            apply_log.log.term = term;
            apply_log.log.appended_at = Some(now);
            logs.push(apply_log.log.clone());
        }
        for apply_log in apply_logs {
            ls.inflight.push_back(apply_log);
        }

        // Write the log entries locally.
        if let Err(e) = self.core.logs.store_logs(&logs).await {
            tracing::error!("failed to commit logs: {}", e);
            let message = e.to_string();
            for mut apply_log in ls.inflight.drain(..) {
                apply_log.respond_error(RaftError::Other(message.clone()));
            }
            self.core.set_state(RaftState::Follower);
            return;
        }
        ls.commitment.match_index(&self.core.local_id, last_index);

        // Update the last log since it's on disk now.
        self.core.shared.set_last_log(last_index, term);

        // Notify the replicators of the new logs.
        for repl in ls.repl_state.values() {
            let _ = repl.trigger_tx.try_send(());
        }
    }

    /// Applies all committed entries that haven't been applied up to the
    /// given index limit. Called from both leaders (with futures from
    /// inflight logs) and followers (with an empty map). Mirrors
    /// `processLogs`.
    async fn process_logs(&mut self, index: u64, mut futures: HashMap<u64, LogFuture>) {
        // Reject logs we've applied already.
        let last_applied = self.core.shared.last_applied();
        if index <= last_applied {
            warn!(index, "skipping application of old log");
            return;
        }

        let max_append_entries = self.core.config().max_append_entries;
        let mut batch: Vec<crate::fsm::CommitTuple> = Vec::with_capacity(max_append_entries);

        for idx in (last_applied + 1)..=index {
            // Get the log, either from the future or from our log store.
            let (log, future) = match futures.remove(&idx) {
                Some(future) => (future.log.clone(), Some(future)),
                None => {
                    let log = self
                        .core
                        .logs
                        .get_log(idx)
                        .await
                        .unwrap_or_else(|e| panic!("failed to get log {}: {}", idx, e));
                    (log, None)
                }
            };

            // Mirrors prepareLog: barrier, command and configuration logs
            // go to the FSM task (which responds to the future); no-op
            // futures respond directly.
            if log.log_type == LogType::Noop {
                if let Some(mut future) = future {
                    future.respond();
                }
                continue;
            }
            batch.push(crate::fsm::CommitTuple { log, future });
            if batch.len() >= max_append_entries {
                self.apply_batch(std::mem::take(&mut batch)).await;
                batch = Vec::with_capacity(max_append_entries);
            }
        }

        // Apply any remaining logs in the batch.
        if !batch.is_empty() {
            self.apply_batch(batch).await;
        }

        // Update the last applied index.
        self.core.shared.set_last_applied(index);
    }

    /// Sends a batch of committed logs to the FSM task, responding to the
    /// futures with shutdown if the send fails. Mirrors the `applyBatch`
    /// closure in `processLogs`.
    async fn apply_batch(&self, batch: Vec<crate::fsm::CommitTuple>) {
        let send = self.core.fsm_mutate_tx.send(FsmMutate::Commit(batch));
        tokio::pin!(send);
        tokio::select! {
            r = &mut send => {
                if let Err(e) = r {
                    if let FsmMutate::Commit(batch) = e.0 {
                        respond_batch_shutdown(batch);
                    }
                }
            }
            _ = self.core.shutdown_wait() => {
                // The FSM task exits on shutdown, which closes the channel
                // and hands the batch back to us.
                if let Err(e) = send.await {
                    if let FsmMutate::Commit(batch) = e.0 {
                        respond_batch_shutdown(batch);
                    }
                }
            }
        }
    }

    /// Sends a vote request to all peers and votes for ourself, incrementing
    /// the current term. The returned receiver yields all responses,
    /// including our own vote. Must only be called from the main task.
    /// Mirrors `electSelf`.
    async fn elect_self(&self) -> mpsc::Receiver<VoteResult> {
        let servers = &self.configurations.latest.servers;
        let (tx, rx) = mpsc::channel(servers.len().max(1));

        // Increment the term.
        let new_term = self.core.shared.current_term() + 1;
        self.core.set_current_term(new_term).await;

        // Construct the request.
        let (last_idx, last_term) = self.core.shared.last_entry();
        let req = RequestVoteRequest {
            header: self.core.rpc_header(),
            term: new_term,
            last_log_index: last_idx,
            last_log_term: last_term,
            leadership_transfer: self
                .core
                .candidate_from_leadership_transfer
                .load(Ordering::Acquire),
        };

        // For each voter, request a vote.
        for server in servers {
            if server.suffrage != ServerSuffrage::Voter {
                continue;
            }
            if server.id == self.core.local_id {
                debug!(term = new_term, "voting for self");
                // Persist a vote for ourselves.
                if let Err(e) = self
                    .core
                    .persist_vote(new_term, req.header.addr.as_bytes())
                    .await
                {
                    tracing::error!("failed to persist vote: {}", e);
                    continue;
                }
                // Include our own vote.
                let _ = tx
                    .send(VoteResult {
                        term: new_term,
                        granted: true,
                        voter_id: self.core.local_id.clone(),
                    })
                    .await;
            } else {
                debug!(term = new_term, from = server.id, "asking for vote");
                let core = Arc::clone(&self.core);
                let req = req.clone();
                let server = server.clone();
                let tx = tx.clone();
                self.core
                    .go_func(async move {
                        let result = match core
                            .trans
                            .request_vote(&server.id, &server.address, &req)
                            .await
                        {
                            Ok(resp) => VoteResult {
                                term: resp.term,
                                granted: resp.granted,
                                voter_id: server.id,
                            },
                            Err(e) => {
                                debug!(target = server.id, error = %e, "failed to make requestVote RPC");
                                VoteResult {
                                    term: req.term,
                                    granted: false,
                                    voter_id: server.id,
                                }
                            }
                        };
                        let _ = tx.send(result).await;
                    })
                    .await;
            }
        }
        rx
    }

    /// Sends a pre-vote request to all peers and pre-votes for ourself,
    /// without incrementing the current term. Mirrors `preElectSelf`.
    async fn pre_elect_self(&self) -> mpsc::Receiver<VoteResult> {
        let servers = &self.configurations.latest.servers;
        let (tx, rx) = mpsc::channel(servers.len().max(1));

        // Propose the next term without actually changing our state.
        let new_term = self.core.shared.current_term() + 1;
        let (last_idx, last_term) = self.core.shared.last_entry();
        let req = RequestPreVoteRequest {
            header: self.core.rpc_header(),
            term: new_term,
            last_log_index: last_idx,
            last_log_term: last_term,
        };

        for server in servers {
            if server.suffrage != ServerSuffrage::Voter {
                continue;
            }
            if server.id == self.core.local_id {
                debug!(term = new_term, "pre-voting for self");
                let _ = tx
                    .send(VoteResult {
                        term: new_term,
                        granted: true,
                        voter_id: self.core.local_id.clone(),
                    })
                    .await;
            } else {
                debug!(term = new_term, from = server.id, "asking for pre-vote");
                let core = Arc::clone(&self.core);
                let req = req.clone();
                let server = server.clone();
                let tx = tx.clone();
                self.core
                    .go_func(async move {
                        let result = match core
                            .trans
                            .request_pre_vote(&server.id, &server.address, &req)
                            .await
                        {
                            Ok(resp) => VoteResult {
                                term: resp.term,
                                granted: resp.granted,
                                voter_id: server.id,
                            },
                            Err(e) if e.to_string().contains("unexpected command") => {
                                // Target does not support pre-vote; count as
                                // granted so the cluster can progress.
                                VoteResult {
                                    term: req.term,
                                    granted: true,
                                    voter_id: server.id,
                                }
                            }
                            Err(e) => {
                                debug!(target = server.id, error = %e, "failed to make requestPreVote RPC");
                                VoteResult {
                                    term: req.term,
                                    granted: false,
                                    voter_id: server.id,
                                }
                            }
                        };
                        let _ = tx.send(result).await;
                    })
                    .await;
            }
        }
        rx
    }

    /// Handles an incoming RPC request. Must only be called from the main
    /// task. Mirrors `processRPC`.
    async fn process_rpc(&mut self, rpc: RPC) {
        let (command, _reader, responder) = rpc.split();
        if let Err(e) = self.core.check_rpc_header(command.header()) {
            responder.respond_error(e);
            return;
        }
        match command {
            RPCCommand::AppendEntries(req) => self.append_entries(req, responder).await,
            RPCCommand::RequestVote(req) => self.request_vote(req, responder).await,
            RPCCommand::RequestPreVote(req) => self.request_pre_vote(req, responder).await,
            RPCCommand::InstallSnapshot(req) => {
                self.install_snapshot(req, _reader, responder).await
            }
            RPCCommand::TimeoutNow(_) => {
                self.core.set_leader(String::new(), String::new());
                self.core.set_state(RaftState::Candidate);
                self.core
                    .candidate_from_leadership_transfer
                    .store(true, Ordering::Release);
                responder.respond(RPCResponse::TimeoutNow(TimeoutNowResponse {
                    header: self.core.rpc_header(),
                }));
            }
        }
    }

    /// Invoked when we get an AppendEntries RPC call. Must only be called
    /// from the main task. Mirrors `appendEntries`.
    async fn append_entries(&mut self, a: AppendEntriesRequest, responder: RPCResponder) {
        let mut resp = AppendEntriesResponse {
            header: self.core.rpc_header(),
            term: self.core.shared.current_term(),
            last_log: self.core.shared.last_index(),
            success: false,
            no_retry_backoff: false,
        };

        // Ignore an older term.
        if a.term < self.core.shared.current_term() {
            responder.respond(RPCResponse::AppendEntries(resp));
            return;
        }

        // Increase the term if we see a newer one, also transition to
        // follower if we ever get an AppendEntries call.
        if a.term > self.core.shared.current_term()
            || (self.core.shared.state() != RaftState::Follower
                && !self
                    .core
                    .candidate_from_leadership_transfer
                    .load(Ordering::Acquire))
        {
            self.core.set_state(RaftState::Follower);
            self.core.set_current_term(a.term).await;
            resp.term = a.term;
        }

        // Save the current leader.
        self.core.set_leader(a.header.addr, a.header.id);

        // Verify the last log entry.
        if a.prev_log_entry > 0 {
            let (last_idx, last_term) = self.core.shared.last_entry();
            let prev_log_term = if a.prev_log_entry == last_idx {
                last_term
            } else {
                match self.core.logs.get_log(a.prev_log_entry).await {
                    Ok(prev_log) => prev_log.term,
                    Err(e) => {
                        warn!(
                            previous_index = a.prev_log_entry,
                            last_index = last_idx,
                            error = %e,
                            "failed to get previous log"
                        );
                        resp.no_retry_backoff = true;
                        responder.respond(RPCResponse::AppendEntries(resp));
                        return;
                    }
                }
            };

            if a.prev_log_term != prev_log_term {
                warn!(
                    ours = prev_log_term,
                    remote = a.prev_log_term,
                    "previous log term mis-match"
                );
                resp.no_retry_backoff = true;
                responder.respond(RPCResponse::AppendEntries(resp));
                return;
            }
        }

        // Process any new entries.
        if !a.entries.is_empty() {
            // Delete any conflicting entries, skip any duplicates.
            let last_log_idx = self.core.shared.last_log_index();
            let mut new_entries_start = a.entries.len();
            for (i, entry) in a.entries.iter().enumerate() {
                if entry.index > last_log_idx {
                    new_entries_start = i;
                    break;
                }
                let store_entry = match self.core.logs.get_log(entry.index).await {
                    Ok(store_entry) => store_entry,
                    Err(e) => {
                        warn!(index = entry.index, error = %e, "failed to get log entry");
                        responder.respond(RPCResponse::AppendEntries(resp));
                        return;
                    }
                };
                if entry.term != store_entry.term {
                    warn!(from = entry.index, to = last_log_idx, "clearing log suffix");
                    if let Err(e) = self.core.logs.delete_range(entry.index, last_log_idx).await {
                        tracing::error!("failed to clear log suffix: {}", e);
                        responder.respond(RPCResponse::AppendEntries(resp));
                        return;
                    }
                    if entry.index <= self.configurations.latest_index {
                        let committed = self.configurations.committed.clone();
                        let committed_index = self.configurations.committed_index;
                        self.set_latest_configuration(committed, committed_index);
                    }
                    new_entries_start = i;
                    break;
                }
            }

            if new_entries_start < a.entries.len() {
                let new_entries = &a.entries[new_entries_start..];

                // Append the new entries.
                if let Err(e) = self.core.logs.store_logs(new_entries).await {
                    tracing::error!("failed to append to logs: {}", e);
                    responder.respond(RPCResponse::AppendEntries(resp));
                    return;
                }

                // Handle any new configuration changes.
                for new_entry in new_entries {
                    self.process_configuration_log_entry(new_entry);
                }

                // Update the last log.
                let last = new_entries.last().expect("non-empty slice");
                self.core.shared.set_last_log(last.index, last.term);
            }
        }

        // Update the commit index.
        if a.leader_commit_index > 0 && a.leader_commit_index > self.core.shared.commit_index() {
            let idx = a.leader_commit_index.min(self.core.shared.last_index());
            self.core.shared.set_commit_index(idx);
            if self.configurations.latest_index <= idx {
                let latest = self.configurations.latest.clone();
                let latest_index = self.configurations.latest_index;
                self.set_committed_configuration(latest, latest_index);
            }
            self.process_logs(idx, HashMap::new()).await;
        }

        // Everything went well, set success.
        resp.success = true;
        self.core.set_last_contact_now();
        responder.respond(RPCResponse::AppendEntries(resp));
    }

    /// Invoked when we get an InstallSnapshot RPC call. We must be in the
    /// follower state for this, since it means we are too far behind a
    /// leader for log replay. Must only be called from the main task.
    /// Mirrors `installSnapshot`.
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest,
        reader: Option<SnapshotReader>,
        responder: RPCResponder,
    ) {
        let mut resp = InstallSnapshotResponse {
            header: self.core.rpc_header(),
            term: self.core.shared.current_term(),
            success: false,
        };

        let Some(mut reader) = reader else {
            responder.respond_error(RaftError::Other("missing snapshot data".into()));
            return;
        };

        // Sanity check the version.
        if req.snapshot_version == 0 || req.snapshot_version > SNAPSHOT_VERSION_MAX {
            drain_reader(&mut reader);
            responder.respond_error(RaftError::Other(format!(
                "unsupported snapshot version {}",
                req.snapshot_version
            )));
            return;
        }

        // Ignore an older term.
        if req.term < self.core.shared.current_term() {
            info!(
                request_term = req.term,
                current_term = self.core.shared.current_term(),
                "ignoring installSnapshot request with older term than current term"
            );
            drain_reader(&mut reader);
            responder.respond(RPCResponse::InstallSnapshot(resp));
            return;
        }

        // Increase the term if we see a newer one.
        if req.term > self.core.shared.current_term() {
            // Ensure transition to follower.
            self.core.set_state(RaftState::Follower);
            self.core.set_current_term(req.term).await;
            resp.term = req.term;
        }

        // Save the current leader.
        self.core.set_leader(req.header.addr, req.header.id);

        // Decode the configuration in the snapshot.
        let req_configuration = match decode_configuration(&req.configuration) {
            Ok(conf) => conf,
            Err(e) => {
                tracing::error!("failed to install snapshot: {}", e);
                drain_reader(&mut reader);
                responder.respond_error(e);
                return;
            }
        };

        // Create a new snapshot.
        let mut sink = match self
            .core
            .snapshots
            .create(
                SNAPSHOT_VERSION_MAX,
                req.last_log_index,
                req.last_log_term,
                &req_configuration,
                req.configuration_index,
            )
            .await
        {
            Ok(sink) => sink,
            Err(e) => {
                tracing::error!("failed to create snapshot to install: {}", e);
                drain_reader(&mut reader);
                responder.respond_error(RaftError::Other(format!(
                    "failed to create snapshot: {}",
                    e
                )));
                return;
            }
        };

        // Spill the remote snapshot to the store.
        match copy_to_sink(&mut reader, &mut sink).await {
            Ok(n) if n != req.size => {
                let _ = sink.cancel().await;
                tracing::error!("failed to receive whole snapshot: {} / {}", n, req.size);
                responder.respond_error(RaftError::Other("short read".into()));
                return;
            }
            Ok(n) => {
                if let Err(e) = sink.close().await {
                    tracing::error!("failed to finalize snapshot: {}", e);
                    responder.respond_error(e);
                    return;
                }
                info!(bytes = n, "copied to local snapshot");
            }
            Err(e) => {
                let _ = sink.cancel().await;
                tracing::error!("failed to copy snapshot: {}", e);
                responder.respond_error(e);
                return;
            }
        }

        // Restore the snapshot into the FSM.
        let (restore_future, restore_rx) = RestoreFuture::new(sink.id());
        tokio::select! {
            r = self.core.fsm_mutate_tx.send(FsmMutate::Restore(restore_future)) => {
                if r.is_err() {
                    responder.respond_error(RaftError::RaftShutdown);
                    return;
                }
            }
            _ = self.core.shutdown_wait() => {
                responder.respond_error(RaftError::RaftShutdown);
                return;
            }
        }

        // Wait for the restore to happen.
        match restore_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("failed to restore snapshot: {}", e);
                responder.respond_error(e);
                return;
            }
            Err(_) => {
                responder.respond_error(RaftError::RaftShutdown);
                return;
            }
        }

        // Update the lastApplied so we don't replay old logs.
        self.core.shared.set_last_applied(req.last_log_index);

        // Update the last stable snapshot info.
        self.core
            .shared
            .set_last_snapshot(req.last_log_index, req.last_log_term);

        // Restore the peer set.
        self.set_latest_configuration(req_configuration.clone(), req.configuration_index);
        self.set_committed_configuration(req_configuration, req.configuration_index);

        // Compact the logs; log any error and continue.
        if let Err(e) = compact_logs(&self.core, req.last_log_index).await {
            tracing::error!("failed to compact logs: {}", e);
        }

        info!("installed remote snapshot");
        resp.success = true;
        self.core.set_last_contact_now();
        responder.respond(RPCResponse::InstallSnapshot(resp));
    }

    /// Manually consumes an external snapshot, such as restoring from a
    /// backup. The current raft configuration is used, not the one from the
    /// snapshot, and the new index is the higher of the snapshot's and the
    /// current one, plus one, leaving a hole so the snapshot gets sent to
    /// followers and new joiners. Can only run on the leader. Mirrors
    /// `restoreUserSnapshot`.
    async fn restore_user_snapshot(
        &mut self,
        ls: &mut LeaderState,
        meta: &SnapshotMeta,
        mut reader: SnapshotReader,
    ) -> Result<()> {
        // Sanity check the version.
        if meta.version == 0 || meta.version > SNAPSHOT_VERSION_MAX {
            return Err(RaftError::Other(format!(
                "unsupported snapshot version {}",
                meta.version
            )));
        }

        // We don't support snapshots while there's a config change
        // outstanding since the snapshot doesn't have a means to represent
        // this state.
        let committed_index = self.configurations.committed_index;
        let latest_index = self.configurations.latest_index;
        if committed_index != latest_index {
            return Err(RaftError::Other(format!(
                "cannot restore snapshot now, wait until the configuration entry at {} has been applied (have applied {})",
                latest_index, committed_index
            )));
        }

        // Cancel any inflight requests.
        for mut future in ls.inflight.drain(..) {
            future.respond_error(RaftError::AbortedByRestore);
        }

        // We overwrite the snapshot metadata with the current term and an
        // index that's greater than the current index, or the last index in
        // the snapshot. It's important that we leave a hole in the index so
        // we know there's nothing in the raft log there and replication will
        // fault and send the snapshot.
        let term = self.core.shared.current_term();
        let mut last_index = self.core.shared.last_index();
        if meta.index > last_index {
            last_index = meta.index;
        }
        last_index += 1;

        // Dump the snapshot. Note that we use the latest configuration, not
        // the one that came with the snapshot.
        let mut sink = self
            .core
            .snapshots
            .create(
                meta.version,
                last_index,
                term,
                &self.configurations.latest,
                latest_index,
            )
            .await
            .map_err(|e| RaftError::Other(format!("failed to create snapshot: {}", e)))?;
        match copy_to_sink(&mut reader, &mut sink).await {
            Ok(n) if n != meta.size => {
                let _ = sink.cancel().await;
                return Err(RaftError::Other(format!(
                    "failed to write snapshot, size didn't match ({} != {})",
                    n, meta.size
                )));
            }
            Ok(n) => {
                sink.close()
                    .await
                    .map_err(|e| RaftError::Other(format!("failed to close snapshot: {}", e)))?;
                info!(bytes = n, "copied to local snapshot");
            }
            Err(e) => {
                let _ = sink.cancel().await;
                return Err(RaftError::Other(format!("failed to write snapshot: {}", e)));
            }
        }

        // Restore the snapshot into the FSM. If this fails we are in a bad
        // state so we panic to take ourselves out.
        let (restore_future, restore_rx) = RestoreFuture::new(sink.id());
        tokio::select! {
            r = self.core.fsm_mutate_tx.send(FsmMutate::Restore(restore_future)) => {
                r.map_err(|_| RaftError::RaftShutdown)?;
            }
            _ = self.core.shutdown_wait() => return Err(RaftError::RaftShutdown),
        }
        match restore_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("failed to restore snapshot: {}", e),
            Err(_) => return Err(RaftError::RaftShutdown),
        }

        // We set the last log so it looks like we've stored the empty index
        // we burned. The last applied is set because we made the FSM take
        // the snapshot state, and we store the last snapshot since we
        // created a snapshot as part of this process.
        self.core.shared.set_last_log(last_index, term);
        self.core.shared.set_last_applied(last_index);
        self.core.shared.set_last_snapshot(last_index, term);

        info!(index = last_index, "restored user snapshot");
        Ok(())
    }

    /// Invoked when we get a RequestVote RPC call. Mirrors `requestVote`.
    async fn request_vote(&mut self, req: RequestVoteRequest, responder: RPCResponder) {
        let mut resp = RequestVoteResponse {
            header: self.core.rpc_header(),
            term: self.core.shared.current_term(),
            granted: false,
        };

        let candidate = req.header.addr;
        let candidate_id = req.header.id;

        // If the servers list is empty the cluster is very likely trying to
        // bootstrap; grant the vote. Otherwise reject candidates that are
        // not in the configuration.
        if !self.configurations.latest.servers.is_empty()
            && !self.configurations.latest.contains(&candidate_id)
        {
            warn!(
                from = candidate,
                "rejecting vote request since node is not in configuration"
            );
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }

        // Reject if we have an existing leader [who's not the candidate] and
        // the LeadershipTransfer flag is not set.
        let (leader_addr, _) = self.core.leader_with_id();
        if !leader_addr.is_empty() && leader_addr != candidate && !req.leadership_transfer {
            warn!(
                from = candidate,
                leader = leader_addr,
                "rejecting vote request since we have a leader"
            );
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }

        // Ignore an older term.
        if req.term < self.core.shared.current_term() {
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }

        // Increase the term if we see a newer one.
        if req.term > self.core.shared.current_term() {
            debug!("lost leadership because received a requestVote with a newer term");
            self.core.set_state(RaftState::Follower);
            self.core.set_current_term(req.term).await;
            resp.term = req.term;
        }

        // A vote request from a non-voter (e.g. a demoted node) at a higher
        // term still steps us down above, but is rejected here so the
        // cluster can make progress.
        if !self.configurations.latest.servers.is_empty()
            && !self.configurations.latest.has_vote(&candidate_id)
        {
            warn!(
                from = candidate,
                "rejecting vote request since node is not a voter"
            );
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }

        // Check if we have voted yet.
        let last_vote_term = match self.core.stable.get_u64(LAST_VOTE_TERM_KEY).await {
            Ok(term) => term.unwrap_or(0),
            Err(e) => {
                tracing::error!("failed to get last vote term: {}", e);
                responder.respond(RPCResponse::RequestVote(resp));
                return;
            }
        };
        let last_vote_cand = match self.core.stable.get(LAST_VOTE_CAND_KEY).await {
            Ok(cand) => cand,
            Err(e) => {
                tracing::error!("failed to get last vote candidate: {}", e);
                responder.respond(RPCResponse::RequestVote(resp));
                return;
            }
        };

        // Check if we've voted in this election before.
        if last_vote_term == req.term && last_vote_cand.is_some() {
            info!(term = req.term, "duplicate requestVote for same term");
            if last_vote_cand.as_deref() == Some(candidate.as_bytes()) {
                warn!(candidate, "duplicate requestVote from candidate");
                resp.granted = true;
            }
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }

        // Reject if their log is older.
        let (last_idx, last_term) = self.core.shared.last_entry();
        if last_term > req.last_log_term {
            warn!(
                candidate,
                "rejecting vote request since our last term is greater"
            );
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }
        if last_term == req.last_log_term && last_idx > req.last_log_index {
            warn!(
                candidate,
                "rejecting vote request since our last index is greater"
            );
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }

        // Persist a vote for safety.
        if let Err(e) = self.core.persist_vote(req.term, candidate.as_bytes()).await {
            tracing::error!("failed to persist vote: {}", e);
            responder.respond(RPCResponse::RequestVote(resp));
            return;
        }

        resp.granted = true;
        self.core.set_last_contact_now();
        responder.respond(RPCResponse::RequestVote(resp));
    }

    /// Invoked when we get a RequestPreVote RPC call. Mirrors
    /// `requestPreVote`.
    async fn request_pre_vote(&mut self, req: RequestPreVoteRequest, responder: RPCResponder) {
        let mut resp = RequestPreVoteResponse {
            header: self.core.rpc_header(),
            term: self.core.shared.current_term(),
            granted: false,
        };

        let candidate = req.header.addr;
        let candidate_id = req.header.id;

        // Reject candidates not in the configuration, unless the cluster is
        // bootstrapping.
        if !self.configurations.latest.servers.is_empty()
            && !self.configurations.latest.contains(&candidate_id)
        {
            warn!(
                from = candidate,
                "rejecting pre-vote request since node is not in configuration"
            );
            responder.respond(RPCResponse::RequestPreVote(resp));
            return;
        }

        // Reject if we have an existing leader who's not the candidate.
        let (leader_addr, _) = self.core.leader_with_id();
        if !leader_addr.is_empty() && leader_addr != candidate {
            warn!(
                from = candidate,
                leader = leader_addr,
                "rejecting pre-vote request since we have a leader"
            );
            responder.respond(RPCResponse::RequestPreVote(resp));
            return;
        }

        // Ignore an older term. A newer term does not change our state for
        // a pre-vote.
        if req.term < self.core.shared.current_term() {
            responder.respond(RPCResponse::RequestPreVote(resp));
            return;
        }
        if req.term > self.core.shared.current_term() {
            debug!("received a requestPreVote with a newer term, grant the pre-vote");
            resp.term = req.term;
        }

        // Reject pre-votes from non-voters.
        if !self.configurations.latest.servers.is_empty()
            && !self.configurations.latest.has_vote(&candidate_id)
        {
            warn!(
                from = candidate,
                "rejecting pre-vote request since node is not a voter"
            );
            responder.respond(RPCResponse::RequestPreVote(resp));
            return;
        }

        // Reject if their log is older.
        let (last_idx, last_term) = self.core.shared.last_entry();
        if last_term > req.last_log_term {
            warn!(
                candidate,
                "rejecting pre-vote request since our last term is greater"
            );
            responder.respond(RPCResponse::RequestPreVote(resp));
            return;
        }
        if last_term == req.last_log_term && last_idx > req.last_log_index {
            warn!(
                candidate,
                "rejecting pre-vote request since our last index is greater"
            );
            responder.respond(RPCResponse::RequestPreVote(resp));
            return;
        }

        resp.granted = true;
        responder.respond(RPCResponse::RequestPreVote(resp));
    }

    /// Fills in and answers a configurations request. Mirrors the
    /// `configurationsCh` arms of the Go loops.
    fn answer_configurations(&mut self, mut future: ConfigurationsFuture) {
        future.configurations = self.configurations.clone();
        future.respond();
    }
}

/// Receives from an optional channel; a `None` channel never yields,
/// mirroring a nil channel in a Go select.
async fn recv_optional<T>(rx: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Resets the leadership-transfer candidacy flag when dropped; mirrors the
/// Go `defer` resetting `candidateFromLeadershipTransfer` after each
/// candidacy.
struct ResetTransferFlag(Arc<RaftCore>);

impl Drop for ResetTransferFlag {
    fn drop(&mut self) {
        self.0
            .candidate_from_leadership_transfer
            .store(false, Ordering::Release);
    }
}

/// Responds to every future in a batch with the shutdown error.
fn respond_batch_shutdown(batch: Vec<crate::fsm::CommitTuple>) {
    for ct in batch {
        if let Some(mut future) = ct.future {
            future.respond_error(RaftError::RaftShutdown);
        }
    }
}

/// Resolves once the flag in the receiver is true. `watch::Ref` is not
/// `Send`, so `Receiver::wait_for` cannot be used directly as a
/// `tokio::select!` arm in spawned tasks; this helper avoids holding the
/// borrow across an await.
pub(crate) async fn wait_flag(rx: &watch::Receiver<bool>) {
    let mut rx = rx.clone();
    if *rx.borrow_and_update() {
        return;
    }
    let _ = rx.changed().await;
}

/// Drives a leadership transfer: catches the target up to the leader's
/// log and then sends a `TimeoutNow` RPC. Mirrors `Raft.leadershipTransfer`
/// in the Go implementation. Returns `Ok(())` if the transfer completed
/// (the leader may or may not still be leader; the caller will discover
/// that via state changes), `Err` on transport failure or timeout.
pub(crate) async fn leadership_transfer(
    core: Arc<RaftCore>,
    id: ServerID,
    address: ServerAddress,
    repl: Arc<FollowerReplication>,
    step_down_tx: mpsc::Sender<()>,
    timeout: std::time::Duration,
) -> Result<()> {
    // Wait for the target to be caught up. We poll the replication state
    // each `commit_timeout`, sending a trigger to nudge replication.
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if repl.next_index.load(Ordering::Acquire) > core.shared.last_log_index() {
            break;
        }
        // Nudge the replication loop so it tries to catch the target up.
        let _ = repl.trigger_tx.try_send(());
        if tokio::time::Instant::now() >= deadline {
            return Err(RaftError::Other(format!(
                "leadership transfer timeout: target {} did not catch up",
                id
            )));
        }
        let commit_timeout = core.config().commit_timeout;
        tokio::select! {
            _ = tokio::time::sleep(commit_timeout) => continue,
            _ = step_down_tx.closed() => {
                return Err(RaftError::RaftShutdown);
            }
        }
    }

    // Send the TimeoutNow RPC. The target server uses this to step up
    // immediately, bypassing its election timeout.
    let req = crate::transport::TimeoutNowRequest {
        header: core.rpc_header(),
    };
    match core.trans.timeout_now(&id, &address, &req).await {
        Ok(_) => Ok(()),
        Err(e) => Err(RaftError::Other(format!(
            "failed to make TimeoutNow RPC to {}: {}",
            id, e
        ))),
    }
}
