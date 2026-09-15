//! The public Raft handle: construction (`new_raft`, `bootstrap_cluster`,
//! `has_existing_state`) and the client API. Mirrors api.go of the Go
//! implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::config::Config;
use crate::configuration::{
    check_configuration, encode_configuration, Configuration, ConfigurationChangeCommand,
    ConfigurationChangeRequest, ServerAddress, ServerID,
};
use crate::fsm::{run_fsm, FSM};
use crate::future::{
    ApplyFuture, BootstrapFuture, ConfigurationChangeFuture, ConfigurationFuture,
    ConfigurationValue, Configurations, IndexFuture, LeadershipTransferFutureState, LogFuture,
    ShutdownFuture, StatusFuture, UserRestoreFutureState, UserSnapshotFutureState, VerifyFuture,
    VerifyState,
};
use crate::log::{Log, LogStore, LogType};
use crate::raft::{MainLoop, RaftCore};
use crate::snapshot::SnapshotStore;
use crate::stable::{StableStore, CURRENT_TERM_KEY};
use crate::state::{RaftSharedState, RaftState};
use crate::transport::Transport;
use crate::{RaftError, Result};

/// A raft node. Cheap to clone; all clones share the same underlying node.
/// Mirrors the Go `*Raft`.
#[derive(Clone)]
pub struct Raft {
    pub(crate) core: Arc<RaftCore>,
}

impl Raft {
    /// The current state of this node. Mirrors `State`.
    pub fn state(&self) -> RaftState {
        self.core.shared.state()
    }

    /// The current leader's address, or empty if unknown. Mirrors `Leader`.
    pub fn leader(&self) -> ServerAddress {
        self.core.leader_with_id().0
    }

    /// The current leader's address and ID, or empty strings if unknown.
    /// Mirrors `LeaderWithID`.
    pub fn leader_with_id(&self) -> (ServerAddress, ServerID) {
        self.core.leader_with_id()
    }

    /// The current term. Mirrors `CurrentTerm`.
    pub fn current_term(&self) -> u64 {
        self.core.shared.current_term()
    }

    /// The last index in stable storage (log or snapshot). Mirrors
    /// `LastIndex`.
    pub fn last_index(&self) -> u64 {
        self.core.shared.last_index()
    }

    /// The highest log entry known to be committed. Mirrors `CommitIndex`.
    pub fn commit_index(&self) -> u64 {
        self.core.shared.commit_index()
    }

    /// The highest log entry applied to the FSM. Mirrors `AppliedIndex`.
    pub fn applied_index(&self) -> u64 {
        self.core.shared.last_applied()
    }

    /// The last time we had contact from the leader (or won an election).
    /// Mirrors `LastContact`; `None` if there was never any contact.
    pub fn last_contact(&self) -> Option<Instant> {
        self.core.shared.last_contact_time()
    }

    /// The local server ID and address of this node.
    pub fn local_id(&self) -> ServerID {
        self.core.local_id.clone()
    }

    pub fn local_addr(&self) -> ServerAddress {
        self.core.local_addr.clone()
    }

    /// A receiver notified of leadership changes: `true` on gaining
    /// leadership and `false` on losing it. Mirrors `LeaderCh`; only the
    /// latest value is retained, as in Go.
    pub fn leader_ch(&self) -> watch::Receiver<bool> {
        self.core.leader_tx.subscribe()
    }

    /// Registers a new observer on this raft. The returned
    /// [`ObserverChannel`] delivers [`Observation`] events. Dropping the
    /// channel (or calling its `deregister` method) removes the observer
    /// from the registry. Mirrors `Raft.RegisterObserver`.
    pub fn register_observer(
        &self,
        observer: crate::observer::Observer,
    ) -> crate::observer::ObserverChannel {
        let core = Arc::clone(&self.core);
        let weak = Arc::downgrade(&core);
        // Build the channel and hand it to the caller; the registration
        // happens after we have the channel so the caller can immediately
        // start consuming events without racing the dispatcher.
        let (_config, channel) = crate::observer::Observer::new(
            observer.channel_size,
            observer.blocking,
            observer.filter,
        );
        core.register_observer(Arc::clone(&channel.handle), weak);
        channel
    }

    /// Basic operational statistics. Mirrors `Stats`.
    pub fn stats(&self) -> HashMap<String, String> {
        let (last_log_index, last_log_term) = self.core.shared.last_log();
        let (last_snap_index, last_snap_term) = self.core.shared.last_snapshot();
        let mut s = HashMap::new();
        let to_string = |v: u64| v.to_string();
        s.insert("state".into(), self.state().to_string());
        s.insert("term".into(), to_string(self.current_term()));
        s.insert("last_log_index".into(), to_string(last_log_index));
        s.insert("last_log_term".into(), to_string(last_log_term));
        s.insert("commit_index".into(), to_string(self.commit_index()));
        s.insert("applied_index".into(), to_string(self.applied_index()));
        s.insert("last_snapshot_index".into(), to_string(last_snap_index));
        s.insert("last_snapshot_term".into(), to_string(last_snap_term));
        s.insert("protocol_version".into(), "3".into());
        s.insert(
            "latest_configuration_index".into(),
            to_string(self.core.latest_configuration().1),
        );
        s.insert(
            "latest_configuration".into(),
            format!("{:?}", self.core.latest_configuration().0.servers),
        );
        s.insert("num_peers".into(), {
            let peers = self
                .core
                .latest_configuration()
                .0
                .servers
                .iter()
                .filter(|srv| srv.id != self.core.local_id)
                .count();
            to_string(peers as u64)
        });
        s
    }

    /// Applies a command to the FSM in a highly consistent manner. Returns a
    /// future that can be used to wait on the application. The timeout limits
    /// how long we wait for the command to be enqueued (zero means forever).
    /// Must be run on the leader or it will fail with
    /// [`RaftError::NotLeader`]. Mirrors `Apply`.
    pub async fn apply(&self, cmd: &[u8], timeout: Duration) -> ApplyFuture {
        self.apply_log(
            Log {
                data: cmd.to_vec(),
                ..Default::default()
            },
            timeout,
        )
        .await
    }

    /// Performs [`Raft::apply`] but takes a [`Log`] directly. Only `data`
    /// and `extensions` are taken from the submitted log. Mirrors `ApplyLog`.
    pub async fn apply_log(&self, log: Log, timeout: Duration) -> ApplyFuture {
        let (log_future, rx) = LogFuture::new(Log {
            log_type: LogType::Command,
            data: log.data,
            extensions: log.extensions,
            ..Default::default()
        });
        match self
            .enqueue(self.core.apply_tx.send(log_future), timeout)
            .await
        {
            Ok(()) => ApplyFuture::from_receiver(rx),
            Err(e) => ApplyFuture::from_error(e),
        }
    }

    /// Issues a command that blocks until all preceding operations have been
    /// applied to the FSM. Mirrors `Barrier`.
    pub async fn barrier(&self, timeout: Duration) -> IndexFuture {
        let (log_future, rx) = LogFuture::new(Log {
            log_type: LogType::Barrier,
            ..Default::default()
        });
        match self
            .enqueue(self.core.apply_tx.send(log_future), timeout)
            .await
        {
            Ok(()) => IndexFuture::from_receiver(rx),
            Err(e) => IndexFuture::from_error(e),
        }
    }

    /// Ensures this peer is still the leader, to prevent stale reads.
    /// Mirrors `VerifyLeader`.
    pub async fn verify_leader(&self) -> VerifyFuture {
        let (state, future) = VerifyState::new();
        match self.core.verify_tx.send(state) {
            Ok(()) => future,
            Err(_) => StatusFuture::from_error(RaftError::RaftShutdown),
        }
    }

    /// Returns the latest configuration in use. It may not yet be committed.
    /// Mirrors `GetConfiguration` (but also reports the configuration index).
    pub fn get_configuration(&self) -> ConfigurationFuture {
        let (configuration, index) = self.core.latest_configuration();
        ConfigurationFuture::ready(ConfigurationValue {
            configuration,
            index,
        })
    }

    /// Adds a voting server to the cluster configuration. `prev_index`, if
    /// nonzero, only allows the change if the previous configuration index
    /// matches. Must be run on the leader. Mirrors `AddVoter`.
    pub async fn add_voter(
        &self,
        id: ServerID,
        address: ServerAddress,
        prev_index: u64,
        timeout: Duration,
    ) -> IndexFuture {
        self.request_config_change(
            ConfigurationChangeRequest {
                command: ConfigurationChangeCommand::AddVoter,
                server_id: id,
                server_address: address,
                prev_index,
            },
            timeout,
        )
        .await
    }

    /// Adds a nonvoting server to the cluster configuration. Mirrors
    /// `AddNonvoter`.
    pub async fn add_nonvoter(
        &self,
        id: ServerID,
        address: ServerAddress,
        prev_index: u64,
        timeout: Duration,
    ) -> IndexFuture {
        self.request_config_change(
            ConfigurationChangeRequest {
                command: ConfigurationChangeCommand::AddNonvoter,
                server_id: id,
                server_address: address,
                prev_index,
            },
            timeout,
        )
        .await
    }

    /// Removes a server from the cluster configuration. Mirrors
    /// `RemoveServer`.
    pub async fn remove_server(
        &self,
        id: ServerID,
        prev_index: u64,
        timeout: Duration,
    ) -> IndexFuture {
        self.request_config_change(
            ConfigurationChangeRequest {
                command: ConfigurationChangeCommand::RemoveServer,
                server_id: id,
                server_address: String::new(),
                prev_index,
            },
            timeout,
        )
        .await
    }

    /// Demotes a voter to a nonvoter. Mirrors `DemoteVoter`.
    pub async fn demote_voter(
        &self,
        id: ServerID,
        prev_index: u64,
        timeout: Duration,
    ) -> IndexFuture {
        self.request_config_change(
            ConfigurationChangeRequest {
                command: ConfigurationChangeCommand::DemoteVoter,
                server_id: id,
                server_address: String::new(),
                prev_index,
            },
            timeout,
        )
        .await
    }

    /// Sends a configuration change request to the main loop. Mirrors
    /// `requestConfigChange`.
    async fn request_config_change(
        &self,
        req: ConfigurationChangeRequest,
        timeout: Duration,
    ) -> IndexFuture {
        let (future, rx) = ConfigurationChangeFuture::new(req, Log::default());
        match self
            .enqueue(self.core.config_change_tx.send(future), timeout)
            .await
        {
            Ok(()) => rx,
            Err(e) => IndexFuture::from_error(e),
        }
    }

    /// Attempts a live bootstrap of the cluster. Only makes sense on a
    /// fresh follower; a bootstrapped cluster refuses with
    /// [`RaftError::CantBootstrap`]. Mirrors `BootstrapCluster` (the member
    /// function).
    pub async fn bootstrap_cluster(&self, configuration: Configuration) -> BootstrapFuture {
        let (future, rx) = crate::future::BootstrapFutureState::new(configuration);
        match self.core.bootstrap_tx.send(future).await {
            Ok(()) => rx,
            Err(_) => StatusFuture::from_error(RaftError::RaftShutdown),
        }
    }

    /// Requests a leadership transfer. Implemented with the membership
    /// phase; currently returns an error future. Mirrors
    /// `LeadershipTransfer`.
    pub async fn leadership_transfer(&self) -> StatusFuture {
        self.initiate_leadership_transfer(None, None).await
    }

    /// Requests a leadership transfer to the given server. Mirrors
    /// `LeadershipTransferToServer`.
    pub async fn leadership_transfer_to_server(
        &self,
        id: ServerID,
        address: ServerAddress,
    ) -> StatusFuture {
        self.initiate_leadership_transfer(Some(id), Some(address))
            .await
    }

    /// Mirrors `initiateLeadershipTransfer`.
    async fn initiate_leadership_transfer(
        &self,
        id: Option<ServerID>,
        address: Option<ServerAddress>,
    ) -> StatusFuture {
        if id.as_ref() == Some(&self.core.local_id) {
            return StatusFuture::from_error(RaftError::Other(
                "cannot transfer leadership to itself".into(),
            ));
        }
        let (future, rx) = LeadershipTransferFutureState::new(id, address);
        // Non-blocking send like the Go select with a default case.
        match self.core.leadership_transfer_tx.try_send(future) {
            Ok(()) => rx,
            Err(mpsc::error::TrySendError::Full(_)) => {
                StatusFuture::from_error(RaftError::EnqueueTimeout)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                StatusFuture::from_error(RaftError::RaftShutdown)
            }
        }
    }

    /// Takes a snapshot of the FSM and returns a future that can be used to
    /// block until complete. The full implementation lands with the snapshot
    /// phase. Mirrors `Snapshot`.
    pub async fn snapshot(&self) -> crate::future::SnapshotFuture {
        let (future, rx) = UserSnapshotFutureState::new();
        match self.core.user_snapshot_tx.send(future).await {
            Ok(()) => rx,
            Err(mpsc::error::SendError(mut future)) => {
                future.respond(Err(RaftError::RaftShutdown));
                rx
            }
        }
    }

    /// Manually forces raft to consume an external snapshot, such as when
    /// restoring from a backup. The current raft configuration is used, not
    /// the one from the snapshot, and the index is forced to the max of the
    /// snapshot's and the current index, plus one, leaving a hole in the
    /// log. Must be run on the leader, and blocks until the restore is
    /// complete and a follow-up no-op has been applied (which shows the
    /// snapshot also reached the followers). Mirrors `Restore`.
    ///
    /// WARNING! This involves a potentially dangerous period where the
    /// leader commits ahead of its followers; only use it for disaster
    /// recovery into a fresh cluster.
    pub async fn restore(
        &self,
        meta: crate::snapshot::SnapshotMeta,
        reader: crate::snapshot::SnapshotReader,
        timeout: Duration,
    ) -> Result<()> {
        // Perform the restore.
        let (future, rx) = UserRestoreFutureState::new(meta, reader);
        self.enqueue(self.core.user_restore_tx.send(future), timeout)
            .await?;
        rx.wait().await?;

        // Apply a no-op log entry. Waiting for this lets us wait until the
        // followers have gotten the restore and replicated at least this
        // new entry, which shows that we've also faulted and installed the
        // snapshot with the contents of the restore.
        let (noop, noop_rx) = LogFuture::new(Log {
            log_type: LogType::Noop,
            ..Default::default()
        });
        self.enqueue(self.core.apply_tx.send(noop), timeout).await?;
        IndexFuture::from_receiver(noop_rx).wait().await?;
        Ok(())
    }

    /// Shuts down this node: stops the main loop and all background tasks,
    /// fails pending futures, and (once awaited) closes the transport.
    /// Mirrors `Shutdown`.
    pub fn shutdown(&self) -> ShutdownFuture {
        self.core.shutdown()
    }

    /// Sends a value to a main-loop channel, bounded by the timeout (zero
    /// means no timeout) and the shutdown signal. Mirrors the timer/shutdown
    /// selects in the Go enqueue helpers.
    async fn enqueue<T>(
        &self,
        send: impl std::future::Future<Output = std::result::Result<(), mpsc::error::SendError<T>>>,
        timeout: Duration,
    ) -> Result<()> {
        tokio::select! {
            _ = self.core.shutdown_wait() => Err(RaftError::RaftShutdown),
            r = send => r.map_err(|_| RaftError::RaftShutdown),
            _ = async {
                if timeout.is_zero() {
                    std::future::pending::<()>().await;
                } else {
                    tokio::time::sleep(timeout).await;
                }
            } => Err(RaftError::EnqueueTimeout),
        }
    }
}

/// Constructs a new Raft node, restoring any persisted state (current term,
/// log, configurations) and starting the background tasks. Mirrors
/// `NewRaft`. Snapshot restore on startup lands with the snapshot phase.
pub async fn new_raft(
    conf: Config,
    fsm: Arc<dyn FSM>,
    logs: Arc<dyn LogStore>,
    stable: Arc<dyn StableStore>,
    snaps: Arc<dyn SnapshotStore>,
    trans: Arc<dyn Transport>,
) -> Result<Raft> {
    // Validate the configuration.
    conf.validate()?;

    // Try to restore the current term.
    let current_term = stable
        .get_u64(CURRENT_TERM_KEY)
        .await
        .map_err(|e| RaftError::Other(format!("failed to load current term: {}", e)))?
        .unwrap_or(0);

    // Read the index of the last log entry.
    let last_index = logs
        .last_index()
        .await
        .map_err(|e| RaftError::Other(format!("failed to find last log: {}", e)))?;

    // Get the last log entry.
    let mut last_log = Log::default();
    if last_index > 0 {
        last_log = logs.get_log(last_index).await.map_err(|e| {
            RaftError::Other(format!(
                "failed to get last log at index {}: {}",
                last_index, e
            ))
        })?;
    }

    let local_addr = trans.local_addr();
    let local_id = conf.local_id.clone();

    // Create the channels; senders go to the core, receivers to the main
    // loop and the FSM/snapshot tasks. Capacities mirror the Go
    // implementation (applyCh buffered as with BatchApplyCh, fsmMutateCh
    // 128, verifyCh 64, configurationsCh 8, leadershipTransferCh 1).
    let (apply_tx, apply_rx) = mpsc::channel(conf.max_append_entries.max(1));
    let (verify_tx, verify_rx) = mpsc::unbounded_channel();
    let (config_change_tx, config_change_rx) = mpsc::channel(1);
    let (configurations_tx, configurations_rx) = mpsc::channel(8);
    let (bootstrap_tx, bootstrap_rx) = mpsc::channel(1);
    let (leadership_transfer_tx, leadership_transfer_rx) = mpsc::channel(1);
    let (user_snapshot_tx, user_snapshot_rx) = mpsc::channel(1);
    let (user_restore_tx, user_restore_rx) = mpsc::channel(1);
    let (fsm_mutate_tx, fsm_mutate_rx) = mpsc::channel(128);
    let (fsm_snapshot_tx, fsm_snapshot_rx) = mpsc::channel(1);
    let (leader_notify_tx, leader_notify_rx) = mpsc::channel(1);
    let (follower_notify_tx, follower_notify_rx) = mpsc::channel(1);
    let (leader_tx, leader_rx) = watch::channel(false);
    let (shutdown_tx, _) = watch::channel(false);
    let rpc_rx = trans.consumer();

    let core = Arc::new(RaftCore {
        conf: RwLock::new(conf.clone()),
        observers: parking_lot::RwLock::new(Vec::new()),
        shared: RaftSharedState::new(),
        fsm: Arc::clone(&fsm),
        logs: Arc::clone(&logs),
        stable: Arc::clone(&stable),
        snapshots: Arc::clone(&snaps),
        trans: Arc::clone(&trans),
        local_id,
        local_addr,
        leader: RwLock::new((String::new(), String::new())),
        leader_tx,
        leader_rx,
        candidate_from_leadership_transfer: std::sync::atomic::AtomicBool::new(false),
        pre_vote_disabled: conf.pre_vote_disabled,
        latest_configuration: RwLock::new((Configuration::default(), 0)),
        shutdown_tx,
        shutdown_initiated: Mutex::new(false),
        tasks: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
        apply_tx,
        verify_tx,
        config_change_tx,
        configurations_tx,
        bootstrap_tx,
        leadership_transfer_tx,
        user_snapshot_tx,
        user_restore_tx,
        fsm_mutate_tx,
        fsm_snapshot_tx,
        leader_notify_tx,
        follower_notify_tx,
    });

    let mut main_loop = MainLoop {
        core: Arc::clone(&core),
        configurations: Configurations::default(),
        rpc_rx,
        apply_rx,
        verify_rx,
        config_change_rx,
        configurations_rx,
        bootstrap_rx,
        leadership_transfer_rx,
        user_restore_rx,
        leader_notify_rx,
        follower_notify_rx,
    };

    // Initialize as a follower.
    core.shared.set_state(RaftState::Follower);

    // Restore the current term and the last log.
    core.set_current_term(current_term).await;
    core.shared.set_last_log(last_log.index, last_log.term);

    // Attempt to restore a snapshot if there are any. Mirrors
    // `restoreSnapshot`: try the snapshots newest to oldest, and fail only
    // if snapshots exist but none can be restored.
    {
        let snapshots = snaps.list().await?;
        let mut restored_any = false;
        for snapshot in &snapshots {
            // Mirrors tryRestoreSingleSnapshot: with
            // no_snapshot_restore_on_start the FSM restore is skipped but
            // the snapshot state is still adopted.
            let mut success = true;
            if !conf.no_snapshot_restore_on_start {
                match snaps.open(&snapshot.id).await {
                    Ok((_, source)) => {
                        if let Err(e) = fsm.restore(source).await {
                            tracing::error!(id = snapshot.id, error = %e, "failed to restore snapshot");
                            success = false;
                        }
                    }
                    Err(e) => {
                        tracing::error!(id = snapshot.id, error = %e, "failed to open snapshot");
                        success = false;
                    }
                }
            }
            if !success {
                continue;
            }

            // Update the lastApplied so we don't replay old logs.
            core.shared.set_last_applied(snapshot.index);

            // Update the last stable snapshot info.
            core.shared.set_last_snapshot(snapshot.index, snapshot.term);

            // Update the configuration.
            main_loop.set_committed_configuration(
                snapshot.configuration.clone(),
                snapshot.configuration_index,
            );
            main_loop.set_latest_configuration(
                snapshot.configuration.clone(),
                snapshot.configuration_index,
            );
            restored_any = true;
            break;
        }
        if !snapshots.is_empty() && !restored_any {
            return Err(RaftError::Other(
                "failed to load any existing snapshots".into(),
            ));
        }
    }

    // Scan through the log for any configuration change entries.
    let snapshot_index = core.shared.last_snapshot_index();
    let last_applied_index = core.shared.last_applied();
    for index in (snapshot_index.max(last_applied_index) + 1)..=last_log.index {
        let entry = logs
            .get_log(index)
            .await
            .unwrap_or_else(|e| panic!("failed to get log {}: {}", index, e));
        main_loop.process_configuration_log_entry(&entry);
    }
    tracing::info!(
        index = main_loop.configurations.latest_index,
        servers = ?main_loop.configurations.latest.servers,
        "initial configuration"
    );

    // Setup a heartbeat fast-path to avoid head-of-line blocking where
    // possible.
    {
        let core = Arc::clone(&core);
        trans.set_heartbeat_handler(Box::new(move |rpc| {
            let core = Arc::clone(&core);
            tokio::spawn(RaftCore::process_heartbeat(core, rpc));
        }));
    }

    // Start the background work.
    let raft = Raft {
        core: Arc::clone(&core),
    };
    core.go_func(main_loop.run()).await;
    core.go_func(run_fsm(
        fsm,
        Arc::clone(&snaps),
        fsm_mutate_rx,
        fsm_snapshot_rx,
        core.shutdown_rx(),
    ))
    .await;
    core.go_func(crate::snapshot::run_snapshots(
        Arc::clone(&core),
        user_snapshot_rx,
        core.shutdown_rx(),
    ))
    .await;
    Ok(raft)
}

impl RaftCore {
    pub(crate) fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }
}

/// Initializes a server's storage with the given cluster configuration. Only
/// callable at the beginning of time for the cluster, with an identical
/// configuration listing all Voter servers on every participant. Mirrors
/// `BootstrapCluster` (protocol v3 only, hence no transport parameter).
pub async fn bootstrap_cluster(
    conf: &Config,
    logs: Arc<dyn LogStore>,
    stable: Arc<dyn StableStore>,
    snaps: Arc<dyn SnapshotStore>,
    configuration: Configuration,
) -> Result<()> {
    // Validate the Raft server config.
    conf.validate()?;

    // Sanity check the Raft peer configuration.
    check_configuration(&configuration)?;

    // Make sure the cluster is in a clean state.
    if has_existing_state(logs.as_ref(), stable.as_ref(), snaps.as_ref()).await? {
        return Err(RaftError::CantBootstrap);
    }

    // Set current term to 1.
    stable
        .set_u64(CURRENT_TERM_KEY, 1)
        .await
        .map_err(|e| RaftError::Other(format!("failed to save current term: {}", e)))?;

    // Append the configuration entry to the log.
    let entry = Log {
        index: 1,
        term: 1,
        log_type: LogType::Configuration,
        data: encode_configuration(&configuration)?,
        ..Default::default()
    };
    logs.store_log(&entry).await.map_err(|e| {
        RaftError::Other(format!(
            "failed to append configuration entry to log: {}",
            e
        ))
    })?;

    Ok(())
}

/// Returns true if the server has any existing state (current term, log
/// entries, or snapshots). Mirrors `HasExistingState`.
pub async fn has_existing_state(
    logs: &dyn LogStore,
    stable: &dyn StableStore,
    snaps: &dyn SnapshotStore,
) -> Result<bool> {
    // Make sure we don't have a current term.
    if let Some(current_term) = stable
        .get_u64(CURRENT_TERM_KEY)
        .await
        .map_err(|e| RaftError::Other(format!("failed to read current term: {}", e)))?
    {
        if current_term > 0 {
            return Ok(true);
        }
    }

    // Make sure we have an empty log.
    let last_index = logs
        .last_index()
        .await
        .map_err(|e| RaftError::Other(format!("failed to get last log index: {}", e)))?;
    if last_index > 0 {
        return Ok(true);
    }

    // Make sure we have no snapshots.
    let snapshots = snaps
        .list()
        .await
        .map_err(|e| RaftError::Other(format!("failed to list snapshots: {}", e)))?;
    if !snapshots.is_empty() {
        return Ok(true);
    }

    Ok(false)
}
