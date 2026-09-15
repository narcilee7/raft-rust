//! Futures returned by the raft API, mirroring future.go of the Go
//! implementation.
//!
//! The Go `deferError` pattern (a buffered channel plus a `responded` guard)
//! maps to a `oneshot` channel per future: the raft core holds the sender
//! (wrapped in a [`FutureResponder`] that responds at most once) and the
//! client holds the receiver (the public future types). Awaiting a future
//! whose sender was dropped without responding yields
//! [`RaftError::RaftShutdown`], mirroring the Go `ShutdownCh` select.

// Several internal future types are only exercised by the raft main loop,
// which lands in a later phase.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch};

use crate::configuration::{Configuration, ConfigurationChangeRequest, ServerAddress, ServerID};
use crate::fsm::{ApplyResponse, FSMSnapshot};
use crate::log::Log;
use crate::snapshot::{SnapshotMeta, SnapshotReader};
use crate::transport::{AppendEntriesRequest, AppendEntriesResponse, Transport};
use crate::{RaftError, Result};

/// Common behavior of all futures: awaiting the error status of the
/// operation. Mirrors the Go `Future` interface (`Error()`).
pub trait RaftFuture: Send + Sized {
    /// Blocks until the future resolves and returns its error status. This
    /// consumes the future; Go allows repeated calls, but in practice callers
    /// check the result once.
    fn error(self) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Maps a dropped sender to the shutdown error, mirroring the Go
/// `<-d.ShutdownCh` branch of `deferError.Error`.
async fn await_oneshot<T>(rx: oneshot::Receiver<Result<T>>) -> Result<T> {
    match rx.await {
        Ok(res) => res,
        Err(_) => Err(RaftError::RaftShutdown),
    }
}

/// Sender half of a future, held by the raft core. Responds at most once,
/// mirroring the `responded` guard of the Go `deferError.respond`.
pub(crate) struct FutureResponder<T> {
    tx: Option<oneshot::Sender<T>>,
}

impl<T> FutureResponder<T> {
    fn new() -> (Self, oneshot::Receiver<T>) {
        let (tx, rx) = oneshot::channel();
        (FutureResponder { tx: Some(tx) }, rx)
    }

    /// Delivers the result to the future. Calls beyond the first are
    /// silently ignored, as in Go.
    pub(crate) fn respond(&mut self, value: T) {
        if let Some(tx) = self.tx.take() {
            // A send error just means the client dropped the future.
            let _ = tx.send(value);
        }
    }
}

/// The outcome of a committed log entry: its index and the FSM response.
/// Combines what Go exposes as `IndexFuture.Index` and
/// `ApplyFuture.Response`.
#[derive(Debug)]
pub struct ApplyOutcome {
    /// Index of the newly applied log entry.
    pub index: u64,
    /// The FSM response for `LogType::Command` entries, if any. Note that an
    /// FSM-level error is reported here (boxed), not via the future's error
    /// status, matching the Go implementation.
    pub response: Option<ApplyResponse>,
}

/// Used to apply a log entry and wait until it is committed and applied.
/// Internal to a single server; mirrors the Go `logFuture`.
pub(crate) struct LogFuture {
    pub(crate) log: Log,
    /// Response slot filled by the FSM task before responding.
    pub(crate) response: Option<ApplyResponse>,
    /// When the log was dispatched on the leader.
    pub(crate) dispatch: Instant,
    responder: FutureResponder<Result<ApplyOutcome>>,
}

impl LogFuture {
    pub(crate) fn new(log: Log) -> (Self, oneshot::Receiver<Result<ApplyOutcome>>) {
        let (responder, rx) = FutureResponder::new();
        (
            LogFuture {
                log,
                response: None,
                dispatch: Instant::now(),
                responder,
            },
            rx,
        )
    }

    /// Index of the log entry, valid once dispatched. Mirrors
    /// `logFuture.Index`.
    #[allow(dead_code)]
    pub(crate) fn index(&self) -> u64 {
        self.log.index
    }

    /// Responds successfully, delivering the index and any FSM response set
    /// on the response slot.
    pub(crate) fn respond(&mut self) {
        let outcome = ApplyOutcome {
            index: self.log.index,
            response: self.response.take(),
        };
        self.responder.respond(Ok(outcome));
    }

    /// Responds with an error.
    pub(crate) fn respond_error(&mut self, err: RaftError) {
        self.responder.respond(Err(err));
    }
}

/// Future returned by `Raft::apply` / `Raft::apply_log`, carrying the FSM
/// response. Mirrors the Go `ApplyFuture` interface.
pub struct ApplyFuture {
    rx: oneshot::Receiver<Result<ApplyOutcome>>,
}

impl ApplyFuture {
    pub(crate) fn from_receiver(rx: oneshot::Receiver<Result<ApplyOutcome>>) -> Self {
        ApplyFuture { rx }
    }

    /// A future that resolves immediately with the given error, used when an
    /// operation cannot even be enqueued (mirrors the Go `errorFuture`).
    pub(crate) fn from_error(err: RaftError) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(err));
        ApplyFuture { rx }
    }

    /// Blocks until the log is committed and applied, returning its index
    /// and the FSM response.
    pub async fn wait(self) -> Result<ApplyOutcome> {
        await_oneshot(self.rx).await
    }
}

impl RaftFuture for ApplyFuture {
    async fn error(self) -> Result<()> {
        self.wait().await.map(|_| ())
    }
}

/// Future for operations that create a log entry but yield no FSM response
/// (barriers, configuration changes). Mirrors the Go `IndexFuture`
/// interface.
pub struct IndexFuture {
    rx: oneshot::Receiver<Result<ApplyOutcome>>,
}

impl IndexFuture {
    pub(crate) fn from_receiver(rx: oneshot::Receiver<Result<ApplyOutcome>>) -> Self {
        IndexFuture { rx }
    }

    /// A future that resolves immediately with the given error.
    pub(crate) fn from_error(err: RaftError) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(err));
        IndexFuture { rx }
    }

    /// Blocks until the log is committed, returning its index.
    pub async fn wait(self) -> Result<u64> {
        await_oneshot(self.rx).await.map(|outcome| outcome.index)
    }
}

impl RaftFuture for IndexFuture {
    async fn error(self) -> Result<()> {
        self.wait().await.map(|_| ())
    }
}

/// A configuration change request appended to the log by the leader loop.
/// Mirrors the Go `configurationChangeFuture`; internal to a single server.
pub(crate) struct ConfigurationChangeFuture {
    pub(crate) log_future: LogFuture,
    pub(crate) req: ConfigurationChangeRequest,
}

impl ConfigurationChangeFuture {
    pub(crate) fn new(req: ConfigurationChangeRequest, log: Log) -> (Self, IndexFuture) {
        let (log_future, rx) = LogFuture::new(log);
        (
            ConfigurationChangeFuture { log_future, req },
            IndexFuture::from_receiver(rx),
        )
    }

    /// Responds with an error. Used to reject the change when this server is
    /// not the leader.
    pub(crate) fn respond_error(&mut self, err: RaftError) {
        self.log_future.respond_error(err);
    }
}

/// A future that only carries an error status. Mirrors the plain Go
/// `Future` returned by operations such as bootstrap and restore.
pub struct StatusFuture {
    rx: oneshot::Receiver<Result<()>>,
}

impl StatusFuture {
    pub(crate) fn from_receiver(rx: oneshot::Receiver<Result<()>>) -> Self {
        StatusFuture { rx }
    }

    /// A future that resolves immediately with the given error.
    pub(crate) fn from_error(err: RaftError) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(err));
        StatusFuture { rx }
    }

    /// Blocks until the operation completes.
    pub async fn wait(self) -> Result<()> {
        await_oneshot(self.rx).await
    }
}

impl RaftFuture for StatusFuture {
    async fn error(self) -> Result<()> {
        self.wait().await
    }
}

/// Future returned by `Raft::bootstrap_cluster`. Mirrors the Go
/// `bootstrapFuture`; the public half only reports an error status.
pub type BootstrapFuture = StatusFuture;

/// Future returned by `Raft::leadership_transfer` and
/// `Raft::leadership_transfer_to_server`.
pub type LeadershipTransferFuture = StatusFuture;

/// Future returned by `Raft::verify_leader`.
pub type VerifyFuture = StatusFuture;

/// Future returned by `Raft::restore` (user-triggered snapshot restore).
pub type UserRestoreFuture = StatusFuture;

/// Used to attempt a live bootstrap of the cluster. Mirrors the Go
/// `bootstrapFuture`; internal to a single server.
pub(crate) struct BootstrapFutureState {
    /// The proposed bootstrap configuration to apply.
    pub(crate) configuration: Configuration,
    responder: FutureResponder<Result<()>>,
}

impl BootstrapFutureState {
    pub(crate) fn new(configuration: Configuration) -> (Self, BootstrapFuture) {
        let (responder, rx) = FutureResponder::new();
        (
            BootstrapFutureState {
                configuration,
                responder,
            },
            StatusFuture::from_receiver(rx),
        )
    }

    pub(crate) fn respond(&mut self, result: Result<()>) {
        self.responder.respond(result);
    }
}

/// Future returned by `Raft::shutdown`. Mirrors the Go `shutdownFuture`:
/// waiting on it blocks until all raft tasks have exited and then closes the
/// transport.
pub struct ShutdownFuture {
    rx: oneshot::Receiver<()>,
    transport: Option<Arc<dyn Transport>>,
}

impl ShutdownFuture {
    pub(crate) fn new(rx: oneshot::Receiver<()>, transport: Option<Arc<dyn Transport>>) -> Self {
        ShutdownFuture { rx, transport }
    }
}

impl RaftFuture for ShutdownFuture {
    async fn error(mut self) -> Result<()> {
        // Wait for the raft tasks to exit. A dropped sender also means
        // shutdown completed.
        let _ = (&mut self.rx).await;
        if let Some(transport) = self.transport.take() {
            transport.close().await?;
        }
        Ok(())
    }
}

/// Opens a user-triggered snapshot once it has been taken. Mirrors the
/// `opener` closure of the Go `userSnapshotFuture`; the returned future
/// yields the snapshot metadata and a reader over its contents.
pub type SnapshotOpener = Box<
    dyn FnOnce() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(SnapshotMeta, SnapshotReader)>> + Send>,
        > + Send,
>;

/// Used for waiting on a user-triggered snapshot to complete. Mirrors the
/// Go `userSnapshotFuture`; internal to a single server.
pub(crate) struct UserSnapshotFutureState {
    /// Filled in by the snapshot task once the snapshot has been taken.
    pub(crate) opener: Option<SnapshotOpener>,
    responder: FutureResponder<Result<SnapshotOpener>>,
}

impl UserSnapshotFutureState {
    pub(crate) fn new() -> (Self, SnapshotFuture) {
        let (responder, rx) = FutureResponder::new();
        (
            UserSnapshotFutureState {
                opener: None,
                responder,
            },
            SnapshotFuture { rx },
        )
    }

    /// Responds with an error, or with the opener if one was set.
    pub(crate) fn respond(&mut self, result: Result<()>) {
        match result {
            Ok(()) => match self.opener.take() {
                Some(opener) => self.responder.respond(Ok(opener)),
                None => self
                    .responder
                    .respond(Err(RaftError::Snapshot("no snapshot available".into()))),
            },
            Err(err) => self.responder.respond(Err(err)),
        }
    }
}

/// Future for waiting on a user-triggered snapshot to complete. Mirrors the
/// Go `SnapshotFuture` interface.
pub struct SnapshotFuture {
    rx: oneshot::Receiver<Result<SnapshotOpener>>,
}

impl SnapshotFuture {
    /// Blocks until the snapshot completes and returns an opener for it.
    pub async fn wait(self) -> Result<SnapshotOpener> {
        await_oneshot(self.rx).await
    }

    /// Blocks until the snapshot completes and opens it, returning the
    /// metadata and a reader over the contents. Mirrors `Open()` being
    /// called right after `Error()`.
    pub async fn open(self) -> Result<(SnapshotMeta, SnapshotReader)> {
        let opener = self.wait().await?;
        opener().await
    }
}

impl RaftFuture for SnapshotFuture {
    async fn error(self) -> Result<()> {
        self.wait().await.map(|_| ())
    }
}

/// Used for waiting on a user-triggered restore of an external snapshot.
/// Mirrors the Go `userRestoreFuture`; internal to a single server.
pub(crate) struct UserRestoreFutureState {
    /// Metadata belonging with the snapshot.
    pub(crate) meta: SnapshotMeta,
    /// Reader over the snapshot contents.
    pub(crate) reader: SnapshotReader,
    responder: FutureResponder<Result<()>>,
}

impl UserRestoreFutureState {
    pub(crate) fn new(meta: SnapshotMeta, reader: SnapshotReader) -> (Self, UserRestoreFuture) {
        let (responder, rx) = FutureResponder::new();
        (
            UserRestoreFutureState {
                meta,
                reader,
                responder,
            },
            StatusFuture::from_receiver(rx),
        )
    }

    pub(crate) fn respond(&mut self, result: Result<()>) {
        self.responder.respond(result);
    }
}

/// The details of a started snapshot, provided by the FSM task. Mirrors the
/// fields filled in on the Go `reqSnapshotFuture`.
pub(crate) struct SnapshotRequestOutcome {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) snapshot: Option<Box<dyn FSMSnapshot>>,
}

/// Used for requesting a snapshot start from the FSM task. Mirrors the Go
/// `reqSnapshotFuture`; internal only.
pub(crate) struct ReqSnapshotFuture {
    responder: FutureResponder<Result<SnapshotRequestOutcome>>,
}

impl ReqSnapshotFuture {
    pub(crate) fn new() -> (Self, oneshot::Receiver<Result<SnapshotRequestOutcome>>) {
        let (responder, rx) = FutureResponder::new();
        (ReqSnapshotFuture { responder }, rx)
    }

    pub(crate) fn respond(&mut self, result: Result<SnapshotRequestOutcome>) {
        self.responder.respond(result);
    }
}

/// Used for requesting the FSM to perform a snapshot restore. Mirrors the
/// Go `restoreFuture`; internal only.
pub(crate) struct RestoreFuture {
    pub(crate) id: String,
    responder: FutureResponder<Result<()>>,
}

impl RestoreFuture {
    pub(crate) fn new(id: String) -> (Self, oneshot::Receiver<Result<()>>) {
        let (responder, rx) = FutureResponder::new();
        (RestoreFuture { id, responder }, rx)
    }

    pub(crate) fn respond(&mut self, result: Result<()>) {
        self.responder.respond(result);
    }
}

/// Shared state of an in-progress leader verification. Mirrors the Go
/// `verifyFuture`, including the quorum vote counting. Heartbeat tasks call
/// [`VerifyState::vote`] as heartbeat responses arrive; once a quorum votes
/// (or any peer reports a newer leader) the state is sent back on the
/// notify channel so the raft main loop can respond to the future.
pub(crate) struct VerifyState {
    responder: Mutex<FutureResponder<Result<()>>>,
    inner: Mutex<VerifyInner>,
}

struct VerifyInner {
    quorum_size: usize,
    votes: usize,
    notify: Option<mpsc::UnboundedSender<Arc<VerifyState>>>,
}

impl VerifyState {
    pub(crate) fn new() -> (Arc<Self>, VerifyFuture) {
        let (responder, rx) = FutureResponder::new();
        (
            Arc::new(VerifyState {
                responder: Mutex::new(responder),
                inner: Mutex::new(VerifyInner {
                    quorum_size: 0,
                    votes: 0,
                    notify: None,
                }),
            }),
            StatusFuture::from_receiver(rx),
        )
    }

    /// Arms verification: records the leader's own vote and registers the
    /// channel used to notify the main loop once the vote is decided.
    /// Mirrors the field setup in the Go `Raft.verifyLeader`.
    pub(crate) fn start(
        &self,
        quorum_size: usize,
        notify: mpsc::UnboundedSender<Arc<VerifyState>>,
    ) {
        let mut inner = self.inner.lock();
        inner.votes = 1;
        inner.quorum_size = quorum_size;
        inner.notify = Some(notify);
    }

    pub(crate) fn quorum_size(&self) -> usize {
        self.inner.lock().quorum_size
    }

    pub(crate) fn votes(&self) -> usize {
        self.inner.lock().votes
    }

    /// Records a vote from a peer. Mirrors `verifyFuture.vote`: a quorum of
    /// positive votes, or any negative vote, decides the verification and
    /// notifies the main loop exactly once.
    pub(crate) fn vote(self: &Arc<Self>, leader: bool) {
        let mut inner = self.inner.lock();
        // Guard against having notified already.
        let Some(notify) = inner.notify.clone() else {
            return;
        };
        if leader {
            inner.votes += 1;
            if inner.votes >= inner.quorum_size {
                let _ = notify.send(Arc::clone(self));
                inner.notify = None;
            }
        } else {
            let _ = notify.send(Arc::clone(self));
            inner.notify = None;
        }
    }

    /// Responds to the future; called from the main loop after cleanup.
    pub(crate) fn respond(&self, result: Result<()>) {
        self.responder.lock().respond(result);
    }
}

/// Used to track the progress of a leadership transfer internally. Mirrors
/// the Go `leadershipTransferFuture`.
pub(crate) struct LeadershipTransferFutureState {
    pub(crate) id: Option<ServerID>,
    pub(crate) address: Option<ServerAddress>,
    responder: FutureResponder<Result<()>>,
}

impl LeadershipTransferFutureState {
    pub(crate) fn new(
        id: Option<ServerID>,
        address: Option<ServerAddress>,
    ) -> (Self, LeadershipTransferFuture) {
        let (responder, rx) = FutureResponder::new();
        (
            LeadershipTransferFutureState {
                id,
                address,
                responder,
            },
            StatusFuture::from_receiver(rx),
        )
    }

    pub(crate) fn respond(&mut self, result: Result<()>) {
        self.responder.respond(result);
    }
}

/// The latest and committed configurations known to a raft node. Mirrors the
/// Go `configurations` struct in raft.go.
#[derive(Debug, Clone, Default)]
pub(crate) struct Configurations {
    pub(crate) committed: Configuration,
    pub(crate) committed_index: u64,
    pub(crate) latest: Configuration,
    pub(crate) latest_index: u64,
}

/// The latest configuration in use by raft and its log index. Combines what
/// Go exposes as `ConfigurationFuture.Configuration` and
/// `IndexFuture.Index`.
#[derive(Debug, Clone)]
pub struct ConfigurationValue {
    pub configuration: Configuration,
    pub index: u64,
}

/// Used to retrieve the current configurations from the main thread.
/// Mirrors the Go `configurationsFuture`; internal to a single server.
pub(crate) struct ConfigurationsFuture {
    pub(crate) configurations: Configurations,
    responder: FutureResponder<Result<Configurations>>,
}

impl ConfigurationsFuture {
    pub(crate) fn new() -> (Self, ConfigurationFuture) {
        let (responder, rx) = FutureResponder::new();
        (
            ConfigurationsFuture {
                configurations: Configurations::default(),
                responder,
            },
            ConfigurationFuture { rx },
        )
    }

    /// Responds after the main thread has filled in `configurations`.
    pub(crate) fn respond(mut self) {
        self.responder
            .respond(Ok(std::mem::take(&mut self.configurations)));
    }

    pub(crate) fn respond_error(&mut self, err: RaftError) {
        self.responder.respond(Err(err));
    }
}

/// Future returned by `Raft::get_configuration`. Mirrors the Go
/// `ConfigurationFuture` interface.
pub struct ConfigurationFuture {
    rx: oneshot::Receiver<Result<Configurations>>,
}

impl ConfigurationFuture {
    /// A future that resolves immediately with the given value, mirroring the
    /// Go `GetConfiguration` which responds before returning.
    pub(crate) fn ready(value: ConfigurationValue) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(Configurations {
            latest: value.configuration,
            latest_index: value.index,
            ..Default::default()
        }));
        ConfigurationFuture { rx }
    }

    /// Blocks until the main thread has answered, returning the latest
    /// configuration and its index.
    pub async fn wait(self) -> Result<ConfigurationValue> {
        let configurations = await_oneshot(self.rx).await?;
        Ok(ConfigurationValue {
            configuration: configurations.latest,
            index: configurations.latest_index,
        })
    }

    /// Internal: waits for the main thread to answer and returns the full
    /// configurations struct, including the committed configuration.
    /// Mirrors the Go `configReq.configurations` access used by
    /// `takeSnapshot`. Only `pub(crate)` because the underlying
    /// [`Configurations`] struct is internal-only.
    pub(crate) async fn wait_full(self) -> Result<Configurations> {
        await_oneshot(self.rx).await
    }
}

impl RaftFuture for ConfigurationFuture {
    async fn error(self) -> Result<()> {
        self.wait().await.map(|_| ())
    }
}

/// Result of a pipelined AppendEntries RPC. The error is shared because an
/// [`AppendFuture`] is delivered both to the pipeline caller and to the
/// pipeline's consumer channel (as in Go, where both hold the same future).
pub type AppendResult = std::result::Result<AppendEntriesResponse, Arc<RaftError>>;

/// Used for waiting on a pipelined AppendEntries RPC. Mirrors the Go
/// `appendFuture`. Cloneable: the pipeline returns one clone to the caller
/// of `append_entries` and delivers another on its consumer channel once the
/// RPC completes, mirroring the shared future pointer in Go.
#[derive(Clone)]
pub struct AppendFuture {
    inner: Arc<AppendFutureInner>,
    result_rx: watch::Receiver<Option<AppendResult>>,
}

impl std::fmt::Debug for AppendFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendFuture")
            .field("start", &self.inner.start)
            .field("args", &self.inner.args)
            .finish()
    }
}

struct AppendFutureInner {
    start: Instant,
    args: AppendEntriesRequest,
}

impl AppendFuture {
    pub(crate) fn new(args: AppendEntriesRequest) -> (Self, AppendFutureResponder) {
        let (tx, rx) = watch::channel(None);
        (
            AppendFuture {
                inner: Arc::new(AppendFutureInner {
                    start: Instant::now(),
                    args,
                }),
                result_rx: rx,
            },
            AppendFutureResponder {
                tx,
                responded: AtomicBool::new(false),
            },
        )
    }

    /// The time the append request was started.
    pub fn start(&self) -> Instant {
        self.inner.start
    }

    /// The parameters of the AppendEntries call.
    pub fn request(&self) -> &AppendEntriesRequest {
        &self.inner.args
    }

    /// Blocks until the RPC completes and returns its result. May be called
    /// any number of times, also concurrently on clones; all calls observe
    /// the same result, as in Go.
    pub async fn wait(&self) -> AppendResult {
        let mut rx = self.result_rx.clone();
        loop {
            {
                let result = rx.borrow_and_update();
                if let Some(result) = result.as_ref() {
                    return result.clone();
                }
            }
            // A closed channel with no result means the pipeline was dropped
            // mid-flight, mirroring ErrRaftShutdown.
            if rx.changed().await.is_err() {
                return Err(Arc::new(RaftError::RaftShutdown));
            }
        }
    }

    /// Blocks until the RPC completes and returns the error status, if any.
    /// Mirrors `appendFuture.Error`.
    pub async fn error(&self) -> Option<Arc<RaftError>> {
        self.wait().await.err()
    }
}

/// Sender half of an [`AppendFuture`], held by the pipeline's response
/// decoder. Internal only; responds at most once.
pub(crate) struct AppendFutureResponder {
    tx: watch::Sender<Option<AppendResult>>,
    responded: AtomicBool,
}

impl AppendFutureResponder {
    pub(crate) fn respond(&self, result: Result<AppendEntriesResponse>) {
        if self.responded.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.tx.send(Some(result.map_err(Arc::new)));
    }
}

/// Used to return a static error. Mirrors the Go `errorFuture`; `index` is
/// always 0 and `response` always `None`, as in Go.
pub struct ErrorFuture(pub RaftError);

impl ErrorFuture {
    pub fn new(err: RaftError) -> Self {
        ErrorFuture(err)
    }

    pub fn index(&self) -> u64 {
        0
    }

    pub fn response(&self) -> Option<ApplyResponse> {
        None
    }
}

impl RaftFuture for ErrorFuture {
    async fn error(self) -> Result<()> {
        Err(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_log(index: u64) -> Log {
        Log {
            index,
            ..Default::default()
        }
    }

    // Mirrors TestDeferFutureSuccess: a future that responds with success
    // yields no error.
    #[tokio::test]
    async fn defer_future_success() {
        let (mut log_future, rx) = LogFuture::new(test_log(1));
        let future = ApplyFuture::from_receiver(rx);
        log_future.respond();
        let outcome = future.wait().await.unwrap();
        assert_eq!(outcome.index, 1);
        assert!(outcome.response.is_none());
    }

    // Mirrors TestDeferFutureError.
    #[tokio::test]
    async fn defer_future_error() {
        let (mut log_future, rx) = LogFuture::new(test_log(1));
        let future = ApplyFuture::from_receiver(rx);
        log_future.respond_error(RaftError::LeadershipLost);
        let err = future.wait().await.unwrap_err();
        assert!(matches!(err, RaftError::LeadershipLost));
    }

    // Mirrors TestDeferFutureConcurrent: responding from another task races
    // with waiting.
    #[tokio::test]
    async fn defer_future_concurrent() {
        let (mut log_future, rx) = LogFuture::new(test_log(7));
        let future = ApplyFuture::from_receiver(rx);
        tokio::spawn(async move {
            log_future.respond();
        });
        let outcome = future.wait().await.unwrap();
        assert_eq!(outcome.index, 7);
    }

    // Responding more than once is a no-op, mirroring the Go `responded`
    // guard.
    #[tokio::test]
    async fn respond_only_happens_once() {
        let (mut log_future, rx) = LogFuture::new(test_log(3));
        let future = ApplyFuture::from_receiver(rx);
        log_future.respond_error(RaftError::NotLeader);
        log_future.respond();
        let err = future.wait().await.unwrap_err();
        assert!(matches!(err, RaftError::NotLeader));
    }

    // A dropped responder resolves the future with ErrRaftShutdown,
    // mirroring the Go ShutdownCh branch.
    #[tokio::test]
    async fn dropped_responder_yields_shutdown() {
        let (log_future, rx) = LogFuture::new(test_log(1));
        let future = ApplyFuture::from_receiver(rx);
        drop(log_future);
        let err = future.wait().await.unwrap_err();
        assert!(matches!(err, RaftError::RaftShutdown));
    }

    // Exercises the verifyFuture quorum logic: enough positive votes decide
    // success, a single negative vote decides failure, and votes after a
    // decision are ignored.
    #[tokio::test]
    async fn verify_quorum_success() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (state, _future) = VerifyState::new();
        state.start(3, tx);

        state.vote(true);
        assert!(rx.try_recv().is_err(), "not decided before quorum");
        state.vote(true);
        let decided = rx.try_recv().expect("decided at quorum");
        assert!(decided.votes() >= decided.quorum_size());
        decided.respond(Ok(()));
        assert!(_future.wait().await.is_ok());

        // Further votes are ignored once decided.
        state.vote(false);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn verify_negative_vote_decides_immediately() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (state, future) = VerifyState::new();
        state.start(3, tx);

        state.vote(false);
        let decided = rx.try_recv().expect("decided on negative vote");
        assert!(decided.votes() < decided.quorum_size());
        decided.respond(Err(RaftError::NotLeader));
        let err = future.wait().await.unwrap_err();
        assert!(matches!(err, RaftError::NotLeader));
    }

    #[tokio::test]
    async fn error_future_reports_static_error() {
        let future = ErrorFuture::new(RaftError::NotLeader);
        assert_eq!(future.index(), 0);
        assert!(future.response().is_none());
        let err = future.error().await.unwrap_err();
        assert!(matches!(err, RaftError::NotLeader));
    }
}
