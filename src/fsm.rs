use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

use crate::configuration::{decode_configuration, Configuration};
use crate::future::{LogFuture, ReqSnapshotFuture, RestoreFuture, SnapshotRequestOutcome};
use crate::log::{Log, LogType};
use crate::snapshot::{SnapshotReader, SnapshotSink, SnapshotStore};
use crate::{RaftError, Result};

/// The response of a single FSM apply, returned to the client as the
/// `ApplyFuture` response. Mirrors the `interface{}` return value of the Go
/// `FSM.Apply`; an FSM that wants to report an application-level error should
/// box an error value (the raft library itself never interprets it).
pub type ApplyResponse = Box<dyn Any + Send>;

/// Implemented by clients to make use of the replicated log. Mirrors the Go
/// `FSM` interface in fsm.go.
///
/// Unlike Go, all methods take `&self`, so implementations must use interior
/// mutability for their state. `apply` and `snapshot` are always called from
/// the same task, but `apply` may run concurrently with
/// `FSMSnapshot::persist`, so the FSM must tolerate concurrent updates while
/// a snapshot is being persisted.
#[async_trait]
pub trait FSM: Send + Sync {
    /// Called once a log entry is committed by a majority of the cluster.
    ///
    /// Must be deterministic and produce the same result on all peers. The
    /// returned value is handed back to the client through the apply future.
    async fn apply(&self, log: &Log) -> Result<ApplyResponse>;

    /// Returns a snapshot used to support log compaction, to restore the FSM
    /// to a previous state, or to bring out-of-date followers up to a recent
    /// log index.
    ///
    /// Should return quickly; expensive IO belongs in `FSMSnapshot::persist`.
    /// Callers must not assume the returned snapshot will actually be stored;
    /// `FSMSnapshot::release` is always called when the snapshot is no longer
    /// needed.
    async fn snapshot(&self) -> Result<Box<dyn FSMSnapshot>>;

    /// Restores an FSM from a snapshot. Not called concurrently with any
    /// other command. The FSM must discard all previous state before
    /// restoring.
    async fn restore(&self, snapshot: SnapshotReader) -> Result<()>;

    /// Returns the configuration-store extension if this FSM implements it.
    /// Mirrors the Go type assertion `fsm.(ConfigurationStore)` used by the
    /// FSM task; the default is `None`.
    fn as_configuration_store(&self) -> Option<&dyn ConfigurationStore> {
        None
    }

    /// Returns the batching extension if this FSM implements it. Mirrors the
    /// Go type assertion `fsm.(BatchingFSM)` used by the FSM task; the
    /// default is `None`.
    fn as_batching_fsm(&self) -> Option<&dyn BatchingFSM> {
        None
    }
}

/// Optional extension of [`FSM`] that applies multiple committed logs in one
/// batch. Mirrors the Go `BatchingFSM` interface. Up to `max_append_entries`
/// logs may be sent in a batch.
#[async_trait]
pub trait BatchingFSM: FSM {
    /// Applies a batch of committed log entries, in commit order and without
    /// gaps. Only `LogType::Command` and `LogType::Configuration` logs are
    /// sent.
    ///
    /// The returned vector must be the same length as the input; each
    /// response corresponds to the log at the same index of the input and is
    /// made available in the apply future returned by `Raft::apply`.
    async fn apply_batch(&self, logs: &[Log]) -> Vec<Result<ApplyResponse>>;
}

/// Returned by an FSM in response to [`FSM::snapshot`]. Mirrors the Go
/// `FSMSnapshot` interface. Must be safe to use concurrently with calls to
/// [`FSM::apply`].
#[async_trait]
pub trait FSMSnapshot: Send + Sync {
    /// Dumps all necessary state to the sink, finishing with
    /// [`SnapshotSink::close`] on success or [`SnapshotSink::cancel`] on
    /// error.
    async fn persist(&self, sink: Box<dyn SnapshotSink>) -> Result<()>;

    /// Invoked when the snapshot is no longer needed. Always called, even if
    /// `persist` never was.
    fn release(&self);
}

/// Optional extension for FSMs that want to persist committed configuration
/// entries. Mirrors the Go `ConfigurationStore` interface in
/// configuration.go, which is a standalone interface detected with a type
/// assertion; [`FSM::as_configuration_store`] plays that role here.
#[async_trait]
pub trait ConfigurationStore: Send + Sync {
    /// Stores a configuration that was committed at the given log index.
    async fn store_configuration(&self, index: u64, configuration: Configuration);
}

/// A committed log index with the optional future to invoke once applied.
/// Mirrors the Go `commitTuple`.
pub(crate) struct CommitTuple {
    pub(crate) log: Log,
    pub(crate) future: Option<LogFuture>,
}

/// A state-changing request to the FSM task. Mirrors the values sent on the
/// Go `fsmMutateCh`: either a batch of committed logs or a snapshot restore.
pub(crate) enum FsmMutate {
    Commit(Vec<CommitTuple>),
    Restore(RestoreFuture),
}

/// Long-running task responsible for applying logs to the FSM, async of the
/// main loop so the FSM cannot block internal operations. Mirrors
/// `Raft.runFSM` in fsm.go.
pub(crate) async fn run_fsm(
    fsm: Arc<dyn FSM>,
    snapshots: Arc<dyn SnapshotStore>,
    mut mutate_rx: mpsc::Receiver<FsmMutate>,
    mut snapshot_rx: mpsc::Receiver<ReqSnapshotFuture>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let mut last_index: u64 = 0;
    let mut last_term: u64 = 0;

    // Applies a single committed log, mirroring applySingle in the Go
    // runFSM. The FSM response (or a boxed apply error, mirroring Go
    // semantics where FSM errors surface via ApplyFuture.Response) is
    // delivered through the future.
    async fn apply_single(
        fsm: &Arc<dyn FSM>,
        ct: CommitTuple,
        last_index: &mut u64,
        last_term: &mut u64,
    ) {
        let mut resp: Option<ApplyResponse> = None;
        match ct.log.log_type {
            LogType::Command => match fsm.apply(&ct.log).await {
                Ok(r) => resp = Some(r),
                Err(e) => resp = Some(Box::new(e)),
            },
            LogType::Configuration => {
                let Some(store) = fsm.as_configuration_store() else {
                    // No configuration store: respond without applying and
                    // without advancing the indexes, as in Go.
                    if let Some(mut future) = ct.future {
                        future.respond();
                    }
                    return;
                };
                match decode_configuration(&ct.log.data) {
                    Ok(conf) => store.store_configuration(ct.log.index, conf).await,
                    Err(e) => {
                        tracing::error!(
                            "failed to decode configuration entry at index {}: {}",
                            ct.log.index,
                            e
                        );
                    }
                }
            }
            // Barriers and no-ops carry no FSM payload.
            _ => {}
        }

        *last_index = ct.log.index;
        *last_term = ct.log.term;
        if let Some(mut future) = ct.future {
            future.response = resp;
            future.respond();
        }
    }

    // Applies a batch of committed logs, mirroring applyBatch in the Go
    // runFSM: when the FSM implements BatchingFSM, command and configuration
    // logs are applied in a single call; otherwise each log is applied
    // individually.
    async fn apply_batch(
        fsm: &Arc<dyn FSM>,
        batch: Vec<CommitTuple>,
        last_index: &mut u64,
        last_term: &mut u64,
    ) {
        let Some(batching) = fsm.as_batching_fsm() else {
            for ct in batch {
                apply_single(fsm, ct, last_index, last_term).await;
            }
            return;
        };

        // Only command and configuration logs are sent to the FSM; barriers
        // are not.
        let should_send =
            |log_type: LogType| matches!(log_type, LogType::Command | LogType::Configuration);
        let send_logs: Vec<Log> = batch
            .iter()
            .filter(|ct| should_send(ct.log.log_type))
            .map(|ct| ct.log.clone())
            .collect();

        // Update the indexes from the whole batch, including barriers.
        if let Some(last) = batch.last() {
            *last_index = last.log.index;
            *last_term = last.log.term;
        }

        let mut responses = if send_logs.is_empty() {
            Vec::new()
        } else {
            batching.apply_batch(&send_logs).await
        };
        assert_eq!(
            responses.len(),
            send_logs.len(),
            "invalid number of responses"
        );
        let mut responses = responses.drain(..);

        for ct in batch {
            let mut resp: Option<ApplyResponse> = None;
            // If the log was sent to the FSM, retrieve the response.
            if should_send(ct.log.log_type) {
                match responses.next().expect("length checked") {
                    Ok(r) => resp = Some(r),
                    Err(e) => resp = Some(Box::new(e)),
                }
            }
            if let Some(mut future) = ct.future {
                future.response = resp;
                future.respond();
            }
        }
    }

    // Restores the FSM from a snapshot, mirroring the `restore` closure in
    // the Go runFSM.
    async fn restore(
        fsm: &Arc<dyn FSM>,
        snapshots: &Arc<dyn SnapshotStore>,
        mut future: RestoreFuture,
        last_index: &mut u64,
        last_term: &mut u64,
    ) {
        let (meta, source) = match snapshots.open(&future.id).await {
            Ok(opened) => opened,
            Err(e) => {
                future.respond(Err(RaftError::Other(format!(
                    "failed to open snapshot {}: {}",
                    future.id, e
                ))));
                return;
            }
        };
        if let Err(e) = fsm.restore(source).await {
            future.respond(Err(RaftError::Other(format!(
                "failed to restore snapshot {}: {}",
                future.id, e
            ))));
            return;
        }

        // Update the last index and term.
        *last_index = meta.index;
        *last_term = meta.term;
        future.respond(Ok(()));
    }

    loop {
        tokio::select! {
            req = mutate_rx.recv() => {
                match req {
                    Some(FsmMutate::Commit(batch)) => {
                        apply_batch(&fsm, batch, &mut last_index, &mut last_term).await;
                    }
                    Some(FsmMutate::Restore(future)) => {
                        restore(&fsm, &snapshots, future, &mut last_index, &mut last_term).await;
                    }
                    None => return,
                }
            }
            req = snapshot_rx.recv() => {
                let Some(mut future) = req else { return };
                // Is there something to snapshot?
                if last_index == 0 {
                    future.respond(Err(RaftError::NothingNewToSnapshot));
                    continue;
                }
                match fsm.snapshot().await {
                    Ok(snapshot) => future.respond(Ok(SnapshotRequestOutcome {
                        index: last_index,
                        term: last_term,
                        snapshot: Some(snapshot),
                    })),
                    Err(e) => future.respond(Err(e)),
                }
            }
            _ = crate::raft::wait_flag(&shutdown_rx) => return,
        }
    }
}
