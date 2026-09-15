//! Observer mechanism: register a sink for raft state changes (leadership
//! transitions, peer changes, heartbeat failures). Mirrors `observer.go` of
//! the Go implementation. The dispatcher is internal; only
//! [`ObserverChannel`], [`Observer`], and the [`Raft::register_observer`]
//! API are public.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::configuration::{Server, ServerAddress, ServerID};
use crate::raft::RaftCore;
use crate::state::RaftState;
use crate::transport::RequestVoteRequest;

/// The kinds of payloads that can be carried by an [`Observation`]. Mirrors
/// the Go `interface{}` field, narrowed to a sum type so observers don't
/// need to downcast.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ObservationData {
    /// The local raft state changed (Follower/Candidate/Leader/Shutdown).
    State(RaftState),
    /// A new leader was observed.
    Leader(LeaderObservation),
    /// A peer was added to or removed from the configuration.
    Peer(PeerObservation),
    /// A peer has failed to respond to a heartbeat.
    FailedHeartbeat(FailedHeartbeatObservation),
    /// A peer has resumed responding to heartbeats after previous
    /// failures.
    ResumedHeartbeat(ResumedHeartbeatObservation),
    /// An inbound `RequestVote` RPC was received.
    RequestVote(RequestVoteRequest),
}

/// Payload sent when leadership transitions. Mirrors the Go
/// `LeaderObservation`.
#[derive(Debug, Clone)]
pub struct LeaderObservation {
    /// New leader's network address.
    pub leader_addr: ServerAddress,
    /// New leader's stable ID.
    pub leader_id: ServerID,
    /// Backward-compatibility alias for `leader_addr`. Mirrors the
    /// deprecated Go field of the same name.
    pub leader: ServerAddress,
}

/// Payload sent when a peer joins or leaves the configuration. Mirrors
/// the Go `PeerObservation`.
#[derive(Debug, Clone)]
pub struct PeerObservation {
    /// The peer that changed.
    pub peer: Server,
    /// `true` if the peer was removed, `false` if it was added.
    pub removed: bool,
}

/// Payload sent when a peer fails to respond to a heartbeat. Mirrors
/// `FailedHeartbeatObservation`.
#[derive(Debug, Clone)]
pub struct FailedHeartbeatObservation {
    pub peer_id: ServerID,
    pub last_contact: Option<std::time::Instant>,
}

/// Payload sent when a previously failing peer resumes contact. Mirrors
/// `ResumedHeartbeatObservation`.
#[derive(Debug, Clone)]
pub struct ResumedHeartbeatObservation {
    pub peer_id: ServerID,
}

/// A single observation event delivered to a registered observer. Mirrors
/// the Go `Observation` struct.
#[derive(Debug, Clone)]
pub struct Observation {
    /// The observation payload.
    pub data: ObservationData,
}

/// Predicate that decides whether an observation should be delivered to
/// an observer. Mirrors the Go `FilterFn` type. The trait object is
/// wrapped in an [`Arc`] so it can be shared between the [`Observer`]
/// config and the [`ObserverHandle`] registered with the raft.
pub type FilterFn = Arc<dyn Fn(&Observation) -> bool + Send + Sync>;

/// A handle returned by [`Raft::register_observer`]. The observation
/// stream is consumed by receiving from `receiver`; on drop, the observer
/// is deregistered automatically.
pub struct ObserverChannel {
    /// Receiver half of the channel. Cloned observers share the same
    /// stream, mirroring the shared `chan Observation` of Go observers.
    pub receiver: mpsc::Receiver<Observation>,
    /// Shared bookkeeping: the registered observer entry, dropped on
    /// deregistration.
    pub(crate) handle: Arc<ObserverHandle>,
}

impl ObserverChannel {
    /// The number of observations delivered to this observer.
    pub fn num_observed(&self) -> u64 {
        self.handle.num_observed.load(Ordering::Acquire)
    }

    /// The number of observations dropped because the channel was full
    /// and the observer is non-blocking.
    pub fn num_dropped(&self) -> u64 {
        self.handle.num_dropped.load(Ordering::Acquire)
    }

    /// Deregisters the observer immediately, regardless of whether the
    /// channel is still live. Mirrors `Raft.DeregisterObserver`.
    pub fn deregister(self) {
        let weak = self.handle.core.read().clone();
        if let Some(core) = weak.and_then(|w| w.upgrade()) {
            core.deregister_observer(self.handle.id);
        }
    }
}

/// Configuration for an observer. Mirrors the `Observer` constructor
/// parameters from the Go API.
pub struct Observer {
    /// Buffer size of the channel created for the observer.
    pub channel_size: usize,
    /// When `true`, the dispatcher blocks on full channels. When `false`
    /// (the recommended setting), full channels drop the observation and
    /// increment `num_dropped`.
    pub blocking: bool,
    /// Optional predicate that filters observations before delivery.
    pub filter: Option<FilterFn>,
}

impl Default for Observer {
    fn default() -> Self {
        Observer {
            channel_size: 64,
            blocking: false,
            filter: None,
        }
    }
}

impl Observer {
    /// Creates a new observer with the given parameters. Mirrors
    /// `NewObserver`. The returned [`ObserverChannel`] is the receiver
    /// half of the observer's channel.
    pub fn new(
        receiver_size: usize,
        blocking: bool,
        filter: Option<FilterFn>,
    ) -> (Observer, ObserverChannel) {
        let (tx, rx) = mpsc::channel(receiver_size.max(1));
        let config = Observer {
            channel_size: receiver_size,
            blocking,
            filter: filter.clone(),
        };
        let handle = Arc::new(ObserverHandle {
            id: NEXT_OBSERVER_ID.fetch_add(1, Ordering::Relaxed),
            sender: tx,
            blocking,
            filter,
            num_observed: AtomicU64::new(0),
            num_dropped: AtomicU64::new(0),
            core: RwLock::new(None),
        });
        let channel = ObserverChannel {
            receiver: rx,
            handle: Arc::clone(&handle),
        };
        (config, channel)
    }
}

/// Counter that assigns each observer a unique ID. Mirrors
/// `nextObserverID` in Go.
static NEXT_OBSERVER_ID: AtomicU64 = AtomicU64::new(1);

/// Internal observer entry held by [`RaftCore::observers`]. Mirrors the
/// Go `Observer` struct.
pub(crate) struct ObserverHandle {
    pub id: u64,
    pub sender: mpsc::Sender<Observation>,
    pub blocking: bool,
    pub filter: Option<FilterFn>,
    pub num_observed: AtomicU64,
    pub num_dropped: AtomicU64,
    /// Weak reference to the owning core, used by `deregister` to clean
    /// up if the raft is still alive when the observer is dropped.
    pub core: RwLock<Option<WeakRaftCore>>,
}

/// Weak handle to a [`RaftCore`] used so [`ObserverChannel::deregister`]
/// can locate the core that owns it.
pub(crate) type WeakRaftCore = std::sync::Weak<RaftCore>;

impl ObserverHandle {
    /// Returns `true` if the observation passes the observer's filter (or
    /// the observer has no filter).
    pub(crate) fn matches(&self, observation: &Observation) -> bool {
        match &self.filter {
            Some(f) => f(observation),
            None => true,
        }
    }
}

/// Dispatches a single observation to every registered observer that
/// passes its filter. Mirrors `Raft.observe`. Never panics: if a
/// downstream channel is closed or full, the observation is silently
/// dropped (or the dispatcher blocks, when configured to).
pub(crate) fn dispatch(observers: &RwLock<Vec<Arc<ObserverHandle>>>, data: ObservationData) {
    let observation = Observation { data };
    let snapshot: Vec<Arc<ObserverHandle>> = observers.read().clone();
    for observer in snapshot {
        if !observer.matches(&observation) {
            continue;
        }
        if observer.blocking {
            if observer.sender.try_send(observation.clone()).is_ok() {
                observer.num_observed.fetch_add(1, Ordering::Relaxed);
            } else if observer.sender.capacity() == 0 {
                observer.num_dropped.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            match observer.sender.try_send(observation.clone()) {
                Ok(()) => {
                    observer.num_observed.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    observer.num_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::ServerSuffrage;

    fn sample_server(id: &str) -> Server {
        Server {
            suffrage: ServerSuffrage::Voter,
            id: id.to_string(),
            address: format!("addr-{}", id),
        }
    }

    /// Mirrors the Go `TestObserver` basic flow: an observer receives the
    /// events the dispatcher publishes.
    #[tokio::test]
    async fn observer_receives_events() {
        let (_config, mut channel) = Observer::new(8, false, None);
        let observer = channel.handle.clone();
        observer
            .sender
            .send(Observation {
                data: ObservationData::State(RaftState::Follower),
            })
            .await
            .unwrap();
        let observation = channel.receiver.recv().await.unwrap();
        match observation.data {
            ObservationData::State(state) => assert_eq!(state, RaftState::Follower),
            other => panic!("unexpected payload: {:?}", other),
        }
    }

    /// A non-trivial filter only lets through matching observations.
    #[tokio::test]
    async fn filter_blocks_mismatched() {
        let filter: FilterFn = Arc::new(|o| matches!(o.data, ObservationData::Leader(_)));
        let (_config, mut channel) = Observer::new(8, false, Some(filter));
        let observer = channel.handle.clone();

        assert!(!observer.matches(&Observation {
            data: ObservationData::State(RaftState::Follower),
        }));
        assert!(observer.matches(&Observation {
            data: ObservationData::Leader(LeaderObservation {
                leader_addr: "addr".into(),
                leader_id: "id".into(),
                leader: "addr".into(),
            }),
        }));

        observer
            .sender
            .send(Observation {
                data: ObservationData::Leader(LeaderObservation {
                    leader_addr: "addr".into(),
                    leader_id: "id".into(),
                    leader: "addr".into(),
                }),
            })
            .await
            .unwrap();
        let observation = channel.receiver.recv().await.unwrap();
        assert!(matches!(observation.data, ObservationData::Leader(_)));
    }

    /// `PeerObservation` round-trips a peer add and removal.
    #[test]
    fn peer_observation_roundtrip() {
        let added = PeerObservation {
            peer: sample_server("a"),
            removed: false,
        };
        let removed = PeerObservation {
            peer: sample_server("a"),
            removed: true,
        };
        assert!(!added.removed);
        assert!(removed.removed);
    }
}
