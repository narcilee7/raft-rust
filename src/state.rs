use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

/// The state of a raft peer. Mirrors `RaftState` in state.go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RaftState {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
    Shutdown = 3,
}

impl std::fmt::Display for RaftState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftState::Follower => write!(f, "Follower"),
            RaftState::Candidate => write!(f, "Candidate"),
            RaftState::Leader => write!(f, "Leader"),
            RaftState::Shutdown => write!(f, "Shutdown"),
        }
    }
}

impl From<u8> for RaftState {
    fn from(v: u8) -> Self {
        match v {
            1 => RaftState::Candidate,
            2 => RaftState::Leader,
            3 => RaftState::Shutdown,
            _ => RaftState::Follower,
        }
    }
}

/// Shared mutable raft state, updated from multiple tasks. Mirrors the
/// `raftState` struct embedded in the Go `Raft` type.
pub struct RaftSharedState {
    /// The current term, persisted to the stable store before any update.
    pub current_term: AtomicU64,
    /// Highest log entry known to be committed.
    pub commit_index: AtomicU64,
    /// Highest log entry applied to the FSM.
    pub last_applied: AtomicU64,
    /// Index/term of the latest snapshot, guarded pairwise like the Go
    /// `lastLock` so readers never observe a mixed pair.
    last_snapshot: Mutex<(u64, u64)>,
    /// Index/term of the latest log entry written to the store (not the
    /// snapshot).
    last_log: Mutex<(u64, u64)>,
    /// Current peer state.
    state: AtomicU8,
    /// Last time we heard from the leader (or won an election).
    pub last_contact: Mutex<Option<Instant>>,
}

impl Default for RaftSharedState {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftSharedState {
    pub fn new() -> Self {
        RaftSharedState {
            current_term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
            last_snapshot: Mutex::new((0, 0)),
            last_log: Mutex::new((0, 0)),
            state: AtomicU8::new(RaftState::Follower as u8),
            last_contact: Mutex::new(None),
        }
    }

    pub fn state(&self) -> RaftState {
        RaftState::from(self.state.load(Ordering::Acquire))
    }

    pub fn set_state(&self, s: RaftState) {
        self.state.store(s as u8, Ordering::Release);
    }

    pub fn current_term(&self) -> u64 {
        self.current_term.load(Ordering::Acquire)
    }

    pub fn set_current_term(&self, term: u64) {
        self.current_term.store(term, Ordering::Release);
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
    }

    pub fn set_commit_index(&self, idx: u64) {
        self.commit_index.store(idx, Ordering::Release);
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::Acquire)
    }

    pub fn set_last_applied(&self, idx: u64) {
        self.last_applied.store(idx, Ordering::Release);
    }

    pub fn last_snapshot_index(&self) -> u64 {
        self.last_snapshot.lock().0
    }

    pub fn last_snapshot_term(&self) -> u64 {
        self.last_snapshot.lock().1
    }

    /// Sets the index/term of the latest snapshot atomically as a pair.
    /// Mirrors `raftState.setLastSnapshot`.
    pub fn set_last_snapshot(&self, index: u64, term: u64) {
        *self.last_snapshot.lock() = (index, term);
    }

    /// Returns the index/term of the latest snapshot as a consistent pair.
    /// Mirrors `raftState.getLastSnapshot`.
    pub fn last_snapshot(&self) -> (u64, u64) {
        *self.last_snapshot.lock()
    }

    pub fn last_log_index(&self) -> u64 {
        self.last_log.lock().0
    }

    pub fn last_log_term(&self) -> u64 {
        self.last_log.lock().1
    }

    /// Sets the index/term of the latest log entry atomically as a pair.
    /// Mirrors `raftState.setLastLog`.
    pub fn set_last_log(&self, index: u64, term: u64) {
        *self.last_log.lock() = (index, term);
    }

    /// Returns the index/term of the latest log entry as a consistent pair.
    /// Mirrors `raftState.getLastLog`.
    pub fn last_log(&self) -> (u64, u64) {
        *self.last_log.lock()
    }

    /// Returns the index of the last entry, considering both the log store
    /// and the latest snapshot.
    pub fn last_index(&self) -> u64 {
        self.last_log_index().max(self.last_snapshot_index())
    }

    /// Returns the index/term of the last entry in stable storage, either
    /// from the last log or the last snapshot. Mirrors
    /// `raftState.getLastEntry`.
    pub fn last_entry(&self) -> (u64, u64) {
        let (log_index, log_term) = self.last_log();
        let (snap_index, snap_term) = self.last_snapshot();
        if log_index >= snap_index {
            (log_index, log_term)
        } else {
            (snap_index, snap_term)
        }
    }

    pub fn set_last_contact(&self, when: Instant) {
        *self.last_contact.lock() = Some(when);
    }

    pub fn last_contact_time(&self) -> Option<Instant> {
        *self.last_contact.lock()
    }
}
