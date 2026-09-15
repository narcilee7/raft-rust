use std::io;

use thiserror::Error;

/// Errors returned by the raft library. Mirrors the `Err*` values in the Go
/// implementation's api.go and replication.go.
#[derive(Debug, Error)]
pub enum RaftError {
    #[error("node is not the leader")]
    NotLeader,

    #[error("node is the leader")]
    Leader,

    #[error("node is not a voter")]
    NotVoter,

    #[error("leadership lost while committing log")]
    LeadershipLost,

    #[error("log restored, local lease lost")]
    AbortedByRestore,

    #[error("raft is already shutdown")]
    RaftShutdown,

    #[error("failed to enqueue operation, timeout")]
    EnqueueTimeout,

    #[error("nothing new to snapshot")]
    NothingNewToSnapshot,

    #[error("operation not supported with current protocol version")]
    UnsupportedProtocol,

    #[error("cluster cannot be bootstrapped if already started")]
    CantBootstrap,

    #[error("leadership transfer in progress")]
    LeadershipTransferInProgress,

    #[error("log not found")]
    LogNotFound,

    #[error("transport does not support pipelined replication")]
    PipelineReplicationNotSupported,

    #[error("incompatible log store for committed log restore")]
    IncompatibleLogStore,

    #[error("pipeline is closed")]
    PipelineShutdown,

    #[error("transport is closed")]
    TransportShutdown,

    #[error("failed to connect to {address}: {source}")]
    TransportConnect { address: String, source: io::Error },

    #[error("timed out waiting for response")]
    Timeout,

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("encoding error: {0}")]
    Encode(String),

    #[error("decoding error: {0}")]
    Decode(String),

    #[error("snapshot error: {0}")]
    Snapshot(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, RaftError>;

impl From<rmp_serde::encode::Error> for RaftError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        RaftError::Encode(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for RaftError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        RaftError::Decode(e.to_string())
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for RaftError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        RaftError::Other(e.to_string())
    }
}

/// Suggested maximum size of a log entry's data, matching the Go
/// implementation's `SuggestedMaxDataSize`.
pub const SUGGESTED_MAX_DATA_SIZE: usize = 512 * 1024;
