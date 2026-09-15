use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::Result;

/// Type of a log entry. Mirrors `LogType` in the Go implementation's log.go.
/// Only the non-deprecated variants are ported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum LogType {
    /// A command applied to the user FSM.
    Command = 0,
    /// A no-op, committed by a new leader to establish its term.
    Noop = 1,
    /// A barrier, used to wait until preceding logs are applied.
    Barrier = 3,
    /// A cluster configuration change.
    Configuration = 4,
}

/// A single log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    /// Index of this entry in the log.
    pub index: u64,
    /// Term in which this entry was created.
    pub term: u64,
    /// Type of this entry.
    pub log_type: LogType,
    /// Opaque payload.
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    /// Opaque extensions, used by transports for leadership transfer.
    #[serde(with = "serde_bytes")]
    pub extensions: Vec<u8>,
    /// When the entry was appended on the leader. Not serialized on the wire.
    #[serde(skip)]
    pub appended_at: Option<Instant>,
}

impl Default for Log {
    fn default() -> Self {
        Log {
            index: 0,
            term: 0,
            log_type: LogType::Command,
            data: Vec::new(),
            extensions: Vec::new(),
            appended_at: None,
        }
    }
}

/// Store for the raft log entries. Mirrors the Go `LogStore` interface.
#[async_trait]
pub trait LogStore: Send + Sync {
    /// Returns the first index written, 0 for empty.
    async fn first_index(&self) -> Result<u64>;

    /// Returns the last index written, 0 for empty.
    async fn last_index(&self) -> Result<u64>;

    /// Gets the log entry at the given index.
    async fn get_log(&self, index: u64) -> Result<Log>;

    /// Stores a single log entry.
    async fn store_log(&self, log: &Log) -> Result<()>;

    /// Stores multiple log entries, in order.
    async fn store_logs(&self, logs: &[Log]) -> Result<()> {
        for log in logs {
            self.store_log(log).await?;
        }
        Ok(())
    }

    /// Deletes a range of log entries, inclusive on both ends.
    async fn delete_range(&self, min: u64, max: u64) -> Result<()>;
}
