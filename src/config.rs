use std::time::Duration;

use crate::{RaftError, Result, ServerID};

/// Maximum protocol version supported; only version 3 is implemented.
pub const PROTOCOL_VERSION_MAX: u8 = 3;
/// Snapshot version implemented by this library.
pub const SNAPSHOT_VERSION_MAX: u8 = 1;

/// Configures a raft node. Mirrors `Config` in config.go, minus legacy
/// protocol and telemetry options.
#[derive(Debug, Clone)]
pub struct Config {
    /// Heartbeat timeout: how often the leader sends heartbeats.
    pub heartbeat_timeout: Duration,
    /// Election timeout: how long a follower waits without contact from the
    /// leader before starting an election.
    pub election_timeout: Duration,
    /// Batch window: the leader batches log application for this long before
    /// triggering replication.
    pub commit_timeout: Duration,
    /// Maximum number of log entries in a single AppendEntries batch.
    pub max_append_entries: usize,
    /// Whether the node shuts down when removed from the cluster.
    pub shutdown_on_remove: bool,
    /// Number of logs left in the store after a snapshot.
    pub trailing_logs: u64,
    /// Interval at which the snapshot check runs (randomized within
    /// `[interval, 2*interval)`).
    pub snapshot_interval: Duration,
    /// Minimum number of new logs since the last snapshot before a new
    /// snapshot is taken.
    pub snapshot_threshold: u64,
    /// How long a leader can go without quorum contact before stepping down.
    pub leader_lease_timeout: Duration,
    /// ID of this node; must be unique within the cluster.
    pub local_id: ServerID,
    /// Channel notified on leader changes, receives `true` on gaining
    /// leadership and `false` on losing it.
    pub notify_ch: Option<tokio::sync::mpsc::Sender<bool>>,
    /// Disable restoring snapshots on startup.
    pub no_snapshot_restore_on_start: bool,
    /// Disable the pre-vote phase.
    pub pre_vote_disabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_timeout: Duration::from_millis(1000),
            election_timeout: Duration::from_millis(1000),
            commit_timeout: Duration::from_millis(50),
            max_append_entries: 64,
            shutdown_on_remove: true,
            trailing_logs: 10240,
            snapshot_interval: Duration::from_secs(120),
            snapshot_threshold: 8192,
            leader_lease_timeout: Duration::from_millis(500),
            local_id: String::new(),
            notify_ch: None,
            no_snapshot_restore_on_start: false,
            pre_vote_disabled: false,
        }
    }
}

impl Config {
    /// Validates the configuration, mirroring `ValidateConfig` in the Go
    /// implementation (for protocol version 3 only).
    pub fn validate(&self) -> Result<()> {
        if self.local_id.is_empty() {
            return Err(RaftError::Configuration("LocalID must be set".into()));
        }
        if self.heartbeat_timeout < Duration::from_millis(5) {
            return Err(RaftError::Configuration(
                "HeartbeatTimeout is too low".into(),
            ));
        }
        if self.election_timeout < Duration::from_millis(5) {
            return Err(RaftError::Configuration(
                "ElectionTimeout is too low".into(),
            ));
        }
        if self.commit_timeout < Duration::from_millis(1) {
            return Err(RaftError::Configuration("CommitTimeout is too low".into()));
        }
        if self.max_append_entries == 0 {
            return Err(RaftError::Configuration(
                "MaxAppendEntries must be positive".into(),
            ));
        }
        if self.max_append_entries > 1024 {
            return Err(RaftError::Configuration(
                "MaxAppendEntries is too large".into(),
            ));
        }
        if self.snapshot_interval < Duration::from_millis(5) {
            return Err(RaftError::Configuration(
                "SnapshotInterval is too low".into(),
            ));
        }
        if self.leader_lease_timeout < Duration::from_millis(5) {
            return Err(RaftError::Configuration(
                "LeaderLeaseTimeout is too low".into(),
            ));
        }
        if self.leader_lease_timeout > self.heartbeat_timeout {
            return Err(RaftError::Configuration(
                "LeaderLeaseTimeout cannot be larger than heartbeat timeout".into(),
            ));
        }
        if self.election_timeout < self.heartbeat_timeout {
            return Err(RaftError::Configuration(
                "ElectionTimeout must be equal or greater than Heartbeat Timeout".into(),
            ));
        }
        Ok(())
    }
}

/// The subset of Config that can be reloaded at runtime. Mirrors
/// `ReloadableConfig`.
#[derive(Debug, Clone, Copy)]
pub struct ReloadableConfig {
    pub trailing_logs: u64,
    pub snapshot_interval: Duration,
    pub snapshot_threshold: u64,
    pub heartbeat_timeout: Duration,
    pub election_timeout: Duration,
}

impl From<&Config> for ReloadableConfig {
    fn from(c: &Config) -> Self {
        ReloadableConfig {
            trailing_logs: c.trailing_logs,
            snapshot_interval: c.snapshot_interval,
            snapshot_threshold: c.snapshot_threshold,
            heartbeat_timeout: c.heartbeat_timeout,
            election_timeout: c.election_timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let c = Config {
            local_id: "node1".into(),
            ..Default::default()
        };
        assert!(c.validate().is_ok());
    }

    #[test]
    fn empty_local_id_is_rejected() {
        assert!(Config::default().validate().is_err());
    }
}
