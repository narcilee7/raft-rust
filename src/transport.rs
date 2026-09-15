use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::configuration::{ServerAddress, ServerID};
use crate::future::AppendFuture;
use crate::log::Log;
use crate::snapshot::SnapshotReader;
use crate::{RaftError, Result};

/// Common sub-structure passed along with every RPC, carrying the protocol
/// version and identity of the sender. Mirrors `RPCHeader` in commands.go.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RPCHeader {
    /// Version of the protocol the sender is speaking.
    pub protocol_version: u8,
    /// ID of the node sending the RPC request or response.
    pub id: ServerID,
    /// Address of the node sending the RPC request or response.
    pub addr: ServerAddress,
}

/// Command used to append entries to the replicated log. Mirrors
/// `AppendEntriesRequest`; the deprecated `Leader` field is replaced by
/// `header.addr`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub header: RPCHeader,
    /// Current term of the leader.
    pub term: u64,
    /// Previous log entry index/term, for integrity checking.
    pub prev_log_entry: u64,
    pub prev_log_term: u64,
    /// New entries to commit; empty for a heartbeat.
    pub entries: Vec<Log>,
    /// Commit index on the leader.
    pub leader_commit_index: u64,
}

/// Response to an [`AppendEntriesRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub header: RPCHeader,
    /// Newer term if the leader is out of date.
    pub term: u64,
    /// Hint of the follower's last log index, to accelerate rebuilding slow
    /// nodes.
    pub last_log: u64,
    /// False if a conflicting entry prevented the append.
    pub success: bool,
    /// True when the request did not succeed but there is no need to
    /// wait/back-off before the next attempt.
    pub no_retry_backoff: bool,
}

/// Command used by a candidate to ask a peer for a vote. Mirrors
/// `RequestVoteRequest`; the deprecated `Candidate` field is replaced by
/// `header.addr`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub header: RPCHeader,
    /// Term of the candidate.
    pub term: u64,
    /// Used to ensure safety: the candidate's last log index/term.
    pub last_log_index: u64,
    pub last_log_term: u64,
    /// Indicates the vote was triggered by a leadership transfer, so peers
    /// aware of an existing leader may still grant it.
    pub leadership_transfer: bool,
}

/// Response to a [`RequestVoteRequest`]. The deprecated `Peers` field is not
/// ported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub header: RPCHeader,
    /// Newer term if the candidate is out of date.
    pub term: u64,
    /// Whether the vote is granted.
    pub granted: bool,
}

/// Command used by a candidate to ask a peer for a pre-vote. Mirrors
/// `RequestPreVoteRequest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestPreVoteRequest {
    pub header: RPCHeader,
    /// Term the candidate would use if elected.
    pub term: u64,
    /// Used to ensure safety: the candidate's last log index/term.
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Response to a [`RequestPreVoteRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestPreVoteResponse {
    pub header: RPCHeader,
    /// Newer term if the candidate is out of date.
    pub term: u64,
    /// Whether the pre-vote is granted.
    pub granted: bool,
}

/// Command sent to a peer to bootstrap its log (and state machine) from a
/// snapshot on another peer. Mirrors `InstallSnapshotRequest`; the
/// deprecated `Peers` and `Leader` fields are not ported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub header: RPCHeader,
    /// Version of the snapshot format; only version 1 is supported.
    pub snapshot_version: u8,
    /// Current term of the leader.
    pub term: u64,
    /// Last index/term included in the snapshot.
    pub last_log_index: u64,
    pub last_log_term: u64,
    /// Encoded cluster membership in the snapshot.
    #[serde(with = "serde_bytes")]
    pub configuration: Vec<u8>,
    /// Log index where `configuration` was originally written.
    pub configuration_index: u64,
    /// Size of the snapshot data that follows on the reader.
    pub size: u64,
}

/// Response to an [`InstallSnapshotRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub header: RPCHeader,
    /// Newer term if the leader is out of date.
    pub term: u64,
    /// Whether the snapshot was installed.
    pub success: bool,
}

/// Command used by a leader to signal another server to start an election.
/// Mirrors `TimeoutNowRequest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutNowRequest {
    pub header: RPCHeader,
}

/// Response to a [`TimeoutNowRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutNowResponse {
    pub header: RPCHeader,
}

/// The command of an incoming RPC. Mirrors the `interface{}` command of the
/// Go `RPC` struct, closed over the supported request types.
#[derive(Debug)]
pub enum RPCCommand {
    AppendEntries(AppendEntriesRequest),
    RequestVote(RequestVoteRequest),
    RequestPreVote(RequestPreVoteRequest),
    InstallSnapshot(InstallSnapshotRequest),
    TimeoutNow(TimeoutNowRequest),
}

impl RPCCommand {
    /// The RPC header of the underlying request.
    pub fn header(&self) -> &RPCHeader {
        match self {
            RPCCommand::AppendEntries(r) => &r.header,
            RPCCommand::RequestVote(r) => &r.header,
            RPCCommand::RequestPreVote(r) => &r.header,
            RPCCommand::InstallSnapshot(r) => &r.header,
            RPCCommand::TimeoutNow(r) => &r.header,
        }
    }
}

/// The response to an incoming RPC, matching the command variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RPCResponse {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
    RequestPreVote(RequestPreVoteResponse),
    InstallSnapshot(InstallSnapshotResponse),
    TimeoutNow(TimeoutNowResponse),
}

/// An incoming RPC: a command plus a response mechanism. Mirrors the Go
/// `RPC` struct in transport.go. The raft main loop consumes these from the
/// transport's consumer channel and responds exactly once.
pub struct RPC {
    pub command: RPCCommand,
    /// Snapshot data stream, set only for `InstallSnapshot`.
    pub reader: Option<SnapshotReader>,
    resp_tx: oneshot::Sender<Result<RPCResponse>>,
}

impl RPC {
    pub fn new(
        command: RPCCommand,
        reader: Option<SnapshotReader>,
        resp_tx: oneshot::Sender<Result<RPCResponse>>,
    ) -> Self {
        RPC {
            command,
            reader,
            resp_tx,
        }
    }

    /// Responds with a response. Mirrors the Go `RPC.Respond`.
    pub fn respond(self, resp: RPCResponse) {
        let _ = self.resp_tx.send(Ok(resp));
    }

    /// Responds with an error.
    pub fn respond_error(self, err: RaftError) {
        let _ = self.resp_tx.send(Err(err));
    }

    /// Splits the RPC into its command, snapshot data reader and responder,
    /// for the raft main loop which dispatches on the command. Mirrors
    /// destructuring the Go `RPC` struct.
    pub fn split(self) -> (RPCCommand, Option<SnapshotReader>, RPCResponder) {
        (
            self.command,
            self.reader,
            RPCResponder {
                resp_tx: self.resp_tx,
            },
        )
    }
}

/// The response half of an [`RPC`]; responds exactly once. Mirrors the Go
/// `RPC.Respond` on the `RespChan`.
pub struct RPCResponder {
    resp_tx: oneshot::Sender<Result<RPCResponse>>,
}

impl RPCResponder {
    /// Responds with a response.
    pub fn respond(self, resp: RPCResponse) {
        let _ = self.resp_tx.send(Ok(resp));
    }

    /// Responds with an error.
    pub fn respond_error(self, err: RaftError) {
        let _ = self.resp_tx.send(Err(err));
    }
}

impl std::fmt::Debug for RPC {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RPC")
            .field("command", &self.command)
            .field("has_reader", &self.reader.is_some())
            .finish()
    }
}

/// Fast-path callback for heartbeat RPCs, used to avoid head-of-line
/// blocking from disk IO. Mirrors the callback accepted by the Go
/// `Transport.SetHeartbeatHandler`. Transports that do not support the fast
/// path may ignore it and push heartbeats onto the consumer channel.
pub type HeartbeatHandler = Box<dyn Fn(RPC) + Send + Sync>;

/// Interface for network transports allowing raft nodes to communicate.
/// Mirrors the Go `Transport` interface in transport.go, with the
/// `WithPreVote`/`WithClose` extensions folded in (only protocol version 3
/// is supported) and responses returned directly instead of through out
/// parameters.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Returns the receiver of incoming RPC requests. May only be called
    /// once; subsequent calls panic.
    fn consumer(&self) -> mpsc::Receiver<RPC>;

    /// Our local address, used to distinguish from our peers.
    fn local_addr(&self) -> ServerAddress;

    /// Returns a pipeline for streaming AppendEntries requests to the
    /// target, or an error if pipelining is not supported or the target is
    /// unreachable.
    async fn append_entries_pipeline(
        &self,
        id: &ServerID,
        target: &ServerAddress,
    ) -> Result<Box<dyn AppendPipeline>>;

    /// Sends an AppendEntries RPC to the target node.
    async fn append_entries(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse>;

    /// Sends a RequestVote RPC to the target node.
    async fn request_vote(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse>;

    /// Sends a RequestPreVote RPC to the target node.
    async fn request_pre_vote(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &RequestPreVoteRequest,
    ) -> Result<RequestPreVoteResponse>;

    /// Pushes a snapshot down to a follower. The data is read from `data`
    /// and streamed to the target.
    async fn install_snapshot(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &InstallSnapshotRequest,
        data: SnapshotReader,
    ) -> Result<InstallSnapshotResponse>;

    /// Signals the target node to start an election, for leadership
    /// transfer.
    async fn timeout_now(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &TimeoutNowRequest,
    ) -> Result<TimeoutNowResponse>;

    /// Serializes a peer's address.
    fn encode_peer(&self, id: &ServerID, addr: &ServerAddress) -> Vec<u8>;

    /// Deserializes a peer's address.
    fn decode_peer(&self, buf: &[u8]) -> ServerAddress;

    /// Sets up a heartbeat fast-path handler. Transports that do not
    /// support this can ignore the call; heartbeats then arrive on the
    /// consumer channel.
    fn set_heartbeat_handler(&self, cb: HeartbeatHandler);

    /// Permanently closes the transport, stopping any associated tasks and
    /// freeing resources.
    async fn close(&self) -> Result<()>;
}

/// Pipelines AppendEntries requests to increase replication throughput by
/// masking latency. Mirrors the Go `AppendPipeline` interface.
#[async_trait]
pub trait AppendPipeline: Send + Sync {
    /// Adds another request to the pipeline. Sending may apply back-pressure.
    /// The returned future completes when the response arrives.
    async fn append_entries(&self, args: AppendEntriesRequest) -> Result<AppendFuture>;

    /// Returns the receiver of completed futures. May only be called once.
    fn consumer(&self) -> mpsc::Receiver<AppendFuture>;

    /// Closes the pipeline and cancels all inflight RPCs.
    async fn close(&self) -> Result<()>;
}
