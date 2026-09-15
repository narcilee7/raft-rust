//! TCP-based network transport. Frames each RPC as a one-byte type tag
//! followed by a 4-byte big-endian length and a msgpack-encoded body.
//! Responses use the same length-prefixed body format. InstallSnapshot
//! streams the snapshot state as raw bytes after the request header.
//!
//! The wire format differs slightly from `net_transport.go` (length-prefixed
//! msgpack instead of streaming) so we can drive the codec with tokio's
//! `AsyncReadExt`/`AsyncWriteExt` without an async-msgpack bridge. This
//! port is not wire-compatible with the Go implementation; the Go code is
//! a behavioral reference only.

use std::collections::HashMap;
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{mpsc, oneshot};

use crate::configuration::{ServerAddress, ServerID};
use crate::future::{AppendFuture, AppendFutureResponder};
use crate::snapshot::SnapshotReader;
use crate::transport::{
    AppendEntriesRequest, AppendEntriesResponse, AppendPipeline, HeartbeatHandler,
    InstallSnapshotRequest, InstallSnapshotResponse, RPCCommand, RPCResponse,
    RequestPreVoteRequest, RequestPreVoteResponse, RequestVoteRequest, RequestVoteResponse,
    TimeoutNowRequest, TimeoutNowResponse, Transport, RPC,
};
use crate::{RaftError, Result};

// RPC type tags, mirroring the constants in net_transport.go.
const RPC_APPEND_ENTRIES: u8 = 0;
const RPC_REQUEST_VOTE: u8 = 1;
const RPC_INSTALL_SNAPSHOT: u8 = 2;
const RPC_TIMEOUT_NOW: u8 = 3;
const RPC_REQUEST_PRE_VOTE: u8 = 4;

/// Default timeout scale: 256 KB. Mirrors `DefaultTimeoutScale`.
pub const DEFAULT_TIMEOUT_SCALE: usize = 256 * 1024;

/// Default pipelining depth. Mirrors `DefaultMaxRPCsInFlight`.
pub const DEFAULT_MAX_RPCS_IN_FLIGHT: usize = 2;

/// Minimum pipelining depth for the pipeline code path. Mirrors
/// `minInFlightForPipelining`.
const MIN_IN_FLIGHT_FOR_PIPELINING: usize = 2;

/// Capacity of the consumer channel that the raft main loop reads from.
const CONSUMER_CAPACITY: usize = 1024;

/// Capacity of the in-progress and done channels of each pipeline.
const PIPELINE_CAPACITY: usize = 64;

/// Abstraction over the low-level stream. Mirrors the `StreamLayer`
/// interface in net_transport.go.
#[async_trait]
pub trait StreamLayer: Send + Sync + 'static {
    /// Local address we are listening on.
    fn local_addr(&self) -> Result<ServerAddress>;

    /// Accept a new inbound connection. Should block until one arrives.
    async fn accept(&self) -> Result<TcpStream>;

    /// Dial the target address.
    async fn dial(&self, target: &ServerAddress, timeout: Duration) -> Result<TcpStream>;

    /// Close the listener. Existing connections are not affected.
    async fn close(&self) -> Result<()>;
}

/// TCP-based stream layer.
pub struct TcpStreamLayer {
    listener: TcpListener,
}

impl TcpStreamLayer {
    pub async fn bind<A: ToSocketAddrs + Send>(addr: A) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(addr).await.map_err(io_to_raft)?;
        Ok(Arc::new(TcpStreamLayer { listener }))
    }
}

#[async_trait]
impl StreamLayer for TcpStreamLayer {
    fn local_addr(&self) -> Result<ServerAddress> {
        self.listener
            .local_addr()
            .map(|a| a.to_string())
            .map_err(io_to_raft)
    }

    async fn accept(&self) -> Result<TcpStream> {
        let (stream, _peer) = self.listener.accept().await.map_err(io_to_raft)?;
        Ok(stream)
    }

    async fn dial(&self, target: &ServerAddress, timeout: Duration) -> Result<TcpStream> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(target.as_str()))
            .await
            .map_err(|_| RaftError::Timeout)?
            .map_err(|e| RaftError::TransportConnect {
                address: target.clone(),
                source: e,
            })?;
        Ok(stream)
    }

    async fn close(&self) -> Result<()> {
        // Dropping the listener closes the socket.
        Ok(())
    }
}

/// Optional override: returns the dial address for a given server ID.
/// Mirrors the `ServerAddressProvider` interface.
pub trait ServerAddressProvider: Send + Sync + 'static {
    fn server_addr(&self, id: &ServerID) -> Result<ServerAddress>;
}

/// Configuration for a [`NetworkTransport`]. Mirrors `NetworkTransportConfig`.
pub struct NetworkTransportConfig {
    pub stream: Arc<dyn StreamLayer>,
    pub max_pool: usize,
    pub max_rpcs_in_flight: usize,
    pub timeout: Duration,
    pub server_address_provider: Option<Arc<dyn ServerAddressProvider>>,
}

impl NetworkTransportConfig {
    pub fn new(stream: Arc<dyn StreamLayer>, max_pool: usize, timeout: Duration) -> Self {
        NetworkTransportConfig {
            stream,
            max_pool,
            max_rpcs_in_flight: DEFAULT_MAX_RPCS_IN_FLIGHT,
            timeout,
            server_address_provider: None,
        }
    }
}

/// Async reader for an AppendEntries response on a pool connection. The
/// decoder task awaits this; we don't need a sync bridge.
async fn read_append_entries_response(conn: &mut PooledConn) -> Result<AppendEntriesResponse> {
    let (err, resp) = read_framed_response(conn).await?;
    if let Some(msg) = err {
        return Err(RaftError::Other(msg));
    }
    resp.ok_or_else(|| RaftError::Other("missing response body".into()))
}

// ---------------------------------------------------------------------------
// Pooled outbound connection.
// ---------------------------------------------------------------------------

/// A pooled outbound TCP stream. Mirrors the `netConn` in net_transport.go.
struct PooledConn {
    stream: Option<TcpStream>,
    #[allow(dead_code)]
    target: ServerAddress,
}

impl PooledConn {
    fn new(stream: TcpStream, target: ServerAddress) -> Self {
        PooledConn {
            stream: Some(stream),
            target,
        }
    }

    fn close_inner(&mut self) {
        // Drop the stream; tokio TcpStream's Drop closes the
        // underlying socket. No async shutdown required.
        let _ = self.stream.take();
    }
}

// ---------------------------------------------------------------------------
// NetworkTransport
// ---------------------------------------------------------------------------

/// Shared network transport. Mirrors `NetworkTransport`.
pub struct NetworkTransport {
    consumer_tx: mpsc::Sender<RPC>,
    consumer_rx: Mutex<Option<mpsc::Receiver<RPC>>>,

    stream: Arc<dyn StreamLayer>,
    max_pool: usize,
    max_in_flight: usize,
    timeout: Duration,
    #[allow(dead_code)]
    timeout_scale: usize,

    conn_pool: Mutex<HashMap<ServerAddress, Vec<Arc<AsyncMutex<PooledConn>>>>>,
    server_address_provider: Option<Arc<dyn ServerAddressProvider>>,

    heartbeat_fn: Mutex<Option<Arc<HeartbeatHandler>>>,
    shutdown: Arc<AtomicBool>,
}

impl NetworkTransport {
    /// Constructs a new [`NetworkTransport`] from a config struct. Spawns
    /// the listener task; the transport is fully operational once this
    /// returns. Mirrors `NewNetworkTransportWithConfig`.
    pub fn new(config: NetworkTransportConfig) -> Result<Arc<Self>> {
        let (consumer_tx, consumer_rx) = mpsc::channel(CONSUMER_CAPACITY);

        let max_in_flight = if config.max_rpcs_in_flight == 0 {
            DEFAULT_MAX_RPCS_IN_FLIGHT
        } else {
            config.max_rpcs_in_flight
        };

        let trans = Arc::new(NetworkTransport {
            consumer_tx,
            consumer_rx: Mutex::new(Some(consumer_rx)),
            stream: Arc::clone(&config.stream),
            max_pool: config.max_pool,
            max_in_flight,
            timeout: config.timeout,
            timeout_scale: DEFAULT_TIMEOUT_SCALE,
            conn_pool: Mutex::new(HashMap::new()),
            server_address_provider: config.server_address_provider,
            heartbeat_fn: Mutex::new(None),
            shutdown: Arc::new(AtomicBool::new(false)),
        });

        // Spawn the listener loop.
        let listener_trans = Arc::clone(&trans);
        tokio::spawn(async move {
            listener_trans.listen_loop().await;
        });

        Ok(trans)
    }

    fn resolve_address(&self, id: &ServerID, target: &ServerAddress) -> ServerAddress {
        if let Some(provider) = &self.server_address_provider {
            match provider.server_addr(id) {
                Ok(addr) => return addr,
                Err(e) => tracing::warn!(
                    server_id = %id,
                    fallback = %target,
                    error = %e,
                    "server address provider failed; falling back to target",
                ),
            }
        }
        target.clone()
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    fn get_pooled(&self, target: &ServerAddress) -> Option<Arc<AsyncMutex<PooledConn>>> {
        let mut pool = self.conn_pool.lock();
        if let Some(conns) = pool.get_mut(target) {
            if !conns.is_empty() {
                return Some(conns.remove(0));
            }
        }
        None
    }

    fn return_to_pool(&self, conn: Arc<AsyncMutex<PooledConn>>, target: ServerAddress) {
        let mut pool = self.conn_pool.lock();
        let entry = pool.entry(target).or_default();
        if entry.len() < self.max_pool && !self.is_shutdown() {
            entry.push(conn);
        } else {
            drop(conn);
        }
    }

    /// Acquires a usable connection to `target`. Mirrors `getConn`.
    async fn get_conn(
        &self,
        id: &ServerID,
        target: &ServerAddress,
    ) -> Result<(Arc<AsyncMutex<PooledConn>>, ServerAddress)> {
        let address = self.resolve_address(id, target);
        if let Some(conn) = self.get_pooled(&address) {
            return Ok((conn, address));
        }
        let raw = self.stream.dial(&address, self.timeout).await?;
        let conn = Arc::new(AsyncMutex::new(PooledConn::new(raw, address.clone())));
        Ok((conn, address))
    }

    /// Closes all pooled connections. Mirrors `CloseStreams`.
    pub async fn close_streams(&self) {
        let drained: Vec<_> = {
            let mut pool = self.conn_pool.lock();
            pool.drain()
                .flat_map(|(_, v)| v.into_iter())
                .collect::<Vec<_>>()
        };
        for conn in drained {
            conn.lock().await.close_inner();
        }
    }

    /// Spawns one task per inbound connection that decodes commands and
    /// writes back responses until either side closes. Mirrors the Go
    /// `listen`/`handleConn` pair.
    async fn listen_loop(self: Arc<Self>) {
        loop {
            if self.is_shutdown() {
                return;
            }
            let accept = self.stream.accept();
            let result = accept.await;
            match result {
                Ok(conn) => {
                    let trans = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Err(e) = trans.handle_conn(conn).await {
                            tracing::debug!(error = %e, "connection handler exited");
                        }
                    });
                }
                Err(e) => {
                    if !self.is_shutdown() {
                        tracing::error!(error = %e, "failed to accept connection");
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    /// Handles a single inbound connection. Mirrors `handleConn`.
    async fn handle_conn(self: Arc<Self>, conn: TcpStream) -> Result<()> {
        // Split into owned halves so the SnapshotStream can own the read
        // side without tying it to a borrowed connection.
        let (reader, writer) = conn.into_split();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));
        // The InstallSnapshot branch consumes the read half. Run it
        // outside the main loop so the borrow checker is happy.
        self.handle_conn_loop(reader, Arc::clone(&writer)).await
    }

    async fn handle_conn_loop(
        self: Arc<Self>,
        mut reader: tokio::net::tcp::OwnedReadHalf,
        writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    ) -> Result<()> {
        loop {
            if self.is_shutdown() {
                return Ok(());
            }
            // 1. Read the RPC type byte.
            let rpc_type = match read_byte(&mut reader).await {
                Ok(b) => b,
                Err(_) => return Ok(()), // EOF or error => close
            };

            // 2. Decode and dispatch the request.
            let command = match rpc_type {
                RPC_APPEND_ENTRIES => {
                    let req: AppendEntriesRequest = read_framed_body(&mut reader).await?;
                    RPCCommand::AppendEntries(req)
                }
                RPC_REQUEST_VOTE => {
                    let req: RequestVoteRequest = read_framed_body(&mut reader).await?;
                    RPCCommand::RequestVote(req)
                }
                RPC_REQUEST_PRE_VOTE => {
                    let req: RequestPreVoteRequest = read_framed_body(&mut reader).await?;
                    RPCCommand::RequestPreVote(req)
                }
                RPC_TIMEOUT_NOW => {
                    let req: TimeoutNowRequest = read_framed_body(&mut reader).await?;
                    RPCCommand::TimeoutNow(req)
                }
                RPC_INSTALL_SNAPSHOT => {
                    // InstallSnapshot owns the rest of the connection; we
                    // delegate to a separate code path.
                    return self.handle_install_snapshot(reader, writer).await;
                }
                other => {
                    return Err(RaftError::Other(format!("unknown rpc type {}", other)));
                }
            };

            // 3. Heartbeat fast-path: a heartbeat is an AppendEntries with
            //    no entries, no commit and no prev-log fields.
            let is_heartbeat = matches!(&command,
                RPCCommand::AppendEntries(req)
                    if req.prev_log_entry == 0
                        && req.prev_log_term == 0
                        && req.entries.is_empty()
                        && req.leader_commit_index == 0
            );

            if is_heartbeat {
                let cb = self.heartbeat_fn.lock().as_ref().cloned();
                if let Some(cb) = cb {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    let rpc = RPC::new(command, None, resp_tx);
                    cb(rpc);
                    let response = match resp_rx.await {
                        Ok(r) => r,
                        Err(_) => return Ok(()),
                    };
                    write_framed_response(Arc::clone(&writer), response).await?;
                    continue;
                }
            }

            // 4. Normal dispatch: push the RPC onto the consumer channel.
            let (resp_tx, resp_rx) = oneshot::channel();
            let to_send = RPC::new(command, None, resp_tx);
            if self.consumer_tx.send(to_send).await.is_err() {
                return Ok(());
            }
            let response = match resp_rx.await {
                Ok(r) => r,
                Err(_) => return Ok(()),
            };
            write_framed_response(Arc::clone(&writer), response).await?;
        }
    }

    async fn handle_install_snapshot(
        self: Arc<Self>,
        mut reader: tokio::net::tcp::OwnedReadHalf,
        writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    ) -> Result<()> {
        let req: InstallSnapshotRequest = read_framed_body(&mut reader).await?;
        let size = req.size;
        let command = RPCCommand::InstallSnapshot(req);
        // Drain the snapshot bytes up front so the FSM can read
        // synchronously.
        let bytes = SnapshotStream::read_all(reader, size).await?;
        let reader_arc: SnapshotReader = Box::new(std::io::Cursor::new(bytes));

        let (resp_tx, resp_rx) = oneshot::channel();
        let to_send = RPC::new(command, Some(reader_arc), resp_tx);
        if self.consumer_tx.send(to_send).await.is_err() {
            return Ok(());
        }
        let response = match resp_rx.await {
            Ok(r) => r,
            Err(_) => return Ok(()),
        };
        write_framed_response(Arc::clone(&writer), response).await?;
        Ok(())
    }
}

impl NetworkTransport {
    /// Sends a single request/response RPC. Mirrors `genericRPC`.
    async fn generic_rpc<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        rpc_type: u8,
        args: &Req,
    ) -> Result<(Resp, Arc<AsyncMutex<PooledConn>>, ServerAddress)> {
        let (conn, target) = self.get_conn(id, target).await?;
        {
            let mut guard = conn.lock().await;
            write_framed_request(&mut guard, rpc_type, args).await?;
        }
        let resp = {
            let mut guard = conn.lock().await;
            let (err, resp) = read_framed_response::<Resp>(&mut guard).await?;
            if let Some(msg) = err {
                return Err(RaftError::Other(msg));
            }
            resp.ok_or_else(|| RaftError::Other("missing response body".into()))?
        };
        Ok((resp, conn, target))
    }
}

#[async_trait]
impl Transport for NetworkTransport {
    fn consumer(&self) -> mpsc::Receiver<RPC> {
        self.consumer_rx
            .lock()
            .take()
            .expect("consumer may only be taken once")
    }

    fn local_addr(&self) -> ServerAddress {
        self.stream
            .local_addr()
            .unwrap_or_else(|_| "<unbound>".to_string())
    }

    async fn append_entries_pipeline(
        &self,
        id: &ServerID,
        target: &ServerAddress,
    ) -> Result<Box<dyn AppendPipeline>> {
        if self.max_in_flight < MIN_IN_FLIGHT_FOR_PIPELINING {
            return Err(RaftError::PipelineReplicationNotSupported);
        }
        let (conn, _addr) = self.get_conn(id, target).await?;
        Ok(NetPipelineHandle::boxed(conn, self.timeout))
    }

    async fn append_entries(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let (resp, conn, target) = self
            .generic_rpc(id, target, RPC_APPEND_ENTRIES, args)
            .await?;
        self.return_to_pool(conn, target);
        Ok(resp)
    }

    async fn request_vote(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        let (resp, conn, target) = self.generic_rpc(id, target, RPC_REQUEST_VOTE, args).await?;
        self.return_to_pool(conn, target);
        Ok(resp)
    }

    async fn request_pre_vote(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &RequestPreVoteRequest,
    ) -> Result<RequestPreVoteResponse> {
        let (resp, conn, target) = self
            .generic_rpc(id, target, RPC_REQUEST_PRE_VOTE, args)
            .await?;
        self.return_to_pool(conn, target);
        Ok(resp)
    }

    async fn install_snapshot(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &InstallSnapshotRequest,
        mut data: SnapshotReader,
    ) -> Result<InstallSnapshotResponse> {
        // InstallSnapshot always uses a fresh, exclusive connection; never
        // pooled, and closed on completion. Mirrors the Go behavior.
        let (conn, _addr) = self.get_conn(id, target).await?;
        let mut guard = conn.lock().await;
        write_framed_request(&mut guard, RPC_INSTALL_SNAPSHOT, args).await?;
        // Stream snapshot data into the connection.
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = data.read(&mut buf)?;
            if n == 0 {
                break;
            }
            if let Some(stream) = guard.stream.as_mut() {
                stream.write_all(&buf[..n]).await.map_err(io_to_raft)?;
            }
        }
        if let Some(stream) = guard.stream.as_mut() {
            stream.flush().await.map_err(io_to_raft)?;
        }
        let (err, resp) = read_framed_response::<InstallSnapshotResponse>(&mut guard).await?;
        drop(guard);
        // Always close the snapshot connection; it is not pooled.
        conn.lock().await.close_inner();
        if let Some(msg) = err {
            return Err(RaftError::Other(msg));
        }
        resp.ok_or_else(|| RaftError::Other("missing install-snapshot response".into()))
    }

    async fn timeout_now(
        &self,
        id: &ServerID,
        target: &ServerAddress,
        args: &TimeoutNowRequest,
    ) -> Result<TimeoutNowResponse> {
        let (resp, conn, target) = self.generic_rpc(id, target, RPC_TIMEOUT_NOW, args).await?;
        self.return_to_pool(conn, target);
        Ok(resp)
    }

    fn encode_peer(&self, id: &ServerID, addr: &ServerAddress) -> Vec<u8> {
        self.resolve_address(id, addr).into_bytes()
    }

    fn decode_peer(&self, buf: &[u8]) -> ServerAddress {
        String::from_utf8_lossy(buf).into_owned()
    }

    fn set_heartbeat_handler(&self, cb: HeartbeatHandler) {
        *self.heartbeat_fn.lock() = Some(Arc::new(cb));
    }

    async fn close(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.stream.close().await;
        self.close_streams().await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Wire framing helpers.
// ---------------------------------------------------------------------------

fn io_to_raft(e: io::Error) -> RaftError {
    RaftError::Io(e)
}

/// Reads a single byte from `r`.
async fn read_byte<R>(r: &mut R) -> Result<u8>
where
    R: AsyncReadExt + Unpin,
{
    let mut buf = [0u8; 1];
    match r.read(&mut buf).await {
        Ok(0) => Err(RaftError::Other("connection closed".into())),
        Ok(_) => Ok(buf[0]),
        Err(e) => Err(io_to_raft(e)),
    }
}

/// Reads a length-prefixed body. Mirrors msgpack framing used elsewhere in
/// the Go codebase. The wire format is a 4-byte big-endian length
/// followed by exactly that many bytes of body.
async fn read_framed_bytes<R>(r: &mut R) -> Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    if r.read_exact(&mut len_buf).await.is_err() {
        return Err(RaftError::Other("connection closed".into()));
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    if r.read_exact(&mut body).await.is_err() {
        return Err(RaftError::Other("connection closed".into()));
    }
    Ok(body)
}

/// Reads a length-prefixed msgpack body and deserializes it.
async fn read_framed_body<R, T>(r: &mut R) -> Result<T>
where
    R: AsyncReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let body = read_framed_bytes(r).await?;
    rmp_serde::from_slice(&body).map_err(|e| RaftError::Decode(e.to_string()))
}

/// Writes a length-prefixed msgpack body.
async fn write_framed_bytes<W>(w: &mut W, body: &[u8]) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let len = (body.len() as u32).to_be_bytes();
    w.write_all(&len).await.map_err(io_to_raft)?;
    w.write_all(body).await.map_err(io_to_raft)?;
    w.flush().await.map_err(io_to_raft)?;
    Ok(())
}

/// Reads a response frame: msgpack-encoded error string followed by a
/// msgpack-encoded response body. Returns `(err, body)` where `err` is
/// `Some(_)` if the peer reported an error and `body` is `None` when
/// there was no response object (e.g. the peer was shut down).
async fn read_framed_response<Resp: serde::de::DeserializeOwned>(
    conn: &mut PooledConn,
) -> Result<(Option<String>, Option<Resp>)> {
    let stream = conn
        .stream
        .as_mut()
        .ok_or_else(|| RaftError::Other("connection closed".into()))?;
    let err_bytes = read_framed_bytes(stream).await?;
    let resp_bytes = read_framed_bytes(stream).await?;
    let err: String =
        rmp_serde::from_slice(&err_bytes).map_err(|e| RaftError::Decode(e.to_string()))?;
    let resp: Option<Resp> = if resp_bytes.is_empty() {
        None
    } else {
        Some(rmp_serde::from_slice(&resp_bytes).map_err(|e| RaftError::Decode(e.to_string()))?)
    };
    let err = if err.is_empty() { None } else { Some(err) };
    Ok((err, resp))
}

/// Writes the request frame for a typed RPC.
async fn write_framed_request<Req: serde::Serialize>(
    conn: &mut PooledConn,
    rpc_type: u8,
    args: &Req,
) -> Result<()> {
    let stream = conn
        .stream
        .as_mut()
        .ok_or_else(|| RaftError::Other("connection closed".into()))?;
    stream.write_u8(rpc_type).await.map_err(io_to_raft)?;
    let body = rmp_serde::to_vec_named(args)?;
    write_framed_bytes(stream, &body).await
}

/// Writes a response frame back to the client. Mirrors the Go
/// `enc.Encode(respErr); enc.Encode(resp.Response)` sequence.
async fn write_framed_response<W>(
    writer: Arc<tokio::sync::Mutex<W>>,
    response: Result<RPCResponse>,
) -> Result<()>
where
    W: tokio::io::AsyncWriteExt + Unpin + Send,
{
    let (err_msg, body) = match response {
        Ok(resp) => (None, Some(resp)),
        Err(e) => (Some(e.to_string()), None),
    };
    let mut writer = writer.lock().await;
    let err_bytes = rmp_serde::to_vec_named(&err_msg.unwrap_or_default())
        .map_err(|e| RaftError::Encode(e.to_string()))?;
    write_framed_bytes(&mut *writer, &err_bytes).await?;
    let resp_bytes = match body {
        Some(resp) => {
            rmp_serde::to_vec_named(&resp).map_err(|e| RaftError::Encode(e.to_string()))?
        }
        None => Vec::new(),
    };
    write_framed_bytes(&mut *writer, &resp_bytes).await
}

// ---------------------------------------------------------------------------
// Snapshot streaming.
// ---------------------------------------------------------------------------

/// Reads the snapshot data portion of an InstallSnapshot request into a
/// `Vec<u8>` and returns a `Cursor` wrapping it. We deliberately avoid a
/// sync/async bridge: the FSM reads snapshots synchronously, so we drain
/// the connection up front on the tokio worker thread.
pub struct SnapshotStream;

impl SnapshotStream {
    pub async fn read_all<R>(mut inner: R, size: u64) -> io::Result<Vec<u8>>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut buf = Vec::with_capacity(size as usize);
        let mut tmp = [0u8; 64 * 1024];
        let mut remaining = size;
        while remaining > 0 {
            let want = std::cmp::min(remaining, tmp.len() as u64) as usize;
            let n = inner.read(&mut tmp[..want]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "snapshot stream closed early",
                ));
            }
            buf.extend_from_slice(&tmp[..n]);
            remaining -= n as u64;
        }
        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Pipelined AppendEntries.
// ---------------------------------------------------------------------------

/// Shared pipeline state. Mirrors the Go `netPipeline`.
struct NetPipeline {
    conn: Arc<AsyncMutex<PooledConn>>,
    #[allow(dead_code)]
    timeout: Duration,
    inprogress_tx: mpsc::Sender<Inflight>,
    inprogress_rx: Mutex<Option<mpsc::Receiver<Inflight>>>,
    done_tx: mpsc::Sender<AppendFuture>,
    done_rx: Mutex<Option<mpsc::Receiver<AppendFuture>>>,
    shutdown: Arc<AtomicBool>,
}

struct Inflight {
    future: AppendFuture,
    responder: AppendFutureResponder,
    _args: AppendEntriesRequest,
}

impl NetPipeline {
    fn new(conn: Arc<AsyncMutex<PooledConn>>, timeout: Duration) -> Arc<Self> {
        let (inprogress_tx, inprogress_rx) = mpsc::channel(PIPELINE_CAPACITY);
        let (done_tx, done_rx) = mpsc::channel(PIPELINE_CAPACITY);
        let pipeline = Arc::new(NetPipeline {
            conn,
            timeout,
            inprogress_tx,
            inprogress_rx: Mutex::new(Some(inprogress_rx)),
            done_tx,
            done_rx: Mutex::new(Some(done_rx)),
            shutdown: Arc::new(AtomicBool::new(false)),
        });

        let decoder = Arc::clone(&pipeline);
        tokio::spawn(async move {
            decoder.run_decoder().await;
        });

        pipeline
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    async fn run_decoder(self: Arc<Self>) {
        let mut inprogress_rx = match self.inprogress_rx.lock().take() {
            Some(rx) => rx,
            None => return,
        };
        while let Some(inflight) = inprogress_rx.recv().await {
            let result = {
                let mut conn = self.conn.lock().await;
                read_append_entries_response(&mut conn).await
            };
            inflight.responder.respond(result);
            if self.done_tx.send(inflight.future).await.is_err() {
                return;
            }
        }
    }
}

/// Handle returned to the raft main loop. Mirrors the public surface of
/// the Go `netPipeline`.
pub struct NetPipelineHandle {
    pipeline: Arc<NetPipeline>,
}

impl NetPipelineHandle {
    fn boxed(conn: Arc<AsyncMutex<PooledConn>>, timeout: Duration) -> Box<dyn AppendPipeline> {
        Box::new(NetPipelineHandle {
            pipeline: NetPipeline::new(conn, timeout),
        })
    }
}

#[async_trait]
impl AppendPipeline for NetPipelineHandle {
    async fn append_entries(&self, args: AppendEntriesRequest) -> Result<AppendFuture> {
        if self.pipeline.is_shutdown() {
            return Err(RaftError::PipelineShutdown);
        }

        let (future, responder) = AppendFuture::new(args.clone());

        // Write the request through the same connection the decoder uses.
        {
            let mut conn = self.pipeline.conn.lock().await;
            write_framed_request(&mut conn, RPC_APPEND_ENTRIES, &args).await?;
        }

        // Hand the future to the decoder.
        let inflight = Inflight {
            future: future.clone(),
            responder,
            _args: args,
        };
        if self.pipeline.inprogress_tx.send(inflight).await.is_err() {
            return Err(RaftError::PipelineShutdown);
        }
        Ok(future)
    }

    fn consumer(&self) -> mpsc::Receiver<AppendFuture> {
        self.pipeline
            .done_rx
            .lock()
            .take()
            .expect("pipeline consumer may only be taken once")
    }

    async fn close(&self) -> Result<()> {
        if !self.pipeline.shutdown.swap(true, Ordering::SeqCst) {
            self.pipeline.conn.lock().await.close_inner();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::RPCHeader;

    fn header() -> RPCHeader {
        RPCHeader {
            protocol_version: 3,
            id: "node-1".into(),
            addr: "addr".into(),
        }
    }

    fn append_entries_req() -> AppendEntriesRequest {
        AppendEntriesRequest {
            header: header(),
            term: 10,
            prev_log_entry: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit_index: 0,
        }
    }

    fn append_entries_resp() -> RPCResponse {
        RPCResponse::AppendEntries(AppendEntriesResponse {
            header: header(),
            term: 10,
            last_log: 0,
            success: true,
            no_retry_backoff: false,
        })
    }

    async fn spawn_echo_server(trans: Arc<NetworkTransport>, reply: RPCResponse) {
        let mut consumer = trans.consumer();
        tokio::spawn(async move {
            while let Some(rpc) = consumer.recv().await {
                let (_cmd, _reader, responder) = rpc.split();
                responder.respond(reply.clone());
            }
        });
    }

    async fn make_pair() -> (Arc<NetworkTransport>, ServerAddress, Arc<NetworkTransport>) {
        let layer1 = TcpStreamLayer::bind("127.0.0.1:0").await.unwrap();
        let _local1 = layer1.local_addr().unwrap();
        let layer2 = TcpStreamLayer::bind("127.0.0.1:0").await.unwrap();
        let local2 = layer2.local_addr().unwrap();
        let t1 = NetworkTransport::new(NetworkTransportConfig::new(
            layer1,
            2,
            Duration::from_secs(2),
        ))
        .unwrap();
        let t2 = NetworkTransport::new(NetworkTransportConfig::new(
            layer2,
            2,
            Duration::from_secs(2),
        ))
        .unwrap();
        (t1, local2, t2)
    }

    #[tokio::test]
    async fn tcp_append_entries_roundtrip() {
        let (t1, addr2, t2) = make_pair().await;
        spawn_echo_server(Arc::clone(&t2), append_entries_resp()).await;
        let resp = t1
            .append_entries(&"id".into(), &addr2, &append_entries_req())
            .await
            .unwrap();
        assert!(resp.success);
    }

    #[tokio::test]
    async fn tcp_request_vote_roundtrip() {
        let (t1, addr2, t2) = make_pair().await;
        let reply = RPCResponse::RequestVote(RequestVoteResponse {
            header: header(),
            term: 10,
            granted: true,
        });
        spawn_echo_server(Arc::clone(&t2), reply).await;
        let resp = t1
            .request_vote(
                &"id".into(),
                &addr2,
                &RequestVoteRequest {
                    header: header(),
                    term: 10,
                    last_log_index: 1,
                    last_log_term: 1,
                    leadership_transfer: false,
                },
            )
            .await
            .unwrap();
        assert!(resp.granted);
    }

    #[tokio::test]
    async fn tcp_install_snapshot_roundtrip() {
        let (t1, addr2, t2) = make_pair().await;
        let reply = RPCResponse::InstallSnapshot(InstallSnapshotResponse {
            header: header(),
            term: 10,
            success: true,
        });
        spawn_echo_server(Arc::clone(&t2), reply).await;
        let data: SnapshotReader = Box::new(std::io::Cursor::new(b"hello".to_vec()));
        let resp = t1
            .install_snapshot(
                &"id".into(),
                &addr2,
                &InstallSnapshotRequest {
                    header: header(),
                    snapshot_version: 1,
                    term: 10,
                    last_log_index: 100,
                    last_log_term: 4,
                    configuration: Vec::new(),
                    configuration_index: 1,
                    size: 5,
                },
                data,
            )
            .await
            .unwrap();
        assert!(resp.success);
    }

    #[tokio::test]
    async fn tcp_timeout_now_roundtrip() {
        let (t1, addr2, t2) = make_pair().await;
        let reply = RPCResponse::TimeoutNow(TimeoutNowResponse { header: header() });
        spawn_echo_server(Arc::clone(&t2), reply).await;
        t1.timeout_now(
            &"id".into(),
            &addr2,
            &TimeoutNowRequest { header: header() },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn tcp_append_entries_pipeline_roundtrip() {
        let (t1, addr2, t2) = make_pair().await;
        spawn_echo_server(Arc::clone(&t2), append_entries_resp()).await;
        let pipeline = t1
            .append_entries_pipeline(&"id".into(), &addr2)
            .await
            .unwrap();
        let future = pipeline.append_entries(append_entries_req()).await.unwrap();
        let resp = future.wait().await.unwrap();
        assert!(resp.success);

        let mut done = pipeline.consumer();
        let ready = done.recv().await.unwrap();
        let resp = ready.wait().await.unwrap();
        assert!(resp.success);

        pipeline.close().await.unwrap();
    }

    #[tokio::test]
    async fn tcp_encode_decode_peer() {
        let (t1, _addr2, _t2) = make_pair().await;
        let id: ServerID = "id".into();
        let addr: ServerAddress = "addr".into();
        let encoded = t1.encode_peer(&id, &addr);
        assert_eq!(t1.decode_peer(&encoded), addr);
    }
}
