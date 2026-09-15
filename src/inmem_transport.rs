use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot, watch};

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

/// Default timeout for in-memory RPCs, matching the Go implementation.
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(500);

/// Capacity of the consumer and pipeline channels, matching the Go
/// implementation.
const CHANNEL_CAPACITY: usize = 16;

/// Returns a new in-memory address with a randomly generated UUID,
/// mirroring `NewInmemAddr`.
pub fn new_inmem_addr() -> ServerAddress {
    use rand::Rng;
    let mut buf = [0u8; 16];
    rand::thread_rng().fill(&mut buf);
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
        buf[11], buf[12], buf[13], buf[14], buf[15]
    )
}

/// Initializes a new in-memory transport with the default 500ms timeout,
/// generating a random local address if none is given. Mirrors
/// `NewInmemTransport`.
pub fn new_inmem_transport(addr: ServerAddress) -> (ServerAddress, Arc<InmemTransport>) {
    new_inmem_transport_with_timeout(addr, DEFAULT_TIMEOUT)
}

/// Initializes a new in-memory transport. The timeout decides how long to
/// wait for a connected peer to process the RPCs sent to it. Mirrors
/// `NewInmemTransportWithTimeout`.
pub fn new_inmem_transport_with_timeout(
    addr: ServerAddress,
    timeout: Duration,
) -> (ServerAddress, Arc<InmemTransport>) {
    let addr = if addr.is_empty() {
        new_inmem_addr()
    } else {
        addr
    };
    let (consumer_tx, consumer_rx) = mpsc::channel(CHANNEL_CAPACITY);
    (
        addr.clone(),
        Arc::new(InmemTransport {
            consumer_tx,
            consumer_rx: Mutex::new(Some(consumer_rx)),
            local_addr: addr,
            state: RwLock::new(InmemTransportState::default()),
            timeout,
        }),
    )
}

#[derive(Default)]
struct InmemTransportState {
    peers: HashMap<ServerAddress, Arc<InmemTransport>>,
    pipelines: Vec<Arc<InmemPipeline>>,
}

/// Implements the [`Transport`] interface in memory, to allow raft to be
/// tested without going over a network. Mirrors the Go `InmemTransport`.
pub struct InmemTransport {
    consumer_tx: mpsc::Sender<RPC>,
    consumer_rx: Mutex<Option<mpsc::Receiver<RPC>>>,
    local_addr: ServerAddress,
    state: RwLock<InmemTransportState>,
    timeout: Duration,
}

impl InmemTransport {
    /// Sends an RPC to the target peer and waits for the response, with a
    /// timeout on both the send and the wait. Mirrors `makeRPC`.
    async fn make_rpc(
        &self,
        target: &ServerAddress,
        command: RPCCommand,
        reader: Option<SnapshotReader>,
        timeout: Duration,
    ) -> Result<RPCResponse> {
        let peer = self
            .state
            .read()
            .peers
            .get(target)
            .cloned()
            .ok_or_else(|| RaftError::Other(format!("failed to connect to peer: {}", target)))?;

        // Send the RPC over.
        let (resp_tx, resp_rx) = oneshot::channel();
        let rpc = RPC::new(command, reader, resp_tx);
        tokio::time::timeout(timeout, peer.consumer_tx.send(rpc))
            .await
            .map_err(|_| RaftError::Other("send timed out".into()))?
            .map_err(|_| RaftError::TransportShutdown)?;

        // Wait for a response.
        match tokio::time::timeout(timeout, resp_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(RaftError::TransportShutdown),
            Err(_) => Err(RaftError::Other("command timed out".into())),
        }
    }

    /// Connects this transport to another transport for a given peer name,
    /// allowing local routing. Mirrors `Connect`.
    pub fn connect(&self, peer: ServerAddress, trans: Arc<InmemTransport>) {
        self.state.write().peers.insert(peer, trans);
    }

    /// Removes the ability to route to a given peer, closing any pipelines
    /// to it. Mirrors `Disconnect`.
    pub fn disconnect(&self, peer: &ServerAddress) {
        let mut state = self.state.write();
        state.peers.remove(peer);

        // Disconnect any pipelines to the peer.
        let mut i = 0;
        while i < state.pipelines.len() {
            if &state.pipelines[i].peer_addr == peer {
                let pipeline = state.pipelines.remove(i);
                pipeline.shutdown();
            } else {
                i += 1;
            }
        }
    }

    /// Removes all routes to peers, possibly to reconnect them later.
    /// Mirrors `DisconnectAll`.
    pub fn disconnect_all(&self) {
        let mut state = self.state.write();
        state.peers.clear();
        for pipeline in state.pipelines.drain(..) {
            pipeline.shutdown();
        }
    }
}

#[async_trait]
impl Transport for InmemTransport {
    fn consumer(&self) -> mpsc::Receiver<RPC> {
        self.consumer_rx
            .lock()
            .take()
            .expect("consumer channel already taken")
    }

    fn local_addr(&self) -> ServerAddress {
        self.local_addr.clone()
    }

    async fn append_entries_pipeline(
        &self,
        _id: &ServerID,
        target: &ServerAddress,
    ) -> Result<Box<dyn AppendPipeline>> {
        let mut state = self.state.write();
        let peer =
            state.peers.get(target).cloned().ok_or_else(|| {
                RaftError::Other(format!("failed to connect to peer: {}", target))
            })?;
        let pipeline = InmemPipeline::new(peer, target.clone(), self.timeout);
        state.pipelines.push(Arc::clone(&pipeline));
        Ok(Box::new(InmemPipelineHandle { pipeline }))
    }

    async fn append_entries(
        &self,
        _id: &ServerID,
        target: &ServerAddress,
        args: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        match self
            .make_rpc(
                target,
                RPCCommand::AppendEntries(args.clone()),
                None,
                self.timeout,
            )
            .await?
        {
            RPCResponse::AppendEntries(resp) => Ok(resp),
            _ => Err(RaftError::Other("unexpected response type".into())),
        }
    }

    async fn request_vote(
        &self,
        _id: &ServerID,
        target: &ServerAddress,
        args: &RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        match self
            .make_rpc(
                target,
                RPCCommand::RequestVote(args.clone()),
                None,
                self.timeout,
            )
            .await?
        {
            RPCResponse::RequestVote(resp) => Ok(resp),
            _ => Err(RaftError::Other("unexpected response type".into())),
        }
    }

    async fn request_pre_vote(
        &self,
        _id: &ServerID,
        target: &ServerAddress,
        args: &RequestPreVoteRequest,
    ) -> Result<RequestPreVoteResponse> {
        match self
            .make_rpc(
                target,
                RPCCommand::RequestPreVote(args.clone()),
                None,
                self.timeout,
            )
            .await?
        {
            RPCResponse::RequestPreVote(resp) => Ok(resp),
            _ => Err(RaftError::Other("unexpected response type".into())),
        }
    }

    async fn install_snapshot(
        &self,
        _id: &ServerID,
        target: &ServerAddress,
        args: &InstallSnapshotRequest,
        data: SnapshotReader,
    ) -> Result<InstallSnapshotResponse> {
        match self
            .make_rpc(
                target,
                RPCCommand::InstallSnapshot(args.clone()),
                Some(data),
                10 * self.timeout,
            )
            .await?
        {
            RPCResponse::InstallSnapshot(resp) => Ok(resp),
            _ => Err(RaftError::Other("unexpected response type".into())),
        }
    }

    async fn timeout_now(
        &self,
        _id: &ServerID,
        target: &ServerAddress,
        args: &TimeoutNowRequest,
    ) -> Result<TimeoutNowResponse> {
        match self
            .make_rpc(
                target,
                RPCCommand::TimeoutNow(args.clone()),
                None,
                10 * self.timeout,
            )
            .await?
        {
            RPCResponse::TimeoutNow(resp) => Ok(resp),
            _ => Err(RaftError::Other("unexpected response type".into())),
        }
    }

    fn encode_peer(&self, _id: &ServerID, addr: &ServerAddress) -> Vec<u8> {
        addr.as_bytes().to_vec()
    }

    fn decode_peer(&self, buf: &[u8]) -> ServerAddress {
        String::from_utf8_lossy(buf).into_owned()
    }

    /// Optional fast-path for heartbeats; not supported by this transport,
    /// mirroring the Go implementation.
    fn set_heartbeat_handler(&self, _cb: HeartbeatHandler) {}

    async fn close(&self) -> Result<()> {
        self.disconnect_all();
        Ok(())
    }
}

/// One in-flight pipelined RPC, waiting for its response.
struct InmemPipelineInflight {
    future: AppendFuture,
    responder: AppendFutureResponder,
    resp_rx: oneshot::Receiver<Result<RPCResponse>>,
}

/// Shared pipeline state. Mirrors the Go `inmemPipeline`.
pub(crate) struct InmemPipeline {
    peer: Arc<InmemTransport>,
    peer_addr: ServerAddress,
    timeout: Duration,
    done_tx: mpsc::Sender<AppendFuture>,
    done_rx: Mutex<Option<mpsc::Receiver<AppendFuture>>>,
    inprogress_tx: mpsc::Sender<InmemPipelineInflight>,
    shutdown_tx: watch::Sender<bool>,
}

impl InmemPipeline {
    fn new(peer: Arc<InmemTransport>, peer_addr: ServerAddress, timeout: Duration) -> Arc<Self> {
        let (done_tx, done_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (inprogress_tx, inprogress_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (shutdown_tx, _) = watch::channel(false);
        let pipeline = Arc::new(InmemPipeline {
            peer,
            peer_addr,
            timeout,
            done_tx,
            done_rx: Mutex::new(Some(done_rx)),
            inprogress_tx,
            shutdown_tx,
        });
        tokio::spawn(decode_responses(Arc::clone(&pipeline), inprogress_rx));
        pipeline
    }

    /// Permanently shuts the pipeline down. Idempotent, mirroring the
    /// shutdown flag + closed channel of the Go implementation.
    fn shutdown(&self) {
        // send_modify updates the value even when no receivers are
        // subscribed, so the flag stays visible to future subscribers.
        self.shutdown_tx.send_modify(|shutdown| *shutdown = true);
    }

    fn is_shutdown(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    /// Resolves once the pipeline is shut down. Race-free even if shutdown
    /// already happened, unlike a plain notify.
    async fn shutdown_wait(&self) {
        let _ = self
            .shutdown_tx
            .subscribe()
            .wait_for(|shutdown| *shutdown)
            .await;
    }
}

/// Waits on in-flight RPCs one at a time, in order, resolving their futures
/// and pushing them onto the done channel. Mirrors `decodeResponses`.
async fn decode_responses(
    pipeline: Arc<InmemPipeline>,
    mut inprogress_rx: mpsc::Receiver<InmemPipelineInflight>,
) {
    loop {
        let inflight = tokio::select! {
            _ = pipeline.shutdown_wait() => return,
            inflight = inprogress_rx.recv() => match inflight {
                Some(inflight) => inflight,
                None => return,
            },
        };

        let result = match tokio::time::timeout(pipeline.timeout, inflight.resp_rx).await {
            Ok(Ok(Ok(RPCResponse::AppendEntries(resp)))) => Ok(resp),
            Ok(Ok(Ok(_))) => Err(RaftError::Other("unexpected response type".into())),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_)) => Err(RaftError::TransportShutdown),
            Err(_) => Err(RaftError::Other("command timed out".into())),
        };
        inflight.responder.respond(result);

        tokio::select! {
            _ = pipeline.shutdown_wait() => return,
            sent = pipeline.done_tx.send(inflight.future) => {
                if sent.is_err() {
                    return;
                }
            }
        }
    }
}

/// Public [`AppendPipeline`] implementation over the shared pipeline state.
struct InmemPipelineHandle {
    pipeline: Arc<InmemPipeline>,
}

#[async_trait]
impl AppendPipeline for InmemPipelineHandle {
    /// Adds another request to the pipeline. Mirrors
    /// `inmemPipeline.AppendEntries`: the request is enqueued on the peer's
    /// consumer channel and the future completes once the peer responds.
    async fn append_entries(&self, args: AppendEntriesRequest) -> Result<AppendFuture> {
        // Check the shutdown flag before racing the channels, so a closed
        // pipeline never enqueues.
        if self.pipeline.is_shutdown() {
            return Err(RaftError::PipelineShutdown);
        }

        let (future, responder) = AppendFuture::new(args.clone());
        let (resp_tx, resp_rx) = oneshot::channel();
        let rpc = RPC::new(RPCCommand::AppendEntries(args), None, resp_tx);

        // Send the RPC over.
        tokio::select! {
            _ = self.pipeline.shutdown_wait() => return Err(RaftError::PipelineShutdown),
            sent = tokio::time::timeout(
                self.pipeline.timeout,
                self.pipeline.peer.consumer_tx.send(rpc),
            ) => {
                match sent {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => return Err(RaftError::TransportShutdown),
                    Err(_) => return Err(RaftError::Other("command enqueue timeout".into())),
                }
            }
        }

        // Hand the response channel to the decoder task.
        let inflight = InmemPipelineInflight {
            future: future.clone(),
            responder,
            resp_rx,
        };
        tokio::select! {
            _ = self.pipeline.shutdown_wait() => Err(RaftError::PipelineShutdown),
            sent = self.pipeline.inprogress_tx.send(inflight) => {
                match sent {
                    Ok(()) => Ok(future),
                    Err(_) => Err(RaftError::PipelineShutdown),
                }
            }
        }
    }

    fn consumer(&self) -> mpsc::Receiver<AppendFuture> {
        self.pipeline
            .done_rx
            .lock()
            .take()
            .expect("pipeline consumer channel already taken")
    }

    async fn close(&self) -> Result<()> {
        self.pipeline.shutdown();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::LogType;
    use crate::transport::RPCHeader;

    fn header() -> RPCHeader {
        RPCHeader {
            protocol_version: 3,
            id: "a".into(),
            addr: "addr-a".into(),
        }
    }

    fn append_entries_req() -> AppendEntriesRequest {
        AppendEntriesRequest {
            header: header(),
            term: 10,
            prev_log_entry: 100,
            prev_log_term: 4,
            entries: vec![crate::log::Log {
                index: 101,
                term: 10,
                log_type: LogType::Command,
                ..Default::default()
            }],
            leader_commit_index: 90,
        }
    }

    /// Receives one RPC and responds to it, mirroring the consumer side of
    /// the Go transport tests.
    async fn respond_to_one(mut consumer: mpsc::Receiver<RPC>) {
        let mut rpc = consumer.recv().await.expect("an RPC arrives");

        if matches!(rpc.command, RPCCommand::InstallSnapshot(_)) {
            // Drain the snapshot data before responding.
            if let Some(mut reader) = rpc.reader.take() {
                let mut buf = Vec::new();
                std::io::Read::read_to_end(&mut reader, &mut buf).unwrap();
                assert_eq!(buf, b"hello");
            }
        }

        if matches!(rpc.command, RPCCommand::AppendEntries(_)) {
            rpc.respond(RPCResponse::AppendEntries(AppendEntriesResponse {
                header: header(),
                term: 10,
                last_log: 100,
                success: true,
                no_retry_backoff: false,
            }));
        } else if matches!(rpc.command, RPCCommand::RequestVote(_)) {
            rpc.respond(RPCResponse::RequestVote(RequestVoteResponse {
                header: header(),
                term: 10,
                granted: true,
            }));
        } else if matches!(rpc.command, RPCCommand::RequestPreVote(_)) {
            rpc.respond(RPCResponse::RequestPreVote(RequestPreVoteResponse {
                header: header(),
                term: 10,
                granted: true,
            }));
        } else if matches!(rpc.command, RPCCommand::InstallSnapshot(_)) {
            rpc.respond(RPCResponse::InstallSnapshot(InstallSnapshotResponse {
                header: header(),
                term: 10,
                success: true,
            }));
        } else {
            rpc.respond(RPCResponse::TimeoutNow(TimeoutNowResponse {
                header: header(),
            }));
        }
    }

    fn connected_pair() -> (Arc<InmemTransport>, ServerAddress, Arc<InmemTransport>) {
        let (addr1, t1) = new_inmem_transport(String::new());
        let (addr2, t2) = new_inmem_transport(String::new());
        t1.connect(addr2.clone(), Arc::clone(&t2));
        t2.connect(addr1, Arc::clone(&t1));
        (t1, addr2, t2)
    }

    // Mirrors the AppendEntries part of TestTransport_AppendEntries.
    #[tokio::test]
    async fn append_entries_roundtrip() {
        let (t1, addr2, t2) = connected_pair();
        let responder = tokio::spawn(respond_to_one(t2.consumer()));

        let resp = t1
            .append_entries(&"a".into(), &addr2, &append_entries_req())
            .await
            .unwrap();
        assert!(resp.success);
        assert_eq!(resp.term, 10);
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn request_vote_roundtrip() {
        let (t1, addr2, t2) = connected_pair();
        tokio::spawn(respond_to_one(t2.consumer()));

        let req = RequestVoteRequest {
            header: header(),
            term: 20,
            last_log_index: 100,
            last_log_term: 4,
            leadership_transfer: false,
        };
        let resp = t1.request_vote(&"a".into(), &addr2, &req).await.unwrap();
        assert!(resp.granted);
        assert_eq!(resp.term, 10);
    }

    #[tokio::test]
    async fn request_pre_vote_roundtrip() {
        let (t1, addr2, t2) = connected_pair();
        tokio::spawn(respond_to_one(t2.consumer()));

        let req = RequestPreVoteRequest {
            header: header(),
            term: 20,
            last_log_index: 100,
            last_log_term: 4,
        };
        let resp = t1
            .request_pre_vote(&"a".into(), &addr2, &req)
            .await
            .unwrap();
        assert!(resp.granted);
    }

    // Mirrors the InstallSnapshot part of TestTransport_InstallSnapshot.
    #[tokio::test]
    async fn install_snapshot_streams_data() {
        let (t1, addr2, t2) = connected_pair();
        tokio::spawn(respond_to_one(t2.consumer()));

        let req = InstallSnapshotRequest {
            header: header(),
            snapshot_version: 1,
            term: 10,
            last_log_index: 100,
            last_log_term: 4,
            configuration: Vec::new(),
            configuration_index: 1,
            size: 5,
        };
        let data: SnapshotReader = Box::new(std::io::Cursor::new(b"hello".to_vec()));
        let resp = t1
            .install_snapshot(&"a".into(), &addr2, &req, data)
            .await
            .unwrap();
        assert!(resp.success);
    }

    #[tokio::test]
    async fn timeout_now_roundtrip() {
        let (t1, addr2, t2) = connected_pair();
        tokio::spawn(respond_to_one(t2.consumer()));
        let req = TimeoutNowRequest { header: header() };
        t1.timeout_now(&"a".into(), &addr2, &req).await.unwrap();
    }

    // Mirrors TestTransport_AppendEntriesPipeline for the inmem transport.
    #[tokio::test]
    async fn append_entries_pipeline_roundtrip() {
        let (t1, addr2, t2) = connected_pair();
        tokio::spawn(respond_to_one(t2.consumer()));

        let pipeline = t1
            .append_entries_pipeline(&"a".into(), &addr2)
            .await
            .unwrap();
        let future = pipeline.append_entries(append_entries_req()).await.unwrap();
        assert_eq!(future.request().term, 10);

        let mut done = pipeline.consumer();
        let ready = done.recv().await.expect("a completed future");
        let resp = ready.wait().await.unwrap();
        assert!(resp.success);

        // The future returned to the caller resolves with the same result.
        let resp = future.wait().await.unwrap();
        assert!(resp.success);

        pipeline.close().await.unwrap();
        assert!(matches!(
            pipeline
                .append_entries(append_entries_req())
                .await
                .unwrap_err(),
            RaftError::PipelineShutdown
        ));
    }

    // Mirrors TestInmemTransportWriteTimeout: a peer that never answers
    // trips the response timeout.
    #[tokio::test]
    async fn times_out_when_peer_not_responding() {
        let (_addr1, t1) =
            new_inmem_transport_with_timeout(String::new(), Duration::from_millis(50));
        let (addr2, t2) = new_inmem_transport(String::new());
        t1.connect(addr2.clone(), t2);
        // Note: the peer's consumer is never taken, so nobody responds.

        for _ in 0..2 {
            let err = t1
                .append_entries(&"a".into(), &addr2, &append_entries_req())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("timed out"));
        }
    }

    #[tokio::test]
    async fn disconnect_blocks_and_connect_restores_routing() {
        let (t1, addr2, t2) = connected_pair();
        t1.disconnect(&addr2);
        let err = t1
            .append_entries(&"a".into(), &addr2, &append_entries_req())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("failed to connect"));

        // Reconnecting restores routing.
        t1.connect(addr2.clone(), Arc::clone(&t2));
        tokio::spawn(respond_to_one(t2.consumer()));
        let resp = t1
            .append_entries(&"a".into(), &addr2, &append_entries_req())
            .await
            .unwrap();
        assert!(resp.success);
    }

    #[tokio::test]
    async fn disconnect_all_closes_pipelines() {
        let (t1, addr2, _t2) = connected_pair();
        let pipeline = t1
            .append_entries_pipeline(&"a".into(), &addr2)
            .await
            .unwrap();
        t1.disconnect_all();
        let err = pipeline
            .append_entries(append_entries_req())
            .await
            .unwrap_err();
        assert!(matches!(err, RaftError::PipelineShutdown));
    }

    #[tokio::test]
    async fn encode_decode_peer() {
        let (_addr, t1) = new_inmem_transport(String::new());
        let id: ServerID = "node-1".into();
        let addr: ServerAddress = "addr-1".into();
        let enc = t1.encode_peer(&id, &addr);
        assert_eq!(t1.decode_peer(&enc), addr);
    }
}
