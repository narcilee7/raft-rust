//! Test harness for cluster-level raft tests: an in-memory cluster with a
//! mock FSM. Ports the essential parts of testing.go from the Go
//! implementation.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use raft::{
    bootstrap_cluster, new_raft, ApplyResponse, Config, Configuration, DiscardSnapshotStore,
    FSMSnapshot, InmemSnapshotStore, InmemStore, InmemTransport, Log, Raft, RaftFuture, RaftState,
    Result, Server, ServerAddress, ServerSuffrage, SnapshotReader, SnapshotSink, SnapshotStore,
    FSM,
};

/// Configuration matching `inmemConfig` in testing.go.
pub fn inmem_config() -> Config {
    Config {
        heartbeat_timeout: Duration::from_millis(50),
        election_timeout: Duration::from_millis(50),
        leader_lease_timeout: Duration::from_millis(50),
        commit_timeout: Duration::from_millis(5),
        ..Default::default()
    }
}

/// An FSM that just stores the logs sequentially. Mirrors `MockFSM`.
#[derive(Default)]
pub struct MockFSM {
    pub logs: Mutex<Vec<Vec<u8>>>,
    pub configurations: Mutex<Vec<Configuration>>,
}

impl MockFSM {
    pub fn new() -> Arc<Self> {
        Arc::new(MockFSM::default())
    }

    pub fn logs(&self) -> Vec<Vec<u8>> {
        self.logs.lock().clone()
    }

    pub fn log_count(&self) -> usize {
        self.logs.lock().len()
    }
}

#[async_trait]
impl FSM for MockFSM {
    async fn apply(&self, log: &Log) -> Result<ApplyResponse> {
        let mut logs = self.logs.lock();
        logs.push(log.data.clone());
        Ok(Box::new(logs.len()))
    }

    async fn snapshot(&self) -> Result<Box<dyn FSMSnapshot>> {
        let logs = self.logs.lock().clone();
        let max_index = logs.len();
        Ok(Box::new(MockSnapshot { logs, max_index }))
    }

    async fn restore(&self, mut snapshot: SnapshotReader) -> Result<()> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut snapshot, &mut buf)?;
        let logs: Vec<Vec<u8>> = rmp_serde::from_slice(&buf)?;
        *self.logs.lock() = logs;
        Ok(())
    }

    fn as_configuration_store(&self) -> Option<&dyn raft::ConfigurationStore> {
        Some(self)
    }
}

#[async_trait]
impl raft::ConfigurationStore for MockFSM {
    async fn store_configuration(&self, _index: u64, configuration: Configuration) {
        self.configurations.lock().push(configuration);
    }
}

/// Mirrors `MockSnapshot`.
struct MockSnapshot {
    logs: Vec<Vec<u8>>,
    max_index: usize,
}

#[async_trait]
impl FSMSnapshot for MockSnapshot {
    async fn persist(&self, mut sink: Box<dyn SnapshotSink>) -> Result<()> {
        let buf = rmp_serde::to_vec(&self.logs[..self.max_index])?;
        sink.write(&buf).await?;
        sink.close().await?;
        Ok(())
    }

    fn release(&self) {}
}

/// A cluster of raft nodes wired together with in-memory transports.
/// Mirrors `cluster` in testing.go.
pub struct Cluster {
    pub conf: Config,
    pub stores: Vec<Arc<InmemStore>>,
    pub fsms: Vec<Arc<MockFSM>>,
    pub snaps: Vec<Arc<dyn SnapshotStore>>,
    pub trans: Vec<Arc<InmemTransport>>,
    pub rafts: Vec<Raft>,
    pub addrs: Vec<ServerAddress>,
    pub longstop_timeout: Duration,
}

/// Snapshot store instance shared with each node of a cluster built via
/// [`make_cluster_with`]. The same handle is cloned into every node so
/// tests can inspect snapshots that any peer created.
pub type SharedSnapshotStore = Arc<dyn SnapshotStore>;

/// Build a cluster where each node uses a discard snapshot store. Used by
/// the bulk of the test suite, which does not exercise snapshot/restore.
pub fn discard_snapshots() -> SharedSnapshotStore {
    Arc::new(DiscardSnapshotStore::new())
}

/// Build a cluster where each node uses an in-memory snapshot store, used
/// by snapshot and restore tests.
pub fn inmem_snapshots() -> SharedSnapshotStore {
    Arc::new(InmemSnapshotStore::new())
}

/// Returns a cluster of `n` peers, bootstrapped if requested, using the
/// discard snapshot store. Mirrors `makeCluster`.
pub async fn make_cluster(n: usize, bootstrap: bool, conf: Option<Config>) -> Cluster {
    make_cluster_with(n, bootstrap, conf, discard_snapshots()).await
}

/// Returns a cluster of `n` peers, bootstrapped if requested, using the
/// supplied snapshot-store factory. Used by snapshot and restore tests to
/// run the cluster with an in-memory store.
pub async fn make_cluster_with(
    n: usize,
    bootstrap: bool,
    conf: Option<Config>,
    shared_snap: SharedSnapshotStore,
) -> Cluster {
    let conf = conf.unwrap_or_else(inmem_config);
    let mut c = Cluster {
        conf,
        stores: Vec::new(),
        fsms: Vec::new(),
        snaps: Vec::new(),
        trans: Vec::new(),
        rafts: Vec::new(),
        addrs: Vec::new(),
        longstop_timeout: Duration::from_secs(5),
    };

    let mut configuration = Configuration::default();

    // Set up the stores and transports.
    for _ in 0..n {
        let store = Arc::new(InmemStore::new());
        c.stores.push(store);
        c.fsms.push(MockFSM::new());
        c.snaps.push(Arc::clone(&shared_snap));

        let (addr, trans) = raft::new_inmem_transport(String::new());
        let local_id = format!("server-{}", addr);
        configuration.servers.push(Server {
            suffrage: ServerSuffrage::Voter,
            id: local_id,
            address: addr.clone(),
        });
        c.addrs.push(addr);
        c.trans.push(trans);
    }

    // Wire the transports together.
    c.fully_connect();

    // Create all the rafts.
    for i in 0..n {
        let mut peer_conf = c.conf.clone();
        peer_conf.local_id = configuration.servers[i].id.clone();

        let logs: Arc<dyn raft::LogStore> = c.stores[i].clone();
        let stable: Arc<dyn raft::StableStore> = c.stores[i].clone();
        let snaps: Arc<dyn raft::SnapshotStore> = c.snaps[i].clone();
        let trans: Arc<dyn raft::Transport> = c.trans[i].clone();

        if bootstrap {
            bootstrap_cluster(
                &peer_conf,
                logs.clone(),
                stable.clone(),
                snaps.clone(),
                configuration.clone(),
            )
            .await
            .expect("bootstrap_cluster failed");
        }

        let raft = new_raft(peer_conf, c.fsms[i].clone(), logs, stable, snaps, trans)
            .await
            .expect("new_raft failed");
        c.rafts.push(raft);
    }

    c
}

impl Cluster {
    /// Polls `cond` until it holds or the longstop timeout expires, then
    /// panics with `what`.
    pub async fn wait_for<F>(&self, what: &str, mut cond: F)
    where
        F: FnMut() -> bool,
    {
        let limit = std::time::Instant::now() + self.longstop_timeout;
        loop {
            if cond() {
                return;
            }
            assert!(
                std::time::Instant::now() < limit,
                "timeout waiting for {}",
                what
            );
            tokio::time::sleep(self.conf.commit_timeout.max(Duration::from_millis(5))).await;
        }
    }

    /// All rafts in the given state.
    pub fn get_in_state(&self, state: RaftState) -> Vec<&Raft> {
        self.rafts.iter().filter(|r| r.state() == state).collect()
    }

    /// Waits for exactly one leader and returns it. Mirrors `Leader`.
    pub async fn leader(&self) -> &Raft {
        self.wait_for("a leader", || {
            self.get_in_state(RaftState::Leader).len() == 1
        })
        .await;
        self.get_in_state(RaftState::Leader)[0]
    }

    /// The followers of the cluster, waiting until a leader exists (n-1
    /// followers). Mirrors `Followers`.
    pub async fn followers(&self) -> Vec<&Raft> {
        let expected = self.rafts.len() - 1;
        self.wait_for("followers", || {
            self.get_in_state(RaftState::Follower).len() == expected
        })
        .await;
        self.get_in_state(RaftState::Follower)
    }

    /// Waits for a leader and checks it is the expected address (if
    /// non-empty). Mirrors `EnsureLeader`.
    pub async fn ensure_leader(&self, expect: &ServerAddress) {
        let leader = self.leader().await;
        if !expect.is_empty() {
            assert_eq!(
                &leader.local_addr(),
                expect,
                "wrong leader: got {}, want {}",
                leader.local_addr(),
                expect
            );
        }
    }

    /// Waits until every FSM has applied exactly `n` logs. Mirrors
    /// `WaitForReplication`.
    pub async fn wait_for_replication(&self, n: usize) {
        self.wait_for(&format!("replication of {} logs", n), || {
            self.fsms.iter().all(|fsm| fsm.log_count() == n)
        })
        .await;
    }

    /// Waits until all FSMs hold identical logs. Mirrors `EnsureSame`.
    pub async fn ensure_same(&self) {
        self.wait_for("identical FSMs", || {
            let first = self.fsms[0].logs();
            self.fsms[1..].iter().all(|fsm| fsm.logs() == first)
        })
        .await;
        self.wait_for("identical FSM configurations", || {
            let first = self.fsms[0].configurations.lock().clone();
            self.fsms[1..]
                .iter()
                .all(|fsm| *fsm.configurations.lock() == first)
        })
        .await;
    }

    /// Connects every transport to every other. Mirrors `FullyConnect`.
    pub fn fully_connect(&self) {
        for (i, t1) in self.trans.iter().enumerate() {
            for (j, t2) in self.trans.iter().enumerate() {
                if i != j {
                    t1.connect(self.addrs[j].clone(), t2.clone());
                }
            }
        }
    }

    /// Disconnects the node with the given address from all others, both
    /// ways. Mirrors `Disconnect`.
    pub fn disconnect(&self, addr: &ServerAddress) {
        for (i, t) in self.trans.iter().enumerate() {
            if &self.addrs[i] == addr {
                t.disconnect_all();
            } else {
                t.disconnect(addr);
            }
        }
    }

    /// Partitions the given nodes away from the rest of the cluster.
    /// Mirrors `Partition`.
    pub fn partition(&self, far: &[ServerAddress]) {
        for (i, t) in self.trans.iter().enumerate() {
            let in_far = far.contains(&self.addrs[i]);
            for (j, other) in self.trans.iter().enumerate() {
                if i == j {
                    continue;
                }
                let other_far = far.contains(&self.addrs[j]);
                if in_far != other_far {
                    t.disconnect(&self.addrs[j]);
                    let _ = other;
                }
            }
        }
    }

    /// The index of the raft with the given address, if any.
    pub fn index_of(&self, addr: &ServerAddress) -> Option<usize> {
        self.addrs.iter().position(|a| a == addr)
    }

    /// Shuts down the raft at `index`, then rebuilds it on top of the
    /// same FSM/log/stable/snapshot stores with a fresh transport on the
    /// same address. Mirrors the restart pattern in `TestRaft_SnapshotRestore`.
    /// Other peers are wired back to the new transport, so a multi-node
    /// cluster stays connected across restarts.
    pub async fn restart_node(&mut self, index: usize) {
        // The existing raft handle is Arc-backed; we can shut it down via a
        // fresh local reference.
        let local_id = self.rafts[index].local_id();
        let local_addr = self.addrs[index].clone();
        let shutdown_future = self.rafts[index].shutdown();
        shutdown_future.error().await.expect("shutdown failed");

        // Fresh transport on the same address.
        let (addr, trans) = raft::new_inmem_transport(local_addr.clone());
        assert_eq!(addr, local_addr);

        let mut peer_conf = self.conf.clone();
        peer_conf.local_id = local_id;

        let logs: Arc<dyn raft::LogStore> = self.stores[index].clone();
        let stable: Arc<dyn raft::StableStore> = self.stores[index].clone();
        let snaps: Arc<dyn raft::SnapshotStore> = self.snaps[index].clone();
        let trans: Arc<dyn raft::Transport> = trans.clone();

        let raft = new_raft(
            peer_conf,
            self.fsms[index].clone(),
            logs,
            stable,
            snaps,
            trans,
        )
        .await
        .expect("new_raft failed");
        self.rafts[index] = raft;

        // Re-wire the new transport into the rest of the cluster.
        for (j, other) in self.trans.iter().enumerate() {
            if j == index {
                continue;
            }
            other.connect(self.addrs[index].clone(), Arc::clone(&self.trans[index]));
            self.trans[index].connect(self.addrs[j].clone(), Arc::clone(other));
        }
    }

    /// Shuts down all nodes and waits for the shutdowns to complete.
    /// Mirrors `Close`.
    pub async fn close(self) {
        let futures: Vec<_> = self.rafts.iter().map(|r| r.shutdown()).collect();
        for future in futures {
            raft::RaftFuture::error(future)
                .await
                .expect("shutdown failed");
        }
    }
}
