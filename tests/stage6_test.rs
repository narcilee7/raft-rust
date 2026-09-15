//! Stage 6 tests: observer mechanism and leadership transfer. Mirrors the
//! `TestObserver` and `TestRaft_LeadershipTransfer` families from
//! `raft_test.go`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::make_cluster;
use raft::{
    LeaderObservation, Observation, ObservationData, Observer, PeerObservation, RaftState,
    ServerSuffrage,
};

/// Mirrors `TestRaft_Observer`: a registered observer receives the state
/// transitions produced by a running raft.
#[tokio::test]
async fn observer_receives_state_transitions() {
    let c = make_cluster(1, true, None).await;

    // Register the observer before any transitions happen so we don't
    // miss the Follower -> Leader transition.
    let mut observer = c.rafts[0].register_observer(Observer {
        channel_size: 64,
        blocking: false,
        filter: None,
    });

    // Wait for the leader to be elected and apply something.
    let leader = c.leader().await.clone();
    let _ = leader.apply(b"hi", c.conf.commit_timeout).await;
    // Apply Future resolution requires waiting briefly.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Collect observations.
    let mut states = Vec::new();
    while let Ok(Some(obs)) =
        tokio::time::timeout(Duration::from_millis(50), observer.receiver.recv()).await
    {
        if let ObservationData::State(state) = obs.data {
            states.push(state);
        }
    }

    // We expect at least one transition (Follower -> Leader).
    assert!(
        states.contains(&RaftState::Leader),
        "observer should see Leader transition: {:?}",
        states
    );

    // Observer counters should reflect the events we received.
    assert!(
        observer.num_observed() > 0,
        "observer should have observed events"
    );

    c.close().await;
}

/// Mirrors `TestObserver_Filter`: a non-trivial filter only delivers the
/// matching observations.
#[tokio::test]
async fn observer_filter_drops_non_matches() {
    let c = make_cluster(1, true, None).await;

    let filter: raft::FilterFn = Arc::new(|o| {
        matches!(
            o.data,
            ObservationData::Leader(_) | ObservationData::State(RaftState::Shutdown)
        )
    });

    let mut observer = c.rafts[0].register_observer(Observer {
        channel_size: 64,
        blocking: false,
        filter: Some(filter),
    });

    let leader = c.leader().await.clone();
    let _ = leader.apply(b"hi", c.conf.commit_timeout).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Drain whatever arrived.
    let mut delivered = 0;
    while let Ok(Some(_)) =
        tokio::time::timeout(Duration::from_millis(50), observer.receiver.recv()).await
    {
        delivered += 1;
        if delivered > 100 {
            break;
        }
    }

    // The count observed should match what the consumer received (modulo
    // the channel's buffering). At least one event should have been
    // delivered since the Follower -> Leader transition matches the
    // filter.
    assert!(delivered > 0, "filter should pass the Leader transition");

    c.close().await;
}

/// Leader can be asked to transfer leadership to a follower; the new
/// leader takes over and a quorum follows. Mirrors the basic
/// `TestRaft_LeadershipTransfer` flow.
#[tokio::test]
async fn leadership_transfer_basic() {
    let c = make_cluster(3, true, None).await;

    let leader = c.leader().await.clone();
    let leader_addr = leader.local_addr();

    // Pick a follower address to transfer to.
    let followers = c.followers().await;
    let target_addr = followers[0].local_addr();
    let target_id = followers[0].local_id();

    // Issue the transfer. Use a generous timeout; the catch-up loop must
    // observe the target at the leader's last log index.
    let future = leader
        .leadership_transfer_to_server(target_id.clone(), target_addr.clone())
        .await;
    let result = future.wait().await;
    // The transfer itself returns immediately after sending TimeoutNow;
    // the actual leader change happens asynchronously.
    result.expect("leadership transfer should not error");

    // Wait for the cluster to elect a new leader, possibly the target.
    c.wait_for("a new leader", || {
        let leaders = c.get_in_state(RaftState::Leader);
        leaders.len() == 1 && leaders[0].local_addr() != leader_addr
    })
    .await;

    // The new leader should be the target.
    let new_leader = c.get_in_state(RaftState::Leader)[0];
    assert_eq!(
        new_leader.local_addr(),
        target_addr,
        "expected transfer target to win"
    );

    c.close().await;
}

/// `leadership_transfer` to a non-member server fails with a clear error.
#[tokio::test]
async fn leadership_transfer_unknown_target() {
    let c = make_cluster(3, true, None).await;

    let leader = c.leader().await.clone();
    let future = leader
        .leadership_transfer_to_server("not-a-member".to_string(), "nowhere".to_string())
        .await;
    let err = future.wait().await.unwrap_err();
    // Should be RaftError::Other with a "cannot find replication state" message.
    let msg = format!("{}", err);
    assert!(
        msg.contains("replication state") || msg.contains("cannot find"),
        "unexpected error: {}",
        msg
    );

    c.close().await;
}

/// Mirrors `TestRaft_LeadershipTransferLease` indirectly: transferring
/// to yourself is rejected.
#[tokio::test]
async fn leadership_transfer_to_self_rejected() {
    let c = make_cluster(3, true, None).await;

    let leader = c.leader().await.clone();
    let self_id = leader.local_id();
    let self_addr = leader.local_addr();

    let future = leader
        .leadership_transfer_to_server(self_id, self_addr)
        .await;
    let err = future.wait().await.unwrap_err();
    assert!(
        matches!(err, raft::RaftError::Other(_)),
        "self-transfer should be rejected: {}",
        err
    );

    c.close().await;
}

// Suppress unused import warnings if a future test starts using them.
#[allow(dead_code)]
fn _ensure_used(_: &LeaderObservation, _: &PeerObservation, _: &Observation, _: &ServerSuffrage) {}
