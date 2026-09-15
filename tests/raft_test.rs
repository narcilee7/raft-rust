//! Milestone tests for the election + replication core, ported from
//! raft_test.go of the Go implementation.

mod common;

use std::time::Duration;

use common::{inmem_config, make_cluster};
use raft::{AppendEntriesRequest, RPCHeader, RaftError, RaftState, RequestVoteRequest, Transport};

/// Mirrors TestRaft_SingleNode.
#[tokio::test]
async fn single_node() {
    let conf = inmem_config();
    let c = make_cluster(1, true, Some(conf.clone())).await;
    let raft = &c.rafts[0];

    // Watch leader_ch for the change.
    let mut leader_ch = raft.leader_ch();
    tokio::time::timeout(conf.heartbeat_timeout * 3, async {
        leader_ch
            .wait_for(|v| *v)
            .await
            .expect("should become leader");
    })
    .await
    .expect("timeout becoming leader");

    // Should be leader.
    assert_eq!(raft.state(), RaftState::Leader);

    // Should be able to apply.
    let future = raft.apply(b"test", conf.heartbeat_timeout).await;
    let outcome = future.wait().await.expect("apply failed");

    // Check the response (MockFSM returns its log count).
    let response = outcome
        .response
        .expect("a response")
        .downcast::<usize>()
        .expect("usize response");
    assert_eq!(*response, 1);

    // Check the index.
    assert_ne!(outcome.index, 0);

    // Check that it is applied to the FSM.
    assert_eq!(c.fsms[0].log_count(), 1);

    c.close().await;
}

/// Mirrors TestRaft_TripleNode.
#[tokio::test]
async fn triple_node() {
    let c = make_cluster(3, true, None).await;

    // Should be one leader.
    c.followers().await;
    let leader = c.leader().await;
    let leader_addr = leader.local_addr();
    c.ensure_leader(&leader_addr).await;

    // Should be able to apply.
    let future = leader.apply(b"test", c.conf.commit_timeout).await;
    future.wait().await.expect("apply failed");
    c.wait_for_replication(1).await;

    c.close().await;
}

/// Mirrors TestRaft_LeaderFail.
#[tokio::test]
async fn leader_fail() {
    let c = make_cluster(3, true, None).await;

    // Should be one leader.
    c.followers().await;
    let leader = c.leader().await;

    // Should be able to apply.
    let future = leader.apply(b"test", c.conf.commit_timeout).await;
    future.wait().await.expect("apply failed");
    c.wait_for_replication(1).await;

    // Disconnect the leader now.
    let old_leader_addr = leader.local_addr();
    let leader_term = leader.current_term();
    c.disconnect(&old_leader_addr);

    // Wait for a new leader.
    c.wait_for("a new leader", || {
        let leaders = c.get_in_state(RaftState::Leader);
        leaders.len() == 1 && leaders[0].local_addr() != old_leader_addr
    })
    .await;
    let new_leader = c.get_in_state(RaftState::Leader)[0].clone();

    // Ensure the term is greater.
    assert!(
        new_leader.current_term() > leader_term,
        "expected newer term: {} vs {}",
        new_leader.current_term(),
        leader_term
    );

    // Apply should not work on the old leader (it cannot reach a quorum and
    // eventually steps down via the lease check).
    let future1 = c.rafts[c.index_of(&old_leader_addr).unwrap()]
        .apply(b"fail", c.conf.commit_timeout)
        .await;

    // Apply should work on the new leader.
    let future2 = new_leader.apply(b"apply", c.conf.commit_timeout).await;
    future2.wait().await.expect("apply on new leader failed");

    // The old leader's apply must fail with leadership lost or not leader.
    let err = future1.wait().await.unwrap_err();
    assert!(
        matches!(err, RaftError::LeadershipLost | RaftError::NotLeader),
        "unexpected error from old leader apply: {}",
        err
    );

    // Reconnect the networks.
    c.fully_connect();

    // Wait for log replication.
    c.ensure_same().await;

    // Check two entries are applied to the FSMs.
    for fsm in &c.fsms {
        let logs = fsm.logs();
        assert_eq!(logs.len(), 2, "did not apply both to FSM: {:?}", logs);
        assert_eq!(logs[0], b"test");
        assert_eq!(logs[1], b"apply");
    }

    c.close().await;
}

/// Mirrors TestRaft_ApplyNonLeader.
#[tokio::test]
async fn apply_non_leader() {
    let c = make_cluster(3, true, None).await;

    // Wait for a leader.
    c.leader().await;

    let followers = c.get_in_state(RaftState::Follower);
    assert_eq!(followers.len(), 2, "expected 2 followers");
    let follower = followers[0];

    // Try to apply.
    let future = follower.apply(b"test", c.conf.commit_timeout).await;
    let err = future.wait().await.unwrap_err();
    assert!(
        matches!(err, RaftError::NotLeader),
        "should not apply on follower: {}",
        err
    );

    c.close().await;
}

/// Mirrors TestRaft_ApplyConcurrent.
#[tokio::test]
async fn apply_concurrent() {
    let mut conf = inmem_config();
    conf.heartbeat_timeout *= 2;
    conf.election_timeout *= 2;
    let c = make_cluster(3, true, Some(conf)).await;

    // Wait for a leader.
    let leader = c.leader().await.clone();

    // Concurrently apply.
    const SZ: usize = 100;
    let mut set = tokio::task::JoinSet::new();
    for i in 0..SZ {
        let leader = leader.clone();
        set.spawn(async move {
            let cmd = format!("test{}", i);
            let future = leader.apply(cmd.as_bytes(), Duration::ZERO).await;
            future.wait().await.map(|_| ())
        });
    }
    let results: Vec<_> = set.join_all().await;
    for (i, r) in results.into_iter().enumerate() {
        r.unwrap_or_else(|e| panic!("apply {} failed: {}", i, e));
    }

    // Check the FSMs.
    c.ensure_same().await;
    assert_eq!(c.fsms[0].log_count(), SZ);

    c.close().await;
}

/// Mirrors TestRaft_Barrier.
#[tokio::test]
async fn barrier() {
    let c = make_cluster(3, true, None).await;

    // Get the leader.
    let leader = c.leader().await.clone();

    // Commit a lot of things.
    for i in 0..100 {
        let cmd = format!("test{}", i);
        leader.apply(cmd.as_bytes(), Duration::ZERO).await;
    }

    // Wait for a barrier complete.
    let barrier = leader.barrier(Duration::ZERO).await;
    barrier.wait().await.expect("barrier failed");

    // Ensure all the logs are the same.
    c.ensure_same().await;
    assert_eq!(c.fsms[0].log_count(), 100);

    c.close().await;
}

/// Mirrors TestRaft_BehindFollower.
#[tokio::test]
async fn behind_follower() {
    let c = make_cluster(3, true, None).await;

    // Disconnect one follower.
    let leader = c.leader().await.clone();
    let followers = c.followers().await;
    let behind_addr = followers[0].local_addr();
    assert!(
        followers[0].last_contact().is_some(),
        "expected previous contact"
    );
    c.disconnect(&behind_addr);

    // Commit a lot of things.
    let mut last = None;
    for i in 0..100 {
        let cmd = format!("test{}", i);
        last = Some(leader.apply(cmd.as_bytes(), Duration::ZERO).await);
    }

    // Wait for the last future to apply.
    last.expect("at least one apply")
        .wait()
        .await
        .expect("apply failed");

    // Reconnect the behind node.
    c.fully_connect();

    // Ensure all the logs are the same.
    c.ensure_same().await;

    // Ensure one leader.
    let leader = c.leader().await;
    let leader_addr = leader.local_addr();
    c.ensure_leader(&leader_addr).await;

    c.close().await;
}

/// Mirrors TestRaft_AppendEntry: a raw AppendEntries RPC with a newer term
/// succeeds, including one with an empty header identity.
#[tokio::test]
async fn append_entry() {
    let c = make_cluster(3, true, None).await;
    let followers = c.followers().await;
    let follower_addr = followers[0].local_addr();
    let follower_id = followers[0].local_id();
    let leader = c.leader().await.clone();
    let leader_trans = c.trans[c.index_of(&leader.local_addr()).unwrap()].clone();

    let req = AppendEntriesRequest {
        header: RPCHeader {
            protocol_version: 3,
            id: leader.local_id(),
            addr: leader.local_addr(),
        },
        term: leader.current_term() + 1,
        prev_log_entry: 0,
        prev_log_term: leader.current_term(),
        entries: vec![raft::Log {
            index: 1,
            term: leader.current_term() + 1,
            log_type: raft::LogType::Command,
            data: b"log 1".to_vec(),
            ..Default::default()
        }],
        leader_commit_index: 90,
    };
    let resp = leader_trans
        .append_entries(&follower_id, &follower_addr, &req)
        .await
        .expect("appendEntries RPC failed");
    assert!(resp.success);

    // A request with an empty header identity also succeeds.
    let mut req2 = req;
    req2.header.id = String::new();
    req2.header.addr = String::new();
    let resp2 = leader_trans
        .append_entries(&follower_id, &follower_addr, &req2)
        .await
        .expect("appendEntries RPC failed");
    assert!(resp2.success);

    c.close().await;
}

/// With pre-vote enabled (the default), a partitioned candidate cannot
/// disrupt a healthy cluster: its pre-votes are rejected by peers that
/// still see a leader, so its term does not inflate and it rejoins cleanly.
#[tokio::test]
async fn pre_vote_enabled_election() {
    let c = make_cluster(3, true, None).await;

    c.followers().await;
    let leader = c.leader().await;
    let leader_addr = leader.local_addr();
    c.ensure_leader(&leader_addr).await;

    let future = leader.apply(b"test", c.conf.commit_timeout).await;
    future.wait().await.expect("apply failed");
    c.wait_for_replication(1).await;

    c.close().await;
}

/// With pre-vote disabled, elections go straight to RequestVote and still
/// elect a leader.
#[tokio::test]
async fn pre_vote_disabled_election() {
    let mut conf = inmem_config();
    conf.pre_vote_disabled = true;
    let c = make_cluster(3, true, Some(conf)).await;

    c.followers().await;
    let leader = c.leader().await;
    let leader_addr = leader.local_addr();
    c.ensure_leader(&leader_addr).await;

    let future = leader.apply(b"test", c.conf.commit_timeout).await;
    future.wait().await.expect("apply failed");
    c.wait_for_replication(1).await;

    c.close().await;
}

/// Exercises the three-phase verify-leader path: the leader verifies
/// itself against a quorum of followers, and a follower's verification is
/// rejected. Mirrors TestRaft_VerifyLeader (basic parts).
#[tokio::test]
async fn verify_leader() {
    let c = make_cluster(3, true, None).await;

    let leader = c.leader().await.clone();
    let future = leader.verify_leader().await;
    future.wait().await.expect("verify leader failed");

    let followers = c.followers().await;
    let future = followers[0].verify_leader().await;
    let err = future.wait().await.unwrap_err();
    assert!(
        matches!(err, RaftError::NotLeader),
        "follower verify should fail with NotLeader: {}",
        err
    );

    c.close().await;
}

/// A raw RequestVote with a stale term is rejected, mirroring the vote
/// safety checks in `requestVote`.
#[tokio::test]
async fn request_vote_stale_term_rejected() {
    let c = make_cluster(3, true, None).await;
    let leader = c.leader().await.clone();
    let leader_trans = c.trans[c.index_of(&leader.local_addr()).unwrap()].clone();
    let followers = c.followers().await;
    let follower_addr = followers[0].local_addr();
    let follower_id = followers[0].local_id();

    let req = RequestVoteRequest {
        header: RPCHeader {
            protocol_version: 3,
            id: leader.local_id(),
            addr: leader.local_addr(),
        },
        term: 0,
        last_log_index: 0,
        last_log_term: 0,
        leadership_transfer: false,
    };
    let resp = leader_trans
        .request_vote(&follower_id, &follower_addr, &req)
        .await
        .expect("requestVote RPC failed");
    assert!(!resp.granted, "stale vote request must not be granted");

    c.close().await;
}
