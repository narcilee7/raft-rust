//! Advances the leader's commit index. Mirrors commitment.go of the Go
//! implementation.

use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::configuration::{Configuration, ServerID, ServerSuffrage};

/// Tracks which log entries a quorum of voters has stored. The leader and
/// the replication tasks report newly written entries with
/// [`Commitment::match_index`], and the commit channel is notified when the
/// commit index advances. Mirrors the Go `commitment` struct; a new one is
/// created each time this server becomes leader for a term.
pub(crate) struct Commitment {
    inner: Mutex<CommitmentInner>,
    /// Notified (non-blocking, single slot) when the commit index increases.
    commit_tx: mpsc::Sender<()>,
}

struct CommitmentInner {
    /// Voter ID to log index: the server stores up through this log entry.
    match_indexes: HashMap<ServerID, u64>,
    /// A quorum stores up through this log entry. Monotonically increases.
    commit_index: u64,
    /// The first index of this leader's term: this needs to be replicated to
    /// a majority of the cluster before this leader may mark anything
    /// committed (per Raft's commitment rule).
    start_index: u64,
}

impl Commitment {
    /// `configuration` is the servers in the cluster; `start_index` is the
    /// first index that may be committed in this term; `commit_tx` (capacity
    /// 1) is notified when the commit index advances.
    pub(crate) fn new(
        configuration: &Configuration,
        start_index: u64,
        commit_tx: mpsc::Sender<()>,
    ) -> Self {
        let match_indexes = configuration
            .servers
            .iter()
            .filter(|s| s.suffrage == ServerSuffrage::Voter)
            .map(|s| (s.id.clone(), 0))
            .collect();
        Commitment {
            inner: Mutex::new(CommitmentInner {
                match_indexes,
                commit_index: 0,
                start_index,
            }),
            commit_tx,
        }
    }

    /// Called when a new cluster membership configuration is created: it
    /// will be used to determine commitment from now on. Match indexes are
    /// kept for servers that remain voters.
    pub(crate) fn set_configuration(&self, configuration: &Configuration) {
        let mut inner = self.inner.lock();
        let old = std::mem::take(&mut inner.match_indexes);
        inner.match_indexes = configuration
            .servers
            .iter()
            .filter(|s| s.suffrage == ServerSuffrage::Voter)
            .map(|s| {
                let index = old.get(&s.id).copied().unwrap_or(0);
                (s.id.clone(), index)
            })
            .collect();
        Self::recalculate(&mut inner, &self.commit_tx);
    }

    /// The current commit index. Called by the leader after the commit
    /// channel is notified.
    pub(crate) fn commit_index(&self) -> u64 {
        self.inner.lock().commit_index
    }

    /// The first index of this leader's term; the leader may not commit
    /// anything below it. Mirrors reading `commitment.startIndex` in
    /// `configurationChangeChIfStable`.
    pub(crate) fn start_index(&self) -> u64 {
        self.inner.lock().start_index
    }

    /// Called once a server completes writing entries to disk: either the
    /// leader wrote the new entry or a follower replied to an AppendEntries
    /// RPC. The given server's disk agrees with this server's log up through
    /// `match_index`. Mirrors `commitment.match`.
    pub(crate) fn match_index(&self, server: &ServerID, match_index: u64) {
        let mut inner = self.inner.lock();
        let prev = inner.match_indexes.get_mut(server);
        match prev {
            Some(prev) if match_index > *prev => {
                *prev = match_index;
                Self::recalculate(&mut inner, &self.commit_tx);
            }
            // Ignore non-voters and stale indexes.
            _ => {}
        }
    }

    /// Recalculates the commit index from the match indexes using the quorum
    /// median rule. Must be called with the lock held.
    fn recalculate(inner: &mut CommitmentInner, commit_tx: &mpsc::Sender<()>) {
        if inner.match_indexes.is_empty() {
            return;
        }
        let mut matched: Vec<u64> = inner.match_indexes.values().copied().collect();
        matched.sort_unstable();
        let quorum_match_index = matched[(matched.len() - 1) / 2];

        // Only advance, and only past the first index of this term.
        if quorum_match_index > inner.commit_index && quorum_match_index >= inner.start_index {
            inner.commit_index = quorum_match_index;
            // Non-blocking notify, mirroring asyncNotifyCh.
            let _ = commit_tx.try_send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::Server;

    fn make_configuration(voters: &[&str]) -> Configuration {
        Configuration {
            servers: voters
                .iter()
                .map(|id| Server {
                    suffrage: ServerSuffrage::Voter,
                    id: id.to_string(),
                    address: format!("{}addr", id),
                })
                .collect(),
        }
    }

    fn voters(n: usize) -> Configuration {
        make_configuration(&["s1", "s2", "s3", "s4", "s5", "s6", "s7"][..n])
    }

    fn new_commitment(conf: &Configuration, start_index: u64) -> (Commitment, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel(1);
        (Commitment::new(conf, start_index, tx), rx)
    }

    fn drained(rx: &mut mpsc::Receiver<()>) -> bool {
        rx.try_recv().is_ok()
    }

    // Mirrors TestCommitment_setVoters: set_configuration keeps match
    // indexes where possible.
    #[test]
    fn set_configuration_keeps_match_indexes() {
        let (c, mut rx) = new_commitment(&make_configuration(&["a", "b", "c"]), 0);
        c.match_index(&"a".into(), 10);
        c.match_index(&"b".into(), 20);
        c.match_index(&"c".into(), 30);
        // commit_index: 20
        assert!(drained(&mut rx));
        c.set_configuration(&make_configuration(&["c", "d", "e"]));
        // c: 30, d: 0, e: 0
        c.match_index(&"e".into(), 40);
        assert_eq!(c.commit_index(), 30);
        assert!(drained(&mut rx));
    }

    // Mirrors TestCommitment_match_max.
    #[test]
    fn match_with_earlier_index_is_ignored() {
        let (c, _rx) = new_commitment(&voters(5), 4);
        c.match_index(&"s1".into(), 8);
        c.match_index(&"s2".into(), 8);
        c.match_index(&"s2".into(), 1);
        c.match_index(&"s3".into(), 8);
        assert_eq!(c.commit_index(), 8);
    }

    // Mirrors TestCommitment_match_nonVoting.
    #[test]
    fn match_from_non_voters_is_ignored() {
        let (c, mut rx) = new_commitment(&voters(5), 4);
        c.match_index(&"s1".into(), 8);
        c.match_index(&"s2".into(), 8);
        c.match_index(&"s3".into(), 8);
        assert!(drained(&mut rx));

        c.match_index(&"s90".into(), 10);
        c.match_index(&"s91".into(), 10);
        c.match_index(&"s92".into(), 10);
        assert_eq!(c.commit_index(), 8);
        assert!(!drained(&mut rx));
    }

    // Mirrors TestCommitment_recalculate.
    #[test]
    fn recalculate_quorum_median() {
        let (c, mut rx) = new_commitment(&voters(5), 0);
        c.match_index(&"s1".into(), 30);
        c.match_index(&"s2".into(), 20);
        assert_eq!(c.commit_index(), 0);
        assert!(!drained(&mut rx));

        c.match_index(&"s3".into(), 10);
        assert_eq!(c.commit_index(), 10);
        assert!(drained(&mut rx));
        c.match_index(&"s4".into(), 15);
        assert_eq!(c.commit_index(), 15);
        assert!(drained(&mut rx));

        c.set_configuration(&voters(3));
        // s1: 30, s2: 20, s3: 10
        assert_eq!(c.commit_index(), 20);
        assert!(drained(&mut rx));

        c.set_configuration(&voters(4));
        // s1: 30, s2: 20, s3: 10, s4: 0
        c.match_index(&"s2".into(), 25);
        assert_eq!(c.commit_index(), 20);
        assert!(!drained(&mut rx));
        c.match_index(&"s4".into(), 23);
        assert_eq!(c.commit_index(), 23);
        assert!(drained(&mut rx));
    }

    // Mirrors TestCommitment_recalculate_startIndex.
    #[test]
    fn recalculate_respects_start_index() {
        let (c, mut rx) = new_commitment(&voters(5), 4);
        c.match_index(&"s1".into(), 3);
        c.match_index(&"s2".into(), 3);
        c.match_index(&"s3".into(), 3);
        assert_eq!(c.commit_index(), 0);
        assert!(!drained(&mut rx));

        c.match_index(&"s1".into(), 4);
        c.match_index(&"s2".into(), 4);
        c.match_index(&"s3".into(), 4);
        assert_eq!(c.commit_index(), 4);
        assert!(drained(&mut rx));
    }

    // Mirrors TestCommitment_noVoterSanity.
    #[test]
    fn no_voters_commits_nothing() {
        let (c, mut rx) = new_commitment(&make_configuration(&[]), 4);
        c.match_index(&"s1".into(), 10);
        c.set_configuration(&make_configuration(&[]));
        c.match_index(&"s1".into(), 10);
        assert_eq!(c.commit_index(), 0);
        assert!(!drained(&mut rx));

        // Add a voter, commit, then remove it again.
        c.set_configuration(&voters(1));
        c.match_index(&"s1".into(), 10);
        assert_eq!(c.commit_index(), 10);
        assert!(drained(&mut rx));

        c.set_configuration(&make_configuration(&[]));
        c.match_index(&"s1".into(), 20);
        assert_eq!(c.commit_index(), 10);
        assert!(!drained(&mut rx));
    }

    // Mirrors TestCommitment_singleVoter.
    #[test]
    fn single_voter_commits_immediately() {
        let (c, mut rx) = new_commitment(&voters(1), 4);
        c.match_index(&"s1".into(), 10);
        assert_eq!(c.commit_index(), 10);
        assert!(drained(&mut rx));
        c.set_configuration(&voters(1));
        assert!(!drained(&mut rx));
        c.match_index(&"s1".into(), 12);
        assert_eq!(c.commit_index(), 12);
        assert!(drained(&mut rx));
    }
}
