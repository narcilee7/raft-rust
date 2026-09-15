use async_trait::async_trait;

use crate::Result;

/// Stable storage for the small amount of raft state that must survive
/// restarts (current term, last vote). Mirrors the Go `StableStore`
/// interface, with `Option` used instead of the Go "not found" error.
#[async_trait]
pub trait StableStore: Send + Sync {
    async fn set(&self, key: &[u8], val: &[u8]) -> Result<()>;

    /// Returns `None` if the key is not found.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    async fn set_u64(&self, key: &[u8], val: u64) -> Result<()>;

    /// Returns `None` if the key is not found.
    async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>>;
}

/// Stable store keys used by the library, matching the Go implementation.
pub const CURRENT_TERM_KEY: &[u8] = b"CurrentTerm";
pub const LAST_VOTE_TERM_KEY: &[u8] = b"LastVoteTerm";
pub const LAST_VOTE_CAND_KEY: &[u8] = b"LastVoteCand";
