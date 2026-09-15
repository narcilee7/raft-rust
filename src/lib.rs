//! A Rust implementation of the Raft consensus protocol, ported from
//! the [hashicorp/raft](https://github.com/hashicorp/raft) Go library.
//!
//! Only protocol version 3 and snapshot version 1 are supported; legacy
//! protocol compatibility code from the Go implementation is not ported.

mod api;
mod commitment;
mod config;
mod configuration;
mod error;
mod fsm;
mod future;
mod log;
mod observer;
mod raft;
mod replication;
mod snapshot;
mod stable;
mod state;
mod transport;

mod file_snapshot;
mod inmem_snapshot;
mod inmem_store;
mod inmem_transport;
mod log_cache;
mod net_transport;

// Re-exports for modules implemented in later phases are commented out
// until they have contents:
// pub use commitment::*;  // internal only (pub(crate))
pub use observer::*;
// pub use raft::*;        // internal only (pub(crate))
// pub use replication::*; // internal only (pub(crate))
pub use file_snapshot::*;
pub use log_cache::*;
pub use net_transport::*;

pub use api::*;
pub use config::*;
pub use configuration::*;
pub use error::*;
pub use fsm::*;
pub use future::*;
pub use log::*;
pub use snapshot::*;
pub use stable::*;
pub use state::*;
pub use transport::*;

pub use inmem_snapshot::*;
pub use inmem_store::*;
pub use inmem_transport::*;
