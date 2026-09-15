# raft-rust

A Rust port of [`hashicorp/raft`](https://github.com/hashicorp/raft), the
production-tested Go library that implements the Raft consensus algorithm.

This port targets protocol version 3 / snapshot version 1 only. Legacy
compatibility code from the Go implementation is intentionally not
ported.

## Project layout

| Module | Mirrors | Purpose |
| --- | --- | --- |
| `api.rs` | `api.go` | Public `Raft` API surface |
| `config.rs` | `config.go` | `Config` and runtime knobs |
| `configuration.rs` | `configuration.go` | Cluster membership types |
| `commitment.rs` | `commitment.go` | Leadership commitment tracking |
| `error.rs` | `api.go` (errors) | `RaftError` / `Result` |
| `fsm.rs` | `fsm.go` | `FSM` / `FSMSnapshot` traits |
| `future.rs` | `future.go` | Internal future / promise types |
| `log.rs` | `log.go` | Log entry types |
| `log_cache.rs` | `log_cache.go` | Bounded in-memory log cache |
| `observer.rs` | `observer.go` | Observability hooks |
| `raft.rs` | `raft.go` | Core Raft state machine |
| `replication.rs` | `replication.go` | Log replication goroutine |
| `snapshot.rs` | `snapshot.go` | `SnapshotStore` / `SnapshotSink` traits, snapshot task |
| `stable.rs` | `stable.go` | `Stable` interface |
| `state.rs` | — | Internal shared state helpers |
| `transport.rs` | `transport.go` | `Transport` / `RPC` traits |
| `inmem_transport.rs` | `inmem_transport.go` | In-memory `Transport` (tests) |
| `inmem_store.rs` | `inmem_store.go` | In-memory log + stable stores |
| `inmem_snapshot.rs` | `inmem_snapshot.go` | In-memory `SnapshotStore` |
| `file_snapshot.rs` | `file_snapshot.go` | File-backed `SnapshotStore` |
| `net_transport.rs` | `net_transport.go` | TCP-based `Transport` |

## Features ported (stages 1–7)

- Skeleton + core types
- `Transport`, `LogStore`, `StableStore`, `FSM`, `SnapshotStore` traits with
  in-memory implementations
- Asynchronous future plumbing
- Leader election, log replication, pre-vote, leadership transfer
- FSM-driven snapshots, snapshot install via streaming
- Observer hook with filtering
- File-backed snapshot store (CRC64-ECMA verified)
- TCP network transport with pipelined `AppendEntries`, heartbeat
  fast-path, and the full request/response RPC surface

## Limitations vs. the Go implementation

- The TCP wire format uses length-prefixed msgpack frames rather than
  streaming msgpack. It is **not wire-compatible** with `hashicorp/raft`.
- I/O timeouts on the network transport are approximated; tokio's
  `TcpStream` no longer exposes `set_deadline`, so the request-level
  deadline scaling used by `InstallSnapshot` is not yet enforced.
- The `ServerAddressProvider` indirection is supported but no built-in
  example is shipped.

## Running the tests

```sh
cargo test --lib              # unit tests (59)
cargo test --tests            # integration tests (raft, snapshot, stage6)
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

## License

MIT — see `LICENSE`.
