# raft-rust

An async-first, type-safe port of [`hashicorp/raft`](https://github.com/hashicorp/raft)
implemented in Rust on top of [`tokio`](https://tokio.rs/).

The port tracks the Go implementation's behavior (protocol version 3,
snapshot version 1) without preserving its wire format or its goroutine
model. Where the Go code leans on runtime tricks — channels for back-pressure,
reflection-light interface dispatch, GC-driven memory churn — this port
uses the type system and `async`/`await` to make the same guarantees
explicit at compile time.

## Why Rust here

- **No GC, no goroutine scheduler.** All tasks are `tokio` futures on a
  configurable runtime; back-pressure is expressed through `async fn`
  signatures rather than channel sends. There is no equivalent of "fire
  and forget" leaking past a request lifecycle.
- **Traits, not interfaces.** `Transport`, `LogStore`, `StableStore`,
  `FSM`, and `SnapshotStore` are real trait objects with `async fn`
  methods. The compiler enforces `Send + Sync` at every boundary, so
  cross-task sharing of cluster state is impossible to get wrong at
  runtime.
- **Errors are values, not control flow.** Every fallible operation
  returns `Result<T, RaftError>`. The `RaftError` enum
  ([`error.rs`](src/error.rs)) is `#[derive(thiserror::Error)]`, so
  errors carry context and are exhaustively matchable — there is no
  `errors.Is`/`errors.As` ritual.
- **Synchronization is explicit.** `parking_lot::Mutex` guards cover
  synchronous critical sections; `tokio::sync::Mutex` covers the few
  critical sections that have to be held across an `.await`. Guards
  must be dropped before suspending the task — this rule is enforced
  by the borrow checker rather than by code review.
- **No global mutable state.** The `Raft` handle owns its transport,
  stores, FSM and snapshot store; `Arc<RaftCore>` is shared with the
  internal tasks. There is no package-level singleton.

## Cargo features

```toml
[dependencies]
raft = { path = "..." }    # nothing optional yet
```

`Cargo.toml` keeps the dependency surface tight: `tokio`, `async-trait`,
`serde`, `serde_json`, `rmp-serde`, `thiserror`, `tracing`, `bytes`,
`parking_lot`, `futures`, `serde_bytes`. Dev-only: `tempfile`,
`tracing-subscriber`.

## Module layout

| Module | What lives here |
| --- | --- |
| `api.rs` | Public `Raft` API: bootstrap, apply, snapshot, stats |
| `config.rs` | `Config` and tuning knobs |
| `configuration.rs` | `Configuration`, `Server`, suffrage types |
| `commitment.rs` | Match-commit tracking (election §5.4.2) |
| `error.rs` | `RaftError` enum + `Result` alias |
| `fsm.rs` | `FSM` and `FSMSnapshot` traits |
| `future.rs` | Promise/future plumbing (append, snapshot, configuration) |
| `log.rs` | `Log` entry type |
| `log_cache.rs` | Bounded in-memory tail of the on-disk log |
| `observer.rs` | `Observer` trait with filter support |
| `raft.rs` | Core state machine — main loop, elections, replication |
| `replication.rs` | Per-follower replication goroutine |
| `snapshot.rs` | `SnapshotStore`/`SnapshotSink` traits + snapshot task |
| `stable.rs` | `Stable` interface |
| `state.rs` | Shared-state helpers (cached indices, last snapshot) |
| `transport.rs` | `Transport`, `RPC`, request/response types |
| `inmem_*` | In-memory implementations (tests) |
| `file_snapshot.rs` | File-backed snapshots with CRC64-ECMA |
| `net_transport.rs` | TCP transport with pipelined `AppendEntries` |

## Wire format (deliberate difference from Go)

`net_transport.rs` uses **length-prefixed msgpack** frames:
`[rpc_type: u8][len: u32 BE][msgpack(body)]`. The Go implementation
streams msgpack directly off a `bufio.Reader`. The change is intentional:

- Tokio's `TcpStream` no longer exposes `set_deadline`; streaming msgpack
  in async Rust would need a custom codec over `AsyncRead`. Length
  prefixes let us `read_exact` and decode synchronously, keeping the
  framing layer small.
- It is **not wire-compatible** with `hashicorp/raft`. Mixed clusters are
  out of scope.

Per-request timeouts are not currently enforced; the `timeout` /
`timeout_scale` knobs are present in the config struct but the TCP
transport ignores them. `tokio::time::timeout` would be the natural
extension point.

## Testing

```sh
cargo test --lib          # 59 unit tests
cargo test --tests        # + 21 integration tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

The in-memory `Cluster` harness in `tests/common/` drives every stage's
integration tests; it builds on `InmemTransport` so the tests are fully
deterministic.

## Limitations vs. the Go implementation

- Single-crate layout; no internal sub-crates
- Wire format differs (see above)
- No per-request deadline enforcement on the TCP transport
- Snapshot reader pre-buffers into a `Vec<u8>` rather than streaming to
  the FSM (avoids a sync/async bridge)
- No `ServerAddressProvider` example shipped; the trait exists and is
  wired through `NetworkTransportConfig`

## License

MIT — see [`LICENSE`](LICENSE).
