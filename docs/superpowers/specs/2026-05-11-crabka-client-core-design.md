# `crabka-client-core` (slice 2) — Design

**Status:** Draft for review
**Date:** 2026-05-11
**Author:** Matthew Stone (with Claude)
**Predecessor:** project meta-spec
(`2026-05-10-crabka-rust-rewrite-design.md`). The coverage slice (slice 1)
is fully shipped via sub-plans 1a–1e.

## Summary

`crabka-client-core` is the first Crabka crate that does I/O. It provides
connection management, API-version negotiation, and correlation-ID
request/response dispatch over TCP against Apache Kafka brokers. Built
on `tokio`. Plaintext only — TLS/SASL/ACLs are slice 11.

No producer or consumer semantics: those are slices 5 and 6. No
transactions: slice 9. No partition-aware routing for produce/fetch:
slices 5/6 will wrap this crate.

## North star (acceptance gate for slice 2)

1. New crate `crabka-client-core` exists in the workspace.
2. `crabka-protocol-codegen` emits `impl ProtocolRequest for <Request>`
   for every Request type generated from the schemas.
3. `Client::builder(bootstrap).build()` connects to a Kafka broker,
   negotiates API versions, and returns a usable handle.
4. `Client::send<R>` round-trips a typed request → typed response.
5. Correlation-ID multiplexing handles concurrent in-flight requests
   on a single connection.
6. Timeout, disconnect, and version-incompatibility surface as typed
   `ClientError` variants — no panics.
7. `BrokerPool::refresh_brokers` populates the pool from a `MetadataResponse`;
   subsequent `client.broker(id).send(...)` reaches that broker.
8. Integration tests against a real Apache Kafka container via
   `testcontainers-rs` pass at least four end-to-end scenarios.
9. CI matrix green; new `client-core-integration` job (Linux only)
   runs the testcontainers suite.
10. No regressions: workspace tests from prior slices still pass.

## Non-goals

- **Producer / consumer semantics.** Slices 5/6.
- **Transactions / exactly-once.** Slice 9.
- **Partition-aware routing.** Slices 5/6 will wrap this crate.
- **TLS, SASL, delegation tokens, ACLs.** Slice 11.
- **Automatic mid-request retry.** A request that hits a `Disconnected`
  returns immediately; the caller decides whether to retry. Higher-level
  retry policy is a follow-up sub-plan.
- **Smart broker routing.** Slice 2's `Client::send` routes to any open
  broker (or the first bootstrap address if no connection exists). It
  does NOT consult Metadata to pick the right node per request.
- **Connection pooling beyond one per broker.** A single broker
  connection multiplexes via correlation ID. Producers may eventually
  want multiple connections per broker for throughput; deferred.
- **DNS re-resolution.** Bootstrap addresses are resolved once at
  `Client::builder`. Production clients may want this; deferred.
- **Metrics / tracing emission.** Add later in a small follow-up
  sub-plan; not blocking initial functionality.

---

# 1. Crate layout

```
crates/client-core/
├── Cargo.toml
├── src/
│   ├── lib.rs               # public API re-exports
│   ├── error.rs             # ClientError
│   ├── transport.rs         # TCP + length-delimited codec
│   ├── version.rs           # ApiVersionTable + negotiation
│   ├── connection.rs        # single-broker Connection (dispatcher + correlation)
│   ├── pool.rs              # BrokerPool: DashMap<i32, Arc<Connection>>
│   ├── bootstrap.rs         # parse "host:port,host:port"; lookup_host
│   ├── client.rs            # Client + ClientBuilder + BrokerHandle
│   ├── request.rs           # ProtocolRequest trait
│   └── mock.rs              # in-process MockBroker (cfg-gated)
└── tests/
    ├── unit.rs              # mock-based tests
    └── integration.rs       # #[ignore]'d testcontainers integration
```

# 2. `Cargo.toml`

```toml
[package]
name = "crabka-client-core"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Connection management and request dispatch for Apache Kafka in Rust"

[lints]
workspace = true

[features]
default = []
mock = []   # exposes the in-process MockBroker beyond #[cfg(test)]

[dependencies]
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
bytes = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true, features = ["net", "rt", "rt-multi-thread", "io-util", "macros", "sync", "time"] }
tokio-util = { workspace = true, features = ["codec"] }
dashmap = "6"
tracing = "0.1"
tokio-util-codec-length-delimited-already-in-tokio-util = false  # placeholder; verify the actual feature flag

[dev-dependencies]
proptest = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
testcontainers = "0.20"
testcontainers-modules = { version = "0.10", features = ["kafka"] }
tokio = { workspace = true, features = ["test-util"] }
```

`dashmap` for the broker pool's concurrent access. `tracing` lays
groundwork for future observability without committing to an
exporter. `testcontainers` + `testcontainers-modules` for the
integration suite.

`tokio` feature flags are minimal: `net` for TcpStream/TcpListener, `rt`
+ `rt-multi-thread` for the runtime, `io-util` for `AsyncReadExt`/
`AsyncWriteExt`, `macros` for `#[tokio::main]` + `tokio::select!`,
`sync` for channels, `time` for timeouts.

Add to root `[workspace.dependencies]`:

```toml
tokio = { version = "1", default-features = false }
tokio-util = { version = "0.7", default-features = false }
```

(Individual crates re-enable feature subsets.)

# 3. Public API

```rust
// crates/client-core/src/lib.rs

pub use client::{Client, ClientBuilder, BrokerHandle};
pub use connection::{Connection, ConnectionOptions};
pub use error::ClientError;
pub use pool::{BrokerInfo, BrokerPool};
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
```

### `Client`

```rust
pub struct Client { /* ... */ }

impl Client {
    pub fn builder(bootstrap: impl Into<String>) -> ClientBuilder;

    /// Send a typed request to whichever broker the pool currently has open
    /// (or opens the first bootstrap address if none). Does NOT do smart
    /// broker selection — slices 5/6 wrap this for partition-aware routing.
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError>;

    /// Get a handle pinned to a specific broker id. Opens the connection
    /// lazily on first use.
    pub fn broker(&self, broker_id: i32) -> BrokerHandle<'_>;

    /// Send a default `MetadataRequest`; update the pool's broker registry
    /// from the response.
    pub async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError>;

    pub async fn close(self);
}

pub struct ClientBuilder { /* ... */ }

impl ClientBuilder {
    pub fn client_id(self, id: impl Into<String>) -> Self;
    pub fn request_timeout(self, t: Duration) -> Self;
    pub fn connect_timeout(self, t: Duration) -> Self;
    pub async fn build(self) -> Result<Client, ClientError>;
}

pub struct BrokerHandle<'a> { /* ... */ }

impl<'a> BrokerHandle<'a> {
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError>;
}
```

### `ProtocolRequest`

```rust
// crates/client-core/src/request.rs

use crabka_protocol::{Decode, Encode};

/// Marker trait implemented by `crabka-protocol`'s generated Request types.
/// Provides the dispatch information (api key, version range, response type)
/// that the client needs to send + decode.
pub trait ProtocolRequest: Encode {
    const API_KEY: i16;
    const MIN_VERSION: i16;
    const MAX_VERSION: i16;
    const FLEXIBLE_MIN: i16;
    type Response: for<'de> Decode<'de>;
}
```

**Codegen change in slice 2:** extend
`crates/protocol-codegen/src/emit/owned.rs` so that for every
`MessageType::Request` it emits, after the inherent items, an
`impl ProtocolRequest for <Request>` block. The body just reads the
already-emitted constants and names the corresponding `Response` type.
Drift check catches stale snapshots.

### `Connection`

```rust
pub struct Connection { /* Arc<ConnectionInner> */ }

pub struct ConnectionOptions {
    pub client_id: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Connection {
    pub async fn connect(addr: SocketAddr, opts: ConnectionOptions)
        -> Result<Self, ClientError>;
    pub async fn send<R: ProtocolRequest>(&self, req: R)
        -> Result<R::Response, ClientError>;
    pub async fn close(self);
}
```

### `ClientError`

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("connect to {addr}: {source}")]
    Connect { addr: SocketAddr, #[source] source: std::io::Error },

    #[error("connection closed")]
    Disconnected,

    #[error("request timed out after {0:?}")]
    Timeout(Duration),

    #[error(
        "incompatible version: broker supports {broker_min}..={broker_max}, \
         client wants {client_min}..={client_max} for api_key {api_key}"
    )]
    IncompatibleVersion {
        api_key: i16,
        broker_min: i16,
        broker_max: i16,
        client_min: i16,
        client_max: i16,
    },

    #[error("protocol error from server: {error_code}")]
    Server { error_code: i16 },

    #[error("codec: {0}")]
    Codec(#[from] crabka_protocol::ProtocolError),

    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}
```

`Server { error_code }` is for unusual transport-level errors (the
broker closing the connection with a server-side rejection). Most
Kafka-level errors flow back through the typed response's `errorCode`
field; the client doesn't inspect those.

# 4. Connection lifecycle

### TCP framing

Kafka uses **4-byte big-endian length prefix, `length` bytes of body**.
Both request and response.

`transport.rs` wraps `tokio::net::TcpStream` with
`tokio_util::codec::LengthDelimitedCodec` configured for a 4-byte
big-endian prefix and a max frame size of 100 MiB (matching Kafka's
default `socket.request.max.bytes`).

### `Connection::connect`

1. `tokio::time::timeout(connect_timeout, TcpStream::connect(addr))`.
2. Spawn two background tokio tasks via `tokio::spawn`:
   - **Reader.** Decodes incoming frames, peels off the response header
     (`ResponseHeader` v0 or v1 depending on the message's
     flexible-versions threshold), routes the body to the `oneshot`
     waiter registered for that correlation ID.
   - **Writer.** Reads from an `mpsc::Receiver<DispatchItem>` channel,
     writes the length-prefixed bytes to the socket.
3. Send `ApiVersionsRequest` (always at v0 — it has to be, because we
   don't know what the broker supports yet). Decode the response,
   build the `ApiVersionTable`.
4. Return the connected `Connection`.

### `Connection::send<R>`

1. Compute the version:
   `min(broker_max_version, R::MAX_VERSION).max(R::MIN_VERSION)`.
   Fail with `ClientError::IncompatibleVersion` if no overlap.
2. Allocate a correlation ID via `next_corr_id.fetch_add(1, Ordering::Relaxed)`.
3. Build the request header: `RequestHeader v1` if the message is
   flexible at the chosen version, else `v0`. Set `apiKey`,
   `apiVersion`, `correlationId`, and `clientId` (the
   `ClientBuilder`-supplied value).
4. Encode header + body into a `BytesMut`. Prepend the 4-byte BE length
   (handled by `LengthDelimitedCodec` when we send via the codec
   framed sink).
5. Create a `tokio::sync::oneshot::channel<Result<R::Response, ClientError>>`.
   Register the sender in a `DashMap<i32, oneshot::Sender<...>>` keyed
   on correlation ID.
6. Send `DispatchItem { frame, corr_id }` through the writer's mpsc.
7. `tokio::time::timeout(request_timeout, oneshot_rx)`. On timeout,
   evict the correlation-ID entry from the map and return
   `ClientError::Timeout`.
8. The reader task, upon receiving a frame:
   - Decode `ResponseHeader` matching the request version's flexibility.
   - Look up correlation ID in the map. If absent (timeout already
     fired, request was cancelled), drop silently.
   - Decode the body as `R::Response`. Send via the oneshot.

### `ApiVersionTable`

```rust
pub struct ApiVersionTable {
    by_key: HashMap<i16, (i16, i16)>,   // api_key -> (broker_min, broker_max)
}

impl ApiVersionTable {
    /// The bootstrap-time version table is fetched via a v0
    /// `ApiVersionsRequest`. The connection cannot use `Connection::send`
    /// for this because it requires a populated table — so we expose a
    /// private "raw" send path on Connection that's used only during
    /// connect.
    pub(crate) async fn fetch(connection: &Connection) -> Result<Self, ClientError>;

    pub fn negotiate<R: ProtocolRequest>(&self) -> Result<i16, ClientError>;
}
```

### Disconnect handling

When the reader sees EOF or an I/O error:
1. Signal shutdown to the writer (cancellation token).
2. Drain any pending correlation-ID waiters with
   `ClientError::Disconnected`.
3. Mark the `Connection` as `closed`; subsequent `send` calls return
   `ClientError::Disconnected` immediately.

The `BrokerPool` notices a closed connection (via a future-on-the-Arc
or by observing `send` errors) and drops its entry. Next `pool.get(id)`
creates a fresh connection.

# 5. Broker pool and discovery

### `BrokerPool`

```rust
pub struct BrokerPool {
    by_id: DashMap<i32, Arc<Connection>>,
    bootstrap: Vec<SocketAddr>,
    opts: ConnectionOptions,
}

pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

impl BrokerPool {
    pub fn new(bootstrap: Vec<SocketAddr>, opts: ConnectionOptions) -> Self;
    pub async fn get(&self, broker_id: i32) -> Result<Arc<Connection>, ClientError>;
    pub async fn bootstrap_connection(&self) -> Result<Arc<Connection>, ClientError>;
    pub async fn refresh_brokers(&self, brokers: &[BrokerInfo]);
    pub async fn close_all(self);
}
```

`get` is lazy: opens on first call, returns existing `Arc<Connection>`
on subsequent calls. Closed connections are evicted from the map so the
next `get` reconnects.

### Bootstrap discovery

`ClientBuilder` parses `"host:port,host:port,..."` and resolves each
via `tokio::net::lookup_host` at build time. At least one address must
resolve, or `build()` fails.

`Client::refresh_metadata` sends a `MetadataRequest` with
`topics: None` (all topics), parses the broker list from the response,
calls `pool.refresh_brokers`, returns the typed `MetadataResponse` to
the caller.

# 6. Test strategy

### Layer 1 — Mock broker (unit tests)

`crates/client-core/src/mock.rs`, gated to `#[cfg(any(test, feature = "mock"))]`:

```rust
pub struct MockBroker {
    pub addr: SocketAddr,
    handler: Arc<Mutex<Box<dyn FnMut(i16, i16, &[u8]) -> Vec<u8> + Send>>>,
    shutdown: CancellationToken,
}

impl MockBroker {
    pub async fn start<H>(handler: H) -> Self
    where H: FnMut(i16, i16, &[u8]) -> Vec<u8> + Send + 'static;
    pub async fn stop(self);
}
```

The mock binds to `127.0.0.1:0`, accepts connections, decodes incoming
length-prefixed frames, calls the handler closure with
`(api_key, version, request_body_bytes)`, frames the response with the
matching correlation ID, and writes it back.

Unit tests in `tests/unit.rs` cover (via `macro_rules! mock_test!` for
the repetitive shape):

- **Handshake.** Mock returns a hand-crafted `ApiVersionsResponse`;
  client populates its version table; `client.send(MetadataRequest)`
  works.
- **Round-trip.** Mock echoes a default `MetadataResponse`; client
  decodes it correctly.
- **Concurrent dispatch.** Two `client.send` calls in parallel; mock
  responds out of order; both callers get the right responses.
- **Timeout.** Handler never responds; client returns
  `ClientError::Timeout`.
- **Disconnect.** Mock closes its end; client returns
  `ClientError::Disconnected`.
- **Incompatible version.** Mock advertises a max version below the
  client's min for a specific API; client returns
  `ClientError::IncompatibleVersion`.

### Layer 2 — testcontainers integration (`#[ignore]`-gated)

`crates/client-core/tests/integration.rs`:

```rust
use testcontainers::ContainerAsync;
use testcontainers_modules::kafka::Kafka;

async fn start_kafka() -> ContainerAsync<Kafka> {
    Kafka::default().start().await.unwrap()
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn round_trip_api_versions_against_real_broker() { /* ... */ }

#[tokio::test]
#[ignore = "requires Docker"]
async fn metadata_against_real_broker_lists_brokers() { /* ... */ }

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_topic_via_admin_request() { /* ... */ }

#[tokio::test]
#[ignore = "requires Docker"]
async fn list_then_delete_topic() { /* ... */ }
```

`testcontainers-modules`'s `kafka` module wraps a maintained Apache
Kafka image. Tests are `#[ignore]`-gated so `cargo test --workspace`
doesn't pull Docker in normal runs.

### Layer 3 — Codegen drift / regression

The `ProtocolRequest` impls are emitted from the codegen. The existing
`drift` workflow verifies no manual edits leak in. The existing
differential and unit tests in `crabka-protocol` continue to pass —
the codegen change is purely additive.

# 7. CI

- **Existing `rust` matrix** picks up `crabka-client-core` for the
  Linux/macOS/Windows × Rust 1.95.0 sweep.
- **Existing `jvm-differential` job** unchanged (this slice doesn't
  touch the JVM oracle).
- **Existing `drift` workflow** picks up the new codegen output.
- **New `client-core-integration` job** runs Linux-only with Docker
  available; runs `cargo test -p crabka-client-core --tests -- --ignored`.

```yaml
# .github/workflows/ci.yml addition
  client-core-integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.95.0"
      - run: cargo test -p crabka-client-core --tests -- --ignored
```

macOS / Windows runners skip the integration job; testcontainers on
those platforms has known flakiness with Docker availability.

# 8. Acceptance criteria

The slice ships when **all** of these hold:

1. `crates/client-core/` exists with the modules listed in Section 1.
2. `crabka-protocol-codegen` emits `impl ProtocolRequest for <Request>`
   for every Request type; the trait is defined in
   `crabka-client-core::request`.
3. `Client::builder(bootstrap).build()` connects, negotiates API
   versions, and returns a usable client. Unit test via mock broker.
4. `Client::send<R>` round-trips for at least three different request
   types against the mock broker.
5. Concurrent dispatch handles out-of-order responses correctly (unit
   test).
6. `ClientError::{Timeout, Disconnected, IncompatibleVersion}` all
   surface for the right scenarios — no panics.
7. `BrokerPool::refresh_brokers` populates the pool from a
   `MetadataResponse`; `client.broker(id).send(...)` then connects to
   that broker (unit test via mock OR integration test against a
   single-broker cluster).
8. Integration tests against an `apache/kafka` testcontainers image
   pass at least four scenarios: ApiVersions, Metadata, CreateTopic,
   DeleteTopic.
9. New `client-core-integration` job runs Linux-only in CI.
10. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
    warnings`, `cargo test --workspace -- --include-ignored` all green.
11. No regressions in existing differential tests, protocol unit
    tests, or compression tests.
12. Rustdoc on every public type; crate-level doc explains the
    connection/pool/dispatch model.

# 9. Open questions deferred to the implementation plan

- **Exact `tokio` feature flag set.** Section 2 lists a reasonable
  starting set; the plan may add/remove as needed during execution.
- **Whether to expose `ConnectionOptions` as a struct or a builder.**
  Struct is simpler; builder might be ergonomic. The plan picks one
  during execution; revisitable.
- **Testcontainers Kafka tag.** The plan pins a specific Apache Kafka
  image tag at implementation time. Currently 4.2.0 matches the
  protocol pin; verify the tag is on Docker Hub and that
  `testcontainers-modules`'s default wait strategy works against it.

None block the design.

# 10. Next step

Invoke `writing-plans` to produce a detailed implementation plan for
slice 2. Slice 3 (`crabka-log`) is being brainstormed in parallel and
gets its own spec + plan.
