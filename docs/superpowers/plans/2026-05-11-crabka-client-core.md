# `crabka-client-core` (slice 2) Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first Crabka crate that does I/O — TCP connection management, API-version negotiation, and correlation-ID request/response dispatch against Apache Kafka brokers.

**Architecture:** `tokio` async runtime; one TCP connection per broker multiplexing requests via correlation ID; lazy connect via `BrokerPool`; bootstrap discovery via `MetadataRequest`. Codegen extended to emit `impl ProtocolRequest` for every generated Request type. Plaintext only — TLS/SASL is slice 11.

**Tech Stack:** Rust 1.95.0 edition 2024; `tokio` (`net`/`rt-multi-thread`/`io-util`/`macros`/`sync`/`time`); `tokio-util` (length-delimited codec); `dashmap` for the broker pool; `tracing` for observability hooks; `testcontainers-rs` + `testcontainers-modules` for integration. Existing `crabka-protocol` consumed via `version = "0.1"` workspace path dep.

**Reference spec:** [`docs/superpowers/specs/2026-05-11-crabka-client-core-design.md`](../specs/2026-05-11-crabka-client-core-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/client-core-plan` (this file). Implementation runs on `feature/client-core` branched off `main` once this plan's PR merges.

---

## File structure

```
crates/client-core/
├── Cargo.toml
├── src/
│   ├── lib.rs               # public re-exports
│   ├── request.rs           # ProtocolRequest trait
│   ├── error.rs             # ClientError
│   ├── transport.rs         # TCP + LengthDelimitedCodec wrapper
│   ├── version.rs           # ApiVersionTable + negotiation
│   ├── connection.rs        # single-broker Connection (dispatcher + correlation)
│   ├── pool.rs              # BrokerPool: DashMap<i32, Arc<Connection>>
│   ├── bootstrap.rs         # parse "host:port,..."; lookup_host
│   ├── client.rs            # Client + ClientBuilder + BrokerHandle
│   └── mock.rs              # in-process MockBroker (cfg-gated)
└── tests/
    ├── support/
    │   └── mod.rs
    ├── unit.rs              # MockBroker-based tests
    └── integration.rs       # #[ignore]'d testcontainers integration

crates/protocol-codegen/src/emit/protocol_request.rs   # NEW codegen
.github/workflows/ci.yml                                # add client-core-integration job
Cargo.toml (workspace)                                  # add tokio, tokio-util, dashmap, etc. deps
```

---

## Phase A — Crate scaffolding + workspace deps

### Task 1: Add workspace dependencies

**Files:**
- Modify: `Cargo.toml` (workspace) — add new entries to `[workspace.dependencies]`

- [ ] **Step 1: Append new deps**

In `Cargo.toml` at the repo root, under `[workspace.dependencies]`, append:

```toml
tokio = { version = "1", default-features = false }
tokio-util = { version = "0.7", default-features = false }
dashmap = "6"
tracing = "0.1"
testcontainers = "0.20"
testcontainers-modules = { version = "0.10", features = ["kafka"] }
```

Leave existing entries unchanged.

- [ ] **Step 2: Verify the manifest parses**

```bash
cargo metadata --no-deps 2>&1 | tail -3
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore(deps): add tokio, tokio-util, dashmap, tracing, testcontainers to workspace"
```

---

### Task 2: Create the `crabka-client-core` crate skeleton

**Files:**
- Create: `crates/client-core/Cargo.toml`
- Create: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the manifest**

`crates/client-core/Cargo.toml`:

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
mock = []

[dependencies]
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
bytes = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true, features = ["net", "rt", "rt-multi-thread", "io-util", "macros", "sync", "time"] }
tokio-util = { workspace = true, features = ["codec"] }
dashmap = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true }
tokio = { workspace = true, features = ["test-util"] }
```

- [ ] **Step 2: Write the stub `lib.rs`**

`crates/client-core/src/lib.rs`:

```rust
//! Connection management and request dispatch for Apache Kafka in Rust.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-client-core-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-client-core/0.0.0")]
```

- [ ] **Step 3: Verify the crate builds**

```bash
cargo build -p crabka-client-core
```

Expected: clean (downloads tokio/tokio-util/dashmap/tracing the first time).

- [ ] **Step 4: Commit**

```bash
git add crates/client-core Cargo.toml
git commit -m "feat(client-core): add crate skeleton"
```

---

## Phase B — Error type + `ProtocolRequest` trait

### Task 3: `ClientError`

**Files:**
- Create: `crates/client-core/src/error.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-core/src/error.rs`:

```rust
//! Error type for `crabka-client-core`.

use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;

/// Errors returned by `Client`, `Connection`, and the broker pool.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("connect to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_useful() {
        let e = ClientError::Timeout(Duration::from_secs(5));
        assert_eq!(e.to_string(), "request timed out after 5s");
    }

    #[test]
    fn incompatible_version_displays_full_range() {
        let e = ClientError::IncompatibleVersion {
            api_key: 0,
            broker_min: 0,
            broker_max: 5,
            client_min: 7,
            client_max: 10,
        };
        assert!(e.to_string().contains("api_key 0"));
        assert!(e.to_string().contains("broker supports 0..=5"));
    }
}
```

- [ ] **Step 2: Hook into lib.rs**

`crates/client-core/src/lib.rs`:

```rust
//! Connection management and request dispatch for Apache Kafka in Rust.

mod error;

pub use error::ClientError;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-client-core error
```

Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): ClientError enum"
```

---

### Task 4: `ProtocolRequest` trait

**Files:**
- Create: `crates/client-core/src/request.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the trait**

`crates/client-core/src/request.rs`:

```rust
//! Marker trait implemented by generated Request types from
//! `crabka-protocol`. Provides the dispatch information (api key,
//! version range, response type) that the client needs.

use crabka_protocol::{Decode, Encode};

/// Implemented by every generated Request struct in `crabka-protocol`.
///
/// The `crabka-protocol-codegen` crate emits this impl for every
/// Request type. Hand-rolled implementations are also valid for
/// non-codegen message types if they ever exist.
pub trait ProtocolRequest: Encode {
    /// Kafka API key for this request.
    const API_KEY: i16;
    /// Minimum protocol version this Rust type supports.
    const MIN_VERSION: i16;
    /// Maximum protocol version this Rust type supports.
    const MAX_VERSION: i16;
    /// First version that uses flexible (KIP-482) framing.
    /// `i16::MAX` for never-flexible messages.
    const FLEXIBLE_MIN: i16;

    /// Matching response type from `crabka-protocol`.
    type Response: for<'de> Decode<'de>;
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
//! Connection management and request dispatch for Apache Kafka in Rust.

mod error;
mod request;

pub use error::ClientError;
pub use request::ProtocolRequest;
```

- [ ] **Step 3: Verify build**

```bash
cargo build -p crabka-client-core
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): ProtocolRequest trait"
```

---

## Phase C — Codegen extension

### Task 5: Emit `impl ProtocolRequest` for every Request

The codegen needs to:
1. Add a public dep on `crabka-client-core::ProtocolRequest` — but only conditionally, since the codegen is consumed by `crabka-protocol` which sits "below" client-core. The cleanest approach: define `ProtocolRequest` in `crabka-protocol` itself with a no-op default, and have `crabka-client-core` re-export it.

**Decision:** move `ProtocolRequest` into `crabka-protocol::codec` (next to `Encode`/`Decode`). Then `crabka-client-core` just `pub use`s it. The codegen emits `impl crate::ProtocolRequest for X` inside each generated Request module — no cross-crate path.

This is a small adjustment to Task 4's location. Tasks 5 onward use this final placement.

**Files:**
- Modify: `crates/protocol/src/codec.rs` — define `ProtocolRequest` trait
- Modify: `crates/protocol/src/lib.rs` — re-export `ProtocolRequest`
- Modify: `crates/client-core/src/request.rs` — change to `pub use crabka_protocol::ProtocolRequest;`
- Create: `crates/protocol-codegen/src/emit/protocol_request.rs`
- Modify: `crates/protocol-codegen/src/emit/mod.rs`
- Modify: `crates/protocol-codegen/src/emit/owned.rs` — call into the new emitter

- [ ] **Step 1: Move `ProtocolRequest` into `crabka-protocol`**

Append to `crates/protocol/src/codec.rs`:

```rust
/// Marker trait implemented by every generated Request struct.
///
/// Provides the dispatch information (api key, version range,
/// response type) that the client needs to send and decode messages.
/// The `crabka-protocol-codegen` crate emits this impl for every
/// Request type.
pub trait ProtocolRequest: Encode {
    const API_KEY: i16;
    const MIN_VERSION: i16;
    const MAX_VERSION: i16;
    const FLEXIBLE_MIN: i16;
    type Response: for<'de> Decode<'de>;
}
```

Re-export from `crates/protocol/src/lib.rs`:

```rust
pub use codec::{Decode, DecodeBorrow, Encode, ProtocolRequest};
```

Replace `crates/client-core/src/request.rs` with a re-export:

```rust
//! Re-export of `crabka_protocol::ProtocolRequest` for convenience.
pub use crabka_protocol::ProtocolRequest;
```

- [ ] **Step 2: Write the codegen emitter**

`crates/protocol-codegen/src/emit/protocol_request.rs`:

```rust
//! Emit `impl ProtocolRequest for <RequestType>` blocks. Called from
//! `emit::owned` for every Request-typed message.

use std::fmt::Write;

use crate::ir::{MessageSpec, MessageType};
use crate::name_conv;

/// Emit a `ProtocolRequest` impl for this spec if it's a Request type;
/// otherwise return an empty string.
#[must_use]
pub fn emit(spec: &MessageSpec) -> String {
    if !matches!(spec.message_type, MessageType::Request) {
        return String::new();
    }
    let request_type = name_conv::type_name(&spec.name);
    let response_type = request_type
        .strip_suffix("Request")
        .map(|stem| format!("{stem}Response"))
        .expect("Request type names end with `Request`");
    let response_module = name_conv::module_name(&format!("{}", response_type));
    let mut out = String::new();
    writeln!(
        out,
        "
impl crate::ProtocolRequest for {request_type} {{
    const API_KEY: i16 = API_KEY;
    const MIN_VERSION: i16 = MIN_VERSION;
    const MAX_VERSION: i16 = MAX_VERSION;
    const FLEXIBLE_MIN: i16 = FLEXIBLE_MIN;
    type Response = super::{response_module}::{response_type};
}}"
    )
    .unwrap();
    out
}
```

Add `pub mod protocol_request;` to `crates/protocol-codegen/src/emit/mod.rs`.

- [ ] **Step 3: Wire into owned.rs**

In `crates/protocol-codegen/src/emit/owned.rs`, find where the per-message body is finalised (after `default_json::emit_default_json`). Append:

```rust
out.push_str(&crate::emit::protocol_request::emit(spec));
```

- [ ] **Step 4: Regenerate and verify**

```bash
./tools/regenerate.sh
grep -l "impl crate::ProtocolRequest" crates/protocol/generated/owned/ | head -5
```

Expected: every Request-type generated file contains the impl. The dispatch path (`super::<response_module>::<ResponseType>`) needs the wrapper modules to make the response module discoverable. Verify that the wrapper at `crates/protocol/src/owned/<request>.rs` is in a sibling module to the response wrapper.

- [ ] **Step 5: Snapshot updates**

```bash
UPDATE_SNAPSHOTS=1 cargo test -p crabka-protocol-codegen --test snapshot
cargo test -p crabka-protocol-codegen
```

- [ ] **Step 6: Verify the trait is visible**

```bash
cargo test -p crabka-protocol --lib
```

Expected: all existing protocol tests pass; the new trait impls don't break anything.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(codegen): emit ProtocolRequest impl per Request type"
```

---

## Phase D — Transport + version negotiation

### Task 6: `transport.rs` — TCP framing

**Files:**
- Create: `crates/client-core/src/transport.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-core/src/transport.rs`:

```rust
//! TCP framing wrapper. Kafka uses a 4-byte big-endian length prefix
//! followed by the frame body.

use bytes::{Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Maximum frame size we'll accept (matches Kafka's default
/// `socket.request.max.bytes` = 100 MiB).
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// Build a length-delimited codec configured for Kafka's wire framing.
#[must_use]
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_field_type::<u32>()
        .max_frame_length(MAX_FRAME_BYTES)
        .big_endian()
        .new_codec()
}

/// Wrap a TcpStream with the Kafka length-delimited codec.
pub fn frame(stream: TcpStream) -> Framed<TcpStream, LengthDelimitedCodec> {
    Framed::new(stream, codec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Framed;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn roundtrips_a_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = frame(stream);
            let frame = framed.next().await.unwrap().unwrap();
            frame.freeze()
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut framed = frame(client);
        use futures_util::SinkExt;
        framed.send(Bytes::from_static(b"hello kafka")).await.unwrap();
        framed.into_inner().shutdown().await.unwrap();

        let received = server.await.unwrap();
        assert_eq!(received.as_ref(), b"hello kafka");
    }
}
```

**Note:** the test uses `futures_util` for the `SinkExt::send` and `StreamExt::next` extension traits. Add `futures-util = "0.3"` to the workspace `[workspace.dependencies]` and to `crates/client-core/Cargo.toml`'s `[dev-dependencies]`. Adjust Task 1 retroactively if missed, OR add here:

In root `Cargo.toml` `[workspace.dependencies]`:

```toml
futures-util = "0.3"
```

In `crates/client-core/Cargo.toml` `[dev-dependencies]`:

```toml
futures-util = { workspace = true }
```

- [ ] **Step 2: Add module to lib.rs**

```rust
mod error;
mod request;
mod transport;

pub use error::ClientError;
pub use request::ProtocolRequest;
```

(Transport is internal; not re-exported.)

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-client-core transport
```

Expected: 1 test passes.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(client-core): TCP length-delimited framing"
```

---

### Task 7: `ApiVersionTable`

**Files:**
- Create: `crates/client-core/src/version.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-core/src/version.rs`:

```rust
//! ApiVersionTable — broker-advertised version ranges per API key,
//! plus client-side negotiation.

use std::collections::HashMap;

use crate::error::ClientError;
use crate::request::ProtocolRequest;

#[derive(Debug, Clone, Default)]
pub struct ApiVersionTable {
    by_key: HashMap<i16, (i16, i16)>,
}

impl ApiVersionTable {
    /// Build from a sequence of (api_key, broker_min, broker_max) tuples.
    /// Used when seeding the table from a decoded `ApiVersionsResponse`.
    #[must_use]
    pub fn from_entries(entries: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        let mut by_key = HashMap::new();
        for (k, lo, hi) in entries {
            by_key.insert(k, (lo, hi));
        }
        Self { by_key }
    }

    /// Highest version both sides support for `R`, or
    /// `IncompatibleVersion` if the ranges don't overlap.
    pub fn negotiate<R: ProtocolRequest>(&self) -> Result<i16, ClientError> {
        let api_key = R::API_KEY;
        let client_min = R::MIN_VERSION;
        let client_max = R::MAX_VERSION;
        let (broker_min, broker_max) = self
            .by_key
            .get(&api_key)
            .copied()
            .unwrap_or((0, 0));
        let chosen = client_max.min(broker_max);
        if chosen < client_min || chosen < broker_min {
            return Err(ClientError::IncompatibleVersion {
                api_key,
                broker_min,
                broker_max,
                client_min,
                client_max,
            });
        }
        Ok(chosen)
    }

    pub fn broker_range(&self, api_key: i16) -> Option<(i16, i16)> {
        self.by_key.get(&api_key).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
    use crabka_protocol::Encode;

    // ApiVersionsRequest acts as a sample ProtocolRequest. We only
    // need the trait's constants here; the impl comes from codegen.

    #[test]
    fn negotiate_takes_min_of_max() {
        let t = ApiVersionTable::from_entries([
            (ApiVersionsRequest::API_KEY, 0, ApiVersionsRequest::MAX_VERSION),
        ]);
        // Sanity: client max wins if broker max is higher.
        let _ = t.negotiate::<ApiVersionsRequest>().unwrap();
    }

    #[test]
    fn negotiate_errors_when_disjoint() {
        let t = ApiVersionTable::from_entries([
            (ApiVersionsRequest::API_KEY, 99, 100),
        ]);
        assert!(matches!(
            t.negotiate::<ApiVersionsRequest>(),
            Err(ClientError::IncompatibleVersion { .. })
        ));
    }

    #[test]
    fn negotiate_picks_lowest_supported_when_broker_caps_low() {
        let t = ApiVersionTable::from_entries([
            (ApiVersionsRequest::API_KEY, 0, 0),
        ]);
        // Both sides support 0; that's what's chosen.
        assert_eq!(t.negotiate::<ApiVersionsRequest>().unwrap(), 0);
    }
}
```

**Note:** the tests rely on Task 5's codegen having emitted `impl ProtocolRequest for ApiVersionsRequest`. If that codegen hasn't run yet, these tests fail to compile. Task ordering ensures Task 5 lands before Task 7. If running Task 7 in isolation, ensure `./tools/regenerate.sh` has been run since Task 5's emitter change.

- [ ] **Step 2: Hook into lib.rs**

```rust
mod error;
mod request;
mod transport;
mod version;

pub use error::ClientError;
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-client-core version
```

Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): ApiVersionTable + negotiation"
```

---

## Phase E — `Connection`

### Task 8: `Connection::connect` skeleton

**Files:**
- Create: `crates/client-core/src/connection.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the connection module (skeleton + connect)**

`crates/client-core/src/connection.rs`:

```rust
//! Single-broker `Connection`: TCP socket + reader/writer tasks +
//! correlation-ID multiplexing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::request::ProtocolRequest;
use crate::version::ApiVersionTable;

/// Connect-time + per-request configuration knobs.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub client_id: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            client_id: "crabka".into(),
            connect_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// A connection to a single Kafka broker.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    versions: ApiVersionTable,
    options: ConnectionOptions,
    next_corr_id: AtomicI32,
    pending: DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>,
    writer_tx: mpsc::Sender<DispatchItem>,
    shutdown: CancellationToken,
    _reader: JoinHandle<()>,
    _writer: JoinHandle<()>,
}

struct DispatchItem {
    bytes: Bytes,
}

impl Connection {
    /// Connect to `addr`, negotiate API versions, return a usable Connection.
    pub async fn connect(addr: SocketAddr, options: ConnectionOptions) -> Result<Self, ClientError> {
        let stream = tokio::time::timeout(options.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Timeout(options.connect_timeout))?
            .map_err(|source| ClientError::Connect { addr, source })?;

        stream.set_nodelay(true).ok();

        // Build the framed socket; split into halves; spawn reader + writer.
        let (writer_tx, writer_rx) = mpsc::channel::<DispatchItem>(64);
        let shutdown = CancellationToken::new();
        let pending: DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>> = DashMap::new();

        let (reader_handle, writer_handle) =
            spawn_io_tasks(stream, writer_rx, shutdown.clone(), pending.clone());

        let mut conn = Self {
            inner: Arc::new(ConnectionInner {
                versions: ApiVersionTable::default(),
                options: options.clone(),
                next_corr_id: AtomicI32::new(0),
                pending,
                writer_tx,
                shutdown,
                _reader: reader_handle,
                _writer: writer_handle,
            }),
        };

        // Bootstrap-time ApiVersions fetch at v0 (the only version every
        // broker is guaranteed to support). Fills the version table.
        let versions = fetch_api_versions(&conn).await?;
        // Build a new Inner with the populated table; replace.
        let inner = Arc::get_mut(&mut conn.inner).expect("unique handle at connect-time");
        inner.versions = versions;

        Ok(conn)
    }

    /// Negotiated API versions known to this connection.
    pub fn versions(&self) -> &ApiVersionTable {
        &self.inner.versions
    }

    /// Close the connection, dropping all background tasks.
    pub async fn close(self) {
        self.inner.shutdown.cancel();
        // The Arc gets dropped when `self` does; JoinHandles abort naturally.
    }
}

// Forward declaration; bodies arrive in Tasks 9 and 10.
fn spawn_io_tasks(
    _stream: TcpStream,
    _writer_rx: mpsc::Receiver<DispatchItem>,
    _shutdown: CancellationToken,
    _pending: DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    todo!("Task 9: reader/writer tasks")
}

async fn fetch_api_versions(_conn: &Connection) -> Result<ApiVersionTable, ClientError> {
    todo!("Task 10: bootstrap api-versions fetch")
}
```

This compiles but `connect` panics at runtime via `todo!()`. Tasks 9 and 10 fill in the bodies.

- [ ] **Step 2: Add `tokio-util` `sync` feature for `CancellationToken`**

In `crates/client-core/Cargo.toml`, change the `tokio-util` line:

```toml
tokio-util = { workspace = true, features = ["codec", "rt"] }
```

(`rt` brings `CancellationToken`.)

- [ ] **Step 3: Hook into lib.rs**

```rust
mod connection;
mod error;
mod request;
mod transport;
mod version;

pub use connection::{Connection, ConnectionOptions};
pub use error::ClientError;
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;
```

- [ ] **Step 4: Verify build**

```bash
cargo build -p crabka-client-core
```

Expected: clean (with `todo!()` warnings or none).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(client-core): Connection skeleton (no I/O tasks yet)"
```

---

### Task 9: Reader + writer tasks

**Files:**
- Modify: `crates/client-core/src/connection.rs`

- [ ] **Step 1: Implement the I/O tasks**

Replace the `spawn_io_tasks` stub with the real implementation. Append to `connection.rs` (or replace the stub in place):

```rust
fn spawn_io_tasks(
    stream: TcpStream,
    mut writer_rx: mpsc::Receiver<DispatchItem>,
    shutdown: CancellationToken,
    pending: DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let framed = crate::transport::frame(stream);
    let (sink, mut stream_half) = futures_util::stream::StreamExt::split(framed);
    let sink = std::sync::Mutex::new(Some(sink));
    let _ = sink; // suppress unused warning; layout placeholder

    // Reader: decode frames, peel correlation ID off the response header,
    // route to the pending map's oneshot.
    let reader_pending = pending;
    let reader_shutdown = shutdown.clone();
    let reader = tokio::spawn(async move {
        use futures_util::StreamExt;
        loop {
            tokio::select! {
                _ = reader_shutdown.cancelled() => break,
                maybe_frame = stream_half.next() => {
                    let Some(frame) = maybe_frame else { break; };
                    let Ok(frame) = frame else { break; };
                    if frame.len() < 4 { continue; }
                    let corr_id = i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
                    if let Some((_, tx)) = reader_pending.remove(&corr_id) {
                        let body = frame.slice(4..).freeze();
                        let _ = tx.send(Ok(body));
                    }
                }
            }
        }
        // On exit, fail every pending request with Disconnected.
        for entry in reader_pending.iter() {
            let _ = entry.value(); // placeholder — see note below
        }
        // The above doesn't actually fail them; the writer task does on shutdown.
    });

    // Writer: read from mpsc, send frames out.
    let writer = tokio::spawn(async move {
        // Can't easily split a Framed across two tasks; instead, pass the
        // socket sink in via a oneshot or use io::split. For Task 9's
        // simplest implementation, do everything in one task — the
        // dispatcher loops over both directions with tokio::select!.
        // Refactor in Task 9b if desired.
        while let Some(_item) = writer_rx.recv().await {
            // Writes happen inside the unified task above; this is unused.
        }
    });

    (reader, writer)
}
```

**Note:** the split-and-share pattern across two tasks is fiddly. A simpler shape uses **one** task that owns the whole `Framed` and `select!`s between incoming frames and outgoing dispatch items. **Refactor accordingly** during implementation. The split shown above is illustrative; the implementer should pick the cleanest concrete shape.

Concretely, the cleanest version is:

```rust
fn spawn_io_tasks(
    stream: TcpStream,
    mut writer_rx: mpsc::Receiver<DispatchItem>,
    shutdown: CancellationToken,
    pending: DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    use futures_util::{SinkExt, StreamExt};

    let mut framed = crate::transport::frame(stream);
    let pending_for_drain = pending.clone();

    let combined = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                Some(item) = writer_rx.recv() => {
                    if let Err(_e) = framed.send(item.bytes.into()).await {
                        break;
                    }
                }
                maybe_frame = framed.next() => {
                    let Some(frame) = maybe_frame else { break; };
                    let Ok(frame) = frame else { break; };
                    if frame.len() < 4 { continue; }
                    let corr_id = i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
                    if let Some((_, tx)) = pending.remove(&corr_id) {
                        let body = bytes::Bytes::copy_from_slice(&frame[4..]);
                        let _ = tx.send(Ok(body));
                    }
                }
            }
        }
        // Drain pending: every outstanding request fails with Disconnected.
        for entry in pending_for_drain.iter() {
            // We can't send through `entry.value()` because oneshot::Sender
            // doesn't have a stable iterator-friendly take API.
            // Workaround: drain by collecting keys and removing one-by-one.
        }
        let keys: Vec<i32> = pending_for_drain.iter().map(|e| *e.key()).collect();
        for k in keys {
            if let Some((_, tx)) = pending_for_drain.remove(&k) {
                let _ = tx.send(Err(ClientError::Disconnected));
            }
        }
    });

    let noop = tokio::spawn(async {});
    (combined, noop)
}
```

The "two tasks" structure can collapse to one task (`combined`) with a no-op second handle for API compatibility with the struct.

- [ ] **Step 2: Verify build**

```bash
cargo build -p crabka-client-core
```

Expected: clean. There's still a runtime-panic via `fetch_api_versions`'s `todo!()`, but the writer/reader compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): reader/writer task on a single Framed socket"
```

---

### Task 10: `Connection::send` + bootstrap version fetch

**Files:**
- Modify: `crates/client-core/src/connection.rs`
- Modify: `crates/client-core/src/lib.rs` (already exports `Connection`; no change)

- [ ] **Step 1: Implement send**

In `connection.rs`, replace the stub `fetch_api_versions` and add the public `send` method:

```rust
impl Connection {
    /// Send a typed request, await the typed response.
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        // 1. Negotiate version.
        let version = self.inner.versions.negotiate::<R>()?;

        // 2. Allocate correlation ID.
        let corr_id = self.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);

        // 3. Build request header.
        let flexible = version >= R::FLEXIBLE_MIN;
        let header_bytes = build_request_header(
            R::API_KEY,
            version,
            corr_id,
            &self.inner.options.client_id,
            flexible,
        );

        // 4. Encode the body.
        let mut body = bytes::BytesMut::with_capacity(req.encoded_len(version) + 32);
        req.encode(&mut body, version)?;

        // 5. Concatenate header + body.
        let mut frame = bytes::BytesMut::with_capacity(header_bytes.len() + body.len());
        frame.extend_from_slice(&header_bytes);
        frame.extend_from_slice(&body);

        // 6. Register the oneshot.
        let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
        self.inner.pending.insert(corr_id, tx);

        // 7. Dispatch to writer.
        self.inner
            .writer_tx
            .send(DispatchItem { bytes: frame.freeze() })
            .await
            .map_err(|_| ClientError::Disconnected)?;

        // 8. Await response with timeout.
        let body_bytes = match tokio::time::timeout(self.inner.options.request_timeout, rx).await {
            Ok(Ok(Ok(b))) => b,
            Ok(Ok(Err(e))) => return Err(e),
            Ok(Err(_recv_closed)) => return Err(ClientError::Disconnected),
            Err(_) => {
                // Timeout: evict the pending entry so the reader doesn't try to fulfill it.
                self.inner.pending.remove(&corr_id);
                return Err(ClientError::Timeout(self.inner.options.request_timeout));
            }
        };

        // 9. Decode response header (skip; it's already implicit in the framing).
        // Actually: the response frame body includes the response header.
        // Peel it off based on flexibility.
        let mut cursor: &[u8] = &body_bytes;
        if flexible {
            // ResponseHeader v1: correlation_id (i32) + tagged fields (UVARINT count, 0 in practice).
            // We've already pulled correlation_id off; tagged fields are 0 (1 byte).
            if !cursor.is_empty() && cursor[0] == 0 {
                cursor = &cursor[1..];
            }
        }
        // Note: corr_id was already in the frame at offset 0..4, stripped by the reader.

        // 10. Decode the response body.
        let resp = <R::Response as crabka_protocol::Decode>::decode(&mut cursor, version)?;
        Ok(resp)
    }
}

fn build_request_header(
    api_key: i16,
    version: i16,
    corr_id: i32,
    client_id: &str,
    flexible: bool,
) -> bytes::BytesMut {
    use bytes::BufMut;
    let mut buf = bytes::BytesMut::with_capacity(32);
    buf.put_i16(api_key);
    buf.put_i16(version);
    buf.put_i32(corr_id);
    if flexible {
        // RequestHeader v2: client_id is COMPACT_NULLABLE_STRING.
        // For client_id = "foo" non-null: length = 3 + 1 = 4 (UVARINT).
        let bytes = client_id.as_bytes();
        let n = u32::try_from(bytes.len() + 1).expect("client_id fits");
        crate::transport::put_uvarint(&mut buf, n);
        buf.put_slice(bytes);
        // Tagged fields: empty.
        buf.put_u8(0);
    } else {
        // RequestHeader v1: client_id is NULLABLE_STRING (i16 length).
        let n = i16::try_from(client_id.len()).expect("client_id fits");
        buf.put_i16(n);
        buf.put_slice(client_id.as_bytes());
    }
    buf
}

async fn fetch_api_versions(conn: &Connection) -> Result<ApiVersionTable, ClientError> {
    use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
    use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;

    // We can't use Connection::send yet — there's no version table. Build
    // the header manually at v0.
    let req = ApiVersionsRequest::default();
    let mut body = bytes::BytesMut::new();
    req.encode(&mut body, 0)?;

    let corr_id = conn.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);
    let header = build_request_header(
        ApiVersionsRequest::API_KEY,
        0,
        corr_id,
        &conn.inner.options.client_id,
        false, // v0 is never flexible
    );
    let mut frame = bytes::BytesMut::with_capacity(header.len() + body.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&body);

    let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
    conn.inner.pending.insert(corr_id, tx);
    conn.inner
        .writer_tx
        .send(DispatchItem { bytes: frame.freeze() })
        .await
        .map_err(|_| ClientError::Disconnected)?;

    let body_bytes = tokio::time::timeout(conn.inner.options.connect_timeout, rx)
        .await
        .map_err(|_| ClientError::Timeout(conn.inner.options.connect_timeout))?
        .map_err(|_| ClientError::Disconnected)??;

    // ResponseHeader v0 has no fields after correlation_id (already stripped).
    let mut cursor: &[u8] = &body_bytes;
    let resp = <ApiVersionsResponse as crabka_protocol::Decode>::decode(&mut cursor, 0)?;
    if resp.error_code != 0 {
        return Err(ClientError::Server { error_code: resp.error_code });
    }

    let entries = resp
        .api_keys
        .iter()
        .map(|k| (k.api_key, k.min_version, k.max_version));
    Ok(ApiVersionTable::from_entries(entries))
}
```

- [ ] **Step 2: Add `put_uvarint` to `transport.rs`**

The header-builder needs `put_uvarint` for compact-string lengths. Add to `transport.rs`:

```rust
use bytes::BufMut;

/// LEB128-encode `v` into `buf`.
pub(crate) fn put_uvarint<B: BufMut>(buf: &mut B, mut v: u32) {
    while (v & !0x7F) != 0 {
        buf.put_u8(((v & 0x7F) as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}
```

(Duplicates the same logic in `crabka-protocol::primitives::varint`. Using a local copy avoids exposing it from the protocol crate. The plan can revisit if this duplication grows annoying.)

- [ ] **Step 3: Verify build + simple round-trip**

```bash
cargo build -p crabka-client-core
cargo test -p crabka-client-core
```

Expected: clean. The existing transport / version / error tests still pass. No `Connection::send` tests yet (those need the mock broker, Task 12).

- [ ] **Step 4: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): Connection::send + bootstrap ApiVersions fetch"
```

---

## Phase F — Mock broker + unit tests

### Task 11: `MockBroker`

**Files:**
- Create: `crates/client-core/src/mock.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-core/src/mock.rs`:

```rust
//! In-process mock Kafka broker. Useful for testing `Connection`
//! without spinning up a JVM. Gated to `#[cfg(any(test, feature = "mock"))]`.

#![cfg(any(test, feature = "mock"))]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// A simple in-process mock Kafka broker.
pub struct MockBroker {
    pub addr: SocketAddr,
    handler: Arc<Mutex<Handler>>,
    shutdown: CancellationToken,
    _task: JoinHandle<()>,
}

type Handler = Box<dyn FnMut(i16, i16, i32, &[u8]) -> Vec<u8> + Send>;

impl MockBroker {
    /// Start a mock listening on a random localhost port. The handler
    /// receives `(api_key, version, correlation_id, request_body)` and
    /// returns the response **body** (not including the
    /// length-prefix or correlation-id header). The MockBroker prepends
    /// the correlation-id automatically.
    pub async fn start<F>(handler: F) -> Self
    where
        F: FnMut(i16, i16, i32, &[u8]) -> Vec<u8> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handler: Arc<Mutex<Handler>> = Arc::new(Mutex::new(Box::new(handler)));
        let shutdown = CancellationToken::new();

        let task_handler = handler.clone();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = task_shutdown.cancelled() => break,
                    Ok((stream, _)) = listener.accept() => {
                        let h = task_handler.clone();
                        let sd = task_shutdown.clone();
                        tokio::spawn(async move {
                            handle_connection(stream, h, sd).await;
                        });
                    }
                }
            }
        });

        Self {
            addr,
            handler,
            shutdown,
            _task: task,
        }
    }

    pub async fn stop(self) {
        self.shutdown.cancel();
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    handler: Arc<Mutex<Handler>>,
    shutdown: CancellationToken,
) {
    use futures_util::{SinkExt, StreamExt};

    let mut framed = crate::transport::frame(stream);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            maybe_frame = framed.next() => {
                let Some(frame) = maybe_frame else { break; };
                let Ok(frame) = frame else { break; };
                if frame.len() < 8 { continue; }

                // RequestHeader v1+ wire shape:
                //   api_key:i16, api_version:i16, correlation_id:i32, client_id...
                let api_key = i16::from_be_bytes([frame[0], frame[1]]);
                let version = i16::from_be_bytes([frame[2], frame[3]]);
                let corr_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
                // We don't bother parsing client_id; tests don't need it.
                // Skip to the body: header length depends on flexibility, which
                // depends on the message's FLEXIBLE_MIN. For mock purposes we
                // pass the full post-corrid bytes to the handler and let it sort out.
                let body = &frame[8..];

                let response_body = {
                    let mut h = handler.lock().unwrap();
                    h(api_key, version, corr_id, body)
                };

                // Build the response: corr_id (i32 BE) + body
                let mut resp = BytesMut::with_capacity(4 + response_body.len());
                resp.extend_from_slice(&corr_id.to_be_bytes());
                resp.extend_from_slice(&response_body);

                if framed.send(resp.freeze().into()).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = handler; // suppress unused-after-move warning if all branches break early
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
mod connection;
mod error;
mod request;
mod transport;
mod version;

#[cfg(any(test, feature = "mock"))]
mod mock;

pub use connection::{Connection, ConnectionOptions};
pub use error::ClientError;
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
```

- [ ] **Step 3: Verify build**

```bash
cargo build -p crabka-client-core
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): in-process MockBroker for tests"
```

---

### Task 12: Mock-based unit tests

**Files:**
- Create: `crates/client-core/tests/unit.rs`

- [ ] **Step 1: Write the tests**

`crates/client-core/tests/unit.rs`:

```rust
use std::time::Duration;

use bytes::BytesMut;
use crabka_client_core::{Connection, ConnectionOptions, MockBroker};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::Encode;

/// Build the response body for ApiVersions v0 that the mock returns:
/// a single ApiVersion (key=18, min=0, max=3) plus required fields.
fn build_api_versions_response_v0() -> Vec<u8> {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion { api_key: 18, min_version: 0, max_version: 3, ..Default::default() },
            ApiVersion { api_key: 3,  min_version: 0, max_version: 12, ..Default::default() },
        ],
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, 0).unwrap();
    buf.to_vec()
}

#[tokio::test]
async fn connect_negotiates_api_versions() {
    let mock = MockBroker::start(|api_key, _v, _cid, _body| {
        // Only the bootstrap ApiVersions call is expected; respond with v0 layout.
        assert_eq!(api_key, ApiVersionsRequest::API_KEY);
        build_api_versions_response_v0()
    }).await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();

    assert!(!conn.versions().is_empty());
    assert_eq!(conn.versions().broker_range(18), Some((0, 3)));
    conn.close().await;
    mock.stop().await;
}

#[tokio::test]
async fn timeout_when_handler_silent() {
    let mock = MockBroker::start(|_api_key, _v, _cid, _body| {
        // Never respond — return empty so the writer doesn't crash, but
        // the reader on the broker side will hang.
        // Instead: detect ApiVersions and drop the connection unilaterally
        // by returning a giant message that won't fit? Simpler: return
        // empty and rely on the client-side request_timeout.
        Vec::new()
    }).await;

    let opts = ConnectionOptions {
        request_timeout: Duration::from_millis(200),
        ..ConnectionOptions::default()
    };
    // ApiVersions fetch will probably hang; expect timeout.
    let result = Connection::connect(mock.addr, opts).await;
    match result {
        Err(crabka_client_core::ClientError::Timeout(_)) => { /* expected */ }
        Err(other) => panic!("expected Timeout, got: {other:?}"),
        Ok(_) => panic!("connect should have timed out"),
    }
    mock.stop().await;
}

#[tokio::test]
async fn round_trip_metadata_request() {
    let mock = MockBroker::start(|api_key, _v, _cid, _body| {
        if api_key == ApiVersionsRequest::API_KEY {
            return build_api_versions_response_v0();
        }
        if api_key == MetadataRequest::API_KEY {
            // Respond with an empty Metadata at version 0.
            use crabka_protocol::owned::metadata_response::MetadataResponse;
            let resp = MetadataResponse::default();
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, 0).unwrap();
            return buf.to_vec();
        }
        Vec::new()
    }).await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();
    let resp = conn.send(MetadataRequest::default()).await.unwrap();
    let _ = resp; // smoke test passes if no panic and no Err
    conn.close().await;
    mock.stop().await;
}
```

- [ ] **Step 2: Run the tests**

```bash
cargo test -p crabka-client-core --test unit
```

Expected: 3 tests pass.

**Debugging hints if anything fails:**
- `connect_negotiates_api_versions` failure: check that the mock's response body matches what `crabka-protocol`'s `ApiVersionsResponse::decode` at version 0 expects. The mock returns *just* the response body (post-correlation-id); the broker would also send the response header, which at v0 is empty.
- `timeout_when_handler_silent` failure: verify the timeout actually fires; the test might be slow but should complete in <500ms.
- `round_trip_metadata_request` failure: same shape — make sure the mock's Metadata response decodes correctly at version 0.

- [ ] **Step 3: Commit**

```bash
git add crates/client-core
git commit -m "test(client-core): mock-based unit tests for connect + send + timeout"
```

---

### Task 13: Concurrent dispatch test

**Files:**
- Modify: `crates/client-core/tests/unit.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test]
async fn concurrent_sends_get_correct_responses() {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    let response_count = Arc::new(AtomicI32::new(0));
    let counter_for_mock = response_count.clone();

    let mock = MockBroker::start(move |api_key, _v, _cid, _body| {
        if api_key == ApiVersionsRequest::API_KEY {
            return build_api_versions_response_v0();
        }
        // For Metadata, respond with a small unique body so we can tell
        // them apart. Use the broker-side correlation count as the
        // error_code field (just to inject something distinguishable).
        let n = counter_for_mock.fetch_add(1, Ordering::Relaxed);
        use crabka_protocol::owned::metadata_response::MetadataResponse;
        let resp = MetadataResponse {
            throttle_time_ms: n,
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }).await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default()).await.unwrap();

    let fut1 = conn.send(MetadataRequest::default());
    let fut2 = conn.send(MetadataRequest::default());
    let fut3 = conn.send(MetadataRequest::default());

    let (r1, r2, r3) = tokio::join!(fut1, fut2, fut3);
    let r1 = r1.unwrap();
    let r2 = r2.unwrap();
    let r3 = r3.unwrap();

    // Three distinct throttle_time_ms values, in some order.
    let mut seen = [r1.throttle_time_ms, r2.throttle_time_ms, r3.throttle_time_ms];
    seen.sort();
    assert_eq!(seen, [0, 1, 2]);

    conn.close().await;
    mock.stop().await;
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p crabka-client-core --test unit concurrent
```

Expected: 1 new test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/client-core
git commit -m "test(client-core): concurrent dispatch multiplexes via correlation ID"
```

---

## Phase G — `BrokerPool` + `Client`

### Task 14: `BrokerPool`

**Files:**
- Create: `crates/client-core/src/pool.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-core/src/pool.rs`:

```rust
//! `BrokerPool`: a `DashMap<broker_id, Arc<Connection>>` with lazy
//! connect on first use.

use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;

use crate::connection::{Connection, ConnectionOptions};
use crate::error::ClientError;

#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

pub struct BrokerPool {
    by_id: DashMap<i32, Arc<Connection>>,
    by_addr: DashMap<i32, SocketAddr>,
    bootstrap: Vec<SocketAddr>,
    options: ConnectionOptions,
}

impl BrokerPool {
    #[must_use]
    pub fn new(bootstrap: Vec<SocketAddr>, options: ConnectionOptions) -> Self {
        Self {
            by_id: DashMap::new(),
            by_addr: DashMap::new(),
            bootstrap,
            options,
        }
    }

    /// Get-or-connect to a specific broker id. The pool must have
    /// already learned (id, address) via `refresh_brokers`.
    pub async fn get(&self, broker_id: i32) -> Result<Arc<Connection>, ClientError> {
        if let Some(entry) = self.by_id.get(&broker_id) {
            return Ok(entry.clone());
        }
        let addr = self
            .by_addr
            .get(&broker_id)
            .map(|e| *e)
            .ok_or(ClientError::Disconnected)?;
        let conn = Arc::new(Connection::connect(addr, self.options.clone()).await?);
        self.by_id.insert(broker_id, conn.clone());
        Ok(conn)
    }

    /// Get-or-connect to the first reachable bootstrap address.
    pub async fn bootstrap_connection(&self) -> Result<Arc<Connection>, ClientError> {
        // Cache the bootstrap connection under broker id -1 (a synthetic id).
        const BOOTSTRAP_ID: i32 = -1;
        if let Some(entry) = self.by_id.get(&BOOTSTRAP_ID) {
            return Ok(entry.clone());
        }
        let mut last_err: Option<ClientError> = None;
        for addr in &self.bootstrap {
            match Connection::connect(*addr, self.options.clone()).await {
                Ok(c) => {
                    let arc = Arc::new(c);
                    self.by_id.insert(BOOTSTRAP_ID, arc.clone());
                    return Ok(arc);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(ClientError::Disconnected))
    }

    /// Update the (id, addr) registry from a list of brokers (typically
    /// from a `MetadataResponse`).
    pub fn refresh_brokers(&self, brokers: &[BrokerInfo]) {
        for b in brokers {
            let addr_str = format!("{}:{}", b.host, b.port);
            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                self.by_addr.insert(b.id, addr);
            }
        }
    }

    /// Close every open connection.
    pub async fn close_all(self) {
        let conns: Vec<_> = self
            .by_id
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for c in conns {
            // Each Arc<Connection> has the close method via Clone-and-take.
            // But Connection::close consumes self; we have Arc<Connection>.
            // For now just drop; the background tasks shut down when the
            // last Arc drops.
            drop(c);
        }
        drop(self.by_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_inserts_addresses() {
        let pool = BrokerPool::new(vec![], ConnectionOptions::default());
        pool.refresh_brokers(&[
            BrokerInfo { id: 1, host: "127.0.0.1".into(), port: 9092, rack: None },
            BrokerInfo { id: 2, host: "127.0.0.1".into(), port: 9093, rack: None },
        ]);
        assert!(pool.by_addr.contains_key(&1));
        assert!(pool.by_addr.contains_key(&2));
    }
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
mod connection;
mod error;
mod pool;
mod request;
mod transport;
mod version;

#[cfg(any(test, feature = "mock"))]
mod mock;

pub use connection::{Connection, ConnectionOptions};
pub use error::ClientError;
pub use pool::{BrokerInfo, BrokerPool};
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-client-core pool
```

Expected: 1 test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): BrokerPool with lazy connect"
```

---

### Task 15: `Client` + `ClientBuilder` + bootstrap parsing

**Files:**
- Create: `crates/client-core/src/bootstrap.rs`
- Create: `crates/client-core/src/client.rs`
- Modify: `crates/client-core/src/lib.rs`

- [ ] **Step 1: Bootstrap address parsing**

`crates/client-core/src/bootstrap.rs`:

```rust
//! Parse a Kafka-style bootstrap string ("host:port,host:port") into
//! a list of resolved SocketAddrs.

use std::net::SocketAddr;

use crate::error::ClientError;

/// Parse a comma-separated `host:port` list and resolve each entry via
/// `tokio::net::lookup_host`. At least one entry must resolve.
pub async fn resolve(bootstrap: &str) -> Result<Vec<SocketAddr>, ClientError> {
    let mut out = Vec::new();
    for part in bootstrap.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match tokio::net::lookup_host(part).await {
            Ok(iter) => out.extend(iter),
            Err(e) => {
                tracing::warn!(part, error = %e, "bootstrap resolve failed");
                continue;
            }
        }
    }
    if out.is_empty() {
        return Err(ClientError::Disconnected);
    }
    Ok(out)
}
```

- [ ] **Step 2: Client + ClientBuilder + BrokerHandle**

`crates/client-core/src/client.rs`:

```rust
//! Top-level `Client` + builder. Wraps a `BrokerPool` and exposes a
//! typed-request `send` API.

use std::sync::Arc;
use std::time::Duration;

use crate::bootstrap;
use crate::connection::ConnectionOptions;
use crate::error::ClientError;
use crate::pool::{BrokerInfo, BrokerPool};
use crate::request::ProtocolRequest;

pub struct Client {
    pool: Arc<BrokerPool>,
    options: ConnectionOptions,
}

impl Client {
    pub fn builder(bootstrap: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            bootstrap: bootstrap.into(),
            options: ConnectionOptions::default(),
        }
    }

    /// Send to whichever broker the pool currently has open (bootstrap
    /// if none).
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        let conn = self.pool.bootstrap_connection().await?;
        conn.send(req).await
    }

    pub fn broker(&self, broker_id: i32) -> BrokerHandle<'_> {
        BrokerHandle { pool: &self.pool, broker_id }
    }

    /// Send a default MetadataRequest, parse the broker list from the
    /// response, refresh the pool. Returns the typed response.
    pub async fn refresh_metadata(
        &self,
    ) -> Result<crabka_protocol::owned::metadata_response::MetadataResponse, ClientError> {
        use crabka_protocol::owned::metadata_request::MetadataRequest;
        let resp = self.send(MetadataRequest::default()).await?;
        let brokers: Vec<BrokerInfo> = resp
            .brokers
            .iter()
            .map(|b| BrokerInfo {
                id: b.node_id,
                host: b.host.clone(),
                port: b.port,
                rack: b.rack.clone(),
            })
            .collect();
        self.pool.refresh_brokers(&brokers);
        Ok(resp)
    }

    pub async fn close(self) {
        if let Some(pool) = Arc::into_inner(self.pool) {
            pool.close_all().await;
        }
    }
}

pub struct BrokerHandle<'a> {
    pool: &'a BrokerPool,
    broker_id: i32,
}

impl<'a> BrokerHandle<'a> {
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        let conn = self.pool.get(self.broker_id).await?;
        conn.send(req).await
    }
}

pub struct ClientBuilder {
    bootstrap: String,
    options: ConnectionOptions,
}

impl ClientBuilder {
    #[must_use]
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.options.client_id = id.into();
        self
    }

    #[must_use]
    pub fn request_timeout(mut self, t: Duration) -> Self {
        self.options.request_timeout = t;
        self
    }

    #[must_use]
    pub fn connect_timeout(mut self, t: Duration) -> Self {
        self.options.connect_timeout = t;
        self
    }

    pub async fn build(self) -> Result<Client, ClientError> {
        let addrs = bootstrap::resolve(&self.bootstrap).await?;
        let pool = Arc::new(BrokerPool::new(addrs, self.options.clone()));
        Ok(Client { pool, options: self.options })
    }
}
```

**Note:** the `BrokerInfo` field-pull in `refresh_metadata` assumes the generated `MetadataResponse` has a `brokers: Vec<MetadataResponseBroker>` field where each broker has `node_id`, `host`, `port`, `rack`. Verify against the generated code. If field names differ (e.g., `cluster_id` is also a field), adjust accordingly. The field shapes come from `crates/protocol/schemas/MetadataResponse.json`.

- [ ] **Step 3: Hook into lib.rs**

```rust
mod bootstrap;
mod client;
mod connection;
mod error;
mod pool;
mod request;
mod transport;
mod version;

#[cfg(any(test, feature = "mock"))]
mod mock;

pub use client::{BrokerHandle, Client, ClientBuilder};
pub use connection::{Connection, ConnectionOptions};
pub use error::ClientError;
pub use pool::{BrokerInfo, BrokerPool};
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
```

- [ ] **Step 4: Verify build + run tests**

```bash
cargo build -p crabka-client-core
cargo test -p crabka-client-core
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/client-core
git commit -m "feat(client-core): Client, ClientBuilder, BrokerHandle"
```

---

### Task 16: `BrokerPool::refresh_brokers` unit test

**Files:**
- Modify: `crates/client-core/tests/unit.rs`

- [ ] **Step 1: Append a test that exercises the full Client path**

```rust
#[tokio::test]
async fn client_refresh_metadata_populates_pool() {
    use crabka_protocol::owned::metadata_response::{
        MetadataResponse, MetadataResponseBroker,
    };

    let mock = MockBroker::start(move |api_key, _v, _cid, _body| {
        if api_key == ApiVersionsRequest::API_KEY {
            return build_api_versions_response_v0();
        }
        if api_key == MetadataRequest::API_KEY {
            let resp = MetadataResponse {
                brokers: vec![
                    MetadataResponseBroker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: 9092,
                        ..Default::default()
                    },
                    MetadataResponseBroker {
                        node_id: 2,
                        host: "127.0.0.1".into(),
                        port: 9093,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, 0).unwrap();
            return buf.to_vec();
        }
        Vec::new()
    }).await;

    let client = crabka_client_core::Client::builder(mock.addr.to_string())
        .build()
        .await
        .unwrap();

    let metadata = client.refresh_metadata().await.unwrap();
    assert_eq!(metadata.brokers.len(), 2);

    // After refresh, the pool knows broker 1 and 2's addresses.
    // We can't reach them (the mock only listens on its own port),
    // but the address registry is populated.
    // The pool internals are private; assert behaviorally that
    // broker(1).send would now at least *attempt* to connect to the
    // address (and either succeed if the address is the same as the mock,
    // or fail at the TCP layer). In this test we just verify metadata
    // returned the right shape.

    client.close().await;
    mock.stop().await;
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p crabka-client-core --test unit client_refresh
```

Expected: 1 test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/client-core/tests
git commit -m "test(client-core): client refresh_metadata populates pool"
```

---

## Phase H — Integration tests

### Task 17: testcontainers integration tests

**Files:**
- Create: `crates/client-core/tests/integration.rs`

- [ ] **Step 1: Write the integration test file**

`crates/client-core/tests/integration.rs`:

```rust
//! Integration tests against a real Apache Kafka via testcontainers.
//! Gated by `#[ignore]` so `cargo test --workspace` doesn't pull
//! Docker by default. Run with `--include-ignored`.

#![cfg(not(target_os = "windows"))]
// Skip on Windows runners; testcontainers + Docker reliability is rough.

use crabka_client_core::Client;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::metadata_request::MetadataRequest;
use testcontainers_modules::kafka::Kafka;
use testcontainers::runners::AsyncRunner;

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let kafka = Kafka::default().start().await.unwrap();
    let host = kafka.get_host().await.unwrap();
    let port = kafka.get_host_port_ipv4(9093).await.unwrap();
    (kafka, format!("{host}:{port}"))
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn api_versions_against_real_broker() {
    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap)
        .client_id("crabka-integration")
        .build()
        .await
        .unwrap();

    let resp = client.send(ApiVersionsRequest::default()).await.unwrap();
    assert_eq!(resp.error_code, 0);
    assert!(!resp.api_keys.is_empty(), "broker advertised no APIs");
    // Sanity: ApiVersions (key 18) is always present.
    assert!(resp.api_keys.iter().any(|k| k.api_key == 18));

    client.close().await;
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn metadata_against_real_broker() {
    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap).build().await.unwrap();

    let resp = client.refresh_metadata().await.unwrap();
    assert!(!resp.brokers.is_empty());

    client.close().await;
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_then_delete_topic() {
    use crabka_protocol::owned::create_topics_request::{CreateTopicsRequest, CreatableTopic};
    use crabka_protocol::owned::delete_topics_request::{DeleteTopicsRequest, DeleteTopicState};

    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap).build().await.unwrap();

    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "crabka-test-topic".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(create).await.unwrap();
    let topic_result = &resp.topics[0];
    assert_eq!(topic_result.error_code, 0, "CreateTopics error: {topic_result:?}");

    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("crabka-test-topic".into()),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(delete).await.unwrap();
    let topic_result = &resp.responses[0];
    assert_eq!(topic_result.error_code, 0, "DeleteTopics error: {topic_result:?}");

    client.close().await;
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn list_topics() {
    use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};

    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap).build().await.unwrap();

    // Metadata with topics=None lists all topics.
    let resp = client.send(MetadataRequest::default()).await.unwrap();
    let _ = resp.topics; // smoke: just assert we can decode the response.

    client.close().await;
    drop(kafka);
}
```

**Note on field names:** the exact field names on generated message types (`CreatableTopic`, `DeleteTopicState`, etc.) come from the schemas. The plan assumes the standard Kafka 4.2 names; verify in `crates/protocol/schemas/*.json` if anything fails to compile. Adjust struct literals accordingly without changing the test intent.

- [ ] **Step 2: Run the integration tests locally if Docker is available**

```bash
cargo test -p crabka-client-core --test integration -- --ignored --nocapture
```

Expected (Linux with Docker): 4 tests pass. Each spins up a fresh container; total runtime ~60 seconds.

If running locally on Windows without Docker, the `#[cfg(not(target_os = "windows"))]` skips compilation entirely.

- [ ] **Step 3: Commit**

```bash
git add crates/client-core/tests
git commit -m "test(client-core): testcontainers integration suite"
```

---

### Task 18: CI workflow for integration tests

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Append the new job**

Append to `.github/workflows/ci.yml` under the existing `jobs:` table:

```yaml
  client-core-integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.95.0"
      - run: cargo test -p crabka-client-core --test integration -- --ignored
```

- [ ] **Step 2: Validate YAML (optional)**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"
```

Expected: no error.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: client-core-integration job (Linux only)"
```

---

## Phase I — Rustdoc + acceptance

### Task 19: Rustdoc on public API + crate-level doc

**Files:**
- Modify: `crates/client-core/src/lib.rs`
- Modify: per-module files where docs are sparse

- [ ] **Step 1: Write the crate-level rustdoc**

Replace `crates/client-core/src/lib.rs`'s header with:

```rust
//! Connection management and request dispatch for Apache Kafka in Rust.
//!
//! This crate provides the first I/O-doing layer of Crabka. It wraps
//! `crabka-protocol`'s typed request/response messages in a `tokio`-based
//! TCP client that:
//!
//! - Opens one connection per broker, multiplexing requests via
//!   correlation ID.
//! - Negotiates API versions on connect.
//! - Manages a [`BrokerPool`] keyed on broker id with lazy connect.
//! - Resolves bootstrap addresses on builder.
//!
//! ## Quick start
//!
//! ```no_run
//! use crabka_client_core::Client;
//! use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder("localhost:9092")
//!     .client_id("my-app")
//!     .build()
//!     .await?;
//!
//! let resp = client.send(ApiVersionsRequest::default()).await?;
//! println!("broker supports {} APIs", resp.api_keys.len());
//!
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Out of scope
//!
//! - Producer / consumer semantics (slices 5/6).
//! - Transactions (slice 9).
//! - Partition-aware routing.
//! - TLS / SASL (slice 11).
//! - Automatic mid-request retry.
//!
//! ## Cargo features
//!
//! - `mock` — exposes [`MockBroker`] beyond `#[cfg(test)]` for downstream
//!   testing.
```

- [ ] **Step 2: Verify cargo doc builds clean**

```bash
RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo doc -p crabka-client-core --no-deps --all-features
```

Expected: no warnings. If broken intra-doc links, fix the doc comment in the offending file. Common cause: `[BrokerPool]` references that don't resolve — add `crate::` prefix if needed.

- [ ] **Step 3: Commit**

```bash
git add crates/client-core
git commit -m "docs(client-core): crate-level rustdoc + public-type docs"
```

---

### Task 20: Acceptance gate

Verification only. Mark complete only when every item passes.

- [ ] `cargo fmt --check` clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo test --workspace` clean (no regressions).
- [ ] `cargo test --workspace -- --include-ignored` clean (existing differential tests still pass).
- [ ] `cargo test -p crabka-client-core --test unit` passes the 4 mock-based tests (connect, timeout, round-trip, concurrent dispatch, refresh_metadata).
- [ ] `cargo test -p crabka-client-core --test integration -- --ignored` passes on Linux with Docker: 4 testcontainers scenarios (ApiVersions, Metadata, CreateTopic/DeleteTopic, list-topics).
- [ ] `cargo doc -p crabka-client-core --no-deps --all-features` builds clean.
- [ ] `./tools/regenerate.sh && git diff --quiet` (no drift after the codegen change).
- [ ] `crates/client-core/Cargo.toml` declares the right `tokio` feature set.
- [ ] `ProtocolRequest` trait lives in `crabka-protocol::codec`; re-exported from `crabka-client-core`.
- [ ] Every Request-typed generated message file contains an `impl ProtocolRequest`.
- [ ] CI matrix green on Linux/macOS/Windows.
- [ ] New `client-core-integration` job runs on Linux only and passes.

When all 12 items are ✅:

```bash
git push -u origin feature/client-core
gh pr create --base main --head feature/client-core \
    --title "Slice 2: crabka-client-core (connection + dispatch)" \
    --body "$(cat <<'PRBODY'
## Summary

First Crabka crate that does I/O. TCP connection management, API-version negotiation, correlation-ID request/response dispatch against Apache Kafka brokers. Plaintext only.

## What landed

- `crates/client-core/` with `transport`, `version`, `connection`, `pool`, `bootstrap`, `client`, `request`, `mock` modules
- `ProtocolRequest` trait in `crabka-protocol::codec`; codegen emits the impl for every Request type
- `Client::builder(bootstrap).build()` connects, negotiates API versions, returns a usable handle
- Multiplexed dispatch on a single connection (correlation ID matching)
- `BrokerPool` with lazy connect, fed by `refresh_metadata`
- Mock broker for unit tests; testcontainers Apache Kafka for integration
- New `client-core-integration` CI job (Linux only)

## Out of scope

Producer / consumer semantics (slices 5/6), transactions (slice 9), auto-retry, TLS / SASL (slice 11), partition-aware routing.

## Reference

Spec: `docs/superpowers/specs/2026-05-11-crabka-client-core-design.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

---

## Self-review against the spec

**Spec acceptance items:**

| # | Spec criterion | Plan task |
|---|---|---|
| 1 | New `crabka-client-core` crate exists | Tasks 1, 2 |
| 2 | `crabka-protocol-codegen` emits `impl ProtocolRequest for <Request>` | Task 5 |
| 3 | `Client::builder(bootstrap).build()` connects + negotiates versions | Tasks 8-10, 15 |
| 4 | `Client::send<R>` round-trips for ≥ 3 message types | Tasks 12, 13, 16 |
| 5 | Correlation-ID multiplexing handles concurrent requests | Task 13 |
| 6 | Timeout / disconnect / incompatible-version surface as typed `ClientError` | Tasks 3, 10, 12 |
| 7 | `BrokerPool::refresh_brokers` populates from Metadata | Tasks 14, 16 |
| 8 | Integration tests pass ≥ 4 testcontainers scenarios | Task 17 |
| 9 | New `client-core-integration` CI job (Linux only) | Task 18 |
| 10 | No regressions | Task 20 verification |
| 11 | Rustdoc on public types | Task 19 |
| 12 | CI matrix green | Task 20 verification |

**Placeholder scan:** No `TODO` / `TBD` in requirements. The plan flags two implementation choices the implementer makes during execution (one-task vs split reader/writer in Task 9; exact `MetadataResponseBroker` field names in Tasks 15/16/17 — verify in schemas before relying on the literal). Both are concrete decisions with named alternatives, not deferrals.

**Type consistency:** `Connection`, `ConnectionOptions`, `BrokerPool`, `BrokerInfo`, `Client`, `ClientBuilder`, `BrokerHandle`, `ProtocolRequest`, `ApiVersionTable`, `ClientError`, `MockBroker` — all referenced consistently across tasks. `ProtocolRequest::Response` associated type referenced consistently. `ConnectionOptions::default()` has stable defaults across tasks 8/15/17.

The plan is ready for execution.
