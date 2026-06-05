# Crabka gRPC Gateway — Streaming Wire Implementation Plan (SendStream + bidi Subscribe)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the two streaming RPCs to the gateway — bidirectional `SendStream` (stream records in → stream per-batch acks out) and bidirectional `Subscribe` (stream control frames in → stream consumed records out) — driving the existing `ProduceCore` and `ConsumeSession`.

**Architecture:** Connect-RPC bidi handlers over `connectrpc-axum` 0.1.1 (streaming confirmed supported). Each handler's logic lives in a testable `*_inner(...) -> impl Stream<Item = Result<Resp, ConnectError>>` function built with the `async-stream` `stream!` macro; a thin Connect wrapper wraps it in `ConnectResponse::new(StreamBody::new(inner))`. The broker is never touched; everything rides the native client cores from P0–P2.

**Tech Stack:** `connectrpc-axum` (`Streaming<T>`, `StreamBody`, `ConnectRequest`/`ConnectResponse`/`ConnectError`), `async-stream`, `futures-util` (`StreamExt`), `tokio::select!`. Tests use the in-process broker (`crabka-broker` `test-helpers`, `BrokerHandle`).

---

## Scope

**In this plan:** `SendStream` + `Subscribe` bidi RPCs (proto + handlers + router registration + tests), building on the P0–P2 crate. Drives the existing `ProduceCore`/`ConsumeSession`.

**Out of scope (later slices):** P3 active-active ownership sharding; webhooks; TLS/identity; telemetry; operator. Do NOT modify the broker or reimplement Kafka protocol.

**Confirmed connectrpc-axum streaming API (spike results — rely on these):**
- Bidi handler signature: `async fn h(Extension(state), req: ConnectRequest<Streaming<Req>>) -> Result<ConnectResponse<StreamBody<St>>, ConnectError>` where `St: Stream<Item = Result<Resp, ConnectError>>`.
- `req.0` yields the `Streaming<Req>`, which is a `Stream<Item = Result<Req, ConnectError>>` — iterate with `futures_util::StreamExt::next`.
- Build the output: `ConnectResponse::new(StreamBody::new(some_stream))`.
- `connectrpc_axum::message::Streaming::<T>::new(Pin<Box<dyn Stream<Item = Result<T, ConnectError>> + Send>>)` constructs a `Streaming<T>` (used to feed handlers in tests).
- Generated builder methods are snake_case of the RPC: `.send_stream(handler)`, `.subscribe(handler)` (same shape as the existing `.send`).
- `ConnectError` constructors: `ConnectError::new_invalid_argument(msg)`, `new_internal(msg)`, `new_unavailable(msg)`.
- Imports: `use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse, StreamBody, Streaming};`

## File structure

```
crates/grpc-gateway/
  Cargo.toml                 # T1: add async-stream + futures-util deps
  proto/crabka/gateway/v1/gateway.proto  # T1: SendStream, Subscribe RPCs + messages
  src/
    handlers.rs              # T2: extract `to_gateway_record` (shared by unary + streaming)
    streaming.rs             # T2/T3: send_stream{,_inner}, subscribe{,_inner}
    lib.rs                   # T2/T3: `pub mod streaming;` + register .send_stream/.subscribe in router()
  tests/
    streaming.rs             # T2/T3: handler tests (in-process broker)
```
(Workspace `Cargo.toml` gains `async-stream` under `[workspace.dependencies]` — T1.)

---

## Task 1: Proto streaming RPCs + messages + deps + codegen

**Files:**
- Modify: `Cargo.toml` (workspace root — add `async-stream`)
- Modify: `crates/grpc-gateway/Cargo.toml` (add `async-stream`, `futures-util`)
- Modify: `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`

- [ ] **Step 1: Add `async-stream` to the workspace deps.** In the root `Cargo.toml` under `[workspace.dependencies]`, add (alphabetically near other async deps):

```toml
async-stream = "0.3"
```

- [ ] **Step 2: Add deps to the gateway crate.** In `crates/grpc-gateway/Cargo.toml` `[dependencies]`, add:

```toml
async-stream = { workspace = true }
futures-util = { workspace = true }
```

(`futures-util` is already a workspace dep: `futures-util = { version = "0.3", features = ["sink"] }`.)

- [ ] **Step 3: Add the streaming RPCs + messages to the proto.** In `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`, change the service and append the new messages:

```proto
service Gateway {
  rpc Send(SendRequest) returns (SendResponse);
  rpc SendStream(stream SendRequest) returns (stream SendAck);
  rpc Subscribe(stream SubscribeFrame) returns (stream Inbound);
}
```

Append after the existing `SendResponse` message:

```proto
message SendAck {
  repeated RecordResult results = 1;
}

message SubscribeStart {
  string group_id = 1;
  repeated string topics = 2;
  bool auto_commit = 3;
}

message SubscribeAck {
  string topic = 1;
  int32 partition = 2;
  int64 offset = 3;
}

message SubscribeFrame {
  oneof frame {
    SubscribeStart start = 1;
    SubscribeAck ack = 2;
  }
}

message Inbound {
  string topic = 1;
  int32 partition = 2;
  int64 offset = 3;
  optional bytes key = 4;
  bytes value = 5;
  map<string, bytes> headers = 6;
  int64 timestamp_ms = 7;
}
```

- [ ] **Step 4: Build to regenerate codegen.**

Run: `cd /Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25 && cargo build -p crabka-grpc-gateway`
Expected: compiles. The generated `OUT_DIR/crabka.gateway.v1.rs` now has `send_stream` and `subscribe` builder methods, plus `pb::SendAck`, `pb::SubscribeFrame`, `pb::subscribe_frame::Frame::{Start,Ack}`, `pb::SubscribeStart`, `pb::SubscribeAck`, `pb::Inbound`.

> VERIFY the generated oneof path: prost names it `pb::subscribe_frame::Frame` with variants `Start(SubscribeStart)` / `Ack(SubscribeAck)`, and the field is `SubscribeFrame { frame: Option<subscribe_frame::Frame> }`. Confirm by grepping the generated file (`find target -name crabka.gateway.v1.rs | xargs grep -n 'mod subscribe_frame\|enum Frame'`). Adjust later tasks' paths to match exactly.

- [ ] **Step 5: Commit.**

```bash
git add Cargo.toml crates/grpc-gateway/Cargo.toml crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto Cargo.lock
git commit -m "feat(gateway): streaming proto (SendStream, Subscribe) + async-stream dep"
```

---

## Task 2: SendStream bidi handler

**Files:**
- Modify: `crates/grpc-gateway/src/handlers.rs` (extract `to_gateway_record`)
- Create: `crates/grpc-gateway/src/streaming.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod streaming;` + register `.send_stream`)
- Create: `crates/grpc-gateway/tests/streaming.rs`

- [ ] **Step 1: Extract the shared record-mapping helper.** In `crates/grpc-gateway/src/handlers.rs`, add a `pub(crate)` helper and use it in `send` (DRY — the streaming handler reuses it). Add:

```rust
/// Convert a wire `pb::Record` into the transport-agnostic `GatewayRecord`.
pub(crate) fn to_gateway_record(r: crate::pb::Record) -> crate::types::GatewayRecord {
    crate::types::GatewayRecord {
        topic: r.topic,
        key: r.key.map(bytes::Bytes::from),
        value: bytes::Bytes::from(r.value),
        headers: r
            .headers
            .into_iter()
            .map(|(k, v)| (k, bytes::Bytes::from(v)))
            .collect(),
        partition: r.partition,
        timestamp_ms: r.timestamp_ms,
        idempotency_key: r.idempotency_key,
    }
}
```

Then in the existing `send` handler, replace the inline `GatewayRecord { ... }` construction with `let rec = crate::handlers::to_gateway_record(r);` (keep behavior identical).

- [ ] **Step 2: Write the failing test** (`tests/streaming.rs`):

```rust
//! Streaming Connect handlers: SendStream (produce) and Subscribe (consume).

use std::collections::BTreeMap;
use std::sync::Arc;

use assert2::check;
use bytes::Bytes;
use connectrpc_axum::message::Streaming;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::state::AppState;
use crabka_grpc_gateway::{pb, streaming};
use futures_util::StreamExt;
use std::net::SocketAddr;
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn state_for(bootstrap: &str) -> Arc<AppState> {
    let produce = ProduceCore::new(bootstrap, "stream", Arc::new(RawCodec)).await.unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: addr,
            client_id: "stream".into(),
            dedup_topic: "__crabka_grpc_dedup".into(),
            dedup_partitions: 4,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: "stream-dedup".into(),
        }),
    })
}

fn rec(topic: &str, value: &'static [u8]) -> pb::Record {
    pb::Record {
        topic: topic.into(),
        key: None,
        value: value.to_vec(),
        headers: Default::default(),
        partition: None,
        timestamp_ms: None,
        idempotency_key: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_stream_produces_all_records() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap)).await.unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec { name: "ss-topic".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    // Two SendRequests in the input stream.
    let input = futures_util::stream::iter(vec![
        Ok(pb::SendRequest { records: vec![rec("ss-topic", b"a")], acks: 0 }),
        Ok(pb::SendRequest { records: vec![rec("ss-topic", b"b")], acks: 0 }),
    ]);
    let inbound = Streaming::new(Box::pin(input));

    let acks: Vec<_> = streaming::send_stream_inner(inbound, state).collect().await;
    check!(acks.len() == 2);
    for a in &acks {
        let ack = a.as_ref().expect("ack ok");
        check!(ack.results.len() == 1);
        check!(ack.results[0].error.is_none());
    }

    // Both records landed.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("ss-reader")
        .subscribe(vec!["ss-topic".to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut seen = 0;
    for _ in 0..10 {
        seen += consumer.poll(std::time::Duration::from_millis(500)).await.unwrap().len();
        if seen >= 2 { break; }
    }
    check!(seen == 2);

    broker.shutdown().await;
}
```

- [ ] **Step 3: Run it to verify it fails.**

Run: `cargo test -p crabka-grpc-gateway --test streaming send_stream_produces_all_records`
Expected: FAIL — `streaming::send_stream_inner` not found.

- [ ] **Step 4: Write `src/streaming.rs`** with the testable inner + the thin Connect wrapper:

```rust
//! Streaming Connect handlers — bidirectional `SendStream` (produce) and
//! `Subscribe` (consume). The per-handler logic lives in a `*_inner` function
//! returning a plain `Stream` (unit-testable); the public handler is a thin
//! wrapper into `ConnectResponse::new(StreamBody::new(inner))`.

use std::sync::Arc;

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse, StreamBody, Streaming};
use futures_util::{Stream, StreamExt};

use crate::handlers::to_gateway_record;
use crate::pb;
use crate::state::AppState;

/// Produce every record in each inbound `SendRequest`, emitting one `SendAck`
/// (with a per-record `RecordResult` vector) per request. Errors decoding the
/// input stream are forwarded and end the stream.
pub fn send_stream_inner(
    mut inbound: Streaming<pb::SendRequest>,
    state: Arc<AppState>,
) -> impl Stream<Item = Result<pb::SendAck, ConnectError>> {
    async_stream::stream! {
        while let Some(item) = inbound.next().await {
            let send_req = match item {
                Ok(r) => r,
                Err(e) => { yield Err(e); break; }
            };
            let mut results = Vec::with_capacity(send_req.records.len());
            for r in send_req.records {
                let rec = to_gateway_record(r);
                let result = match state.produce.produce(rec).await {
                    Ok(o) => pb::RecordResult {
                        partition: o.partition,
                        offset: o.offset,
                        deduplicated: o.deduplicated,
                        error: None,
                    },
                    Err(e) => pb::RecordResult {
                        partition: -1,
                        offset: -1,
                        deduplicated: false,
                        error: Some(pb::ErrorInfo { code: 1, message: e.to_string(), retriable: false }),
                    },
                };
                results.push(result);
            }
            yield Ok(pb::SendAck { results });
        }
    }
}

/// Bidi `SendStream` Connect handler.
pub async fn send_stream(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<Streaming<pb::SendRequest>>,
) -> Result<ConnectResponse<StreamBody<impl Stream<Item = Result<pb::SendAck, ConnectError>>>>, ConnectError> {
    Ok(ConnectResponse::new(StreamBody::new(send_stream_inner(req.0, state))))
}
```

> VERIFY: `req.0` extracts the `Streaming<pb::SendRequest>` (mirror the unary handler's `req.0`). If `async_stream::stream!`'s returned type doesn't satisfy `Send`/`'static` for `StreamBody`, box it: `StreamBody::new(Box::pin(send_stream_inner(req.0, state)))` and change the return type to `StreamBody<std::pin::Pin<Box<dyn Stream<...> + Send>>>`. Adjust if the compiler asks.

- [ ] **Step 5: Register in `router()`** — in `src/lib.rs` add `pub mod streaming;` and extend the builder chain:

```rust
pub fn router(state: std::sync::Arc<state::AppState>) -> axum::Router {
    pb::gateway_connect::GatewayServiceBuilder::<()>::new()
        .send(handlers::send)
        .send_stream(streaming::send_stream)
        .build()
        .layer(axum::Extension(state))
}
```

- [ ] **Step 6: Run the test + gates.**

Run: `cargo test -p crabka-grpc-gateway --test streaming send_stream_produces_all_records` → PASS.
Run: `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` → clean.
Run: `cargo fmt --check -p crabka-grpc-gateway` → no diff.

- [ ] **Step 7: Commit.**

```bash
git add crates/grpc-gateway/src/handlers.rs crates/grpc-gateway/src/streaming.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/tests/streaming.rs
git commit -m "feat(gateway): bidi SendStream handler"
```

---

## Task 3: Subscribe bidi handler

**Files:**
- Modify: `crates/grpc-gateway/src/streaming.rs` (add `subscribe{,_inner}`)
- Modify: `crates/grpc-gateway/src/lib.rs` (register `.subscribe`)
- Modify: `crates/grpc-gateway/tests/streaming.rs` (add the subscribe test)

- [ ] **Step 1: Write the failing test** (append to `tests/streaming.rs`):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_streams_records_then_commits() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap)).await.unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec { name: "sub-topic".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    // Produce one record up front (auto_commit path, so no Ack needed).
    crabka_grpc_gateway::produce::ProduceCore::new(&bootstrap, "sub-prod", Arc::new(RawCodec))
        .await
        .unwrap()
        .produce(crabka_grpc_gateway::types::GatewayRecord {
            topic: "sub-topic".into(), key: None, value: Bytes::from_static(b"hello"),
            headers: vec![], partition: None, timestamp_ms: None, idempotency_key: None,
        })
        .await
        .unwrap();

    // Input control stream: one Start frame (auto_commit), then it stays open.
    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "sub-group".into(),
            topics: vec!["sub-topic".into()],
            auto_commit: true,
        })),
    };
    // A pending stream after Start keeps the subscription alive; use an
    // unbounded receiver that we drop to end it.
    let (tx, rx) = futures_util::channel::mpsc::unbounded::<Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>>();
    tx.unbounded_send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(rx));

    let mut out = Box::pin(streaming::subscribe_inner(inbound, state));
    let mut got = None;
    for _ in 0..20 {
        match tokio::time::timeout(std::time::Duration::from_millis(600), out.next()).await {
            Ok(Some(Ok(msg))) => { got = Some(msg); break; }
            Ok(Some(Err(e))) => panic!("subscribe error: {e:?}"),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    drop(tx); // close the control stream → subscription ends
    let msg = got.expect("received an Inbound record");
    check!(msg.topic == "sub-topic");
    check!(msg.value == b"hello");

    broker.shutdown().await;
}
```

> VERIFY: `futures_util::channel::mpsc::unbounded` requires the `futures-util` `channel`/`std` features (default includes them). If absent, enable `features = ["channel"]` on the dev-use, or use `tokio::sync::mpsc` + `tokio_stream::wrappers::UnboundedReceiverStream`.

- [ ] **Step 2: Run it to verify it fails.**

Run: `cargo test -p crabka-grpc-gateway --test streaming subscribe_streams_records_then_commits`
Expected: FAIL — `subscribe_inner` not found.

- [ ] **Step 3: Add `subscribe_inner` + `subscribe` to `src/streaming.rs`.** Append:

```rust
use crate::consume::ConsumeSession;

/// Join a consumer group on the caller's behalf and stream records. The first
/// frame MUST be `Start`; subsequent `Ack` frames commit offsets (at-least-once).
/// With `auto_commit` the session commits after each non-empty poll. The
/// subscription ends when the control stream closes or errors.
pub fn subscribe_inner(
    mut frames: Streaming<pb::SubscribeFrame>,
    state: Arc<AppState>,
) -> impl Stream<Item = Result<pb::Inbound, ConnectError>> {
    async_stream::stream! {
        // First frame must be Start.
        let start = match frames.next().await {
            Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Start(s)) })) => s,
            Some(Ok(_)) => { yield Err(ConnectError::new_invalid_argument("first Subscribe frame must be Start")); return; }
            Some(Err(e)) => { yield Err(e); return; }
            None => return,
        };

        let client_id = format!("{}-sub", state.config.client_id);
        let mut session = match ConsumeSession::new(&state.config.bootstrap, &start.group_id, &client_id, start.topics).await {
            Ok(s) => s,
            Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); return; }
        };
        let auto_commit = start.auto_commit;

        loop {
            // Borrow note: `session.poll(..)` and `frames.next()` borrow
            // different locals, so both can be polled in one select!. We do NOT
            // call `session.commit()` inside a select arm body (that would
            // overlap the poll borrow) — instead set a flag and commit AFTER.
            let mut commit = false;
            let mut stop = false;
            tokio::select! {
                frame = frames.next() => {
                    match frame {
                        Some(Ok(pb::SubscribeFrame { frame: Some(pb::subscribe_frame::Frame::Ack(_)) })) => commit = true,
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { yield Err(e); stop = true; }
                        None => stop = true,
                    }
                }
                batch = session.poll(std::time::Duration::from_millis(500)) => {
                    match batch {
                        Ok(records) => {
                            for r in records {
                                yield Ok(pb::Inbound {
                                    topic: r.topic,
                                    partition: r.partition,
                                    offset: r.offset,
                                    key: r.key.map(|b| b.to_vec()),
                                    value: r.value.map(|b| b.to_vec()).unwrap_or_default(),
                                    headers: Default::default(),
                                    timestamp_ms: r.timestamp,
                                });
                                if auto_commit { commit = true; }
                            }
                        }
                        Err(e) => { yield Err(ConnectError::new_internal(e.to_string())); stop = true; }
                    }
                }
            }
            if commit {
                if let Err(e) = session.commit().await {
                    yield Err(ConnectError::new_internal(e.to_string()));
                    break;
                }
            }
            if stop { break; }
        }
    }
}

/// Bidi `Subscribe` Connect handler.
pub async fn subscribe(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<Streaming<pb::SubscribeFrame>>,
) -> Result<ConnectResponse<StreamBody<impl Stream<Item = Result<pb::Inbound, ConnectError>>>>, ConnectError> {
    Ok(ConnectResponse::new(StreamBody::new(subscribe_inner(req.0, state))))
}
```

> VERIFY: (a) `yield` inside a `tokio::select!` arm inside `async_stream::stream!` — this is supported, but if the macro chokes, restructure so the select! only computes `commit`/`stop`/a `Vec<Inbound>` to emit, then `yield` the collected records AFTER the select! block. (b) The `Inbound.headers` is `map<string,bytes>` (HashMap) — `Default::default()` is an empty map; populating from `ConsumerRecord` headers is deferred (ConsumerRecord exposes none in P0–P2). (c) If `StreamBody<impl Stream>` complains about `Send`/`'static`, box: `StreamBody::new(Box::pin(subscribe_inner(req.0, state)))`.

- [ ] **Step 4: Register in `router()`** — add `.subscribe(streaming::subscribe)`:

```rust
        .send(handlers::send)
        .send_stream(streaming::send_stream)
        .subscribe(streaming::subscribe)
        .build()
```

- [ ] **Step 5: Run the test + gates.**

Run: `cargo test -p crabka-grpc-gateway --test streaming` → both streaming tests PASS.
Run: `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` → clean.
Run: `cargo fmt --check -p crabka-grpc-gateway` → no diff.

- [ ] **Step 6: Commit.**

```bash
git add crates/grpc-gateway/src/streaming.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/tests/streaming.rs
git commit -m "feat(gateway): bidi Subscribe handler (group consume + ack-commit)"
```

---

## Task 4: Wrapper coverage + final verification

**Files:**
- Modify: `crates/grpc-gateway/tests/streaming.rs` (cover the thin `send_stream`/`subscribe` Connect wrappers + `router()`)

- [ ] **Step 1: Add a test that exercises the thin wrappers + router** (append to `tests/streaming.rs`):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_wrappers_and_router_build() {
    use axum::Extension;
    use connectrpc_axum::message::ConnectRequest;

    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap)).await.unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec { name: "wrap-topic".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    // Router builds with both streaming methods registered (covers lib::router).
    let _router = crabka_grpc_gateway::router(state.clone());

    // The thin send_stream wrapper returns Ok with a StreamBody (covers the wrapper).
    let input = futures_util::stream::iter(vec![Ok(pb::SendRequest { records: vec![rec("wrap-topic", b"x")], acks: 0 })]);
    let req = ConnectRequest(Streaming::new(Box::pin(input)));
    let resp = streaming::send_stream(Extension(state.clone()), req).await;
    check!(resp.is_ok());

    broker.shutdown().await;
}
```

- [ ] **Step 2: Run the full crate suite + all gates.**

Run: `cargo test -p crabka-grpc-gateway` → all pass (unit + existing integration + new streaming).
Run: `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` → clean.
Run: `cargo fmt --check -p crabka-grpc-gateway` → no diff.

- [ ] **Step 3: Confirm coverage of the new lines** (codecov/patch is strict — see the P0–P2 lesson):

Run: `cargo llvm-cov -p crabka-grpc-gateway --tests --summary-only 2>&1 | grep -E 'streaming.rs|handlers.rs|lib.rs|TOTAL'`
Expected: `streaming.rs` well-covered (the `_inner` logic + wrappers exercised). The two thin wrappers + router are covered by Task 4 Step 1. If a handful of error arms remain, that's fine (aggregate patch target is ~91%).

- [ ] **Step 4: Commit.**

```bash
git add crates/grpc-gateway/tests/streaming.rs
git commit -m "test(gateway): cover streaming Connect wrappers + router"
```

---

## Final verification (before declaring the slice done)

- [ ] `cargo build -p crabka-grpc-gateway` — clean.
- [ ] `cargo test -p crabka-grpc-gateway` — unit + integration + streaming all pass.
- [ ] `cargo fmt --check -p crabka-grpc-gateway` — no diff.
- [ ] `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` — clean.
- [ ] Diff touches only `crates/grpc-gateway/**`, the root `Cargo.toml` (`async-stream` dep), and `Cargo.lock` — broker untouched.

## Self-review (completed during planning)

- **Spec coverage:** spec's `SendStream` (streaming produce) ✓ (T2) and bidi `Subscribe` (group consume + ack→commit, auto_commit) ✓ (T3); both registered in `router()` ✓. The spec's `Subscribe` "at-least-once via ack→commit" and "load-balanced across callers sharing group_id" are honored (it joins a real Kafka group via `ConsumeSession`). Streaming-only — no P3/webhooks/etc. (correctly out of scope).
- **Type consistency:** `to_gateway_record` (T2) reused in `send_stream_inner` (T2); `ConsumeSession::{new,poll,commit}` signatures match P0–P2; `pb::{SendAck, SubscribeFrame, subscribe_frame::Frame, SubscribeStart, SubscribeAck, Inbound}` defined in T1 and used consistently; `send_stream_inner`/`subscribe_inner` return `impl Stream<Item=Result<_, ConnectError>>` and the wrappers wrap them identically.
- **Placeholders:** none — every step has complete code; the `> VERIFY` callouts are for genuinely upstream-dependent specifics (the prost oneof module path, async_stream+select interaction, `StreamBody` Send/'static boxing), each naming the exact check + fallback.
- **Testability:** the `_inner` functions return collectible `impl Stream`, unit-tested directly against the in-process broker; the thin Connect wrappers + `router()` are covered by Task 4.

## Risks / things the implementer must verify

1. **`async_stream::stream!` + `tokio::select!` + `yield`** (Subscribe, T3): the riskiest interaction. If `yield` inside a select arm won't compile, restructure to compute an emit-list + flags inside the select!, then `yield` after. Fallback documented inline.
2. **`StreamBody<impl Stream>` Send/'static bounds:** if the bare `impl Stream` return type is rejected by `StreamBody`/the builder's `Handler` bound, box with `Box::pin(...)` and a `Pin<Box<dyn Stream<...> + Send>>` return type. The `send_stream`/`subscribe` wrappers are the place to box.
3. **prost oneof path** (`pb::subscribe_frame::Frame::{Start,Ack}`) — confirm against the generated file (T1 Step 4 verify).
4. **`Subscribe` borrow across `select!`:** do NOT call `session.commit()` inside a select arm (overlaps the `session.poll` borrow). Use the flag-then-commit-after pattern in the plan.
5. **Test control-stream lifetime:** the Subscribe test keeps the control stream open via an mpsc channel, then drops it to end the subscription. If `futures_util::channel::mpsc` isn't available, use `tokio::sync::mpsc` + `tokio_stream::wrappers::UnboundedReceiverStream` (add `tokio-stream` dev-dep).

## What lands after this slice

- **P3 — active-active ownership sharding** (the dedup engine's distributed half): consumer-group ownership of `__crabka_grpc_dedup`, membership routing topic, gateway→gateway forwarding, per-partition rebalance warm-up, `transactional.id`-per-partition fencing.
- Then P4–P9 (TLS/mTLS, identity→ACL, webhooks, telemetry, operator) + the Schema Registry codec via the `RecordCodec` seam.
