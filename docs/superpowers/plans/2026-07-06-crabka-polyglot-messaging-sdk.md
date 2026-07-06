# Polyglot serverless messaging SDK (MSG-5) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a **Go** client library over the gateway's Connect-RPC surface — `publish`/`publishEvent` (unary) + `subscribe`-with-filter (`auto_commit`) — plus the polyglot foundation (buf codegen, `sdks/` layout) and a real gateway test harness, enabling the gateway's h2c listener so a connect-go client can open the bidi `Subscribe` stream.

**Architecture:** buf generates connect-go stubs from the canonical `gateway.proto` into `sdks/go/gen`; a thin `sdks/go` wrapper maps `publish`/`publishEvent`/`subscribe` onto them. The gateway plaintext listener gains h2c (`auto::Builder`) so bidi `Subscribe` works from connect-go. SDK tests run against a live gateway via a new OCI image + docker-compose.

**Tech Stack:** Go 1.2x + connect-go, buf, `protoc-gen-connect-go`/`protoc-gen-go`, Rust `hyper_util` (h2c), apko (OCI image), docker-compose, `cargo +nightly fmt`, `clippy::pedantic`, `go test`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-polyglot-messaging-sdk-design.md`](../specs/2026-07-06-crabka-polyglot-messaging-sdk-design.md).

**PREREQUISITES (for a *complete* CE round-trip, not for publish):** MSG-1 (SDK CE-*consume*), MSG-3 (manual per-offset ack). MSG-5 v1 ships without them (publish CE is transparent today; subscribe defaults to `auto_commit`).

---

## Invariants

1. **Connect codegen only** — connect-go stubs from buf; never a hand-rolled or gRPC/gRPC-Web client.
2. **`.proto` canonical** — rooted at `crates/grpc-gateway/proto`; never copied into `sdks/`; stubs drift-checked.
3. **Subscribe needs h2** — the gateway plaintext listener serves h1+h2c; a connect-go bidi `Subscribe` over h2c is proven end-to-end.
4. **Honest surface** — manual ack / share-consume / auto-provision / CE-consume are labeled experimental/unimplemented, not silently promised; the filter is EQUALS-only.
5. **Behavior-tested** — SDK tests exercise a live gateway, never read SDK source text.
6. **Out of the Cargo workspace** — `sdks/` and buf config don't touch `crates/*` or release-plz.
7. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** buf config + `sdks/go` + connect-go stubs; the gateway h2c swap; the Go `publish`/`publishEvent`/`subscribe`+filter wrapper; the gateway OCI image + compose harness; a Go CI job; the honest SDK README.
- **Deferred:** TS/Python SDKs (need `setup-node`/`setup-python`); manual per-offset ack (MSG-3); `SendStream`; `EnsureTopic`/share-group Subscribe RPCs; SDK CE-consume (MSG-1).

---

## File Structure & Batching

- **`buf.yaml`, `buf.gen.yaml`** (repo root, new) — Connect codegen (Task 1).
- **`sdks/go/`** (new: `go.mod`, `gen/`, `client.go`, `subscribe.go`, `README.md`) — the Go SDK (Tasks 1, 3, 5, 6).
- **`crates/grpc-gateway/src/serve.rs:30`** — h2c listener swap (Task 2).
- **`packaging/apko/crabka-gateway.yaml`** + **`sdks/go/testdata/docker-compose.yml`** (new) — harness (Task 4).
- **`.github/workflows/`** — buf drift + Go SDK job (Task 6).

**Batching:** Task 1 (buf + `sdks/`) ∥ Task 2 (`serve.rs`) — disjoint. Task 3 (Go publish, needs Task 1 stubs) ∥ Task 4 (harness) — disjoint. Task 5 (subscribe round-trip) needs 2+3+4. Task 6 last.

---

## Task 1 (Batch A): buf foundation + `sdks/go` scaffold + connect-go stubs

**Files:**
- Create: `buf.yaml`, `buf.gen.yaml`, `sdks/go/go.mod`, `sdks/go/gen/**` (generated)

- [ ] **Step 1: buf config**

`buf.yaml` (module rooted at the gateway proto):

```yaml
version: v2
modules:
  - path: crates/grpc-gateway/proto
lint: { use: [STANDARD] }
breaking: { use: [FILE] }
```

`buf.gen.yaml` (connect-go + go):

```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: sdks/go/gen
    opt: [paths=source_relative]
  - remote: buf.build/connectrpc/go
    out: sdks/go/gen
    opt: [paths=source_relative]
```

- [ ] **Step 2: Generate + module**

`sdks/go/go.mod` (module e.g. `github.com/robot-head/crabka/sdks/go`, require `connectrpc.com/connect`). Run `buf generate`; commit the generated `sdks/go/gen/crabka/gateway/v1/*.go` (+ `...connect/*.go`).

- [ ] **Step 3: Verify + commit**

Run: `buf lint` (clean) and `cd sdks/go && go build ./...` (stubs compile).

```bash
git add buf.yaml buf.gen.yaml sdks/go/go.mod sdks/go/go.sum sdks/go/gen
git commit -m "build(sdk): buf config + connect-go stub generation for the gateway proto"
```

---

## Task 2 (Batch A): Gateway h2c listener

**Files:**
- Modify: `crates/grpc-gateway/src/serve.rs:22-35`

- [ ] **Step 1: Write the failing test**

Add a gateway integration test (in the existing `tests/` harness that boots `Broker::start` + the gateway on a plaintext port) that opens an **HTTP/2 prior-knowledge (h2c)** connection and completes a unary `Send` — asserting the plaintext listener accepts h2, not just h1. (An h2c client: `hyper` client with `http2_only(true)`, or a raw `PRI * HTTP/2.0` preface probe.) It fails today because `axum::serve` is h1-only.

- [ ] **Step 2: Run to verify it fails; implement**

Replace the plaintext `axum::serve(listener, app)` (`serve.rs:30`) with a `hyper_util` auto listener that negotiates h1 **and** h2c (`TokioIo` is already imported, `:11`):

```rust
use hyper_util::server::conn::auto;
use hyper_util::rt::{TokioExecutor, TokioIo};
// ...
loop {
    let (tcp, peer) = tokio::select! {
        () = shutdown.cancelled() => break,
        res = listener.accept() => match res { Ok(v) => v, Err(_) => continue },
    };
    let app = app.clone();
    tokio::spawn(async move {
        let io = TokioIo::new(tcp);
        let svc = hyper::service::service_fn(move |mut req| {
            let app = app.clone();
            async move { req.extensions_mut().insert(peer); app.clone().oneshot(req).await }
        });
        // auto::Builder serves HTTP/1.1 AND HTTP/2 cleartext (h2c) on the same port,
        // so connect-go can open the bidi Subscribe stream over h2c.
        let _ = auto::Builder::new(TokioExecutor::new())
            .serve_connection(io, svc)
            .await;
    });
}
```

(Confirm the `hyper-util` feature `server-auto` is enabled in `Cargo.toml`; add it if not. The TLS path at `:85` may also move to `auto::Builder` + ALPN as a follow-up — not required for MSG-5's h2c harness.)

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway --test <serve/streaming test>` → PASS (h1 tests still pass; the new h2c `Send` succeeds).

```bash
git add crates/grpc-gateway/src/serve.rs crates/grpc-gateway/Cargo.toml
git commit -m "feat(gateway): serve h2c on the plaintext listener (unblocks non-Rust Connect streaming)"
```

---

## Task 3 (Batch B): Go SDK — `publish` + `publishEvent` (CloudEvents)

**Files:**
- Create: `sdks/go/client.go`, `sdks/go/cloudevents.go`, `sdks/go/cloudevents_test.go`

Depends on Task 1 (stubs).

- [ ] **Step 1: Write the failing unit test (pure CE mapping — no gateway)**

```go
package crabka

import ("testing"; gatewayv1 "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1")

func TestRecordForEvent_BinaryMode(t *testing.T) {
    rec := recordForEvent("orders", CloudEvent{
        ID: "1", Source: "/svc", Type: "order.created", DataContentType: "application/json",
        Data: []byte(`{"n":7}`),
    })
    h := rec.GetHeaders()
    if string(h["ce_id"]) != "1" || string(h["ce_source"]) != "/svc" ||
        string(h["ce_type"]) != "order.created" || string(h["ce_specversion"]) != "1.0" {
        t.Fatalf("missing/incorrect ce_ headers: %v", h)
    }
    if string(h["content-type"]) != "application/json" { t.Fatalf("datacontenttype must map to bare content-type") }
    if _, bad := h["ce_datacontenttype"]; bad { t.Fatalf("must never emit ce_datacontenttype") }
    if string(rec.GetRaw()) != `{"n":7}` { t.Fatalf("data must be the raw value") }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd sdks/go && go test ./...`
Expected: FAIL — `recordForEvent`/`CloudEvent` undefined.

- [ ] **Step 3: Implement `client.go` + `cloudevents.go`**

```go
// client.go
package crabka

import (
    "context"; "net/http"
    "connectrpc.com/connect"
    gatewayv1 "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1"
    "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1/gatewayv1connect"
)

type Client struct { rpc gatewayv1connect.GatewayServiceClient; bearer string }

// New dials the gateway. For h2c (plaintext HTTP/2, required by Subscribe) pass
// an h2c http.Client; bearer is an optional dev/test token.
func New(endpoint string, httpClient *http.Client, opts ...Option) *Client {
    c := &Client{rpc: gatewayv1connect.NewGatewayServiceClient(httpClient, endpoint)}
    for _, o := range opts { o(c) }
    return c
}

type RecordResult struct { Partition int32; Offset int64; Deduplicated bool }

func (c *Client) Publish(ctx context.Context, topic string, value []byte, opts ...PublishOption) (RecordResult, error) {
    rec := &gatewayv1.Record{Topic: topic, Body: &gatewayv1.Record_Raw{Raw: value}}
    for _, o := range opts { o(rec) }
    return c.send(ctx, rec)
}

func (c *Client) PublishEvent(ctx context.Context, topic string, ev CloudEvent) (RecordResult, error) {
    return c.send(ctx, recordForEvent(topic, ev))
}

func (c *Client) send(ctx context.Context, rec *gatewayv1.Record) (RecordResult, error) {
    req := connect.NewRequest(&gatewayv1.SendRequest{
        Records: []*gatewayv1.Record{rec}, Acks: gatewayv1.Acks_ACKS_ALL,
    })
    if c.bearer != "" { req.Header().Set("Authorization", "Bearer "+c.bearer) }
    resp, err := c.rpc.Send(ctx, req)
    if err != nil { return RecordResult{}, err }
    r := resp.Msg.GetResults()[0]
    if e := r.GetError(); e != nil { return RecordResult{}, &SendError{Code: e.GetCode(), Message: e.GetMessage()} }
    return RecordResult{Partition: r.GetPartition(), Offset: r.GetOffset(), Deduplicated: r.GetDeduplicated()}, nil
}
```

```go
// cloudevents.go
package crabka

import gatewayv1 "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1"

type CloudEvent struct {
    ID, Source, Type, SpecVersion, Subject, Time, DataContentType string
    Data []byte
}

// recordForEvent maps a CloudEvent to the MSG-2 binary-mode in-Kafka form:
// ce_<name> underscore headers, datacontenttype -> bare content-type, data -> raw.
func recordForEvent(topic string, ev CloudEvent) *gatewayv1.Record {
    spec := ev.SpecVersion; if spec == "" { spec = "1.0" }
    h := map[string][]byte{
        "ce_id": []byte(ev.ID), "ce_source": []byte(ev.Source),
        "ce_type": []byte(ev.Type), "ce_specversion": []byte(spec),
    }
    if ev.Subject != "" { h["ce_subject"] = []byte(ev.Subject) }
    if ev.Time != "" { h["ce_time"] = []byte(ev.Time) }
    if ev.DataContentType != "" { h["content-type"] = []byte(ev.DataContentType) } // never ce_datacontenttype
    return &gatewayv1.Record{Topic: topic, Body: &gatewayv1.Record_Raw{Raw: ev.Data}, Headers: h}
}
```

(Add `Option`/`PublishOption` (key, headers, partition, idempotencyKey), `SendError`, and a `WithBearer` option.)

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cd sdks/go && go test ./...` → PASS.

```bash
git add sdks/go/client.go sdks/go/cloudevents.go sdks/go/cloudevents_test.go
git commit -m "feat(sdk-go): publish + publishEvent (CloudEvents binary mode)"
```

---

## Task 4 (Batch B): Gateway OCI image + compose harness

**Files:**
- Create: `packaging/apko/crabka-gateway.yaml`, `sdks/go/testdata/docker-compose.yml`
- Modify: `.github/workflows/publish-images.yml` (matrix entry)

- [ ] **Step 1:** Author `packaging/apko/crabka-gateway.yaml` mirroring an existing apko config (e.g. broker), entrypoint `/usr/bin/gateway`; add a `crabka-gateway` entry to the `publish-images.yml` build matrix.
- [ ] **Step 2:** `sdks/go/testdata/docker-compose.yml` launches the broker + gateway (plaintext, h2c) exposing the gateway port on `localhost`.
- [ ] **Step 3:** Verify the image builds and the gateway answers a unary `Send` over h2c (`docker compose up` + a quick `go run` publish). Commit.

```bash
git add packaging/apko/crabka-gateway.yaml sdks/go/testdata/docker-compose.yml .github/workflows/publish-images.yml
git commit -m "build(gateway): OCI image + docker-compose harness for SDK integration tests"
```

---

## Task 5: Go SDK — `subscribe` + filter + live round-trip

**Files:**
- Create: `sdks/go/subscribe.go`, `sdks/go/integration_test.go`

Depends on Tasks 2 + 3 + 4.

- [ ] **Step 1: Write the failing integration test (against the compose gateway, h2c)**

Boot the compose harness; `Publish(topic, v)`; `Subscribe(ctx, group, []string{topic})` receives an `Inbound` whose value == `v`; a second case with an equality `Filter` delivers only matching structured records. Build the client with an **h2c** `http.Client` (`golang.org/x/net/http2` with `AllowHTTP:true` + a plaintext `DialTLS`). Tag `//go:build integration` so it only runs in the harness job.

- [ ] **Step 2: Run to verify it fails; implement `subscribe.go`**

```go
package crabka

import (
    "context"
    "connectrpc.com/connect"
    gatewayv1 "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1"
)

type Inbound = gatewayv1.Inbound

type SubscribeOptions struct {
    Filters []*gatewayv1.FieldPredicate // EQUALS-only, structured records only (documented)
}

// Subscribe opens the bidi Subscribe stream (over h2c), sends one SubscribeStart
// with auto_commit=true, then yields server Inbound messages until ctx is done.
// Manual per-offset ack is NOT exposed in v1 (advisory until MSG-3).
func (c *Client) Subscribe(ctx context.Context, group string, topics []string, opt SubscribeOptions) (<-chan *Inbound, <-chan error) {
    out := make(chan *Inbound); errc := make(chan error, 1)
    stream := c.rpc.Subscribe(ctx)
    if c.bearer != "" { stream.RequestHeader().Set("Authorization", "Bearer "+c.bearer) }
    go func() {
        defer close(out); defer close(errc)
        if err := stream.Send(&gatewayv1.SubscribeFrame{
            Frame: &gatewayv1.SubscribeFrame_Start{Start: &gatewayv1.SubscribeStart{
                GroupId: group, Topics: topics, AutoCommit: true, Predicates: opt.Filters,
            }},
        }); err != nil { errc <- err; return }
        for {
            msg, err := stream.Receive()
            if err != nil { errc <- err; return }
            select { case out <- msg: case <-ctx.Done(): errc <- ctx.Err(); return }
        }
    }()
    return out, errc
}

// Equals builds an EQUALS FieldPredicate over a JSONPath (the Chapter-G filter
// surface; equality-only, matches decoded structured records only).
func Equals(jsonPath, value string) *gatewayv1.FieldPredicate {
    return &gatewayv1.FieldPredicate{Path: jsonPath, Op: gatewayv1.PredicateOp_EQUALS, Value: value}
}
```

(Match the generated `SubscribeStart`/`FieldPredicate`/`PredicateOp` field names exactly; adjust if codegen differs.)

- [ ] **Step 3: Run to verify it passes; commit**

Run (in the harness): `cd sdks/go && go test -tags integration ./...` → PASS (round-trip + filter).

```bash
git add sdks/go/subscribe.go sdks/go/integration_test.go
git commit -m "feat(sdk-go): subscribe with filter over h2c (auto_commit)"
```

---

## Task 6: CI job + honest README + final gate

**Files:**
- Create: `sdks/go/README.md`; Modify: `.github/workflows/` (a Go SDK job + buf drift check)

- [ ] **Step 1: buf drift + Go SDK CI job**

Add a workflow job (reuse `actions/setup-go@v6`): `buf generate` then `git diff --exit-code sdks/go/gen` (mirror `codegen-check.yml`); then bring up the compose harness (built gateway image) and run `go test -tags integration ./sdks/go/...`. Note in the workflow that TS/Python jobs are blocked pending `setup-node`/`setup-python`.

- [ ] **Step 2: Honest README** — `sdks/go/README.md`: document `publish`/`publishEvent`/`subscribe`+`Equals`; and clearly label as **experimental/unimplemented** with links to the gating work: manual per-offset ack (h2 + MSG-3), share-group consume (net-new gateway RPC), topic auto-provision (net-new `EnsureTopic` RPC), CloudEvents *consume* (MSG-1); note the filter is **EQUALS-only, structured-records-only** and bearer tokens are **dev/test-only** (unsecured JWS).
- [ ] **Step 3: Gate** — `cargo +nightly fmt --check` (serve.rs); `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`; `cd sdks/go && gofmt -l . && go vet ./...`; `buf lint`. Commit.

```bash
git add sdks/go/README.md .github/workflows/
git commit -m "ci(sdk-go): buf drift + integration job; docs: honest SDK surface"
```

---

## Self-Review

**1. Spec coverage:** buf foundation + `sdks/go` + connect-go stubs (Task 1); the h2c enablement that makes bidi Subscribe work (Task 2); `publish`/`publishEvent` CE binary mapping (Task 3); the gateway OCI image + compose harness (Task 4); `subscribe`+filter + live round-trip over h2c (Task 5); CI drift+integration job + honest README (Task 6). Deferred set (TS/Python, manual ack, share-consume, auto-provision, CE-consume) named and untouched — Scope boundary. ✅

**2. Placeholder scan:** Task 1 gives real buf configs; Task 2 gives the exact `serve.rs:30` swap; Task 3 is complete Go (`recordForEvent` + `Publish`/`send`); Task 5 is complete Go (`Subscribe` + `Equals`). Codegen-dependent field names (`Record_Raw`, `SubscribeFrame_Start`, `PredicateOp_EQUALS`) are flagged to confirm against the generated stubs. No `TBD`.

**3. Type consistency:** `recordForEvent` (Task 3) returns the `*gatewayv1.Record` `Publish`/`send` consume; `Client`/`bearer` (Task 3) is reused by `Subscribe` (Task 5); the h2c listener (Task 2) is the transport the integration test's h2c `http.Client` (Task 5) requires; `buf.gen.yaml`'s `out: sdks/go/gen` (Task 1) is the import path used throughout.

**4. Invariant check:** Connect codegen only (Task 1); `.proto` canonical + drift check (Tasks 1, 6); Subscribe proven over h2c (Tasks 2, 5); honest surface labeling (Task 6 README + experimental in code comments); behavior-tested against a live gateway (Tasks 4, 5); `sdks/` out of the Cargo workspace (Task 1). Each task green before commit.

**5. Prerequisites:** none blocks publish/subscribe v1. MSG-1 (SDK CE-consume) and MSG-3 (manual per-offset ack) are named prerequisites for the *deferred* surface only. Batching: Task 1 ∥ Task 2 → Task 3 ∥ Task 4 → Task 5 → Task 6.
