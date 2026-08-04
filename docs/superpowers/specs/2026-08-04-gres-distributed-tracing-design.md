# Rich distributed traces for crabka-gres (design)

**Date:** 2026-08-04
**Status:** Implemented
**Phase:** 6 (Observability). Builds on slice 42 (`crabka-broker` OTLP pipeline, [design](2026-05-23-crabka-broker-otlp-tracing-42-design.md)) and reuses its `crabka_telemetry` pipeline wholesale.

## Goal

Make a slow gres query explain itself.

Before this change no gres crate participated in tracing at all: `crabka-gres` installed a bare `tracing_subscriber::fmt()`, there was not a single span across `pgwire`, `pgexec`, `pgparser`, `pgkv`, `pgmvcc`, `gres-ranges` or `gres-substrate`, and `EXPLAIN ANALYZE` reported row counts with no timings. When a statement was slow there was no way to say *which step* was slow — parse, timestamp grant, routing, the cross-node RPC hop, the MVCC scan, a row-lock wait, the 2PC round, or the WAL append — and no way to connect a slow application request to the query behind it.

The outcome is that an application request carrying a W3C `traceparent` produces **one** trace that descends through the pgwire session, the statement, routing, every cross-node RPC hop, the timestamp round, the executor's scans and the durable WAL append, viewable as a waterfall in Grafana against Crabka's own traces backend.

## Design Goals

**Free when off.** gres is a database front door; the query path may not pay for telemetry nobody collects. Every span sits on a dedicated `DEBUG` target that the stdout `fmt` filter deliberately does not name, so on a gres with no OTLP configured the callsites are a load-and-branch and nothing more. Any span whose field expressions cost more than a struct field read is built behind an explicit `tracing::enabled!` guard with a `Span::none()` fallback, because a disabled callsite still evaluates its arguments.

**One trace, not one per hop.** A trace that stops at the gateway is worth very little — the interesting latency is usually on the range owner. The context therefore has to survive three separate boundaries, each with a different transport and a different right answer (below).

**The client says which trace; gres says how much of it.** The traceparent is attacker-controlled input on a database connection. Honouring it verbatim lets any client force 100% export of every span on every range owner it touches, which is a denial-of-service against the collector. Honouring it not at all breaks the very join the feature exists for.

**Never fail a query for telemetry.** A malformed traceparent, an unreachable collector, a full export queue: none of these may surface to the client. Ingress validation rejects silently; export is batched and lossy by construction.

## Architecture Overview

```
client (OTel-instrumented driver)
  │  SQL + /*traceparent='00-<trace>-<span>-01'*/
  ▼
┌──────────────────── gres node A (gateway) ────────────────────┐
│ gres.session ──┐                                              │
│                └─ gres.statement  ← ingress parent attaches   │
│                     └─ db.statement / pg.select / pg.write    │
│                          └─ pg.routed_statement               │
│                               └─ gres.range_rpc  [CLIENT]     │
└───────────────────────────────┬───────────────────────────────┘
                                │ RangeEnvelope { trace, request }
                                │ over the mTLS range RPC
┌───────────────────────────────▼─── gres node B (range owner) ─┐
│ gres.range_serve [SERVER]                                     │
│   └─ pg.parse.sql, db.statement, pg.select/pg.write           │
│        ├─ pg.scan, pg.lock.row, pg.execute_write              │
│        ├─ pg.timestamp.read → tso.grant, range.barrier        │
│        └─ pg.commit → gres.wal_append ──┐                     │
└─────────────────────────────────────────┼─────────────────────┘
                                          │ traceparent in WAL record headers
                                          ▼  (attached as a LINK, never a parent)
                        gres.wal_apply on every follower, checkpointer, replay
```

`crabka-trace-context` is a new published leaf crate holding the whole propagation vocabulary: `TraceCarrier` (capture, apply as parent, apply as link, render as headers) and `extract_sqlcommenter`. It exists because the crate graph forces it — `crabka-telemetry` is `publish = false` while `crabka-pgwire`, `crabka-pgexec` and `crabka-pgparser` are published, and a published crate cannot depend on an unpublished one. Independently, `crabka-telemetry` pulls in `axum`, `clap`, `pprof`, `tonic` and `opentelemetry-otlp`, none of which belong in a wire-protocol crate. `crabka-telemetry` now re-exports it, so its eight existing call sites compile untouched.

### A worked waterfall: single-range SELECT through a non-owning gateway

Captured from the live two-process cluster the verification test drives. Node 0 is the gateway; `t1000000` lives on range 1, hosted by node 1. Indentation is parent-child; `@node` is the `service.instance.id` resource attribute.

```
gres.statement                      [SERVER]  @node0   parent = the client's span
  SELECT t1000000                   [INTERNAL] @node0  ← gateway routing span, renamed by otel.name
    Sql                             [CLIENT]  @node0   ← gres.range_rpc, renamed to its rpc.method
      Sql                           [SERVER]  @node1   ← gres.range_serve
        pg.parse.sql                [INTERNAL] @node1
        SELECT t1000000             [INTERNAL] @node1  ← pgexec db.statement
          pg.select                 [INTERNAL] @node1  pg.read_ts=1 pg.fast_path=true
            gres.wal_append         [PRODUCER] @node1
            range.barrier           [CLIENT]  @node1   pg.barrier.mode=read
            pg.timestamp.read       [CLIENT]  @node1
              tso.grant             [CLIENT]  @node1
```

Two things in that tree are worth pointing out because they surprise people. A **read** appends to the WAL — the range records its read marker durably, so `gres.wal_append` is on the read path and its latency belongs to the SELECT. And `pg.select` carries `pg.fast_path=true`, which is only meaningful because the streaming fast path was actually reachable: `RuntimeSession` was found to forward every `Session` method except `simple_query_into`, so every simple-protocol query fell to the buffering default and both streaming paths were dead in production. That was a pre-existing performance bug, fixed as part of this work because otherwise the attribute would have read `false` forever.

### A worked waterfall: cross-shard transaction

`BEGIN; INSERT INTO t0 …; INSERT INTO t1000000 …; COMMIT;` at node 0's gateway, where `t0` is on range 0 (local) and `t1000000` on range 1 (remote). Each statement is its own `gres.statement`; the client's traceparent joins them into one trace.

```
gres.statement  BEGIN                                     @node0
gres.statement  INSERT t0                                 @node0
  pg.routed_statement  pg.local=true  pg.range_id=0       @node0
    db.statement INSERT t0                                @node0
      pg.write → pg.execute_write                         @node0
      pg.commit  pg.gate_wait_ms=0.0018                   @node0
        gres.wal_append  pg.wal.frames=1 pg.wal.bytes=132 @node0
        kv.apply                                          @node0
gres.statement  INSERT t1000000                           @node0
  pg.routed_statement  pg.local=false pg.range_id=1       @node0
    pg.remote_statement                     [CLIENT]      @node0
      Session                               [CLIENT]      @node0  ← gres.range_rpc
        Session                             [SERVER]      @node1  ← gres.range_serve
          gres.session_operation  pg.operation=SimpleQuery @node1
            db.statement INSERT t1000000                   @node1
              pg.write                                     @node1
                range.barrier                              @node1
                pg.execute_write                           @node1
                pg.commit → gres.wal_append, kv.apply      @node1
gres.statement  COMMIT                                     @node0
```

The shape an operator reads off this: the remote arm costs one `Session` round trip per engine operation (`BEGIN`, `SetTimestampOwner`, the statement itself — three RPCs for one INSERT), and `pg.gate_wait_ms` on each `pg.commit` separates queueing behind the WAL write gate from the append itself. That single field is the most useful commit-latency signal in the system.

## Key Design Decisions

### Ingress: sqlcommenter first, GUC second, never a wire-format change

Clients attach the context by appending `/*traceparent='00-<32 hex>-<16 hex>-<2 hex>'*/` to the SQL — the [sqlcommenter](https://google.github.io/sqlcommenter/) convention OpenTelemetry-instrumented drivers already emit, so an application that is already traced needs no gres-specific code. The PostgreSQL wire protocol has no header to carry it and adding one would break every existing client.

This is free because `crabka_pgparser`'s lexer already skips `--` and `/* */` comments, nesting-aware, without emitting a token. The tag changes no AST, and — importantly — the SQL text is **not** rewritten to strip it: `Parser::new` keeps the original string and a `ParseError` carries a byte offset into it that surfaces as the SQLSTATE 42601 error position. Rewriting would silently misreport every syntax error's column.

The cost when no tag is present is one `str::find("traceparent")` returning `None`. On a hit the extractor walks genuine comment regions, reusing the shape of the existing positional-parameter scanner, which is what gets the correctness trap right: `SELECT '/*traceparent=…*/'` is a string literal and must not extract.

`SET crabka.traceparent = '…'` is the secondary channel, working with zero engine changes because the GUC layer already falls through to `is_custom_guc_name` for any `extension.parameter` name. Statement sqlcommenter outranks the GUC, because a per-statement signal is more specific than a session one.

The extended protocol needs its own answer, because `Bind` and `Execute` carry no SQL to read a tag from. The `gres.statement` span is created at `Execute` and takes its parent from, in order: the `crabka.traceparent` GUC (the only genuinely per-execution channel), then the carrier captured at `Parse` time and cleared on `Sync`. That lifetime is exactly right — an unnamed prepared statement is a one-shot pipelined batch, which is what every ORM emits, while a named statement surviving a `Sync` is genuinely reused and its `Parse`-time trace is stale. `Parse` also gets a span of its own, because for a sharded table it forwards the prepare to the range owner: a real network hop that would otherwise be invisible.

### Why `Resample` recomputes the sampled flag instead of clearing it

The ingress policy is `IngressTracePolicy = Off | Link | Resample { ratio } | Trust`, defaulting to `Resample`.

The problem it solves: the SDK sampler is `ParentBased(TraceIdRatioBased(ratio))`, and a *sampled* remote parent makes `ParentBased` return `RecordAndSample` unconditionally. A client that appends `-01` to every statement therefore forces 100% export of every gres span on every range owner it touches — from an unprivileged SQL connection.

The obvious fix, clearing the sampled bit and letting gres decide, is wrong, and wrong in a way that only shows up in production. **`ParentBased` with a non-sampled parent returns `Drop`. It does not fall through to the root sampler.** Clearing the bit would drop exactly the statements the client took the trouble to instrument, and the trace would end at the application tier.

`Resample` therefore keeps the trace-id and span-id and **recomputes the sampled flag locally** at gres's configured ratio, using a byte-for-byte restatement of `TraceIdRatioBased`. Because that decision is a pure function of the trace-id, a client and a gres running at the same ratio reach the same answer for the same trace by construction, so traces stay whole rather than being half-exported. The reimplementation (rather than calling `opentelemetry_sdk`) is deliberate: the SDK is an exporter-side dependency with no business inside a wire-protocol crate.

`Link` is the setting for untrusted clients that should still be joinable after the fact; `Trust` is documented as trusted-clients-only; `Off` ignores ingress entirely.

Validation is unconditional and silent: the traceparent must match `00-<32 hex>-<16 hex>-<2 hex>` with non-zero trace-id and span-id, and a `tracestate` over 512 bytes or 32 members is dropped. On rejection there is no parent, no link, and **no error to the client** — a bad traceparent must never fail a query. The raw client string is never stored or logged; the hex is re-rendered from the parsed `SpanContext`.

### Node-to-node: a private envelope at the codec layer

`crates/gres-ranges/src/transport.rs` gained a module-private

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]   // deliberately NOT PartialEq
struct RangeEnvelope { trace: TraceCarrier, request: RangeRequest }
```

constructed at write and destructured at read. This touches **zero** of the 24 `FramedTcpClient::call` call sites and zero of the 17 `impl RangeService` blocks; only four private functions change. Keeping `RangeEnvelope` out of `PartialEq` preserves `RangeRequest`'s equality as a pure function of the payload, so the existing codec round-trip assertions stay valid.

Two alternatives were rejected. A `traceparent` field per variant means ~25 variants, 155 construction sites, and breaks those equality tests. A separate preamble frame doubles syscalls on a path that sets `TCP_NODELAY` specifically to avoid that.

One correctness consequence had to be fixed alongside: `JoinRangeReq::fits_transport_frame` sized the request against the 1 MiB `MAX_FRAME`, but the frame on the wire is now the envelope. Without a named `ENVELOPE_RESERVE` subtracted, a join sized to exactly 1 MiB turns a clean `JoinValidationError` into a `TransportError::FrameTooLarge` — a good error replaced by a confusing one.

### WAL: links, never parent-child

The producer injects `traceparent` into WAL record headers, hoisted out of the per-frame loop. Consumers attach it as an OpenTelemetry **link** on one span per apply batch, never as a parent. Three independent reasons, any one of which would be sufficient:

A replay at recovery may run hours after the commit, and parenting would stretch the trace's wall-clock width to the WAL retention period. One commit fans out to every follower, every checkpoint service and every future replay, so the child set is unbounded. And `ParentBased` would force export of *every apply of every sampled write, forever*, on the hottest loop in the system.

The context could not travel through `ReplayItem` — that is a pure decode-and-apply type with ~10 construction sites, and threading telemetry through it would be the wrong kind of coupling. It travels instead via a defaulted `CommittedWalReader::committed_from_traced` returning `TracedWalRecords { items, links }`; readers that decode no headers inherit the default and honestly report zero links. Links are capped at 8 distinct trace-ids per batch.

### SQL text: three tiers, and why the verbatim tier is off

1. `db.query.summary` — `"SELECT orders"` — derived from the **already-parsed** statement, so it costs a match arm and a format, not a second parse. Always on. A literal-normalizer was considered and rejected: it is a second parser pass for grouping power the summary already provides.
2. `db.operation.name`, `db.collection.name`, `db.namespace`, `pg.table_id`. Always on.
3. `db.query.text` — verbatim SQL, truncated at 4 KiB. **Off by default**, gated by `CRABKA_OTLP_SQL_TEXT`.

Tier 3 is the only real secret and personal-data exposure in the whole feature: the query text is the statement as the client sent it, literals included — `INSERT INTO users VALUES ('123-45-6789', …)`, `ALTER ROLE app PASSWORD 'hunter2'`. Anything that reaches the collector reaches everyone who can read the trace backend, which is usually a much wider audience than the database. It is therefore an environment-only opt-in for a targeted investigation, and is deliberately absent from the `Gres` CRD so it cannot be turned on fleet-wide by editing a manifest. The flag is read through a `LazyLock<bool>` in each recording crate, which keeps `pgexec` and `gres-ranges` free of a `crabka-telemetry` dependency.

Text and summary are recorded only on `gres.statement` and `db.statement`, never repeated on children.

### Targets, levels, and the operator's three settings

Five targets, each named for the crate that owns it so an `EnvFilter` directive reads as the subsystem it selects:

| Target | Level | Spans |
|---|---|---|
| `crabka_pgwire::session` | `DEBUG` | `gres.session`, `gres.statement`, `gres.parse`/`bind`/`describe` |
| `crabka_pgexec::statement` | `DEBUG` | `pg.parse.sql`, `db.statement`, `pg.select`, `pg.write`, `pg.ddl` |
| `crabka_pgexec::exec` | `DEBUG`/`TRACE` | `gres.exec_read`, `pg.execute_write`, `pg.read_context` at `DEBUG`; `pg.scan`, `pg.lock.row`, `pg.blocking_worker` at `TRACE` |
| `crabka_gres_ranges::route` | `DEBUG`/`TRACE` | `gres.range_rpc`, `gres.range_serve`, the 2PC rounds, `tso.grant`, `range.barrier` at `DEBUG`; `pg.route` at `TRACE` |
| `crabka_gres_substrate::wal` | `DEBUG`/`TRACE` | `pg.commit`, `gres.wal_append`, `gres.wal_apply` at `DEBUG`; `wal.chunk` at `TRACE` |

The three recipes an operator actually chooses between, documented in full in `crabka_gres::telemetry`:

- **Statement level only** — `info,crabka_pgwire::session=debug,crabka_pgexec::statement=debug`. One span per session and per statement, nothing about routing or storage. The cheapest setting that is still useful for always-on production tracing.
- **Default** — the statement tier plus routing, the cross-node hops, the 2PC rounds and the WAL append. This is the waterfall above, and it is what you get by leaving `CRABKA_OTLP_FILTER` unset.
- **Full internal detail** — every target widened to `=trace`. A single large scan emits many spans, so this is for a targeted investigation, not steady state.

The stdout `fmt` filter names none of the five, exactly as the broker's does. Naming one there would print a span line per statement — or per scan — to stdout on a gres that is not exporting at all.

### No spans on streaming sinks, and wait-only row locks

`WireResultSink::send`, `OffsetResultSink`, `write_row_chunks` and `into_bounded_row_pages` get no spans: a 100k-row result would emit roughly a hundred page spans the exporter would drop anyway. Counters accumulate in the caller's loop and are recorded once on the enclosing span. Likewise `pg.lock.row` is created **only when the acquire actually waits** — otherwise it is one span per row touched, and an uncontended lock is not information.

`pgkv`, `pgmvcc`, `pgparser` and `pgcatalog` get no spans at all. They do not depend on `tracing` today, and they carry no attribute the enclosing `pg.scan` does not already have.

## Findings that cost real debugging time

None of these are in the OpenTelemetry documentation. Each was found by a failing test or a silently-wrong export.

**`tracing-opentelemetry` 0.33 recognises `otel.status_description`, not `otel.status_message`.** The wrong name exports as an ordinary attribute and yields `Error { description: "" }` — the status is right, the message is silently gone. **And the order of recording matters:** set `otel.status_code` *first*, then the description. The layer treats setting the code as setting a status with an empty description, so recording the description first erases it. A test that asserts only "status is `Error`" passes either way; pin the description text.

**`u64` and `usize` fields export as OTLP strings.** OTLP has no unsigned integer type, so `tracing-opentelemetry` stringifies them. Recorded naively, `pg.rows_affected`, `pg.participants`, `pg.read_ts`, xids, table ids and every count arrive as strings, and Tempo and Grafana cannot compare, sort or range-filter them — `pg.participants > 2` silently matches nothing. Every numeric attribute goes through a saturating `TryInto<i64>` helper declared in each crate's telemetry module. Deliberately-textual fields such as `pg.participant_ranges` (a comma-joined list) stay strings. Assert the exported attribute is `Value::I64(_)`, not merely that it exists.

**`otel.name` renames the exported span.** It maps onto the OTel span name, so `db.statement` arrives as `SELECT orders` and `gres.range_serve` arrives as `Sql` — see both waterfalls above, where every renamed span is shown under its exported name. Nothing downstream may search for a span literally named `db.statement` or `gres.range_serve`: filter on `db.system.name = "postgresql"` and on `rpc.system = "crabka.range"` plus the span kind. This applies to Grafana dashboards and to every test assertion; the cross-process test pins the rename explicitly so a future change to it fails loudly.

**Test harnesses must install the propagator.** A subscriber alone is not enough. Without `opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new())`, `TraceCarrier::apply_to` silently no-ops and every ingress and propagation test passes **vacuously**. `crabka_telemetry::init` installs it in production; tests must do it themselves. Tests that cross a `spawn_blocking` or `thread::scope` boundary must additionally use `set_global_default` rather than `with_default`, because a thread-local subscriber is invisible on those threads and the test then passes with zero spans. This is the single easiest way to ship a broken propagation path with a green suite.

**In-process context loss has exactly two shapes, and they need different fixes.** A `tracing::Span` carries its own `Dispatch` and `tracing-opentelemetry` keys the OTel context off the registry by span id, not off a thread-local — so a cloned `Span` handle reconstitutes full context on any thread in any runtime, and none of this needs `opentelemetry::Context::attach`. For the five `spawn_blocking` sites the payload is a synchronous closure, so capture the span before the move and `let _g = span.enter();` as the outermost wrapper (bind the guard to a name; `let _ = …entered()` drops instantly and does nothing). For `thread::scope` plus a fresh current-thread runtime the payload is a future, so `block_on(fut.instrument(span))` is right — `Instrumented` re-enters on every poll, which an `enter()` guard across `block_on` only appears to do until something spawns.

**`std::future::pending()` after an error hand-off loses the span.** Two WAL writer paths hand an error to a channel and then await `pending()` forever. The span status must be recorded *before* the `pending()`, or the span never closes, never exports, and the one span you needed is the one you lose.

## Integration

The pipeline is `crabka_telemetry`'s, unchanged: `crabka-gres` reads the same environment contract as the broker (`CRABKA_OTLP_ENABLED`, `CRABKA_OTLP_ENDPOINT`, `CRABKA_OTLP_PROTOCOL`, `CRABKA_OTLP_SAMPLE_RATIO`, `CRABKA_OTLP_TIMEOUT`, `CRABKA_OTLP_HEARTBEAT_INTERVAL`, `CRABKA_OTLP_FILTER`) with the standard `OTEL_*` fallbacks and the `OTEL_SDK_DISABLED` kill switch. OTLP is off unless something opts in. `main` holds the `TelemetryGuard` for the process lifetime and calls `shutdown()` on both exit paths, because the final batch is the one containing whatever made gres stop.

`service.instance.id` is derived from the node's advertised range endpoint — the address the cluster already identifies a compute node by — with `OTEL_SERVICE_INSTANCE_ID` winning when set and `HOSTNAME` prefixed when the container runtime provides it.

`Gres.spec.tracing` exposes the deployment-policy subset, reusing the `Kafka.spec.tracing` types rather than declaring a parallel set. The export filter, the heartbeat interval and `CRABKA_OTLP_SQL_TEXT` stay environment-only: the first two are debugging controls rather than fleet policy, and the third can export secrets. See [`docs/configuration-audit.md`](../../configuration-audit.md#gres-distributed-tracing-configuration).

Traces land in Crabka's own traces backend (`crates/traces`, the Tempo-equivalent) via the demo observability stack, alongside the nine binaries that already export to it.

## Kafka / KIP Compliance

No Kafka wire-protocol surface changes. The WAL carrier uses ordinary Kafka **record headers**, which is where the OpenTelemetry Kafka interceptors put `traceparent` too, so a Crabka WAL topic read by any OTel-aware consumer carries the context in the place that consumer expects. The `RangeEnvelope` is on Crabka's private range RPC, not on any Kafka-compatible protocol.

## Testing

Five layers, each pinning values rather than presence — "a traceparent exists" survives a mutant that injects a constant, while `value.contains(&trace_id) && value.ends_with("-01")` does not.

1. **Unit** (`crabka-trace-context`) — table-driven sqlcommenter cases: trailing and leading comment, traceparent plus tracestate, nested `/* /* */ */`, `--` line comment, absent (which asserts the fast path), malformed, oversized tracestate, and `SELECT '/*traceparent=…*/'` which must **not** extract. Plus a behavioural parser check that `parse(with_comment)` equals `parse(without)` over the whole `Vec<Statement>`.
2. **pgwire ingress** — drives `run_session` over `tokio::io::duplex` against a stub engine that reports the current span's `SpanContext`, for both protocols, including that a *named* statement reused after a `Sync` does not inherit the stale `Parse`-time trace.
3. **Real TLS hop** — `gateway_local.rs` with an `InMemorySpanExporter`, asserting the `gres.range_serve` span's `parent_span_id` equals the `gres.range_rpc` span's `span_id` across one trace-id.
4. **WAL links** — asserts the polled record headers carry `traceparent`, that `gres.wal_apply` has a link with that trace-id, and — explicitly — that its `parent_span_id` is *not* the remote one. That last assertion is what stops someone later "fixing" links back into `set_remote_parent`.
5. **Cross-process** (`crates/gres-loadtest/tests/cross_process_tracing.rs`) — the only layer that can falsify the propagation claim, because every other layer runs in one process where a cloned span handle would satisfy it whether or not the wire carried anything. It stands up an in-test OTLP/gRPC collector, launches a real two-node broker-backed cluster of `crabka-gres` binaries pointed at it, and runs one sqlcommenter-tagged SELECT against a table whose range the gateway does not host. It then asserts that the statement span's parent is the span-id the client wrote into the tag, that the trace spans both named processes, and that a `SERVER`-kind range RPC span is the child of a `CLIENT`-kind one **emitted by a different process**. It skips cleanly when the binaries have not been built.

Manual: `docker compose -f demo/observability/docker-compose.yml up`, `psql` with a tagged query, and confirm the waterfall in Grafana.

## Known gaps

These are real and deliberate; none is a reason to hold the feature.

**COPY-from-STDIN has no statement span.** The `Query` arm's `gres.statement` covers `begin_copy_in` and then ends when the arm continues into copy-in mode, so the `CopyData`/`CopyDone`/`CopyFail` messages that follow carry no statement span and bulk-load latency is untraced. Closing this means modelling a span whose lifetime spans many frontend messages, which is a separate change.

**The 2PC-round spans are unverified end to end.** `pg.timestamp_scatter`, `pg.prewrite`, `pg.resolve`, `pg.commit_global` and `pg.abort_scatter` exist and are unit-covered, but the cross-shard transaction captured above — two participants under `LogicalTso` — produced none of them: the participants' writes and the commit travelled through per-range sessions instead, and the gateway's `COMMIT` routed locally. Whether that is the gateway declining to escalate or a path these spans do not cover is unresolved, and it is the largest remaining hole in the inventory's verification.

**`tso.grant` is in the statement's trace but the `Tso` RPC underneath it is not.** The grant conveyor batches requests onto a background task, so the actual cross-node `Tso` round trip appears as a root span in a trace of its own. A waiter's `tso.grant` therefore shows the wait but not what was waited on. Linking the conveyor's RPC to its waiters is the natural fix.

**`pg.join_strategy` is untested.** It needs a sharded distributed join, which the in-process engine cannot plan.

**Checkpoint restore-tail apply spans carry zero links.** The headers are gone by the time the tail is restored, so those spans honestly report no links rather than pretending.

**The 2PC-round spans did not fire on a real two-range transaction.** On a 2-node/2-range `LogicalTso` cluster, `BEGIN; INSERT t0; INSERT t1000000; COMMIT;` produced no `pg.timestamp_scatter`, `pg.prewrite`, `pg.resolve` or `pg.commit_global` — the participants committed through per-range `Session` RPCs and the gateway's `COMMIT` routed locally with `pg.statement_kind = local`. Either this topology legitimately takes a path those spans do not sit on, or the gateway is not escalating a genuinely cross-range transaction to two-phase commit — which would be an atomicity problem, not a tracing one. Unresolved, and tracked separately. The in-process scatter test in `crates/gres-ranges/tests/gateway_tracing.rs` *does* exercise `pg.timestamp_scatter`, so comparing the two paths is the place to start.

**gres has no Prometheus metrics and no admin port.** No gres crate uses `prometheus_client` and there is nothing to scrape. Spans give per-step latency out of band, but they are sampled and are not a substitute for counters. A real gap, and a separate change.

**`EXPLAIN ANALYZE` still reports no timings.** Wiring real per-node timings into the EXPLAIN tree is a distinct feature — the tree is syntactic and is not what the executor walks.
