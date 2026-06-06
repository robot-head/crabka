# Crabka Schema Registry — Slice 5: HA (cp-exact Kafka-group election + write-forwarding) — design

- **Date:** 2026-06-06
- **Status:** Approved (brainstorm); ready for an implementation plan
- **Builds on:** slices 1+2+2b+2c+3+4 (registry + compat trilogy + deletes/modes/lookups + references). The `KafkaStore` facade (single-node always-primary, write-gate + group-less reader), the axum REST surface, and `RegistryConfig`/the binary all exist. Stacks on slice 4 (PR #410).
- **Parent roadmap:** `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md` (slice 5).

## Motivation

Today the registry is **single-node always-primary**: the `KafkaStore` facade takes a write-gate and is the sole writer of `_schemas`. Slice 5 makes it **multi-node HA**: registry nodes coordinate so exactly one is the **primary** (writer); secondaries serve reads and **forward** mutating REST requests (POST/PUT/DELETE) to the primary; **failover** when the primary dies. This matches Confluent SR, which elects a primary via a Kafka consumer-group ("primary election") where the group leader becomes the SR primary and each node advertises its REST URL in the JoinGroup metadata so secondaries can forward. We match cp's election wire **byte-exactly** so a mixed cp+Crabka SR cluster can co-elect one primary through the Crabka broker — the ultimate fidelity test, consistent with the program's cp-byte-exactness discipline.

## Load-bearing decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Election fidelity** | **cp-exact `"sr"` wire** — the JoinGroup metadata (`SchemaRegistryIdentity`) + SyncGroup assignment (`SchemaRegistryGroupAssignment`) match cp-schema-registry 7.4.0 byte-exactly, Docker-captured. | A mixed cp+Crabka SR cluster co-elects one primary; consistent with every prior slice matching cp. The broker coordinator is already protocol-type-generic, so this needs only the right metadata bytes. |
| **Election client** | A **self-contained `election` module** in the schema-registry crate, implementing the `"sr"` group-membership loop directly over `client-core`'s generic `Client::send`. | Focused on the SR's needs; doesn't refactor `client-consumer` (which would risk consumer regressions). Rejected: extracting a shared group-membership crate (bigger refactor); a `_schemas`-topic leader lease (not cp-faithful). |
| **Scope** | **Full HA in one slice** — election + forwarding + failover + multi-node conformance + the cp election capture. | The parts are coupled (election without forwarding is an unsafe multi-writer; failover falls out of a correct election client). User chose one slice. |
| **Forwarding** | An axum **middleware** that proxies mutating REST from a secondary to the primary's advertised URL via `reqwest`. | The `grpc-gateway` `Forwarder` is a proven pattern; reads + primary-side writes pass through unchanged. |
| **Authority** | The exact `SchemaRegistryIdentity`/assignment bytes, the protocol type/name, and the leader's primary-selection rule are **cp-captured** (Docker via `DescribeGroups`) + asserted. | Same Docker-capture fidelity discipline as slices 2–4. |

## Architecture

Each node runs an **election task** that joins a `"sr"` Kafka group via the Crabka broker's (protocol-generic) coordinator, advertising its REST URL. The coordinator picks a group leader; the leader deterministically selects the **primary** among eligible members and broadcasts the primary's identity in every member's SyncGroup assignment. The election task publishes a shared `PrimaryState { is_primary, primary_url }` over a `watch` channel. An axum **forwarding middleware** wraps the router: a mutating request on a non-primary node is proxied to `primary_url`; reads and primary-side writes pass through. The `KafkaStore` write-gate is unchanged — it is now only ever exercised on the primary, so there is exactly one `_schemas` writer.

```
node N: election task ── JoinGroup{protocol_type:"sr", metadata=SchemaRegistryIdentity} ──► broker coordinator
                     ◄── SyncGroup{ assignment = SchemaRegistryGroupAssignment{ master } } ──┘
                         └─► watch::send(PrimaryState{ is_primary = master==me, primary_url = master.url })

REST request (POST/PUT/DELETE) ─► forwarding middleware
   is_primary?  ── yes ─► handlers ─► KafkaStore (the sole writer)
                └─ no  ─► reqwest proxy to {primary_url}{path} (X-Forwarded-For-Registry header) ─► relay status+body
GET ─► handlers (every node serves reads from its own replayed store)
```

### The `"sr"` election protocol (`election/protocol.rs`)

cp's `SchemaRegistryProtocol` types, serialized byte-exactly (JSON; exact field set + protocol type/name **cp-captured**, expected shapes):
- **`SchemaRegistryIdentity`** (the JoinGroup protocol metadata): `{ version, host, port, scheme, master_eligibility }` (cp historically keeps the `master_eligibility` wire field). This is `to_value`/`serde_json`-serialized into the JoinGroup protocol `metadata` bytes.
- **`SchemaRegistryGroupAssignment`** (the SyncGroup assignment): `{ error, master: <SchemaRegistryIdentity | null> }`.
- `protocol_type = "sr"`; the JoinGroup protocol `name` (the assignor name) is cp-captured.
- **Leader's primary-selection rule:** when this node is the group leader (chosen by the coordinator), it selects the master among the `master_eligibility = true` members using cp's deterministic rule (the member whose identity sorts first by cp's comparison — captured/verified) and writes that master into **every** member's assignment.

### The group-membership client (`election/client.rs`)

A loop over `crabka_client_core::Client::send` (generic over `ProtocolRequest`), using the codecs in `crabka_protocol::owned::{join_group_request, sync_group_request, heartbeat_request, leave_group_request, find_coordinator_request}`:
1. `FindCoordinator(group_id)` → the group-coordinator broker.
2. `JoinGroup{ group_id, protocol_type:"sr", protocols:[{ name, metadata: identity_bytes }], member_id, session_timeout, rebalance_timeout }` → assigned `member_id`, `generation_id`, `leader_id`, and (if leader) the members + their metadata.
3. If **leader**: decode each member's `SchemaRegistryIdentity`, run the selection rule, encode each member's `SchemaRegistryGroupAssignment`, send them in `SyncGroup`. If **follower**: `SyncGroup` with an empty assignment list → receive our assignment.
4. Decode our assignment → `master` → publish `PrimaryState`.
5. `Heartbeat` on the heartbeat interval; on `REBALANCE_IN_PROGRESS` / `UNKNOWN_MEMBER_ID` / `ILLEGAL_GENERATION` → rejoin (back to step 2).
6. `LeaveGroup` on graceful shutdown (so failover is prompt).

Models the proven `client-consumer` join/sync/heartbeat loop (timing, retries, parking), but generic over `protocol_type` + opaque metadata/assignment bytes.

### `PrimaryState` + the facade (`election/mod.rs`)

`Election::start(cfg, client, cancel) -> watch::Receiver<PrimaryState>`. `PrimaryState { is_primary: bool, primary_url: Option<String> }`. The middleware reads the watch lock-free. The `KafkaStore` is unchanged (it trusts the middleware); a brief split-brain window during rebalance is inherent to group election (cp has it too) and is documented, not engineered around in this slice.

### Write-forwarding middleware (`rest/forward.rs`)

An axum `from_fn`/`from_fn_with_state` layer holding `{ primary: watch::Receiver<PrimaryState>, http: reqwest::Client, my_node_id }`:
- **Read** (GET) → pass through.
- **Mutating** (POST/PUT/DELETE) on the **primary** → pass through to the handlers.
- **Mutating** on a **secondary**: proxy to `{primary_url}{path_and_query}` (copy method + body + the vendor content-type; add `X-Forwarded-For-Registry: <my_node_id>`), relay the primary's status + body verbatim. `primary_url == None` (no primary yet) → `503`. A request that **arrives already carrying** `X-Forwarded-For-Registry` is processed locally only if `is_primary`; otherwise → a retriable status so the original forwarder re-resolves (prevents forward loops + stale-primary races).

### Config + binary (`config.rs`, `bin/schema-registry.rs`)

`RegistryConfig` gains `advertised_url: String` (e.g. `http://host:8081`), `group_id: String` (default `"schema-registry"`), `leader_eligibility: bool` (default `true`). The binary gets `--advertised-url`, `--group-id`, `--leader-eligible`; it starts the election task, builds the router, wraps it with the forwarding middleware, and serves. `reqwest` moves from `[dev-dependencies]` to `[dependencies]`.

## Error handling

- Election errors (coordinator unavailable, rebalance, connection loss) → back-off + retry; the node stays secondary with `primary_url = None` until an assignment arrives (mutating requests return `503`).
- Forward failures (primary unreachable / mid-failover) → `502`/`503` (client retries); optionally one re-resolve+retry after the watch updates.
- No write fencing of `_schemas` by generation (cp doesn't either); the single-master assignment + fast group convergence keep the multi-writer window small — documented as a known limitation.

## Validation

- **`capture_election_fixtures.rs`** (`#[ignore]`, Docker): start **two** real `cp-schema-registry:7.4.0` nodes both pointed at the Crabka broker with the same `group.id`; wait for them to form the `"sr"` group and elect a primary; then `DescribeGroups("sr"-group)` (admin API) to read each member's **metadata** + **assignment** bytes → `tests/fixtures/election/*.json`. Assert (a) our `SchemaRegistryIdentity`/`SchemaRegistryGroupAssignment` encoders reproduce cp's bytes byte-exactly, (b) cp's two nodes successfully elect exactly one primary **through the Crabka coordinator** (proving the broker runs the `"sr"` group), and (c) the captured `protocol_type`/protocol-name/selection-rule. (If `DescribeGroups` doesn't surface assignment bytes, fall back to a broker-side coordinator log hook.)
- **`ha.rs`** (in-process, Mac-friendly, no Docker): boot one in-process broker; start **2–3** registry nodes (each a `KafkaStore` + router + election task) on distinct `127.0.0.1` ports. Assert: exactly one primary elected; `POST` to a **secondary** forwards to the primary and the write lands (a subsequent GET reflects it on every node); reads served on all; **failover** — stop the primary's election task → a new primary is elected → writes resume on the new primary (and a secondary's forward now targets it).
- **Election unit tests**: `SchemaRegistryIdentity`/assignment serde round-trip + byte-shape vs the captured cp bytes; the leader primary-selection rule (deterministic master among eligible members); the forwarding middleware's primary/secondary/loop-guard branches.

## File structure / sequencing

`src/election/{mod,protocol,client}.rs` (new), `src/rest/forward.rs` (new) + `src/rest/mod.rs` (wire the layer), `src/config.rs` (+3 fields), `src/bin/schema-registry.rs` (CLI + start election + wrap router), `Cargo.toml` (`reqwest` → deps), `tests/{capture_election_fixtures,ha}.rs`, `tests/fixtures/election/`.

**Implementation batches:** (1) config fields + the `"sr"` protocol types (`SchemaRegistryIdentity`/`SchemaRegistryGroupAssignment` serde + the leader selection rule) + unit tests. (2) the group-membership client (`FindCoordinator`→`JoinGroup`→`SyncGroup`→`Heartbeat`→`LeaveGroup` loop) + the `Election` task + `PrimaryState` watch. (3) the forwarding middleware + binary wiring + the `reqwest` dep + the in-process `ha.rs` multi-node + failover tests. (4) the cp Docker election capture + byte/selection-rule calibration against cp 7.4.0.

## Out of scope

- SR↔broker authentication for the election connection (SASL/TLS) — slice 6 (security).
- HTTPS / mTLS request forwarding (the `grpc-gateway` `Forwarder` shows the pattern; add later).
- More than one group / schema "contexts"; per-request leader stickiness beyond the current primary.
- Generation-fencing of `_schemas` writes (cp doesn't fence either; the brief rebalance multi-writer window is documented).
- Operator/CRD packaging — slice 7.

## Risks

1. **cp's exact `SchemaRegistryIdentity` field set + protocol name + primary-selection rule** — the core fidelity risk; mitigated by the Docker `DescribeGroups` capture (cp is authority on any disagreement).
2. **The election rejoin/heartbeat timing under failover** — a stalled rebalance vs a client bug is hard to distinguish; mitigated by modeling `client-consumer`'s proven loop and a deterministic in-process failover test.
3. **Forward-loop / stale-primary races** — mitigated by the `X-Forwarded-For-Registry` header + the `is_primary` recheck on forwarded requests.
4. **`DescribeGroups` assignment visibility** — if the broker's `DescribeGroups` doesn't return assignment bytes, the capture falls back to a coordinator-side log hook (documented).
5. **Split-brain window** during rebalance (two nodes briefly both writing) — inherent to group election; cp has the same; documented as a known limitation rather than engineered away in this slice.

## Dependencies

No new external crates beyond promoting `reqwest` (already a dev-dep + a prod dep elsewhere in the workspace) to `[dependencies]`. Reuses `crabka_client_core::Client` (generic `send`), the `crabka_protocol::owned` group-membership codecs, the broker's protocol-generic coordinator, and `axum`/`watch`. The Docker capture uses the existing `testcontainers` + `cp-schema-registry:7.4.0` setup.
