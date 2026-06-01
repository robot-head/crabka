# KIP-932 Share Groups — Slice C (Share-Partition Leader + ShareFetch/ShareAcknowledge) Design

**Date:** 2026-05-31
**Status:** Approved (design). Builds on Slice A (membership) + Slice B (persister).
**KIP:** [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka).
**Target:** Apache Kafka 4.3.0.
**Scope choice:** Full acquisition semantics in one slice (locks + all ack types + delivery-attempt-limit). `read_committed` and the `RENEW` ack type are deferred to Slice F.

## Goal

The **share-partition leader**: an in-memory per-`(group, topicId, partition)` acquisition
state machine, co-located with the topic-partition log leader, that materializes records
from the log, hands them out under time-limited acquisition locks, applies
acknowledgements (`Accept`/`Release`/`Reject`/`Gap`), advances the Share-Partition Start
Offset (SPSO), archives poison pills past the delivery-attempt limit, and persists durable
state to the Slice-B share coordinator. Driven by `ShareFetch(78)` (acquire + piggybacked
acks + long-poll) and `ShareAcknowledge(79)` (acks only). End result: a consumer can
`poll → acknowledge` records with real queue semantics (redelivery, locks, poison-pill
handling), surviving broker restart.

## Background (reused infrastructure — verbatim from research)

- **Fetch path** (`crates/broker/src/handlers/fetch.rs`): `handle(broker, version, corr, req_bytes, ctx)` (inline-intercepted with Read ACL); `do_read()` reads via `Partition::read_log`/`log.read_raw` in `spawn_blocking`, returns raw verbatim batch bytes wrapped as `RecordsPayload::Raw`; visibility clamps at HWM (read_uncommitted). Long-poll: `long_poll_then_reread(broker, pending, max_wait_ms)` parks on each partition's `append_notify` (`Arc<Notify>`) with `tokio::time::timeout`, re-reads on wake. **ShareFetch reuses this read + long-poll machinery.**
- **Dispatch** (`crates/broker/src/network/dispatch.rs`): serial per connection; long-poll Fetch head-of-line-blocks the socket — **expected/correct for ShareFetch too** (it's a long-poll by design). Fetch(1) is an inline `intercept!` arm with ACL + `RequestContext { principal, client_id, peer }`. New RPCs need `handler_body_flexible` arms or the typed client mis-parses them (the Slice-B lesson).
- **Partition** (`crates/broker/src/partition.rs`): `read_log(offset, max_bytes)->Result<ReadOutput>`, `high_watermark()`, `log_start_offset()`, `append_notify`. Leadership: the broker leads a topic partition per the metadata image (`partition_leader_for_test`/`image.partition(topic,p).leader == node_id`).
- **Slice-B persister** (`crates/broker/src/share_coordinator/`): `ShareCoordinator::{write(group,topic,part,state_epoch,leader_epoch,start_offset,delivery_complete_count,batches), read(..)->Option<SharePartitionState>, read_summary(..)}`. `SharePersister` has `initialize`/`delete` routing local-or-remote; **Slice C adds `read_state`/`write_state`** mirroring that routing. `StateBatch { first_offset, last_offset, delivery_state: i8, delivery_count: i16 }`.
- **Session-tick pattern** (`coordinator/unified/share/actor.rs`): `tokio::time::interval` + eviction sweep — template for the lock-timeout sweep.
- **ShareGroupConfig** (`coordinator/unified/share/config.rs`): has `enable`, session/heartbeat timeouts, `max_groups`, `max_size`. **Slice C adds** `record_lock_duration` (30s), `max_delivery_attempts` (5), `max_inflight_records`.
- **Wire shapes** (generated, present): `ShareFetchRequest { group_id, member_id, share_session_epoch: i32, max_wait_ms, min_bytes, max_bytes, max_records, batch_size, share_acquire_mode: i8, is_renew_ack: bool, topics: [{topic_id, partitions: [{partition_index, partition_max_bytes, acknowledgement_batches: [{first_offset, last_offset, acknowledge_types: [i8]}]}]}], forgotten_topics_data }`. `ShareFetchResponse { throttle_time_ms, error_code, error_message, acquisition_lock_timeout_ms, responses: [{topic_id, partitions: [{partition_index, error_code, error_message, acknowledge_error_code, acknowledge_error_message, current_leader{leader_id,leader_epoch}, records, acquired_records: [{first_offset, last_offset, delivery_count: i16}]}]}], node_endpoints }`. `ShareAcknowledge{Request,Response}` analogous (ack-only). Both apiKey 78/79, v1-2, flexible 0+. Error codes (all added in Slice A): `INVALID_RECORD_STATE=121`, `SHARE_SESSION_NOT_FOUND=122`, `INVALID_SHARE_SESSION_EPOCH=123`, `SHARE_SESSION_LIMIT_REACHED=133`.

## Non-goals (Slice C)

- `read_committed` isolation (Slice C clamps at HWM, read_uncommitted) — Slice F.
- `RENEW` ack type (KIP-1222) — Slice F. Slice C handles `Gap(0)`/`Accept(1)`/`Release(2)`/`Reject(3)`.
- Native share consumer client — Slice E. Tests drive raw `ShareFetch`/`ShareAcknowledge` via the typed client.
- Cross-broker share-fetch routing nuances beyond returning `current_leader`/`NodeEndpoints`; single-broker is the tested path.

---

## The acquisition state machine (`SharePartitionState`) — the core (C1)

In-memory, per `(group, topicId, partition)`. Models in-flight records as a **sorted list of
offset-range batches**:

```rust
enum RecordState { Available, Acquired, Acknowledged, Archived }

struct InFlightBatch {
    first_offset: i64,
    last_offset: i64,          // inclusive
    state: RecordState,
    delivery_count: i16,
    acquired_by: Option<String>,   // member id holding the lock (Acquired only)
    lock_deadline: Option<Instant>,// Acquired only
}

struct SharePartitionState {
    start_offset: i64,   // SPSO — durable queue head
    end_offset: i64,     // SPEO — highest offset+1 materialized into in-flight
    state_epoch: i32,
    leader_epoch: i32,
    batches: Vec<InFlightBatch>, // sorted, covering [start_offset, end_offset)
    dirty: bool,         // needs persist
}
```

**Wire `delivery_state` mapping** (for persistence/Read): `0=Available`, `1=Acquired`,
`2=Acknowledged`, `4=Archived` (Kafka's codes, skipping 3; use the same i8 codes Slice B's `StateBatch` carries —
confirm against `ShareCoordinator`/Kafka; `Acquired` is **transient**: on persist it is
written as `Available` with its delivery count, and on load `Acquired`→`Available`).

**Operations (pure, unit-tested):**

- `acquire(member, max_records, max_bytes, now, lock_duration, max_attempts) -> Vec<AcquiredRange{first,last,delivery_count}>`:
  walk `batches` from `start_offset`; for each `Available` batch (delivery_count < max_attempts), mark `Acquired` (split if it exceeds max_records), set `acquired_by=member`, `lock_deadline=now+lock_duration`, `delivery_count+=1`, collect the range. Batches with `delivery_count >= max_attempts` → `Archived` (poison pill), then `advance_spso`. Stop at max_records/max_bytes. Returns the acquired ranges (the caller reads those offsets' bytes from the log).
- `materialize(up_to_hwm, now)`: if no `Available` records remain and `end_offset < hwm`, append a new `Available` batch `[end_offset, min(hwm-1, end_offset+max_inflight-1)]`, advance `end_offset`. (Called by the handler before `acquire` when the in-flight window is drained.)
- `acknowledge(member, first, last, ack_type, now) -> Result<(), i16>`:
  the range must currently be `Acquired` by `member` (else `INVALID_RECORD_STATE=121` — covers lock-expired/never-acquired). Split batches at `first`/`last` boundaries. Set: `Accept`→`Acknowledged`, `Release`→`Available` (clear lock/acquired_by; **retain** delivery_count), `Reject`→`Archived`, `Gap`→`Archived`. Then `advance_spso`. Mark `dirty`.
- `expire_locks(now)`: any `Acquired` batch past `lock_deadline` → `Available` (clear lock; retain delivery_count). Mark `dirty` if any.
- `advance_spso()`: while the batch at `start_offset` is `Acknowledged` or `Archived`, advance `start_offset` past it and drop it. Coalesce adjacent same-state batches to keep the list small.
- `to_persist_batches() -> (start_offset, delivery_complete_count, Vec<StateBatch>)`: emit batches in `[start_offset, end_offset)` with `Acquired` mapped to `Available`; `delivery_complete_count` = count of acknowledged+archived since init (KIP-1226 lag metric).
- `load_from(start_offset, state_epoch, leader_epoch, persisted_batches)`: set SPSO, rebuild `batches` (Acquired→Available), `end_offset = max(last_offset)+1` or `start_offset`.

**Invariants:** batches are sorted, contiguous-or-gapless within `[start_offset, end_offset)`; `start_offset <= end_offset`; an `Acquired` batch always has `acquired_by` + `lock_deadline`.

---

## Components

### C2 — `SharePartitionLeaderManager` + persister read/write

- `crates/broker/src/share_partition/manager.rs`: `SharePartitionLeaderManager { partitions: DashMap<(String, Uuid, i32), Arc<Mutex<SharePartitionState>>>, persister: Arc<SharePersister>, config, metadata, ... }` held as `Arc` on `Broker` (sibling to `share_coordinator`).
  - `get_or_load(group, topic_id, partition) -> Arc<Mutex<SharePartitionState>>`: on first use, `persister.read_state(..)` to load SPSO + batches (or initialize empty at the auto-offset-reset); cache it.
  - `is_leader(topic, partition)`: this broker leads the *topic* partition (metadata image). ShareFetch/Acknowledge for a non-led partition → `NOT_LEADER_OR_FOLLOWER` + `current_leader`.
  - `persist_if_dirty(group, topic_id, partition)`: `persister.write_state(group, topic_id, partition, state_epoch, leader_epoch, start_offset, delivery_complete_count, batches)`; clear `dirty`.
- `SharePersister::read_state(group, topic_id, partition) -> Result<Option<LoadedShareState>, BrokerError>` and `write_state(group, topic_id, partition, state_epoch, leader_epoch, start_offset, delivery_complete_count, batches) -> Result<(), BrokerError>` — route local (`ShareCoordinator::{read,write}`) or remote (`ReadShareGroupStateRequest`/`WriteShareGroupStateRequest` via `inter_broker_client`), mirroring `initialize`/`delete`.

### C3 — handlers + session cache + wiring

- `crates/broker/src/handlers/share_fetch.rs` (78) and `share_acknowledge.rs` (79): inline-intercepted in `network/dispatch.rs` (need `RequestContext` for Read ACL on each topic, like Fetch). Add `handler_body_flexible` arms (78/79 flexible from v0). Advertise `v!(share_fetch_request)` / `v!(share_acknowledge_request)` in `api_catalog.rs`.
- **ShareFetch flow:** validate share session (below); membership check (member belongs to the share group — consult local share group coordinator; else `UNKNOWN_MEMBER_ID`); for each requested partition: Read ACL; if piggybacked `acknowledgement_batches` present, apply them (like ShareAcknowledge) and set `acknowledge_error_code`; then `manager.get_or_load`, `expire_locks(now)`, `materialize(hwm)`, `acquire(...)`; read the acquired ranges' bytes from the log (`do_read`-style); build `PartitionData { records, acquired_records, .. }`; `persist_if_dirty`. If zero acquired across all partitions and `max_wait_ms>0`, long-poll on `append_notify` then retry once. Response carries `acquisition_lock_timeout_ms = record_lock_duration`.
- **ShareAcknowledge flow:** validate session; for each (first,last,types) apply `acknowledge`; advance SPSO; `persist_if_dirty`; per-partition `error_code`.
- **Share session cache** (`crates/broker/src/share_partition/session.rs`): `DashMap<(group, member), ShareSession { epoch: i32, partitions: HashSet<(Uuid,i32)> }>`. `share_session_epoch == 0` opens a session (epoch→1); `-1` closes (drop); else must equal the stored epoch (`INVALID_SHARE_SESSION_EPOCH=123`); unknown non-zero → `SHARE_SESSION_NOT_FOUND=122`. Bounded by a max (→ `SHARE_SESSION_LIMIT_REACHED=133`). (Kafka identifies the share session by `(group, member)`; there is no separate session-id field.)

### C4 — acquisition-lock-timeout sweep

A background `tokio::time::interval` task on the manager (tick ≈ `record_lock_duration / 2`, bounded): iterate cached partitions, `expire_locks(now)`, and `persist_if_dirty` for any that changed (so a released-by-timeout record becomes re-acquirable and durably reflected). Mirrors the share actor's session-tick. (Lock expiry is also applied opportunistically at the start of each ShareFetch, so the sweep is a backstop ensuring redelivery even without further fetches.)

### C5 — integration tests (`tests/share_consume.rs`)

Drive the typed client; produce via `ProduceRequest`. Cases (real assertions):
1. **consume + accept + restart:** produce N records; ShareFetch (session epoch 0) acquires them, returns `records` + `acquired_records` (delivery_count 1); ShareAcknowledge `Accept` all; a follow-up ShareFetch returns nothing (SPSO advanced past them); restart (Rejoin) → SPSO recovered (re-fetch returns nothing).
2. **release → redelivery:** acquire, `Release`; next ShareFetch re-acquires the same offsets with `delivery_count == 2`.
3. **reject → archived:** acquire, `Reject`; SPSO advances past them; never redelivered.
4. **lock timeout → redelivery:** acquire, do not ack; after `record_lock_duration` (use a tiny test config), the records are re-acquirable with incremented delivery_count.
5. **delivery-attempt-limit → poison pill:** with `max_delivery_attempts=2` (test config), repeatedly acquire+lock-expire; after the limit the record is Archived and SPSO advances past it (not redelivered).
6. **session epoch validation:** epoch -1 closes; a stale/unknown epoch → `INVALID_SHARE_SESSION_EPOCH`/`SHARE_SESSION_NOT_FOUND`.

---

## Persistence cadence

- **Load:** `manager.get_or_load` calls `read_state` once per (group,topic,partition) on first use after (re)assignment; `Acquired` loaded as `Available`.
- **Write:** after any acknowledgement or lock-expiry that changes durable state (SPSO advance, Acknowledged/Archived/Available transitions), `write_state` with the current SPSO + persist-batches. Acquire alone (which only sets transient `Acquired` + delivery count) is persisted too if delivery counts must survive — Kafka persists delivery counts; so persist after acquire as well (batched/debounced is fine, but Slice C persists eagerly for simplicity).

## Error handling

- Not the topic-partition leader → per-partition `NOT_LEADER_OR_FOLLOWER` + `current_leader`.
- Share feature disabled → `UNSUPPORTED_VERSION` (top-level).
- Bad share session epoch → `INVALID_SHARE_SESSION_EPOCH` / `SHARE_SESSION_NOT_FOUND`; cache full → `SHARE_SESSION_LIMIT_REACHED`.
- Ack on a non-Acquired/expired range → per-partition `acknowledge_error_code = INVALID_RECORD_STATE`.
- Unknown member of the share group → `UNKNOWN_MEMBER_ID`.
- Persister failure on write → log; keep in-memory state authoritative; retry on next op (do not fail the fetch/ack response — return the records; durability catches up). On load failure → `COORDINATOR_NOT_AVAILABLE`-style retry or treat as empty (document the choice).

## Concurrency

Per-share-partition `Arc<Mutex<SharePartitionState>>` (tokio mutex). The mutex guard MAY be held across the persister `.await` (tokio mutex is async-aware) but MUST NOT be held across the log `do_read`/`spawn_blocking` in a way that blocks other partitions — acquire ranges under the lock, then read bytes after computing ranges (or read under the lock if simplest; document). Never hold a `DashMap` guard across `.await` (the Slice-B lesson): clone the `Arc<Mutex<..>>` out first.

## Acceptance gate (Slice C)

1. `cargo fmt --check` clean. 2. `cargo clippy --workspace --all-targets -- -D warnings` clean (the CI command, not `--lib`). 3. `cargo test --workspace` green. 4. No codegen drift. 5. ShareFetch/ShareAcknowledge advertised (78/79) + flexible arms. 6. All six `tests/share_consume.rs` cases green (consume+accept+restart, release-redelivery, reject-archive, lock-timeout-redelivery, delivery-limit poison pill, session-epoch validation). 7. SPSO persists/recovers across restart via the Slice-B persister.

## File-set sketch (batching)

- **C1 (leaf, pure):** `crates/broker/src/share_partition/state.rs` (`SharePartitionState` + ops) — heavy unit tests, no I/O. `ShareGroupConfig` field additions (`config.rs` + `BrokerConfig` defaults).
- **C2:** `share_partition/manager.rs` + `SharePersister::{read_state,write_state}` (+ `share_partition/mod.rs`, `pub mod share_partition;`, `Broker` field + construction).
- **C3:** `handlers/share_fetch.rs`, `handlers/share_acknowledge.rs`, `share_partition/session.rs`, dispatch arms (78/79) + `handler_body_flexible` + `api_catalog` advertise.
- **C4:** lock-timeout sweep task (manager) + wiring into `Broker::start`.
- **C5:** `tests/share_consume.rs`.

`network/dispatch.rs`, `api_catalog.rs`, `broker.rs`, `config.rs`, and `share/config.rs`
are shared-file edits — sequence those; the `share_partition/*` modules are new/parallel-safe.
