# KIP-932 Share Groups — Slice C (Share-Partition Leader + ShareFetch/ShareAcknowledge) Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes.

**Goal:** A share-partition leader with an in-memory acquisition state machine driving `ShareFetch(78)`/`ShareAcknowledge(79)` end-to-end: acquire under locks, ack (Accept/Release/Reject/Gap), advance SPSO, archive poison pills, persist to the Slice-B coordinator, survive restart.

**Spec:** `docs/superpowers/specs/2026-05-31-crabka-kip-932-share-groups-slice-c-design.md`

**Naming (avoid clash):** Slice B already defines `SharePartitionState` (the *persisted* type, `share_coordinator/state.rs`). Slice C's in-memory machine is **`AcquisitionState`** in a NEW `crates/broker/src/share_partition/` module. Manager = `SharePartitionLeaderManager`.

---

## Established facts (verbatim from research)

- **Fetch long-poll** (`handlers/fetch.rs`): `long_poll_then_reread` collects `part.append_notify.clone()` (+ `part.hw_advance_notify.clone()` for consumers) into `Vec<Arc<Notify>>`, boxes each `n.notified().await` as `WaitFut = Pin<Box<dyn Future<Output=()>+Send>>`, then `tokio::time::timeout(Duration::from_millis(max_wait), futures_util::future::select_all(waits)).await`. It is Fetch-typed — **write a parallel ShareFetch version** reusing this pattern. Byte read: lock `part.log`, `log.read_raw(fetch_offset, limit_offset, read_max)` inside `tokio::task::spawn_blocking`; result `crabka_log::RawRead { bytes, total }`; wrap `out.records = Some(RecordsPayload::Raw(raw.bytes))`.
- **Partition** (`partition.rs`): `read_log(offset, max_bytes)->Result<ReadOutput>` (sync, decoded); NO `read_raw` on Partition (use `Log::read_raw` via `part.log.lock()` in spawn_blocking); `high_watermark()->i64` **async**; `log_start_offset()/log_end_offset()/lso()->i64` sync; `pub append_notify: Arc<Notify>`, `pub hw_advance_notify: Arc<Notify>`. NO `is_leader` — compare `controller.current_image().partition(topic,p).leader` to `config.node_id` (as `ShareCoordinator::refresh_leader_partitions` does).
- **Dispatch** (`network/dispatch.rs`): `intercept!($call, $label)` macro; `handle_fetch_frame(broker,&frame,&auth,&peer)` parses header → `handler_body_flexible(api_key,version)` → `principal_or_anonymous(auth)` + `peek_client_id(frame)` → `RequestContext { principal, peer, client_id }` → `handlers::fetch::handle(broker,version,corr,body,&ctx)` → `encode_response(api_key,corr,body_flexible,&resp)`. Dispatch arm `Some(1) => intercept!(handle_fetch_frame(...), "Fetch")`; share precedent `Some(77) => intercept!(handle_share_group_describe_frame(...), "ShareGroupDescribe")`. `handler_body_flexible` tail has `83..=87` arms then `_ => false`.
- **RequestContext** (`handlers/context.rs`): `pub(crate) struct RequestContext<'a> { pub principal: &'a Principal, pub peer: &'a SocketAddr, pub client_id: &'a str }`.
- **Slice-B persister** (`share_coordinator/`): `ShareCoordinator::read(group,topic_id:uuid::Uuid,partition)->Option<SharePartitionState>`; `write(group,topic_id,partition,state_epoch,leader_epoch,start_offset,delivery_complete_count,batches:Vec<StateBatch>)->Result<(),i16>`; `read_summary(..)->Option<(i32,i32,i64,i32)>`. `SharePartitionState { state_epoch, leader_epoch, start_offset, delivery_complete_count, state_batches: Vec<StateBatch>, snapshot_epoch, last_snapshot_offset, updates_since_snapshot }`. `StateBatch { first_offset:i64, last_offset:i64, delivery_state:i8, delivery_count:i16 }` (bare i8, NO enum). `SharePersister { node_id, share_coordinator, controller, inter_broker_client, inter_broker_listener_protocol, inter_broker_listener_name }` with `initialize`/`delete` (routing: `state_partition_for` → `is_leader(sp).await` → local `share_coordinator.X` else build typed req + `send_to_leader(sp, req)`; `send_to_leader` discards the response via `let _resp`).
- **ShareGroupConfig** (`coordinator/unified/share/config.rs`): fields enable, session/heartbeat timeouts, max_groups, max_size. Reached via `BrokerConfig.share_group: Box<ShareGroupConfig>` (`config.rs:325`); cloned into `GroupCoordinator` at `broker.rs:1354`.
- **Broker** (`broker.rs`): fields `controller: Arc<dyn MetadataSource>`, `partitions: Arc<PartitionRegistry>`, `share_coordinator: Arc<ShareCoordinator>` (line 52). `start` constructs `share_coordinator` (~1390) then `SharePersister` (~1408) which is moved into `coord.set_share_persister(share_persister)`. Read it back via `coord.share_persister() -> Option<&Arc<SharePersister>>` (`unified/mod.rs:103`), OR clone the Arc before passing to `set_share_persister`. Struct literal at ~2195.
- **Membership** (`unified/mod.rs`): `GroupCoordinator::find_share(group)->Option<Arc<ShareGroupActorHandle>>`; actor `ShareGroupActorMessage::Describe { reply: oneshot::Sender<ShareDescribeView> }`; `ShareDescribeView.members: Vec<ShareDescribeMember>` with `pub member_id: String`. (Best-effort membership check; no dedicated accessor.)
- **Generated types** (import `crabka_protocol::owned::share_fetch_request` etc.; `Uuid=crabka_protocol::primitives::uuid::Uuid`; all four MIN=1 MAX=2 FLEXIBLE_MIN=0): `ShareFetchRequest { group_id: Option<String>, member_id: Option<String>, share_session_epoch:i32, max_wait_ms, min_bytes, max_bytes, max_records, batch_size, share_acquire_mode:i8, is_renew_ack:bool, topics: Vec<FetchTopic{topic_id, partitions: Vec<FetchPartition{partition_index, partition_max_bytes, acknowledgement_batches: Vec<AcknowledgementBatch{first_offset, last_offset, acknowledge_types: Vec<i8>}>}>}>, forgotten_topics_data }`. `ShareFetchResponse { throttle_time_ms, error_code, error_message, acquisition_lock_timeout_ms, responses: Vec<ShareFetchableTopicResponse{topic_id, partitions: Vec<PartitionData{partition_index, error_code, error_message, acknowledge_error_code, acknowledge_error_message, current_leader: LeaderIdAndEpoch{leader_id,leader_epoch}, records: Option<crate::records::RecordsPayload>, acquired_records: Vec<AcquiredRecords{first_offset,last_offset,delivery_count:i16}>}>}>, node_endpoints }`. `ShareAcknowledgeRequest { group_id, member_id, share_session_epoch, is_renew_ack, topics: Vec<AcknowledgeTopic{topic_id, partitions: Vec<AcknowledgePartition{partition_index, acknowledgement_batches: Vec<AcknowledgementBatch>}>}> }`; `ShareAcknowledgeResponse { ..., responses: Vec<ShareAcknowledgeTopicResponse{topic_id, partitions: Vec<PartitionData{partition_index, error_code, error_message, current_leader}>}> }` (no records).
- **Tests** (`tests/share_groups.rs`): `boot()/connect()/create_topic()`. No produce helper — copy from `tests/acl_handlers.rs:594` (`ProduceRequest { acks:-1, timeout_ms, topic_data: Vec<TopicProduceData{name, partition_data: Vec<PartitionProduceData{index, records: Some(RecordBatch{...}.into())}>}> }`). Config override before start: `let mut cfg = BrokerConfig::for_tests(dir); cfg.share_group.record_lock_duration = Duration::from_millis(200); cfg.share_group.max_delivery_attempts = 2;`.
- **delivery_state codes (define in share_partition):** `DS_AVAILABLE=0, DS_ACQUIRED=1, DS_ACKNOWLEDGED=2, DS_ARCHIVED=4` (Kafka values). `Acquired` is transient — persist as `Available(0)`.

## Batching (sequential dispatch, git-safe; full `--all-targets` clippy gate)
- **C-α:** Tasks 1 (config) + 2 (`AcquisitionState` machine — the pure core).
- **C-core:** Tasks 3 (`SharePartitionLeaderManager` + `SharePersister::read_state/write_state` + Broker wiring) + 6 (lock-timeout sweep).
- **C-handlers:** Tasks 4 (ShareFetch) + 5 (ShareAcknowledge) + session cache + dispatch/flexible/api_catalog.
- **C-tests:** Task 7.

Every implementer: worktree-only, `git -C <worktree>`, assert branch `claude/intelligent-bouman-224792`, identity overrides, `cargo fmt --all` pre-commit, verify `cargo clippy --workspace --all-targets -- -D warnings`.

---

## Task 1: `ShareGroupConfig` additions

**Files:** `crates/broker/src/coordinator/unified/share/config.rs` (+ the `for_tests`/default sites already default `share_group` via `ShareGroupConfig::default()`, so only the struct+Default change here).

- [ ] **Step 1: failing test** in config.rs:
```rust
#[test]
fn slice_c_defaults() {
    let c = ShareGroupConfig::default();
    assert!(c.record_lock_duration == std::time::Duration::from_secs(30));
    assert!(c.max_delivery_attempts == 5);
    assert!(c.max_inflight_records == 200);
}
```
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** add fields `pub record_lock_duration: Duration`, `pub max_delivery_attempts: i16`, `pub max_inflight_records: i32` and Default values `Duration::from_secs(30)`, `5`, `200`.
- [ ] **Step 4:** test passes; `cargo build -p crabka-broker`.
- [ ] **Step 5:** commit `feat(kip-932): ShareGroupConfig record-lock/delivery-limit/inflight fields`.

## Task 2: `AcquisitionState` machine (the core)

**Files:** Create `crates/broker/src/share_partition/mod.rs` (`pub mod state;` + later modules) + `crates/broker/src/share_partition/state.rs`; add `pub mod share_partition;` next to `pub mod share_coordinator;` in `lib.rs`.

Implement exactly the spec's state machine. Pure, no I/O, exhaustively tested.

- [ ] **Step 1: write failing tests** (cover: acquire from empty+materialize, acquire sets delivery_count 1 + lock deadline, partial acknowledge splits a batch, Accept advances SPSO, Release → re-Available retains delivery_count, Reject → Archived + SPSO advances past it, Gap → Archived, expire_locks reverts Acquired→Available, delivery-attempt-limit archives, to_persist_batches maps Acquired→Available, load_from rebuilds). Example:
```rust
#[test]
fn acquire_then_accept_advances_spso() {
    let mut s = AcquisitionState::new(0);
    s.materialize(5 /*hwm*/, 100 /*max_inflight*/);     // [0,4] Available
    let acq = s.acquire("m1", 10, i32::MAX, t0(), Duration::from_secs(30), 5);
    assert!(acq == vec![AcquiredRange { first: 0, last: 4, delivery_count: 1 }]);
    s.acknowledge("m1", 0, 4, AckType::Accept, t0()).unwrap();
    assert!(s.start_offset == 5);
}
#[test]
fn release_redelivers_with_incremented_count() {
    let mut s = AcquisitionState::new(0);
    s.materialize(3, 100);
    let _ = s.acquire("m1", 10, i32::MAX, t0(), Duration::from_secs(30), 5);
    s.acknowledge("m1", 0, 2, AckType::Release, t0()).unwrap();
    let acq2 = s.acquire("m1", 10, i32::MAX, t0(), Duration::from_secs(30), 5);
    assert!(acq2[0].delivery_count == 2);
}
#[test]
fn delivery_limit_archives_poison_pill() {
    let mut s = AcquisitionState::new(0);
    s.materialize(1, 100);
    for _ in 0..2 { // max_attempts=2
        let _ = s.acquire("m1", 10, i32::MAX, t0(), Duration::from_secs(30), 2);
        s.expire_locks(t0() + Duration::from_secs(31));
    }
    let acq = s.acquire("m1", 10, i32::MAX, t0()+Duration::from_secs(62), Duration::from_secs(30), 2);
    assert!(acq.is_empty());          // archived, not redelivered
    assert!(s.start_offset == 1);     // SPSO advanced past the poison pill
}
```
(Provide `fn t0() -> Instant` via `Instant::now()`; avoid `Instant - Duration` per [[feedback-instant-checked-sub-ci]] — add to a base now.)
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** implement per the spec:
```rust
use std::time::{Duration, Instant};
use crate::share_coordinator::persistence::StateBatch;

pub const DS_AVAILABLE: i8 = 0;
pub const DS_ACQUIRED: i8 = 1;
pub const DS_ACKNOWLEDGED: i8 = 2;
pub const DS_ARCHIVED: i8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordState { Available, Acquired, Acknowledged, Archived }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckType { Gap, Accept, Release, Reject }
impl AckType {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v { 0 => Some(Self::Gap), 1 => Some(Self::Accept),
                  2 => Some(Self::Release), 3 => Some(Self::Reject), _ => None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredRange { pub first: i64, pub last: i64, pub delivery_count: i16 }

#[derive(Debug, Clone)]
struct InFlightBatch {
    first_offset: i64, last_offset: i64, state: RecordState,
    delivery_count: i16, acquired_by: Option<String>, lock_deadline: Option<Instant>,
}

#[derive(Debug)]
pub struct AcquisitionState {
    pub start_offset: i64,  // SPSO
    pub end_offset: i64,    // SPEO
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub dirty: bool,
    batches: Vec<InFlightBatch>,
}
```
Methods: `new(start_offset)`, `materialize(hwm, max_inflight)`, `acquire(member,max_records,max_bytes,now,lock_dur,max_attempts)->Vec<AcquiredRange>`, `acknowledge(member,first,last,AckType,now)->Result<(),i16>` (returns `codes::INVALID_RECORD_STATE` when the range isn't Acquired-by-member), `expire_locks(now)`, `advance_spso()` (private, called after ack/archive), `to_persist_batches()->(i64 start, i32 dcc, Vec<StateBatch>)`, `load_from(start_offset, state_epoch, leader_epoch, &[StateBatch])`. Implement batch splitting at acknowledge boundaries; coalesce same-state neighbors in `advance_spso`. `max_bytes` may be approximated (count records; bytes are enforced at the handler's read step) — document.
- [ ] **Step 4:** all tests pass.
- [ ] **Step 5:** commit `feat(kip-932): share-partition acquisition state machine`.

## Task 3: `SharePartitionLeaderManager` + persister read/write + Broker wiring

**Files:** `crates/broker/src/share_partition/manager.rs`; `crates/broker/src/share_coordinator/persister_client.rs` (add `read_state`/`write_state` + a response-returning `send_to_leader_resp`); `crates/broker/src/broker.rs` (field + construct + literal).

- [ ] **Step 1:** add to `SharePersister`:
```rust
pub(crate) async fn read_state(&self, group: &str, topic_id: uuid::Uuid, partition: i32)
    -> Result<Option<crate::share_coordinator::state::SharePartitionState>, BrokerError>;
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_state(&self, group: &str, topic_id: uuid::Uuid, partition: i32,
    state_epoch: i32, leader_epoch: i32, start_offset: i64, delivery_complete_count: i32,
    batches: Vec<StateBatch>) -> Result<(), BrokerError>;
```
`write_state` mirrors `initialize` (local `share_coordinator.write(...)` else `WriteShareGroupStateRequest` via `send_to_leader`). `read_state` routes local `share_coordinator.read(...)` else builds `ReadShareGroupStateRequest` and uses a NEW `send_to_leader_resp` that returns `conn.send(req).await` (a copy of `send_to_leader` that returns the typed response instead of `let _resp`); map the response's per-partition result into `Option<SharePartitionState>`.
- [ ] **Step 2:** failing test for the manager (unit): `get_or_load` on a fresh (group,topic,part) loads empty state (SPSO 0) when the persister has none; `persist_if_dirty` is a no-op when not dirty. Construct a `SharePartitionLeaderManager` with a `SharePersister` over a local `ShareCoordinator` (mirror how `share_coordinator` unit tests build their deps; the persister/coordinator need a `PartitionRegistry` + controller — if too heavy for a unit test, cover `get_or_load`/`persist_if_dirty` via the Task 7 integration tests and keep only a pure construction/smoke test here).
- [ ] **Step 3:** implement `manager.rs`:
```rust
pub(crate) struct SharePartitionLeaderManager {
    node_id: NodeId,
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn MetadataSource>,
    persister: Arc<SharePersister>,
    config: Arc<ShareGroupConfig>,
    leaders: DashMap<(String, uuid::Uuid, i32), Arc<tokio::sync::Mutex<AcquisitionState>>>,
}
```
Methods: `new(...)`; `topic_leader_is_self(topic, partition)->bool` (image compare); `async get_or_load(group, topic_id: uuid::Uuid, partition) -> Arc<Mutex<AcquisitionState>>` (clone Arc out of DashMap — NEVER hold the DashMap guard across the `persister.read_state(..).await`; load on miss: read_state → `AcquisitionState::load_from(...)` or `AcquisitionState::new(0)`); `async persist_if_dirty(group, topic_id, partition, st: &mut AcquisitionState)` (if `st.dirty`: `to_persist_batches()` → `persister.write_state(...)`; clear dirty; log+swallow errors). `state_epoch`: reuse the loaded state_epoch (or 0); `leader_epoch`: from the topic partition's `current_leader_epoch`.
- [ ] **Step 4:** wire into `Broker`: add `pub(crate) share_partition_leaders: Arc<SharePartitionLeaderManager>`; in `start`, after the `SharePersister` is built, `let share_persister = Arc::new(...)`, pass `share_persister.clone()` to `set_share_persister` and another clone to `SharePartitionLeaderManager::new(config.node_id, partitions.clone(), controller.clone(), share_persister.clone(), Arc::new((*config.share_group).clone()))`; add `share_partition_leaders,` to the literal.
- [ ] **Step 5:** `cargo build`; tests pass.
- [ ] **Step 6:** commit `feat(kip-932): SharePartitionLeaderManager + persister read_state/write_state`.

## Task 4: `ShareFetch(78)` handler

**Files:** `crates/broker/src/handlers/share_fetch.rs`; `crates/broker/src/share_partition/session.rs` (share-session cache); `crates/broker/src/network/dispatch.rs` (arm + frame handler + flexible); `crates/broker/src/api_catalog.rs` (advertise 78); `handlers/mod.rs` (module decl).

- [ ] **Step 1:** session cache `session.rs`:
```rust
pub(crate) struct ShareSessionCache { sessions: DashMap<(String,String), ShareSession>, max: usize }
struct ShareSession { epoch: i32, partitions: HashSet<(uuid::Uuid,i32)> }
```
`validate(group, member, epoch) -> Result<(), i16>`: epoch 0 → open (set epoch 1); -1 → close (remove); else must equal stored (`INVALID_SHARE_SESSION_EPOCH`), unknown → `SHARE_SESSION_NOT_FOUND`; over `max` → `SHARE_SESSION_LIMIT_REACHED`. Held on `SharePartitionLeaderManager` or `Broker`. Unit-test the epoch transitions.
- [ ] **Step 2:** `handlers/share_fetch.rs` `handle(broker, version, _corr, req_bytes, ctx)`:
  1. decode `ShareFetchRequest`; if `!broker.config.share_group.enable` → response `error_code = UNSUPPORTED_VERSION`.
  2. validate session (group, member, share_session_epoch).
  3. best-effort membership check (find_share + Describe; if the group has no share actor or member absent → `UNKNOWN_MEMBER_ID` top-level — or lenient: skip if not found; document).
  4. for each topic/partition: Read ACL (mirror fetch's `ctx.principal` authorize on the topic); if not topic-leader → `NOT_LEADER_OR_FOLLOWER` + `current_leader`; else: `let cell = mgr.get_or_load(group, topic_id, p).await; let mut st = cell.lock().await;` apply piggybacked `acknowledgement_batches` (each → `st.acknowledge(member, first, last, AckType::from_i8(t), now)`, set `acknowledge_error_code`); `st.expire_locks(now)`; `let hwm = part.high_watermark().await; st.materialize(hwm, cfg.max_inflight_records); let acq = st.acquire(member, max_records, max_bytes, now, cfg.record_lock_duration, cfg.max_delivery_attempts);` read the acquired offset range's bytes from the log (`Log::read_raw` in spawn_blocking, like `do_read`) → `records`; build `PartitionData { records, acquired_records: acq.map(|r| AcquiredRecords{first,last,delivery_count}), .. }`; `mgr.persist_if_dirty(...).await`; drop the lock.
  5. if total acquired across partitions == 0 and `max_wait_ms > 0`: long-poll (parallel of `long_poll_then_reread` — collect `append_notify`+`hw_advance_notify` for the requested partitions, `select_all`+`timeout`), then retry the acquire pass ONCE.
  6. response `acquisition_lock_timeout_ms = cfg.record_lock_duration.as_millis() as i32`.
  Register: dispatch `Some(78) => intercept!(handle_share_fetch_frame(...), "ShareFetch")` + `handle_share_fetch_frame` mirroring `handle_fetch_frame`; `handler_body_flexible` `78 => version >= owned::share_fetch_request::FLEXIBLE_MIN`; `api_catalog` `v!(share_fetch_request)`.
- [ ] **Step 3:** `cargo build`; covered e2e by Task 7. Add a session-cache unit test now.
- [ ] **Step 4:** commit `feat(kip-932): ShareFetch(78) handler + share-session cache`.

## Task 5: `ShareAcknowledge(79)` handler

**Files:** `crates/broker/src/handlers/share_acknowledge.rs`; dispatch arm + frame handler + flexible; api_catalog; mod decl.

- [ ] **Step 1:** `handle(...)`: decode; validate session; for each topic/partition: not-leader → `NOT_LEADER_OR_FOLLOWER`; else lock the cell, apply each `acknowledgement_batch` via `st.acknowledge(member, first, last, AckType::from_i8(t)?, now)`, set per-partition `error_code` (INVALID_RECORD_STATE on failure); `persist_if_dirty`. Build `ShareAcknowledgeResponse`. Dispatch `Some(79) => intercept!(handle_share_acknowledge_frame(...), "ShareAcknowledge")`; `handler_body_flexible` `79 => ...share_acknowledge_request::FLEXIBLE_MIN`; `api_catalog` `v!(share_acknowledge_request)`.
- [ ] **Step 2:** `cargo build`; covered e2e by Task 7.
- [ ] **Step 3:** commit `feat(kip-932): ShareAcknowledge(79) handler`.

## Task 6: acquisition-lock-timeout sweep

**Files:** `crates/broker/src/share_partition/manager.rs` (sweep task); `broker.rs` (spawn in `start`).

- [ ] **Step 1:** add `SharePartitionLeaderManager::spawn_lock_sweeper(self: &Arc<Self>)` — `tokio::spawn` a loop: `tokio::time::interval(cfg.record_lock_duration / 2)` (min 100ms); each tick iterate `leaders` (clone Arcs out of DashMap first), `lock().await`, `expire_locks(now)`, and if dirty `persist_if_dirty`. In `Broker::start`, call `share_partition_leaders.spawn_lock_sweeper()` after construction (store the JoinHandle if the broker tracks tasks; else detached is fine for now).
- [ ] **Step 2:** unit-test `expire_locks` already covers the logic; the sweep is wiring — covered by the Task 7 lock-timeout test. `cargo build`.
- [ ] **Step 3:** commit `feat(kip-932): acquisition-lock-timeout background sweep`.

## Task 7: integration tests

**Files:** `crates/broker/tests/share_consume.rs`.

- [ ] **Step 1:** harness (copy `boot`/`connect`/`create_topic` from `share_groups.rs`; copy the produce helper from `acl_handlers.rs`; add a `share_fetch`/`share_acknowledge` typed-client helper and a `join` helper that sends a `ShareGroupHeartbeat` so the member exists). Tests:
  - `consume_accept_restart`: produce 3 records; join "g1"; ShareFetch (epoch 0) → `acquired_records` covering 3 offsets, delivery_count 1, non-empty `records`; ShareAcknowledge Accept(1) all → ok; ShareFetch again → no acquired records (SPSO advanced); restart (Rejoin) → ShareFetch still returns nothing (SPSO recovered via persister).
  - `release_redelivers`: produce 2; fetch; ShareAcknowledge Release(2); next fetch re-acquires with `delivery_count == 2`.
  - `reject_archives`: produce 2; fetch; Reject(3); next fetch returns nothing and SPSO advanced.
  - `lock_timeout_redelivers`: cfg `record_lock_duration = 200ms`; produce 1; fetch (don't ack); sleep 400ms; next fetch re-acquires with `delivery_count == 2`.
  - `delivery_limit_archives`: cfg `max_delivery_attempts = 2`, `record_lock_duration = 150ms`; produce 1; fetch+expire twice; subsequent fetch returns nothing (archived).
  - `session_epoch_validation`: open (epoch 0), then a fetch with a stale epoch → `INVALID_SHARE_SESSION_EPOCH`; unknown member non-zero epoch → `SHARE_SESSION_NOT_FOUND`.
  Use the `share_state.rs` retry pattern for any coordinator-not-ready transients; decode `records` via the protocol record types to assert payloads where useful.
- [ ] **Step 2:** run → iterate. Fix real bugs in the share-partition code in separate commits (do not weaken assertions).
- [ ] **Step 3: full gate:** `cargo fmt --all && cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`; `bash tools/regenerate.sh && git status --porcelain crates/protocol` empty.
- [ ] **Step 4:** commit `test(kip-932): share-partition consume/ack integration tests`.

---

## Acceptance gate (Slice C)
1. fmt clean. 2. `clippy --workspace --all-targets -D warnings` clean. 3. `cargo test --workspace` green. 4. no drift. 5. ShareFetch/ShareAcknowledge advertised (78/79) + flexible arms. 6. All six `tests/share_consume.rs` cases green. 7. SPSO persists/recovers across restart.

## Self-review
- **Spec coverage:** acquisition machine (T2), config (T1), manager+persister (T3), ShareFetch (T4), ShareAcknowledge (T5), lock sweep (T6), tests incl. redelivery/poison-pill/restart (T7). Deferred read_committed/RENEW unchanged.
- **Type consistency:** `AcquisitionState` (not `SharePartitionState` — avoids the Slice-B clash), `AckType`, `AcquiredRange`, `RecordState`, `SharePartitionLeaderManager`, `ShareSessionCache`, `SharePersister::{read_state,write_state}`, `DS_*` codes used consistently.
- **Confirm-at-build:** exact `Log::read_raw` arg names / `RawRead` fields; whether `send_to_leader` can be refactored to share code with a response-returning variant; the topic-partition `leader_epoch` accessor; how `NOT_LEADER_OR_FOLLOWER` is spelled in `codes`; the produce helper's exact `RecordBatch.into()` payload type; membership-check leniency decision.
