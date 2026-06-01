# KIP-932 Share Groups — Slice E (Native Share Consumer Client) Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** a `ShareConsumer` in `crabka-client-consumer` — ShareGroupHeartbeat membership + ShareFetch acquire + poll() delivery + ShareAcknowledge (implicit/explicit).

**Spec:** `docs/superpowers/specs/2026-05-31-crabka-kip-932-share-groups-slice-e-design.md`

## Established facts (verbatim from research)
- **Crate:** `crates/client-consumer/` — `src/{lib.rs, consumer.rs, builder.rs, poll.rs, coordinator.rs, error.rs}`. `Consumer` uses `#[bon::bon] impl Consumer { #[builder(start_fn=builder, finish_fn=build)] pub async fn start(...) -> Result<Self, ConsumerError> }`. `poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>, ConsumerError>`. `ConsumerRecord { topic: String, partition: i32, offset: i64, timestamp: i64, key: Option<Bytes>, value: Option<Bytes> }`. `ConsumerError` enum (`Client(#[from] ClientError)`, `Protocol(#[from] ProtocolError)`, `Server(i16)`, `CoordinatorUnavailable`, `NotSubscribed`, …).
- **Heartbeat loop template** (`coordinator.rs`): `pub(crate) async fn run(mut state: CoordinatorState, shutdown: CancellationToken)` — `let mut ticker = tokio::time::interval(state.heartbeat_interval); ticker.set_missed_tick_behavior(Skip);` loop `tokio::select! { () = shutdown.cancelled() => break, _ = ticker.tick() => {} }` then `heartbeat_once(&state).await`. `heartbeat_once` sends the RPC via `state.client.send(...)` and maps `error_code` to an outcome. Mirror this with `ShareGroupHeartbeatRequest`.
- **Fetch+decode template** (`poll.rs`): build request topics from the live assignment, `client.send(FetchRequest{..})`, then per partition `let Some(payload) = &part.records else { continue }; let Some(batches) = payload.as_v2() else { continue }; for batch in batches { for r in &batch.records { offset = batch.base_offset + i64::from(r.offset_delta); ... key: r.key.clone(), value: r.value.clone(), timestamp: batch.base_timestamp + r.timestamp_delta } }`. `RecordsPayload::as_v2(&self) -> Option<&[RecordBatch]>` (`crabka_protocol::records`).
- **client-core:** `Client::builder().bootstrap(&s).client_id(s).maybe_security(sec).build().await -> Result<Client, ClientError>`; `client.send::<R: ProtocolRequest>(req) -> Result<R::Response, ClientError>` (bootstrap conn). Send share RPCs via `client.send` (single-broker).
- **Generated owned types** (impl `ProtocolRequest`; `Uuid=crabka_protocol::primitives::uuid::Uuid`; group_id/member_id on ShareFetch are `Option<String>`): see spec for full field lists. `ShareGroupHeartbeatResponse.assignment: Option<Assignment{ topic_partitions: Vec<common::share_group_heartbeat_response::topic_partitions::TopicPartitions{ topic_id, partitions: Vec<i32> }> }>` — READ the generated file for the exact nested module path. `ShareFetchResponse...PartitionData{ records: Option<RecordsPayload>, acquired_records: Vec<AcquiredRecords{ first_offset, last_offset, delivery_count: i16 }>, acknowledge_error_code }`. Ack wire codes: Accept=1, Release=2, Reject=3.
- **Tests:** `crates/client-consumer/tests/integration.rs` uses `crabka_broker::{Broker, BrokerConfig}` + `crabka_client_core::Client` + a `record_batch_with_values` helper + produce via the producer or a `ProduceRequest`. In-process broker: `Broker::start(BrokerConfig::for_tests(dir)).await`; `broker.listen_addr()`. Reuse this pattern.
- **Session epoch:** client tracks `share_session_epoch`; send 0 first (open), then the value the broker expects next (the Slice-C `ShareSessionCache` opens at 0→1 and validates the next as the stored epoch; have the client send 0, then 1, 2, … incrementing per successful ShareFetch — verify against `share_partition/session.rs` validate logic and the Slice-C `tests/share_consume.rs` which sequences epochs 0→1→2).

## Batching: E1 (skeleton + heartbeat) → E2 (poll + ack) → E3 (tests). Sequential, full `--all-targets` clippy gate.

---

## Task E1: ShareConsumer skeleton + heartbeat loop
**Files:** `crates/client-consumer/src/share/{mod,types,coordinator,consumer}.rs`; `src/lib.rs` (add `mod share; pub use share::{ShareConsumer, ShareConsumerRecord, ShareAckMode, ShareAckType};`).

- [ ] **Step 1:** `share/types.rs`: `ShareConsumerRecord { topic, partition, offset, timestamp, key: Option<Bytes>, value: Option<Bytes>, delivery_count: i16 }`; `#[derive(Clone,Copy)] enum ShareAckMode { Implicit, Explicit }` (Default Implicit); `enum ShareAckType { Accept, Release, Reject }` with `fn wire(self)->i8` (1/2/3). Reuse `ConsumerError` (or a thin `ShareConsumerError` wrapping it — pick the simpler; reuse `ConsumerError` if its variants suffice).
- [ ] **Step 2:** `share/consumer.rs`: `ShareConsumer` struct (fields per spec: `client: Client`, `group_id`, `member_id`, `member_epoch: Arc<Mutex<i32>>`, `assignment: Arc<Mutex<Vec<(Uuid,String,i32)>>>`, `topic_names: Arc<Mutex<HashMap<Uuid,String>>>`, `share_session_epoch: i32`, `ack_mode`, `pending_acks: Vec<...>`, `prev_delivered: Vec<...>`, `shutdown: CancellationToken`, `hb_handle: Option<JoinHandle<()>>`). bon builder `#[builder(start_fn=builder, finish_fn=build)] pub async fn start(bootstrap, client_id, group_id, subscribe: Vec<String>, ack_mode, session_timeout, heartbeat_interval, security) -> Result<Self, ConsumerError>`: build `Client`; send first `ShareGroupHeartbeat{ group_id, member_id:"".into(), member_epoch:0, subscribed_topic_names: Some(subscribe) }`; capture member_id/member_epoch/assignment/heartbeat_interval; `client.send(MetadataRequest::default())` → map assignment topic_ids→names; spawn `share::coordinator::run`; return.
- [ ] **Step 3:** `share/coordinator.rs`: `ShareCoordinatorState { client, group_id, member_id, member_epoch: Arc<Mutex<i32>>, assignment: Arc<Mutex<..>>, topic_names: Arc<Mutex<..>>, subscribe, heartbeat_interval }` + `pub(crate) async fn run(state, shutdown)` mirroring `coordinator.rs::run` (ticker + select on shutdown; each tick `ShareGroupHeartbeat{ member_id, member_epoch: *lock, subscribed_topic_names: None }`; on ok update `member_epoch` + `assignment`; on fenced/unknown-member rejoin from scratch). `close(&mut self)`: cancel token, await handle, send a leave heartbeat (`member_epoch: -1`).
- [ ] **Step 4:** unit/smoke: a test that builds a `ShareConsumer` against an in-process broker, joins (member_id non-empty), and `close()`s cleanly. `cargo build -p crabka-client-consumer`; clippy.
- [ ] **Step 5:** commit `feat(kip-932): ShareConsumer skeleton + ShareGroupHeartbeat loop`.

## Task E2: poll() + acknowledge
**Files:** `crates/client-consumer/src/share/poll.rs` (or in `consumer.rs`); `share/consumer.rs` (acknowledge/commit).

- [ ] **Step 1:** `poll(&mut self, timeout) -> Result<Vec<ShareConsumerRecord>, ConsumerError>`: gather piggyback acks (Implicit: Accept the `prev_delivered` ranges; Explicit: drain `pending_acks`); build `ShareFetch` over the live assignment with those `acknowledgement_batches`, `share_session_epoch`, `max_wait_ms=timeout`, `max_records`; `client.send`; advance `share_session_epoch`; decode `records.as_v2()` + pair with `acquired_records` (record offset ∈ [first,last] → delivery_count); set `prev_delivered` to the returned ranges; return records.
- [ ] **Step 2:** `acknowledge(&mut self, record: &ShareConsumerRecord, ack: ShareAckType)` (Explicit): push `(topic_id, partition, offset, offset, ack.wire())` to `pending_acks`. `commit(&mut self)`: flush `pending_acks` via a standalone `ShareAcknowledge` (clear after). `close()` also flushes.
- [ ] **Step 3:** `cargo build`; clippy. Commit `feat(kip-932): ShareConsumer poll + acknowledge (implicit/explicit)`.

## Task E3: integration tests
**Files:** `crates/client-consumer/tests/share_consumer.rs`.

- [ ] **Step 1:** the 5 spec cases (basic consume+implicit-accept; explicit release→redelivery delivery_count 2; explicit reject→no redelivery+SPSO advance; two consumers share a topic; close leaves the group). Reuse the `integration.rs` broker+produce harness; create the topic; for "close leaves group" send a raw `ShareGroupDescribeRequest` via a `Client` and assert the member is gone.
- [ ] **Step 2:** run → iterate; fix real bugs (separate commits).
- [ ] **Step 3: full gate:** fmt; `clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`; `bash tools/regenerate.sh && git status --porcelain crates/protocol` empty.
- [ ] **Step 4:** commit `test(kip-932): share consumer integration tests`.

## Self-review
- Spec coverage: skeleton+heartbeat (E1), poll+ack both modes (E2), tests incl. redelivery + two-consumer sharing + close (E3). Multi-broker routing / RENEW / read_committed deferred.
- Type consistency: `ShareConsumer`, `ShareConsumerRecord`, `ShareAckMode`, `ShareAckType::wire`, `share_session_epoch`, `prev_delivered`/`pending_acks`.
- Confirm-at-build: exact `Assignment.topic_partitions` nested module path; whether `bon` is already a dep of client-consumer (it is — `Consumer` uses it); the session-epoch sequence the broker expects (mirror `tests/share_consume.rs`); whether to reuse `ConsumerError` or add `ShareConsumerError`.
