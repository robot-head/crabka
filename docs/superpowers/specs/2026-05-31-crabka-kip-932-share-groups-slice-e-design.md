# KIP-932 Share Groups — Slice E (Native Share Consumer Client) Design

**Date:** 2026-05-31
**Status:** Approved. Builds on broker slices A–D.
**KIP:** KIP-932 (KafkaShareConsumer).

## Goal

A native Rust `ShareConsumer` in `crabka-client-consumer` (alongside `Consumer`): joins a
share group via `ShareGroupHeartbeat` (background loop), `ShareFetch`es assigned partitions to
acquire records under locks, delivers them via `poll()`, and acknowledges them (Implicit:
auto-Accept the prior poll's batch on the next poll; Explicit: app calls accept/release/reject).

## Reused infra (verbatim from research)

- `crabka-client-consumer`: `Consumer` (bon builder `#[builder(start_fn=builder, finish_fn=build)]`), `poll(timeout)->Result<Vec<ConsumerRecord>,ConsumerError>`, `coordinator.rs` heartbeat loop (`tokio::time::interval` + `tokio::select!` on a `CancellationToken`; `heartbeat_once` sends the RPC; outcome state machine), `poll.rs` fetch+decode (`RecordsPayload::as_v2() -> Option<&[RecordBatch]>`, iterate `batch.records` → offset = `base_offset + offset_delta`, key/value/timestamp). `ConsumerRecord { topic, partition, offset, timestamp, key: Option<Bytes>, value: Option<Bytes> }`. `ConsumerError` (`Client`/`Protocol`/`Server(i16)`/…). `AutoOffsetReset`, `IsolationLevel` enums in `builder.rs`.
- `crabka_client_core::Client`: `Client::builder().bootstrap(..).client_id(..).maybe_security(..).build().await`; `client.send::<R: ProtocolRequest>(req) -> Result<R::Response, ClientError>` (bootstrap connection); `client.broker(id).send(req)` (specific broker). The classic consumer sends group + fetch RPCs to the bootstrap connection and the broker serves locally — **the share consumer does the same** (single-broker; multi-broker FindCoordinator+leader routing is a follow-up).
- Generated (impl `ProtocolRequest`, import `crabka_protocol::owned::*`): `ShareGroupHeartbeatRequest{ group_id, member_id, member_epoch, rack_id, subscribed_topic_names: Option<Vec<String>> }` → `Response{ error_code, member_id: Option<String>, member_epoch, heartbeat_interval_ms, assignment: Option<Assignment{ topic_partitions: Vec<TopicPartitions{ topic_id, partitions }> }> }`. `ShareFetchRequest{ group_id: Option<String>, member_id: Option<String>, share_session_epoch: i32, max_wait_ms, min_bytes, max_bytes, max_records, batch_size, share_acquire_mode: i8, is_renew_ack: bool, topics: Vec<FetchTopic{ topic_id, partitions: Vec<FetchPartition{ partition_index, partition_max_bytes, acknowledgement_batches: Vec<AcknowledgementBatch{ first_offset, last_offset, acknowledge_types: Vec<i8> }> }> }>, forgotten_topics_data }` → `Response{ error_code, acquisition_lock_timeout_ms, responses: Vec<ShareFetchableTopicResponse{ topic_id, partitions: Vec<PartitionData{ partition_index, error_code, acknowledge_error_code, records: Option<RecordsPayload>, acquired_records: Vec<AcquiredRecords{ first_offset, last_offset, delivery_count: i16 }> }> }> }`. `ShareAcknowledgeRequest{ group_id, member_id, share_session_epoch, is_renew_ack, topics: Vec<AcknowledgeTopic{ topic_id, partitions: Vec<AcknowledgePartition{ partition_index, acknowledgement_batches }> }> }`. `MetadataRequest` for topic_id↔name + partition counts.
- Ack type wire codes (match broker `AckType::from_i8`): `Accept=1, Release=2, Reject=3` (`Gap=0` is broker-internal). Session epoch: client sends `0` to open; the broker (Slice-C `ShareSessionCache`) sets it to 1 and expects the client to send the returned/incremented epoch next; client tracks + increments per successful ShareFetch/ShareAcknowledge.

## Components

`crates/client-consumer/src/share/` (new submodule; re-export from `lib.rs`):
- `share/consumer.rs` — `ShareConsumer` struct + bon builder + `poll`/`acknowledge`/`commit`/`close`.
- `share/coordinator.rs` — background `ShareGroupHeartbeat` loop (mirror `coordinator.rs`).
- `share/types.rs` — `ShareConsumerRecord { topic, partition, offset, timestamp, key, value, delivery_count: i16 }`, `ShareAckMode { Implicit, Explicit }`, `ShareAckType { Accept, Release, Reject }`, `ShareConsumerError` (reuse `ConsumerError` variants + share-specific).

**`ShareConsumer` state:** `client: Client`, `group_id`, `member_id`, `member_epoch` (shared `Arc<Mutex<i32>>` updated by the heartbeat loop), `assignment: Arc<Mutex<Vec<(topic_id, name, partition)>>>`, `topic_names: Arc<Mutex<HashMap<Uuid,String>>>`, `share_session_epoch: i32` (per-poll), `ack_mode`, `pending_acks: Vec<(topic_id, partition, first, last, ack_type)>` (the prior batch in Implicit, or app-supplied in Explicit), `heartbeat task handle` + `CancellationToken`.

**Builder/build:** create `Client`; send first `ShareGroupHeartbeat` (member_id "", epoch 0, subscribed_topic_names) → capture `member_id`/`member_epoch`/`heartbeat_interval`/`assignment`; `Metadata` to map assignment topic_ids→names + partition counts; spawn the heartbeat loop; return the consumer.

**Heartbeat loop:** `interval(heartbeat_interval)`; each tick send `ShareGroupHeartbeat{ group_id, member_id, member_epoch, subscribed_topic_names: None (steady-state) }`; on response update `member_epoch` + `assignment` (if changed); on fenced/unknown-member error, rejoin from scratch (epoch 0, empty member_id) like the classic loop. Shutdown via the token.

**`poll(timeout) -> Result<Vec<ShareConsumerRecord>, ShareConsumerError>`:**
1. Determine acks to piggyback: Implicit ⇒ `Accept` for the previous poll's delivered ranges; Explicit ⇒ the app's accumulated `pending_acks`. Clear after attaching.
2. Build `ShareFetch` for the current assignment (one `FetchTopic` per assigned topic_id, the assigned partitions, `acknowledgement_batches` = the piggybacked acks for that partition), `share_session_epoch` (0 first call, then the tracked value), `max_wait_ms = timeout`, `max_records`. Send via `client.send`.
3. Increment/track `share_session_epoch` from the exchange. Decode each partition's `records.as_v2()` → records; pair with `acquired_records` ranges (a record's offset within `[first,last]` carries that range's `delivery_count`). Return `ShareConsumerRecord`s. Record the delivered ranges as the next-poll Implicit-Accept set.
4. Honor `acknowledge_error_code` per partition (log/surface).

**`acknowledge(&record, ShareAckType)` (Explicit):** push `(topic_id, partition, offset, offset, ack_type)` into `pending_acks` (coalesce contiguous same-type ranges where easy). **`commit()`** / **`close()`** flush `pending_acks` via a standalone `ShareAcknowledge` (and `close` leaves the group via heartbeat epoch -1 + cancels the loop).

## Error handling
- Heartbeat fenced/unknown-member → rejoin from scratch (mirror classic). ShareFetch/Acknowledge per-partition errors surfaced via the record/ack result; transient → retry next poll. `close()` best-effort (swallow flush errors).

## Non-goals (Slice E)
- Multi-broker FindCoordinator + per-leader routing (send to bootstrap; follow-up).
- `RENEW` ack / read_committed (broker Slice F).
- Streams-style cooperative assignment (share assignment is server-driven, non-exclusive).

## Testing (`crates/client-consumer/tests/share_consumer.rs`, in-process broker)
1. **basic consume + implicit accept:** produce N; `ShareConsumer.poll` returns N records (delivery_count 1); second poll auto-Accepts the first batch and returns nothing new (SPSO advanced).
2. **explicit release → redelivery:** explicit mode; poll, `acknowledge(Release)` all, poll again → same records, delivery_count 2.
3. **explicit reject:** reject → not redelivered; SPSO advances.
4. **two consumers share a topic:** two `ShareConsumer`s in one group on a multi-partition topic both receive records (non-overlapping under the SimpleAssignor); together they cover all produced records.
5. **close leaves the group:** after `close()`, `ShareGroupDescribe` (via a raw client) shows the member gone.

## Acceptance gate
1. fmt clean. 2. `clippy --workspace --all-targets -- -D warnings` clean. 3. `cargo test --workspace` green. 4. no drift. 5. All `tests/share_consumer.rs` cases green.

## Decomposition
- **E1:** `share/{types,coordinator,consumer}.rs` skeleton — builder, first heartbeat + background loop, state, `close()`. lib.rs re-exports. (No fetch.)
- **E2:** `poll()` + ShareFetch + session epoch + decode/pair + implicit auto-ack + explicit `acknowledge`/`commit` + Metadata resolution.
- **E3:** `tests/share_consumer.rs`.
