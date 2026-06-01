# KIP-932 Share Groups — Slice D (Admin Offset RPCs) Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** `DescribeShareGroupOffsets(90)`, `AlterShareGroupOffsets(91)`, `DeleteShareGroupOffsets(92)` served by the share group coordinator, proxying to the Slice-B persister.

**Spec:** `docs/superpowers/specs/2026-05-31-crabka-kip-932-share-groups-slice-d-design.md`

## Established facts (verbatim from research)
- **Persister access:** `GroupCoordinator::share_persister() -> Option<&Arc<SharePersister>>` (`coordinator/unified/mod.rs`). Reach via `broker.group_manager.next_gen()?.share_persister()`. `SharePersister`: `async initialize(group:&str, topic_id:uuid::Uuid, partition:i32, state_epoch:i32, start_offset:i64) -> Result<(),BrokerError>`; `async delete(group, topic_id, partition) -> Result<(),BrokerError>`; `async read_state(group, topic_id, partition) -> Result<Option<crate::share_coordinator::state::SharePartitionState>, BrokerError>` (`SharePartitionState.start_offset` = SPSO, `.state_epoch`).
- **Group-empty check:** `GroupCoordinator::find_share(group)->Option<Arc<ShareGroupActorHandle>>`; send `ShareGroupActorMessage::Describe { reply }` over `handle.tx` (mpsc), await `ShareDescribeView { members: Vec<ShareDescribeMember>, .. }`; empty = `members.is_empty()`. Absent actor (find_share None) ⇒ treat as empty.
- **Initialized partitions:** `ShareGroupStatePartitionMetadataValue { initialized: Vec<(uuid::Uuid, Vec<i32>)>, deleting: Vec<uuid::Uuid> }` (`coordinator/unified/share/persistence.rs`). Stored per group in `GroupCoordinator.share_seeds_cache` (via `replay_share_state_partition_metadata`). NO read accessor — ADD one (Task 1).
- **Topic name↔id:** `broker.controller.current_image()`; `image.topic(name) -> Option<&TopicRecord{ name, topic_id: uuid::Uuid (crabka_metadata::Uuid = uuid::Uuid), partitions: i32, .. }>`. No reverse map — for the response `TopicId`, resolve from the request name; store the request `topic_name` in the response.
- **Lag:** `broker.partitions.get(topic_name, partition) -> Option<Arc<Partition>>`; `Partition::high_watermark().await -> i64`. Lag = `hwm - spso` when local, else `-1`.
- **Leader-cache invalidate:** `SharePartitionLeaderManager` (`share_partition/manager.rs`) has `leaders: DashMap<(String,uuid::Uuid,i32), Arc<Mutex<AcquisitionState>>>`. ADD `invalidate(&self, group:&str, topic_id:uuid::Uuid, partition:i32)` → `self.leaders.remove(&(group.into(), topic_id, partition))`. Reach via `broker.share_partition_leaders`.
- **Dispatch:** inline-intercept like `delete_groups`/`describe_groups`. `intercept!($call,$label)`; frame handler parses header → `handler_body_flexible` → `principal_or_anonymous(auth)` + `peek_client_id(frame)` → `RequestContext { principal, peer, client_id }` → `handlers::X::handle(broker,version,corr,body,&ctx)` → `encode_response(api_key,corr,body_flexible,&resp)`. `handler_body_flexible` `_ => false` tail — ADD 90/91/92 arms. `api_catalog.rs` `v!(...)`.
- **ACL:** `AuthorizationRequest { principal: ctx.principal, host: ctx.peer, resource_type: ResourceType::Group, resource_name: gid, operation: AclOperation::{Describe|Alter|Delete} }`; `broker.config.authorizer.authorize(&image, &req) == AuthorizationResult::Deny` → `GROUP_AUTHORIZATION_FAILED(30)`.
- **Codes (all exist):** `GROUP_AUTHORIZATION_FAILED=30`, `NON_EMPTY_GROUP=68`, `GROUP_ID_NOT_FOUND=69`, `UNKNOWN_TOPIC_OR_PARTITION=3`, `COORDINATOR_NOT_AVAILABLE=15`, `UNSUPPORTED_VERSION`.
- **Generated types** (import `crabka_protocol::owned::{describe,alter,delete}_share_group_offsets_{request,response}`; `Uuid=crabka_protocol::primitives::uuid::Uuid`; impl `ProtocolRequest`, FLEXIBLE_MIN=0):
  - `DescribeShareGroupOffsetsRequest { groups: Vec<DescribeShareGroupOffsetsRequestGroup{ group_id: String, topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic{ topic_name: String, partitions: Vec<i32> }>> }> }` (90, v0-1). Response `{ throttle_time_ms, groups: Vec<DescribeShareGroupOffsetsResponseGroup{ group_id, topics: Vec<DescribeShareGroupOffsetsResponseTopic{ topic_name, topic_id: Uuid, partitions: Vec<DescribeShareGroupOffsetsResponsePartition{ partition_index, start_offset, leader_epoch, lag, error_code, error_message }> }>, error_code, error_message }> }`.
  - `AlterShareGroupOffsetsRequest { group_id: String, topics: Vec<AlterShareGroupOffsetsRequestTopic{ topic_name, partitions: Vec<AlterShareGroupOffsetsRequestPartition{ partition_index, start_offset }> }> }` (91, v0). Response `{ throttle_time_ms, error_code, error_message, responses: Vec<AlterShareGroupOffsetsResponseTopic{ topic_name, topic_id, partitions: Vec<AlterShareGroupOffsetsResponsePartition{ partition_index, error_code, error_message }> }> }`.
  - `DeleteShareGroupOffsetsRequest { group_id: String, topics: Vec<DeleteShareGroupOffsetsRequestTopic{ topic_name }> }` (92, v0). Response `{ throttle_time_ms, error_code, error_message, responses: Vec<DeleteShareGroupOffsetsResponseTopic{ topic_name, topic_id, error_code, error_message }> }`. (CONFIRM whether DeleteRequestTopic has a `partitions` field by reading the generated file — the schema showed only TopicName.)
- **Tests:** harness in `tests/share_groups.rs`/`share_state.rs`/`share_consume.rs` (boot/connect/create_topic; the `share_consume.rs` produce + join + `bootstrap_share_state`/`wait_for_share_init` + share_fetch/share_acknowledge helpers — reuse them). Config mutable before `Broker::start`.

## Batching: D1 (accessor + invalidate + 3 handlers + wiring) → D2 (tests). Sequential, full `--all-targets` clippy gate.

---

## Task 1: accessor + invalidate + the three handlers

**Files:** `coordinator/unified/mod.rs` (accessor); `share_partition/manager.rs` (invalidate); `handlers/{describe,alter,delete}_share_group_offsets.rs` (new) + `handlers/mod.rs` decls; `network/dispatch.rs` (arms + frame handlers + flexible); `api_catalog.rs` (advertise).

- [ ] **Step 1:** add `GroupCoordinator::share_state_partition_metadata(&self, group_id: &str) -> Option<share::persistence::ShareGroupStatePartitionMetadataValue>` reading `share_seeds_cache` (clone the value). Add `SharePartitionLeaderManager::invalidate(&self, group: &str, topic_id: uuid::Uuid, partition: i32)`. Small unit tests for each (accessor returns None for unknown group / Some after a replay; invalidate removes a cached entry).
- [ ] **Step 2:** implement `describe_share_group_offsets.rs` `handle(broker, version, _corr, req_bytes, ctx)`: feature-gate; per group: ACL Describe → on deny set group `error_code=30`; for each topic resolve name→id (unknown → per-partition `UNKNOWN_TOPIC_OR_PARTITION`); partitions empty ⇒ use `share_state_partition_metadata(group)` initialized list for that topic_id; per partition `persister.read_state` → `start_offset` (or -1 if None), `leader_epoch`, `lag` (hwm−spso if `broker.partitions.get(topic_name,p)` Some, else -1). Build response.
- [ ] **Step 3:** implement `alter_share_group_offsets.rs`: feature-gate; ACL Alter → deny top-level 30; empty-group check (`find_share`+Describe; absent ⇒ empty) else top-level `NON_EMPTY_GROUP(68)`; per (topic,partition): resolve id; `let cur = persister.read_state(...).await?.map(|s| s.state_epoch).unwrap_or(0); persister.initialize(group, topic_id, partition, cur+1, start_offset).await`; on Ok `broker.share_partition_leaders.invalidate(group, topic_id, partition)`; per-partition error_code. Build response.
- [ ] **Step 4:** implement `delete_share_group_offsets.rs`: feature-gate; ACL Delete → 30; empty-group check else 68; per topic: resolve id (unknown → topic `UNKNOWN_TOPIC_OR_PARTITION`); enumerate that topic's initialized partitions from `share_state_partition_metadata(group)`; `persister.delete(group, topic_id, p)` each + `invalidate`; (best-effort) update the group's `ShareGroupStatePartitionMetadata` to drop the topic — if wiring the metadata rewrite is heavy, at minimum delete the persister state + invalidate, and note the metadata-rewrite deferral. Build response.
- [ ] **Step 5:** dispatch: `Some(90/91/92) => intercept!(handle_X_frame(...), "...")` + three frame handlers mirroring `handle_delete_groups_frame`/`handle_describe_groups_frame`; `handler_body_flexible` arms `90/91/92 => version >= owned::X_request::FLEXIBLE_MIN`; `api_catalog` `v!(describe_share_group_offsets_request)`, `v!(alter_share_group_offsets_request)`, `v!(delete_share_group_offsets_request)`; `handlers/mod.rs` module decls.
- [ ] **Step 6:** `cargo build --workspace`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`; `cargo fmt --all`. Commit `feat(kip-932): share-group admin offset RPCs (Describe/Alter/Delete 90-92)`.

## Task 2: integration tests

**Files:** `tests/share_admin_offsets.rs` (reuse the `share_consume.rs` helpers — copy boot/connect/create_topic/produce/join/bootstrap_share_state/share_fetch/share_acknowledge or factor a shared `mod`).

- [ ] **Step 1:** the 5 cases from the spec: describe-reflects-SPSO (after consume+accept), alter-resets-empty-group (+ subsequent ShareFetch starts at the new offset → proves invalidate), alter-non-empty→68, delete-removes-topic, describe-unknown-topic→3. Real assertions over the typed wire path. Use the bootstrap/wait-for-init helpers from `share_consume.rs` so the persister is write-ready.
- [ ] **Step 2:** run → iterate; fix real bugs in separate commits.
- [ ] **Step 3: full gate:** fmt; `clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`; `bash tools/regenerate.sh && git status --porcelain crates/protocol` empty.
- [ ] **Step 4:** commit `test(kip-932): share-group admin offsets integration tests`.

## Self-review
- Spec coverage: 90 (describe), 91 (alter+invalidate), 92 (delete), accessor, dispatch/flexible/api_catalog, tests incl. invalidate + NON_EMPTY_GROUP. Lag best-effort + cross-broker invalidate deferral noted.
- Confirm-at-build: exact `AclOperation` for Delete (Delete vs Alter); whether DeleteRequestTopic carries partitions; how the share actor persists an updated `ShareGroupStatePartitionMetadata` v14 record (reuse the Slice-C lifecycle path) or defer the metadata rewrite; the `principal_or_anonymous`/`peek_client_id` helper names.
