# Slice 64e — KIP-848 JVM-client engagement: client-generated member IDs

**Status:** design
**Date:** 2026-05-29
**Roadmap:** follow-up to slice 64a (foundations, PR #260) and the 64a follow-up
(persistence + `group.version=1` advertisement, PR #267). Closes the
"JVM-client end-to-end engagement" gap that left four `jvm_kip848_*`
acceptance tests `#[ignore]`d.

## Goal

Make a real GA `kafka-clients 4.0` consumer with `group.protocol=consumer`
drive Crabka's KIP-848 path end to end — join, receive an assignment, fetch,
and commit — and land all four `jvm_kip848_*` acceptance tests in
`crates/broker/tests/jvm_consumer_group_next_gen.rs` as **passing**, not
`#[ignore]`d, including in CI.

## Root cause (diagnosed empirically, 2026-05-29)

Driving `mirror.gcr.io/apache/kafka:4.0.0`'s `kafka-console-consumer.sh
--consumer-property group.protocol=consumer` against an in-process broker and
tracing every request shows the consumer issues `ApiVersions` →
`FindCoordinator` → `GetTelemetrySubscriptions`, then **84,621
`ConsumerGroupHeartbeat` requests and nothing else** — no `Metadata`, no
`Fetch`, no `OffsetFetch` — at roughly 10,000 req/s with no backoff, until the
client's `--timeout-ms` expires with `TimeoutException: null`.

Every heartbeat carries a **client-generated member UUID** (e.g.
`ybolt61zTU-jmzAXipTM0A`) with `member_epoch = 0`, and every response returns
**`error_code = 25` (`UNKNOWN_MEMBER_ID`)**.

Crabka's first-join detection in
`coordinator/next_gen/group_actor.rs::handle_heartbeat` is:

```rust
if req.member_epoch == 0 && req.member_id.is_empty() {  // ← obsolete draft
```

This reflects an **early KIP-848 draft** in which the *server* minted member
IDs and the client sent an empty `member_id` on first join. The **finalized
protocol** (and the GA `kafka-clients 4.0` implementation) has the **consumer
generate its own member UUID** (`Uuid.randomUuid()`) and send it from the very
first heartbeat with `member_epoch = 0`. Because the client's `member_id` is
non-empty, Crabka skips first-join, falls through to the existing-member epoch
check, finds no such member (`cur_epoch == -2`), and returns
`UNKNOWN_MEMBER_ID`. The client retries instantly forever, never reaches a
non-empty assignment, never fetches.

**Verification.** A throwaway one-line spike changing the trigger to
`req.member_epoch == 0 && !state.members.contains_key(&req.member_id)` (adopting
the client-supplied id) made the consumer receive its assignment
(`asg=1, asg_parts=1` at `member_epoch=1`) and consume the messages. With the
spike in place, **all four `jvm_kip848_*` tests pass** — `single_consumer_round_trip`,
`describe_group`, `delete_group`, and `coexists_with_classic`. The spike was
reverted; this slice lands the real change with tests.

No further walls were found behind this one. The topic-ID assignment path,
Fetch v13 topic-ID resolution, and `__consumer_offsets` persistence — all
already present — work correctly once the member is registered.

## Non-goals

- Classic → next-gen group migration (the `group.consumer.migration.policy`
  program; see the companion migration roadmap). This slice does **not** touch
  the permanent group-type lock. A classic-locked group still rejects
  heartbeats with `GROUP_ID_NOT_FOUND`; a next-gen group still rejects classic
  `JoinGroup` — the `coexists_with_classic` test uses two *separate* groups.
- `OffsetFetch`/`OffsetCommit` next-gen member-epoch fencing changes beyond what
  the foundations already do. (`--from-beginning` with no committed offsets
  resets to earliest, which the acceptance tests exercise.)
- Share groups (KIP-932).

## Architecture

### The change

`coordinator/next_gen/group_actor.rs::handle_heartbeat`, first-join branch:

```rust
// KIP-848 (finalized): the consumer generates its own member UUID and sends
// it with member_epoch == 0 on first join. Treat epoch 0 from a member we
// don't yet know as a first-join, adopting the client-supplied id. An empty
// member_id is tolerated as a fallback (older/raw-RPC callers) by minting a
// server-side UUID.
if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
    let new_member_id = if req.member_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        req.member_id.clone()
    };
    // ... unchanged: instance-id unrelease check, build_member, add_or_update_member,
    //     run_reconcile, advance_member_epoch, flush_pending, build_assignment_resp.
}
```

`build_member` already accepts the id as a parameter; `build_assignment_resp`
already echoes `member_id: Some(member_id.into())`. No other signature changes.

### Why `!contains_key` rather than just `member_epoch == 0`

A known member that re-sends `member_epoch == 0` (rather than the epoch we
handed it) is **not** a first-join — it is a fenced/stale condition. Guarding on
`!contains_key` routes a known id at epoch 0 to the existing-member epoch
validation, which returns `STALE_MEMBER_EPOCH`. GA clients only send epoch 0
once per member lifetime (a reconnecting consumer mints a fresh UUID), so this
path is rare; defining it as stale matches Kafka's fencing posture and avoids
silently resetting a live member's assignment.

### Persistence

Unchanged. The member is now keyed in `__consumer_offsets` by the
client-supplied UUID instead of a server-minted one — the record schema
(k3/k5/k6/k7/k8) and the bootstrap-replay path are agnostic to which side
chose the id. The id is just a string.

## Error handling & edge cases

| Case | Handling |
|------|----------|
| First heartbeat, client UUID, epoch 0 | First-join: register under the client's id, reconcile, advance epoch, return assignment. (The fix.) |
| First heartbeat, empty `member_id`, epoch 0 (raw-RPC / older callers) | Fallback: mint a server-side UUID. Preserves existing raw-RPC integration tests. |
| Known member id, epoch 0 | Not first-join → existing-member branch → `STALE_MEMBER_EPOCH`. |
| Known member id, correct epoch | Steady-state path, unchanged. |
| `member_epoch == -1` (leave) | Leave path, unchanged. |
| Static member (`instance_id`) re-join at epoch 0 | The existing `UNRELEASED_INSTANCE_ID` guard inside the first-join branch is unchanged and still runs. |

No new wire error codes.

## Testing

### Unit (`group_actor.rs`)

Existing first-join tests send `member_id: String::new()` and exercise the
**fallback** path — they continue to pass unchanged. Add tests for the **GA**
path:

- `first_join_adopts_client_member_id` — heartbeat with a client-supplied
  `member_id` + epoch 0 registers a member under that exact id and the response
  echoes it; `member_epoch` advances to 1.
- `first_join_client_id_emits_one_batch` — the client-id first-join writes
  exactly one record batch (parity with `first_join_emits_one_batch`).
- `known_member_id_epoch_zero_is_stale` — a second heartbeat from a registered
  member with `member_epoch = 0` returns `STALE_MEMBER_EPOCH`.

### Integration (raw RPC, `consumer_group_next_gen.rs`)

The existing six tests use empty `member_id` (fallback) and must continue to
pass. Add one test that supplies a client-generated `member_id` on first join
and asserts the response echoes it and carries an assignment.

### JVM acceptance (`jvm_consumer_group_next_gen.rs`)

- Remove `#[ignore]` from all four `jvm_kip848_*` tests. All must pass against
  `mirror.gcr.io/apache/kafka:4.0.0` with `group.protocol=consumer`.

### CI (`.github/workflows/ci.yml`, `broker-jvm-acceptance` job)

- Add `--test jvm_consumer_group_next_gen` to the `cargo llvm-cov` invocation
  alongside `--test jvm_acceptance`.
- **Image preload gap:** the job currently preloads `mirror.gcr.io/confluentinc/cp-kafka:7.4.0`
  and `mirror.gcr.io/apache/kafka:4.0.0`. The next-gen test's `KAFKA_IMAGE_CLASSIC` is
  `mirror.gcr.io/confluentinc/cp-kafka:7.5.0` (used for topic-create + the classic producer/
  consumer). Either add `docker pull mirror.gcr.io/confluentinc/cp-kafka:7.5.0` to the preload
  step, or align the next-gen test's `KAFKA_IMAGE_CLASSIC` to `7.4.0` to match
  the existing preload. Prefer aligning to `7.4.0` to avoid pulling a second
  large image in CI; confirm `7.4.0`'s `kafka-topics`/`kafka-console-producer`
  behave identically (they do for these flags).

## Acceptance gates

1. `cargo test --workspace` green.
2. `cargo test -p crabka-broker --test jvm_consumer_group_next_gen -- --ignored`
   — all four pass against `mirror.gcr.io/apache/kafka:4.0.0`.
3. `cargo clippy --workspace --all-targets -- -D warnings` clean.
4. `cargo fmt --check` clean.
5. CI `broker-jvm-acceptance` runs `jvm_consumer_group_next_gen` and is green.
6. STATUS.md updated: mark the JVM-engagement gap closed under slice 64a's
   out-of-scope list; add a 64e entry. README KIP-848 row/notes refreshed to
   reflect that GA `group.protocol=consumer` clients now work end to end.
