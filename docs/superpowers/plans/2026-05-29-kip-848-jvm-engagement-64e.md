# KIP-848 JVM-Client Engagement (Slice 64e) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make GA `kafka-clients 4.0` `group.protocol=consumer` consumers drive Crabka's KIP-848 path end to end by accepting client-generated member IDs on first join.

**Architecture:** Crabka's next-gen first-join detection requires an empty `member_id` (an obsolete KIP-848 draft where the *server* minted IDs). The finalized protocol has the *client* generate its own member UUID and send it from heartbeat #1 with `member_epoch == 0`. Change the first-join trigger to fire on `member_epoch == 0` for any member not already known, adopting the client-supplied id (server-minted UUID kept only as the empty-id fallback). No other behavior changes.

**Tech Stack:** Rust (edition 2024), tokio, `crabka-broker` crate. JVM acceptance via `docker` + `apache/kafka:4.0.0`. Tests: `cargo test`, `cargo clippy`, `cargo fmt`.

Spec: `docs/superpowers/specs/2026-05-29-crabka-kip-848-jvm-engagement-64e-design.md`

---

## File Structure

- `crates/broker/src/coordinator/next_gen/group_actor.rs` — the fix (first-join branch, ~line 286) + new unit tests in the existing `#[cfg(test)] mod tests`.
- `crates/broker/tests/consumer_group_next_gen.rs` — new raw-RPC integration test for the client-supplied-id path.
- `crates/broker/tests/jvm_consumer_group_next_gen.rs` — remove `#[ignore]` from 4 tests; align the classic image tag.
- `.github/workflows/ci.yml` — add `--test jvm_consumer_group_next_gen` to the `broker-jvm-acceptance` job.
- `STATUS.md`, `README.md` — documentation updates.

---

## Task 1: Fix first-join detection (TDD)

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/group_actor.rs:286`
- Test: `crates/broker/src/coordinator/next_gen/group_actor.rs` (the `#[cfg(test)] mod tests` block, after `first_join_emits_one_batch` ~line 890)

- [ ] **Step 1: Write the failing unit test**

Add to the `mod tests` block in `group_actor.rs` (after `first_join_emits_one_batch`):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_adopts_client_member_id() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create("g");
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "client-uuid-1".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    let resp = rx.await.unwrap();
    assert_eq!(resp.error_code, 0, "client-id first-join must succeed");
    assert_eq!(
        resp.member_id.as_deref(),
        Some("client-uuid-1"),
        "response must echo the client-supplied member id"
    );
    assert!(resp.member_epoch >= 1, "epoch must advance off 0 on join");
    // Folds the spec's `first_join_client_id_emits_one_batch`: the client-id
    // first-join takes the same flush path as the empty-id case and persists
    // exactly one batch.
    assert_eq!(
        log.batches().await.len(),
        1,
        "client-id first join writes exactly one batch"
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-broker --lib first_join_adopts_client_member_id`
Expected: FAIL — before the fix the client-supplied id skips first-join and the existing-member branch returns `error_code = 25` (`UNKNOWN_MEMBER_ID`) with `member_id = None`.

- [ ] **Step 3: Apply the fix**

In `group_actor.rs::handle_heartbeat`, replace the first-join branch (currently `if req.member_epoch == 0 && req.member_id.is_empty() {` at ~line 286, followed by `let new_member_id = uuid::Uuid::new_v4().to_string();`):

```rust
    // ─── First-join path ─────────────────────────────────────────
    // KIP-848 (finalized): the consumer generates its own member UUID and
    // sends it with `member_epoch == 0` on first join. Treat epoch 0 from a
    // member we don't yet know as a first-join, adopting the client-supplied
    // id. An empty `member_id` is tolerated as a fallback (raw-RPC / older
    // callers) by minting a server-side UUID.
    if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
        let new_member_id = if req.member_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.member_id.clone()
        };
```

Leave the rest of the branch (instance-id unrelease check, `build_member`, `add_or_update_member`, `run_reconcile`, `advance_member_epoch`, `flush_pending`, `build_assignment_resp`) unchanged.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --lib first_join_adopts_client_member_id`
Expected: PASS.

- [ ] **Step 5: Add the stale-epoch edge-case test**

Add to the same `mod tests` block:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn known_member_id_epoch_zero_is_stale() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create("g");
    // First join with a client id, epoch 0 → succeeds, epoch advances.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "client-uuid-2".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    assert_eq!(rx.await.unwrap().error_code, 0);

    // Same id re-sending epoch 0 is now a known member at a higher epoch →
    // stale, not a re-join.
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "client-uuid-2".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_host: String::new(),
            reply: tx,
        })
        .await
        .unwrap();
    assert_eq!(rx.await.unwrap().error_code, codes::STALE_MEMBER_EPOCH);
}
```

- [ ] **Step 6: Run both new tests to verify they pass**

Run: `cargo test -p crabka-broker --lib first_join_adopts_client_member_id known_member_id_epoch_zero_is_stale`
Expected: PASS (2 tests). If `codes` is not already imported in the test module, add `use crate::codes;` to the `mod tests` `use` block.

- [ ] **Step 7: Run the full next-gen lib suite to confirm no regression**

Run: `cargo test -p crabka-broker --lib next_gen`
Expected: PASS — the existing empty-`member_id` tests (`first_join_emits_one_batch`, `unchanged_heartbeat_emits_no_batch`, `leave_emits_tombstone_batch`, etc.) still pass via the fallback path.

- [ ] **Step 8: Commit**

```bash
git add crates/broker/src/coordinator/next_gen/group_actor.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "fix(broker): KIP-848 first-join accepts client-generated member IDs (64e)

GA kafka-clients 4.0 generates its own member UUID and sends it with
member_epoch=0 on first join; Crabka required an empty member_id (obsolete
draft) and returned UNKNOWN_MEMBER_ID forever. Trigger first-join on epoch 0
for any unknown member, adopting the client id; keep server-UUID minting only
as the empty-id fallback.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Raw-RPC integration test for the client-id path

**Files:**
- Test: `crates/broker/tests/consumer_group_next_gen.rs` (new test alongside the existing single-member tests)

- [ ] **Step 1: Write the integration test**

Append to `crates/broker/tests/consumer_group_next_gen.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_with_client_member_id_echoes_and_assigns() {
    let (_b, bootstrap, _d) = boot().await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, "tc", 2).await;

    // Client supplies its own member id (GA KIP-848 semantics).
    let mut req = heartbeat("gc", "client-generated-id", 0);
    req.subscribed_topic_names = Some(vec!["tc".into()]);
    let resp = client.send(req).await.unwrap();

    assert_eq!(resp.error_code, 0, "client-id first-join failed");
    assert_eq!(
        resp.member_id.as_deref(),
        Some("client-generated-id"),
        "broker must echo the client-supplied member id"
    );
    let parts: usize = resp
        .assignment
        .expect("assignment present")
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert_eq!(parts, 2, "single member should be assigned both partitions");
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --test consumer_group_next_gen first_join_with_client_member_id_echoes_and_assigns`
Expected: PASS.

- [ ] **Step 3: Run the whole raw-RPC suite to confirm no regression**

Run: `cargo test -p crabka-broker --test consumer_group_next_gen`
Expected: PASS — the existing six empty-`member_id` tests still pass.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/consumer_group_next_gen.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(broker): raw-RPC KIP-848 client-supplied member-id first-join (64e)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Un-ignore JVM acceptance tests + align classic image

**Files:**
- Modify: `crates/broker/tests/jvm_consumer_group_next_gen.rs` (remove 4 `#[ignore]`; align `KAFKA_IMAGE_CLASSIC`)

- [ ] **Step 1: Align the classic image tag to the CI preload**

In `crates/broker/tests/jvm_consumer_group_next_gen.rs`, change:

```rust
const KAFKA_IMAGE_CLASSIC: &str = "confluentinc/cp-kafka:7.5.0";
```

to match the existing CI preload (`confluentinc/cp-kafka:7.4.0`):

```rust
const KAFKA_IMAGE_CLASSIC: &str = "confluentinc/cp-kafka:7.4.0";
```

(The classic image is used only for `kafka-topics --create`, the classic producer, and — in `coexists_with_classic` — a classic consumer; `7.4.0`'s tools behave identically for these flags. This avoids pulling a second large image in CI.)

- [ ] **Step 2: Remove the four `#[ignore]` attributes**

Delete the `#[ignore = "..."]` line above each of these four tests:
- `jvm_kip848_single_consumer_round_trip`
- `jvm_kip848_describe_group`
- `jvm_kip848_delete_group`
- `jvm_kip848_coexists_with_classic`

- [ ] **Step 3: Run the four tests against the real client**

Ensure `apache/kafka:4.0.0` and `confluentinc/cp-kafka:7.4.0` are pulled, then:

Run: `cargo test -p crabka-broker --test jvm_consumer_group_next_gen -- --test-threads=1`
Expected: `test result: ok. 4 passed; 0 failed; 0 ignored`.
(`--test-threads=1` because the tests bind the broker to a fixed host port `9092`; parallel runs hit `AddrInUse`.)

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/jvm_consumer_group_next_gen.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "test(broker): enable KIP-848 JVM acceptance tests (64e)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Wire the JVM acceptance tests into CI

**Files:**
- Modify: `.github/workflows/ci.yml` (the `broker-jvm-acceptance` job, ~lines 245-251)

- [ ] **Step 1: Add the next-gen test binary to the coverage invocation**

In `.github/workflows/ci.yml`, the `Broker JVM acceptance coverage` step currently runs:

```yaml
          cargo llvm-cov -p crabka-broker \
            --test jvm_acceptance \
            --lcov --output-path coverage/broker-jvm-acceptance.lcov \
            -- --ignored --nocapture --test-threads=1
```

Add the next-gen binary:

```yaml
          cargo llvm-cov -p crabka-broker \
            --test jvm_acceptance \
            --test jvm_consumer_group_next_gen \
            --lcov --output-path coverage/broker-jvm-acceptance.lcov \
            -- --ignored --nocapture --test-threads=1
```

(`--ignored` still runs `jvm_acceptance`'s ignored tests; the now-un-ignored next-gen tests run as normal tests in the same binary invocation.)

- [ ] **Step 2: Confirm the preload covers both images**

Verify the `Preload Docker images` step (~line 238) pulls both `confluentinc/cp-kafka:7.4.0` and `apache/kafka:4.0.0`. It already does. No change needed (this is why Task 3 aligned to `7.4.0`).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "ci(broker): run KIP-848 JVM acceptance in broker-jvm-acceptance (64e)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Documentation updates

**Files:**
- Modify: `STATUS.md` (slice 64a out-of-scope list ~line 4621-4625, slice 64a-followup note ~line 4757-4761; append a 64e entry)
- Modify: `README.md` (KIP-848 gaps prose ~line 67-70)

- [ ] **Step 1: Update STATUS.md**

In slice 64a's "Out of scope (follow-up slices)" list, remove the "JVM-client end-to-end engagement" bullet (the one stating `group.version=1 is advertised but kafka-clients 4.0 still times out fetching`).

In the slice 64a-followup section, update the "4 JVM-acceptance tests remain `#[ignore]`d … still fails with `TimeoutException`" note to record that 64e resolved it.

Append a new slice entry at the end of STATUS.md:

```markdown
## Slice 64e — KIP-848 JVM-client engagement (2026-05-29)

- **Root cause.** GA `kafka-clients 4.0` (`group.protocol=consumer`)
  generates its own member UUID and sends it with `member_epoch=0` on first
  join. Crabka's first-join detection required an *empty* `member_id` (an
  obsolete KIP-848 draft where the server minted IDs), so every heartbeat
  returned `UNKNOWN_MEMBER_ID` (25) and the consumer looped ~10k req/s with no
  assignment until `TimeoutException`. Diagnosed by tracing every request from
  a live `apache/kafka:4.0.0` consumer.
- **Fix.** First-join triggers on `member_epoch == 0` for any not-yet-known
  member, adopting the client-supplied `member_id`; an empty id falls back to
  a server-minted UUID (preserves raw-RPC callers).
- **Tests.** 2 new actor unit tests (client-id adoption, known-id-epoch-0
  stale); 1 raw-RPC integration test (echo + assignment); the 4 `jvm_kip848_*`
  acceptance tests un-`#[ignore]`d and passing against `apache/kafka:4.0.0`;
  `broker-jvm-acceptance` CI job now runs `jvm_consumer_group_next_gen`.
- **Image alignment.** `jvm_consumer_group_next_gen` classic image moved from
  `cp-kafka:7.5.0` to `cp-kafka:7.4.0` to match the existing CI preload.
```

- [ ] **Step 2: Update README.md**

In the KIP-848 prose (~line 67), the phrase "next-gen consumer group protocol (KIP-848) is in progress — broker-side foundations, … are in tree; classic→next-gen group migration and the pluggable server-side assignor surface are still pending" — refresh to note that GA `group.protocol=consumer` clients now consume end to end, and the pluggable assignor (64c) is no longer pending (it merged in #276). Keep classic→next-gen migration listed as pending.

- [ ] **Step 3: Commit**

```bash
git add STATUS.md README.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "docs: record KIP-848 JVM-client engagement (64e)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Final verification gates

**Files:** none (verification only)

- [ ] **Step 1: Full workspace test**

Run: `cargo test --workspace`
Expected: green (the JVM acceptance tests are excluded here unless docker is present — they run under their own binary; this confirms unit + integration suites pass).

- [ ] **Step 2: JVM acceptance**

Run: `cargo test -p crabka-broker --test jvm_consumer_group_next_gen -- --test-threads=1`
Expected: `4 passed; 0 failed`.

- [ ] **Step 3: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Format**

Run: `cargo fmt --check`
Expected: clean. (Per project memory: CI gates on `cargo fmt --check`; run `cargo fmt` first if needed, then re-commit.)

- [ ] **Step 5: Confirm no stray diagnostic code**

Run: `git grep -n "kip848diag\|DIAG hb"`
Expected: no matches (the diagnostic instrumentation from the brainstorm was already reverted; this guards against reintroduction).
