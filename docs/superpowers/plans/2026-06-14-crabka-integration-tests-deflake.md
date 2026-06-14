# Integration-Tests Sleep De-flake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 27 reducible `sleep(...)` calls across the 5 integration-test files with deterministic condition-awaits and bounded timeouts, keeping the 2 deliberately time-based share-lock tests.

**Architecture:** Test-only changes. Three mechanical recipes — delete-redundant-fixed-sleep, wrap-unbounded-loop-in-`timeout`, swap-ad-hoc-materialize-loop-for-`BrokerHandle`-awaiter. No shared helper module (the poll loops mutate consumers, which fights a generic closure-helper's borrows; bound each in place). No production code beyond reusing existing test-helper-`cfg` `BrokerHandle` awaiters.

**Tech Stack:** Rust, `tokio::time::timeout`, in-process Crabka broker + Crabka Rust clients (`crabka-client-consumer`/`-admin`/`-core`), existing `BrokerHandle` awaiters.

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-integration-tests-deflake-design.md`

Each task touches exactly one test file (file sets are disjoint), so Tasks 1–5 may run in parallel; Task 6 (final verification) runs after.

---

## Recipes (referenced by every task)

**Recipe A — delete redundant fixed-sleep-then-act.** A fixed `sleep(...)` that *guesses* an async transition settled, when a deterministic `wait_*` await already brackets it, is removed outright (the await is the real precondition).

**Recipe B — wrap an unbounded poll loop in a bounded `timeout`.** The loop already breaks on the correct observable condition; wrap it so it can't hang and fails with a named message. The inner retry sleep stays.

```rust
// before
loop {
    if <CONDITION> { break; }
    tokio::time::sleep(Duration::from_millis(N)).await;
}
// after
tokio::time::timeout(Duration::from_secs(30), async {
    loop {
        if <CONDITION> { break; }
        tokio::time::sleep(Duration::from_millis(N)).await;
    }
})
.await
.expect("<short description of CONDITION> within 30s");
```

For a loop that **produces a value** (`let x = loop { ... break v; ... };`), bind the timeout result:

```rust
let x = tokio::time::timeout(Duration::from_secs(30), async {
    loop {
        // ...
        if <COND> { break <v>; }
        tokio::time::sleep(Duration::from_millis(N)).await;
    }
})
.await
.expect("<desc> within 30s");
```

**Recipe C — replace an ad-hoc "poll until partition/state materializes" loop with the existing `BrokerHandle` awaiter.** Signatures (both already exist, behind the test-helper `cfg`):

```rust
// crates/broker/src/broker.rs:1131
pub async fn wait_until_partition_present(&self, topic: &str, partition: i32);
// crates/broker/src/broker.rs:450 (30s-bounded internally; panics on timeout)
pub async fn wait_for_share_state_summary(&self, group: &str, topic_id: uuid::Uuid, partition: i32);
```

```rust
// before
for _ in 0..200 {
    if handle.has_partition("t", 0).await && handle.has_partition("t", 1).await { break; }
    tokio::time::sleep(Duration::from_millis(50)).await;
}
// after
handle.wait_until_partition_present("t", 0).await;
handle.wait_until_partition_present("t", 1).await;
```

Per-task verification uses a stress loop (the de-flake discipline) — run the file's tests 10× and require zero failures:

```bash
for i in $(seq 1 10); do cargo test -p crabka-integration-tests --test <FILE_STEM> || { echo "FLAKE on run $i"; break; }; done
```

(Package name: confirm with `cargo metadata`/`Cargo.toml`; expected `crabka-integration-tests`. `<FILE_STEM>` is the file name without `.rs`.)

---

## Task 1: De-flake `consumer_cooperative_rebalance.rs` (11 sleeps: 2 broker→A/C, 9 poll→B)

**Files:** Modify `crates/integration-tests/tests/consumer_cooperative_rebalance.rs`

- [ ] **Step 1: Bound the two shared helpers (Recipe B)**

Replace `wait_for_assignment_count` (lines 429-437) and `wait_for_total_assignment` (lines 442-455) with bounded versions:

```rust
async fn wait_for_assignment_count(consumer: &Consumer, expected: usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if consumer.assignment().await.len() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("assignment count did not reach {expected} within 30s"));
}

/// Wait until the union of all consumers' assignments has `expected` unique
/// `(topic, partition)` entries. Used to confirm a cooperative rebalance has
/// settled before introducing the next membership change.
async fn wait_for_total_assignment(consumers: &[&Consumer], expected: usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut union: HashSet<(String, i32)> = HashSet::new();
            for c in consumers {
                for tp in c.assignment().await {
                    union.insert(tp);
                }
            }
            if union.len() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("total assignment did not reach {expected} within 30s"));
}
```

- [ ] **Step 2: Remove the two redundant fixed pre-join sleeps (Recipe A)**

At line 51, delete `tokio::time::sleep(Duration::from_millis(500)).await;` — `wait_for_assignment_count(&m1, 6)` (line 42) already proves m1 settled, and `wait_for_total_assignment(&[&m1, &m2], 6)` (line 53) covers the m1+m2 settle. Replace the stale comment block (lines 45-50) with a one-liner noting m1 is already settled. Likewise delete the line-56 `sleep(500ms)` before m3 (the `wait_for_total_assignment` on line 53 already gated m1+m2; the 3-member settle loop below gates the rest).

- [ ] **Step 3: Bound the remaining poll loops (Recipe B)**

Wrap each of these loops in `tokio::time::timeout(Duration::from_secs(30), async { ... }).await.expect(...)`, preserving the existing loop body and break condition:
- the 3-member settle loop `let union = loop { ... };` (lines 74-91; bind the result — the `break union;`) → expect `"3-member cooperative assignment settled within 30s"`
- the first/second/subsequent record-drain loops (lines ~149, ~175, ~242, ~298) — each `while values_seen.len() < N { ... poll ...; sleep }` → expect `"drained N records within 30s"`
- the partition-rebalance wait loop (line ~219, `m1_n == 2 && m2_n == 2`) → expect `"2/2 split within 30s"`

- [ ] **Step 4: Handle the produce-retry-on-metadata-race sleep (line ~394)**

In the `produce_to_partition` helper, the retry loop sleeps on `err == 3` (UNKNOWN_TOPIC_OR_PARTITION) before retrying. Precede the first produce attempt with `broker.wait_until_partition_present(topic, partition).await` (Recipe C) so the topic is present before producing; keep the bounded attempt loop (≤5) as a defensive backstop. (If the helper lacks a `&BrokerHandle`, thread it in from the caller.)

- [ ] **Step 5: Build + stress-run**

Run: `cargo test -p crabka-integration-tests --test consumer_cooperative_rebalance` — expect PASS.
Then the 10× stress loop (above) — expect zero flakes.

- [ ] **Step 6: Commit**

```bash
git add crates/integration-tests/tests/consumer_cooperative_rebalance.rs
git commit -m "test(integration): de-flake cooperative-rebalance waits (bounded awaits, drop fixed sleeps)"
```

---

## Task 2: De-flake `consumer_integration.rs` (5 sleeps: all poll→B, 2 metadata-race)

**Files:** Modify `crates/integration-tests/tests/consumer_integration.rs`

- [ ] **Step 1: Bound the assignment-settle poll loops (Recipe B)**

Wrap each in `timeout(30s, …)`:
- the eager-rebalance 1/1-split loop (line ~354, `m1==1 && m2==1`) → expect `"1/1 eager split within 30s"`
- the m1-reacquire-both loop (line ~373, `m1.assignment().len() == 2`) → expect `"m1 reacquired both partitions within 30s"`
- the seed-assignment loop (line ~673, `while seed.assignment().await.is_empty()`) → expect `"seed consumer assigned within 30s"`

- [ ] **Step 2: Handle the two produce-retry-on-metadata sleeps (lines ~98, ~151)**

Same as Task 1 Step 4: await `broker.wait_until_partition_present(topic, partition)` before producing, keeping the bounded ≤5-attempt loop as a backstop.

- [ ] **Step 3: Build + stress-run**

Run: `cargo test -p crabka-integration-tests --test consumer_integration` then the 10× stress loop. Expect zero flakes.

- [ ] **Step 4: Commit**

```bash
git add crates/integration-tests/tests/consumer_integration.rs
git commit -m "test(integration): de-flake consumer_integration waits (bounded awaits + await-partition-present)"
```

---

## Task 3: De-flake `consumer_share_consumer.rs` (9 sleeps: 4 broker→C, 3 poll→B, 2 KEEP)

**Files:** Modify `crates/integration-tests/tests/consumer_share_consumer.rs`

- [ ] **Step 1: Swap partition/state-materialize loops for `BrokerHandle` awaiters (Recipe C)**

- `create_topic_led` loop (line ~99, `broker.has_partition(topic, 0)`) → `broker.wait_until_partition_present(topic, 0).await;`
- `bootstrap_share_state` loop (line ~144, awaiting all `SHARE_STATE_PARTITIONS`) → loop `for p in 0..SHARE_STATE_PARTITIONS { broker.wait_until_partition_present(SHARE_STATE_TOPIC, p).await; }` (use the share-state topic name/const already in the file).
- `wait_for_share_init` loop (line ~169, `share_state_summary_for_test(...).is_some()`) → `broker.wait_for_share_state_summary(group, tid, partition).await;`
- `create_multi_partition_led` loop (line ~243, all `num_partitions`) → `for p in 0..num_partitions { broker.wait_until_partition_present(topic, p).await; }`

- [ ] **Step 2: Handle the two produce-leadership-race sleeps (lines ~216, ~292)**

These retry on `error_code == 3 || error_code == 6` (UNKNOWN_TOPIC_OR_PARTITION / NOT_LEADER_OR_FOLLOWER). Precede the produce with `broker.wait_until_partition_present(topic, partition).await` (Recipe C); keep the bounded attempt loop as a backstop.

- [ ] **Step 3: Bound the leave-eviction poll loop (Recipe B)**

The loop awaiting a member's eviction from `describe_group` (line ~718, `if absent { break }`) → wrap in `timeout(30s, …)` → expect `"member evicted from group within 30s"`.

- [ ] **Step 4: Annotate the two intentional timing sleeps (KEEP)**

At line ~791 (`renew_at = acquired_at + 400ms`) and line ~803 (`target = acquired_at + 1150ms`), add a one-line comment above each, e.g.:

```rust
// Intentional real-time delay: this exercises share-lock renew-before-expiry
// (lock TTL is 1s); it tests time-based behavior and must not be replaced with
// state polling. See spec 2026-06-14-crabka-integration-tests-deflake-design.md.
```

(and the analogous redelivery-after-expiry note at line ~803). Do not change the sleeps.

- [ ] **Step 5: Build + stress-run**

Run: `cargo test -p crabka-integration-tests --test consumer_share_consumer` then the 10× stress loop. Expect zero flakes. (Per memory, share-group tests can be slow on Windows — allow a generous per-run timeout; the 2 KEEP timing tests add ~1.5s each.)

- [ ] **Step 6: Commit**

```bash
git add crates/integration-tests/tests/consumer_share_consumer.rs
git commit -m "test(integration): de-flake share-consumer waits via BrokerHandle awaiters; keep 2 timing tests"
```

---

## Task 4: De-flake `admin_round_trip.rs` (2 sleeps: both poll→B)

**Files:** Modify `crates/integration-tests/tests/admin_round_trip.rs`

- [ ] **Step 1: Replace the two "brief wait for metadata refresh" sleeps (Recipe B)**

At line ~94 (after `create_partitions`), replace `sleep(200ms); let md = admin.metadata(&["foo"]).await;` with a bounded poll on the observable condition:

```rust
let md = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
        let md = admin.metadata(&["foo"]).await.expect("metadata");
        if md.topics.iter().any(|t| t.name == "foo" && t.partitions.len() == 5) {
            break md;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
})
.await
.expect("partition count reached 5 within 10s");
```

(Adapt the field accessors — `topics`/`name`/`partitions` — to the actual `Metadata` return type in this file; the existing post-sleep assertion shows the right shape.) At line ~154 (after `delete_topics`), the same pattern polling until `foo` is absent or error-marked in the metadata response.

- [ ] **Step 2: Build + stress-run**

Run: `cargo test -p crabka-integration-tests --test admin_round_trip` then the 10× stress loop. Expect zero flakes.

- [ ] **Step 3: Commit**

```bash
git add crates/integration-tests/tests/admin_round_trip.rs
git commit -m "test(integration): de-flake admin_round_trip metadata waits (bounded poll on observed state)"
```

---

## Task 5: De-flake `admin_log_dirs_round_trip.rs` (2 sleeps: 1 broker→C, 1 poll→B)

**Files:** Modify `crates/integration-tests/tests/admin_log_dirs_round_trip.rs`

- [ ] **Step 1: Swap the partition-materialize loop for awaiters (Recipe C)**

At line ~50, replace the `for _ in 0..200 { if handle.has_partition("t", 0) && handle.has_partition("t", 1) { break } sleep }` loop with:

```rust
handle.wait_until_partition_present("t", 0).await;
handle.wait_until_partition_present("t", 1).await;
```

- [ ] **Step 2: Bound the log-dir-move completion loop (Recipe B)**

At line ~125, wrap the move-completion loop (`!any_future && current_in_target == vec![0, 1]`) in `timeout(30s, …)` → expect `"AlterReplicaLogDirs move completed within 30s"`.

- [ ] **Step 3: Build + stress-run**

Run: `cargo test -p crabka-integration-tests --test admin_log_dirs_round_trip` then the 10× stress loop. Expect zero flakes.

- [ ] **Step 4: Commit**

```bash
git add crates/integration-tests/tests/admin_log_dirs_round_trip.rs
git commit -m "test(integration): de-flake admin_log_dirs waits (BrokerHandle awaiter + bounded move poll)"
```

---

## Task 6: Final verification + fmt/clippy

**Files:** none (verification only)

- [ ] **Step 1: Confirm no fixed-sleep-then-assert remains**

Run: `git grep -nE "sleep\(" -- crates/integration-tests/tests/`
Expected: every remaining `sleep(` is either (a) the inner retry sleep inside a `tokio::time::timeout(...)` block, or (b) one of the 2 annotated KEEP sleeps in `consumer_share_consumer.rs`. No bare fixed-sleep-before-assert.

- [ ] **Step 2: fmt + clippy**

```bash
cargo +nightly fmt -p crabka-integration-tests
cargo +nightly fmt -p crabka-integration-tests -- --check   # expect exit 0
cargo clippy -p crabka-integration-tests --tests -- -D warnings   # expect exit 0
```

- [ ] **Step 3: Full integration-tests run**

Run: `cargo test -p crabka-integration-tests` — expect all pass. (If share-consumer is slow on Windows, run it separately with a generous timeout.)

- [ ] **Step 4: (No commit)** Tasks 1–5 already committed. Proceed to finishing-a-development-branch.

---

## Self-Review

**Spec coverage:**
- Pattern A (fixed-sleep → await/delete) → Task 1 Step 2. ✓
- Pattern B (unbounded loop → `timeout`) → Tasks 1.1/1.3, 2.1, 3.3, 4.1, 5.2. ✓
- Pattern C (materialize loop → `BrokerHandle` awaiter) → Tasks 1.4, 2.2, 3.1/3.2, 5.1. ✓
- Keep list (2 timing tests) → Task 3 Step 4 (annotate, don't change). ✓
- Metadata-race produce-retry handling → Tasks 1.4, 2.2, 3.2 (await-partition-present + bounded backstop). ✓
- Test-only / no shared module / reuse existing awaiters → Architecture + recipes. ✓
- Verification (stress ≥10×, fmt, clippy) → per-task Step + Task 6. ✓
- All 7 broker + 20 poll + 2 keep = 29 accounted for: cooperative 2C/9B(+helpers), consumer 5B, share 4C/3B/2keep, admin_round_trip 2B, log_dirs 1C/1B. ✓

**Placeholder scan:** The per-site transformations reference exact line numbers + the existing break-condition (already in the file) + a named recipe with full template code. The metadata field-accessor in Task 4 is the one spot that adapts to the real `Metadata` type — flagged explicitly with how to derive it (the existing post-sleep assertion). Not a hidden TODO.

**Type consistency:** `wait_until_partition_present(&self, &str, i32)` and `wait_for_share_state_summary(&self, &str, Uuid, i32)` match broker.rs:1131/:450. The bounded helper signatures match their originals (only the body is wrapped). `expect`/`unwrap_or_else(panic)` on `timeout` results is consistent throughout.
