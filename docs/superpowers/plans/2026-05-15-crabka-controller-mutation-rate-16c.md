# Slice 16c: controller_mutation_rate (KIP-599) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Implement KIP-599 `controller_mutation_rate` — a partition-mutations-per-second quota enforced on `CreateTopics`, `CreatePartitions`, `DeleteTopics`. Throttle delays via `tokio::time::sleep` (KIP-257 idiom). User + client-id entity scopes only (no IP per KIP-599).

**Architecture:** No new metadata records — `ClientQuotaRecord` already carries arbitrary `(entity, key, value)` tuples; `lookup_quota_with_key` (slice 16) handles the 8-priority lookup directly. Add a shared helper `consume_controller_mutation_quota` in `crates/broker/src/quota/controller_mutation.rs`. Each of the three handlers counts mutations BEFORE running its existing logic (so invalid requests still count), calls the helper after the response is built, sets `throttle_time_ms`, and `tokio::time::sleep`s before encoding.

**Tech Stack:** Rust 1.95.0; reuses slice 16's `QuotaBuckets`, `lookup_quota_with_key`, refresh task. Wire surfaces unchanged.

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-controller-mutation-rate-16c-design.md`](../specs/2026-05-15-crabka-controller-mutation-rate-16c-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/controller-mutation-rate-16c` already created with spec committed at `4c7d33b` (stacked on `feature/ip-quotas-16b`).

---

## File structure

```
crates/broker/src/
├── quota/
│   ├── controller_mutation.rs       # NEW — consume_controller_mutation_quota + 3 unit tests
│   └── mod.rs                       # MODIFIED — re-export
├── handlers/
│   ├── alter_client_quotas.rs       # MODIFIED — KNOWN_QUOTA_KEYS += "controller_mutation_rate" + 1 test
│   ├── create_topics.rs             # MODIFIED — mutation count + throttle hook
│   ├── create_partitions.rs         # MODIFIED — mutation count + throttle hook
│   └── delete_topics.rs             # MODIFIED — mutation count + throttle hook

crates/broker/tests/
├── controller_mutation_quota.rs     # NEW — 3 broker integration tests
└── jvm_acceptance.rs                # MODIFIED — 1 new JVM test
```

7 tasks across 5 batches.

---

## Batch 1 — Helper + validator (parallel: T1, T2)

### Task 1: `consume_controller_mutation_quota` helper + 3 unit tests

**Files:**
- Create: `crates/broker/src/quota/controller_mutation.rs`
- Modify: `crates/broker/src/quota/mod.rs` (one append)

- [ ] **Step 1: Write the module**

```rust
//! KIP-599 controller_mutation_rate helper. Called from CreateTopics,
//! CreatePartitions, DeleteTopics handlers after response assembly.

use std::time::Duration;

use crabka_metadata::MetadataImage;

use super::buckets::QuotaBuckets;
use super::lookup::lookup_quota_with_key;

/// Consume `mutations` from the controller_mutation_rate bucket for
/// `(principal, client_id)`. Returns the throttle delay to apply
/// before sending the response. `Duration::ZERO` if no quota
/// configured, no overage, or `mutations == 0`. Capped at 1 second.
#[must_use]
pub fn consume_controller_mutation_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    mutations: u64,
) -> Duration {
    if mutations == 0 {
        return Duration::ZERO;
    }
    let Some((entity_key, rate)) =
        lookup_quota_with_key(image, principal, client_id, "controller_mutation_rate")
    else {
        return Duration::ZERO;
    };
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let initial_rate = rate as u64;
    let bucket =
        buckets.get_or_create("controller_mutation_rate", &entity_key, initial_rate);
    let granted = bucket.try_consume(mutations);
    if granted >= mutations {
        return Duration::ZERO;
    }
    let overage = mutations - granted;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
    let delay_micros = ((overage as f64 / rate) * 1_000_000.0) as u64;
    Duration::from_micros(delay_micros).min(Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|(t, n)| QuotaEntity {
                    entity_type: t.into(),
                    entity_name: n.map(Into::into),
                })
                .collect(),
            config_key: "controller_mutation_rate".into(),
            config_value: Some(rate),
        }));
        img
    }

    #[test]
    fn zero_mutations_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 0);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn under_rate_returns_zero_delay() {
        // rate=10/sec, burst capacity=10 (one second of capacity).
        // 5 mutations consumed → bucket has 5 left → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 5);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=1/sec, burst=1; 100 mutations → overage 99 → delay 99s
        // → capped at 1s.
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 100);
        assert_eq!(delay, Duration::from_secs(1));
    }
}
```

- [ ] **Step 2: Append to `crates/broker/src/quota/mod.rs`**

After the existing `pub use ...` lines:

```rust
mod controller_mutation;
pub use controller_mutation::consume_controller_mutation_quota;
```

(Place in alphabetical position alongside `mod buckets;`, `mod lookup;`, `mod refresh;`.)

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib quota::controller_mutation
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 3 new tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/quota/
git commit -m "$(cat <<'EOF'
feat(broker): consume_controller_mutation_quota helper

KIP-599 helper. Looks up controller_mutation_rate via slice-16's
8-priority lookup; consumes `mutations` tokens from the bucket;
returns throttle delay (capped at 1s). Used by CreateTopics,
CreatePartitions, DeleteTopics in tasks 3/4/5.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Validator extension + 1 unit test

**Files:**
- Modify: `crates/broker/src/handlers/alter_client_quotas.rs`

- [ ] **Step 1: Add `"controller_mutation_rate"` to `KNOWN_QUOTA_KEYS`**

Find the existing `KNOWN_QUOTA_KEYS` constant (slice 16/16b added the others). Extend:

```rust
const KNOWN_QUOTA_KEYS: &[&str] = &[
    "producer_byte_rate",
    "consumer_byte_rate",
    "request_percentage",
    "connection_creation_rate",
    "controller_mutation_rate", // KIP-599 (slice 16c)
];
```

- [ ] **Step 2: Append 1 unit test**

In the existing `#[cfg(test)] mod tests`, append:

```rust
    #[test]
    fn controller_mutation_rate_key_accepted() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("controller_mutation_rate", 2.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert_eq!(records.len(), 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!("wrong variant");
        };
        assert_eq!(r.config_key, "controller_mutation_rate");
        assert_eq!(r.config_value, Some(2.0));
    }
```

(The `entry` test helper exists from slice 16 T5.)

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib alter_client_quotas
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 1 new test PASS + slice 16/16b's existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/handlers/alter_client_quotas.rs
git commit -m "$(cat <<'EOF'
feat(broker): accept controller_mutation_rate in AlterClientQuotas

KIP-599 quota key added to the allowlist. No cross-validation with
entity_type — matches slice 16/16b's permissive validator. The key
on (ip) is accepted but never enforced.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 2 — Handler hooks (parallel: T3, T4, T5)

All three tasks consume `consume_controller_mutation_quota` (T1) and need the validator allowlist (T2). All three modify different files, so they parallelize.

### Task 3: `CreateTopics` enforcement hook

**Files:**
- Modify: `crates/broker/src/handlers/create_topics.rs`

- [ ] **Step 1: Read the existing handler structure**

```
rg "fn handle\|req\.topics\|throttle_time_ms\|principal\|client_id" crates/broker/src/handlers/create_topics.rs
```

Identify:
- The decode point (where `req` becomes available).
- The response variable name and where it's built.
- Where `principal` and `client_id` reach the handler.

- [ ] **Step 2: Count mutations after decode, before authorize**

Insert immediately after `req` is decoded:

```rust
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
let mutation_count: u64 = req.topics.iter()
    .map(|t| t.num_partitions.max(1) as u64)
    .sum();
```

(`num_partitions == -1` → "use cluster default" → counted as 1 for accounting. Conservative under-count; operators set throttles loosely.)

- [ ] **Step 3: Apply the throttle delay after response assembly, before encode**

Find where `response` is fully built but before encoding (slice 16 T9 pattern). Insert:

```rust
use std::time::Duration;

let principal_name = principal.name.as_str();
let delay = crate::quota::consume_controller_mutation_quota(
    &image,
    &broker.quota_buckets,
    principal_name,
    "", // client_id not threaded through HandlerTable — see slice 16 known limitation
    mutation_count,
);
response.throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
if delay > Duration::ZERO {
    tokio::time::sleep(delay).await;
}
```

(The `image` and `broker.quota_buckets` access patterns match slice 16 T9's Produce hook. If the handler doesn't already have an `image` variable in scope, hoist via `let image = broker.controller.current_image();` near the top.)

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib create_topics
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass. No quota is configured in pre-slice-16c tests, so the hook is a no-op.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/create_topics.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-599 controller_mutation_rate on CreateTopics

Mutation count = sum of num_partitions across all requested topics
(num_partitions == -1 treated as 1 for accounting). Throttle applied
after response assembly, before encode. client_id="" — slice-16
HandlerTable signature limitation; (user)-only quotas work.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `CreatePartitions` enforcement hook

**Files:**
- Modify: `crates/broker/src/handlers/create_partitions.rs`

- [ ] **Step 1: Read the existing handler**

```
rg "fn handle\|req\.topics\|count\|throttle_time_ms" crates/broker/src/handlers/create_partitions.rs
```

Identify the request field that carries `(topic_name, target_partition_count)` pairs.

- [ ] **Step 2: Count mutations after decode**

```rust
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
let mutation_count: u64 = {
    let image = broker.controller.current_image();
    req.topics.iter()
        .map(|t| {
            let current: i32 = i32::try_from(image.partitions_of(&t.name).count()).unwrap_or(i32::MAX);
            (t.count - current).max(0) as u64
        })
        .sum()
};
```

Verify the field name `t.count` against the generated owned-type (it may be `t.partition_count` or similar — check `crates/protocol/generated/CreatePartitionsRequest.owned.rs`).

For nonexistent topics, `partitions_of(&t.name)` returns an empty iterator → `current = 0` → if `t.count > 0`, the full count is mutations. Handler will reject the request later; the accounting still applies (intentional per spec — bad-faith requests count).

- [ ] **Step 3: Apply the throttle delay after response assembly, before encode**

Same pattern as T3:

```rust
use std::time::Duration;
let principal_name = principal.name.as_str();
let delay = crate::quota::consume_controller_mutation_quota(
    &image,
    &broker.quota_buckets,
    principal_name,
    "",
    mutation_count,
);
response.throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
if delay > Duration::ZERO {
    tokio::time::sleep(delay).await;
}
```

(If `image` is hoisted in Step 2, reuse the same binding; don't re-call `current_image()`.)

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib create_partitions
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/create_partitions.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-599 controller_mutation_rate on CreatePartitions

Mutation count = sum of (target_count - current_count) across all
requested topics. Nonexistent topics count their full target (handler
rejects later; accounting still applies).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: `DeleteTopics` enforcement hook

**Files:**
- Modify: `crates/broker/src/handlers/delete_topics.rs`

- [ ] **Step 1: Read the existing handler**

```
rg "fn handle\|topic_names\|topics\|throttle_time_ms" crates/broker/src/handlers/delete_topics.rs
```

`DeleteTopicsRequest` shape depends on version — older versions use `topic_names: Vec<String>`, newer use `topics: Vec<DeleteTopicState>` with `name: Option<String>` and/or `topic_id: Uuid`. Adapt to whatever the handler currently destructures.

- [ ] **Step 2: Count mutations after decode**

```rust
let mutation_count: u64 = {
    let image = broker.controller.current_image();
    // Adapt to the actual request field (topic_names vs topics with name/id).
    // Pseudo-code for the common case where req.topic_names: Vec<String> exists:
    req.topic_names.iter()
        .map(|name| image.partitions_of(name).count() as u64)
        .sum()
};
```

For the newer shape with `Vec<DeleteTopicState>`, resolve each entry's name (look up by topic_id in the image if name is None) then count partitions. Match the existing handler's resolution path — it already does this for the actual delete operation.

Nonexistent topics → 0 mutations.

- [ ] **Step 3: Apply the throttle delay**

Same pattern as T3/T4. Set `response.throttle_time_ms`; `tokio::time::sleep(delay).await` before encode.

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib delete_topics
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/delete_topics.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-599 controller_mutation_rate on DeleteTopics

Mutation count = sum of partition counts across all topics being
deleted (image lookup). Nonexistent topics count 0.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 3 — Integration tests (sequential: T6)

### Task 6: 3 broker integration tests

**Files:**
- Create: `crates/broker/tests/controller_mutation_quota.rs`

- [ ] **Step 1: File scaffold + copied helpers**

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]
```

Copy from slice 16's `tests/client_quotas.rs`:
- `round_trip`
- `sasl_plain_authenticate`
- `start_single_broker_sasl_plaintext_with_users`
- `drive_alter_client_quotas_sasl`

Plus two new wire drivers:

```rust
async fn drive_create_topics_sasl(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
    partitions: i32,
) -> (i32 /* throttle_time_ms */, i16 /* per-topic error_code */)
```

```rust
async fn drive_delete_topics_sasl(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
) -> (i32 /* throttle_time_ms */, i16 /* per-topic error_code */)
```

Use the generated `CreateTopicsRequest` / `DeleteTopicsRequest` owned types; send via `round_trip` to api_keys 19 / 20 respectively (verify constants from the generated modules — they may differ).

- [ ] **Step 2: Test 1 — `controller_mutation_rate_throttles_create_topics`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_create_topics() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    ).await;

    // Seed unrelated ACL to disable slice-13 compat shim, plus grant alice
    // Create on Cluster (CreateTopics needs Cluster Create or Topic Create with prefix).
    let admin_acl = crabka_metadata::MetadataRecord::V1AccessControlEntry(
        crabka_metadata::AclEntry {
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: "kafka-cluster".into(),
            pattern_type: crabka_metadata::PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: crabka_metadata::AclOperation::Create,
            permission_type: crabka_metadata::PermissionType::Allow,
        },
    );
    handle.submit_metadata_record_for_test(admin_acl).await.expect("seed ACL");

    // Set controller_mutation_rate=2.0 for (user=alice).
    let alter = drive_alter_client_quotas_sasl(
        addr, "admin", "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("controller_mutation_rate".into(), 2.0, false)],
        )],
        false,
    ).await;
    assert_eq!(alter[0].1, 0, "alter should succeed");

    // Wait for refresh task to pick up the rate.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Create topic with 10 partitions (mutations=10, burst=2, overage=8 → delay=4s, capped at 1s).
    let started = std::time::Instant::now();
    let (throttle_ms, err_code) = drive_create_topics_sasl(
        addr, "alice", "alice-secret", "throttled-topic", 10,
    ).await;
    let elapsed = started.elapsed();
    assert_eq!(err_code, 0, "create-topics should succeed (alice has Cluster Create ACL)");
    assert!(throttle_ms > 0, "expected throttle_time_ms > 0, got {throttle_ms}");
    assert!(
        elapsed >= std::time::Duration::from_millis(800),
        "expected >=800ms wall delay, got {elapsed:?}"
    );
}
```

- [ ] **Step 3: Test 2 — `unthrottled_create_topics_unaffected`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_create_topics_unaffected() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret")],
    ).await;
    // No controller_mutation_rate quota configured.

    let (throttle_ms, err_code) = drive_create_topics_sasl(
        addr, "admin", "admin-secret", "unthrottled-topic", 10,
    ).await;
    assert_eq!(err_code, 0);
    assert_eq!(throttle_ms, 0);
}
```

- [ ] **Step 4: Test 3 — `controller_mutation_rate_throttles_delete_topics`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_delete_topics() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    ).await;

    // Grant alice Cluster Create + Delete.
    for op in [crabka_metadata::AclOperation::Create, crabka_metadata::AclOperation::Delete] {
        let acl = crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Cluster,
                resource_name: "kafka-cluster".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: op,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        );
        handle.submit_metadata_record_for_test(acl).await.expect("seed ACL");
    }

    // Pre-create topic as admin (no quota for admin) with 10 partitions.
    let (_, ec) = drive_create_topics_sasl(addr, "admin", "admin-secret", "to-delete", 10).await;
    assert_eq!(ec, 0);

    // Now set the quota for alice and delete.
    let alter = drive_alter_client_quotas_sasl(
        addr, "admin", "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("controller_mutation_rate".into(), 2.0, false)],
        )],
        false,
    ).await;
    assert_eq!(alter[0].1, 0);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let (throttle_ms, err_code) = drive_delete_topics_sasl(
        addr, "alice", "alice-secret", "to-delete",
    ).await;
    assert_eq!(err_code, 0);
    assert!(throttle_ms > 0, "expected throttle_time_ms > 0, got {throttle_ms}");
}
```

- [ ] **Step 5: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test controller_mutation_quota -- --nocapture --test-threads=1"
```

Expected: 3 tests PASS.

- [ ] **Step 6: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/controller_mutation_quota.rs
git commit -m "$(cat <<'EOF'
test(broker): controller_mutation_quota throttle on CreateTopics + DeleteTopics

Three integration tests: throttled create with throttle_time_ms +
wall-clock proof, unthrottled baseline, throttled delete. Asserts on
throttle_time_ms in the response — slice-16 idiom.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 4 — JVM acceptance (sequential: T7)

### Task 7: JVM `kafka-configs --add-config controller_mutation_rate` round-trip

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

Pattern after slice 16 T13's `jvm_kafka_configs_alter_client_quota_end_to_end`.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_controller_mutation_rate_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (h1, _h2, _h3, _d1, _d2, _d3, _c1, _c2, _c3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN, ADMIN_PASS, &[(ALICE, ALICE_PASS)],
        ).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Alter — set controller_mutation_rate=2.0 for alice.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--alter",
            "--entity-type", "users", "--entity-name", ALICE,
            "--add-config", "controller_mutation_rate=2.0",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(out.status.success(), "alter failed: {}", String::from_utf8_lossy(&out.stderr));

    // Describe — slice 16 T13 found that --describe --entity-type users exits non-zero
    // due to DescribeUserScramCredentials side-call. Use std::process::Command directly
    // and assert on stdout.
    let desc = std::process::Command::new("docker")
        .args([
            "run", "--rm", "-v", &admin_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-configs", "--describe",
            "--entity-type", "users", "--entity-name", ALICE,
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ])
        .output().expect("spawn kafka-configs --describe");
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("controller_mutation_rate=2"),
        "expected quota in describe output: {stdout}"
    );

    // Delete.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--alter",
            "--entity-type", "users", "--entity-name", ALICE,
            "--delete-config", "controller_mutation_rate",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(out.status.success(), "delete failed: {}", String::from_utf8_lossy(&out.stderr));

    // Confirm cleared from image.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some(ALICE.into()))];
        if img.client_quotas().get(&key)
            .and_then(|m| m.get("controller_mutation_rate"))
            .is_none()
        {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("controller_mutation_rate not cleared after delete-config");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

**No wall-time enforcement test in the JVM acceptance.** `kafka-topics --create` is one request; max throttle = 1 second. The Rust integration test (T6 test 1) proves enforcement; this test proves the wire round-trip.

- [ ] **Step 2: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance jvm_kafka_configs_alter_controller_mutation_rate_end_to_end -- --ignored --nocapture --test-threads=1"
```

Expected: PASS in 30-60 seconds.

- [ ] **Step 3: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "$(cat <<'EOF'
test(jvm): kafka-configs --entity-type users controller_mutation_rate round-trip

Three-broker SASL/PLAINTEXT cluster; --alter + --describe (stdout
substring) + --delete-config on (user=alice) controller_mutation_rate.
No wall-time enforcement test — single kafka-topics --create is one
request, max throttle 1s. Rust integration test covers enforcement.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 5 — Sweep + docs + PR (sequential: T8)

### Task 8: Sweep + README + STATUS + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Full local sweep**

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
```

All clean.

- [ ] **Step 2: Update the Quotas matrix in `README.md`**

Slice 16b flipped the IP row; slice 16c flips the controller mutation row. Current state (from slice 16b):

```markdown
| Controller mutation rate (KIP-599) | ❌ |
```

Change to:

```markdown
| Controller mutation rate (KIP-599) | ✅ |
```

- [ ] **Step 3: Append to `STATUS.md`**

```markdown
## Slice 16c — controller_mutation_rate (2026-05-15)

- KIP-599 `controller_mutation_rate` quota type — partition-mutations-per-second; user / client-id entity scopes (no IP per KIP-599).
- Validator extension: `KNOWN_QUOTA_KEYS += "controller_mutation_rate"` in `alter_client_quotas.rs`. 1 unit test.
- New helper `consume_controller_mutation_quota` in `crates/broker/src/quota/controller_mutation.rs`. Reuses slice-16's `lookup_quota_with_key` (8-priority) and `QuotaBuckets`. 3 unit tests.
- Enforcement on three handlers:
  - `CreateTopics` — mutation count = sum of `num_partitions` across all topics (`-1` → 1 for accounting).
  - `CreatePartitions` — count = sum of `(target_count - current_partition_count)` across topics; nonexistent topics count their full target.
  - `DeleteTopics` — count = sum of partition counts (image lookup); nonexistent topics count 0.
- Counted BEFORE handler runs (so invalid requests still count — bad-faith clients can't escape the throttle by spamming malformed RPCs).
- Throttle delay set on `throttle_time_ms` + `tokio::time::sleep` before encoding response. Capped at 1 second per slice-16 convention.
- 3 broker integration tests (`tests/controller_mutation_quota.rs`): throttled CreateTopics with wall-clock proof, unthrottled baseline, throttled DeleteTopics.
- 1 new JVM acceptance test.
- **Inherits slice 16 known limitations:**
  - `client_id` not threaded through `HandlerTable` — `(user, client-id)` tuple quotas don't fire from these handlers; `(user)`-only quotas work. Closing requires the slice-16 cleanup work.
  - Per-entity bucket cache grows unbounded over broker's lifetime.
- Out of scope: IP entity (KIP-599 doesn't apply to IP); other admin operations (ACL CRUD, IncrementalAlterConfigs, AlterPartitionReassignments — KIP-599 limits to topic/partition CRUD).
```

- [ ] **Step 4: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "$(cat <<'EOF'
docs(slice-16c): README matrix + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Push + open PR**

If 16b is still open, this PR's base is `main` and it includes 16b's commits too (stacked). After 16b merges to main, rebase 16c onto the new main; the duplicate commits drop out.

```
git push -u origin feature/controller-mutation-rate-16c
gh pr create --base main --head feature/controller-mutation-rate-16c \
  --title "Slice 16c: controller_mutation_rate (KIP-599)" \
  --body "$(cat <<'EOF'
## Summary

KIP-599 \`controller_mutation_rate\` — partition-mutations-per-second quota on the topic/partition CRUD handlers:

1. **Three handlers enforce** — \`CreateTopics\` (sum of num_partitions), \`CreatePartitions\` (sum of new_count - current), \`DeleteTopics\` (sum of partition counts from image).
2. **Count before run** — invalid requests still count, so bad-faith clients can't escape the throttle by sending malformed RPCs.
3. **Throttle delay** via \`tokio::time::sleep\` before encoding response; \`throttle_time_ms\` populated. Capped at 1s per slice-16 convention.

JVM \`kafka-configs --alter --entity-type users --entity-name alice --add-config controller_mutation_rate=...\` round-trips end-to-end.

## Verified

- 4 new unit tests (helper 3, validator 1).
- 3 broker integration tests in \`tests/controller_mutation_quota.rs\`.
- 1 new JVM acceptance test.
- Workspace \`cargo fmt --check\`, \`cargo clippy --workspace --all-targets -- -D warnings\`, \`cargo test --workspace\` all green.

## Inherits slice 16 known limitations

- \`client_id\` not threaded through \`HandlerTable\` — \`(user, client-id)\` tuple quotas don't fire; \`(user)\` only.
- Per-entity bucket cache grows unbounded.

## Out of scope

- IP entity for this quota (KIP-599 only applies to user/client-id).
- Other admin operations (ACL CRUD, IncrementalAlterConfigs, AlterPartitionReassignments) — KIP-599 limits to topic/partition CRUD.

## Plan / spec

- Spec: \`docs/superpowers/specs/2026-05-15-crabka-controller-mutation-rate-16c-design.md\`
- Plan: \`docs/superpowers/plans/2026-05-15-crabka-controller-mutation-rate-16c.md\` (7 tasks across 5 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Capture PR URL** and return.

---

## Notes for the executing agent

1. **Branch is stacked on 16b** — if 16b hasn't merged when 16c work begins, the branch shows 16b's commits in `git log`. That's expected. After 16b merges, rebase 16c onto the new main; duplicates drop out.

2. **CLAUDE.md compatibility rule** — no metadata schema changes in this slice. Reuses `ClientQuotaRecord`.

3. **Parallel batches** (per CLAUDE.md):
   - **B1 (T1 + T2)**: T1 touches `quota/`; T2 touches `handlers/alter_client_quotas.rs`. Disjoint.
   - **B2 (T3 + T4 + T5)**: three handler hooks in different files. Disjoint. All depend on T1 (helper) being committed. T2 also needs to land first (for the validator allowlist to accept the key — though the handler hooks themselves don't reference the key string from the validator's perspective).
   - **B3 (T6)**: integration tests. Sequential (depends on all handler hooks).
   - **B4 (T7)**: JVM acceptance. Sequential.
   - **B5 (T8)**: sweep + PR. Sequential.

4. **`Principal::name` access** — slice 12's struct uses `principal.name` (String field), not `.name()`. Slice 16's helpers established `principal.name.as_str()`. Reuse.

5. **`client_id = ""`** — slice 16's known limitation propagates here. Each handler passes `""` to the helper. Documented in STATUS as continued limitation.

6. **`response.throttle_time_ms` field** exists on all three response types (`CreateTopicsResponse`, `CreatePartitionsResponse`, `DeleteTopicsResponse`). Verify field name via the generated owned types if unsure.

7. **Image lookup in handlers** — `broker.controller.current_image()` returns `Arc<MetadataImage>`. Cheap to call; called once per request is fine. Hoist into a local if both the mutation count and the helper need it.

8. **Integration tests need authorize ACLs** for alice on Cluster Create/Delete since slice 13 added authorization to topic CRUD. Tests T6.2 + T6.4 seed the ACL via `submit_metadata_record_for_test`.
