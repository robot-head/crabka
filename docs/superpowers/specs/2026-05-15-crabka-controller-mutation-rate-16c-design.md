# Slice 16c: `controller_mutation_rate` (KIP-599) — Design

**Status:** Approved 2026-05-15.

**Goal:** Implement KIP-599 `controller_mutation_rate` — a partition-mutation-per-second quota enforced on `CreateTopics`, `CreatePartitions`, and `DeleteTopics` handlers. Throttle delays via `tokio::time::sleep` (KIP-257 idiom).

**Branch:** `feature/controller-mutation-rate-16c`, stacked on top of `feature/ip-quotas-16b`. When 16b merges, rebase onto main.

**Out of scope:**
- IP entity for this quota (KIP-599 only applies to user/client-id)
- Other admin operations (ACL CRUD, IncrementalAlterConfigs, AlterPartitionReassignments, etc.)
- Slice 16 known follow-ups (client_id through HandlerTable still applies — `(user, client-id)` tuples don't fire from these handlers either)

---

## 1. Scope

### In

- `controller_mutation_rate` quota type — units of "partition mutations per second" (`f64`)
- Validator extension in `alter_client_quotas.rs`: add `"controller_mutation_rate"` to `KNOWN_QUOTA_KEYS`
- Lookup via existing slice-16 `lookup_quota_with_key` (8-priority user/client-id; NOT slice-16b ip-only lookup)
- Enforcement on three handlers:
  - **`CreateTopics`** — mutation count = sum of partitions across requested topics (handle `num_partitions == -1` by treating as 1 for accounting; the cluster-default resolution happens in the handler)
  - **`CreatePartitions`** — count = sum of `(new_count - current_count)` across requested topics (image lookup for current; nonexistent topics count 0)
  - **`DeleteTopics`** — count = sum of partition counts for each topic being deleted (image lookup; nonexistent topics count 0)
- **Count BEFORE handler runs** — even invalid requests count, so clients can't escape the throttle by sending malformed RPCs
- Throttle delay applied AFTER response built, BEFORE encoding (set `throttle_time_ms` + `tokio::time::sleep`). Capped at 1 second.
- JVM acceptance: `kafka-configs --alter --entity-type users --entity-name alice --add-config controller_mutation_rate=2.0` round-trip

### Not in

- IP entity for `controller_mutation_rate` (accepted by validator per slice-16 permissive policy but never enforced)
- Other admin operations
- `controller_mutation_rate` on `(ip)` enforcement
- Closing the slice-16 `client_id`-not-threaded gap (separate cleanup slice)

---

## 2. Storage, validation, lookup

**No new storage.** `ClientQuotaRecord` (slice 16 T1) already carries arbitrary `(entity, key, f64)` tuples. `MetadataImage::client_quotas` (slice 16 T1) already stores them. No new metadata records, no new image accessors.

**No new lookup function.** Slice 16's `lookup_quota_with_key` (8-priority user/client-id walk) handles `controller_mutation_rate` lookups directly — the function is generic over the quota key string.

### Validator extension

`crates/broker/src/handlers/alter_client_quotas.rs`:

```rust
const KNOWN_QUOTA_KEYS: &[&str] = &[
    "producer_byte_rate",
    "consumer_byte_rate",
    "request_percentage",
    "connection_creation_rate",
    "controller_mutation_rate",   // KIP-599 (slice 16c)
];
```

**No entity/key cross-validation.** `controller_mutation_rate` on `(ip)` is accepted but never enforced (no controller-side IP-aware enforcement). Matches slice 16/16b permissive stance.

---

## 3. Enforcement on the three handlers

### Helper — `consume_controller_mutation_quota`

Co-located in a new `crates/broker/src/quota/controller_mutation.rs` to avoid duplicating it across three handler files:

```rust
//! KIP-599 controller_mutation_rate helper. Called from CreateTopics,
//! CreatePartitions, DeleteTopics handlers.

use std::time::Duration;

use crabka_metadata::MetadataImage;

use super::buckets::QuotaBuckets;
use super::lookup::lookup_quota_with_key;

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
    let Some((entity_key, rate)) = lookup_quota_with_key(
        image, principal, client_id, "controller_mutation_rate"
    ) else {
        return Duration::ZERO;
    };
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    let bucket = buckets.get_or_create("controller_mutation_rate", &entity_key, rate as u64);
    let granted = bucket.try_consume(mutations);
    if granted >= mutations {
        return Duration::ZERO;
    }
    let overage = mutations - granted;
    let delay_secs = overage as f64 / rate;
    Duration::from_micros((delay_secs * 1_000_000.0) as u64).min(Duration::from_secs(1))
}
```

Re-exported via `crates/broker/src/quota/mod.rs`.

### Per-handler integration

Each handler:

1. Decode request.
2. **Count mutations** (per-handler logic — sections below).
3. Run existing authorize + handler logic.
4. After response is built, before encoding:
   ```rust
   let delay = crate::quota::consume_controller_mutation_quota(
       &image, &broker.quota_buckets,
       principal_name, &client_id,
       mutation_count,
   );
   response.throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
   if delay > Duration::ZERO {
       tokio::time::sleep(delay).await;
   }
   ```

### `CreateTopics`

`crates/broker/src/handlers/create_topics.rs`. Count immediately after decode:

```rust
let mutation_count: u64 = req.topics.iter()
    .map(|t| t.num_partitions.max(1) as u64)
    .sum();
```

Treats `num_partitions == -1` (use cluster default) as 1 for accounting. Slight under-count on the default path is acceptable — operators set throttles conservatively.

### `CreatePartitions`

`crates/broker/src/handlers/create_partitions.rs`. Count after decode:

```rust
let mutation_count: u64 = req.topics.iter()
    .map(|t| {
        let current = image.partitions_of(&t.name).count() as i32;
        ((t.count - current).max(0)) as u64
    })
    .sum();
```

Nonexistent topics → `partitions_of` returns empty iterator → `current = 0` → if request's count is positive, all of it counts as mutations. The handler will reject the request later but the accounting still applies (intentional — bad-faith client requests count).

### `DeleteTopics`

`crates/broker/src/handlers/delete_topics.rs`. Count after decode:

```rust
let mutation_count: u64 = req.topic_names.iter()
    .map(|name| image.partitions_of(name).count() as u64)
    .sum();
```

Nonexistent topics → 0 mutations.

(If the request uses `req.topics` with `Vec<DeleteTopicState>` instead of `topic_names`, adapt the field access — check the generated owned type.)

### Counting timing — load-bearing

**Count BEFORE authorize.** A non-super-user spamming CreateTopics with bad topic names should still get throttled. Pattern: extract `mutation_count` right after the request decode succeeds, then run the rest of the handler. The throttle accounting consumes from the bucket regardless of whether the request succeeds.

### Response field

All three response types (`CreateTopicsResponse`, `CreatePartitionsResponse`, `DeleteTopicsResponse`) have `throttle_time_ms: i32`. Slice-16 idiom: cast `delay.as_millis()` via `i32::try_from(...).unwrap_or(i32::MAX)`.

---

## 4. Testing

### Unit tests (~4 new)

**`crates/broker/src/quota/controller_mutation.rs` (3 tests):**
- `zero_mutations_returns_zero_delay`
- `under_rate_returns_zero_delay` — mutations <= rate, no overage
- `overage_returns_capped_delay` — mutations >> rate, expect 1-second cap

**`crates/broker/src/handlers/alter_client_quotas.rs` (1 test):**
- `controller_mutation_rate_key_accepted` — `KNOWN_QUOTA_KEYS` allowlist test

### Broker integration tests (`crates/broker/tests/controller_mutation_quota.rs`, 3 tests)

1. **`controller_mutation_rate_throttles_create_topics`** — single-broker SASL/PLAIN; set `(user=alice) controller_mutation_rate=2.0`; alice creates 1 topic with 10 partitions; assert `throttle_time_ms > 0` AND wall-clock delay observed (≥800ms).

2. **`unthrottled_create_topics_unaffected`** — same setup, no quota configured; create topic with 10 partitions; assert `throttle_time_ms == 0`.

3. **`controller_mutation_rate_throttles_delete_topics`** — set `(user=alice) controller_mutation_rate=2.0`; pre-create topic with 10 partitions; alice deletes it; assert `throttle_time_ms > 0`.

Skip `CreatePartitions` integration test; the unit helper + the two integration tests above prove the pattern.

### JVM acceptance (1 new test)

`jvm_kafka_configs_alter_controller_mutation_rate_end_to_end` — `#[ignore]`-tagged, WSL:

1. 3-broker SASL/PLAINTEXT cluster.
2. `kafka-configs --alter --entity-type users --entity-name alice --add-config controller_mutation_rate=2.0` → exit 0.
3. `kafka-configs --describe --entity-type users --entity-name alice` → stdout contains `controller_mutation_rate=2`.
4. `kafka-configs --alter --entity-type users --entity-name alice --delete-config controller_mutation_rate` → exit 0.
5. Confirm cleared from image (poll).

**No wall-time enforcement in the JVM test.** `kafka-topics --create` is a single request; max throttle delay = 1 second. The Rust integration test 1 covers enforcement; the JVM test proves the wire round-trip.

---

## Slice 16 `client_id` gap inherits

Slice 16's known limitation — `client_id` is hard-coded to `""` in handler signatures — applies here too. The CreateTopics/CreatePartitions/DeleteTopics handlers get `client_id = ""` from dispatch. So `(user, client-id)` tuple quotas don't fire from these handlers; `(user)`-only quotas work. Documented in STATUS as continued limitation; will close when slice-16 follow-up plumbs `client_id` through `HandlerTable`.
