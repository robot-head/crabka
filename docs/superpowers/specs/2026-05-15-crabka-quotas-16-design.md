# Slice 16: Client quotas (KIP-13 + KIP-124 + KIP-257) — Design

**Status:** Approved 2026-05-15.

**Goal:** Implement Kafka client-side quotas — `AlterClientQuotas` (api_key 49) and `DescribeClientQuotas` (api_key 48). Three quota types (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`) with four entity scopes (user / client-id / (user, client-id) / default). Enforced on Produce, Fetch, and per-request dispatch via the existing TokenBucket primitive from slice 15b. KIP-257 throttle delays — server-side sleep before sending the response.

**Out of scope:**
- `ip` entity type (slice 16b)
- `connection_creation_rate` KIP-612 (slice 16b)
- `controller_mutation_rate` KIP-599 (slice 16c)
- Static-config quota defaults via `BrokerConfig` — runtime AlterClientQuotas only

---

## 1. Scope

### In

- `AlterClientQuotas` (api_key 49, v0–1) — set/remove quota values per entity tuple
- `DescribeClientQuotas` (api_key 48, v0–1) — filter + read quotas
- Three quota types:
  - `producer_byte_rate` — bytes/sec; KIP-13; throttles Produce
  - `consumer_byte_rate` — bytes/sec; KIP-13; throttles Fetch (consumer fetches, `replica_id < 0`)
  - `request_percentage` — % of one core; KIP-124; throttles per-request CPU time
- Four entity scopes (KIP-546 subset):
  - `(user)` — keyed by authenticated principal name
  - `(client-id)` — keyed by request `client_id` header
  - `(user, client-id)` — tuple, most specific
  - `<default>` — entity_name=null catches every name within that type
- Kafka's entity precedence lookup (8 priority levels, first match wins)
- KIP-257 throttle delays — populate `throttle_time_ms` in Produce/Fetch responses; server-side `tokio::time::sleep` before write
- New metadata record `ClientQuotaRecord` + `MetadataImage::client_quotas` map + per-broker `QuotaBuckets` cache with image-driven refresh
- JVM acceptance: `kafka-configs --alter --entity-type users --entity-name alice --add-config 'producer_byte_rate=1024'` round-trip with producer throttle verification

### Not in (deferred)

- `ip` entity type + `connection_creation_rate` (slice 16b)
- `controller_mutation_rate` (slice 16c)
- Static `BrokerConfig` quota defaults — runtime AlterClientQuotas only
- Routing `throttle_time_ms` through admin/coordinator response types — only Produce + Fetch surface the value in slice 16

### Wire shapes — confirmed

`AlterClientQuotasRequest` (v0–1, flex from v1):
- `entries: Vec<EntryData { entity: Vec<EntityData { entity_type, entity_name: Option<String> }>, ops: Vec<OpData { key, value: f64, remove: bool }> }>`
- `validate_only: bool`

`DescribeClientQuotasRequest` (v0–1, flex from v1):
- `components: Vec<ComponentData { entity_type, match_type: i8, match: Option<String> }>`
- `strict: bool`

`DescribeClientQuotasResponse`:
- `entries: Vec<EntryDataResponse { entity: Vec<EntityData>, values: Vec<ValueData { key, value: f64 }> }>`

---

## 2. Storage & entity matching

### `ClientQuotaRecord` metadata record

```rust
pub struct ClientQuotaRecord {
    /// Canonicalized entity tuple — sorted by entity_type alphabetically.
    pub entity: Vec<QuotaEntity>,
    pub config_key: String,
    pub config_value: Option<f64>,   // None = remove
}

pub struct QuotaEntity {
    pub entity_type: String,
    pub entity_name: Option<String>,
}

// MetadataRecord variant:
V1ClientQuota(ClientQuotaRecord),
```

### `MetadataImage` storage

```rust
pub type EntityKey = Vec<(String, Option<String>)>;

client_quotas: HashMap<EntityKey, HashMap<String, f64>>,

pub fn client_quotas(&self) -> &HashMap<EntityKey, HashMap<String, f64>> {
    &self.client_quotas
}
```

Canonicalization sorts tuples by `entity_type` alphabetically. `(user, client-id)` written by a client in either order collapses to the same key (since "client-id" < "user").

```rust
pub fn canonicalize(mut tuple: Vec<(String, Option<String>)>) -> EntityKey {
    tuple.sort_by(|a, b| a.0.cmp(&b.0));
    tuple
}
```

`MetadataImage::apply` arm for `V1ClientQuota`:

```rust
MetadataRecord::V1ClientQuota(rec) => {
    let key = canonicalize(
        rec.entity.iter().map(|e| (e.entity_type.clone(), e.entity_name.clone())).collect()
    );
    let configs = self.client_quotas.entry(key).or_default();
    match rec.config_value {
        Some(v) => { configs.insert(rec.config_key.clone(), v); }
        None => { configs.remove(&rec.config_key); }
    }
}
```

### Entity matching algorithm (Kafka precedence)

For an authenticated request with `(principal=alice, client_id=app1)` looking up `producer_byte_rate`, the broker tries candidates in this order; first match wins:

1. `[("client-id", Some("app1")), ("user", Some("alice"))]` — exact tuple
2. `[("client-id", Some("app1")), ("user", None)]` — alice's-client default
3. `[("client-id", None), ("user", Some("alice"))]` — alice's default
4. `[("client-id", None), ("user", None)]` — global tuple default
5. `[("user", Some("alice"))]` — alice only
6. `[("client-id", Some("app1"))]` — client-id only
7. `[("user", None)]` — default user
8. `[("client-id", None)]` — default client-id

All candidates are pre-sorted by entity_type — `"client-id" < "user"` alphabetically, so no extra canonicalize step needed at lookup time.

Implementation in `crates/broker/src/quota/lookup.rs::lookup_quota(image, principal, client_id, quota_key) -> Option<f64>`.

### Per-entity `TokenBucket` cache

```rust
pub struct QuotaBuckets {
    buckets: dashmap::DashMap<(String, EntityKey), Arc<TokenBucket>>,
}

impl QuotaBuckets {
    pub fn get_or_create(&self, quota_key: &str, entity_key: &EntityKey, rate: f64) -> Arc<TokenBucket> {
        self.buckets
            .entry((quota_key.to_string(), entity_key.clone()))
            .or_insert_with(|| {
                let b = Arc::new(TokenBucket::new());
                b.set_rate(rate as u64);
                b
            })
            .clone()
    }
}
```

When the metadata image changes (refresh task), iterate every existing bucket entry, look up the current rate via `lookup_quota`, and call `set_rate`. Buckets for entities no longer present in the image get set to rate 0 (effectively unthrottled — no eviction in slice 16; YAGNI for cache cleanup).

### Throttle delay semantics

- Compute `delay = overage / rate` (seconds).
- Cap at `Duration::from_secs(1)` — matches Kafka's hardcoded `quota.window.size.seconds` default. Slice 16 does not expose this as a configurable.
- The handler awaits `tokio::time::sleep(delay)` BEFORE writing the response to the socket. The dispatch loop is async, so the worker can interleave other requests during the delay.

Per Kafka semantics, byte-rate quotas DELAY the response without truncating it. (Contrast with slice 15b's leader-side inter-broker throttle, which truncates by dropping partition chunks.)

---

## 3. Enforcement

### Pattern: `consume_quota`

```rust
fn consume_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    quota_key: &str,
    consumed: u64,
) -> Duration {
    let Some(rate) = lookup_quota(image, principal, client_id, quota_key) else {
        return Duration::ZERO;
    };
    if rate <= 0.0 { return Duration::ZERO; }
    let entity_key = matched_entity_key(image, principal, client_id, quota_key);
    let bucket = buckets.get_or_create(quota_key, &entity_key, rate);
    let granted = bucket.try_consume(consumed);
    if granted >= consumed { return Duration::ZERO; }
    let overage = consumed - granted;
    let delay_secs = overage as f64 / rate;
    Duration::from_micros((delay_secs * 1_000_000.0) as u64).min(Duration::from_secs(1))
}
```

`matched_entity_key` lives alongside `lookup_quota` in `quota/lookup.rs`. Returns the same `EntityKey` that `lookup_quota` matched. Implementation refactor: extract the candidate-walk into a private helper returning `Option<(EntityKey, f64)>`; both `lookup_quota` and `matched_entity_key` (and a combined `lookup_quota_with_key`) delegate to it.

### Produce hot path (`producer_byte_rate`)

`crates/broker/src/handlers/produce.rs::handle`. After the handler computes the response, before encoding:

```rust
let total_bytes: u64 = req.topic_data.iter()
    .flat_map(|t| t.partition_data.iter())
    .map(|p| p.records.as_ref().map_or(0, |r| r.len() as u64))
    .sum();
let delay = consume_quota(
    &image, &broker.quota_buckets,
    principal_name, client_id,
    "producer_byte_rate",
    total_bytes,
);
response.throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
if delay > Duration::ZERO {
    tokio::time::sleep(delay).await;
}
```

`principal_name` from `auth.principal().name()` (slice 12). `client_id` from the request header (already extracted by the dispatcher).

### Fetch hot path (`consumer_byte_rate`)

`crates/broker/src/handlers/fetch.rs::handle`. After response chunks are assembled, gated on `req.replica_id < 0`:

```rust
if req.replica_id < 0 {
    let total_bytes: u64 = assembled_response_size(&responses);
    let delay = consume_quota(
        &image, &broker.quota_buckets,
        principal_name, client_id,
        "consumer_byte_rate",
        total_bytes,
    );
    fetch_response.throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
    if delay > Duration::ZERO {
        tokio::time::sleep(delay).await;
    }
}
```

The slice-15b inter-broker leader throttle (which fires when `replica_id >= 0`) is mutually exclusive with this consumer-quota path. Add a code comment so reviewers see they're not redundant.

### Per-request dispatch (`request_percentage`)

`crates/broker/src/network/dispatch.rs`. Wrap the handler dispatch:

```rust
let started = std::time::Instant::now();
let response = handle_request(...).await?;
let elapsed_micros = started.elapsed().as_micros() as u64;

// request_percentage units: 100% = 1 core = 1_000_000 μs/sec.
let rate_pct = lookup_quota(&image, principal, client_id, "request_percentage").unwrap_or(0.0);
let delay = if rate_pct > 0.0 {
    let rate_micros_per_sec = (rate_pct * 10_000.0) as u64;
    let entity_key = matched_entity_key_for_request_pct(&image, principal, client_id);
    let bucket = buckets.get_or_create("request_percentage", &entity_key, rate_micros_per_sec as f64);
    let granted = bucket.try_consume(elapsed_micros);
    if granted < elapsed_micros {
        let overage = elapsed_micros - granted;
        Duration::from_micros(overage * 1_000_000 / rate_micros_per_sec)
            .min(Duration::from_secs(1))
    } else {
        Duration::ZERO
    }
} else {
    Duration::ZERO
};

if delay > Duration::ZERO {
    tokio::time::sleep(delay).await;
}
write_response(stream, response).await?;
```

**Known limitation for slice 16:** `throttle_time_ms` on the response is only surfaced for Produce and Fetch. Other response types (admin RPCs, etc.) absorb the delay but don't communicate it back to the client. Closing this requires threading the throttle value through the handler trait, which is a refactor; deferred. Document in STATUS.

### `Broker` field + refresh task

`Broker` gains `pub quota_buckets: Arc<QuotaBuckets>`. `Broker::start` spawns `quota::refresh::run(controller, quota_buckets, shutdown)` after the slice-15b throttle refresh spawn. The refresh task mirrors slice 15b's shape — subscribes to `controller.watch_image()`, on every change iterates `buckets.buckets` and calls `set_rate` per the looked-up current value.

---

## 4. Handlers

### Files (new)

```
crates/broker/src/quota/
├── mod.rs        # re-exports
├── lookup.rs     # lookup_quota + matched_entity_key + 8 unit tests
├── buckets.rs    # QuotaBuckets cache
└── refresh.rs    # image-watcher refresh task + 2 unit tests

crates/broker/src/handlers/
├── alter_client_quotas.rs       # api_key 49 + process_one_entry + 6 unit tests
└── describe_client_quotas.rs    # api_key 48 + entity_matches_filter + 4 unit tests
```

### `AlterClientQuotas` flow

1. Cluster Alter authorize.
2. For each `EntryData`:
   - Call `process_one_entry(entry)` → `Result<Vec<MetadataRecord>, (i16, String)>`.
   - On Ok: extend `to_submit` with the produced `V1ClientQuota` records (unless `validate_only`).
   - On Err: per-entry error.
3. `submit_change(to_submit).await` — on failure, downgrade queued OK entries to `COORDINATOR_NOT_AVAILABLE`.
4. Encode response.

### `process_one_entry` validation

- Empty entity tuple → `INVALID_REQUEST`.
- Each `entity_type` ∈ `{"user", "client-id"}`; otherwise `INVALID_REQUEST` (slice 16 — `ip` is slice 16b).
- No duplicate `entity_type` within one entry → `INVALID_REQUEST`.
- Each op:
  - `key` ∈ `{"producer_byte_rate", "consumer_byte_rate", "request_percentage"}` → otherwise `INVALID_CONFIG`.
  - Value:
    - Byte rates: `value > 0.0` (positive finite f64) — `0` accepted as "throttle everything"; negative or NaN rejected.
    - request_percentage: `0.0 < value <= 100.0` (single-core upper bound; slice 16 doesn't multiplex CPU).
  - `remove == true` → emit `config_value: None`; the `value` field is ignored.

### `DescribeClientQuotas` flow

1. Cluster Describe authorize. On Deny: top-level `error_code = CLUSTER_AUTHORIZATION_FAILED`, empty `entries`.
2. Walk `image.client_quotas()`; filter via `entity_matches_filter(stored_key, req.components, req.strict)`.
3. For each matching entry: emit a row with the canonicalized entity tuple + all configured `(key, value)` pairs.

### `entity_matches_filter`

- `strict=true`: `stored.len() == components.len()`.
- For each component:
  - Find a `stored_entity` with matching `entity_type`. None → no match.
  - `match_type=0` (exact): `stored_entity.entity_name == Some(req.match)`.
  - `match_type=1` (default): `stored_entity.entity_name == None`.
  - `match_type=2` (any): match regardless of name.

### Dispatch wiring

Inline-intercept pattern (slice 13/14/15 precedent). Both handlers need `&Principal` + `&SocketAddr`. In `crates/broker/src/network/dispatch.rs`:

```rust
if peek_api_key(&frame) == Some(48) { handle_describe_client_quotas_frame(...).await?; continue; }
if peek_api_key(&frame) == Some(49) { handle_alter_client_quotas_frame(...).await?; continue; }
```

Plus flex-table arms for 48/49 and `v!(alter_client_quotas_request)` / `v!(describe_client_quotas_request)` in `supported_apis`.

### Error code mapping

| Condition | Wire code |
|---|---|
| Empty entity tuple | `INVALID_REQUEST (42)` |
| Unsupported entity_type | `INVALID_REQUEST (42)` |
| Duplicate entity_type within entry | `INVALID_REQUEST (42)` |
| Unknown quota key | `INVALID_CONFIG (40)` |
| Out-of-range value (negative / NaN / >100% for percentage) | `INVALID_CONFIG (40)` |
| Submit failed (raft) | `COORDINATOR_NOT_AVAILABLE (15)` |
| Non-super-user, no ACL | `CLUSTER_AUTHORIZATION_FAILED (31)` |

---

## 5. Testing

### Unit tests (~20 total)

**`quota/lookup.rs` (8 tests):**
- `exact_user_client_pair_match`
- `user_default_falls_back_to_client_specific`
- `single_user_match_when_no_pair_exists`
- `single_client_id_match_when_no_user_exists`
- `default_user_default_client_pair`
- `default_user_alone`
- `default_client_alone`
- `no_match_returns_none`

**`handlers/alter_client_quotas.rs` (6 tests):**
- `start_writes_v1_client_quota_record`
- `validate_only_does_not_submit`
- `remove_writes_none_value`
- `unsupported_entity_type_rejected`
- `duplicate_entity_type_rejected`
- `out_of_range_value_rejected`

**`handlers/describe_client_quotas.rs` (4 tests):**
- `strict_exact_match_filters_correctly`
- `non_strict_filter_returns_supersets`
- `default_match_type_filters_by_none_entity_name`
- `any_match_type_returns_all_names_of_type`

**`metadata/src/image.rs` (2 tests):**
- `client_quota_record_apply_canonicalizes_entity_order`
- `client_quota_record_delete_removes_from_map`

### Broker integration tests (`crates/broker/tests/client_quotas.rs`, 5 tests)

1. **`alter_then_describe_round_trip`** — single-broker SASL/PLAIN; alter `(user=alice) producer_byte_rate=1024`; describe with `user any-name`; assert returned value matches.

2. **`producer_byte_rate_throttles_produce`** — single-broker; set `(user=alice) producer_byte_rate=512`; alice produces 4 KB; measure that response carries `throttle_time_ms > 0` and wall time elapsed ≥ throttle delay.

3. **`consumer_byte_rate_throttles_fetch`** — single-broker; set `(user=alice) consumer_byte_rate=512`; alice fetches 4 KB; assert `throttle_time_ms > 0`.

4. **`tuple_quota_wins_over_user_only`** — set both `(user=alice) producer_byte_rate=8192` and `(user=alice, client-id=app1) producer_byte_rate=512`; alice as app1 produces 4 KB; assert tighter throttle applied (delay matches 512 rate).

5. **`non_super_user_denied`** — alice has PLAIN creds, no ACLs; seed one unrelated ACL to disable slice-13 compat shim; expect `AlterClientQuotas` returns `CLUSTER_AUTHORIZATION_FAILED (31)` on every entry.

### JVM acceptance (1 new test in `jvm_acceptance.rs`)

`jvm_kafka_configs_alter_client_quota_end_to_end` — `#[ignore]`-tagged, WSL-driven:

1. 3-broker SASL/PLAINTEXT cluster (reuse slice 14 helper).
2. `kafka-configs --alter --entity-type users --entity-name alice --add-config 'producer_byte_rate=1024'` → exit 0.
3. `kafka-configs --describe --entity-type users --entity-name alice` → stdout contains `producer_byte_rate=1024`.
4. `kafka-console-producer` as alice pushes ~4 KB → wall time ≥ ~3 seconds (one-second burst + ~3s payback at 1 KB/sec).
5. `kafka-configs --alter --entity-type users --entity-name alice --delete-config 'producer_byte_rate'` → exit 0.
6. Confirm config gone from image.

### Slice 15b interaction note

Both slice 15b's inter-broker leader-throttle and slice 16's consumer_byte_rate live in `fetch.rs::handle`. They're mutually exclusive by `replica_id` sign — 15b fires when `>= 0` (inter-broker), 16 fires when `< 0` (consumer). Add a code comment so reviewers don't think they're redundant.
