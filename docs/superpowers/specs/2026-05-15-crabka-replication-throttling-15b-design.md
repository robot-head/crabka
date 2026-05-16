# Slice 15b: Replication throttling (KIP-73) — Design

**Status:** Approved 2026-05-15.

**Goal:** Implement KIP-73 throttled inter-broker replication. Persist the four KIP-73 configs (2 topic-level + 2 broker-level), enforce them via a token-bucket rate limiter on the Fetch path, and surface the values via `DescribeConfigs`. Closes the slice-15 T11 known-limitation gap: `kafka-reassign-partitions --verify` exits 0 because `IncrementalAlterConfigs` broker-scoped (`resource_type=4`) now succeeds.

**Out of scope:**
- Metrics emission (`replication_bytes_in/out`) — Crabka has no metrics framework yet; defer to a dedicated observability slice
- Dynamic reload of arbitrary configs — only throttle configs subscribe to live updates; other configs continue to require restart
- Per-listener config refresh
- KIP-841 force-elect, KIP-113 log-dir reassignment

---

## 1. Scope

### In

- `IncrementalAlterConfigs` broker-scoped (`resource_type=4`) — stores rate configs in metadata, closes slice 15 T11 gap
- Topic-level configs `leader.replication.throttled.replicas` and `follower.replication.throttled.replicas` — parsed list of `partition:broker` pairs (or `*` wildcard), persisted on `TopicConfigRecord`
- Broker-level configs `leader.replication.throttled.rate` and `follower.replication.throttled.rate` — bytes/sec, persisted on a new `BrokerConfigRecord` metadata record
- Wildcard `*` support in throttled.replicas configs
- `DescribeConfigs` surfaces all four configs for both Topic and Broker resource types
- Throttle state propagated via `watch::Receiver<Arc<MetadataImage>>` — live config changes apply without broker restart (throttle-only)
- Token-bucket rate limiter on the Fetch hot path:
  - **Leader side:** cap response bytes when a partition is in `leader.replication.throttled.replicas` and the fetching follower is included; bucket fed by `leader.replication.throttled.rate`
  - **Follower side:** cap `max_bytes` in outgoing Fetch requests when the partition is in `follower.replication.throttled.replicas`; bucket fed by `follower.replication.throttled.rate`
- JVM acceptance: `kafka-reassign-partitions --execute --throttle 1024 ...` round-trip including `--verify` exiting 0

### Not in

- Metrics emission — deferred
- Dynamic reload of non-throttle configs — deferred
- Per-listener config refresh
- KIP-841 force-elect, KIP-113 log-dir reassignment

---

## 2. Config storage & parsing

### Topic-level throttle configs

Stored via existing `TopicConfigRecord` (`crates/metadata/src/records.rs`) + `MetadataImage::topic_configs`. Two new keys added to the validator:

- `leader.replication.throttled.replicas`
- `follower.replication.throttled.replicas`

**Value format (Kafka-native):**
- `""` (empty) → no replicas throttled
- `"*"` → all replicas of this topic throttled
- `"0:1,0:2,1:3"` → comma-separated `partition:broker` pairs

**Parser** in new `crates/broker/src/throttle.rs`:

```rust
pub enum ThrottledReplicas {
    None,
    All,
    List(Vec<(i32, NodeId)>),
}

impl ThrottledReplicas {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() { return Ok(Self::None); }
        if value == "*"    { return Ok(Self::All); }
        let mut out = Vec::new();
        for pair in value.split(',') {
            let (p, n) = pair.split_once(':')
                .ok_or_else(|| format!("invalid pair {pair:?}"))?;
            out.push((p.trim().parse().map_err(|e| format!("partition: {e}"))?,
                      n.trim().parse().map_err(|e| format!("broker: {e}"))?));
        }
        Ok(Self::List(out))
    }

    pub fn contains(&self, partition: i32, node: NodeId) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::List(v) => v.iter().any(|&(p, n)| p == partition && n == node),
        }
    }
}
```

`MetadataImage::topic_throttle(topic) -> TopicThrottle { leader: ThrottledReplicas, follower: ThrottledReplicas }` reads the two configs from `topic_config(topic)` and parses on demand.

### Broker-level rate configs

New metadata record:

```rust
pub struct BrokerConfigRecord {
    pub node_id: NodeId,
    pub config_name: String,
    pub config_value: Option<String>,  // None = delete
}

// In MetadataRecord enum:
V1BrokerConfig(BrokerConfigRecord),
```

`MetadataImage` gains:

```rust
broker_configs: HashMap<NodeId, BTreeMap<String, String>>,

pub fn broker_config(&self, node_id: NodeId) -> Option<&BTreeMap<String, String>> { ... }

pub fn broker_throttle_rate(&self, node_id: NodeId, kind: ThrottleKind) -> Option<u64> {
    let key = match kind {
        ThrottleKind::Leader => "leader.replication.throttled.rate",
        ThrottleKind::Follower => "follower.replication.throttled.rate",
    };
    self.broker_config(node_id)?.get(key)?.parse::<i64>().ok()
        .filter(|&v| v >= 0)
        .map(|v| v as u64)
}
```

`-1` disables (Kafka convention). Empty/missing means no throttle.

**Recognized broker config keys (slice 15b):**
- `leader.replication.throttled.rate`
- `follower.replication.throttled.rate`

Future slices extend the allowlist by registering more keys.

### Apply logic in `MetadataImage::apply`

Add an arm for `V1BrokerConfig`:

```rust
MetadataRecord::V1BrokerConfig(rec) => {
    let entry = self.broker_configs.entry(rec.node_id).or_default();
    match &rec.config_value {
        Some(v) => { entry.insert(rec.config_name.clone(), v.clone()); }
        None => { entry.remove(&rec.config_name); }
    }
}
```

---

## 3. Fetch-path enforcement

### Token bucket

New `crates/broker/src/throttle/bucket.rs`:

```rust
pub struct TokenBucket {
    rate_bytes_per_sec: AtomicU64,  // 0 = unthrottled (fast path)
    available: AtomicU64,
    last_refill_nanos: AtomicU64,
}

impl TokenBucket {
    pub fn new() -> Self;
    pub fn set_rate(&self, new_rate: u64);
    /// Try to consume up to `requested` bytes. Refills based on
    /// elapsed wall time; capped at one second of capacity. Returns
    /// number of bytes granted (0..=requested).
    pub fn try_consume(&self, requested: u64) -> u64;
}
```

Implementation uses `std::time::Instant` against a startup-fixed origin (stored as `AtomicU64` of nanoseconds since origin). Refill formula: `tokens = elapsed_nanos * rate / 1_000_000_000`; capacity capped at `rate` (one second of burst). Updates to `available` and `last_refill_nanos` are non-atomic across the pair (relaxed ordering, no compare-exchange) — under contention the bucket may briefly grant slightly more than the configured rate, which is acceptable: KIP-73 throttling is statistical, not strict.

### `ThrottleState` — broker-wide

```rust
pub struct ThrottleState {
    pub leader_out: Arc<TokenBucket>,
    pub follower_in: Arc<TokenBucket>,
}
```

Constructed once at `Broker::start`; rates default to 0 (unthrottled). Lives as a field on `Broker`. Shared between Fetch handler and replicator.

### Leader-side enforcement

Modify `crates/broker/src/handlers/fetch.rs::handle`:

After per-partition response chunks are assembled but **before** encoding, walk the assembled rows:

```rust
let fetcher = req.replica_id;  // -1 for consumer fetch — skip throttling
if fetcher >= 0 {
    let mut throttled_bytes: u64 = 0;
    let mut throttled_rows: Vec<usize> = Vec::new();
    for (idx, (topic, partition, chunk)) in assembled.iter().enumerate() {
        let throttle = image.topic_throttle(topic);
        if throttle.leader.contains(*partition, broker.node_id) {
            throttled_bytes += chunk.len() as u64;
            throttled_rows.push(idx);
        }
    }
    let granted = throttle_state.leader_out.try_consume(throttled_bytes);
    if granted < throttled_bytes {
        // Walk throttled_rows in order; trim/drop chunks until total fits.
        // Untruncatable invariant: each partition's chunk is fully included
        // or fully excluded (no mid-batch truncation).
        truncate_response(&mut assembled, &throttled_rows, granted);
    }
}
```

**Why the in-order walk + whole-chunk drop:** Kafka clients expect complete record batches. We can drop a partition's chunk entirely (the client retries next round) but cannot truncate mid-batch. In-order traversal gives deterministic behavior across runs.

### Follower-side enforcement

`crates/broker/src/replicator_supervisor.rs` (or wherever the replicator's Fetch issuer lives). Before issuing a Fetch:

```rust
let throttle = image.topic_throttle(topic);
let max_bytes_cap = if throttle.follower.contains(partition, self.node_id)
    && throttle_state.follower_in.rate_bytes_per_sec.load(Relaxed) > 0
{
    throttle_state.follower_in.try_consume(fetch_max_bytes)
} else {
    fetch_max_bytes
};
if max_bytes_cap == 0 {
    // Skip this round; wait for bucket refill.
    return;
}
// Issue Fetch with max_bytes = max_bytes_cap.
```

### Throttle state refresh

`crates/broker/src/throttle/refresh.rs`:

```rust
pub async fn run(
    controller: Arc<dyn ImageWatcher>,
    node_id: NodeId,
    throttle: Arc<ThrottleState>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    loop {
        tokio::select! {
            _ = watcher.changed() => {},
            _ = shutdown.cancelled() => return,
        }
        let image = controller.current_image();
        let leader_rate = image.broker_throttle_rate(node_id, ThrottleKind::Leader).unwrap_or(0);
        let follower_rate = image.broker_throttle_rate(node_id, ThrottleKind::Follower).unwrap_or(0);
        throttle.leader_out.set_rate(leader_rate);
        throttle.follower_in.set_rate(follower_rate);
    }
}
```

Spawned from `Broker::start` unconditionally. The `is_leader()` check used in slice 14/15 doesn't apply here — every broker enforces its own throttles, regardless of controller leadership.

### `replica_id` extraction

Slice 10b's Fetch handler already extracts `replica_id`. Reuse: `replica_id >= 0` means inter-broker Fetch (throttle applies); `replica_id == -1` means consumer Fetch (no throttle).

---

## 4. Handler & dispatch wiring

### `IncrementalAlterConfigs` extensions

Modify `crates/broker/src/handlers/incremental_alter_configs.rs`:

**Topic-scoped (resource_type=2):** add the two throttled-replicas keys to `config_keys::validate_topic_config`. Value validation calls `ThrottledReplicas::parse`; malformed strings → `INVALID_CONFIG (40)` with descriptive `error_message`.

**Broker-scoped (resource_type=4):** replace the current "resource_type=4 not supported" branch with:

1. Parse `resource_name` as `NodeId`. Empty name (cluster-wide default) → `INVALID_REQUEST` ("cluster-wide broker config not supported"). Slice 15b targets per-broker only.
2. Validate broker exists in the image; if not → `INVALID_REQUEST` ("unknown broker {n}").
3. For each config in the resource:
   - Reject unknown keys with `INVALID_CONFIG`.
   - For SET: validate value parses as `i64`; reject non-integer with `INVALID_CONFIG`.
   - For DELETE: accept; `config_value: None`.
4. Submit `MetadataRecord::V1BrokerConfig(BrokerConfigRecord { ... })` per config.

`is_known_broker_config` returns `true` only for the two throttle-rate keys in slice 15b.

### `DescribeConfigs` extensions

Modify `crates/broker/src/handlers/describe_configs.rs`:

- **Topic resource:** no change. The two new keys live in `topic_configs` and are surfaced by the existing iteration.
- **Broker resource:** wire to read `image.broker_config(node_id)` and emit one `DescribeConfigsResource` entry per key. `config_source = DYNAMIC_BROKER_CONFIG (4)` per Kafka convention. Authorize via Cluster Describe (matches existing pattern).

### `Broker::start` spawn

In `crates/broker/src/broker.rs`, after slice 15 T8's reassignment spawn:

```rust
let throttle_state = Arc::new(crate::throttle::ThrottleState::new());
{
    let throttle = throttle_state.clone();
    let controller = controller.clone();
    let shutdown = supervisor_shutdown.child_token();
    let node_id = config.node_id;
    tokio::spawn(crate::throttle::refresh::run(controller, node_id, throttle, shutdown));
}
broker.throttle_state = throttle_state;
```

`Broker` struct gains `pub throttle_state: Arc<ThrottleState>`. Fetch handler and replicator read via the shared `Arc`.

### Dispatch wiring

No new api_keys. `IncrementalAlterConfigs` (api_key 44) and `DescribeConfigs` (api_key 32) are already in `supported_apis` from slice 11. No new intercept arms.

---

## 5. Testing strategy

### Unit tests (~20 total)

**`crates/broker/src/throttle.rs` — `ThrottledReplicas::parse` (6 tests):**
- `empty_string_parses_as_none`
- `wildcard_parses_as_all`
- `single_pair_parses`
- `multiple_pairs_parse`
- `malformed_pair_rejected`
- `whitespace_tolerated`

**`crates/broker/src/throttle/bucket.rs` — `TokenBucket` (6 tests):**
- `zero_rate_grants_full_request`
- `first_consume_under_rate_succeeds`
- `consume_drains_bucket`
- `bucket_refills_at_rate_after_elapsed_time`
- `bucket_caps_at_one_second_capacity`
- `set_rate_resets_available`

**`crates/metadata/src/image.rs` — `BrokerConfigRecord` apply (4 tests):**
- `broker_config_set_inserts_into_image`
- `broker_config_delete_removes_from_image`
- `broker_throttle_rate_parses_positive_value`
- `broker_throttle_rate_returns_none_for_negative_one`

**`crates/broker/src/handlers/incremental_alter_configs.rs` — validator (4 tests):**
- `topic_throttle_config_value_validated`
- `broker_scoped_rate_config_accepted`
- `broker_scoped_unknown_config_rejected`
- `broker_scoped_invalid_value_rejected`

### Broker integration tests (`crates/broker/tests/throttle.rs`, 4 tests)

1. **`broker_scoped_alter_persists_in_image`** — single-broker SASL/PLAIN; alter `leader.replication.throttled.rate=2048` via wire; read back via `DescribeConfigs`; assert value.

2. **`topic_throttle_config_propagates`** — alter topic config `leader.replication.throttled.replicas="0:1,0:2"`; assert `image.topic_throttle("foo").leader.contains(0, 1)` is true.

3. **`throttle_rate_caps_fetch_response_size`** — 2-broker PLAINTEXT; create rf=1 topic on broker 1; alter `leader.replication.throttled.rate=512` and `leader.replication.throttled.replicas="0:2"`; produce 8 KB; broker 2 issues Fetch with `replica_id=2`; assert response size ≤ ~1 KB within one fetch round. Permissive assertion to accommodate batch-header overhead.

4. **`unthrottled_partition_unaffected`** — same setup as test 3 but no throttle configs; assert full 8 KB delivered in one Fetch.

### JVM acceptance (1 new test in `jvm_acceptance.rs`)

`jvm_kafka_reassign_partitions_with_throttle_end_to_end` — `#[ignore]`-tagged, WSL-driven:

1. 3-broker SASL/PLAINTEXT cluster (reuse slice 14 helper).
2. Create rf=2 topic.
3. Run `kafka-reassign-partitions --execute --throttle 1024 --reassignment-json-file ...`.
4. Assert exit code 0.
5. Read configs via `kafka-configs --describe --entity-type brokers --entity-name 1` — assert `leader.replication.throttled.rate=1024` present.
6. Inject ISR so background reassignment-completion task can finish (slice 15 idiom).
7. Run `kafka-reassign-partitions --verify` — assert exit code 0 **and** no "not supported" error in stderr.
8. Confirm throttle configs cleared from both brokers and topic.

### Slice 15 regression closure

When slice 15b lands, update slice 15's `jvm_kafka_reassign_partitions_end_to_end` test:
- Replace the stdout-substring assertion for `--verify` with `out.status.success()` (the exit code now lands correctly).
- Either change inline as part of slice 15b's commit, or note in STATUS.md as a future cleanup.
