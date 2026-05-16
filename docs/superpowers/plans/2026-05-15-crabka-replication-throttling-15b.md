# Slice 15b: Replication throttling (KIP-73) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Implement KIP-73 throttled inter-broker replication. Persist the four KIP-73 configs (2 topic-level + 2 broker-level), enforce them via a token-bucket rate limiter on the Fetch path, and surface the values via `DescribeConfigs`. Close the slice-15 T11 known-limitation gap.

**Architecture:** Topic-level `*.throttled.replicas` configs already store via `TopicConfigRecord` — just add to the validator allowlist. Broker-level `*.throttled.rate` configs use a new `BrokerConfigRecord` metadata record. A `ThrottleState` holds two `TokenBucket`s (leader-out + follower-in); a background refresh task subscribes to `controller.watch_image()` and updates bucket rates when configs change. The Fetch handler (leader side) and the replicator (follower side) consult the buckets and cap response bytes / outgoing `max_bytes`.

**Tech Stack:** Rust 1.95.0; reuses slice 11 `IncrementalAlterConfigs`/`DescribeConfigs` handlers, slice 12 `topic_configs` plumbing, slice 14 `ControllerHandle::watch_image()`, slice 15's metadata layer (no new dependencies).

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-replication-throttling-15b-design.md`](../specs/2026-05-15-crabka-replication-throttling-15b-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/replication-throttling-15b` already created with spec committed at `47dc509` (rebased on slice 15's merge `a8f6dc4`).

**Compat note:** Per `CLAUDE.md`, no backwards-compat shims. `BrokerConfigRecord` is a new metadata-record variant; raft-log replay across the slice boundary requires wiping data dirs (greenfield project).

---

## File structure

```
crates/metadata/src/
├── records.rs   # MODIFIED — BrokerConfigRecord + V1BrokerConfig variant
└── image.rs     # MODIFIED — broker_configs map + accessors + apply arm + 4 unit tests

crates/broker/src/
├── throttle.rs                                 # NEW — ThrottledReplicas + TopicThrottle + 6 unit tests
├── throttle/
│   ├── bucket.rs                               # NEW — TokenBucket + ThrottleState + 6 unit tests
│   └── refresh.rs                              # NEW — background refresh task + 2 unit tests
├── handlers/
│   ├── incremental_alter_configs.rs            # MODIFIED — broker-scoped path + validator allowlist
│   └── describe_configs.rs                     # MODIFIED — broker-resource path
├── config_keys.rs (or wherever the validator lives)
│                                               # MODIFIED — accept the four throttle keys
├── handlers/fetch.rs                           # MODIFIED — leader-side throttle enforcement
├── replicator_supervisor.rs (or replicator.rs) # MODIFIED — follower-side throttle enforcement
├── broker.rs                                   # MODIFIED — spawn refresh task + Broker.throttle_state
└── lib.rs                                      # MODIFIED — pub mod throttle

crates/broker/tests/
├── throttle.rs            # NEW — 4 broker integration tests
└── jvm_acceptance.rs      # MODIFIED — 1 new JVM test + closure of slice 15 stdout-substring fallback
```

13 tasks across 6 batches.

---

## Batch 1 — Metadata layer (parallel: T1, T2)

### Task 1: `BrokerConfigRecord` + apply + accessors

**Files:**
- Modify: `crates/metadata/src/records.rs`
- Modify: `crates/metadata/src/image.rs`
- Modify: `crates/metadata/src/lib.rs` (re-export)

- [ ] **Step 1: Add the new record type to `records.rs`**

Append after the existing `TopicConfigRecord`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigRecord {
    pub node_id: NodeId,
    pub config_name: String,
    /// `Some(value)` = set; `None` = delete.
    pub config_value: Option<String>,
}
```

Add the enum variant to `MetadataRecord`:

```rust
V1BrokerConfig(BrokerConfigRecord),
```

(Bump nothing else; per CLAUDE.md, no compat shims. Existing raft logs without this variant will fail to deserialize — devs wipe data dirs.)

- [ ] **Step 2: Add the round-trip test in `records.rs`**

Append to the existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn broker_config_record_round_trip() {
        let r = MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 7,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        });
        let bytes = serde_wincode::serialize(&r).expect("encode");
        let decoded: MetadataRecord = serde_wincode::deserialize(&bytes).expect("decode");
        assert_eq!(r, decoded);
    }
```

(Use whatever serde-wincode API slice 14's existing `topic_config_record_round_trip` uses — copy that test's shape verbatim.)

- [ ] **Step 3: Extend `MetadataImage` in `image.rs`**

Add the field:

```rust
broker_configs: HashMap<NodeId, BTreeMap<String, String>>,
```

Initialize to `HashMap::new()` in `MetadataImage::new`.

Add accessors:

```rust
pub fn broker_config(&self, node_id: NodeId) -> Option<&BTreeMap<String, String>> {
    self.broker_configs.get(&node_id)
}

#[derive(Debug, Clone, Copy)]
pub enum ThrottleKind {
    Leader,
    Follower,
}

pub fn broker_throttle_rate(&self, node_id: NodeId, kind: ThrottleKind) -> Option<u64> {
    let key = match kind {
        ThrottleKind::Leader => "leader.replication.throttled.rate",
        ThrottleKind::Follower => "follower.replication.throttled.rate",
    };
    let raw = self.broker_config(node_id)?.get(key)?;
    let v: i64 = raw.parse().ok()?;
    if v < 0 { None } else { Some(v as u64) }
}
```

Add the apply arm in `MetadataImage::apply`:

```rust
MetadataRecord::V1BrokerConfig(rec) => {
    let entry = self.broker_configs.entry(rec.node_id).or_default();
    match &rec.config_value {
        Some(v) => { entry.insert(rec.config_name.clone(), v.clone()); }
        None => { entry.remove(&rec.config_name); }
    }
}
```

- [ ] **Step 4: Re-export from `lib.rs`**

In `crates/metadata/src/lib.rs`, append `BrokerConfigRecord, ThrottleKind` to the existing re-export line.

- [ ] **Step 5: 4 unit tests in `image.rs`**

Append to the existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn broker_config_set_inserts_into_image() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        let bc = img.broker_config(1).expect("broker config");
        assert_eq!(bc.get("leader.replication.throttled.rate"), Some(&"2048".to_string()));
    }

    #[test]
    fn broker_config_delete_removes_from_image() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: None,
        }));
        let bc = img.broker_config(1).expect("broker_configs entry retained");
        assert!(bc.get("leader.replication.throttled.rate").is_none());
    }

    #[test]
    fn broker_throttle_rate_parses_positive_value() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        assert_eq!(img.broker_throttle_rate(1, ThrottleKind::Leader), Some(2048));
    }

    #[test]
    fn broker_throttle_rate_returns_none_for_negative_one() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("-1".into()),
        }));
        assert!(img.broker_throttle_rate(1, ThrottleKind::Leader).is_none());
    }
```

- [ ] **Step 6: Build + tests + lints**

```
cargo build --workspace
cargo test -p crabka-metadata
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: round-trip + 4 new tests PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/metadata/src/
git commit -m "$(cat <<'EOF'
feat(metadata): BrokerConfigRecord + broker_configs accessor

V1BrokerConfig metadata record carries per-broker config key/value
pairs. MetadataImage tracks broker_configs as HashMap<NodeId,
BTreeMap<String, String>>. broker_throttle_rate parses the two KIP-73
rate configs with -1 treated as "disabled" (Kafka convention).

Per CLAUDE.md (greenfield), no serde(default) compat shim — pre-slice
raft logs require data-dir wipe.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `ThrottledReplicas` parser + accessor

**Files:**
- Create: `crates/broker/src/throttle.rs`
- Modify: `crates/broker/src/lib.rs` (add `pub mod throttle;`)
- Modify: `crates/metadata/src/image.rs` (add `topic_throttle` accessor — minor edit, parallel-safe with T1 if T1 sticks to its own sections; see note below)

> **Parallel coordination note:** T1 modifies `crates/metadata/src/image.rs` to add `broker_configs` plumbing. T2 wants to add a `topic_throttle` accessor to the same file. Both append to the existing `impl MetadataImage`. To avoid merge conflicts: T2 should put its accessor in a NEW file `crates/broker/src/throttle.rs` and only read from `image.topic_config(...)` rather than adding methods to `MetadataImage` itself. The plan below does this — `TopicThrottle::for_image_topic(image, topic)` is a free function, not a method on `MetadataImage`. Safe to parallelize.

- [ ] **Step 1: Create `crates/broker/src/throttle.rs`**

```rust
//! KIP-73 throttled replication — value types and parser.

use crabka_metadata::{MetadataImage, NodeId};

/// Topic-level `*.throttled.replicas` config value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottledReplicas {
    /// Empty string — no replicas throttled.
    None,
    /// `"*"` wildcard — all replicas of this topic throttled.
    All,
    /// `"partition:broker,partition:broker,..."` — specific pairs.
    List(Vec<(i32, NodeId)>),
}

impl ThrottledReplicas {
    pub fn parse(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Ok(Self::None);
        }
        if value == "*" {
            return Ok(Self::All);
        }
        let mut out = Vec::new();
        for pair in value.split(',') {
            let (p_str, n_str) = pair
                .split_once(':')
                .ok_or_else(|| format!("invalid pair {pair:?}"))?;
            let p: i32 = p_str
                .trim()
                .parse()
                .map_err(|e| format!("partition: {e}"))?;
            let n: NodeId = n_str
                .trim()
                .parse()
                .map_err(|e| format!("broker: {e}"))?;
            out.push((p, n));
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

/// Both leader-side and follower-side throttled replicas for a topic.
#[derive(Debug, Clone)]
pub struct TopicThrottle {
    pub leader: ThrottledReplicas,
    pub follower: ThrottledReplicas,
}

impl TopicThrottle {
    pub fn for_topic(image: &MetadataImage, topic: &str) -> Self {
        let configs = image.topic_config(topic);
        let read = |key: &str| -> ThrottledReplicas {
            configs
                .and_then(|c| c.get(key))
                .and_then(|v| ThrottledReplicas::parse(v).ok())
                .unwrap_or(ThrottledReplicas::None)
        };
        Self {
            leader: read("leader.replication.throttled.replicas"),
            follower: read("follower.replication.throttled.replicas"),
        }
    }
}

pub const LEADER_THROTTLED_REPLICAS_KEY: &str = "leader.replication.throttled.replicas";
pub const FOLLOWER_THROTTLED_REPLICAS_KEY: &str = "follower.replication.throttled.replicas";
pub const LEADER_THROTTLED_RATE_KEY: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_THROTTLED_RATE_KEY: &str = "follower.replication.throttled.rate";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_parses_as_none() {
        assert_eq!(ThrottledReplicas::parse("").unwrap(), ThrottledReplicas::None);
    }

    #[test]
    fn wildcard_parses_as_all() {
        assert_eq!(ThrottledReplicas::parse("*").unwrap(), ThrottledReplicas::All);
    }

    #[test]
    fn single_pair_parses() {
        let r = ThrottledReplicas::parse("0:1").unwrap();
        assert!(r.contains(0, 1));
        assert!(!r.contains(0, 2));
        assert!(!r.contains(1, 1));
    }

    #[test]
    fn multiple_pairs_parse() {
        let r = ThrottledReplicas::parse("0:1,0:2,1:3").unwrap();
        assert!(r.contains(0, 1));
        assert!(r.contains(0, 2));
        assert!(r.contains(1, 3));
        assert!(!r.contains(1, 1));
    }

    #[test]
    fn malformed_pair_rejected() {
        assert!(ThrottledReplicas::parse("not-a-pair").is_err());
        assert!(ThrottledReplicas::parse("0:x").is_err());
        assert!(ThrottledReplicas::parse("x:1").is_err());
    }

    #[test]
    fn whitespace_tolerated() {
        let r = ThrottledReplicas::parse(" 0 : 1 , 2:3 ").unwrap();
        assert!(r.contains(0, 1));
        assert!(r.contains(2, 3));
    }
}
```

- [ ] **Step 2: Register module + spawn slot**

In `crates/broker/src/lib.rs`:

```rust
pub mod throttle;
```

(Place alphabetically alongside other `pub mod ...;`)

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib throttle
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 6 new tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/throttle.rs crates/broker/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(broker): ThrottledReplicas parser + TopicThrottle accessor

KIP-73 *.throttled.replicas config parsing. Supports "" (none),
"*" (all), and "p:n,p:n" pair lists. TopicThrottle::for_topic reads
both leader and follower keys from MetadataImage::topic_config.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 2 — Token bucket + config validators (parallel: T3, T4)

### Task 3: `TokenBucket` + `ThrottleState` + 6 unit tests

**Files:**
- Create: `crates/broker/src/throttle/bucket.rs`
- Modify: `crates/broker/src/throttle.rs` (add `mod bucket; pub use bucket::*;`)

> **Module structure note:** T2 created `crates/broker/src/throttle.rs` as a flat file. T3 converts it to a directory module by:
> 1. Renaming `crates/broker/src/throttle.rs` → `crates/broker/src/throttle/mod.rs`
> 2. Adding submodules `bucket.rs` (this task) and later `refresh.rs` (T7).
>
> Verify the `mod throttle` declaration in `lib.rs` works either way (Rust handles both). If anything breaks, double-check via `cargo check` after the move.

- [ ] **Step 1: Move `throttle.rs` to `throttle/mod.rs`**

```
mkdir crates/broker/src/throttle
mv crates/broker/src/throttle.rs crates/broker/src/throttle/mod.rs
```

(Use Bash tool with `mv` or PowerShell `Move-Item`.)

- [ ] **Step 2: Create `crates/broker/src/throttle/bucket.rs`**

```rust
//! KIP-73 token bucket rate limiter.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Instant;

use once_cell::sync::Lazy;

static EPOCH: Lazy<Instant> = Lazy::new(Instant::now);

#[inline]
fn now_nanos() -> u64 {
    EPOCH.elapsed().as_nanos() as u64
}

#[derive(Debug)]
pub struct TokenBucket {
    rate_bytes_per_sec: AtomicU64,
    available: AtomicU64,
    last_refill_nanos: AtomicU64,
}

impl TokenBucket {
    pub fn new() -> Self {
        Self {
            rate_bytes_per_sec: AtomicU64::new(0),
            available: AtomicU64::new(0),
            last_refill_nanos: AtomicU64::new(now_nanos()),
        }
    }

    /// Update the rate. Resets `available` to a one-second burst at
    /// the new rate; sets `last_refill` to now.
    pub fn set_rate(&self, new_rate: u64) {
        self.rate_bytes_per_sec.store(new_rate, Relaxed);
        self.available.store(new_rate, Relaxed);
        self.last_refill_nanos.store(now_nanos(), Relaxed);
    }

    pub fn rate(&self) -> u64 {
        self.rate_bytes_per_sec.load(Relaxed)
    }

    /// Try to consume up to `requested` bytes. Returns the number
    /// actually granted (0..=requested). Rate-0 grants the full
    /// request (fast path for unthrottled).
    pub fn try_consume(&self, requested: u64) -> u64 {
        let rate = self.rate_bytes_per_sec.load(Relaxed);
        if rate == 0 {
            return requested;
        }
        // Refill.
        let now = now_nanos();
        let last = self.last_refill_nanos.swap(now, Relaxed);
        let elapsed = now.saturating_sub(last);
        let refill = ((u128::from(elapsed) * u128::from(rate)) / 1_000_000_000) as u64;
        let mut cur = self.available.load(Relaxed);
        let new_avail = (cur.saturating_add(refill)).min(rate);
        self.available.store(new_avail, Relaxed);
        cur = new_avail;
        // Consume.
        let grant = requested.min(cur);
        self.available.fetch_sub(grant, Relaxed);
        grant
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

/// Broker-wide throttle state. Two buckets: outbound when this broker
/// is leader, inbound when this broker is follower.
#[derive(Debug)]
pub struct ThrottleState {
    pub leader_out: Arc<TokenBucket>,
    pub follower_in: Arc<TokenBucket>,
}

impl ThrottleState {
    pub fn new() -> Self {
        Self {
            leader_out: Arc::new(TokenBucket::new()),
            follower_in: Arc::new(TokenBucket::new()),
        }
    }
}

impl Default for ThrottleState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn zero_rate_grants_full_request() {
        let b = TokenBucket::new();
        assert_eq!(b.try_consume(1024), 1024);
    }

    #[test]
    fn first_consume_under_rate_succeeds() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        assert_eq!(b.try_consume(512), 512);
    }

    #[test]
    fn consume_drains_bucket() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        assert_eq!(b.try_consume(1024), 1024);
        // Immediately after, available is ~0 (no time elapsed).
        let g = b.try_consume(1024);
        assert!(g < 100, "expected near-zero grant, got {g}");
    }

    #[test]
    fn bucket_refills_at_rate_after_elapsed_time() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        b.try_consume(1024); // drain
        std::thread::sleep(Duration::from_millis(500));
        // After ~500ms at 1024 bytes/sec, ~512 bytes refilled.
        let g = b.try_consume(1024);
        assert!(g >= 400 && g <= 700, "expected ~512, got {g}");
    }

    #[test]
    fn bucket_caps_at_one_second_capacity() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        b.try_consume(1024); // drain
        std::thread::sleep(Duration::from_millis(1500));
        // After 1.5s, refill would be 1536, but cap is 1024.
        let g = b.try_consume(2048);
        assert!(g >= 900 && g <= 1024, "expected ~1024 (capped), got {g}");
    }

    #[test]
    fn set_rate_resets_available() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        b.try_consume(1024); // drain
        b.set_rate(2048);
        assert_eq!(b.try_consume(2048), 2048); // fresh capacity
    }
}
```

**Note on `once_cell`:** Crabka workspace already uses it (slice 12+ SASL credential cache). If not, switch to `std::sync::OnceLock<Instant>` (stable since Rust 1.70).

- [ ] **Step 3: Wire submodule into `throttle/mod.rs`**

At the top of `crates/broker/src/throttle/mod.rs`, add:

```rust
mod bucket;
pub use bucket::{ThrottleState, TokenBucket};
```

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib throttle
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 6 new tests PASS (timing-sensitive tests may flake on heavily-loaded CI — adjust tolerances if needed during implementation).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/throttle/
git commit -m "$(cat <<'EOF'
feat(broker): TokenBucket + ThrottleState

KIP-73 token bucket rate limiter. One-second burst capacity at
configured rate. Rate-0 fast path grants full request (unthrottled).
ThrottleState holds two buckets: leader-out and follower-in.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Config validator extensions + 4 unit tests

**Files:**
- Modify: `crates/broker/src/config_keys.rs` (or wherever the topic-config validator lives — search via `rg "validate_topic_config\|known_topic_configs" crates/broker/src/`)
- Modify: `crates/broker/src/handlers/incremental_alter_configs.rs` (add `is_known_broker_config` helper)

- [ ] **Step 1: Add throttle keys to the topic-config validator allowlist**

Locate the existing topic-config validator. It likely has a `match` or static set of known keys. Add:

```rust
"leader.replication.throttled.replicas" => /* validate via ThrottledReplicas::parse */,
"follower.replication.throttled.replicas" => /* validate via ThrottledReplicas::parse */,
```

For the value validation, call `crate::throttle::ThrottledReplicas::parse(&value)` and propagate the error.

- [ ] **Step 2: Add `is_known_broker_config` helper in `incremental_alter_configs.rs`**

Near the top of the file (alongside `RESOURCE_TYPE_BROKER`):

```rust
fn is_known_broker_config(name: &str) -> bool {
    matches!(
        name,
        crate::throttle::LEADER_THROTTLED_RATE_KEY
            | crate::throttle::FOLLOWER_THROTTLED_RATE_KEY
    )
}

fn validate_broker_config_value(name: &str, value: &str) -> Result<(), String> {
    match name {
        crate::throttle::LEADER_THROTTLED_RATE_KEY
        | crate::throttle::FOLLOWER_THROTTLED_RATE_KEY => {
            value.parse::<i64>().map(|_| ()).map_err(|e| format!("invalid rate: {e}"))
        }
        _ => Err(format!("unknown broker config {name}")),
    }
}
```

(The actual broker-scoped handler dispatch is T5 — this task just adds the helpers.)

- [ ] **Step 3: 4 unit tests**

Append to the existing `#[cfg(test)] mod tests` in `incremental_alter_configs.rs`:

```rust
    #[test]
    fn topic_throttle_config_value_validated() {
        // Verify ThrottledReplicas::parse rejects malformed input that
        // the validator delegates to.
        assert!(crate::throttle::ThrottledReplicas::parse("not-a-pair").is_err());
        assert!(crate::throttle::ThrottledReplicas::parse("0:bad").is_err());
    }

    #[test]
    fn broker_scoped_rate_config_accepted() {
        assert!(is_known_broker_config(crate::throttle::LEADER_THROTTLED_RATE_KEY));
        assert!(is_known_broker_config(crate::throttle::FOLLOWER_THROTTLED_RATE_KEY));
    }

    #[test]
    fn broker_scoped_unknown_config_rejected() {
        assert!(!is_known_broker_config("not.a.real.config"));
        assert!(validate_broker_config_value("not.a.real.config", "1024").is_err());
    }

    #[test]
    fn broker_scoped_invalid_value_rejected() {
        assert!(validate_broker_config_value(crate::throttle::LEADER_THROTTLED_RATE_KEY, "not-a-number").is_err());
        assert!(validate_broker_config_value(crate::throttle::LEADER_THROTTLED_RATE_KEY, "1024").is_ok());
    }
```

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib incremental_alter_configs
cargo test -p crabka-broker --lib config_keys
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 4 new tests + existing pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/
git commit -m "$(cat <<'EOF'
feat(broker): config validators for KIP-73 throttle keys

Topic config validator accepts leader/follower replication
throttled.replicas with ThrottledReplicas::parse value check.
incremental_alter_configs gains is_known_broker_config +
validate_broker_config_value helpers for the two broker-scoped
rate keys. T5 wires them into the broker-scoped dispatch path.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 3 — Handlers (parallel: T5, T6)

### Task 5: `IncrementalAlterConfigs` broker-scoped path

**Files:**
- Modify: `crates/broker/src/handlers/incremental_alter_configs.rs`

- [ ] **Step 1: Replace the "not supported" branch**

Locate the current branch around `crates/broker/src/handlers/incremental_alter_configs.rs:112-116`:

```rust
if resource.resource_type != RESOURCE_TYPE_TOPIC {
    out.error_code = INVALID_REQUEST;
    out.error_message = Some(format!("resource_type={} not supported", resource.resource_type));
    continue;
}
```

Restructure to handle both Topic and Broker:

```rust
match resource.resource_type {
    RESOURCE_TYPE_TOPIC => {
        // Existing topic-scoped code path stays here (unchanged).
        // ... existing topic-config handling ...
    }
    RESOURCE_TYPE_BROKER => {
        handle_broker_scoped(&resource, &image, &mut out, &mut to_submit);
    }
    other => {
        out.error_code = INVALID_REQUEST;
        out.error_message = Some(format!("resource_type={other} not supported"));
        continue;
    }
}
```

- [ ] **Step 2: Add `handle_broker_scoped` helper**

Below the existing top-level handler:

```rust
fn handle_broker_scoped(
    resource: &/* request resource type */,
    image: &MetadataImage,
    out: &mut /* response resource type */,
    to_submit: &mut Vec<MetadataRecord>,
) {
    // Empty resource_name = cluster-wide default; not supported in 15b.
    if resource.resource_name.is_empty() {
        out.error_code = INVALID_REQUEST;
        out.error_message = Some("cluster-wide broker config not supported".into());
        return;
    }
    let node_id: NodeId = match resource.resource_name.parse() {
        Ok(n) => n,
        Err(_) => {
            out.error_code = INVALID_REQUEST;
            out.error_message = Some(format!("invalid broker id {:?}", resource.resource_name));
            return;
        }
    };
    if image.broker(node_id).is_none() {
        out.error_code = INVALID_REQUEST;
        out.error_message = Some(format!("unknown broker {node_id}"));
        return;
    }
    for cfg in &resource.configs {
        if !is_known_broker_config(&cfg.name) {
            out.error_code = INVALID_CONFIG;
            out.error_message = Some(format!("unknown broker config {}", cfg.name));
            return; // halt processing this resource
        }
        // Handle SET vs DELETE per the op_type field.
        let new_value = match cfg.config_operation {
            OP_SET => {
                let v = cfg.value.clone().unwrap_or_default();
                if let Err(e) = validate_broker_config_value(&cfg.name, &v) {
                    out.error_code = INVALID_CONFIG;
                    out.error_message = Some(e);
                    return;
                }
                Some(v)
            }
            OP_DELETE => None,
            _ => {
                out.error_code = INVALID_REQUEST;
                out.error_message = Some(format!("unsupported op_type {}", cfg.config_operation));
                return;
            }
        };
        to_submit.push(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: cfg.name.clone(),
            config_value: new_value,
        }));
    }
}
```

**Look up the exact field names** in slice 11's existing handler — the request resource type, the config op_type field, and the constants (`OP_SET`, `OP_DELETE`). Slice 11 already wires `OP_SET` for the topic-scoped path; reuse those constants.

- [ ] **Step 3: `image.broker(node_id)` — verify it exists**

Slice 15 T3 noted that `broker_exists` doesn't exist; the canonical accessor is `image.broker(n).is_some()`. Confirm:

```
rg "fn broker\(" crates/metadata/src/
```

If `broker(node_id) -> Option<&BrokerRegistrationRecord>` exists, use it. Otherwise check existing slice-15 usages for the right accessor name.

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib incremental_alter_configs
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass. T4's 4 new tests also pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/incremental_alter_configs.rs
git commit -m "$(cat <<'EOF'
feat(broker): IncrementalAlterConfigs broker-scoped path

resource_type=4 (Broker) now handled: validates broker id, checks
allowlist, validates value parses as i64, submits V1BrokerConfig
metadata record. Replaces slice 15 T11's "not supported" stub.

Closes the gap that made kafka-reassign-partitions --verify exit 1.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `DescribeConfigs` broker-resource path

**Files:**
- Modify: `crates/broker/src/handlers/describe_configs.rs`

- [ ] **Step 1: Check current state of broker-resource path**

```
rg "resource_type\|RESOURCE_TYPE_BROKER\|broker_config" crates/broker/src/handlers/describe_configs.rs
```

If the handler currently rejects broker resources or returns empty, this task wires the path through.

- [ ] **Step 2: Wire broker-resource emission**

Within the per-resource loop, when `resource.resource_type == RESOURCE_TYPE_BROKER`:

1. Parse `resource.resource_name` as `NodeId`. Empty → unsupported (return INVALID_REQUEST).
2. Authorize Cluster Describe (same pattern as IncrementalAlterConfigs broker-scoped auth).
3. Read `image.broker_config(node_id).cloned().unwrap_or_default()`.
4. Filter against the `configuration_keys` request field (if provided; empty = all).
5. Emit one `DescribeConfigsResourceResult` per key, with `config_source = DYNAMIC_BROKER_CONFIG (4)`.

The exact response shape (field names, etc.) lives in `crates/protocol/generated/DescribeConfigsResponse.owned.rs` — match its `Default` derive and field set.

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib describe_configs
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/handlers/describe_configs.rs
git commit -m "$(cat <<'EOF'
feat(broker): DescribeConfigs broker-resource path

Reads MetadataImage::broker_config(node_id) and emits per-key
DescribeConfigsResourceResult entries with config_source=4
(DYNAMIC_BROKER_CONFIG). Cluster Describe authorize.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 4 — Background task + spawn (sequential: T7 then T8)

### Task 7: `throttle/refresh.rs` background task + 2 unit tests

**Files:**
- Create: `crates/broker/src/throttle/refresh.rs`
- Modify: `crates/broker/src/throttle/mod.rs` (add `mod refresh; pub use refresh::run;`)

- [ ] **Step 1: Write the module**

```rust
//! Background task that subscribes to MetadataImage changes and
//! updates the throttle bucket rates. Runs unconditionally on every
//! broker; the bucket itself handles the unthrottled fast path.

use std::sync::Arc;

use async_trait::async_trait;
use crabka_metadata::{MetadataImage, NodeId, ThrottleKind};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::bucket::ThrottleState;

#[async_trait]
pub(crate) trait ImageWatcher: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
}

pub async fn run(
    controller: Arc<dyn ImageWatcher>,
    node_id: NodeId,
    throttle: Arc<ThrottleState>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    // Apply initial state.
    apply_image(&controller.current_image(), node_id, &throttle);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!("throttle refresh task shutting down");
                return;
            }
            r = watcher.changed() => {
                if r.is_err() {
                    info!("throttle refresh task: image channel closed");
                    return;
                }
            }
        }
        apply_image(&controller.current_image(), node_id, &throttle);
    }
}

fn apply_image(image: &MetadataImage, node_id: NodeId, throttle: &ThrottleState) {
    let leader_rate = image.broker_throttle_rate(node_id, ThrottleKind::Leader).unwrap_or(0);
    let follower_rate = image
        .broker_throttle_rate(node_id, ThrottleKind::Follower)
        .unwrap_or(0);
    if throttle.leader_out.rate() != leader_rate {
        debug!(node_id, leader_rate, "throttle: leader-out rate update");
        throttle.leader_out.set_rate(leader_rate);
    }
    if throttle.follower_in.rate() != follower_rate {
        debug!(node_id, follower_rate, "throttle: follower-in rate update");
        throttle.follower_in.set_rate(follower_rate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{BrokerConfigRecord, MetadataRecord};
    use uuid::Uuid;

    #[test]
    fn apply_image_sets_rates() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "follower.replication.throttled.rate".into(),
            config_value: Some("1024".into()),
        }));
        let throttle = ThrottleState::new();
        apply_image(&img, 1, &throttle);
        assert_eq!(throttle.leader_out.rate(), 2048);
        assert_eq!(throttle.follower_in.rate(), 1024);
    }

    #[test]
    fn apply_image_resets_to_zero_when_config_deleted() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        let throttle = ThrottleState::new();
        apply_image(&img, 1, &throttle);
        assert_eq!(throttle.leader_out.rate(), 2048);
        // Delete the config.
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: None,
        }));
        apply_image(&img, 1, &throttle);
        assert_eq!(throttle.leader_out.rate(), 0);
    }
}
```

- [ ] **Step 2: Wire submodule in `throttle/mod.rs`**

```rust
mod refresh;
pub use refresh::{run, ImageWatcher};
```

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib throttle::refresh
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 2 new tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/throttle/
git commit -m "$(cat <<'EOF'
feat(broker): throttle refresh background task

Subscribes to ControllerHandle::watch_image; on each image change,
reads broker_throttle_rate for both leader and follower and pushes
to the ThrottleState buckets. Image-driven (not timer-driven).
Spawned from Broker::start in task 8.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: `Broker::start` spawn + `Broker.throttle_state`

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Add `ThrottleState` field on `Broker`**

Find `pub struct Broker { ... }`. Append:

```rust
    pub throttle_state: std::sync::Arc<crate::throttle::ThrottleState>,
```

(Look at slice 15's struct shape for the local convention. Some fields may be `pub`, others `pub(crate)`.)

- [ ] **Step 2: Add a `ThrottleControllerAdapter` adapter**

Anywhere in `broker.rs` near slice 15's `ReassignmentControllerAdapter`:

```rust
struct ThrottleControllerAdapter {
    handle: std::sync::Arc<crabka_raft::ControllerHandle>,
}

#[async_trait::async_trait]
impl crate::throttle::ImageWatcher for ThrottleControllerAdapter {
    fn current_image(&self) -> std::sync::Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }
    fn watch_image(&self) -> tokio::sync::watch::Receiver<std::sync::Arc<crabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }
}
```

- [ ] **Step 3: Spawn the refresh task in `Broker::start`**

After slice 15 T8's reassignment-task spawn (search for `crate::reassignment::run`):

```rust
        // KIP-73 throttle refresh task. Always-on; the bucket itself
        // has a rate-0 fast path so unthrottled clusters pay nothing.
        let throttle_state = std::sync::Arc::new(crate::throttle::ThrottleState::new());
        {
            let throttle = throttle_state.clone();
            let watcher: std::sync::Arc<dyn crate::throttle::ImageWatcher> =
                std::sync::Arc::new(ThrottleControllerAdapter {
                    handle: controller.clone(),
                });
            let shutdown = supervisor_shutdown.child_token();
            let node_id = config.node_id;
            tokio::spawn(crate::throttle::run(watcher, node_id, throttle, shutdown));
        }
```

- [ ] **Step 4: Wire `throttle_state` into the `Broker` construction**

Wherever `Broker { ... }` is constructed (later in `Broker::start`), pass `throttle_state` as the new field. If the construction is via a builder, add a builder method.

- [ ] **Step 5: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass. The refresh task starts on every broker; buckets stay at rate 0 until a config writes a non-zero value.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "$(cat <<'EOF'
feat(broker): spawn throttle refresh task in Broker::start

ThrottleControllerAdapter wraps ControllerHandle. Refresh task
spawned unconditionally; rate-0 fast path makes it free when
unthrottled. Broker struct gains pub throttle_state field for
Fetch handler + replicator to consult.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 5 — Fetch enforcement (parallel: T9, T10)

### Task 9: Leader-side throttle in `fetch.rs`

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Read the existing handler**

```
rg "fn handle\|replica_id\|assembled\|fetch_responses" crates/broker/src/handlers/fetch.rs
```

Identify:
- Where the per-partition response chunks are assembled.
- Where the final response is encoded.
- Where `replica_id` is extracted from the request.

The throttle hook goes between assembly and encoding.

- [ ] **Step 2: Add the throttle hook**

After the per-partition assembly loop, before encoding:

```rust
// KIP-73 leader-side throttle.
let fetcher_id = req.replica_id;
if fetcher_id >= 0 {
    use crate::throttle::TopicThrottle;
    let node_id = broker.config.node_id;
    let mut throttled_byte_count: u64 = 0;
    let mut throttled_idxs: Vec<usize> = Vec::new();
    for (idx, partition_response) in /* iterate the assembled response */ {
        let topic = /* topic name from row */;
        let partition = /* partition id */;
        let chunk_bytes = /* record-set byte length */;
        let throttle = TopicThrottle::for_topic(&image, topic);
        if throttle.leader.contains(partition, node_id) {
            throttled_byte_count += chunk_bytes as u64;
            throttled_idxs.push(idx);
        }
    }
    if throttled_byte_count > 0 {
        let granted = broker.throttle_state.leader_out.try_consume(throttled_byte_count);
        if granted < throttled_byte_count {
            truncate_throttled_responses(/* assembled */, &throttled_idxs, granted);
        }
    }
}
```

`truncate_throttled_responses` walks `throttled_idxs` in order and drops whole partition chunks from the response until the remaining throttled bytes fit within `granted`. **Do not** mid-batch truncate — drop whole partition chunks.

Pseudocode:

```rust
fn truncate_throttled_responses(
    responses: &mut [/* response row type */],
    throttled_idxs: &[usize],
    budget: u64,
) {
    let mut remaining = budget;
    for &idx in throttled_idxs {
        let chunk_size = responses[idx].records.as_ref().map(|r| r.len()).unwrap_or(0) as u64;
        if chunk_size <= remaining {
            remaining -= chunk_size;
        } else {
            // Drop this chunk (and all subsequent throttled ones).
            responses[idx].records = None;
        }
    }
}
```

(Match the actual response-row struct field names. The records field may be `Bytes`, `Option<Bytes>`, or similar — adapt accordingly.)

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib fetch
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass. The throttle hook only fires when `replica_id >= 0` AND the partition is in `leader.replication.throttled.replicas` AND the rate is > 0 — none of these are true in slice 10b/11/12 tests, so no regression.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/handlers/fetch.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-73 leader-side throttle on Fetch response

When a partition is in leader.replication.throttled.replicas AND the
fetcher (replica_id) is included, response bytes are capped by the
leader-out token bucket. Whole-chunk drop (no mid-batch truncation
since Kafka clients expect complete record batches).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Follower-side throttle in replicator

**Files:**
- Modify: `crates/broker/src/replicator.rs` (or `replicator_supervisor.rs` — wherever the outbound Fetch request is built)

- [ ] **Step 1: Identify the Fetch-issue site**

```
rg "fn fetch\|FetchRequest\|max_bytes" crates/broker/src/replicator*.rs
```

Find the function that constructs a `FetchRequest` and sends it to the leader.

- [ ] **Step 2: Add the throttle hook**

Before constructing the request body's `max_bytes` (or `partition_max_bytes`):

```rust
// KIP-73 follower-side throttle.
let throttle = crate::throttle::TopicThrottle::for_topic(&image, &topic);
let throttled = throttle.follower.contains(partition, self.node_id);
let max_bytes_cap = if throttled && self.throttle_state.follower_in.rate() > 0 {
    let granted = self.throttle_state.follower_in.try_consume(default_max_bytes);
    if granted == 0 {
        debug!(topic, partition, "follower throttle: skip fetch this round");
        return;  // or wherever the per-partition skip flow lives
    }
    granted as i32
} else {
    default_max_bytes
};
// ... build FetchRequest with max_bytes = max_bytes_cap ...
```

`self.throttle_state` — the replicator gets the `ThrottleState` Arc passed in at construction. Look at how slice 10b's replicator is built; add `throttle_state: Arc<ThrottleState>` to its constructor + struct.

If the replicator builds one Fetch request that covers multiple partitions, the throttle accounting is more complex: cap the SUM of `partition_max_bytes` across all throttled partitions. Either:
- (a) Issue separate Fetches per throttled partition (simpler, less efficient)
- (b) Per-partition `max_bytes` caps with shared bucket consumption (precise, more code)

For slice 15b, go with (a) — issue throttled-partition Fetches separately. Document this in a code comment so future readers know it's a deliberate simplification.

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib replicator
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/
git commit -m "$(cat <<'EOF'
feat(broker): KIP-73 follower-side throttle on outbound Fetch

When a partition is in follower.replication.throttled.replicas AND
this broker is a follower, the outbound Fetch's max_bytes is capped
by the follower-in token bucket. Throttled partitions are fetched
in separate requests to keep the bucket accounting simple.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 6 — Integration tests + JVM + final (sequential)

### Task 11: 4 broker integration tests

**Files:**
- Create: `crates/broker/tests/throttle.rs`

- [ ] **Step 1: File scaffold**

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

// Helpers copied from slice 14/15 — see `elect_leaders.rs` / `partition_reassignment.rs`.
```

Copy these helpers verbatim from `crates/broker/tests/elect_leaders.rs` or `partition_reassignment.rs`:
- `round_trip` (raw wire request/response framing)
- `start_single_broker_sasl_plaintext_with_users(super_user, users)`
- `sasl_plain_authenticate`
- `create_topic_as_admin(addr, topic, partitions, rf)`
- `start_three_broker_plaintext_cluster()` (or its 2-broker variant if available)
- `wait_partition_exists`

Also need wire drivers for the four configs RPCs:

```rust
async fn drive_incremental_alter_configs(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    resources: Vec<(/* resource_type */ i8, /* name */ String, Vec<(String, Option<String>, /* op */ i8)>)>,
) -> i16 /* top-level error_code */ { ... }

async fn drive_describe_configs(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    resources: Vec<(i8, String)>,
) -> Vec<(i16 /* per-resource error */, Vec<(String, String)>)> { ... }
```

(Reuse slice 11's IncrementalAlterConfigsRequest / DescribeConfigsRequest generated types. Build the request bodies, send via `round_trip` to api_keys 44 and 32 respectively.)

- [ ] **Step 2: Test 1 — `broker_scoped_alter_persists_in_image`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_scoped_alter_persists_in_image() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin", &[("admin", "admin-secret")],
    ).await;

    let err = drive_incremental_alter_configs(
        addr, "admin", "admin-secret",
        vec![(
            /*resource_type=Broker*/ 4,
            handle.node_id().to_string(),
            vec![("leader.replication.throttled.rate".into(), Some("2048".into()), /*OP_SET*/ 0)],
        )],
    ).await;
    assert_eq!(err, 0, "alter should succeed");

    // Poll the image until the config is visible.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();  // see slice 14 accessor
        if img.broker_throttle_rate(handle.node_id(), crabka_metadata::ThrottleKind::Leader) == Some(2048) {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("config not visible in image");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
```

(`controller_image_for_test` may need to be added to `BrokerHandle` — check slice 14 / slice 15 accessors. If absent, add a minimal accessor in this task.)

- [ ] **Step 3: Test 2 — `topic_throttle_config_propagates`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_throttle_config_propagates() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin", &[("admin", "admin-secret")],
    ).await;
    create_topic_as_admin(addr, "foo", 1, 1).await;
    wait_partition_exists(&handle, "foo", 0).await;

    let err = drive_incremental_alter_configs(
        addr, "admin", "admin-secret",
        vec![(
            /*Topic*/ 2,
            "foo".into(),
            vec![("leader.replication.throttled.replicas".into(), Some("0:1,0:2".into()), 0)],
        )],
    ).await;
    assert_eq!(err, 0);

    // Verify via accessor.
    let img = handle.controller_image_for_test();
    let throttle = crabka_broker::throttle::TopicThrottle::for_topic(&img, "foo");
    assert!(throttle.leader.contains(0, 1));
    assert!(throttle.leader.contains(0, 2));
}
```

- [ ] **Step 4: Test 3 — `throttle_rate_caps_fetch_response_size`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_rate_caps_fetch_response_size() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;
    // ... create rf=1 topic, set broker leader-rate=512 and topic throttled replicas list ...
    // ... produce 8 KB via wire ...
    // ... issue Fetch with replica_id=2 ...
    // ... assert response size <= ~1KB ...
}
```

The actual byte assertions are loose because of batch-header overhead. Test should fail clearly if the throttle is silently bypassed (response size >> 1KB).

- [ ] **Step 5: Test 4 — `unthrottled_partition_unaffected`**

Same setup as test 3 but no throttle configs; assert full 8 KB delivered.

- [ ] **Step 6: Run + lints + commit**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test throttle -- --nocapture --test-threads=1"

cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings

git add crates/broker/tests/throttle.rs
git commit -m "$(cat <<'EOF'
test(broker): KIP-73 throttle integration tests

Four broker-side tests: broker-scoped alter persists in image,
topic throttle config propagates, leader-side rate caps Fetch
response size, unthrottled topic unaffected.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: JVM acceptance + slice 15 regression closure

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append `jvm_kafka_reassign_partitions_with_throttle_end_to_end`**

Pattern after slice 15 T11's `jvm_kafka_reassign_partitions_end_to_end`. Differences:
- Add `--throttle 1024` to the `kafka-reassign-partitions --execute` invocation.
- Before `--verify`, also run `kafka-configs --describe --entity-type brokers --entity-name 1` and assert `leader.replication.throttled.rate=1024` appears in stdout.
- After completion (metadata-inject ISR), run `--verify` and assert `out.status.success()` (no exit-1 throttle-clearing failure).
- Assert throttle configs cleared from image after `--verify` (poll `image.broker_throttle_rate(1, Leader)` until `None`).

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_reassign_partitions_with_throttle_end_to_end() {
    // ... 3-broker SASL/PLAINTEXT cluster, create rf=2 topic ...
    // ... write reassignment.json ...

    // Execute with --throttle.
    let out = std::process::Command::new("docker")
        .args(["run", "--rm", "-v", &admin_mount, "-v", &json_mount,
               "--add-host=host.docker.internal:host-gateway",
               KAFKA_IMAGE_TXN,
               "kafka-reassign-partitions", "--execute",
               "--reassignment-json-file", "/reassignment.json",
               "--throttle", "1024",
               "--bootstrap-server", BOOTSTRAP,
               "--command-config", "/client.properties"])
        .output().expect("spawn");
    assert!(out.status.success(), "execute --throttle failed: {}", String::from_utf8_lossy(&out.stderr));

    // Verify configs were applied via kafka-configs.
    let desc = std::process::Command::new("docker")
        .args(["run", "--rm", "-v", &admin_mount,
               "--add-host=host.docker.internal:host-gateway",
               KAFKA_IMAGE_TXN,
               "kafka-configs", "--describe",
               "--entity-type", "brokers", "--entity-name", "1",
               "--bootstrap-server", BOOTSTRAP,
               "--command-config", "/client.properties"])
        .output().expect("spawn");
    let desc_stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(desc_stdout.contains("leader.replication.throttled.rate=1024"),
            "config not visible: {desc_stdout}");

    // Inject ISR to complete the reassignment (slice 15 idiom).
    // ... (same as slice 15 T11) ...

    // Verify exits success — no longer 1 due to the throttle-clearing IncrementalAlterConfigs.
    let verify_out = std::process::Command::new("docker")
        .args(["run", "--rm", "-v", &admin_mount, "-v", &json_mount,
               "--add-host=host.docker.internal:host-gateway",
               KAFKA_IMAGE_TXN,
               "kafka-reassign-partitions", "--verify",
               "--reassignment-json-file", "/reassignment.json",
               "--bootstrap-server", BOOTSTRAP,
               "--command-config", "/client.properties"])
        .output().expect("spawn");
    assert!(verify_out.status.success(),
            "verify failed (slice 15b should fix this): stderr={}",
            String::from_utf8_lossy(&verify_out.stderr));

    // Confirm throttle configs cleared from image.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        if img.broker_throttle_rate(1, crabka_metadata::ThrottleKind::Leader).is_none() {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("throttle config not cleared from image after --verify");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

- [ ] **Step 2: Close slice 15's `jvm_kafka_reassign_partitions_end_to_end` known-limitation regression**

Find the existing test in `jvm_acceptance.rs`. Locate the line that asserts on `verify_out.stdout.contains(...)` instead of `verify_out.status.success()`. Replace with:

```rust
assert!(
    verify_out.status.success(),
    "verify failed: stderr={}",
    String::from_utf8_lossy(&verify_out.stderr),
);
```

This passes now that slice 15b supports broker-scoped throttle-clearing.

- [ ] **Step 3: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1"
```

Both `jvm_kafka_reassign_partitions_end_to_end` (slice 15) and `jvm_kafka_reassign_partitions_with_throttle_end_to_end` (slice 15b) PASS.

- [ ] **Step 4: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "$(cat <<'EOF'
test(jvm): kafka-reassign-partitions --throttle end-to-end

New jvm_kafka_reassign_partitions_with_throttle_end_to_end test
covers KIP-73 throttle round-trip including kafka-configs visibility
and --verify exit 0.

Closes slice 15 T11 known limitation: the existing
jvm_kafka_reassign_partitions_end_to_end test now asserts
verify_out.status.success() instead of stdout substring.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: Sweep + docs + PR

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

- [ ] **Step 2: `README.md` — append slice 15b entry**

```markdown
- **Slice 15b** — KIP-73 throttled replication: `IncrementalAlterConfigs` now
  handles broker-scoped (`resource_type=4`); `*.throttled.replicas` (topic) +
  `*.throttled.rate` (broker) configs persist and are surfaced via
  `DescribeConfigs`. A token-bucket rate limiter on the Fetch path enforces
  both leader and follower throttles. JVM `kafka-reassign-partitions --throttle`
  works end-to-end including `--verify` exit 0. Metrics emission deferred
  to a future observability slice.
```

- [ ] **Step 3: `STATUS.md` — append section**

```markdown
## Slice 15b — Replication throttling (2026-05-15)

- `IncrementalAlterConfigs` broker-scoped (`resource_type=4`) accepts the
  two KIP-73 rate configs (`leader.replication.throttled.rate`,
  `follower.replication.throttled.rate`); other broker keys still rejected.
- Topic-level `*.throttled.replicas` configs added to the validator
  allowlist; values parse via `ThrottledReplicas` enum (none / `*` / pair list).
- New `BrokerConfigRecord` + `V1BrokerConfig` metadata record carries
  per-broker key/value pairs. `MetadataImage::broker_configs` map +
  `broker_throttle_rate` accessor. 4 unit tests.
- `TokenBucket` in `crates/broker/src/throttle/bucket.rs` — one-second burst
  capacity at configured rate, rate-0 fast path for unthrottled. 6 unit
  tests covering refill / drain / cap / set.
- Background refresh task in `throttle/refresh.rs` subscribes to
  `controller.watch_image()` and pushes rate updates to the buckets on every
  image apply. 2 unit tests via a mock `ImageWatcher`.
- Fetch handler: leader-side enforcement caps response bytes when partitions
  in `leader.replication.throttled.replicas` are fetched by listed followers.
  Whole-partition-chunk drop (no mid-batch truncation).
- Replicator: follower-side enforcement caps `max_bytes` in outgoing Fetch
  requests when this broker is a throttled follower.
- `DescribeConfigs` broker-resource path now emits per-broker configs
  with `config_source=DYNAMIC_BROKER_CONFIG (4)`.
- 4 broker integration tests: broker-scoped alter persists, topic throttle
  propagates, rate caps Fetch response size, unthrottled topic unaffected.
- New JVM acceptance test `jvm_kafka_reassign_partitions_with_throttle_end_to_end`
  exercises the full KIP-73 round-trip including `kafka-configs --describe`
  visibility check.
- Closes slice 15 T11 known limitation: `kafka-reassign-partitions --verify`
  now exits 0 cleanly; slice 15's JVM test updated to assert on exit status.
- Out of scope: metrics emission, dynamic reload of non-throttle configs,
  per-listener config refresh.
```

- [ ] **Step 4: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "$(cat <<'EOF'
docs(slice-15b): README + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Push + open PR**

```
git push -u origin feature/replication-throttling-15b
gh pr create --base main --head feature/replication-throttling-15b \
  --title "Slice 15b: Replication throttling (KIP-73)" \
  --body "$(cat <<'EOF'
## Summary

KIP-73 throttled inter-broker replication:

1. **Configs persist** — \`IncrementalAlterConfigs\` broker-scoped (\`resource_type=4\`) now accepted; \`*.throttled.replicas\` (topic) and \`*.throttled.rate\` (broker) keys validated and stored in metadata.
2. **Enforcement** — token-bucket rate limiter on the Fetch hot path. Leader caps response bytes for throttled (partition, follower) pairs; follower caps outgoing Fetch \`max_bytes\` for throttled (partition, self) pairs.
3. **Visibility** — \`DescribeConfigs\` surfaces both topic and broker configs.

JVM \`kafka-reassign-partitions --throttle\` works end-to-end including \`--verify\` exit 0 (closes slice 15 T11 known limitation).

## Verified

- 22 new unit tests (config parsing, token bucket, image apply, validator allowlist, refresh task).
- 4 broker integration tests in \`tests/throttle.rs\`.
- 1 new JVM acceptance test; the existing slice-15 JVM test updated to assert on exit code now that \`--verify\` lands cleanly.
- Workspace \`cargo fmt --check\`, \`cargo clippy --workspace --all-targets -- -D warnings\`, \`cargo test --workspace\` all green.

## Out of scope

Metrics emission (deferred to a dedicated observability slice; Crabka has no metrics framework yet), dynamic reload of non-throttle configs, per-listener config refresh.

## Plan / spec

- Spec: \`docs/superpowers/specs/2026-05-15-crabka-replication-throttling-15b-design.md\`
- Plan: \`docs/superpowers/plans/2026-05-15-crabka-replication-throttling-15b.md\` (13 tasks across 6 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Capture PR URL** and return.

---

## Notes for the executing agent

1. **CLAUDE.md compatibility rule** governs T1 in particular — no `#[serde(default)]` shim on the new `BrokerConfigRecord`. Wipe data dirs across the slice boundary.

2. **Parallel batches** (per CLAUDE.md `## Execution`):
   - B1 (T1 + T2): T1 touches `crates/metadata/`, T2 touches `crates/broker/src/throttle.rs` (new) + `lib.rs` (one append). Disjoint — parallel.
   - B2 (T3 + T4): T3 creates `throttle/bucket.rs`, T4 modifies `config_keys.rs` + adds helpers in `incremental_alter_configs.rs`. Disjoint — parallel.
   - B3 (T5 + T6): T5 modifies `incremental_alter_configs.rs` body, T6 modifies `describe_configs.rs`. **WARNING:** T5 modifies the same file as T4's helpers. T4 adds helpers at the module level; T5 modifies the handler body. They can still parallelize if both edit different sections of the file using `Edit` with surrounding context (no `replace_all`). If conflicts arise, fall back to sequential T5 after T4.
   - B4 (T7 → T8): sequential. T8 spawns T7's task.
   - B5 (T9 + T10): different files. Parallel.
   - B6 (T11 → T12 → T13): sequential. T12 builds on T11's tests; T13 wraps up.

3. **TokenBucket atomic ordering** — `Relaxed` is fine because the throttle is statistical (KIP-73 explicitly says "approximate"). Under contention, the bucket may briefly grant slightly more than the configured rate. Don't over-engineer with CAS loops.

4. **Replicator structural change** — slice 10b's replicator may need a struct field for `Arc<ThrottleState>`. Wire it through from `Broker::start` to whatever constructs the replicator. Slice 10b precedent is the replicator-supervisor.

5. **Fetch handler `replica_id` extraction** — already done by slice 10b for ISR purposes. Don't reinvent. Read the existing code.

6. **`controller_image_for_test` accessor** — if not present on `BrokerHandle`, add a one-liner in T11 (similar to slice 14's `partition_leader_for_test`). Or use the existing `partition_record_for_test` accessor (slice 15 T9) — though that only returns one partition's record, not the whole image. Adding `image_for_test` is cleaner.

7. **`once_cell::sync::Lazy`** — verify the workspace already depends on `once_cell`. If not, use `std::sync::OnceLock` (stable since 1.70).
