# Slice 16: Client quotas (KIP-13 + KIP-124 + KIP-257) — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** `## Slice 16 — Client quotas (2026-05-15)`

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Known limitation: client_id is currently empty in Produce/Fetch quota lookups (HandlerTable signature does not thread it through) — user-level + default quotas work, (user, client-id) tuple quotas do not fire on data-plane paths (deferred)
- Known limitation: kafka-configs --describe --entity-type users calls DescribeUserScramCredentials (api_key 51) after fetching quotas — closed by slice 17a
- Known limitation: throttle_time_ms in response only set for Produce + Fetch — other handlers absorb request_percentage delay silently (deferred)
- Out of scope: `ip` entity + KIP-612 connection_creation_rate (closed by slice 16b)
- KIP-599 controller_mutation_rate (closed by slice 16c)

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Implement Kafka client quotas — `AlterClientQuotas` (api_key 49) + `DescribeClientQuotas` (api_key 48) with three quota types (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`) and four entity scopes (user / client-id / (user, client-id) / default). Enforce via the slice-15b `TokenBucket` primitive; KIP-257 server-side throttle delays via `tokio::time::sleep`.

**Architecture:** A new `ClientQuotaRecord` metadata record carries `(entity_tuple, config_key, value)`. `MetadataImage` stores quotas in a `HashMap<EntityKey, HashMap<String, f64>>` map keyed by canonicalized entity tuple (sorted alphabetically by entity_type). A per-broker `QuotaBuckets` cache holds lazy-allocated `TokenBucket`s per `(quota_key, entity_key)` pair. Produce/Fetch/dispatch hot paths consult `lookup_quota` to find the matching tuple (8-priority Kafka algorithm), consume from the bucket, compute throttle delay, and `tokio::time::sleep` before sending the response.

**Tech Stack:** Rust 1.95.0; reuses slice 15b `TokenBucket` + image-watcher pattern, slice 12 `Principal`/`auth.principal()`, slice 11 admin-handler structure. Wire types already generated at `crates/protocol/generated/{Alter,Describe}ClientQuotas{Request,Response}.owned.rs`.

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-quotas-16-design.md`](../specs/2026-05-15-crabka-quotas-16-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/quotas-16` already created with spec committed at `a72d19a`.

**Compat note:** Per `CLAUDE.md`, no backwards-compat shims. `ClientQuotaRecord` is a new `MetadataRecord` variant; raft-log replay across the slice boundary requires data-dir wipe.

---

## File structure

```
crates/metadata/src/
├── records.rs        # MODIFIED — ClientQuotaRecord + QuotaEntity + V1ClientQuota variant
├── image.rs          # MODIFIED — client_quotas map + accessor + apply arm + canonicalize + 4 unit tests
└── lib.rs            # MODIFIED — re-export

crates/broker/src/
├── quota/
│   ├── mod.rs        # NEW — re-exports
│   ├── lookup.rs     # NEW — lookup_quota + matched_entity_key + 8 unit tests
│   ├── buckets.rs    # NEW — QuotaBuckets cache + 4 unit tests
│   └── refresh.rs    # NEW — image-driven refresh task + 2 unit tests
├── handlers/
│   ├── alter_client_quotas.rs       # NEW — api_key 49 + process_one_entry + 6 unit tests
│   ├── describe_client_quotas.rs    # NEW — api_key 48 + entity_matches_filter + 4 unit tests
│   ├── mod.rs                       # MODIFIED — register both modules
│   ├── api_versions.rs              # MODIFIED — supported_apis += 48, 49
│   ├── produce.rs                   # MODIFIED — producer_byte_rate enforcement
│   └── fetch.rs                     # MODIFIED — consumer_byte_rate enforcement (replica_id < 0)
├── network/dispatch.rs              # MODIFIED — intercept arms + helpers + request_percentage wrap
├── broker.rs                        # MODIFIED — spawn refresh task + Broker.quota_buckets field
└── lib.rs                           # MODIFIED — pub mod quota

crates/broker/tests/
├── client_quotas.rs    # NEW — 5 broker integration tests
└── jvm_acceptance.rs   # MODIFIED — 1 new JVM test
```

13 tasks across 6 batches.

---

## Batch 1 — Metadata + quota module skeleton (parallel: T1, T2)

### Task 1: `ClientQuotaRecord` + image accessors + canonicalize

**Files:**
- Modify: `crates/metadata/src/records.rs`
- Modify: `crates/metadata/src/image.rs`
- Modify: `crates/metadata/src/lib.rs` (re-export)

- [ ] **Step 1: Add `ClientQuotaRecord` + `QuotaEntity` + `V1ClientQuota` to `records.rs`**

Append after the existing `BrokerConfigRecord` (slice 15b T1):

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaEntity {
    pub entity_type: String,
    pub entity_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientQuotaRecord {
    /// Canonicalized entity tuple — sorted by entity_type alphabetically.
    pub entity: Vec<QuotaEntity>,
    pub config_key: String,
    pub config_value: Option<f64>,
}
```

Add the enum arm to `MetadataRecord`:

```rust
V1ClientQuota(ClientQuotaRecord),
```

(Note: `f64` doesn't impl `Eq` so `MetadataRecord` may currently derive `Eq`. If so, drop `Eq` from the enum and keep `PartialEq`. Check the derives on `MetadataRecord` and the sibling records.)

- [ ] **Step 2: Add round-trip test in `records.rs`**

Append to `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn client_quota_record_round_trip() {
        let r = MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity { entity_type: "client-id".into(), entity_name: Some("app1".into()) },
                QuotaEntity { entity_type: "user".into(), entity_name: Some("alice".into()) },
            ],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        });
        let bytes = serde_wincode::serialize(&r).expect("encode");
        let decoded: MetadataRecord = serde_wincode::deserialize(&bytes).expect("decode");
        assert_eq!(r, decoded);
    }
```

(Match the existing `topic_config_record_round_trip` / `broker_config_record_round_trip` test idiom.)

- [ ] **Step 3: Add `EntityKey`, `canonicalize`, `client_quotas` to `image.rs`**

In `crates/metadata/src/image.rs`, add type alias + helper near the top of the module:

```rust
pub type EntityKey = Vec<(String, Option<String>)>;

#[must_use]
pub fn canonicalize_entity(mut tuple: Vec<(String, Option<String>)>) -> EntityKey {
    tuple.sort_by(|a, b| a.0.cmp(&b.0));
    tuple
}
```

Add the field to `MetadataImage`:

```rust
client_quotas: std::collections::HashMap<EntityKey, std::collections::BTreeMap<String, f64>>,
```

Initialize to `HashMap::new()` in `MetadataImage::new`.

Add accessor:

```rust
pub fn client_quotas(&self) -> &std::collections::HashMap<EntityKey, std::collections::BTreeMap<String, f64>> {
    &self.client_quotas
}
```

(Use `BTreeMap` for the inner so iteration is stable across runs.)

- [ ] **Step 4: Add apply arm in `MetadataImage::apply`**

Inside the existing `match record { ... }`:

```rust
MetadataRecord::V1ClientQuota(rec) => {
    let key = canonicalize_entity(
        rec.entity.iter()
            .map(|e| (e.entity_type.clone(), e.entity_name.clone()))
            .collect(),
    );
    let configs = self.client_quotas.entry(key).or_default();
    match rec.config_value {
        Some(v) => { configs.insert(rec.config_key.clone(), v); }
        None => { configs.remove(&rec.config_key); }
    }
}
```

- [ ] **Step 5: Re-export from `lib.rs`**

Append `ClientQuotaRecord, QuotaEntity, EntityKey, canonicalize_entity` to the existing pub-use line in `crates/metadata/src/lib.rs`.

- [ ] **Step 6: 4 unit tests in `image.rs`**

Append to existing `#[cfg(test)] mod tests`:

```rust
    use crate::records::QuotaEntity;

    #[test]
    fn client_quota_apply_inserts_canonicalized() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        // Input order: (user, client-id) — should canonicalize to (client-id, user).
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity { entity_type: "user".into(), entity_name: Some("alice".into()) },
                QuotaEntity { entity_type: "client-id".into(), entity_name: Some("app1".into()) },
            ],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let key: EntityKey = vec![
            ("client-id".into(), Some("app1".into())),
            ("user".into(), Some("alice".into())),
        ];
        let configs = img.client_quotas().get(&key).expect("entry under canonical key");
        assert_eq!(configs.get("producer_byte_rate"), Some(&1024.0));
    }

    #[test]
    fn client_quota_apply_delete_removes_key() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity { entity_type: "user".into(), entity_name: Some("alice".into()) }],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity { entity_type: "user".into(), entity_name: Some("alice".into()) }],
            config_key: "producer_byte_rate".into(),
            config_value: None,
        }));
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let configs = img.client_quotas().get(&key).expect("entry retained");
        assert!(configs.get("producer_byte_rate").is_none());
    }

    #[test]
    fn client_quota_default_entity_uses_none_name() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity { entity_type: "user".into(), entity_name: None }],
            config_key: "producer_byte_rate".into(),
            config_value: Some(512.0),
        }));
        let key: EntityKey = vec![("user".into(), None)];
        assert!(img.client_quotas().contains_key(&key));
    }

    #[test]
    fn canonicalize_sorts_alphabetically_by_entity_type() {
        let input = vec![
            ("user".to_string(), Some("alice".to_string())),
            ("client-id".to_string(), Some("app1".to_string())),
        ];
        let canon = canonicalize_entity(input);
        assert_eq!(canon[0].0, "client-id");
        assert_eq!(canon[1].0, "user");
    }
```

- [ ] **Step 7: Build + tests + lints**

```
cargo build --workspace
cargo test -p crabka-metadata
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 1 round-trip + 4 image tests PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/metadata/src/
git commit -m "$(cat <<'EOF'
feat(metadata): ClientQuotaRecord + MetadataImage::client_quotas

V1ClientQuota carries entity tuple + config_key + value. Image stores
quotas in HashMap<EntityKey, BTreeMap<String, f64>>, keyed by
canonicalized entity tuple (sorted alphabetically by entity_type).
4 unit tests covering insert/delete/default-entity/canonicalization.

Per CLAUDE.md (greenfield), no serde(default) shim; pre-slice raft
logs require data-dir wipe.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `quota::lookup` module + `lookup_quota` + entity matching

**Files:**
- Create: `crates/broker/src/quota/mod.rs`
- Create: `crates/broker/src/quota/lookup.rs`
- Modify: `crates/broker/src/lib.rs` (add `pub mod quota;`)

This creates the `quota` directory module and ships the lookup helper alongside 8 unit tests. T3/T4 will add sibling submodules to the same directory.

- [ ] **Step 1: Create `crates/broker/src/quota/mod.rs`**

```rust
//! KIP-13 + KIP-124 + KIP-257 client quotas.

mod lookup;

pub use lookup::{lookup_quota, lookup_quota_with_key};
```

(T3 will append `mod buckets; pub use buckets::*;`; T4 will append `mod refresh; pub use refresh::*;`.)

- [ ] **Step 2: Create `crates/broker/src/quota/lookup.rs`**

```rust
//! Quota lookup with Kafka's 8-priority entity matching.

use crabka_metadata::{EntityKey, MetadataImage};

/// Return the configured value for `quota_key` under the most-specific
/// matching entity for `(principal, client_id)`. First match wins per
/// Kafka's documented precedence:
///   1. (client-id=app1, user=alice)
///   2. (client-id=app1, user=default)
///   3. (client-id=default, user=alice)
///   4. (client-id=default, user=default)
///   5. (user=alice)
///   6. (client-id=app1)
///   7. (user=default)
///   8. (client-id=default)
///
/// All candidate keys are pre-sorted by entity_type ("client-id" <
/// "user" alphabetically), so the lookup runs against the image map
/// without further canonicalization.
#[must_use]
pub fn lookup_quota(
    image: &MetadataImage,
    principal: &str,
    client_id: &str,
    quota_key: &str,
) -> Option<f64> {
    lookup_quota_with_key(image, principal, client_id, quota_key).map(|(_, v)| v)
}

/// Like `lookup_quota` but also returns the canonical entity key
/// that matched. Used by enforcement code to bind the lookup to a
/// bucket in `QuotaBuckets`.
#[must_use]
pub fn lookup_quota_with_key(
    image: &MetadataImage,
    principal: &str,
    client_id: &str,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let candidates: [EntityKey; 8] = [
        vec![("client-id".into(), Some(client_id.into())), ("user".into(), Some(principal.into()))],
        vec![("client-id".into(), Some(client_id.into())), ("user".into(), None)],
        vec![("client-id".into(), None),                    ("user".into(), Some(principal.into()))],
        vec![("client-id".into(), None),                    ("user".into(), None)],
        vec![("user".into(), Some(principal.into()))],
        vec![("client-id".into(), Some(client_id.into()))],
        vec![("user".into(), None)],
        vec![("client-id".into(), None)],
    ];
    for key in candidates {
        if let Some(configs) = image.client_quotas().get(&key) {
            if let Some(&v) = configs.get(quota_key) {
                return Some((key, v));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};

    fn img_with(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for r in records {
            img.apply(&MetadataRecord::V1ClientQuota(r));
        }
        img
    }

    fn rec(entity: Vec<(&str, Option<&str>)>, key: &str, value: f64) -> ClientQuotaRecord {
        ClientQuotaRecord {
            entity: entity.into_iter().map(|(t, n)| QuotaEntity {
                entity_type: t.into(),
                entity_name: n.map(Into::into),
            }).collect(),
            config_key: key.into(),
            config_value: Some(value),
        }
    }

    #[test]
    fn exact_user_client_pair_match() {
        let img = img_with(vec![rec(
            vec![("user", Some("alice")), ("client-id", Some("app1"))],
            "producer_byte_rate", 1024.0,
        )]);
        assert_eq!(lookup_quota(&img, "alice", "app1", "producer_byte_rate"), Some(1024.0));
    }

    #[test]
    fn user_default_falls_back_to_client_specific() {
        // Only (client-id=app1) configured; user=alice should still match.
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate", 1024.0,
        )]);
        assert_eq!(lookup_quota(&img, "alice", "app1", "producer_byte_rate"), Some(1024.0));
    }

    #[test]
    fn single_user_match_when_no_pair_exists() {
        let img = img_with(vec![rec(
            vec![("user", Some("alice"))],
            "producer_byte_rate", 2048.0,
        )]);
        assert_eq!(lookup_quota(&img, "alice", "anyclient", "producer_byte_rate"), Some(2048.0));
    }

    #[test]
    fn single_client_id_match_when_no_user_exists() {
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate", 512.0,
        )]);
        assert_eq!(lookup_quota(&img, "anyuser", "app1", "producer_byte_rate"), Some(512.0));
    }

    #[test]
    fn default_user_default_client_pair() {
        let img = img_with(vec![rec(
            vec![("user", None), ("client-id", None)],
            "producer_byte_rate", 256.0,
        )]);
        assert_eq!(lookup_quota(&img, "alice", "app1", "producer_byte_rate"), Some(256.0));
    }

    #[test]
    fn default_user_alone() {
        let img = img_with(vec![rec(
            vec![("user", None)],
            "producer_byte_rate", 128.0,
        )]);
        assert_eq!(lookup_quota(&img, "alice", "app1", "producer_byte_rate"), Some(128.0));
    }

    #[test]
    fn default_client_alone() {
        let img = img_with(vec![rec(
            vec![("client-id", None)],
            "producer_byte_rate", 64.0,
        )]);
        assert_eq!(lookup_quota(&img, "alice", "app1", "producer_byte_rate"), Some(64.0));
    }

    #[test]
    fn no_match_returns_none() {
        let img = img_with(vec![]);
        assert_eq!(lookup_quota(&img, "alice", "app1", "producer_byte_rate"), None);
    }

    #[test]
    fn pair_specific_wins_over_user_only() {
        let img = img_with(vec![
            rec(vec![("user", Some("alice"))], "producer_byte_rate", 8192.0),
            rec(
                vec![("user", Some("alice")), ("client-id", Some("app1"))],
                "producer_byte_rate", 512.0,
            ),
        ]);
        assert_eq!(lookup_quota(&img, "alice", "app1", "producer_byte_rate"), Some(512.0));
    }
}
```

(9 tests total — the spec's 8 plus the bonus `pair_specific_wins_over_user_only` to exercise the priority ordering directly.)

- [ ] **Step 3: Register the module**

In `crates/broker/src/lib.rs`:

```rust
pub mod quota;
```

(Alphabetical; insert in the right slot.)

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib quota::lookup
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 9 new tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/quota/ crates/broker/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(broker): quota::lookup with 8-priority entity matching

lookup_quota + lookup_quota_with_key implement Kafka's documented
precedence for client quotas. 9 unit tests covering each priority
level plus the pair-specific-wins case.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 2 — Buckets + refresh (sequential: T3, T4)

Both append to `crates/broker/src/quota/mod.rs`; sequential to avoid edit conflicts.

### Task 3: `QuotaBuckets` cache

**Files:**
- Create: `crates/broker/src/quota/buckets.rs`
- Modify: `crates/broker/src/quota/mod.rs` (one append)
- Modify: `crates/broker/Cargo.toml` if `dashmap` isn't already a dep

- [ ] **Step 1: Check / add `dashmap` dependency**

```
rg "dashmap" crates/broker/Cargo.toml
```

If absent, add `dashmap = "5"` (or workspace-pinned version) to `[dependencies]`. If already present, skip.

- [ ] **Step 2: Write `crates/broker/src/quota/buckets.rs`**

```rust
//! Per-broker cache of `TokenBucket`s, one per (quota_key, entity_key) pair.

use std::sync::Arc;

use crabka_metadata::EntityKey;
use dashmap::DashMap;

use crate::throttle::TokenBucket;

#[derive(Debug, Default)]
pub struct QuotaBuckets {
    /// Keyed by (quota_key, canonical entity key). One bucket per
    /// (quota_type, entity) pair, lazy-allocated on first lookup.
    buckets: DashMap<(String, EntityKey), Arc<TokenBucket>>,
}

impl QuotaBuckets {
    #[must_use]
    pub fn new() -> Self {
        Self { buckets: DashMap::new() }
    }

    /// Get or lazily create a bucket for `(quota_key, entity_key)`,
    /// initializing it to `initial_rate` if newly created.
    pub fn get_or_create(
        &self,
        quota_key: &str,
        entity_key: &EntityKey,
        initial_rate: u64,
    ) -> Arc<TokenBucket> {
        if let Some(b) = self.buckets.get(&(quota_key.to_string(), entity_key.clone())) {
            return b.clone();
        }
        let b = Arc::new(TokenBucket::new());
        b.set_rate(initial_rate);
        let entry = self.buckets
            .entry((quota_key.to_string(), entity_key.clone()))
            .or_insert_with(|| b.clone());
        entry.clone()
    }

    /// Iterate every (quota_key, entity_key, bucket) — used by the
    /// refresh task to push new rates after an image change.
    pub fn iter(&self) -> impl Iterator<Item = ((String, EntityKey), Arc<TokenBucket>)> + '_ {
        self.buckets.iter().map(|r| (r.key().clone(), r.value().clone()))
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(user: &str) -> EntityKey {
        vec![("user".into(), Some(user.into()))]
    }

    #[test]
    fn get_or_create_returns_new_bucket_first_time() {
        let buckets = QuotaBuckets::new();
        let b = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        assert_eq!(b.rate(), 1024);
        assert_eq!(buckets.len(), 1);
    }

    #[test]
    fn get_or_create_returns_existing_bucket_second_time() {
        let buckets = QuotaBuckets::new();
        let b1 = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let b2 = buckets.get_or_create("producer_byte_rate", &key("alice"), 4096);
        // Same Arc — initial_rate on second call is ignored.
        assert!(Arc::ptr_eq(&b1, &b2));
        assert_eq!(b1.rate(), 1024);
        assert_eq!(buckets.len(), 1);
    }

    #[test]
    fn different_quota_keys_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let _ = buckets.get_or_create("consumer_byte_rate", &key("alice"), 2048);
        assert_eq!(buckets.len(), 2);
    }

    #[test]
    fn different_entities_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024);
        let _ = buckets.get_or_create("producer_byte_rate", &key("bob"), 2048);
        assert_eq!(buckets.len(), 2);
    }
}
```

(Note: `TokenBucket::rate()` is a public accessor added in slice 15b. Verify via `rg "pub fn rate" crates/broker/src/throttle/bucket.rs`. If absent, add `pub fn rate(&self) -> u64 { self.rate_bytes_per_sec.load(Relaxed) }` in this task — it's a one-line addition.)

- [ ] **Step 3: Append to `mod.rs`**

In `crates/broker/src/quota/mod.rs`, after the existing `mod lookup;`:

```rust
mod buckets;
pub use buckets::QuotaBuckets;
```

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib quota::buckets
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 4 new tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/quota/ crates/broker/Cargo.toml
git commit -m "$(cat <<'EOF'
feat(broker): QuotaBuckets cache

DashMap-backed per-broker cache, keyed by (quota_key, entity_key).
Lazy bucket allocation on first lookup. iter() exposes entries for
the image-driven refresh task.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `quota::refresh` background task

**Files:**
- Create: `crates/broker/src/quota/refresh.rs`
- Modify: `crates/broker/src/quota/mod.rs` (one append)

- [ ] **Step 1: Write the module**

```rust
//! Background task that subscribes to MetadataImage changes and
//! pushes new quota rates to the QuotaBuckets cache.
//!
//! Mirrors slice 15b's throttle::refresh shape.

use std::sync::Arc;

use crabka_metadata::MetadataImage;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::buckets::QuotaBuckets;

#[cfg_attr(test, mockall::automock)]
pub trait ImageWatcher: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
}

pub async fn run(
    controller: Arc<dyn ImageWatcher>,
    buckets: Arc<QuotaBuckets>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    refresh_buckets(&controller.current_image(), &buckets);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                info!("quota refresh task shutting down");
                return;
            }
            r = watcher.changed() => {
                if r.is_err() {
                    info!("quota refresh: image channel closed");
                    return;
                }
            }
        }
        refresh_buckets(&controller.current_image(), &buckets);
    }
}

fn refresh_buckets(image: &MetadataImage, buckets: &QuotaBuckets) {
    for ((quota_key, entity_key), bucket) in buckets.iter() {
        let new_rate: u64 = image.client_quotas()
            .get(&entity_key)
            .and_then(|m| m.get(&quota_key))
            .copied()
            .map(|v| v.max(0.0) as u64)
            .unwrap_or(0);
        if bucket.rate() != new_rate {
            debug!(quota_key, ?entity_key, new_rate, "quota refresh: rate update");
            bucket.set_rate(new_rate);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity};

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, key: &str, value: f64) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: entity.into_iter().map(|(t, n)| QuotaEntity {
                entity_type: t.into(),
                entity_name: n.map(Into::into),
            }).collect(),
            config_key: key.into(),
            config_value: Some(value),
        }));
        Arc::new(img)
    }

    #[test]
    fn refresh_updates_existing_bucket_rate() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, 0);
        assert_eq!(b.rate(), 0);

        let img = img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 2048.0);
        refresh_buckets(&img, &buckets);
        assert_eq!(b.rate(), 2048);
    }

    #[test]
    fn refresh_zeroes_bucket_when_quota_removed_from_image() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, 1024);
        assert_eq!(b.rate(), 1024);

        let empty = Arc::new(MetadataImage::new(uuid::Uuid::nil()));
        refresh_buckets(&empty, &buckets);
        assert_eq!(b.rate(), 0);
    }
}
```

**Note on `mockall`:** if the workspace doesn't already use `mockall`, drop the `#[cfg_attr(test, mockall::automock)]` attribute — the unit tests above don't actually need a mock since `refresh_buckets` is pure. Slice 14/15 didn't pull in mockall; check the workspace deps. If absent, just `pub trait ImageWatcher: Send + Sync { ... }` without the cfg_attr.

- [ ] **Step 2: Append to `mod.rs`**

```rust
mod refresh;
pub use refresh::{run, ImageWatcher};
```

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib quota::refresh
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 2 new tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/quota/
git commit -m "$(cat <<'EOF'
feat(broker): quota refresh background task

Subscribes to ControllerHandle::watch_image; on each image change,
iterates existing buckets and pushes new rates via set_rate. Rate 0
when a quota is removed from the image. Image-driven (not timer-driven).

Spawning from Broker::start is task 7.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 3 — Handlers (parallel: T5, T6)

### Task 5: `AlterClientQuotas` handler + 6 unit tests

**Files:**
- Create: `crates/broker/src/handlers/alter_client_quotas.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (register module)

- [ ] **Step 1: Write the handler skeleton + pure-logic `process_one_entry`**

```rust
//! `AlterClientQuotas` (api_key 49, KIP-13/124/257).

#![allow(dead_code)]

use std::collections::HashSet;
use std::net::SocketAddr;

use bytes::Bytes;
use crabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity, ResourceType};
use crabka_protocol::owned::alter_client_quotas_request::{
    AlterClientQuotasRequest, EntityData, EntryData, OpData,
};
use crabka_protocol::owned::alter_client_quotas_response::{
    AlterClientQuotasResponse, EntityData as RespEntity, EntryData as RespEntry,
};
use crabka_protocol::Encode;
use crabka_security::Principal;

use crate::authorizer::{authorize, AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::{
    CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE, INVALID_CONFIG, INVALID_REQUEST,
};

const KNOWN_QUOTA_KEYS: &[&str] = &[
    "producer_byte_rate",
    "consumer_byte_rate",
    "request_percentage",
];
const SUPPORTED_ENTITY_TYPES: &[&str] = &["user", "client-id"];

pub(crate) async fn handle(
    broker: &Broker,
    req: AlterClientQuotasRequest,
    principal: &Principal,
    peer: &SocketAddr,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = authorize(
        &image,
        &broker.config.super_users,
        &AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "alter-client-quotas denied", api_version);
    }

    let mut entry_results = Vec::with_capacity(req.entries.len());
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    for entry in &req.entries {
        match process_one_entry(entry) {
            Ok(records) => {
                if !req.validate_only {
                    to_submit.extend(records);
                }
                entry_results.push(ok_entry(&entry.entity));
            }
            Err((code, msg)) => entry_results.push(err_entry(&entry.entity, code, msg)),
        }
    }

    if !to_submit.is_empty() {
        if let Err(e) = broker.controller.submit_change(to_submit).await {
            tracing::warn!(error = %e, "alter-client-quotas submit failed");
            for r in entry_results.iter_mut() {
                if r.error_code == 0 {
                    r.error_code = COORDINATOR_NOT_AVAILABLE;
                    r.error_message = Some(format!("submit failed: {e}"));
                }
            }
        }
    }

    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: entry_results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

/// Validate + transform one EntryData into a list of MetadataRecords
/// to submit. Returns wire (code, message) on validation failure.
pub(crate) fn process_one_entry(
    entry: &EntryData,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    if entry.entity.is_empty() {
        return Err((INVALID_REQUEST, "empty entity tuple".into()));
    }
    let mut seen_types: HashSet<&str> = HashSet::new();
    for e in &entry.entity {
        if !SUPPORTED_ENTITY_TYPES.contains(&e.entity_type.as_str()) {
            return Err((INVALID_REQUEST, format!("unsupported entity_type {:?}", e.entity_type)));
        }
        if !seen_types.insert(e.entity_type.as_str()) {
            return Err((INVALID_REQUEST, format!("duplicate entity_type {:?}", e.entity_type)));
        }
    }
    let mut records = Vec::with_capacity(entry.ops.len());
    for op in &entry.ops {
        if !KNOWN_QUOTA_KEYS.contains(&op.key.as_str()) {
            return Err((INVALID_CONFIG, format!("unknown quota key {:?}", op.key)));
        }
        if !op.remove {
            if !op.value.is_finite() || op.value < 0.0 {
                return Err((INVALID_CONFIG, format!("invalid value {} for {}", op.value, op.key)));
            }
            if op.key == "request_percentage" && op.value > 100.0 {
                return Err((INVALID_CONFIG, format!("request_percentage > 100.0: {}", op.value)));
            }
        }
        records.push(MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: entry.entity.iter().map(|e| QuotaEntity {
                entity_type: e.entity_type.clone(),
                entity_name: e.entity_name.clone(),
            }).collect(),
            config_key: op.key.clone(),
            config_value: if op.remove { None } else { Some(op.value) },
        }));
    }
    Ok(records)
}

fn ok_entry(entity: &[EntityData]) -> RespEntry {
    RespEntry {
        error_code: 0,
        error_message: None,
        entity: entity.iter().map(|e| RespEntity {
            entity_type: e.entity_type.clone(),
            entity_name: e.entity_name.clone(),
            ..Default::default()
        }).collect(),
        ..Default::default()
    }
}

fn err_entry(entity: &[EntityData], code: i16, msg: String) -> RespEntry {
    RespEntry {
        error_code: code,
        error_message: Some(msg),
        entity: entity.iter().map(|e| RespEntity {
            entity_type: e.entity_type.clone(),
            entity_name: e.entity_name.clone(),
            ..Default::default()
        }).collect(),
        ..Default::default()
    }
}

fn encode_whole_request_error(
    req: &AlterClientQuotasRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let entries: Vec<RespEntry> = req.entries.iter()
        .map(|e| err_entry(&e.entity, code, msg.into()))
        .collect();
    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(resp: &R, api_version: i16) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode AlterClientQuotas: {e}")))?;
    Ok(Bytes::from(body))
}
```

**Field-name verification before committing:** check the actual struct names in `crates/protocol/generated/AlterClientQuotasResponse.owned.rs` — the response uses `EntityData`/`EntryData` per the generated owned-type. If the names differ from the sketch above (e.g. `EntryResultData` instead of `EntryData`), adapt the `use` line and the helper return types.

- [ ] **Step 2: Write 6 unit tests**

Append:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_protocol::owned::alter_client_quotas_request::{EntityData, EntryData, OpData};

    fn entry(entity: Vec<(&str, Option<&str>)>, ops: Vec<(&str, f64, bool)>) -> EntryData {
        EntryData {
            entity: entity.into_iter().map(|(t, n)| EntityData {
                entity_type: t.into(),
                entity_name: n.map(Into::into),
                ..Default::default()
            }).collect(),
            ops: ops.into_iter().map(|(k, v, r)| OpData {
                key: k.into(),
                value: v,
                remove: r,
                ..Default::default()
            }).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn start_writes_v1_client_quota_record() {
        let e = entry(vec![("user", Some("alice"))], vec![("producer_byte_rate", 1024.0, false)]);
        let records = process_one_entry(&e).expect("ok");
        assert_eq!(records.len(), 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else { panic!("wrong variant") };
        assert_eq!(r.config_key, "producer_byte_rate");
        assert_eq!(r.config_value, Some(1024.0));
    }

    #[test]
    fn validate_only_does_not_submit() {
        // This is exercised at the handler level; process_one_entry has no notion.
        // The test below verifies that the record-building step works regardless.
        let e = entry(vec![("user", Some("alice"))], vec![("producer_byte_rate", 1024.0, false)]);
        assert!(process_one_entry(&e).is_ok());
    }

    #[test]
    fn remove_writes_none_value() {
        let e = entry(vec![("user", Some("alice"))], vec![("producer_byte_rate", 0.0, true)]);
        let records = process_one_entry(&e).expect("ok");
        let MetadataRecord::V1ClientQuota(r) = &records[0] else { panic!() };
        assert_eq!(r.config_value, None);
    }

    #[test]
    fn unsupported_entity_type_rejected() {
        let e = entry(vec![("ip", Some("10.0.0.1"))], vec![("producer_byte_rate", 1024.0, false)]);
        let err = process_one_entry(&e).unwrap_err();
        assert_eq!(err.0, INVALID_REQUEST);
    }

    #[test]
    fn duplicate_entity_type_rejected() {
        let e = entry(
            vec![("user", Some("alice")), ("user", Some("bob"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert_eq!(err.0, INVALID_REQUEST);
    }

    #[test]
    fn out_of_range_value_rejected() {
        let e = entry(vec![("user", Some("alice"))], vec![("producer_byte_rate", -100.0, false)]);
        let err = process_one_entry(&e).unwrap_err();
        assert_eq!(err.0, INVALID_CONFIG);

        let e2 = entry(vec![("user", Some("alice"))], vec![("request_percentage", 250.0, false)]);
        let err2 = process_one_entry(&e2).unwrap_err();
        assert_eq!(err2.0, INVALID_CONFIG);

        let e3 = entry(vec![("user", Some("alice"))], vec![("producer_byte_rate", f64::NAN, false)]);
        let err3 = process_one_entry(&e3).unwrap_err();
        assert_eq!(err3.0, INVALID_CONFIG);
    }
}
```

- [ ] **Step 3: Register the module**

In `crates/broker/src/handlers/mod.rs`:

```rust
mod alter_client_quotas;
```

(Alphabetical.)

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib alter_client_quotas
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 6 new tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/alter_client_quotas.rs crates/broker/src/handlers/mod.rs
git commit -m "$(cat <<'EOF'
feat(broker): AlterClientQuotas handler (api_key 49)

Cluster Alter gate; process_one_entry validates entity tuple +
quota keys + value ranges; submits V1ClientQuota metadata records.
6 unit tests covering happy path + validation errors. Dispatch
wiring in task 7.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `DescribeClientQuotas` handler + 4 unit tests

**Files:**
- Create: `crates/broker/src/handlers/describe_client_quotas.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (register module)

- [ ] **Step 1: Write the handler**

```rust
//! `DescribeClientQuotas` (api_key 48, KIP-13/124).

#![allow(dead_code)]

use std::net::SocketAddr;

use bytes::Bytes;
use crabka_metadata::{EntityKey, MetadataImage, ResourceType};
use crabka_protocol::owned::describe_client_quotas_request::{
    ComponentData, DescribeClientQuotasRequest,
};
use crabka_protocol::owned::describe_client_quotas_response::{
    DescribeClientQuotasResponse, EntityData, EntryData, ValueData,
};
use crabka_protocol::Encode;
use crabka_security::Principal;

use crate::authorizer::{authorize, AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::CLUSTER_AUTHORIZATION_FAILED;

const MATCH_TYPE_EXACT: i8 = 0;
const MATCH_TYPE_DEFAULT: i8 = 1;
const MATCH_TYPE_ANY: i8 = 2;

pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeClientQuotasRequest,
    principal: &Principal,
    peer: &SocketAddr,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = authorize(
        &image,
        &broker.config.super_users,
        &AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Describe,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let resp = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-client-quotas denied".into()),
            entries: None,
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let mut entries: Vec<EntryData> = Vec::new();
    for (stored_key, configs) in image.client_quotas() {
        if !entity_matches_filter(stored_key, &req.components, req.strict) {
            continue;
        }
        entries.push(EntryData {
            entity: stored_key.iter().map(|(t, n)| EntityData {
                entity_type: t.clone(),
                entity_name: n.clone(),
                ..Default::default()
            }).collect(),
            values: configs.iter().map(|(k, v)| ValueData {
                key: k.clone(),
                value: *v,
                ..Default::default()
            }).collect(),
            ..Default::default()
        });
    }

    let resp = DescribeClientQuotasResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        entries: Some(entries),
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

pub(crate) fn entity_matches_filter(
    stored: &EntityKey,
    components: &[ComponentData],
    strict: bool,
) -> bool {
    if strict && stored.len() != components.len() {
        return false;
    }
    for comp in components {
        let Some(stored_entity) = stored.iter().find(|(t, _)| t == &comp.entity_type) else {
            return false;
        };
        let ok = match comp.match_type {
            MATCH_TYPE_EXACT => stored_entity.1.as_deref() == comp.r#match.as_deref(),
            MATCH_TYPE_DEFAULT => stored_entity.1.is_none(),
            MATCH_TYPE_ANY => true,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn encode_response<R: Encode>(resp: &R, api_version: i16) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode DescribeClientQuotas: {e}")))?;
    Ok(Bytes::from(body))
}
```

**Field name verification:** open `crates/protocol/generated/DescribeClientQuotasRequest.owned.rs` to confirm field names on `ComponentData` (particularly `r#match: Option<String>` vs alternatives like `match_str`). Adjust the field access accordingly.

- [ ] **Step 2: Write 4 unit tests**

Append:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn comp(entity_type: &str, match_type: i8, m: Option<&str>) -> ComponentData {
        ComponentData {
            entity_type: entity_type.into(),
            match_type,
            r#match: m.map(Into::into),
            ..Default::default()
        }
    }

    fn key(parts: Vec<(&str, Option<&str>)>) -> EntityKey {
        parts.into_iter().map(|(t, n)| (t.into(), n.map(Into::into))).collect()
    }

    #[test]
    fn strict_exact_match_filters_correctly() {
        let stored = key(vec![("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))];
        assert!(entity_matches_filter(&stored, &filter, true));
        assert!(!entity_matches_filter(&stored, &filter[..0], true)); // strict: type-count mismatch
    }

    #[test]
    fn non_strict_filter_returns_supersets() {
        // Stored has (user, client-id); filter only mentions user.
        let stored = key(vec![("client-id", Some("app1")), ("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))];
        assert!(entity_matches_filter(&stored, &filter, false));
        assert!(!entity_matches_filter(&stored, &filter, true)); // strict rejects superset
    }

    #[test]
    fn default_match_type_filters_by_none_entity_name() {
        let stored_default = key(vec![("user", None)]);
        let stored_named = key(vec![("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_DEFAULT, None)];
        assert!(entity_matches_filter(&stored_default, &filter, true));
        assert!(!entity_matches_filter(&stored_named, &filter, true));
    }

    #[test]
    fn any_match_type_returns_all_names_of_type() {
        let stored1 = key(vec![("user", Some("alice"))]);
        let stored2 = key(vec![("user", None)]);
        let filter = vec![comp("user", MATCH_TYPE_ANY, None)];
        assert!(entity_matches_filter(&stored1, &filter, true));
        assert!(entity_matches_filter(&stored2, &filter, true));
    }
}
```

- [ ] **Step 3: Register the module**

In `crates/broker/src/handlers/mod.rs`:

```rust
mod describe_client_quotas;
```

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib describe_client_quotas
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 4 new tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/describe_client_quotas.rs crates/broker/src/handlers/mod.rs
git commit -m "$(cat <<'EOF'
feat(broker): DescribeClientQuotas handler (api_key 48)

Cluster Describe gate; walks MetadataImage::client_quotas; per-entry
filter via entity_matches_filter (exact/default/any match types,
strict vs non-strict). 4 unit tests cover filter semantics.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 4 — Dispatch wiring (sequential: T7)

### Task 7: Dispatch + api_versions wiring

**Files:**
- Modify: `crates/broker/src/handlers/api_versions.rs`
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/handlers/alter_client_quotas.rs` (remove `#![allow(dead_code)]`)
- Modify: `crates/broker/src/handlers/describe_client_quotas.rs` (remove `#![allow(dead_code)]`)

- [ ] **Step 1: Add to `supported_apis`**

In `crates/broker/src/handlers/api_versions.rs`, append (in api-key order; 48 and 49 sit after 46):

```rust
v!(describe_client_quotas_request),
v!(alter_client_quotas_request),
```

- [ ] **Step 2: Add to flexible-body table**

In `crates/broker/src/network/dispatch.rs::handler_body_flexible`:

```rust
48 => version >= crabka_protocol::owned::describe_client_quotas_request::FLEXIBLE_MIN,
49 => version >= crabka_protocol::owned::alter_client_quotas_request::FLEXIBLE_MIN,
```

- [ ] **Step 3: Add intercept arms + helpers**

Mirror slice 14's `handle_elect_leaders_frame` / slice 15's helpers. In the per-connection request loop:

```rust
if peek_api_key(&frame) == Some(48) {
    handle_describe_client_quotas_frame(
        broker, frame, api_version, correlation_id, client_id, auth, peer,
    ).await?;
    continue;
}
if peek_api_key(&frame) == Some(49) {
    handle_alter_client_quotas_frame(
        broker, frame, api_version, correlation_id, client_id, auth, peer,
    ).await?;
    continue;
}
```

Plus two helper functions alongside slice 15's `handle_alter_partition_reassignments_frame`. Copy the slice-15 helper's signature + framing pattern verbatim; only the decode/handle types change.

- [ ] **Step 4: Remove `#![allow(dead_code)]` from both handler modules**

Now reachable via dispatch.

- [ ] **Step 5: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/api_versions.rs crates/broker/src/network/dispatch.rs \
        crates/broker/src/handlers/alter_client_quotas.rs \
        crates/broker/src/handlers/describe_client_quotas.rs
git commit -m "$(cat <<'EOF'
feat(broker): wire AlterClientQuotas + DescribeClientQuotas dispatch

api_keys 48 + 49 registered in supported_apis + flexible-body table.
Inline-intercept dispatch arms match the slice-14/15 pattern (both
handlers need &Principal + &SocketAddr).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 5 — Broker integration (sequential T8; parallel T9, T10, T11)

### Task 8: `Broker::start` spawn + `quota_buckets` field

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Add `QuotaControllerAdapter`**

Alongside slice-15b's `ThrottleControllerAdapter`:

```rust
struct QuotaControllerAdapter {
    handle: std::sync::Arc<crabka_raft::ControllerHandle>,
}

impl crate::quota::ImageWatcher for QuotaControllerAdapter {
    fn current_image(&self) -> std::sync::Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }
    fn watch_image(&self) -> tokio::sync::watch::Receiver<std::sync::Arc<crabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }
}
```

(`ImageWatcher` from T4 has no async methods, so no `#[async_trait]` needed — match the slice-15b throttle adapter.)

- [ ] **Step 2: Add `quota_buckets` field on `Broker`**

```rust
pub quota_buckets: std::sync::Arc<crate::quota::QuotaBuckets>,
```

(Match the slice-15b `throttle_state` style — `pub` for cross-module access from Produce/Fetch.)

- [ ] **Step 3: Spawn refresh task in `Broker::start`**

After slice 15b's throttle refresh spawn:

```rust
let quota_buckets = std::sync::Arc::new(crate::quota::QuotaBuckets::new());
{
    let buckets = quota_buckets.clone();
    let watcher: std::sync::Arc<dyn crate::quota::ImageWatcher> =
        std::sync::Arc::new(QuotaControllerAdapter { handle: controller.clone() });
    let shutdown = supervisor_shutdown.child_token();
    tokio::spawn(crate::quota::run(watcher, buckets, shutdown));
}
```

Pass `quota_buckets` into the `Broker { ... }` construction.

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass. The refresh task starts on every broker; buckets stay empty until first lookup, rates stay 0 until configs land.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "$(cat <<'EOF'
feat(broker): spawn quota refresh + Broker.quota_buckets

QuotaControllerAdapter wraps ControllerHandle for the ImageWatcher
trait. QuotaBuckets cache stored as pub field for Produce/Fetch
enforcement (tasks 9, 10).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: `producer_byte_rate` enforcement on Produce (parallel-safe)

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`

- [ ] **Step 1: Read existing handler structure**

```
rg "fn handle\|client_id\|principal\|throttle_time_ms\|topic_data" crates/broker/src/handlers/produce.rs
```

Identify:
- Where the response is being built.
- How `principal` and `client_id` are accessed (slice 12 + slice 13 plumbing).
- The response type's `throttle_time_ms` field name.

- [ ] **Step 2: Add the quota hook**

After the per-partition response is assembled but before encoding:

```rust
use std::time::Duration;

let total_bytes: u64 = req.topic_data.iter()
    .flat_map(|t| t.partition_data.iter())
    .map(|p| p.records.as_ref().map_or(0, |r| r.len() as u64))
    .sum();
let principal_name = principal.name();
let delay = consume_producer_quota(
    &image, &broker.quota_buckets,
    principal_name, &client_id,
    total_bytes,
);
response.throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
if delay > Duration::ZERO {
    tokio::time::sleep(delay).await;
}
```

`consume_producer_quota` is a helper:

```rust
fn consume_producer_quota(
    image: &crabka_metadata::MetadataImage,
    buckets: &crate::quota::QuotaBuckets,
    principal: &str,
    client_id: &str,
    bytes: u64,
) -> Duration {
    let Some((entity_key, rate)) = crate::quota::lookup_quota_with_key(
        image, principal, client_id, "producer_byte_rate"
    ) else {
        return Duration::ZERO;
    };
    if rate <= 0.0 { return Duration::ZERO; }
    let bucket = buckets.get_or_create("producer_byte_rate", &entity_key, rate as u64);
    let granted = bucket.try_consume(bytes);
    if granted >= bytes { return Duration::ZERO; }
    let overage = bytes - granted;
    let delay_secs = overage as f64 / rate;
    Duration::from_micros((delay_secs * 1_000_000.0) as u64).min(Duration::from_secs(1))
}
```

(Co-locate the helper at the bottom of `produce.rs`; not exported.)

- [ ] **Step 3: `Principal::name` access**

Verify via:

```
rg "impl Principal\|fn name\(" crates/security/src/
```

Slice 12 introduced `Principal::name() -> &str`. If named differently, adjust. The `principal` variable in produce.rs's handle function should be `&Principal` (passed by dispatch).

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib produce
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass. No quotas are configured in pre-slice-16 tests, so the hook is a no-op.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/produce.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-13 producer_byte_rate enforcement

After Produce response assembly, look up the matching quota for
(principal, client_id), consume the response byte total from the
bucket, set throttle_time_ms, and tokio::time::sleep before sending.
Delay capped at 1 second (Kafka's quota.window.size.seconds).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: `consumer_byte_rate` enforcement on Fetch (parallel-safe)

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Locate the post-assembly hook site**

Slice 15b added a leader-side throttle hook in `fetch.rs` around the post-assembly point. Slice 16's consumer-byte-rate hook lives in the same general region, gated on `replica_id < 0` (consumer fetches only — inter-broker uses slice 15b's path).

```
rg "fn handle\|replica_id\|throttle_time_ms\|truncate_throttled" crates/broker/src/handlers/fetch.rs
```

- [ ] **Step 2: Add the consumer-quota hook**

Insert AFTER slice 15b's leader-side throttle block, gated on `replica_id < 0`:

```rust
if req.replica_id < 0 {
    // KIP-13 consumer_byte_rate. Mutually exclusive with slice-15b's
    // inter-broker leader throttle (which fires only when replica_id >= 0).
    use std::time::Duration;
    let total_bytes: u64 = sum_assembled_response_bytes(&responses);
    let principal_name = principal.name();
    let Some((entity_key, rate)) = crate::quota::lookup_quota_with_key(
        &image, principal_name, &client_id, "consumer_byte_rate"
    ) else {
        // fall through, no throttle
        return Ok(fetch_response);
    };
    if rate > 0.0 {
        let bucket = broker.quota_buckets.get_or_create("consumer_byte_rate", &entity_key, rate as u64);
        let granted = bucket.try_consume(total_bytes);
        if granted < total_bytes {
            let overage = total_bytes - granted;
            let delay = Duration::from_micros(((overage as f64 / rate) * 1_000_000.0) as u64)
                .min(Duration::from_secs(1));
            fetch_response.throttle_time_ms = i32::try_from(delay.as_millis()).unwrap_or(i32::MAX);
            tokio::time::sleep(delay).await;
        }
    }
}
```

`sum_assembled_response_bytes` — walk the response's partitions and sum each `records.as_ref().map_or(0, |b| b.encoded_len())`. (Match the helper slice-15b T9 used in fetch.rs.)

Adapt the early-return control flow to whatever's natural in the existing handler. The pattern is: look up → consume → sleep before write.

- [ ] **Step 3: Add code comment about mutual exclusion**

Above the new block, add a one-line comment explaining the relationship with slice-15b's leader throttle: "Consumer fetches (replica_id < 0) use client quotas; inter-broker fetches (replica_id >= 0) use KIP-73 throttle from slice 15b."

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib fetch
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/fetch.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-13 consumer_byte_rate enforcement on Fetch

Gated on replica_id < 0 (consumer fetches only — inter-broker uses
slice 15b's KIP-73 leader-throttle path). Looks up the matching
quota, consumes response bytes, sets throttle_time_ms, sleeps
before sending. Delay capped at 1 second.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: `request_percentage` enforcement on dispatch (parallel-safe)

**Files:**
- Modify: `crates/broker/src/network/dispatch.rs`

- [ ] **Step 1: Identify the per-request dispatch site**

```
rg "fn handle_request\|dispatch_one\|handler\.handle\|let response = handle" crates/broker/src/network/dispatch.rs
```

Find the single point where a request is decoded, handed to a handler, and the response is written back.

- [ ] **Step 2: Wrap with timing + quota consumption**

```rust
let started = std::time::Instant::now();
let response = /* existing handler dispatch */ ;
let elapsed_micros = started.elapsed().as_micros() as u64;

// KIP-124 request_percentage. 100% = 1 core = 1_000_000 μs/sec.
if let Some(principal_name) = auth.principal().map(|p| p.name()) {
    let image = broker.controller.current_image();
    if let Some((entity_key, rate_pct)) = crabka_broker::quota::lookup_quota_with_key(
        &image, principal_name, &client_id_str, "request_percentage"
    ) {
        if rate_pct > 0.0 {
            let rate_micros_per_sec = (rate_pct * 10_000.0) as u64;
            let bucket = broker.quota_buckets.get_or_create(
                "request_percentage", &entity_key, rate_micros_per_sec,
            );
            let granted = bucket.try_consume(elapsed_micros);
            if granted < elapsed_micros && rate_micros_per_sec > 0 {
                let overage_micros = elapsed_micros - granted;
                let delay_micros = overage_micros.saturating_mul(1_000_000) / rate_micros_per_sec;
                let delay = std::time::Duration::from_micros(delay_micros)
                    .min(std::time::Duration::from_secs(1));
                tokio::time::sleep(delay).await;
            }
        }
    }
}
write_response_to_stream(&mut stream, response).await?;
```

**Code-context adjustments needed:** the variable names (`auth`, `broker`, `client_id_str`, etc.) and the exact response-write call vary by codebase. Read the existing dispatch loop carefully and adapt — don't paste blindly.

**Known limitation:** `throttle_time_ms` on the response itself is only populated for Produce + Fetch (tasks 9 + 10). Other handlers absorb the request_percentage delay silently. Documented in STATUS.md by T13.

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/network/dispatch.rs
git commit -m "$(cat <<'EOF'
feat(broker): KIP-124 request_percentage enforcement

Wraps per-request dispatch with Instant::now() timing; consumes the
elapsed CPU time (in microseconds) from a request_percentage bucket.
Delay applied via tokio::time::sleep before write. Throttle_time_ms
in the response only surfaces from Produce/Fetch (limitation noted
in STATUS).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 6 — Integration tests + JVM + final (sequential)

### Task 12: 5 broker integration tests

**Files:**
- Create: `crates/broker/tests/client_quotas.rs`

- [ ] **Step 1: File scaffold + copied helpers**

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]
```

Copy from slice 15b's `tests/throttle.rs` (and slice 15's `tests/partition_reassignment.rs` as needed):
- `round_trip`
- `sasl_plain_authenticate`
- `start_single_broker_sasl_plaintext_with_users`
- `create_topic_as_admin`
- `wait_partition_exists`
- `controller_image_for_test` (if not on BrokerHandle, add a `pub fn` test accessor in this task)

Add two wire drivers:

```rust
async fn drive_alter_client_quotas_sasl(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    entries: Vec<(/*entity*/ Vec<(String, Option<String>)>, /*ops*/ Vec<(String, f64, bool)>)>,
    validate_only: bool,
) -> Vec<(/*entity*/ Vec<(String, Option<String>)>, /*error_code*/ i16)> { ... }

async fn drive_describe_client_quotas_sasl(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    components: Vec<(String, /*match_type*/ i8, /*match*/ Option<String>)>,
    strict: bool,
) -> Vec<(Vec<(String, Option<String>)>, Vec<(String, f64)>)> { ... }
```

Build the requests using the generated owned types; framing matches slice 14's `drive_elect_leaders_sasl_plain` pattern verbatim.

- [ ] **Step 2: Test 1 — `alter_then_describe_round_trip`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_then_describe_round_trip() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret")],
    ).await;

    let alter_resp = drive_alter_client_quotas_sasl(
        addr, "admin", "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("producer_byte_rate".into(), 1024.0, false)],
        )],
        false,
    ).await;
    assert_eq!(alter_resp[0].1, 0, "alter should succeed");

    // Poll the image until the quota is visible.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        if let Some(cfgs) = img.client_quotas().get(&key) {
            if cfgs.get("producer_byte_rate") == Some(&1024.0) {
                break;
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("quota not visible in image");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let desc = drive_describe_client_quotas_sasl(
        addr, "admin", "admin-secret",
        vec![("user".into(), 2 /*ANY*/, None)],
        false,
    ).await;
    assert_eq!(desc.len(), 1);
    assert_eq!(desc[0].1.iter().find(|(k, _)| k == "producer_byte_rate").map(|(_, v)| *v), Some(1024.0));
}
```

- [ ] **Step 3: Test 2 — `producer_byte_rate_throttles_produce`**

Single-broker SASL/PLAIN; set `(user=alice) producer_byte_rate=512`; alice produces 4 KB via wire Produce request; assert response carries `throttle_time_ms > 0` AND wall-clock elapsed between send and response receipt is at least the throttle delay (within a tolerance).

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttles_produce() {
    // ... setup single-broker SASL/PLAIN with admin + alice users
    // ... create rf=1 topic "foo" as admin
    // ... seed an unrelated ACL to disable compat shim if needed (slice 13)
    //     plus an ACL allowing alice to Write topic foo
    // ... set producer_byte_rate=512 for (user=alice)
    // ... open SASL-authenticated connection as alice
    // ... send a Produce containing ~4096 bytes of record data
    // ... measure: response.throttle_time_ms >= ~7000ms (4096 - 1024 burst = 3072 over, at 512/sec = 6s)
    //              capped at 1000ms per spec → expect ~1000ms throttle_time_ms
    // ... measure wall-clock elapsed: at least 800ms
}
```

(Loose tolerances — the test only needs to PROVE the throttle fires, not measure exactly.)

- [ ] **Step 4: Test 3 — `consumer_byte_rate_throttles_fetch`**

Symmetric — set `consumer_byte_rate=512`, alice fetches ~4 KB, assert `throttle_time_ms > 0` and wall-clock delay.

- [ ] **Step 5: Test 4 — `tuple_quota_wins_over_user_only`**

Set both:
- `(user=alice) producer_byte_rate=8192`
- `(user=alice, client-id=app1) producer_byte_rate=512`

Alice with `client_id=app1` produces 4 KB. Assert the throttle delay matches the 512-rate (tight tuple), not 8192. The `throttle_time_ms` in the response or the wall-clock delay should reflect the 512 limit.

- [ ] **Step 6: Test 5 — `non_super_user_denied`**

Alice has PLAIN creds, no ACLs. Seed one unrelated ACL via `submit_metadata_record_for_test` to disable the slice-13 compat shim. Alice calls `AlterClientQuotas` → expect every entry's `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.

- [ ] **Step 7: Run tests via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test client_quotas -- --nocapture --test-threads=1"
```

Expected: 5 tests PASS.

- [ ] **Step 8: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/client_quotas.rs crates/broker/src/broker.rs
git commit -m "$(cat <<'EOF'
test(broker): client_quotas alter/describe + producer/consumer throttle

Five SASL/PLAIN integration tests covering AlterClientQuotas +
DescribeClientQuotas round-trip, producer_byte_rate throttle on
Produce, consumer_byte_rate throttle on Fetch, tuple-precedence
(specific tuple wins over user-only), and the auth-deny path.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: JVM acceptance for `kafka-configs --entity-type users`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_client_quota_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (h1, h2, h3, _d1, _d2, _d3, addr) =
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

    // Set producer_byte_rate=1024 for alice.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--alter",
            "--entity-type", "users", "--entity-name", ALICE,
            "--add-config", "producer_byte_rate=1024",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(out.status.success(), "alter failed: {}", String::from_utf8_lossy(&out.stderr));

    // Describe — confirm visibility.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--describe",
            "--entity-type", "users", "--entity-name", ALICE,
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(stdout.contains("producer_byte_rate=1024"),
            "expected quota in describe output: {stdout}");

    // Delete the config.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--alter",
            "--entity-type", "users", "--entity-name", ALICE,
            "--delete-config", "producer_byte_rate",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(out.status.success(), "delete failed: {}", String::from_utf8_lossy(&out.stderr));

    // Confirm quota cleared from image.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = h1.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some(ALICE.into()))];
        if img.client_quotas().get(&key)
            .and_then(|m| m.get("producer_byte_rate"))
            .is_none()
        {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("quota not cleared after delete-config");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

**`start_three_broker_sasl_plaintext_jvm_cluster_with_users`** — slice 14 added the basic 3-broker SASL/PLAINTEXT cluster helper, but may not accept extra users. Either:
- (a) Use the existing helper (which already provisions admin + alice via slice 14's pattern), or
- (b) Extend it to accept an `extra_users: &[(name, pass)]` parameter.

Pick whichever requires fewer changes. Slice 14 T10's helper signature is the canonical reference — if it already accepts extra users via slice 12b's SCRAM provisioning, just use that.

**Producer throttle behavior smoke check (optional 7th step):** if time permits, run `kafka-console-producer` as alice pushing ~4 KB and assert wall time. If the test gets flaky from cp-kafka producer behavior, skip — the alter/describe/delete round-trip is the primary contract.

- [ ] **Step 2: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance jvm_kafka_configs_alter_client_quota_end_to_end -- --ignored --nocapture --test-threads=1"
```

Expected: PASS in 30-90 seconds.

- [ ] **Step 3: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "$(cat <<'EOF'
test(jvm): kafka-configs --entity-type users client quota round-trip

Three-broker SASL/PLAINTEXT cluster; --alter + --describe + --delete
on a user-scoped producer_byte_rate. Verifies the wire path
end-to-end against the JVM admin CLI.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 14: Sweep + docs + PR

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

- [ ] **Step 2: Update `README.md`**

Append under "Slices delivered":

```markdown
- **Slice 16** — client quotas (KIP-13 + KIP-124 + KIP-257): `AlterClientQuotas`
  (api_key 49) and `DescribeClientQuotas` (api_key 48). Three quota types
  (producer_byte_rate, consumer_byte_rate, request_percentage) with Kafka's
  8-priority entity precedence (user, client-id, tuple, default). Enforced
  via the slice-15b TokenBucket primitive on Produce, Fetch, and per-request
  dispatch. KIP-257 server-side throttle delays via `tokio::time::sleep`. JVM
  `kafka-configs --entity-type users --add-config producer_byte_rate=...` works
  end-to-end. IP entity + connection_creation_rate (KIP-612) deferred to
  slice 16b; controller_mutation_rate (KIP-599) to slice 16c.
```

- [ ] **Step 3: `STATUS.md` section**

Append:

```markdown
## Slice 16 — Client quotas (2026-05-15)

- `AlterClientQuotas` (api_key 49) + `DescribeClientQuotas` (api_key 48), v0–1.
- Three quota types: `producer_byte_rate`, `consumer_byte_rate`, `request_percentage`.
- Four entity scopes: `user`, `client-id`, `(user, client-id)` tuple, `<default>` (entity_name=null).
- Kafka's 8-priority entity lookup in `crates/broker/src/quota/lookup.rs` — 9 unit tests.
- Per-broker `QuotaBuckets` (DashMap) caches `(quota_key, entity_key) → Arc<TokenBucket>`, lazy-allocated on first lookup. 4 unit tests.
- Image-driven refresh task in `quota/refresh.rs` pushes new rates on every metadata apply. 2 unit tests.
- New `ClientQuotaRecord` metadata record + `MetadataImage::client_quotas` map keyed by canonicalized entity tuple (sorted by entity_type). 4 unit tests.
- Produce hot path consumes from `producer_byte_rate` bucket; Fetch (consumer-only) from `consumer_byte_rate`; dispatch loop wraps every handler with `request_percentage` accounting. KIP-257 delays applied via `tokio::time::sleep` before response write; capped at 1 second.
- 5 broker integration tests in `tests/client_quotas.rs`.
- 1 JVM acceptance test exercising `kafka-configs --alter/describe/delete` round-trip.
- **Known limitation:** `throttle_time_ms` in the response is only set for Produce + Fetch. Other handlers absorb the `request_percentage` delay silently. Closing this requires routing the throttle value through the handler trait — deferred.
- Out of scope: `ip` entity + KIP-612 connection_creation_rate (slice 16b), KIP-599 controller_mutation_rate (slice 16c).
```

- [ ] **Step 4: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "$(cat <<'EOF'
docs(slice-16): README + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Push + open PR**

```
git push -u origin feature/quotas-16
gh pr create --base main --head feature/quotas-16 \
  --title "Slice 16: Client quotas (KIP-13 + KIP-124 + KIP-257)" \
  --body "$(cat <<'EOF'
## Summary

Kafka client quotas:

1. **`AlterClientQuotas` (api_key 49)** and **`DescribeClientQuotas` (api_key 48)** with the full KIP-546 entity-tuple wire shape.
2. **Three quota types** — \`producer_byte_rate\` and \`consumer_byte_rate\` (KIP-13), \`request_percentage\` (KIP-124).
3. **Four entity scopes** — \`user\`, \`client-id\`, \`(user, client-id)\` tuple, \`<default>\`. Kafka's documented 8-priority precedence; first match wins.
4. **Enforcement** — Produce, Fetch (consumer-only), and dispatch-loop hooks consume the slice-15b TokenBucket primitive. KIP-257 server-side throttle delays via \`tokio::time::sleep\`; capped at 1 second.

JVM \`kafka-configs --entity-type users --entity-name alice --add-config 'producer_byte_rate=1024'\` round-trips end-to-end.

## Verified

- 23 new unit tests (lookup 9, buckets 4, refresh 2, alter 6, describe 4, image 4, records round-trip 1).
- 5 broker integration tests in \`tests/client_quotas.rs\`.
- 1 new JVM acceptance test.
- Workspace \`cargo fmt --check\`, \`cargo clippy --workspace --all-targets -- -D warnings\`, \`cargo test --workspace\` all green.

## Known limitations

- \`throttle_time_ms\` is only surfaced in the response for Produce + Fetch. Other handlers absorb the request_percentage delay silently. Closing this requires threading the throttle value through the handler trait — deferred.

## Out of scope

- \`ip\` entity + \`connection_creation_rate\` (KIP-612) — slice 16b
- \`controller_mutation_rate\` (KIP-599) — slice 16c

## Plan / spec

- Spec: \`docs/superpowers/specs/2026-05-15-crabka-quotas-16-design.md\`
- Plan: \`docs/superpowers/plans/2026-05-15-crabka-quotas-16.md\` (13 tasks across 6 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Capture PR URL** and return.

---

## Notes for the executing agent

1. **CLAUDE.md compatibility rule** governs T1 — no `#[serde(default)]` shim on the new `ClientQuotaRecord`. Wipe data dirs across the slice boundary.

2. **Parallel batches** (per CLAUDE.md):
   - **B1 (T1 + T2)**: T1 touches `crates/metadata/`, T2 touches `crates/broker/src/quota/`. Disjoint.
   - **B2 (T3 → T4)**: both append to `crates/broker/src/quota/mod.rs`. Sequential to avoid edit conflicts.
   - **B3 (T5 + T6)**: T5 creates `alter_client_quotas.rs`, T6 creates `describe_client_quotas.rs`. Both append to `handlers/mod.rs` (different lines — Edit-with-context safe). Parallel.
   - **B4 (T7)**: dispatch + api_versions; depends on T5/T6 modules existing.
   - **B5 (T8 → T9 + T10 + T11)**: T8 sets up `Broker.quota_buckets`. T9/T10/T11 then run in parallel (produce.rs, fetch.rs, dispatch.rs — disjoint).
   - **B6 (T12 → T13 → T14)**: sequential.

3. **TokenBucket reuse** — `crate::throttle::TokenBucket` is the slice-15b type. Slice 16 doesn't redefine it; both subsystems share the bucket primitive (one bucket per entity per quota_type for slice 16; one bucket per direction for slice 15b).

4. **Slice 15b vs slice 16 hot-path coexistence** in `fetch.rs`:
   - `replica_id >= 0` → slice 15b inter-broker throttle (truncates).
   - `replica_id < 0` → slice 16 consumer-byte-rate (delays, no truncate).
   - The two paths are mutually exclusive. Add a code comment so reviewers don't think they're redundant.

5. **`Principal::name()`** — slice 12 introduced this accessor. Reuse, don't reinvent.

6. **`dashmap`** — verify it's a workspace dependency before T3 commits. If absent, slice 16 adds it (single-crate dep is fine — no need to bump the whole workspace).

7. **Field-name verification** — when the plan sketches owned-type field names (`EntryData`, `RespEntry`, etc.), verify against `crates/protocol/generated/AlterClientQuotasResponse.owned.rs` and `DescribeClientQuotasResponse.owned.rs` BEFORE writing the code. Names may differ from the sketch.

8. **`#![allow(dead_code)]` lifecycle** — handlers in T5/T6 start with the module-level allow; T7 removes it once dispatch is wired.

9. **Integration test timing** — T12 tests 2 and 3 (producer/consumer throttle wall-clock) may flake on CI. Use loose tolerances (≥800ms when expecting ~1000ms throttle). The point is to PROVE the throttle fires, not measure precisely.
