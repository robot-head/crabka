# KIP-714 Client Metrics Push Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Crabka's no-op KIP-714 stubs with a full client-metrics receiver: operator-configured `CLIENT_METRICS` subscriptions, per-broker client-instance state, OTLP `MetricsData` decode, and a dual sink (dynamic Prometheus collector + OTLP forward).

**Architecture:** Subscriptions are stored as dynamic configs on a `CLIENT_METRICS(16)` resource through the existing metadata-record → raft → `MetadataImage` path. A per-broker `ClientMetricsManager` (in-memory; KIP-714 is per-broker by design) matches connecting clients to subscriptions, computes a stable `subscription_id`, tracks instance state/throttling, and on `PushTelemetry` decodes the OTLP payload and fans it out to a Prometheus `Collector` (served on `/metrics`) and an OTLP forwarder.

**Tech Stack:** Rust 2024, `prometheus-client 0.24` (dynamic `Collector`), `opentelemetry-proto 0.32` (prost `MetricsData`/`ExportMetricsServiceRequest`), `crc32c 0.6`, `crabka-compression` (codec decode), `serde`/`wincode` (metadata records).

**Design spec:** `docs/superpowers/specs/2026-05-30-crabka-kip-714-client-metrics-design.md`

---

## Reference facts (verified against Apache Kafka trunk + the codebase)

- Error codes: `UNKNOWN_SUBSCRIPTION_ID=117` (**already in `codes.rs:106`**), `TELEMETRY_TOO_LARGE=118`, `UNSUPPORTED_COMPRESSION_TYPE=76`, `THROTTLING_QUOTA_EXCEEDED=89`. `INVALID_RESOURCE_TYPE` is an alias of `INVALID_REQUEST=42`.
- `ConfigResource.Type.CLIENT_METRICS = 16`. `DescribeConfigsResponse` config-source byte for client metrics = **7** (`CLIENT_METRICS_CONFIG`).
- Config keys: `metrics` (CSV prefix list; single `"*"` = all), `interval.ms` (int, **100‥3_600_000**, default `300000`), `match` (list of `key=regex`; six selectors).
- Match selectors (exact strings): `client_instance_id`, `client_id`, `client_software_name`, `client_software_version`, `client_source_address`, `client_source_port`.
- `subscription_id = (Crc32C(utf8(set_to_string(metrics) + decimal(push_interval_ms))) as i32) XOR uuid_hashcode(client_instance_id)`. Self-consistency (change detection across re-fetch) is the only requirement — JVM byte-equality is **not** required, so a deterministic sorted `set_to_string` is acceptable.
- `GetTelemetrySubscriptionsResponse` fixed fields: `accepted_compression_types = [4,3,1,2]` (ZSTD,LZ4,GZIP,SNAPPY; NONE not advertised), `delta_temporality = true`, `telemetry_max_bytes` default `1048576`.
- Telemetry RPCs (71/72) are **unauthenticated**. `IncrementalAlterConfigs(CLIENT_METRICS)` needs `ALTER_CONFIGS` on `Cluster`; describe/list need `Describe`/`DescribeConfigs` on `Cluster`.
- `crabka-compression`: `decompress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError>`; `CompressionType::from_attribute_bits(b: u8) -> Option<Self>` maps codec ids 0–4.

## File structure

| File | Responsibility | Tasks |
|---|---|---|
| `crates/broker/src/codes.rs` | add 3 error constants | 1 |
| `crates/metadata/src/records.rs` | `ClientMetricsConfigRecord` + enum variant | 2 |
| `crates/metadata/src/image.rs` | store/apply/snapshot/accessors | 2 |
| `Cargo.toml`, `crates/broker/Cargo.toml` | add `opentelemetry-proto` | 3 |
| `crates/broker/src/client_metrics/config.rs` | config key validation + `match`/metrics parsing | 4 |
| `crates/broker/src/handlers/incremental_alter_configs.rs` | CLIENT_METRICS alter branch | 5 |
| `crates/broker/src/handlers/describe_configs.rs` | CLIENT_METRICS describe (defaults+synonyms, src 7) | 6 |
| `crates/broker/src/handlers/list_config_resources.rs` | enumerate subscriptions | 7 |
| `crates/broker/src/client_metrics/mod.rs` | `ClientMetricsManager`, `ClientInstance`, matching, sub-id, throttle | 8 |
| `crates/broker/src/client_metrics/otlp.rs` | OTLP `MetricsData` decode | 9 |
| `crates/broker/src/client_metrics/prometheus_sink.rs` | dynamic `Collector` | 10 |
| `crates/broker/src/client_metrics/otlp_sink.rs` | OTLP forward | 11 |
| `crates/broker/src/broker.rs` | own `ClientMetricsManager`, register collector | 12 |
| `crates/broker/src/network/dispatch.rs`, `.../handlers/context.rs`, `.../handlers/mod.rs` | capture software name/version, `TelemetryContext`, inline-intercept 71/72 | 13 |
| `crates/broker/src/handlers/get_telemetry_subscriptions.rs` | real handshake | 14 |
| `crates/broker/src/handlers/push_telemetry.rs` | real ingest + error ladder | 15 |
| `crates/broker/tests/...` | integration | 16, 17, 18 |

## Execution batches (per CLAUDE.md — parallel where file sets are disjoint)

- **Batch A (parallel):** Task 1, Task 2, Task 3, Task 4 — disjoint files/crates.
- **Batch B (parallel):** Task 5, Task 6, Task 7 — disjoint handler files; depend on A(2,4).
- **Batch C:** Task 8 ∥ Task 9 first; then Task 10 ∥ Task 11. Depend on A(3,4).
- **Batch D:** Task 12 ∥ Task 13; then Task 14 ∥ Task 15. Depend on B, C.
- **Batch E (sequential):** Task 16, Task 17, Task 18.

---

## Batch A — Foundations

### Task 1: Add wire error codes

**Files:**
- Modify: `crates/broker/src/codes.rs` (append before the `from_broker_error` fn near line 225)

- [ ] **Step 1: Confirm which constants are missing**

Run: `grep -nE "UNSUPPORTED_COMPRESSION_TYPE|THROTTLING_QUOTA_EXCEEDED|TELEMETRY_TOO_LARGE|UNKNOWN_SUBSCRIPTION_ID" crates/broker/src/codes.rs`
Expected: only `UNKNOWN_SUBSCRIPTION_ID` (117) is present. The other three are absent.

- [ ] **Step 2: Add the constants**

Insert after the `INCONSISTENT_CLUSTER_ID` constant (line ~227), before the `from_broker_error` doc comment:

```rust
/// `UNSUPPORTED_COMPRESSION_TYPE` (76) — KIP-714 `PushTelemetry` carried a
/// `compression_type` the broker can't decompress.
pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;

/// `THROTTLING_QUOTA_EXCEEDED` (89) — KIP-714 client pushed/fetched
/// telemetry faster than the configured interval allows.
pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;

/// `TELEMETRY_TOO_LARGE` (118) — KIP-714 `PushTelemetry` payload exceeded
/// `telemetry.max.bytes`.
pub const TELEMETRY_TOO_LARGE: i16 = 118;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p crabka-broker 2>&1 | tail -5`
Expected: builds (warnings about unused consts are fine until later tasks use them).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/codes.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(codes): add KIP-714 telemetry error codes"
```

---

### Task 2: Client-metrics metadata record + image storage

**Files:**
- Modify: `crates/metadata/src/records.rs` (record struct + enum variant)
- Modify: `crates/metadata/src/image.rs` (field, `apply` arm, accessors, `to_records`)
- Test: inline `#[cfg(test)]` in both files

- [ ] **Step 1: Write failing record round-trip test**

In `crates/metadata/src/records.rs` `mod tests`, add:

```rust
#[test]
fn client_metrics_config_round_trip() {
    let mut overrides = std::collections::BTreeMap::new();
    overrides.insert("interval.ms".to_string(), "60000".to_string());
    overrides.insert("metrics".to_string(), "org.apache.kafka.consumer.".to_string());
    let r = MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
        name: "sub-a".into(),
        configs: overrides,
    });
    assert_eq!(round_trip(&r), r);
}
```

- [ ] **Step 2: Run it — must fail to compile**

Run: `cargo test -p crabka-metadata client_metrics_config_round_trip 2>&1 | tail -5`
Expected: FAIL — `ClientMetricsConfigRecord` / `V1ClientMetricsConfig` not found.

- [ ] **Step 3: Add the record struct and enum variant**

In `crates/metadata/src/records.rs`, after `BrokerConfigRecord` (line ~99):

```rust
/// KIP-714 client-metrics subscription config. Authoritative target
/// state: each `V1ClientMetricsConfig` fully replaces the previous
/// override map for `name` (the subscription name). Empty map = delete
/// the subscription. Merging happens at the `IncrementalAlterConfigs`
/// handler before the record is submitted (same pattern as
/// [`TopicConfigRecord`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetricsConfigRecord {
    pub name: String,
    pub configs: std::collections::BTreeMap<String, String>,
}
```

In the `MetadataRecord` enum (after `V1FeatureLevel`, line ~202):

```rust
    V1ClientMetricsConfig(ClientMetricsConfigRecord),
```

- [ ] **Step 4: Run record test — must pass**

Run: `cargo test -p crabka-metadata client_metrics_config_round_trip 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 5: Write failing image apply/accessor test**

In `crates/metadata/src/image.rs` `mod tests`, add:

```rust
#[test]
fn client_metrics_config_apply_and_clear() {
    use crate::records::ClientMetricsConfigRecord;
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    let mut cfgs = std::collections::BTreeMap::new();
    cfgs.insert("interval.ms".to_string(), "60000".to_string());
    img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
        name: "sub-a".into(),
        configs: cfgs,
    }));
    assert_eq!(
        img.client_metrics_config("sub-a").and_then(|m| m.get("interval.ms")).map(String::as_str),
        Some("60000")
    );
    assert_eq!(img.client_metrics_subscriptions().count(), 1);
    // Empty map clears.
    img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
        name: "sub-a".into(),
        configs: std::collections::BTreeMap::new(),
    }));
    assert!(img.client_metrics_config("sub-a").is_none());
    assert_eq!(img.client_metrics_subscriptions().count(), 0);
}
```

- [ ] **Step 6: Run it — must fail to compile**

Run: `cargo test -p crabka-metadata client_metrics_config_apply_and_clear 2>&1 | tail -5`
Expected: FAIL — `client_metrics_config` / `client_metrics_subscriptions` not found.

- [ ] **Step 7: Add image field, apply arm, accessors, snapshot**

In the `MetadataImage` struct (near the other `*_configs` fields, line ~62):

```rust
    client_metrics_configs: HashMap<String, BTreeMap<String, String>>,
```

(Ensure it is initialized to `HashMap::new()` in `MetadataImage::new` alongside the other config maps.)

In `apply`, add an arm mirroring `V1TopicConfig` (near line ~382):

```rust
            MetadataRecord::V1ClientMetricsConfig(c) => {
                if c.configs.is_empty() {
                    self.client_metrics_configs.remove(&c.name);
                } else {
                    self.client_metrics_configs.insert(c.name.clone(), c.configs.clone());
                }
            }
```

Add accessors near `topic_config` (line ~146):

```rust
    /// Override map for a single KIP-714 client-metrics subscription.
    #[must_use]
    pub fn client_metrics_config(&self, name: &str) -> Option<&BTreeMap<String, String>> {
        self.client_metrics_configs.get(name)
    }

    /// All configured client-metrics subscriptions, `(name, overrides)`.
    pub fn client_metrics_subscriptions(
        &self,
    ) -> impl Iterator<Item = (&String, &BTreeMap<String, String>)> {
        self.client_metrics_configs.iter()
    }
```

In `to_records` (near the topic-config emission, line ~521), emit one record per subscription:

```rust
        for (name, configs) in &self.client_metrics_configs {
            records.push(MetadataRecord::V1ClientMetricsConfig(
                crate::records::ClientMetricsConfigRecord {
                    name: name.clone(),
                    configs: configs.clone(),
                },
            ));
        }
```

- [ ] **Step 8: Run image test — must pass**

Run: `cargo test -p crabka-metadata client_metrics 2>&1 | tail -10`
Expected: both tests PASS.

- [ ] **Step 9: Verify exhaustiveness elsewhere**

Run: `cargo build -p crabka-metadata 2>&1 | tail -15`
Expected: builds. If any other `match` on `MetadataRecord` is non-`_`-terminated and breaks, add the new arm there (search `grep -rn "MetadataRecord::V1FeatureLevel" crates/`). Fix any non-exhaustive match by handling `V1ClientMetricsConfig` analogously to `V1TopicConfig`.

- [ ] **Step 10: Commit**

```bash
git add crates/metadata/src/records.rs crates/metadata/src/image.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(metadata): ClientMetricsConfigRecord + image storage"
```

---

### Task 3: Add `opentelemetry-proto` dependency

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/broker/Cargo.toml` (consume it)

- [ ] **Step 1: Add the workspace dependency**

In `Cargo.toml`, in `[workspace.dependencies]` near the other opentelemetry lines (after line 72):

```toml
# Slice KIP-714: prost-generated OTLP MetricsData / ExportMetricsServiceRequest
# message types (aligned with the opentelemetry 0.32 stack). gen-tonic-messages
# pulls the prost structs without a tonic client; metrics gates the metrics module.
opentelemetry-proto = { version = "0.32", default-features = false, features = ["gen-tonic-messages", "metrics"] }
```

- [ ] **Step 2: Consume it in the broker crate**

In `crates/broker/Cargo.toml`, in `[dependencies]` near the other opentelemetry deps:

```toml
opentelemetry-proto = { workspace = true }
```

- [ ] **Step 3: Verify the message types resolve**

Run: `cargo build -p crabka-broker 2>&1 | tail -5`
Expected: builds (no usage yet).

Verify the path that Task 9 will import exists:

Run: `cargo doc -p opentelemetry-proto --no-deps 2>&1 | tail -3; echo done`
Expected: `done` (doc builds). The types Task 9 uses are `opentelemetry_proto::tonic::metrics::v1::MetricsData` and `opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest`.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/broker/Cargo.toml Cargo.lock
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "build: add opentelemetry-proto for KIP-714 OTLP decode"
```

---

### Task 4: Client-metrics config validation module

**Files:**
- Create: `crates/broker/src/client_metrics/config.rs`
- Modify: `crates/broker/src/client_metrics/mod.rs` (create as a stub that declares `pub(crate) mod config;`)
- Modify: `crates/broker/src/lib.rs` (or `broker.rs`) to add `mod client_metrics;`

This module is pure (no broker/image deps) so it can be built and tested in isolation.

- [ ] **Step 1: Create the module wiring**

Create `crates/broker/src/client_metrics/mod.rs`:

```rust
//! KIP-714 client metrics receiver: subscription config, client-instance
//! registry, OTLP decode, and the Prometheus + OTLP sinks.

pub(crate) mod config;
```

Find where modules are declared (search `grep -n "^mod \|^pub(crate) mod \|^pub mod " crates/broker/src/lib.rs crates/broker/src/broker.rs | head`) and add, in the same style and alphabetical position:

```rust
mod client_metrics;
```

- [ ] **Step 2: Write failing validation tests**

Create `crates/broker/src/client_metrics/config.rs` containing only its `#[cfg(test)]` module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_bounds_enforced() {
        assert!(validate("interval.ms", "300000").is_ok());
        assert!(validate("interval.ms", "100").is_ok());
        assert!(validate("interval.ms", "3600000").is_ok());
        assert!(validate("interval.ms", "99").is_err());
        assert!(validate("interval.ms", "3600001").is_err());
        assert!(validate("interval.ms", "not-a-number").is_err());
    }

    #[test]
    fn unknown_key_rejected() {
        assert!(validate("bogus.key", "x").is_err());
    }

    #[test]
    fn match_selectors_validated() {
        assert!(validate("match", "client_id=my-app.*").is_ok());
        assert!(validate("match", "client_software_name=apache-kafka-java,client_id=svc-.*").is_ok());
        // unknown selector
        assert!(validate("match", "client_foo=x").is_err());
        // missing '='
        assert!(validate("match", "client_id").is_err());
        // bad regex
        assert!(validate("match", "client_id=[unclosed").is_err());
    }

    #[test]
    fn metrics_list_accepts_star_and_prefixes() {
        assert!(validate("metrics", "*").is_ok());
        assert!(validate("metrics", "org.apache.kafka.consumer.,org.apache.kafka.producer.").is_ok());
        assert!(validate("metrics", "").is_ok()); // empty = no metrics
    }

    #[test]
    fn effective_interval_defaults_and_clamps() {
        let mut m = std::collections::BTreeMap::new();
        assert_eq!(effective_interval_ms(&m), DEFAULT_INTERVAL_MS);
        m.insert("interval.ms".to_string(), "60000".to_string());
        assert_eq!(effective_interval_ms(&m), 60_000);
    }

    #[test]
    fn parse_match_rules_roundtrip() {
        let rules = parse_match_rules("client_id=svc-.*,client_software_name=java").unwrap();
        assert_eq!(rules.len(), 2);
        assert!(rules.iter().any(|r| r.selector == MatchSelector::ClientId));
    }

    #[test]
    fn parse_metrics_collapses_star() {
        assert_eq!(parse_metrics("*"), vec!["*".to_string()]);
        assert_eq!(parse_metrics(""), Vec::<String>::new());
        assert_eq!(parse_metrics("a.,b."), vec!["a.".to_string(), "b.".to_string()]);
    }
}
```

- [ ] **Step 3: Run — must fail to compile**

Run: `cargo test -p crabka-broker client_metrics::config 2>&1 | tail -5`
Expected: FAIL — items not defined.

- [ ] **Step 4: Implement the module (above the test module)**

Prepend to `crates/broker/src/client_metrics/config.rs`:

```rust
//! Validation + parsing for KIP-714 `CLIENT_METRICS` config resources.
//!
//! Three keys only (matching `org.apache.kafka.server.metrics.ClientMetricsConfigs`):
//! `metrics` (CSV prefix list; the single token `"*"` = all), `interval.ms`
//! (int, 100..=3_600_000, default 300000), and `match` (CSV of `selector=regex`).

use std::collections::BTreeMap;

use regex::Regex;

pub(crate) const KEY_METRICS: &str = "metrics";
pub(crate) const KEY_INTERVAL_MS: &str = "interval.ms";
pub(crate) const KEY_MATCH: &str = "match";

pub(crate) const DEFAULT_INTERVAL_MS: i32 = 300_000;
pub(crate) const MIN_INTERVAL_MS: i64 = 100;
pub(crate) const MAX_INTERVAL_MS: i64 = 3_600_000;
pub(crate) const ALL_METRICS: &str = "*";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MatchSelector {
    ClientInstanceId,
    ClientId,
    ClientSoftwareName,
    ClientSoftwareVersion,
    ClientSourceAddress,
    ClientSourcePort,
}

impl MatchSelector {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "client_instance_id" => Self::ClientInstanceId,
            "client_id" => Self::ClientId,
            "client_software_name" => Self::ClientSoftwareName,
            "client_software_version" => Self::ClientSoftwareVersion,
            "client_source_address" => Self::ClientSourceAddress,
            "client_source_port" => Self::ClientSourcePort,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MatchRule {
    pub selector: MatchSelector,
    pub pattern: Regex,
}

/// Validate a single `(key, value)` against KIP-714 rules. Returns a
/// human-readable reason on failure (surfaced as `INVALID_CONFIG`).
pub(crate) fn validate(key: &str, value: &str) -> Result<(), String> {
    match key {
        KEY_METRICS => Ok(()), // any CSV of prefixes (or "*"); always valid
        KEY_INTERVAL_MS => {
            let n: i64 = value
                .parse()
                .map_err(|_| format!("interval.ms must be an integer, got `{value}`"))?;
            if (MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&n) {
                Ok(())
            } else {
                Err(format!(
                    "interval.ms must be in [{MIN_INTERVAL_MS}, {MAX_INTERVAL_MS}], got {n}"
                ))
            }
        }
        KEY_MATCH => parse_match_rules(value).map(|_| ()),
        other => Err(format!("unknown client-metrics config key `{other}`")),
    }
}

/// True if `key` is one of the three recognized client-metrics keys.
pub(crate) fn is_recognized(key: &str) -> bool {
    matches!(key, KEY_METRICS | KEY_INTERVAL_MS | KEY_MATCH)
}

/// Effective push interval for a subscription's override map (default when unset).
pub(crate) fn effective_interval_ms(configs: &BTreeMap<String, String>) -> i32 {
    configs
        .get(KEY_INTERVAL_MS)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(DEFAULT_INTERVAL_MS)
}

/// Parse the `metrics` value into prefixes. `"*"` collapses to `["*"]`;
/// empty string yields an empty list (no metrics).
pub(crate) fn parse_metrics(value: &str) -> Vec<String> {
    if value.trim().is_empty() {
        return Vec::new();
    }
    value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Parse the `match` value into compiled selector rules. Empty = match-all.
pub(crate) fn parse_match_rules(value: &str) -> Result<Vec<MatchRule>, String> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut rules = Vec::new();
    for entry in value.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (sel, pat) = entry
            .split_once('=')
            .ok_or_else(|| format!("match entry `{entry}` is not `selector=regex`"))?;
        let selector = MatchSelector::parse(sel.trim())
            .ok_or_else(|| format!("unknown match selector `{}`", sel.trim()))?;
        let pattern = Regex::new(pat.trim())
            .map_err(|e| format!("invalid regex for `{}`: {e}", sel.trim()))?;
        rules.push(MatchRule { selector, pattern });
    }
    Ok(rules)
}
```

Note: confirm `regex` is a broker dependency (`grep -n '^regex' crates/broker/Cargo.toml`); it is used widely in the codebase. If absent, add `regex = { workspace = true }`.

- [ ] **Step 5: Run — must pass**

Run: `cargo test -p crabka-broker client_metrics::config 2>&1 | tail -15`
Expected: all 7 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/client_metrics/ crates/broker/src/lib.rs crates/broker/Cargo.toml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): config key validation + match/metrics parsing"
```

---

## Batch B — Config handlers (depend on Task 2, Task 4)

### Task 5: IncrementalAlterConfigs — CLIENT_METRICS branch

**Files:**
- Modify: `crates/broker/src/handlers/incremental_alter_configs.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing unit test**

Add to `mod tests` in `incremental_alter_configs.rs`:

```rust
    #[test]
    fn client_metrics_set_produces_record() {
        use crabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;
        let img = MetadataImage::new(uuid::Uuid::nil());
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![
                AlterableConfig { name: "interval.ms".into(), config_operation: OP_SET, value: Some("60000".into()), ..Default::default() },
                AlterableConfig { name: "metrics".into(), config_operation: OP_SET, value: Some("org.apache.kafka.consumer.".into()), ..Default::default() },
            ],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert_eq!(out.error_code, codes::NONE);
        assert_eq!(to_submit.len(), 1);
        match &to_submit[0] {
            MetadataRecord::V1ClientMetricsConfig(rec) => {
                assert_eq!(rec.name, "sub-a");
                assert_eq!(rec.configs.get("interval.ms").map(String::as_str), Some("60000"));
            }
            other => panic!("expected V1ClientMetricsConfig, got {other:?}"),
        }
    }

    #[test]
    fn client_metrics_bad_interval_rejected() {
        use crabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;
        let img = MetadataImage::new(uuid::Uuid::nil());
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![AlterableConfig { name: "interval.ms".into(), config_operation: OP_SET, value: Some("5".into()), ..Default::default() }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert_eq!(out.error_code, codes::INVALID_CONFIG);
        assert!(to_submit.is_empty());
    }

    #[test]
    fn client_metrics_delete_drops_key() {
        use crabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;
        use crabka_metadata::ClientMetricsConfigRecord;
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let mut existing = std::collections::BTreeMap::new();
        existing.insert("interval.ms".to_string(), "60000".to_string());
        existing.insert("metrics".to_string(), "a.".to_string());
        img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord { name: "sub-a".into(), configs: existing }));
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![AlterableConfig { name: "interval.ms".into(), config_operation: OP_DELETE, value: None, ..Default::default() }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert_eq!(out.error_code, codes::NONE);
        match &to_submit[0] {
            MetadataRecord::V1ClientMetricsConfig(rec) => {
                assert!(!rec.configs.contains_key("interval.ms"));
                assert_eq!(rec.configs.get("metrics").map(String::as_str), Some("a."));
            }
            other => panic!("expected V1ClientMetricsConfig, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Run — must fail to compile**

Run: `cargo test -p crabka-broker incremental_alter_configs::tests::client_metrics 2>&1 | tail -5`
Expected: FAIL — `RESOURCE_TYPE_CLIENT_METRICS` / `handle_client_metrics_scoped` not found.

- [ ] **Step 3: Add the constant, ACL arm, dispatch arm, and helper**

Add near the other resource-type consts (line ~36):

```rust
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;
```

Add a new import: `use crabka_metadata::ClientMetricsConfigRecord;` (extend the existing `use crabka_metadata::{...}` block).

In the ACL match (after the `RESOURCE_TYPE_BROKER` arm, line ~116), add a CLIENT_METRICS arm — same `Cluster` `AlterConfigs` gate as BROKER:

```rust
            RESOURCE_TYPE_CLIENT_METRICS => broker.config.authorizer.authorize(
                &image,
                &AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Cluster,
                    resource_name: "kafka-cluster",
                    operation: AclOperation::AlterConfigs,
                },
            ),
```

In the deny mapping (line ~128), CLIENT_METRICS already falls into the `_ => CLUSTER_AUTHORIZATION_FAILED` arm — leave it.

In the post-ACL dispatch `match resource.resource_type` (line ~139), add an arm before the `_`:

```rust
            RESOURCE_TYPE_CLIENT_METRICS => {
                handle_client_metrics_scoped(&resource, &image, &mut out, &mut to_submit);
                if out.error_code != codes::NONE {
                    responses.push(out);
                    continue;
                }
            }
```

Add the helper next to `handle_broker_scoped`:

```rust
/// Merge per-key ops into a client-metrics subscription's override map and
/// stage a `V1ClientMetricsConfig` record. SET validates per KIP-714;
/// DELETE drops the override (effective value reverts to its default at
/// read time); APPEND/SUBTRACT are rejected.
fn handle_client_metrics_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    if resource.resource_name.is_empty() {
        out.error_code = codes::INVALID_REQUEST;
        out.error_message = Some("client-metrics subscription name must not be empty".into());
        return;
    }
    let mut merged = image
        .client_metrics_config(&resource.resource_name)
        .cloned()
        .unwrap_or_default();
    for cfg in &resource.configs {
        match cfg.config_operation {
            OP_SET => {
                let value = cfg.value.clone().unwrap_or_default();
                if let Err(reason) = crate::client_metrics::config::validate(&cfg.name, &value) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(reason);
                    return;
                }
                merged.insert(cfg.name.clone(), value);
            }
            OP_DELETE => {
                if !crate::client_metrics::config::is_recognized(&cfg.name) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(format!("unrecognized config key `{}`", cfg.name));
                    return;
                }
                merged.remove(&cfg.name);
            }
            op => {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(format!(
                    "config_operation={op} (APPEND/SUBTRACT) not supported for client-metrics key `{}`",
                    cfg.name
                ));
                return;
            }
        }
    }
    to_submit.push(MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
        name: resource.resource_name.clone(),
        configs: merged,
    }));
}
```

- [ ] **Step 4: Run — must pass**

Run: `cargo test -p crabka-broker incremental_alter_configs::tests::client_metrics 2>&1 | tail -10`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/incremental_alter_configs.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(configs): IncrementalAlterConfigs CLIENT_METRICS branch"
```

---

### Task 6: DescribeConfigs — CLIENT_METRICS branch (defaults + synonyms, source 7)

**Files:**
- Modify: `crates/broker/src/handlers/describe_configs.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing test**

Add to `mod tests`:

```rust
    #[test]
    fn client_metrics_describe_emits_defaults() {
        use crabka_metadata::{ClientMetricsConfigRecord, MetadataRecord};
        let mut img = MetadataImage::new(Uuid::nil());
        let mut cfgs = std::collections::BTreeMap::new();
        cfgs.insert("metrics".to_string(), "a.".to_string());
        img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "sub-a".into(),
            configs: cfgs,
        }));
        let r = crabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configuration_keys: None,
            ..Default::default()
        };
        let res = super::describe_one(&img, r);
        assert_eq!(res.error_code, crate::codes::NONE);
        // All three keys reported, including defaulted interval.ms and match.
        let by_name: std::collections::HashMap<_, _> =
            res.configs.iter().map(|c| (c.name.as_str(), c)).collect();
        assert_eq!(by_name["metrics"].value.as_deref(), Some("a."));
        assert_eq!(by_name["metrics"].config_source, super::CONFIG_SOURCE_CLIENT_METRICS);
        assert_eq!(by_name["interval.ms"].value.as_deref(), Some("300000"));
        assert_eq!(by_name["interval.ms"].config_source, super::CONFIG_SOURCE_DEFAULT);
    }
```

- [ ] **Step 2: Run — must fail to compile**

Run: `cargo test -p crabka-broker describe_configs::tests::client_metrics 2>&1 | tail -5`
Expected: FAIL — consts/arm not defined.

- [ ] **Step 3: Implement**

Add consts near line 36:

```rust
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;
/// `DescribeConfigsResponse.ConfigSource::CLIENT_METRICS_CONFIG` wire byte.
const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;
/// `ConfigSource::DEFAULT_CONFIG` — used for keys reported at their default.
const CONFIG_SOURCE_DEFAULT: i8 = 5;
```

In `describe_one`, before the final `ok(Vec::new())` (line ~108):

```rust
    if r.resource_type == RESOURCE_TYPE_CLIENT_METRICS {
        use crate::client_metrics::config::{
            DEFAULT_INTERVAL_MS, KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS,
        };
        let overrides = image.client_metrics_config(&r.resource_name).cloned().unwrap_or_default();
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let mut configs = Vec::new();
        // Emit all three keys: set values use CLIENT_METRICS_CONFIG source;
        // unset keys report their default value/source (KAFKA-17516 — tooling
        // needs effective values, not blanks).
        let mut emit = |key: &str, default: &str| {
            if key_filter.is_some_and(|ks| !ks.iter().any(|f| f == key)) {
                return;
            }
            match overrides.get(key) {
                Some(v) => configs.push(make_entry(key, v, CONFIG_SOURCE_CLIENT_METRICS)),
                None => configs.push(make_entry(key, default, CONFIG_SOURCE_DEFAULT)),
            }
        };
        emit(KEY_METRICS, "");
        emit(KEY_INTERVAL_MS, &DEFAULT_INTERVAL_MS.to_string());
        emit(KEY_MATCH, "");
        return ok(configs);
    }
```

- [ ] **Step 4: Run — must pass**

Run: `cargo test -p crabka-broker describe_configs::tests::client_metrics 2>&1 | tail -8`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/describe_configs.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(configs): DescribeConfigs CLIENT_METRICS with defaults+synonyms"
```

---

### Task 7: ListConfigResources — enumerate subscriptions

**Files:**
- Modify: `crates/broker/src/handlers/list_config_resources.rs`

- [ ] **Step 1: Replace the failing-expectation tests**

In `list_config_resources.rs`, the tests `v0_returns_client_metrics_only_which_is_empty` and `v1_client_metrics_filter_returns_empty` assert emptiness. Update them to expect enumerated subscriptions, and extend the test image builder. Replace those two tests with:

```rust
    fn image_with_subs(names: &[&str]) -> MetadataImage {
        use crabka_metadata::ClientMetricsConfigRecord;
        let mut img = MetadataImage::new(Uuid::nil());
        for n in names {
            let mut cfgs = std::collections::BTreeMap::new();
            cfgs.insert("interval.ms".to_string(), "60000".to_string());
            img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
                name: (*n).into(),
                configs: cfgs,
            }));
        }
        img
    }

    #[test]
    fn v0_returns_client_metrics_subscriptions() {
        let img = image_with_subs(&["sub-b", "sub-a"]);
        let out = collect_resources(&img, 0, &[]);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r.resource_type == RESOURCE_TYPE_CLIENT_METRICS));
        assert_eq!(out[0].resource_name, "sub-a"); // sorted
        assert_eq!(out[1].resource_name, "sub-b");
    }

    #[test]
    fn v1_client_metrics_filter_returns_subscriptions() {
        let img = image_with_subs(&["sub-a"]);
        let out = collect_resources(&img, 1, &[RESOURCE_TYPE_CLIENT_METRICS]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].resource_type, RESOURCE_TYPE_CLIENT_METRICS);
        assert_eq!(out[0].resource_name, "sub-a");
    }
```

- [ ] **Step 2: Run — must fail**

Run: `cargo test -p crabka-broker list_config_resources 2>&1 | tail -10`
Expected: FAIL — CLIENT_METRICS arm still empty; new tests expect subscriptions.

- [ ] **Step 3: Implement the enumeration arm**

In `collect_resources`, replace the empty CLIENT_METRICS comment/`_ => {}` handling (line ~134) with an explicit arm before the catch-all `_`:

```rust
            RESOURCE_TYPE_CLIENT_METRICS => {
                for (name, _cfgs) in image.client_metrics_subscriptions() {
                    out.push(ConfigResource {
                        resource_name: name.clone(),
                        resource_type: RESOURCE_TYPE_CLIENT_METRICS,
                        ..Default::default()
                    });
                }
            }
            // Unknown types (BROKER_LOGGER, GROUP, anything new) silently drop.
            _ => {}
```

- [ ] **Step 4: Run — must pass**

Run: `cargo test -p crabka-broker list_config_resources 2>&1 | tail -12`
Expected: all PASS (existing topic/broker tests + new subscription tests).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/list_config_resources.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(configs): ListConfigResources enumerates client-metrics subscriptions"
```

---

## Batch C — Manager & sinks

### Task 8: ClientMetricsManager — matching, subscription_id, instance state, throttle

**Files:**
- Modify: `crates/broker/src/client_metrics/mod.rs`
- Create: `crates/broker/src/client_metrics/manager.rs`
- Test: inline `#[cfg(test)]` in `manager.rs`

This task implements pure logic against an injected `&MetadataImage` and an injected clock, so it is testable without a running broker. Sinks are wired in Task 12/15 — keep this file sink-agnostic.

- [ ] **Step 1: Declare the submodule**

In `crates/broker/src/client_metrics/mod.rs` add:

```rust
pub(crate) mod manager;
pub(crate) use manager::ClientMetricsManager;
```

- [ ] **Step 2: Write failing tests**

Create `crates/broker/src/client_metrics/manager.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn img_with(name: &str, kvs: &[(&str, &str)]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        let mut cfgs = BTreeMap::new();
        for (k, v) in kvs {
            cfgs.insert((*k).to_string(), (*v).to_string());
        }
        img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: name.into(),
            configs: cfgs,
        }));
        img
    }

    fn attrs() -> ClientAttributes {
        ClientAttributes {
            client_instance_id: Uuid::from_u128(1),
            client_id: "svc-1".into(),
            software_name: "apache-kafka-java".into(),
            software_version: "3.9.0".into(),
            source_address: "10.0.0.5".into(),
            source_port: 5556,
        }
    }

    #[test]
    fn no_subscription_means_no_metrics() {
        let img = MetadataImage::new(Uuid::nil());
        let m = compute_subscription(&img, &attrs());
        assert!(m.metrics.is_empty());
        assert_eq!(m.push_interval_ms, 300_000);
    }

    #[test]
    fn match_all_empty_match_applies() {
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let m = compute_subscription(&img, &attrs());
        assert_eq!(m.metrics, vec!["*".to_string()]);
        assert_eq!(m.push_interval_ms, 60_000);
    }

    #[test]
    fn selector_filters_clients() {
        let img = img_with("java-only", &[("metrics", "a."), ("match", "client_software_name=apache-kafka-java")]);
        let m = compute_subscription(&img, &attrs());
        assert_eq!(m.metrics, vec!["a.".to_string()]);

        let img2 = img_with("py-only", &[("metrics", "a."), ("match", "client_software_name=kafka-python")]);
        let m2 = compute_subscription(&img2, &attrs());
        assert!(m2.metrics.is_empty(), "java client must not match python selector");
    }

    #[test]
    fn min_interval_and_metric_union_across_subs() {
        let mut img = img_with("s1", &[("metrics", "a."), ("interval.ms", "60000")]);
        img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "s2".into(),
            configs: {
                let mut c = BTreeMap::new();
                c.insert("metrics".into(), "b.".into());
                c.insert("interval.ms".into(), "30000".into());
                c
            },
        }));
        let m = compute_subscription(&img, &attrs());
        let mut got = m.metrics.clone();
        got.sort();
        assert_eq!(got, vec!["a.".to_string(), "b.".to_string()]);
        assert_eq!(m.push_interval_ms, 30_000); // min
    }

    #[test]
    fn star_collapses_union() {
        let mut img = img_with("s1", &[("metrics", "a.")]);
        img.apply(&MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "s2".into(),
            configs: { let mut c = BTreeMap::new(); c.insert("metrics".into(), "*".into()); c },
        }));
        let m = compute_subscription(&img, &attrs());
        assert_eq!(m.metrics, vec!["*".to_string()]);
    }

    #[test]
    fn subscription_id_stable_and_change_sensitive() {
        let a = attrs();
        let s1 = ComputedSubscription { metrics: vec!["a.".into(), "b.".into()], push_interval_ms: 60_000 };
        let id1 = subscription_id(&s1, a.client_instance_id);
        // order-independent
        let s1b = ComputedSubscription { metrics: vec!["b.".into(), "a.".into()], push_interval_ms: 60_000 };
        assert_eq!(id1, subscription_id(&s1b, a.client_instance_id));
        // interval change → different id
        let s2 = ComputedSubscription { metrics: vec!["a.".into(), "b.".into()], push_interval_ms: 30_000 };
        assert_ne!(id1, subscription_id(&s2, a.client_instance_id));
        // metrics change → different id
        let s3 = ComputedSubscription { metrics: vec!["a.".into()], push_interval_ms: 60_000 };
        assert_ne!(id1, subscription_id(&s3, a.client_instance_id));
    }
}
```

- [ ] **Step 3: Run — must fail to compile**

Run: `cargo test -p crabka-broker client_metrics::manager 2>&1 | tail -5`
Expected: FAIL — items undefined.

- [ ] **Step 4: Implement the manager (above the test module)**

```rust
//! Per-broker KIP-714 client-metrics state: instance registry, subscription
//! matching, stable subscription-id computation, and push throttling. All
//! state is in-memory (KIP-714 is per-broker — a client pins telemetry to
//! one broker, so no raft replication is needed).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crabka_metadata::MetadataImage;

use super::config::{self, ALL_METRICS};

/// Connection-derived attributes used for subscription matching.
#[derive(Debug, Clone)]
pub(crate) struct ClientAttributes {
    pub client_instance_id: Uuid,
    pub client_id: String,
    pub software_name: String,
    pub software_version: String,
    pub source_address: String,
    pub source_port: u16,
}

/// The metric prefixes + push interval a client should use, after unioning
/// every matched subscription.
#[derive(Debug, Clone)]
pub(crate) struct ComputedSubscription {
    pub metrics: Vec<String>,
    pub push_interval_ms: i32,
}

/// Live per-instance state.
#[derive(Debug)]
struct ClientInstance {
    subscription_id: i32,
    push_interval: Duration,
    metrics: Vec<String>,
    last_get: Instant,
    last_push: Option<Instant>,
    terminating: bool,
    last_error: i16,
}

/// Outcome of registering/refreshing an instance on GetTelemetrySubscriptions.
pub(crate) struct SubscriptionAssignment {
    pub subscription_id: i32,
    pub push_interval_ms: i32,
    pub metrics: Vec<String>,
}

/// Decision returned by [`ClientMetricsManager::authorize_push`].
pub(crate) enum PushDecision {
    Accept { metrics: Vec<String> },
    /// `error_code` from `crate::codes` (UNKNOWN_SUBSCRIPTION_ID,
    /// THROTTLING_QUOTA_EXCEEDED, INVALID_REQUEST).
    Reject { error_code: i16, throttle_ms: i32 },
}

pub(crate) struct ClientMetricsManager {
    instances: Mutex<HashMap<Uuid, ClientInstance>>,
    telemetry_max_bytes: i32,
}

/// Compression codecs the broker advertises, in Kafka's fixed order:
/// ZSTD(4), LZ4(3), GZIP(1), SNAPPY(2). NONE is intentionally not advertised.
pub(crate) const ACCEPTED_COMPRESSION_TYPES: [i8; 4] = [4, 3, 1, 2];

impl ClientMetricsManager {
    pub(crate) fn new(telemetry_max_bytes: i32) -> Self {
        Self { instances: Mutex::new(HashMap::new()), telemetry_max_bytes }
    }

    pub(crate) fn telemetry_max_bytes(&self) -> i32 {
        self.telemetry_max_bytes
    }

    /// Handle a GetTelemetrySubscriptions: compute the matched subscription,
    /// register/refresh the instance, and return what to send back.
    pub(crate) fn assign(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
    ) -> SubscriptionAssignment {
        let computed = compute_subscription(image, attrs);
        let sub_id = subscription_id(&computed, attrs.client_instance_id);
        let now = Instant::now();
        let mut guard = self.instances.lock().expect("client-metrics mutex poisoned");
        let inst = guard.entry(attrs.client_instance_id).or_insert(ClientInstance {
            subscription_id: sub_id,
            push_interval: Duration::from_millis(computed.push_interval_ms as u64),
            metrics: computed.metrics.clone(),
            last_get: now,
            last_push: None,
            terminating: false,
            last_error: crate::codes::NONE,
        });
        inst.subscription_id = sub_id;
        inst.push_interval = Duration::from_millis(computed.push_interval_ms as u64);
        inst.metrics = computed.metrics.clone();
        inst.last_get = now;
        inst.last_error = crate::codes::NONE;
        SubscriptionAssignment {
            subscription_id: sub_id,
            push_interval_ms: computed.push_interval_ms,
            metrics: computed.metrics,
        }
    }

    /// Validate a PushTelemetry against instance state + throttling rules.
    /// `payload_len` is the on-wire metrics byte length (before decompress).
    pub(crate) fn authorize_push(
        &self,
        client_instance_id: Uuid,
        subscription_id_in: i32,
        terminating: bool,
        payload_len: usize,
    ) -> PushDecision {
        let now = Instant::now();
        let mut guard = self.instances.lock().expect("client-metrics mutex poisoned");
        let Some(inst) = guard.get_mut(&client_instance_id) else {
            // Unknown/evicted instance — Kafka has no dedicated code; the
            // client must re-fetch subscriptions.
            return PushDecision::Reject { error_code: crate::codes::INVALID_REQUEST, throttle_ms: 0 };
        };
        if inst.terminating {
            return PushDecision::Reject { error_code: crate::codes::INVALID_REQUEST, throttle_ms: 0 };
        }
        if subscription_id_in != inst.subscription_id {
            inst.last_error = crate::codes::UNKNOWN_SUBSCRIPTION_ID;
            return PushDecision::Reject { error_code: crate::codes::UNKNOWN_SUBSCRIPTION_ID, throttle_ms: 0 };
        }
        if payload_len as i32 > self.telemetry_max_bytes {
            return PushDecision::Reject { error_code: crate::codes::TELEMETRY_TOO_LARGE, throttle_ms: 0 };
        }
        // Throttle: accept if first push after a fresh get, or interval elapsed,
        // or terminating (bypasses interval once).
        let interval_elapsed = inst
            .last_push
            .is_none_or(|lp| now.duration_since(lp) >= inst.push_interval);
        let first_after_get = inst.last_push.is_none_or(|lp| inst.last_get > lp);
        if !terminating && !interval_elapsed && !first_after_get {
            inst.last_error = crate::codes::THROTTLING_QUOTA_EXCEEDED;
            let throttle_ms = inst.push_interval.as_millis() as i32;
            return PushDecision::Reject { error_code: crate::codes::THROTTLING_QUOTA_EXCEEDED, throttle_ms };
        }
        inst.last_push = Some(now);
        inst.last_error = crate::codes::NONE;
        let metrics = inst.metrics.clone();
        if terminating {
            inst.terminating = true;
        }
        PushDecision::Accept { metrics }
    }

    /// Drop instances idle beyond `max(interval * factor, floor)`. Call
    /// periodically from a broker background task.
    pub(crate) fn evict_stale(&self, factor: u32, floor: Duration) {
        let now = Instant::now();
        let mut guard = self.instances.lock().expect("client-metrics mutex poisoned");
        guard.retain(|_, inst| {
            if inst.terminating {
                return false;
            }
            let ttl = (inst.push_interval * factor).max(floor);
            let last = inst.last_push.unwrap_or(inst.last_get);
            now.duration_since(last) < ttl
        });
    }
}

/// Evaluate every subscription in `image` against `attrs` and union the result.
pub(crate) fn compute_subscription(
    image: &MetadataImage,
    attrs: &ClientAttributes,
) -> ComputedSubscription {
    let mut matched_metrics: Vec<String> = Vec::new();
    let mut min_interval: Option<i32> = None;
    let mut any_star = false;

    for (_name, configs) in image.client_metrics_subscriptions() {
        let rules = match configs.get(config::KEY_MATCH) {
            Some(v) => match config::parse_match_rules(v) {
                Ok(r) => r,
                Err(_) => continue, // a stored-invalid match never matches
            },
            None => Vec::new(),
        };
        if !rules.iter().all(|r| selector_matches(r, attrs)) {
            continue;
        }
        let metrics = configs.get(config::KEY_METRICS).map_or_else(Vec::new, |v| config::parse_metrics(v));
        if metrics.is_empty() {
            // matched but subscribes to nothing — still counts toward interval? No:
            // Kafka only lowers the interval for subscriptions that contribute
            // metrics. Skip empty metric sets.
            continue;
        }
        if metrics.iter().any(|m| m == ALL_METRICS) {
            any_star = true;
        }
        for m in metrics {
            if !matched_metrics.contains(&m) {
                matched_metrics.push(m);
            }
        }
        let interval = config::effective_interval_ms(configs);
        min_interval = Some(min_interval.map_or(interval, |cur| cur.min(interval)));
    }

    let metrics = if any_star { vec![ALL_METRICS.to_string()] } else { matched_metrics };
    ComputedSubscription {
        metrics,
        push_interval_ms: min_interval.unwrap_or(config::DEFAULT_INTERVAL_MS),
    }
}

fn selector_matches(rule: &config::MatchRule, attrs: &ClientAttributes) -> bool {
    use config::MatchSelector::*;
    let target: std::borrow::Cow<'_, str> = match rule.selector {
        ClientInstanceId => attrs.client_instance_id.to_string().into(),
        ClientId => (&attrs.client_id).into(),
        ClientSoftwareName => (&attrs.software_name).into(),
        ClientSoftwareVersion => (&attrs.software_version).into(),
        ClientSourceAddress => (&attrs.source_address).into(),
        ClientSourcePort => attrs.source_port.to_string().into(),
    };
    // Full-match semantics (anchored), matching Kafka's Pattern.matches().
    rule.pattern.find(&target).is_some_and(|m| m.start() == 0 && m.end() == target.len())
}

/// Stable, change-sensitive subscription id. CRC32C over a canonical
/// (sorted) rendering of the metric set + push interval, XORed with the
/// instance-id hash. Self-consistent across re-fetch; not byte-identical to
/// the JVM broker (which is not required — the client only compares the id
/// to its own previous value).
pub(crate) fn subscription_id(sub: &ComputedSubscription, client_instance_id: Uuid) -> i32 {
    let mut sorted = sub.metrics.clone();
    sorted.sort();
    let rendered = format!("[{}]{}", sorted.join(", "), sub.push_interval_ms);
    let crc = crc32c::crc32c(rendered.as_bytes()) as i32;
    crc ^ uuid_hashcode(client_instance_id)
}

/// Reproduces `java.util.UUID.hashCode()` for parity of shape (the exact
/// value need not match the JVM — it is XORed into a CRC).
fn uuid_hashcode(id: Uuid) -> i32 {
    let bytes = id.as_bytes();
    let msb = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let lsb = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let hilo = msb ^ lsb;
    ((hilo >> 32) as i32) ^ (hilo as i32)
}
```

Note: confirm `crc32c` is a broker dependency (`grep -n 'crc32c' crates/broker/Cargo.toml`); add `crc32c = { workspace = true }` if missing.

- [ ] **Step 5: Run — must pass**

Run: `cargo test -p crabka-broker client_metrics::manager 2>&1 | tail -15`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/client_metrics/ crates/broker/Cargo.toml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): manager — matching, subscription_id, throttle"
```

---

### Task 9: OTLP MetricsData decode

**Files:**
- Create: `crates/broker/src/client_metrics/otlp.rs`
- Modify: `crates/broker/src/client_metrics/mod.rs` (`pub(crate) mod otlp;`)

- [ ] **Step 1: Declare submodule + write failing test**

Add to `mod.rs`: `pub(crate) mod otlp;`

Create `crates/broker/src/client_metrics/otlp.rs` with test first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{
        Metric, MetricsData, ResourceMetrics, ScopeMetrics, Gauge, NumberDataPoint,
        number_data_point::Value, metric::Data,
    };
    use prost::Message;

    fn sample_metrics_data() -> Vec<u8> {
        let dp = NumberDataPoint { value: Some(Value::AsInt(42)), ..Default::default() };
        let metric = Metric {
            name: "org.apache.kafka.consumer.fetch.size".into(),
            data: Some(Data::Gauge(Gauge { data_points: vec![dp] })),
            ..Default::default()
        };
        let md = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics { metrics: vec![metric], ..Default::default() }],
                ..Default::default()
            }],
        };
        md.encode_to_vec()
    }

    #[test]
    fn decodes_valid_metrics_data() {
        let bytes = sample_metrics_data();
        let md = decode_metrics(&bytes).expect("decode");
        assert_eq!(md.resource_metrics.len(), 1);
    }

    #[test]
    fn rejects_garbage() {
        // Not all garbage fails prost (it's permissive), but truncated varints do.
        let bad = vec![0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_metrics(&bad).is_err());
    }
}
```

- [ ] **Step 2: Run — must fail to compile**

Run: `cargo test -p crabka-broker client_metrics::otlp 2>&1 | tail -5`
Expected: FAIL — `decode_metrics` not defined.

- [ ] **Step 3: Implement**

Prepend:

```rust
//! Decode KIP-714 PushTelemetry payloads (OTLP `MetricsData` v1 protobuf).

use opentelemetry_proto::tonic::metrics::v1::MetricsData;
use prost::Message;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OtlpDecodeError {
    #[error("OTLP MetricsData protobuf decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Decode a (decompressed) OTLP `MetricsData` protobuf payload.
pub(crate) fn decode_metrics(bytes: &[u8]) -> Result<MetricsData, OtlpDecodeError> {
    Ok(MetricsData::decode(bytes)?)
}
```

Confirm `thiserror` and `prost` are broker deps (both are used widely; `grep -n 'thiserror\|^prost' crates/broker/Cargo.toml`). Add `prost = { workspace = true }` if absent.

- [ ] **Step 4: Run — must pass**

Run: `cargo test -p crabka-broker client_metrics::otlp 2>&1 | tail -8`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/client_metrics/ crates/broker/Cargo.toml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): OTLP MetricsData decode"
```

---

### Task 10: Prometheus sink — dynamic `Collector`

**Files:**
- Create: `crates/broker/src/client_metrics/prometheus_sink.rs`
- Modify: `crates/broker/src/client_metrics/mod.rs` (`pub(crate) mod prometheus_sink;`)

The sink holds a snapshot of recently-pushed numeric data points and renders them at scrape time via `prometheus_client::collector::Collector`. Histograms render as a gauge of `_count`/`_sum` (KIP-714 client histograms are rare; full bucket fidelity is out of scope per the spec).

- [ ] **Step 1: Declare submodule + write failing test**

Add to `mod.rs`: `pub(crate) mod prometheus_sink;`

Create `prometheus_sink.rs` test-first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn ingest_then_encode_contains_series() {
        let sink = ClientMetricsCollector::new(Duration::from_secs(60));
        sink.ingest(&[DataPoint {
            metric: "org.apache.kafka.consumer.fetch.size".into(),
            client_instance_id: "11111111-1111-1111-1111-111111111111".into(),
            client_id: "svc-1".into(),
            value: 42.0,
        }]);
        // Encode through a Registry that registers this collector.
        use prometheus_client::registry::Registry;
        let mut reg = Registry::default();
        reg.register_collector(Box::new(sink));
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &reg).unwrap();
        assert!(buf.contains("client_instance_id=\"11111111-1111-1111-1111-111111111111\""), "got:\n{buf}");
        assert!(buf.contains("42"), "value missing:\n{buf}");
    }

    #[test]
    fn stale_points_evicted_on_encode() {
        let sink = ClientMetricsCollector::new(Duration::from_millis(0));
        sink.ingest(&[DataPoint { metric: "m".into(), client_instance_id: "i".into(), client_id: "c".into(), value: 1.0 }]);
        // ttl 0 → immediately stale
        assert_eq!(sink.live_point_count(), 0);
    }
}
```

- [ ] **Step 2: Run — must fail**

Run: `cargo test -p crabka-broker client_metrics::prometheus_sink 2>&1 | tail -5`
Expected: FAIL — items undefined.

- [ ] **Step 3: Implement**

Prepend:

```rust
//! Prometheus sink for KIP-714 client metrics. Client metric *names* are
//! dynamic, so we register a custom `Collector` (rather than static
//! `Family`s) that renders a live, staleness-pruned snapshot at scrape time
//! as `crabka_client_*` series.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::gauge::ConstGauge;

/// A single decoded client metric data point destined for Prometheus.
#[derive(Debug, Clone)]
pub(crate) struct DataPoint {
    pub metric: String,
    pub client_instance_id: String,
    pub client_id: String,
    pub value: f64,
}

#[derive(Debug)]
struct StoredPoint {
    value: f64,
    at: Instant,
}

/// Key identifies a unique series: (metric name, instance id, client id).
type SeriesKey = (String, String, String);

#[derive(Debug)]
pub(crate) struct ClientMetricsCollector {
    points: Mutex<HashMap<SeriesKey, StoredPoint>>,
    ttl: Duration,
}

impl ClientMetricsCollector {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self { points: Mutex::new(HashMap::new()), ttl }
    }

    /// Record/replace the latest value for each point and prune stale ones.
    pub(crate) fn ingest(&self, points: &[DataPoint]) {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        for p in points {
            guard.insert(
                (p.metric.clone(), p.client_instance_id.clone(), p.client_id.clone()),
                StoredPoint { value: p.value, at: now },
            );
        }
        guard.retain(|_, sp| now.duration_since(sp.at) < self.ttl);
    }

    /// Count of non-stale points (test helper / gauge feed).
    pub(crate) fn live_point_count(&self) -> usize {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        guard.retain(|_, sp| now.duration_since(sp.at) < self.ttl);
        guard.len()
    }
}

impl Collector for ClientMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let now = Instant::now();
        let guard = self.points.lock().expect("prom sink mutex poisoned");
        // Group by sanitized metric name → emit each (instance,client) series.
        for ((metric, instance, client), sp) in guard.iter() {
            if now.duration_since(sp.at) >= self.ttl {
                continue;
            }
            let name = sanitize(metric);
            let mut m = encoder.encode_descriptor(&name, "client-reported metric (KIP-714)", None, prometheus_client::metrics::MetricType::Gauge)?;
            let labels = [("client_instance_id", instance.as_str()), ("client_id", client.as_str())];
            let gauge = ConstGauge::new(sp.value);
            let enc = m.encode_family(&labels)?;
            gauge.encode(enc)?;
        }
        Ok(())
    }
}

/// Prometheus metric names allow `[a-zA-Z0-9_:]`; map everything else to `_`
/// and prefix with `crabka_client_`.
fn sanitize(metric: &str) -> String {
    let body: String = metric
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' })
        .collect();
    format!("crabka_client_{body}")
}
```

Note: the exact `DescriptorEncoder`/`encode_family` API shape is `prometheus-client 0.24`. If a method signature differs at build time, adjust to the 0.24 `Collector` API (the trait is `prometheus_client::collector::Collector` with `fn encode(&self, encoder: DescriptorEncoder) -> Result<(), fmt::Error>`). Verify against `cargo doc -p prometheus-client --no-deps` if needed.

- [ ] **Step 4: Run — must pass**

Run: `cargo test -p crabka-broker client_metrics::prometheus_sink 2>&1 | tail -12`
Expected: PASS. If the encoder API differs, fix the `encode` body to the 0.24 signatures until tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/client_metrics/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): dynamic Prometheus collector sink"
```

---

### Task 11: OTLP forward sink

**Files:**
- Create: `crates/broker/src/client_metrics/otlp_sink.rs`
- Modify: `crates/broker/src/client_metrics/mod.rs` (`pub(crate) mod otlp_sink;`)

The forwarder wraps decoded `ResourceMetrics` in an `ExportMetricsServiceRequest`, injects client-instance-id as a resource attribute, and POSTs to the OTLP metrics endpoint on a bounded background channel (so a slow collector never blocks the request path). It is a no-op when no endpoint is configured.

- [ ] **Step 1: Declare submodule + write failing test**

Add to `mod.rs`: `pub(crate) mod otlp_sink;`

Create `otlp_sink.rs` test-first (test the pure transform, not the network send):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{MetricsData, ResourceMetrics};

    #[test]
    fn wraps_and_injects_instance_id() {
        let md = MetricsData { resource_metrics: vec![ResourceMetrics::default()] };
        let req = build_export_request(md, "abc-123");
        assert_eq!(req.resource_metrics.len(), 1);
        let res = req.resource_metrics[0].resource.as_ref().expect("resource");
        assert!(res.attributes.iter().any(|kv| kv.key == "client_instance_id"));
    }

    #[test]
    fn disabled_forwarder_is_noop() {
        let f = OtlpForwarder::disabled();
        // Must not panic and must report disabled.
        assert!(!f.is_enabled());
        f.forward(MetricsData::default(), "x"); // no-op
    }
}
```

- [ ] **Step 2: Run — must fail**

Run: `cargo test -p crabka-broker client_metrics::otlp_sink 2>&1 | tail -5`
Expected: FAIL — items undefined.

- [ ] **Step 3: Implement**

Prepend:

```rust
//! OTLP forward sink for KIP-714 client metrics. Re-emits decoded client
//! `MetricsData` to the OTLP collector already used for traces. Sends happen
//! on a bounded background task; overflow is dropped + counted so the request
//! path never blocks on a slow collector.

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::MetricsData;
use tokio::sync::mpsc;

/// Build an OTLP export request from decoded metrics, tagging every resource
/// with the originating client's instance id.
pub(crate) fn build_export_request(
    mut md: MetricsData,
    client_instance_id: &str,
) -> ExportMetricsServiceRequest {
    for rm in &mut md.resource_metrics {
        let resource = rm.resource.get_or_insert_with(Default::default);
        resource.attributes.push(KeyValue {
            key: "client_instance_id".to_string(),
            value: Some(AnyValue { value: Some(Value::StringValue(client_instance_id.to_string())) }),
        });
    }
    ExportMetricsServiceRequest { resource_metrics: md.resource_metrics }
}

pub(crate) struct OtlpForwarder {
    tx: Option<mpsc::Sender<(MetricsData, String)>>,
}

impl OtlpForwarder {
    /// Disabled forwarder (no endpoint configured). All `forward` calls no-op.
    pub(crate) fn disabled() -> Self {
        Self { tx: None }
    }

    /// Spawn a background worker that POSTs export requests to `endpoint`
    /// (HTTP/protobuf `/v1/metrics`). `capacity` bounds the in-flight queue.
    pub(crate) fn spawn(endpoint: String, capacity: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<(MetricsData, String)>(capacity);
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/v1/metrics", endpoint.trim_end_matches('/'));
            while let Some((md, instance)) = rx.recv().await {
                let req = build_export_request(md, &instance);
                let body = {
                    use prost::Message;
                    req.encode_to_vec()
                };
                if let Err(e) = client
                    .post(&url)
                    .header("content-type", "application/x-protobuf")
                    .body(body)
                    .send()
                    .await
                {
                    tracing::debug!(error = %e, "client-metrics OTLP forward failed");
                }
            }
        });
        Self { tx: Some(tx) }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.tx.is_some()
    }

    /// Enqueue metrics for forwarding. Drops (with a debug log) if the queue
    /// is full or the forwarder is disabled — never blocks.
    pub(crate) fn forward(&self, md: MetricsData, client_instance_id: &str) {
        if let Some(tx) = &self.tx {
            if let Err(e) = tx.try_send((md, client_instance_id.to_string())) {
                tracing::debug!(error = %e, "client-metrics OTLP forward queue full; dropping");
            }
        }
    }
}
```

Confirm `reqwest` and `tokio` (with `sync`/`rt`) are broker deps (both are; `grep -n 'reqwest\|^tokio' crates/broker/Cargo.toml`). Add `reqwest = { workspace = true }` if absent.

- [ ] **Step 4: Run — must pass**

Run: `cargo test -p crabka-broker client_metrics::otlp_sink 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/client_metrics/ crates/broker/Cargo.toml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): OTLP forward sink"
```

---

## Batch D — Wiring & handlers

### Task 12: Own `ClientMetricsManager` on `Broker`; register the collector

**Files:**
- Modify: `crates/broker/src/client_metrics/mod.rs` (a `ClientMetrics` facade bundling manager + sinks)
- Modify: `crates/broker/src/broker.rs` (field + construction)

- [ ] **Step 1: Add a facade type bundling manager + sinks**

In `crates/broker/src/client_metrics/mod.rs`, append:

```rust
use std::sync::Arc;
use std::time::Duration;

use self::manager::ClientMetricsManager;
use self::otlp_sink::OtlpForwarder;
use self::prometheus_sink::ClientMetricsCollector;

/// Default `telemetry.max.bytes` (1 MiB), matching Kafka.
pub(crate) const DEFAULT_TELEMETRY_MAX_BYTES: i32 = 1_048_576;
/// Staleness TTL for the Prometheus snapshot (data points older than this are
/// pruned at scrape time).
pub(crate) const PROM_SNAPSHOT_TTL: Duration = Duration::from_secs(300);

/// Broker-held bundle: the manager (instance state + matching) plus the two
/// sinks. The Prometheus collector is shared with the metrics registry.
pub(crate) struct ClientMetrics {
    pub manager: ClientMetricsManager,
    pub prometheus: Arc<ClientMetricsCollector>,
    pub otlp: OtlpForwarder,
}

impl ClientMetrics {
    /// `otlp_endpoint` is `None` when OTLP forwarding is disabled.
    pub(crate) fn new(telemetry_max_bytes: i32, otlp_endpoint: Option<String>) -> Self {
        let otlp = match otlp_endpoint {
            Some(ep) => OtlpForwarder::spawn(ep, 256),
            None => OtlpForwarder::disabled(),
        };
        Self {
            manager: ClientMetricsManager::new(telemetry_max_bytes),
            prometheus: Arc::new(ClientMetricsCollector::new(PROM_SNAPSHOT_TTL)),
            otlp,
        }
    }
}
```

Make the `ClientMetricsCollector` registrable while shared: `prometheus-client`'s `register_collector` takes `Box<dyn Collector>`. Register an `Arc`-backed shim — implement `Collector` for `Arc<ClientMetricsCollector>` by delegating:

```rust
impl prometheus_client::collector::Collector for Arc<ClientMetricsCollector> {
    fn encode(&self, encoder: prometheus_client::encoding::DescriptorEncoder) -> Result<(), std::fmt::Error> {
        (**self).encode(encoder)
    }
}
```

(Place this `impl` in `prometheus_sink.rs` next to the inherent impl, or in `mod.rs`.)

- [ ] **Step 2: Add the field to `Broker` and construct it**

In `crates/broker/src/broker.rs`, add to the `Broker` struct (near `metrics`):

```rust
    pub(crate) client_metrics: std::sync::Arc<crate::client_metrics::ClientMetrics>,
```

In `Broker::start_with_controller_listener`, where `BrokerMetrics::new()` is built and the registry is available, construct the bundle and register the collector into the shared registry **before** the `/metrics` server is spawned:

```rust
    let otlp_metrics_endpoint = crate::telemetry::OtlpConfig::from_env().map(|c| c.endpoint);
    let client_metrics = std::sync::Arc::new(crate::client_metrics::ClientMetrics::new(
        crate::client_metrics::DEFAULT_TELEMETRY_MAX_BYTES,
        otlp_metrics_endpoint,
    ));
    {
        // Register the dynamic client-metrics collector into the shared registry.
        let mut reg = metrics.registry().lock().await;
        reg.register_collector(Box::new(client_metrics.prometheus.clone()));
    }
```

Add `client_metrics` to the `Broker { ... }` struct literal.

If `BrokerMetrics` does not expose `registry()`, add a `pub(crate) fn registry(&self) -> SharedRegistry { self.registry.clone() }` accessor in `metrics.rs`.

- [ ] **Step 3: Add a periodic eviction task (optional but cheap)**

Where the broker spawns background loops, add:

```rust
    {
        let cm = client_metrics.clone();
        let token = shutdown.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tick.tick() => cm.manager.evict_stale(3, std::time::Duration::from_secs(600)),
                }
            }
        });
    }
```

- [ ] **Step 4: Verify build**

Run: `cargo build -p crabka-broker 2>&1 | tail -15`
Expected: builds. Resolve any registry-accessor or borrow issues. (`register_collector` exists on `prometheus_client::registry::Registry` in 0.24.)

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/client_metrics/ crates/broker/src/broker.rs crates/broker/src/metrics.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): wire ClientMetrics onto Broker + register collector"
```

---

### Task 13: Connection context plumbing (software name/version + inline intercept)

**Files:**
- Modify: `crates/broker/src/handlers/context.rs` (add `TelemetryContext`)
- Modify: `crates/broker/src/network/dispatch.rs` (capture software, intercept 71/72)
- Modify: `crates/broker/src/handlers/mod.rs` (remove 71/72 from `build_table`)

- [ ] **Step 1: Add a `TelemetryContext`**

In `crates/broker/src/handlers/context.rs`, add:

```rust
/// Connection attributes a KIP-714 telemetry handler needs to match a
/// client to a subscription. Telemetry RPCs are unauthenticated, so this
/// carries no principal — just the wire/connection-derived fields.
pub(crate) struct TelemetryContext<'a> {
    pub client_id: &'a str,
    pub peer: &'a std::net::SocketAddr,
    pub software_name: &'a str,
    pub software_version: &'a str,
}
```

- [ ] **Step 2: Capture client software on the connection**

In `crates/broker/src/network/dispatch.rs`, near where `peer` and `auth` are declared in the connection loop (line ~197), add:

```rust
    // KIP-714 matching needs the client's software fingerprint from the
    // ApiVersions v3+ handshake; capture it per-connection.
    let mut client_software_name = String::new();
    let mut client_software_version = String::new();
```

Find where `ApiVersions` (api_key 18) is handled in this loop (search the file for the ApiVersions intercept / `record_client_software`). On a successful v3+ ApiVersions request, after decoding, set:

```rust
        client_software_name = req.client_software_name.clone();
        client_software_version = req.client_software_version.clone();
```

(Match the exact variable name of the decoded ApiVersions request in that scope.)

- [ ] **Step 3: Intercept 71/72 inline with context**

In the same loop, where the api_key is peeked and inline interceptors run (mirror the IncrementalAlterConfigs `44` intercept), add arms for 71 and 72 that build a `TelemetryContext` and call the handlers directly. Example shape (adapt to the file's actual intercept structure and the `peek_client_id`/`peek_api_key` helpers):

```rust
            71 => {
                let client_id = peek_client_id(&frame).unwrap_or("");
                let tctx = crate::handlers::context::TelemetryContext {
                    client_id,
                    peer: &peer,
                    software_name: &client_software_name,
                    software_version: &client_software_version,
                };
                let (api_version, correlation_id, body) = split_header(&frame)?; // existing helper
                let resp = crate::handlers::get_telemetry_subscriptions::handle(&broker, api_version, correlation_id, body, &tctx).await?;
                encode_response(71, correlation_id, body_flexible(71, api_version), &resp)
            }
            72 => {
                let client_id = peek_client_id(&frame).unwrap_or("");
                let tctx = crate::handlers::context::TelemetryContext {
                    client_id,
                    peer: &peer,
                    software_name: &client_software_name,
                    software_version: &client_software_version,
                };
                let (api_version, correlation_id, body) = split_header(&frame)?;
                let resp = crate::handlers::push_telemetry::handle(&broker, api_version, correlation_id, body, &tctx).await?;
                encode_response(72, correlation_id, body_flexible(72, api_version), &resp)
            }
```

Use the exact header-split / `encode_response` helpers the neighboring intercept arms use (e.g. the Metadata/IncrementalAlterConfigs arms). The goal: 71/72 now receive `TelemetryContext`.

- [ ] **Step 4: Remove 71/72 from the HandlerTable**

In `crates/broker/src/handlers/mod.rs`, delete lines 247-248:

```rust
    t.register(71, get_telemetry_subscriptions::handle);
    t.register(72, push_telemetry::handle);
```

(The handlers now have a different signature and are dispatched inline.)

- [ ] **Step 5: Build (handlers still old-signature — expect errors, resolved in Task 14/15)**

Run: `cargo build -p crabka-broker 2>&1 | tail -20`
Expected: errors only about the 71/72 handler signatures (fixed next). If other errors appear, fix the intercept wiring.

- [ ] **Step 6: Commit (WIP allowed mid-batch)**

```bash
git add crates/broker/src/handlers/context.rs crates/broker/src/network/dispatch.rs crates/broker/src/handlers/mod.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): capture client software + inline-intercept 71/72"
```

---

### Task 14: GetTelemetrySubscriptions real handler

**Files:**
- Modify: `crates/broker/src/handlers/get_telemetry_subscriptions.rs`

- [ ] **Step 1: Rewrite the handler**

Replace the body of `get_telemetry_subscriptions.rs` with the context-taking, manager-backed implementation:

```rust
//! `GetTelemetrySubscriptions` (`api_key=71`, KIP-714). Assigns/echoes the
//! client instance id, matches the client against configured CLIENT_METRICS
//! subscriptions, and returns the computed subscription (metrics, interval,
//! id). See `client_metrics::manager`.

use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use crabka_protocol::owned::get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest;
use crabka_protocol::owned::get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::client_metrics::manager::{ClientAttributes, ACCEPTED_COMPRESSION_TYPES};
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::context::TelemetryContext;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &TelemetryContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = GetTelemetrySubscriptionsRequest::decode(&mut cur, version)?;

    // Assign a fresh id when the client sends nil; otherwise adopt theirs.
    // The response echoes a non-zero id only on first assignment.
    let (instance_uuid, echo_id) = if req.client_instance_id == WireUuid::ZERO {
        let fresh = Uuid::new_v4();
        (fresh, WireUuid(fresh.into_bytes()))
    } else {
        (Uuid::from_bytes(req.client_instance_id.0), WireUuid::ZERO)
    };

    let attrs = ClientAttributes {
        client_instance_id: instance_uuid,
        client_id: ctx.client_id.to_string(),
        software_name: ctx.software_name.to_string(),
        software_version: ctx.software_version.to_string(),
        source_address: ctx.peer.ip().to_string(),
        source_port: ctx.peer.port(),
    };

    let image = broker.controller.current_image();
    let assignment = broker.client_metrics.manager.assign(&image, &attrs);

    let resp = GetTelemetrySubscriptionsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        client_instance_id: echo_id,
        subscription_id: assignment.subscription_id,
        accepted_compression_types: ACCEPTED_COMPRESSION_TYPES.to_vec(),
        push_interval_ms: assignment.push_interval_ms,
        telemetry_max_bytes: broker.client_metrics.manager.telemetry_max_bytes(),
        delta_temporality: true,
        requested_metrics: assignment.metrics,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    // Behavioral coverage lives in the manager unit tests (matching/id) and
    // the integration test (full handshake over the wire). The handler is a
    // thin adapter; assert only the nil→assigned id contract here if desired.
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p crabka-broker 2>&1 | tail -15`
Expected: this handler compiles (push_telemetry still pending → Task 15).

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/handlers/get_telemetry_subscriptions.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): real GetTelemetrySubscriptions handler"
```

---

### Task 15: PushTelemetry real handler (error ladder + ingest)

**Files:**
- Modify: `crates/broker/src/handlers/push_telemetry.rs`

- [ ] **Step 1: Rewrite the handler**

```rust
//! `PushTelemetry` (`api_key=72`, KIP-714). Validates the push against the
//! client's subscription + throttle state, decompresses + decodes the OTLP
//! payload, and fans it out to the Prometheus + OTLP sinks.

use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use crabka_compression::CompressionType;
use crabka_protocol::owned::push_telemetry_request::PushTelemetryRequest;
use crabka_protocol::owned::push_telemetry_response::PushTelemetryResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::client_metrics::manager::PushDecision;
use crate::client_metrics::{otlp, prometheus_sink::DataPoint};
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::context::TelemetryContext;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    _ctx: &TelemetryContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = PushTelemetryRequest::decode(&mut cur, version)?;
    let instance = Uuid::from_bytes(req.client_instance_id.0);

    let mut error_code = codes::NONE;
    let mut throttle_time_ms = 0i32;

    // Unsupported compression is checked first among "payload" errors but
    // after the instance/sub-id/throttle gates handled by the manager.
    let codec = CompressionType::from_attribute_bits(u8::try_from(req.compression_type).unwrap_or(0xff));

    match broker.client_metrics.manager.authorize_push(
        instance,
        req.subscription_id,
        req.terminating,
        req.metrics.len(),
    ) {
        PushDecision::Reject { error_code: ec, throttle_ms } => {
            error_code = ec;
            throttle_time_ms = throttle_ms;
        }
        PushDecision::Accept { .. } => {
            match codec {
                None => error_code = codes::UNSUPPORTED_COMPRESSION_TYPE,
                Some(ct) => {
                    // Decompress → decode → fan out. Decode/sink failures are
                    // logged + counted, never surfaced (the push was valid).
                    match crabka_compression::decompress(ct, &req.metrics) {
                        Ok(raw) => match otlp::decode_metrics(&raw) {
                            Ok(md) => {
                                let instance_str = instance.to_string();
                                let points = flatten_for_prometheus(&md, &instance_str, _ctx.client_id);
                                broker.client_metrics.prometheus.ingest(&points);
                                broker.client_metrics.otlp.forward(md, &instance_str);
                            }
                            Err(e) => tracing::debug!(error = %e, "client-metrics OTLP decode failed"),
                        },
                        Err(e) => tracing::debug!(error = %e, "client-metrics decompress failed"),
                    }
                }
            }
        }
    }

    let resp = PushTelemetryResponse { throttle_time_ms, error_code, ..Default::default() };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Flatten an OTLP `MetricsData` into Prometheus data points (Sum/Gauge
/// numbers; Histogram → count/sum gauges). Best-effort — unknown shapes skipped.
fn flatten_for_prometheus(
    md: &opentelemetry_proto::tonic::metrics::v1::MetricsData,
    instance: &str,
    client_id: &str,
) -> Vec<DataPoint> {
    use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point::Value};
    let mut out = Vec::new();
    let num = |v: &Value| -> f64 {
        match v {
            Value::AsDouble(d) => *d,
            Value::AsInt(i) => *i as f64,
        }
    };
    for rm in &md.resource_metrics {
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let mut push_points = |dps: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint]| {
                    for dp in dps {
                        if let Some(v) = &dp.value {
                            out.push(DataPoint {
                                metric: m.name.clone(),
                                client_instance_id: instance.to_string(),
                                client_id: client_id.to_string(),
                                value: num(v),
                            });
                        }
                    }
                };
                match &m.data {
                    Some(Data::Gauge(g)) => push_points(&g.data_points),
                    Some(Data::Sum(s)) => push_points(&s.data_points),
                    Some(Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            out.push(DataPoint { metric: format!("{}_count", m.name), client_instance_id: instance.to_string(), client_id: client_id.to_string(), value: dp.count as f64 });
                            if let Some(sum) = dp.sum {
                                out.push(DataPoint { metric: format!("{}_sum", m.name), client_instance_id: instance.to_string(), client_id: client_id.to_string(), value: sum });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    out
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p crabka-broker 2>&1 | tail -20`
Expected: builds clean. Fix any field-name mismatches against the `opentelemetry-proto 0.32` generated types (e.g. histogram `sum` is `Option<f64>`; adjust if the generated shape differs).

- [ ] **Step 3: Run all broker unit tests**

Run: `cargo test -p crabka-broker client_metrics 2>&1 | tail -20`
Expected: all client_metrics unit tests still PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/handlers/push_telemetry.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(client-metrics): real PushTelemetry handler + dual-sink ingest"
```

---

## Batch E — Integration & verification

### Task 16: Integration test — config round-trip

**Files:**
- Create or extend: `crates/broker/tests/client_metrics_config.rs` (follow the existing integration-test harness used by other handler tests — search `grep -rl "IncrementalAlterConfigs\|incremental_alter" crates/broker/tests`)

- [ ] **Step 1: Write the round-trip test**

Using the existing in-process broker test harness, drive `IncrementalAlterConfigs(CLIENT_METRICS, "sub-a", {metrics=org.apache.kafka.consumer.,interval.ms=60000})`, then `DescribeConfigs(CLIENT_METRICS, "sub-a")` and assert: `metrics` returned with source 7, `interval.ms=60000` source 7, `match` defaulted; then `ListConfigResources(v1, [16])` returns `sub-a`. (Model the harness setup on the nearest existing config integration test.)

- [ ] **Step 2: Run**

Run: `cargo test -p crabka-broker --test client_metrics_config 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/client_metrics_config.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(client-metrics): config alter/describe/list round-trip"
```

---

### Task 17: Integration test — handshake + push + scrape

**Files:**
- Create: `crates/broker/tests/client_metrics_push.rs`

- [ ] **Step 1: Write the end-to-end test**

With a subscription `metrics=*, interval.ms=100, match=` configured: send `GetTelemetrySubscriptions{client_instance_id=nil}`, assert an id is assigned, `requested_metrics=["*"]`, `push_interval_ms=100`, `delta_temporality=true`, `accepted_compression_types=[4,3,1,2]`. Build an OTLP `MetricsData` (one Gauge), send `PushTelemetry{client_instance_id, subscription_id, compression=0, metrics=<bytes>}`, assert `error_code=NONE`. Then scrape the broker's `/metrics` HTTP endpoint and assert the body contains `crabka_client_` and the instance id. Also assert: a push with a wrong `subscription_id` → `UNKNOWN_SUBSCRIPTION_ID`; an oversized payload → `TELEMETRY_TOO_LARGE`; `compression_type=9` → `UNSUPPORTED_COMPRESSION_TYPE`.

- [ ] **Step 2: Run**

Run: `cargo test -p crabka-broker --test client_metrics_push 2>&1 | tail -25`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/client_metrics_push.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(client-metrics): handshake + push + /metrics scrape e2e"
```

---

### Task 18: cp-kafka parity check + full verification

**Files:** none (verification only; record findings in commit message if a fix is needed)

- [ ] **Step 1: Empirically confirm DescribeConfigs/ListConfigResources shapes**

Per CLAUDE.md, verify against the latest cp-kafka image: run `kafka-client-metrics.sh --bootstrap-server <crabka> --alter --name sub-a --metrics "*" --interval 60000`, then `--describe --name sub-a` and `--list`, comparing output framing to a real Kafka broker. If `kafka-configs.sh --entity-type client-metrics --describe --all` shows blanks against Crabka, the defaults/synonyms emission (Task 6) needs adjustment. Document any delta.

If a containerized Kafka isn't available in this environment, note that and rely on the byte-level response assertions in Tasks 16/17.

- [ ] **Step 2: Full workspace gate**

Run: `cargo fmt --all`
Run: `cargo clippy --workspace --all-targets 2>&1 | tail -20`
Expected: no warnings (the project gates on clippy + `cargo fmt --check`).

Run: `cargo test -p crabka-broker -p crabka-metadata 2>&1 | tail -20`
Expected: all PASS.

- [ ] **Step 3: Update API docs if generated**

Crabka auto-generates broker/API docs (`crabka-docgen`). If the build regenerates reference output, include it.

Run: `cargo build --workspace 2>&1 | tail -5`
Expected: builds; commit any regenerated docs.

- [ ] **Step 4: Final commit**

```bash
git add -A
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "chore(client-metrics): fmt + clippy + parity verification"
```

---

## Self-review notes

- **Spec coverage:** §3 storage → Tasks 2,5,6,7; §4 manager/matching/sub-id → Task 8; §4.5 plumbing → Task 13; §5 push ladder → Tasks 8,15; §5 codes → Task 1; §6 decode+sinks → Tasks 9,10,11; §6.2/§8 config → Tasks 11,12; §7 authz → Tasks 5 (alter ACL), list handler already gates Describe; §9 tests → Tasks 8,16,17,18. All covered.
- **Type consistency:** `ClientMetricsConfigRecord{name,configs}`, `MetadataRecord::V1ClientMetricsConfig`, `image.client_metrics_config`/`client_metrics_subscriptions`, `config::{validate,is_recognized,parse_metrics,parse_match_rules,effective_interval_ms,DEFAULT_INTERVAL_MS,ALL_METRICS,KEY_*}`, `manager::{ClientAttributes,ComputedSubscription,ClientMetricsManager,compute_subscription,subscription_id,PushDecision,SubscriptionAssignment,ACCEPTED_COMPRESSION_TYPES}`, `prometheus_sink::{ClientMetricsCollector,DataPoint}`, `otlp::decode_metrics`, `otlp_sink::{OtlpForwarder,build_export_request}`, `ClientMetrics{manager,prometheus,otlp}`, `TelemetryContext{client_id,peer,software_name,software_version}` — names are consistent across tasks.
- **Known soft spots flagged inline for the implementer:** exact `prometheus-client 0.24` `DescriptorEncoder`/`encode_family` signatures (Task 10), `opentelemetry-proto 0.32` generated field shapes (Tasks 9,11,15), and the precise dispatch-loop intercept helpers (`peek_client_id`/`split_header`/`encode_response`) to mirror (Task 13). Each step says to adapt to the real API and gives the verification command.
