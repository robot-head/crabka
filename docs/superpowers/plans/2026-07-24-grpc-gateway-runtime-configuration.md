# gRPC Gateway Runtime Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose every gRPC gateway deployment-policy value through validated CLI/environment inputs and the `KafkaGrpcGateway` CRD while keeping wire, correctness, and instrumentation invariants fixed.

**Architecture:** `GatewayConfig` remains the runtime owner. One nested `GatewayRuntimeConfig` carries common topic, polling, readiness, and Schema Registry cache policy; existing TLS, bearer, webhook, outbound, and dedup structs keep their fields. The operator renders typed CRD fields through the gateway's existing flags and TOML Secrets.

**Tech Stack:** Rust 2024, `refined_type` 0.6, Clap, Serde/TOML, kube/schemars CRDs, Cargo nextest.

## Global Constraints

- Preserve distinct existing direct and operator defaults; do not silently normalize their current differences.
- Configure only deployment policy. Keep Kafka/gRPC/HTTP codes, membership partition count one, cleanup-policy semantics, framing/hash/varint constants, sentinels, derived capacities, and metrics buckets fixed.
- New validated scalar inputs use `refined_type`; never use `unsafe_new`.
- Every process setting has a Clap flag backed by `CRABKA_GATEWAY_*`.
- Every operator-managed process setting has a typed CRD field validated before child rendering.
- Reject invalid values; remove `.max(1)` policy clamps instead of silently rewriting input.
- Use existing config/CRD/TOML paths; no global config crate, factories, or generic maps.
- Tests use `assert2`; add no Clippy suppressions.

## Runtime Field Table

| Field | Type | Default | Constraint |
|---|---:|---:|---|
| `internal_topic_replication_factor` | `i16` | `3` | `>= 1` |
| `internal_topic_allow_replication_fallback` | bool | `true` | boolean |
| `internal_topic_create_timeout_ms` | `i32` | `10000` | `>= 1` |
| `internal_topic_segment_ms` | `i64` | `60000` | `>= 1` |
| `internal_topic_min_cleanable_dirty_ratio_basis_points` | `u32` | `100` | `0..=10000` |
| `consumer_poll_timeout_ms` | `u64` | `500` | `>= 1` |
| `ownership_warmup_empty_polls` | `u32` | `2` | `>= 1` |
| `readiness_poll_interval_ms` | `u64` | `250` | `>= 1` |
| `schema_registry_latest_cache_ttl_ms` | `u64` | `5000` | `>= 1` |
| `schema_registry_frame_raw` | bool | `false` | boolean |

Existing fields gaining missing surfaces:

- `bearer_allowable_clock_skew_ms`: `i64`, default `30000`, `>= 0`.
- `dedup_ownership_group`: string, direct default remains derived from client id; CRD default remains gateway-derived.
- `membership_topic`: existing direct flag gains a top-level CRD field.
- `schema_registry_url`: existing direct flag gains `spec.schemaRegistry.url`.
- `tls_reload_secs`: existing direct flag gains `spec.tls.reloadIntervalSecs`.
- inbound webhook `schema_subject`/`schema_format` and outbound `group_id`/`decode_to_json` gain CRD fields and TOML rendering.

Preserve effective defaults: direct dedup window `3_600_000`, ACL refresh 30 seconds, authz off, TLS client auth disabled; operator dedup window `86_400_000`, ACL refresh 60 seconds, authz simple, TLS client auth required.

---

### Task 1: Add Validated Gateway Runtime Inputs

**Files:**

- Modify: `crates/grpc-gateway/Cargo.toml`
- Create: `crates/grpc-gateway/src/config_value.rs`
- Modify: `crates/grpc-gateway/src/lib.rs`
- Modify: `crates/grpc-gateway/src/config.rs`
- Modify: `crates/grpc-gateway/src/bin/gateway.rs`
- Modify: `crates/grpc-gateway/src/outbound_config.rs`
- Test: colocated modules.

**Interfaces:**

- Produces: local refined wrappers, `GatewayRuntimeConfig`, and validated existing CLI values.
- Consumes: `refined_type` integer rules.

- [ ] **Step 1: Write failing scalar/default/relationship tests**

Require exact defaults and boundaries:

```rust
#[test]
fn runtime_defaults_and_boundaries() {
    assert2::assert!(
        GatewayRuntimeConfig::default()
            == GatewayRuntimeConfig {
                internal_topic_replication_factor: 3,
                internal_topic_allow_replication_fallback: true,
                internal_topic_create_timeout_ms: 10_000,
                internal_topic_segment_ms: 60_000,
                internal_topic_min_cleanable_dirty_ratio_basis_points: 100,
                consumer_poll_timeout_ms: 500,
                ownership_warmup_empty_polls: 2,
                readiness_poll_interval_ms: 250,
                schema_registry_latest_cache_ttl_ms: 5_000,
                schema_registry_frame_raw: false,
            }
    );
    assert2::check!(PositiveU64::new(0).is_err());
    assert2::check!(PositiveU64::new(1).is_ok());
    assert2::check!(DirtyRatioBasisPoints::new(10_001).is_err());
    assert2::check!(DirtyRatioBasisPoints::new(10_000).is_ok());
}
```

Add tests proving dedup partitions reject zero and values above `i32::MAX`, dedup window/TLS reload/ACL refresh/attempts/backoffs/timeouts reject zero, bearer skew accepts zero but rejects negative, and outbound `max_backoff_ms < base_backoff_ms` is rejected rather than clamped.

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p crabka-grpc-gateway runtime_defaults_and_boundaries
cargo test -p crabka-grpc-gateway refined_
cargo test -p crabka-grpc-gateway outbound_config
```

Expected: compilation/test failures because the refined inputs and strict validation do not exist.

- [ ] **Step 3: Implement the minimum local refined wrappers**

Expose wrappers backed by `refined_type` rules:

```rust
pub struct PositiveU64(u64);       // GreaterU64<0>
pub struct PositiveI64(i64);       // GreaterI64<0>
pub struct PositiveI32(i32);       // GreaterI32<0>
pub struct PositiveI16(i16);       // GreaterI16<0>
pub struct PositiveU32(u32);       // GreaterU32<0>
pub struct NonNegativeI64(i64);    // GreaterEqualI64<0>
pub struct PartitionCount(u32);    // MinMaxU32<1, { i32::MAX as u32 }>
pub struct DirtyRatioBasisPoints(u32); // MinMaxU32<0, 10_000>
```

Each has checked `new`, `into_value`, and `FromStr`. If `refined_type` names a rule differently, use the crate's built-in equivalent; do not add a custom rule for integers.

- [ ] **Step 4: Add `GatewayRuntimeConfig` and strict existing validation**

Add the field table to a `Default + Clone + PartialEq + Eq` struct and place `pub runtime: GatewayRuntimeConfig` on `GatewayConfig`.

Replace raw Clap fields with refined wrappers at trust boundaries. Change outbound compilation to return an error for zero values and for `max_backoff_ms < base_backoff_ms`; remove all `.max(1)` clamps. Keep booleans unwrapped.

- [ ] **Step 5: Add CLI/environment fields and precedence tests**

Add exact kebab-case flags with matching `CRABKA_GATEWAY_<UPPER_FIELD>` env names. Example:

```rust
#[arg(long, env = "CRABKA_GATEWAY_CONSUMER_POLL_TIMEOUT_MS")]
consumer_poll_timeout_ms: Option<PositiveU64>,
```

Add `--dedup-ownership-group`, `--bearer-allowable-clock-skew-ms`, and `--schema-registry-frame-raw`. Build config from `Default` plus overrides exactly once.

One table-driven test proves defaults, zero/range rejection, environment override, and CLI-over-env precedence.

- [ ] **Step 6: Run focused checks and commit**

Run:

```bash
cargo test -p crabka-grpc-gateway config_value
cargo test -p crabka-grpc-gateway --bin crabka-grpc-gateway
cargo test -p crabka-grpc-gateway outbound_config
cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings
```

Commit:

```bash
git commit -m "feat(gateway): add runtime inputs"
```

---

### Task 2: Route Gateway Runtime Policy to Production

**Files:**

- Modify: `crates/grpc-gateway/src/bin/gateway.rs`
- Modify: `crates/grpc-gateway/src/dedup/topic.rs`
- Modify: `crates/grpc-gateway/src/dedup/membership.rs`
- Modify: `crates/grpc-gateway/src/dedup/store.rs`
- Modify: `crates/grpc-gateway/src/outbound.rs`
- Modify: `crates/grpc-gateway/src/streaming.rs`
- Modify: `crates/grpc-gateway/src/schema/client.rs`
- Modify: `crates/grpc-gateway/src/schema/codec.rs`
- Modify: affected integration-test constructors/signatures.

**Interfaces:**

- Consumes: `GatewayConfig.runtime`.
- Produces: one runtime source for internal topics, polling, readiness, and Schema Registry caching/framing.

- [ ] **Step 1: Write behavior tests that distinguish configured policy**

Add production-used helpers/tests requiring:

```rust
assert2::assert!(
    internal_topic_policy(&runtime)
        == InternalTopicPolicy {
            replication_factor: 2,
            allow_replication_fallback: false,
            create_timeout_ms: 7_000,
            segment_ms: 22_000,
            min_cleanable_dirty_ratio: "0.025".into(),
        }
);
assert2::assert!(ownership_is_warm(2, 3) == false);
assert2::assert!(ownership_is_warm(3, 3) == true);
```

Use a Schema Registry mock to prove a short configured TTL refetches while a long configured TTL retains the cached latest schema. Test explicit `frame_raw=true` reaches codec construction.

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p crabka-grpc-gateway internal_topic_policy
cargo test -p crabka-grpc-gateway ownership_is_warm
cargo test -p crabka-grpc-gateway schema_registry_cache
```

Expected: compilation fails until helpers and production wiring exist.

- [ ] **Step 3: Configure internal topic creation**

Pass `InternalTopicPolicy` into both ensure functions. Render:

```rust
let ratio = f64::from(policy.min_cleanable_dirty_ratio_basis_points) / 10_000.0;
configs.insert("min.cleanable.dirty.ratio".into(), ratio.to_string());
configs.insert("segment.ms".into(), policy.segment_ms.to_string());
```

Pass configured timeout to `create_topics`. Retry with RF 1 only when `allow_replication_fallback && rf > 1`; otherwise return the broker error. Delete both RF constants and the topic `10_000`, `60_000`, `0.01`, and unconditional fallback policy.

- [ ] **Step 4: Configure all polling and warmup consumers**

Copy `consumer_poll_timeout` into membership, ownership, outbound, and streaming tasks before spawn and use it at all four current 500ms poll sites. Replace the hardcoded warm threshold with `ownership_warmup_empty_polls`. Use `readiness_poll_interval` in the readiness watcher.

One config field intentionally drives the four consumer poll sites because they implement the same gateway-wide broker poll policy.

- [ ] **Step 5: Configure Schema Registry client behavior**

Add `latest_cache_ttl: Duration` to `SchemaRegistryClient`; change `new` to accept it or add `new_with_policy` used by production. Remove `LATEST_TTL`. Pass `schema_registry_frame_raw` into `SchemaRegistryCodec` instead of the literal `false`.

- [ ] **Step 6: Run focused checks and commit**

Run:

```bash
cargo test -p crabka-grpc-gateway dedup
cargo test -p crabka-grpc-gateway outbound
cargo test -p crabka-grpc-gateway streaming
cargo test -p crabka-grpc-gateway schema
cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings
```

Commit:

```bash
git commit -m "refactor(gateway): use runtime policy"
```

---

### Task 3: Complete the Gateway CRD Surface

**Files:**

- Modify: `crates/operator/src/crd/grpc_gateway.rs`
- Modify: `crates/operator/src/controller/grpc_gateway.rs`
- Modify: `crates/operator/tests/reconcile_gateway.rs`
- Modify: `deploy/crds/crabka.io_kafkagrpcgateways.yaml`

**Interfaces:**

- Produces: `KafkaGrpcGatewaySpec.tuning`, `schemaRegistry`, and missing existing fields.
- Consumes: Task 1 flags and existing webhook/outbound TOML formats.

- [ ] **Step 1: Write failing deployment/TOML/validation tests**

Construct a CRD with every runtime value nondefault and assert the Deployment contains every exact flag. Also assert:

- `membershipTopic`, Schema Registry URL/cache TTL/frame mode, TLS reload, bearer skew, and dedup ownership group render.
- inbound webhook schema subject/format render into `webhooks.toml`.
- outbound group id/decode-to-JSON render into `outbound.toml`.
- invalid scalar/range/backoff relation yields `Ready=False`, reason `GatewayConfigInvalid`, before Deployment rendering.
- omitted fields preserve the current operator defaults, including 24h dedup and 60s ACL refresh.

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p crabka-operator --test reconcile_gateway runtime_
cargo test -p crabka-operator --test reconcile_gateway config_secret_
```

Expected: compilation fails because the CRD fields do not exist.

- [ ] **Step 3: Add the minimal typed CRD structures**

Add:

```rust
pub struct GatewayTuning {
    pub internal_topic_replication_factor: Option<i16>,
    pub internal_topic_allow_replication_fallback: Option<bool>,
    pub internal_topic_create_timeout_ms: Option<i32>,
    pub internal_topic_segment_ms: Option<i64>,
    pub internal_topic_min_cleanable_dirty_ratio_basis_points: Option<u32>,
    pub consumer_poll_timeout_ms: Option<u64>,
    pub ownership_warmup_empty_polls: Option<u32>,
    pub readiness_poll_interval_ms: Option<u64>,
}

pub struct GatewaySchemaRegistrySpec {
    pub url: Option<String>,
    pub latest_cache_ttl_ms: Option<u64>,
    pub frame_raw: Option<bool>,
}
```

Add optional top-level `tuning`, `membership_topic`, and `schema_registry`; extend existing dedup/TLS/bearer/webhook/outbound structs with the fields listed above. Add `schemars` integer ranges matching Task 1.

Correct only inaccurate documentation: absent dedup does not disable the current controller path; outbound defaults are five attempts and 500ms base backoff. Do not change those effective defaults.

- [ ] **Step 4: Validate and render through existing paths**

Use `refined_type` directly in the operator for all integer constraints and an explicit relation check for outbound backoffs. Validate OTLP `sample_ratio` manually as finite and `0.0..=1.0`; float const-generic rules are unavailable. Emit `GatewayConfigInvalid` before rendering children.

Append present tuning values to `gateway_args`, preserving its deterministic sort. Render added webhook/outbound fields in their existing TOML Secrets.

- [ ] **Step 5: Regenerate, test, and commit**

Run:

```bash
cargo test -p crabka-operator --test reconcile_gateway
cargo test -p crabka-operator --lib crd::grpc_gateway
cargo run -p crabka-operator -- gen-crds /tmp/crabka-gateway-crds
diff -u deploy/crds/crabka.io_kafkagrpcgateways.yaml /tmp/crabka-gateway-crds/crabka.io_kafkagrpcgateways.yaml
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Commit:

```bash
git commit -m "feat(operator): expose gateway tuning"
```

---

### Task 4: Close the gRPC Gateway Audit

**Files:**

- Modify: `docs/configuration-audit.md`
- Modify only if a real gap is found: Task 1–3 files.

- [ ] **Step 1: Run the fresh semantic scan**

Run:

```bash
tools/audit-runtime-values.sh | rg '^crates/grpc-gateway/' > /tmp/crabka-gateway-runtime-values.txt
```

Classify every result. Fixed groups must include protocol/error codes, membership partition count one, cleanup policy, framing/hash/varint constants, sentinels, derived capacities, histogram buckets, retry math, and test fixtures. Configure any remaining production policy before continuing.

- [ ] **Step 2: Run completion gates**

Run:

```bash
cargo +nightly fmt --all -- --check
cargo clippy -p crabka-grpc-gateway -p crabka-operator --all-targets -- -D warnings
cargo nextest run -p crabka-grpc-gateway -p crabka-operator
cargo run -p crabka-grpc-gateway -- --help | rg 'internal-topic|consumer-poll|ownership-warmup|schema-registry-latest|bearer-allowable'
cargo run -p crabka-operator -- gen-crds /tmp/crabka-gateway-crds
diff -u deploy/crds/crabka.io_kafkagrpcgateways.yaml /tmp/crabka-gateway-crds/crabka.io_kafkagrpcgateways.yaml
git diff --check
```

- [ ] **Step 3: Record only gateway completion and commit**

Update the ledger with exact counts, classifications, and gate evidence. Do not claim the operator or repository complete.

```bash
git commit -m "docs: close gateway config audit"
```
