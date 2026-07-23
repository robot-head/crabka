# Runtime Configuration Broker Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove broker-owned operational constants and magic values by routing them through validated runtime configuration, CLI/environment inputs, and the Kafka CRD.

**Architecture:** The broker remains the owner of its runtime settings. Raw CLI, environment, file-config, and CRD values are converted through `refined_type` rules before reaching `BrokerConfig`; the operator renders the typed Kafka CRD settings into the existing broker TOML ConfigMap. A checked-in audit ledger distinguishes tunable broker policy from fixed compatibility and invariant values.

**Tech Stack:** Rust 2024, `refined_type` 0.6, Clap, Serde/TOML, kube/schemars CRDs, Cargo nextest.

## Global Constraints

- Configure only values that reasonably tune deployment policy; keep protocol constants, PostgreSQL/Kafka semantics, mathematical conversions, safety invariants, and test-only values fixed.
- Preserve every current value as the default.
- New validated scalar inputs must use `refined_type`; never call `Refined::unsafe_new`.
- Every direct process setting must have a Clap option backed by an environment variable.
- Every operator-managed broker setting must have a typed Kafka CRD field and render through the broker ConfigMap.
- Tests must exercise behavior rather than inspect source text.
- Use `assert2::assert!`, never the standard assertion macros.
- Do not add Clippy suppressions.

---

## Program Boundary

This plan is the broker/operator slice of the repo-wide configuration program. It deliberately does not mix independent service changes into the same review unit. After this plan passes, create separate plans for:

1. Operator and rebalancer process policy.
2. Schema Registry and gRPC gateway policy plus their CRDs.
3. Gres control, range, substrate, activator, and tenant policy plus the Gres CRDs.
4. Metrics, traces, profiles, observability, and demo-service policy.
5. Remaining standalone binaries and library-owned policy reachable from them.

The repository-wide goal is complete only after those plans pass and the final audit ledger has no unclassified production candidates.

## Broker Runtime Field Table

Tasks 3–5 implement every row in this table. Integer constraints are inclusive unless stated otherwise.

| Field | Type | Default | Constraint |
|---|---:|---:|---|
| `startup_leader_wait_timeout_ms` | `u64` | `120000` | `>= 1` |
| `self_registration_backoff_min_ms` | `u64` | `100` | `>= 1`, `<= max` |
| `self_registration_backoff_max_ms` | `u64` | `5000` | `>= min` |
| `observer_poll_interval_ms` | `u64` | `100` | `>= 1` |
| `audit_spool_replay_interval_ms` | `u64` | `2000` | `>= 1` |
| `audit_stats_poll_interval_ms` | `u64` | `1000` | `>= 1` |
| `audit_partition_wait_timeout_ms` | `u64` | `10000` | `>= 1` |
| `liveness_tick_interval_ms` | `u64` | `1000` | `>= 1` |
| `gauge_poll_interval_ms` | `u64` | `1000` | `>= 1` |
| `isr_scan_interval_ms` | `u64` | `1000` | `>= 1` |
| `cleaner_interval_ms` | `u64` | `30000` | `>= 1` |
| `diskless_flush_interval_ms` | `u64` | `250` | `>= 1` |
| `future_log_move_retry_backoff_ms` | `u64` | `50` | `>= 1` |
| `client_metrics_eviction_tick_ms` | `u64` | `60000` | `>= 1` |
| `client_metrics_stale_floor_ms` | `u64` | `600000` | `>= eviction tick` |
| `client_metrics_default_interval_ms` | `i32` | `300000` | `>= 1` |
| `client_metrics_telemetry_max_bytes` | `i32` | `1048576` | `>= 1` |
| `client_metrics_prom_snapshot_ttl_ms` | `u64` | `300000` | `>= 1` |
| `rlmm_reconcile_tick_ms` | `u64` | `30000` | `>= 1` |
| `rlmm_bootstrap_backoff_initial_ms` | `u64` | `250` | `>= 1`, `<= max` |
| `rlmm_bootstrap_backoff_max_ms` | `u64` | `10000` | `>= initial` |
| `connection_creation_throttle_max_ms` | `u64` | `1000` | `>= 1` |
| `opa_http_timeout_ms` | `u64` | `5000` | `>= 1` |
| `oauth_jwks_http_timeout_ms` | `u64` | `10000` | `>= 1` |
| `auto_join_retry_backoff_ms` | `u64` | `500` | `>= 1` |
| `replication_fetch_max_bytes` | `i32` | `1048576` | `>= 1` |
| `replication_fetch_max_wait_ms` | `i32` | `500` | `>= 1` |
| `replication_fetch_min_bytes` | `i32` | `1` | `>= 1`, `<= max bytes` |
| `replication_throttle_exhausted_backoff_ms` | `u64` | `100` | `>= 1` |
| `replication_send_error_backoff_ms` | `u64` | `1000` | `>= 1` |
| `replication_unknown_topic_retry_delay_ms` | `u64` | `100` | `>= 1` |
| `replication_epoch_fence_backoff_ms` | `u64` | `200` | `>= 1` |
| `replication_unexpected_error_backoff_ms` | `u64` | `500` | `>= 1` |
| `replication_reconnect_initial_delay_ms` | `u64` | `100` | `>= 1`, `<= cap` |
| `replication_reconnect_delay_cap_ms` | `u64` | `5000` | `>= initial` |
| `coordinator_session_expiry_tick_ms` | `u64` | `1000` | `>= 1` |
| `coordinator_shutdown_ack_timeout_ms` | `u64` | `5000` | `>= 1` |
| `consumer_group_session_timeout_ms` | `u64` | `45000` | within min/max |
| `consumer_group_heartbeat_interval_ms` | `u64` | `5000` | within min/max |
| `consumer_group_min_session_timeout_ms` | `u64` | `45000` | `>= 1`, `<= max` |
| `consumer_group_max_session_timeout_ms` | `u64` | `60000` | `>= min` |
| `consumer_group_min_heartbeat_interval_ms` | `u64` | `5000` | `>= 1`, `<= max` |
| `consumer_group_max_heartbeat_interval_ms` | `u64` | `15000` | `>= min` |
| `consumer_group_max_size` | `usize` | `200` | `>= 1` |
| `classic_group_initial_rebalance_delay_ms` | `u64` | `3000` | `>= 1` |
| `sync_group_follower_wait_ms` | `u64` | `30000` | `>= 1` |
| `unclean_recovery_aggressive_deadline_ms` | `u64` | `2000` | `>= 1` |
| `unclean_recovery_balanced_deadline_ms` | `u64` | `30000` | `>= aggressive` |
| `operator_recovery_deadline_ms` | `u64` | `25000` | `>= 1` |
| `quota_throttle_max_ms` | `u64` | `1000` | `>= 1` |

The following broker settings already have direct CLI/environment inputs. Task 5 adds their missing typed CRD path; Tasks 1 and 4 replace ad hoc scalar validation with refined parsers where a constraint exists.

| Existing field | Type | Default | Constraint |
|---|---:|---:|---|
| `partition_disk_scan_interval_secs` | `u64` | `60` | any value; `0` disables |
| `observer_lag_bound` | `u64` | `100` | any value |
| `heartbeat_interval_ms` | `u64` | `3000` | `>= 1`, below timeout |
| `heartbeat_timeout_ms` | `u64` | `9000` | above interval |
| `replica_lag_time_max_ms` | `u64` | `30000` | `>= 1` |
| `controller_election_timeout_ms` | `u64` | `5000` | `>= 1`, above heartbeat |
| `controller_heartbeat_interval_ms` | `u64` | `500` | `>= 1`, below election timeout |
| `controlled_shutdown_drain_timeout_ms` | `u64` | `20000` | `>= 1` |
| `metadata_max_bytes_between_snapshots` | `u64` | `20971520` | `>= 1` |
| `metadata_max_snapshot_interval_ms` | `u64` | `3600000` | any value; `0` disables |
| `metadata_snapshot_interval_records` | `u64` | `10000` | `>= 1` |
| `txn_abort_cleanup_interval_ms` | `u64` | `10000` | any value; `0` disables |
| `leader_imbalance_check_interval_secs` | `u64` | `300` | `>= 1` |
| `leader_imbalance_per_broker_percentage` | `u32` | `10` | `0..=100` |
| `tls_reload_interval_ms` | `u64` | `30000` | any value; `0` disables |
| `max_incremental_fetch_session_cache_slots` | `usize` | `1000` | any value; `0` disables caching |
| `max_connections` | `usize` | `usize::MAX` | any value; `0` rejects connections |
| `max_connections_per_ip` | `usize` | `usize::MAX` | any value; `0` rejects connections |
| `delegation_token_max_lifetime_ms` | `i64` | `604800000` | `>= 1` |
| `delegation_token_expiry_check_interval_ms` | `i64` | `3600000` | `>= 1` |
| `delegation_token_default_renew_period_ms` | `i64` | `86400000` | `>= 1`, at most max lifetime |
| `remote_log_manager_interval_ms` | `u64` | `30000` | `>= 1` |

## File Structure

- `crates/broker/src/config_value.rs`: broker input aliases and parsers backed by `refined_type`.
- `crates/broker/src/config.rs`: authoritative broker runtime fields, defaults, and cross-field validation.
- `crates/broker/src/bin/broker.rs`: direct CLI/environment surface.
- `crates/broker/src/file_config.rs`: operator-managed TOML input and conversion.
- `crates/broker/src/{broker,auto_join,cleaner,future_log,isr_maintenance,oauth_jwks,replicator}.rs`: consume configuration instead of local policy constants.
- `crates/broker/src/authorizer/opa.rs`: consume configured OPA timeout.
- `crates/broker/src/client_metrics/{config,mod}.rs`: consume configured telemetry limits and maintenance intervals.
- `crates/broker/src/coordinator/unified/{actor,classic_ops,config,mod}.rs`: consume configured coordinator policy.
- `crates/operator/src/crd/kafka.rs`: typed optional `brokerTuning` CRD surface.
- `crates/operator/src/controller/{common,listeners}.rs`: validate and render `brokerTuning`.
- `deploy/crds/crabka.io_kafkas.yaml`: generated CRD schema.
- `docs/configuration-audit.md`: completion ledger.
- `tools/audit-runtime-values.sh`: reproducible candidate scan.

---

### Task 1: Add Refined Broker Input Types

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/broker/Cargo.toml`
- Create: `crates/broker/src/config_value.rs`
- Modify: `crates/broker/src/lib.rs`

**Interfaces:**

- Produces: `PositiveMillis`, `PositiveI32`, `PositiveI64`, `PositiveCount`, `Percentage`, and the `parse_*` Clap value parsers.
- Consumes: `refined_type::rule::{GreaterI32, GreaterI64, GreaterU64, GreaterUsize, MinMaxU32}`.

- [ ] **Step 1: Write failing boundary tests**

Create `config_value.rs` with a test module that specifies accepted and rejected values:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn refined_scalar_boundaries() {
        assert!(parse_positive_millis("1").is_ok());
        assert!(parse_positive_millis("0").is_err());
        assert!(parse_positive_i32("1").is_ok());
        assert!(parse_positive_i32("0").is_err());
        assert!(parse_positive_i64("1").is_ok());
        assert!(parse_positive_i64("-1").is_err());
        assert!(parse_positive_count("1").is_ok());
        assert!(parse_positive_count("0").is_err());
        assert!(parse_percentage("0").is_ok());
        assert!(parse_percentage("100").is_ok());
        assert!(parse_percentage("101").is_err());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p crabka-broker config_value::tests::refined_scalar_boundaries
```

Expected: compilation fails because the module, dependency, and parser functions are not implemented.

- [ ] **Step 3: Add the dependency and minimal refined aliases**

Add this workspace dependency:

```toml
refined_type = "0.6"
```

Add `refined_type.workspace = true` to `crates/broker/Cargo.toml`.

Implement:

```rust
use std::str::FromStr;

use refined_type::rule::{GreaterI32, GreaterI64, GreaterU64, GreaterUsize, MinMaxU32};

pub type PositiveMillis = GreaterU64<0>;
pub type PositiveI32 = GreaterI32<0>;
pub type PositiveI64 = GreaterI64<0>;
pub type PositiveCount = GreaterUsize<0>;
pub type Percentage = MinMaxU32<0, 100>;

pub fn parse_positive_millis(value: &str) -> Result<PositiveMillis, String> {
    value
        .parse::<u64>()
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveMillis::new(value).map_err(|error| error.to_string()))
}

pub fn parse_positive_i32(value: &str) -> Result<PositiveI32, String> {
    value
        .parse::<i32>()
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveI32::new(value).map_err(|error| error.to_string()))
}

pub fn parse_positive_i64(value: &str) -> Result<PositiveI64, String> {
    value
        .parse::<i64>()
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveI64::new(value).map_err(|error| error.to_string()))
}

pub fn parse_positive_count(value: &str) -> Result<PositiveCount, String> {
    usize::from_str(value)
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveCount::new(value).map_err(|error| error.to_string()))
}

pub fn parse_percentage(value: &str) -> Result<Percentage, String> {
    value
        .parse::<u32>()
        .map_err(|error| error.to_string())
        .and_then(|value| Percentage::new(value).map_err(|error| error.to_string()))
}
```

Export the module from `lib.rs`:

```rust
pub mod config_value;
```

- [ ] **Step 4: Run focused validation**

Run:

```bash
cargo test -p crabka-broker config_value::tests::refined_scalar_boundaries
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: both commands pass. Remove any unused import rather than suppressing the warning.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/broker/Cargo.toml crates/broker/src/config_value.rs crates/broker/src/lib.rs
git commit -m "feat(broker): add refined config inputs"
```

---

### Task 2: Establish the Runtime-Value Audit Ledger

**Files:**

- Create: `tools/audit-runtime-values.sh`
- Create: `docs/configuration-audit.md`

**Interfaces:**

- Produces: a deterministic production-source candidate list grouped by crate.
- Consumes: `rg`, already required by contributor tooling.

- [ ] **Step 1: Add the candidate scanner**

Create this executable script:

```bash
#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
cd "$root"

rg -n \
  --glob '*.rs' \
  --glob '!**/tests/**' \
  --glob '!**/benches/**' \
  --glob '!**/*_model.rs' \
  --glob '!**/test_*.rs' \
  '(^[[:space:]]*(pub([[:space:]]*\\([^)]*\\))?[[:space:]]+)?const[[:space:]]+[A-Z][A-Z0-9_]*|Duration::from_(secs|millis|micros|nanos|mins)\\([0-9_]+\\)|with_capacity\\([0-9_]+\\))' \
  crates \
  | sort
```

Mark it executable:

```bash
chmod +x tools/audit-runtime-values.sh
```

- [ ] **Step 2: Run the scanner and capture the broker candidates**

Run:

```bash
tools/audit-runtime-values.sh | rg '^crates/broker/' > /tmp/crabka-broker-runtime-values.txt
wc -l /tmp/crabka-broker-runtime-values.txt
```

Expected: the count is nonzero and includes `STARTUP_LEADER_WAIT_TIMEOUT`, `AUTO_JOIN` retry policy, replicator policy, cleaner cadence, coordinator cadence, and protocol-code constants.

- [ ] **Step 3: Create the ledger with the complete broker classifications**

Start `docs/configuration-audit.md` with these configured groups:

```markdown
# Runtime Configuration Audit

The audit follows the scope in the runtime-configuration design. Paths and line numbers are refreshed before completion; names are stable identifiers.

## Broker

### Configurable

- Startup and registration: `STARTUP_LEADER_WAIT_TIMEOUT`, `SELF_REGISTRATION_BACKOFF_MIN`, `SELF_REGISTRATION_BACKOFF_MAX`, `OBSERVER_POLL_INTERVAL`.
- Audit maintenance: `AUDIT_SPOOL_REPLAY_INTERVAL`, `AUDIT_STATS_POLL_INTERVAL`, `AUDIT_PARTITION_WAIT_TIMEOUT`.
- Broker maintenance: `LIVENESS_TICK_INTERVAL`, `GAUGE_POLL_INTERVAL`, `ISR_SCAN_INTERVAL`, `DEFAULT_COMPACTION_INTERVAL`, `FLUSH_INTERVAL`, `MOVE_RETRY_BACKOFF`.
- Client metrics: `CLIENT_METRICS_EVICTION_TICK`, `CLIENT_METRICS_STALE_FLOOR`, `DEFAULT_TELEMETRY_MAX_BYTES`, `PROM_SNAPSHOT_TTL`.
- Remote log metadata: `RLMM_RECONCILE_TICK`, `RLMM_BOOTSTRAP_BACKOFF_INITIAL`, `RLMM_BOOTSTRAP_BACKOFF_MAX`.
- Network and auth: `CONNECTION_CREATION_THROTTLE_MAX`, `OPA_HTTP_TIMEOUT`, JWKS HTTP timeout.
- Auto-join and replication: `RETRY_BACKOFF`, `FETCH_MAX_BYTES`, `FETCH_MAX_WAIT_MS`, `FETCH_MIN_BYTES`, `THROTTLE_EXHAUSTED_BACKOFF`, `SEND_ERROR_BACKOFF`, `UNKNOWN_TOPIC_RETRY_DELAY`, `EPOCH_FENCE_BACKOFF`, `UNEXPECTED_ERROR_BACKOFF`, `RECONNECT_INITIAL_DELAY`, `RECONNECT_DELAY_CAP`.
- Coordinators: `SESSION_EXPIRY_TICK_INTERVAL`, `SHUTDOWN_ACK_TIMEOUT`, `DEFAULT_SESSION_TIMEOUT`, `DEFAULT_HEARTBEAT_INTERVAL`, `DEFAULT_MIN_SESSION_TIMEOUT`, `DEFAULT_MAX_SESSION_TIMEOUT`, `DEFAULT_MIN_HEARTBEAT_INTERVAL`, `DEFAULT_MAX_HEARTBEAT_INTERVAL`, `DEFAULT_MAX_GROUP_SIZE`, `INITIAL_REBALANCE_DELAY`, `FOLLOWER_WAIT`.
- Recovery and quota policy: `AGGRESSIVE_DEADLINE`, `BALANCED_DEADLINE`, `OPERATOR_RECOVERY_DEADLINE`, maximum quota throttle delay.

### Fixed

- `codes.rs` error codes: Kafka wire compatibility.
- `raft_handshake.rs` API keys, versions, and header sizes: Kafka wire compatibility.
- `wal/quorum/wire.rs` metadata topic id, API version, and error codes: KRaft wire compatibility.
- State-file names and probe filenames: persisted-format identifiers, not tuning.
- Metrics histogram bucket arrays: exported metric schema; changing them breaks time-series continuity.
- Model-checking constants and all values under test-only modules: verification inputs.
- Epoch sentinels, bit masks, record markers, and fixed collection sizes derived from protocol shapes: invariants.
```

For every remaining line in `/tmp/crabka-broker-runtime-values.txt`, add it to one of these groups before continuing. Do not leave an unclassified broker candidate.

- [ ] **Step 4: Verify ledger coverage**

Run:

```bash
tools/audit-runtime-values.sh | rg '^crates/broker/' | cut -d: -f3- | sed -n '1,20p'
rg -n 'TODO|TBD|unclassified' docs/configuration-audit.md
```

Expected: the candidate command prints representative source expressions; the placeholder scan prints nothing.

- [ ] **Step 5: Commit**

```bash
git add tools/audit-runtime-values.sh docs/configuration-audit.md
git commit -m "docs: audit broker runtime values"
```

---

### Task 3: Move Broker Policy into `BrokerConfig`

**Files:**

- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/broker.rs`
- Modify: `crates/broker/src/auto_join.rs`
- Modify: `crates/broker/src/cleaner.rs`
- Modify: `crates/broker/src/future_log.rs`
- Modify: `crates/broker/src/isr_maintenance.rs`
- Modify: `crates/broker/src/oauth_jwks.rs`
- Modify: `crates/broker/src/replicator.rs`
- Modify: `crates/broker/src/authorizer/opa.rs`
- Modify: `crates/broker/src/client_metrics/config.rs`
- Modify: `crates/broker/src/client_metrics/mod.rs`
- Modify: `crates/broker/src/coordinator/unified/actor.rs`
- Modify: `crates/broker/src/coordinator/unified/classic_ops.rs`
- Modify: `crates/broker/src/coordinator/unified/config.rs`
- Modify: `crates/broker/src/coordinator/unified/mod.rs`
- Modify: `crates/broker/src/handlers/elect_leaders.rs`
- Modify: `crates/broker/src/handlers/sync_group.rs`
- Test: colocated `#[cfg(test)]` modules in the modified files.

**Interfaces:**

- Produces: one authoritative `BrokerConfig` value for every ledger entry classified as configurable.
- Consumes: validated raw values supplied by Tasks 1, 4, and 5.

- [ ] **Step 1: Add a failing whole-default test**

Extend the config tests with one whole-value policy snapshot:

```rust
#[test]
fn operational_policy_defaults_match_existing_behavior() {
    let config = BrokerConfig::default();
    let actual = (
        config.startup_leader_wait_timeout,
        config.self_registration_backoff_min,
        config.self_registration_backoff_max,
        config.observer_poll_interval,
        config.cleaner_interval,
        config.isr_scan_interval,
        config.opa_http_timeout,
        config.replication.fetch_max_bytes,
        config.replication.fetch_max_wait_ms,
        config.replication.fetch_min_bytes,
    );
    let expected = (
        Duration::from_mins(2),
        Duration::from_millis(100),
        Duration::from_secs(5),
        Duration::from_millis(100),
        Duration::from_secs(30),
        Duration::from_secs(1),
        Duration::from_secs(5),
        1 << 20,
        500,
        1,
    );
    assert!(actual == expected);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p crabka-broker operational_policy_defaults_match_existing_behavior
```

Expected: compilation fails because the fields and nested replication config do not yet exist.

- [ ] **Step 3: Add focused config groups**

Add plain data structs rather than a builder or trait hierarchy:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationRuntimeConfig {
    pub fetch_max_bytes: i32,
    pub fetch_max_wait_ms: i32,
    pub fetch_min_bytes: i32,
    pub throttle_exhausted_backoff: Duration,
    pub send_error_backoff: Duration,
    pub unknown_topic_retry_delay: Duration,
    pub epoch_fence_backoff: Duration,
    pub unexpected_error_backoff: Duration,
    pub reconnect_initial_delay: Duration,
    pub reconnect_delay_cap: Duration,
}
```

Add scalar `BrokerConfig` fields for every non-replication row in the Broker Runtime Field Table. Use the table's exact defaults. Keep zero-disables semantics only where they already exist.

Do not add configuration for fixed ledger entries.

- [ ] **Step 4: Add cross-field validation**

Extend `BrokerConfig::validate` with:

```rust
if self.self_registration_backoff_min > self.self_registration_backoff_max {
    return Err(BrokerError::InvalidRuntimeConfig(
        "self registration minimum backoff exceeds maximum".into(),
    ));
}
if self.replication.reconnect_initial_delay > self.replication.reconnect_delay_cap {
    return Err(BrokerError::InvalidRuntimeConfig(
        "replication reconnect initial delay exceeds cap".into(),
    ));
}
if self.controller_heartbeat_interval >= self.controller_election_timeout {
    return Err(BrokerError::InvalidRuntimeConfig(
        "controller heartbeat interval must be below election timeout".into(),
    ));
}
```

Add `BrokerError::InvalidRuntimeConfig(String)` beside existing configuration errors.

- [ ] **Step 5: Replace local constants at their common consumers**

Pass the config value into the owning task or component. Delete the local operational constants after the final caller moves.

For replication, change construction to accept `ReplicationRuntimeConfig` once and read all fetch and retry policy from it. Do not add environment reads inside `replicator.rs`.

For coordinator config structs that already exist, replace their default literals with values copied from `BrokerConfig` at startup rather than creating duplicate fields.

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test -p crabka-broker operational_policy_defaults_match_existing_behavior
cargo test -p crabka-broker config
cargo test -p crabka-broker replicator
cargo test -p crabka-broker coordinator
```

Expected: all tests pass, and the default snapshot equals the pre-change values.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src
git commit -m "refactor(broker): centralize runtime policy"
```

---

### Task 4: Expose Direct CLI, Environment, and File Configuration

**Files:**

- Modify: `crates/broker/src/bin/broker.rs`
- Modify: `crates/broker/src/file_config.rs`

**Interfaces:**

- Produces: direct `--<name>` plus `CRABKA_<NAME>` inputs and `[runtime]` TOML inputs.
- Consumes: refined parser functions from Task 1 and `BrokerConfig` fields from Task 3.

- [ ] **Step 1: Add failing CLI parsing tests**

Add a table-driven test using Clap's `try_parse_from`:

```rust
#[test]
fn runtime_policy_cli_rejects_invalid_and_accepts_valid_values() {
    let cases = [
        (vec!["bin", "--cleaner-interval-ms=0"], false),
        (vec!["bin", "--cleaner-interval-ms=1"], true),
        (vec!["bin", "--replication-fetch-min-bytes=0"], false),
        (vec!["bin", "--replication-fetch-min-bytes=1"], true),
        (vec!["bin", "--leader-imbalance-per-broker-percentage=101"], false),
        (vec!["bin", "--leader-imbalance-per-broker-percentage=100"], true),
    ];

    for (args, accepted) in cases {
        assert!(Args::try_parse_from(args).is_ok() == accepted);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p crabka-broker runtime_policy_cli_rejects_invalid_and_accepts_valid_values
```

Expected: the new option names are unknown.

- [ ] **Step 3: Add CLI/environment options**

Add one field per configurable ledger entry. Follow this exact pattern:

```rust
#[arg(
    long,
    env = "CRABKA_CLEANER_INTERVAL_MS",
    default_value = "30000",
    value_parser = crabka_broker::config_value::parse_positive_millis
)]
cleaner_interval_ms: crabka_broker::config_value::PositiveMillis,
```

Use the positive refined parsers for nonzero milliseconds, counts, signed fetch values, delegation-token durations, and byte sizes. Use the percentage parser for bounded percentages.

Map every refined value with `into_value()` exactly once while constructing `BrokerConfig`.

- [ ] **Step 4: Add the operator TOML section**

Add a Serde input struct:

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFileConfig {
    pub cleaner_interval_ms: Option<u64>,
    pub isr_scan_interval_ms: Option<u64>,
    pub opa_http_timeout_ms: Option<u64>,
    pub replication_fetch_max_bytes: Option<i32>,
    pub replication_fetch_max_wait_ms: Option<i32>,
    pub replication_fetch_min_bytes: Option<i32>,
}
```

Include every Broker Runtime Field Table row in `RuntimeFileConfig` using the table's snake-case name and primitive type. Validate each present scalar through the Task 1 refined type before applying it. Validate the table's relational constraints through `BrokerConfig::validate`.

- [ ] **Step 5: Add file-config behavior tests**

Test a complete representative TOML:

```toml
[runtime]
cleaner_interval_ms = 7000
isr_scan_interval_ms = 800
opa_http_timeout_ms = 2500
replication_fetch_max_bytes = 2097152
replication_fetch_max_wait_ms = 750
replication_fetch_min_bytes = 2
```

Compare the resulting config tuple against the exact durations and integers. Add one invalid TOML case with `cleaner_interval_ms = 0` and assert the error names that field.

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test -p crabka-broker --bin crabka-broker runtime_policy_cli
cargo test -p crabka-broker file_config
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/bin/broker.rs crates/broker/src/file_config.rs
git commit -m "feat(broker): expose runtime policy inputs"
```

---

### Task 5: Add Kafka CRD Broker Tuning

**Files:**

- Modify: `crates/operator/Cargo.toml`
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/controller/common.rs`
- Modify: `crates/operator/src/controller/listeners.rs`
- Modify: `crates/operator/tests/reconcile_kafka.rs`
- Modify: `deploy/crds/crabka.io_kafkas.yaml`

**Interfaces:**

- Produces: `Kafka.spec.brokerTuning` and the matching `[runtime]` broker TOML.
- Consumes: broker runtime field names and constraints from Task 4.

- [ ] **Step 1: Add a failing render test**

Construct a Kafka resource with:

```rust
broker_tuning: Some(BrokerTuning {
    cleaner_interval_ms: Some(7_000),
    isr_scan_interval_ms: Some(800),
    opa_http_timeout_ms: Some(2_500),
    replication_fetch_max_bytes: Some(2_097_152),
    replication_fetch_max_wait_ms: Some(750),
    replication_fetch_min_bytes: Some(2),
    ..BrokerTuning::default()
}),
```

Assert that the reconciled broker ConfigMap contains:

```toml
[runtime]
cleaner_interval_ms = 7000
isr_scan_interval_ms = 800
opa_http_timeout_ms = 2500
replication_fetch_max_bytes = 2097152
replication_fetch_max_wait_ms = 750
replication_fetch_min_bytes = 2
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p crabka-operator --test reconcile_kafka broker_tuning
```

Expected: compilation fails because `broker_tuning` and `BrokerTuning` do not exist.

- [ ] **Step 3: Add the typed CRD struct**

Add:

```rust
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrokerTuning {
    #[schemars(range(min = 1))]
    pub cleaner_interval_ms: Option<u64>,
    #[schemars(range(min = 1))]
    pub isr_scan_interval_ms: Option<u64>,
    #[schemars(range(min = 1))]
    pub opa_http_timeout_ms: Option<u64>,
    #[schemars(range(min = 1))]
    pub replication_fetch_max_bytes: Option<i32>,
    #[schemars(range(min = 1))]
    pub replication_fetch_max_wait_ms: Option<i32>,
    #[schemars(range(min = 1))]
    pub replication_fetch_min_bytes: Option<i32>,
}
```

Add every Broker Runtime Field Table row and every Existing Field Table row to `BrokerTuning` with the corresponding primitive type and `schemars` minimum or range. Add to `KafkaSpec`:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub broker_tuning: Option<BrokerTuning>,
```

- [ ] **Step 4: Validate CRD values with `refined_type`**

Add `refined_type.workspace = true` to the operator and validate each present field before rendering. Use the same `Greater*` or `MinMax*` rule as the broker input parser. Return a `KafkaConfigInvalid` condition whose message contains the camel-case CRD path.

Do not depend on `crabka-broker` from production operator code merely to share aliases; use `refined_type` directly and keep the service-specific field ownership in the broker.

- [ ] **Step 5: Render `[runtime]` deterministically**

Write fields in declaration order and omit the whole section when `broker_tuning` is absent or all fields are `None`. Keep values numeric in TOML.

Add an invalid reconciliation test for a zero interval and a cross-field test where an initial backoff exceeds its cap.

- [ ] **Step 6: Regenerate and inspect CRDs**

Run:

```bash
tools/regen-crds.sh
git diff --check
rg -n 'brokerTuning|cleanerIntervalMs|minimum: 1' deploy/crds/crabka.io_kafkas.yaml
```

Expected: generated schema contains `brokerTuning`, representative fields, and numeric minimum constraints.

- [ ] **Step 7: Run operator tests**

Run:

```bash
cargo test -p crabka-operator --lib crd
cargo test -p crabka-operator --test reconcile_kafka
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/operator/Cargo.toml crates/operator/src/crd/kafka.rs crates/operator/src/controller/common.rs crates/operator/src/controller/listeners.rs crates/operator/tests/reconcile_kafka.rs deploy/crds/crabka.io_kafkas.yaml Cargo.lock
git commit -m "feat(operator): expose broker tuning"
```

---

### Task 6: Verify the Broker Slice and Close Its Audit

**Files:**

- Modify: `docs/configuration-audit.md`
- Modify only if verification finds a real gap: files from Tasks 1–5.

**Interfaces:**

- Produces: evidence that the broker slice has no unclassified operational candidates.
- Consumes: scanner and ledger from Task 2.

- [ ] **Step 1: Run formatting and static checks**

Run:

```bash
cargo +nightly fmt --all -- --check
cargo clippy -p crabka-broker -p crabka-operator --all-targets -- -D warnings
git diff --check
```

Expected: all commands pass.

- [ ] **Step 2: Run broker and operator tests**

Run:

```bash
cargo nextest run -p crabka-broker -p crabka-operator
```

Expected: all tests pass.

- [ ] **Step 3: Re-run the broker audit**

Run:

```bash
tools/audit-runtime-values.sh | rg '^crates/broker/' > /tmp/crabka-broker-runtime-values-final.txt
```

Review every line against `docs/configuration-audit.md`. Any operational value still consumed directly is incomplete work; configure it before continuing. Any newly discovered fixed value gets a concrete exclusion reason.

- [ ] **Step 4: Verify the public configuration paths**

Run:

```bash
cargo run -p crabka-broker -- --help | rg 'cleaner-interval|replication-fetch|opa-http'
cargo run -p crabka-operator -- gen-crds /tmp/crabka-config-crds
diff -u deploy/crds/crabka.io_kafkas.yaml /tmp/crabka-config-crds/crabka.io_kafkas.yaml
```

Expected: help lists the representative direct options and regenerated CRD output is identical.

- [ ] **Step 5: Record broker-slice completion**

Add a dated evidence section to the ledger containing the exact passing commands and the final candidate count. Do not claim repo-wide completion.

- [ ] **Step 6: Commit**

```bash
git add docs/configuration-audit.md
git commit -m "docs: close broker config audit"
```
