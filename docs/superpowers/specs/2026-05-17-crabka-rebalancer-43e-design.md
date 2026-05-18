# Crabka rebalancer — slice 43e — usage scraper + usage goals (design)

**Date:** 2026-05-17
**Status:** Spec, ready for implementation plan
**Scope:** Bundle of two halves — broker-side per-partition metric emission (the `43e-core` flagged in the roadmap) plus rebalancer-side metric scraping, window storage, four new soft usage goals, and metric-driven bodies for three of the four 43d capacity stubs.

## Goal

Land slice 43e: the rebalancer collects per-partition usage data from each broker's `/metrics` endpoint, maintains rolling windows, and runs four new soft goals (`DiskUsage`, `LeaderBytesIn`, `NetworkInUsage`, `NetworkOutUsage`) plus real (no-longer-stub) bodies for `DiskCapacity` / `NetworkInCapacity` / `NetworkOutCapacity`. `CpuCapacity` stays a stub — slice 43f wires that.

## Out of scope (deferred)

- **`CpuUsage` (soft) + real `CpuCapacity` body** — slice 43f. Requires a per-partition CPU metric that's harder to attribute correctly.
- **Discovery of scrape targets via `Metadata`** — operator supplies `--metrics-scrape-targets` for now.
- **Per-topic resource hints in capacity config** — usage metrics provide the real input now.
- **Anomaly detection** (`detector` module) — slice 43g.
- **Operator `KafkaRebalance` CRD** — slice 44.
- **Histogram-based metrics** (P99 / P50 distributions) — flat averages are sufficient.

## Decisions captured during brainstorm

1. **Bundle broker + rebalancer in one slice.** The broker-side per-partition counters and the rebalancer-side scraper land together; alternative was two PRs in sequence.
2. **Periodic log-dir scan for disk usage.** Broker spawns a tokio task that ticks every `--partition-disk-scan-interval-secs` (default 60), walks each partition's log directory, sums segment file sizes, and updates a `crabka_broker_partition_disk_bytes` gauge.
3. **Fixed ring buffer for window storage.** Per-series ring buffer sized to the longest window (`retention / scrape_interval`). Goals average over the requested window. Operator caps memory via the scrape interval + retention.
4. **Operator-supplied scrape targets** in `id:host:port,id:host:port,…` format (the broker_id prefix is needed to attribute metrics to specific brokers; the leader sees full traffic, followers only see replication).
5. **Add `Goal::is_satisfied_with_ctx` to the trait** to close the 43d known trade. Default impl forwards to `is_satisfied`; the four capacity goals (`ReplicaCapacity`, `DiskCapacity`, `NetworkInCapacity`, `NetworkOutCapacity`) override to use the context's capacity + usage data. Optimizer's incremental hard-goal validation switches to call the new method.

## Goal lineup

### New soft goals (added by 43e)

- **`DiskUsage`** — balance per-broker total disk usage (sum of partition disk_bytes for partitions a broker hosts) within `imbalance_threshold_pct`.
- **`LeaderBytesIn`** — balance ingress on partitions a broker leads (per-partition bytes_in rate × broker-led mask). The right proxy for producer load.
- **`NetworkInUsage`** — balance per-broker total bytes_in rate (followers + leaders).
- **`NetworkOutUsage`** — balance per-broker total bytes_out rate.

### Capacity goals — stubs replaced with real bodies

- **`DiskCapacity`** (hard) — enforce `disk_bytes` limit using scraped disk gauges.
- **`NetworkInCapacity`** (hard) — enforce ingress rate limit using bytes_in rates.
- **`NetworkOutCapacity`** (hard) — enforce egress rate limit using bytes_out rates.
- **`CpuCapacity`** stays a stub (slice 43f).

`GoalRegistry::default_registry` grows from 11 goals to **15** (4 new soft).

### `Goal` trait addition

```rust
pub trait Goal: Send + Sync {
    fn name(&self) -> &'static str;
    fn priority(&self) -> GoalPriority;
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement>;
    fn is_satisfied(&self, _state: &ClusterState) -> bool { true }

    /// Same as `is_satisfied` but with `GoalContext` access. The
    /// optimizer's incremental hard-goal validation calls this so
    /// capacity goals can consult `broker_capacities` / `broker_usages`
    /// when deciding whether a tentative movement keeps their
    /// invariant intact.
    fn is_satisfied_with_ctx(&self, state: &ClusterState, _ctx: &GoalContext) -> bool {
        self.is_satisfied(state)
    }
}
```

`optimizer/mod.rs` swaps its current `gg.is_satisfied(&tentative)` (slice 43c's incremental check at the hard-goal validation step) for `gg.is_satisfied_with_ctx(&tentative, ctx)`. Soft goals inherit the default (which forwards to `is_satisfied`) and continue to work identically. Hard goals that need ctx override — closes the 43d trade.

## Broker-side changes (43e-core)

### `crates/broker/src/metrics.rs` — extensions

```rust
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
    pub topic: String,
    pub partition: i32,
}

// Added to BrokerMetrics:
pub partition_bytes_in: Family<PartitionLabel, Counter>,
pub partition_bytes_out: Family<PartitionLabel, Counter>,
pub partition_disk_bytes: Family<PartitionLabel, Gauge>,
```

Metric names emitted by `prometheus-client`:
- `crabka_broker_partition_bytes_in_total{topic="...", partition="0"}`
- `crabka_broker_partition_bytes_out_total{topic="...", partition="0"}`
- `crabka_broker_partition_disk_bytes{topic="...", partition="0"}`

Topic-level counters (`crabka_broker_topic_bytes_in_total` etc.) stay — slice 39 consumers still want them, and per-topic + per-partition both have legitimate uses.

### Emit-site changes

**`crates/broker/src/handlers/produce.rs`** — current code at line 153 sums bytes across all partitions of a topic and emits one `record_produce(&topic_name, topic_bytes)` call. Restructure:

```rust
for partition in &topic.partition_data {
    let partition_bytes = partition
        .records
        .as_ref()
        .map_or(0, |r| r.encoded_len() as u64);
    if partition_bytes > 0 {
        broker.metrics.record_partition_produce(&topic_name, partition.index, partition_bytes);
    }
}
// Existing topic-level call retained for slice-39 consumers.
let topic_bytes: u64 = topic.partition_data.iter()
    .map(|p| p.records.as_ref().map_or(0, |r| r.encoded_len() as u64))
    .sum();
if !topic_name.is_empty() {
    broker.metrics.record_produce(&topic_name, topic_bytes);
}
```

New method `BrokerMetrics::record_partition_produce(topic, partition, bytes)` increments `partition_bytes_in`.

**`crates/broker/src/handlers/fetch.rs:375`** — analogous change: emit one `record_partition_fetch(topic, partition, bytes)` per partition's `bytes`, plus keep the existing per-topic emit.

### `crates/broker/src/disk_scanner/`

```
disk_scanner/
├── mod.rs    # DiskScanner struct, run loop, CancellationToken integration
└── scan.rs   # Pure-logic: walk one log dir, sum segment file sizes
```

`scan::sum_partition_dir(path: &Path) -> Result<u64, io::Error>`: reads the directory entry, sums the size of every regular file. Pure-logic, easily unit-tested with a tempdir.

`DiskScanner::run(self)`:
1. `tokio::time::interval(self.interval)` tick loop.
2. Per tick: read the log manager's `partition_dirs() -> impl Iterator<Item=(PartitionLabel, PathBuf)>` (a new accessor on the existing log manager). For each entry, `sum_partition_dir(path)`, set `metrics.partition_disk_bytes.get_or_create(&label).set(bytes as i64)`.
3. Errors: `warn!` with topic/partition + the io error, skip that partition, continue the tick.
4. Shutdown via `CancellationToken` (mirrors `Ingester` pattern).

New broker CLI flag: `--partition-disk-scan-interval-secs <secs>` (env `CRABKA_PARTITION_DISK_SCAN_INTERVAL_SECS`, default 60). Setting to `0` disables the scanner entirely (no tick task spawned).

`Broker::start` spawns the `DiskScanner` task alongside its other background tasks. The task's `JoinHandle` is owned by the `Broker` so shutdown can drain it.

## Rebalancer-side changes

### `crates/rebalancer/src/scraper/`

```
scraper/
├── mod.rs       # Scraper struct + tick loop
├── parse.rs     # Pure-logic OpenMetrics text parser
├── targets.rs   # "id:host:port,..." → Vec<ScrapeTarget>
└── window.rs    # UsageStore + RingBuffer + Sample + MetricKind + Window
```

#### `targets.rs`

```rust
pub struct ScrapeTarget {
    pub broker_id: i32,
    pub addr: String,  // host:port; resolved at scrape time
}

pub fn parse_targets(spec: &str) -> Result<Vec<ScrapeTarget>, TargetParseError> { ... }
```

Empty input returns `Ok(vec![])`. Malformed entries (no `id:`, non-numeric id) → `TargetParseError::Malformed { entry: String }`. The binary entry treats an empty list as "scraper disabled" (skips the spawn).

#### `parse.rs`

Scoped OpenMetrics text parser that extracts only the three known families. Returns `Vec<ParsedSample>` where:

```rust
pub struct ParsedSample {
    pub metric: MetricKind,
    pub topic: String,
    pub partition: i32,
    pub value: f64,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub enum MetricKind {
    BytesIn,
    BytesOut,
    DiskBytes,
}
```

Implementation: line-oriented. Skip `#` comments + blank lines. For each metric line, match the metric name prefix against the three known families:
- `crabka_broker_partition_bytes_in_total` → `BytesIn`
- `crabka_broker_partition_bytes_out_total` → `BytesOut`
- `crabka_broker_partition_disk_bytes` → `DiskBytes`

Parse labels (`{topic="...",partition="..."}`), value. Skip lines for any other metric.

Don't use a generic Prometheus parser dep — keeps the scraper free of external dependencies and the parser is small (~80 lines).

#### `window.rs`

```rust
pub struct UsageStore {
    inner: parking_lot::RwLock<Inner>,
    config: WindowConfig,
}

#[derive(Clone, Copy)]
pub struct WindowConfig {
    pub scrape_interval: Duration,
    pub retention: Duration,
}

struct Inner {
    series: HashMap<SeriesKey, RingBuffer>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct SeriesKey {
    pub broker_id: i32,
    pub topic: String,
    pub partition: i32,
    pub metric: MetricKind,
}

pub struct RingBuffer {
    capacity: usize,
    samples: VecDeque<Sample>,
}

#[derive(Clone, Copy)]
pub struct Sample {
    pub at_ms: i64,
    pub value: f64,
}

#[derive(Clone, Copy)]
pub enum Window { FiveMin, OneHour, TwelveHour }
```

**API:**
- `UsageStore::new(config: WindowConfig) -> Self`
- `UsageStore::default() -> Self` — empty, default config (30s scrape / 12h retention)
- `UsageStore::insert(&self, broker_id: i32, samples: Vec<ParsedSample>, at_ms: i64)` — bulk-insert one scrape's worth. Drops samples older than `retention`. Single write lock.
- `UsageStore::bytes_in_rate(broker_id, topic, partition, window) -> Option<f64>` — earliest + latest sample within window; rate in bytes/sec. `None` if <2 samples.
- `UsageStore::bytes_out_rate(broker_id, topic, partition, window) -> Option<f64>` — same.
- `UsageStore::disk_bytes_avg(broker_id, topic, partition, window) -> Option<f64>` — arithmetic average of gauge samples within window.

Window length lookup:
- `FiveMin` → `Duration::from_secs(300)`
- `OneHour` → `Duration::from_secs(3600)`
- `TwelveHour` → `Duration::from_secs(43200)`

`parking_lot::RwLock` is preferred for cheaper read paths. Verify `parking_lot` is reachable (T1 adds it to the workspace if needed; the operator crate may already use it via `kube-runtime`).

#### `mod.rs`

```rust
pub struct Scraper {
    targets: Vec<ScrapeTarget>,
    interval: Duration,
    store: Arc<UsageStore>,
    http: reqwest::Client,
    shutdown: CancellationToken,
}
```

`Scraper::run(self)` — mirrors `Ingester::run`. Each tick: for each target, HTTP GET `http://{addr}/metrics` with a 5s timeout, parse the body, push samples into the store keyed by `broker_id`. Failures per target are logged at `warn!` once per consecutive-failure run; recovery clears the flag.

The scraper task is spawned from `bin/rebalancer.rs` when `--metrics-scrape-targets` is non-empty.

### `crates/rebalancer/src/goals/` — new + modified

Four new soft goal files (`disk_usage.rs`, `leader_bytes_in.rs`, `network_in_usage.rs`, `network_out_usage.rs`) each ~120 lines. Common shape:

```rust
impl Goal for DiskUsage {
    fn priority(&self) -> GoalPriority { GoalPriority::Soft }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        // Compute per-broker disk usage = sum over partitions hosted
        // by that broker of usage_store.disk_bytes_avg(...). Skip if
        // no usage data (the store returns None for partitions/series
        // it hasn't seen).
        // ... greedy hot-broker → cold-broker swap, threshold-driven,
        // same shape as ReplicaDistribution but driven by usage data.
    }
}
```

`LeaderBytesIn` differs in one detail: it sums `bytes_in_rate(broker, topic, partition, FiveMin)` only for partitions where the broker is the **leader**, not where it's any replica. This catches producer-driven load specifically.

Three modified capacity goal files:

- `disk_capacity.rs` — replace stub `propose` with a real body that emits movements when a broker's total disk usage (sum of partition disk_bytes) exceeds its `disk_bytes` capacity. Replace stub `is_satisfied` and add `is_satisfied_with_ctx(state, ctx) -> bool` that consults `ctx.broker_capacities` + `ctx.broker_usages`.
- `network_in_capacity.rs`, `network_out_capacity.rs` — same shape, sum of partition bytes_in/_out rates compared against the respective capacity.
- `cpu_capacity.rs` — unchanged stub.
- `replica_capacity.rs` — adds `is_satisfied_with_ctx` override that uses `ctx.broker_capacities` for the strict `max_replicas` check (closes 43d's known trade for this goal too).

### `GoalContext` extension

```rust
pub struct GoalContext {
    pub imbalance_threshold_pct: u32,
    pub max_movements_per_proposal: usize,
    pub min_topic_leaders_per_broker: u32,
    pub broker_capacities: Arc<BrokerCapacities>,
    pub broker_usages: Arc<UsageStore>,  // NEW
}
```

Default = empty `UsageStore::default()`. Threaded into the goals via the existing `&GoalContext` parameter on `propose`.

### `GoalRegistry::default_registry`

15 goals total:

Hard (unchanged order; some now have real bodies):
1. `PreferredLeaderIdempotency`
2. `RackAware`
3. `ReplicaCapacity`
4. `DiskCapacity` (functional now)
5. `NetworkInCapacity` (functional now)
6. `NetworkOutCapacity` (functional now)
7. `CpuCapacity` (still stub)

Soft (extended with four new):
8. `ReplicaDistribution`
9. `LeaderDistribution`
10. `TopicReplicaDistribution`
11. `MinTopicLeadersPerBroker`
12. `DiskUsage` *(new)*
13. `LeaderBytesIn` *(new)*
14. `NetworkInUsage` *(new)*
15. `NetworkOutUsage` *(new)*

### Binary entry

`bin/rebalancer.rs` gains three new CLI flags + one new spawn:

```rust
#[arg(long, env = "CRABKA_METRICS_SCRAPE_TARGETS", default_value = "")]
metrics_scrape_targets: String,

#[arg(long, env = "CRABKA_METRICS_SCRAPE_INTERVAL_SECS", default_value_t = 30)]
metrics_scrape_interval_secs: u64,

#[arg(long, env = "CRABKA_METRICS_RETENTION_SECS", default_value_t = 43_200)]
metrics_retention_secs: u64,
```

Loader block — after the capacity loader, before `AppState` construction:

```rust
let usage_store = Arc::new(UsageStore::new(WindowConfig {
    scrape_interval: Duration::from_secs(args.metrics_scrape_interval_secs),
    retention: Duration::from_secs(args.metrics_retention_secs),
}));

if !args.metrics_scrape_targets.is_empty() {
    let targets = scraper::targets::parse_targets(&args.metrics_scrape_targets)?;
    info!(
        target_count = targets.len(),
        scrape_interval_secs = args.metrics_scrape_interval_secs,
        retention_secs = args.metrics_retention_secs,
        "starting metrics scraper"
    );
    let scraper = Scraper::new(
        targets,
        Duration::from_secs(args.metrics_scrape_interval_secs),
        usage_store.clone(),
        shutdown.clone(),
    );
    tokio::spawn(scraper.run());
}
```

`AppState`'s `GoalContext` literal swaps the default `Arc::new(UsageStore::default())` (set in T2's mount step) for `usage_store.clone()`.

## Helm chart updates

`values.yaml` additions:

```yaml
# Per-broker metrics scrape targets. Format: "id:host:port,id:host:port,…".
# Empty = scraper disabled (four usage goals and three metric-driven
# capacity goals become no-ops).
metricsScrapeTargets: ""

metricsScrapeIntervalSecs: 30
metricsRetentionSecs: 43200
```

`templates/deployment.yaml` — three new env entries gated on the targets being set:

```yaml
{{- if .Values.metricsScrapeTargets }}
- name: CRABKA_METRICS_SCRAPE_TARGETS
  value: {{ .Values.metricsScrapeTargets | quote }}
- name: CRABKA_METRICS_SCRAPE_INTERVAL_SECS
  value: {{ .Values.metricsScrapeIntervalSecs | quote }}
- name: CRABKA_METRICS_RETENTION_SECS
  value: {{ .Values.metricsRetentionSecs | quote }}
{{- end }}
```

`tests/deployment_test.yaml` — new test asserting the three env vars render when `metricsScrapeTargets` is set.

## Testing

### Broker-side

- **`metrics::tests`** (1 new): assert `crabka_broker_partition_bytes_in_total{topic="t",partition="0"}` and `_out_total` render after `record_partition_*` calls.
- **`disk_scanner::scan::tests`** (3 tests): empty dir → 0; tempdir with N segment files → correct sum; missing dir → error.
- **`disk_scanner::tests`** (1 test): mock log-manager iterator → gauge values land on the right labels after one tick.
- **`tests/metrics.rs`** (integration, 1 new): real broker fixture, produce records, scrape `/metrics`, assert both topic-level and partition-level counters present with expected values.

### Rebalancer-side

- **`scraper::parse::tests`** (6 tests): empty input; well-formed counter; gauge; mixed metrics — only known families surface; malformed line skipped; metric without expected labels skipped.
- **`scraper::targets::tests`** (3 tests): empty; well-formed; malformed entry returns error.
- **`scraper::window::tests`** (5 tests): empty store → None; two-sample counter → correct bytes/sec; gauge averaging; retention drops old samples; concurrent insert/read smoke.
- **Per goal** (4 new + 3 modified ≈ 24 tests): empty UsageStore no-op; one hot broker → emits; threshold respected; (capacity) `is_satisfied_with_ctx` returns false when over.
- **`replica_capacity`** (1 new): `is_satisfied_with_ctx` returns false when broker exceeds `max_replicas`.
- **`optimizer::tests`** (1 new): soft movement that would violate a capacity goal's `is_satisfied_with_ctx` is dropped.
- **`tests/end_to_end.rs`** (1 new): `disk_usage_evicts_hot_broker` — synthetic ClusterState + pre-populated `UsageStore` with one hot broker; assert `DiskUsage.propose` emits.

### Helm

- **`deployment_test.yaml`** (1 new assertion): the three metrics env vars render when `metricsScrapeTargets` is set.

**Total new tests:** ~40 unit + 1 broker integration + 1 rebalancer integration + 1 helm-unittest assertion.

## Risks

- **Metric explosion.** A large cluster (10 brokers × 10000 partitions × 3 metrics × 1440 samples) is ~10 GB. The `--metrics-retention-secs` flag lets operators trim, but the failure mode (OOM) is unfriendly. Document the rough memory cost in STATUS and the Helm values comment.
- **Counter reset detection.** A broker restart resets counters to 0. The naive `(latest - earliest) / dt` rate computation will produce a huge negative value when the latest reading is post-reset. Defensive: if `latest < earliest`, treat as a counter reset and return `None` (no rate signal until two post-reset samples accumulate). Document in `window.rs`.
- **OpenMetrics text format quirks.** Our parser handles only what `prometheus-client` actually emits. Don't claim general OpenMetrics support; document it as scoped to the three known metric names.
- **`partition_dirs()` accessor on log manager.** The existing log manager may not expose this; T3 needs to add a small public accessor. Verify during T3 — if the accessor isn't easy to add, fall back to the broker config's `log_dir` + reading `<log_dir>/<topic>-<partition>/` directly.
- **`is_satisfied_with_ctx` correctness vs. tentative state.** The optimizer applies the tentative movement to a clone of the state before calling `is_satisfied_with_ctx`. Capacity goals consult `ctx.broker_usages` for current rates — but the *tentative* state has moved a replica, while the usages store is unchanged. So the check uses fresh placement + stale usage. That's fine for the first usage-driven proposal (operator-supplied placement causes usage to flow); subsequent proposals would benefit from usage re-estimation, deferred.

## Acceptance criteria

1. `cargo test -p crabka-broker` — existing + new tests pass.
2. `cargo test -p crabka-rebalancer` — existing + new tests pass.
3. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings` clean.
4. `helm lint charts/crabka-rebalancer --set bootstrapServers=test:9092` clean.
5. `helm unittest charts/crabka-rebalancer` clean.
6. `STATUS.md` gains a slice-43e entry covering broker-side per-partition metrics, the rebalancer scraper, the four new soft goals, the three now-functional capacity goals, and the `Goal::is_satisfied_with_ctx` trait addition.
7. Running the binary with `--metrics-scrape-targets ""` leaves usage-dependent goals as no-ops; setting it to a real broker activates them. Same fail-safe pattern as the 43d capacity config.

## File layout (summary)

```
crates/broker/
├── src/
│   ├── metrics.rs                                   # MODIFIED — PartitionLabel + 3 Family + 2 emit helpers
│   ├── disk_scanner/
│   │   ├── mod.rs                                   # NEW — DiskScanner task
│   │   └── scan.rs                                  # NEW — pure log-dir walk
│   ├── lib.rs                                       # MODIFIED — pub mod disk_scanner;
│   ├── broker.rs                                    # MODIFIED — spawn DiskScanner + new CLI flag plumbing
│   └── handlers/
│       ├── produce.rs                               # MODIFIED — per-partition emit
│       └── fetch.rs                                 # MODIFIED — per-partition emit
└── tests/metrics.rs                                 # MODIFIED — new partition-level integration assertion

crates/rebalancer/
├── src/
│   ├── scraper/
│   │   ├── mod.rs                                   # NEW — Scraper task
│   │   ├── parse.rs                                 # NEW — OpenMetrics text parser
│   │   ├── targets.rs                               # NEW — target list parser
│   │   └── window.rs                                # NEW — UsageStore + RingBuffer
│   ├── goals/
│   │   ├── mod.rs                                   # MODIFIED — trait gains is_satisfied_with_ctx; GoalContext gains broker_usages; 4 new pub mod
│   │   ├── disk_usage.rs                            # NEW
│   │   ├── leader_bytes_in.rs                       # NEW
│   │   ├── network_in_usage.rs                      # NEW
│   │   ├── network_out_usage.rs                     # NEW
│   │   ├── disk_capacity.rs                         # MODIFIED — real body + is_satisfied_with_ctx
│   │   ├── network_in_capacity.rs                   # MODIFIED — real body + is_satisfied_with_ctx
│   │   ├── network_out_capacity.rs                  # MODIFIED — real body + is_satisfied_with_ctx
│   │   └── replica_capacity.rs                      # MODIFIED — is_satisfied_with_ctx override
│   ├── api/mod.rs                                   # MODIFIED — registry: 11 → 15 + renamed test
│   ├── optimizer/mod.rs                             # MODIFIED — incremental validation uses is_satisfied_with_ctx
│   ├── bin/rebalancer.rs                            # MODIFIED — 3 new CLI flags + scraper spawn
│   └── lib.rs                                       # MODIFIED — pub mod scraper;
└── tests/end_to_end.rs                              # MODIFIED — 1 new test + fixture GoalContext literal

charts/crabka-rebalancer/
├── values.yaml                                       # MODIFIED — metricsScrape* values
├── templates/deployment.yaml                         # MODIFIED — 3 env entries
└── tests/deployment_test.yaml                        # MODIFIED — 1 assertion

STATUS.md                                             # MODIFIED — slice 43e entry
```
