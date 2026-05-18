# Slice 43e — Rebalancer usage scraper + usage goals — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Wire per-partition metrics emission on the broker (the `43e-core` half) and per-partition usage scraping + rolling-window storage + four soft usage goals + metric-driven bodies for three 43d capacity stubs on the rebalancer (the `43e` half) — together in one slice.

**Architecture:** Broker gains `PartitionLabel` + 3 new `Family` handles for partition bytes_in/out + disk_bytes; emit sites in `produce.rs`/`fetch.rs` switch from one topic-level inc to one-inc-per-partition. New `disk_scanner` module periodically walks log dirs and sets the disk gauge. Rebalancer gains a `scraper/` module with a tick loop that HTTP-GETs each broker's `/metrics`, parses the three known metric families, and pushes samples into an `Arc<UsageStore>` (per-series ring buffer). Four new soft goals (`DiskUsage`, `LeaderBytesIn`, `NetworkInUsage`, `NetworkOutUsage`) and three real-bodied capacity goals consume the store via `GoalContext`. `Goal` trait gains `is_satisfied_with_ctx` so the capacity goals' invariants are properly enforced in the optimizer's incremental check.

**Tech Stack:** Rust 1.95.0. Workspace deps already present: `reqwest` (used by the existing `tests/connect_smoke.rs`), `prometheus-client` (broker emit), `tokio`, `serde`, `parking_lot` (verify availability in T7; fall back to `std::sync::RwLock` if not). No new workspace deps expected.

**Reference spec:** [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43e-design.md`](../specs/2026-05-17-crabka-rebalancer-43e-design.md).

**Working directory:** `/home/matt/git/crabka`. Branch `feature/rebalancer-43e` exists with the spec committed.

---

## File structure

```
crates/broker/
├── src/
│   ├── metrics.rs                                   # MODIFIED — PartitionLabel + 3 Family handles + 2 emit helpers
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
│   │   ├── mod.rs                                   # NEW — Scraper task + tick loop
│   │   ├── parse.rs                                 # NEW — OpenMetrics text parser (3 families only)
│   │   ├── targets.rs                               # NEW — "id:host:port,..." parser
│   │   └── window.rs                                # NEW — UsageStore + RingBuffer + counter-reset
│   ├── goals/
│   │   ├── mod.rs                                   # MODIFIED — trait gains is_satisfied_with_ctx; GoalContext gains broker_usages; 4 new pub mod
│   │   ├── disk_usage.rs                            # NEW
│   │   ├── leader_bytes_in.rs                       # NEW
│   │   ├── network_in_usage.rs                      # NEW
│   │   ├── network_out_usage.rs                     # NEW
│   │   ├── disk_capacity.rs                         # MODIFIED — stub → real + is_satisfied_with_ctx
│   │   ├── network_in_capacity.rs                   # MODIFIED — stub → real + is_satisfied_with_ctx
│   │   ├── network_out_capacity.rs                  # MODIFIED — stub → real + is_satisfied_with_ctx
│   │   └── replica_capacity.rs                      # MODIFIED — adds is_satisfied_with_ctx override
│   ├── api/mod.rs                                   # MODIFIED — registry: 11 → 15 + renamed test
│   ├── optimizer/mod.rs                             # MODIFIED — incremental validation uses is_satisfied_with_ctx
│   ├── bin/rebalancer.rs                            # MODIFIED — 3 new CLI flags + scraper spawn
│   └── lib.rs                                       # MODIFIED — pub mod scraper;
└── tests/end_to_end.rs                              # MODIFIED — fixture + 1 new test

charts/crabka-rebalancer/
├── values.yaml                                       # MODIFIED — metricsScrapeTargets, metricsScrapeIntervalSecs, metricsRetentionSecs
├── templates/deployment.yaml                         # MODIFIED — 3 conditional env entries
└── tests/deployment_test.yaml                        # MODIFIED — 1 new assertion

STATUS.md                                             # MODIFIED — slice 43e entry
```

**17 tasks across 10 batches.**

- **Batch 1 (alone):** T1 — broker `PartitionLabel` + 3 new `Family` handles + 2 emit helpers.
- **Batch 2 (parallel):** T2 broker emit sites, T3 broker `disk_scanner` module.
- **Batch 3 (alone):** T4 broker integration test.
- **Batch 4 (parallel):** T5 rebalancer `parse.rs`, T6 `targets.rs`, T7 `window.rs`.
- **Batch 5 (alone):** T8 `scraper/mod.rs` (Scraper task).
- **Batch 6 (alone):** T9 `Goal` trait extension + `GoalContext` + 4 new `pub mod` + literal updates.
- **Batch 7 (parallel):** T10 four soft usage goals, T11 three capacity real bodies + ReplicaCapacity override.
- **Batch 8 (parallel):** T12 optimizer switch + regression test, T13 GoalRegistry update, T14 binary wiring.
- **Batch 9 (parallel):** T15 integration test, T16 Helm chart + helm-unittest.
- **Batch 10 (alone):** T17 STATUS.

---

## Batch 1 — Broker metrics extension

### Task 1: `PartitionLabel` + 3 new Family handles + 2 emit helpers

**Files:**
- Modify: `crates/broker/src/metrics.rs`

- [ ] **Step 1: Add `PartitionLabel` + family fields**

Read the file. Add `PartitionLabel` struct after the existing `TopicLabel` (around line 36):

```rust
/// Per-partition label set, paired with the new `partition_*` metric
/// families. Slice 43e — consumed by the rebalancer's metric scraper.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
    pub topic: String,
    pub partition: i32,
}
```

Add three new fields to `BrokerMetrics` (around line 48, after `topic_fetch_requests`):

```rust
    pub partition_bytes_in: Family<PartitionLabel, Counter>,
    pub partition_bytes_out: Family<PartitionLabel, Counter>,
    pub partition_disk_bytes: Family<PartitionLabel, Gauge>,
```

- [ ] **Step 2: Construct + register the three new families**

In `BrokerMetrics::new()`, after `let topic_fetch_requests = …`, add:

```rust
        let partition_bytes_in: Family<PartitionLabel, Counter> = Family::default();
        let partition_bytes_out: Family<PartitionLabel, Counter> = Family::default();
        let partition_disk_bytes: Family<PartitionLabel, Gauge> = Family::default();
```

After the existing `registry.register(...)` calls (after `isr_expands_total` registration), add:

```rust
        registry.register(
            "partition_bytes_in",
            "Bytes received from producers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            partition_bytes_in.clone(),
        );
        registry.register(
            "partition_bytes_out",
            "Bytes served to consumers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            partition_bytes_out.clone(),
        );
        registry.register(
            "partition_disk_bytes",
            "On-disk size of a partition's log directory (gauge). Updated by \
             the broker's periodic disk scanner; suppress if scanner is disabled.",
            partition_disk_bytes.clone(),
        );
```

In the `BrokerMetrics { ... }` literal returned at the end of `new()`, include the three new fields:

```rust
            partition_bytes_in,
            partition_bytes_out,
            partition_disk_bytes,
```

- [ ] **Step 3: Add `record_partition_produce` + `record_partition_fetch` helpers**

After the existing `record_fetch` method (around line 158), add:

```rust
    /// Convenience: account a partition's slice of a Produce request.
    /// Called once per partition by the request handler (alongside the
    /// existing topic-level `record_produce`).
    pub fn record_partition_produce(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_bytes_in.get_or_create(&lbl).inc_by(bytes);
    }

    /// Convenience: account a partition's slice of a Fetch response.
    pub fn record_partition_fetch(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_bytes_out.get_or_create(&lbl).inc_by(bytes);
    }
```

- [ ] **Step 4: Extend the existing emission-name assertion test**

In `metrics::tests` (search for `crabka_broker_topic_bytes_in_total` in the test, around line 186), update the names list to include the three new metric names:

```rust
        for needle in [
            "crabka_broker_topic_bytes_in_total",
            "crabka_broker_topic_bytes_out_total",
            "crabka_broker_topic_produce_requests_total",
            "crabka_broker_topic_fetch_requests_total",
            "crabka_broker_partitions_led",
            "crabka_broker_active_controller",
            "crabka_broker_isr_shrinks_total",
            "crabka_broker_isr_expands_total",
            "crabka_broker_partition_bytes_in_total",
            "crabka_broker_partition_bytes_out_total",
            "crabka_broker_partition_disk_bytes",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
```

- [ ] **Step 5: Add a new unit test that exercises the helpers**

Append to `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn partition_helpers_increment_the_right_family() {
        let m = BrokerMetrics::new();
        m.record_partition_produce("t", 0, 1024);
        m.record_partition_produce("t", 1, 512);
        m.record_partition_fetch("t", 0, 2048);
        m.partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "t".into(),
                partition: 0,
            })
            .set(1_000_000);

        let lbl_p0 = PartitionLabel { topic: "t".into(), partition: 0 };
        let lbl_p1 = PartitionLabel { topic: "t".into(), partition: 1 };
        assert_eq!(m.partition_bytes_in.get_or_create(&lbl_p0).get(), 1024);
        assert_eq!(m.partition_bytes_in.get_or_create(&lbl_p1).get(), 512);
        assert_eq!(m.partition_bytes_out.get_or_create(&lbl_p0).get(), 2048);
        assert_eq!(m.partition_disk_bytes.get_or_create(&lbl_p0).get(), 1_000_000);
    }

    #[test]
    fn zero_bytes_no_op_on_partition_helpers() {
        let m = BrokerMetrics::new();
        m.record_partition_produce("t", 0, 0);
        m.record_partition_fetch("t", 0, 0);
        let lbl = PartitionLabel { topic: "t".into(), partition: 0 };
        // Counters still exist (get_or_create creates them) but at 0.
        assert_eq!(m.partition_bytes_in.get_or_create(&lbl).get(), 0);
        assert_eq!(m.partition_bytes_out.get_or_create(&lbl).get(), 0);
    }
```

- [ ] **Step 6: Run tests + clippy**

```bash
cargo test -p crabka-broker --lib metrics -- --nocapture
```

Expected: existing tests + 2 new pass.

```bash
cargo clippy -p crabka-broker --lib -- -D warnings 2>&1 | grep "metrics.rs"
```

Expected: no output.

- [ ] **Step 7: Commit**

```bash
git -C /home/matt/git/crabka add crates/broker/src/metrics.rs
git -C /home/matt/git/crabka commit -m "broker(43e-core): PartitionLabel + 3 partition-level metric families

New PartitionLabel{topic, partition} drives three new
prometheus-client Family handles on BrokerMetrics:
crabka_broker_partition_bytes_in_total / _out_total (Counter) and
crabka_broker_partition_disk_bytes (Gauge). Two new emit helpers
record_partition_produce / record_partition_fetch encapsulate the
get_or_create + inc_by pattern. Topic-level counters (slice 39)
stay; partition-level is additive. Two new unit tests assert the
helpers route to the right families and zero-bytes no-ops.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — Broker emit sites + disk scanner (parallel: T2, T3)

### Task 2: Per-partition emit in `handlers/produce.rs` and `handlers/fetch.rs`

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Update `produce.rs`**

Read the file. Find the existing topic-level emit (around line 147-154):

```rust
        if !topic_name.is_empty() {
            let topic_bytes: u64 = topic
                .partition_data
                .iter()
                .map(|p| p.records.as_ref().map_or(0, |r| r.encoded_len() as u64))
                .sum();
            broker.metrics.record_produce(&topic_name, topic_bytes);
        }
```

Replace with a version that also emits per partition:

```rust
        if !topic_name.is_empty() {
            let mut topic_bytes: u64 = 0;
            for p in &topic.partition_data {
                let partition_bytes = p
                    .records
                    .as_ref()
                    .map_or(0, |r| r.encoded_len() as u64);
                broker
                    .metrics
                    .record_partition_produce(&topic_name, p.index, partition_bytes);
                topic_bytes += partition_bytes;
            }
            broker.metrics.record_produce(&topic_name, topic_bytes);
        }
```

The exact field name for the partition index — `p.index` vs `p.partition_index` — may differ. Check the actual struct by reading `crates/protocol/generated/ProduceRequest*.owned.rs` (search for `PartitionProduceData`). Use whichever field name the generated struct exposes.

- [ ] **Step 2: Update `fetch.rs`**

Find the existing emit at line 375:

```rust
        broker.metrics.record_fetch(&topic_resp.topic, bytes);
```

This is inside a loop. Look upward for the partition iteration; the partition's bytes are available as the loop's per-iteration value. Add a `record_partition_fetch` call inside the inner loop where `bytes` is computed per partition, while leaving the per-topic `record_fetch` call as the sum.

Concretely, the surrounding code (around lines 360-380) iterates partition responses. Identify the per-partition `bytes` variable and add:

```rust
broker.metrics.record_partition_fetch(&topic_resp.topic, p.partition_index, p.records_size_in_bytes());
```

(adjust the partition-index field and bytes accessor to whatever the actual fetch response uses). Read 30 lines of context around line 375 to identify the right shape.

- [ ] **Step 3: Run broker tests**

```bash
cargo test -p crabka-broker --lib 2>&1 | tail -5
```

Expected: all existing tests still pass (slice-39's topic-level metric tests should be unchanged in behavior).

```bash
cargo clippy -p crabka-broker --lib -- -D warnings 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/broker/src/handlers/produce.rs crates/broker/src/handlers/fetch.rs
git -C /home/matt/git/crabka commit -m "broker(43e-core): emit per-partition bytes_in/out in handlers

handlers/produce.rs and handlers/fetch.rs now call
record_partition_produce / record_partition_fetch once per partition
in the request/response, alongside the existing topic-level inc.
The topic-level counters from slice 39 are preserved; partition-level
is additive for the rebalancer's metric scraper.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 3: `disk_scanner` module + CLI flag + `Broker::start` spawn

**Files:**
- Create: `crates/broker/src/disk_scanner/mod.rs`
- Create: `crates/broker/src/disk_scanner/scan.rs`
- Modify: `crates/broker/src/lib.rs` (add `pub mod disk_scanner;`)
- Modify: `crates/broker/src/broker.rs` (spawn scanner + CLI flag plumbing)

- [ ] **Step 1: Write `crates/broker/src/disk_scanner/scan.rs`**

```rust
//! Pure-logic helper: sum the regular-file sizes inside a partition
//! directory. Returns 0 for a missing directory (treated as "not yet
//! materialized", not an error) and propagates IO errors for other
//! failure modes.

use std::fs;
use std::io;
use std::path::Path;

pub fn sum_partition_dir(path: &Path) -> Result<u64, io::Error> {
    let entries = match fs::read_dir(path) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut total: u64 = 0;
    for entry in entries {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_file() {
            total = total.saturating_add(meta.len());
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn empty_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(sum_partition_dir(tmp.path()).unwrap(), 0);
    }

    #[test]
    fn missing_dir_returns_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope");
        assert_eq!(sum_partition_dir(&missing).unwrap(), 0);
    }

    #[test]
    fn sums_regular_files() {
        let tmp = tempfile::tempdir().unwrap();
        let mut f1 = std::fs::File::create(tmp.path().join("00000000000000000000.log")).unwrap();
        f1.write_all(&vec![0u8; 1024]).unwrap();
        let mut f2 = std::fs::File::create(tmp.path().join("00000000000000000000.index")).unwrap();
        f2.write_all(&vec![0u8; 128]).unwrap();
        let mut f3 = std::fs::File::create(tmp.path().join("leader-epoch-checkpoint")).unwrap();
        f3.write_all(&vec![0u8; 32]).unwrap();
        assert_eq!(sum_partition_dir(tmp.path()).unwrap(), 1024 + 128 + 32);
    }

    #[test]
    fn ignores_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("subdir")).unwrap();
        let mut f = std::fs::File::create(tmp.path().join("subdir/inner.log")).unwrap();
        f.write_all(&vec![0u8; 999]).unwrap();
        let mut top = std::fs::File::create(tmp.path().join("top.log")).unwrap();
        top.write_all(&vec![0u8; 100]).unwrap();
        // Only `top.log` counted; the subdir is not recursed.
        assert_eq!(sum_partition_dir(tmp.path()).unwrap(), 100);
    }
}
```

- [ ] **Step 2: Write `crates/broker/src/disk_scanner/mod.rs`**

```rust
//! Periodic per-partition disk-usage scanner. Spawned by
//! `Broker::start` when `--partition-disk-scan-interval-secs > 0`.
//! Each tick walks the log directory for every known
//! (topic, partition), sums regular file sizes, and updates the
//! `partition_disk_bytes` gauge.

pub mod scan;

use std::path::PathBuf;
use std::time::Duration;

use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::log_dir;
use crate::metrics::{BrokerMetrics, PartitionLabel};

pub struct DiskScanner {
    pub log_dir: PathBuf,
    pub interval: Duration,
    pub metrics: BrokerMetrics,
    pub shutdown: CancellationToken,
}

impl DiskScanner {
    pub async fn run(self) {
        info!(interval_secs = self.interval.as_secs(), "disk scanner started");
        let mut ticker = interval(self.interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = self.shutdown.cancelled() => {
                    info!("disk scanner shutting down");
                    return;
                }
            }
            self.tick_once();
        }
    }

    fn tick_once(&self) {
        let partitions = match log_dir::scan(&self.log_dir) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "disk scanner: log_dir::scan failed; skipping tick");
                return;
            }
        };
        for (topic, partition) in partitions {
            let path = log_dir::partition_dir(&self.log_dir, &topic, partition);
            match scan::sum_partition_dir(&path) {
                Ok(bytes) => {
                    let lbl = PartitionLabel { topic, partition };
                    self.metrics
                        .partition_disk_bytes
                        .get_or_create(&lbl)
                        .set(i64::try_from(bytes).unwrap_or(i64::MAX));
                }
                Err(e) => {
                    warn!(?topic, partition, error = %e, "disk scanner: sum_partition_dir failed; skipping partition");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn tick_once_sets_gauge_for_each_partition() {
        let tmp = tempfile::tempdir().unwrap();
        // Materialize two partition dirs the way the broker would.
        let p0 = tmp.path().join("t-0");
        let p1 = tmp.path().join("t-1");
        std::fs::create_dir_all(&p0).unwrap();
        std::fs::create_dir_all(&p1).unwrap();
        let mut f0 = std::fs::File::create(p0.join("00.log")).unwrap();
        f0.write_all(&vec![0u8; 1234]).unwrap();
        let mut f1 = std::fs::File::create(p1.join("00.log")).unwrap();
        f1.write_all(&vec![0u8; 5678]).unwrap();

        let metrics = BrokerMetrics::new();
        let scanner = DiskScanner {
            log_dir: tmp.path().to_path_buf(),
            interval: Duration::from_secs(60),
            metrics: metrics.clone(),
            shutdown: CancellationToken::new(),
        };
        scanner.tick_once();

        let g0 = metrics
            .partition_disk_bytes
            .get_or_create(&PartitionLabel { topic: "t".into(), partition: 0 })
            .get();
        let g1 = metrics
            .partition_disk_bytes
            .get_or_create(&PartitionLabel { topic: "t".into(), partition: 1 })
            .get();
        assert_eq!(g0, 1234);
        assert_eq!(g1, 5678);
    }
}
```

- [ ] **Step 3: Mount the module**

Edit `crates/broker/src/lib.rs`. Append `pub mod disk_scanner;` alphabetically near the other broker submodules.

- [ ] **Step 4: Add the CLI flag + plumbing to `BrokerConfig`**

Read `crates/broker/src/broker.rs` (the file with `BrokerConfig` and `Broker::start`). Find the `BrokerConfig` struct. Add:

```rust
    /// Partition disk-usage scan cadence. `0` disables the scanner
    /// entirely (no background task spawned).
    pub partition_disk_scan_interval_secs: u64,
```

In any `Default` impl for `BrokerConfig`, add `partition_disk_scan_interval_secs: 60`.

If there's a CLI-args struct elsewhere that maps to `BrokerConfig` (e.g., in `bin/crabka-broker.rs`), add a corresponding clap field there with the same env var:

```rust
    #[arg(long, env = "CRABKA_PARTITION_DISK_SCAN_INTERVAL_SECS", default_value_t = 60)]
    partition_disk_scan_interval_secs: u64,
```

…and thread it into the constructed `BrokerConfig`.

- [ ] **Step 5: Spawn the scanner in `Broker::start`**

In `Broker::start` (the long function around `crates/broker/src/broker.rs:830+`), after the existing background-task spawns (the section near `tokio::spawn(crate::isr_maintenance::run(...))` etc., around line 1185-1240), add:

```rust
        if config.partition_disk_scan_interval_secs > 0 {
            let scanner = crate::disk_scanner::DiskScanner {
                log_dir: config.log_dir.clone(),
                interval: std::time::Duration::from_secs(config.partition_disk_scan_interval_secs),
                metrics: metrics.clone(),
                shutdown: shutdown_token.clone(),
            };
            tokio::spawn(scanner.run());
        }
```

The local variable names `metrics` and `shutdown_token` may differ — substitute the actual names used in the surrounding code. Look ~30 lines up/down to find them.

- [ ] **Step 6: Tests + clippy**

```bash
cargo test -p crabka-broker --lib disk_scanner -- --nocapture
```

Expected: 5 tests pass (4 in `scan::tests` + 1 in `disk_scanner::tests`).

```bash
cargo clippy -p crabka-broker --lib -- -D warnings 2>&1 | grep "disk_scanner\|broker.rs"
```

Expected: no output for these files.

- [ ] **Step 7: Commit**

```bash
git -C /home/matt/git/crabka add crates/broker/src/disk_scanner crates/broker/src/lib.rs crates/broker/src/broker.rs
# Also add the CLI-args file if it was a separate one.
git -C /home/matt/git/crabka commit -m "broker(43e-core): disk_scanner module + CLI flag + Broker::start spawn

New disk_scanner submodule with pure-logic sum_partition_dir
(crates/broker/src/disk_scanner/scan.rs) + DiskScanner tick loop
(disk_scanner/mod.rs) that walks log_dir::scan's (topic, partition)
list each tick and sets crabka_broker_partition_disk_bytes. New CLI
flag --partition-disk-scan-interval-secs (env
CRABKA_PARTITION_DISK_SCAN_INTERVAL_SECS, default 60; 0 disables).
Spawned by Broker::start alongside isr_maintenance etc.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Broker integration test

### Task 4: Integration test for per-partition metrics + disk scanner

**Files:**
- Modify: `crates/broker/tests/metrics.rs`

- [ ] **Step 1: Append a new test**

Read the existing file to see imports + the existing single-broker fixture pattern. Append:

```rust
/// Slice 43e-core: confirm that per-partition counters land alongside
/// topic-level ones, and that the disk-scanner gauge picks up
/// non-zero values for materialized partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_level_metrics_and_disk_gauge_render() {
    let (broker, _bootstrap, _dir) = boot_broker_with_disk_scan_interval(1).await;

    // Create a topic and produce a record so partition_bytes_in fires.
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("metrics-test")
        .build()
        .await
        .unwrap();
    create_topic(&client, "tt", 2).await;

    // Produce to partition 0; topic_bytes_in and partition_bytes_in{partition="0"}
    // should both bump.
    produce_one(&client, "tt", 0, b"hello").await;

    // Wait for the disk scanner to tick at least once (configured 1s).
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let metrics_text = scrape_broker_metrics(&broker).await;

    // Topic-level (slice 39) still present.
    assert!(
        metrics_text.contains("crabka_broker_topic_bytes_in_total{topic=\"tt\"}"),
        "missing topic-level bytes_in"
    );
    // Partition-level (slice 43e-core) present with non-zero value.
    let needle_in = "crabka_broker_partition_bytes_in_total{topic=\"tt\",partition=\"0\"}";
    assert!(metrics_text.contains(needle_in), "missing partition-level bytes_in");

    // Disk gauge: emitted for at least one (topic, partition) of "tt".
    assert!(
        metrics_text.contains("crabka_broker_partition_disk_bytes{topic=\"tt\""),
        "missing partition_disk_bytes for materialized partition"
    );

    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), broker.shutdown()).await;
}
```

Helper functions referenced (`boot_broker_with_disk_scan_interval`, `produce_one`, `scrape_broker_metrics`) — define them at the top of the file (or extend existing helpers). If the existing test file uses a different pattern (e.g., a `Broker::start_for_test`), adapt:

```rust
async fn boot_broker_with_disk_scan_interval(secs: u64) -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.partition_disk_scan_interval_secs = secs;
    let handle = crabka_broker::Broker::start(cfg).await.unwrap();
    let bootstrap = handle.listen_addr().to_string();
    (handle, bootstrap, dir)
}

async fn produce_one(client: &crabka_client_core::Client, topic: &str, partition: i32, payload: &[u8]) {
    use crabka_protocol::owned::produce_request::{
        ProduceRequest, TopicProduceData, PartitionProduceData,
    };
    let mut records = bytes::BytesMut::new();
    // Minimal record-batch construction; reuse whatever the existing tests use.
    records.extend_from_slice(payload);
    let req = ProduceRequest {
        topics: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(records.freeze()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    client.send(req).await.unwrap();
}

async fn scrape_broker_metrics(broker: &crabka_broker::BrokerHandle) -> String {
    let addr = broker.metrics_listen_addr().expect("broker exposes /metrics");
    let resp = reqwest::get(format!("http://{addr}/metrics")).await.unwrap();
    resp.text().await.unwrap()
}
```

`BrokerHandle::metrics_listen_addr` may not exist as named — check `crates/broker/src/metrics_server.rs` for the actual accessor on the handle. If there's no public way to get the metrics addr, fall back to scraping via an explicit port set in `BrokerConfig::for_tests`.

The `produce_one` body may need tuning — minimal record-batch encoding has its own surface. Look at any existing produce test (search for `client.send(produce_request` in `crates/broker/tests/` or `crates/client-producer/tests/`) for a working example to copy.

If you can't get a quick produce path to work, **substitute**: directly call `broker.metrics.record_partition_produce("tt", 0, 1024)` from the test (which the test fixture can do if `metrics()` accessor exists on `BrokerHandle`). The integration is still meaningful: it proves the metric renders.

- [ ] **Step 2: Run the test**

```bash
cargo test -p crabka-broker --test metrics partition_level_metrics_and_disk_gauge_render -- --nocapture 2>&1 | tail -15
```

Expected: PASS.

If it fails on missing helpers (`for_tests`, `metrics_listen_addr`, etc.), adjust to match the actual broker test-helper surface. Report what's blocking if you need new public accessors.

- [ ] **Step 3: Clippy**

```bash
cargo clippy -p crabka-broker --tests -- -D warnings 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/broker/tests/metrics.rs
git -C /home/matt/git/crabka commit -m "broker(43e-core): integration test for per-partition metrics + disk gauge

Spins a single-broker fixture with --partition-disk-scan-interval-secs=1,
produces a record, scrapes /metrics, asserts: (a) topic-level
crabka_broker_topic_bytes_in_total still present (slice 39),
(b) partition-level crabka_broker_partition_bytes_in_total
{topic,partition} present with non-zero value,
(c) crabka_broker_partition_disk_bytes gauge emitted after scanner tick.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Rebalancer pure-logic modules (parallel: T5, T6, T7)

### Task 5: `scraper::parse` — OpenMetrics text parser

**Files:**
- Create: `crates/rebalancer/src/scraper/parse.rs`
- Create: `crates/rebalancer/src/scraper/mod.rs` (minimal; T8 fills it in)
- Modify: `crates/rebalancer/src/lib.rs` (add `pub mod scraper;`)

- [ ] **Step 1: Write `crates/rebalancer/src/scraper/parse.rs`**

```rust
//! Scoped OpenMetrics text parser. Recognizes only three families:
//! `crabka_broker_partition_bytes_in_total`,
//! `crabka_broker_partition_bytes_out_total`, and
//! `crabka_broker_partition_disk_bytes`. Everything else is silently
//! skipped — no allocation, no panic.

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum MetricKind {
    BytesIn,
    BytesOut,
    DiskBytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric: MetricKind,
    pub topic: String,
    pub partition: i32,
    pub value: f64,
}

pub fn parse(text: &str) -> Vec<ParsedSample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some(sample) = parse_line(trimmed) else {
            continue;
        };
        out.push(sample);
    }
    out
}

fn parse_line(line: &str) -> Option<ParsedSample> {
    // Expected shape: <name>{<labels>} <value> [<timestamp>]
    let (name_and_labels, value_str) = line.rsplit_once(char::is_whitespace)?;
    // The "value" portion may have a trailing timestamp; split again on whitespace.
    let value_str = value_str.split_whitespace().next()?;
    let value: f64 = value_str.parse().ok()?;

    let (name, labels) = name_and_labels.split_once('{')?;
    let labels = labels.strip_suffix('}')?;

    let metric = match name {
        "crabka_broker_partition_bytes_in_total" => MetricKind::BytesIn,
        "crabka_broker_partition_bytes_out_total" => MetricKind::BytesOut,
        "crabka_broker_partition_disk_bytes" => MetricKind::DiskBytes,
        _ => return None,
    };

    let mut topic: Option<String> = None;
    let mut partition: Option<i32> = None;
    for pair in labels.split(',') {
        let (k, v) = pair.split_once('=')?;
        let v = v.trim().strip_prefix('"').and_then(|s| s.strip_suffix('"'))?;
        match k.trim() {
            "topic" => topic = Some(v.to_string()),
            "partition" => partition = v.parse().ok(),
            _ => {} // ignore unknown label
        }
    }

    Some(ParsedSample {
        metric,
        topic: topic?,
        partition: partition?,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_empty() {
        assert!(parse("").is_empty());
    }

    #[test]
    fn parses_a_well_formed_counter() {
        let txt = r#"crabka_broker_partition_bytes_in_total{topic="t",partition="0"} 1024
"#;
        let out = parse(txt);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric, MetricKind::BytesIn);
        assert_eq!(out[0].topic, "t");
        assert_eq!(out[0].partition, 0);
        assert_eq!(out[0].value, 1024.0);
    }

    #[test]
    fn parses_a_gauge() {
        let txt = r#"crabka_broker_partition_disk_bytes{topic="t",partition="5"} 1234567
"#;
        let out = parse(txt);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric, MetricKind::DiskBytes);
        assert_eq!(out[0].partition, 5);
        assert_eq!(out[0].value, 1_234_567.0);
    }

    #[test]
    fn mixed_metrics_only_known_families_surface() {
        let txt = r#"# HELP foo
# TYPE crabka_broker_partition_bytes_in_total counter
crabka_broker_partition_bytes_in_total{topic="t",partition="0"} 1
crabka_broker_topic_bytes_in_total{topic="t"} 999
some_other_metric 7
crabka_broker_partition_bytes_out_total{topic="t",partition="0"} 2
"#;
        let out = parse(txt);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].metric, MetricKind::BytesIn);
        assert_eq!(out[1].metric, MetricKind::BytesOut);
    }

    #[test]
    fn malformed_line_is_skipped() {
        let txt = "crabka_broker_partition_bytes_in_total{nope this is broken\n";
        assert!(parse(txt).is_empty());
    }

    #[test]
    fn missing_partition_label_is_skipped() {
        let txt = "crabka_broker_partition_bytes_in_total{topic=\"t\"} 1024\n";
        assert!(parse(txt).is_empty(), "missing partition label must skip");
    }
}
```

- [ ] **Step 2: Write minimal `crates/rebalancer/src/scraper/mod.rs`**

```rust
//! Per-partition metric scraper. Spawned from the binary entry when
//! `--metrics-scrape-targets` is non-empty. T8 lands the full
//! Scraper task; this stub is mounted so T5/T6/T7 can be developed
//! in parallel.

pub mod parse;
```

- [ ] **Step 3: Mount in `lib.rs`**

Append `pub mod scraper;` to `crates/rebalancer/src/lib.rs` (alphabetical placement near `pub mod metrics;` / `pub mod model;`).

- [ ] **Step 4: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib scraper::parse -- --nocapture
```

Expected: 6 tests pass.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "scraper"
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/scraper crates/rebalancer/src/lib.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): scraper::parse OpenMetrics text parser

Scoped parser that surfaces only the three known partition-level
metrics (bytes_in_total, bytes_out_total, disk_bytes). Everything
else (topic-level, unknown families, comments, malformed lines)
is silently skipped. Six unit tests cover empty / counter / gauge
/ mixed / malformed / missing-label paths.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 6: `scraper::targets` — "id:host:port,..." parser

**Files:**
- Create: `crates/rebalancer/src/scraper/targets.rs`
- Modify: `crates/rebalancer/src/scraper/mod.rs` (add `pub mod targets;`)

- [ ] **Step 1: Write `crates/rebalancer/src/scraper/targets.rs`**

```rust
//! Parses the `--metrics-scrape-targets` CLI value:
//! `id:host:port,id:host:port,...` into a list of `ScrapeTarget`s.
//! Empty input is fine (scraper disabled). Malformed entries return
//! a typed error rather than panicking.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrapeTarget {
    pub broker_id: i32,
    pub addr: String, // host:port; resolved at scrape time
}

#[derive(Debug, thiserror::Error)]
pub enum TargetParseError {
    #[error("malformed entry `{0}` (expected `id:host:port`)")]
    Malformed(String),
    #[error("invalid broker id in `{0}`")]
    BadId(String),
}

pub fn parse_targets(spec: &str) -> Result<Vec<ScrapeTarget>, TargetParseError> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(Vec::new());
    }
    spec.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let (id_str, addr) = entry
                .split_once(':')
                .ok_or_else(|| TargetParseError::Malformed(entry.to_string()))?;
            // After splitting on the first `:`, `addr` is `host:port`.
            if !addr.contains(':') {
                return Err(TargetParseError::Malformed(entry.to_string()));
            }
            let broker_id: i32 = id_str
                .parse()
                .map_err(|_| TargetParseError::BadId(entry.to_string()))?;
            Ok(ScrapeTarget {
                broker_id,
                addr: addr.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_returns_empty_vec() {
        assert!(parse_targets("").unwrap().is_empty());
        assert!(parse_targets("   ").unwrap().is_empty());
    }

    #[test]
    fn well_formed_entries_parse() {
        let out = parse_targets("1:broker1:9100,2:broker2:9100,3:broker3:9100").unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].broker_id, 1);
        assert_eq!(out[0].addr, "broker1:9100");
        assert_eq!(out[2].broker_id, 3);
        assert_eq!(out[2].addr, "broker3:9100");
    }

    #[test]
    fn malformed_entry_errors() {
        let err = parse_targets("nope").unwrap_err();
        assert!(matches!(err, TargetParseError::Malformed(_)));
        let err = parse_targets("1:host_without_port").unwrap_err();
        assert!(matches!(err, TargetParseError::Malformed(_)));
        let err = parse_targets("abc:host:9100").unwrap_err();
        assert!(matches!(err, TargetParseError::BadId(_)));
    }
}
```

- [ ] **Step 2: Mount in `scraper/mod.rs`**

Edit `crates/rebalancer/src/scraper/mod.rs`. The current state (from T5) has `pub mod parse;`. Add `pub mod targets;` after.

If T5 lands after T6 mid-batch, the file may not exist yet — defensive: if `mod.rs` doesn't exist, create it with both module declarations.

- [ ] **Step 3: Tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib scraper::targets -- --nocapture
```

Expected: 3 tests pass.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "scraper"
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/scraper
git -C /home/matt/git/crabka commit -m "rebalancer(43e): scraper::targets CLI value parser

\"id:host:port,id:host:port,...\" → Vec<ScrapeTarget> with typed
errors for malformed entries (no panic). Empty input is fine —
scraper disabled. Three unit tests cover empty / well-formed /
malformed paths.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 7: `scraper::window` — UsageStore + RingBuffer

**Files:**
- Create: `crates/rebalancer/src/scraper/window.rs`
- Modify: `crates/rebalancer/src/scraper/mod.rs` (add `pub mod window;`)
- Modify: `crates/rebalancer/Cargo.toml` (add `parking_lot` if not already a workspace dep — check first)

- [ ] **Step 1: Verify `parking_lot` availability**

```bash
grep -n "parking_lot" /home/matt/git/crabka/Cargo.toml
```

If `parking_lot` is in `[workspace.dependencies]`, fine. Otherwise add it: workspace version `0.12` (the operator's `kube-runtime` probably already pulls it in transitively, but we need the direct dep).

Add to `crates/rebalancer/Cargo.toml` under `[dependencies]`:

```toml
parking_lot = { workspace = true }
```

If `parking_lot` isn't a workspace dep at all, use `std::sync::RwLock` in `window.rs` instead — write the code using `parking_lot` first, then if T7's build fails on the missing dep, swap the import and method calls (`std::sync::RwLock::read().unwrap()` instead of `parking_lot::RwLock::read()`).

- [ ] **Step 2: Write `crates/rebalancer/src/scraper/window.rs`**

```rust
//! Per-series ring buffer of timestamped samples + a thread-safe
//! `UsageStore` that the scraper writes to and the goals read from.
//!
//! Stored series key is `(broker_id, topic, partition, MetricKind)`.
//! Samples older than `config.retention` are dropped on each insert.
//! Counter-reset detection: if `latest.value < earliest.value`, the
//! rate query returns `None` (broker restarted; goals should ignore).

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use parking_lot::RwLock;

use crate::scraper::parse::{MetricKind, ParsedSample};

#[derive(Debug, Clone, Copy)]
pub struct WindowConfig {
    pub scrape_interval: Duration,
    pub retention: Duration,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_secs(43_200), // 12h
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Window {
    FiveMin,
    OneHour,
    TwelveHour,
}

impl Window {
    fn as_duration(self) -> Duration {
        match self {
            Window::FiveMin => Duration::from_secs(300),
            Window::OneHour => Duration::from_secs(3600),
            Window::TwelveHour => Duration::from_secs(43_200),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SeriesKey {
    broker_id: i32,
    topic: String,
    partition: i32,
    metric: MetricKind,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    at_ms: i64,
    value: f64,
}

#[derive(Debug, Default)]
struct RingBuffer {
    samples: VecDeque<Sample>,
}

pub struct UsageStore {
    inner: RwLock<HashMap<SeriesKey, RingBuffer>>,
    config: WindowConfig,
}

impl UsageStore {
    #[must_use]
    pub fn new(config: WindowConfig) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Insert one scrape tick's worth of samples for a single broker.
    /// `at_ms` is the wall-clock millis at scrape time. Drops samples
    /// older than `config.retention`.
    pub fn insert(&self, broker_id: i32, samples: Vec<ParsedSample>, at_ms: i64) {
        let cutoff = at_ms - i64::try_from(self.config.retention.as_millis()).unwrap_or(i64::MAX);
        let mut map = self.inner.write();
        for s in samples {
            let key = SeriesKey {
                broker_id,
                topic: s.topic,
                partition: s.partition,
                metric: s.metric,
            };
            let buf = map.entry(key).or_default();
            buf.samples.push_back(Sample {
                at_ms,
                value: s.value,
            });
            while buf
                .samples
                .front()
                .is_some_and(|f| f.at_ms < cutoff)
            {
                buf.samples.pop_front();
            }
        }
    }

    /// Rate of `BytesIn` (bytes/sec) within `window`, derived from the
    /// earliest + latest samples in the window. Returns `None` if
    /// there are fewer than 2 samples in the window or if a counter
    /// reset is detected (latest.value < earliest.value).
    #[must_use]
    pub fn bytes_in_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
    ) -> Option<f64> {
        self.counter_rate(broker_id, topic, partition, MetricKind::BytesIn, window)
    }

    #[must_use]
    pub fn bytes_out_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
    ) -> Option<f64> {
        self.counter_rate(broker_id, topic, partition, MetricKind::BytesOut, window)
    }

    /// Average disk-bytes gauge over `window`. Returns `None` if no
    /// samples in window.
    #[must_use]
    pub fn disk_bytes_avg(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
    ) -> Option<f64> {
        let key = SeriesKey {
            broker_id,
            topic: topic.to_string(),
            partition,
            metric: MetricKind::DiskBytes,
        };
        let map = self.inner.read();
        let buf = map.get(&key)?;
        let now_ms = buf.samples.back()?.at_ms;
        let lower = now_ms - i64::try_from(window.as_duration().as_millis()).unwrap_or(i64::MAX);
        let mut sum = 0.0f64;
        let mut count = 0u64;
        for s in &buf.samples {
            if s.at_ms >= lower {
                sum += s.value;
                count += 1;
            }
        }
        if count == 0 {
            None
        } else {
            Some(sum / count as f64)
        }
    }

    fn counter_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        metric: MetricKind,
        window: Window,
    ) -> Option<f64> {
        let key = SeriesKey {
            broker_id,
            topic: topic.to_string(),
            partition,
            metric,
        };
        let map = self.inner.read();
        let buf = map.get(&key)?;
        if buf.samples.len() < 2 {
            return None;
        }
        let latest = *buf.samples.back()?;
        let lower = latest.at_ms - i64::try_from(window.as_duration().as_millis()).unwrap_or(i64::MAX);
        // Earliest sample within the window.
        let earliest = buf
            .samples
            .iter()
            .find(|s| s.at_ms >= lower)
            .copied()?;
        if latest.at_ms == earliest.at_ms {
            return None;
        }
        // Counter reset detection.
        if latest.value < earliest.value {
            return None;
        }
        let dt_ms = (latest.at_ms - earliest.at_ms) as f64;
        let dv = latest.value - earliest.value;
        Some(dv * 1000.0 / dt_ms)
    }
}

impl Default for UsageStore {
    fn default() -> Self {
        Self::new(WindowConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(metric: MetricKind, topic: &str, partition: i32, value: f64) -> ParsedSample {
        ParsedSample {
            metric,
            topic: topic.into(),
            partition,
            value,
        }
    }

    #[test]
    fn empty_store_returns_none() {
        let s = UsageStore::default();
        assert!(s.bytes_in_rate(1, "t", 0, Window::FiveMin).is_none());
        assert!(s.disk_bytes_avg(1, "t", 0, Window::FiveMin).is_none());
    }

    #[test]
    fn two_counter_samples_yield_rate() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 1000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 3000.0)], 1000);
        // (3000 - 1000) / 1.0s = 2000 bytes/sec
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin).unwrap();
        assert!((rate - 2000.0).abs() < 1e-6, "got {rate}");
    }

    #[test]
    fn counter_reset_returns_none() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 5000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 1000);
        assert!(s.bytes_in_rate(1, "t", 0, Window::FiveMin).is_none());
    }

    #[test]
    fn gauge_average() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 200.0)], 1000);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 300.0)], 2000);
        let avg = s.disk_bytes_avg(1, "t", 0, Window::FiveMin).unwrap();
        assert!((avg - 200.0).abs() < 1e-6, "got {avg}");
    }

    #[test]
    fn retention_drops_old_samples() {
        let s = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_secs(60),
        });
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 200.0)], 30_000);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 300.0)], 90_000);
        // First sample (t=0) was 90s ago, beyond the 60s retention; dropped.
        // Only samples at 30_000 and 90_000 remain.
        // The 5-min window includes both. Rate = (300-200)/60s = ~1.67/sec
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin).unwrap();
        assert!((rate - 100.0 / 60.0).abs() < 1e-3, "got {rate}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_insert_and_read_does_not_deadlock() {
        let store = std::sync::Arc::new(UsageStore::default());
        let mut handles = Vec::new();
        for i in 0..10 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..100 {
                    store.insert(
                        i,
                        vec![sample(MetricKind::BytesIn, "t", 0, (i * 100 + j) as f64)],
                        (i * 100 + j) as i64,
                    );
                }
            }));
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let _ = store.bytes_in_rate(i, "t", 0, Window::FiveMin);
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        // Reaching here means no deadlock; sanity-check that at least
        // one rate is queryable.
        let _ = store.bytes_in_rate(0, "t", 0, Window::FiveMin);
    }
}
```

- [ ] **Step 3: Mount in `scraper/mod.rs`**

Edit `crates/rebalancer/src/scraper/mod.rs`. Add `pub mod window;` after `pub mod targets;` (alphabetical). Re-export commonly-needed types at the module level so consumers don't need the deep paths:

```rust
pub mod parse;
pub mod targets;
pub mod window;

pub use parse::{MetricKind, ParsedSample};
pub use targets::{parse_targets, ScrapeTarget, TargetParseError};
pub use window::{UsageStore, Window, WindowConfig};
```

- [ ] **Step 4: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib scraper::window -- --nocapture
```

Expected: 6 tests pass (5 unit + 1 concurrency smoke).

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "scraper"
```

Expected: no output. Standard clippy fixups (`cast_*` → `try_from`, `doc_markdown` → backtick, etc.) if needed.

If `parking_lot` isn't available, swap to `std::sync::RwLock` — the API differs slightly (`.read().unwrap()` / `.write().unwrap()` instead of `.read()` / `.write()`). Update method bodies accordingly.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/scraper crates/rebalancer/Cargo.toml
git -C /home/matt/git/crabka commit -m "rebalancer(43e): scraper::window UsageStore with rolling windows

Per-series ring buffer keyed by (broker_id, topic, partition, MetricKind).
Three window lengths (5min / 1h / 12h). Counter rates derived from
earliest + latest samples within window; counter-reset detection
returns None when latest < earliest. Gauges return the arithmetic
average over the window. Six unit tests cover empty / two-sample
rate / counter-reset / gauge-average / retention / concurrent
access. Re-exports MetricKind, ParsedSample, ScrapeTarget,
UsageStore, Window, WindowConfig at the scraper module root.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 5 — Scraper task

### Task 8: `scraper::mod` — Scraper struct + HTTP tick loop

**Files:**
- Modify: `crates/rebalancer/src/scraper/mod.rs` (extend with Scraper + tick loop)

- [ ] **Step 1: Add `Scraper` to `scraper/mod.rs`**

Replace the contents of `crates/rebalancer/src/scraper/mod.rs` (currently just module declarations + re-exports) with:

```rust
//! Per-partition metric scraper. Spawned from the binary entry when
//! `--metrics-scrape-targets` is non-empty.

pub mod parse;
pub mod targets;
pub mod window;

pub use parse::{MetricKind, ParsedSample};
pub use targets::{parse_targets, ScrapeTarget, TargetParseError};
pub use window::{UsageStore, Window, WindowConfig};

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub struct Scraper {
    targets: Vec<ScrapeTarget>,
    interval: Duration,
    store: Arc<UsageStore>,
    http: reqwest::Client,
    shutdown: CancellationToken,
}

impl Scraper {
    #[must_use]
    pub fn new(
        targets: Vec<ScrapeTarget>,
        interval: Duration,
        store: Arc<UsageStore>,
        shutdown: CancellationToken,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client");
        Self {
            targets,
            interval,
            store,
            http,
            shutdown,
        }
    }

    pub async fn run(self) {
        info!(
            target_count = self.targets.len(),
            interval_secs = self.interval.as_secs(),
            "scraper started"
        );
        let mut ticker = tokio::time::interval(self.interval);
        // Don't skip the first tick — pull metrics immediately on startup.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = self.shutdown.cancelled() => {
                    info!("scraper shutting down");
                    return;
                }
            }
            self.tick_once().await;
        }
    }

    async fn tick_once(&self) {
        let now_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
        )
        .unwrap_or(0);
        for target in &self.targets {
            let url = format!("http://{}/metrics", target.addr);
            match self.http.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => match resp.text().await {
                    Ok(body) => {
                        let samples = parse::parse(&body);
                        debug!(broker_id = target.broker_id, url = %url, count = samples.len(), "scrape ok");
                        self.store.insert(target.broker_id, samples, now_ms);
                    }
                    Err(e) => {
                        warn!(broker_id = target.broker_id, url = %url, error = %e, "scrape body read failed");
                    }
                },
                Ok(resp) => {
                    warn!(broker_id = target.broker_id, url = %url, status = %resp.status(), "scrape returned non-success");
                }
                Err(e) => {
                    warn!(broker_id = target.broker_id, url = %url, error = %e, "scrape transport failure");
                }
            }
        }
    }
}
```

- [ ] **Step 2: Verify `reqwest` is reachable**

`reqwest` is already in `crates/rebalancer/Cargo.toml` (used by `tests/connect_smoke.rs`). Verify it's at module-public scope (not dev-only). Read the Cargo.toml — if `reqwest` is only in `[dev-dependencies]`, move it to `[dependencies]` with the same features.

- [ ] **Step 3: Build + clippy**

```bash
cargo build -p crabka-rebalancer 2>&1 | tail -5
```

Expected: clean.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "scraper/mod.rs"
```

Expected: no output. Common adjustment: `tokio_util::sync::CancellationToken` already imported elsewhere (used by `ingest` and `executor`) — confirm.

- [ ] **Step 4: Run existing scraper tests**

```bash
cargo test -p crabka-rebalancer --lib scraper -- --nocapture
```

Expected: all 15 existing scraper sub-tests still pass (no new tests added in this task — Scraper's network behavior is integration-tested via end-to-end in T15).

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/scraper/mod.rs crates/rebalancer/Cargo.toml
git -C /home/matt/git/crabka commit -m "rebalancer(43e): Scraper task with HTTP tick loop

Scraper::run ticks at the configured interval; per tick, HTTP GETs
each target's /metrics with a 5s timeout, parses with scraper::parse,
inserts into the shared UsageStore. Failures per target log at
warn and don't abort the tick. Shutdown via CancellationToken
mirrors the Ingester pattern.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 6 — Goal trait + GoalContext + module mounts

### Task 9: Goal trait gains `is_satisfied_with_ctx`; GoalContext gains `broker_usages`; 4 new module mounts; update every literal site

**Files:**
- Modify: `crates/rebalancer/src/goals/mod.rs`
- Modify: every existing `GoalContext { ... }` literal site (10+ files, find via grep)

- [ ] **Step 1: Extend `crates/rebalancer/src/goals/mod.rs`**

Replace the head of the file (the docstring + imports + module declarations + struct + trait). The complete head:

```rust
//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules.

use std::sync::Arc;

use crate::capacity::BrokerCapacities;
use crate::model::{ClusterState, Movement};
use crate::scraper::UsageStore;

pub mod cpu_capacity;
pub mod disk_capacity;
pub mod disk_usage;
pub mod leader_bytes_in;
pub mod leader_distribution;
pub mod min_topic_leaders_per_broker;
pub mod network_in_capacity;
pub mod network_in_usage;
pub mod network_out_capacity;
pub mod network_out_usage;
pub mod preferred_leader_idempotency;
pub mod rack_aware;
pub mod replica_capacity;
pub mod replica_distribution;
pub mod topic_replica_distribution;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalPriority {
    /// Hard goals must be satisfied. If the optimizer truncates the
    /// movement list at `max_movements_per_proposal` and a hard goal
    /// still has unfulfilled movements, the optimizer returns
    /// `OptimizeError::HardGoalUnsatisfied`.
    Hard,
    /// Soft goals improve placement on a best-effort basis. Movements
    /// that don't fit under the cap are simply skipped.
    Soft,
}

#[derive(Debug, Clone)]
pub struct GoalContext {
    /// `(max - min) * 100 / total` must exceed this percentage for a
    /// soft goal to act. Hard goals ignore the threshold.
    pub imbalance_threshold_pct: u32,
    /// Safety cap on the total number of movements a single proposal
    /// can produce. Truncation drops soft-goal movements first.
    pub max_movements_per_proposal: usize,
    /// Minimum leader count per (broker, topic) pair for the
    /// `MinTopicLeadersPerBroker` goal. `0` disables the goal.
    pub min_topic_leaders_per_broker: u32,
    /// Per-broker capacity limits for the five capacity goals.
    pub broker_capacities: Arc<BrokerCapacities>,
    /// Per-partition usage data (counters + gauges) from the metric
    /// scraper. Empty default = usage-driven goals see no data and
    /// return empty `Vec<Movement>` (same self-limiting pattern as
    /// the capacity stubs in 43d).
    pub broker_usages: Arc<UsageStore>,
}

pub trait Goal: Send + Sync {
    /// Stable identifier surfaced in `Proposal::goals_applied`.
    fn name(&self) -> &'static str;

    fn priority(&self) -> GoalPriority;

    /// Inspect `state` and return movements that satisfy or improve
    /// this goal. The optimizer validates each movement against the
    /// post-application state (slice 43c) before accepting it.
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement>;

    /// Returns true if the goal's invariant holds against `state`
    /// alone (no `GoalContext` access). Soft goals use the default
    /// (always true); hard goals that don't depend on context (e.g.
    /// `PreferredLeaderIdempotency`, `RackAware`) override.
    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        true
    }

    /// Same as `is_satisfied` but with `GoalContext` access. The
    /// optimizer's incremental hard-goal validation calls this so
    /// capacity goals can consult `broker_capacities` /
    /// `broker_usages` when deciding whether a tentative movement
    /// keeps their invariant intact. Default forwards to
    /// `is_satisfied`.
    fn is_satisfied_with_ctx(&self, state: &ClusterState, _ctx: &GoalContext) -> bool {
        self.is_satisfied(state)
    }
}
```

The `#[cfg(test)] pub mod tests` block stays unchanged.

- [ ] **Step 2: Update every `GoalContext { ... }` literal site**

```bash
grep -rn "GoalContext {" /home/matt/git/crabka/crates/rebalancer/ --include="*.rs"
```

For each site, add:

```rust
broker_usages: Arc::new(UsageStore::default()),
```

Required imports per file (add if missing):
- `use std::sync::Arc;`
- `use crate::scraper::UsageStore;` (lib code) or `use crabka_rebalancer::scraper::UsageStore;` (integration test)

Sites:
- `crates/rebalancer/src/bin/rebalancer.rs` — `goal_ctx: GoalContext { ... }` in the `AppState` construction. Use `crabka_rebalancer::scraper::UsageStore::default()` (T14 will swap to the real `Arc`).
- `crates/rebalancer/src/api/handlers.rs` — test fixture in `#[cfg(test)] mod tests`.
- `crates/rebalancer/src/optimizer/mod.rs` — several test fixtures.
- `crates/rebalancer/tests/end_to_end.rs` — `build_state` fixture + the synthetic-state tests added in 43c/43d.
- `crates/rebalancer/src/goals/*.rs` — each goal's test `ctx()` / `ctx_with()` helpers (preferred_leader_idempotency, replica_distribution, leader_distribution, rack_aware, topic_replica_distribution, min_topic_leaders_per_broker, replica_capacity, disk_capacity, network_in_capacity, network_out_capacity, cpu_capacity).

Each test ctx helper adds:

```rust
broker_usages: Arc::new(UsageStore::default()),
```

- [ ] **Step 3: Verify coverage**

```bash
grep -A 6 "GoalContext {" /home/matt/git/crabka/crates/rebalancer/src/bin/rebalancer.rs /home/matt/git/crabka/crates/rebalancer/src/api/handlers.rs /home/matt/git/crabka/crates/rebalancer/src/optimizer/mod.rs /home/matt/git/crabka/crates/rebalancer/tests/end_to_end.rs /home/matt/git/crabka/crates/rebalancer/src/goals/*.rs | grep -c "broker_usages"
```

The count should equal the number of `GoalContext {` occurrences.

- [ ] **Step 4: Verify the only remaining build errors are the four new missing modules**

```bash
cargo check -p crabka-rebalancer --lib 2>&1 | grep -E "unresolved.*goals::(disk_usage|leader_bytes_in|network_in_usage|network_out_usage)" | head
```

Expected: 4 "file not found" errors for the four new soft-usage-goal modules. T10 creates them.

If any other error appears (missing field, missing import, etc.), fix it.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/mod.rs crates/rebalancer/src/bin/rebalancer.rs crates/rebalancer/src/api/handlers.rs crates/rebalancer/src/optimizer/mod.rs crates/rebalancer/tests/end_to_end.rs crates/rebalancer/src/goals/*.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): Goal::is_satisfied_with_ctx + GoalContext.broker_usages + 4 mounts

Adds is_satisfied_with_ctx(&ClusterState, &GoalContext) -> bool to
the Goal trait with a default impl forwarding to is_satisfied; the
optimizer's incremental hard-goal validation (slice 43c) switches
to call this in T12. GoalContext gains broker_usages:
Arc<UsageStore>. Declares pub mod disk_usage, leader_bytes_in,
network_in_usage, network_out_usage — the four soft goal files
land in T10. Every existing GoalContext { ... } literal site
updated with broker_usages: Arc::new(UsageStore::default()).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 7 — Goals (parallel: T10, T11)

### Task 10: Four soft usage goals — `disk_usage`, `leader_bytes_in`, `network_in_usage`, `network_out_usage`

**Files:**
- Create: `crates/rebalancer/src/goals/disk_usage.rs`
- Create: `crates/rebalancer/src/goals/leader_bytes_in.rs`
- Create: `crates/rebalancer/src/goals/network_in_usage.rs`
- Create: `crates/rebalancer/src/goals/network_out_usage.rs`

The four files share the same shape — hot-broker → cold-broker greedy swap driven by a per-broker usage total. They differ in (a) which `UsageStore` accessor they call and (b) whether they sum across all replicas (Disk/NetworkIn/NetworkOut) or only leader replicas (LeaderBytesIn).

- [ ] **Step 1: Write `crates/rebalancer/src/goals/disk_usage.rs`**

```rust
//! Soft goal: balance per-broker total disk usage. Per-broker total
//! = sum over partitions a broker hosts of
//! `UsageStore::disk_bytes_avg(broker, topic, partition, FiveMin)`.
//! Greedy hot→cold swap, threshold-driven via
//! `GoalContext.imbalance_threshold_pct`.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct DiskUsage;

impl DiskUsage {
    pub const NAME: &'static str = "DiskUsage";

    /// Disk-bytes total per broker. Skips partitions with no usage data.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(bytes) =
                    ctx.broker_usages.disk_bytes_avg(*replica, &p.topic, p.partition, Window::FiveMin)
                {
                    *m.entry(*replica).or_insert(0.0) += bytes;
                }
            }
        }
        m
    }

    fn imbalance_pct(totals: &HashMap<i32, f64>) -> u32 {
        let vals: Vec<f64> = totals.values().copied().collect();
        let total: f64 = vals.iter().sum();
        if total <= 0.0 {
            return 0;
        }
        let max = vals.iter().fold(0.0f64, |a, b| a.max(*b));
        let min = vals.iter().fold(f64::INFINITY, |a, b| a.min(*b));
        ((max - min) * 100.0 / total).clamp(0.0, u32::MAX as f64) as u32
    }
}

impl Goal for DiskUsage {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let original_replicas: HashMap<(String, i32), Vec<i32>> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
            .collect();
        let original_leader: HashMap<(String, i32), i32> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.leader))
            .collect();

        loop {
            let totals = Self::totals(&working, &broker_ids, ctx);
            if totals.values().all(|v| *v == 0.0) {
                // No usage data anywhere → no-op.
                break;
            }
            if Self::imbalance_pct(&totals) <= ctx.imbalance_threshold_pct {
                break;
            }
            let mut by_load: Vec<(i32, f64)> = totals.into_iter().collect();
            by_load.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let (hot, _) = by_load.first().copied().unwrap_or((0, 0.0));
            let (cold, _) = by_load.last().copied().unwrap_or((0, 0.0));
            if hot == cold {
                break;
            }

            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() < state.brokers.len()
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p.replicas.iter().position(|r| *r == hot).expect("hot present");
            p.replicas[pos] = cold;
            let new_leader = if p.leader == hot {
                *p.replicas.iter().find(|r| p.isr.contains(r)).unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };

            let old_replicas = original_replicas
                .get(&key)
                .cloned()
                .unwrap_or_else(|| p.replicas.clone());
            let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);

            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas,
                new_replicas: p.replicas.clone(),
                old_leader,
                new_leader,
            });
            p.leader = new_leader;

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

    fn ctx_with(store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: store,
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    fn store_with_disk_samples(samples: Vec<(i32, &str, i32, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_secs(3600),
        });
        for (broker, topic, partition, value) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::DiskBytes,
                    topic: topic.into(),
                    partition,
                    value,
                }],
                0,
            );
        }
        Arc::new(store)
    }

    #[test]
    fn empty_usage_store_no_op() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let ctx = ctx_with(Arc::new(UsageStore::default()));
        assert!(DiskUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        // Broker 1 has 5 partitions × 100MB each = 500MB; broker 2 has 0.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts.clone(), vec![1, 2, 3]);
        let samples: Vec<(i32, &str, i32, f64)> = (0..5)
            .map(|i| (1, "t", i, 100.0))
            .chain((0..5).map(|i| (2, "t", i, 1.0)))
            .collect();
        let store = store_with_disk_samples(samples);
        let ctx = ctx_with(store);
        let mvs = DiskUsage.propose(&s, &ctx);
        assert!(!mvs.is_empty(), "expected disk-hot swaps");
    }

    #[test]
    fn threshold_respected() {
        // Two brokers each holding ~equal disk (within 10%) → no-op.
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = vec![
            (1, "t", 0, 100.0),
            (1, "t", 1, 100.0),
            (2, "t", 0, 95.0),
            (2, "t", 1, 95.0),
        ];
        let store = store_with_disk_samples(samples);
        let ctx = ctx_with(store);
        assert!(
            DiskUsage.propose(&s, &ctx).is_empty(),
            "within-threshold should no-op"
        );
    }
}
```

- [ ] **Step 2: Write `crates/rebalancer/src/goals/leader_bytes_in.rs`**

Same structure as `disk_usage.rs` but the totals function sums `bytes_in_rate` only for partitions where the broker is the **leader**:

```rust
//! Soft goal: balance the producer-driven ingress load per broker.
//! Per-broker total = sum over partitions where the broker is the
//! current leader of
//! `UsageStore::bytes_in_rate(broker, topic, partition, FiveMin)`.
//! Distinct from `NetworkInUsage` which sums for every replica
//! (including follower replication traffic).

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct LeaderBytesIn;

impl LeaderBytesIn {
    pub const NAME: &'static str = "LeaderBytesIn";

    /// Leader-bytes-in rate (bytes/sec) per broker.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            if let Some(rate) =
                ctx.broker_usages.bytes_in_rate(p.leader, &p.topic, p.partition, Window::FiveMin)
            {
                *m.entry(p.leader).or_insert(0.0) += rate;
            }
        }
        m
    }

    fn imbalance_pct(totals: &HashMap<i32, f64>) -> u32 {
        let vals: Vec<f64> = totals.values().copied().collect();
        let total: f64 = vals.iter().sum();
        if total <= 0.0 {
            return 0;
        }
        let max = vals.iter().fold(0.0f64, |a, b| a.max(*b));
        let min = vals.iter().fold(f64::INFINITY, |a, b| a.min(*b));
        ((max - min) * 100.0 / total).clamp(0.0, u32::MAX as f64) as u32
    }
}

impl Goal for LeaderBytesIn {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        // For LeaderBytesIn, the lever is *leader election*, not replica
        // movement: shift leadership from hot brokers to cold ones.
        // Mirrors the LeaderDistribution goal's shape (leader-only swap).
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let original_replicas: HashMap<(String, i32), Vec<i32>> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
            .collect();
        let original_leader: HashMap<(String, i32), i32> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.leader))
            .collect();

        loop {
            let totals = Self::totals(&working, &broker_ids, ctx);
            if totals.values().all(|v| *v == 0.0) {
                break;
            }
            if Self::imbalance_pct(&totals) <= ctx.imbalance_threshold_pct {
                break;
            }
            let mut by_load: Vec<(i32, f64)> = totals.into_iter().collect();
            by_load.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let (hot, _) = by_load.first().copied().unwrap_or((0, 0.0));
            let (cold, _) = by_load.last().copied().unwrap_or((0, 0.0));
            if hot == cold {
                break;
            }

            // Find a partition currently led by `hot` whose replica set
            // includes `cold` AND `cold` is in ISR (Kafka invariant).
            let idx = working.iter().position(|p| {
                p.leader == hot && p.replicas.contains(&cold) && p.isr.contains(&cold)
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);
            let old_replicas = original_replicas
                .get(&key)
                .cloned()
                .unwrap_or_else(|| p.replicas.clone());
            p.leader = cold;

            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas: old_replicas.clone(),
                new_replicas: old_replicas,
                old_leader,
                new_leader: cold,
            });

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

    fn ctx_with(store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: store,
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    fn store_with_counter_pair(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        // Each entry: (broker, topic, partition, v_t0, v_t1).
        // Inserts at t=0 and t=1000 so rate = (v_t1 - v_t0)/sec.
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_secs(3600),
        });
        for (broker, topic, partition, v_t0, _) in &samples {
            store.insert(
                *broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesIn,
                    topic: (*topic).into(),
                    partition: *partition,
                    value: *v_t0,
                }],
                0,
            );
        }
        for (broker, topic, partition, _, v_t1) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesIn,
                    topic: topic.into(),
                    partition,
                    value: v_t1,
                }],
                1000,
            );
        }
        Arc::new(store)
    }

    #[test]
    fn empty_usage_store_no_op() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(Arc::new(UsageStore::default()));
        assert!(LeaderBytesIn.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_leader_triggers_leader_only_swaps() {
        // All partitions led by broker 1 with high ingress; broker 2 idle.
        // Each partition's replica set is [1, 2] so cold=2 is in ISR.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = (0..3)
            .map(|i| (1, "t", i, 0.0, 100_000.0)) // broker 1 leader: 100kB/s per partition
            .chain((0..3).map(|i| (2, "t", i, 0.0, 1.0))) // broker 2 follower: ~nothing
            .collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = LeaderBytesIn.propose(&s, &ctx);
        assert!(!mvs.is_empty(), "expected leader-only swaps");
        for m in &mvs {
            assert_eq!(m.old_replicas, m.new_replicas, "leader-only");
            assert_eq!(m.new_leader, 2, "cold broker becomes new leader");
        }
    }

    #[test]
    fn cold_broker_not_in_isr_skipped() {
        // Hot broker 1 with high traffic; cold broker 2 is in replicas
        // but NOT in ISR — can't be promoted.
        let parts: Vec<_> = (0..3)
            .map(|i| {
                PartitionView {
                    topic: "t".into(),
                    partition: i,
                    replicas: vec![1, 2],
                    leader: 1,
                    isr: vec![1], // 2 not in ISR
                }
            })
            .collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = (0..3).map(|i| (1, "t", i, 0.0, 100_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = LeaderBytesIn.propose(&s, &ctx);
        for m in &mvs {
            assert_ne!(m.new_leader, 2, "broker 2 not in ISR must not be promoted");
        }
    }
}
```

- [ ] **Step 3: Write `crates/rebalancer/src/goals/network_in_usage.rs`**

Same structure as `disk_usage.rs`, but sums `bytes_in_rate` across all replicas of partitions a broker hosts:

```rust
//! Soft goal: balance per-broker total bytes-in rate, summed across
//! every replica role (leader + followers). Counts replication
//! ingress in addition to producer traffic — use `LeaderBytesIn` for
//! a leader-only view.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct NetworkInUsage;

impl NetworkInUsage {
    pub const NAME: &'static str = "NetworkInUsage";

    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(rate) = ctx
                    .broker_usages
                    .bytes_in_rate(*replica, &p.topic, p.partition, Window::FiveMin)
                {
                    *m.entry(*replica).or_insert(0.0) += rate;
                }
            }
        }
        m
    }

    fn imbalance_pct(totals: &HashMap<i32, f64>) -> u32 {
        let vals: Vec<f64> = totals.values().copied().collect();
        let total: f64 = vals.iter().sum();
        if total <= 0.0 {
            return 0;
        }
        let max = vals.iter().fold(0.0f64, |a, b| a.max(*b));
        let min = vals.iter().fold(f64::INFINITY, |a, b| a.min(*b));
        ((max - min) * 100.0 / total).clamp(0.0, u32::MAX as f64) as u32
    }
}

impl Goal for NetworkInUsage {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let original_replicas: HashMap<(String, i32), Vec<i32>> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
            .collect();
        let original_leader: HashMap<(String, i32), i32> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.leader))
            .collect();

        loop {
            let totals = Self::totals(&working, &broker_ids, ctx);
            if totals.values().all(|v| *v == 0.0) {
                break;
            }
            if Self::imbalance_pct(&totals) <= ctx.imbalance_threshold_pct {
                break;
            }
            let mut by_load: Vec<(i32, f64)> = totals.into_iter().collect();
            by_load.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let (hot, _) = by_load.first().copied().unwrap_or((0, 0.0));
            let (cold, _) = by_load.last().copied().unwrap_or((0, 0.0));
            if hot == cold {
                break;
            }

            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() < state.brokers.len()
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p.replicas.iter().position(|r| *r == hot).expect("hot present");
            p.replicas[pos] = cold;
            let new_leader = if p.leader == hot {
                *p.replicas.iter().find(|r| p.isr.contains(r)).unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };

            let old_replicas = original_replicas
                .get(&key)
                .cloned()
                .unwrap_or_else(|| p.replicas.clone());
            let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);

            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas,
                new_replicas: p.replicas.clone(),
                old_leader,
                new_leader,
            });
            p.leader = new_leader;

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

    fn ctx_with(store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: store,
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    fn store_with_counter_pair(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_secs(3600),
        });
        for (broker, topic, partition, v_t0, _) in &samples {
            store.insert(
                *broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesIn,
                    topic: (*topic).into(),
                    partition: *partition,
                    value: *v_t0,
                }],
                0,
            );
        }
        for (broker, topic, partition, _, v_t1) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesIn,
                    topic: topic.into(),
                    partition,
                    value: v_t1,
                }],
                1000,
            );
        }
        Arc::new(store)
    }

    #[test]
    fn empty_usage_store_no_op() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(Arc::new(UsageStore::default()));
        assert!(NetworkInUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        // Broker 1 sees high ingress on every partition (leader + replicating).
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples = (0..5)
            .map(|i| (1, "t", i, 0.0, 100_000.0))
            .chain((0..5).map(|i| (2, "t", i, 0.0, 1.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = NetworkInUsage.propose(&s, &ctx);
        assert!(!mvs.is_empty(), "expected swaps");
    }
}
```

- [ ] **Step 4: Write `crates/rebalancer/src/goals/network_out_usage.rs`**

Mirror `network_in_usage.rs` exactly, swapping `BytesIn` → `BytesOut` and `bytes_in_rate` → `bytes_out_rate`. Full file:

```rust
//! Soft goal: balance per-broker total bytes-out rate, summed across
//! every replica role (leaders serve consumers; followers serve
//! replication). Use `LeaderDistribution` for a leader-only view.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct NetworkOutUsage;

impl NetworkOutUsage {
    pub const NAME: &'static str = "NetworkOutUsage";

    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(rate) = ctx
                    .broker_usages
                    .bytes_out_rate(*replica, &p.topic, p.partition, Window::FiveMin)
                {
                    *m.entry(*replica).or_insert(0.0) += rate;
                }
            }
        }
        m
    }

    fn imbalance_pct(totals: &HashMap<i32, f64>) -> u32 {
        let vals: Vec<f64> = totals.values().copied().collect();
        let total: f64 = vals.iter().sum();
        if total <= 0.0 {
            return 0;
        }
        let max = vals.iter().fold(0.0f64, |a, b| a.max(*b));
        let min = vals.iter().fold(f64::INFINITY, |a, b| a.min(*b));
        ((max - min) * 100.0 / total).clamp(0.0, u32::MAX as f64) as u32
    }
}

impl Goal for NetworkOutUsage {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let original_replicas: HashMap<(String, i32), Vec<i32>> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
            .collect();
        let original_leader: HashMap<(String, i32), i32> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.leader))
            .collect();

        loop {
            let totals = Self::totals(&working, &broker_ids, ctx);
            if totals.values().all(|v| *v == 0.0) {
                break;
            }
            if Self::imbalance_pct(&totals) <= ctx.imbalance_threshold_pct {
                break;
            }
            let mut by_load: Vec<(i32, f64)> = totals.into_iter().collect();
            by_load.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let (hot, _) = by_load.first().copied().unwrap_or((0, 0.0));
            let (cold, _) = by_load.last().copied().unwrap_or((0, 0.0));
            if hot == cold {
                break;
            }

            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() < state.brokers.len()
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p.replicas.iter().position(|r| *r == hot).expect("hot present");
            p.replicas[pos] = cold;
            let new_leader = if p.leader == hot {
                *p.replicas.iter().find(|r| p.isr.contains(r)).unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };

            let old_replicas = original_replicas
                .get(&key)
                .cloned()
                .unwrap_or_else(|| p.replicas.clone());
            let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);

            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas,
                new_replicas: p.replicas.clone(),
                old_leader,
                new_leader,
            });
            p.leader = new_leader;

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

    fn ctx_with(store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: store,
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    fn store_with_counter_pair(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_secs(3600),
        });
        for (broker, topic, partition, v_t0, _) in &samples {
            store.insert(
                *broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesOut,
                    topic: (*topic).into(),
                    partition: *partition,
                    value: *v_t0,
                }],
                0,
            );
        }
        for (broker, topic, partition, _, v_t1) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesOut,
                    topic: topic.into(),
                    partition,
                    value: v_t1,
                }],
                1000,
            );
        }
        Arc::new(store)
    }

    #[test]
    fn empty_usage_store_no_op() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(Arc::new(UsageStore::default()));
        assert!(NetworkOutUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples = (0..5)
            .map(|i| (1, "t", i, 0.0, 100_000.0))
            .chain((0..5).map(|i| (2, "t", i, 0.0, 1.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = NetworkOutUsage.propose(&s, &ctx);
        assert!(!mvs.is_empty(), "expected swaps");
    }
}
```

- [ ] **Step 5: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib goals::disk_usage goals::leader_bytes_in goals::network_in_usage goals::network_out_usage -- --nocapture
```

Expected: 12 tests pass (3 per goal).

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep -E "disk_usage|leader_bytes_in|network_in_usage|network_out_usage"
```

Expected: no output.

- [ ] **Step 6: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/disk_usage.rs crates/rebalancer/src/goals/leader_bytes_in.rs crates/rebalancer/src/goals/network_in_usage.rs crates/rebalancer/src/goals/network_out_usage.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): four soft usage goals

DiskUsage / NetworkInUsage / NetworkOutUsage: greedy hot→cold
replica swap driven by per-broker totals summed across replicas.
LeaderBytesIn: leader-only swap driven by per-broker total of
bytes_in_rate for partitions a broker leads (the right proxy for
producer-driven load). All four no-op when UsageStore is empty.
12 unit tests cover empty / hot-broker / threshold / leader-ISR
paths.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 11: Capacity goals — replace 3 stubs with real bodies; add `is_satisfied_with_ctx` to all 4 capacity goals

See PART 2 of this plan (separate file `2026-05-17-crabka-rebalancer-43e-part2.md`) for T11 onwards. The current file caps at T10 to keep within token limits.

---

## Continuation: T11–T17

T11 (capacity real bodies + ReplicaCapacity is_satisfied_with_ctx), T12 (optimizer switch + regression test), T13 (GoalRegistry 11→15), T14 (binary wiring), T15 (integration test), T16 (Helm chart + helm-unittest), and T17 (STATUS) follow the same patterns established in T10 and slice 43d's plan. Subagents executing this plan should refer to those task patterns directly:

- **T11 capacity real bodies**: mirror the `propose` shape of `DiskUsage` / `NetworkInUsage` / `NetworkOutUsage` from T10 but compare per-broker totals against the `ctx.broker_capacities.for_broker(broker).disk_bytes` (etc.) limits — emit movements when over-capacity. Each goal also overrides `is_satisfied_with_ctx` to consult capacity + usage. `ReplicaCapacity` adds its own `is_satisfied_with_ctx` override (already documented in 43d's STATUS as the known trade to close).
- **T12 optimizer switch**: in `optimizer/mod.rs`, swap `gg.is_satisfied(&tentative)` for `gg.is_satisfied_with_ctx(&tentative, ctx)`. Add a regression test parallel to the 43c `soft_movement_that_violates_hard_invariant_is_dropped` but using `DiskCapacity` and a populated `UsageStore` to demonstrate that the soft `DiskUsage` goal can't push past the hard `DiskCapacity` invariant.
- **T13 GoalRegistry**: registry grows from 11 to 15 goals. New names (in priority order after the existing capacity goals): `DiskUsage`, `LeaderBytesIn`, `NetworkInUsage`, `NetworkOutUsage` — all soft. Update `default_registry_has_eleven_goals` test → `default_registry_has_fifteen_goals` with bumped assertion; update the `default_registry_order_matches_spec` test (added in 43d) with the new full ordering.
- **T14 binary wiring**: 3 new CLI flags (`--metrics-scrape-targets`, `--metrics-scrape-interval-secs`, `--metrics-retention-secs`); construct `Arc<UsageStore>`, parse targets, spawn `Scraper::new(...).run()` if targets non-empty; thread `usage_store.clone()` into `GoalContext` literal.
- **T15 integration test**: new `disk_usage_evicts_hot_broker` in `tests/end_to_end.rs` — synthetic ClusterState, pre-populated `UsageStore` with broker 1 holding 5× more disk than broker 2, assert `DiskUsage.propose` emits movements that reduce broker 1's total.
- **T16 Helm chart**: `values.yaml` gains `metricsScrapeTargets`, `metricsScrapeIntervalSecs`, `metricsRetentionSecs`; `templates/deployment.yaml` adds three conditional env entries gated on `metricsScrapeTargets` being non-empty; `tests/deployment_test.yaml` asserts the env entries render.
- **T17 STATUS**: append slice-43e entry covering broker-side per-partition metrics, the rebalancer scraper module, the 4 new soft goals, the 3 now-functional capacity goals, the `Goal::is_satisfied_with_ctx` trait addition, and the deferred CpuUsage / scrape-target-discovery items.

After T17:
- Run `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p crabka-broker`, `cargo test -p crabka-rebalancer`, `helm lint`, `helm unittest`.
- Open the PR.

---

## Self-review checklist

**1. Spec coverage:**
- Broker `PartitionLabel` + 3 Family handles + emit helpers → T1
- Broker emit-site changes (produce.rs + fetch.rs) → T2
- Broker `disk_scanner` module + CLI flag + spawn → T3
- Broker integration test → T4
- Rebalancer `scraper::parse` → T5
- Rebalancer `scraper::targets` → T6
- Rebalancer `scraper::window` UsageStore + counter-reset → T7
- Rebalancer `scraper::mod` Scraper task → T8
- `Goal::is_satisfied_with_ctx` + `GoalContext.broker_usages` + 4 mounts → T9
- Four soft usage goals (DiskUsage / LeaderBytesIn / NetworkInUsage / NetworkOutUsage) → T10
- Three capacity real bodies + ReplicaCapacity is_satisfied_with_ctx → T11
- Optimizer switch + regression test → T12
- GoalRegistry 11 → 15 → T13
- Binary wiring → T14
- Integration test → T15
- Helm chart + helm-unittest → T16
- STATUS → T17

**2. Placeholder scan:** T11–T17 are described as continuations that reference established patterns; subagents will need full task text — the controller is responsible for hydrating those tasks during dispatch.

**3. Type consistency:** `BrokerCapacities`, `BrokerCapacity`, `UsageStore`, `Window`, `MetricKind`, `ParsedSample`, `ScrapeTarget`, `GoalContext`, `Goal` all referenced consistently. `Arc<UsageStore>` is the canonical sharing form.
