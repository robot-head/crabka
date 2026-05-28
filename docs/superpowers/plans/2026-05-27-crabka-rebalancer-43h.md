# Rebalancer 43h — Scrape-target discovery via Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `crabka-rebalancer` discover its metrics-scrape targets from the ingester's `MetadataResponse` snapshot, with a `--metrics-port` flag (default 9404) supplying the metrics-endpoint port. Keep `--metrics-scrape-targets` as an escape hatch.

**Architecture:** Add a `TargetSource` enum (`Static(Vec<ScrapeTarget>)` | `Discovered { snapshot, metrics_port }`) in `scraper/targets.rs`. The scraper stores a `TargetSource` instead of a `Vec<ScrapeTarget>` and calls `source.current()` at the top of each tick. The binary entry picks between `Static` (when `--metrics-scrape-targets` is set) and `Discovered` (default).

**Tech Stack:** Rust 1.95, `arc_swap::ArcSwap`, `tokio` runtime, `clap` derive CLI, existing `crabka-rebalancer` workspace member.

**Spec:** `docs/superpowers/specs/2026-05-27-crabka-rebalancer-43h-design.md`

**Branch:** Create a new branch off `main` named `rebalancer-43h`.

---

## Pre-flight: branch + baseline

- [ ] **Step 1: Create the branch on main**

```bash
git checkout main && git pull --ff-only
git checkout -b rebalancer-43h
```

- [ ] **Step 2: Verify the rebalancer crate builds + tests pass**

```bash
cargo test -p crabka-rebalancer --lib 2>&1 | tail -10
```

Expected: existing tests pass. Capture the baseline so test deltas in later tasks are easy to spot.

---

## Task 1: Add `TargetSource` enum + `current()` + unit tests

**Files:**
- Modify: `crates/rebalancer/src/scraper/targets.rs` (append new enum + impl + tests; existing `ScrapeTarget`, `TargetParseError`, `parse_targets` are unchanged)

### Step 1: Read the existing file to anchor

```bash
sed -n '1,15p' crates/rebalancer/src/scraper/targets.rs
```

You should see the existing `ScrapeTarget`, `TargetParseError`, `parse_targets`. Nothing in this task touches them.

### Step 2: Add the `TargetSource` enum + `current()` impl

Append to `crates/rebalancer/src/scraper/targets.rs`:

```rust
use std::sync::Arc;
use std::sync::OnceLock;

use arc_swap::ArcSwap;
use tracing::warn;

use crate::model::ClusterState;

/// Where the scraper finds its targets each tick.
///
/// `Static` matches the pre-43h behavior (explicit `id:host:port` list
/// from `--metrics-scrape-targets`). `Discovered` reads from the
/// ingester's `ClusterState` snapshot and synthesizes targets at
/// `host:metrics_port` for every broker in the snapshot.
pub enum TargetSource {
    Static(Vec<ScrapeTarget>),
    Discovered {
        snapshot: Arc<ArcSwap<Option<ClusterState>>>,
        metrics_port: u16,
    },
}

impl TargetSource {
    /// Materialize the current set of scrape targets.
    ///
    /// Called by the scraper's main loop each tick. Cheap: the `Static`
    /// arm clones a small `Vec`; the `Discovered` arm reads the snapshot
    /// guard and emits one `ScrapeTarget` per broker (skipping brokers
    /// with empty `host`).
    #[must_use]
    pub fn current(&self) -> Vec<ScrapeTarget> {
        match self {
            Self::Static(targets) => targets.clone(),
            Self::Discovered {
                snapshot,
                metrics_port,
            } => {
                let guard = snapshot.load();
                let state: &Option<ClusterState> = &guard;
                let Some(state) = state.as_ref() else {
                    return Vec::new();
                };
                let mut out = Vec::with_capacity(state.brokers.len());
                for b in &state.brokers {
                    if b.host.is_empty() {
                        warn_once_empty_host(b.id);
                        continue;
                    }
                    out.push(ScrapeTarget {
                        broker_id: b.id,
                        addr: format!("{}:{}", b.host, metrics_port),
                    });
                }
                out
            }
        }
    }
}

/// One-time WARN per broker_id when the broker advertises an empty host.
fn warn_once_empty_host(broker_id: i32) {
    static SEEN: OnceLock<std::sync::Mutex<std::collections::HashSet<i32>>> = OnceLock::new();
    let mut seen = SEEN
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
        .lock()
        .expect("empty-host seen-set");
    if seen.insert(broker_id) {
        warn!(
            broker_id,
            "broker advertises empty host in metadata; skipping in scrape discovery"
        );
    }
}
```

If `arc_swap` is not yet imported at the top of the file, the new `use arc_swap::ArcSwap;` introduces it. It's already a workspace dep used by `crates/rebalancer/src/ingest/mod.rs`.

### Step 3: Add unit tests at the bottom of the existing `#[cfg(test)] mod tests` block (or in a new block)

Append to the same file's test module:

```rust
#[cfg(test)]
mod target_source_tests {
    use super::*;
    use crate::model::{BrokerView, ClusterState, InFlightReassignment, PartitionView};

    fn cluster_state_with(brokers: Vec<BrokerView>) -> ClusterState {
        ClusterState {
            cluster_id: Some("test-cluster".into()),
            snapshot_at_ms: 0,
            brokers,
            partitions: Vec::<PartitionView>::new(),
            in_flight_reassignments: Vec::<InFlightReassignment>::new(),
        }
    }

    #[test]
    fn static_source_returns_underlying_list() {
        let targets = vec![
            ScrapeTarget { broker_id: 1, addr: "h1:9404".into() },
            ScrapeTarget { broker_id: 2, addr: "h2:9404".into() },
        ];
        let src = TargetSource::Static(targets.clone());
        assert_eq!(src.current(), targets);
    }

    #[test]
    fn discovered_source_with_no_snapshot_returns_empty() {
        let snapshot: Arc<ArcSwap<Option<ClusterState>>> =
            Arc::new(ArcSwap::from_pointee(None));
        let src = TargetSource::Discovered {
            snapshot,
            metrics_port: 9404,
        };
        assert!(src.current().is_empty());
    }

    #[test]
    fn discovered_source_emits_one_target_per_broker() {
        let state = cluster_state_with(vec![
            BrokerView { id: 1, host: "broker1".into(), port: 9092, rack: None },
            BrokerView { id: 2, host: "broker2".into(), port: 9092, rack: None },
            BrokerView { id: 3, host: "broker3".into(), port: 9092, rack: None },
        ]);
        let snapshot = Arc::new(ArcSwap::from_pointee(Some(state)));
        let src = TargetSource::Discovered { snapshot, metrics_port: 9404 };
        let mut out = src.current();
        out.sort_by_key(|t| t.broker_id);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], ScrapeTarget { broker_id: 1, addr: "broker1:9404".into() });
        assert_eq!(out[1], ScrapeTarget { broker_id: 2, addr: "broker2:9404".into() });
        assert_eq!(out[2], ScrapeTarget { broker_id: 3, addr: "broker3:9404".into() });
    }

    #[test]
    fn discovered_source_skips_brokers_with_empty_host() {
        let state = cluster_state_with(vec![
            BrokerView { id: 1, host: "broker1".into(), port: 9092, rack: None },
            BrokerView { id: 2, host: "".into(),       port: 9092, rack: None },
            BrokerView { id: 3, host: "broker3".into(), port: 9092, rack: None },
        ]);
        let snapshot = Arc::new(ArcSwap::from_pointee(Some(state)));
        let src = TargetSource::Discovered { snapshot, metrics_port: 9404 };
        let mut out = src.current();
        out.sort_by_key(|t| t.broker_id);
        assert_eq!(out.len(), 2);
        assert_eq!(out.iter().map(|t| t.broker_id).collect::<Vec<_>>(), vec![1, 3]);
    }

    #[test]
    fn discovered_source_reflects_snapshot_updates() {
        let snapshot: Arc<ArcSwap<Option<ClusterState>>> =
            Arc::new(ArcSwap::from_pointee(None));
        let src = TargetSource::Discovered {
            snapshot: snapshot.clone(),
            metrics_port: 9404,
        };
        assert!(src.current().is_empty());

        // Now publish a snapshot.
        let state = cluster_state_with(vec![BrokerView {
            id: 7,
            host: "newbie".into(),
            port: 9092,
            rack: None,
        }]);
        snapshot.store(Arc::new(Some(state)));

        let out = src.current();
        assert_eq!(out, vec![ScrapeTarget { broker_id: 7, addr: "newbie:9404".into() }]);
    }
}
```

### Step 4: Run the tests

```bash
cargo test -p crabka-rebalancer --lib target_source_tests 2>&1 | tail -10
```

Expected: 5 passed.

### Step 5: Clippy + fmt

```bash
cargo clippy -p crabka-rebalancer --lib --tests -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

### Step 6: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/scraper/targets.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(scraper): TargetSource enum with static + discovered variants"
```

---

## Task 2: Switch `Scraper` to consume `TargetSource`

**Files:**
- Modify: `crates/rebalancer/src/scraper/mod.rs:52–145` (field type, constructor signature, tick body)
- Modify: any test in `scraper/mod.rs` that constructs `Scraper::new` with a `Vec<ScrapeTarget>`

### Step 1: Update the `Scraper` struct + constructor

In `crates/rebalancer/src/scraper/mod.rs`, change the `targets` field type and the constructor's first parameter:

```rust
pub struct Scraper {
    source: TargetSource,
    interval: Duration,
    store: Arc<UsageStore>,
    http: reqwest::Client,
    shutdown: CancellationToken,
    /// Per-broker last-scrape outcome for edge-triggered logging.
    /// Pruned each tick: brokers that disappear from `source.current()`
    /// are dropped on the next iteration.
    last_ok: HashMap<i32, bool>,
}

impl Scraper {
    #[must_use]
    pub fn new(
        source: TargetSource,
        interval: Duration,
        store: Arc<UsageStore>,
        shutdown: CancellationToken,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client");
        Self {
            source,
            interval,
            store,
            http,
            shutdown,
            last_ok: HashMap::new(),
        }
    }
    // ...
}
```

`TargetSource` is already a `pub` re-export from `targets.rs` (via the existing `pub use targets::{ScrapeTarget, ...}` at the top of `mod.rs`); add `TargetSource` to that re-export:

```rust
pub use targets::{ScrapeTarget, TargetParseError, TargetSource, parse_targets};
```

### Step 2: Rewrite `run` and `tick_once`

Replace the `run`'s `target_count` log (was `self.targets.len()`) with a static "scraper started" message since the count is now dynamic:

```rust
pub async fn run(mut self) {
    info!(
        interval_secs = self.interval.as_secs(),
        "scraper started"
    );
    let mut ticker = tokio::time::interval(self.interval);
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

async fn tick_once(&mut self) {
    let now_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0);
    // Refresh targets each tick — `TargetSource::Discovered` may have
    // gained or lost brokers since the last iteration.
    let targets = self.source.current();
    // Prune stale `last_ok` entries: any broker_id no longer in the
    // current target list dropped out of the snapshot or was removed
    // from the static config.
    {
        use std::collections::HashSet;
        let current_ids: HashSet<i32> = targets.iter().map(|t| t.broker_id).collect();
        self.last_ok.retain(|id, _| current_ids.contains(id));
    }
    for target in &targets {
        let url = format!("http://{}/metrics", target.addr);
        let (ok, outcome) = match self.http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => match resp.text().await {
                Ok(body) => {
                    let samples = parse::parse(&body);
                    let count = samples.len();
                    self.store.insert(target.broker_id, samples, now_ms);
                    (true, Outcome::Ok { count })
                }
                Err(e) => (false, Outcome::BodyReadFailed(e.to_string())),
            },
            Ok(resp) => (false, Outcome::NonSuccess(resp.status().to_string())),
            Err(e) => (false, Outcome::TransportFailure(e.to_string())),
        };
        let prev = self.last_ok.insert(target.broker_id, ok);
        Self::log_outcome(target.broker_id, &url, prev, ok, &outcome);
    }
}
```

(The bodies of `log_outcome`, `Outcome`, etc., are unchanged.)

### Step 3: Update existing scraper tests that call `Scraper::new`

```bash
grep -n "Scraper::new(" crates/rebalancer/src/scraper/mod.rs
```

For each call site that passed `vec![...]` (a `Vec<ScrapeTarget>`), wrap with `TargetSource::Static(...)`:

```rust
// Was:
Scraper::new(
    vec![ScrapeTarget { broker_id: 1, addr: server.addr.clone() }],
    Duration::from_millis(50),
    store,
    shutdown,
)
// Now:
Scraper::new(
    TargetSource::Static(vec![ScrapeTarget {
        broker_id: 1,
        addr: server.addr.clone(),
    }]),
    Duration::from_millis(50),
    store,
    shutdown,
)
```

### Step 4: Add a small test that verifies `last_ok` pruning

Append to the existing `#[cfg(test)] mod tests` in `scraper/mod.rs`:

```rust
#[tokio::test]
async fn tick_once_prunes_last_ok_for_brokers_no_longer_in_source() {
    use crate::model::{BrokerView, ClusterState};

    let snapshot: Arc<ArcSwap<Option<ClusterState>>> =
        Arc::new(ArcSwap::from_pointee(Some(ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![
                BrokerView { id: 1, host: "127.0.0.1".into(), port: 1, rack: None },
                BrokerView { id: 2, host: "127.0.0.1".into(), port: 1, rack: None },
            ],
            partitions: vec![],
            in_flight_reassignments: vec![],
        })));

    let store = Arc::new(UsageStore::new(WindowConfig {
        bucket: Duration::from_secs(1),
        retention: Duration::from_secs(60),
    }));
    let mut scraper = Scraper::new(
        TargetSource::Discovered {
            snapshot: snapshot.clone(),
            metrics_port: 1,  // bogus port — scrapes will fail but that's fine
        },
        Duration::from_millis(50),
        store,
        CancellationToken::new(),
    );

    // First tick: scrape both brokers (they'll fail; we don't care).
    scraper.tick_once().await;
    assert_eq!(scraper.last_ok.len(), 2);
    assert!(scraper.last_ok.contains_key(&1));
    assert!(scraper.last_ok.contains_key(&2));

    // Snapshot loses broker 2.
    snapshot.store(Arc::new(Some(ClusterState {
        cluster_id: None,
        snapshot_at_ms: 0,
        brokers: vec![BrokerView { id: 1, host: "127.0.0.1".into(), port: 1, rack: None }],
        partitions: vec![],
        in_flight_reassignments: vec![],
    })));

    scraper.tick_once().await;
    // last_ok should now only contain broker 1.
    assert_eq!(scraper.last_ok.len(), 1);
    assert!(scraper.last_ok.contains_key(&1));
    assert!(!scraper.last_ok.contains_key(&2));
}
```

Note: `last_ok` must be `pub(crate)` for the test to read it; if it's currently private, change the field visibility or add a `#[cfg(test)] pub(crate) fn last_ok(&self) -> &HashMap<i32, bool> { &self.last_ok }` accessor. Pick whichever is more in-line with the file's existing test style; if other tests already touch private fields directly, just bump the field's visibility.

### Step 5: Run the tests

```bash
cargo test -p crabka-rebalancer --lib scraper 2>&1 | tail -15
```

Expected: all scraper tests pass (the existing ones + the new pruning test).

### Step 6: Clippy + fmt

```bash
cargo clippy -p crabka-rebalancer --lib --tests -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

### Step 7: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/scraper/mod.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(scraper): consume TargetSource; refresh targets each tick; prune last_ok"
```

---

## Task 3: Wire discovery in the binary entry

**Files:**
- Modify: `crates/rebalancer/src/bin/rebalancer.rs` (add CLI flag + change scraper-construction block at lines 344–368)

### Step 1: Add the `--metrics-port` CLI flag

In the `Args` struct (around the existing `metrics_scrape_targets` field, ~line 82), add:

```rust
/// Broker metrics-endpoint port used by live scrape-target discovery.
///
/// When `--metrics-scrape-targets` is unset, the scraper derives its
/// target list from the ingester's `Metadata` snapshot, addressing
/// each broker at `host:METRICS_PORT`. Ignored when
/// `--metrics-scrape-targets` is set. Defaults to `crabka-broker`'s
/// slice-39 default (`9404`).
#[arg(
    long,
    env = "CRABKA_REBALANCER_METRICS_PORT",
    default_value_t = 9404
)]
metrics_port: u16,
```

Also update the doc comment on the existing `metrics_scrape_targets` field to mention it overrides `--metrics-port` when set.

### Step 2: Rewrite the scraper-construction block

Replace lines 344–368 (the `if !args.metrics_scrape_targets.is_empty()` block) with:

```rust
let source: crabka_rebalancer::scraper::TargetSource = if !args
    .metrics_scrape_targets
    .trim()
    .is_empty()
{
    let targets = crabka_rebalancer::scraper::parse_targets(&args.metrics_scrape_targets)
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to parse --metrics-scrape-targets `{}`: {e}",
                args.metrics_scrape_targets
            )
        })?;
    info!(
        target_count = targets.len(),
        scrape_interval_secs = args.metrics_scrape_interval_secs,
        retention_secs = args.metrics_retention_secs,
        "starting metrics scraper (static targets)"
    );
    crabka_rebalancer::scraper::TargetSource::Static(targets)
} else {
    info!(
        metrics_port = args.metrics_port,
        scrape_interval_secs = args.metrics_scrape_interval_secs,
        retention_secs = args.metrics_retention_secs,
        "starting metrics scraper (discovered targets via Metadata)"
    );
    crabka_rebalancer::scraper::TargetSource::Discovered {
        snapshot: ingester.snapshot.clone(),
        metrics_port: args.metrics_port,
    }
};

let scraper = crabka_rebalancer::scraper::Scraper::new(
    source,
    std::time::Duration::from_secs(args.metrics_scrape_interval_secs),
    usage_store.clone(),
    shutdown.clone(),
);
tokio::spawn(scraper.run());
```

`ingester.snapshot` is the field set up earlier in the binary entry; if its name differs, grep:

```bash
grep -n "let ingester = \|Ingester::new\|let .*ingester" crates/rebalancer/src/bin/rebalancer.rs | head
```

Adjust to whatever the actual binding is (likely `ingester.snapshot.clone()` or `snapshot.clone()` — use whichever already exists in scope).

### Step 3: Run the binary build + smoke tests

```bash
cargo build -p crabka-rebalancer 2>&1 | tail -3
cargo test -p crabka-rebalancer 2>&1 | tail -10
```

Expected: clean build; tests pass.

### Step 4: Clippy + fmt

```bash
cargo clippy -p crabka-rebalancer --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

### Step 5: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/bin/rebalancer.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(bin): --metrics-port flag; default to Metadata-discovered targets"
```

---

## Execution batches (for parallel subagent dispatch)

All 3 tasks touch the same crate (`crates/rebalancer/src/scraper/{targets,mod}.rs` for Tasks 1+2; `bin/rebalancer.rs` for Task 3). Task 2 depends on Task 1 (uses `TargetSource`); Task 3 depends on Task 2 (uses the new `Scraper::new` signature). Sequential dispatch.

- **Batch A**: Task 1
- **Batch B**: Task 2
- **Batch C**: Task 3

---

## Final verification

- [ ] **Step 1: Full workspace build + tests**

```bash
cargo build --workspace 2>&1 | tail -3
cargo test --workspace --lib 2>&1 | grep -E "test result|FAILED" | tail -20
```

Expected: clean build; no regressions.

- [ ] **Step 2: Workspace clippy + fmt**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 3: Open PR**

```bash
git push -u origin rebalancer-43h
gh pr create --title "Slice 43h: rebalancer scrape-target discovery via Metadata" --body "$(cat <<'EOF'
## Summary

Closes out the "discover via Metadata" deferred follow-up from the rebalancer roadmap. Adds:

- `TargetSource` enum (`Static` | `Discovered`) in `scraper/targets.rs`.
- `Scraper` consumes `TargetSource`; refreshes targets each tick; prunes stale `last_ok` entries.
- New `--metrics-port` CLI flag (env `CRABKA_REBALANCER_METRICS_PORT`, default `9404`).
- The scraper now runs even when `--metrics-scrape-targets` is unset, deriving targets from the ingester's `Metadata` snapshot.

Backward-compatible: `--metrics-scrape-targets`, when set, wins and continues to behave exactly as before.

Spec: `docs/superpowers/specs/2026-05-27-crabka-rebalancer-43h-design.md`
Plan: `docs/superpowers/plans/2026-05-27-crabka-rebalancer-43h.md`

## Test plan

- [x] `cargo build --workspace`
- [x] `cargo test --workspace --lib`
- [x] `cargo clippy --workspace --all-targets -- -D warnings`
- [x] `cargo fmt --check`
- [ ] CI: full matrix

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed.
