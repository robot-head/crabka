# Crabka rebalancer 43h — Scrape-target discovery via Metadata

**Date:** 2026-05-27
**Status:** Slice design. Follows slices 43a–43g (rebalancer foundation
through anomaly detector) and slice 44 (operator `KafkaRebalance` CRD).
Part of the rebalancer roadmap
(`docs/superpowers/specs/2026-05-17-crabka-rebalancer-roadmap-design.md`).

## Why this exists

Today `crabka-rebalancer` takes `--metrics-scrape-targets
id:host:port,…` as a static CLI list. Scaling the broker pool or
re-IPing a broker requires restarting the rebalancer. The roadmap
explicitly flagged this as a deferred follow-up ("discover via
Metadata"). The ingester already pulls `MetadataResponse` every tick;
the broker list lands in `ClusterState::brokers` and is shared via
`Arc<ArcSwap<Option<ClusterState>>>` to the API module. The scraper
should consume the same handle.

Kafka has no protocol surface for advertising the broker's metrics
endpoint, so the *port* still has to come from the operator. The
broker's slice-39 metrics endpoint defaults to `0.0.0.0:9404`; the
operator templates broker pods with the same port. A single
`--metrics-port` flag (default `9404`) handles the uniform case;
`--metrics-scrape-targets` stays as an escape hatch for heterogeneous
or non-operator deployments.

## Goal

When the rebalancer starts without `--metrics-scrape-targets`, every
broker that appears in the ingester's snapshot is automatically
scraped at `host:metrics_port`. Brokers added to the cluster start
being scraped on the next tick; brokers removed stop being scraped.
No restart needed.

## Non-goals

- **Per-broker metrics-port overrides.** The single-flag default
  works for any operator-managed cluster (which is the
  production deployment shape). Per-broker `id:port` overrides can
  be added later if a real need surfaces.
- **Helm-chart wiring.** The chart at
  `charts/crabka-rebalancer/` continues to expose
  `--metrics-scrape-targets` for backward compatibility. A chart
  follow-up can switch the default to discovery (set the flag empty,
  expose `--metrics-port`) in a separate operator-roadmap slice.
- **Discovering the metrics port itself via some new protocol or
  annotation surface.** Kafka has no advertisement mechanism for
  non-Kafka ports; inventing one is way outside this slice.
- **Replacing the static-target path entirely.** Greenfield, but
  the escape hatch is genuinely useful (clusters where the metrics
  endpoint is fronted by a sidecar / on a different host / on a
  different port per broker). Keep `--metrics-scrape-targets`.

## Architecture

### `TargetSource` enum

In `crates/rebalancer/src/scraper/targets.rs`, add a small enum that
unifies the two ways the scraper can find its targets:

```rust
pub enum TargetSource {
    /// Operator-supplied explicit `id:host:port` list. Wins when
    /// `--metrics-scrape-targets` is set; the existing fallback path.
    Static(Vec<ScrapeTarget>),

    /// Live discovery from the ingester's `ClusterState` snapshot.
    /// Every broker in the snapshot becomes a target at
    /// `host:metrics_port`. Brokers with empty `host` are skipped
    /// (one-time WARN per broker_id).
    Discovered {
        snapshot: Arc<ArcSwap<Option<ClusterState>>>,
        metrics_port: u16,
    },
}

impl TargetSource {
    /// Materialize the current target list. Called by the scraper's
    /// main loop each tick. Cheap (clones existing fields into a
    /// fresh Vec); the snapshot guard is the only allocation.
    pub fn current(&self) -> Vec<ScrapeTarget> { … }
}
```

`ScrapeTarget { broker_id, addr }` is unchanged.

`Arc<ArcSwap<Option<ClusterState>>>` is the same type the ingester
shares with the API module today — no new wiring, just a second
consumer.

### Scraper loop

Today (`crates/rebalancer/src/scraper/mod.rs`) the scraper takes
`Vec<ScrapeTarget>` at construction and iterates over it each tick.
Change the field to `TargetSource`, and inside the loop call
`self.source.current()` at the top of each iteration. The rest of
the loop body — `scrape_target`, `ScrapeLogLevel` transitions,
`UsageStore` updates — is unchanged.

State across ticks (the `HashMap<broker_id, last_ok: bool>` that
drives `ScrapeLogLevel`) lives in the scraper. When a broker
disappears from `current()`, its entry sits unreferenced. The map
GC's lazily: on each tick, prune entries whose `broker_id` isn't in
`current()` (single pass, O(n)).

### Binary entry

`crates/rebalancer/src/bin/rebalancer.rs`. Add:

```rust
/// Broker metrics-endpoint port for live discovery. Used when
/// `--metrics-scrape-targets` is unset; targets are derived from the
/// ingester's `Metadata` snapshot as `host:METRICS_PORT`. Defaults
/// to crabka-broker's slice-39 default.
#[arg(long, env = "CRABKA_REBALANCER_METRICS_PORT", default_value_t = 9404)]
metrics_port: u16,
```

At startup:

```rust
let source = if !args.metrics_scrape_targets.trim().is_empty() {
    TargetSource::Static(parse_targets(&args.metrics_scrape_targets)?)
} else {
    TargetSource::Discovered {
        snapshot: ingester.snapshot.clone(),
        metrics_port: args.metrics_port,
    }
};
```

When `--metrics-scrape-targets` is set, `--metrics-port` is silently
ignored; documented in the flag's doc comment.

## Error handling

| Failure                                          | Surface                                                                  |
|--------------------------------------------------|---------------------------------------------------------------------------|
| No snapshot yet (cold start)                     | `current()` returns empty Vec; loop tick does nothing. First ingest populates; next scrape cycle picks up.|
| Broker with empty `host` in metadata             | Skipped from `current()` with a one-time WARN per `broker_id`.            |
| Scrape fails at a discovered target              | Existing `ScrapeLogLevel::Warn` (first-time failure) / `Recovered` / `Debug`. No new error path. |
| Both `--metrics-scrape-targets` AND `--metrics-port` set | Static wins; port silently ignored. Documented in `--help`.       |
| `--metrics-port 0` (or other invalid u16)        | Clap rejects at CLI parse — `u16` typed arg.                              |

## Testing

- **Unit (`scraper/targets.rs`)**:
  - `TargetSource::Static(...).current()` returns the underlying Vec.
  - `TargetSource::Discovered.current()` with a 3-broker snapshot →
    3 targets at the configured port.
  - `TargetSource::Discovered.current()` with one broker missing
    `host` (empty string) → 2 targets, third skipped.
  - `TargetSource::Discovered.current()` with snapshot = `None` →
    empty list.
  - `TargetSource::Discovered.current()` is reactive: store a new
    `ClusterState` into the `ArcSwap`, call `current()` again, observe
    the new broker.
- **Unit (`scraper/mod.rs`)**: extend the existing main-loop test (or
  add a new one) to verify a broker disappearing from `current()`
  causes its prev-state entry to be pruned and stops emitting log
  events.
- **No new integration test**: the existing scraper integration tests
  with a fake broker `/metrics` endpoint already exercise the loop
  end-to-end. Switching the source from `Static` to `Discovered`
  doesn't change what those tests exercise.

## Out of scope

- Per-broker port overrides (hybrid `id:port` flag).
- Helm chart default change.
- Bootstrap-server / advertised-listener vs. metrics-endpoint host
  reconciliation. The slice assumes `MetadataResponse.brokers[].host`
  is reachable from the rebalancer pod — true under the operator,
  and any deployment where it isn't can fall back to
  `--metrics-scrape-targets`.

## Implementation order

1. Add `TargetSource` enum + `current()` impl + unit tests
   (`scraper/targets.rs`).
2. Switch the scraper's stored target list from `Vec<ScrapeTarget>` to
   `TargetSource`; call `source.current()` at the top of each tick;
   prune stale prev-state entries (`scraper/mod.rs`).
3. Add `--metrics-port` CLI flag (default `9404`) +
   binary-entry source-construction logic (`bin/rebalancer.rs`).
4. Update the flag-`--help` text on `--metrics-scrape-targets` to
   note it overrides discovery.

## Risk register

- **Default-port mismatch.** Operators not using the operator (e.g.
  bare-metal) may run brokers on a different metrics port; without
  setting `--metrics-port` or `--metrics-scrape-targets`, every scrape
  fails. The existing `ScrapeLogLevel::Warn` surfaces this loudly
  enough; no extra guard needed beyond the documented default.
- **Snapshot lag at startup.** First few seconds of the rebalancer's
  life have no snapshot; nothing is scraped. Acceptable — the
  ingester runs at `--scrape-interval-secs` (default 10s), so the
  scraper starts within 10s of the rebalancer. No new SLA breach.
- **Stale broker entries in prev-state.** The lazy GC is O(n) per tick
  where n = brokers ever seen. For a 100-broker cluster that loses
  one broker, the GC catches that on the next tick. Fine.
