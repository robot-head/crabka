# Serverless share-group backlog autoscaling (MSG-4) — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. **The differentiated slice** of the [serverless messaging cycle](2026-07-06-crabka-gateway-header-carrythrough-design.md) — exposes KIP-932 share-group backlog as a KEDA-consumable signal so serverless consumer replicas autoscale on queue depth, including scale-to-zero.

## Context — the one differentiated slice, honestly

Of the messaging cycle's five slices, four are interop/parity (header carry-through, CloudEvents, per-offset ack, the SDK). **MSG-4 is the only genuine differentiation**: KEDA's stock `kafka` scaler understands *consumer-group* lag only, not KIP-932 *share-group* backlog, and no share-group scaler exists upstream. Since Crabka already ships the full KIP-932 stack (`ShareFetch`/`ShareAcknowledge`, `AcquisitionState`, redelivery, archiving), broker-native queue-backlog-driven scale-to-zero for competing-consumer workloads is something neither Confluent nor Supabase ships today.

Two honest bounds on that moat: (1) share groups are an Apache Kafka KIP, so any KIP-932-compatible broker (Redpanda, Strimzi) *could* build the same bridge — the defensibility is first-mover + the fleet-wide correctness that makes it work rather than demo; (2) the durable moat is **combinatorial** — the same share-group topic on the same bucket is simultaneously a pub/sub channel, a work queue, a CDC stream, and an observability WAL. This slice scales **consumers** on backlog; it does **not** scale brokers (broker elasticity is gated on the diskless chapter — do not conflate).

The workload: serverless functions run as **share-group consumers** competing on a topic (each `ShareFetch`es, invokes, `ShareAcknowledge`s). More backlog → more replicas; zero backlog → zero replicas.

## The correctness landmine (why the naive design is wrong)

The obvious design — *each broker emits its local partitions' backlog, PromQL sums fleet-wide* — **undercounts and causes false scale-to-zero of a backlogged group.** Grounding proved the two required states are **not co-located**:

- The **`initialized` enumeration** (which `(group, topic, partition)` have share state) lives in `share_seeds_cache`, populated *only* by replaying the local `__consumer_offsets-0` log during bootstrap (`unified/mod.rs:896`, `bootstrap.rs:353`). With `OFFSETS_NUM_PARTITIONS = 1` (`bootstrap.rs:44`), this cache is complete on **exactly one broker**: the `__consumer_offsets-0` leader (the group coordinator).
- The **high-watermark** is data-partition-leader state (`Partition::high_watermark` locks local `replica_state`, `partition.rs:432`) — spread across the fleet, with **no remote-HWM accessor**.

So "each broker emits its local intersection" produces series only for `initialized ∩ locally-led-data` on the coordinator; partitions led elsewhere are emitted by *no* broker → `sum()` reads low → a backlogged group scales to zero. This spec's architecture exists to close exactly that gap.

## Design Goals

- **Fleet-complete backlog** as a Prometheus gauge on the broker's existing `/metrics` (`metrics_server.rs`, default `:9404`); `sum(crabka_broker_share_group_backlog{group_id="G"})` is the true fleet total.
- **Never emit `-1`:** uninitialized SPSO → **full available backlog** (`hwm − log_start`); a data partition led elsewhere → fetch its **authoritative HWM cross-broker**, never `-1`, never a false `0`.
- **Exactly one series per partition** — the coordinator broker is the single emitter, so `sum()` has no double-count and no gap; `sum() == 0` iff every initialized partition is genuinely drained.
- **Scale-to-zero, safely** — `minReplicaCount: 0` with an `activationThreshold`; because emission is complete and never `-1`, an empty/zero `sum()` means *actually drained*, not *unknown*.
- **Reuse landed infra** — the metrics registry + `/metrics`, `ListOffsets`, and the peer-RPC client (`network/client.rs`); **no new HTTP surface, no new port, no wire/KIP byte change.**

## Non-goals

- **Consumer-group lag autoscaling** — already **free**: Crabka advertises `ListOffsets`/`OffsetFetch`/`FindCoordinator`/`DescribeGroups` (`api_catalog.rs:46,48,56,75`), so KEDA's stock `kafka` scaler autoscales consumer-group workloads over the wire with zero Crabka code. Verify-only.
- **The KEDA external-scaler gRPC service** — deferred; the stock `prometheus` scaler over the landed `/metrics` suffices. Only justified if sub-scrape-latency (~15 s) scale-to-zero is required.
- **Changing `DescribeShareGroupOffsets` wire semantics** — the handler stays Kafka-byte-exact (`-1` for uninitialized on the wire); the metric is a *separate, more-complete* computation over the same coordinator/persister sources.
- **Broker autoscaling / scale-to-zero of brokers** — gated on the diskless chapter; out of scope.
- CloudEvents (MSG-2), per-offset ack (MSG-3), the SDK (MSG-5).

## Architecture Overview

```
COORDINATOR broker only (the __consumer_offsets-0 leader — the one broker with the complete `initialized` set)
  backlog poll loop (tokio interval ~15s, mirrors spawn_lock_sweeper):
    self-gate: if not the __consumer_offsets-0 leader this tick → clear series, skip   (handoff safety)
    for group in GroupCoordinator::share_group_ids():                                  (unified/mod.rs:394)
      for (topic_id, partitions) in share_state_partition_metadata(group).initialized:  (unified/mod.rs:915)
        topic = resolve topic_id → name (metadata image)
        for p in partitions:
          spso        = SharePersister::read_state(group, topic_id, p)   (local or RPC-forwarded)
          hwm, log_st = local Partition::high_watermark()/log_start_offset()  IF co-leading the data partition
                        ELSE ListOffsets(LATEST) → image.partition(topic,p).leader   (peer-RPC, network/client)
          backlog     = effective_backlog(hwm, spso, log_st)   // (hwm − (spso≥0 ? spso : log_st)).max(0); never −1
          gauge.share_group_backlog{group_id, topic, partition}.set(backlog)
    apply snapshot: remove series not seen this tick (drained-away / tombstoned / de-led)

/metrics (only the coordinator's endpoint carries share-group series)
  → Prometheus scrapes all brokers → KEDA `prometheus` scaler: sum(...{group_id="G"}) / threshold → HPA replicas (min 0)
```

## Key Design Decisions

### Coordinator-single-emitter (the core correction)

Enumeration runs **only on the coordinator broker**, because it is the only broker whose `share_seeds_cache` is complete (`OFFSETS_NUM_PARTITIONS = 1`). That one broker computes every initialized partition's backlog — reaching SPSO via `SharePersister::read_state` (local when it leads the `__share_group_state` partition, else RPC-forwarded, `persister_client.rs:223-253`) and HWM either locally or cross-broker (next decision). Because exactly one broker emits each `(group, topic, partition)`, `sum()` is exact. *Alternative rejected — data-leader-driven emission* (each broker emits partitions it leads, HWM stays local): needs a `partition → groups` reverse index that does not exist (only the forward `group → initialized partitions` index exists, on the coordinator). It is the documented scale-out path if coordinator fan-out becomes a bottleneck, not the MVP.

### `effective_backlog` — the `-1` avoidance, as a pure function

The metric **deliberately diverges** from `DescribeShareGroupOffsets`'s per-broker-partial `-1` semantics (`describe_share_group_offsets.rs:216-237`). The pure kernel:

```rust
fn effective_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 { spso } else { log_start }; // uninitialized SPSO → full available backlog
    (hwm - base).max(0)                                  // never negative, never -1
}
```

Cause (a) — uninitialized SPSO (`Ok(None)` → `UNINITIALIZED_START_OFFSET = -1`, `coordinator.rs:68`): a never-fetched partition still has its full contents queued, so backlog = `hwm − log_start`, **not** `-1` and **not** `0`. This is precisely the case the scaler most needs to catch (a brand-new group with a full topic must scale *up*). Cause (b) — non-local data partition: resolved by fetching HWM cross-broker, never by emitting `-1`.

### Remote-HWM read over the existing peer-RPC surface

For a data partition the coordinator does not co-lead, HWM is read with an outbound **`ListOffsets(LATEST)`** to `image.partition(topic, p).leader` (the fleet-wide leader map every broker has, `manager.rs:103`), issued over the same peer-RPC client (`network/client.rs`) the replicator uses for `Fetch` and `persister_client` uses for share-state. `ListOffsets` is already advertised and implemented (`api_catalog.rs:46`); **no new wire API.** This is the one genuinely new cross-broker read the slice introduces — see Risks for its fan-out cost and the scale-out path.

### Coordinator-leadership self-gate (handoff safety)

Each tick begins by checking whether this broker still leads `__consumer_offsets-0`. If not, it clears its series and skips. This structurally prevents a double-count during a coordinator-leadership handoff: both the old and new coordinator run the loop, but only the current `__consumer_offsets-0` leader emits, so `sum()` never counts a partition twice across the overlap window.

### Stale-series hygiene

The loop computes a full snapshot (all `await`s — `read_state`, HWM — happen here), then applies it: series present last tick but absent this tick (drained-away, tombstoned via delete-offsets, or de-led after handoff) are removed so a stale non-zero value can't stick and block scale-to-zero. Whether this uses `Family::remove` / `Family::clear` or a rebuild is resolved in the plan against the pinned `prometheus-client 0.25` API (Risk).

### Gauge shape

One labeled family on `BrokerMetrics`, mirroring the landed `partition_disk_bytes: Family<PartitionLabel, Gauge>` (`metrics.rs:144`): `share_group_backlog: Family<ShareGroupLabel, Gauge>` with `ShareGroupLabel { group_id, topic, partition }`. The registry prefix (`Registry::with_prefix("crabka_broker")`, `metrics.rs:344`) makes it encode as `crabka_broker_share_group_backlog`; it is a `Gauge` (no `_total` suffix). Set via `get_or_create(&lbl).set(backlog)`.

### KEDA integration — stock `prometheus` scaler

No Crabka code; a user-supplied `ScaledObject`:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: my-share-group-scaler, namespace: functions }
spec:
  scaleTargetRef: { name: my-share-consumer }
  pollingInterval: 15
  minReplicaCount: 0            # scale-to-zero
  maxReplicaCount: 20
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.monitoring.svc:9090
      query: 'sum(crabka_broker_share_group_backlog{group_id="my-group"})'
      threshold: '100'           # target backlog-per-replica; HPA drives replicas toward sum/threshold
      activationThreshold: '1'   # wake from 0 only when backlog >= 1
```

Scale-to-zero is correct **only because emission is complete and never `-1`**: `sum()` reflects true fleet backlog, so an empty/zero result means genuinely drained, and `activationThreshold: 1` wakes a replica on any real backlog.

## Integration

- **`crates/broker/src/metrics.rs`** — add `ShareGroupLabel` + `share_group_backlog: Family<ShareGroupLabel, Gauge>` (declare/init/register mirroring `partition_disk_bytes`, `:53-57,:144,:358,:554-559`).
- **`crates/broker/src/share_partition/backlog_poller.rs`** (new) — the pure `effective_backlog` + the free `fn spawn_backlog_poller(coordinator, partitions, persister, metrics, period)` (mirrors `spawn_lock_sweeper`, `manager.rs:276-300`); NOT a method on `SharePartitionLeaderManager` (which holds only `Arc<dyn MetadataSource>`, not `GroupCoordinator`).
- **`crates/broker/src/coordinator/unified/mod.rs`** — enumeration seams `share_group_ids()` (`:394`) + `share_state_partition_metadata()` (`:915`); a topic-id→name resolver if not already reachable.
- **`crates/broker/src/network/client.rs`** + `handlers/list_offsets.rs` — the peer-RPC surface reused for the remote-HWM `ListOffsets(LATEST)`.
- **`crates/broker/src/broker.rs:2464-2473`** — spawn the poller right after `spawn_lock_sweeper()`; `group_coordinator` + `metrics` are already in scope.
- **`crates/broker/src/config.rs`** — optional `share_group.backlog_poll_interval_secs` (default 15), else hardcode.
- **`docs/`** (example) — the KEDA `ScaledObject` manifest + operator notes.

## Kafka / wire compliance

- **No wire/KIP byte change** — this adds an internal Prometheus gauge only. `DescribeShareGroupOffsets` response bytes are untouched (its `-1` semantics stay Kafka-exact); the metric is a separate computation.
- **`ListOffsets` reused internally** — the coordinator's remote-HWM read uses the same wire API KEDA's `kafka` scaler and any client uses; no new API, no new port.

## Testing

- **`effective_backlog` unit tests:** initialized (`hwm − spso`); uninitialized (`hwm − log_start`, the full-backlog case, **not** `-1`/`0`); `spso > hwm` clamps to `0`; all-zero → `0`.
- **Metric encode:** register + `get_or_create` + `set(N)` + encode the registry → assert `crabka_broker_share_group_backlog{group_id=..,topic=..,partition=..} N` appears with **no** `_total` suffix (behavioral encode, not source-text).
- **Poll loop, local:** a coordinator broker with one initialized share group, `hwm > log_start`, **uninitialized SPSO** → after a tick, the scraped series equals `hwm − log_start` (full backlog), not `-1`, not `0`.
- **Poll loop, remote-HWM:** the same group's data partition led by a *different* broker → the series is **still emitted** with the correct backlog (exercises the `ListOffsets` peer read) — the co-location regression guard.
- **Drained → 0:** acquire+Accept all records → the series reads `0` (enables scale-to-zero).
- **Coordinator self-gate:** a non-coordinator broker emits **no** share-group series.
- **Stale-series:** a partition that leaves `initialized` (or a group that disappears) is removed, not stuck at its last value.

## Risks (carried into the plan)

- **Remote-HWM fan-out / coordinator hotspot:** the coordinator issues a `ListOffsets` per non-co-led partition per tick, and concentrates all share-group metric work on one broker. Acceptable for MVP cardinality; the **data-leader-driven model** (with a coordinator-pushed `partition→groups` index) is the documented scale-out path. The plan must confirm the outbound `ListOffsets` path over `network/client.rs` (the replicator/`persister_client` peer-RPC pattern) and bound the per-tick cost.
- **Leadership-handoff overlap:** the self-gate prevents double-count *if* `share_group_ids()`/`is_leader(__consumer_offsets-0)` flip together on handoff — a genuine correctness point the plan must test, not just assert.
- **Stale-series API:** `Family::remove`/`clear` in `prometheus-client 0.25` is unconfirmed; the plan verifies and, if absent, rebuilds the family per tick (snapshot-then-swap to avoid an empty-scrape window).
- **`read_state` RPC per partition per tick:** a forwarded read for each non-locally-led `__share_group_state` partition; accept for MVP, consider a coordinator-side SPSO cache if load bites.
- **Non-atomic HWM/SPSO reads:** sampled at slightly different instants (worse across the remote read); a momentarily-negative true backlog reads as `0` for one tick via `.max(0)` — acceptable for a ~15 s gauge, stated.
- **KEDA schema drift:** the deprecated `metricName` trigger field is omitted; confirm against the target KEDA version — a deployment-doc caveat, not a Crabka correctness issue.

## Resolved decisions (from grounding)

- **Emission:** coordinator-single-emitter (not local-data-leader-per-partition); fleet aggregation is PromQL `sum()` at query time.
- **`-1` handling:** uninitialized → `hwm − log_start` (full backlog); non-local → remote `ListOffsets`; never `-1`/false-`0`, via the pure `effective_backlog`.
- **Host:** free `spawn_backlog_poller` from `Broker::start`, closing over `GroupCoordinator` + `BrokerMetrics`; not on the manager.
- **Scope:** share-group backlog only (consumer-group lag is free via wire-compat); external-scaler gRPC deferred; no wire/KIP byte change.
- **Handoff:** coordinator-leadership self-gate each tick.
