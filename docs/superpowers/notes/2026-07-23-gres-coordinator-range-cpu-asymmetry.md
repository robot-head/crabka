# Investigation: why range 0 costs ~3.5× less CPU per write than data ranges

*Follow-up to the `crabka-gres` scalability harness (`crates/gres-loadtest`, PR #896).*

## Summary

Under 100 % single-shard-insert saturation, the node hosting **range 0** (catalog /
2PC coordinator / timestamp authority) burns ~3.5× less CPU per committed write
than the nodes hosting data ranges, even though the insert load is uniform across
ranges. The asymmetry is **not** the timestamp path, the WAL-apply path, or the
checkpoint configuration. It is the **range-0 read barrier**: every data-range
engine, before it takes a global read snapshot for a write, runs
`ensure_global_readable()`, which issues a **fresh broker connection + committed-tail
sample of range 0's WAL on every statement**. The node that hosts range 0 co-hosts
the catalog and therefore installs **no barrier at all** (`Range0Barrier` is
`None` on range 0's own engine), so it skips that work entirely.

The barrier is load-bearing for catalog / global-clog linearizability, so range 0
being cheaper is partly *structural* (it needs no coherence round-trip to itself).
But the barrier's *implementation* is also substantially redundant: it re-establishes
a broker connection (TLS handshake + admin metadata + topic-UUID resolution) per
call instead of reusing the follower's already-live connection or the follower's
already-tracked end. Removing that per-statement reconnect is the concrete path to
closing most of the gap — up to ~3× cluster throughput on this workload.

## Reproduction

The exact scenario in the brief (`topology { nodes: 4, ranges: 4, cpus_per_node: 2,
broker_cpus: 4 }`) needs 12 online CPUs; this investigation ran on a 4-CPU host, so
the cluster was run **unpinned** (no `cpus_per_node`). Per-process
`cpu_core_seconds` measures actual CPU consumed and is independent of CPU pinning,
so the *work-per-write* asymmetry reproduces regardless. Load is uniform per range
by construction: `single_shard_insert` picks a target table uniformly at random,
independent of which node's front door the connection landed on
(`crates/gres-loadtest/src/workload.rs`).

Scenario: `nodes: 4, ranges: 4, connections: 256, duration_s: 30/80, warmup_s: 5,
mix { single_shard_insert: 100 }`, `logical-tso`.

| run | node0 (range 0) | node1 | node2 | node3 | broker |
|-----|-----------------|-------|-------|-------|--------|
| E0 — shipped harness (node0 checkpoints, others none) | **7.84** | 27.67 | 27.00 | 26.73 | 27.27 |
| E1 — no node checkpoints                              | **8.07** | 27.28 | 27.65 | 27.49 | 28.29 |

Values are `cpu_core_seconds` over a 30 s window (0 failed txns, 30 021 committed).
node0 is ~3.4× cheaper — matching the ~3.5× reported on the pinned 2-vCPU cluster.

## Ruled-out hypotheses

- **Checkpoint configuration (the harness gives node0 `--checkpoint-store local
  --checkpoint-frames 1` and data nodes nothing).** *Ruled out.* E1 disables
  checkpointing everywhere and node0 stays cheap (8.07 vs ~27.5). Checkpointing is
  background/threshold work that runs *only* on node0, so if anything it is a
  headwind that makes node0's true per-write advantage larger than the raw number.
  E0 (checkpoint-pruned range-0 WAL) and E1 (unpruned) also produce near-identical
  data-node cost — see "The dominant cost is the reconnect" below.

- **Per-write timestamp RPC to node0.** *Ruled out as the driver.* Single-shard
  autocommit inserts use the local-sequence bypass when the seat co-hosts the target
  range, and the global path otherwise; the global path's timestamp grant is
  coalesced by `BatchedTsoClient` and is a *seat-side* cost that is symmetric across
  all four seats. node0 also *serves* those grants, which would make it busier, not
  idler.

- **WAL-apply / commit path.** *Ruled out.* Every autocommit insert routes through
  `execute_timestamp_scatter` → `prewrite_as_primary` + `resolve` (two WAL frames) on
  the owning range, identically for range 0 and data ranges. Both ranges have a
  broker WAL topic (`__gres_wal.loadtest.r{0..3}`) and both self-apply via
  `ProducerWalWriter::commit_group`.

## Root cause: the range-0 read barrier

`crates/pgexec/src/session.rs::read_context` (autocommit arm) runs, before taking a
global read snapshot for the statement:

```
self.linearizer.ensure_readable().await?;     // own range
self.ensure_global_readable().await?;         // range 0 caught up before the gsnap
```

`ensure_global_readable()` is a no-op on range 0's own engine — `range0_barrier` is
`None` there ("Range 0's own engine needs no barrier", `pgexec/src/lib.rs`). On a
**data-range** engine it drives `Range0Barrier::ensure_readable`
(`crates/gres-ranges/src/barrier.rs`):

1. `sample_end_after_call_begins()` →
   `crabka_gres_substrate::recovery::live_committed_end()`, which
   **`AdminClient::connect_secured` (a new broker TLS connection) + `resolve_topic_uuid`
   + opens a reader connection + fetches range 0's committed tail** to find the end
   offset, and
2. waits for the local range-0 follower tail to apply up to that offset.

`handle_range0_barrier` in `forward.rs` states the structural reason directly: a node
that "contains COORDINATOR" returns `Range0Barriered` immediately because "co-hosted
engines share the owner's catalog Arc directly, so every committed catalog write is
already visible here." A data node has no such shortcut — it must sample range 0's
end from the broker and wait for its follower.

### Profile evidence

`perf` is unavailable on this kernel; a stack-sampling profiler built on `eu-stack`
(sampling only threads in `R` state) was used instead. Sampling node0 (range 0) and
node1 (a data range) concurrently under the 80 s run:

| frame group | node0 (coordinator) | node1 (data) |
|-------------|---------------------|--------------|
| total running-frame samples | 1 799 | 4 432 |
| `Range0Barrier` / `*EndSampler` / `live_committed_end` | **0** | 242 |
| broker fetch + `FetchResponse::decode` / `RecordBatch::decode` / `RecordsPayload::from_fetch_bytes` / client connection setup | 8 (own producer) | 232 |

node0 shows **zero** barrier frames — it has no barrier. On node1, ~11 % of all
running-frame samples land directly in the barrier + broker-fetch/decode machinery
(plus the surrounding `malloc`/`free`, TLS, and tokio-I/O they drive). This is
exactly the `RecordBatch::decode` / crc32c / malloc / memmove signature reported in
the original data-node profile.

### The dominant cost is the reconnect, not the scan

`live_committed_end` scans from the WAL's `log_start_offset` to `last_stable_offset`.
Under E0, node0's `--checkpoint-frames 1` prunes range 0's WAL so that span is tiny;
under E1 nothing prunes it, so the span grows without bound during the run. Data-node
cost is essentially identical between E0 and E1 (27.67 vs 27.28). If the record scan
dominated, E1 would be dramatically more expensive and would degrade over time. It
does not — so the **per-call fixed overhead (establishing the broker connection +
admin metadata + topic-UUID resolution) is the dominant cost**, incurred on a
continuous back-to-back stream of samples because the barrier's linearizability rule
(sample a fetch that *started after* the call began) prevents concurrent inserts from
fully coalescing onto one sample.

## Equalization

The barrier's *semantics* (a fresh, linearizable sample of range 0's committed end,
then wait for the local follower) are correctness-load-bearing and should be
preserved. The *redundant transport* is what to remove. In rough order of
impact-per-risk:

1. **Reuse a broker connection + cached topic UUID in the sampler, with
   fallback-to-fresh on error.** The sampled offset is byte-identical; only the
   per-call `connect_secured` + `resolve_topic_uuid` + reader `open_connection` are
   amortized. A cached connection that errors falls back to today's fresh-connect
   path, so availability under partition/broker-restart is no worse than today. This
   targets the measured dominant cost directly.

2. **Sample from the follower that is already tailing range 0.** Each data node
   already runs a continuous range-0 follower with a live broker connection and a
   known applied end; the barrier already carries a `refresh_poke` to wake it. Have
   the sampler poke the follower for one fresh fetch on its existing connection and
   return that `last_stable_offset`, instead of opening an independent consumer. This
   also removes the second, duplicate broker consumer per data node.

3. **Skip the global barrier for blind single-shard writes.** An `INSERT ... VALUES`
   reads no existing rows, so its global read snapshot is never consulted; taking it
   (and thus barriering range 0) for such statements is unnecessary. This is more
   surgical but touches MVCC snapshot logic and needs care around constraints /
   `RETURNING` subqueries.

Options 1 and 2 preserve the barrier contract exactly and should recover most of the
~19 core-seconds/30 s gap per data node, i.e. approach the ~3× throughput headroom.
Any of these must be validated against the fault and formal suites the barrier
underpins (`tso-partition`, `node-crash`, `flappy-network`, and the two-range
closed-timestamp Stateright model), not just the happy-path load test.

## Fix (2026-07-23): persistent incremental committed-end sampling

Option 1 was implemented, extended with an incremental scan cursor
(`crates/gres-substrate/src/follower.rs`):

- `LiveCommittedEndSampler` now keeps **one broker attachment** (raw fetch
  connection + resolved topic UUID, dialed via a `CommittedEndDialer` seam in
  `crates/gres-substrate/src/recovery.rs`) and a **monotone scan cursor**
  (`next_fetch`, `last_visible`) across calls.
- Per barrier call the remaining work is: one `Fetch` round-trip on the live
  connection positioned at the cursor (with `max_wait_ms = 0`, so an
  at-the-end cursor returns immediately). The response's `last_stable_offset`
  is the linearization point — read by a fetch that started after the call
  began — and only records in `[cursor, stable_end)` are decoded. History
  below the cursor is immutable and already folded into `last_visible`, so
  the per-call full-topic re-scan is gone along with the per-call
  `connect_secured` + `resolve_topic_uuid` + reader `open_connection`.
- Records at or above the stable end are never counted and the cursor never
  crosses it, so an open transaction that later aborts cannot poison the
  cached value.
- **Invalidation:** any error on the cached attachment drops it and falls
  back to a fresh dial inside the same call (exactly the old per-call path);
  only the fresh attempt's failure surfaces to the barrier, so availability
  under broker restart/partition is unchanged. The cursor survives redials —
  it describes broker-side log state, not connection state. Pruned history
  (OFFSET_OUT_OF_RANGE) jumps the cursor to the retained log start.
- The follower poll loop in `crates/gres/src/lib.rs` shares the same sampler
  instance as the barrier, removing its independent reconnect-per-tick end
  probe as well. (Its bounded tail *read* still dials per catch-up; that is
  per poll tick under load, not per statement.)
- `live_committed_end()` now delegates to a one-shot sampler, so recovery and
  split-activation callers keep today's fresh-dial semantics through one scan
  implementation.

The barrier semantics in `crates/gres-ranges/src/barrier.rs` (fresh sample
per generation, never adopt an in-flight sample, wait for the local tail) are
untouched — the sampler still performs a broker fetch that begins after every
call, so no staleness window was introduced. Covered by unit tests over a
counting fake broker (connection reuse, incremental cursor, redial fallback,
stable-end exclusion, pruning) plus an end-to-end barrier test in
`follower.rs`, and the existing barrier/jepsen/nemesis suites.

## Tooling used

- `crates/gres-loadtest`: added `CRABKA_GRES_LOADTEST_CHECKPOINT_NODES`
  (`all` | `none`, default = node0-only) to A/B the checkpoint config without
  rebuilding — this produced E1 and ruled checkpointing out.
- A `eu-stack`-based running-thread sampler (no `perf` on this kernel) to diff the
  node0 vs node1 on-CPU profiles.
