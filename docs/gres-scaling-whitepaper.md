# Gres Scaling Whitepaper

How the gres engine scales — the architecture that makes throughput linear in ranges, the measured evidence behind that claim, the deliberate singleton at its center, and the ceilings we know about.

**Status:** 2026-07-15. Reflects `main` through [#813](https://github.com/robot-head/crabka/pull/813), the 2026-07-15 soak program (`claude/crabka-gres-soak-testing-092155`, pending merge), and the timestamp-oracle reliability/throughput work (`claude/scale-timestamp-oracle-9d9438`, pending merge).

## The scaling model

Gres is a PostgreSQL-compatible engine whose storage, durability, and transaction machinery are built on Crabka's Kafka substrate. Its unit of scale is the **range**: a contiguous span of table rows with its own WAL topic, its own writer (fenced by Kafka transactional-producer epochs), its own checkpoints, and its own group commit. A tenant is a set of ranges plus **range 0**, the coordinator range that owns the catalog, layout, and — since G-9 — the timestamp oracle.

The design goal fixed by the [G-9 distributed-maturity design](superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md) is that per-tenant capacity grows ~linearly with ranges: ingest, storage, scan, and — the hard one — *commit rate*. Commit rate is hard because a naive design serializes every transaction decision through one range's group commit. G-8 measured exactly that ceiling; G-9a removed it.

### Timestamp transactions (G-9a)

Sharded tables use a Percolator-class protocol built on three pieces:

- **A monotone timestamp oracle on range 0.** Every statement gets a `read_ts`; every transaction gets a `start_ts` and `commit_ts` from the same monotone authority. Grants are served from memory below a durable horizon (`max_ts`) that advances in strides through range 0's ordinary WAL, so a grant costs no per-grant durability.
- **Durable intents as locks.** A transaction's writes land as intents in each participant range's WAL; the **primary range** (first write) holds the single write-once commit record at `commit_ts`; secondaries resolve asynchronously and lazily by readers.
- **Visibility as a comparison.** A version is visible iff `commit_ts ≤ read_ts`. No read barrier, no coordination on the read path: a timestamp is its own consistency proof.

Because decisions are range-local (intents + one commit record on the primary), commit throughput scales with ranges instead of serializing through range 0. Range 0's remaining hot-path role is handing out integers.

## Measured evidence

**Linear commit scaling** ([evidence, 2026-07-11](superpowers/evidence/2026-07-11-gres-g9-scaling.md)): the robust persistent-session workload (two sessions/range, three trials, median) measured, at 1/2/4 ranges:

- range-local transactions: 565 → 1031 → 1860 tx/s (**3.29× at 4 ranges**);
- sharded timestamp transactions: 248 → 456 → 853 tx/s (**3.44× at 4 ranges**, 0.86 of the range-local envelope).

The first sharded measurement of that cycle was **flat (0.96×)** — the fix (row-ID boundaries, immutable first-write timestamp primary, leases + liveness certificates removing per-write allocation from the steady state) is part of the same evidence trail, retained deliberately: the gate reports failures rather than relabeling them. CI now reruns this workload via [`scripts/gres-range-scaling.sh`](../scripts/gres-range-scaling.sh) on relevant PRs and gates on the un-flattened curve.

**Single-range statement costs** ([#813](https://github.com/robot-head/crabka/pull/813), on `main`): the 2026-07 load-test program found three O(data) statement costs (transaction-horizon scan, unique-check scan, write-path scans) that put a wall in front of any per-range throughput claim, plus wire-protocol defects under load. #813 removed them — O(data) costs eliminated, write concurrency unlocked, validated at 10× data volume, `TCP_NODELAY` and `COPY FROM STDIN` on the extended protocol fixed.

**Sustained load** (2026-07-15 soak program, pending merge): a full soak run recorded **zero correctness violations** end to end. The throughput story needed work: sustained-write tps collapse was root-caused to local vacuum pacing and fjall (LSM) filter/index block eviction under read amplification; both are fixed in the soak branch (adaptive vacuum pacing with idle drain; partitioned, pinned filter/index blocks at every level), alongside nine SQL/wire-surface fixes that let `pgbench` run unmodified. Milestone: **927 tps TPC-B** through pgwire on the local rig.

## The timestamp oracle: a deliberate singleton

Every scalable piece above leans on one *unscaled* piece: a single monotone oracle per tenant. That is a choice, not an accident. One authority per epoch gives external consistency — *no granted `read_ts` precedes a commit acknowledged before the grant* — without synchronized clocks, uncertainty intervals, or read restarts. The [G-9 design](superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md) records the decision (Percolator-class over HLC) and the hazard that forbids naive scale-out: timestamp blocks leased to multiple grantors break cross-session read-your-writes silently.

So the oracle's scaling posture is: **make the singleton fast and un-killable rather than plural.**

### Throughput

- **Two-level conveyor batching.** Gateways coalesce concurrent grant requests into one RPC ([`BatchedTsoClient`](../crates/gres-ranges/src/tso/client.rs)); at most one upstream RPC is in flight, and everything that queues behind it drains into the next single RPC — batch size self-tunes to RPC latency with no added delay. The oracle side batches again into one allocation.
- **Lock-free within-stride grants.** The hot path is a compare-exchange reservation against atomics ([`TsoOracle`](../crates/gres-ranges/src/tso/oracle.rs)); only horizon advancement (one 8-byte WAL put per stride) and certificate renewal serialize through a mutex.
- **Per-tenant sharding is inherent.** Each tenant has its own range 0 and oracle; the fleet scales horizontally today. Only a single tenant's ceiling is ever at issue, and grant/batch-fill counters ([`TsoStats`](../crates/gres-ranges/src/tso/stats.rs)) exist so that ceiling is measured, not guessed.

### Reliability

There is **no durability SPOF**: the horizon rides range 0's replicated WAL, and a successor resumes strictly past the persisted stride. Availability is bounded by fencing plus recovery:

- **Epoch fencing with amortized liveness.** Grants are gated on producer-epoch liveness certificates; a fenced-but-alive zombie stops granting within one heartbeat interval.
- **Successor grace period.** A recovering oracle waits out the predecessor's largest possible certificate before its first grant, so arbitrarily fast failover cannot let a zombie's stale `read_ts` overlap a successor-acknowledged commit. The exhaustive Stateright model ([`tso_monotonicity_model`](../crates/gres-ranges/tests/tso_monotonicity_model.rs), ~11.4M states) proves the grace rule is load-bearing: removing it yields a freshness counterexample.
- **Gateways converge on the new writer.** Grant RPCs re-resolve the registry and retry once on re-resolvable and transport errors — safe because an unclaimed grant only burns timestamps.
- **Early activation.** On a live multirange boot the range transport binds *before* recovery and the oracle activates the moment range 0 itself is fenced and replayed, while SQL stays gated behind the full prologue ([design note](superpowers/specs/2026-07-15-gres-early-tso-activation-design.md)). The grant outage of a range-0 host restart is now bounded by range 0's own recovery, not the whole node's — proven by a real-process harness test that observes grants serving strictly before the assembled SQL topology.

### Ceilings and rejected alternatives

The cheap levers still un-pulled, in order: larger strides and batch windows (config), then **read-shedding** — every SQL statement currently burns a grant for its `read_ts`, so a closed-timestamp/session-causality mechanism would remove the dominant grant traffic without touching the write path or the total order.

True multi-oracle designs (HLC with uncertainty and read restarts, TrueTime-style commit-wait, two-tier local/global TSOs) were assessed and rejected: each re-founds ordering on physical clocks, threads uncertainty through every read and commit path, invalidates the correctness corpus, and re-litigates a resolved design decision. If a single tenant ever sustains beyond ~10⁶ grants/s *after* read-shedding, that work deserves its own design cycle.

## What to watch next

- **Merge state.** The soak fixes and the TSO branch are pending merge; this document should be re-dated when they land.
- **Grant telemetry under soak.** `TsoStats` counters are in the code but not yet plumbed into the assembled server; the next soak cycle should poll grants/s and batch fill to find the real single-tenant margin.
- **G-timeline unification.** Unsharded cross-range transactions still ride the G-7 g-timeline; unifying them onto timestamp transactions is a named simplification follow-up in the G-9 design, not a scaling gate.

## References

- [G-8 sharded tables design](superpowers/specs/2026-07-09-crabka-gres-g8-sharded-tables-design.md) — the decision ceiling that motivated G-9a.
- [G-9 distributed maturity design](superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md) — timestamp transactions, TSO decision record.
- [G-9 scaling evidence](superpowers/evidence/2026-07-11-gres-g9-scaling.md) — measured commit-rate curves and the gate discipline.
- [Early TSO activation design](superpowers/specs/2026-07-15-gres-early-tso-activation-design.md) — startup sequencing.
- [`crates/gres-ranges/src/tso/`](../crates/gres-ranges/src/tso/) — oracle, batching client, stats.
