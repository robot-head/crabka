# Gres G-7: Multi-range tenants — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design. Revives the donor's multi-range router / cross-range 2PC / GTM layers over substrate-backed ranges, giving one tenant database **table-granular write scale-out**: aggregate write throughput grows linearly with ranges, each range being exactly the single-writer WAL-topic compute G-2/G-3 built. Structured **G-7a** (in-process multi-range) → **G-7b** (distributed range computes). Reverses the chapter's cluster-not-vendored decision for the non-raft ~two-thirds of the crate; the chapter doc is amended accordingly. Single-**table** sharding is deliberately out — that is [G-8](2026-07-09-crabka-gres-g8-sharded-tables-design.md).

## Context — what the donor actually holds (all claims source-verified)

1. **Ranges are strictly table-granular and static.** `RangeMap { boundaries: Vec<TableId> }`; `range_for_table` is a pure function of table id, so one table maps to exactly one range; the map is a write-once blob (`/0/meta/range_map` on range 0) seeded at bootstrap; splits exist only as "D4" comments. Cross-range single statements are rejected `0A000` in `pinning_range` before any engine work, on both protocols (one caveat: cross-range subqueries inside DML bypass the router check but die in the executor's subquery restriction — a different error, never a wrong read).
2. **All durable 2PC state flows through the `Committer` seam** — g allocation (`begin_global_durable`), the write-once decision + read-back (`commit_global_decision`, one range-0 batch), `Prepared(Li→g)` markers folded atomically into row batches, and the recovery watermark. Releases write nothing durable. The single durable arbiter is range 0's global clog; participant lists are never durable (presumed-abort + write-once abort-race from any actor). This is why the port is clean: the substrate's `SubstrateCommitter` slots in wherever `RaftCommitter` sat.
3. **The separability is measured, not hoped:** of 9,460 lines, KEEP ≈ 2,497 (the 2,036-line router has *zero* openraft imports and ports untouched behind `LeadsRange`/`RemoteForward`/`GlobalCoordinator` traits; the frame codec, range map, addr helpers), ADAPT ≈ 3,872 (server_node bring-up skeleton, twopc's coordinator/participant machinery — raft appears only in leader-resolution and barrier snippets, transport server minus its raft arms, recovery gate with term→epoch), DROP ≈ 3,091 (raft log/state-machine storage, network factories, `RaftCommitter`/`RaftLinearizer`). Test-side, ~8k lines port: **eight raft-free Stateright models** (stage idempotency SP21, settle-before-serve SP22 + overlap/cascade SP26, GTM reuse SP23, abort atomicity SP24, watermark floor SP20, write-once decisions, MVCC first-committer-wins) plus the protocol-level suites (`jepsen_bank`, `jepsen_elle` strict-serializability over real processes, `crossrange_2pc*`, multiprocess harness).
4. **Every donor node hosts a local range-0 replica as `catalog_kv`** (raft voter; catalog and global-clog reads are always local, never RPC), with `Range0Barrier` layering freshness on top: fetch range 0's linearizable applied index, wait until the local replica applies through it — called before every fresh-snapshot statement. The substrate substitution was assessed explicitly and holds: a READ_COMMITTED tail consumer of range 0's topic is the same construction as a raft learner.
5. **The verification's correction, now load-bearing:** the barrier offset must **not** be answered by the range-0 *writer* — a fenced-but-unaware zombie would answer with a too-low offset (the deposed-leader ReadIndex hole the donor's `linearizable_read_model` exists for). The offset must come from the log itself. Relatedly, the donor's rise sweep depends on election being atomically "fence all older writers + complete log for the riser"; on the substrate that is exactly **fence before reading the log end** — the ordering G-2's recovery already has (`init_transactions` → barrier → replay), and the GTM-reuse model's two-live-versions tear is the counterexample if reversed.

## Design Goals

- **Linear write scale-out per tenant, table-granular:** N ranges ⇒ ~N× aggregate commit throughput for range-local transactions, with cross-range transactions correct (snapshot-isolated, first-committer-wins) via 2PC.
- **A range is a tenant, structurally:** every G-2/G-3 property (fencing, barrier replay, checkpoints, garbage horizon, torn-truncation refusal) applies per range with zero new durability machinery.
- **The donor's proven correctness corpus survives whole:** all eight models, the bank/Elle suites, and the settle/reseed disciplines port — plus one new model action the port itself demands.
- **Single-range tenants are unchanged:** the one-range map is the degenerate case; today's compute, topics, and gates are untouched.

## Non-goals

- **Sharding one table** (rowid boundaries, cross-range statements on one table, splits/rebalance) — G-8.
- **Dynamic range-map changes** — the map is write-once at tenant provisioning in G-7 (the donor's posture); mutability arrives with G-8b's splits.
- **Cross-range statements** — `0A000` stands in G-7 (lifted for global-visibility tables in G-8a).
- **Suspend/scale-to-zero for multi-range tenants** — v1 excluded (they are big by definition; the size gate already excludes them in spirit); revisit after G-8b.
- **mTLS on the east-west transport** — in-cluster plaintext v1, consistent with the chapter's internal-leg posture; flagged.

## Architecture Overview

```
psql → PgDog → per-tenant Service (all range computes are gateways)
                     │
        range compute (pod) — hosts one or more ranges + a gateway
        ├─ RangeRouter (ported verbatim): parse → pinning_range →
        │    local range: SqlSession on that range's engine
        │    remote range: forward single-statement SQL over pgwire (ForwardPool)
        │    2nd range in a txn: escalate → NetCoordinator (TxnRpc over framed TCP)
        ├─ per hosted range r:
        │    SqlEngine::replicated(catalog_kv, sm_kv_r, SubstrateCommitter_r, Linearizer_r)
        │    topic __gres_wal.<tenant>.r<r>, txn id __gres.<tenant>.r<r>,
        │    checkpoints gres/<tenant>/r<r>/ckpt/…       (G-2/G-3 per range, unchanged)
        │    RecoveryGate keyed on producer epoch; bring-up prologue (below)
        └─ catalog_kv = local store fed by a READ_COMMITTED tail of range 0's topic
             Range0Barrier = broker-log end offset of range 0 + wait local tail ≥ it

range 0 (the system range): catalog, global clog, GTM, range_map — one fenced writer,
  DDL forwarded to its gateway; g allocation + write-once global decisions through
  its SubstrateCommitter (group-commit batches decisions like any other traffic)

bring-up prologue per range (the donor's rise sweep, now a straight line):
  fence (epoch bump) → produce barrier → replay to own barrier → reseed counters
  → re-acquire in-doubt locks from Prepared markers → abort-race in-doubt g's
  → settle-COMPLETE re-scan (retry until empty) → gate opens for this epoch
```

## Key Design Decisions

### Ranges ride the substrate; raft rides into the sea

Each range gets its own WAL topic, fenced transactional producer, checkpoints, and recovery — the G-2/G-3 machinery instantiated per range. `RaftCommitter` → `SubstrateCommitter`; raft **terms → producer epochs** throughout (the `RecoveryGate`'s "leads AND served_term == current_term" becomes "holds the fenced epoch AND the prologue settled that epoch", re-closing on any epoch change); the donor's edge-triggered leadership watchers become lifecycle phases (rise = the recovery prologue; loss = discovered lazily on a fenced append, which is safe because the successor re-derives all in-doubt locks from durable markers; the 500 ms participant-silence sweeper stays a timer and remains the liveness backstop). The apply-time merge rules (counter max-merge, clog write-once-first-terminal) already live in G-2's replay applier — they came from this very crate's `durable.rs`/`store.rs`, closing the loop.

### The range-0 replica is a topic tail; the barrier is derived from the log, never from the writer

Every range compute tails range 0's topic at READ_COMMITTED into a local store — that store is the `catalog_kv` handed to every engine, exactly the donor's local-replica wiring (catalog reads, global-clog reads, and the recovery scan's deliberately-unbarriered decidedness reads all stay local and keep their staleness contracts, which write-once makes safe). `Range0Barrier` becomes: fetch range 0's **broker-log end offset** and wait until the local tail has applied through it. Fetching from the log (partition leader) rather than asking the range-0 SQL writer eliminates the zombie-answer hole outright — a fenced ex-writer never participates in the read path. The offset is the LEO (Crabka's ListOffsets divergence, known from G-2/G-3): gating on LEO is *conservative* — if an open producer transaction holds LSO back, the barrier waits until its markers land (bounded by the transaction timeout), never passes early. This barrier sits on every fresh-snapshot statement's path, so the design names its cost and its remedy — with the freshness discipline stated precisely *(sharpened by the panel review, I5: a free-running refresh is a bounded-staleness read that the Elle gate would catch)*: a cached end-offset sample may satisfy a statement **only if the fetch that produced it began after the statement began** (the ReadIndex discipline). The barrier task therefore batches: concurrent statements piggyback on the next in-flight fetch rather than each issuing one, keeping the amortization while every statement's barrier reflects a fetch that started after it — the common case is one fetch shared by many statements plus an already-caught-up check, never a stale free-running watermark.

### Fence-first is the whole recovery story — and it is already built

The donor's leadership-rise sweep (apply-wait → reseed → re-acquire locks → abort-race → settle-complete → open gate) becomes a straight-line prologue after G-2's fence-and-replay: because `init_transactions()` precedes the barrier-produce and replay, the replayed state provably contains every append any predecessor ever acked — the precise property the sweep's `ensure_linearizable` + apply-wait provided, and the property whose absence is the GTM-reuse counterexample. The prologue then runs the ported steps verbatim over replayed state: lift-only counter reseeds (SP23, fail-closed), lock re-derivation from `Prepared` markers below the watermark (SP24 — the in-memory lock table died with the predecessor, by design), abort-racing in-doubt g's against range 0 (write-once makes any racer safe), and the settle-**complete** re-scan loop (SP26 — the gate stays closed until zero in-doubt markers remain, retrying while range 0 is unreachable). One genuinely new Stateright action joins the ported models: a zombie append landing **between** the successor's end-offset read and its fence, proving fence-before-end-read is load-bearing (the substrate analog of the donor's `reseed: false` teeth).

### The router ports verbatim; discovery moves to the registry

`RangeRouter` — per-statement pinning, DDL→range 0, transaction pinning with escalation-on-second-range, the 0A000 wall — is kept as-is behind its three traits. `RemoteForward` keeps the donor's shape (single-statement SQL text over pooled pgwire to the owning compute, one bounded re-resolve-and-retry on 40001, the SP14 rule that retry lives in the wire layer); `GlobalCoordinator`/`TxnService` keep the donor's `TxnRpc` protocol over the ported framed-TCP transport (raft envelope variants deleted; `Barrier{applied_index}` becomes an offset). What changes is *discovery*: raft-metrics leader resolution becomes a registry lookup — the G-4 tenant record grows a range layout (`ranges: [{range_id, tables_end, compute, endpoint}]`), maintained by the control plane, cached per compute with the existing bounded re-resolve on `NotLeader`-equivalents. Every range compute runs a gateway; the tenant's PgDog entry targets a Service across all of them, so any compute answers any statement (locally or by forwarding), the donor's own topology.

### G-7a before G-7b, mirroring the donor's own build order

**G-7a (in-process):** one compute process hosts *all* ranges of a tenant — router + coordinator + N engines over N topics + the range-0 tail — no transport, no forwarding, `LocalCoordinator`. This lands the entire correctness surface (all ported models, `crossrange_2pc` suites, the prologue, the barrier) against the substrate with the smallest moving-parts set; it scales nothing yet and says so. **G-7b (distributed):** ranges spread across compute pods; transport server + `NetCoordinator` + `ForwardPool` + registry discovery + operator placement (a Deployment per range compute; the `GresTenant` CRD grows the layout) + the ported multiprocess/jepsen harnesses re-targeted at pods; the fence-based failover ("kill the range writer, the successor's prologue settles") replaces leader-election choreography in every scenario test.

## Integration

- **New crate `crates/gres-ranges`** (`crabka-gres-ranges`, `publish = false`): the KEEP/ADAPT subset — router, range map/meta, forward pool, twopc (coordinator/participant/silence sweeper), transport (frames/protocol/server minus raft), recovery gate, the range-compute bring-up skeleton; depends on `crabka-gres-substrate` (per-range committer/recovery/checkpoints), `crabka-gres-control` (layout/discovery), and the vendored engine crates.
- **`crates/gres-substrate`:** per-range parameterization (topic/txn-id/bucket-prefix naming gain a range dimension); the range-0 tail consumer + barrier watermark task.
- **`crates/gres-control` / operator / CLI:** range layout in the tenant record and `GresTenant` CRD; `crabka gres create-tenant --ranges` (boundaries by table count or explicit); per-range-compute Deployments; the tenant Service.
- **`crates/pgexec`:** consumed through existing seams (`replicated`, `set_range0_barrier`, the 2PC entry points `begin_global_durable`/`commit_global_decision`/`join_global`/`reacquire_in_doubt_locks`/`advance_clog_scan_lo` — all already public for the cluster crate's sake).
- **Broker:** zero changes; more topics per tenant (the chapter's fleet ceilings and the named broker cross-track dependencies now bind at ranges × tenants).

## Kafka / wire compliance

Per-range journaling is G-2's transactional produce, unchanged. The east-west legs (framed TCP for `TxnRpc`, pgwire for forwarding) are tenant-internal and never touch the Kafka wire. The barrier uses stock ListOffsets/fetch. Forwarding's donor Trust-auth posture is acceptable only inside the tenant boundary and is named as such; mTLS is the flagged follow-up.

## Testing

- **Ported Stateright models (all eight)** with terms re-read as epochs, plus the new fence-ordering action (zombie append between end-read and fence must produce the GTM-reuse/missed-marker counterexamples when the ordering is broken).
- **Ported protocol suites** on the in-process harness (G-7a): `crossrange_2pc` (abort-race, cross-range visibility), stage idempotency, settle-before-serve under injected fencing, watermark advance.
- **G-7b system suites:** the multiprocess harness re-targeted (kill-the-writer nemesis instead of elections), `jepsen_bank` conservation, **`jepsen_elle` strict-serializability** over real range-compute pods — the slice's headline gate.
- **The scaling demo (gate):** N ranges, range-local workload, measured ~N× aggregate commits/s versus the single-range baseline, published as a per-PR artifact (the SLO-pipeline idiom).
- **Conformance:** unchanged for single-range tenants (the parity baseline binds); a multi-range smoke (DDL on range 0, DML on data ranges, cross-range transactions) rather than corpus-through-ranges, since corpus table placement is creation-order and cross-range joins are 0A000 by design.

## Risks

- **Barrier latency on every fresh-snapshot statement** — named, with the watermark-task remedy; measured in the scaling demo. If it still dominates, snapshot-reuse windows are the next lever (a bounded-staleness read mode is a *product* decision, not a patch — deferred).
- **Range-0 as coordination hub** — g allocation and global decisions serialize through one range's group commit; decision batching amortizes it, and G-7's workloads (range-local dominant) rarely touch it. The real pressure arrives with G-8 and is treated there (g-block leases, batched decisions, and the honest ceiling).
- **Transport security** — plaintext east-west inside the tenant boundary v1; **mTLS is owned here, not merely flagged** *(panel amendment I7)*: G-7b — the slice that creates the multi-tenant-shared network path — carries an explicit hardening task (mTLS on the node-protocol and forwarding legs, or a NetworkPolicy-enforced per-tenant segmentation posture with the trade recorded), and its e2e gate asserts a cross-tenant connection to a range compute's node port is refused. The G-4 ACL principals (C5) cover the Kafka plane; this covers the bespoke transport.
- **Topic multiplication** — ranges × tenants topics; the chapter's broker cross-track dependencies (fetch multiplexing, partitions-by-topic indexing) bind sooner; parked-topic economics do not apply to active ranges.
- **Complexity budget** — this is the chapter's largest slice; the mitigations are the donor's finished proofs (the models port, the invariants are already written down) and the G-7a/G-7b split keeping the correctness surface separated from the distribution surface.

## Resolved decisions

- Port scope: KEEP+ADAPT subset (~6.4k lines) into `crabka-gres-ranges`; raft storage/consensus dropped; all eight models + protocol suites ported; one new fence-ordering model action.
- Substitutions: terms→producer epochs (gate re-keys), rise sweep→recovery prologue (fence first), raft-metrics discovery→registry layout, `Range0Barrier`→broker-log end offset + local tail catch-up (never writer-answered; LEO-conservative), local range-0 replica→READ_COMMITTED topic tail.
- Topology: every range compute is a gateway; PgDog → tenant Service; forwarding = single-statement SQL over pgwire; 2PC over framed TCP.
- Layout: write-once at provisioning (registry + CRD); mutability deferred to G-8b.
- Structure: G-7a in-process correctness, then G-7b distribution; multi-range tenants excluded from suspend v1.
- Honesty: table-granular scale-out; 0A000 stands; single-table sharding is G-8.
