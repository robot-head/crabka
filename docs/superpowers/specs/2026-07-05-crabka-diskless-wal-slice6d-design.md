# Diskless WAL — Slice 6d: re-composed durability gate + Jepsen — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (final sub-slice of Slice 6). **The shipping gate — diskless does not ship until this is green.**

## Context — where this sits

Final sub-slice of Slice 6 (see the [6a spec](2026-07-05-crabka-diskless-wal-slice6a-design.md) for the decomposition). 6a gave a quorum-durable WAL, 6b leaderless serving, 6c the concurrent/leaderless write path. 6d **composes all three** and mechanically proves the whole diskless data path never loses an acknowledged record — turning Slice-5's out-of-scope `NodeLoss` **in-scope** (a surviving quorum retains the un-flushed acked tail) across **concurrent appenders + WAL-node loss within quorum + sequencer-authority handoff on leader change** — plus a **Jepsen-style black-box** harness against a real running cluster under real faults. This is the roadmap's gate: *"no acknowledged record lost across broker death, WAL-node loss within quorum, or PUT failure."*

6d builds **no new data-path machinery** — it composes and adversarially verifies what 6a–6c built. Its deliverables are proofs and a fault-injection harness.

**Prerequisites (unlanded):** Slices 1–5 + 6a + 6b + 6c. The model extends the Slice-5 diskless crash model + the 6a quorum-frontier delta; the harness runs a 6a–6c cluster.

## Design Goals

- **Re-compose the no-acked-loss gate** over concurrent appenders (6c) + minority WAL-node loss (6a) + sequencer handoff (6c): `wal_acked_durable` holds across every interleaving of the composed action set — with `NodeLoss` in-scope up to a minority of WAL nodes.
- **Extend the KRaft linearizability model** for concurrent stateless appenders (drop the single-leader gate; linearize at WAL-quorum-durable).
- **A Jepsen-style black-box harness:** a real in-process 6a–6c cluster + a fault nemesis + a no-acked-loss ledger checker + the linearizability checker + a JVM byte-exact differential leg.

### Non-goals (6d)

- **No new data-path code** — 6d is proofs + harness over 6a–6c.
- **No full-quorum-loss durability** — loss of a *quorum* (f+1 nodes) of the un-flushed tail stays out of scope, asserted for flushed offsets only (the object tier covers those); node-loss is in-scope only up to a minority.
- **No throughput/perf gating** (a separate axis; Ch. 0).

## Architecture Overview

```
STATERIGHT (exhaustive, tiny model) — extend the Slice-5 diskless crash model:
   DELTA 1 (from 6a): WAL frontier = majority presence across N nodes; NodeLoss(minority) IN-scope
   DELTA 2 (from 6c): N concurrent appenders — WalAppend→KraftAssign(concurrent)→WalFsync(quorum)
   DELTA 3 (from 6c): SequencerHandoff on leader change (may regress advertised HWM, never wal_acked)
   ASSERT: wal_acked_durable (composed) + committed_durable + offset gap_free/unique — always
           + sometimes-witnesses for each composed crash/loss/handoff (non-vacuous)

LINEARIZABILITY (crates/raft/tests/model/mod.rs) — extend:
   ClientAppend(single-leader-gated, :172/:566-576)  ──►  AppendVia(appender, value) per stateless appender
   linearization point in settle_committed (:467): HWM-passes  ──►  WAL-quorum-durable
   assert linearizable + log_matching alongside gap_free/unique

JEPSEN (greenfield, crates/integration-tests/tests/diskless_jepsen.rs) — black-box, real cluster:
   SUBSTRATE: 3× Broker::start (broker.rs) + KRaft-quorum-WAL diskless cluster
   GENERATOR: real crabka-client-producer/consumer → an acked-record ledger
   NEMESIS  : kill-accepting-broker / kill-WAL-node-within-quorum / PUT-failure / KRaft-leader-change
              (drop BrokerHandle = in-process kill; pattern from durability.rs/leader_election.rs)
   CHECKER  : every acked offset still consumable (no-acked-loss) + feed history to
              LinearizabilityTester/KraftLogSpec + a JVM byte-exact differential leg
```

## Key Design Decisions

### Re-compose the stateright gate (three deltas over Slice 5)

Extend the Slice-5 diskless crash model (a tiny model, not `data_path_model.rs`):
- **Delta 1 (6a):** the WAL frontier advances on **majority presence** across N nodes (the majority-th-largest — reuse `recompute_high_watermark`, `verified/src/consensus.rs:267`). `NodeLoss(b)` bounded to a **minority** is **in-scope**: `wal_acked_durable` holds because a surviving majority retains every acked offset; `Recover` re-derives it.
- **Delta 2 (6c):** model **N concurrent appenders** — `committed`/`wal_acked` advance by the commit-order merge of concurrent per-appender ranges; assert **gap-free + unique + monotonic** offsets across appenders (fold into the linearizability tester) and that every `wal_acked` offset from *any* appender survives.
- **Delta 3 (6c):** `SequencerHandoff` on leader change (analogous to `apply_elect`): it **may** regress the advertised HWM (like the modeled KIP-207 regress) but must **never** regress `wal_acked` durability — the new authority re-derives from the quorum medium.

Three always-properties carried green: `committed_durable`, `wal_acked_durable` (now under in-quorum `NodeLoss`), `offset_gap_free/unique`. Mandatory `sometimes` witnesses so each composed crash/loss/handoff is actually reached (non-vacuous). **State-explosion is the central risk** — keep the config tiny (2 appenders, 2 appends, `max_epoch=2`, minimal WAL nodes).

### Extend the KRaft linearizability model

In `crates/raft/tests/model/mod.rs`: the `LinearizabilityTester` (`:147`) already supports concurrent in-flight ops — only the emission gate blocks it (`ClientAppend` emits solely when `leaders.len()==1`, `:172`/`:566-576`). Replace it with **`AppendVia(appender_id, value)`** per live stateless appender (a fresh `ClientId` each), and move the linearization point in `settle_committed` (`:467`) from **HWM-passes-offset** to **WAL-quorum-durable** (`KraftLogSpec::invoke` unchanged — it abstracts over which offset). Assert `linearizable`/`log_matching` (`:682`/`:699`) alongside `gap_free`/`unique`.

### Jepsen — greenfield, assembled from three in-tree parts

No jepsen/nemesis crate exists (confirmed); assemble one at `crates/integration-tests/tests/diskless_jepsen.rs`:
- **Substrate:** extend the in-process `Broker::start`/`BrokerHandle`/`listen_addr()` pattern to a 3-broker + quorum-WAL diskless cluster.
- **Generator:** real `crabka-client-producer`/`consumer` recording an **acked-record ledger**.
- **Nemesis:** the model actions as fault injectors — kill-accepting-broker, kill-a-WAL-quorum-node-*within-quorum*, force-a-PUT-failure, trigger-a-KRaft-leader-change (in-process "kill" = drop the `BrokerHandle`; the metadata-driven leader resolution + kill pattern from `durability.rs`/`leader_election.rs`).
- **Checker:** after the fault schedule, assert **every acked offset in the ledger is still consumable** (no-acked-loss), feed the produce/consume history into the `LinearizabilityTester`/`KraftLogSpec` for serializability, and run the **JVM byte-exact differential** leg (reuse the existing differential oracle used by the `jvm_*` acceptance tests).

*Clean split:* stateright = exhaustive interleavings on a tiny model; Creusot = offset-allocator arithmetic (6c) + the WAL-durability watermark (reuse `recompute_high_watermark`); Jepsen = a real running cluster under real faults.

### Creusot — reuse, one thin lemma

No new kernel. Reuse `recompute_high_watermark` as the WAL-durability watermark; reuse the 6c `assign_ranges`/`is_gap_free_partition` kernel. Add only a thin **never-regresses-on-handoff** monotonicity lemma over the existing `#[ensures(result@ >= current_hwm@)]`. No I/O-shaped kernel (the index projection stays in stateright — Creusot can't translate the async surface).

## Integration

- **The Slice-5 diskless crash model** — extended with the three deltas + `NodeLoss(minority)` in-scope + concurrent appenders + `SequencerHandoff`.
- **`crates/raft/tests/model/mod.rs`** — `AppendVia` per appender; linearization at WAL-quorum-durable.
- **`crates/integration-tests/tests/diskless_jepsen.rs`** (new) — the black-box harness.
- **`crates/verified/src/consensus.rs`** — the thin handoff-monotonicity lemma.
- **CI** — the model checks + Creusot replay + the Jepsen harness are the **shipping-gate** checks.

## Kafka / KIP compliance

- **The differential leg** asserts diskless byte-exactness against the JVM oracle under faults — the ultimate wire-compat gate.
- **No wire change** — 6d observes and verifies; it changes no data path.

## Testing (this slice *is* the test suite — the shipping gate)

- **Stateright:** `wal_acked_durable` + `committed_durable` + `offset_gap_free/unique` hold across every interleaving of `{WalAppend, KraftAssign(concurrent), WalFsync(quorum), ObjectPut, IndexPublish, Trim, Crash, Recover, NodeLoss(minority), SequencerHandoff}`; all `sometimes` witnesses reached.
- **Linearizability:** the produce/consume history is linearizable with concurrent `AppendVia` appenders and the WAL-quorum-durable linearization point.
- **Jepsen:** under the fault schedule (kill-accepting-broker / kill-WAL-node-within-quorum / PUT-failure / leader-change), **no acked offset is ever lost or unreadable**, the history is serializable, and the JVM differential leg is byte-exact.
- **Out-of-scope confirmed:** full-quorum WAL-node loss shows the un-flushed tail is *not* recoverable (Slice-6 boundary), while flushed offsets remain recoverable — asserted explicitly.

## Risks (carried into the plan)

- **State explosion** (central) — the composed model multiplies ghosts × Crash/Recover × NodeLoss × concurrent appenders; keep bounds tiny and lean on `sometimes` witnesses for coverage, not size.
- **Jepsen infra cost** — a real 3-broker + quorum-WAL cluster under fault injection is heavy; the in-process `Broker::start` substrate keeps it CI-runnable, but the fault schedule must be bounded/seeded (no `Math.random` — seed via `args`/config).
- **Vacuous proof** — mandatory `sometimes` witnesses for every composed crash/loss/handoff, else the gate passes trivially.
- **Differential leg flakiness** — the JVM oracle under faults must compare only acked/consumable records; a naive full-history diff would flag benign crash gaps.

## Resolved decisions (from brainstorming)

- **6d composes 6a–6c; no new data path.** Its output is the proof + Jepsen harness.
- **Model:** three deltas (quorum frontier, concurrent appenders, handoff); `NodeLoss(minority)` in-scope; full-quorum loss out of scope (flushed-only).
- **Linearizability:** `AppendVia` per appender; linearize at WAL-quorum-durable.
- **Jepsen:** greenfield from `Broker::start` + failover-style nemesis + `LinearizabilityTester` + JVM differential.
- **Creusot:** reuse `recompute_high_watermark` + the 6c kernel; one thin handoff-monotonicity lemma.
