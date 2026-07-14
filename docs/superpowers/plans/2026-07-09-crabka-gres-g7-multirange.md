# Chapter Gres G-7: Multi-Range Tenants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One tenant database scales writes linearly across table-granular ranges: the donor's router/2PC/GTM layers run over topic-per-range substrate durability, with the donor's full correctness corpus (eight Stateright models, bank/Elle suites) ported and green.

**Architecture:** New crate `crabka-gres-ranges` vendors the verified KEEP/ADAPT subset of donor `crates/cluster` (~6.4k of 9.5k lines; raft storage/consensus dropped). Substitutions are mechanical at named seams: `RaftCommitter`→`SubstrateCommitter` per range, raft terms→producer epochs (RecoveryGate re-keyed), raft-metrics discovery→registry layout, local range-0 raft replica→READ_COMMITTED topic tail, `Range0Barrier`→broker-log end-offset + local tail catch-up, rise sweep→fence-first recovery prologue. G-7a lands the whole correctness surface in one process; G-7b distributes it.

**Tech Stack:** the donor port (framed-TCP serde_json transport, pooled pgwire forwarding, TxnRpc protocol), `crabka-gres-substrate` per range, `crabka-gres-control` for layout/discovery, stateright, the multiprocess/jepsen harnesses.

## Global Constraints

- **Prerequisites:** G-1…G-3 landed (G-7a); plus G-4 (registry/operator) for G-7b. Verify every signature against the landed tree; donor claims were verified at `crabgresql@93f3d17` (donor clone convention: `/tmp/crabgresql-donor`).
- **Spec:** [2026-07-09-crabka-gres-g7-multirange-design.md](../specs/2026-07-09-crabka-gres-g7-multirange-design.md). The KEEP/ADAPT/DROP map and the 2PC portability briefing live in the G-7 research (all claims re-verifiable at the donor pin); the plan references donor files by their in-tree paths.
- **The load-bearing ordering, everywhere:** fence (epoch bump) → produce barrier → read/replay to the fenced end → reseed → settle → serve. Never read a log end before fencing that log's writers. The new model action (Task 2) is the executable statement of this rule.
- **Naming (decided — panel minor):** topics are `__gres_wal.<tenant>.r<id>` universally, with a single-range tenant being `…​.r0`; G-7's Task 1 migrates the constant in `gres-substrate` (and its uses in the G-2/G-4/G-5 code and CI scripts) in one greenfield rename commit — no compat shim, no dual spelling. Transactional ids `__gres.<tenant>.r<id>`; checkpoints `gres/<tenant>/r<id>/ckpt/…`; the G-4 ACL prefixes cover both spellings' prefix (`__gres_wal.<tenant>`), so authorization is unaffected.
- **Error-contract preservation:** the donor's retryability mapping (40001 `SerializationFailure` for NotLeader-equivalents, 08006 for wire loss, one bounded re-resolve+retry in the wire layer only — never in the router) is a compatibility surface; tests pin it.
- Lints/format/commit/test conventions as in the G-2 plan; every ported test keeps its donor name for traceability.

---

## Batch 1 — G-7a foundations (serial: Task 1 then Task 2 — the models compile against the crate Task 1 creates; panel amendment I8)

### Task 1: `crabka-gres-ranges` crate — vendor the KEEP subset + range-parameterize the substrate

**Files:**
- Create: `crates/gres-ranges/` (manifest per internal-crate house style; deps: `crabka-gres-substrate`, `crabka-gres-control`, `crabka-pgexec`, `crabka-pgwire`, `crabka-pgparser`, `crabka-pgcatalog`, `crabka-pgkv`, `crabka-pgmvcc`, `tokio`, `serde`+`serde_json`, `bytes`, `arc-swap`, `async-trait`, `thiserror`, `tracing`; dev: `assert2`, `stateright`, `proptest`, `tokio-postgres`, `tempfile`, `crabka-broker`); release-plz entry.
- Vendor from donor `crates/cluster/src` (rename-sed per the G-1 recipe, plus `cluster::`→`crabka_gres_ranges::`): `range/router.rs`, `range/map.rs`, `range/meta.rs` (write path re-pointed at a Committer — Step below), `transport/frame.rs`, `transport/partition.rs`, `transport/protocol.rs` (raft variants deleted), `addr.rs`, `recovery_gate.rs` (term→epoch rename with semantics table in rustdoc), `types.rs` (`WriteBatch` only).
- Modify: `crates/gres-substrate/src/{topic,writer,recover,committer}.rs` — every `tenant`-keyed name gains a `range: RangeId` dimension (`wal_topic(tenant, range)`, txn id, bucket prefix, `SubstrateCommitter` unchanged in shape).

**Interfaces:**
- Produces: the compiling KEEP subset with its in-file tests green (the router's ~1,090 in-file test lines come along and pass against stub trait impls), `RecoveryGate` keyed `(RangeId, epoch: i16)`, and range-parameterized substrate constructors consumed by Tasks 3–5.
- The three router seams stay traits exactly as donor-shaped: `LeadsRange`, `RemoteForward`, `GlobalCoordinator` (+ `RecoveryGate`).

Steps: vendor+rename+lint per the G-1 per-crate recipe (import commit, pedantic commit); `range/meta.rs`'s `write_range_map` re-signatured to take `&dyn Committer` instead of a raft handle (one-batch append, same bytes); substrate range-parameterization with its unit tests updated; `cargo nextest run -p crabka-gres-ranges -p crabka-gres-substrate` green. Commit `feat(gres): vendor the donor multi-range router onto range-parameterized substrate`.

### Task 2: Port the eight Stateright models + the new fence-ordering action

**Files:** Create `crates/gres-ranges/tests/models/` — vendor all eight donor model files (`crossrange_2pc_model`, `crossrange_2pc_abort_atomicity_model`, `crossrange_2pc_gtm_reuse_model`, `crossrange_2pc_settle_model`, `crossrange_2pc_overlap_settle_model`, `write_once_decision_model`, `mvcc_write_conflict_model`, `recovery_watermark_model`) with donor names kept; terms renamed to epochs in state/action names only where the donor's comments say "term".

The new action (added to the GTM-reuse and settle models): `ZombieAppendAfterEndRead` — a deposed writer's append lands after the successor read the log end but before the fence. With the models' `fence_first: true` config the invariants hold; with `fence_first: false` the checker must produce the two named counterexamples (reused g → two live versions; missed `Prepared` marker → gate opens with an in-doubt row). Both teeth pinned as tests, mirroring the donor's positive+teeth discipline.

Steps: vendor, adapt imports, run (`cargo nextest run -p crabka-gres-ranges --test models` under the model test-group — add a nextest group if BFS times warrant, mirroring donor budgets), add the new action TDD-style (teeth first). Commit `test(gres): port the donor 2PC/recovery models with the fence-ordering action`.

---

## Batch 2 — G-7a runtime (serial; both tasks touch gres-ranges core files)

### Task 3: The range-0 tail, the barrier, and the recovery prologue

**Files:** Create `crates/gres-ranges/src/{range0_tail.rs, barrier.rs, prologue.rs}`; modify `crates/gres-substrate/src/recover.rs` (expose replay internals the prologue composes).

**Interfaces:**
- `range0_tail::spawn(bootstrap, tenant, store: Arc<dyn Kv>) -> Range0Tail` — a READ_COMMITTED consumer of `__gres_wal.<tenant>.r0` applying frames through the G-2 merge rules into a local store (this store is the `catalog_kv` for every engine on the compute), publishing `applied_offset: watch::Receiver<i64>`.
- `barrier::Range0Barrier { tail: Range0Tail, inflight: <batched-fetch state>, }` implementing `crabka_pgexec::Linearizer`: `ensure_readable` = obtain an end-offset sample **from a fetch that began after this call began** (the ReadIndex discipline — panel amendment I5; concurrent callers piggyback on the next in-flight ListOffsets(-1) rather than each issuing one, and a *free-running cached watermark is explicitly forbidden*), then await `tail.applied_offset >= sample` (bounded; timeout → `ExecError::Unavailable`). Conservative-LEO semantics per the spec. Unit-tested against an in-process broker with (a) an open producer transaction proving the barrier waits for markers, never passes early, and (b) a freshness test: a commit acked before `ensure_readable` is called is always visible after it returns.
- `prologue::recover_range(...) -> Result<ServingRange, ...>` — the straight line: G-2 fence+barrier+replay (per range) → `reseed_counters`/`reseed_gtm` (range 0 only; fail-closed) → `reacquire_in_doubt_locks` (the executor API, verbatim donor step) → abort-race in-doubt g's via the coordinator seam → settle-complete re-scan loop (retry-interval + bounded attempts; gate stays closed on any in-doubt remainder) → `gate.mark_served(range, epoch)`.

Steps: TDD each piece (tail applies + publishes; barrier conservative-wait; prologue happy path + in-doubt-marker path against an in-process broker with a hand-journaled `Prepared` marker), then an integration: kill a two-range in-process tenant mid-2PC, recover, assert the prologue settles and state matches the decision. Commit `feat(gres): range-0 tail, log-derived barrier, and the recovery prologue`.

### Task 4: G-7a assembly — the in-process multi-range tenant

**Files:** Create `crates/gres-ranges/src/tenant.rs` (in-process assembly: N engines over N topics, `LocalCoordinator`, router wiring, gateway `Engine` impl); vendor+adapt donor `route.rs`'s `RangeGatewayEngine`/`serve_range_routed` (zero-raft portion); modify `crates/gres/src/main.rs` (substrate mode accepts `--ranges <boundaries>`; multi-range assembly behind it; single-range path untouched).

**Interfaces:** `MultiRangeTenant::start(cfg) -> (gateway: impl pgwire Engine, handles)` — consumed by the bin and every G-7a test. `LeadsRange` for in-process = "this process hosts and has opened range r" (all ranges local in G-7a).

Steps: TDD via ported donor suites — vendor `crossrange_2pc.rs`, `multirange.rs` (routing isolation), `gateway_local.rs`, `sql_over_raft.rs`→`sql_over_journal.rs`, `durable_scenarios.rs` (bounce/crash/full-restart via replay) into `crates/gres-ranges/tests/`, re-pointed at `MultiRangeTenant` on an in-process broker; make them green; add the conformance smoke (DDL→r0, DML→data ranges, one cross-range txn) to the gres-conformance CI leg as a script step. Commit `feat(gres): in-process multi-range tenants (G-7a)`.

---

## Batch 3 — G-7b distribution (serial: Tasks 5 → 6; then Batch 4)

### Task 5: Transport, forwarding, NetCoordinator, discovery

**Files:** Vendor+adapt donor `transport/server.rs` (raft arms deleted; `RangeRegistry` re-typed to range→engine/writer handles), `forward.rs` (`resolve_leader` → registry lookup via `crabka-gres-control`), `twopc.rs` (`TwoPcClient` discovery → registry; `Range0Barrier` deleted in favor of Task 3's; `TxnResp::Barrier` carries an offset); extend `crates/gres-control` records with the range layout (`ranges: [{range_id, tables_end, endpoint, wal_generation}]` — the endpoint doubles as the spec's `compute` field, and per-range `wal_generation` lands here as the registry home the G-8 parking mechanics need; panel amendments I4 + minor).

Steps: TDD the discovery seam (registry-backed resolve with the donor's one-bounded-retry contract — port `remote_forward.rs`'s injected-NotLeader retry proof); port `crossrange_2pc_net.rs`; wire the silence sweeper; nextest/clippy/fmt. Commit `feat(gres): distributed range transport, forwarding, and coordination (G-7b core)`.

### Task 6: Range-compute binary mode + operator placement

**Files:** Modify `crates/gres/src/main.rs` (`--host-ranges r0,r2` mode: host listed ranges + gateway + range-0 tail), `crates/operator` (GresTenant CRD grows the range layout; one Deployment per range compute; the tenant Service selects all of them), `crates/cli` (`crabka gres create-tenant --ranges`), `crates/gres-control` (layout mutations).

Steps: operator mock-harness tests for the multi-Deployment render; CLI integration (create a 3-range tenant, registry shows the layout); a compose-style two-pod smoke in the e2e script (two computes, forwarded DML, one cross-range txn); **east-west hardening** *(panel amendment I7 — owned here, in the slice that creates the shared path)*: either mTLS on the node-protocol + forwarding legs (rustls over the framed transport; certs from the fleet CA the operator already manages) or NetworkPolicy-enforced per-tenant segmentation with the trade recorded in the G-7 spec's Risks — decided and implemented in this task, with an e2e negative: a connection to a range compute's node port from outside its tenant's workloads is refused. Commit `feat(gres): distributed range placement (G-7b)`.

---

## Batch 4 — proof (run Tasks 7 and 8 in parallel; disjoint files)

### Task 7: The system suites — multiprocess, bank, Elle

**Files:** Vendor+re-target the donor harness (`crates/crabgresql/tests/harness/mod.rs` → `crates/gres-ranges/tests/harness/`— spawn `crabka-gres` children + an in-process broker; control channel for kill/respawn/partition), then `multiprocess.rs`, `jepsen_bank.rs`, `participant_kill_bank.rs`, `range0_cascade_kill_bank.rs`, `range0_leader_kill_drain.rs` (kill-the-writer replaces elections; "drain" = fence + prologue), `crossrange_2pc_nemesis.rs`, and **`jepsen_elle.rs`** (the stateright list-append linearizability checker over real processes — the headline gate). nextest groups sized per the donor's `.config/nextest.toml` precedents.

Steps: harness first (deterministic, no-sleep — condition-driven readiness as everywhere), then suites one at a time keeping donor names. Commit per suite; final commit `test(gres): jepsen bank + Elle strict-serializability over substrate ranges`.

### Task 8: The scaling demo (gate)

**Files:** Create `scripts/gres-range-scaling.sh` + a CI leg (artifact-only job like the cold-start pipeline).

Range-local workload (per-range tables, K concurrent sessions per range) against 1, 2, 4 ranges on the same broker; measure aggregate committed txn/s; emit `range-scaling.json`; assert monotone scaling with a generous linearity floor (e.g. 4 ranges ≥ 2.5× the 1-range baseline in CI conditions — the number lives in one place, environment-qualified) and publish the curve per PR. Commit `ci: gres range-scaling demo`.

## Completion checklist (maps to the G-7 gate)

- All eight ported models + the fence-ordering action green (Task 2).
- Ported 2PC/routing/durability suites green in-process (Task 4) and distributed (Tasks 5–7).
- `jepsen_elle` strict-serializability over real range-compute processes (Task 7).
- Measured ~N× range-local commit scaling, published (Task 8).
- Single-range tenants: conformance baseline and every existing gate untouched throughout.
