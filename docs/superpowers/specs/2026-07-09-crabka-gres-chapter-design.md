# Chapter Gres — a pure-Rust Postgres compute engine on the Crabka substrate

**Date:** 2026-07-09
**Status:** Approved
**Type:** Chapter design. Adds a second, pure-Rust tenant tier to the serverless-Postgres story by vendoring the [crabgresql](https://github.com/robot-head/crabgresql) engine into the workspace and rebasing its storage seam onto Crabka durability. Complements — does not replace — the [PG chapter](2026-07-06-crabka-postgres-chapter-roadmap-design.md)'s real-Postgres compute (PG-5b).

## Context — what this chapter is and is not

The PG chapter rebuilds Neon's disaggregated architecture around a *real* patched Postgres 17 compute: byte-faithful pages, physical WAL, C extensions, and the workspace's one sanctioned `unsafe` boundary (`crabka-compute-client`). That tier buys full fidelity at the cost of the C toolchain, the vendored core patch, and real-Postgres resource weight per tenant.

Chapter Gres adds the opposite trade: a from-scratch, pure-Rust, Postgres-*compatible* engine — compatible at the wire and SQL-semantics level, not at the page or physical-WAL level. It has no C anywhere, so external C plugins (the Postgres extension ecosystem) are out of scope for this engine *by definition*, and the `unsafe`-forbidden workspace lint applies to every crate in it. Its unit of tenancy is a disposable single-writer compute process whose durable truth lives entirely on the Crabka substrate: a per-tenant internal topic for the write-ahead log and the object-store bucket for checkpoints.

The donor is crabgresql: ~68k lines of Rust 2024 built as independently shippable slices with a differential-oracle culture (a conformance corpus diffed against a real `postgres:18` over the wire in CI, proptest, Stateright models, cargo-mutants, fuzzing). It contributes a complete pgwire v3 implementation (extended query protocol, SCRAM-SHA-256, rustls TLS), a hand-written parser with a `pg_query` dev-only oracle, a PostgreSQL-faithful MVCC layer (xid/clog/snapshot/`HeapTupleSatisfiesMVCC`), a catalog, a ~16k-line tree-walking executor, a pluggable ordered-KV storage trait with in-memory and fjall (pure-Rust LSM) backends, and a Kafka foreign-data wrapper that already consumes the published `crabka-*` client crates. Its openraft cluster layer (multi-range, cross-range 2PC, linearizable reads) is deliberately **not** imported: in this chapter the substrate owns replication and durability, so per-tenant consensus would duplicate Crabka's own quorum infrastructure.

## Design Goals

- **Serverless per-tenant Postgres as the product:** many small Postgres-compatible databases, each a topic + a bucket prefix + at most one live compute; computes are disposable and recover entirely from the substrate.
- **Zero broker changes:** the engine journals over the ordinary Kafka wire, exactly the posture PG-1 resolved for the safekeeper — the durability tier inherits the diskless-WAL upgrades (classic today, fsync, then quorum) with no engine change. *(Qualified during the G-2 design: the WAL uses transactional produce, which diskless partitions reject today — `__gres_wal.*` topics stay classic-tier until the diskless track supports transactional batches; a named cross-track dependency.)*
- **Shared-substrate co-location as the moat:** tenant databases, topics, blobs, and (later) the PG tier's pages ride the same log and the same bucket; the FDW makes the cluster's own topics queryable from any tenant database.
- **The donor's conformance culture, preserved whole:** vendoring must provably regress nothing, and every new gres feature keeps the differential-oracle + Stateright discipline.

## Non-goals

- **C extensions / external plugins** — definitionally out for this engine; tenants who need them are the PG chapter's tier.
- **Read replicas and branching** — they need a disaggregated KV store (the "approach B" follow-on named below), not the v1 checkpoint model.
- **Vendoring or forking PgDog** — it is AGPL-3.0 and stays an external co-deployed service consuming generated configuration.
- **pgbench parity claims** — pgbench requires primary keys, which the engine does not have yet; the chapter's gates use the conformance corpus and a deterministic synthetic OLTP workload instead.
- **SQL-surface completeness as a chapter gate** — constraints, indexes, and window functions continue as ongoing breadth slices (the donor's SP41 constraints design imports as reference material), but the chapter's slices gate on durability and lifecycle, not breadth.

## Scaling model and ceilings

*(Added after the chapter-wide scaling review, which asked: "can it horizontally scale postgres infinitely, and where does it break first?" The honest answer belongs in the chapter, stated once, plainly.)*

**The chapter scales on two axes: tenant count, and — since G-7/G-8 joined the chapter — ranges within a tenant.** *(This section originally read "one database: no, structurally, ever"; the [G-7](2026-07-09-crabka-gres-g7-multirange-design.md)/[G-8](2026-07-09-crabka-gres-g8-sharded-tables-design.md) slices revise it.)* One database's write throughput scales linearly across **ranges**: table-granular in G-7 (the donor's ported router/2PC layer over topic-per-range substrate durability), and within a single table in G-8 (rowid-interval sharding, global-visibility tables, online checkpoint-fork splits). The remaining honest ceilings: a sharded table's aggregate **commit rate** is bounded by range-0's batched decision throughput under G-8a (order 10⁴ commits/s per tenant) **until [G-9a](2026-07-09-crabka-gres-g9-distributed-maturity-design.md) lands** — timestamp transactions distribute decisions to primary ranges and the commit rate scales with ranges too, leaving only per-statement latency floors (durability round trips amortized by group commit) and the TSO as a measured observation point rather than a projected ceiling. And the scaling is **not gated on vendor SQL**: once G-9e's conversion goal lands, a plain unadorned `CREATE TABLE` is auto-converted and split by policy when it grows — `SHARDED`/`SHARDED BY HASH` remain as optional pre-shard hints, and the compatibility boundary a normal Postgres application actually meets is the engine's SQL-surface breadth (the G-6 track's named backlog), not the scaling machinery.

**Per-range envelope (v1, approach A, order-of-magnitude):** the vendored executor is a full-scanning, materializing tree-walker with no indexes, so hot tables are comfortable to ~10⁴ rows and degrade visibly by ~10⁵–10⁶ row-versions *per range* (G-8 splits spread a table across ranges; indexes — the G-6 breadth cycle — matter even more once tables shard); nested-loop joins constrain multi-table queries well below that. The commit path awaits durable produce per statement (group-commit amortized), a few-ms floor per write statement and low-thousands of commits/s per range. Checkpoint, suspend, and cold-start costs scale with live store size per range (G-3's garbage horizon keeps that live, not historical). Tenants that outgrow single-range envelopes grow ranges (G-7/G-8); tenants needing C extensions or byte-level fidelity graduate to the PG chapter's tier; approach B remains the size/spin-up/read-replica evolution.

**Fleet envelope and the cell posture:** one `Gres` fleet (one PgDog cell + one aggregated config) is sized for **~10³ tenants**; beyond that, the horizontal unit is more cells. Known ceilings the designs carry: the fleet config pipeline (O(N) render, 1 MiB Secret cap ≈ 10⁴ tenants hard), Kubernetes object fan-out (one Deployment/Service per tenant), and — the binding broker-side costs — per-topic replication overhead for many tiny topics (idle follower fetch loops with dedicated connections; mitigated by G-5's topic parking) and O(N-topics²) metadata scans in the broker's replication supervisor and metadata handlers. The latter two are **named cross-track broker dependencies** (fetch multiplexing; indexing partitions by topic), not gres-chapter work.

**What breaks first:** for one growing tenant — the engine's read path (full scans until the index breadth cycle lands), then, for sharded tables, the range-0 decision ceiling (G-8c territory). For the fleet — the config/RELOAD pipeline around ~10³ tenants, then broker per-topic overhead around ~10⁴ topics (which ranges multiply — the broker cross-track dependencies bind sooner with G-7/G-8 deployed). Kafka-as-log is the last thing to break on both axes.

## Architecture Overview

```
psql / drivers
      │ pgwire
   PgDog   (external, AGPL, co-deployed: pooling, database→compute routing, failover)
      │ pgwire
crabka-gres compute   (per tenant, single writer, disposable, pure Rust)
   ├─ engine: pgwire ▸ pgparser ▸ executor ▸ mvcc ▸ catalog     (vendored, unchanged)
   └─ SubstrateKv (implements the donor's Kv trait)
        ├─ local working store: fjall LSM on ephemeral disk (MemKv for tiny tenants)
        ├─ WAL: every write_batch → __gres_wal.<tenant> (single partition, acks=all,
        │        transactional producer id __gres.<tenant> — epoch fences zombies)
        └─ checkpoints: consistent snapshot @ WAL offset → gres/<tenant>/ckpt/… ;
           manifest written last; topic truncated (DeleteRecords) up to the offset

spin-up: latest manifest → download snapshot → replay topic tail → InitProducerId
         (fences any predecessor) → serve
```

Reads never leave the compute: the working store holds the whole database, and MVCC visibility is unchanged from the donor. Writes flow through the one seam the donor already guarantees atomic and durable (`Kv::write_batch`) plus the executor's existing `Committer` seam, which this chapter implements against the substrate instead of the donor's raft.

## Key Design Decisions

### A separate engine at the wire+SQL level, not a second compute for the pageserver

Three positions were considered: a pure-Rust compute speaking the PG chapter's `GetPage@LSN` (requires reading real page formats *and generating byte-faithful physical WAL* — a multi-year storage-engine program), a read-only pure-Rust compute over the pageserver, and a separate engine that is Postgres-compatible at the wire and SQL level with its own storage model. The third was chosen: it ships a usable product on the substrate without interlocking with PG-1…PG-4, and the two tiers remain complementary rather than redundant.

### Vendor the donor crates; leave the cluster layer behind

The reusable crates are imported once (donor history stays in the donor repo; the import commit records the donor SHA) and become ordinary workspace members: `crabka-pgtypes`, `crabka-pgparser`, `crabka-pgwire`, `crabka-pgkv`, `crabka-pgmvcc`, `crabka-pgcatalog`, `crabka-pgexec`, `crabka-gres` (the compute service), `crabka-gres-fdw`, and `crabka-gres-conformance` (`publish = false`). Depending on crabgresql as an upstream was rejected (two repos to co-evolve, unpublished git dependencies block crates.io releases), as was rewrite-with-reference (discards a working, tested engine). The donor's `cluster` crate and node binary are not imported: single-writer computes need no per-tenant consensus for *replication* — the substrate owns that. Be precise about what this gives up, though *(corrected after the scaling review)*: the cluster layer was also the donor's range-**partitioning** and cross-range-2PC machinery, i.e. the only path in this program's universe to scaling one database's writes across processes. The substrate does not own that problem; dropping the crate makes single-writer-per-tenant a structural property of the *tenant tier* (see "Scaling model and ceilings"). *(Revision: [G-7](2026-07-09-crabka-gres-g7-multirange-design.md) revives exactly the non-raft two-thirds of that layer — router, 2PC, GTM, recovery disciplines — over topic-per-range substrate durability, and [G-8](2026-07-09-crabka-gres-g8-sharded-tables-design.md) builds the donor's unbuilt single-table sharding on top; the raft storage/consensus third stays dropped.)* crabgresql is PostgreSQL-licensed (Apache-2.0-compatible); imported files keep their copyright notice and the license is recorded in `NOTICE`.

Adaptation on import is mechanical: both trees are edition 2024; the crates adopt workspace lints (they already forbid `unsafe`; the pedantic set means a one-time cleanup, not behavior change), reuse existing workspace deps (`rustls`, `zerocopy`, `stateright`, `proptest`, `tokio-postgres`), and add `fjall`, `jiff`, `bigdecimal`, and `dashu-float`. `pg_query` remains a dev-only parser oracle behind a feature. The FDW switches from published `crabka-*` 0.3 crates to path dependencies and sheds its off-by-default `kafka` feature gate — inside this workspace, pure-Rust is the ambient guarantee.

### Substrate WAL via ordinary produce, with commit gated on durable ack

`SubstrateKv` journals every `write_batch`, in order, as a framed record (`GRW1`: generation, sequence, serialized ops) to the tenant's single-partition internal topic. In the donor's MVCC, in-transaction tuple writes hit the KV store before commit and commit itself is a clog flip — so local apply stays immediate (preserving read-your-writes), pre-commit batches pipeline asynchronously, and only the clog-flip batch must be produce-acked (`acks=all`) before `COMMIT` returns; single-partition ordering means that ack implies every preceding batch is durable too. The working store may run ahead of the log for in-flight transactions — it is disposable, and state that dies with the process was never acknowledged to a client. A produce failure or timeout aborts the SQL transaction with a clear error — never ack-then-hope.

Alternatives rejected: a disaggregated object-store-native KV layer store (instant spin-up at any size, replica/branching-ready — the right *evolution*, a whole storage program before first boot; named as the follow-on), and embedding the engine in the broker against internal `WalStore` APIs (lowest latency, but couples engine lifecycle and isolation to the broker and gives up zero-broker-changes). The `GRW1` framing and checkpoint manifests are designed so the follow-on can replace the storage behind the same `Kv` trait without touching the engine. To be explicit about what the follow-on buys *(qualified after the scaling review)*: approach B removes the size-proportional spin-up/checkpoint costs and enables read replicas and branching — it does **not** buy write scale-out; the single-writer commit path is structural in approaches A and B alike.

### Single-writer fencing reuses Kafka EOS

Two computes for one tenant must never interleave writes. Rather than inventing a lease service, the compute produces with `transactional.id = __gres.<tenant>`: a successor's `InitProducerId` bumps the producer epoch and the broker fences the predecessor, which gets hard produce errors and self-terminates. Crabka already implements transactional produce with KIP-faithful semantics, so correct failover costs zero new broker machinery, and recovery-then-fence ordering (replay to HWM, then `InitProducerId`, then serve) makes respawn safe by construction.

### Checkpoints bound both spin-up and log growth

A background checkpointer snapshots the working store at a point where the applied state equals the durable log prefix (so the snapshot corresponds exactly to a WAL offset), uploads it under the tenant's bucket prefix using the existing object-store plumbing, and writes the manifest last so a torn upload is invisible. After a checkpoint lands, the WAL topic is truncated up to the covered offset via DeleteRecords, so the replay tail stays bounded by checkpoint cadence. Recovery refuses to serve if the manifest's covered offset is beyond the topic high-water mark (evidence of torn truncation) — loud, not silent. Spin-up cost scales with database size, which fits the many-small-tenants product; removing that scaling is exactly the named follow-on's job.

### PgDog is the front door; Crabka owns the control plane

PgDog (AGPL-3.0, Rust) provides pooling, database-name routing, and failover as a co-deployed external service — the pgbouncer posture, kept outside the Apache-2.0 workspace and never forked. Crabka's control plane owns what PgDog cannot: tenant provisioning (CLI/admin API), compute placement through the existing Kubernetes operator (computes are pods), generated PgDog configuration with reloads, and the idle-suspend/spawn-on-demand lifecycle. Per-tenant SCRAM credentials are control-plane data: static configuration in early slices, Crabka security integration later.

## The slices

### G-1 — Vendor the engine
Crates imported and renamed, lints and deps merged, conformance corpus + oracle harness running in Crabka CI against a `postgres:18` service container (CI already runs postgres containers for `connect-postgres`), and `crabka-gres` serving a single tenant on local fjall. **Gate:** the donor repo's parity baseline reproduced in Crabka CI — vendoring regressed nothing.

### G-2 — Substrate WAL
`SubstrateKv` + the `Committer` implementation, `GRW1` framing, transactional-producer fencing; recovery by full replay (no checkpoints yet). **Gate:** deterministic kill−9/respawn with zero acked-transaction loss; a fenced stale compute provably cannot commit. *(Refined during the [G-2 design](2026-07-09-crabka-gres-g2-substrate-wal-design.md): the seam is `SqlEngine::replicated` + a `SubstrateCommitter` — not a `Kv` wrapper — because Replicated mode is what forces the xid/rowid counters into the batch stream; the WAL rides one Kafka transaction per commit-group, since the coordinator-checked transactional path is the only authoritative fence; statement batches await durability in v1 (async pipelining is a named optimization); and replay applies the donor's max-merge/write-once rules rather than blind LWW.)*

### G-3 — Checkpoints
Snapshot upload, manifest-last ordering, DeleteRecords truncation, fast spin-up. **Gate:** spin-up bounded by snapshot-download + tail-replay, and a Stateright model of the WAL/checkpoint/recovery/fencing protocol (the donor's SP21 torn-commit is the cautionary tale for skipping this). *(Refined during the [G-3 design](2026-07-09-crabka-gres-g3-checkpoints-design.md): snapshots come from a new streaming `SnapshotKv` seam in `crabka-pgkv` (fjall MVCC snapshot taken between commit-groups — the existing `Kv` scans materialize the whole store); checkpoints are immutable `ckpt/<offset>-<epoch>/` prefixes with the manifest written last, truncation via a new public `AdminClient::delete_records`; and G-2's replay terminator was amended to a post-fence barrier record, since the broker's ListOffsets ignores `isolation_level`.)*

### G-4 — Front door
Tenant provisioning, PgDog config generation and reload, operator-managed computes, per-tenant SCRAM. **Gate:** N tenants served through one PgDog endpoint with per-tenant isolation. *(Refined during the [G-4 design](2026-07-09-crabka-gres-g4-front-door-design.md): the tenant registry is a compacted `__gres_tenants` topic over the ordinary wire (the `_schemas` pattern — the KRaft path is broker-internal); `Gres`/`GresTenant` CRDs are intent reconciled by two new operator controllers, with PgDog config rendered by aggregation and reloaded via its admin database; auth is PgDog passthrough with the tenant's verifier living in the registry and enforced by the compute; a `crabka gres` CLI drives the same control plane for non-Kubernetes deployments.)*

### G-5 — Lifecycle
Idle suspend, spawn-on-demand, and a cold-start SLO for small tenants (fencing makes respawn safe; this slice makes it fast and automatic). *(Refined during the [G-5 design](2026-07-09-crabka-gres-g5-lifecycle-design.md): PgDog errors rather than queues on down backends, so wake rides an always-accepting activator that peeks the Postgres startup prelude, writes idempotent resume-request records, and pipes bytes once the compute recovers; suspend is compute-initiated after a final checkpoint with the controller executing scale-to-zero; the SLO is a measured pipeline with an environment-qualified CI ceiling. Amended after the scaling review: the wake path contains no config render or RELOAD — the activator pipes directly to the recovered compute and routing flips back lazily; suspended tenants' WAL topics are parked (deleted behind the final checkpoint, `wal_generation` bumped) so idle tenants stop costing the brokers; suspend is size-gated as policy.)*

### G-6 — FDW and SQL breadth
`crabka-gres-fdw` exposes cluster topics as foreign tables inside tenant databases (Avro/JSON/Protobuf via Schema Registry, `IMPORT FOREIGN SCHEMA`). SQL-breadth slices (constraints first, per the donor's SP41 design, then indexes, window functions, …) continue indefinitely as their own design cycles. G-6 depends only on G-1 and runs parallel to G-2…G-5. *(Refined during the [G-6 design](2026-07-09-crabka-gres-g6-fdw-sql-breadth-design.md): the FDW track is three gated items — record headers surfaced through the published `crabka-client-core` fetch API, protobuf decoding completed via `writer_message_type`, and a no-config default server targeting the tenant's own cluster; the breadth track is a standing process where each feature's corpus growth ratchets the parity baseline in the same reviewed commit.)*

### G-7 — Multi-range tenants (write scale-out, table-granular)
The donor's router / cross-range-2PC / GTM layers ported over substrate ranges — each range a topic-per-range G-2/G-3 compute; terms become producer epochs; the rise sweep becomes the fence-first recovery prologue; the range-0 replica becomes a READ_COMMITTED topic tail with a broker-log-derived barrier. **G-7a** in-process (all eight donor Stateright models + 2PC suites ported, plus a new fence-ordering action), **G-7b** distributed (transport, registry-based discovery, operator placement, jepsen_elle over pods). **Gate:** strict-serializability (Elle) over real range computes + a measured ~N× commit-throughput demo. See the [G-7 design](2026-07-09-crabka-gres-g7-multirange-design.md).

### G-8 — Sharded tables (single-table scale-out)
The donor's unbuilt "D4": rowid-interval boundaries (RangeMap v2), sharded tables as **global-visibility tables** on the existing g-timeline (leased g-blocks; batched range-0 decisions with the ceiling stated), scatter-gather execution at a new `RangeScanner` seam (0A000 lifted for all-global-visibility statements), and online **checkpoint-fork splits**. **G-8a** visibility + execution, **G-8b** splits/moves (the chapter's hairiest Stateright model), **G-8c** named research for unbounded commit rate. **Gate:** conformance corpus green on sharded tables + split-under-nemesis + linear single-table ingest demo. See the [G-8 design](2026-07-09-crabka-gres-g8-sharded-tables-design.md).

### The SQL-Parity Program
"Finish all SQL language support": full PostgreSQL 18 surface parity as ~30 wave cycles across six tracks (foundations/types/DDL/query/session/procedural), every one of the 190 PG18 commands answered in a CI-guarded `PG_COMPAT_MATRIX.md`, milestones defined by working software (M0 drivers — extended-protocol **parameters**, verified missing today, go first — M1 pgbench, M2 psql, M3 ORMs/migrations, M4 progressive pg_regress adoption, M5 all-answered), with explicit stock-PG-default dispositions for 2PC-SQL/databases/large-objects and named C-bound non-goals. Absorbs G-6's breadth track; every wave runs its ratchet process and states its sharded-table and pooler stories. See the [program design](2026-07-09-crabka-gres-sql-parity-program-design.md).

### G-9 — Distributed maturity
Closes the frontier G-8 left open, as five sub-slices: **G-9a** timestamp transactions — range 0 becomes a batched monotone timestamp oracle (stride-ahead durability) and decisions move to primary ranges as durable intents, removing the commit-rate ceiling and **superseding the g-timeline for sharded tables** (this resolves and closes "G-8c"); **G-9b** pushdown execution behind a light planner seam (predicates, projections, partial aggregates, top-K, broadcast/co-partitioned joins — equivalence-proven rewrites, no cost model); **G-9c** hash sharding as bucket-prefix interval sharding (the G-8 machinery applies unchanged; co-location groups); **G-9d** secondary indexes — local per-range first, global-on-ts-transactions after 9a, with placement constraints; **G-9e** a goal-based auto-rebalancer (split/move/merge through the G-8b orchestrator; merge designed as the inverse checkpoint-fork). See the [G-9 design](2026-07-09-crabka-gres-g9-distributed-maturity-design.md).

```
G-1 ──► G-2 ──► G-3 ──► G-4 ──► G-5
  │       │       └────► G-7a ──► G-7b ──► G-8a ──► G-8b   (G-7b also needs G-4;
  │                                          │       │      G-8b reuses G-5 parking)
  │                                          ├─► G-9a ──► G-9d(global; +G-6 indexes)
  │                                          ├─► G-9b      G-8b ─► G-9c ─► G-9e
  └────► G-6 + SQL-breadth slices (parallel track; its index cycle gates G-9d local)
```

## Integration

- **Broker:** none required. The WAL path is ordinary transactional produce + DeleteRecords over the Kafka wire; internal-topic naming (`__gres_wal.<tenant>`) follows the `__diskless_wal_index` precedent.
- **Object store:** the existing `ObjectStoreConfig`/`ObjectOps` plumbing, under `gres/<tenant>/` prefixes.
- **Operator:** gres computes and PgDog become managed workloads; provisioning wires topic creation, bucket prefix, credentials, and PgDog config together.
- **Rust clients:** the compute and the FDW consume the workspace's own `crabka-client-*` crates — the engine dogfoods the client stack.
- **PG chapter:** no code coupling. The two tiers share the substrate and the product frame; a tenant needing C extensions or byte-level fidelity graduates to the PG tier.

## Kafka / wire compliance

The engine's server-facing surface is the *Postgres* wire protocol; its Kafka-facing surface is a standard client. Nothing in this chapter changes Kafka wire behavior, and the fencing design leans on Crabka's existing KIP-faithful transactional-produce semantics rather than extending them. The FDW's snapshot reads use `READ_COMMITTED` isolation, as the donor already does.

## Testing

- **Conformance (differential):** the vendored corpus vs a real `postgres:18` oracle on every PR, parity reported; G-1 gates on matching the donor baseline, and breadth slices move it upward.
- **Durability and fencing (deterministic, no-sleep):** kill/respawn mid-workload with zero acked loss; stale-writer fencing; recovery refusal on torn truncation. Condition-driven waits only, per the donor's and Crabka's shared discipline.
- **Model checking:** a Stateright model of generation/offset/checkpoint invariants for the recovery protocol (G-3 gate).
- **Workload smoke:** a deterministic synthetic OLTP driver for spin-up and latency measurements (pgbench is out until primary keys exist).
- **Donor test freight:** the vendored crates bring their unit/property/integration suites; the FDW's tests run against in-workspace broker fixtures instead of published crates.

## Risks

- **Pedantic-lint churn on import** could smear the donor diff; mitigated by importing at a pinned donor SHA with lint cleanup as its own reviewable commit per crate.
- **Commit latency is a produce round-trip;** group commit amortizes it, and the diskless-WAL track's tiers change durability strength without engine changes — but latency-sensitive tenants are a known trade of approach A.
- **Checkpoint cost scales with DB size;** acceptable for the many-small-tenants product, removed by the disaggregated-store follow-on rather than patched here.
- **DeleteRecords-based retention makes the topic the only WAL copy between checkpoints;** the durability tier of the internal topic (quorum once diskless 6a lands) is therefore the tenant's durability tier — stated, not hidden.
- **AGPL hygiene:** PgDog stays an unmodified external deployment; any behavior Crabka needs lives in generated config or the control plane, never in a PgDog patch.

## Resolved decisions

- Positioning: separate wire+SQL-level engine on the substrate (not a pageserver compute); complements the PG chapter's C tier.
- Product: serverless per-tenant databases; single-writer disposable computes; substrate owns durability.
- Reuse: vendor into the workspace at a pinned SHA; cluster layer and node binary excluded; donor becomes archive/reference.
- Storage: approach A (WAL topic + checkpoints) with the disaggregated KV store as the named follow-on behind the same `Kv` seam.
- Fencing: Kafka transactional-producer epochs (`__gres.<tenant>`).
- Front door: co-deployed stock PgDog + Crabka-owned control plane.
- Naming: `crabka-pg*` for vendored engine crates, `crabka-gres*` for the service/FDW/conformance, `__gres_wal.<tenant>` topics, `gres/<tenant>/` bucket prefixes.
