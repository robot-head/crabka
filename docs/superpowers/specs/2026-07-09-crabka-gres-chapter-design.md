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
- **Zero broker changes:** the engine journals over the ordinary Kafka wire, exactly the posture PG-1 resolved for the safekeeper — the durability tier inherits the diskless-WAL upgrades (classic today, fsync, then quorum) with no engine change.
- **Shared-substrate co-location as the moat:** tenant databases, topics, blobs, and (later) the PG tier's pages ride the same log and the same bucket; the FDW makes the cluster's own topics queryable from any tenant database.
- **The donor's conformance culture, preserved whole:** vendoring must provably regress nothing, and every new gres feature keeps the differential-oracle + Stateright discipline.

## Non-goals

- **C extensions / external plugins** — definitionally out for this engine; tenants who need them are the PG chapter's tier.
- **Read replicas and branching** — they need a disaggregated KV store (the "approach B" follow-on named below), not the v1 checkpoint model.
- **Vendoring or forking PgDog** — it is AGPL-3.0 and stays an external co-deployed service consuming generated configuration.
- **pgbench parity claims** — pgbench requires primary keys, which the engine does not have yet; the chapter's gates use the conformance corpus and a deterministic synthetic OLTP workload instead.
- **SQL-surface completeness as a chapter gate** — constraints, indexes, and window functions continue as ongoing breadth slices (the donor's SP41 constraints design imports as reference material), but the chapter's slices gate on durability and lifecycle, not breadth.

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

The reusable crates are imported once (donor history stays in the donor repo; the import commit records the donor SHA) and become ordinary workspace members: `crabka-pgtypes`, `crabka-pgparser`, `crabka-pgwire`, `crabka-pgkv`, `crabka-pgmvcc`, `crabka-pgcatalog`, `crabka-pgexec`, `crabka-gres` (the compute service), `crabka-gres-fdw`, and `crabka-gres-conformance` (`publish = false`). Depending on crabgresql as an upstream was rejected (two repos to co-evolve, unpublished git dependencies block crates.io releases), as was rewrite-with-reference (discards a working, tested engine). The donor's `cluster` crate and node binary are not imported: single-writer computes need no per-tenant consensus, and its 2PC/linearizable-read machinery solves problems the substrate already owns. crabgresql is PostgreSQL-licensed (Apache-2.0-compatible); imported files keep their copyright notice and the license is recorded in `NOTICE`.

Adaptation on import is mechanical: both trees are edition 2024; the crates adopt workspace lints (they already forbid `unsafe`; the pedantic set means a one-time cleanup, not behavior change), reuse existing workspace deps (`rustls`, `zerocopy`, `stateright`, `proptest`, `tokio-postgres`), and add `fjall`, `jiff`, `bigdecimal`, and `dashu-float`. `pg_query` remains a dev-only parser oracle behind a feature. The FDW switches from published `crabka-*` 0.3 crates to path dependencies and sheds its off-by-default `kafka` feature gate — inside this workspace, pure-Rust is the ambient guarantee.

### Substrate WAL via ordinary produce, with commit gated on durable ack

`SubstrateKv` journals every `write_batch`, in order, as a framed record (`GRW1`: generation, sequence, serialized ops) to the tenant's single-partition internal topic. In the donor's MVCC, in-transaction tuple writes hit the KV store before commit and commit itself is a clog flip — so local apply stays immediate (preserving read-your-writes), pre-commit batches pipeline asynchronously, and only the clog-flip batch must be produce-acked (`acks=all`) before `COMMIT` returns; single-partition ordering means that ack implies every preceding batch is durable too. The working store may run ahead of the log for in-flight transactions — it is disposable, and state that dies with the process was never acknowledged to a client. A produce failure or timeout aborts the SQL transaction with a clear error — never ack-then-hope.

Alternatives rejected: a disaggregated object-store-native KV layer store (instant spin-up at any size, replica/branching-ready — the right *evolution*, a whole storage program before first boot; named as the follow-on), and embedding the engine in the broker against internal `WalStore` APIs (lowest latency, but couples engine lifecycle and isolation to the broker and gives up zero-broker-changes). The `GRW1` framing and checkpoint manifests are designed so the follow-on can replace the storage behind the same `Kv` trait without touching the engine.

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
`SubstrateKv` + the `Committer` implementation, `GRW1` framing, transactional-producer fencing; recovery by full replay (no checkpoints yet). **Gate:** deterministic kill−9/respawn with zero acked-transaction loss; a fenced stale compute provably cannot commit.

### G-3 — Checkpoints
Snapshot upload, manifest-last ordering, DeleteRecords truncation, fast spin-up. **Gate:** spin-up bounded by snapshot-download + tail-replay, and a Stateright model of the WAL/checkpoint/recovery/fencing protocol (the donor's SP21 torn-commit is the cautionary tale for skipping this).

### G-4 — Front door
Tenant provisioning, PgDog config generation and reload, operator-managed computes, per-tenant SCRAM. **Gate:** N tenants served through one PgDog endpoint with per-tenant isolation.

### G-5 — Lifecycle
Idle suspend, spawn-on-demand, and a cold-start SLO for small tenants (fencing makes respawn safe; this slice makes it fast and automatic).

### G-6 — FDW and SQL breadth
`crabka-gres-fdw` exposes cluster topics as foreign tables inside tenant databases (Avro/JSON/Protobuf via Schema Registry, `IMPORT FOREIGN SCHEMA`). SQL-breadth slices (constraints first, per the donor's SP41 design, then indexes, window functions, …) continue indefinitely as their own design cycles. G-6 depends only on G-1 and runs parallel to G-2…G-5.

```
G-1 ──► G-2 ──► G-3 ──► G-4 ──► G-5
  └────► G-6 + SQL-breadth slices (parallel track)
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
