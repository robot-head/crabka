# Gres G-2: Substrate WAL — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design. The second slice of [Chapter Gres](2026-07-09-crabka-gres-chapter-design.md): a gres tenant's durable truth moves from local fjall to a per-tenant Crabka topic, making the compute disposable. Three chapter-spec refinements were resolved in this cycle and are recorded below.

## Context — what the tree actually holds

Three verified facts shape this design (each re-checked against the sources, not taken from memory):

1. **The donor engine was built for exactly this seam.** `SqlEngine::replicated(catalog_kv, sm_kv, committer, linearizer)` puts the engine in `PersistMode::Replicated`, where every transactional mutation — per-statement row-version batches, the clog flip at COMMIT/ROLLBACK, DDL batches, and crucially the `next_xid` and per-table rowid counters — flows through `Committer::commit(ops: Vec<WriteOp>) -> Result<(), ExecError>` (`crates/pgexec/src/commit.rs`), whose documented contract is "returns only once the batch is durable … and applied". The engine never writes its store directly in this mode; the store (`sm_kv`) is a pure read model, and for a single-range engine `catalog_kv` is the same `Arc`. The cluster crate's `RaftCommitter` was the intended consumer of this seam; this slice replaces raft with the topic. One wart, verified: **FDW-object DDL bypasses the seam in every mode** — `catalog::create_fdw/create_server/create_user_mapping/create_foreign_table` and their drops write directly to the kv store (`crates/pgcatalog/src/lib.rs`), a pre-existing donor bug that would have silently skipped replication under raft too.
2. **Replay is not blind-LWW-safe.** The donor's replicated state machine applies batches with two merge rules and documents why: counter keys (`/0/meta/next_xid`, `/0/seq/*`) **max-merge** because concurrent sessions fold counter ops at allocation time, so journal order can carry non-monotone values; clog keys are **write-once, first terminal decision wins**, because an abort race can journal two terminal decisions for one xid. Everything else is last-writer-wins. Those rules live in the non-vendored cluster crate (~50 lines, `store.rs`/`durable.rs`) and must be reimplemented here.
3. **Fencing genuinely requires the transactional produce path.** The workspace producer has a complete transactional API (`init_transactions`, `begin_transaction`, commit/abort, KIP-890 epoch bumps), and the broker checks the coordinator's authoritative epoch on **every** transactional produce (`INVALID_PRODUCER_EPOCH` = 47 for zombies; `InitProducerId` on an existing transactional id aborts the predecessor's open transaction before bumping). Idempotent-only produce has no cross-incarnation fence at all: without a transactional id each producer instance gets a fresh pid, and per-partition producer state never compares different pids. The broker also **rejects transactional batches on diskless partitions** (`INVALID_TXN_STATE`) — which forces the third refinement below.

## Refinements to the chapter spec (resolved in this cycle)

- **The WAL rides Kafka transactions, not bare `acks=all` produce.** The chapter described plain journaling with transactional-id fencing alongside; grounding showed the coordinator-checked transactional path is the only authoritative fence. One Kafka transaction per commit-group (below); SQL `COMMIT` is acknowledged after its group's `EndTxn(commit)`.
- **Statement writes await durability in v1.** The chapter said pre-commit batches "pipeline asynchronously" with local apply immediate; the `Committer` contract the donor's engine and tests actually pin is await-per-batch (exactly how the raft cluster ran it, per-statement `client_write`). v1 keeps that contract — group commit amortizes the round trips — and the async-statement-ack variant (apply locally at enqueue, flush at the flip) is a named latency optimization the writer-task architecture already accommodates, to be taken up only with latency evidence.
- **"Durability tier inherits diskless upgrades" gains a qualifier.** Because diskless partitions reject transactional batches today, `__gres_wal.*` topics stay classic-tier until the diskless track supports transactional produce. This is a named cross-track dependency, not a silent assumption; the classic path's fsync-tier improvements still apply.

## Design Goals

- **Disposable compute:** kill a gres compute at any instant; a successor reconstructs exactly the acknowledged state from the topic and serves. No acked transaction is ever lost; no unacked transaction ever resurrects.
- **Zero engine forks:** the engine is consumed through its public seams (`replicated`, `Committer`, `Linearizer`); the only engine change is fixing the FDW-DDL bypass, which is upstream-correct in every mode.
- **Zero broker changes:** ordinary transactional produce, fetch, ListOffsets, and CreateTopics over the Kafka wire.
- **Conformance-neutral:** the substrate-backed engine must reproduce the same conformance parity as local mode — the seam must change nothing observable.

## Non-goals

- **Checkpoints, truncation, and bounded spin-up** — G-3 (recovery here is full replay from offset 0).
- **Multi-tenant computes, provisioning, PgDog** — G-4/G-5.
- **Async statement acknowledgement** — named optimization, needs latency evidence first.
- **Cross-tenant transactions** — one tenant, one topic, one writer; nothing spans tenants.

## Architecture Overview

```
crabka-gres --substrate --tenant t1 --bootstrap broker:9092
   │
   ├─ SqlEngine::replicated(store, store, SubstrateCommitter, SubstrateLinearizer)
   │     store = local read model (MemKv, or FjallKv on an ephemeral dir)
   │
   ├─ SubstrateCommitter ── mpsc ──► WAL-writer task (single writer owns the log)
   │                                   loop: drain queue → begin_transaction
   │                                     → produce each batch as one GRW1 record
   │                                     → EndTxn(commit) → apply group to store
   │                                     → ack every waiter in the group
   │                                   fencing error (47) → set fenced flag → exit
   │
   └─ recovery (before serving):
        init_transactions()             ← fences predecessors, aborts their open txn
        stable_end = ListOffsets(-1, read_committed)
        replay [0, stable_end) via fetch(READ_COMMITTED), merge-rule apply
        reseed_counters() → serve

topic: __gres_wal.<tenant>  (1 partition, cleanup.policy=delete, retention.ms=-1)
record: GRW1 frame = u8 version | u64 journal_seq | u32 op_count | ops
        (op = u8 tag | u32 klen | key | u32 vlen | value; proptest round-tripped)
```

## Key Design Decisions

### The Committer seam, in Replicated mode — not a Kv wrapper

Wrapping `Kv::write_batch` was the chapter's sketch, but Replicated mode exists precisely to force the counter allocations into the batch stream: in Durable mode (`SqlEngine::open`/`with_kv`), `ProcArray::begin_write` and `SequenceManager::alloc` write the store directly on every allocation, so a Kv wrapper would journal per-allocation micro-batches and interleave them with statement batches nondeterministically. Through `replicated`, the journal is exactly the donor's proven replication stream. The store passed as both `catalog_kv` and `sm_kv` is local and disposable — `MemKv` for tiny tenants, `FjallKv` on an ephemeral directory otherwise (an accelerator for restarts that keep the disk, never the truth).

### One WAL-writer task; one Kafka transaction per commit-group

A transactional producer admits one open transaction at a time, and concurrent sessions call `commit()` concurrently — so a single writer task owns the producer (the workspace's single-writer-task idiom) and group-commits: drain everything queued, produce each `Vec<WriteOp>` as one framed record inside one Kafka transaction, `EndTxn(commit)`, apply the group to the local store in order, then ack every waiter. Queue order is journal order is apply order. A produce or EndTxn failure fails every waiter in the group (`ExecError::Unavailable`; sessions abort), and nothing from a failed group is applied locally — the store never runs ahead of the durable log, which keeps a later G-3 checkpoint trivially consistent. Kafka-transaction *visibility* is not load-bearing for data batches (each batch is one record, atomic by itself); the transaction buys the per-produce authoritative epoch check, and READ_COMMITTED replay additionally skips any group whose EndTxn never landed — a group that was never acked to anyone.

### Fence first, then replay to the stable end, then serve

Recovery order matters: `init_transactions()` first (the epoch bump fences every predecessor and aborts any open transaction it left), then read the partition's stable end, then replay `[0, stable_end)` at READ_COMMITTED. After the fence no new committed record can appear, so the replay endpoint is final; aborted zombie tails are invisible at READ_COMMITTED. Replay applies each frame's ops with the reimplemented merge rules — max-merge for `/0/meta/next_xid` and `/0/seq/*`, first-terminal-wins for `/0/clog/*`, LWW for the rest — then `reseed_counters()` lifts the in-memory allocators, exactly the donor's leadership-rise path. A fenced running compute learns it is fenced from the writer's first 47: the writer sets a shared fenced flag and the process exits; `SubstrateLinearizer::ensure_readable` checks the same flag so even the read path refuses once fenced (a zombie serving stale reads forever is not a v1 behavior we accept silently).

### `journal_seq` is a replay tripwire, not a protocol

Kafka's idempotent sequencing already dedups and orders within the producer session; the frame's `journal_seq` (monotone per generation, logged with the producer epoch) exists so replay can assert continuity and fail loudly on the impossible — a gap or regression means broker-side truncation or a framing bug, and recovery must refuse to serve rather than reconstruct silently wrong state.

### The FDW-DDL bypass gets fixed at the source

`CREATE/DROP` of foreign-object catalog entries gains ops-returning catalog functions (mirroring the existing `create_table_ops`/`drop_table_ops` shape), and the executor's FDW-DDL arms return those ops for the session to commit through the seam like every other DDL. This is behavior-preserving for local mode, makes FDW definitions durable on the substrate, and fixes latent replication-skip for any future clustered use. It lands with its own tests (FDW DDL → kill → replay → definitions present).

## Integration

- **New crate `crates/gres-substrate`** (`crabka-gres-substrate`, `publish = false`): `SubstrateCommitter`, `SubstrateLinearizer`, the WAL-writer task, GRW1 framing, merge-rule apply, recovery, and topic-ensure (AdminClient `create_topics` with `{cleanup.policy=delete, retention.ms=-1}`, tolerate `TOPIC_ALREADY_EXISTS` — the `__remote_log_metadata` idiom).
- **`crates/gres`**: new `--substrate` mode flags (`--bootstrap`, `--tenant`, optional `--cache-dir` for the fjall read model); local mode is unchanged and remains the default.
- **`crates/pgexec` + `crates/pgcatalog`**: the FDW-DDL seam fix.
- **Clients used:** `crabka-client-producer` (transactional), `crabka-client-core` (`fetch_partition_with_isolation`, `ListOffsets` via `Client::send`), `crabka-client-admin` (`create_topics`).
- **Broker:** none. The diskless-transactions dependency is tracked against the diskless track, not solved here.

## Kafka / wire compliance

All traffic is standard Kafka wire: transactional produce with KIP-890 epoch semantics, READ_COMMITTED fetch, ListOffsets, CreateTopics. The slice leans on Crabka's existing KIP-faithful transaction coordinator rather than extending it; `__gres_wal.*` follows the client-side internal-topic conventions (`__remote_log_metadata`, `__diskless_wal_index`).

## Testing

- **Framing and merge rules (unit/proptest):** GRW1 round-trips under proptest; merge-rule apply pinned by ports of the donor's `durable.rs` cases (counter non-regression, first-terminal-wins on clog races).
- **Conformance neutrality (the slice's parity gate):** the conformance corpus runs against `crabka-gres --substrate` (in-process or CI-side broker) and must match `crates/gres-conformance/baseline.json` exactly — the substrate changes nothing observable.
- **Disposability (deterministic, no-sleep):** drive committed and in-flight transactions over pgwire against a substrate engine on an in-process broker (the `KafkaStack` harness precedent from gres-fdw); drop the compute without shutdown; recover a successor; assert every acked transaction visible and every unacked one absent — waits are condition-driven (acked offsets, applied counters), never settle-sleeps.
- **Fencing:** start compute B for the same tenant while A still holds sessions; assert A's next commit fails and A exits; read the journal and assert no A-record appears after B's fence point; assert B replayed exactly A's acked prefix.
- **FDW DDL durability:** create foreign objects, kill, recover, assert definitions replayed.
- **Model checking:** the Stateright model of the fence/replay/checkpoint protocol lands with G-3 (per the chapter), where truncation makes the state space interesting; G-2's invariants (fence-before-serve, journal_seq continuity) are inputs to that model.

## Risks

- **Commit latency is produce + EndTxn per group** — amortized by group commit; the async-statement-ack optimization is designed-for but deliberately deferred until measured.
- **Transaction timeout vs. slow groups:** a group must produce and end within `transaction.timeout` (builder-configurable, default 60 s); groups are drained continuously by a dedicated task, so a timeout indicates a broker outage, which already fails the group loudly.
- **Coordinator single-broker caveat:** the broker's inter-broker transactional auto-add is not finished ("trusts the producer" when it doesn't hold the tid's state); single-partition WAL topics keep the produce leg on one partition leader, and the dependency is on the broker track's existing roadmap.
- **Full replay from offset 0** makes restart time proportional to history until G-3 lands — accepted, G-3 is next.
- **Diskless inheritance is qualified** (see refinements): classic-tier only until diskless supports transactional batches.

## Resolved decisions

- Seam: `SqlEngine::replicated` + `SubstrateCommitter`/`SubstrateLinearizer`; local store is a disposable read model (MemKv or ephemeral fjall); no Kv wrapper.
- Journal: one GRW1 record per `Committer` batch; single WAL-writer task; one Kafka transaction per commit-group; ack after `EndTxn(commit)`; apply-after-durable.
- Fencing: `transactional.id = __gres.<tenant>`; fence-then-replay-then-serve; fenced flag fails reads and exits the process.
- Replay: READ_COMMITTED to the fence-time stable end; max-merge counters; write-once clog; `journal_seq` continuity or refuse to serve.
- Engine change: FDW-DDL routed through the seam (ops-returning catalog functions).
- Chapter refinements: transactional produce as the fence; await-per-batch v1; diskless-tier qualifier.
