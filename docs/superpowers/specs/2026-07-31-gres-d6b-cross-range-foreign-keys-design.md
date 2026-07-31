# Gres D6b cross-range foreign keys — design

**Date:** 2026-07-31
**Status:** Proposed — not scheduled, and deliberately written so that "not yet" is an easy conclusion to reach.
**Type:** Companion cycle to [D6 foreign keys, local engine](2026-07-31-gres-d6-foreign-keys-local-design.md). Extends referential integrity to sharded tables, whose rows, referenced keys, and referencing rows live on different ranges.

This document exists because the obvious reading of D6's integration section is wrong. That section says the cross-range case is a matter of routing the probe and lifting a lock key, and this design's first job is to explain why it is neither. Two things block: a foreign key's referent must be a unique key and a sharded table cannot have one; and a foreign key is a write-skew-shaped invariant while the engine is snapshot isolation with first-committer-wins. The first is a missing feature. The second is a property of the isolation level, and no amount of careful probing closes it.

## Design Goals

Enforce referential integrity on sharded tables with the same observable PostgreSQL 18.4 semantics D6 achieved locally, or refuse the constraint at DDL time with a typed error naming what is missing. A half-enforced foreign key is worse than a refused one: it looks like a guarantee and is not.

Add no waiting to the distributed write path. The timestamp transaction path never blocks — every conflict is a CAS or a scan at prewrite that fails fast into a retryable `40001` — and it has no distributed wait-for graph to make blocking safe. A design that introduces a durable distributed lock introduces distributed deadlock at the same moment, and the engine's only detector is a 2 s timeout that the timestamp path never arms (`gres-ranges/src/tenant.rs:5475-5485`, `gres-ranges/src/forward.rs:112`).

Rest the correctness argument on key identity rather than on timestamps. The clock this engine can be configured with under HLC gives snapshot isolation with an uncertainty window, not linearizability, and commit-wait was explicitly rejected (`2026-07-20-hlc-distributed-mode-design.md:22,32`). Any protocol whose safety depends on two nodes agreeing about real-time order is not implementable here.

Make the prerequisite chain visible rather than absorbed. Most of this design's cost is in one component — global unique enforcement — that is useful on its own and blocks several unrelated features. The document is ordered so that component can be built, evaluated, and shipped without any commitment to foreign keys.

## Architecture Overview

Nothing in this design is a foreign-key mechanism. Every part of it is a general capability the engine is missing, and the foreign key is the consumer that motivates the ordering.

**A global unique index becomes a routable object.** Today's global index is not global in placement: `prewrite_ops` writes its intents into the participant range's own KV alongside the base row (`pgexec/src/timestamp_txn.rs:2525-2530`), so the entries are a differently-encoded *local* index. Making the index sharded by indexed key — G-9d's actual proposal (`2026-07-09-crabka-gres-g9-distributed-maturity-design.md:77`) — gives every index key exactly one owning range, which is the single fact the rest of the design stands on.

**That owning range becomes the rendezvous.** Uniqueness, the child's "the parent exists" evidence, and the parent's "I am removing this key" intent all land on one range's totally-ordered WAL, where first-committer-wins can see them. The write skew that hides when a `DELETE parent` and an `INSERT child` touch disjoint rows on disjoint ranges stops hiding, because both transactions are made to touch the same key.

**The two-phase commit grows a validate phase.** Between the last participant prewrite and the primary's decision record, the gateway places the transaction's referential evidence on the key-owning ranges. Placing it *is* the check: the placement is conditional and fails when the key is absent or contended. A failure aborts before any decision is written, which is a path 2PC already has.

**Referential actions are excluded, not deferred cheaply.** A cascade is multi-statement, and sharded writes inside explicit transactions are refused at the engine (`pgexec/src/session.rs:5273-5277`) while multi-range scatter accepts plain `INSERT` only (`gres-ranges/src/tenant.rs:5722-5732`). This design supports `NO ACTION` and `RESTRICT` and refuses the rest at DDL time.

## Key Design Decisions

### Global uniqueness is the deliverable; foreign keys are its second consumer

A foreign key's referent must be a unique key. On a sharded table there is none and no way to make one: `CREATE TABLE ... SHARDED` refuses any index-backed constraint (`pgexec/src/exec.rs:343-347`), `ALTER TABLE ... ADD CONSTRAINT` refuses the same (`pgexec/src/exec.rs:13495-13501`), and `CREATE UNIQUE INDEX ... GLOBAL` is refused outright (`pgexec/src/exec.rs:708-712`, re-checked at `:4278-4283`, `:4388-4391`, `:5453-5457`).

So deleting D6's sharded refusals (`pgexec/src/fk.rs:226-227`, `:249-250`) does not produce a working cross-range foreign key; it produces a `42830` one function later, when `primary_key_columns` (`pgexec/src/fk.rs:355-362`) or `select_referenced_index` (`pgexec/src/fk.rs:371-399`) finds nothing to select. D6's claim that this wave's job "is to make that a one-line deletion" is true about the seams and false about the schedule: the seams are right, the deletion is last, not first.

The same gap blocks `ON CONFLICT` on sharded tables, and its refusal comment states the problem better than a fresh sentence would (`pgexec/src/exec.rs:5419-5421`): *"ON CONFLICT arbitration probes and locks a unique key on the local range; a sharded table's unique keys live on other ranges, so the conflict can neither be seen nor locked here."* Global uniqueness is therefore worth building whether or not foreign keys are ever built, and this document's prerequisite order is arranged so that decision stays open.

### The index entry's range is the arbiter, and a key reservation is the mechanism

Place a global index's entries by indexed key: the index gets its own id in the range map's key space and its own hash spec, so an entry lives at `(index_object_id, bucket = hash(encoded key) mod n, encoded key, base rowid)`. This is G-9c's observation applied one level over — a bucket is just the leading component of an interval key space, so the existing map, splits, moves, balancer, and filtered restore all apply unchanged (`2026-07-09-crabka-gres-g9-distributed-maturity-design.md:73`). Routing an index key then costs nothing new: it is `route_hash_equality` (`gres-ranges/src/map.rs:398-404`) with the index's spec instead of the table's.

A base-table write consequently becomes a two-participant timestamp transaction — the row's range and each touched index key's range — which is precisely the case G-9d says is routine after G-9a (`:77`). Uniqueness is then enforced on the entry range, because that range is the key's sole owner, and sole ownership is what `validate_hash_shard_boundaries` (`gres-ranges/src/map.rs:415-420`) already guarantees for tables and must be extended to index objects.

The mechanism is a reservation, not a lock. Alongside the entry, a unique index write does a `ConditionalPut` on `index/ts_unique/<index_id>/<encoded key>` with `expected: None` — exactly the shape the row path already uses for its prewrite reservation (`pgexec/src/timestamp_txn.rs:2512-2516`). One writer wins the compare-and-set; the loser fails immediately. No waiting, so no deadlock, so nothing that needs a wait-for graph the engine does not have.

Two encoding changes are load-bearing and neither is a migration concern on a greenfield system. First, the global index *intent* key today orders `(index_id, start_ts, indexed values, base table, base rowid)` (`pgexec/src/timestamp_txn.rs:2860-2872`), which makes "is another transaction holding an intent on this key" unanswerable by a prefix scan — the key must be re-ordered to put the indexed values ahead of `start_ts`. The *entry* key already orders correctly (`pgexec/src/timestamp_txn.rs:2888-2904`) and needs no change. Second, `GlobalIndexIntent` carries `base_table_id` and `base_rowid` but not the base row's bucket (`pgexec/src/timestamp_txn.rs:564-577`), while `TimestampWrite` does carry it (`:539`); without the bucket in the entry, the second hop from index entry to base row is a scatter rather than a point route.

### The prewrite that discovers a duplicate must distinguish two answers

A failed reservation CAS is ambiguous. It can mean a concurrent transaction holds the key and has not yet decided, or it can mean a committed row already holds it. PostgreSQL reports the first by blocking and then, if the other side commits, `23505`; it reports the second as `23505` immediately. Reporting both as `40001` would be a visible regression, because a plain duplicate insert would come back retryable.

The entry range therefore resolves the ambiguity before answering: it reads the committed entries under the key prefix (the wire form of `read_visible_global_index_entries`, `pgexec/src/timestamp_txn.rs:2795-2826`, which today has no production caller at all). A visible committed entry naming a different base rowid is `23505`. Only a live foreign intent is `40001`.

That leaves one divergence worth stating rather than discovering. PostgreSQL *blocks* the second inserter until the first decides and then answers definitively; this design aborts it. Under autocommit the gateway's retry hides the difference — the retry re-probes and gets `23505`. Inside an explicit transaction it cannot, and the client sees `40001` where PostgreSQL would have shown `23505`. This is the price of refusing to wait, and it is the right price: the alternative is a durable cross-range lock with no detector.

Note that this is a second conflict axis, not a replacement for the first. `ensure_prewrite_can_win` (`pgexec/src/timestamp_txn.rs:2990-3028`) detects conflicts by scanning the versions under `row_key(table_id, rowid)` — row identity. Two `INSERT`s of the same unique key touch different rowids and are invisible to it. The key reservation is an independent domain checked in the same prewrite batch, possibly on a different range.

### The write-skew closure: both parties leave evidence on the key's owning range

This is the crux, and it is not a wiring problem.

`DELETE FROM parent WHERE id = 7` on range A and `INSERT INTO child (parent_id) VALUES (7)` on range B write disjoint rows. `ensure_prewrite_can_win` sees no conflict on either side because it only ever compares row identities. Both commit. The child now references nothing. G-9 defers SSI explicitly and names the bar as "SI + first-committer-wins" (`2026-07-09-crabka-gres-g9-distributed-maturity-design.md:21`), so this is not an oversight to be patched at a call site; it is the isolation level behaving as designed.

Four closures are available and three are wrong here.

A **durable key-identified shared lock** — D6's local protocol lifted to a distributed lock manager — is the most faithful. It fails on the engine it would run in. A shared mode cannot be a single-valued CAS, so it needs a durable set representation with its own resolve and recovery; and because it is a lock, the child waits for the parent and the parent waits for the child, which is a cross-range deadlock in a system whose only detector is a timeout armed on a different transaction class (`gres-ranges/src/tenant.rs:5403-5415`, `:5475-5485`).

**Reference-count intents** — a counter per parent key, incremented by children, checked at zero by the parent — turn every child insert into a write-write conflict with every other child of the same parent. That is exactly the convoy on a hot dimension row that D6 exists to avoid (`2026-07-31-gres-d6-foreign-keys-local-design.md:13`), reintroduced at a different key.

**Promoting foreign-key transactions to a serializable sub-protocol** changes the isolation level of user transactions to fix one constraint, and the transactions that need it are not identifiable in advance.

**Read-set validation at commit** is the right shape, and it is cheap here for one specific reason: the referenced key is *unique*, so the predicate the child reads is not a range but a point. There are no predicate locks and no interval bookkeeping — the read set is a set of exact index keys, and validating it is a point lookup per entry.

The realization exploits sole ownership. The child's evidence — "index key K of unique global index I existed" — is written as a durable **read intent** on K's owning range, under the same prefix family as the write intents: `index/ts_read/<index_id>/<encoded key>/<start_ts>`. The parent's evidence — the removal of K's entry — is already a write intent on the same range under the same prefix. Both prewrites then check the other's family on the one range that owns the key:

- A child placing a read intent conflicts if a foreign write intent exists under K, or if a committed entry-removal under K carries `commit_ts > start_ts` — the same rule `ensure_prewrite_can_win` applies to rows, restated over key identity.
- A parent removing K's entry conflicts if any unresolved foreign read intent exists under K.

Both directions terminate, and they compose into ordinary first-committer-wins. If the parent lands first, the child's prewrite fails and the insert is refused. If the child lands first, the parent's prewrite fails with `40001`; the parent retries at a fresh timestamp, the check now finds the committed child, and it reports `23503`. Either way the invariant holds, and neither side ever waits.

Call this what it is rather than "distributed SSI": the foreign-key predicate has a single owner, so both parties are made to visit it. It is a carve-out for one predicate, built out of machinery that already exists — intents are already durable before prewrite acknowledgment (`pgexec/src/timestamp_txn.rs:2481-2531`, `:2247-2269`), already resolved from the primary's decision, already recovered by the tenant's in-doubt sweep.

**What it costs.** Every child insert gains a participant, a WAL append, and a resolve: foreign-key writes are roughly twice the write amplification of plain ones. Read intents need garbage collection, and the silence/settle machinery that sweeps abandoned write intents is keyed on write intents — extending it is real work, not a configuration change. And because read intents are per-transaction, N concurrent children of one parent leave N entries under one prefix, so the parent's conflict scan is O(concurrent children) — the non-convoy property is preserved (children never conflict with each other) but the parent's check is not free.

**What it does not guarantee.** Only foreign keys get read-set validation. Every other write-skew-shaped invariant a user can express — a `CHECK` over a sum maintained across rows, an application invariant spanning two tables — remains unprotected, because the engine is still snapshot isolation. This design does not raise the isolation level; it carves one predicate out of it, and saying otherwise in a release note would be a lie.

**And it has no fairness.** A parent key under continuous child inserts can starve its own deletion indefinitely: every retry of the parent finds another live read intent. There is no queue and no aging. A `DELETE` of a hot dimension row may need application-level quiescing, which is a real operational wart and belongs in the user-facing documentation, not in a footnote here.

### Key-routed point lookups need a new RPC, not a wider scan

`route_hash_equality` maps a value to a range only for a table's own declared hash columns (`gres-ranges/src/map.rs:398-404`). A foreign-key probe asks a different question — which range owns index key K of index I — and the parent's shard key need not be the referenced key. Once the index is a routable object with its own spec, that question is answered by the same function against the index's spec, so the routing itself needs no new algorithm.

`route_key` (`gres-ranges/src/map.rs:425-438`) must not be mistaken for this. Despite the name it routes to the table's *first* range via `range_for_key(table_id, 0)` and uses the key only to compute an fnv1a field on the returned route.

The transport needs one new request: a point lookup carrying `(index_id, encoded key values, read_ts, own_start_ts)` and returning visible entries. It is deliberately not an extension of `ScanRangeReq` (`gres-ranges/src/transport.rs:743-757`), which narrows by rowid interval plus a pushed predicate — and a rowid interval is meaningless across the ranges of a hash-sharded table, as the scanner's own comment says when it falls back to bucket-wise segmentation (`gres-ranges/src/tenant.rs:2512-2523`). `RangeRequest` (`gres-ranges/src/transport.rs:50-110`) has no key-lookup variant today.

**The scatter case is on the child side, and it is the common one.** The parent probe is two point hops — entry range, then base-row range — provided the entry carries the base row's bucket. But the *parent-side* question is "who references me?", and answering it means finding child rows holding the key. D6 already accepts a full child scan when no child index matches the foreign key's columns (`2026-07-31-gres-d6-foreign-keys-local-design.md:138`). Distributed, "full child scan" means a scatter over every range of the child table, on every parent delete and every parent key update. Because the child's foreign-key columns are usually not its shard key, that is the expected case rather than the degenerate one.

The decision is to refuse it. A foreign key whose child is sharded requires a global index on the child's referencing columns, and the DDL error names the index to create. PostgreSQL does not require this, so it is a deliberate divergence — but PostgreSQL's cost for the missing index is a local sequential scan, and here it is an RPC to every range of the table on every parent write. Silently accepting a per-write scatter is the kind of footgun D6's goals section rules out.

### Deferred constraints move to the gateway, and the drain reads its own intents

`DeferredConstraints` lives on `SqlSession` (`pgexec/src/session.rs:2326`), one per range engine, drained at `COMMIT` from `take_all` (`:4516`). `GatewayTransaction::Timestamp` carries only an identity and a participant map (`gres-ranges/src/tenant.rs:3144-3147`). There is no place a commit-time drain can stand and see every range's pending checks.

A deferred check is not range-local state — it is a promise about the transaction, and the transaction's identity lives at the gateway. So the pending queue moves there, alongside `participants`, and each range's write path returns its entries with the prewrite acknowledgment rather than storing them.

D6's argument for what the commit drain reads does not survive the move, and the way it fails is instructive. Locally, a child entry drops the staged key at promotion so the commit drain re-derives it from durable state, "because by `COMMIT` the row is durable in the KV" (`2026-07-31-gres-d6-foreign-keys-local-design.md:87`). In a timestamp transaction the rows are durable as *intents*, not as committed versions, until the primary writes its decision — so a re-derivation reading at a fresh `read_ts` finds nothing at all. The fix is already on the wire: `ScanRangeReq` carries `own_start_ts` (`gres-ranges/src/transport.rs:752`, set at `gres-ranges/src/forward.rs:2726-2728`) precisely so a transaction sees its own intents remotely. The drain reads with it set. Carrying the key values in the entry instead is rejected for D6's own reason — a referential action may have rewritten the row after the entry was queued.

This adds a phase to the two-phase commit: prewrite every participant, then **validate**, then decide. The ordering is forced. Validate must run after all prewrites so every row of the transaction exists as an intent, and before the decision so a violation aborts rather than requiring compensation. It costs one RPC round per foreign-key-bearing transaction.

The read-intent placement of the previous decision *is* the validate phase, not a separate round. Placing a read intent on the key's owning range is a conditional operation that succeeds exactly when the key is present and uncontended, so the evidence and the check are one write. That is why the phase is affordable at all.

Immediate (`NOT DEFERRABLE`) checks collapse into the same phase today, because an autocommit sharded write's end-of-statement and end-of-transaction are the same instant. They stop collapsing the moment multi-statement sharded transactions exist, at which point each statement needs its own validate round.

### Referential actions are refused until multi-statement sharded transactions exist

A cascade is multi-statement by nature: delete the parent, then delete or update the children. Both doors are shut — the engine refuses sharded writes inside explicit transactions (`pgexec/src/session.rs:5273-5277`), and multi-range scatter refuses everything but a plain `INSERT` (`gres-ranges/src/tenant.rs:5722-5732`) and additionally refuses any write carrying global-index intents at all (`gres-ranges/src/tenant.rs:4775-4782`).

Opening those doors is not enough, because the cascade's termination argument does not distribute. D6 terminates cycles by sharing the outer statement's write bookkeeping, so an action that revisits an already-modified row stops (`2026-07-31-gres-d6-foreign-keys-local-design.md:61`). That bookkeeping is per-engine state on one range. A cascade that reaches a row on another range has no shared "already modified" set to stop at, so the terminator is exactly the cross-range knowledge that is missing.

Running the cascade range-locally on each participant is rejected for that reason: it is not slower, it is non-terminating on mutually referencing sharded tables. A gateway-held modified-row set would work and is unbounded in the size of the transaction's write set; a depth bound would terminate and be semantically wrong.

So `ON DELETE CASCADE`, `SET NULL`, and `SET DEFAULT` are refused at DDL time with a typed `0A000` naming the action, and a cross-range foreign key supports `NO ACTION` and `RESTRICT` only. When multi-statement sharded transactions land, the gateway will already hold per-transaction state and the modified-row set can live beside it — which is the right time to revisit this, not before.

### Distributed deadlock is avoided by construction, and the residue is livelock

The wait-for graph is engine-local by design, and the module says so: cycles spanning two engines are invisible to it (`pgexec/src/lockmgr.rs:14-19`), and the lock manager is purely in-memory because "after a restart no transactions are in flight" (`:11-12`). The substitute is a 2 s wait cap (`gres-ranges/src/forward.rs:112`) armed only when a gateway transaction escalates past one range (`gres-ranges/src/tenant.rs:5403-5415`) or when a statement is forwarded under an escalated transaction (`:5475-5485`). `GatewayTransaction::Timestamp` sets neither. Two foreign-key checks locking parent keys in opposite orders on two ranges would hang, uncapped, until the transport's own timeout.

The design's answer is to have nothing to deadlock over. Every foreign-key conflict is a CAS or a prefix scan at prewrite that succeeds or fails; there is no wait, so there is no cycle. This is the strongest reason to prefer the intent rendezvous over the durable shared lock, and it is worth being explicit that it was the deciding factor rather than an incidental benefit.

The residue is livelock: two transactions that each abort the other can retry indefinitely. It is bounded the way the timestamp path already bounds write conflicts — `WriteConflict` maps to `SerializationFailure` (`pgexec/src/timestamp_txn.rs:3030-3034`) — with bounded gateway retries under autocommit and `40001` to the client otherwise. `40001` is a correct PostgreSQL answer for a referential check under a serializable-flavoured protocol, which is why abort-and-retry is preferable to inventing distributed locking.

One line item follows for later: if multi-statement sharded transactions land, user statements will be able to block on ordinary row locks across ranges, and the wait cap must be extended to the timestamp path at that point. It is not needed for this design and is needed for the one after it.

### What the correctness argument actually rests on, and what the clock does not give

The foreign-key invariant here is **not** protected by read timestamps. Conflicts are detected on key identity at prewrite, on a range whose writer is a single totally-ordered WAL. A parent removal and a child insert that race are ordered by whichever intent lands first, and that ordering is a local fact requiring no agreement about real time. This is what lets the protocol survive a clock that gives snapshot isolation with an uncertainty window rather than linearizability — a property the HLC design gives up explicitly (`2026-07-20-hlc-distributed-mode-design.md:22`) after rejecting commit-wait (`:32`).

What it does rest on is narrower and checkable: intents are durable before the prewrite is acknowledged; the range map never lets two ranges claim one index key, which `validate_hash_shard_boundaries` (`gres-ranges/src/map.rs:415-420`) enforces for tables and must be extended to index objects; and intents survive a split or move of an index range, which is the same obligation splits already carry.

Three things are missing, and their absence bounds what this design may claim.

`ReadVerdict::Uncertain` exists (`pgmvcc/src/visibility.rs:135-149`) and nothing consumes it. The scan path uses the two-valued `satisfies_ts` (`pgmvcc/src/visibility.rs:88-94`), and the only non-test reference to `uncertainty_window` is the trait's zero default (`pgexec/src/timestamp_txn.rs:169-171`) and the HLC source's override (`pgexec/src/hlc_source.rs:280-282`). Under `Hlc` an ordinary read can therefore miss a commit that preceded it in real time, with nothing to restart it. The foreign-key protocol does not depend on that, per the argument above — but D6's parent-side re-probe does. `NO ACTION` deferred to `COMMIT` re-probes for a live parent still supplying the key (`2026-07-31-gres-d6-foreign-keys-local-design.md:89`), and a re-probe is a timestamped read. Under HLC it can miss a concurrent re-supply and raise `23503` spuriously. That is a false positive rather than a false negative — an availability defect, not a corruption — and it should be recorded as one.

Node self-fencing on clock dispersion is specified as the correctness bound the whole uncertainty window is sized to (`2026-07-20-hlc-distributed-mode-design.md:42`) and is implemented nowhere; `hlc_source.rs` has no dispersion monitor. A node outside `max_offset` commits in the past beyond where readers look. That invalidates every snapshot guarantee under `Hlc`, foreign keys included, and it is not this design's to fix. Under `LogicalTso` the window is zero, the branch is dead, and none of this fires — which is the only configuration in which this design's claims are currently complete.

### Bypass eligibility is re-derived from the constraint graph, statically

The single-shard bypass fires when a statement is autocommit, routes to exactly one range, and that range is hosted locally (`gres-ranges/src/tenant.rs:4762-4766`), committing against the range's own sequence instead of the global timestamp source (`pgexec/src/lib.rs:1817-1826`) with correctness resting on closed timestamps (`pgexec/src/lib.rs:1873-1879`).

`ranges` there is the *routed write set*, and a foreign-key-bearing write has participants that set does not name: the referenced key's entry range, and the read-intent range that is the same. So the condition is wrong for these tables — it would bypass the global source for a transaction that is not single-shard.

Bypass eligibility therefore becomes a static property of a table's constraint closure, computed from the catalog at plan time: a table on either side of any foreign key is never bypass-eligible. The alternative — compute the full participant set including entry ranges and bypass when it is one — is correct and rejected, because it couples a latency-critical decision to the live range map, so a rebalance silently moves a table between performance classes, and the bypass's local-sequence commit would need the entry range's sequence as well. Refusing the bypass statically costs some throughput on small foreign-key tables and keeps the fast path's condition cheap and stable.

### Where D6 is wrong about the distributed case

D6's integration section says (`2026-07-31-gres-d6-foreign-keys-local-design.md:118`):

> Cross-range enforcement is a companion cycle: the parent probe becomes a durable shared reference lock plus a latest-committed read on the parent's range, because the engine's snapshot isolation with first-committer-wins does not prevent write skew and a foreign key is a write-skew-shaped invariant. This wave's job is to make that a one-line deletion, which it does by routing the parent probe and the child search through two narrow functions that take the probe target as a parameter. The key-lock identity is already a byte string derived from ids and values, so it lifts to a distributed lock key unchanged.

The diagnosis is right and three of the conclusions are wrong.

**The manager does not lift.** `FkKeyLocks::lock_key` (`pgexec/src/fk.rs:1070-1077`) reaches `RowLockManager::acquire_key` with a `LockKey::UniqueKey` (`pgexec/src/exec.rs:1900-1917`, `pgexec/src/lockmgr.rs:63-71`), and that manager is in-memory and per-engine as a stated design property (`pgexec/src/lockmgr.rs:11-12`). Two engines never see one another's key locks, and no key lock survives a restart. The sentence is true about the bytes and false about the manager.

**The bytes do not lift either.** `lock_bytes` (`pgexec/src/fk.rs:1346-1352`) is `secondary_index_entry_prefix(parent.id, index.id, values)` (`pgkv/src/key.rs:219-231`), the *local* secondary-index encoding, keyed by base table id. A global index's entries are keyed `\0\0\0\0index/ts_entry/<index_id><values>` with no table id at all (`pgexec/src/timestamp_txn.rs:2888-2896`). A distributed identity has to be derived against the global encoding, and the local and global encodings must then be reconciled or one key acquires two names.

**A latest-committed read cannot be the protocol.** Under snapshot isolation no read at any timestamp can observe a concurrent removal that has not yet committed, which is the entire write-skew problem restated. The protection has to be a durable artifact placed on the key's owning range — which is what this design proposes, and what the phrase "durable shared reference lock" was reaching for without a mechanism behind it.

What D6 got exactly right is the seam shape. `FkKeyLocks` and `FkCascade` (`pgexec/src/fk.rs:1070-1077`, `:1127-1137`) take the probe target as a parameter, and `rows_with_key` (`pgexec/src/fk.rs:1207-1232`) is a single function with the lookup isolated in it. Those are the right seams and this design does not change them; it changes what stands behind them.

## Prerequisite order, and what is useful on its own

1. **Global unique enforcement.** Place index entries by indexed key rather than beside the base row (`pgexec/src/timestamp_txn.rs:2525-2530`); re-order the intent key so indexed values precede `start_ts` (`:2860-2872`); add the key reservation CAS and the `23505`/`40001` disambiguation; lift the scatter refusal on global-index maintenance (`gres-ranges/src/tenant.rs:4775-4782`). **Independently useful:** it unblocks `ON CONFLICT` on sharded tables (`pgexec/src/exec.rs:5419-5430`) and unique global indexes generally (`pgexec/src/exec.rs:708-712`, `:4278-4283`, `:4388-4391`, `:5453-5457`), neither of which mentions foreign keys.
2. **The key-routed point-lookup RPC**, plus the base row's bucket in the entry so the second hop is a point route. **Independently useful:** it is the single-range secondary-key point read G-9d promises and the envelope section counts on (`2026-07-09-crabka-gres-g9-distributed-maturity-design.md:77`, `:89`), and it gives `read_visible_global_index_entries` (`pgexec/src/timestamp_txn.rs:2795-2826`) its first production caller.
3. **The read-intent rendezvous.** Useful only for foreign keys — and as a working prototype of the read-set validation any later SSI work would need.
4. **The validate phase and the gateway-held deferred queue.** Useful only here.
5. **Then** the foreign-key probe: delete the sharded refusals (`pgexec/src/fk.rs:226-227`, `:249-250`) and parameterize the probe target. At *this* point, and only at this point, D6's "one-line deletion" description is accurate.
6. **Gated on multi-statement sharded transactions:** referential actions, per-statement immediate checks, and extending the cross-range wait cap to the timestamp path.

Steps 1 and 2 are worth building whether or not steps 3 through 5 are ever scheduled. That separation is the main practical claim this document makes.

## What remains unsafe or unsupported

Stated here rather than distributed through the text, because a reader deciding whether to build this needs it in one place.

Referential actions across ranges are refused, not unsafe — but the refusal is a real functional gap, and applications that use `ON DELETE CASCADE` on sharded tables have no path.

Every write-skew-shaped invariant that is not a foreign key remains unprotected. The engine is snapshot isolation with first-committer-wins and this design does not change that; it carves out one predicate.

A hot parent key under continuous child inserts can starve its own deletion indefinitely. There is no fairness mechanism and none is proposed.

Under `Hlc`, node self-fencing on clock dispersion is unimplemented (`2026-07-20-hlc-distributed-mode-design.md:42`), so a node outside `max_offset` invalidates every snapshot guarantee, foreign keys included. Under `Hlc`, `ReadVerdict::Uncertain` (`pgmvcc/src/visibility.rs:135-149`) has no consumer, so a deferred `NO ACTION` re-probe can raise `23503` spuriously. Both are properties of the mode rather than of this feature, and both must be closed before any correctness claim under `Hlc` is complete.

## Integration

**`crates/pgexec`.** The global index becomes a placement-aware object with its own routing spec; `timestamp_txn.rs` gains the key reservation, the re-ordered intent key, the read-intent family and its resolve/recover paths; `fk.rs` keeps its seams and gains a remote probe target behind `rows_with_key`; the sharded refusals in `fk.rs` are replaced by narrower DDL refusals for referential actions and for sharded children lacking a global index on their referencing columns.

**`crates/gres-ranges`.** A key-lookup request on the existing transport; index objects in the range map with their own hash specs and boundary validation; the validate phase in the timestamp 2PC; the deferred-check queue on `GatewayTransaction::Timestamp`; the static bypass-eligibility rule.

**`crates/pgcatalog`.** A hash spec and bucket count on `Index` for globally placed indexes, and the constraint-closure query that bypass eligibility and DDL validation both read.

**`crates/pgmvcc`.** Nothing new for foreign keys. The `Uncertain` consumer is a separate obligation of the HLC mode and is named here only because this design's error semantics inherit it.

**The single-shard bypass and the balancer** compose without change: index ranges are ordinary ranges, so splits, moves, merges, co-location and anti-affinity apply to them as G-9d's placement vocabulary already assumes.

## PostgreSQL compliance

The oracle is `postgres:18.4`, as in D6, and the measurement is the same regression corpus. Every semantic this design implements is one D6 already pinned against a live oracle; nothing new about PostgreSQL's behaviour is being asserted here, only about where it can be enforced.

Deliberate divergences beyond D6's list, each to be recorded in the implementing item's rustdoc and matrix row:

- Referential actions other than `NO ACTION` and `RESTRICT` are refused with `0A000` when either relation is sharded.
- A foreign key whose child is sharded requires a global index on the referencing columns; PostgreSQL requires no index at all.
- A concurrent duplicate insert on a sharded unique key inside an explicit transaction reports `40001` where PostgreSQL blocks and then reports `23505`. Under autocommit the gateway's retry restores PostgreSQL's answer.
- Under `Hlc`, a deferred `NO ACTION` re-probe may raise `23503` spuriously until `ReadVerdict::Uncertain` has a consumer.

## Testing

The models carry the weight here, because the failure this design exists to prevent is a two-node interleaving that no single-process test reaches. Extend the G-9a Stateright corpus with a key-conflict model: a parent removal and a child check racing on one key's owning range must never both commit, under every interleaving of prewrite, decision, crash, and fence — and the variant with the read intent removed must produce a counterexample, or the model is not testing anything.

Add a unique-key first-committer-wins model over the key reservation, including the split of a duplicate into `23505` versus `40001`, and a livelock-termination property showing that bounded retries make progress under a fixed adversary.

System gates reuse the existing shape: the bank and Elle suites run on sharded tables carrying foreign keys under writer kills and TSO fences, and a referential-integrity invariant joins the checked set — no committed state may contain a child row whose parent key is absent, evaluated at every quiescent point.

Behaviour tests follow D6's split by concern, with the cross-range cases run against a multi-range harness rather than the in-process engine. The load-bearing regressions are the ones that pin a decision a plausible implementation gets wrong: a parent delete and a child insert on different ranges must not both commit; a duplicate unique key inserted concurrently from two gateways must yield exactly one row; a deferred check must observe rows its own transaction wrote as intents on another range; and a bypass-eligible-looking write to a foreign-key table must take the global timestamp source anyway.
