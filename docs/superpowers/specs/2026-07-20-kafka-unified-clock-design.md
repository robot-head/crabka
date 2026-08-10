# Kafka Unified Clock Design

Give Kafka records an internal `TimestampSource` stamp — an additional cross-domain coordinate stored beside the wire-exact log — and let a Kafka transaction join the gres cross-range coordinator as a resource manager, so a SQL row and a Kafka record can commit atomically and be read under one snapshot.

**Type:** Broker-side storage/txn change plus a coordinator bridge, realizing the decision record's ["stamp is an additional internal coordinate"](../../decisions/2026-07-20-write-scalability-two-mode-timestamp-source.md) and "one 2PC coordinator; Kafka partition is another participant" decisions. Builds on the [TimestampSource seam](2026-07-20-timestamp-source-seam-design.md); composes with [HLC mode](2026-07-20-hlc-distributed-mode-design.md).

## Design Goals

- Kafka clients see zero change: offsets remain the sole on-wire ordering, record batches stay byte-exact (the verbatim-append path is untouched), LSO/high-watermark semantics and the `aborted_transactions` mechanism are exactly Apache Kafka's. The stamp exists only server-side.
- "Write a row and emit an event atomically": a cross-domain transaction either commits in both domains or neither, and no snapshot reader ever observes exactly one half.
- One decision authority. The gres coordinator (GTM xid, prepare/commit) is the top level; the broker contributes no second commit protocol — its existing EOS machinery is driven as a resource manager.
- The bridge leans on machinery that already exists: the gres SQL WAL already commits through Kafka transactions (`ProducerWalWriter`, with the broker's producer epoch as the zombie fence), KIP-939 groundwork (2PC sentinel, never-reap predicate, pure decision cores) is in the tree, and the `.txnindex` sidecar is the established pattern for per-offset-range internal metadata.

## Architecture Overview

**The stamp.** Each partition gains a `.stampindex` sidecar — fixed-width entries mapping an offset range to a packed timestamp — written at the same append seam that maintains the `.txnindex` and the LSO today. Non-transactional batches are stamped as they are appended. Transactional data is stamped when its COMMIT marker lands: the entry covers the transaction's data range and carries the transaction's commit stamp, mirroring how the abort index records aborted ranges. The `.log` file itself holds only wire bytes, exactly as now; the stampindex is derived state, rebuildable by rescanning the log's own machinery, and never leaves the broker on any client-facing API.

**The clock.** Each broker holds the tenant's `TimestampSource` like any other node: in HLC mode a broker-local clock (this is where "a partition and a SQL row share a physical-ish order" cashes out); in solo mode a grant client against the tenant's range-0 oracle, conveyor-batched so stamping costs one amortized RPC per batch of appends, not one per record. Stamps are folded (observed) at append, so within a partition stamp order never contradicts offset order.

**The commit bridge.** A cross-domain transaction is a SQL timestamp transaction plus a transactional produce under a 2PC-enabled producer (KIP-939 `enable2Pc`: transaction timeout pinned to the no-timeout sentinel, never reaped by the idle-transaction reaper). The gres coordinator runs its normal protocol with one more participant class:

1. **Prepare (Kafka):** flush all transactional produce; the broker acknowledges with its current clock reading, which the coordinator observes. The transaction sits in `Ongoing`, fenced by producer epoch, immune to the reaper — KIP-939's prepared state. The coordinator durably records the producer identity (id, epoch) alongside its SQL prewrite records, so the decision can always be re-driven.
2. **Decide:** the coordinator allocates `commit_ts` after observing all participants (SQL prewrite acks and the broker's reading alike), then persists its decision — the same single decision point the SQL path has today.
3. **Complete (Kafka):** the coordinator drives marker writing with `commit_ts` attached; the marker append clears the pending-transaction entry, advances the LSO through the one mechanism that already exists, and writes the `.stampindex` entry for the data range at `commit_ts`. Aborts are symmetric minus the stampindex entry (the abort index covers those ranges as today).

Atomic visibility falls out: a snapshot reader at `read_ts` sees the SQL rows iff `commit_ts <= read_ts` and sees the Kafka records iff below-LSO, non-aborted, and stamp `<= read_ts` — the same stamp, so both or neither.

## Key Design Decisions

### Stamp in a sidecar index, not in the batch bytes

Embedding the stamp in record headers or batch attributes was rejected outright: the produce path stores client bytes verbatim (patching only base offset and epoch), and that verbatim guarantee *is* the wire-exactness constraint. The sidecar keeps the guarantee structurally — there is no code path that could leak the stamp into fetch responses, because fetch serves log bytes and the stampindex is never consulted by the Kafka read path. It also prices the feature honestly: a fixed-width entry per batch/transaction range, aligned with the storage-cost con accepted in the decision record.

### Commit stamps ride the internal marker path, never the public wire

The commit stamp must reach the marker append (it is what the stampindex records for transactional data), but `EndTxn` and `WriteTxnMarkers` are Kafka protocol messages and grow no fields. Cross-domain completion therefore rides a Crabka-internal coordinator RPC that carries `(producer id, epoch, decision, commit_ts)` to partition leaders and lands in the same marker-append seam; the public `EndTxn` path remains byte-exact and continues to serve pure-Kafka transactions, whose stampindex entries take the broker's own clock reading at marker time. Extending the inter-broker `WriteTxnMarkers` schema was rejected even though clients never see it — keeping every Kafka-schema message exactly Kafka-shaped is what makes the differential suites trustworthy.

### The Kafka participant is the transaction, not the partition

The resource manager enrolled with the gres coordinator is the broker transaction (producer id + epoch), not each individual partition. Kafka's own machinery already fans a single transaction decision out to every touched partition and survives partial marker writes by retrying — re-implementing per-partition enrollment in the gres coordinator would duplicate exactly that fan-out one level up. The coordinator therefore holds one participant entry per domain: N SQL ranges plus one Kafka transaction, however many partitions it spans.

### Recovery completes KIP-939 rather than inventing a parallel path

A prepared cross-domain transaction must survive anything. Broker restart: the transaction state replays from `__transaction_state`, and the 2PC sentinel keeps it unreapable. Coordinator restart: the decision (or its absence) is durable in the coordinator's own log, and completion is idempotent — markers re-driven at the current epoch are the existing retry story. Producer/application restart: opt-in `transaction.version=3` implements `keepPreparedTxn` on `InitProducerId`, the "reattach to a prepared transaction and complete it" primitive KIP-939 defines. Orphan resolution is one rule: the broker never times out a 2PC transaction, and anyone discovering one asks the gres coordinator, which always has (or will re-derive) the answer — a single authority, per the decision record, rather than a two-coordinator agreement protocol.

### Stamping is enabled per tenant with a SQL domain

Tenants running pure Kafka pay nothing: no stampindex, no clock client, no solo-mode dependency edge from broker to the gres range transport. The new dependency (broker reaching the tenant's timestamp source in solo mode) was the accepted cost of unification, and scoping it to tenants that actually have two domains keeps the blast radius at zero for everyone else.

## Integration

- **gres-substrate:** the existing `ProducerWalWriter` (SQL WAL inside a Kafka transaction) is the proof of concept this generalizes; it can later enroll as a cross-domain participant instead of terminating compute on indeterminate outcomes.
- **gres coordinator:** gains a participant class, not a new protocol — prepare/decide/complete keyed by GTM xid exactly as for ranges, with the Kafka participant's prepare/complete driven over the internal bridge RPC.
- **TimestampSource seam:** the broker is just another holder of the trait object; solo/HLC differences are entirely the seam's concern.
- **Single-shard bypass:** a transaction touching one Kafka transaction and zero-or-one SQL ranges still crosses domains and takes the coordinator path; the bypass applies only within the SQL domain.

## Kafka / KIP Compliance

- **Byte-exact surfaces (unchanged):** record batch format, offsets, LSO/high-watermark, `aborted_transactions` in fetch responses, transaction-state log schema, all client-facing request/response schemas. The differential suites against the released `cp-kafka` image are the enforcement mechanism and must pass unchanged.
- **KIP-98/KIP-890 (EOS, epochs):** untouched; producer-epoch fencing is reused as the zombie fence for cross-domain participants.
- **KIP-939 (2PC participation):** the broker-side groundwork is complete behind opt-in `transaction.version=3`: `enable2Pc`, the never-reap predicate, persisted recovery identities, and `keepPreparedTxn` recovery. Where KIP-939 leaves the external coordinator abstract, the gres coordinator is that coordinator; the broker-visible behavior stays within what the KIP specifies for a 2PC-enabled transactional producer.
- **KIP-447 (fenced offset commits):** unaffected; buffered transactional offsets commit under the same markers.

## Testing

- Differential tests against `cp-kafka` for every touched broker path (produce, fetch, EndTxn, transaction recovery) — the wire-invisibility claim is only as good as these.
- Extend the existing Stateright EOS/2PC models (`two_pc_model`, `eos_composition_model`) with the external-coordinator participant: no interleaving of crashes across prepare/decide/complete may yield a one-domain commit, an unresolvable orphan, or a reaped prepared transaction.
- Atomic-visibility property test: for random schedules of cross-domain commits and snapshot reads, no `read_ts` ever observes exactly one domain of a transaction.
- Stampindex rebuild: deleting the sidecar and replaying the log reproduces it byte-identically; crash between marker append and stampindex write is recoverable in one direction only (index lags log, never leads).
