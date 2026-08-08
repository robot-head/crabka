# crabka-connect-postgres

`crabka-connect-postgres` is a Postgres logical-decoding source connector for
`crabka-connect`.

The connector reads `pgoutput` changes from a logical replication slot and emits
Kafka Connect-style records with Protobuf-framed keys and values. DELETE events
emit a Protobuf-framed key with a `None` value, which represents a tombstone for
compacted topics. Each record targets its schema-qualified relation name (for
example, `public.orders`) as the Kafka topic and leaves partition selection to
the Kafka partitioner.

## Offsets and acknowledgement

The connector tracks Postgres LSNs with `SourceOffset`. The offset partition
contains the database and replication slot, and the offset position contains the
LSN.

The live source reads with `pg_logical_slot_peek_binary_changes`, so a poll does
not advance the replication slot. The runtime calls `Source::acknowledge` after
three steps. It writes the sink record, commits the sink, and durably saves the
checkpoint. The connector then advances the Postgres slot to the acknowledged
LSN.

## Setup expectations

This slice needs:

- A `pgoutput` logical replication slot.
- A publication that covers the configured tables.
- Configured table names for the source.
- No initial snapshot. The connector emits only the changes that logical
  decoding makes available.

If you configure table names, the connector creates the publication when it is
missing. The connector also checks that the publication covers the configured
tables. It creates the logical slot when that slot is missing.
