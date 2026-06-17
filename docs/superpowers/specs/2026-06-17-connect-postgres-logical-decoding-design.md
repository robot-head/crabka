# Connect Postgres Logical Decoding Source - Design

**Date:** 2026-06-17
**Status:** Design approved
**Workstream:** Connect framework, Postgres CDC source connector
**Predecessors:** `crabka-connect` embeddable connector runtime + lifecycle; connector config + secrets SPI; `schema-serde` Protobuf framing

## Goal

Add a `crates/connect-postgres` workspace crate that implements a Postgres
logical-decoding source connector for Crabka Connect. The connector consumes
Postgres WAL through a logical replication slot, converts row-level changes
into an `EntityDifference` CDC envelope, emits Protobuf-framed Kafka records,
and resumes from durable LSN checkpoints through the existing
`crabka_connect::SourceOffset` contract.

This replaces the planned in-memory `Broadcaster` shape for Postgres-backed
CDC with a durable, LSN-tracked source that can survive process restarts and
continue from the last committed WAL position.

## Requirements

- Provide a new `crabka-connect-postgres` crate under `crates/connect-postgres`.
- Implement `crabka_connect::Source<bytes::Bytes, bytes::Bytes>` so the
  existing runtime can drive the connector without runtime changes.
- Expose typed CDC domain structs internally and publicly enough for tests and
  downstream users: `EntityDifference`, `EntityKey`, `Operation`,
  `ColumnValue`, and `TableSchema`.
- Consume Postgres WAL through a replication slot using `tokio-postgres`
  logical replication support.
- Track source offsets by Postgres LSN and encode them in `SourceOffset`.
- Resume from the last committed LSN when `seek` receives a valid checkpoint.
- Emit tombstones on DELETE: key is present, value is `None`.
- Emit Protobuf-framed bytes for keys and non-delete values, reusing
  `prost-reflect` and `crabka-schema-serde` framing conventions.
- Handle basic schema metadata/DDL by refreshing relation metadata when WAL
  relation messages show a changed table shape.
- Keep the Postgres-specific dependencies out of `crabka-connect`.

## Non-Goals

- A full Kafka Connect worker protocol or distributed task protocol.
- Snapshotting existing table contents before WAL streaming.
- Exactly-once source semantics beyond the existing runtime contract:
  checkpoint only after sink commit.
- Full arbitrary DDL replay. The connector tracks relation metadata needed to
  decode subsequent row changes and fails clearly on unsupported WAL messages.
- A `storage/pgdb` crate in this slice. The current checkout has no such crate;
  this work lands as `connect-postgres`.

## Architecture

The crate is split around a parser boundary so the durable source can be tested
without a live database.

```text
crates/connect-postgres/
  src/lib.rs
  src/config.rs      # PostgresSourceConfig via ConnectorConfig derive
  src/source.rs      # PostgresWalSource implements Source<Bytes, Bytes>
  src/offset.rs      # LSN <-> SourceOffset helpers
  src/model.rs       # EntityDifference and supporting CDC domain types
  src/schema.rs      # dynamic Protobuf descriptor/envelope construction
  src/pgoutput.rs    # logical message model + parser boundary
  src/error.rs       # crate-local errors mapped to ConnectError
```

`PostgresWalSource` owns:

- connector config
- SQL client for metadata lookups and slot/publication setup
- replication client/stream
- relation metadata cache keyed by Postgres relation id
- current checkpoint LSN
- small pending queue for WAL messages that expand into multiple records

The public constructor accepts a resolved `PostgresSourceConfig`. Runtime users
wire it like any other source:

```rust
let source = PostgresWalSource::connect(config).await?;
let handle = ConnectorRuntime::new()
    .add_source(source)
    .add_sink(sink)
    .checkpoint_store(store)
    .run()?;
```

## Configuration

`PostgresSourceConfig` uses the existing connector config SPI:

```rust
#[derive(ConnectorConfig)]
pub struct PostgresSourceConfig {
    #[config(required)]
    pub database_url: String,

    #[config(required)]
    pub slot_name: String,

    #[config(default = "crabka_connect")]
    pub publication_name: String,

    #[config(default = "public")]
    pub schema: String,

    #[config(name = "tables")]
    pub table_names: Vec<String>,

    #[config(default = 1000)]
    pub max_messages_per_poll: u32,
}
```

The first implementation assumes credentials are embedded in `database_url` or
provided by the caller through the config SPI before construction. A later
operator-facing slice can add richer Kubernetes/Vault secret references without
changing the source SPI.

## CDC Model

`EntityDifference` is the connector's stable row-change envelope:

```rust
pub struct EntityDifference {
    pub table: String,
    pub key: EntityKey,
    pub op: Operation,
    pub before: Vec<ColumnValue>,
    pub after: Vec<ColumnValue>,
    pub lsn: String,
    pub txid: Option<i64>,
    pub commit_timestamp_ms: Option<i64>,
    pub schema: TableSchema,
}
```

`Operation` covers `Insert`, `Update`, and `Delete`. DELETE records emit the
same key envelope but no value, matching Kafka tombstone semantics.

`ColumnValue` stores the decoded logical value as a typed scalar where possible
and as bytes/text for unsupported Postgres types. The first slice supports the
common scalar set: null, bool, signed integers, floats, text, bytea, numeric as
string, date/time/timestamp as string or epoch millis where Postgres supplies a
stable representation.

## Protobuf Framing

The connector constructs dynamic Protobuf descriptors for:

- `crabka.connect.postgres.EntityKey`
- `crabka.connect.postgres.EntityDifference`
- supporting repeated column/value messages

Encoding uses `prost-reflect::DynamicMessage` so table-specific columns do not
require generated Rust types. The wire bytes are framed using the same
Confluent-compatible Protobuf framing conventions already implemented by
`crabka-schema-serde`: magic byte, schema id, Protobuf message index, and body.

The source exposes key/value bytes directly as `ConnectRecord<Bytes, Bytes>`.
This keeps the generic runtime unchanged and leaves schema-aware registration
and converter reuse in one place.

## Logical Decoding Flow

Startup:

1. Connect to Postgres for metadata and replication.
2. Ensure the configured publication exists for the configured tables.
3. Ensure the configured logical replication slot exists.
4. If `seek` supplied an LSN, start replication from that LSN; otherwise start
   from the slot's confirmed flush position.
5. Load initial relation metadata for configured tables.

Polling:

1. Read the next logical replication message.
2. Update transaction and relation metadata for BEGIN/COMMIT/RELATION messages.
3. Convert INSERT/UPDATE/DELETE row messages into `EntityDifference`.
4. Encode key and value bytes.
5. Return one `ConnectRecord` at a time.
6. Advance the in-memory checkpoint to the record's commit LSN only after the
   record is yielded.

The runtime persists that checkpoint after the downstream sink commit is
durable. On restart, the runtime calls `seek`, and the source resumes from that
LSN.

## Offset Contract

Offsets use `SourceOffset` as:

```json
{
  "partition": {
    "database": "app",
    "slot": "crabka_slot"
  },
  "position": {
    "lsn": "16/B374D848"
  }
}
```

The source accepts only offsets whose slot matches the configured slot. LSN
parsing rejects malformed values and maps them to `ConnectError::Offset`.

If Postgres reports that the requested LSN is no longer available for the slot,
`seek` returns `ConnectError::Offset` with a clear unresumable-position message.

## Basic DDL Handling

Postgres `pgoutput` relation messages are the source of truth for table shape
while streaming. When a relation message changes the known column layout, the
connector replaces the cached `TableSchema` for that relation and uses it for
subsequent rows.

Records include schema identity metadata in headers:

- `crabka.pg.schema`
- `crabka.pg.table`
- `crabka.pg.lsn`
- `crabka.pg.operation`

Unsupported relation changes fail closed only when they make row decoding
ambiguous. This avoids silently producing incorrectly mapped fields.

## Error Handling

Errors map into `ConnectError` at the source boundary:

- connection/setup failures -> `ConnectError::Backend`
- WAL stream failures -> `ConnectError::Backend`
- malformed logical messages -> `ConnectError::Backend`
- unsupported row/DDL shape -> `ConnectError::Backend`
- malformed or unresumable checkpoints -> `ConnectError::Offset`
- Protobuf/schema framing failures -> `ConnectError::Convert`

The connector does not include credentials or secret values in error messages.

## Testing

TDD starts with unit tests that do not require Postgres:

- LSN string parsing and formatting round-trips.
- `SourceOffset` round-trips and rejects wrong slot/malformed LSN.
- INSERT logical event encodes a non-empty Protobuf-framed value.
- UPDATE logical event carries table/key/op/LSN metadata.
- DELETE logical event emits a tombstone value.
- Relation metadata refresh changes column mapping for later row events.
- Unsupported logical event fails with a deterministic connector error.

Integration tests are optional in the first pass and gated behind an ignored or
feature-gated Postgres test because they require a local Docker/Postgres setup
with logical replication enabled.

## Success Criteria

- `crabka-connect-postgres` builds as a workspace member.
- The connector can be constructed from typed config.
- It implements `Source<Bytes, Bytes>`.
- It emits Protobuf-framed insert/update values and DELETE tombstones.
- Its checkpoints are LSN-based and resumable through `SourceOffset`.
- Unit tests cover parser, offset, tombstone, and schema-refresh behavior.
