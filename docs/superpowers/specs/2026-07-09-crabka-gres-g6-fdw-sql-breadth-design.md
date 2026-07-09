# Gres G-6: FDW + SQL breadth — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design. The sixth slice of [Chapter Gres](2026-07-09-crabka-gres-chapter-design.md): the FDW becomes an honest product surface, and SQL breadth becomes a standing process with a ratcheting conformance gate. Depends only on G-1; runs parallel to G-2…G-5.

## Context — the verified gaps

1. **`_headers` is always empty.** The FDW's envelope column exists, but `crabka-client-core`'s `FetchedRecord` surfaces only `offset/key/value/timestamp` — the fetch helper does not decode record headers, and the donor deliberately refused to fabricate them. The fix belongs in the published client crate, not the FDW.
2. **The protobuf path is a stub for a reason that no longer exists.** The donor's `build_message_descriptor` could not resolve which message type a schema id denotes because published schema-serde 0.3.7 returned bare schema text; the current workspace exposes `FetchedSchema.message_type` and `SchemaCache::writer_message_type`/`seed_writer_message_type` — exactly the missing input.
3. **SQL breadth has a known frontier and a known order.** The donor's SP41 table-constraints design exists (approved, unbuilt — `ColumnDef` is still `{name, ty}`); indexes and window functions are the two structural absences behind it (the executor is a full-scanner; there is no `OVER`). pgbench requires primary keys, so constraints are also the path to the chapter's deferred pgbench ambition.
4. **The parity gate already exists and wants to ratchet.** G-1's `baseline.json` pins `{total, matched}` with total fixed and matched floor-checked; growing the corpus/parity is a deliberate baseline bump — the mechanism is built, this slice makes it the standing process.

## Design Goals

- **Topics-as-tables is real:** headers populated, all three wire formats (Avro/JSON/protobuf) decode end-to-end, and querying the tenant's own cluster is a one-liner.
- **Breadth as a pipeline:** every SQL feature lands as its own design cycle with corpus growth and a deliberate, reviewed baseline ratchet — the differential culture as permanent process, not a one-time gate.

## Non-goals

- **Kafka *sink* through SQL (INSERT into foreign tables)** — read-only FDW stays read-only this slice.
- **Any specific breadth feature's design** — constraints, indexes, and windows each get their own spec when their cycle starts; this document fixes scope, order, and process only.
- **PL/pgSQL, C extensions** — the first is far-future breadth; the second is definitionally out (chapter).

## The FDW track (concrete items, each independently gated)

### Headers through the published client

`crabka-client-core`'s fetch path gains record-header decoding — `FetchedRecord` grows `headers: Vec<(String, Option<Bytes>)>` (or the crate's existing header type if one exists in the record-batch layer; the v2 record format already carries them on the wire, they are simply dropped today). This is a public API addition to a published crate: reviewed as a client feature with unit coverage against real record batches, and consumed by every future client user, not just the FDW. The FDW then populates `_headers` (rendering per the donor's chosen text shape for the envelope column, with a round-trip test producing headers through `crabka-client-producer` and reading them back via SQL). **Gate:** produce-with-headers → `SELECT _headers` returns them.

### Protobuf completion

`build_message_descriptor` is implemented over `writer_message_type`: resolve the schema text + message type via the cache, compile the descriptor set (the `protox`-based test path already proves the compilation approach), decode via `prost-reflect` `DynamicMessage` as the existing typed tests anticipate. `IMPORT FOREIGN SCHEMA` derives columns for protobuf subjects the way it already does for Avro/JSON. **Gate:** register protobuf schema → produce → `IMPORT FOREIGN SCHEMA` → typed `SELECT`, end-to-end in the roundtrip harness.

### Own-cluster ergonomics

A `CREATE SERVER … FOREIGN DATA WRAPPER kafka_fdw` with no `bootstrap` option defaults to the tenant compute's own substrate cluster (the compute knows its bootstrap from G-2 wiring; local-mode engines have no default and keep requiring the option). Registry-provisioned tenants get topics-as-tables with zero configuration. **Gate:** the roundtrip test rewritten against a default-server tenant.

## The SQL-breadth track (a standing process)

*(Superseded as scope, preserved as process: the breadth track's contents are absorbed and completed by the [SQL-Parity Program](2026-07-09-crabka-gres-sql-parity-program-design.md) — a full PG18-surface wave map with milestone gates and a per-command compatibility matrix. The ratchet/oracle/cycle process defined below is unchanged and is what every parity wave runs under; the big-three ordering below survives as parity waves D1/D2/Q2.)*

- **Order:** table constraints (port the donor's SP41 design as the first in-tree breadth cycle — NOT NULL/DEFAULT/CHECK/UNIQUE/PK; prerequisite for pgbench) → secondary indexes (the deepest cut: a new index key family in `crabka-pgkv`, DML maintenance, point/range access paths in the executor — its own multi-slice program in all likelihood) → window functions (`OVER`, frames — executor-local). Re-ordering requires a chapter-level decision; inserting smaller features (e.g. missing scalar functions) between cycles does not.
- **The named "normal DDL" backlog** *(added after the compatibility review, so the gap between "normal Postgres table definitions" and this engine stops being invisible)* — cycle candidates after the big three, each still its own designed-and-ratcheted cycle, roughly ordered by how often a stock application hits them: **core types** (smallint/int2, real/float4, varchar(n)/char(n), uuid, json/jsonb, arrays, SERIAL/identity — today's surface is exactly 12 types and every one of these is a parse error), **statement completeness** (INSERT…SELECT, RETURNING, ON CONFLICT, UPDATE…FROM / DELETE…USING, IF NOT EXISTS, TRUNCATE, ALTER TABLE beyond constraints, multi-name DROP), **session and tooling** (SAVEPOINT/ROLLBACK TO, EXPLAIN, COPY, views, sequences, DISTINCT ON, NULLS FIRST/LAST, LIMIT expressions). Explicitly *not* breadth-track items, recorded so nobody looks for them here: `SERIALIZABLE`/SSI (an engine-architecture question, flagged in G-9's non-goals), PL/pgSQL, triggers, and C extensions (definitional).
- **Each cycle ships:** a slice design doc; parser/AST + executor + (where relevant) storage changes; corpus files covering the feature *including PostgreSQL's error cases*; and a **deliberate baseline ratchet** — `baseline.json`'s `total` and `matched` bump in the same reviewed commit as the corpus growth, with the parity report demonstrating the new floor. A baseline change in any other kind of commit is a review-blocking smell (the G-1 rule, now permanent).
- **Oracle discipline:** new corpus areas are validated against the same pinned `postgres:18` oracle; where PostgreSQL's behavior is surprising, the quirk is the spec (the donor's north star carries over verbatim).

## Integration

- **`crates/client-core`:** header decoding in the fetch path (published API addition).
- **`crates/gres-fdw`:** `_headers` population, protobuf descriptor completion, default-server resolution (fed by G-2's substrate wiring when present).
- **`crates/pgparser` / `crates/pgexec` / `crates/pgkv`:** per breadth cycle, under that cycle's own design doc.
- **`crates/gres-conformance`:** corpus growth + baseline ratchets per cycle.

## Kafka / wire compliance

Header decoding follows the Kafka record-format v2 header encoding exactly (varint counts/lengths), verified against batches produced by the workspace's own producer and — where cheap — a JVM-produced fixture, keeping the client crates' differential culture. Nothing else touches the wire.

## Testing

- Headers: client-core unit tests on real encoded batches (+ JVM fixture if available); FDW roundtrip with produced headers.
- Protobuf: descriptor-resolution units (message_type present/absent/pending) + the end-to-end roundtrip gate.
- Default server: roundtrip harness variant on a substrate tenant.
- Breadth cycles: each carries its own suite + corpus + ratchet; the conformance job enforces the floor continuously.

## Risks

- **Header API shape in a published crate** — additive struct growth is still semver-relevant pre-1.0; reviewed as a client-core change with its own changelog entry.
- **Index cycle scope** — flagged now as likely multi-slice; the breadth process explicitly permits decomposing it when its design cycle starts.
- **Baseline ratchet abuse** (bumping to absorb a regression) — countered by the same-commit rule and the parity report artifact making direction visible in review.

## Resolved decisions

- FDW items: headers via client-core, protobuf via `writer_message_type`, own-cluster default server. Read-only stays read-only.
- Breadth order: constraints → indexes → windows; one design cycle per feature; corpus + deliberate baseline ratchet in the same reviewed commit.
- G-6 has no single terminal gate; each item/cycle carries its own, and the slice is "done" as a scope definition once the three FDW items land — breadth continues indefinitely by design.
