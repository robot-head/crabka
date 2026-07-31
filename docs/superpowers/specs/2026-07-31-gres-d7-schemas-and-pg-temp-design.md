# Gres D7 schemas and `pg_temp` — design

**Date:** 2026-07-31
**Status:** Proposed
**Type:** SQL-parity wave. Implements wave D7 of the [SQL-Parity Program](2026-07-09-crabka-gres-sql-parity-program-design.md): real SQL schemas, a real `search_path`, and a real per-session `pg_temp`, replacing the single flat namespace the engine has today. The foreign-key/temp boundary is the seam shared with [D6](2026-07-31-gres-d6-foreign-keys-local-design.md).

## Design Goals

Match PostgreSQL 18.4's observable name-resolution semantics, which were captured from a live `postgres:18.4` before any of this was designed. Several of the captured behaviours contradict what a careful reading of the documentation would suggest — `DROP TABLE nope.t` reports a missing *schema* where `SELECT * FROM nope.t` reports a missing *relation*; a nonexistent entry in `search_path` is silently skipped rather than rejected; a syntactically broken `search_path` value is accepted. Every semantic in this document comes from that capture, and where the capture is silent this document says so rather than guessing.

Have exactly one implementation of resolution. Today there are three independently coded resolution orders and four mutually inconsistent parser policies for qualified names, which is why the same missing relation surfaces as `3F000` at parse time from `INSERT` (`crates/pgparser/src/parser.rs:3489-3493`) and as `42P01` at execution time from `SELECT` (`crates/pgparser/src/parser.rs:8718-8722`). Under a flat namespace those divergences are cosmetic. Under a search path they are the whole feature, because every one of them is a place shadowing can silently not happen.

Make `pg_temp` cheap enough to use the way people actually use it. The pattern this exists to serve is a temporary table per request, created and dropped inside one short transaction. A design in which `CREATE TEMP TABLE` costs a cluster-wide lock across a Raft round-trip is a feature nobody can turn on.

Change the on-disk catalog format outright. Crabka is greenfield and undeployed: there is no persisted state to migrate and no client pinned to a build. The catalog key layout simply changes, local data directories are wiped, and no compatibility shim is written.

## Architecture Overview

A relation name exists in three forms, and almost every defect described below comes from the engine collapsing two of them into one `String`.

**As written** is what the parser produces: an optional schema qualifier and a name, each already case-folded by the lexer. This is `RelationRef`, and it is the only thing the AST carries. It is deliberately unresolved: the parser has no catalog, and the oracle requires that a missing schema be reported differently depending on what the statement was going to do with it, which is a decision only the executor can make.

**As resolved** is a schema, a name, a relation kind and an id, produced by one function from a `RelationRef` and a resolution context. The resolution context is the search path plus the session's backend id — everything needed to turn an unqualified name into a schema.

**As stored** is a two-part catalog key built from `(schema, name)` with each part length-prefixed. It is never derived from a name by string surgery.

One function crosses each boundary, and the type system enforces that: after this wave, `crabka_pgcatalog::get_table(kv, &str)` does not exist, so a bare-name lookup does not compile. That is the mechanism, not a convention, and it is what makes shadowing work for *every* operation rather than for the handful someone remembered to update.

The thing making a change of this size tractable is that there is no catalog cache anywhere in the engine — no `OnceLock`, no snapshot, no generation counter, no invalidation. `get_table` (`crates/pgcatalog/src/lib.rs:953-975`) is a bare `kv.get`. So there is no cache-coherence problem hiding behind the resolution seam; there is only a large mechanical edit.

## Key Design Decisions

### `RelationRef` carries an optional schema, and the `3F000` decision leaves the parser

Every relation name in the AST is a bare `String` today (`crates/pgparser/src/ast.rs:12-13`), which forces each of the four parser paths to decide what a dot means, and they disagree. `expect_relation_name` (`crates/pgparser/src/parser.rs:3467-3494`) carries a hardcoded four-schema whitelist and raises `3F000` at parse time for anything else. The `FROM` clause (`:8718-8722`) does not call it and keeps `s1.t` verbatim. `qualified_relation_name` (`:8008-8016`) also keeps it verbatim, for a different set of statements. `qualified_object_name` (`:6826-6833`) silently discards the qualifier entirely. `CREATE VIEW` uses `expect_object_name` (`:3445-3456`), which has no dot handling at all, so `CREATE VIEW public.v` is a syntax error.

All four collapse into one function producing a `RelationRef`, and the parse-time `3F000` is deleted. It has to be: the oracle draws a distinction the parser cannot see.

```
SELECT * FROM nope.t;      ERROR: 42P01  relation "nope.t" does not exist
DROP TABLE nope.t;         ERROR: 3F000  schema "nope" does not exist
CREATE TABLE nope.t (x);   ERROR: 3F000  schema "nope" does not exist
```

The split is not lookup-versus-creation, which is the natural guess. `DROP` resolves the schema first and reports the schema; only a `SELECT`-style *reference* reports the relation. So the resolver takes a disposition argument with three values, not two, and only the executor knows which one applies.

Note also that the error names the case-folded, dotted, unquoted form — `SELECT * FROM S.T` reports `relation "s.t" does not exist`, not the source text. Since the lexer already folds unquoted identifiers and preserves quoted ones, a `RelationRef` built straight from `Ident` tokens renders correctly with no extra machinery.

Rejected: an N-part name list, mirroring PostgreSQL's `List *names`. The engine has one database, so a three-part name is only ever the `cross-database references are not implemented` error, which is a one-line check against a two-field struct. The generality would buy nothing and would push a `match` on list length into every consumer.

One wrinkle worth flagging before it is discovered during implementation: `Statement::Comment.object_name` is documented as `table.column` for a column comment — a dotted flattening of a different pair. It cannot become a `RelationRef` without also carrying the column, and its key builder (`crates/pgcatalog/src/lib.rs:1205-1211`) concatenates without a separator that distinguishes the two dots.

### Catalog keys are two-part and length-prefixed; `unqualified_relation` is deleted, not extended

`catalog_key` (`crates/pgkv/src/key.rs:337-341`) appends a relation name to a fixed prefix after passing it through `unqualified_relation` (`:355-362`), which strips a leading `public.` or `pg_temp.` case-insensitively and passes every other dotted name through unchanged. This is the entire namespace model, and it cannot be repaired by extending the strip list, because the oracle demands that two relations whose flattened names are identical coexist:

```
CREATE TABLE "a.b" (x int);   -- relation "a.b" in schema public
CREATE SCHEMA a;
CREATE TABLE a.b (y int);     -- relation "b" in schema a
```

Both exist at once with distinct contents, and `SELECT * FROM "a.b"` and `SELECT * FROM a.b` read different relations. No flattened `schema.relation` string can represent that, so the key must be built from two parts that can be recovered unambiguously.

Length-prefixed parts rather than a separator byte. A `\0` separator would technically work — a PostgreSQL identifier cannot contain a NUL — but `\0` is already the sub-family separator inside the catalog prefix (`view_key`, `crates/pgcatalog/src/lib.rs:1004-1009`, builds `catalog_key("") + "\0view/" + name`), and the existing scan filter is exactly the kind of ad-hoc disambiguation a length prefix retires. That filter is a live bug: `list_tables` recovers a name by stripping the prefix and rejecting any suffix containing `/` (`:996-1001`), so `CREATE TABLE "a/b"` succeeds and `get_table` finds it, but the relation never appears in `pg_class`, `information_schema.tables`, or `schema_contents` — which means `DROP SCHEMA … CASCADE` silently misses it.

`split_schema` and `qualify` (`crates/pgcatalog/src/lib.rs:475-491`) go with it. `split_schema` is not merely dead weight; `pg_class_rows` calls it on the stored name (`crates/pgexec/src/exec.rs:9169`), which is why `CREATE TABLE "a.b"` today reports namespace `a` in `pg_class` for a relation that is in `public`. Same flattening, second symptom.

Seven other builders follow the same change: `view_key` (`:1004`), `catalog_sequence_key` (`:1929-1933`), `catalog_index_key` (`:1174-1178`), `comment_key` (`:1205-1211`), `table_privilege_key` (`:2366-2374`), `catalog_sharding_key` (`crates/pgkv/src/key.rs:366-370`), and the three partition prefixes (`crates/pgexec/src/partition.rs:35-39`).

What this does **not** fix, and should not be claimed to: `ALTER SCHEMA … RENAME TO` stays refused with `0A000` (`crates/pgexec/src/exec.rs:521-539`). Its refusal message is accurate before and after — a relation's catalog key carries its schema, so renaming the schema still means rewriting every relation key in it. Making that atomic is a separate problem about batch size, not about key layout.

### One resolution seam, and a bare-name lookup that cannot compile

Three resolution orders exist today and no two agree. `build_table_expr` (`crates/pgexec/src/exec.rs:7381-7403`) tries CTE, then virtual catalog relation, then view, then table. `build_table_expr_schema_with_ctes` (`:8648-8678`) repeats that order in a second, schema-only copy. `catalog_fn::resolve_relation_by_name` (`crates/pgexec/src/catalog_fn.rs:509-532`) strips `pg_catalog.` or `public.` — a *fourth* qualifier policy, disagreeing with `unqualified_relation`'s `public.`/`pg_temp.` — then tries base relation, view, sequence, index. Every DDL site bypasses all three and calls `get_table` directly with no order at all.

These collapse into `resolve(ctx, ref, disposition)`. The disposition is the three-valued thing the oracle forced above; the context supplies the search path and the session's backend id.

The load-bearing part is the demotion. There are 88 `get_table`/`get_view` call sites outside test modules across the engine crates — 55 in `crates/pgexec/src/exec.rs`, 12 in `session.rs`, 6 in `crates/gres-ranges/src/forward.rs`, 5 in `fk.rs`, 4 each in `catalog_rel.rs` and `lib.rs`, 2 in `catalog_fn.rs` — and 140 counting the in-file test modules that will need the same treatment. Any single one left taking a bare name is an operation that silently does not shadow, which is precisely the defect the compatibility matrix already records: today an unqualified `DROP TABLE t` resolves to the permanent relation where PostgreSQL drops the temporary one. Removing the bare-name entry point turns "did we update all 140?" from a review question into a compile error. The catalog crate keeps id-keyed and `(schema, name)`-keyed lookups; the `&str` form disappears.

This is what makes the oracle's headline case work:

```
INSERT INTO shad VALUES (99);     -- hits the TEMP one
SELECT x FROM shad;               -> 99
SELECT x FROM public.shad;        -> 1
DROP TABLE shad;                  -- drops the TEMP one
SELECT x FROM shad;               -> 1   (the permanent one is now visible)
```

Rejected: keep `get_table(kv, &str)` and add a `get_table_in(kv, schema, name)` beside it, migrating callers opportunistically. It is the cheaper diff and it is the wrong trade — it preserves, for an unbounded period, exactly the situation where whether an operation honours the search path depends on which of two similarly named functions its author reached for.

### The resolution context rides `EvalCtx` as an optional session capability

`EvalCtx` (`crates/pgexec/src/clock.rs:43-65`) already carries `current_user` and `session_user` as plain fields, and `sequence: Option<Arc<SequenceRuntime>>` (`:58`) for the session-only capability `nextval` needs — including the session's catalog KV handle. A resolution context has the same shape and the same lifetime, so it goes in the same place rather than becoming a new parameter threaded through every evaluator signature. It is `Option` for the same reason `sequence` is: planning and DDL paths construct `EvalCtx::test_default()` (`crates/pgexec/src/exec.rs:314`, `:8615`) and genuinely have no session.

It reaches `current_schema()` (`crates/pgexec/src/func.rs:1115-1118`, a literal today), `current_schemas()` (`crates/pgexec/src/catalog_fn.rs:370-378`, hardcoded to `{pg_catalog,public}` with no session handle in scope), and the `regclass` paths.

`search_path` stops being a string. The GUC is registered with an identity parser that accepts anything (`crates/pgexec/src/session.rs:984-986`) and has no reader anywhere in the workspace; it must become a parsed list, for two verified reasons rather than one aesthetic one. First, `set_value` (`crates/pgparser/src/parser.rs:3605-3688`) flattens a comma list into one `String` joined with `", "`, and `Token::Ident` (`crates/pgparser/src/token.rs:5`) carries no was-quoted bit — so `SET search_path = "MySchema", public` renders as `MySchema, public`, and `SHOW` cannot reproduce the oracle's `"MySchema", public`. Second, an entry containing a comma is unrepresentable in the flattened form at all. The fix is not to remember quoting but to store the list and re-quote on output where an entry is not a bare identifier, which is what PostgreSQL does.

What the design must **not** add is validation, and this is worth stating because the obvious instinct is wrong in both directions. `SET search_path = '"unbalanced'` **succeeds** on the oracle; PostgreSQL's list parsing is far more permissive than it looks, and inventing a `22023` here would manufacture a divergence. A nonexistent schema in the path is likewise never an error — it is silently skipped:

```
SET search_path = notme;
SELECT current_schema;         -> NULL (empty)
SELECT current_schemas(true);  -> {pg_catalog}
CREATE TABLE t (x int);        -> ERROR: 3F000 no schema has been selected to create in
```

Two consequences follow that are easy to miss. `current_schemas` filters against the catalog at read time, so resolution consults the catalog and not just the GUC. And `current_schema` returns NULL when no explicit entry exists, so its return type becomes nullable — today's `Datum::Text("public")` literal cannot express that.

`pg_catalog` is implicit-first unless listed explicitly, in which case it sits where it was written, and the implicit entry genuinely shadows: after `CREATE TABLE public.pg_class (x int)`, `SELECT count(*) FROM pg_class` still reads the catalog relation. Creation lands in the first *existing* explicit entry — `SET search_path = nosuch, s1, s2` puts a new table in `s1`.

The cost of not having this is already measured in the tree: one leaked `SET search_path` in the conformance corpus accounted for 75 false mismatches (`crates/gres-conformance/src/main.rs:358-366`).

### `pg_temp` is a real schema named from a per-session backend id that already exists

The temp namespace is `pg_temp_<backendid>` (observed `pg_temp_27`) and sits **first** in the resolution order, ahead of the implicit `pg_catalog`: `current_schemas(true)` returns `{pg_temp_27,pg_catalog,public}`. It is not in `search_path` and is never written there.

That name needs a per-session id, and the engine reports a per-*process* one. `pg_backend_pid()` returns `std::process::id()` (`crates/pgexec/src/catalog_fn.rs:325-329`), so every session in the process reports the same value, and `pg_stat_activity.pid` (`crates/pgexec/src/catalog_rel.rs:1009`) repeats it. But a genuine per-connection id already exists one crate away: `CancelRegistry::register` allocates one from a `NEXT_PID` counter (`crates/pgwire/src/server.rs:30`, `:191-193`), announces it in `BackendKeyData` (`crates/pgwire/src/session.rs:602`), and hands it to the engine at `connect_with_pid` (`:608`) — where `SqlEngine` uses it for exactly one thing, registering the session on the notification bus (`crates/pgexec/src/lib.rs:3063`), and keeps no field of its own.

So this is plumbing, not invention. The session gains a `backend_id`, `pg_backend_pid()` and `pg_stat_activity.pid` report it instead of the process id — which is independently a bug fix, since in PostgreSQL those are by definition the value `BackendKeyData` announced, that being how a cancel request addresses a backend — and the temp namespace is named from it. `Engine::connect()` passing 0 (`crates/pgexec/src/lib.rs:3030`) has to go: a session with no registered id has no temp namespace.

Temp relations are then ordinary catalog rows in an ordinary schema. Rejected: a session-local KV overlay, which reads as the cleaner model and is not. There is zero session-scoped catalog state in the engine today — `catalog_kv` is one `Arc<dyn Kv>` cloned into every session (`crates/pgexec/src/lib.rs:3036`) — so an overlay would be a new concept that every projection, every `list_tables`, every DDL batch and the entire remote scan path would have to learn. Putting temp relations in the shared catalog under a per-session schema name leaves all of those working unchanged, and makes teardown a `DROP SCHEMA … CASCADE`-shaped walk rather than a new mechanism.

The honest cost: a crashed backend leaves its rows behind, and `NEXT_PID` restarts at 1 on process restart, so a fresh session can inherit a dead one's namespace. The remedy is that the first temp-relation creation in a session clears its own namespace before creating anything in it — one extra catalog batch on first use, not per statement. That a session's temp namespace is the session's alone to reset is a property of this design, not something the capture speaks to.

`ON COMMIT DELETE ROWS` and `ON COMMIT DROP` are refused today with a message that names exactly this gap: "a temporary table is an ordinary relation here, with no session-scoped lifetime to hang the disposition off" (`crates/pgexec/src/exec.rs:297-310`). Once there is one, the disposition is stored on the table record and a commit-time drain walks the session's temp namespace. `DISCARD TEMP`, an explicit no-op today (`crates/pgexec/src/session.rs:3741-3742`) and the primitive a connection pooler issues on reset, is the same walk with `DROP`; so is session teardown. Three callers, one function.

One wrinkle to record rather than discover: DDL here is non-transactional and commits its own batch (`crates/pgexec/src/session.rs:5217-5227`), so `ON COMMIT DROP` issues a catalog batch *after* the data commit rather than as part of it. A process death between the two leaves the relation, which the first-use purge then cleans up.

`CREATE TEMP TABLE s.t` is `42P16 cannot create temporary relation in non-temporary schema`. No new error variant is needed — `ExecError::InvalidTableDefinition` already maps to `42P16` (`crates/pgexec/src/error.rs:48`, `:605`).

### Table ids come from a per-session block, and the catalog lock is split in two

Every DDL statement takes `catalog_lock` (`crates/pgexec/src/session.rs:5206`) and holds it across `committer.commit(ops).await` (`:5224`) — a Raft round-trip on a multi-node deployment. The comment above it (`:5050-5053`) is explicit that this protects two distinct things: the shared catalog keyspace, and the atomicity of `next_table_id`'s read-bump-commit (`read_next_table_id`, `crates/pgcatalog/src/lib.rs:2841-2850`, read at `:729` and bumped in the same batch at `:739-742`).

A temp relation needs the second and not the first. Nothing outside the session can see its schema, so nothing outside the session can collide on its name. It does still need a globally unique *id*, because row keys are `/<table_id>/<index_id>/<rowid>` in one shared keyspace (`crates/pgkv/src/key.rs:1-3`).

Leaving it alone was considered and is the option this design exists to avoid: it makes every `CREATE TEMP TABLE` a cluster-wide serialization point, which for a temp-table-per-request workload is not a slow feature but an unusable one.

Splitting `catalog_lock` into a keyspace lock and a counter lock, holding the counter lock only across the read and bump, is a real improvement — it removes the Raft round-trip from the critical *section*. It does not remove it from the *statement*: the counter bump is still a durable write, so `CREATE TEMP TABLE` still costs a commit.

So: split the lock **and** allocate ids in per-session blocks. A session claims a contiguous run under the counter lock and hands ids out locally, so the common `CREATE TEMP TABLE` needs no coordination at all and the amortized cost is one bump per block. The block is claimed lazily on first use, so a session that creates no temp table claims nothing. Block size is a constant rather than a GUC; eight is enough that a request-scoped temp table almost never refills, and a wasted block costs eight ids out of a `u32`. The lock split is independently useful and is the first step toward the concurrent-DDL work the `:5053` comment already defers.

The consequence to state plainly: table ids stop being densely allocated in creation order. Nothing in the engine depends on that — `pg_class.oid` is derived from the id (`crates/pgexec/src/exec.rs:9171`), and PostgreSQL's oids are not dense either — but any test asserting that the second table created has id 2 breaks, and that is a real cost.

Rejected: a separate reserved id band for temp relations, so they never touch the shared counter. The band would have to be sized in advance, and row keys, per-table sequence keys and lock identities all key on `table_id` (`crates/pgkv/src/key.rs:374-378`), so a second allocator is a second thing that can collide — for no gain over a block.

### The wire carries a `table_id`, not a name

`ScanRequest` and its relatives ship `table_name: String` (`crates/gres-ranges/src/forward.rs:2715`, `:3003`, `:3213`, `:3859`) and the *remote* node resolves it against *its own* catalog (`:3394-3395`, `:3472`, `:3535`). A session-dependent name cannot survive that.

Shipping a canonical `(schema, name)` pair is the minimal change and is wrong for temp relations: the remote node has no notion of the originating session, so `pg_temp_27` there is either meaningless or — if that node also has a session 27 — someone else's data. It also re-resolves, so a rename between planning and scanning changes what is read.

Excluding temp relations from distributed plans is sound and overreaching: it forbids joining a temp table to a sharded one, which is a principal reason to want a temp table in an analytics path.

Ship the id. It is already the authority everywhere else — row keys, lock identities, and D6's foreign-key referents, whose record documents this exact rationale (`crates/pgcatalog/src/lib.rs:192-215`): names are denormalized display copies rewritten on rename, ids are identity. Making the wire id-keyed makes the schema question disappear from it rather than answering it, and the remote side gets its column layout from an id-keyed lookup instead of a name-keyed one. That works for a temp relation for the same reason the overlay was rejected: its catalog row is in the one shared keyspace, readable from any node.

What this does not buy is isolation. A temp relation becomes unresolvable *by name* from another session, but a deliberate `SELECT * FROM pg_temp_27.t` from a different session still reads it.

The open question this document recorded — what PostgreSQL raises there — was put to the oracle afterwards, with one session holding a temp table open while another qualified it. **PostgreSQL raises nothing consistent, and there is no refusal to copy:**

```
SELECT * FROM pg_temp_93.ttq;      -- succeeds, returns ZERO rows
INSERT INTO pg_temp_93.ttq …;      0A000  cannot access temporary tables of other sessions
DROP TABLE pg_temp_93.ttq;         -- SUCCEEDS: drops another session's relation
TRUNCATE pg_temp_93.ttq;           42P01  (after the drop above)
CREATE TABLE pg_temp_93.other …;   42P16  cannot create relations in temporary schemas
CREATE TEMP TABLE pg_temp_93.o2 …; 42P16  of other sessions
```

The `SELECT` answering zero rows is not a rule but an artefact: the relation's pages are in the owning backend's local buffers, so a foreign reader sees a relation with no blocks on disk, and only a path that actually reaches `ReadBufferExtended` raises `0A000`. Only the *creation* refusal is a real, stated rule, and it is the one implemented. A reference to another session's namespace is left alone, and that this engine then reads the rows where PostgreSQL reads none is recorded as a divergence.

Two other things the same capture settled. `pg_temp_<n>`'s `n` is PostgreSQL's backend *slot* id, not its pid: a session reporting `pg_backend_pid() = 4004` had namespace `pg_temp_88`. In this engine the wire layer's per-connection id is both, so the two agree by construction. And `pg_class` and `pg_namespace` both show *other* sessions' temporary relations and namespaces, while `information_schema.tables` and `.columns` hide them — PostgreSQL's `pg_is_other_temp_schema` filter is on the standard's views only, not on the catalog.

### The `public` schema bug is real and is fixable ahead of everything else

`BUILTIN_SCHEMAS` is `["pg_catalog", "information_schema"]` (`crates/pgcatalog/src/lib.rs:456`), while the doc comment directly above it (`:453-455`) says "`public` is a real, droppable schema". `public` is in neither the builtin list nor the store, so:

- `schema_exists(kv, "public")` is false on a fresh store (`:527-529`), so `CREATE SCHEMA public` passes the duplicate gate (`:537-549`) — and the executor's `CreateSchema` arm adds no guard of its own (`crates/pgexec/src/exec.rs:490-520`) — and writes a row.
- `pg_namespace_rows` then emits **two** rows for `public`, both with oid 2200: one hardcoded (`crates/pgexec/src/exec.rs:9134-9139`) and one from `list_schemas` (`:9140-9147`), since `namespace_oid("public")` is the constant either way (`crates/pgexec/src/catalog_rel.rs:266-277`, `crates/pgexec/src/exec.rs:8761`).
- Symmetrically, `DROP SCHEMA public` on a fresh store is `3F000` (`crates/pgcatalog/src/lib.rs:598-605`).

The fix has to respect that `public` is droppable where the other two builtins are not, so it cannot simply join `BUILTIN_SCHEMAS`, whose entries `list_schemas` appends unconditionally (`:512-517`). Two options. Bootstrapping a real `public` row at store initialization is the cleaner model and needs an initialization seam the catalog does not have — every default today is derived lazily from an absent key, `read_next_table_id` returning 1 being the pattern (`:2841-2849`). Making `public` a *droppable builtin* — present unless a tombstone key exists, `DROP SCHEMA public` writing the tombstone, `CREATE SCHEMA public` removing it and otherwise reporting `42P06` — stays lazy and is three small changes. Take the tombstone, and delete the hardcoded projection row.

This is independent of everything else in this document and should land first, on its own.

## Migration order, and what actually ships independently

Schemas and `pg_temp` are one piece of work, not two phases. They share the `RelationRef`, the two-part keys and the resolution seam entirely, and `pg_temp` adds nothing structural on top — only a per-session namespace name, a search-path ordering rule, a lifetime, and the id-allocation change. Designing them separately would mean designing the resolution seam twice.

Nothing here needs a migration shim. The catalog's on-disk key layout changes, local data directories are wiped, and that is the whole story.

The internal ordering is:

**The two independent bug fixes first, each on its own.** The `public` schema defect described above, and `pg_backend_pid()` reporting the process id rather than the announced backend id. Neither depends on anything else here, both are small, and the second is a prerequisite for the temp namespace name.

**`RelationRef` alone, as a mechanical AST change.** This is genuinely independently shippable and genuinely worth shipping on its own. It unifies the four parser policies, moves the `3F000` decision to the executor, and stops `SELECT * FROM s1.t` and `INSERT INTO s1.t` failing with different codes in different phases. It changes no observable behaviour for a correct program, so it is a refactor with a compatibility-matrix note rather than a feature — but it is large, mechanical, conflicts with essentially every other change in the tree, and gates everything below it.

**Two-part keys, the resolution seam and the resolution context as one change.** This is the part where an appealing-looking split does not survive contact.

Keys cannot precede `RelationRef`: builders taking `(schema, name)` with every caller holding only a `String` means every call site does an ad-hoc split, which is `split_schema` reintroduced at every site instead of deleted at one.

Keys and `RelationRef` without the resolution seam *would* compile and *would* give `CREATE TABLE s.t` and `SELECT * FROM s.t` end to end — real schemas, everything qualified. That is the ordering the research recommended, and it is the one place this design disagrees with it. Without the seam, each of the 88 non-test lookup sites needs a schema from somewhere, and that "somewhere" becomes a per-site judgement made 88 times; a good fraction would hardcode `public`. The tree would be correct for fully qualified names while carrying 88 fresh opportunities to get shadowing wrong — which is exactly the defect class the seam exists to make impossible. It is mergeable in the narrow sense that it builds and passes tests, and it is not a state anyone should want to stop at.

The seam without the context is not a smaller change either; it is a rename, since a resolution function with nothing to resolve against just relocates today's hardcoded order.

**`pg_temp` last, and separately.** Once the above has landed, `pg_temp` really is a schema whose name comes from the session, a search-path ordering rule, a lifetime with three call sites, the id-block change, and the `42P16` boundaries. All of it is additive, and none of it needs the resolution seam to change shape.

## Integration

**Foreign keys (D6).** The FK/temp boundary is symmetric, and both directions are `42P16` with distinct wording: `constraints on permanent tables may reference only permanent tables` and `constraints on temporary tables may reference only temporary tables`. The second direction is the one a reader expects not to exist. Enforcement is a persistence comparison inside `fk::resolve_foreign_key`, which already resolves both sides and already refuses sharded relations there. D6's identity decision means nothing else is disturbed: because a foreign key names its referent by id, a relation that moves schema or is renamed leaves every constraint untouched. `DROP SCHEMA … CASCADE` gains one obligation — a constraint reaching into the schema from outside it must be accounted for.

**Views.** Two problems, one of which this wave makes worse before anyone fixes it.

The conversion rule is oracle-verified: a permanent view over a temp table is silently converted, landing in the temp namespace with `relpersistence = 't'`, and reported with `NOTICE: view "t_v" will be a temporary view`. The conversion is implementable — resolve the view body once at `CREATE VIEW` and set the view's persistence from what it reads. The notice is not, because there is no notice channel anywhere: `pgwire::Severity` has `Error` and `Fatal` only (`crates/pgwire/src/error.rs:20-26`), there is no `NoticeResponse` encoder, and `execute_ddl` returns `(QueryResult, Vec<WriteOp>)` (`crates/pgexec/src/session.rs:5217`) with nowhere to put one. The compatibility matrix already records a sibling gap — PostgreSQL's `NOTICE` on a skipped `CREATE TABLE IF NOT EXISTS` is not emitted either. A notice channel is a small wave of its own that would pay for itself across all of these; it is out of scope here because it touches the wire crate and every result path, and because the conformance harness diffs SQLSTATE and cannot see it either way.

The deeper problem is that views have no dependency identity. A view is stored as source text (`crates/pgcatalog/src/lib.rs:333-337`) and re-parsed in the *reader's* context at scan time (`crates/pgexec/src/exec.rs:7390`). Today that is merely fragile — `rename_table_view_ops` has to token-walk the stored text, rewriting only `FROM`/`JOIN` slots and unaliased `<table>.<column>` qualifiers, and refuses with `0A000` when the name appears anywhere it cannot prove (`crates/pgexec/src/exec.rs:13931-13951`). With a search path it becomes a correctness regression: a view created under `search_path = s1` and read under `search_path = s2` silently reads a different relation.

The mitigation is one field — store the creator's resolved search path beside the definition and re-parse under it — and it should land with this wave, because the regression is introduced by this wave. It is a patch over the representation and not a fix: a later `DROP SCHEMA s1` still silently changes what the view returns, where PostgreSQL refuses the drop. The real fix is to store a view as a resolved dependency list plus a rewritten body, the analogue of PostgreSQL's `pg_rewrite`, and that is a wave of its own.

**Catalog projections.** `pg_class_rows` hardcodes `PUBLIC_NAMESPACE_OID` for views, sequences and indexes (`crates/pgexec/src/exec.rs:9188`, `:9198`, `:9216`), `information_schema_table_row` hardcodes the literal `public` (`:9370-9377`), `information_schema.schemata` is three literals that never read the catalog (`:9339-9344`) — so a `CREATE SCHEMA`d schema appears in `pg_namespace` but not in `schemata` — and `relpersistence` is synthesized as `"p"` at projection time and never stored (`:9279-9281`). All of these become reads of the relation's actual schema and persistence. The oid side is already ready: `namespace_oid` hashes an unrecognised schema into a reserved band (`crates/pgexec/src/catalog_rel.rs:266-277`).

**Extended protocol.** Every new form must describe correctly through `Parse`/`Describe` and not merely execute. Mostly that is a negative obligation — `CREATE SCHEMA`, `SET search_path`, `DISCARD TEMP` and `CREATE TEMP TABLE` describe as zero-field results — plus the positive one that a prepared statement's relation reference must resolve against the search path in force at `Parse`, since that is when the result descriptor is fixed.

## PostgreSQL compliance

The oracle is `postgres:18.4`, captured 2026-07-31. Every semantic asserted in this document came from that capture, and three of them contradicted the plan this design started from: the `DROP`/`SELECT` error split, the permissiveness of `search_path` parsing, and the second direction of the FK/temp refusal. Two of the three would have shipped as confident divergences.

The corpus that measures this wave is PostgreSQL's own regression files, several of which currently fail early because a schema-qualified name does not parse. The `search_path` leak in `create_index.sql` is the clearest single signal available: it accounted for 75 false mismatches (`crates/gres-conformance/src/main.rs:358-366`), and after this wave it should account for none — while the statements it previously masked start being measured honestly, so the headline conformance number may move in either direction on the first run.

The compatibility matrix rows that this wave rewrites are `CREATE SCHEMA` (currently "a schema is inert"), `DROP SCHEMA` (currently "`CASCADE` and `RESTRICT` are indistinguishable"), `DISCARD` (currently "no temporary-table support"), and the temporary-table divergence block on `CREATE TABLE`, which today records all three symptoms — not dropped at session end, visible to other sessions, and does not shadow.

Divergences this wave deliberately leaves in place:

- A `NOTICE` is never emitted, so the temp-view conversion is silent. No notice channel exists.
- A view's stored text is re-parsed in the *reader's* context, and the creator's search path is not recorded beside it. A view created under one `search_path` and read under another resolves its base relation against the reader's path. The conversion rule itself does not depend on this — a view body here is one `FROM` item naming a base relation, so its persistence is decided once, at `CREATE VIEW` — but the rebinding remains.
- A temp relation is unresolvable by name from another session, and creating in another session's namespace is refused, but a deliberate `SELECT * FROM pg_temp_<n>.t` reads the rows where PostgreSQL reads none. PostgreSQL has no refusal to copy there; see the open question above, now answered.
- `ALTER SCHEMA … RENAME TO` stays `0A000`, for the reason its existing message gives.
- Table ids are no longer densely allocated in creation order.

## Testing

Behaviour tests live in `crates/pgexec/tests/`, split by concern — qualified-name resolution and its three error dispositions, `search_path` semantics, temp-relation lifetime, and shadowing — using the in-process engine harness and `assert2`, with the multi-session cases following the two-session pattern the D6 concurrency tests established.

Five regressions are load-bearing rather than routine, because each pins a decision that a plausible implementation gets wrong. An unqualified `DROP TABLE` must take the temporary relation and leave the permanent one visible. `CREATE TABLE "a.b"` and `CREATE TABLE a.b` must coexist with distinct contents. `SET search_path = nosuch, s1, s2` must create in `s1` and must not error on `nosuch`. `SET search_path = '"unbalanced'` must succeed. And a scan forwarded to a remote node against a temp relation must read the right rows, which is the test that fails if the wire keeps a name.

Catalog introspection is verified by comparing whole `pg_namespace`, `pg_class` and `information_schema.schemata` rows against the oracle rather than field by field, since the projections are where a half-migrated schema model shows up first.
