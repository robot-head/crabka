# Gres D6 foreign keys, local engine — design

**Date:** 2026-07-31
**Status:** In progress
**Type:** SQL-parity wave. Implements wave D6 of the [SQL-Parity Program](2026-07-09-crabka-gres-sql-parity-program-design.md) on the single-node MVCC write path: `FOREIGN KEY` and `REFERENCES` constraints, referential actions, `MATCH` semantics, and real constraint deferral. Cross-range enforcement on sharded tables is a companion cycle and is named here only at its seam.

## Design Goals

Match PostgreSQL 18.4's observable referential-integrity semantics, not a reasonable approximation of them. The corpus that measures this wave is PostgreSQL's own `alter_table` and `truncate` regression files, so the target is the oracle's exact SQLSTATE, message text, and — critically — its *timing*, since when a check fires is user-visible in ways that are easy to get subtly wrong.

Cost nothing when no foreign key exists. The engine's write path is already the throughput-critical surface, and the overwhelming majority of relations participate in no foreign key. A relation with no FK must pay one boolean test per write, not a catalog lookup.

Preserve the concurrency property that makes foreign keys usable under load. In PostgreSQL a foreign key does not serialize writers against the parent row: many children may reference one parent concurrently, and a non-key `UPDATE` of the parent proceeds while they do. A design that takes an exclusive lock on the parent turns every hot dimension row into a convoy, and that is the difference between a feature and a footgun.

Add no new concurrency primitive. The engine already has a lock manager with a shared wait-for graph and eager deadlock detection. A new lock mode means a new way to deadlock and a new thing to reason about; if the existing primitives express the protocol, use them.

## Architecture Overview

A foreign key is three separable concerns, and keeping them separate is most of the design.

**Storage** is a catalog record in its own id-keyed namespace, with a reverse index so the parent side can answer "who references me?" without scanning. It is deliberately *not* a field on the table record.

**Timing** is a queue. Every check — even a `NOT DEFERRABLE` one — is appended during the row loop and drained after the statement completes. A `DEFERRABLE INITIALLY DEFERRED` constraint promotes its entry out of the per-statement drain into a transaction-scoped queue drained at `COMMIT`.

**Enforcement** is a probe plus a lock, both expressed in terms the engine already has: the referenced index's entry prefix is the lock identity, and the existing equality-probe helper is the lookup.

Nearly all of the new logic lives in one module. The write path gains hook sites that append to a queue and a single drain call; the DDL path gains one shared resolver. That concentration is what keeps `crates/pgexec/src/exec.rs` — already 17,000 lines and the schedule's bottleneck — from being the place this feature is written.

## Key Design Decisions

### Checks are queued and drained at end of statement, never inline

PostgreSQL implements referential integrity as `AFTER ROW` triggers, so even a constraint declared `NOT DEFERRABLE` is checked after the statement's rows exist. This is directly observable and not a subtlety: with a self-referencing foreign key, `INSERT INTO t (id, boss) VALUES (1, 1)` succeeds in PostgreSQL with no `DEFERRABLE` clause anywhere, because the row is in place by the time the check runs.

An inline probe at the point of the write rejects that statement. So the natural-looking implementation — check the parent as each child row is built, next to the uniqueness enforcement that is already there — is wrong, and wrong in a way that a single-row test suite never notices.

The hooks therefore append to a queue, and the drain runs once at the end of the statement. It runs at the end of the whole `WITH`-list-plus-body rather than the end of the body, because PostgreSQL treats that as one command and fires its trigger queue once for it.

This also settles a structural problem. The engine's per-row funnel that enforces `NOT NULL`, generated columns and `CHECK` predicates is synchronous and holds neither the KV handle nor the lock manager. A referential check needs both and must be `await`ed. Queueing sidesteps the conflict rather than restructuring the funnel.

Rejected alternative: make the funnel async and probe inline. It would cost a signature change through every write path, would still be semantically wrong per the paragraph above, and would put an `await` inside the hottest loop in the engine.

### The concurrency protocol reuses the unique-key lock; no new lock mode

PostgreSQL's referential-integrity triggers take a `FOR KEY SHARE` lock on the referenced row. The engine's lock manager has only `Shared` and `Exclusive`, and its row-lock mapping already folds `FOR KEY SHARE` onto `Shared` with a documented over-blocking divergence.

The temptation is to add a third mode. The better answer is to notice that the property `FOR KEY SHARE` buys is not about *rows* — it is about *keys*. So both sides of a foreign key name the same key-lock identity: the referenced index's entry prefix for the key value, the same byte string the uniqueness check already locks. The child side, inserting or updating a referencing row, takes it `Shared`. The parent side, deleting or changing the referenced key of a parent row, takes it `Exclusive`.

Three properties fall out. Many concurrent children of one parent key all take `Shared` and never contend, which is the convoy this design exists to avoid. A *non-key* update of the parent never touches the key lock at all, because the "indexed key unchanged, so skip the probe" rule that the uniqueness path already applies fires first — so PostgreSQL's headline property is preserved exactly. And because key locks and row locks live in the same wait-for graph, a cycle spanning both is detected and reported as `40P01` with no new machinery.

The pre-existing `FOR KEY SHARE` row-lock over-blocking is untouched by this wave and is a different thing entirely. It is worth saying so in the module documentation, because a reader who knows about it will otherwise assume the foreign-key protocol inherits it.

### Cascades fold their own writes into what the drain reads, and that is what terminates cycles

A referential action re-enters the write path: `ON DELETE CASCADE` deletes child rows, `SET NULL` updates them. Two problems follow.

The first is visibility. Writes staged by a statement are not yet in the KV, so a cascaded delete cannot see a previous cascade's tombstone by probing the store. Left there, termination could not come from observing the data — and neither could correctness, because a second constraint's action would then read the row as its own action never happened, and overwrite it.

The second is cycles. Mutually referencing tables with `ON DELETE CASCADE`, and self-referencing trees, must converge rather than recurse.

Both are solved by making the staged view the drain reads *grow*: each action's ops are folded into it before they are handed back. Every probe then sees the transaction's current state, so termination is what it is in PostgreSQL — the row a cycle comes back to reads as deleted, or its key no longer matches — and a second constraint's action operates on the image the first one left, which is how one `DELETE` of a doubly-referenced key nulls both referencing columns.

Cascaded writes still take the *outer statement's* bookkeeping rather than a fresh one, for the unique-key ledger and for a bound on the work: one constraint's action writes a given row at most once. That claim is keyed by `(row, constraint)`, not by row, precisely so a second constraint is not turned away from a row the first has just rewritten.

### The referent is identified by id, with the name kept only for display

PostgreSQL stores `pg_constraint.confrelid` — an object identifier, not a name. Copying that is not merely faithful, it is what makes the constraint survive the next wave: a relation that is renamed, or moved to another schema, keeps its id, so foreign-key identity is untouched by either operation.

The record therefore stores the referenced table's and index's ids as the authority, and their names purely so error messages and `pg_get_constraintdef` can render without a lookup. The display copies are rewritten by the catalog's rename batch, which already has a targeted site for exactly this kind of denormalization.

Column references are the exception: they are stored as names, matching how index metadata already stores its columns, and they ride the existing `RENAME COLUMN` rewrite path.

### Foreign-key records live in their own catalog namespace

An index is not a field on the table record — it is a separate record under its own key family, with a by-name key and a by-table key. A foreign key is the same shape of object, and gets the same treatment: a by-table key that is authoritative and a by-ref key that is an empty-payload reverse index.

The reverse index is not an optimization. The parent side needs "who references me?" on every delete and every key-changing update of a referenced table, and without it that question is a full catalog scan on the hot path.

Three further consequences argued for this over extending the table record. The table record's serialization is owned by a concurrent wave, and two waves editing one format is an expensive collision. The table struct has roughly fifteen literal construction sites across six crates, every one of which a new field breaks. And because these keys are keyed by *id*, the wave that re-keys the name-keyed catalog families by schema does not have to touch them at all.

### Deferral is a transaction-scoped queue, modelled on the notification queue

The session already carries one piece of transaction-scoped state with exactly the right lifecycle: pending `NOTIFY` payloads, which accumulate during a transaction, are committed with it, and are discarded on rollback through a shared teardown hook. The deferred-constraint queue is the same shape and reuses the same pattern rather than inventing a second one.

What a pending entry records differs by side, and the asymmetry is not arbitrary. A child-side entry is anchored on the row's identifier, because in this engine an `UPDATE` preserves it — so it is a stable row identity for the life of the transaction, the analogue of PostgreSQL following a tuple's update chain. A parent-side entry cannot do that, because by drain time the parent row is gone or re-keyed; it records the key *values*, which is what the check is actually about.

Implementation found one wrinkle in that story worth recording, because the obvious reading of it is wrong. Re-deriving the child key from the row identifier only works **at commit**. Within a statement the rows are still staged in the write batch and not yet in the KV, so re-reading by identifier finds nothing at all after an `INSERT`, or the pre-image after an `UPDATE`. A child entry therefore carries the key the hook staged, and promoting it to the transaction queue *drops* that staged key so the commit drain re-derives it from durable state. Same asymmetry, one more moving part.

The parent side has a mirror-image trap. PostgreSQL's `ri_Check_Pk_Match` re-probes for a live parent still supplying the key, and skips the violation if it finds one. In the end-of-statement drain the deleted or re-keyed parent row is *still live in the KV* — the delete is staged, not applied — so a naive re-probe always finds it and silently skips every check. Parent entries therefore record the originating row identifier and the statement drain discounts it, which is exact because the referenced columns are unique. The commit drain does not discount it, and that is precisely what makes `DELETE; INSERT; COMMIT` succeed under a deferred `NO ACTION`.

`RESTRICT` entries are never deferred, whatever the constraint's clause says — PostgreSQL creates `RESTRICT` triggers non-deferrable regardless. That is what makes `RESTRICT` and `NO ACTION` differ at all, and the difference is narrower than it first appears. Probing the oracle pinned it precisely:

- With an **immediate** constraint, both behave identically. `BEGIN; DELETE FROM parent; INSERT INTO parent VALUES (1); COMMIT;` fails at the `DELETE` under either, because an immediate check fires at end of statement, when the key genuinely is still referenced and not yet re-supplied.
- With **`DEFERRABLE INITIALLY DEFERRED`**, they diverge. `NO ACTION` defers to `COMMIT`, re-probes, finds a live parent supplying the key, and succeeds. `RESTRICT` ignores the deferral, fires at end of statement anyway, and fails.

So the re-supply idiom is a property of *deferral*, not of `NO ACTION`; `NO ACTION` is merely the mode that permits deferral to happen. Getting this backwards would produce an engine that wrongly accepts the immediate case.

The second difference is easier to miss and would never be guessed from the implementation, since the two modes read as synonyms: **they raise different SQLSTATEs**. `NO ACTION` reports `23503` with `update or delete on table "p" violates foreign key constraint "c_fk" on table "c"` and a `Key (id)=(1) is still referenced` detail. `RESTRICT` reports `23001` (`restrict_violation`) with `violates RESTRICT setting of foreign key constraint` and a detail saying `is referenced` rather than `is still referenced`. Two SQLSTATEs, two messages, two detail wordings.

A failed drain at `COMMIT` is an ordinary failed commit and needs no new path: the transaction's rows are already durable in the KV but invisible because the commit-log entry is never written, so the existing abort path makes them stay that way and the client sees the violation followed by an idle ready-for-query, as PostgreSQL does.

Savepoints need almost nothing, and the reason is worth recording so the next reader does not add machinery. Rolling back to a savepoint across a row-modifying sub-transaction is already refused, and every statement that can enqueue a check is a row-modifying statement — so the pending queue can never need unwinding. But `SET CONSTRAINTS` is a utility statement and *is* rollback-able, so the savepoint frame must capture the deferral *mode*, exactly as it already captures GUC state.

### `TRUNCATE` errors rather than cascading

`TRUNCATE` currently desugars to one unfiltered `DELETE` per table, which is elegant — it inherits MVCC, transactionality and rollback for free. With foreign keys it becomes wrong, because those deletes would fire `ON DELETE CASCADE` and PostgreSQL's `TRUNCATE` does not: it refuses when a table outside the truncate set references one inside it, and `TRUNCATE ... CASCADE` widens the *set* rather than firing the actions.

The desugaring survives intact. The truncate set is computed first, expanded transitively under `CASCADE`, and any referencing child outside it is a refusal. The set is then carried on the write context, and the parent-side hook skips foreign keys whose child is in it. By construction that is every remaining foreign key, so no referential action ever fires — expressed as a set-membership test rather than a "suppress referential integrity" mode, which is both narrower and easier to prove correct.

### DETAIL becomes a real wire field

The wire error carries severity, SQLSTATE and message, and nothing else. For most errors that is a gap; for `23503` it is a usability regression, because PostgreSQL's message says *that* a foreign key was violated and only the `DETAIL` line says *which key*. The same applies to the `42804` type-mismatch error raised when a foreign key cannot be implemented.

So the wire error gains optional detail and hint fields. The scope is deliberately narrow: the fields exist, the encoder emits them in PostgreSQL's field order, and only the foreign-key errors populate them. Every other error keeps omitting `DETAIL` — a pre-existing gap this wave does not widen, and one the conformance harness cannot catch either way, since it diffs SQLSTATE rather than message text.

## Integration

**Sharded tables** are refused with a typed `0A000` naming the constraint, for the duration of this wave. The observation that the engine's snapshot isolation with first-committer-wins does not prevent write skew, and that a foreign key is a write-skew-shaped invariant, is the right one and survives. Two things this section originally claimed about the sequel do not, and are corrected here rather than left to mislead — see [the cross-range design](2026-07-31-gres-d6b-cross-range-foreign-keys-design.md) for the full account.

The first was that this wave makes cross-range enforcement "a one-line deletion". It does not, and the gap is not in the probe. A foreign key's referent must be a unique key, and a sharded table cannot have one at all: `PRIMARY KEY`/`UNIQUE` on a sharded table is refused outright, `CREATE UNIQUE INDEX ... GLOBAL` with it, and deleting the refusal here only moves the failure to `42830` in `select_referenced_index`, which would have nothing to select. The prerequisite is global unique enforcement — the same work that blocks `ON CONFLICT` on sharded tables, and a larger piece of work than foreign keys themselves.

The second was that the key-lock identity, being a byte string derived from ids and values, "lifts to a distributed lock key unchanged". It does not lift, and the reason is worth stating precisely because the shape of the claim was appealing. `lock_bytes` builds `secondary_index_entry_prefix(parent_table_id, index_id, values)` — keyed by the *base table*. A global index entry is keyed by the index alone, with no table id in it, so the two name different byte strings for the same logical key. The manager is the more obvious problem — `RowLockManager` is per-engine and in-memory by design, and Percolator intents are keyed by `(table_id, rowid)`, which cannot express "no row may come to hold key K" — but the bytes do not survive the trip either.

What does survive is the seam. The parent probe and the child search both route through narrow functions taking the probe target as a parameter, so the call sites do not change shape; what changes is what sits behind them.

**Schemas** are the other companion wave, and the seam is the identity decision above: because the referent is an id, a relation that moves schema or is renamed does not disturb any foreign key. The one thing schemas add is cross-boundary policy — a permanent table may not reference a temporary one, and dropping a schema must account for constraints reaching into it.

**Extended protocol.** Every new statement form must describe correctly through `Parse`/`Describe`, not merely execute. For this wave that is mostly a negative obligation — `ALTER TABLE ... ADD FOREIGN KEY` and `SET CONSTRAINTS` must describe as zero-field results rather than erroring — plus the positive one that a parameterized `INSERT` into a table with a foreign key must still report its parameter types and then raise `23503` at execute, with the detail readable by a client driver.

## PostgreSQL compliance

The oracle is `postgres:18.4`, and the already-vendored `alter_table` and `truncate` regression files are the primary measurement: between them they carry the parent/child DDL block, circular references, and `ON DELETE CASCADE` under `TRUNCATE`. Several other adopted files use foreign keys incidentally in their setup and currently fail at the refusal, so a number of downstream statements should ratchet up without being targeted.

Because the conformance harness diffs SQLSTATE rather than message text, no message string in this wave is hard-coded from documentation or memory. The full set — every DDL validation error, both violation sides, `MATCH FULL`'s mixed-null detail, the three `2BP01` dependency refusals, `TRUNCATE`'s detail and hint, `pg_get_constraintdef` across the spelling matrix, and the `pg_constraint` and `information_schema` column values — was captured from a live `postgres:18.4` and is the reference the implementation is written against.

That exercise paid for itself twice. It corrected the type-mismatch detail, which names both sides with "of the referencing table" / "of the referenced table" phrasing rather than the shorter form that seemed natural. And it found the `RESTRICT` SQLSTATE split described above, which no amount of reading the implementation would have suggested, since `RESTRICT` and `NO ACTION` are otherwise near-synonyms.

It also settled a question the design had guessed at conservatively: composite foreign keys store both column lists **in the order written in the FK clause**, paired positionally, with the referenced *index* matched by column set and permuted into at probe time. So `FOREIGN KEY (b, a) REFERENCES p(y, x)` records `conkey = {2,1}`, `confkey = {2,1}`, renders as written, and reports violations naming `Key (b, a)`.

Deliberate divergences, each recorded in the implementing item's rustdoc and in the matrix row:

- A cascaded referential action cannot modify a row the triggering command already modified, per the cascade decision above.
- `ALTER TABLE ... ALTER COLUMN ... TYPE` on a key column does not revalidate the constraint. This section originally proposed refusing it with `0A000`, mirroring the refusal for a column a generated column reads; probing the oracle showed that PostgreSQL *succeeds* there, so a refusal would have invented a divergence rather than recorded one. What is actually implemented is the permissive path, and it was measured: the retype succeeds on both sides and the key stays enforced. The narrower gap is that the retype does not re-check that the two sides are still type-compatible — if they stop being, the mismatch surfaces as `23503` on a later child write, by way of the "no representation in the parent's types" short-circuit, rather than at DDL time.
- Parent-side actions fall back to a full child scan when no child index matches the foreign key's columns. PostgreSQL has the same asymptotics without a user-created index; what differs here is that a *leading-prefix* index does not help, because the secondary-index key encoding length-prefixes the whole encoded tuple, so a partial value list is not a byte prefix of a full key. That encoding detail is also why the referenced index's column order must be permuted into rather than assumed — a composite foreign key whose column order differs from the referenced key's would otherwise probe the wrong bytes while every single-column test passed.
- Foreign keys on sharded and partitioned tables are refused, per the integration section.

## Testing

Behavior tests live in `crates/pgexec/tests/`, split by concern — DDL validation, child-side DML, referential actions, deferral, concurrency, and `TRUNCATE` — using the in-process engine harness and `assert2`, with the concurrency cases following the two-session pattern the unique-key lock tests established.

Four regressions are load-bearing rather than routine, because each pins a decision that a plausible implementation gets wrong: the self-referencing single-statement insert must succeed under a `NOT DEFERRABLE` constraint; a composite foreign key whose column order differs from the referenced key's must probe correctly; back-validation must read the in-flight rewritten rows of a multi-subcommand `ALTER TABLE` rather than storage; and a non-key update of a parent must not block behind a concurrent child insert.

Catalog introspection is verified by comparing whole `pg_constraint` rows and byte-exact `pg_get_constraintdef` output across the full spelling matrix, since `psql`'s `\d` renders its foreign-key sections entirely from that function.
