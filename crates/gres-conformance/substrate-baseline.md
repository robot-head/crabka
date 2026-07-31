# Substrate-backed engine baseline

The `gres-conformance` CI job runs the primary corpus twice: once against an in-process `crabka-gres` and once against a substrate-backed one (`--substrate-bootstrap`, whose WAL is a Kafka tenant topic on a live Crabka broker). This file explains why the second leg gates on `substrate-baseline.json` (6191/6198) rather than on the primary `baseline.json` (6192/6198), and enumerates every statement behind the difference. The number is a floor for one specific engine assembly, not a claim of parity with the in-process leg.

Seven statements mismatch here, and the set is now exactly the primary leg's own mismatch set plus one. Six of them are the primary leg's slack — the same six statements `baseline.json`'s 6192 already accounts for — and belong to that file, not this one. The seventh is a harness artifact of this leg's fresh oracle database. Nothing mismatches here for a reason intrinsic to the replicated engine.

## The oracle-database artifact (1 statement)

`catalog_functions.sql` runs `SELECT current_database()`. The corpus never drops the relations it creates, so a second leg replayed against the same oracle database would accumulate rows and degrade parity for reasons that have nothing to do with the subject. CI therefore creates a fresh `oracle2` database for this leg and points the oracle at it. The oracle then answers `oracle2` while the subject answers `postgres`, which is the constant Gres returns from `current_database()` regardless of the connection.

This mismatch is a property of the harness, not of the subject: the same statement matches on the primary leg, where the oracle database *is* `postgres`. With the sequence gap below closed it is the *only* thing separating this leg from the primary floor — the leg measures 6191 against a floor of 6192, and that one statement is the whole difference. The sharded gate creates `gres_sharded_oracle` and carries `sharded-baseline.json` for the same structural reason.

## The replicated-sequence gap (closed, 6183 → 6191)

This leg used to refuse every sequence advance. A substrate-backed engine is built by `SqlEngine::replicated` (via `open_substrate_engine` → `build_replicated_substrate_engine` in `crates/gres/src/lib.rs`), which puts the counter managers in `PersistMode::Replicated`: a counter advance must ride the commit batch into the WAL rather than be written straight to the applied store. `SequenceManager::alloc` — the internal per-table rowid counter — already did that; the user-visible sequence path did not, and both `setval_written` and the `advance` behind `nextval`/`nextval_written` answered `0A000 replicated SQL sequence updates are not wired yet`. That cost eight statements in `create_table_breadth.sql` — five refused `INSERT`s into `ctb_id`/`ctb_id2` and the three follow-up `SELECT`s that then found no rows.

All eight now match. `nextval`, `setval`, `SERIAL` and both `GENERATED … AS IDENTITY` forms work under `PersistMode::Replicated`, on three pieces:

- **An engine-wide cache** in `SequenceManager`, keyed by `RelationName`, holding each sequence's record as of this writer's most recent advance. Without it every `nextval` inside one uncommitted statement would re-read the same applied-store record and hand out the same value, so a multi-row `INSERT` into an identity column would violate its own primary key. It is engine-wide rather than per session so two sessions inserting into the same `SERIAL` table cannot both be served the value the other already took.
- **A staging seam**, `PendingSequences`, reached through `EvalCtx`'s `SequenceRuntime` — the same shape `EvalCtx::notify` gives `pg_notify()`, and for the same reason: expression evaluation is synchronous and cannot await a commit. The advance is staged there and folded into the next batch the session commits. A write statement already commits a batch, so the fold is free; a read-only `SELECT nextval('s')`, which reached the committer nowhere before, now commits in `finish_statement`.
- **Invalidation on the two events that can make a cache entry wrong**: a writer-generation change (`reseed_counters` → `reseed_sql_sequences`), and a catalog batch that creates or drops a sequence (`forget_sequences`, which reads the affected names out of the committed ops so a `SERIAL`'s implicit sequence is caught even though no statement spelled it).

### The failover invariant

Getting the invalidation wrong hands out duplicate identity values after a failover, so it is worth stating what actually makes re-seeding safe: **no `nextval` value reaches a client before the op recording it is durable.** Every statement that can advance a sequence commits before it returns, so at the moment a successor writer re-seeds, the applied store already reflects every value its predecessor handed out — re-seeding can only move forward. Values a dead writer took but never committed were never observed by anyone, so re-issuing them is invisible.

The converse is why the SQL sequence cache is *not* cleared by `SequenceManager::reseed_from_applied`, which the rowid counter uses: that one also fires whenever a distributed transaction another node owned resolves, so it can land midway through a statement. Clearing there would drop advances this writer had handed out but not yet committed, and the re-seed would issue them a second time.

### Transactional behaviour

A replicated advance keeps PostgreSQL's non-transactional `nextval` rather than following `alloc` and riding the transaction. On `postgres:18.4`, advancing, rolling back and advancing again yields `1` then `3`; the burned `2` is a documented, normal gap. The staged ops are therefore taken unconditionally — the abort paths fold them into the `clog Aborted` batch they were already committing — so the gap survives a rollback. This costs no extra round-trip in either direction: the advance rides a batch the statement was committing anyway, and only a read-only `SELECT nextval('s')` adds a commit where there was none.

### The supplied-value bug this exposed

`ctb_id.a` is `GENERATED BY DEFAULT AS IDENTITY` and `INSERT INTO ctb_id (a, b) VALUES (100, 'explicit')` supplies it, yet the old refusal still fired — because the engine evaluated every column's default before overwriting the supplied ones, consuming a sequence value for a column the statement had written explicitly. PostgreSQL advances a sequence only for a column it actually defaults; verified on `postgres:18.4` for `GENERATED BY DEFAULT`, `GENERATED ALWAYS` (with `OVERRIDING SYSTEM VALUE`), a plain `DEFAULT nextval('…')` column and `COPY` alike, and it is decided per row and per column. That is fixed in `unsupplied_defaults` (`crates/pgexec/src/exec.rs`), which was an in-process bug too.

The corpus does not catch it: `create_table_breadth.sql` reads `ctb_id` back after the explicit insert but never inserts again, so the burned value is invisible. Adding a statement that would catch it changes `total`, which is a hard equality gate on **four** baselines that share this corpus (`baseline.json`, `substrate-baseline.json`, `sharded-baseline.json`, `pooler-baseline.json`), and the README requires a measured parity report for each. The behaviour is covered instead by `crates/pgexec/tests/sequences_replicated.rs`, which asserts it in both persistence modes.

Removing this baseline now requires only reconciling `current_database()` with the fresh-oracle requirement; the sequence gap no longer contributes to it.
