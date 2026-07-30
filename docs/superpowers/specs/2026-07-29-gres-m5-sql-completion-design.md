# Gres M5 SQL-completion — design

**Date:** 2026-07-29
**Status:** In progress
**Type:** Program execution cycle. Drives the [SQL-Parity Program](2026-07-09-crabka-gres-sql-parity-program-design.md) to its M5 gate — no row in [`docs/PG_COMPAT_MATRIX.md`](../../PG_COMPAT_MATRIX.md) left in a `Wave-assigned` disposition — against a pinned PostgreSQL 18.4 oracle.

## Design Goals

- **Measured, not asserted.** Every claim of "supported" is a differential result against a live `postgres:18.4` oracle, recorded in the conformance corpora and ratcheted in the baselines. The matrix disposition follows the measurement; it never leads it.
- **The regression suite is the yardstick.** PostgreSQL's own `src/test/regress` corpus is the only honest measure of "all SQL language features". Adoption breadth is expanded first so the remaining work is visible before it is scheduled.
- **Parallel by file ownership.** The program's remaining waves are wide but shallowly coupled. Work is dispatched in batches whose per-task file sets are disjoint, so wall-clock is bounded by the slowest task in a batch rather than the sum of the batch.
- **No silent narrowing.** A feature that cannot be completed lands with its divergence stated in rustdoc, in the matrix row, and in the wave report — never as a quiet subset.

## Architecture Overview

Three surfaces move together in every wave:

1. **Grammar** (`crabka-pgparser`) — lexer tokens, expression forms, statement forms. The parser is the program's narrowest resource: nearly every remaining wave needs a grammar change, so batches allocate parser regions (expression parsing, statement dispatch, `FROM`-item parsing) to at most one task each.
2. **Evaluation** (`crabka-pgexec`) — the executor materializes relations (`Relation { scope, rows }`), which makes the query-shape waves (window functions, `GROUPING SETS`, `DISTINCT ON`, recursive CTEs) tractable as post-materialization passes rather than planner surgery.
3. **Measurement** (`crabka-gres-conformance`) — the primary corpus grows with each wave; the adopted `pg_regress` corpus grows independently and ratchets per file.

## Key Design Decisions

### Widen the measurement surface before widening the engine

The adopted `pg_regress` corpus started at two files. Adopting the type, query, and DDL families up front converts an unknown backlog into a ranked one: the triage groups failing statements by the missing feature that would unlock them, so wave ordering is driven by statements-unlocked rather than by feature-family tidiness. The cost is a temporary drop in reported regress parity, which the per-file ratchet absorbs by design.

Rejected alternative: adopt each regress file as its owning wave lands. That keeps the headline number flattering and hides the size of the remaining work — the opposite of what an M5 gate needs.

### One registry seam per extensible dispatch point

The engine had a hardcoded `unnest`-only set-returning-function path in `FROM` position, mirrored in three places (execution, schema-describe, distributed planning). Each such hardcoded point is replaced by a single registry seam before the family that needs it is populated, so the family's members are data rather than code, and the schema-describe path cannot drift from the execution path. The same treatment applies to scalar functions, aggregates, and window functions.

### The harness must fail loud, because its failure mode is flattering

Widening the corpus surfaced three defects in the conformance harness itself, and all three had the same shape: they made parity look *better* than it was.

- A `COPY` statement sent down the simple query path left the connection in copy mode. Every later statement then returned the harness's no-SQLSTATE marker on **both** sides — and two dead connections compare equal, so they scored as matches. One `COPY` could turn the remainder of a run into free parity.
- A rejected `COPY` desynchronized the extended-protocol exchange and killed the connection one statement later, with the same consequence.
- Statements were reassembled byte-by-byte as Latin-1, so every non-ASCII literal was mangled identically on both sides — again comparing equal.

The lesson generalizes past `COPY`: a differential harness compares two engines, so any fault that hits both sides symmetrically is invisible in the score and *raises* it. The design response is that the harness treats a lost connection as a measurement failure rather than a result — it reconnects and re-runs the statement — and caps every statement with a wall clock so a wedged engine costs one statement instead of the run. `COPY` is routed through its own subprotocol in both directions, absorbing the inline `\.`-terminated data block that `pg_regress` files carry.

Two environmental requirements follow, and they are load-bearing rather than hygiene: the oracle database must be empty when a run starts, and the subject must be freshly started. Neither engine is reset between corpus files, so a leftover relation turns `CREATE TABLE` into `42P07` on one side only and skews everything downstream. For the same reason the corpus itself must not reuse a table name across files with different definitions — the second definition silently runs against the first one's schema.

### Every feature family is refuted before it is believed

Each wave lands as implement-then-verify, where the verifier is a separate reviewer whose brief is to *disprove* the implementer's claims against the oracle, not to confirm them, and who defaults to "overstated" when the evidence is thin.

This is not ceremony. Applied to the second wave's six families, it returned five "overstated" and one "partial" — 59 refuted claims — and what it caught was not cosmetic:

- `ALTER TABLE … ADD COLUMN c int CHECK (c > 0)` aborted the server process on any non-empty table, because the back-validation scan read pre-`ADD` row widths against a post-`ADD` scope. On an empty table it passed, which is exactly why the implementer's own testing missed it.
- `ALTER COLUMN … TYPE` did not rebuild indexes, so an index scan returned *zero rows* for a row a sequential scan could see, and primary-key enforcement silently disappeared.
- `CHECK` predicates were never analyzed at DDL time, so a constraint naming a nonexistent column was accepted and left the table permanently unwritable.
- The compatibility matrix asserted things about *PostgreSQL* that the oracle contradicts.

The common shape is that all of these produce a plausible-looking success. A differential corpus catches them only if a statement happens to exercise the exact combination; a reviewer told to hunt for silent wrong answers finds them on purpose. The cost — roughly one verifier per implementer — is small against the alternative, which is a matrix row that says `Implemented` over a feature that returns wrong rows.

The rule that follows: a wave is not done when its author says so. It is done when an independent pass has tried to break it and reported what survived.

### Divergences are typed, not implicit

Where PostgreSQL's behavior is reproducible only at disproportionate cost, the wave records the divergence in three places that CI checks against each other: the rustdoc on the implementing item, the matrix row's disposition and note, and the typed behavior manifest in `crabka-gres-conformance`. A behavior that is refused must refuse with PostgreSQL's SQLSTATE and a stable message fragment, asserted by the anti-rot check.

## Integration

- **Sharded/timestamp tables:** each wave states its sharded-table story. "Single-range only, fail clear with `0A000` on sharded" remains an acceptable stated answer under the program's standing rules; silence is not.
- **Pooler:** behavior through PgDog transaction mode is stated per wave; session-scoped features (cursors, prepared statements, `LISTEN`) carry the pooler caveat explicitly.
- **Extended protocol:** every new expression and statement form must describe correctly through `Parse`/`Describe`, not only execute through the simple query path. Schema-only evaluation paths are extended in the same change as the execution path.

## PostgreSQL compliance

The oracle is `postgres:18.4`. The quirk is the spec: output formatting, SQLSTATE, message text, and NULL/edge behavior are matched to the oracle, and any deliberate deviation is a documented, scoped non-goal in the matrix. The corpus and its baselines land in the same reviewed change as the engine work that moves them.
