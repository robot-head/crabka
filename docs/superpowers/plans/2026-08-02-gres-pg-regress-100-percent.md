# Gres pg_regress 100% Compatibility Plan

**Goal:** Make the unmodified PostgreSQL 18.4 core regression schedule pass against Gres, replacing the partial adopted-corpus percentage with a literal upstream `pg_regress` result.

**Current state:** The adopted 50-file corpus matches 9323 / 14272 statements (65.3%). PostgreSQL 18.4's core schedule contains 231 test files, so the adopted percentage remains a development signal rather than the final compatibility claim. The checked-in monotone baseline remains the first deterministic run: 6 / 231 tests (`comments`, `infinite_recurse`, `collate.icu.utf8`, `psql_crosstab`, `collate.linux.utf8`, and `collate.windows.win1252`) with 225 semantic failures, 172573 changed diff lines across 4322 hunks. The latest infrastructure-clean serial review run passes 8 / 231 by additionally passing `test_setup` and `portals_p2`, leaving 223 semantic failures and 185694 changed diff lines across 4447 hunks. It removes both failures and improves 84 recorded failures, but the now-complete fixtures expose 14 larger diffs and 19 equal-sized changed fingerprints, so the checked-in monotone baseline cannot yet move. PostgreSQL `point` and `path` have native datums and complete storage/wire paths. Catalog-backed table inheritance merges parent definitions, emits PostgreSQL's duplicate-column notices, exposes `pg_inherits`, scans descendants by default, and honors `ONLY`. The untouched shared setup now matches exactly: range definitions persist subtype/collation, while tablespace and operator-class creation have their command surfaces accepted. Range types now have distinct type and datum identities; all six built-ins resolve with PostgreSQL OIDs/subtypes, survive schema/value storage, support binary/text wire encoding, expose `pg_type` rows, validate input, apply discrete canonicalization, construct typed values, expose bounds, compare, and implement containment, overlap, directional, adjacency, union, intersection, difference, merge, and intersection aggregation. Built-in range arrays reuse the native array input, storage, catalog, and wire paths. User-defined range constructors resolve through the durable type registry, and range parse errors expose PostgreSQL-compatible structured diagnostics through direct casts and `pg_input_error_info`. The focused `rangetypes` diff has fallen from 1720 changed lines / 15 hunks to 423 / 19; it still fails on expression error positions, polymorphic range routines, non-btree access methods, exclusion constraints, date extremes, and multiranges. The next semantic wave must implement those native range surfaces and catalog-visible tablespace/operator-class lifecycle before their owning files can pass. Both PostgreSQL self-check modes pass, Gres completes the whole schedule with zero infrastructure failures, and the pinned `REL_18_4` schedule fingerprint is `63419f82d4a5faaf711658608a3b7b6b45ccc5c2a64e1b4e5c111ed9de648118`.

**Architecture:** Use PostgreSQL's own `pg_regress`, `psql`, schedule, SQL, data, expected output, and `resultmap` as the authority. Keep `crabka-gres-conformance` as the fast statement-level diagnostic tool while mismatches remain; do not grow it into a second implementation of `pg_regress`.

**Completion contract:** A pass uses the pinned `REL_18_4` corpus without edited SQL, Gres-specific expected files, ignored tests, or result normalizers beyond PostgreSQL's own `resultmap`. The final result is 231 / 231 in serial and parallel modes, with no crash, timeout, or connection loss.

## Task 1: Add the authoritative upstream runner

**Files:** Create `scripts/gres-pg-regress.sh`; modify `.github/workflows/ci.yml`; update `crates/gres-conformance/README.md`.

- [x] Fetch the official PostgreSQL 18.4 release archive into `target/` and verify its pinned SHA-256 before extracting or building it; record the corresponding `REL_18_4` tag as provenance.
- [x] Build the matching PostgreSQL 18.4 `pg_regress`, `psql`, server binaries, and `src/test/regress/regress` shared library from that one verified source; pass the built library directory through `pg_regress --dlpath` so its `PG_LIBDIR`/`PG_DLSUFFIX` environment points every `:regresslib` reference at the pinned module.
- [x] Run PostgreSQL against itself in a fixed-locale `pg_regress --temp-instance` built from that source. A containerized oracle is not a valid self-check unless the regression data and shared-library paths are mounted at the exact absolute paths emitted by `pg_regress`.
- [x] Run Gres with the official `--use-existing --dbname=crab` path, retaining `regression.diffs`, per-test output, server logs, and exit status under one artifact directory.
- [x] Prove the self-check passes the untouched schedule with `--max-connections=1` and at normal parallelism before using the runner's Gres result as evidence; restart Gres with empty state between those subject modes.

**Gate:** PostgreSQL against itself is 231 / 231; an intentionally changed expected file makes the wrapper fail.

## Task 2: Establish a monotone progressive gate

**Files:** Create `crates/gres-conformance/pg-regress-baseline.json`; modify `scripts/gres-pg-regress.sh` and `.github/workflows/ci.yml`.

- [x] Seed one entry per failing test from the first clean upstream run, recording the test name, mismatched unified-diff hunk/line count, and SHA-256 of that test's diff; do not permit wildcards or reason-free categories.
- [x] Fail CI when a passing test regresses, an upstream scheduled test disappears, a known failure starts passing without removal, its mismatch count increases, or its diff fingerprint changes without a reviewed baseline update.
- [x] Permit a baseline update only when the owning test's mismatch count decreases or the test is removed; the changed fingerprint and supporting result artifact land in the same review.
- [x] Keep the existing per-statement baseline for local root-cause ranking, but label it adopted-corpus parity everywhere.
- [x] Publish upstream passed/total and the shrinking per-test mismatch baseline in the job summary.

**Gate:** The wrapper distinguishes new, removed, improved, worsened, and same-count-but-different failures; the checked-in mismatch surface can only shrink.

## Task 3: Remove harness and setup blockers

**Files:** Primarily `crates/pgwire`, `crates/pgexec`, `crates/pgcatalog`, and `crates/gres-conformance`; exact files follow each minimized failure.

- [x] Make `test_setup` and PostgreSQL's shared data files load before dependent tests.
- [x] Add native PostgreSQL `point` and `path` values through type resolution, storage, casts, catalog rows, and wire encodings so the shared geometry fixtures can load.
- [x] Implement inherited fixture tables, including merged parent columns, duplicate-definition notices, `pg_inherits`, descendant scans, and `ONLY` semantics before treating `person`/`road` dependents as meaningful failures.
- [x] Implement server-side `COPY table FROM 'file'` through the same atomic text-import path as wire `COPY FROM STDIN`; missing files report `58P01`.
- [x] Accept schema GRANT/REVOKE syntax, including schema lists and `CREATE`/`USAGE`, and validate every schema and role target under the current trust-auth model.
- [x] Treat SQL-standard `RETURN expression` routine bodies as `LANGUAGE SQL`, while retaining the missing-language error for source-string bodies.
- [x] Register PostgreSQL's `allow_in_place_tablespaces` boolean GUC so the official setup preamble is accepted.
- [ ] Match complete wire-visible outcomes needed by `psql`: notices, warnings, diagnostics, command tags, row counts, headings, and COPY state transitions.
- [x] Make the five remaining shared-setup statements load: accept in-place tablespace and operator-class creation, and persist range subtype/collation metadata.
- [ ] Complete the staged setup surfaces: native range bounds/operators/multiranges plus catalog-visible tablespace and operator-class lifecycle and support functions.
- [x] Validate and canonicalize literals for staged user-defined range types, including infinite bounds, quoted/escaped bounds, malformed delimiters, subtype casts, ordering, and empty-range normalization; the focused upstream `rangetypes` diff shrinks from 1720 lines / 15 hunks to 1396 / 12.
- [x] Give built-in and user-defined ranges a distinct `ColumnType` identity; resolve PostgreSQL's six built-in range OIDs/subtypes, serialize them, expose their `pg_type` rows, and canonicalize discrete bounds. The focused diff falls again to 1163 changed lines / 20 hunks.
- [x] Add typed range datums, durable storage, text/binary wire encoding, six built-in constructors, bound accessors, btree comparison, containment, and overlap. The focused diff falls to 852 changed lines / 38 hunks.
- [x] Add directional, adjacency, union, intersection, difference, and merge operators/functions; add `range_intersect_agg`, registry-backed user-defined constructors, and PostgreSQL boolean GUC prefixes. The focused diff falls to 463 changed lines / 23 hunks.
- [x] Implement `pg_input_error_info` through the shared cast validator, including structured range-literal diagnostics and valid-input null fields. The focused diff falls to 443 changed lines / 19 hunks.
- [x] Preserve range parser DETAIL fields through the shared type-error and wire-error path. The focused diff falls to 434 changed lines / 19 hunks; the remaining lines in that block are expression positions, not parser diagnostics.
- [x] Add built-in range arrays through element resolution, literal casts, durable schema/row storage, catalog OIDs, and text/binary wire encoding. The focused diff falls to 423 changed lines / 19 hunks; the remaining array-adjacent failures belong to polymorphic routine resolution.
- [ ] Eliminate nondeterministic unordered results rather than weakening comparisons.
- [x] Treat every crash, I/O loss, or timeout as a harness failure, never as an SQL mismatch or a match on two dead connections.
- [x] Reject positional parameter numbers outside PostgreSQL's signed-32-bit lexer range before allocating parameter-shape vectors; the upstream `numerology` case now returns `42601` rather than consuming unbounded CPU and memory.
- [x] Keep regress-scale lateral derived joins bounded by caching only conservative, nonvolatile specializations (including the semantic no-op `OFFSET 0`) and reusing their equijoin indexes under the blocking-query memory limit.
- [x] Evaluate default-frame window `count` and `sum` incrementally by peer group; retain the general frame evaluator for every other aggregate and explicit frame.
- [ ] Re-run the full serial schedule after each shared fix and ratchet only tests whose recorded mismatch surface shrank.

**Gate:** Every remaining failure reproduces as an engine semantic difference; infrastructure failures are zero.

## Task 4: Burn down the measured semantic roots

For each item, first add one focused test at the shared layer that fails before the fix, implement the smallest shared correction, run the owning upstream file, then run the complete serial schedule and ratchet both compatibility ledgers.

- [ ] Split the 1089 wrong-row mismatches by statement family; fix the largest coherent family first rather than treating it as one issue.
- [ ] Fix the earliest error in each transaction-abort cascade before touching its downstream `25P02` statements.
- [ ] Resolve the fixture-enabled baseline changes before ratcheting: 14 worsened tests (`type_sanity`, `updatable_views`, `sanity_check`, `join`, `object_address`, `groupingsets`, `misc`, `merge`, `tidscan`, `psql`, `cluster`, `temp`, `returning`, and `with`) and 19 equal-size changed fingerprints (`line`, `box`, `polygon`, `geometry`, `opr_sanity`, `expressions`, `copyencoding`, `create_type`, `create_am`, `brin`, `brin_multi`, `misc_functions`, `create_role`, `subscription`, `select_views`, `foreign_data`, `xmlmap`, `rangefuncs`, and `compression`).
- [ ] Complete bit strings, composite/record values, `bytea`, `reg*` object identifiers, and exact float special-value behavior.
- [ ] Implement aggregate `ORDER BY`, ordered-set aggregates, record-returning function column definitions, and recursive CTE `SEARCH`/`CYCLE`.
- [ ] Complete expression/partial indexes, stored views over general queries, sequence lifecycle, array-slice assignment, and partitioned-table update semantics.
- [ ] Finish catalog descriptions and dependency rows required by upstream sanity and introspection queries.

**Gate:** The adopted corpus reaches 100% for every already-vendored file, and the upstream serial failure list strictly shrinks in each reviewed wave.

## Task 5: Reopen incompatible non-goals

Literal 231 / 231 is incompatible with retaining PostgreSQL-visible exclusions exercised by the core suite. Each item must either implement the observable behavior or remain an explicit blocker to the 100% claim; test-only canned output does not count.

- [ ] Multiple databases and reconnect behavior.
- [ ] Roles, privileges, ownership, and row-level security.
- [ ] Tablespaces, large objects, prepared transactions, publications, and subscriptions.
- [ ] Access methods, operator classes/families, casts, collations, and encoding variants.
- [ ] C-language regression functions or a production-grade compatible execution mechanism.
- [ ] Planner and `EXPLAIN` details asserted by upstream expected output.

**Gate:** No scheduled upstream test is excluded because its feature is marked `Non-goal` or `Error-with-notice` in `PG_COMPAT_MATRIX.md`.

## Task 6: Turn on concurrency and distributed storage

- [ ] Reach 231 / 231 with `--max-connections=1` before diagnosing parallel-only failures.
- [ ] Run the untouched parallel schedule at PostgreSQL's normal concurrency and fix MVCC, locking, concurrent DDL, advisory-lock, and catalog races at their shared source.
- [ ] Add one focused Rust regression for each parallel-only defect.
- [ ] Run one parallel pass per PR and require three consecutive clean passes for milestone certification.
- [ ] Repeat the authoritative gate against substrate-backed Gres; keep PgDog transaction-pooling checks separate because session-affine upstream tests are not valid transaction-pool workloads.

**Gate:** Standalone and substrate-backed Gres each pass 231 / 231 serially and in parallel, without flakes across three certification runs.

## Task 7: Certify and simplify

- [ ] Empty and delete `pg-regress-baseline.json`.
- [ ] Change the published compatibility headline to the upstream passed/total result.
- [ ] Keep the hand-written and extended-protocol corpora for focused coverage, but remove the duplicate adopted-corpus headline and any obsolete runner code.
- [ ] Record the PostgreSQL tag, archive hash, commands, artifacts, and three clean run IDs in a dated evidence document.

**Final gate:** PostgreSQL self-check, standalone Gres, and substrate-backed Gres are all 231 / 231 in serial and parallel modes; all existing hand-written, extended-protocol, sharded, and PgDog gates remain green.
