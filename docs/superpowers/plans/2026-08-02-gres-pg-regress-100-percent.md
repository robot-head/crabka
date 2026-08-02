# Gres pg_regress 100% Compatibility Plan

**Goal:** Make the unmodified PostgreSQL 18.4 core regression schedule pass against Gres, replacing the partial adopted-corpus percentage with a literal upstream `pg_regress` result.

**Current state:** The adopted 50-file corpus matches 9323 / 14272 statements (65.3%). PostgreSQL 18.4's core schedule contains 231 test files, so the adopted percentage remains a development signal rather than the final compatibility claim. The checked-in monotone baseline remains the first deterministic run: 6 / 231 tests (`comments`, `infinite_recurse`, `collate.icu.utf8`, `psql_crosstab`, `collate.linux.utf8`, and `collate.windows.win1252`) with 225 semantic failures, 172573 changed diff lines across 4322 hunks. The latest infrastructure-clean serial review run passes 7 / 231 by additionally passing `portals_p2`, leaving 224 semantic failures. It improves 26 recorded failures and removes one, but server-side file COPY also makes official fixture rows visible to downstream tests: five tests have larger diffs and four have equal-sized changed fingerprints, for 185983 changed lines across 4377 hunks. That run is evidence of progress, not a legal ratchet replacement; the larger and changed surfaces must be resolved before updating the checked-in baseline. Both PostgreSQL self-check modes pass, Gres completes the whole schedule with zero infrastructure failures, and the pinned `REL_18_4` schedule fingerprint is `63419f82d4a5faaf711658608a3b7b6b45ccc5c2a64e1b4e5c111ed9de648118`.

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

- [ ] Make `test_setup` and PostgreSQL's shared data files load before dependent tests.
- [x] Implement server-side `COPY table FROM 'file'` through the same atomic text-import path as wire `COPY FROM STDIN`; missing files report `58P01`.
- [ ] Match complete wire-visible outcomes needed by `psql`: notices, warnings, diagnostics, command tags, row counts, headings, and COPY state transitions.
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
- [ ] Resolve the fixture-enabled baseline changes before ratcheting: worsened `join`, `groupingsets`, `misc`, `tidscan`, and `cluster`; changed equal-size fingerprints for `copy`, `copyencoding`, `insert`, and `create_view`.
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
