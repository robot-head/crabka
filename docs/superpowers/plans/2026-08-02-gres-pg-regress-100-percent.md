# Gres pg_regress 100% Compatibility Plan

**Goal:** Make the unmodified PostgreSQL 18.4 core regression schedule pass against Gres, replacing the partial adopted-corpus percentage with a literal upstream `pg_regress` result.

**Current state:** The adopted 50-file corpus matches 9323 / 14272 statements (65.3%). PostgreSQL 18.4's core schedule contains 231 test files, so the adopted percentage remains a development signal rather than the final compatibility claim.

**Architecture:** Use PostgreSQL's own `pg_regress`, `psql`, schedule, SQL, data, expected output, and `resultmap` as the authority. Keep `crabka-gres-conformance` as the fast statement-level diagnostic tool while mismatches remain; do not grow it into a second implementation of `pg_regress`.

**Completion contract:** A pass uses the pinned `REL_18_4` corpus without edited SQL, Gres-specific expected files, ignored tests, or result normalizers beyond PostgreSQL's own `resultmap`. The final result is 231 / 231 in serial and parallel modes, with no crash, timeout, or connection loss.

## Task 1: Add the authoritative upstream runner

**Files:** Create `scripts/gres-pg-regress.sh`; modify `.github/workflows/ci.yml`; update `crates/gres-conformance/README.md`.

- [ ] Fetch the `REL_18_4` source archive into `target/` and verify a pinned SHA-256 before extracting `src/test/regress`.
- [ ] Build the matching PostgreSQL 18.4 `pg_regress`, `psql`, and `src/test/regress/regress` shared library from that verified source; export its directory and platform suffix through `PG_LIBDIR` and `PG_DLSUFFIX` so tests using `:regresslib` load the pinned module.
- [ ] Start a fixed-locale PostgreSQL oracle from an exact `postgres:18.4` image digest, fail unless `SELECT version()` reports 18.4, start a clean Gres endpoint, then run PostgreSQL against itself as a harness self-check.
- [ ] Run Gres with the official `--use-existing --dbname=crab` path, retaining `regression.diffs`, per-test output, server logs, and exit status under one artifact directory.
- [ ] Prove the self-check passes both the serial and parallel schedules before using the runner's Gres result as evidence.

**Gate:** PostgreSQL against itself is 231 / 231; an intentionally changed expected file makes the wrapper fail.

## Task 2: Establish a monotone progressive gate

**Files:** Create `crates/gres-conformance/pg-regress-baseline.json`; modify `scripts/gres-pg-regress.sh` and `.github/workflows/ci.yml`.

- [ ] Seed one entry per failing test from the first clean upstream run, recording the test name, mismatched unified-diff hunk/line count, and SHA-256 of that test's diff; do not permit wildcards or reason-free categories.
- [ ] Fail CI when a passing test regresses, an upstream scheduled test disappears, a known failure starts passing without removal, its mismatch count increases, or its diff fingerprint changes without a reviewed baseline update.
- [ ] Permit a baseline update only when the owning test's mismatch count decreases or the test is removed; the changed fingerprint and supporting result artifact land in the same review.
- [ ] Keep the existing per-statement baseline for local root-cause ranking, but label it adopted-corpus parity everywhere.
- [ ] Publish upstream passed/total and the shrinking per-test mismatch baseline in the job summary.

**Gate:** The wrapper distinguishes new, removed, improved, worsened, and same-count-but-different failures; the checked-in mismatch surface can only shrink.

## Task 3: Remove harness and setup blockers

**Files:** Primarily `crates/pgwire`, `crates/pgexec`, `crates/pgcatalog`, and `crates/gres-conformance`; exact files follow each minimized failure.

- [ ] Make `test_setup` and PostgreSQL's shared data files load before dependent tests.
- [ ] Match complete wire-visible outcomes needed by `psql`: notices, warnings, diagnostics, command tags, row counts, headings, and COPY state transitions.
- [ ] Eliminate nondeterministic unordered results rather than weakening comparisons.
- [ ] Treat every crash, I/O loss, or timeout as a harness failure, never as an SQL mismatch or a match on two dead connections.
- [ ] Re-run the full serial schedule after each shared fix and ratchet only tests whose recorded mismatch surface shrank.

**Gate:** Every remaining failure reproduces as an engine semantic difference; infrastructure failures are zero.

## Task 4: Burn down the measured semantic roots

For each item, first add one focused test at the shared layer that fails before the fix, implement the smallest shared correction, run the owning upstream file, then run the complete serial schedule and ratchet both compatibility ledgers.

- [ ] Split the 1089 wrong-row mismatches by statement family; fix the largest coherent family first rather than treating it as one issue.
- [ ] Fix the earliest error in each transaction-abort cascade before touching its downstream `25P02` statements.
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
