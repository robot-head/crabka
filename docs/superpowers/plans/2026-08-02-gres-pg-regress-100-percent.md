# Gres pg_regress 100% Compatibility Plan

**Goal:** Make the unmodified PostgreSQL 18.4 core regression schedule pass against Gres, replacing the partial adopted-corpus percentage with a literal upstream `pg_regress` result.

**Current state:** The authoritative serial result is `24/231` whole upstream
test files, not the adopted corpus's statement matches. The checked-in monotone
floor remains `6/231`; this review is still non-monotone against that floor and
does not ratchet it. Serial completes all 231 files with zero infrastructure
failures, leaving 207 semantic failures across 175682 canonical changed lines
and 4582 hunks. Both PostgreSQL self-check modes pass 231/231, the Gres
postflight probe succeeds, and the infrastructure report is empty.

The measurement immediately before this wave was `22/231` at 176686 changed
lines / 4606 hunks, from the same runner and the same pinned corpus. The
waves below are therefore `+2` exact files and `-1004` changed lines with
**zero newly failing files**: `int4` and `int2` become exact, 37 files improve,
and 4 gain a combined 22 lines.

Those 4 — `timestamptz` +12, `horology` +6, `alter_table` +2, `timestamp` +2 —
are all one benign category: a caret correctly attached to an error Gres should
not be raising at all. `insert into child values (12, 13, 'testing')` and
`'Jan 01 00:00:00 1000 LMT'::timestamptz` both *succeed* in PostgreSQL under the
schedule's settings, so Gres's pre-existing wrong error now carries two more
correct lines. Fixing the underlying rejection removes the error and its caret
together; no over-attachment remains. (Beware isolated re-checks of the `LMT`
cases: they depend on the run's `TimeZone`, and a bare `psql` session reproduces
neither PostgreSQL's success nor its error.)

`int4` and `int2` join the 22 previously exact files (`test_setup`, `boolean`, `varchar`,
`md5`, `comments`, `mvcc`, `euc_kr`, `create_function_c`, `infinite_recurse`,
`delete`, `security_label`, `async`, `dbsize`, `collate.icu.utf8`,
`psql_crosstab`, `collate.linux.utf8`, `collate.windows.win1252`,
`vacuum_parallel`, `portals_p2`, `bitmapops`, `numa`, `compression_pglz`).
Parallel mode has not been re-measured since this wave.

The branch now includes PL/pgSQL, triggers, foreign keys, real schema
namespaces, and full-text-search surfaces. Their serial owner diffs are
`create_schema` 54/1, `triggers` 1272/88, `foreign_key` 1515/78,
`tsearch` 1510/73, `tsdicts` 741/8, `tstypes` 480/23, and `plpgsql`
2047/173 changed lines/hunks. This wave also adds bounded two-branch `OR`
counts, `OPERATOR(...)` expression parsing, anonymous-record OID 2249
plumbing, native `jsonpath`/`jsonpath[]` plumbing and domain enforcement,
PostgreSQL ALTER ordering and dependency fixes, and nonfatal legacy Fastpath
rejection. The JSONPath family remains `0/3` at 3110/99: `jsonpath` 911/21,
`jsonpath_encoding` 131/1, and `jsonb_jsonpath` 2068/77. Native type identity
is implemented; grammar, canonicalization, and evaluator compatibility remain.
`largeobject` remains 468/5, but Fastpath rejection no longer loses the
connection. The pinned `REL_18_4` schedule fingerprint is
`63419f82d4a5faaf711658608a3b7b6b45ccc5c2a64e1b4e5c111ed9de648118`.

The complete artifact certifies the focused `boolean` and `varchar` gains.
`sanity_check` was exact in isolation, but the full schedule leaves it at 5
changed lines / 1 hunk with a catalog-query error after preceding schedule
state; that order-sensitive residual remains open. A prior measurement
completed serial at 20 / 231 but its parallel mode stalled
at 92 / 231, so `target/pg-regress-runs/20260803T213156Z-exists-projection-certified-gres`
is explicitly non-certifying. A retained non-certifying replay at
`target/pg-regress-runs/20260803T221236Z-parallel-stall-repro-gres` completed
that cohort and reproduced a CPU-heavy OR nested loop: `join` took 294.320
seconds and its final OR query exceeded the blocking-query memory budget. The
replay did not reproduce a lock cycle. This classifies the replayed hotspot,
not the exact historical blocker. The current full parallel schedule clears
that point and completes all 231 files.

The current certification also makes `create_function_c`, `delete`,
`security_label`, `dbsize`, `vacuum_parallel`, `numa`, and `compression_pglz`
exact. `dbsize` now covers exact size parsing/formatting and physical local
secondary-index key/value bytes; heap, TOAST, PostgreSQL page and auxiliary-fork
storage, and database/tablespace totals remain zero, and cluster-size names/OIDs
are not validated. The metadata-gated PGLZ decompressor deliberately caps its
declared output at 64 MiB and returns `54000` above that bound.

**Review history:** The checked task entries below retain point-in-time
implementation and artifact evidence. Only the current-state paragraphs above
and their certified artifact describe current conformance.

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
- [x] Parse the serial baseline from the runner's retained `command.log`; accept a missing `regression.diffs` only when the complete TAP stream reports every scheduled test passing.

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
- [ ] Complete the setup surfaces still present in the latest artifact: tablespace bulk/default/constraint/partition placement and REINDEX, plus residual multirange behavior. Operator-class/family lifecycle is no longer a generic setup blocker.
- [x] Validate and canonicalize literals for staged user-defined range types, including infinite bounds, quoted/escaped bounds, malformed delimiters, subtype casts, ordering, and empty-range normalization; the focused upstream `rangetypes` diff shrinks from 1720 lines / 15 hunks to 1396 / 12.
- [x] Give built-in and user-defined ranges a distinct `ColumnType` identity; resolve PostgreSQL's six built-in range OIDs/subtypes, serialize them, expose their `pg_type` rows, and canonicalize discrete bounds. The focused diff falls again to 1163 changed lines / 20 hunks.
- [x] Add typed range datums, durable storage, text/binary wire encoding, six built-in constructors, bound accessors, btree comparison, containment, and overlap. The focused diff falls to 852 changed lines / 38 hunks.
- [x] Add directional, adjacency, union, intersection, difference, and merge operators/functions; add `range_intersect_agg`, registry-backed user-defined constructors, and PostgreSQL boolean GUC prefixes. The focused diff falls to 463 changed lines / 23 hunks.
- [x] Implement `pg_input_error_info` through the shared cast validator, including structured range-literal diagnostics and valid-input null fields. The focused diff falls to 443 changed lines / 19 hunks.
- [x] Preserve range parser DETAIL fields through the shared type-error and wire-error path. The focused diff falls to 434 changed lines / 19 hunks; the remaining lines in that block are expression positions, not parser diagnostics.
- [x] Add built-in range arrays through element resolution, literal casts, durable schema/row storage, catalog OIDs, and text/binary wire encoding. The focused diff falls to 423 changed lines / 19 hunks; the remaining array-adjacent failures belong to polymorphic routine resolution.
- [x] Apply startup-packet GUCs and shell-quoted `PGOPTIONS` before login hooks, committing them as the rollback-safe session baseline. `pg_regress` now preserves `Postgres, MDY`, `America/Los_Angeles`, and `postgres_verbose` after expected errors; the focused diff falls to 404 changed lines / 19 hunks.
- [x] Resolve traditional and compatible polymorphic range signatures against their array/element subtype, enforce the range-result input contract, accept one-argument functional range casts, and inline SQL routines whose repeated arguments are immutable range constructors. The focused diff falls to 337 changed lines / 22 hunks; the complete serial schedule remains 8 / 231 while shrinking to 183115 changed lines.
- [x] Preserve discrete date infinity endpoints during range canonicalization and persist hash/GiST/SP-GiST access-method metadata without manufacturing unusable physical entries. The focused diff falls to 320 changed lines / 18 hunks; the complete serial schedule remains 8 / 231 while shrinking to 183065 changed lines / 4487 hunks.
- [ ] Complete the residual multirange semantics shown by the latest diff. The earlier list of missing companion types, coercions, directional operators, `unnest`, support functions, and polymorphic resolution is implemented and must not remain described as pending; re-triage the current mismatches before naming the next shared root.
- [x] Derive and hydrate the automatic user-range multirange companion from the reserved oid slot, allow range-to-multirange casts, resolve unknown containment literals to the subtype, cover every range/multirange directional pairing, and extend the existing `unnest` SRF. Focused `multirangetypes` falls from 2471 changed lines / 57 hunks to 1496 / 44; the complete serial review remains 8 / 231 with zero infrastructure failures and shrinks to 260633 changed lines / 4529 hunks.
- [x] Add multirange union, intersection, split-preserving difference, `range_merge(multirange)`, and the nine named overlap/containment support functions through the shared operation and scalar-function paths. Focused `multirangetypes` falls to 1053 changed lines / 36 hunks; the complete serial review remains 8 / 231 with zero infrastructure failures and shrinks to 260190 changed lines / 4521 hunks.
- [x] Add `range_agg` for range and multirange inputs and extend `range_intersect_agg` to multiranges through the existing canonical algebra. Focused `multirangetypes` falls to 874 changed lines / 34 hunks; the complete serial review remains 8 / 231 with 223 semantic failures and zero infrastructure failures and shrinks to 260012 changed lines / 4519 hunks.
- [x] Complete traditional and compatible polymorphic multirange argument/result resolution, generic `multirange(anyrange)`, bound accessors, overload selection, SQL output inference, and PL/pgSQL positional binding. Focused `multirangetypes` falls to 725 changed lines / 41 hunks; the complete serial review remains 8 / 231 with zero infrastructure failures and shrinks to 259629 changed lines / 4544 hunks.
- [x] Preserve explicit multirange companion names through parsing, durable range metadata, hydration, schema-relative naming, shared namespace collisions, and exact named-constructor range identity. Focused `multirangetypes` falls to 706 changed lines / 41 hunks; the complete serial review remains 8 / 231 with zero infrastructure failures and shrinks to 259608 changed lines / 4544 hunks.
- [x] Add built-in and user-defined multirange arrays through native element identity, text casts, durable storage codes, wire/parameter OIDs, and built-in catalog rows. The upstream arrays-of-multiranges block matches exactly and focused `multirangetypes` falls to 686 changed lines / 41 hunks; the complete serial diff falls to 246540 / 4476, while a run-specific `test_setup` memory-budget error leaves this run at 7 / 231 pending recheck.
- [x] Cascade dropped user types through transitive composite/range/domain dependencies and route `row_to_json` through native record conversion with compact JSON output. The composite range blocks now execute without stale-name collisions; focused `rangetypes` falls to 447 / 18, `multirangetypes` to 663 / 41, and `json` to 2498 / 35, while the complete serial diff falls to 246491 / 4476.
- [x] Add `ALTER TYPE … ADD ATTRIBUTE` and traverse the complete user-type graph for composite self-inclusion. Focused `rangetypes` falls to 444 / 18, `multirangetypes` to 660 / 41, and `alter_table` to 5223 / 107, while the complete serial diff falls to 246480 / 4476.
- [x] Resolve range operators nominally before falling back through domain storage types, preserving domain subtypes while enabling domains over multiranges and their cast-time constraints. Focused `multirangetypes` falls to 646 / 41 and the complete serial diff falls to 246452 / 4476.
- [x] Resolve `varbit` through ordered variable-width storage for user ranges and their automatic multiranges, and require range/multirange adjacency at an external component boundary. Both upstream `varbit` blocks and all five adjacency counts now match; raw `rangetypes` falls to 390 / 15 and `multirangetypes` to 617 / 39.
- [x] Implement GiST exclusion constraints with durable operator metadata, catalog visibility, create/alter back-validation, transaction-safe insert/update enforcement, same-statement conflict detection, and PostgreSQL-compatible `23P01` diagnostics. The upstream exclusion block is exact; the final complete serial artifact is 7 / 231 and 164801 / 4478, with focused `rangetypes` at 169 / 14.
- [x] Persist and validate immutable GiST/SP-GiST expression keys through the existing index catalog, exact-scan execution, `pg_get_indexdef`, and column rename/drop dependencies. The upstream range expression index executes; focused `rangetypes` improves to 168 / 14 and the complete serial artifact is 164815 / 4476.
- [x] Attach runtime range-literal cast positions only when lexer evidence proves one unique direct range cast. The complete ten-error range-input caret hunk is exact; focused `rangetypes` improves to 146 / 12 and the complete serial artifact to 164791 / 4474.
- [x] Attach undefined-function positions only when lexer evidence proves one unique matching call. The three remaining range polymorphism carets are exact; focused `rangetypes` improves to 140 / 12. The complete run remains 7 / 231 and 4473 hunks, while correct diagnostics on fallback errors expand the temporary line count to 167435.
- [ ] Complete tablespace lifecycle. Durable metadata, relation/index/CTAS placement, moves, `pg_class` visibility, rename/drop cleanup, and dependency-safe drop are implemented. Bulk moves, default and constraint-index placement, partition-index attachment, and REINDEX remain. Focused `tablespace` is 778 / 6 and the complete serial artifact is 7 / 231 at 167408 / 4473.
- [x] Complete operator class/family lifecycle. Durable create metadata, implicit family creation, OID linkage, `pg_opclass`/`pg_opfamily` visibility, rename/owner/schema changes, dependency-safe drops, cascade cleanup, `IF EXISTS` notices, membership-aware ownership checks, the complete upstream ADD/DROP member block, built-in families, ordering-family OID resolution, and built-in plus durable user `pg_amop`/`pg_amproc` rows are implemented. The latest artifact stays 7 / 231 at 170869 / 4521; focused `alter_generic` is 328 / 11 and `opr_sanity` 5320 / 80. Resolve the now-visible built-in sanity mismatches next.
- [x] Give `oidvector`, `regtype`, and `regprocedure` native catalog identities. `pg_proc.proargtypes` now has zero-based array semantics and exact wire text, registry-aware type/routine casts render canonical identities, and the `oidvector` to `regtype[]` path resolves every element. The complete serial and parallel schedules remain infrastructure-clean at 7 / 231; `opr_sanity` is 1667 / 69 with deeper catalog gaps exposed.
- [x] Execute the upstream `binary_coercible` helper through PostgreSQL's built-in identity and implicit binary-relabel relation, and populate nonempty `pg_proc.probin` for C-language routines. The complete serial artifact remains infrastructure-clean at 7 / 231 while `opr_sanity` improves to 1575 / 68 and the aggregate to 167183 / 4514.
- [x] Expose all 235 pinned PostgreSQL 18.4 built-in casts through `pg_cast` and reuse the same relation for `binary_coercible`. The complete serial artifact remains infrastructure-clean at 7 / 231 while `opr_sanity` improves to 1550 / 67 and the aggregate to 167159 / 4513.
- [x] Expose all 161 pinned PostgreSQL 18.4 built-in aggregates through `pg_aggregate`. The complete serial artifact remains infrastructure-clean at 7 / 231 while `opr_sanity` improves to 1505 / 64 and the aggregate to 167115 / 4510.
- [x] Expose all 128 pinned PostgreSQL 18.4 built-in conversions, PostgreSQL encoding identities, ASCII/default-pair conversion, and native bytea input. Correct `pg_proc.prosupport` to OID identity so its sanity self-join remains indexed. The complete serial artifact is infrastructure-clean at 8 / 231; `opr_sanity` improves to 1490 / 62 and the aggregate to 166997 / 4509.
- [x] Preserve exact pinned `pg_proc.prosrc` for all 3397 built-ins with reproducible generation, complete hex-bytea whitespace input, and preserve non-ASCII bytes for same-ID and SQL_ASCII-destination conversions. Serial and parallel complete all 231 files with zero infrastructure failures and remain 8 / 231; the serial `opr_sanity` improves to 461 / 60 and the aggregate to 165962 / 4507.
- [x] Route only the exact regression C `binary_coercible` helper to the native implementation and propagate configured memory through write-fed scans. Serial and parallel remain infrastructure-clean at 8 / 231; `opr_sanity` improves to 441 / 56 with 17 memory fallbacks and the aggregate to 165942 / 4503. Raising the runner to 20MiB makes `test_setup` exact but exposes a parallel deadlock, which remains before that budget can become the certified default.
- [x] Finish exact `pg_proc` argument metadata and remove the parallel resource cycle. All three built-in arrays and user `RETURNS TABLE` names are exact; relation-scoped unique-index gates retain same-table backfill exclusion without cross-table convoying. Serial and parallel are infrastructure-clean at 8 / 231; serial is 154984 / 4501, parallel is 155220 / 4502, and `opr_sanity` is 331 / 54.
- [x] Replace the transaction-local unique-index `0A000` fallback with atomic relation upgrades in the shared row/key/relation deadlock graph. Earlier uncommitted writes remain protected against prequeued backfills, crossed A/B and row/gate cycles return one `40P01`, exclusive ownership lasts through transaction end, and savepoint rollback restores the earlier lock set.
- [x] Adopt the explicit 20 MiB blocking-query memory policy after the atomic gate fix. `test_setup` is exact; serial and parallel both complete all 231 tests with zero infrastructure failures at 9 / 231. Serial is 169365 / 4554 and parallel is 170119 / 4555. Certified artifact: `target/pg-regress-runs/20260803T064530Z-3296344-gres`.
- [x] Add the zero-queue `pg_notification_queue_usage()` surface, accept unreserved `DATA` as a column name and index key, decode strict `convert_from(..., 'EUC_KR')` aliases, preserve the `POSITION` output label, and preserve plus validate operator classes on plain index keys. `mvcc`, `euc_kr`, and `async` become exact. Serial and parallel are infrastructure-clean at 12 / 231; serial is 169230 / 4552 and parallel is 169647 / 4552. Certified artifact: `target/pg-regress-runs/20260803T091747Z-6393-gres`.
- [x] Bound scalar `count(*)` over exactly two strict, pushdown-safe top-level `OR` branches with three scans and inclusion-exclusion. `bitmapops` becomes the thirteenth exact upstream file.
- [x] Parse `OPERATOR([pg_catalog.]symbol)` in prefix, infix, and `ANY`/`ALL`/`SOME` expression positions with PostgreSQL generic-operator precedence. Operator-definition DDL remains a separate compatibility surface.
- [x] Preserve anonymous `record` as `ColumnType::Record(None)` through durable schema serialization and map query-field OID 2249 back to that type. This does not complete named composite or record-returning-function semantics.
- [x] Add native `jsonpath` and `jsonpath[]` identity with PostgreSQL OIDs 4072 and 4073, scalar and array datums, durable schema/default/row storage, text/binary parameters and results, `pg_type` rows, common-type and assignment coercion, routine/PL/pgSQL use, and PostgreSQL-compatible rejection gates for equality, ordering, hashing, and default operator classes. The upstream family remains `0/3` at 3110 changed lines / 99 hunks because language and evaluator compatibility remain incomplete.
- [x] Enforce `jsonpath` and `jsonpath[]` domain constraints after explicit casts, assignments, omitted/default values, COPY input, and PL/pgSQL declarations, arguments, assignments, and returns.
- [x] Run ALTER actions in PostgreSQL pass order, retain expression-index dependencies during column drops, and apply the shared default-operator-class checks to ALTER-added unique and primary-key constraints.
- [x] Revalidate an `ALTER TABLE ... ADD PRIMARY KEY/UNIQUE` target after taking the catalog gate, so a concurrent relation replacement cannot bypass PostgreSQL's view diagnostic or build an index for a stale table identity.
- [x] Fully consume legacy frontend Fastpath (`F`) messages and reject them nonfatally with `0A000`, or `25P02` in a failed transaction, while preserving extended-protocol ignore-until-Sync behavior. `largeobject` remains 468 / 5, but `psql \lo_unlink` no longer terminates the connection; legacy functions are not executed.
- [x] Re-run the upstream owners for the implemented PL/pgSQL, trigger, foreign-key, schema, and full-text-search surfaces. Serial results are `create_schema` 54/1, `triggers` 1272/88, `foreign_key` 1515/78, `tsearch` 1510/73, `tsdicts` 741/8, `tstypes` 480/23, and `plpgsql` 2047/173 changed lines/hunks.
- [x] Match PostgreSQL's hidden-target DELETE diagnostic for the uniquely provable outer alias case, including `42P01`, alias hint, and source position. `delete` becomes exact; broader DML alias diagnostics remain pending.
- [x] Admit non-unique local B-tree expression indexes through the existing catalog-only GiST/SP-GiST path. These indexes retain definitions and dependencies but deliberately have no physical entries or expression evaluator; unique, partial, and executable expression indexes remain pending.
- [x] Implement exact `pg_size_bytes(text)` parsing/diagnostics and PostgreSQL's bigint/numeric `pg_size_pretty` overloads, including values above `int8` and symmetric negative rounding. Count physical local-secondary-index main-fork key/value bytes through split catalog/data engines; `pg_indexes_size` sums a table's indexes and `pg_total_relation_size` includes them. Heap, TOAST, PostgreSQL page and auxiliary-fork storage remain zero. `pg_database_size` and `pg_tablespace_size` return zero for non-NULL inputs without validating names/OIDs. `dbsize` becomes exact.
- [x] Add the platform-independent NUMA fallback (`pg_numa_available() = false` and the exact unsupported `pg_shmem_allocations_numa` surface). `vacuum_parallel` and `numa` become exact.
- [x] Retain SECURITY LABEL's regression TABLE/ROLE target forms and return the exact no-provider/named-provider `22023` errors before target resolution. Provider registration and label persistence remain pending; `security_label` becomes exact.
- [x] Preserve C routine object files separately from link symbols, validate the explicitly configured static regression module and pinned internal symbols before catalog writes, and project `pg_proc.prosrc`/`probin` plus `pg_get_functiondef` correctly. Arbitrary server files are never read or executed, and general C execution remains pending; `create_function_c` becomes exact.
- [x] Execute only the metadata-gated regression `test_pglz_compress`/`test_pglz_decompress` signatures through a bounded safe-Rust PGLZ codec and add the exact `length(bytea)` overload. The decompressor deliberately caps declared output at 64 MiB and returns `54000` above it; `compression_pglz` is exact within that safety bound, not a general C ABI.
- [x] Encode PostgreSQL's optional one-based `P` source-position field without changing errors that do not carry a known position. Full-suite review rejected unconditional parser attachment because unsupported valid SQL would gain additional wrong output.
- [x] Match PostgreSQL boolean input ambiguity and canonical derived type labels, and attach a source position only to the exact legacy `bool 'literal'` form. The untouched PostgreSQL 18.4 `boolean` file is exact in `target/pg-regress-runs/20260803T220700Z-focused-boolean-current`.
- [x] Apply scalar-to-`varchar(n)`/`char(n)` assignment coercion through the shared cast path and preserve the target typmod in truncation and `pg_input_error_info` diagnostics. The untouched `varchar` file is exact in `target/pg-regress-runs/20260803T222100Z-focused-varchar-current`.
- [x] Expose PostgreSQL 18.4's 31 pinned catalog OID indexes consistently through `pg_class`, `pg_attribute`, and `pg_index`, including uniqueness/immediacy and index-key invariants. The untouched `sanity_check` file is exact in `target/pg-regress-runs/20260803T222000Z-focused-sanity-current`; the authoritative full schedule still leaves an order-sensitive 5-line / 1-hunk catalog-query residual, so focused success is not file-level certification.
- [x] Box recursively re-entered SELECT/function futures in PL/pgSQL and cap parser recursion below the default 2 MiB thread-stack limit while retaining the explicit twenty-level acceptance floor. Focused PL/pgSQL, parser, and recursion-guard suites pass without a process abort.
- [x] Implement PostgreSQL's real integer input grammar for `int2`/`int4`/`int8`:
      the `0x`/`0o`/`0b` base prefixes and `_` digit separators it has accepted
      since 16, with each separator required to precede a digit of that base so
      `1__0`, `100_`, `_100`, and `0x__1` stay 22P02 while `1_0` and `0x_10` are
      values. The magnitude accumulates negatively so `-0x8000000000000000`
      reaches `i64::MIN`. A well-formed but too-wide value is now 22003
      `value "…" is out of range for type <t>` for all three widths — `int4` and
      `int8` previously fell back to the bare arithmetic message — and integer
      arithmetic overflow names its own width (`smallint`/`integer`/`bigint`)
      rather than always saying `integer`.
- [x] Attach PostgreSQL's source position to type-input failures generally,
      replacing the `bool`-only attacher. A rejected value is positioned when
      exactly one string literal in the statement carries it *and* any type the
      source states next to that literal is the type that rejected it, so
      `int2 '34.5'`, `'34.5'::int2`, `CAST('zz' AS int4)`, and a `VALUES` item
      coerced to its column all get a caret, while a value coerced through an
      intermediate type (`'  tru e '::text::boolean`) or computed rather than
      written (`('12'||'x')::int4`) correctly gets none — the latter two are
      raised at execution time, where PostgreSQL has no parse location either.
      Covers 22P02, 22003, and 22007, each of which names its type in the
      message; a repeated literal stays undecorated rather than guessed. Every
      case was checked byte-for-byte against a PostgreSQL 18.4 oracle, including
      a sweep over `date`, `uuid`, `numeric`, `interval`, `time`, `timestamp`,
      arrays, and floats confirming Gres never positions an error PostgreSQL
      leaves bare.
- [x] Position type-input failures raised by the datetime family (SQLSTATE
      22007), and exclude *function arguments* from positioning entirely. A
      literal passed to a function reaches it as an already-typed value and the
      function raises its own error at execution time —
      `to_timestamp('97/Feb/16', 'YYMonDD')` is `invalid value "/Feb/16" for
      "Mon"` with no position — so the attacher walks back to the parenthesis
      opening the literal's enclosing argument list and declines when it follows
      an identifier. A `VALUES` row is not a call: its parenthesis follows the
      `VALUES` keyword, and PostgreSQL does position each item's coercion to its
      target column. A cast written on an argument still binds tighter than the
      call, so `length(upper('zz'::interval::text))` keeps its caret — and the
      `CAST('x' AS type)` spelling is recognised only when the literal sits
      directly inside `CAST(`, because otherwise the *column alias* in
      `SELECT bool 'test' AS error` reads as a target type named `error` and
      suppresses a caret PostgreSQL does emit.
      The complete schedule is what exposed both halves, and neither showed up
      in targeted oracle probes: attaching to arguments cost `horology` 54
      lines, `timestamptz` 12 and `timestamp` 2, and the alias misreading cost
      the whole `boolean` file its exactness (20 lines). Treat "was this literal
      coerced directly?" as a question only the full corpus can answer.
- [x] Accept PostgreSQL's optional `TABLE` object-type keyword in
      `GRANT`/`REVOKE`, so a bare relation name after `ON` names a table. 334
      statements across 34 upstream files use that spelling, which was
      previously a syntax error. `SCHEMA` still requires its keyword.
- [x] Report `float8` text input overflow *and* underflow as 22003
      `"…" is out of range for type double precision`, matching the existing
      `float4` handling; overflow previously surfaced as the bare arithmetic
      `integer out of range` and underflow silently returned zero.
- [x] Accept the `TABLE t` query form as a derived table (`FROM (TABLE t) AS s`).
      `set_primary` already parsed `TABLE t` as a query body; only
      `table_factor`'s post-parenthesis check omitted it.
- [ ] Make `int2` exact. The `(TABLE …)` half is done; the rest is
      `int2vector`, which mirrors the existing `OidVector` across roughly 25
      registration sites (`ColumnType`/`Datum` variants, OID 22, element type,
      text input, wire encoding, `pg_type` row).
- [x] Implement `ALTER ROLE` and persist role attributes. `CREATE ROLE`
      previously swallowed its attribute list with a bare `bump()`, so
      `SUPERUSER`/`CREATEDB`/… were parsed and discarded, and `ALTER ROLE` did
      not parse at all while `pg_authid`/`pg_roles` hardcoded every attribute.
      A shared option list now serves `CREATE ROLE`/`CREATE USER` and
      `ALTER ROLE`, carrying `Option<bool>` so only written options apply and
      the rest keep their stored value; the seven booleans persist as a one-byte
      bitset on the role record and both catalogs project them. Verified
      byte-identical to the oracle on a clean cluster. `ALTER USER` stays the
      `ALTER USER MAPPING` spelling; `PASSWORD`, `VALID UNTIL`,
      `CONNECTION LIMIT` and role-level GUCs remain unmodelled, and the
      attributes do not yet gate authorization.
- [ ] Match remaining exact wire-visible diagnostics.
- [ ] Attach `22008 date/time field value out of range` and `malformed array
      literal` source positions. Neither message names its type, so the
      "literal was coerced directly to the type that rejected it" evidence the
      22P02/22003/22007 attacher relies on is unavailable; these need a
      separate rule that instead rejects a literal opening a cast *chain*.
- [ ] Emit `HINT: Perhaps you need a different "DateStyle" setting.` Measured
      against the oracle, PostgreSQL attaches it only to *month/day* field
      overflow (`'2024-13-01'::date`), not to other datetime range errors
      (`'2024-02-30'::date`, `'25:00:00'::time`) — it is PostgreSQL's distinct
      `DTERR_MD_FIELD_OVERFLOW`, so the datetime parser must separate that case
      before the hint can be added. 30 occurrences in the expected files.
- [ ] Implement PostgreSQL's quoted `"char"` type (OID 18, one byte). It owns
      the whole 36-line `char` residual, which is otherwise exact. Semantics
      measured against the oracle: input takes the first byte, decoding a
      `\nnn` octal escape (`'\101'` is `A`); output renders byte 0 as the empty
      string and a non-printable byte as `\nnn` (`'\377'` round-trips as the
      4-character text `\377`); `int4` converts both ways (`65` ↔ `A`);
      `pg_typeof` prints it quoted.
      **Blocker:** `crabka_pgparser::lexer` strips the quotes from a quoted
      identifier and emits the same `Token::Ident` an unquoted one produces, so
      `"char"` and `char` are indistinguishable and cannot resolve to different
      types. This needs quoted-identifier provenance on the token (and on type
      names in the AST) before the type itself is worth adding — a
      cross-cutting parser change, not a type-registration change.
- [ ] Give `oid` its own type identity. `crabka_pgtypes::datum` resolves `oid`
      to `ColumnType::Int4`, so every `oid` input failure reports `invalid input
      syntax for type integer`; the upstream `oid` file expects `... for type
      oid`, which is roughly half that file's residual. Its source positions are
      already correct.
- [ ] Finish source-aware `bpchar` coercion. PostgreSQL treats a `bpchar`'s
      trailing blanks as insignificant on *every* conversion out of the type —
      `c::text`, `c::varchar`, `length(c)`, `lower(c)`/`upper(c)`, and `||` all
      strip them — so the fix belongs at the shared bpchar-to-text coercion, not
      at the explicit cast alone. Owns the padded-output residuals in
      `select_having`, `select_implicit`, and `char`.
- [ ] Eliminate nondeterministic unordered results rather than weakening comparisons.
- [x] Treat every crash, I/O loss, or timeout as a harness failure, never as an SQL mismatch or a match on two dead connections.
- [ ] Bound the postflight probe by a query timeout, not just a connect
      timeout. `probe_gres` passes `PGCONNECT_TIMEOUT=5`, which limits only the
      TCP/startup phase; when a schedule run is killed at
      `GRES_PG_REGRESS_TIMEOUT` and leaves the server wedged, the probe's
      `SELECT 1` blocks forever and the wrapper hangs instead of writing
      `postflight-failed` and returning. Observed directly: a run killed at
      exit-status 124 after 193/231 left `psql --command='SELECT 1'` blocked for
      over two hours against a server whose log showed no panic and no I/O
      error. Add `-v STATEMENT_TIMEOUT` / a `timeout` wrapper so a wedged server
      is reported as the infrastructure failure it is.
- [ ] Investigate why a `SIGTERM` of `pg_regress` mid-schedule can leave the
      server unable to answer a new `SELECT 1`. This was observed under heavy
      concurrent CPU load and after the volume had repeatedly hit 100%, so it is
      not yet isolated from those, but a client disconnect must never wedge the
      server.
- [x] Reject positional parameter numbers outside PostgreSQL's signed-32-bit lexer range before allocating parameter-shape vectors; the upstream `numerology` case now returns `42601` rather than consuming unbounded CPU and memory.
- [x] Keep regress-scale lateral derived joins bounded by caching only conservative, nonvolatile specializations (including the semantic no-op `OFFSET 0`) and reusing their equijoin indexes under the blocking-query memory limit.
- [x] Index a top-level OR join only when every disjunct has a safe hash-comparable equality key. Union and deduplicate candidate right-row positions in original order, then recheck the full ON predicate; all four join kinds match an independent nested loop with NULLs, duplicates, overlap, and unmatched rows, while an unsafe branch declines the entire optimization.
- [x] Rebuild Gres and replay both the isolated upstream two-branch OR join and its retained 20-test cohort under the 20 MiB policy. The isolated PostgreSQL query returns the expected `19000` in 0.30 seconds without a memory error (`target/pg-regress-runs/20260803T230847Z-or-join-postfix-pass`); the complete serial schedule returns the same final count and reduces `join` from the retained pre-fix replay's 294.320 seconds to 9.532 seconds, while the complete parallel schedule clears the prior cohort stall (`target/pg-regress-runs/20260803T231638Z-rebased-final-gres`). Bounded post-build index accounting, fixed-capacity OR merge scratch, and count-only join folding close this scoped hotspot.
- [ ] Generalize bounded count folding to safe three-or-more-branch OR joins. An earlier three-branch tenk1-by-tenk1 count in the same upstream `join` file still exceeds the blocking-query memory budget, so the completed two-branch gate does not claim all OR joins are memory-error-free or semantically exact.
- [ ] Eliminate bounded lateral-cache thrashing without retaining every full right relation: for cacheable `INNER`/`LEFT` exact equijoins, group outer rows by stable specialization, build each right relation/index once, restore outer-row order, and stream or projection-prune downstream aggregation so wide joined results remain inside the same memory policy. The current serial `subselect` file takes 296.989 seconds and makes the same parallel cohort finish its four buffered files together at roughly 302--306 seconds.
- [ ] Reduce the other measured full-schedule performance roots without weakening the 20 MiB policy or semantics: serial `alter_table` 118.091 seconds, `psql` 110.858, `reloptions` 89.371, `tablespace` 85.356, `fast_default` 63.751, `partition_join` 55.476, and `indexing` 50.267.
- [x] Evaluate default-frame window `count` and `sum` incrementally by peer group; retain the general frame evaluator for every other aggregate and explicit frame.
- [ ] Re-run the full serial schedule after each shared fix and ratchet only tests whose recorded mismatch surface shrank.

**Gate:** Every remaining failure reproduces as an engine semantic difference; infrastructure failures are zero.

## Task 4: Burn down the measured semantic roots

For each item, first add one focused test at the shared layer that fails before the fix, implement the smallest shared correction, run the owning upstream file, then run the complete serial schedule and ratchet both compatibility ledgers.

- [ ] Reclassify the fresh artifact by semantic root and fix the largest coherent family first; do not carry forward the stale 1089 wrong-row count.
- [ ] Fix the earliest error in each transaction-abort cascade before touching its downstream `25P02` statements.
- [x] Review the current certified result against the monotone baseline: there are no new failures; 18 retain exact baseline signatures, 58 worsen, 20 retain their mismatch size with a changed fingerprint, 113 improve, and 16 failures disappear. The worsened and changed fingerprints keep the checked-in `6/231` floor from ratcheting.
- [ ] Explain every non-monotone fingerprint before ratcheting the checked-in `6/231` floor.
- [ ] Finish JSONPath grammar and canonicalization: recursive-descent bounds, escape and surrogate handling, context-sensitive `last` and `@`, numeric methods, and exact output formatting.
- [ ] Finish JSONPath evaluator gaps, especially datetime/template behavior and remaining strict/lax path semantics.
- [ ] Implement durable user-defined operator objects and `CREATE`/`ALTER`/`DROP OPERATOR` in Q4, separately from the supported `OPERATOR(...)` expression wrapper: implementation-routine/type linkage, unary `NONE` signatures, commutator/negator links and cleanup, `pg_operator` projection/dependencies, signature-based drop, `IF EXISTS`, and schema/type diagnostics. The bounded representatives currently refuse with `0A000`.
- [x] Implement correlated `SELECT ... WHERE` subqueries for the tested `EXISTS`/`NOT EXISTS`, `IN`, and scalar forms. Preserve inner-name shadowing, case-sensitive qualifiers, ambiguous-column errors, empty-input validation, lazy CASE/COALESCE/initplans, and locking EPQ behavior. For the narrow scalar shape of one local base table, one directly projected column, an immutable same-typed outer equality key, and literal `LIMIT 1` without ordering, grouping, or locking, build one lazy statement-local hash lookup while preserving first-visible-row, duplicate, NULL, snapshot, and fallback semantics. Eligible equality-key `EXISTS`/`NOT EXISTS`, including an `EXISTS` nested under `OR`, reuse the same lazy lookup while projecting only the retained key/result columns under the blocking-memory policy.
- [ ] Extend correlation to projection, HAVING, UPDATE SET, RETURNING, and grouped-output scopes; the completed WHERE forms do not imply general decorrelation.
- [ ] Verify PostgreSQL operator lookup and coercion for `varchar(n)[]`/`bpchar(n)[]`, including typmod preservation across array construction, comparison, containment, concatenation, and `ANY`/`ALL`.
- [x] Implement exact `(schema, name)` identity for schema-qualified user types and automatic/explicit multirange companions, including quoted identifiers containing dots, durable catalog serialization/hydration, namespace collisions, rename/drop behavior, and fresh-session lookup.
- [x] Certify the schema-qualified user-type and multirange identity wave in a complete serial and parallel artifact before updating the certified headline or score. The complete artifact is `target/pg-regress-runs/20260803T231638Z-rebased-final-gres`; certification records the wave's measured result and does not imply that its owner files are exact.
- [x] Include schema-qualified user types and their generated multirange/dependent types in schema dependency discovery and cleanup. `DROP SCHEMA ... RESTRICT` rejects a nonempty type-only schema; `CASCADE` and the shared temp cleanup remove roots and transitive dependents in dependents-first order, including primary and multirange registry identities. The schema lifecycle integration target passes 10 / 10 and pgcatalog schema tests pass 9 / 9.
- [x] Within one active catalog, publish the process type registry only from the durable catalog delta after TYPE/DOMAIN create, alter, drop, `DROP SCHEMA CASCADE`, `DISCARD TEMP`, session teardown, or stale-temp reclamation commits and event triggers accept it. Successful nested event-trigger DDL re-reads the final durable type set before publication; a rejected hook publishes the current-to-restored rollback delta. Event-trigger rejection, partial multi-drop builder failure, commit/read failure, and savepoint rollback leave parser-visible names aligned with catalog state; one registry write lock applies removals and replacements atomically, including multirange mappings.
- [ ] Scope the process user-type registry by a stable catalog identity and replace each catalog namespace atomically during hydration. Independent `SqlEngine` catalogs currently allocate the same local user-type OIDs and can overwrite one another's global name/OID mappings; single-catalog atomic publication does not provide multi-catalog isolation.
- [ ] **User types are not fully rehydrated on durable restart, which makes
      `pg_class` and `pg_attribute` unreadable.** Run the 91 tests preceding
      `sanity_check` against a `--data-dir` instance, stop it, and start a new
      process on the same directory: every scan of either catalog then returns
      `catalog storage error: corrupt row encoding: column type oid 300119 is
      not a registered type`, while `pg_type` and `pg_namespace` still read and
      300119 is absent from the projected `pg_type`. One unhydrated type
      therefore breaks *all* relation introspection, not merely the query that
      touches it.
      Scope, established by measurement rather than assumed: the originating
      run itself logs **zero** such errors, and so does the complete certified
      serial schedule — the failure appears only *after* the restart. It is a
      hydration defect (see the process-registry item below), not a
      `DROP TYPE` closure defect, and it does not affect the pg_regress score,
      which runs in-memory without restarting. It does mean Gres cannot be
      restarted with user types present.
      Consequence for tooling: a snapshot-and-restart harness is **not** sound
      for reproducing an in-memory schedule failure, because restart changes
      catalog behaviour. The `sanity_check` residual — whose real message in the
      certified run is a truncated `ERROR:  column "` — is still unreproduced
      and still unexplained.
- [ ] Extend schema/type dependency closure beyond user types to every non-type dependent: table columns and defaults, routines, views, indexes, and their catalog dependency rows. In particular, `DROP TYPE`/`DROP SCHEMA ... CASCADE` can remove a user-type record while leaving a table outside the dropped schema with a column that references the tombstoned OID. A type-to-type cascade is not yet full PostgreSQL object dependency closure.
- [ ] Make stored SQL/expression reparsers (views, SQL/PL/pgSQL bodies, and domain/check expressions) use the session or captured type search path; today an unqualified non-`public` user type can fail to resolve or rebind after creation.
- [x] Retain usable hash-compatible keys in mixed-key equijoins and recheck the full join predicate for every candidate.
- [x] Let PL/pgSQL event-trigger functions fall through with a NULL result while ordinary trigger functions still report `2F005` when control reaches the end.
- [x] Implement strict `booleq`/`boolne`, including argument validation before NULL short-circuiting and boolean-domain inputs.
- [x] Resolve range/multirange relation and arithmetic families before applying strict NULL semantics, including typed-peer inference for unknown multirange literals and typed-invalid/all-unknown operator errors.
- [x] Match `pg_class` relation-kind, access-method, and filenode semantics for mapped catalogs, ordinary and partitioned relations/indexes, views, sequences, and composite-type relations.
- [x] Classify multirange array `pg_type` rows as category `A` / base type `b`, while scalar multiranges remain category `R` / multirange type `m`.
- [ ] Implement native `tid`/`tid[]` identity and input diagnostics, stable `ctid` system-column projection, `currtid2`, `WHERE CURRENT OF`, and TID/TID-range access semantics. The KV row identity is not a PostgreSQL heap page/offset TID, so `tid`, `tidscan`, and `tidrangescan` remain storage-semantic blockers rather than EXPLAIN-only mismatches.
- [ ] Complete bit strings, remaining named composite/record semantics, `bytea`, `reg*` object identifiers, and exact float special-value behavior. Anonymous-record OID 2249 plumbing is only the shared foundation.
- [ ] Implement aggregate `ORDER BY`, ordered-set aggregates, record-returning function column definitions, and recursive CTE `SEARCH`/`CYCLE`.
- [ ] Burn down the measured residuals in the PL/pgSQL, trigger, foreign-key, schema, and full-text-search owner files.
- [ ] Complete expression/partial indexes next, then stored views over general queries, sequence lifecycle, array-slice assignment, and partitioned-table update semantics.
- [ ] Implement real ANALYZE target parsing and durable TableId-keyed `reltuples` statistics.
      This owns the whole 4-line `maintain_every` residual, which is otherwise
      exact: `pg_class` currently hardcodes `reltuples` to `Float4(-1.0)` and
      `relhassubclass` to `false` (`crates/pgexec/src/exec.rs`), so the test's
      `0 | t` then `0 | f` both read `-1 | f`. Note the trap: projecting
      `relhassubclass` from the live child graph would reproduce both expected
      lines, because the test only samples it after an `ANALYZE` — but that is
      the test-only shortcut this plan forbids, since PostgreSQL clears the hint
      *in `ANALYZE`* rather than at `DROP`. Implement the persisted hint. Preserve PostgreSQL's nontransactional row-count updates, target preflight, inheritance/partition counting, and stale-after-DML behavior. Keep `relhassubclass` separate: it is a persisted, potentially stale hint with different rollback semantics, not a projection of the current child graph.
- [ ] Finish source-aware `bpchar` to text coercion, input-independent scalar-HAVING scan elision, and clause-aware error positions. The measured `select_having` and `select_implicit` residuals otherwise already match.
- [ ] Implement full `name` type identity plus executable unique/partial expression indexes and hash-index entries/options before claiming `hash_index`; catalog-only expression metadata is insufficient.
- [ ] Implement schema element transformation/execution atomically, `CURRENT_ROLE` authorization resolution, ColId relation components, and DROP CASCADE notices before claiming `create_schema` exactness.
- [ ] Treat `unicode.out`, not the non-UTF8 skip alternate, as the UTF8 authority; finish U& strings/identifiers, UESCAPE, normalization syntax/predicates, and Unicode catalog helpers.
- [ ] Preserve typed COPY query/TO, CSV/file, and encoding-conversion semantics as one coherent COPY wave; `copyencoding` is not a one-error fix. Implement it through one typed relation-or-query COPY AST, the ordinary write/trigger epilogue for COPY FROM, option-aware text/CSV field decoding, COPY TO file and pgwire output, resumable multi-statement COPY state, and explicit/client encoding conversion.
- [ ] Implement database lifecycle/routing and `pg_database` metadata as one coherent database wave; a canned CREATE DATABASE success cannot satisfy reconnect/isolation tests.
- [ ] Finish catalog descriptions and dependency rows required by upstream sanity and introspection queries. Generate complete PostgreSQL 18.4 `pg_type` metadata and built-in `pg_range` rows; make user/catalog relation OIDs and system columns consistent across virtual catalogs; use exact catalog field identities; add the remaining small catalog helpers; and expose the generated `pg_get_catalog_foreign_keys()` descriptor set for `oidjoins`.

**Gate:** The adopted corpus reaches 100% for every already-vendored file, and the upstream serial failure list strictly shrinks in each reviewed wave.

## Task 5: Reopen incompatible non-goals

Literal 231 / 231 is incompatible with retaining PostgreSQL-visible exclusions exercised by the core suite. Each item must either implement the observable behavior or remain an explicit blocker to the 100% claim; test-only canned output does not count.

- [ ] Multiple databases and reconnect behavior.
- [ ] Roles, privileges, ownership, and row-level security.
- [ ] Tablespaces, large objects, prepared transactions, publications, and subscriptions.
- [ ] Access methods, user-defined operators, operator classes/families, casts, collations, and encoding variants.
- [ ] C-language regression functions or a production-grade compatible execution mechanism.
- [ ] Planner and `EXPLAIN` details asserted by upstream expected output.

**Gate:** No scheduled upstream test is excluded because its feature is marked `Non-goal` or `Error-with-notice` in `PG_COMPAT_MATRIX.md`.

## Task 6: Turn on concurrency and distributed storage

- [ ] Reach 231 / 231 with `--max-connections=1` before spending a wave on parallel-only semantic parity; infrastructure crashes, stalls, and connection loss are investigated immediately in either mode.
- [x] Diagnose the non-certifying parallel stall recorded at 92 / 231 in `target/pg-regress-runs/20260803T213156Z-exists-projection-certified-gres`. The retained non-certifying replay at `target/pg-regress-runs/20260803T221236Z-parallel-stall-repro-gres` did not reproduce a lock cycle and exposed the two-branch OR hotspot; the scoped fix reduces serial `join` from 294.320s to 9.532s and the complete parallel artifact now reaches 231 / 231 scheduled files. The residual cohort wall time is the 296.989-second serial lateral `subselect` root, not the fixed final OR query.
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
