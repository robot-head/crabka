# Chapter Gres: SQL-Parity Program Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Full PostgreSQL 18 SQL-surface parity for the Gres engine, delivered as ~30 dependency-ordered wave cycles across six tracks, with every one of the 190 PG18 commands answered in a CI-guarded compatibility matrix and milestones defined by working software (drivers → pgbench → psql → ORMs → pg_regress).

**Architecture:** Per the [program design](../specs/2026-07-09-crabka-gres-sql-parity-program-design.md). This plan has two kinds of content: **concrete program-infrastructure tasks** (the matrix, the pg_regress pipeline, the F-0/F-1 foundations — buildable now, planned here in full) and **the wave cadence** (each subsequent wave is its own design cycle producing its own spec + plan under the standing rules; this plan sequences them and fixes their gates, it does not pre-design them — the pg-chapter-roadmap idiom).

**Tech Stack:** the vendored engine crates, the conformance/oracle harness, `docs/PG_COMPAT_MATRIX.md` (KIP_MATRIX idiom), PostgreSQL's regression corpus (PostgreSQL License), stateright where waves carry protocols (SSI, savepoints).

## Global Constraints

- **Prerequisites:** G-1 landed (all waves); G-2+ only where a wave's sharded story needs it. **G-4 (the PgDog front door) is required for the M0 gate and every pooler-story leg** — the conformance-through-PgDog runs and the transaction-pooling smokes cannot execute without it *(corrected after the PR panel review — "G-1 only" was wrong for those gates)*. Verify every quoted seam against the landed tree at execution time.
- **Spec:** the program design above. The four standing per-cycle rules (oracle/ratchet, sharded story, pooler story, matrix update) bind every wave without restatement.
- **The matrix cannot rot:** CI diffs `PG_COMPAT_MATRIX.md` against the parser's accepted-statement surface (Task 1); a wave that changes acceptance without a matrix row fails CI.
- **Explicit dispositions are code, not prose:** stock-PG-default errors (2PC-SQL 55000, CREATE DATABASE, non-goal commands) are implemented as recognizable parse-then-error paths with the documented SQLSTATE and hint — never generic syntax errors — so client software sees PostgreSQL-shaped refusals.
- Lints/format/commit/test conventions as in the G-2 plan.

---

## Batch 1 — program infrastructure (run Tasks 1 and 2 in parallel)

### Task 1: `PG_COMPAT_MATRIX.md` + the anti-rot check

**Files:** Create `docs/PG_COMPAT_MATRIX.md` (one row per PG18 command — all 190 — plus major language-feature rows; columns: item, disposition {Implemented, Wave-assigned(<wave>), Mapped(<semantics>), Error-with-notice(<SQLSTATE>), Non-goal(<reason>)}, notes; seeded from the program design's wave map and decisions — zero `UNDECIDED` rows at seed time; M5 is reached when zero `Wave-assigned` rows remain, per the design's M5 wording), `tools/check-pg-compat-matrix.sh` (extracts the parser's accepted statement kinds — a `cargo run` helper in gres-conformance that dumps the Statement enum's accepted-command list — and diffs against the matrix's Implemented/Mapped rows), a `ci.yml` step in the `gres` filter's job set, `CONTRIBUTING.md` pointer.

Steps: seed the matrix from the spec (every command, exhaustively — the grounding report is the checklist); write the dump helper + differ TDD (a deliberately-missing row fails); wire CI; commit `docs(gres): the PostgreSQL compatibility matrix with CI anti-rot`.

### Task 2: The pg_regress adoption pipeline (M4 standing gate)

**Files:** Create `tools/gres-adopt-regress.sh` (fetches a named `src/test/regress/sql/*.sql` + `expected/*.out` pair from a pinned postgres tag into `crates/gres-conformance/corpus-regress/`, with provenance header + NOTICE entry for the PostgreSQL-License vendoring), extend the conformance harness with a `--corpus-regress` mode (per-file baselines: `{file, total, matched}` ratchets instead of the single global baseline — regress files will be far from 100% initially and ratchet independently), CI artifact publishing the adoption percentage.

Steps: harness mode TDD (per-file baseline gate; a regressed file fails, an improved file requires the deliberate ratchet commit); adopt the first two files (`boolean.sql`, `int4.sql` — areas the corpus already mirrors) as the worked example; NOTICE provenance; commit `test(gres): progressive pg_regress corpus adoption`.

---

## Batch 2 — F-0: extended-protocol reality (serial; the one-way door)

### Task 3: `Session` trait v2 + parameter execution

**Files:** Modify `crates/pgwire/src/engine.rs` (the one-time widening: `parse/bind/execute(max_rows)`-shaped API with portal objects and reserved enum variants for CopyIn/CopyOut/Notification — designed once, reviewed hard), `session.rs` (Bind stores values; Execute honors `max_rows` with `PortalSuspended` (`s`) encoder in `backend.rs`; portal store with close-at-Sync semantics preserved per stock PG), `crates/pgexec` (`Expr::Param` evaluation: parameter values + type inference at bind, text/binary decode via `crabka-pgtypes` wire codecs), `crates/gres-conformance` (an extended-protocol mode re-running the corpus through prepared statements with parameters where statements permit).

Steps: strict TDD per layer — pgtypes param decode round-trips; executor param evaluation (typed placeholders across the expression suite); pgwire bind/execute with `max_rows` slicing + suspension (golden traces extended); the M0 gate: parameterized smokes via tokio-postgres AND sqlx AND one dynamic-driver-style trace, through PgDog transaction mode; corpus-through-extended-protocol at baseline. Commit sequence per layer; final `feat(pgwire): extended-protocol parameters and real portals (Session trait v2)`.

### Task 4: F-1 session/GUC machinery

**Files:** `crates/pgexec` (parameter registry: typed GUCs, SET/SET LOCAL/RESET/RESET ALL/SHOW semantics — transactional per stock PG; `DISCARD`), `crates/pgparser` (grammar), F-2 hook points (`pg_settings` lands with F-2's scanner; `current_setting()`/`set_config()` in the function library now), matrix rows flip.

Steps: TDD the registry (SET LOCAL scoping, RESET source-default semantics, transaction rollback of SET per the donor's existing quirk-faithful behavior extended to the full registry); driver-connect smokes (the exact SET batches sqlx/tokio-postgres/psycopg emit at connect, replayed as goldens); pooler story (SET through transaction pooling — deviations recorded in the pooler baseline with rationale). Commit `feat(gres): session parameter machinery (M0 complete with Task 3)`.

---

## Batch 3 — the wave cadence (each row = one future design cycle; this plan fixes sequence + gates)

Order within tracks is binding; across tracks, parallelize freely once F-0/F-1 land. Each cycle produces its own spec+plan; its exit gate is listed here and is not renegotiable at cycle time without a program-level decision.

| # | Wave | Gate (in addition to the four standing rules) |
|---|---|---|
| 5 | **F-2 pg_catalog** | **M2**: psql `\d`/`\dt`/`\di`/`\l`/`\du` golden sessions green |
| 6 | **D1 constraints** (SP41 port + GENERATED + NULLS NOT DISTINCT) | constraint corpus incl. PG error texts; pgbench schema parses |
| 7 | **D2 indexes** (multi-slice) | index-backed point/range reads measured; the chapter envelope's read numbers revised in the same commit |
| 8 | **D3 sequences/SERIAL** | sharded story = block allocation; pgbench init schema complete |
| 9 | **Q5 COPY** | **M1** with 6–8: stock pgbench initializes and runs; chunked-commit design decided in-cycle |
| 10 | **Q1 statement completeness** | ORM write-path smokes (RETURNING, ON CONFLICT, CREATE TABLE AS) |
| 11 | **D4 ALTER TABLE/TRUNCATE/COMMENT** | migration-tool ALTER corpus |
| 12 | **S1 SAVEPOINT** (sub-xids as first-class) | its Stateright model; Django/Rails nested-txn smoke — **M3** with 10–11 |
| 13 | **T1–T2 core types + uuid** | type corpus vs oracle incl. typmod truncation quirks |
| 14 | **T3 json/jsonb + SQL/JSON** | jsonpath corpus; `JSON_TABLE` |
| 15 | **T4 arrays** | array corpus; unnest-in-FROM lands with Q3 if sequenced earlier |
| 16 | **Q2 windows** | window corpus (pg_regress `window.sql` adopted as its ratchet file) |
| 17 | **Q3 SELECT completeness** | SKIP LOCKED job-queue smoke; recursive-CTE corpus |
| 18 | **Q4 expression/aggregate completeness** | operator corpus; aggregate corpus |
| 19 | **D5 views/matviews** → **P1b information_schema** | REFRESH CONCURRENTLY semantics; information_schema introspection smokes |
| 20 | **D6 FK/DEFERRABLE** | referential-action corpus; sharded FK = G-9d dependency stated |
| 21 | **S2 cursors + SQL PREPARE** | WITH HOLD semantics; driver cursor smokes |
| 22 | **D7 schemas/TEMP/declarative partitioning** | PARTITION BY → native sharding mapping (G-8/9-coupled cycle) |
| 23 | **S4 LISTEN/NOTIFY** | cross-gateway delivery via range-0 bus; commit-time semantics corpus |
| 24 | **S3 LOCK TABLE + advisory locks** | multi-range lock-service design (range-0 home) |
| 25 | **D8 RLS + roles/privileges** | RLS corpus; pooler+SET ROLE interplay stated |
| 26 | **T5–T8 remaining types + collations** | per-family corpora; icu4x decision executed |
| 27 | **P2 routines (SQL functions/procedures/CALL/DO)** | function corpus |
| 28 | **S6 EXPLAIN [ANALYZE]** | stable golden plans over the planner seam |
| 29 | **S5 SERIALIZABLE (SSI)** | SIREAD Stateright model; single-range SSI corpus; cross-range = named G-9 coupled cycle |
| 30 | **P3 PL/pgSQL** (internally staged) | plpgsql corpus adoption; in-proc txn control |
| 31 | **P4 triggers** | trigger corpus; INSTEAD OF with D5 |
| 32 | **P5 utility bucket + FDW lifecycle completeness** | every remaining matrix row leaves UNDECIDED-adjacent states — **M5** |
| 33 | **P6 stretch (CREATE CAST/AGGREGATE)** | optional; matrix rows flip from stretch |

## Completion checklist (maps to the program gates)

- M0 (Tasks 3–4), M1 (waves 6–9), M2 (wave 5), M3 (waves 10–12), M5 (wave 32) — each a published, dated matrix state.
- M4 standing: the pg_regress adoption percentage published per PR from Task 2 onward, monotone by ratchet.
- The matrix has zero unanswered rows at M5; CI enforced from Task 1 forever.
