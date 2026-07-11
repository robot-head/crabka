# Gres compatibility truthfulness evidence — 2026-07-11

Scope: SQL-Parity Program Task 1 metadata and executable behavior only. This does not advance any later SQL wave.

## Authoritative inventory

- `docs/pg18-command-inventory.json` records PostgreSQL major 18 and the pinned PostgreSQL 18 SQL Commands reference URL.
- The checker requires exactly 190 sorted, unique command titles and compares them bidirectionally with the command table in `PG_COMPAT_MATRIX.md`.
- Major language-feature rows are parsed separately and excluded from the 190-command count.

## Executable behavior manifest

- Report format v2 contains 92 probes: all 42 `Implemented`/`Mapped` command rows and all 50 `Error-with-notice`/executable `Non-goal` command rows.
- Every probe records representative SQL and expected parser shape. Refusals additionally record exact SQLSTATE and a stable message fragment.
- `compatibility_behavior.rs` executes all 92 through `SqlEngine` sessions with deterministic per-probe setup. Its COPY representative uses the session CopyIn API rather than treating a simple-query refusal as success.
- Database and extension lifecycle refuse with `0A000`; SQL-level two-phase-commit lifecycle refuses with `55000`.
- All 40 architectural Non-goal rows use centralized typed metadata, one bounded PostgreSQL syntax representative, `0A000`, and an architecture-specific message. Differential parser tests reject an appended arbitrary token.

## Checker failure directions

`--self-test` proves failure for an inventory with a missing, extra/renamed, or duplicate command; a resolved row without a probe; a probe without a row; disposition/behavior disagreement; and a parser-accepted wave-assigned row without an intentional refusal.

## Verification

The following commands were run from the repository root and passed on 2026-07-11:

```text
cargo test -p crabka-pgparser -p crabka-pgexec -p crabka-gres-conformance --lib --no-fail-fast
cargo test -p crabka-gres-conformance --test compatibility_behavior --no-fail-fast
tools/check-pg-compat-matrix.sh --self-test
tools/check-pg-compat-matrix.sh
```

The final all-target check, clippy, formatting, structural gate, and diff check are recorded in the task handoff after this artifact is committed.
