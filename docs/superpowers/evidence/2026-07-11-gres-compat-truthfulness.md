# Gres compatibility truthfulness evidence — 2026-07-11

**Status: review-pending — INCOMPLETE until independent re-review accepts the remediation.**

Scope: SQL-Parity Program Task 1 metadata and executable behavior only. This does not advance any later SQL wave.

## Authoritative inventory

- Four immutable PostgreSQL `REL_18_0` artifacts are checked and hashed: `allfiles.sgml` (`4240987b5fddaa5ab5ffa2562551cb1325f2e5527b552c3bbe5be7ca6fd42fc7`), `alter_table.sgml` (`dc44b2b50476dff8ed0e7f79d425e6b404f3b0860a91f18f536490f912c02dbe`), `create_table.sgml` (`8f281d48523129f41a81d6c6e1fdc4d6de7637cf31f36f5c63940fd2d1b51972`), and `select.sgml` (`300d0d5eb2bc5b7a1ef69f528c2a673c11819bb4dc975f9a7f82dff7fe2c560d`).
- The deterministic extractor reads 183 SQL-command entities, expands PostgreSQL's 20 abbreviated entity filenames, and extracts the remaining seven titles from the authoritative command synopses. No unconditional title expansion set exists.
- The checker requires exact equality between that derived set, `pg18-command-inventory.json`, and the command matrix. Mutation tests cover missing, extra, renamed, fake, duplicate, and wrong-count inventories plus missing/fake/extra synopsis sources and mappings.
- All 23 major language-feature rows live in a separate typed manifest and are excluded from the 190-command count. Implemented representatives execute, the extended-protocol representative binds a parameter, explicit refusals assert their error contracts, and pending representatives record observed parser/session behavior.

## Executable behavior manifest

- Actual parser dispatch branches emit `ParsedStatement { statement, command_identity }`; a bare successful branch does not type-check. The registry contains 92 accepted identities, aliases sharing AST variants emit distinct identities in their consuming branch, and public wrappers only strip or expose already-emitted identities. No raw-SQL/AST classifier exists.
- Report format v2 separately contains 92 behavior probes: all 42 `Implemented`/`Mapped` command rows and all 50 `Error-with-notice`/executable `Non-goal` command rows.
- Every probe records representative SQL and expected parser shape. Refusals additionally record exact SQLSTATE and a stable message fragment.
- `compatibility_behavior.rs` executes all 92 through `SqlEngine` sessions with deterministic per-probe setup. Its COPY representative uses the session CopyIn API rather than treating a simple-query refusal as success.
- Database and extension lifecycle refuse with `0A000`; SQL-level two-phase-commit lifecycle refuses with `55000`.
- All 40 architectural Non-goal rows use centralized typed metadata and bounded token grammars accepting identifier/literal substitutions while rejecting malformed/appended neighbors. Representatives and systematic variants pass the `libpg_query` grammar oracle.

## Checker failure directions

`--self-test` proves failure for every inventory mutation direction; an injected accepted alias absent from the matrix; a resolved row without a behavior probe; a probe without a row; command disposition/behavior disagreement; a parser-accepted wave-assigned row without intentional refusal; and feature-manifest missing/orphan/mismatch directions.

## Verification

The following commands were run from the repository root and passed on 2026-07-11:

```text
cargo test -p crabka-pgparser -p crabka-pgexec -p crabka-gres-conformance --lib --no-fail-fast
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/15/include' CFLAGS='-I/usr/lib/gcc/x86_64-linux-gnu/15/include' cargo test -p crabka-pgparser --features oracle --test libpg_query_oracle compatibility_refusal_representatives --no-fail-fast
cargo test -p crabka-gres-conformance --test compatibility_behavior --no-fail-fast
cargo check --workspace --all-targets --locked
cargo clippy -p crabka-pgparser -p crabka-pgexec -p crabka-gres-conformance --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
python3 scripts/tests/gres_f0_runtime_gates.py
tools/check-pg-compat-matrix.sh --self-test
tools/check-pg-compat-matrix.sh
git diff --check
```

The workspace-wide clippy command was also attempted. It reached unrelated existing `manual_assert_eq` warnings in `crates/gres-substrate/src/writer.rs` and `crates/broker/src/diskless/hot_tail.rs`; the three task-owning crates pass all-target clippy with warnings denied as recorded above.
