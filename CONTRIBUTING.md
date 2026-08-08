# Contributing to Crabka

## Code & documentation style

Code and docs follow the [style guides](docs/style_guides/README.md) in `docs/style_guides/`:
[code style](docs/style_guides/code_style_guide.md), [rustdoc](docs/style_guides/rustdoc_style_guide.md),
[README](docs/style_guides/readme_style_guide.md), [design docs](docs/style_guides/design_doc_style_guide.md),
and [coverage reports](docs/style_guides/coverage_report_style_guide.md). Read the code style guide before your
first change. CI enforces the formatting and the lints:

```bash
cargo +nightly fmt --all -- --check              # nightly: rustfmt.toml enables format_code_in_doc_comments
cargo clippy --workspace --all-targets -- -D warnings
```

## Prerequisites

- Rust toolchain pinned by `rust-toolchain.toml`.
- JDK 17 (for the differential-test oracle).
- `gradle` is *not* required at the system level. The repository contains the wrapper.

## Build

```bash
cargo build --workspace
```

## Run all tests (excluding JVM-dependent ones)

```bash
cargo test --workspace
```

## Run JVM-differential tests

```bash
(cd tools/oracle && ./gradlew installDist)
cargo test --workspace -- --include-ignored
```

## Gres conformance

The Gres conformance baseline is a ratchet against PostgreSQL 18. Do not use it
to hide regressions. When you change the SQL corpus or `baseline.json`, follow
the baseline-ratchet rules in
[`crates/gres-conformance/README.md`](crates/gres-conformance/README.md). Corpus
growth, baseline changes, and parity evidence belong in one reviewed change.

An SQL-surface change must also update
[`docs/PG_COMPAT_MATRIX.md`](docs/PG_COMPAT_MATRIX.md) in the same pull request.
SQL-surface changes are parser acceptance changes, executor semantic changes,
deliberate PostgreSQL-shaped refusals, aliases, and feature rows whose
disposition changes. Reviewers should reject an SQL-surface pull request that
leaves the matrix stale. Reviewers should also reject a pull request that marks
an accepted statement as anything other than `Implemented` or `Mapped(...)`.
Run the anti-rot gate locally before review:

```bash
tools/check-pg-compat-matrix.sh --self-test
tools/check-pg-compat-matrix.sh
```

## Mutation testing

CI runs [cargo-mutants](https://mutants.rs) on each pull request, but only on the
lines that the pull request changes (`--in-diff`). A surviving mutant means that
a changed line runs, but that no test asserts on its behavior. This is a stronger
signal than line coverage, and a surviving mutant fails the build. Reproduce a
run locally with:

```bash
cargo install cargo-mutants
git diff origin/main...HEAD | tee git.diff
cargo mutants --in-diff git.diff
```

The settings for the nextest runner, the timeouts, and the excluded paths are in
`.cargo/mutants.toml`.

## Regenerate code after editing schemas

```bash
./tools/regenerate.sh
git diff crates/protocol/generated
```

CI fails if `crates/protocol/generated` is out of sync with `crates/protocol/schemas`.

## Bumping the upstream Kafka version

1. `./tools/sync-schemas.sh <new-kafka-tag>`
2. `./tools/regenerate.sh`
3. Update the `kafka-clients` version in `tools/oracle/build.gradle.kts` to match.
4. `(cd tools/oracle && ./gradlew installDist)`
5. `cargo test --workspace -- --include-ignored`
6. Commit `schemas/VERSION`, regenerated files, and the Gradle bump together.
