# Contributing to Crabka

## Code & documentation style

Code and docs follow the [style guides](docs/style_guides/README.md) in `docs/style_guides/`:
[code style](docs/style_guides/code_style_guide.md), [rustdoc](docs/style_guides/rustdoc_style_guide.md),
[README](docs/style_guides/readme_style_guide.md), [design docs](docs/style_guides/design_doc_style_guide.md),
and [coverage reports](docs/style_guides/coverage_report_style_guide.md). Read the code style guide before your
first change. Formatting and linting are enforced in CI:

```bash
cargo +nightly fmt --all -- --check              # nightly: rustfmt.toml enables format_code_in_doc_comments
cargo clippy --workspace --all-targets -- -D warnings
```

## Prerequisites

- Rust toolchain pinned by `rust-toolchain.toml`.
- JDK 17 (for the differential-test oracle).
- `gradle` is *not* required at the system level — the wrapper is checked in.

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

## Mutation testing

CI runs [cargo-mutants](https://mutants.rs) on each pull request, but only on the
lines the PR changes (`--in-diff`). A surviving mutant means a changed line runs
but no test asserts on its behaviour — a stronger signal than line coverage, and
a surviving mutant fails the build. Reproduce a run locally with:

```bash
cargo install cargo-mutants
git diff origin/main...HEAD | tee git.diff
cargo mutants --in-diff git.diff
```

Settings (nextest runner, timeouts, excluded paths) live in `.cargo/mutants.toml`.

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
