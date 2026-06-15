# Contributing to Crabka

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
but no test asserts on its behaviour — a stronger signal than line coverage.
Reproduce a run locally with:

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
