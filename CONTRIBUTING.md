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
