# Gres G-6 FDW and SQL-breadth evidence — 2026-07-11

Scope is G-6 only. This evidence does not claim G-7 or later chapters.

## Source and runtime pins

- Verified tree: `e2e8c4f3` plus the G-6 verification-closure change.
- Rust toolchain: `rustc 1.97.0 (2d8144b78 2026-07-07)`.
- PostgreSQL compatibility oracle pinned by the conformance harness: PostgreSQL 18.
- Kafka and Schema Registry for the FDW product proof are the workspace's real
  in-process `crabka-broker` and `crabka-schema-registry`, not mocks or Docker
  substitutes.

## Requirement audit

### Headers

`FetchedRecord` exposes the protocol crate's owned `RecordHeader`, retaining
wire order, duplicate keys, binary values, and null values. The real broker
roundtrip produces headers through `crabka-client-producer`, fetches them
through `crabka-client-core`, and projects this exact deterministic `_headers`
text:

```text
{"dup":"\\x6f6e65","dup":null,"z":"\\x00ff"}
```

The same roundtrip proves the empty-header form is `{}` and that header-only
projection composes with partition/offset pushdown. Client-core's complete unit
suite passed 51/51.

### Protobuf

The descriptor path compiles schema text with runtime `protox`, selects the
first message when registry metadata is absent, honors explicit and leading-dot
message names, follows Confluent frame message indexes (including nested
messages), rejects invalid/conflicting indexes, and resolves registry-provided
references without filesystem fallback. The cold-cache roundtrip exercises the
bounded `WriterSchemaPending` retry path.

The real sequence passed end to end:

```text
register referenced and multi-message schemas
-> produce Confluent-framed DynamicMessage values
-> IMPORT FOREIGN SCHEMA
-> typed SELECT
```

It returned `ProtoEvent(42, "protobuf row", true)` and referenced
`ProtoOrder(7, USD=1)` exactly. All FDW units passed 54/54.

### Own-cluster default server

Gres derives the FDW scanner default only in substrate mode. The real broker
roundtrip creates a server with no `bootstrap` option and reads the tenant's
topic through that default. A second real-I/O leg gives the scanner an
unusable default (`127.0.0.1:1`) and supplies the real bootstrap explicitly;
metadata, import, and fetch all succeed, proving explicit server options win.
The Gres wiring units passed, including local-mode `None` and substrate-mode
bootstrap propagation.

### Baseline ratchet

`crates/gres-conformance/README.md` requires corpus growth, baseline updates,
and PostgreSQL 18 parity evidence in one reviewed change and tells reviewers to
block baseline-only changes. `CONTRIBUTING.md` links that rule from its Gres
conformance section. The CI conformance job supplies all three baseline paths;
the F-0 structural validator and compatibility matrix anti-rot checks passed.

## Fresh verification

Passed:

```text
cargo nextest run -p crabka-client-core
  51 passed, 4 skipped
cargo nextest run -p crabka-gres-fdw
  54 passed
cargo nextest run -p crabka-gres-fdw --features roundtrip --test roundtrip
  1 passed (real broker + registry; repeated after explicit-override addition)
cargo clippy -p crabka-client-core -p crabka-gres-fdw -p crabka-gres \
  --all-targets --features crabka-gres-fdw/roundtrip -- -D warnings
cargo check --workspace --all-targets
cargo +nightly fmt --all -- --check
python3 scripts/tests/gres_f0_runtime_gates.py
bash scripts/tests/gres-e2e-topic-probe.sh
bash scripts/tests/gres-kind-lifecycle-structure.sh
./tools/check-pg-compat-matrix.sh --self-test
./tools/check-pg-compat-matrix.sh
git diff --check
```

The wider gates have two explicit non-G-6 contradictions:

- `cargo nextest run -p crabka-gres`: 55/56 passed; the G-8 transfer test
  `live_multirange_transfer_stages_populated_successor_without_publishing_it`
  forces a checkpoint while its WAL writer is paused and fails with
  `WAL topic unavailable: WAL writer is paused`.
- `cargo clippy --workspace --all-targets -- -D warnings` reached two existing
  `clippy::manual_assert_eq` failures in blockstore tests. G-6's complete
  dependency/target scope is strict-Clippy clean.
- `cargo nextest run --workspace --all-targets` could not reach test execution:
  its all-target link expanded `target/` to 485 GiB and failed with `ENOSPC`.
  Generated Cargo artifacts were then removed; no source or evidence was
  deleted.

These are recorded as wider-goal work, not reclassified as G-6 failures. Every
G-6 completion item has direct source and real-runtime evidence above.

