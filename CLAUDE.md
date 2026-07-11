# Crabka — project-specific guidance

## Compatibility

**Crabka is greenfield and undeployed.** There are no production users, no persisted state to migrate, no clients pinned to a specific build. Don't write backwards-compatibility shims:

- No `#[serde(default)]` on metadata fields "to keep old raft logs readable"
- No `V2` enum variants kept around alongside `V1` to support replay
- No feature flags that gate new behavior behind a default-off switch
- No migration code or one-shot upgraders for on-disk format changes
- No deprecated-but-kept API surfaces

When a schema, enum, wire format, or interface changes, just change it. Wipe local raft logs / data dirs if needed during development.

**Kafka compatibility is the constraint that matters.** Always preserve:

- Apache Kafka wire-protocol byte exactness (request/response shapes, field order, error codes, version negotiation)
- KIP semantics for whatever feature is being implemented
- Behavior the JVM admin tools (`kafka-topics`, `kafka-acls`, `kafka-leader-election`, `kafka-reassign-partitions`, etc.) rely on

When in doubt, match Kafka. When Kafka's behavior is undocumented or version-dependent, check the latest released cp-kafka image's behavior empirically rather than reading the wiki.

## Code & Documentation Style

Follow the style guides in [`docs/style_guides/`](docs/style_guides/README.md) — [code](docs/style_guides/code_style_guide.md), [rustdoc](docs/style_guides/rustdoc_style_guide.md), [README](docs/style_guides/readme_style_guide.md), [design docs](docs/style_guides/design_doc_style_guide.md), and [coverage reports](docs/style_guides/coverage_report_style_guide.md). They record Crabka's actual conventions (pinned stable toolchain, `cargo +nightly fmt`, `unsafe` forbidden, `clippy::pedantic`, workspace lints/deps, `crabka-<name>` crates, thiserror error enums, tokio, `assert2`/`nextest`/mutation testing).

Don't make style-only sweeps across untouched files — bring a file into line with the guides only when you're already editing it, keeping the tidy-up proportionate to the change.

### Assertions and Clippy

- Never add `#[allow(clippy::...)]` (or any equivalent Clippy suppression). Fix every Clippy warning in the code, regardless of the effort required.
- Never use Rust's plain `assert!`, `assert_eq!`, or `assert_ne!` macros. Use the `assert2` crate's `assert!` macro instead, including for equality and inequality comparisons.

## Execution

When executing implementation plans, always use **subagent-driven development in parallel batches** where the per-task file sets don't overlap. The plan groups tasks into batches; dispatch all tasks within a batch concurrently (single message, multiple Agent calls), wait for the batch to complete, review, then move to the next batch. Sequential dispatch one-task-at-a-time is wasted wall-clock — use it only when later tasks genuinely depend on earlier ones in the same batch.

A "conflict" between parallel implementers requires the same file being edited by both. Tasks like "add wire codes" (codes.rs) and "add metadata fields" (records.rs) don't conflict and should run together. When in doubt, list the file set each task touches before deciding.

Tests must exercise behavior, not source text. Do not read source files in tests (for example with `include_str!`/`fs::read_to_string`) and assert against their contents. If a behavior is hard to test, introduce a narrow helper or seam and test that behavior directly.

When checking generated protocol records or other structured values in tests, prefer comparing the whole expected struct over long chains of field-by-field assertions. Use table-driven or parameterized tests for repeated scenarios that differ only by inputs, protocol version, or expected request shape.

## Release Process

- Uses **release-plz** for automated semantic versioning
- Conventional commits drive version bumps:
  - `feat:` → Minor version
  - `fix:` → Patch version
  - `feat!:` → Major version
- Auto-generates changelogs and publishes to crates.io
