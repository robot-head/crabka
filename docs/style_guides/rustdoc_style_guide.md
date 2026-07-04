# Rustdoc Style Guide

This guide defines conventions for rustdoc comments across Crabka crates. It is based on patterns already established in `crabka-protocol`, `crabka-raft`, `crabka-broker`, and `crabka-metadata`. It complements the general [code style guide](code_style_guide.md).

## Crate-Level Documentation

Every library crate must have a crate-level doc comment at the top of `lib.rs`. Binary crates (`main.rs`) do not need one.

Crabka uses `//!` line comments for crate-level docs (not `/*! … */` blocks), matching the existing crates and sidestepping the pitfall that `/* */` blocks cannot contain a `*/` sequence — which matters here because doc text routinely mentions glob patterns, regexes, and byte sequences.

The crate-level doc should include:

1. **One-line summary** — what the crate does.
2. **Overview paragraph** — context, relationship to other Crabka crates, and the Kafka standard(s) / KIP(s) it implements.
3. **Key modules or types** — a brief list linking to the main entry points.
4. **Feature flags** — if any, with descriptions and which are on by default.

```rust
//! Kafka wire-protocol codec.
//!
//! `crabka-protocol` encodes and decodes every Apache Kafka request and
//! response message, byte-equivalent to the upstream JVM implementation. It
//! performs no I/O and makes no async assumptions; it is consumed by the
//! broker, client, and tooling crates in the workspace.
//!
//! # Key Types
//!
//! - [`owned`] — messages that own their data; easy to move across `await`.
//! - [`borrowed`] — zero-copy messages that reference the input buffer.
//! - [`ApiKey`] — the enum of every Kafka 4.3 API.
//!
//! # Feature Flags
//!
//! - `snappy`, `zstd`, `gzip`, `lz4` — record-batch compression codecs (all on by default).
```

Where a crate is published to docs.rs, set `#![doc(html_root_url = "https://docs.rs/<crate>/<version>")]` as the existing crates do.

## Public Item Documentation

Every public item (`pub fn`, `pub struct`, `pub enum`, `pub trait`, `pub type`) that is part of the crate's published API should have a doc comment. Items that are not part of the published API do not need rustdoc:

- Private items (`fn`, `struct`, etc. without `pub`).
- `pub(crate)` items — visible within the crate but not to consumers.
- `pub` items inside private modules (`mod foo`, not `pub mod foo`).

Use `//` comments on these if the logic needs explaining. Confirm that public types are actually exposed via a public module path before adding rustdoc. (Crabka does not force the `missing_docs` lint, so this is a review expectation, not a compiler error — hold the line at review.)

### One-Liner Items

Simple items use a single `///` line:

```rust
/// The last offset this partition has durably persisted.
pub log_end_offset: i64,
```

### Functions and Methods

Document what the function does, not how. Include parameters only when their purpose is not obvious from name and type.

```rust
/// Applies a controller record to the image, returning the mutated topics.
///
/// Called only from the Raft state machine; everywhere else the image is
/// read through shared references.
pub fn apply(&mut self, record: &MetadataRecord) -> Vec<TopicId> {
```

### Complex Items

For types or functions with non-trivial behaviour, use structured sections:

```rust
/// A cancellable pool of async tasks with panic propagation.
///
/// Tasks spawned on the pool are cancelled when the pool is dropped.
/// If any task panics, the panic is propagated to the next `join()` call.
///
/// # Examples
///
/// ```no_run
/// # async fn f(pool: crabka_broker::TaskPool) {
/// pool.spawn(async { /* ... */ });
/// pool.join().await;
/// # }
/// ```
///
/// # Panics
///
/// `join()` panics if any spawned task panicked.
```

## Sections

Use only these standard sections, in this order:

| Section | When to use |
|---------|-------------|
| `# Examples` | Complex APIs where usage is not obvious |
| `# Panics` | When the function can panic in normal use |
| `# Errors` | When returning `Result` and the error conditions are worth calling out |

There is no `# Safety` section: Crabka forbids `unsafe` (`unsafe_code = "forbid"`), so there are no `unsafe fn` to document.

`missing_errors_doc` and `missing_panics_doc` are relaxed in the workspace lints, so `# Errors` / `# Panics` are **not** lint-forced. Add them where they genuinely help the caller; omit them when the error or panic condition is already obvious from the signature. Do not add sections that merely repeat the summary.

## Examples

- Doc examples compile **and run** in CI (`cargo test --workspace --doc`). Keep them correct against the current API.
- Use ```` ```no_run ```` for examples that need a runtime, a network, or a live broker — they compile but are not executed.
- Use ```` ```ignore ```` only for genuinely incomplete snippets, and sparingly.
- Keep examples minimal — show the API call, not the setup. Use `#`-hidden lines for boilerplate the reader doesn't need to see.
- Because `rustfmt.toml` sets `format_code_in_doc_comments = true`, `cargo +nightly fmt` formats the code inside your examples — keep them fmt-clean so the format check passes.

## KIP and Standard References

When a type or function implements a specific Kafka behaviour or KIP, reference it so a reader can trace the requirement:

```rust
/// Assigns partitions using the uniform-sticky strategy from [KIP-848].
///
/// [KIP-848]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
```

Use reference-style links at the bottom of the doc comment. Do not inline long confluence URLs in the prose.

## Cross-References

Link to other types and modules using rustdoc syntax:

```rust
/// Returns the [`RecordBatch`] decoded from the given bytes.
///
/// See [`RecordBatchBuilder`] for constructing batches programmatically.
```

Use full paths when referencing items in other crates:

```rust
/// Uses [`crabka_protocol::records`] for record-batch decoding.
```

## Configuration Structs

All `Config` structs with a `serde` derive must document every field, including its default value where one exists:

```rust
/// Broker listener configuration.
pub struct Config {
    /// Kafka wire-protocol listen address. Default: `0.0.0.0:9092`.
    pub listen_address: String,

    /// Maximum in-flight produce requests per connection. Default: `5`.
    pub max_in_flight: NonZeroUsize,
}
```

These fields surface in the generated configuration reference, so the doc line is the operator-facing documentation — keep it accurate and name the default.

## Traits

Trait documentation should describe the contract, not the implementation. Include:

1. What implementors must provide.
2. What callers can expect.
3. Lifecycle (if registration / handles / shutdown are involved).

```rust
/// A pluggable remote-storage backend for tiered log segments (KIP-405).
///
/// Implementors provide durable, offset-addressed segment storage. The broker
/// uploads sealed segments and fetches them on read-through; a backend must be
/// safe to call concurrently from multiple partitions.
pub trait RemoteStorage: Send + Sync + 'static {
```

## Trait Implementations

Trait impl methods do not need `///` doc comments unless the implementation behaviour is surprising or deviates from the trait documentation. A normal `//` comment explaining the approach is useful:

```rust
impl Encode for RecordBatch {
    // Length and CRC are back-patched after the body is written.
    fn encode(&self, buf: &mut BytesMut, version: i16) {
```

## What Not To Document

- Re-exports (document at the source).
- `impl` blocks for derived traits (`Debug`, `Clone`, etc.).
- Trait impl methods (unless behaviour is surprising — use `//` comments instead).
- Test modules and test helper functions.
- Items behind `#[doc(hidden)]` and generated code (the protocol codec is generated; document the generator's output shape at the module level, not per generated field).

## Checking Documentation

```bash
# Check for missing docs / broken links on public items
cargo doc --no-deps --package <crate>

# Build docs for the whole workspace
cargo doc --no-deps --workspace

# Verify examples compile and run
cargo test --package <crate> --doc
```

## Correctness Pass

After adding or updating doc comments, always perform a correctness pass against the actual code. Documentation that describes wrong behaviour is worse than no documentation.

Check for:

1. **Signature mismatches** — do parameter types and return types in docs match the code?
2. **Stale defaults** — do documented default values match the `Default` impl and the config generator?
3. **Renamed or removed items** — do cross-references point to types/methods that still exist?
4. **Example code** — would the examples compile and run against the current API?
5. **Feature flag references** — are conditional compilation features still valid?
6. **Behavioural and KIP claims** — does the code actually do what the doc says, and does the referenced KIP still describe that behaviour?

This pass should be performed whenever docs are written or code is refactored. The weekly `docs-freshness` CI job re-derives the generated reference from live code and flags drift — but that catches the generated docs, not your hand-written rustdoc; the correctness pass is how the latter stays true.
