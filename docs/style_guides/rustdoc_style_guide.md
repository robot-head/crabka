# Rustdoc Style Guide

This guide defines conventions for rustdoc comments across Crabka crates. It follows the patterns that `crabka-protocol`, `crabka-raft`, `crabka-broker`, and `crabka-metadata` already use. It complements the general [code style guide](code_style_guide.md).

This guide defines **structure**. The [prose style guide](prose_style_guide.md) defines **wording**, and it applies to every doc comment: Simplified Technical English, one short summary sentence on the first line, and `must` only where the code enforces the rule.

## Crate-Level Documentation

Every library crate must have a crate-level doc comment at the top of `lib.rs`. Binary crates (`main.rs`) do not need one.

Crabka uses `//!` line comments for crate-level docs, not `/*! … */` blocks. This matches the existing crates. It also avoids a limit of `/* */` blocks: they cannot contain a `*/` sequence. That limit matters here, because doc text often mentions glob patterns, regexes, and byte sequences.

The crate-level doc should include:

1. **One-line summary** — what the crate does.
2. **Overview paragraph** — context, the relationship to other Crabka crates, and the Kafka standards and KIPs it implements.
3. **Key modules or types** — a short list with links to the main entry points.
4. **Feature flags** — if the crate has any, with a description of each and which ones are on by default.

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

If Crabka publishes a crate to docs.rs, set `#![doc(html_root_url = "https://docs.rs/<crate>/<version>")]` as the existing crates do.

## Public Item Documentation

Every public item that is part of the crate's published API should have a doc comment. This covers `pub fn`, `pub struct`, `pub enum`, `pub trait`, and `pub type`. Items that are not part of the published API do not need rustdoc:

- Private items (`fn`, `struct`, etc. without `pub`).
- `pub(crate)` items — visible within the crate but not to consumers.
- `pub` items inside private modules (`mod foo`, not `pub mod foo`).

Use `//` comments on these if the logic needs an explanation. Before you add rustdoc, confirm that a public module path exposes the public type. Crabka does not force the `missing_docs` lint, so this is a review expectation and not a compiler error. Enforce it at review.

### One-Liner Items

Simple items use a single `///` line:

```rust
/// The last offset this partition has durably persisted.
pub log_end_offset: i64,
```

### Functions and Methods

Document what the function does, not how. Include parameters only when their purpose is not obvious from the name and the type.

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
| `# Errors` | When the function returns `Result` and the error conditions are worth a note |

There is no `# Safety` section: Crabka forbids `unsafe` (`unsafe_code = "forbid"`), so there are no `unsafe fn` to document.

The workspace lints relax `missing_errors_doc` and `missing_panics_doc`, so the lints do **not** force a `# Errors` or `# Panics` section. Add them where they genuinely help the caller. Omit them when the error or panic condition is already obvious from the signature. Do not add sections that only repeat the summary.

**State the real conditions for the item the section sits on. Never paste a generic sentence across a crate.** A section that does not describe its own item is worse than no section, because a reader trusts it. If you cannot state the real condition, delete the section. Do not leave a placeholder.

A prose audit of this workspace found eight such boilerplate strings and hundreds of copies. Many were false. One `# Panics` body named a poisoned mutex on a file with no mutex. One `# Errors` body named Kubernetes in a crate with no Kubernetes dependency. Doc comments said that encode functions fail on truncated input. One `# Errors` section sat on a function whose `Result` error type is not an error.

## Examples

- Doc examples compile **and run** in CI (`cargo test --workspace --doc`). Keep them correct against the current API.
- Use ```` ```no_run ```` for examples that need a runtime, a network, or a live broker. These examples compile, but CI does not run them.
- Use ```` ```ignore ```` only for genuinely incomplete snippets, and sparingly.
- Keep examples minimal. Show the API call, not the setup. Use `#`-hidden lines for boilerplate the reader does not need to see.
- `rustfmt.toml` sets `format_code_in_doc_comments = true`, so `cargo +nightly fmt` formats the code inside your examples. Keep them fmt-clean so the format check passes.

## KIP and Standard References

When a type or function implements a specific Kafka behaviour or KIP, reference it so a reader can trace the requirement:

```rust
/// Assigns partitions using the uniform-sticky strategy from [KIP-848].
///
/// [KIP-848]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
```

Use reference-style links at the bottom of the doc comment. Do not inline long confluence URLs in the prose.

## Cross-References

Use rustdoc syntax to link to other types and modules:

```rust
/// Returns the [`RecordBatch`] decoded from the given bytes.
///
/// See [`RecordBatchBuilder`] for constructing batches programmatically.
```

Use full paths when you reference an item in another crate:

```rust
/// Uses [`crabka_protocol::records`] for record-batch decoding.
```

## Configuration Structs

All `Config` structs with a `serde` derive must document every field. A field with a default value must also document that default:

```rust
/// Broker listener configuration.
pub struct Config {
    /// Kafka wire-protocol listen address. Default: `0.0.0.0:9092`.
    pub listen_address: String,

    /// Maximum in-flight produce requests per connection. Default: `5`.
    pub max_in_flight: NonZeroUsize,
}
```

These fields surface in the generated configuration reference, so the doc line is the operator-facing documentation. Keep it accurate and name the default.

## Traits

Trait documentation should describe the contract, not the implementation. Include:

1. What implementors must provide.
2. What callers can expect.
3. Lifecycle, if the trait involves registration, handles, or shutdown.

```rust
/// A pluggable remote-storage backend for tiered log segments (KIP-405).
///
/// Implementors provide durable, offset-addressed segment storage. The broker
/// uploads sealed segments and fetches them on read-through; a backend must be
/// safe to call concurrently from multiple partitions.
pub trait RemoteStorage: Send + Sync + 'static {
```

## Trait Implementations

Trait impl methods do not need `///` doc comments, unless the implementation behaviour is surprising or it deviates from the trait documentation. A normal `//` comment that explains the approach is useful:

```rust
impl Encode for RecordBatch {
    // Length and CRC are back-patched after the body is written.
    fn encode(&self, buf: &mut BytesMut, version: i16) {
```

## What Not To Document

- Re-exports (document at the source).
- `impl` blocks for derived traits (`Debug`, `Clone`, etc.).
- Trait impl methods, unless the behaviour is surprising. Use `//` comments instead.
- Test modules and test helper functions.
- Items behind `#[doc(hidden)]` and generated code. Crabka generates the protocol codec. Document the generator's output shape at the module level, not for each generated field.

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

After you add or update doc comments, always do a correctness pass against the code. Documentation that describes wrong behaviour is worse than no documentation.

Check for:

1. **Signature mismatches** — do parameter types and return types in docs match the code?
2. **Stale defaults** — do documented default values match the `Default` impl and the config generator?
3. **Renamed or removed items** — do cross-references point to types and methods that still exist?
4. **Example code** — would the examples compile and run against the current API?
5. **Feature flag references** — are conditional compilation features still valid?
6. **Behavioural and KIP claims** — does the code do what the doc says, and does the referenced KIP still describe that behaviour?

You should do this pass whenever you write docs or refactor code. The weekly `docs-freshness` CI job re-derives the generated reference from live code and flags drift. But that job catches the generated docs, not your hand-written rustdoc. The correctness pass is how your hand-written rustdoc stays true.
