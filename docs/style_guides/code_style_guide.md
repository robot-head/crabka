# Rust Code Style Guide

This guide defines the general Rust coding conventions used across Crabka crates. It complements the more specialised guides — [rustdoc](rustdoc_style_guide.md) for doc comments, [design docs](design_doc_style_guide.md), [READMEs](readme_style_guide.md), and [coverage reports](coverage_report_style_guide.md) — and is based on the patterns already established in `crabka-protocol`, `crabka-raft`, `crabka-broker`, `crabka-log`, and `crabka-metadata`.

The conventions here are what reviewers expect and what keeps a change reading like the code around it. When in doubt, match the surrounding module and write idiomatic Rust.

## Applying These Conventions

These conventions have converged over time, so not all existing code complies with every rule here. That is expected. **Do not make style-only sweeps across untouched files** — they create large, hard-to-review diffs and churn history for no functional gain.

Bring a file into line with this guide only when you are already modifying it for another reason, and keep the tidy-ups proportionate to the change so the substantive diff stays easy to review.

## Idiomatic Rust First

Default to idiomatic, community-standard Rust. This guide records Crabka's *project-specific* conventions and the few places we deviate from the norm — it is not a complete style manual. For everything it does not cover, follow the canonical references:

- [The Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) — the checklist for predictable, idiomatic public APIs (naming, trait implementations, interoperability).
- [The Rust Style Guide](https://doc.rust-lang.org/style-guide/) — the formatting and layout conventions that `rustfmt` implements.
- [The Rust Book](https://doc.rust-lang.org/book/) and [Rust by Example](https://doc.rust-lang.org/rust-by-example/) — idioms and patterns.

Clippy is the practical enforcer of most of these idioms; under `-D warnings` with `clippy::pedantic` enabled (see [Linting](#linting)) its suggestions are not optional. Prefer the idioms the compiler and Clippy steer you toward:

- Express flow with iterators and `Option`/`Result` combinators (`map`, `and_then`, `ok_or`, `?`) rather than manual index loops or nested matches, where it reads more clearly.
- Implement the standard conversion and formatting traits — `From` / `TryFrom`, `Display`, `FromStr`, `Default`, `Iterator` — instead of bespoke `to_x` / `from_x` methods, so types compose with the ecosystem. Wire messages implement `Encode` / `Decode`; error types convert via `#[from]`; resources release on `Drop`.
- Accept borrowed types in function signatures (`&str`, `&[u8]`, `impl AsRef<…>`) and return owned values; do not take `String` / `Vec<T>` by value just to read from it. On the decode hot path prefer the borrowed message flavour (`crate::borrowed::*`) that references the input buffer rather than copying it.
- Use the newtype pattern to give wire and domain values distinct types rather than threading raw integers around (`ApiKey`, `NodeId`, offsets, epochs) — the compiler then stops you crossing them. See [Newtypes for Domain Values](#newtypes-for-domain-values).
- Avoid needless `clone()` and intermediate allocations on hot paths — borrow, or move, instead. Producing/fetching runs per record batch; an extra allocation there is a throughput regression.

## Toolchain and Edition

- The workspace pins an **exact stable toolchain** in [`rust-toolchain.toml`](../../rust-toolchain.toml) (`channel = "1.97.0"` with `rustfmt` and `clippy`), so every developer and CI build compiles against the same compiler.
- The edition and MSRV are defined once in the workspace [`Cargo.toml`](../../Cargo.toml) (`[workspace.package]` `edition = "2024"` and `rust-version`) and inherited per crate via `edition.workspace = true` / `rust-version.workspace = true` — never hard-code them in a crate's `Cargo.toml`. Check that file for the current values.
- Do not use nightly-only *language* features in crate code. If a feature is not available on the pinned stable toolchain, it is not available here.
- The **one** sanctioned use of nightly is `cargo +nightly fmt` (see [Formatting](#formatting)), because `rustfmt.toml` enables a formatting option that is still nightly-gated. That affects only how source is laid out, never what the compiled crates depend on.

## Formatting

Formatting is decided by `rustfmt`, configured by [`rustfmt.toml`](../../rustfmt.toml):

```toml
format_code_in_doc_comments = true
```

That option keeps the code inside `///` / `//!` examples formatted like the rest of the tree. It is currently a nightly-only rustfmt feature, so CI runs:

```bash
cargo +nightly fmt --all -- --check
```

Run `cargo +nightly fmt` before committing (editors in the repo format on save). Do not hand-format code to deviate from what `rustfmt` produces; a mismatch fails the build.

## Linting

Clippy is a hard gate. CI runs:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Every warning is an error, across all targets. Fix the lint rather than suppressing it. If a lint genuinely does not apply, use a narrowly-scoped `#[allow(...)]` on the specific item (not a crate-wide allow) with a `//` comment explaining why.

Lint levels are set **once**, in the workspace `[workspace.lints]` table, and inherited by every crate through `[lints] workspace = true`. The current policy is:

- `unsafe_code = "forbid"` — Crabka crates contain no `unsafe`. See [Wire-Format Safety](#wire-format-safety). The single documented exception is the `crabka-log-iobench` benchmark crate, which opts out of the workspace lints (rather than weakening the project-wide forbid) because `memmap2::Mmap::map` is `unsafe` by contract; don't reach for that pattern in production crates.
- `clippy::pedantic = "warn"` — the pedantic group is on, so expect Clippy to be stricter than its defaults.
- A small set of pedantic lints are deliberately relaxed while the public API is pre-1.0 (`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc`). These are documented in the workspace `Cargo.toml`; do not add to the list without discussing it first.

Do not add per-crate lint configuration — a crate that needs a different lint level almost always wants a scoped `#[allow]` on the item instead.

## Naming

Follow standard Rust naming (Clippy enforces most of it):

- `snake_case` for functions, methods, variables, modules, and crate features.
- `UpperCamelCase` for types, traits, and enum variants.
- `SCREAMING_SNAKE_CASE` for constants and statics.
- Crates are named `crabka-<name>` in `Cargo.toml` (the directory under `crates/` is the bare `<name>`), and imported as `crabka_<name>`.
- Prefer descriptive names over abbreviations, but keep the Kafka domain vocabulary intact (`kraft`, `isr`, `offset`, `epoch`, `produce`, `fetch`, `coordinator`, `kip`) — it is the vocabulary of the codebase and of the KIPs Crabka implements. A field named to match the Kafka wire schema should keep that name.

## Imports and `use` Blocks

Organise `use` statements into up to three blocks, each separated by a single blank line, in this order:

1. The standard library — `std::…`, `core::…`, and `alloc::…` all share this one block.
2. Third-party crates — this includes the sibling `crabka_<name>` workspace crates, which are ordinary external dependencies to the crate importing them (`crabka_log::…`, `crabka_protocol::…`, `tokio::…`).
3. Local imports — `crate::…`, `super::…`, and `self::…` share this one block.

This layout is enforced by rustfmt: `rustfmt.toml` sets `group_imports = "StdExternalCrate"`, so `cargo +nightly fmt` produces exactly these three blocks in this order and the CI format check keeps them that way. (The option is nightly-gated, like `format_code_in_doc_comments`; plain stable `cargo fmt` silently skips it, so run `cargo +nightly fmt`.) Omit any block that has no imports (hence "up to three") — never leave an empty block.

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Notify, mpsc, oneshot};

use crabka_log::{Log, ReadOutput};
use crabka_protocol::records::RecordBatch;

use crate::error::BrokerError;
use crate::replica_state::ReplicaState;
```

**Collapse imports that share a leading path** into a single nested statement rather than repeating that prefix across lines: factor out the shared part and group the divergent tails in braces.

```rust
// preferred
use crate::{error::BrokerError, replica_state::ReplicaState};

// avoid — repeats the `crate` prefix
use crate::error::BrokerError;
use crate::replica_state::ReplicaState;
```

This is the rustfmt `imports_granularity = "Crate"` layout, also set in `rustfmt.toml` and applied by `cargo +nightly fmt`, so the merging is automatic — you don't have to hand-collapse imports, and you shouldn't hand-expand them either. Avoid the opposite, Java-style extreme of giving every item its own fully-qualified line.

**Avoid glob imports** (`use some_crate::*;`). Pulling every item from a crate into scope hides where each name comes from: when a dependency is later updated and removes or renames an item, the compiler reports an undefined symbol with no hint which glob it came from, and a newly-added upstream item can silently clash with a name from another glob. Explicit imports keep that traceable for the next maintainer.

`use super::*;` is the one sanctioned glob, and even then it is the exception, not the default. Reach for it only in a **leaf module** — where one logical module has been split across several deeply-interrelated files that legitimately share the parent's imports — or in a **unit-test module**, where `#[cfg(test)] mod tests { use super::*; … }` is the well-defined idiom (see [Tests](#tests)). In ordinary submodules, list imports explicitly rather than glob-importing the parent.

**Don't glob enum variants into scope** (`use SomeEnum::*;`) — it hides the variants' type and can obscure that an enum is being handled at all. This matters most for the generated protocol enums (`ApiKey`, error codes): spell out `ApiKey::Produce` in full, including in `match` arms. The visible type is the point, not noise.

**Keep `use` at module scope.** Imports belong at the top of the file (or `mod` block), never inside a function body. A reader should find every path a function depends on in one place, and a function-local `use` — an alias especially — hides where a name really comes from.

**Name child modules through `self::`** when importing or re-exporting from them (`use self::foo::Foo;`, `pub use self::foo::Foo;`). The explicit `self::` distinguishes the child module from a same-named dependency, so introducing a crate called `foo` later cannot silently change what the path resolves to.

```rust
mod config;
mod error;

pub use self::config::Config;
pub use self::error::Error;
```

## Visibility

Control an item's visibility **at its own definition**, using `pub` or `pub(crate)` on the item. Do not widen or restrict visibility indirectly through re-exports or wrapper modules elsewhere — the `pub` keyword on the declaration is the single source of truth for how visible something is.

Inside a module that is itself private or `pub(crate)`, write plain `pub`, not `pub(crate)`. The enclosing module already caps visibility, so `pub(crate)` is redundant noise (the workspace does not enable `unreachable_pub`).

Re-export public API at the crate root where it aids discoverability — Crabka crates commonly re-export their error type and primary types from `lib.rs` — but document each item at its definition, not at the re-export.

## Error Handling

Crates and subsystems define their own error enums with [`thiserror`](https://docs.rs/thiserror), named `<Subsystem>Error` and re-exported from the crate root (`BrokerError`, `RaftError`, `ProtocolError`, `MetadataError`, `LogError`):

```rust
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RaftError {
    #[error("storage: {0}")]
    Storage(#[from] crabka_log::LogError),

    /// The node received a write but is not the current leader.
    #[error("not leader; current leader: {current_leader:?}")]
    NotLeader { current_leader: Option<NodeId> },
}
```

- Give every variant an `#[error("…")]` message. Add a `///` doc line where the variant's meaning is not already obvious from that message.
- Mark public error enums `#[non_exhaustive]` so a new variant is not a breaking change (Crabka is pre-1.0, but this keeps error matching honest about the open set).
- Prefer **focused, leaf error types** for self-contained sub-parsers over reusing a crate-root `Error`. A small decoder returning the whole crate's error enum leaks unrelated variants into its signature.
- Use `?` for propagation. Convert between error types with `#[from]` or explicit `map_err`, not by stringifying.
- Avoid `.unwrap()` / `.expect()` on fallible paths in library code. They are acceptable only where an invariant guarantees success — in which case use `.expect("reason the invariant holds")` so the message documents the invariant. Tests may unwrap freely.
- Never `panic!` in response to malformed wire input — decoders return errors. This is a security property, verified by property tests and fuzzing (see [Wire-Format Safety](#wire-format-safety)).

Note that `missing_errors_doc` and `missing_panics_doc` are relaxed in the workspace lints, so `# Errors` / `# Panics` rustdoc sections are *encouraged where they add value* rather than lint-enforced — see the [rustdoc guide](rustdoc_style_guide.md).

## Wire-Format Safety

Crabka forbids `unsafe` workspace-wide (`unsafe_code = "forbid"`), so there are no raw-pointer or transmute footguns to guard. The safety concern that remains is **untrusted wire input**: a Crabka broker decodes bytes sent by arbitrary Kafka clients, and a malicious or buggy client can put any value in a length or count field.

**Never size an allocation directly from a wire-supplied length or element count.** A `Vec::with_capacity(n)` where `n` came off the wire is a denial-of-service vector — one small request claiming a four-billion-element array can exhaust memory. Validate the value against a sane bound first, and convert with `try_from` (handling the error) rather than `as`:

```rust
if count > MAX_ARRAY_LEN {
    return Err(ProtocolError::ArrayTooLong(count));
}
let count = usize::try_from(count).map_err(|_| ProtocolError::ArrayTooLong(count))?;
```

A silent `as usize` truncation, or an unbounded `with_capacity`, on client-controlled input is a parsing bug and a potential vulnerability. Decoders return errors on such input; they do not panic and do not allocate on the attacker's say-so.

## Newtypes for Domain Values

A raw `i32` or `i64` carries no meaning, and Kafka's domain is full of same-typed identifiers that are catastrophic to mix up. A broker id, a partition index, a leader epoch, a producer epoch, and a correlation id are all `i32`; an offset, a producer id, and a log-start offset are all `i64`. A function that takes two or three of these as bare integers can be called with the arguments transposed and it still compiles — the bug only surfaces at run time, as data routed to the wrong partition or an offset compared against an epoch.

**Wrap a domain value in a newtype when confusing it with another value of the same primitive type would be a real bug.** The compiler then rejects the transposition at the call site instead of letting it become a production incident. This follows the [newtype-safety guidance](https://github.com/leonardomso/rust-skills/blob/master/rules/api-newtype-safety.md) and the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/); Crabka already does it for `NodeId`, `ApiKey`, and topic ids (`Uuid`), and this section makes it the default for new domain identifiers.

```rust
use derive_more::{Add, Display, From, Into, Sub};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, From, Into)]
pub struct BrokerId(pub i32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, Add, Sub)]
pub struct Offset(pub i64);

// Signature that no longer accepts a transposed call:
fn assign_replica(partition: PartitionIndex, broker: BrokerId) { /* … */ }
```

Guidance:

- **Where the confusion risk is real, prefer a newtype over a bare primitive** — most acutely for functions and constructors that take **two or more parameters of the same primitive type** with different meanings. That is the swap-bug shape the newtype exists to prevent.
- **Split the derives by origin.** The identity/ordering traits come from the standard library — `Copy, Clone, PartialEq, Eq, Hash` for anything used as a `HashMap` key, plus `PartialOrd, Ord` for ordered values like offsets and epochs. Keep id newtypes `Copy` (see [own-copy-small](https://github.com/leonardomso/rust-skills/blob/master/rules/own-copy-small.md)).
- **Use [`derive_more`](https://lib.rs/crates/derive_more) for the wrapper boilerplate** — do not hand-write these impls. It is a workspace dependency (`derive_more.workspace = true`); reach for:
  - `Display` — so the value logs as the bare inner primitive without an explicit `.0` at every `tracing` call. Write a manual `impl Display` only when the value needs formatting (e.g. `ORD-{:08}`).
  - `From` / `Into` — the explicit, visible conversions to and from the inner primitive, used at the [wire boundary](#newtypes-for-domain-values) (`BrokerId::from(raw)`, `let raw: i32 = id.into()`).
  - `FromStr` — for ids parsed from config or CLI args.
  - `Deref` / `AsRef` — sparingly, only where transparent access to the inner value genuinely reads better; a broad `Deref` can undermine the type distinction the newtype exists for, so prefer an explicit accessor or `Into` in most cases.
  - `Add` / `Sub` / `AddAssign` / `Sum` — for values with real arithmetic (advancing an `Offset`, summing byte counts). Don't derive arithmetic on ids where `id + id` is meaningless.
  - `Constructor` — a `Foo::new(inner)` when you want a named constructor but no validation.

  ```rust
  use derive_more::{Add, Display, From, FromStr, Into, Sub};

  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, From, Into, FromStr)]
  pub struct ProducerId(pub i64);
  ```
- **Use `#[serde(transparent)]`** on newtypes that are serialised, so the wire/JSON encoding is exactly the inner primitive — never a wrapping object.
- **Comparison against the inner primitive is allowed; nothing else is relaxed.** The shared `crabka-ids` identifiers hand-implement `PartialEq`/`PartialOrd` against their inner primitive in both directions, so a value check like `offset >= 0` or `epoch == LeaderEpoch::UNKNOWN` reads without an explicit `.0`. This is deliberately narrow — it does **not** let the newtype be passed where its primitive is expected, keyed differently in a map, or compared to a *different* newtype, so the swap-bug safety is intact. Expose Kafka sentinels (`-1` unknown/none, `0` initial) as **named constants** (`LeaderEpoch::UNKNOWN`, `ProducerId::NONE`, `Offset::ZERO`) so the comparison reads as intent rather than a magic number.
- **Validate in the constructor** for newtypes over `String` or other unconstrained inputs (`ClientId`, a validated principal): expose `fn new(..) -> Result<Self, _>` and an `as_str`/accessor — and do **not** also derive `From`, since an infallible `From` would bypass the validation. An instance should be proof the value is well-formed. This is [parse, don't validate](https://github.com/leonardomso/rust-skills/blob/master/rules/api-parse-dont-validate.md).
- **The newtype is zero-cost** — same size and layout as the primitive — so there is no runtime reason to avoid one.

`derive_more` is also the right tool elsewhere it removes hand-written boilerplate — a `Display`/`From`/`Constructor` on a small domain struct, `From` conversions between related types — but it is not a licence to derive `Deref` broadly or to replace the `thiserror`-based error enums (those stay on `thiserror`; see [Error Handling](#error-handling)).

**The wire boundary is the exception.** The generated protocol codec (`crates/protocol/generated`) is produced from the Kafka schemas and must stay byte-exact — do **not** newtype generated message fields, and do not hand-edit generated code. Newtypes belong in the **hand-written domain layer** (broker, raft, metadata, coordination, storage). Convert at the boundary: read the raw integer out of a decoded request, wrap it in the domain newtype, and unwrap back to the primitive when encoding a response. A `From`/`Into` or an `as_wire()` accessor keeps that conversion explicit and in one place.

**Don't newtype for its own sake.** A value used in a single place, where no other same-typed value is in scope to confuse it with, does not need a wrapper — `struct X(i32)` around a lone loop counter is noise. The test is whether a *mix-up is possible and would be a bug*, not whether a primitive appears.

## Feature Flags

Crabka is not `no_std` — the broker and services run on `std`, and the client crates target `std` hosts. Two portability constraints do exist and must be preserved: `crabka-voters` (and the `crabka-playground` consensus demo built on it) is kept crypto-free so the consensus core compiles to WebAssembly. Don't pull `std`-only or native-only dependencies into those crates.

For feature flags generally:

- Features must be **additive** — enabling one must not break or change the behaviour of another. Code must compile under the feature combinations CI exercises and with default features.
- Gate optional integrations (compression codecs, columnar/dataframe support, backend adapters) behind features rather than making them mandatory dependencies.
- Document every feature flag in the crate-level doc comment (see the [rustdoc guide](rustdoc_style_guide.md)), including which are on by default.

## Comments

- Doc comments (`///`, `//!`) document the public API — see the [rustdoc guide](rustdoc_style_guide.md). Public items should carry them; private and `pub(crate)` items do not need them.
- Use `//` line comments for non-obvious private logic and for surprising trait-impl behaviour.
- **Comments describe the present state of the code, not its history.** No "moved from X", "replaces the old Y", "now takes Z" porting narration — git holds that history, and stale migration notes mislead. (Crabka is greenfield and undeployed; there is no old version to reference — see [`CLAUDE.md`](../../CLAUDE.md).)
- Explain *why*, not *what*, when the *what* is already clear from the code. A comment tying a decode branch to a specific KIP or Kafka version quirk earns its place; one restating the `if` condition does not.

## Async and Concurrency

Crabka is built on `tokio` (pinned in `[workspace.dependencies]`).

- Prefer the `tokio::sync` primitives — `mpsc`, `oneshot`, `Notify`, `RwLock`, and the async `Mutex` — for anything that coordinates across `.await` points.
- **Never hold a blocking lock across an `.await`.** `std::sync::Mutex` is fine for an O(1) synchronous critical section on a hot path (Crabka uses it that way to guard a partition's log), but if the critical section awaits or does real work, use `tokio::sync::Mutex`. Holding a `std` lock across an await can deadlock the runtime.
- Structure long-lived work as tasks with explicit shutdown, not detached fire-and-forget. The **single-writer-task** pattern — one task owns a resource and drains an `mpsc` channel of commands, callers send messages and await a `oneshot` reply — is the established shape for serialising writes (see the partition writer); match it for new serialised-mutation components.
- Spawn blocking or CPU-bound work (compression, checksum, Parquet encode) with `spawn_blocking` so it doesn't stall the async worker threads.

## Logging and Observability

- Use the `tracing` macros (`tracing::debug!`, `error!`, `instrument`) for logging and spans — not `println!` / `eprintln!` in library or server code.
- Services install the shared JSON formatter from `crabka-logfmt` so log output is structured for downstream ingestion. Don't hand-roll a formatter in a service.
- Keep log levels meaningful: `error!` for faults needing attention, `warn!` for recoverable anomalies, `debug!`/`trace!` for diagnostics. Don't log per-record or per-request at `info!` on the produce/fetch hot path.

## Cargo and Dependencies

- Inherit shared package metadata from the workspace (`edition`, `rust-version`, `license`, `authors`) with `.workspace = true`, and inherit lint policy with `[lints] workspace = true`.
- Crabka **does** use a `[workspace.dependencies]` table. Declare a shared dependency and its version/features there once, and reference it from a crate with `<dep>.workspace = true`. This keeps versions consistent across 50-plus crates and is where cross-crate version pins live.
- Several pins are **lock-stepped** and carry an explanatory comment in the workspace `Cargo.toml` (the `datafusion` git revision with `arrow` / `parquet` / `object_store`; `polars` with `polars-arrow`). Do not bump one of these alone — read the comment and move the whole set together. Renovate is configured to hold them.
- Keep dependencies minimal and justified; new third-party crates pass through the `cargo deny` gate (`deny.toml`) and the security-audit CI workflow. Check a sibling crate or the lockfile before introducing a new version of something already in the tree.

## Tests

Where a test lives depends on what it needs to reach:

- **Integration tests — the default for public behaviour.** If a test exercises a crate's public API, put it in the crate's `tests/` directory (a sibling of `src/`). These compile as separate crates and can only see the public surface, which keeps them honest about what the crate actually exposes.
- **In-file unit tests — only for private access.** Add a `#[cfg(test)] mod tests { use super::*; … }` block at the bottom of a source file *only* when the test needs access to private module or file internals that are not (and should not be) public. This is the one place `use super::*;` is expected. When such a test module grows large relative to the source it covers, move it into a dedicated `tests.rs` file in the module (declared with `#[cfg(test)] mod tests;`) rather than letting it dominate the source file; as a child module it retains the same private access.

Conventions for both:

- Test functions are `snake_case` and name the scenario under test. Do not add rustdoc to test functions or test helpers.
- Use [`assert2`](https://docs.rs/assert2) (`assert!` / `check!`) for assertions — it is the workspace-wide standard and gives captured-expression diagnostics on failure.
- Wire-facing and codec code carries **property tests** (`proptest`) — typically encode/decode round-trips — and file-backed corpora run through [`datatest-stable`](https://docs.rs/datatest-stable) so each fixture is scheduled as its own test process. New parsing code should come with round-trip coverage.
- Use `mockall` (behind `#[cfg_attr(test, …)]`) to mock trait seams so IO-decision logic is unit-testable without a live broker or quorum. Consensus-correctness properties are model-checked with `stateright` in dev-only tests.
- Broker/client behaviour that must match the JVM is checked by **differential tests against a real Kafka oracle**; these are `#[ignore]`d by default and run with `--include-ignored` / `-- --ignored` (they need the JVM oracle or `testcontainers`). See [`CONTRIBUTING.md`](../../CONTRIBUTING.md) for the commands.
- The test suite runs under `cargo nextest`, and CI enforces **mutation testing** (`cargo mutants --in-diff`) on changed lines: a surviving mutant means a changed line runs but no test asserts on its behaviour. Write the assertion that kills it.

See [`CONTRIBUTING.md`](../../CONTRIBUTING.md) for how to run each tier (workspace tests, JVM-differential, mutation) and the [coverage report guide](coverage_report_style_guide.md) for how coverage is reported per crate.

## Markdown and Prose (for docs you write)

- **One line per paragraph — do not hard-wrap at 80 columns.** Let the renderer wrap. (Some older Crabka docs are hard-wrapped; that is not a reason to reflow them, but new prose should follow this rule.)
- Follow the relevant guide for the document type: [design docs](design_doc_style_guide.md), [READMEs](readme_style_guide.md), [coverage reports](coverage_report_style_guide.md).
