# Chapter Gres G-1: Vendor the crabgresql Engine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Vendor the crabgresql engine into the Crabka workspace as ten `crabka-pg*`/`crabka-gres*` crates, with the conformance corpus + oracle harness gating parity against a recorded donor baseline in CI, and a `crabka-gres` binary serving a single tenant on local fjall.

**Architecture:** One-time source import from the donor repo at a pinned SHA. Batch 0 lays down all ten crate skeletons with their *final* manifests plus every new workspace dependency, so `Cargo.toml`/`Cargo.lock` are settled once and the per-crate import tasks touch only their own crate directory (safe to run in parallel). Each crate task copies donor sources, applies the crate-rename sed, satisfies workspace pedantic lints, and lands with its donor test suite green. New code is limited to: a `--baseline` parity gate in the conformance harness, the `crabka-gres` binary (donor serve mode minus the cluster subcommand), a CLI smoke test, two smoke scripts, and CI wiring.

**Tech Stack:** Rust 1.96.0 (edition 2024), tokio, fjall (pure-Rust LSM), rustls + rustls-rustcrypto, cargo-nextest, cargo-llvm-cov, dorny/paths-filter CI, postgres:18 oracle container.

## Global Constraints

- **Donor pin:** all sources come from `https://github.com/robot-head/crabgresql` at commit `93f3d17168d056a28b4abe60af3b489d4bf62f1d`. The clone is read-only; never edit it.
- **Spec:** [docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md](../specs/2026-07-09-crabka-gres-chapter-design.md). G-1 gate: donor parity baseline reproduced in Crabka CI; `crabka-gres` serves a single tenant on local fjall.
- **No behavior changes.** Vendoring must not change observable engine behavior; the conformance baseline (exact statement count, match count ≥ donor's) is the arbiter. Lint fixes must be behavior-preserving.
- **Naming:** package `crabka-<name>`, directory `crates/<name>`, imports `crabka_<name>::`. Sibling path deps are declared `crabka-x = { version = "0.3.9", path = "../x" }` (broker style), never renamed.
- **Publish set:** `crabka-pgtypes`, `crabka-pgparser`, `crabka-pgwire`, `crabka-pgkv`, `crabka-pgmvcc`, `crabka-pgcatalog`, `crabka-pgexec` are published (allowlist + release-plz `publish = true`); `crabka-gres`, `crabka-gres-fdw`, `crabka-gres-conformance` are `publish = false`.
- **Lints:** workspace lints apply (`unsafe_code = "forbid"`, `clippy::pedantic` warn, CI runs `cargo clippy --workspace --all-targets -- -D warnings`). Pedantic cleanup policy: prefer a real fix; for a false positive or a fix that would risk behavior, use a narrowly scoped `#[expect(clippy::<lint>, reason = "…")]` on the item. Never add crate-level lint configuration.
- **Format:** `cargo +nightly fmt` (workspace rustfmt.toml: `group_imports = "StdExternalCrate"`, `imports_granularity = "Crate"`).
- **Tests:** run under `cargo nextest run -p <crate>`; doctests via `cargo test -p <crate> --doc` (donor crates have none). Vendored tests keep their assertion style (no `assert2` sweep); **new** tests use `assert2` and condition-driven bounded waits, never bare settle-sleeps.
- **Commits:** conventional commits. Per crate: `feat(<name>): vendor <donor-crate> from crabgresql@93f3d17` for the compiling import, then `chore(<name>): satisfy workspace pedantic lints` for the lint pass. CI/infra commits use `ci:` / `chore:`.
- **Donor clone location:** `/tmp/crabgresql-donor` (Task 1 creates it). Referenced below as `$DONOR`. Set `DONOR=/tmp/crabgresql-donor` in each shell.
- **Version string `0.3.9`** in sibling path deps and `html_root_url`s must match the current `[workspace.package] version` at execution time — if the workspace has bumped past 0.3.9, use the current value everywhere and run `tools/html-root-url.sh --fix`.

## Reference: crate mapping

| Donor crate | Crabka dir | Package | Publish | lib/bin |
|---|---|---|---|---|
| `crates/pgtypes` | `crates/pgtypes` | `crabka-pgtypes` | yes | lib |
| `crates/pgparser` | `crates/pgparser` | `crabka-pgparser` | yes | lib |
| `crates/pgwire` | `crates/pgwire` | `crabka-pgwire` | yes | lib |
| `crates/kv` | `crates/pgkv` | `crabka-pgkv` | yes | lib |
| `crates/mvcc` | `crates/pgmvcc` | `crabka-pgmvcc` | yes | lib |
| `crates/catalog` | `crates/pgcatalog` | `crabka-pgcatalog` | yes | lib |
| `crates/executor` | `crates/pgexec` | `crabka-pgexec` | yes | lib |
| `crates/kafka_fdw` | `crates/gres-fdw` | `crabka-gres-fdw` | no | lib |
| `crates/conformance` | `crates/gres-conformance` | `crabka-gres-conformance` | no | lib + 2 bins |
| `crates/crabgresql` (serve mode only) | `crates/gres` | `crabka-gres` | no | bin |

**NOT vendored:** `crates/cluster` (openraft/2PC — the substrate replaces it), the donor bin's `node` subcommand, `fuzz/`.

## Reference: the rename sed

Run inside a vendored crate directory after copying sources. Order matters (`crabka_pgkv::` contains `kv::` but `\b` guards it — verify with the grep below anyway):

```bash
find src tests -name '*.rs' 2>/dev/null | xargs -r sed -i \
  -e 's/\bpgtypes::/crabka_pgtypes::/g' \
  -e 's/\bpgparser::/crabka_pgparser::/g' \
  -e 's/\bpgwire::/crabka_pgwire::/g' \
  -e 's/\bkv::/crabka_pgkv::/g' \
  -e 's/\bmvcc::/crabka_pgmvcc::/g' \
  -e 's/\bcatalog::/crabka_pgcatalog::/g' \
  -e 's/\bexecutor::/crabka_pgexec::/g' \
  -e 's/\bkafka_fdw::/crabka_gres_fdw::/g'
# Sanity: no double-renames or misses.
grep -rn 'crabka_crabka\|crabka_pgcrabka' src tests && echo "BAD DOUBLE RENAME" && exit 1
grep -rnE '^use (pgtypes|pgparser|pgwire|kv|mvcc|catalog|executor|kafka_fdw)::' src tests && echo "MISSED RENAME" && exit 1
echo "rename clean"
```

`use crate::…` and field accesses like `self.kv` are untouched by design. Prose mentions of donor crate names in comments may rename too — that is fine.

## Reference: README template

Published crates (badges included); internal crates drop the badge lines. Substitute `{CRATE}`, `{ONELINER}`, `{OVERVIEW}` from the table in each task.

```markdown
# {CRATE}

[![crates.io](https://img.shields.io/crates/v/{CRATE}.svg)](https://crates.io/crates/{CRATE})
[![docs.rs](https://docs.rs/{CRATE}/badge.svg)](https://docs.rs/{CRATE})

{ONELINER}

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres — a pure-Rust
Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`; see the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

{OVERVIEW}

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
```

---

## Batch 0 — workspace scaffolding (serial; everything else depends on it)

### Task 1: Donor clone, workspace deps, crate skeletons, NOTICE, mutants excludes

Settles every shared file (`Cargo.toml`, `Cargo.lock`, `NOTICE`, `.cargo/mutants.toml`) once, so Tasks 2–11 never touch a shared file and parallel batches cannot conflict.

**Files:**
- Create: `/tmp/crabgresql-donor` (clone, outside the repo)
- Modify: `Cargo.toml` (workspace deps), `NOTICE`, `.cargo/mutants.toml`, `Cargo.lock` (regenerated)
- Create: `crates/{pgtypes,pgparser,pgwire,pgkv,pgmvcc,pgcatalog,pgexec,gres,gres-fdw,gres-conformance}/Cargo.toml` + stub `src/` files

**Interfaces:**
- Consumes: nothing.
- Produces: final manifests for all ten crates (later tasks replace stub sources only); workspace dep entries `fjall`, `jiff`, `bigdecimal`, `dashu-float`, `num-bigint`, `rand`, `rustls-rustcrypto`, `rustls-pemfile`, `pg_query`, `sqlx`.

- [ ] **Step 1: Clone the donor at the pinned SHA**

```bash
git clone https://github.com/robot-head/crabgresql /tmp/crabgresql-donor
git -C /tmp/crabgresql-donor checkout 93f3d17168d056a28b4abe60af3b489d4bf62f1d
```

Expected: `HEAD is now at 93f3d17 …`. (A clone may already exist at the session scratchpad's `crabgresql/` dir — reuse only if `git rev-parse HEAD` matches.)

- [ ] **Step 2: Add the new external workspace dependencies**

In the root `Cargo.toml` `[workspace.dependencies]` section, append (keep alphabetical-ish grouping with a comment header):

```toml
# Chapter Gres — vendored crabgresql engine (crabgresql@93f3d17).
bigdecimal = "0.4"
dashu-float = "0.4"
fjall = "3.1.5"
jiff = { version = "0.2", features = ["tzdb-bundle-always"] }
num-bigint = "0.4"
# pg_query wraps libpg_query (C): dev/oracle-only, optional, never default —
# the shipped gres tree stays pure Rust.
pg_query = "6"
rand = "0.10"
rustls-pemfile = "2"
rustls-rustcrypto = "0.0.2-alpha"
sqlx = { version = "0.9.0", default-features = false, features = ["postgres", "runtime-tokio"] }
```

(`apache-avro`, `prost-reflect`, `protox`, `serde_json`, `bytes`, `thiserror`, `tokio`, `tokio-util`, `tokio-postgres`, `tracing`, `tracing-subscriber`, `rustls`, `tokio-rustls`, `sha2`, `hmac`, `pbkdf2`, `base64`, `subtle`, `zerocopy`, `serde`, `async-trait`, `clap`, `proptest`, `tempfile`, `assert2` already exist — do not duplicate. `rand` is genuinely absent today; verify with `grep -n '^rand' Cargo.toml` first and skip if it has appeared.)

- [ ] **Step 3: Create the ten crate skeletons with final manifests**

For each crate below: `mkdir -p crates/<dir>/src`, write the manifest exactly, and write stub sources (`src/lib.rs` containing `//! Populated by the Chapter Gres G-1 vendoring tasks.` for libs; `fn main() {}` in `src/main.rs` for bins). `crates/gres-conformance` additionally needs `mkdir -p crates/gres-conformance/src/bin` and stub `src/bin/record.rs` (`fn main() {}`); `crates/gres` has no lib, only `src/main.rs`.

`crates/pgtypes/Cargo.toml`:

```toml
[package]
name = "crabka-pgtypes"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "PostgreSQL value layer for the Crabka Gres engine: Datum, column types, text and binary wire encodings, casts, and operator semantics"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pgtypes"
readme = "README.md"
keywords = ["postgres", "types", "datum", "crabka", "gres"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
thiserror = { workspace = true }
bigdecimal = { workspace = true }
dashu-float = { workspace = true }
jiff = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
```

`crates/pgparser/Cargo.toml`:

```toml
[package]
name = "crabka-pgparser"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Hand-written PostgreSQL SQL lexer and parser producing the Crabka Gres AST"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pgparser"
readme = "README.md"
keywords = ["postgres", "sql", "parser", "crabka", "gres"]
categories = ["database-implementations", "parser-implementations"]

[lints]
workspace = true

[features]
# Differential accept/reject oracle vs libpg_query (C build dep) — dev/CI only,
# never a default: the shipped tree stays pure Rust.
oracle = ["dep:pg_query"]

[dependencies]
thiserror = { workspace = true }
crabka-pgtypes = { version = "0.3.9", path = "../pgtypes" }
pg_query = { workspace = true, optional = true }

[dev-dependencies]
proptest = { workspace = true }
```

`crates/pgwire/Cargo.toml`:

```toml
[package]
name = "crabka-pgwire"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "PostgreSQL v3 wire-protocol server: simple and extended query protocols, SCRAM-SHA-256, TLS, and CancelRequest"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pgwire"
readme = "README.md"
keywords = ["postgres", "wire-protocol", "scram", "crabka", "gres"]
categories = ["database-implementations", "network-programming"]

[lints]
workspace = true

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "io-util", "time", "sync"] }
tokio-util = { workspace = true }
tracing = { workspace = true }
rustls = { workspace = true }
tokio-rustls = { workspace = true }
sha2 = { workspace = true }
hmac = { workspace = true }
pbkdf2 = { workspace = true }
base64 = { workspace = true }
rand = { workspace = true }
subtle = { workspace = true }

[dev-dependencies]
tokio-postgres = { workspace = true }
proptest = { workspace = true }
rustls-rustcrypto = { workspace = true }
rustls-pemfile = { workspace = true }
tokio = { workspace = true, features = ["full"] }
sqlx = { workspace = true }
```

`crates/pgkv/Cargo.toml`:

```toml
[package]
name = "crabka-pgkv"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Ordered key-value storage seam for the Crabka Gres engine with order-preserving key encoding and versioned row encoding"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pgkv"
readme = "README.md"
keywords = ["postgres", "storage", "kv", "crabka", "gres"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
thiserror = { workspace = true }
crabka-pgtypes = { version = "0.3.9", path = "../pgtypes" }
serde = { workspace = true }
fjall = { workspace = true }
zerocopy = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
tempfile = { workspace = true }
jiff = { workspace = true }
```

`crates/pgmvcc/Cargo.toml`:

```toml
[package]
name = "crabka-pgmvcc"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "PostgreSQL-faithful MVCC for the Crabka Gres engine: xids, clog, snapshots, and HeapTupleSatisfiesMVCC visibility"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pgmvcc"
readme = "README.md"
keywords = ["postgres", "mvcc", "transactions", "crabka", "gres"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
thiserror = { workspace = true }
crabka-pgkv = { version = "0.3.9", path = "../pgkv" }
crabka-pgtypes = { version = "0.3.9", path = "../pgtypes" }
zerocopy = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
```

`crates/pgcatalog/Cargo.toml`:

```toml
[package]
name = "crabka-pgcatalog"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "System catalog for the Crabka Gres engine: tables, columns, and FDW metadata as a stateless view over the KV storage seam"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pgcatalog"
readme = "README.md"
keywords = ["postgres", "catalog", "crabka", "gres"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
thiserror = { workspace = true }
crabka-pgtypes = { version = "0.3.9", path = "../pgtypes" }
crabka-pgkv = { version = "0.3.9", path = "../pgkv" }
zerocopy = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
tempfile = { workspace = true }
```

`crates/pgexec/Cargo.toml`:

```toml
[package]
name = "crabka-pgexec"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "SQL execution engine for Crabka Gres: turns parsed SQL into catalog and KV operations under MVCC and implements the pgwire Engine trait"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pgexec"
readme = "README.md"
keywords = ["postgres", "sql", "executor", "crabka", "gres"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
thiserror = { workspace = true }
crabka-pgtypes = { version = "0.3.9", path = "../pgtypes" }
crabka-pgkv = { version = "0.3.9", path = "../pgkv" }
crabka-pgmvcc = { version = "0.3.9", path = "../pgmvcc" }
crabka-pgcatalog = { version = "0.3.9", path = "../pgcatalog" }
crabka-pgparser = { version = "0.3.9", path = "../pgparser" }
crabka-pgwire = { version = "0.3.9", path = "../pgwire" }
bytes = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "io-util", "time", "sync"] }
async-trait = { workspace = true }
zerocopy = { workspace = true }
bigdecimal = { workspace = true }
jiff = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["full"] }
tokio-postgres = { workspace = true }
tempfile = { workspace = true }
```

`crates/gres-fdw/Cargo.toml` (feature gate removed — inside this workspace the Kafka client crates are siblings and pure-Rust is ambient, per the chapter design):

```toml
[package]
name = "crabka-gres-fdw"
publish = false
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Foreign-data wrapper exposing Kafka topics as SQL tables inside Crabka Gres tenant databases"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-gres-fdw"
readme = "README.md"
keywords = ["kafka", "postgres", "fdw", "crabka", "gres"]
categories = ["database", "asynchronous"]

[lints]
workspace = true

[dependencies]
crabka-pgcatalog = { version = "0.3.9", path = "../pgcatalog" }
crabka-pgtypes = { version = "0.3.9", path = "../pgtypes" }
crabka-pgexec = { version = "0.3.9", path = "../pgexec" }
crabka-pgkv = { version = "0.3.9", path = "../pgkv" }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "io-util", "time", "sync"] }
thiserror = { workspace = true }
crabka-client-core = { version = "0.3.9", path = "../client-core" }
crabka-client-admin = { version = "0.3.9", path = "../client-admin" }
crabka-protocol = { version = "0.3.9", path = "../protocol" }
crabka-schema-serde = { version = "0.3.9", path = "../schema-serde", features = ["avro", "json", "protobuf"] }
crabka-security = { workspace = true }
apache-avro = { workspace = true }
serde_json = { workspace = true }
rustls = { workspace = true }
rustls-rustcrypto = { workspace = true }
jiff = { workspace = true }
bigdecimal = { workspace = true }
num-bigint = { workspace = true }
prost-reflect = { workspace = true }

[dev-dependencies]
protox = { workspace = true }
crabka-broker = { version = "0.3.9", path = "../broker" }
crabka-schema-registry = { version = "0.3.9", path = "../schema-registry" }
crabka-client-producer = { version = "0.3.9", path = "../client-producer" }
crabka-pgwire = { version = "0.3.9", path = "../pgwire" }
tokio-postgres = { workspace = true }
tokio-util = { workspace = true }
tempfile = { workspace = true }
bytes = { workspace = true }
```

(Note vs donor: the `kafka` feature and all `optional = true` markers are gone; the unused `crabka-client-consumer` dev-dep is dropped; `crabka-security` uses the existing workspace entry. If `crabka-broker`/`crabka-schema-registry` manifests declare `publish = false` without a `version` field, drop the `version = "0.3.9"` from those two dev-dep lines and keep only `path`.)

`crates/gres-conformance/Cargo.toml`:

```toml
[package]
name = "crabka-gres-conformance"
publish = false
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Differential conformance harness diffing Crabka Gres against a real PostgreSQL oracle over the wire"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-gres-conformance"
readme = "README.md"
keywords = ["postgres", "conformance", "testing", "crabka", "gres"]
categories = ["development-tools::testing"]

[lints]
workspace = true

[[bin]]
name = "crabka-gres-record"
path = "src/bin/record.rs"

[dependencies]
tokio = { workspace = true, features = ["full"] }
tokio-postgres = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
clap = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
```

(The default bin auto-discovers `src/main.rs` as `crabka-gres-conformance`. This crate's skeleton needs three stub sources: `src/lib.rs`, `src/main.rs` with `fn main() {}`, and `src/bin/record.rs` with `fn main() {}`.)

`crates/gres/Cargo.toml`:

```toml
[package]
name = "crabka-gres"
publish = false
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Crabka Gres service: a pure-Rust Postgres-compatible tenant compute for the Crabka substrate"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-gres"
readme = "README.md"
keywords = ["postgres", "database", "serverless", "crabka", "gres"]
categories = ["database-implementations"]

[lints]
workspace = true

[[bin]]
name = "crabka-gres"
path = "src/main.rs"

[dependencies]
crabka-pgexec = { version = "0.3.9", path = "../pgexec" }
crabka-pgwire = { version = "0.3.9", path = "../pgwire" }
crabka-gres-fdw = { path = "../gres-fdw" }
clap = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net"] }
tracing-subscriber = { workspace = true }
rustls = { workspace = true }
tokio-rustls = { workspace = true }
rustls-pemfile = { workspace = true }
rustls-rustcrypto = { workspace = true }
rand = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
tokio = { workspace = true, features = ["full"] }
tokio-postgres = { workspace = true }
```

- [ ] **Step 4: Append the crabgresql entry to `NOTICE`**

Append (copy the two liability/disclaimer paragraphs verbatim from `$DONOR/LICENSE` — do not retype them):

```
crabgresql
Copyright (c) 2026, Matthew Stone

The Chapter Gres crates (crates/pgtypes, crates/pgparser, crates/pgwire,
crates/pgkv, crates/pgmvcc, crates/pgcatalog, crates/pgexec, crates/gres,
crates/gres-fdw, crates/gres-conformance) are derived from the crabgresql
project (https://github.com/robot-head/crabgresql), imported at commit
93f3d17168d056a28b4abe60af3b489d4bf62f1d under the PostgreSQL License:

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement
is hereby granted, provided that the above copyright notice and this
paragraph and the following two paragraphs appear in all copies.

<the two remaining paragraphs from $DONOR/LICENSE, verbatim>
```

- [ ] **Step 5: Exclude the vendored crates from cargo-mutants**

In `.cargo/mutants.toml`, append to the existing `exclude_globs` array (before its closing `]`):

```toml
    # Chapter Gres vendored crates (crabgresql@93f3d17): imported wholesale in
    # the G-1 vendoring PR; mutating ~68k imported lines under --in-diff would
    # swamp the shard budget. Re-evaluate per-crate once gres development is
    # incremental.
    "crates/pgtypes/**",
    "crates/pgparser/**",
    "crates/pgwire/**",
    "crates/pgkv/**",
    "crates/pgmvcc/**",
    "crates/pgcatalog/**",
    "crates/pgexec/**",
    "crates/gres/**",
    "crates/gres-fdw/**",
    "crates/gres-conformance/**",
```

- [ ] **Step 6: Build, lint, format the skeleton workspace**

```bash
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --all -- --check
```

Expected: all green (empty stubs compile; `Cargo.lock` gains fjall/jiff/bigdecimal/dashu/num-bigint/rand/rustls-rustcrypto/rustls-pemfile/pg_query/sqlx entries). If `fmt --check` flags the stubs, run `cargo +nightly fmt --all` and re-check.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock NOTICE .cargo/mutants.toml crates/pgtypes crates/pgparser crates/pgwire crates/pgkv crates/pgmvcc crates/pgcatalog crates/pgexec crates/gres crates/gres-fdw crates/gres-conformance
git commit -m "feat(gres): scaffold Chapter Gres crates and workspace deps

Skeleton manifests for the ten vendored crates, new workspace dependencies,
NOTICE provenance for crabgresql@93f3d17, and cargo-mutants excludes for the
vendoring PR.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 1 — leaf crates (run Tasks 2, 3, 4 in parallel)

### Task 2: Vendor `crabka-pgtypes`

**Files:**
- Replace: `crates/pgtypes/src/` (from `$DONOR/crates/pgtypes/src/`)
- Create: `crates/pgtypes/tests/` (from donor), `crates/pgtypes/README.md`

**Interfaces:**
- Consumes: nothing (leaf crate).
- Produces: `crabka_pgtypes::{Datum, ColumnType, …}` — donor public API unchanged; every later engine crate imports it as `crabka_pgtypes::`.

- [ ] **Step 1: Copy sources and tests**

```bash
rm -rf crates/pgtypes/src
cp -r $DONOR/crates/pgtypes/src crates/pgtypes/
cp -r $DONOR/crates/pgtypes/tests crates/pgtypes/
```

- [ ] **Step 2: Apply the rename sed** (see "Reference: the rename sed"; run from `crates/pgtypes/`). Expected: `rename clean`. (The oracle test `tests/numeric_transcendental_oracle.rs` imports `pgtypes::numeric` — the sed covers it.)

- [ ] **Step 3: Add the docs.rs root URL** — insert immediately after the crate-level `//!` block at the top of `crates/pgtypes/src/lib.rs` (matching the placement in `crates/protocol/src/lib.rs`):

```rust
#![doc(html_root_url = "https://docs.rs/crabka-pgtypes/0.3.9")]
```

- [ ] **Step 4: Format, build, and commit the import**

```bash
cargo +nightly fmt -p crabka-pgtypes
cargo check -p crabka-pgtypes --all-targets
git add crates/pgtypes && git commit -m "feat(pgtypes): vendor pgtypes from crabgresql@93f3d17

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

Expected: `cargo check` green (pedantic warnings are allowed at this commit; `-D warnings` comes next).

- [ ] **Step 5: Pedantic lint pass**

```bash
cargo clippy -p crabka-pgtypes --all-targets -- -D warnings
```

Fix per the Global Constraints lint policy until clean. This crate is numeric/datetime-heavy — expect `clippy::cast_possible_truncation`/`cast_sign_loss` sites; where a cast is provably in-range, prefer `usize::try_from(x).expect("reason")` or a scoped `#[expect(clippy::cast_possible_truncation, reason = "…")]`; never change numeric behavior to appease a lint.

- [ ] **Step 6: Run the crate tests**

```bash
cargo nextest run -p crabka-pgtypes
cargo test -p crabka-pgtypes --doc
```

Expected: all tests pass, zero failures; the 2 `#[ignore]` oracle tests in `tests/numeric_transcendental_oracle.rs` stay skipped (they shell out to a local Windows psql — they remain local-only tooling; do not wire them into CI).

- [ ] **Step 7: Write `crates/pgtypes/README.md`** from the README template with:
  - `{CRATE}` = `crabka-pgtypes`
  - `{ONELINER}` = "PostgreSQL value layer for the Crabka Gres engine: `Datum`, column types, text and binary wire encodings, casts, and operator semantics."
  - `{OVERVIEW}` = "Implements the PostgreSQL-faithful value semantics the Gres engine executes over: the `Datum` and `ColumnType` enums, numeric (arbitrary precision) and date/time arithmetic, cast rules, operator semantics, and both wire encodings. It is the root of the engine crate graph and has no sibling dependencies."

- [ ] **Step 8: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-pgtypes
git add crates/pgtypes && git commit -m "chore(pgtypes): satisfy workspace pedantic lints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 3: Vendor `crabka-pgwire`

**Files:**
- Replace: `crates/pgwire/src/` (from `$DONOR/crates/pgwire/src/`)
- Create: `crates/pgwire/tests/` **including `tests/fixtures/`** (PEM certs + `psql-select1.trace`), `crates/pgwire/README.md`

**Interfaces:**
- Consumes: nothing (standalone; defines the engine seam).
- Produces: `crabka_pgwire::engine::{Engine, Session}`, `crabka_pgwire::server::{serve, serve_tls, serve_conn}`, `crabka_pgwire::session::{SessionConfig, AuthMode}`, `crabka_pgwire::scram::ScramVerifier` — donor API unchanged. Task 9 (pgexec) implements `Engine`; Task 11 (gres bin) calls `serve_tls`.

- [ ] **Step 1: Copy sources, tests, and fixtures**

```bash
rm -rf crates/pgwire/src
cp -r $DONOR/crates/pgwire/src crates/pgwire/
cp -r $DONOR/crates/pgwire/tests crates/pgwire/
ls crates/pgwire/tests/fixtures/
```

Expected: fixtures listing includes `test-ca.pem`, `test-ca-key.pem`, `test-server.pem`, `test-server-key.pem`, `psql-select1.trace`.

- [ ] **Step 2: Apply the rename sed** (from `crates/pgwire/`; renames `pgwire::` self-references in tests). Expected: `rename clean`.

- [ ] **Step 3: Add the docs.rs root URL** after the `//!` block in `crates/pgwire/src/lib.rs`:

```rust
#![doc(html_root_url = "https://docs.rs/crabka-pgwire/0.3.9")]
```

- [ ] **Step 4: Format, build, commit the import**

```bash
cargo +nightly fmt -p crabka-pgwire
cargo check -p crabka-pgwire --all-targets
git add crates/pgwire && git commit -m "feat(pgwire): vendor pgwire from crabgresql@93f3d17

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Pedantic lint pass** — `cargo clippy -p crabka-pgwire --all-targets -- -D warnings`; fix per policy. SCRAM/protocol code: never weaken constant-time comparisons (`subtle`) or wire bounds-checks to satisfy a lint.

- [ ] **Step 6: Run the crate tests**

```bash
cargo nextest run -p crabka-pgwire
```

Expected: all pass — including `tls` (rustcrypto provider + PEM fixtures), `scram_auth`, `extended_query`, `cancel`, `golden_trace`, `simple_query`, `sqlx_driver` (both real drivers connect against the in-crate `StubEngine`).

- [ ] **Step 7: Write `crates/pgwire/README.md`** from the template with:
  - `{CRATE}` = `crabka-pgwire`
  - `{ONELINER}` = "PostgreSQL v3 wire-protocol server: simple and extended query protocols, SCRAM-SHA-256, TLS, and CancelRequest."
  - `{OVERVIEW}` = "A standalone pgwire server that any engine can sit behind via the `Engine`/`Session` traits: startup + SSLRequest negotiation (rustls), Trust and SCRAM-SHA-256 auth (RustCrypto, with anti-username-enumeration mock verifiers), Parse/Bind/Describe/Execute portals, per-column format codes, and CancelRequest semantics. Verified against tokio-postgres, sqlx, and recorded psql byte traces."

- [ ] **Step 8: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-pgwire
git add crates/pgwire && git commit -m "chore(pgwire): satisfy workspace pedantic lints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 4: Vendor `crabka-gres-conformance` + add the `--baseline` parity gate

**Files:**
- Replace: `crates/gres-conformance/src/` (from `$DONOR/crates/conformance/src/`)
- Create: `crates/gres-conformance/corpus/` (25 `.sql` files from donor), `crates/gres-conformance/README.md`

**Interfaces:**
- Consumes: nothing (black-box harness; connects over the wire only).
- Produces: bins `crabka-gres-conformance` (runner; args `--oracle-url`, `--subject-url`, `--corpus`, `--out`, `--summary`, **new** `--baseline`) and `crabka-gres-record`; lib types `Report { total, matched, parity_percent, cases }`, **new** `Baseline { total, matched }` and `Report::check_baseline(&Baseline) -> Result<(), String>`. Task 12 records `crates/gres-conformance/baseline.json`; Task 13's CI job passes `--baseline`.

- [ ] **Step 1: Copy sources and corpus**

```bash
rm -rf crates/gres-conformance/src
cp -r $DONOR/crates/conformance/src crates/gres-conformance/
cp -r $DONOR/crates/conformance/corpus crates/gres-conformance/
ls crates/gres-conformance/corpus | wc -l
```

Expected: 25 corpus files.

- [ ] **Step 2: Rename-adjust the copied sources** (this crate imports no siblings, so the sed is not needed; three manual edits):
  1. `src/main.rs`: the `--corpus` default `crates/conformance/corpus` → `crates/gres-conformance/corpus`.
  2. `src/lib.rs` `markdown_summary()`: report title `# crabgresql conformance report` → `# crabka-gres conformance report` (adjust any unit test asserting the old title).
  3. `src/main.rs` + `src/bin/record.rs`: references to the lib crate by name (`conformance::…` / `use conformance::`) → `crabka_gres_conformance::…`.

- [ ] **Step 3: Write the failing baseline-gate tests** — append to the `#[cfg(test)] mod tests` in `crates/gres-conformance/src/lib.rs` (create the module if the donor keeps tests elsewhere):

```rust
use assert2::assert;

#[test]
fn baseline_passes_on_exact_match() {
    let r = report_with(613, 591);
    let b = Baseline { total: 613, matched: 591 };
    assert!(r.check_baseline(&b).is_ok());
}

#[test]
fn baseline_passes_on_improvement() {
    let r = report_with(613, 600);
    let b = Baseline { total: 613, matched: 591 };
    assert!(r.check_baseline(&b).is_ok());
}

#[test]
fn baseline_fails_on_parity_regression() {
    let r = report_with(613, 580);
    let b = Baseline { total: 613, matched: 591 };
    let err = r.check_baseline(&b).expect_err("regression must fail");
    assert!(err.contains("parity regression"));
}

#[test]
fn baseline_fails_on_corpus_size_change() {
    let r = report_with(620, 620);
    let b = Baseline { total: 613, matched: 591 };
    let err = r.check_baseline(&b).expect_err("size change must fail");
    assert!(err.contains("corpus size changed"));
}

/// Report with the given counts and no per-case detail.
fn report_with(total: usize, matched: usize) -> Report {
    Report {
        total,
        matched,
        parity_percent: if total == 0 { 0.0 } else { 100.0 * matched as f64 / total as f64 },
        cases: Vec::new(),
    }
}
```

(If `Report`'s fields are not all `pub`, construct via whatever donor constructor exists and adapt `report_with` accordingly — but the donor declares them `pub`.)

- [ ] **Step 4: Run the tests to verify they fail**

```bash
cargo nextest run -p crabka-gres-conformance
```

Expected: FAIL to compile — `Baseline` and `check_baseline` not defined.

- [ ] **Step 5: Implement the gate** — add to `crates/gres-conformance/src/lib.rs`:

```rust
/// Machine-readable parity floor for CI. The G-1 gate: the vendored engine
/// must reproduce exactly the donor repository's conformance results, so
/// `total` is pinned and `matched` may only ratchet upward.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Baseline {
    pub total: usize,
    pub matched: usize,
}

impl Report {
    /// Gate this report against a recorded baseline; `Err` carries the
    /// human-readable failure for CI logs.
    pub fn check_baseline(&self, baseline: &Baseline) -> Result<(), String> {
        if self.total != baseline.total {
            return Err(format!(
                "corpus size changed: report has {} statements, baseline records {} — \
                 update crates/gres-conformance/baseline.json deliberately, never incidentally",
                self.total, baseline.total
            ));
        }
        if self.matched < baseline.matched {
            return Err(format!(
                "parity regression: {}/{} statements match the oracle, baseline requires at least {}",
                self.matched, self.total, baseline.matched
            ));
        }
        Ok(())
    }
}
```

And in `src/main.rs`: add the clap field

```rust
    /// Optional parity baseline; when set, exit nonzero on any regression.
    #[arg(long)]
    baseline: Option<std::path::PathBuf>,
```

and, after the reports are written (donor `main` prints the parity line last):

```rust
    if let Some(path) = &args.baseline {
        let text = std::fs::read_to_string(path)?;
        let baseline: crabka_gres_conformance::Baseline = serde_json::from_str(&text)?;
        match report.check_baseline(&baseline) {
            Ok(()) => println!(
                "baseline gate passed: {}/{} matched (floor {})",
                report.matched, report.total, baseline.matched
            ),
            Err(msg) => {
                eprintln!("baseline gate FAILED: {msg}");
                std::process::exit(1);
            }
        }
    }
```

(Adapt the local variable name to the donor's — the report value it serializes to `--out`.)

- [ ] **Step 6: Run tests to verify they pass**

```bash
cargo nextest run -p crabka-gres-conformance
cargo clippy -p crabka-gres-conformance --all-targets -- -D warnings
cargo +nightly fmt -p crabka-gres-conformance
```

Expected: all green (fix pedantic per policy).

- [ ] **Step 7: Write `crates/gres-conformance/README.md`** — template (internal variant, no badges) with:
  - `{CRATE}` = `crabka-gres-conformance`
  - `{ONELINER}` = "Differential conformance harness diffing Crabka Gres against a real PostgreSQL oracle over the wire."
  - `{OVERVIEW}` = "Runs every statement in `corpus/*.sql` through both a real PostgreSQL (the oracle) and a Crabka Gres subject via the simple query protocol, diffing rows and SQLSTATEs into `parity.json`/`parity.md`. `--baseline baseline.json` turns the report into a CI gate: the statement total is pinned and the match count may only ratchet up. `baseline.json` records the parity of the vendored engine as measured against the donor repository at import (crabgresql@93f3d17, postgres:18 oracle); update it only deliberately — e.g. a corpus change, an engine improvement, or a documented postgres:18 minor-version drift — never to absorb a regression."

- [ ] **Step 8: Commit**

```bash
git add crates/gres-conformance
git commit -m "feat(gres): vendor the conformance harness with a baseline parity gate

Vendors crabgresql's differential oracle harness and corpus, and adds
--baseline: CI fails on any parity regression against the recorded donor
baseline (the G-1 gate).

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 2 — depends on pgtypes (run Tasks 5, 6 in parallel)

### Task 5: Vendor `crabka-pgkv`

**Files:**
- Replace: `crates/pgkv/src/` (from `$DONOR/crates/kv/src/`)
- Create: `crates/pgkv/README.md` (donor has no `tests/` dir — unit tests are in-src)

**Interfaces:**
- Consumes: `crabka_pgtypes` (Task 2).
- Produces: `crabka_pgkv::{Kv, MemKv, FjallKv, WriteOp, …}` — donor API unchanged; the permanent storage seam (G-2's `SubstrateKv` will implement `Kv` behind it).

- [ ] **Step 1: Copy sources**

```bash
rm -rf crates/pgkv/src
cp -r $DONOR/crates/kv/src crates/pgkv/
```

- [ ] **Step 2: Apply the rename sed** (from `crates/pgkv/`). Expected: `rename clean`.

- [ ] **Step 3: Add the docs.rs root URL** after the `//!` block in `crates/pgkv/src/lib.rs`:

```rust
#![doc(html_root_url = "https://docs.rs/crabka-pgkv/0.3.9")]
```

- [ ] **Step 4: Format, build, commit the import**

```bash
cargo +nightly fmt -p crabka-pgkv
cargo check -p crabka-pgkv --all-targets
git add crates/pgkv && git commit -m "feat(pgkv): vendor kv from crabgresql@93f3d17

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Pedantic lint pass** — `cargo clippy -p crabka-pgkv --all-targets -- -D warnings`; fix per policy (key-encoding code: never change byte layouts).

- [ ] **Step 6: Run the crate tests**

```bash
cargo nextest run -p crabka-pgkv
```

Expected: all pass (in-src unit + proptest suites, including the fjall store round-trips under `tempfile`).

- [ ] **Step 7: Write `crates/pgkv/README.md`** from the template with:
  - `{CRATE}` = `crabka-pgkv`
  - `{ONELINER}` = "Ordered key-value storage seam for the Crabka Gres engine with order-preserving key encoding and versioned row encoding."
  - `{OVERVIEW}` = "Defines the `Kv` trait (`get`/`put`/`delete`/`scan_prefix`/`scan_range`/`write_batch`, with `write_batch` atomic and durable) that the whole Gres engine consumes, plus two local backends: `MemKv` (ephemeral) and `FjallKv` (pure-Rust LSM). This trait is the permanent storage seam — Chapter Gres G-2 puts the Crabka substrate behind it without touching the engine."

- [ ] **Step 8: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-pgkv
git add crates/pgkv && git commit -m "chore(pgkv): satisfy workspace pedantic lints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 6: Vendor `crabka-pgparser`

**Files:**
- Replace: `crates/pgparser/src/` (from `$DONOR/crates/pgparser/src/`)
- Create: `crates/pgparser/tests/` (from donor; the `libpg_query_oracle.rs` file is `#![cfg(feature = "oracle")]`), `crates/pgparser/README.md`

**Interfaces:**
- Consumes: `crabka_pgtypes` (Task 2).
- Produces: `crabka_pgparser::{parse, ast::Statement, …}` — donor API unchanged; Task 9 (pgexec) consumes the AST.

- [ ] **Step 1: Copy sources and tests**

```bash
rm -rf crates/pgparser/src
cp -r $DONOR/crates/pgparser/src crates/pgparser/
cp -r $DONOR/crates/pgparser/tests crates/pgparser/
```

- [ ] **Step 2: Apply the rename sed** (from `crates/pgparser/`). Expected: `rename clean`.

- [ ] **Step 3: Add the docs.rs root URL** after the `//!` block in `crates/pgparser/src/lib.rs`:

```rust
#![doc(html_root_url = "https://docs.rs/crabka-pgparser/0.3.9")]
```

- [ ] **Step 4: Format, build, commit the import**

```bash
cargo +nightly fmt -p crabka-pgparser
cargo check -p crabka-pgparser --all-targets
git add crates/pgparser && git commit -m "feat(pgparser): vendor pgparser from crabgresql@93f3d17

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Pedantic lint pass** — `cargo clippy -p crabka-pgparser --all-targets -- -D warnings`; fix per policy. Do NOT enable the `oracle` feature in this pass (it builds C; local-only tooling).

- [ ] **Step 6: Run the crate tests**

```bash
cargo nextest run -p crabka-pgparser
```

Expected: all pass (lexer/parser unit + proptest suites; the libpg_query oracle test compiles out without the feature).

- [ ] **Step 7: Write `crates/pgparser/README.md`** from the template with:
  - `{CRATE}` = `crabka-pgparser`
  - `{ONELINER}` = "Hand-written PostgreSQL SQL lexer and parser producing the Crabka Gres AST."
  - `{OVERVIEW}` = "An original recursive-descent/Pratt parser (no third-party SQL engine) producing the `Statement` AST the Gres executor consumes. The optional `oracle` feature (never default; C build dep) enables a differential accept/reject test against libpg_query for local verification."

- [ ] **Step 8: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-pgparser
git add crates/pgparser && git commit -m "chore(pgparser): satisfy workspace pedantic lints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 3 — depends on pgkv (run Tasks 7, 8 in parallel)

### Task 7: Vendor `crabka-pgmvcc`

**Files:**
- Replace: `crates/pgmvcc/src/` (from `$DONOR/crates/mvcc/src/`)
- Create: `crates/pgmvcc/README.md` (no `tests/` dir in donor)

**Interfaces:**
- Consumes: `crabka_pgkv` (Task 5), `crabka_pgtypes` (Task 2).
- Produces: `crabka_pgmvcc::{Xid, Snapshot, visibility::satisfies_mvcc, clog, …}` — donor API unchanged; Task 9 consumes it.

- [ ] **Step 1: Copy sources**

```bash
rm -rf crates/pgmvcc/src
cp -r $DONOR/crates/mvcc/src crates/pgmvcc/
```

- [ ] **Step 2: Apply the rename sed** (from `crates/pgmvcc/`). Expected: `rename clean`.

- [ ] **Step 3: Add the docs.rs root URL** after the `//!` block in `crates/pgmvcc/src/lib.rs`:

```rust
#![doc(html_root_url = "https://docs.rs/crabka-pgmvcc/0.3.9")]
```

- [ ] **Step 4: Format, build, commit the import**

```bash
cargo +nightly fmt -p crabka-pgmvcc
cargo check -p crabka-pgmvcc --all-targets
git add crates/pgmvcc && git commit -m "feat(pgmvcc): vendor mvcc from crabgresql@93f3d17

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Pedantic lint pass** — `cargo clippy -p crabka-pgmvcc --all-targets -- -D warnings`; fix per policy (visibility logic is a faithful `HeapTupleSatisfiesMVCC` port — structure must not change).

- [ ] **Step 6: Run the crate tests**

```bash
cargo nextest run -p crabka-pgmvcc
```

Expected: all pass.

- [ ] **Step 7: Write `crates/pgmvcc/README.md`** from the template with:
  - `{CRATE}` = `crabka-pgmvcc`
  - `{ONELINER}` = "PostgreSQL-faithful MVCC for the Crabka Gres engine: xids, clog, snapshots, and `HeapTupleSatisfiesMVCC` visibility."
  - `{OVERVIEW}` = "Snapshot-isolation machinery ported faithfully from PostgreSQL: transaction ids, the clog (pg_xact) status store over the KV seam, xid-list `Snapshot`s, and tuple `(xmin, xmax)` visibility."

- [ ] **Step 8: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-pgmvcc
git add crates/pgmvcc && git commit -m "chore(pgmvcc): satisfy workspace pedantic lints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 8: Vendor `crabka-pgcatalog`

**Files:**
- Replace: `crates/pgcatalog/src/` (from `$DONOR/crates/catalog/src/`)
- Create: `crates/pgcatalog/README.md` (no `tests/` dir in donor)

**Interfaces:**
- Consumes: `crabka_pgkv` (Task 5), `crabka_pgtypes` (Task 2).
- Produces: `crabka_pgcatalog::{Table, TableId, Column, ForeignDataWrapper, ForeignServer, UserMapping, ForeignTableMeta, …}` — donor API unchanged; Tasks 9 and 10 consume it.

- [ ] **Step 1: Copy sources**

```bash
rm -rf crates/pgcatalog/src
cp -r $DONOR/crates/catalog/src crates/pgcatalog/
```

- [ ] **Step 2: Apply the rename sed** (from `crates/pgcatalog/`). Expected: `rename clean`. (Note: `src/serde.rs` is hand-rolled byte serialization, not the serde crate — the sed does not touch it.)

- [ ] **Step 3: Add the docs.rs root URL** after the `//!` block in `crates/pgcatalog/src/lib.rs`:

```rust
#![doc(html_root_url = "https://docs.rs/crabka-pgcatalog/0.3.9")]
```

- [ ] **Step 4: Format, build, commit the import**

```bash
cargo +nightly fmt -p crabka-pgcatalog
cargo check -p crabka-pgcatalog --all-targets
git add crates/pgcatalog && git commit -m "feat(pgcatalog): vendor catalog from crabgresql@93f3d17

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Pedantic lint pass** — `cargo clippy -p crabka-pgcatalog --all-targets -- -D warnings`; fix per policy (the versioned byte encodings in `serde.rs` must not change layout).

- [ ] **Step 6: Run the crate tests**

```bash
cargo nextest run -p crabka-pgcatalog
```

Expected: all pass.

- [ ] **Step 7: Write `crates/pgcatalog/README.md`** from the template with:
  - `{CRATE}` = `crabka-pgcatalog`
  - `{ONELINER}` = "System catalog for the Crabka Gres engine: tables, columns, and FDW metadata as a stateless view over the KV storage seam."
  - `{OVERVIEW}` = "OID-style table ids, column definitions, and foreign-data-wrapper metadata (`FOREIGN DATA WRAPPER`/`SERVER`/`USER MAPPING`/`FOREIGN TABLE`), persisted through the `Kv` trait with PostgreSQL error codes on CRUD."

- [ ] **Step 8: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-pgcatalog
git add crates/pgcatalog && git commit -m "chore(pgcatalog): satisfy workspace pedantic lints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 4 — the engine (serial)

### Task 9: Vendor `crabka-pgexec`

The largest crate (~16k src lines, 23 end-to-end integration test files that each spawn an in-process pgwire server and drive it with tokio-postgres).

**Files:**
- Replace: `crates/pgexec/src/` (from `$DONOR/crates/executor/src/`)
- Create: `crates/pgexec/tests/` (from donor), `crates/pgexec/README.md`

**Interfaces:**
- Consumes: `crabka_pgtypes`, `crabka_pgkv`, `crabka_pgmvcc`, `crabka_pgcatalog`, `crabka_pgparser`, `crabka_pgwire` (Tasks 2–8).
- Produces: `crabka_pgexec::SqlEngine` with `new()` (ephemeral MemKv), `open(path) -> Result<Self, ExecError>` (durable fjall), `with_kv(Arc<dyn Kv>)`, `set_foreign_scanner(Arc<dyn ForeignScanner>)`, plus `crabka_pgexec::foreign::{ForeignScanner, ImportFilter, ImportedTable, ScanBounds}` and the `Committer`/`Linearizer` seams. `SqlEngine` implements `crabka_pgwire::engine::Engine`. Tasks 10 and 11 consume this. **Note:** the donor's `SqlEngine::replicated(…)` constructor exists solely for the non-vendored cluster crate — it must still compile (its `Committer`/`Linearizer` args are executor-local traits), so keep it; it is the seam G-2 implements.

- [ ] **Step 1: Copy sources and tests**

```bash
rm -rf crates/pgexec/src
cp -r $DONOR/crates/executor/src crates/pgexec/
cp -r $DONOR/crates/executor/tests crates/pgexec/
ls crates/pgexec/tests | wc -l
```

Expected: the count matches `ls $DONOR/crates/executor/tests | wc -l` exactly (the suite includes aggregates, casts, concurrency, ctes, datetime, durability, end_to_end, floating_point, formatting, joins, linearizable_reads, math_string_functions, mutation_semantics, numeric, ordering, predicates, query_expressions, recovery, recursion_guard, scalar_functions, set_operations, subqueries, transactions, values_query).

- [ ] **Step 2: Apply the rename sed** (from `crates/pgexec/`). Expected: `rename clean`.

- [ ] **Step 3: Add the docs.rs root URL** after the `//!` block in `crates/pgexec/src/lib.rs`:

```rust
#![doc(html_root_url = "https://docs.rs/crabka-pgexec/0.3.9")]
```

- [ ] **Step 4: Format, build, commit the import**

```bash
cargo +nightly fmt -p crabka-pgexec
cargo check -p crabka-pgexec --all-targets
git add crates/pgexec && git commit -m "feat(pgexec): vendor executor from crabgresql@93f3d17

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Pedantic lint pass** — `cargo clippy -p crabka-pgexec --all-targets -- -D warnings`. This is the bulk of the chapter's lint work; budget accordingly, apply the policy mechanically, and keep fixes behavior-preserving (the conformance baseline in Task 12 is the backstop).

- [ ] **Step 6: Run the crate tests**

```bash
cargo nextest run -p crabka-pgexec
```

Expected: all pass, zero failures, none ignored.

- [ ] **Step 7: Write `crates/pgexec/README.md`** from the template with:
  - `{CRATE}` = `crabka-pgexec`
  - `{ONELINER}` = "SQL execution engine for Crabka Gres: turns parsed SQL into catalog and KV operations under MVCC and implements the pgwire `Engine` trait."
  - `{OVERVIEW}` = "The engine behind a Gres tenant: session management, transactions (Read Committed / Repeatable Read), row-level locking for concurrent writers, joins, aggregates, subqueries, CTEs, set operations, and the PostgreSQL function library, executing over the `Kv` seam with `crabka-pgmvcc` visibility. The `Committer`/`Linearizer` seams are where Chapter Gres G-2 attaches substrate-backed durability; `foreign::ForeignScanner` is the FDW seam."

- [ ] **Step 8: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-pgexec
git add crates/pgexec && git commit -m "chore(pgexec): satisfy workspace pedantic lints

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 5 — FDW (serial)

### Task 10: Vendor `crabka-gres-fdw` (un-gated, path-dep rewire)

**Files:**
- Replace: `crates/gres-fdw/src/` (from `$DONOR/crates/kafka_fdw/src/`)
- Create: `crates/gres-fdw/tests/` (from donor), `crates/gres-fdw/README.md`
- Modify: `.config/nextest.toml` (new test group — no other task touches this file)

**Interfaces:**
- Consumes: `crabka_pgexec::foreign::ForeignScanner` (Task 9), `crabka_pgcatalog`, `crabka_pgkv`, `crabka_pgtypes`; workspace crates `crabka-client-core`, `crabka-client-admin`, `crabka-protocol`, `crabka-schema-serde`, `crabka-security` (path deps now, previously published 0.3.7).
- Produces: `crabka_gres_fdw::KafkaFdw` (implements `ForeignScanner`) and `crabka_gres_fdw::provider::install_default_provider()`. Task 11 wires both into the binary.

- [ ] **Step 1: Copy sources and tests**

```bash
rm -rf crates/gres-fdw/src
cp -r $DONOR/crates/kafka_fdw/src crates/gres-fdw/
cp -r $DONOR/crates/kafka_fdw/tests crates/gres-fdw/
```

- [ ] **Step 2: Remove the feature gate** (the manifest from Task 1 already has no `kafka` feature):
  1. Delete the `#![cfg(feature = "kafka")]` line from `crates/gres-fdw/src/lib.rs`.
  2. Delete the `#![cfg(feature = "kafka")]` / `#[cfg(feature = "kafka")]` gate lines from `tests/roundtrip.rs` and `tests/harness/mod.rs`.

- [ ] **Step 3: Apply the rename sed** (from `crates/gres-fdw/`). Expected: `rename clean`.

- [ ] **Step 4: Apply the two published-API drift fixes** (verified against current workspace sources):
  1. `src/lib.rs` (donor line ~199): `RegistryClient::schema_by_id` now returns `FetchedSchema { schema, message_type }` instead of `String` — append `.schema`:
     the donor's `let schema_text = registry.schema_by_id(id).await.ok()?;` becomes
     `let schema_text = registry.schema_by_id(id).await.ok()?.schema;`
     (match the donor's actual binding shape at the call site).
  2. `tests/harness/mod.rs` (donor lines ~179-184): `KafkaStore::register` now takes a `RegisterSchema<'_>` struct and returns `Registered { id: SchemaId, version }`. Rewrite the 6-positional-arg call as:

```rust
let reg = store
    .register(crabka_schema_registry::kafkastore::RegisterSchema {
        subject: &subject,
        ty: SchemaType::Avro,
        schema: schema_json,
        references: &[],
        message_type: None,
        import_id: None,
        import_version: None,
    })
    .await
    .expect("register schema");
let id = u32::try_from(reg.id.0).expect("schema id fits u32");
```

(Adapt field/binding names to the harness's actual locals; the struct is at `crates/schema-registry/src/kafkastore/mod.rs:41` and `Registered` at `crates/schema-registry/src/store/mod.rs:18`.)

- [ ] **Step 5: Format, build, commit the import**

```bash
cargo +nightly fmt -p crabka-gres-fdw
cargo check -p crabka-gres-fdw --all-targets
git add crates/gres-fdw && git commit -m "feat(gres): vendor kafka_fdw as crabka-gres-fdw on workspace path deps

Drops the donor's off-by-default kafka feature gate (pure-Rust is ambient in
this workspace) and adapts to two published-API drifts: schema_by_id's
FetchedSchema return and KafkaStore::register's RegisterSchema struct.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Pedantic lint pass** — `cargo clippy -p crabka-gres-fdw --all-targets -- -D warnings`; fix per policy.

- [ ] **Step 7: Add the nextest test group** — in `.config/nextest.toml`, append to `[test-groups]`:

```toml
# Chapter Gres FDW roundtrip tests: each boots an in-process broker + schema
# registry; cap concurrency to avoid broker saturation on constrained runners.
gres-fdw = { max-threads = 2 }
```

and after the existing `[[profile.default.overrides]]` blocks:

```toml
[[profile.default.overrides]]
# crabka-gres-fdw's integration tests boot an in-process broker + registry.
filter = 'package(crabka-gres-fdw) & kind(test)'
test-group = 'gres-fdw'
```

- [ ] **Step 8: Run the crate tests**

```bash
cargo nextest run -p crabka-gres-fdw
```

Expected: all pass — unit suites (source-bounds clamping, config resolution, Avro/JSON/protobuf projection) plus `kafka_fdw_roundtrip_avro_and_raw_fallback`, which boots an in-process Crabka broker + schema registry (no Docker) and round-trips `CREATE SERVER` / `IMPORT FOREIGN SCHEMA` / `SELECT`.

- [ ] **Step 9: Write `crates/gres-fdw/README.md`** — template (internal variant, no badges) with:
  - `{CRATE}` = `crabka-gres-fdw`
  - `{ONELINER}` = "Foreign-data wrapper exposing Kafka topics as SQL tables inside Crabka Gres tenant databases."
  - `{OVERVIEW}` = "Implements the executor's `ForeignScanner` seam over the workspace's own Rust Kafka clients: bounded per-partition snapshot reads at `READ_COMMITTED`, envelope columns (`_partition`, `_offset`, `_timestamp`, `_key`, `_headers`), Avro/JSON/Protobuf value decoding via Schema Registry, and `IMPORT FOREIGN SCHEMA` from registered schemas."

- [ ] **Step 10: Commit the cleanup**

```bash
cargo +nightly fmt -p crabka-gres-fdw
git add crates/gres-fdw .config/nextest.toml
git commit -m "chore(gres): gres-fdw pedantic lints and nextest concurrency group

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 6 — the service binary (serial)

### Task 11: `crabka-gres` binary, CLI smoke test, smoke scripts

**Files:**
- Create: `crates/gres/src/main.rs` (donor serve mode, adapted), `crates/gres/tests/cli_smoke.rs`, `crates/gres/README.md`, `scripts/gres-psql-smoke.sh`, `scripts/gres-durable-restart-smoke.sh`

**Interfaces:**
- Consumes: `crabka_pgexec::SqlEngine`, `crabka_pgwire::{server::serve_tls, session::{SessionConfig, AuthMode}, scram::ScramVerifier}`, `crabka_gres_fdw::{KafkaFdw, provider::install_default_provider}`.
- Produces: the `crabka-gres` binary — CLI: `--listen` (default `127.0.0.1:5433`), `--data-dir` (optional; absent = ephemeral), `--tls-cert`/`--tls-key` (paired), `--auth trust|scram`, `--user-cred USER=PASSWORD` (repeatable). Tasks 12 and 13 run it.

- [ ] **Step 1: Copy the donor bin and cut it down**

```bash
cp $DONOR/crates/crabgresql/src/main.rs crates/gres/src/main.rs
```

Then edit `crates/gres/src/main.rs`:
1. Delete everything cluster-related: the `enum Command { Node(NodeArgs) }` declaration, the `NodeArgs` struct, the whole `run_node()` function, the `Some(Command::Node(args)) => …` match arm and the `subcommand` field it matches on, and every `use cluster::…`/`cluster::` reference. After this, `main` unconditionally runs the serve path.
2. Un-gate the FDW wiring: the `#[cfg(feature = "kafka")]`-gated statements in the serve path become unconditional (delete the attribute/gating, keep the bodies), ending up as:

```rust
crabka_gres_fdw::provider::install_default_provider();
engine.set_foreign_scanner(Arc::new(crabka_gres_fdw::KafkaFdw));
```

(placed exactly where the donor's gated versions sat: after the engine is constructed, before `Arc::new(engine)`).
3. Update the clap command attribute to `#[command(name = "crabka-gres", version, about = "Crabka Gres — pure-Rust Postgres-compatible tenant compute")]`.

- [ ] **Step 2: Apply the rename sed** (from `crates/gres/`; renames `executor::`, `pgwire::`, `kafka_fdw::` references). Expected: `rename clean`.

- [ ] **Step 3: Build and run it manually once**

```bash
cargo run -p crabka-gres -- --listen 127.0.0.1:54399 &
sleep 1 && psql "host=127.0.0.1 port=54399 user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1'; kill %1
```

Expected: prints `1`. (If psql is not installed locally, skip this step — the smoke test in Step 4 covers it.)

- [ ] **Step 4: Write the failing CLI smoke test** — `crates/gres/tests/cli_smoke.rs`:

```rust
//! Boots the real `crabka-gres` binary and drives it over the Postgres wire.

use std::process::{Child, Command};
use std::time::Duration;

use assert2::assert;

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Reserve an ephemeral port: bind :0, read the assigned port, release it.
/// The bounded readiness loop below fails loudly if the port is stolen.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

#[tokio::test]
async fn select_one_roundtrips_through_the_real_binary() {
    let port = free_port();
    let _server = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_crabka-gres"))
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .spawn()
            .expect("spawn crabka-gres"),
    );

    let conn_str = format!("host=127.0.0.1 port={port} user=crab dbname=crab");
    // Bounded, condition-driven readiness wait: retry the actual condition
    // (connect + query) instead of sleeping for the server to "settle".
    let mut last_err = String::new();
    for _ in 0..100 {
        match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
            Ok((client, conn)) => {
                let conn_task = tokio::spawn(conn);
                let rows = client.query("SELECT 1", &[]).await.expect("SELECT 1");
                let v: i32 = rows[0].get(0);
                assert!(v == 1);
                drop(client);
                conn_task.abort();
                return;
            }
            Err(e) => {
                last_err = e.to_string();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("crabka-gres never became ready after 10s: {last_err}");
}
```

- [ ] **Step 5: Run the smoke test**

```bash
cargo nextest run -p crabka-gres
```

Expected: PASS (1 test). If it fails, the binary is broken — fix `main.rs`, not the test.

- [ ] **Step 6: Lint and format**

```bash
cargo clippy -p crabka-gres --all-targets -- -D warnings
cargo +nightly fmt -p crabka-gres
```

- [ ] **Step 7: Write the smoke scripts** — both are the donor scripts with three substitutions: build/run `crabka-gres` (package `-p crabka-gres`, binary `./target/debug/crabka-gres`) instead of `crabgresql`; everything else (ports, cert fixture paths under `crates/pgwire/tests/fixtures`, readiness loops, TLS/SCRAM legs) is kept verbatim.

`scripts/gres-psql-smoke.sh` — copy `$DONOR/scripts/psql-smoke.sh`, then:

```bash
sed -i -e 's/-p crabgresql/-p crabka-gres/' -e 's#/target/debug/crabgresql#/target/debug/crabka-gres#g' -e 's#\./target/debug/crabgresql#./target/debug/crabka-gres#g' scripts/gres-psql-smoke.sh
chmod +x scripts/gres-psql-smoke.sh
```

`scripts/gres-durable-restart-smoke.sh` — copy `$DONOR/scripts/durable-restart-smoke.sh`, apply the same sed, `chmod +x`.

- [ ] **Step 8: Run both scripts locally** (require `psql`; they self-skip without it)

```bash
./scripts/gres-psql-smoke.sh
./scripts/gres-durable-restart-smoke.sh
```

Expected: `PASS: psql SELECT 1 -> 1` (+ TLS and TLS+SCRAM legs, since the pgwire fixtures exist) and `PASS: data survived restart -> durable`.

- [ ] **Step 9: Write `crates/gres/README.md`** — template (internal variant, no badges) with:
  - `{CRATE}` = `crabka-gres`
  - `{ONELINER}` = "Crabka Gres service: a pure-Rust Postgres-compatible tenant compute for the Crabka substrate."
  - `{OVERVIEW}` = "Serves one tenant database over the Postgres v3 wire protocol (TLS and SCRAM-SHA-256 optional): ephemeral by default, durable on local fjall with `--data-dir`. Kafka topics are queryable as foreign tables via `crabka-gres-fdw`. Substrate-backed durability (WAL topic + object-store checkpoints) arrives in Chapter Gres G-2/G-3; the PgDog front door and lifecycle in G-4/G-5."
  Plus a `## Quick Start` section after Overview:

```markdown
## Quick Start

    cargo run -p crabka-gres -- --listen 127.0.0.1:5433 --data-dir /tmp/gres-data
    psql "host=127.0.0.1 port=5433 user=crab dbname=crab"
```

- [ ] **Step 10: Commit**

```bash
git add crates/gres scripts/gres-psql-smoke.sh scripts/gres-durable-restart-smoke.sh
git commit -m "feat(gres): crabka-gres tenant compute binary with psql and durability smokes

The donor's serve mode (pgwire + SqlEngine on MemKv/fjall, TLS, SCRAM) minus
the cluster node subcommand, with the FDW always wired; adds a CLI smoke test
that boots the real binary and the two shell smokes CI runs.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Batch 7 — the parity baseline (serial; needs Tasks 4 and 11)

### Task 12: Capture and check in the donor parity baseline

The G-1 gate: prove the vendored engine reproduces the donor's conformance results exactly, then pin those numbers. Requires Docker locally.

**Files:**
- Create: `crates/gres-conformance/baseline.json`

**Interfaces:**
- Consumes: `crabka-gres` + `crabka-gres-conformance` binaries; the donor clone.
- Produces: `crates/gres-conformance/baseline.json` (`{"total": T, "matched": M}`) consumed by Task 13's CI job via `--baseline`.

- [ ] **Step 1: Run the DONOR harness against a fresh postgres:18 oracle**

```bash
docker run -d --name gres-oracle -e POSTGRES_HOST_AUTH_METHOD=trust -p 54320:5432 postgres:18
until docker exec gres-oracle pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done

cd $DONOR
cargo build -p crabgresql -p conformance
./target/debug/crabgresql --listen 127.0.0.1:54341 &
DONOR_PID=$!
until bash -c 'exec 3<>/dev/tcp/127.0.0.1/54341' 2>/dev/null; do sleep 0.3; done
./target/debug/conformance \
  --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
  --subject-url "host=127.0.0.1 port=54341 user=crab dbname=crab" \
  --corpus crates/conformance/corpus \
  --out /tmp/donor-parity.json --summary /tmp/donor-parity.md
kill $DONOR_PID
docker rm -f gres-oracle
```

Expected: `parity: NN.N% (M / T) -> …` on stdout. (The donor build resolves published `crabka-*` 0.3 crates from crates.io — expected, that is how the donor pins them.)

- [ ] **Step 2: Run OUR harness against a fresh identical oracle** (fresh container — the corpus creates tables, so an oracle cannot be reused across runs)

```bash
docker run -d --name gres-oracle -e POSTGRES_HOST_AUTH_METHOD=trust -p 54320:5432 postgres:18
until docker exec gres-oracle pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done

cd <repo root>
cargo build -p crabka-gres -p crabka-gres-conformance
./target/debug/crabka-gres --listen 127.0.0.1:54342 &
GRES_PID=$!
until bash -c 'exec 3<>/dev/tcp/127.0.0.1/54342' 2>/dev/null; do sleep 0.3; done
./target/debug/crabka-gres-conformance \
  --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
  --subject-url "host=127.0.0.1 port=54342 user=crab dbname=crab" \
  --corpus crates/gres-conformance/corpus \
  --out /tmp/gres-parity.json --summary /tmp/gres-parity.md
kill $GRES_PID
docker rm -f gres-oracle
```

- [ ] **Step 3: Compare — the counts must be IDENTICAL**

```bash
jq '{total, matched}' /tmp/donor-parity.json
jq '{total, matched}' /tmp/gres-parity.json
```

Expected: the two objects are equal. If they differ, the vendoring changed behavior — STOP. Diff the failing cases (`jq -r '.cases[] | select(.matched | not) | "\(.file): \(.sql)"' <file>`) between the two reports, find the vendored-crate regression (a lint "fix" is the prime suspect), fix it, and rerun Step 2. **Never** check in a baseline lower than the donor's.

- [ ] **Step 4: Check in the baseline**

```bash
jq '{total, matched}' /tmp/gres-parity.json > crates/gres-conformance/baseline.json
git add crates/gres-conformance/baseline.json
git commit -m "test(gres): record the donor conformance parity baseline

Captured against a postgres:18 oracle: the vendored engine reproduces
crabgresql@93f3d17's parity exactly; CI gates on it via --baseline.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Verify the gate end-to-end once** — rerun Step 2's harness command (fresh oracle container again) with `--baseline crates/gres-conformance/baseline.json` appended. Expected: `baseline gate passed: M/T matched (floor M)` and exit code 0.

---

## Batch 8 — registration and CI (run Tasks 13, 14 in parallel)

### Task 13: CI wiring — conformance job, integration job, codecov

**Files:**
- Modify: `.github/workflows/ci.yml` (changes-filter, two new jobs, gatekeeper needs), `codecov.yml`

**Interfaces:**
- Consumes: `scripts/gres-*.sh`, the three gres binaries, `crates/gres-conformance/baseline.json` (Task 12).
- Produces: CI jobs `gres-conformance` and `gres-integration`; codecov flag `gres-integration`.

- [ ] **Step 1: Add the paths filter** — in the `changes` job of `.github/workflows/ci.yml`, add an output `gres: ${{ steps.filter.outputs.gres }}` alongside the existing outputs, and in the `filters:` block (mirroring the existing entries' style):

```yaml
          gres:
            - "crates/pgtypes/**"
            - "crates/pgparser/**"
            - "crates/pgwire/**"
            - "crates/pgkv/**"
            - "crates/pgmvcc/**"
            - "crates/pgcatalog/**"
            - "crates/pgexec/**"
            - "crates/gres/**"
            - "crates/gres-fdw/**"
            - "crates/gres-conformance/**"
            - "scripts/gres-psql-smoke.sh"
            - "scripts/gres-durable-restart-smoke.sh"
```

- [ ] **Step 2: Add the `gres-conformance` job** (modeled on `metrics-conformance` + the donor's oracle service; place it near the other `*-conformance` jobs; use the same toolchain value the sibling jobs use):

```yaml
  gres-conformance:
    needs: changes
    if: ${{ needs.changes.outputs.gres == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 45
    services:
      oracle:
        image: postgres:18
        env:
          POSTGRES_HOST_AUTH_METHOD: trust
        ports:
          - 54320:5432
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s --health-timeout 5s --health-retries 10
    steps:
      - uses: actions/checkout@v7
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.96.0"
      - uses: Swatinem/rust-cache@v2
        with:
          key: gres-conformance
      - name: Install psql 18 (pgdg)
        run: |
          sudo install -d /usr/share/postgresql-common/pgdg
          sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc
          echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
          sudo apt-get update && sudo apt-get install -y postgresql-client-18
      - run: cargo build --locked -p crabka-gres -p crabka-gres-conformance
      - run: ./scripts/gres-psql-smoke.sh
      - name: Durable restart smoke
        run: ./scripts/gres-durable-restart-smoke.sh
      - name: Conformance harness against the parity baseline
        run: |
          ./target/debug/crabka-gres --listen 127.0.0.1:54333 &
          for _ in $(seq 30); do
            if psql "host=127.0.0.1 port=54333 user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null 2>&1; then break; fi
            sleep 0.3
          done
          ./target/debug/crabka-gres-conformance \
            --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=postgres" \
            --subject-url "host=127.0.0.1 port=54333 user=crab dbname=crab" \
            --corpus crates/gres-conformance/corpus \
            --baseline crates/gres-conformance/baseline.json \
            --out parity.json --summary parity.md
      - name: Publish parity summary
        if: ${{ !cancelled() }}
        run: cat parity.md >> "$GITHUB_STEP_SUMMARY"
      - name: Upload parity report
        if: ${{ !cancelled() }}
        uses: actions/upload-artifact@v7
        with:
          name: gres-parity-report
          path: |
            parity.json
            parity.md
          if-no-files-found: warn
```

- [ ] **Step 3: Add the `gres-integration` job** (modeled on `client-consumer-integration`; mirror that job's coverage-upload step verbatim — same action version, token/secret handling, and flags key — substituting the flag name):

```yaml
  gres-integration:
    needs: changes
    if: ${{ needs.changes.outputs.gres == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v7
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.96.0"
      - uses: Swatinem/rust-cache@v2
        with:
          key: gres-integration
      - uses: taiki-e/install-action@nextest
      - uses: taiki-e/install-action@cargo-llvm-cov
      - name: Gres engine integration tests
        run: |
          cargo llvm-cov nextest -p crabka-pgwire -p crabka-pgexec -p crabka-gres-fdw -p crabka-gres \
            --profile ci --tests --lcov --output-path lcov.info
      - name: Upload coverage
        uses: codecov/codecov-action@v5   # ← mirror the sibling job's exact upload step
        with:
          files: lcov.info
          flags: gres-integration
```

- [ ] **Step 4: Gate it** — add `gres-conformance` and `gres-integration` to the `gatekeeper-ci` job's `needs:` list.

- [ ] **Step 5: codecov.yml** — bump `after_n_builds: 11` → `12` in **both** places (`codecov.notify` and `comment`), and add to the `flags:` section:

```yaml
  gres-integration:
    carryforward: true
```

- [ ] **Step 6: Validate and commit**

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); yaml.safe_load(open('codecov.yml')); print('yaml ok')"
git add .github/workflows/ci.yml codecov.yml
git commit -m "ci: gres conformance gate and integration coverage jobs

gres-conformance boots a postgres:18 oracle service, runs the psql and
durable-restart smokes, and gates the corpus against the recorded donor
baseline; gres-integration runs the pgwire/pgexec/fdw/bin suites with
coverage (new codecov flag, after_n_builds 12).

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

### Task 14: Registration — release-plz, publish allowlist, Bazel manifests, root README

**Files:**
- Modify: `release-plz.toml`, `tools/check-publish-allowlist.sh`, `MODULE.bazel`, `README.md`

**Interfaces:**
- Consumes: all ten crates existing in `cargo metadata` (the allowlist script rejects release-plz entries for unknown packages — this is why registration is last).
- Produces: a green `tools/check-publish-allowlist.sh`; release-plz coverage for all ten crates.

- [ ] **Step 1: release-plz.toml** — in the public-crates group add, alphabetically among the existing entries:

```toml
[[package]]
name = "crabka-pgcatalog"
publish = true
release = true

[[package]]
name = "crabka-pgexec"
publish = true
release = true

[[package]]
name = "crabka-pgkv"
publish = true
release = true

[[package]]
name = "crabka-pgmvcc"
publish = true
release = true

[[package]]
name = "crabka-pgparser"
publish = true
release = true

[[package]]
name = "crabka-pgtypes"
publish = true
release = true

[[package]]
name = "crabka-pgwire"
publish = true
release = true
```

and in the internal group:

```toml
[[package]]
name = "crabka-gres"
publish = false
release = false

[[package]]
name = "crabka-gres-conformance"
publish = false
release = false

[[package]]
name = "crabka-gres-fdw"
publish = false
release = false
```

(Match the exact entry shape of the neighboring entries in each group.)

- [ ] **Step 2: Publish allowlist** — in `tools/check-publish-allowlist.sh`, add the seven published names to the `allowlist = { … }` set (keep it sorted):

```python
    "crabka-pgcatalog",
    "crabka-pgexec",
    "crabka-pgkv",
    "crabka-pgmvcc",
    "crabka-pgparser",
    "crabka-pgtypes",
    "crabka-pgwire",
```

- [ ] **Step 3: Run the allowlist check**

Run the script exactly as the `rust` CI job invokes it (see `.github/workflows/ci.yml`, typically `./tools/check-publish-allowlist.sh`). Expected: exit 0, no complaints about missing/misconfigured entries.

- [ ] **Step 4: MODULE.bazel** — add the ten manifests to the `crate.from_cargo(… manifests = […])` list (the list's own comment says it needs every workspace member; note ~21 pre-existing crates are already missing and Bazel is not in CI — we add ours to avoid deepening the drift):

```python
        "//crates:pgtypes/Cargo.toml",
        "//crates:pgparser/Cargo.toml",
        "//crates:pgwire/Cargo.toml",
        "//crates:pgkv/Cargo.toml",
        "//crates:pgmvcc/Cargo.toml",
        "//crates:pgcatalog/Cargo.toml",
        "//crates:pgexec/Cargo.toml",
        "//crates:gres/Cargo.toml",
        "//crates:gres-fdw/Cargo.toml",
        "//crates:gres-conformance/Cargo.toml",
```

- [ ] **Step 5: Root README workspace table** — add one row to the "Workspace" layer table in `README.md`:

```markdown
| Postgres-compatible engine (Chapter Gres) | [`crabka-gres`](crates/gres), [`crabka-pgexec`](crates/pgexec), [`crabka-pgwire`](crates/pgwire), [`crabka-pgtypes`](crates/pgtypes), [`crabka-pgparser`](crates/pgparser), [`crabka-pgkv`](crates/pgkv), [`crabka-pgmvcc`](crates/pgmvcc), [`crabka-pgcatalog`](crates/pgcatalog), [`crabka-gres-fdw`](crates/gres-fdw) |
```

- [ ] **Step 6: Full-workspace verification**

```bash
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --profile ci --lib --bins
cargo test --workspace --doc
tools/html-root-url.sh
```

Expected: all green. (This is the same gauntlet the `rust` CI job runs.)

- [ ] **Step 7: Commit**

```bash
git add release-plz.toml tools/check-publish-allowlist.sh MODULE.bazel README.md
git commit -m "chore(gres): register Chapter Gres crates with release-plz, publish allowlist, bazel, README

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Completion checklist (maps to the G-1 gate)

- All ten crates build and their donor test suites pass under `cargo nextest run --workspace`.
- `crates/gres-conformance/baseline.json` records donor-identical parity, and the `gres-conformance` CI job fails on any regression (`--baseline`).
- `./scripts/gres-durable-restart-smoke.sh` proves single-tenant durability on local fjall through a real restart.
- `tools/check-publish-allowlist.sh`, `tools/html-root-url.sh`, fmt, clippy `-D warnings` all green workspace-wide.
- Follow-ups deliberately NOT in G-1: substrate-backed `SubstrateKv` (G-2), checkpoints (G-3), PgDog/control plane (G-4/5), FDW product work (G-6), re-enabling cargo-mutants for the vendored crates, migrating the gres TLS stack from rustls-rustcrypto to the workspace's ring provider.
