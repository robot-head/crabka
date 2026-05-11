# 0.1.0 Publish Prep (sub-plan 1e) — Design

**Status:** Draft for review
**Date:** 2026-05-11
**Author:** Matthew Stone (with Claude)
**Predecessors:** coverage meta-spec
(`2026-05-11-crabka-protocol-coverage-design.md`); 1a (codegen
generalization, merged); 1b (compression, merged); 1c (typed
RecordBatch, merged); 1d (mass rollout, merged).

## Summary

Final sub-plan of the coverage slice. Make `crabka-compression` and
`crabka-protocol` publish-ready at version `0.1.0`: complete crate
metadata, per-crate README + CHANGELOG, supply-chain hygiene via
`cargo-deny`, API-shape watcher via `cargo-semver-checks`,
docs.rs configuration, `cargo publish --dry-run` clean, and a tagged
`v0.1.0` GitHub release with notes.

**Not in 1e:** the actual `cargo publish` to crates.io. 1e ships the
"flip-the-publish-switch" readiness; the real upload is a one-line
change once you have a crates.io account.

## North star (acceptance gate for sub-plan 1e)

1. Both publishable crates carry all crates.io-required + recommended
   metadata fields.
2. Per-crate `README.md` and `CHANGELOG.md` files exist with the
   `[0.1.0]` entry seeded.
3. `cargo-deny` config at the repo root, hard-gated in CI.
4. `cargo-semver-checks` runs in CI as informational pre-1.0.
5. `release-plz` configured with `publish = false` for the dry-run
   stance.
6. `cargo publish --dry-run` succeeds for both crates.
7. `cargo doc` builds clean with `--cfg docsrs`.
8. Tagged `v0.1.0` GitHub release exists with notes pulled from the
   CHANGELOGs.

## Non-goals

- **Real `cargo publish` to crates.io.** Dry-run only; flipping
  `publish = false` to `publish = true` in `release-plz.toml` is the
  one-line change once the user has a crates.io account.
- **Private registry support.** Not configured; not on the meta-spec's
  roadmap.
- **Automated release on tag push.** `release-plz` opens release PRs;
  merging the release PR triggers the tag + GitHub release. No "push
  a tag, get a publish" path.
- **API-stability promise.** Pre-1.0; `cargo-semver-checks` is
  informational.

---

# 1. Crate metadata

Both `crabka-compression` and `crabka-protocol` need their manifests
filled in for `cargo publish --dry-run` to pass cleanly.

### `crates/compression/Cargo.toml`

```toml
[package]
name = "crabka-compression"
version = "0.1.0"
edition.workspace = true
rust-version = "1.95.0"
license = "Apache-2.0"
authors = ["The Crabka Authors"]
description = "Kafka wire-protocol compression codecs for Rust"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-compression"
readme = "README.md"
keywords = ["kafka", "compression", "wire-protocol", "snappy", "zstd"]
categories = ["compression", "encoding"]
include = [
    "src/**/*",
    "Cargo.toml",
    "README.md",
    "LICENSE",
    "NOTICE",
]

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

### `crates/protocol/Cargo.toml`

Same shape with:
- `name = "crabka-protocol"`
- `description = "Apache Kafka wire-protocol codec (4.2.0), with typed RecordBatch and zero-copy borrowed decode"`
- `keywords = ["kafka", "wire-protocol", "codec", "serialization", "decoder"]` (max 5)
- `categories = ["encoding", "parser-implementations"]`
- Dep on `crabka-compression` becomes:
  ```toml
  crabka-compression = { version = "0.1", path = "../compression", default-features = false }
  ```

### Workspace version

The workspace `[workspace.package]` table currently has `version = "0.0.0"`. Bump to `0.1.0`. All workspace crates that use `version.workspace = true` follow.

`crabka-protocol-codegen` keeps `publish = false` (already set) and may stay at `0.0.0` by overriding its version locally, or follow the workspace bump — either way it's never uploaded.

### License files

`LICENSE` and `NOTICE` at the repo root get vendored into each `.crate`
package via the `include` lists above. crates.io renders the
`License` field as a badge; the file ships inside the archive.

---

# 2. `cargo-deny` configuration

`cargo-deny check` hard-gates PR CI. Config at the repo root.

### `deny.toml`

```toml
[graph]
all-features = true
no-default-features = false

[output]
feature-depth = 1

[advisories]
db-path = "~/.cargo/advisory-db"
db-urls = ["https://github.com/rustsec/advisory-db"]
yanked = "deny"
ignore = []

[licenses]
allow = [
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "MIT",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "Unicode-DFS-2016",
    "Unicode-3.0",
    "MPL-2.0",
    "Zlib",
]
exceptions = []
confidence-threshold = 0.93

[bans]
multiple-versions = "warn"
wildcards = "deny"
highlight = "all"
deny = []

[sources]
unknown-registry = "deny"
unknown-git = "deny"
allow-registry = ["https://github.com/rust-lang/crates.io-index"]
allow-git = []
```

### CI wiring

Append to `.github/workflows/ci.yml`:

```yaml
  cargo-deny:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: EmbarkStudios/cargo-deny-action@v2
        with:
          command: check advisories bans sources licenses
```

### Expected first-run state

Current dep graph licenses are all standard ecosystem permissive
licenses (Apache-2.0, MIT, BSD-*, ISC, Unicode-3.0, MPL-2.0). All are
in the allowlist. If anything surfaces unexpectedly, the resolution
order is:
1. Replace the dep with a permissive-licensed alternative.
2. If not possible, add to `licenses.exceptions` with a rationale
   comment.

`multiple-versions = "warn"` rather than `deny` accommodates transient
ecosystem-churn dup deps (e.g., `syn 1.x` and `2.x` coexisting). Promote
to `deny` once 1.0 ships.

---

# 3. `cargo-semver-checks` setup (informational)

Pre-1.0 per the coverage meta-spec: breaking changes per minor version
allowed. `cargo-semver-checks` runs and reports; does not gate.

### CI wiring

Append to `.github/workflows/ci.yml`:

```yaml
  cargo-semver-checks:
    runs-on: ubuntu-latest
    continue-on-error: true
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: obi1kenobi/cargo-semver-checks-action@v2
        with:
          rust-toolchain: stable
```

### What it covers

The action auto-detects publishable crates. `crabka-compression` and
`crabka-protocol` are the two checked. `crabka-protocol-codegen`
(`publish = false`) is correctly skipped.

Until 0.1.0 is actually published, the action is a no-op (nothing to
compare against). Post-publish, it compares the current branch against
the last released version on crates.io. Pre-1.0 every "break" reported
is just informational; the job stays green via `continue-on-error`.

When we ship 1.0, flip to `continue-on-error: false`.

### Non-exhaustive enums

Most of our pub enums (`CompressionType`, `CompressionError`,
`RecordsError`, `ProtocolError`, `ApiKey`) carry `#[non_exhaustive]`
from prior sub-plans, so adding variants doesn't break SemVer. Generated
message structs are NOT currently `#[non_exhaustive]`. Pre-1.0 this is
fine; 1.0 prep will revisit.

---

# 4. `release-plz` configuration + per-crate CHANGELOGs

[release-plz](https://release-plz.dev/) watches `main` for commits,
accumulates them per crate, and opens release PRs that bump versions
and update CHANGELOGs. Merging a release PR cuts the tag + GitHub
release. Optionally publishes to crates.io — we **disable** that for
1e's dry-run stance.

### `release-plz.toml` at the repo root

```toml
[workspace]
changelog_update = true
git_release_enable = true
git_tag_enable = true
publish = false              # ← flip to true when ready for crates.io
release = true
changelog_config = "release-plz-changelog.toml"
git_release_type = "auto"
semver_check = false         # informational-only per Section 3
pr_branch_prefix = "release-plz-"
pr_labels = ["release"]

[[package]]
name = "crabka-protocol-codegen"
publish = false
release = false              # internal bin

[[package]]
name = "crabka-compression"
publish = false
release = true

[[package]]
name = "crabka-protocol"
publish = false
release = true
```

### `release-plz-changelog.toml`

```toml
[changelog]
body = """
## [{{ version }}] — {{ timestamp | date(format="%Y-%m-%d") }}

{% for group, commits in commits | group_by(attribute="group") %}
### {{ group | upper_first }}

{% for commit in commits %}
- {{ commit.message | upper_first }}{% if commit.breaking %} (**breaking**){% endif %}
{% endfor %}
{% endfor %}
"""
commit_parsers = [
    { message = "^feat", group = "Added" },
    { message = "^fix", group = "Fixed" },
    { message = "^perf", group = "Performance" },
    { message = "^docs", group = "Documentation" },
    { message = "^test", group = "Tests" },
    { message = "^refactor", group = "Refactored" },
    { message = "^chore\\(deps\\)", group = "Dependencies" },
    { message = "^chore", group = "Maintenance" },
    { message = "^ci", group = "CI" },
    { message = "^build", group = "Build" },
]
filter_unconventional = false
```

### Per-crate initial CHANGELOG.md

**`crates/protocol/CHANGELOG.md`:**

```markdown
# Changelog

All notable changes to `crabka-protocol` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-11

### Added

- Wire protocol codec for Apache Kafka 4.2.0.
- Owned + borrowed flavors for every active Kafka 4.2 message (189
  message types across 604 supported `(api_key, version)` pairs).
- Typed `RecordBatch` v2 decoder/encoder with `zerocopy` header
  reinterpretation and `crabka-compression` integration.
- Central `ApiKey` enum listing every Kafka 4.2 API.
- Differential testing against `kafka-clients` 4.2.0 for every active
  `(api_key, version)` pair — all byte-equal.

### Supported Kafka versions

- Wire protocol: 4.2.0.

### MSRV

- Rust 1.95.0.
```

**`crates/compression/CHANGELOG.md`:**

```markdown
# Changelog

## [0.1.0] — 2026-05-11

### Added

- gzip via `flate2` rust_backend.
- snappy with xerial-snappy framing over `snap` raw blocks.
- lz4 frame format with independent blocks via `lz4_flex`.
- zstd via `zstd-sys`.
- Free-function API parameterised on a `CompressionType` enum matching
  Kafka's record-batch attribute bits.
- Per-codec Cargo features (default-enabled); disabled codecs return
  `CompressionError::FeatureDisabled` at runtime.
- Differential testing against Apache Kafka's compression codecs for
  every codec, both directions.

### MSRV

- Rust 1.95.0.
```

### `.github/workflows/release-plz.yml`

```yaml
name: release-plz
on:
  push:
    branches: [main]

concurrency:
  group: release-plz-${{ github.ref }}

jobs:
  release-plz:
    runs-on: ubuntu-latest
    permissions:
      contents: write
      pull-requests: write
    steps:
      - uses: actions/checkout@v6
        with:
          fetch-depth: 0
      - uses: dtolnay/rust-toolchain@stable
      - uses: MarcoIeni/release-plz-action@v0.5
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

No `CARGO_REGISTRY_TOKEN` required because `publish = false`. When
flipping to a real publish, add the token to repo secrets and reference
it here.

### Initial 0.1.0 release: manual, then release-plz takes over

Sequence after 1e merges:
1. The 1e PR lands on `main` with `version = "0.1.0"`, both crate
   CHANGELOGs seeded, release-plz config in place.
2. **Manual final task:** tag `v0.1.0` + `gh release create v0.1.0`
   using a body that summarises the seeded CHANGELOGs.
3. From 0.1.1 onwards, release-plz manages release PRs automatically.

Bootstrap is manual because the seeded 0.1.0 CHANGELOG entries span
four sub-plans of curated history — release-plz auto-generation from
git commits would produce a messier first-release changelog.

---

# 5. docs.rs and per-crate README

### `[package.metadata.docs.rs]`

Each published crate's `Cargo.toml`:

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

The `--cfg docsrs` is a common convention for opting into doc-only
items. We use it sparingly — only where a doc comment clarifies
behaviour that varies by feature configuration.

### Local docs.rs preview

Test before publishing:

```bash
RUSTDOCFLAGS="--cfg docsrs -D warnings" \
    cargo doc --workspace --no-deps --all-features
```

Must build with zero warnings. Any warning is a real defect (a broken
intra-doc link or a missing item) that needs fixing before publish.

### Per-crate README files

Each crates.io page renders the per-crate `README.md`. The
workspace-level README at the repo root is not what users see when they
land on `crates.io/crates/crabka-protocol`.

**`crates/protocol/README.md`:**

```markdown
# crabka-protocol

[![Crates.io](https://img.shields.io/crates/v/crabka-protocol.svg)](https://crates.io/crates/crabka-protocol)
[![Docs.rs](https://docs.rs/crabka-protocol/badge.svg)](https://docs.rs/crabka-protocol)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Apache Kafka wire-protocol codec for Rust. Implements every message
type Apache Kafka 4.2.0 defines (189 messages, 604 `(api_key, version)`
pairs), with byte-level wire compatibility verified against the JVM
`kafka-clients` implementation.

## Quick start

\`\`\`rust
use bytes::BytesMut;
use crabka_protocol::{Decode, Encode};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

let req = ApiVersionsRequest::default();
let mut buf = BytesMut::with_capacity(req.encoded_len(3));
req.encode(&mut buf, 3)?;

let mut cur: &[u8] = &buf;
let decoded = ApiVersionsRequest::decode(&mut cur, 3)?;
assert_eq!(decoded, req);
\`\`\`

## Features

- **Two flavors per message:** owned (`crate::owned::*`) and zero-copy
  borrowed (`crate::borrowed::*`).
- **Typed `RecordBatch` v2** via `crate::records::*`, with eager
  decompression through `crabka-compression`.
- **Central `ApiKey` enum** listing every Kafka 4.2 API.

## Cargo features

Default features enable all four compression codecs. Disable per-codec
via `--no-default-features` and selective `--features`:

\`\`\`toml
crabka-protocol = { version = "0.1", default-features = false, features = ["snappy", "zstd"] }
\`\`\`

## MSRV

Rust 1.95.0.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org);
see `NOTICE`.
```

**`crates/compression/README.md`** mirrors the same structure with
codec-focused content (covered in Section 5 of the brainstorm; same
text as appears in the brainstorm transcript).

### Top-level repo README

The repo-level `README.md` already exists. Add a "Published crates"
section linking the two crates and their docs.rs pages.

---

# 6. Pre-publish dry-run script

`tools/publish-dryrun.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "==> cargo fmt --check"
cargo fmt --check

echo "==> cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> cargo test --workspace"
cargo test --workspace

echo "==> cargo deny check"
cargo deny check

echo "==> cargo publish --dry-run for crabka-compression"
cargo publish -p crabka-compression --dry-run --allow-dirty

echo "==> cargo publish --dry-run for crabka-protocol"
cargo publish -p crabka-protocol --dry-run --allow-dirty

echo "==> rustdoc with --cfg docsrs"
RUSTDOCFLAGS="--cfg docsrs -D warnings" \
    cargo doc --workspace --no-deps --all-features

echo "==> All publish-readiness checks passed."
```

`chmod +x tools/publish-dryrun.sh`. Run locally before tagging the
release. CI runs the same checks individually (cargo-deny as its own
job, fmt/clippy/test in the rust matrix).

---

# 7. Acceptance criteria

1. Workspace `version` bumped to `0.1.0`.
2. `crabka-compression` and `crabka-protocol` carry all
   crates.io-required + recommended fields per Section 1.
3. `crabka-protocol`'s dep on `crabka-compression` declares both
   `version = "0.1"` and `path = "../compression"`.
4. Per-crate `README.md` exists with badges, quickstart, MSRV, license.
5. Per-crate `CHANGELOG.md` seeded with the `[0.1.0]` entry per Section 4.
6. `deny.toml` exists; `cargo deny check` passes; `cargo-deny` job in
   CI hard-gates.
7. `release-plz.toml` + `release-plz-changelog.toml` exist with
   `publish = false`.
8. `.github/workflows/release-plz.yml` exists.
9. `cargo-semver-checks` job runs in CI with `continue-on-error: true`.
10. `cargo publish -p crabka-compression --dry-run --allow-dirty` exits 0.
11. `cargo publish -p crabka-protocol --dry-run --allow-dirty` exits 0.
12. `RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo doc --workspace --no-deps --all-features` builds clean.
13. `tools/publish-dryrun.sh` runs end-to-end successfully.
14. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace -- --include-ignored` all green (no regression).
15. **Manual final task after PR merge:** tag `v0.1.0`; `gh release create v0.1.0` with notes pulled from the CHANGELOGs.

---

# 8. Open questions deferred to the implementation plan

- **Whether docs.rs needs special handling for the JVM-dependent
  differential tests.** They're gated by `#[ignore]`, so docs.rs's
  `cargo doc` run never executes them. No special config needed
  unless docs.rs's `cargo test` runs (it doesn't by default).
- **README badge URLs.** The repo URL is currently `robot-head/crabka`.
  Verify before publishing dry-run that the badge URLs render.
- **Whether to also seed a CHANGELOG at the workspace root.** The
  meta-spec says per-crate. Workspace-level is redundant.

None block this design.

---

# 9. Flipping from dry-run to real publish (future task)

When ready to actually publish:

1. Acquire a crates.io API token via `cargo login`.
2. Add `CARGO_REGISTRY_TOKEN` to the repo's secrets.
3. Flip `publish = false` → `publish = true` (or remove the line) in
   `release-plz.toml` (and the per-crate `[[package]]` entries).
4. Add `env: CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}`
   to the `release-plz` workflow job.
5. The next release-plz release PR will publish on merge.

That's a one-PR change. Not part of 1e.

---

# 10. Next step

Invoke `writing-plans` to produce a detailed implementation plan for
sub-plan 1e. With 1e complete, the entire coverage slice is done.
