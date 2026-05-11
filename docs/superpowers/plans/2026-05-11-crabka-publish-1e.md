# 0.1.0 Publish Prep (sub-plan 1e) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `crabka-compression` and `crabka-protocol` publish-ready at `0.1.0` (dry-run only — no upload to crates.io).

**Architecture:** Fill in crate metadata, add per-crate README + CHANGELOG, configure `cargo-deny` (hard-gated) and `cargo-semver-checks` (informational), wire `release-plz` with `publish = false`, configure docs.rs, validate via `cargo publish --dry-run` and a local `publish-dryrun.sh` script. Cut `v0.1.0` GitHub release manually as the final step (`release-plz` manages 0.1.x+ automatically).

**Tech Stack:** Rust 1.95.0 (edition 2024); `cargo-deny`, `cargo-semver-checks`, `release-plz` (existing tooling, configured here); GitHub Actions.

**Reference spec:** [`docs/superpowers/specs/2026-05-11-crabka-publish-1e-design.md`](../specs/2026-05-11-crabka-publish-1e-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch: `feature/publish-1e` (already checked out off `main`). The plan commit goes on this branch; subsequent implementation commits land on the same branch; final PR contains both.

---

## File structure

```
Cargo.toml                                 # bump workspace.version to 0.1.0
crates/compression/Cargo.toml              # metadata + docs.rs section
crates/compression/README.md               # NEW
crates/compression/CHANGELOG.md            # NEW
crates/protocol/Cargo.toml                 # metadata + docs.rs section + crabka-compression dep version
crates/protocol/README.md                  # NEW
crates/protocol/CHANGELOG.md               # NEW

deny.toml                                  # NEW (repo root)
release-plz.toml                           # NEW
release-plz-changelog.toml                 # NEW

.github/workflows/ci.yml                   # add cargo-deny + cargo-semver-checks jobs
.github/workflows/release-plz.yml          # NEW

tools/publish-dryrun.sh                    # NEW

README.md                                  # add "Published crates" section
```

---

## Phase A — Metadata

### Task 1: Bump workspace version + fill `crabka-compression` metadata

**Files:**
- Modify: `Cargo.toml` (workspace) — bump `version = "0.0.0"` to `"0.1.0"`
- Modify: `crates/compression/Cargo.toml` — fill metadata fields + `[package.metadata.docs.rs]`

- [ ] **Step 1: Bump the workspace version**

In `Cargo.toml` at the repo root, find `[workspace.package]` and change:

```toml
version = "0.0.0"
```

to:

```toml
version = "0.1.0"
```

- [ ] **Step 2: Fill `crabka-compression`'s metadata**

In `crates/compression/Cargo.toml`, replace the `[package]` section with:

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

Leave the rest of the file (`[features]`, `[dependencies]`, `[dev-dependencies]`, `[[bench]]`, etc.) unchanged.

- [ ] **Step 3: Copy LICENSE/NOTICE into the crate dirs (if not already)**

`cargo publish` packages files based on the crate's directory plus the `include` list. The workspace's top-level `LICENSE` and `NOTICE` need to be accessible from the crate dir. Two options:

1. Symlink (doesn't work on Windows for git-tracked files).
2. Copy the files into each crate dir and keep them in sync.

Take option 2:

```bash
cd "/c/Users/Matt Stone/git/crabka"
cp LICENSE crates/compression/LICENSE
cp NOTICE crates/compression/NOTICE
```

Update the `include` list to reference `LICENSE` and `NOTICE` (already done in Step 2).

- [ ] **Step 4: Verify the manifest parses**

```bash
cargo metadata --no-deps 2>&1 | tail -3
```

Expected: no errors.

- [ ] **Step 5: Verify `cargo build` still works**

```bash
cargo build -p crabka-compression
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/compression
git commit -m "chore(compression): 0.1.0 crate metadata + docs.rs config"
```

---

### Task 2: Fill `crabka-protocol` metadata + bump compression dep

**Files:**
- Modify: `crates/protocol/Cargo.toml`

- [ ] **Step 1: Replace the `[package]` section**

```toml
[package]
name = "crabka-protocol"
version = "0.1.0"
edition.workspace = true
rust-version = "1.95.0"
license = "Apache-2.0"
authors = ["The Crabka Authors"]
description = "Apache Kafka wire-protocol codec (4.2.0), with typed RecordBatch and zero-copy borrowed decode"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-protocol"
readme = "README.md"
keywords = ["kafka", "wire-protocol", "codec", "serialization", "decoder"]
categories = ["encoding", "parser-implementations"]
include = [
    "src/**/*",
    "generated/**/*",
    "schemas/**/*",
    "build.rs",
    "Cargo.toml",
    "README.md",
    "LICENSE",
    "NOTICE",
]

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

Notice the `include` list pulls in `generated/`, `schemas/`, and `build.rs` because they're needed for the crate to compile when downloaded from crates.io.

- [ ] **Step 2: Bump the `crabka-compression` dep to declare a version**

Find the dependency entry for `crabka-compression` and change it to declare both a version AND a path (cargo uses path for local builds, version when publishing):

```toml
crabka-compression = { version = "0.1", path = "../compression", default-features = false }
```

(Keep any existing features list if present.)

- [ ] **Step 3: Copy LICENSE/NOTICE**

```bash
cp LICENSE crates/protocol/LICENSE
cp NOTICE crates/protocol/NOTICE
```

- [ ] **Step 4: Verify build**

```bash
cargo build -p crabka-protocol
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol
git commit -m "chore(protocol): 0.1.0 crate metadata + docs.rs config"
```

---

## Phase B — Per-crate README + CHANGELOG

### Task 3: `crabka-compression` README + CHANGELOG

**Files:**
- Create: `crates/compression/README.md`
- Create: `crates/compression/CHANGELOG.md`

- [ ] **Step 1: Write `crates/compression/README.md`**

```markdown
# crabka-compression

[![Crates.io](https://img.shields.io/crates/v/crabka-compression.svg)](https://crates.io/crates/crabka-compression)
[![Docs.rs](https://docs.rs/crabka-compression/badge.svg)](https://docs.rs/crabka-compression)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Kafka wire-protocol compression codecs for Rust. Implements the four
codecs Apache Kafka uses on the wire — gzip, snappy, lz4, zstd — with
byte-level wire compatibility verified against the JVM `kafka-clients`
implementation.

## Quick start

\`\`\`rust
use crabka_compression::{compress, decompress, CompressionType};

let bytes = compress(CompressionType::Snappy, b"hello kafka").unwrap();
let back  = decompress(CompressionType::Snappy, &bytes).unwrap();
assert_eq!(back.as_ref(), b"hello kafka");
\`\`\`

## Features

Default features enable all four codecs. Disable individually:

\`\`\`toml
crabka-compression = { version = "0.1", default-features = false, features = ["gzip", "zstd"] }
\`\`\`

Calling a codec whose feature is off returns
`CompressionError::FeatureDisabled`.

## Kafka-specific framing

- **Snappy** uses xerial-snappy framing (Kafka does not use Google's
  official Snappy stream format).
- **LZ4** uses the LZ4 frame format with independent blocks, 64 KiB
  block size, no checksums.
- **Gzip** is plain RFC-1952.
- **Zstd** is plain zstd frame at level 3.

## MSRV

Rust 1.95.0.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see `NOTICE`.
```

- [ ] **Step 2: Write `crates/compression/CHANGELOG.md`**

```markdown
# Changelog

All notable changes to `crabka-compression` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

- [ ] **Step 3: Commit**

```bash
git add crates/compression
git commit -m "docs(compression): per-crate README and CHANGELOG for 0.1.0"
```

---

### Task 4: `crabka-protocol` README + CHANGELOG

**Files:**
- Create: `crates/protocol/README.md`
- Create: `crates/protocol/CHANGELOG.md`

- [ ] **Step 1: Write `crates/protocol/README.md`**

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
req.encode(&mut buf, 3).unwrap();

let mut cur: &[u8] = &buf;
let decoded = ApiVersionsRequest::decode(&mut cur, 3).unwrap();
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
via `--no-default-features` + selective `--features`:

\`\`\`toml
crabka-protocol = { version = "0.1", default-features = false, features = ["snappy", "zstd"] }
\`\`\`

## MSRV

Rust 1.95.0.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see `NOTICE`.
```

- [ ] **Step 2: Write `crates/protocol/CHANGELOG.md`**

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

- [ ] **Step 3: Commit**

```bash
git add crates/protocol
git commit -m "docs(protocol): per-crate README and CHANGELOG for 0.1.0"
```

---

### Task 5: Top-level README "Published crates" section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Read the current top-level README**

```bash
cat README.md
```

Look for an existing structure. There should be a brief project intro.

- [ ] **Step 2: Append a "Published crates" section**

Append (or insert in an appropriate location) to `README.md`:

```markdown
## Published crates

- [`crabka-compression`](https://crates.io/crates/crabka-compression) — Kafka wire-protocol compression codecs ([docs](https://docs.rs/crabka-compression)).
- [`crabka-protocol`](https://crates.io/crates/crabka-protocol) — Apache Kafka wire-protocol codec ([docs](https://docs.rs/crabka-protocol)).
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add Published crates section pointing to crates.io + docs.rs"
```

---

## Phase C — Supply-chain hygiene (`cargo-deny`)

### Task 6: `deny.toml` + CI job

**Files:**
- Create: `deny.toml`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Write `deny.toml` at the repo root**

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

- [ ] **Step 2: Run `cargo-deny` locally to surface any issues**

```bash
cargo install --locked cargo-deny --version 0.16.4
cargo deny check
```

(Skip the install if you already have `cargo-deny`.)

Expected: all four sections (advisories, bans, sources, licenses) pass. If a license isn't in the allowlist:
- **Preferred:** find a permissive-licensed alternative.
- **Fallback:** add a per-crate `exceptions` entry with a one-line rationale.

If a yanked dep is reported: bump it. If an advisory is reported: bump or pin around.

- [ ] **Step 3: Add the CI job**

In `.github/workflows/ci.yml`, append (under the existing `jobs:` section):

```yaml
  cargo-deny:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: EmbarkStudios/cargo-deny-action@v2
        with:
          command: check advisories bans sources licenses
```

- [ ] **Step 4: Commit**

```bash
git add deny.toml .github/workflows/ci.yml
git commit -m "ci: cargo-deny hard-gated supply-chain check"
```

---

## Phase D — `cargo-semver-checks` (informational)

### Task 7: Informational `cargo-semver-checks` CI job

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Append the job**

In `.github/workflows/ci.yml`:

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

`continue-on-error: true` makes this informational pre-1.0 — the job runs, reports in the PR UI, doesn't fail the workflow.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: cargo-semver-checks (informational pre-1.0)"
```

---

## Phase E — `release-plz`

### Task 8: `release-plz.toml` + changelog template

**Files:**
- Create: `release-plz.toml`
- Create: `release-plz-changelog.toml`

- [ ] **Step 1: Write `release-plz.toml`**

```toml
[workspace]
changelog_update = true
git_release_enable = true
git_tag_enable = true
publish = false
release = true
changelog_config = "release-plz-changelog.toml"
git_release_type = "auto"
semver_check = false
pr_branch_prefix = "release-plz-"
pr_labels = ["release"]

[[package]]
name = "crabka-protocol-codegen"
publish = false
release = false

[[package]]
name = "crabka-compression"
publish = false
release = true

[[package]]
name = "crabka-protocol"
publish = false
release = true
```

- [ ] **Step 2: Write `release-plz-changelog.toml`**

```toml
[changelog]
body = """
## [{{ version }}] — {{ timestamp | date(format=\"%Y-%m-%d\") }}

{% for group, commits in commits | group_by(attribute=\"group\") %}
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

- [ ] **Step 3: Commit**

```bash
git add release-plz.toml release-plz-changelog.toml
git commit -m "ci: release-plz configuration (publish=false dry-run stance)"
```

---

### Task 9: `release-plz` GitHub workflow

**Files:**
- Create: `.github/workflows/release-plz.yml`

- [ ] **Step 1: Write the workflow**

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

No `CARGO_REGISTRY_TOKEN` required since `publish = false`.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/release-plz.yml
git commit -m "ci: release-plz workflow"
```

---

## Phase F — Publish dry-run smoke test + verification

### Task 10: `tools/publish-dryrun.sh`

**Files:**
- Create: `tools/publish-dryrun.sh`

- [ ] **Step 1: Write the script**

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

- [ ] **Step 2: Mark executable**

```bash
chmod +x tools/publish-dryrun.sh
git update-index --chmod=+x tools/publish-dryrun.sh
```

(The `git update-index` is the Windows-friendly way to mark a file executable in git regardless of the filesystem.)

- [ ] **Step 3: Run it locally end-to-end**

```bash
./tools/publish-dryrun.sh 2>&1 | tail -30
```

Expected: every section succeeds; final line is `==> All publish-readiness checks passed.` If any step fails, fix the root cause:

- `cargo publish --dry-run` reports manifest validation issues: re-read the error, fix the offending field.
- `cargo deny` reports a license/advisory: per Task 6 Step 2 guidance.
- `cargo doc` reports broken intra-doc links: fix the doc comment in the offending file.

- [ ] **Step 4: Commit**

```bash
git add tools/publish-dryrun.sh
git commit -m "tools: pre-publish dry-run smoke script"
```

---

### Task 11: Acceptance gate

Verification only. Mark complete only when every item passes.

- [ ] `cargo fmt --check` clean
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo test --workspace` clean
- [ ] `cargo test --workspace -- --include-ignored` clean (no regressions from prior sub-plans)
- [ ] `cargo deny check` passes
- [ ] `cargo publish -p crabka-compression --dry-run --allow-dirty` exits 0
- [ ] `cargo publish -p crabka-protocol --dry-run --allow-dirty` exits 0
- [ ] `RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo doc --workspace --no-deps --all-features` builds clean
- [ ] `./tools/publish-dryrun.sh` runs end-to-end successfully
- [ ] Workspace `version = "0.1.0"` confirmed
- [ ] Both publishable crates carry full crates.io metadata (per Section 1 of the spec)
- [ ] `crabka-protocol`'s dep on `crabka-compression` declares both `version` and `path`
- [ ] Per-crate `README.md` and `CHANGELOG.md` exist with `[0.1.0]` entries
- [ ] `deny.toml` exists at repo root
- [ ] `release-plz.toml` + `release-plz-changelog.toml` exist with `publish = false`
- [ ] `.github/workflows/release-plz.yml` exists
- [ ] `cargo-deny` job in `ci.yml` exists and is configured (no `continue-on-error`)
- [ ] `cargo-semver-checks` job in `ci.yml` exists with `continue-on-error: true`
- [ ] Top-level `README.md` has the Published crates section

Once verified, open the PR:

```bash
git push -u origin feature/publish-1e
gh pr create --base main --head feature/publish-1e \
    --title "Sub-plan 1e: 0.1.0 publish prep (dry-run)" \
    --body "$(cat <<'PRBODY'
## Summary

Final sub-plan of the coverage slice. Makes `crabka-compression` and `crabka-protocol` publish-ready at version 0.1.0 (dry-run only — no upload to crates.io).

## What landed

- Workspace `version` bumped to `0.1.0`
- Both publishable crates carry full crates.io metadata + `[package.metadata.docs.rs]` config
- Per-crate `README.md` and `CHANGELOG.md` files
- `cargo-deny` hard-gated in CI (advisories deny, licenses allowlist per meta-spec, sources locked to crates.io)
- `cargo-semver-checks` informational in CI (pre-1.0 stance)
- `release-plz` configured with `publish = false` and a Keep-a-Changelog template
- `release-plz` workflow that opens release PRs on push to `main`
- `tools/publish-dryrun.sh` runs fmt/clippy/test/deny/publish-dryrun/docs end-to-end

## Manual final step after merge

Tag `v0.1.0`, run `gh release create v0.1.0 --notes-file <combined-changelog>`. `release-plz` manages 0.1.x+ from then on.

## Flipping to real crates.io publish

One-PR change documented in the spec (Section 9): set `publish = true` in `release-plz.toml`, add `CARGO_REGISTRY_TOKEN` secret, reference it in the workflow.

## Reference

Spec: `docs/superpowers/specs/2026-05-11-crabka-publish-1e-design.md` (merged in PR #27).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

---

## Phase G — Manual final step (post-merge, by hand)

After the PR merges to `main`:

1. `git checkout main && git pull origin main`
2. `git tag v0.1.0`
3. `git push origin v0.1.0`
4. `gh release create v0.1.0 --title "v0.1.0" --notes "$(cat <<'EOF'

## crabka 0.1.0

First numbered release. Pre-1.0; API may change between minor versions.

Published crates (dry-run only — not yet on crates.io):

- **crabka-compression 0.1.0** — Kafka wire-protocol compression codecs (gzip, snappy, lz4, zstd). See [CHANGELOG](crates/compression/CHANGELOG.md).
- **crabka-protocol 0.1.0** — Apache Kafka wire-protocol codec for every active 4.2 schema. See [CHANGELOG](crates/protocol/CHANGELOG.md).

MSRV: Rust 1.95.0. License: Apache-2.0.

EOF
)"`

This is a manual step, NOT a plan task — it happens after the PR merges. Documented here so it's not forgotten.

---

## Self-review against the spec

**Spec coverage:**

| Spec acceptance item | Plan task |
|---|---|
| 1. Workspace version → 0.1.0 | Task 1 Step 1 |
| 2. Crate metadata complete | Tasks 1, 2 |
| 3. `crabka-protocol` dep on `crabka-compression` has both `version` and `path` | Task 2 Step 2 |
| 4. Per-crate README | Tasks 3, 4 |
| 5. Per-crate CHANGELOG | Tasks 3, 4 |
| 6. `deny.toml` + hard-gated CI | Task 6 |
| 7. `release-plz.toml` + `release-plz-changelog.toml` (publish=false) | Task 8 |
| 8. `release-plz.yml` workflow | Task 9 |
| 9. `cargo-semver-checks` informational | Task 7 |
| 10. `cargo publish --dry-run` succeeds | Task 10 (via `publish-dryrun.sh`); Task 11 confirms |
| 11. `cargo doc --cfg docsrs` builds clean | Task 10 (in the script); Task 11 confirms |
| 12. `tools/publish-dryrun.sh` end-to-end | Task 10 |
| 13. fmt/clippy/test green | Task 11 |
| 14. v0.1.0 tagged GitHub release | Phase G (manual post-merge) |

**Placeholder scan:** No `TODO` / `TBD` in requirements. Task 10 Step 3's "fix the root cause" wording lists the specific failure classes and how to address each. Phase G is explicitly a manual post-merge step, not a placeholder.

**Type consistency:** `crabka-compression` and `crabka-protocol` referenced consistently. `release-plz.toml` and `release-plz-changelog.toml` filenames consistent across Tasks 8, 9, and the spec.

Plan is ready for execution.
