# Creusot Formal Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Functional-correctness proofs (Creusot v0.12.0) for six pure kernels — consensus core, log data-path, throttle bucket — replayed as a required CI check, with a melange/apko toolchain image for Windows dev and CI.

**Architecture:** New `crabka-verified` crate holds the kraft-core/log kernels as pure free functions over primitives; hosts call through (originals deleted). `crabka-throttle` is verified in place with its runtime shell behind `#[cfg(not(creusot))]`. A `crabka-creusot` Docker image (melange/apko, pinned tag v0.12.0) is the single toolchain artifact for local proving (Windows Docker) and the `creusot-verify` CI job, which replays checked-in why3find proof sessions.

**Tech Stack:** Creusot v0.12.0 + `creusot-std` 0.12.0 (crates.io — the renamed successor of the deprecated `creusot-contracts`; the matched pair for the tool tag), Why3/why3find, melange/apko on Wolfi, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-07-04-creusot-formal-verification-design.md`

---

## Verified facts (do not re-derive)

- `creusot-contracts` on crates.io is **deprecated** and stops at 0.8.0. The crate was renamed: **`creusot-std`** has 0.9.0–0.12.0; **0.12.0 (2026-06-12) matches tool tag v0.12.0**. Use `creusot-std = "0.12"`. Never use a git dependency — release-plz publishes these crates to crates.io.
- `cargo creusot` compiles the target crate with `--cfg creusot`; contract macros erase to no-ops under normal rustc. Because the workspace clippy gate runs `-D warnings`, every crate that mentions `cfg(creusot)` or depends on `creusot-std` needs `cargo::rustc-check-cfg=cfg(creusot)` from a `build.rs` (the crates use `[lints] workspace = true`, so per-crate `[lints.rust.unexpected_cfgs]` tables are not an option).
- Types: `NodeId = u64` (crates/voters/src/lib.rs:16), `LeaderEpoch = u32` (crates/kraft-core/src/types.rs:23), `LogEnd { last_epoch: LeaderEpoch, last_offset: i64 }` (crates/kraft-core/src/event.rs:7), `QuorumState::majority() = voters.len()/2 + 1` (crates/kraft-core/src/types.rs:77).
- `OffsetIndex.entries: Vec<(u32, u32)>` (relative_offset, position), strictly increasing by construction (crates/log/src/index.rs). `TimeIndex` has its own separate `lookup` — **out of scope, do not touch it**.
- CI "required check" semantics come from the `gatekeeper-ci` job (fails on any `failure`/`cancelled` in `needs`; `skipped` passes). New jobs slot in via the `changes` paths-filter + a `needs` entry in `gatekeeper-ci`.
- On this Windows machine, melange/apko run via the chainguard container images (see memory: bench image build); `cargo +nightly fmt --all` fails with OS 206 in deep worktrees — use `cargo +nightly fmt -p <crate>`.
- The kernels deliberately avoid `sort_unstable_by`/`binary_search_by_key` (std-spec modeling risk): HWM uses a definition-mirroring O(n²) selection (n = voter count ≤ ~7), lookup uses a hand-rolled binary search (the canonical Creusot tutorial loop).

## File structure

| File | Responsibility |
|---|---|
| `.creusot-version` | Single-source toolchain pin (`v0.12.0`) read by scripts, CI, docs |
| `packaging/melange/creusot-toolchain.yaml` | Builds the Creusot toolchain APK from Wolfi |
| `packaging/apko/creusot-toolchain.yaml` | Assembles the `crabka-creusot` image |
| `tools/build-creusot-image.sh` | melange build → apko build (mirrors `tools/build-image.sh`) |
| `tools/creusot.sh` | Wrapper: run any command inside the pinned image with the workspace mounted |
| `crates/verified/Cargo.toml`, `build.rs`, `src/lib.rs` | New `crabka-verified` crate |
| `crates/verified/src/consensus.rs` | `election_jitter_ms`, `log_is_up_to_date`, `recompute_high_watermark` + contracts |
| `crates/verified/src/log_index.rs` | `offset_index_lookup` + contracts |
| `crates/verified/src/compaction.rs` | `RecordMeta`/`BatchMeta`/`TxnDataState`/`RetainDecision`, `compute_horizon`, `retain_decision` + contracts |
| `crates/throttle/src/lib.rs` | Pure `plan_consume` + contracts; module decls only |
| `crates/throttle/src/runtime.rs` | `TokenBucket`/`ThrottleState` runtime shell, gated `#[cfg(not(creusot))]` |
| `crates/throttle/build.rs` | check-cfg registration |
| `crates/kraft-core/src/core.rs` | Call-throughs to `crabka_verified` (bodies deleted) |
| `crates/log/src/index.rs`, `src/compact.rs` | Call-through + re-exports (bodies/types deleted) |
| `.github/workflows/ci.yml` | `creusot` filter + `creusot-verify` job + gatekeeper entry |
| `.github/workflows/publish-creusot-image.yml` | Builds/publishes `ghcr.io/robot-head/crabka-creusot:<pin>` |
| `docs/verification.md` | How to prove, replay, debug, bump the pin |

## Execution batching (per CLAUDE.md: parallel subagent batches)

- **Batch 1** (parallel): Task 1 (toolchain image), Task 2 (throttle split), Task 3 (`crabka-verified` crate). Disjoint file sets. Task 1 is long-running (~30–60 min image build) — start it first.
- **Batch 2** (parallel, after Batch 1): Task 4 (kraft-core call-through), Task 5 (log call-through), Task 6 (contracts + `creusot-std`). Disjoint: kraft-core / log / verified+throttle. (All touch `Cargo.lock`; it regenerates — commit it with whichever task lands last.)
- **Batch 3**: Task 7 (author proofs in the image, check in sessions). Needs Tasks 1 and 6.
- **Batch 4** (parallel): Task 8 (CI), Task 9 (docs + wrapper).

---

### Task 1: Creusot toolchain image (melange/apko)

**Files:**
- Create: `.creusot-version`
- Create: `packaging/melange/creusot-toolchain.yaml`
- Create: `packaging/apko/creusot-toolchain.yaml`
- Create: `tools/build-creusot-image.sh`

This task has no cargo tests; its "test" is building the image and proving Creusot's own scaffold project inside it. Expect iteration on the Wolfi package list — that's normal; the smoke test is the acceptance gate.

- [ ] **Step 1: Write the version pin**

`.creusot-version` (one line, no trailing newline needed):

```
v0.12.0
```

- [ ] **Step 2: Write the melange recipe**

`packaging/melange/creusot-toolchain.yaml`:

```yaml
package:
  name: creusot-toolchain
  version: 0.12.0
  epoch: 0
  description: Creusot deductive verifier toolchain (cargo-creusot, Why3, why3find, SMT provers)
  copyright:
    - license: LGPL-2.1-or-later

environment:
  contents:
    repositories:
      - https://packages.wolfi.dev/os
    keyring:
      - https://packages.wolfi.dev/os/wolfi-signing.rsa.pub
    packages:
      - autoconf
      - automake
      - bash
      - build-base
      - busybox
      - ca-certificates-bundle
      - curl
      - git
      - gmp-dev
      - ocaml
      - opam
      - pkgconf
      - rsync
      - rustup
      - unzip
      - zlib-dev

# The toolchain is built under a FIXED HOME (/opt/creusot/home) that is
# identical at build time and runtime, because opam switches and rustup
# toolchains bake absolute paths. The tree is then copied verbatim into the
# package. Dev/CI tool: hermeticity standards are relaxed vs the broker
# images (network-enabled pipeline, same precedent as crabka.yaml's cargo build).
pipeline:
  - name: Build Creusot at the pinned tag
    runs: |
      set -eux
      export HOME=/opt/creusot/home
      mkdir -p "$HOME"
      export PATH="$HOME/.cargo/bin:$PATH"

      git clone --depth 1 --branch v0.12.0 https://github.com/creusot-rs/creusot /tmp/creusot
      cd /tmp/creusot

      # rustup: install exactly the nightly the repo pins via rust-toolchain.
      rustup-init -y --default-toolchain none --no-modify-path
      rustup show

      # opam: bare init (no sandbox inside the build container), then let
      # Creusot's own INSTALL drive Why3/why3find/prover setup.
      opam init --disable-sandboxing --bare -y
      ./INSTALL

      # Sanity inside the build env before packaging.
      cargo creusot --help

      mkdir -p "${{targets.destdir}}/opt/creusot"
      cp -a /opt/creusot/home "${{targets.destdir}}/opt/creusot/home"
```

- [ ] **Step 3: Write the apko config**

`packaging/apko/creusot-toolchain.yaml`:

```yaml
contents:
  repositories:
    - https://packages.wolfi.dev/os
  keyring:
    - https://packages.wolfi.dev/os/wolfi-signing.rsa.pub
  packages:
    - bash
    - build-base
    - busybox
    - ca-certificates-bundle
    - git
    - gmp
    - wolfi-baselayout
    - zlib
    - creusot-toolchain

environment:
  HOME: /opt/creusot/home
  PATH: /opt/creusot/home/.cargo/bin:/usr/sbin:/sbin:/usr/bin:/bin

# Root on purpose: this is a dev/CI tool that bind-mounts the workspace
# read-write and writes proof sessions back through the mount.
accounts:
  run-as: 0

entrypoint:
  command: /bin/bash -lc

archs:
  - x86_64
```

Note: if `cargo creusot` inside the image cannot find why3/why3find, the opam bin dir must be appended to `PATH`. Find it with `docker run --rm crabka-creusot:v0.12.0 "opam var bin"` and add that literal path to the apko `PATH` — then rebuild.

- [ ] **Step 4: Write the build script**

`tools/build-creusot-image.sh`:

```bash
#!/usr/bin/env bash
# Build the Creusot toolchain image (melange APK -> apko OCI tar).
# Mirrors tools/build-image.sh. Load the result with:
#   docker load < creusot-toolchain.tar
set -euo pipefail
PIN="$(cat "$(dirname "$0")/../.creusot-version")"
TAG="${1:-crabka-creusot:${PIN}}"
RUNNER="${MELANGE_RUNNER:-docker}"
WORK="$(pwd)"
mkdir -p packages

if [ ! -f melange.rsa ]; then
  melange keygen
fi

melange build packaging/melange/creusot-toolchain.yaml \
  --source-dir "$WORK" \
  --signing-key melange.rsa \
  --arch x86_64 \
  --runner "$RUNNER" \
  --out-dir packages/

apko build packaging/apko/creusot-toolchain.yaml \
  "$TAG" \
  creusot-toolchain.tar \
  --arch x86_64 \
  --repository-append "$WORK/packages" \
  --keyring-append "$WORK/melange.rsa.pub"

echo "Built image archive: creusot-toolchain.tar (tag: $TAG)"
```

- [ ] **Step 5: Build the image (Windows: chainguard containers from a staging copy)**

On this machine melange/apko run as containers, from a robocopy staging copy that excludes `.claude` and `target` (see memory: bench image build). From a Git Bash shell:

```bash
STAGE=/c/Users/MATTST~1/AppData/Local/Temp/crabka-creusot-stage
robocopy "C:\\Users\\Matt Stone\\git\\crabka\\.claude\\worktrees\\sad-cori-68d652" "$(cygpath -w "$STAGE")" //MIR //XD .claude target .git //NFL //NDL //NJH //NJS || true
cd "$STAGE"
docker run --rm --privileged -v "$PWD":/work -w /work cgr.dev/chainguard/melange keygen
docker run --rm --privileged -v "$PWD":/work -w /work cgr.dev/chainguard/melange build \
  packaging/melange/creusot-toolchain.yaml --source-dir /work \
  --signing-key melange.rsa --arch x86_64 --out-dir packages/
docker run --rm -v "$PWD":/work -w /work cgr.dev/chainguard/apko build \
  packaging/apko/creusot-toolchain.yaml crabka-creusot:v0.12.0 creusot-toolchain.tar \
  --arch x86_64 --repository-append /work/packages --keyring-append /work/melange.rsa.pub
docker load < creusot-toolchain.tar
```

Expected: `docker load` prints `Loaded image: crabka-creusot:v0.12.0` (possibly arch-suffixed; if the tar contains an index, `docker load` output names the loadable tag — use that tag below and retag with `docker tag <loaded> crabka-creusot:v0.12.0`).

- [ ] **Step 6: Smoke-test — prove Creusot's own scaffold end-to-end**

```bash
docker run --rm crabka-creusot:v0.12.0 "cargo creusot --help"
docker run --rm crabka-creusot:v0.12.0 \
  "cd /tmp && cargo creusot new demo && cd demo && cargo creusot"
```

Expected: `--help` prints the cargo-creusot usage; the demo project builds to Coma and the provers discharge its obligations (output reports proved goals, exit 0). **While here, record two facts needed by Tasks 6–7** (paste them into the task notes/commit message):
1. The exact `creusot-std` import line and Cargo.toml dependency entry that `cargo creusot new` generates (e.g. `use creusot_std::prelude::*;`).
2. Where proof artifacts/sessions live in the scaffold (e.g. a `verif/` dir) and what command replays without re-searching (check `cargo creusot --help` for the prove/replay subcommands and flags).

- [ ] **Step 7: Commit**

```bash
git add .creusot-version packaging/melange/creusot-toolchain.yaml packaging/apko/creusot-toolchain.yaml tools/build-creusot-image.sh
git commit -m "feat: melange/apko Creusot toolchain image (pinned v0.12.0)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Split crabka-throttle into pure kernel + runtime module

Pure refactor — zero behavior change, no Creusot anything yet (that's Task 6). The existing tests are the safety net.

**Files:**
- Modify: `crates/throttle/src/lib.rs`
- Create: `crates/throttle/src/runtime.rs`

- [ ] **Step 1: Create `crates/throttle/src/runtime.rs`**

Move, **verbatim and unchanged**, from the current `crates/throttle/src/lib.rs` into `runtime.rs`: `clock_nanos` (lines 8–17), `TokenBucket` struct + its `Debug` impl (lines 19–46), the whole `impl TokenBucket` block and `impl Default for TokenBucket` (lines 59–190), `ThrottleState` + impls (lines 192–214), and the entire `mod tests` block (lines 216–466 — it pokes the private `generation` field, so it must live beside the struct). Do NOT move `plan_consume` or `mod plan_fuzz`. Add this header at the top of `runtime.rs`:

```rust
//! The concurrent [`TokenBucket`] runtime shell around the pure
//! [`plan_consume`] arithmetic: atomics, the seqlock generation protocol, and
//! the injected [`NanoClock`]. Split from `lib.rs` so the pure kernel is the
//! only thing the Creusot verifier ever sees (this module uses atomics and
//! `dyn` trait objects, which Creusot cannot translate).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering, Ordering::Relaxed};

use qubit_clock::{NanoClock, NanoMonotonicClock};

use crate::plan_consume;
```

(The moved `mod tests` keeps `use super::*;`, which now resolves `plan_consume` via the `use crate::plan_consume;` above — but its `plan_consume_grants_and_caps` test moves back to `lib.rs`, next step.)

- [ ] **Step 2: Rewrite `crates/throttle/src/lib.rs`**

Keep the crate doc, `plan_consume` (verbatim, lines 48–57), and `mod plan_fuzz` (verbatim, lines 468–490). Move the `plan_consume_grants_and_caps` test (lines 274–288) out of the old `mod tests` into a new `mod plan_tests` in `lib.rs`. Resulting `lib.rs` shape:

```rust
//! Shared KIP-73 token bucket rate limiter.

mod runtime;

pub use runtime::{ThrottleState, TokenBucket};

/// Pure token-bucket consume arithmetic. Given the current `available`, the
/// `refill` claimed for this call, the `burst` cap, and `requested` tokens,
/// return `(grant, new_available)` where `capped = (available + refill).min(burst)`,
/// `grant = requested.min(capped)`, and `new_available = capped - grant`.
#[must_use]
pub fn plan_consume(available: u64, refill: u64, burst: u64, requested: u64) -> (u64, u64) {
    let capped = available.saturating_add(refill).min(burst);
    let grant = requested.min(capped);
    (grant, capped - grant)
}

#[cfg(test)]
mod plan_tests {
    use assert2::assert;

    use super::plan_consume;

    #[test]
    fn plan_consume_grants_and_caps() {
        for ((available, refill, burst, requested), want) in [
            ((100, 0, 1000, 50), (50, 50)),
            ((100, 0, 1000, 200), (100, 0)),
            ((900, 500, 1000, 200), (200, 800)),
            ((0, 0, 1000, 100), (0, 0)),
            ((u64::MAX, u64::MAX, 1000, 1000), (1000, 0)),
        ] {
            assert!(
                plan_consume(available, refill, burst, requested) == want,
                "plan_consume({available}, {refill}, {burst}, {requested})"
            );
        }
    }
}

#[cfg(test)]
mod plan_fuzz {
    // ... moved verbatim from the old lib.rs lines 468-490 ...
}
```

- [ ] **Step 3: Verify — tests, clippy, fmt**

```bash
cargo test -p crabka-throttle
cargo clippy -p crabka-throttle --all-targets -- -D warnings
cargo +nightly fmt -p crabka-throttle
```

Expected: all existing tests pass (including `tests/bucket_model.rs`, which imports `crabka_throttle::plan_consume` — unchanged public API), clippy clean.

- [ ] **Step 4: Commit**

```bash
git add crates/throttle/src/lib.rs crates/throttle/src/runtime.rs
git commit -m "refactor(throttle): split TokenBucket runtime shell from pure plan_consume kernel

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Create the crabka-verified crate (kernels + oracle tests, no contracts yet)

**Files:**
- Create: `crates/verified/Cargo.toml`
- Create: `crates/verified/src/lib.rs`
- Create: `crates/verified/src/consensus.rs`
- Create: `crates/verified/src/log_index.rs`
- Create: `crates/verified/src/compaction.rs`

TDD note: the oracle tests (Step 2) are written first and encode "behaves exactly like the production implementations being replaced" — sort-based selection and `binary_search_by_key`. Write them, watch them fail to compile (functions don't exist), then implement.

- [ ] **Step 1: Create `crates/verified/Cargo.toml`**

```toml
[package]
name = "crabka-verified"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Formally verified pure kernels (Creusot) shared by Crabka's consensus and log crates"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-verified"
keywords = ["kafka", "verification", "creusot", "crabka"]
categories = ["algorithms"]

[lints]
workspace = true

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
```

- [ ] **Step 2: Write the failing oracle tests**

Bottom of `crates/verified/src/consensus.rs` (create the file with just the tests for now; the `use super::*` targets Step 4's implementations):

```rust
#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use proptest::prelude::*;

    use super::*;

    /// The production implementation this kernel replaced: sort descending,
    /// take the majority-th largest, gate on epoch_start, clamp monotonic.
    fn hwm_sort_oracle(
        log_end: i64,
        follower_offsets: &[i64],
        majority: usize,
        epoch_start_offset: i64,
        current_hwm: i64,
    ) -> i64 {
        let mut match_offsets: Vec<i64> = Vec::with_capacity(follower_offsets.len() + 1);
        match_offsets.push(log_end);
        match_offsets.extend_from_slice(follower_offsets);
        match_offsets.sort_unstable_by(|a, b| b.cmp(a));
        let majority_offset = match_offsets[majority - 1];
        let gated = if majority_offset > epoch_start_offset {
            majority_offset
        } else {
            current_hwm
        };
        gated.max(current_hwm)
    }

    proptest! {
        #[test]
        fn hwm_matches_sort_oracle(
            log_end in 0i64..1_000,
            followers in proptest::collection::vec(0i64..1_000, 0..7),
            majority_seed in 0usize..8,
            epoch_start_offset in 0i64..1_000,
            current_hwm in 0i64..1_000,
        ) {
            let majority = 1 + majority_seed % (followers.len() + 1);
            // Kernel precondition domain: clamp like the kraft-core call site does.
            let followers: Vec<i64> = followers.iter().map(|o| (*o).min(log_end)).collect();
            let current_hwm = current_hwm.min(log_end);
            prop_assert_eq!(
                recompute_high_watermark(log_end, &followers, majority, epoch_start_offset, current_hwm),
                hwm_sort_oracle(log_end, &followers, majority, epoch_start_offset, current_hwm)
            );
        }

        #[test]
        fn jitter_in_range(me in any::<u64>(), epoch in any::<u32>(), base in 1u64..10_000) {
            prop_assert!(election_jitter_ms(me, epoch, base) < base);
        }
    }

    #[test]
    fn jitter_zero_base_is_zero() {
        assert!(election_jitter_ms(7, 3, 0) == 0);
    }

    #[test]
    fn up_to_date_is_the_kip595_rule() {
        // higher epoch wins regardless of offset
        check!(log_is_up_to_date(5, 100, 6, 0));
        // same epoch: candidate offset must be >= ours
        check!(log_is_up_to_date(5, 100, 5, 100));
        check!(!log_is_up_to_date(5, 100, 5, 99));
        // lower epoch never wins
        check!(!log_is_up_to_date(5, 0, 4, i64::MAX));
    }

    #[test]
    fn hwm_never_regresses_and_gates_on_epoch_start() {
        // majority offset (2 of {10, 3, 9} with majority=2 -> 9) is <= epoch_start 9: hold.
        check!(recompute_high_watermark(10, &[3, 9], 2, 9, 5) == 5);
        // majority offset 9 > epoch_start 8: advance.
        check!(recompute_high_watermark(10, &[3, 9], 2, 8, 5) == 9);
        // a fallen follower offset can't drag the HWM back down.
        check!(recompute_high_watermark(10, &[1, 1], 2, 0, 7) == 7);
    }
}
```

Bottom of `crates/verified/src/log_index.rs` (same pattern):

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use proptest::prelude::*;

    use super::*;

    /// The production implementation this kernel replaced (index.rs lookup).
    fn binary_search_oracle(entries: &[(u32, u32)], target: u32) -> u32 {
        match entries.binary_search_by_key(&target, |&(rel, _)| rel) {
            Ok(i) => entries[i].1,
            Err(0) => 0,
            Err(i) => entries[i - 1].1,
        }
    }

    proptest! {
        #[test]
        fn lookup_matches_binary_search_oracle(
            rels in proptest::collection::btree_set(0u32..10_000, 0..64),
            target in 0u32..10_000,
        ) {
            // btree_set gives strictly-sorted unique keys, matching the
            // OffsetIndex construction invariant.
            let entries: Vec<(u32, u32)> =
                rels.iter().enumerate().map(|(i, r)| (*r, i as u32 * 17)).collect();
            prop_assert_eq!(offset_index_lookup(&entries, target), binary_search_oracle(&entries, target));
        }
    }

    #[test]
    fn empty_index_returns_zero() {
        assert!(offset_index_lookup(&[], 42) == 0);
    }
}
```

- [ ] **Step 3: Run to verify the tests fail**

```bash
cargo test -p crabka-verified
```

Expected: FAIL to compile — `recompute_high_watermark`, `election_jitter_ms`, `log_is_up_to_date`, `offset_index_lookup` not found. (You'll need minimal `src/lib.rs` module decls from Step 5 first for the crate to exist; that's fine — add them now.)

- [ ] **Step 4: Implement the kernels**

Top of `crates/verified/src/consensus.rs` (above the tests):

```rust
//! KIP-595 consensus decision kernels, extracted from `crabka-kraft-core` so
//! Creusot can verify them (the host crate's `Instant`/async surface is
//! untranslatable). Contracts are added in a follow-up task; the bodies here
//! are already written in the loop style the proofs need (no std sort).

/// Deterministic per-`(node, epoch)` election-timeout jitter in `[0, base_ms)`
/// — Raft's randomized backoff, made reproducible for the deterministic sims.
/// Different nodes (and the same node across re-election epochs) get different
/// spreads, so closely-synchronized voters don't arm their election timers in
/// lockstep and split the vote indefinitely.
#[must_use]
pub fn election_jitter_ms(me: u64, epoch: u32, base_ms: u64) -> u64 {
    if base_ms == 0 {
        return 0;
    }
    // Cheap integer hash of (node id, epoch); avoids any RNG so the sims stay
    // deterministic.
    let mix = me.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(epoch).wrapping_mul(0xD1B5_4A32_D192_ED03);
    mix % base_ms
}

/// `true` if the candidate's log is at least as up-to-date as ours
/// (KIP-595: higher last epoch wins; on tie, higher/equal offset wins).
#[must_use]
pub fn log_is_up_to_date(my_epoch: u32, my_end: i64, cand_epoch: u32, cand_offset: i64) -> bool {
    cand_epoch > my_epoch || (cand_epoch == my_epoch && cand_offset >= my_end)
}

/// The HWM as the majority-th largest match offset across the leader's own
/// log end and every follower's acknowledged fetch offset, gated on the
/// leader-completeness rule (Raft Fig.8 / KIP-595): the HWM may only advance
/// once the majority offset is strictly past `epoch_start_offset`. Never
/// regresses below `current_hwm`.
///
/// The majority-th largest is computed by its definition — the greatest
/// member m of `{log_end} ∪ follower_offsets` with at least `majority`
/// members ≥ m — rather than by sorting: voter counts are tiny (≤ ~7), and a
/// definition-mirroring loop is what the Creusot proof quantifies over.
#[must_use]
pub fn recompute_high_watermark(
    log_end: i64,
    follower_offsets: &[i64],
    majority: usize,
    epoch_start_offset: i64,
    current_hwm: i64,
) -> i64 {
    let n = follower_offsets.len();
    let mut majority_offset = i64::MIN;
    let mut i = 0;
    while i <= n {
        let cand = if i == 0 { log_end } else { follower_offsets[i - 1] };
        if cand > majority_offset {
            let mut count: usize = 0;
            let mut j = 0;
            while j <= n {
                let x = if j == 0 { log_end } else { follower_offsets[j - 1] };
                if x >= cand {
                    count += 1;
                }
                j += 1;
            }
            if count >= majority {
                majority_offset = cand;
            }
        }
        i += 1;
    }
    let gated = if majority_offset > epoch_start_offset {
        majority_offset
    } else {
        current_hwm
    };
    gated.max(current_hwm)
}
```

Top of `crates/verified/src/log_index.rs`:

```rust
//! Offset-index lookup kernel, extracted from `crabka-log`'s `OffsetIndex` so
//! Creusot can verify it. Hand-rolled binary search (the canonical Creusot
//! loop) instead of `binary_search_by_key`, so the proof doesn't depend on
//! std's search being modeled.

/// The byte position to start reading at for `target`: the position field of
/// the largest entry with `relative_offset <= target`, or 0 if none exists.
/// `entries` must be strictly sorted by relative offset (true by construction
/// of `OffsetIndex`).
#[must_use]
pub fn offset_index_lookup(entries: &[(u32, u32)], target: u32) -> u32 {
    let mut lo = 0usize; // entries[..lo] all have rel <= target
    let mut hi = entries.len(); // entries[hi..] all have rel > target
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].0 <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 { 0 } else { entries[lo - 1].1 }
}
```

`crates/verified/src/compaction.rs` — the KIP-534 core moves from `crates/log/src/compact.rs` with visibility widened from `pub(crate)` to `pub` and doc comments carried over verbatim:

```rust
//! KIP-534 log-compaction decision core, extracted from `crabka-log` so
//! Creusot can verify it. The host crate re-exports these; the stateright
//! model in `crabka-log/src/compact_model.rs` drives these exact functions.

/// Per-record facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordMeta {
    pub has_key: bool,
    pub has_value: bool,
}

/// Per-batch facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchMeta {
    pub is_control: bool,
    pub producer_id: i64,
    /// The batch's existing delete horizon (`base_timestamp` when bit 6 is
    /// set), `None` if the batch has never been stamped.
    pub existing_horizon: Option<i64>,
}

/// Whether a producer's transactional DATA still survives compaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnDataState {
    /// `producer_id < 0`: not a transactional producer.
    NotTransactional,
    /// At least one of this producer's data records survives compaction.
    DataSurvives,
    /// All of this producer's data records have been compacted away.
    DataFullyGone,
}

/// What to do with a record during the rewrite pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainDecision {
    /// Keep the record as-is.
    Keep,
    /// Keep the record but stamp its batch with this delete horizon
    /// (`base_timestamp = horizon`, bit 6 set).
    SetHorizon(i64),
    /// Drop the record.
    Delete,
}

/// Compute the delete horizon timestamp: `now + delete.retention.ms`. The
/// tombstone/marker is retained until wall-clock reaches this value.
#[must_use]
pub fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    now_ms.saturating_add(delete_retention_ms)
}

/// The single per-record KIP-534 retain decision.
///
/// Control batches (txn commit/abort markers) are retained as long as their
/// transaction's data survives; once the data is fully compacted away the
/// marker ages out via the delete horizon. Data records dedup newest-wins;
/// tombstones (null value) age out via the delete horizon once they are the
/// newest entry for their key.
#[must_use]
pub fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        return match txn {
            TxnDataState::DataSurvives | TxnDataState::NotTransactional => RetainDecision::Keep,
            TxnDataState::DataFullyGone => match batch.existing_horizon {
                Some(h) if now_ms >= h => RetainDecision::Delete,
                Some(_) => RetainDecision::Keep,
                None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
            },
        };
    }
    if !rec.has_key {
        return RetainDecision::Delete;
    }
    if !is_newest_for_key {
        return RetainDecision::Delete;
    }
    if rec.has_value {
        return RetainDecision::Keep;
    }
    // Newest-for-key tombstone: age out via the delete horizon.
    match batch.existing_horizon {
        Some(h) if now_ms >= h => RetainDecision::Delete,
        Some(_) => RetainDecision::Keep,
        None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
    }
}
```

(No new unit tests for `retain_decision` here — its exhaustive `core_tests` and the stateright model stay in `crabka-log` and keep running against this exact function through the re-export, per Task 5.)

- [ ] **Step 5: Write `crates/verified/src/lib.rs`**

```rust
//! Formally verified pure kernels shared by Crabka's consensus and log crates.
//!
//! Every function here is a total, synchronous, allocation-light kernel whose
//! functional contract is proven with Creusot (see `docs/verification.md`).
//! Host crates call through — there are no duplicate bodies anywhere.
#![doc(html_root_url = "https://docs.rs/crabka-verified/0.3.8")]

pub mod compaction;
pub mod consensus;
pub mod log_index;

pub use compaction::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision,
};
pub use consensus::{election_jitter_ms, log_is_up_to_date, recompute_high_watermark};
pub use log_index::offset_index_lookup;
```

Check the exact `html_root_url` format against a sibling (e.g. `crates/kraft-core/src/lib.rs`) and run `./tools/html-root-url.sh` (bash) — CI gates on it.

- [ ] **Step 6: Run tests to verify they pass**

```bash
cargo test -p crabka-verified
cargo clippy -p crabka-verified --all-targets -- -D warnings
cargo +nightly fmt -p crabka-verified
./tools/html-root-url.sh
```

Expected: all tests PASS (oracle proptests prove behavior-equivalence to the production implementations), clippy clean, html_root_url in sync.

- [ ] **Step 7: Commit**

```bash
git add crates/verified Cargo.lock
git commit -m "feat: crabka-verified crate with consensus, log-index, and compaction kernels

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: kraft-core call-through

**Files:**
- Modify: `crates/kraft-core/Cargo.toml`
- Modify: `crates/kraft-core/src/core.rs` (lines 19–29, 90–94, 224–260)

- [ ] **Step 1: Add the dependency**

In `crates/kraft-core/Cargo.toml` `[dependencies]` (keep the block's comment about the crate being a sans-IO leaf — `crabka-verified` is pure and preserves that):

```toml
crabka-verified = { version = "0.3.8", path = "../verified" }
```

- [ ] **Step 2: Replace `election_jitter_ms` body (core.rs lines 19–29)**

Keep the existing doc comment (lines 12–18) exactly. Replace the function with:

```rust
#[must_use]
pub fn election_jitter_ms(me: NodeId, epoch: LeaderEpoch, base_ms: u64) -> u64 {
    crabka_verified::election_jitter_ms(me, epoch, base_ms)
}
```

(`NodeId = u64`, `LeaderEpoch = u32`, so this is a direct delegation. The `pub use core::{QuorumStateMachine, election_jitter_ms}` re-export in lib.rs:31 and the `crabka-raft` call sites are unchanged.)

- [ ] **Step 3: Replace `log_is_up_to_date` body (core.rs lines 88–94)**

Keep the doc comment. Replace with:

```rust
fn log_is_up_to_date(log: &dyn LogView, cand: LogEnd) -> bool {
    crabka_verified::log_is_up_to_date(
        log.last_epoch(),
        log.end_offset(),
        cand.last_epoch,
        cand.last_offset,
    )
}
```

- [ ] **Step 4: Replace `recompute_high_watermark` body (core.rs lines 224–260)**

Keep the method doc comment (lines 214–223). Replace the body with:

```rust
    fn recompute_high_watermark(&self, log_end: i64) -> i64 {
        let Role::Leader {
            replicas,
            high_watermark,
            epoch_start_offset,
        } = &self.role
        else {
            return 0;
        };
        // Clamp inputs into the verified kernel's precondition domain: a
        // follower's acknowledged offset never legitimately exceeds the
        // leader's log end, and the leader's HWM is always within its log.
        // Both are invariants of correct operation; clamping makes them
        // locally evident instead of a distributed assumption.
        let follower_offsets: Vec<i64> = replicas
            .values()
            .map(|progress| progress.fetch_offset.min(log_end))
            .collect();
        let new_hwm = crabka_verified::recompute_high_watermark(
            log_end,
            &follower_offsets,
            self.state.majority(),
            *epoch_start_offset,
            (*high_watermark).min(log_end),
        );
        debug_assert!(
            new_hwm <= log_end,
            "HWM {new_hwm} must not exceed leader log end {log_end}"
        );
        new_hwm
    }
```

- [ ] **Step 5: Verify — kraft-core, raft (stateright kraft_model), wasm leaf**

```bash
cargo test -p crabka-kraft-core
cargo test -p crabka-raft
cargo clippy -p crabka-kraft-core -p crabka-raft --all-targets -- -D warnings
rustup target add wasm32-unknown-unknown
cargo check -p crabka-kraft-core --target wasm32-unknown-unknown
cargo +nightly fmt -p crabka-kraft-core
```

Expected: all PASS. `crabka-raft`'s `kraft_model.rs` stateright suite now drives the verified kernel through this call path — if any of its properties fail, the delegation changed behavior: stop and diff against the original bodies (the only intended delta is the clamps, which are identity under the model's invariants). The wasm check guards the "sans-IO leaf" promise in kraft-core's Cargo.toml.

- [ ] **Step 6: Commit**

```bash
git add crates/kraft-core/Cargo.toml crates/kraft-core/src/core.rs Cargo.lock
git commit -m "refactor(kraft-core): delegate consensus kernels to crabka-verified

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: log call-through

**Files:**
- Modify: `crates/log/Cargo.toml`
- Modify: `crates/log/src/index.rs` (lines 92–103)
- Modify: `crates/log/src/compact.rs` (lines 39–77, 86–90, 123–163 deleted; re-export added)

- [ ] **Step 1: Add the dependency**

In `crates/log/Cargo.toml` `[dependencies]`:

```toml
crabka-verified = { version = "0.3.8", path = "../verified" }
```

- [ ] **Step 2: Delegate `OffsetIndex::lookup` (index.rs lines 92–103)**

Keep the doc comment. Replace the method body with:

```rust
    #[must_use]
    pub fn lookup(&self, target: u32) -> u32 {
        crabka_verified::offset_index_lookup(&self.entries, target)
    }
```

Do NOT touch `TimeIndex` or its `lookup` — it is a different index and out of scope.

- [ ] **Step 3: Replace the compaction core with re-exports (compact.rs)**

Delete from `compact.rs`: the `RecordMeta`, `BatchMeta`, `TxnDataState`, `RetainDecision` definitions (lines 39–77), `compute_horizon` (lines 86–90), and `retain_decision` (lines 123–163). Keep `should_index_key`, `rewrite_batch_horizon`, and `txn_data_fully_gone` (out of scope). In their place, under the existing "KIP-534 pure decision cores" banner comment (keep the banner, update its text):

```rust
// ---------------------------------------------------------------------------
// KIP-534 pure decision cores
//
// The retain/horizon core now lives in `crabka-verified`, where its contract
// is proven with Creusot. Re-exported `pub(crate)` so `compact_model.rs` and
// `core_tests` keep driving the exact production functions.
// ---------------------------------------------------------------------------

pub(crate) use crabka_verified::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision,
};
```

- [ ] **Step 4: Verify — the moved code's tests still exercise the same functions**

```bash
cargo test -p crabka-log
cargo clippy -p crabka-log --all-targets -- -D warnings
cargo +nightly fmt -p crabka-log
```

Expected: all PASS — in particular `compact.rs`'s `core_tests`, the `compact_model.rs` stateright suite, and the segment tests that hit `lookup` (index.rs tests at lines 166–218, segment.rs callers). These now drive the `crabka-verified` kernels through the re-exports/delegation, which is exactly the drift protection the spec requires.

- [ ] **Step 5: Commit**

```bash
git add crates/log/Cargo.toml crates/log/src/index.rs crates/log/src/compact.rs Cargo.lock
git commit -m "refactor(log): delegate offset-index lookup and KIP-534 core to crabka-verified

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Contracts (`creusot-std`) on crabka-verified and crabka-throttle

**Files:**
- Modify: `crates/verified/Cargo.toml`, `crates/verified/src/{lib,consensus,log_index,compaction}.rs`
- Create: `crates/verified/build.rs`
- Modify: `crates/throttle/Cargo.toml`, `crates/throttle/src/lib.rs`
- Create: `crates/throttle/build.rs`

Pearlite caveat (applies to every contract below): the clause *meanings* are fixed by the spec; the surface syntax (`@` model projection, `Seq` indexing, `#[logic]` visibility, prelude path) must match `creusot-std` 0.12 — validate against the `cargo creusot new` scaffold recorded in Task 1 Step 6 and the guide (https://guide.creusot.rs), and adjust syntax (never meaning) as needed. On stable rustc all these attributes erase; the acceptance gate for this task is stable builds staying green. Proving is Task 7.

- [ ] **Step 1: Add `creusot-std` + check-cfg to both crates**

`crates/verified/Cargo.toml` and `crates/throttle/Cargo.toml`, in `[dependencies]`:

```toml
creusot-std = "0.12"
```

`crates/verified/build.rs` and `crates/throttle/build.rs` (identical content):

```rust
fn main() {
    // `cargo creusot` compiles with --cfg creusot; register the cfg so the
    // workspace clippy gate (-D warnings, unexpected_cfgs) stays quiet on
    // stable builds.
    println!("cargo::rustc-check-cfg=cfg(creusot)");
}
```

- [ ] **Step 2: Confirm `creusot-std` erases on stable (spec risk 1)**

```bash
cargo check -p crabka-verified -p crabka-throttle
```

Expected: clean build on stable 1.96. If `creusot-std` itself fails to compile on stable, apply the spec's fallback: make it `optional = true` behind a `creusot-proofs` feature, wrap every contract attribute in `#[cfg_attr(creusot, ...)]`, and have Task 7 pass `--features creusot-proofs` to `cargo creusot`. Do not proceed with a git dependency under any circumstances.

- [ ] **Step 3: Gate the throttle runtime module and add `plan_consume` contracts**

In `crates/throttle/src/lib.rs`, change the module decl and re-export to:

```rust
#[cfg(not(creusot))]
mod runtime;

#[cfg(not(creusot))]
pub use runtime::{ThrottleState, TokenBucket};
```

Add above `plan_consume` (with `use creusot_std::prelude::*;` at the top of the file, adjusted to the scaffold's import):

```rust
use creusot_std::prelude::*;

/// `min(available + refill, burst)` in unbounded integers. Equal to the
/// executable `available.saturating_add(refill).min(burst)` whenever
/// `burst <= u64::MAX`, i.e. always.
#[cfg(creusot)]
#[logic]
fn capped(available: Int, refill: Int, burst: Int) -> Int {
    if available + refill <= burst { available + refill } else { burst }
}

#[ensures(result.0@ <= requested@)]
#[ensures(result.1@ <= burst@)]
#[ensures(result.0@ + result.1@ == capped(available@, refill@, burst@))]
#[ensures(result.0@ == if requested@ <= capped(available@, refill@, burst@) {
    requested@
} else {
    capped(available@, refill@, burst@)
})]
#[must_use]
pub fn plan_consume(available: u64, refill: u64, burst: u64, requested: u64) -> (u64, u64) {
    let capped = available.saturating_add(refill).min(burst);
    let grant = requested.min(capped);
    (grant, capped - grant)
}
```

- [ ] **Step 4: Contracts on `consensus.rs`**

```rust
use creusot_std::prelude::*;
```

`election_jitter_ms` gains:

```rust
#[ensures(base_ms@ == 0 ==> result@ == 0)]
#[ensures(base_ms@ > 0 ==> result@ < base_ms@)]
```

`log_is_up_to_date` gains the full functional spec:

```rust
#[ensures(result == (cand_epoch@ > my_epoch@
    || (cand_epoch@ == my_epoch@ && cand_offset@ >= my_end@)))]
```

`recompute_high_watermark` gains a counting logic function plus the spec's three clauses:

```rust
/// Members of `{log_end} ∪ s` with value >= v (the majority-replication witness).
#[cfg(creusot)]
#[logic]
#[variant(s.len())]
fn count_ge(log_end: Int, s: Seq<i64>, v: Int) -> Int {
    pearlite! {
        (if log_end >= v { 1 } else { 0 })
            + count_ge_seq(s, v)
    }
}

#[cfg(creusot)]
#[logic]
#[variant(s.len())]
fn count_ge_seq(s: Seq<i64>, v: Int) -> Int {
    pearlite! {
        if s.len() == 0 { 0 }
        else {
            (if s[0]@ >= v { 1 } else { 0 }) + count_ge_seq(s.subsequence(1, s.len()), v)
        }
    }
}

#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[requires(current_hwm@ <= log_end@)]
#[requires(forall<k: Int> 0 <= k && k < follower_offsets@.len()
    ==> follower_offsets@[k]@ <= log_end@)]
#[ensures(result@ >= current_hwm@)]
#[ensures(result@ <= log_end@)]
#[ensures(result@ > current_hwm@
    ==> result@ > epoch_start_offset@
        && count_ge(log_end@, follower_offsets@, result@) >= majority@)]
```

- [ ] **Step 5: Contracts on `log_index.rs`**

```rust
use creusot_std::prelude::*;

#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ < entries@[j].0@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].0@ <= target@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && entries@[i].0@ <= target@
        && result@ == entries@[i].1@
        && (forall<j: Int> i < j && j < entries@.len() ==> entries@[j].0@ > target@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ > target@)
    ==> result@ == 0)]
```

- [ ] **Step 6: Contracts on `compaction.rs`**

The types need model derives under verification (per the scaffold/guide — typically `#[cfg_attr(creusot, derive(DeepModel))]` or nothing at all if pearlite structural equality suffices in 0.12; use whatever the guide's enum-matching chapter prescribes). `retain_decision` gains the full case-space spec:

```rust
use creusot_std::prelude::*;

#[ensures(batch.is_control && (txn == TxnDataState::DataSurvives || txn == TxnDataState::NotTransactional)
    ==> result == RetainDecision::Keep)]
#[ensures(batch.is_control && txn == TxnDataState::DataFullyGone && batch.existing_horizon == None
    ==> result == RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)))]
#[ensures(forall<h: i64> batch.is_control && txn == TxnDataState::DataFullyGone
        && batch.existing_horizon == Some(h)
    ==> result == (if now_ms@ >= h@ { RetainDecision::Delete } else { RetainDecision::Keep }))]
#[ensures(!batch.is_control && !rec.has_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && !is_newest_for_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && rec.has_value
    ==> result == RetainDecision::Keep)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && !rec.has_value
        && batch.existing_horizon == None
    ==> result == RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)))]
#[ensures(forall<h: i64> !batch.is_control && rec.has_key && is_newest_for_key && !rec.has_value
        && batch.existing_horizon == Some(h)
    ==> result == (if now_ms@ >= h@ { RetainDecision::Delete } else { RetainDecision::Keep }))]
```

(`compute_horizon` also needs to be callable from pearlite — mark it with the guide's mechanism for "logic-visible program function", e.g. `#[logic]`-paired or `#[ensures(result@ == ...)]` on itself: give it `#[ensures(result@ == if now_ms@ + delete_retention_ms@ <= i64::MAX@ { now_ms@ + delete_retention_ms@ } else { i64::MAX@ })]` and reference the saturation explicitly in the SetHorizon clauses if direct calls aren't allowed.)

- [ ] **Step 7: Verify stable builds are fully unaffected**

```bash
cargo test -p crabka-verified -p crabka-throttle
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt -p crabka-verified
cargo +nightly fmt -p crabka-throttle
```

Expected: all tests PASS, workspace clippy clean (workspace-wide per the shared-test-support gotcha, not `-p` only — this also proves no unexpected-cfg leakage into dependents).

- [ ] **Step 8: Commit**

```bash
git add crates/verified crates/throttle Cargo.lock
git commit -m "feat: Creusot contracts on the verified kernels and plan_consume

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Prove both packages and check in proof sessions

**Files:**
- Modify: `crates/verified/src/*.rs` (loop invariants, `proof_assert!` lemmas — bodies/contracts unchanged)
- Create: proof session artifacts under `crates/verified/` and `crates/throttle/` (exact layout per the Task 1 Step 6 recording — typically a `verif/` dir)

- [ ] **Step 1: Run the verifier on both packages via the image**

```bash
docker volume create crabka-creusot-target
docker run --rm -v "$PWD:/work" -w /work \
  -v crabka-creusot-target:/ctarget -e CARGO_TARGET_DIR=/ctarget \
  crabka-creusot:v0.12.0 "cd crates/verified && cargo creusot"
docker run --rm -v "$PWD:/work" -w /work \
  -v crabka-creusot-target:/ctarget -e CARGO_TARGET_DIR=/ctarget \
  crabka-creusot:v0.12.0 "cd crates/throttle && cargo creusot"
```

(The named target volume keeps verification builds out of the host's MSVC `target/` dir. Expect syntax errors on first run — fix pearlite surface syntax per the 0.12 guide, meanings fixed.)

- [ ] **Step 2: Add loop invariants until the provers discharge everything**

The two loops that need `#[invariant(...)]` annotations (inside the function, attached to the `while` loops):

`offset_index_lookup` — the classic bracket:

```rust
#[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
#[invariant(forall<k: Int> 0 <= k && k < lo@ ==> entries@[k].0@ <= target@)]
#[invariant(forall<k: Int> hi@ <= k && k < entries@.len() ==> entries@[k].0@ > target@)]
```

`recompute_high_watermark` outer loop — `majority_offset` is the best qualifying candidate seen so far:

```rust
#[invariant(i@ <= follower_offsets@.len() + 1)]
#[invariant(majority_offset@ == i64::MIN@
    || (count_ge(log_end@, follower_offsets@, majority_offset@) >= majority@
        && majority_offset@ <= log_end@))]
// every already-visited candidate that qualifies is <= majority_offset
#[invariant(forall<k: Int> 0 <= k && k < i@
    ==> (if k == 0 { log_end@ } else { follower_offsets@[k-1]@ })
        > majority_offset@
        ==> count_ge(log_end@, follower_offsets@, if k == 0 { log_end@ } else { follower_offsets@[k-1]@ }) < majority@)]
```

inner counting loop — `count` tracks the logic count over the visited prefix:

```rust
#[invariant(j@ <= follower_offsets@.len() + 1)]
#[invariant(count@ == /* count over the first j members of {log_end} ∪ follower_offsets >= cand */)]
```

(the inner invariant needs a prefix-counting `#[logic]` helper — add `count_ge_prefix(log_end, s, v, j)` analogous to `count_ge_seq` and prove `count_ge_prefix(.., s.len()+1) == count_ge(..)` with a small lemma function). If a goal resists automation, add `proof_assert!(...)` stepping stones or lemma `#[logic]` functions — never weaken a contract clause. Debug interactively with the Why3 IDE if needed: rerun with `-i` under WSLg, per `cargo creusot --help`.

- [ ] **Step 3: Verify everything proves, then verify stable is still green**

```bash
docker run --rm -v "$PWD:/work" -w /work \
  -v crabka-creusot-target:/ctarget -e CARGO_TARGET_DIR=/ctarget \
  crabka-creusot:v0.12.0 "cd crates/verified && cargo creusot && cd ../throttle && cargo creusot"
cargo test -p crabka-verified -p crabka-throttle
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: the verifier reports every obligation proved (exit 0); stable tests/clippy unaffected (invariant attributes erase).

- [ ] **Step 4: Produce and check in replayable proof sessions**

Using the replay mechanism recorded in Task 1 Step 6 (why3find sessions; the prove/replay subcommand split shown by `cargo creusot --help`): run the session-producing command in both crates, confirm the replay-only form succeeds from a clean checkout state, and check the session files in. Also verify `.gitignore` doesn't swallow them (`git status` must show them).

```bash
git add crates/verified crates/throttle
git commit -m "feat: check in Creusot proof sessions for crabka-verified and crabka-throttle

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: CI — creusot-verify required check + image publish workflow

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `.github/workflows/publish-creusot-image.yml`

- [ ] **Step 1: Bootstrap-push the locally built image to ghcr**

The CI job pulls `ghcr.io/robot-head/crabka-creusot:v0.12.0`; the publish workflow (Step 3) only runs on pushes to main, so seed the registry once from the Task 1 local build:

```bash
docker tag crabka-creusot:v0.12.0 ghcr.io/robot-head/crabka-creusot:v0.12.0
gh auth token | docker login ghcr.io -u robot-head --password-stdin
docker push ghcr.io/robot-head/crabka-creusot:v0.12.0
```

Expected: push succeeds. (If the package is private by default, make it public in the ghcr package settings so the unauthenticated CI pull works, matching the other crabka images.)

- [ ] **Step 2: Wire the `creusot` filter + job + gatekeeper into ci.yml**

In the `changes` job: add `creusot: ${{ steps.filter.outputs.creusot }}` to `outputs`, and to the `filters:` block:

```yaml
            creusot:
              - 'crates/verified/**'
              - 'crates/throttle/**'
              - 'packaging/melange/creusot-toolchain.yaml'
              - 'packaging/apko/creusot-toolchain.yaml'
              - '.creusot-version'
              - '.github/workflows/ci.yml'
```

Add the job (alongside the other integration jobs):

```yaml
  # Replays the checked-in Creusot/why3find proof sessions for the two
  # verified packages inside the pinned toolchain image. Red means a contract
  # no longer proves — a functional regression in a verified kernel, or an
  # edit that needs its proof session refreshed (see docs/verification.md).
  creusot-verify:
    needs: changes
    if: ${{ needs.changes.outputs.creusot == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v7
      - name: Replay proof sessions
        run: |
          PIN="$(cat .creusot-version)"
          docker run --rm -v "$PWD:/work" -w /work \
            -e CARGO_TARGET_DIR=/tmp/creusot-target \
            "ghcr.io/robot-head/crabka-creusot:${PIN}" \
            "cd crates/verified && cargo creusot <REPLAY-ARGS> && cd ../throttle && cargo creusot <REPLAY-ARGS>"
```

Replace `<REPLAY-ARGS>` with the exact replay invocation validated in Task 7 Step 4 — it must fail (nonzero exit) when a session no longer replays, and must not silently re-search. Verify the failure mode locally before committing: flip a contract bound (e.g. `result@ < base_ms@` → `<=`), run the replay command, confirm nonzero exit, revert.

Add `- creusot-verify` to the `gatekeeper-ci` job's `needs:` list. Because gatekeeper treats `skipped` as pass, unrelated PRs sail through — this is the required-check short-circuit the spec asks for.

- [ ] **Step 3: Create `.github/workflows/publish-creusot-image.yml`**

```yaml
name: publish-creusot-image

# Rebuilds and publishes the Creusot toolchain image when the pin or the
# recipes change. The image is a dev/CI tool (single-arch, unattested) — not a
# shipped artifact like the broker images in publish-images.yml.
on:
  push:
    branches: [main]
    paths:
      - '.creusot-version'
      - 'packaging/melange/creusot-toolchain.yaml'
      - 'packaging/apko/creusot-toolchain.yaml'
      - '.github/workflows/publish-creusot-image.yml'
  workflow_dispatch:

permissions:
  contents: read
  packages: write

concurrency:
  group: publish-creusot-image-${{ github.ref }}
  cancel-in-progress: true

jobs:
  build-publish:
    runs-on: ubuntu-latest
    timeout-minutes: 120
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v6
        with:
          go-version: stable
      - name: Install melange + apko
        run: |
          go install chainguard.dev/melange@latest
          go install chainguard.dev/apko@latest
          echo "$HOME/go/bin" >> "$GITHUB_PATH"
      - uses: docker/login-action@v4
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - name: Build the toolchain APK
        run: |
          mkdir -p packages
          melange keygen melange.rsa
          melange build packaging/melange/creusot-toolchain.yaml \
            --source-dir . \
            --signing-key melange.rsa \
            --arch x86_64 \
            --runner docker \
            --out-dir packages/
      - name: Publish the image
        run: |
          PIN="$(cat .creusot-version)"
          apko publish packaging/apko/creusot-toolchain.yaml \
            "ghcr.io/robot-head/crabka-creusot:${PIN}" \
            --arch x86_64 \
            --repository-append "$PWD/packages" \
            --keyring-append "$PWD/melange.rsa.pub"
```

- [ ] **Step 4: Validate workflow syntax and the end-to-end gate**

```bash
# Syntax check both workflows locally (act is available per the kind/act memory):
act pull_request -W .github/workflows/ci.yml --list
act workflow_dispatch -W .github/workflows/publish-creusot-image.yml --list
```

Expected: both parse and list their jobs. Then push the branch and confirm on the PR: `creusot-verify` runs (this branch touches `crates/verified/**`) and goes green; `gatekeeper-ci` includes it.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml .github/workflows/publish-creusot-image.yml
git commit -m "ci: creusot-verify proof-replay gate + toolchain image publish workflow

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Developer docs + wrapper script

**Files:**
- Create: `docs/verification.md`
- Create: `tools/creusot.sh`
- Modify: `packaging/README.md` (add a section pointing at the toolchain image)

- [ ] **Step 1: Write `tools/creusot.sh`**

```bash
#!/usr/bin/env bash
# Run a command inside the pinned Creusot toolchain image with the workspace
# bind-mounted. Verification builds go to a named volume so they never collide
# with the host target dir (MSVC artifacts on Windows).
#
#   ./tools/creusot.sh                                  # interactive shell
#   ./tools/creusot.sh "cd crates/verified && cargo creusot"
set -euo pipefail
PIN="$(cat "$(dirname "$0")/../.creusot-version")"
docker volume create crabka-creusot-target >/dev/null
exec docker run --rm -it \
  -v "$(pwd):/work" -w /work \
  -v crabka-creusot-target:/ctarget -e CARGO_TARGET_DIR=/ctarget \
  "ghcr.io/robot-head/crabka-creusot:${PIN}" "${*:-bash}"
```

- [ ] **Step 2: Write `docs/verification.md`**

Content requirements (state current facts only — no development journey, per the docs house rule):

```markdown
# Formal verification (Creusot)

## What is verified

| Kernel | Crate | Contract (informal) |
|---|---|---|
| `plan_consume` | `crabka-throttle` | grant ≤ requested; grant+new = min(available⊕refill, burst); never exceeds burst; grant maximal |
| `election_jitter_ms` | `crabka-verified` | result < base_ms (0 when base_ms = 0) |
| `log_is_up_to_date` | `crabka-verified` | exactly the KIP-595 up-to-date rule |
| `recompute_high_watermark` | `crabka-verified` | HWM monotonic, ≤ log end; any advance is past epoch_start with a ≥-majority replication witness |
| `offset_index_lookup` | `crabka-verified` | position of the greatest entry ≤ target, else 0 |
| `retain_decision` | `crabka-verified` | the full KIP-534 case space (control markers, dedup, tombstone horizons) |

Host crates call these kernels directly (no duplicated bodies); the stateright
models in `crabka-throttle`, `crabka-raft`, and `crabka-log` drive the same
functions, so model-checked and proven code are one artifact.

## Toolchain

Creusot is pinned in `.creusot-version` and ships as a Docker image
(`ghcr.io/robot-head/crabka-creusot:<pin>`) built by
`packaging/melange/creusot-toolchain.yaml` + `packaging/apko/creusot-toolchain.yaml`
(`tools/build-creusot-image.sh`, published by `publish-creusot-image.yml`).
Contracts erase to no-ops under stable rustc — normal builds, clippy, and
tests never see Creusot.

## Running the verifier

    ./tools/creusot.sh "cd crates/verified && cargo creusot"
    ./tools/creusot.sh "cd crates/throttle && cargo creusot"

Proof sessions are checked in; CI (`creusot-verify` in ci.yml) replays them:
<exact replay command from Task 7>. If you edit a verified function, rerun the
prover, update the session, and commit it — a red creusot-verify means a
contract no longer holds or the session is stale.

## Authoring / debugging proofs

- Contracts: `#[requires]` / `#[ensures]`; loop invariants: `#[invariant]`;
  stepping stones: `proof_assert!`; logic helpers: `#[logic]`.
  Guide: https://guide.creusot.rs
- Interactive debugging needs the Why3 IDE (X11): run under WSLg with
  `cargo creusot -i <goal>` inside the image (`--net=host`, `-e DISPLAY`).

## Bumping the pin

1. Edit `.creusot-version` and the `creusot-std` version in
   `crates/verified/Cargo.toml` + `crates/throttle/Cargo.toml` — they MUST
   stay a matched pair, and `creusot-std` must exist on crates.io at that
   version (git dependencies are unpublishable; release-plz publishes these
   crates).
2. Update `version:` in `packaging/melange/creusot-toolchain.yaml` and the
   `--branch` in its pipeline.
3. Rebuild + publish the image (merge to main triggers
   `publish-creusot-image.yml`), reprove both crates, commit refreshed
   sessions.
```

Fill in the `<exact replay command>` placeholder from Task 7's result before committing — the doc must not ship with a placeholder.

- [ ] **Step 3: Add the packaging/README.md section**

Append a short section: the `creusot-toolchain` recipe builds a dev/CI verifier image (single-arch, root, unattested by design — it never ships to users), pointer to `docs/verification.md`.

- [ ] **Step 4: Verify**

```bash
bash -n tools/creusot.sh
./tools/creusot.sh "cargo creusot --help"
```

Expected: syntax-clean; the wrapper pulls/runs the image and prints cargo-creusot usage.

- [ ] **Step 5: Commit**

```bash
git add docs/verification.md tools/creusot.sh packaging/README.md
git commit -m "docs: Creusot verification guide + toolchain wrapper script

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Plan self-review notes

- **Spec coverage:** six kernels ✓ (Tasks 3, 6), call-through with deleted originals ✓ (Tasks 4–5), throttle in place with one gate ✓ (Tasks 2, 6), melange/apko image + Windows dev flow ✓ (Tasks 1, 9), required CI replay with short-circuit ✓ (Task 8, via gatekeeper semantics), checked-in sessions ✓ (Task 7), `docs/verification.md` ✓ (Task 9), pin single-sourced ✓ (`.creusot-version`), stateright models driving proven functions ✓ (verified in Tasks 4–5 test steps).
- **Known deferred validations** (deliberate, recorded in-task, not placeholders): exact `creusot-std` prelude path and replay subcommand (Task 1 Step 6 records them; Tasks 6–8 consume them), pearlite surface syntax (meanings fixed by spec).
- **Type consistency:** kernel signatures used in Tasks 4–6 match Task 3's definitions (`u64`/`u32`/`i64`/`usize`/`&[(u32, u32)]`); `NodeId = u64`, `LeaderEpoch = u32` verified against source.
