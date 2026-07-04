# Creusot formal verification — design

**Date:** 2026-07-04
**Status:** Approved

## Goal

Prove **functional correctness** (full `#[requires]`/`#[ensures]` contracts, discharged
by SMT via [Creusot](https://github.com/creusot-rs/creusot)) for six pure kernels whose
bugs would silently lose committed data, corrupt reads, or break rate limiting.
Proof replay is a **required CI check**: a contract that no longer proves blocks merge.

Creusot verifies synchronous, mostly-safe Rust only (no async, no trait objects, no
atomics, limited std). Crabka's broker paths are heavily async, so verification targets
extracted pure kernels — the deductive counterpart to the existing stateright
model-checking program: stateright explores concurrent interleavings; Creusot proves
sequential functional contracts over the *entire* input space.

## Scope

Three kernel sets (wire-format kernels — records-legacy offset rewriting, batch-header
patching — are explicitly out of scope for this iteration):

1. **Consensus core** (kraft-core): `recompute_high_watermark`, `log_is_up_to_date`,
   `election_jitter_ms`
2. **Log data-path** (log): `OffsetIndex::lookup`, `compact::retain_decision`
3. **Throttle** (throttle): `plan_consume`

## Architecture

### New crate `crates/verified` (package `crabka-verified`)

Holds the five kraft-core/log kernels as **pure free functions over primitive types**
(`u64`/`i64`/`u32`/slices) plus the small plain data types `retain_decision` needs
(`RecordMeta`, `BatchMeta`, `TxnDataState`, `RetainDecision`, moved from
`crabka-log`). No dependency on `kraft-core` or `log` types, so the crate is trivially
translatable by Creusot. Sole dependency: `creusot-std` (the renamed successor of the
deprecated `creusot-contracts`; its macros erase to no-ops under normal rustc, so
stable builds/clippy/tests are unaffected).

Host crates **call through** — original bodies are deleted, never duplicated:

- `kraft-core`: `recompute_high_watermark` keeps its method shell (destructures
  `Role::Leader`, collects follower offsets) and delegates the math;
  `log_is_up_to_date`'s call site extracts `(my_epoch, my_end)` from `&dyn LogView`
  and passes values; `election_jitter_ms` moves wholesale and kraft-core re-exports it
  (the async engine also calls it).
- `log`: `OffsetIndex::lookup` delegates over `&self.entries`; `compact` imports the
  moved types and delegates `retain_decision`.

### `crabka-throttle` verified in place

The crate has no async/IO, and `plan_consume` is already a pure free function. The
`TokenBucket` runtime shell (struct + impl + `clock_nanos`, which use `AtomicU64` and
`Arc<dyn NanoClock>` — untranslatable) moves into a `mod runtime` gated
`#[cfg(not(creusot))]`, re-exported so the public API is unchanged. Creusot sees only
the pure arithmetic. The crate adds the `creusot-std` dependency.

### Verification targets

`cargo creusot` runs on exactly two packages: `crabka-verified` and `crabka-throttle`.
No other crate ever sees the Creusot toolchain.

## Contracts

### `plan_consume(available, refill, burst, requested) -> (grant, new_available)`

Total; no preconditions. Ensures (⊕ = saturating add, `capped = min(available ⊕ refill, burst)`):

- `grant ≤ requested`
- `grant + new_available == capped`
- `new_available ≤ burst`
- `grant == min(requested, capped)` (maximality — never under-grants while tokens remain)

### `election_jitter_ms(me, epoch, base_ms) -> u64`

- `base_ms > 0 ⟹ result < base_ms`
- `base_ms == 0 ⟹ result == 0`
- Proven free of division-by-zero and overflow (wrapping ops are explicit).

### `log_is_up_to_date(my_epoch, my_end, cand_epoch, cand_offset) -> bool`

Full functional spec — the ensures clause *is* the KIP-595 rule:
`result == (cand_epoch > my_epoch ∨ (cand_epoch == my_epoch ∧ cand_offset ≥ my_end))`.
Stated once in logic and once in code so a transposed operator cannot slip through
either alone.

### `recompute_high_watermark(log_end, follower_offsets, majority, epoch_start_offset, current_hwm) -> i64`

Requires (upheld and documented at the kraft-core call site):

- `1 ≤ majority ≤ follower_offsets.len() + 1`
- `current_hwm ≤ log_end`
- every `o ∈ follower_offsets`: `o ≤ log_end`

Ensures:

- **(a)** `result ≥ current_hwm` — the HWM never regresses
- **(b)** `result ≤ log_end`
- **(c)** `result > current_hwm ⟹ result > epoch_start_offset ∧
  |{ m ∈ {log_end} ∪ follower_offsets : m ≥ result }| ≥ majority` — the Raft-Fig.8 /
  KIP-595 leader-completeness gate with an explicit majority-replication witness.

### `offset_index_lookup(entries: &[(u32, u32)], target: u32) -> u32`

Requires: `entries` strictly sorted by relative offset (true by construction of
`OffsetIndex`; documented at the call site). Ensures: result is the position field of
the **greatest** entry with `rel ≤ target`, or `0` when no such entry exists.

### `retain_decision(rec, batch, is_newest_for_key, txn, now_ms, delete_retention_ms) -> RetainDecision`

Ensures clauses cover the full KIP-534 case space:

- control batch ∧ (data survives ∨ not transactional) ⟹ `Keep`
- control batch ∧ data fully gone: `Delete` iff `now_ms ≥ horizon`; `Keep` if horizon
  set but not reached; `SetHorizon(now_ms + delete_retention_ms)` iff no horizon set
- data record without key ⟹ `Delete`
- data record not newest for its key ⟹ `Delete`
- newest for key with value ⟹ `Keep`
- newest-for-key tombstone: same horizon algebra as orphaned control batches

## Toolchain

- Creusot at a **pinned release tag** (the latest release at implementation time;
  v0.12.0 as of this writing): the Creusot compiler, `cargo-creusot`, Why3, why3find,
  the SMT provers (Z3/CVC5/Alt-Ergo), and the specific nightly Rust that Creusot pins
  for verification builds. The workspace itself stays on stable 1.96.
- The pinned Creusot version lives in one place (version file read by the image
  recipe, CI, and docs).
- `docs/verification.md`: building/pulling the toolchain image, running
  `cargo creusot` on the two packages, proof debugging via the Why3 IDE (`-i`), how
  to update proof sessions.
- why3find **proof sessions are checked in** so CI replays rather than re-searches.

### Creusot toolchain image (melange/apko)

The Creusot toolchain is Linux-only (opam/Why3), so it ships as a Docker image built
with the house melange/apko pattern (`packaging/melange/`, `packaging/apko/`,
mirroring `tools/build-image.sh`) — the single toolchain artifact used both for local
development on Windows and by CI:

- **`packaging/melange/creusot-toolchain.yaml`**: builds a `creusot-toolchain` APK
  from Wolfi — pipeline installs OCaml/opam, builds Creusot at the pinned tag via its
  `./INSTALL` flow (Why3, why3find, Alt-Ergo via opam), stages Z3/CVC5 (Wolfi packages
  where available, upstream release binaries otherwise), and installs rustup with
  Creusot's pinned nightly preloaded.
- **`packaging/apko/creusot-toolchain.yaml`**: assembles the image (tagged with the
  Creusot pin, e.g. `crabka-creusot:v0.12.0`) with `git`, `build-base`, and a
  non-root user matching the bench-driver image conventions.
- **`tools/build-creusot-image.sh`**: mirrors `tools/build-image.sh` (melange build →
  apko build, `MELANGE_RUNNER=docker` on Windows).
- Published to ghcr via the existing image-publish workflow pattern, rebuilt only when
  the pin or the recipes change.

**Windows dev flow**: `docker run` the image with the workspace bind-mounted, e.g.
`docker run --rm -v ${PWD}:/work -w /work crabka-creusot:v0.12.0 cargo creusot ...`
(a thin wrapper script/justfile recipe provides this). `CARGO_TARGET_DIR` inside the
container points at a named Docker volume so verification builds never collide with
the host's MSVC target dir; proof sessions are written back through the bind mount so
they can be committed from Windows. The Why3 IDE flow (`-i`) needs X forwarding and
stays a documented Linux/WSLg option; day-to-day proving and replay are headless.

## CI

New Linux job `creusot-verify`, a **required check**, running **in the toolchain
image** (`container: ghcr.io/.../crabka-creusot:<pin>`) so CI and local dev share one
environment and there is no opam install or cache to manage in the job:

1. Replay checked-in proof sessions for `crabka-verified` and `crabka-throttle`
   (`cargo creusot` replay mode); red if any contract no longer proves.
2. The job **always runs** but short-circuits to success when the PR touches neither
   the two crates, the proof sessions, the version pin, nor the toolchain image
   recipes — a required check with workflow-level path filters would wedge as
   "expected".

## Testing and drift protection

- Normal workspace CI is untouched: contracts erase under stable rustc, so existing
  builds, clippy, unit tests, and stateright models run as today.
- The stateright models (`throttle/tests/bucket_model.rs`, `raft/tests/kraft_model.rs`)
  now drive the **same functions** the proofs cover — model-checked code and proven
  code are one artifact, closing the fidelity gap the adversarial-faithfulness reviews
  exist to catch.
- No duplicated bodies anywhere: hosts call through, originals deleted (greenfield —
  no compatibility shims).
- Contract rot is impossible by construction: the required check replays proofs on
  every touch of the verified surface.

## Sequencing

Slice 1 is deliberately `plan_consume` end-to-end — toolchain image built and
published, contract, proof authored via the image on Windows, checked-in session,
CI job green in the image — before any extraction work, to de-risk the rest.
Then the `crabka-verified` crate + kernel extractions + their proofs.

## Known risks

1. **`creusot-std` erasure on stable / edition 2024** must be confirmed in slice 1
   against Rust 1.96 (`creusot-std` 0.12.0 on crates.io is the matched pair for tool
   tag v0.12.0). If it does not build on stable, fallback: make it an optional
   dependency behind a `creusot-proofs` cargo feature enabled only in verification
   builds, with contract attributes applied via `#[cfg_attr(creusot, ...)]` — a git
   dependency is NOT an option because release-plz publishes these crates to crates.io.
2. **std-modeling gaps**: if Creusot's std spec doesn't cover `sort_unstable_by` /
   `binary_search_by_key`, the kernel bodies are rewritten as explicit loops with
   `#[invariant]`s (standard Creusot pattern, behavior-identical; contracts unchanged).
3. **Proof difficulty**: `recompute_high_watermark`'s majority-witness postcondition is
   the hardest obligation. If SMT won't discharge it automatically, add
   `proof_assert!`/lemma functions rather than weakening the contract.
4. **Toolchain image build**: Wolfi may not package OCaml/opam (or Z3/CVC5) at the
   versions Creusot needs. Fallbacks, in order: build opam from source inside the
   melange pipeline; fetch upstream prover release binaries; as a last resort the
   melange step delegates to Creusot's `./INSTALL` in a network-enabled pipeline
   (the same way the crabka recipe already runs `cargo build` with network). The
   image is a dev/CI tool, not a shipped artifact, so size and hermeticity standards
   are relaxed relative to the broker images.
