# Formal verification (Creusot)

Crabka keeps formally verified pure kernels in the crates that own their public
interfaces. Host crates call those kernels directly; there are no duplicated
bodies for verified and runtime code paths.

## What is verified

| Kernel | Crate | Verified behavior |
| --- | --- | --- |
| `plan_consume` | `crabka-throttle` | Token-bucket grant/refill arithmetic stays capped by burst and never grants more than requested or available. |
| `election_jitter_ms` | `crabka-verified` | Election jitter remains inside the configured timeout range, including the zero-timeout case. |
| `log_is_up_to_date` | `crabka-verified` | KRaft vote freshness comparison follows the leader-epoch, then offset, ordering. |
| `recompute_high_watermark` | `crabka-verified` | High watermark recomputation advances only when a majority has replicated an offset in the current epoch. |
| `offset_index_lookup` | `crabka-verified` | Offset-index lookup returns the greatest indexed position not greater than the target, or zero when none exists. |
| `retain_decision` | `crabka-verified` | Log compaction retention decisions preserve KIP-534 transaction/marker semantics and newest-key retention. |

The stateright models in `crabka-throttle`, `crabka-raft`, and `crabka-log`
drive the same functions through the runtime APIs, so model checking and
deductive verification cover the same bodies.

## Toolchain

- The verifier pin lives in `.creusot-version`.
- The Docker image is `ghcr.io/robot-head/crabka-creusot:<pin>`.
- The image is built by `packaging/melange/creusot-toolchain.yaml` and
  `packaging/apko/creusot-toolchain.yaml`.
- Local image builds use `tools/build-creusot-image.sh`.
- CI publishing uses `.github/workflows/publish-creusot-image.yml`.
- Creusot contracts erase under stable `rustc`; normal builds do not run the
  verifier.

## Running the verifier

Run verification from the repository root through the Docker wrapper. Use
package-qualified commands so `cfg(creusot)` does not compile unrelated
workspace dependents.

```bash
./tools/creusot.sh "cargo creusot -p crabka-verified"
./tools/creusot.sh "cargo creusot -p crabka-throttle"
```

## CI replay

Replay the committed proof sessions with the same root/package shape CI uses:

```bash
./tools/creusot.sh "cargo creusot --replay -p crabka-verified && cargo creusot --replay -p crabka-throttle"
```

The proof artifacts live in top-level `verif/`, with top-level
`why3find.json`.

## Authoring and debugging proofs

Use Creusot contracts and proof helpers such as `#[requires]`, `#[ensures]`,
`#[invariant]`, `proof_assert!`, and `#[logic]`. The upstream guide is
<https://guide.creusot.rs>.

For interactive Why3 debugging, run `cargo creusot -i <goal>` inside the image
with X11 configured. On Windows, WSLg is the expected X11 path for the Why3 IDE.

## Bumping the pin

1. Edit `.creusot-version`.
2. Update `creusot-std` in `crates/verified/Cargo.toml` and
   `crates/throttle/Cargo.toml`.
3. Keep the tool pin and `creusot-std` matched.
4. Confirm the selected `creusot-std` exists on crates.io; do not use a git
   dependency.
5. Update the melange `version:` and Creusot clone `--branch` in
   `packaging/melange/creusot-toolchain.yaml`.
6. Rebuild and publish the image.
7. Reprove both crates.
8. Commit the refreshed proof sessions.
