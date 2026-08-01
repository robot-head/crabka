# Gres Ranges Runtime Policy Implementation Plan

**Goal:** Configure deployment-owned Gres range limits and timing through the
existing CLI/environment and compute CRD paths while preserving defaults.

**Architecture:** Add one validated `RangeRuntimePolicy` to
`crabka-gres-ranges`; resolve it in `ServeArgs`/`SubstrateRuntimeConfig`; pass it
through existing runtime owners; render identical values from
`GresComputeSpec`.

### Task 1: Shared validated policy

- [x] Add failing default and invariant tests.
- [x] Add UOM fields and refined positive count/stride newtypes.
- [x] Keep fixed/derived protocol values out of configuration.
- [x] Run focused tests and strict Clippy; commit.

### Task 2: Gres CLI and runtime ownership

- [x] Add optional `CRABKA_GRES_RANGE_*`-backed `ServeArgs` fields.
- [x] Resolve one policy in `SubstrateRuntimeConfig` and reject invalid combinations.
- [x] Thread the policy through transport, forwarding, barrier, inspection, release, and timestamp owners.
- [x] Preserve default public constructors.
- [x] Run Gres and ranges tests plus strict Clippy; commit.

### Task 3: Compute CRD

- [x] Add matching optional unit-bearing `GresComputeSpec` fields.
- [x] Validate with the shared policy and render existing CLI arguments.
- [x] Regenerate and verify the CRD schema.
- [x] Run operator tests and strict Clippy; commit.

### Task 4: Closure

- [x] Run `gres-ranges`, `gres`, and operator all-target tests.
- [x] Run workspace all-target check and strict Clippy.
- [x] Run nightly formatting and diff hygiene.
- [x] Update `docs/configuration-audit.md` and commit closure docs.
- [x] Leave the repository-wide goal active.
