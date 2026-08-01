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

- [ ] Add optional `CRABKA_GRES_RANGE_*`-backed `ServeArgs` fields.
- [ ] Resolve one policy in `SubstrateRuntimeConfig` and reject invalid combinations.
- [ ] Thread the policy through transport, forwarding, barrier, inspection, release, and timestamp owners.
- [ ] Preserve default public constructors.
- [ ] Run Gres and ranges tests plus strict Clippy; commit.

### Task 3: Compute CRD

- [ ] Add matching optional unit-bearing `GresComputeSpec` fields.
- [ ] Validate with the shared policy and render existing CLI arguments.
- [ ] Regenerate and verify the CRD schema.
- [ ] Run operator tests and strict Clippy; commit.

### Task 4: Closure

- [ ] Run `gres-ranges`, `gres`, and operator all-target tests.
- [ ] Run workspace all-target check and strict Clippy.
- [ ] Run nightly formatting and diff hygiene.
- [ ] Update `docs/configuration-audit.md` and commit closure docs.
- [ ] Leave the repository-wide goal active.
