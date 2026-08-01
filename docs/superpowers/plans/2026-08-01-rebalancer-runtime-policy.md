# Rebalancer Runtime Policy Implementation Plan

**Goal:** Configure the standalone rebalancer's remaining production runtime
and state-topic policy while preserving defaults.

**Architecture:** Resolve one validated `RebalancerRuntimePolicy` from the
existing CLI/environment boundary, pass it through current runtime owners, and
render the same environment variables from the Helm chart.

### Task 1: Shared policy and boundary

- [x] Add default and invariant tests.
- [x] Add UOM fields and a `refined_type`-validated positive count newtype.
- [x] Add optional CLI/environment overrides and resolve one policy before I/O.
- [x] Run focused tests and strict Clippy; commit.

### Task 2: Runtime owners

- [x] Thread policy through recovery, shutdown, scraper, cancellation, detector, and state-topic owners.
- [x] Preserve default public constructors.
- [x] Remove superseded production literals.
- [x] Run rebalancer tests and strict Clippy; commit.

### Task 3: Helm deployment surface

- [x] Add unit-bearing chart values and matching environment variables.
- [x] Extend chart rendering tests.
- [ ] Run chart tests and diff hygiene; commit.

### Task 4: Closure

- [ ] Run rebalancer all-target tests.
- [ ] Run workspace all-target check and strict Clippy.
- [ ] Run nightly formatting and diff hygiene.
- [ ] Update `docs/configuration-audit.md` and commit closure docs.
- [ ] Leave the repository-wide goal active.
