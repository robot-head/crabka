# Rebalancer Runtime Policy Implementation Plan

**Goal:** Configure the standalone rebalancer's remaining production runtime
and state-topic policy while preserving defaults.

**Architecture:** Resolve one validated `RebalancerRuntimePolicy` from the
existing CLI/environment boundary, pass it through current runtime owners, and
render the same environment variables from the Helm chart.

### Task 1: Shared policy and boundary

- [ ] Add default and invariant tests.
- [ ] Add UOM fields and a `refined_type`-validated positive count newtype.
- [ ] Add optional CLI/environment overrides and resolve one policy before I/O.
- [ ] Run focused tests and strict Clippy; commit.

### Task 2: Runtime owners

- [ ] Thread policy through recovery, shutdown, scraper, cancellation, detector, and state-topic owners.
- [ ] Preserve default public constructors.
- [ ] Remove superseded production literals.
- [ ] Run rebalancer tests and strict Clippy; commit.

### Task 3: Helm deployment surface

- [ ] Add unit-bearing chart values and matching environment variables.
- [ ] Extend chart rendering tests.
- [ ] Run chart tests and diff hygiene; commit.

### Task 4: Closure

- [ ] Run rebalancer all-target tests.
- [ ] Run workspace all-target check and strict Clippy.
- [ ] Run nightly formatting and diff hygiene.
- [ ] Update `docs/configuration-audit.md` and commit closure docs.
- [ ] Leave the repository-wide goal active.
