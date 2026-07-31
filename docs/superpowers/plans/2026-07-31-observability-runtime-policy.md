# Observability Runtime Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expose the remaining observability deployment policy through the
existing CLI/environment configuration without changing defaults.

**Architecture:** Extend `ServiceConfig`; carry typed values directly into the
existing role-specific paths. Reuse `Time`, `NonZeroUsize`, current parsers,
and current builders/functions. Add no policy container or dependency.

**Tech Stack:** Rust, clap, crabka-units, refined_type where validation needs a
newtype, tokio tests.

### Task 1: Distributor policy

- [x] Add failing default, override, environment, zero-rejection, and related-bound tests.
- [x] Add ingest age, future grace, quota burst, startup deadline/attempt timeout, and initial/maximum startup backoff fields to `ServiceConfig`.
- [x] Route the typed values through ingest validation, quota limiting, and dependency startup retry.
- [x] Run focused observability library and environment tests; commit the distributor slice.

### Task 2: Compactor policy

- [ ] Add failing tests for WAL poll timeout, accumulation window/poll timeout, maximum records, idle interval, and initial/maximum object-store retry backoff.
- [ ] Add the typed CLI/environment fields and cross-field validation.
- [ ] Replace the existing literals in all compactor entry points and retry paths.
- [ ] Run focused compactor and CLI/environment tests; commit the compactor slice.

### Task 3: Querier policy

- [ ] Add failing tests for frontier refresh, both index-cache TTLs, shard and cold-block fetch concurrency, hot-tail bucket/cadence, and dependency reconnect interval.
- [ ] Add the typed CLI/environment fields.
- [ ] Route each value through the existing querier construction and background loops.
- [ ] Run focused querier, HTTP, and CLI/environment tests; commit the querier slice.

### Task 4: Closure

- [ ] Run `cargo test -p crabka-observability --all-targets --locked`.
- [ ] Run workspace all-target check and strict warnings-as-errors Clippy.
- [ ] Run nightly formatting and `git diff --check`.
- [ ] Update `docs/configuration-audit.md` with the implemented surface and evidence.
- [ ] Commit the closure documents; leave the broader repository goal active.
