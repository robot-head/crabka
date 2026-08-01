# Telemetry Profiling Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make profiling durations, sample frequency, and native-frame filtering
operator-configurable while preserving existing behavior.

**Architecture:** Add a shared `ProfilingConfig` and policy-aware profiling
router/admin entry points. Existing entry points delegate with defaults. Owning
binaries flatten and pass the shared config.

**Tech Stack:** Rust, axum, clap, crabka-units, refined_type, pprof, jemalloc.

### Task 1: Shared validated profiling policy

- [x] Add failing default, override, invalid-value, and duration-bound tests.
- [x] Add UOM duration/frequency fields and refined whole-Hz validation.
- [x] Add policy-aware router and server functions; keep default wrappers.
- [x] Prove configured CPU and heap request default/cap normalization.
- [x] Run telemetry tests and strict crate Clippy; commit the shared policy.

### Task 2: Direct admin-server owners

- [x] Flatten `ProfilingConfig` into metrics, traces, profiles, schema-registry, metrics-service, observability, and observability-demo-app CLI surfaces.
- [x] Pass the parsed policy to the policy-aware admin server functions.
- [x] Add focused CLI/environment default and override tests.
- [x] Run affected crate tests and strict Clippy; commit direct-owner wiring.

### Task 3: Broker owner

- [x] Flatten `ProfilingConfig` into the broker CLI/config boundary.
- [x] Pass policy through the metrics-server router construction.
- [x] Add focused broker CLI/environment and router tests.
- [x] Run broker tests and strict Clippy; commit broker wiring.

### Task 4: Closure

- [x] Run `cargo test -p crabka-telemetry --all-targets --locked` and affected owner tests.
- [x] Run workspace all-target check and strict warnings-as-errors Clippy.
- [x] Run nightly formatting and `git diff --check`.
- [x] Update `docs/configuration-audit.md` with the implemented surface and evidence.
- [x] Commit closure documents; leave the broader repository goal active.
