# Telemetry Profiling Policy Design

## Goal

Expose the in-process CPU and heap profiling deployment policy without changing
route behavior or defaults.

## Configuration

Add one shared, defaultable `ProfilingConfig` in `crabka-telemetry`, usable as
flattened clap arguments by each owning binary. It contains:

- CPU default duration (`30s`) and maximum duration (`60s`);
- CPU sampling frequency (`99Hz`);
- heap activation default duration (`5s`) and maximum duration (`30s`); and
- native-frame blocklist (`libc,libgcc,pthread,vdso`).

Durations remain UOM `Time`; sampling remains UOM `Frequency`. Positive,
finite, whole-Hz sampling validation uses `refined_type`, and related default
durations must not exceed their maximums. Request `seconds` remains the
compatible public query shape and is bounded by the configured policy.

Use `CRABKA_PROFILING_*` environment variables with matching
`--profiling-*` arguments. No CRD owns the process-local profiling admin
server.

## Routing and compatibility

Add policy-aware router and admin-server entry points. Keep current public
entry points as default-compatible wrappers so library callers do not break.
Thread policy from the owning binaries; do not read profiling policy from the
environment inside handlers.

Keep gzip encoding, media types, route names, allocation hints, Unix feature
gates, and heap activation/deactivation behavior fixed.

## Verification

Tests cover defaults, overrides, environment parsing, zero/negative/non-whole
frequency rejection, invalid duration bounds, and configured request
default/cap behavior. Close with all telemetry tests, affected binary tests,
workspace check, strict Clippy, nightly formatting, and diff hygiene.
