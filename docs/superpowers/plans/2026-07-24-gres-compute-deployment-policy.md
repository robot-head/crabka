# Gres Compute Deployment Policy

**Goal:** Make Kubernetes-managed Gres compute workloads honor tenant checkpoint policy and expose the remaining workload-specific image and readiness settings through the correct CRDs.

**Architecture:** Tenant checkpoint thresholds remain registry-backed `Gres.defaults` / `GresTenant.overrides` policy and are loaded by compute at startup. Deployment image is tenant-owned with operator CLI/environment fallback. Readiness cadence is fleet-owned under `Gres.spec.compute`. Validation is fail-fast and uses `refined_type`; no silent clamp or duplicate source is allowed.

**Constraints:** Keep protocol constants, ports, TLS paths, security IDs, and the final-checkpoint capability invariant fixed. Add no dependency. Run all Cargo commands with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

## Task 1: Restore checkpoint-threshold precedence

**Files:**

- Modify: `crates/operator/src/controller/gres_tenant.rs`

- [x] Add a failing test proving object-store args do not inject a checkpoint threshold.
- [x] Remove the unconditional `--checkpoint-frames 1`.
- [x] Preserve object-store/credential forwarding and final-checkpoint support: when registry thresholds are absent, `crabka-gres` uses its validated standalone defaults.
- [x] Prove tenant-record checkpoint values remain the effective source and CLI still wins only when explicitly supplied outside the operator path.
- [x] Run focused and full operator tests, strict all-target/all-feature Clippy, nightly formatting, generated CRD equality, and `git diff --check`.
- [x] Commit only implementation files.

## Task 2: Expose compute image and readiness cadence

**Files:**

- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/crd/gres_tenant.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify generated Gres and GresTenant CRDs

- [x] Add optional `GresTenant.spec.image` with schema minimum length and `NonEmptyString` runtime validation.
- [x] Implement image precedence: tenant CRD, `--default-gres-image` / `DEFAULT_GRES_IMAGE`, compiled default.
- [x] Add optional positive `Gres.spec.compute.readinessProbePeriodSeconds`; validate with `GreaterI32<0>` and default to 5.
- [x] Route the effective readiness period to every range compute deployment.
- [x] Reject invalid image/period before applying any compute deployment; never trim, clamp, or silently fall back.
- [x] Add schema, precedence, deployment rendering, invalid-value, and config-hash/update tests.
- [x] Run focused/full operator tests, strict gates, and exact all-nine CRD generation.
- [x] Commit only implementation and generated CRD files.

## Task 3: Independent review and audit closure

- [x] Review each implementation task independently.
- [x] No findings required remediation; the independent review remained clean.
- [x] Confirm scanner candidates for checkpoint threshold, compute image, and readiness cadence are fully classified.
- [x] Record exact verification evidence and hand off to deeper compute checkpoint/lifecycle pacing.

The fresh runtime-value scan reported 5,900 repository candidates. The 64 candidates in the scoped Gres and Gres operator files split into 47 production and 17 test values; the only task-direct production hits were the four deeper checkpoint defaults in `crates/gres/src/lib.rs:35-38` and the intended compiled compute image fallback in `crates/operator/src/controller/gres_tenant.rs:61`. Focused compute tests passed 23/23, the checkpoint-argument regression passed 1/1, and the full operator suite passed 925/925. Strict all-target/all-feature Clippy, nightly formatting, all nine freshly generated CRDs, and `git diff --check` were exact and clean.

The next compute-owned slice starts at `crates/gres/src/lib.rs:35-39`, `crates/gres/src/lib.rs:596-614`, `crates/gres/src/lib.rs:1476`, and `crates/gres/src/lib.rs:2378` for checkpoint defaults, delete timeout, and idle-monitor cadence. Shared checkpoint-service polling remains at `crates/gres-substrate/src/checkpoint/service.rs:63-87`; operator lifecycle polling remains at `crates/operator/src/controller/gres_tenant.rs:67`.
