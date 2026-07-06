# Crate Publication Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the crates.io publication surface so only Crabka crates that are useful on their own remain publishable.

**Architecture:** Keep internal workspace crate boundaries where they encode useful build, test, wasm, or service-sharing boundaries, but make publication explicit. A repository-local allowlist check is the source of truth for which packages may omit `publish = false`.

**Tech Stack:** Cargo workspace manifests, release-plz, Bash, Python 3, `cargo metadata`, GitHub Actions.

## Global Constraints

- Crabka is greenfield and undeployed; do not add backwards-compatibility shims.
- Preserve Kafka wire-protocol byte exactness and behavior JVM Kafka tools rely on.
- Do not move Rust code in this first pass; only publication policy, release configuration, docs, and CI enforcement change.
- Public allowlist: `crabka-client-admin`, `crabka-client-consumer`, `crabka-client-core`, `crabka-client-producer`, `crabka-client-streams`, `crabka-compression`, `crabka-connect`, `crabka-connect-derive`, `crabka-log`, `crabka-protocol`, `crabka-schema-serde`, `crabka-security`.
- Every workspace package outside the public allowlist must have `publish = false` in its package manifest.
- `release-plz.toml` must explicitly prevent private packages from being released or published.
- CI must fail when `cargo metadata` finds a publishable workspace package outside the allowlist.

---

### Task 1: Enforce Crate Publication Allowlist

**Files:**
- Modify: `crates/*/Cargo.toml`
- Modify: `release-plz.toml`
- Create: `tools/check-publish-allowlist.sh`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Cargo package metadata where `publish = false` appears as `publish: []` and omitted `publish` appears as `publish: null`.
- Produces: `tools/check-publish-allowlist.sh`, an executable verifier that exits non-zero for any publishable package not in the public allowlist.

- [ ] **Step 1: Mark private manifests**

Add `publish = false` directly under `name = ...` in every private package that does not already have it.

- [ ] **Step 2: Update release-plz**

Keep workspace publishing enabled, add explicit `publish = true` package entries for the public allowlist, and add `publish = false` / `release = false` entries for every private package.

- [ ] **Step 3: Add the allowlist verifier**

Create `tools/check-publish-allowlist.sh`. It must run `cargo metadata --no-deps --format-version 1`, compare publishable package names to the allowlist, print unexpected publishable packages, and exit 1 on drift.

- [ ] **Step 4: Wire CI**

Run the verifier in the Linux leg of the Rust CI job after the Rust toolchain is installed, and include the verifier and `release-plz.toml` in the Rust paths filter.

- [ ] **Step 5: Verify**

Run `tools/check-publish-allowlist.sh` and `cargo metadata --no-deps --format-version 1` locally. Expected: the verifier exits 0 and metadata still resolves.
