# Pprof Debuginfod Resource Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make debuginfod artifact and timeout policy configurable through validated UOM values in every Profiles role.

**Architecture:** `crabka-pprof` owns one validated `DebuginfodConfig`; `DebuginfodResolver::new` remains default-backed and a config-aware constructor supplies deployment values. `crabka-profiles` preserves its existing public helpers while adding explicit config paths used by the standalone binary.

**Tech Stack:** Rust, `crabka-units`, `refined_type`, Reqwest blocking client, Clap environment arguments.

## Global Constraints

- Preserve defaults: artifact cap `512MiB`, connect timeout `5s`, request timeout `10s`.
- Require positive whole bytes, positive finite timeouts, and `connect_timeout <= request_timeout`.
- Keep redirect prohibition, URL construction, build-ID validation, parser panic guards, and capped streaming fixed.
- Preserve existing public constructors as default-backed wrappers.
- Keep an empty URL list as the no-egress security default.
- Add no CRD or Helm field because neither owns the standalone Profiles service.
- Do not modify or stage the four protected untracked plans dated 2026-07-28.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not run `cargo clean` until the entire repository goal is complete.

---

### Task 1: Add validated pprof debuginfod configuration

**Files:**
- Modify: `crates/pprof/Cargo.toml`
- Modify: `crates/pprof/src/symbolizer.rs`
- Modify: `Cargo.lock`

**Interfaces:**
- Produces: `DEFAULT_DEBUGINFOD_MAX_ARTIFACT_SIZE: ByteSize`
- Produces: `DEFAULT_DEBUGINFOD_CONNECT_TIMEOUT: Time`
- Produces: `DEFAULT_DEBUGINFOD_REQUEST_TIMEOUT: Time`
- Produces: `DebuginfodConfig::new(ByteSize, Time, Time) -> Result<Self, String>`
- Produces: `DebuginfodResolver::with_config(Vec<String>, DebuginfodConfig) -> Result<Self, String>`

- [x] **Step 1: Write failing configuration tests**

Add tests in `symbolizer.rs`:

```rust
#[test]
fn debuginfod_config_preserves_defaults_and_custom_values() {
    let defaults = DebuginfodConfig::default();
    assert2::assert!(defaults.max_artifact_size() == mebibytes(512));
    assert2::assert!(defaults.connect_timeout() == secs(5));
    assert2::assert!(defaults.request_timeout() == secs(10));

    let custom = DebuginfodConfig::new(mebibytes(64), millis(250), secs(3)).unwrap();
    assert2::assert!(custom.max_artifact_size() == mebibytes(64));
    assert2::assert!(custom.connect_timeout() == millis(250));
    assert2::assert!(custom.request_timeout() == secs(3));
}

#[test]
fn debuginfod_config_rejects_invalid_values() {
    for result in [
        DebuginfodConfig::new(ByteSize::ZERO, secs(1), secs(2)),
        DebuginfodConfig::new(ByteSize::from_bytes_f64(0.5), secs(1), secs(2)),
        DebuginfodConfig::new(mebibytes(1), Time::ZERO, secs(2)),
        DebuginfodConfig::new(
            mebibytes(1),
            Time::from_secs_f64(f64::INFINITY),
            secs(2),
        ),
        DebuginfodConfig::new(mebibytes(1), secs(3), secs(2)),
    ] {
        assert2::assert!(result.is_err());
    }
}
```

Change the existing downloaded-artifact test to call the missing
`DebuginfodResolver::with_config`.

- [x] **Step 2: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-pprof debuginfod_config --locked
```

Expected: compilation fails because `DebuginfodConfig` and `with_config` do
not exist.

- [x] **Step 3: Implement the minimal configuration**

Add:

```toml
refined_type = { workspace = true }
```

Define the three public defaults and:

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DebuginfodConfig {
    max_artifact_size: ByteSize,
    connect_timeout: Time,
    request_timeout: Time,
}
```

Validate whole bytes by checking finiteness, positivity, `fract() == 0.0`, and
the repository's exact `f64` integer ceiling, then pass `bytes_u64()` through
`refined_type::rule::GreaterU64::<0>`. Validate each timeout with
`std::time::Duration::try_from_secs_f64`, reject zero, and enforce
`connect_timeout <= request_timeout`. Add getters and `Default`.

- [x] **Step 4: Apply the configuration to the resolver**

Keep:

```rust
pub fn new(base_urls: Vec<String>) -> Result<Self, String> {
    Self::with_config(base_urls, DebuginfodConfig::default())
}
```

Add `with_config`, pass its two timeouts to Reqwest, and store
`config.max_artifact_size()` as the existing cap. Replace the private
`with_max_debuginfo` test path with a custom `DebuginfodConfig`.

- [x] **Step 5: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-pprof debuginfod_config --offline
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-pprof debuginfod_resolver_fetches_and_caches --locked
```

Expected: configuration and explicit-cap tests pass.

### Task 2: Propagate configuration through all Profiles roles

**Files:**
- Modify: `crates/profiles/src/cold_store.rs`
- Modify: `crates/profiles/src/symbolizer.rs`

**Interfaces:**
- Consumes: `crabka_pprof::DebuginfodConfig`
- Produces: `ColdProfileStore::new_with_debuginfod_config`
- Produces: `native_resolver_from_debuginfod_config`
- Produces: `symbolizer::run_with_config`

- [x] **Step 1: Write failing propagation tests**

Add tests that construct a custom config and call the missing config-aware
helpers:

```rust
let config =
    DebuginfodConfig::new(mebibytes(64), millis(250), secs(3)).unwrap();
native_resolver_from_debuginfod_config(
    vec!["http://127.0.0.1:1".to_string()],
    config,
)
.unwrap();
```

In `cold_store.rs`, construct an in-memory store and empty index through
`new_with_debuginfod_config` with the same config. Existing helper tests remain
unchanged to prove default compatibility.

- [x] **Step 2: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles debuginfod_config --locked
```

Expected: compilation fails on the missing config-aware helpers.

- [x] **Step 3: Add default wrappers and explicit paths**

Route `new_with_debuginfod_urls` through
`new_with_debuginfod_config(..., DebuginfodConfig::default())`, and construct
the resolver with `DebuginfodResolver::with_config` in the explicit path.

Do the same for `native_resolver_from_debuginfod_urls` and `run`, adding
`native_resolver_from_debuginfod_config` and `run_with_config`.

- [x] **Step 4: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles debuginfod_config --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles symbolizer --locked
```

Expected: explicit propagation and compatibility tests pass.

### Task 3: Add Profiles CLI and environment overrides

**Files:**
- Modify: `crates/profiles/Cargo.toml`
- Modify: `crates/profiles/src/bin/crabka-profiles.rs`

**Interfaces:**
- Produces: `--debuginfod-max-artifact-size`
- Produces: `--debuginfod-connect-timeout`
- Produces: `--debuginfod-request-timeout`
- Produces: `CRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE`
- Produces: `CRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT`
- Produces: `CRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT`
- Produces: `CRABKA_PROFILES_DEBUGINFOD_URLS`

- [x] **Step 1: Write failing CLI and environment tests**

Add `temp-env = "0.3"` as a dev dependency and use the repository environment
lock pattern. Assert absent overrides produce `DebuginfodConfig::default`,
explicit CLI values produce `64MiB`, `250ms`, and `3s`, and:

```text
CRABKA_PROFILES_DEBUGINFOD_URLS=http://one.example,http://two.example
CRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE=32MiB
CRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT=500ms
CRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT=4s
```

produce the corresponding URL vector and config. Assert connect `5s` with
request `4s` fails effective-config validation.

- [x] **Step 2: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles --bin crabka-profiles debuginfod_config --offline
```

Expected: compilation fails because the arguments and effective-config helper
do not exist.

- [x] **Step 3: Add optional UOM overrides**

Add `env = "CRABKA_PROFILES_DEBUGINFOD_URLS"` to the existing URL argument.
Add three optional arguments with `parse_positive_whole_byte_size` and
`parse::positive_time` parsers. Implement:

```rust
fn debuginfod_config(cli: &Cli) -> Result<DebuginfodConfig, String>
```

by overlaying supplied values on `DebuginfodConfig::default()` and calling
`DebuginfodConfig::new`.

Validate once immediately after `Cli::parse`, then pass the config to
querier, query-frontend, and symbolizer config-aware helpers.

- [x] **Step 4: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles --bin crabka-profiles debuginfod --locked
```

Expected: all CLI, environment, relation, and existing URL tests pass.

### Task 4: Close the pprof audit slice and verify

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify: `docs/superpowers/plans/2026-07-30-pprof-debuginfod-policy.md`

- [x] **Step 1: Update audit evidence and plan checkboxes**

Record the current scanner count, exact CLI/environment names, all three live
roles, preserved defaults, and verification counts. Keep fixed security
invariants classified as non-configurable.

- [x] **Step 2: Run focused and repository gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-pprof --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-profiles --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

Expected: all focused tests, strict Clippy, formatting, and diff hygiene pass.
