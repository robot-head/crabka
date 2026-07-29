# Admin UI Session TTL Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed eight-hour admin UI session lifetime with a positive, platform-representable setting resolved from CLI, environment, or the existing default.

**Architecture:** Extend the existing `AdminUiRuntimeArgs` Clap boundary with a `SessionTtlSeconds` newtype validated by `refined_type` and `Instant::checked_add`. Store the typed value in `AdminUiConfig` and convert it to `Duration` only when `AppState` constructs `SessionStore`.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Tokio, Cargo test/Clippy.

## Global Constraints

- Preserve the exact 28,800-second default.
- `--session-ttl-seconds` overrides `CRABKA_ADMIN_UI_SESSION_TTL_SECONDS`.
- Reject zero, malformed, negative, and platform-unrepresentable values before listener or broker I/O.
- Keep `SessionStore`'s `Duration` API and its zero/oversized defensive behavior unchanged.
- Add no CRD or operator field because no checked-in Kubernetes owner deploys `crabka-admin-ui`.
- Do not migrate unrelated existing admin UI settings.
- Any crate in the repository may add the existing workspace-pinned `refined_type` dependency when it owns a validated newtype.
- This slice adds no dependency and must not change `Cargo.lock`.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for lock-aware commands.
- Preserve unrelated dirty and untracked files; stage only paths named by the current task.

---

## File Map

- `crates/admin-ui/src/config.rs`: typed session-TTL boundary, default, Clap input, and runtime configuration storage.
- `crates/admin-ui/src/main.rs`: copy the resolved runtime TTL into `AdminUiConfig`.
- `crates/admin-ui/src/server.rs`: convert the typed TTL to `Duration` at `SessionStore` construction.
- `crates/admin-ui/tests/config.rs`: typed boundary, parse failures, environment input, and CLI precedence.
- `crates/admin-ui/tests/server_fns.rs`: prove `AppState` passes the configured TTL to `SessionStore`.
- `docs/configuration-audit.md`: evidence, classification, and next unresolved owner.

### Task 1: Expose the validated session TTL

**Files:**
- Modify: `crates/admin-ui/src/config.rs`
- Modify: `crates/admin-ui/src/main.rs`
- Modify: `crates/admin-ui/src/server.rs`
- Modify: `crates/admin-ui/tests/config.rs`
- Modify: `crates/admin-ui/tests/server_fns.rs`

**Interfaces:**
- Produces: `pub const DEFAULT_SESSION_TTL_SECONDS: u64`
- Produces: `pub struct SessionTtlSeconds(u64)`
- Produces: `SessionTtlSeconds::new(u64) -> Result<SessionTtlSeconds, String>`
- Produces: `SessionTtlSeconds::duration(self) -> Duration`
- Produces: `FromStr`, `Display`, and `Default` for `SessionTtlSeconds`
- Produces: `AdminUiRuntimeArgs::session_ttl: SessionTtlSeconds`
- Produces: `AdminUiConfig::session_ttl: SessionTtlSeconds`
- Consumes: `refined_type::rule::GreaterU64<0>`

- [ ] **Step 1: Write failing typed-boundary and precedence tests**

Extend the imports in `crates/admin-ui/tests/config.rs`:

```rust
use std::{net::SocketAddr, path::PathBuf, process::Command, time::Duration};

use crabka_admin_ui::config::{
    AdminUiConfig, AdminUiRuntimeArgs, BrokerSecurityConfig, ConfigError,
    DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES, MutationJsonBodyLimitBytes, SessionTtlSeconds,
};
```

Add these tests after the mutation-body-limit tests:

```rust
#[test]
fn session_ttl_default_remains_eight_hours() {
    let cfg = AdminUiConfig::default();

    assert_eq!(cfg.session_ttl.duration(), Duration::from_hours(8));
}

#[test]
fn session_ttl_accepts_one_second() {
    assert_eq!(
        SessionTtlSeconds::new(1)
            .expect("one second is valid")
            .duration(),
        Duration::from_secs(1)
    );
}

#[test]
fn session_ttl_rejects_invalid_values() {
    assert!(SessionTtlSeconds::new(0).is_err());
    assert!(SessionTtlSeconds::new(u64::MAX).is_err());

    let unrepresentable = u64::MAX.to_string();
    for invalid in ["0", "not-a-number", "-1", unrepresentable.as_str()] {
        assert!(
            AdminUiRuntimeArgs::try_parse_from([
                "crabka-admin-ui",
                "--session-ttl-seconds",
                invalid,
            ])
            .is_err(),
            "{invalid:?} must be rejected"
        );
    }
}

#[test]
fn session_ttl_environment_and_cli_precedence() {
    let output = Command::new(std::env::current_exe().expect("test binary path is available"))
        .arg("--exact")
        .arg("session_ttl_precedence_child")
        .arg("--nocapture")
        .env("CRABKA_ADMIN_UI_SESSION_TTL_CHILD", "1")
        .env("CRABKA_ADMIN_UI_SESSION_TTL_SECONDS", "32")
        .output()
        .expect("child test process runs");

    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn session_ttl_precedence_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_SESSION_TTL_CHILD").is_none() {
        return;
    }

    let from_env = AdminUiRuntimeArgs::try_parse_from(["crabka-admin-ui"])
        .expect("environment value is valid");
    assert_eq!(from_env.session_ttl.duration(), Duration::from_secs(32));

    let from_cli = AdminUiRuntimeArgs::try_parse_from([
        "crabka-admin-ui",
        "--session-ttl-seconds",
        "64",
    ])
    .expect("CLI value is valid");
    assert_eq!(from_cli.session_ttl.duration(), Duration::from_secs(64));
}
```

In `crates/admin-ui/tests/server_fns.rs`, import the new type:

```rust
config::{AdminUiConfig, BrokerSecurityConfig, SessionTtlSeconds},
```

Change the configured field in `app_state_carries_config_and_sessions`:

```rust
session_ttl: SessionTtlSeconds::new(37).expect("test TTL is valid"),
```

- [ ] **Step 2: Run the focused tests to verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test config session_ttl --locked
```

Expected: compilation fails because `SessionTtlSeconds`,
`DEFAULT_SESSION_TTL_SECONDS`, and the new configuration fields do not exist.

- [ ] **Step 3: Implement the minimal validated type**

In `crates/admin-ui/src/config.rs`, extend the standard-library and
`refined_type` imports:

```rust
use std::{
    fmt,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use refined_type::rule::{GreaterU64, GreaterUsize};
```

Add the type after `MutationJsonBodyLimitBytes` and its trait implementations:

```rust
/// Default server-side lifetime for an authenticated admin UI session.
pub const DEFAULT_SESSION_TTL_SECONDS: u64 = 28_800;

/// A positive session lifetime representable by the platform monotonic clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTtlSeconds(u64);

impl SessionTtlSeconds {
    /// Validate an admin UI session lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or cannot be added to the
    /// platform monotonic clock.
    pub fn new(value: u64) -> Result<Self, String> {
        let value = GreaterU64::<0>::new(value)
            .map(refined_type::Refined::into_value)
            .map_err(|error| error.to_string())?;

        if Instant::now()
            .checked_add(Duration::from_secs(value))
            .is_none()
        {
            return Err("session TTL exceeds the platform monotonic clock".to_string());
        }

        Ok(Self(value))
    }

    /// Return the validated session lifetime.
    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}

impl Default for SessionTtlSeconds {
    fn default() -> Self {
        Self::new(DEFAULT_SESSION_TTL_SECONDS)
            .expect("default session TTL is positive and representable")
    }
}

impl fmt::Display for SessionTtlSeconds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SessionTtlSeconds {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}
```

- [ ] **Step 4: Add the CLI/environment field and typed config storage**

Add this field to `AdminUiRuntimeArgs`:

```rust
/// Server-side lifetime for an authenticated session, in seconds.
#[arg(
    long = "session-ttl-seconds",
    env = "CRABKA_ADMIN_UI_SESSION_TTL_SECONDS",
    default_value_t = SessionTtlSeconds::default()
)]
pub session_ttl: SessionTtlSeconds,
```

In `AdminUiConfig`, replace:

```rust
pub session_ttl_seconds: u64,
```

with:

```rust
pub session_ttl: SessionTtlSeconds,
```

In `AdminUiConfig::default`, replace:

```rust
session_ttl_seconds: 8 * 60 * 60,
```

with:

```rust
session_ttl: SessionTtlSeconds::default(),
```

- [ ] **Step 5: Propagate the typed setting**

In `crates/admin-ui/src/main.rs`, add:

```rust
cfg.session_ttl = runtime_args.session_ttl;
```

In `crates/admin-ui/src/server.rs`, remove the unused `Duration` import and
replace:

```rust
let session_ttl = Duration::from_secs(cfg.session_ttl_seconds);
```

with:

```rust
let session_ttl = cfg.session_ttl.duration();
```

Do not change `SessionStore`, cookie handling, or session-expiry logic.

- [ ] **Step 6: Run focused tests to verify GREEN**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test config session_ttl --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test server_fns app_state_carries_config_and_sessions --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test session --locked
```

Expected: all focused tests pass, including the unchanged zero-duration
immediate-expiry and oversized-duration no-panic tests.

- [ ] **Step 7: Run package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-admin-ui --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p crabka-admin-ui --locked -- --help
target/debug/crabka-admin-ui --help | rg -c -- '--session-ttl-seconds'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Expected: tests and strict Clippy pass; help lists
`--session-ttl-seconds` exactly once; formatting and diff checks pass;
`Cargo.lock` has no diff.

- [ ] **Step 8: Review and commit the implementation**

Inspect the focused diff:

```bash
git diff -- \
  crates/admin-ui/src/config.rs \
  crates/admin-ui/src/main.rs \
  crates/admin-ui/src/server.rs \
  crates/admin-ui/tests/config.rs \
  crates/admin-ui/tests/server_fns.rs
```

Stage and commit only those files:

```bash
git add \
  crates/admin-ui/src/config.rs \
  crates/admin-ui/src/main.rs \
  crates/admin-ui/src/server.rs \
  crates/admin-ui/tests/config.rs \
  crates/admin-ui/tests/server_fns.rs
git commit -m "feat(admin-ui): expose session TTL"
```

### Task 2: Close the audit slice

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: the implemented `SessionTtlSeconds` configuration flow.
- Produces: exact audit evidence and the next unresolved admin UI owner.

- [ ] **Step 1: Capture exact audit evidence**

Run:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/admin-ui/'
rg -n "session_ttl|SessionTtlSeconds|DEFAULT_SESSION_TTL_SECONDS|session-ttl-seconds|CRABKA_ADMIN_UI_SESSION_TTL_SECONDS" crates/admin-ui docs/configuration-audit.md
rg -n "30_000|Duration::|_seconds|_millis" crates/admin-ui/src
```

Record the exact total scanner line/file counts. Classify every admin UI
scanner line and every focused-search line into mutually exclusive production
flow, test/harness, prior-audit, invariant, structural, or unresolved-owner
categories whose counts sum to the exact totals.

Confirm that the three `30_000` broker-admin request timeouts in
`crates/admin-ui/src/server_fns.rs` are the next unresolved operational owner.

- [ ] **Step 2: Append the audit section**

Append `## Admin UI Session TTL` to `docs/configuration-audit.md` with:

- the 28,800-second default;
- flag, environment variable, and CLI precedence;
- positive and platform-representability validation;
- the exact `AdminUiRuntimeArgs -> AdminUiConfig -> AppState -> SessionStore`
  flow;
- preserved session and cookie behavior;
- why no CRD/operator field exists;
- exact scanner and focused-search counts and classifications;
- focused test, package test, strict Clippy, help, formatting, diff, and
  lockfile evidence;
- the three fixed 30,000-millisecond broker-admin request timeouts as the
  adjacent pending policy.

- [ ] **Step 3: Re-run final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-admin-ui --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

Expected: package tests, strict Clippy, formatting, and diff hygiene pass;
`Cargo.lock` remains unchanged; scanner counts match the audit text.

- [ ] **Step 4: Review and commit the audit**

Inspect and stage only the audit:

```bash
git diff -- docs/configuration-audit.md
git add docs/configuration-audit.md
git commit -m "docs(audit): record admin UI session TTL"
```

After the commit, inspect `git status --short` and confirm only the user's
pre-existing unrelated changes and untracked plans remain.
