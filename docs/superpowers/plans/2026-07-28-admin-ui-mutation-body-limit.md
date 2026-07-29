# Admin UI Mutation Body Limit Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed admin UI mutation JSON body limit with a positive typed setting resolved from CLI, environment, or the existing one-mebibyte default.

**Architecture:** `AdminUiRuntimeArgs` owns the new Clap CLI/environment boundary, while `AdminUiConfig` stores a `MutationJsonBodyLimitBytes` validated through `refined_type`. The existing authenticated mutation helper reads the value from `AppState`, so every mutation route shares one configured limit and preserves authentication-first behavior.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Axum, Tokio, Cargo test/Clippy.

## Global Constraints

- Preserve the exact default of 1,048,576 bytes.
- CLI `--mutation-json-body-limit-bytes` overrides `CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES`.
- Reject zero, malformed, negative, and platform-overflowing inputs before listener or broker I/O.
- Keep authentication before body buffering and JSON decoding.
- Preserve HTTP 413 with `request body too large` for authenticated oversized requests.
- Add no CRD or operator field because no checked-in Kubernetes owner deploys `crabka-admin-ui`.
- Do not migrate unrelated existing admin UI environment settings.
- Use `refined_type::rule::GreaterUsize<0>` for numeric validation.
- Any crate in the repository may add the existing workspace-pinned
  `refined_type` dependency when it owns a validated newtype. This slice adds
  no other dependency.
- Allow `Cargo.lock` to add the corresponding direct `refined_type` dependency
  entry for any such crate; versions and transitive packages must not change.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for lock-aware commands.
- Do not touch or stage unrelated dirty or untracked files.

---

### Task 1: Typed runtime input and shared mutation limit

**Files:**
- Modify: `crates/admin-ui/Cargo.toml:27-45`
- Modify: `crates/admin-ui/src/config.rs:3-120`
- Modify: `crates/admin-ui/src/main.rs:1-11`
- Modify: `crates/admin-ui/src/server.rs:25-26,492-510`
- Test: `crates/admin-ui/tests/config.rs:1-32`
- Test: `crates/admin-ui/tests/smoke.rs:367-386`

**Interfaces:**
- Produces: `DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES: usize`
- Produces: `MutationJsonBodyLimitBytes::new(usize) -> Result<Self, String>`
- Produces: `MutationJsonBodyLimitBytes::into_value(self) -> usize`
- Produces: `AdminUiRuntimeArgs::mutation_json_body_limit_bytes`
- Produces: `AdminUiConfig::mutation_json_body_limit_bytes`
- Consumes: `AppState::cfg` in the existing central `parse_authenticated_json_request`

- [ ] **Step 1: Write failing configuration tests**

In `crates/admin-ui/tests/config.rs`, import `clap::Parser` and the new
configuration types, then add tests equivalent to:

```rust
use clap::Parser;
use crabka_admin_ui::config::{
    AdminUiConfig, AdminUiRuntimeArgs, BrokerSecurityConfig, ConfigError,
    DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES, MutationJsonBodyLimitBytes,
};

#[test]
fn mutation_json_body_limit_default_and_boundaries_are_typed() {
    let cfg = AdminUiConfig::default();

    assert_eq!(
        cfg.mutation_json_body_limit_bytes.into_value(),
        DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES
    );
    assert_eq!(
        MutationJsonBodyLimitBytes::new(1)
            .expect("one byte is valid")
            .into_value(),
        1
    );
    assert!(MutationJsonBodyLimitBytes::new(0).is_err());

    let overflowing = format!("{}0", usize::MAX);
    for invalid in ["0", "not-a-number", "-1", overflowing.as_str()] {
        assert!(
            AdminUiRuntimeArgs::try_parse_from([
                "crabka-admin-ui",
                "--mutation-json-body-limit-bytes",
                invalid,
            ])
            .is_err(),
            "{invalid:?} must be rejected"
        );
    }
}

#[test]
fn mutation_json_body_limit_environment_and_cli_precedence() {
    let output = Command::new(std::env::current_exe().expect("test binary path is available"))
        .arg("--exact")
        .arg("mutation_json_body_limit_precedence_child")
        .arg("--nocapture")
        .env("CRABKA_ADMIN_UI_BODY_LIMIT_CHILD", "1")
        .env("CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES", "32")
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
fn mutation_json_body_limit_precedence_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_BODY_LIMIT_CHILD").is_none() {
        return;
    }

    let from_env = AdminUiRuntimeArgs::try_parse_from(["crabka-admin-ui"])
        .expect("environment value is valid");
    assert_eq!(from_env.mutation_json_body_limit_bytes.into_value(), 32);

    let from_cli = AdminUiRuntimeArgs::try_parse_from([
        "crabka-admin-ui",
        "--mutation-json-body-limit-bytes",
        "64",
    ])
    .expect("CLI value is valid");
    assert_eq!(from_cli.mutation_json_body_limit_bytes.into_value(), 64);
}
```

- [ ] **Step 2: Write the failing shared-route limit test**

Replace the oversized-body test in `crates/admin-ui/tests/smoke.rs` with a
small configured limit and iterate over all eleven mutation paths:

```rust
#[tokio::test]
async fn authenticated_mutation_routes_share_the_configured_body_limit() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(
        Arc::new(AdminUiConfig {
            mutation_json_body_limit_bytes: MutationJsonBodyLimitBytes::new(16)
                .expect("test limit is valid"),
            ..AdminUiConfig::default()
        }),
        sessions,
    );
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    for path in [
        "/topics/create",
        "/topics/delete",
        "/topics/partitions",
        "/topics/configs",
        "/acls/create",
        "/acls/delete",
        "/users/scram/upsert",
        "/users/scram/delete",
        "/quotas/upsert",
        "/quotas/delete",
        "/log-dirs/move",
    ] {
        let response =
            post_json_from(app.clone(), path, "x".repeat(17), Some(cookie.clone())).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{path}");
        let text = response_text(response).await;
        assert!(text.contains("request body too large"), "{path}: {text}");
    }

    assert_eq!(factory.mutation_seam_calls.load(Ordering::SeqCst), 0);
    assert_eq!(factory.total_mutation_calls(), 0);
}
```

Add `MutationJsonBodyLimitBytes` to the test's existing config imports.

- [ ] **Step 3: Run the new tests and verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test config mutation_json_body_limit --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test smoke authenticated_mutation_routes_share_the_configured_body_limit --locked
```

Expected: compilation fails because `AdminUiRuntimeArgs`,
`MutationJsonBodyLimitBytes`, the default constant, and the config field do not
exist. This is the required feature-missing failure.

- [ ] **Step 4: Implement the validated configuration boundary**

Add `refined_type.workspace = true` to `crates/admin-ui/Cargo.toml`.

In `crates/admin-ui/src/config.rs`, add:

```rust
use std::{fmt, net::SocketAddr, path::PathBuf, str::FromStr};

use clap::Parser;
use refined_type::rule::GreaterUsize;

pub const DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationJsonBodyLimitBytes(usize);

impl MutationJsonBodyLimitBytes {
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> usize {
        self.0
    }
}

impl Default for MutationJsonBodyLimitBytes {
    fn default() -> Self {
        Self::new(DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES)
            .expect("default mutation JSON body limit is positive")
    }
}

impl fmt::Display for MutationJsonBodyLimitBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for MutationJsonBodyLimitBytes {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

#[derive(Debug, Clone, Parser)]
#[command(name = "crabka-admin-ui")]
pub struct AdminUiRuntimeArgs {
    #[arg(
        long,
        env = "CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES",
        default_value_t = MutationJsonBodyLimitBytes::default()
    )]
    pub mutation_json_body_limit_bytes: MutationJsonBodyLimitBytes,
}
```

Document the public constant, type, constructor, accessor, argument struct, and
field. Add this field to `AdminUiConfig`:

```rust
pub mutation_json_body_limit_bytes: MutationJsonBodyLimitBytes,
```

and initialize it in `AdminUiConfig::default()`:

```rust
mutation_json_body_limit_bytes: MutationJsonBodyLimitBytes::default(),
```

- [ ] **Step 5: Route the resolved value through main and Axum**

In `crates/admin-ui/src/main.rs`, import `clap::Parser`, parse
`AdminUiRuntimeArgs` before `AdminUiConfig::from_env()`, and assign its field:

```rust
use anyhow::Context;
use clap::Parser;

use crabka_admin_ui::config::{AdminUiConfig, AdminUiRuntimeArgs};

let runtime_args = AdminUiRuntimeArgs::parse();
let mut cfg = AdminUiConfig::from_env().context("load admin UI config")?;
cfg.mutation_json_body_limit_bytes = runtime_args.mutation_json_body_limit_bytes;
```

In `crates/admin-ui/src/server.rs`, delete
`MUTATION_JSON_BODY_LIMIT_BYTES` and replace the body read with:

```rust
let body = to_bytes(
    request.into_body(),
    state
        .app
        .cfg
        .mutation_json_body_limit_bytes
        .into_value(),
)
```

- [ ] **Step 6: Run focused tests and verify GREEN**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test config mutation_json_body_limit --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test smoke authenticated_mutation_routes_share_the_configured_body_limit --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test smoke post_mutation_routes_authenticate_before_decoding_request_body --locked
```

Expected: all selected tests pass; the shared-route test reports eleven HTTP
413 responses and no mutation seam invocation.

- [ ] **Step 7: Run the implementation gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-admin-ui --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p crabka-admin-ui --locked -- --help
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Expected: tests and strict Clippy pass; help lists
`--mutation-json-body-limit-bytes` exactly once; formatting and diff checks
pass; the lockfile diff contains only `refined_type` in
`crabka-admin-ui`'s dependency list.

- [ ] **Step 8: Commit the implementation**

Stage only:

```bash
git add Cargo.lock crates/admin-ui/Cargo.toml crates/admin-ui/src/config.rs crates/admin-ui/src/main.rs crates/admin-ui/src/server.rs crates/admin-ui/tests/config.rs crates/admin-ui/tests/smoke.rs docs/superpowers/specs/2026-07-28-admin-ui-mutation-body-limit-design.md
git commit -m "feat(admin-ui): expose mutation body limit"
```

### Task 2: Audit evidence and next owner

**Files:**
- Modify: `docs/configuration-audit.md:2894-end`

**Interfaces:**
- Consumes: the committed Task 1 value flow and verification output
- Produces: exact repository scanner evidence and the next unresolved admin UI owner

- [ ] **Step 1: Refresh scanner and focused evidence**

Run:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/admin-ui/'
rg -n "mutation_json_body_limit_bytes|MutationJsonBodyLimitBytes|DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES|mutation-json-body-limit-bytes|CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES" crates/admin-ui docs/configuration-audit.md
```

Record the exact total scanner line/file counts and focused line counts. The
admin UI direct scanner subset must classify every line into:

- configured mutation-body policy;
- fixed permission bits;
- fixed session cookie protocol name;
- fixed static sidebar navigation;
- test or prior-audit evidence;
- unresolved operational owner.

Inspect all remaining numeric and duration literals in `crates/admin-ui/src`.
The existing `session_ttl_seconds: 8 * 60 * 60` default has no CLI/environment
input and is expected to become the next unresolved owner unless current code
proves otherwise.

- [ ] **Step 2: Append the audit section**

Append `## Admin UI Mutation JSON Body Limit` to
`docs/configuration-audit.md`. Include:

- exact default, flag, environment variable, and CLI precedence;
- `GreaterUsize<0>` validation and pre-I/O rejection;
- the exact value-flow chain from input to `to_bytes`;
- preserved authentication-first, HTTP 413, and HTTP 400 behavior;
- no CRD/operator field because there is no deployment owner;
- exact scanner/focused counts and mutually exclusive classification;
- exact Task 1 verification results;
- `session_ttl_seconds` as the adjacent pending policy if it remains
  runtime-unexposed.

- [ ] **Step 3: Verify documentation and the combined slice**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-admin-ui --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Re-run both scanner commands from Step 1 and confirm the documented totals
match their live output.

- [ ] **Step 4: Commit the audit evidence**

Stage only:

```bash
git add docs/configuration-audit.md
git commit -m "docs(audit): record admin UI body limit"
```

- [ ] **Step 5: Obtain final review**

Review the full slice from the design commit through the audit commit. Reject
completion for any raw integer at the runtime boundary, missing CLI/environment
precedence proof, route that bypasses the configured limit, changed
authentication/error behavior, unjustified CRD field, stale scanner count, or
unrelated dirty-file inclusion.
