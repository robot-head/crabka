# Admin UI Topic Mutation Timeout Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the three fixed 30,000-millisecond admin UI topic-mutation timeouts with one positive setting resolved from CLI, environment, or the existing default.

**Architecture:** Extend the existing `AdminUiRuntimeArgs` Clap boundary with a `TopicMutationTimeoutMs` newtype validated by `refined_type`. Store it in `AdminUiConfig`; the existing `BrokerAdminMutationSeam` reads the shared value for topic creation, deletion, and partition expansion.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Cargo test/Clippy.

## Global Constraints

- Preserve the exact 30,000-millisecond default.
- `--topic-mutation-timeout-ms` overrides `CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS`.
- Reject zero, malformed, negative, and `i32` overflow values before listener or broker I/O.
- Use one shared setting for create-topic, delete-topic, and create-partitions requests.
- Preserve authentication, request validation, outcome mapping, and `NOT_CONTROLLER` retry behavior.
- Add no CRD or operator field because no checked-in Kubernetes owner deploys `crabka-admin-ui`.
- Do not migrate unrelated existing admin UI settings.
- Any crate in the repository may add the existing workspace-pinned `refined_type` dependency when it owns a validated newtype.
- This slice adds no dependency and must not change `Cargo.lock`.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for lock-aware commands.
- Preserve unrelated dirty and untracked files; stage only paths named by the current task.

---

## File Map

- `crates/admin-ui/src/config.rs`: validated timeout type, default, Clap input, and runtime configuration storage.
- `crates/admin-ui/src/main.rs`: copy the resolved runtime timeout into `AdminUiConfig`.
- `crates/admin-ui/src/server_fns.rs`: pass the configured timeout to all three topic-mutation client calls.
- `crates/admin-ui/tests/config.rs`: typed boundary, parse failures, environment input, and CLI precedence.
- `docs/configuration-audit.md`: evidence, classification, and next unresolved owner.

### Task 1: Expose and propagate the topic-mutation timeout

**Files:**
- Modify: `crates/admin-ui/src/config.rs`
- Modify: `crates/admin-ui/src/main.rs`
- Modify: `crates/admin-ui/src/server_fns.rs`
- Modify: `crates/admin-ui/tests/config.rs`

**Interfaces:**
- Produces: `pub const DEFAULT_TOPIC_MUTATION_TIMEOUT_MS: i32`
- Produces: `pub struct TopicMutationTimeoutMs(i32)`
- Produces: `TopicMutationTimeoutMs::new(i32) -> Result<TopicMutationTimeoutMs, String>`
- Produces: `TopicMutationTimeoutMs::into_value(self) -> i32`
- Produces: `FromStr`, `Display`, and `Default` for `TopicMutationTimeoutMs`
- Produces: `AdminUiRuntimeArgs::topic_mutation_timeout_ms: TopicMutationTimeoutMs`
- Produces: `AdminUiConfig::topic_mutation_timeout_ms: TopicMutationTimeoutMs`
- Consumes: `refined_type::rule::GreaterI32<0>`

- [ ] **Step 1: Write failing typed-boundary and precedence tests**

Extend the imports in `crates/admin-ui/tests/config.rs`:

```rust
use crabka_admin_ui::config::{
    AdminUiConfig, AdminUiRuntimeArgs, BrokerSecurityConfig, ConfigError,
    DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES, MutationJsonBodyLimitBytes,
    SessionTtlSeconds, TopicMutationTimeoutMs,
};
```

Add these tests after the session-TTL tests:

```rust
#[test]
fn topic_mutation_timeout_default_remains_thirty_seconds() {
    let cfg = AdminUiConfig::default();

    assert_eq!(cfg.topic_mutation_timeout_ms.into_value(), 30_000);
}

#[test]
fn topic_mutation_timeout_accepts_one_millisecond() {
    assert_eq!(
        TopicMutationTimeoutMs::new(1)
            .expect("one millisecond is valid")
            .into_value(),
        1
    );
}

#[test]
fn topic_mutation_timeout_rejects_invalid_values() {
    assert!(TopicMutationTimeoutMs::new(0).is_err());

    let overflowing = format!("{}0", i32::MAX);
    for invalid in ["0", "not-a-number", "-1", overflowing.as_str()] {
        assert!(
            AdminUiRuntimeArgs::try_parse_from([
                "crabka-admin-ui",
                "--topic-mutation-timeout-ms",
                invalid,
            ])
            .is_err(),
            "{invalid:?} must be rejected"
        );
    }
}

#[test]
fn topic_mutation_timeout_environment_and_cli_precedence() {
    let output = Command::new(std::env::current_exe().expect("test binary path is available"))
        .arg("--exact")
        .arg("topic_mutation_timeout_precedence_child")
        .arg("--nocapture")
        .env("CRABKA_ADMIN_UI_TOPIC_TIMEOUT_CHILD", "1")
        .env("CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS", "32")
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
fn topic_mutation_timeout_precedence_child() {
    if std::env::var_os("CRABKA_ADMIN_UI_TOPIC_TIMEOUT_CHILD").is_none() {
        return;
    }

    let from_env = AdminUiRuntimeArgs::try_parse_from(["crabka-admin-ui"])
        .expect("environment value is valid");
    assert_eq!(from_env.topic_mutation_timeout_ms.into_value(), 32);

    let from_cli = AdminUiRuntimeArgs::try_parse_from([
        "crabka-admin-ui",
        "--topic-mutation-timeout-ms",
        "64",
    ])
    .expect("CLI value is valid");
    assert_eq!(from_cli.topic_mutation_timeout_ms.into_value(), 64);
}
```

These tests catch a changed default, missing positivity or range validation,
an incorrectly derived flag name, missing environment support, and reversed
precedence. Expected values are hand-derived literals rather than mirrors of
production constants.

- [ ] **Step 2: Run the focused tests to verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test config topic_mutation_timeout --locked
```

Expected: compilation fails because `TopicMutationTimeoutMs` and the new
runtime/configuration fields do not exist.

- [ ] **Step 3: Implement the minimal validated type**

In `crates/admin-ui/src/config.rs`, extend the import:

```rust
use refined_type::rule::{GreaterI32, GreaterU64, GreaterUsize};
```

Add the type after `SessionTtlSeconds` and its trait implementations:

```rust
/// Default Kafka request timeout for admin UI topic mutations.
pub const DEFAULT_TOPIC_MUTATION_TIMEOUT_MS: i32 = 30_000;

/// A positive Kafka request timeout for admin UI topic mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicMutationTimeoutMs(i32);

impl TopicMutationTimeoutMs {
    /// Validate a topic-mutation request timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not positive.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated timeout in milliseconds.
    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

impl Default for TopicMutationTimeoutMs {
    fn default() -> Self {
        Self::new(DEFAULT_TOPIC_MUTATION_TIMEOUT_MS)
            .expect("default topic-mutation timeout is positive")
    }
}

impl fmt::Display for TopicMutationTimeoutMs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for TopicMutationTimeoutMs {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}
```

- [ ] **Step 4: Add CLI/environment input and typed config storage**

Add this field to `AdminUiRuntimeArgs`:

```rust
/// Kafka request timeout for topic mutations, in milliseconds.
#[arg(
    long = "topic-mutation-timeout-ms",
    env = "CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS",
    default_value_t = TopicMutationTimeoutMs::default()
)]
pub topic_mutation_timeout_ms: TopicMutationTimeoutMs,
```

Add this field to `AdminUiConfig`:

```rust
pub topic_mutation_timeout_ms: TopicMutationTimeoutMs,
```

Initialize it in `AdminUiConfig::default`:

```rust
topic_mutation_timeout_ms: TopicMutationTimeoutMs::default(),
```

- [ ] **Step 5: Propagate the typed timeout to all three calls**

In `crates/admin-ui/src/main.rs`, add:

```rust
cfg.topic_mutation_timeout_ms = runtime_args.topic_mutation_timeout_ms;
```

In `crates/admin-ui/src/server_fns.rs`, replace each of the three `30_000`
arguments with:

```rust
self.0.cfg.topic_mutation_timeout_ms.into_value()
```

Do not change the `AdminMutationSeam` trait, `AdminFacade`, `AdminClient`, or
the request/retry logic.

- [ ] **Step 6: Run focused tests to verify GREEN**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --test config topic_mutation_timeout --locked
```

Expected: all five focused tests pass.

- [ ] **Step 7: Prove all production call sites use the setting**

Run:

```bash
if rg -n "30_000" crates/admin-ui/src/server_fns.rs; then
  exit 1
fi
rg -o "topic_mutation_timeout_ms\\.into_value\\(\\)" crates/admin-ui/src/server_fns.rs | wc -l
```

Expected: the first search prints nothing; the second command prints exactly
`3`.

- [ ] **Step 8: Run package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-admin-ui --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-admin-ui --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p crabka-admin-ui --locked -- --help
target/debug/crabka-admin-ui --help | rg -c -- '--topic-mutation-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Expected: tests and strict Clippy pass; help lists the flag exactly once;
formatting and diff checks pass; `Cargo.lock` has no diff.

- [ ] **Step 9: Review and commit the implementation**

Inspect the focused diff:

```bash
git diff -- \
  crates/admin-ui/src/config.rs \
  crates/admin-ui/src/main.rs \
  crates/admin-ui/src/server_fns.rs \
  crates/admin-ui/tests/config.rs
```

Stage and commit only those files:

```bash
git add \
  crates/admin-ui/src/config.rs \
  crates/admin-ui/src/main.rs \
  crates/admin-ui/src/server_fns.rs \
  crates/admin-ui/tests/config.rs
git commit -m "feat(admin-ui): expose topic mutation timeout"
```

### Task 2: Close the audit slice

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: the implemented `TopicMutationTimeoutMs` configuration flow.
- Produces: exact audit evidence and the next unresolved repository owner.

- [ ] **Step 1: Capture exact audit evidence**

Run:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/admin-ui/'
rg -n "topic_mutation_timeout_ms|TopicMutationTimeoutMs|DEFAULT_TOPIC_MUTATION_TIMEOUT_MS|topic-mutation-timeout-ms|CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS" crates/admin-ui docs/configuration-audit.md
rg -n "Duration::|_seconds|_millis|timeout|interval|backoff|capacity|limit" crates/admin-ui/src
```

Record the exact total scanner line/file counts. Classify every admin UI
scanner line and every focused-search line into mutually exclusive production
flow, test/harness, prior-audit, invariant, structural, or unresolved-owner
categories whose counts sum to the exact totals.

Inspect the remaining repository scanner output and name the next real
unresolved operational owner. Do not classify protocol constants, sentinels,
test fixtures, ignored arguments, static UI data, or already-configured
defaults as unresolved.

- [ ] **Step 2: Append the audit section**

Append `## Admin UI Topic Mutation Timeout` to
`docs/configuration-audit.md` with:

- the 30,000-millisecond default;
- flag, environment variable, and CLI precedence;
- positive `i32` validation;
- the exact `AdminUiRuntimeArgs -> AdminUiConfig -> BrokerAdminMutationSeam`
  flow to all three client calls;
- preserved authentication, validation, outcome, and retry behavior;
- why no CRD/operator field exists;
- exact scanner and focused-search counts and classifications;
- focused test, call-site count, package test, strict Clippy, help,
  formatting, diff, and lockfile evidence;
- the next real unresolved repository owner.

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
git commit -m "docs(audit): record admin UI topic timeout"
```

After the commit, inspect `git status --short` and confirm only the user's
pre-existing unrelated changes and untracked plans remain.
