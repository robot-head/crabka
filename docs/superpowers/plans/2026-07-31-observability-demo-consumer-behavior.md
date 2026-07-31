# Observability Demo Consumer Behavior Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose classic Consumer offset-reset, isolation, and assignor choices on the observability demo Consume role without changing defaults.

**Architecture:** The existing client-consumer enums own strict `FromStr` parsing. Optional demo CLI/env fields resolve to the existing defaults, reject explicit use on other roles, and flow directly into the existing Consumer builder setters.

**Tech Stack:** Rust, `std::str::FromStr`, Clap derive/env, Docker Compose, Cargo tests and Clippy.

## Global Constraints

- Preserve defaults: `latest`, `read-uncommitted`, and `range`.
- Accept only the exact spellings listed in the design.
- CLI overrides environment through Clap's existing precedence.
- Reject explicit values on Produce or Stream before telemetry or external I/O.
- Add no dependency, mirror enum, wrapper type, CRD, UOM quantity, or `refined_type`.
- Expose Compose variables only on `demo-consume`.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not stage or edit the four protected untracked plans dated 2026-07-28.
- Do not run `cargo clean`; that remains the final repository-goal cleanup.

---

### Task 1: Shared enum parsing

**Files:**
- Modify: `crates/client-consumer/src/builder.rs`
- Modify: `crates/client-consumer/src/assignor/mod.rs`

**Interfaces:**
- Produces: `FromStr<Err = String>` for `AutoOffsetReset`, `IsolationLevel`, and `Assignor`.
- Accepted values: `latest`, `earliest`, `none`, `read-uncommitted`, `read-committed`, `range`, and `cooperative-sticky`.

- [ ] **Step 1: Write failing parsing tests**

Add table-driven tests beside each owning enum:

```rust
#[test]
fn consumer_behavior_values_parse_exact_spellings() {
    assert2::assert!(matches!(
        "earliest".parse::<AutoOffsetReset>(),
        Ok(AutoOffsetReset::Earliest)
    ));
    assert2::assert!(matches!(
        "latest".parse::<AutoOffsetReset>(),
        Ok(AutoOffsetReset::Latest)
    ));
    assert2::assert!(matches!(
        "none".parse::<AutoOffsetReset>(),
        Ok(AutoOffsetReset::None)
    ));
    assert2::assert!("EARLIEST".parse::<AutoOffsetReset>().is_err());
    assert2::assert!("unknown".parse::<AutoOffsetReset>().is_err());
}

#[test]
fn isolation_level_values_parse_exact_spellings() {
    assert2::assert!(
        "read-uncommitted".parse::<IsolationLevel>().unwrap()
            == IsolationLevel::ReadUncommitted
    );
    assert2::assert!(
        "read-committed".parse::<IsolationLevel>().unwrap()
            == IsolationLevel::ReadCommitted
    );
    assert2::assert!("read_committed".parse::<IsolationLevel>().is_err());
    assert2::assert!("unknown".parse::<IsolationLevel>().is_err());
}
```

Add this test in `assignor/mod.rs`:

```rust
#[test]
fn assignor_values_parse_exact_spellings() {
    assert2::assert!("range".parse::<Assignor>().unwrap() == Assignor::Range);
    assert2::assert!(
        "cooperative-sticky".parse::<Assignor>().unwrap() == Assignor::CooperativeSticky
    );
    assert2::assert!("cooperative_sticky".parse::<Assignor>().is_err());
    assert2::assert!("unknown".parse::<Assignor>().is_err());
}
```

- [ ] **Step 2: Run focused tests and verify the red state**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer consumer_behavior_values_parse_exact_spellings --locked
```

Expected: compilation fails because the three enums do not implement
`FromStr`.

- [ ] **Step 3: Implement the minimum shared parsers**

Implement direct matches on each enum:

```rust
impl std::str::FromStr for AutoOffsetReset {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            "none" => Ok(Self::None),
            _ => Err(format!("invalid auto offset reset: {value}")),
        }
    }
}

impl std::str::FromStr for IsolationLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "read-uncommitted" => Ok(Self::ReadUncommitted),
            "read-committed" => Ok(Self::ReadCommitted),
            _ => Err(format!("invalid isolation level: {value}")),
        }
    }
}

impl std::str::FromStr for Assignor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "range" => Ok(Self::Range),
            "cooperative-sticky" => Ok(Self::CooperativeSticky),
            _ => Err(format!("invalid assignor: {value}")),
        }
    }
}
```

- [ ] **Step 4: Verify and commit shared parsing**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
```

Then commit only the two client-consumer files:

```bash
git add crates/client-consumer/src/builder.rs crates/client-consumer/src/assignor/mod.rs
git commit -m "feat(consumer): parse behavior choices"
```

### Task 2: Demo CLI, propagation, and Compose

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create: `crates/observability-demo-app/tests/consumer_behavior_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: the three `FromStr` implementations from Task 1.
- Produces: `effective_consumer_behavior(&Cli) -> io::Result<(AutoOffsetReset, IsolationLevel, Assignor)>`.
- Propagates: resolved values into the matching existing Consumer builder setters.

- [ ] **Step 1: Write failing resolver and subprocess tests**

Add optional typed CLI fields with the exact long names and environment names
from the design, then write resolver tests asserting unchanged defaults,
independent overrides, and non-Consume rejection.

Create `consumer_behavior_config.rs` using
`env!("CARGO_BIN_EXE_observability-demo-app")` and a hostile bootstrap address.
Assert:

```rust
// Environment values are accepted before any connection attempt.
command.env("CRABKA_DEMO_CONSUMER_ASSIGNOR", "cooperative-sticky");

// CLI wins over environment.
command.args(["--consumer-assignor", "range"]);

// Unknown values and explicit non-Consume use exit unsuccessfully without
// reaching telemetry or broker I/O.
```

- [ ] **Step 2: Write failing Compose ownership test**

Extend `consumer_behavior_is_configurable_only_on_the_consume_role` in
`observability_demo_config.rs` to assert the three variables occur exactly
under `demo-consume`, with defaults:

```yaml
CRABKA_DEMO_CONSUMER_AUTO_OFFSET_RESET: ${CRABKA_DEMO_CONSUMER_AUTO_OFFSET_RESET:-latest}
CRABKA_DEMO_CONSUMER_ISOLATION_LEVEL: ${CRABKA_DEMO_CONSUMER_ISOLATION_LEVEL:-read-uncommitted}
CRABKA_DEMO_CONSUMER_ASSIGNOR: ${CRABKA_DEMO_CONSUMER_ASSIGNOR:-range}
```

- [ ] **Step 3: Run focused tests and verify the red state**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app consumer_behavior --locked
```

Expected: failure because the resolver, CLI fields, and Compose variables are
not implemented.

- [ ] **Step 4: Implement direct demo propagation**

Import the existing enums, define the three optional CLI/env fields, and add:

```rust
fn effective_consumer_behavior(
    cli: &Cli,
) -> std::io::Result<(AutoOffsetReset, IsolationLevel, Assignor)> {
    let configured = [
        (
            "--consumer-auto-offset-reset",
            cli.consumer_auto_offset_reset.is_some(),
        ),
        (
            "--consumer-isolation-level",
            cli.consumer_isolation_level.is_some(),
        ),
        ("--consumer-assignor", cli.consumer_assignor.is_some()),
    ];
    if cli.role != Role::Consume
        && let Some((name, _)) = configured.into_iter().find(|(_, set)| *set)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{name} is only valid with --role consume"),
        ));
    }
    Ok((
        cli.consumer_auto_offset_reset.unwrap_or(AutoOffsetReset::Latest),
        cli.consumer_isolation_level.unwrap_or(IsolationLevel::ReadUncommitted),
        cli.consumer_assignor.unwrap_or(Assignor::Range),
    ))
}
```

Resolve it before telemetry initialization, pass the tuple through
`main -> run_consume`, call the three existing builder setters, and add only
the three `demo-consume` Compose entries.

- [ ] **Step 5: Verify and commit the demo surface**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
```

Commit only Task 2 files:

```bash
git add crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/consumer_behavior_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose consumer behavior"
```

### Task 3: Audit and close the slice

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify: `docs/superpowers/plans/2026-07-31-observability-demo-consumer-behavior.md`

**Interfaces:**
- Consumes: verified implementation and exact test counts from Tasks 1 and 2.
- Produces: a completed plan and permanent audit record.

- [ ] **Step 1: Audit ownership and defaults**

Use `rg` to confirm each CLI/env name appears only in its declaration,
resolver/tests, and `demo-consume`; confirm the three builder defaults remain
unchanged and no demo mirror enum or new dependency was added.

- [ ] **Step 2: Run workspace gates**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

- [ ] **Step 3: Re-run demo tests after formatting**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
```

- [ ] **Step 4: Document and commit**

Record defaults, exact CLI/env pairs, accepted spellings, precedence, early
role rejection, direct builder propagation, Compose ownership, exclusions,
test counts, and workspace-gate results in `configuration-audit.md`. Mark
every plan checkbox complete, run `git diff --check`, and commit:

```bash
git add docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-observability-demo-consumer-behavior.md
git commit -m "docs(config): close demo consumer behavior"
```
