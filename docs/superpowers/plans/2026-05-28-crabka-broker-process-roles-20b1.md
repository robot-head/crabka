# Broker `process.roles` + Role Gating — Implementation Plan (Plan 1 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the broker a KRaft `process.roles` concept (`controller`, `broker`, or both) and gate self-registration and data-partition hosting on the `broker` role, so a controller-only node is a quorum voter that hosts no data and is not advertised as a broker.

**Architecture:** Add a `NodeRole` enum and a `roles: Vec<NodeRole>` field to `BrokerConfig` (default `[Controller, Broker]` so all existing single-node behavior is unchanged). Parse it from the `[process]` TOML section and a `--process-roles` CLI flag. Gate the two role-dependent startup side effects (self-registration in `Broker::start`, partition disk scan/recovery) behind a tested `is_broker()` predicate. Controller-only nodes therefore never write a `V1BrokerRegistration`, so they fall out of `Metadata`/`DescribeCluster` automatically (no handler change needed).

**Tech Stack:** Rust, `crabka-broker` crate, `serde`/`toml` (file config), `clap` (CLI). This is the foundational slice of the KRaft role-separation design (`docs/superpowers/specs/2026-05-28-crabka-kraft-role-separation-20ab-design.md`, Component A). End-to-end role separation (broker-only observer fetch, multi-node integration tests) lands in Plan 2.

**Scope note:** Plan 1 does **not** change the raft membership model. A broker-only node still starts its `Controller` exactly as today; turning broker-only nodes into true non-voting observers of `__cluster_metadata` is Plan 2. Plan 1 delivers the config contract plus controller-only data/advertisement gating.

---

### Task 1: `NodeRole` enum + `roles` field on `BrokerConfig`

**Files:**
- Modify: `crates/broker/src/config.rs` (add enum near top; add field to `BrokerConfig`; set in `for_tests` ~line 404 and `Default` ~line 605)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/broker/src/config.rs` (after `for_tests_uses_bootstrap_mode`, ~line 792):

```rust
    #[test]
    fn defaults_to_combined_roles() {
        let d = BrokerConfig::default();
        assert!(d.is_controller(), "default node is a controller");
        assert!(d.is_broker(), "default node is a broker");
        assert_eq!(
            d.roles,
            vec![NodeRole::Controller, NodeRole::Broker],
            "default roles are the combined set"
        );

        let t = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(t.is_controller() && t.is_broker());
    }

    #[test]
    fn controller_only_is_not_a_broker() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        assert!(c.is_controller());
        assert!(!c.is_broker());
    }

    #[test]
    fn broker_only_is_not_a_controller() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Broker],
            ..BrokerConfig::default()
        };
        assert!(c.is_broker());
        assert!(!c.is_controller());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker --lib config::tests::defaults_to_combined_roles`
Expected: FAIL — compile error, `NodeRole` not found / no field `roles` / no method `is_controller`.

- [ ] **Step 3: Write minimal implementation**

In `crates/broker/src/config.rs`, add the enum after the imports (before `ListenerSpec`, ~line 15):

```rust
/// KRaft `process.roles`. A node is a metadata-quorum `Controller`, a data
/// `Broker`, or both. Default is the combined set `[Controller, Broker]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeRole {
    Controller,
    Broker,
}
```

Add the field to `BrokerConfig` immediately after `broker_id` (~line 50):

```rust
    /// KRaft `process.roles`. Controls whether this node is a metadata
    /// quorum voter (`Controller`), hosts data partitions + registers as a
    /// broker (`Broker`), or both. Default: `[Controller, Broker]`.
    pub roles: Vec<NodeRole>,
```

Set it in `for_tests` (in the struct literal, right after `broker_id: 1,` ~line 405) and in `Default` (after `broker_id: 1,` ~line 606), identically:

```rust
            roles: vec![NodeRole::Controller, NodeRole::Broker],
```

Add the helper methods inside `impl BrokerConfig` (after `effective_listeners`, ~line 598):

```rust
    /// True when this node hosts data partitions and registers as a broker.
    #[must_use]
    pub fn is_broker(&self) -> bool {
        self.roles.contains(&NodeRole::Broker)
    }

    /// True when this node participates in the `__cluster_metadata` quorum.
    #[must_use]
    pub fn is_controller(&self) -> bool {
        self.roles.contains(&NodeRole::Controller)
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-broker --lib config::tests::`
Expected: PASS (the three new tests plus all existing config tests).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/config.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): add process.roles (NodeRole) to BrokerConfig

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: Validate `roles` in `BrokerConfig::validate`

**Files:**
- Modify: `crates/broker/src/config.rs` (`validate`, ~line 499; add error variants in `crates/broker/src/lib.rs` or wherever `BrokerError` is defined)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/broker/src/config.rs`:

```rust
    #[test]
    fn rejects_empty_roles() {
        let c = BrokerConfig {
            roles: vec![],
            ..BrokerConfig::default()
        };
        assert!(matches!(c.validate(), Err(BrokerError::EmptyRoles)));
    }

    #[test]
    fn rejects_broker_only_node_listed_as_its_own_voter() {
        // node_id 1 is in the default single-voter quorum; a broker-only
        // node must not be a voter of itself.
        let c = BrokerConfig {
            roles: vec![NodeRole::Broker],
            node_id: 1,
            controller_quorum_voters: vec![(1, "127.0.0.1:9093".parse().unwrap())],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::NonControllerIsVoter { node_id: 1 })
        ));
    }

    #[test]
    fn combined_default_passes_role_validation() {
        BrokerConfig::default()
            .validate()
            .expect("combined default validates");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker --lib config::tests::rejects_empty_roles`
Expected: FAIL — `BrokerError::EmptyRoles` / `NonControllerIsVoter` not defined.

- [ ] **Step 3: Write minimal implementation**

Find the `BrokerError` enum (search `crates/broker/src/` for `pub enum BrokerError`). Add two variants (match the existing style — most variants are unit or struct-like with `#[error(...)]` thiserror attributes):

```rust
    #[error("process.roles must list at least one role")]
    EmptyRoles,

    #[error("node {node_id} is not a controller but appears in its own controller_quorum_voters")]
    NonControllerIsVoter { node_id: crabka_raft::NodeId },
```

In `BrokerConfig::validate`, add at the very top of the function body (before the listener checks, ~line 500):

```rust
        if self.roles.is_empty() {
            return Err(BrokerError::EmptyRoles);
        }
        if !self.is_controller()
            && self
                .controller_quorum_voters
                .iter()
                .any(|(id, _)| *id == self.node_id)
        {
            return Err(BrokerError::NonControllerIsVoter {
                node_id: self.node_id,
            });
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-broker --lib config::tests::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/config.rs crates/broker/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): validate process.roles (non-empty; non-controller not self-voter)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

(Adjust the `git add` path if `BrokerError` lives in a different file, e.g. `crates/broker/src/error.rs`.)

---

### Task 3: Parse `[process] roles` from TOML in `file_config`

**Files:**
- Modify: `crates/broker/src/file_config.rs` (add `FileProcessConfig` struct; add `process` field to `FileConfig` ~line 44; map it in `apply_to`)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module of `crates/broker/src/file_config.rs` (mirror the existing `apply_to`-style tests; they typically parse a TOML string into `FileConfig`, call `.apply_to(&mut cfg)`, and assert on `cfg`):

```rust
    #[test]
    fn process_roles_controller_only_from_toml() {
        let toml = r#"
            [process]
            roles = ["controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert_eq!(cfg.roles, vec![crate::config::NodeRole::Controller]);
    }

    #[test]
    fn process_roles_both_from_toml() {
        let toml = r#"
            [process]
            roles = ["broker", "controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert_eq!(
            cfg.roles,
            vec![crate::config::NodeRole::Broker, crate::config::NodeRole::Controller]
        );
    }

    #[test]
    fn process_roles_rejects_unknown_role() {
        let toml = r#"
            [process]
            roles = ["wizard"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        let err = fc.apply_to(&mut cfg).expect_err("unknown role rejected");
        assert!(matches!(err, FileConfigError::InvalidConfig(_)));
    }

    #[test]
    fn process_section_absent_leaves_default_roles() {
        let fc: FileConfig = toml::from_str("").expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert_eq!(
            cfg.roles,
            vec![crate::config::NodeRole::Controller, crate::config::NodeRole::Broker]
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker --lib file_config::tests::process_roles_controller_only_from_toml`
Expected: FAIL — no field `process` on `FileConfig` / `FileProcessConfig` undefined.

- [ ] **Step 3: Write minimal implementation**

Add the section struct near the other `File*Config` structs in `crates/broker/src/file_config.rs` (e.g. after `FileDelegationTokenConfig` ~line 267):

```rust
/// `[process]` TOML section — KRaft `process.roles`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileProcessConfig {
    /// Role strings: `"controller"`, `"broker"` (case-insensitive). Empty
    /// or absent leaves the `BrokerConfig` default `[Controller, Broker]`.
    #[serde(default)]
    pub roles: Vec<String>,
}
```

Add the field to `FileConfig` (after `super_users`, ~line 90):

```rust
    #[serde(default)]
    pub process: Option<FileProcessConfig>,
```

In `apply_to`, add a block (alongside the other section handlers, e.g. just before the delegation-token block ~line 716):

```rust
        // KRaft `process.roles`. Absent / empty leaves the BrokerConfig
        // default (`[Controller, Broker]`).
        if let Some(p) = &self.process {
            if !p.roles.is_empty() {
                let mut roles = Vec::with_capacity(p.roles.len());
                for r in &p.roles {
                    let role = match r.to_ascii_lowercase().as_str() {
                        "controller" => crate::config::NodeRole::Controller,
                        "broker" => crate::config::NodeRole::Broker,
                        other => {
                            return Err(FileConfigError::InvalidConfig(format!(
                                "unknown process.role `{other}` (expected `controller` or `broker`)"
                            )));
                        }
                    };
                    roles.push(role);
                }
                cfg.roles = roles;
            }
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-broker --lib file_config::tests::process`
Expected: PASS (all four new tests).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/file_config.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): parse [process] roles from broker.toml

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 4: `--process-roles` CLI flag

**Files:**
- Modify: `crates/broker/src/bin/broker.rs` (add to `Args` ~line 11-86; map in `main` ~line 128-149)

- [ ] **Step 1: Write the failing test**

`Args` is a private struct in the binary, so test the role-string parsing via a small free function. Add to `crates/broker/src/bin/broker.rs` a `#[cfg(test)] mod tests` (or extend an existing one):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_roles_arg_maps_strings() {
        assert_eq!(
            parse_roles_arg(&["controller".to_string(), "broker".to_string()]).unwrap(),
            vec![
                crabka_broker::config::NodeRole::Controller,
                crabka_broker::config::NodeRole::Broker
            ]
        );
    }

    #[test]
    fn parse_roles_arg_rejects_unknown() {
        assert!(parse_roles_arg(&["nope".to_string()]).is_err());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker --bin crabka-broker parse_roles_arg`
Expected: FAIL — `parse_roles_arg` not defined.

- [ ] **Step 3: Write minimal implementation**

Add the field to the `Args` struct (after `broker_id`, ~line 60):

```rust
    /// KRaft `process.roles`, comma-separated (`controller`, `broker`).
    /// Defaults to the combined set when unset. The operator normally sets
    /// this via the `[process]` section of `--config-file` instead.
    #[arg(
        long,
        env = "CRABKA_PROCESS_ROLES",
        value_delimiter = ',',
        num_args = 0..
    )]
    process_roles: Vec<String>,
```

Add the free function near the top of the file (after the `use` statements, before `Args`):

```rust
fn parse_roles_arg(
    roles: &[String],
) -> Result<Vec<crabka_broker::config::NodeRole>, String> {
    use crabka_broker::config::NodeRole;
    roles
        .iter()
        .map(|r| match r.to_ascii_lowercase().as_str() {
            "controller" => Ok(NodeRole::Controller),
            "broker" => Ok(NodeRole::Broker),
            other => Err(format!(
                "unknown --process-roles value `{other}` (expected `controller` or `broker`)"
            )),
        })
        .collect()
}
```

In `main`, after the `config` struct literal is built and before `apply_to` is called against the TOML file (so a `[process]` section in `--config-file` still wins), set roles only when the flag was provided:

```rust
    if !args.process_roles.is_empty() {
        config.roles = parse_roles_arg(&args.process_roles)
            .map_err(|e| BrokerError::Startup(e))?;
    }
```

(If `main` returns a different error type, map to it; match the surrounding `?`-error convention in that function.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-broker --bin crabka-broker parse_roles_arg`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/bin/broker.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): --process-roles CLI flag

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 5: Gate self-registration on the `broker` role

**Files:**
- Modify: `crates/broker/src/broker.rs` (self-registration block, ~line 923-976)

- [ ] **Step 1: Write the failing test**

The registration block lives inside `Broker::start` and is awkward to unit-test directly. Assert the decision predicate instead — add to the `tests` module in `crates/broker/src/config.rs`:

```rust
    #[test]
    fn controller_only_does_not_register() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        // Registration is gated on is_broker(); a controller-only node skips it.
        assert!(!c.is_broker());
    }
```

(This pins the contract the call-site relies on; the end-to-end "controller absent from Metadata" assertion is a Plan 2 integration test.)

- [ ] **Step 2: Run test to verify it fails / passes**

Run: `cargo test -p crabka-broker --lib config::tests::controller_only_does_not_register`
Expected: PASS already (Task 1 added `is_broker`). This test documents the contract; the behavioral change is the wiring in Step 3.

- [ ] **Step 3: Write minimal implementation**

In `crates/broker/src/broker.rs`, wrap the entire self-registration block (the `{ … }` scope at ~line 923 that builds `endpoints`, `self_reg`, waits for a leader, and calls `controller.submit_change`) in a role guard. Change the opening of the block from:

```rust
        // 2. Wait for a leader, then submit a self-registration record so
        //    other brokers can discover us. ...
        {
```

to:

```rust
        // 2. Wait for a leader, then submit a self-registration record so
        //    other brokers can discover us. Controller-only nodes never
        //    register — they host no data and must not appear as brokers
        //    in Metadata/DescribeCluster.
        if config.is_broker() {
```

The block's closing `}` stays as-is. (The body is unchanged; only the `{` becomes `if config.is_broker() {`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo build -p crabka-broker && cargo test -p crabka-broker --lib config::tests::controller_only_does_not_register`
Expected: PASS, and the crate compiles (the guarded block still type-checks).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/broker.rs crates/broker/src/config.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): controller-only nodes skip self-registration

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 6: Gate partition scan/recovery on the `broker` role

**Files:**
- Modify: `crates/broker/src/broker.rs` (partition scan block, ~line 1015-1034)

- [ ] **Step 1: Write the failing test**

Same predicate-contract approach — add to the `tests` module in `crates/broker/src/config.rs`:

```rust
    #[test]
    fn controller_only_hosts_no_partitions() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        // Partition scan/recovery is gated on is_broker().
        assert!(!c.is_broker());
    }
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p crabka-broker --lib config::tests::controller_only_hosts_no_partitions`
Expected: PASS (documents the contract; behavioral wiring is Step 3).

- [ ] **Step 3: Write minimal implementation**

In `crates/broker/src/broker.rs`, guard the scan/spawn loop. The block is:

```rust
        let partitions: Arc<DashMap<(String, i32), Arc<Partition>>> = Arc::new(DashMap::new());
        let scan_dirs = log_dir_status.online_subset(&config.all_log_dirs());
        for (topic, partition_id, owning_dir) in log_dir::scan_all(&scan_dirs)? {
            let dir = log_dir::partition_dir(&owning_dir, &topic, partition_id);
            let log = crabka_log::Log::open(&dir, config.log_config.clone())?;
            let part = spawn_partition(
                topic.clone(),
                partition_id,
                owning_dir,
                log,
                log_dir_status.clone(),
            );
            partitions.insert((topic.clone(), partition_id), part);
        }
```

Wrap only the `scan_dirs` + `for` loop in the role guard, leaving the `partitions` map always created (handlers expect it to exist, just empty for controller-only nodes):

```rust
        let partitions: Arc<DashMap<(String, i32), Arc<Partition>>> = Arc::new(DashMap::new());
        // Controller-only nodes host no data partitions, so they skip the
        // disk scan/recovery entirely.
        if config.is_broker() {
            let scan_dirs = log_dir_status.online_subset(&config.all_log_dirs());
            for (topic, partition_id, owning_dir) in log_dir::scan_all(&scan_dirs)? {
                let dir = log_dir::partition_dir(&owning_dir, &topic, partition_id);
                let log = crabka_log::Log::open(&dir, config.log_config.clone())?;
                let part = spawn_partition(
                    topic.clone(),
                    partition_id,
                    owning_dir,
                    log,
                    log_dir_status.clone(),
                );
                partitions.insert((topic.clone(), partition_id), part);
            }
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo build -p crabka-broker && cargo test -p crabka-broker --lib`
Expected: PASS — crate compiles, all broker lib unit tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/broker.rs crates/broker/src/config.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): controller-only nodes skip partition scan/recovery

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 7: Workspace check + format

**Files:** none (verification only)

- [ ] **Step 1: Run the full broker test suite**

Run: `cargo test -p crabka-broker`
Expected: PASS (lib unit tests + any integration tests unaffected — defaults keep combined-mode behavior identical).

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -p crabka-broker --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 3: Format (required before push — CI gates on `cargo fmt --check`)**

Run: `cargo fmt -p crabka-broker`
Then: `cargo fmt --check -p crabka-broker`
Expected: clean.

- [ ] **Step 4: Commit any formatting changes**

```bash
git add -A
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "style: cargo fmt for process.roles changes

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>" || echo "nothing to format"
```

---

## Self-Review

**Spec coverage (Component A of the design):**
- §3.1 config field + TOML `process.roles` + default `[Controller, Broker]` → Tasks 1, 3, 4. ✓
- §3.1 validation (non-empty; non-controller not its own voter) → Task 2. ✓
- §3.3 data-partition gating (skip scan when not a broker) → Task 6. ✓
- §3.4 registration gating (controller-only emits no `V1BrokerRegistration`) → Task 5. ✓
- §3.4 controllers absent from `Metadata`/`DescribeCluster` → falls out of Task 5 (no handler change; verified end-to-end in Plan 2). ✓
- Deferred to Plan 2: broker-only raft observer behavior, multi-node integration verification. Explicitly noted in the Scope note.

**Type consistency:** `NodeRole::{Controller, Broker}` and `BrokerConfig::{roles, is_broker, is_controller}` are used identically across Tasks 1–6. `FileProcessConfig.roles: Vec<String>` (Task 3) and `Args.process_roles: Vec<String>` (Task 4) both parse to `Vec<NodeRole>`. `BrokerError::{EmptyRoles, NonControllerIsVoter}` defined in Task 2, used only there.

**Placeholder scan:** No TBD/TODO; every code step shows full code. Two call-site adjustments (`git add` path for `BrokerError`; `main`'s error-mapping convention) are flagged with concrete fallbacks rather than left vague.
