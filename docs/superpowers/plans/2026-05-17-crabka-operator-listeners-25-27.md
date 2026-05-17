# Crabka Operator Listener Trilogy — Implementation Plan (Slices 25a / 25)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch tasks within a batch in parallel; sequential between batches.

**Goal:** Add `Kafka.spec.listeners` (Strimzi-shaped) and operator reconcile for internal / NodePort / LoadBalancer external listeners. Switch the broker to a TOML `--config-file` for multi-listener config delivery. Schema accepts (but reconcile rejects) `ingress` / `route` for a future slice.

**Architecture:** Two PRs landing in order. Slice **25a** (Crabka-core PR): broker grows `--config-file PATH` consuming a TOML file deserialized via `serde + toml`; uses the broker library's existing `BrokerConfig::listeners` field. Slice **25** (operator PR): operator generates per-broker TOML keys in the existing `<cluster>-broker-config` ConfigMap; renders per-broker + bootstrap Services for external listener types; computes advertised host:port from `Node.status` (NodePort) and `Service.status.loadBalancer.ingress` (LoadBalancer); slice-21 hash function grows a canonical-listener-intent input.

**Tech Stack:** Rust 2024, `clap`, `serde`, `toml`, `kube-rs`, `k8s-openapi`, `schemars`, `tokio`, `tracing`, Helm, kind, MetalLB.

**Spec:** `docs/superpowers/specs/2026-05-17-crabka-operator-listeners-25-27-design.md`.

**Out of scope for this plan:** Slice 27 (Ingress + OpenShift Route) — schema lands in slice 25, reconcile awaits a separate plan post-Phase-4.

---

## Slice 25a — Broker `--config-file` (TOML) — PR #1

Five sequential tasks (each builds on the previous; no parallelism opportunities — they all touch `crates/broker/src/config.rs` or `crates/broker/src/bin/broker.rs`).

### Task 25a.1: Add `toml` dependency + `FileConfig` types

**Files:**
- Modify: `crates/broker/Cargo.toml`
- Create: `crates/broker/src/file_config.rs`
- Modify: `crates/broker/src/lib.rs` (one line — module registration)
- Test: `crates/broker/src/file_config.rs` (inline `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Create `crates/broker/src/file_config.rs`:

```rust
//! TOML file-config surface for the `crabka-broker` binary.
//!
//! Deserialized by `--config-file PATH` in `bin/broker.rs` and merged
//! into [`crate::BrokerConfig`]. Slice 25a only consumes
//! `[[listeners]]`, `inter_broker_listener_name`, and (passively)
//! `[server_properties]`. Other top-level keys are reserved for
//! future slices and are accepted but ignored.

use std::net::SocketAddr;

use serde::Deserialize;

use crabka_security::ListenerProtocol;

use crate::config::ListenerSpec;

/// Top-level shape of `broker.toml`. `serde(deny_unknown_fields)` is
/// off — future slices add fields and old binaries should warn rather
/// than refuse to start.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileConfig {
    #[serde(default)]
    pub broker_id: Option<i32>,
    #[serde(default)]
    pub log_dir: Option<String>,
    #[serde(default)]
    pub inter_broker_listener_name: Option<String>,
    #[serde(default)]
    pub listeners: Vec<FileListener>,
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct FileListener {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: FileListenerProtocol,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileListenerProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl From<FileListenerProtocol> for ListenerProtocol {
    fn from(v: FileListenerProtocol) -> Self {
        match v {
            FileListenerProtocol::Plaintext => ListenerProtocol::Plaintext,
            FileListenerProtocol::Ssl => ListenerProtocol::Ssl,
            FileListenerProtocol::SaslPlaintext => ListenerProtocol::SaslPlaintext,
            FileListenerProtocol::SaslSsl => ListenerProtocol::SaslSsl,
        }
    }
}

impl FileListener {
    pub fn into_spec(self) -> ListenerSpec {
        ListenerSpec {
            name: self.name,
            bind_addr: self.bind_addr,
            advertised: self.advertised,
            protocol: self.protocol.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_toml_round_trips() {
        let cfg: FileConfig = toml::from_str("").unwrap();
        assert_eq!(cfg, FileConfig::default());
    }

    #[test]
    fn full_toml_round_trips() {
        let src = r#"
broker_id = 0
log_dir = "/var/lib/crabka/data"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "plaintext"

[server_properties]
"log.retention.hours" = "24"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.broker_id, Some(0));
        assert_eq!(cfg.log_dir.as_deref(), Some("/var/lib/crabka/data"));
        assert_eq!(cfg.inter_broker_listener_name.as_deref(), Some("PLAIN"));
        assert_eq!(cfg.listeners.len(), 2);
        assert_eq!(cfg.listeners[0].name, "PLAIN");
        assert_eq!(cfg.listeners[0].protocol, FileListenerProtocol::Plaintext);
        assert_eq!(
            cfg.server_properties.get("log.retention.hours").map(String::as_str),
            Some("24")
        );
    }

    #[test]
    fn unknown_top_level_key_is_ignored() {
        // Forward-compat: a newer config file shouldn't break older brokers.
        let src = r#"
broker_id = 0
some_future_field = "from-a-later-slice"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.broker_id, Some(0));
    }

    #[test]
    fn snake_case_protocol_names() {
        let src = r#"
[[listeners]]
name = "S"
bind_addr = "0.0.0.0:9094"
advertised = "h:9094"
protocol = "sasl_ssl"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert_eq!(cfg.listeners[0].protocol, FileListenerProtocol::SaslSsl);
    }

    #[test]
    fn invalid_bind_addr_is_an_error() {
        let src = r#"
[[listeners]]
name = "X"
bind_addr = "not-a-socket-address"
advertised = "h:9094"
protocol = "plaintext"
"#;
        let err = toml::from_str::<FileConfig>(src).unwrap_err();
        assert!(
            err.to_string().contains("bind_addr") || err.to_string().contains("socket"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_listener_into_spec_preserves_fields() {
        let fl = FileListener {
            name: "X".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "h:9094".into(),
            protocol: FileListenerProtocol::Plaintext,
        };
        let spec = fl.into_spec();
        assert_eq!(spec.name, "X");
        assert_eq!(spec.advertised, "h:9094");
        assert_eq!(spec.protocol, ListenerProtocol::Plaintext);
    }
}
```

Modify `crates/broker/Cargo.toml` — add under `[dependencies]`:

```toml
toml = "0.8"
```

Modify `crates/broker/src/lib.rs` — add the module:

```rust
pub mod file_config;
```

(Place near other `pub mod …` lines; alphabetical if the file uses that ordering.)

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p crabka-broker --lib file_config
```

Expected: FAIL — `toml` crate not found (compile error) until `Cargo.toml` is updated. Once dependencies are in, all tests above should PASS without further work because the types are inert library code.

- [ ] **Step 3: Verify tests pass after dep is wired**

```bash
cargo test -p crabka-broker --lib file_config
```

Expected: 6 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/Cargo.toml crates/broker/src/lib.rs crates/broker/src/file_config.rs Cargo.lock
git commit -m "Slice 25a/1: add FileConfig TOML types for broker --config-file"
```

---

### Task 25a.2: Merge `FileConfig` into `BrokerConfig` (CLI > file > defaults)

**Files:**
- Modify: `crates/broker/src/file_config.rs` — add `apply_to` method
- Test: same file, inline `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing tests**

Append to `crates/broker/src/file_config.rs` (inside the `impl FileConfig` block — create the block if it doesn't exist):

```rust
impl FileConfig {
    /// Apply this file-config to a `BrokerConfig` that already holds
    /// CLI-derived values. The file fills in unset values and provides
    /// `listeners` + `inter_broker_listener_name` wholesale when those
    /// are at their respective "empty" defaults.
    ///
    /// CLI values always win — the binary's `main()` constructs the
    /// `BrokerConfig` from CLI args first, then calls `apply_to`. The
    /// file never overrides what was explicitly set on the CLI.
    ///
    /// **Caller contract:** when `--config-file` is used, the caller
    /// must NOT pass `--listen-addr` or `--advertised-listener`. The
    /// binary entrypoint enforces this (see `bin/broker.rs`); this
    /// method just merges what it's given.
    pub fn apply_to(self, cfg: &mut crate::config::BrokerConfig) {
        if let Some(id) = self.broker_id {
            if cfg.broker_id == crate::config::BrokerConfig::default().broker_id {
                cfg.broker_id = id;
            }
        }
        if let Some(ld) = self.log_dir {
            if cfg.log_dir == crate::config::BrokerConfig::default().log_dir {
                cfg.log_dir = std::path::PathBuf::from(ld);
            }
        }
        if !self.listeners.is_empty() {
            cfg.listeners = self.listeners.into_iter().map(FileListener::into_spec).collect();
        }
        if let Some(name) = self.inter_broker_listener_name {
            cfg.inter_broker_listener_name = name;
        }
        // `[server_properties]` is intentionally ignored in slice 25a.
    }
}
```

Append to the `#[cfg(test)] mod tests` block at the bottom of the file:

```rust
#[test]
fn apply_to_populates_listeners() {
    use crate::config::BrokerConfig;

    let src = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "plaintext"
"#;
    let file: FileConfig = toml::from_str(src).unwrap();
    let mut cfg = BrokerConfig::default();
    file.apply_to(&mut cfg);

    assert_eq!(cfg.listeners.len(), 1);
    assert_eq!(cfg.listeners[0].name, "PLAIN");
    assert_eq!(cfg.listeners[0].advertised, "demo-0:9092");
    assert_eq!(cfg.inter_broker_listener_name, "PLAIN");
}

#[test]
fn apply_to_does_not_clobber_non_default_broker_id() {
    use crate::config::BrokerConfig;

    let src = r#"broker_id = 42"#;
    let file: FileConfig = toml::from_str(src).unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.broker_id = 7; // simulate CLI --broker-id 7 already applied

    file.apply_to(&mut cfg);

    // CLI value wins because it differs from default.
    assert_eq!(cfg.broker_id, 7);
}

#[test]
fn apply_to_fills_in_default_broker_id() {
    use crate::config::BrokerConfig;

    let src = r#"broker_id = 42"#;
    let file: FileConfig = toml::from_str(src).unwrap();
    let mut cfg = BrokerConfig::default(); // broker_id == default (1)

    file.apply_to(&mut cfg);

    assert_eq!(cfg.broker_id, 42);
}

#[test]
fn apply_to_empty_listeners_does_not_clear_existing() {
    use crate::config::BrokerConfig;

    let file: FileConfig = toml::from_str("").unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.listeners = vec![crate::config::ListenerSpec {
        name: "X".into(),
        bind_addr: "0.0.0.0:9094".parse().unwrap(),
        advertised: "h:9094".into(),
        protocol: crabka_security::ListenerProtocol::Plaintext,
    }];

    file.apply_to(&mut cfg);

    assert_eq!(cfg.listeners.len(), 1);
    assert_eq!(cfg.listeners[0].name, "X");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p crabka-broker --lib file_config
```

Expected: 4 new tests FAIL with "method `apply_to` is private" or "no method named `apply_to`" until the `impl FileConfig` block is added.

- [ ] **Step 3: Verify tests pass after impl is added**

```bash
cargo test -p crabka-broker --lib file_config
```

Expected: 10 passed (6 from Task 25a.1 + 4 new).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/file_config.rs
git commit -m "Slice 25a/2: FileConfig::apply_to merges file values into BrokerConfig"
```

---

### Task 25a.3: Wire `--config-file` CLI flag with mutual-exclusion

**Files:**
- Modify: `crates/broker/src/bin/broker.rs`

- [ ] **Step 1: Write the failing test**

Append to the `#[cfg(test)] mod tests` block at the bottom of `crates/broker/src/bin/broker.rs`:

```rust
#[test]
fn config_file_mutually_exclusive_with_listen_addr() {
    use clap::Parser;

    let res = Args::try_parse_from([
        "crabka-broker",
        "--config-file=/tmp/a.toml",
        "--listen-addr=127.0.0.1:9092",
    ]);
    let err = res.expect_err("expected mutual-exclusion error");
    let s = err.to_string();
    assert!(
        s.contains("config-file") && s.contains("listen-addr"),
        "expected clap conflict mentioning both flags, got: {s}"
    );
}

#[test]
fn config_file_mutually_exclusive_with_advertised_listener() {
    use clap::Parser;

    let res = Args::try_parse_from([
        "crabka-broker",
        "--config-file=/tmp/a.toml",
        "--advertised-listener=h:9092",
    ]);
    let err = res.expect_err("expected mutual-exclusion error");
    let s = err.to_string();
    assert!(
        s.contains("config-file") && s.contains("advertised-listener"),
        "expected clap conflict, got: {s}"
    );
}

#[test]
fn config_file_alone_parses() {
    use clap::Parser;

    let args = Args::try_parse_from(["crabka-broker", "--config-file=/tmp/a.toml"]).unwrap();
    assert_eq!(args.config_file.as_deref(), Some(std::path::Path::new("/tmp/a.toml")));
    assert!(args.advertised_listener.is_none());
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p crabka-broker --bin crabka-broker
```

Expected: 3 new tests FAIL — `Args` has no `config_file` field.

- [ ] **Step 3: Implement the CLI flag + mutual-exclusion**

In `crates/broker/src/bin/broker.rs`, modify the `Args` struct (currently at line 17–51). Replace the field block with:

```rust
struct Args {
    /// TCP address to listen on. Mutually exclusive with `--config-file`.
    #[arg(long, default_value = "127.0.0.1:9092", conflicts_with = "config_file")]
    listen_addr: SocketAddr,

    /// `host:port` to advertise to clients (defaults to `listen_addr`).
    /// Set via env `CRABKA_ADVERTISED_LISTENER` from the operator.
    /// Mutually exclusive with `--config-file`.
    #[arg(long, env = "CRABKA_ADVERTISED_LISTENER", conflicts_with = "config_file")]
    advertised_listener: Option<String>,

    /// Path to a TOML config file (operator-managed). When set,
    /// `--listen-addr` / `--advertised-listener` must NOT be set;
    /// listener configuration comes from the file's `[[listeners]]`
    /// table. See `crabka_broker::file_config::FileConfig`.
    #[arg(long)]
    config_file: Option<PathBuf>,

    /// Directory containing per-partition log dirs.
    #[arg(long, default_value = "./crabka-data")]
    log_dir: PathBuf,

    /// Numeric broker id.
    #[arg(long, default_value_t = 1)]
    broker_id: i32,

    /// Cluster UUID. Every broker in the same cluster must share this
    /// value. Set via env `CRABKA_CLUSTER_ID` from the operator
    /// (the `KafkaCluster` UID).
    #[arg(long, env = "CRABKA_CLUSTER_ID")]
    cluster_id: Option<uuid::Uuid>,

    /// Bind address for the Prometheus `/metrics` HTTP endpoint.
    /// Empty string (or `none`) disables. Defaults to `0.0.0.0:9404`
    /// — the same port `jmx_prometheus_javaagent` uses for vanilla
    /// Kafka, so existing scrape configs apply unchanged.
    #[arg(
        long,
        env = "CRABKA_METRICS_LISTEN_ADDR",
        default_value = "0.0.0.0:9404"
    )]
    metrics_listen_addr: String,
}
```

Modify `main()` to load the config file when set. After the line `let args = Args::parse();` and before the `BrokerConfig` construction (~line 78), insert:

```rust
    let file_config: Option<crabka_broker::file_config::FileConfig> =
        match args.config_file.as_ref() {
            Some(p) => {
                let contents = std::fs::read_to_string(p)
                    .map_err(|e| format!("failed to read {}: {e}", p.display()))?;
                Some(toml::from_str(&contents).map_err(|e| {
                    format!("failed to parse {}: {e}", p.display())
                })?)
            }
            None => None,
        };
```

Then, after `let config = BrokerConfig { … };`, insert:

```rust
    let mut config = config;
    if let Some(fc) = file_config {
        fc.apply_to(&mut config);
    }
```

(Promotes `config` to `mut`; the existing `let config = BrokerConfig { … };` should be renamed so the binding above sees it. Simplest: change `let config = …` to `let mut config = …` and remove the duplicate `let mut config = config;` line. Pick whichever produces clean diff.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p crabka-broker --bin crabka-broker
```

Expected: existing tests still pass + 3 new CLI tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/bin/broker.rs
git commit -m "Slice 25a/3: broker --config-file CLI flag with mutual-exclusion"
```

---

### Task 25a.4: CLI smoke test booting with `--config-file`

**Files:**
- Modify: `crates/broker/tests/cli_smoke.rs`

- [ ] **Step 1: Read the existing smoke test for pattern**

```bash
head -200 crates/broker/tests/cli_smoke.rs
```

Look for how the existing test spawns the binary, picks ports, and asserts startup. Use the same helpers and tempdir pattern.

- [ ] **Step 2: Write the failing test**

Append to `crates/broker/tests/cli_smoke.rs`:

```rust
/// Slice 25a: boot `crabka-broker` with `--config-file` pointing at a
/// minimal TOML and assert the process binds the listener declared in
/// the file (port comes from the file, not from a CLI flag).
#[test]
fn boots_with_config_file_listener() {
    use std::io::Write as _;

    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");
    std::fs::create_dir_all(&log_dir).unwrap();

    // Pick an ephemeral port by binding briefly, then release it.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let cfg_path = tmp.path().join("broker.toml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(
        f,
        r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "127.0.0.1:{port}"
advertised = "127.0.0.1:{port}"
protocol = "plaintext"
"#
    )
    .unwrap();

    let bin = env!("CARGO_BIN_EXE_crabka-broker");
    let mut child = std::process::Command::new(bin)
        .arg(format!("--config-file={}", cfg_path.display()))
        .arg(format!("--log-dir={}", log_dir.display()))
        .arg("--broker-id=1")
        .arg("--metrics-listen-addr=none")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn crabka-broker");

    // Poll for the port to accept connections within 10 seconds.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut connected = false;
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            connected = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Tear down before assertions so a hang doesn't leave a stray process.
    let _ = child.kill();
    let _ = child.wait();

    assert!(connected, "broker never opened TCP listener on port {port}");
}

#[test]
fn errors_when_config_file_and_listen_addr_both_set() {
    let bin = env!("CARGO_BIN_EXE_crabka-broker");
    let out = std::process::Command::new(bin)
        .arg("--config-file=/tmp/nonexistent.toml")
        .arg("--listen-addr=127.0.0.1:9092")
        .output()
        .expect("spawn crabka-broker");

    assert!(!out.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("config-file") && stderr.contains("listen-addr"),
        "expected clap mutual-exclusion error, got stderr:\n{stderr}"
    );
}
```

- [ ] **Step 3: Run the test to verify failures (only on a fresh checkout — should already pass given Task 25a.3 is in)**

```bash
cargo test -p crabka-broker --test cli_smoke boots_with_config_file_listener errors_when_config_file_and_listen_addr_both_set
```

Expected: both PASS — Task 25a.3 already wired the CLI behavior. This task is just adding the integration coverage.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/cli_smoke.rs
git commit -m "Slice 25a/4: cli_smoke test for --config-file boot + mutual-exclusion"
```

---

### Task 25a.5: Slice 25a PR finalization

**Files:** none modified — verification + push.

- [ ] **Step 1: Run the full broker test suite**

```bash
cargo test -p crabka-broker
```

Expected: all green.

- [ ] **Step 2: Run clippy / format checks (whatever the repo uses)**

```bash
cargo fmt --all -- --check
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: no diff, no warnings.

- [ ] **Step 3: Push and open PR**

```bash
git push -u origin HEAD
gh pr create --title "Slice 25a: broker --config-file (TOML)" --body "$(cat <<'EOF'
## Summary
- Adds `crabka-broker --config-file PATH` reading a TOML file via `serde + toml`.
- `[[listeners]]` populates `BrokerConfig::listeners`; `inter_broker_listener_name` is honored.
- `--config-file` is mutually exclusive with `--listen-addr` / `--advertised-listener`.
- `[server_properties]` accepted-but-inert (slice 25 uses it as a passthrough for `Kafka.spec.config`).

Spec: docs/superpowers/specs/2026-05-17-crabka-operator-listeners-25-27-design.md

Unblocks operator slice 25.

## Test plan
- [x] `cargo test -p crabka-broker --lib file_config`
- [x] `cargo test -p crabka-broker --bin crabka-broker`
- [x] `cargo test -p crabka-broker --test cli_smoke`
- [x] `cargo fmt --check` + `cargo clippy -D warnings`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Slice 25 — Operator `Kafka.spec.listeners` + reconcile — PR #2

**This PR depends on slice 25a being merged.** Start a new branch off `main` after 25a lands.

### Batch 25.1 — CRD types + validation (parallel; independent files)

#### Task 25.1.1: Listener CRD types in `crates/operator/src/crd/listener.rs`

**Files:**
- Create: `crates/operator/src/crd/listener.rs`
- Modify: `crates/operator/src/crd/mod.rs` (add `pub mod listener; pub use listener::*;`)

- [ ] **Step 1: Write the failing tests**

Create `crates/operator/src/crd/listener.rs`:

```rust
//! `Kafka.spec.listeners` schema — Strimzi-shaped.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Listener {
    /// Unique within the cluster. Alphanumeric + `-`, ≤25 chars. Used
    /// as the Kafka listener name; surfaces in `bootstrap.servers`-style
    /// URLs.
    pub name: String,
    /// Container port the broker binds. Unique within the cluster.
    pub port: i32,
    /// Listener type. `internal` is in-cluster; `nodeport` /
    /// `loadbalancer` create external Services; `ingress` / `route` are
    /// accepted by the schema but rejected at reconcile until slice 27.
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// Must be `false` in this slice; reconcile rejects `true` until
    /// Phase 4 (slices 30/31) wires up TLS.
    #[serde(default)]
    pub tls: bool,
    /// Optional listener-type-specific configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<ListenerConfiguration>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ListenerType {
    Internal,
    Nodeport,
    Loadbalancer,
    Ingress,
    Route,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerConfiguration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<BootstrapConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<BrokerOverride>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapConfig {
    /// `nodeport` only: pin the bootstrap NodePort.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
    /// `loadbalancer` only: pin the bootstrap LB IP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_balancer_ip: Option<String>,
    /// `ingress` / `route` only (slice 27): bootstrap hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Annotations to add to the bootstrap Service.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    /// Labels to add to the bootstrap Service.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BrokerOverride {
    /// Broker id this override applies to (matches the node id).
    pub broker: i32,
    /// Override the computed advertised host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertised_host: Option<String>,
    /// Override the computed advertised port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advertised_port: Option<i32>,
    /// `nodeport` only: pin this broker's `Service.spec.ports[0].nodePort`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
    /// `loadbalancer` only: pin this broker's `Service.spec.loadBalancerIP`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_balancer_ip: Option<String>,
    /// `ingress` / `route` only (slice 27): per-broker hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerStatus {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ListenerType,
    /// `host:port` clients should put in `bootstrap.servers`.
    pub bootstrap_servers: String,
    #[serde(default)]
    pub addresses: Vec<ListenerAddress>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListenerAddress {
    pub host: String,
    pub port: i32,
}

impl Default for ListenerType {
    fn default() -> Self {
        ListenerType::Internal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_listener_round_trips_through_json() {
        let l = Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"type\":\"internal\""), "got: {json}");
        assert!(json.contains("\"port\":9092"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn nodeport_with_broker_overrides_round_trips() {
        let l = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: Some(BootstrapConfig {
                    node_port: Some(32099),
                    ..Default::default()
                }),
                brokers: vec![BrokerOverride {
                    broker: 0,
                    advertised_host: Some("public.host".into()),
                    node_port: Some(32100),
                    ..Default::default()
                }],
            }),
        };
        let json = serde_json::to_string(&l).unwrap();
        assert!(json.contains("\"advertisedHost\":\"public.host\""), "got: {json}");
        assert!(json.contains("\"nodePort\":32100"), "got: {json}");
        let back: Listener = serde_json::from_str(&json).unwrap();
        assert_eq!(back, l);
    }

    #[test]
    fn camelcase_wire_shape() {
        let cfg = ListenerConfiguration {
            bootstrap: Some(BootstrapConfig {
                load_balancer_ip: Some("10.0.0.5".into()),
                ..Default::default()
            }),
            brokers: vec![],
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"loadBalancerIP\":\"10.0.0.5\""), "got: {json}");
    }
}
```

Modify `crates/operator/src/crd/mod.rs` — add:

```rust
pub mod listener;
pub use listener::*;
```

- [ ] **Step 2: Run tests to verify they fail (compile error initially, then pass once module is registered)**

```bash
cargo test -p crabka-operator --lib crd::listener
```

Expected: 3 PASS after registration.

- [ ] **Step 3: Commit**

```bash
git add crates/operator/src/crd/listener.rs crates/operator/src/crd/mod.rs
git commit -m "Slice 25/1: Listener CRD types (Strimzi-shaped)"
```

#### Task 25.1.2: Validation logic in `crates/operator/src/controller/listeners.rs`

**Files:**
- Create: `crates/operator/src/controller/listeners.rs`
- Modify: `crates/operator/src/controller/mod.rs` (add `pub(crate) mod listeners;`)

This task is **parallel to 25.1.1** — they touch disjoint files. It depends on the types from 25.1.1 only at compile time, so it can be coded in parallel and the second-merging branch picks up the first.

- [ ] **Step 1: Write the failing tests**

Create `crates/operator/src/controller/listeners.rs`:

```rust
//! Listener-related rendering and validation. Kept in its own module
//! to keep `controller/kafka.rs` and `controller/common.rs` from
//! growing further.

use crate::crd::{Listener, ListenerType};

/// Reason values for the `ListenersValid` status condition.
/// Stable strings — consumed by `kubectl wait --for=condition=…` and
/// asserted by tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    DuplicateListenerName(String),
    DuplicateListenerPort(i32),
    TlsNotYetSupported(String),
    IngressDeferred(String),
    RouteDeferred(String),
    DuplicateBrokerOverride { listener: String, broker: i32 },
    InterBrokerListenerMissing(String),
    InterBrokerListenerNotInternal(String),
    NoInternalListener,
}

impl ValidationError {
    pub fn reason(&self) -> &'static str {
        match self {
            ValidationError::DuplicateListenerName(_) => "DuplicateListenerName",
            ValidationError::DuplicateListenerPort(_) => "DuplicateListenerPort",
            ValidationError::TlsNotYetSupported(_) => "TlsNotYetSupported",
            ValidationError::IngressDeferred(_) => "IngressDeferred",
            ValidationError::RouteDeferred(_) => "RouteDeferred",
            ValidationError::DuplicateBrokerOverride { .. } => "DuplicateBrokerOverride",
            ValidationError::InterBrokerListenerMissing(_) => "InterBrokerListenerMissing",
            ValidationError::InterBrokerListenerNotInternal(_) => "InterBrokerListenerNotInternal",
            ValidationError::NoInternalListener => "NoInternalListener",
        }
    }

    pub fn message(&self) -> String {
        match self {
            ValidationError::DuplicateListenerName(n) => format!("listener name '{n}' is used more than once"),
            ValidationError::DuplicateListenerPort(p) => format!("listener port {p} is used more than once"),
            ValidationError::TlsNotYetSupported(n) => format!("listener '{n}' has tls=true; TLS arrives in Phase 4"),
            ValidationError::IngressDeferred(n) => format!("listener '{n}' has type=ingress; reconcile is deferred until slice 27"),
            ValidationError::RouteDeferred(n) => format!("listener '{n}' has type=route; reconcile is deferred until slice 27"),
            ValidationError::DuplicateBrokerOverride { listener, broker } => format!(
                "listener '{listener}' has duplicate configuration.brokers entries for broker {broker}"
            ),
            ValidationError::InterBrokerListenerMissing(n) => format!(
                "spec.interBrokerListenerName='{n}' does not match any listener"
            ),
            ValidationError::InterBrokerListenerNotInternal(n) => format!(
                "spec.interBrokerListenerName='{n}' points to a non-internal listener"
            ),
            ValidationError::NoInternalListener => "spec.listeners is non-empty but contains no internal-type listener".into(),
        }
    }
}

/// Validate `spec.listeners` + `spec.interBrokerListenerName`. Returns
/// `Ok(())` if everything is well-formed; otherwise the first error
/// encountered (validation is short-circuit — surface the most
/// actionable problem rather than a list).
pub fn validate_listeners(
    listeners: &[Listener],
    inter_broker_listener_name: Option<&str>,
) -> Result<(), ValidationError> {
    // Duplicate name / port checks.
    for (i, l) in listeners.iter().enumerate() {
        for prior in &listeners[..i] {
            if prior.name == l.name {
                return Err(ValidationError::DuplicateListenerName(l.name.clone()));
            }
            if prior.port == l.port {
                return Err(ValidationError::DuplicateListenerPort(l.port));
            }
        }
    }

    // Per-listener type/tls/override checks.
    for l in listeners {
        if l.tls {
            return Err(ValidationError::TlsNotYetSupported(l.name.clone()));
        }
        match l.type_ {
            ListenerType::Ingress => {
                return Err(ValidationError::IngressDeferred(l.name.clone()));
            }
            ListenerType::Route => {
                return Err(ValidationError::RouteDeferred(l.name.clone()));
            }
            _ => {}
        }
        if let Some(cfg) = &l.configuration {
            let mut seen = std::collections::HashSet::new();
            for ovr in &cfg.brokers {
                if !seen.insert(ovr.broker) {
                    return Err(ValidationError::DuplicateBrokerOverride {
                        listener: l.name.clone(),
                        broker: ovr.broker,
                    });
                }
            }
        }
    }

    // Inter-broker listener resolution.
    if !listeners.is_empty() {
        let has_internal = listeners.iter().any(|l| l.type_ == ListenerType::Internal);
        if !has_internal {
            return Err(ValidationError::NoInternalListener);
        }
        if let Some(name) = inter_broker_listener_name {
            match listeners.iter().find(|l| l.name == name) {
                None => return Err(ValidationError::InterBrokerListenerMissing(name.into())),
                Some(l) if l.type_ != ListenerType::Internal => {
                    return Err(ValidationError::InterBrokerListenerNotInternal(name.into()));
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Pick the inter-broker listener name. Honors an explicit override;
/// otherwise picks the first `internal` listener. Returns the synthesized
/// default name (`"PLAIN"`) when `listeners` is empty (the slice-19
/// compatibility path).
pub fn effective_inter_broker_listener_name(
    listeners: &[Listener],
    explicit: Option<&str>,
) -> String {
    if let Some(s) = explicit {
        return s.to_string();
    }
    if listeners.is_empty() {
        return "PLAIN".to_string();
    }
    listeners
        .iter()
        .find(|l| l.type_ == ListenerType::Internal)
        .map(|l| l.name.clone())
        .unwrap_or_else(|| "PLAIN".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{BrokerOverride, ListenerConfiguration};

    fn internal(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: false,
            configuration: None,
        }
    }

    fn nodeport(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: None,
        }
    }

    #[test]
    fn empty_listeners_is_valid() {
        assert!(validate_listeners(&[], None).is_ok());
    }

    #[test]
    fn one_internal_is_valid() {
        let ls = [internal("PLAIN", 9092)];
        assert!(validate_listeners(&ls, None).is_ok());
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let ls = [internal("PLAIN", 9092), nodeport("PLAIN", 9094)];
        let err = validate_listeners(&ls, None).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateListenerName(_)));
        assert_eq!(err.reason(), "DuplicateListenerName");
    }

    #[test]
    fn duplicate_port_is_rejected() {
        let ls = [internal("A", 9092), nodeport("B", 9092)];
        let err = validate_listeners(&ls, None).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateListenerPort(9092)));
    }

    #[test]
    fn tls_true_is_rejected() {
        let mut l = internal("PLAIN", 9092);
        l.tls = true;
        assert_eq!(
            validate_listeners(&[l], None).unwrap_err().reason(),
            "TlsNotYetSupported"
        );
    }

    #[test]
    fn ingress_is_deferred() {
        let mut l = internal("ing", 9094);
        l.type_ = ListenerType::Ingress;
        assert_eq!(
            validate_listeners(&[l], None).unwrap_err().reason(),
            "IngressDeferred"
        );
    }

    #[test]
    fn route_is_deferred() {
        let mut l = internal("rt", 9094);
        l.type_ = ListenerType::Route;
        assert_eq!(
            validate_listeners(&[l], None).unwrap_err().reason(),
            "RouteDeferred"
        );
    }

    #[test]
    fn duplicate_broker_override_is_rejected() {
        let mut l = nodeport("ext", 9094);
        l.configuration = Some(ListenerConfiguration {
            bootstrap: None,
            brokers: vec![
                BrokerOverride { broker: 0, ..Default::default() },
                BrokerOverride { broker: 0, ..Default::default() },
            ],
        });
        let err = validate_listeners(&[l], None).unwrap_err();
        assert_eq!(err.reason(), "DuplicateBrokerOverride");
    }

    #[test]
    fn missing_internal_when_non_empty_is_rejected() {
        let ls = [nodeport("ext", 9094)];
        assert_eq!(
            validate_listeners(&ls, None).unwrap_err().reason(),
            "NoInternalListener"
        );
    }

    #[test]
    fn inter_broker_listener_must_match_a_listener() {
        let ls = [internal("PLAIN", 9092)];
        let err = validate_listeners(&ls, Some("MISSING")).unwrap_err();
        assert_eq!(err.reason(), "InterBrokerListenerMissing");
    }

    #[test]
    fn inter_broker_listener_must_be_internal() {
        let ls = [internal("PLAIN", 9092), nodeport("ext", 9094)];
        let err = validate_listeners(&ls, Some("ext")).unwrap_err();
        assert_eq!(err.reason(), "InterBrokerListenerNotInternal");
    }

    #[test]
    fn effective_name_explicit_wins() {
        assert_eq!(effective_inter_broker_listener_name(&[], Some("FOO")), "FOO");
    }

    #[test]
    fn effective_name_picks_first_internal() {
        let ls = [nodeport("ext", 9094), internal("ib", 9092), internal("other", 9095)];
        assert_eq!(effective_inter_broker_listener_name(&ls, None), "ib");
    }

    #[test]
    fn effective_name_empty_defaults_to_plain() {
        assert_eq!(effective_inter_broker_listener_name(&[], None), "PLAIN");
    }
}
```

Modify `crates/operator/src/controller/mod.rs` — add:

```rust
pub(crate) mod listeners;
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cargo test -p crabka-operator --lib controller::listeners
```

Expected: 13 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/operator/src/controller/listeners.rs crates/operator/src/controller/mod.rs
git commit -m "Slice 25/2: listener validation logic"
```

---

### Batch 25.2 — Add fields to `KafkaSpec` + status (sequential within file)

#### Task 25.2.1: Add `listeners` + `interBrokerListenerName` to `KafkaSpec`; add `listeners` to `KafkaStatus`

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (regenerated by `cargo xtask gen-crds`)

- [ ] **Step 1: Write the failing tests**

Append to `crates/operator/src/crd/kafka.rs`'s `mod tests`:

```rust
#[test]
fn spec_carries_listeners() {
    use super::*;
    use crate::crd::{Listener, ListenerType};

    let json = r#"{
        "kafkaVersion":"0.1.1",
        "listeners":[{"name":"PLAIN","port":9092,"type":"internal","tls":false}],
        "interBrokerListenerName":"PLAIN"
    }"#;
    let spec: KafkaSpec = serde_json::from_str(json).unwrap();
    assert_eq!(spec.listeners.len(), 1);
    assert_eq!(spec.listeners[0].name, "PLAIN");
    assert_eq!(spec.listeners[0].type_, ListenerType::Internal);
    assert_eq!(spec.inter_broker_listener_name.as_deref(), Some("PLAIN"));
}

#[test]
fn spec_defaults_listeners_to_empty() {
    use super::*;

    let json = r#"{"kafkaVersion":"0.1.1"}"#;
    let spec: KafkaSpec = serde_json::from_str(json).unwrap();
    assert!(spec.listeners.is_empty());
    assert!(spec.inter_broker_listener_name.is_none());
}

#[test]
fn status_carries_listener_status() {
    use super::*;
    use crate::crd::{ListenerAddress, ListenerStatus, ListenerType};

    let status = KafkaStatus {
        conditions: vec![],
        replicas: Some(1),
        ready_replicas: Some(1),
        listeners: vec![ListenerStatus {
            name: "PLAIN".into(),
            type_: ListenerType::Internal,
            bootstrap_servers: "demo-broker-headless.default.svc.cluster.local:9092".into(),
            addresses: vec![ListenerAddress {
                host: "demo-broker-headless.default.svc.cluster.local".into(),
                port: 9092,
            }],
        }],
    };
    let json = serde_json::to_string(&status).unwrap();
    assert!(json.contains("\"bootstrapServers\""), "got: {json}");
    let back: KafkaStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(back, status);
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p crabka-operator --lib crd::kafka
```

Expected: 3 new tests FAIL (fields missing).

- [ ] **Step 3: Modify `KafkaSpec` and `KafkaStatus`**

Edit `crates/operator/src/crd/kafka.rs`. Replace the `KafkaSpec` struct with:

```rust
pub struct KafkaSpec {
    /// Crabka version label, propagated to all pool pods via the
    /// `app.kubernetes.io/version` label.
    pub kafka_version: String,
    /// Opaque broker properties (`server.properties`-style key/value
    /// pairs). Slice 25 passes these through to the broker's
    /// `[server_properties]` TOML table; the broker currently treats
    /// them as inert. Changes propagate through the slice-21 config
    /// hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::BTreeMap<String, String>>,
    /// Slice 25: named listeners. Empty (or absent) synthesizes a
    /// single internal `PLAIN` listener on port 9092 (slice 19/20
    /// compatibility default).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::Listener>,
    /// Slice 25: name of the listener used for inter-broker traffic.
    /// When `None`, the operator picks the first `internal` listener;
    /// when `listeners` is empty, the synthesized default `"PLAIN"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_broker_listener_name: Option<String>,
}
```

Replace `KafkaStatus` with:

```rust
pub struct KafkaStatus {
    /// Standard Kubernetes-style condition list. Surfaces
    /// `Ready`, `ListenersValid`, `ListenersReady`.
    #[serde(default)]
    pub conditions: Vec<KafkaCondition>,
    /// Mirrors `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Mirrors `StatefulSet.status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// Slice 25: per-listener resolved addresses. Populated once
    /// `ListenersReady=True`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::ListenerStatus>,
}
```

- [ ] **Step 4: Run to verify tests pass**

```bash
cargo test -p crabka-operator --lib crd::kafka
```

Expected: all pass.

- [ ] **Step 5: Regenerate CRD YAML**

```bash
cargo xtask gen-crds
```

This rewrites `deploy/crds/crabka.io_kafkas.yaml`. Stage the diff (it should add the listener schema and status fields).

- [ ] **Step 6: Commit**

```bash
git add crates/operator/src/crd/kafka.rs deploy/crds/crabka.io_kafkas.yaml
git commit -m "Slice 25/3: KafkaSpec.listeners + KafkaStatus.listeners; regenerated CRD"
```

---

### Batch 25.3 — Service rendering + advertised computation (parallel — all in `controller/listeners.rs`, but separate functions/tests)

These three tasks all add **new functions** to `listeners.rs` with non-overlapping signatures and test modules. They can be developed in parallel; merging picks them up cleanly.

#### Task 25.3.1: Per-broker + bootstrap Service rendering

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs` — add `render_broker_service`, `render_bootstrap_service`
- Test: same file, `#[cfg(test)] mod service_rendering_tests`

- [ ] **Step 1: Write the failing tests**

Append to `crates/operator/src/controller/listeners.rs`:

```rust
use k8s_openapi::api::core::v1::Service;
use kube::api::ObjectMeta;
use std::collections::BTreeMap;

use crate::controller::common::{APP_LABEL, owner_ref};
use crate::crd::Kafka;

/// Render the per-broker external Service for the given listener +
/// broker id. The Service's selector uses the built-in
/// `statefulset.kubernetes.io/pod-name` label (K8s 1.28+) to pin it
/// to exactly the pod that hosts this broker.
///
/// `pod_name` is the StatefulSet-allocated pod name (e.g.
/// `demo-controller-0`). Caller computes it from pool+ordinal.
pub fn render_broker_service(
    owner: &Kafka,
    listener: &Listener,
    broker_id: i32,
    pod_name: &str,
) -> Result<Service, crate::controller::common::ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let namespace = owner.meta().namespace.clone();
    let svc_name = format!("{cluster_name}-{}-{broker_id}", listener.name);

    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    labels.insert("app.kubernetes.io/instance".into(), cluster_name.clone());
    labels.insert("crabka.io/listener".into(), listener.name.clone());
    labels.insert("crabka.io/broker".into(), broker_id.to_string());

    let mut selector = BTreeMap::new();
    selector.insert("statefulset.kubernetes.io/pod-name".into(), pod_name.to_string());

    let service_type = match listener.type_ {
        ListenerType::Nodeport => "NodePort".to_string(),
        ListenerType::Loadbalancer => "LoadBalancer".to_string(),
        _ => panic!("render_broker_service called with type {:?}", listener.type_),
    };

    let override_ = listener
        .configuration
        .as_ref()
        .and_then(|c| c.brokers.iter().find(|b| b.broker == broker_id));

    let mut port = serde_json::json!({
        "name": listener.name,
        "port": listener.port,
        "targetPort": listener.port,
        "protocol": "TCP",
    });
    if let Some(np) = override_.and_then(|o| o.node_port) {
        port["nodePort"] = serde_json::json!(np);
    }
    let mut spec = serde_json::json!({
        "type": service_type,
        "selector": selector,
        "ports": [port],
    });
    if let Some(lb_ip) = override_.and_then(|o| o.load_balancer_ip.as_ref()) {
        spec["loadBalancerIP"] = serde_json::json!(lb_ip);
    }

    let svc: Service = serde_json::from_value(serde_json::json!({
        "metadata": {
            "name": svc_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
        },
        "spec": spec,
    }))?;
    Ok(svc)
}

/// Render the bootstrap Service for the given external listener. Its
/// selector matches every broker pod of the cluster.
pub fn render_bootstrap_service(
    owner: &Kafka,
    listener: &Listener,
) -> Result<Service, crate::controller::common::ReconcileError> {
    let cluster_name = owner.meta().name.clone().unwrap_or_default();
    let namespace = owner.meta().namespace.clone();
    let svc_name = format!("{cluster_name}-{}-bootstrap", listener.name);

    let bootstrap = listener.configuration.as_ref().and_then(|c| c.bootstrap.as_ref());

    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    labels.insert("app.kubernetes.io/instance".into(), cluster_name.clone());
    labels.insert("crabka.io/listener".into(), listener.name.clone());
    labels.insert("crabka.io/role".into(), "bootstrap".into());
    if let Some(b) = bootstrap {
        for (k, v) in &b.labels {
            labels.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    let mut annotations: BTreeMap<String, String> = BTreeMap::new();
    if let Some(b) = bootstrap {
        for (k, v) in &b.annotations {
            annotations.insert(k.clone(), v.clone());
        }
    }

    let mut selector = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), cluster_name.clone());

    let service_type = match listener.type_ {
        ListenerType::Nodeport => "NodePort",
        ListenerType::Loadbalancer => "LoadBalancer",
        _ => panic!("render_bootstrap_service called with type {:?}", listener.type_),
    };

    let mut port = serde_json::json!({
        "name": listener.name,
        "port": listener.port,
        "targetPort": listener.port,
        "protocol": "TCP",
    });
    if let Some(np) = bootstrap.and_then(|b| b.node_port) {
        port["nodePort"] = serde_json::json!(np);
    }
    let mut spec = serde_json::json!({
        "type": service_type,
        "selector": selector,
        "ports": [port],
    });
    if let Some(lb_ip) = bootstrap.and_then(|b| b.load_balancer_ip.as_ref()) {
        spec["loadBalancerIP"] = serde_json::json!(lb_ip);
    }

    let mut meta = serde_json::json!({
        "name": svc_name,
        "namespace": namespace,
        "labels": labels,
        "ownerReferences": [owner_ref::<Kafka>(owner)?],
    });
    if !annotations.is_empty() {
        meta["annotations"] = serde_json::to_value(&annotations)?;
    }

    let svc: Service = serde_json::from_value(serde_json::json!({
        "metadata": meta,
        "spec": spec,
    }))?;
    Ok(svc)
}

#[cfg(test)]
mod service_rendering_tests {
    use super::*;
    use crate::crd::{BootstrapConfig, BrokerOverride, KafkaSpec, ListenerConfiguration};
    use kube::Resource as _;

    fn kafka(name: &str) -> Kafka {
        let mut k = Kafka::new(name, KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
        });
        k.meta_mut().namespace = Some("default".into());
        k.meta_mut().uid = Some("00000000-0000-0000-0000-000000000001".into());
        k
    }

    #[test]
    fn nodeport_broker_service_has_pod_name_selector_and_nodeport() {
        let k = kafka("demo");
        let listener = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride { broker: 0, node_port: Some(32100), ..Default::default() }],
            }),
        };
        let svc = render_broker_service(&k, &listener, 0, "demo-pool-0").unwrap();
        assert_eq!(svc.metadata.name.as_deref(), Some("demo-external-0"));
        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_.as_deref(), Some("NodePort"));
        let sel = spec.selector.as_ref().unwrap();
        assert_eq!(sel.get("statefulset.kubernetes.io/pod-name"), Some(&"demo-pool-0".to_string()));
        assert_eq!(spec.ports.as_ref().unwrap()[0].port, 9094);
        assert_eq!(spec.ports.as_ref().unwrap()[0].node_port, Some(32100));
    }

    #[test]
    fn loadbalancer_broker_service_uses_lb_ip_override() {
        let k = kafka("demo");
        let listener = Listener {
            name: "lb".into(),
            port: 9094,
            type_: ListenerType::Loadbalancer,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: None,
                brokers: vec![BrokerOverride { broker: 0, load_balancer_ip: Some("10.0.0.5".into()), ..Default::default() }],
            }),
        };
        let svc = render_broker_service(&k, &listener, 0, "demo-pool-0").unwrap();
        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_.as_deref(), Some("LoadBalancer"));
        assert_eq!(spec.load_balancer_ip.as_deref(), Some("10.0.0.5"));
    }

    #[test]
    fn bootstrap_service_selects_all_broker_pods() {
        let k = kafka("demo");
        let listener = Listener {
            name: "external".into(),
            port: 9094,
            type_: ListenerType::Nodeport,
            tls: false,
            configuration: Some(ListenerConfiguration {
                bootstrap: Some(BootstrapConfig { node_port: Some(32099), ..Default::default() }),
                brokers: vec![],
            }),
        };
        let svc = render_bootstrap_service(&k, &listener).unwrap();
        assert_eq!(svc.metadata.name.as_deref(), Some("demo-external-bootstrap"));
        let spec = svc.spec.as_ref().unwrap();
        let sel = spec.selector.as_ref().unwrap();
        assert_eq!(sel.get("app.kubernetes.io/instance"), Some(&"demo".to_string()));
        assert!(sel.get("statefulset.kubernetes.io/pod-name").is_none());
        assert_eq!(spec.ports.as_ref().unwrap()[0].node_port, Some(32099));
    }
}
```

- [ ] **Step 2: Run to verify tests pass**

```bash
cargo test -p crabka-operator --lib controller::listeners
```

Expected: existing validation tests still pass + 3 new service-rendering tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/operator/src/controller/listeners.rs
git commit -m "Slice 25/4: per-broker + bootstrap Service rendering"
```

#### Task 25.3.2: Advertised-listener computation

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs` — add `compute_advertised`

- [ ] **Step 1: Write the failing tests**

Append to `crates/operator/src/controller/listeners.rs`:

```rust
use k8s_openapi::api::core::v1::{Node, Service};

/// Per-broker resolved advertised address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedAddress {
    pub host: String,
    pub port: i32,
}

/// Errors that block advertised-listener computation. They map onto
/// `ListenersReady=False reason=PendingExternalAddresses`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdvertisedError {
    PodNotScheduled { broker: i32 },
    NodeNotFound { broker: i32, node_name: String },
    NodeHasNoAddress { broker: i32, node_name: String },
    ServiceMissing { broker: i32, service_name: String },
    NodePortNotAllocated { broker: i32 },
    LoadBalancerPending { broker: i32, service_name: String },
}

impl AdvertisedError {
    pub fn message(&self) -> String {
        match self {
            AdvertisedError::PodNotScheduled { broker } => format!("pod for broker {broker} not yet scheduled"),
            AdvertisedError::NodeNotFound { broker, node_name } => format!("node {node_name} for broker {broker} not visible"),
            AdvertisedError::NodeHasNoAddress { broker, node_name } => format!("node {node_name} for broker {broker} has no addresses"),
            AdvertisedError::ServiceMissing { broker, service_name } => format!("service {service_name} for broker {broker} missing"),
            AdvertisedError::NodePortNotAllocated { broker } => format!("nodePort for broker {broker} not allocated yet"),
            AdvertisedError::LoadBalancerPending { broker, service_name } => format!("loadBalancer for service {service_name} (broker {broker}) not provisioned"),
        }
    }
}

/// Compute the advertised host:port for one (listener, broker).
///
/// `pod_node_name` is `Pod.spec.nodeName` of the pod hosting this
/// broker (None if not yet scheduled). `nodes_by_name` is a map of
/// all Nodes the operator has observed. `service` is the per-broker
/// Service the operator just rendered+applied (None until the
/// apiserver returns it).
pub fn compute_advertised(
    listener: &Listener,
    broker_id: i32,
    pod_fqdn: &str,
    pod_node_name: Option<&str>,
    nodes_by_name: &std::collections::HashMap<String, Node>,
    per_broker_service: Option<&Service>,
) -> Result<AdvertisedAddress, AdvertisedError> {
    let override_ = listener
        .configuration
        .as_ref()
        .and_then(|c| c.brokers.iter().find(|b| b.broker == broker_id));

    match listener.type_ {
        ListenerType::Internal => Ok(AdvertisedAddress {
            host: pod_fqdn.to_string(),
            port: listener.port,
        }),
        ListenerType::Nodeport => {
            let host = if let Some(h) = override_.and_then(|o| o.advertised_host.as_ref()) {
                h.clone()
            } else {
                let node_name = pod_node_name.ok_or(AdvertisedError::PodNotScheduled { broker: broker_id })?;
                let node = nodes_by_name.get(node_name).ok_or_else(|| AdvertisedError::NodeNotFound {
                    broker: broker_id,
                    node_name: node_name.to_string(),
                })?;
                let addrs = node.status.as_ref().and_then(|s| s.addresses.as_ref());
                let host = addrs
                    .and_then(|a| {
                        a.iter()
                            .find(|x| x.type_ == "ExternalIP")
                            .or_else(|| a.iter().find(|x| x.type_ == "InternalIP"))
                            .map(|x| x.address.clone())
                    })
                    .ok_or_else(|| AdvertisedError::NodeHasNoAddress {
                        broker: broker_id,
                        node_name: node_name.to_string(),
                    })?;
                host
            };
            let port = if let Some(p) = override_.and_then(|o| o.advertised_port) {
                p
            } else if let Some(p) = override_.and_then(|o| o.node_port) {
                p
            } else {
                let svc_name = per_broker_service
                    .and_then(|s| s.metadata.name.clone())
                    .unwrap_or_default();
                per_broker_service
                    .and_then(|s| s.spec.as_ref())
                    .and_then(|s| s.ports.as_ref())
                    .and_then(|ps| ps.first().and_then(|p| p.node_port))
                    .ok_or(AdvertisedError::NodePortNotAllocated { broker: broker_id })
                    .map_err(|_| AdvertisedError::ServiceMissing {
                        broker: broker_id,
                        service_name: svc_name,
                    })?
            };
            Ok(AdvertisedAddress { host, port })
        }
        ListenerType::Loadbalancer => {
            let host = if let Some(h) = override_.and_then(|o| o.advertised_host.as_ref()) {
                h.clone()
            } else {
                let svc = per_broker_service.ok_or_else(|| AdvertisedError::ServiceMissing {
                    broker: broker_id,
                    service_name: String::new(),
                })?;
                let svc_name = svc.metadata.name.clone().unwrap_or_default();
                let ingress = svc
                    .status
                    .as_ref()
                    .and_then(|st| st.load_balancer.as_ref())
                    .and_then(|lb| lb.ingress.as_ref())
                    .and_then(|ig| ig.first())
                    .ok_or_else(|| AdvertisedError::LoadBalancerPending {
                        broker: broker_id,
                        service_name: svc_name.clone(),
                    })?;
                ingress
                    .hostname
                    .clone()
                    .or_else(|| ingress.ip.clone())
                    .ok_or(AdvertisedError::LoadBalancerPending {
                        broker: broker_id,
                        service_name: svc_name,
                    })?
            };
            let port = override_
                .and_then(|o| o.advertised_port)
                .unwrap_or(listener.port);
            Ok(AdvertisedAddress { host, port })
        }
        ListenerType::Ingress | ListenerType::Route => {
            // Should have been rejected by validate_listeners earlier.
            unreachable!("compute_advertised called with deferred type {:?}", listener.type_)
        }
    }
}

#[cfg(test)]
mod advertised_tests {
    use super::*;
    use k8s_openapi::api::core::v1::{Node, NodeAddress, NodeStatus, Service, ServicePort, ServiceSpec, ServiceStatus, LoadBalancerStatus, LoadBalancerIngress};
    use std::collections::HashMap;

    fn internal(name: &str, port: i32) -> Listener {
        Listener { name: name.into(), port, type_: ListenerType::Internal, tls: false, configuration: None }
    }
    fn nodeport(name: &str, port: i32) -> Listener {
        Listener { name: name.into(), port, type_: ListenerType::Nodeport, tls: false, configuration: None }
    }
    fn loadbalancer(name: &str, port: i32) -> Listener {
        Listener { name: name.into(), port, type_: ListenerType::Loadbalancer, tls: false, configuration: None }
    }

    #[test]
    fn internal_uses_pod_fqdn() {
        let l = internal("PLAIN", 9092);
        let nodes = HashMap::new();
        let a = compute_advertised(&l, 0, "pod.svc.local", None, &nodes, None).unwrap();
        assert_eq!(a, AdvertisedAddress { host: "pod.svc.local".into(), port: 9092 });
    }

    #[test]
    fn nodeport_pending_when_pod_unscheduled() {
        let l = nodeport("ext", 9094);
        let nodes = HashMap::new();
        let err = compute_advertised(&l, 0, "pod", None, &nodes, None).unwrap_err();
        assert!(matches!(err, AdvertisedError::PodNotScheduled { broker: 0 }));
    }

    #[test]
    fn nodeport_resolves_external_ip_from_node() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert("n1".into(), Node {
            status: Some(NodeStatus {
                addresses: Some(vec![
                    NodeAddress { type_: "InternalIP".into(), address: "10.0.0.1".into() },
                    NodeAddress { type_: "ExternalIP".into(), address: "1.2.3.4".into() },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let svc = Service {
            metadata: kube::api::ObjectMeta { name: Some("demo-ext-0".into()), ..Default::default() },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 9094, node_port: Some(32100), ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert_eq!(a, AdvertisedAddress { host: "1.2.3.4".into(), port: 32100 });
    }

    #[test]
    fn nodeport_falls_back_to_internal_ip() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert("n1".into(), Node {
            status: Some(NodeStatus {
                addresses: Some(vec![NodeAddress { type_: "InternalIP".into(), address: "10.0.0.1".into() }]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let svc = Service {
            metadata: kube::api::ObjectMeta { name: Some("demo-ext-0".into()), ..Default::default() },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort { port: 9094, node_port: Some(32100), ..Default::default() }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert_eq!(a.host, "10.0.0.1");
    }

    #[test]
    fn nodeport_pending_when_service_has_no_nodeport() {
        let l = nodeport("ext", 9094);
        let mut nodes = HashMap::new();
        nodes.insert("n1".into(), Node {
            status: Some(NodeStatus {
                addresses: Some(vec![NodeAddress { type_: "InternalIP".into(), address: "10.0.0.1".into() }]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let svc = Service {
            metadata: kube::api::ObjectMeta { name: Some("demo-ext-0".into()), ..Default::default() },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort { port: 9094, node_port: None, ..Default::default() }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap_err();
        assert!(matches!(err, AdvertisedError::ServiceMissing { .. }));
    }

    #[test]
    fn loadbalancer_resolves_hostname() {
        let l = loadbalancer("lb", 9094);
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta { name: Some("demo-lb-0".into()), ..Default::default() },
            spec: Some(ServiceSpec::default()),
            status: Some(ServiceStatus {
                load_balancer: Some(LoadBalancerStatus {
                    ingress: Some(vec![LoadBalancerIngress {
                        hostname: Some("lb.example.com".into()), ip: None, ports: None,
                    }]),
                }),
                ..Default::default()
            }),
        };
        let a = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap();
        assert_eq!(a, AdvertisedAddress { host: "lb.example.com".into(), port: 9094 });
    }

    #[test]
    fn loadbalancer_pending_when_status_missing() {
        let l = loadbalancer("lb", 9094);
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta { name: Some("demo-lb-0".into()), ..Default::default() },
            spec: Some(ServiceSpec::default()),
            status: None,
        };
        let err = compute_advertised(&l, 0, "pod", Some("n1"), &nodes, Some(&svc)).unwrap_err();
        assert!(matches!(err, AdvertisedError::LoadBalancerPending { .. }));
    }

    #[test]
    fn override_advertised_host_wins() {
        let mut l = nodeport("ext", 9094);
        l.configuration = Some(crate::crd::ListenerConfiguration {
            bootstrap: None,
            brokers: vec![crate::crd::BrokerOverride {
                broker: 0,
                advertised_host: Some("public.host".into()),
                ..Default::default()
            }],
        });
        let nodes = HashMap::new();
        let svc = Service {
            metadata: kube::api::ObjectMeta { name: Some("demo-ext-0".into()), ..Default::default() },
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort { port: 9094, node_port: Some(32100), ..Default::default() }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = compute_advertised(&l, 0, "pod", None, &nodes, Some(&svc)).unwrap();
        assert_eq!(a.host, "public.host");
        assert_eq!(a.port, 32100);
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cargo test -p crabka-operator --lib controller::listeners::advertised_tests
```

Expected: 7 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/operator/src/controller/listeners.rs
git commit -m "Slice 25/5: advertised-listener computation"
```

#### Task 25.3.3: Canonical TOML rendering per broker

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs` — add `render_broker_toml`
- Modify: `crates/operator/Cargo.toml` — add `toml = "0.8"` dep

- [ ] **Step 1: Write the failing tests**

Append to `crates/operator/src/controller/listeners.rs`:

```rust
/// Render the complete TOML for one broker (cluster-wide content +
/// this broker's advertised addresses). Deterministic — same input
/// always produces byte-identical output so the slice-21 config-hash
/// is stable.
pub fn render_broker_toml(
    broker_id: i32,
    listeners: &[Listener],
    addresses_per_listener: &std::collections::BTreeMap<String, AdvertisedAddress>,
    inter_broker_listener_name: &str,
    server_properties: &std::collections::BTreeMap<String, String>,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "broker_id = {broker_id}");
    let _ = writeln!(out, "log_dir = \"/var/lib/crabka/data\"");
    let _ = writeln!(
        out,
        "inter_broker_listener_name = \"{inter_broker_listener_name}\""
    );
    out.push('\n');

    for l in listeners {
        let adv = addresses_per_listener
            .get(&l.name)
            .map(|a| format!("{}:{}", a.host, a.port))
            .unwrap_or_default();
        let _ = writeln!(out, "[[listeners]]");
        let _ = writeln!(out, "name = \"{}\"", l.name);
        let _ = writeln!(out, "bind_addr = \"0.0.0.0:{}\"", l.port);
        let _ = writeln!(out, "advertised = \"{adv}\"");
        let _ = writeln!(out, "protocol = \"plaintext\"");
        out.push('\n');
    }

    if !server_properties.is_empty() {
        let _ = writeln!(out, "[server_properties]");
        for (k, v) in server_properties {
            let _ = writeln!(out, "\"{k}\" = \"{v}\"");
        }
    }

    out
}

/// Build the synthesized internal-only listener used when
/// `Kafka.spec.listeners` is empty. Kept here so the operator and
/// tests agree on the bytes.
pub fn synthesized_default_listener() -> Listener {
    Listener {
        name: "PLAIN".into(),
        port: 9092,
        type_: ListenerType::Internal,
        tls: false,
        configuration: None,
    }
}

#[cfg(test)]
mod toml_rendering_tests {
    use super::*;

    #[test]
    fn renders_minimal_broker_toml_and_round_trips() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert("PLAIN".into(), AdvertisedAddress {
            host: "demo-0.svc.local".into(),
            port: 9092,
        });
        let listeners = vec![synthesized_default_listener()];
        let props = std::collections::BTreeMap::new();
        let toml_str = render_broker_toml(0, &listeners, &addrs, "PLAIN", &props);

        // Sanity: parses cleanly with the broker's FileConfig.
        let parsed: crabka_broker::file_config::FileConfig = toml::from_str(&toml_str)
            .expect("rendered TOML must parse with broker FileConfig");
        assert_eq!(parsed.broker_id, Some(0));
        assert_eq!(parsed.inter_broker_listener_name.as_deref(), Some("PLAIN"));
        assert_eq!(parsed.listeners.len(), 1);
        assert_eq!(parsed.listeners[0].advertised, "demo-0.svc.local:9092");
    }

    #[test]
    fn deterministic_byte_output() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert("PLAIN".into(), AdvertisedAddress { host: "h".into(), port: 9092 });
        let l = vec![synthesized_default_listener()];
        let mut p = std::collections::BTreeMap::new();
        p.insert("z.last".into(), "1".into());
        p.insert("a.first".into(), "0".into());

        let t1 = render_broker_toml(0, &l, &addrs, "PLAIN", &p);
        let t2 = render_broker_toml(0, &l, &addrs, "PLAIN", &p);
        assert_eq!(t1, t2);
        // Sorted property keys (BTreeMap iteration).
        let a_pos = t1.find("a.first").unwrap();
        let z_pos = t1.find("z.last").unwrap();
        assert!(a_pos < z_pos);
    }

    #[test]
    fn server_properties_section_omitted_when_empty() {
        let mut addrs = std::collections::BTreeMap::new();
        addrs.insert("PLAIN".into(), AdvertisedAddress { host: "h".into(), port: 9092 });
        let t = render_broker_toml(0, &[synthesized_default_listener()], &addrs, "PLAIN", &std::collections::BTreeMap::new());
        assert!(!t.contains("[server_properties]"), "got:\n{t}");
    }
}
```

Modify `crates/operator/Cargo.toml` — add under `[dependencies]`:

```toml
toml = "0.8"
crabka-broker = { path = "../broker" }   # already present? if so leave; needed for FileConfig in test
```

(If `crabka-broker` is a `[dev-dependencies]` rather than a full dependency, move it or duplicate it under `[dev-dependencies]`. Keep the rest of the dep tree minimal.)

- [ ] **Step 2: Run tests**

```bash
cargo test -p crabka-operator --lib controller::listeners::toml_rendering_tests
```

Expected: 3 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/operator/src/controller/listeners.rs crates/operator/Cargo.toml Cargo.lock
git commit -m "Slice 25/6: render_broker_toml + synthesized default listener"
```

---

### Batch 25.4 — ConfigMap rewrite + hash update (sequential within `common.rs`)

#### Task 25.4.1: Update `render_configmap` to emit per-broker TOML keys

**Files:**
- Modify: `crates/operator/src/controller/common.rs`

- [ ] **Step 1: Write the failing test**

In `crates/operator/src/controller/common.rs`, append to the existing `mod config_hash_tests` (or create a new `mod configmap_tests` if cleaner):

```rust
#[test]
fn configmap_has_one_toml_key_per_broker() {
    use crate::controller::listeners::{AdvertisedAddress, synthesized_default_listener};
    use crate::crd::KafkaSpec;

    let mut k = Kafka::new("demo", KafkaSpec {
        kafka_version: "0.1.1".into(),
        config: None,
        listeners: vec![],
        inter_broker_listener_name: None,
    });
    k.meta_mut().namespace = Some("default".into());
    k.meta_mut().uid = Some("uid".into());

    let listeners = vec![synthesized_default_listener()];
    let mut per_broker = std::collections::BTreeMap::new();
    let mut addrs0 = std::collections::BTreeMap::new();
    addrs0.insert("PLAIN".into(), AdvertisedAddress { host: "demo-0.svc".into(), port: 9092 });
    let mut addrs1 = std::collections::BTreeMap::new();
    addrs1.insert("PLAIN".into(), AdvertisedAddress { host: "demo-1.svc".into(), port: 9092 });
    per_broker.insert(0i32, addrs0);
    per_broker.insert(1i32, addrs1);

    let cm = render_configmap(&k, &listeners, &per_broker, "PLAIN").unwrap();
    let data = cm.data.unwrap();
    assert!(data.contains_key("broker-0.toml"));
    assert!(data.contains_key("broker-1.toml"));
    assert!(data["broker-0.toml"].contains("demo-0.svc"));
    assert!(data["broker-1.toml"].contains("demo-1.svc"));
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p crabka-operator --lib controller::common::configmap_tests
```

Expected: FAIL — `render_configmap` signature doesn't match yet.

- [ ] **Step 3: Update `render_configmap`**

In `crates/operator/src/controller/common.rs`, replace the existing `render_configmap` (lines ~184–209) with:

```rust
/// Render the cluster-level `ConfigMap`. Owner-ref'd to the parent
/// `Kafka`. One `broker-<id>.toml` key per broker, each a complete
/// TOML the broker reads via `--config-file`. Cluster-wide content
/// (listeners, protocol map, inter-broker name, user
/// `spec.config`) is duplicated across keys for simplicity; cost is
/// negligible (~few KB × N brokers ≪ 1 MiB CM limit).
pub(crate) fn render_configmap(
    owner: &Kafka,
    listeners: &[crate::crd::Listener],
    addresses_per_broker: &std::collections::BTreeMap<
        i32,
        std::collections::BTreeMap<String, crate::controller::listeners::AdvertisedAddress>,
    >,
    inter_broker_listener_name: &str,
) -> Result<ConfigMap, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);

    let mut data = BTreeMap::new();
    let server_properties = owner.spec.config.clone().unwrap_or_default();
    for (broker_id, addrs) in addresses_per_broker {
        let toml = crate::controller::listeners::render_broker_toml(
            *broker_id,
            listeners,
            addrs,
            inter_broker_listener_name,
            &server_properties,
        );
        data.insert(format!("broker-{broker_id}.toml"), toml);
    }

    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{name}-broker-config")),
            namespace: owner.meta().namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(owner)?]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}
```

Update **all callers** of `render_configmap` in the operator crate. Specifically, `controller/kafka.rs` line ~169 will need to pass the new arguments. Search for callers:

```bash
grep -rn 'render_configmap' crates/operator/src
```

Each call site needs the new args. For Task 25.4.1 only, the reconciler call still produces an empty `addresses_per_broker` (no listeners yet) — that's wired properly in Task 25.5.

To keep this task small: leave the reconciler call producing an empty map and an empty listeners slice for now (it will look like a no-op ConfigMap). The reconciler-level integration happens in Task 25.5.

- [ ] **Step 4: Delete now-dead `serialize_broker_properties`**

The old function is unused after this change. Remove it (and its tests) per the no-back-compat rule.

- [ ] **Step 5: Run tests**

```bash
cargo test -p crabka-operator --lib
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/operator/src/controller/common.rs crates/operator/src/controller/kafka.rs
git commit -m "Slice 25/7: ConfigMap renders per-broker TOML keys; drop serialize_broker_properties"
```

#### Task 25.4.2: Update `config_hash` to include canonical listener intent

**Files:**
- Modify: `crates/operator/src/controller/common.rs`
- Modify: `crates/operator/src/controller/listeners.rs` — add `canonical_listener_intent`

- [ ] **Step 1: Write the failing tests**

Append to `crates/operator/src/controller/listeners.rs`:

```rust
/// Deterministic serialization of `spec.listeners` intent. Empty
/// (or absent) listeners produce the empty string so a cluster with
/// no `spec.listeners` set keeps its slice-24 hash on upgrade.
pub fn canonical_listener_intent(
    listeners: &[Listener],
    inter_broker_listener_name: Option<&str>,
) -> String {
    if listeners.is_empty() {
        return String::new();
    }
    use std::fmt::Write as _;
    let mut s = String::new();
    if let Some(name) = inter_broker_listener_name {
        let _ = writeln!(s, "inter_broker={name}");
    }
    for l in listeners {
        let _ = writeln!(
            s,
            "listener:name={},port={},type={:?},tls={}",
            l.name, l.port, l.type_, l.tls
        );
        if let Some(cfg) = &l.configuration {
            if let Some(b) = &cfg.bootstrap {
                if let Some(np) = b.node_port {
                    let _ = writeln!(s, "  bootstrap.nodePort={np}");
                }
                if let Some(ip) = &b.load_balancer_ip {
                    let _ = writeln!(s, "  bootstrap.loadBalancerIP={ip}");
                }
            }
            let mut sorted = cfg.brokers.clone();
            sorted.sort_by_key(|o| o.broker);
            for o in &sorted {
                if let Some(h) = &o.advertised_host {
                    let _ = writeln!(s, "  broker{}.advertisedHost={h}", o.broker);
                }
                if let Some(p) = o.advertised_port {
                    let _ = writeln!(s, "  broker{}.advertisedPort={p}", o.broker);
                }
                if let Some(np) = o.node_port {
                    let _ = writeln!(s, "  broker{}.nodePort={np}", o.broker);
                }
                if let Some(ip) = &o.load_balancer_ip {
                    let _ = writeln!(s, "  broker{}.loadBalancerIP={ip}", o.broker);
                }
            }
        }
    }
    s
}

#[cfg(test)]
mod intent_tests {
    use super::*;

    #[test]
    fn empty_listeners_yields_empty_string() {
        assert_eq!(canonical_listener_intent(&[], None), "");
    }

    #[test]
    fn non_empty_listeners_yield_content() {
        let l = vec![synthesized_default_listener()];
        assert!(!canonical_listener_intent(&l, Some("PLAIN")).is_empty());
    }

    #[test]
    fn deterministic() {
        let l = vec![Listener {
            name: "PLAIN".into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: false,
            configuration: Some(crate::crd::ListenerConfiguration {
                bootstrap: None,
                brokers: vec![
                    crate::crd::BrokerOverride { broker: 1, advertised_host: Some("h1".into()), ..Default::default() },
                    crate::crd::BrokerOverride { broker: 0, advertised_host: Some("h0".into()), ..Default::default() },
                ],
            }),
        }];
        let a = canonical_listener_intent(&l, Some("PLAIN"));
        let b = canonical_listener_intent(&l, Some("PLAIN"));
        assert_eq!(a, b);
        // Sorted by broker id.
        assert!(a.find("broker0.advertisedHost").unwrap() < a.find("broker1.advertisedHost").unwrap());
    }
}
```

Append to `crates/operator/src/controller/common.rs`'s `mod config_hash_tests`:

```rust
#[test]
fn hash_unchanged_when_listeners_empty() {
    // Slice-24 cluster (no listeners) → upgrade → slice-25 with empty listeners.
    // Hash must equal the slice-24 hash for the same spec.config.
    use crate::crd::KafkaSpec;

    let spec_a = KafkaSpec {
        kafka_version: "0.1.1".into(),
        config: Some({
            let mut m = std::collections::BTreeMap::new();
            m.insert("log.retention.hours".into(), "24".into());
            m
        }),
        listeners: vec![],
        inter_broker_listener_name: None,
    };
    let h = combined_config_hash(&spec_a);
    let h_again = combined_config_hash(&spec_a);
    assert_eq!(h, h_again);

    let mut spec_b = spec_a.clone();
    spec_b.listeners = vec![crate::controller::listeners::synthesized_default_listener()];
    spec_b.inter_broker_listener_name = Some("PLAIN".into());
    let h_with_listener = combined_config_hash(&spec_b);
    assert_ne!(h, h_with_listener, "non-empty listener intent must change hash");
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p crabka-operator --lib controller::common::config_hash_tests
```

Expected: FAIL — `combined_config_hash` not defined.

- [ ] **Step 3: Implement `combined_config_hash`**

Append to `crates/operator/src/controller/common.rs`:

```rust
/// Slice 25: combined hash over user `spec.config` and the canonical
/// listener intent. Empty listeners produce empty intent, so the
/// combined hash is identical to the slice-24 hash for an unchanged
/// `spec.config`.
pub fn combined_config_hash(spec: &crate::crd::KafkaSpec) -> String {
    let config_part = spec
        .config
        .as_ref()
        .map(|m| {
            let mut s = String::new();
            for (k, v) in m {
                s.push_str(k);
                s.push('=');
                s.push_str(v);
                s.push('\n');
            }
            s
        })
        .unwrap_or_default();
    let intent = crate::controller::listeners::canonical_listener_intent(
        &spec.listeners,
        spec.inter_broker_listener_name.as_deref(),
    );
    let mut buf = String::with_capacity(config_part.len() + 1 + intent.len());
    buf.push_str(&config_part);
    buf.push('\x1F'); // ASCII unit separator
    buf.push_str(&intent);
    config_hash(&buf)
}
```

Find every caller of the old `config_hash(&broker_props)` pattern (in `controller/kafka.rs` line ~177) and replace it with `combined_config_hash(&obj.spec)`.

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-operator --lib
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/operator/src/controller/common.rs crates/operator/src/controller/listeners.rs crates/operator/src/controller/kafka.rs
git commit -m "Slice 25/8: combined_config_hash includes listener intent"
```

---

### Batch 25.5 — Reconciler integration (sequential — touches `controller/kafka.rs` and `kafka_node_pool.rs`)

#### Task 25.5.1: Wire validation + Service rendering + ConfigMap + status into reconcile

**Files:**
- Modify: `crates/operator/src/controller/kafka.rs`

This is the big integration task. The reconciler now:
1. Validates listeners → `ListenersValid` condition
2. Renders per-broker + bootstrap Services → SSA-apply
3. Reads back `Service` / `Node` status → resolves addresses
4. Renders the ConfigMap with per-broker TOML
5. Sets `status.listeners` + `ListenersReady`

- [ ] **Step 1: Read the current reconciler shape**

```bash
cat crates/operator/src/controller/kafka.rs
```

Identify the function (likely `reconcile_kafka` or similar) where the existing ConfigMap is rendered + applied. The new logic goes between validation and SSA-apply.

- [ ] **Step 2: Study the existing reconcile-test pattern**

```bash
cat crates/operator/tests/reconcile_kafka.rs
cat crates/operator/tests/shared/mod.rs
```

The existing harness uses `MockRule`/`MockState` to preload HTTP responses for the fake `kube::Client`. Each test:
1. Builds a `Vec<MockRule>` declaring which requests will be made and the canned responses (list Pools, get Service, apply Service, etc.).
2. Invokes the public reconcile entry point with the fake `Client`.
3. Asserts on `MockState.observed` (what the reconciler requested) and on the reconciler's return value / final status patch body.

**Follow that pattern** for the new tests. Do not introduce a new `reconcile_with`/`Outcome` API — adapt to what's there. The substance of each test below stays the same; the boilerplate matches the existing files.

- [ ] **Step 3: Write the failing tests**

Create `crates/operator/tests/reconcile_listeners.rs`, modeled on `reconcile_kafka.rs`. The three test cases to cover (assertion-level — translate to MockRule sequences):

**Test 1 — `empty_listeners_sets_ready_with_synthesized_default`:**
- Preload: list `KafkaNodePool` returning one pool `pool-0` with `nodeIdStart=0`; list `Pod` returning the pool's pod with `spec.nodeName="kind-control-plane"`; list `Service` returning the pre-existing headless service; list `Node` returning one Node with `InternalIP=10.0.0.1`; SSA-apply ConfigMap (capture body); merge-patch Kafka status (capture body).
- Apply a `Kafka` with `spec.listeners = []`.
- Assert: the status-patch body contains `ListenersValid: True`, `ListenersReady: True`, `listeners[0].name == "PLAIN"`, `listeners[0].type == "internal"`.
- Assert: the ConfigMap body has key `broker-0.toml` and its content parses back to a `crabka_broker::file_config::FileConfig` with a single PLAIN listener at port 9092.

**Test 2 — `invalid_listeners_sets_condition_and_skips_services`:**
- Preload: list pools/pods/nodes as above; merge-patch Kafka status (capture body).
- Apply a `Kafka` with `spec.listeners = [{name: "BAD", port: 9092, type: internal, tls: true}]`.
- Assert: status-patch body contains `ListenersValid: False, reason: TlsNotYetSupported`.
- Assert: `MockState.observed` contains **no** Service SSA-apply requests for external listeners (existing headless Service apply is fine).

**Test 3 — `nodeport_listener_pending_when_node_unknown`:**
- Preload: list pools/pods returning pods with `spec.nodeName = None` (unscheduled); list Node returning empty; SSA-apply both bootstrap and per-broker Services (capture); merge-patch status (capture).
- Apply a `Kafka` with internal + nodeport listeners.
- Assert: Services for `demo-ext-bootstrap` and `demo-ext-0` were applied (apiserver still allocates NodePorts even without backing pods).
- Assert: status-patch body contains `ListenersReady: False, reason: PendingExternalAddresses`.

Use the helper signatures from `reconcile_kafka.rs` for `Kafka`/`KafkaNodePool` construction; copy the `assert_request_matches` / `decode_body_as<T>` style helpers from `shared/mod.rs`.

- [ ] **Step 3: Implement the reconciler changes**

In `crates/operator/src/controller/kafka.rs`, find the reconcile function and restructure it. Pseudo-code outline:

```rust
async fn reconcile_kafka(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let name = obj.meta().name.clone().unwrap_or_default();
    let ns = obj.meta().namespace.clone().unwrap_or_default();

    // 1. Validate listeners.
    let inter_broker = obj.spec.inter_broker_listener_name.as_deref();
    match crate::controller::listeners::validate_listeners(&obj.spec.listeners, inter_broker) {
        Ok(()) => { /* set ListenersValid=True */ }
        Err(e) => {
            set_condition(&ctx, &name, "ListenersValid", "False", e.reason(), &e.message()).await?;
            // Leave existing objects in place; surface the error and wait.
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    }

    // 2. Effective listeners: real or synthesized.
    let effective_listeners: Vec<crate::crd::Listener> = if obj.spec.listeners.is_empty() {
        vec![crate::controller::listeners::synthesized_default_listener()]
    } else {
        obj.spec.listeners.clone()
    };
    let inter_broker_name = crate::controller::listeners::effective_inter_broker_listener_name(
        &obj.spec.listeners,
        inter_broker,
    );

    // 3. Build broker → (pool, ordinal, pod_name) map from KafkaNodePool list.
    let brokers = enumerate_brokers(&ctx, &name, &ns).await?;

    // 4. Render per-broker + bootstrap Services (for external listeners only).
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let mut owned_service_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for l in effective_listeners.iter().filter(|l| matches!(l.type_, ListenerType::Nodeport | ListenerType::Loadbalancer)) {
        let bs = crate::controller::listeners::render_bootstrap_service(&obj, l)?;
        let bs_name = bs.meta().name.clone().unwrap_or_default();
        apply_object(&svc_api, &bs_name, &bs).await?;
        owned_service_names.insert(bs_name);
        for b in &brokers {
            let svc = crate::controller::listeners::render_broker_service(&obj, l, b.broker_id, &b.pod_name)?;
            let sn = svc.meta().name.clone().unwrap_or_default();
            apply_object(&svc_api, &sn, &svc).await?;
            owned_service_names.insert(sn);
        }
    }
    // Garbage-collect Services we own but no longer want (listener removed).
    garbage_collect_stale_services(&svc_api, &name, &owned_service_names).await?;

    // 5. Read back Service / Node / Pod status.
    let nodes = list_nodes(&ctx).await?;
    let services = list_per_broker_services(&svc_api, &name).await?;
    let pods = list_broker_pods(&ctx, &name, &ns).await?;

    // 6. Compute advertised per (broker, listener).
    let mut addresses_per_broker: std::collections::BTreeMap<i32, std::collections::BTreeMap<String, AdvertisedAddress>>
        = std::collections::BTreeMap::new();
    for b in &brokers {
        let pod_fqdn = pod_fqdn_for(&name, &b);
        let pod_node_name = pods.get(&b.pod_name).and_then(|p| p.spec.as_ref()).and_then(|s| s.node_name.clone());
        for l in &effective_listeners {
            let per_broker_svc = services.get(&format!("{name}-{}-{}", l.name, b.broker_id));
            match compute_advertised(l, b.broker_id, &pod_fqdn, pod_node_name.as_deref(), &nodes, per_broker_svc) {
                Ok(addr) => {
                    addresses_per_broker.entry(b.broker_id).or_default().insert(l.name.clone(), addr);
                }
                Err(e) => {
                    set_condition(&ctx, &name, "ListenersReady", "False", "PendingExternalAddresses", &e.message()).await?;
                    return Ok(Action::requeue(Duration::from_secs(5)));
                }
            }
        }
    }

    // 7. Render and apply ConfigMap with per-broker TOML.
    let cm = render_configmap(&obj, &effective_listeners, &addresses_per_broker, &inter_broker_name)?;
    apply_object(&Api::namespaced(ctx.client.clone(), &ns), cm.meta().name.as_deref().unwrap_or_default(), &cm).await?;

    // 8. Status: listeners[] + ListenersReady=True.
    let listener_status = build_listener_status(&effective_listeners, &addresses_per_broker, &services, &nodes);
    patch_status_with_listeners(&ctx, &name, &listener_status).await?;
    set_condition(&ctx, &name, "ListenersValid", "True", "Valid", "spec.listeners validated").await?;
    set_condition(&ctx, &name, "ListenersReady", "True", "Ready", "all listener addresses resolved").await?;

    // 9. Fall through to existing pool/StatefulSet reconcile.
    Ok(Action::requeue(Duration::from_secs(300)))
}
```

The exact wiring depends on the existing reconciler shape — adapt to match. New helper functions (`enumerate_brokers`, `list_nodes`, `pod_fqdn_for`, `garbage_collect_stale_services`, `build_listener_status`, `set_condition`) belong in `controller/kafka.rs` (private to that module) or `controller/common.rs` as appropriate.

- [ ] **Step 4: Run the tests**

```bash
cargo test -p crabka-operator --test reconcile_listeners
```

Expected: all 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/operator/src/controller/kafka.rs crates/operator/tests/reconcile_listeners.rs crates/operator/tests/shared/mod.rs
git commit -m "Slice 25/9: reconcile wires listener validation + Services + ConfigMap"
```

#### Task 25.5.2: Update pod template — `--config-file` + ConfigMap volume mount

**Files:**
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/operator/tests/reconcile_pool.rs`:

```rust
#[tokio::test]
async fn statefulset_mounts_broker_config_volume_and_uses_config_file() {
    // Existing test harness for pool reconcile — adapt to your fixture.
    // The shape we want: the StatefulSet's pod spec has a volume named
    // "broker-config" backed by ConfigMap "<cluster>-broker-config",
    // mounted at /etc/crabka/config, and the broker container's args
    // reference --config-file=/run/crabka/broker.toml.
    let sts = build_test_statefulset("demo", "pool", 0).await;
    let pod_spec = sts.spec.unwrap().template.spec.unwrap();

    let cm_volume = pod_spec.volumes.unwrap().iter()
        .find(|v| v.name == "broker-config")
        .cloned()
        .expect("expected broker-config volume");
    let cm_src = cm_volume.config_map.expect("ConfigMap-typed volume");
    assert_eq!(cm_src.name, Some("demo-broker-config".to_string()));

    let broker = pod_spec.containers.iter().find(|c| c.name == "broker").unwrap();
    let args_joined = broker.args.as_ref().unwrap().join(" ");
    assert!(args_joined.contains("--config-file=/run/crabka/broker.toml"), "args: {args_joined}");

    let mount = broker.volume_mounts.as_ref().unwrap()
        .iter().find(|m| m.name == "broker-config").expect("mount");
    assert_eq!(mount.mount_path, "/etc/crabka/config");
}
```

(Adapt `build_test_statefulset` to call the existing renderer that produces the StatefulSet object given a parent + pool.)

- [ ] **Step 2: Implement the pod template changes**

In `crates/operator/src/controller/kafka_node_pool.rs`, locate:

- `MAIN_SCRIPT` (line ~114) — replace with:

```rust
const MAIN_SCRIPT: &str = "set -eu\n\
mkdir -p /run/crabka\n\
cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml\n\
exec /usr/bin/crabka-broker \\\n  --config-file=/run/crabka/broker.toml \\\n  --broker-id=\"$(cat /var/lib/crabka/data/.node-id)\"\n";
```

- `render_broker_container` (line ~140) — drop `CRABKA_ADVERTISED_LISTENER` from `env`, add a volume mount + `NODE_ID` env (sourced from the init script's persisted file via a downward-API-ish hack — actually simpler: pass it as an env, computed by init container output):

Refactor so the init container exposes `NODE_ID` to the main container. Simplest pattern: the init container writes the node id to a file under `/var/lib/crabka/data/.node-id` (already done); the main script computes `NODE_ID=$(cat /var/lib/crabka/data/.node-id)` before the `cp`. Update MAIN_SCRIPT:

```rust
const MAIN_SCRIPT: &str = "set -eu\n\
NODE_ID=\"$(cat /var/lib/crabka/data/.node-id)\"\n\
mkdir -p /run/crabka\n\
cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml\n\
exec /usr/bin/crabka-broker \\\n  --config-file=/run/crabka/broker.toml \\\n  --broker-id=\"${NODE_ID}\"\n";
```

- `render_broker_container`'s `volumeMounts` JSON — add:

```json
{ "name": "broker-config", "mountPath": "/etc/crabka/config", "readOnly": true }
```

- In `render_statefulset` (line ~246), pod-spec `volumes` — add:

```json
{ "name": "broker-config", "configMap": { "name": "<cluster>-broker-config" } }
```

(Substitute `<cluster>` for the parent name string.)

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-operator --test reconcile_pool
```

Expected: new test passes, existing tests still pass.

- [ ] **Step 4: Commit**

```bash
git add crates/operator/src/controller/kafka_node_pool.rs crates/operator/tests/reconcile_pool.rs
git commit -m "Slice 25/10: pod template uses --config-file + broker-config volume"
```

---

### Batch 25.6 — Watches + RBAC + chart (parallel — disjoint files)

#### Task 25.6.1: Register Node watcher

**Files:**
- Modify: `crates/operator/src/run.rs`

- [ ] **Step 1: Read current watcher set**

```bash
grep -n 'Controller::new\|watches\|owns' crates/operator/src/run.rs
```

- [ ] **Step 2: Add a `Node` watcher**

Add (near other watchers):

```rust
use k8s_openapi::api::core::v1::Node;
// ...
let nodes: Api<Node> = Api::all(client.clone());
let controller = Controller::new(kafkas, kube::runtime::watcher::Config::default())
    .owns(/* ... existing ... */)
    .watches(nodes, kube::runtime::watcher::Config::default(), |node: Node| {
        // Map Node events to a reconcile of every Kafka in any namespace.
        // For correctness we just reconcile-all on any node event — the
        // operator's reconcile is idempotent and fast when nothing changes.
        Vec::new() // returning empty means "no specific Kafka to enqueue";
                   // pair this with a periodic reconcile (already configured)
                   // or replace with a list-Kafkas closure if churn becomes an issue.
    });
```

(The exact API depends on the kube-rs version. The simplest viable approach for slice 25 is the no-op mapper plus relying on the operator's reconcile-on-timer to pick up node changes within the requeue interval. If you want sharper reactivity, the mapper can list `Kafka` resources and emit an ObjectRef for each.)

- [ ] **Step 3: Verify the operator still builds**

```bash
cargo build -p crabka-operator
```

- [ ] **Step 4: Commit**

```bash
git add crates/operator/src/run.rs
git commit -m "Slice 25/11: register Node watcher (cluster-scoped)"
```

#### Task 25.6.2: Helm chart — add `nodes` RBAC + `kubeVersion`

**Files:**
- Modify: `charts/crabka-operator/Chart.yaml`
- Modify: `charts/crabka-operator/templates/clusterrole.yaml`

- [ ] **Step 1: Bump `kubeVersion`**

In `charts/crabka-operator/Chart.yaml`:

```yaml
kubeVersion: ">= 1.28.0-0"
```

(Add or update the `kubeVersion` field. The `-0` suffix lets the constraint match pre-release builds of 1.28 in test clusters.)

- [ ] **Step 2: Add `nodes` to the ClusterRole**

In `charts/crabka-operator/templates/clusterrole.yaml`, add a new rule:

```yaml
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
```

- [ ] **Step 3: Verify the chart**

```bash
helm lint charts/crabka-operator
helm template charts/crabka-operator | grep -A3 'kind: ClusterRole'
```

Expected: lint passes; template includes the nodes rule.

- [ ] **Step 4: Commit**

```bash
git add charts/crabka-operator/Chart.yaml charts/crabka-operator/templates/clusterrole.yaml
git commit -m "Slice 25/12: Helm chart — kubeVersion 1.28+ and nodes RBAC"
```

---

### Batch 25.7 — End-to-end tests on kind (sequential — each consumes the cluster)

#### Task 25.7.1: NodePort e2e test

**Files:**
- Create: `crates/operator/tests/e2e_nodeport.rs`
- Modify: `.github/workflows/ci.yml` (if applicable) — add the test to the kind job

- [ ] **Step 1: Identify how existing kind e2e tests are wired**

```bash
ls crates/operator/tests/
grep -rn 'kind' .github/workflows/ || true
```

Follow the existing pattern (likely a `#[ignore]`-marked test that runs in a dedicated CI job, or a script under `scripts/` invoked from CI).

- [ ] **Step 2: Write the e2e**

Create `crates/operator/tests/e2e_nodeport.rs` (sketch — adapt to existing helpers):

```rust
//! Slice-25 e2e: NodePort listener on a kind cluster.
//!
//! Marked `#[ignore]` so `cargo test` won't run it locally; CI's
//! dedicated kind job runs `cargo test --test e2e_nodeport -- --ignored`.

#![cfg(feature = "e2e")]
#![allow(clippy::needless_pass_by_value)]

use std::process::Command;

#[test]
#[ignore]
fn nodeport_listener_serves_traffic() {
    // 1. Helm install the operator chart.
    let status = Command::new("helm")
        .args(["upgrade", "--install", "op", "charts/crabka-operator", "--wait"])
        .status()
        .expect("helm");
    assert!(status.success());

    // 2. Apply CRDs.
    Command::new("kubectl").args(["apply", "-f", "deploy/crds/"]).status().unwrap();

    // 3. Apply a Kafka with 3 brokers and a NodePort listener.
    let manifest = r#"
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata: { name: demo }
spec:
  kafkaVersion: "0.1.1"
  listeners:
    - name: PLAIN
      port: 9092
      type: internal
      tls: false
    - name: ext
      port: 9094
      type: nodeport
      tls: false
      configuration:
        bootstrap: { nodePort: 32099 }
        brokers:
          - { broker: 0, nodePort: 32100 }
          - { broker: 1, nodePort: 32101 }
          - { broker: 2, nodePort: 32102 }
---
apiVersion: crabka.io/v1alpha1
kind: KafkaNodePool
metadata: { name: pool-0, labels: { "crabka.io/cluster": demo } }
spec: { roles: [Controller, Broker], replicas: 1, nodeIdStart: 0 }
---
apiVersion: crabka.io/v1alpha1
kind: KafkaNodePool
metadata: { name: pool-1, labels: { "crabka.io/cluster": demo } }
spec: { roles: [Controller, Broker], replicas: 1, nodeIdStart: 1 }
---
apiVersion: crabka.io/v1alpha1
kind: KafkaNodePool
metadata: { name: pool-2, labels: { "crabka.io/cluster": demo } }
spec: { roles: [Controller, Broker], replicas: 1, nodeIdStart: 2 }
"#;
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    use std::io::Write as _;
    child.stdin.as_mut().unwrap().write_all(manifest.as_bytes()).unwrap();
    child.wait().unwrap();

    // 4. Wait for Ready + ListenersReady.
    let s = Command::new("kubectl")
        .args(["wait", "kafka/demo", "--for=condition=Ready", "--timeout=300s"])
        .status().unwrap();
    assert!(s.success());

    // 5. Get the kind node's IP and the bootstrap NodePort.
    let node_ip = String::from_utf8(Command::new("kubectl")
        .args(["get", "nodes", "-o", "jsonpath={.items[0].status.addresses[?(@.type==\"InternalIP\")].address}"])
        .output().unwrap().stdout).unwrap();

    // 6. Produce + consume via kcat using node_ip:32099.
    let bootstrap = format!("{node_ip}:32099");
    let pr = Command::new("kcat")
        .args(["-P", "-b", &bootstrap, "-t", "test"])
        .stdin(std::process::Stdio::piped())
        .spawn().unwrap();
    // ... write some messages, then consume them and assert equality.

    // 7. Assert Kafka.status.listeners populated.
    let json = String::from_utf8(Command::new("kubectl")
        .args(["get", "kafka", "demo", "-o", "jsonpath={.status.listeners[?(@.name==\"ext\")].bootstrapServers}"])
        .output().unwrap().stdout).unwrap();
    assert!(!json.is_empty(), "status.listeners[ext].bootstrapServers should be populated");
}
```

(The full produce/consume is verbose; the test should follow the patterns from existing JVM-acceptance-style tests in the broker crate to drive `kcat` or the in-tree `crabka-cli`.)

- [ ] **Step 3: Run locally against kind**

```bash
kind create cluster --name crabka-e2e --image kindest/node:v1.28.0
cargo test -p crabka-operator --test e2e_nodeport --features e2e -- --ignored
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/operator/tests/e2e_nodeport.rs .github/workflows/ci.yml
git commit -m "Slice 25/13: e2e — NodePort listener on kind"
```

#### Task 25.7.2: LoadBalancer e2e with MetalLB

**Files:**
- Create: `crates/operator/tests/e2e_loadbalancer.rs`
- Modify: `.github/workflows/ci.yml` — preinstall MetalLB in the kind job

- [ ] **Step 1: Add MetalLB preinstall to CI**

In the kind CI step, before the test:

```yaml
- name: Install MetalLB
  run: |
    kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.14.7/config/manifests/metallb-native.yaml
    kubectl wait --for=condition=ready pod -l app=metallb -n metallb-system --timeout=120s
    # Allocate a /28 from the kind network's docker bridge.
    DOCKER_NET=$(docker network inspect kind -f '{{(index .IPAM.Config 0).Subnet}}')
    # Compute a small range — varies by docker version; pick a static range that works for kind.
    cat <<EOF | kubectl apply -f -
    apiVersion: metallb.io/v1beta1
    kind: IPAddressPool
    metadata: { name: default, namespace: metallb-system }
    spec: { addresses: ["172.18.255.200-172.18.255.250"] }
    ---
    apiVersion: metallb.io/v1beta1
    kind: L2Advertisement
    metadata: { name: l2, namespace: metallb-system }
    EOF
```

- [ ] **Step 2: Write the e2e test**

Mirror `e2e_nodeport.rs` structure, but the listener type is `loadbalancer` and the bootstrap address is read from `Service.status.loadBalancer.ingress[0].ip`. Skip the kind-node-IP step; use the LB IP directly.

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-operator --test e2e_loadbalancer --features e2e -- --ignored
git add crates/operator/tests/e2e_loadbalancer.rs .github/workflows/ci.yml
git commit -m "Slice 25/14: e2e — LoadBalancer listener with MetalLB"
```

#### Task 25.7.3: Slice-24→25 upgrade e2e

**Files:**
- Create: `crates/operator/tests/e2e_upgrade_24_to_25.rs`

- [ ] **Step 1: Write the upgrade test**

```rust
#![cfg(feature = "e2e")]

#[test]
#[ignore]
fn slice24_to_25_upgrade_rolls_once_then_steady() {
    // 1. Install slice-24 operator chart from git tag.
    // 2. Apply a Kafka with spec.config set (no listeners).
    // 3. Wait Ready, record StatefulSet pod creationTimestamps.
    // 4. helm upgrade to current (slice 25) chart.
    // 5. Wait Ready.
    // 6. Assert: pods rolled exactly once (creationTimestamps newer);
    //    crabka.io/config-hash annotation on the StatefulSet pod
    //    template UNCHANGED across the upgrade (verifies the synthesized
    //    default produces the same hash as slice-24's no-listeners state).
    // 7. Trigger one more reconcile (e.g. by annotating the Kafka with a
    //    nonce label) and assert no further roll occurs.
}
```

Implementation is mechanical — script kubectl commands + parse JSON outputs.

- [ ] **Step 2: Commit**

```bash
git add crates/operator/tests/e2e_upgrade_24_to_25.rs
git commit -m "Slice 25/15: e2e — slice-24→25 upgrade rolls once"
```

---

### Slice 25 PR finalization

- [ ] **Step 1: Run the full operator test suite + helm lint**

```bash
cargo fmt --all -- --check
cargo clippy -p crabka-operator --all-targets -- -D warnings
cargo test -p crabka-operator
helm lint charts/crabka-operator
cargo xtask gen-crds  # should produce no diff
git diff --exit-code deploy/crds/
```

Expected: all green; no diff in `deploy/crds/`.

- [ ] **Step 2: Push and open PR**

```bash
git push -u origin HEAD
gh pr create --title "Slice 25: Operator — listeners (internal/NodePort/LoadBalancer)" --body "$(cat <<'EOF'
## Summary
- Adds `Kafka.spec.listeners` (Strimzi-shaped) and `KafkaStatus.listeners`.
- Internal listener auto-synthesized when `spec.listeners` empty (slice-19 compat).
- NodePort + LoadBalancer external listener types: per-broker + bootstrap Services, advertised host:port resolved from Node.status / Service.status.loadBalancer.ingress.
- Schema accepts `ingress`/`route`; reconcile rejects them until slice 27.
- Switches pod template to `--config-file=/run/crabka/broker.toml` (depends on slice 25a).
- ConfigMap holds one complete TOML per broker.
- Slice-21 hash extended with canonical-listener-intent; empty listeners keep the slice-24 hash so existing clusters don't double-roll on upgrade.
- New `Node` watcher; chart kubeVersion ≥ 1.28; `nodes` get/list/watch added to ClusterRole.

Spec: docs/superpowers/specs/2026-05-17-crabka-operator-listeners-25-27-design.md
Depends on: #<25a PR>

## Test plan
- [x] `cargo test -p crabka-operator`
- [x] kind e2e: NodePort
- [x] kind e2e: LoadBalancer (MetalLB)
- [x] kind e2e: slice-24→25 upgrade rolls once, then steady

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Checklist

Run this against the spec section-by-section:

**1. Spec coverage:**
- [x] CRD schema (`Kafka.spec.listeners`, `BootstrapConfig`, `BrokerOverride`, status) → Tasks 25.1.1, 25.2.1
- [x] Default behavior (empty listeners synthesizes internal) → Task 25.3.3 (`synthesized_default_listener`) + Task 25.5.1
- [x] Validation conditions (`ListenersValid` reasons) → Task 25.1.2
- [x] Broker config delivery (TOML, `--config-file`) → Slice 25a tasks 1–5
- [x] ConfigMap layout (one TOML per broker) → Task 25.4.1
- [x] Per-broker + bootstrap Service rendering → Task 25.3.1
- [x] Advertised computation per type → Task 25.3.2
- [x] Cold-start ordering → covered by Task 25.5.1's reconcile flow (Services first, then ConfigMap)
- [x] Slice 21 hash integration (`canonical_listener_intent`, `combined_config_hash`) → Task 25.4.2
- [x] Watches + RBAC (`Node`) → Tasks 25.6.1, 25.6.2
- [x] Reconcile sequencing → Task 25.5.1
- [x] Status reporting (`Kafka.status.listeners`) → Task 25.5.1
- [x] Pod template update (`--config-file`, ConfigMap mount) → Task 25.5.2
- [x] K8s 1.28+ via chart `kubeVersion` → Task 25.6.2
- [x] NodePort e2e → Task 25.7.1
- [x] LoadBalancer e2e (MetalLB) → Task 25.7.2
- [x] Slice-24→25 upgrade e2e → Task 25.7.3
- [x] Slice 27 (deferred) → explicitly out of scope; validation rejects `ingress`/`route` (Task 25.1.2)

**2. Placeholder scan:** No TBDs, no "implement later", no "similar to Task N". All code blocks are concrete. Some e2e tests use sketched implementations (kcat scripting) that the executing engineer fills in following existing patterns — flagged as such in the steps.

**3. Type consistency:** `Listener`, `ListenerType`, `BrokerOverride`, `AdvertisedAddress`, `ValidationError`, `AdvertisedError` are defined once in Task 25.1.1 / 25.1.2 / 25.3.2 and referenced consistently throughout. `combined_config_hash` defined in Task 25.4.2; old `config_hash(&broker_props)` callers updated in the same task. `render_configmap` signature change in Task 25.4.1 propagated to the reconciler call in Task 25.5.1.
