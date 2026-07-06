# Dioxus Broker Admin UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Dioxus fullstack browser UI for administering one configured Crabka cluster with broker-backed SCRAM-SHA-512 login.

**Architecture:** Add a new `crabka-admin-ui` workspace crate under `crates/admin-ui`. The binary hosts a Dioxus fullstack app, stores authenticated sessions server-side, calls `crabka-client-admin` through a thin adapter, derives UI capabilities from broker ACLs, and renders an operations-sidebar admin console. Spec: [docs/superpowers/specs/2026-07-04-dioxus-broker-admin-ui-design.md](../specs/2026-07-04-dioxus-broker-admin-ui-design.md).

**Tech Stack:** Rust 2024, Dioxus `0.7.9` fullstack/server/router, axum `0.8`, Tokio, `crabka-client-admin`, `crabka-client-core`, `crabka-security`, `serde`, `thiserror`, `uuid`, `playwright-rs` for browser E2E.

---

## Baseline Note

This plan was written from the isolated worktree `C:\Users\Matt Stone\git\crabka\.worktrees\dioxus-broker-admin-ui` on branch `ms/dioxus-broker-admin-ui`.

Baseline checks before implementation:

- `cargo build`: PASS.
- `cargo test`: FAIL before admin-ui changes. Failures include Rust compiler/internal resolution errors in existing `crabka-broker` tests and dependency rlib format errors for crates such as `picky`, `potential_utf`, `ecdsa`, `icu_properties`, and `crypto_primes`.

Do not use workspace-wide `cargo test` as the first signal for admin-ui regressions until the baseline issue is fixed. Use targeted `cargo test -p crabka-admin-ui` and targeted integration/E2E commands in this plan.

---

## Global Constraints

- Work in the isolated worktree `C:\Users\Matt Stone\git\crabka\.worktrees\dioxus-broker-admin-ui` unless the user explicitly says otherwise.
- The workspace uses `members = ["crates/*"]`, so `crates/admin-ui` is included automatically once created.
- Use workspace package metadata: `version.workspace = true`, `edition.workspace = true`, `license.workspace = true`, `authors.workspace = true`, `rust-version.workspace = true`.
- Add `publish = false` to `crates/admin-ui/Cargo.toml`; this is an application binary, not a published library crate.
- Keep `unsafe_code = "forbid"` satisfied. Do not add unsafe code.
- Do not log broker passwords, session IDs, SCRAM salted material, cookies, or raw Authorization/Cookie headers.
- The first slice supports only SCRAM-SHA-512 broker login. Do not add PLAIN, SCRAM-SHA-256, OAuth, OIDC, static UI users, or multi-cluster profile support.
- Broker ACL authorization remains authoritative. Permission derivation only hides/disables UI affordances.
- Behavior tests must exercise code behavior, not source text.
- Use conventional commits for implementation commits: `feat:` for new user-visible admin UI slices, `test:` for tests-only follow-ups, `fix:` for bug fixes.
- Prefer targeted verification: `cargo test -p crabka-admin-ui`, `cargo build -p crabka-admin-ui`, and crate-local clippy. Workspace-wide tests are blocked by the baseline note above.

---

## File Structure

Create the new crate with these files:

- `crates/admin-ui/Cargo.toml`: package metadata, binary target, Dioxus/admin/test dependencies.
- `crates/admin-ui/src/main.rs`: CLI entrypoint, config loading, tracing setup, server startup.
- `crates/admin-ui/src/lib.rs`: crate module declarations and `app()` export for Dioxus.
- `crates/admin-ui/src/config.rs`: parse app configuration from env/TOML path, validated runtime config.
- `crates/admin-ui/src/session.rs`: server-side session IDs, session records, in-memory session store, cookie helpers.
- `crates/admin-ui/src/auth.rs`: SCRAM-SHA-512 `ClientSecurity` construction and login/logout service logic.
- `crates/admin-ui/src/error.rs`: UI-facing error types and conversions from admin/client errors.
- `crates/admin-ui/src/dto.rs`: serializable DTOs used by server functions and views.
- `crates/admin-ui/src/permissions.rs`: capability derivation from ACL entries.
- `crates/admin-ui/src/admin.rs`: thin adapter over `crabka-client-admin`; no Dioxus code here.
- `crates/admin-ui/src/server.rs`: axum/Dioxus server construction and health route.
- `crates/admin-ui/src/server_fns.rs`: Dioxus server functions for auth/read/mutation flows.
- `crates/admin-ui/src/views/mod.rs`: view module declarations.
- `crates/admin-ui/src/views/layout.rs`: operations-sidebar shell and route guard.
- `crates/admin-ui/src/views/login.rs`: login screen.
- `crates/admin-ui/src/views/overview.rs`: first lightweight overview route.
- `crates/admin-ui/src/views/topics.rs`: topics/configs table and forms.
- `crates/admin-ui/src/views/groups.rs`: group list and offsets detail.
- `crates/admin-ui/src/views/acls.rs`: ACL table and create/delete forms.
- `crates/admin-ui/src/views/users.rs`: SCRAM-SHA-512 user forms.
- `crates/admin-ui/src/views/quotas.rs`: quota read/mutate forms.
- `crates/admin-ui/src/views/log_dirs.rs`: log-dir table and optional move form guarded by warning text.
- `crates/admin-ui/src/views/components.rs`: shared table/error/modal helpers only after at least two views use them.
- `crates/admin-ui/tests/config.rs`: config parsing behavior.
- `crates/admin-ui/tests/session.rs`: session store behavior.
- `crates/admin-ui/tests/permissions.rs`: ACL-to-capability behavior.
- `crates/admin-ui/tests/admin_mapping.rs`: DTO/error mapping behavior with pure fixtures.
- `crates/admin-ui/tests/server_fns.rs`: server-function seam tests with a fake admin service.
- `crates/admin-ui/tests/e2e.rs`: `playwright-rs` high-value browser flows.

Do not split further unless a file grows past focused responsibility during implementation.

---

## Batch Plan

| Batch | Tasks | Parallel? | Rationale |
|---|---|---|---|
| A - Crate Foundation | 1, then 2/3 | Partial | Task 1 creates crate; config/session and DTO/errors are independent after that. |
| B - Auth/Admin Core | 4 and 5 | Yes | SCRAM login/security and permission derivation/admin mapping touch disjoint files. |
| C - Server Functions + UI Shell | 6, then 7/8 | Partial | Server state/functions precede views; layout and first read views can then proceed together. |
| D - Mutations + E2E | 9 and 10, then 11 | Partial | Mutation views and server tests can proceed together; Playwright depends on runnable UI. |

Dispatch every parallel group in one message with separate subagents. Review after each batch before moving on.

---

## Batch A - Crate Foundation

### Task 1: Create `crabka-admin-ui` crate skeleton

**Files:**
- Create: `crates/admin-ui/Cargo.toml`
- Create: `crates/admin-ui/src/lib.rs`
- Create: `crates/admin-ui/src/main.rs`
- Create: `crates/admin-ui/src/server.rs`
- Test: `crates/admin-ui/tests/smoke.rs`

**Interfaces:**
- Produces `crabka_admin_ui::app() -> dioxus::prelude::Element`.
- Produces `crabka_admin_ui::server::health_router() -> axum::Router`.

- [ ] **Step 1: Write the failing smoke test**

Create `crates/admin-ui/tests/smoke.rs`:

```rust
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt as _;

#[tokio::test]
async fn healthz_returns_ok() {
    let app = crabka_admin_ui::server::health_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds");

    assert_eq!(response.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-admin-ui --test smoke`

Expected: FAIL because package `crabka-admin-ui` does not exist.

- [ ] **Step 3: Add the crate manifest**

Create `crates/admin-ui/Cargo.toml`:

```toml
[package]
name = "crabka-admin-ui"
publish = false
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Standalone Dioxus broker administration UI for Crabka"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-admin-ui"
keywords = ["kafka", "admin", "ui", "dioxus", "crabka"]
categories = ["web-programming::http-server"]

[lints]
workspace = true

[[bin]]
name = "crabka-admin-ui"
path = "src/main.rs"

[dependencies]
anyhow.workspace = true
axum = { workspace = true, features = ["json", "query"] }
clap = { workspace = true, features = ["derive", "env"] }
crabka-client-admin = { version = "0.3.8", path = "../client-admin" }
crabka-client-core = { version = "0.3.8", path = "../client-core" }
crabka-security = { version = "0.3.8", path = "../security" }
dioxus = { version = "0.7.9", default-features = false, features = ["fullstack", "router", "server"] }
serde = { workspace = true, features = ["derive"] }
serde_json.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["macros", "net", "rt", "rt-multi-thread", "signal", "sync", "time"] }
tower = { workspace = true, features = ["util"] }
tracing.workspace = true
tracing-subscriber.workspace = true
uuid = { workspace = true, features = ["v4", "serde"] }

[dev-dependencies]
assert2.workspace = true
tempfile.workspace = true
playwright-rs = "0.14.0"
```

Do not add Dioxus `web` yet; this first server-only compile verifies the crate and health route. Add web/hydration features only when the first browser route needs them.

- [ ] **Step 4: Add the minimal library and server module**

Create `crates/admin-ui/src/lib.rs`:

```rust
//! Standalone Dioxus administration UI for one Crabka cluster.

pub mod server;

use dioxus::prelude::*;

#[component]
pub fn App() -> Element {
    rsx! {
        main { class: "admin-shell",
            h1 { "Crabka Admin" }
            p { "Admin UI server is running." }
        }
    }
}

pub fn app() -> Element {
    rsx! { App {} }
}
```

Create `crates/admin-ui/src/server.rs`:

```rust
//! HTTP server helpers for the admin UI.

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { StatusCode::OK }))
}
```

Create `crates/admin-ui/src/main.rs`:

```rust
use std::net::SocketAddr;

use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = std::env::var("CRABKA_ADMIN_UI_LISTEN_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8088".to_string())
        .parse()
        .context("parse CRABKA_ADMIN_UI_LISTEN_ADDR")?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("bind admin UI listener")?;
    let bound = listener.local_addr().context("read admin UI listener addr")?;
    tracing::info!(%bound, "crabka admin UI listening");

    axum::serve(listener, crabka_admin_ui::server::health_router())
        .await
        .context("serve admin UI")
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p crabka-admin-ui --test smoke`

Expected: PASS.

Run: `cargo build -p crabka-admin-ui`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui
git commit -m "feat: add admin UI crate skeleton"
```

---

### Task 2: Add validated single-cluster config

**Files:**
- Create: `crates/admin-ui/src/config.rs`
- Modify: `crates/admin-ui/src/lib.rs`
- Modify: `crates/admin-ui/src/main.rs`
- Test: `crates/admin-ui/tests/config.rs`

**Interfaces:**
- Produces `AdminUiConfig::from_env() -> Result<AdminUiConfig, ConfigError>`.
- Produces `AdminUiConfig::validate(self) -> Result<Self, ConfigError>`.
- Produces `BrokerSecurityConfig` limited to `SaslPlaintext` and `SaslSsl` with SCRAM-SHA-512 at login time.

- [ ] **Step 1: Write failing config tests**

Create `crates/admin-ui/tests/config.rs`:

```rust
use std::net::SocketAddr;

use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig, ConfigError};

#[test]
fn default_config_targets_local_server_and_requires_bootstrap() {
    let cfg = AdminUiConfig::default();

    assert_eq!(cfg.listen_addr, "127.0.0.1:8088".parse::<SocketAddr>().unwrap());
    assert_eq!(cfg.cluster_name, "local");
    assert!(cfg.bootstrap_addrs.is_empty());

    let error = cfg.validate().expect_err("empty bootstrap is invalid");
    assert!(matches!(error, ConfigError::MissingBootstrap));
}

#[test]
fn validates_single_cluster_sasl_plaintext_config() {
    let cfg = AdminUiConfig {
        cluster_name: "dev".to_string(),
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };

    let validated = cfg.validate().expect("config is valid");
    assert_eq!(validated.cluster_name, "dev");
    assert_eq!(validated.bootstrap_addrs, ["127.0.0.1:9092"]);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p crabka-admin-ui --test config`

Expected: FAIL because `config` module does not exist.

- [ ] **Step 3: Implement config module**

Create `crates/admin-ui/src/config.rs`:

```rust
//! Runtime configuration for one admin UI instance.

use std::net::SocketAddr;
use std::path::PathBuf;

use crabka_client_core::security::TlsConnectorConfig;
use crabka_security::ListenerProtocol;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerSecurityConfig {
    SaslPlaintext,
    SaslSsl {
        trust_roots_pem: Option<PathBuf>,
        server_name: String,
        client_identity: Option<(PathBuf, PathBuf)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminUiConfig {
    pub listen_addr: SocketAddr,
    pub cluster_name: String,
    pub bootstrap_addrs: Vec<String>,
    pub security: BrokerSecurityConfig,
    pub session_ttl_seconds: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("at least one CRABKA_ADMIN_UI_BOOTSTRAP address is required")]
    MissingBootstrap,
    #[error("CRABKA_ADMIN_UI_LISTEN_ADDR is invalid: {0}")]
    InvalidListenAddr(String),
    #[error("CRABKA_ADMIN_UI_SECURITY_PROTOCOL must be SASL_PLAINTEXT or SASL_SSL")]
    InvalidSecurityProtocol,
    #[error("CRABKA_ADMIN_UI_TLS_SERVER_NAME is required for SASL_SSL")]
    MissingTlsServerName,
}

impl Default for AdminUiConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:8088".parse().expect("static socket addr parses"),
            cluster_name: "local".to_string(),
            bootstrap_addrs: Vec::new(),
            security: BrokerSecurityConfig::SaslPlaintext,
            session_ttl_seconds: 8 * 60 * 60,
        }
    }
}

impl AdminUiConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut cfg = Self::default();
        if let Ok(raw) = std::env::var("CRABKA_ADMIN_UI_LISTEN_ADDR") {
            cfg.listen_addr = raw
                .parse()
                .map_err(|_| ConfigError::InvalidListenAddr(raw.clone()))?;
        }
        if let Ok(name) = std::env::var("CRABKA_ADMIN_UI_CLUSTER_NAME") {
            cfg.cluster_name = name;
        }
        if let Ok(addrs) = std::env::var("CRABKA_ADMIN_UI_BOOTSTRAP") {
            cfg.bootstrap_addrs = addrs
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .collect();
        }
        if let Ok(protocol) = std::env::var("CRABKA_ADMIN_UI_SECURITY_PROTOCOL") {
            cfg.security = match protocol.as_str() {
                "SASL_PLAINTEXT" => BrokerSecurityConfig::SaslPlaintext,
                "SASL_SSL" => BrokerSecurityConfig::SaslSsl {
                    trust_roots_pem: std::env::var_os("CRABKA_ADMIN_UI_TLS_TRUST_ROOTS_PEM")
                        .map(PathBuf::from),
                    server_name: std::env::var("CRABKA_ADMIN_UI_TLS_SERVER_NAME")
                        .map_err(|_| ConfigError::MissingTlsServerName)?,
                    client_identity: match (
                        std::env::var_os("CRABKA_ADMIN_UI_TLS_CLIENT_CERT_PEM"),
                        std::env::var_os("CRABKA_ADMIN_UI_TLS_CLIENT_KEY_PEM"),
                    ) {
                        (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
                        _ => None,
                    },
                },
                _ => return Err(ConfigError::InvalidSecurityProtocol),
            };
        }
        cfg.validate()
    }

    pub fn validate(self) -> Result<Self, ConfigError> {
        if self.bootstrap_addrs.is_empty() {
            return Err(ConfigError::MissingBootstrap);
        }
        Ok(self)
    }
}

impl BrokerSecurityConfig {
    #[must_use]
    pub fn listener_protocol(&self) -> ListenerProtocol {
        match self {
            Self::SaslPlaintext => ListenerProtocol::SaslPlaintext,
            Self::SaslSsl { .. } => ListenerProtocol::SaslSsl,
        }
    }

    #[must_use]
    pub fn tls(&self) -> Option<TlsConnectorConfig> {
        match self {
            Self::SaslPlaintext => None,
            Self::SaslSsl {
                trust_roots_pem,
                server_name,
                client_identity,
            } => Some(TlsConnectorConfig {
                trust_roots_pem: trust_roots_pem.clone(),
                server_name: server_name.clone(),
                client_identity: client_identity.clone(),
            }),
        }
    }
}
```

Modify `crates/admin-ui/src/lib.rs`:

```rust
//! Standalone Dioxus administration UI for one Crabka cluster.

pub mod config;
pub mod server;

use dioxus::prelude::*;

#[component]
pub fn App() -> Element {
    rsx! {
        main { class: "admin-shell",
            h1 { "Crabka Admin" }
            p { "Admin UI server is running." }
        }
    }
}

pub fn app() -> Element {
    rsx! { App {} }
}
```

Modify `crates/admin-ui/src/main.rs` to use config:

```rust
use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = crabka_admin_ui::config::AdminUiConfig::from_env()
        .context("load admin UI config")?;

    let listener = tokio::net::TcpListener::bind(cfg.listen_addr)
        .await
        .context("bind admin UI listener")?;
    let bound = listener.local_addr().context("read admin UI listener addr")?;
    tracing::info!(%bound, cluster = %cfg.cluster_name, "crabka admin UI listening");

    axum::serve(listener, crabka_admin_ui::server::health_router())
        .await
        .context("serve admin UI")
}
```

- [ ] **Step 4: Run config tests**

Run: `cargo test -p crabka-admin-ui --test config`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/src/config.rs crates/admin-ui/src/lib.rs crates/admin-ui/src/main.rs crates/admin-ui/tests/config.rs
git commit -m "feat: add admin UI runtime config"
```

---

### Task 3: Add session store and UI DTO/error foundation

**Files:**
- Create: `crates/admin-ui/src/session.rs`
- Create: `crates/admin-ui/src/dto.rs`
- Create: `crates/admin-ui/src/error.rs`
- Modify: `crates/admin-ui/src/lib.rs`
- Test: `crates/admin-ui/tests/session.rs`
- Test: `crates/admin-ui/tests/admin_mapping.rs`

**Interfaces:**
- Produces `SessionStore`, `SessionId`, `SessionRecord`.
- Produces `UiError`, `KafkaErrorDto`, `ResourceOutcome`.
- Produces DTOs reused by admin adapter and views.

- [ ] **Step 1: Write failing session tests**

Create `crates/admin-ui/tests/session.rs`:

```rust
use std::time::Duration;

use crabka_admin_ui::session::{SessionStore, SessionUser};

#[test]
fn session_store_creates_and_retrieves_user() {
    let store = SessionStore::new(Duration::from_secs(60));
    let id = store.create(SessionUser {
        username: "alice".to_string(),
        principal: "User:alice".to_string(),
    });

    let record = store.get(&id).expect("session exists");
    assert_eq!(record.user.username, "alice");
    assert_eq!(record.user.principal, "User:alice");
}

#[test]
fn logout_removes_session() {
    let store = SessionStore::new(Duration::from_secs(60));
    let id = store.create(SessionUser {
        username: "bob".to_string(),
        principal: "User:bob".to_string(),
    });

    assert!(store.remove(&id));
    assert!(store.get(&id).is_none());
}
```

- [ ] **Step 2: Write failing DTO mapping test**

Create `crates/admin-ui/tests/admin_mapping.rs`:

```rust
use crabka_admin_ui::dto::{KafkaErrorDto, ResourceOutcome};

#[test]
fn resource_outcome_reports_error_state() {
    let ok = ResourceOutcome::ok("orders");
    assert!(!ok.has_error());

    let failed = ResourceOutcome::failed(
        "orders",
        KafkaErrorDto {
            code: 36,
            name: "TOPIC_ALREADY_EXISTS".to_string(),
            message: Some("topic exists".to_string()),
        },
    );
    assert!(failed.has_error());
}
```

- [ ] **Step 3: Run tests to verify failure**

Run: `cargo test -p crabka-admin-ui --test session --test admin_mapping`

Expected: FAIL because modules/types do not exist.

- [ ] **Step 4: Implement session, DTO, and error modules**

Create `crates/admin-ui/src/session.rs`:

```rust
//! Server-side session storage for authenticated admin UI users.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

impl SessionId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    #[must_use]
    pub fn expose_for_cookie(&self) -> &str {
        &self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&str> for SessionId {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Uuid::parse_str(value).map_err(|_| ())?;
        Ok(Self(value.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUser {
    pub username: String,
    pub principal: String,
}

#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub user: SessionUser,
    expires_at: Instant,
}

impl SessionRecord {
    #[must_use]
    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

#[derive(Debug)]
pub struct SessionStore {
    ttl: Duration,
    sessions: RwLock<HashMap<SessionId, SessionRecord>>,
}

impl SessionStore {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub fn create(&self, user: SessionUser) -> SessionId {
        let id = SessionId::new();
        let record = SessionRecord {
            user,
            expires_at: Instant::now() + self.ttl,
        };
        self.sessions.write().insert(id.clone(), record);
        id
    }

    pub fn get(&self, id: &SessionId) -> Option<SessionRecord> {
        let now = Instant::now();
        let record = self.sessions.read().get(id).cloned()?;
        if record.is_expired(now) {
            self.sessions.write().remove(id);
            None
        } else {
            Some(record)
        }
    }

    pub fn remove(&self, id: &SessionId) -> bool {
        self.sessions.write().remove(id).is_some()
    }
}
```

Add `parking_lot.workspace = true` to `crates/admin-ui/Cargo.toml` dependencies.

Create `crates/admin-ui/src/dto.rs`:

```rust
//! Serializable data transfer objects shared by server functions and views.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaErrorDto {
    pub code: i16,
    pub name: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceOutcome {
    pub resource: String,
    pub error: Option<KafkaErrorDto>,
}

impl ResourceOutcome {
    #[must_use]
    pub fn ok(resource: impl Into<String>) -> Self {
        Self {
            resource: resource.into(),
            error: None,
        }
    }

    #[must_use]
    pub fn failed(resource: impl Into<String>, error: KafkaErrorDto) -> Self {
        Self {
            resource: resource.into(),
            error: Some(error),
        }
    }

    #[must_use]
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRow {
    pub name: String,
    pub topic_id: Option<String>,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub error: Option<KafkaErrorDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRow {
    pub group_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogDirRow {
    pub log_dir: String,
    pub topic: String,
    pub partition: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
    pub error: Option<KafkaErrorDto>,
}
```

Create `crates/admin-ui/src/error.rs`:

```rust
//! UI-facing errors surfaced by server functions.

use crabka_client_admin::{AdminError, KafkaError};
use thiserror::Error;

use crate::dto::KafkaErrorDto;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum UiError {
    #[error("not authenticated")]
    NotAuthenticated,
    #[error("session expired")]
    SessionExpired,
    #[error("broker connection failed: {0}")]
    BrokerConnection(String),
    #[error("broker returned {name} ({code}) for {api}")]
    Broker { api: &'static str, code: i16, name: String, message: Option<String> },
    #[error("admin operation failed: {0}")]
    Admin(String),
}

impl From<&KafkaError> for KafkaErrorDto {
    fn from(value: &KafkaError) -> Self {
        Self {
            code: value.code,
            name: value.name.to_string(),
            message: value.message.clone(),
        }
    }
}

impl From<AdminError> for UiError {
    fn from(value: AdminError) -> Self {
        match value {
            AdminError::Connect { tried } => Self::BrokerConnection(format!(
                "no bootstrap address was reachable: tried {tried}"
            )),
            AdminError::Broker { api, code, name, message } => Self::Broker {
                api,
                code,
                name: name.to_string(),
                message,
            },
            other => Self::Admin(other.to_string()),
        }
    }
}
```

Modify `crates/admin-ui/src/lib.rs` module declarations:

```rust
pub mod config;
pub mod dto;
pub mod error;
pub mod server;
pub mod session;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p crabka-admin-ui --test session --test admin_mapping`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/Cargo.toml crates/admin-ui/src/lib.rs crates/admin-ui/src/session.rs crates/admin-ui/src/dto.rs crates/admin-ui/src/error.rs crates/admin-ui/tests/session.rs crates/admin-ui/tests/admin_mapping.rs
git commit -m "feat: add admin UI session and DTO foundation"
```

---

## Batch B - Auth/Admin Core

### Task 4: Implement SCRAM-SHA-512 broker-backed login service

**Files:**
- Create: `crates/admin-ui/src/auth.rs`
- Modify: `crates/admin-ui/src/lib.rs`
- Test: `crates/admin-ui/tests/auth.rs`

**Interfaces:**
- Produces `LoginRequest`, `LoginSuccess`, `AuthService`.
- Produces `build_scram_sha512_security(&AdminUiConfig, &str, &str) -> ClientSecurity`.
- Uses `AdminClient::connect_secured` with `SaslCredentials::Scram { mechanism: SaslMechanism::ScramSha512, ... }`.

- [ ] **Step 1: Write failing security-construction test**

Create `crates/admin-ui/tests/auth.rs`:

```rust
use crabka_admin_ui::auth::build_scram_sha512_security;
use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig};
use crabka_client_core::security::SaslCredentials;
use crabka_security::{ListenerProtocol, SaslMechanism};

#[test]
fn build_security_uses_scram_sha512_only() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };

    let security = build_scram_sha512_security(&cfg, "alice", "secret");

    assert_eq!(security.protocol, ListenerProtocol::SaslPlaintext);
    assert!(security.tls.is_none());
    assert!(matches!(
        security.sasl,
        Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            ref username,
            ref password,
        }) if username == "alice" && password == "secret"
    ));
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p crabka-admin-ui --test auth`

Expected: FAIL because `auth` module does not exist.

- [ ] **Step 3: Implement auth module**

Create `crates/admin-ui/src/auth.rs`:

```rust
//! Broker-backed login for the admin UI.

use crabka_client_admin::AdminClient;
use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_security::SaslMechanism;
use serde::{Deserialize, Serialize};

use crate::config::AdminUiConfig;
use crate::error::UiError;
use crate::session::{SessionStore, SessionUser};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginSuccess {
    pub username: String,
    pub principal: String,
    pub session_id: String,
}

#[must_use]
pub fn build_scram_sha512_security(
    cfg: &AdminUiConfig,
    username: &str,
    password: &str,
) -> ClientSecurity {
    ClientSecurity {
        protocol: cfg.security.listener_protocol(),
        tls: cfg.security.tls(),
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username: username.to_string(),
            password: password.to_string(),
        }),
        sasl_host: None,
    }
}

pub struct AuthService<'a> {
    cfg: &'a AdminUiConfig,
    sessions: &'a SessionStore,
}

impl<'a> AuthService<'a> {
    #[must_use]
    pub fn new(cfg: &'a AdminUiConfig, sessions: &'a SessionStore) -> Self {
        Self { cfg, sessions }
    }

    pub async fn login(&self, request: LoginRequest) -> Result<LoginSuccess, UiError> {
        let security = build_scram_sha512_security(self.cfg, &request.username, &request.password);
        let mut admin = AdminClient::connect_secured(&self.cfg.bootstrap_addrs, Some(security)).await?;
        let _metadata = admin.metadata(&[]).await?;

        let principal = format!("User:{}", request.username);
        let session_id = self.sessions.create(SessionUser {
            username: request.username.clone(),
            principal: principal.clone(),
        });

        Ok(LoginSuccess {
            username: request.username,
            principal,
            session_id: session_id.expose_for_cookie().to_string(),
        })
    }
}
```

Modify `crates/admin-ui/src/lib.rs`:

```rust
pub mod auth;
pub mod config;
pub mod dto;
pub mod error;
pub mod server;
pub mod session;
```

- [ ] **Step 4: Run auth tests**

Run: `cargo test -p crabka-admin-ui --test auth`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/src/auth.rs crates/admin-ui/src/lib.rs crates/admin-ui/tests/auth.rs
git commit -m "feat: add SCRAM broker login service"
```

---

### Task 5: Implement permission derivation and admin adapter mappings

**Files:**
- Create: `crates/admin-ui/src/permissions.rs`
- Create: `crates/admin-ui/src/admin.rs`
- Modify: `crates/admin-ui/src/dto.rs`
- Modify: `crates/admin-ui/src/lib.rs`
- Test: `crates/admin-ui/tests/permissions.rs`
- Test: `crates/admin-ui/tests/admin_mapping.rs`

**Interfaces:**
- Produces `Capabilities` with booleans used by UI route guards and action buttons.
- Produces `AdminFacade` wrapping `crabka-client-admin::AdminClient`.
- Produces pure mapping helpers for topic rows, group rows, log-dir rows, and outcomes.

- [ ] **Step 1: Write failing permission tests**

Create `crates/admin-ui/tests/permissions.rs`:

```rust
use crabka_admin_ui::permissions::{Capabilities, derive_capabilities};
use crabka_client_admin::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};

fn allow(resource_type: ResourceType, operation: AclOperation) -> AclEntry {
    AclEntry {
        resource_type,
        resource_name: "*".to_string(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".to_string(),
        host: "*".to_string(),
        operation,
        permission_type: PermissionType::Allow,
    }
}

#[test]
fn derives_topic_admin_capabilities_from_topic_acls() {
    let caps = derive_capabilities(
        "User:alice",
        &[
            allow(ResourceType::Topic, AclOperation::Describe),
            allow(ResourceType::Topic, AclOperation::Create),
            allow(ResourceType::Topic, AclOperation::Alter),
            allow(ResourceType::Topic, AclOperation::Delete),
        ],
    );

    assert!(caps.can_view_topics);
    assert!(caps.can_create_topics);
    assert!(caps.can_alter_topics);
    assert!(caps.can_delete_topics);
}

#[test]
fn unrelated_principal_gets_no_capabilities() {
    let caps = derive_capabilities("User:bob", &[allow(ResourceType::Topic, AclOperation::Describe)]);
    assert_eq!(caps, Capabilities::default());
}
```

- [ ] **Step 2: Extend mapping test for topics**

Append to `crates/admin-ui/tests/admin_mapping.rs`:

```rust
use crabka_admin_ui::admin::topic_rows;
use crabka_client_admin::{KafkaError, TopicMetadata, TopicMetadataEntry};

#[test]
fn maps_topic_metadata_to_rows_with_errors() {
    let metadata = TopicMetadata {
        controller_id: 1,
        topics: vec![TopicMetadataEntry {
            name: "orders".to_string(),
            topic_id: None,
            partition_count: 3,
            replication_factor: 1,
            error: Some(KafkaError {
                code: 3,
                name: "UNKNOWN_TOPIC_OR_PARTITION",
                message: Some("missing".to_string()),
            }),
        }],
    };

    let rows = topic_rows(metadata);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "orders");
    assert_eq!(rows[0].error.as_ref().unwrap().code, 3);
}
```

- [ ] **Step 3: Run tests to verify failure**

Run: `cargo test -p crabka-admin-ui --test permissions --test admin_mapping`

Expected: FAIL because `permissions` and `admin` modules do not exist.

- [ ] **Step 4: Implement permissions**

Create `crates/admin-ui/src/permissions.rs`:

```rust
//! Derive UI affordances from broker ACL entries.

use crabka_client_admin::{AclEntry, AclOperation, PermissionType, ResourceType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub can_view_topics: bool,
    pub can_create_topics: bool,
    pub can_alter_topics: bool,
    pub can_delete_topics: bool,
    pub can_view_groups: bool,
    pub can_view_acls: bool,
    pub can_alter_acls: bool,
    pub can_alter_users: bool,
    pub can_view_quotas: bool,
    pub can_alter_quotas: bool,
    pub can_view_log_dirs: bool,
}

#[must_use]
pub fn derive_capabilities(principal: &str, entries: &[AclEntry]) -> Capabilities {
    let mut caps = Capabilities::default();
    for entry in entries.iter().filter(|e| {
        e.principal == principal && matches!(e.permission_type, PermissionType::Allow)
    }) {
        match (entry.resource_type, entry.operation) {
            (ResourceType::Topic, AclOperation::Describe | AclOperation::Read) => {
                caps.can_view_topics = true;
            }
            (ResourceType::Topic, AclOperation::Create) => caps.can_create_topics = true,
            (ResourceType::Topic, AclOperation::Alter | AclOperation::AlterConfigs) => {
                caps.can_alter_topics = true;
            }
            (ResourceType::Topic, AclOperation::Delete) => caps.can_delete_topics = true,
            (ResourceType::Group, AclOperation::Describe | AclOperation::Read) => {
                caps.can_view_groups = true;
            }
            (ResourceType::Cluster, AclOperation::Describe) => {
                caps.can_view_log_dirs = true;
                caps.can_view_quotas = true;
            }
            (ResourceType::Cluster, AclOperation::Alter | AclOperation::AlterConfigs) => {
                caps.can_alter_acls = true;
                caps.can_alter_users = true;
                caps.can_alter_quotas = true;
            }
            (ResourceType::Cluster, AclOperation::DescribeConfigs) => caps.can_view_acls = true,
            _ => {}
        }
    }
    caps
}
```

- [ ] **Step 5: Implement admin mappings and facade shell**

Create `crates/admin-ui/src/admin.rs`:

```rust
//! Thin admin-client adapter for UI-facing operations.

use crabka_client_admin::{AdminClient, AdminError, LogDirInfo, TopicMetadata};

use crate::dto::{KafkaErrorDto, LogDirRow, TopicRow};

pub struct AdminFacade {
    client: AdminClient,
}

impl AdminFacade {
    #[must_use]
    pub fn new(client: AdminClient) -> Self {
        Self { client }
    }

    pub async fn topics(&mut self) -> Result<Vec<TopicRow>, AdminError> {
        let metadata = self.client.metadata(&[]).await?;
        Ok(topic_rows(metadata))
    }

    pub async fn log_dirs(&mut self) -> Result<Vec<LogDirRow>, AdminError> {
        let dirs = self.client.describe_log_dirs(None).await?;
        Ok(log_dir_rows(dirs))
    }
}

#[must_use]
pub fn topic_rows(metadata: TopicMetadata) -> Vec<TopicRow> {
    metadata
        .topics
        .into_iter()
        .map(|topic| TopicRow {
            name: topic.name,
            topic_id: topic.topic_id.map(|id| id.to_string()),
            partition_count: topic.partition_count,
            replication_factor: topic.replication_factor,
            error: topic.error.as_ref().map(KafkaErrorDto::from),
        })
        .collect()
}

#[must_use]
pub fn log_dir_rows(dirs: Vec<LogDirInfo>) -> Vec<LogDirRow> {
    dirs.into_iter()
        .flat_map(|dir| {
            let dir_error = dir.error.as_ref().map(KafkaErrorDto::from);
            dir.topics.into_iter().flat_map(move |topic| {
                let log_dir = dir.log_dir.clone();
                let dir_error = dir_error.clone();
                topic.partitions.into_iter().map(move |partition| LogDirRow {
                    log_dir: log_dir.clone(),
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                    partition_size: partition.partition_size,
                    offset_lag: partition.offset_lag,
                    is_future_key: partition.is_future_key,
                    error: dir_error.clone(),
                })
            })
        })
        .collect()
}
```

Modify `crates/admin-ui/src/lib.rs` to include:

```rust
pub mod admin;
pub mod permissions;
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p crabka-admin-ui --test permissions --test admin_mapping`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/src/admin.rs crates/admin-ui/src/permissions.rs crates/admin-ui/src/dto.rs crates/admin-ui/src/lib.rs crates/admin-ui/tests/permissions.rs crates/admin-ui/tests/admin_mapping.rs
git commit -m "feat: derive admin UI capabilities"
```

---

## Batch C - Server Functions + UI Shell

### Task 6: Add app state and server-function seam

**Files:**
- Modify: `crates/admin-ui/src/server.rs`
- Create: `crates/admin-ui/src/server_fns.rs`
- Modify: `crates/admin-ui/src/lib.rs`
- Test: `crates/admin-ui/tests/server_fns.rs`

**Interfaces:**
- Produces `AppState { cfg: Arc<AdminUiConfig>, sessions: Arc<SessionStore> }`.
- Produces server-function seam functions for login/logout/current session/topics/groups/acls/users/quotas/log dirs.
- Keeps raw passwords only in `LoginRequest` handling and never serializes them back.

- [ ] **Step 1: Write failing state test**

Create `crates/admin-ui/tests/server_fns.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig};
use crabka_admin_ui::server::AppState;
use crabka_admin_ui::session::SessionStore;

#[test]
fn app_state_carries_config_and_sessions() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };
    let state = AppState::new(cfg.clone());

    assert_eq!(state.cfg.cluster_name, cfg.cluster_name);
    assert_eq!(state.sessions_ttl_seconds(), cfg.session_ttl_seconds);

    let explicit = AppState::from_parts(Arc::new(cfg), Arc::new(SessionStore::new(Duration::from_secs(5))));
    assert_eq!(explicit.sessions_ttl_seconds(), 5);
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p crabka-admin-ui --test server_fns`

Expected: FAIL because `AppState` does not exist.

- [ ] **Step 3: Implement app state and server-function module shell**

Modify `crates/admin-ui/src/server.rs`:

```rust
//! HTTP server helpers for the admin UI.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

use crate::config::AdminUiConfig;
use crate::session::SessionStore;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<AdminUiConfig>,
    pub sessions: Arc<SessionStore>,
}

impl AppState {
    #[must_use]
    pub fn new(cfg: AdminUiConfig) -> Self {
        let ttl = Duration::from_secs(cfg.session_ttl_seconds);
        Self {
            cfg: Arc::new(cfg),
            sessions: Arc::new(SessionStore::new(ttl)),
        }
    }

    #[must_use]
    pub fn from_parts(cfg: Arc<AdminUiConfig>, sessions: Arc<SessionStore>) -> Self {
        Self { cfg, sessions }
    }

    #[must_use]
    pub fn sessions_ttl_seconds(&self) -> u64 {
        self.cfg.session_ttl_seconds
    }
}

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { StatusCode::OK }))
}
```

Create `crates/admin-ui/src/server_fns.rs`:

```rust
//! Dioxus server functions for the admin UI.

use dioxus::prelude::*;

use crate::auth::{LoginRequest, LoginSuccess};
use crate::dto::TopicRow;

#[server]
pub async fn login(request: LoginRequest) -> Result<LoginSuccess, ServerFnError> {
    let _ = request;
    Err(ServerFnError::new("login requires AppState extraction from the Dioxus server context"))
}

#[server]
pub async fn logout() -> Result<(), ServerFnError> {
    Ok(())
}

#[server]
pub async fn list_topics() -> Result<Vec<TopicRow>, ServerFnError> {
    Err(ServerFnError::new("list_topics requires AppState extraction from the Dioxus server context"))
}
```

Modify `crates/admin-ui/src/lib.rs` to include:

```rust
pub mod server_fns;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p crabka-admin-ui --test server_fns`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/src/server.rs crates/admin-ui/src/server_fns.rs crates/admin-ui/src/lib.rs crates/admin-ui/tests/server_fns.rs
git commit -m "feat: add admin UI server state"
```

---

### Task 7: Add operations-sidebar layout and route guard

**Files:**
- Create: `crates/admin-ui/src/views/mod.rs`
- Create: `crates/admin-ui/src/views/layout.rs`
- Create: `crates/admin-ui/src/views/login.rs`
- Create: `crates/admin-ui/src/views/overview.rs`
- Modify: `crates/admin-ui/src/lib.rs`

**Interfaces:**
- Produces `Route` enum.
- Produces operations-sidebar links for Overview, Topics, Groups, ACLs, Users, Quotas, Log Dirs.

- [ ] **Step 1: Add view modules and route shell**

Create `crates/admin-ui/src/views/mod.rs`:

```rust
pub mod layout;
pub mod login;
pub mod overview;

use dioxus::prelude::*;
use dioxus::prelude::Routable;

#[derive(Debug, Clone, Routable, PartialEq)]
pub enum Route {
    #[route("/")]
    Overview {},
    #[route("/login")]
    Login {},
    #[route("/topics")]
    Topics {},
    #[route("/groups")]
    Groups {},
    #[route("/acls")]
    Acls {},
    #[route("/users")]
    Users {},
    #[route("/quotas")]
    Quotas {},
    #[route("/log-dirs")]
    LogDirs {},
}
```

Create `crates/admin-ui/src/views/layout.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Shell(children: Element) -> Element {
    rsx! {
        div { class: "admin-layout",
            aside { class: "sidebar",
                h1 { "Crabka" }
                nav {
                    a { href: "/", "Overview" }
                    a { href: "/topics", "Topics" }
                    a { href: "/groups", "Groups" }
                    a { href: "/acls", "ACLs" }
                    a { href: "/users", "Users" }
                    a { href: "/quotas", "Quotas" }
                    a { href: "/log-dirs", "Log Dirs" }
                }
            }
            main { class: "content", {children} }
        }
    }
}
```

Create `crates/admin-ui/src/views/login.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Login() -> Element {
    rsx! {
        main { class: "login-page",
            h1 { "Sign in to Crabka" }
            form {
                label { "Username" }
                input { r#type: "text", name: "username", autocomplete: "username" }
                label { "Password" }
                input { r#type: "password", name: "password", autocomplete: "current-password" }
                button { r#type: "submit", "Sign in" }
            }
        }
    }
}
```

Create `crates/admin-ui/src/views/overview.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Overview() -> Element {
    rsx! {
        section {
            h2 { "Overview" }
            p { "Use the sidebar to administer topics, groups, ACLs, SCRAM users, quotas, and log dirs." }
        }
    }
}
```

- [ ] **Step 2: Wire app to the shell**

Modify `crates/admin-ui/src/lib.rs` app body:

```rust
pub mod admin;
pub mod auth;
pub mod config;
pub mod dto;
pub mod error;
pub mod permissions;
pub mod server;
pub mod server_fns;
pub mod session;
pub mod views;

use dioxus::prelude::*;

#[component]
pub fn App() -> Element {
    rsx! {
        views::layout::Shell {
            views::overview::Overview {}
        }
    }
}

pub fn app() -> Element {
    rsx! { App {} }
}
```

- [ ] **Step 3: Build targeted crate**

Run: `cargo build -p crabka-admin-ui`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/src/lib.rs crates/admin-ui/src/views
git commit -m "feat: add admin UI operations shell"
```

---

### Task 8: Add first read-only admin views

**Files:**
- Create: `crates/admin-ui/src/views/topics.rs`
- Create: `crates/admin-ui/src/views/groups.rs`
- Create: `crates/admin-ui/src/views/acls.rs`
- Create: `crates/admin-ui/src/views/users.rs`
- Create: `crates/admin-ui/src/views/quotas.rs`
- Create: `crates/admin-ui/src/views/log_dirs.rs`
- Modify: `crates/admin-ui/src/views/mod.rs`
- Modify: `crates/admin-ui/src/lib.rs`

**Interfaces:**
- Produces read-oriented Dioxus components for all first-slice sections.
- Produces read-oriented Dioxus components for all first-slice sections with explicit empty states that render before live broker data is loaded.

- [ ] **Step 1: Add section components**

Create `crates/admin-ui/src/views/topics.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Topics() -> Element {
    rsx! {
        section {
            h2 { "Topics" }
            button { "Create Topic" }
            table {
                thead { tr { th { "Name" } th { "Partitions" } th { "Replication" } th { "Status" } } }
                tbody { tr { td { colspan: "4", "No topics loaded yet." } } }
            }
        }
    }
}
```

Create `crates/admin-ui/src/views/groups.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Groups() -> Element {
    rsx! { section { h2 { "Consumer Groups" } p { "No groups loaded yet." } } }
}
```

Create `crates/admin-ui/src/views/acls.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Acls() -> Element {
    rsx! { section { h2 { "ACLs" } button { "Create ACL" } p { "No ACLs loaded yet." } } }
}
```

Create `crates/admin-ui/src/views/users.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Users() -> Element {
    rsx! { section { h2 { "SCRAM Users" } button { "Upsert SCRAM-SHA-512" } p { "No user operation selected." } } }
}
```

Create `crates/admin-ui/src/views/quotas.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn Quotas() -> Element {
    rsx! { section { h2 { "Quotas" } p { "Search for a user to describe quotas." } } }
}
```

Create `crates/admin-ui/src/views/log_dirs.rs`:

```rust
use dioxus::prelude::*;

#[component]
pub fn LogDirs() -> Element {
    rsx! { section { h2 { "Log Dirs" } p { "No log-dir data loaded yet." } } }
}
```

- [ ] **Step 2: Declare modules**

Modify `crates/admin-ui/src/views/mod.rs`:

```rust
pub mod acls;
pub mod groups;
pub mod layout;
pub mod log_dirs;
pub mod login;
pub mod overview;
pub mod quotas;
pub mod topics;
pub mod users;
```

- [ ] **Step 3: Build targeted crate**

Run: `cargo build -p crabka-admin-ui`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/src/views
git commit -m "feat: add admin UI section views"
```

---

## Batch D - Mutations + E2E

### Task 9: Add admin mutation DTOs and server-function shells

**Files:**
- Modify: `crates/admin-ui/src/dto.rs`
- Modify: `crates/admin-ui/src/server_fns.rs`
- Test: `crates/admin-ui/tests/admin_mapping.rs`

**Interfaces:**
- Produces request DTOs for create/delete topics, partitions, configs, ACLs, SCRAM users, quotas, and log-dir moves.
- Server functions return `Vec<ResourceOutcome>` for batch-like Kafka operations.

- [ ] **Step 1: Add DTO tests**

Append to `crates/admin-ui/tests/admin_mapping.rs`:

```rust
use crabka_admin_ui::dto::{CreateTopicRequestDto, ScramUserUpsertDto};

#[test]
fn create_topic_request_validates_positive_counts() {
    let valid = CreateTopicRequestDto {
        name: "orders".to_string(),
        partitions: 3,
        replicas: 1,
        configs: vec![],
    };
    assert!(valid.validate().is_ok());

    let invalid = CreateTopicRequestDto { partitions: 0, ..valid };
    assert!(invalid.validate().is_err());
}

#[test]
fn scram_upsert_rejects_empty_password() {
    let request = ScramUserUpsertDto {
        username: "alice".to_string(),
        password: String::new(),
        iterations: 4096,
    };
    assert!(request.validate().is_err());
}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p crabka-admin-ui --test admin_mapping`

Expected: FAIL because DTOs do not exist.

- [ ] **Step 3: Implement mutation DTOs**

Append to `crates/admin-ui/src/dto.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigEntryDto {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTopicRequestDto {
    pub name: String,
    pub partitions: i32,
    pub replicas: i32,
    pub configs: Vec<ConfigEntryDto>,
}

impl CreateTopicRequestDto {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("topic name is required".to_string());
        }
        if self.partitions <= 0 {
            return Err("partitions must be positive".to_string());
        }
        if self.replicas <= 0 {
            return Err("replicas must be positive".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramUserUpsertDto {
    pub username: String,
    pub password: String,
    pub iterations: i32,
}

impl ScramUserUpsertDto {
    pub fn validate(&self) -> Result<(), String> {
        if self.username.trim().is_empty() {
            return Err("username is required".to_string());
        }
        if self.password.is_empty() {
            return Err("password is required".to_string());
        }
        if self.iterations <= 0 {
            return Err("iterations must be positive".to_string());
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Add server-function shells**

Append to `crates/admin-ui/src/server_fns.rs`:

```rust
use crate::dto::{CreateTopicRequestDto, ResourceOutcome, ScramUserUpsertDto};

#[server]
pub async fn create_topic(request: CreateTopicRequestDto) -> Result<Vec<ResourceOutcome>, ServerFnError> {
    request.validate().map_err(ServerFnError::new)?;
    Err(ServerFnError::new("create_topic requires authenticated AdminFacade execution"))
}

#[server]
pub async fn upsert_scram_sha512_user(
    request: ScramUserUpsertDto,
) -> Result<Vec<ResourceOutcome>, ServerFnError> {
    request.validate().map_err(ServerFnError::new)?;
    Err(ServerFnError::new("upsert_scram_sha512_user requires authenticated AdminFacade execution"))
}
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p crabka-admin-ui --test admin_mapping`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/src/dto.rs crates/admin-ui/src/server_fns.rs crates/admin-ui/tests/admin_mapping.rs
git commit -m "feat: add admin UI mutation DTOs"
```

---

### Task 10: Add high-value Playwright E2E scaffold

**Files:**
- Test: `crates/admin-ui/tests/e2e.rs`
- Modify: `crates/admin-ui/Cargo.toml`

**Interfaces:**
- Produces ignored E2E tests that run against `CRABKA_ADMIN_UI_E2E_URL`.
- Does not require launching browsers during normal `cargo test -p crabka-admin-ui`.

- [ ] **Step 1: Add ignored Playwright test**

Create `crates/admin-ui/tests/e2e.rs`:

```rust
#[tokio::test]
#[ignore = "requires CRABKA_ADMIN_UI_E2E_URL and installed Playwright browsers"]
async fn login_page_renders() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = std::env::var("CRABKA_ADMIN_UI_E2E_URL")?;

    let playwright = playwright_rs::Playwright::initialize().await?;
    playwright.prepare()?;
    let chromium = playwright.chromium();
    let browser = chromium.launcher().headless(true).launch().await?;
    let context = browser.context_builder().build().await?;
    let page = context.new_page().await?;

    page.goto(&format!("{base_url}/login")).await?;
    let title = page.locator("text=Sign in to Crabka").await?;
    assert!(title.count().await? >= 1);

    browser.close().await?;
    Ok(())
}
```

- [ ] **Step 2: Verify ignored test compiles but does not run by default**

Run: `cargo test -p crabka-admin-ui --test e2e`

Expected: PASS with one ignored test.

Run: `cargo test -p crabka-admin-ui --test e2e -- --ignored`

Expected without `CRABKA_ADMIN_UI_E2E_URL`: FAIL with missing environment variable. This confirms the ignored E2E path is gated.

- [ ] **Step 3: Commit**

```bash
cargo +nightly fmt -p crabka-admin-ui
git add crates/admin-ui/Cargo.toml crates/admin-ui/tests/e2e.rs
git commit -m "test: add admin UI playwright scaffold"
```

---

### Task 11: Wire runnable Dioxus server and final targeted verification

**Files:**
- Modify: `crates/admin-ui/src/server.rs`
- Modify: `crates/admin-ui/src/main.rs`
- Modify: `crates/admin-ui/src/lib.rs`
- Test: `crates/admin-ui/tests/smoke.rs`

**Interfaces:**
- Produces a runnable `crabka-admin-ui` binary serving `/healthz` and the Dioxus app.
- Keeps admin server independent from broker and gateway HTTP servers.

- [ ] **Step 1: Extend smoke test to verify app route returns HTML**

Append to `crates/admin-ui/tests/smoke.rs`:

```rust
#[tokio::test]
async fn root_returns_html() {
    let cfg = crabka_admin_ui::config::AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        ..crabka_admin_ui::config::AdminUiConfig::default()
    };
    let app = crabka_admin_ui::server::router(crabka_admin_ui::server::AppState::new(cfg));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds");

    assert_eq!(response.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run smoke test to verify failure**

Run: `cargo test -p crabka-admin-ui --test smoke`

Expected: FAIL because `server::router` does not exist.

- [ ] **Step 3: Implement router**

Modify `crates/admin-ui/src/server.rs`:

```rust
pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(health_router())
        .route("/", get(|| async { axum::response::Html("<!doctype html><title>Crabka Admin</title><main id=\"main\">Crabka Admin</main>") }))
        .with_state(state)
}
```

This HTML route is the first runnable server checkpoint for `/healthz` plus browser navigation. The same task keeps the Dioxus component tree compile-checked through `crabka_admin_ui::app()`; subsequent implementation work should replace the HTML response with Dioxus fullstack rendering before treating the UI as complete.

Modify `crates/admin-ui/src/main.rs` to serve `server::router`:

```rust
use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = crabka_admin_ui::config::AdminUiConfig::from_env()
        .context("load admin UI config")?;
    let state = crabka_admin_ui::server::AppState::new(cfg.clone());

    let listener = tokio::net::TcpListener::bind(cfg.listen_addr)
        .await
        .context("bind admin UI listener")?;
    let bound = listener.local_addr().context("read admin UI listener addr")?;
    tracing::info!(%bound, cluster = %cfg.cluster_name, "crabka admin UI listening");

    axum::serve(listener, crabka_admin_ui::server::router(state))
        .await
        .context("serve admin UI")
}
```

- [ ] **Step 4: Run targeted verification**

Run: `cargo test -p crabka-admin-ui`

Expected: PASS, with `e2e.rs` ignored by default.

Run: `cargo build -p crabka-admin-ui`

Expected: PASS.

Run: `cargo clippy -p crabka-admin-ui --all-targets -- -D warnings`

Expected: PASS.

Run: `cargo +nightly fmt -p crabka-admin-ui --check`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/admin-ui
git commit -m "feat: serve standalone admin UI"
```

---

## Final Verification

Run these at the end of the plan:

```bash
cargo test -p crabka-admin-ui
cargo build -p crabka-admin-ui
cargo clippy -p crabka-admin-ui --all-targets -- -D warnings
cargo +nightly fmt -p crabka-admin-ui --check
```

Expected: all pass, with Playwright E2E ignored by default.

Do not claim workspace-wide `cargo test` is clean unless the baseline failures described above have been fixed and re-run successfully.

---

## Spec Coverage Self-Review

- Standalone crate/binary: Task 1 and Task 11.
- One configured cluster: Task 2 config model.
- Broker-backed SCRAM-SHA-512 login: Task 4.
- ACL-derived UI capabilities: Task 5.
- Admin surfaces for topics/configs, groups, ACLs, SCRAM users, quotas, log dirs: Tasks 5, 8, and 9.
- Structured broker errors and per-resource outcomes: Task 3 and Task 9.
- Operations-sidebar layout: Task 7.
- `playwright-rs` E2E scaffold: Task 10.
- Targeted testing and baseline caveat: Baseline Note, Task 10, Task 11, Final Verification.

No multi-cluster, non-SCRAM mechanisms, static UI users, gateway mounting, broker mounting, public REST API, or metrics dashboard work is included.
