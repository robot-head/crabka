# Crabka gRPC Gateway P4 — TLS / mTLS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Serve the gateway's Connect/HTTP listener over rustls (config-gated, hot cert reload), with optional mTLS (Disabled/Optional/Required); extract the mTLS peer principal into request extensions; and secure the P3b gateway→gateway forward channel with mTLS (https + the gateway's own cert as client identity), requiring a cert-authenticated peer on `/internal/v1/forward`. (User chose "Full TLS everywhere".)

**Architecture:** A manual `tokio_rustls::TlsAcceptor` accept loop (per-connection spawn) drives the existing `axum::Router` over each `TlsStream` via `hyper::server::conn::http1::serve_connection`; the plaintext path stays on `axum::serve` (unchanged ⇒ all existing tests pass). TLS material comes from `crabka-security` (`TlsConfig::build_server_config` + `DynamicServerConfig` ArcSwap hot reload + `extract_principal_from_cert`). The forwarder builds a reqwest client from a rustls `ClientConfig` carrying the gateway's client identity (new `crabka-security` method) and dials `https://`.

**Tech Stack:** rustls 0.23 (ring), tokio-rustls 0.26, `hyper` 1 (`server`,`http1`) + `hyper-util` 0.1 (`tokio`) to serve axum over a TLS stream, `tower` (`util`) for `ServiceExt::oneshot`, reqwest 0.13 (`rustls`) preconfigured-TLS for the forwarder, `crabka-security` (TLS config, reload, principal, `ca` cert-gen in tests). **Broker is NEVER modified** (`crabka-security` is a separate crate and may gain an additive method).

**Out of scope (P5):** ACL evaluation / authorization decisions, principal allow-listing beyond "a cert-authenticated peer", on-behalf-of auditing, identity forwarding of the *caller* principal. P4 establishes the transport + the principal seam only.

---

## Execution constraints (every task)

- **Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25`. Subagent shells reset cwd to the MAIN repo — prefix every Bash with `cd /Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25 && ...`, use `git -C <worktree>`.
- **Branch:** `claude/gateway-p4`, **stacked on `claude/gateway-p3b`** (P3b, PR #403 — unmerged; P4 modifies P3b's `forward.rs`/`bin`/`config`). PR bases on #403, or rebases onto `main` if #403 merges first. Assert `git -C <worktree> rev-parse --abbrev-ref HEAD` == `claude/gateway-p4` before every commit (else STOP → BLOCKED).
- **Git identity:** `git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit ...` (never `git config`). Stage `Cargo.lock` when deps change.
- **Each task ends GREEN:** `cargo test -p crabka-grpc-gateway` (+ `-p crabka-security` for Task 1), `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`, `cargo fmt --check -p crabka-grpc-gateway`.

## Confirmed external APIs (rely on these — investigated)

- `crabka_security::TlsConfig { cert_chain_path, private_key_path, trust_roots_path: Option<PathBuf>, client_ca_path: Option<PathBuf>, client_auth: ClientAuthMode }`; `build_server_config(&self) -> Result<Arc<rustls::ServerConfig>, TlsError>`; `build_client_config(&self) -> Result<Arc<rustls::ClientConfig>, TlsError>` (no client auth — Task 1 adds the with-identity variant).
- `crabka_security::ClientAuthMode { Disabled, Optional, Required }` (Default = Disabled).
- `crabka_security::reload::DynamicServerConfig::{from_tls_config(&TlsConfig) -> Result<Arc<Self>,TlsError>, current() -> Arc<rustls::ServerConfig>, reload_from(&TlsConfig) -> Result<(),TlsError>}` (re-exported? use `crabka_security::reload::DynamicServerConfig` or check `crabka_security::DynamicServerConfig`).
- `crabka_security::extract_principal_from_cert(&[u8]) -> Option<String>`; `crabka_security::Principal { name: String, auth_method: AuthMethod, groups: Vec<String> }`; `crabka_security::AuthMethod::MTls`.
- `crabka_security::ca::{generate_clients_ca(cn,&days) -> CaMaterial{cert_pem,key_pem}, issue_broker_cert(ca_cert,ca_key,cn,&[SubjectAltName],&[SubjectAltName],days) -> BrokerCert{cert_pem,key_pem,..}, issue_user_cert(ca_cert,ca_key,cn,days) -> UserCert{cert_pem,key_pem,..}}`; `SubjectAltName::{Dns(String),Ip(IpAddr)}`.
- `tokio_rustls::TlsAcceptor::from(Arc<rustls::ServerConfig>)`; `acceptor.accept(tcp).await -> io::Result<TlsStream<TcpStream>>`; `tls.get_ref() -> (&TcpStream, &rustls::ServerConnection)`; `server_conn.peer_certificates() -> Option<&[CertificateDer]>`.
- axum 0.8 `Router: Service<Request<hyper::body::Incoming>>` (so `router.clone().oneshot(req)` works directly — no body mapping). `tower::ServiceExt::oneshot`.
- `hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(tls), svc)` where `svc = hyper::service::service_fn(...)`. `hyper_util::rt::TokioIo`.
- reqwest 0.13 with `rustls` feature: `ClientBuilder::use_preconfigured_tls(impl Any)` downcasts `Option<rustls::ClientConfig>` ⇒ pass `Some(client_config)`. (If that signature mismatches, fall back to `reqwest::Identity::from_pem(cert+key)` + `reqwest::Certificate::from_pem(ca)` + `.identity(..).add_root_certificate(..)`.)
- `rustls::ClientConfig::builder().with_root_certificates(roots).with_client_auth_cert(cert_chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Result<ClientConfig, rustls::Error>`.
- **Crypto provider:** call `rustls::crypto::ring::default_provider().install_default().ok();` once at process startup (bin `main`) and once per test binary before building any TLS config.

## File map

- Modify `crates/security/src/tls.rs` — add `build_client_config_with_identity` (+ unit test). [Task 1]
- Modify `crates/grpc-gateway/Cargo.toml` — add `hyper`, `hyper-util`, `tower(util)`, `rustls`, `tokio-rustls`, `crabka-security`. [Task 2]
- Modify `crates/grpc-gateway/src/config.rs` — `tls: Option<TlsSettings>` + `TlsSettings` + `to_security()`. [Task 2]
- Update `tests/wire.rs`, `tests/streaming.rs`, `tests/forwarding.rs` `GatewayConfig` literals (`tls: None`). [Task 2]
- Create `crates/grpc-gateway/src/serve.rs` — `serve`, `serve_tls`, `peer_principal`, `spawn_tls_reload`. Modify `src/lib.rs` (`pub mod serve;`). [Task 3]
- Modify `crates/grpc-gateway/src/forward.rs` — `Forwarder` https/mTLS + `forward_handler` principal gate. [Task 4]
- Modify `crates/grpc-gateway/src/bin/gateway.rs` — TLS CLI args, build/reload, serve via `serve::serve`, forwarder client config, provider install. [Task 5]
- Create `crates/grpc-gateway/tests/tls.rs` — server TLS + mTLS reject + principal + TLS forward end-to-end. [Task 6]

## Batches (sequential deps; parallel where file sets are disjoint)

- **Batch A:** Task 1 (security crate) ∥ Task 2 (gateway deps+config). Disjoint crates.
- **Batch B:** Task 3 (`serve.rs`) ∥ Task 4 (`forward.rs`). Disjoint files; both need Task 2; Task 4 needs Task 1.
- **Batch C:** Task 5 (bin) — needs Tasks 3 + 4.
- **Batch D:** Task 6 (tests) — needs Task 5.

---

## Task 1: `crabka-security` — client config with identity

**Files:** Modify `crates/security/src/tls.rs`.

- [ ] **Step 1: Add the method** to `impl TlsConfig`, after `build_client_config`:

```rust
    /// Build a rustls `ClientConfig` that BOTH verifies the peer's server cert
    /// against `trust_roots_path` AND presents this node's own
    /// `cert_chain_path`/`private_key_path` as a client certificate (mTLS).
    /// Used by peer-to-peer dialers (e.g. the gRPC gateway forwarding to an
    /// owning replica) that must mutually authenticate.
    ///
    /// # Errors
    /// Propagates `TlsError` from cert/key loading or rustls config building.
    pub fn build_client_config_with_identity(&self) -> Result<Arc<rustls::ClientConfig>, TlsError> {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(path) = &self.trust_roots_path {
            for cert in load_certs(path)? {
                roots.add(cert)?;
            }
        }
        let certs = load_certs(&self.cert_chain_path)?;
        let key = load_private_key(&self.private_key_path)?;
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .map_err(TlsError::Rustls)?;
        Ok(Arc::new(cfg))
    }
```

(`load_certs`, `load_private_key`, `TlsError::Rustls` already exist in this file.)

- [ ] **Step 2: Add a unit test** in the existing `#[cfg(test)] mod tests` block. It generates a CA + a leaf via the crate's own `ca` module, writes them, and asserts the client config builds:

```rust
    #[test]
    fn client_config_with_identity_builds() {
        install_provider();
        let dir = tempfile::tempdir().unwrap();
        let ca = crate::ca::generate_clients_ca("p4-ca", 365).expect("ca");
        let leaf = crate::ca::issue_user_cert(&ca.cert_pem, &ca.key_pem, "gw", 365).expect("leaf");
        let cert_path = dir.path().join("c.pem");
        let key_path = dir.path().join("k.pem");
        let ca_path = dir.path().join("ca.pem");
        File::create(&cert_path).unwrap().write_all(leaf.cert_pem.as_bytes()).unwrap();
        File::create(&key_path).unwrap().write_all(leaf.key_pem.as_bytes()).unwrap();
        File::create(&ca_path).unwrap().write_all(ca.cert_pem.as_bytes()).unwrap();
        let cfg = TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: Some(ca_path),
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        };
        cfg.build_client_config_with_identity().expect("client cfg with identity");
    }
```

- [ ] **Step 3: Gates + commit.** `cargo test -p crabka-security`, `cargo clippy -p crabka-security --all-targets -- -D warnings`, `cargo fmt --check -p crabka-security`. Commit `feat(security): TlsConfig::build_client_config_with_identity for mTLS dialers`. Stage `crates/security/src/tls.rs`.

---

## Task 2: Gateway deps + TLS config

**Files:** Modify `crates/grpc-gateway/Cargo.toml`, `crates/grpc-gateway/src/config.rs`, `crates/grpc-gateway/tests/wire.rs`, `crates/grpc-gateway/tests/streaming.rs`, `crates/grpc-gateway/tests/forwarding.rs`.

- [ ] **Step 1: Cargo.toml deps.** Add to `[dependencies]`:

```toml
crabka-security = { version = "0.2", path = "../security" }
rustls = { workspace = true }
tokio-rustls = { workspace = true }
hyper = { version = "1", features = ["server", "http1"] }
hyper-util = { version = "0.1", features = ["tokio"] }
tower = { workspace = true, features = ["util"] }
```

(`rustls`/`tokio-rustls`/`tower` are workspace deps. `hyper`/`hyper-util` aren't — pin directly. `tower` workspace entry has no features, so the `features=["util"]` here adds `ServiceExt::oneshot`.)

- [ ] **Step 2: `config.rs` — TLS settings.** Add imports + a `TlsSettings` struct + an `Option<TlsSettings>` field on `GatewayConfig`. Re-export `ClientAuthMode`.

```rust
use std::path::PathBuf;
// (keep existing `use std::net::SocketAddr;`)

pub use crabka_security::ClientAuthMode;

/// TLS / mTLS settings for the gateway listener and the forward channel.
/// Present ⇒ the gateway serves over rustls; absent ⇒ plaintext.
#[derive(Debug, Clone)]
pub struct TlsSettings {
    /// Server cert chain (PEM). Doubles as the gateway's client identity when
    /// forwarding (the cert is issued with server+client EKU).
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    /// CA(s) the forwarder trusts when verifying a peer gateway's server cert.
    pub trust_roots_path: Option<PathBuf>,
    /// CA(s) used to verify incoming client certs (mTLS). Required if
    /// `client_auth != Disabled`.
    pub client_ca_path: Option<PathBuf>,
    pub client_auth: ClientAuthMode,
    /// Cert hot-reload poll interval (seconds).
    pub reload_interval_secs: u64,
}

impl TlsSettings {
    /// Map to the `crabka-security` config used to build server/client configs.
    #[must_use]
    pub fn to_security(&self) -> crabka_security::TlsConfig {
        crabka_security::TlsConfig {
            cert_chain_path: self.cert_chain_path.clone(),
            private_key_path: self.private_key_path.clone(),
            trust_roots_path: self.trust_roots_path.clone(),
            client_ca_path: self.client_ca_path.clone(),
            client_auth: self.client_auth,
        }
    }
}
```

Add to `GatewayConfig` (after `membership_topic`):

```rust
    /// TLS/mTLS settings; `None` ⇒ plaintext (all current tests).
    pub tls: Option<TlsSettings>,
```

- [ ] **Step 3: Fix the `GatewayConfig` literals** in `tests/wire.rs`, `tests/streaming.rs`, `tests/forwarding.rs` — add `tls: None,` to each. (Grep `GatewayConfig {` to find all; there are three test literals plus the bin, which Task 5 handles.)

- [ ] **Step 4: Gates + commit.** `cargo build -p crabka-grpc-gateway` (pulls hyper/hyper-util), full gateway test + clippy + fmt. Commit `feat(gateway): TLS deps + GatewayConfig.tls settings`. Stage the Cargo.toml, config.rs, the 3 test files, and `Cargo.lock`.

---

## Task 3: TLS serving module (the crux)

**Files:** Create `crates/grpc-gateway/src/serve.rs`; modify `crates/grpc-gateway/src/lib.rs` (`pub mod serve;`).

- [ ] **Step 1: Create `src/serve.rs`:**

```rust
//! Listener serving: plaintext via `axum::serve`, or rustls via a manual
//! accept loop that hands each `TlsStream` to hyper and injects the mTLS peer
//! principal (cert subject DN) into request extensions. TLS material is hot-
//! reloadable (`DynamicServerConfig`); the plaintext path is unchanged from
//! pre-P4 so existing tests are unaffected.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crabka_security::reload::DynamicServerConfig;
use crabka_security::{AuthMethod, Principal, TlsConfig};

/// Serve `app` on `listener`. With `tls = Some(..)`, terminate rustls per
/// connection; otherwise serve plaintext. Returns when `shutdown` is cancelled.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    tls: Option<Arc<DynamicServerConfig>>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    match tls {
        None => {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await
        }
        Some(dynamic) => serve_tls(listener, app, dynamic, shutdown).await,
    }
}

async fn serve_tls(
    listener: TcpListener,
    app: Router,
    dynamic: Arc<DynamicServerConfig>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    loop {
        let (tcp, peer) = tokio::select! {
            () = shutdown.cancelled() => break,
            res = listener.accept() => match res {
                Ok(v) => v,
                Err(e) => { tracing::warn!(error = %e, "tcp accept failed"); continue; }
            },
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(dynamic.current());
        let app = app.clone();
        tokio::spawn(async move {
            let tls = match acceptor.accept(tcp).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(error = %e, %peer, "tls handshake failed");
                    return;
                }
            };
            let principal = peer_principal(&tls);
            let io = TokioIo::new(tls);
            let svc = hyper::service::service_fn(move |mut req: hyper::Request<hyper::body::Incoming>| {
                let app = app.clone();
                let principal = principal.clone();
                async move {
                    if let Some(p) = principal {
                        req.extensions_mut().insert(p);
                    }
                    app.oneshot(req).await
                }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(error = %e, "tls connection error");
            }
        });
    }
    Ok(())
}

/// Extract the mTLS peer principal (cert subject DN) after a handshake.
fn peer_principal(tls: &tokio_rustls::server::TlsStream<TcpStream>) -> Option<Principal> {
    let (_, conn) = tls.get_ref();
    let cert = conn.peer_certificates()?.first()?;
    let name = crabka_security::extract_principal_from_cert(cert.as_ref())?;
    Some(Principal { name, auth_method: AuthMethod::MTls, groups: vec![] })
}

/// Build the hot-reloadable server config + spawn the reload watcher. Returns
/// the dynamic config to pass to [`serve`].
///
/// # Errors
/// Propagates `crabka_security::TlsError` if the initial config fails to build.
pub fn build_and_watch_tls(
    cfg: TlsConfig,
    reload_interval_secs: u64,
    shutdown: CancellationToken,
) -> Result<Arc<DynamicServerConfig>, crabka_security::TlsError> {
    let dynamic = DynamicServerConfig::from_tls_config(&cfg)?;
    let watch = dynamic.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(reload_interval_secs.max(1)));
        ticker.tick().await; // skip the immediate first tick (already loaded)
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = shutdown.cancelled() => return,
            }
            if let Err(e) = watch.reload_from(&cfg) {
                tracing::warn!(error = %e, "tls reload failed; keeping prior config");
            }
        }
    });
    Ok(dynamic)
}
```

> VERIFY against the installed crates while implementing: (a) the `DynamicServerConfig` path — it may be `crabka_security::DynamicServerConfig` (re-export) rather than `crabka_security::reload::DynamicServerConfig`; use whichever the compiler accepts. (b) `app.oneshot(req)` requires `Router: Service<Request<Incoming>>` — confirmed for axum 0.8; if the body type mismatches, map with `req.map(axum::body::Body::new)` before `oneshot`. (c) `hyper::service::service_fn` + `http1::Builder::serve_connection` match the axum `low-level-rustls` example.

- [ ] **Step 2: `lib.rs`** — add `pub mod serve;`.

- [ ] **Step 3: Gates + commit.** `cargo build -p crabka-grpc-gateway` + full gates (the module is unused until Task 5 — `pub` items, no dead-code warnings; if clippy flags an unused private item, that's a real bug — fix it). Commit `feat(gateway): rustls serve loop + mTLS principal injection + hot reload`. Stage `src/serve.rs`, `src/lib.rs`.

---

## Task 4: Forward channel mTLS

**Files:** Modify `crates/grpc-gateway/src/forward.rs`.

- [ ] **Step 1: Forwarder over https with a client identity.** Change `Forwarder` to carry a scheme + a reqwest client built (optionally) with a preconfigured rustls client config:

```rust
pub struct Forwarder {
    http: reqwest::Client,
    scheme: &'static str,
}

impl Forwarder {
    /// Plaintext forwarder (http://). Used when the gateway runs without TLS.
    #[must_use]
    pub fn new() -> Self {
        Self { http: reqwest::Client::new(), scheme: "http" }
    }

    /// mTLS forwarder (https://) presenting `client_config`'s identity and
    /// trusting its roots — the gateway's own cert authenticates it to the
    /// owning replica.
    ///
    /// # Errors
    /// Returns `GatewayError::Forward` if the reqwest client cannot be built.
    pub fn with_tls(client_config: std::sync::Arc<rustls::ClientConfig>) -> Result<Self, GatewayError> {
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(Some((*client_config).clone()))
            .build()
            .map_err(|e| GatewayError::Forward(format!("build tls forward client: {e}")))?;
        Ok(Self { http, scheme: "https" })
    }
    // ... `forward` unchanged except the URL scheme:
    //     let url = format!("{}://{}/internal/v1/forward", self.scheme, owner_addr);
}
```

> VERIFY: `use_preconfigured_tls(Some(cfg))` — reqwest 0.13's `__rustls` impl downcasts `Option<rustls::ClientConfig>`, so pass `Some(..)`. If it doesn't compile, try `.use_preconfigured_tls((*client_config).clone())`, or the `Identity`/`Certificate` fallback (build `reqwest::Identity::from_pem` from cert+key bytes and `reqwest::Certificate::from_pem` from the CA, then `.identity(id).add_root_certificate(ca)`). `rustls::ClientConfig` is `Clone`.

Update `forward` to use `self.scheme` in the URL (the only change to that method).

- [ ] **Step 2: Require a cert-authed peer on the internal endpoint.** Change `forward_handler` to read the injected principal and reject anonymous callers when the gateway runs with TLS. Change its return type to `axum::response::Response`:

```rust
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

async fn forward_handler(
    Extension(state): Extension<Arc<AppState>>,
    principal: Option<Extension<crabka_security::Principal>>,
    Json(req): Json<ForwardRecord>,
) -> Response {
    // When TLS is configured, the internal forward endpoint only accepts a
    // cert-authenticated peer (an mTLS principal must be present). Plaintext
    // mode (no TLS) skips this so existing non-TLS forwarding still works.
    if state.config.tls.is_some() && principal.is_none() {
        return (
            StatusCode::FORBIDDEN,
            Json(ForwardResult {
                partition: -1,
                offset: -1,
                deduplicated: false,
                error: Some(ForwardError {
                    message: "forward requires an authenticated mTLS peer".into(),
                    retriable: false,
                }),
            }),
        )
            .into_response();
    }
    let rec = req.into_record();
    match state.produce.produce_local(rec).await {
        Ok(o) => Json(ForwardResult { partition: o.partition, offset: o.offset, deduplicated: o.deduplicated, error: None }).into_response(),
        Err(e) => {
            let retriable = matches!(e, GatewayError::Unavailable);
            Json(ForwardResult {
                partition: -1, offset: -1, deduplicated: false,
                error: Some(ForwardError { message: e.to_string(), retriable }),
            })
            .into_response()
        }
    }
}
```

> VERIFY: `crabka_security::Principal` must be `Clone + Send + Sync + 'static` for `Extension<Principal>` (it is a plain struct — confirm it derives `Clone`). The forwarder's `Forwarder::forward` maps an HTTP 403 (non-success status) to `GatewayError::Unavailable` already, so a rejected forward surfaces as retriable — acceptable.

- [ ] **Step 3: Gates + commit.** Full gateway gates (existing plaintext forwarding tests still pass: `tls: None` ⇒ no principal required; `Forwarder::new()` still used until Task 5 wires `with_tls`). Commit `feat(gateway): forward over mTLS + require authed peer on /internal/forward`. Stage `src/forward.rs`.

---

## Task 5: Binary wiring

**Files:** Modify `crates/grpc-gateway/src/bin/gateway.rs`.

- [ ] **Step 1: Install the crypto provider** at the very top of `main` (before any TLS work):

```rust
    rustls::crypto::ring::default_provider().install_default().ok();
```

- [ ] **Step 2: CLI args** on `struct Args` (after `membership_topic`):

```rust
    /// Server cert chain (PEM). Enables TLS when set together with --tls-key.
    #[arg(long, env = "CRABKA_GATEWAY_TLS_CERT")]
    tls_cert: Option<std::path::PathBuf>,
    /// Server private key (PEM).
    #[arg(long, env = "CRABKA_GATEWAY_TLS_KEY")]
    tls_key: Option<std::path::PathBuf>,
    /// CA(s) used to verify incoming client certs (mTLS). Required if --tls-client-auth != disabled.
    #[arg(long, env = "CRABKA_GATEWAY_TLS_CLIENT_CA")]
    tls_client_ca: Option<std::path::PathBuf>,
    /// Client-cert mode: disabled | optional | required.
    #[arg(long, env = "CRABKA_GATEWAY_TLS_CLIENT_AUTH", default_value = "disabled")]
    tls_client_auth: String,
    /// CA(s) the forwarder trusts for peer gateway server certs (defaults to --tls-client-ca).
    #[arg(long, env = "CRABKA_GATEWAY_TLS_TRUST_ROOTS")]
    tls_trust_roots: Option<std::path::PathBuf>,
    /// Cert hot-reload poll interval (seconds).
    #[arg(long, env = "CRABKA_GATEWAY_TLS_RELOAD_SECS", default_value_t = 30)]
    tls_reload_secs: u64,
```

- [ ] **Step 3: Build `TlsSettings`** in `main` and put it on the config. After parsing `args`, before constructing `GatewayConfig`:

```rust
    let tls = match (args.tls_cert.clone(), args.tls_key.clone()) {
        (Some(cert_chain_path), Some(private_key_path)) => {
            let client_auth = match args.tls_client_auth.as_str() {
                "disabled" => crabka_grpc_gateway::config::ClientAuthMode::Disabled,
                "optional" => crabka_grpc_gateway::config::ClientAuthMode::Optional,
                "required" => crabka_grpc_gateway::config::ClientAuthMode::Required,
                other => anyhow::bail!("invalid --tls-client-auth: {other}"),
            };
            Some(crabka_grpc_gateway::config::TlsSettings {
                cert_chain_path,
                private_key_path,
                trust_roots_path: args.tls_trust_roots.clone().or_else(|| args.tls_client_ca.clone()),
                client_ca_path: args.tls_client_ca.clone(),
                client_auth,
                reload_interval_secs: args.tls_reload_secs,
            })
        }
        (None, None) => None,
        _ => anyhow::bail!("--tls-cert and --tls-key must be set together"),
    };
```

Add `tls: tls.clone(),` to the `GatewayConfig { ... }` construction.

- [ ] **Step 4: Build the forwarder + dynamic server config from `config.tls`.** In `run`, replace the `Forwarder::new()` construction:

```rust
    let forwarder = match config.tls.as_ref() {
        Some(t) => {
            let client_cfg = t
                .to_security()
                .build_client_config_with_identity()
                .map_err(|e| anyhow::anyhow!("build forward client tls: {e}"))?;
            std::sync::Arc::new(forward::Forwarder::with_tls(client_cfg)?)
        }
        None => std::sync::Arc::new(forward::Forwarder::new()),
    };
```

and pass `forwarder` into `.with_forwarding(membership.clone(), forwarder, config.advertised_addr.clone())`.

- [ ] **Step 5: Serve via `serve::serve`.** Replace the `axum::serve(listener, app).with_graceful_shutdown(...)` block. First spawn a ctrl-c → cancel task, then build the optional dynamic TLS config, then serve:

```rust
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown.cancel();
        });
    }

    let tls_dynamic = match config.tls.as_ref() {
        Some(t) => Some(
            crabka_grpc_gateway::serve::build_and_watch_tls(
                t.to_security(),
                t.reload_interval_secs,
                shutdown.clone(),
            )
            .map_err(|e| anyhow::anyhow!("build tls server config: {e}"))?,
        ),
        None => None,
    };

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %listener.local_addr()?, tls = tls_dynamic.is_some(), "gateway listening");
    crabka_grpc_gateway::serve::serve(listener, app, tls_dynamic, shutdown).await?;
    Ok(())
```

(Remove the old `axum::serve(...).with_graceful_shutdown(...)`. `app` is built just above as before. Ensure `forward::Forwarder` import covers `with_tls`.)

- [ ] **Step 6: Gates + commit.** `cargo build -p crabka-grpc-gateway` (bin compiles), full gates. Commit `feat(gateway): wire TLS/mTLS listener + reload + mTLS forwarder into the binary`. Stage `src/bin/gateway.rs`.

---

## Task 6: TLS integration tests

**Files:** Create `crates/grpc-gateway/tests/tls.rs`.

- [ ] **Step 1: Create the test file.** Generate a CA + gateway server/client cert + a client cert at runtime, write to a `TempDir`, and exercise: (1) plaintext still works (sanity, optional), (2) a reqwest https client with the gateway's CA reaches `/healthz` over TLS, (3) mTLS `Required` rejects a client with no cert, (4) the mTLS peer principal is injected (assert via a forward or a small check), (5) two TLS+mTLS gateways forward over mTLS end-to-end.

```rust
//! TLS / mTLS: the gateway serves the listener over rustls; mTLS `Required`
//! rejects un-certed clients; the mTLS peer principal is extracted; and two
//! TLS gateways forward over mutually-authenticated https. Certs are generated
//! at runtime via `crabka_security::ca` (no fixtures). Pure 127.0.0.1 — no Docker.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::{ClientAuthMode, GatewayConfig, TlsSettings};
use crabka_grpc_gateway::dedup::membership::{MembershipPublisher, MembershipStore};
use crabka_grpc_gateway::dedup::store::DedupStore;
use crabka_grpc_gateway::dedup::topic::{ensure_dedup_topic, ensure_membership_topic};
use crabka_grpc_gateway::dedup::{partition_for, DedupEngine};
use crabka_grpc_gateway::forward::{self, Forwarder};
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::serve;
use crabka_grpc_gateway::state::AppState;
use crabka_grpc_gateway::types::GatewayRecord;
use crabka_security::ca::{generate_clients_ca, issue_broker_cert, issue_user_cert, SubjectAltName};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn write(dir: &std::path::Path, name: &str, pem: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, pem).unwrap();
    p
}

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Generate CA + a gateway server/client cert (SAN 127.0.0.1) + a standalone
/// client cert, all chaining to one CA. Returns paths in `dir`.
struct Certs { ca: PathBuf, gw_cert: PathBuf, gw_key: PathBuf, client_cert: PathBuf, client_key: PathBuf }

fn gen_certs(dir: &std::path::Path) -> Certs {
    let ca = generate_clients_ca("p4-ca", 365).unwrap();
    let sans = vec![SubjectAltName::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)), SubjectAltName::Dns("localhost".into())];
    let gw = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "gateway", &sans, &[], 365).unwrap();
    let client = issue_user_cert(&ca.cert_pem, &ca.key_pem, "peer-gateway", 365).unwrap();
    Certs {
        ca: write(dir, "ca.pem", &ca.cert_pem),
        gw_cert: write(dir, "gw_cert.pem", &gw.cert_pem),
        gw_key: write(dir, "gw_key.pem", &gw.key_pem),
        client_cert: write(dir, "client_cert.pem", &client.cert_pem),
        client_key: write(dir, "client_key.pem", &client.key_pem),
    }
}
```

Then implement (the implementer writes the remaining helper + 2-3 `#[tokio::test(flavor="multi_thread", worker_threads=4..6)]` tests):

- **`server_tls_handshake_and_health`:** spin one gateway (build `AppState` like `tests/forwarding.rs`'s `spawn_gateway`, but bind a listener, build `serve::build_and_watch_tls` from a `TlsSettings { client_auth: Optional, client_ca: ca, ... }`, and `tokio::spawn(serve::serve(listener, app, Some(dynamic), token))`). Build a reqwest client with `reqwest::Certificate::from_pem(ca_pem)` as root + `.tls_built_in_root_certs(false)`; GET `https://127.0.0.1:{port}/healthz` ⇒ 200. (Wait briefly for readiness or just hit /healthz which is always 200.)
- **`mtls_required_rejects_no_client_cert`:** serve with `client_auth: Required`; a reqwest client WITHOUT an identity (only the CA root) → the request FAILS (handshake/connection error). Assert `result.is_err()`.
- **`tls_forward_between_two_gateways`:** the end-to-end proof. Two gateways, each: `TlsSettings { client_auth: Required, cert/key = gw cert, client_ca = ca, trust_roots = ca }`, `Forwarder::with_tls(to_security().build_client_config_with_identity())`, served over TLS via `serve::serve`. advertised_addr = `127.0.0.1:{port}`. Wait for the ownership split + membership routing (as in `tests/forwarding.rs`), pick a key owned by B, submit through **A**'s `produce` ⇒ A forwards to B over **mTLS https** ⇒ produced once; resend ⇒ deduplicated. Assert exactly one record in the user topic. (This proves the forward client identity + the server mTLS verification + the principal gate all line up.)

Key construction notes for the implementer:
- Call `install_provider()` at the start of every test.
- The gateway's own cert (`issue_broker_cert`) carries SAN `127.0.0.1`, so reqwest/rustls server-cert verification passes when dialing `https://127.0.0.1:{port}` (forwarder and the test client).
- For the forward test, build each gateway's `Forwarder` with `with_tls(TlsSettings.to_security().build_client_config_with_identity().unwrap())`; the `AppState.config.tls = Some(settings)` so `/internal/v1/forward` enforces the principal gate (the peer presents the gw client cert ⇒ principal present ⇒ allowed).
- Reuse the `spawn_gateway` structure from `tests/forwarding.rs` (ownership consumer on the shared owners group, membership reader on a unique group, `set_membership` before `run_ownership`), adding the TLS serve + TLS forwarder.
- If timing-flaky on the split/route wait, raise the bound (don't weaken assertions). Re-run 3×.

- [ ] **Step 2: Gates.** `cargo test -p crabka-grpc-gateway --test tls` (PASS, re-run 3×), full suite, clippy `--all-targets -D warnings`, fmt. Add `#[allow(clippy::too_many_lines)]` on long test fns (matches `tests/ownership.rs`).

- [ ] **Step 3: Commit** `test(gateway): TLS handshake, mTLS reject, principal, mTLS forward end-to-end`. Stage `tests/tls.rs`.

---

## Final review + finish

After Task 6: dispatch a final adversarial reviewer over the whole P4 diff (`git diff <base>..claude/gateway-p4`), focusing on: TLS accept-loop correctness (no handshake-in-accept stall — per-connection spawn; graceful shutdown stops accepting), mTLS verification actually enforced (Required rejects no-cert), principal extraction + the `/internal/forward` gate, the forward client presenting its identity + verifying the peer's cert, plaintext path byte-identical to pre-P4 (all old tests green), crypto-provider installed once, no broker changes (only `crabka-security` gained an additive method), no P5 scope creep (no ACL/authz decisions). Address nits, then finish the branch (push + PR stacked on #403 / rebased to main).

## Self-review notes (author)

- **Spec coverage (§9):** rustls listener via crabka-security ✓; hot cert reload ✓; optional mTLS (Disabled/Optional/Required) ✓; cert→principal ✓ (injected into extensions). Config-driven (config-gated; plaintext when unset) ✓. Plus the user-requested forward-channel mTLS ✓.
- **Why manual accept loop:** axum's `Connected`/`IncomingStream` only exposes the addr + a wrapped IO, not the `TlsStream`'s `peer_certificates()`. The manual loop (per-connection spawn, like the broker) is the only way to reach the peer cert; it also keeps the plaintext path on `axum::serve` (zero regression).
- **Greenfield:** no compat shims. `config.tls: Option<_>` is a config seam (plaintext default), not a compat toggle. `crabka-security` gains one additive method (broker untouched).
- **P5 boundary:** P4 injects the principal and gates `/internal/forward` on *presence* of a cert-authed peer; it does NOT evaluate ACLs or allow-list specific principals — that's P5.
