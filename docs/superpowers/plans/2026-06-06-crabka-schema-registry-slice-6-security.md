# Crabka Schema Registry — Slice 6 (Security) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add authentication (Basic + Bearer/OAuth + mTLS), authorization (per-subject Kafka Topic ACLs), server TLS (HTTPS), and SR↔broker client security (SASL/TLS) to the registry — reusing Crabka's security crates, mirroring the grpc-gateway P5 pattern.

**Architecture:** Three axum layers wrap the router — `auth` (resolve a `Principal`), `authz` (map request→ACL, gate, skip trusted forwards), then the slice-5 `forward` layer — so execution is auth→authz→forward→handler. TLS via `crabka_security::TlsConfig`; client security via `client-core::ClientSecurity`. Security is opt-in (no `SecurityConfig` ⇒ today's open HTTP behavior).

**Tech Stack:** Rust 2024; `crabka-authz`, `crabka-security`, `crabka-metadata`, `crabka-client-admin`, `axum` middleware, `tokio-rustls`, `arc-swap`, `base64`, `bcrypt`.

**Spec:** `docs/superpowers/specs/2026-06-06-crabka-schema-registry-slice-6-security-design.md`. Read it.

---

## Verified APIs (grounded in the tree — trust these)

```rust
// crabka_security
pub struct Principal { pub name: String, pub auth_method: AuthMethod, pub groups: Vec<String> }
pub enum AuthMethod { Anonymous, SaslPlain, SaslScramSha256, SaslScramSha512, SaslOAuthBearer, SaslGssapi, MTls }
pub fn extract_principal_from_cert(cert_der: &[u8]) -> Option<String>;     // RFC2253 DN
pub struct TlsConfig { pub cert_chain_path: PathBuf, pub private_key_path: PathBuf,
    pub trust_roots_path: Option<PathBuf>, pub client_ca_path: Option<PathBuf>, pub client_auth: ClientAuthMode }
pub enum ClientAuthMode { Disabled, Optional, Required }
impl TlsConfig { pub fn build_server_config(&self) -> Result<Arc<rustls::ServerConfig>, TlsError>; }
pub enum OAuthBearerValidator { Unsecured(UnsecuredJwsValidator), Signed(SignedJwsValidator), Introspection(IntrospectionValidator) }
impl OAuthBearerValidator { pub async fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError>; }
pub struct AuthOutcome { pub principal: Principal, pub expires_at_ms: Option<i64> }

// crabka_authz
pub struct AuthorizationRequest<'a> { pub principal: &'a Principal, pub host: &'a SocketAddr,
    pub resource_type: ResourceType, pub resource_name: &'a str, pub operation: AclOperation }
pub enum AuthorizationResult { Allow, Deny }
pub trait Authorizer: Send + Sync + std::fmt::Debug { fn authorize(&self, source: &dyn AclSource, req: &AuthorizationRequest<'_>) -> AuthorizationResult; }
pub struct SimpleAclAuthorizer { /* ::new(super_users: HashSet<KafkaPrincipal>) or similar — confirm ctor */ }
pub struct AclCache { /* opaque */ }  impl AclCache { pub fn new(entries: Vec<AclEntry>) -> Self; pub fn default() -> Self; }  // implements AclSource

// crabka_metadata
pub enum ResourceType { Topic, Group, Cluster, TransactionalId }
pub enum AclOperation { All, Read, Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite }

// crabka_client_core
Client::builder().bootstrap(s).client_id(s).maybe_security(Option<ClientSecurity>).build().await  // bon; security is an Option<T> setter
pub struct ClientSecurity { pub protocol: ListenerProtocol, pub tls: Option<TlsConnectorConfig>, pub sasl: Option<SaslCredentials>, pub sasl_host: Option<String> }

// crabka_client_admin
AdminClient::connect_secured(bootstrap: &[String], security: Option<ClientSecurity>) -> Result<AdminClient, _>   // confirm exact arg shape at lib.rs:389
admin.describe_acls(&crabka_client_admin::AclEntryFilter::default()).await -> Result<Vec<AclEntry>, _>

// Reference implementations to MODEL ON (read them):
//   crates/grpc-gateway/src/authz/auth_layer.rs   — Bearer middleware: resolve_principal(req,next), bearer_token(req)
//   crates/grpc-gateway/src/authz/mod.rs           — ArcSwap<AclCache> + run_acl_refresh(refresh, shutdown) polling describe_acls
//   crates/grpc-gateway/src/serve.rs               — TLS accept loop, peer_principal(tls)->Principal, build_and_watch_tls
```

> The two ctors flagged "confirm" (`SimpleAclAuthorizer::new`, `AdminClient::connect_secured` arg shape) MUST be read from source by the implementer before use — `crates/authz/src/simple.rs` and `crates/client-admin/src/lib.rs:383-389`.

## Branch / commit / gate discipline (executors read this)
- Worktree `/Users/mattstone/git/crabka/.claude/worktrees/schema-registry-slice-6`, branch `claude/schema-registry-slice-6` (assert NOT main). Always `git -C <worktree>` and `cd <worktree> && cargo`. Do NOT push.
- Commits: `git -C <worktree> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; body ends `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Per change before commit:** `cargo clippy --workspace --all-targets -- -D warnings` (NOT `-p` — pedantic is workspace-wide; force-relint touched files, the per-target cache masks lints) + `cargo fmt`.
- **`clippy::doc_markdown` windows trap:** any NEW `#[ignore]`/test file with `#![cfg(not(target_os = "windows"))]` MUST put `#![allow(clippy::pedantic)]` + the `#![cfg(...)]` ABOVE the `//!` module docs (else doc_markdown fires un-suppressed on the windows CI runner only). Backtick code-like identifiers in docs.
- New `tests/<x>.rs` files must be added to the `schema-registry-integration` llvm-cov `--test` list in `.github/workflows/ci.yml` (else codecov/patch reports 0% on that code).
- Greenfield (CLAUDE.md): clean signatures, no shims. Security is OPT-IN — every task keeps the no-`SecurityConfig` path == today's behavior, and slices 1–5 tests + compat conformance (Avro 21 / Protobuf 88 / JSON 92) stay green.

## File structure
```
crates/schema-registry/
  Cargo.toml                 # T1: + crabka-authz, crabka-security, crabka-metadata, arc-swap, base64, bcrypt, tokio-rustls
  src/
    config.rs                # T1: SecurityConfig + sub-structs
    auth/mod.rs              # T2: AuthState, resolve_principal(), auth_layer middleware, 401 logic
    auth/basic.rs            # T2: BasicAuthStore (new)
    authz.rs                 # T3: authz_target(), SchemaRegistryAuthz, run_acl_refresh, authz_layer
    rest/mod.rs              # T4: router_with_security()
    rest/serve.rs            # T4: serve_http()/serve_https() (TLS accept + mTLS principal)
    kafkastore/{mod,writer,reader,topic}.rs  # T1: thread Option<ClientSecurity>
    election/client.rs       # T1: thread Option<ClientSecurity>
    election/mod.rs          # T1: Election::start takes security
    lib.rs                   # T2/T3: pub mod auth; pub mod authz;
    bin/schema-registry.rs   # T1+T4: SecurityConfig CLI/env; build layers; serve
  tests/
    security.rs              # T5: in-process broker + ACLs; 401/403/200; forward-authz; TLS
    capture_auth_fixtures.rs # T6: #[ignore] Docker cp BASIC oracle
    fixtures/auth/basic.json
```

## Tasks (sequential; one implementer each; T1→T6)

---

### Task 1 — deps + `SecurityConfig` + client-security passthrough

**Files:** `Cargo.toml`, `src/config.rs`, `src/kafkastore/{mod,writer,reader,topic}.rs`, `src/election/{client,mod}.rs`, `src/bin/schema-registry.rs`; existing `RegistryConfig` literals in `tests/`.

- [ ] **Step 1: deps (`Cargo.toml`).** Add to `[dependencies]`: `crabka-authz`, `crabka-security`, `crabka-metadata` (workspace path deps, version "0.2" matching siblings), `arc-swap = "1"`, `base64 = "0.22"`, `bcrypt = "0.15"`, `tokio-rustls` + `rustls` (match the versions the gateway uses — read `crates/grpc-gateway/Cargo.toml`). `crabka-client-admin` + `crabka-client-core` are already deps. Run `cargo build -p crabka-schema-registry` (compiles).

- [ ] **Step 2: `SecurityConfig` (`config.rs`).** Add the structs + a `pub security: SecurityConfig` field to `RegistryConfig` (default = open):
```rust
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    /// When true, an unauthenticated (Anonymous) request is rejected with 401.
    pub require_auth: bool,
    /// `WWW-Authenticate: Basic realm="<realm>"`.
    pub realm: String,
    pub basic: Option<BasicAuthConfig>,
    pub bearer: Option<BearerAuthConfig>,
    /// Server TLS (HTTPS). None ⇒ plain HTTP.
    pub tls: Option<crabka_security::TlsConfig>,
    pub authz: Option<AuthzConfig>,
    /// SR↔broker Kafka-client security. None ⇒ PLAINTEXT.
    pub client: Option<crabka_client_core::ClientSecurity>,
}

/// Inline `user → credential` (plaintext per cp `PropertyFileLoginModule`, or a
/// `$2…` bcrypt hash). `file` is an htpasswd-style `user:cred` path (one wins).
#[derive(Debug, Clone, Default)]
pub struct BasicAuthConfig { pub users: HashMap<String, String>, pub file: Option<PathBuf> }

/// Reuse the broker's OAuth knobs; stored as the already-built validator.
#[derive(Clone)]
pub struct BearerAuthConfig { pub validator: std::sync::Arc<crabka_security::OAuthBearerValidator> }
impl std::fmt::Debug for BearerAuthConfig { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("BearerAuthConfig") } }

#[derive(Debug, Clone)]
pub struct AuthzConfig {
    pub enabled: bool,
    pub super_users: std::collections::HashSet<String>,   // principal names (e.g. "ANONYMOUS", "admin")
    pub acl_refresh: std::time::Duration,
}
impl Default for AuthzConfig { fn default() -> Self { Self { enabled: false, super_users: Default::default(), acl_refresh: std::time::Duration::from_secs(30) } } }
```
Update every `RegistryConfig { .. }` literal (binary + `tests/{integration,interop,rest_conformance,ha}.rs` + capture harnesses) to add `security: Default::default()`. Run `cargo build -p crabka-schema-registry --all-targets` (compiles; the new field defaults to open).

- [ ] **Step 3: thread `Option<ClientSecurity>` into every client.** The registry builds clients in: `kafkastore/writer.rs` (producer `Client::builder()`), `kafkastore/reader.rs` (the reader's client/connection — find it), `kafkastore/topic.rs` (`AdminClient::connect` → `connect_secured`), `election/client.rs:130,152` (`Client::builder()`), and the ACL-refresh admin (added in T3). Thread `cfg.security.client.clone()` through `KafkaStore::start` / `Election::start` to each, e.g.:
```rust
// writer.rs / election/client.rs:
Client::builder().bootstrap(...).client_id(...).maybe_security(security.clone()).build().await?
// topic.rs:
let mut admin = AdminClient::connect_secured(&bootstrap, security.clone()).await?;   // confirm connect_secured arg shape
```
Pass `security: Option<ClientSecurity>` as a new param to the internal start fns (`KafkaStore::start` reads it from `cfg.security.client`; `Election::start` already takes `&cfg`, read `cfg.security.client`). No behavior change when `None`.

- [ ] **Step 4: build + existing tests.** `cargo build -p crabka-schema-registry --all-targets`; `cargo test -p crabka-schema-registry --lib --test integration --test ha --test compat_conformance 2>&1 | tail -8` → all green (default-open behavior unchanged). clippy `--workspace --all-targets -D warnings` + fmt.

- [ ] **Step 5: commit** (`Cargo.toml`, `config.rs`, `kafkastore/*`, `election/*`, `bin`, bumped test literals): `schema-registry: SecurityConfig + client-security passthrough (opt-in, default open)`.

---

### Task 2 — authentication: `BasicAuthStore` + `auth_layer`

**Files:** Create `src/auth/mod.rs`, `src/auth/basic.rs`; Modify `src/lib.rs` (`pub mod auth;`).

- [ ] **Step 1: failing unit tests (`auth/basic.rs` `mod tests`).**
```rust
#[test] fn plaintext_verify() {
    let s = BasicAuthStore::from_users([("alice".into(), "pw".into())].into());
    assert!(s.verify("alice", "pw")); assert!(!s.verify("alice", "bad")); assert!(!s.verify("bob", "pw"));
}
#[test] fn bcrypt_verify() {
    let hash = bcrypt::hash("pw", 4).unwrap();
    let s = BasicAuthStore::from_users([("alice".into(), hash)].into());
    assert!(s.verify("alice", "pw")); assert!(!s.verify("alice", "bad"));
}
```
- [ ] **Step 2: run — FAIL.** `cargo test -p crabka-schema-registry --lib auth::basic`.
- [ ] **Step 3: implement `auth/basic.rs`.**
```rust
//! HTTP Basic credential store (the only new auth primitive; the rest reuses
//! `crabka-security`). Plaintext = cp `PropertyFileLoginModule` parity; a `$2…`
//! value is bcrypt-verified.
use std::collections::HashMap;
use subtle::ConstantTimeEq;  // if not present, use a manual constant-time eq; confirm a const-time crate in-tree

#[derive(Debug, Clone, Default)]
pub struct BasicAuthStore { users: HashMap<String, String> }
impl BasicAuthStore {
    #[must_use] pub fn from_users(users: HashMap<String, String>) -> Self { Self { users } }
    /// Load htpasswd-style `user:cred` lines from a file, merged over `users`.
    pub fn load(cfg: &crate::config::BasicAuthConfig) -> std::io::Result<Self> { /* read file lines, split_once(':'), insert; then extend with cfg.users */ Ok(Self::from_users(cfg.users.clone())) }
    #[must_use] pub fn verify(&self, user: &str, pass: &str) -> bool {
        let Some(stored) = self.users.get(user) else { return false };
        if let Some(_) = stored.strip_prefix("$2") { return bcrypt::verify(pass, stored).unwrap_or(false); }
        // constant-time plaintext compare
        stored.as_bytes().ct_eq(pass.as_bytes()).into()
    }
}
```
(If `subtle` is not a workspace dep, use a manual constant-time byte compare; do NOT add `subtle` unless already present.)
- [ ] **Step 4: run — PASS.** Same command.

- [ ] **Step 5: failing tests for `resolve` (`auth/mod.rs` `mod tests`).** The 401/precedence logic is a pure fn over (headers, an optional mTLS principal, config):
```rust
// AuthDecision = Authn(Principal) | Unauthorized
#[tokio::test] async fn anonymous_when_no_creds_and_not_required() { /* resolve(no auth header, None mtls, require_auth=false) == Authn(ANONYMOUS) */ }
#[tokio::test] async fn unauthorized_when_required_and_no_creds() { /* require_auth=true ⇒ Unauthorized */ }
#[tokio::test] async fn bad_basic_is_unauthorized() { /* Basic with wrong pw ⇒ Unauthorized even if require_auth=false */ }
#[tokio::test] async fn good_basic_authenticates() { /* Basic alice:pw ⇒ Authn(alice) */ }
#[tokio::test] async fn mtls_principal_wins() { /* mtls=Some(p) ⇒ Authn(p) regardless of headers */ }
```
- [ ] **Step 6: implement `auth/mod.rs`.** Model the Bearer extraction on `crates/grpc-gateway/src/authz/auth_layer.rs`. `AuthState { basic: Option<Arc<BasicAuthStore>>, bearer: Option<Arc<OAuthBearerValidator>>, require_auth, realm }`. A pure `async fn resolve(headers: &HeaderMap, mtls: Option<Principal>, st: &AuthState, now_ms: i64) -> AuthDecision` doing: mtls → Bearer (`validate`) → Basic (`verify` → `Principal{name:user, auth_method:SaslPlain}`) → Anonymous (or `Unauthorized` if `require_auth`); a present-but-invalid credential ⇒ `Unauthorized`. Then `auth_layer` (`from_fn_with_state`): read the mTLS `Principal` from extensions (inserted by T4's TLS loop), call `resolve`, on `Authn(p)` insert `p` into extensions + `next.run`, on `Unauthorized` return `401` with `WWW-Authenticate: Basic realm="{realm}"` when basic is enabled. Use `axum::http::HeaderMap`, `base64::engine::general_purpose::STANDARD`.
- [ ] **Step 7: run — PASS.** `cargo test -p crabka-schema-registry --lib auth`.
- [ ] **Step 8: `lib.rs` `pub mod auth;`** + clippy/fmt + commit (`auth/`, `lib.rs`): `schema-registry: auth middleware (Basic + Bearer + mTLS + anonymous, 401/WWW-Authenticate)`.

---

### Task 3 — authorization: `authz_target` + `SchemaRegistryAuthz` + `authz_layer`

**Files:** Create `src/authz.rs`; Modify `src/lib.rs` (`pub mod authz;`).

- [ ] **Step 1: failing tests for `authz_target` (`authz.rs` `mod tests`).** The (method,path)→(resource,op) map is the pure core:
```rust
use crabka_metadata::{ResourceType, AclOperation};
fn t(m: &str, p: &str) -> Option<(ResourceType, String, AclOperation)> { authz_target(&m.parse().unwrap(), p) }
#[test] fn register_is_write_on_topic_subject() { assert_eq!(t("POST","/subjects/orders-value/versions"), Some((ResourceType::Topic,"orders-value".into(),AclOperation::Write))); }
#[test] fn read_version_is_read_on_topic() { assert_eq!(t("GET","/subjects/orders-value/versions/1"), Some((ResourceType::Topic,"orders-value".into(),AclOperation::Read))); }
#[test] fn delete_subject_is_delete() { assert_eq!(t("DELETE","/subjects/orders-value"), Some((ResourceType::Topic,"orders-value".into(),AclOperation::Delete))); }
#[test] fn global_config_put_is_alter_cluster() { assert_eq!(t("PUT","/config"), Some((ResourceType::Cluster,"kafka-cluster".into(),AclOperation::Alter))); }
#[test] fn list_subjects_is_describe_cluster() { assert_eq!(t("GET","/subjects"), Some((ResourceType::Cluster,"kafka-cluster".into(),AclOperation::Describe))); }
#[test] fn root_is_unauthenticated() { assert_eq!(t("GET","/"), None); }
```
- [ ] **Step 2: run — FAIL.** `cargo test -p crabka-schema-registry --lib authz`.
- [ ] **Step 3: implement `authz_target` (the spec's table).** Parse the path segments; map per the spec's table (`/` and unknown → `None` = no authz). `kafka-cluster` is the Kafka cluster resource name. Percent-decode the subject segment.
- [ ] **Step 4: implement `SchemaRegistryAuthz` + `run_acl_refresh` + `authz_layer`.** Mirror `crates/grpc-gateway/src/authz/mod.rs`:
```rust
pub struct SchemaRegistryAuthz {
    authorizer: Arc<dyn crabka_authz::Authorizer>,
    cache: arc_swap::ArcSwap<crabka_authz::AclCache>,
    super_users: std::collections::HashSet<String>,
    enabled: bool,
}
impl SchemaRegistryAuthz {
    pub fn new(super_users: HashSet<String>, enabled: bool) -> Self { /* SimpleAclAuthorizer::new(...) — confirm ctor */ }
    pub async fn run_acl_refresh(&self, mut admin: AdminClient, refresh: Duration, shutdown: CancellationToken) { /* loop: describe_acls(&AclEntryFilter::default()) → cache.store(Arc::new(AclCache::new(entries))); sleep(refresh) or break on shutdown — copy gateway */ }
    pub fn authorize(&self, principal: &Principal, host: &SocketAddr, rt: ResourceType, name: &str, op: AclOperation) -> bool {
        if !self.enabled || self.super_users.contains(&principal.name) { return true; }
        let cache = self.cache.load();
        matches!(self.authorizer.authorize(&**cache, &AuthorizationRequest { principal, host, resource_type: rt, resource_name: name, operation: op }), AuthorizationResult::Allow)
    }
}
/// from_fn_with_state. SKIP authz for trusted forwards (loop-guard header present).
pub async fn authz_layer(State(az): State<Arc<SchemaRegistryAuthz>>, req: Request, next: Next) -> Response {
    if req.headers().contains_key(crate::rest::forward::FORWARD_HEADER) { return next.run(req).await; }
    let Some((rt, name, op)) = authz_target(req.method(), req.uri().path()) else { return next.run(req).await; };
    let principal = req.extensions().get::<Principal>().cloned().unwrap_or_else(anonymous);
    let host = /* peer SocketAddr from extensions or a fixed 0.0.0.0 if unavailable */;
    if az.authorize(&principal, &host, rt, &name, op) { next.run(req).await }
    else { (StatusCode::FORBIDDEN, "authorization denied").into_response() }
}
```
- [ ] **Step 5: tests for `authorize`** (enabled+allow ACL, deny, super-user bypass, disabled=allow) using `AclCache::new(vec![entry…])` (construct an `AclEntry` allowing `User:alice Write Topic:s`; read `crates/authz/src/cache.rs` tests for the `AclEntry` shape) + a `forward-skip` assertion on `authz_layer`.
- [ ] **Step 6:** `lib.rs` `pub mod authz;` + clippy/fmt + commit (`authz.rs`, `lib.rs`): `schema-registry: Topic-ACL authorization (authz_target map + SchemaRegistryAuthz + forward-trust)`.

---

### Task 4 — TLS serve + `router_with_security` + binary wiring

**Files:** Modify `src/rest/mod.rs`; Create `src/rest/serve.rs`; Modify `src/bin/schema-registry.rs`.

- [ ] **Step 1: `router_with_security` (`rest/mod.rs`).** Compose the layers so execution = auth→authz→forward→handler:
```rust
pub struct SecurityLayers { pub auth: auth::AuthState, pub authz: Option<Arc<authz::SchemaRegistryAuthz>>, pub forward: forward::ForwardState }
pub fn router_with_security(state: AppState, sec: SecurityLayers) -> Router {
    let mut r = router(state).layer(from_fn_with_state(sec.forward, forward::forward_layer));
    if let Some(az) = sec.authz { r = r.layer(from_fn_with_state(az, authz::authz_layer)); }
    r.layer(from_fn_with_state(Arc::new(sec.auth), auth::auth_layer))
}
```
- [ ] **Step 2: `rest/serve.rs` — HTTP/HTTPS.** Model on `crates/grpc-gateway/src/serve.rs`. `serve_http(listener, app, shutdown)` = today's `axum::serve(...).with_graceful_shutdown(...)`. `serve_https(listener, app, tls: TlsConfig, shutdown)` = build `Arc<rustls::ServerConfig>` via `tls.build_server_config()`, accept loop with `tokio_rustls::TlsAcceptor`, and for each connection extract the mTLS `Principal` (`peer_principal` via `extract_principal_from_cert`) and inject it into the request extensions before serving (gateway pattern; use `axum::Extension` / a per-connection `tower` layer). Keep it small; copy the gateway's accept loop structure.
- [ ] **Step 3: binary wiring (`bin/schema-registry.rs`).** Add clap args/env for `SecurityConfig` (auth method toggles + basic users/file + realm + require-auth; bearer issuer/JWKS; tls cert/key/ca/client-auth; authz enable/super-users/refresh; client security protocol/sasl/tls). Build `SecurityLayers`; if `authz.enabled`, spawn `run_acl_refresh` (with an `AdminClient::connect_secured`); build `router_with_security`; serve via `serve_https` when `tls` is set else `serve_http`.
- [ ] **Step 4: build + smoke.** `cargo build -p crabka-schema-registry`; `cargo test -p crabka-schema-registry --lib --test ha 2>&1 | tail -6` green. clippy `--workspace --all-targets -D warnings` + fmt.
- [ ] **Step 5: commit** (`rest/mod.rs`, `rest/serve.rs`, `bin`): `schema-registry: security middleware stack + TLS/HTTPS serving + binary wiring`.

---

### Task 5 — in-process integration tests (`tests/security.rs`)

**Files:** Create `tests/security.rs`; Modify `.github/workflows/ci.yml` (add `--test security` to the `schema-registry-integration` llvm-cov list).

- [ ] **Step 1: helpers + tests.** `#![cfg(not(target_os = "windows"))]` (attributes ABOVE any `//!` doc per the windows trap). Boot an in-process broker; seed ACLs via `AdminClient` `create_acls` (`User:alice Allow Write Topic:s`); boot an SR node (`KafkaStore` + `router_with_security`) with auth(require)+authz(enabled) + a `BasicAuthStore{alice:pw}`; serve on `127.0.0.1:0`. Assert with `reqwest`:
  - no `Authorization` → `401` + `WWW-Authenticate: Basic`.
  - `alice:wrong` → `401`.
  - `bob:pw` (unknown) → `401`.
  - `alice:pw` register to subject `s` → `200` (has Write ACL); register to subject `other` (no ACL) → `403`.
  - `alice:pw` GET `/subjects/s/versions` → `200`.
  - two-node: a write to a secondary with `alice:pw` is authorized-at-ingress + forwarded + lands (GET reflects on both); an unauthorized write to the secondary → `403`, never forwarded.
  - TLS: an HTTPS round-trip with a generated self-signed cert (use the gateway tests' cert helper if present) → `200`.
- [ ] **Step 2: run** `cargo test -p crabka-schema-registry --test security -- --nocapture` → all pass. Raise election/forward await budgets if tight (reuse `ha.rs` patterns). clippy/fmt.
- [ ] **Step 3:** add `--test security` to ci.yml; commit (`tests/security.rs`, `ci.yml`): `schema-registry: in-process security integration (401/403/200, forward-authz, TLS)`.

---

### Task 6 — cp Docker capture + Basic-auth calibration

**Files:** Create `tests/capture_auth_fixtures.rs` (`#[ignore]` Docker) + `tests/fixtures/auth/basic.json`; calibrate `auth/mod.rs` 401 shape if cp differs.

- [ ] **Step 1: harness.** `#![allow(clippy::pedantic)]` + `#![cfg(not(target_os = "windows"))]` ABOVE the `//!` docs (windows trap). Model on `tests/capture_admin_fixtures.rs` + the new `describe_groups_jvm.rs` container plumbing. Boot `confluentinc/cp-schema-registry:7.4.0` with `SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC`, `SCHEMA_REGISTRY_AUTHENTICATION_ROLES=admin`, and a JAAS/properties file mapping `alice=pw,admin` (pass via env + a mounted file or `SCHEMA_REGISTRY_OPTS`). From the host: GET `/subjects` with (a) no creds, (b) `alice:wrong`, (c) `alice:pw`; capture status + the `WWW-Authenticate` header + body → `tests/fixtures/auth/basic.json`.
- [ ] **Step 2: run (Docker).** `cargo test -p crabka-schema-registry --test capture_auth_fixtures -- --ignored --nocapture`. **If Docker unavailable, STOP + report** (commit the harness; our 401 stays as-designed). Report cp's exact `401` status + `WWW-Authenticate: Basic realm="…"` (the realm string!) + body.
- [ ] **Step 3: calibrate** `auth/mod.rs` to cp's realm default + body shape; add a no-Docker byte-pin unit test asserting our 401 response matches the captured cp `WWW-Authenticate` + body. Report seed→cp changes.
- [ ] **Step 4: full gate + commit.** `cargo test -p crabka-schema-registry --lib --test security --test ha --test integration --test compat_conformance` green; clippy/fmt. Commit (`tests/capture_auth_fixtures.rs`, `fixtures/auth/`, `auth/mod.rs`): `schema-registry: cp-calibrated Basic-auth 401 (capture + byte-pin)`.

---

## Self-review (plan author)

**Spec coverage:** authn 3 methods + anonymous + 401 (T2) ✓; Topic-ACL authz + map + forward-trust (T3) ✓; TLS/mTLS serving (T4) ✓; client-security passthrough (T1) ✓; config (T1) ✓; middleware stack order (T4) ✓; cp Basic oracle (T6) ✓; integration incl. forward-authz + TLS (T5) ✓; opt-in/default-open (T1, every task) ✓.

**Placeholder scan:** the two "confirm ctor" notes (`SimpleAclAuthorizer::new`, `connect_secured` arg shape) are explicit READ-THE-SOURCE instructions, not hand-waves — the implementer reads `crates/authz/src/simple.rs` + `crates/client-admin/src/lib.rs:383-389`. The TLS accept loop + AclCache refresh point at exact gateway files to copy. No "TBD".

**Type consistency:** `SecurityConfig`/`BasicAuthConfig`/`BearerAuthConfig`/`AuthzConfig` (T1) consumed by `AuthState`/`BasicAuthStore` (T2) + `SchemaRegistryAuthz` (T3) + `SecurityLayers` (T4); `authz_target(&Method,&str)->Option<(ResourceType,String,AclOperation)>`, `authorize(&Principal,&SocketAddr,ResourceType,&str,AclOperation)->bool`, `FORWARD_HEADER` (slice-5) reused for the authz forward-skip — all consistent. `maybe_security(Option<ClientSecurity>)` + `connect_secured` used uniformly in T1.

**Risks:** the biggest is the TLS accept loop + mTLS-principal-into-extensions plumbing (T4) — mitigated by copying the gateway's `serve.rs` verbatim. The `SimpleAclAuthorizer`/`AclEntry`/`connect_secured` exact shapes are read-from-source in T3/T1.
