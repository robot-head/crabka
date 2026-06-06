# Crabka gRPC Gateway P5 — Identity → ACL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Factor the broker's ACL evaluator into a shared `crabka-authz` crate, then make the gateway a **trusted-proxy authorizer**: authenticate each caller (mTLS cert — P4 — *or* bearer/JWT), authorize their produce/consume against a polled snapshot of broker ACLs, deny the unauthorized, audit every decision (on-behalf-of), and forward the resolved caller identity so the owning replica authorizes identically. The gateway still performs Kafka ops as its own service principal.

**Architecture:** New leaf crate `crabka-authz` holds the `Authorizer` trait + `SimpleAclAuthorizer`/`AllowAllAuthorizer` + an `AclSource` abstraction (`impl AclSource for MetadataImage` serves the broker; a gateway `AclCache` over a `Vec<AclEntry>` from `describe_acls` serves the gateway). The broker's `authorizer` module becomes thin re-exports of `crabka-authz` (behavior-preserving, Kafka wire byte-exact). The gateway resolves a per-request `Principal` (bearer header overrides connection mTLS, else Anonymous), and gates `send`/`subscribe` on `Write`/`Read` ACLs. Authz is **config-gated**: default `AllowAllAuthorizer` ⇒ no enforcement ⇒ all existing tests unchanged.

**Tech Stack:** `crabka-metadata` (ACL data types — already shared), `crabka-security` (`Principal`, OAUTHBEARER/JWKS bearer validation — already implemented), `crabka-client-admin` (`describe_acls` — already implemented), axum middleware for per-request bearer auth. **Kafka wire stays byte-exact**; the broker change is a pure-logic move with no behavior change.

**Scope (user chose FULL P5):** crabka-authz factor + gateway ACL cache + mTLS **and** bearer caller auth + authz gating of produce/consume + on-behalf-of audit logging (gateway-side structured audit) + identity-forwarding (owner re-authorizes the forwarded caller). The broker-side on-behalf-of *wire header* is out of scope (would change broker wire/audit beyond the factor) — audit is gateway-side structured logging.

---

## Execution constraints (every task)

- **Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25`. Subagent shells reset cwd to MAIN repo — prefix every Bash with `cd /Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25 && ...`, use `git -C <worktree>`.
- **Branch:** `claude/gateway-p5`, **stacked on `claude/gateway-p4`** (#406 — unmerged; P5 builds on P4's mTLS principal injection in `serve.rs`, the forward channel, and `config.tls`). PR bases on #406, or rebases onto `main` if #406 merges first. Assert `git -C <worktree> rev-parse --abbrev-ref HEAD` == `claude/gateway-p5` before every commit.
- **Git identity:** `git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit ...` (never `git config`). Stage `Cargo.lock` when deps/members change.
- **Broker change is behavior-preserving ONLY.** Kafka wire bytes, error codes, and the 16 authorizer unit tests' semantics must be identical. The broker's full test suite must stay green.
- **Each task ends GREEN:** `cargo test -p <crate>`, `cargo clippy -p <crate> --all-targets -- -D warnings`, `cargo fmt --check`. For broker-touching tasks also run `cargo test -p crabka-broker` (a subset if full is too slow — at minimum the authorizer + a produce/fetch handler test).

## Confirmed APIs (investigated — rely on these)

- Broker authorizer (`crates/broker/src/authorizer/`): `trait Authorizer: Send+Sync+Debug { fn authorize(&self, image: &MetadataImage, req: &AuthorizationRequest) -> AuthorizationResult; }`; `AuthorizationRequest<'a> { principal: &'a Principal, host: &'a SocketAddr, resource_type: ResourceType, resource_name: &'a str, operation: AclOperation }`; `AuthorizationResult::{Allow,Deny}`; `SimpleAclAuthorizer::new(super_users: HashSet<String>)`; `AllowAllAuthorizer`; `authorize_topics(...)`; plus `opa::OpaAuthorizer` (stays in broker). The SimpleAcl logic: super-user bypass → iterate `image.matching_acls(rt,name)` → deny-wins → default-deny; `matches_principal` (`User:*` or `User:{name}`), `matches_host` (`*` or ip), `matches_operation` (exact / `All` / implies table Read|Write|Delete|Alter→Describe, AlterConfigs→DescribeConfigs).
- `crabka_metadata`: `AclEntry { resource_type, resource_name, pattern_type, principal, host, operation, permission_type }`, `AclEntryFilter` (all `Option`, `Default`), `ResourceType::{Topic,Group,Cluster,TransactionalId,DelegationToken}`, `AclOperation::{All,Read,Write,Create,Delete,Alter,Describe,ClusterAction,DescribeConfigs,AlterConfigs,IdempotentWrite}`, `PatternType::{Literal,Prefixed}`, `PermissionType::{Allow,Deny}`. `MetadataImage::matching_acls(rt, name) -> impl Iterator<Item=&AclEntry>` (literal exact + literal `*` + prefixed `name.starts_with(pattern)`), `all_acls()`. `crabka-metadata` deps = `crabka-security` + `crabka-protocol` (NO broker dep ⇒ no cycle).
- `crabka_security`: `Principal { name: String, auth_method: AuthMethod, groups: Vec<String> }`, `AuthMethod::{Anonymous,SaslOAuthBearer,MTls,...}`, `extract_principal_from_cert`, `parse_client_initial_response(&[u8]) -> Result<ClientInitialResponse{token,authzid}, AuthError>`, `OAuthBearerValidator::{Unsecured(UnsecuredJwsValidator),Signed(SignedJwsValidator),Introspection(...)}` with `async fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome{principal,expires_at_ms}, AuthError>`, `UnsecuredJwsValidator` (Default + fields incl `principal_claim_name`).
- `crabka_client_admin::AdminClient::describe_acls(&mut self, filter: &AclEntryFilter) -> Result<Vec<AclEntry>, AdminError>` (already implemented).
- Gateway (P4 state): `serve.rs` injects `crabka_security::Principal` into request extensions per connection (mTLS). `handlers::send` / `streaming::{send_stream,subscribe}` take `Extension<Arc<AppState>>`; do NOT yet read the principal. `forward.rs::forward_handler` already reads `Option<Extension<Principal>>`. `AppState { produce: Arc<ProduceCore>, config: Arc<GatewayConfig> }`.

## Crate/file map

- **Create** `crates/authz/` (Cargo.toml, src/lib.rs, src/source.rs, src/simple.rs, src/allow_all.rs, src/cache.rs) + add to workspace `members`.
- **Modify** `crates/broker/src/authorizer/{mod.rs,simple_acl.rs,allow_all.rs,opa.rs}` (move logic out → re-export from crabka-authz; opa re-points to the moved trait), `crates/broker/Cargo.toml` (+crabka-authz dep). Possibly broker call sites for the `&dyn AclSource` coercion.
- **Create** `crates/grpc-gateway/src/authz/{mod.rs,cache.rs,auth_layer.rs}` (ACL cache poll task + the authorizer holder; per-request bearer/principal resolution layer).
- **Modify** `grpc-gateway/src/{config.rs,state.rs,handlers.rs,streaming.rs,forward.rs,lib.rs,serve.rs,bin/gateway.rs,error.rs}` + `Cargo.toml` (+crabka-authz, +crabka-metadata, +crabka-client-admin already dep).
- **Create** gateway tests `tests/authz.rs` (+ extend `tests/forwarding.rs`/`tls.rs` as needed).

## Batches (dependency-ordered; parallel where file sets are disjoint)

- **Batch A:** Task 1 (crabka-authz crate) — foundation, solo.
- **Batch B:** Task 2 (broker refactor) ∥ Task 3 (gateway authz core: cache + state + config). Disjoint crates; both need Task 1.
- **Batch C:** Task 4 (bearer/principal auth layer) ∥ Task 5 (handler authz gating + audit) ∥ Task 6 (identity forwarding). T5 & T6 both touch handler/forward paths — see per-task file sets; if they overlap (`forward.rs`), sequence T6 after T5. Safe split: T4 = `auth_layer.rs` (new); T5 = `handlers.rs`+`streaming.rs`; T6 = `forward.rs`+`produce.rs`. Disjoint ⇒ parallel.
- **Batch D:** Task 7 (bin wiring) — needs T3–T6.
- **Batch E:** Task 8 (integration tests) — needs T7.

---

## Task 1: `crabka-authz` crate (factor the evaluator)

**Files:** Create `crates/authz/Cargo.toml`, `crates/authz/src/lib.rs`, `src/source.rs`, `src/simple.rs`, `src/allow_all.rs`, `src/cache.rs`. Modify root `Cargo.toml` (`members`).

- [ ] **Step 1: Cargo.toml + workspace member.** Create `crates/authz/Cargo.toml`:

```toml
[package]
name = "crabka-authz"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Shared Kafka-ACL authorization evaluator for the Crabka broker and gateway"

[lints]
workspace = true

[dependencies]
crabka-metadata = { version = "0.2", path = "../metadata" }
crabka-security = { version = "0.2", path = "../security" }

[dev-dependencies]
assert2 = { workspace = true }
uuid = { workspace = true }
```

(Confirm the exact internal-version string + workspace pin convention against a sibling leaf crate, e.g. `crates/security/Cargo.toml`. Add `crabka-authz` to the root `Cargo.toml` `[workspace] members` list, alphabetically.)

- [ ] **Step 2: `src/source.rs` — the AclSource abstraction.**

```rust
//! Abstraction over "where ACL entries come from", so one evaluator serves both
//! the broker (a `MetadataImage` snapshot) and the gateway (a `Vec<AclEntry>`
//! cache fetched via DescribeAcls).

use crabka_metadata::{AclEntry, ResourceType};

/// A source of ACL entries the authorizer can match against. `matching_acls`
/// MUST return every entry whose resource pattern matches `(rt, name)`:
/// LITERAL entries equal to `name`, LITERAL `*` (wildcard), and PREFIXED
/// entries where `name.starts_with(entry.resource_name)`. (Mirror
/// `MetadataImage::matching_acls` — `crates/metadata/src/image.rs`.)
pub trait AclSource {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a>;
}

// The broker's MetadataImage already implements the exact matching semantics;
// adapt its iterator. (Trait is local ⇒ orphan rule satisfied for the foreign
// MetadataImage type.)
impl AclSource for crabka_metadata::MetadataImage {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a> {
        Box::new(crabka_metadata::MetadataImage::matching_acls(self, rt, name))
    }
}
```

> VERIFY `MetadataImage::matching_acls` is `pub` and returns `impl Iterator<Item=&AclEntry>` (image.rs:295). If its name/signature differs, adapt.

- [ ] **Step 3: `src/lib.rs` — trait + request/result + `authorize_topics`.** Move `AuthorizationRequest`, `AuthorizationResult`, `Authorizer`, and `authorize_topics` from `crates/broker/src/authorizer/mod.rs` VERBATIM, but change the trait + helper to take `&dyn AclSource` instead of `&MetadataImage`:

```rust
//! Shared Kafka-ACL authorization evaluator (broker + gateway).
#![forbid(unsafe_code)]

mod allow_all;
pub mod cache;
mod simple;
mod source;

pub use allow_all::AllowAllAuthorizer;
pub use cache::AclCache;
pub use simple::SimpleAclAuthorizer;
pub use source::AclSource;

use std::net::SocketAddr;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::Principal;

#[derive(Debug, Clone)]
pub struct AuthorizationRequest<'a> {
    pub principal: &'a Principal,
    pub host: &'a SocketAddr,
    pub resource_type: ResourceType,
    pub resource_name: &'a str,
    pub operation: AclOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationResult {
    Allow,
    Deny,
}

pub trait Authorizer: Send + Sync + std::fmt::Debug {
    fn authorize(&self, source: &dyn AclSource, req: &AuthorizationRequest<'_>) -> AuthorizationResult;
}

#[must_use]
pub fn authorize_topics<'a>(
    authorizer: &dyn Authorizer,
    source: &dyn AclSource,
    principal: &Principal,
    host: &SocketAddr,
    operation: AclOperation,
    topic_names: impl IntoIterator<Item = &'a str>,
) -> std::collections::HashMap<&'a str, AuthorizationResult> {
    topic_names
        .into_iter()
        .map(|name| {
            let req = AuthorizationRequest {
                principal, host, resource_type: ResourceType::Topic, resource_name: name, operation,
            };
            (name, authorizer.authorize(source, &req))
        })
        .collect()
}
```

- [ ] **Step 4: `src/simple.rs`** — move `SimpleAclAuthorizer` + `matches_principal`/`matches_host`/`matches_operation`/`implies` from broker `simple_acl.rs` VERBATIM, changing only `image: &MetadataImage` → `source: &dyn AclSource` and `image.matching_acls(...)` → `source.matching_acls(...)`. Move the 16 `#[cfg(test)]` unit tests too (they build a `MetadataImage`, apply ACL records, and authorize — they now pass `&image` which coerces to `&dyn AclSource`). Keep behavior identical.

- [ ] **Step 5: `src/allow_all.rs`** — move `AllowAllAuthorizer` (always `Allow`), trait method signature updated to `(&self, _source: &dyn AclSource, _req)`.

- [ ] **Step 6: `src/cache.rs` — the gateway ACL cache.**

```rust
//! Gateway-side ACL snapshot: a flat `Vec<AclEntry>` (from `describe_acls`)
//! that implements `AclSource` with EXACTLY the broker's matching semantics.

use crabka_metadata::{AclEntry, PatternType, ResourceType};

use crate::AclSource;

/// Immutable ACL snapshot; rebuilt wholesale on each refresh.
#[derive(Debug, Clone, Default)]
pub struct AclCache {
    entries: Vec<AclEntry>,
}

impl AclCache {
    #[must_use]
    pub fn new(entries: Vec<AclEntry>) -> Self {
        Self { entries }
    }
    #[must_use]
    pub fn len(&self) -> usize { self.entries.len() }
    #[must_use]
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
}

impl AclSource for AclCache {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a> {
        // MUST mirror MetadataImage::matching_acls: same resource_type, and
        // (LITERAL == name) || (LITERAL == "*") || (PREFIXED && name.starts_with(resource_name)).
        Box::new(self.entries.iter().filter(move |e| {
            e.resource_type == rt
                && match e.pattern_type {
                    PatternType::Literal => e.resource_name == name || e.resource_name == "*",
                    PatternType::Prefixed => name.starts_with(e.resource_name.as_str()),
                }
        }))
    }
}
```

- [ ] **Step 7: cross-validation test** in `src/cache.rs` `#[cfg(test)]`: build the same set of `AclEntry`s, apply them to a `MetadataImage` (via `MetadataImage::apply(MetadataRecord::V1AccessControlEntry(...))` — match how the moved simple.rs tests build images) AND into an `AclCache`; for several `(ResourceType, name)` probes (literal hit, wildcard `*`, prefixed hit, prefixed miss, wrong-type), assert the two `matching_acls` iterators yield the **same set** of entries. This guards against drift between the broker's image matching and the gateway cache.

- [ ] **Step 8: Gates + commit.** `cargo test -p crabka-authz` (16 moved tests + cross-validation pass), clippy, fmt. Commit `feat(authz): crabka-authz crate (Authorizer + SimpleAcl + AllowAll + AclSource + AclCache)`. Stage `crates/authz/**` + root `Cargo.toml` + `Cargo.lock`.

---

## Task 2: Broker delegates to `crabka-authz` (behavior-preserving)

**Files:** Modify `crates/broker/src/authorizer/{mod.rs,opa.rs}`, delete `simple_acl.rs`+`allow_all.rs` content (move to re-export), `crates/broker/Cargo.toml`. Possibly broker authorize call sites.

- [ ] **Step 1: Add the dep.** `crates/broker/Cargo.toml` `[dependencies]`: `crabka-authz = { version = "0.2", path = "../authz" }`.

- [ ] **Step 2: Re-export from `authorizer/mod.rs`.** Replace the moved types with re-exports so all `crate::authorizer::*` call sites keep compiling:

```rust
//! Cluster authorizer. The trait + ACL evaluator now live in `crabka-authz`
//! (shared with the gateway); this module re-exports them and keeps the
//! broker-only OPA plugin.
pub mod opa;

pub use crabka_authz::{
    AclSource, AuthorizationRequest, AuthorizationResult, Authorizer, AllowAllAuthorizer,
    SimpleAclAuthorizer, authorize_topics,
};
```

Delete `crates/broker/src/authorizer/simple_acl.rs` and `allow_all.rs` (their content moved to crabka-authz in Task 1). Remove their `mod` lines.

- [ ] **Step 3: Re-point `opa.rs`.** `OpaAuthorizer` impl's the trait — change `use super::{...}` to `use crabka_authz::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};` and change its `authorize(&self, image: &MetadataImage, req)` signature to `authorize(&self, _source: &dyn AclSource, req: &AuthorizationRequest<'_>)` (OPA ignores the ACL source — it calls the policy server). Keep all OPA logic + tests.

- [ ] **Step 4: Fix authorize call sites.** Broker handlers call `authorizer.authorize(&image, &req)` and `authorize_topics(authorizer, &image, ...)`. With the trait now taking `&dyn AclSource`, `&image` (a `MetadataImage`) coerces to `&dyn AclSource` (impl is in crabka-authz). Build the broker; if any call site fails the unsized coercion, add `use crabka_authz::AclSource;` in that module or coerce explicitly `&image as &dyn crabka_authz::AclSource`. Grep `\.authorize(` and `authorize_topics(` across `crates/broker/src` and fix each.

- [ ] **Step 5: Gates + commit.** `cargo build -p crabka-broker`, then `cargo test -p crabka-broker` (MUST stay green — the moved authorizer tests now live in crabka-authz; broker handler/auth tests still exercise the re-exported types). `cargo clippy -p crabka-broker --all-targets -- -D warnings`, `cargo fmt --check -p crabka-broker`. Commit `refactor(broker): delegate ACL authorization to crabka-authz (behavior-preserving)`. Stage the broker changes + `Cargo.lock`.

---

## Task 3: Gateway ACL cache + authorizer state + config

**Files:** Create `crates/grpc-gateway/src/authz/{mod.rs,cache.rs}`; modify `src/config.rs`, `src/state.rs`, `src/lib.rs`, `Cargo.toml`. (Bearer layer = Task 4; gating = Task 5.)

- [ ] **Step 1: Deps.** `grpc-gateway/Cargo.toml`: add `crabka-authz = { version = "0.2", path = "../authz" }` and `crabka-metadata = { version = "0.2", path = "../metadata" }`. (`crabka-client-admin` already a dep.)

- [ ] **Step 2: Config.** In `config.rs`, add:

```rust
/// Authorization settings. `None` ⇒ AllowAll (no enforcement; default).
#[derive(Debug, Clone)]
pub struct AuthzSettings {
    /// Principals (bare names) that bypass ACL checks.
    pub super_users: Vec<String>,
    /// ACL-cache refresh interval (seconds).
    pub acl_refresh_secs: u64,
}
```
and `pub authz: Option<AuthzSettings>,` on `GatewayConfig` (after `tls`). Update the GatewayConfig literals in `tests/{wire,streaming,forwarding,tls,forward_unit}.rs` + `bin/gateway.rs` with `authz: None`.

- [ ] **Step 3: `src/authz/mod.rs` + `cache.rs`.** A holder for the authorizer + a swappable ACL cache + a background poll task:

```rust
//! Gateway trusted-proxy authorization: holds the `crabka_authz::Authorizer`
//! and an ArcSwap'd `AclCache` refreshed by polling the broker's DescribeAcls.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crabka_authz::{AclCache, Authorizer};
use crabka_client_admin::AdminClient;
use crabka_metadata::AclEntryFilter;
use tokio_util::sync::CancellationToken;

pub struct GatewayAuthz {
    authorizer: Arc<dyn Authorizer>,
    cache: ArcSwap<AclCache>,
}

impl GatewayAuthz {
    #[must_use]
    pub fn new(authorizer: Arc<dyn Authorizer>) -> Self {
        Self { authorizer, cache: ArcSwap::from_pointee(AclCache::default()) }
    }
    #[must_use]
    pub fn authorizer(&self) -> &Arc<dyn Authorizer> { &self.authorizer }
    #[must_use]
    pub fn cache(&self) -> arc_swap::Guard<Arc<AclCache>> { self.cache.load() }

    /// Poll DescribeAcls into the cache until `shutdown`. Logs + keeps the prior
    /// snapshot on error.
    pub async fn run_acl_refresh(
        self: Arc<Self>,
        bootstrap: String,
        refresh: Duration,
        shutdown: CancellationToken,
    ) {
        let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
        loop {
            match Self::fetch(&addrs).await {
                Ok(entries) => self.cache.store(Arc::new(AclCache::new(entries))),
                Err(e) => tracing::warn!(error = %e, "ACL refresh failed; keeping prior snapshot"),
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(refresh) => {}
            }
        }
    }

    async fn fetch(addrs: &[String]) -> Result<Vec<crabka_metadata::AclEntry>, crate::error::GatewayError> {
        let mut admin = AdminClient::connect(addrs).await
            .map_err(|e| crate::error::GatewayError::Other(format!("acl admin connect: {e}")))?;
        admin.describe_acls(&AclEntryFilter::default()).await
            .map_err(|e| crate::error::GatewayError::Other(format!("describe_acls: {e}")))
    }
}
```

> VERIFY: `arc_swap` is available (add `arc-swap = { workspace = true }` to gateway deps); `AdminClient::describe_acls(&AclEntryFilter)` + `AclEntryFilter::default()` signatures; `AdminError`'s Display.

- [ ] **Step 4: `lib.rs`** `pub mod authz;`. Build-only gate (the module is used by Tasks 5/7). `cargo build`/test/clippy/fmt. Commit `feat(gateway): ACL cache + authorizer holder + DescribeAcls refresh task`.

---

## Task 4: Per-request caller authentication (bearer + principal resolution)

**Files:** Create `crates/grpc-gateway/src/authz/auth_layer.rs`; modify `src/serve.rs` (inject peer addr), `src/authz/mod.rs` (re-export). Bearer validator config in `config.rs`.

- [ ] **Step 1: Principal resolution layer.** A tower/axum middleware that runs per request: if an `Authorization: Bearer <token>` header is present and a bearer validator is configured, validate it (`OAuthBearerValidator::validate`) → inject the resulting `Principal` (overriding any connection mTLS principal); else leave the mTLS principal (injected by `serve.rs`); else the handlers treat absence as `Anonymous`. Provide a helper to read the effective principal from request extensions defaulting to `Principal { name: "ANONYMOUS".into(), auth_method: AuthMethod::Anonymous, groups: vec![] }`.

```rust
//! Per-request caller-principal resolution: bearer header overrides the
//! connection mTLS principal; absence ⇒ Anonymous.

use std::sync::Arc;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use crabka_security::{AuthMethod, OAuthBearerValidator, Principal};

/// Wrapper so the validator can be an Extension on the router.
#[derive(Clone)]
pub struct BearerValidator(pub Arc<OAuthBearerValidator>);

#[must_use]
pub fn anonymous() -> Principal {
    Principal { name: "ANONYMOUS".into(), auth_method: AuthMethod::Anonymous, groups: vec![] }
}

/// axum middleware: resolve `Authorization: Bearer` → Principal (if a validator
/// is layered in), else keep the mTLS principal from `serve.rs`.
pub async fn resolve_principal(mut req: Request, next: Next) -> Response {
    if let Some(BearerValidator(validator)) = req.extensions().get::<BearerValidator>().cloned() {
        if let Some(token) = bearer_token(&req) {
            // now_ms: pass a real clock; tests can tolerate exp via unsecured validator.
            let now_ms = now_ms();
            match validator.validate(&token, now_ms).await {
                Ok(outcome) => { req.extensions_mut().insert(outcome.principal); }
                Err(e) => { tracing::debug!(error = %e, "bearer validation failed; falling back"); }
            }
        }
    }
    next.run(req).await
}

fn bearer_token(req: &Request) -> Option<String> {
    let h = req.headers().get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    h.strip_prefix("Bearer ").map(str::to_string)
}

fn now_ms() -> i64 {
    // SystemTime is allowed in the gateway runtime (unlike workflow scripts).
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
```

> VERIFY: `OAuthBearerValidator::validate` is `async` and returns `Result<AuthOutcome, _>` with `AuthOutcome.principal`; `axum::middleware::{from_fn, Next}` + `axum::extract::Request` signatures for axum 0.8. Add a config `BearerSettings` (e.g. unsecured principal-claim, or JWKS url/issuer/audience) to `config.rs` and a constructor mapping it to `OAuthBearerValidator`; for v1 default support the **unsecured** validator (dev) + **signed/JWKS** (prod) per `crabka_security`.

- [ ] **Step 2: Inject the caller host in `serve.rs`.** In `serve_tls`'s per-connection spawn, also inject the peer `SocketAddr` into request extensions (next to the principal) so the authorizer can do host-based ACL matching: `req.extensions_mut().insert(peer);` (peer is the `SocketAddr` from `listener.accept()`). For the plaintext arm, peer injection is best-effort (absent ⇒ authz uses a `0.0.0.0:0` default; host=`*` ACLs still match). Provide a helper `peer_or_default(req) -> SocketAddr`.

- [ ] **Step 3: Gates + commit.** Build/test/clippy/fmt. Commit `feat(gateway): bearer-token principal resolution layer + caller-host injection`.

---

## Task 5: Authorize produce/consume + on-behalf-of audit

**Files:** Modify `crates/grpc-gateway/src/handlers.rs`, `src/streaming.rs`, `src/state.rs` (add `authz: Arc<GatewayAuthz>`), `src/error.rs` (authz-denied error).

- [ ] **Step 1: State + error.** `AppState` gains `pub authz: Arc<crate::authz::GatewayAuthz>`. `GatewayError` gains `#[error("not authorized: {0}")] Unauthorized(String)`. Map `Unauthorized` in `handlers::error_result` to a non-retriable per-record error (gRPC code 7 = PERMISSION_DENIED).

- [ ] **Step 2: Authorize `send` (unary).** In `handlers::send`, read the effective principal + host from extensions (use the Task-4 helpers; default Anonymous / `0.0.0.0:0`). For each record, before `state.produce.produce(rec)`: build `AuthorizationRequest { principal: &eff, host: &peer, resource_type: ResourceType::Topic, resource_name: &rec.topic, operation: AclOperation::Write }`, call `state.authz.authorizer().authorize(&**state.authz.cache(), &req)`. If `Deny` ⇒ push an `error_result(&GatewayError::Unauthorized(...))` for that record (do NOT produce). **Audit:** emit `tracing::info!(target: "gateway::audit", principal = %eff.name, op = "Write", topic = %rec.topic, decision = ?result, "produce authz")`. With the default `AllowAllAuthorizer`, every decision is `Allow` ⇒ unchanged behavior.

To read the principal in the handler, add a handler arg `principal: Option<Extension<crabka_security::Principal>>` and `peer: Option<Extension<SocketAddr>>` (axum extracts from request extensions; `None` ⇒ defaults). (Connect handlers are axum handlers, so extra `Extension` extractors compose.)

- [ ] **Step 3: Authorize `send_stream`.** Same per-record gating inside `send_stream_inner`; thread the principal + host into the generator.

- [ ] **Step 4: Authorize `subscribe`.** On the `Start` frame, before opening the consume session: authorize `(Group, group_id, Read)` AND for each topic `(Topic, topic, Read)`. Any `Deny` ⇒ end the stream with a PERMISSION_DENIED error frame (subscribe can't proceed without read access). Audit each decision.

- [ ] **Step 5: Gates + commit.** Existing tests still pass (default AllowAll). Commit `feat(gateway): authorize produce/consume against ACL cache + on-behalf-of audit`.

---

## Task 6: Identity forwarding (owner re-authorizes)

**Files:** Modify `crates/grpc-gateway/src/forward.rs`, `src/produce.rs` (thread caller identity into the forward), `src/handlers.rs`/`streaming.rs` call sites if the produce signature changes.

- [ ] **Step 1: Carry the caller principal on the wire.** `ForwardRecord` gains `pub principal: Option<ForwardPrincipal>` where `ForwardPrincipal { name: String, auth_method: String, groups: Vec<String> }` (serde). The origin populates it from the resolved caller principal; `Forwarder::forward` takes the principal and serializes it.

- [ ] **Step 2: Owner re-authorizes.** In `forward_handler`, after the existing mTLS-peer gate (which authenticates the *forwarding gateway*), reconstruct the **caller** `Principal` from `req.principal` and authorize `(Topic, rec.topic, Write)` against the owner's ACL cache (`state.authz`). `Deny` ⇒ 403 with a non-retriable error. This makes the owner enforce the original caller's authorization (defense-in-depth; the origin already checked, but the owner trusts the forwarding peer only for *identity relay*, not for the decision). Audit the decision (`on-behalf-of` = the forwarded principal).

- [ ] **Step 3: Thread identity through produce→forward.** `ProduceCore::produce` (the forwarding entry) must receive the caller principal so the `Forwarder::forward` call carries it. Add a `principal: &Principal` parameter (or a small `CallerCtx`) to `produce`/the forward path; update `handlers::send`/`streaming` call sites to pass the resolved principal. (When authz is unconfigured/AllowAll, the principal is Anonymous and the owner's AllowAll allows it ⇒ unchanged.)

- [ ] **Step 4: Gates + commit.** Existing forwarding tests still pass (Anonymous + AllowAll). Commit `feat(gateway): forward caller identity so the owning replica re-authorizes`.

---

## Task 7: Binary wiring

**Files:** Modify `crates/grpc-gateway/src/bin/gateway.rs`.

- [ ] CLI/config: `--authz` (off|simple), `--authz-super-users` (comma list), `--acl-refresh-secs` (default 30); bearer: `--bearer` (off|unsecured|jwks ...) + its inputs. Build `Arc<dyn Authorizer>` (`AllowAllAuthorizer` default; `SimpleAclAuthorizer::new(super_users)` when `--authz simple`), build `GatewayAuthz::new(authorizer)`, spawn `run_acl_refresh` under the shutdown token (only when authz != off), build the optional `OAuthBearerValidator`, layer `resolve_principal` middleware + the `BearerValidator` extension onto the router, put `authz` into `AppState`. Commit `feat(gateway): wire authorizer + ACL refresh + bearer auth into the binary`.

---

## Task 8: Integration tests

**Files:** Create `crates/grpc-gateway/tests/authz.rs` (+ extend `tls.rs`/`forwarding.rs` if convenient).

- [ ] Tests (boot in-process broker; create ACLs via `AdminClient::create_acls`):
  1. **Default AllowAll ⇒ unrestricted** (sanity: produce/consume with no authz config works — covered by existing tests, add one explicit assertion).
  2. **SimpleAcl denies unauthorized produce**: configure `SimpleAclAuthorizer` + an ACL cache built from an empty/space ACL set; a keyed/unkeyed `Send` for a topic the principal lacks `Write` on ⇒ per-record `Unauthorized` (PERMISSION_DENIED), nothing produced.
  3. **SimpleAcl allows authorized produce**: `create_acls` granting `User:alice Write Topic:t`; with principal `alice`, `Send` to `t` succeeds; to `other` denied.
  4. **Subscribe authz**: Read on Group + Topic required; missing ⇒ denied.
  5. **Bearer token → principal → authz**: a request with a valid unsecured-JWS bearer for `alice` is authorized per alice's ACLs.
  6. **Identity forwarding re-authz**: two gateways; a record owned by B, submitted through A as `alice`; B re-authorizes `alice`'s `Write` on the topic (allowed if ACL present, denied otherwise) — proving the owner enforces the forwarded identity, not the gateway's.
  7. **Audit log**: assert an audit event is emitted on a decision (capture via a tracing subscriber, or assert behaviorally).
- [ ] Re-run timing-sensitive tests 3×. Gates. Commit `test(gateway): identity→ACL authz, bearer, forwarding re-authz, audit`.

---

## Final review + finish

Dispatch a final adversarial reviewer over the whole P5 diff focusing on: (1) **broker behavior-preservation** — the factor is byte-exact, broker tests green, the 16 authorizer tests moved with identical semantics, OPA still works; (2) **no ACL drift** — `AclCache::matching_acls` ≡ `MetadataImage::matching_acls` (the cross-validation test); (3) **authz is config-gated** — default AllowAll ⇒ all prior tests unchanged; (4) **identity forwarding** — the owner authorizes the *forwarded caller*, and the forward channel's mTLS still authenticates the *peer gateway* (two distinct identities); (5) **default-deny safety** — a misconfigured/empty ACL set denies (no accidental allow-all when SimpleAcl is selected); (6) **no cycle** (authz is a leaf); (7) scope — no broker-side on-behalf-of wire header, no schema-registry coupling. Then finish the branch (push + PR stacked on #406 / rebased to main).

## Self-review notes (author)

- **Spec coverage (§4 / roadmap P5):** factor `crabka-authz` ✓ (shared, one source of truth); trusted-proxy authorizer ✓; ACL-snapshot cache via DescribeAcls ✓ (poll); identity → ACL (mTLS + bearer) ✓; on-behalf-of auditing ✓ (gateway-side structured audit; broker-side wire header explicitly deferred); identity forwarding ✓ (owner re-authorizes the forwarded caller). super_users ✓.
- **Broker-untouched exception:** P5 is the first slice to modify broker source, by explicit user choice ("factor out of broker"). The change is a pure-logic move + re-export — Kafka wire stays byte-exact; broker tests are the guardrail.
- **Drift risk** is the one real correctness hazard: the gateway's `AclCache` matching must equal the broker's `MetadataImage` matching. Mitigated by the cross-validation test in Task 1 Step 7 + keeping the *decision* logic (deny-wins, implies) literally shared in `crabka-authz`.
- **Config-gated default-AllowAll** keeps every prior gateway + broker test green and makes authz opt-in.
- **Greenfield:** no compat shims; `authz: Option<_>` is a config seam (opt-in), not a compat toggle.
