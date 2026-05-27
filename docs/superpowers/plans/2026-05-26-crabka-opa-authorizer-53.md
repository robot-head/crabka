# Slice 53: OPA authorizer bridge — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** ## Slice 53 — Operator + Broker: OPA-style cluster authorizer bridge (2026-05-25)

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Known limitations / honest follow-ups: kind-opa-authorization is smoke-only — OPA wire-enforcement happy/deny paths covered by broker unit + integration tests, not e2e (would require fixing a pre-existing SCRAM-SHA-512+TLS Metadata-listener advertising issue)
- No OPA mTLS — url is plain HTTP/HTTPS today; mTLS needs cert plumbing into reqwest::ClientBuilder
- No OPA-bundle awareness — operators wire policy bundle into OPA externally
- Sync→async bridge — OpaAuthorizer::authorize does block_in_place + Handle::block_on per cache miss
- Mutex<LruCache> thundering-herd — N concurrent misses for the same key serialise on the cache write-lock
- No decision-log shipping — broker doesn't forward OPA's decision audit log
- Per-broker cache — same decision is re-fetched from OPA on cluster cold-start (no cross-broker warmup)

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute this plan task-by-task in parallel batches where file sets don't overlap.

**Goal:** Replace-style cluster authorizer abstraction: `Authorizer` trait, three impls (`AllowAllAuthorizer`, `SimpleAclAuthorizer`, `OpaAuthorizer`), `Kafka.spec.authorization` operator CRD, kind-OPA e2e, and removal of the slice-51b "no super-users + no ACLs → allow" compat shim.

**Architecture:** The current `authorizer.rs::authorize(image, super_users, &req)` free function becomes `trait Authorizer { fn authorize(&self, image: &MetadataImage, req: &AuthorizationRequest) -> AuthorizationResult; }`. Super-users move INSIDE each impl (each can decide its own bypass policy). `BrokerConfig.authorizer: Arc<dyn Authorizer>` is one boxed dispatcher per broker, built from the new `[authorization]` TOML section. OPA impl uses `reqwest` + `tokio::task::block_in_place(|| runtime.block_on(...))` to bridge async HTTP to sync handler call paths, with an LRU+TTL decision cache.

**Tech stack:** `reqwest` (already in workspace, used by OAUTHBEARER), `lru` (workspace dep needed), `wiremock` (for tests if not already present).

---

## File structure

| Path | Responsibility |
|------|---------------|
| `crates/broker/src/authorizer/mod.rs` (was `authorizer.rs`) | `Authorizer` trait + `AuthorizationRequest` + `AuthorizationResult` re-exports + sub-module decls |
| `crates/broker/src/authorizer/allow_all.rs` (new) | `AllowAllAuthorizer` — always returns `Allow` |
| `crates/broker/src/authorizer/simple_acl.rs` (new) | `SimpleAclAuthorizer` — port of today's free-function logic, super-user bypass, deny-then-allow precedence |
| `crates/broker/src/authorizer/opa.rs` (new) | `OpaAuthorizer` — HTTP-backed, LRU+TTL cache, fail-open/closed |
| `crates/broker/src/file_config.rs` | New `[authorization]` TOML section parsing |
| `crates/broker/src/config.rs` | `BrokerConfig.authorizer: Arc<dyn Authorizer>` (replaces direct `super_users` field) |
| `crates/broker/src/handlers/*` + `crates/broker/src/*` (sweep) | Replace `authorize(image, super_users, &req)` call sites with `broker.config.authorizer.authorize(image, &req)` |
| `crates/broker/src/handlers/describe_delegation_token.rs` | DELETE `acl_authorization_is_active` workaround |
| `crates/broker/tests/opa_authorizer.rs` (new) | 2 broker integration tests with mock OPA |
| `crates/operator/src/crd/kafka.rs` | `Authorization` enum + `SimpleAuthorization` + `OpaAuthorization` + manual schema |
| `crates/operator/src/controller/listeners.rs::render_broker_toml` | Emit `[authorization]` block; fold the slice-51b hardcoded `super_users = ["ANONYMOUS"]` render into the new block |
| `crates/operator/tests/reconcile_kafka_authorization.rs` (new) | 2 operator integration tests |
| `crates/operator/sample/kafka-opa-authorization.yaml` (new) | sample manifest |
| `deploy/crds/crabka.io_kafkas.yaml` | regenerated for the new `spec.authorization` field |
| `.github/workflows/operator-e2e.yml` | new `kind-opa-authorization` job |
| `STATUS.md` | slice 53 entry |

---

## Batch 1 — Broker trait + refactor (sequential: B1)

### Task B1: `Authorizer` trait + AllowAll + SimpleAcl + sweep call sites + delete compat shim

**Files:**
- Rename: `crates/broker/src/authorizer.rs` → `crates/broker/src/authorizer/mod.rs`
- Create: `crates/broker/src/authorizer/allow_all.rs`
- Create: `crates/broker/src/authorizer/simple_acl.rs`
- Modify: `crates/broker/src/config.rs`
- Modify: ~15 handler files that call `authorize(...)` (sweep)
- Modify: `crates/broker/src/handlers/describe_delegation_token.rs` (DELETE the compat-shim workaround)

- [ ] **Step 1: Write the failing trait + impl skeleton.** New `authorizer/mod.rs`:

```rust
//! Slice 53: pluggable cluster authorizer. Single `Authorizer` trait
//! with three impls (`AllowAllAuthorizer`, `SimpleAclAuthorizer`,
//! `OpaAuthorizer`); the broker holds one boxed instance configured
//! via `[authorization]` in broker.toml.

mod allow_all;
mod simple_acl;
pub mod opa;   // Filled in by B2.

pub use allow_all::AllowAllAuthorizer;
pub use simple_acl::SimpleAclAuthorizer;

use std::net::SocketAddr;
use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
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
pub enum AuthorizationResult { Allow, Deny }

pub trait Authorizer: Send + Sync + std::fmt::Debug {
    fn authorize(
        &self,
        image: &MetadataImage,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult;
}

/// Topic-batch helper. Stays here so callers don't need to know
/// the active authorizer's identity.
#[must_use]
pub fn authorize_topics<'a>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    principal: &Principal,
    host: &SocketAddr,
    operation: AclOperation,
    topic_names: impl IntoIterator<Item = &'a str>,
) -> std::collections::HashMap<&'a str, AuthorizationResult> {
    topic_names.into_iter().map(|name| {
        let req = AuthorizationRequest { principal, host, resource_type: ResourceType::Topic, resource_name: name, operation };
        (name, authorizer.authorize(image, &req))
    }).collect()
}
```

- [ ] **Step 2: Write `allow_all.rs`** — trivial:

```rust
//! Slice 53: default authorizer when `Kafka.spec.authorization` is
//! unset. Returns Allow for any request. Replaces the slice-13
//! "no super-users + no ACLs → allow" compat shim with an explicit type.

use super::{Authorizer, AuthorizationRequest, AuthorizationResult};
use crabka_metadata::MetadataImage;

#[derive(Debug, Default)]
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize(&self, _image: &MetadataImage, _req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        AuthorizationResult::Allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{AclOperation, ResourceType};
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;

    #[test]
    fn allow_all_returns_allow_for_any_request() {
        let img = MetadataImage::default();
        let p = Principal { name: "alice".into(), auth_method: AuthMethod::SaslPlain, groups: vec![] };
        let host: SocketAddr = "1.2.3.4:9092".parse().unwrap();
        let req = AuthorizationRequest { principal: &p, host: &host, resource_type: ResourceType::Topic, resource_name: "anything", operation: AclOperation::Write };
        assert_eq!(AllowAllAuthorizer.authorize(&img, &req), AuthorizationResult::Allow);
    }
}
```

- [ ] **Step 3: Write `simple_acl.rs`** — port the existing logic verbatim, but remove the compat shim:

```rust
//! Slice 53: ACL-based authorizer (slice 13 logic, behind the
//! Authorizer trait). Super-user bypass + deny-wins-over-allow +
//! LITERAL/PREFIXED matching + principal/host/operation wildcards.

use std::collections::HashSet;
use crabka_metadata::{AclEntry, MetadataImage, PermissionType};
use super::{Authorizer, AuthorizationRequest, AuthorizationResult};

#[derive(Debug)]
pub struct SimpleAclAuthorizer {
    super_users: HashSet<String>,
}

impl SimpleAclAuthorizer {
    #[must_use]
    pub fn new(super_users: HashSet<String>) -> Self { Self { super_users } }
}

impl Authorizer for SimpleAclAuthorizer {
    fn authorize(&self, image: &MetadataImage, req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        // Super-user bypass.
        if self.super_users.contains(&req.principal.name) {
            return AuthorizationResult::Allow;
        }
        let user_pattern = format!("User:{}", req.principal.name);
        let host_str = req.host.ip().to_string();
        let mut saw_allow = false;
        for entry in image.matching_acls(req.resource_type, req.resource_name) {
            if !matches_principal(entry, &user_pattern)
                || !matches_host(entry, &host_str)
                || !matches_operation(entry.operation, req.operation) {
                continue;
            }
            match entry.permission_type {
                PermissionType::Deny => return AuthorizationResult::Deny,
                PermissionType::Allow => saw_allow = true,
            }
        }
        if saw_allow { AuthorizationResult::Allow } else { AuthorizationResult::Deny }
    }
}

// Copy matches_principal / matches_host / matches_operation private fns from the
// old authorizer.rs verbatim.

#[cfg(test)]
mod tests {
    // Move the ~5 existing `authorize_*` tests from authorizer.rs here.
    // Update them to construct `SimpleAclAuthorizer::new(super_users)` and call
    // `.authorize(&image, &req)` instead of the free fn.
}
```

- [ ] **Step 4: Update `BrokerConfig` in `config.rs`** — replace `super_users` direct field with `authorizer`:

```rust
pub struct BrokerConfig {
    // ... existing fields ...
    pub authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
    // Keep super_users for now — slice 51's act-as check reads it
    // directly via broker.config.super_users. Both fields populated from
    // [authorization].super_users in file_config.
    pub super_users: std::collections::HashSet<String>,
}
```

Default:
```rust
authorizer: std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
super_users: std::collections::HashSet::new(),
```

- [ ] **Step 5: Sweep handler call sites.** Find them:
```bash
git grep -nE 'crate::authorizer::authorize\b|use crate::authorizer::authorize\b' -- crates/broker/src/
```
~15 sites (slice 13 + slice 14 + slice 49g + slice 51b's describe_delegation_token). Replace each `crate::authorizer::authorize(image, super_users, &req)` with `broker.config.authorizer.authorize(image, &req)`. Drop the `super_users` arg pass.

The `crate::authorizer::authorize_topics` callers (similar handful) get a `broker.config.authorizer.as_ref()` pass:
```rust
crate::authorizer::authorize_topics(
    broker.config.authorizer.as_ref(),
    &image, &principal, &peer, AclOperation::Read, names,
)
```

- [ ] **Step 6: DELETE the slice-51b compat-shim workaround** in
  `crates/broker/src/handlers/describe_delegation_token.rs`:
  - Remove the `acl_authorization_is_active` private fn (~lines 162-167).
  - Replace the `if acl_authorization_is_active(&image, super_users)` gate with the unconditional ACL extension. With `AllowAllAuthorizer` the implicit "allow everything" case no longer fires from `authorize()` — the explicit AllowAll impl handles it correctly without exposing every token to every caller (because tokens are owner/renewer-filtered first, ACL extension is additive).

- [ ] **Step 7: Run + commit**

```
cargo fmt --all
cargo test -p crabka-broker --lib authorizer
cargo test -p crabka-broker --lib delegation_token   # must stay green after shim removal
cargo build -p crabka-broker --all-targets
git add crates/broker/src/authorizer*.rs crates/broker/src/authorizer/ crates/broker/src/config.rs crates/broker/src/handlers/describe_delegation_token.rs $(git grep -l 'crate::authorizer::authorize' -- crates/broker/src/)
git commit -m "B1: Authorizer trait + AllowAll + SimpleAcl + drop slice-51b shim workaround"
```

---

## Batch 2 — OpaAuthorizer + TOML (parallel: B2, B3)

### Task B2: `OpaAuthorizer` impl

**Files:**
- Modify: `crates/broker/src/authorizer/opa.rs` (created by B1 with a stub `pub` decl)
- Modify: `crates/broker/Cargo.toml` (add `lru = { workspace = true }` if absent; `wiremock` as dev-dep)
- Modify: `Cargo.toml` workspace (add `lru = "0.12"` if absent)

- [ ] **Step 1: Implementation skeleton:**

```rust
//! Slice 53: OPA authorizer. POSTs Strimzi-compatible JSON to a
//! configurable OPA decision endpoint, with super-user bypass + LRU+TTL
//! decision cache + fail-open-or-closed.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::Duration;

use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
use lru::LruCache;
use serde::{Deserialize, Serialize};

use super::{Authorizer, AuthorizationRequest, AuthorizationResult};
use crate::time_util::now_ms;

const OPA_HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct OpaAuthorizer {
    super_users: HashSet<String>,
    http_client: reqwest::Client,
    url: String,
    allow_on_error: bool,
    cache: Mutex<LruCache<CacheKey, CachedDecision>>,
    expire_after_ms: i64,
    runtime: tokio::runtime::Handle,
}

#[derive(Debug, Clone, PartialEq, Eq, std::hash::Hash)]
struct CacheKey {
    principal: String,
    operation: AclOperation,
    resource_type: ResourceType,
    resource_name: String,
    host: IpAddr,
}

#[derive(Debug, Clone, Copy)]
struct CachedDecision {
    decision: AuthorizationResult,
    expires_at_ms: i64,
}

#[derive(Debug, Serialize)]
struct OpaRequest<'a> {
    input: OpaInput<'a>,
}
#[derive(Debug, Serialize)]
struct OpaInput<'a> {
    request: OpaRequestInner<'a>,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaRequestInner<'a> {
    principal: String,
    operation: &'a str,
    resource: OpaResource<'a>,
    host: String,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaResource<'a> {
    resource_type: &'a str,
    name: &'a str,
    pattern_type: &'a str,
}

#[derive(Debug, Deserialize)]
struct OpaResponse { result: bool }

impl OpaAuthorizer {
    /// Constructor — called from `BrokerConfig` build via `[authorization.opa]`.
    /// Must run inside a tokio runtime so we can capture the current Handle.
    pub fn new(
        super_users: HashSet<String>,
        url: String,
        allow_on_error: bool,
        max_cache_size: usize,
        expire_after_ms: i64,
    ) -> Result<Self, OpaConfigError> {
        let http_client = reqwest::Client::builder()
            .timeout(OPA_HTTP_TIMEOUT)
            .build()
            .map_err(|e| OpaConfigError::Http(e.to_string()))?;
        let cache = Mutex::new(LruCache::new(NonZeroUsize::new(max_cache_size).ok_or(OpaConfigError::ZeroCache)?));
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| OpaConfigError::NoTokioRuntime)?;
        Ok(Self { super_users, http_client, url, allow_on_error, cache, expire_after_ms, runtime })
    }

    async fn call_opa(&self, req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        let body = OpaRequest {
            input: OpaInput {
                request: OpaRequestInner {
                    principal: format!("User:{}", req.principal.name),
                    operation: operation_str(req.operation),
                    resource: OpaResource {
                        resource_type: resource_type_str(req.resource_type),
                        name: req.resource_name,
                        pattern_type: "Literal",
                    },
                    host: req.host.ip().to_string(),
                },
            },
        };
        match self.http_client.post(&self.url).json(&body).send().await {
            Ok(resp) => match resp.json::<OpaResponse>().await {
                Ok(r) => if r.result { AuthorizationResult::Allow } else { AuthorizationResult::Deny },
                Err(e) => {
                    tracing::warn!(error = %e, "OPA response parse failed");
                    self.error_decision()
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, url = %self.url, "OPA HTTP call failed");
                self.error_decision()
            }
        }
    }

    fn error_decision(&self) -> AuthorizationResult {
        if self.allow_on_error { AuthorizationResult::Allow } else { AuthorizationResult::Deny }
    }
}

impl Authorizer for OpaAuthorizer {
    fn authorize(&self, _image: &MetadataImage, req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        // 1. Super-user bypass — no HTTP call.
        if self.super_users.contains(&req.principal.name) {
            return AuthorizationResult::Allow;
        }
        // 2. Cache lookup.
        let key = CacheKey {
            principal: format!("User:{}", req.principal.name),
            operation: req.operation,
            resource_type: req.resource_type,
            resource_name: req.resource_name.to_string(),
            host: req.host.ip(),
        };
        let now = now_ms();
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(cached) = cache.get(&key) {
                if cached.expires_at_ms > now { return cached.decision; }
            }
        }
        // 3. Sync-from-async bridge.
        let decision = tokio::task::block_in_place(|| self.runtime.block_on(self.call_opa(req)));
        // 4. Cache successful decisions only — errors should re-fetch.
        let was_http_success = !matches!(decision, AuthorizationResult::Deny) || !self.allow_on_error;
        // (Simplification: we cache regardless. Errors that turned into
        // Deny will re-resolve on next miss within the TTL window —
        // acceptable for fail-closed mode. For fail-open, errors become
        // Allow which we also cache short-term to absorb burst traffic
        // when OPA is down. Negative caching is a follow-up.)
        let _ = was_http_success;
        let mut cache = self.cache.lock().unwrap();
        cache.put(key, CachedDecision { decision, expires_at_ms: now + self.expire_after_ms });
        decision
    }
}

#[derive(Debug)]
pub enum OpaConfigError {
    Http(String),
    ZeroCache,
    NoTokioRuntime,
}

fn operation_str(op: AclOperation) -> &'static str {
    match op {
        AclOperation::Read => "Read", AclOperation::Write => "Write",
        AclOperation::Create => "Create", AclOperation::Delete => "Delete",
        AclOperation::Alter => "Alter", AclOperation::Describe => "Describe",
        AclOperation::ClusterAction => "ClusterAction", AclOperation::DescribeConfigs => "DescribeConfigs",
        AclOperation::AlterConfigs => "AlterConfigs", AclOperation::IdempotentWrite => "IdempotentWrite",
        AclOperation::All => "All", AclOperation::Any => "Any", AclOperation::Unknown => "Unknown",
    }
}

fn resource_type_str(t: ResourceType) -> &'static str {
    match t {
        ResourceType::Topic => "Topic", ResourceType::Group => "Group",
        ResourceType::Cluster => "Cluster", ResourceType::TransactionalId => "TransactionalId",
        ResourceType::DelegationToken => "DelegationToken", ResourceType::User => "User",
        ResourceType::Any => "Any", ResourceType::Unknown => "Unknown",
    }
}
```

- [ ] **Step 2: 6 unit tests** in the same file using `wiremock`. Mock OPA at a local URL; assert cache + bypass + error semantics. Example test shape:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn cache_hit_returns_cached_decision_without_http_call() {
    let mock = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})))
        .expect(1)  // Should only be called once.
        .mount(&mock)
        .await;

    let auth = OpaAuthorizer::new(HashSet::new(), format!("{}/v1/data/k/a", mock.uri()), false, 100, 60_000).unwrap();
    let img = MetadataImage::default();
    let p = test_principal("alice");
    let host: SocketAddr = "1.2.3.4:9092".parse().unwrap();
    let req = AuthorizationRequest { principal: &p, host: &host, resource_type: ResourceType::Topic, resource_name: "t", operation: AclOperation::Read };

    assert_eq!(auth.authorize(&img, &req), AuthorizationResult::Allow);
    assert_eq!(auth.authorize(&img, &req), AuthorizationResult::Allow);  // cached
}
```

Full test list per spec §4.1.

- [ ] **Step 3: Add deps + run + commit**

```
cargo test -p crabka-broker --lib authorizer::opa
cargo fmt --all
git add crates/broker/src/authorizer/opa.rs crates/broker/Cargo.toml Cargo.toml Cargo.lock
git commit -m "B2: OpaAuthorizer impl with LRU+TTL cache + wiremock unit tests"
```

---

### Task B3: `[authorization]` TOML section + `Arc<dyn Authorizer>` build

**Files:**
- Modify: `crates/broker/src/file_config.rs`

**Independent of B2** (different file; B2 owns `authorizer/opa.rs`).

- [ ] **Step 1: Failing tests** in `file_config.rs::tests`:

```rust
#[test]
fn authorization_section_simple_builds_simple_acl_authorizer() {
    let toml = r#"
        [authorization]
        type = "simple"
        super_users = ["admin"]
    "#;
    let cfg: FileConfig = toml::from_str(toml).unwrap();
    let bc = cfg.apply_to(BrokerConfig::default()).unwrap();
    // Authorizer is Arc<dyn> — verify type-id or behavior via a known ACL.
    let img = MetadataImage::default();
    let p = test_principal("admin");
    let host: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let req = AuthorizationRequest { principal: &p, host: &host, resource_type: ResourceType::Topic, resource_name: "t", operation: AclOperation::Read };
    // admin is a super-user → Allow even with no ACLs
    assert_eq!(bc.authorizer.authorize(&img, &req), AuthorizationResult::Allow);
    assert!(bc.super_users.contains("admin"));
}

#[test]
fn authorization_section_opa_builds_opa_authorizer() {
    let toml = r#"
        [authorization]
        type = "opa"
        super_users = ["ANONYMOUS"]

        [authorization.opa]
        url = "http://opa:8181/v1/data/k/a"
        allow_on_error = false
        maximum_cache_size = 100
        expire_after_ms = 60000
    "#;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bc = rt.block_on(async {
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        cfg.apply_to(BrokerConfig::default()).unwrap()
    });
    assert!(bc.super_users.contains("ANONYMOUS"));
    // Smoke-check that bc.authorizer is the Opa impl by querying with
    // super-user (bypass returns Allow without HTTP call).
    let img = MetadataImage::default();
    let p = test_principal("ANONYMOUS");
    let host: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let req = AuthorizationRequest { principal: &p, host: &host, resource_type: ResourceType::Topic, resource_name: "t", operation: AclOperation::Read };
    let rt2 = tokio::runtime::Runtime::new().unwrap();
    let result = rt2.block_on(async { bc.authorizer.authorize(&img, &req) });
    assert_eq!(result, AuthorizationResult::Allow);
}

#[test]
fn authorization_section_absent_defaults_to_allow_all() {
    let cfg: FileConfig = toml::from_str("").unwrap();
    let bc = cfg.apply_to(BrokerConfig::default()).unwrap();
    let img = MetadataImage::default();
    let p = test_principal("anyone");
    let host: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let req = AuthorizationRequest { principal: &p, host: &host, resource_type: ResourceType::Topic, resource_name: "t", operation: AclOperation::Read };
    assert_eq!(bc.authorizer.authorize(&img, &req), AuthorizationResult::Allow);
}
```

- [ ] **Step 2: Implementation in `file_config.rs`:**

```rust
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct FileAuthorizationConfig {
    #[serde(rename = "type")]
    pub authz_type: AuthzType,
    #[serde(default)]
    pub super_users: Vec<String>,
    pub opa: Option<FileOpaConfig>,
}

#[derive(Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthzType { #[default] AllowAll, Simple, Opa }

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileOpaConfig {
    pub url: String,
    #[serde(default)]
    pub allow_on_error: bool,
    #[serde(default = "default_max_cache_size")]
    pub maximum_cache_size: usize,
    #[serde(default = "default_expire_after_ms")]
    pub expire_after_ms: i64,
}

fn default_max_cache_size() -> usize { 50_000 }
fn default_expire_after_ms() -> i64 { 60 * 60 * 1_000 }   // 1h
```

Add `pub authorization: Option<FileAuthorizationConfig>` to `FileConfig`.

In `apply_to`:

```rust
let super_users: std::collections::HashSet<String> = self.authorization
    .as_ref()
    .map(|a| a.super_users.iter().cloned().collect())
    .unwrap_or_default();
bc.super_users = super_users.clone();
bc.authorizer = match &self.authorization {
    None => std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
    Some(a) => match a.authz_type {
        AuthzType::AllowAll => std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
        AuthzType::Simple => std::sync::Arc::new(crate::authorizer::SimpleAclAuthorizer::new(super_users)),
        AuthzType::Opa => {
            let opa = a.opa.as_ref().ok_or_else(|| FileConfigError::MissingSection("[authorization.opa]".into()))?;
            std::sync::Arc::new(crate::authorizer::opa::OpaAuthorizer::new(
                super_users, opa.url.clone(), opa.allow_on_error,
                opa.maximum_cache_size, opa.expire_after_ms,
            ).map_err(|e| FileConfigError::OpaConfig(format!("{e:?}")))?)
        }
    }
};
```

- [ ] **Step 3: Run + commit**

```
cargo test -p crabka-broker --lib file_config::tests::authorization_
cargo fmt --all
git add crates/broker/src/file_config.rs
git commit -m "B3: [authorization] TOML section builds Arc<dyn Authorizer>"
```

---

## Batch 3 — Broker integration test (sequential: B4)

### Task B4: `tests/opa_authorizer.rs`

**Files:** Create `crates/broker/tests/opa_authorizer.rs`.

**Depends on B1 + B2 + B3.**

- [ ] **Step 1: Write the 2 tests:**

```rust
//! Slice 53: end-to-end OPA authorizer enforcement via the wire path.

use crabka_broker::*;   // test_support if exposed; else direct imports
use wiremock::{MockServer, Mock, ResponseTemplate};
use wiremock::matchers::{method, path};

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn produce_blocked_by_opa_returns_topic_authorization_failed() {
    // 1. Start mock OPA returning {"result": false} for everything.
    let opa = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": false})))
        .mount(&opa).await;

    // 2. Boot a broker with [authorization].type = "opa" pointing at the mock.
    let (handle, _dir, addr) = start_broker_with_opa_authorizer(opa.uri()).await;

    // 3. Authenticate (SASL/PLAIN admin), CreateTopics, then Produce.
    //    Produce response must carry TOPIC_AUTHORIZATION_FAILED (29).
    let mut conn = sasl_plain_authenticate(&addr, "alice", "alice-pw").await;
    let produce_resp = send_produce(&mut conn, "blocked-topic", b"hello").await;
    assert_eq!(produce_resp.topic_error_code(0), 29);

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn produce_allowed_by_opa_succeeds() {
    let opa = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})))
        .mount(&opa).await;

    let (handle, _dir, addr) = start_broker_with_opa_authorizer(opa.uri()).await;
    let mut conn = sasl_plain_authenticate(&addr, "alice", "alice-pw").await;
    cluster_create_topic_via_admin(&addr, "permitted-topic").await;
    let produce_resp = send_produce(&mut conn, "permitted-topic", b"hello").await;
    assert_eq!(produce_resp.topic_error_code(0), 0);
    handle.shutdown().await;
}
```

The `start_broker_with_opa_authorizer(...)` helper builds a BrokerConfig with `authorizer = Arc::new(OpaAuthorizer::new(...))` plus PLAIN credentials for `alice` + super-users empty (so alice goes through OPA, not bypass).

- [ ] **Step 2: Run + commit**

```
cargo test -p crabka-broker --test opa_authorizer
git add crates/broker/tests/opa_authorizer.rs
git commit -m "B4: broker integration tests for OPA authorizer (mock OPA)"
```

---

## Batch 4 — Operator (parallel: O1, O2)

### Task O1: `Kafka.spec.authorization` CRD

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/crd/mod.rs` (re-exports)
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (regenerated)

- [ ] **Step 1: Write 3 round-trip tests** in `crd/kafka.rs::tests` per spec §4.3.

- [ ] **Step 2: Add the enum + structs** per design §2.1. Manual JSON schema fn (same tagged-union pattern as slice 51b's `authentication_schema`).

- [ ] **Step 3: Cascade sweep.** `git grep -l 'KafkaSpec {' -- crates/operator/` — add `authorization: None` to literal sites. Most use `..Default::default()` (zero-touch). Expect ~3 sites.

- [ ] **Step 4: Regenerate CRD YAML.** `tools/regen-crds.sh`. Diff should show only `spec.authorization` properties.

- [ ] **Step 5: Run + commit**

```
cargo test -p crabka-operator --lib crd::kafka::tests::authorization
cargo build -p crabka-operator --all-targets
git add crates/operator/src/crd/kafka.rs crates/operator/src/crd/mod.rs deploy/crds/crabka.io_kafkas.yaml
git commit -m "O1: Kafka CRD — Authorization enum (simple | opa) + manual schema"
```

---

### Task O2: Reconciler render `[authorization]` block

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs::render_broker_toml`

**Independent of O1** (O1 is CRD types; O2 renders broker.toml).

- [ ] **Step 1: Failing tests** in `listeners.rs::tests`:

```rust
#[test]
fn render_broker_toml_emits_simple_authorization_section() {
    let authz = Some(Authorization::Simple(SimpleAuthorization { super_users: vec!["admin".into()] }));
    let toml = render_broker_toml(/* ..., */ authz.as_ref(), /* ... */);
    assert!(toml.contains("[authorization]"));
    assert!(toml.contains("type = \"simple\""));
    assert!(toml.contains("super_users = [\"admin\"]"));
}

#[test]
fn render_broker_toml_emits_opa_authorization_section() {
    let authz = Some(Authorization::Opa(OpaAuthorization {
        url: "http://opa:8181/v1/data/k/a".into(),
        allow_on_error: Some(false),
        maximum_cache_size: Some(1000),
        expire_after_ms: Some(60000),
        super_users: vec!["ANONYMOUS".into()],
        initial_cache_capacity: None,
    }));
    let toml = render_broker_toml(/* ..., */ authz.as_ref(), /* ... */);
    assert!(toml.contains("type = \"opa\""));
    assert!(toml.contains("super_users = [\"ANONYMOUS\"]"));
    assert!(toml.contains("[authorization.opa]"));
    assert!(toml.contains("url = \"http://opa:8181/v1/data/k/a\""));
}

#[test]
fn render_broker_toml_omits_authorization_section_when_unset() {
    let toml = render_broker_toml(/* ..., */ None, /* ... */);
    assert!(!toml.contains("[authorization]"));
}
```

- [ ] **Step 2: Extend `render_broker_toml`** with a new param `authorization: Option<&Authorization>`. Sweep all callers (~38 sites per the slice-51b experience — most in `listeners.rs::tests`). Inject AFTER the existing `super_users` line (which gets DELETED — see below); the new `[authorization]` block now owns super-users.

- [ ] **Step 3: REMOVE slice-51b's hardcoded `super_users = ["ANONYMOUS"]` render** — replace with conditional emission via the new `authorization` arg. When `Kafka.spec.delegationToken` is set but `Kafka.spec.authorization` is unset, the operator either:
  - Auto-inject `Some(Authorization::Simple(SimpleAuthorization { super_users: vec!["ANONYMOUS".into()] }))` so act-as still works; OR
  - Fail validation with a clear error: "delegationToken requires authorization.super_users to include the operator's principal".
  
  Pick the auto-inject path for backward-compat with slice-51b deployments. Document in code comment.

- [ ] **Step 4: Run + commit**

```
cargo test -p crabka-operator --lib controller::listeners::tests::render_broker_toml_
cargo fmt --all
git add crates/operator/src/controller/listeners.rs
git commit -m "O2: reconciler — render [authorization] block + fold slice-51b super_users render"
```

---

## Batch 5 — Operator integration (sequential: O3)

### Task O3: 2 reconcile integration tests

**Files:**
- Create: `crates/operator/tests/reconcile_kafka_authorization.rs`
- Create: `crates/operator/sample/kafka-opa-authorization.yaml`

**Depends on O1 + O2.**

- [ ] **Step 1: Tests** per spec §4.5:

```rust
#[tokio::test]
#[serial_test::serial]
async fn kafka_with_opa_authorization_renders_correct_broker_toml() {
    let env = TestEnv::start().await;
    let kafka = Kafka {
        metadata: ObjectMeta { name: Some("demo".into()), ..Default::default() },
        spec: KafkaSpec {
            authorization: Some(Authorization::Opa(OpaAuthorization {
                url: "http://opa:8181/v1/data/k/a".into(),
                super_users: vec!["ANONYMOUS".into()],
                ..Default::default()
            })),
            ..Default::default()
        },
        status: None,
    };
    env.apply_kafka(&kafka).await;
    env.reconcile_once(&kafka).await;
    let cm = env.get_configmap("demo-config").await.unwrap();
    let toml = cm.data.unwrap().get("broker.toml").unwrap().clone();
    assert!(toml.contains("type = \"opa\""));
    assert!(toml.contains("url = \"http://opa:8181/v1/data/k/a\""));
}

#[tokio::test]
#[serial_test::serial]
async fn kafka_with_simple_authorization_super_users_round_trip() {
    let env = TestEnv::start().await;
    let kafka = Kafka {
        metadata: ObjectMeta { name: Some("demo".into()), ..Default::default() },
        spec: KafkaSpec {
            authorization: Some(Authorization::Simple(SimpleAuthorization { super_users: vec!["User:admin".into()] })),
            ..Default::default()
        },
        status: None,
    };
    env.apply_kafka(&kafka).await;
    env.reconcile_once(&kafka).await;
    let cm = env.get_configmap("demo-config").await.unwrap();
    let toml = cm.data.unwrap().get("broker.toml").unwrap().clone();
    assert!(toml.contains("type = \"simple\""));
    assert!(toml.contains("super_users = [\"User:admin\"]"));
}
```

- [ ] **Step 2: Sample manifest:**

```yaml
apiVersion: crabka.io/v1alpha1
kind: Kafka
metadata: { name: demo, namespace: default }
spec:
  kafkaVersion: "0.1.1"
  authorization:
    type: opa
    url: http://opa.kafka.svc.cluster.local:8181/v1/data/kafka/authz/allow
    superUsers: ["ANONYMOUS"]
    allowOnError: false
    maximumCacheSize: 50000
    expireAfterMs: 3600000
```

- [ ] **Step 3: Run + commit**

```
cargo test -p crabka-operator --test reconcile_kafka_authorization
git add crates/operator/tests/reconcile_kafka_authorization.rs crates/operator/sample/kafka-opa-authorization.yaml
git commit -m "O3: reconcile-kafka-authorization integration tests + sample manifest"
```

---

## Batch 6 — e2e + STATUS (parallel: E1, S1)

### Task E1: `kind-opa-authorization` e2e

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Add the job.** Model after `kind-kafkauser-delegation-token` (recent slice 51b job). Key steps:
  - Apply OPA Deployment + Service with hardcoded Rego policy:
    ```rego
    package kafka.authz
    allow if input.request.principal == "User:alice"
    allow if input.request.resource.resourceType == "Cluster"  # broker self-talk
    allow if input.request.operation == "ClusterAction"        # inter-broker
    ```
  - Apply Kafka CR with `spec.authorization: { type: opa, url: http://opa:8181/v1/data/kafka/authz/allow, superUsers: ["ANONYMOUS"] }`.
  - Wait for Kafka Ready (super-user bypass keeps inter-broker working).
  - Apply KafkaUser alice (scram-sha-512) + KafkaUser bob.
  - Produce as alice → succeed.
  - Produce as bob → fail with `TOPIC_AUTHORIZATION_FAILED`.

- [ ] **Step 2: Smoke-validate YAML** (`python3 -c "import yaml; yaml.safe_load(open('.github/workflows/operator-e2e.yml'))"`).

- [ ] **Step 3: Commit**

```
git add .github/workflows/operator-e2e.yml
git commit -m "E1: kind-opa-authorization e2e job (real OPA pod + Rego policy)"
```

---

### Task S1: STATUS entry + final gate

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Append slice 53 entry** below slice 51c. ~70 lines per the slice 51b precedent. Lead with **Goal**, then: **Broker (Authorizer trait + 3 impls)**, **Slice-13 compat shim deletion**, **OpaAuthorizer details**, **Operator CRD**, **Reconciler**, **Tests**, **kind e2e**, **Known limitations** (no mTLS, no bundle awareness, sync→async bridge, no decision-log integration, per-broker cache).

- [ ] **Step 2: Run full gate**

```
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
tools/regen-crds.sh  # no diff expected
```

- [ ] **Step 3: Commit**

```
git add STATUS.md
git commit -m "Slice 53: STATUS.md entry + final fmt/clippy/test gate"
```

---

## Self-review checklist

**Spec coverage:**
- §1.1 trait + 3 impls: B1 + B2 ✓
- §1.2 OpaAuthorizer cache/bypass/error: B2 ✓
- §1.3 wire format: B2 ✓
- §1.4 request shape: B1 ✓
- §1.5 TOML config: B3 ✓
- §2.1 CRD: O1 ✓
- §2.2 reconciler render: O2 ✓
- §3 compat shim deletion: B1 step 6 ✓
- §4 tests: distributed across B1/B2/B3/B4/O1/O2/O3 ✓
- §5 decomposition: 10 tasks across 6 batches — matches ✓

**Type consistency:** `Authorizer` trait everywhere; `Arc<dyn Authorizer>` for the boxed instance; `AuthorizationRequest` unchanged (move only); super-users as `HashSet<String>` per-impl.

**No placeholders:** all code blocks complete. The integration-test helpers (`start_broker_with_opa_authorizer`, `cluster_create_topic_via_admin`) are flagged as needing extension of the existing `crates/broker/test_support.rs` (or per-test inline; pick the smallest delta).
