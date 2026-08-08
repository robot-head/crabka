//! OPA authorizer. POSTs Strimzi-compatible JSON to a
//! configurable OPA decision endpoint. It adds a super-user bypass, an
//! LRU+TTL decision cache, and a fail-open-or-closed policy.
//!
//! The trait method [`Authorizer::authorize`] is synchronous, because sync
//! handler hot paths call it, but `reqwest` is async. This module bridges the
//! two with [`tokio::task::block_in_place`] and a captured runtime
//! [`tokio::runtime::Handle`]. That is acceptable for a tail authorization
//! check, which takes under a millisecond on a cache hit and low double-digit
//! milliseconds on a miss. A cache miss on a single-threaded
//! runtime would deadlock, but the broker is multi-thread.
//!
//! Cache semantics: the authorizer caches decisions on BOTH success and error.
//! Negative caching is deliberate. Under `allow_on_error = false` an
//! error becomes `Deny`, which is the safe behavior for a brief OPA
//! outage. Entries expire on TTL, so an OPA recovery is observable.

use std::{
    collections::HashSet,
    net::IpAddr,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use crabka_authz::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_units::{Time, convert::TimeExt as _, fmt::Human as _};
use lru::LruCache;
use serde::{Deserialize, Serialize};

/// HTTP-backed pluggable authorizer. Owns its `super_users` bypass set,
/// HTTP client, decision cache, and a captured `tokio::runtime::Handle`
/// so the synchronous [`Authorizer::authorize`] entry point can call
/// `reqwest`'s async API through `block_in_place`.
///
/// # Security
///
/// The `allow_on_error` knob is
/// **security-sensitive**. When it is `true`, any OPA outage (timeout,
/// 5xx, unparseable response) causes `error_decision`
/// to return `Allow`. An unreachable policy server then authorizes
/// *every* request, which is fail-open. The default is `false`, which is
/// fail-closed and matches the upstream Open Policy Agent Kafka plugin's
/// `allow.on.error = false`. Only enable fail-open in environments where
/// brief over-permission is strictly preferable to a block during an OPA
/// outage.
pub struct OpaAuthorizer {
    super_users: HashSet<String>,
    http_client: reqwest::Client,
    url: String,
    /// **Security-sensitive.** `true` ⇒ OPA errors authorize the request,
    /// which is fail-open. An OPA outage then authorizes every request. The
    /// secure default, which is also the upstream OPA Kafka plugin default,
    /// is `false` (fail-closed).
    allow_on_error: bool,
    cache: Mutex<LruCache<CacheKey, CachedDecision>>,
    expire_after: Time,
    runtime: tokio::runtime::Handle,
    /// Clock backing the decision-cache TTL (the `expires_at_ms` stamp and its
    /// expiry comparison). Production uses [`qubit_clock::SystemClock`], which
    /// is wall time. Tests inject a [`qubit_clock::MockClock`] so cache entries
    /// expire on a controlled timeline instead of a real `sleep`. The clock
    /// governs *only* cache freshness, never the authorization decision.
    clock: Arc<dyn qubit_clock::Clock>,
}

impl std::fmt::Debug for OpaAuthorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Skip `http_client`, `cache`, and `runtime` — they're not
        // `Debug`-friendly (Mutex would lock, Handle prints nothing
        // useful, Client prints the whole TLS config). Field-list is
        // operator-relevant config.
        f.debug_struct("OpaAuthorizer")
            .field("super_users", &self.super_users)
            .field("url", &self.url)
            .field("allow_on_error", &self.allow_on_error)
            .field(
                "expire_after",
                &format_args!("{}", self.expire_after.human()),
            )
            .finish_non_exhaustive()
    }
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

/// Outer envelope of the Strimzi-compatible OPA request.
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

/// Decision payload returned by OPA. Strimzi expects exactly
/// `{"result": true|false}`. Anything else parses as an error, and the
/// caller falls through to [`OpaAuthorizer::error_decision`].
#[derive(Debug, Deserialize)]
struct OpaResponse {
    result: bool,
}

impl OpaAuthorizer {
    /// Build a new OPA authorizer. The caller MUST call this from inside a
    /// tokio runtime, because the constructor captures the current `Handle`
    /// to drive async HTTP from the synchronous [`Authorizer::authorize`]
    /// entry point.
    ///
    /// # Errors
    ///
    /// * [`OpaConfigError::Http`] if the constructor cannot build the
    ///   `reqwest::Client`. A TLS misconfig is the realistic failure.
    /// * [`OpaConfigError::ZeroCache`] if `max_cache_size == 0`.
    /// * [`OpaConfigError::NoTokioRuntime`] if no tokio runtime is
    ///   active on the current thread.
    pub fn new(
        super_users: HashSet<String>,
        url: String,
        allow_on_error: bool,
        max_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
    ) -> Result<Self, OpaConfigError> {
        Self::with_clock(
            super_users,
            url,
            allow_on_error,
            max_cache_size,
            expire_after,
            http_timeout,
            Arc::new(qubit_clock::SystemClock::new()),
        )
    }

    /// Same as [`OpaAuthorizer::new`] but with a caller-supplied
    /// [`qubit_clock::Clock`] backing the decision-cache TTL. Production uses
    /// [`OpaAuthorizer::new`] with a [`qubit_clock::SystemClock`]. Tests pass a
    /// [`qubit_clock::MockClock`] so cached decisions expire on a controlled
    /// timeline without a real `sleep`. The clock affects *only* cache
    /// freshness, never the authorization decision.
    ///
    /// # Errors
    ///
    /// Same as [`OpaAuthorizer::new`].
    pub fn with_clock(
        super_users: HashSet<String>,
        url: String,
        allow_on_error: bool,
        max_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
        clock: Arc<dyn qubit_clock::Clock>,
    ) -> Result<Self, OpaConfigError> {
        let http_client = reqwest::Client::builder()
            .timeout(http_timeout.to_std())
            .build()
            .map_err(|e| OpaConfigError::Http(e.to_string()))?;
        let capacity = NonZeroUsize::new(max_cache_size).ok_or(OpaConfigError::ZeroCache)?;
        let cache = Mutex::new(LruCache::new(capacity));
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| OpaConfigError::NoTokioRuntime)?;
        Ok(Self {
            super_users,
            http_client,
            url,
            allow_on_error,
            cache,
            expire_after,
            runtime,
            clock,
        })
    }

    /// POST the request to OPA and translate the boolean response into
    /// the binary decision. Any HTTP-level or JSON-level error falls through
    /// to [`Self::error_decision`], which honours `allow_on_error`.
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
                Ok(r) => {
                    if r.result {
                        AuthorizationResult::Allow
                    } else {
                        AuthorizationResult::Deny
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, url = %self.url, "OPA response parse failed");
                    self.error_decision()
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, url = %self.url, "OPA HTTP call failed");
                self.error_decision()
            }
        }
    }

    /// What to return when OPA is unreachable or returned garbage.
    /// Fail-closed (`allow_on_error = false`, the default) denies, which is
    /// the secure behavior. Fail-open (`allow_on_error = true`) is
    /// **security-sensitive**. It authorizes every request for the
    /// duration of an OPA outage, and it is only for environments where
    /// a block during that outage is strictly worse than over-permission.
    fn error_decision(&self) -> AuthorizationResult {
        if self.allow_on_error {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    }
}

impl Authorizer for OpaAuthorizer {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        // 1. Super-user bypass — no HTTP, no cache touch.
        if self.super_users.contains(&req.principal.name) {
            return AuthorizationResult::Allow;
        }
        // 2. Cache lookup. We do NOT eagerly evict expired entries; the
        //    lookup just rejects them. Lazy eviction is good enough at
        //    LRU capacities measured in the tens of thousands.
        let key = CacheKey {
            principal: format!("User:{}", req.principal.name),
            operation: req.operation,
            resource_type: req.resource_type,
            resource_name: req.resource_name.to_string(),
            host: req.host.ip(),
        };
        // Cache-freshness timestamp only — read from the injected clock so tests
        // can expire entries on a mock timeline. Not part of the decision.
        let now = self.clock.millis();
        {
            let mut cache = self.cache.lock().expect("OPA cache mutex poisoned");
            if let Some(cached) = cache.get(&key)
                && cached.expires_at_ms > now
            {
                return cached.decision;
            }
        }
        // 3. Sync→async bridge. `block_in_place` releases the current
        //    worker for other tasks; the captured runtime drives the
        //    HTTP call on its own threads.
        let decision = tokio::task::block_in_place(|| self.runtime.block_on(self.call_opa(req)));
        // 4. Cache the decision — both successes AND errors. Negative
        //    caching keeps OPA outages from amplifying broker load;
        //    TTL expiry lets recovery propagate naturally.
        let mut cache = self.cache.lock().expect("OPA cache mutex poisoned");
        cache.put(
            key,
            CachedDecision {
                decision,
                expires_at_ms: now + self.expire_after.millis_i64(),
            },
        );
        decision
    }
}

/// Constructor-time failures for [`OpaAuthorizer::new`]. They travel
/// up through `file_config::FileConfigError` at broker startup, so a
/// misconfigured deployment fails at startup and not at the first request.
#[derive(Debug, thiserror::Error)]
pub enum OpaConfigError {
    /// `reqwest::Client::build` failed, from a TLS, DNS, or proxy misconfig.
    #[error("OPA HTTP client build failed: {0}")]
    Http(String),
    /// `max_cache_size = 0` would mean the LRU rejects every entry. That is
    /// an invariant violation, not a useful "disable cache" knob.
    #[error("OPA cache size must be > 0")]
    ZeroCache,
    /// `OpaAuthorizer::new` MUST run inside a tokio runtime, because it
    /// captures the current `Handle` for the sync→async bridge in
    /// `authorize`.
    #[error("OPA authorizer requires an active tokio runtime")]
    NoTokioRuntime,
}

/// Map [`AclOperation`] to its Strimzi-compatible OPA wire string. The
/// vocabulary mirrors Kafka's `AclOperation.name()` exactly so existing
/// Strimzi Rego policies port unchanged.
fn operation_str(op: AclOperation) -> &'static str {
    match op {
        AclOperation::All => "All",
        AclOperation::Read => "Read",
        AclOperation::Write => "Write",
        AclOperation::Create => "Create",
        AclOperation::Delete => "Delete",
        AclOperation::Alter => "Alter",
        AclOperation::Describe => "Describe",
        AclOperation::ClusterAction => "ClusterAction",
        AclOperation::DescribeConfigs => "DescribeConfigs",
        AclOperation::AlterConfigs => "AlterConfigs",
        AclOperation::IdempotentWrite => "IdempotentWrite",
        AclOperation::TwoPhaseCommit => "TwoPhaseCommit",
    }
}

/// Map [`ResourceType`] to its Strimzi-compatible OPA wire string.
fn resource_type_str(t: ResourceType) -> &'static str {
    match t {
        ResourceType::Topic => "Topic",
        ResourceType::Group => "Group",
        ResourceType::Cluster => "Cluster",
        ResourceType::TransactionalId => "TransactionalId",
        ResourceType::DelegationToken => "DelegationToken",
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use assert2::assert;
    use crabka_metadata::MetadataImage;
    use crabka_security::{AuthMethod, Principal};
    use crabka_units::{millis, minutes, secs};
    use uuid::Uuid;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::*;

    fn test_principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        }
    }

    fn img() -> MetadataImage {
        MetadataImage::new(Uuid::nil())
    }

    /// The OPA input's `operation` string must be the Strimzi-compatible name,
    /// including KIP-939's `TwoPhaseCommit`. This pins the mapping, so the
    /// test catches a regression or a blanket mutation of `operation_str`.
    #[test]
    fn operation_str_maps_kafka_names() {
        for (op, want) in [
            (AclOperation::Read, "Read"),
            (AclOperation::Write, "Write"),
            (AclOperation::TwoPhaseCommit, "TwoPhaseCommit"),
        ] {
            assert!(operation_str(op) == want, "{op:?}");
        }
    }

    fn host() -> SocketAddr {
        "1.2.3.4:9092".parse().unwrap()
    }

    fn req<'a>(p: &'a Principal, h: &'a SocketAddr, topic: &'a str) -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            principal: p,
            host: h,
            resource_type: ResourceType::Topic,
            resource_name: topic,
            operation: AclOperation::Read,
        }
    }

    fn opa_url(server: &MockServer) -> String {
        format!("{}/v1/data/kafka/authz/allow", server.uri())
    }

    fn supers(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn super_user_bypasses_opa_call() {
        let mock = MockServer::start().await;
        // expect(0) verifies on drop that no HTTP call landed.
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": false})),
            )
            .expect(0)
            .mount(&mock)
            .await;

        let auth = OpaAuthorizer::new(
            supers(&["admin"]),
            opa_url(&mock),
            false,
            100,
            minutes(1),
            secs(5),
        )
        .unwrap();
        let image = img();
        let p = test_principal("admin");
        let h = host();
        assert!(auth.authorize(&image, &req(&p, &h, "anything")) == AuthorizationResult::Allow);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_hit_returns_cached_decision_without_http_call() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})),
            )
            .expect(1) // exactly one call — second authorize() must hit cache.
            .mount(&mock)
            .await;

        let auth = OpaAuthorizer::new(
            HashSet::new(),
            opa_url(&mock),
            false,
            100,
            minutes(1),
            secs(5),
        )
        .unwrap();
        let image = img();
        let p = test_principal("alice");
        let h = host();
        assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
        assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_miss_calls_opa_and_caches_result() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})),
            )
            .expect(1)
            .mount(&mock)
            .await;

        let auth = OpaAuthorizer::new(
            HashSet::new(),
            opa_url(&mock),
            false,
            100,
            minutes(1),
            secs(5),
        )
        .unwrap();
        let image = img();
        let p = test_principal("alice");
        let h = host();
        assert!(auth.authorize(&image, &req(&p, &h, "fresh-topic")) == AuthorizationResult::Allow);
        // Cache populated; introspect by asserting a second call doesn't
        // bump the mock's request count when the assertion fires on drop.
        assert!(auth.authorize(&image, &req(&p, &h, "fresh-topic")) == AuthorizationResult::Allow);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cache_entry_expires_after_ttl() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})),
            )
            .expect(2) // first call + post-expiry call.
            .mount(&mock)
            .await;

        // 10ms decision-cache TTL, driven by an injected mock clock so the entry
        // expires on a controlled timeline — deterministic, no wall-clock sleep.
        let clock = Arc::new(qubit_clock::MockClock::new());
        let auth = OpaAuthorizer::with_clock(
            HashSet::new(),
            opa_url(&mock),
            false,
            100,
            millis(10),
            secs(5),
            clock.clone(),
        )
        .unwrap();
        let image = img();
        let p = test_principal("alice");
        let h = host();
        // Cache miss -> HTTP call #1; caches the decision with expires_at = now+10ms.
        assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
        // Advance the mock clock past the TTL so the cached entry is now stale.
        clock.advance(Duration::from_millis(50));
        // Cache entry expired -> HTTP call #2 (verified by the mock's expect(2)).
        assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_error_with_allow_on_error_true_returns_allow() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock)
            .await;

        // allow_on_error=true → 500 maps to Allow.
        let auth = OpaAuthorizer::new(
            HashSet::new(),
            opa_url(&mock),
            true,
            100,
            minutes(1),
            secs(5),
        )
        .unwrap();
        let image = img();
        let p = test_principal("alice");
        let h = host();
        assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_error_with_allow_on_error_false_returns_deny() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock)
            .await;

        let auth = OpaAuthorizer::new(
            HashSet::new(),
            opa_url(&mock),
            false,
            100,
            minutes(1),
            secs(5),
        )
        .unwrap();
        let image = img();
        let p = test_principal("alice");
        let h = host();
        assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn configured_http_timeout_fails_closed() {
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(250))
                    .set_body_json(serde_json::json!({"result": true})),
            )
            .mount(&mock)
            .await;

        let auth = OpaAuthorizer::new(
            HashSet::new(),
            opa_url(&mock),
            false,
            100,
            minutes(1),
            millis(25),
        )
        .unwrap();
        let image = img();
        let p = test_principal("alice");
        let h = host();

        assert!(auth.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn json_response_parse_error_returns_per_allow_on_error_config() {
        // 200 OK but body isn't valid OPA JSON. The shape parses as
        // serde-json but lacks the `result` field — should fall through
        // to error_decision().
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not-json-at-all"))
            .mount(&mock)
            .await;

        let p = test_principal("alice");
        let h = host();
        let image = img();

        let auth_open = OpaAuthorizer::new(
            HashSet::new(),
            opa_url(&mock),
            true,
            100,
            minutes(1),
            secs(5),
        )
        .unwrap();
        assert!(auth_open.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Allow);

        let auth_closed = OpaAuthorizer::new(
            HashSet::new(),
            opa_url(&mock),
            false,
            100,
            minutes(1),
            secs(5),
        )
        .unwrap();
        assert!(auth_closed.authorize(&image, &req(&p, &h, "t")) == AuthorizationResult::Deny);
    }
}
