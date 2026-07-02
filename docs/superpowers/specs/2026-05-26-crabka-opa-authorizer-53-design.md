# Slice 53: OPA authorizer bridge — Design

**Status:** Drafted 2026-05-26.

**Goal:** Add Open Policy Agent (OPA) as a first-class authorizer for Crabka.
A new `Kafka.spec.authorization.type: opa` activates a broker-side
`OpaAuthorizer` that delegates every ACL decision to an OPA HTTP REST
endpoint, with super-user bypass, configurable allow-on-error,
decision caching, and Strimzi-byte-compatible JSON request format.
Single bundled slice: broker plugin + operator CRD + reconciler wiring
+ kind e2e + delete the slice-51b-flagged "no super-users + no ACLs →
allow" compat shim.

**Out of scope:**

- OPA bundle/discovery management — operators wire the policy bundle
  into the OPA pod themselves; Crabka just calls OPA's REST API.
- Per-KafkaUser policy assignment — that's an OPA-side concern; the
  KafkaUser CRD still authors `spec.authorization.acls` (slice 36) but
  with `type: opa` those ACLs are NOT enforced by the broker (OPA owns
  every decision).
- OPA mTLS — `url` is plain HTTP/HTTPS via reqwest; mTLS-to-OPA is a
  follow-up.
- Decision-log shipping (OPA's audit feature) — operators wire that on
  the OPA side.

---

## 1. Broker half

### 1.1 `Authorizer` trait

Refactor `crates/broker/src/authorizer/mod.rs`. The current top-level
`authorize(image, super_users, &req)` function becomes a trait:

```rust
pub trait Authorizer: Send + Sync + std::fmt::Debug {
    fn authorize(&self, req: &AuthorizationRequest<'_>) -> AuthorizationResult;
}
```

Three impls:

1. **`AllowAllAuthorizer`** (new, in `authorizer/allow_all.rs`):
   Always returns `AuthorizationResult::Allow`. This is the default when
   `Kafka.spec.authorization` is unset — replaces the current
   "no super-users + no ACLs → allow" compat shim with an explicit type,
   and **deletes** the slice-51b workaround in
   `describe_delegation_token.rs::acl_authorization_is_active`.

2. **`SimpleAclAuthorizer`** (in `authorizer/simple_acl.rs`):
   Current behavior — looks up ACLs in `MetadataImage`, applies the
   slice-13 deny-then-allow precedence, super-user bypass. Carries
   `super_users: HashSet<String>` + a reference to the image (via the
   request — see §1.4 below).

3. **`OpaAuthorizer`** (new, in `authorizer/opa.rs`):
   HTTP-backed. See §1.2.

`BrokerConfig` carries `authorizer: Arc<dyn Authorizer>` instead of the
current `super_users: HashSet<String>` + image-driven implicit-authorizer
pattern. Super-users move INSIDE each authorizer impl (since both
SimpleAcl and Opa want the bypass).

### 1.2 `OpaAuthorizer`

```rust
pub struct OpaAuthorizer {
    super_users: HashSet<String>,
    http_client: reqwest::Client,
    /// Full URL of the OPA decision endpoint, e.g.
    /// `http://opa:8181/v1/data/kafka/authz/allow`. Operator-rendered.
    url: String,
    allow_on_error: bool,
    cache: Mutex<lru::LruCache<CacheKey, CachedDecision>>,
    expire_after_ms: i64,
    /// Tokio runtime handle for blocking-thread → async bridge. The
    /// `authorize()` method is called from sync handler code paths.
    runtime: tokio::runtime::Handle,
}

struct CacheKey {
    principal: String,        // "User:alice"
    operation: AclOperation,  // existing enum
    resource_type: ResourceType,
    resource_name: String,
    host: std::net::IpAddr,
}

struct CachedDecision {
    decision: AuthorizationResult,
    expires_at_ms: i64,
}
```

**Request flow:**

1. Super-user check: `if super_users.contains(&req.principal.name) → Allow`.
2. Cache lookup: hash `CacheKey`, check `expires_at_ms > now_ms()`. Hit
   → return cached decision.
3. Build JSON payload (see §1.3); POST to `url`; 5-second timeout.
4. On HTTP success + parse-success: cache + return decision.
5. On HTTP error / timeout / parse error: if `allow_on_error` → Allow;
   else → Deny. **NO** cache write on errors (fail-fast on next call).

**Sync-from-async bridge:** existing handler code paths call `authorize`
synchronously. `OpaAuthorizer::authorize` uses
`tokio::task::block_in_place(|| runtime.block_on(self.opa_call(req)))`
to call OPA's async HTTP API from sync code. Acceptable for a tail-call
authorization check (sub-millisecond on cache hit, low-double-digit-ms
on miss).

**Caching:** `lru::LruCache` with `maximum_cache_size` entries. Entries
expire on `expires_at_ms <= now`. No background eviction — entries are
purged lazily on lookup. This is good enough; pre-mature eviction would
need a periodic sweep we don't need yet.

### 1.3 OPA wire format

Strimzi-compatible JSON. Request body:

```json
{
  "input": {
    "request": {
      "principal": "User:alice",
      "operation": "Read",
      "resource": {
        "resourceType": "Topic",
        "name": "orders",
        "patternType": "Literal"
      },
      "host": "10.0.1.42"
    }
  }
}
```

Response body:

```json
{ "result": true }
```

(Boolean `result`; anything else is a parse error → fail-open/closed
per config.)

Operation values: `Read`, `Write`, `Create`, `Delete`, `Alter`,
`Describe`, `ClusterAction`, `DescribeConfigs`, `AlterConfigs`,
`IdempotentWrite`. ResourceType: `Cluster`, `Topic`, `Group`,
`DelegationToken`, `TransactionalId`.

### 1.4 `AuthorizationRequest` change

Today's request struct (`crates/broker/src/authorizer/mod.rs`) carries
references back to the image + super-users so the function can be free.
With trait-based authorizers, the authorizer OWNS those (super-users +
image-accessor for SimpleAcl). The request shrinks to:

```rust
pub struct AuthorizationRequest<'a> {
    pub principal: &'a Principal,
    pub host: &'a SocketAddr,
    pub resource_type: ResourceType,
    pub resource_name: &'a str,
    pub operation: AclOperation,
}
```

Removed fields: none — the old struct already had this shape; only the
top-level `authorize(image, super_users, req)` function signature
changes.

SimpleAclAuthorizer carries an `Arc<RwLock<MetadataImage>>` (or some
image-handle) so it can look up ACLs at call-time.

### 1.5 Broker config

`crates/broker/src/file_config.rs` — new TOML section:

```toml
[authorization]
type = "opa"           # or "simple" or "allow_all" (default if missing)
super_users = ["ANONYMOUS", "User:admin"]

[authorization.opa]    # only when type = "opa"
url = "http://opa.kafka.svc.cluster.local:8181/v1/data/kafka/authz/allow"
allow_on_error = false
initial_cache_capacity = 5000
maximum_cache_size = 50000
expire_after_ms = 3_600_000     # 1h
```

`apply_to` builds an `Arc<dyn Authorizer>` and assigns it to
`BrokerConfig.authorizer`.

---

## 2. Operator half

### 2.1 CRD

`crates/operator/src/crd/kafka.rs` — new `Authorization` enum at the
top-level `KafkaSpec`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Authorization {
    #[serde(rename = "simple")]
    Simple(SimpleAuthorization),
    #[serde(rename = "opa")]
    Opa(OpaAuthorization),
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SimpleAuthorization {
    /// Principal strings (e.g. `"User:admin"`, `"ANONYMOUS"`) that
    /// bypass ACL checks. Empty = no super-users.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OpaAuthorization {
    /// OPA decision endpoint URL — must include the data-API path, e.g.
    /// `http://opa:8181/v1/data/kafka/authz/allow`.
    pub url: String,
    /// Permit the operation on any OPA error (timeout, 5xx, parse).
    /// Default false (fail-closed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_on_error: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub initial_cache_capacity: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub maximum_cache_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1000))]
    pub expire_after_ms: Option<i64>,
    /// Principal strings that bypass OPA entirely. The broker's
    /// internal calls (replication etc.) use ANONYMOUS by default,
    /// which MUST be a super-user for inter-broker traffic to work.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}
```

`Kafka.spec.authorization: Option<Authorization>` — `None` means
"AllowAll" (today's default-on-no-config behavior).

### 2.2 Reconciler

Render the `[authorization]` block in `render_broker_toml` from the new
`spec.authorization`. When absent → omit (broker defaults to AllowAll).

The slice-51b super_users render (currently hardcoded to `["ANONYMOUS"]`
when `delegationToken` is set) folds into the new authorization render:
when `Kafka.spec.authorization.opa.super_users` (or `.simple.super_users`)
includes ANONYMOUS, the operator's inter-broker connection passes through.

For `delegationToken` to work without `Kafka.spec.authorization`, the
default `AllowAll` authorizer covers the act-as super-user check (since
it allows everything). With `authorization.type: simple` or `opa`, the
operator's principal MUST be in `super_users` — operators must
explicitly opt in to ANONYMOUS-as-super-user.

### 2.3 Status

No new status fields. Authorization is configuration, not state.

---

## 3. Delete the slice-51b compat-shim workaround

`crates/broker/src/handlers/describe_delegation_token.rs::acl_authorization_is_active`
exists ONLY because the old `authorize()` returned `Allow` unconditionally
when both super_users and ACLs were empty. With explicit
`AllowAllAuthorizer`, that's no longer "empty config = special-case allow"
— it's just the default authorizer. The Describe-via-ACL extension can
now call `authorize()` directly without the gate.

Delete the `acl_authorization_is_active` helper + its 1 call site.
Slice 51b's STATUS limitation note about this gets updated to "removed
by slice 53".

---

## 4. Tests (~25 total)

### 4.1 Broker unit (~12)

- `crates/broker/src/authorizer/allow_all.rs::tests` — 1 test:
  `allow_all_returns_allow_for_any_request`.
- `crates/broker/src/authorizer/simple_acl.rs::tests` — port the existing
  `authorize` tests (~5 already in the repo) into this impl unchanged.
- `crates/broker/src/authorizer/opa.rs::tests` — ~6 tests:
  - `super_user_bypasses_opa_call` (no HTTP, returns Allow).
  - `cache_hit_returns_cached_decision_without_http_call`.
  - `cache_miss_calls_opa_and_caches_result`.
  - `cache_entry_expires_after_ttl`.
  - `http_error_with_allow_on_error_true_returns_allow`.
  - `http_error_with_allow_on_error_false_returns_deny`.
  - `json_response_parse_error_falls_to_allow_or_deny_per_config`.

Use `wiremock` or `httpmock` (whichever the repo already uses) for OPA
mocking; if neither exists, use `tokio::net::TcpListener` + a hand-rolled
HTTP responder.

### 4.2 Broker integration (~2)

`crates/broker/tests/opa_authorizer.rs`:

- `produce_blocked_by_opa_returns_topic_authorization_failed`: boot
  broker with OPA authorizer + a `wiremock` server that returns
  `{"result": false}` for all calls; produce → TOPIC_AUTHORIZATION_FAILED.
- `produce_allowed_by_opa_succeeds`: same with `{"result": true}`.

### 4.3 Operator CRD (~3)

- `simple_authorization_round_trip`.
- `opa_authorization_round_trip_full_fields`.
- `opa_authorization_minimal_omits_optional_fields`.

### 4.4 Operator reconciler (~3)

- `render_broker_toml_emits_simple_authorization_section`.
- `render_broker_toml_emits_opa_authorization_section`.
- `render_broker_toml_omits_authorization_section_when_unset`.

### 4.5 Operator integration (~2)

- `kafka_with_opa_authorization_renders_correct_broker_toml`.
- `kafka_with_simple_authorization_super_users_round_trip`.

### 4.6 Kind e2e (1)

`kind-opa-authorization`:
- Apply the `mirror.gcr.io/openpolicyagent/opa:0.65.0` container as a Deployment with
  a hardcoded Rego policy allowing only `User:alice` to write to topic
  `permitted-topic`.
- Apply a Kafka cluster with
  `spec.authorization: { type: opa, url: http://opa:8181/v1/data/kafka/authz/allow }`.
- Wait for Ready.
- Produce as alice → succeed; produce as bob → fail.

---

## 5. Decomposition (~10 tasks, 6 batches)

**Batch 1 — broker refactor + AllowAll** (sequential: B1)
- **B1**: Extract `Authorizer` trait + `AllowAllAuthorizer` + port
  existing `authorize()` into `SimpleAclAuthorizer`. Drop the compat
  shim. Update `BrokerConfig` field (`authorizer: Arc<dyn Authorizer>`).
  Sweep all call-sites. Delete `describe_delegation_token.rs::acl_authorization_is_active`.

**Batch 2 — OpaAuthorizer** (parallel: B2, B3)
- **B2**: `crates/broker/src/authorizer/opa.rs` — OpaAuthorizer impl +
  6 unit tests (wiremock or equivalent).
- **B3**: `[authorization]` TOML section in `crates/broker/src/file_config.rs`
  → builds the right `Arc<dyn Authorizer>` + tests.

**Batch 3 — Broker integration tests** (sequential: B4)
- **B4**: `crates/broker/tests/opa_authorizer.rs` — 2 e2e tests with
  mock OPA.

**Batch 4 — Operator** (parallel: O1, O2)
- **O1**: CRD — `Kafka.spec.authorization` enum + `SimpleAuthorization` +
  `OpaAuthorization` + manual JSON schema. Cascade sweep.
- **O2**: Reconciler — render `[authorization]` block in
  `render_broker_toml`. Integrate with slice-51b's super_users render.

**Batch 5 — Operator integration** (sequential: O3)
- **O3**: 2 integration tests.

**Batch 6 — e2e + STATUS** (parallel: E1, S1)
- **E1**: `kind-opa-authorization` job with real OPA pod + Rego policy.
- **S1**: STATUS entry + final fmt/clippy/test gate.

---

## 6. Known limitations / honest follow-ups

- **No OPA mTLS** — `url` is plain HTTP/HTTPS. mTLS-to-OPA needs
  `tls_trusted_certificates` + client-cert config; follow-up.
- **No OPA-bundle awareness** — operator doesn't manage OPA's policy
  bundle; users wire that into the OPA pod externally (sidecar volume
  mount, OCI bundle, etc.).
- **Sync→async bridge** — `OpaAuthorizer::authorize` does
  `block_in_place(|| runtime.block_on(...))` on every cache miss.
  Acceptable for a tail authorization call but visible under heavy
  cache-miss load. Follow-up: refactor `authorize()` to be `async`
  end-to-end through the request handlers.
- **No decision-log integration** — Crabka doesn't send decision logs
  to OPA (or scrape OPA's audit log); operators wire OPA's own audit
  shipping.
- **Per-broker cache** — each broker has its own LRU; same decision
  re-fetched from OPA on cluster cold-start. Acceptable.

---

## 7. Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo test -p crabka-broker --test opa_authorizer`
- `cargo test -p crabka-operator --test reconcile_kafka_authorization` (new file)
- CRD drift gate (`tools/regen-crds.sh`)
- `kind-opa-authorization` e2e green on slice branch
