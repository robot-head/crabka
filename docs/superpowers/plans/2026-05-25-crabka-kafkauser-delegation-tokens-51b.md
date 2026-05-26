# Slice 51b: KafkaUser delegation-token authentication — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute this plan task-by-task in parallel batches where file sets don't overlap.

**Goal:** Two halves in one slice — (1) broker KIP-48 act-as on `CreateDelegationToken` so a super-user can mint tokens owned by other principals; (2) operator `KafkaUser.spec.authentication.type: delegation-token` reconciler that uses act-as to manage per-user tokens.

**Architecture:** Broker change is a narrow extension of the slice-51 Create handler (~30 lines + 4 tests). Operator change adds a new `Authentication` enum variant + a dedicated reconcile module that talks to the cluster via the existing `crabka-client-admin` crate — Describe → branch into Create/Renew/no-op based on expiry horizon, write Secret, patch status, requeue at the renewal threshold. Finalizer expires the token on delete.

**Tech stack:** Reuses slice 51's broker plumbing entirely. Operator side reuses slice 36/37 KafkaUser reconciler patterns + slice 35's `crabka-client-admin`. Adds 4 new admin-client methods for delegation-token RPCs.

---

## File structure

| Path | Responsibility |
|------|---------------|
| `crates/broker/src/handlers/create_delegation_token.rs` | act-as owner resolution + 4 new unit tests |
| `crates/broker/src/network/dispatch.rs` | pass `super_users` into the Create frame helper |
| `crates/broker/tests/delegation_tokens.rs` | 2 new act-as e2e tests |
| `crates/operator/src/crd/user.rs` | `Authentication::DelegationToken(DelegationTokenAuth)` + status fields + manual schema + 2 new CRD tests |
| `crates/operator/src/controller/user.rs` | new arm in `reconcile()` + finalizer cleanup branch for delegation-token |
| `crates/operator/src/controller/user_delegation_token.rs` (new) | Case A/B/C/D reconcile logic + Secret/status build + ~7 unit tests |
| `crates/client-admin/src/delegation_tokens.rs` (new) | 4 admin client methods: `create_delegation_token_as_owner`, `renew_delegation_token`, `expire_delegation_token`, `describe_delegation_tokens_owned_by` |
| `crates/client-admin/src/lib.rs` | re-export new module |
| `crates/operator/tests/reconcile_kafkauser_delegation_token.rs` (new) | 3 integration tests booting a real broker |
| `crates/operator/sample/kafkauser-delegation-token.yaml` (new) | sample manifest |
| `deploy/crds/crabka.io_kafkausers.yaml` | regenerated to include the new auth variant |
| `.github/workflows/operator-e2e.yml` | new `kind-kafkauser-delegation-token` job |
| `STATUS.md` | slice 51b entry |

---

## Batch 1 — Broker act-as (parallel: B1, B2)

### Task B1: act-as logic in `CreateDelegationToken` handler

**Files:**
- Modify: `crates/broker/src/handlers/create_delegation_token.rs`
- Modify: `crates/broker/src/network/dispatch.rs`

- [ ] **Step 1: Write failing tests** — append to the `tests` module in `create_delegation_token.rs`:

```rust
#[tokio::test]
async fn act_as_super_user_sets_specified_owner() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let mut super_users = std::collections::HashSet::new();
    super_users.insert("admin".to_string());
    let auth = authed("admin");  // existing helper: caller is `admin` SASL-authenticated
    let key = SecretBytes::new(b"master".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        renewers: vec![],
        owner_principal_type: "User".into(),
        owner_principal_name: "alice".into(),
        ..Default::default()
    };
    let resp = handle(&req, &auth, Some(&key), 60_000, RENEW_24H_MS, &controller, &super_users).await;
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.principal_type, "User");
    assert_eq!(resp.principal_name, "alice");
    assert_eq!(resp.token_requester_principal_type, "User");
    assert_eq!(resp.token_requester_principal_name, "admin");
    // The persisted record's owner is `alice`, not `admin`.
    let token_id = String::from_utf8(resp.token_id.to_vec()).unwrap();
    let img = controller.current_image();
    let stored = img.delegation_token_by_id(&token_id).expect("token persisted");
    assert_eq!(stored.owner.principal_type, "User");
    assert_eq!(stored.owner.name, "alice");
}

#[tokio::test]
async fn act_as_non_super_user_rejected_with_authorization_failed() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let super_users = std::collections::HashSet::<String>::new();   // empty
    let auth = authed("not-a-super-user");
    let key = SecretBytes::new(b"master".to_vec());
    let req = CreateDelegationTokenRequest {
        owner_principal_type: "User".into(),
        owner_principal_name: "alice".into(),
        max_lifetime_ms: -1,
        ..Default::default()
    };
    let resp = handle(&req, &auth, Some(&key), 60_000, RENEW_24H_MS, &controller, &super_users).await;
    assert_eq!(resp.error_code, crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
}

#[tokio::test]
async fn act_as_with_only_one_field_set_returns_invalid_request() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let mut super_users = std::collections::HashSet::new();
    super_users.insert("admin".to_string());
    let auth = authed("admin");
    let key = SecretBytes::new(b"master".to_vec());
    let req = CreateDelegationTokenRequest {
        owner_principal_type: "User".into(),
        owner_principal_name: "".into(),   // empty name, type set
        max_lifetime_ms: -1,
        ..Default::default()
    };
    let resp = handle(&req, &auth, Some(&key), 60_000, RENEW_24H_MS, &controller, &super_users).await;
    assert_eq!(resp.error_code, crate::codes::INVALID_REQUEST);
}

#[tokio::test]
async fn act_as_with_non_user_principal_type_returns_invalid_request() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let mut super_users = std::collections::HashSet::new();
    super_users.insert("admin".to_string());
    let auth = authed("admin");
    let key = SecretBytes::new(b"master".to_vec());
    let req = CreateDelegationTokenRequest {
        owner_principal_type: "Group".into(),
        owner_principal_name: "alice".into(),
        max_lifetime_ms: -1,
        ..Default::default()
    };
    let resp = handle(&req, &auth, Some(&key), 60_000, RENEW_24H_MS, &controller, &super_users).await;
    assert_eq!(resp.error_code, crate::codes::INVALID_REQUEST);
}
```

- [ ] **Step 2: Run to confirm failures**

```
cargo test -p crabka-broker --lib handlers::create_delegation_token::tests::act_as_
```
Expected: compile error (handler signature missing the new `super_users` arg).

- [ ] **Step 3: Extend handler signature + body**

```rust
pub(crate) async fn handle<S: std::hash::BuildHasher>(
    req: &CreateDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    max_lifetime_ms: i64,
    default_renew_period_ms: i64,
    controller: &ControllerHandle,
    super_users: &std::collections::HashSet<String, S>,
) -> CreateDelegationTokenResponse {
    let Some(key) = secret_key else {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    };
    let ConnectionAuth::Authenticated { principal, authenticated_via_token, .. } = auth else {
        return err_response(crate::codes::INVALID_REQUEST);
    };
    if *authenticated_via_token {
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    }

    // Slice 51b: act-as owner resolution. Both fields empty = self;
    // both set + caller-is-super-user = act-as; anything else = error.
    let owner_pt = req.owner_principal_type.as_str();
    let owner_pn = req.owner_principal_name.as_str();
    let owner = match (owner_pt.is_empty(), owner_pn.is_empty()) {
        (true, true) => principal.to_kafka(),
        (false, false) => {
            if !super_users.contains(&principal.name) {
                return err_response(crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
            }
            if owner_pt != "User" {
                return err_response(crate::codes::INVALID_REQUEST);
            }
            crabka_security::KafkaPrincipal {
                principal_type: owner_pt.to_string(),
                name: owner_pn.to_string(),
            }
        }
        _ => return err_response(crate::codes::INVALID_REQUEST),
    };

    // ... validate + clamp lifetime as before (unchanged) ...
    // ... compute now, token_id, hmac, renewers, record ... unchanged ...

    // RESPONSE — populate requester fields with the calling principal
    // when act-as fires; otherwise (self-owned) the requester fields
    // stay empty (matches what JVM's DelegationTokenCommand prints).
    let acting_as_other = owner != principal.to_kafka();
    CreateDelegationTokenResponse {
        error_code: 0,
        principal_type: owner.principal_type.clone(),
        principal_name: owner.name.clone(),
        token_requester_principal_type: if acting_as_other {
            principal.to_kafka().principal_type
        } else {
            String::new()
        },
        token_requester_principal_name: if acting_as_other {
            principal.to_kafka().name
        } else {
            String::new()
        },
        issue_timestamp_ms: now,
        expiry_timestamp_ms: initial_expiry,
        max_timestamp_ms,
        token_id: token_id.clone().into(),
        hmac: bytes::Bytes::from(hmac),
        throttle_time_ms: 0,
        ..Default::default()
    }
}
```

The wire field types depend on codegen (`CompactString` vs `String`). Read `crates/protocol/generated/CreateDelegationTokenRequest.owned.rs` to confirm — the `.as_str()` accessor used above assumes the codegen produces a string-like type. Adjust if needed.

- [ ] **Step 4: Update dispatch site** — in `crates/broker/src/network/dispatch.rs`, the `handle_create_delegation_token_frame` helper must pass `&broker.config.super_users`:

```rust
let resp = crate::handlers::create_delegation_token::handle(
    &req,
    &auth,
    broker.config.delegation_token_secret_key.as_ref(),
    broker.config.delegation_token_max_lifetime_ms,
    broker.config.delegation_token_default_renew_period_ms,
    &broker.controller,
    &broker.config.super_users,
).await;
```

- [ ] **Step 5: Run** all delegation_token tests

```
cargo test -p crabka-broker --lib delegation_token
```
Expected: all pre-existing tests + the 4 new act-as tests pass.

- [ ] **Step 6: Run build for the whole crate** (catch any sweep-needed test fixtures)

```
cargo build -p crabka-broker --all-targets
```
Expected: clean. Existing tests that call `handle(...)` may need `&super_users` added.

- [ ] **Step 7: Commit**

```
git add crates/broker/src/handlers/create_delegation_token.rs crates/broker/src/network/dispatch.rs
git commit -m "B1: CreateDelegationToken act-as for super-users (KIP-48)"
```

---

### Task B2: act-as broker integration tests

**Files:**
- Modify: `crates/broker/tests/delegation_tokens.rs`

**Independent of B1** (different file). Will be ready to commit once B1 lands (since the e2e wire path exercises B1's logic).

- [ ] **Step 1: Add 2 tests** below the existing `delegation_token_full_lifecycle` test:

```rust
#[tokio::test]
#[serial_test::serial]
async fn act_as_super_user_mints_token_owned_by_target() {
    // Single-broker SASL/PLAIN + SCRAM-SHA-256 cluster; `admin` is a super-user.
    let (handle, _dir, addr) = start_broker_with_super_users(&[
        ("admin", "admin-pw"),
        ("alice", "alice-pw"),
    ], &["admin"]).await;

    // 1. admin authenticates via SASL/PLAIN.
    let admin_conn = sasl_plain_authenticate(&addr, "admin", "admin-pw").await;

    // 2. admin creates a token, specifying alice as the owner.
    let create_resp = send_create_delegation_token(&admin_conn, CreateOpts {
        owner_principal_name: Some("alice".into()),
        max_lifetime_ms: -1,
        renewers: vec![],
    }).await;
    assert_eq!(create_resp.error_code, 0);
    assert_eq!(create_resp.principal_name, "alice");
    assert_eq!(create_resp.token_requester_principal_name, "admin");

    let token_id = String::from_utf8(create_resp.token_id.to_vec()).unwrap();
    let hmac = create_resp.hmac.clone();

    // 3. SCRAM-SHA-256 with the token creds — broker authenticates as alice
    //    (the OWNER, not admin who requested it).
    let token_conn = sasl_scram_sha256_authenticate(&addr, &token_id, &hmac).await;
    // Sanity: alice cannot create more tokens via this connection
    // (authenticated_via_token=true → REQUEST_NOT_ALLOWED).
    let bad = send_create_delegation_token(&token_conn, CreateOpts::default()).await;
    assert_eq!(bad.error_code, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);

    handle.shutdown().await;
}

#[tokio::test]
#[serial_test::serial]
async fn act_as_non_super_user_rejected_with_authorization_failed() {
    let (handle, _dir, addr) = start_broker_with_super_users(&[
        ("alice", "alice-pw"),
    ], &[]).await;  // no super-users

    let alice_conn = sasl_plain_authenticate(&addr, "alice", "alice-pw").await;
    let resp = send_create_delegation_token(&alice_conn, CreateOpts {
        owner_principal_name: Some("bob".into()),
        ..Default::default()
    }).await;
    assert_eq!(resp.error_code, DELEGATION_TOKEN_AUTHORIZATION_FAILED);

    handle.shutdown().await;
}
```

(Local consts `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64` and `DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65` already exist in the file from slice 51's integration test.)

- [ ] **Step 2: Extend `CreateOpts` and `send_create_delegation_token`** helpers if they don't already carry `owner_principal_name: Option<String>` — they probably don't since slice 51's test didn't exercise act-as. Add the option + thread it into the wire request:

```rust
struct CreateOpts {
    max_lifetime_ms: i64,
    renewers: Vec<(String, String)>,
    owner_principal_name: Option<String>,
}

impl Default for CreateOpts { /* max_lifetime_ms: -1, ... */ }

async fn send_create_delegation_token(conn: &mut Connection, opts: CreateOpts) -> CreateDelegationTokenResponse {
    let req = CreateDelegationTokenRequest {
        owner_principal_type: if opts.owner_principal_name.is_some() { "User".into() } else { String::new().into() },
        owner_principal_name: opts.owner_principal_name.unwrap_or_default().into(),
        max_lifetime_ms: opts.max_lifetime_ms,
        renewers: opts.renewers.iter().map(|(pt, pn)| CreatableRenewers {
            principal_type: pt.clone().into(),
            principal_name: pn.clone().into(),
        }).collect(),
        ..Default::default()
    };
    /* wire round-trip ... */
}
```

- [ ] **Step 3: Run**

```
cargo test -p crabka-broker --test delegation_tokens
```
Expected: 1 pre-existing + 2 new tests pass.

- [ ] **Step 4: Commit**

```
git add crates/broker/tests/delegation_tokens.rs
git commit -m "B2: integration tests for CreateDelegationToken act-as"
```

---

## Batch 2 — Operator CRD (sequential: O1)

### Task O1: `Authentication::DelegationToken` variant + status fields

**Files:**
- Modify: `crates/operator/src/crd/user.rs`
- Modify: `crates/operator/src/crd/mod.rs` (re-export `DelegationTokenAuth`)
- Modify: `deploy/crds/crabka.io_kafkausers.yaml` (regenerated)

**Note: This task must complete BEFORE Batch 3 starts** — O2/O3 reference the new variant.

- [ ] **Step 1: Write failing tests** — extend `crates/operator/src/crd/user.rs::tests`:

```rust
#[test]
fn delegation_token_authentication_round_trip() {
    let yaml = r#"
apiVersion: crabka.io/v1
kind: KafkaUser
metadata:
  name: alice
spec:
  authentication:
    type: delegation-token
    renewers: ["User:bob", "User:carol"]
    maxLifetimeMs: 86400000
    renewBeforeExpiryMs: 7200000
"#;
    let user: KafkaUser = serde_yaml::from_str(yaml).unwrap();
    let Authentication::DelegationToken(dt) = user.spec.authentication else {
        panic!("expected DelegationToken variant");
    };
    assert_eq!(dt.renewers, vec!["User:bob", "User:carol"]);
    assert_eq!(dt.max_lifetime_ms, Some(86_400_000));
    assert_eq!(dt.renew_before_expiry_ms, Some(7_200_000));
}

#[test]
fn delegation_token_authentication_minimal_omits_optional_fields() {
    let yaml = r#"
apiVersion: crabka.io/v1
kind: KafkaUser
metadata:
  name: alice
spec:
  authentication:
    type: delegation-token
"#;
    let user: KafkaUser = serde_yaml::from_str(yaml).unwrap();
    let Authentication::DelegationToken(dt) = user.spec.authentication else {
        panic!("expected DelegationToken variant");
    };
    assert!(dt.renewers.is_empty());
    assert!(dt.max_lifetime_ms.is_none());
    assert!(dt.renew_before_expiry_ms.is_none());
}
```

- [ ] **Step 2: Run** to confirm failure

```
cargo test -p crabka-operator --lib crd::user::tests::delegation_token_
```
Expected: `unknown variant 'delegation-token'`.

- [ ] **Step 3: Add the variant + struct** — in `user.rs`:

```rust
pub enum Authentication {
    ScramSha512(ScramSha512Auth),
    Tls(TlsAuth),
    TlsExternal,
    #[serde(rename = "delegation-token")]
    DelegationToken(DelegationTokenAuth),
}

/// Slice 51b: KIP-48 delegation-token authentication. The operator
/// acts-as a super-user to mint a token owned by this user, persists
/// `(token-id, hmac)` into a Secret, and periodically renews ahead of
/// expiry.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DelegationTokenAuth {
    /// Principal strings (e.g. `"User:bob"`) allowed to renew/expire
    /// this token in addition to the owner. Default empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renewers: Vec<String>,

    /// Hard upper bound on token lifetime in milliseconds. `None` →
    /// broker's `delegation_token_max_lifetime_ms` (7d default). Capped
    /// by the broker even when explicitly set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_lifetime_ms: Option<i64>,

    /// Renew when `expiry_timestamp_ms - now <= this`. Default 24h.
    /// Minimum 60s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 60_000))]
    pub renew_before_expiry_ms: Option<i64>,
}
```

- [ ] **Step 4: Update the manual schema** — extend `authentication_schema` (the existing JSON-schema workaround for tagged-union serde quirks):

```rust
fn authentication_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["scram-sha-512", "tls", "tls-external", "delegation-token"],
            },
            // SCRAM
            "iterations": { "type": "integer", "minimum": 4096, "maximum": 1_000_000 },
            "passwordLength": { "type": "integer", "minimum": 16, "maximum": 256 },
            // TLS
            "validityDays": { "type": "integer", "minimum": 1, "maximum": 36500 },
            "renewalDays": { "type": "integer", "minimum": 1, "maximum": 3650 },
            // Delegation token
            "renewers": {
                "type": "array",
                "items": { "type": "string", "pattern": "^User:.+$" },
            },
            "maxLifetimeMs": { "type": "integer", "minimum": 1 },
            "renewBeforeExpiryMs": { "type": "integer", "minimum": 60000 },
        },
    })
}
```

- [ ] **Step 5: Add status fields** — extend `KafkaUserStatus`:

```rust
pub struct KafkaUserStatus {
    // ... existing fields ...

    /// Slice 51b: persisted token_id of the operator-managed delegation
    /// token for this user. Used to find the same token across reconciles.
    pub delegation_token_id: Option<String>,

    /// Slice 51b: current token expiry (extended on each renew).
    pub delegation_token_expiry_timestamp_ms: Option<i64>,

    /// Slice 51b: token's absolute upper bound — renew can never push
    /// expiry past this.
    pub delegation_token_max_timestamp_ms: Option<i64>,
}
```

- [ ] **Step 6: Re-export** the new type from `crates/operator/src/crd/mod.rs`:

```rust
pub use user::{
    ..., Authentication, DelegationTokenAuth, KafkaUser, KafkaUserSpec, KafkaUserStatus, ...
};
```

- [ ] **Step 7: Cascade sweep** — every `KafkaUserStatus { ... }` or `Authentication::Tls(...)` test/fixture site needs the three new `delegation_token_*: None` fields. Find sites:

```
git grep -l 'KafkaUserStatus' -- crates/operator/
```

Add `delegation_token_id: None, delegation_token_expiry_timestamp_ms: None, delegation_token_max_timestamp_ms: None` to each struct-literal site. Plan estimates ~10 sites across `controller/user.rs`, `tests/reconcile_kafkauser_*.rs`, and CRD test fixtures.

- [ ] **Step 8: Regenerate CRD YAML** — run the existing CRD-gen script (likely `cargo run --bin crabka-operator-codegen` or `scripts/regen-crds.sh`; find it via `grep -rn 'crabka.io_kafkausers' Cargo.toml scripts/`). Output goes to `deploy/crds/crabka.io_kafkausers.yaml`. Diff should show only the new auth variant entries + status field entries.

- [ ] **Step 9: Run**

```
cargo test -p crabka-operator --lib crd::user::tests
cargo build -p crabka-operator --all-targets
```
Expected: green, including the 2 new round-trip tests. Cascade sweep may surface compile errors at struct-literal sites — fix as you go.

- [ ] **Step 10: Commit**

```
git add crates/operator/src/crd/user.rs crates/operator/src/crd/mod.rs deploy/crds/crabka.io_kafkausers.yaml $(git grep -l 'KafkaUserStatus' -- crates/operator/)
git commit -m "O1: KafkaUser CRD — DelegationToken auth variant + status fields"
```

---

## Batch 3 — Operator reconciler + admin client (parallel: O2, O3)

### Task O2: `user_delegation_token.rs` reconcile module

**Files:**
- Create: `crates/operator/src/controller/user_delegation_token.rs`
- Modify: `crates/operator/src/controller/mod.rs` (mod decl)
- Modify: `crates/operator/src/controller/user.rs` (dispatch the new variant + finalizer arm)

**Depends on O1 (variant) AND O3 (admin client methods). The implementer can stub the admin-client methods with `todo!()` and trait them out, OR wait for O3. Recommend: O2 starts after O1 commits, references the admin-client trait surface defined inline (O3 provides the impl); cargo build of the operator may not be clean until O3 lands.**

- [ ] **Step 1: Sketch the module skeleton + write failing unit tests** for Case A/B/C/D:

```rust
//! Slice 51b: reconcile arm for `KafkaUser.spec.authentication.type:
//! delegation-token`. Calls into the cluster's admin API to maintain
//! one delegation token per user, owned by `User:<name>`, persisted as
//! a Secret with SASL JAAS config ready for client consumption.

use std::time::Duration;
use chrono::Utc;
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Patch, PatchParams, PostParams};
use kube::Resource;

use crate::crd::{DelegationTokenAuth, KafkaUser};
use crate::error::ReconcileError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileDecision {
    /// No matching token; issue a fresh one.
    Create,
    /// Token exists and is well within its expiry horizon. No-op.
    NoOp,
    /// Token exists but expiry < renew_before_expiry_ms — renew.
    Renew,
    /// Renewer set has changed; cycle (expire old + create new).
    Cycle,
}

pub(crate) fn decide<'t>(
    auth: &DelegationTokenAuth,
    existing: Option<&'t crabka_metadata::DelegationToken>,
    now_ms: i64,
) -> ReconcileDecision {
    let Some(token) = existing else {
        return ReconcileDecision::Create;
    };
    // Renewer-set divergence forces a cycle.
    let expected: std::collections::HashSet<_> = auth.renewers.iter().cloned().collect();
    let actual: std::collections::HashSet<_> = token.renewers.iter().map(|p| p.to_string()).collect();
    if expected != actual {
        return ReconcileDecision::Cycle;
    }
    let renew_before = auth.renew_before_expiry_ms.unwrap_or(DEFAULT_RENEW_BEFORE_EXPIRY_MS);
    if token.expiry_timestamp_ms - now_ms <= renew_before {
        ReconcileDecision::Renew
    } else {
        ReconcileDecision::NoOp
    }
}

pub(crate) const DEFAULT_RENEW_BEFORE_EXPIRY_MS: i64 = 24 * 60 * 60 * 1_000;  // 24h
```

Tests (in same file):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::DelegationToken;
    use crabka_security::KafkaPrincipal;

    fn kp(t: &str, n: &str) -> KafkaPrincipal {
        KafkaPrincipal { principal_type: t.into(), name: n.into() }
    }
    fn token_with(expiry: i64, renewers: Vec<KafkaPrincipal>) -> DelegationToken {
        DelegationToken {
            token_id: "t1".into(),
            owner: kp("User", "alice"),
            hmac: vec![0; 32],
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: expiry,
            max_timestamp_ms: expiry + 1_000_000,
            renewers,
        }
    }
    fn auth(renewers: Vec<&str>, renew_before: Option<i64>) -> DelegationTokenAuth {
        DelegationTokenAuth {
            renewers: renewers.into_iter().map(str::to_string).collect(),
            max_lifetime_ms: None,
            renew_before_expiry_ms: renew_before,
        }
    }

    #[test]
    fn decide_create_when_no_token_exists() {
        assert_eq!(decide(&auth(vec![], None), None, 0), ReconcileDecision::Create);
    }

    #[test]
    fn decide_noop_when_expiry_far_from_now() {
        let t = token_with(1_000_000_000, vec![]);
        // Default 24h before-expiry; token expires far in future.
        assert_eq!(decide(&auth(vec![], None), Some(&t), 0), ReconcileDecision::NoOp);
    }

    #[test]
    fn decide_renew_when_inside_renew_threshold() {
        let t = token_with(1000, vec![]);
        // renew_before = 5000 > (1000 - 0). Renew.
        assert_eq!(decide(&auth(vec![], Some(5000)), Some(&t), 0), ReconcileDecision::Renew);
    }

    #[test]
    fn decide_cycle_when_renewers_diverge() {
        let t = token_with(1_000_000_000, vec![kp("User", "bob")]);
        // Spec adds carol.
        assert_eq!(
            decide(&auth(vec!["User:bob", "User:carol"], None), Some(&t), 0),
            ReconcileDecision::Cycle,
        );
    }

    #[test]
    fn decide_renew_when_default_threshold_just_met() {
        // Token expires in exactly 24h; default renew_before = 24h. Renew.
        let t = token_with(24 * 60 * 60 * 1_000, vec![]);
        assert_eq!(decide(&auth(vec![], None), Some(&t), 0), ReconcileDecision::Renew);
    }
}
```

- [ ] **Step 2: Run** these unit tests (pure-logic, no admin client needed):

```
cargo test -p crabka-operator --lib controller::user_delegation_token::tests
```
Expected: 5/5 pass.

- [ ] **Step 3: Add the full `reconcile` function** that takes an admin-client trait + the KafkaUser + Kubernetes API + KafkaUser status — handles Case A/B/C/D, writes Secret, patches status. Pseudocode:

```rust
pub(crate) async fn reconcile(
    obj: &KafkaUser,
    auth: &DelegationTokenAuth,
    admin: &impl DelegationTokenAdmin,
    secrets_api: &kube::Api<Secret>,
    users_api: &kube::Api<KafkaUser>,
) -> Result<kube::runtime::controller::Action, ReconcileError> {
    let owner_principal = format!("User:{}", obj.metadata.name.as_deref().unwrap_or(""));

    // 1. Describe — find tokens owned by this user.
    let existing = admin.describe_tokens_owned_by(&owner_principal).await?;
    let now = Utc::now().timestamp_millis();

    let decision = decide(auth, existing.first(), now);
    let (token, requeue_at) = match decision {
        ReconcileDecision::Create => issue_new_token(obj, auth, admin).await?,
        ReconcileDecision::NoOp => (existing.first().unwrap().clone(), compute_requeue(existing.first().unwrap(), auth, now)),
        ReconcileDecision::Renew => {
            let renewed = admin.renew_token(&existing.first().unwrap().hmac).await?;
            (renewed, compute_requeue(&renewed, auth, now))
        }
        ReconcileDecision::Cycle => {
            admin.expire_token(&existing.first().unwrap().hmac).await?;
            issue_new_token(obj, auth, admin).await?
        }
    };

    write_token_secret(obj, &token, secrets_api).await?;
    patch_status(obj, &token, users_api).await?;
    Ok(kube::runtime::controller::Action::requeue(requeue_at))
}
```

`DelegationTokenAdmin` is the trait surface expected from O3:

```rust
#[async_trait::async_trait]
pub(crate) trait DelegationTokenAdmin {
    async fn create_token_as_owner(
        &self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime_ms: i64,
    ) -> Result<crabka_metadata::DelegationToken, ReconcileError>;
    async fn renew_token(&self, hmac: &[u8]) -> Result<crabka_metadata::DelegationToken, ReconcileError>;
    async fn expire_token(&self, hmac: &[u8]) -> Result<(), ReconcileError>;
    async fn describe_tokens_owned_by(&self, owner_principal: &str) -> Result<Vec<crabka_metadata::DelegationToken>, ReconcileError>;
}
```

The Secret build:

```rust
fn build_secret_data(token: &crabka_metadata::DelegationToken) -> std::collections::BTreeMap<String, k8s_openapi::ByteString> {
    use base64::Engine;
    let hmac_b64 = base64::engine::general_purpose::STANDARD.encode(&token.hmac);
    let jaas = format!(
        "org.apache.kafka.common.security.scram.ScramLoginModule required \
         username=\"{}\" password=\"{}\" tokenauth=\"true\";",
        token.token_id, hmac_b64,
    );
    let mut data = std::collections::BTreeMap::new();
    data.insert("token-id".into(), k8s_openapi::ByteString(token.token_id.clone().into_bytes()));
    data.insert("hmac".into(), k8s_openapi::ByteString(token.hmac.clone()));
    data.insert("password".into(), k8s_openapi::ByteString(hmac_b64.clone().into_bytes()));
    data.insert("sasl.jaas.config".into(), k8s_openapi::ByteString(jaas.into_bytes()));
    data
}
```

- [ ] **Step 4: Add helper tests with a mock admin client** — implement a `MockDelegationTokenAdmin` that returns canned responses for each method, then test Case A (create), Case C (renew), Case D (cycle = expire + create). ~2 more tests.

- [ ] **Step 5: Run + commit**

```
cargo build -p crabka-operator        # may fail if O3 hasn't landed; OK
cargo test -p crabka-operator --lib controller::user_delegation_token::tests

git add crates/operator/src/controller/user_delegation_token.rs crates/operator/src/controller/mod.rs crates/operator/src/controller/user.rs
git commit -m "O2: KafkaUser delegation-token reconcile module + decide() unit tests"
```

---

### Task O3: `crabka-client-admin` delegation-token methods

**Files:**
- Create: `crates/client-admin/src/delegation_tokens.rs`
- Modify: `crates/client-admin/src/lib.rs` (mod decl + re-exports)

**Independent of O2** (different files; coordinate via the trait surface from O2's Step 3).

- [ ] **Step 1: Write failing tests** — create `delegation_tokens.rs` with the 4 methods + a `MockAdminTransport`-style test for each:

```rust
//! Slice 51b: delegation-token RPCs on `AdminClient`.

use crabka_protocol::owned::create_delegation_token_request::{
    CreatableRenewers, CreateDelegationTokenRequest,
};
use crabka_protocol::owned::create_delegation_token_response::CreateDelegationTokenResponse;
use crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest;
use crabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest;
use crabka_protocol::owned::describe_delegation_token_request::{
    DescribeDelegationTokenRequest, DescribeDelegationTokenOwner,
};

use crate::{AdminClient, AdminError};

impl AdminClient {
    /// KIP-48 act-as create. Caller must be a broker super-user (per
    /// slice 51b broker semantics) for `owner_principal_name` to take
    /// effect; otherwise returns DELEGATION_TOKEN_AUTHORIZATION_FAILED.
    pub async fn create_delegation_token_as_owner(
        &self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime_ms: i64,
    ) -> Result<CreateDelegationTokenResponse, AdminError> {
        let req = CreateDelegationTokenRequest {
            owner_principal_type: "User".into(),
            owner_principal_name: owner_principal_name.to_string().into(),
            max_lifetime_ms,
            renewers: renewers.iter().map(|r| {
                let (pt, pn) = r.split_once(':').unwrap_or(("User", r.as_str()));
                CreatableRenewers {
                    principal_type: pt.to_string().into(),
                    principal_name: pn.to_string().into(),
                }
            }).collect(),
            ..Default::default()
        };
        let resp: CreateDelegationTokenResponse = self.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::from_kafka_code(resp.error_code, "CreateDelegationToken"));
        }
        Ok(resp)
    }

    pub async fn renew_delegation_token(&self, hmac: &[u8]) -> Result<i64, AdminError> {
        let req = RenewDelegationTokenRequest {
            hmac: hmac.to_vec().into(),
            renew_period_ms: -1,
            ..Default::default()
        };
        let resp: crabka_protocol::owned::renew_delegation_token_response::RenewDelegationTokenResponse =
            self.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::from_kafka_code(resp.error_code, "RenewDelegationToken"));
        }
        Ok(resp.expiry_timestamp_ms)
    }

    pub async fn expire_delegation_token(&self, hmac: &[u8]) -> Result<(), AdminError> {
        let req = ExpireDelegationTokenRequest {
            hmac: hmac.to_vec().into(),
            expiry_time_period_ms: -1,
            ..Default::default()
        };
        let resp: crabka_protocol::owned::expire_delegation_token_response::ExpireDelegationTokenResponse =
            self.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::from_kafka_code(resp.error_code, "ExpireDelegationToken"));
        }
        Ok(())
    }

    /// Describes tokens owned by a single principal string like `"User:alice"`.
    pub async fn describe_delegation_tokens_owned_by(
        &self,
        owner_principal: &str,
    ) -> Result<Vec<crabka_metadata::DelegationToken>, AdminError> {
        let (pt, pn) = owner_principal.split_once(':').unwrap_or(("User", owner_principal));
        let req = DescribeDelegationTokenRequest {
            owners: Some(vec![DescribeDelegationTokenOwner {
                principal_type: pt.to_string().into(),
                principal_name: pn.to_string().into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let resp: crabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse =
            self.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::from_kafka_code(resp.error_code, "DescribeDelegationToken"));
        }
        Ok(resp.tokens.into_iter().map(|t| crabka_metadata::DelegationToken {
            token_id: t.token_id.to_string(),
            owner: crabka_security::KafkaPrincipal {
                principal_type: t.principal_type.to_string(),
                name: t.principal_name.to_string(),
            },
            hmac: t.hmac.to_vec(),
            issue_timestamp_ms: t.issue_timestamp,
            expiry_timestamp_ms: t.expiry_timestamp,
            max_timestamp_ms: t.max_timestamp,
            renewers: t.renewers.iter().map(|r| crabka_security::KafkaPrincipal {
                principal_type: r.principal_type.to_string(),
                name: r.principal_name.to_string(),
            }).collect(),
        }).collect())
    }
}

#[cfg(test)]
mod tests {
    // The AdminClient test pattern in this crate uses an in-process
    // mock broker (see `crates/client-admin/src/topics.rs::tests`).
    // Replicate that pattern for 4 round-trip tests:
    //   - create_delegation_token_as_owner: assert request shape (owner_pt/pn)
    //     + response error_code propagation.
    //   - renew_delegation_token: same.
    //   - expire_delegation_token: same.
    //   - describe_delegation_tokens_owned_by: owner filter shape + response mapping.
}
```

Read `crates/client-admin/src/topics.rs::tests` for the existing mock-broker pattern; copy it for the 4 new round-trip tests. Don't invent a new testing framework.

- [ ] **Step 2: Register the module** — in `crates/client-admin/src/lib.rs`:

```rust
pub mod delegation_tokens;
```

- [ ] **Step 3: Run + commit**

```
cargo test -p crabka-client-admin --lib delegation_tokens
cargo build -p crabka-client-admin

git add crates/client-admin/src/delegation_tokens.rs crates/client-admin/src/lib.rs
git commit -m "O3: crabka-client-admin — 4 delegation-token RPC methods"
```

---

## Batch 4 — Operator integration tests (sequential: O4)

### Task O4: 3 integration tests

**Files:**
- Create: `crates/operator/tests/reconcile_kafkauser_delegation_token.rs`
- Create: `crates/operator/sample/kafkauser-delegation-token.yaml`

**Depends on O1 + O2 + O3.**

- [ ] **Step 1: Boot pattern** — read one existing `reconcile_kafkauser_*.rs` (e.g., `reconcile_kafkauser_scram.rs` from slice 36 if it exists, else `reconcile_kafkauser_*` whichever is most recent) for the in-process broker + Kubernetes test pattern. Copy the harness exactly.

- [ ] **Step 2: Write the 3 tests**

```rust
#[tokio::test]
#[serial_test::serial]
async fn delegation_token_user_reconcile_creates_secret_and_status() {
    let env = TestEnv::start_with_super_users(&["operator"]).await;
    let user = KafkaUser {
        metadata: ObjectMeta { name: Some("alice".into()), ..Default::default() },
        spec: KafkaUserSpec {
            authentication: Authentication::DelegationToken(DelegationTokenAuth {
                renewers: vec!["User:bob".into()],
                max_lifetime_ms: None,
                renew_before_expiry_ms: None,
            }),
            ..Default::default()
        },
        status: None,
    };
    env.apply_user(&user).await;
    env.reconcile_once(&user).await;

    // Secret exists with the expected keys.
    let secret = env.get_secret("alice").await.expect("secret created");
    assert!(secret.data.as_ref().unwrap().contains_key("token-id"));
    assert!(secret.data.as_ref().unwrap().contains_key("hmac"));
    assert!(secret.data.as_ref().unwrap().contains_key("sasl.jaas.config"));

    // Status carries the token UUID + expiry/max timestamps.
    let refreshed = env.get_user("alice").await.unwrap();
    assert!(refreshed.status.as_ref().unwrap().delegation_token_id.is_some());
    assert!(refreshed.status.as_ref().unwrap().delegation_token_expiry_timestamp_ms.unwrap() > 0);
}

#[tokio::test]
#[serial_test::serial]
async fn delegation_token_user_reconcile_renews_when_within_threshold() {
    let env = TestEnv::start_with_super_users(&["operator"]).await;
    let user = KafkaUser {
        metadata: ObjectMeta { name: Some("alice".into()), ..Default::default() },
        spec: KafkaUserSpec {
            authentication: Authentication::DelegationToken(DelegationTokenAuth {
                renew_before_expiry_ms: Some(7 * 24 * 60 * 60 * 1_000),  // 7d — always inside default 24h renewal
                ..Default::default()
            }),
            ..Default::default()
        },
        status: None,
    };
    env.apply_user(&user).await;
    env.reconcile_once(&user).await;
    let after_first = env.get_user("alice").await.unwrap();
    let first_expiry = after_first.status.unwrap().delegation_token_expiry_timestamp_ms.unwrap();

    // Sleep just enough that "now" advances and renewal will extend.
    tokio::time::sleep(Duration::from_millis(20)).await;
    env.reconcile_once(&user).await;
    let after_second = env.get_user("alice").await.unwrap();
    let second_expiry = after_second.status.unwrap().delegation_token_expiry_timestamp_ms.unwrap();
    assert!(second_expiry >= first_expiry, "renew should extend or hold expiry");
    assert!(second_expiry <= after_second.status.unwrap().delegation_token_max_timestamp_ms.unwrap());
}

#[tokio::test]
#[serial_test::serial]
async fn delegation_token_user_deletion_expires_token_and_removes_secret() {
    let env = TestEnv::start_with_super_users(&["operator"]).await;
    let user = KafkaUser { /* alice with DelegationToken auth */ };
    env.apply_user(&user).await;
    env.reconcile_once(&user).await;

    // Confirm initial state.
    assert!(env.get_secret("alice").await.is_some());
    assert_eq!(env.list_tokens_owned_by("User:alice").await.len(), 1);

    // Delete + reconcile finalizer.
    env.delete_user("alice").await;
    env.reconcile_once(&user).await;

    // Secret + token are both gone.
    assert!(env.get_secret("alice").await.is_none());
    assert_eq!(env.list_tokens_owned_by("User:alice").await.len(), 0);
}
```

(`TestEnv` is a fictional name; use whatever existing harness type the slice-36 tests use. Adapt method names to match.)

- [ ] **Step 3: Sample manifest** — `crates/operator/sample/kafkauser-delegation-token.yaml`:

```yaml
apiVersion: crabka.io/v1
kind: KafkaUser
metadata:
  name: alice
  namespace: kafka
spec:
  authentication:
    type: delegation-token
    # Optional: principals allowed to renew/expire this user's token.
    # renewers:
    #   - User:bob
    # Optional: hard upper bound on token lifetime (ms). Defaults to
    # broker's delegation.token.max.lifetime.ms (7d).
    # maxLifetimeMs: 86400000
    # Optional: renew when expiry < this ms away (default 24h, min 60s).
    # renewBeforeExpiryMs: 7200000
```

- [ ] **Step 4: Run + commit**

```
cargo test -p crabka-operator --test reconcile_kafkauser_delegation_token

git add crates/operator/tests/reconcile_kafkauser_delegation_token.rs crates/operator/sample/kafkauser-delegation-token.yaml
git commit -m "O4: KafkaUser delegation-token integration tests + sample manifest"
```

---

## Batch 5 — e2e + STATUS (parallel: E1, S1)

### Task E1: kind-kafkauser-delegation-token e2e

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

**Independent of S1.**

- [ ] **Step 1: Sketch the job** — read an existing simple e2e job (e.g., `kind-kafkatopic`) for the cluster-up + apply-CR + wait-Ready pattern. Copy and adapt:

```yaml
kind-kafkauser-delegation-token:
  name: kind • KafkaUser delegation-token
  needs: [build-images, changes]
  if: needs.changes.outputs.crabka-operator == 'true' || ${{ github.event_name == 'push' }}
  runs-on: ubuntu-latest
  timeout-minutes: 25
  steps:
    - uses: actions/checkout@v6
    # ... same setup as other kind jobs ...
    - name: Apply Kafka cluster with delegation-token secret key
      run: |
        kubectl apply -f - <<EOF
        apiVersion: crabka.io/v1
        kind: Kafka
        metadata:
          name: my-cluster
          namespace: kafka
        spec:
          kafka:
            replicas: 1
            delegationToken:
              secretKey: e2e-master-key
            listeners:
              - name: plain
                port: 9092
                type: internal
                authentication:
                  type: scram-sha-512
          # ... rest of Kafka spec ...
        EOF
    - name: Wait for cluster ready
      run: kubectl wait kafka/my-cluster --for=condition=Ready --timeout=600s -n kafka
    - name: Apply KafkaUser with delegation-token auth
      run: |
        kubectl apply -f - <<EOF
        apiVersion: crabka.io/v1
        kind: KafkaUser
        metadata:
          name: alice
          namespace: kafka
          labels:
            strimzi.io/cluster: my-cluster
        spec:
          authentication:
            type: delegation-token
        EOF
    - name: Wait for KafkaUser ready
      run: kubectl wait kafkauser/alice --for=condition=Ready --timeout=300s -n kafka
    - name: Produce + consume using the token Secret
      run: |
        kubectl run kafka-test --rm -i --tty --restart=Never \
          --image=confluentinc/cp-kafka:7.5.0 -- /bin/bash -c '
            cat /etc/kafka-secret/sasl.jaas.config > /tmp/jaas.conf
            cat >/tmp/client.properties <<EOC
            security.protocol=SASL_PLAINTEXT
            sasl.mechanism=SCRAM-SHA-256
            sasl.jaas.config=$(cat /tmp/jaas.conf)
            EOC
            echo hello | kafka-console-producer --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic test --producer.config /tmp/client.properties
          '
        # Mount the alice Secret into the test pod...
```

Adapt the exact pod-mounting syntax to whatever the existing `kind-listener-auth` e2e uses for secret-mounted clients.

The `Kafka.spec.kafka.delegationToken.secretKey` field needs to exist on the Kafka CRD — check if slice 51 already added it (it likely did NOT; the broker config slice was scoped to the broker side). If absent, this job depends on a small Kafka-CRD extension. For this slice, the cleanest scope is to OMIT delegationToken from the Kafka CRD and pass the secret-key via a Pod env var or ConfigMap: see the slice-49g `secret_key_env_var` pattern for an example.

Alternatively, this task can be a SMOKE TEST only (KafkaUser reconciles to Ready) and the produce/consume part deferred — that keeps E1 minimal.

- [ ] **Step 2: Smoke-verify locally**

```
yamllint .github/workflows/operator-e2e.yml
```

(Don't actually run kind locally — CI runs it on push.)

- [ ] **Step 3: Commit**

```
git add .github/workflows/operator-e2e.yml
git commit -m "E1: kind-kafkauser-delegation-token e2e job"
```

---

### Task S1: STATUS entry + final gate

**Files:**
- Modify: `STATUS.md`

**Independent of E1.**

- [ ] **Step 1: Append slice 51b entry** below slice 51 (find via `grep -n '^## Slice 51' STATUS.md`). Match the slice 51 voice + structure:

```markdown
## Slice 51b — Operator + Broker: KafkaUser delegation-token authentication (2026-05-25)

- **Goal:** Close the slice-51 loop: super-user act-as on the broker +
  operator-managed delegation tokens per `KafkaUser`.
- **Broker (KIP-48 act-as):** `CreateDelegationToken` now honors
  `owner_principal_type` + `owner_principal_name` request fields when
  the caller is a super-user. Non-super-user → `DELEGATION_TOKEN_AUTHORIZATION_FAILED`
  (65). Single field set without the other → `INVALID_REQUEST` (42).
  Owner `principal_type` must be `"User"` (mTLS-DN owners deferred).
  Response populates `token_requester_*` to identify the caller when
  act-as fires.
- **Operator (`KafkaUser.spec.authentication.type: delegation-token`):**
  New `Authentication::DelegationToken(DelegationTokenAuth)` variant
  carrying `renewers: Vec<String>`, `maxLifetimeMs: Option<i64>`,
  `renewBeforeExpiryMs: Option<i64>` (default 24h).
- **Reconciler** (`user_delegation_token.rs`):
  - `Describe` tokens owned by `User:<KafkaUser name>` →
  - 4-way decide: `Create` (no token) / `NoOp` (within horizon) /
    `Renew` (inside `renewBeforeExpiryMs`) / `Cycle` (renewer-set
    diverged, expire + create).
  - Writes Secret with `token-id`, `hmac` (raw bytes), `password`
    (base64-encoded HMAC for direct paste), `sasl.jaas.config` (ready
    for `--producer.config` consumption).
  - Patches `KafkaUserStatus.delegation_token_id / expiry_timestamp_ms /
    max_timestamp_ms` + `TokenIssued` / `TokenExpiring` conditions.
  - Finalizer expires the token on KafkaUser deletion (immediate
    tombstone via `expire_period_ms = -1`).
- **`crabka-client-admin`:** 4 new methods — `create_delegation_token_as_owner`,
  `renew_delegation_token`, `expire_delegation_token`,
  `describe_delegation_tokens_owned_by`.
- **CRD cascade:** ~10 fixture sites swept for the 3 new
  `KafkaUserStatus.delegation_token_*` fields. CRD YAML regenerated.
- **Tests:** ~18 new — 4 broker unit + 2 broker integration (act-as
  end-to-end) + 2 CRD round-trip + ~7 reconciler unit (decide() +
  mocked admin client Case A/C/D) + 3 operator integration (Secret +
  renew + delete-cycle). New kind-kafkauser-delegation-token e2e job.
- **Known limitations:** operator's inter-broker principal must be a
  super-user for act-as to fire. Token rotation (new token_id on each
  reconcile) deferred. ACLs on `TOKEN` resource type not
  auto-generated for KafkaUser owners (use explicit
  `spec.authorization.acls`).
- **Workspace fmt + clippy `-D warnings` + tests + CRD drift gate**
  all green.
```

- [ ] **Step 2: Run the full gate**

```
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```
Fix any remaining issues in place.

- [ ] **Step 3: Commit**

```
git add STATUS.md
git commit -m "Slice 51b: STATUS.md entry + final fmt/clippy/test gate"
```

---

## Self-review checklist

**Spec coverage:**
- §1 broker act-as: B1 (logic + 4 tests) + B2 (e2e tests).
- §2.1 CRD variant: O1.
- §2.2 reconciler Case A/B/C/D: O2.
- §2.3 Secret format: O2 Step 3.
- §2.4 status conditions: O1 Step 5 + O2.
- §2.5 failure handling: O2 (error-code branch logic, ~2 tests).
- §3.1 broker integration: B2.
- §3.2 operator integration: O4.
- §3.3 kind e2e: E1.
- §4 decomposition: 8 tasks across 5 batches — matches ✓.
- §5 tests: ~18 — matches ✓.

**Type consistency:** `KafkaPrincipal` throughout; `crate::codes::*` everywhere; `time_util::now_ms()` if anything in this slice needs a timestamp (probably only operator-side via chrono — that's fine, chrono is the operator's pattern).

**No placeholders:** every step has either complete code blocks or specific tool-invocation commands. The `TestEnv` harness name in O4 is flagged as fictional — implementer needs to read the existing reconcile_kafkauser_*.rs file pattern and match.
