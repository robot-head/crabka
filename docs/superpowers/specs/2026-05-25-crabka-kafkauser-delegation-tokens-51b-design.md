# Slice 51b: KafkaUser delegation-token authentication — Design

**Status:** Drafted 2026-05-25.

**Goal:** Close the loop on slice 51 by giving operator-managed
`KafkaUser`s first-class support for KIP-48 delegation tokens. Two halves
in one slice:

1. **Broker** — implement KIP-48 act-as on `CreateDelegationToken`: a
   super-user caller may specify `owner_principal_type` +
   `owner_principal_name` to mint a token *owned by another principal*.
   Required because slice 51 only honors "owner = caller", which means an
   operator-issued token would carry the operator's super-user authority.
2. **Operator** — new `KafkaUser.spec.authentication.type:
   delegation-token` auth variant. The operator authenticates as itself
   (super-user inter-broker credential), uses act-as to mint a token
   owned by `User:<KafkaUser.metadata.name>`, persists `(token-id, hmac,
   sasl.jaas.config)` into a Secret, periodically renews before expiry,
   and expires the token on KafkaUser deletion.

**Out of scope:**

- ACLs on the `TOKEN` resource type via `KafkaUser.spec.authorization` —
  defer; the existing `acls` shape already accepts arbitrary resource
  types and works fine for explicit grants.
- Per-KafkaUser delegation-token quotas.
- Token rotation. The operator renews the SAME token; it does not roll
  to a new token_id on each reconcile (rolling would invalidate every
  active session). Rotation is a follow-up.
- mTLS-derived principal as the owner (operator's principal is always
  `User:<inter-broker-username>`; specifying e.g. an mTLS DN as the
  owner is rejected by the broker since the act-as path can only assert
  `principal_type = "User"`).

---

## 1. Broker half: KIP-48 act-as on `CreateDelegationToken`

### 1.1 Wire surface (no change)

The `CreateDelegationToken` request wire struct already carries optional
`owner_principal_type: CompactString` + `owner_principal_name:
CompactString` fields (per KIP-373 / the generated codec). Slice 51's
handler ignored them. This slice consults them.

### 1.2 Semantics

In the handler (around `create_delegation_token.rs::handle`):

1. Auth gate (unchanged): SASL-authenticated, not `authenticated_via_token`.
2. **NEW — owner resolution:**
   - If both `owner_principal_type` and `owner_principal_name` are empty
     (the slice-51 case): owner = caller. No change in behavior.
   - If both are non-empty: caller must be a super-user (check
     `broker.config.super_users`). On non-super-user → return
     `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65). On super-user, the
     owner becomes `KafkaPrincipal { principal_type: <wire>, name:
     <wire> }`. Renewers default to the caller's principal in this case
     so the operator can renew/expire the token without needing
     bootstrap-as-the-owner flow.
   - If exactly one of the two is empty: `INVALID_REQUEST` (42). Both
     must be set or both unset.
3. Validate `principal_type == "User"`. Slice 51b doesn't support
   arbitrary principal types as owner (mTLS-DN owners deferred).
   Anything else → `INVALID_REQUEST`.
4. Rest of the body proceeds as slice 51 — UUID + HMAC + persist via
   `submit_change`.

### 1.3 Response

`CreateDelegationTokenResponse` already has both `principal_type` /
`principal_name` (the OWNER) AND `token_requester_principal_type` /
`token_requester_principal_name` (the caller). Slice 51 left the
requester fields default. Slice 51b populates them: requester = caller
when act-as fires, owner = act-as target. Matches the wire response that
the JVM admin tool prints.

### 1.4 Tests

- `act_as_super_user_sets_specified_owner` — caller is super-user `admin`;
  request carries `owner_principal_type=User, owner_principal_name=alice`;
  expect `error_code=0`, response `principal_name=alice`,
  `token_requester_principal_name=admin`. Verify the persisted record's
  owner field.
- `act_as_non_super_user_rejected_with_authorization_failed` —
  non-super-user with the same request → 65.
- `act_as_with_only_one_field_set_returns_invalid_request` — partial
  owner_principal_* → 42.
- `act_as_with_non_user_principal_type_returns_invalid_request` —
  `owner_principal_type=Group` → 42.

---

## 2. Operator half: `KafkaUser.spec.authentication.type: delegation-token`

### 2.1 CRD

Extend `crates/operator/src/crd/user.rs::Authentication`:

```rust
pub enum Authentication {
    ScramSha512(ScramSha512Auth),
    Tls(TlsAuth),
    TlsExternal,
    // NEW (slice 51b)
    DelegationToken(DelegationTokenAuth),
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DelegationTokenAuth {
    /// Optional list of additional principals (`User:<name>`) allowed
    /// to call RenewDelegationToken / ExpireDelegationToken on this
    /// user's token, beyond the owner. Default empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renewers: Vec<String>,

    /// Hard upper bound on the token's lifetime, in milliseconds.
    /// Passed as `max_lifetime_ms` to CreateDelegationToken. `None` =
    /// use broker config default (7 days). Capped at the broker's
    /// `delegation_token_max_lifetime_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_lifetime_ms: Option<i64>,

    /// Renew the token when `expiry_timestamp_ms - now <= this`.
    /// Default 24h (86_400_000 ms). Tuning lever: smaller = fewer raft
    /// appends, more risk of expiry on a controller stall; larger =
    /// more headroom.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 60_000))]
    pub renew_before_expiry_ms: Option<i64>,
}
```

Schema entry (`authentication_schema` JSON-schema):

```json
"type": { "enum": ["scram-sha-512", "tls", "tls-external", "delegation-token"] },
"renewers": { "type": "array", "items": { "type": "string", "pattern": "^User:.+$" } },
"maxLifetimeMs": { "type": "integer", "minimum": 1 },
"renewBeforeExpiryMs": { "type": "integer", "minimum": 60000 }
```

(The cross-variant-field-on-untagged-enum schemars limitation is the
same one slice 50 worked around — keep the manual schema.)

### 2.2 Reconciler

New module `crates/operator/src/controller/user_delegation_token.rs`,
called from the existing `user::reconcile` arm for the new variant.

**Inputs:**
- `KafkaUser` object (immutable spec; mutable status).
- Admin client connection to the cluster (existing pattern: operator's
  inter-broker SASL credential, which is a super-user).
- Token state: read from the cluster via `DescribeDelegationToken`
  filtered by `owners=[User:<KafkaUser.metadata.name>]`. Don't keep a
  separate cache in the operator — the broker is the source of truth.

**Reconcile flow:**

```
1. List tokens owned by `User:<KafkaUser.metadata.name>` via Describe.
2. Find tokens this operator manages (filter to one specific token_id
   stored in `KafkaUser.status.delegation_token_id` if present; else
   take the first match).
3. Case A — no matching token exists:
     a. Call CreateDelegationToken {
            owner_principal_type: "User",
            owner_principal_name: <KafkaUser name>,
            max_lifetime_ms: spec.maxLifetimeMs.unwrap_or(-1),
            renewers: [...spec.renewers...],
        }
     b. Capture (token_id, hmac, issue_ts, expiry_ts, max_ts).
     c. Write Secret with `token-id`, `hmac` (base64), `sasl.jaas.config`.
     d. Patch KafkaUser.status with `delegation_token_id`, `expiry_timestamp_ms`,
        `max_timestamp_ms`, `Ready=true`.
     e. Requeue at expiry_ts - renew_before_expiry_ms.
4. Case B — matching token exists, expiry_ts - now > renew_before_expiry_ms:
     No-op. Requeue at the same horizon as above.
5. Case C — matching token exists, expiry_ts - now <= renew_before_expiry_ms:
     a. Call RenewDelegationToken {
            hmac: <stored hmac bytes>,
            renew_period_ms: -1,  // broker uses delegation_token_default_renew_period_ms
        }
     b. Update Secret's metadata (HMAC unchanged; only the expiry shifts).
     c. Patch status with new expiry_ts.
     d. Requeue.
6. Case D — KafkaUser was renamed (spec.renewers diverged): if the
     stored renewers don't match spec.renewers, the operator does NOT
     in-place mutate (no such API in KIP-48 — renewers are fixed at
     create-time). Expire the old token and fall back to Case A. Log a
     warning.
```

**Finalizer:**

KafkaUser gets the standard operator finalizer
(`crabka.io/finalizer`, already used by the SCRAM + TLS arms). On
delete: ExpireDelegationToken with period -1 (immediate tombstone) for
every token owned by `User:<KafkaUser name>`. Drop the Secret. Then
remove the finalizer.

### 2.3 Secret format

Strimzi-style, key set:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: <kafkauser-name>
  ownerReferences: [<kafkauser>]
data:
  token-id: base64(<token uuid>)             # the SCRAM "username" for token auth
  hmac: base64(<32 raw hmac bytes>)          # the SCRAM "password equivalent" (raw, not base64-of-base64)
  password: base64(base64(<hmac bytes>))     # convenience: ready-to-paste base64 password
  sasl.jaas.config: |
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="<token-id>"
    password="<base64-of-hmac>"
    tokenauth="true";
```

`tokenauth="true"` is the Kafka client JAAS marker per KIP-48 §"Client
configuration"; it's purely a client-side hint that tells the SCRAM
mechanism to use SHA-256 + treat the password as a base64-encoded HMAC.
The broker ignores it (the broker's SCRAM-fallback logic in slice 51
auth.rs already does the right thing without it).

### 2.4 Status conditions

Two new conditions added to `KafkaUserStatus`:

- `TokenIssued` — last successful `CreateDelegationToken` outcome.
  Reason: `Issued` (success) / `IssueFailed` (with the broker's error
  code in the message).
- `TokenExpiring` — `expiry_ts - now < renew_before_expiry_ms * 2`.
  Pre-emptive warning surfaces in `kubectl get kafkausers`.

`Ready` aggregates both: True only when `TokenIssued.status=True` and
the Secret exists.

Status fields added:
- `delegation_token_id: Option<String>` (the UUID)
- `delegation_token_expiry_timestamp_ms: Option<i64>`
- `delegation_token_max_timestamp_ms: Option<i64>`

### 2.5 Failure handling

| Broker response | Operator action |
|----------------|-----------------|
| `error_code=0` | Normal flow, update status. |
| `61` (`AUTH_DISABLED`) | Operator never issued — patch `TokenIssued.status=False, reason=BrokerAuthDisabled`. Requeue with 5m backoff. Do not crash. |
| `65` (`AUTHORIZATION_FAILED`) | Operator's principal isn't a super-user. Patch `TokenIssued.status=False, reason=OperatorNotSuperUser`. Requeue with 5m backoff. |
| `42` (`INVALID_REQUEST`) | Spec is malformed; patch with reason `InvalidSpec`. No automatic recovery. |
| `64` (`REQUEST_NOT_ALLOWED`) | Operator's connection is somehow token-authed. Should be impossible (the operator uses inter-broker SASL/PLAIN or SCRAM). Patch + requeue. |
| Network / timeout | Bubble up to the reconciler's standard retry. |

---

## 3. e2e and integration

### 3.1 Broker integration

Extend `crates/broker/tests/delegation_tokens.rs`:

- `act_as_super_user_mints_token_owned_by_target`: admin caller specifies
  `owner_principal_name=alice`; second SCRAM-token connection authenticates
  as `User:alice`.
- `act_as_non_super_user_rejected_with_65`.

### 3.2 Operator integration

New `crates/operator/tests/reconcile_kafkauser_delegation_token.rs`:

- `delegation_token_user_reconcile_creates_secret_and_status` — create
  KafkaUser with `authentication.type: delegation-token`, run one
  reconcile, assert Secret exists with the expected keys and that
  KafkaUser.status.delegation_token_id is set.
- `delegation_token_user_reconcile_renews_when_within_threshold` — fake
  out the cluster clock or set `renewBeforeExpiryMs` very large so the
  reconciler always thinks renewal is due; assert expiry_ts moves
  forward on subsequent reconciles.
- `delegation_token_user_deletion_expires_token_and_removes_secret` —
  delete the KafkaUser; assert the cluster has no remaining tokens
  owned by that user AND the Secret is gone AND the finalizer is
  removed.

These will need an in-process Crabka broker per the slice 35 pattern
(the existing `reconcile_kafkauser_*.rs` integration tests already boot
a real broker).

### 3.3 Kind e2e

Add a `kind-kafkauser-delegation-token` job to
`.github/workflows/operator-e2e.yml`. Smallest viable scenario: deploy
a Kafka cluster + KafkaUser with `delegation-token` auth, wait for
Ready, exec a kafka client pod that mounts the Secret and produces +
consumes against the cluster using `sasl.jaas.config` from the Secret.

---

## 4. Decomposition (~10 tasks across 5 batches)

**Batch 1 — Broker act-as (parallel: B1, B2)**
- **B1**: act-as logic in `create_delegation_token.rs` + dispatch passing
  super_users to the handler + 4 unit tests.
- **B2**: extend `crates/broker/tests/delegation_tokens.rs` with 2 act-as
  end-to-end integration tests.

**Batch 2 — Operator CRD (sequential: O1)**
- **O1**: `Authentication::DelegationToken(DelegationTokenAuth)` variant
  + manual schema + status field additions + cascade sweep of fixture
  sites + CRD regen.

**Batch 3 — Operator reconciler (parallel: O2, O3)**
- **O2**: `user_delegation_token.rs` reconciler module — Describe →
  Create/Renew/No-op → Secret build/patch + status patch + requeue.
  Unit tests for Case A/B/C/D logic with mocked admin client.
- **O3**: `user::reconcile` arm + finalizer cleanup + admin-client
  helpers `create_delegation_token_as_owner` / `renew_delegation_token` /
  `expire_delegation_token` / `describe_delegation_tokens_owned_by` on
  `crates/client-admin`.

**Batch 4 — Operator integration tests (sequential: O4)**
- **O4**: 3 integration tests in `tests/reconcile_kafkauser_delegation_token.rs`.

**Batch 5 — e2e + STATUS (parallel: E1, S1)**
- **E1**: kind-kafkauser-delegation-token e2e workflow.
- **S1**: STATUS.md entry + final fmt/clippy/test gate.

Total: 8 substantive tasks. Smaller than slice 51's 13 because we reuse
all the slice 51 broker plumbing.

---

## 5. Tests (~18 total)

| Layer | Count |
|------:|-------|
| Broker unit (act-as in Create handler) | 4 |
| Broker integration (act-as end-to-end) | 2 |
| Operator CRD (round-trip + schema regression) | 2 |
| Operator reconciler unit (Case A/B/C/D + failure cases) | ~7 |
| Operator integration (Secret + lifecycle + finalizer) | 3 |

---

## 6. Known limitations

- Operator's principal must be a super-user for act-as to work. The
  inter-broker credential typically already is (slice 12 set
  `super_users` to include the inter-broker SASL username); if not, the
  reconciler surfaces `OperatorNotSuperUser` and does nothing useful.
  Document in STATUS.
- KafkaUser-managed delegation tokens use a single token at a time; no
  rolling. Renewal extends the same token. Compromise of the Secret
  means rotating the KafkaUser (delete + recreate) or implementing
  rotation as a follow-up.
- ACLs on the `TOKEN` resource type are NOT automatically generated for
  the KafkaUser owner. If you want explicit `Describe` grants to other
  principals, list them in `spec.authorization.acls` like any other
  resource.

---

## 7. Acceptance

- `cargo fmt --all --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo test -p crabka-broker --test delegation_tokens`
- `cargo test -p crabka-operator --test reconcile_kafkauser_delegation_token`
- New kind-kafkauser-delegation-token e2e job green on the slice branch.
- CRD drift check stays green (`deploy/crds/crabka.io_kafkausers.yaml` regenerated).
