# Crabka Operator Slice 36 — `KafkaUser` CRD: SCRAM-SHA-512 + ACLs (design)

**Date:** 2026-05-18
**Status:** Design ready for implementation
**Depends on:** slice 35 (KafkaTopic CRD + `crates/client-admin`), slice 13 (broker ACL handlers), slice 12 (SCRAM-SHA-512), slice 12b (operator-bootstrap super-user channel).

## Goal

Ship the third Phase-5 CRD: `KafkaUser`. The operator generates a random password,
publishes it via a Kubernetes `Secret`, provisions the user via Crabka's
`AlterUserScramCredentials` admin RPC (KIP-554), and keeps the user's ACL set in
sync with `spec.authorization.acls` via `CreateAcls` / `DeleteAcls` /
`DescribeAcls`.

Unidirectional reconciliation, matching slice 35's posture: the CRD is the
source of truth; out-of-band edits via `kafka-acls.sh` will be reverted.

## Scope

In:

- New `KafkaUser` CRD (`v1alpha1`) with two top-level concerns:
  - `authentication.type: scram-sha-512` (the only mechanism this slice ships)
  - `authorization.type: simple` + `acls: [...]` (Crabka's only authorizer today)
- New `crates/client-admin` module `users` adding 5 admin operations:
  `alter_user_scram_credentials` (upsert + delete), `create_acls`, `delete_acls`,
  `describe_acls`.
- New operator controller `controller/user.rs` with finalizer-driven cleanup.
- Helm `ClusterRole` grants `kafkausers`, `kafkausers/status`,
  `kafkausers/finalizers`.
- Generated `deploy/crds/crabka.io_kafkausers.yaml`.
- Unit tests for the CRD types, the SCRAM PBKDF2 round-trip, and the ACL diff.

Out (deferred):

- mTLS authentication (slice 37, depends on cluster CA management in slice 30).
- SCRAM-SHA-256 (slice 32 is shipped in core but slice 36 ships only SHA-512
  to bound the surface; `KafkaUser.spec.authentication.type` is an enum that
  will grow).
- Quotas (slice 38).
- End-to-end kind job (follow-up slice — slice 36 ships unit tests; the
  KafkaTopic e2e covers most of the same operator plumbing).

## CRD shape

```yaml
apiVersion: crabka.io/v1alpha1
kind: KafkaUser
metadata:
  name: my-app
  namespace: kafka
  labels:
    crabka.io/cluster: demo
spec:
  authentication:
    type: scram-sha-512
    # Optional override of generated password length (default 32 bytes,
    # base64-encoded to 44 chars).
    passwordLength: 32
    # Iterations count for PBKDF2 (minimum 4096, default 8192).
    iterations: 8192
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: orders
          patternType: literal
        operations: [Read, Describe]
        host: "*"        # optional, defaults to "*"
        type: allow      # optional, defaults to "allow"
      - resource:
          type: group
          name: my-app
          patternType: literal
        operations: [Read]
status:
  conditions:
    - type: Ready
      status: "True"
      reason: Ready
      message: user in sync
  observedGeneration: 3
  username: my-app                              # effective Kafka principal name
  secret: my-app                                # name of the credential Secret
  scramSha512: true                             # mechanism currently provisioned
```

### Field decisions

- **Effective username = `metadata.name`.** Matches Strimzi. No
  `spec.username` override surface in this slice.
- **Generated `Secret` name = `metadata.name`** in the same namespace.
  Holds keys `password` (raw UTF-8 password) and `sasl.jaas.config` (a
  ready-to-paste JAAS line for clients).
- **`authentication.type` is a discriminator (serde tag `type`)** so adding
  `tls` in slice 37 is a clean variant add. Mirrors how `listener.spec.type` and
  `metricsConfig.type` are shaped today.
- **`authorization.type: simple`** matches Strimzi's `KafkaUserAuthorizationSimple`
  shape. Anything else returns `Status[Ready]=False reason=UnsupportedAuthorization`.
- **`acls[].host` defaults to `"*"`.** All operations on Kafka's existing
  authorizer accept `"*"` (matches all source IPs).
- **`acls[].type` defaults to `allow`** — `deny` ACLs are supported but rare
  enough that defaulting to `allow` matches user intent.
- **`patternType` is `literal` or `prefixed`.** `match` / `any` are filter-only
  modes and would be ambiguous in a desired-state list; rejected at validation.
- **`operations` is a plural list** so a user with both Read and Describe on
  one resource expresses that as one entry. The reconciler expands the list
  into one `AclEntry` per (resource, operation, host, type) tuple before
  diffing against the cluster, matching Kafka's storage model.

## Reconcile pipeline

1. **Cluster label.** Same `crabka.io/cluster: <name>` label requirement as
   `KafkaTopic`. Missing → `Ready=False reason=MissingClusterLabel`.
2. **Cluster ready + bootstrap.** Same `Kafka.status.listeners[<inter_broker>]`
   lookup as `KafkaTopic`; not-ready → 30 s requeue with
   `Ready=False reason=ClusterNotReady`.
3. **Finalizer cleanup path** if `deletionTimestamp` is set:
   - Best-effort delete the user's SCRAM credential
     (`AlterUserScramCredentials.deletions`), then delete every ACL whose
     `principal` matches `User:<name>` (single `DeleteAcls` with a principal
     filter). Failures here are logged but don't block finalizer removal.
   - Remove finalizer + `Action::await_change`.
4. **Add finalizer if missing**, then re-enter.
5. **Validate `spec`**:
   - `authentication.type`: `scram-sha-512` required.
   - `authorization.type`: `simple` required.
   - `acls[].resource.patternType` is `literal` or `prefixed`.
   - `acls[].operations` non-empty.
6. **Ensure password Secret.** Server-side-apply a `Secret` named
   `metadata.name` in the same namespace; if it already has `password`, reuse
   it (passwords are stable across reconciles until the user deletes the
   Secret). Owner-referenced to the `KafkaUser` so cluster delete cascades.
7. **Upsert SCRAM credential** via `AlterUserScramCredentials.upsertions`,
   computing the salted-password client-side per KIP-554:
   - 16-byte random salt.
   - PBKDF2-HMAC-SHA-512 of the Secret's `password` over that salt for
     `spec.authentication.iterations` iterations.
   - `mechanism = 2` (SHA-512 wire constant).
   The broker stores `stored_key` + `server_key` derived from the wire
   `salted_password` — we never send the raw password.
8. **Reconcile ACLs.** `DescribeAcls(principal_filter = User:<name>)` to list
   the cluster's current ACLs for this user, expand `spec.authorization.acls`
   into `AclEntry` tuples, diff. Apply `CreateAcls` for additions; apply one
   `DeleteAcls` request whose `filters` enumerate the deletions (one filter
   per tuple to make the deletion exact).
9. **Status patch** with `Ready=True reason=Ready` + observed generation +
   `username`, `secret`, `scramSha512: true`.

### Error handling

- Transport failures evict the cached admin client (matches `KafkaTopic`).
- Broker-level errors on individual rows surface as
  `Ready=False reason=BrokerError` with the api / code / name.
- Validation errors surface as `Ready=False reason=InvalidSpec` with a
  message.

## `crates/client-admin` additions

```rust
// users.rs
pub struct ScramUpsertion { pub username: String, pub password: String, pub iterations: i32 }
pub struct ScramDeletion  { pub username: String }
pub struct ScramUserOutcome { pub username: String, pub error: Option<KafkaError> }

pub struct AclEntry {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub pattern_type: PatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: PermissionType,
}
pub struct AclEntryFilter { /* same shape as broker authorizer, all Optional */ }

pub struct CreateAclOutcome { pub error: Option<KafkaError> }
pub struct DeleteAclFilterOutcome { pub error: Option<KafkaError>, pub matched: Vec<AclEntry> }

impl AdminClient {
    pub async fn alter_user_scram_credentials_sha512(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError>;

    pub async fn describe_acls(
        &mut self,
        filter: &AclEntryFilter,
    ) -> Result<Vec<AclEntry>, AdminError>;

    pub async fn create_acls(
        &mut self,
        creations: &[AclEntry],
    ) -> Result<Vec<CreateAclOutcome>, AdminError>;

    pub async fn delete_acls(
        &mut self,
        filters: &[AclEntryFilter],
    ) -> Result<Vec<DeleteAclFilterOutcome>, AdminError>;
}
```

The wire `i8` discriminants are kept private to `users.rs` — callers use
the typed Rust enums. The slice intentionally does **not** depend on
`crabka-metadata` or `crabka-broker`: the operator-side admin client owns
its own enum copies so client-admin stays a leaf crate. Round-trip tests
guard the wire bytes.

### SCRAM client-side computation

`crabka-security` already exposes `derive_keys_from_salted` and
`scram_hash_len`. This slice adds `pbkdf2_salted_sha512` (plus a
mechanism-aware variant) that returns the salted-password bytes for the
KIP-554 wire field. No new crypto, just exposing the intermediate value.

## Testing

Per-PR (unit + integration, no kind):

- `client-admin/src/users.rs` round-trip tests for the wire-encoding helpers
  (`acl_to_creation`, `acl_to_filter`, `outcome_from_response`).
- `client-admin/tests/users_round_trip.rs` against a live `crabka-broker`
  spawned in-process (mirrors `tests/round_trip.rs`).
- `crd/user.rs` serde tests (omit-optional, parse-minimum, round-trip JSON).
- `controller/user.rs` pure-fn tests for `expand_spec_acls` and `diff_acls`.
- One reconcile test using `FakeAdminClient` (mirrors `reconcile_topic.rs`):
  first reconcile creates the Secret, upserts SCRAM, applies ACLs, transitions
  to `Ready=True`.

Deferred to a follow-up:

- kind operator-e2e job mirroring `kind-kafkatopic` (depends on a Helm-chart
  super-user/SCRAM bootstrap convention which is its own surface).

## Out of scope (and why)

- **mTLS user auth.** Requires cluster CA management (slice 30) which is not
  yet shipped.
- **Quotas.** Slice 38 owns this surface (`AlterClientQuotas`).
- **SCRAM-SHA-256.** Easy follow-up — the protocol type is already shared.
  Single-mechanism scope keeps the validation surface small for the initial
  slice.
- **Password rotation.** This slice writes the Secret once. Rotation API and
  the operator's "rotate now" annotation are a follow-up.
