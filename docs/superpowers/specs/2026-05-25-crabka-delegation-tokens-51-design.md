# Slice 51: Delegation tokens (KIP-48) — Design

**Status:** Drafted 2026-05-25.

**Goal:** Implement KIP-48 delegation tokens end-to-end in the broker:
the four wire handlers (`CreateDelegationToken`, `RenewDelegationToken`,
`ExpireDelegationToken`, `DescribeDelegationToken`), raft-persisted
`DelegationTokenRecord` metadata, the broker-wide master HMAC key, the
SCRAM-SHA-256/512 lookup extension so clients can authenticate **as the
token's owner** by presenting `(tokenId, HMAC)`, the `TOKEN` ACL
resource type (currently rejected outright at `acl_wire.rs:24`), and a
background expiry sweep that prunes stale tokens.

**Out of scope:**

- Operator-side `KafkaUser` surface for delegation tokens (roadmap calls
  it out as a follow-up sub-slice; not bundled here).
- Master-key hot-swap. The key is loaded from broker config /
  environment at startup; changing it invalidates all in-flight tokens.
  Documented as a known limitation; out of scope for this slice.
- Cross-broker rate-limiting of `CreateDelegationToken` calls.
  Hand-wavy "abuse vector" mentioned in KIP-48 commentary — Kafka's own
  implementation has none either. Defer.
- Per-token request-quota carrying. KIP-48 ties token quotas to the
  owner principal, which already flows through the existing
  `KafkaPrincipal` plumbing — nothing new required.

---

## 1. Wire surface

### 1.1 Request handlers (4 new, all flexible-v2 framing)

All four handlers live under `crates/broker/src/handlers/`:

| api_key | request                       | min ver | max ver | handler file                          |
|--------:|-------------------------------|--------:|--------:|---------------------------------------|
| 38      | `CreateDelegationToken`       |       0 |       3 | `create_delegation_token.rs`          |
| 39      | `RenewDelegationToken`        |       0 |       2 | `renew_delegation_token.rs`           |
| 40      | `ExpireDelegationToken`       |       0 |       2 | `expire_delegation_token.rs`          |
| 41      | `DescribeDelegationToken`     |       0 |       3 | `describe_delegation_token.rs`        |

Wire-struct generation is already in `crates/protocol/generated/` (the
owned + borrowed forms are present per the survey I did) — no new
codegen required.

### 1.2 `CreateDelegationToken` semantics

1. **Auth gate.** Caller must be SASL-authenticated AND not itself
   authenticated via a delegation token (KIP-48 disallows token-to-token
   delegation chains; `INVALID_REQUEST` if violated).
2. **Compute tokenId.** UUIDv4 (use `uuid::Uuid::new_v4()`).
3. **Compute HMAC.** `hmac_sha512(master_key, tokenId.to_string())`.
   Returned to caller as the SCRAM "password equivalent". Persisted as
   the lookup key for token-SCRAM auth.
4. **Compute expiry.** Caller may pass `max_lifetime_ms`:
   - `-1` or omitted → use broker config `delegation_token_max_lifetime_ms` (default 7 days).
   - `>0` → clamp to `min(requested, delegation_token_max_lifetime_ms)`.
   - `0` or negative-non-`-1` → `INVALID_REQUEST`.
5. **Issue + expire timestamps.** `issue_ts = now`, `expire_ts = issue_ts + chosen_lifetime`.
6. **Renewers list.** Whatever caller passed (zero-or-more SASL
   principal strings, e.g. `"User:alice"`). Renewers in addition to the
   owner can call `RenewDelegationToken`.
7. **Persist via raft.** Append a `DelegationTokenRecord` (see §3).
   Wait for commit; respond with token bytes + metadata after the
   record applies to local image.
8. **Response.** Standard KIP-48 shape — `owner`, `principal_type`,
   `issue_timestamp_ms`, `expiry_timestamp_ms`, `max_timestamp_ms`,
   `token_id`, `hmac`. Error code `0` on success; `61
   DELEGATION_TOKEN_AUTH_DISABLED` if no master key configured; `62
   DELEGATION_TOKEN_NOT_FOUND` is a Describe/Renew/Expire-only
   condition; `81 DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` for the
   token-creating-token case.

### 1.3 `RenewDelegationToken` semantics

1. **Auth gate.** Caller must be SASL-authenticated. Not necessarily
   via SCRAM — any working SASL/mTLS principal qualifies.
2. **Look up token by HMAC.** Request body is `hmac: Vec<u8>` and
   `renew_period_ms: i64`. Find the token where `stored_hmac == hmac`;
   `DELEGATION_TOKEN_NOT_FOUND` if no match.
3. **Authorization.** The calling principal must be the token's owner
   OR appear in `renewers`. `DELEGATION_TOKEN_OWNER_MISMATCH` (err 64)
   otherwise.
4. **Compute new expiry.** `new_expire = min(now + renew_period_ms,
   issue_ts + max_lifetime_ms)`. If `renew_period_ms == -1`, fall back
   to broker config `delegation_token_expiry_check_interval_ms` (Kafka
   default 24h).
5. **Persist via raft.** Append a `DelegationTokenRecord` with the
   updated expiry (records replace; matched by token_id).
6. **Response.** `expiry_timestamp_ms` = new expiry.

### 1.4 `ExpireDelegationToken` semantics

1. **Auth gate.** Same as Renew.
2. **Look up token by HMAC.** Same as Renew.
3. **Authorization.** Owner or renewer can call. Per KIP-48,
   `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (err 65) if not authorized.
4. **Compute decision:**
   - `expire_period_ms < 0` → delete the token immediately (append a
     tombstone-like record; see §3).
   - `expire_period_ms == 0` → expire at `now`.
   - `expire_period_ms > 0` → expire at `now + expire_period_ms`
     (clamped to `issue_ts + max_lifetime_ms`).
5. **Persist via raft.**
6. **Response.** `expiry_timestamp_ms` = new expiry (or the deletion
   sentinel; KIP-48 returns the past timestamp for deletes).

### 1.5 `DescribeDelegationToken` semantics

1. **Auth gate.** SASL-authenticated; token-authenticated callers can
   only describe their own tokens (filter implicitly).
2. **Owners filter.** Request carries optional `owners: Vec<RequestedOwner>`.
   - Empty → describe all tokens visible to the caller.
   - Non-empty → only tokens whose `owner` matches one of the entries.
3. **Authorization:** For each token in the result set, the caller must
   either be the token's owner, a renewer, OR have `Describe` ACL on
   resource `TOKEN:owner_name`. Token-authenticated callers see only
   their own. Standard SuperUser bypass applies.
4. **Response.** List of `DescribedDelegationToken` (owner, principal
   type, issue/expiry/max timestamps, token_id, hmac, renewers).

---

## 2. Token-SCRAM authentication

### 2.1 Mechanism

SCRAM-SHA-256 (mandatory for KIP-48; tokens are scoped to this single
mechanism). Crabka already supports SCRAM-SHA-256 (slice 32) and
SCRAM-SHA-512 (slice 12). Token auth uses the SHA-256 path.

### 2.2 Credential synthesis

The token's `hmac` (raw bytes, base64-encoded for transport) acts as
the SCRAM-SHA-256 "password equivalent". For each token we synthesize a
`StoredScramCredential` on-the-fly at lookup time, NOT at storage time
— the per-token SCRAM salt + iteration count is derived deterministically:

```rust
// In `crates/broker/src/network/auth.rs::handle_authenticate_scram`,
// at the round-1 lookup site:
let cred = controller
    .current_image()
    .scram_credential(&username, mech)
    .cloned()
    .or_else(|| {
        // Slice 51: fall back to delegation-token lookup. Token-id is
        // the SCRAM username; the token's stored HMAC bytes become the
        // SCRAM password equivalent. Salt = token_id bytes (UUID is
        // already random); iterations = 4096 (KIP-48 fixed).
        if mech != ScramMechanism::Sha256 { return None; }
        let img = controller.current_image();
        let token = img.delegation_token_by_id(&username)?;
        Some(token.to_scram_credential())
    });
```

`DelegationToken::to_scram_credential(&self) -> StoredScramCredential`
runs the SCRAM key-derivation on `(self.hmac.as_bytes(), salt=token_id,
iters=4096)` — same as if a regular SCRAM user existed with that
password.

### 2.3 Principal override

The `ScramServerExchange::step` `Done` arm normally returns the SCRAM
username as the authenticated `KafkaPrincipal`. For token auth, the
principal must be the **token's owner**. We pass an optional
`principal_override: Option<KafkaPrincipal>` into the exchange's
constructor; the `Done` arm prefers the override when set:

```rust
// In crates/security/src/scram.rs (or wherever ScramServerExchange lives):
pub struct ScramServerExchange {
    // ... existing fields ...
    principal_override: Option<KafkaPrincipal>,
}

impl ScramServerExchange {
    pub fn new(username: String, cred: StoredScramCredential) -> Self { ... }
    pub fn new_with_principal(
        username: String,
        cred: StoredScramCredential,
        override_principal: KafkaPrincipal,
    ) -> Self { ... }
}
```

Auth handler:

```rust
let principal_override = img
    .delegation_token_by_id(&username)
    .map(|t| t.owner.clone());
let mut server = match principal_override {
    Some(owner) => ScramServerExchange::new_with_principal(username, cred, owner),
    None => ScramServerExchange::new(username, cred),
};
```

### 2.4 Re-auth ceiling

`ConnectionAuth::Authenticated { expires_at_ms: ... }` carries a
session-lifetime cap (shipped slice 50d). For token-authenticated
connections, `expires_at_ms = Some(token.expire_ts_ms)` — when the
token expires, re-auth fails and the connection drops. Reuse the
existing slice 50d ceiling plumbing.

### 2.5 Token-authenticated callers cannot delegate

`CreateDelegationToken` rejects with `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`
(err 81) when the calling connection's `ConnectionAuth::Authenticated`
principal corresponds to a token (i.e. the principal matches a token's
owner AND the SASL mechanism is SCRAM-SHA-256 AND the SCRAM username
was a tokenId). Detect by stamping a marker on the connection at
auth-time. Add `authenticated_via_token: bool` to `ConnectionAuth::Authenticated`.

---

## 3. Storage: `DelegationTokenRecord`

### 3.1 Record shape

New metadata record under `crates/protocol/src/records.rs`:

```rust
pub struct DelegationTokenRecord {
    pub token_id: String,                  // UUIDv4
    pub owner: KafkaPrincipal,             // e.g. ("User", "alice")
    pub hmac: Vec<u8>,                     // 32-byte HMAC-SHA-256 output (in slice 51 we use SHA-256, not 512)
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    pub max_timestamp_ms: i64,             // issue + lifetime; renew can't push past this
    pub renewers: Vec<KafkaPrincipal>,
    pub tombstone: bool,                   // true = remove from image
}
```

### 3.2 Tombstone vs replacement

- **CreateDelegationToken**: append record with `tombstone=false`.
- **RenewDelegationToken**: append record with same `token_id` +
  updated `expiry_timestamp_ms`, `tombstone=false`. Image replaces by
  `token_id`.
- **ExpireDelegationToken** with negative period: append record with
  `tombstone=true`. Image removes by `token_id`.
- **Background expiry sweep**: same as above — emit a tombstone record
  when a token's `expiry_timestamp_ms <= now`.

### 3.3 Image accessor

`crabka_raft::Image` gains:

```rust
pub struct Image {
    // ... existing fields ...
    delegation_tokens: HashMap<String /* token_id */, DelegationToken>,
}

impl Image {
    pub fn delegation_token_by_id(&self, token_id: &str) -> Option<&DelegationToken>;
    pub fn delegation_tokens_by_owner(&self, owner: &KafkaPrincipal) -> Vec<&DelegationToken>;
    pub fn delegation_tokens_visible_to(&self, principal: &KafkaPrincipal) -> Vec<&DelegationToken>;
    pub fn all_delegation_tokens(&self) -> impl Iterator<Item = &DelegationToken>;
}
```

`DelegationToken` is the in-memory image type (without `tombstone`).

### 3.4 Apply path

`apply_record` extends with:

```rust
RecordKind::DelegationToken(rec) => {
    if rec.tombstone {
        self.delegation_tokens.remove(&rec.token_id);
    } else {
        self.delegation_tokens.insert(
            rec.token_id.clone(),
            DelegationToken::from_record(rec),
        );
    }
}
```

### 3.5 Encoding

Owned + borrowed forms in `crates/protocol/src/records.rs` follow the
slice 16 / 16b / 49 pattern. Bincode for raft journal. No JSON
representation.

---

## 4. Master key

### 4.1 Source

Required; loaded once at broker startup. Two sources, precedence
order:

1. Env var `CRABKA_DELEGATION_TOKEN_SECRET_KEY` (Kafka's
   `KAFKA_DELEGATION_TOKEN_SECRET_KEY` convention — namespaced).
2. Broker config TOML: `[delegation_token] secret_key = "..."`.

If neither is set, the four handlers respond
`DELEGATION_TOKEN_AUTH_DISABLED` (err 61). Token-SCRAM lookups in
`auth.rs` short-circuit to the regular "unknown user" path. The token
expiry-sweep task does not start.

### 4.2 Storage in `BrokerConfig`

```rust
pub struct BrokerConfig {
    // ...
    pub delegation_token_secret_key: Option<Arc<SecretBytes>>,
    pub delegation_token_max_lifetime_ms: i64,           // default 7 * 24 * 3_600_000 = 604_800_000
    pub delegation_token_expiry_check_interval_ms: i64,  // default 1 * 3_600_000 = 3_600_000 (1 h)
}
```

`SecretBytes` is a `bytes::Bytes` newtype with `Debug` that prints
`SecretBytes(<32 bytes redacted>)` to keep keys out of logs.

### 4.3 HMAC computation helper

`crates/security/src/delegation_token.rs` (new file):

```rust
pub fn compute_token_hmac(secret_key: &[u8], token_id: &str) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret_key)
        .expect("HMAC accepts any key size");
    mac.update(token_id.as_bytes());
    mac.finalize().into_bytes().to_vec()
}
```

Adds `sha2` + `hmac` workspace deps — already present (used by SCRAM).

---

## 5. Authorization: `TOKEN` resource type

### 5.1 acl_wire.rs unblock

`crates/broker/src/handlers/acl_wire.rs:24` currently rejects
ResourceType 6 (`DELEGATION_TOKEN`). Replace the unconditional reject
with the normal pass-through; add `DELEGATION_TOKEN` to the
canonicalization table.

### 5.2 Resource matching

Resource name for delegation-token ACLs is the **owner's principal
string** (e.g. `"User:alice"`). Pattern types `LITERAL`, `PREFIXED`,
`MATCH` all apply via the existing matcher.

### 5.3 Operation matrix

| operation  | Create   | Renew         | Expire        | Describe         |
|-----------:|----------|---------------|---------------|------------------|
| any auth'd | implicit | implicit if owner/renewer | implicit if owner/renewer | implicit if owner/renewer |
| ACL        | n/a      | n/a           | n/a           | `Describe` on `TOKEN:<owner>` extends visibility |

Only `Describe` is an externally grantable token operation —
`Create/Renew/Expire` are implicit-on-ownership.

---

## 6. Background expiry sweep

### 6.1 Task

`crates/broker/src/delegation_token_cleanup.rs` (new), spawned from
`Broker::start` when the master key is set. Runs every
`delegation_token_expiry_check_interval_ms` (default 1h).

```rust
pub async fn run(
    controller: ControllerHandle,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => sweep(&controller).await,
            _ = shutdown.cancelled() => return,
        }
    }
}

async fn sweep(controller: &ControllerHandle) {
    let now = chrono::Utc::now().timestamp_millis();
    let expired: Vec<String> = controller
        .current_image()
        .all_delegation_tokens()
        .filter(|t| t.expiry_timestamp_ms <= now)
        .map(|t| t.token_id.clone())
        .collect();
    for id in expired {
        controller.append_record(MetadataRecord::DelegationToken(
            DelegationTokenRecord { token_id: id, tombstone: true, .. }
        )).await;
    }
}
```

### 6.2 Single-broker-emits guarantee

Multiple brokers running the sweep concurrently would all emit
tombstones for the same expired token. Raft serializes appends; the
second tombstone is a no-op (image already removed). Acceptable —
matches Kafka's "every broker sweeps, idempotent" pattern.

---

## 7. Decomposition for the plan

Eight tasks, grouped into batches by file-set independence:

**Batch 1 — record + image + helper** (parallel: T1, T2, T3)
- **T1**: `DelegationTokenRecord` in `crates/protocol/src/records.rs`
  + bincode encode/decode + image apply branch in `crabka_raft`.
- **T2**: `DelegationToken` image type + accessors (`delegation_token_by_id`,
  `delegation_tokens_by_owner`, `delegation_tokens_visible_to`,
  `all_delegation_tokens`).
- **T3**: `compute_token_hmac` + `SecretBytes` newtype in
  `crates/security/src/delegation_token.rs`.

**Batch 2 — config + handler scaffolding** (parallel: T4, T5)
- **T4**: BrokerConfig fields (secret key, lifetime config, sweep
  interval) + TOML parsing in `file_config.rs` + env-var precedence.
- **T5**: Four handler stubs (Create/Renew/Expire/Describe) returning
  `DELEGATION_TOKEN_AUTH_DISABLED` when key absent, wired into
  `network/dispatch.rs` request routing.

**Batch 3 — handler bodies** (parallel: T6, T7) — depends on B1 + B2
- **T6**: `CreateDelegationToken` + `DescribeDelegationToken` full
  implementations (more complex — UUID, HMAC, persist + auth-gate
  checks for Create; owner/renewer visibility for Describe).
- **T7**: `RenewDelegationToken` + `ExpireDelegationToken` full
  implementations (HMAC-lookup-based, simpler).

**Batch 4 — auth + ACL + sweep** (parallel: T8, T9, T10) — depends on B3
- **T8**: SCRAM auth.rs token-fallback + `ScramServerExchange::new_with_principal`
  + `ConnectionAuth::Authenticated.authenticated_via_token` + token-creates-token rejection.
- **T9**: `acl_wire.rs` TOKEN resource type unblock + matcher
  canonicalization + Describe-on-TOKEN authorization gate in T6's
  Describe handler.
- **T10**: Background expiry sweep task + `Broker::start` wiring +
  shutdown integration.

**Batch 5 — integration + e2e** (parallel: T11, T12) — depends on B4
- **T11**: Broker integration test
  `crates/broker/tests/delegation_tokens.rs`: create → describe →
  authenticate-as-token → renew → expire → verify gone.
- **T12**: JVM acceptance test
  `jvm_kafka_delegation_tokens_end_to_end` — `kafka-delegation-tokens.sh
  --create` then SCRAM auth with returned creds.

**Batch 6 — STATUS** (sequential: T13)
- **T13**: STATUS entry + final fmt/clippy/test gate.

---

## 8. Testing

### 8.1 Unit tests (~22 total)

| file                                                  | new tests | covers                                                                 |
|-------------------------------------------------------|----------:|------------------------------------------------------------------------|
| `crates/security/src/delegation_token.rs`             |         3 | HMAC determinism, key sensitivity, SecretBytes Debug redaction         |
| `crates/protocol/src/records.rs` (or wherever record encoding lives) | 2 | record round-trip encode/decode, tombstone flag round-trip   |
| `crabka_raft` image apply tests                       |         3 | apply insert / apply replace / apply tombstone                         |
| `crates/broker/src/handlers/create_delegation_token.rs` | 4       | success, auth-disabled, token-creates-token rejected, max-lifetime clamping |
| `crates/broker/src/handlers/renew_delegation_token.rs` | 3        | success-as-owner, success-as-renewer, owner-mismatch                    |
| `crates/broker/src/handlers/expire_delegation_token.rs`| 3        | future-expiry, immediate-delete (negative period), owner-mismatch       |
| `crates/broker/src/handlers/describe_delegation_token.rs` | 3     | empty-filter, owner-filter-match, token-authed-sees-own-only             |
| `crates/broker/src/network/auth.rs`                   |         1 | token-username-falls-back-to-token-cred (synthetic image fixture)       |

### 8.2 Broker integration test

`crates/broker/tests/delegation_tokens.rs`, one big end-to-end test:

1. Boot single-broker SASL/PLAIN cluster with master key configured.
2. Authenticate as `alice` via SASL/PLAIN.
3. `CreateDelegationToken { renewers: [User:bob], max_lifetime_ms: -1 }`
   → returns token_id + hmac.
4. Open a second connection. SASL handshake SCRAM-SHA-256 with
   username=token_id, password=hmac → success. Verify the connection's
   principal is `User:alice`.
5. Token-authed connection: `CreateDelegationToken` →
   `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`.
6. `RenewDelegationToken` from `bob`'s connection → success.
7. `DescribeDelegationToken { owners: [User:alice] }` from `alice` → 1 token returned.
8. `ExpireDelegationToken { expire_period_ms: -1 }` from `alice` → token gone.
9. Re-auth on the token-authed connection (or just open a fresh
   token-SCRAM connection) → fails.

### 8.3 JVM acceptance

`jvm_kafka_delegation_tokens_end_to_end` in `crates/broker/tests/jvm_acceptance.rs`,
`#[ignore]`-tagged, WSL.

1. 3-broker SASL/PLAINTEXT + SCRAM-SHA-256 cluster with master key in env.
2. `kafka-delegation-tokens.sh --create --max-life-time-period -1 --command-config admin.props`
   → exit 0; capture `token_id` + `HMAC` from stdout.
3. Write a temp `token.props` with `sasl.mechanism=SCRAM-SHA-256` +
   `sasl.jaas.config=...username=<token_id> password=<hmac>...`.
4. `kafka-console-producer --topic foo --producer.config token.props <<<'hello'` → exit 0.
5. `kafka-delegation-tokens.sh --describe --owner-principal User:admin --command-config admin.props`
   → stdout shows the token.
6. `kafka-delegation-tokens.sh --expire --expiry-time-period -1 --hmac <hmac> --command-config admin.props`
   → exit 0.

---

## 9. Known limitations / non-goals

- **Master key hot-swap**: any key change invalidates all in-flight
  tokens; restart-only change. Document in STATUS.
- **IPv6 entity-string in TOKEN ACLs**: TOKEN ACLs name principals
  (e.g. `"User:alice"`), not IPs. The slice 13 / slice 16b IPv4-only
  limitation is orthogonal; no IPv6 concern here.
- **No KafkaUser operator surface**. The operator CRD does not yet
  surface `KafkaUser.spec.authentication.type: delegation-token` or
  similar. A follow-up sub-slice will add it (roadmap acknowledges).
- **No per-token rate-limit on CreateDelegationToken**. A
  rogue/compromised principal can spam token creation. Matches Kafka.

---

## 10. Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo test -p crabka-broker --test delegation_tokens`
- `cargo test -p crabka-broker --test jvm_acceptance -- --ignored jvm_kafka_delegation_tokens_end_to_end` (WSL; not run in CI)
- CRD drift check stays green (no CRD changes in this slice).
