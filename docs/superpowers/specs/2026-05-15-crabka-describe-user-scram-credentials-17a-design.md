# Slice 17a: DescribeUserScramCredentials (api_key 50) — Design

**Status:** Approved 2026-05-15.

**Goal:** Implement `DescribeUserScramCredentials` (api_key 50, KIP-554 read half) so that `kafka-configs --describe --entity-type users` and `--delete-config` exit 0 cleanly. Closes the JVM-tool quirk that slices 16/16b/16c worked around via stdout-substring assertions.

**Out of scope:**
- Slice 16 `client_id` HandlerTable gap — slice 17b
- SCRAM-SHA-256 (Crabka only implements SHA-512; the handler maps both mechanism codes but only SHA-512 entries appear in practice)

---

## 1. Scope

### In

- `DescribeUserScramCredentials` (api_key 50, v0, flexible from v0) handler
- Filter semantics: `users: None` OR `Some([])` → describe all known SCRAM users; non-empty list → filter
- Per-user response: `(user, error_code, error_message, credential_infos: Vec<{mechanism: i8, iterations: i32}>)`
- Mechanism encoding: SCRAM-SHA-256 → 1, SCRAM-SHA-512 → 2 (Kafka's `ScramMechanism` enum)
- Reads from existing `MetadataImage::scram_credentials: HashMap<(String, SaslMechanism), ScramCredential>` (slice 12)
- Authorization: **Cluster Alter** (matches slice 12 `AlterUserScramCredentials` — JVM AdminClient uses Alter for both Alter and Describe SCRAM operations)
- Unknown users → per-user `RESOURCE_NOT_FOUND (83)`
- Inline-intercept dispatch (handler needs `&Principal` for authorize)
- JVM acceptance retroactive cleanup: 3 existing slice-16-family tests bypass `assert!(status.success())` on `kafka-configs --describe`/`--delete-config users` because of this very gap. Slice 17a updates them to assert on exit code.
- 1 new JVM acceptance test for the describe round-trip.

### Not in

- `client_id` HandlerTable threading (slice 17b)
- KIP-857 extensions
- SCRAM-SHA-256 storage / Alter handler (out of scope; slice 12 declined; would require codepath additions)

### Wire shapes — confirmed via `crates/protocol/generated/DescribeUserScramCredentials{Request,Response}.owned.rs`

`DescribeUserScramCredentialsRequest` (api_key 50, v0, flex from v0):
- `users: Option<Vec<UserName { name: String }>>`

`DescribeUserScramCredentialsResponse`:
- `throttle_time_ms: i32`
- `error_code: i16`
- `error_message: Option<String>`
- `results: Vec<DescribeUserScramCredentialsResult { user: String, error_code: i16, error_message: Option<String>, credential_infos: Vec<CredentialInfo { mechanism: i8, iterations: i32 }> }>`

---

## 2. Storage & accessors

No new storage. Existing `MetadataImage::scram_credentials` (slice 12) holds `(user, mechanism) → ScramCredential { iterations, salt, server_key, stored_key }`. Slice 17a adds iteration accessors:

```rust
/// All distinct users with at least one SCRAM credential.
pub fn scram_credentials_users(&self) -> Vec<String> {
    self.scram_credentials
        .keys()
        .map(|(u, _)| u.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect()
}

/// All (mechanism, iterations) pairs for a given user. Empty if user
/// has no SCRAM credentials.
pub fn scram_credentials_for_user(&self, user: &str) -> Vec<(SaslMechanism, i32)> {
    self.scram_credentials
        .iter()
        .filter(|((u, _), _)| u == user)
        .map(|((_, mech), cred)| (*mech, cred.iterations))
        .collect()
}
```

(Verify `ScramCredential.iterations` field name in `crates/metadata/src/records.rs` — slice 12 calls it `iterations` per the wire spec.)

---

## 3. Handler

`crates/broker/src/handlers/describe_user_scram_credentials.rs` (new):

1. **Authorize** Cluster Alter (deny → top-level `error_code = CLUSTER_AUTHORIZATION_FAILED`, empty `results`).
2. **Compute targets:**
   - `req.users = None` OR `Some([])` → all users from `image.scram_credentials_users()`.
   - Non-empty → use the filter list.
3. **Per user:**
   - Lookup `image.scram_credentials_for_user(user)`.
   - If empty AND user wasn't in `scram_credentials_users()` → emit row with `error_code = RESOURCE_NOT_FOUND (83)`.
   - Otherwise emit row with `credential_infos = [(mechanism_byte, iterations) ...]`.
4. **Encode** response.

```rust
fn sasl_mechanism_to_byte(m: SaslMechanism) -> i8 {
    match m {
        SaslMechanism::ScramSha256 => 1,
        SaslMechanism::ScramSha512 => 2,
        _ => 0,
    }
}
```

### Error code

`RESOURCE_NOT_FOUND = 83` — verify presence in `crates/broker/src/codes.rs`. If absent, slice 17a adds it. (Likely already exists from slice 11 or earlier.)

### Dispatch wiring

Slice-13/14/15/16 inline-intercept pattern. Add to `crates/broker/src/network/dispatch.rs`:

```rust
if peek_api_key(&frame) == Some(50) {
    handle_describe_user_scram_credentials_frame(...).await?;
    continue;
}
```

Plus flex-table entry + `v!(describe_user_scram_credentials_request)` in `supported_apis`.

---

## 4. Testing

### Unit tests (4) — in the handler module

- `describe_all_users_when_filter_none`
- `describe_filter_returns_only_listed_users`
- `unknown_user_returns_resource_not_found`
- `non_authorized_returns_cluster_authorization_failed`

### Broker integration tests (`crates/broker/tests/describe_user_scram_credentials.rs`, 2 tests)

1. **`describe_all_users_round_trip`** — single-broker SASL/PLAIN; admin user pre-provisioned with SCRAM-SHA-512 (slice 12 idiom); use AlterUserScramCredentials to add alice; call DescribeUserScramCredentials with `users=None`; assert both users appear with `mechanism=2, iterations≥4096`.

2. **`describe_unknown_user_returns_error`** — provision admin; call describe with `users=[ghost]`; assert per-user `error_code=RESOURCE_NOT_FOUND (83)`.

### JVM acceptance retroactive cleanup (3 existing tests modified)

Slice 16 T13, 16b T5, 16c T7 currently use `std::process::Command` directly for `--describe` and `--delete-config` because the underlying api_key 50 wasn't implemented. Slice 17a swaps each back to `docker_run_kafka_tool_with_image_and_mount` + `assert!(status.success())`:

- `jvm_kafka_configs_alter_client_quota_end_to_end` (slice 16 T13)
- `jvm_kafka_configs_alter_ip_quota_end_to_end` (slice 16b T5)
- `jvm_kafka_configs_alter_controller_mutation_rate_end_to_end` (slice 16c T7)

### JVM acceptance new test (1)

`jvm_kafka_configs_describe_users_scram_credentials_end_to_end`:

1. Spin up 3-broker SASL/PLAINTEXT cluster (already has admin + at least one SCRAM user).
2. Run `kafka-configs --describe --entity-type users --entity-name <username>` → assert exit 0, stdout contains `SCRAM-SHA-512` mention.

(Skip the alter/delete round-trip since slice 12 already covers it.)

---

## 5. File structure & task layout

```
crates/metadata/src/image.rs                     # MODIFIED — scram_credentials_users + scram_credentials_for_user + 2 unit tests
crates/broker/src/codes.rs                       # MODIFIED if needed — RESOURCE_NOT_FOUND
crates/broker/src/handlers/
├── describe_user_scram_credentials.rs           # NEW — handler + 4 unit tests
├── mod.rs                                       # MODIFIED — register
└── api_versions.rs                              # MODIFIED — supported_apis += 50
crates/broker/src/network/dispatch.rs            # MODIFIED — intercept arm + helper
crates/broker/tests/
├── describe_user_scram_credentials.rs           # NEW — 2 broker integration tests
└── jvm_acceptance.rs                            # MODIFIED — 3 retro-fix + 1 new
```

Implementation plan target: ~5 tasks.
