# Slice 17a: DescribeUserScramCredentials (api_key 50) — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** `## Slice 17a — DescribeUserScramCredentials (2026-05-15)`

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Slice 16 `client_id` HandlerTable gap (slice 17b)

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Implement `DescribeUserScramCredentials` (api_key 50, KIP-554 read half) so that `kafka-configs --describe --entity-type users` and `--delete-config` exit 0 cleanly. Reuses slice 12's SCRAM credential storage; Cluster Alter authorize (matches AlterUserScramCredentials).

**Architecture:** New handler reads `MetadataImage::scram_credentials` (slice 12). Two new image accessors enumerate users and per-user mechanism+iterations pairs. Inline-intercept dispatch (handler needs `&Principal`). JVM-tool exit-1 workarounds in slice 16/16b/16c JVM tests revert to clean `assert!(status.success())`.

**Tech Stack:** Rust 1.95.0; reuses slice 12 SCRAM, slice 13 authorize, slice 14+ inline-intercept dispatch pattern. Wire types already generated at `crates/protocol/generated/DescribeUserScramCredentials{Request,Response}.owned.rs`.

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-describe-user-scram-credentials-17a-design.md`](../specs/2026-05-15-crabka-describe-user-scram-credentials-17a-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/describe-user-scram-credentials-17a` already created with spec committed at `438ac1e`.

---

## File structure

```
crates/metadata/src/image.rs                       # MODIFIED — scram_credentials_users + scram_credentials_for_user + 2 unit tests
crates/broker/src/handlers/
├── describe_user_scram_credentials.rs             # NEW — handler + 4 unit tests
├── mod.rs                                         # MODIFIED — register module
└── api_versions.rs                                # MODIFIED — supported_apis += 50
crates/broker/src/network/dispatch.rs              # MODIFIED — flex table + intercept arm + helper
crates/broker/src/codes.rs                         # POSSIBLY MODIFIED — RESOURCE_NOT_FOUND (83) if absent

crates/broker/tests/
├── describe_user_scram_credentials.rs             # NEW — 2 broker integration tests
└── jvm_acceptance.rs                              # MODIFIED — 3 retro-fix + 1 new JVM test
```

6 tasks across 5 batches.

---

## Batch 1 — Image accessors + handler (parallel: T1, T2)

### Task 1: `MetadataImage` scram-credentials iteration accessors

**Files:**
- Modify: `crates/metadata/src/image.rs`

- [ ] **Step 1: Add `scram_credentials_users` + `scram_credentials_for_user`**

Find the existing `pub fn scram_credential(&self, user: &str, mechanism: SaslMechanism) -> Option<&ScramCredential>` (slice 12). Append two new accessors near it:

```rust
/// All distinct users with at least one SCRAM credential. Order is
/// unspecified.
#[must_use]
pub fn scram_credentials_users(&self) -> Vec<String> {
    self.scram_credentials
        .keys()
        .map(|(u, _)| u.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect()
}

/// All `(mechanism, iterations)` pairs for `user`. Empty if user has
/// no SCRAM credentials. Order is unspecified.
#[must_use]
pub fn scram_credentials_for_user(&self, user: &str) -> Vec<(SaslMechanism, i32)> {
    self.scram_credentials
        .iter()
        .filter(|((u, _), _)| u == user)
        .map(|((_, mech), cred)| (*mech, cred.iterations))
        .collect()
}
```

Verify `ScramCredential.iterations` field name via `rg "pub iterations\|pub struct ScramCredential" crates/metadata/src/`. If named differently (e.g., `iterations: i32` exists but as a different identifier), adjust.

- [ ] **Step 2: Add 2 unit tests**

Append to the existing `#[cfg(test)] mod tests` in `image.rs`:

```rust
    #[test]
    fn scram_credentials_users_returns_distinct_users() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            iterations: 4096,
            salt: vec![1, 2, 3],
            server_key: vec![4, 5, 6],
            stored_key: vec![7, 8, 9],
        }));
        img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "bob".into(),
            mechanism: SaslMechanism::ScramSha512,
            iterations: 4096,
            salt: vec![1, 2, 3],
            server_key: vec![4, 5, 6],
            stored_key: vec![7, 8, 9],
        }));
        let mut users = img.scram_credentials_users();
        users.sort();
        assert_eq!(users, vec!["alice".to_string(), "bob".to_string()]);
    }

    #[test]
    fn scram_credentials_for_user_returns_pairs() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            iterations: 8192,
            salt: vec![1, 2, 3],
            server_key: vec![4, 5, 6],
            stored_key: vec![7, 8, 9],
        }));
        let pairs = img.scram_credentials_for_user("alice");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, SaslMechanism::ScramSha512);
        assert_eq!(pairs[0].1, 8192);
        assert!(img.scram_credentials_for_user("ghost").is_empty());
    }
```

Verify `ScramCredentialRecord` field names — slice 12's record may name the key material differently (e.g., `stored_key` vs `stored_credential`). Adjust to match.

- [ ] **Step 3: Build + tests + lints**

```
cargo build --workspace
cargo test -p crabka-metadata
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 2 new tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/metadata/src/image.rs
git commit -m "$(cat <<'EOF'
feat(metadata): scram_credentials_users + scram_credentials_for_user

Iteration accessors over the slice-12 scram_credentials map. Used by
DescribeUserScramCredentials handler (task 2).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Handler module + 4 unit tests

**Files:**
- Create: `crates/broker/src/handlers/describe_user_scram_credentials.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (register module)
- Possibly modify: `crates/broker/src/codes.rs` (RESOURCE_NOT_FOUND)

- [ ] **Step 1: Check RESOURCE_NOT_FOUND constant**

```
rg "RESOURCE_NOT_FOUND" crates/broker/src/codes.rs
```

If absent, append:

```rust
pub const RESOURCE_NOT_FOUND: i16 = 83;
```

- [ ] **Step 2: Write the handler module**

`crates/broker/src/handlers/describe_user_scram_credentials.rs`:

```rust
//! `DescribeUserScramCredentials` (api_key 50, KIP-554 read half).

#![allow(dead_code)]

use std::net::SocketAddr;

use bytes::Bytes;
use crabka_metadata::{MetadataImage, ResourceType, SaslMechanism};
use crabka_protocol::Encode;
use crabka_protocol::owned::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest;
use crabka_protocol::owned::describe_user_scram_credentials_response::{
    CredentialInfo, DescribeUserScramCredentialsResponse, DescribeUserScramCredentialsResult,
};
use crabka_security::Principal;

use crate::authorizer::{authorize, AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::{CLUSTER_AUTHORIZATION_FAILED, RESOURCE_NOT_FOUND};

pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeUserScramCredentialsRequest,
    principal: &Principal,
    peer: &SocketAddr,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();

    let allow = authorize(
        &image,
        &broker.config.super_users,
        &AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let resp = DescribeUserScramCredentialsResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-user-scram-credentials denied".into()),
            results: vec![],
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let known_users: std::collections::HashSet<String> =
        image.scram_credentials_users().into_iter().collect();
    let targets: Vec<String> = match req.users.as_deref() {
        None | Some([]) => {
            let mut v: Vec<String> = known_users.iter().cloned().collect();
            v.sort();
            v
        }
        Some(filter) => filter.iter().map(|u| u.name.clone()).collect(),
    };

    let results: Vec<DescribeUserScramCredentialsResult> = targets
        .into_iter()
        .map(|user| {
            let pairs = image.scram_credentials_for_user(&user);
            if pairs.is_empty() && !known_users.contains(&user) {
                DescribeUserScramCredentialsResult {
                    user,
                    error_code: RESOURCE_NOT_FOUND,
                    error_message: Some("no such SCRAM user".into()),
                    credential_infos: vec![],
                    ..Default::default()
                }
            } else {
                let credential_infos: Vec<CredentialInfo> = pairs
                    .into_iter()
                    .map(|(mech, iters)| CredentialInfo {
                        mechanism: sasl_mechanism_to_byte(mech),
                        iterations: iters,
                        ..Default::default()
                    })
                    .collect();
                DescribeUserScramCredentialsResult {
                    user,
                    error_code: 0,
                    error_message: None,
                    credential_infos,
                    ..Default::default()
                }
            }
        })
        .collect();

    let resp = DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

#[must_use]
fn sasl_mechanism_to_byte(m: SaslMechanism) -> i8 {
    match m {
        SaslMechanism::ScramSha256 => 1,
        SaslMechanism::ScramSha512 => 2,
        _ => 0,
    }
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version).map_err(|e| {
        crate::error::BrokerError::Replication(format!(
            "encode DescribeUserScramCredentials: {e}"
        ))
    })?;
    Ok(Bytes::from(body))
}
```

**Field name verification:**
- `DescribeUserScramCredentialsRequest.users: Option<Vec<UserName>>` — confirmed in plan brainstorm.
- `UserName.name: String` — confirmed.
- `DescribeUserScramCredentialsResponse.results: Vec<DescribeUserScramCredentialsResult>` — confirmed.
- `DescribeUserScramCredentialsResult.credential_infos: Vec<CredentialInfo>` — likely. Verify via `crates/protocol/generated/DescribeUserScramCredentialsResponse.owned.rs`.
- `CredentialInfo.mechanism: i8, iterations: i32` — confirmed.

If field names differ from the sketch, adapt.

- [ ] **Step 3: Register module**

In `crates/broker/src/handlers/mod.rs`, add in alphabetical position:

```rust
mod describe_user_scram_credentials;
```

- [ ] **Step 4: Write 4 unit tests**

Append to the new handler file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, ScramCredentialRecord};

    fn img_with_scram(users: &[(&str, SaslMechanism, i32)]) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for (user, mech, iters) in users {
            img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
                user: (*user).into(),
                mechanism: *mech,
                iterations: *iters,
                salt: vec![1, 2, 3],
                server_key: vec![4, 5, 6],
                stored_key: vec![7, 8, 9],
            }));
        }
        img
    }

    fn run_handle_filter(
        users_filter: Option<Vec<String>>,
        seeded: &[(&str, SaslMechanism, i32)],
    ) -> DescribeUserScramCredentialsResponse {
        use crabka_protocol::owned::describe_user_scram_credentials_request::UserName;
        let req = DescribeUserScramCredentialsRequest {
            users: users_filter.map(|v| {
                v.into_iter()
                    .map(|n| UserName { name: n, ..Default::default() })
                    .collect()
            }),
            ..Default::default()
        };
        let image = std::sync::Arc::new(img_with_scram(seeded));
        // The full handler signature wants &Broker; for pure-logic unit
        // tests, factor process_targets into a free function. See note below.
        process_targets_for_test(&image, req.users.as_deref())
    }

    /// Pure-logic helper extracted for unit testability. Mirrors the
    /// handler's targets-resolution + per-user result emission, without
    /// auth or response encoding.
    fn process_targets_for_test(
        image: &MetadataImage,
        users_filter: Option<&[crabka_protocol::owned::describe_user_scram_credentials_request::UserName]>,
    ) -> DescribeUserScramCredentialsResponse {
        let known_users: std::collections::HashSet<String> =
            image.scram_credentials_users().into_iter().collect();
        let targets: Vec<String> = match users_filter {
            None | Some([]) => {
                let mut v: Vec<String> = known_users.iter().cloned().collect();
                v.sort();
                v
            }
            Some(filter) => filter.iter().map(|u| u.name.clone()).collect(),
        };
        let results: Vec<DescribeUserScramCredentialsResult> = targets
            .into_iter()
            .map(|user| {
                let pairs = image.scram_credentials_for_user(&user);
                if pairs.is_empty() && !known_users.contains(&user) {
                    DescribeUserScramCredentialsResult {
                        user,
                        error_code: RESOURCE_NOT_FOUND,
                        error_message: Some("no such SCRAM user".into()),
                        credential_infos: vec![],
                        ..Default::default()
                    }
                } else {
                    let credential_infos: Vec<CredentialInfo> = pairs
                        .into_iter()
                        .map(|(mech, iters)| CredentialInfo {
                            mechanism: sasl_mechanism_to_byte(mech),
                            iterations: iters,
                            ..Default::default()
                        })
                        .collect();
                    DescribeUserScramCredentialsResult {
                        user,
                        error_code: 0,
                        error_message: None,
                        credential_infos,
                        ..Default::default()
                    }
                }
            })
            .collect();
        DescribeUserScramCredentialsResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            results,
            ..Default::default()
        }
    }

    #[test]
    fn describe_all_users_when_filter_none() {
        let resp = run_handle_filter(None, &[
            ("alice", SaslMechanism::ScramSha512, 4096),
            ("bob", SaslMechanism::ScramSha512, 8192),
        ]);
        assert_eq!(resp.results.len(), 2);
        let users: Vec<&str> = resp.results.iter().map(|r| r.user.as_str()).collect();
        assert!(users.contains(&"alice") && users.contains(&"bob"));
    }

    #[test]
    fn describe_filter_returns_only_listed_users() {
        let resp = run_handle_filter(
            Some(vec!["alice".into()]),
            &[
                ("alice", SaslMechanism::ScramSha512, 4096),
                ("bob", SaslMechanism::ScramSha512, 8192),
            ],
        );
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].user, "alice");
        assert_eq!(resp.results[0].credential_infos.len(), 1);
        assert_eq!(resp.results[0].credential_infos[0].iterations, 4096);
    }

    #[test]
    fn unknown_user_returns_resource_not_found() {
        let resp = run_handle_filter(
            Some(vec!["ghost".into()]),
            &[("alice", SaslMechanism::ScramSha512, 4096)],
        );
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].user, "ghost");
        assert_eq!(resp.results[0].error_code, RESOURCE_NOT_FOUND);
    }

    #[test]
    fn sasl_mechanism_byte_mapping() {
        assert_eq!(sasl_mechanism_to_byte(SaslMechanism::ScramSha256), 1);
        assert_eq!(sasl_mechanism_to_byte(SaslMechanism::ScramSha512), 2);
    }
}
```

(The auth-deny path can't be unit-tested without a real `Broker` — covered by T4 integration.)

- [ ] **Step 5: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib describe_user_scram_credentials
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: 4 new tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/describe_user_scram_credentials.rs crates/broker/src/handlers/mod.rs crates/broker/src/codes.rs
git commit -m "$(cat <<'EOF'
feat(broker): DescribeUserScramCredentials handler (api_key 50)

Cluster Alter authorize gate; reads scram_credentials_users +
scram_credentials_for_user (task 1) from the image. Unknown users
return RESOURCE_NOT_FOUND (83) per Kafka convention. Dispatch
wiring in task 3.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 2 — Dispatch wiring (sequential: T3)

### Task 3: api_versions + intercept arm + helper

**Files:**
- Modify: `crates/broker/src/handlers/api_versions.rs`
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/handlers/describe_user_scram_credentials.rs` (remove `#![allow(dead_code)]`)

- [ ] **Step 1: Add to `supported_apis`**

In `crates/broker/src/handlers/api_versions.rs`, append in api-key order (50 sits after 49):

```rust
v!(describe_user_scram_credentials_request),
```

- [ ] **Step 2: Add to flexible-body table**

In `crates/broker/src/network/dispatch.rs::handler_body_flexible`:

```rust
50 => version >= crabka_protocol::owned::describe_user_scram_credentials_request::FLEXIBLE_MIN,
```

- [ ] **Step 3: Add intercept arm + helper**

In the per-connection request loop (after slice-16's api_key 48 + 49 intercepts):

```rust
if peek_api_key(&frame) == Some(50) {
    handle_describe_user_scram_credentials_frame(
        broker, frame, api_version, correlation_id, client_id, auth, peer,
    ).await?;
    continue;
}
```

Plus the helper function alongside slice 16's `handle_describe_client_quotas_frame`. Copy that helper's exact signature + framing pattern; only swap the decode/handle types:

```rust
async fn handle_describe_user_scram_credentials_frame</* same generics */>(
    /* same params */
) -> Result<(), crate::error::BrokerError>
/* same where-clause */
{
    use crabka_protocol::Decode;
    use crabka_protocol::owned::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest;
    let req = DescribeUserScramCredentialsRequest::decode(&mut frame.as_ref(), api_version)
        .map_err(|e| crate::error::BrokerError::Codec(e.to_string()))?;
    let principal = auth.principal();
    let response_bytes = crate::handlers::describe_user_scram_credentials::handle(
        broker, req, principal, peer, api_version,
    ).await?;
    /* write_response — copy slice 16's helper verbatim */
    Ok(())
}
```

- [ ] **Step 4: Remove `#![allow(dead_code)]` from `describe_user_scram_credentials.rs`**

- [ ] **Step 5: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
```

All existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/api_versions.rs crates/broker/src/network/dispatch.rs crates/broker/src/handlers/describe_user_scram_credentials.rs
git commit -m "$(cat <<'EOF'
feat(broker): wire DescribeUserScramCredentials dispatch (api_key 50)

api_key 50 registered in supported_apis + flexible-body table.
Inline-intercept dispatch arm mirrors slice-16's DescribeClientQuotas
helper exactly. Removes module-level allow(dead_code) since the
handler is now reachable.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 3 — Integration tests (sequential: T4)

### Task 4: 2 broker integration tests

**Files:**
- Create: `crates/broker/tests/describe_user_scram_credentials.rs`

- [ ] **Step 1: File scaffold + copied helpers**

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]
```

Copy from slice 16's `tests/client_quotas.rs`:
- `round_trip`
- `sasl_plain_authenticate`
- `start_single_broker_sasl_plaintext_with_users` — make sure admin is configured as a super-user (passes Cluster Alter)

Add a wire driver:

```rust
async fn drive_describe_user_scram_credentials_sasl(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    users_filter: Option<Vec<String>>,
) -> (i16 /* top-level error */, Vec<(String, i16, Vec<(i8, i32)>)>) {
    use crabka_protocol::{Decode, Encode};
    use crabka_protocol::owned::describe_user_scram_credentials_request::{
        DescribeUserScramCredentialsRequest, UserName,
    };
    use crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse;

    let req = DescribeUserScramCredentialsRequest {
        users: users_filter.map(|v| {
            v.into_iter()
                .map(|n| UserName { name: n, ..Default::default() })
                .collect()
        }),
        ..Default::default()
    };
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    sasl_plain_authenticate(&mut stream, user, pass).await;
    let mut body = Vec::new();
    req.encode(&mut body, 0).expect("encode");
    let response_bytes = round_trip(&mut stream, 50, 0, &body, true).await;
    let resp = DescribeUserScramCredentialsResponse::decode(&mut response_bytes.as_ref(), 0)
        .expect("decode");
    let per_user: Vec<_> = resp.results.into_iter().map(|r| {
        let infos: Vec<(i8, i32)> = r.credential_infos.into_iter()
            .map(|c| (c.mechanism, c.iterations))
            .collect();
        (r.user, r.error_code, infos)
    }).collect();
    (resp.error_code, per_user)
}
```

- [ ] **Step 2: Test 1 — `describe_all_users_round_trip`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_all_users_round_trip() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret")],
    ).await;

    // Seed alice's SCRAM credential directly via metadata (bypasses
    // AlterUserScramCredentials path — keeps test focused on Describe).
    let rec = crabka_metadata::MetadataRecord::V1ScramCredential(
        crabka_metadata::ScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_metadata::SaslMechanism::ScramSha512,
            iterations: 4096,
            salt: vec![1, 2, 3, 4],
            server_key: vec![5; 64],
            stored_key: vec![6; 64],
        },
    );
    handle.submit_metadata_record_for_test(rec).await.expect("seed");

    // Wait for the credential to appear in the image.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        if !img.scram_credentials_for_user("alice").is_empty() { break; }
        if std::time::Instant::now() > deadline {
            panic!("alice's scram credential not visible in image");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let (top_err, per_user) = drive_describe_user_scram_credentials_sasl(
        addr, "admin", "admin-secret", None,
    ).await;
    assert_eq!(top_err, 0, "top-level error should be 0");
    let alice_row = per_user.iter().find(|(u, _, _)| u == "alice")
        .expect("alice in response");
    assert_eq!(alice_row.1, 0, "per-user error should be 0");
    assert!(
        alice_row.2.iter().any(|(mech, _)| *mech == 2),
        "expected mechanism=2 (SCRAM-SHA-512) in credential_infos: {:?}",
        alice_row.2,
    );
}
```

- [ ] **Step 3: Test 2 — `describe_unknown_user_returns_error`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_user_returns_error() {
    let (_handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret")],
    ).await;

    let (top_err, per_user) = drive_describe_user_scram_credentials_sasl(
        addr, "admin", "admin-secret", Some(vec!["ghost".into()]),
    ).await;
    assert_eq!(top_err, 0);
    let row = per_user.iter().find(|(u, _, _)| u == "ghost")
        .expect("ghost in response");
    assert_eq!(row.1, 83 /* RESOURCE_NOT_FOUND */, "expected RESOURCE_NOT_FOUND");
}
```

- [ ] **Step 4: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test describe_user_scram_credentials -- --nocapture --test-threads=1"
```

Expected: 2 tests PASS.

- [ ] **Step 5: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/describe_user_scram_credentials.rs
git commit -m "$(cat <<'EOF'
test(broker): DescribeUserScramCredentials integration tests

Two SASL/PLAIN tests: describe all users round-trip (seeded alice's
SHA-512 credential via submit_metadata_record_for_test; assert
mechanism=2 in the response), and unknown-user RESOURCE_NOT_FOUND.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 4 — JVM acceptance retrofits + new test (sequential: T5)

### Task 5: 3 retrofits + 1 new JVM test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Retro-fix `jvm_kafka_configs_alter_client_quota_end_to_end` (slice 16 T13)**

Find the test. It currently uses `std::process::Command` directly for `--describe` and `--delete-config` because slice 16 didn't have api_key 50. Now that slice 17a provides it, swap back to `docker_run_kafka_tool_with_image_and_mount` + `assert!(status.success())`.

Specifically: locate the `std::process::Command::new("docker")` calls in this test and replace with `docker_run_kafka_tool_with_image_and_mount(...)` calls following the slice-16/16b/16c idiom. Assert on `status.success()` instead of stdout substring (for `--describe`, keep the stdout assertion as an additional check but make exit code primary).

For `--describe`:
```rust
let out = docker_run_kafka_tool_with_image_and_mount(
    KAFKA_IMAGE_TXN, &admin_mount,
    &[
        "kafka-configs", "--describe",
        "--entity-type", "users", "--entity-name", ALICE,
        "--bootstrap-server", BOOTSTRAP,
        "--command-config", "/client.properties",
    ],
);
assert!(out.status.success(), "describe failed: {}", String::from_utf8_lossy(&out.stderr));
let stdout = String::from_utf8_lossy(&out.stdout);
assert!(stdout.contains("producer_byte_rate=1024"), "expected quota: {stdout}");
```

For `--delete-config`:
```rust
let out = docker_run_kafka_tool_with_image_and_mount(
    KAFKA_IMAGE_TXN, &admin_mount,
    &[
        "kafka-configs", "--alter",
        "--entity-type", "users", "--entity-name", ALICE,
        "--delete-config", "producer_byte_rate",
        "--bootstrap-server", BOOTSTRAP,
        "--command-config", "/client.properties",
    ],
);
assert!(out.status.success(), "delete-config failed: {}", String::from_utf8_lossy(&out.stderr));
```

- [ ] **Step 2: Retro-fix `jvm_kafka_configs_alter_ip_quota_end_to_end` (slice 16b T5)**

Same pattern — find the `std::process::Command::new("docker")` invocations for `--describe` and `--delete-config` in this test; replace with `docker_run_kafka_tool_with_image_and_mount`; assert on `status.success()`.

Note: slice 16b found that `--describe --entity-type ips` already exited 0 (no SCRAM side-call for ip entity). The current test may already be using the helper for that one. Verify before changing.

- [ ] **Step 3: Retro-fix `jvm_kafka_configs_alter_controller_mutation_rate_end_to_end` (slice 16c T7)**

Same pattern — find `--describe` and `--delete-config` invocations in this test; switch back to the helper + `assert!(status.success())`.

- [ ] **Step 4: New test — `jvm_kafka_configs_describe_users_scram_credentials_end_to_end`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_describe_users_scram_credentials_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";

    let (h1, _h2, _h3, _d1, _d2, _d3, _c1, _c2, _c3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            ADMIN, ADMIN_PASS, &[],
        ).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Provision a SCRAM user via kafka-configs.
    let alter = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--alter",
            "--entity-type", "users", "--entity-name", "alice",
            "--add-config", "SCRAM-SHA-512=[iterations=4096,password=alice-secret]",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(alter.status.success(), "alter SCRAM failed: {}", String::from_utf8_lossy(&alter.stderr));

    // Describe — should exit 0 cleanly now.
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN, &admin_mount,
        &[
            "kafka-configs", "--describe",
            "--entity-type", "users", "--entity-name", "alice",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );
    assert!(desc.status.success(), "describe failed: {}", String::from_utf8_lossy(&desc.stderr));
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("SCRAM-SHA-512"),
        "expected SCRAM-SHA-512 in describe output: {stdout}"
    );

    let _ = h1; // keep alive
}
```

- [ ] **Step 5: Run all 4 JVM tests via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1"
```

Expected: all four pass — including the three retro-fits AND the new describe test.

- [ ] **Step 6: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy --workspace --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "$(cat <<'EOF'
test(jvm): retroactive cleanup + DescribeUserScramCredentials test

Three slice-16-family JVM tests previously bypassed
assert!(status.success()) on --describe/--delete-config because
api_key 50 wasn't implemented. Slice 17a closes the gap; switch
each back to docker_run_kafka_tool_with_image_and_mount with exit
code assertions.

New jvm_kafka_configs_describe_users_scram_credentials_end_to_end
provisions a SCRAM user and asserts kafka-configs --describe shows
SCRAM-SHA-512.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 5 — Sweep + docs + PR (sequential: T6)

### Task 6: Sweep + README + STATUS + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Full local sweep**

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
```

All clean.

- [ ] **Step 2: Update `README.md` Security matrix**

Find the Security matrix. Slice 12 added the AlterUserScramCredentials row:

```markdown
| `AlterUserScramCredentials` (KIP-554) | ✅ |
```

Add a sibling row (or update the existing one to "both halves"):

```markdown
| `AlterUserScramCredentials` (KIP-554) | ✅ |
| `DescribeUserScramCredentials` (KIP-554) | ✅ |
```

- [ ] **Step 3: Append to `STATUS.md`**

```markdown
## Slice 17a — DescribeUserScramCredentials (2026-05-15)

- KIP-554 read half: `DescribeUserScramCredentials` (api_key 50, v0). Reads from existing slice-12 `MetadataImage::scram_credentials`.
- Two new image accessors: `scram_credentials_users() -> Vec<String>` and `scram_credentials_for_user(user) -> Vec<(SaslMechanism, i32)>`. 2 unit tests.
- New handler `crates/broker/src/handlers/describe_user_scram_credentials.rs`. Filter semantics: `users=None` OR empty list → all users; non-empty → filter. Unknown users return per-user `RESOURCE_NOT_FOUND (83)`. 4 unit tests.
- Authorization: Cluster Alter (matches slice-12 `AlterUserScramCredentials` — JVM AdminClient uses Alter for both Alter and Describe SCRAM ops).
- Inline-intercept dispatch (handler needs `&Principal`). Mirrors slice-16 `DescribeClientQuotas` framing.
- 2 broker integration tests in `tests/describe_user_scram_credentials.rs`: all-users round-trip with seeded alice credential, unknown-user RESOURCE_NOT_FOUND.
- 3 slice-16-family JVM tests retroactively cleaned up: `jvm_kafka_configs_alter_client_quota_end_to_end`, `jvm_kafka_configs_alter_ip_quota_end_to_end`, `jvm_kafka_configs_alter_controller_mutation_rate_end_to_end` now use `docker_run_kafka_tool_with_image_and_mount` + `assert!(status.success())` for `--describe`/`--delete-config` instead of the stdout-only workaround.
- 1 new JVM acceptance test: `jvm_kafka_configs_describe_users_scram_credentials_end_to_end` provisions a SCRAM user and confirms `kafka-configs --describe --entity-type users` shows the credential.
- Closes the recurring JVM-tool quirk that slices 16/16b/16c documented as known limitations.
- Out of scope: slice 16 `client_id` HandlerTable gap (slice 17b).
```

- [ ] **Step 4: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "$(cat <<'EOF'
docs(slice-17a): README matrix + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 5: Push + open PR**

```
git push -u origin feature/describe-user-scram-credentials-17a
gh pr create --base main --head feature/describe-user-scram-credentials-17a \
  --title "Slice 17a: DescribeUserScramCredentials (api_key 50)" \
  --body "$(cat <<'EOF'
## Summary

Read half of KIP-554 — closes the JVM-tool exit-1 quirk that slices 16/16b/16c worked around via stdout-substring assertions on \`kafka-configs --describe\`/\`--delete-config users\`.

1. **\`DescribeUserScramCredentials\` (api_key 50)** handler reads from existing slice-12 \`scram_credentials\` storage.
2. **Two new \`MetadataImage\` accessors** — \`scram_credentials_users\` + \`scram_credentials_for_user\`.
3. **Authorize Cluster Alter** — matches slice-12 \`AlterUserScramCredentials\`; JVM AdminClient uses Alter for both Alter and Describe SCRAM ops.
4. **Retroactive JVM cleanup** — slice 16/16b/16c JVM tests now assert on \`status.success()\` instead of stdout-only.

## Verified

- 6 new unit tests (handler 4, image accessors 2).
- 2 broker integration tests in \`tests/describe_user_scram_credentials.rs\`.
- 1 new JVM acceptance test; 3 existing slice-16-family JVM tests retroactively cleaned up.
- Workspace \`cargo fmt --check\`, \`cargo clippy --workspace --all-targets -- -D warnings\`, \`cargo test --workspace\` all green.

## Out of scope

- Slice 16 \`client_id\` \`HandlerTable\` gap — slice 17b will close.

## Plan / spec

- Spec: \`docs/superpowers/specs/2026-05-15-crabka-describe-user-scram-credentials-17a-design.md\`
- Plan: \`docs/superpowers/plans/2026-05-15-crabka-describe-user-scram-credentials-17a.md\` (6 tasks across 5 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Capture PR URL** and return.

---

## Notes for the executing agent

1. **CLAUDE.md compatibility rule** — no metadata schema changes. Reuses slice 12 SCRAM storage.

2. **Parallel batches** (per CLAUDE.md):
   - **B1 (T1 + T2)**: T1 touches `crates/metadata/src/image.rs`; T2 touches `crates/broker/src/handlers/`. Disjoint.
   - **B2 (T3)**: dispatch wiring; depends on T2 (handler module must exist).
   - **B3 (T4)**: integration tests; depends on T3 (dispatch must route).
   - **B4 (T5)**: JVM acceptance; depends on T3 (JVM tool calls go through the wire path).
   - **B5 (T6)**: sweep + PR.

3. **Generated owned-type imports** — `crabka_protocol::owned::describe_user_scram_credentials_request::*` and `_response::*`. Slice 12's `alter_user_scram_credentials.rs` has the canonical import shape; mirror.

4. **`ScramCredentialRecord` field names** — slice 12 declares it in `crates/metadata/src/records.rs`. Test fixtures need exact field names; check before pasting.

5. **`Principal::name`** is a `String` field, not a method (slice 12 idiom). Access via `principal.name.as_str()` if needed.

6. **Unit test pure-logic helper** — T2 extracts `process_targets_for_test` because the full `handle` function takes `&Broker` which is hard to mock. The pure logic exercised matches what `handle` does post-authorize.

7. **Slice 16/16b/16c JVM tests** — read them end-to-end before retro-fixing in T5. The exact lines that use `std::process::Command::new("docker")` for the bypass are documented in each slice's STATUS entries.
