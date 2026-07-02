//! `AlterUserScramCredentials` handler (`api_key` 51, KIP-554).
//!
//! KIP-554 puts PBKDF2 on the *client* side: the wire request carries the
//! already-stretched PBKDF2 output as `salted_password` (32 bytes for
//! SHA-256, 64 bytes for SHA-512). The broker derives `stored_key` /
//! `server_key` from that without ever seeing the user's plaintext
//! password.
//!
//! Per-user validation (each upsertion is checked independently):
//!
//! - `iterations >= 4096` else `UNACCEPTABLE_CREDENTIAL` (78).
//! - `salt` non-empty else `UNACCEPTABLE_CREDENTIAL`.
//! - `salted_password.len()` matches the chosen mechanism's hash length
//!   (32 for SHA-256, 64 for SHA-512) else `UNACCEPTABLE_CREDENTIAL`.
//! - Unknown mechanism wire value → `UNACCEPTABLE_CREDENTIAL`.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On Deny,
//! every per-user result is `CLUSTER_AUTHORIZATION_FAILED` (31). The
//! authorizer's super-user bypass short-circuits inside `authorize` → ALLOW
//! when `super_users` is configured.
//!
//! Duplicate detection: the same `(user, mechanism)` appearing twice in one
//! request (either two upsertions, two deletions, or one of each) gets
//! `DUPLICATE_RESOURCE` (84) on the second occurrence.
//!
//! Deletion targets that are not present in the current metadata image get
//! `RESOURCE_NOT_FOUND` (66).
//!
//! On a successful submit the handler emits one `V1ScramCredential` or
//! `V1DeleteScramCredential` record per accepted row through
//! `controller.submit_change`. A single batched commit keeps the metadata
//! image consistent across multiple rows in the same request.

use std::collections::HashSet;

use crabka_metadata::{
    AclOperation, DeleteScramCredentialRecord, MetadataRecord, ScramCredentialRecord,
};
use crabka_protocol::owned::{
    alter_user_scram_credentials_request::{
        AlterUserScramCredentialsRequest, ScramCredentialDeletion, ScramCredentialUpsertion,
    },
    alter_user_scram_credentials_response::{
        AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
    },
};
use crabka_security::SaslMechanism;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// Lowest PBKDF2 iteration count accepted for a SCRAM credential (KIP-554);
/// upsertions below this get `UNACCEPTABLE_CREDENTIAL`.
const MIN_ITERATIONS: i32 = 4096;

/// KIP-554 wire byte identifying a SCRAM mechanism (see [`wire_to_mech`]).
type MechanismWireByte = i8;

/// Run the `AlterUserScramCredentials` request and return the typed
/// response. The caller (dispatch.rs) is responsible for wire-encoding
/// the response and prepending the response header.
#[tracing::instrument(
    name = "handle_alter_user_scram_credentials",
    level = "info",
    skip_all,
    fields(api = "AlterUserScramCredentials")
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: AlterUserScramCredentialsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
) -> AlterUserScramCredentialsResponse {
    // ── ACL preamble ────────────────────────────────────────
    // Whole-request Cluster Alter gate. On Deny, every per-user row
    // reports CLUSTER_AUTHORIZATION_FAILED. The authorizer's super-user
    // bypass short-circuits inside `authorize` → ALLOW when `super_users`
    // is configured.
    let image = broker.controller.current_image();
    let authorized = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Alter,
        },
    ) == AuthorizationResult::Allow;

    // KIP-554/KIP-778: KRaft SCRAM requires metadata.version >= 3.5-IV2.
    if crate::features::require_feature(
        &image,
        crate::features::METADATA_VERSION,
        crabka_metadata::metadata_version::SCRAM_MIN_LEVEL,
    )
    .is_err()
    {
        let msg = "SCRAM is not enabled at the cluster's metadata.version.";
        let mut results = Vec::new();
        for d in &req.deletions {
            results.push(err_result(d.name.clone(), codes::UNSUPPORTED_VERSION, msg));
        }
        for u in &req.upsertions {
            results.push(err_result(u.name.clone(), codes::UNSUPPORTED_VERSION, msg));
        }
        return AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        };
    }

    let mut seen: HashSet<(String, MechanismWireByte)> = HashSet::new();
    let mut user_results: Vec<AlterUserScramCredentialsResult> = Vec::new();
    let mut records: Vec<MetadataRecord> = Vec::new();

    for d in req.deletions {
        user_results.push(process_deletion(
            broker,
            d,
            authorized,
            &mut seen,
            &mut records,
        ));
    }

    for u in req.upsertions {
        user_results.push(process_upsertion(u, authorized, &mut seen, &mut records));
    }

    // Submit accepted records as a single batch. A submit failure converts
    // every pending "ok" row to a generic error (per-row errors already in
    // `user_results` keep their existing codes).
    if !records.is_empty()
        && let Err(e) = broker.controller.submit_change(records).await
    {
        tracing::warn!(error = %e, "AlterUserScramCredentials: submit_change failed");
        let msg = format!("submit failed: {e}");
        apply_submit_error(&mut user_results, &msg);
    }

    AlterUserScramCredentialsResponse {
        results: user_results,
        ..Default::default()
    }
}

/// Validate and optionally accept a single deletion. Returns the per-user
/// result row to push into the response; pushes the metadata record to
/// `records` on accept.
fn process_deletion(
    broker: &Broker,
    d: ScramCredentialDeletion,
    authorized: bool,
    seen: &mut HashSet<(String, MechanismWireByte)>,
    records: &mut Vec<MetadataRecord>,
) -> AlterUserScramCredentialsResult {
    let key = (d.name.clone(), d.mechanism);
    if !seen.insert(key) {
        return err_result(d.name, codes::DUPLICATE_RESOURCE, "duplicate resource");
    }
    let Some(mech) = wire_to_mech(d.mechanism) else {
        return err_result(d.name, codes::UNACCEPTABLE_CREDENTIAL, "unknown mechanism");
    };
    if !authorized {
        return err_result(
            d.name,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "not super-user",
        );
    }
    if broker
        .controller
        .current_image()
        .scram_credential(&d.name, mech)
        .is_none()
    {
        return err_result(d.name, codes::RESOURCE_NOT_FOUND, "credential not found");
    }
    records.push(MetadataRecord::V1DeleteScramCredential(
        DeleteScramCredentialRecord {
            user: d.name.clone(),
            mechanism: mech,
        },
    ));
    ok_result(d.name)
}

/// Validate and optionally accept a single upsertion. Returns the per-user
/// result row to push into the response; pushes the metadata record to
/// `records` on accept.
fn process_upsertion(
    u: ScramCredentialUpsertion,
    authorized: bool,
    seen: &mut HashSet<(String, MechanismWireByte)>,
    records: &mut Vec<MetadataRecord>,
) -> AlterUserScramCredentialsResult {
    let key = (u.name.clone(), u.mechanism);
    if !seen.insert(key) {
        return err_result(u.name, codes::DUPLICATE_RESOURCE, "duplicate resource");
    }
    let Some(mech) = wire_to_mech(u.mechanism) else {
        return err_result(u.name, codes::UNACCEPTABLE_CREDENTIAL, "unknown mechanism");
    };
    if u.iterations < MIN_ITERATIONS {
        return err_result(u.name, codes::UNACCEPTABLE_CREDENTIAL, "iterations < 4096");
    }
    if u.salt.is_empty() {
        return err_result(u.name, codes::UNACCEPTABLE_CREDENTIAL, "empty salt");
    }
    let expected_salted_len = crabka_security::scram_hash_len(mech);
    if u.salted_password.len() != expected_salted_len {
        return err_result(
            u.name,
            codes::UNACCEPTABLE_CREDENTIAL,
            "wrong salted_password length",
        );
    }
    if !authorized {
        return err_result(
            u.name,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "not super-user",
        );
    }
    // Per KIP-554 the wire `salted_password` is the PBKDF2 output (32
    // bytes for SHA-256, 64 for SHA-512); recompute `stored_key` and
    // `server_key` from it for storage in the metadata image.
    let (stored_key, server_key) =
        crabka_security::derive_keys_from_salted(mech, &u.salted_password);
    records.push(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
        user: u.name.clone(),
        mechanism: mech,
        salt: u.salt.to_vec(),
        stored_key,
        server_key,
        iterations: u.iterations.try_into().unwrap_or(u32::MAX),
    }));
    ok_result(u.name)
}

/// Map the KIP-554 wire mechanism byte to a [`SaslMechanism`].
///
/// Per KIP-554:
/// - `0` — unknown (reserved)
/// - `1` — SCRAM-SHA-256
/// - `2` — SCRAM-SHA-512
fn wire_to_mech(wire: MechanismWireByte) -> Option<SaslMechanism> {
    match wire {
        1 => Some(SaslMechanism::ScramSha256),
        2 => Some(SaslMechanism::ScramSha512),
        _ => None,
    }
}

fn ok_result(name: String) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        ..Default::default()
    }
}

fn err_result(name: String, code: i16, msg: &str) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: code,
        error_message: Some(msg.to_string()),
        ..Default::default()
    }
}

fn apply_submit_error(results: &mut [AlterUserScramCredentialsResult], msg: &str) {
    for r in results.iter_mut().filter(|r| r.error_code == 0) {
        r.error_code = codes::UNKNOWN_SERVER_ERROR;
        r.error_message = Some(msg.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use assert2::assert;
    use bytes::Bytes;
    use crabka_metadata::FeatureLevelRecord;
    use crabka_protocol::UnknownTaggedFields;
    use crabka_security::{AuthMethod, Principal};

    use crate::{authorizer::Authorizer, test_support::DenyAll};

    fn valid_upsertion(name: &str) -> ScramCredentialUpsertion {
        ScramCredentialUpsertion {
            name: name.into(),
            mechanism: 1,
            iterations: MIN_ITERATIONS,
            salt: Bytes::from_static(b"salt"),
            salted_password: Bytes::from(vec![
                7;
                crabka_security::scram_hash_len(
                    SaslMechanism::ScramSha256,
                )
            ]),
            ..Default::default()
        }
    }

    fn deletion(name: &str) -> ScramCredentialDeletion {
        ScramCredentialDeletion {
            name: name.into(),
            mechanism: 1,
            ..Default::default()
        }
    }

    /// A fully-pinned per-user result row as the handler renders it.
    fn expected_result(
        user: &str,
        error_code: i16,
        error_message: Option<&str>,
    ) -> AlterUserScramCredentialsResult {
        AlterUserScramCredentialsResult {
            user: user.into(),
            error_code,
            error_message: error_message.map(Into::into),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "admin-client")
    }

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = authorizer;
        })
        .await
    }

    async fn wait_for_leader(broker: &Broker) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|n| n == broker.config.node_id)
            {
                return;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    use super::*;

    #[test]
    fn wire_to_mech_maps_both_scram_variants() {
        let cases = [
            (1, Some(SaslMechanism::ScramSha256)),
            (2, Some(SaslMechanism::ScramSha512)),
            (0, None),
            (99, None),
        ];
        for (wire, expected) in cases {
            assert!(wire_to_mech(wire) == expected, "wire {wire}");
        }
    }

    #[test]
    fn err_result_carries_code_and_message() {
        let r = err_result("alice".into(), codes::UNACCEPTABLE_CREDENTIAL, "bad");
        assert!(r == expected_result("alice", codes::UNACCEPTABLE_CREDENTIAL, Some("bad")));
    }

    #[test]
    fn ok_result_has_zero_error_code() {
        let r = ok_result("alice".into());
        assert!(r == expected_result("alice", 0, None));
    }

    #[test]
    fn submit_error_rewrites_only_success_rows() {
        let mut results = vec![
            ok_result("alice".into()),
            err_result(
                "bob".into(),
                codes::DUPLICATE_RESOURCE,
                "duplicate resource",
            ),
        ];

        apply_submit_error(&mut results, "submit failed: not controller");

        let expected = vec![
            expected_result(
                "alice",
                codes::UNKNOWN_SERVER_ERROR,
                Some("submit failed: not controller"),
            ),
            expected_result("bob", codes::DUPLICATE_RESOURCE, Some("duplicate resource")),
        ];
        assert!(results == expected);
    }

    #[test]
    fn scram_gate_permits_unknown_and_at_or_above_level() {
        use crabka_metadata::{
            FeatureLevelRecord, MetadataImage, MetadataRecord, metadata_version::SCRAM_MIN_LEVEL,
        };

        let gate = |level: Option<i16>| {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = level {
                image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: crate::features::METADATA_VERSION.to_string(),
                    level,
                }));
            }
            crate::features::require_feature(
                &image,
                crate::features::METADATA_VERSION,
                SCRAM_MIN_LEVEL,
            )
            .is_err()
        };

        let cases = [
            // No finalized metadata.version — gate permits.
            (None, false),
            // Below SCRAM_MIN_LEVEL — gate rejects.
            (Some(10), true),
            // At SCRAM_MIN_LEVEL — gate permits.
            (Some(11), false),
        ];
        for (level, want_err) in cases {
            assert!(gate(level) == want_err, "level {level:?}");
        }
    }

    #[test]
    fn process_upsertion_validates_boundaries_and_records_success() {
        let mut seen = HashSet::new();
        let mut records = Vec::new();

        let rejections = [
            (
                {
                    let mut u = valid_upsertion("too-few");
                    u.iterations = MIN_ITERATIONS - 1;
                    u
                },
                "iterations < 4096",
            ),
            (
                {
                    let mut u = valid_upsertion("empty-salt");
                    u.salt = Bytes::new();
                    u
                },
                "empty salt",
            ),
            (
                {
                    let mut u = valid_upsertion("wrong-len");
                    u.salted_password = Bytes::from(vec![7; 31]);
                    u
                },
                "wrong salted_password length",
            ),
        ];
        for (upsertion, msg) in rejections {
            let user = upsertion.name.clone();
            let r = process_upsertion(upsertion, true, &mut seen, &mut records);
            assert!(
                r == expected_result(&user, codes::UNACCEPTABLE_CREDENTIAL, Some(msg)),
                "case: {user}"
            );
            assert!(records.is_empty(), "case: {user}");
        }

        let r = process_upsertion(valid_upsertion("alice"), true, &mut seen, &mut records);
        assert!(r == expected_result("alice", 0, None));
        let (stored_key, server_key) = crabka_security::derive_keys_from_salted(
            SaslMechanism::ScramSha256,
            &valid_upsertion("alice").salted_password,
        );
        let expected_records = vec![MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha256,
            salt: b"salt".to_vec(),
            stored_key,
            server_key,
            iterations: u32::try_from(MIN_ITERATIONS).expect("min fits"),
        })];
        assert!(records == expected_records);
    }

    #[test]
    fn process_upsertion_rejects_duplicates_and_unauthorized_users() {
        let mut seen = HashSet::new();
        let mut records = Vec::new();

        let r = process_upsertion(valid_upsertion("alice"), true, &mut seen, &mut records);
        assert!(r.error_code == 0);
        let r = process_upsertion(valid_upsertion("alice"), true, &mut seen, &mut records);
        let expected = expected_result(
            "alice",
            codes::DUPLICATE_RESOURCE,
            Some("duplicate resource"),
        );
        assert!(r == expected);
        assert!(records.len() == 1);

        let mut seen = HashSet::new();
        let mut records = Vec::new();
        let r = process_upsertion(valid_upsertion("bob"), false, &mut seen, &mut records);
        let expected = expected_result(
            "bob",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("not super-user"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn process_deletion_rejects_duplicates_and_missing_credentials() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut seen = HashSet::new();
        let mut records = Vec::new();

        let r = process_deletion(&broker, deletion("alice"), true, &mut seen, &mut records);
        let expected = expected_result(
            "alice",
            codes::RESOURCE_NOT_FOUND,
            Some("credential not found"),
        );
        assert!(r == expected);
        assert!(records.is_empty());

        let r = process_deletion(&broker, deletion("alice"), true, &mut seen, &mut records);
        let expected = expected_result(
            "alice",
            codes::DUPLICATE_RESOURCE,
            Some("duplicate resource"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn process_deletion_rejects_unauthorized_users() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut seen = HashSet::new();
        let mut records = Vec::new();

        let r = process_deletion(&broker, deletion("alice"), false, &mut seen, &mut records);
        let expected = expected_result(
            "alice",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("not super-user"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_authorizes_and_persists_valid_upsertion() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterUserScramCredentialsRequest {
            upsertions: vec![valid_upsertion("alice")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result("alice", 0, None)],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        let image = broker.controller.current_image();
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha256)
                .is_some()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denies_valid_upsertion_without_cluster_alter() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterUserScramCredentialsRequest {
            upsertions: vec![valid_upsertion("alice")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "alice",
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("not super-user"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        let image = broker.controller.current_image();
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha256)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unsupported_metadata_version_reports_every_requested_user() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::METADATA_VERSION.to_string(),
                level: crabka_metadata::metadata_version::SCRAM_MIN_LEVEL - 1,
            })])
            .await
            .expect("seed low metadata.version");
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterUserScramCredentialsRequest {
            deletions: vec![deletion("alice")],
            upsertions: vec![valid_upsertion("bob")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let msg = "SCRAM is not enabled at the cluster's metadata.version.";
        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![
                expected_result("alice", codes::UNSUPPORTED_VERSION, Some(msg)),
                expected_result("bob", codes::UNSUPPORTED_VERSION, Some(msg)),
            ],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
