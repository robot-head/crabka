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
use crabka_protocol::owned::alter_user_scram_credentials_request::{
    AlterUserScramCredentialsRequest, ScramCredentialDeletion, ScramCredentialUpsertion,
};
use crabka_protocol::owned::alter_user_scram_credentials_response::{
    AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
};
use crabka_security::SaslMechanism;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;

const MIN_ITERATIONS: i32 = 4096;

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
            resource_name: "kafka-cluster",
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

    let mut seen: HashSet<(String, i8)> = HashSet::new();
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
        for r in user_results.iter_mut().filter(|r| r.error_code == 0) {
            r.error_code = codes::UNKNOWN_SERVER_ERROR;
            r.error_message = Some(msg.clone());
        }
    }

    AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
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
    seen: &mut HashSet<(String, i8)>,
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
    seen: &mut HashSet<(String, i8)>,
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
fn wire_to_mech(wire: i8) -> Option<SaslMechanism> {
    match wire {
        1 => Some(SaslMechanism::ScramSha256),
        2 => Some(SaslMechanism::ScramSha512),
        _ => None,
    }
}

fn ok_result(name: String) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: 0,
        error_message: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn wire_to_mech_maps_both_scram_variants() {
        assert!(wire_to_mech(1) == Some(SaslMechanism::ScramSha256));
        assert!(wire_to_mech(2) == Some(SaslMechanism::ScramSha512));
        assert!(wire_to_mech(0).is_none());
        assert!(wire_to_mech(99).is_none());
    }

    #[test]
    fn err_result_carries_code_and_message() {
        let r = err_result("alice".into(), codes::UNACCEPTABLE_CREDENTIAL, "bad");
        assert!(r.user == "alice");
        assert!(r.error_code == codes::UNACCEPTABLE_CREDENTIAL);
        assert!(r.error_message.as_deref() == Some("bad"));
    }

    #[test]
    fn ok_result_has_zero_error_code() {
        let r = ok_result("alice".into());
        assert!(r.error_code == 0);
        assert!(r.error_message.is_none());
    }

    #[test]
    fn scram_gate_permits_unknown_and_at_or_above_level() {
        use crabka_metadata::metadata_version::SCRAM_MIN_LEVEL;
        use crabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};

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

        assert!(!gate(None));
        assert!(gate(Some(10)));
        assert!(!gate(Some(11)));
    }
}
