//! `AlterUserScramCredentials` handler (`api_key` 51, KIP-554).
//!
//! KIP-554 puts PBKDF2 on the *client* side: the wire request carries the
//! already-stretched PBKDF2 output as `salted_password`. The broker derives
//! `stored_key` / `server_key` from the supplied bytes without ever seeing the
//! user's plaintext password.
//!
//! Per-user validation (each upsertion is checked independently):
//!
//! - `iterations >= 4096` else `UNACCEPTABLE_CREDENTIAL` (93).
//! - `iterations <= 16384` else `UNACCEPTABLE_CREDENTIAL`.
//! - Unknown mechanism wire value → `UNSUPPORTED_SASL_MECHANISM` (33).
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On Deny,
//! every per-user result is `CLUSTER_AUTHORIZATION_FAILED` (31). The
//! authorizer's super-user bypass short-circuits inside `authorize` → ALLOW
//! when `super_users` is configured.
//!
//! Duplicate detection preserves Kafka's first per-user validation/resource
//! error. If the first alteration for a user has already recorded an error,
//! later alterations for that user are ignored and the original error remains.
//! `DUPLICATE_RESOURCE` (92) is returned only when the prior same-user
//! alteration was otherwise valid and pending in the request. An empty username
//! is always an `UNACCEPTABLE_CREDENTIAL` (93) validation error unless the
//! whole request is denied by authorization first.
//!
//! Deletion targets that are not present in the current metadata image get
//! `RESOURCE_NOT_FOUND` (91).
//!
//! On a successful submit the handler emits one `V1ScramCredential` or
//! `V1DeleteScramCredential` record per accepted row through
//! `controller.submit_change`. A single batched commit keeps the metadata
//! image consistent across multiple rows in the same request.

use std::collections::HashMap;

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
/// Highest PBKDF2 iteration count accepted by Kafka's SCRAM controller path.
const MAX_ITERATIONS: i32 = 16_384;
const DUPLICATE_ALTERATION_MESSAGE: &str =
    "A user credential cannot be altered twice in the same request";
const EMPTY_USERNAME_MESSAGE: &str = "Username must not be empty";

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

    if !authorized {
        return AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: distinct_requested_users(&req)
                .into_iter()
                .map(|user| err_result(user, codes::CLUSTER_AUTHORIZATION_FAILED, "not super-user"))
                .collect(),
            ..Default::default()
        };
    }

    // KIP-554/KIP-778: KRaft SCRAM requires metadata.version >= 3.5-IV2.
    if crate::features::require_feature(
        &image,
        crate::features::METADATA_VERSION,
        crabka_metadata::metadata_version::SCRAM_MIN_LEVEL,
    )
    .is_err()
    {
        let msg = "SCRAM is not enabled at the cluster's metadata.version.";
        return AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: distinct_requested_users(&req)
                .into_iter()
                .map(|user| err_result(user, codes::UNSUPPORTED_VERSION, msg))
                .collect(),
            ..Default::default()
        };
    }

    let AlterationPlan {
        mut user_results,
        records,
    } = plan_alterations(broker, req, authorized);

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

struct AlterationPlan {
    user_results: Vec<AlterUserScramCredentialsResult>,
    records: Vec<MetadataRecord>,
}

#[derive(Debug)]
struct AlterationError {
    code: i16,
    message: &'static str,
}

fn plan_alterations(
    broker: &Broker,
    req: AlterUserScramCredentialsRequest,
    authorized: bool,
) -> AlterationPlan {
    let mut user_order = Vec::new();
    let mut deletions = HashMap::new();
    let mut upsertions = HashMap::new();
    let mut errors = HashMap::new();

    if !authorized {
        for deletion in &req.deletions {
            remember_user(&mut user_order, &deletion.name);
        }
        for upsertion in &req.upsertions {
            remember_user(&mut user_order, &upsertion.name);
        }
        return AlterationPlan {
            user_results: user_order
                .into_iter()
                .map(|user| err_result(user, codes::CLUSTER_AUTHORIZATION_FAILED, "not super-user"))
                .collect(),
            records: Vec::new(),
        };
    }

    for deletion in req.deletions {
        remember_user(&mut user_order, &deletion.name);
        stage_deletion(broker, deletion, authorized, &mut deletions, &mut errors);
    }

    for upsertion in req.upsertions {
        remember_user(&mut user_order, &upsertion.name);
        stage_upsertion(
            upsertion,
            authorized,
            &mut deletions,
            &mut upsertions,
            &mut errors,
        );
    }

    let mut user_results = Vec::with_capacity(user_order.len());
    let mut records = Vec::new();
    for user in user_order {
        if let Some(error) = errors.remove(&user) {
            user_results.push(err_result(user, error.code, error.message));
            continue;
        }

        if let Some((deletion, mechanism)) = deletions.remove(&user) {
            records.push(delete_record(&deletion, mechanism));
            user_results.push(ok_result(deletion.name));
            continue;
        }

        if let Some((upsertion, mechanism)) = upsertions.remove(&user) {
            records.push(upsertion_record(&upsertion, mechanism));
            user_results.push(ok_result(upsertion.name));
        }
    }

    AlterationPlan {
        user_results,
        records,
    }
}

fn remember_user(user_order: &mut Vec<String>, user: &str) {
    if user_order.iter().any(|seen| seen == user) {
        return;
    }
    user_order.push(user.to_string());
}

fn distinct_requested_users(req: &AlterUserScramCredentialsRequest) -> Vec<String> {
    let mut users = Vec::new();
    for deletion in &req.deletions {
        remember_user(&mut users, &deletion.name);
    }
    for upsertion in &req.upsertions {
        remember_user(&mut users, &upsertion.name);
    }
    users
}

fn stage_deletion(
    broker: &Broker,
    deletion: ScramCredentialDeletion,
    authorized: bool,
    deletions: &mut HashMap<String, (ScramCredentialDeletion, SaslMechanism)>,
    errors: &mut HashMap<String, AlterationError>,
) {
    // Kafka reports the first per-user validation/resource error. Once an
    // error exists, later same-user rows must not replace it with DUPLICATE.
    if errors.contains_key(&deletion.name) {
        return;
    }

    // A pending prior deletion means the previous same-user alteration was
    // accepted so far; the second alteration converts that pending success
    // into Kafka's DUPLICATE_RESOURCE result.
    if deletions.remove(&deletion.name).is_some() {
        errors.insert(deletion.name, duplicate_alteration_error());
        return;
    }

    match validate_deletion(broker, &deletion, authorized) {
        Ok(mechanism) => {
            deletions.insert(deletion.name.clone(), (deletion, mechanism));
        }
        Err(error) => {
            errors.insert(deletion.name, error);
        }
    }
}

fn stage_upsertion(
    upsertion: ScramCredentialUpsertion,
    authorized: bool,
    deletions: &mut HashMap<String, (ScramCredentialDeletion, SaslMechanism)>,
    upsertions: &mut HashMap<String, (ScramCredentialUpsertion, SaslMechanism)>,
    errors: &mut HashMap<String, AlterationError>,
) {
    // Kafka reports the first per-user validation/resource error. Once an
    // error exists, later same-user rows must not replace it with DUPLICATE.
    if errors.contains_key(&upsertion.name) {
        return;
    }

    // A pending prior deletion/upsertion means the previous same-user
    // alteration was accepted so far; the second alteration converts that
    // pending success into Kafka's DUPLICATE_RESOURCE result.
    if deletions.remove(&upsertion.name).is_some() || upsertions.remove(&upsertion.name).is_some() {
        errors.insert(upsertion.name, duplicate_alteration_error());
        return;
    }

    match validate_upsertion(&upsertion, authorized) {
        Ok(mechanism) => {
            upsertions.insert(upsertion.name.clone(), (upsertion, mechanism));
        }
        Err(error) => {
            errors.insert(upsertion.name, error);
        }
    }
}

fn duplicate_alteration_error() -> AlterationError {
    AlterationError {
        code: codes::DUPLICATE_RESOURCE,
        message: DUPLICATE_ALTERATION_MESSAGE,
    }
}

/// Validate and optionally accept a single deletion. Returns the per-user
/// result row to push into the response; pushes the metadata record to
/// `records` on accept.
fn process_deletion(
    broker: &Broker,
    d: ScramCredentialDeletion,
    authorized: bool,
    records: &mut Vec<MetadataRecord>,
) -> AlterUserScramCredentialsResult {
    let mech = match validate_deletion(broker, &d, authorized) {
        Ok(mech) => mech,
        Err(error) => return err_result(d.name, error.code, error.message),
    };
    records.push(delete_record(&d, mech));
    ok_result(d.name)
}

/// Validate and optionally accept a single upsertion. Returns the per-user
/// result row to push into the response; pushes the metadata record to
/// `records` on accept.
fn process_upsertion(
    u: ScramCredentialUpsertion,
    authorized: bool,
    records: &mut Vec<MetadataRecord>,
) -> AlterUserScramCredentialsResult {
    let mech = match validate_upsertion(&u, authorized) {
        Ok(mech) => mech,
        Err(error) => return err_result(u.name, error.code, error.message),
    };
    records.push(upsertion_record(&u, mech));
    ok_result(u.name)
}

fn validate_deletion(
    broker: &Broker,
    deletion: &ScramCredentialDeletion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    if !authorized {
        return Err(AlterationError {
            code: codes::CLUSTER_AUTHORIZATION_FAILED,
            message: "not super-user",
        });
    }
    if deletion.name.is_empty() {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: EMPTY_USERNAME_MESSAGE,
        });
    }
    let Some(mech) = wire_to_mech(deletion.mechanism) else {
        return Err(AlterationError {
            code: codes::UNSUPPORTED_SASL_MECHANISM,
            message: "unknown mechanism",
        });
    };
    if broker
        .controller
        .current_image()
        .scram_credential(&deletion.name, mech)
        .is_none()
    {
        return Err(AlterationError {
            code: codes::RESOURCE_NOT_FOUND,
            message: "credential not found",
        });
    }
    Ok(mech)
}

fn validate_upsertion(
    upsertion: &ScramCredentialUpsertion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    if !authorized {
        return Err(AlterationError {
            code: codes::CLUSTER_AUTHORIZATION_FAILED,
            message: "not super-user",
        });
    }
    if upsertion.name.is_empty() {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: EMPTY_USERNAME_MESSAGE,
        });
    }
    let Some(mech) = wire_to_mech(upsertion.mechanism) else {
        return Err(AlterationError {
            code: codes::UNSUPPORTED_SASL_MECHANISM,
            message: "unknown mechanism",
        });
    };
    if upsertion.iterations < MIN_ITERATIONS {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "iterations < 4096",
        });
    }
    if upsertion.iterations > MAX_ITERATIONS {
        return Err(AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "iterations > 16384",
        });
    }
    Ok(mech)
}

fn delete_record(deletion: &ScramCredentialDeletion, mechanism: SaslMechanism) -> MetadataRecord {
    MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
        user: deletion.name.clone(),
        mechanism,
    })
}

fn upsertion_record(
    upsertion: &ScramCredentialUpsertion,
    mechanism: SaslMechanism,
) -> MetadataRecord {
    // Per KIP-554 the wire `salted_password` is the PBKDF2 output; recompute
    // `stored_key` and `server_key` from the supplied bytes for storage in the
    // metadata image.
    let (stored_key, server_key) =
        crabka_security::derive_keys_from_salted(mechanism, &upsertion.salted_password);
    MetadataRecord::V1ScramCredential(ScramCredentialRecord {
        user: upsertion.name.clone(),
        mechanism,
        salt: upsertion.salt.to_vec(),
        stored_key,
        server_key,
        iterations: u32::try_from(upsertion.iterations)
            .expect("validated SCRAM iterations fit u32"),
    })
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

    const KAFKA_DUPLICATE_RESOURCE: i16 = 92;
    const KAFKA_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
    const KAFKA_UNACCEPTABLE_CREDENTIAL: i16 = 93;
    const KAFKA_MAX_SCRAM_ITERATIONS: i32 = 16_384;

    fn valid_upsertion(name: &str) -> ScramCredentialUpsertion {
        valid_upsertion_for_mechanism(name, 1, SaslMechanism::ScramSha256)
    }

    fn valid_upsertion_for_mechanism(
        name: &str,
        wire_mechanism: i8,
        mechanism: SaslMechanism,
    ) -> ScramCredentialUpsertion {
        ScramCredentialUpsertion {
            name: name.into(),
            mechanism: wire_mechanism,
            iterations: MIN_ITERATIONS,
            salt: Bytes::from_static(b"salt"),
            salted_password: Bytes::from(vec![7; crabka_security::scram_hash_len(mechanism)]),
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
    fn process_upsertion_rejects_unknown_mechanism_with_unsupported_sasl_mechanism() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.mechanism = 99;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNSUPPORTED_SASL_MECHANISM,
                Some("unknown mechanism"),
            )
        );
        assert!(records.is_empty());
    }

    #[test]
    fn process_upsertion_rejects_iterations_above_kafka_maximum() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.iterations = KAFKA_MAX_SCRAM_ITERATIONS + 1;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNACCEPTABLE_CREDENTIAL,
                Some("iterations > 16384"),
            )
        );
        assert!(records.is_empty());
    }

    #[test]
    fn process_upsertion_allows_kafka_maximum_iterations() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.iterations = KAFKA_MAX_SCRAM_ITERATIONS;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(r == expected_result("alice", 0, None));
        assert!(records.len() == 1);
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
        let mut records = Vec::new();

        let rejections = [(
            {
                let mut u = valid_upsertion("too-few");
                u.iterations = MIN_ITERATIONS - 1;
                u
            },
            "iterations < 4096",
        )];
        for (upsertion, msg) in rejections {
            let user = upsertion.name.clone();
            let r = process_upsertion(upsertion, true, &mut records);
            assert!(
                r == expected_result(&user, codes::UNACCEPTABLE_CREDENTIAL, Some(msg)),
                "case: {user}"
            );
            assert!(records.is_empty(), "case: {user}");
        }

        let r = process_upsertion(valid_upsertion("alice"), true, &mut records);
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
    fn process_upsertion_accepts_empty_salt() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("empty-salt");
        upsertion.salt = Bytes::new();

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(r == expected_result("empty-salt", 0, None));
        assert!(records.len() == 1);
        let MetadataRecord::V1ScramCredential(record) = &records[0] else {
            panic!("accepted upsertion must persist a SCRAM credential record");
        };
        assert!(record.salt.is_empty());
    }

    #[test]
    fn process_upsertion_accepts_non_hash_length_salted_password() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("odd-bytes");
        let salted_password = Bytes::from_static(b"not-a-sha-sized-secret");
        upsertion.salted_password = salted_password.clone();

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(r == expected_result("odd-bytes", 0, None));
        assert!(records.len() == 1);
        let MetadataRecord::V1ScramCredential(record) = &records[0] else {
            panic!("accepted upsertion must persist a SCRAM credential record");
        };
        let (stored_key, server_key) =
            crabka_security::derive_keys_from_salted(SaslMechanism::ScramSha256, &salted_password);
        assert!(record.stored_key == stored_key);
        assert!(record.server_key == server_key);
    }

    #[test]
    fn process_upsertion_rejects_unauthorized_users() {
        let mut records = Vec::new();

        let r = process_upsertion(valid_upsertion("bob"), false, &mut records);
        let expected = expected_result(
            "bob",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("not super-user"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn process_deletion_rejects_missing_credentials() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut records = Vec::new();

        let r = process_deletion(&broker, deletion("alice"), true, &mut records);
        assert!(
            r.error_code == 91,
            "missing SCRAM deletion target must use Kafka RESOURCE_NOT_FOUND (91), got {}",
            r.error_code
        );
        let expected = expected_result(
            "alice",
            codes::RESOURCE_NOT_FOUND,
            Some("credential not found"),
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
        let mut records = Vec::new();

        let r = process_deletion(&broker, deletion("alice"), false, &mut records);
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
    async fn process_deletion_rejects_unknown_mechanism_with_unsupported_sasl_mechanism() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut records = Vec::new();
        let mut deletion = deletion("alice");
        deletion.mechanism = 99;

        let r = process_deletion(&broker, deletion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNSUPPORTED_SASL_MECHANISM,
                Some("unknown mechanism"),
            )
        );
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_across_upsertion_mechanisms_returns_one_error_row() {
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
            upsertions: vec![
                valid_upsertion_for_mechanism("alice", 1, SaslMechanism::ScramSha256),
                valid_upsertion_for_mechanism("alice", 2, SaslMechanism::ScramSha512),
            ],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "alice",
                KAFKA_DUPLICATE_RESOURCE,
                Some("A user credential cannot be altered twice in the same request"),
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
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_between_deletion_and_upsertion_returns_one_error_row() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1ScramCredential(
                ScramCredentialRecord {
                    user: "alice".into(),
                    mechanism: SaslMechanism::ScramSha512,
                    salt: b"salt".to_vec(),
                    stored_key: vec![1; 64],
                    server_key: vec![2; 64],
                    iterations: u32::try_from(MIN_ITERATIONS).expect("min fits"),
                },
            )])
            .await
            .expect("seed alice SCRAM credential");
        broker_handle
            .wait_for_image(|image| {
                image
                    .scram_credential("alice", SaslMechanism::ScramSha512)
                    .is_some()
            })
            .await;
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterUserScramCredentialsRequest {
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: 2,
                ..Default::default()
            }],
            upsertions: vec![valid_upsertion_for_mechanism(
                "alice",
                1,
                SaslMechanism::ScramSha256,
            )],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "alice",
                KAFKA_DUPLICATE_RESOURCE,
                Some("A user credential cannot be altered twice in the same request"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        let image = broker.controller.current_image();
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_some()
        );
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha256)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_after_missing_deletion_preserves_resource_not_found() {
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
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: 2,
                ..Default::default()
            }],
            upsertions: vec![valid_upsertion_for_mechanism(
                "alice",
                1,
                SaslMechanism::ScramSha256,
            )],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "alice",
                codes::RESOURCE_NOT_FOUND,
                Some("credential not found"),
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
        assert!(
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denies_invalid_rows_before_scram_validation() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let mut invalid_upsertion = valid_upsertion("bob");
        invalid_upsertion.iterations = MIN_ITERATIONS - 1;
        let req = AlterUserScramCredentialsRequest {
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: 99,
                ..Default::default()
            }],
            upsertions: vec![invalid_upsertion, valid_upsertion("bob")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![
                expected_result(
                    "alice",
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("not super-user"),
                ),
                expected_result(
                    "bob",
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("not super-user"),
                ),
            ],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_empty_deletion_username_is_unacceptable_credential() {
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
            deletions: vec![deletion("")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "",
                KAFKA_UNACCEPTABLE_CREDENTIAL,
                Some("Username must not be empty"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        assert!(
            broker
                .controller
                .current_image()
                .scram_credential("", SaslMechanism::ScramSha256)
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_empty_upsertion_username_is_unacceptable_credential() {
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
            upsertions: vec![valid_upsertion("")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![expected_result(
                "",
                KAFKA_UNACCEPTABLE_CREDENTIAL,
                Some("Username must not be empty"),
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        assert!(
            broker
                .controller
                .current_image()
                .scram_credential("", SaslMechanism::ScramSha256)
                .is_none()
        );
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

    #[tokio::test]
    async fn handle_low_metadata_version_denied_request_reports_authorization_per_distinct_user() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
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
        let mut invalid_upsertion = valid_upsertion("bob");
        invalid_upsertion.iterations = MIN_ITERATIONS - 1;
        let req = AlterUserScramCredentialsRequest {
            deletions: vec![ScramCredentialDeletion {
                name: "alice".into(),
                mechanism: 99,
                ..Default::default()
            }],
            upsertions: vec![invalid_upsertion, valid_upsertion("bob")],
            ..Default::default()
        };

        let resp = handle(&broker, req, &ctx).await;

        let expected = AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: vec![
                expected_result(
                    "alice",
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("not super-user"),
                ),
                expected_result(
                    "bob",
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("not super-user"),
                ),
            ],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_low_metadata_version_authorized_request_deduplicates_unsupported_users() {
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
            upsertions: vec![
                valid_upsertion("bob"),
                valid_upsertion("bob"),
                valid_upsertion("alice"),
            ],
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
