//! `KafkaUser` admin RPCs.
//!
//! Five admin operations the `KafkaUser` reconciler drives:
//! `AlterUserScramCredentials` (upsert + delete in a single call),
//! `CreateAcls`, `DeleteAcls`, `DescribeAcls`.
//!
//! Wire `i8` discriminants are kept private to this module; callers use
//! the typed Rust enums below. `crates/client-admin` depends on
//! `crabka-metadata` for shared image types (`DelegationToken`)
//! but stays free of `crabka-broker` so the
//! crate remains usable from out-of-process clients — the local enum
//! copies are unit-tested for wire round-trip.

use bytes::Bytes;
use crabka_protocol::owned::{
    alter_user_scram_credentials_request::{
        AlterUserScramCredentialsRequest, ScramCredentialDeletion, ScramCredentialUpsertion,
    },
    create_acls_request::{AclCreation, CreateAclsRequest},
    delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
    describe_acls_request::DescribeAclsRequest,
    describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
};
use crabka_security::SaslMechanism;
use ring::rand::{SecureRandom, SystemRandom};

use crate::{AdminClient, AdminError, KafkaError, kafka_error_name};

/// KIP-554 wire byte for SCRAM-SHA-512. SHA-256 is byte `1`.
const SCRAM_SHA_512_WIRE: i8 = 2;
/// SCRAM mechanism byte for SCRAM-SHA-256 (1, KIP-554).
/// Paired with the `*_sha256` builders/helpers below.
const SCRAM_SHA_256_WIRE: i8 = 1;

/// Default PBKDF2 iteration count for new SCRAM credentials. Matches
/// Kafka's `org.apache.kafka.common.security.scram.internals.ScramFormatter`
/// default and exceeds the broker's `MIN_ITERATIONS = 4096`.
pub const DEFAULT_SCRAM_ITERATIONS: i32 = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PatternType {
    Literal,
    Prefixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PermissionType {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    /// KIP-939: 2PC participation permission on a `TransactionalId`.
    TwoPhaseCommit,
}

/// A concrete (non-filter) ACL entry — every field populated. Matches
/// the shape the broker stores in its metadata image.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AclEntry {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub pattern_type: PatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: PermissionType,
}

/// Filter for `DescribeAcls` / `DeleteAcls`. Each `None` axis matches
/// anything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AclEntryFilter {
    pub resource_type: Option<ResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<PatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission_type: Option<PermissionType>,
}

#[derive(Debug, Clone)]
pub struct ScramUpsertion {
    pub username: String,
    pub password: String,
    pub iterations: i32,
}

#[derive(Debug, Clone)]
pub struct ScramDeletion {
    pub username: String,
}

#[derive(Debug, Clone)]
pub struct ScramUserOutcome {
    pub username: String,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredential {
    pub mechanism: String,
    pub iterations: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScramCredentials {
    pub username: String,
    pub credentials: Vec<UserScramCredential>,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone)]
pub struct CreateAclOutcome {
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone)]
pub struct DeleteAclFilterOutcome {
    pub error: Option<KafkaError>,
    pub matched: Vec<AclEntry>,
}

impl AdminClient {
    /// Upsert and/or delete SCRAM-SHA-512 credentials in a single call.
    ///
    /// `upsertions` carry plaintext passwords — the function generates a
    /// fresh 16-byte salt per row and computes the KIP-554 wire
    /// `salted_password` (PBKDF2-HMAC-SHA-512) client-side via
    /// `crabka_security::pbkdf2_salted`. The broker never sees the raw
    /// password.
    pub async fn alter_user_scram_credentials_sha512(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        let rng = SystemRandom::new();
        let req = build_alter_scram_request_sha512(upsertions, deletions, &rng)?;
        let resp = self.conn.send(req).await?;
        Ok(parse_alter_scram_results(resp))
    }

    /// SCRAM-SHA-256 sibling of
    /// [`Self::alter_user_scram_credentials_sha512`]. Iteration counts,
    /// salt generation, and salted-password derivation are identical
    /// to the SHA-512 path; only the mechanism wire byte and HMAC
    /// algorithm differ.
    ///
    /// # Errors
    ///
    /// Same as `_sha512`: returns [`AdminError::Protocol`] when the
    /// system RNG fails, otherwise propagates the broker's
    /// per-username outcome rows.
    pub async fn alter_user_scram_credentials_sha256(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        let rng = SystemRandom::new();
        let req = build_alter_scram_request_sha256(upsertions, deletions, &rng)?;
        let resp = self.conn.send(req).await?;
        Ok(parse_alter_scram_results(resp))
    }

    /// List ACLs matching `filter`. The broker's response is
    /// resource-grouped on the wire (one block per `(resource_type,
    /// resource_name, pattern_type)`); we flatten back into `AclEntry`
    /// rows for diffing.
    pub async fn describe_acls(
        &mut self,
        filter: &AclEntryFilter,
    ) -> Result<Vec<AclEntry>, AdminError> {
        let req = filter_to_describe_request(filter);
        let resp = match self.conn.send(req.clone()).await {
            Ok(resp) => resp,
            Err(error) if AdminClient::is_retriable_transport_error(&error) => {
                self.reconnect_bootstrap().await?;
                self.conn.send(req).await?
            }
            Err(error) => return Err(AdminError::from(error)),
        };
        parse_describe_acls(resp)
    }

    pub async fn describe_user_scram_credentials(
        &mut self,
        users: Option<&[String]>,
    ) -> Result<Vec<UserScramCredentials>, AdminError> {
        let req = DescribeUserScramCredentialsRequest {
            users: users.map(|users| {
                users
                    .iter()
                    .map(|name| UserName {
                        name: name.clone(),
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;

        parse_describe_user_scram_credentials_response(resp)
    }

    /// Create the supplied ACLs.
    pub async fn create_acls(
        &mut self,
        creations: &[AclEntry],
    ) -> Result<Vec<CreateAclOutcome>, AdminError> {
        let req = CreateAclsRequest {
            creations: creations.iter().map(acl_to_creation).collect(),
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;
        Ok(resp
            .results
            .into_iter()
            .map(|r| CreateAclOutcome {
                error: error_if(r.error_code, r.error_message),
            })
            .collect())
    }

    /// Delete every ACL matching any of `filters`. Each filter's
    /// response surfaces the matched ACL set so callers can confirm
    /// the deletion converged on the expected rows.
    pub async fn delete_acls(
        &mut self,
        filters: &[AclEntryFilter],
    ) -> Result<Vec<DeleteAclFilterOutcome>, AdminError> {
        let req = DeleteAclsRequest {
            filters: filters.iter().map(acl_filter_to_wire).collect(),
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;
        let mut out = Vec::with_capacity(resp.filter_results.len());
        for fr in resp.filter_results {
            if let Some(err) = error_if(fr.error_code, fr.error_message) {
                out.push(DeleteAclFilterOutcome {
                    error: Some(err),
                    matched: Vec::new(),
                });
                continue;
            }
            let mut matched = Vec::with_capacity(fr.matching_acls.len());
            for m in fr.matching_acls {
                if m.error_code != 0 {
                    // Per-row deletion error — bubble it up as the
                    // filter-level error so the reconciler retries.
                    return Err(AdminError::Broker {
                        api: "DeleteAcls",
                        code: m.error_code,
                        name: kafka_error_name(m.error_code),
                        message: m.error_message,
                    });
                }
                matched.push(AclEntry {
                    resource_type: wire_to_resource_type(m.resource_type)?,
                    resource_name: m.resource_name,
                    pattern_type: wire_to_pattern_type(m.pattern_type)?,
                    principal: m.principal,
                    host: m.host,
                    operation: wire_to_operation(m.operation)?,
                    permission_type: wire_to_permission(m.permission_type)?,
                });
            }
            out.push(DeleteAclFilterOutcome {
                error: None,
                matched,
            });
        }
        Ok(out)
    }
}

fn error_if(code: i16, message: Option<String>) -> Option<KafkaError> {
    if code == 0 {
        None
    } else {
        Some(KafkaError {
            code,
            name: kafka_error_name(code),
            message,
        })
    }
}

fn build_alter_scram_request_sha512(
    upsertions: &[ScramUpsertion],
    deletions: &[ScramDeletion],
    rng: &SystemRandom,
) -> Result<AlterUserScramCredentialsRequest, AdminError> {
    build_alter_scram_request(
        upsertions,
        deletions,
        rng,
        SaslMechanism::ScramSha512,
        SCRAM_SHA_512_WIRE,
    )
}

/// SCRAM-SHA-256 sibling of
/// [`build_alter_scram_request_sha512`]. Pulled into
/// [`build_alter_scram_request`] so the two helpers can't drift.
fn build_alter_scram_request_sha256(
    upsertions: &[ScramUpsertion],
    deletions: &[ScramDeletion],
    rng: &SystemRandom,
) -> Result<AlterUserScramCredentialsRequest, AdminError> {
    build_alter_scram_request(
        upsertions,
        deletions,
        rng,
        SaslMechanism::ScramSha256,
        SCRAM_SHA_256_WIRE,
    )
}

fn build_alter_scram_request(
    upsertions: &[ScramUpsertion],
    deletions: &[ScramDeletion],
    rng: &SystemRandom,
    mechanism: SaslMechanism,
    wire_mechanism: i8,
) -> Result<AlterUserScramCredentialsRequest, AdminError> {
    let mut wire_upserts = Vec::with_capacity(upsertions.len());
    for u in upsertions {
        let mut salt = vec![0u8; 16];
        rng.fill(&mut salt)
            .map_err(|_| AdminError::Protocol("system RNG failure".into()))?;
        let salted = crabka_security::pbkdf2_salted(
            u.password.as_bytes(),
            mechanism,
            u32::try_from(u.iterations.max(0)).unwrap_or(0),
            &salt,
        );
        wire_upserts.push(ScramCredentialUpsertion {
            name: u.username.clone(),
            mechanism: wire_mechanism,
            iterations: u.iterations,
            salt: Bytes::from(salt),
            salted_password: Bytes::from(salted),
            ..Default::default()
        });
    }
    let wire_deletes = deletions
        .iter()
        .map(|d| ScramCredentialDeletion {
            name: d.username.clone(),
            mechanism: wire_mechanism,
            ..Default::default()
        })
        .collect();
    Ok(AlterUserScramCredentialsRequest {
        upsertions: wire_upserts,
        deletions: wire_deletes,
        ..Default::default()
    })
}

fn parse_alter_scram_results(
    resp: <AlterUserScramCredentialsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Vec<ScramUserOutcome> {
    resp.results
        .into_iter()
        .map(|r| ScramUserOutcome {
            username: r.user,
            error: error_if(r.error_code, r.error_message),
        })
        .collect()
}

fn parse_describe_user_scram_credentials_response(
    resp: <DescribeUserScramCredentialsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Result<Vec<UserScramCredentials>, AdminError> {
    if resp.error_code != 0 {
        return Err(AdminError::Broker {
            api: "DescribeUserScramCredentials",
            code: resp.error_code,
            name: kafka_error_name(resp.error_code),
            message: resp.error_message,
        });
    }

    Ok(resp
        .results
        .into_iter()
        .map(|result| UserScramCredentials {
            username: result.user,
            credentials: result
                .credential_infos
                .into_iter()
                .map(|credential| UserScramCredential {
                    mechanism: scram_mechanism_name(credential.mechanism).to_string(),
                    iterations: credential.iterations,
                })
                .collect(),
            error: error_if(result.error_code, result.error_message),
        })
        .collect())
}

fn scram_mechanism_name(mechanism: i8) -> &'static str {
    match mechanism {
        SCRAM_SHA_256_WIRE => "SCRAM-SHA-256",
        SCRAM_SHA_512_WIRE => "SCRAM-SHA-512",
        _ => "UNKNOWN",
    }
}

fn filter_to_describe_request(f: &AclEntryFilter) -> DescribeAclsRequest {
    DescribeAclsRequest {
        resource_type_filter: f.resource_type.map_or(WIRE_ANY, resource_type_to_wire),
        resource_name_filter: f.resource_name.clone(),
        pattern_type_filter: f.pattern_type.map_or(WIRE_ANY, pattern_type_to_wire),
        principal_filter: f.principal.clone(),
        host_filter: f.host.clone(),
        operation: f.operation.map_or(WIRE_ANY, operation_to_wire),
        permission_type: f.permission_type.map_or(WIRE_ANY, permission_to_wire),
        ..Default::default()
    }
}

fn parse_describe_acls(
    resp: <DescribeAclsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Result<Vec<AclEntry>, AdminError> {
    if resp.error_code != 0 {
        return Err(AdminError::Broker {
            api: "DescribeAcls",
            code: resp.error_code,
            name: kafka_error_name(resp.error_code),
            message: resp.error_message,
        });
    }
    let mut out = Vec::new();
    for resource in resp.resources {
        let rt = wire_to_resource_type(resource.resource_type)?;
        let pt = wire_to_pattern_type(resource.pattern_type)?;
        for desc in resource.acls {
            out.push(AclEntry {
                resource_type: rt,
                resource_name: resource.resource_name.clone(),
                pattern_type: pt,
                principal: desc.principal,
                host: desc.host,
                operation: wire_to_operation(desc.operation)?,
                permission_type: wire_to_permission(desc.permission_type)?,
            });
        }
    }
    Ok(out)
}

/// Pure: serialize an `AclEntry` to the wire representation
/// `CreateAcls` expects.
pub(crate) fn acl_to_creation(e: &AclEntry) -> AclCreation {
    AclCreation {
        resource_type: resource_type_to_wire(e.resource_type),
        resource_name: e.resource_name.clone(),
        resource_pattern_type: pattern_type_to_wire(e.pattern_type),
        principal: e.principal.clone(),
        host: e.host.clone(),
        operation: operation_to_wire(e.operation),
        permission_type: permission_to_wire(e.permission_type),
        ..Default::default()
    }
}

/// Pure: serialize an `AclEntryFilter` to the wire `DeleteAcls` filter.
/// `None` axes use the wire ANY discriminant.
pub(crate) fn acl_filter_to_wire(f: &AclEntryFilter) -> DeleteAclsFilter {
    DeleteAclsFilter {
        resource_type_filter: f.resource_type.map_or(WIRE_ANY, resource_type_to_wire),
        resource_name_filter: f.resource_name.clone(),
        pattern_type_filter: f.pattern_type.map_or(WIRE_ANY, pattern_type_to_wire),
        principal_filter: f.principal.clone(),
        host_filter: f.host.clone(),
        operation: f.operation.map_or(WIRE_ANY, operation_to_wire),
        permission_type: f.permission_type.map_or(WIRE_ANY, permission_to_wire),
        ..Default::default()
    }
}

// --- wire constants & enum encoding/decoding -----------------------------
//
// Kept private to this module; the broker has its own copy in
// `crabka_broker::handlers::acl_wire`. Round-trip tests below lock the
// encoding against the values Kafka's protocol-spec docs publish.

/// Kafka `AclBindingFilter.ANY` discriminant — used as the
/// "match anything" placeholder on filter requests.
const WIRE_ANY: i8 = 1;

fn resource_type_to_wire(rt: ResourceType) -> i8 {
    match rt {
        ResourceType::Topic => 2,
        ResourceType::Group => 3,
        ResourceType::Cluster => 4,
        ResourceType::TransactionalId => 5,
    }
}

fn wire_to_resource_type(b: i8) -> Result<ResourceType, AdminError> {
    match b {
        2 => Ok(ResourceType::Topic),
        3 => Ok(ResourceType::Group),
        4 => Ok(ResourceType::Cluster),
        5 => Ok(ResourceType::TransactionalId),
        _ => Err(AdminError::Protocol(format!(
            "unknown ACL resource_type discriminant: {b}",
        ))),
    }
}

fn pattern_type_to_wire(pt: PatternType) -> i8 {
    match pt {
        PatternType::Literal => 3,
        PatternType::Prefixed => 4,
    }
}

fn wire_to_pattern_type(b: i8) -> Result<PatternType, AdminError> {
    match b {
        3 => Ok(PatternType::Literal),
        4 => Ok(PatternType::Prefixed),
        _ => Err(AdminError::Protocol(format!(
            "unknown ACL pattern_type discriminant: {b}",
        ))),
    }
}

fn permission_to_wire(pt: PermissionType) -> i8 {
    match pt {
        PermissionType::Deny => 2,
        PermissionType::Allow => 3,
    }
}

fn wire_to_permission(b: i8) -> Result<PermissionType, AdminError> {
    match b {
        2 => Ok(PermissionType::Deny),
        3 => Ok(PermissionType::Allow),
        _ => Err(AdminError::Protocol(format!(
            "unknown ACL permission discriminant: {b}",
        ))),
    }
}

fn operation_to_wire(op: AclOperation) -> i8 {
    match op {
        AclOperation::All => 2,
        AclOperation::Read => 3,
        AclOperation::Write => 4,
        AclOperation::Create => 5,
        AclOperation::Delete => 6,
        AclOperation::Alter => 7,
        AclOperation::Describe => 8,
        AclOperation::ClusterAction => 9,
        AclOperation::DescribeConfigs => 10,
        AclOperation::AlterConfigs => 11,
        AclOperation::IdempotentWrite => 12,
        AclOperation::TwoPhaseCommit => 15,
    }
}

fn wire_to_operation(b: i8) -> Result<AclOperation, AdminError> {
    match b {
        2 => Ok(AclOperation::All),
        3 => Ok(AclOperation::Read),
        4 => Ok(AclOperation::Write),
        5 => Ok(AclOperation::Create),
        6 => Ok(AclOperation::Delete),
        7 => Ok(AclOperation::Alter),
        8 => Ok(AclOperation::Describe),
        9 => Ok(AclOperation::ClusterAction),
        10 => Ok(AclOperation::DescribeConfigs),
        11 => Ok(AclOperation::AlterConfigs),
        12 => Ok(AclOperation::IdempotentWrite),
        15 => Ok(AclOperation::TwoPhaseCommit),
        _ => Err(AdminError::Protocol(format!(
            "unknown ACL operation discriminant: {b}",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_protocol::UnknownTaggedFields;

    use super::*;

    fn sample_entry() -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }
    }

    #[test]
    fn resource_type_round_trips() {
        for rt in [
            ResourceType::Topic,
            ResourceType::Group,
            ResourceType::Cluster,
            ResourceType::TransactionalId,
        ] {
            assert!(wire_to_resource_type(resource_type_to_wire(rt)).unwrap() == rt);
        }
    }

    #[test]
    fn pattern_type_round_trips() {
        for pt in [PatternType::Literal, PatternType::Prefixed] {
            assert!(wire_to_pattern_type(pattern_type_to_wire(pt)).unwrap() == pt);
        }
    }

    #[test]
    fn permission_round_trips() {
        for p in [PermissionType::Allow, PermissionType::Deny] {
            assert!(wire_to_permission(permission_to_wire(p)).unwrap() == p);
        }
    }

    #[test]
    fn operation_round_trips() {
        for op in [
            AclOperation::All,
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
            AclOperation::IdempotentWrite,
            // KIP-939: TWO_PHASE_COMMIT (wire byte 15).
            AclOperation::TwoPhaseCommit,
        ] {
            assert!(wire_to_operation(operation_to_wire(op)).unwrap() == op);
        }
    }

    #[test]
    fn wire_to_unknown_resource_type_errors() {
        assert!(matches!(
            wire_to_resource_type(99),
            Err(AdminError::Protocol(_))
        ));
        // ANY (1) is intentionally rejected on the concrete decoder so
        // `DescribeAcls`/`DeleteAcls` responses can never silently
        // claim an "any-type" match — Kafka never returns this in real
        // responses.
        assert!(matches!(
            wire_to_resource_type(1),
            Err(AdminError::Protocol(_))
        ));
    }

    #[test]
    fn acl_to_creation_matches_discriminants() {
        let e = sample_entry();
        let c = acl_to_creation(&e);
        assert!(
            c == AclCreation {
                resource_type: 2,
                resource_name: "orders".to_string(),
                resource_pattern_type: 3,
                principal: "User:alice".to_string(),
                host: "*".to_string(),
                operation: 3,
                permission_type: 3,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn acl_filter_to_wire_uses_any_for_none_axes() {
        let f = AclEntryFilter::default();
        let w = acl_filter_to_wire(&f);
        // 1 == Kafka's `AclBindingFilter.ANY` discriminant (`WIRE_ANY`).
        assert!(
            w == DeleteAclsFilter {
                resource_type_filter: 1,
                resource_name_filter: None,
                pattern_type_filter: 1,
                principal_filter: None,
                host_filter: None,
                operation: 1,
                permission_type: 1,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn acl_filter_to_wire_passes_concrete_axes_through() {
        let f = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            resource_name: Some("orders".into()),
            pattern_type: Some(PatternType::Literal),
            principal: Some("User:alice".into()),
            host: Some("10.0.0.0".into()),
            operation: Some(AclOperation::Read),
            permission_type: Some(PermissionType::Allow),
        };
        let w = acl_filter_to_wire(&f);
        assert!(
            w == DeleteAclsFilter {
                resource_type_filter: 2,
                resource_name_filter: Some("orders".to_string()),
                pattern_type_filter: 3,
                principal_filter: Some("User:alice".to_string()),
                host_filter: Some("10.0.0.0".to_string()),
                operation: 3,
                permission_type: 3,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn scram_request_carries_pbkdf2_intermediate_not_password() {
        let rng = SystemRandom::new();
        let upserts = [ScramUpsertion {
            username: "alice".into(),
            password: "hunter2".into(),
            iterations: 4096,
        }];
        let req = build_alter_scram_request_sha512(&upserts, &[], &rng).unwrap();
        assert!(req.upsertions.len() == 1);
        let u = &req.upsertions[0];
        check!(u.name == "alice");
        check!(u.mechanism == 2); // SCRAM_SHA_512_WIRE, KIP-554
        check!(u.iterations == 4096);
        check!(u.salt.len() == 16);
        // SHA-512 output is 64 bytes — KIP-554 mandates the wire field
        // carries the PBKDF2 intermediate, not the raw password.
        check!(u.salted_password.len() == 64);
        check!(u.salted_password.as_ref() != b"hunter2");
    }

    #[test]
    fn scram_request_deletions_use_sha512_mechanism() {
        let rng = SystemRandom::new();
        let dels = [ScramDeletion {
            username: "alice".into(),
        }];
        let req = build_alter_scram_request_sha512(&[], &dels, &rng).unwrap();
        assert!(
            req == AlterUserScramCredentialsRequest {
                deletions: vec![ScramCredentialDeletion {
                    name: "alice".to_string(),
                    mechanism: 2, // SCRAM_SHA_512_WIRE, KIP-554
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                upsertions: vec![],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn scram_request_two_upserts_get_distinct_salts() {
        let rng = SystemRandom::new();
        let upserts = [
            ScramUpsertion {
                username: "alice".into(),
                password: "p".into(),
                iterations: 4096,
            },
            ScramUpsertion {
                username: "bob".into(),
                password: "p".into(),
                iterations: 4096,
            },
        ];
        let req = build_alter_scram_request_sha512(&upserts, &[], &rng).unwrap();
        assert!(req.upsertions[0].salt != req.upsertions[1].salt);
    }

    #[test]
    fn describe_request_uses_any_for_unspecified_axes() {
        let f = AclEntryFilter {
            principal: Some("User:alice".into()),
            ..Default::default()
        };
        let r = filter_to_describe_request(&f);
        // 1 == Kafka's `AclBindingFilter.ANY` discriminant (`WIRE_ANY`).
        assert!(
            r == DescribeAclsRequest {
                resource_type_filter: 1,
                resource_name_filter: None,
                pattern_type_filter: 1,
                principal_filter: Some("User:alice".to_string()),
                host_filter: None,
                operation: 1,
                permission_type: 1,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn users_describe_scram_top_level_error_returns_broker_error() {
        let resp = crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse {
            error_code: 31,
            error_message: Some("cluster auth denied".to_string()),
            ..Default::default()
        };

        let err = parse_describe_user_scram_credentials_response(resp)
            .expect_err("top-level SCRAM describe errors must fail the request");

        match err {
            AdminError::Broker {
                api,
                code,
                name,
                message,
            } => {
                check!(api == "DescribeUserScramCredentials");
                check!(code == 31);
                check!(name == "CLUSTER_AUTHORIZATION_FAILED");
                check!(message == Some("cluster auth denied".to_string()));
            }
            other => panic!("expected broker error, got {other:?}"),
        }
    }

    #[test]
    fn users_describe_scram_preserves_credential_iterations() {
        let resp = crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse {
            results: vec![
                crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResult {
                    user: "alice".to_string(),
                    credential_infos: vec![
                        crabka_protocol::owned::describe_user_scram_credentials_response::CredentialInfo {
                            mechanism: 1,
                            iterations: 4096,
                            ..Default::default()
                        },
                        crabka_protocol::owned::describe_user_scram_credentials_response::CredentialInfo {
                            mechanism: 2,
                            iterations: 8192,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let users = parse_describe_user_scram_credentials_response(resp)
            .expect("valid SCRAM describe response should parse");

        assert!(users.len() == 1);
        check!(users[0].username == "alice");
        check!(
            users[0].credentials
                == vec![
                    UserScramCredential {
                        mechanism: "SCRAM-SHA-256".to_string(),
                        iterations: 4096,
                    },
                    UserScramCredential {
                        mechanism: "SCRAM-SHA-512".to_string(),
                        iterations: 8192,
                    },
                ]
        );
    }

    #[test]
    fn users_describe_scram_unknown_user_preserves_resource_not_found_name() {
        let resp = crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse {
            results: vec![
                crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResult {
                    user: "ghost".to_string(),
                    error_code: 91,
                    error_message: Some("no such SCRAM user".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let users = parse_describe_user_scram_credentials_response(resp)
            .expect("per-user SCRAM describe errors should parse into rows");

        check!(users.len() == 1);
        check!(users[0].username == "ghost");
        check!(
            users[0].error
                == Some(KafkaError {
                    code: 91,
                    name: "RESOURCE_NOT_FOUND",
                    message: Some("no such SCRAM user".to_string()),
                })
        );
    }
}
