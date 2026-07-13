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

use crate::{AdminClient, AdminError, KafkaError, kafka_error_if, kafka_error_name};

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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
                error: kafka_error_if(r.error_code, r.error_message),
            })
            .collect())
    }

    /// Delete every ACL matching any of `filters`. Each filter's
    /// response surfaces the matched ACL set so callers can confirm
    /// the deletion converged on the expected rows.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
            if let Some(err) = kafka_error_if(fr.error_code, fr.error_message) {
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
            error: kafka_error_if(r.error_code, r.error_message),
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
            error: kafka_error_if(result.error_code, result.error_message),
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
    let wire = acl_filter_wire_fields(f);
    DescribeAclsRequest {
        resource_type_filter: wire.resource_type_filter,
        resource_name_filter: wire.resource_name_filter,
        pattern_type_filter: wire.pattern_type_filter,
        principal_filter: wire.principal_filter,
        host_filter: wire.host_filter,
        operation: wire.operation,
        permission_type: wire.permission_type,
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
    let wire = acl_filter_wire_fields(f);
    DeleteAclsFilter {
        resource_type_filter: wire.resource_type_filter,
        resource_name_filter: wire.resource_name_filter,
        pattern_type_filter: wire.pattern_type_filter,
        principal_filter: wire.principal_filter,
        host_filter: wire.host_filter,
        operation: wire.operation,
        permission_type: wire.permission_type,
        ..Default::default()
    }
}

struct AclFilterWireFields {
    resource_type_filter: i8,
    resource_name_filter: Option<String>,
    pattern_type_filter: i8,
    principal_filter: Option<String>,
    host_filter: Option<String>,
    operation: i8,
    permission_type: i8,
}

fn acl_filter_wire_fields(f: &AclEntryFilter) -> AclFilterWireFields {
    AclFilterWireFields {
        resource_type_filter: f.resource_type.map_or(WIRE_ANY, resource_type_to_wire),
        resource_name_filter: f.resource_name.clone(),
        pattern_type_filter: f.pattern_type.map_or(WIRE_ANY, pattern_type_to_wire),
        principal_filter: f.principal.clone(),
        host_filter: f.host.clone(),
        operation: f.operation.map_or(WIRE_ANY, operation_to_wire),
        permission_type: f.permission_type.map_or(WIRE_ANY, permission_to_wire),
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

macro_rules! acl_wire_enum {
    ($to_wire:ident, $from_wire:ident, $ty:ty, $unknown:literal, {$($variant:path => $wire:literal),+ $(,)?}) => {
        fn $to_wire(value: $ty) -> i8 {
            match value {
                $($variant => $wire,)+
            }
        }

        fn $from_wire(value: i8) -> Result<$ty, AdminError> {
            match value {
                $($wire => Ok($variant),)+
                _ => Err(AdminError::Protocol(format!(concat!($unknown, ": {}"), value))),
            }
        }
    };
}

acl_wire_enum!(
    resource_type_to_wire,
    wire_to_resource_type,
    ResourceType,
    "unknown ACL resource_type discriminant",
    {
        ResourceType::Topic => 2,
        ResourceType::Group => 3,
        ResourceType::Cluster => 4,
        ResourceType::TransactionalId => 5,
    }
);

acl_wire_enum!(
    pattern_type_to_wire,
    wire_to_pattern_type,
    PatternType,
    "unknown ACL pattern_type discriminant",
    {
        PatternType::Literal => 3,
        PatternType::Prefixed => 4,
    }
);

acl_wire_enum!(
    permission_to_wire,
    wire_to_permission,
    PermissionType,
    "unknown ACL permission discriminant",
    {
        PermissionType::Deny => 2,
        PermissionType::Allow => 3,
    }
);

acl_wire_enum!(
    operation_to_wire,
    wire_to_operation,
    AclOperation,
    "unknown ACL operation discriminant",
    {
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
);

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::check;
    use bytes::{Buf, BytesMut};
    use crabka_client_core::MockBroker;
    use crabka_protocol::{
        Decode, Encode, UnknownTaggedFields,
        owned::{
            alter_user_scram_credentials_response::{
                AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
            },
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            create_acls_request,
            create_acls_response::{AclCreationResult, CreateAclsResponse},
            delete_acls_request,
            delete_acls_response::{
                DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse,
            },
        },
    };

    use super::*;

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        encode_at(resp, 0)
    }

    fn encode_at(resp: &impl Encode, version: i16) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn api_versions_response(api_key: i16, version: i16) -> Vec<u8> {
        encode_v0(&ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 0,
                    ..Default::default()
                },
                ApiVersion {
                    api_key,
                    min_version: version,
                    max_version: version,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
    }

    fn request_body_after_header(mut body: &[u8], flexible_header: bool) -> &[u8] {
        let client_id_len = body.get_i16();
        assert2::assert!(client_id_len >= 0);
        body.advance(usize::try_from(client_id_len).expect("client id length is non-negative"));
        if flexible_header {
            assert2::assert!(body.get_u8() == 0);
        }
        body
    }

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
        for (_name, rt) in [
            ("topic", ResourceType::Topic),
            ("group", ResourceType::Group),
            ("cluster", ResourceType::Cluster),
            ("transactional id", ResourceType::TransactionalId),
        ] {
            assert2::assert!(wire_to_resource_type(resource_type_to_wire(rt)).unwrap() == rt);
        }
    }

    #[test]
    fn pattern_type_round_trips() {
        for (_name, pt) in [
            ("literal", PatternType::Literal),
            ("prefixed", PatternType::Prefixed),
        ] {
            assert2::assert!(wire_to_pattern_type(pattern_type_to_wire(pt)).unwrap() == pt);
        }
    }

    #[test]
    fn permission_round_trips() {
        for (_name, permission) in [
            ("allow", PermissionType::Allow),
            ("deny", PermissionType::Deny),
        ] {
            assert2::assert!(
                wire_to_permission(permission_to_wire(permission)).unwrap() == permission
            );
        }
    }

    #[test]
    fn operation_round_trips() {
        for (_name, operation) in [
            ("all", AclOperation::All),
            ("read", AclOperation::Read),
            ("write", AclOperation::Write),
            ("create", AclOperation::Create),
            ("delete", AclOperation::Delete),
            ("alter", AclOperation::Alter),
            ("describe", AclOperation::Describe),
            ("cluster action", AclOperation::ClusterAction),
            ("describe configs", AclOperation::DescribeConfigs),
            ("alter configs", AclOperation::AlterConfigs),
            ("idempotent write", AclOperation::IdempotentWrite),
            // KIP-939: TWO_PHASE_COMMIT (wire byte 15).
            ("two phase commit", AclOperation::TwoPhaseCommit),
        ] {
            assert2::assert!(wire_to_operation(operation_to_wire(operation)).unwrap() == operation);
        }
    }

    #[test]
    fn wire_to_unknown_resource_type_errors() {
        assert2::assert!(matches!(
            wire_to_resource_type(99),
            Err(AdminError::Protocol(_))
        ));
        // ANY (1) is intentionally rejected on the concrete decoder so
        // `DescribeAcls`/`DeleteAcls` responses can never silently
        // claim an "any-type" match — Kafka never returns this in real
        // responses.
        assert2::assert!(matches!(
            wire_to_resource_type(1),
            Err(AdminError::Protocol(_))
        ));
    }

    #[test]
    fn acl_to_creation_matches_discriminants() {
        let e = AclEntry {
            pattern_type: PatternType::Prefixed,
            ..sample_entry()
        };
        let c = acl_to_creation(&e);
        assert2::assert!(
            c == AclCreation {
                resource_type: 2,
                resource_name: "orders".to_string(),
                resource_pattern_type: 4,
                principal: "User:alice".to_string(),
                host: "*".to_string(),
                operation: 3,
                permission_type: 3,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_acls_maps_non_empty_broker_results() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(create_acls_request::API_KEY, 1));
            }
            if api_key == create_acls_request::API_KEY {
                let mut body =
                    request_body_after_header(body, version >= create_acls_request::FLEXIBLE_MIN);
                let request = CreateAclsRequest::decode(&mut body, version)
                    .expect("create ACLs request decodes");
                assert2::assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_at(
                    &CreateAclsResponse {
                        results: vec![AclCreationResult {
                            error_code: 36,
                            error_message: Some("acl exists".into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    1,
                ));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");
        let creation = AclEntry {
            pattern_type: PatternType::Prefixed,
            ..sample_entry()
        };

        let outcomes = admin
            .create_acls(&[creation])
            .await
            .expect("create ACL response maps");

        let error = outcomes[0]
            .error
            .as_ref()
            .expect("broker error is surfaced");
        assert2::assert!(
            (outcomes.len(), error.code, error.message.as_deref()) == (1, 36, Some("acl exists"))
        );
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("create ACLs request was captured");
        assert2::assert!(
            request
                == CreateAclsRequest {
                    creations: vec![AclCreation {
                        resource_type: resource_type_to_wire(ResourceType::Topic),
                        resource_name: "orders".into(),
                        resource_pattern_type: pattern_type_to_wire(PatternType::Prefixed),
                        principal: "User:alice".into(),
                        host: "*".into(),
                        operation: operation_to_wire(AclOperation::Read),
                        permission_type: permission_to_wire(PermissionType::Allow),
                        ..Default::default()
                    }],
                    ..Default::default()
                }
        );
        mock.stop();
    }

    #[test]
    fn acl_filter_to_wire_uses_any_for_none_axes() {
        let f = AclEntryFilter::default();
        let w = acl_filter_to_wire(&f);
        // 1 == Kafka's `AclBindingFilter.ANY` discriminant (`WIRE_ANY`).
        assert2::assert!(
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
        assert2::assert!(
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_acls_maps_non_empty_matched_acls() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(delete_acls_request::API_KEY, 1));
            }
            if api_key == delete_acls_request::API_KEY {
                let mut body =
                    request_body_after_header(body, version >= delete_acls_request::FLEXIBLE_MIN);
                let request = DeleteAclsRequest::decode(&mut body, version)
                    .expect("delete ACLs request decodes");
                assert2::assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_at(
                    &DeleteAclsResponse {
                        filter_results: vec![DeleteAclsFilterResult {
                            matching_acls: vec![DeleteAclsMatchingAcl {
                                resource_type: resource_type_to_wire(ResourceType::Topic),
                                resource_name: "orders".into(),
                                pattern_type: pattern_type_to_wire(PatternType::Literal),
                                principal: "User:alice".into(),
                                host: "*".into(),
                                operation: operation_to_wire(AclOperation::Read),
                                permission_type: permission_to_wire(PermissionType::Allow),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    1,
                ));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");
        let filter = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            resource_name: Some("orders".into()),
            ..Default::default()
        };

        let outcomes = admin
            .delete_acls(&[filter])
            .await
            .expect("delete ACL response maps");

        assert2::assert!(
            (
                outcomes.len(),
                outcomes[0].error.as_ref(),
                outcomes[0].matched.as_slice()
            ) == (1, None, [sample_entry()].as_slice())
        );
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("delete ACLs request was captured");
        assert2::assert!(
            request
                == DeleteAclsRequest {
                    filters: vec![DeleteAclsFilter {
                        resource_type_filter: resource_type_to_wire(ResourceType::Topic),
                        resource_name_filter: Some("orders".into()),
                        pattern_type_filter: WIRE_ANY,
                        principal_filter: None,
                        host_filter: None,
                        operation: WIRE_ANY,
                        permission_type: WIRE_ANY,
                        ..Default::default()
                    }],
                    ..Default::default()
                }
        );
        mock.stop();
    }

    #[test]
    fn parse_alter_scram_results_preserves_usernames_and_errors() {
        let outcomes = parse_alter_scram_results(AlterUserScramCredentialsResponse {
            results: vec![AlterUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 42,
                error_message: Some("bad credentials".into()),
                ..Default::default()
            }],
            ..Default::default()
        });

        let error = outcomes[0].error.as_ref().expect("error is preserved");
        assert2::assert!(
            (
                outcomes.len(),
                outcomes[0].username.as_str(),
                error.code,
                error.message.as_deref()
            ) == (1, "alice", 42, Some("bad credentials"))
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
        let u = &req.upsertions[0];
        check!(
            (
                req.upsertions.len(),
                u.name.as_str(),
                u.mechanism,
                u.iterations,
                u.salt.len(),
                u.salted_password.len(),
                u.salted_password.as_ref() != b"hunter2",
            ) == (1, "alice", 2, 4096, 16, 64, true)
        );
    }

    #[test]
    fn scram_request_deletions_use_sha512_mechanism() {
        let rng = SystemRandom::new();
        let dels = [ScramDeletion {
            username: "alice".into(),
        }];
        let req = build_alter_scram_request_sha512(&[], &dels, &rng).unwrap();
        assert2::assert!(
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
        assert2::assert!(req.upsertions[0].salt != req.upsertions[1].salt);
    }

    #[test]
    fn describe_request_uses_any_for_unspecified_axes() {
        let f = AclEntryFilter {
            principal: Some("User:alice".into()),
            ..Default::default()
        };
        let r = filter_to_describe_request(&f);
        // 1 == Kafka's `AclBindingFilter.ANY` discriminant (`WIRE_ANY`).
        assert2::assert!(
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
                check!(
                    (api, code, name, message.as_deref())
                        == (
                            "DescribeUserScramCredentials",
                            31,
                            "CLUSTER_AUTHORIZATION_FAILED",
                            Some("cluster auth denied"),
                        )
                );
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

        check!(
            (
                users.len(),
                users[0].username.as_str(),
                users[0].credentials.as_slice(),
            ) == (
                1,
                "alice",
                [
                    UserScramCredential {
                        mechanism: "SCRAM-SHA-256".to_string(),
                        iterations: 4096,
                    },
                    UserScramCredential {
                        mechanism: "SCRAM-SHA-512".to_string(),
                        iterations: 8192,
                    },
                ]
                .as_slice(),
            )
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

        check!(
            (
                users.len(),
                users[0].username.as_str(),
                users[0].error.as_ref()
            ) == (
                1,
                "ghost",
                Some(&KafkaError {
                    code: 91,
                    name: "RESOURCE_NOT_FOUND",
                    message: Some("no such SCRAM user".to_string()),
                })
            )
        );
    }

    #[test]
    fn users_describe_scram_duplicate_user_preserves_duplicate_resource_name() {
        let resp = crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse {
            results: vec![
                crabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResult {
                    user: "alice".to_string(),
                    error_code: 92,
                    error_message: Some("duplicate user".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let users = parse_describe_user_scram_credentials_response(resp)
            .expect("per-user SCRAM describe errors should parse into rows");

        check!(
            (
                users.len(),
                users[0].username.as_str(),
                users[0].error.as_ref()
            ) == (
                1,
                "alice",
                Some(&KafkaError {
                    code: 92,
                    name: "DUPLICATE_RESOURCE",
                    message: Some("duplicate user".to_string()),
                })
            )
        );
    }

    #[test]
    fn users_alter_scram_preserves_scram_error_names() {
        let resp = crabka_protocol::owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResponse {
            results: vec![
                crabka_protocol::owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResult {
                    user: "alice".to_string(),
                    error_code: 33,
                    error_message: Some("unknown mechanism".to_string()),
                    ..Default::default()
                },
                crabka_protocol::owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResult {
                    user: "bob".to_string(),
                    error_code: 93,
                    error_message: Some("too many iterations".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let users = parse_alter_scram_results(resp);

        check!(
            (
                users.len(),
                users[0].error.as_ref(),
                users[1].error.as_ref(),
            ) == (
                2,
                Some(&KafkaError {
                    code: 33,
                    name: "UNSUPPORTED_SASL_MECHANISM",
                    message: Some("unknown mechanism".to_string()),
                }),
                Some(&KafkaError {
                    code: 93,
                    name: "UNACCEPTABLE_CREDENTIAL",
                    message: Some("too many iterations".to_string()),
                }),
            )
        );
    }

    #[test]
    fn describe_request_passes_resource_name_and_host_filters_through() {
        let f = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            resource_name: Some("orders".into()),
            pattern_type: Some(PatternType::Literal),
            principal: Some("User:alice".into()),
            host: Some("10.0.0.0".into()),
            operation: Some(AclOperation::Read),
            permission_type: Some(PermissionType::Allow),
        };

        let r = filter_to_describe_request(&f);

        assert2::assert!(
            r == DescribeAclsRequest {
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
}
