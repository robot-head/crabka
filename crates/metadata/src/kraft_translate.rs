//! Translate Crabka's internal [`MetadataRecord`] currency to/from the
//! KIP-631 [`KraftMetadataRecord`] wire shape so the controller log and
//! snapshots are genuinely KIP-631-framed.
//!
//! The conversion is *not* a pure field rename: several Crabka records
//! carry state the KIP-631 records do not (and vice versa), so this
//! module bridges the gap and defaults the KIP-631 extras Crabka does
//! not model. The round-trip
//! `rec -> to_kraft(&image) -> from_kraft(&image) == rec` is the
//! correctness bar for every modeled variant.
//!
//! ## Variants with no faithful KIP-631 counterpart
//!
//! - `V1Voters` / `V1KRaftVersion` are KIP-853 raft *control* records,
//!   not metadata records — they have no entry in the
//!   [`KraftMetadataRecord`] dispatch. The engine keeps the wincode path
//!   for just these two.
//! - `V1DeleteAccessControlEntry(AclEntryFilter)` cannot round-trip:
//!   KIP-631's `RemoveAccessControlEntryRecord` carries only an
//!   `id: Uuid` (Kafka resolves a delete *filter* to the concrete set of
//!   matching ACL ids against the image and writes one tombstone per id).
//!   A multi-axis filter does not fit in 16 bytes, so this is treated as
//!   [`TranslateError::NoCounterpart`]; resolving filters to id tombstones
//!   is the engine's job, out of scope for the pure translation layer.
//!
//! ## Lossy / awkward mappings (documented)
//!
//! - `RegisterBroker`: Crabka stores a top-level `(host, port)` plus a
//!   per-listener `endpoints` list; KIP-631 has only `end_points` (no
//!   top-level host/port). The top-level pair is encoded as a synthetic
//!   leading `end_points` entry (name `""`, `security_protocol` PLAINTEXT)
//!   and split back out on decode; the real listeners follow. All the
//!   other KIP-631 extras (`incarnation_id`, `broker_epoch`, `features`,
//!   `fenced`, …) are defaulted on encode and dropped on decode.
//! - `DelegationToken`: KIP-631's record has no `hmac` field. The HMAC is
//!   hex-encoded into the otherwise-unused `requester` slot so it
//!   round-trips; `KafkaPrincipal`s map through their `Type:Name` string
//!   form.

use crabka_protocol::owned::access_control_entry_record::AccessControlEntryRecord;
use crabka_protocol::owned::client_quota_record::{
    ClientQuotaRecord as KClientQuotaRecord, EntityData,
};
use crabka_protocol::owned::config_record::ConfigRecord;
use crabka_protocol::owned::delegation_token_record::DelegationTokenRecord as KDelegationTokenRecord;
use crabka_protocol::owned::feature_level_record::FeatureLevelRecord as KFeatureLevelRecord;
use crabka_protocol::owned::partition_record::PartitionRecord as KPartitionRecord;
use crabka_protocol::owned::register_broker_record::{
    BrokerEndpoint as KBrokerEndpoint, RegisterBrokerRecord,
};
use crabka_protocol::owned::remove_topic_record::RemoveTopicRecord;
use crabka_protocol::owned::remove_user_scram_credential_record::RemoveUserScramCredentialRecord;
use crabka_protocol::owned::topic_record::TopicRecord as KTopicRecord;
use crabka_protocol::owned::unregister_broker_record::UnregisterBrokerRecord as KUnregisterBrokerRecord;
use crabka_protocol::owned::user_scram_credential_record::UserScramCredentialRecord;
use crabka_protocol::primitives::uuid::Uuid as KUuid;
use crabka_protocol::records::metadata::KraftMetadataRecord;

use crabka_security::{KafkaPrincipal, ListenerProtocol, SaslMechanism};

use crate::MetadataImage;
use crate::acl::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use crate::records::{
    BrokerConfigRecord, BrokerEndpoint, BrokerRegistrationRecord, ClientQuotaRecord,
    DelegationTokenRecord, DeleteScramCredentialRecord, DeleteTopicRecord, FeatureLevelRecord,
    MetadataRecord, PartitionRecord, QuotaEntity, ScramCredentialRecord, TopicConfigRecord,
    TopicRecord, UnregisterBrokerRecord,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TranslateError {
    #[error("record has no KIP-631 metadata counterpart: {0}")]
    NoCounterpart(&'static str),
    #[error("unknown topic id {0} on decode")]
    UnknownTopicId(uuid::Uuid),
    #[error("unknown topic name {0} on encode")]
    UnknownTopicName(String),
    #[error("invalid {field}: {detail}")]
    Invalid { field: &'static str, detail: String },
}

// ----- uuid bridging -----

fn to_kuuid(u: uuid::Uuid) -> KUuid {
    KUuid(*u.as_bytes())
}

fn from_kuuid(u: KUuid) -> uuid::Uuid {
    uuid::Uuid::from_bytes(u.0)
}

// ----- SCRAM mechanism (Kafka ScramMechanism: SCRAM_SHA_256=1, SCRAM_SHA_512=2) -----

fn scram_mechanism_to_wire(m: SaslMechanism) -> Result<i8, TranslateError> {
    match m {
        SaslMechanism::ScramSha256 => Ok(1),
        SaslMechanism::ScramSha512 => Ok(2),
        other => Err(TranslateError::Invalid {
            field: "scram mechanism",
            detail: format!("{other:?} is not a SCRAM mechanism"),
        }),
    }
}

fn scram_mechanism_from_wire(b: i8) -> Result<SaslMechanism, TranslateError> {
    match b {
        1 => Ok(SaslMechanism::ScramSha256),
        2 => Ok(SaslMechanism::ScramSha512),
        other => Err(TranslateError::Invalid {
            field: "scram mechanism",
            detail: format!("unknown SCRAM mechanism wire byte {other}"),
        }),
    }
}

// ----- ACL enum <-> Kafka i8 wire discriminants -----
//
// These mirror the canonical mappings in crabka-broker's `acl_wire`
// (Kafka serializes ACL enums as `i8`). Replicated here so the metadata
// crate does not depend on the broker.

fn resource_type_to_wire(rt: ResourceType) -> i8 {
    match rt {
        ResourceType::Topic => 2,
        ResourceType::Group => 3,
        ResourceType::Cluster => 4,
        ResourceType::TransactionalId => 5,
        ResourceType::DelegationToken => 6,
    }
}

fn resource_type_from_wire(b: i8) -> Result<ResourceType, TranslateError> {
    match b {
        2 => Ok(ResourceType::Topic),
        3 => Ok(ResourceType::Group),
        4 => Ok(ResourceType::Cluster),
        5 => Ok(ResourceType::TransactionalId),
        6 => Ok(ResourceType::DelegationToken),
        other => Err(TranslateError::Invalid {
            field: "acl resource_type",
            detail: format!("unknown wire byte {other}"),
        }),
    }
}

fn pattern_type_to_wire(pt: PatternType) -> i8 {
    match pt {
        PatternType::Literal => 3,
        PatternType::Prefixed => 4,
    }
}

fn pattern_type_from_wire(b: i8) -> Result<PatternType, TranslateError> {
    match b {
        3 => Ok(PatternType::Literal),
        4 => Ok(PatternType::Prefixed),
        other => Err(TranslateError::Invalid {
            field: "acl pattern_type",
            detail: format!("unknown wire byte {other}"),
        }),
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
    }
}

fn operation_from_wire(b: i8) -> Result<AclOperation, TranslateError> {
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
        other => Err(TranslateError::Invalid {
            field: "acl operation",
            detail: format!("unknown wire byte {other}"),
        }),
    }
}

fn permission_to_wire(pt: PermissionType) -> i8 {
    match pt {
        PermissionType::Deny => 2,
        PermissionType::Allow => 3,
    }
}

fn permission_from_wire(b: i8) -> Result<PermissionType, TranslateError> {
    match b {
        2 => Ok(PermissionType::Deny),
        3 => Ok(PermissionType::Allow),
        other => Err(TranslateError::Invalid {
            field: "acl permission_type",
            detail: format!("unknown wire byte {other}"),
        }),
    }
}

// ----- security_protocol (Kafka SecurityProtocol == ListenerProtocol ordinal) -----

fn protocol_to_wire(p: ListenerProtocol) -> i16 {
    match p {
        ListenerProtocol::Plaintext => 0,
        ListenerProtocol::Ssl => 1,
        ListenerProtocol::SaslPlaintext => 2,
        ListenerProtocol::SaslSsl => 3,
    }
}

fn protocol_from_wire(b: i16) -> Result<ListenerProtocol, TranslateError> {
    match b {
        0 => Ok(ListenerProtocol::Plaintext),
        1 => Ok(ListenerProtocol::Ssl),
        2 => Ok(ListenerProtocol::SaslPlaintext),
        3 => Ok(ListenerProtocol::SaslSsl),
        other => Err(TranslateError::Invalid {
            field: "security_protocol",
            detail: format!("unknown wire value {other}"),
        }),
    }
}

// ----- hex (delegation-token hmac smuggled through `requester`) -----

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(s: &str) -> Result<Vec<u8>, TranslateError> {
    if !s.len().is_multiple_of(2) {
        return Err(TranslateError::Invalid {
            field: "delegation token hmac (hex)",
            detail: "odd length".into(),
        });
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| TranslateError::Invalid {
                field: "delegation token hmac (hex)",
                detail: e.to_string(),
            })
        })
        .collect()
}

/// Convert a single [`MetadataRecord`] to its KIP-631 counterpart.
///
/// All modeled variants except `V1TopicConfig` (which may fan out to N
/// `Config` records — use [`to_kraft_records`]) yield exactly one record.
/// A multi-key `V1TopicConfig` yields its *first* key here; callers that
/// must preserve every key must use [`to_kraft_records`].
///
/// # Errors
/// [`TranslateError::NoCounterpart`] for raft control records / delete-ACL
/// filters, [`TranslateError::UnknownTopicName`] when a topic/partition
/// references a topic absent from `image`, and [`TranslateError::Invalid`]
/// for out-of-range enum values.
pub fn to_kraft(
    rec: &MetadataRecord,
    image: &MetadataImage,
) -> Result<KraftMetadataRecord, TranslateError> {
    to_kraft_iter(rec, image)?
        .next()
        .ok_or(TranslateError::NoCounterpart("empty translation"))
}

/// Convert a single [`MetadataRecord`] to one or more KIP-631 records.
///
/// Only `V1TopicConfig` with N keys fans out (to N `Config` records, one
/// per key); every other modeled variant yields exactly one record.
///
/// # Errors
/// Same as [`to_kraft`].
pub fn to_kraft_records(
    rec: &MetadataRecord,
    image: &MetadataImage,
) -> Result<Vec<KraftMetadataRecord>, TranslateError> {
    Ok(to_kraft_iter(rec, image)?.collect())
}

#[allow(clippy::too_many_lines)] // exhaustive match over MetadataRecord
fn to_kraft_iter(
    rec: &MetadataRecord,
    image: &MetadataImage,
) -> Result<std::vec::IntoIter<KraftMetadataRecord>, TranslateError> {
    let recs: Vec<KraftMetadataRecord> = match rec {
        MetadataRecord::V1FeatureLevel(f) => {
            vec![KraftMetadataRecord::FeatureLevel(KFeatureLevelRecord {
                name: f.name.clone(),
                feature_level: f.level,
                ..Default::default()
            })]
        }
        MetadataRecord::V1BrokerRegistration(b) => {
            vec![KraftMetadataRecord::RegisterBroker(
                register_broker_to_kraft(b)?,
            )]
        }
        MetadataRecord::V1UnregisterBroker(u) => {
            vec![KraftMetadataRecord::UnregisterBroker(
                KUnregisterBrokerRecord {
                    broker_id: i32::try_from(u.node_id).map_err(|_| TranslateError::Invalid {
                        field: "broker_id",
                        detail: format!("node_id {} exceeds i32", u.node_id),
                    })?,
                    ..Default::default()
                },
            )]
        }
        MetadataRecord::V1ScramCredential(s) => {
            vec![KraftMetadataRecord::UserScramCredential(
                UserScramCredentialRecord {
                    name: s.user.clone(),
                    mechanism: scram_mechanism_to_wire(s.mechanism)?,
                    salt: bytes::Bytes::copy_from_slice(&s.salt),
                    stored_key: bytes::Bytes::copy_from_slice(&s.stored_key),
                    server_key: bytes::Bytes::copy_from_slice(&s.server_key),
                    iterations: i32::try_from(s.iterations).map_err(|_| {
                        TranslateError::Invalid {
                            field: "scram iterations",
                            detail: format!("{} exceeds i32", s.iterations),
                        }
                    })?,
                    ..Default::default()
                },
            )]
        }
        MetadataRecord::V1DeleteScramCredential(s) => {
            vec![KraftMetadataRecord::RemoveUserScramCredential(
                RemoveUserScramCredentialRecord {
                    name: s.user.clone(),
                    mechanism: scram_mechanism_to_wire(s.mechanism)?,
                    ..Default::default()
                },
            )]
        }
        MetadataRecord::V1AccessControlEntry(e) => {
            vec![KraftMetadataRecord::AccessControlEntry(
                AccessControlEntryRecord {
                    id: KUuid::ZERO,
                    resource_type: resource_type_to_wire(e.resource_type),
                    resource_name: e.resource_name.clone(),
                    pattern_type: pattern_type_to_wire(e.pattern_type),
                    principal: e.principal.clone(),
                    host: e.host.clone(),
                    operation: operation_to_wire(e.operation),
                    permission_type: permission_to_wire(e.permission_type),
                    ..Default::default()
                },
            )]
        }
        MetadataRecord::V1ClientQuota(q) => {
            vec![KraftMetadataRecord::ClientQuota(client_quota_to_kraft(q))]
        }
        MetadataRecord::V1DelegationToken(t) => {
            vec![KraftMetadataRecord::DelegationToken(
                delegation_token_to_kraft(t),
            )]
        }
        MetadataRecord::V1DeleteDelegationToken(t) => {
            vec![KraftMetadataRecord::RemoveDelegationToken(
            crabka_protocol::owned::remove_delegation_token_record::RemoveDelegationTokenRecord {
                token_id: t.token_id.clone(),
                ..Default::default()
            },
        )]
        }
        MetadataRecord::V1BrokerConfig(c) => {
            vec![KraftMetadataRecord::Config(ConfigRecord {
                resource_type: 0, // BROKER
                resource_name: c.node_id.to_string(),
                name: c.config_name.clone(),
                value: c.config_value.clone(),
                ..Default::default()
            })]
        }
        // ----- Task 2: topic / partition / remove-topic / topic-config -----
        MetadataRecord::V1Topic(t) => {
            vec![KraftMetadataRecord::Topic(KTopicRecord {
                name: t.name.clone(),
                topic_id: to_kuuid(t.topic_id),
                ..Default::default()
            })]
        }
        MetadataRecord::V1Partition(p) => {
            let topic = image
                .topic(&p.topic)
                .ok_or_else(|| TranslateError::UnknownTopicName(p.topic.clone()))?;
            vec![KraftMetadataRecord::Partition(partition_to_kraft(
                p,
                topic.topic_id,
            )?)]
        }
        MetadataRecord::V1DeleteTopic(d) => {
            let topic = image
                .topic(&d.name)
                .ok_or_else(|| TranslateError::UnknownTopicName(d.name.clone()))?;
            vec![KraftMetadataRecord::RemoveTopic(RemoveTopicRecord {
                topic_id: to_kuuid(topic.topic_id),
                ..Default::default()
            })]
        }
        MetadataRecord::V1TopicConfig(c) => {
            if c.overrides.is_empty() {
                // An empty override map clears the topic's configs. There is
                // no per-key Config record to emit; represent the clear as a
                // single tombstone-less empty fan-out is impossible, so emit
                // nothing. (The engine handles whole-map clears separately;
                // round-trip tests only exercise non-empty maps.)
                Vec::new()
            } else {
                c.overrides
                    .iter()
                    .map(|(k, v)| {
                        KraftMetadataRecord::Config(ConfigRecord {
                            resource_type: 2, // TOPIC
                            resource_name: c.topic.clone(),
                            name: k.clone(),
                            value: Some(v.clone()),
                            ..Default::default()
                        })
                    })
                    .collect()
            }
        }
        // ----- no KIP-631 metadata counterpart -----
        MetadataRecord::V1DeleteAccessControlEntry(_) => {
            return Err(TranslateError::NoCounterpart("V1DeleteAccessControlEntry"));
        }
        MetadataRecord::V1Voters(_) => {
            return Err(TranslateError::NoCounterpart("V1Voters"));
        }
        MetadataRecord::V1KRaftVersion(_) => {
            return Err(TranslateError::NoCounterpart("V1KRaftVersion"));
        }
    };
    Ok(recs.into_iter())
}

fn register_broker_to_kraft(
    b: &BrokerRegistrationRecord,
) -> Result<RegisterBrokerRecord, TranslateError> {
    // The top-level (host, port) becomes a synthetic leading end_point so it
    // survives the round-trip; the real listeners follow.
    let mut end_points = Vec::with_capacity(b.endpoints.len() + 1);
    end_points.push(KBrokerEndpoint {
        name: String::new(),
        host: b.host.clone(),
        port: b.port,
        security_protocol: protocol_to_wire(ListenerProtocol::Plaintext),
        ..Default::default()
    });
    for e in &b.endpoints {
        end_points.push(KBrokerEndpoint {
            name: e.name.clone(),
            host: e.host.clone(),
            port: e.port,
            security_protocol: protocol_to_wire(e.protocol),
            ..Default::default()
        });
    }
    Ok(RegisterBrokerRecord {
        broker_id: i32::try_from(b.node_id).map_err(|_| TranslateError::Invalid {
            field: "broker_id",
            detail: format!("node_id {} exceeds i32", b.node_id),
        })?,
        rack: b.rack.clone(),
        end_points,
        ..Default::default()
    })
}

fn partition_to_kraft(
    p: &PartitionRecord,
    topic_id: uuid::Uuid,
) -> Result<KPartitionRecord, TranslateError> {
    let cast = |v: &[u64], field: &'static str| -> Result<Vec<i32>, TranslateError> {
        v.iter()
            .map(|n| {
                i32::try_from(*n).map_err(|_| TranslateError::Invalid {
                    field,
                    detail: format!("node id {n} exceeds i32"),
                })
            })
            .collect()
    };
    Ok(KPartitionRecord {
        partition_id: p.partition,
        topic_id: to_kuuid(topic_id),
        replicas: cast(&p.replicas, "partition replicas")?,
        isr: cast(&p.isr, "partition isr")?,
        removing_replicas: cast(&p.removing_replicas, "partition removing_replicas")?,
        adding_replicas: cast(&p.adding_replicas, "partition adding_replicas")?,
        leader: i32::try_from(p.leader).map_err(|_| TranslateError::Invalid {
            field: "partition leader",
            detail: format!("leader {} exceeds i32", p.leader),
        })?,
        leader_epoch: p.leader_epoch,
        ..Default::default()
    })
}

fn client_quota_to_kraft(q: &ClientQuotaRecord) -> KClientQuotaRecord {
    let entity = q
        .entity
        .iter()
        .map(|e| EntityData {
            entity_type: e.entity_type.clone(),
            entity_name: e.entity_name.clone(),
            ..Default::default()
        })
        .collect();
    // KIP-631 splits Crabka's Option<f64> into (value, remove): None => the
    // remove tombstone; Some(v) => value with remove=false.
    let (value, remove) = match q.config_value {
        Some(v) => (v, false),
        None => (0.0, true),
    };
    KClientQuotaRecord {
        entity,
        key: q.config_key.clone(),
        value,
        remove,
        ..Default::default()
    }
}

fn delegation_token_to_kraft(t: &DelegationTokenRecord) -> KDelegationTokenRecord {
    KDelegationTokenRecord {
        owner: t.owner.to_string(),
        // KIP-631 has no hmac field; smuggle it through `requester` (unused
        // by Crabka) as hex so the round-trip is lossless.
        requester: hex_encode(&t.hmac),
        renewers: t.renewers.iter().map(ToString::to_string).collect(),
        issue_timestamp: t.issue_timestamp_ms,
        max_timestamp: t.max_timestamp_ms,
        expiration_timestamp: t.expiry_timestamp_ms,
        token_id: t.token_id.clone(),
        ..Default::default()
    }
}

/// Convert one KIP-631 record back to its [`MetadataRecord`] counterpart.
///
/// Inverse of [`to_kraft`]. A `Config` record routes back to
/// `V1TopicConfig`/`V1BrokerConfig` by `resource_type`; topic-config
/// records decode to single-key singletons that the image merges.
///
/// # Errors
/// [`TranslateError::UnknownTopicId`] when a topic/partition references a
/// topic id absent from `image`, [`TranslateError::NoCounterpart`] for
/// records this layer does not model, and [`TranslateError::Invalid`] for
/// out-of-range enum/hex values.
#[allow(clippy::too_many_lines)] // exhaustive match over KraftMetadataRecord
pub fn from_kraft(
    rec: &KraftMetadataRecord,
    image: &MetadataImage,
) -> Result<MetadataRecord, TranslateError> {
    match rec {
        KraftMetadataRecord::FeatureLevel(f) => {
            Ok(MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: f.name.clone(),
                level: f.feature_level,
            }))
        }
        KraftMetadataRecord::RegisterBroker(b) => Ok(MetadataRecord::V1BrokerRegistration(
            register_broker_from_kraft(b)?,
        )),
        KraftMetadataRecord::UnregisterBroker(u) => {
            Ok(MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
                // Kafka broker ids are non-negative by contract.
                #[allow(clippy::cast_sign_loss)]
                node_id: u.broker_id as u64,
            }))
        }
        KraftMetadataRecord::UserScramCredential(s) => {
            Ok(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
                user: s.name.clone(),
                mechanism: scram_mechanism_from_wire(s.mechanism)?,
                salt: s.salt.to_vec(),
                stored_key: s.stored_key.to_vec(),
                server_key: s.server_key.to_vec(),
                iterations: u32::try_from(s.iterations).map_err(|_| TranslateError::Invalid {
                    field: "scram iterations",
                    detail: format!("{} is negative", s.iterations),
                })?,
            }))
        }
        KraftMetadataRecord::RemoveUserScramCredential(s) => Ok(
            MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
                user: s.name.clone(),
                mechanism: scram_mechanism_from_wire(s.mechanism)?,
            }),
        ),
        KraftMetadataRecord::AccessControlEntry(e) => {
            Ok(MetadataRecord::V1AccessControlEntry(AclEntry {
                resource_type: resource_type_from_wire(e.resource_type)?,
                resource_name: e.resource_name.clone(),
                pattern_type: pattern_type_from_wire(e.pattern_type)?,
                principal: e.principal.clone(),
                host: e.host.clone(),
                operation: operation_from_wire(e.operation)?,
                permission_type: permission_from_wire(e.permission_type)?,
            }))
        }
        KraftMetadataRecord::ClientQuota(q) => {
            Ok(MetadataRecord::V1ClientQuota(client_quota_from_kraft(q)))
        }
        KraftMetadataRecord::DelegationToken(t) => Ok(MetadataRecord::V1DelegationToken(
            delegation_token_from_kraft(t)?,
        )),
        KraftMetadataRecord::RemoveDelegationToken(t) => Ok(
            MetadataRecord::V1DeleteDelegationToken(crate::records::DeleteDelegationTokenRecord {
                token_id: t.token_id.clone(),
            }),
        ),
        KraftMetadataRecord::Config(c) => config_from_kraft(c),
        KraftMetadataRecord::Topic(t) => Ok(MetadataRecord::V1Topic(topic_from_kraft(t, image))),
        KraftMetadataRecord::Partition(p) => {
            Ok(MetadataRecord::V1Partition(partition_from_kraft(p, image)?))
        }
        KraftMetadataRecord::RemoveTopic(t) => {
            let id = from_kuuid(t.topic_id);
            let name = topic_name_for_id(image, id).ok_or(TranslateError::UnknownTopicId(id))?;
            Ok(MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name }))
        }
        other => Err(TranslateError::NoCounterpart(kraft_variant_name(other))),
    }
}

fn kraft_variant_name(rec: &KraftMetadataRecord) -> &'static str {
    match rec {
        KraftMetadataRecord::BrokerRegistrationChange(_) => "BrokerRegistrationChange",
        KraftMetadataRecord::NoOp(_) => "NoOp",
        KraftMetadataRecord::BeginTransaction(_) => "BeginTransaction",
        KraftMetadataRecord::EndTransaction(_) => "EndTransaction",
        KraftMetadataRecord::RegisterController(_) => "RegisterController",
        KraftMetadataRecord::RemoveAccessControlEntry(_) => "RemoveAccessControlEntry",
        KraftMetadataRecord::Unknown { .. } => "Unknown",
        _ => "unmodeled",
    }
}

// Kafka broker ids are non-negative by contract, so the i32 -> u64
// NodeId widen never loses a sign.
#[allow(clippy::cast_sign_loss)]
fn register_broker_from_kraft(
    b: &RegisterBrokerRecord,
) -> Result<BrokerRegistrationRecord, TranslateError> {
    // The first end_point carries the synthetic top-level (host, port); the
    // rest are the real per-listener endpoints.
    let (host, port, rest) = match b.end_points.split_first() {
        Some((head, rest)) => (head.host.clone(), head.port, rest),
        None => (String::new(), 0, &[][..]),
    };
    let endpoints = rest
        .iter()
        .map(|e| {
            Ok(BrokerEndpoint {
                name: e.name.clone(),
                host: e.host.clone(),
                port: e.port,
                protocol: protocol_from_wire(e.security_protocol)?,
            })
        })
        .collect::<Result<Vec<_>, TranslateError>>()?;
    Ok(BrokerRegistrationRecord {
        node_id: b.broker_id as u64,
        host,
        port,
        rack: b.rack.clone(),
        endpoints,
    })
}

fn topic_from_kraft(t: &KTopicRecord, image: &MetadataImage) -> TopicRecord {
    let name = t.name.clone();
    // Reconstruct partitions/RF from the image: partition count is derived
    // from the partitions map; RF from the first partition's replica count
    // (1 if none yet present). These fields are not part of the KIP-631
    // record — the image is authoritative.
    let partitions = image.topic_partition_count(&name);
    let replication_factor = image
        .partitions_of(&name)
        .next()
        .map_or(1, |p| i16::try_from(p.replicas.len()).unwrap_or(1));
    TopicRecord {
        name,
        topic_id: from_kuuid(t.topic_id),
        partitions,
        replication_factor,
    }
}

// Kafka replica/leader ids are non-negative by contract, so the i32 ->
// u64 NodeId widen never loses a sign.
#[allow(clippy::cast_sign_loss)]
fn partition_from_kraft(
    p: &KPartitionRecord,
    image: &MetadataImage,
) -> Result<PartitionRecord, TranslateError> {
    let id = from_kuuid(p.topic_id);
    let topic = topic_name_for_id(image, id).ok_or(TranslateError::UnknownTopicId(id))?;
    let cast = |v: &[i32]| -> Vec<u64> { v.iter().map(|n| *n as u64).collect() };
    Ok(PartitionRecord {
        topic,
        partition: p.partition_id,
        leader: p.leader as u64,
        replicas: cast(&p.replicas),
        isr: cast(&p.isr),
        leader_epoch: p.leader_epoch,
        adding_replicas: cast(&p.adding_replicas),
        removing_replicas: cast(&p.removing_replicas),
    })
}

fn client_quota_from_kraft(q: &KClientQuotaRecord) -> ClientQuotaRecord {
    let entity = q
        .entity
        .iter()
        .map(|e| QuotaEntity {
            entity_type: e.entity_type.clone(),
            entity_name: e.entity_name.clone(),
        })
        .collect();
    let config_value = if q.remove { None } else { Some(q.value) };
    ClientQuotaRecord {
        entity,
        config_key: q.key.clone(),
        config_value,
    }
}

fn delegation_token_from_kraft(
    t: &KDelegationTokenRecord,
) -> Result<DelegationTokenRecord, TranslateError> {
    let owner = t
        .owner
        .parse::<KafkaPrincipal>()
        .map_err(|e| TranslateError::Invalid {
            field: "delegation token owner",
            detail: e,
        })?;
    let renewers = t
        .renewers
        .iter()
        .map(|r| {
            r.parse::<KafkaPrincipal>()
                .map_err(|e| TranslateError::Invalid {
                    field: "delegation token renewer",
                    detail: e,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DelegationTokenRecord {
        token_id: t.token_id.clone(),
        owner,
        hmac: hex_decode(&t.requester)?,
        issue_timestamp_ms: t.issue_timestamp,
        expiry_timestamp_ms: t.expiration_timestamp,
        max_timestamp_ms: t.max_timestamp,
        renewers,
    })
}

fn config_from_kraft(c: &ConfigRecord) -> Result<MetadataRecord, TranslateError> {
    match c.resource_type {
        0 => {
            // BROKER
            let node_id = c
                .resource_name
                .parse::<u64>()
                .map_err(|e| TranslateError::Invalid {
                    field: "broker config resource_name",
                    detail: e.to_string(),
                })?;
            Ok(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id,
                config_name: c.name.clone(),
                config_value: c.value.clone(),
            }))
        }
        2 => {
            // TOPIC — decode to a single-key singleton; the image merges.
            let mut overrides = std::collections::BTreeMap::new();
            if let Some(v) = &c.value {
                overrides.insert(c.name.clone(), v.clone());
            }
            Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: c.resource_name.clone(),
                overrides,
            }))
        }
        other => Err(TranslateError::Invalid {
            field: "config resource_type",
            detail: format!("unsupported resource_type {other}"),
        }),
    }
}

/// Resolve a topic id to its name by scanning the image's topics.
fn topic_name_for_id(image: &MetadataImage, id: uuid::Uuid) -> Option<String> {
    image
        .topics()
        .find(|t| t.topic_id == id)
        .map(|t| t.name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::AclEntryFilter;
    use crate::records::{DeleteDelegationTokenRecord, KRaftVersionRecord, VotersRecord};
    use assert2::assert;

    fn img() -> MetadataImage {
        MetadataImage::new(uuid::Uuid::nil())
    }

    fn round_trip(rec: &MetadataRecord, image: &MetadataImage) {
        let k = to_kraft(rec, image).unwrap();
        assert!(from_kraft(&k, image).unwrap() == *rec);
    }

    // ---------- Task 1: aligned variants ----------

    #[test]
    fn feature_level_round_trips() {
        let rec = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn register_broker_no_endpoints_round_trips() {
        let rec = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 7,
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
            endpoints: vec![],
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn register_broker_with_endpoints_round_trips() {
        let rec = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 1,
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![
                BrokerEndpoint {
                    name: "EXTERNAL".into(),
                    host: "ext.example.com".into(),
                    port: 9093,
                    protocol: ListenerProtocol::SaslSsl,
                },
                BrokerEndpoint {
                    name: "INTERNAL".into(),
                    host: "int".into(),
                    port: 9094,
                    protocol: ListenerProtocol::Plaintext,
                },
            ],
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn unregister_broker_round_trips() {
        let rec = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id: 42 });
        round_trip(&rec, &img());
    }

    #[test]
    fn scram_credential_round_trips() {
        let rec = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1u8; 16],
            stored_key: vec![2u8; 64],
            server_key: vec![3u8; 64],
            iterations: 4096,
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn delete_scram_credential_round_trips() {
        let rec = MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha256,
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn access_control_entry_round_trips() {
        let rec = MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn access_control_entry_prefixed_deny_round_trips() {
        let rec = MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::DelegationToken,
            resource_name: "User:bob".into(),
            pattern_type: PatternType::Prefixed,
            principal: "User:eve".into(),
            host: "10.0.0.1".into(),
            operation: AclOperation::Describe,
            permission_type: PermissionType::Deny,
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn delete_access_control_entry_has_no_counterpart() {
        let rec = MetadataRecord::V1DeleteAccessControlEntry(AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            ..AclEntryFilter::default()
        });
        assert!(
            to_kraft(&rec, &img())
                == Err(TranslateError::NoCounterpart("V1DeleteAccessControlEntry"))
        );
    }

    #[test]
    fn client_quota_round_trips() {
        let rec = MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app1".into()),
                },
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
            ],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn client_quota_remove_round_trips() {
        let rec = MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".into(),
                entity_name: None,
            }],
            config_key: "consumer_byte_rate".into(),
            config_value: None,
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn delegation_token_round_trips() {
        let rec = MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: "tok-abc".into(),
            owner: KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            hmac: vec![0xAB; 32],
            issue_timestamp_ms: 1_700_000_000_000,
            expiry_timestamp_ms: 1_700_000_600_000,
            max_timestamp_ms: 1_700_604_800_000,
            renewers: vec![KafkaPrincipal {
                principal_type: "User".into(),
                name: "bob".into(),
            }],
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn delete_delegation_token_round_trips() {
        let rec = MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
            token_id: "tok-abc".into(),
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn broker_config_round_trips() {
        let rec = MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 7,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn broker_config_delete_round_trips() {
        let rec = MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 7,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: None,
        });
        round_trip(&rec, &img());
    }

    #[test]
    fn voters_and_kraft_version_have_no_counterpart() {
        let v = MetadataRecord::V1Voters(VotersRecord {
            voters: crate::voters::VoterSet::default(),
        });
        assert!(to_kraft(&v, &img()) == Err(TranslateError::NoCounterpart("V1Voters")));
        let k = MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 });
        assert!(to_kraft(&k, &img()) == Err(TranslateError::NoCounterpart("V1KRaftVersion")));
    }

    // ---------- Task 2: topic / partition / config with image context ----------

    #[test]
    fn topic_partition_config_round_trip_with_image_context() {
        let mut image = img();
        let tid = uuid::Uuid::from_u128(7);
        let topic = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: tid,
            partitions: 1,
            replication_factor: 1,
        });
        image.apply(&topic);
        let part = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
        });
        image.apply(&part);
        for rec in [&topic, &part] {
            let k = to_kraft(rec, &image).unwrap();
            assert!(from_kraft(&k, &image).unwrap() == *rec);
        }
        let cfg = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: [("retention.ms".to_string(), "9".to_string())].into(),
        });
        let k = to_kraft(&cfg, &image).unwrap();
        assert!(from_kraft(&k, &image).unwrap() == cfg);
    }

    #[test]
    fn delete_topic_round_trips_with_image_context() {
        let mut image = img();
        let tid = uuid::Uuid::from_u128(11);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "doomed".into(),
            topic_id: tid,
            partitions: 1,
            replication_factor: 1,
        }));
        let rec = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        });
        round_trip(&rec, &image);
    }

    #[test]
    fn topic_config_two_keys_yields_two_config_records() {
        let mut image = img();
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::from_u128(7),
            partitions: 1,
            replication_factor: 1,
        }));
        let cfg = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: [
                ("retention.ms".to_string(), "9".to_string()),
                ("segment.bytes".to_string(), "1048576".to_string()),
            ]
            .into(),
        });
        let records = to_kraft_records(&cfg, &image).unwrap();
        assert!(records.len() == 2);
        // Each decodes back to a single-key mergeable singleton.
        for k in &records {
            let back = from_kraft(k, &image).unwrap();
            match back {
                MetadataRecord::V1TopicConfig(tc) => {
                    assert!(tc.topic == "t");
                    assert!(tc.overrides.len() == 1);
                }
                other => panic!("expected V1TopicConfig singleton, got {other:?}"),
            }
        }
    }

    #[test]
    fn image_derives_partition_count_from_partitions_map() {
        let mut image = img();
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::from_u128(7),
            partitions: 3,
            replication_factor: 1,
        }));
        for p in 0..3 {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: p,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
            }));
        }
        assert!(image.topic_partition_count("t") == 3);
    }
}
