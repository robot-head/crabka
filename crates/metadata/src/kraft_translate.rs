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
//!   [`KraftMetadataRecord`] dispatch and are never submitted through the
//!   translation boundary in this slice (reconfiguration is Slice 5), so
//!   [`to_kraft`] returns [`TranslateError::NoCounterpart`] for them.
//!
//! ## Fan-out / image-keyed mappings
//!
//! - `V1TopicConfig` is whole-map authoritative; KIP-631 has only per-key
//!   `ConfigRecord`s. [`to_kraft_records`] diffs the new map against the
//!   image's current overrides and emits a *set* per present key plus a
//!   *tombstone* (`value: None`) per dropped key; [`from_kraft`] merges one
//!   key's delta back onto the image map and re-emits the whole map (so
//!   `MetadataImage::apply`'s replace semantics are unchanged). Records must
//!   apply in log order against a live image.
//! - `V1DeleteAccessControlEntry(AclEntryFilter)`: KIP-631's
//!   `RemoveAccessControlEntryRecord` deletes by `id: Uuid` only. Crabka does
//!   not model ACL ids, so [`to_kraft_records`] resolves the filter against
//!   the image to the matching entries and emits one `RemoveAccessControlEntry`
//!   per match, keyed by a deterministic content-hash id (the same id the add
//!   path stamps); [`from_kraft`] rescans the image by that hash to recover the
//!   entry and emit a pinned filter. Self-consistent within Crabka; Slice 6
//!   swaps in Kafka's real id scheme for cross-impl fidelity.
//!
//! ## Lossy / awkward mappings (documented)
//!
//! - `RegisterBroker`: Crabka stores a top-level `(host, port)` plus a
//!   per-listener `endpoints` list; KIP-631 has only `end_points` (no
//!   top-level host/port). The top-level pair is encoded as a synthetic
//!   leading `end_points` entry (name `""`, `security_protocol` PLAINTEXT)
//!   and split back out on decode; the real listeners follow. All the
//!   other KIP-631 extras (`incarnation_id`, `features`, `fenced`, …) are
//!   defaulted on encode and dropped on decode. `broker_epoch` IS carried
//!   (KIP-903 ISR fencing).
//! - `DelegationToken`: KIP-631's record has no `hmac` field. The HMAC is
//!   hex-encoded into the otherwise-unused `requester` slot so it
//!   round-trips; `KafkaPrincipal`s map through their `Type:Name` string
//!   form.

use bytes::Bytes;
use wincode::{Deserialize as _, Serialize as _};

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
use crabka_protocol::owned::remove_access_control_entry_record::RemoveAccessControlEntryRecord;
use crabka_protocol::owned::remove_topic_record::RemoveTopicRecord;
use crabka_protocol::owned::remove_user_scram_credential_record::RemoveUserScramCredentialRecord;
use crabka_protocol::owned::topic_record::TopicRecord as KTopicRecord;
use crabka_protocol::owned::unregister_broker_record::UnregisterBrokerRecord as KUnregisterBrokerRecord;
use crabka_protocol::owned::user_scram_credential_record::UserScramCredentialRecord;
use crabka_protocol::primitives::uuid::Uuid as KUuid;
use crabka_protocol::records::metadata::KraftMetadataRecord;

use crabka_security::{KafkaPrincipal, ListenerProtocol, SaslMechanism};

use crate::MetadataImage;
use crate::acl::{
    AclEntry, AclEntryFilter, AclOperation, PatternType, PermissionType, ResourceType,
};
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
    #[error("KIP-631 value encode failed: {0}")]
    Encode(String),
    #[error("KIP-631 value decode failed: {0}")]
    Decode(String),
    /// A `RemoveAccessControlEntry{{id}}` referenced an ACL id absent from the
    /// image. On the Crabka-only path the matching add always precedes the
    /// remove in log order, so this means a corrupt / out-of-order log.
    #[error("unknown ACL id {0} on decode")]
    UnknownAclId(uuid::Uuid),
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

// ----- ACL content-hash id (KIP-631 keys ACLs by Uuid; Crabka keys by content) -----
//
// KIP-631's `AccessControlEntryRecord` carries a controller-assigned `id`, and
// `RemoveAccessControlEntryRecord` deletes by that id alone. Crabka's image does
// not model ACL ids (it keys entries by their full tuple), so to keep the log
// genuinely id-keyed and round-trippable we synthesize a *deterministic*
// 128-bit id from the entry's canonical content. The add path stamps this id;
// the delete path resolves a filter to matching entries and emits one
// `RemoveAccessControlEntry{id}` each; decode rescans the image by the same hash
// to recover the entry. The derivation is self-consistent within Crabka — Slice
// 6 replaces it with Kafka's real id scheme for cross-impl fidelity.
fn acl_id(e: &AclEntry) -> KUuid {
    // Canonical byte encoding: the four enum axes (i8) then the three string
    // axes, each length-prefixed and separated, so distinct entries never alias.
    let mut buf: Vec<u8> = vec![
        resource_type_to_wire(e.resource_type).cast_unsigned(),
        pattern_type_to_wire(e.pattern_type).cast_unsigned(),
        operation_to_wire(e.operation).cast_unsigned(),
        permission_to_wire(e.permission_type).cast_unsigned(),
    ];
    for s in [&e.resource_name, &e.principal, &e.host] {
        buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    }
    KUuid(fnv1a_128(&buf))
}

/// FNV-1a over 128 bits: two independent 64-bit FNV-1a lanes (distinct offset
/// bases) concatenated. Deterministic and dependency-free (unlike `DefaultHasher`,
/// whose output is not guaranteed stable across toolchains).
fn fnv1a_128(bytes: &[u8]) -> [u8; 16] {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let lane = |mut h: u64| -> u64 {
        for b in bytes {
            h ^= u64::from(*b);
            h = h.wrapping_mul(PRIME);
        }
        h
    };
    let hi = lane(0xcbf2_9ce4_8422_2325);
    let lo = lane(0x1099_5663_8a1b_c7e9);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&hi.to_be_bytes());
    out[8..].copy_from_slice(&lo.to_be_bytes());
    out
}

/// A filter pinned to exactly one entry (every axis set). Used on decode of a
/// `RemoveAccessControlEntry` so `apply` removes precisely the resolved entry.
fn exact_filter(e: &AclEntry) -> AclEntryFilter {
    AclEntryFilter {
        resource_type: Some(e.resource_type),
        resource_name: Some(e.resource_name.clone()),
        pattern_type: Some(e.pattern_type),
        principal: Some(e.principal.clone()),
        host: Some(e.host.clone()),
        operation: Some(e.operation),
        permission_type: Some(e.permission_type),
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

/// The engine/snapshot framing entrypoint: translate one [`MetadataRecord`] to
/// the KIP-631 value byte blobs to put on the log — one per fanned-out record
/// (most records yield one; a `V1TopicConfig` yields one per key ± tombstones,
/// a `V1DeleteAccessControlEntry` one per matched ACL), each enveloped at
/// apiVersion 0. Yields an empty `Vec` when a record fans out to nothing (an
/// empty config clear with no prior keys, or a delete-ACL filter matching
/// nothing) — callers treat an all-empty batch as a committed no-op.
///
/// apiVersion 0 is the "defaulted KIP-631 framing" of Slice 3d-2: the core
/// fields Crabka populates all exist at v0; the higher-version KIP-631 extras
/// (broker incarnation/epoch, partition ELR, …) stay defaulted. Slice 6 lifts
/// the version for cross-impl JVM fidelity.
///
/// # Errors
/// Propagates [`to_kraft_records`] errors, plus [`TranslateError::Encode`] if
/// the KIP-631 value encoder fails.
pub fn to_kraft_values(
    rec: &MetadataRecord,
    image: &MetadataImage,
) -> Result<Vec<Bytes>, TranslateError> {
    to_kraft_records(rec, image)?
        .iter()
        .map(|kr| {
            kr.encode_value(0)
                .map_err(|e| TranslateError::Encode(e.to_string()))
        })
        .collect()
}

/// The inverse framing entrypoint: decode one KIP-631 value blob back to a
/// [`MetadataRecord`], resolving image-keyed fields (topic ids, the whole-map
/// config merge, ACL ids) against `image`. The caller must apply decoded
/// records to `image` in log order so each subsequent decode sees the
/// up-to-date state.
///
/// # Errors
/// [`TranslateError::Decode`] if the envelope/body is malformed, plus any
/// [`from_kraft`] error (unknown topic/ACL id, unmodeled record).
pub fn from_kraft_value(
    value: &[u8],
    image: &MetadataImage,
) -> Result<MetadataRecord, TranslateError> {
    let (rec, _version) = KraftMetadataRecord::decode_value(value)
        .map_err(|e| TranslateError::Decode(e.to_string()))?;
    from_kraft(&rec, image)
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
                    id: acl_id(e),
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
            // Crabka's `V1TopicConfig` is whole-map authoritative; KIP-631 has
            // only per-key `ConfigRecord`s. Diff the new map against the image's
            // current overrides: emit a *set* for every key in the new map and a
            // *tombstone* (`value: None`) for every key the new map drops. Empty
            // new map + existing keys => all tombstones (a clear). The records
            // apply in order against a live image (engine + snapshot), so the
            // merged result reproduces the new map exactly.
            let empty = std::collections::BTreeMap::new();
            let old = image.topic_config(&c.topic).unwrap_or(&empty);
            let mut out = Vec::with_capacity(c.overrides.len());
            for (k, v) in &c.overrides {
                out.push(KraftMetadataRecord::Config(ConfigRecord {
                    resource_type: 2, // TOPIC
                    resource_name: c.topic.clone(),
                    name: k.clone(),
                    value: Some(v.clone()),
                    ..Default::default()
                }));
            }
            for k in old.keys() {
                if !c.overrides.contains_key(k) {
                    out.push(KraftMetadataRecord::Config(ConfigRecord {
                        resource_type: 2, // TOPIC
                        resource_name: c.topic.clone(),
                        name: k.clone(),
                        value: None, // tombstone: removed key
                        ..Default::default()
                    }));
                }
            }
            out
        }
        MetadataRecord::V1DeleteAccessControlEntry(filter) => {
            // KIP-631 deletes ACLs by id. Resolve the filter against the image
            // to the concrete matching entries and emit one
            // `RemoveAccessControlEntry{id}` per match (id = content-hash, the
            // same id the add path stamped). A filter matching nothing yields no
            // records (the original apply is a no-op anyway).
            image
                .all_acls()
                .filter(|e| filter.matches(e))
                .map(|e| {
                    KraftMetadataRecord::RemoveAccessControlEntry(RemoveAccessControlEntryRecord {
                        id: acl_id(e),
                        ..Default::default()
                    })
                })
                .collect()
        }
        // ----- records main's KIP-584/KIP-714 work added with no KIP-631 wire
        // counterpart yet: carry verbatim (wincode body) in an Unknown envelope
        // so Crabka-only logs/snapshots round-trip. They never reach a JVM peer
        // in the static-quorum interop path (no snapshot fetch / no client-metrics
        // there); full KIP-631 fidelity (real ClientMetricsRecord; deriving the
        // features epoch from FeatureLevel offsets) is future work. -----
        MetadataRecord::V1ClientMetricsConfig(_) => {
            vec![wincode_carrier(rec, PRIVATE_CLIENT_METRICS_KEY)?]
        }
        MetadataRecord::V1FeaturesEpoch(_) => {
            vec![wincode_carrier(rec, PRIVATE_FEATURES_EPOCH_KEY)?]
        }
        // ----- no KIP-631 metadata counterpart -----
        MetadataRecord::V1Voters(_) => {
            return Err(TranslateError::NoCounterpart("V1Voters"));
        }
        MetadataRecord::V1KRaftVersion(_) => {
            return Err(TranslateError::NoCounterpart("V1KRaftVersion"));
        }
    };
    Ok(recs.into_iter())
}

/// Crabka-private metadata-record apiKeys (well outside Kafka's real
/// non-sequential range 0..=27) for records carried verbatim through the
/// KIP-631 `Unknown` envelope. NOT wire-faithful to a JVM peer — Crabka-only
/// round-trip carriers.
const PRIVATE_CLIENT_METRICS_KEY: u32 = 1000;
const PRIVATE_FEATURES_EPOCH_KEY: u32 = 1001;

/// Wrap a wincode-serialized `MetadataRecord` in an `Unknown` KIP-631 envelope
/// under a Crabka-private apiKey, so it round-trips byte-faithfully through the
/// KIP-631 log/snapshot without a real KIP-631 schema.
fn wincode_carrier(
    rec: &MetadataRecord,
    api_key: u32,
) -> Result<KraftMetadataRecord, TranslateError> {
    let body = <serde_wincode::SerdeCompat<MetadataRecord>>::serialize(rec)
        .map_err(|e| TranslateError::Encode(e.to_string()))?;
    Ok(KraftMetadataRecord::Unknown {
        api_key,
        api_version: 0,
        body: bytes::Bytes::from(body),
    })
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
        broker_epoch: b.broker_epoch,
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
        KraftMetadataRecord::Config(c) => config_from_kraft(c, image),
        KraftMetadataRecord::Topic(t) => Ok(MetadataRecord::V1Topic(topic_from_kraft(t, image))),
        KraftMetadataRecord::Partition(p) => {
            Ok(MetadataRecord::V1Partition(partition_from_kraft(p, image)?))
        }
        KraftMetadataRecord::RemoveTopic(t) => {
            let id = from_kuuid(t.topic_id);
            let name = topic_name_for_id(image, id).ok_or(TranslateError::UnknownTopicId(id))?;
            Ok(MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name }))
        }
        KraftMetadataRecord::RemoveAccessControlEntry(r) => {
            // Resolve the id back to the concrete entry by rescanning the image
            // for the entry whose content-hash matches, then emit a pinned
            // (every-axis) filter so `apply` removes exactly that entry. The
            // matching add always precedes this remove in log order, so the
            // entry is present when this decodes.
            let entry = image
                .all_acls()
                .find(|e| acl_id(e) == r.id)
                .ok_or_else(|| TranslateError::UnknownAclId(from_kuuid(r.id)))?;
            Ok(MetadataRecord::V1DeleteAccessControlEntry(exact_filter(
                entry,
            )))
        }
        // Crabka-private carriers (see `wincode_carrier`): decode the verbatim
        // wincode body back to the original record.
        KraftMetadataRecord::Unknown { api_key, body, .. }
            if *api_key == PRIVATE_CLIENT_METRICS_KEY || *api_key == PRIVATE_FEATURES_EPOCH_KEY =>
        {
            <serde_wincode::SerdeCompat<MetadataRecord>>::deserialize(body)
                .map_err(|e| TranslateError::Decode(e.to_string()))
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
        broker_epoch: b.broker_epoch,
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

fn config_from_kraft(
    c: &ConfigRecord,
    image: &MetadataImage,
) -> Result<MetadataRecord, TranslateError> {
    match c.resource_type {
        0 => {
            // BROKER — `V1BrokerConfig` is already per-key (Some=set, None=delete),
            // so a single `ConfigRecord` maps 1:1 (the image merges per-key).
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
            // TOPIC — `V1TopicConfig` is whole-map (replace) authoritative, so
            // merge this one key's delta onto the image's current overrides and
            // emit the resulting whole map. `Some(v)` sets the key; `None` (a
            // tombstone) removes it. Applying the emitted record (replace) lands
            // the merged map. Decoding successive Config records against the
            // live image (engine apply / snapshot read) reproduces the original
            // whole map exactly.
            let mut overrides = image
                .topic_config(&c.resource_name)
                .cloned()
                .unwrap_or_default();
            match &c.value {
                Some(v) => {
                    overrides.insert(c.name.clone(), v.clone());
                }
                None => {
                    overrides.remove(&c.name);
                }
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
    use crate::records::{
        ClientMetricsConfigRecord, DeleteDelegationTokenRecord, FeaturesEpochRecord,
        KRaftVersionRecord, VotersRecord,
    };
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
        let image = img();
        let rec = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 7,
            broker_epoch: 42,
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
            endpoints: vec![],
        });
        let k = to_kraft(&rec, &image).unwrap();
        let decoded = from_kraft(&k, &image).unwrap();
        let MetadataRecord::V1BrokerRegistration(out) = &decoded else {
            panic!("expected V1BrokerRegistration, got {decoded:?}");
        };
        assert!(
            out.broker_epoch == 42,
            "broker_epoch lost on round-trip: {}",
            out.broker_epoch
        );
        assert!(decoded == rec);
    }

    #[test]
    fn register_broker_with_endpoints_round_trips() {
        let image = img();
        let rec = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 1,
            broker_epoch: 7,
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
        let k = to_kraft(&rec, &image).unwrap();
        let decoded = from_kraft(&k, &image).unwrap();
        let MetadataRecord::V1BrokerRegistration(out) = &decoded else {
            panic!("expected V1BrokerRegistration, got {decoded:?}");
        };
        assert!(
            out.broker_epoch == 7,
            "broker_epoch lost on round-trip: {}",
            out.broker_epoch
        );
        assert!(decoded == rec);
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

    fn acl(rt: ResourceType, name: &str, principal: &str) -> AclEntry {
        AclEntry {
            resource_type: rt,
            resource_name: name.into(),
            pattern_type: PatternType::Literal,
            principal: principal.into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }
    }

    #[test]
    fn distinct_acls_get_distinct_ids() {
        let a = acl(ResourceType::Topic, "foo", "User:alice");
        let b = acl(ResourceType::Topic, "foo", "User:bob");
        let c = acl(ResourceType::Group, "foo", "User:alice");
        assert!(acl_id(&a) != acl_id(&b));
        assert!(acl_id(&a) != acl_id(&c));
        // Deterministic: same entry hashes identically.
        assert!(acl_id(&a) == acl_id(&a.clone()));
    }

    #[test]
    fn delete_acl_filter_resolves_to_remove_records_and_round_trips() {
        // Image holds three ACLs; a Topic-only filter deletes the two Topic
        // entries and leaves the Group entry.
        let mut image = img();
        let e1 = acl(ResourceType::Topic, "foo", "User:alice");
        let e2 = acl(ResourceType::Topic, "bar", "User:bob");
        let e3 = acl(ResourceType::Group, "cg", "User:alice");
        for e in [&e1, &e2, &e3] {
            image.apply(&MetadataRecord::V1AccessControlEntry(e.clone()));
        }
        let del = MetadataRecord::V1DeleteAccessControlEntry(AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            ..AclEntryFilter::default()
        });
        let records = to_kraft_records(&del, &image).unwrap();
        assert!(records.len() == 2); // e1, e2 — not e3
        // Each RemoveAccessControlEntry decodes back to a pinned filter that
        // removes exactly its entry. Applying both clears e1+e2, keeps e3.
        for k in &records {
            let back = from_kraft(k, &image).unwrap();
            image.apply(&back);
        }
        assert!(image.all_acls().count() == 1);
        let survivor = image.all_acls().next().unwrap();
        assert!(*survivor == e3);
    }

    #[test]
    fn delete_acl_filter_matching_nothing_yields_no_records() {
        let mut image = img();
        image.apply(&MetadataRecord::V1AccessControlEntry(acl(
            ResourceType::Topic,
            "foo",
            "User:alice",
        )));
        let del = MetadataRecord::V1DeleteAccessControlEntry(AclEntryFilter {
            resource_type: Some(ResourceType::Group),
            ..AclEntryFilter::default()
        });
        assert!(to_kraft_records(&del, &image).unwrap().is_empty());
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
    fn client_metrics_config_round_trips_via_private_carrier() {
        // KIP-714 client-metrics config has no modeled KIP-631 counterpart yet,
        // so `to_kraft` envelopes it verbatim in a Crabka-private `Unknown`
        // carrier and `from_kraft` decodes it back. Snapshots round-trip it this
        // way; it never reaches the JVM in the static mixed-quorum path.
        let rec = MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "metrics-sub-1".into(),
            configs: [
                ("interval.ms".to_string(), "60000".to_string()),
                ("metrics".to_string(), "org.apache.kafka".to_string()),
            ]
            .into_iter()
            .collect(),
        });
        // The carrier is an `Unknown` record, not a modeled KIP-631 variant.
        let k = to_kraft(&rec, &img()).unwrap();
        assert!(matches!(
            k,
            crabka_protocol::records::metadata::KraftMetadataRecord::Unknown { api_key, .. }
                if api_key == PRIVATE_CLIENT_METRICS_KEY
        ));
        round_trip(&rec, &img());
    }

    #[test]
    fn features_epoch_round_trips_via_private_carrier() {
        // The synthetic `V1FeaturesEpoch` epoch pin is Crabka-internal (no KRaft
        // record); it round-trips through the same private `Unknown` carrier so
        // a snapshot preserves `finalized_features_epoch`.
        let rec = MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord { epoch: 42 });
        let k = to_kraft(&rec, &img()).unwrap();
        assert!(matches!(
            k,
            crabka_protocol::records::metadata::KraftMetadataRecord::Unknown { api_key, .. }
                if api_key == PRIVATE_FEATURES_EPOCH_KEY
        ));
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

    /// Seed `image` with topic `t` and the config map `kv`.
    fn seed_topic_config(image: &mut MetadataImage, kv: &[(&str, &str)]) {
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::from_u128(7),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: kv
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }));
    }

    #[test]
    fn topic_config_change_emits_set_and_tombstone_and_merges_back() {
        // Current config {a:1, b:2}; new config {a:9} (a changed, b dropped).
        let mut image = img();
        seed_topic_config(&mut image, &[("a", "1"), ("b", "2")]);
        let submit = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: [("a".to_string(), "9".to_string())].into(),
        });
        let records = to_kraft_records(&submit, &image).unwrap();
        // One set (a=9) + one tombstone (b removed).
        assert!(records.len() == 2);
        // Apply the fanned records in order against the live image; the merge
        // reproduces the new whole map exactly.
        for k in &records {
            let back = from_kraft(k, &image).unwrap();
            image.apply(&back);
        }
        let want: std::collections::BTreeMap<String, String> =
            [("a".to_string(), "9".to_string())].into();
        assert!(image.topic_config("t") == Some(&want));
    }

    #[test]
    fn topic_config_clear_emits_tombstones_for_every_key() {
        // Current {a:1, b:2}; an empty new map clears all → two tombstones.
        let mut image = img();
        seed_topic_config(&mut image, &[("a", "1"), ("b", "2")]);
        let submit = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: std::collections::BTreeMap::new(),
        });
        let records = to_kraft_records(&submit, &image).unwrap();
        assert!(records.len() == 2);
        for k in &records {
            let back = from_kraft(k, &image).unwrap();
            image.apply(&back);
        }
        // All overrides gone (apply of an empty whole-map removes the entry).
        assert!(image.topic_config("t").is_none());
    }

    #[test]
    fn to_kraft_values_round_trips_through_value_bytes() {
        // The crate framing entrypoints: a record encodes to KIP-631 value
        // bytes and decodes back identically (single-output record).
        let image = img();
        let rec = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        });
        let values = to_kraft_values(&rec, &image).unwrap();
        assert!(values.len() == 1);
        let back = from_kraft_value(&values[0], &image).unwrap();
        assert!(back == rec);
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
