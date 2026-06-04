//! Versioned metadata records. Future versions add variants; older
//! readers can skip unknown ones because we encode each variant
//! length-prefixed inside the `bincode` payload.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub name: String,
    pub topic_id: Uuid,
    pub partitions: i32,
    pub replication_factor: i16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    /// Per-partition leader epoch. Bumped on every leader change.
    /// Older on-disk metadata is not migrated.
    pub leader_epoch: i32,
    /// Replicas being added in an in-flight reassignment. Empty when no
    /// reassignment in flight. KIP-455.
    pub adding_replicas: Vec<NodeId>,
    /// Replicas being removed in an in-flight reassignment. Empty when
    /// no reassignment in flight. KIP-455.
    pub removing_replicas: Vec<NodeId>,
}

/// A single named listener endpoint advertised by a broker. Stored as a
/// list on [`BrokerRegistrationRecord::endpoints`] so KRaft-style metadata
/// can advertise per-listener `host:port`/protocol triples to clients on
/// `Metadata` v9+. Legacy single-listener brokers leave the list empty
/// and rely on the top-level `host`+`port` fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEndpoint {
    /// Listener name (e.g. `"PLAINTEXT"`, `"SSL"`, `"SASL_SSL"`).
    pub name: String,
    pub host: String,
    pub port: u16,
    pub protocol: crabka_security::ListenerProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRegistrationRecord {
    pub node_id: NodeId,
    /// KIP-903 broker epoch: the raft log offset at which this registration
    /// record committed. The controller leader assigns it at append time
    /// (`on_submit_change`); a freshly-built literal carries `0` until the
    /// leader stamps it. Used to fence stale replicas from the ISR on
    /// `AlterPartition`.
    pub broker_epoch: i64,
    /// Legacy single-listener host, used as inter-broker default and by
    /// pre-v9 `Metadata` responses. v9+ projects [`Self::endpoints`].
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    /// Per-listener endpoints. Empty on records written before this
    /// field was added; populated from
    /// `BrokerConfig::effective_listeners()` for self-registration.
    pub endpoints: Vec<BrokerEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicRecord {
    pub name: String,
}

/// KIP-185 / `UnregisterBroker` (`api_key` 64). Marks a broker as
/// permanently unregistered: the admin operator confirms the broker is
/// gone for good and asks the cluster to drop its registration entry
/// from the metadata image. Subsequent `Metadata` responses no longer
/// advertise the broker's endpoints; clients stop routing to it.
///
/// Idempotent — applying twice (or against an unknown `node_id`) is a
/// no-op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnregisterBrokerRecord {
    pub node_id: NodeId,
}

/// Mutable topic configuration overrides. Authoritative target state:
/// each `V1TopicConfig` record fully replaces the previous override map
/// for `topic`. Empty map = clear all overrides. Merging happens at the
/// `AlterConfigs` handler before the record is submitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigRecord {
    pub topic: String,
    pub overrides: std::collections::BTreeMap<String, String>,
}

/// Per-broker configuration key/value pair. `Some(value)` = set; `None` = delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigRecord {
    pub node_id: NodeId,
    pub config_name: String,
    /// `Some(value)` = set; `None` = delete.
    pub config_value: Option<String>,
}

/// KIP-714 client-metrics subscription config. Authoritative target
/// state: each `V1ClientMetricsConfig` fully replaces the previous
/// override map for `name` (the subscription name). Empty map = delete
/// the subscription. Merging happens at the `IncrementalAlterConfigs`
/// handler before the record is submitted (same pattern as
/// [`TopicConfigRecord`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetricsConfigRecord {
    pub name: String,
    pub configs: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaEntity {
    pub entity_type: String,
    pub entity_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientQuotaRecord {
    /// Canonicalized entity tuple — sorted by `entity_type` alphabetically.
    pub entity: Vec<QuotaEntity>,
    pub config_key: String,
    pub config_value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramCredentialRecord {
    pub user: String,
    pub mechanism: crabka_security::SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteScramCredentialRecord {
    pub user: String,
    pub mechanism: crabka_security::SaslMechanism,
}

/// A single delegation token's authoritative state (KIP-48).
/// Replacement semantics — appending a new record with the same
/// `token_id` overwrites the prior one in the image (used by both
/// Create and Renew). Removal goes through
/// [`DeleteDelegationTokenRecord`]. `hmac` is the 32-byte HMAC-SHA-256
/// over `token_id` keyed by the broker's master secret key; clients
/// authenticate via SCRAM-SHA-256 using the hex-encoded HMAC as the
/// password.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationTokenRecord {
    pub token_id: String,
    pub owner: crabka_security::KafkaPrincipal,
    pub hmac: Vec<u8>,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    /// Issue + max-lifetime; renewals cannot push `expiry_timestamp_ms`
    /// past this ceiling.
    pub max_timestamp_ms: i64,
    pub renewers: Vec<crabka_security::KafkaPrincipal>,
}

/// Tombstone record removing a delegation token (KIP-48)
/// from the image. Emitted by `ExpireDelegationToken` handlers and the
/// background expiry sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteDelegationTokenRecord {
    pub token_id: String,
}

/// KIP-853: finalizes the cluster-wide kraft.version feature level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KRaftVersionRecord {
    pub kraft_version: u16,
}

/// KIP-853: full snapshot of the controller voter set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub voters: crate::voters::VoterSet,
}

/// KIP-584 finalized feature level. `level` is the finalized
/// `max_version_level` for `name`. `level == 0` is the KIP-584 sentinel
/// for "delete this finalized feature" — `MetadataImage::apply` removes the
/// entry rather than storing a zero. Replacement semantics: a later record
/// with the same `name` overwrites the previous level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureLevelRecord {
    pub name: String,
    pub level: i16,
}

/// Snapshot-only carrier for the KIP-584 finalized-features epoch.
///
/// The epoch is normally apply-derived (one bump per `V1FeatureLevel`
/// applied, so it tracks the history of `UpdateFeatures` calls, not the live
/// feature count). That derivation can't survive a snapshot: a snapshot
/// stores resulting *state*, so it emits at most one `V1FeatureLevel` per
/// live feature — fewer records than the original apply history. Replaying
/// those alone would reconstruct a smaller epoch and diverge from a replica
/// that replayed the full log.
///
/// So [`MetadataImage::to_records`](crate::MetadataImage::to_records) emits
/// this record last, and [`MetadataImage::apply`](crate::MetadataImage::apply)
/// SETS the epoch from it verbatim (rather than bumping), pinning the
/// reconstructed epoch to the original. It is produced only by `to_records`
/// and consumed only on snapshot replay — it is never submitted as a
/// controller change, so it never appears in the live Raft log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeaturesEpochRecord {
    pub epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
    V1TopicConfig(TopicConfigRecord),
    V1ScramCredential(ScramCredentialRecord),
    V1DeleteScramCredential(DeleteScramCredentialRecord),
    V1AccessControlEntry(crate::AclEntry),
    V1DeleteAccessControlEntry(crate::AclEntryFilter),
    V1BrokerConfig(BrokerConfigRecord),
    V1ClientQuota(ClientQuotaRecord),
    V1DelegationToken(DelegationTokenRecord),
    V1DeleteDelegationToken(DeleteDelegationTokenRecord),
    V1UnregisterBroker(UnregisterBrokerRecord),
    V1KRaftVersion(KRaftVersionRecord),
    V1Voters(VotersRecord),
    V1FeatureLevel(FeatureLevelRecord),
    V1ClientMetricsConfig(ClientMetricsConfigRecord),
    /// Snapshot-only: pins the finalized-features epoch on reconstruction.
    /// Never submitted via the controller; see [`FeaturesEpochRecord`].
    V1FeaturesEpoch(FeaturesEpochRecord),
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};

    fn round_trip(r: &MetadataRecord) -> MetadataRecord {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(r).unwrap();
        <SerdeCompat<MetadataRecord>>::deserialize(&bytes).unwrap()
    }

    #[test]
    fn feature_level_round_trip() {
        let r = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn features_epoch_round_trip() {
        let r = MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord { epoch: 7 });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn topic_record_round_trip() {
        let r = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 1,
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn partition_record_round_trip() {
        let r = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_registration_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 7,
            broker_epoch: 0,
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
            endpoints: vec![],
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_registration_with_endpoints_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 1,
            broker_epoch: 0,
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![BrokerEndpoint {
                name: "EXTERNAL".into(),
                host: "ext.example.com".into(),
                port: 9092,
                protocol: crabka_security::ListenerProtocol::SaslSsl,
            }],
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_topic_round_trip() {
        let r = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn unregister_broker_round_trip() {
        let r = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id: 42 });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn topic_config_record_round_trip() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        overrides.insert("segment.bytes".to_string(), "1048576".to_string());
        let r = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn scram_credential_round_trip() {
        let r = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
            salt: vec![1u8; 16],
            stored_key: vec![2u8; 64],
            server_key: vec![3u8; 64],
            iterations: 4096,
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_scram_credential_round_trip() {
        let r = MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn v1_access_control_entry_round_trip() {
        let entry = crate::AclEntry {
            resource_type: crate::ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: crate::PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: crate::AclOperation::Read,
            permission_type: crate::PermissionType::Allow,
        };
        let r = MetadataRecord::V1AccessControlEntry(entry);
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn v1_delete_access_control_entry_round_trip() {
        let filter = crate::AclEntryFilter {
            resource_type: Some(crate::ResourceType::Group),
            resource_name: Some("cg-foo".into()),
            pattern_type: Some(crate::PatternType::Literal),
            principal: None,
            host: None,
            operation: None,
            permission_type: None,
        };
        let r = MetadataRecord::V1DeleteAccessControlEntry(filter);
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_config_record_round_trip() {
        let r = MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 7,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn client_quota_record_round_trip() {
        let r = MetadataRecord::V1ClientQuota(ClientQuotaRecord {
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
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn delegation_token_record_round_trip() {
        let r = MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: "tok-abc".into(),
            owner: crabka_security::KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            hmac: vec![0xAB; 32],
            issue_timestamp_ms: 1_700_000_000_000,
            expiry_timestamp_ms: 1_700_000_600_000,
            max_timestamp_ms: 1_700_604_800_000,
            renewers: vec![crabka_security::KafkaPrincipal {
                principal_type: "User".into(),
                name: "bob".into(),
            }],
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_delegation_token_record_round_trip() {
        let r = MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
            token_id: "tok-abc".into(),
        });
        assert!(round_trip(&r) == r);
    }

    #[test]
    fn voters_record_round_trips() {
        let rec = MetadataRecord::V1Voters(VotersRecord {
            voters: crate::voters::VoterSet::from_voters([crate::voters::Voter {
                id: 7,
                directory_id: uuid::Uuid::from_u128(7),
                endpoints: vec![crate::voters::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "h".into(),
                    port: 1,
                }],
                kraft_version: crate::voters::KRaftVersionRange::default(),
            }]),
        });
        assert!(round_trip(&rec) == rec);
    }

    #[test]
    fn kraft_version_record_round_trips() {
        let rec = MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 });
        assert!(round_trip(&rec) == rec);
    }

    #[test]
    fn client_metrics_config_round_trip() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("interval.ms".to_string(), "60000".to_string());
        overrides.insert(
            "metrics".to_string(),
            "org.apache.kafka.consumer.".to_string(),
        );
        let r = MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "sub-a".into(),
            configs: overrides,
        });
        assert_eq!(round_trip(&r), r);
    }
}
