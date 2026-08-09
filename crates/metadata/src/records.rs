//! Versioned metadata records. Future versions add variants. An older
//! reader can skip an unknown variant because we encode each variant
//! length-prefixed inside the `bincode` payload.

pub use crabka_ids::LeaderEpoch;
pub use crabka_voters::NodeId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub name: String,
    pub topic_id: Uuid,
    pub partitions: i32,
    pub replication_factor: i16,
}

fn default_partition_epoch() -> i32 {
    -1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    /// Per-partition leader epoch (KIP-320). Bumped on every leader change.
    /// Older on-disk metadata is not migrated. `#[serde(transparent)]` on
    /// [`LeaderEpoch`] keeps the on-disk bincode bytes identical to a bare
    /// `i32`.
    pub leader_epoch: LeaderEpoch,
    /// Replicas being added in an in-flight reassignment. Empty when no
    /// reassignment in flight. KIP-455.
    pub adding_replicas: Vec<NodeId>,
    /// Replicas being removed in an in-flight reassignment. Empty when
    /// no reassignment in flight. KIP-455.
    pub removing_replicas: Vec<NodeId>,
    /// KIP-858: the log-directory UUID that hosts each replica, parallel to
    /// [`Self::replicas`] in the same index order. `Uuid::nil()` is
    /// `DirectoryId.UNASSIGNED`: the owning broker has not yet reported
    /// its `AssignReplicasToDirs` for this replica. The controller matches
    /// this against the replica slot of a broker to map the failed-dir UUID
    /// of that broker to the partitions it must fail over.
    pub directories: Vec<Uuid>,
    /// KIP-631: per-partition state epoch. It increments on every state
    /// change, such as a leader election, an ISR change, or a reassignment.
    /// It is 0 on creation. The default of -1 matches the KIP-631 schema
    /// default, for compatibility with records written before this field
    /// existed.
    #[serde(default = "default_partition_epoch")]
    pub partition_epoch: i32,
}

/// KIP-858 directory-assignment delta. A broker reports which log-dir UUID
/// hosts its replica of `(topic, partition)`. Apply treats it as a DELTA: it
/// sets ONLY the slot of the reporting replica in
/// `PartitionRecord.directories` and never touches leader, isr, replicas,
/// adding, or removing. It therefore cannot clobber a concurrent reassignment
/// or ISR change. On the `KRaft` log it rides a Crabka-private carrier through
/// `to_kraft`, so it decodes back to this same delta and applies as a one-slot
/// merge, never as a full-record replace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionDirAssignmentRecord {
    pub topic: String,
    pub partition: i32,
    /// The reporting broker (must be a replica of the partition).
    pub replica: NodeId,
    /// The log-directory UUID hosting this broker's replica.
    pub directory: Uuid,
}

/// Diskless offset-sequencer delta: advance a partition's committed
/// next-offset by `count`.
///
/// Applied as a delta, never a full-record replace, so sequential advances on
/// the committed metadata log yield a gap-free, strictly-monotonic, unique
/// offset sequence. On the `KRaft` log it rides a Crabka-private carrier like
/// [`PartitionDirAssignmentRecord`], so it decodes back to this same delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionOffsetAdvanceRecord {
    pub topic: String,
    pub partition: i32,
    /// Offsets consumed by the produce group. The producer's base offset is the
    /// committed next-offset before this increment applies.
    pub count: i64,
}

/// A single named listener endpoint advertised by a broker. It is stored as a
/// list on [`BrokerRegistrationRecord::endpoints`], so KRaft-style metadata
/// can advertise per-listener `host:port` and protocol triples to clients on
/// `Metadata` v9+. A legacy single-listener broker leaves the list empty and
/// relies on the top-level `host` and `port` fields.
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
    /// record committed. The controller leader assigns it at append time in
    /// `on_submit_change`. A freshly-built literal carries `0` until the
    /// leader stamps it. `AlterPartition` uses it to fence stale replicas
    /// from the ISR.
    pub broker_epoch: i64,
    /// KIP-631: UUID that identifies this specific process invocation of the
    /// broker. Generated once at first boot and persisted in
    /// `{log_dir}/incarnation_id`. A JVM controller uses it to detect
    /// broker restarts and fence stale replica memberships.
    #[serde(default)]
    pub incarnation_id: uuid::Uuid,
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

/// KIP-919 / `UnregisterBroker` (`api_key` 64). Marks a broker as
/// permanently unregistered: the admin operator confirms that the broker is
/// gone for good and asks the cluster to drop its registration entry
/// from the metadata image. Later `Metadata` responses no longer
/// advertise the endpoints of the broker, and clients stop routing to it.
///
/// The record is idempotent. A second apply, or an apply against an unknown
/// `node_id`, does nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnregisterBrokerRecord {
    pub node_id: NodeId,
}

/// Mutable topic configuration overrides. Authoritative target state:
/// each `V1TopicConfig` record fully replaces the previous override map
/// for `topic`. An empty map clears all overrides. The `AlterConfigs`
/// handler merges before it submits the record.
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
/// override map for `name`, the subscription name. An empty map deletes
/// the subscription. The `IncrementalAlterConfigs` handler merges before it
/// submits the record, in the same pattern as [`TopicConfigRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetricsConfigRecord {
    pub name: String,
    pub configs: std::collections::BTreeMap<String, String>,
}

/// KIP-1071 dynamic configuration for one group resource. Each record is the
/// authoritative override map for `group_id`; an empty map clears the resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupConfigRecord {
    pub group_id: String,
    pub configs: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaEntity {
    pub entity_type: String,
    pub entity_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientQuotaRecord {
    /// Canonicalized entity tuple, sorted alphabetically by `entity_type`.
    pub entity: Vec<QuotaEntity>,
    pub config_key: String,
    pub config_value: Option<f64>,
}

/// Durable controller state for cluster-wide producer-ID block allocation.
/// `next_producer_id` is the first ID not covered by any committed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerIdsRecord {
    pub broker_id: NodeId,
    pub broker_epoch: i64,
    pub next_producer_id: i64,
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
/// The record has replacement semantics: a new record with the same
/// `token_id` overwrites the prior one in the image. Both Create and Renew
/// use that. Removal goes through
/// [`DeleteDelegationTokenRecord`]. `hmac` is the 32-byte HMAC-SHA-256
/// over `token_id` keyed by the broker's master secret key. A client
/// authenticates with SCRAM-SHA-256 and uses the hex-encoded HMAC as the
/// password.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationTokenRecord {
    pub token_id: String,
    pub owner: crabka_security::KafkaPrincipal,
    pub hmac: Vec<u8>,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    /// Issue plus max-lifetime. A renewal cannot push `expiry_timestamp_ms`
    /// past this ceiling.
    pub max_timestamp_ms: i64,
    pub renewers: Vec<crabka_security::KafkaPrincipal>,
}

/// Tombstone record that removes a delegation token (KIP-48)
/// from the image. The `ExpireDelegationToken` handlers and the
/// background expiry sweep emit it.
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
/// for "delete this finalized feature": `MetadataImage::apply` removes the
/// entry and does not store a zero. Replacement semantics: a later record
/// with the same `name` overwrites the previous level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureLevelRecord {
    pub name: String,
    pub level: i16,
}

/// Snapshot-only carrier for the KIP-584 finalized-features epoch.
///
/// Apply normally derives the epoch, with one bump per applied
/// `V1FeatureLevel`, so it tracks the history of `UpdateFeatures` calls and
/// not the live feature count. That derivation cannot survive a snapshot. A
/// snapshot stores resulting *state*, so it emits at most one
/// `V1FeatureLevel` per live feature, which is fewer records than the
/// original apply history. A replay of those records alone reconstructs a
/// smaller epoch and diverges from a replica that replayed the full log.
///
/// [`MetadataImage::to_records`](crate::MetadataImage::to_records) therefore
/// emits this record last, and
/// [`MetadataImage::apply`](crate::MetadataImage::apply) SETS the epoch from
/// it verbatim and does not bump it. That pins the reconstructed epoch to the
/// original. Only `to_records` produces this record, and only snapshot replay
/// consumes it. Nothing submits it as a controller change, so it never appears
/// in the live Raft log.
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
    V1ProducerIds(ProducerIdsRecord),
    V1DelegationToken(DelegationTokenRecord),
    V1DeleteDelegationToken(DeleteDelegationTokenRecord),
    V1UnregisterBroker(UnregisterBrokerRecord),
    V1KRaftVersion(KRaftVersionRecord),
    V1Voters(VotersRecord),
    V1FeatureLevel(FeatureLevelRecord),
    V1ClientMetricsConfig(ClientMetricsConfigRecord),
    /// Snapshot-only: pins the finalized-features epoch on reconstruction.
    /// Nothing submits it through the controller. See [`FeaturesEpochRecord`].
    V1FeaturesEpoch(FeaturesEpochRecord),
    /// KIP-858 directory-assignment delta (see [`PartitionDirAssignmentRecord`]).
    /// Applied as a merge into one replica's `directories` slot; on the `KRaft`
    /// log it rides a Crabka-private carrier so it stays a delta end-to-end.
    V1PartitionDirAssignment(PartitionDirAssignmentRecord),
    /// Diskless offset-sequencer delta (see [`PartitionOffsetAdvanceRecord`]).
    /// Applied as an increment to the partition's committed next-offset.
    V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord),
    /// KIP-1071 dynamic GROUP resource configuration.
    V1GroupConfig(GroupConfigRecord),
}

#[cfg(test)]
mod tests {

    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};

    use super::*;

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
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn group_config_round_trip() {
        let r = MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "streams-app".into(),
            configs: std::collections::BTreeMap::from([(
                "streams.num.standby.replicas".into(),
                "1".into(),
            )]),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn producer_ids_round_trip() {
        let r = MetadataRecord::V1ProducerIds(ProducerIdsRecord {
            broker_id: NodeId(3),
            broker_epoch: 9,
            next_producer_id: 2_000,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn features_epoch_round_trip() {
        let r = MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord { epoch: 7 });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn topic_record_round_trip() {
        let r = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 1,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn partition_record_round_trip() {
        let r = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2)],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::nil()],
            partition_epoch: 0,
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn partition_dir_assignment_round_trip() {
        let r = MetadataRecord::V1PartitionDirAssignment(PartitionDirAssignmentRecord {
            topic: "t".into(),
            partition: 2,
            replica: NodeId(3),
            directory: Uuid::from_u128(0xAB),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_registration_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(7),
            broker_epoch: 0,
            incarnation_id: Uuid::from_u128(0xdeadbeef_cafe_babe_0123_456789abcdef),
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
            endpoints: vec![],
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_registration_with_endpoints_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(1),
            broker_epoch: 0,
            incarnation_id: Uuid::from_u128(0xfeedface_0000_0000_0000_000000000001),
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
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_topic_round_trip() {
        let r = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn unregister_broker_round_trip() {
        let r = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
            node_id: NodeId(42),
        });
        assert2::assert!(round_trip(&r) == r);
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
        assert2::assert!(round_trip(&r) == r);
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
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_scram_credential_round_trip() {
        let r = MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
        });
        assert2::assert!(round_trip(&r) == r);
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
        assert2::assert!(round_trip(&r) == r);
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
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn broker_config_record_round_trip() {
        let r = MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(7),
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        });
        assert2::assert!(round_trip(&r) == r);
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
        assert2::assert!(round_trip(&r) == r);
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
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn delete_delegation_token_record_round_trip() {
        let r = MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
            token_id: "tok-abc".into(),
        });
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn voters_record_round_trips() {
        let rec = MetadataRecord::V1Voters(VotersRecord {
            voters: crate::voters::VoterSet::from_voters([crate::voters::Voter {
                id: NodeId(7),
                directory_id: uuid::Uuid::from_u128(7),
                endpoints: vec![crate::voters::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "h".into(),
                    port: 1,
                }],
                kraft_version: crate::voters::KRaftVersionRange::default(),
            }]),
        });
        assert2::assert!(round_trip(&rec) == rec);
    }

    #[test]
    fn kraft_version_record_round_trips() {
        let rec = MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 });
        assert2::assert!(round_trip(&rec) == rec);
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
        assert2::assert!(round_trip(&r) == r);
    }

    #[test]
    fn partition_epoch_serde_default_is_minus_one() {
        // -1 is Kafka's "unknown epoch" sentinel; pin it (mutants flip it to
        // 0/1). This is the `#[serde(default)]` fallback for partition_epoch.
        assert2::assert!(default_partition_epoch() == -1);
    }
}
