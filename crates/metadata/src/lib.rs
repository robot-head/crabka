//! Versioned metadata records and the immutable image they apply to.
//!
//! `crabka-metadata` provides [`MetadataRecord`], the versioned union
//! of topic, partition, broker-registration, and topic-deletion records, and
//! [`MetadataImage`], an immutable snapshot of the cluster's metadata.
//!
//! Only [`MetadataImage::apply`] mutates the image, and the Raft state
//! machine in `crabka-raft` calls that method. Everywhere else reads it
//! through shared references and Arc clones.
//!
//! ## Applying controller records
//!
//! ```rust
//! use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
//! use uuid::Uuid;
//!
//! let mut image = MetadataImage::new(Uuid::new_v4());
//! let topic_id = Uuid::new_v4();
//!
//! image.apply(&MetadataRecord::V1Topic(TopicRecord {
//!     name: "orders".into(),
//!     topic_id,
//!     partitions: 0,
//!     replication_factor: 3,
//! }));
//!
//! let topic = image.topic("orders").expect("topic exists");
//! assert_eq!(topic.topic_id, topic_id);
//! ```
//!
//! ## Canonical quota entity keys
//!
//! ```rust
//! use crabka_metadata::canonicalize_entity;
//!
//! let key = canonicalize_entity(vec![
//!     ("user".to_string(), Some("alice".to_string())),
//!     ("client-id".to_string(), Some("analytics".to_string())),
//! ]);
//!
//! assert_eq!(key[0].0, "client-id");
//! assert_eq!(key[1].0, "user");
//! ```

#![doc(html_root_url = "https://docs.rs/crabka-metadata/0.3.9")]

pub mod acl;
mod error;
mod feature;
pub mod group_version;
mod image;
pub mod kafka_record;
pub mod kraft_translate;
pub mod metadata_version;
mod records;
pub mod transaction_version;

pub use acl::{AclEntry, AclEntryFilter, AclOperation, PatternType, PermissionType, ResourceType};
/// KIP-853 voter-set value types, re-exported from the [`crabka_voters`] leaf
/// crate. The path stays `crabka_metadata::voters`, so existing call sites are
/// unchanged. The types live in their own crypto-free crate, so the consensus
/// core can compile to WebAssembly.
pub use crabka_voters as voters;
pub use error::MetadataError;
pub use feature::{
    Feature, bootstrap_feature_records, bootstrap_feature_records_with_overrides, feature,
    feature_registry, is_supported_level, validate_feature_dependencies,
};
pub use image::{DelegationToken, EntityKey, MetadataImage, ThrottleKind, canonicalize_entity};
pub use kafka_record::{KafkaRecordError, from_kafka_record, to_kafka_record};
pub use kraft_translate::{
    TranslateError, from_kraft, from_kraft_value, to_kraft, to_kraft_records, to_kraft_values,
};
pub use records::{
    BrokerConfigRecord, BrokerEndpoint, BrokerRegistrationRecord, ClientMetricsConfigRecord,
    ClientQuotaRecord, DelegationTokenRecord, DeleteDelegationTokenRecord,
    DeleteScramCredentialRecord, DeleteTopicRecord, FeatureLevelRecord, FeaturesEpochRecord,
    GroupConfigRecord, KRaftVersionRecord, LeaderEpoch, MetadataRecord, NodeId,
    PartitionDirAssignmentRecord, PartitionOffsetAdvanceRecord, PartitionRecord, ProducerIdsRecord,
    QuotaEntity, ScramCredentialRecord, TopicConfigRecord, TopicRecord, UnregisterBrokerRecord,
    VotersRecord,
};
pub use voters::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
