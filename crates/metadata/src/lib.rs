//! Versioned metadata records and the immutable image they apply to.
//!
//! `crabka-metadata` provides [`MetadataRecord`] (the versioned union
//! of topic / partition / broker registrations / topic deletions) and
//! [`MetadataImage`] (an immutable snapshot of the cluster's metadata).
//!
//! The image is mutated only by [`MetadataImage::apply`] called from
//! the Raft state machine in `crabka-raft`. Everywhere else it's read
//! via shared references and Arc clones.
//!
//! See the design spec at
//! `docs/superpowers/specs/2026-05-12-crabka-metadata-quorum-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-metadata/0.0.0")]

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
pub mod voters;

pub use acl::{AclEntry, AclEntryFilter, AclOperation, PatternType, PermissionType, ResourceType};
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
    KRaftVersionRecord, MetadataRecord, NodeId, PartitionDirAssignmentRecord, PartitionRecord,
    QuotaEntity, ScramCredentialRecord, TopicConfigRecord, TopicRecord, UnregisterBrokerRecord,
    VotersRecord,
};
pub use voters::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
