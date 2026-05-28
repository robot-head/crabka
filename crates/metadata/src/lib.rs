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
mod image;
mod records;

pub use acl::{AclEntry, AclEntryFilter, AclOperation, PatternType, PermissionType, ResourceType};
pub use error::MetadataError;
pub use image::{DelegationToken, EntityKey, MetadataImage, ThrottleKind, canonicalize_entity};
pub use records::{
    BrokerConfigRecord, BrokerEndpoint, BrokerRegistrationRecord, ClientQuotaRecord,
    DelegationTokenRecord, DeleteDelegationTokenRecord, DeleteScramCredentialRecord,
    DeleteTopicRecord, FeatureLevelRecord, MetadataRecord, NodeId, PartitionRecord, QuotaEntity,
    ScramCredentialRecord, TopicConfigRecord, TopicRecord, UnregisterBrokerRecord,
};
