//! Versioned metadata records and the immutable image they apply to.

#![doc(html_root_url = "https://docs.rs/crabka-metadata/0.0.0")]

mod error;
mod records;

pub use error::MetadataError;
pub use records::{
    BrokerRegistrationRecord, DeleteTopicRecord, MetadataRecord, NodeId, PartitionRecord,
    TopicRecord,
};
