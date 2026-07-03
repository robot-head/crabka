//! The payload that travels through the connect runtime for one replicated record.

use bytes::Bytes;

use crate::ids::{Offset, PartitionIndex, Timestamp};

/// One record from the source cluster, carried as the connect-value type `V`
/// through the [`crabka_connect`] runtime.
///
/// Because [`crabka_connect::ConnectRecord`] has no topic/partition fields,
/// all envelope metadata is bundled here alongside the raw payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedRecord {
    /// The source topic name.
    pub topic: String,
    /// The source partition index.
    pub partition: PartitionIndex,
    /// The source offset (0-based).
    pub offset: Offset,
    /// Record timestamp in epoch milliseconds.
    pub timestamp: Timestamp,
    /// Record key, or `None` for a null key.
    pub key: Option<Bytes>,
    /// Record value, or `None` for a tombstone.
    pub value: Option<Bytes>,
    /// Per-record headers in declaration order.
    pub headers: Vec<(String, Option<Bytes>)>,
}
