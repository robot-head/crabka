//! The payload that travels through the connect runtime for one replicated record.

use bytes::Bytes;

use crate::ids::{Offset, PartitionIndex, Timestamp};

/// One record from the source cluster, carried as the connect-value type `V`
/// through the [`crabka_connect`] runtime.
///
/// Replication keeps its full source envelope here because the offset and
/// provenance metadata are consumed by the specialized target sink in
/// addition to the generic [`crabka_connect::ConnectRecord`] routing fields.
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
