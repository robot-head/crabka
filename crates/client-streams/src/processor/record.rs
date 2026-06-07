//! `Record<K,V>` flowing through the processor graph + `RecordContext`.

/// A key/value record with a timestamp.
///
/// `key` is optional because Kafka records may have null keys. `value` is typed
/// and present at this layer; table deletions are represented in the DSL as
/// change records whose `new` value is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<K, V> {
    pub key: Option<K>,
    pub value: V,
    pub timestamp: i64,
}

impl<K, V> Record<K, V> {
    #[must_use]
    pub fn new(key: Option<K>, value: V, timestamp: i64) -> Self {
        Self {
            key,
            value,
            timestamp,
        }
    }
}

/// Metadata about the source record currently being processed (JVM
/// `RecordContext`). Exposed via
/// [`ProcessorContext::record_context`](crate::processor::ProcessorContext::record_context).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordContext {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}
