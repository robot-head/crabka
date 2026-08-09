//! `Record<K,V>` and `RecordContext`, which flow through the processor graph.

/// A key/value record with a timestamp.
///
/// `key` is optional, because a Kafka record can have a null key. `value` is
/// typed and always present at this layer. The DSL represents a table deletion
/// as a change record whose `new` value is `None`.
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

/// Metadata about the source record that the processor handles now. This is the
/// JVM `RecordContext`.
///
/// [`ProcessorContext::record_context`](crate::processor::ProcessorContext::record_context)
/// exposes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordContext {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}
