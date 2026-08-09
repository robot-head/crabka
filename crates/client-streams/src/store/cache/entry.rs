//! A single record-cache entry.
//!
//! The entry holds the value bytes, a dirty flag, and the record context. A
//! `None` value is a tombstone. The cache uses the context to forward the entry
//! downstream on flush.
use bytes::Bytes;

use crate::processor::record::RecordContext;

#[derive(Clone, Debug)]
pub(crate) struct LruCacheEntry {
    pub value: Option<Bytes>,
    pub dirty: bool,
    pub context: RecordContext,
}

impl LruCacheEntry {
    pub fn new(value: Option<Bytes>, dirty: bool, context: RecordContext) -> Self {
        Self {
            value,
            dirty,
            context,
        }
    }

    /// Returns the heap footprint for `ThreadCache` budget accounting.
    ///
    /// This method ports Kafka's `LRUCacheEntry.size()`. The size is the value
    /// bytes plus the context overhead of 8 timestamp bytes, 8 offset bytes,
    /// 4 partition bytes, and the topic bytes. `NamedCache` adds the key length,
    /// because `NamedCache` owns the key.
    pub fn value_size(&self) -> usize {
        let v = self.value.as_ref().map_or(0, Bytes::len);
        v + 8 + 8 + 4 + self.context.topic.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(topic: &str) -> RecordContext {
        RecordContext {
            topic: topic.to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    #[test]
    fn value_size_counts_value_and_context() {
        let entry = LruCacheEntry::new(Some(Bytes::from_static(b"abcd")), false, ctx("t"));
        // 4 (value) + 8 + 8 + 4 + 1 (topic "t") = 25
        assert_eq!(entry.value_size(), 25);
    }
}
