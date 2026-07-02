use super::{Reader, Writer};
use crate::error::ReplicatorError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetSync {
    pub topic: String,
    pub partition: i32,
    pub upstream: i64,
    pub downstream: i64,
}

impl OffsetSync {
    #[must_use]
    pub fn topic_name(source_alias: &str) -> String {
        format!("mm2-offset-syncs.{source_alias}.internal")
    }

    #[must_use]
    pub fn key_bytes(&self) -> Vec<u8> {
        Writer::keyless()
            .string(&self.topic)
            .i32(self.partition)
            .finish()
    }

    #[must_use]
    pub fn value_bytes(&self) -> Vec<u8> {
        // OffsetSync has no HEADER_SCHEMA / version field at all in the JVM MM2
        // codec — both key and value are versionless (unlike Heartbeat and
        // Checkpoint, whose *value* carries a version header).
        Writer::keyless()
            .i64(self.upstream)
            .i64(self.downstream)
            .finish()
    }

    pub fn from_bytes(key: &[u8], val: &[u8]) -> Result<Self, ReplicatorError> {
        let mut k = Reader::keyless(key);
        let mut v = Reader::keyless(val);
        Ok(Self {
            topic: k.string()?,
            partition: k.i32()?,
            upstream: v.i64()?,
            downstream: v.i64()?,
        })
    }
}
