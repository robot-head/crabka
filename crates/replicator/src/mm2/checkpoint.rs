use super::{Reader, Writer};
use crate::error::ReplicatorError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub upstream: i64,
    pub downstream: i64,
    pub metadata: String,
}

impl Checkpoint {
    #[must_use]
    pub fn topic_name(source_alias: &str) -> String {
        format!("{source_alias}.checkpoints.internal")
    }

    #[must_use]
    pub fn key_bytes(&self) -> Vec<u8> {
        Writer::keyless()
            .string(&self.group)
            .string(&self.topic)
            .i32(self.partition)
            .finish()
    }

    #[must_use]
    pub fn value_bytes(&self) -> Vec<u8> {
        Writer::new()
            .i64(self.upstream)
            .i64(self.downstream)
            .string(&self.metadata)
            .finish()
    }

    pub fn from_bytes(key: &[u8], val: &[u8]) -> Result<Self, ReplicatorError> {
        let mut k = Reader::keyless(key);
        let mut v = Reader::new(val)?;
        Ok(Self {
            group: k.string()?,
            topic: k.string()?,
            partition: k.i32()?,
            upstream: v.i64()?,
            downstream: v.i64()?,
            metadata: v.string()?,
        })
    }
}
