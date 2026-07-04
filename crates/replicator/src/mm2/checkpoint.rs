use super::{Reader, Writer};
use crate::{
    error::ReplicatorError,
    ids::{DownstreamOffset, PartitionIndex, UpstreamOffset},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub group: String,
    pub topic: String,
    pub partition: PartitionIndex,
    pub upstream: UpstreamOffset,
    pub downstream: DownstreamOffset,
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
            .i32(self.partition.0)
            .finish()
    }

    #[must_use]
    pub fn value_bytes(&self) -> Vec<u8> {
        Writer::new()
            .i64(self.upstream.0)
            .i64(self.downstream.0)
            .string(&self.metadata)
            .finish()
    }

    pub fn from_bytes(key: &[u8], val: &[u8]) -> Result<Self, ReplicatorError> {
        let mut k = Reader::keyless(key);
        let mut v = Reader::new(val)?;
        Ok(Self {
            group: k.string()?,
            topic: k.string()?,
            partition: PartitionIndex(k.i32()?),
            upstream: UpstreamOffset(v.i64()?),
            downstream: DownstreamOffset(v.i64()?),
            metadata: v.string()?,
        })
    }
}
