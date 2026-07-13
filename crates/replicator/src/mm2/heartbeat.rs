use super::{Reader, Writer};
use crate::error::ReplicatorError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    pub source: String,
    pub target: String,
    pub timestamp_ms: i64,
}

impl Heartbeat {
    pub const TOPIC: &'static str = "heartbeats";

    #[must_use]
    pub fn key_bytes(&self) -> Vec<u8> {
        Writer::keyless()
            .string(&self.source)
            .string(&self.target)
            .finish()
    }

    #[must_use]
    pub fn value_bytes(&self) -> Vec<u8> {
        Writer::new().i64(self.timestamp_ms).finish()
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn from_bytes(key: &[u8], val: &[u8]) -> Result<Self, ReplicatorError> {
        let mut k = Reader::keyless(key);
        let mut v = Reader::new(val)?;
        Ok(Self {
            source: k.string()?,
            target: k.string()?,
            timestamp_ms: v.i64()?,
        })
    }
}
