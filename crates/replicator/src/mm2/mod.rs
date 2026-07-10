//! MirrorMaker-2 byte-compatible record codecs.
mod checkpoint;
mod heartbeat;
mod offset_sync;

pub use checkpoint::Checkpoint;
pub use heartbeat::Heartbeat;
pub use offset_sync::OffsetSync;

use crate::error::ReplicatorError;

pub(crate) const VERSION: i16 = 0;

pub(crate) struct Writer(Vec<u8>);

impl Writer {
    /// Versioned buffer: emits the `[int16 version]` header first.
    ///
    /// The real JVM MM2 codecs version only the record *value* (via
    /// `HEADER_SCHEMA`), never the key (`serializeKey` writes `KEY_SCHEMA`
    /// alone). Use [`Writer::new`] for values, [`Writer::keyless`] for keys.
    pub fn new() -> Self {
        let mut w = Vec::new();
        w.extend_from_slice(&VERSION.to_be_bytes());
        Self(w)
    }

    /// Versionless buffer for record keys (no `[int16 version]` header), as
    /// produced by the JVM MM2 `serializeKey()` path.
    pub fn keyless() -> Self {
        Self(Vec::new())
    }

    pub fn string(&mut self, s: &str) -> &mut Self {
        let len = i16::try_from(s.len()).expect("string < 32k");
        self.0.extend_from_slice(&len.to_be_bytes());
        self.0.extend_from_slice(s.as_bytes());
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    #[must_use]
    pub fn finish(&self) -> Vec<u8> {
        self.0.clone()
    }
}

pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Reader over a versioned buffer (record *value*): consumes and validates
    /// the leading `[int16 version]` header.
    pub fn new(buf: &'a [u8]) -> Result<Self, ReplicatorError> {
        if buf.len() < 2 || i16::from_be_bytes([buf[0], buf[1]]) != VERSION {
            return Err(ReplicatorError::Codec(
                "bad/unsupported MM2 record version".into(),
            ));
        }
        Ok(Self { buf, pos: 2 })
    }

    /// Reader over a versionless buffer (record *key*): the JVM MM2
    /// `serializeKey()` path emits no version header.
    pub fn keyless(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ReplicatorError> {
        let end = self.pos + n;
        if end > self.buf.len() {
            return Err(ReplicatorError::Codec("short MM2 record".into()));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    pub fn string(&mut self) -> Result<String, ReplicatorError> {
        let raw = self.take(2)?;
        // SAFETY: take(2) guarantees exactly 2 bytes.
        let len_i16 = i16::from_be_bytes([raw[0], raw[1]]);
        // MM2 string lengths are non-negative i16 values (0..=32767).
        // Casting to usize is safe: the value is non-negative and fits in usize
        // on any platform Crabka targets (≥ 16-bit address space).
        #[allow(clippy::cast_sign_loss)]
        let len = len_i16 as usize;
        Ok(String::from_utf8_lossy(self.take(len)?).into_owned())
    }

    pub fn i32(&mut self) -> Result<i32, ReplicatorError> {
        let raw = self.take(4)?;
        Ok(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    pub fn i64(&mut self) -> Result<i64, ReplicatorError> {
        let raw = self.take(8)?;
        Ok(i64::from_be_bytes([
            raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{DownstreamOffset, PartitionIndex, UpstreamOffset};

    #[test]
    fn heartbeat_key_value_bytes_exact() {
        let hb = Heartbeat {
            source: "us-east".into(),
            target: "eu-west".into(),
            timestamp_ms: 100,
        };
        // Key is versionless (JVM MM2 serializeKey writes no version header).
        assert2::assert!(hb.key_bytes() == b"\x00\x07us-east\x00\x07eu-west".to_vec());
        // value = version(i16=0x0000) + timestamp(i64=100=0x0000_0000_0000_0064) = 10 bytes
        assert2::assert!(hb.value_bytes() == b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x64".to_vec());
        let (k, v) = (hb.key_bytes(), hb.value_bytes());
        assert2::assert!(Heartbeat::from_bytes(&k, &v).unwrap() == hb);
    }

    #[test]
    fn offset_sync_bytes_roundtrip() {
        let os = OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(7),
            upstream: UpstreamOffset(1000),
            downstream: DownstreamOffset(742),
        };
        assert2::assert!(OffsetSync::from_bytes(&os.key_bytes(), &os.value_bytes()).unwrap() == os);
        // Key is versionless (JVM MM2 serializeKey writes no version header).
        assert2::assert!(os.key_bytes() == b"\x00\x06orders\x00\x00\x00\x07".to_vec());
    }

    #[test]
    fn internal_topic_names_match_mm2() {
        // MM2-compatible internal topic naming, pinned exactly.
        assert2::assert!(Checkpoint::topic_name("us-east") == "us-east.checkpoints.internal");
        assert2::assert!(OffsetSync::topic_name("us-east") == "mm2-offset-syncs.us-east.internal");
    }

    #[test]
    fn checkpoint_bytes_roundtrip() {
        let c = Checkpoint {
            group: "analytics".into(),
            topic: "orders".into(),
            partition: PartitionIndex(7),
            upstream: UpstreamOffset(1000),
            downstream: DownstreamOffset(742),
            metadata: String::new(),
        };
        assert2::assert!(Checkpoint::from_bytes(&c.key_bytes(), &c.value_bytes()).unwrap() == c);
    }
}
