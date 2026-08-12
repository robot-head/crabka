//! Combined diskless-WAL object framing.
//!
//! One object concatenates many partitions' verbatim v2-batch runs, delimited by
//! a footer manifest:
//! `[MAGIC + version:u16] + runs + [manifest] + [footer_len:u32 + MAGIC]`.
//! Framing is little-endian and Crabka-private. Only the embedded runs need
//! Kafka byte-exactness.

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

const MAGIC: [u8; 4] = *b"CKWL";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 6;
const TRAILER_LEN: usize = 8;
const ENTRY_LEN: usize = 48;

/// One partition's run within a combined object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalObjectEntry {
    /// Topic UUID for the partition.
    pub topic_id: Uuid,
    /// Partition index.
    pub partition: i32,
    /// First offset included in the run.
    pub first_offset: i64,
    /// Last offset included in the run.
    pub last_offset: i64,
    /// Absolute byte offset of the run within the object.
    pub byte_start: u64,
    /// Byte length of the run.
    pub byte_len: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum WalObjectError {
    /// Object does not contain the minimum header and trailer.
    #[error("wal object too short")]
    TooShort,
    /// Header or trailer magic does not match the diskless WAL object format.
    #[error("bad wal object magic")]
    BadMagic,
    /// Object uses an unsupported framing version.
    #[error("unsupported wal object version {0}")]
    BadVersion(u16),
    /// Footer manifest length or entry ranges are invalid.
    #[error("corrupt wal object manifest")]
    BadManifest,
}

/// Accumulates verbatim partition runs and serializes one combined WAL object.
#[derive(Default)]
pub struct WalObjectBuilder {
    body: BytesMut,
    entries: Vec<WalObjectEntry>,
}

impl WalObjectBuilder {
    /// Create an empty object builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            body: BytesMut::new(),
            entries: Vec::new(),
        }
    }

    /// Bytes accumulated so far, excluding framing overhead.
    #[must_use]
    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    /// Whether the builder has no runs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Append one partition's verbatim run and record its absolute object range.
    pub fn append_run(
        &mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) {
        let byte_start = u64::try_from(HEADER_LEN + self.body.len())
            .expect("wal object body length fits in u64");
        let byte_len = u32::try_from(run.len()).expect("wal object run length fits in u32");

        self.body.extend_from_slice(run);
        self.entries.push(WalObjectEntry {
            topic_id,
            partition,
            first_offset,
            last_offset,
            byte_start,
            byte_len,
        });
    }

    /// Serialize the final object bytes.
    #[must_use]
    pub fn finish(self) -> Bytes {
        let manifest_len = self
            .entries
            .len()
            .checked_mul(ENTRY_LEN)
            .expect("wal object manifest length fits in usize");
        let capacity = HEADER_LEN + self.body.len() + manifest_len + TRAILER_LEN;
        let mut out = BytesMut::with_capacity(capacity);

        out.extend_from_slice(&MAGIC);
        out.put_u16_le(VERSION);
        out.extend_from_slice(&self.body);

        let footer_start = out.len();
        for entry in &self.entries {
            out.extend_from_slice(entry.topic_id.as_bytes());
            out.put_i32_le(entry.partition);
            out.put_i64_le(entry.first_offset);
            out.put_i64_le(entry.last_offset);
            out.put_u64_le(entry.byte_start);
            out.put_u32_le(entry.byte_len);
        }

        let footer_len = u32::try_from(out.len() - footer_start)
            .expect("wal object manifest length fits in u32");
        out.put_u32_le(footer_len);
        out.extend_from_slice(&MAGIC);
        out.freeze()
    }

    /// Test convenience. It appends one run and finishes.
    #[cfg(test)]
    #[must_use]
    pub fn finish_with_run(
        mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) -> Bytes {
        self.append_run(topic_id, partition, first_offset, last_offset, run);
        self.finish()
    }
}

/// Parse the footer manifest of a combined WAL object.
///
/// # Errors
///
/// Returns an error when the object header, trailer, version, manifest size, or
/// manifest byte ranges are invalid.
pub fn parse_wal_object(obj: &Bytes) -> Result<Vec<WalObjectEntry>, WalObjectError> {
    if obj.len() < HEADER_LEN + TRAILER_LEN {
        return Err(WalObjectError::TooShort);
    }
    if obj[..MAGIC.len()] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }

    let version = u16::from_le_bytes([obj[4], obj[5]]);
    if version != VERSION {
        return Err(WalObjectError::BadVersion(version));
    }

    let trailer_start = obj.len() - TRAILER_LEN;
    if obj[trailer_start + 4..] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }

    let footer_len = u32::from_le_bytes(
        obj[trailer_start..trailer_start + 4]
            .try_into()
            .map_err(|_| WalObjectError::BadManifest)?,
    );
    let footer_len = usize::try_from(footer_len).map_err(|_| WalObjectError::BadManifest)?;
    if footer_len % ENTRY_LEN != 0 {
        return Err(WalObjectError::BadManifest);
    }

    let footer_start = trailer_start
        .checked_sub(footer_len)
        .ok_or(WalObjectError::BadManifest)?;
    if footer_start < HEADER_LEN {
        return Err(WalObjectError::BadManifest);
    }

    let mut entries = Vec::with_capacity(footer_len / ENTRY_LEN);
    for entry_bytes in obj[footer_start..trailer_start].chunks_exact(ENTRY_LEN) {
        let topic_id =
            Uuid::from_slice(&entry_bytes[..16]).map_err(|_| WalObjectError::BadManifest)?;
        let partition = i32::from_le_bytes(
            entry_bytes[16..20]
                .try_into()
                .map_err(|_| WalObjectError::BadManifest)?,
        );
        let first_offset = i64::from_le_bytes(
            entry_bytes[20..28]
                .try_into()
                .map_err(|_| WalObjectError::BadManifest)?,
        );
        let last_offset = i64::from_le_bytes(
            entry_bytes[28..36]
                .try_into()
                .map_err(|_| WalObjectError::BadManifest)?,
        );
        let byte_start = u64::from_le_bytes(
            entry_bytes[36..44]
                .try_into()
                .map_err(|_| WalObjectError::BadManifest)?,
        );
        let byte_len = u32::from_le_bytes(
            entry_bytes[44..48]
                .try_into()
                .map_err(|_| WalObjectError::BadManifest)?,
        );

        let range_start = usize::try_from(byte_start).map_err(|_| WalObjectError::BadManifest)?;
        let range_len = usize::try_from(byte_len).map_err(|_| WalObjectError::BadManifest)?;
        let range_end = range_start
            .checked_add(range_len)
            .ok_or(WalObjectError::BadManifest)?;
        if range_start < HEADER_LEN || range_end > footer_start {
            return Err(WalObjectError::BadManifest);
        }

        entries.push(WalObjectEntry {
            topic_id,
            partition,
            first_offset,
            last_offset,
            byte_start,
            byte_len,
        });
    }

    Ok(entries)
}

/// Slice out a run's bytes without copying.
#[cfg(test)]
#[must_use]
pub fn run_bytes(obj: &Bytes, entry: &WalObjectEntry) -> Bytes {
    let start = usize::try_from(entry.byte_start).expect("wal object byte_start fits in usize");
    let len = usize::try_from(entry.byte_len).expect("wal object byte_len fits in usize");
    obj.slice(start..start + len)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn build_then_parse_round_trips_all_runs_byte_exact() {
        let t = Uuid::from_u128(1);
        let mut b = WalObjectBuilder::new();
        b.append_run(t, 0, 0, 2, b"partition-0-verbatim-bytes");
        b.append_run(t, 1, 10, 11, b"p1-bytes");
        let obj = b.finish();

        let entries = parse_wal_object(&obj).unwrap();
        assert!(entries.len() == 2);
        assert!(
            entries[0].partition == 0
                && entries[0].first_offset == 0
                && entries[0].last_offset == 2
        );
        assert!(&run_bytes(&obj, &entries[0])[..] == b"partition-0-verbatim-bytes");
        assert!(&run_bytes(&obj, &entries[1])[..] == b"p1-bytes");
    }

    #[test]
    fn parse_rejects_bad_trailer_magic() {
        let obj = WalObjectBuilder::new().finish_with_run(Uuid::nil(), 0, 0, 0, b"x");
        let last = obj.len() - 1;
        let mut v = obj.to_vec();
        v[last] ^= 0xff;
        let obj = bytes::Bytes::from(v);

        assert!(parse_wal_object(&obj).is_err());
    }
}
