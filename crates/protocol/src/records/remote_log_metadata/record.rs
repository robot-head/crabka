//! Dispatch enum over the generated KIP-405 remote-log metadata record types.
//!
//! The enum is keyed by the `__remote_log_metadata` record apiKey. This apiKey
//! namespace is distinct from both RPC apiKeys and the `KRaft`
//! `__cluster_metadata` namespace
//! ([`KraftMetadataRecord`](crate::records::metadata::record::KraftMetadataRecord)),
//! even though the small integers overlap.
//!
//! This module encodes and decodes through the same `AbstractApiMessageSerde`
//! value envelope that the JVM `RemoteLogMetadataSerde` uses: `frameVersion=1`,
//! apiKey, apiVersion, and body.

use bytes::{Bytes, BytesMut};

use crate::{
    Decode, Encode, ProtocolError,
    owned::{
        remote_log_segment_metadata_record::RemoteLogSegmentMetadataRecord,
        remote_log_segment_metadata_snapshot_record::RemoteLogSegmentMetadataSnapshotRecord,
        remote_log_segment_metadata_update_record::RemoteLogSegmentMetadataUpdateRecord,
        remote_partition_delete_metadata_record::RemotePartitionDeleteMetadataRecord,
    },
    records::metadata::envelope::{decode_value_header, encode_value},
};

/// One `__remote_log_metadata` record, which is the value of a Kafka `Record`.
///
/// All four record schemas are version 0, so the apiVersion is always written
/// as 0.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoteLogMetadataRecord {
    SegmentMetadata(RemoteLogSegmentMetadataRecord), // apiKey 0
    SegmentMetadataUpdate(RemoteLogSegmentMetadataUpdateRecord), // apiKey 1
    PartitionDelete(RemotePartitionDeleteMetadataRecord), // apiKey 2
    SegmentMetadataSnapshot(RemoteLogSegmentMetadataSnapshotRecord), // apiKey 3
    /// A record this build does not model. The body is the post-envelope bytes.
    Unknown {
        api_key: u32,
        api_version: u32,
        body: Bytes,
    },
}

/// Narrow the envelope's apiVersion to the `i16` the generated codecs take.
///
/// # Errors
/// Returns [`ProtocolError::SchemaMismatch`] if the declared version does not
/// fit in `i16`. No real remote-log metadata record version is that large.
fn api_version_to_i16(version: u32) -> Result<i16, ProtocolError> {
    i16::try_from(version)
        .map_err(|_| ProtocolError::SchemaMismatch("remote-log metadata record apiVersion"))
}

impl RemoteLogMetadataRecord {
    /// The fixed remote-log metadata-record apiKey this variant encodes as.
    #[must_use]
    pub fn api_key(&self) -> u32 {
        match self {
            Self::SegmentMetadata(_) => 0,
            Self::SegmentMetadataUpdate(_) => 1,
            Self::PartitionDelete(_) => 2,
            Self::SegmentMetadataSnapshot(_) => 3,
            Self::Unknown { api_key, .. } => *api_key,
        }
    }

    /// Encode to value bytes: the envelope, then the body.
    ///
    /// All real records are apiVersion 0.
    ///
    /// # Errors
    /// Propagates a [`ProtocolError`] from the underlying message encoder.
    pub fn encode_value(&self) -> Result<Bytes, ProtocolError> {
        let api_key = self.api_key();
        let v: i16 = 0;
        let mut body = BytesMut::new();
        match self {
            Self::SegmentMetadata(r) => r.encode(&mut body, v)?,
            Self::SegmentMetadataUpdate(r) => r.encode(&mut body, v)?,
            Self::PartitionDelete(r) => r.encode(&mut body, v)?,
            Self::SegmentMetadataSnapshot(r) => r.encode(&mut body, v)?,
            Self::Unknown {
                body: raw,
                api_version,
                ..
            } => {
                return Ok(encode_value(api_key, *api_version, raw));
            }
        }
        Ok(encode_value(api_key, 0, &body))
    }

    /// Decode one record from its value bytes.
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] if the envelope or body cannot be decoded, or
    /// if a modeled body leaves trailing bytes.
    pub fn decode_value(value: &[u8]) -> Result<Self, ProtocolError> {
        let mut cur: &[u8] = value;
        let hdr = decode_value_header(&mut cur)
            .map_err(|_| ProtocolError::SchemaMismatch("remote-log metadata record envelope"))?;
        let v = api_version_to_i16(hdr.api_version)?;
        let rec = match hdr.api_key {
            0 => Self::SegmentMetadata(RemoteLogSegmentMetadataRecord::decode(&mut cur, v)?),
            1 => Self::SegmentMetadataUpdate(RemoteLogSegmentMetadataUpdateRecord::decode(
                &mut cur, v,
            )?),
            2 => Self::PartitionDelete(RemotePartitionDeleteMetadataRecord::decode(&mut cur, v)?),
            3 => Self::SegmentMetadataSnapshot(RemoteLogSegmentMetadataSnapshotRecord::decode(
                &mut cur, v,
            )?),
            other => {
                // Unknown records keep their raw post-envelope bytes verbatim,
                // so trailing bytes are intentional here (not checked below).
                return Ok(Self::Unknown {
                    api_key: other,
                    api_version: hdr.api_version,
                    body: Bytes::copy_from_slice(cur),
                });
            }
        };
        // A modeled record body must consume the whole value. Trailing bytes mean
        // the record carries fields this build does not model — fail loudly so a
        // round-trip would not silently re-encode shorter (mirrors the
        // trailing-byte rejection in the `RecordBatch`/`Record` layers).
        if !cur.is_empty() {
            return Err(ProtocolError::SchemaMismatch(
                "trailing bytes after remote-log metadata record body",
            ));
        }
        Ok(rec)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn segment_record_roundtrips_through_dispatch() {
        let rec =
            RemoteLogMetadataRecord::SegmentMetadata(RemoteLogSegmentMetadataRecord::default());
        let bytes = rec.encode_value().expect("encodes");
        let back = RemoteLogMetadataRecord::decode_value(&bytes).expect("decodes");
        assert2::assert!(back == rec);
    }

    #[test]
    fn unknown_api_key_is_preserved() {
        use crate::records::metadata::envelope::encode_value;
        // apiKey 99 is not modeled.
        let bytes = encode_value(99, 0, &[0xAA]);
        let rec = RemoteLogMetadataRecord::decode_value(&bytes).expect("decodes");
        assert2::assert!(matches!(
            rec,
            RemoteLogMetadataRecord::Unknown { api_key: 99, .. }
        ));
    }
}
