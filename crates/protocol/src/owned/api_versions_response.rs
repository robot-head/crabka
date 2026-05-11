use bytes::{Buf, BufMut};

use crate::primitives::fixed::{get_i16, get_i32, put_i16, put_i32};
use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};
use crate::tagged_fields::{WriteTaggedFields, read_tagged_fields, tagged_fields_len};
use crate::{Decode, Encode, ProtocolError, UnknownTaggedFields};

/// `ApiVersionsResponse`, owned flavor.
///
/// Field availability by version (per upstream schema `ApiVersionsResponse.json`):
/// - `error_code: int16`          — v0+  (non-tagged)
/// - `api_keys: []ApiVersion`     — v0+  (non-tagged)
/// - `throttle_time_ms: int32`    — v1+  (non-tagged, ignorable)
/// - `supported_features`         — v3+  (tagged, tag=0) — deferred to coverage
/// - `finalized_features_epoch`   — v3+  (tagged, tag=1) — deferred to coverage
/// - `finalized_features`         — v3+  (tagged, tag=2) — deferred to coverage
/// - `zk_migration_ready`         — v3+  (tagged, tag=3) — deferred to coverage
///
/// All tagged fields flow through `unknown_tagged_fields`; no known-tag decoding
/// is performed in this pilot.
///
/// Version 4 (Kafka 4.2, KAFKA-17011) adds no new fields; the bump fixes a
/// `SupportedFeatures.MinVersion = 0` encoding bug in the response.
/// Encode/decode behaviour is identical to v3.
pub const API_KEY: i16 = 18;
pub const MIN_VERSION: i16 = 0;
/// Schema `validVersions`: "0-4" (Kafka 4.2.0). v4 adds no new fields.
pub const MAX_VERSION: i16 = 4;
/// Schema `flexibleVersions`: "3+".
pub const FLEXIBLE_MIN: i16 = 3;

fn is_flexible(version: i16) -> bool {
    version >= FLEXIBLE_MIN
}

/// One entry in the `api_keys` array.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

/// Owned `ApiVersionsResponse`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersion>,
    /// Zero when `version < 1` (field is ignorable).
    pub throttle_time_ms: i32,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

// ── ApiVersion helpers ──────────────────────────────────────────────────────

impl ApiVersion {
    #[allow(clippy::unnecessary_wraps)]
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        put_i16(buf, self.api_key);
        put_i16(buf, self.min_version);
        put_i16(buf, self.max_version);
        if is_flexible(version) {
            // No known tagged fields on ApiVersion in this pilot.
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        let mut n = 6; // api_key + min_version + max_version (3 × i16)
        if is_flexible(version) {
            let known: &[(u32, usize)] = &[];
            n += tagged_fields_len(known, &self.unknown_tagged_fields);
        }
        n
    }

    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        let api_key = get_i16(buf)?;
        let min_version = get_i16(buf)?;
        let max_version = get_i16(buf)?;
        let unknown_tagged_fields = if is_flexible(version) {
            // All declared tagged fields (tags 0-3 on the outer message) live
            // on ApiVersionsResponse, not on ApiVersion.  ApiVersion has no
            // declared tagged fields; return Ok(false) for anything we see.
            read_tagged_fields(buf, |_, _| Ok(false))?
        } else {
            UnknownTaggedFields::default()
        };
        Ok(Self {
            api_key,
            min_version,
            max_version,
            unknown_tagged_fields,
        })
    }
}

// ── Array-length helpers ────────────────────────────────────────────────────

fn put_array_len<B: BufMut>(buf: &mut B, n: usize, flexible: bool) {
    if flexible {
        // Compact array: encode (n + 1) as uvarint.
        put_uvarint(buf, u32::try_from(n + 1).unwrap());
    } else {
        put_i32(buf, i32::try_from(n).unwrap());
    }
}

fn array_len_len(n: usize, flexible: bool) -> usize {
    if flexible {
        uvarint_len(u32::try_from(n + 1).unwrap())
    } else {
        4
    }
}

fn get_array_len<B: Buf>(buf: &mut B, flexible: bool) -> Result<usize, ProtocolError> {
    if flexible {
        let raw = get_uvarint(buf)?;
        if raw == 0 {
            return Err(ProtocolError::InvalidValue("non-nullable array was null"));
        }
        Ok((raw - 1) as usize)
    } else {
        let n = get_i32(buf)?;
        if n < 0 {
            return Err(ProtocolError::InvalidValue(
                "non-nullable array had negative length",
            ));
        }
        #[allow(clippy::cast_sign_loss)]
        Ok(n as usize)
    }
}

// ── Encode / Decode for ApiVersionsResponse ─────────────────────────────────

impl Encode for ApiVersionsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion {
                api_key: API_KEY,
                version,
            });
        }
        let flex = is_flexible(version);
        put_i16(buf, self.error_code);
        put_array_len(buf, self.api_keys.len(), flex);
        for v in &self.api_keys {
            v.encode(buf, version)?;
        }
        if version >= 1 {
            put_i32(buf, self.throttle_time_ms);
        }
        if flex {
            // Known tagged fields (supported_features, finalized_features_epoch,
            // finalized_features, zk_migration_ready) are deferred; only unknown
            // fields are written.
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        let flex = is_flexible(version);
        let mut n = 2 + array_len_len(self.api_keys.len(), flex); // error_code + array length prefix
        for v in &self.api_keys {
            n += v.encoded_len(version);
        }
        if version >= 1 {
            n += 4; // throttle_time_ms
        }
        if flex {
            let known: &[(u32, usize)] = &[];
            n += tagged_fields_len(known, &self.unknown_tagged_fields);
        }
        n
    }
}

impl Decode<'_> for ApiVersionsResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion {
                api_key: API_KEY,
                version,
            });
        }
        let flex = is_flexible(version);
        let error_code = get_i16(buf)?;
        let n = get_array_len(buf, flex)?;
        let mut api_keys = Vec::with_capacity(n);
        for _ in 0..n {
            api_keys.push(ApiVersion::decode(buf, version)?);
        }
        let throttle_time_ms = if version >= 1 { get_i32(buf)? } else { 0 };
        let unknown_tagged_fields = if flex {
            // Known tagged fields are deferred; return Ok(false) for all tags
            // so they flow into unknown_tagged_fields.
            read_tagged_fields(buf, |_, _| Ok(false))?
        } else {
            UnknownTaggedFields::default()
        };
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            unknown_tagged_fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn sample(version: i16) -> ApiVersionsResponse {
        ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: 0,
                    min_version: 0,
                    max_version: 10,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: 1,
                    min_version: 0,
                    max_version: 17,
                    ..Default::default()
                },
            ],
            throttle_time_ms: if version >= 1 { 5 } else { 0 },
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    #[test]
    fn v0_roundtrip() {
        let r = sample(0);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 0).unwrap();
        assert_eq!(r.encoded_len(0), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 0).unwrap(), r);
        assert!(cur.is_empty());
    }

    #[test]
    fn v1_includes_throttle_time() {
        let r = sample(1);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 1).unwrap();
        assert_eq!(r.encoded_len(1), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 1).unwrap(), r);
        assert!(cur.is_empty());
    }

    #[test]
    fn v3_flexible_roundtrip() {
        let r = sample(3);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 3).unwrap();
        assert_eq!(r.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 3).unwrap(), r);
        assert!(cur.is_empty());
    }
}
