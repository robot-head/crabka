use bytes::{Buf, BufMut};

use crate::primitives::string_bytes::{
    compact_string_len, get_compact_string_owned, put_compact_string,
};
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{Decode, Encode, ProtocolError, UnknownTaggedFields};

/// `ApiVersionsRequest`, owned flavor.
///
/// Field availability by version (per upstream schema):
/// - `client_software_name`, `client_software_version`: v3+
///
/// Version 4 (Kafka 4.2, KAFKA-17011) adds no new fields; the bump fixes a
/// response encoding bug. Encode/decode behaviour is identical to v3.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsRequest {
    pub client_software_name: String,
    pub client_software_version: String,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

pub const API_KEY: i16 = 18;
pub const MIN_VERSION: i16 = 0;
/// Schema `validVersions`: "0-4" (Kafka 4.2.0).
pub const MAX_VERSION: i16 = 4;
/// Schema `flexibleVersions`: "3+".
pub const FLEXIBLE_MIN: i16 = 3;

fn is_flexible(version: i16) -> bool {
    version >= FLEXIBLE_MIN
}

impl Encode for ApiVersionsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if version >= 3 {
            put_compact_string(buf, &self.client_software_name);
            put_compact_string(buf, &self.client_software_version);
            // No known tagged fields on this message; emit only unknown.
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        // v0..=v2 have an empty body.
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        if !is_flexible(version) {
            return 0;
        }
        let known: &[(u32, usize)] = &[];
        compact_string_len(&self.client_software_name)
            + compact_string_len(&self.client_software_version)
            + tagged_fields_len(known, &self.unknown_tagged_fields)
    }
}

impl<'de> Decode<'de> for ApiVersionsRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if !is_flexible(version) {
            return Ok(Self::default());
        }
        let client_software_name = get_compact_string_owned(buf)?;
        let client_software_version = get_compact_string_owned(buf)?;
        let unknown = read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        Ok(Self {
            client_software_name,
            client_software_version,
            unknown_tagged_fields: unknown,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn v0_is_empty() {
        let req = ApiVersionsRequest::default();
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 0).unwrap();
        assert!(buf.is_empty());
        let mut cur = &buf[..];
        let decoded = ApiVersionsRequest::decode(&mut cur, 0).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka".to_string(),
            client_software_version: "0.0.0".to_string(),
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        assert_eq!(req.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsRequest::decode(&mut cur, 3).unwrap();
        assert_eq!(decoded, req);
        // No trailing bytes.
        assert!(cur.is_empty());
    }

    #[test]
    fn rejects_unsupported_version() {
        let req = ApiVersionsRequest::default();
        let mut buf = BytesMut::new();
        assert!(matches!(
            req.encode(&mut buf, 99),
            Err(ProtocolError::UnsupportedVersion { api_key: 18, version: 99 })
        ));
    }
}
