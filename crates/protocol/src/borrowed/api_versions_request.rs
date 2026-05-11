use bytes::BufMut;

use crate::owned;
use crate::primitives::string_bytes::{compact_string_len, put_compact_string};
use crate::primitives::string_bytes_borrowed::get_compact_string_borrowed;
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields};

pub use crate::owned::api_versions_request::{API_KEY, FLEXIBLE_MIN, MAX_VERSION, MIN_VERSION};

fn is_flexible(version: i16) -> bool {
    version >= FLEXIBLE_MIN
}

/// `ApiVersionsRequest`, borrowed flavor. Strings reference the input buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequest<'a> {
    pub client_software_name: &'a str,
    pub client_software_version: &'a str,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl<'a> Default for ApiVersionsRequest<'a> {
    fn default() -> Self {
        Self {
            client_software_name: "",
            client_software_version: "",
            unknown_tagged_fields: Default::default(),
        }
    }
}

impl<'a> ApiVersionsRequest<'a> {
    pub fn to_owned(&self) -> owned::api_versions_request::ApiVersionsRequest {
        owned::api_versions_request::ApiVersionsRequest {
            client_software_name: self.client_software_name.to_string(),
            client_software_version: self.client_software_version.to_string(),
            unknown_tagged_fields: self.unknown_tagged_fields.clone(),
        }
    }
}

impl<'a> Encode for ApiVersionsRequest<'a> {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if version >= 3 {
            put_compact_string(buf, self.client_software_name);
            put_compact_string(buf, self.client_software_version);
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        if !is_flexible(version) {
            return 0;
        }
        let known: &[(u32, usize)] = &[];
        compact_string_len(self.client_software_name)
            + compact_string_len(self.client_software_version)
            + tagged_fields_len(known, &self.unknown_tagged_fields)
    }
}

impl<'de> DecodeBorrow<'de> for ApiVersionsRequest<'de> {
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if !is_flexible(version) {
            return Ok(Self::default());
        }
        let client_software_name = get_compact_string_borrowed(buf)?;
        let client_software_version = get_compact_string_borrowed(buf)?;
        let mut tail: &[u8] = buf;
        let unknown_tagged_fields = read_tagged_fields(&mut tail, |_, _| Ok(false))?;
        *buf = tail;
        Ok(Self { client_software_name, client_software_version, unknown_tagged_fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn borrowed_v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        let frozen = buf.freeze();
        let mut cur: &[u8] = &frozen;
        let decoded = ApiVersionsRequest::decode_borrow(&mut cur, 3).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn to_owned_matches_owned_codec() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: Default::default(),
        };
        let mut a = BytesMut::new();
        req.encode(&mut a, 3).unwrap();
        let owned = req.to_owned();
        let mut b = BytesMut::new();
        owned.encode(&mut b, 3).unwrap();
        assert_eq!(a.as_ref(), b.as_ref());
    }
}
