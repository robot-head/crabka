use std::fmt::Write;

use crate::emit::common;
use crate::emit::owned::EmitError;
use crate::ir::{FlexibleVersions, MessageSpec};

pub fn emit(spec: &MessageSpec, schemas_version: &str) -> Result<String, EmitError> {
    if spec.name != "ApiVersionsRequest" {
        return Err(EmitError::Unsupported(format!(
            "borrowed emitter does not yet support {}",
            spec.name
        )));
    }
    let api_key = spec.api_key.expect("validated earlier");
    let (flex_min, _) = match spec.flexible_versions {
        FlexibleVersions::Range(r) => (r.min, r.max),
        FlexibleVersions::None => (i16::MAX, i16::MAX),
    };
    let min_version = spec.valid_versions.min;
    let max_version = spec.valid_versions.max;

    let mut out = common::banner(schemas_version);
    out.push_str(STATIC);
    writeln!(out, "pub const API_KEY: i16 = {api_key};").unwrap();
    writeln!(out, "pub const MIN_VERSION: i16 = {min_version};").unwrap();
    writeln!(out, "pub const MAX_VERSION: i16 = {max_version};").unwrap();
    writeln!(out, "pub const FLEXIBLE_MIN: i16 = {flex_min};").unwrap();
    out.push_str(IMPLS);
    Ok(out)
}

const STATIC: &str = r#"
use bytes::BufMut;

use crate::owned;
use crate::primitives::string_bytes::{compact_string_len, put_compact_string};
use crate::primitives::string_bytes_borrowed::get_compact_string_borrowed;
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequest<'a> {
    pub client_software_name: &'a str,
    pub client_software_version: &'a str,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl<'a> Default for ApiVersionsRequest<'a> {
    fn default() -> Self {
        Self { client_software_name: "", client_software_version: "", unknown_tagged_fields: Default::default() }
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

"#;

const IMPLS: &str = r"
fn is_flexible(version: i16) -> bool { version >= FLEXIBLE_MIN }

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
        if !is_flexible(version) { return 0; }
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
";
