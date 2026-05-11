#![cfg(feature = "arbitrary")]

use arbitrary::{Arbitrary, Unstructured};

use crate::owned::api_versions_request::ApiVersionsRequest;
use crate::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crate::UnknownTaggedFields;

fn ascii(u: &mut Unstructured, min: usize, max: usize) -> arbitrary::Result<String> {
    let len = u.int_in_range(min..=max)?;
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let c: u8 = u.int_in_range(0x20..=0x7E)?;
        s.push(c as char);
    }
    Ok(s)
}

impl<'a> Arbitrary<'a> for ApiVersionsRequest {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            client_software_name: ascii(u, 0, 32)?,
            client_software_version: ascii(u, 0, 32)?,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }
}

impl<'a> Arbitrary<'a> for ApiVersion {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            api_key: u.arbitrary()?,
            min_version: u.arbitrary()?,
            max_version: u.arbitrary()?,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }
}

impl<'a> Arbitrary<'a> for ApiVersionsResponse {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let n = u.int_in_range(0..=8usize)?;
        let mut api_keys = Vec::with_capacity(n);
        for _ in 0..n {
            api_keys.push(ApiVersion::arbitrary(u)?);
        }
        Ok(Self {
            error_code: u.arbitrary()?,
            api_keys,
            throttle_time_ms: u.arbitrary()?,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }
}
