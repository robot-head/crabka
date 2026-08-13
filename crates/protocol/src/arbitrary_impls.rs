#![cfg(feature = "arbitrary")]

use arbitrary::{Arbitrary, Unstructured};

use crate::{
    UnknownTaggedFields,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
    },
};

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
            cluster_id: u
                .arbitrary::<bool>()?
                .then(|| ascii(u, 0, 32))
                .transpose()?,
            node_id: u.arbitrary()?,
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
            supported_features: Vec::new(),
            finalized_features_epoch: 0,
            finalized_features: Vec::new(),
            zk_migration_ready: false,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn ascii_uses_input_and_stays_printable() {
        let mut found = None;
        for seed in 0u8..=u8::MAX {
            let data = [seed, seed.wrapping_mul(37), seed.wrapping_add(91), 0x7E];
            let mut u = Unstructured::new(&data);
            if let Ok(s) = ascii(&mut u, 1, 8)
                && !s.is_empty()
                && s != "xyzzy"
            {
                found = Some(s);
                break;
            }
        }

        let s = found.expect("expected at least one generated string");
        assert2::assert!(s.bytes().all(|b| (0x20..=0x7E).contains(&b)));
    }

    #[test]
    fn arbitrary_api_versions_request_can_be_non_default() {
        let mut found = false;
        for seed in 0u8..=u8::MAX {
            let data = [seed, seed.wrapping_add(1), seed.wrapping_add(2), 0x55, 0xAA];
            let mut u = Unstructured::new(&data);
            if let Ok(req) = ApiVersionsRequest::arbitrary(&mut u)
                && req != ApiVersionsRequest::default()
            {
                assert2::assert!(req.client_software_name.bytes().all(|b| b.is_ascii()));
                assert2::assert!(req.client_software_version.bytes().all(|b| b.is_ascii()));
                found = true;
                break;
            }
        }

        assert2::assert!(found);
    }

    #[test]
    fn arbitrary_api_version_can_be_non_default() {
        let mut found = false;
        for seed in 0u8..=u8::MAX {
            let data = [
                seed,
                seed.wrapping_mul(3),
                seed.wrapping_add(5),
                seed.wrapping_add(8),
                seed.wrapping_add(13),
                seed.wrapping_add(21),
            ];
            let mut u = Unstructured::new(&data);
            if let Ok(version) = ApiVersion::arbitrary(&mut u)
                && version != ApiVersion::default()
            {
                found = true;
                break;
            }
        }

        assert2::assert!(found);
    }

    #[test]
    fn arbitrary_api_versions_response_can_be_non_default() {
        let mut found = false;
        for seed in 0u8..=u8::MAX {
            let data = [
                seed,
                seed.wrapping_add(1),
                seed.wrapping_add(2),
                seed.wrapping_add(3),
                seed.wrapping_add(4),
                seed.wrapping_add(5),
                seed.wrapping_add(6),
                seed.wrapping_add(7),
                seed.wrapping_add(8),
            ];
            let mut u = Unstructured::new(&data);
            if let Ok(resp) = ApiVersionsResponse::arbitrary(&mut u)
                && resp != ApiVersionsResponse::default()
            {
                found = true;
                break;
            }
        }

        assert2::assert!(found);
    }
}
