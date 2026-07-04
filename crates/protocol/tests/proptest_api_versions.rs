use arbitrary::Arbitrary;
use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode,
    owned::{api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse},
};
use proptest::prelude::*;

fn arb_request() -> impl Strategy<Value = ApiVersionsRequest> {
    (any::<Vec<u8>>(), 0i64..1024).prop_map(|(seed, _)| {
        let mut u = arbitrary::Unstructured::new(&seed);
        ApiVersionsRequest::arbitrary(&mut u).unwrap_or_default()
    })
}

fn arb_response() -> impl Strategy<Value = ApiVersionsResponse> {
    any::<Vec<u8>>().prop_map(|seed| {
        let mut u = arbitrary::Unstructured::new(&seed);
        ApiVersionsResponse::arbitrary(&mut u).unwrap_or_default()
    })
}

proptest! {
    #[test]
    fn request_v3_roundtrip(v in arb_request()) {
        let mut buf = BytesMut::new();
        v.encode(&mut buf, 3).unwrap();
        prop_assert_eq!(v.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsRequest::decode(&mut cur, 3).unwrap();
        prop_assert_eq!(decoded, v);
        prop_assert!(cur.is_empty());
    }

    #[test]
    fn response_v3_roundtrip(v in arb_response()) {
        let mut buf = BytesMut::new();
        v.encode(&mut buf, 3).unwrap();
        prop_assert_eq!(v.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsResponse::decode(&mut cur, 3).unwrap();
        prop_assert_eq!(decoded, v);
        prop_assert!(cur.is_empty());
    }

    #[test]
    fn response_v0_roundtrip(v in arb_response()) {
        let mut buf = BytesMut::new();
        v.encode(&mut buf, 0).unwrap();
        prop_assert_eq!(v.encoded_len(0), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsResponse::decode(&mut cur, 0).unwrap();
        // v0 doesn't include throttle_time_ms or any flexible/tagged fields — normalize.
        let default = ApiVersionsResponse::default();
        let mut expected = v.clone();
        expected.throttle_time_ms = 0;
        // Tagged fields don't exist at v0; after decode they will be their schema defaults.
        expected.supported_features = default.supported_features.clone();
        expected.finalized_features_epoch = default.finalized_features_epoch;
        expected.finalized_features = default.finalized_features.clone();
        expected.zk_migration_ready = default.zk_migration_ready;
        prop_assert_eq!(decoded, expected);
    }
}
