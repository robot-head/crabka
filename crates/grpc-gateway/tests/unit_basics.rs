use bytes::Bytes;
use crabka_grpc_gateway::codec::{RawCodec, RecordCodec};

#[test]
fn raw_codec_is_identity() {
    let codec = RawCodec;
    let v = Bytes::from_static(b"hello");
    assert_eq!(codec.encode_value("t", v.clone()), v);
    assert_eq!(codec.decode_value("t", v.clone()), v);
}

#[test]
fn partition_for_is_deterministic_and_bounded() {
    use crabka_grpc_gateway::dedup::partition_for;
    let a = partition_for("order-42", 16);
    let b = partition_for("order-42", 16);
    assert_eq!(a, b);
    assert!(a < 16);
    let spread: std::collections::HashSet<u32> = (0..100)
        .map(|i| partition_for(&format!("k{i}"), 16))
        .collect();
    assert!(spread.len() > 1);
}

#[test]
fn claim_value_round_trips() {
    use crabka_grpc_gateway::dedup::store::ClaimValue;
    let c = ClaimValue {
        topic: "t".into(),
        partition: 3,
        offset: 99,
    };
    let bytes = serde_json::to_vec(&c).unwrap();
    let back: ClaimValue = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(c, back);
}
