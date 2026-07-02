use bytes::Bytes;
use crabka_grpc_gateway::codec::{EncodeBody, RawCodec, RecordCodec};

#[tokio::test]
async fn raw_codec_is_identity() {
    let codec = RawCodec;
    let v = Bytes::from_static(b"hello");
    // Raw passthrough: encode returns the bytes verbatim.
    let encoded = codec.encode("t", EncodeBody::Raw(v.clone())).await.unwrap();
    assert_eq!(encoded, v);
    // Decode returns the bytes verbatim with no schema/structured view.
    let decoded = codec.decode("t", v.clone()).await.unwrap();
    assert_eq!(decoded.value, v);
    assert!(decoded.schema.is_none());
    assert!(decoded.json.is_none());
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

#[tokio::test]
async fn dedup_produce_before_ownership_is_unavailable() {
    use std::sync::Arc;

    use crabka_grpc_gateway::{
        dedup::{DedupEngine, store::DedupStore},
        error::GatewayError,
        types::GatewayRecord,
    };

    // The store is constructed but run_ownership has never run, so owns()
    // returns false for every partition. The engine must refuse keyed produces
    // (and `/readyz` stays 503) rather than risk a cold-start double-write.
    // No broker is needed: the ownership check precedes all I/O.
    let store = Arc::new(DedupStore::new(4));
    let engine = DedupEngine::new(
        "127.0.0.1:0",
        "gw-notready",
        "crabka-grpc-dedup",
        "__crabka_grpc_dedup".to_string(),
        4,
        store,
        None,
    );
    let rec = GatewayRecord {
        topic: "t".into(),
        key: None,
        value: Bytes::from_static(b"x"),
        body_structured: None,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("k".into()),
    };
    let err = engine
        .dedup_produce(&rec, Bytes::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(matches!(err, GatewayError::Unavailable));
}
