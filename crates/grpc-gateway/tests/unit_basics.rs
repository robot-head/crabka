use bytes::Bytes;
use crabka_grpc_gateway::codec::{RawCodec, RecordCodec};

#[test]
fn raw_codec_is_identity() {
    let codec = RawCodec;
    let v = Bytes::from_static(b"hello");
    assert_eq!(codec.encode_value("t", v.clone()), v);
    assert_eq!(codec.decode_value("t", v.clone()), v);
}
