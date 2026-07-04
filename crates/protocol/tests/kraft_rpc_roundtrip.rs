//! Byte-identity: decode each captured KIP-595 RPC frame (header + body, from a
//! real `mirror.gcr.io/apache/kafka:4.0.0` 3-node controller quorum) through the generated
//! types and re-encode, asserting the bytes are unchanged.

use std::path::Path;

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        begin_quorum_epoch_request::BeginQuorumEpochRequest,
        begin_quorum_epoch_response::BeginQuorumEpochResponse,
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::DescribeQuorumResponse,
        end_quorum_epoch_request::EndQuorumEpochRequest,
        end_quorum_epoch_response::EndQuorumEpochResponse, fetch_request::FetchRequest,
        fetch_response::FetchResponse, request_header::RequestHeader,
        response_header::ResponseHeader, vote_request::VoteRequest, vote_response::VoteResponse,
    },
};

/// Header version for a flexible message: `RequestHeader` v2 / `ResponseHeader` v1.
const FLEX_REQ_HDR: i16 = 2;
const FLEX_RESP_HDR: i16 = 1;

fn roundtrip_request<T>(frame: &[u8], api_version: i16)
where
    T: for<'de> Decode<'de> + Encode,
{
    let mut cur: &[u8] = frame;
    let hdr = RequestHeader::decode(&mut cur, FLEX_REQ_HDR).expect("request header decodes");
    let body = T::decode(&mut cur, api_version).expect("request body decodes");
    assert!(cur.is_empty(), "trailing bytes after request body");
    let mut out = BytesMut::new();
    hdr.encode(&mut out, FLEX_REQ_HDR)
        .expect("header re-encodes");
    body.encode(&mut out, api_version).expect("body re-encodes");
    assert!(out.as_ref() == frame, "request frame not byte-identical");
}

fn roundtrip_response<T>(frame: &[u8], api_version: i16)
where
    T: for<'de> Decode<'de> + Encode,
{
    let mut cur: &[u8] = frame;
    let hdr = ResponseHeader::decode(&mut cur, FLEX_RESP_HDR).expect("response header decodes");
    let body = T::decode(&mut cur, api_version).expect("response body decodes");
    assert!(cur.is_empty(), "trailing bytes after response body");
    let mut out = BytesMut::new();
    hdr.encode(&mut out, FLEX_RESP_HDR)
        .expect("header re-encodes");
    body.encode(&mut out, api_version).expect("body re-encodes");
    assert!(out.as_ref() == frame, "response frame not byte-identical");
}

fn rpc_frame_roundtrips(path: &Path) -> datatest_stable::Result<()> {
    let frame = std::fs::read(path)?;
    match path.file_name().and_then(|name| name.to_str()) {
        Some("vote_request.bin") => roundtrip_request::<VoteRequest>(&frame, 2),
        Some("vote_response.bin") => roundtrip_response::<VoteResponse>(&frame, 2),
        Some("begin_quorum_epoch_request.bin") => {
            roundtrip_request::<BeginQuorumEpochRequest>(&frame, 1);
        }
        Some("begin_quorum_epoch_response.bin") => {
            roundtrip_response::<BeginQuorumEpochResponse>(&frame, 1);
        }
        Some("end_quorum_epoch_request.bin") => {
            roundtrip_request::<EndQuorumEpochRequest>(&frame, 1);
        }
        Some("end_quorum_epoch_response.bin") => {
            roundtrip_response::<EndQuorumEpochResponse>(&frame, 1);
        }
        Some("describe_quorum_request.bin") => {
            roundtrip_request::<DescribeQuorumRequest>(&frame, 2);
        }
        Some("describe_quorum_response.bin") => {
            roundtrip_response::<DescribeQuorumResponse>(&frame, 2);
        }
        Some("fetch_request.bin") => roundtrip_request::<FetchRequest>(&frame, 17),
        Some("fetch_response.bin") => roundtrip_response::<FetchResponse>(&frame, 17),
        other => panic!("unexpected RPC fixture {other:?}"),
    }
    Ok(())
}

datatest_stable::harness! {
    { test = rpc_frame_roundtrips, root = "tests/fixtures/rpc", pattern = r".*\.bin$" },
}
