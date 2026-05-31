//! Byte-identity: decode each captured KIP-595 RPC frame (header + body, from a
//! real `apache/kafka:4.0.0` 3-node controller quorum) through the generated
//! types and re-encode, asserting the bytes are unchanged. Validates the
//! generated RPC codecs against genuine JVM wire.
//!
//! Fixtures (frame minus the 4-byte length prefix) captured per the slice-2
//! plan. Captured versions: `Vote` v2, `BeginQuorumEpoch` v1, `EndQuorumEpoch`
//! v1, `DescribeQuorum` v2, `Fetch` v17 — all flexible at these versions, so the
//! `RequestHeader` is v2 and the `ResponseHeader` is v1.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::owned::request_header::RequestHeader;
use crabka_protocol::owned::response_header::ResponseHeader;
use crabka_protocol::{Decode, Encode};

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

use crabka_protocol::owned::begin_quorum_epoch_request::BeginQuorumEpochRequest;
use crabka_protocol::owned::begin_quorum_epoch_response::BeginQuorumEpochResponse;
use crabka_protocol::owned::describe_quorum_request::DescribeQuorumRequest;
use crabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse;
use crabka_protocol::owned::end_quorum_epoch_request::EndQuorumEpochRequest;
use crabka_protocol::owned::end_quorum_epoch_response::EndQuorumEpochResponse;
use crabka_protocol::owned::fetch_request::FetchRequest;
use crabka_protocol::owned::fetch_response::FetchResponse;
use crabka_protocol::owned::vote_request::VoteRequest;
use crabka_protocol::owned::vote_response::VoteResponse;

#[test]
fn vote_request_roundtrips() {
    roundtrip_request::<VoteRequest>(include_bytes!("fixtures/rpc/vote_request.bin"), 2);
}
#[test]
fn vote_response_roundtrips() {
    roundtrip_response::<VoteResponse>(include_bytes!("fixtures/rpc/vote_response.bin"), 2);
}
#[test]
fn begin_quorum_epoch_request_roundtrips() {
    roundtrip_request::<BeginQuorumEpochRequest>(
        include_bytes!("fixtures/rpc/begin_quorum_epoch_request.bin"),
        1,
    );
}
#[test]
fn begin_quorum_epoch_response_roundtrips() {
    roundtrip_response::<BeginQuorumEpochResponse>(
        include_bytes!("fixtures/rpc/begin_quorum_epoch_response.bin"),
        1,
    );
}
#[test]
fn end_quorum_epoch_request_roundtrips() {
    roundtrip_request::<EndQuorumEpochRequest>(
        include_bytes!("fixtures/rpc/end_quorum_epoch_request.bin"),
        1,
    );
}
#[test]
fn end_quorum_epoch_response_roundtrips() {
    roundtrip_response::<EndQuorumEpochResponse>(
        include_bytes!("fixtures/rpc/end_quorum_epoch_response.bin"),
        1,
    );
}
#[test]
fn describe_quorum_request_roundtrips() {
    roundtrip_request::<DescribeQuorumRequest>(
        include_bytes!("fixtures/rpc/describe_quorum_request.bin"),
        2,
    );
}
#[test]
fn describe_quorum_response_roundtrips() {
    roundtrip_response::<DescribeQuorumResponse>(
        include_bytes!("fixtures/rpc/describe_quorum_response.bin"),
        2,
    );
}
#[test]
fn fetch_request_roundtrips() {
    roundtrip_request::<FetchRequest>(include_bytes!("fixtures/rpc/fetch_request.bin"), 17);
}
#[test]
fn fetch_response_roundtrips() {
    roundtrip_response::<FetchResponse>(include_bytes!("fixtures/rpc/fetch_response.bin"), 17);
}
