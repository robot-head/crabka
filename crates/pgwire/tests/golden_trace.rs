//! Replays frontend bytes recorded from a real psql/libpq session through our
//! decoder. If libpq frames something we can't parse, this catches it.

use bytes::BytesMut;
use crabka_pgwire::messages::frontend::{self, FrontendMessage, StartupPacket};

fn frontend_bytes(trace: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for line in trace.lines() {
        if let Some(hex) = line.strip_prefix("F ") {
            for i in (0..hex.len()).step_by(2) {
                out.push(u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"));
            }
        }
    }
    out
}

#[test]
fn psql_select1_trace_decodes_cleanly() {
    let trace = include_str!("fixtures/psql-select1.trace");
    let mut buf = BytesMut::from(&frontend_bytes(trace)[..]);

    // Phase 1: startup packet (sslmode=disable -> plain StartupMessage first).
    // Tolerate an optional leading GssEncRequest packet (psql 14 on some builds
    // sends one even with sslmode=disable).
    let startup = loop {
        let pkt = frontend::decode_startup(&mut buf)
            .expect("valid startup")
            .expect("complete");
        match pkt {
            StartupPacket::Startup { .. } => break pkt,
            StartupPacket::SslRequest | StartupPacket::GssEncRequest => {
                // These negotiation packets are proxied to the real server which
                // replies N; the client then sends the real startup.
            }
            other @ StartupPacket::CancelRequest { .. } => {
                panic!("unexpected pre-startup packet: {other:?}")
            }
        }
    };

    let StartupPacket::Startup { params } = startup else {
        panic!("expected StartupMessage, got {startup:?}");
    };
    assert!(
        params.iter().any(|(k, v)| k == "user" && v == "postgres"),
        "startup params should contain user=postgres; got {params:?}"
    );

    // Phase 2: every remaining tagged message must decode.
    let mut decoded = Vec::new();
    while !buf.is_empty() {
        match frontend::decode_message(&mut buf).expect("valid message") {
            Some(msg) => decoded.push(msg),
            None => panic!("trace ends mid-message: {} bytes left", buf.len()),
        }
    }
    assert!(
        decoded
            .iter()
            .any(|m| matches!(m, FrontendMessage::Query { sql } if sql == "SELECT 1")),
        "expected the SELECT 1 query in {decoded:?}"
    );
    assert!(
        decoded
            .iter()
            .any(|m| matches!(m, FrontendMessage::Terminate)),
        "expected Terminate message in {decoded:?}"
    );
}
