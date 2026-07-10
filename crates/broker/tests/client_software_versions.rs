// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` in test files
// (same upstream bug as `tests/mtls.rs` etc).
#![allow(clippy::pedantic)]

//! KIP-511 — `ApiVersions` v3+ client-information validation and the
//! `client_software_versions_total` Prometheus counter.
//!
//! These tests boot a broker on a plaintext loopback listener with the
//! Prometheus exporter bound on `127.0.0.1:0`. Each test drives a raw
//! `ApiVersions` request over TCP (rather than going through the
//! `Client`) so we can pin the version and the exact
//! `client_software_name` / `client_software_version` bytes the broker
//! sees.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use crabka_protocol::{
    Decode, Encode,
    owned::{api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse},
};
use crabka_security::ListenerProtocol;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const INVALID_REQUEST: i16 = 42;

/// Build a broker with the metrics endpoint enabled. Returns the Kafka
/// listener address, the metrics endpoint address, and the
/// `BrokerHandle` so the test can shut down cleanly.
async fn boot() -> (
    SocketAddr,
    SocketAddr,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "PLAINTEXT".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::Plaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "PLAINTEXT".into();
    cfg.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());

    let handle = Broker::start(cfg).await.expect("broker start");
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");
    (kafka_addr, metrics_addr, handle, tempdir)
}

/// Send one ApiVersions request at the requested version with the given
/// client-info fields and return the decoded response.
async fn send_api_versions(
    addr: SocketAddr,
    version: i16,
    software_name: &str,
    software_version: &str,
) -> io::Result<ApiVersionsResponse> {
    let req = ApiVersionsRequest {
        client_software_name: software_name.to_string(),
        client_software_version: software_version.to_string(),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;

    // ApiVersions request header is v2 (flexible) on v3+; v1 (plain) on
    // v0-2. Either way the response header is v0 — ApiVersions intentionally
    // keeps its response header at v0 across versions so v0 clients can
    // parse the error code on negotiated downgrade.
    let flexible = version >= 3;

    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(18); // api_key = ApiVersions
    frame.put_i16(version);
    frame.put_i32(99); // correlation_id
    let client_id = "crabka-kip-511-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // header tagged-fields byte
    }
    frame.put_slice(&body);

    let mut stream = TcpStream::connect(addr).await?;
    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;
    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32();
    // ApiVersionsResponse uses the v0 response header (no tagged byte)
    // even on flexible versions — the request decodes the response body
    // directly at the negotiated version.
    ApiVersionsResponse::decode(&mut cur, version)
        .map_err(|e| io::Error::other(format!("ApiVersionsResponse decode: {e}")))
}

async fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let s = String::from_utf8(buf).unwrap();
    let body_start = s.find("\r\n\r\n").map_or(0, |i| i + 4);
    s[body_start..].to_string()
}

// ── KIP-511 validation paths ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_valid_client_info_accepted() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "crabka-client-core", "0.1.1")
        .await
        .expect("ApiVersions");
    assert!(
        (resp.error_code, resp.api_keys.is_empty()) == (0, false),
        "valid v3 must succeed with API list: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_empty_software_name_rejected_with_invalid_request() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "", "1.0.0")
        .await
        .expect("ApiVersions");
    assert!(
        (resp.error_code, resp.api_keys.is_empty()) == (INVALID_REQUEST, true),
        "empty name rejection mismatch: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_empty_software_version_rejected_with_invalid_request() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "crabka", "")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == INVALID_REQUEST,
        "empty version must be rejected: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_invalid_char_in_name_rejected() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "has space", "1.0.0")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == INVALID_REQUEST,
        "spaces must be rejected: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v3_leading_dash_in_version_rejected() {
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 3, "crabka", "-1.0.0")
        .await
        .expect("ApiVersions");
    assert!(
        resp.error_code == INVALID_REQUEST,
        "leading dash must be rejected: {resp:?}"
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_v3_does_not_validate_client_info() {
    // v0-2 don't carry the fields. The codegen leaves them as default
    // empty strings on the wire — KIP-511 says don't validate. Apache
    // Kafka brokers happily accept v0/v1/v2 calls from old clients that
    // don't know about ClientSoftwareName at all.
    let (kafka_addr, _metrics_addr, handle, _td) = boot().await;

    let resp = send_api_versions(kafka_addr, 0, "", "")
        .await
        .expect("ApiVersions");
    assert!(
        (resp.error_code, resp.api_keys.is_empty()) == (0, false),
        "v0 ApiVersions projection mismatch: {resp:?}"
    );

    handle.shutdown().await;
}

// ── Prometheus metric ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_v3_handshake_bumps_client_software_versions_counter() {
    let (kafka_addr, metrics_addr, handle, _td) = boot().await;

    // Drive two distinct (name, version) tuples + one repeat so the
    // expected sample emits with three series, one of them at count 2.
    for _ in 0..2 {
        send_api_versions(kafka_addr, 3, "crabka-it", "1.0.0")
            .await
            .expect("ApiVersions");
    }
    send_api_versions(kafka_addr, 3, "crabka-it", "1.0.1")
        .await
        .expect("ApiVersions");
    send_api_versions(kafka_addr, 3, "another-lib", "9.9.9")
        .await
        .expect("ApiVersions");

    let body = scrape(metrics_addr).await;

    // Three series, one count, plus the family HELP/TYPE lines.
    assert!(
        body.contains("# TYPE crabka_broker_client_software_versions counter"),
        "TYPE line missing in:\n{body}",
    );
    let needle_repeat = "crabka_broker_client_software_versions_total{software_name=\"crabka-it\",software_version=\"1.0.0\"} 2";
    let needle_new = "crabka_broker_client_software_versions_total{software_name=\"crabka-it\",software_version=\"1.0.1\"} 1";
    let needle_other = "crabka_broker_client_software_versions_total{software_name=\"another-lib\",software_version=\"9.9.9\"} 1";
    for needle in [needle_repeat, needle_new, needle_other] {
        assert!(
            body.contains(needle),
            "expected sample {needle:?} not found in:\n{body}",
        );
    }

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_v3_handshake_does_not_bump_counter() {
    let (kafka_addr, metrics_addr, handle, _td) = boot().await;

    // First send a valid one so the family has at least one series and
    // the metric appears in the scrape — otherwise an empty Family of
    // Counters renders no samples at all.
    send_api_versions(kafka_addr, 3, "valid-client", "1.0.0")
        .await
        .expect("ApiVersions");
    // Now an invalid one. The counter must not gain a row labelled with
    // the rejected name/version.
    send_api_versions(kafka_addr, 3, "bad client", "1.0.0")
        .await
        .expect("ApiVersions");

    let body = scrape(metrics_addr).await;
    assert!(
        !body.contains("software_name=\"bad client\""),
        "rejected handshake must not be recorded:\n{body}",
    );
    // Spot-check the valid one is recorded.
    assert!(
        body.contains("software_name=\"valid-client\""),
        "valid handshake must be recorded:\n{body}",
    );

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_v3_handshake_does_not_bump_counter() {
    let (kafka_addr, metrics_addr, handle, _td) = boot().await;

    // v0 ApiVersions has no client-info fields, so the counter (which
    // would label series with empty strings) must not increment.
    send_api_versions(kafka_addr, 0, "", "")
        .await
        .expect("ApiVersions");

    let body = scrape(metrics_addr).await;
    // The metric *family* may still appear in the scrape registration
    // (HELP/TYPE lines emit even with no series), so just check no
    // sample row with software_name="" was emitted.
    assert!(
        !body.contains("crabka_broker_client_software_versions_total{software_name=\"\""),
        "v0 handshake must not be recorded:\n{body}",
    );

    handle.shutdown().await;
}
