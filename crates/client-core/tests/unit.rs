//! MockBroker-based unit tests for `Connection`.
//!
//! These tests spin up an in-process mock Kafka broker and verify that
//! `Connection::connect`, `Connection::send`, and the timeout path all
//! behave correctly without any JVM dependency.
//!
//! Run with: `cargo test -p crabka-client-core --features mock --test unit`

// Doc-comment prose uses Kafka term names that clippy wants backtick-escaped.
// Suppressing here keeps comments readable without cluttering them.
#![allow(clippy::doc_markdown)]

use std::time::Duration;

use assert2::check;
use bytes::BytesMut;
use crabka_client_core::{ClientError, Connection, ConnectionOptions, MockBroker};
// Use the raw constants so we don't need `ProtocolRequest` in scope.
use crabka_protocol::owned::api_versions_request;
use crabka_protocol::{
    Encode,
    owned::{
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        metadata_request as metadata_request_mod,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
    },
};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Encode an `ApiVersionsResponse` at version 0 that advertises:
/// - api_key 18 (ApiVersions): min 0, max 3
/// - api_key  3 (Metadata):    min 0, max 12
///
/// The mock returns this as the response body (after the correlation-id,
/// which `MockBroker` prepends automatically).
fn api_versions_response_v0() -> Vec<u8> {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            },
            ApiVersion {
                api_key: metadata_request_mod::API_KEY,
                min_version: 0,
                max_version: 12,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, 0).unwrap();
    buf.to_vec()
}

/// Encode a response body at the given version, with the correct
/// `ResponseHeader` prefix.
///
/// Kafka's `ResponseHeader`:
/// - v0 (non-flexible): only `correlation_id` (already prepended by MockBroker).
///   No additional bytes.
/// - v1 (flexible, i.e., version >= FLEXIBLE_MIN): one additional byte,
///   the UVARINT-encoded empty tagged-fields count (0x00).
///
/// Exception: `ApiVersionsResponse` always uses ResponseHeader v0 even when
/// the request version is flexible — but the tests here handle MetadataResponse
/// which follows the normal rule.
fn metadata_response_at(version: i16) -> Vec<u8> {
    metadata_response_with_throttle(version, 0)
}

/// Encode a `MetadataResponse` at the given version with a custom
/// `throttle_time_ms`, preceded by the correct `ResponseHeader` prefix.
fn metadata_response_with_throttle(version: i16, throttle_time_ms: i32) -> Vec<u8> {
    use crabka_protocol::owned::metadata_response::FLEXIBLE_MIN;
    let resp = MetadataResponse {
        throttle_time_ms,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    // Prepend ResponseHeader v1 tagged-fields byte for flexible versions.
    if version >= FLEXIBLE_MIN {
        buf.extend_from_slice(&[0x00u8]); // empty tagged fields
    }
    resp.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

// ── tests ────────────────────────────────────────────────────────────────────

/// `Connection::connect` successfully negotiates API versions via the mock.
#[tokio::test]
async fn connect_negotiates_api_versions() {
    let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
        // Only the bootstrap ApiVersions call is expected here.
        assert2::assert!(api_key == api_versions_request::API_KEY);
        Some(api_versions_response_v0())
    })
    .await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();

    check!(
        (
            conn.versions().is_empty(),
            conn.versions().broker_range(api_versions_request::API_KEY),
            conn.versions().broker_range(metadata_request_mod::API_KEY),
        ) == (false, Some((0, 3)), Some((0, 12)))
    );

    conn.close();
    mock.stop();
}

/// When the mock never responds to ApiVersions, `Connection::connect` returns
/// `ClientError::Timeout` once `connect_timeout` elapses.
///
/// Design note: the handler returns `None` (the `Option<Vec<u8>>` sentinel in
/// `MockBroker`'s API) so the broker silently drops the request instead of
/// sending even an empty frame. An empty length-delimited frame would still
/// be a valid frame with 0 body bytes that the client would attempt to decode,
/// producing a codec error rather than a timeout. `None` correctly simulates
/// a hung or unreachable broker.
#[tokio::test]
async fn timeout_when_handler_silent() {
    let mock = MockBroker::start(|_api_key, _version, _corr_id, _body| {
        // Return None to drop the request; the client's connect_timeout fires.
        None
    })
    .await;

    let opts = ConnectionOptions {
        connect_timeout: Duration::from_millis(200),
        request_timeout: Duration::from_secs(30),
        ..ConnectionOptions::default()
    };

    match Connection::connect(mock.addr, opts).await {
        Err(ClientError::Timeout(_)) => { /* expected */ }
        Err(other) => panic!("expected Timeout, got: {other:?}"),
        Ok(_conn) => panic!("connect should have timed out but succeeded"),
    }

    mock.stop();
}

/// A full round-trip: connect (ApiVersions handshake) then send a
/// `MetadataRequest` and receive a `MetadataResponse`.
///
/// The mock encodes the `MetadataResponse` at the same version as the
/// request, which is the version negotiated by the client.
#[tokio::test]
async fn round_trip_metadata_request() {
    let mock = MockBroker::start(|api_key, version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            return Some(api_versions_response_v0());
        }
        if api_key == metadata_request_mod::API_KEY {
            // Encode at the negotiated version so flexible framing matches.
            return Some(metadata_response_at(version));
        }
        None
    })
    .await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();

    // send() should succeed; smoke-test passes on no error.
    let _resp = conn.send(MetadataRequest::default()).await.unwrap();

    conn.close();
    mock.stop();
}

/// Three concurrent `send` calls on the same connection are all dispatched and
/// routed back to the correct caller via correlation IDs.
///
/// The mock increments an atomic counter per Metadata request and stamps the
/// `throttle_time_ms` field with the counter value. After `tokio::join!` all
/// three futures, the three `throttle_time_ms` values should be 0, 1, 2 in
/// some order.
#[tokio::test]
async fn concurrent_sends_get_correct_responses() {
    use std::sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    };

    let counter = Arc::new(AtomicI32::new(0));
    let counter_for_mock = Arc::clone(&counter);

    let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            return Some(api_versions_response_v0());
        }
        if api_key == metadata_request_mod::API_KEY {
            let n = counter_for_mock.fetch_add(1, Ordering::Relaxed);
            return Some(metadata_response_with_throttle(version, n));
        }
        None
    })
    .await;

    let conn = Connection::connect(mock.addr, ConnectionOptions::default())
        .await
        .unwrap();

    // Send three requests concurrently; the connection multiplexes them via
    // correlation IDs and routes each response back to its caller.
    let (r1, r2, r3) = tokio::join!(
        conn.send(MetadataRequest::default()),
        conn.send(MetadataRequest::default()),
        conn.send(MetadataRequest::default()),
    );

    let r1 = r1.unwrap();
    let r2 = r2.unwrap();
    let r3 = r3.unwrap();

    // All three requests received distinct responses stamped 0, 1, 2.
    let mut seen = [
        r1.throttle_time_ms,
        r2.throttle_time_ms,
        r3.throttle_time_ms,
    ];
    seen.sort_unstable();
    assert2::assert!(seen == [0, 1, 2]);

    conn.close();
    mock.stop();
}

/// `Client::refresh_metadata` calls the bootstrap broker, decodes the broker
/// list, and populates the pool's address registry.
///
/// The mock serves two synthetic brokers (ids 1 and 2). After `refresh_metadata`
/// the test verifies the response decoded correctly (2 brokers in the list).
/// We cannot connect to those addresses (they're fake ports the mock isn't
/// listening on), but the registry population is exercised.
#[tokio::test]
async fn client_refresh_metadata_populates_pool() {
    use crabka_protocol::owned::metadata_response::{
        FLEXIBLE_MIN, MetadataResponse, MetadataResponseBroker,
    };

    let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            return Some(api_versions_response_v0());
        }
        if api_key == metadata_request_mod::API_KEY {
            let resp = MetadataResponse {
                brokers: vec![
                    MetadataResponseBroker {
                        node_id: 1,
                        host: "127.0.0.1".into(),
                        port: 9092,
                        ..Default::default()
                    },
                    MetadataResponseBroker {
                        node_id: 2,
                        host: "127.0.0.1".into(),
                        port: 9093,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };
            // Encode the response with the correct ResponseHeader prefix.
            let mut buf = BytesMut::new();
            if version >= FLEXIBLE_MIN {
                buf.extend_from_slice(&[0x00u8]);
            }
            resp.encode(&mut buf, version).unwrap();
            return Some(buf.to_vec());
        }
        None
    })
    .await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(mock.addr.to_string())
        .build()
        .await
        .unwrap();

    let metadata = client.refresh_metadata().await.unwrap();
    assert2::assert!(metadata.brokers.len() == 2);

    // After refresh the pool knows broker 1 and 2's addresses. We can't
    // actually connect to those ports (the mock isn't listening there), but
    // the metadata response decoded correctly and has the right shape.
    client.close();
    mock.stop();
}
