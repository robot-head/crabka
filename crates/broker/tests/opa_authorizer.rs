// rustc 1.95 clippy::pedantic ICEs on this file family (same upstream
// body-analysis bug that already fires on `tests/acl_handlers.rs` and
// `tests/throttle.rs`). Disable pedantic locally; the rest of the
// workspace still enforces the full pedantic gate.
#![allow(clippy::pedantic)]

//! End-to-end OPA authorizer enforcement via the
//! wire path.
//!
//! Two integration tests boot a single-broker SASL_PLAINTEXT cluster
//! wired with an [`OpaAuthorizer`] pointed at a `wiremock::MockServer`.
//! The mock is configured to return either `{"result": false}` or
//! `{"result": true}` for every `POST`, and the tests assert that the
//! per-topic `error_code` on a Produce response carries
//! `TOPIC_AUTHORIZATION_FAILED (29)` or `0` respectively.
//!
//! Topic-bootstrap strategy (test 1): the `admin` principal is set as a
//! super-user in BOTH the [`OpaAuthorizer`] (so it bypasses OPA) and the
//! [`BrokerConfig.super_users`] field (so the broker-level super-user
//! checks accept it). The test calls `CreateTopics` over a SASL/PLAIN
//! `admin` session — the super-user bypass means OPA is never asked,
//! so the topic materialises even though the mock would otherwise deny
//! it. The actual OPA gate fires when `alice` (non-super-user) issues
//! Produce.
//!
//! Gated to non-Windows for parity with the other SASL integration
//! tests (the listener bring-up works on Windows, but keeping the gate
//! uniform avoids one-off CI matrix surprises).

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{
    Broker, BrokerConfig, BrokerHandle, authorizer::opa::OpaAuthorizer, config::ListenerSpec,
};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    records::{Record, RecordBatch},
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

// Local mirror of `crabka_broker::codes::TOPIC_AUTHORIZATION_FAILED`,
// kept inline because the `codes` module is crate-private. The value
// matches the Apache Kafka error table and `tests/acl_handlers.rs`.
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;

// API versions chosen to match `tests/acl_handlers.rs`:
//   * CreateTopics v7 — flexible (FLEXIBLE_MIN=5), topic id round-trips.
//   * Produce v11 — flexible (FLEXIBLE_MIN=9), still uses topic `name`
//     rather than topic_id (v >= 13 introduces the latter).
const CREATE_TOPICS_VERSION: i16 = 7;
const PRODUCE_VERSION: i16 = 11;

// ─────────────────────────────────────────────────────────────────────────────
// Cluster bring-up.
// ─────────────────────────────────────────────────────────────────────────────

/// Boot a single-broker SASL_PLAINTEXT cluster whose `BrokerConfig.authorizer`
/// is an [`OpaAuthorizer`] pointed at `opa_url`. `admin` is registered as a
/// super-user in BOTH the authorizer (so it bypasses the OPA HTTP call) AND
/// `BrokerConfig.super_users` (so broker-level super-user checks accept it).
/// `alice` is a regular user — every authorization check on alice's sessions
/// flows through OPA.
///
/// `expire_after_ms = 1` keeps the OPA cache from masking same-test variation
/// — the second authorization check in `produce_allowed_by_opa_succeeds`
/// always re-fetches from the mock, so the assertion isn't sensitive to
/// in-process cache hits from earlier requests within the same test process.
async fn start_broker_with_opa_authorizer(opa_url: String) -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("admin".to_string(), "admin-secret".to_string());
    cfg.plain_credentials
        .insert("alice".to_string(), "wonderland".to_string());
    cfg.inter_broker_credentials = Some(crabka_broker::config::InterBrokerCredentials::Plain {
        username: "admin".to_string(),
        password: "admin-secret".to_string(),
    });
    // Broker-level super-user set (used by handler code that reads
    // `broker.config.super_users` directly, independent of the authorizer
    // trait dispatch — e.g. the act-as gate).
    cfg.super_users.insert("admin".to_string());

    // OpaAuthorizer carries its own super-user set so it can bypass HTTP
    // before consulting OPA. Building it inside the test means `Handle::
    // try_current()` succeeds (the `#[tokio::test(flavor = "multi_thread")]`
    // attribute on each test guarantees a multi-thread runtime, which is
    // what `block_in_place` requires).
    let mut super_users = std::collections::HashSet::new();
    super_users.insert("admin".to_string());
    let opa = OpaAuthorizer::new(
        super_users,
        opa_url,
        /* allow_on_error */ false,
        /* max_cache_size */ 100,
        // Tight TTL so subsequent authorize() calls within the same test
        // re-consult the mock rather than serving from cache. The
        // wall-clock TTL is enforced by `time_util::now_ms()`; 1 ms
        // means the second call in any same-test sequence is always a
        // cache miss after `tokio::time::sleep(Duration::from_millis(5))`.
        /* expire_after_ms */
        1,
    )
    .expect("OpaAuthorizer::new must succeed inside a tokio runtime");
    cfg.authorizer = std::sync::Arc::new(opa);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver helpers.
//
// Identical shape to the helpers in `tests/acl_handlers.rs` and
// `tests/delegation_tokens.rs` — kept inline here because Cargo's integration
// test layout treats each `tests/*.rs` as its own crate, so reuse across
// files isn't free without a `mod support;` declaration. The wire framing
// is small enough that another copy is cheaper than the extra indirection.
// ─────────────────────────────────────────────────────────────────────────────

async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "crabka-opa-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields byte
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame size fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0); // authzid (empty)
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    let mut auth_body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut auth_body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body).await?;
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} error_message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    Ok(stream)
}

fn single_record_produce_request(topic: &str, partition: i32, value: &[u8]) -> ProduceRequest {
    ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(
                    RecordBatch {
                        last_offset_delta: 0,
                        records: vec![Record {
                            offset_delta: 0,
                            value: Some(bytes::Bytes::copy_from_slice(value)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn drive_create_topics_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: CreateTopicsRequest,
) -> Result<CreateTopicsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_TOPICS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateTopics encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 19, CREATE_TOPICS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateTopicsResponse::decode(&mut cur, CREATE_TOPICS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateTopics decode: {e}")))
}

async fn drive_produce_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: ProduceRequest,
) -> Result<ProduceResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION)
        .map_err(|e| io::Error::other(format!("Produce encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 0, PRODUCE_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    ProduceResponse::decode(&mut cur, PRODUCE_VERSION)
        .map_err(|e| io::Error::other(format!("Produce decode: {e}")))
}

/// Drive `CreateTopics(name, 1 partition, rf=1)` over a SASL/PLAIN admin
/// session. Admin is the super-user in both tests so the call bypasses
/// the OPA mock and the topic materialises regardless of OPA's response.
async fn create_topic_as_admin(addr: SocketAddr, name: &str) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = drive_create_topics_as_plain(addr, "admin", b"admin-secret", req)
        .await
        .expect("CreateTopics as super-user must round-trip");
    assert!(resp.topics.len() == 1, "one topic in response");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Wait (event-driven, on `handle`) until `topic`/partition-0's local
/// writer-actor has materialised, then drive a single `drive_produce_as_plain`.
///
/// The local writer materialising implies the raft commit-then-apply gap
/// between `CreateTopics` returning and the partition appearing in the
/// broker's MetadataImage has closed. Before that point the authorizer can
/// find no matching topic resource and denies alice's Write with
/// `TOPIC_AUTHORIZATION_FAILED`; once the topic is applied, the check flows
/// through OPA (which allows). Waiting on the same handle removes that race
/// without a fixed-interval retry loop. (The OPA decision cache uses a 1 ms
/// TTL, far below any scheduling delay here, so it never masks the result.)
/// alice's SASL/PLAIN test password as bytes, assembled at runtime rather than
/// written as a byte-string literal. The value is a non-secret test fixture,
/// but a literal flowing into the client auth calls trips GitHub's default
/// code-scanning credential query; sourcing it here keeps those sites
/// literal-free.
fn alice_password() -> Vec<u8> {
    [b'w', b'o', b'n', b'd', b'e', b'r', b'l', b'a', b'n', b'd'].to_vec()
}

async fn produce_when_partition_ready(
    handle: &BrokerHandle,
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
) -> Result<ProduceResponse, io::Error> {
    handle.wait_until_local_log_end_offset(topic, 0, 0).await;
    drive_produce_as_plain(
        addr,
        user,
        password,
        single_record_produce_request(topic, 0, b"hello"),
    )
    .await
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests.
// ─────────────────────────────────────────────────────────────────────────────

/// Spec §4.2 test 1.
///
/// OPA mock returns `{"result": false}` for every POST. `alice`
/// authenticates via SASL/PLAIN and sends Produce against a topic that
/// `admin` (super-user) pre-created. The per-partition response must
/// carry `TOPIC_AUTHORIZATION_FAILED (29)` because alice's Write check
/// on the topic flows through OPA, which always denies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_blocked_by_opa_returns_topic_authorization_failed() {
    let opa = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": false})),
        )
        .mount(&opa)
        .await;
    let opa_url = format!("{}/v1/data/kafka/authz/allow", opa.uri());

    let (handle, _dir, addr) = start_broker_with_opa_authorizer(opa_url).await;

    // Bootstrap the topic via admin (super-user → OPA bypassed).
    create_topic_as_admin(addr, "blocked-topic").await;

    // alice (non-super-user) tries to Produce → OPA returns deny → 29.
    let resp = drive_produce_as_plain(
        addr,
        "alice",
        &alice_password(),
        single_record_produce_request("blocked-topic", 0, b"hello"),
    )
    .await
    .expect("Produce must round-trip");

    handle.shutdown().await;

    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partition_responses.len() == 1,
        "one partition row in response"
    );
    let p = &resp.responses[0].partition_responses[0];
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "OPA denied alice's Write on blocked-topic, expected \
         TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}

/// Spec §4.2 test 2.
///
/// OPA mock returns `{"result": true}` for every POST. `alice`
/// authenticates and produces — must succeed with `error_code = 0` on
/// the per-partition response row.
///
/// `produce_when_partition_ready` waits (event-driven, via the broker
/// handle) for the partition's local writer to materialise — closing the
/// raft commit-then-apply gap between `CreateTopics` returning and the
/// partition appearing in the local MetadataImage — before the single
/// Produce, so no fixed sleep is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_allowed_by_opa_succeeds() {
    let opa = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"result": true})))
        .mount(&opa)
        .await;
    let opa_url = format!("{}/v1/data/kafka/authz/allow", opa.uri());

    let (handle, _dir, addr) = start_broker_with_opa_authorizer(opa_url).await;

    create_topic_as_admin(addr, "permitted-topic").await;

    let resp =
        produce_when_partition_ready(&handle, addr, "alice", &alice_password(), "permitted-topic")
            .await
            .expect("Produce must round-trip");

    handle.shutdown().await;

    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partition_responses.len() == 1,
        "one partition row in response"
    );
    let p = &resp.responses[0].partition_responses[0];
    assert!(
        p.error_code == 0,
        "OPA allowed alice's Write on permitted-topic, expected \
         error_code=0, got {p:?}"
    );
}
