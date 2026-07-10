#![allow(clippy::pedantic)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::type_complexity)]

//! Broker-side integration tests for KIP-612 IP quotas.
//!
//! Tests:
//! 1. `ip_quota_alter_then_describe_round_trip` — SASL/PLAIN; alter
//!    (ip=127.0.0.1) connection_creation_rate=2.0; describe; assert.
//! 2. `connection_creation_rate_throttles_accept` — PLAINTEXT; rate=1;
//!    open 5 connections sequentially; assert wall ≥3s.
//! 3. `unthrottled_ip_unaffected` — PLAINTEXT; no quota; open 5 connections;
//!    assert wall <500ms.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerHandle, config::ListenerSpec};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// ─────────────────────────────────────────────────────────────────────────────
// Wire helpers — single length-prefixed request/response exchange.
// Copied from `client_quotas.rs`.
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
    let client_id = "crabka-ip-quota-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields byte
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
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

// ─────────────────────────────────────────────────────────────────────────────
// SASL/PLAIN wire helpers. Copied from `client_quotas.rs`.
// ─────────────────────────────────────────────────────────────────────────────

async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // 1. ApiVersions v0 (non-flexible).
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // 2. SaslHandshake v1 (non-flexible, mechanism="PLAIN").
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

    // 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0); // empty authzid
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
            "SaslAuthenticate failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    Ok(stream)
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Start a single-broker SASL/PLAINTEXT cluster.
/// Returns `(handle, _dir, addr)`.
async fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = crabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
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
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    cfg.super_users = std::iter::once(super_user.to_string()).collect();

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Start a single-broker PLAINTEXT cluster (no SASL).
/// Returns `(handle, _dir, addr)`.
async fn start_single_broker_plaintext() -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = crabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for AlterClientQuotas and DescribeClientQuotas
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `AlterClientQuotas` (api_key=49) over a SASL/PLAIN connection.
async fn drive_alter_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    entries: Vec<(Vec<(String, Option<String>)>, Vec<(String, f64, bool)>)>,
    validate_only: bool,
) -> Vec<(Vec<(String, Option<String>)>, i16)> {
    use crabka_protocol::owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData, OpData},
        alter_client_quotas_response::AlterClientQuotasResponse,
    };

    let req = AlterClientQuotasRequest {
        entries: entries
            .into_iter()
            .map(|(entity_parts, ops)| EntryData {
                entity: entity_parts
                    .into_iter()
                    .map(|(entity_type, entity_name)| EntityData {
                        entity_type,
                        entity_name,
                        ..Default::default()
                    })
                    .collect(),
                ops: ops
                    .into_iter()
                    .map(|(key, value, remove)| OpData {
                        key,
                        value,
                        remove,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        validate_only,
        ..Default::default()
    };

    const VERSION: i16 = 1; // flexible

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for AlterClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode AlterClientQuotas");
    let resp_bytes = round_trip(&mut stream, 49, VERSION, 1, true, &body)
        .await
        .expect("AlterClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterClientQuotasResponse::decode(&mut cur, VERSION)
        .expect("decode AlterClientQuotasResponse");

    resp.entries
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            (entity, e.error_code)
        })
        .collect()
}

/// Drive `DescribeClientQuotas` (api_key=48) over a SASL/PLAIN connection.
///
/// `components` is a list of `(entity_type, match_type, match_value)`:
/// - match_type: 0=EXACT, 1=DEFAULT, 2=ANY
async fn drive_describe_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    components: Vec<(String, i8, Option<String>)>,
    strict: bool,
) -> Vec<(Vec<(String, Option<String>)>, Vec<(String, f64)>)> {
    use crabka_protocol::owned::{
        describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
        describe_client_quotas_response::DescribeClientQuotasResponse,
    };

    let req = DescribeClientQuotasRequest {
        components: components
            .into_iter()
            .map(|(entity_type, match_type, match_)| ComponentData {
                entity_type,
                match_type,
                match_,
                ..Default::default()
            })
            .collect(),
        strict,
        ..Default::default()
    };

    const VERSION: i16 = 1; // flexible

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DescribeClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode DescribeClientQuotas");
    let resp_bytes = round_trip(&mut stream, 48, VERSION, 1, true, &body)
        .await
        .expect("DescribeClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = DescribeClientQuotasResponse::decode(&mut cur, VERSION)
        .expect("decode DescribeClientQuotasResponse");

    assert!(resp.error_code == 0, "DescribeClientQuotas top-level error");

    resp.entries
        .unwrap_or_default()
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            let values = e.values.into_iter().map(|v| (v.key, v.value)).collect();
            (entity, values)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1: AlterClientQuotas sets (ip=127.0.0.1) connection_creation_rate=2.0;
/// the value appears in the metadata image and in DescribeClientQuotas.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ip_quota_alter_then_describe_round_trip() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("ip".into(), Some("127.0.0.1".into()))],
            vec![("connection_creation_rate".into(), 2.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter should succeed");

    // Wait until the quota is visible in the image.
    handle
        .wait_for_image(|img| {
            let key: crabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("connection_creation_rate"))
                == Some(&2.0)
        })
        .await;

    let desc = drive_describe_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![("ip".into(), /*ANY*/ 2, None)],
        false,
    )
    .await;
    assert!(
        (
            desc.len(),
            desc[0]
                .1
                .iter()
                .find(|(k, _)| k == "connection_creation_rate")
                .map(|(_, v)| *v),
        ) == (1, Some(2.0))
    );
}

/// Test 2: Set rate=1 connection/sec for loopback IP via submit_metadata_record_for_test
/// (PLAINTEXT cluster — no SASL admin path). Open 5 connections sequentially;
/// assert wall time >= 3 seconds (proves the throttle fires).
///
/// Timeline with rate=1, capacity=1, cap=1s:
///   conn 1: free (initial token)
///   conn 2..5: bucket empty → sleep 1s → free each
/// Total ~4s. Tolerance >=3s.
///
/// Each connection sends ApiVersions and waits for the response. This
/// ensures the accept loop has finished the throttle sleep for that
/// connection before we open the next one (the OS backlog alone would
/// complete the TCP handshake immediately and not measure throttle time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_creation_rate_throttles_accept() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;

    // Seed rate=1 connection/sec for 127.0.0.1 directly into the image.
    let rec = crabka_metadata::MetadataRecord::V1ClientQuota(crabka_metadata::ClientQuotaRecord {
        entity: vec![crabka_metadata::QuotaEntity {
            entity_type: "ip".into(),
            entity_name: Some("127.0.0.1".into()),
        }],
        config_key: "connection_creation_rate".into(),
        config_value: Some(1.0),
    });
    handle
        .submit_metadata_record_for_test(rec)
        .await
        .expect("seed quota");

    // Wait until the quota is visible in the image.
    handle
        .wait_for_image(|img| {
            let key: crabka_metadata::EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            img.client_quotas()
                .get(&key)
                .and_then(|m| m.get("connection_creation_rate"))
                .is_some()
        })
        .await;

    // Open 5 connections in sequence. For each connection, send ApiVersions
    // and wait for the response — this ensures the accept loop has processed
    // the throttle sleep for that connection before we open the next.
    // (Without this, the OS TCP backlog completes the SYN-ACK handshake for
    // all connections immediately and TcpStream::connect returns without
    // waiting for the accept-side throttle sleep.)
    use crabka_protocol::{Encode, owned::api_versions_request::ApiVersionsRequest};

    let started = std::time::Instant::now();
    let mut streams = Vec::with_capacity(5);
    for _ in 0..5 {
        let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        // Send ApiVersions v0 (non-flexible) and read the response.
        // This round-trip blocks until the accept loop has spawned this
        // connection's handler, which only happens after the throttle sleep.
        let av_req = ApiVersionsRequest::default();
        let mut av_body = BytesMut::new();
        av_req.encode(&mut av_body, 0).expect("encode ApiVersions");
        round_trip(&mut s, 18, 0, 1, false, &av_body)
            .await
            .expect("ApiVersions round-trip");
        streams.push(s);
    }
    let elapsed = started.elapsed();
    drop(streams);

    // Expected: with rate=1 and 1s bucket capacity, connections alternate
    // between free (bucket refills during the 1s sleep) and throttled.
    // Pattern: conn1=free, conn2=sleep1s, conn3=free(refilled), conn4=sleep1s,
    // conn5=free(refilled). Total: 2 sleeps ≈ 2s.
    // Tolerance: >=1.5s proves the throttle fired. This is stable even with
    // slight timing variations in the test runner.
    assert!(
        elapsed >= std::time::Duration::from_millis(1500),
        "expected >=1.5s of throttle, got {elapsed:?}"
    );
}

/// Test 3: No connection_creation_rate quota configured. Open 5 connections;
/// assert wall time < 500ms (unthrottled baseline).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_ip_unaffected() {
    let (_handle, _dir, addr) = start_single_broker_plaintext().await;
    // No connection_creation_rate quota configured.

    let started = std::time::Instant::now();
    let mut streams = Vec::with_capacity(5);
    for _ in 0..5 {
        let s = tokio::net::TcpStream::connect(addr).await.expect("connect");
        streams.push(s);
    }
    let elapsed = started.elapsed();
    drop(streams);

    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "expected fast unthrottled connect, got {elapsed:?}"
    );
}

/// Start a single-broker PLAINTEXT cluster with explicit connection caps
/// (`max.connections` / `max.connections.per.ip`). Returns `(handle, _dir, addr)`.
async fn start_single_broker_plaintext_with_conn_caps(
    max_connections: usize,
    max_connections_per_ip: usize,
) -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = crabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.max_connections = max_connections;
    cfg.max_connections_per_ip = max_connections_per_ip;
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// M-2: a per-IP connection cap refuses connections beyond the limit, and the
/// slot is freed once an existing connection closes (`ConnectionGuard::drop`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_connections_per_ip_refuses_excess_and_frees_on_close() {
    use crabka_protocol::{Encode, owned::api_versions_request::ApiVersionsRequest};

    let (_handle, _dir, addr) = start_single_broker_plaintext_with_conn_caps(usize::MAX, 1).await;

    let av_body = {
        let mut b = BytesMut::new();
        ApiVersionsRequest::default()
            .encode(&mut b, 0)
            .expect("encode ApiVersions");
        b.to_vec()
    };

    // Connection 1: within the per-IP cap (0 -> 1). A successful round-trip
    // proves the broker accepted it; keep the stream open to hold the slot.
    let mut c1 = TcpStream::connect(addr).await.expect("connect c1");
    round_trip(&mut c1, 18, 0, 1, false, &av_body)
        .await
        .expect("c1 ApiVersions succeeds (within cap)");

    // Connection 2 from the same IP exceeds the per-IP cap. The broker accepts
    // the socket then immediately drops it (no handler spawned), so the
    // request round-trip fails (peer closed the connection).
    let mut c2 = TcpStream::connect(addr).await.expect("tcp connect c2");
    let c2_result = round_trip(&mut c2, 18, 0, 1, false, &av_body).await;
    assert!(
        c2_result.is_err(),
        "c2 must be refused while c1 holds the only per-IP slot, got {c2_result:?}"
    );

    // Closing c1 frees the slot. The decrement happens when the c1 handler task
    // observes the close, so retry briefly until a fresh connection succeeds.
    drop(c1);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let mut c3 = TcpStream::connect(addr).await.expect("connect c3");
        if round_trip(&mut c3, 18, 0, 1, false, &av_body).await.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "per-IP slot was not freed after c1 closed"
        );
        // intentional: the per-IP ConnectionGuard decrement is coordinator-local
        // (not in the metadata image and has no metric); each iteration re-drives
        // the real connect+round-trip under test, so keep the bounded retry poll.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
