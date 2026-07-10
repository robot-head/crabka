// rustc 1.95 clippy ICEs on this file in the same places as elect_leaders.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
#![allow(clippy::pedantic)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::type_complexity)]

//! Broker-side integration tests for KIP-73 replication throttle.
//!
//! Tests:
//! 1. `broker_scoped_alter_persists_in_image` — `IncrementalAlterConfigs`
//!    (resource_type=Broker) sets `leader.replication.throttled.rate`; visible
//!    in `MetadataImage` via `controller_image_for_test`.
//! 2. `topic_throttle_config_propagates` — `IncrementalAlterConfigs`
//!    (resource_type=Topic) sets `leader.replication.throttled.replicas`; the
//!    `TopicThrottle` helper reports it correctly.
//! 3. `throttle_rate_caps_fetch_response_size` — produce 8 KB, set
//!    leader-rate=512, then Fetch with `replica_id >= 0`; assert response is
//!    well under 8 KB.
//! 4. `unthrottled_partition_unaffected` — same setup without throttle
//!    config; Fetch delivers the full 8 KB.
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b/14/15.

use std::{io, net::SocketAddr};

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
// Copied verbatim from `partition_reassignment.rs`.
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
    let client_id = "crabka-throttle-test";
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
// SASL/PLAIN wire helpers. Copied from `partition_reassignment.rs`.
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

/// Create a topic via SASL/PLAIN as the given admin user.
/// Copied from `partition_reassignment.rs`.
async fn create_topic_as_admin(
    addr: SocketAddr,
    topic: &str,
    partitions: i32,
    replication_factor: i16,
) {
    use crabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = sasl_plain_authenticate(addr, "admin", b"admin-secret")
        .await
        .expect("SASL authenticate for CreateTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, 7, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");
    assert2::assert!((resp.topics.len(), resp.topics[0].error_code) == (1, 0));
}

/// Create a topic via PLAINTEXT (no SASL, compat shim = allow-all).
async fn create_topic_plaintext(addr: SocketAddr, topic: &str, partitions: i32, rf: i16) {
    use crabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: rf,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, 7, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");
    assert2::assert!((resp.topics.len(), resp.topics[0].error_code) == (1, 0));
}

/// Await until `handle` sees `(topic, partition)` present in its image.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for IncrementalAlterConfigs and DescribeConfigs
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `IncrementalAlterConfigs` (api_key=44) over a SASL/PLAIN connection.
/// `resources` is a list of `(resource_type, name, [(config_name, value, op)])`.
/// Returns the top-level error code from the first resource response (0 = success).
async fn drive_incremental_alter_configs(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    resources: Vec<(i8, String, Vec<(String, Option<String>, i8)>)>,
) -> i16 {
    use crabka_protocol::owned::{
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    };

    let req = IncrementalAlterConfigsRequest {
        resources: resources
            .into_iter()
            .map(
                |(resource_type, resource_name, configs)| AlterConfigsResource {
                    resource_type,
                    resource_name,
                    configs: configs
                        .into_iter()
                        .map(|(name, value, config_operation)| AlterableConfig {
                            name,
                            config_operation,
                            value,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
            .collect(),
        validate_only: false,
        ..Default::default()
    };

    // Use version 1 (flexible).
    const VERSION: i16 = 1;

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for IncrementalAlterConfigs");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode IncrementalAlterConfigs");
    let resp_bytes = round_trip(&mut stream, 44, VERSION, 1, true, &body)
        .await
        .expect("IncrementalAlterConfigs round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = IncrementalAlterConfigsResponse::decode(&mut cur, VERSION)
        .expect("decode IncrementalAlterConfigsResponse");

    resp.responses.first().map(|r| r.error_code).unwrap_or(0)
}

/// Drive `IncrementalAlterConfigs` (api_key=44) over a PLAINTEXT connection
/// (no SASL — compat shim allows everything).
async fn drive_incremental_alter_configs_plaintext(
    addr: SocketAddr,
    resources: Vec<(i8, String, Vec<(String, Option<String>, i8)>)>,
) -> i16 {
    use crabka_protocol::owned::{
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    };

    let req = IncrementalAlterConfigsRequest {
        resources: resources
            .into_iter()
            .map(
                |(resource_type, resource_name, configs)| AlterConfigsResource {
                    resource_type,
                    resource_name,
                    configs: configs
                        .into_iter()
                        .map(|(name, value, config_operation)| AlterableConfig {
                            name,
                            config_operation,
                            value,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
            .collect(),
        validate_only: false,
        ..Default::default()
    };

    const VERSION: i16 = 1;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode IncrementalAlterConfigs");
    let resp_bytes = round_trip(&mut stream, 44, VERSION, 1, true, &body)
        .await
        .expect("IncrementalAlterConfigs round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = IncrementalAlterConfigsResponse::decode(&mut cur, VERSION)
        .expect("decode IncrementalAlterConfigsResponse");

    resp.responses.first().map(|r| r.error_code).unwrap_or(0)
}

/// Drive `DescribeConfigs` (api_key=32, version=1) over a SASL/PLAIN connection.
/// Returns `Vec<(per-resource error_code, Vec<(name, value)>)>`.
#[allow(dead_code)]
async fn drive_describe_configs(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    resources: Vec<(i8, String)>,
) -> Vec<(i16, Vec<(String, String)>)> {
    use crabka_protocol::owned::{
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResponse,
    };

    let req = DescribeConfigsRequest {
        resources: resources
            .into_iter()
            .map(|(resource_type, resource_name)| DescribeConfigsResource {
                resource_type,
                resource_name,
                configuration_keys: None,
                ..Default::default()
            })
            .collect(),
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    };

    // Use version 1 (non-flexible, supports include_synonyms).
    const VERSION: i16 = 1;

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DescribeConfigs");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode DescribeConfigs");
    let resp_bytes = round_trip(&mut stream, 32, VERSION, 1, false, &body)
        .await
        .expect("DescribeConfigs round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        DescribeConfigsResponse::decode(&mut cur, VERSION).expect("decode DescribeConfigsResponse");

    resp.results
        .into_iter()
        .map(|r| {
            let configs = r
                .configs
                .into_iter()
                .map(|c| (c.name, c.value.unwrap_or_default()))
                .collect();
            (r.error_code, configs)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for Produce and Fetch (PLAINTEXT path)
// ─────────────────────────────────────────────────────────────────────────────

/// Produce `count` records of `record_bytes` bytes each to `(topic, 0)` over
/// a PLAINTEXT connection. Asserts `error_code=0` on the partition row.
async fn produce_plaintext(addr: SocketAddr, topic: &str, record_bytes: usize, count: usize) {
    use crabka_protocol::{
        owned::{
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::ProduceResponse,
        },
        records::{Record, RecordBatch},
    };

    let value = vec![0u8; record_bytes];
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(&value)),
            ..Default::default()
        })
        .collect();

    let req = ProduceRequest {
        acks: 1, // leader ack only (rf=1 topic)
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(
                    RecordBatch {
                        last_offset_delta: i32::try_from(count - 1).unwrap(),
                        records,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    const VERSION: i16 = 9; // flexible, pre-KIP-516 (no topic_id needed)
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = round_trip(&mut stream, 0, VERSION, 1, true, &body)
        .await
        .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ProduceResponse::decode(&mut cur, VERSION).expect("decode ProduceResponse");
    let part = &resp.responses[0].partition_responses[0];
    assert2::assert!(part.error_code == 0);
}

/// Issue a single Fetch request with `replica_id` (>= 0 = inter-broker
/// replica fetch, subject to leader-side throttle) over a PLAINTEXT
/// connection. Returns the raw response payload byte length.
async fn fetch_plaintext_replica(addr: SocketAddr, topic: &str, replica_id: i32) -> usize {
    use crabka_protocol::owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    };

    let req = FetchRequest {
        replica_id,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    const VERSION: i16 = 12; // flexible
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Fetch");

    // Send raw frame and capture the full raw response (before decode) so we
    // can measure response bytes.
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(1i16); // api_key
    frame.put_i16(VERSION);
    frame.put_i32(1i32); // corr_id
    let client_id = "crabka-throttle-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    frame.put_u8(0); // flexible header tagged-fields
    frame.put_slice(&body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    let resp_len = stream.read_u32().await.unwrap();
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await.unwrap();

    // Decode to assert no transport error.
    let mut cur: &[u8] = &resp[4..]; // skip corr_id
    let _tagged = cur.get_u8(); // v1 header tagged-fields
    let _decoded = FetchResponse::decode(&mut cur, VERSION).expect("decode FetchResponse");

    resp.len()
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1: `IncrementalAlterConfigs` with resource_type=Broker sets the
/// `leader.replication.throttled.rate` key. The value must be visible in
/// the metadata image via `controller_image_for_test`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_scoped_alter_persists_in_image() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    let node_id = handle.node_id();

    let err = drive_incremental_alter_configs(
        addr,
        "admin",
        "admin-secret",
        vec![(
            4, // resource_type = Broker
            node_id.to_string(),
            vec![(
                "leader.replication.throttled.rate".into(),
                Some("2048".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert2::assert!(err == 0);

    // Await until the config is visible (absorb raft commit latency).
    handle
        .wait_for_image(|img| {
            img.broker_throttle_rate(
                crabka_metadata::NodeId(node_id),
                crabka_metadata::ThrottleKind::Leader,
            ) == Some(2048)
        })
        .await;
    handle.shutdown().await;
}

/// Test 2: `IncrementalAlterConfigs` with resource_type=Topic sets
/// `leader.replication.throttled.replicas`; `TopicThrottle::for_topic`
/// returns the correct throttled-replica entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_throttle_config_propagates() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    create_topic_as_admin(addr, "foo", 1, 1).await;
    wait_partition_exists(&handle, "foo", 0).await;

    let err = drive_incremental_alter_configs(
        addr,
        "admin",
        "admin-secret",
        vec![(
            2, // resource_type = Topic
            "foo".into(),
            vec![(
                "leader.replication.throttled.replicas".into(),
                Some("0:1,0:2".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert2::assert!(err == 0);

    // Allow raft commit to propagate.
    handle
        .wait_for_image(|img| {
            let throttle = crabka_broker::throttle::TopicThrottle::for_topic(img, "foo");
            throttle.leader.contains(0, crabka_broker::NodeId(1))
                && throttle.leader.contains(0, crabka_broker::NodeId(2))
        })
        .await;
    handle.shutdown().await;
}

/// Test 3: After setting a very low leader throttle rate (512 bytes/sec) and
/// marking partition 0 as throttled for replica_id=2, a Fetch issued with
/// `replica_id=2` must return a response well under 8 KB.
///
/// The token bucket has a one-second burst capacity at the configured rate, so
/// we set the rate to 512 bytes/sec; a 8 KB response must be capped to at
/// most 512 bytes of record data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_rate_caps_fetch_response_size() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;
    let node_id = handle.node_id();

    // Create topic rf=1 so this broker is always the leader.
    create_topic_plaintext(addr, "bar", 1, 1).await;
    wait_partition_exists(&handle, "bar", 0).await;

    // Set the leader throttle rate to 512 bytes/sec.
    let err = drive_incremental_alter_configs_plaintext(
        addr,
        vec![(
            4, // resource_type = Broker
            node_id.to_string(),
            vec![(
                "leader.replication.throttled.rate".into(),
                Some("512".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert2::assert!(err == 0);

    // Mark partition 0 as throttled for follower replica_id=2.
    let err = drive_incremental_alter_configs_plaintext(
        addr,
        vec![(
            2, // resource_type = Topic
            "bar".into(),
            vec![(
                "leader.replication.throttled.replicas".into(),
                Some("0:2".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert2::assert!(err == 0);

    // Wait for the configs to appear in the image before producing (so the
    // throttle enforcement is armed when the Fetch arrives).
    handle
        .wait_for_image(|img| {
            let rate = img.broker_throttle_rate(
                crabka_metadata::NodeId(node_id),
                crabka_metadata::ThrottleKind::Leader,
            );
            let throttle = crabka_broker::throttle::TopicThrottle::for_topic(img, "bar");
            rate == Some(512) && throttle.leader.contains(0, crabka_broker::NodeId(2))
        })
        .await;

    // Produce 8 KB of data (8 records of 1 KB each).
    produce_plaintext(addr, "bar", 1024, 8).await;

    // Fetch with replica_id=2 (inter-broker follower path → leader throttle applies).
    let resp_bytes = fetch_plaintext_replica(addr, "bar", 2).await;

    // The throttled response must be much smaller than the 8 KB we produced.
    // We allow up to 2 KB as the upper bound to give headroom for framing
    // overhead (batch headers, response wrapper).
    assert2::assert!(resp_bytes <= 2048);

    handle.shutdown().await;
}

/// Test 4: Without any throttle config, a Fetch with `replica_id >= 0` delivers
/// all 8 KB of data unimpeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unthrottled_partition_unaffected() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;

    // Create topic rf=1.
    create_topic_plaintext(addr, "baz", 1, 1).await;
    wait_partition_exists(&handle, "baz", 0).await;

    // Produce 8 KB of data (8 records of 1 KB each). No throttle configured.
    produce_plaintext(addr, "baz", 1024, 8).await;

    // Fetch with replica_id=2 (inter-broker path). No throttle → full data.
    let resp_bytes = fetch_plaintext_replica(addr, "baz", 2).await;

    // Full 8 KB data plus framing. The response should be well over 4 KB.
    assert2::assert!(resp_bytes >= 4096);

    handle.shutdown().await;
}
