// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` in test files
// (same upstream bug as `tests/mtls.rs` etc).

//! Prometheus `/metrics` HTTP endpoint.
//!
//! Boots a broker with a metrics listener on `127.0.0.1:0`, scrapes
//! `/metrics`, exercises the Produce wire path, and verifies that the
//! topic-labelled counters tick up. Proves the registry → handler →
//! HTTP endpoint chain works end-to-end.
//!
//! Gated to non-Windows: the broker handle's `metrics_addr()` is
//! Linux/macOS-only by convention (matches the other integration
//! test conventions).

use std::{io, time::Duration};

use assert2::{assert, check};
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, config::ListenerSpec, metrics::PartitionLabel};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
};
use crabka_security::ListenerProtocol;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const TOPIC: &str = "metrics-it";
const FETCH_VERSION: i16 = 12;
const PRODUCE_VERSION: i16 = 9;
const CREATE_TOPICS_VERSION: i16 = 7;

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
    let client_id = "crabka-metrics-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;
    let mut cur = &resp[..];
    let _corr = cur.get_i32();
    if flexible {
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

async fn create_topic(addr: std::net::SocketAddr) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: TOPIC.into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_TOPICS_VERSION).unwrap();
    let resp = round_trip(&mut stream, 19, CREATE_TOPICS_VERSION, 1, true, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = CreateTopicsResponse::decode(&mut cur, CREATE_TOPICS_VERSION).unwrap();
    assert!(r.topics[0].error_code == 0, "create: {:?}", r.topics[0]);
}

async fn produce_one(addr: std::net::SocketAddr) -> u64 {
    use crabka_protocol::records::{Record, RecordBatch};
    let batch = RecordBatch {
        records: vec![Record {
            offset_delta: 0,
            value: Some(bytes::Bytes::from_static(b"hello")),
            ..Default::default()
        }],
        ..Default::default()
    };
    let part = PartitionProduceData {
        index: 0,
        records: Some(batch.into()),
        ..Default::default()
    };
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: TOPIC.into(),
            partition_data: vec![part],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = round_trip(&mut stream, 0, PRODUCE_VERSION, 1, true, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = ProduceResponse::decode(&mut cur, PRODUCE_VERSION).unwrap();
    let topic = r.responses.into_iter().next().expect("one topic in resp");
    let part = topic
        .partition_responses
        .into_iter()
        .next()
        .expect("one partition in resp");
    assert!(part.error_code == 0, "produce: {part:?}");
    body.len() as u64
}

async fn fetch_one(addr: std::net::SocketAddr) {
    use crabka_protocol::owned::fetch_response::FetchResponse;
    let req = FetchRequest {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        topics: vec![FetchTopic {
            topic: TOPIC.into(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024 * 1024,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, FETCH_VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = round_trip(&mut stream, 1, FETCH_VERSION, 1, true, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = FetchResponse::decode(&mut cur, FETCH_VERSION).unwrap();
    assert!(r.error_code == 0, "fetch top-level: {r:?}");
}

async fn scrape(addr: std::net::SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let s = String::from_utf8(buf).unwrap();
    // Strip the HTTP head, keep the body so we can grep metric names.
    let body_start = s.find("\r\n\r\n").map_or(0, |i| i + 4);
    s[body_start..].to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_serves_openmetrics_and_counters_tick() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
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

    let handle = Broker::start(cfg).await.unwrap();
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");

    // Initial scrape: gauges and scalar counters are present
    // immediately, even pre-traffic. (Topic-labelled `Family` metrics
    // need at least one label entry to emit a series — those are
    // verified after we drive traffic below.)
    let body = scrape(metrics_addr).await;
    for needle in [
        "crabka_broker_partitions_led",
        "crabka_broker_active_controller",
        "crabka_broker_isr_shrinks_total",
        "crabka_broker_isr_expands_total",
    ] {
        assert!(body.contains(needle), "missing {needle} in:\n{body}");
    }
    assert!(body.contains("# EOF"), "no EOF marker in:\n{body}");

    // Drive a CreateTopics + Produce + Fetch so the topic-labelled
    // counters get at least one entry and start emitting series.
    create_topic(kafka_addr).await;
    // Wait for the partition to materialize in the metadata image (the
    // partition writer is ready on commit) instead of guessing a duration.
    handle.wait_until_partition_present(TOPIC, 0).await;
    produce_one(kafka_addr).await;
    fetch_one(kafka_addr).await;

    // Wait for the background gauge sampler (1s tick) to publish
    // partitions_led rather than sleeping past one tick. active_controller
    // is set in the same sampler iteration, so it is live once this holds.
    handle
        .wait_for_metrics("partitions_led >= 1", |m| m.partitions_led.get() >= 1)
        .await;

    let body = scrape(metrics_addr).await;
    for needle in [
        "crabka_broker_topic_bytes_in_total",
        "crabka_broker_topic_bytes_out_total",
        "crabka_broker_topic_produce_requests_total",
        "crabka_broker_topic_fetch_requests_total",
        "crabka_broker_messages_in_total",
    ] {
        assert!(
            body.contains(needle),
            "post-traffic scrape missing {needle} in:\n{body}"
        );
    }
    // `produce_one` writes a single v2 record into the
    // topic, so `messages_in_total{topic=TOPIC}` must read exactly 1.
    let messages_needle = format!("crabka_broker_messages_in_total{{topic=\"{TOPIC}\"}} 1");
    check!(
        body.contains(&messages_needle),
        "messages_in_total should be 1 for the lone produced record, body:\n{body}"
    );
    check!(
        body.contains(&format!(
            "crabka_broker_topic_produce_requests_total{{topic=\"{TOPIC}\"}}"
        )),
        "produce-requests counter missing topic label:\n{body}"
    );
    check!(
        body.contains(&format!(
            "crabka_broker_topic_fetch_requests_total{{topic=\"{TOPIC}\"}}"
        )),
        "fetch-requests counter missing topic label:\n{body}"
    );
    // partitions_led >= 1 once the partition writer is up.
    let led_line = body
        .lines()
        .find(|l| l.starts_with("crabka_broker_partitions_led "))
        .expect("partitions_led series present");
    let value: i64 = led_line
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(
        value >= 1,
        "partitions_led should be >=1, got line: {led_line}"
    );
    // Single-node broker: it's its own controller leader.
    assert!(
        body.contains("crabka_broker_active_controller 1"),
        "active_controller should be 1 on single-node, body:\n{body}"
    );

    handle.shutdown().await;
}

/// Confirm that per-partition counters land alongside
/// topic-level ones, and that the disk-scanner gauge picks up
/// non-zero values for materialized partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_level_metrics_and_disk_gauge_render() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
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
    // Enable the disk scanner with a 1s tick so the gauge gets a
    // chance to populate within the test's wait window.
    cfg.partition_disk_scan_interval = crabka_units::secs(1);

    let handle = Broker::start(cfg).await.unwrap();
    let kafka_addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");

    // Create the topic + produce a record so the per-partition
    // counters fire and the on-disk segment exists for the scanner.
    create_topic(kafka_addr).await;
    // Wait for the partition to materialize (writer ready on commit)
    // instead of guessing a duration.
    handle.wait_until_partition_present(TOPIC, 0).await;
    produce_one(kafka_addr).await;

    // Wait for the disk scanner to publish a non-zero on-disk size for the
    // materialized partition rather than sleeping past its 1s tick.
    handle
        .wait_for_metrics("partition_disk_bytes > 0", |m| {
            m.partition_disk_bytes
                .get_or_create(&PartitionLabel {
                    topic: TOPIC.to_string(),
                    partition: 0,
                })
                .get()
                > 0
        })
        .await;

    let body = scrape(metrics_addr).await;

    // Topic-level still present.
    let topic_needle = format!("crabka_broker_topic_bytes_in_total{{topic=\"{TOPIC}\"}}");
    assert!(
        body.contains(&topic_needle),
        "missing topic-level bytes_in in:\n{body}"
    );

    // Partition-level present with non-zero value.
    let partition_needle =
        format!("crabka_broker_partition_bytes_in_total{{topic=\"{TOPIC}\",partition=\"0\"}}");
    assert!(
        body.contains(&partition_needle),
        "missing partition-level bytes_in in:\n{body}"
    );
    // Per-partition CPU micros counter must be emitted with
    // the topic/partition label set after the produce + fetch path
    // runs. Value is timing-dependent (could be 0 if the handler was
    // sub-microsecond), so we only assert presence of the series name,
    // not a specific value.
    let cpu_needle = format!("crabka_broker_partition_cpu_micros_total{{topic=\"{TOPIC}\"");
    assert!(
        body.contains(&cpu_needle),
        "missing partition_cpu_micros_total in:\n{body}"
    );
    // Confirm the value is non-zero.
    let part_line = body
        .lines()
        .find(|l| l.starts_with(&partition_needle))
        .expect("partition_bytes_in line present");
    let value: u64 = part_line
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(
        value > 0,
        "partition_bytes_in should be >0 after produce, got line: {part_line}"
    );

    // Disk gauge: emitted for at least one (topic, partition) of TOPIC.
    let disk_prefix = format!("crabka_broker_partition_disk_bytes{{topic=\"{TOPIC}\"");
    assert!(
        body.contains(&disk_prefix),
        "missing partition_disk_bytes for materialized partition in:\n{body}"
    );

    let _ = tokio::time::timeout(Duration::from_secs(10), handle.shutdown()).await;
}
