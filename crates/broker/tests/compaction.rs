// rustc 1.95 clippy ICEs on this file in the same places as elect_leaders.rs:
// `clippy::pedantic` lints — annotate-snippets upstream bug.

//! Log compaction end-to-end broker integration test.
//!
//! The test produces 30 records across 3 keys, k1, k2, and k3, into a compacted
//! topic. It waits for a compaction pass, force-rolls the active segment, and
//! waits for another pass. It then fetches the records and asserts that exactly
//! 3 distinct keys survive with only their latest values, v10-kN. Old values
//! v0..v9 must be gone from the sealed segments.
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b/14/15.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle, metrics::PartitionLabel};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// ─────────────────────────────────────────────────────────────────────────────
// Wire helpers
// ─────────────────────────────────────────────────────────────────────────────

const CLIENT_ID: &str = "crabka-compaction-test";

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
    frame.put_i16(i16::try_from(CLIENT_ID.len()).expect("client_id fits"));
    frame.put_slice(CLIENT_ID.as_bytes());
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
    // v1 response header: tagged-fields byte (for flexible APIs, except ApiVersions=18)
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
// Cluster setup
// ─────────────────────────────────────────────────────────────────────────────

/// Start a single PLAINTEXT broker with a 1s cleaner interval.
async fn start_broker_with_fast_cleaner() -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.cleaner_interval_override = Some(crabka_units::secs(1));
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

// ─────────────────────────────────────────────────────────────────────────────
// Protocol helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Create a topic with config overrides, on PLAINTEXT and with no SASL.
async fn create_topic_with_configs(
    addr: SocketAddr,
    topic: &str,
    partitions: i32,
    rf: i16,
    configs: Vec<(&str, &str)>,
) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: rf,
            configs: configs
                .into_iter()
                .map(|(name, value)| CreatableTopicConfig {
                    name: name.to_string(),
                    value: Some(value.to_string()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };

    let version: i16 = 7; // flexible
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, version, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        CreateTopicsResponse::decode(&mut cur, version).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({topic}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Get `topic_id` with Metadata. Produce and Fetch v9+ need it.
async fn get_topic_id(addr: SocketAddr, topic: &str) -> Uuid {
    let req = MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            name: Some(topic.to_string()),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let version: i16 = 12; // flexible
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode Metadata");
    let resp_bytes = round_trip(&mut stream, 3, version, 1, true, &body)
        .await
        .expect("Metadata round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = MetadataResponse::decode(&mut cur, version).expect("decode MetadataResponse");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Produce one record with an explicit key and value to (topic, partition 0).
async fn produce_record(addr: SocketAddr, topic: &str, topic_id: Uuid, key: &[u8], value: &[u8]) {
    let record = Record {
        offset_delta: 0,
        key: Some(Bytes::copy_from_slice(key)),
        value: Some(Bytes::copy_from_slice(value)),
        ..Default::default()
    };
    let batch = RecordBatch {
        last_offset_delta: 0,
        records: vec![record],
        ..Default::default()
    };

    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let version: i16 = 9; // flexible, pre-KIP-516 (no topic_id required on the wire at v9)
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode Produce");
    let resp_bytes = round_trip(&mut stream, 0, version, 1, true, &body)
        .await
        .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ProduceResponse::decode(&mut cur, version).expect("decode ProduceResponse");
    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "Produce must succeed: error_code={}",
        part.error_code
    );
}

/// A flattened record: key and value as plain byte vecs.
#[derive(Debug)]
struct FlatRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Fetch all records from (topic, partition 0) and start at offset 0.
///
/// The function returns a flat list of (key, value) pairs from all batches.
///
/// The generated `FetchResponse` codec decodes only the FIRST batch from each
/// partition's `records` field and silently discards the rest of the byte
/// stream. To collect every batch, the function fetches again and again. Each
/// time it moves `fetch_offset` past the last batch it saw. It stops when the
/// broker returns no batch.
async fn fetch_all(addr: SocketAddr, topic: &str, topic_id: Uuid) -> Vec<FlatRecord> {
    let version: i16 = 12; // flexible
    let mut out: Vec<FlatRecord> = Vec::new();
    let mut next_offset: i64 = 0;
    loop {
        let req = FetchRequest {
            replica_id: -1,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 22,
            topics: vec![FetchTopic {
                topic: topic.to_string(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: next_offset,
                    partition_max_bytes: 1 << 22,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let mut body = BytesMut::new();
        req.encode(&mut body, version).expect("encode Fetch");
        let resp_bytes = round_trip(&mut stream, 1, version, 1, true, &body)
            .await
            .expect("Fetch round-trip");
        let mut cur: &[u8] = &resp_bytes;
        let resp = FetchResponse::decode(&mut cur, version).expect("decode FetchResponse");

        let mut got_any = false;
        for topic_resp in &resp.responses {
            for part_resp in &topic_resp.partitions {
                assert!(
                    part_resp.error_code == 0,
                    "Fetch partition error: {}",
                    part_resp.error_code
                );
                if let Some(batches) = part_resp.records.as_ref().and_then(|p| p.as_v2()) {
                    for batch in batches {
                        got_any = true;
                        let batch_last_abs = batch.base_offset + i64::from(batch.last_offset_delta);
                        for record in &batch.records {
                            let key = match &record.key {
                                Some(k) => k.to_vec(),
                                None => continue,
                            };
                            let value = match &record.value {
                                Some(v) => v.to_vec(),
                                None => Vec::new(),
                            };
                            out.push(FlatRecord { key, value });
                        }
                        next_offset = batch_last_abs + 1;
                    }
                }
            }
        }
        if !got_any {
            break;
        }
    }
    out
}

fn assert_latest_records_survive(records: &[FlatRecord]) {
    let distinct_keys: std::collections::BTreeSet<String> = records
        .iter()
        .map(|record| String::from_utf8(record.key.clone()).unwrap())
        .filter(|key| key != "__pad__")
        .collect();
    assert!(
        distinct_keys
            == ["k1".to_string(), "k2".to_string(), "k3".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
        "k1, k2, k3 must all survive compaction; got: {distinct_keys:?}"
    );

    for key in ["k1", "k2", "k3"] {
        let values_for_key: Vec<String> = records
            .iter()
            .filter(|record| record.key == key.as_bytes())
            .map(|record| String::from_utf8(record.value.clone()).unwrap())
            .collect();
        let expected_latest = format!("v10-{key}");
        assert!(
            values_for_key.contains(&expected_latest),
            "key {key} must have latest value {expected_latest}; got {values_for_key:?}"
        );
        for stale_round in 0..10u32 {
            let stale = format!("v{stale_round}-{key}");
            assert!(
                !values_for_key.contains(&stale),
                "key {key} must NOT retain stale value {stale}; got {values_for_key:?}"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test
// ─────────────────────────────────────────────────────────────────────────────

/// End-to-end compaction test:
///
/// 1. Boot a single broker with cleaner interval = 1s.
/// 2. Create topic `compacted` with `cleanup.policy=compact` and `segment.bytes=256`.
/// 3. Produce 30 records, 10 for each of the 3 keys, values v0-k1..v9-k3.
/// 4. Wait for a compaction pass so the cleaner compacts the sealed segments.
/// 5. Force-roll the active segment. Produce v10-k1, v10-k2, and v10-k3.
/// 6. Wait for another compaction pass so the cleaner compacts the newly-sealed
///    segments.
/// 7. Fetch all records from offset 0.
/// 8. Assert that exactly 3 distinct keys survive.
/// 9. Assert that no stale value from v0-* to v9-* remains.
/// 10. Assert that each key has its latest value, v10-kN.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_dedupes_via_native_client() {
    let (handle, _dir, addr) = start_broker_with_fast_cleaner().await;

    // Create the compacted topic.
    create_topic_with_configs(
        addr,
        "compacted",
        1,
        1,
        vec![("cleanup.policy", "compact"), ("segment.bytes", "256")],
    )
    .await;

    // Wait for the partition to appear in the broker's registry.
    handle.wait_until_partition_present("compacted", 0).await;

    // Wait for the topic-config overrides (cleanup.policy=compact +
    // segment.bytes=256) to propagate from the metadata image through the
    // ReplicatorSupervisor reconcile loop into the partition's LogConfig.
    // Without this wait, produces can start before the supervisor reconciles,
    // so they land in a default-config Log (1GiB segments, Delete policy) →
    // no segment rolls, no compaction, test sees every record.
    // The LogConfig materializes downstream of the image, so poll the
    // partition's live LogConfig rather than the metadata image itself.
    handle
        .wait_for_metrics(
            "cleanup.policy/segment.bytes propagate to partition LogConfig",
            |_m| {
                handle
                    .partition_log_config_for_test("compacted", 0)
                    .is_some_and(|cfg| {
                        cfg.cleanup_policy == crabka_log::CleanupPolicy::Compact
                            && cfg.segment_size == crabka_units::bytes(256)
                    })
            },
        )
        .await;

    // Get the topic_id (needed for Fetch).
    let topic_id = get_topic_id(addr, "compacted").await;

    // Produce 30 records: 10 each under k1, k2, k3.
    // Values are "v{round}-{key}" so we can identify stale vs. latest.
    for round in 0..10u32 {
        for key in ["k1", "k2", "k3"] {
            let value = format!("v{round}-{key}");
            produce_record(
                addr,
                "compacted",
                topic_id,
                key.as_bytes(),
                value.as_bytes(),
            )
            .await;
        }
    }

    // The 256-byte segment limit causes many segment rolls during the produce
    // loop, so the cleaner finds sealed segments ready for compaction. Wait for
    // a compaction pass to run on this partition instead of sleeping. Capture
    // the current pass count right before the wait so the +1 pass is guaranteed
    // to run after the sealed segments exist.
    let compactions_before = handle
        .metrics()
        .log_compactions_total
        .get_or_create(&PartitionLabel {
            topic: "compacted".to_string(),
            partition: 0,
        })
        .get();
    handle
        .wait_for_metrics("compaction pass ran on sealed segments", |m| {
            m.log_compactions_total
                .get_or_create(&PartitionLabel {
                    topic: "compacted".to_string(),
                    partition: 0,
                })
                .get()
                > compactions_before
        })
        .await;

    // Force-roll the active segment by writing one more record per key.
    // After this the previously-active segment becomes sealed and eligible
    // for the next compaction pass.
    for key in ["k1", "k2", "k3"] {
        let value = format!("v10-{key}");
        produce_record(
            addr,
            "compacted",
            topic_id,
            key.as_bytes(),
            value.as_bytes(),
        )
        .await;
    }

    // Push the active segment into a sealed state so the FINAL v10-* records
    // can also be deduped. Without this the active still holds (at least) the
    // very last v10-k3 record, the compactor (which never touches the active)
    // can't see it, and the previous compaction's "latest" entry for k3 — the
    // v9-k3 record in the now-sealed segment — survives.
    //
    // We can't directly call `Log::roll_active_segment` from a test, so we
    // produce a small burst of records using a sentinel "pad" key (which the
    // assertions below ignore) until enough bytes accumulate to roll the
    // segment past `segment.bytes=256`. ~8 small records is more than enough.
    for round in 0..8 {
        let value = format!("padding-{round}");
        produce_record(addr, "compacted", topic_id, b"__pad__", value.as_bytes()).await;
    }

    // Wait for another compaction pass so the newly-sealed segments (holding
    // the final v10-* records and the now-sealed prior "latest" entries) get
    // compacted. Capture the pass count after the force-roll + padding burst
    // seal the active segment so the awaited +1 pass runs against them.
    let compactions_before_reroll = handle
        .metrics()
        .log_compactions_total
        .get_or_create(&PartitionLabel {
            topic: "compacted".to_string(),
            partition: 0,
        })
        .get();
    handle
        .wait_for_metrics("compaction pass ran on newly-sealed segments", |m| {
            m.log_compactions_total
                .get_or_create(&PartitionLabel {
                    topic: "compacted".to_string(),
                    partition: 0,
                })
                .get()
                > compactions_before_reroll
        })
        .await;

    // Fetch all records from offset 0.
    let records = fetch_all(addr, "compacted", topic_id).await;

    assert_latest_records_survive(&records);

    handle.shutdown().await;
}
