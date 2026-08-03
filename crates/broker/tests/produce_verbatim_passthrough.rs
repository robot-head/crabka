//! End-to-end coverage for the verbatim produce passthrough (the
//! header-only decode path): a producer-LZ4-compressed v2 batch is stored
//! WITHOUT being decompressed/re-encoded and round-trips byte-identically on
//! Fetch, while a recompression-forcing topic config, a control batch, and
//! an idempotent producer all behave correctly across the path.
//!
//! These complement the unit tests in `handlers::produce::tests::verbatim`,
//! which pin the dispatch (`prepare_batch` / `build_produce_data`) at the
//! function level; here we drive the whole broker over the wire. Produce /
//! Fetch auto-negotiate to v13 (KIP-516 topic-id), so every batch travels the
//! v≥3 native-v2 path the verbatim dispatch covers.

use assert2::{assert, check};
mod support;

use std::time::{Duration, Instant};

use bytes::Bytes;
use crabka_compression::CompressionType;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Attributes, HEADER_LEN, Record, RecordBatch, RecordsPayload},
};

async fn topic_id_for(client: &crabka_client_core::Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// Build a single v2 `RecordBatch` carrying `n` copies of `value`, with the
/// given codec. Encoding compresses the body when the codec isn't `None`.
fn batch(codec: CompressionType, n: usize, value: &[u8]) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: i32::try_from(n).unwrap() - 1,
        max_timestamp: 12_345,
        producer_id: -1,
        ..RecordBatch::default()
    };
    b.attributes = b.attributes.with_compression(codec);
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(Bytes::copy_from_slice(value)),
            ..Default::default()
        });
    }
    b
}

fn encode_batch(b: &RecordBatch) -> Bytes {
    let mut buf = bytes::BytesMut::new();
    b.encode(&mut buf).unwrap();
    buf.freeze()
}

async fn create_topic(broker: &crabka_broker::BrokerHandle, bootstrap: &str, name: &str) {
    create_topic_with_configs(broker, bootstrap, name, vec![]).await;
}

async fn wait_for_compression(
    broker: &crabka_broker::BrokerHandle,
    topic: &str,
    expected: Option<CompressionType>,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(cfg) = broker.partition_log_config_for_test(topic, 0)
            && cfg.compression_type == expected
        {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "compression_type={expected:?} never propagated to partition LogConfig within 10s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_topic_with_configs(
    broker: &crabka_broker::BrokerHandle,
    bootstrap: &str,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {resp:?}"
    );
    broker.wait_until_partition_present(name, 0).await;
}

/// Produce a single batch to `topic` partition 0 (acks=1), returning the
/// assigned base offset.
async fn produce_one(
    client: &crabka_client_core::Client,
    topic: &str,
    topic_id: WireUuid,
    b: RecordBatch,
) -> Result<i64, i16> {
    produce_batches(client, topic, topic_id, vec![b]).await
}

async fn produce_batches(
    client: &crabka_client_core::Client,
    topic: &str,
    topic_id: WireUuid,
    batches: Vec<RecordBatch>,
) -> Result<i64, i16> {
    let resp = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::V2(batches)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}

/// Fetch partition 0 from offset 0 and return the first decoded batch.
///
/// `n` is the number of records already produced to partition 0. Wait for the
/// high watermark, not just the log end offset: `acks=1` can return before the
/// asynchronous watermark update makes those records readable by consumers.
async fn fetch_first_batch(
    broker: &crabka_broker::BrokerHandle,
    client: &crabka_client_core::Client,
    topic: &str,
    topic_id: WireUuid,
    n: i64,
) -> RecordBatch {
    broker.wait_until_high_watermark(topic, 0, n).await;
    let resp = client
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 1_000,
            min_bytes: 1,
            max_bytes: 8 << 20,
            topics: vec![FetchTopic {
                topic: topic.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 8 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("Fetch");
    let pd = &resp.responses[0].partitions[0];
    assert!(pd.error_code == 0, "fetch error: {pd:?}");
    let payload = pd.records.as_ref().expect("records present");
    let batches = payload.as_v2().expect("v2 payload");
    batches.first().cloned().expect("at least one batch")
}

async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
        dir.path().to_path_buf(),
    ))
    .await
    .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// A producer-LZ4-compressed v2 batch whose DECOMPRESSED form is large
/// (~100 KiB) takes the verbatim path: the broker stores it WITHOUT
/// decompressing, preserves the Lz4 codec (no recompression), and the data
/// round-trips correctly on Fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lz4_batch_passes_through_and_roundtrips() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "lz4t").await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "lz4t").await;

    // 200 records of a highly-compressible 512-byte value → ~100 KiB raw,
    // tiny compressed. A decompress on the produce path would be obvious.
    let value = vec![b'Z'; 512];
    let b = batch(CompressionType::Lz4, 200, &value);
    let wire = encode_batch(&b);
    let raw_uncompressed_size = 200 * 512;
    assert!(
        wire.len() < raw_uncompressed_size / 8,
        "lz4 wire ({} B) must be far smaller than raw ({} B)",
        wire.len(),
        raw_uncompressed_size
    );

    let base = produce_one(&client, "lz4t", topic_id, b.clone())
        .await
        .expect("produce ok");
    assert!(base == 0);

    // Fetch it back: the stored batch must still be Lz4-compressed (no
    // recompression to a different codec) and decode to the same records.
    let fetched = fetch_first_batch(&broker, &client, "lz4t", topic_id, 200).await;
    check!(
        fetched.attributes.compression() == CompressionType::Lz4,
        "stored batch must keep producer's Lz4 codec; got {:?}",
        fetched.attributes.compression()
    );
    assert!(fetched.records.len() == 200, "all records round-trip");
    check!(fetched.records[0].value.as_deref() == Some(&value[..]));
    check!(fetched.records[199].value.as_deref() == Some(&value[..]));
    check!(fetched.base_offset == 0);

    broker.shutdown().await;
}

/// An UNCOMPRESSED v2 batch takes the verbatim path and round-trips
/// byte-identically: the CRC-covered region (bytes 21..) of the stored bytes
/// equals the producer's wire bytes exactly — only `base_offset` /
/// `partition_leader_epoch` (both before the CRC region) are patched. We
/// re-encode the fetched batch and compare; for an uncompressed batch the
/// re-encode is deterministic and byte-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncompressed_batch_roundtrips_byte_identically() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "raw").await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "raw").await;

    let b = batch(CompressionType::None, 3, b"payload");
    let wire = encode_batch(&b);

    produce_one(&client, "raw", topic_id, b.clone())
        .await
        .expect("produce ok");

    let fetched = fetch_first_batch(&broker, &client, "raw", topic_id, 3).await;
    let fetched_wire = encode_batch(&fetched);

    // The CRC-covered region (attributes onward) must be byte-identical to
    // what the producer sent — proving no decode/re-encode/recompress.
    check!(
        fetched_wire[HEADER_LEN..] == wire[HEADER_LEN..],
        "record body must be verbatim"
    );
    check!(
        fetched_wire[21..HEADER_LEN] == wire[21..HEADER_LEN],
        "CRC-covered header (attributes..records_count) must be verbatim"
    );
    // The producer's CRC bytes (17..21) are preserved (no recompute).
    check!(fetched_wire[17..21] == wire[17..21], "CRC field unchanged");

    broker.shutdown().await;
}

/// A topic configured with a concrete `compression.type` that differs from
/// the producer's codec forces broker-side recompression → the OWNED path.
/// The stored batch must carry the TOPIC's codec, and the data must still be
/// correct. This pins that the verbatim predicate's recompression gate routes
/// to the owned fallback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recompression_config_takes_owned_path() {
    let (broker, bootstrap, _dir) = boot().await;
    // Topic forces zstd; producer sends lz4 → must recompress (owned path).
    create_topic_with_configs(
        &broker,
        &bootstrap,
        "recmp",
        vec![CreatableTopicConfig {
            name: "compression.type".into(),
            value: Some("zstd".into()),
            ..Default::default()
        }],
    )
    .await;
    wait_for_compression(&broker, "recmp", Some(CompressionType::Zstd)).await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "recmp").await;

    let value = vec![b'Q'; 256];
    let b = batch(CompressionType::Lz4, 10, &value);
    produce_one(&client, "recmp", topic_id, b.clone())
        .await
        .expect("produce ok");

    let fetched = fetch_first_batch(&broker, &client, "recmp", topic_id, 10).await;
    // Owned path recompressed lz4 → zstd: stored batch carries the TOPIC codec.
    check!(
        fetched.attributes.compression() == CompressionType::Zstd,
        "recompression config must rewrite codec to zstd; got {:?}",
        fetched.attributes.compression()
    );
    assert!(fetched.records.len() == 10);
    check!(fetched.records[0].value.as_deref() == Some(&value[..]));

    broker.shutdown().await;
}

/// A control batch never takes the verbatim path (its LSO bookkeeping needs
/// the inner marker record). Producing a control batch must route to the
/// owned path and stay correct on fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_batch_takes_owned_path() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "ctrl").await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "ctrl").await;

    // A (non-compressed) control batch with one marker-shaped record.
    let mut b = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 99,
        producer_id: -1,
        ..RecordBatch::default()
    };
    b.attributes = Attributes::default().with_control(true);
    b.records.push(Record {
        offset_delta: 0,
        key: Some(Bytes::from_static(&[0, 0, 0, 0])),
        value: Some(Bytes::from_static(&[0, 0, 0, 0])),
        ..Default::default()
    });

    produce_one(&client, "ctrl", topic_id, b.clone())
        .await
        .expect("produce ok");

    let fetched = fetch_first_batch(&broker, &client, "ctrl", topic_id, 1).await;
    assert!(
        fetched.attributes.is_control_batch(),
        "control bit preserved"
    );

    broker.shutdown().await;
}

/// Idempotent-producer dedup is driven by the HEADER fields the verbatim path
/// exposes (pid / epoch / `base_sequence` / `last_offset_delta`). Two appends with
/// increasing sequences both succeed; a retry of the first sequence is
/// recognized as a duplicate and returns the SAME base offset; an out-of-order
/// sequence is rejected — all without the broker ever decompressing the (lz4)
/// batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_dedup_over_verbatim_path() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "idem").await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "idem").await;

    // Build an lz4 idempotent batch with explicit pid/epoch/base_sequence so
    // the dedup gate must read those from the header (no record decode).
    let big = vec![b'I'; 1024];
    let make = |base_seq: i32, n: usize| -> RecordBatch {
        let mut b = RecordBatch {
            last_offset_delta: i32::try_from(n).unwrap() - 1,
            max_timestamp: 77,
            producer_id: 9_001,
            producer_epoch: 0,
            base_sequence: base_seq,
            ..RecordBatch::default()
        };
        b.attributes = b.attributes.with_compression(CompressionType::Lz4);
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i32::try_from(i).unwrap(),
                value: Some(Bytes::copy_from_slice(&big)),
                ..Default::default()
            });
        }
        b
    };

    // seq 0..=2 (3 records) → base offset 0.
    let base0 = produce_one(&client, "idem", topic_id, make(0, 3))
        .await
        .expect("first append ok");
    assert!(base0 == 0);

    // seq 3..=4 (2 records) → base offset 3.
    let base1 = produce_one(&client, "idem", topic_id, make(3, 2))
        .await
        .expect("second append ok");
    assert!(base1 == 3);
    broker.wait_until_local_log_end_offset("idem", 0, 5).await;

    // Retry the MOST RECENT batch (seq 3..=4) → DUPLICATE: the dedup tracker
    // tracks the last committed batch and echoes its base offset (3), no error.
    // This is driven purely by the header pid/epoch/base_sequence — the lz4
    // body is never decompressed.
    let base_dup = produce_one(&client, "idem", topic_id, make(3, 2))
        .await
        .expect("duplicate must be NONE");
    assert!(base_dup == 3, "duplicate returns the committed base offset");

    // An out-of-order sequence (skip ahead past last+1) →
    // OUT_OF_ORDER_SEQUENCE_NUMBER (45). The last committed sequence is 4, so
    // base_sequence 99 leaves a gap.
    let err = produce_one(&client, "idem", topic_id, make(99, 1))
        .await
        .expect_err("out-of-order must error");
    assert!(err == 45, "out-of-order sequence must be 45; got {err}");

    broker.shutdown().await;
}
