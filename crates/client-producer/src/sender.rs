//! Background sender task. Drains ready batches from every accumulator
//! and ships them as `ProduceRequest`s through `crabka-client-core`.
//!
//! The sender is `tokio::spawn`'d by the builder. It owns the `wake_rx`
//! `Receiver` end of the wake channel (the `Producer` holds the
//! `wake_tx` `Sender`), the `flush_notify`, the `accumulators` map, and
//! the `next_seq` map. On every linger tick or wake signal it walks the
//! accumulators, seals + drains a batch from each, builds a v2
//! `RecordBatch`, frames a `ProduceRequest`, sends it via `Client`, and
//! resolves each record's `oneshot::Sender` from the per-partition
//! response.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_compression::CompressionType;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader};

use crate::accumulator::{Accumulator, InProgressBatch, PendingRecord};
use crate::compression::Compression;
use crate::error::ProducerError;
use crate::producer::{Acks, STATE_ACTIVE, STATE_FENCED, TopicMetadata};
use crate::record::RecordMetadata;

/// Wire error codes referenced when interpreting `PartitionProduceResponse`.
mod codes {
    pub const NONE: i16 = 0;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub const INVALID_PRODUCER_EPOCH: i16 = 53;
}

/// All the bits of state the sender task needs. The builder constructs
/// one of these, hands it to [`run`], and drops it.
#[allow(clippy::type_complexity)] // accumulators map mirrors the Producer field; alias deferred.
pub(crate) struct SenderConfig {
    pub client: Client,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub acks: Acks,
    pub compression: Compression,
    pub linger: Duration,
    pub request_timeout: Duration,
    pub retries: i32,
    pub retry_backoff: Duration,
    pub metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    pub accumulators: Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>,
    pub next_seq: Arc<DashMap<(String, i32), i32>>,
    pub state: Arc<AtomicU8>,
    pub wake_rx: tokio::sync::mpsc::Receiver<()>,
    pub flush_notify: Arc<Notify>,
    pub shutdown: CancellationToken,
}

pub(crate) async fn run(mut cfg: SenderConfig) {
    let mut ticker = tokio::time::interval(cfg.linger.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cfg.shutdown.cancelled() => break,
            _ = ticker.tick() => {
                drain_once(&mut cfg).await;
            }
            _ = cfg.wake_rx.recv() => {
                drain_once(&mut cfg).await;
            }
        }
    }

    // Drain anything left when we shut down so `close()` doesn't drop records.
    drain_once(&mut cfg).await;
}

/// Walk every accumulator, seal its current batch, pop one ready batch,
/// and send it. Notifies `flush_notify` when there was nothing to do —
/// that's the signal `Producer::flush` waits on.
async fn drain_once(cfg: &mut SenderConfig) {
    let keys: Vec<(String, i32)> = cfg.accumulators.iter().map(|e| e.key().clone()).collect();
    let mut any_work = false;
    for key in keys {
        let acc = match cfg.accumulators.get(&key) {
            Some(a) => a.value().clone(),
            None => continue,
        };
        let batch = {
            let mut a = acc.lock().await;
            a.seal_current();
            a.ready.pop_front()
        };
        let Some(batch) = batch else { continue };
        any_work = true;
        send_one(cfg, &key.0, key.1, batch).await;
    }
    if !any_work {
        cfg.flush_notify.notify_waiters();
    }
}

/// Send a single (topic, partition) batch and resolve its records'
/// oneshot acks from the response.
async fn send_one(cfg: &SenderConfig, topic: &str, partition: i32, batch: InProgressBatch) {
    // 1. Allocate the base_sequence range for this batch.
    let base_sequence = {
        let mut entry = cfg
            .next_seq
            .entry((topic.to_string(), partition))
            .or_insert(0);
        let cur = *entry;
        let count = i32::try_from(batch.records.len()).unwrap_or(i32::MAX);
        *entry = cur.wrapping_add(count);
        cur
    };

    // 2. Resolve the topic_id from the metadata cache (zero is fine —
    //    the broker falls back to the `name` field for v ≤ 12).
    let topic_id = cfg
        .metadata_cache
        .lock()
        .await
        .get(topic)
        .map_or(Uuid::ZERO, |m| m.topic_id);

    // 3. Build the v2 RecordBatch (compression handled by RecordBatch::encode).
    let record_batch = build_record_batch(cfg, &batch, base_sequence);

    // 4. Frame the ProduceRequest.
    let req = ProduceRequest {
        transactional_id: None,
        acks: cfg.acks.wire(),
        timeout_ms: i32::try_from(cfg.request_timeout.as_millis()).unwrap_or(i32::MAX),
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(record_batch),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // 5. Send with retries.
    let mut attempts: i32 = 0;
    let resp = loop {
        attempts += 1;
        match cfg.client.send(req.clone()).await {
            Ok(r) => break r,
            Err(e) => {
                if attempts > cfg.retries {
                    tracing::error!(
                        topic,
                        partition,
                        error = %e,
                        "producer giving up after {attempts} attempts",
                    );
                    fail_batch(batch.records, ProducerError::Client(e));
                    return;
                }
                tracing::warn!(topic, partition, error = %e, "produce attempt {attempts} failed; retrying");
                tokio::time::sleep(cfg.retry_backoff).await;
            }
        }
    };

    // 6. Resolve the per-(topic, partition) entry in the response.
    let part_resp = resp
        .responses
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partition_responses.iter().find(|p| p.index == partition));
    let Some(part_resp) = part_resp else {
        fail_batch(batch.records, ProducerError::Closed);
        return;
    };

    match part_resp.error_code {
        codes::NONE | codes::DUPLICATE_SEQUENCE_NUMBER => {
            // DUPLICATE_SEQUENCE_NUMBER means the broker already committed
            // this batch — same base_offset is returned, so we can ack the
            // caller as if the original write succeeded.
            for r in batch.records {
                let _ = r.ack.send(Ok(RecordMetadata {
                    topic_index: 0,
                    partition,
                    offset: part_resp.base_offset + i64::from(r.offset_delta),
                    timestamp_ms: r.timestamp_ms,
                }));
            }
        }
        codes::OUT_OF_ORDER_SEQUENCE_NUMBER | codes::INVALID_PRODUCER_EPOCH => {
            cfg.state
                .compare_exchange(
                    STATE_ACTIVE,
                    STATE_FENCED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .ok();
            fail_batch(batch.records, ProducerError::FencedProducer);
        }
        code => {
            fail_batch(batch.records, ProducerError::Server(code));
        }
    }
}

/// Build a v2 `RecordBatch` from a sealed `InProgressBatch`. Compression
/// is encoded into the batch attributes; the actual compress step runs
/// inside `RecordBatch::encode`.
fn build_record_batch(
    cfg: &SenderConfig,
    batch: &InProgressBatch,
    base_sequence: i32,
) -> RecordBatch {
    let codec = match cfg.compression {
        Compression::None => CompressionType::None,
        Compression::Gzip => CompressionType::Gzip,
        Compression::Snappy => CompressionType::Snappy,
        Compression::Lz4 => CompressionType::Lz4,
        Compression::Zstd => CompressionType::Zstd,
    };
    let attributes = Attributes::default().with_compression(codec);

    let base_timestamp = batch.records.first().map_or(0, |r| r.timestamp_ms);
    let max_timestamp = batch
        .records
        .iter()
        .map(|r| r.timestamp_ms)
        .max()
        .unwrap_or(0);
    let last_offset_delta =
        i32::try_from(batch.records.len().saturating_sub(1)).unwrap_or(i32::MAX);

    let mut records: Vec<Record> = Vec::with_capacity(batch.records.len());
    for r in &batch.records {
        let headers: Vec<RecordHeader> = r
            .headers
            .iter()
            .map(|h| RecordHeader {
                key: h.key.clone(),
                value: h.value.clone(),
            })
            .collect();
        records.push(Record {
            attributes: 0,
            timestamp_delta: r.timestamp_ms - base_timestamp,
            offset_delta: r.offset_delta,
            key: r.key.clone(),
            value: r.value.clone(),
            headers,
        });
    }

    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 0,
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id: cfg.producer_id,
        producer_epoch: cfg.producer_epoch,
        base_sequence,
        records,
    }
}

/// Resolve every record in `records` with an error. `ClientError` is not
/// `Clone`, so we hand the real error to the first record and a generic
/// `Closed` to the rest.
fn fail_batch(records: Vec<PendingRecord>, err: ProducerError) {
    let mut iter = records.into_iter();
    if let Some(first) = iter.next() {
        let _ = first.ack.send(Err(err));
    }
    for r in iter {
        let _ = r.ack.send(Err(ProducerError::Closed));
    }
}
