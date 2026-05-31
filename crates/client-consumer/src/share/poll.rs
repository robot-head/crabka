//! `ShareConsumer::poll` + acknowledgement (`ShareFetch` / `ShareAcknowledge`).
//!
//! `poll()` issues one `ShareFetch` over the live assignment. Acquired records
//! are paired with the `acquired_records` ranges the broker returns (so each
//! carries the broker's `delivery_count`), and the ranges are remembered for the
//! next poll's implicit auto-`Accept`.
//!
//! ## Acknowledgement
//!
//! - [`Implicit`](super::types::ShareAckMode::Implicit) (default): the *next*
//!   `poll()` (and `close()`) implicitly `Accept`s every range returned by the
//!   previous `poll()`. Nothing is required of the application.
//! - [`Explicit`](super::types::ShareAckMode::Explicit): the application calls
//!   [`acknowledge`](ShareConsumer::acknowledge) per record; staged acks are
//!   flushed on the next `poll()` (piggybacked) or via
//!   [`commit`](ShareConsumer::commit) (standalone `ShareAcknowledge`).
//!
//! ## Session epoch
//!
//! The broker's share-session cache opens at epoch 0 (storing 1) and then
//! expects each subsequent `ShareFetch` *or* `ShareAcknowledge` to carry the
//! stored epoch, incrementing on each accepted request. We mirror that exactly:
//! send `self.share_session_epoch`, and advance it by one after every successful
//! `ShareFetch` / `ShareAcknowledge` (sequence 0 → 1 → 2 → …). Getting this wrong
//! makes the broker drop the session (`INVALID_SHARE_SESSION_EPOCH`).

use std::collections::HashMap;
use std::time::Duration;

use crabka_protocol::owned::share_acknowledge_request::{
    AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
    ShareAcknowledgeRequest,
};
use crabka_protocol::owned::share_fetch_request::{
    AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ShareFetchRequest,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use super::consumer::ShareConsumer;
use super::types::{ShareAckMode, ShareAckType, ShareConsumerRecord};
use crate::error::ConsumerError;

/// `partition_max_bytes` / `max_bytes` budget for a `ShareFetch` (mirrors the
/// classic consumer's 50 MiB fetch budget).
const MAX_BYTES: i32 = 50 * 1024 * 1024;
/// Per-partition byte budget.
const PARTITION_MAX_BYTES: i32 = 1 << 20;
/// Cap on records returned per fetch.
const MAX_RECORDS: i32 = 500;

impl ShareConsumer {
    /// Acquire and return the next batch of records.
    ///
    /// Carries acknowledgements for the previous `poll()` (implicit auto-`Accept`
    /// or drained explicit `acknowledge()` calls) piggybacked on the
    /// `ShareFetch`, then decodes the acquired records — each paired with the
    /// broker-reported `delivery_count` from its `acquired_records` range.
    ///
    /// With no assignment yet, sleeps for `timeout` and returns empty (mirroring
    /// the classic [`Consumer::poll`](crate::Consumer::poll)).
    #[allow(clippy::too_many_lines)]
    pub async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ShareConsumerRecord>, ConsumerError> {
        // Snapshot the live assignment; with nothing assigned there is nothing
        // to fetch — sleep out the timeout and return empty (matches classic).
        let assignment = self.assignment.lock().await.clone();
        if assignment.is_empty() {
            tokio::time::sleep(timeout).await;
            return Ok(Vec::new());
        }

        // Build the piggyback acknowledgement batches per (topic_id, partition)
        // from the previous poll, draining the source so each ack is sent once.
        let acks = self.take_piggyback_acks();

        // Group assigned partitions by topic id, attaching the (topic_id,
        // partition) acks to the matching partition entry.
        let mut by_topic: HashMap<WireUuid, Vec<(i32, Vec<FetchAckBatch>)>> = HashMap::new();
        for (tid, _name, partition) in &assignment {
            let packs = acks.get(&(*tid, *partition)).cloned().unwrap_or_default();
            by_topic.entry(*tid).or_default().push((*partition, packs));
        }

        let topics: Vec<FetchTopic> = by_topic
            .into_iter()
            .map(|(topic_id, parts)| FetchTopic {
                topic_id,
                partitions: parts
                    .into_iter()
                    .map(
                        |(partition_index, acknowledgement_batches)| FetchPartition {
                            partition_index,
                            partition_max_bytes: PARTITION_MAX_BYTES,
                            acknowledgement_batches,
                            ..Default::default()
                        },
                    )
                    .collect(),
                ..Default::default()
            })
            .collect();

        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        let resp = self
            .client
            .send(ShareFetchRequest {
                group_id: Some(self.group_id.clone()),
                member_id: Some(self.member_id.clone()),
                share_session_epoch: self.share_session_epoch,
                max_wait_ms: timeout_ms,
                min_bytes: 1,
                max_bytes: MAX_BYTES,
                max_records: MAX_RECORDS,
                batch_size: MAX_RECORDS,
                share_acquire_mode: 0,
                is_renew_ack: false,
                topics,
                forgotten_topics_data: vec![],
                ..Default::default()
            })
            .await?;

        if resp.error_code != 0 {
            return Err(ConsumerError::Server(resp.error_code));
        }
        // A successful ShareFetch consumes one session epoch; advance to the
        // value the broker now expects (it stored ours + 1).
        self.share_session_epoch = self.share_session_epoch.wrapping_add(1);

        // Reverse-map topic id → name for the returned rows (the fetch response
        // carries only topic_id). Fall back to the cached topic_names.
        let name_for: HashMap<WireUuid, String> = assignment
            .iter()
            .map(|(tid, name, _)| (*tid, name.clone()))
            .collect();

        let mut out: Vec<ShareConsumerRecord> = Vec::new();
        let mut delivered: Vec<(WireUuid, i32, i64, i64)> = Vec::new();
        for topic in &resp.responses {
            let topic_name = name_for.get(&topic.topic_id).cloned().unwrap_or_default();
            for part in &topic.partitions {
                if part.acknowledge_error_code != 0 {
                    tracing::warn!(
                        topic = %topic_name,
                        partition = part.partition_index,
                        acknowledge_error_code = part.acknowledge_error_code,
                        "share fetch piggyback acknowledge error"
                    );
                }
                if part.error_code != 0 {
                    tracing::warn!(
                        topic = %topic_name,
                        partition = part.partition_index,
                        error_code = part.error_code,
                        "share fetch partition error"
                    );
                    continue;
                }

                // Remember the acquired ranges for the next implicit auto-Accept.
                for ar in &part.acquired_records {
                    delivered.push((
                        topic.topic_id,
                        part.partition_index,
                        ar.first_offset,
                        ar.last_offset,
                    ));
                }

                let Some(payload) = &part.records else {
                    continue;
                };
                let Some(batches) = payload.as_v2() else {
                    continue;
                };
                for batch in batches {
                    if batch.attributes.is_control_batch() {
                        continue;
                    }
                    for r in &batch.records {
                        let offset = batch.base_offset + i64::from(r.offset_delta);
                        // Pair the record with the acquired range that contains
                        // it to read the broker's delivery_count for this offset.
                        let delivery_count = part
                            .acquired_records
                            .iter()
                            .find(|ar| ar.first_offset <= offset && offset <= ar.last_offset)
                            .map_or(0, |ar| ar.delivery_count);
                        out.push(ShareConsumerRecord {
                            topic: topic_name.clone(),
                            partition: part.partition_index,
                            offset,
                            timestamp: batch.base_timestamp + r.timestamp_delta,
                            key: r.key.clone(),
                            value: r.value.clone(),
                            delivery_count,
                        });
                    }
                }
            }
        }

        // Implicit mode auto-Accepts these ranges on the next poll/close; explicit
        // mode acknowledges per record, so the ranges are not auto-accepted.
        self.prev_delivered = delivered;
        Ok(out)
    }

    /// Stage an explicit acknowledgement for `record` (Explicit mode).
    ///
    /// The ack is flushed on the next [`poll`](ShareConsumer::poll) (piggybacked)
    /// or [`commit`](ShareConsumer::commit) (standalone `ShareAcknowledge`).
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::IllegalState`] in
    /// [`Implicit`](super::types::ShareAckMode::Implicit) mode: there, every
    /// delivered record is auto-`Accept`ed on the next poll/close, so an explicit
    /// `acknowledge()` cannot be honored (staging it would silently leak into
    /// `pending_acks`, which the implicit path never flushes). This mirrors the
    /// JVM `KafkaShareConsumer`, which raises `IllegalStateException` if you
    /// explicitly acknowledge while in implicit acknowledgement mode.
    pub fn acknowledge(
        &mut self,
        record: &ShareConsumerRecord,
        ack: ShareAckType,
    ) -> Result<(), ConsumerError> {
        if self.ack_mode == ShareAckMode::Implicit {
            return Err(ConsumerError::IllegalState(
                "acknowledge() is not allowed in implicit ack mode; \
                 records are auto-accepted on the next poll/close"
                    .into(),
            ));
        }
        let topic_id = self.topic_id_for(&record.topic);
        self.pending_acks.push((
            topic_id,
            record.partition,
            record.offset,
            record.offset,
            ack.wire(),
        ));
        Ok(())
    }

    /// Flush staged explicit acknowledgements via a standalone
    /// `ShareAcknowledge`. No-op when nothing is staged.
    pub async fn commit(&mut self) -> Result<(), ConsumerError> {
        self.flush_pending_acks().await
    }

    /// Drain `pending_acks` into a `ShareAcknowledge`. Advances the session epoch
    /// on success (an accepted `ShareAcknowledge` consumes one epoch, exactly
    /// like a `ShareFetch`). No-op (and no epoch advance) when nothing is staged.
    pub(crate) async fn flush_pending_acks(&mut self) -> Result<(), ConsumerError> {
        if self.pending_acks.is_empty() {
            return Ok(());
        }
        let drained = std::mem::take(&mut self.pending_acks);
        let topics = build_ack_topics(drained);

        let resp = self
            .client
            .send(ShareAcknowledgeRequest {
                group_id: Some(self.group_id.clone()),
                member_id: Some(self.member_id.clone()),
                share_session_epoch: self.share_session_epoch,
                is_renew_ack: false,
                topics,
                ..Default::default()
            })
            .await?;
        if resp.error_code != 0 {
            return Err(ConsumerError::Server(resp.error_code));
        }
        self.share_session_epoch = self.share_session_epoch.wrapping_add(1);
        Ok(())
    }

    /// Build the piggyback acknowledgement batches for the next `ShareFetch`,
    /// keyed by `(topic_id, partition)`, consuming the source state.
    ///
    /// - Implicit: one `Accept` batch per previously-delivered range.
    /// - Explicit: the drained `pending_acks`, grouped into per-offset batches.
    fn take_piggyback_acks(&mut self) -> HashMap<(WireUuid, i32), Vec<FetchAckBatch>> {
        let mut out: HashMap<(WireUuid, i32), Vec<FetchAckBatch>> = HashMap::new();
        match self.ack_mode {
            ShareAckMode::Implicit => {
                for (tid, partition, first, last) in std::mem::take(&mut self.prev_delivered) {
                    let count = usize::try_from(last - first + 1).unwrap_or(0);
                    out.entry((tid, partition))
                        .or_default()
                        .push(FetchAckBatch {
                            first_offset: first,
                            last_offset: last,
                            acknowledge_types: vec![ShareAckType::Accept.wire(); count],
                            ..Default::default()
                        });
                }
            }
            ShareAckMode::Explicit => {
                for (tid, partition, first, last, ack) in std::mem::take(&mut self.pending_acks) {
                    let count = usize::try_from(last - first + 1).unwrap_or(0);
                    out.entry((tid, partition))
                        .or_default()
                        .push(FetchAckBatch {
                            first_offset: first,
                            last_offset: last,
                            acknowledge_types: vec![ack; count],
                            ..Default::default()
                        });
                }
                // Explicit mode never auto-accepts; clear any stale ranges.
                self.prev_delivered.clear();
            }
        }
        out
    }

    /// Resolve a topic id from a topic name via the live assignment / cached
    /// `topic_names`. Returns the zero uuid if unknown (the broker will reject the
    /// ack, surfacing the misuse rather than silently mis-acking).
    fn topic_id_for(&self, name: &str) -> WireUuid {
        // The assignment carries (topic_id, name, partition); use it first since
        // it is the set the application is acking against. `try_lock` keeps this
        // sync (acknowledge() takes &mut self, not async).
        if let Ok(assignment) = self.assignment.try_lock()
            && let Some((tid, _, _)) = assignment.iter().find(|(_, n, _)| n == name)
        {
            return *tid;
        }
        if let Ok(names) = self.topic_names.try_lock()
            && let Some((tid, _)) = names.iter().find(|(_, n)| n.as_str() == name)
        {
            return *tid;
        }
        WireUuid::default()
    }
}

/// Group `(topic_id, partition, first, last, ack_wire)` acks into
/// `ShareAcknowledge` topic/partition/batch shape, coalescing by topic and
/// partition.
fn build_ack_topics(acks: Vec<(WireUuid, i32, i64, i64, i8)>) -> Vec<AcknowledgeTopic> {
    let mut by_topic: HashMap<WireUuid, HashMap<i32, Vec<AckAckBatch>>> = HashMap::new();
    for (tid, partition, first, last, ack) in acks {
        let count = usize::try_from(last - first + 1).unwrap_or(0);
        by_topic
            .entry(tid)
            .or_default()
            .entry(partition)
            .or_default()
            .push(AckAckBatch {
                first_offset: first,
                last_offset: last,
                acknowledge_types: vec![ack; count],
                ..Default::default()
            });
    }
    by_topic
        .into_iter()
        .map(|(topic_id, parts)| AcknowledgeTopic {
            topic_id,
            partitions: parts
                .into_iter()
                .map(
                    |(partition_index, acknowledgement_batches)| AcknowledgePartition {
                        partition_index,
                        acknowledgement_batches,
                        ..Default::default()
                    },
                )
                .collect(),
            ..Default::default()
        })
        .collect()
}
