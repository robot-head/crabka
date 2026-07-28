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

use std::{collections::HashMap, time::Duration};

use crabka_protocol::{
    owned::{
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
            ShareAcknowledgeRequest,
        },
        share_fetch_request::{
            AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::{
    consumer::ShareConsumer,
    types::{ShareAckMode, ShareAckType, ShareAcquireMode, ShareConsumerRecord},
};
use crate::error::ConsumerError;

fn build_share_fetch_topics(
    assignment: &[(WireUuid, String, i32)],
    acks: &HashMap<(WireUuid, i32), Vec<FetchAckBatch>>,
) -> Vec<FetchTopic> {
    let mut by_topic: HashMap<WireUuid, Vec<(i32, Vec<FetchAckBatch>)>> = HashMap::new();
    for (tid, _name, partition) in assignment {
        let packs = acks.get(&(*tid, *partition)).cloned().unwrap_or_default();
        by_topic.entry(*tid).or_default().push((*partition, packs));
    }

    by_topic
        .into_iter()
        .map(|(topic_id, parts)| FetchTopic {
            topic_id,
            partitions: parts
                .into_iter()
                .map(
                    |(partition_index, acknowledgement_batches)| FetchPartition {
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

#[allow(clippy::too_many_arguments)]
fn build_share_fetch_request(
    group_id: String,
    member_id: String,
    share_session_epoch: i32,
    timeout: Duration,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    acquire_mode: ShareAcquireMode,
    topics: Vec<FetchTopic>,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group_id),
        member_id: Some(member_id),
        share_session_epoch,
        max_wait_ms: i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX),
        min_bytes,
        max_bytes,
        max_records,
        batch_size: max_records,
        share_acquire_mode: acquire_mode.wire(),
        topics,
        ..Default::default()
    }
}

fn build_share_ack_request(
    group_id: String,
    member_id: String,
    share_session_epoch: i32,
    is_renew_ack: bool,
    topics: Vec<AcknowledgeTopic>,
) -> ShareAcknowledgeRequest {
    ShareAcknowledgeRequest {
        group_id: Some(group_id),
        member_id: Some(member_id),
        share_session_epoch,
        is_renew_ack,
        topics,
        ..Default::default()
    }
}

fn response_has_error(error_code: i16) -> bool {
    error_code != 0
}

fn range_len(first: i64, last: i64) -> usize {
    usize::try_from(last - first + 1).unwrap_or(0)
}

fn offset_in_range(first: i64, offset: i64, last: i64) -> bool {
    first <= offset && offset <= last
}

fn record_offset(base_offset: i64, offset_delta: i32) -> i64 {
    base_offset + i64::from(offset_delta)
}

fn record_timestamp(base_timestamp: i64, timestamp_delta: i64) -> i64 {
    base_timestamp + timestamp_delta
}

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
    #[tracing::instrument(
        name = "share_consumer.poll",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            member_id = %self.member_id,
            session_epoch = self.share_session_epoch,
            timeout_ms = timeout.as_millis(),
            assigned_partitions = tracing::field::Empty,
            records = tracing::field::Empty,
        ),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ShareConsumerRecord>, ConsumerError> {
        // Snapshot the live assignment; with nothing assigned there is nothing
        // to fetch — sleep out the timeout and return empty (matches classic).
        let assignment = self.assignment.lock().await.clone();
        tracing::Span::current().record("assigned_partitions", assignment.len());
        if assignment.is_empty() {
            tokio::time::sleep(timeout).await;
            return Ok(Vec::new());
        }

        // Build the piggyback acknowledgement batches per (topic_id, partition)
        // from the previous poll, draining the source so each ack is sent once.
        let acks = self.take_piggyback_acks();

        // Group assigned partitions by topic id, attaching the (topic_id,
        // partition) acks to the matching partition entry.
        let topics = build_share_fetch_topics(&assignment, &acks);

        let resp = self
            .client
            .send(build_share_fetch_request(
                self.group_id.clone(),
                self.member_id.clone(),
                self.share_session_epoch,
                timeout,
                self.fetch_min_bytes,
                self.fetch_max_bytes,
                self.fetch_max_records,
                self.acquire_mode,
                topics,
            ))
            .await?;

        if response_has_error(resp.error_code) {
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
                if response_has_error(part.acknowledge_error_code) {
                    tracing::warn!(
                        topic = %topic_name,
                        partition = part.partition_index,
                        acknowledge_error_code = part.acknowledge_error_code,
                        "share fetch piggyback acknowledge error"
                    );
                }
                if response_has_error(part.error_code) {
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
                        let offset = record_offset(batch.base_offset, r.offset_delta);
                        // Pair the record with the acquired range that contains
                        // it to read the broker's delivery_count for this offset.
                        let delivery_count = part
                            .acquired_records
                            .iter()
                            .find(|ar| offset_in_range(ar.first_offset, offset, ar.last_offset))
                            .map_or(0, |ar| ar.delivery_count);
                        out.push(ShareConsumerRecord {
                            topic: topic_name.clone(),
                            partition: part.partition_index,
                            offset,
                            timestamp: record_timestamp(batch.base_timestamp, r.timestamp_delta),
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
        tracing::Span::current().record("records", out.len());
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

    /// Renew the acquisition lock on a single delivered `record` (KIP-932
    /// RENEW). Sends a standalone `ShareAcknowledge` with `is_renew_ack = true`
    /// and an empty `acknowledge_types` for the record's offset, which extends
    /// the broker-side lock deadline without changing the record's state. Like
    /// [`acknowledge`](ShareConsumer::acknowledge), this is only valid in
    /// explicit ack mode. Advances the session epoch on success.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::IllegalState`] in
    /// [`Implicit`](super::types::ShareAckMode::Implicit) mode (records are
    /// auto-accepted on the next poll/close, so renewing a lock is meaningless),
    /// and [`ConsumerError::Server`] if the broker rejects the renew.
    #[tracing::instrument(
        name = "share_consumer.renew",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            partition = record.partition,
            offset = record.offset,
        ),
        err
    )]
    pub async fn renew(&mut self, record: &ShareConsumerRecord) -> Result<(), ConsumerError> {
        if self.ack_mode == ShareAckMode::Implicit {
            return Err(ConsumerError::IllegalState(
                "renew() is not allowed in implicit ack mode; \
                 records are auto-accepted on the next poll/close"
                    .into(),
            ));
        }
        let topic_id = self.topic_id_for(&record.topic);
        let topics = build_ack_topics(vec![(
            topic_id,
            record.partition,
            record.offset,
            record.offset,
            0,
        )]);

        let resp = self
            .client
            .send(build_share_ack_request(
                self.group_id.clone(),
                self.member_id.clone(),
                self.share_session_epoch,
                true,
                topics,
            ))
            .await?;
        if response_has_error(resp.error_code) {
            return Err(ConsumerError::Server(resp.error_code));
        }
        self.share_session_epoch = self.share_session_epoch.wrapping_add(1);
        Ok(())
    }

    /// Flush staged explicit acknowledgements via a standalone
    /// `ShareAcknowledge`. No-op when nothing is staged.
    #[tracing::instrument(
        name = "share_consumer.commit",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, pending_acks = self.pending_acks.len()),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn commit(&mut self) -> Result<(), ConsumerError> {
        self.flush_pending_acks().await
    }

    /// Drain `pending_acks` into a `ShareAcknowledge`. Advances the session epoch
    /// on success (an accepted `ShareAcknowledge` consumes one epoch, exactly
    /// like a `ShareFetch`). No-op (and no epoch advance) when nothing is staged.
    #[tracing::instrument(
        name = "share_consumer.flush_pending_acks",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            session_epoch = self.share_session_epoch,
            pending_acks = self.pending_acks.len(),
        ),
        err
    )]
    pub(crate) async fn flush_pending_acks(&mut self) -> Result<(), ConsumerError> {
        if self.pending_acks.is_empty() {
            return Ok(());
        }
        let drained = std::mem::take(&mut self.pending_acks);
        let topics = build_ack_topics(drained);

        let resp = self
            .client
            .send(build_share_ack_request(
                self.group_id.clone(),
                self.member_id.clone(),
                self.share_session_epoch,
                false,
                topics,
            ))
            .await?;
        if response_has_error(resp.error_code) {
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
                    let count = range_len(first, last);
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
                    let count = range_len(first, last);
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
        let count = if ack == 0 { 0 } else { range_len(first, last) };
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use crabka_client_core::Client;
    use crabka_protocol::tagged_fields::UnknownTaggedFields;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
    }

    async fn test_consumer(ack_mode: ShareAckMode) -> ShareConsumer {
        ShareConsumer {
            client: Client::builder()
                .bootstrap("127.0.0.1:1")
                .client_id("share-poll-test")
                .build()
                .await
                .unwrap(),
            group_id: "group-a".into(),
            member_id: "member-a".into(),
            member_epoch: Arc::new(Mutex::new(3)),
            assignment: Arc::new(Mutex::new(vec![(id(7), "topic-a".into(), 2)])),
            topic_names: Arc::new(Mutex::new(HashMap::new())),
            share_session_epoch: 4,
            fetch_min_bytes: crate::share::DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES,
            fetch_max_bytes: crate::share::DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES,
            fetch_max_records: crate::share::DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS,
            acquire_mode: ShareAcquireMode::BatchOptimized,
            ack_mode,
            pending_acks: Vec::new(),
            prev_delivered: Vec::new(),
            shutdown: CancellationToken::new(),
            hb_handle: None,
        }
    }

    fn only<T>(items: &[T]) -> &T {
        assert2::assert!(items.len() == 1);
        &items[0]
    }

    #[test]
    fn share_fetch_request_preserves_acquire_mode_limits_and_timeout_bounds() {
        let topic = FetchTopic {
            topic_id: id(7),
            partitions: vec![FetchPartition {
                partition_index: 2,
                partition_max_bytes: 123,
                acknowledgement_batches: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let req = build_share_fetch_request(
            "group-a".into(),
            "member-a".into(),
            4,
            Duration::from_millis(250),
            7,
            65_536,
            37,
            ShareAcquireMode::RecordLimit,
            vec![topic.clone()],
        );

        assert2::assert!(
            req == ShareFetchRequest {
                group_id: Some("group-a".into()),
                member_id: Some("member-a".into()),
                share_session_epoch: 4,
                max_wait_ms: 250,
                min_bytes: 7,
                max_bytes: 65_536,
                max_records: 37,
                batch_size: 37,
                share_acquire_mode: 1,
                is_renew_ack: false,
                topics: vec![topic],
                forgotten_topics_data: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }
        );

        let saturated = build_share_fetch_request(
            "group-a".into(),
            "member-a".into(),
            4,
            Duration::from_millis(u64::from(u32::MAX)),
            7,
            65_536,
            37,
            ShareAcquireMode::BatchOptimized,
            Vec::new(),
        );
        assert2::assert!((saturated.max_wait_ms, saturated.share_acquire_mode) == (i32::MAX, 0));
    }

    #[test]
    fn share_fetch_topics_group_assignment_and_attach_partition_acks() {
        let ack = FetchAckBatch {
            first_offset: 10,
            last_offset: 11,
            acknowledge_types: vec![ShareAckType::Accept.wire(); 2],
            ..Default::default()
        };
        let mut acks = HashMap::new();
        acks.insert((id(7), 2), vec![ack.clone()]);

        let topics = build_share_fetch_topics(
            &[
                (id(7), "topic-a".into(), 2),
                (id(7), "topic-a".into(), 3),
                (id(8), "topic-b".into(), 1),
            ],
            &acks,
        );

        let topic = topics.iter().find(|topic| topic.topic_id == id(7)).unwrap();
        assert2::assert!(topic.partitions.len() == 2);
        let part = topic
            .partitions
            .iter()
            .find(|part| part.partition_index == 2)
            .unwrap();
        assert2::assert!(
            (
                part.partition_max_bytes,
                part.acknowledgement_batches.as_slice()
            ) == (0, &[ack][..])
        );
        let empty = topic
            .partitions
            .iter()
            .find(|part| part.partition_index == 3)
            .unwrap();
        assert2::assert!(empty.acknowledgement_batches.is_empty());
    }

    #[test]
    fn share_ack_request_preserves_identity_epoch_renew_flag_and_topics() {
        let topics = build_ack_topics(vec![(id(7), 2, 10, 12, ShareAckType::Reject.wire())]);

        let req =
            build_share_ack_request("group-a".into(), "member-a".into(), 5, true, topics.clone());

        assert2::assert!(
            req == ShareAcknowledgeRequest {
                group_id: Some("group-a".into()),
                member_id: Some("member-a".into()),
                share_session_epoch: 5,
                is_renew_ack: true,
                topics,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }
        );
    }

    #[test]
    fn response_and_record_helpers_preserve_boundaries() {
        for (name, code, expected) in [("success", 0, false), ("error", 17, true)] {
            check!(response_has_error(code) == expected, "case {name}");
        }
        for (name, first, last, expected) in [
            ("inclusive range", 10, 12, 3),
            ("reversed range", 12, 10, 0),
        ] {
            check!(range_len(first, last) == expected, "case {name}");
        }
        for (name, first, offset, last, expected) in [
            ("lower bound", 10, 10, 12, true),
            ("upper bound", 10, 12, 12, true),
            ("below range", 10, 9, 12, false),
            ("above range", 10, 13, 12, false),
        ] {
            check!(
                offset_in_range(first, offset, last) == expected,
                "case {name}"
            );
        }
        check!(record_offset(100, 7) == 107);
        check!(record_timestamp(1000, 33) == 1033);
    }

    #[tokio::test]
    async fn acknowledge_rejects_implicit_mode_and_stages_explicit_record() {
        let record = ShareConsumerRecord {
            topic: "topic-a".into(),
            partition: 2,
            offset: 10,
            timestamp: 0,
            key: None,
            value: None,
            delivery_count: 1,
        };

        let mut implicit = test_consumer(ShareAckMode::Implicit).await;
        assert2::assert!(
            implicit
                .acknowledge(&record, ShareAckType::Accept)
                .unwrap_err()
                .to_string()
                .contains("implicit ack mode")
        );

        let mut explicit = test_consumer(ShareAckMode::Explicit).await;
        explicit
            .acknowledge(&record, ShareAckType::Release)
            .unwrap();
        assert2::assert!(
            explicit.pending_acks == vec![(id(7), 2, 10, 10, ShareAckType::Release.wire())]
        );
    }

    #[tokio::test]
    async fn renew_rejects_implicit_mode_before_sending() {
        let record = ShareConsumerRecord {
            topic: "topic-a".into(),
            partition: 2,
            offset: 10,
            timestamp: 0,
            key: None,
            value: None,
            delivery_count: 1,
        };
        let mut consumer = test_consumer(ShareAckMode::Implicit).await;

        assert2::assert!(
            consumer
                .renew(&record)
                .await
                .unwrap_err()
                .to_string()
                .contains("implicit ack mode")
        );
    }

    #[tokio::test]
    async fn take_piggyback_acks_drains_implicit_deliveries_as_accept_ranges() {
        let mut consumer = test_consumer(ShareAckMode::Implicit).await;
        consumer.prev_delivered = vec![(id(7), 2, 10, 12)];

        let acks = consumer.take_piggyback_acks();

        assert2::assert!(consumer.prev_delivered.is_empty());
        let batch = only(acks.get(&(id(7), 2)).unwrap());
        assert2::assert!(
            *batch
                == FetchAckBatch {
                    first_offset: 10,
                    last_offset: 12,
                    acknowledge_types: vec![1, 1, 1],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );
    }

    #[tokio::test]
    async fn take_piggyback_acks_drains_explicit_pending_and_clears_stale_deliveries() {
        let mut consumer = test_consumer(ShareAckMode::Explicit).await;
        consumer.prev_delivered = vec![(id(7), 2, 1, 1)];
        consumer.pending_acks = vec![(id(7), 2, 10, 11, ShareAckType::Reject.wire())];

        let acks = consumer.take_piggyback_acks();

        assert2::assert!(
            (
                consumer.prev_delivered.is_empty(),
                consumer.pending_acks.is_empty()
            ) == (true, true)
        );
        let batch = only(acks.get(&(id(7), 2)).unwrap());
        assert2::assert!(
            *batch
                == FetchAckBatch {
                    first_offset: 10,
                    last_offset: 11,
                    acknowledge_types: vec![3, 3],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );
    }

    #[tokio::test]
    async fn topic_id_for_prefers_assignment_then_names_and_defaults_unknown() {
        let consumer = test_consumer(ShareAckMode::Explicit).await;
        consumer
            .topic_names
            .lock()
            .await
            .insert(id(8), "topic-b".into());

        for (name, expected) in [
            ("topic-a", id(7)),
            ("topic-b", id(8)),
            ("missing", WireUuid::default()),
        ] {
            assert2::assert!(consumer.topic_id_for(name) == expected);
        }
    }

    #[test]
    fn build_ack_topics_groups_offsets_by_topic_and_partition() {
        let topics = build_ack_topics(vec![
            (id(7), 2, 10, 12, ShareAckType::Accept.wire()),
            (id(7), 3, 20, 20, ShareAckType::Release.wire()),
            (id(8), 1, 30, 31, ShareAckType::Reject.wire()),
        ]);

        let topic = topics.iter().find(|topic| topic.topic_id == id(7)).unwrap();
        assert2::assert!(topic.partitions.len() == 2);
        let part = topic
            .partitions
            .iter()
            .find(|part| part.partition_index == 2)
            .unwrap();
        let batch = only(&part.acknowledgement_batches);
        assert2::assert!(
            *batch
                == AckAckBatch {
                    first_offset: 10,
                    last_offset: 12,
                    acknowledge_types: vec![1, 1, 1],
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );

        let renew = build_ack_topics(vec![(id(7), 2, 10, 10, 0)]);
        let renew_batch = only(&only(&only(&renew).partitions).acknowledgement_batches);
        assert2::assert!(
            *renew_batch
                == AckAckBatch {
                    first_offset: 10,
                    last_offset: 10,
                    acknowledge_types: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }
        );
    }
}
