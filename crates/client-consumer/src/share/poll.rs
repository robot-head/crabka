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

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

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
    records::RecordHeader,
};

use super::{
    consumer::ShareConsumer,
    types::{ShareAckMode, ShareAckType, ShareConsumerRecord},
};
use crate::{Header, error::ConsumerError};

/// `partition_max_bytes` / `max_bytes` budget for a `ShareFetch` (mirrors the
/// classic consumer's 50 MiB fetch budget).
const MAX_BYTES: i32 = 52_428_800;
/// Per-partition byte budget.
const PARTITION_MAX_BYTES: i32 = 1_048_576;
/// Cap on records returned per fetch.
const MAX_RECORDS: i32 = 500;

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
                        partition_max_bytes: PARTITION_MAX_BYTES,
                        acknowledgement_batches,
                        ..Default::default()
                    },
                )
                .collect(),
            ..Default::default()
        })
        .collect()
}

fn build_share_fetch_request(
    group_id: String,
    member_id: String,
    share_session_epoch: i32,
    timeout: Duration,
    topics: Vec<FetchTopic>,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group_id),
        member_id: Some(member_id),
        share_session_epoch,
        max_wait_ms: i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX),
        min_bytes: 1,
        max_bytes: MAX_BYTES,
        max_records: MAX_RECORDS,
        batch_size: MAX_RECORDS,
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

fn record_headers(headers: &[RecordHeader]) -> Vec<Header> {
    headers
        .iter()
        .map(|header| Header {
            key: header.key.clone(),
            value: header.value.clone(),
        })
        .collect()
}

fn failed_share_fetch_ack_partitions(
    responses: &[crabka_protocol::owned::share_fetch_response::ShareFetchableTopicResponse],
) -> HashSet<(WireUuid, i32)> {
    responses
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().filter_map(|partition| {
                response_has_error(partition.acknowledge_error_code)
                    .then_some((topic.topic_id, partition.partition_index))
            })
        })
        .collect()
}

fn failed_share_ack_partitions(
    responses: &[crabka_protocol::owned::share_acknowledge_response::ShareAcknowledgeTopicResponse],
) -> HashSet<(WireUuid, i32)> {
    responses
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().filter_map(|partition| {
                response_has_error(partition.error_code)
                    .then_some((topic.topic_id, partition.partition_index))
            })
        })
        .collect()
}

fn first_share_ack_partition_error(
    responses: &[crabka_protocol::owned::share_acknowledge_response::ShareAcknowledgeTopicResponse],
) -> Option<i16> {
    responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|partition| partition.error_code)
        .find(|error_code| response_has_error(*error_code))
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
    #[allow(clippy::too_many_lines)]
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
        // from the previous poll without clearing the source yet. The staged
        // state is consumed only after the broker accepts the ShareFetch, so a
        // timeout or top-level fetch error can be retried with the same acks.
        let acks = self.build_piggyback_acks();

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
                topics,
            ))
            .await?;

        if response_has_error(resp.error_code) {
            return Err(ConsumerError::Server(resp.error_code));
        }
        let failed_ack_partitions = failed_share_fetch_ack_partitions(&resp.responses);
        self.clear_piggyback_acks_after_success(&acks, &failed_ack_partitions);
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
                            headers: record_headers(&r.headers),
                            delivery_count,
                        });
                    }
                }
            }
        }

        // Implicit mode auto-Accepts these ranges on the next poll/close; explicit
        // mode acknowledges per record, so the ranges are not auto-accepted.
        match self.ack_mode {
            ShareAckMode::Implicit => self.prev_delivered.extend(delivered),
            ShareAckMode::Explicit => self.prev_delivered = delivered,
        }
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
        let pending_ack = (
            topic_id,
            record.partition,
            record.offset,
            record.offset,
            ack.wire(),
        );
        if self.pending_acks.contains(&pending_ack) {
            return Ok(());
        }

        self.pending_acks.push(pending_ack);
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
        if let Some(error_code) = first_share_ack_partition_error(&resp.responses) {
            return Err(ConsumerError::Server(error_code));
        }
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
    pub async fn commit(&mut self) -> Result<(), ConsumerError> {
        self.flush_pending_acks().await
    }

    /// Send `pending_acks` in a `ShareAcknowledge`. Advances the session epoch
    /// and clears staged acks only on success (an accepted `ShareAcknowledge`
    /// consumes one epoch, exactly like a `ShareFetch`). No-op (and no epoch
    /// advance) when nothing is staged.
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
        let topics = build_ack_topics(self.pending_acks.iter().copied());

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
        let failed_ack_partitions = failed_share_ack_partitions(&resp.responses);
        self.clear_pending_acks_after_ack_response(&failed_ack_partitions);
        self.share_session_epoch = self.share_session_epoch.wrapping_add(1);
        if let Some(error_code) = first_share_ack_partition_error(&resp.responses) {
            return Err(ConsumerError::Server(error_code));
        }
        Ok(())
    }

    /// Build the piggyback acknowledgement batches for the next `ShareFetch`,
    /// keyed by `(topic_id, partition)`, keeping the source state retryable until
    /// the `ShareFetch` succeeds.
    ///
    /// - Implicit: one `Accept` batch per previously-delivered range.
    /// - Explicit: the drained `pending_acks`, grouped into per-offset batches.
    fn build_piggyback_acks(&self) -> HashMap<(WireUuid, i32), Vec<FetchAckBatch>> {
        let mut out: HashMap<(WireUuid, i32), Vec<FetchAckBatch>> = HashMap::new();
        match self.ack_mode {
            ShareAckMode::Implicit => {
                for &(tid, partition, first, last) in &self.prev_delivered {
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
                for &(tid, partition, first, last, ack) in &self.pending_acks {
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
            }
        }
        out
    }

    /// Clear the state represented by a successful piggyback `ShareFetch`.
    fn clear_piggyback_acks_after_success(
        &mut self,
        sent_acks: &HashMap<(WireUuid, i32), Vec<FetchAckBatch>>,
        failed_ack_partitions: &HashSet<(WireUuid, i32)>,
    ) {
        let was_ack_accepted = |tid: &WireUuid, partition: &i32| {
            sent_acks.contains_key(&(*tid, *partition))
                && !failed_ack_partitions.contains(&(*tid, *partition))
        };
        match self.ack_mode {
            ShareAckMode::Implicit => self
                .prev_delivered
                .retain(|(tid, partition, _, _)| !was_ack_accepted(tid, partition)),
            ShareAckMode::Explicit => {
                self.pending_acks
                    .retain(|(tid, partition, _, _, _)| !was_ack_accepted(tid, partition));
                // Explicit mode never auto-accepts; clear any stale ranges.
                self.prev_delivered.clear();
            }
        }
    }

    fn clear_pending_acks_after_ack_response(
        &mut self,
        failed_ack_partitions: &HashSet<(WireUuid, i32)>,
    ) {
        self.pending_acks.retain(|(tid, partition, _, _, _)| {
            failed_ack_partitions.contains(&(*tid, *partition))
        });
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
fn build_ack_topics(
    acks: impl IntoIterator<Item = (WireUuid, i32, i64, i64, i8)>,
) -> Vec<AcknowledgeTopic> {
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

    use assert2::{assert, check};
    use bytes::BytesMut;
    use crabka_client_core::{Client, ClientError, MockBroker};
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            share_acknowledge_request, share_acknowledge_response,
            share_acknowledge_response::ShareAcknowledgeResponse,
            share_fetch_request, share_fetch_response,
            share_fetch_response::ShareFetchResponse,
        },
        tagged_fields::UnknownTaggedFields,
    };
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn id(n: u8) -> WireUuid {
        let mut b = [0u8; 16];
        b[15] = n;
        WireUuid(b)
    }

    async fn test_consumer(ack_mode: ShareAckMode) -> ShareConsumer {
        let client = Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id("share-poll-test")
            .build()
            .await
            .unwrap();

        test_consumer_with_client(ack_mode, client)
    }

    fn test_consumer_with_client(ack_mode: ShareAckMode, client: Client) -> ShareConsumer {
        ShareConsumer {
            client,
            group_id: "group-a".into(),
            member_id: "member-a".into(),
            member_epoch: Arc::new(Mutex::new(3)),
            assignment: Arc::new(Mutex::new(vec![(id(7), "topic-a".into(), 2)])),
            topic_names: Arc::new(Mutex::new(HashMap::new())),
            share_session_epoch: 4,
            ack_mode,
            pending_acks: Vec::new(),
            prev_delivered: Vec::new(),
            shutdown: CancellationToken::new(),
            hb_handle: None,
        }
    }

    fn api_versions_for_share_data_path() -> Vec<u8> {
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
                    api_key: share_acknowledge_request::API_KEY,
                    min_version: share_acknowledge_request::MIN_VERSION,
                    max_version: share_acknowledge_request::MAX_VERSION,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: share_fetch_request::API_KEY,
                    min_version: share_fetch_request::MIN_VERSION,
                    max_version: share_fetch_request::MAX_VERSION,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    async fn mock_client(addr: std::net::SocketAddr) -> Client {
        Client::builder()
            .bootstrap(addr.to_string())
            .client_id("share-poll-test")
            .request_timeout(Duration::from_millis(50))
            .build()
            .await
            .unwrap()
    }

    fn share_ack_response_at(version: i16, error_code: i16) -> Vec<u8> {
        let resp = ShareAcknowledgeResponse {
            error_code,
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= share_acknowledge_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00]);
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn share_ack_partition_response_at(
        version: i16,
        partition_errors: impl IntoIterator<Item = (i32, i16)>,
    ) -> Vec<u8> {
        let resp = ShareAcknowledgeResponse {
            error_code: 0,
            responses: vec![share_acknowledge_response::ShareAcknowledgeTopicResponse {
                topic_id: id(7),
                partitions: partition_errors
                    .into_iter()
                    .map(|(partition_index, error_code)| {
                        share_acknowledge_response::PartitionData {
                            partition_index,
                            error_code,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= share_acknowledge_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00]);
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn share_fetch_response_at(version: i16, error_code: i16) -> Vec<u8> {
        let resp = ShareFetchResponse {
            error_code,
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= share_fetch_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00]);
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn share_fetch_ack_partition_response_at(version: i16, acknowledge_error_code: i16) -> Vec<u8> {
        let resp = ShareFetchResponse {
            error_code: 0,
            responses: vec![share_fetch_response::ShareFetchableTopicResponse {
                topic_id: id(7),
                partitions: vec![share_fetch_response::PartitionData {
                    partition_index: 2,
                    acknowledge_error_code,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= share_fetch_response::FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00]);
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    fn only<T>(items: &[T]) -> &T {
        assert!(items.len() == 1);
        &items[0]
    }

    fn test_record() -> ShareConsumerRecord {
        ShareConsumerRecord {
            topic: "topic-a".into(),
            partition: 2,
            offset: 10,
            timestamp: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            delivery_count: 1,
        }
    }

    #[test]
    fn share_fetch_request_preserves_wire_fields_and_timeout_bounds() {
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
            vec![topic.clone()],
        );

        assert!(
            req == ShareFetchRequest {
                group_id: Some("group-a".into()),
                member_id: Some("member-a".into()),
                share_session_epoch: 4,
                max_wait_ms: 250,
                min_bytes: 1,
                max_bytes: 52_428_800,
                max_records: 500,
                batch_size: 500,
                share_acquire_mode: 0,
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
            Vec::new(),
        );
        assert!(saturated.max_wait_ms == i32::MAX);
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
        assert!(topic.partitions.len() == 2);
        let part = topic
            .partitions
            .iter()
            .find(|part| part.partition_index == 2)
            .unwrap();
        assert!(part.partition_max_bytes == PARTITION_MAX_BYTES);
        assert!(part.acknowledgement_batches == vec![ack]);
        let empty = topic
            .partitions
            .iter()
            .find(|part| part.partition_index == 3)
            .unwrap();
        assert!(empty.acknowledgement_batches.is_empty());
    }

    #[test]
    fn share_ack_request_preserves_identity_epoch_renew_flag_and_topics() {
        let topics = build_ack_topics(vec![(id(7), 2, 10, 12, ShareAckType::Reject.wire())]);

        let req =
            build_share_ack_request("group-a".into(), "member-a".into(), 5, true, topics.clone());

        assert!(
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
        check!(!response_has_error(0));
        check!(response_has_error(17));
        check!(range_len(10, 12) == 3);
        check!(range_len(12, 10) == 0);
        check!(offset_in_range(10, 10, 12));
        check!(offset_in_range(10, 12, 12));
        check!(!offset_in_range(10, 9, 12));
        check!(!offset_in_range(10, 13, 12));
        check!(record_offset(100, 7) == 107);
        check!(record_timestamp(1000, 33) == 1033);
    }

    #[test]
    fn share_record_headers_preserve_order_and_null_values() {
        let headers = record_headers(&[
            RecordHeader {
                key: "trace".into(),
                value: Some(bytes::Bytes::from_static(b"abc")),
            },
            RecordHeader {
                key: "empty".into(),
                value: Some(bytes::Bytes::new()),
            },
            RecordHeader {
                key: "null".into(),
                value: None,
            },
        ]);

        assert!(
            headers
                == vec![
                    Header {
                        key: "trace".into(),
                        value: Some(bytes::Bytes::from_static(b"abc")),
                    },
                    Header {
                        key: "empty".into(),
                        value: Some(bytes::Bytes::new()),
                    },
                    Header {
                        key: "null".into(),
                        value: None,
                    },
                ]
        );
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
            headers: Vec::new(),
            delivery_count: 1,
        };

        let mut implicit = test_consumer(ShareAckMode::Implicit).await;
        assert!(
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
        assert!(explicit.pending_acks == vec![(id(7), 2, 10, 10, ShareAckType::Release.wire())]);
    }

    #[tokio::test]
    async fn acknowledge_is_idempotent_for_same_record_and_ack_type() {
        let record = test_record();
        let mut consumer = test_consumer(ShareAckMode::Explicit).await;

        consumer.acknowledge(&record, ShareAckType::Accept).unwrap();
        consumer.acknowledge(&record, ShareAckType::Accept).unwrap();

        assert!(consumer.pending_acks == vec![(id(7), 2, 10, 10, ShareAckType::Accept.wire())]);
    }

    #[tokio::test]
    async fn acknowledge_preserves_conflicting_ack_types_for_broker_validation() {
        let record = test_record();
        let mut consumer = test_consumer(ShareAckMode::Explicit).await;

        consumer.acknowledge(&record, ShareAckType::Accept).unwrap();
        consumer.acknowledge(&record, ShareAckType::Reject).unwrap();

        assert!(
            consumer.pending_acks
                == vec![
                    (id(7), 2, 10, 10, ShareAckType::Accept.wire()),
                    (id(7), 2, 10, 10, ShareAckType::Reject.wire()),
                ]
        );
    }

    #[tokio::test]
    async fn renew_rejects_implicit_mode_before_sending() {
        let record = test_record();
        let mut consumer = test_consumer(ShareAckMode::Implicit).await;

        assert!(
            consumer
                .renew(&record)
                .await
                .unwrap_err()
                .to_string()
                .contains("implicit ack mode")
        );
    }

    #[tokio::test]
    async fn renew_returns_top_level_error_without_advancing_epoch() {
        let mock = MockBroker::start(|api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_acknowledge_request::API_KEY {
                return None;
            }
            Some(share_ack_response_at(version, 91))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);

        let err = consumer.renew(&test_record()).await.unwrap_err();

        mock.stop();
        assert!(matches!(err, ConsumerError::Server(91)));
        assert!(consumer.share_session_epoch == 4);
    }

    #[tokio::test]
    async fn renew_returns_partition_error_after_advancing_epoch() {
        let mock = MockBroker::start(|api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_acknowledge_request::API_KEY {
                return None;
            }
            Some(share_ack_partition_response_at(version, [(2, 42)]))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);

        let err = consumer.renew(&test_record()).await.unwrap_err();

        mock.stop();
        assert!(matches!(err, ConsumerError::Server(42)));
        assert!(consumer.share_session_epoch == 5);
    }

    #[tokio::test]
    async fn renew_advances_epoch_on_success() {
        let mock = MockBroker::start(|api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_acknowledge_request::API_KEY {
                return None;
            }
            Some(share_ack_partition_response_at(version, [(2, 0)]))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);

        consumer.renew(&test_record()).await.unwrap();

        mock.stop();
        assert!(consumer.share_session_epoch == 5);
    }

    #[tokio::test]
    async fn build_piggyback_acks_keeps_implicit_deliveries_retryable() {
        let mut consumer = test_consumer(ShareAckMode::Implicit).await;
        consumer.prev_delivered = vec![(id(7), 2, 10, 12)];

        let acks = consumer.build_piggyback_acks();

        assert!(consumer.prev_delivered == vec![(id(7), 2, 10, 12)]);
        let batch = only(acks.get(&(id(7), 2)).unwrap());
        assert!(
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
    async fn build_piggyback_acks_keeps_explicit_pending_and_stale_deliveries_retryable() {
        let mut consumer = test_consumer(ShareAckMode::Explicit).await;
        consumer.prev_delivered = vec![(id(7), 2, 1, 1)];
        consumer.pending_acks = vec![(id(7), 2, 10, 11, ShareAckType::Reject.wire())];

        let acks = consumer.build_piggyback_acks();

        assert!(consumer.prev_delivered == vec![(id(7), 2, 1, 1)]);
        assert!(consumer.pending_acks == vec![(id(7), 2, 10, 11, ShareAckType::Reject.wire())]);
        let batch = only(acks.get(&(id(7), 2)).unwrap());
        assert!(
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
    async fn clear_piggyback_acks_after_success_consumes_only_successful_state() {
        let mut explicit = test_consumer(ShareAckMode::Explicit).await;
        explicit.prev_delivered = vec![(id(7), 2, 1, 1)];
        explicit.pending_acks = vec![(id(7), 2, 10, 10, ShareAckType::Accept.wire())];
        let sent_acks = explicit.build_piggyback_acks();
        let failed_ack_partitions = HashSet::new();

        explicit.clear_piggyback_acks_after_success(&sent_acks, &failed_ack_partitions);

        assert!(explicit.prev_delivered.is_empty());
        assert!(explicit.pending_acks.is_empty());

        let mut implicit = test_consumer(ShareAckMode::Implicit).await;
        implicit.prev_delivered = vec![(id(7), 2, 20, 21)];
        let sent_acks = implicit.build_piggyback_acks();
        let failed_ack_partitions = HashSet::new();

        implicit.clear_piggyback_acks_after_success(&sent_acks, &failed_ack_partitions);

        assert!(implicit.prev_delivered.is_empty());
    }

    #[tokio::test]
    async fn clear_piggyback_acks_after_success_preserves_failed_partitions() {
        let mut consumer = test_consumer(ShareAckMode::Explicit).await;
        consumer.pending_acks = vec![
            (id(7), 2, 10, 10, ShareAckType::Accept.wire()),
            (id(7), 3, 20, 20, ShareAckType::Accept.wire()),
        ];
        let sent_acks = consumer.build_piggyback_acks();
        let failed_ack_partitions = HashSet::from([(id(7), 2)]);

        consumer.clear_piggyback_acks_after_success(&sent_acks, &failed_ack_partitions);

        assert!(consumer.pending_acks == vec![(id(7), 2, 10, 10, ShareAckType::Accept.wire())]);

        let mut implicit = test_consumer(ShareAckMode::Implicit).await;
        implicit.prev_delivered = vec![(id(7), 2, 30, 30), (id(7), 3, 40, 40)];
        let sent_acks = implicit.build_piggyback_acks();
        let failed_ack_partitions = HashSet::from([(id(7), 3)]);

        implicit.clear_piggyback_acks_after_success(&sent_acks, &failed_ack_partitions);

        assert!(implicit.prev_delivered == vec![(id(7), 3, 40, 40)]);
    }

    #[tokio::test]
    async fn flush_pending_acks_preserves_pending_acks_on_transport_failure() {
        let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_for_share_data_path())
            } else {
                None
            }
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);
        consumer.pending_acks = vec![(id(7), 2, 10, 10, ShareAckType::Accept.wire())];
        let expected_pending = consumer.pending_acks.clone();

        let err = consumer.flush_pending_acks().await.unwrap_err();

        mock.stop();
        assert!(matches!(
            err,
            ConsumerError::Client(ClientError::Timeout(_))
        ));
        assert!(consumer.pending_acks == expected_pending);
        assert!(consumer.share_session_epoch == 4);
    }

    #[tokio::test]
    async fn flush_pending_acks_preserves_broker_failed_acks_for_retry() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_in_mock = Arc::clone(&attempts);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_acknowledge_request::API_KEY {
                return None;
            }
            let attempt = attempts_in_mock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let error_code = if attempt == 0 { 42 } else { 0 };
            Some(share_ack_response_at(version, error_code))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);
        consumer.pending_acks = vec![(id(7), 2, 10, 10, ShareAckType::Reject.wire())];
        let expected_pending = consumer.pending_acks.clone();

        let first_err = consumer.flush_pending_acks().await.unwrap_err();
        assert!(matches!(first_err, ConsumerError::Server(42)));
        assert!(consumer.pending_acks == expected_pending);
        assert!(consumer.share_session_epoch == 4);

        consumer.flush_pending_acks().await.unwrap();

        mock.stop();
        assert!(consumer.pending_acks.is_empty());
        assert!(consumer.share_session_epoch == 5);
        assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn flush_pending_acks_preserves_partition_failed_acks_for_retry() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_in_mock = Arc::clone(&attempts);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_acknowledge_request::API_KEY {
                return None;
            }
            let attempt = attempts_in_mock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let error_code = if attempt == 0 { 42 } else { 0 };
            Some(share_ack_partition_response_at(version, [(2, error_code)]))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);
        consumer.pending_acks = vec![(id(7), 2, 10, 10, ShareAckType::Reject.wire())];
        let expected_pending = consumer.pending_acks.clone();

        let first_err = consumer.flush_pending_acks().await.unwrap_err();
        assert!(matches!(first_err, ConsumerError::Server(42)));
        assert!(consumer.pending_acks == expected_pending);
        assert!(consumer.share_session_epoch == 5);

        consumer.flush_pending_acks().await.unwrap();

        mock.stop();
        assert!(consumer.pending_acks.is_empty());
        assert!(consumer.share_session_epoch == 6);
        assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn flush_pending_acks_clears_successful_partitions_in_mixed_response() {
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_acknowledge_request::API_KEY {
                return None;
            }
            Some(share_ack_partition_response_at(version, [(2, 42), (3, 0)]))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);
        consumer.pending_acks = vec![
            (id(7), 2, 10, 10, ShareAckType::Reject.wire()),
            (id(7), 3, 20, 20, ShareAckType::Accept.wire()),
        ];

        let err = consumer.flush_pending_acks().await.unwrap_err();

        mock.stop();
        assert!(matches!(err, ConsumerError::Server(42)));
        assert!(consumer.pending_acks == vec![(id(7), 2, 10, 10, ShareAckType::Reject.wire())]);
        assert!(consumer.share_session_epoch == 5);
    }

    #[tokio::test]
    async fn flush_pending_acks_does_not_resend_after_success() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_in_mock = Arc::clone(&attempts);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_acknowledge_request::API_KEY {
                return None;
            }
            attempts_in_mock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Some(share_ack_response_at(version, 0))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);
        consumer.pending_acks = vec![(id(7), 2, 10, 10, ShareAckType::Accept.wire())];

        consumer.flush_pending_acks().await.unwrap();
        consumer.flush_pending_acks().await.unwrap();

        mock.stop();
        assert!(consumer.pending_acks.is_empty());
        assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn poll_preserves_explicit_piggyback_state_on_fetch_broker_error() {
        let mock = MockBroker::start(|api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_fetch_request::API_KEY {
                return None;
            }
            Some(share_fetch_response_at(version, 91))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);
        consumer.pending_acks = vec![(id(7), 2, 10, 10, ShareAckType::Release.wire())];
        consumer.prev_delivered = vec![(id(7), 2, 1, 1)];
        let expected_pending = consumer.pending_acks.clone();
        let expected_delivered = consumer.prev_delivered.clone();

        let err = consumer.poll(Duration::from_millis(1)).await.unwrap_err();

        mock.stop();
        assert!(matches!(err, ConsumerError::Server(91)));
        assert!(consumer.pending_acks == expected_pending);
        assert!(consumer.prev_delivered == expected_delivered);
        assert!(consumer.share_session_epoch == 4);
    }

    #[tokio::test]
    async fn poll_preserves_explicit_piggyback_state_on_partition_ack_error_for_retry() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_in_mock = Arc::clone(&attempts);
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_fetch_request::API_KEY {
                return None;
            }
            let attempt = attempts_in_mock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let acknowledge_error_code = if attempt == 0 { 42 } else { 0 };
            Some(share_fetch_ack_partition_response_at(
                version,
                acknowledge_error_code,
            ))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Explicit, client);
        consumer.pending_acks = vec![(id(7), 2, 10, 10, ShareAckType::Release.wire())];
        let expected_pending = consumer.pending_acks.clone();

        let records = consumer.poll(Duration::from_millis(1)).await.unwrap();
        assert!(records.is_empty());
        assert!(consumer.pending_acks == expected_pending);
        assert!(consumer.share_session_epoch == 5);

        let records = consumer.poll(Duration::from_millis(1)).await.unwrap();

        mock.stop();
        assert!(records.is_empty());
        assert!(consumer.pending_acks.is_empty());
        assert!(consumer.share_session_epoch == 6);
        assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn poll_preserves_implicit_piggyback_state_on_partition_ack_error() {
        let mock = MockBroker::start(move |api_key, version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_for_share_data_path());
            }
            if api_key != share_fetch_request::API_KEY {
                return None;
            }
            Some(share_fetch_ack_partition_response_at(version, 42))
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Implicit, client);
        consumer.prev_delivered = vec![(id(7), 2, 20, 21)];

        let records = consumer.poll(Duration::from_millis(1)).await.unwrap();

        mock.stop();
        assert!(records.is_empty());
        assert!(consumer.prev_delivered == vec![(id(7), 2, 20, 21)]);
        assert!(consumer.share_session_epoch == 5);
    }

    #[tokio::test]
    async fn poll_preserves_implicit_delivered_state_on_fetch_transport_failure() {
        let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_for_share_data_path())
            } else {
                None
            }
        })
        .await;
        let client = mock_client(mock.addr).await;
        let mut consumer = test_consumer_with_client(ShareAckMode::Implicit, client);
        consumer.prev_delivered = vec![(id(7), 2, 20, 21)];
        let expected_delivered = consumer.prev_delivered.clone();

        let err = consumer.poll(Duration::from_millis(1)).await.unwrap_err();

        mock.stop();
        assert!(matches!(
            err,
            ConsumerError::Client(ClientError::Timeout(_))
        ));
        assert!(consumer.prev_delivered == expected_delivered);
        assert!(consumer.share_session_epoch == 4);
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
            assert!(consumer.topic_id_for(name) == expected, "name: {name}");
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
        assert!(topic.partitions.len() == 2);
        let part = topic
            .partitions
            .iter()
            .find(|part| part.partition_index == 2)
            .unwrap();
        let batch = only(&part.acknowledgement_batches);
        assert!(
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
        assert!(
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
