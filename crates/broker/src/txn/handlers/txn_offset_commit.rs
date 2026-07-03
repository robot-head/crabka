//! `TxnOffsetCommit` (`api_key=28`). The consumer side of the
//! consume-process-produce pattern. A transactional producer that also
//! reads commits its consumed offsets atomically with its transaction by
//! appending them to `__consumer_offsets` with `is_transactional=true` +
//! the producer's (pid, epoch). The offsets are held under the partition's
//! LSO until a `WriteTxnMarkers` commit or abort marker arrives.
//!
//! Versions 0–2: non-flexible (no `generation_id`/`member_id` fields).
//! Versions 3–5: flexible (tagged fields; adds `generation_id`, `member_id`,
//!               `group_instance_id`). On v3+ the consumer-group metadata is
//!               validated via the shared `validate_group_commit` (KIP-447:
//!               fencing "consistent with normal offset fencing") — classic
//!               generation or KIP-848 next-gen member epoch.
//!
//! ## ACL preamble
//!
//! Three gates run in order:
//! * `Write` on `TransactionalId(transactional_id)`. Deny → whole-response
//!   `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`.
//! * `Read` on `Group(group_id)`. Deny → whole-response
//!   `GROUP_AUTHORIZATION_FAILED (30)`.
//! * Per-topic `Read` on `Topic(name)`. Deny → per-partition
//!   `TOPIC_AUTHORIZATION_FAILED (29)` on the rows of that topic.

use bytes::{Bytes, BytesMut};
use crabka_ids::PartitionIndex;
use crabka_log::Offset;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        txn_offset_commit_request::TxnOffsetCommitRequest,
        txn_offset_commit_response::{
            TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        },
    },
    records::{Attributes, Record, RecordBatch},
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::{
        bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC},
        persistence::OffsetCommitValue,
        unified::{
            actor::{GroupKindTag, validate_group_commit},
            classic_state::OffsetEntry,
            streams::actor::validate_streams_group_commit,
        },
    },
    error::BrokerError,
    txn::{coordinator::OffsetKey, util::now_millis},
};

#[tracing::instrument(
    name = "handle_txn_offset_commit",
    level = "info",
    skip_all,
    fields(api = "TxnOffsetCommit", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let partitions = broker.partitions.clone();
    let mut cur: &[u8] = req_bytes;
    let req = TxnOffsetCommitRequest::decode(&mut cur, version)?;

    // ── ACL preamble: Write on TransactionalId ────────────────
    {
        let image = broker.controller.current_image();
        let authorizer = broker.config.authorizer.as_ref();
        let tid_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: req.transactional_id.as_str(),
            operation: AclOperation::Write,
        };
        if authorizer.authorize(&*image, &tid_req) == AuthorizationResult::Deny {
            return encode_err_all(version, &req, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        }
        // Group Read gate.
        let group_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: req.group_id.as_str(),
            operation: AclOperation::Read,
        };
        if authorizer.authorize(&*image, &group_req) == AuthorizationResult::Deny {
            return encode_err_all(version, &req, codes::GROUP_AUTHORIZATION_FAILED);
        }
    }

    // ── ACL preamble: per-topic Read ──────────────────────────
    let topic_decisions = {
        let image = broker.controller.current_image();
        let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
        authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names,
        )
    };
    let denied_topics: std::collections::HashSet<String> = topic_decisions
        .into_iter()
        .filter_map(|(name, r)| {
            if r == AuthorizationResult::Deny {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect();

    // 1. Verify the group coordinator is this broker.  In the current
    //    single-broker MVP every group is local. We check that the group
    //    exists (or create it) — if the partition for __consumer_offsets
    //    is not present we'll detect that below and return NOT_COORDINATOR.
    //    For a multi-broker future this would route to the leader for
    //    hash(group_id) % __consumer_offsets.partition_count.
    let handle = broker
        .group_coordinator
        .find(&req.group_id)
        .unwrap_or_else(|| {
            broker
                .group_coordinator
                .get_or_create_group(&req.group_id, GroupKindTag::Classic)
        });

    // 2. KIP-447 / KIP-1319 fencing — identical to a regular OffsetCommit
    //    (KIP-447: "consistent with normal offset fencing"). For a classic
    //    group this checks member id + group.instance.id + generation
    //    (ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID / FENCED_INSTANCE_ID); for a
    //    KIP-848 next-gen group the `generation_id` field carries the member
    //    epoch and we return STALE_MEMBER_EPOCH / FENCED_MEMBER_EPOCH /
    //    UNKNOWN_MEMBER_ID. A producer that supplies no metadata (empty
    //    member_id, generation_id = -1) is a simple consumer and is not fenced.
    //    The fields only exist on v3+, so older requests carry the
    //    simple-consumer defaults and no-op. `validate_group_commit` dispatches
    //    on the actor's LIVE `group.kind`, so a KIP-848-flipped group is fenced
    //    against its current protocol, not the stale spawn-time `handle.kind`.
    // KIP-1071: a streams-group consumer's membership lives in the STREAMS
    // group actor, not the classic one. Route its fencing there (member_epoch
    // check) — `validate_group_commit` only knows the classic/consumer actor,
    // so validating a streams member against the freshly-created empty classic
    // actor would wrongly reject every EOS offset commit with UNKNOWN_MEMBER_ID.
    if version >= 3 {
        let code = if let Some(streams) = broker.group_coordinator.find_streams(&req.group_id) {
            validate_streams_group_commit(&streams, &req.member_id, req.generation_id).await
        } else {
            validate_group_commit(
                &handle,
                &req.member_id,
                req.generation_id,
                req.group_instance_id.as_deref(),
            )
            .await
        };
        if let Some(code) = code {
            return encode_err_all(version, &req, code);
        }
    }

    // 3. Append a transactional RecordBatch to __consumer_offsets.
    //    We reuse the OffsetCommitKey/Value layout but stamp the batch with
    //    is_transactional=true + (producer_id, producer_epoch) so the log's
    //    LSO machinery holds the offsets until EndTxn commits/aborts.
    //    Topics denied by the per-topic Read ACL are skipped from the
    //    batch and surfaced as TOPIC_AUTHORIZATION_FAILED in the response.
    let now_ms = now_millis();
    let buffered = match append_txn_batch(&req, &partitions, now_ms, &denied_topics).await {
        Ok(entries) => entries,
        Err(code) => {
            return encode_resp(version, &build_response(&req, code, &denied_topics));
        }
    };

    // 3b. Buffer the committed offsets on the txn coordinator, keyed by the
    //     producer_id, pending the transaction's COMMIT/ABORT marker. They are
    //     NOT yet visible to OffsetFetch (Kafka: a transactional offset becomes
    //     visible only after the commit marker). `EndTxn` materializes the
    //     buffer into the group's `committed_offsets` on COMMIT, or drops it on
    //     ABORT. The records are already in `__consumer_offsets` (held under the
    //     LSO) from the append above; this buffer is the in-memory bridge to the
    //     group coordinator that the commit marker has no other way to drive.
    broker.txn_coordinator.buffer_txn_offsets(
        crabka_log::ProducerId(req.producer_id),
        &req.group_id,
        buffered,
    );

    // 4. Success — per-(topic, partition) error_code = NONE for allowed,
    //    TOPIC_AUTHORIZATION_FAILED for denied.
    encode_resp(version, &build_response(&req, codes::NONE, &denied_topics))
}

// ── batch construction ────────────────────────────────────────────────────────

/// Append the transactional offset records to `__consumer_offsets` and return
/// the same `(topic, partition) → OffsetEntry` rows so the caller can buffer
/// them on the txn coordinator (to be materialized into the group's committed
/// offsets when the COMMIT marker arrives). The returned vec is empty when
/// every topic was denied (nothing was appended).
async fn append_txn_batch(
    req: &TxnOffsetCommitRequest,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    now_ms: i64,
    denied_topics: &std::collections::HashSet<String>,
) -> Result<Vec<(OffsetKey, OffsetEntry)>, i16> {
    let mut batch = RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        max_timestamp: now_ms,
        producer_id: req.producer_id,
        producer_epoch: req.producer_epoch,
        ..RecordBatch::default()
    };
    let mut entries: Vec<(OffsetKey, OffsetEntry)> = Vec::new();
    let mut delta: i32 = 0;
    for topic in &req.topics {
        if denied_topics.contains(&topic.name) {
            continue;
        }
        for part in &topic.partitions {
            let value = OffsetCommitValue {
                offset: Offset(part.committed_offset),
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: now_ms,
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(OffsetCommitValue::encode_key(
                    &req.group_id,
                    &topic.name,
                    part.partition_index,
                )),
                value: Some(value.encode_value()),
                ..Default::default()
            });
            entries.push((
                (topic.name.clone(), part.partition_index),
                OffsetEntry {
                    offset: value.offset,
                    leader_epoch: value.leader_epoch,
                    metadata: value.metadata.clone(),
                    commit_timestamp_ms: value.commit_timestamp_ms,
                },
            ));
            delta += 1;
        }
    }

    // If every topic was denied, there's nothing to append; succeed silently.
    if batch.records.is_empty() {
        return Ok(entries);
    }

    batch.last_offset_delta = (delta - 1).max(0);

    let Some(part_handle) = partitions.get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION)) else {
        // __consumer_offsets not hosted here — report NOT_COORDINATOR.
        return Err(codes::NOT_COORDINATOR);
    };
    // `produce_batch` drives the single-writer task and returns the
    // assigned base_offset; we don't need it here.
    part_handle
        .produce_batch(batch)
        .await
        .map(|_| entries)
        .map_err(|e| {
            tracing::error!(
                group = %req.group_id,
                tid   = %req.transactional_id,
                error = %e,
                "TxnOffsetCommit: produce_batch failed"
            );
            codes::UNKNOWN_SERVER_ERROR
        })
}

// ── response helpers ──────────────────────────────────────────────────────────

fn build_response(
    req: &TxnOffsetCommitRequest,
    code: i16,
    denied_topics: &std::collections::HashSet<String>,
) -> TxnOffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| {
            let row_code = if denied_topics.contains(&t.name) {
                codes::TOPIC_AUTHORIZATION_FAILED
            } else {
                code
            };
            TxnOffsetCommitResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| TxnOffsetCommitResponsePartition {
                        partition_index: p.partition_index,
                        error_code: row_code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect();
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    }
}

fn encode_resp(version: i16, resp: &TxnOffsetCommitResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn encode_err_all(
    version: i16,
    req: &TxnOffsetCommitRequest,
    code: i16,
) -> Result<Bytes, BrokerError> {
    let empty: std::collections::HashSet<String> = std::collections::HashSet::new();
    encode_resp(version, &build_response(req, code, &empty))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path, sync::Arc};

    use assert2::{assert, check};
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::owned::{
        txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
        txn_offset_commit_response::TxnOffsetCommitResponse,
    };

    use super::*;
    use crate::partition_registry::PartitionRegistry;

    fn request() -> TxnOffsetCommitRequest {
        TxnOffsetCommitRequest {
            transactional_id: "tid".into(),
            group_id: "group-a".into(),
            producer_id: 47,
            producer_epoch: 5,
            topics: vec![TxnOffsetCommitRequestTopic {
                name: "orders".into(),
                partitions: vec![
                    TxnOffsetCommitRequestPartition {
                        partition_index: 2,
                        committed_offset: 103,
                        committed_leader_epoch: 7,
                        committed_metadata: Some("first".into()),
                        ..Default::default()
                    },
                    TxnOffsetCommitRequestPartition {
                        partition_index: 3,
                        committed_offset: 107,
                        committed_leader_epoch: 8,
                        committed_metadata: Some("second".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn open_offsets_partition(registry: &PartitionRegistry, log_dir: &Path) {
        let part_dir = crate::log_dir::partition_dir(log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        std::fs::create_dir_all(&part_dir).expect("create offsets partition dir");
        let log = Log::open(&part_dir, LogConfig::default()).expect("open offsets log");
        let part = crate::broker::spawn_partition(
            OFFSETS_TOPIC.to_string(),
            PartitionIndex(OFFSETS_PARTITION),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        );
        registry.insert(
            OFFSETS_TOPIC.to_string(),
            PartitionIndex(OFFSETS_PARTITION),
            part,
        );
    }

    fn decode_response(bytes: &Bytes, version: i16) -> TxnOffsetCommitResponse {
        crate::test_support::decode_response(bytes, version)
    }

    fn assert_response_rows(resp: &TxnOffsetCommitResponse, code: i16) {
        let expected = TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![TxnOffsetCommitResponseTopic {
                name: "orders".into(),
                partitions: vec![
                    TxnOffsetCommitResponsePartition {
                        partition_index: 2,
                        error_code: code,
                        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                    },
                    TxnOffsetCommitResponsePartition {
                        partition_index: 3,
                        error_code: code,
                        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                    },
                ],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(*resp == expected);
    }

    #[test]
    fn build_response_preserves_topic_partition_rows_and_error_codes() {
        let req = request();
        let resp = build_response(&req, codes::GROUP_AUTHORIZATION_FAILED, &HashSet::new());

        assert_response_rows(&resp, codes::GROUP_AUTHORIZATION_FAILED);
    }

    #[test]
    fn build_response_overrides_denied_topics_with_topic_authorization_error() {
        let req = request();
        let denied = HashSet::from(["orders".to_string()]);

        let resp = build_response(&req, codes::NONE, &denied);

        assert_response_rows(&resp, codes::TOPIC_AUTHORIZATION_FAILED);
    }

    #[test]
    fn encode_resp_round_trips_non_empty_response() {
        let req = request();
        let resp = build_response(&req, codes::INVALID_TXN_STATE, &HashSet::new());

        let bytes = encode_resp(5, &resp).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 5);

        assert_response_rows(&decoded, codes::INVALID_TXN_STATE);
    }

    #[test]
    fn encode_err_all_round_trips_rows_for_whole_request_error() {
        let req = request();

        let bytes = encode_err_all(5, &req, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
            .expect("encode all-error response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 5);

        assert_response_rows(&decoded, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }

    #[tokio::test]
    async fn append_txn_batch_writes_transactional_batch_and_buffers_entries() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        open_offsets_partition(&registry, dir.path());
        let req = request();

        let entries = append_txn_batch(&req, &registry, 12_345, &HashSet::new())
            .await
            .expect("append batch");

        // `OffsetEntry` has no `PartialEq`, so project each row into a tuple
        // covering the key plus every `OffsetEntry` field.
        let rows: Vec<(&str, i32, i64, i32, &str, i64)> = entries
            .iter()
            .map(|((topic, partition), e)| {
                (
                    topic.as_str(),
                    *partition,
                    e.offset.0,
                    e.leader_epoch,
                    e.metadata.as_str(),
                    e.commit_timestamp_ms,
                )
            })
            .collect();
        assert!(
            rows == vec![
                ("orders", 2, 103, 7, "first", 12_345),
                ("orders", 3, 107, 8, "second", 12_345),
            ]
        );

        let part = registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition");
        let log = part.log.lock().expect("lock offsets log");
        let read = log
            .read(crabka_log::Offset(0), 1024 * 1024)
            .expect("read offsets log");
        assert!(read.batches.len() == 1);
        let batch = &read.batches[0];
        check!(batch.attributes.is_transactional());
        check!(batch.max_timestamp == 12_345);
        check!(batch.producer_id == 47);
        check!(batch.producer_epoch == 5);
        check!(batch.last_offset_delta == 1);
        let record_rows: Vec<_> = batch
            .records
            .iter()
            .map(|r| {
                (
                    r.offset_delta,
                    r.timestamp_delta,
                    r.key.is_some(),
                    r.value.is_some(),
                )
            })
            .collect();
        assert!(record_rows == vec![(0, 0, true, true), (1, 0, true, true)]);
    }

    #[tokio::test]
    async fn append_txn_batch_skips_denied_topics_without_appending() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        open_offsets_partition(&registry, dir.path());
        let req = request();
        let denied = HashSet::from(["orders".to_string()]);

        let entries = append_txn_batch(&req, &registry, 12_345, &denied)
            .await
            .expect("all denied succeeds");

        assert!(entries.is_empty());
        let part = registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition");
        let log = part.log.lock().expect("lock offsets log");
        let read = log
            .read(crabka_log::Offset(0), 1024 * 1024)
            .expect("read offsets log");
        assert!(read.batches.is_empty());
    }

    #[tokio::test]
    async fn append_txn_batch_returns_not_coordinator_when_offsets_partition_missing() {
        let registry = Arc::new(PartitionRegistry::new());
        let err = append_txn_batch(&request(), &registry, 12_345, &HashSet::new())
            .await
            .expect_err("missing offsets partition");

        assert!(err == codes::NOT_COORDINATOR);
    }
}
