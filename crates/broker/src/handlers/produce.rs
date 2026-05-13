//! `Produce` (`api_key=0`). Routes each partition's records to that
//! partition's writer-actor and awaits the assigned base offset.
//!
//! MVP scope: one `RecordBatch` per (topic, partition) per request. The
//! generated `PartitionProduceData.records` field is already an
//! `Option<RecordBatch>`, so if the on-wire records buffer contained
//! multiple concatenated batches the codegen would have rejected the
//! trailing bytes during decode. Clients that send a single batch per
//! partition (the typical case) are fully supported.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::produce_request::ProduceRequest;
use crabka_protocol::owned::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::{Decode, Encode};
use tokio::sync::oneshot;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::partition::{ProduceJob, WriterMessage};

#[allow(clippy::too_many_lines)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let controller = broker.controller.clone();
    let producer_state = broker.producer_state.clone();
    let txn_coordinator = broker.txn_coordinator.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ProduceRequest::decode(&mut cur, version)?;
        let timeout = Duration::from_millis(u64::try_from(req.timeout_ms.max(0)).unwrap_or(0));

        let mut topic_results: Vec<TopicProduceResponse> = Vec::with_capacity(req.topic_data.len());

        for topic in req.topic_data {
            // v ≤ 12 sends the topic name; v ≥ 13 sends only topic_id and
            // we look it up in the metadata image.
            let topic_name = if !topic.name.is_empty() {
                topic.name.clone()
            } else if topic.topic_id != WireUuid::ZERO {
                let image = controller.current_image();
                image
                    .topics()
                    .find(|t| t.topic_id.into_bytes() == topic.topic_id.0)
                    .map(|t| t.name.clone())
                    .unwrap_or_default()
            } else {
                String::new()
            };

            let mut partition_results: Vec<PartitionProduceResponse> =
                Vec::with_capacity(topic.partition_data.len());

            for part_data in topic.partition_data {
                let idx = part_data.index;
                let mut out = PartitionProduceResponse {
                    index: idx,
                    ..Default::default()
                };

                // Either there's a single decoded RecordBatch to append, or
                // the field was null / undecodable → INVALID_REQUEST.
                let Some(mut batch) = part_data.records else {
                    out.error_code = codes::INVALID_REQUEST;
                    partition_results.push(out);
                    continue;
                };

                let part = if topic_name.is_empty() {
                    None
                } else {
                    partitions
                        .get(&(topic_name.clone(), idx))
                        .map(|p| p.clone())
                };
                let Some(part) = part else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    partition_results.push(out);
                    continue;
                };

                // Stamp the current leader epoch onto the batch — this becomes
                // the `partition_leader_epoch` carried on the wire and used by
                // KIP-101 fence validation on the follower's Fetch.
                batch.partition_leader_epoch = part
                    .current_leader_epoch
                    .load(std::sync::atomic::Ordering::Acquire);

                // ── transactional produce verify (KIP-1319 v2) ──────────
                // This check is more authoritative than idempotent dedup,
                // so it runs first. Non-transactional batches (pid < 0 or
                // is_transactional=false) skip directly to the dedup gate.
                let is_transactional = batch.attributes.is_transactional();
                {
                    let pid_txn = batch.producer_id;
                    let epoch_txn = batch.producer_epoch;
                    if is_transactional && pid_txn >= 0 {
                        let Some(tid) = txn_coordinator.tid_for_pid(pid_txn) else {
                            // Unknown producer_id — reject.
                            out.error_code = codes::INVALID_PRODUCER_ID_MAPPING;
                            partition_results.push(out);
                            continue;
                        };
                        if txn_coordinator.is_coordinator_for(&tid).await {
                            let Some(entry_mutex) = txn_coordinator.get(&tid) else {
                                out.error_code = codes::INVALID_PRODUCER_ID_MAPPING;
                                partition_results.push(out);
                                continue;
                            };
                            let mut entry = entry_mutex.lock().await;
                            if entry.producer_epoch != epoch_txn {
                                out.error_code = codes::INVALID_PRODUCER_EPOCH;
                                partition_results.push(out);
                                continue;
                            }
                            let tp = crate::txn::state::TopicPartition {
                                topic: topic_name.clone(),
                                partition: idx,
                            };
                            // Consider the partition "needs registering" if it
                            // isn't in the current partition set OR if the
                            // current state is CompleteCommit/CompleteAbort
                            // (indicating a new transaction after a completed
                            // one — the partition set is stale).
                            let needs_register = !entry.partitions.contains(&tp)
                                || matches!(
                                    entry.state,
                                    crate::txn::state::TxnState::CompleteCommit
                                        | crate::txn::state::TxnState::CompleteAbort
                                );
                            if needs_register {
                                // v2 auto-AddPartitionsToTxn: register the
                                // partition inline if the state allows it.
                                if !entry
                                    .state
                                    .can_transition_to(crate::txn::state::TxnState::Ongoing)
                                {
                                    out.error_code = codes::INVALID_TXN_STATE;
                                    partition_results.push(out);
                                    continue;
                                }
                                // If starting a new txn after a completed one,
                                // clear the stale partition set.
                                if matches!(
                                    entry.state,
                                    crate::txn::state::TxnState::CompleteCommit
                                        | crate::txn::state::TxnState::CompleteAbort
                                ) {
                                    entry.partitions.clear();
                                    entry.offset_commit_groups.clear();
                                }
                                entry.state = crate::txn::state::TxnState::Ongoing;
                                entry.partitions.insert(tp);
                                entry.last_update_ms = crate::txn::util::now_millis();
                                let snap = entry.clone();
                                // Lock must be dropped before the async put.
                                drop(entry);
                                txn_coordinator.put(snap).await?;
                            }
                            // else: partition already registered in an active txn — fall through.
                        }
                        // else: not the coordinator for this tid (slice-9 MVP).
                        // Trust the producer to have called AddPartitionsToTxn
                        // through the correct coordinator. Inter-broker v2
                        // auto-add is deferred (slice 10+).
                    }
                }

                // ── idempotent-producer dedup gate ───────────────────────
                let pid = batch.producer_id;
                let epoch = batch.producer_epoch;
                let base_seq = batch.base_sequence;
                let last_offset_delta = batch.last_offset_delta;
                let max_timestamp = batch.max_timestamp;

                let dedup_outcome = if pid >= 0 {
                    Some(
                        producer_state
                            .check(&topic_name, idx, pid, epoch, base_seq, last_offset_delta)
                            .await,
                    )
                } else {
                    None
                };

                match dedup_outcome {
                    Some(crate::producer_state::Decision::Duplicate { base_offset }) => {
                        out.error_code = codes::NONE;
                        out.base_offset = base_offset;
                        partition_results.push(out);
                        continue;
                    }
                    Some(crate::producer_state::Decision::OutOfOrder) => {
                        out.error_code = codes::OUT_OF_ORDER_SEQUENCE_NUMBER;
                        partition_results.push(out);
                        continue;
                    }
                    Some(crate::producer_state::Decision::Fenced) => {
                        out.error_code = codes::INVALID_PRODUCER_EPOCH;
                        partition_results.push(out);
                        continue;
                    }
                    Some(crate::producer_state::Decision::Append) | None => {
                        // fall through to writer dispatch
                    }
                }

                let (ack_tx, ack_rx) = oneshot::channel();
                let job = WriterMessage::Produce(ProduceJob { batch, ack: ack_tx });

                if part.writer_tx.send(job).await.is_err() {
                    out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                    partition_results.push(out);
                    continue;
                }

                match tokio::time::timeout(timeout, ack_rx).await {
                    Ok(Ok(Ok(base_offset))) => {
                        if req.acks == -1 {
                            let target = base_offset + i64::from(last_offset_delta) + 1;
                            let deadline = std::time::Instant::now() + timeout;
                            match part.await_hw_at_least(target, deadline).await {
                                Ok(()) => {
                                    out.error_code = codes::NONE;
                                    out.base_offset = base_offset;
                                    if pid >= 0 {
                                        producer_state
                                            .commit(
                                                &topic_name,
                                                idx,
                                                pid,
                                                epoch,
                                                base_seq,
                                                last_offset_delta,
                                                base_offset,
                                                max_timestamp,
                                            )
                                            .await;
                                    }
                                }
                                Err(_timeout) => {
                                    out.error_code = codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND;
                                    out.base_offset = base_offset;
                                    if pid >= 0 {
                                        producer_state
                                            .commit(
                                                &topic_name,
                                                idx,
                                                pid,
                                                epoch,
                                                base_seq,
                                                last_offset_delta,
                                                base_offset,
                                                max_timestamp,
                                            )
                                            .await;
                                    }
                                }
                            }
                        } else {
                            out.error_code = codes::NONE;
                            out.base_offset = base_offset;
                            if pid >= 0 {
                                producer_state
                                    .commit(
                                        &topic_name,
                                        idx,
                                        pid,
                                        epoch,
                                        base_seq,
                                        last_offset_delta,
                                        base_offset,
                                        max_timestamp,
                                    )
                                    .await;
                            }
                        }
                    }
                    Ok(Ok(Err(e))) => {
                        out.error_code = codes::from_broker_error(&e);
                    }
                    Ok(Err(_)) => {
                        // Writer dropped the oneshot without sending — shouldn't
                        // happen unless the writer task panicked between recv
                        // and ack. Map to NOT_LEADER_OR_FOLLOWER.
                        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                    }
                    Err(_) => {
                        out.error_code = codes::REQUEST_TIMED_OUT;
                    }
                }
                partition_results.push(out);
            }

            topic_results.push(TopicProduceResponse {
                name: topic_name,
                topic_id: topic.topic_id,
                partition_responses: partition_results,
                ..Default::default()
            });
        }

        let resp = ProduceResponse {
            responses: topic_results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
