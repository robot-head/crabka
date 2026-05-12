//! `AddPartitionsToTxn` (`api_key=24`). Registers one or more
//! (topic, partition) pairs with an ongoing transaction.
//!
//! Wire-format versions:
//!  - v0-3: single `(transactional_id, producer_id, producer_epoch, topics)`
//!    on the request; `results_by_topic_v3_and_below` on the response.
//!  - v4-5: batched `transactions` array on the request;
//!    `results_by_transaction` on the response.
//!
//! This broker only handles the single-tid case (the only shape a
//! producer client ever sends). If a v4+ request carries more than one
//! transaction entry we process them all sequentially.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use crabka_protocol::owned::add_partitions_to_txn_response::{
    AddPartitionsToTxnResponse, AddPartitionsToTxnResult,
};
use crabka_protocol::owned::common::add_partitions_to_txn_topic::AddPartitionsToTxnTopic;
use crabka_protocol::owned::common::add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult;
use crabka_protocol::owned::common::add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::txn::state::{TopicPartition, TxnState};
use crate::txn::util::now_millis;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AddPartitionsToTxnRequest::decode(&mut cur, version)?;

        // Mirror Task 12's race-fix pattern: refresh leader-partition view
        // from the current metadata image before checking coordinator-ness.
        coord
            .refresh_leader_partitions(&controller.current_image())
            .await;

        if version >= 4 {
            handle_v4(&coord, version, &req).await
        } else {
            handle_v3(&coord, version, &req).await
        }
    })
}

// ── v4+ path ─────────────────────────────────────────────────────────────────

async fn handle_v4(
    coord: &crate::txn::coordinator::TxnCoordinator,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> Result<Bytes, BrokerError> {
    let mut results_by_transaction: Vec<AddPartitionsToTxnResult> =
        Vec::with_capacity(req.transactions.len());

    for txn in &req.transactions {
        let topic_results = process_one_txn(coord, txn.transactional_id.as_str(), txn.producer_id, txn.producer_epoch, &txn.topics).await;
        results_by_transaction.push(AddPartitionsToTxnResult {
            transactional_id: txn.transactional_id.clone(),
            topic_results,
            ..Default::default()
        });
    }

    let resp = AddPartitionsToTxnResponse {
        throttle_time_ms: 0,
        results_by_transaction,
        ..Default::default()
    };
    encode_response(&resp, version)
}

// ── v0-3 path ─────────────────────────────────────────────────────────────────

async fn handle_v3(
    coord: &crate::txn::coordinator::TxnCoordinator,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> Result<Bytes, BrokerError> {
    let topic_results = process_one_txn(
        coord,
        req.v3_and_below_transactional_id.as_str(),
        req.v3_and_below_producer_id,
        req.v3_and_below_producer_epoch,
        &req.v3_and_below_topics,
    )
    .await;

    let resp = AddPartitionsToTxnResponse {
        throttle_time_ms: 0,
        results_by_topic_v3_and_below: topic_results,
        ..Default::default()
    };
    encode_response(&resp, version)
}

// ── shared per-transaction logic ──────────────────────────────────────────────

/// Process a single `transactional_id` / `producer_id` / `producer_epoch`.
/// Returns per-topic, per-partition result entries (all with the same
/// error code on failure, or NONE on success).
async fn process_one_txn(
    coord: &crate::txn::coordinator::TxnCoordinator,
    tid: &str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &[AddPartitionsToTxnTopic],
) -> Vec<AddPartitionsToTxnTopicResult> {
    // 1. Coordinator check.
    if !coord.is_coordinator_for(tid).await {
        return topic_error(topics, codes::NOT_COORDINATOR);
    }

    // 2. Look up entry; verify (pid, epoch).
    let Some(entry_mutex) = coord.get(tid) else {
        return topic_error(topics, codes::INVALID_PRODUCER_ID_MAPPING);
    };

    let mut entry = entry_mutex.lock().await;
    if entry.producer_id != producer_id || entry.producer_epoch != producer_epoch {
        return topic_error(topics, codes::INVALID_PRODUCER_EPOCH);
    }

    // 3. State machine: Empty/Ongoing → Ongoing.
    //    CompleteCommit/CompleteAbort → Ongoing is also allowed to support
    //    re-use of a transactional_id without an intervening InitProducerId.
    //    In that case we clear the stale partition set so EndTxn only fans out
    //    markers to the new transaction's partitions.
    if !entry.state.can_transition_to(TxnState::Ongoing) {
        return topic_error(topics, codes::INVALID_TXN_STATE);
    }
    let was_complete = matches!(
        entry.state,
        TxnState::CompleteCommit | TxnState::CompleteAbort
    );
    entry.state = TxnState::Ongoing;
    if was_complete {
        // Starting a new transaction after a completed one: discard the stale
        // partition set so the new transaction starts clean.
        entry.partitions.clear();
        entry.offset_commit_groups.clear();
    }

    // 4. Register partitions.
    for t in topics {
        for &p in &t.partitions {
            entry.partitions.insert(TopicPartition {
                topic: t.name.clone(),
                partition: p,
            });
        }
    }
    entry.last_update_ms = now_millis();
    let snap = entry.clone();
    // Drop lock before the async persist call.
    drop(entry);

    // 5. Persist.
    if let Err(e) = coord.put(snap).await {
        tracing::error!(tid, error = %e, "AddPartitionsToTxn: failed to persist TxnEntry");
        return topic_error(topics, codes::UNKNOWN_SERVER_ERROR);
    }

    // 6. Success — all error codes = NONE.
    topic_ok(topics)
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a per-topic/per-partition result list with every partition carrying
/// `error_code`.
fn topic_error(topics: &[AddPartitionsToTxnTopic], code: i16) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| AddPartitionsToTxnTopicResult {
            name: t.name.clone(),
            results_by_partition: t
                .partitions
                .iter()
                .map(|&p| AddPartitionsToTxnPartitionResult {
                    partition_index: p,
                    partition_error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

/// Build a per-topic/per-partition result list with every partition carrying
/// `NONE` (success).
fn topic_ok(topics: &[AddPartitionsToTxnTopic]) -> Vec<AddPartitionsToTxnTopicResult> {
    topic_error(topics, codes::NONE)
}

fn encode_response(resp: &AddPartitionsToTxnResponse, version: i16) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

