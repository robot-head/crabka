//! `DescribeTransactions` (`api_key=65`, KIP-664). Admin RPC that
//! returns the full state of every requested transactional id —
//! producer id/epoch, current state, txn timeout, start time, and the
//! set of `(topic, partition)` tuples enrolled in the current
//! transaction.
//!
//! ## ACL
//!
//! Per-tid `Describe` on `TransactionalId(name)`. Deny → per-row
//! `error_code = TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)` with all
//! other fields cleared. Unknown tid → per-row
//! `TRANSACTIONAL_ID_NOT_FOUND (75)`. Matches the JVM `KafkaApis`
//! shape.

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        describe_transactions_request::DescribeTransactionsRequest,
        describe_transactions_response::{
            DescribeTransactionsResponse, TopicData, TransactionState,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    txn::state::{TxnEntry, TxnState},
};

const TRANSACTIONAL_ID_NOT_FOUND: i16 = 75;

fn txn_state_str(s: TxnState) -> &'static str {
    match s {
        TxnState::Empty => "Empty",
        TxnState::Ongoing => "Ongoing",
        TxnState::PrepareCommit => "PrepareCommit",
        TxnState::PrepareAbort => "PrepareAbort",
        TxnState::CompleteCommit => "CompleteCommit",
        TxnState::CompleteAbort => "CompleteAbort",
        TxnState::Dead => "Dead",
    }
}

/// Build the `topics` list for one txn entry by grouping the entry's
/// `(topic, partition)` set by topic name. Ordering: topics
/// alphabetical, partitions ascending — JVM clients don't depend on
/// ordering but deterministic output keeps wire-snapshot tests stable.
fn topics_for(entry: &TxnEntry) -> Vec<TopicData> {
    let mut by_topic: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for tp in &entry.partitions {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition.get());
    }
    by_topic
        .into_iter()
        .map(|(topic, mut parts)| {
            parts.sort_unstable();
            TopicData {
                topic,
                partitions: parts,
                ..Default::default()
            }
        })
        .collect()
}

#[tracing::instrument(
    name = "handle_describe_transactions",
    level = "info",
    skip_all,
    fields(api = "DescribeTransactions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeTransactionsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    let mut rows: Vec<TransactionState> = Vec::with_capacity(req.transactional_ids.len());
    for tid in &req.transactional_ids {
        // ACL gate: per-tid `Describe` on `TransactionalId`.
        let allow = broker.config.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::TransactionalId,
                resource_name: tid.as_str(),
                operation: AclOperation::Describe,
            },
        );
        if allow == AuthorizationResult::Deny {
            rows.push(TransactionState {
                error_code: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                transactional_id: tid.clone(),
                ..Default::default()
            });
            continue;
        }

        // Look up the coordinator's local entry. Unknown → 75.
        let Some(handle) = broker.txn_coordinator.get(tid.as_str()) else {
            rows.push(TransactionState {
                error_code: TRANSACTIONAL_ID_NOT_FOUND,
                transactional_id: tid.clone(),
                ..Default::default()
            });
            continue;
        };
        let entry = handle.lock().await;

        let row = TransactionState {
            error_code: codes::NONE,
            transactional_id: entry.transactional_id.clone(),
            transaction_state: txn_state_str(entry.state).to_string(),
            transaction_timeout_ms: entry.txn_timeout_ms,
            transaction_start_time_ms: entry.start_ms,
            // Unwrap into the raw-`i64` wire field.
            producer_id: entry.producer_id.get(),
            producer_epoch: entry.producer_epoch,
            topics: topics_for(&entry),
            ..Default::default()
        };
        drop(entry);
        rows.push(row);
    }

    let resp = DescribeTransactionsResponse {
        throttle_time_ms: 0,
        transaction_states: rows,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::txn::state::TopicPartition;

    fn entry() -> TxnEntry {
        let mut e = TxnEntry::new_empty("tx".into(), crabka_log::ProducerId(100), 0, 60_000, 1_000);
        e.partitions.insert(TopicPartition {
            topic: "b".into(),
            partition: crabka_ids::PartitionIndex(2),
        });
        e.partitions.insert(TopicPartition {
            topic: "b".into(),
            partition: crabka_ids::PartitionIndex(0),
        });
        e.partitions.insert(TopicPartition {
            topic: "a".into(),
            partition: crabka_ids::PartitionIndex(1),
        });
        e
    }

    #[test]
    fn topics_for_groups_and_sorts() {
        let e = entry();
        let t = topics_for(&e);
        // Alphabetical topics, ascending partitions.
        let expected = vec![
            TopicData {
                topic: "a".to_string(),
                partitions: vec![1],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            TopicData {
                topic: "b".to_string(),
                partitions: vec![0, 2],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(Vec::new()),
            },
        ];
        assert!(t == expected);
    }
}
