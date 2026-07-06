//! `DescribeProducers` (`api_key=61`, KIP-664). Admin RPC that surfaces
//! the broker's in-memory producer-state snapshot for a set of
//! `(topic, partition)` pairs. Used by JVM `Admin.describeProducers`
//! and `kafka-transactions --describe-producers` to debug stuck
//! idempotent / transactional producers.
//!
//! ## ACL
//!
//! Per-topic `Read` on `Topic(name)` (mirrors `Fetch` per KIP-664).
//! Deny → every partition of that topic carries
//! `TOPIC_AUTHORIZATION_FAILED (29)`. Unknown topic / out-of-range
//! partition → per-partition `UNKNOWN_TOPIC_OR_PARTITION (3)`.
//!
//! ## Field semantics
//!
//! `producer_id`, `producer_epoch`, `last_sequence`, `last_timestamp`
//! come straight from `crate::producer_state`. The transactional
//! fields `coordinator_epoch` and `current_txn_start_offset` aren't
//! wired up — the broker doesn't track them per `(topic, partition)`
//! today, so they default to `-1` (the schema's "unknown / no current
//! txn" sentinel). When transactional in-flight tracking lands, only
//! the row builder needs to look those up.

use bytes::Bytes;
use crabka_metadata::AclOperation;
use crabka_protocol::{
    Decode,
    owned::{
        describe_producers_request::DescribeProducersRequest,
        describe_producers_response::{
            DescribeProducersResponse, PartitionResponse, ProducerState, TopicResponse,
        },
    },
};

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
};

#[allow(clippy::unused_async)] // signature symmetry with other inline-intercept handlers
#[tracing::instrument(
    name = "handle_describe_producers",
    level = "info",
    skip_all,
    fields(api = "DescribeProducers", version, req_bytes = req_bytes.len()),
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
    let req = DescribeProducersRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Batch-authorize Read on every requested topic in one pass.
    let topic_decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Read,
        req.topics.iter().map(|t| t.name.as_str()),
    );

    let mut topics_out: Vec<TopicResponse> = Vec::with_capacity(req.topics.len());
    for topic_req in &req.topics {
        let allow = topic_decisions
            .get(topic_req.name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny);

        let mut parts_out: Vec<PartitionResponse> =
            Vec::with_capacity(topic_req.partition_indexes.len());

        if allow == AuthorizationResult::Deny {
            // KIP-664: per-partition TOPIC_AUTHORIZATION_FAILED on every
            // requested partition of a denied topic.
            for &idx in &topic_req.partition_indexes {
                parts_out.push(PartitionResponse {
                    partition_index: idx,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    error_message: None,
                    active_producers: Vec::new(),
                    ..Default::default()
                });
            }
            topics_out.push(TopicResponse {
                name: topic_req.name.clone(),
                partitions: parts_out,
                ..Default::default()
            });
            continue;
        }

        // Topic-existence + per-partition-bounds check. The image
        // exposes `partition(name, idx) -> Option<&PartitionRecord>`
        // which combines both checks in one lookup.
        for &idx in &topic_req.partition_indexes {
            if image.partition(topic_req.name.as_str(), idx).is_none() {
                parts_out.push(PartitionResponse {
                    partition_index: idx,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: None,
                    active_producers: Vec::new(),
                    ..Default::default()
                });
                continue;
            }

            let snapshot = broker
                .producer_state
                .snapshot(topic_req.name.as_str(), crabka_ids::PartitionIndex(idx))
                .await;
            let active_producers: Vec<ProducerState> = snapshot
                .into_iter()
                .map(|(producer_id, entry)| ProducerState {
                    producer_id,
                    producer_epoch: i32::from(entry.epoch),
                    last_sequence: entry.last_sequence,
                    last_timestamp: entry.last_timestamp,
                    // Crabka doesn't track per-(topic, partition) txn
                    // bookkeeping on the producer-state map; these stay
                    // at -1 (the schema "unknown / no current txn"
                    // sentinel) until that work lands.
                    coordinator_epoch: -1,
                    current_txn_start_offset: -1,
                    ..Default::default()
                })
                .collect();

            parts_out.push(PartitionResponse {
                partition_index: idx,
                error_code: codes::NONE,
                error_message: None,
                active_producers,
                ..Default::default()
            });
        }

        topics_out.push(TopicResponse {
            name: topic_req.name.clone(),
            partitions: parts_out,
            ..Default::default()
        });
    }

    let resp = DescribeProducersResponse {
        throttle_time_ms: 0,
        topics: topics_out,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
