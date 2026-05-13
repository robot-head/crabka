//! `OffsetForLeaderEpoch` (`api_key=23`). For each requested (topic,
//! partition, `leader_epoch`), returns the `end_offset` of that epoch —
//! i.e., the first offset of the *next* epoch, which is the truncation
//! point a follower should use when recovering from `FENCED_LEADER_EPOCH`.
//!
//! Protocol:
//! - `requested_epoch > current_leader_epoch` → `UNKNOWN_LEADER_EPOCH`
//! - `requested_epoch == current_leader_epoch` → `end_offset = log_end_offset`
//! - `requested_epoch < current_leader_epoch` → `end_offset` from checkpoint,
//!   or `-1` (`UNDEFINED_OFFSET`) if not in checkpoint.
//!
//! Reference: KIP-101 (Alter Replication Protocol to use Leader Epoch
//! rather than High Watermark for Truncation).

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use std::sync::atomic::Ordering;

use crabka_protocol::owned::offset_for_leader_epoch_request::OffsetForLeaderEpochRequest;
use crabka_protocol::owned::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = OffsetForLeaderEpochRequest::decode(&mut cur, version)?;

        let mut topics_out: Vec<OffsetForLeaderTopicResult> =
            Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            let mut parts_out: Vec<EpochEndOffset> =
                Vec::with_capacity(topic.partitions.len());

            for part in &topic.partitions {
                let mut out = EpochEndOffset {
                    partition: part.partition,
                    leader_epoch: part.leader_epoch,
                    end_offset: -1,
                    ..Default::default()
                };

                let Some(p) = partitions.get(&(topic.topic.clone(), part.partition)) else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };

                let current_epoch =
                    p.value().current_leader_epoch.load(Ordering::Acquire);

                if part.leader_epoch > current_epoch {
                    // Follower is ahead of us — stale metadata on our side.
                    out.error_code = codes::UNKNOWN_LEADER_EPOCH;
                    out.end_offset = -1;
                } else {
                    // Compute end_offset via the epoch checkpoint.
                    // `end_offset_for_epoch` returns log_end_offset when
                    // leader_epoch == current_epoch (the epoch is still
                    // open), or the start-offset of the next epoch (which
                    // is the truncation point) for older epochs.
                    let log = p.value().log.lock().expect("log mutex poisoned");
                    let leo = log.log_end_offset();
                    let end_offset = log
                        .epoch_checkpoint()
                        .end_offset_for_epoch(part.leader_epoch, leo);
                    drop(log);
                    out.error_code = codes::NONE;
                    out.end_offset = end_offset;
                    // Report the leader's view of the epoch (same as
                    // requested unless our checkpoint doesn't know the
                    // exact epoch, in which case end_offset == -1).
                    out.leader_epoch = current_epoch;
                }

                parts_out.push(out);
            }

            topics_out.push(OffsetForLeaderTopicResult {
                topic: topic.topic,
                partitions: parts_out,
                ..Default::default()
            });
        }

        let resp = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: topics_out,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
