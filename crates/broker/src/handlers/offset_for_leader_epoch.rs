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

use std::sync::atomic::Ordering;

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
        offset_for_leader_epoch_response::{
            EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
        },
    },
};

use crate::{broker::Broker, codes, error::BrokerError};

#[allow(clippy::unused_async)] // async to match the inline-intercept handler shape.
#[tracing::instrument(
    name = "handle_offset_for_leader_epoch",
    level = "info",
    skip_all,
    fields(api = "OffsetForLeaderEpoch", version, req_bytes = req_bytes.len()),
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
    // Test-only: count served OFLE requests so the KIP-320 proactive-validation
    // integration test can prove the consumer's validate pass issued an OFLE
    // RPC (vs. the reactive in-band fetch paths, which issue none).
    #[cfg(any(test, feature = "test-helpers"))]
    let ofle_counter = broker.offset_for_leader_epoch_requests.clone();
    {
        #[cfg(any(test, feature = "test-helpers"))]
        ofle_counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        let mut cur: &[u8] = req_bytes;
        let req = OffsetForLeaderEpochRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // Per-topic `Describe` on `Topic(name)`. A denied topic gets
        // `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row it
        // requested; authorized topics proceed unchanged.
        let acl_image = broker.controller.current_image();

        let mut topics_out: Vec<OffsetForLeaderTopicResult> = Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            if crate::handlers::acl_denied(
                broker.config.authorizer.as_ref(),
                &acl_image,
                ctx,
                ResourceType::Topic,
                &topic.topic,
                AclOperation::Describe,
            ) {
                let parts_out = topic
                    .partitions
                    .iter()
                    .map(|part| EpochEndOffset {
                        partition: part.partition,
                        leader_epoch: part.leader_epoch,
                        end_offset: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        ..Default::default()
                    })
                    .collect();
                topics_out.push(OffsetForLeaderTopicResult {
                    topic: topic.topic,
                    partitions: parts_out,
                    ..Default::default()
                });
                continue;
            }
            let mut parts_out: Vec<EpochEndOffset> = Vec::with_capacity(topic.partitions.len());

            for part in &topic.partitions {
                let mut out = EpochEndOffset {
                    partition: part.partition,
                    leader_epoch: part.leader_epoch,
                    end_offset: -1,
                    ..Default::default()
                };

                let Some(p) =
                    partitions.get(&topic.topic, crabka_ids::PartitionIndex(part.partition))
                else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };

                let current_epoch = p.current_leader_epoch.load(Ordering::Acquire);

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
                    let log = p.log.lock().expect("log mutex poisoned");
                    let leo = log.log_end_offset();
                    // Wrap the raw wire `requested_epoch` for the log-crate seam.
                    let end_offset = log
                        .epoch_checkpoint()
                        .end_offset_for_epoch(crabka_log::LeaderEpoch(part.leader_epoch), leo);
                    drop(log);
                    out.error_code = codes::NONE;
                    // Unwrap the log-layer `Offset` into the wire `i64` field.
                    out.end_offset = end_offset.0;
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
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn topic_describe_denied_yields_topic_authorization_failed_rows() {
        use crabka_protocol::owned::offset_for_leader_epoch_response::{
            self, EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = crate::handlers::RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "client-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        };
        assert!(crate::handlers::acl_denied(
            &authorizer,
            &image,
            &ctx,
            ResourceType::Topic,
            "t",
            AclOperation::Describe,
        ));

        let resp = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetForLeaderTopicResult {
                topic: "t".into(),
                partitions: vec![EpochEndOffset {
                    partition: 0,
                    leader_epoch: 0,
                    end_offset: -1,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let version = offset_for_leader_epoch_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version).expect("encode");
        let mut cur: &[u8] = &buf;
        let decoded = OffsetForLeaderEpochResponse::decode(&mut cur, version).unwrap();
        assert!(decoded.topics[0].partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
    }
}
