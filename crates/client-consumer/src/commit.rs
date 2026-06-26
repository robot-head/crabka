//! `Consumer::commit_sync` and `commit_async`.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;
use crabka_protocol::owned::offset_commit_response::OffsetCommitResponse;

use crate::consumer::Consumer;
use crate::coordinator::{COORDINATOR_RETRY_TIMEOUT, find_coordinator, with_coordinator_refind};
use crate::error::ConsumerError;
use crate::offset_wire::build_commit_topics;
use crate::position::PartitionPosition;

/// First non-zero per-partition `error_code` in an `OffsetCommitResponse`, or
/// `0` if every partition committed cleanly. `with_coordinator_refind` reads
/// this to decide whether to re-discover the coordinator and retry.
fn first_commit_error(resp: &OffsetCommitResponse) -> i16 {
    for t in &resp.topics {
        for p in &t.partitions {
            if p.error_code != 0 {
                return p.error_code;
            }
        }
    }
    0
}

fn commit_offsets(
    raw_offsets: HashMap<(String, i32), i64>,
    positions: &HashMap<(String, i32), PartitionPosition>,
) -> HashMap<(String, i32), (i64, i32)> {
    raw_offsets
        .into_iter()
        .map(|(k, v)| {
            let epoch = positions.get(&k).map_or(-1, |p| p.offset_epoch);
            (k, (v, epoch))
        })
        .collect()
}

fn build_commit_request(
    group_id: String,
    generation_id_or_member_epoch: i32,
    member_id: String,
    topics: Vec<crabka_protocol::owned::offset_commit_request::OffsetCommitRequestTopic>,
) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id,
        generation_id_or_member_epoch,
        member_id,
        topics,
        ..Default::default()
    }
}

/// Map an `OffsetCommit` response to a result. `0` is success. The rebalance
/// codes `ILLEGAL_GENERATION (22)` and `REBALANCE_IN_PROGRESS (27)` are DEFERRED
/// — treated as `Ok` — because the generation we stamped only went stale due to
/// a rebalance: the coordinator task rejoins and publishes the new generation
/// (`current_generation`), and the offsets recommit on the next call. A
/// long-running block-builder/compactor commit loop must therefore NOT crash on
/// them (the offset is at-least-once). Any other non-zero code is fatal.
fn commit_response_result(resp: &OffsetCommitResponse) -> Result<(), ConsumerError> {
    match first_commit_error(resp) {
        0 | 22 | 27 => Ok(()),
        code => Err(ConsumerError::Server(code)),
    }
}

impl Consumer {
    /// Commit the current next-offsets for every assigned partition.
    /// Blocks until the broker acks.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: I/O-bound coordinator RPC, exercised by integration tests
    pub async fn commit_sync(&self) -> Result<(), ConsumerError> {
        let raw_offsets = self.next_offsets.lock().await.clone();
        if raw_offsets.is_empty() {
            return Ok(());
        }
        let pos = self.positions.lock().await;
        let offsets = commit_offsets(raw_offsets, &pos);
        drop(pos);
        let topic_ids = self.topic_ids.lock().await.clone();
        let topics = build_commit_topics(offsets, &topic_ids);

        // OffsetCommit is a coordinator RPC: route it to the coordinator broker
        // (discovered at build time, kept current by the coordinator task), and
        // re-discover on a cold/relocating-coordinator code so a coordinator
        // move is chased rather than looping NOT_COORDINATOR on the stale id.
        let resp = with_coordinator_refind(
            &self.client,
            &self.group_id,
            &self.coordinator_id,
            COORDINATOR_RETRY_TIMEOUT,
            first_commit_error,
            || {
                let group_id = self.group_id.clone();
                let member_id = self.member_id.clone();
                let topics = topics.clone();
                let client = &self.client;
                let target = self.coordinator_id.load(Ordering::Relaxed);
                async move {
                    client
                        .broker(target)
                        .send(build_commit_request(
                            group_id,
                            self.current_generation.load(Ordering::Relaxed),
                            member_id,
                            topics,
                        ))
                        .await
                        .map_err(ConsumerError::from)
                }
            },
        )
        .await?;

        // A rebalance can move the group out from under this commit (22
        // ILLEGAL_GENERATION / 27 REBALANCE_IN_PROGRESS). `commit_response_result`
        // treats those as deferred (Ok) — the coordinator rejoins, publishes the
        // new generation, and the offsets recommit next round — so the commit
        // loop survives a routine rebalance instead of crashing. Surface it once
        // for ops visibility.
        let code = first_commit_error(&resp);
        if code == 22 || code == 27 {
            tracing::warn!(
                group = %self.group_id,
                error_code = code,
                "offset commit deferred: group rebalancing; will recommit after the coordinator rejoins",
            );
        }
        commit_response_result(&resp)
    }

    /// Fire-and-forget commit. Returns once the request is enqueued on the
    /// client's writer task; does NOT wait for the broker ack. Errors are
    /// logged but not returned.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: fire-and-forget I/O spawn, exercised by integration tests
    pub fn commit_async(&self) {
        let client = self.client.clone();
        let group_id = self.group_id.clone();
        let generation = self.current_generation.load(Ordering::Relaxed);
        let member_id = self.member_id.clone();
        let offsets = self.next_offsets.clone();
        let positions = self.positions.clone();
        let topic_ids = self.topic_ids.clone();
        let coordinator_id = self.coordinator_id.clone();
        tokio::spawn(async move {
            let raw_snapshot = offsets.lock().await.clone();
            if raw_snapshot.is_empty() {
                return;
            }
            let pos = positions.lock().await;
            let snapshot = commit_offsets(raw_snapshot, &pos);
            drop(pos);
            let topic_ids = topic_ids.lock().await.clone();
            let topics = build_commit_topics(snapshot, &topic_ids);
            // Route to the coordinator broker. If it returns a moved/cold
            // coordinator code (or the socket is gone), re-discover once and
            // retry — but don't block a background commit on the full retry
            // loop; one re-find recovers a coordinator move at-least-once.
            let make_req = |topics: Vec<_>| {
                build_commit_request(group_id.clone(), generation, member_id.clone(), topics)
            };
            let target = coordinator_id.load(Ordering::Relaxed);
            let res = client.broker(target).send(make_req(topics.clone())).await;
            let moved = match &res {
                Ok(resp) => {
                    crate::coordinator::is_retriable_coordinator_code(first_commit_error(resp))
                }
                Err(crabka_client_core::ClientError::Disconnected) => true,
                Err(_) => false,
            };
            if moved {
                match find_coordinator(&client, &group_id).await {
                    Ok(id) => {
                        coordinator_id.store(id, Ordering::Relaxed);
                        if let Err(e) = client.broker(id).send(make_req(topics)).await {
                            tracing::warn!(error = %e, "commit_async retry after re-find failed");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "commit_async coordinator re-discovery failed");
                    }
                }
            } else if let Err(e) = res {
                tracing::warn!(error = %e, "commit_async failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::UnknownTaggedFields;
    use crabka_protocol::owned::offset_commit_request::OffsetCommitRequestPartition;
    use crabka_protocol::owned::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    use crabka_protocol::primitives::uuid::Uuid;

    fn response(errors: &[i16]) -> OffsetCommitResponse {
        OffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitResponseTopic {
                name: "topic".into(),
                topic_id: Uuid::ZERO,
                partitions: errors
                    .iter()
                    .enumerate()
                    .map(
                        |(partition_index, error_code)| OffsetCommitResponsePartition {
                            partition_index: i32::try_from(partition_index).unwrap(),
                            error_code: *error_code,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    )
                    .collect(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    #[test]
    fn first_commit_error_returns_first_non_zero_partition_error() {
        assert!(first_commit_error(&response(&[0, 0])) == 0);
        assert!(first_commit_error(&response(&[0, 27, 42])) == 27);
        assert!(first_commit_error(&response(&[16, 27])) == 16);
    }

    #[test]
    fn commit_offsets_use_position_epoch_or_unknown_epoch() {
        let mut raw = HashMap::new();
        raw.insert(("known".into(), 0), 11);
        raw.insert(("unknown".into(), 1), 22);

        let mut positions = HashMap::new();
        positions.insert(
            ("known".into(), 0),
            PartitionPosition {
                offset_epoch: 7,
                ..Default::default()
            },
        );

        let offsets = commit_offsets(raw, &positions);
        assert!(offsets.get(&("known".into(), 0)) == Some(&(11, 7)));
        assert!(offsets.get(&("unknown".into(), 1)) == Some(&(22, -1)));
    }

    #[test]
    fn build_commit_request_preserves_group_member_generation_and_topics() {
        let topics = vec![
            crabka_protocol::owned::offset_commit_request::OffsetCommitRequestTopic {
                name: "topic".into(),
                topic_id: Uuid::ZERO,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 3,
                    committed_offset: 99,
                    committed_leader_epoch: 5,
                    committed_metadata: Some(String::new()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];

        let req = build_commit_request("group-a".into(), 42, "member-a".into(), topics);

        assert!(req.group_id == "group-a");
        assert!(req.generation_id_or_member_epoch == 42);
        assert!(req.member_id == "member-a");
        assert!(req.topics.len() == 1);
        assert!(req.topics[0].name == "topic");
        assert!(req.topics[0].partitions[0].partition_index == 3);
        assert!(req.topics[0].partitions[0].committed_offset == 99);
        assert!(req.topics[0].partitions[0].committed_leader_epoch == 5);
    }

    #[test]
    fn commit_response_result_defers_rebalance_codes_and_fails_others() {
        // success
        assert!(commit_response_result(&response(&[0, 0])).is_ok());
        // a non-rebalance error is fatal
        let err = commit_response_result(&response(&[0, 42])).unwrap_err();
        assert!(matches!(err, ConsumerError::Server(42)));
        // ILLEGAL_GENERATION (22) and REBALANCE_IN_PROGRESS (27) are DEFERRED,
        // not fatal — a commit loop must survive a rebalance; the coordinator
        // rejoins (publishing the new generation) and the offsets recommit.
        assert!(commit_response_result(&response(&[22])).is_ok());
        assert!(commit_response_result(&response(&[27])).is_ok());
        assert!(commit_response_result(&response(&[0, 22])).is_ok());
        // first-error precedence: a fatal code ahead of a rebalance code stays fatal
        let err = commit_response_result(&response(&[16, 27])).unwrap_err();
        assert!(matches!(err, ConsumerError::Server(16)));
    }
}
