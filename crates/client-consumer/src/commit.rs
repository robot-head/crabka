//! `Consumer::commit_sync` and `commit_async`.

use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

use crabka_protocol::{
    owned::{
        offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestTopic},
        offset_commit_response::OffsetCommitResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::sync::Mutex;

use crate::{
    consumer::Consumer,
    coordinator::{
        COORDINATOR_RETRY_TIMEOUT, find_coordinator, is_retriable_transport_error,
        with_coordinator_refind,
    },
    error::ConsumerError,
    offset_wire::build_commit_topics,
    position::PartitionPosition,
};

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
            // Unwrap the position's leader epoch to raw wire `int32` for the
            // OffsetCommit `committed_leader_epoch` field.
            let epoch = positions.get(&k).map_or(-1, |p| p.offset_epoch.get());
            (k, (v, epoch))
        })
        .collect()
}

async fn snapshot_commit_topics(
    offsets: &Arc<Mutex<HashMap<(String, i32), i64>>>,
    positions: &Arc<Mutex<HashMap<(String, i32), PartitionPosition>>>,
    topic_ids: &Arc<Mutex<HashMap<String, WireUuid>>>,
) -> Option<(usize, Vec<OffsetCommitRequestTopic>)> {
    let raw_offsets = offsets.lock().await.clone();
    if raw_offsets.is_empty() {
        return None;
    }
    let partitions = raw_offsets.len();
    let pos = positions.lock().await;
    let offsets = commit_offsets(raw_offsets, &pos);
    drop(pos);
    let topic_ids = topic_ids.lock().await.clone();
    Some((partitions, build_commit_topics(offsets, &topic_ids)))
}

fn build_commit_request(
    group_id: String,
    generation_id_or_member_epoch: i32,
    member_id: String,
    group_instance_id: Option<String>,
    topics: Vec<crabka_protocol::owned::offset_commit_request::OffsetCommitRequestTopic>,
) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id,
        generation_id_or_member_epoch,
        member_id,
        group_instance_id,
        topics,
        ..Default::default()
    }
}

/// Map an `OffsetCommit` response to a result, given whether the coordinator
/// task is still alive. `0` is success. The rebalance codes `ILLEGAL_GENERATION
/// (22)` and `REBALANCE_IN_PROGRESS (27)` are DEFERRED — treated as `Ok` — ONLY
/// while the coordinator task is alive to rejoin and republish the generation
/// (`current_generation`): the offsets stay in `next_offsets` and recommit on
/// the next call (at-least-once), so a long-running block-builder/compactor
/// commit loop survives a routine rebalance instead of crashing. If the
/// coordinator task has EXITED it can never republish a fresh generation, so
/// deferring would silently never-advance — surface those codes as fatal
/// instead, so the process restarts and rejoins from scratch. Any other
/// non-zero code is always fatal.
fn commit_response_result(
    resp: &OffsetCommitResponse,
    coordinator_alive: bool,
) -> Result<(), ConsumerError> {
    match first_commit_error(resp) {
        0 => Ok(()),
        22 | 25 | 27 if coordinator_alive => Ok(()),
        code => Err(ConsumerError::Server(code)),
    }
}

impl Consumer {
    /// Commit the current next-offsets for every assigned partition.
    /// Blocks until the broker acks.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: I/O-bound coordinator RPC, exercised by integration tests
    #[tracing::instrument(
        name = "consumer.commit_sync",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            member_id = %self.member_id,
            generation = self.current_generation.load(Ordering::Relaxed),
            partitions = tracing::field::Empty,
        ),
        err
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn commit_sync(&self) -> Result<(), ConsumerError> {
        let Some((partitions, topics)) =
            snapshot_commit_topics(&self.next_offsets, &self.positions, &self.topic_ids).await
        else {
            return Ok(());
        };
        tracing::Span::current().record("partitions", partitions);

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
                let group_instance_id = self.group_instance_id.clone();
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
                            group_instance_id,
                            topics,
                        ))
                        .await
                        .map_err(ConsumerError::from)
                }
            },
        )
        .await?;

        // A rebalance can move the group out from under this commit (22
        // ILLEGAL_GENERATION / 27 REBALANCE_IN_PROGRESS). While the coordinator
        // task is alive it rejoins, republishes the generation, and the offsets
        // recommit next round, so `commit_response_result` defers (Ok) and the
        // commit loop survives the rebalance. If the coordinator task has exited
        // it returns fatal instead — a dead coordinator can never recover the
        // generation, so a loud restart beats a silent never-advance.
        let coordinator_alive = self
            .coordinator_handle
            .as_ref()
            .is_none_or(|h| !h.is_finished());
        let code = first_commit_error(&resp);
        if (code == 22 || code == 27) && coordinator_alive {
            tracing::warn!(
                group = %self.group_id,
                error_code = code,
                "offset commit deferred: group rebalancing; will recommit after the coordinator rejoins",
            );
        }
        commit_response_result(&resp, coordinator_alive)
    }

    /// Fire-and-forget commit. Returns once the request is enqueued on the
    /// client's writer task; does NOT wait for the broker ack. Errors are
    /// logged but not returned.
    #[cfg_attr(test, mutants::skip)] // cargo-mutants: fire-and-forget I/O spawn, exercised by integration tests
    #[tracing::instrument(
        name = "consumer.commit_async",
        level = "debug",
        skip_all,
        fields(
            group_id = %self.group_id,
            member_id = %self.member_id,
            generation = self.current_generation.load(Ordering::Relaxed),
        )
    )]
    pub fn commit_async(&self) {
        let client = self.client.clone();
        let group_id = self.group_id.clone();
        let generation = self.current_generation.load(Ordering::Relaxed);
        let member_id = self.member_id.clone();
        let group_instance_id = self.group_instance_id.clone();
        let offsets = Arc::clone(&self.next_offsets);
        let positions = Arc::clone(&self.positions);
        let topic_ids = Arc::clone(&self.topic_ids);
        let coordinator_id = Arc::clone(&self.coordinator_id);
        tokio::spawn(async move {
            let Some((_, topics)) = snapshot_commit_topics(&offsets, &positions, &topic_ids).await
            else {
                return;
            };
            // Route to the coordinator broker. If it returns a moved/cold
            // coordinator code (or the socket is gone), re-discover once and
            // retry — but don't block a background commit on the full retry
            // loop; one re-find recovers a coordinator move at-least-once.
            let make_req = |topics: Vec<_>| {
                build_commit_request(
                    group_id.clone(),
                    generation,
                    member_id.clone(),
                    group_instance_id.clone(),
                    topics,
                )
            };
            let target = coordinator_id.load(Ordering::Relaxed);
            let res = client.broker(target).send(make_req(topics.clone())).await;
            let moved = match &res {
                Ok(resp) => {
                    crate::coordinator::is_retriable_coordinator_code(first_commit_error(resp))
                }
                Err(e) if is_retriable_transport_error(e) => true,
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
    use assert2::check;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
            offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
        },
        primitives::uuid::Uuid,
    };

    use super::*;

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
        for (_name, errors, want) in [
            ("all successful", &[0, 0][..], 0),
            ("later first error", &[0, 27, 42][..], 27),
            ("first partition errors", &[16, 27][..], 16),
        ] {
            assert2::assert!(first_commit_error(&response(errors)) == want);
        }
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
                offset_epoch: crabka_ids::LeaderEpoch(7),
                ..Default::default()
            },
        );

        let offsets = commit_offsets(raw, &positions);
        assert2::assert!(
            offsets
                == HashMap::from([
                    (("known".into(), 0), (11, 7)),
                    (("unknown".into(), 1), (22, -1)),
                ])
        );
    }

    #[tokio::test]
    async fn snapshot_commit_topics_returns_none_for_empty_offsets() {
        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let positions = Arc::new(Mutex::new(HashMap::new()));
        let topic_ids = Arc::new(Mutex::new(HashMap::new()));

        let snapshot = snapshot_commit_topics(&offsets, &positions, &topic_ids).await;

        assert2::assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn snapshot_commit_topics_preserves_count_topics_offsets_and_epochs() {
        let offsets = Arc::new(Mutex::new(HashMap::from([
            (("alpha".to_string(), 0), 10),
            (("alpha".to_string(), 1), 20),
        ])));
        let positions = Arc::new(Mutex::new(HashMap::from([(
            ("alpha".to_string(), 1),
            PartitionPosition {
                offset_epoch: crabka_ids::LeaderEpoch(7),
                ..Default::default()
            },
        )])));
        let topic_id = Uuid([1; 16]);
        let topic_ids = Arc::new(Mutex::new(HashMap::from([("alpha".to_string(), topic_id)])));

        let (partition_count, topics) = snapshot_commit_topics(&offsets, &positions, &topic_ids)
            .await
            .expect("non-empty offsets are snapshotted");
        let mut topics = topics;
        topics[0].partitions.sort_by_key(|p| p.partition_index);

        assert2::assert!(
            (partition_count, topics)
                == (
                    2,
                    vec![OffsetCommitRequestTopic {
                        name: "alpha".into(),
                        topic_id,
                        partitions: vec![
                            OffsetCommitRequestPartition {
                                partition_index: 0,
                                committed_offset: 10,
                                committed_leader_epoch: -1,
                                committed_metadata: Some(String::new()),
                                unknown_tagged_fields: UnknownTaggedFields::default(),
                            },
                            OffsetCommitRequestPartition {
                                partition_index: 1,
                                committed_offset: 20,
                                committed_leader_epoch: 7,
                                committed_metadata: Some(String::new()),
                                unknown_tagged_fields: UnknownTaggedFields::default(),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }]
                )
        );
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

        let req = build_commit_request(
            "group-a".into(),
            42,
            "member-a".into(),
            Some("instance-a".into()),
            topics.clone(),
        );

        assert2::assert!(
            req == OffsetCommitRequest {
                group_id: "group-a".into(),
                generation_id_or_member_epoch: 42,
                member_id: "member-a".into(),
                group_instance_id: Some("instance-a".into()),
                retention_time_ms: -1,
                topics,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        );
    }

    #[test]
    fn commit_response_result_defers_rebalance_codes_only_while_coordinator_alive() {
        for (name, errors, coordinator_alive, expected_error) in [
            ("success while alive", &[0, 0][..], true, None),
            ("success after coordinator exit", &[0, 0][..], false, None),
            ("non-rebalance error", &[0, 42][..], true, Some(42)),
            ("illegal generation deferred", &[22][..], true, None),
            ("unknown member deferred", &[25][..], true, None),
            ("rebalance deferred", &[27][..], true, None),
            (
                "later illegal generation deferred",
                &[0, 22][..],
                true,
                None,
            ),
            ("illegal generation after exit", &[22][..], false, Some(22)),
            ("unknown member after exit", &[25][..], false, Some(25)),
            ("rebalance after exit", &[27][..], false, Some(27)),
            (
                "fatal error takes precedence",
                &[16, 27][..],
                true,
                Some(16),
            ),
        ] {
            let actual = commit_response_result(&response(errors), coordinator_alive);
            let actual_error = match actual {
                Ok(()) => None,
                Err(ConsumerError::Server(code)) => Some(code),
                Err(other) => panic!("case {name}: unexpected error {other:?}"),
            };
            check!(actual_error == expected_error, "case {name}");
        }
    }
}
