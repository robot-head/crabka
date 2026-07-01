//! KIP-858: report which log-directory UUID hosts each local replica by
//! sending `AssignReplicasToDirs` (api key 73) to the controller leader.
//!
//! Two entry points are exported:
//!
//! - [`build_request`] — pure grouping builder, unit-testable with no
//!   network.
//! - [`send_assignments`] — async sender that mirrors the pattern used by
//!   `isr_maintenance::send_alter_partition`.

use std::collections::BTreeMap;
use std::sync::Arc;

use crabka_protocol::owned::assign_replicas_to_dirs_request::{
    AssignReplicasToDirsRequest, DirectoryData, PartitionData, TopicData,
};

/// Group flat `(topic_id, partition, dir_uuid)` assignments into the nested
/// `AssignReplicasToDirs` wire shape:
/// `directories[]` → `topics[]` → `partitions[]`.
///
/// Groups deterministically (`BTreeMap` keyed by the 16-byte UUID representation)
/// so the resulting request is stable across calls, which is important for
/// unit tests.
///
/// `broker_epoch` is set to `-1` (unknown), matching the convention used by
/// `send_alter_partition`.
pub(crate) fn build_request(
    broker_id: i32,
    assignments: &[(uuid::Uuid, i32, uuid::Uuid)], // (topic_id, partition, dir_uuid)
) -> AssignReplicasToDirsRequest {
    // dir_uuid → topic_id → [partition_index]
    let mut by_dir: BTreeMap<[u8; 16], BTreeMap<[u8; 16], Vec<i32>>> = BTreeMap::new();

    for (topic_id, partition, dir_uuid) in assignments {
        by_dir
            .entry(*dir_uuid.as_bytes())
            .or_default()
            .entry(*topic_id.as_bytes())
            .or_default()
            .push(*partition);
    }

    let directories: Vec<DirectoryData> = by_dir
        .into_iter()
        .map(|(dir_bytes, topics_map)| {
            let topics: Vec<TopicData> = topics_map
                .into_iter()
                .map(|(topic_bytes, mut partitions)| {
                    partitions.sort_unstable();
                    TopicData {
                        topic_id: crabka_protocol::primitives::uuid::Uuid(topic_bytes),
                        partitions: partitions
                            .into_iter()
                            .map(|p| PartitionData {
                                partition_index: p,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }
                })
                .collect();
            DirectoryData {
                id: crabka_protocol::primitives::uuid::Uuid(dir_bytes),
                topics,
                ..Default::default()
            }
        })
        .collect();

    AssignReplicasToDirsRequest {
        broker_id,
        broker_epoch: -1,
        directories,
        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
    }
}

/// Send an `AssignReplicasToDirs` report to the controller leader.
///
/// Resolves the controller leader's address from `controller`, opens a
/// short-lived `crabka_client_core::Client`, sends `req`, and checks the
/// top-level `error_code`.
///
/// Returns `Err` on:
/// - no controller leader in the image
/// - leader broker record not in the image
/// - connection failure
/// - send/receive failure
/// - non-zero `error_code` in the response
pub(crate) async fn send_assignments(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    client_id: &str,
    req: AssignReplicasToDirsRequest,
) -> Result<(), String> {
    // Resolve the controller leader address — same pattern as
    // `isr_maintenance::send_alter_partition`.
    let leader_id = *controller.watch_leader().borrow();
    let Some(leader_id) = leader_id else {
        return Err("no controller leader".into());
    };
    let image = controller.current_image();
    let Some(broker_rec) = image.broker(leader_id) else {
        return Err("controller leader not in image".into());
    };
    let addr = format!("{}:{}", broker_rec.host, broker_rec.port);

    let client = crabka_client_core::Client::builder()
        .bootstrap(addr)
        .client_id(client_id.to_owned())
        .build()
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let resp = client.send(req).await.map_err(|e| format!("send: {e}"))?;
    validate_assign_response(resp.error_code)
}

fn validate_assign_response(error_code: i16) -> Result<(), String> {
    if error_code != 0 {
        return Err(format!(
            "AssignReplicasToDirs rejected by controller: error_code={error_code}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::MetadataImage;
    use crabka_protocol::{Decode, Encode};
    use crabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter,
        SnapshotRange, UpdateVoter,
    };
    use std::{collections::BTreeSet, net::SocketAddr};
    use tokio::sync::watch;
    use uuid::Uuid;

    struct MockSource {
        image: Arc<MetadataImage>,
        leader: Option<NodeId>,
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for MockSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_tx, rx) = watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            let (_tx, rx) = watch::channel(self.leader);
            rx
        }

        fn quorum_state(&self) -> QuorumState {
            panic!("not used by assign_dirs tests")
        }

        async fn submit_change(
            &self,
            _records: Vec<crabka_metadata::MetadataRecord>,
        ) -> Result<(), RaftError> {
            panic!("not used by assign_dirs tests")
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            panic!("not used by assign_dirs tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            panic!("not used by assign_dirs tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            panic!("not used by assign_dirs tests")
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            panic!("not used by assign_dirs tests")
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            panic!("not used by assign_dirs tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by assign_dirs tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by assign_dirs tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            panic!("not used by assign_dirs tests")
        }

        async fn cancel(&self) {}
    }

    #[test]
    fn build_request_groups_correctly() {
        // Assignments:
        //   (tA, 0, dX)  →  dir dX, topic tA, partition 0
        //   (tA, 1, dX)  →  dir dX, topic tA, partition 1
        //   (tB, 0, dY)  →  dir dY, topic tB, partition 0
        let ta = Uuid::from_u128(0xAAAA);
        let tb = Uuid::from_u128(0xBBBB);
        let dx = Uuid::from_u128(0xDDDD);
        let dy = Uuid::from_u128(0xEEEE);

        let assignments = [(ta, 0i32, dx), (ta, 1i32, dx), (tb, 0i32, dy)];

        let req = build_request(7, &assignments);

        // broker_id, epoch, and two directories.
        assert!((req.broker_id, req.broker_epoch, req.directories.len()) == (7, -1, 2));

        // Find dir dX and dY by their UUID bytes.
        let dir_x = req
            .directories
            .iter()
            .find(|d| d.id.0 == *dx.as_bytes())
            .expect("dir dX missing");
        let dir_y = req
            .directories
            .iter()
            .find(|d| d.id.0 == *dy.as_bytes())
            .expect("dir dY missing");

        // dX should have exactly one topic (tA) with two partitions [0, 1].
        assert!(dir_x.topics.len() == 1);
        let topic_a = dir_x
            .topics
            .iter()
            .find(|t| t.topic_id.0 == *ta.as_bytes())
            .expect("topic tA in dX missing");
        let mut part_indices: Vec<i32> = topic_a
            .partitions
            .iter()
            .map(|p| p.partition_index)
            .collect();
        part_indices.sort_unstable();
        assert!(part_indices == vec![0, 1]);

        // dY should have exactly one topic (tB) with one partition [0].
        assert!(dir_y.topics.len() == 1);
        let topic_b = dir_y
            .topics
            .iter()
            .find(|t| t.topic_id.0 == *tb.as_bytes())
            .expect("topic tB in dY missing");
        assert!(topic_b.partitions.len() == 1);
        assert!(topic_b.partitions[0].partition_index == 0);
    }

    #[test]
    fn build_request_empty_assignments() {
        let req = build_request(1, &[]);
        assert!((req.broker_id, req.broker_epoch, req.directories.len()) == (1, -1, 0));
    }

    #[test]
    fn build_request_encodes_unknown_broker_epoch() {
        let req = build_request(3, &[]);
        let mut bytes = bytes::BytesMut::new();

        req.encode(&mut bytes, 0).expect("encode request");
        let decoded =
            AssignReplicasToDirsRequest::decode(&mut bytes.freeze(), 0).expect("decode request");

        assert!(decoded.broker_id == 3);
        assert!(decoded.broker_epoch == -1);
    }

    #[tokio::test]
    async fn send_assignments_rejects_bad_controller_leader() {
        let cases = [
            // No controller leader elected at all.
            (None, "no controller leader"),
            // Leader elected but its broker record is missing from the image.
            (Some(42), "controller leader not in image"),
        ];
        for (leader, expected) in cases {
            let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(MockSource {
                image: Arc::new(MetadataImage::new(uuid::Uuid::nil())),
                leader,
            });

            let err = send_assignments(&source, "assign-test", build_request(1, &[]))
                .await
                .expect_err("bad controller leader must fail");

            assert!(err == expected, "case: leader={leader:?}");
        }
    }

    #[test]
    fn validate_assign_response_rejects_controller_error() {
        assert!(validate_assign_response(0).is_ok());
        let err = validate_assign_response(42).expect_err("non-zero error_code must fail");
        assert!(err.contains("error_code=42"));
    }
}
