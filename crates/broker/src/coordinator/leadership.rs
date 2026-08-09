//! Group-coordinator actor ownership across offsets-partition leader changes.

use std::{collections::HashSet, sync::Arc, time::Duration};

use crabka_ids::PartitionIndex;
use crabka_metadata::{MetadataImage, NodeId};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    GroupCoordinator,
    bootstrap::{OFFSETS_TOPIC, replay_partition},
    partitioner::partition_for_group,
    unified::{
        actor::GroupActorMessage, share::actor::ShareGroupActorMessage,
        streams::actor::StreamsGroupActorMessage,
    },
};
use crate::{metadata_source::MetadataSource, partition_registry::PartitionRegistry};

pub(crate) fn spawn(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    coordinator: Arc<GroupCoordinator>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut images = metadata.watch_image();
        let mut previous_image = images.borrow_and_update().clone();
        let mut led = led_partitions(&previous_image, node_id);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                changed = images.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let image = images.borrow_and_update().clone();
                    let next = led_partitions(&image, node_id);
                    for partition in led.difference(&next).copied().collect::<Vec<_>>() {
                        unload_partition(&coordinator, &previous_image, partition).await;
                    }
                    for partition in next.difference(&led).copied().collect::<Vec<_>>() {
                        spawn_partition_load(
                            node_id,
                            Arc::clone(&metadata),
                            Arc::clone(&partitions),
                            Arc::clone(&coordinator),
                            partition,
                            shutdown.child_token(),
                        );
                    }
                    led = next;
                    previous_image = image;
                }
            }
        }
    });
}

fn spawn_partition_load(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    coordinator: Arc<GroupCoordinator>,
    partition: PartitionIndex,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            let still_leader = metadata
                .current_image()
                .partition(OFFSETS_TOPIC, partition.get())
                .is_some_and(|record| record.leader == node_id);
            if !still_leader {
                return;
            }
            if partitions.contains(OFFSETS_TOPIC, partition) {
                if let Err(error) = replay_partition(&partitions, &coordinator, partition) {
                    tracing::error!(
                        partition = partition.get(),
                        %error,
                        "could not load newly-led group coordinator partition"
                    );
                }
                return;
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }
    });
}

fn led_partitions(image: &MetadataImage, node_id: NodeId) -> HashSet<PartitionIndex> {
    image
        .partitions_of(OFFSETS_TOPIC)
        .filter(|partition| partition.leader == node_id)
        .map(|partition| PartitionIndex(partition.partition))
        .collect()
}

async fn unload_partition(
    coordinator: &GroupCoordinator,
    image: &MetadataImage,
    partition: PartitionIndex,
) {
    let timeout = coordinator
        .config
        .shutdown_ack_timeout
        .max(Duration::from_millis(1));
    // Offset-only groups have a classic actor but no protocol-type record.
    // Include every live actor and seed map so losing an offsets partition
    // cannot leave any stale coordinator state reachable on the old leader.
    let mut known_group_ids = HashSet::new();
    known_group_ids.extend(
        coordinator
            .group_types
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(coordinator.groups.iter().map(|entry| entry.key().clone()));
    known_group_ids.extend(
        coordinator
            .share_groups
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_groups
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(coordinator.seeds.iter().map(|entry| entry.key().clone()));
    known_group_ids.extend(
        coordinator
            .seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .share_seeds
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .share_seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_seeds
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    let group_ids: Vec<String> = known_group_ids
        .into_iter()
        .filter(|group_id| partition_for_group(image, group_id) == partition.get())
        .collect();
    for group_id in group_ids {
        if let Some((_, handle)) = coordinator.groups.remove(&group_id) {
            let (reply, ack) = oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::Shutdown(reply))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(timeout, ack).await;
            }
        }
        if let Some((_, handle)) = coordinator.share_groups.remove(&group_id) {
            let (reply, ack) = oneshot::channel();
            if handle
                .tx
                .send(ShareGroupActorMessage::Shutdown(reply))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(timeout, ack).await;
            }
        }
        if let Some((_, handle)) = coordinator.streams_groups.remove(&group_id) {
            let (reply, ack) = oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Shutdown(reply))
                .await
                .is_ok()
            {
                let _ = tokio::time::timeout(timeout, ack).await;
            }
        }
        coordinator.seeds.remove(&group_id);
        coordinator.seeds_cache.remove(&group_id);
        coordinator.share_seeds.remove(&group_id);
        coordinator.share_seeds_cache.remove(&group_id);
        coordinator.streams_seeds.remove(&group_id);
        coordinator.streams_seeds_cache.remove(&group_id);
        coordinator.group_types.remove(&group_id);
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_metadata::{MetadataRecord, PartitionRecord};

    use super::*;

    #[test]
    fn led_partition_set_tracks_all_local_offsets_leaders() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (partition, leader) in [(0, NodeId(1)), (1, NodeId(2)), (2, NodeId(1))] {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: OFFSETS_TOPIC.into(),
                partition,
                leader,
                replicas: vec![leader],
                isr: vec![leader],
                ..PartitionRecord::default()
            }));
        }
        check!(
            led_partitions(&image, NodeId(1))
                == HashSet::from([PartitionIndex(0), PartitionIndex(2)])
        );
    }
}
