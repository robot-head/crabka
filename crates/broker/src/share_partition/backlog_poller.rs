//! Fleet-complete KIP-932 backlog sampling for Prometheus/KEDA.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use crabka_client_core::ConnectionOptions;
use crabka_ids::PartitionIndex;
use crabka_metadata::NodeId;
use crabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use crabka_security::ListenerProtocol;
use tokio_util::sync::CancellationToken;

use crate::{
    codes,
    coordinator::{GroupCoordinator, bootstrap::OFFSETS_TOPIC, partitioner},
    metadata_source::MetadataSource,
    metrics::{BrokerMetrics, ShareGroupLabel},
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    share_coordinator::persister_client::SharePersister,
};

pub(crate) fn effective_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 {
        spso.max(log_start)
    } else {
        log_start
    };
    (hwm - base).max(0)
}

pub(crate) struct BacklogPoller {
    pub node_id: NodeId,
    pub coordinator: Arc<GroupCoordinator>,
    pub metadata: Arc<dyn MetadataSource>,
    pub partitions: Arc<PartitionRegistry>,
    pub persister: Arc<SharePersister>,
    pub inter_broker: Arc<InterBrokerClient>,
    pub listener_protocol: ListenerProtocol,
    pub listener_name: String,
    pub period: Duration,
    pub metrics: BrokerMetrics,
    pub shutdown: CancellationToken,
}

impl BacklogPoller {
    pub(crate) fn spawn(self) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last = HashMap::new();
            loop {
                tokio::select! {
                    () = self.shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let groups = self.coordinator.share_group_ids();
                        let image = self.metadata.current_image();
                        prune_stale(
                            &self.metrics,
                            &mut last,
                            &image,
                            self.node_id,
                            &groups,
                        );
                        tracing::debug!(
                            groups = groups.len(),
                            "sampling share-group backlog",
                        );
                        match self.snapshot(&image, groups).await {
                            Ok(next) => {
                                tracing::debug!(samples = next.len(), "share-group backlog sample complete");
                                replace(&self.metrics, &mut last, next);
                            }
                            Err(error) => tracing::warn!(%error, "share-group backlog sample failed"),
                        }
                    }
                }
            }
            clear(&self.metrics, &mut last);
        });
    }

    async fn snapshot(
        &self,
        image: &crabka_metadata::MetadataImage,
        groups: Vec<String>,
    ) -> Result<HashMap<ShareGroupLabel, i64>, String> {
        let mut snapshot = HashMap::new();
        for group_id in groups {
            if !owns_group(image, self.node_id, &group_id) {
                tracing::debug!(%group_id, "skipping share group owned by another coordinator");
                continue;
            }
            let Some(state) = self.coordinator.share_state_partition_metadata(&group_id) else {
                tracing::debug!(%group_id, "share group has no partition metadata");
                continue;
            };
            tracing::debug!(
                %group_id,
                initialized_topics = state.initialized.len(),
                "sampling initialized share-group partitions",
            );
            for (topic_id, partitions) in state.initialized {
                let Some(topic) = image.topic_name_by_id(&topic_id).map(str::to_owned) else {
                    tracing::debug!(%group_id, %topic_id, "skipping deleted share-group topic");
                    continue;
                };
                for partition in partitions {
                    let spso = self
                        .persister
                        .read_state(&group_id, topic_id, partition)
                        .await
                        .map_err(|error| error.to_string())?
                        .map_or(-1, |state| state.start_offset.0);
                    let (hwm, log_start) = self.offsets(image, &topic, partition).await?;
                    snapshot.insert(
                        ShareGroupLabel {
                            group_id: group_id.clone(),
                            topic: topic.clone(),
                            partition,
                        },
                        effective_backlog(hwm, spso, log_start),
                    );
                }
            }
        }
        Ok(snapshot)
    }

    async fn offsets(
        &self,
        image: &crabka_metadata::MetadataImage,
        topic: &str,
        partition: i32,
    ) -> Result<(i64, i64), String> {
        let leader = image
            .partition(topic, partition)
            .ok_or_else(|| format!("missing metadata for {topic}-{partition}"))?
            .leader;
        if leader == self.node_id {
            let local = self
                .partitions
                .get(topic, PartitionIndex(partition))
                .ok_or_else(|| format!("leader partition {topic}-{partition} is not local"))?;
            return Ok((local.high_watermark().await.0, local.log_start_offset().0));
        }
        self.remote_offsets(image, leader, topic, partition).await
    }

    async fn remote_offsets(
        &self,
        image: &crabka_metadata::MetadataImage,
        leader: NodeId,
        topic: &str,
        partition: i32,
    ) -> Result<(i64, i64), String> {
        let broker = image
            .broker(leader)
            .ok_or_else(|| format!("unknown leader broker {leader}"))?;
        let endpoint = broker
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == self.listener_name);
        let (host, port) = endpoint.map_or_else(
            || (broker.host.as_str(), broker.port),
            |e| (e.host.as_str(), e.port),
        );
        let options = ConnectionOptions {
            client_id: format!("crabka-share-backlog-{}", self.node_id),
            ..ConnectionOptions::default()
        };
        let connection = self
            .inter_broker
            .connect_as_connection(host, port, self.listener_protocol, "localhost", options)
            .await
            .map_err(|error| error.to_string())?;
        let partition_metadata = image
            .partition(topic, partition)
            .ok_or_else(|| format!("missing metadata for {topic}-{partition}"))?;
        let topic_id = image
            .topic(topic)
            .ok_or_else(|| format!("missing topic metadata for {topic}"))?
            .topic_id;
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0,
            isolation_level: 0,
            topics: vec![FetchTopic {
                topic: topic.to_owned(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                partitions: vec![FetchPartition {
                    partition,
                    current_leader_epoch: partition_metadata.leader_epoch.0,
                    // A consumer Fetch reports the committed high watermark and
                    // log start even when it returns no records. Asking beyond
                    // the end keeps this metadata probe payload-free.
                    fetch_offset: i64::MAX,
                    partition_max_bytes: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response: FetchResponse = connection
            .send(request)
            .await
            .map_err(|error| error.to_string())?;
        connection.close();
        fetch_offsets(&response, topic, WireUuid(*topic_id.as_bytes()), partition)
    }
}

fn owns_group(image: &crabka_metadata::MetadataImage, node_id: NodeId, group_id: &str) -> bool {
    let partition = partitioner::partition_for_group(image, group_id);
    image
        .partition(OFFSETS_TOPIC, partition)
        .is_some_and(|record| record.leader == node_id)
}

fn fetch_offsets(
    response: &FetchResponse,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
) -> Result<(i64, i64), String> {
    if response.error_code != codes::NONE {
        return Err(format!(
            "Fetch metadata probe {topic}-{partition} returned top-level code {}",
            response.error_code
        ));
    }
    let result = response
        .responses
        .iter()
        .find(|row| row.topic_id == topic_id || row.topic == topic)
        .and_then(|row| {
            row.partitions
                .iter()
                .find(|row| row.partition_index == partition)
        })
        .ok_or_else(|| format!("empty Fetch metadata response for {topic}-{partition}"))?;
    if result.error_code != codes::NONE {
        return Err(format!(
            "Fetch metadata probe {topic}-{partition} returned code {}",
            result.error_code
        ));
    }
    if result.log_start_offset < 0 || result.high_watermark < result.log_start_offset {
        return Err(format!(
            "Fetch metadata probe {topic}-{partition} returned invalid offsets: high watermark {}, log start {}",
            result.high_watermark, result.log_start_offset
        ));
    }
    Ok((result.high_watermark, result.log_start_offset))
}

fn prune_stale(
    metrics: &BrokerMetrics,
    last: &mut HashMap<ShareGroupLabel, i64>,
    image: &crabka_metadata::MetadataImage,
    node_id: NodeId,
    groups: &[String],
) {
    let groups: HashSet<&str> = groups.iter().map(String::as_str).collect();
    last.retain(|label, _| {
        let current = groups.contains(label.group_id.as_str())
            && owns_group(image, node_id, &label.group_id)
            && image.topic(&label.topic).is_some();
        if !current {
            metrics.share_group_backlog.remove(label);
        }
        current
    });
}

fn replace(
    metrics: &BrokerMetrics,
    last: &mut HashMap<ShareGroupLabel, i64>,
    next: HashMap<ShareGroupLabel, i64>,
) {
    for label in last.keys().filter(|label| !next.contains_key(*label)) {
        metrics.share_group_backlog.remove(label);
    }
    for (label, value) in &next {
        metrics.share_group_backlog.get_or_create(label).set(*value);
    }
    *last = next;
}

fn clear(metrics: &BrokerMetrics, last: &mut HashMap<ShareGroupLabel, i64>) {
    for label in last.keys() {
        metrics.share_group_backlog.remove(label);
    }
    last.clear();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crabka_ids::LeaderEpoch;
    use crabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
    use crabka_protocol::{
        owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        primitives::uuid::Uuid as WireUuid,
    };

    use super::{effective_backlog, fetch_offsets, owns_group, prune_stale, replace};
    use crate::{
        coordinator::{bootstrap::OFFSETS_TOPIC, partitioner},
        metrics::{BrokerMetrics, ShareGroupLabel},
    };

    fn routing_image() -> MetadataImage {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: OFFSETS_TOPIC.into(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 2,
            replication_factor: 1,
        }));
        for (partition, leader) in [(0, NodeId(1)), (1, NodeId(2))] {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: OFFSETS_TOPIC.into(),
                partition,
                leader,
                replicas: vec![leader],
                isr: vec![leader],
                leader_epoch: LeaderEpoch(7),
                ..Default::default()
            }));
        }
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "work".into(),
            topic_id: uuid::Uuid::from_u128(2),
            partitions: 1,
            replication_factor: 1,
        }));
        image
    }

    fn group_for_partition(partition: i32) -> String {
        (0..100)
            .map(|i| format!("group-{i}"))
            .find(|group| partitioner::partition_for_group_with_count(group, 2) == partition)
            .expect("both offsets partitions receive a group")
    }

    #[test]
    fn backlog_uses_spso_or_log_start_and_never_goes_negative() {
        assert_eq!(effective_backlog(100, 40, 0), 60);
        assert_eq!(effective_backlog(100, -1, 10), 90);
        assert_eq!(effective_backlog(100, 100, 0), 0);
        assert_eq!(effective_backlog(100, 120, 0), 0);
        assert_eq!(effective_backlog(110, 0, 100), 10);
    }

    #[test]
    fn ownership_is_checked_for_each_groups_offsets_partition() {
        let image = routing_image();
        let on_zero = group_for_partition(0);
        let on_one = group_for_partition(1);

        assert!(owns_group(&image, NodeId(1), &on_zero));
        assert!(!owns_group(&image, NodeId(1), &on_one));
        assert!(owns_group(&image, NodeId(2), &on_one));
        assert!(!owns_group(&image, NodeId(2), &on_zero));
    }

    #[test]
    fn remote_offsets_are_the_fetch_high_watermark_not_log_end() {
        let topic_id = WireUuid([9; 16]);
        let response = FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic_id,
                partitions: vec![PartitionData {
                    partition_index: 3,
                    high_watermark: 7,
                    log_start_offset: 2,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(fetch_offsets(&response, "work", topic_id, 3), Ok((7, 2)));
    }

    #[test]
    fn zero_is_published_then_departed_series_are_removed() {
        let metrics = BrokerMetrics::new();
        let label = ShareGroupLabel {
            group_id: "workers".into(),
            topic: "work".into(),
            partition: 0,
        };
        let mut last = HashMap::new();

        replace(&metrics, &mut last, HashMap::from([(label.clone(), 0)]));
        assert_eq!(
            metrics
                .share_group_backlog
                .get(&label)
                .map(|gauge| gauge.get()),
            Some(0)
        );

        replace(&metrics, &mut last, HashMap::new());
        assert!(metrics.share_group_backlog.get(&label).is_none());
        assert!(last.is_empty());
    }

    #[test]
    fn ownership_loss_prunes_a_stale_series_before_sampling() {
        let image = routing_image();
        let metrics = BrokerMetrics::new();
        let group_id = group_for_partition(1);
        let label = ShareGroupLabel {
            group_id: group_id.clone(),
            topic: "work".into(),
            partition: 0,
        };
        metrics.share_group_backlog.get_or_create(&label).set(11);
        let mut last = HashMap::from([(label.clone(), 11)]);

        prune_stale(&metrics, &mut last, &image, NodeId(1), &[group_id]);

        assert!(last.is_empty());
        assert!(metrics.share_group_backlog.get(&label).is_none());
    }
}
