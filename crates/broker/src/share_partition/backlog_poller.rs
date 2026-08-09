//! Fleet-complete KIP-932 backlog sampling for Prometheus/KEDA.

use std::{collections::HashMap, sync::Arc, time::Duration};

use crabka_client_core::ConnectionOptions;
use crabka_ids::PartitionIndex;
use crabka_metadata::NodeId;
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use crabka_security::ListenerProtocol;
use tokio_util::sync::CancellationToken;

use crate::{
    coordinator::{GroupCoordinator, bootstrap::OFFSETS_TOPIC},
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
                        let is_group_coordinator = self.is_group_coordinator();
                        let groups = self.coordinator.share_group_ids();
                        tracing::debug!(
                            is_group_coordinator,
                            groups = groups.len(),
                            "sampling share-group backlog",
                        );
                        if !is_group_coordinator {
                            clear(&self.metrics, &mut last);
                            continue;
                        }
                        match self.snapshot(groups).await {
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

    fn is_group_coordinator(&self) -> bool {
        self.metadata
            .current_image()
            .partitions_of(OFFSETS_TOPIC)
            .any(|partition| partition.leader == self.node_id)
    }

    async fn snapshot(&self, groups: Vec<String>) -> Result<HashMap<ShareGroupLabel, i64>, String> {
        let image = self.metadata.current_image();
        let mut snapshot = HashMap::new();
        for group_id in groups {
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
                    let (hwm, log_start) = self.offsets(&image, &topic, partition).await?;
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
        let request = |timestamp| ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: topic.to_owned(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: partition,
                    current_leader_epoch: -1,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let earliest = connection
            .send(request(-2))
            .await
            .map_err(|error| error.to_string())?;
        let latest = connection
            .send(request(-1))
            .await
            .map_err(|error| error.to_string())?;
        connection.close();
        let offset =
            |response: &crabka_protocol::owned::list_offsets_response::ListOffsetsResponse,
             kind: &str|
             -> Result<i64, String> {
                let result = response
                    .topics
                    .first()
                    .and_then(|topic| topic.partitions.first())
                    .ok_or_else(|| {
                        format!("empty ListOffsets({kind}) response for {topic}-{partition}")
                    })?;
                if result.error_code != 0 {
                    return Err(format!(
                        "ListOffsets({kind}) {topic}-{partition} returned code {}",
                        result.error_code
                    ));
                }
                Ok(result.offset)
            };
        Ok((offset(&latest, "latest")?, offset(&earliest, "earliest")?))
    }
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
    use super::effective_backlog;

    #[test]
    fn backlog_uses_spso_or_log_start_and_never_goes_negative() {
        assert_eq!(effective_backlog(100, 40, 0), 60);
        assert_eq!(effective_backlog(100, -1, 10), 90);
        assert_eq!(effective_backlog(100, 100, 0), 0);
        assert_eq!(effective_backlog(100, 120, 0), 0);
        assert_eq!(effective_backlog(110, 0, 100), 10);
    }
}
