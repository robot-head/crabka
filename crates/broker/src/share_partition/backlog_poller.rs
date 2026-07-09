//! Share-group backlog poll loop: the coordinator broker emits a fleet-complete
//! `share_group_backlog` gauge. Pure math is [`effective_backlog`].

use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc, time::Duration};

use crabka_ids::PartitionIndex;
use crabka_metadata::{BrokerRegistrationRecord, NodeId};
use crabka_protocol::owned::{
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
};
use uuid::Uuid;

use crate::{
    codes,
    coordinator::unified::GroupCoordinator,
    metadata_source::MetadataSource,
    metrics::{BrokerMetrics, ShareGroupLabel},
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    share_coordinator::persister_client::SharePersister,
};

type OffsetRead<'a> = Pin<Box<dyn Future<Output = Option<PartitionBacklogOffsets>> + Send + 'a>>;
type StartOffsetRead<'a> = Pin<Box<dyn Future<Output = i64> + Send + 'a>>;

const LIST_OFFSETS_EARLIEST_TIMESTAMP: i64 = -2;
const LIST_OFFSETS_LATEST_TIMESTAMP: i64 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionBacklogOffsets {
    high_watermark: i64,
    log_start: i64,
}

trait BacklogOffsetReader {
    fn read_offsets<'a>(&'a self, topic: &'a str, partition: i32) -> OffsetRead<'a>;
}

struct LocalBacklogOffsetReader<'a> {
    partitions: &'a PartitionRegistry,
}

impl BacklogOffsetReader for LocalBacklogOffsetReader<'_> {
    fn read_offsets<'a>(&'a self, topic: &'a str, partition: i32) -> OffsetRead<'a> {
        Box::pin(async move {
            let partition_handle = self.partitions.get(topic, PartitionIndex(partition))?;
            Some(PartitionBacklogOffsets {
                high_watermark: partition_handle.high_watermark().await.0,
                log_start: partition_handle.log_start_offset().0,
            })
        })
    }
}

struct ClusterBacklogOffsetReader<'a> {
    node_id: NodeId,
    partitions: &'a PartitionRegistry,
    controller: &'a Arc<dyn MetadataSource>,
    inter_broker_client: &'a Arc<InterBrokerClient>,
    inter_broker_listener_protocol: crabka_security::ListenerProtocol,
    inter_broker_listener_name: &'a str,
}

impl BacklogOffsetReader for ClusterBacklogOffsetReader<'_> {
    fn read_offsets<'a>(&'a self, topic: &'a str, partition: i32) -> OffsetRead<'a> {
        Box::pin(async move {
            let local_reader = LocalBacklogOffsetReader {
                partitions: self.partitions,
            };
            if let Some(offsets) = local_reader.read_offsets(topic, partition).await {
                return Some(offsets);
            }

            self.read_remote_offsets(topic, partition).await
        })
    }
}

impl ClusterBacklogOffsetReader<'_> {
    async fn read_remote_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<PartitionBacklogOffsets> {
        // There is no separate internal HWM RPC today. The remote path uses the
        // Kafka-compatible `ListOffsets(LATEST, EARLIEST)` seam so a coordinator
        // can observe partitions it does not host without a new wire API. In the
        // RF=1 topology covered by in-process tests, LATEST equals the local
        // high-watermark used by [`LocalBacklogOffsetReader`]; replicated-fleet
        // HWM parity remains an explicit external gate in the KEDA example.
        let image = self.controller.current_image();
        let partition_record = image.partition(topic, partition)?;
        let leader_id = partition_record.leader;
        if leader_id == self.node_id {
            tracing::warn!(
                topic,
                partition,
                leader = leader_id.0,
                "share-group backlog partition is led locally but missing from local registry"
            );
            return None;
        }

        let broker = image.broker(leader_id)?;
        let (host, port) = resolve_broker_endpoint(broker, self.inter_broker_listener_name);
        let connection_options = crabka_client_core::ConnectionOptions {
            client_id: "crabka-share-backlog-poller".to_string(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let connection = match self
            .inter_broker_client
            .connect_as_connection(
                &host,
                port,
                self.inter_broker_listener_protocol,
                "localhost",
                connection_options,
            )
            .await
        {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(
                    topic,
                    partition,
                    leader = leader_id.0,
                    host = %host,
                    port,
                    error = %error,
                    "share-group backlog remote ListOffsets connect failed"
                );
                return None;
            }
        };

        let response = match connection
            .send(build_backlog_list_offsets_request(topic, partition))
            .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(
                    topic,
                    partition,
                    leader = leader_id.0,
                    error = %error,
                    "share-group backlog remote ListOffsets request failed"
                );
                return None;
            }
        };

        partition_offsets_from_list_offsets_response(topic, partition, &response).or_else(|| {
            tracing::warn!(
                topic,
                partition,
                leader = leader_id.0,
                "share-group backlog remote ListOffsets response did not contain usable offsets"
            );
            None
        })
    }
}

fn resolve_broker_endpoint(
    broker: &BrokerRegistrationRecord,
    listener_name: &str,
) -> (String, u16) {
    broker
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == listener_name)
        .map_or_else(
            || (broker.host.clone(), broker.port),
            |endpoint| (endpoint.host.clone(), endpoint.port),
        )
}

fn build_backlog_list_offsets_request(topic: &str, partition: i32) -> ListOffsetsRequest {
    ListOffsetsRequest {
        replica_id: -1,
        topics: vec![ListOffsetsTopic {
            name: topic.to_string(),
            partitions: vec![
                ListOffsetsPartition {
                    partition_index: partition,
                    timestamp: LIST_OFFSETS_LATEST_TIMESTAMP,
                    ..ListOffsetsPartition::default()
                },
                ListOffsetsPartition {
                    partition_index: partition,
                    timestamp: LIST_OFFSETS_EARLIEST_TIMESTAMP,
                    ..ListOffsetsPartition::default()
                },
            ],
            ..ListOffsetsTopic::default()
        }],
        ..ListOffsetsRequest::default()
    }
}

fn partition_offsets_from_list_offsets_response(
    topic: &str,
    partition: i32,
    response: &ListOffsetsResponse,
) -> Option<PartitionBacklogOffsets> {
    let topic_response = response
        .topics
        .iter()
        .find(|topic_response| topic_response.name == topic)?;
    let mut offsets = topic_response
        .partitions
        .iter()
        .filter(|partition_response| partition_response.partition_index == partition);
    let latest = offsets.next()?;
    let earliest = offsets.next()?;
    if latest.error_code != codes::NONE || earliest.error_code != codes::NONE {
        return None;
    }
    if latest.offset < 0 || earliest.offset < 0 {
        return None;
    }
    Some(PartitionBacklogOffsets {
        high_watermark: latest.offset,
        log_start: earliest.offset,
    })
}

trait ShareStartOffsetReader {
    fn read_start_offset<'a>(
        &'a self,
        group_id: &'a str,
        topic_id: Uuid,
        partition: i32,
    ) -> StartOffsetRead<'a>;
}

impl ShareStartOffsetReader for SharePersister {
    fn read_start_offset<'a>(
        &'a self,
        group_id: &'a str,
        topic_id: Uuid,
        partition: i32,
    ) -> StartOffsetRead<'a> {
        Box::pin(async move {
            self.read_state(group_id, topic_id, partition)
                .await
                .ok()
                .flatten()
                .map_or(-1, |state| state.start_offset.0)
        })
    }
}

pub(crate) struct BacklogPollerConfig {
    pub(crate) node_id: NodeId,
    pub(crate) coordinator: Arc<GroupCoordinator>,
    pub(crate) controller: Arc<dyn MetadataSource>,
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) inter_broker_client: Arc<InterBrokerClient>,
    pub(crate) inter_broker_listener_protocol: crabka_security::ListenerProtocol,
    pub(crate) inter_broker_listener_name: String,
    pub(crate) persister: Arc<SharePersister>,
    pub(crate) metrics: BrokerMetrics,
    pub(crate) period: Duration,
}

/// Backlog in records for one share-group partition.
///
/// `spso < 0` means the share group has never initialized this partition (the
/// persister returned no start offset). In that state the full retained log is
/// queued, so the base is `log_start`, never the `-1` sentinel.
#[must_use]
#[allow(dead_code)]
pub(crate) fn effective_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 { spso } else { log_start };
    if hwm <= base {
        return 0;
    }
    hwm.saturating_sub(base)
}

/// Spawn the coordinator-local share-group backlog poller.
///
/// Each tick is self-gated on this broker leading `__consumer_offsets-0`. The
/// coordinator leader owns the complete initialized-share-partition set, so it
/// is the only broker that should emit `share_group_backlog` series. This
/// foundation reads partition offsets through a seam so the production caller can
/// grow from local registry reads to peer/ListOffsets reads without changing the
/// backlog math or stale-series handling.
pub(crate) fn spawn_backlog_poller(config: BacklogPollerConfig) {
    tokio::spawn(async move {
        let BacklogPollerConfig {
            node_id,
            coordinator,
            controller,
            partitions,
            inter_broker_client,
            inter_broker_listener_protocol,
            inter_broker_listener_name,
            persister,
            metrics,
            period,
        } = config;
        let mut tick = tokio::time::interval(period);
        let mut last_seen_labels = HashSet::new();
        loop {
            tick.tick().await;

            if should_skip_backlog_tick(
                coordinator.leads_offsets_partition(node_id),
                &metrics,
                &mut last_seen_labels,
            ) {
                continue;
            }

            let mut seen_labels = HashSet::new();
            let offset_reader = ClusterBacklogOffsetReader {
                node_id,
                partitions: &partitions,
                controller: &controller,
                inter_broker_client: &inter_broker_client,
                inter_broker_listener_protocol,
                inter_broker_listener_name: &inter_broker_listener_name,
            };
            for group_id in coordinator.share_group_ids() {
                poll_group_backlog(
                    &coordinator,
                    &offset_reader,
                    &persister,
                    &metrics,
                    &group_id,
                    &mut seen_labels,
                )
                .await;
            }

            finish_backlog_tick(&metrics, &mut last_seen_labels, seen_labels);
        }
    });
}

fn should_skip_backlog_tick(
    leads_offsets_partition: bool,
    metrics: &BrokerMetrics,
    last_seen_labels: &mut HashSet<ShareGroupLabel>,
) -> bool {
    if leads_offsets_partition {
        return false;
    }

    clear_backlog_labels(metrics, last_seen_labels);
    true
}

async fn poll_group_backlog(
    coordinator: &GroupCoordinator,
    offset_reader: &impl BacklogOffsetReader,
    persister: &SharePersister,
    metrics: &BrokerMetrics,
    group_id: &str,
    seen_labels: &mut HashSet<ShareGroupLabel>,
) {
    let Some(metadata) = coordinator.share_state_partition_metadata(group_id) else {
        return;
    };

    poll_group_backlog_from_metadata(
        group_id,
        metadata.initialized,
        |topic_id| coordinator.topic_name_for(topic_id),
        offset_reader,
        persister,
        metrics,
        seen_labels,
    )
    .await;
}

async fn poll_group_backlog_from_metadata(
    group_id: &str,
    initialized: Vec<(Uuid, Vec<i32>)>,
    topic_name_for: impl Fn(Uuid) -> Option<String>,
    offset_reader: &impl BacklogOffsetReader,
    start_offset_reader: &impl ShareStartOffsetReader,
    metrics: &BrokerMetrics,
    seen_labels: &mut HashSet<ShareGroupLabel>,
) {
    for (topic_id, partition_ids) in initialized {
        let Some(topic) = topic_name_for(topic_id) else {
            continue;
        };
        for partition in partition_ids {
            let Some(offsets) = offset_reader.read_offsets(&topic, partition).await else {
                tracing::debug!(
                    group_id,
                    %topic_id,
                    topic = %topic,
                    partition,
                    "share-group backlog offsets unavailable for initialized partition"
                );
                continue;
            };
            let spso = start_offset_reader
                .read_start_offset(group_id, topic_id, partition)
                .await;
            let backlog = effective_backlog(offsets.high_watermark, spso, offsets.log_start);
            let label = ShareGroupLabel {
                group_id: group_id.to_string(),
                topic: topic.clone(),
                partition,
            };
            metrics
                .share_group_backlog
                .get_or_create(&label)
                .set(backlog);
            seen_labels.insert(label);
        }
    }
}

fn remove_backlog_labels(
    metrics: &BrokerMetrics,
    labels: impl IntoIterator<Item = ShareGroupLabel>,
) {
    for label in labels {
        let _ = metrics.share_group_backlog.remove(&label);
    }
}

fn clear_backlog_labels(metrics: &BrokerMetrics, labels: &mut HashSet<ShareGroupLabel>) {
    remove_backlog_labels(metrics, labels.drain());
}

fn finish_backlog_tick(
    metrics: &BrokerMetrics,
    last_seen_labels: &mut HashSet<ShareGroupLabel>,
    seen_labels: HashSet<ShareGroupLabel>,
) {
    remove_backlog_labels(metrics, last_seen_labels.difference(&seen_labels).cloned());
    *last_seen_labels = seen_labels;
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use assert2::assert;
    use crabka_metadata::BrokerEndpoint;
    use crabka_protocol::owned::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    use super::*;

    #[derive(Default)]
    struct FakeOffsetReader {
        offsets: HashMap<(String, i32), PartitionBacklogOffsets>,
        reads: Mutex<Vec<(String, i32)>>,
    }

    impl FakeOffsetReader {
        fn with_offset(
            mut self,
            topic: &str,
            partition: i32,
            high_watermark: i64,
            log_start: i64,
        ) -> Self {
            self.offsets.insert(
                (topic.to_string(), partition),
                PartitionBacklogOffsets {
                    high_watermark,
                    log_start,
                },
            );
            self
        }

        fn reads(&self) -> Vec<(String, i32)> {
            self.reads.lock().expect("reads mutex poisoned").clone()
        }
    }

    impl BacklogOffsetReader for FakeOffsetReader {
        fn read_offsets<'a>(&'a self, topic: &'a str, partition: i32) -> OffsetRead<'a> {
            Box::pin(async move {
                self.reads
                    .lock()
                    .expect("reads mutex poisoned")
                    .push((topic.to_string(), partition));
                self.offsets.get(&(topic.to_string(), partition)).copied()
            })
        }
    }

    #[derive(Default)]
    struct FakeStartOffsetReader {
        offsets: HashMap<(String, Uuid, i32), i64>,
    }

    impl FakeStartOffsetReader {
        fn with_start_offset(
            mut self,
            group_id: &str,
            topic_id: Uuid,
            partition: i32,
            start_offset: i64,
        ) -> Self {
            self.offsets
                .insert((group_id.to_string(), topic_id, partition), start_offset);
            self
        }
    }

    impl ShareStartOffsetReader for FakeStartOffsetReader {
        fn read_start_offset<'a>(
            &'a self,
            group_id: &'a str,
            topic_id: Uuid,
            partition: i32,
        ) -> StartOffsetRead<'a> {
            Box::pin(async move {
                self.offsets
                    .get(&(group_id.to_string(), topic_id, partition))
                    .copied()
                    .unwrap_or(-1)
            })
        }
    }

    async fn encode_metrics(metrics: &BrokerMetrics) -> String {
        let mut buf = String::new();
        let registry = metrics.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &registry).expect("metrics encode");
        buf
    }

    #[test]
    fn initialized_uses_spso() {
        assert!(effective_backlog(100, 40, 0) == 60);
    }

    #[test]
    fn uninitialized_uses_log_start_full_backlog() {
        // spso = -1 (uninitialized) => full available backlog, not -1 or 0.
        assert!(effective_backlog(100, -1, 10) == 90);
    }

    #[test]
    fn drained_is_zero() {
        assert!(effective_backlog(100, 100, 0) == 0);
    }

    #[test]
    fn negative_backlog_clamps_to_zero() {
        // Non-atomic reads can momentarily sample spso > hwm.
        assert!(effective_backlog(100, 120, 0) == 0);
    }

    #[test]
    fn log_start_past_hwm_clamps_to_zero() {
        assert!(effective_backlog(100, -1, 120) == 0);
    }

    #[test]
    fn extreme_offsets_saturate_instead_of_overflowing() {
        assert!(effective_backlog(i64::MAX, i64::MIN, i64::MIN) == i64::MAX);
    }

    #[tokio::test]
    async fn poller_uses_offset_reader_for_non_local_partition() {
        let topic_id = Uuid::from_u128(1);
        let offset_reader = FakeOffsetReader::default().with_offset("remote-topic", 2, 70, 10);
        let start_offset_reader =
            FakeStartOffsetReader::default().with_start_offset("remote-group", topic_id, 2, 25);
        let metrics = BrokerMetrics::new();
        let mut seen_labels = HashSet::new();

        poll_group_backlog_from_metadata(
            "remote-group",
            vec![(topic_id, vec![2])],
            |resolved_topic_id| (resolved_topic_id == topic_id).then(|| "remote-topic".to_string()),
            &offset_reader,
            &start_offset_reader,
            &metrics,
            &mut seen_labels,
        )
        .await;

        assert!(offset_reader.reads() == vec![("remote-topic".to_string(), 2)]);
        assert!(seen_labels.contains(&ShareGroupLabel {
            group_id: "remote-group".to_string(),
            topic: "remote-topic".to_string(),
            partition: 2,
        }));
        let encoded = encode_metrics(&metrics).await;
        assert!(
            encoded.contains(
                "crabka_broker_share_group_backlog{group_id=\"remote-group\",topic=\"remote-topic\",partition=\"2\"} 45"
            ),
            "missing remote backlog gauge in:\n{encoded}"
        );
    }

    #[test]
    fn backlog_list_offsets_request_asks_for_latest_then_earliest() {
        let request = build_backlog_list_offsets_request("remote-topic", 2);

        assert!(request.replica_id == -1);
        assert!(
            request.topics
                == vec![ListOffsetsTopic {
                    name: "remote-topic".to_string(),
                    partitions: vec![
                        ListOffsetsPartition {
                            partition_index: 2,
                            timestamp: LIST_OFFSETS_LATEST_TIMESTAMP,
                            ..ListOffsetsPartition::default()
                        },
                        ListOffsetsPartition {
                            partition_index: 2,
                            timestamp: LIST_OFFSETS_EARLIEST_TIMESTAMP,
                            ..ListOffsetsPartition::default()
                        },
                    ],
                    ..ListOffsetsTopic::default()
                }]
        );
    }

    #[test]
    fn list_offsets_response_extracts_remote_backlog_offsets() {
        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: "remote-topic".to_string(),
                partitions: vec![
                    ListOffsetsPartitionResponse {
                        partition_index: 2,
                        error_code: codes::NONE,
                        offset: 70,
                        ..ListOffsetsPartitionResponse::default()
                    },
                    ListOffsetsPartitionResponse {
                        partition_index: 2,
                        error_code: codes::NONE,
                        offset: 10,
                        ..ListOffsetsPartitionResponse::default()
                    },
                ],
                ..ListOffsetsTopicResponse::default()
            }],
            ..ListOffsetsResponse::default()
        };

        assert!(
            partition_offsets_from_list_offsets_response("remote-topic", 2, &response)
                == Some(PartitionBacklogOffsets {
                    high_watermark: 70,
                    log_start: 10,
                })
        );
    }

    #[test]
    fn list_offsets_response_rejects_error_rows() {
        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: "remote-topic".to_string(),
                partitions: vec![
                    ListOffsetsPartitionResponse {
                        partition_index: 2,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        offset: -1,
                        ..ListOffsetsPartitionResponse::default()
                    },
                    ListOffsetsPartitionResponse {
                        partition_index: 2,
                        error_code: codes::NONE,
                        offset: 10,
                        ..ListOffsetsPartitionResponse::default()
                    },
                ],
                ..ListOffsetsTopicResponse::default()
            }],
            ..ListOffsetsResponse::default()
        };

        assert!(
            partition_offsets_from_list_offsets_response("remote-topic", 2, &response).is_none()
        );
    }

    #[test]
    fn broker_endpoint_prefers_inter_broker_listener() {
        let broker = BrokerRegistrationRecord {
            node_id: NodeId(2),
            broker_epoch: 9,
            incarnation_id: Uuid::nil(),
            host: "legacy-host".to_string(),
            port: 9092,
            rack: None,
            endpoints: vec![BrokerEndpoint {
                name: "INTERNAL".to_string(),
                host: "internal-host".to_string(),
                port: 19092,
                protocol: crabka_security::ListenerProtocol::Plaintext,
            }],
        };

        assert!(
            resolve_broker_endpoint(&broker, "INTERNAL") == ("internal-host".to_string(), 19092)
        );
        assert!(resolve_broker_endpoint(&broker, "MISSING") == ("legacy-host".to_string(), 9092));
    }

    #[tokio::test]
    async fn stale_backlog_series_are_removed_when_not_seen_again() {
        let topic_id = Uuid::from_u128(2);
        let offset_reader = FakeOffsetReader::default().with_offset("gone-topic", 0, 9, 0);
        let start_offset_reader = FakeStartOffsetReader::default();
        let metrics = BrokerMetrics::new();
        let mut previous_seen_labels = HashSet::new();

        poll_group_backlog_from_metadata(
            "stale-group",
            vec![(topic_id, vec![0])],
            |resolved_topic_id| (resolved_topic_id == topic_id).then(|| "gone-topic".to_string()),
            &offset_reader,
            &start_offset_reader,
            &metrics,
            &mut previous_seen_labels,
        )
        .await;
        assert!(encode_metrics(&metrics).await.contains(
            "crabka_broker_share_group_backlog{group_id=\"stale-group\",topic=\"gone-topic\",partition=\"0\"} 9"
        ));

        finish_backlog_tick(&metrics, &mut previous_seen_labels, HashSet::new());

        let encoded = encode_metrics(&metrics).await;
        assert!(
            !encoded.contains(
                "crabka_broker_share_group_backlog{group_id=\"stale-group\",topic=\"gone-topic\",partition=\"0\"}"
            ),
            "stale backlog gauge remained in:\n{encoded}"
        );
    }

    #[tokio::test]
    async fn label_hygiene_removes_only_departed_series() {
        let metrics = BrokerMetrics::new();
        let kept = ShareGroupLabel {
            group_id: "label-group".to_string(),
            topic: "kept-topic".to_string(),
            partition: 0,
        };
        let removed = ShareGroupLabel {
            group_id: "label-group".to_string(),
            topic: "removed-topic".to_string(),
            partition: 1,
        };
        metrics.share_group_backlog.get_or_create(&kept).set(7);
        metrics.share_group_backlog.get_or_create(&removed).set(9);
        let mut last_seen_labels = HashSet::from([kept.clone(), removed]);

        finish_backlog_tick(&metrics, &mut last_seen_labels, HashSet::from([kept]));

        let encoded = encode_metrics(&metrics).await;
        assert!(encoded.contains(
            "crabka_broker_share_group_backlog{group_id=\"label-group\",topic=\"kept-topic\",partition=\"0\"} 7"
        ));
        assert!(
            !encoded.contains(
                "crabka_broker_share_group_backlog{group_id=\"label-group\",topic=\"removed-topic\",partition=\"1\"}"
            ),
            "departed backlog gauge remained in:\n{encoded}"
        );
        assert!(
            last_seen_labels
                == HashSet::from([ShareGroupLabel {
                    group_id: "label-group".to_string(),
                    topic: "kept-topic".to_string(),
                    partition: 0,
                }])
        );
    }

    #[tokio::test]
    async fn coordinator_gate_clears_owned_backlog_series() {
        let metrics = BrokerMetrics::new();
        let label = ShareGroupLabel {
            group_id: "handoff-group".to_string(),
            topic: "handoff-topic".to_string(),
            partition: 0,
        };
        metrics.share_group_backlog.get_or_create(&label).set(11);
        let mut last_seen_labels = HashSet::from([label]);

        assert!(should_skip_backlog_tick(
            false,
            &metrics,
            &mut last_seen_labels
        ));

        assert!(last_seen_labels.is_empty());
        let encoded = encode_metrics(&metrics).await;
        assert!(
            !encoded.contains("crabka_broker_share_group_backlog"),
            "non-coordinator backlog gauge remained in:\n{encoded}"
        );
    }

    #[tokio::test]
    async fn coordinator_gate_keeps_leader_series_for_polling() {
        let metrics = BrokerMetrics::new();
        let label = ShareGroupLabel {
            group_id: "leader-group".to_string(),
            topic: "leader-topic".to_string(),
            partition: 0,
        };
        metrics.share_group_backlog.get_or_create(&label).set(13);
        let mut last_seen_labels = HashSet::from([label]);

        assert!(!should_skip_backlog_tick(
            true,
            &metrics,
            &mut last_seen_labels
        ));

        let encoded = encode_metrics(&metrics).await;
        assert!(encoded.contains(
            "crabka_broker_share_group_backlog{group_id=\"leader-group\",topic=\"leader-topic\",partition=\"0\"} 13"
        ));
    }
}
