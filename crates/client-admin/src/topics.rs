//! Topic CRUD wrappers.

use std::collections::{BTreeMap, HashSet};

use crabka_protocol::{
    owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
        create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        describe_cluster_request::DescribeClusterRequest,
        list_partition_reassignments_request::ListPartitionReassignmentsRequest,
        list_partition_reassignments_response::ListPartitionReassignmentsResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use crabka_units::{Time, convert::TimeExt as _};
use uuid::Uuid;

use crate::{AdminClient, AdminError, KafkaError, NOT_CONTROLLER, kafka_error_if};

#[derive(Debug, Clone)]
pub struct CreateTopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replicas: i32,
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicOutcome {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsOp {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsOutcome {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
    pub low_watermark: i64,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsOp {
    pub name: String,
    pub new_total_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicMetadata {
    pub controller_id: i32,
    pub topics: Vec<TopicMetadataEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadataEntry {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub error: Option<KafkaError>,
}

/// Result of converging one topic's partition assignments to a replication
/// factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicReplicationStatus {
    /// Every partition already has the requested number of replicas.
    InSync,
    /// Kafka is still completing an earlier reassignment for the topic.
    ReassignmentInProgress,
    /// A reassignment for every out-of-sync partition was accepted.
    ReassignmentSubmitted,
}

impl AdminClient {
    /// Metadata for the named topics. Pass an empty slice to fetch all
    /// topics, per Kafka semantics.
    ///
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        let req = build_metadata(topics);
        let resp = self.conn.send(req).await?;
        Ok(parse_metadata(resp))
    }

    /// Converges every partition of `topic` to `replication_factor` through
    /// `AlterPartitionReassignments`.
    ///
    /// Existing replicas are retained where possible. Added replicas rotate
    /// across the broker list, and decreases preserve the leading replica
    /// order. An existing reassignment is never overwritten.
    ///
    /// # Errors
    /// Returns an error when the factor is invalid for the live broker set,
    /// metadata cannot be read, Kafka rejects a reassignment, or transport I/O
    /// fails.
    pub async fn reconcile_topic_replication_factor(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<TopicReplicationStatus, AdminError> {
        if self.conn.uses_controller_bootstrap() {
            return Err(AdminError::Broker {
                api: "ControllerEndpoint",
                code: 115,
                name: "UNSUPPORTED_ENDPOINT_TYPE",
                message: Some(
                    "replication-factor reconciliation requires a broker bootstrap endpoint".into(),
                ),
            });
        }
        // Replica selection depends on the controller's authoritative
        // heartbeat registry. Connect to the active controller before reading
        // Metadata so dead-but-still-registered brokers are not candidates.
        self.refresh_controller_connection().await?;
        let first = self
            .reconcile_topic_replication_factor_once(topic, replication_factor, timeout)
            .await;
        if !matches!(
            first,
            Err(AdminError::Broker {
                code: NOT_CONTROLLER,
                ..
            })
        ) {
            return first;
        }

        self.refresh_controller_connection().await?;
        match self
            .reconcile_topic_replication_factor_once(topic, replication_factor, timeout)
            .await
        {
            Err(AdminError::Broker {
                code: NOT_CONTROLLER,
                ..
            }) => Err(AdminError::NotControllerExhausted),
            result => result,
        }
    }

    async fn reconcile_topic_replication_factor_once(
        &mut self,
        topic: &str,
        replication_factor: i32,
        timeout: Time,
    ) -> Result<TopicReplicationStatus, AdminError> {
        let ongoing: ListPartitionReassignmentsResponse = self
            .conn
            .send(ListPartitionReassignmentsRequest::default())
            .await?;
        broker_error(
            "ListPartitionReassignments",
            ongoing.error_code,
            ongoing.error_message,
        )?;
        if ongoing.topics.iter().any(|entry| entry.name == topic) {
            return Ok(TopicReplicationStatus::ReassignmentInProgress);
        }

        // The public entrypoint reconnects to the active controller before
        // calling this method, so DescribeCluster carries that controller's
        // authoritative heartbeat/fencing state.
        let cluster = self
            .conn
            .send_at_least(
                DescribeClusterRequest {
                    include_fenced_brokers: true,
                    ..Default::default()
                },
                2,
            )
            .await?;
        broker_error("DescribeCluster", cluster.error_code, cluster.error_message)?;
        let eligible_brokers = cluster
            .brokers
            .iter()
            .filter(|broker| !broker.is_fenced)
            .map(|broker| broker.broker_id)
            .collect::<Vec<_>>();

        let metadata: MetadataResponse = self.conn.send(build_metadata(&[topic])).await?;
        let Some(request) = build_replication_factor_reassignment(
            &metadata,
            &eligible_brokers,
            topic,
            replication_factor,
            timeout,
        )?
        else {
            return Ok(TopicReplicationStatus::InSync);
        };
        let response: AlterPartitionReassignmentsResponse = self.conn.send(request).await?;
        broker_error(
            "AlterPartitionReassignments",
            response.error_code,
            response.error_message,
        )?;
        if let Some(error) = response.responses.into_iter().find_map(|topic_response| {
            topic_response
                .partitions
                .into_iter()
                .find(|partition| partition.error_code != 0)
        }) {
            return Err(AdminError::Broker {
                api: "AlterPartitionReassignments",
                code: error.error_code,
                name: crate::kafka_error_name(error.error_code),
                message: error.error_message,
            });
        }
        Ok(TopicReplicationStatus::ReassignmentSubmitted)
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout: Time,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        let first = {
            let req = build_create_topics(specs, timeout);
            let resp = self.conn.send(req).await?;
            parse_create_topics(resp)
        };
        if !any_not_controller(&first, |o| o.error.as_ref()) {
            return Ok(first);
        }
        self.refresh_controller_connection().await?;
        let second = {
            let req = build_create_topics(specs, timeout);
            let resp = self.conn.send(req).await?;
            parse_create_topics(resp)
        };
        if any_not_controller(&second, |o| o.error.as_ref()) {
            return Err(AdminError::NotControllerExhausted);
        }
        Ok(second)
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout: Time,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        // Populate BOTH fields so the request works regardless of the
        // negotiated protocol version: `topic_names` is the legacy field
        // (v0-v5) and `topics` is the v6+ replacement. The
        // `ApiVersionTable`-driven encoder picks the version-relevant
        // field and ignores the other.
        let build = || DeleteTopicsRequest {
            topic_names: names.iter().map(|s| (*s).to_string()).collect(),
            topics: names
                .iter()
                .map(|s| DeleteTopicState {
                    name: Some((*s).to_string()),
                    topic_id: ProtoUuid::ZERO,
                    ..Default::default()
                })
                .collect(),
            timeout_ms: timeout.millis_i32(),
            ..Default::default()
        };
        let first = parse_delete_topics(self.conn.send(build()).await?);
        if !any_not_controller(&first, |o| o.error.as_ref()) {
            return Ok(first);
        }
        self.refresh_controller_connection().await?;
        let second = parse_delete_topics(self.conn.send(build()).await?);
        if any_not_controller(&second, |o| o.error.as_ref()) {
            return Err(AdminError::NotControllerExhausted);
        }
        Ok(second)
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        timeout: Time,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        let build = || CreatePartitionsRequest {
            topics: ops
                .iter()
                .map(|o| CreatePartitionsTopic {
                    name: o.name.clone(),
                    count: o.new_total_count,
                    assignments: None,
                    ..Default::default()
                })
                .collect(),
            timeout_ms: timeout.millis_i32(),
            validate_only: false,
            ..Default::default()
        };
        let first = parse_create_partitions(self.conn.send(build()).await?);
        if !any_not_controller(&first, |o| o.error.as_ref()) {
            return Ok(first);
        }
        self.refresh_controller_connection().await?;
        let second = parse_create_partitions(self.conn.send(build()).await?);
        if any_not_controller(&second, |o| o.error.as_ref()) {
            return Err(AdminError::NotControllerExhausted);
        }
        Ok(second)
    }

    /// Deletes records below each requested partition offset.
    ///
    /// `offset == -1` follows Kafka `DeleteRecords` semantics and truncates to
    /// the partition high watermark. The returned `low_watermark` is the
    /// broker's resulting log-start offset for that partition.
    ///
    /// # Errors
    ///
    /// Returns an [`AdminError`] when metadata lookup, leader routing, transport,
    /// or protocol handling fails. Kafka partition-level failures remain in the
    /// returned [`DeleteRecordsOutcome::error_code`].
    pub async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        timeout: Time,
    ) -> Result<Vec<DeleteRecordsOutcome>, AdminError> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }

        let mut outcomes = Vec::new();
        let groups = self
            .delete_records_leader_groups(ops, &mut outcomes)
            .await?;
        for (endpoint, leader_ops) in groups {
            self.reconnect(&endpoint).await?;
            let req = build_delete_records(&leader_ops, timeout);
            let resp = self.conn.send(req).await?;
            outcomes.extend(parse_delete_records(resp));
        }
        Ok(outcomes)
    }

    /// Fetches Metadata, finds the controller's `host:port`, and replaces
    /// `self.conn` with a connection to it. The per-method `NOT_CONTROLLER`
    /// retry paths above use it.
    pub(crate) async fn refresh_controller_connection(&mut self) -> Result<(), AdminError> {
        if self.conn.uses_controller_bootstrap() {
            return self.conn.rebootstrap().await;
        }
        let md_resp = self.conn.send(build_metadata(&[])).await?;
        let Some(controller_addr) = controller_endpoint(&md_resp) else {
            // In-process/test brokers can advertise port 0 while the
            // bootstrap address still contains the actual bound port. Reuse
            // that known-good address instead of attempting `host:0`.
            if controller_requires_bootstrap_fallback(&md_resp) {
                return self.conn.rebootstrap().await;
            }
            return Err(AdminError::NotControllerExhausted);
        };
        self.reconnect(&controller_addr).await
    }

    async fn delete_records_leader_groups(
        &mut self,
        ops: &[DeleteRecordsOp],
        outcomes: &mut Vec<DeleteRecordsOutcome>,
    ) -> Result<BTreeMap<String, Vec<DeleteRecordsOp>>, AdminError> {
        let mut topic_names = ops.iter().map(|op| op.topic.as_str()).collect::<Vec<_>>();
        topic_names.sort_unstable();
        topic_names.dedup();

        let metadata = self.conn.send(build_metadata(&topic_names)).await?;
        let broker_endpoints = metadata
            .brokers
            .iter()
            .map(|broker| {
                let endpoint = if broker.port > 0 {
                    format!("{}:{}", broker.host, broker.port)
                } else {
                    self.bootstrap_addrs
                        .first()
                        .cloned()
                        .unwrap_or_else(|| format!("{}:{}", broker.host, broker.port))
                };
                (broker.node_id, endpoint)
            })
            .collect::<BTreeMap<_, _>>();
        let mut groups = BTreeMap::<String, Vec<DeleteRecordsOp>>::new();

        for op in ops {
            let Some(topic) = metadata
                .topics
                .iter()
                .find(|topic| topic.name.as_deref() == Some(op.topic.as_str()))
            else {
                outcomes.push(delete_records_error_outcome(op, 3));
                continue;
            };

            if topic.error_code != 0 {
                outcomes.push(delete_records_error_outcome(op, topic.error_code));
                continue;
            }

            let Some(partition) = topic
                .partitions
                .iter()
                .find(|partition| partition.partition_index == op.partition)
            else {
                outcomes.push(delete_records_error_outcome(op, 3));
                continue;
            };

            if partition.error_code != 0 {
                outcomes.push(delete_records_error_outcome(op, partition.error_code));
                continue;
            }

            let Some(endpoint) = broker_endpoints.get(&partition.leader_id) else {
                return Err(AdminError::Protocol(format!(
                    "no broker endpoint for DeleteRecords leader {}",
                    partition.leader_id
                )));
            };
            groups.entry(endpoint.clone()).or_default().push(op.clone());
        }

        Ok(groups)
    }
}

fn any_not_controller<T, F: Fn(&T) -> Option<&KafkaError>>(items: &[T], get_err: F) -> bool {
    items
        .iter()
        .any(|o| matches!(get_err(o), Some(e) if e.code == NOT_CONTROLLER))
}

fn build_metadata(topics: &[&str]) -> MetadataRequest {
    MetadataRequest {
        topics: if topics.is_empty() {
            None
        } else {
            Some(
                topics
                    .iter()
                    .map(|n| MetadataRequestTopic {
                        topic_id: ProtoUuid::ZERO,
                        name: Some((*n).to_string()),
                        ..Default::default()
                    })
                    .collect(),
            )
        },
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
        ..Default::default()
    }
}

fn broker_error(api: &'static str, code: i16, message: Option<String>) -> Result<(), AdminError> {
    if code == 0 {
        return Ok(());
    }
    Err(AdminError::Broker {
        api,
        code,
        name: crate::kafka_error_name(code),
        message,
    })
}

fn build_replication_factor_reassignment(
    metadata: &MetadataResponse,
    eligible_brokers: &[i32],
    topic_name: &str,
    replication_factor: i32,
    timeout: Time,
) -> Result<Option<AlterPartitionReassignmentsRequest>, AdminError> {
    broker_error("Metadata", metadata.error_code, None)?;
    let desired = usize::try_from(replication_factor).map_err(|_| {
        AdminError::Protocol("replication factor must be a positive integer".into())
    })?;
    if desired == 0 {
        return Err(AdminError::Protocol(
            "replication factor must be a positive integer".into(),
        ));
    }

    let mut brokers = eligible_brokers.to_vec();
    brokers.sort_unstable();
    brokers.dedup();
    if desired > brokers.len() {
        return Err(AdminError::Broker {
            api: "AlterPartitionReassignments",
            code: 38,
            name: crate::kafka_error_name(38),
            message: Some(format!(
                "replication factor {replication_factor} exceeds live broker count {}",
                brokers.len()
            )),
        });
    }

    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(topic_name))
        .ok_or_else(|| AdminError::Broker {
            api: "Metadata",
            code: 3,
            name: crate::kafka_error_name(3),
            message: Some(format!("topic {topic_name:?} is absent from metadata")),
        })?;
    broker_error("Metadata", topic.error_code, None)?;

    let available = brokers.iter().copied().collect::<HashSet<_>>();
    let mut partitions = Vec::new();
    for (rotation, partition) in topic.partitions.iter().enumerate() {
        broker_error("Metadata", partition.error_code, None)?;
        let mut selected = HashSet::with_capacity(desired);
        let mut target = partition
            .replica_nodes
            .iter()
            .copied()
            .filter(|broker| available.contains(broker) && selected.insert(*broker))
            .take(desired)
            .collect::<Vec<_>>();
        for offset in 0..brokers.len() {
            if target.len() == desired {
                break;
            }
            let broker = brokers[(rotation + offset) % brokers.len()];
            if selected.insert(broker) {
                target.push(broker);
            }
        }
        if target != partition.replica_nodes {
            partitions.push(ReassignablePartition {
                partition_index: partition.partition_index,
                replicas: Some(target),
                ..Default::default()
            });
        }
    }
    if partitions.is_empty() {
        return Ok(None);
    }

    Ok(Some(AlterPartitionReassignmentsRequest {
        timeout_ms: timeout.millis_i32(),
        allow_replication_factor_change: true,
        topics: vec![ReassignableTopic {
            name: topic_name.to_string(),
            partitions,
            ..Default::default()
        }],
        ..Default::default()
    }))
}

fn build_create_topics(specs: &[CreateTopicSpec], timeout: Time) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics: specs
            .iter()
            .map(|s| CreatableTopic {
                name: s.name.clone(),
                num_partitions: s.partitions,
                replication_factor: i16::try_from(s.replicas).unwrap_or(i16::MAX),
                assignments: Vec::new(),
                configs: s
                    .configs
                    .iter()
                    .map(|(k, v)| CreatableTopicConfig {
                        name: k.clone(),
                        value: Some(v.clone()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        timeout_ms: timeout.millis_i32(),
        validate_only: false,
        ..Default::default()
    }
}

fn build_delete_records(ops: &[DeleteRecordsOp], timeout: Time) -> DeleteRecordsRequest {
    let mut topics = BTreeMap::<String, Vec<DeleteRecordsPartition>>::new();
    for op in ops {
        topics
            .entry(op.topic.clone())
            .or_default()
            .push(DeleteRecordsPartition {
                partition_index: op.partition,
                offset: op.offset,
                ..Default::default()
            });
    }

    DeleteRecordsRequest {
        topics: topics
            .into_iter()
            .map(|(name, partitions)| DeleteRecordsTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect(),
        timeout_ms: timeout.millis_i32(),
        ..Default::default()
    }
}

fn parse_create_topics(
    resp: <CreateTopicsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Vec<CreateTopicOutcome> {
    resp.topics
        .into_iter()
        .map(|t| CreateTopicOutcome {
            name: t.name,
            topic_id: proto_uuid_to_opt(t.topic_id),
            error: kafka_error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_delete_topics(
    resp: <DeleteTopicsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Vec<DeleteTopicOutcome> {
    resp.responses
        .into_iter()
        .map(|t| DeleteTopicOutcome {
            name: t.name.unwrap_or_default(),
            error: kafka_error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_create_partitions(
    resp: <CreatePartitionsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Vec<CreatePartitionsOutcome> {
    resp.results
        .into_iter()
        .map(|t| CreatePartitionsOutcome {
            name: t.name,
            error: kafka_error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_delete_records(
    resp: <DeleteRecordsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Vec<DeleteRecordsOutcome> {
    resp.topics
        .into_iter()
        .flat_map(|topic| {
            let topic_name = topic.name;
            topic
                .partitions
                .into_iter()
                .map(move |partition| DeleteRecordsOutcome {
                    topic: topic_name.clone(),
                    partition: partition.partition_index,
                    error_code: partition.error_code,
                    low_watermark: partition.low_watermark,
                })
        })
        .collect()
}

fn delete_records_error_outcome(op: &DeleteRecordsOp, error_code: i16) -> DeleteRecordsOutcome {
    DeleteRecordsOutcome {
        topic: op.topic.clone(),
        partition: op.partition,
        error_code,
        low_watermark: -1,
    }
}

fn parse_metadata(
    resp: <MetadataRequest as crabka_protocol::ProtocolRequest>::Response,
) -> TopicMetadata {
    let topics = resp
        .topics
        .into_iter()
        .map(|t| {
            let partition_count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
            let replication_factor = i32::from(t.partitions.first().map_or(0, |p| {
                i16::try_from(p.replica_nodes.len()).unwrap_or(i16::MAX)
            }));
            TopicMetadataEntry {
                name: t.name.unwrap_or_default(),
                topic_id: proto_uuid_to_opt(t.topic_id),
                partition_count,
                replication_factor,
                error: kafka_error_if(t.error_code, None),
            }
        })
        .collect();
    TopicMetadata {
        controller_id: resp.controller_id,
        topics,
    }
}

fn controller_endpoint(
    resp: &<MetadataRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Option<String> {
    let id = resp.controller_id;
    resp.brokers
        .iter()
        .find(|b| b.node_id == id && !b.host.is_empty() && b.port > 0)
        .map(|b| format!("{}:{}", b.host, b.port))
}

fn controller_requires_bootstrap_fallback(
    resp: &<MetadataRequest as crabka_protocol::ProtocolRequest>::Response,
) -> bool {
    resp.brokers
        .iter()
        .any(|broker| broker.node_id == resp.controller_id && broker.port <= 0)
}

fn proto_uuid_to_opt(u: ProtoUuid) -> Option<Uuid> {
    if u == ProtoUuid::ZERO {
        None
    } else {
        Some(Uuid::from_bytes(u.0))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crabka_protocol::{
        UnknownTaggedFields,
        owned::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
    };

    use super::*;

    fn reassignment_metadata(assignments: &[&[i32]]) -> MetadataResponse {
        MetadataResponse {
            brokers: (1..=3)
                .map(|node_id| MetadataResponseBroker {
                    node_id,
                    ..Default::default()
                })
                .collect(),
            topics: vec![MetadataResponseTopic {
                name: Some("orders".into()),
                partitions: assignments
                    .iter()
                    .enumerate()
                    .map(|(partition_index, replicas)| MetadataResponsePartition {
                        partition_index: i32::try_from(partition_index)
                            .expect("test partition index fits i32"),
                        replica_nodes: replicas.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn replication_factor_reassignment_preserves_and_rotates_replicas() {
        let cases = [
            (
                "increase",
                reassignment_metadata(&[&[1], &[2]]),
                2,
                vec![(0, vec![1, 2]), (1, vec![2, 3])],
            ),
            (
                "decrease",
                reassignment_metadata(&[&[1, 2, 3], &[2, 3, 1]]),
                2,
                vec![(0, vec![1, 2]), (1, vec![2, 3])],
            ),
        ];

        for (case, metadata, replication_factor, expected_partitions) in cases {
            let actual = build_replication_factor_reassignment(
                &metadata,
                &[1, 2, 3],
                "orders",
                replication_factor,
                crabka_units::secs(5),
            )
            .unwrap();
            let expected = Some(AlterPartitionReassignmentsRequest {
                timeout_ms: 5_000,
                allow_replication_factor_change: true,
                topics: vec![ReassignableTopic {
                    name: "orders".into(),
                    partitions: expected_partitions
                        .into_iter()
                        .map(|(partition_index, replicas)| ReassignablePartition {
                            partition_index,
                            replicas: Some(replicas),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            });
            assert2::assert!(actual == expected, "case {case}");
        }
    }

    #[test]
    fn replication_factor_reassignment_rejects_factor_above_broker_count() {
        let error = build_replication_factor_reassignment(
            &reassignment_metadata(&[&[1]]),
            &[1, 2, 3],
            "orders",
            4,
            crabka_units::secs(5),
        )
        .unwrap_err();

        assert2::assert!(matches!(
            error,
            AdminError::Broker {
                api: "AlterPartitionReassignments",
                code: 38,
                name: "INVALID_REPLICATION_FACTOR",
                ..
            }
        ));
    }

    #[test]
    fn replication_factor_reassignment_replaces_fenced_replica() {
        let actual = build_replication_factor_reassignment(
            &reassignment_metadata(&[&[1, 2]]),
            &[1, 3],
            "orders",
            2,
            crabka_units::secs(5),
        )
        .unwrap()
        .expect("fenced replica requires reassignment");

        assert2::assert!(actual.topics[0].partitions[0].replicas == Some(vec![1, 3]));
    }

    #[test]
    fn build_create_topics_one_spec() {
        let req = build_create_topics(
            &[CreateTopicSpec {
                name: "foo".into(),
                partitions: 3,
                replicas: 1,
                configs: BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]),
            }],
            crabka_units::secs(5),
        );
        assert2::assert!(
            req == CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "foo".to_string(),
                    num_partitions: 3,
                    replication_factor: 1,
                    assignments: vec![],
                    configs: vec![CreatableTopicConfig {
                        name: "retention.ms".to_string(),
                        value: Some("60000".to_string()),
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                timeout_ms: 5_000,
                validate_only: false,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn build_delete_records_groups_partitions_by_topic() {
        let req = build_delete_records(
            &[
                DeleteRecordsOp {
                    topic: "beta".to_string(),
                    partition: 1,
                    offset: -1,
                },
                DeleteRecordsOp {
                    topic: "alpha".to_string(),
                    partition: 0,
                    offset: 50,
                },
                DeleteRecordsOp {
                    topic: "alpha".to_string(),
                    partition: 2,
                    offset: 75,
                },
            ],
            crabka_units::secs(5),
        );

        assert2::assert!(
            req == DeleteRecordsRequest {
                topics: vec![
                    DeleteRecordsTopic {
                        name: "alpha".to_string(),
                        partitions: vec![
                            DeleteRecordsPartition {
                                partition_index: 0,
                                offset: 50,
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                            DeleteRecordsPartition {
                                partition_index: 2,
                                offset: 75,
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    DeleteRecordsTopic {
                        name: "beta".to_string(),
                        partitions: vec![DeleteRecordsPartition {
                            partition_index: 1,
                            offset: -1,
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                ],
                timeout_ms: 5_000,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn parse_delete_records_flattens_partition_errors() {
        use crabka_protocol::owned::delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
        };

        let resp = DeleteRecordsResponse {
            topics: vec![DeleteRecordsTopicResult {
                name: "wal".to_string(),
                partitions: vec![
                    DeleteRecordsPartitionResult {
                        partition_index: 0,
                        low_watermark: 50,
                        error_code: 0,
                        ..Default::default()
                    },
                    DeleteRecordsPartitionResult {
                        partition_index: 1,
                        low_watermark: -1,
                        error_code: 1,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let outcomes = parse_delete_records(resp);
        assert_eq!(
            outcomes,
            vec![
                DeleteRecordsOutcome {
                    topic: "wal".to_string(),
                    partition: 0,
                    error_code: 0,
                    low_watermark: 50,
                },
                DeleteRecordsOutcome {
                    topic: "wal".to_string(),
                    partition: 1,
                    error_code: 1,
                    low_watermark: -1,
                },
            ]
        );
    }

    // ── NOT_CONTROLLER retry predicate ─────────────────────────────
    //
    // The full retry pipeline (first response carries NOT_CONTROLLER →
    // refresh controller endpoint → reconnect → second response succeeds)
    // is exercised against a real broker in `tests/round_trip.rs`. The
    // unit tests below lock the two pure pieces — the predicate that
    // decides whether to retry, and the metadata-response → host:port
    // resolver — so a refactor can't silently flip either one.

    /// Spec test name: `not_controller_triggers_one_retry`, the predicate
    /// half. It verifies that `any_not_controller` returns `true` iff at least
    /// one outcome carries the `NOT_CONTROLLER (41)` error code.
    #[test]
    fn any_not_controller_predicate_matches_code_41() {
        let outcomes = vec![
            CreateTopicOutcome {
                name: "a".into(),
                topic_id: None,
                error: None,
            },
            CreateTopicOutcome {
                name: "b".into(),
                topic_id: None,
                error: Some(KafkaError {
                    code: NOT_CONTROLLER,
                    name: "NOT_CONTROLLER",
                    message: None,
                }),
            },
        ];
        assert2::assert!(any_not_controller(&outcomes, |o| o.error.as_ref()));

        let all_ok = vec![CreateTopicOutcome {
            name: "a".into(),
            topic_id: None,
            error: None,
        }];
        assert2::assert!(!any_not_controller(&all_ok, |o| o.error.as_ref()));
    }

    /// Spec test name: `repeated_not_controller_errors_return_exhausted`, the
    /// predicate half. Non-`NOT_CONTROLLER` errors must NOT trigger the retry
    /// path. Only code 41 does. With the integration test, this locks the
    /// retry-eligibility check. If the predicate fired on, for example,
    /// `TOPIC_ALREADY_EXISTS`, callers would see spurious reconnects and
    /// `NotControllerExhausted` returns on real failures.
    #[test]
    fn any_not_controller_ignores_other_errors() {
        let outcomes = vec![CreateTopicOutcome {
            name: "b".into(),
            topic_id: None,
            error: Some(KafkaError {
                code: 36, // TOPIC_ALREADY_EXISTS
                name: "TOPIC_ALREADY_EXISTS",
                message: None,
            }),
        }];
        assert2::assert!(!any_not_controller(&outcomes, |o| o.error.as_ref()));
    }

    // ── controller_endpoint resolver ───────────────────────────────

    /// Spec test name: `connect_walks_bootstrap_list`, the resolver half. The
    /// bootstrap-walking integration coverage itself lives in
    /// `tests/connect.rs`. `controller_endpoint` extracts the `host:port` of
    /// the broker whose `node_id` matches the metadata response's
    /// `controller_id`. This is the address the `NOT_CONTROLLER` retry path
    /// reconnects to.
    #[test]
    fn controller_endpoint_picks_broker_with_matching_node_id() {
        use crabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseBroker};
        let resp = MetadataResponse {
            controller_id: 2,
            brokers: vec![
                MetadataResponseBroker {
                    node_id: 1,
                    host: "h1".into(),
                    port: 9092,
                    rack: None,
                    ..Default::default()
                },
                MetadataResponseBroker {
                    node_id: 2,
                    host: "h2".into(),
                    port: 9093,
                    rack: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let addr = controller_endpoint(&resp);
        assert2::assert!(addr.as_deref() == Some("h2:9093"));
    }

    /// When the controller id does not appear in the broker list, for example
    /// when the cluster is mid-failover, `controller_endpoint` returns `None`.
    /// The retry path maps that to `AdminError::NotControllerExhausted`.
    #[test]
    fn controller_endpoint_returns_none_when_no_match() {
        use crabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseBroker};
        let resp = MetadataResponse {
            controller_id: 99,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "h1".into(),
                port: 9092,
                rack: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert2::assert!(controller_endpoint(&resp).is_none());
    }

    #[test]
    fn controller_endpoint_rejects_non_dialable_ephemeral_port() {
        use crabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseBroker};
        let resp = MetadataResponse {
            controller_id: 1,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: 0,
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(controller_endpoint(&resp).is_none());
        assert2::assert!(controller_requires_bootstrap_fallback(&resp));
    }

    // ── parse_metadata ─────────────────────────────────────────────────
    //
    // `parse_metadata` is the pure response→`TopicMetadata` transformer
    // the live `metadata` RPC delegates to. The tests below feed it
    // synthetic responses and assert the per-topic fields are projected
    // correctly. Covers the error-mapping, uuid-zeroing, and
    // partition/replication-factor count paths.

    #[test]
    fn parse_metadata_carries_through_per_topic_errors() {
        use crabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseTopic};
        let resp = MetadataResponse {
            topics: vec![
                MetadataResponseTopic {
                    name: Some("ok-topic".into()),
                    error_code: 0,
                    ..Default::default()
                },
                MetadataResponseTopic {
                    name: Some("missing".into()),
                    error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let md = parse_metadata(resp);
        assert2::assert!(
            md == TopicMetadata {
                controller_id: -1,
                topics: vec![
                    TopicMetadataEntry {
                        name: "ok-topic".to_string(),
                        topic_id: None,
                        partition_count: 0,
                        replication_factor: 0,
                        error: None,
                    },
                    TopicMetadataEntry {
                        name: "missing".to_string(),
                        topic_id: None,
                        partition_count: 0,
                        replication_factor: 0,
                        error: Some(KafkaError {
                            code: 3,
                            name: "UNKNOWN_TOPIC_OR_PARTITION",
                            message: None,
                        }),
                    },
                ],
            }
        );
    }

    #[test]
    fn parse_metadata_zero_uuid_becomes_none() {
        use crabka_protocol::owned::metadata_response::{MetadataResponse, MetadataResponseTopic};
        let resp = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("foo".into()),
                topic_id: ProtoUuid::ZERO,
                ..Default::default()
            }],
            ..Default::default()
        };
        let md = parse_metadata(resp);
        assert2::assert!(md.topics[0].topic_id.is_none());
    }

    #[test]
    fn parse_metadata_computes_partition_count_and_replication_factor() {
        use crabka_protocol::owned::metadata_response::{
            MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
        };
        let part = MetadataResponsePartition {
            replica_nodes: vec![1, 2],
            ..Default::default()
        };
        let resp = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("foo".into()),
                partitions: vec![part.clone(), part.clone(), part],
                ..Default::default()
            }],
            ..Default::default()
        };
        let md = parse_metadata(resp);
        assert2::assert!(
            (
                md.topics[0].partition_count,
                md.topics[0].replication_factor
            ) == (3, 2)
        );
    }

    // ── parse_create_topics ────────────────────────────────────────────

    #[test]
    fn parse_create_topics_per_topic_error() {
        use crabka_protocol::owned::create_topics_response::{
            CreatableTopicResult, CreateTopicsResponse,
        };
        let resp = CreateTopicsResponse {
            topics: vec![
                CreatableTopicResult {
                    name: "ok".into(),
                    topic_id: ProtoUuid([7; 16]),
                    error_code: 0,
                    error_message: None,
                    ..Default::default()
                },
                CreatableTopicResult {
                    name: "dup".into(),
                    error_code: 36, // TOPIC_ALREADY_EXISTS
                    error_message: Some("already there".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let outcomes = parse_create_topics(resp);
        assert2::assert!(
            outcomes
                == vec![
                    CreateTopicOutcome {
                        name: "ok".to_string(),
                        // Non-zero uuid maps to Some.
                        topic_id: Some(Uuid::from_bytes([7; 16])),
                        error: None,
                    },
                    CreateTopicOutcome {
                        name: "dup".to_string(),
                        topic_id: None,
                        error: Some(KafkaError {
                            code: 36,
                            name: "TOPIC_ALREADY_EXISTS",
                            message: Some("already there".to_string()),
                        }),
                    },
                ]
        );
    }

    // ── parse_delete_topics ────────────────────────────────────────────

    #[test]
    fn parse_delete_topics_handles_missing_name() {
        use crabka_protocol::owned::delete_topics_response::{
            DeletableTopicResult, DeleteTopicsResponse,
        };
        let resp = DeleteTopicsResponse {
            responses: vec![
                DeletableTopicResult {
                    name: None,
                    error_code: 0,
                    ..Default::default()
                },
                DeletableTopicResult {
                    name: Some("named".into()),
                    error_code: 3,
                    error_message: Some("nope".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let outs = parse_delete_topics(resp);
        assert2::assert!(
            outs == vec![
                DeleteTopicOutcome {
                    // `name: None` falls through to `unwrap_or_default()`
                    // → empty string.
                    name: String::new(),
                    error: None,
                },
                DeleteTopicOutcome {
                    name: "named".to_string(),
                    error: Some(KafkaError {
                        code: 3,
                        name: "UNKNOWN_TOPIC_OR_PARTITION",
                        message: Some("nope".to_string()),
                    }),
                },
            ]
        );
    }

    // ── parse_create_partitions ────────────────────────────────────────

    #[test]
    fn parse_create_partitions_per_topic_error() {
        use crabka_protocol::owned::create_partitions_response::{
            CreatePartitionsResponse, CreatePartitionsTopicResult,
        };
        let resp = CreatePartitionsResponse {
            results: vec![
                CreatePartitionsTopicResult {
                    name: "ok".into(),
                    error_code: 0,
                    error_message: None,
                    ..Default::default()
                },
                CreatePartitionsTopicResult {
                    name: "bad".into(),
                    error_code: 37,
                    error_message: Some("bad count".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let outs = parse_create_partitions(resp);
        assert2::assert!(
            outs == vec![
                CreatePartitionsOutcome {
                    name: "ok".to_string(),
                    error: None,
                },
                CreatePartitionsOutcome {
                    name: "bad".to_string(),
                    error: Some(KafkaError {
                        code: 37,
                        name: "INVALID_PARTITIONS",
                        message: Some("bad count".to_string()),
                    }),
                },
            ]
        );
    }
}
