//! Production `ClientFacade` over `crabka_client_core::Client`. Maps
//! each trait method to the corresponding admin RPC via raw
//! `Client::send`, mirroring the ingester pattern.

use async_trait::async_trait;
use crabka_client_core::Client;
use crabka_protocol::owned::alter_partition_reassignments_request::{
    AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
};
use crabka_protocol::owned::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
};
use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use std::collections::BTreeMap;

use crate::executor::phases::{ClientFacade, ConfigOp, PhaseError};
use crate::executor::throttle::ThrottleTargets;
use crate::model::Movement;

/// Kafka admin resource type ids.
const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

/// `IncrementalAlterConfigs` op type ids.
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

const RATE_KEY_LEADER: &str = "leader.replication.throttled.rate";
const RATE_KEY_FOLLOWER: &str = "follower.replication.throttled.rate";
const REPLICAS_KEY_LEADER: &str = "leader.replication.throttled.replicas";
const REPLICAS_KEY_FOLLOWER: &str = "follower.replication.throttled.replicas";

fn check_alter_configs_response(
    resp: &crabka_protocol::owned::incremental_alter_configs_response::IncrementalAlterConfigsResponse,
) -> Result<(), PhaseError> {
    let failures: Vec<String> = resp
        .responses
        .iter()
        .filter(|r| r.error_code != 0)
        .map(|r| {
            let msg = r.error_message.as_deref().unwrap_or("");
            format!(
                "resource `{}` (type={}): error_code={} {}",
                r.resource_name, r.resource_type, r.error_code, msg
            )
        })
        .collect();
    if !failures.is_empty() {
        return Err(PhaseError::Broker(format!(
            "IncrementalAlterConfigs failed: {}",
            failures.join("; ")
        )));
    }
    Ok(())
}

fn check_reassign_response(
    resp: &crabka_protocol::owned::alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
) -> Result<(), PhaseError> {
    if resp.error_code != 0 {
        let msg = resp.error_message.as_deref().unwrap_or("");
        return Err(PhaseError::Broker(format!(
            "AlterPartitionReassignments top-level error_code={} {}",
            resp.error_code, msg
        )));
    }
    let failures: Vec<String> = resp
        .responses
        .iter()
        .flat_map(|t| {
            let topic = t.name.clone();
            t.partitions.iter().filter_map(move |p| {
                if p.error_code == 0 {
                    None
                } else {
                    let msg = p.error_message.as_deref().unwrap_or("");
                    Some(format!(
                        "`{}`/{} error_code={} {}",
                        topic, p.partition_index, p.error_code, msg
                    ))
                }
            })
        })
        .collect();
    if !failures.is_empty() {
        return Err(PhaseError::Broker(format!(
            "AlterPartitionReassignments per-partition failures: {}",
            failures.join("; ")
        )));
    }
    Ok(())
}

pub struct LiveClient {
    pub inner: Client,
}

impl LiveClient {
    #[must_use]
    pub fn new(inner: Client) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ClientFacade for LiveClient {
    async fn alter_throttle_configs(
        &self,
        op: ConfigOp,
        targets: &ThrottleTargets,
        throttle_bytes_per_sec: i64,
    ) -> Result<(), PhaseError> {
        let op_byte = match op {
            ConfigOp::Set => OP_SET,
            ConfigOp::Delete => OP_DELETE,
        };
        let rate_str = throttle_bytes_per_sec.to_string();
        let mut resources: Vec<AlterConfigsResource> = Vec::new();

        // Per-broker rate configs.
        for broker in &targets.leader_brokers {
            resources.push(AlterConfigsResource {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: broker.to_string(),
                configs: vec![AlterableConfig {
                    name: RATE_KEY_LEADER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some(rate_str.clone()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
        for broker in &targets.follower_brokers {
            resources.push(AlterConfigsResource {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: broker.to_string(),
                configs: vec![AlterableConfig {
                    name: RATE_KEY_FOLLOWER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some(rate_str.clone()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        // Per-topic replicas configs.
        let topics: BTreeMap<String, (Option<&str>, Option<&str>)> = {
            let mut m: BTreeMap<String, (Option<&str>, Option<&str>)> = BTreeMap::new();
            for (topic, val) in &targets.leader_replicas_per_topic {
                m.entry(topic.clone()).or_default().0 = Some(val.as_str());
            }
            for (topic, val) in &targets.follower_replicas_per_topic {
                m.entry(topic.clone()).or_default().1 = Some(val.as_str());
            }
            m
        };

        for (topic, (leader_val, follower_val)) in &topics {
            let mut configs = Vec::new();
            if let Some(v) = leader_val {
                configs.push(AlterableConfig {
                    name: REPLICAS_KEY_LEADER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some((*v).to_string()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                });
            }
            if let Some(v) = follower_val {
                configs.push(AlterableConfig {
                    name: REPLICAS_KEY_FOLLOWER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some((*v).to_string()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                });
            }
            resources.push(AlterConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.clone(),
                configs,
                ..Default::default()
            });
        }

        let req = IncrementalAlterConfigsRequest {
            resources,
            ..Default::default()
        };
        let resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        check_alter_configs_response(&resp)?;
        Ok(())
    }

    async fn submit_reassignments(&self, movements: &[Movement]) -> Result<(), PhaseError> {
        let mut topics_map: BTreeMap<String, Vec<ReassignablePartition>> = BTreeMap::new();
        for m in movements {
            topics_map
                .entry(m.topic.clone())
                .or_default()
                .push(ReassignablePartition {
                    partition_index: m.partition,
                    replicas: Some(m.new_replicas.clone()),
                    ..Default::default()
                });
        }
        let topics: Vec<ReassignableTopic> = topics_map
            .into_iter()
            .map(|(name, partitions)| ReassignableTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect();
        let req = AlterPartitionReassignmentsRequest {
            timeout_ms: 60_000,
            topics,
            ..Default::default()
        };
        let resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        check_reassign_response(&resp)?;
        Ok(())
    }

    async fn cancel_reassignments(&self, partitions: &[(String, i32)]) -> Result<(), PhaseError> {
        let mut topics_map: BTreeMap<String, Vec<ReassignablePartition>> = BTreeMap::new();
        for (topic, partition) in partitions {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(ReassignablePartition {
                    partition_index: *partition,
                    replicas: None, // null = cancel
                    ..Default::default()
                });
        }
        let topics: Vec<ReassignableTopic> = topics_map
            .into_iter()
            .map(|(name, partitions)| ReassignableTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect();
        let req = AlterPartitionReassignmentsRequest {
            timeout_ms: 60_000,
            topics,
            ..Default::default()
        };
        let resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        check_reassign_response(&resp)?;
        Ok(())
    }

    async fn list_in_flight(
        &self,
        of_interest: &[(String, i32)],
    ) -> Result<Vec<(String, i32)>, PhaseError> {
        // Send with `topics = None` (all in-flight), then filter to `of_interest`.
        let req = ListPartitionReassignmentsRequest::default();
        let resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        let want: std::collections::HashSet<(String, i32)> = of_interest.iter().cloned().collect();
        let mut out = Vec::new();
        for t in &resp.topics {
            for p in &t.partitions {
                let key = (t.name.clone(), p.partition_index);
                if want.contains(&key) {
                    out.push(key);
                }
            }
        }
        Ok(out)
    }
}
