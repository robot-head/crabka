//! Production `ClientFacade` over `crabka_client_core::Client`. It maps each
//! trait method to the matching admin RPC through raw `Client::send`, and
//! mirrors the ingester pattern.

use std::collections::BTreeMap;

use async_trait::async_trait;
use crabka_client_core::Client;
use crabka_protocol::owned::{
    alter_partition_reassignments_request::{
        AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
    },
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    list_partition_reassignments_request::ListPartitionReassignmentsRequest,
    list_partition_reassignments_response::ListPartitionReassignmentsResponse,
};
use crabka_units::{
    ByteRate, Time,
    convert::{ByteRateExt as _, TimeExt as _},
    secs,
};
use refined_type::{Refined, rule::GreaterI32};

use crate::{
    executor::{
        phases::{ClientFacade, ConfigOp, PhaseError},
        throttle::ThrottleTargets,
    },
    model::Movement,
};

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

/// Default Kafka broker-side timeout for submitting or cancelling
/// reassignments.
pub const DEFAULT_REASSIGNMENT_REQUEST_TIMEOUT: Time = secs(60);

/// Positive whole-millisecond timeout carried by
/// `AlterPartitionReassignmentsRequest`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReassignmentRequestTimeout(i32);

impl ReassignmentRequestTimeout {
    /// Validate a Kafka protocol timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is non-finite, zero, negative,
    /// fractional in milliseconds, or greater than `i32::MAX` milliseconds.
    pub fn new(value: Time) -> Result<Self, String> {
        let milliseconds = value.millis_i64();
        if !value.secs_f64().is_finite() || Time::from_millis(milliseconds) != value {
            return Err(
                "reassignment request timeout must be a whole number of milliseconds".to_string(),
            );
        }
        let milliseconds = i32::try_from(milliseconds).map_err(|_| {
            "reassignment request timeout must be within 1..=i32::MAX milliseconds".to_string()
        })?;
        GreaterI32::<0>::new(milliseconds)
            .map(Refined::into_value)
            .map(Self)
            .map_err(|error| format!("reassignment request timeout: {error}"))
    }

    /// Return the dimensioned timeout.
    #[must_use]
    pub fn time(self) -> Time {
        Time::from_millis(i64::from(self.0))
    }

    /// Return the Kafka protocol millisecond value.
    #[must_use]
    pub const fn milliseconds(self) -> i32 {
        self.0
    }
}

impl Default for ReassignmentRequestTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_REASSIGNMENT_REQUEST_TIMEOUT)
            .expect("default reassignment request timeout is valid")
    }
}

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

fn build_alter_throttle_request(
    op: ConfigOp,
    targets: &ThrottleTargets,
    throttle: ByteRate,
) -> IncrementalAlterConfigsRequest {
    let op_byte = match op {
        ConfigOp::Set => OP_SET,
        ConfigOp::Delete => OP_DELETE,
    };
    // KIP-73 expresses both rate keys as a decimal bytes-per-second string.
    let rate_str = throttle.bytes_per_sec_i64().to_string();
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

    IncrementalAlterConfigsRequest {
        resources,
        ..Default::default()
    }
}

fn build_submit_reassignments_request(
    movements: &[Movement],
    timeout: ReassignmentRequestTimeout,
) -> AlterPartitionReassignmentsRequest {
    build_reassignments_request(
        movements
            .iter()
            .map(|m| (m.topic.clone(), m.partition, Some(m.new_replicas.clone()))),
        timeout,
    )
}

fn build_cancel_reassignments_request(
    partitions: &[(String, i32)],
    timeout: ReassignmentRequestTimeout,
) -> AlterPartitionReassignmentsRequest {
    build_reassignments_request(
        partitions
            .iter()
            .map(|(topic, partition)| (topic.clone(), *partition, None)),
        timeout,
    )
}

fn build_reassignments_request(
    partitions: impl IntoIterator<Item = (String, i32, Option<Vec<i32>>)>,
    timeout: ReassignmentRequestTimeout,
) -> AlterPartitionReassignmentsRequest {
    let mut topics_map: BTreeMap<String, Vec<ReassignablePartition>> = BTreeMap::new();
    for (topic, partition, replicas) in partitions {
        topics_map
            .entry(topic)
            .or_default()
            .push(ReassignablePartition {
                partition_index: partition,
                replicas,
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
    AlterPartitionReassignmentsRequest {
        timeout_ms: timeout.milliseconds(),
        topics,
        ..Default::default()
    }
}

fn filter_in_flight_response(
    resp: &ListPartitionReassignmentsResponse,
    of_interest: &[(String, i32)],
) -> Vec<(String, i32)> {
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
    out
}

pub struct LiveClient {
    pub inner: Client,
    reassignment_request_timeout: ReassignmentRequestTimeout,
}

impl LiveClient {
    #[must_use]
    pub fn new(inner: Client) -> Self {
        Self::with_reassignment_request_timeout(inner, ReassignmentRequestTimeout::default())
    }

    #[must_use]
    pub fn with_reassignment_request_timeout(
        inner: Client,
        reassignment_request_timeout: ReassignmentRequestTimeout,
    ) -> Self {
        Self {
            inner,
            reassignment_request_timeout,
        }
    }
}

#[async_trait]
impl ClientFacade for LiveClient {
    async fn alter_throttle_configs(
        &self,
        op: ConfigOp,
        targets: &ThrottleTargets,
        throttle: ByteRate,
    ) -> Result<(), PhaseError> {
        let req = build_alter_throttle_request(op, targets, throttle);
        let resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        check_alter_configs_response(&resp)?;
        Ok(())
    }

    async fn submit_reassignments(&self, movements: &[Movement]) -> Result<(), PhaseError> {
        let req = build_submit_reassignments_request(movements, self.reassignment_request_timeout);
        let resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        check_reassign_response(&resp)?;
        Ok(())
    }

    async fn cancel_reassignments(&self, partitions: &[(String, i32)]) -> Result<(), PhaseError> {
        let req = build_cancel_reassignments_request(partitions, self.reassignment_request_timeout);
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
        Ok(filter_in_flight_response(&resp, of_interest))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use assert2::check;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            alter_partition_reassignments_response::{
                AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
                ReassignableTopicResponse,
            },
            incremental_alter_configs_response::{
                AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
            },
            list_partition_reassignments_response::{
                OngoingPartitionReassignment, OngoingTopicReassignment,
            },
        },
    };
    use crabka_units::{Time, bytes_per_sec, convert::TimeExt as _, micros, millis};

    use super::*;

    /// Connect and request timeout for the deliberately unreachable test
    /// client.
    const CLIENT_TIMEOUT: Time = millis(50);

    fn movement(topic: &str, partition: i32, old: Vec<i32>, new: Vec<i32>) -> Movement {
        Movement {
            topic: topic.into(),
            partition,
            old_replicas: old,
            new_replicas: new,
            old_leader: 1,
            new_leader: 1,
        }
    }

    fn targets() -> ThrottleTargets {
        ThrottleTargets {
            leader_brokers: BTreeSet::from([1]),
            follower_brokers: BTreeSet::from([2]),
            leader_replicas_per_topic: BTreeMap::from([("orders".into(), "0:1".into())]),
            follower_replicas_per_topic: BTreeMap::from([("orders".into(), "0:2".into())]),
        }
    }

    #[test]
    fn alter_configs_response_reports_resource_failures() {
        let resp = IncrementalAlterConfigsResponse {
            responses: vec![AlterConfigsResourceResponse {
                error_code: 42,
                error_message: Some("nope".into()),
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = check_alter_configs_response(&resp).expect_err("failure should surface");
        assert2::assert!(err.to_string().contains("IncrementalAlterConfigs failed"));
        assert2::assert!(err.to_string().contains("error_code=42"));
    }

    #[test]
    fn reassign_response_reports_top_level_and_partition_failures() {
        let top = AlterPartitionReassignmentsResponse {
            error_code: 7,
            error_message: Some("top".into()),
            ..Default::default()
        };
        check!(
            check_reassign_response(&top)
                .unwrap_err()
                .to_string()
                .contains("top-level error_code=7")
        );

        let per_partition = AlterPartitionReassignmentsResponse {
            responses: vec![ReassignableTopicResponse {
                name: "orders".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 3,
                    error_code: 9,
                    error_message: Some("bad partition".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = check_reassign_response(&per_partition).expect_err("partition failure");
        check!(err.to_string().contains("orders"));
        check!(err.to_string().contains("error_code=9"));
    }

    #[test]
    fn build_alter_throttle_request_sets_all_resource_fields() {
        let req = build_alter_throttle_request(ConfigOp::Set, &targets(), bytes_per_sec(1234));

        assert2::assert!(
            req == IncrementalAlterConfigsRequest {
                resources: vec![
                    AlterConfigsResource {
                        resource_type: 4,
                        resource_name: "1".into(),
                        configs: vec![AlterableConfig {
                            name: "leader.replication.throttled.rate".into(),
                            config_operation: 0,
                            value: Some("1234".into()),
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    AlterConfigsResource {
                        resource_type: 4,
                        resource_name: "2".into(),
                        configs: vec![AlterableConfig {
                            name: "follower.replication.throttled.rate".into(),
                            config_operation: 0,
                            value: Some("1234".into()),
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    AlterConfigsResource {
                        resource_type: 2,
                        resource_name: "orders".into(),
                        configs: vec![
                            AlterableConfig {
                                name: "leader.replication.throttled.replicas".into(),
                                config_operation: 0,
                                value: Some("0:1".into()),
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                            AlterableConfig {
                                name: "follower.replication.throttled.replicas".into(),
                                config_operation: 0,
                                value: Some("0:2".into()),
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                ],
                validate_only: false,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn build_alter_throttle_delete_request_tombstones_values() {
        let req = build_alter_throttle_request(ConfigOp::Delete, &targets(), bytes_per_sec(1234));
        assert2::assert!(req.resources.iter().all(|r| {
            r.configs
                .iter()
                .all(|c| c.config_operation == OP_DELETE && c.value.is_none())
        }));
    }

    #[test]
    fn build_submit_reassignments_request_groups_topic_partitions() {
        let req = build_submit_reassignments_request(
            &[
                movement("orders", 1, vec![1], vec![2, 3]),
                movement("orders", 0, vec![1], vec![4]),
                movement("payments", 0, vec![2], vec![3]),
            ],
            ReassignmentRequestTimeout::default(),
        );

        assert2::assert!(
            req == AlterPartitionReassignmentsRequest {
                timeout_ms: 60_000,
                allow_replication_factor_change: true,
                topics: vec![
                    ReassignableTopic {
                        name: "orders".into(),
                        partitions: vec![
                            ReassignablePartition {
                                partition_index: 1,
                                replicas: Some(vec![2, 3]),
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                            ReassignablePartition {
                                partition_index: 0,
                                replicas: Some(vec![4]),
                                unknown_tagged_fields: UnknownTaggedFields(vec![]),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    ReassignableTopic {
                        name: "payments".into(),
                        partitions: vec![ReassignablePartition {
                            partition_index: 0,
                            replicas: Some(vec![3]),
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn build_cancel_reassignments_request_uses_null_replicas() {
        let req = build_cancel_reassignments_request(
            &[("orders".to_string(), 1), ("payments".to_string(), 0)],
            ReassignmentRequestTimeout::default(),
        );

        assert2::assert!(
            req == AlterPartitionReassignmentsRequest {
                timeout_ms: 60_000,
                allow_replication_factor_change: true,
                topics: vec![
                    ReassignableTopic {
                        name: "orders".into(),
                        partitions: vec![ReassignablePartition {
                            partition_index: 1,
                            replicas: None,
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                    ReassignableTopic {
                        name: "payments".into(),
                        partitions: vec![ReassignablePartition {
                            partition_index: 0,
                            replicas: None,
                            unknown_tagged_fields: UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn reassignment_request_timeout_validates_protocol_milliseconds() {
        let timeout = ReassignmentRequestTimeout::new(millis(37)).unwrap();
        assert2::assert!(timeout.time() == millis(37));
        assert2::assert!(timeout.milliseconds() == 37);
        assert2::assert!(ReassignmentRequestTimeout::default().milliseconds() == 60_000);
        assert2::assert!(
            ReassignmentRequestTimeout::new(Time::from_millis(i64::from(i32::MAX)))
                .unwrap()
                .milliseconds()
                == i32::MAX
        );

        for invalid in [
            Time::ZERO,
            Time::from_millis(-1),
            micros(500),
            Time::from_secs_f64(f64::NAN),
            Time::from_secs_f64(f64::INFINITY),
            Time::from_millis(i64::from(i32::MAX) + 1),
        ] {
            assert2::assert!(ReassignmentRequestTimeout::new(invalid).is_err());
        }
    }

    #[test]
    fn reassignment_builders_frame_configured_timeout() {
        let timeout = ReassignmentRequestTimeout::new(millis(37)).unwrap();
        let submit =
            build_submit_reassignments_request(&[movement("orders", 0, vec![1], vec![2])], timeout);
        let cancel = build_cancel_reassignments_request(&[("orders".to_string(), 0)], timeout);

        assert2::assert!(submit.timeout_ms == 37);
        assert2::assert!(cancel.timeout_ms == 37);
    }

    #[test]
    fn filter_in_flight_response_returns_only_requested_keys() {
        let resp = ListPartitionReassignmentsResponse {
            topics: vec![
                OngoingTopicReassignment {
                    name: "orders".into(),
                    partitions: vec![
                        OngoingPartitionReassignment {
                            partition_index: 0,
                            ..Default::default()
                        },
                        OngoingPartitionReassignment {
                            partition_index: 2,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                OngoingTopicReassignment {
                    name: "payments".into(),
                    partitions: vec![OngoingPartitionReassignment {
                        partition_index: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let filtered =
            filter_in_flight_response(&resp, &[("orders".into(), 2), ("payments".into(), 9)]);

        assert2::assert!(filtered == vec![("orders".to_string(), 2)]);
    }

    async fn unreachable_live_client(suffix: &str) -> LiveClient {
        let inner = Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id(format!("rebalancer-live-client-test-{suffix}"))
            .connect_timeout(CLIENT_TIMEOUT)
            .request_timeout(CLIENT_TIMEOUT)
            .build()
            .await
            .expect("client build does not connect");
        LiveClient::new(inner)
    }

    #[tokio::test]
    async fn live_client_methods_propagate_send_errors() {
        let client = unreachable_live_client("send-errors").await;

        assert2::assert!(matches!(
            client
                .alter_throttle_configs(ConfigOp::Set, &targets(), bytes_per_sec(1234))
                .await,
            Err(PhaseError::Client(_))
        ));
        assert2::assert!(matches!(
            client
                .submit_reassignments(&[movement("orders", 0, vec![1], vec![2])])
                .await,
            Err(PhaseError::Client(_))
        ));
        assert2::assert!(matches!(
            client
                .cancel_reassignments(&[("orders".to_string(), 0)])
                .await,
            Err(PhaseError::Client(_))
        ));
        assert2::assert!(matches!(
            client.list_in_flight(&[("orders".to_string(), 0)]).await,
            Err(PhaseError::Client(_))
        ));
    }
}
