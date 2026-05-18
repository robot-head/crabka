//! Topic CRUD wrappers.

use std::collections::BTreeMap;

use crabka_protocol::owned::{
    create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
    create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    delete_topics_request::DeleteTopicsRequest,
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};
use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;
use uuid::Uuid;

use crate::{AdminClient, AdminError, KafkaError, NOT_CONTROLLER, kafka_error_name};

#[derive(Debug, Clone)]
pub struct CreateTopicSpec {
    pub name: String,
    pub partitions: i32,
    pub replicas: i32,
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicOutcome {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone)]
pub struct DeleteTopicOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsOp {
    pub name: String,
    pub new_total_count: i32,
}

#[derive(Debug, Clone)]
pub struct CreatePartitionsOutcome {
    pub name: String,
    pub error: Option<KafkaError>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub controller_id: i32,
    pub topics: Vec<TopicMetadataEntry>,
}

#[derive(Debug, Clone)]
pub struct TopicMetadataEntry {
    pub name: String,
    pub topic_id: Option<Uuid>,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub error: Option<KafkaError>,
}

impl AdminClient {
    /// Metadata for the named topics. Pass an empty slice to fetch all
    /// topics, per Kafka semantics.
    pub async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        let req = build_metadata(topics);
        let resp = self.conn.send(req).await?;
        Ok(parse_metadata(resp))
    }

    pub async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout_ms: i32,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        let first = {
            let req = build_create_topics(specs, timeout_ms);
            let resp = self.conn.send(req).await?;
            parse_create_topics(resp)
        };
        if !any_not_controller(&first, |o| o.error.as_ref()) {
            return Ok(first);
        }
        self.refresh_controller_connection().await?;
        let second = {
            let req = build_create_topics(specs, timeout_ms);
            let resp = self.conn.send(req).await?;
            parse_create_topics(resp)
        };
        if any_not_controller(&second, |o| o.error.as_ref()) {
            return Err(AdminError::NotControllerExhausted);
        }
        Ok(second)
    }

    pub async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout_ms: i32,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        let build = || DeleteTopicsRequest {
            topic_names: names.iter().map(|s| (*s).to_string()).collect(),
            topics: Vec::new(),
            timeout_ms,
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

    pub async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        timeout_ms: i32,
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
            timeout_ms,
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

    /// Fetch Metadata, find the controller's `host:port`, and replace
    /// `self.conn` with a connection to it. Used by the per-method
    /// `NOT_CONTROLLER` retry paths above.
    async fn refresh_controller_connection(&mut self) -> Result<(), AdminError> {
        let md_resp = self.conn.send(build_metadata(&[])).await?;
        let controller_addr =
            controller_endpoint(&md_resp).ok_or(AdminError::NotControllerExhausted)?;
        self.reconnect(&controller_addr).await
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

fn build_create_topics(specs: &[CreateTopicSpec], timeout_ms: i32) -> CreateTopicsRequest {
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
        timeout_ms,
        validate_only: false,
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
            error: error_if(t.error_code, t.error_message),
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
            error: error_if(t.error_code, t.error_message),
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
            error: error_if(t.error_code, t.error_message),
        })
        .collect()
}

fn parse_metadata(
    resp: <MetadataRequest as crabka_protocol::ProtocolRequest>::Response,
) -> TopicMetadata {
    let topics = resp
        .topics
        .into_iter()
        .map(|t| {
            let partition_count = i32::try_from(t.partitions.len()).unwrap_or(i32::MAX);
            let replication_factor = i32::from(
                t.partitions
                    .first()
                    .map_or(0, |p| i16::try_from(p.replica_nodes.len()).unwrap_or(i16::MAX)),
            );
            TopicMetadataEntry {
                name: t.name.unwrap_or_default(),
                topic_id: proto_uuid_to_opt(t.topic_id),
                partition_count,
                replication_factor,
                error: error_if(t.error_code, None),
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
        .find(|b| b.node_id == id)
        .map(|b| format!("{}:{}", b.host, b.port))
}

fn proto_uuid_to_opt(u: ProtoUuid) -> Option<Uuid> {
    if u == ProtoUuid::ZERO {
        None
    } else {
        Some(Uuid::from_bytes(u.0))
    }
}

fn error_if(code: i16, message: Option<String>) -> Option<KafkaError> {
    if code == 0 {
        None
    } else {
        Some(KafkaError {
            code,
            name: kafka_error_name(code),
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn build_create_topics_one_spec() {
        let req = build_create_topics(
            &[CreateTopicSpec {
                name: "foo".into(),
                partitions: 3,
                replicas: 1,
                configs: BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]),
            }],
            5_000,
        );
        assert_eq!(req.topics.len(), 1);
        let t = &req.topics[0];
        assert_eq!(t.name, "foo");
        assert_eq!(t.num_partitions, 3);
        assert_eq!(t.replication_factor, 1);
        assert_eq!(t.configs.len(), 1);
        assert_eq!(t.configs[0].name, "retention.ms");
        assert_eq!(t.configs[0].value.as_deref(), Some("60000"));
        assert_eq!(req.timeout_ms, 5_000);
        assert!(!req.validate_only);
    }

    #[test]
    fn error_if_zero_code_is_none() {
        assert!(error_if(0, None).is_none());
    }

    #[test]
    fn error_if_nonzero_carries_name() {
        let e = error_if(36, Some("dup".into())).unwrap();
        assert_eq!(e.code, 36);
        assert_eq!(e.name, "TOPIC_ALREADY_EXISTS");
        assert_eq!(e.message.as_deref(), Some("dup"));
    }

    // ── NOT_CONTROLLER retry predicate ─────────────────────────────
    //
    // The full retry pipeline (first response carries NOT_CONTROLLER →
    // refresh controller endpoint → reconnect → second response succeeds)
    // is exercised against a real broker in `tests/round_trip.rs`. The
    // unit tests below lock the two pure pieces — the predicate that
    // decides whether to retry, and the metadata-response → host:port
    // resolver — so a refactor can't silently flip either one.

    /// Spec test name: `not_controller_triggers_one_retry` (predicate
    /// half). Verifies that `any_not_controller` returns `true` iff at
    /// least one outcome carries the `NOT_CONTROLLER (41)` error code.
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
        assert!(any_not_controller(&outcomes, |o| o.error.as_ref()));

        let all_ok = vec![CreateTopicOutcome {
            name: "a".into(),
            topic_id: None,
            error: None,
        }];
        assert!(!any_not_controller(&all_ok, |o| o.error.as_ref()));
    }

    /// Spec test name: `repeated_not_controller_errors_return_exhausted`
    /// (predicate half). Non-`NOT_CONTROLLER` errors must NOT trigger
    /// the retry path — only code 41 does. Combined with the integration
    /// test, this locks the retry-eligibility check: if the predicate
    /// fired on, say, `TOPIC_ALREADY_EXISTS`, callers would see spurious
    /// reconnects + `NotControllerExhausted` returns on real failures.
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
        assert!(!any_not_controller(&outcomes, |o| o.error.as_ref()));
    }

    // ── controller_endpoint resolver ───────────────────────────────

    /// Spec test name: `connect_walks_bootstrap_list` (resolver half —
    /// the actual bootstrap-walking integration coverage lives in
    /// `tests/connect.rs`). `controller_endpoint` extracts the
    /// `host:port` of the broker whose `node_id` matches the metadata
    /// response's `controller_id`. This is the address the
    /// `NOT_CONTROLLER` retry path reconnects to.
    #[test]
    fn controller_endpoint_picks_broker_with_matching_node_id() {
        use crabka_protocol::owned::metadata_response::{
            MetadataResponse, MetadataResponseBroker,
        };
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
        assert_eq!(addr.as_deref(), Some("h2:9093"));
    }

    /// When the controller id doesn't appear in the broker list (e.g.
    /// the cluster is mid-failover), `controller_endpoint` returns
    /// `None`, which the retry path maps to
    /// `AdminError::NotControllerExhausted`.
    #[test]
    fn controller_endpoint_returns_none_when_no_match() {
        use crabka_protocol::owned::metadata_response::{
            MetadataResponse, MetadataResponseBroker,
        };
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
        assert!(controller_endpoint(&resp).is_none());
    }
}
