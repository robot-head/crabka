//! Topic-config wrappers.
//!
//! `DescribeConfigs` filters to the subset of entries the user/operator
//! has explicitly set (i.e. dynamic topic config, `ConfigSource =
//! DYNAMIC_TOPIC_CONFIG = 1`), so the diff against `spec.config` is
//! against overrides only — never broker defaults.

use std::collections::BTreeMap;

use crabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::DescribeConfigsResourceResult,
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
};

use crate::{AdminClient, AdminError, KafkaError, kafka_error_if, kafka_error_name};

/// `ConfigSource = DYNAMIC_TOPIC_CONFIG` per
/// <https://kafka.apache.org/protocol#The_Messages_DescribeConfigs>.
const DYNAMIC_TOPIC_CONFIG_SOURCE: i8 = 1;

/// Kafka's `ConfigResource.type` for topic resources.
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Per-topic dynamic config overrides (broker defaults are filtered out).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicConfigOverrides {
    pub topic: String,
    pub overrides: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum IncrementalAlterOp {
    Set {
        topic: String,
        key: String,
        value: String,
    },
    Delete {
        topic: String,
        key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsOutcome {
    pub topic: String,
    pub error: Option<KafkaError>,
}

/// Filter a `DescribeConfigsResult`'s entries down to dynamic-topic
/// overrides only. Pure function, unit-tested in isolation.
///
/// Per KIP-226 / Kafka's `DescribeConfigs` semantics, only entries
/// whose `config_source == DYNAMIC_TOPIC_CONFIG (1)` represent values
/// the user has explicitly set on the topic; everything else (broker
/// defaults, static config, etc.) is filtered out so the operator
/// diffs `spec.config` against overrides only.
pub(crate) fn filter_dynamic_overrides(
    topic: String,
    entries: impl IntoIterator<Item = DescribeConfigsResourceResult>,
) -> TopicConfigOverrides {
    let mut overrides = BTreeMap::new();
    for entry in entries {
        if entry.config_source == DYNAMIC_TOPIC_CONFIG_SOURCE
            && let Some(value) = entry.value
        {
            overrides.insert(entry.name, value);
        }
    }
    TopicConfigOverrides { topic, overrides }
}

/// Pure helper: take one `DescribeConfigsResult` (one resource's slice
/// of the response) and either return its dynamic-topic-config
/// overrides or surface its broker error. Extracted so both the success
/// and error branches can be unit-tested without standing up a broker.
pub(crate) fn parse_describe_configs_resource(
    r: crabka_protocol::owned::describe_configs_response::DescribeConfigsResult,
) -> Result<TopicConfigOverrides, AdminError> {
    if r.error_code != 0 {
        return Err(AdminError::Broker {
            api: "DescribeConfigs",
            code: r.error_code,
            name: kafka_error_name(r.error_code),
            message: r.error_message,
        });
    }
    Ok(filter_dynamic_overrides(r.resource_name, r.configs))
}

/// Pure helper: project an `IncrementalAlterConfigsResponse` into the
/// per-topic outcome list the operator consumes.
pub(crate) fn parse_incremental_alter_outcomes(
    resp: <IncrementalAlterConfigsRequest as crabka_protocol::ProtocolRequest>::Response,
) -> Vec<AlterConfigsOutcome> {
    resp.responses
        .into_iter()
        .map(|r| AlterConfigsOutcome {
            topic: r.resource_name,
            error: kafka_error_if(r.error_code, r.error_message),
        })
        .collect()
}

impl AdminClient {
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn describe_configs(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<TopicConfigOverrides>, AdminError> {
        let req = DescribeConfigsRequest {
            resources: topics
                .iter()
                .map(|t| DescribeConfigsResource {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: (*t).to_string(),
                    configuration_keys: None,
                    ..Default::default()
                })
                .collect(),
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;
        let mut out = Vec::with_capacity(resp.results.len());
        for r in resp.results {
            out.push(parse_describe_configs_resource(r)?);
        }
        Ok(out)
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn incremental_alter_configs(
        &mut self,
        ops: &[IncrementalAlterOp],
    ) -> Result<Vec<AlterConfigsOutcome>, AdminError> {
        // Group ops by topic.
        let mut by_topic: BTreeMap<String, Vec<AlterableConfig>> = BTreeMap::new();
        for op in ops {
            match op {
                IncrementalAlterOp::Set { topic, key, value } => {
                    by_topic
                        .entry(topic.clone())
                        .or_default()
                        .push(AlterableConfig {
                            name: key.clone(),
                            config_operation: 0, // SET
                            value: Some(value.clone()),
                            ..Default::default()
                        });
                }
                IncrementalAlterOp::Delete { topic, key } => {
                    by_topic
                        .entry(topic.clone())
                        .or_default()
                        .push(AlterableConfig {
                            name: key.clone(),
                            config_operation: 1, // DELETE
                            value: None,
                            ..Default::default()
                        });
                }
            }
        }
        let req = IncrementalAlterConfigsRequest {
            resources: by_topic
                .into_iter()
                .map(|(topic, configs)| AlterConfigsResource {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: topic,
                    configs,
                    ..Default::default()
                })
                .collect(),
            validate_only: false,
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;
        Ok(parse_incremental_alter_outcomes(resp))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn dynamic_topic_config_source_is_one() {
        // Guard so a future protocol change can't silently flip the
        // filter we use to distinguish overrides from broker defaults.
        assert2::assert!(DYNAMIC_TOPIC_CONFIG_SOURCE == 1);
    }

    #[test]
    fn resource_type_topic_is_two() {
        assert2::assert!(RESOURCE_TYPE_TOPIC == 2);
    }

    /// Spec test name: `describe_configs_filters_to_dynamic_topic`.
    ///
    /// Mixed `config_source` values: only entries with
    /// `DYNAMIC_TOPIC_CONFIG (1)` survive; `STATIC_BROKER_CONFIG (4)`
    /// and other sources are dropped. Also verifies entries with
    /// `value: None` are filtered out.
    #[test]
    fn describe_configs_filters_to_dynamic_topic() {
        let entries = vec![
            DescribeConfigsResourceResult {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                config_source: 1, // DYNAMIC_TOPIC_CONFIG
                ..Default::default()
            },
            DescribeConfigsResourceResult {
                name: "log.dirs".into(),
                value: Some("/data".into()),
                config_source: 4, // STATIC_BROKER_CONFIG
                ..Default::default()
            },
            DescribeConfigsResourceResult {
                name: "cleanup.policy".into(),
                value: Some("compact".into()),
                config_source: 1,
                ..Default::default()
            },
            // Dynamic-topic source but no value: should be dropped.
            DescribeConfigsResourceResult {
                name: "segment.bytes".into(),
                value: None,
                config_source: 1,
                ..Default::default()
            },
            // Another non-dynamic source (DEFAULT_CONFIG = 5).
            DescribeConfigsResourceResult {
                name: "max.message.bytes".into(),
                value: Some("1048576".into()),
                config_source: 5,
                ..Default::default()
            },
        ];
        let r = filter_dynamic_overrides("foo".into(), entries);
        assert2::assert!(
            r == TopicConfigOverrides {
                topic: "foo".to_string(),
                overrides: BTreeMap::from([
                    ("cleanup.policy".to_string(), "compact".to_string()),
                    ("retention.ms".to_string(), "60000".to_string()),
                ]),
            }
        );
    }

    // ── parse_describe_configs_resource ────────────────────────────────
    //
    // The success / error variants of the per-resource parser used by
    // `describe_configs`. The full `describe_configs` RPC short-circuits
    // on the first error_code != 0 entry; these tests lock that decision
    // point so a refactor can't silently drop the error mapping or the
    // dynamic-config filtering.

    #[test]
    fn parse_describe_configs_resource_returns_overrides_on_success() {
        use crabka_protocol::owned::describe_configs_response::DescribeConfigsResult;
        let r = DescribeConfigsResult {
            error_code: 0,
            error_message: None,
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "foo".into(),
            configs: vec![DescribeConfigsResourceResult {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                config_source: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let parsed = parse_describe_configs_resource(r).expect("Ok branch");
        assert2::assert!(
            parsed
                == TopicConfigOverrides {
                    topic: "foo".to_string(),
                    overrides: BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]),
                }
        );
    }

    #[test]
    fn parse_describe_configs_resource_returns_broker_error_when_error_code_set() {
        use crabka_protocol::owned::describe_configs_response::DescribeConfigsResult;
        let r = DescribeConfigsResult {
            error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
            error_message: Some("nope".into()),
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "missing".into(),
            configs: Vec::new(),
            ..Default::default()
        };
        let err = parse_describe_configs_resource(r).expect_err("Err branch");
        match err {
            AdminError::Broker {
                api,
                code,
                name,
                message,
            } => {
                check!(
                    (api, code, name, message.as_deref())
                        == (
                            "DescribeConfigs",
                            3,
                            "UNKNOWN_TOPIC_OR_PARTITION",
                            Some("nope")
                        )
                );
            }
            other => panic!("expected Broker, got {other:?}"),
        }
    }

    // ── parse_incremental_alter_outcomes ───────────────────────────────

    #[test]
    fn parse_incremental_alter_outcomes_success() {
        use crabka_protocol::owned::incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        };
        let resp = IncrementalAlterConfigsResponse {
            responses: vec![AlterConfigsResourceResponse {
                error_code: 0,
                error_message: None,
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "foo".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let outs = parse_incremental_alter_outcomes(resp);
        assert2::assert!(
            outs == vec![AlterConfigsOutcome {
                topic: "foo".to_string(),
                error: None,
            }]
        );
    }

    #[test]
    fn parse_incremental_alter_outcomes_carries_errors() {
        use crabka_protocol::owned::incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        };
        let resp = IncrementalAlterConfigsResponse {
            responses: vec![
                AlterConfigsResourceResponse {
                    error_code: 0,
                    error_message: None,
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "ok".into(),
                    ..Default::default()
                },
                AlterConfigsResourceResponse {
                    error_code: 40, // INVALID_CONFIG
                    error_message: Some("bad value".into()),
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "bad".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let outs = parse_incremental_alter_outcomes(resp);
        assert2::assert!(
            outs == vec![
                AlterConfigsOutcome {
                    topic: "ok".to_string(),
                    error: None,
                },
                AlterConfigsOutcome {
                    topic: "bad".to_string(),
                    error: Some(KafkaError {
                        code: 40,
                        name: "INVALID_CONFIG",
                        message: Some("bad value".to_string()),
                    }),
                },
            ]
        );
    }
}
