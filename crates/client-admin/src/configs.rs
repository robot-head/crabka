//! Topic-config wrappers.
//!
//! `DescribeConfigs` filters to the subset of entries the user/operator
//! has explicitly set (i.e. dynamic topic config, `ConfigSource =
//! DYNAMIC_TOPIC_CONFIG = 1`), so the diff against `spec.config` is
//! against overrides only — never broker defaults.

use std::collections::BTreeMap;

use crabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
};

use crate::{AdminClient, AdminError, KafkaError, kafka_error_name};

/// `ConfigSource = DYNAMIC_TOPIC_CONFIG` per
/// <https://kafka.apache.org/protocol#The_Messages_DescribeConfigs>.
const DYNAMIC_TOPIC_CONFIG_SOURCE: i8 = 1;

/// Kafka's `ConfigResource.type` for topic resources.
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Per-topic dynamic config overrides (broker defaults are filtered out).
#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone)]
pub struct AlterConfigsOutcome {
    pub topic: String,
    pub error: Option<KafkaError>,
}

impl AdminClient {
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
            if r.error_code != 0 {
                return Err(AdminError::Broker {
                    api: "DescribeConfigs",
                    code: r.error_code,
                    name: kafka_error_name(r.error_code),
                    message: r.error_message,
                });
            }
            let mut overrides = BTreeMap::new();
            for entry in r.configs {
                if entry.config_source == DYNAMIC_TOPIC_CONFIG_SOURCE
                    && let Some(value) = entry.value
                {
                    overrides.insert(entry.name, value);
                }
            }
            out.push(TopicConfigOverrides {
                topic: r.resource_name,
                overrides,
            });
        }
        Ok(out)
    }

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
        Ok(resp
            .responses
            .into_iter()
            .map(|r| AlterConfigsOutcome {
                topic: r.resource_name,
                error: if r.error_code == 0 {
                    None
                } else {
                    Some(KafkaError {
                        code: r.error_code,
                        name: kafka_error_name(r.error_code),
                        message: r.error_message,
                    })
                },
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_topic_config_source_is_one() {
        // Guard so a future protocol change can't silently flip the
        // filter we use to distinguish overrides from broker defaults.
        assert_eq!(DYNAMIC_TOPIC_CONFIG_SOURCE, 1);
    }

    #[test]
    fn resource_type_topic_is_two() {
        assert_eq!(RESOURCE_TYPE_TOPIC, 2);
    }
}
