//! `DescribeConfigs` (`api_key=32`). It returns the dynamic override configs
//! that the metadata image holds.
//!
//! - `resource_type=2` (TOPIC): the handler reads the per-topic override map
//!   and emits entries with `config_source = DYNAMIC_TOPIC_CONFIG (1)`.
//! - `resource_type=4` (BROKER): a numeric name returns the effective dynamic
//!   per-broker and cluster-default overrides. An empty name returns the
//!   cluster-wide defaults. Sources distinguish `DYNAMIC_BROKER_CONFIG (2)`
//!   from `DYNAMIC_DEFAULT_BROKER_CONFIG (3)`.
//! - Every other resource type receives an empty configs list and no error.
//!   The JVM `AdminClient` accepts that.
//!
//! The handler honors the `configuration_keys` filter on the request. When the
//! client supplies an explicit key list, the response holds only those keys.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        },
    },
};
use crabka_units::convert::TimeExt as _;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

/// `ConfigSource::DYNAMIC_TOPIC_CONFIG`, the value Kafka uses for per-topic
/// overrides held in `ZooKeeper` or `KRaft` metadata.
///
/// From `org.apache.kafka.clients.admin.ConfigEntry.ConfigSource`:
/// `DYNAMIC_TOPIC_CONFIG = 1`, `DYNAMIC_BROKER_CONFIG = 2`,
/// `DYNAMIC_DEFAULT_BROKER_CONFIG = 3`, `STATIC_BROKER_CONFIG = 4`,
/// `DEFAULT_CONFIG = 5`, `DYNAMIC_BROKER_LOGGER_CONFIG = 6`.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
const CONFIG_SOURCE_DYNAMIC_BROKER: i8 = 2;
const CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER: i8 = 3;
/// `ConfigSource::DEFAULT_CONFIG`, for keys reported at their default.
const CONFIG_SOURCE_DEFAULT: i8 = 5;
/// `DescribeConfigsResponse.ConfigSource::CLIENT_METRICS_CONFIG` wire byte.
const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;
/// `ConfigSource::DYNAMIC_GROUP_CONFIG`.
const CONFIG_SOURCE_DYNAMIC_GROUP: i8 = 8;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;
const RESOURCE_TYPE_GROUP: i8 = 32;

/// `ConfigDef.Type::UNKNOWN` wire byte. Crabka reports no typed config
/// metadata, which matches brokers from before KIP-226's typed responses.
const CONFIG_TYPE_UNKNOWN: i8 = 0;

/// Produces a `DescribeConfigsResourceResult` for one `(key, value)` pair.
fn make_entry(key: &str, value: &str, config_source: i8) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: key.to_owned(),
        value: Some(value.to_owned()),
        read_only: false,
        config_source,
        is_sensitive: false,
        synonyms: Vec::new(),
        config_type: CONFIG_TYPE_UNKNOWN,
        documentation: None,
        ..Default::default()
    }
}

/// Dispatches one resource entry from a `DescribeConfigs` request.
fn describe_one(
    image: &crabka_metadata::MetadataImage,
    r: crabka_protocol::owned::describe_configs_request::DescribeConfigsResource,
    client_metrics_default_interval_ms: i32,
    streams_defaults: &crate::coordinator::unified::streams::config::StreamsGroupConfig,
) -> DescribeConfigsResult {
    let ok = |configs| DescribeConfigsResult {
        error_code: codes::NONE,
        error_message: None,
        resource_type: r.resource_type,
        resource_name: r.resource_name.clone(),
        configs,
        ..Default::default()
    };

    if r.resource_type == RESOURCE_TYPE_TOPIC {
        let configs = match image.topic_config(&r.resource_name) {
            None => Vec::new(),
            Some(map) => {
                let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
                map.iter()
                    .filter(|(k, _)| key_filter.is_none_or(|ks| ks.iter().any(|f| f == *k)))
                    .map(|(k, v)| make_entry(k, v, CONFIG_SOURCE_DYNAMIC_TOPIC))
                    .collect()
            }
        };
        return ok(configs);
    }

    if r.resource_type == RESOURCE_TYPE_BROKER {
        let node_id = if r.resource_name.is_empty() {
            None
        } else {
            let Ok(node_id) = r.resource_name.parse::<u64>() else {
                return DescribeConfigsResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some(format!(
                        "resource_name `{}` is not a valid broker id",
                        r.resource_name
                    )),
                    resource_type: r.resource_type,
                    resource_name: r.resource_name,
                    configs: Vec::new(),
                    ..Default::default()
                };
            };
            Some(crabka_metadata::NodeId(node_id))
        };
        let defaults = image.default_broker_config();
        let per_broker = node_id.and_then(|node_id| image.broker_config(node_id));
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let mut keys = std::collections::BTreeSet::new();
        keys.extend(
            defaults
                .into_iter()
                .flat_map(std::collections::BTreeMap::keys),
        );
        keys.extend(
            per_broker
                .into_iter()
                .flat_map(std::collections::BTreeMap::keys),
        );
        let configs: Vec<DescribeConfigsResourceResult> = keys
            .into_iter()
            .filter(|key| key_filter.is_none_or(|ks| ks.iter().any(|filter| filter == *key)))
            .map(|key| {
                per_broker.and_then(|configs| configs.get(key)).map_or_else(
                    || {
                        make_entry(
                            key,
                            defaults
                                .and_then(|configs| configs.get(key))
                                .expect("key came from broker defaults"),
                            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                        )
                    },
                    |value| make_entry(key, value, CONFIG_SOURCE_DYNAMIC_BROKER),
                )
            })
            .collect();
        return ok(configs);
    }

    if r.resource_type == RESOURCE_TYPE_CLIENT_METRICS {
        use crate::client_metrics::config::{KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS};
        let overrides = image
            .client_metrics_config(&r.resource_name)
            .cloned()
            .unwrap_or_default();
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let mut configs = Vec::new();
        // Emit all three keys: set values use CLIENT_METRICS_CONFIG source;
        // unset keys report their default value/source (KAFKA-17516 — tooling
        // needs effective values, not blanks).
        let default_interval = client_metrics_default_interval_ms.to_string();
        let mut emit = |key: &str, default: &str| {
            if key_filter.is_some_and(|ks| !ks.iter().any(|f| f == key)) {
                return;
            }
            match overrides.get(key) {
                Some(v) => configs.push(make_entry(key, v, CONFIG_SOURCE_CLIENT_METRICS)),
                None => configs.push(make_entry(key, default, CONFIG_SOURCE_DEFAULT)),
            }
        };
        emit(KEY_METRICS, "");
        emit(KEY_INTERVAL_MS, &default_interval);
        emit(KEY_MATCH, "");
        return ok(configs);
    }

    if r.resource_type == RESOURCE_TYPE_GROUP {
        let overrides = image
            .group_config(&r.resource_name)
            .cloned()
            .unwrap_or_default();
        let effective = streams_defaults
            .with_group_overrides(&overrides)
            .unwrap_or_else(|_| streams_defaults.clone())
            .group_config_values();
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let configs = effective
            .iter()
            .filter(|(key, _)| key_filter.is_none_or(|keys| keys.iter().any(|k| k == *key)))
            .map(|(key, value)| {
                let source = if overrides.contains_key(key) {
                    CONFIG_SOURCE_DYNAMIC_GROUP
                } else {
                    CONFIG_SOURCE_DEFAULT
                };
                make_entry(key, value, source)
            })
            .collect();
        return ok(configs);
    }

    // All other resource types: empty configs, no error.
    ok(Vec::new())
}

/// Per-resource `DescribeConfigs` ACL check.
///
/// A Topic resource needs `DescribeConfigs` on `Topic(name)`. A Broker
/// resource needs `DescribeConfigs` on `Cluster("kafka-cluster")`.
///
/// This function returns the authorization-failed code to stamp on a Deny. It
/// returns `None` when the check allows the request, and for a resource type
/// that it does not gate. An ungated resource type still gets an empty configs
/// list with no error.
fn resource_authz_failure(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
    resource_type: i8,
    resource_name: &str,
) -> Option<i16> {
    let (rt, name, failure_code): (ResourceType, &str, i16) = match resource_type {
        RESOURCE_TYPE_TOPIC => (
            ResourceType::Topic,
            resource_name,
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        RESOURCE_TYPE_BROKER => (
            ResourceType::Cluster,
            crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
        RESOURCE_TYPE_GROUP => (
            ResourceType::Group,
            resource_name,
            codes::GROUP_AUTHORIZATION_FAILED,
        ),
        _ => return None,
    };
    let allow = authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: rt,
            resource_name: name,
            operation: AclOperation::DescribeConfigs,
        },
    );
    (allow == AuthorizationResult::Deny).then_some(failure_code)
}

/// Builds a `DescribeConfigsResult` that carries only the
/// authorization-failed error code, for a denied resource.
fn denied_result(
    resource_type: i8,
    resource_name: String,
    error_code: i16,
) -> DescribeConfigsResult {
    DescribeConfigsResult {
        error_code,
        error_message: Some("authorization failed".into()),
        resource_type,
        resource_name,
        configs: Vec::new(),
        ..Default::default()
    }
}

#[tracing::instrument(
    name = "handle_describe_configs",
    level = "info",
    skip_all,
    fields(api = "DescribeConfigs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();

    {
        let mut cur: &[u8] = req_bytes;
        let req = DescribeConfigsRequest::decode(&mut cur, version)?;

        let image = controller.current_image();
        // ── ACL preamble ────────────────────────────────────────────
        // Per-resource `DescribeConfigs`: Topic → `Topic(name)`; Broker →
        // `Cluster("kafka-cluster")`. On Deny stamp the result entry with
        // the matching authorization-failed code; authorized resources
        // resolve normally.
        let results: Vec<DescribeConfigsResult> = req
            .resources
            .into_iter()
            .map(|r| {
                if let Some(code) = resource_authz_failure(
                    broker.config.authorizer.as_ref(),
                    &image,
                    ctx.principal,
                    ctx.peer,
                    r.resource_type,
                    &r.resource_name,
                ) {
                    denied_result(r.resource_type, r.resource_name, code)
                } else {
                    describe_one(
                        &image,
                        r,
                        broker.config.client_metrics_default_interval.millis_i32(),
                        &broker.config.streams_group,
                    )
                }
            })
            .collect();

        let resp = DescribeConfigsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use crabka_metadata::{BrokerConfigRecord, MetadataImage, MetadataRecord};
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    };
    use uuid::Uuid;

    /// Builds a minimal `MetadataImage` with one broker config entry.
    fn image_with_broker_config(node_id: u64, key: &str, value: &str) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: crabka_metadata::NodeId(node_id),
            config_name: key.to_string(),
            config_value: Some(value.to_string()),
        }));
        img
    }

    #[test]
    fn broker_resource_name_invalid_fails_parse() {
        // Non-numeric resource_name must fail to parse as NodeId.
        assert!("not-a-number".parse::<u64>().is_err());
    }

    #[test]
    fn broker_resource_all_keys_returned_when_no_filter() {
        let img = image_with_broker_config(1, "leader.replication.throttled.rate", "1024");
        let map = img
            .broker_config(crabka_metadata::NodeId(1))
            .cloned()
            .unwrap_or_default();
        assert!(
            map.get("leader.replication.throttled.rate")
                .map(String::as_str)
                == Some("1024")
        );
    }

    #[test]
    fn make_entry_preserves_wire_metadata_fields() {
        let entry = super::make_entry(
            "leader.replication.throttled.rate",
            "1024",
            super::CONFIG_SOURCE_DYNAMIC_BROKER,
        );

        let expected = DescribeConfigsResourceResult {
            name: "leader.replication.throttled.rate".to_string(),
            value: Some("1024".to_string()),
            read_only: false,
            config_source: super::CONFIG_SOURCE_DYNAMIC_BROKER,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: 0,
            documentation: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(entry == expected);
    }

    #[test]
    fn topic_describe_one_preserves_result_and_filtered_config_fields() {
        use crabka_metadata::TopicConfigRecord;

        let mut img = MetadataImage::new(Uuid::nil());
        let mut overrides = BTreeMap::new();
        overrides.insert("cleanup.policy".to_string(), "compact".to_string());
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides,
        }));
        let result = super::describe_one(
            &img,
            crabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
                resource_type: super::RESOURCE_TYPE_TOPIC,
                resource_name: "orders".into(),
                configuration_keys: Some(vec!["cleanup.policy".into()]),
                ..Default::default()
            },
            300_000,
            &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        );

        let expected = DescribeConfigsResult {
            error_code: crate::codes::NONE,
            error_message: None,
            resource_type: super::RESOURCE_TYPE_TOPIC,
            resource_name: "orders".to_string(),
            configs: vec![DescribeConfigsResourceResult {
                name: "cleanup.policy".to_string(),
                value: Some("compact".to_string()),
                read_only: false,
                config_source: super::CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: Vec::new(),
                config_type: 0,
                documentation: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(result == expected);
    }

    #[test]
    fn broker_describe_one_rejects_non_numeric_resource_name_with_fields() {
        let img = MetadataImage::new(Uuid::nil());
        let result = super::describe_one(
            &img,
            crabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
                resource_type: super::RESOURCE_TYPE_BROKER,
                resource_name: "not-a-number".into(),
                configuration_keys: None,
                ..Default::default()
            },
            300_000,
            &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        );

        let expected = DescribeConfigsResult {
            error_code: crate::codes::INVALID_REQUEST,
            error_message: Some(
                "resource_name `not-a-number` is not a valid broker id".to_string(),
            ),
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: "not-a-number".to_string(),
            configs: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(result == expected);
    }

    #[test]
    fn broker_resource_key_filter_applied() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: crabka_metadata::NodeId(2),
            config_name: "leader.replication.throttled.rate".to_string(),
            config_value: Some("512".to_string()),
        }));
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: crabka_metadata::NodeId(2),
            config_name: "follower.replication.throttled.rate".to_string(),
            config_value: Some("256".to_string()),
        }));

        let map = img
            .broker_config(crabka_metadata::NodeId(2))
            .cloned()
            .unwrap_or_default();
        let key_filter = ["leader.replication.throttled.rate".to_string()];
        let filtered: BTreeMap<_, _> = map
            .into_iter()
            .filter(|(k, _)| key_filter.iter().any(|f| f == k))
            .collect();

        let expected: BTreeMap<String, String> = [(
            "leader.replication.throttled.rate".to_string(),
            "512".to_string(),
        )]
        .into_iter()
        .collect();
        assert!(filtered == expected);
    }

    #[test]
    fn broker_resource_missing_node_returns_empty_configs() {
        let img = MetadataImage::new(Uuid::nil());
        // Node 99 has no broker configs in the image.
        let map = img
            .broker_config(crabka_metadata::NodeId(99))
            .cloned()
            .unwrap_or_default();
        assert!(map.is_empty());
    }

    #[test]
    fn config_source_dynamic_broker_is_2() {
        assert!(super::CONFIG_SOURCE_DYNAMIC_BROKER == 2i8);
    }

    #[test]
    fn broker_describe_inherits_default_and_prefers_per_broker_override() {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: crabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("1024".into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: crabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: "follower.replication.throttled.rate".into(),
            config_value: Some("512".into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: crabka_metadata::NodeId(1),
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));

        let result = super::describe_one(
            &image,
            crabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
                resource_type: super::RESOURCE_TYPE_BROKER,
                resource_name: "1".into(),
                configuration_keys: None,
                ..Default::default()
            },
            300_000,
            &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        );

        assert!(
            result.configs
                == vec![
                    super::make_entry(
                        "follower.replication.throttled.rate",
                        "512",
                        super::CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                    ),
                    super::make_entry(
                        "leader.replication.throttled.rate",
                        "2048",
                        super::CONFIG_SOURCE_DYNAMIC_BROKER,
                    ),
                ]
        );
    }

    #[test]
    fn empty_broker_name_describes_cluster_defaults() {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: crabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("1024".into()),
        }));

        let result = super::describe_one(
            &image,
            crabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
                resource_type: super::RESOURCE_TYPE_BROKER,
                resource_name: String::new(),
                configuration_keys: None,
                ..Default::default()
            },
            300_000,
            &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        );

        assert!(
            result.configs
                == vec![super::make_entry(
                    "leader.replication.throttled.rate",
                    "1024",
                    super::CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                )]
        );
    }

    #[test]
    fn client_metrics_describe_emits_defaults() {
        use crabka_metadata::{ClientMetricsConfigRecord, MetadataRecord};
        let mut img = MetadataImage::new(Uuid::nil());
        let mut cfgs = std::collections::BTreeMap::new();
        cfgs.insert("metrics".to_string(), "a.".to_string());
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "sub-a".into(),
                configs: cfgs,
            },
        ));
        let r = crabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configuration_keys: None,
            ..Default::default()
        };
        let res = super::describe_one(
            &img,
            r,
            12_345,
            &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        );
        assert_eq!(res.error_code, crate::codes::NONE);
        let by_name: std::collections::HashMap<_, _> =
            res.configs.iter().map(|c| (c.name.as_str(), c)).collect();
        let cases = [
            ("metrics", Some("a."), super::CONFIG_SOURCE_CLIENT_METRICS),
            ("interval.ms", Some("12345"), super::CONFIG_SOURCE_DEFAULT),
        ];
        for (key, want_value, want_source) in cases {
            assert!(
                (by_name[key].value.as_deref(), by_name[key].config_source)
                    == (want_value, want_source),
                "key {key}"
            );
        }
    }

    #[test]
    fn group_describe_merges_dynamic_overrides_with_defaults() {
        use crabka_metadata::GroupConfigRecord;

        use crate::coordinator::unified::streams::config::{
            KEY_NUM_STANDBY_REPLICAS, KEY_SESSION_TIMEOUT_MS, StreamsGroupConfig,
        };

        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "streams-app".into(),
            configs: BTreeMap::from([(KEY_NUM_STANDBY_REPLICAS.into(), "1".into())]),
        }));
        let result = super::describe_one(
            &image,
            crabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
                resource_type: super::RESOURCE_TYPE_GROUP,
                resource_name: "streams-app".into(),
                configuration_keys: Some(vec![
                    KEY_NUM_STANDBY_REPLICAS.into(),
                    KEY_SESSION_TIMEOUT_MS.into(),
                ]),
                ..Default::default()
            },
            300_000,
            &StreamsGroupConfig::default(),
        );
        let by_name: std::collections::HashMap<_, _> = result
            .configs
            .iter()
            .map(|entry| (entry.name.as_str(), entry))
            .collect();
        assert!(
            by_name[KEY_NUM_STANDBY_REPLICAS].value.as_deref() == Some("1")
                && by_name[KEY_NUM_STANDBY_REPLICAS].config_source
                    == super::CONFIG_SOURCE_DYNAMIC_GROUP
        );
        assert!(
            by_name[KEY_SESSION_TIMEOUT_MS].value.as_deref() == Some("45000")
                && by_name[KEY_SESSION_TIMEOUT_MS].config_source == super::CONFIG_SOURCE_DEFAULT
        );
    }

    fn anon() -> crabka_security::Principal {
        crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        }
    }

    #[test]
    fn topic_resource_denied_yields_topic_authorization_failed() {
        let authz = crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = MetadataImage::new(Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = super::resource_authz_failure(
            &authz,
            &image,
            &anon(),
            &peer,
            super::RESOURCE_TYPE_TOPIC,
            "t",
        );
        assert!(code == Some(crate::codes::TOPIC_AUTHORIZATION_FAILED));
        let res = super::denied_result(
            super::RESOURCE_TYPE_TOPIC,
            "t".into(),
            crate::codes::TOPIC_AUTHORIZATION_FAILED,
        );
        let expected = DescribeConfigsResult {
            error_code: crate::codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("authorization failed".to_string()),
            resource_type: super::RESOURCE_TYPE_TOPIC,
            resource_name: "t".to_string(),
            configs: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(res == expected);
    }

    #[test]
    fn broker_resource_denied_yields_cluster_authorization_failed() {
        let authz = crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = MetadataImage::new(Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = super::resource_authz_failure(
            &authz,
            &image,
            &anon(),
            &peer,
            super::RESOURCE_TYPE_BROKER,
            "1",
        );
        assert!(code == Some(crate::codes::CLUSTER_AUTHORIZATION_FAILED));
    }
}
