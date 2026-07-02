//! `DescribeConfigs` (`api_key=32`). Returns dynamic (override) configs
//! stored in the metadata image.
//!
//! - `resource_type=2` (TOPIC): reads per-topic override map, emits entries
//!   with `config_source = DYNAMIC_TOPIC_CONFIG (1)`.
//! - `resource_type=4` (BROKER): parses the resource name as a `NodeId`,
//!   reads the broker override map from `MetadataImage::broker_config`, emits
//!   entries with `config_source = DYNAMIC_BROKER_CONFIG (2)`.
//! - All other resource types receive an empty configs list with no error —
//!   the JVM `AdminClient` tolerates that.
//!
//! The `configuration_keys` filter on the request is honored: if the client
//! supplies an explicit key list, only those keys are returned.

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

/// `ConfigSource::DYNAMIC_TOPIC_CONFIG` — the value Kafka uses for per-topic
/// overrides stored in `ZooKeeper` / `KRaft` metadata.
///
/// From `org.apache.kafka.clients.admin.ConfigEntry.ConfigSource`:
/// `DYNAMIC_TOPIC_CONFIG = 1`, `DYNAMIC_BROKER_CONFIG = 2`,
/// `DYNAMIC_DEFAULT_BROKER_CONFIG = 3`, `STATIC_BROKER_CONFIG = 4`,
/// `DEFAULT_CONFIG = 5`, `DYNAMIC_BROKER_LOGGER_CONFIG = 6`.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
const CONFIG_SOURCE_DYNAMIC_BROKER: i8 = 2;
/// `ConfigSource::DEFAULT_CONFIG` — used for keys reported at their default.
const CONFIG_SOURCE_DEFAULT: i8 = 5;
/// `DescribeConfigsResponse.ConfigSource::CLIENT_METRICS_CONFIG` wire byte.
const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;

/// `ConfigDef.Type::UNKNOWN` wire byte — Crabka doesn't report typed config
/// metadata, matching brokers that predate KIP-226's typed responses.
const CONFIG_TYPE_UNKNOWN: i8 = 0;

/// Produce a `DescribeConfigsResourceResult` for a single `(key, value)` pair.
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

/// Dispatch a single resource entry from a `DescribeConfigs` request.
fn describe_one(
    image: &crabka_metadata::MetadataImage,
    r: crabka_protocol::owned::describe_configs_request::DescribeConfigsResource,
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
        let map = image.broker_config(node_id).cloned().unwrap_or_default();
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let configs: Vec<DescribeConfigsResourceResult> = map
            .iter()
            .filter(|(k, _)| key_filter.is_none_or(|ks| ks.iter().any(|f| f == *k)))
            .map(|(k, v)| make_entry(k, v, CONFIG_SOURCE_DYNAMIC_BROKER))
            .collect();
        return ok(configs);
    }

    if r.resource_type == RESOURCE_TYPE_CLIENT_METRICS {
        use crate::client_metrics::config::{
            DEFAULT_INTERVAL_MS, KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS,
        };
        let overrides = image
            .client_metrics_config(&r.resource_name)
            .cloned()
            .unwrap_or_default();
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let mut configs = Vec::new();
        // Emit all three keys: set values use CLIENT_METRICS_CONFIG source;
        // unset keys report their default value/source (KAFKA-17516 — tooling
        // needs effective values, not blanks).
        let default_interval = DEFAULT_INTERVAL_MS.to_string();
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

    // All other resource types: empty configs, no error.
    ok(Vec::new())
}

/// Per-resource `DescribeConfigs` ACL check. Topic resources require
/// `DescribeConfigs` on `Topic(name)`; Broker resources require
/// `DescribeConfigs` on `Cluster("kafka-cluster")`. Returns the
/// authorization-failed code to stamp on Deny, or `None` when allowed (or
/// for resource types we don't gate — they get an empty configs list with
/// no error, as before).
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

/// Build a `DescribeConfigsResult` carrying only the authorization-failed
/// error code for a denied resource.
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

#[allow(clippy::unused_async)] // async to match the inline-intercept handler shape.
#[tracing::instrument(
    name = "handle_describe_configs",
    level = "info",
    skip_all,
    fields(api = "DescribeConfigs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
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
                    describe_one(&image, r)
                }
            })
            .collect();

        let resp = DescribeConfigsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
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

    /// Build a minimal `MetadataImage` with one broker config entry.
    fn image_with_broker_config(node_id: u64, key: &str, value: &str) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: key.to_string(),
            config_value: Some(value.to_string()),
        }));
        img
    }

    #[test]
    fn broker_resource_name_invalid_fails_parse() {
        // Non-numeric resource_name must fail to parse as NodeId.
        assert!("not-a-number".parse::<u64>().is_err());
        // Empty string also fails.
        assert!("".parse::<u64>().is_err());
    }

    #[test]
    fn broker_resource_all_keys_returned_when_no_filter() {
        let img = image_with_broker_config(1, "leader.replication.throttled.rate", "1024");
        let map = img.broker_config(1).cloned().unwrap_or_default();
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
            node_id: 2,
            config_name: "leader.replication.throttled.rate".to_string(),
            config_value: Some("512".to_string()),
        }));
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 2,
            config_name: "follower.replication.throttled.rate".to_string(),
            config_value: Some("256".to_string()),
        }));

        let map = img.broker_config(2).cloned().unwrap_or_default();
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
        let map = img.broker_config(99).cloned().unwrap_or_default();
        assert!(map.is_empty());
    }

    #[test]
    fn config_source_dynamic_broker_is_2() {
        assert!(super::CONFIG_SOURCE_DYNAMIC_BROKER == 2i8);
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
        let res = super::describe_one(&img, r);
        assert_eq!(res.error_code, crate::codes::NONE);
        let by_name: std::collections::HashMap<_, _> =
            res.configs.iter().map(|c| (c.name.as_str(), c)).collect();
        let cases = [
            ("metrics", Some("a."), super::CONFIG_SOURCE_CLIENT_METRICS),
            ("interval.ms", Some("300000"), super::CONFIG_SOURCE_DEFAULT),
        ];
        for (key, want_value, want_source) in cases {
            assert!(
                (by_name[key].value.as_deref(), by_name[key].config_source)
                    == (want_value, want_source),
                "key {key}"
            );
        }
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
