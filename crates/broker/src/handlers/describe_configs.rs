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
use futures_util::future::BoxFuture;

use crabka_protocol::owned::describe_configs_request::DescribeConfigsRequest;
use crabka_protocol::owned::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

/// `ConfigSource::DYNAMIC_TOPIC_CONFIG` — the value Kafka uses for per-topic
/// overrides stored in `ZooKeeper` / `KRaft` metadata.
///
/// From `org.apache.kafka.clients.admin.ConfigEntry.ConfigSource`:
/// `DYNAMIC_TOPIC_CONFIG = 1`, `DYNAMIC_BROKER_CONFIG = 2`,
/// `DYNAMIC_DEFAULT_BROKER_CONFIG = 3`, `STATIC_BROKER_CONFIG = 4`,
/// `DEFAULT_CONFIG = 5`, `DYNAMIC_BROKER_LOGGER_CONFIG = 6`.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
const CONFIG_SOURCE_DYNAMIC_BROKER: i8 = 2;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

/// Produce a `DescribeConfigsResourceResult` for a single `(key, value)` pair.
fn make_entry(key: &str, value: &str, config_source: i8) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: key.to_owned(),
        value: Some(value.to_owned()),
        read_only: false,
        config_source,
        is_sensitive: false,
        synonyms: Vec::new(),
        config_type: 0,
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

    // All other resource types: empty configs, no error.
    ok(Vec::new())
}

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DescribeConfigsRequest::decode(&mut cur, version)?;

        let image = controller.current_image();
        let results: Vec<DescribeConfigsResult> = req
            .resources
            .into_iter()
            .map(|r| describe_one(&image, r))
            .collect();

        let resp = DescribeConfigsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crabka_metadata::{BrokerConfigRecord, MetadataImage, MetadataRecord};
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
        assert_eq!(
            map.get("leader.replication.throttled.rate")
                .map(String::as_str),
            Some("1024")
        );
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

        assert_eq!(filtered.len(), 1);
        assert!(filtered.contains_key("leader.replication.throttled.rate"));
        assert!(!filtered.contains_key("follower.replication.throttled.rate"));
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
        assert_eq!(super::CONFIG_SOURCE_DYNAMIC_BROKER, 2i8);
    }
}
