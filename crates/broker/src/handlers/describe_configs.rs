//! `DescribeConfigs` (`api_key=32`). Returns dynamic (override) topic configs
//! stored in the metadata image. Only `resource_type=2` (TOPIC) is handled;
//! all other resource types receive an empty configs list with no error — the
//! JVM `AdminClient` tolerates that.
//!
//! `config_source` is set to `1` (`DYNAMIC_TOPIC_CONFIG`) for any key that
//! has a stored override. The `configuration_keys` filter on the request is
//! honored: if the client supplies an explicit key list, only those keys are
//! returned.

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

const RESOURCE_TYPE_TOPIC: i8 = 2;

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
            .map(|r| {
                // Only TOPIC resources are backed by the metadata image.
                // Other types (BROKER, BROKER_LOGGER, etc.) receive an empty
                // configs list — the JVM AdminClient is tolerant of this.
                let configs = if r.resource_type == RESOURCE_TYPE_TOPIC {
                    let overrides = image.topic_config(&r.resource_name);
                    match overrides {
                        None => Vec::new(),
                        Some(map) => {
                            // Honor the per-request key filter when present.
                            let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
                            map.iter()
                                .filter(|(k, _)| {
                                    key_filter.is_none_or(|ks| ks.iter().any(|f| f == *k))
                                })
                                .map(|(k, v)| DescribeConfigsResourceResult {
                                    name: k.clone(),
                                    value: Some(v.clone()),
                                    read_only: false,
                                    config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                                    is_sensitive: false,
                                    synonyms: Vec::new(),
                                    config_type: 0,
                                    documentation: None,
                                    ..Default::default()
                                })
                                .collect()
                        }
                    }
                } else {
                    Vec::new()
                };

                DescribeConfigsResult {
                    error_code: codes::NONE,
                    error_message: None,
                    resource_type: r.resource_type,
                    resource_name: r.resource_name,
                    configs,
                    ..Default::default()
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
    })
}
