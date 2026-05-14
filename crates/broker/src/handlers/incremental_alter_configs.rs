//! `IncrementalAlterConfigs` (`api_key=44`). Same target as `AlterConfigs`
//! (a single `V1TopicConfig` record per resource) but the wire request
//! carries per-key operations (SET/DELETE/APPEND/SUBTRACT). The handler
//! reads the current overrides from the metadata image, applies the ops,
//! validates the result, and submits the merged map.
//!
//! Slice-11 scope:
//! - SET (0)    — set/replace
//! - DELETE (1) — remove
//! - APPEND (2) and SUBTRACT (3) are list-valued operations. None of our
//!   whitelisted keys are list-valued, so we reject these with
//!   `INVALID_CONFIG`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, TopicConfigRecord};
use crabka_protocol::owned::incremental_alter_configs_request::IncrementalAlterConfigsRequest;
use crabka_protocol::owned::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::broker::Broker;
use crate::codes;
use crate::config_keys;
use crate::error::BrokerError;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

#[allow(clippy::too_many_lines)]
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
        let req = IncrementalAlterConfigsRequest::decode(&mut cur, version)?;

        let mut responses: Vec<AlterConfigsResourceResponse> =
            Vec::with_capacity(req.resources.len());

        for resource in req.resources {
            let mut out = AlterConfigsResourceResponse {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                error_code: codes::NONE,
                error_message: None,
                ..Default::default()
            };

            if resource.resource_type != RESOURCE_TYPE_TOPIC {
                out.error_code = codes::INVALID_RESOURCE_TYPE;
                out.error_message = Some(format!(
                    "resource_type={} not supported",
                    resource.resource_type
                ));
                responses.push(out);
                continue;
            }

            let image = controller.current_image();
            if image.topic(&resource.resource_name).is_none() {
                out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                out.error_message = Some(format!("unknown topic `{}`", resource.resource_name));
                responses.push(out);
                continue;
            }

            // Start from the current overrides; apply each op in order.
            let mut merged = image
                .topic_config(&resource.resource_name)
                .cloned()
                .unwrap_or_default();
            let mut validation_err: Option<String> = None;
            for cfg in &resource.configs {
                match cfg.config_operation {
                    OP_SET => {
                        let value = cfg.value.clone().unwrap_or_default();
                        if let Err(reason) = config_keys::validate_topic_config(&cfg.name, &value) {
                            validation_err = Some(reason);
                            break;
                        }
                        merged.insert(cfg.name.clone(), value);
                    }
                    OP_DELETE => {
                        if !config_keys::is_recognized(&cfg.name) {
                            validation_err =
                                Some(format!("unrecognized config key `{}`", cfg.name));
                            break;
                        }
                        merged.remove(&cfg.name);
                    }
                    op => {
                        validation_err = Some(format!(
                            "config_operation={op} (APPEND/SUBTRACT) not supported for key `{}` — \
                             only SET and DELETE are honored on this broker",
                            cfg.name
                        ));
                        break;
                    }
                }
            }
            if let Some(reason) = validation_err {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(reason);
                responses.push(out);
                continue;
            }

            let record = MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: resource.resource_name.clone(),
                overrides: merged,
            });
            if req.validate_only {
                // Validation pass already happened above (per-config loop). Nothing
                // to submit; the response already carries the per-resource result
                // (NONE if all configs validated, INVALID_CONFIG with reason on any
                // rejection). This matches Apache Kafka's --dry-run behavior.
                responses.push(out);
                continue;
            }
            match controller.submit_change(vec![record]).await {
                Ok(()) => {}
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    out.error_code = codes::NOT_CONTROLLER;
                }
                Err(e) => {
                    tracing::error!(error = %e, "IncrementalAlterConfigs submit_change failed");
                    out.error_code = codes::UNKNOWN_SERVER_ERROR;
                }
            }
            responses.push(out);
        }

        let resp = IncrementalAlterConfigsResponse {
            responses,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
