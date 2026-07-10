// rustc 1.95 clippy::pedantic ICEs on this file (an upstream bug in
// clippy's body-analysis pass). Disable pedantic locally; the rest of
// the workspace still enforces the full pedantic gate.
#![allow(clippy::pedantic)]

//! Broker-side integration test for KIP-714 CLIENT_METRICS config round-trip.
//!
//! Drives `IncrementalAlterConfigs` → `DescribeConfigs` → `ListConfigResources`
//! for a CLIENT_METRICS subscription over the real in-process broker, asserting
//! the full operator-facing round-trip.

#![allow(clippy::default_trait_access)]

use assert2::{assert, check};
mod support;

use crabka_protocol::owned::{
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
    describe_configs_response::DescribeConfigsResponse,
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    list_config_resources_request::ListConfigResourcesRequest,
    list_config_resources_response::ListConfigResourcesResponse,
};
use support::start_n_node;

/// Kafka resource type id for CLIENT_METRICS (KIP-714).
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;

/// `config_operation` SET = 0 in the IncrementalAlterConfigs wire protocol.
const CONFIG_OP_SET: i8 = 0;

/// `config_source` CLIENT_METRICS_CONFIG = 7.
const CONFIG_SOURCE_CLIENT_METRICS: i8 = 7;

/// `config_source` DEFAULT_CONFIG = 5.
const CONFIG_SOURCE_DEFAULT: i8 = 5;

// ── helpers ───────────────────────────────────────────────────────────────────

async fn build_client(addr: std::net::SocketAddr) -> crabka_client_core::Client {
    crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", addr.port()))
        .client_id("client-metrics-config-test")
        .build()
        .await
        .expect("client build")
}

// ── test ──────────────────────────────────────────────────────────────────────

/// CONFIG ROUND-TRIP:
///
/// 1. `IncrementalAlterConfigs` for CLIENT_METRICS "sub-a" with
///    `metrics=org.apache.kafka.consumer.` and `interval.ms=60000` (SET ops).
///    Asserts per-resource error_code == NONE (0).
///
/// 2. `DescribeConfigs` for CLIENT_METRICS "sub-a". Asserts:
///    - `metrics` value == "org.apache.kafka.consumer.", config_source == 7.
///    - `interval.ms` value == "60000", config_source == 7.
///    - `match` entry present (defaulted), config_source == 5.
///
/// 3. `ListConfigResources` (v1, resource_types=[16]). Asserts the result
///    contains a ConfigResource with resource_type 16 and resource_name "sub-a".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_metrics_config_alter_describe_list_round_trip() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    // ── Step 1: IncrementalAlterConfigs ──────────────────────────────────────
    let alter_req = IncrementalAlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".to_string(),
            configs: vec![
                AlterableConfig {
                    name: "metrics".to_string(),
                    config_operation: CONFIG_OP_SET,
                    value: Some("org.apache.kafka.consumer.".to_string()),
                    ..Default::default()
                },
                AlterableConfig {
                    name: "interval.ms".to_string(),
                    config_operation: CONFIG_OP_SET,
                    value: Some("60000".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };

    let alter_resp: IncrementalAlterConfigsResponse = client
        .send(alter_req)
        .await
        .expect("IncrementalAlterConfigs");

    assert!(
        alter_resp.responses.len() == 1,
        "expected exactly one resource response, got {}",
        alter_resp.responses.len()
    );
    let resource_resp = &alter_resp.responses[0];
    assert!(
        resource_resp.error_code == 0,
        "IncrementalAlterConfigs CLIENT_METRICS must succeed; error_code={} message={:?}",
        resource_resp.error_code,
        resource_resp.error_message
    );

    // ── Step 2: DescribeConfigs ───────────────────────────────────────────────
    let describe_req = DescribeConfigsRequest {
        resources: vec![DescribeConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".to_string(),
            configuration_keys: None, // all keys
            ..Default::default()
        }],
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    };

    let describe_resp: DescribeConfigsResponse =
        client.send(describe_req).await.expect("DescribeConfigs");

    assert!(
        describe_resp.results.len() == 1,
        "expected exactly one describe result, got {}",
        describe_resp.results.len()
    );
    let result = &describe_resp.results[0];
    check!(
        (
            result.error_code,
            result.resource_type,
            result.resource_name.as_str(),
            result.configs.len(),
        ) == (0, RESOURCE_TYPE_CLIENT_METRICS, "sub-a", 3),
        "DescribeConfigs result mismatch: {result:?}"
    );

    // Assert the three expected config entries.
    let find_config = |name: &str| result.configs.iter().find(|c| c.name == name);

    assert!(
        (
            find_config("metrics").map(|cfg| (cfg.value.as_deref(), cfg.config_source)),
            find_config("interval.ms").map(|cfg| (cfg.value.as_deref(), cfg.config_source)),
            find_config("match").map(|cfg| (cfg.value.as_deref(), cfg.config_source)),
        ) == (
            Some((
                Some("org.apache.kafka.consumer."),
                CONFIG_SOURCE_CLIENT_METRICS,
            )),
            Some((Some("60000"), CONFIG_SOURCE_CLIENT_METRICS)),
            // The generated wire type decodes the defaulted empty value as
            // `Some("")`; keep that normalization explicit in the projection.
            Some((Some(""), CONFIG_SOURCE_DEFAULT)),
        )
    );

    // ── Step 3: ListConfigResources ───────────────────────────────────────────
    let list_req = ListConfigResourcesRequest {
        resource_types: vec![RESOURCE_TYPE_CLIENT_METRICS],
        ..Default::default()
    };

    let list_resp: ListConfigResourcesResponse =
        client.send(list_req).await.expect("ListConfigResources");

    let found = list_resp
        .config_resources
        .iter()
        .any(|r| r.resource_type == RESOURCE_TYPE_CLIENT_METRICS && r.resource_name == "sub-a");
    assert!(
        (list_resp.error_code, found) == (0, true),
        "ListConfigResources must contain CLIENT_METRICS 'sub-a'; got: {:?}",
        list_resp
            .config_resources
            .iter()
            .map(|r| (r.resource_type, &r.resource_name))
            .collect::<Vec<_>>()
    );
}
