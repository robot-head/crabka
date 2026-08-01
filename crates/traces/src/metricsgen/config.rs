//! Metrics-generator configuration.

use crabka_units::{Time, secs};
use serde::{Deserialize, Serialize};

/// Tempo-default latency histogram bucket edges, in nanoseconds.
pub const DEFAULT_LATENCY_BUCKETS_NS: &[f64] = &[
    2_000_000.0,
    4_000_000.0,
    8_000_000.0,
    16_000_000.0,
    32_000_000.0,
    64_000_000.0,
    128_000_000.0,
    256_000_000.0,
    512_000_000.0,
    1_024_000_000.0,
    2_048_000_000.0,
    4_096_000_000.0,
    8_192_000_000.0,
    16_384_000_000.0,
];

/// Metrics-generator runtime configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsGenConfig {
    #[serde(
        rename = "collection_interval_secs",
        with = "crabka_units::serde_units::numeric::secs_i64"
    )]
    pub collection_interval: Time,
    pub histogram_buckets_ns: Vec<f64>,
    pub max_exemplars_per_series: usize,
    #[serde(
        rename = "edge_ttl_secs",
        with = "crabka_units::serde_units::numeric::secs_i64"
    )]
    pub edge_ttl: Time,
    pub edge_store_max_items: usize,
    pub enable_target_info: bool,
    pub enable_status_message: bool,
    pub enable_messaging_system_latency: bool,
    pub remote_write_url: String,
}

impl Default for MetricsGenConfig {
    fn default() -> Self {
        Self {
            collection_interval: secs(15),
            histogram_buckets_ns: DEFAULT_LATENCY_BUCKETS_NS.to_vec(),
            max_exemplars_per_series: 0,
            edge_ttl: secs(10),
            edge_store_max_items: 10_000,
            enable_target_info: false,
            enable_status_message: false,
            enable_messaging_system_latency: false,
            remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_tempo() {
        let c = MetricsGenConfig::default();
        assert2::assert!(
            c == MetricsGenConfig {
                collection_interval: secs(15),
                histogram_buckets_ns: DEFAULT_LATENCY_BUCKETS_NS.to_vec(),
                max_exemplars_per_series: 0,
                edge_ttl: secs(10),
                edge_store_max_items: 10_000,
                enable_target_info: false,
                enable_status_message: false,
                enable_messaging_system_latency: false,
                remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
            }
        );
    }

    #[test]
    fn parses_partial_yaml_falling_back_to_defaults() {
        let c: MetricsGenConfig =
            serde_yaml::from_str("collection_interval_secs: 30\nmax_exemplars_per_series: 5\n")
                .unwrap();
        assert2::assert!(
            c == MetricsGenConfig {
                collection_interval: secs(30),
                histogram_buckets_ns: DEFAULT_LATENCY_BUCKETS_NS.to_vec(),
                max_exemplars_per_series: 5,
                edge_ttl: secs(10),
                edge_store_max_items: 10_000,
                enable_target_info: false,
                enable_status_message: false,
                enable_messaging_system_latency: false,
                remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
            }
        );
    }
}
