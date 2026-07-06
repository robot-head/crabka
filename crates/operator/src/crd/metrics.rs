//! `Kafka.spec.metricsConfig` — operator-side surface for the
//! broker's Prometheus `/metrics` endpoint.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    #[serde(default)]
    pub r#type: MetricsType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_monitor: Option<PodMonitorSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_monitor: Option<ServiceMonitorSpec>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MetricsType {
    #[default]
    Prometheus,
}

macro_rules! monitor_spec {
    ($name:ident) => {
        #[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub interval: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            pub scrape_timeout: Option<String>,
            #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
            pub labels: BTreeMap<String, String>,
        }
    };
}

monitor_spec!(PodMonitorSpec);
monitor_spec!(ServiceMonitorSpec);

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn metrics_config_defaults_type_prometheus() {
        let cfg: MetricsConfig = serde_json::from_str("{}").unwrap();
        assert!(
            cfg == MetricsConfig {
                r#type: MetricsType::Prometheus,
                pod_monitor: None,
                service_monitor: None,
            }
        );
    }

    #[test]
    fn metrics_config_round_trips() {
        let cfg = MetricsConfig {
            r#type: MetricsType::Prometheus,
            pod_monitor: Some(PodMonitorSpec {
                interval: Some("15s".into()),
                scrape_timeout: None,
                labels: [("team".to_string(), "platform".to_string())].into(),
            }),
            service_monitor: None,
        };
        let j = serde_json::to_string(&cfg).unwrap();
        assert!(j.contains("\"podMonitor\""));
        assert!(j.contains("\"interval\":\"15s\""));
        let back: MetricsConfig = serde_json::from_str(&j).unwrap();
        assert!(back == cfg);
    }

    #[test]
    fn metrics_type_rejects_unknown() {
        let err = serde_json::from_str::<MetricsType>("\"jmxExporter\"").unwrap_err();
        assert!(err.to_string().contains("prometheus"), "got: {err}");
    }
}
