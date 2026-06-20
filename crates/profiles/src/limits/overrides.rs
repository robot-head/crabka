use std::collections::HashMap;

use serde::Deserialize;

use super::Limits;

/// Pyroscope-style runtime overrides resolved into full per-tenant limits.
#[derive(Clone, Debug)]
pub struct OverridesProvider {
    defaults: Limits,
    per_tenant: HashMap<String, Limits>,
}

impl OverridesProvider {
    #[must_use]
    pub fn new(defaults: Limits) -> Self {
        Self {
            defaults,
            per_tenant: HashMap::new(),
        }
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        Self::from_yaml_with_defaults(yaml, Limits::default())
    }

    pub fn from_yaml_with_defaults(yaml: &str, defaults: Limits) -> Result<Self, OverridesError> {
        let parsed: RuntimeFile =
            serde_yaml::from_str(yaml).map_err(|err| OverridesError::Yaml(err.to_string()))?;
        let per_tenant = parsed
            .overrides
            .into_iter()
            .map(|(tenant, partial)| (tenant, partial.merge_over(&defaults)))
            .collect();
        Ok(Self {
            defaults,
            per_tenant,
        })
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> &Limits {
        self.per_tenant.get(tenant).unwrap_or(&self.defaults)
    }

    #[must_use]
    pub fn has_tenant_override(&self, tenant: &str) -> bool {
        self.per_tenant.contains_key(tenant)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OverridesError {
    #[error("profiles overrides yaml: {0}")]
    Yaml(String),
}

#[derive(Debug, Deserialize)]
struct RuntimeFile {
    #[serde(default)]
    overrides: HashMap<String, PartialLimits>,
}

// Tenant entries are intentionally partial: Pyroscope overrides merge the
// tenant-specific fields over the process defaults.
#[derive(Debug, Default, Deserialize)]
struct PartialLimits {
    #[serde(default)]
    ingestion_rate_profiles_per_sec: Option<f64>,
    #[serde(default)]
    ingestion_burst_profiles: Option<u64>,
    #[serde(default)]
    max_series: Option<u64>,
    #[serde(default)]
    max_label_name_length: Option<u64>,
    #[serde(default)]
    max_label_value_length: Option<u64>,
    #[serde(default)]
    max_label_names_per_series: Option<u64>,
    #[serde(default)]
    max_flamegraph_nodes_default: Option<i64>,
    #[serde(default)]
    max_flamegraph_nodes_max: Option<i64>,
    #[serde(default)]
    max_query_length_secs: Option<u64>,
    #[serde(default)]
    max_session_id_cardinality: Option<u64>,
}

impl PartialLimits {
    fn merge_over(self, defaults: &Limits) -> Limits {
        Limits {
            ingestion_rate_profiles_per_sec: self
                .ingestion_rate_profiles_per_sec
                .unwrap_or(defaults.ingestion_rate_profiles_per_sec),
            ingestion_burst_profiles: self
                .ingestion_burst_profiles
                .unwrap_or(defaults.ingestion_burst_profiles),
            max_series: self.max_series.unwrap_or(defaults.max_series),
            max_label_name_length: self
                .max_label_name_length
                .unwrap_or(defaults.max_label_name_length),
            max_label_value_length: self
                .max_label_value_length
                .unwrap_or(defaults.max_label_value_length),
            max_label_names_per_series: self
                .max_label_names_per_series
                .unwrap_or(defaults.max_label_names_per_series),
            max_flamegraph_nodes_default: self
                .max_flamegraph_nodes_default
                .unwrap_or(defaults.max_flamegraph_nodes_default),
            max_flamegraph_nodes_max: self
                .max_flamegraph_nodes_max
                .unwrap_or(defaults.max_flamegraph_nodes_max),
            max_query_length_secs: self
                .max_query_length_secs
                .unwrap_or(defaults.max_query_length_secs),
            max_session_id_cardinality: self
                .max_session_id_cardinality
                .unwrap_or(defaults.max_session_id_cardinality),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const YAML: &str = r#"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 500
    max_series: 1000
  tenant-b:
    max_label_value_length: 64
"#;

    #[test]
    fn tenant_override_merges_over_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_a = provider.for_tenant("tenant-a");

        assert!(tenant_a.ingestion_rate_profiles_per_sec == 500.0);
        assert!(tenant_a.max_series == 1000);
        assert!(tenant_a.max_label_value_length == Limits::default().max_label_value_length);
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_b = provider.for_tenant("tenant-b");

        assert!(tenant_b.max_label_value_length == 64);
        assert!(
            tenant_b.ingestion_rate_profiles_per_sec
                == Limits::default().ingestion_rate_profiles_per_sec
        );
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();

        assert!(*provider.for_tenant("tenant-z") == Limits::default());
        assert!(!provider.has_tenant_override("tenant-z"));
        assert!(provider.has_tenant_override("tenant-a"));
    }
}
