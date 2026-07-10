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
        let mut per_tenant = HashMap::new();
        for (tenant, partial) in parsed.overrides {
            partial.validate(&tenant)?;
            per_tenant.insert(tenant, partial.merge_over(&defaults));
        }
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
    #[error("profiles overrides for tenant {tenant:?}: {reason}")]
    Invalid { tenant: String, reason: String },
}

// `deny_unknown_fields` rejects typo'd / unsupported keys at load instead of
// silently ignoring them (a footgun where an operator's intended limit never
// takes effect).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeFile {
    #[serde(default)]
    overrides: HashMap<String, PartialLimits>,
}

// Tenant entries are intentionally partial: Pyroscope overrides merge the
// tenant-specific fields over the process defaults. Unknown keys are rejected
// (see `RuntimeFile`).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Validate numeric ranges before the partial is merged into full limits.
    ///
    /// Rejects (with [`OverridesError::Invalid`]):
    /// - a non-finite (`NaN`/`inf`) or negative `ingestion_rate_profiles_per_sec`,
    /// - a negative flamegraph node cap (`max_flamegraph_nodes_default` /
    ///   `max_flamegraph_nodes_max`).
    ///
    /// The remaining caps are `u64` and therefore cannot be negative; an
    /// out-of-range YAML literal for them is already rejected by serde during
    /// deserialization.
    fn validate(&self, tenant: &str) -> Result<(), OverridesError> {
        let invalid = |reason: &str| OverridesError::Invalid {
            tenant: tenant.to_string(),
            reason: reason.to_string(),
        };
        if let Some(rate) = self.ingestion_rate_profiles_per_sec
            && (!rate.is_finite() || rate < 0.0)
        {
            return Err(invalid(
                "ingestion_rate_profiles_per_sec must be finite and >= 0",
            ));
        }
        if let Some(nodes) = self.max_flamegraph_nodes_default
            && nodes < 0
        {
            return Err(invalid("max_flamegraph_nodes_default must be >= 0"));
        }
        if let Some(nodes) = self.max_flamegraph_nodes_max
            && nodes < 0
        {
            return Err(invalid("max_flamegraph_nodes_max must be >= 0"));
        }
        Ok(())
    }

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
    use assert2::{assert, check};

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

        assert!(
            *tenant_a
                == Limits {
                    ingestion_rate_profiles_per_sec: 500.0,
                    ingestion_burst_profiles: 10_000,
                    max_series: 1000,
                    max_label_name_length: 1024,
                    max_label_value_length: 2048,
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 2048,
                    max_flamegraph_nodes_max: 0,
                    max_query_length_secs: 2_595_600,
                    max_session_id_cardinality: 0,
                }
        );
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_b = provider.for_tenant("tenant-b");

        assert_eq!(
            (
                tenant_b.max_label_value_length,
                tenant_b.ingestion_rate_profiles_per_sec,
            ),
            (64, Limits::default().ingestion_rate_profiles_per_sec)
        );
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();

        check!(*provider.for_tenant("tenant-z") == Limits::default());
        check!(!provider.has_tenant_override("tenant-z"));
        check!(provider.has_tenant_override("tenant-a"));
    }

    #[test]
    fn unknown_tenant_key_is_rejected() {
        // `max_serie` is a typo of `max_series`; with `deny_unknown_fields`
        // this is now a load error rather than a silently-ignored field.
        let err = OverridesProvider::from_yaml(
            r#"
overrides:
  tenant-a:
    max_serie: 1000
"#,
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Yaml(_)), "{err:?}");
    }

    #[test]
    fn unknown_top_level_key_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r#"
overrides: {}
bogus_top_level: true
"#,
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Yaml(_)), "{err:?}");
    }

    #[test]
    fn negative_ingestion_rate_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r#"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: -1
"#,
        )
        .unwrap_err();

        assert!(
            matches!(err, OverridesError::Invalid { ref tenant, .. } if tenant == "tenant-a"),
            "{err:?}"
        );
    }

    #[test]
    fn non_finite_ingestion_rate_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r#"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: .nan
"#,
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Invalid { .. }), "{err:?}");
    }

    #[test]
    fn negative_flamegraph_node_cap_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r#"
overrides:
  tenant-a:
    max_flamegraph_nodes_max: -5
"#,
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Invalid { .. }), "{err:?}");
    }

    #[test]
    fn zero_and_positive_numeric_values_are_accepted() {
        let provider = OverridesProvider::from_yaml(
            r#"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 0
    max_flamegraph_nodes_default: 0
    max_flamegraph_nodes_max: 4096
"#,
        )
        .unwrap();
        let tenant_a = provider.for_tenant("tenant-a");

        assert!(
            *tenant_a
                == Limits {
                    ingestion_rate_profiles_per_sec: 0.0,
                    ingestion_burst_profiles: 10_000,
                    max_series: 0,
                    max_label_name_length: 1024,
                    max_label_value_length: 2048,
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 0,
                    max_flamegraph_nodes_max: 4096,
                    max_query_length_secs: 2_595_600,
                    max_session_id_cardinality: 0,
                }
        );
    }
}
