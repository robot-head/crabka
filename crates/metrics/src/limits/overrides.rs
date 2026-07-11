use std::collections::HashMap;

use serde::Deserialize;
use thiserror::Error;

use super::Limits;

#[derive(Clone, Debug)]
pub struct OverridesProvider {
    defaults: Limits,
    per_tenant: HashMap<String, Limits>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OverridesError {
    #[error("failed to parse runtime overrides YAML: {0}")]
    Yaml(String),
}

impl OverridesProvider {
    #[must_use]
    pub fn new(defaults: Limits) -> Self {
        Self {
            defaults,
            per_tenant: HashMap::new(),
        }
    }

    /// Parse Mimir-style `runtime.yaml` overrides.
    ///
    /// Tenant maps are partial by design: `#[serde(default)]` below represents
    /// sparse per-tenant overrides, not backwards-compatibility migration.
    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        let runtime: RuntimeFile =
            serde_yaml::from_str(yaml).map_err(|error| OverridesError::Yaml(error.to_string()))?;
        let defaults = merge_limits(&Limits::default(), &runtime.defaults);
        let per_tenant = runtime
            .overrides
            .into_iter()
            .map(|(tenant, partial)| (tenant, merge_limits(&defaults, &partial)))
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
}

#[derive(Debug, Default, Deserialize)]
struct RuntimeFile {
    #[serde(default)]
    defaults: PartialLimits,
    #[serde(default)]
    overrides: HashMap<String, PartialLimits>,
}

#[derive(Debug, Default, Deserialize)]
struct PartialLimits {
    #[serde(default)]
    ingestion_rate: Option<f64>,
    #[serde(default)]
    ingestion_burst_size: Option<u64>,
    #[serde(default)]
    max_global_series_per_user: Option<u64>,
    #[serde(default)]
    max_label_name_length: Option<u64>,
    #[serde(default)]
    max_label_value_length: Option<u64>,
    #[serde(default)]
    max_samples_per_query: Option<u64>,
    #[serde(default)]
    max_fetched_series_per_query: Option<u64>,
    #[serde(default)]
    max_query_lookback_secs: Option<u64>,
    #[serde(default)]
    max_query_length_secs: Option<u64>,
    #[serde(default)]
    out_of_order_time_window_ms: Option<i64>,
}

/// Overlay a sparse per-tenant (or defaults) override on top of `base`.
///
/// Overrides are **fully trusted**: any field set in `partial` replaces the
/// corresponding `base` value verbatim, with no floor or hard cap applied. In
/// particular, a value of `0` is not rejected — for the limits that treat `0`
/// as a sentinel (`ingestion_rate`, `max_global_series_per_user`, the per-query
/// and query-range/lookback caps) this *disables* that cap for the tenant. This
/// matches Mimir's runtime-overrides semantics, where operator-supplied
/// overrides are authoritative and `0` means "unlimited".
fn merge_limits(base: &Limits, partial: &PartialLimits) -> Limits {
    Limits {
        ingestion_rate: partial.ingestion_rate.unwrap_or(base.ingestion_rate),
        ingestion_burst_size: partial
            .ingestion_burst_size
            .unwrap_or(base.ingestion_burst_size),
        max_global_series_per_user: partial
            .max_global_series_per_user
            .unwrap_or(base.max_global_series_per_user),
        max_label_name_length: partial
            .max_label_name_length
            .unwrap_or(base.max_label_name_length),
        max_label_value_length: partial
            .max_label_value_length
            .unwrap_or(base.max_label_value_length),
        max_samples_per_query: partial
            .max_samples_per_query
            .unwrap_or(base.max_samples_per_query),
        max_fetched_series_per_query: partial
            .max_fetched_series_per_query
            .unwrap_or(base.max_fetched_series_per_query),
        max_query_lookback_secs: partial
            .max_query_lookback_secs
            .unwrap_or(base.max_query_lookback_secs),
        max_query_length_secs: partial
            .max_query_length_secs
            .unwrap_or(base.max_query_length_secs),
        out_of_order_time_window_ms: partial
            .out_of_order_time_window_ms
            .unwrap_or(base.out_of_order_time_window_ms),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const YAML: &str = r"
overrides:
  tenant-a:
    ingestion_rate: 500
    max_global_series_per_user: 1000
  tenant-b:
    max_label_value_length: 64
  tenant-c:
    out_of_order_time_window_ms: 1500
";

    #[test]
    fn tenant_override_merges_over_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let a = p.for_tenant("tenant-a");
        assert2::assert!((a.ingestion_rate - 500.0).abs() < f64::EPSILON);
        assert2::assert!(a.max_global_series_per_user == 1000);
        assert2::assert!(a.max_label_name_length == Limits::default().max_label_name_length);
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let b = p.for_tenant("tenant-b");
        assert2::assert!(b.max_label_value_length == 64);
        assert2::assert!(
            (b.ingestion_rate - Limits::default().ingestion_rate).abs() < f64::EPSILON
        );
    }

    #[test]
    fn parses_out_of_order_window_override() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert2::assert!(p.for_tenant("tenant-c").out_of_order_time_window_ms == 1500);
        assert2::assert!(p.for_tenant("tenant-a").out_of_order_time_window_ms == 0);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert2::assert!(*p.for_tenant("tenant-z") == Limits::default());
    }
}
