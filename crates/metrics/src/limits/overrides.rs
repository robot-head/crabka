use std::collections::HashMap;

use crabka_units::{prelude::*, serde_units};
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

    /// Parses Mimir-style `runtime.yaml` overrides.
    ///
    /// Tenant maps are partial by design. The `#[serde(default)]` below
    /// represents sparse per-tenant overrides, and not a
    /// backwards-compatibility migration.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
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
    #[serde(default, with = "serde_units::human::option_frequency")]
    ingestion_rate: Option<Frequency>,
    #[serde(default)]
    ingestion_burst_size: Option<u64>,
    #[serde(default)]
    max_global_series_per_user: Option<u64>,
    #[serde(default, with = "serde_units::human::option_byte_size")]
    max_label_name_length: Option<ByteSize>,
    #[serde(default, with = "serde_units::human::option_byte_size")]
    max_label_value_length: Option<ByteSize>,
    #[serde(default)]
    max_samples_per_query: Option<u64>,
    #[serde(default)]
    max_fetched_series_per_query: Option<u64>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_time::deserialize"
    )]
    max_query_lookback: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_time::deserialize"
    )]
    max_query_length: Option<Time>,
    #[serde(default, with = "serde_units::human::option_time")]
    out_of_order_time_window: Option<Time>,
}

/// Overlays a sparse per-tenant override, or a defaults override, on top of
/// `base`.
///
/// Overrides are **fully trusted**. Any field set in `partial` replaces the
/// matching `base` value verbatim, with no floor and no hard cap. A value of `0`
/// is not rejected. For the limits that treat `0` as a sentinel, this *turns
/// off* that cap for the tenant. Those limits are `ingestion_rate`,
/// `max_global_series_per_user`, and the per-query, query-range, and lookback
/// caps. This matches Mimir's runtime-overrides semantics, where an
/// operator-supplied override is authoritative and `0` means "unlimited".
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
        max_query_lookback: partial
            .max_query_lookback
            .unwrap_or(base.max_query_lookback),
        max_query_length: partial.max_query_length.unwrap_or(base.max_query_length),
        out_of_order_time_window: partial
            .out_of_order_time_window
            .unwrap_or(base.out_of_order_time_window),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    const YAML: &str = r#"
overrides:
  tenant-a:
    ingestion_rate: "500/s"
    max_global_series_per_user: 1000
  tenant-b:
    max_label_value_length: "64B"
  tenant-c:
    out_of_order_time_window: "1500ms"
    max_query_length: "1h"
    max_query_lookback: "7d"
"#;

    #[test]
    fn tenant_override_merges_over_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let a = p.for_tenant("tenant-a");
        check!(a.ingestion_rate == per_sec(500));
        check!(a.max_global_series_per_user == 1000);
        check!(a.max_label_name_length == Limits::default().max_label_name_length);
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let b = p.for_tenant("tenant-b");
        assert!(b.max_label_value_length == bytes(64));
        assert!(b.ingestion_rate == Limits::default().ingestion_rate);
    }

    #[test]
    fn parses_out_of_order_window_override() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert!(p.for_tenant("tenant-c").out_of_order_time_window == millis(1500));
        assert!(p.for_tenant("tenant-a").out_of_order_time_window == Time::ZERO);
    }

    #[test]
    fn parses_query_span_cap_overrides() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let c = p.for_tenant("tenant-c");
        check!(c.max_query_length == hours(1));
        check!(c.max_query_lookback == days(7));
        check!(p.for_tenant("tenant-a").max_query_length == Time::ZERO);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert!(*p.for_tenant("tenant-z") == Limits::default());
    }

    #[test]
    fn dimensioned_override_without_a_unit_is_rejected() {
        // A bare `30` for a window that used to be `_ms` must not be guessed at;
        // the human encoding demands the unit the type now carries.
        let error = OverridesProvider::from_yaml(
            "overrides:\n  tenant-a:\n    out_of_order_time_window: 1500\n",
        )
        .unwrap_err();

        assert!(matches!(error, OverridesError::Yaml(_)));
    }

    /// A negative cap would read as "unlimited" downstream, because the
    /// enforcer applies only a cap greater than zero. Zero is the documented
    /// sentinel.
    #[test]
    fn negative_query_span_caps_are_rejected() {
        const NEGATIVE: &str = "overrides:\n  tenant-a:\n    max_query_length: \"-1s\"\n";
        const ZERO: &str = "overrides:\n  tenant-a:\n    max_query_length: \"0\"\n";

        assert!(let Err(_) = OverridesProvider::from_yaml(NEGATIVE));
        assert!(let Ok(_) = OverridesProvider::from_yaml(ZERO));
    }
}
