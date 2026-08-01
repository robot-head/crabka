use std::collections::HashMap;

use crabka_units::{
    ByteSize, Frequency, Time,
    convert::{ByteSizeExt as _, FrequencyExt as _, TimeExt},
};
use serde::Deserialize;
use thiserror::Error;

use super::Limits;

#[derive(Clone, Debug)]
pub struct OverridesProvider {
    defaults: Limits,
    per_tenant: HashMap<String, Limits>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OverridesError {
    #[error("failed to parse overrides yaml: {0}")]
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

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError> {
        let defaults = Limits::default();
        let file = serde_yaml::from_str::<RuntimeFile>(yaml)
            .map_err(|err| OverridesError::Yaml(err.to_string()))?;
        let per_tenant = file
            .overrides
            .into_iter()
            .map(|(tenant, limits)| (tenant, merge_limits(&defaults, &limits)))
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

#[derive(Deserialize)]
struct RuntimeFile {
    #[serde(default)]
    overrides: HashMap<String, PartialLimits>,
}

// The Tempo-shaped runtime-overrides keys, in the units an operator writes them
// (spans/sec, bytes, seconds). This is intentionally partial configuration, not
// old-schema compatibility: each tenant entry overrides only the limit fields it
// names, and `merge_limits` lifts them into the dimensioned `Limits`.
#[derive(Default, Deserialize)]
#[serde(default)]
struct PartialLimits {
    ingestion_rate_spans_per_sec: Option<f64>,
    ingestion_burst_spans: Option<u64>,
    max_traces_per_search: Option<u64>,
    max_spans_per_trace: Option<u64>,
    max_attribute_bytes: Option<u64>,
    max_search_duration_secs: Option<u64>,
}

fn merge_limits(defaults: &Limits, partial: &PartialLimits) -> Limits {
    Limits {
        ingestion_rate: partial
            .ingestion_rate_spans_per_sec
            .map_or(defaults.ingestion_rate, Frequency::from_per_sec),
        ingestion_burst_spans: partial
            .ingestion_burst_spans
            .unwrap_or(defaults.ingestion_burst_spans),
        max_traces_per_search: partial
            .max_traces_per_search
            .unwrap_or(defaults.max_traces_per_search),
        max_spans_per_trace: partial
            .max_spans_per_trace
            .unwrap_or(defaults.max_spans_per_trace),
        max_attribute: partial
            .max_attribute_bytes
            .map_or(defaults.max_attribute, ByteSize::from_bytes),
        max_search_duration: partial
            .max_search_duration_secs
            .map_or(defaults.max_search_duration, |secs| {
                Time::from_secs(i64::try_from(secs).unwrap_or(i64::MAX))
            }),
    }
}

#[cfg(test)]
mod tests {
    use crabka_units::{bytes, per_sec};

    use super::*;
    use crate::limits::Limits;

    const YAML: &str = r"
overrides:
  tenant-a:
    ingestion_rate_spans_per_sec: 500
    max_spans_per_trace: 1000
  tenant-b:
    max_attribute_bytes: 64
";

    #[test]
    fn tenant_override_merges_over_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_a = provider.for_tenant("tenant-a");

        // Overridden fields take the yaml values; the rest keep the defaults.
        assert2::assert!(
            *tenant_a
                == Limits {
                    ingestion_rate: per_sec(500),
                    ingestion_burst_spans: 100_000,
                    max_traces_per_search: 1000,
                    max_spans_per_trace: 1000,
                    max_attribute: bytes(2048),
                    max_search_duration: <Time as TimeExt>::ZERO,
                }
        );
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_b = provider.for_tenant("tenant-b");

        assert2::assert!(tenant_b.max_attribute == bytes(64));
        assert2::assert!(tenant_b.ingestion_rate == Limits::default().ingestion_rate);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();

        assert2::assert!(*provider.for_tenant("tenant-z") == Limits::default());
    }
}
