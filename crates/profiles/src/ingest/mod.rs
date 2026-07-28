//! Decode targets and pre-WAL ingest pipeline helpers.
//!
//! Push doors lower into these types, then the distributor applies relabeling,
//! required labels, structural limits, and the `__session_id__` cardinality cap
//! before writing to the profile WAL.

pub mod legacy;
pub mod otlp;
pub mod push_v1;
pub mod split;

use std::collections::BTreeMap;

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;
use crabka_units::{ByteSize, bytes, convert::ByteSizeExt as _};
pub use legacy::{
    IngestFormat, IngestQuery, decode_ingest_body, decode_ingest_multipart, parse_ingest_query,
};
pub use otlp::decode_otlp;
pub use push_v1::{decode_push, gunzip};
use serde::{Deserialize, Serialize};
pub use split::split_sample_types;

use crate::error::ProfilesError;

/// One decoded pprof plus its series labels, before the multi-value split.
#[derive(Debug, Clone)]
pub struct RawProfile {
    pub labels: Labels,
    pub profile: PprofProfile,
    pub delta: bool,
    pub sample_timestamps_ns: Vec<Vec<i64>>,
    pub sample_span_ids: Vec<Option<u64>>,
    pub sample_trace_ids: Vec<Option<Vec<u8>>>,
}

/// One series after the multi-value split: a single `__profile_type__`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedProfile {
    pub labels: Labels,
    pub profile_type: String,
    pub samples: Vec<DecodedSample>,
}

/// One sample's raw payload, still unsymbolized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSample {
    pub stacktrace_location_refs: Vec<u32>,
    pub value: i64,
    pub timestamp_ns: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

/// Per-tenant ingest limits for structural validation.
///
/// Not `Eq`: the label caps are [`ByteSize`] quantities, which store `f64`.
/// These limits are only ever a map *value* (`TenantLimitConfig::tenants`), so
/// nothing needs the derive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantLimits {
    /// Cap on the UTF-8 bytes of a label name.
    #[serde(
        default = "default_max_label_name",
        with = "crabka_units::serde_units::human::byte_size"
    )]
    pub max_label_name: ByteSize,
    pub max_label_names_per_series: usize,
    /// Cap on the UTF-8 bytes of a label value.
    #[serde(with = "crabka_units::serde_units::human::byte_size")]
    pub max_label_value: ByteSize,
    pub session_id_buckets: u64,
}

const fn default_max_label_name() -> ByteSize {
    bytes(1024)
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_label_name: default_max_label_name(),
            max_label_names_per_series: 30,
            max_label_value: bytes(2048),
            session_id_buckets: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TenantLimitConfig {
    #[serde(default)]
    pub default: TenantLimits,
    #[serde(default)]
    pub tenants: BTreeMap<String, TenantLimits>,
}

impl TenantLimitConfig {
    #[must_use]
    pub fn with_tenant_limits(mut self, tenant: impl Into<String>, limits: TenantLimits) -> Self {
        self.tenants.insert(tenant.into(), limits);
        self
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> &TenantLimits {
        self.tenants.get(tenant).unwrap_or(&self.default)
    }
}

/// A Prometheus-style relabel rule subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelabelConfig {
    pub source_labels: Vec<String>,
    pub regex: String,
    pub target_label: String,
    pub replacement: String,
    pub action: RelabelAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelabelAction {
    Replace,
    Keep,
    Drop,
}

/// Inject `service_name="unknown_service"` when absent or empty.
pub fn require_service_name(labels: &mut Labels) {
    if labels.get("service_name").unwrap_or("").is_empty() {
        labels.insert("service_name", "unknown_service");
    }
}

/// Cardinality-cap `__session_id__` via stable modulo hash.
pub fn cap_session_id(labels: &mut Labels, buckets: u64) {
    let Some(raw) = labels.get("__session_id__").map(str::to_owned) else {
        return;
    };
    let bucket = fnv1a(raw.as_bytes()) % buckets.max(1);
    replace_label(labels, "__session_id__", &bucket.to_string());
}

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Enforce per-tenant structural caps.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn enforce_limits(labels: &Labels, limits: &TenantLimits) -> Result<(), ProfilesError> {
    if labels.len() > limits.max_label_names_per_series {
        return Err(ProfilesError::Invalid(format!(
            "too many label names: {} > {}",
            labels.len(),
            limits.max_label_names_per_series
        )));
    }

    for (name, value) in labels.iter() {
        if name.len() > limits.max_label_name.bytes_usize() {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` name exceeds {} bytes",
                limits.max_label_name.bytes_usize()
            )));
        }
        if value.len() > limits.max_label_value.bytes_usize() {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` value exceeds {} bytes",
                limits.max_label_value.bytes_usize()
            )));
        }
    }

    Ok(())
}

/// Apply relabel rules in order. Returns `false` when the series is rejected.
pub fn apply_relabel(labels: &mut Labels, configs: &[RelabelConfig]) -> bool {
    for config in configs {
        let joined = config
            .source_labels
            .iter()
            .map(|name| labels.get(name).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(";");
        let Ok(regex) = regex_anchored(&config.regex) else {
            continue;
        };
        let matched = regex.is_match(&joined);

        match config.action {
            RelabelAction::Drop if matched => return false,
            RelabelAction::Keep if !matched => return false,
            RelabelAction::Replace if matched => {
                if config.replacement.is_empty() {
                    remove_label(labels, &config.target_label);
                } else {
                    replace_label(labels, &config.target_label, &config.replacement);
                }
            }
            RelabelAction::Drop | RelabelAction::Keep | RelabelAction::Replace => {}
        }
    }
    true
}

fn regex_anchored(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
}

fn replace_label(labels: &mut Labels, target: &str, replacement: &str) {
    let mut rebuilt = Labels::new();
    for (name, value) in labels.iter() {
        if name != target {
            rebuilt.insert(name, value);
        }
    }
    rebuilt.insert(target, replacement);
    *labels = rebuilt;
}

fn remove_label(labels: &mut Labels, target: &str) {
    let mut rebuilt = Labels::new();
    for (name, value) in labels.iter() {
        if name != target {
            rebuilt.insert(name, value);
        }
    }
    *labels = rebuilt;
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_blockstore::Labels;

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    #[test]
    fn require_service_name_injects_unknown() {
        let mut labels = labels(&[("__name__", "process_cpu")]);
        require_service_name(&mut labels);
        assert!(labels.get("service_name") == Some("unknown_service"));
    }

    #[test]
    fn require_service_name_keeps_existing() {
        let mut labels = labels(&[("__name__", "process_cpu"), ("service_name", "api")]);
        require_service_name(&mut labels);
        assert!(labels.get("service_name") == Some("api"));
    }

    #[test]
    fn session_id_is_modulo_hashed() {
        let mut a = labels(&[("__session_id__", "deadbeefcafef00d")]);
        cap_session_id(&mut a, 16);
        let value = a.get("__session_id__").unwrap();
        let bucket: u64 = value.parse().unwrap();
        assert!(bucket < 16);

        let mut b = labels(&[("__session_id__", "deadbeefcafef00d")]);
        cap_session_id(&mut b, 16);
        assert!(b.get("__session_id__") == a.get("__session_id__"));
    }

    #[test]
    fn enforce_limits_rejects_too_many_labels() {
        let limits = TenantLimits {
            max_label_names_per_series: 1,
            ..Default::default()
        };
        let labels = labels(&[("a", "1"), ("b", "2")]);
        assert!(enforce_limits(&labels, &limits).is_err());
    }

    #[test]
    fn enforce_limits_rejects_too_long_label_names() {
        let limits = TenantLimits {
            max_label_name: bytes(3),
            ..Default::default()
        };
        let labels = labels(&[("too_long", "1")]);
        assert!(enforce_limits(&labels, &limits).is_err());
    }

    #[test]
    fn tenant_limit_config_uses_override_before_default() {
        let config = TenantLimitConfig::default().with_tenant_limits(
            "tenant-a",
            TenantLimits {
                max_label_names_per_series: 2,
                max_label_value: bytes(5),
                session_id_buckets: 8,
                ..Default::default()
            },
        );

        assert!(config.for_tenant("tenant-a").max_label_value == bytes(5));
        assert!(config.for_tenant("tenant-b") == &TenantLimits::default());
    }

    #[test]
    fn relabel_drop_rejects_series() {
        let mut labels = labels(&[("env", "dev"), ("__name__", "cpu")]);
        let config = RelabelConfig {
            source_labels: vec!["env".to_string()],
            regex: "dev".to_string(),
            target_label: String::new(),
            replacement: String::new(),
            action: RelabelAction::Drop,
        };
        assert!(!apply_relabel(&mut labels, &[config]));
    }
}
