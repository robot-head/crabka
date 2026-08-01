//! Per-broker capacity configuration. Loaded from a YAML file at
//! startup; threaded into `GoalContext` so the capacity goals can
//! enforce operator-supplied limits.
//!
//! Sparse-by-design: missing field = no limit for that resource on
//! that broker. Missing broker entry = no limits at all for that
//! broker. Both are operator-explicit "this is unconstrained"
//! signals.

pub mod load;

use std::collections::HashMap;

use crabka_units::{ByteRate, ByteSize};
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct BrokerCapacities {
    #[serde(default)]
    pub by_broker: HashMap<i32, BrokerCapacity>,
}

/// Limits for one broker. Field names are the keys of the operator-supplied
/// capacity YAML, so they keep their unit suffixes even though the types now
/// carry the dimension.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct BrokerCapacity {
    pub max_replicas: Option<u32>,
    #[serde(default, with = "crabka_units::serde_units::numeric::option_bytes_u64")]
    pub disk_bytes: Option<ByteSize>,
    #[serde(
        default,
        with = "crabka_units::serde_units::numeric::option_bytes_per_sec_i64"
    )]
    pub network_in_bytes_per_sec: Option<ByteRate>,
    #[serde(
        default,
        with = "crabka_units::serde_units::numeric::option_bytes_per_sec_i64"
    )]
    pub network_out_bytes_per_sec: Option<ByteRate>,
    pub cpu_cores: Option<f64>,
}

impl BrokerCapacities {
    /// Convenience lookup. Returns `None` if the broker has no entry
    /// at all (= entirely unconstrained).
    #[must_use]
    pub fn for_broker(&self, broker_id: i32) -> Option<&BrokerCapacity> {
        self.by_broker.get(&broker_id)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn default_is_empty() {
        let c = BrokerCapacities::default();
        assert2::assert!((c.by_broker.is_empty(), c.for_broker(1)) == (true, None));
    }
}
