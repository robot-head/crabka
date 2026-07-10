//! DSL config objects mirroring JVM `Grouped`/`Materialized`/`Repartitioned`.
//! `Consumed`/`Produced` are reused from `crate::processor::serde`.

/// Serdes (+ optional repartition name) for `groupBy`/`groupByKey`.
#[derive(Debug, Clone)]
pub struct Grouped<KS, VS> {
    #[allow(dead_code)]
    pub(crate) key_serde: KS,
    #[allow(dead_code)]
    pub(crate) value_serde: VS,
    pub(crate) name: Option<String>,
}
impl<KS, VS> Grouped<KS, VS> {
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self {
            key_serde,
            value_serde,
            name: None,
        }
    }
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

/// Versioned-store settings for a `Materialized` (KIP-889). `segment_interval_ms`
/// only affects JVM eviction granularity (non-observable here); accepted for API
/// parity. `None` segment interval uses the JVM default segment heuristic.
#[derive(Debug, Clone, Copy)]
pub struct VersionedConfig {
    pub history_retention_ms: i64,
    pub segment_interval_ms: Option<i64>,
}

/// Store name + serdes + logging flag for a materialized `KTable`.
#[derive(Debug, Clone)]
pub struct Materialized<KS, VS> {
    #[allow(dead_code)]
    pub(crate) key_serde: KS,
    #[allow(dead_code)]
    pub(crate) value_serde: VS,
    pub(crate) store_name: Option<String>,
    pub(crate) logging: bool,
    pub(crate) caching: bool,
    pub(crate) versioned: Option<VersionedConfig>,
}
impl<KS, VS> Materialized<KS, VS> {
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self {
            key_serde,
            value_serde,
            store_name: None,
            logging: true,
            caching: true,
            versioned: None,
        }
    }
    #[must_use]
    pub fn as_store(mut self, name: impl Into<String>) -> Self {
        self.store_name = Some(name.into());
        self
    }
    #[must_use]
    pub fn with_logging(mut self, on: bool) -> Self {
        self.logging = on;
        self
    }
    /// Enable/disable the record cache for this store (JVM
    /// `Materialized.withCachingEnabled/Disabled`). Default enabled.
    #[must_use]
    pub fn with_caching(mut self, on: bool) -> Self {
        self.caching = on;
        self
    }
    #[must_use]
    pub fn caching_enabled(&self) -> bool {
        self.caching
    }
    /// Materialize this table into a versioned key-value store (KIP-889) named
    /// `name`, retaining `history_retention_ms` of version history.
    #[must_use]
    pub fn as_versioned(mut self, name: impl Into<String>, history_retention_ms: i64) -> Self {
        self.store_name = Some(name.into());
        self.versioned = Some(VersionedConfig {
            history_retention_ms,
            segment_interval_ms: None,
        });
        self
    }
}

impl<KS, VS> From<(KS, VS)> for Materialized<KS, VS> {
    fn from((key_serde, value_serde): (KS, VS)) -> Self {
        Self::with(key_serde, value_serde)
    }
}

impl<KS, VS> From<(KS, VS)> for Grouped<KS, VS> {
    fn from((key_serde, value_serde): (KS, VS)) -> Self {
        Self::with(key_serde, value_serde)
    }
}

impl<KS, VS> From<(KS, VS)> for Repartitioned<KS, VS> {
    fn from((key_serde, value_serde): (KS, VS)) -> Self {
        Self::with(key_serde, value_serde)
    }
}

impl<KS, V1S, V2S> From<(KS, V1S, V2S)> for StreamJoined<KS, V1S, V2S> {
    fn from((key_serde, value1_serde, value2_serde): (KS, V1S, V2S)) -> Self {
        Self::with(key_serde, value1_serde, value2_serde)
    }
}

/// Serdes for a windowed `KStream`-`KStream` join (`StreamJoined`): the key
/// serde plus the per-side value serdes (`value1` for this/left side, `value2`
/// for the other/right side). The two join-window stores are registered with
/// `key_serde` + the matching value serde, mirroring JVM `StreamJoined.with`.
#[allow(clippy::struct_field_names)] // key/value serdes mirror JVM `StreamJoined`
#[derive(Debug, Clone, Copy)]
pub struct StreamJoined<KS, V1S, V2S> {
    pub(crate) key_serde: KS,
    pub(crate) value1_serde: V1S,
    pub(crate) value2_serde: V2S,
}
impl<KS, V1S, V2S> StreamJoined<KS, V1S, V2S> {
    pub fn with(key_serde: KS, value1_serde: V1S, value2_serde: V2S) -> Self {
        Self {
            key_serde,
            value1_serde,
            value2_serde,
        }
    }
}

/// Stream–table join config (KIP-923). Carries an optional grace period that
/// buffers stream records so out-of-order records still join the table value
/// as-of their own timestamp. `grace_ms` requires the joined table to be
/// versioned and must be `< history_retention_ms` (asserted at build time by the
/// join DSL). `name` optionally names the grace buffer store.
#[derive(Debug, Clone, Default)]
pub struct Joined {
    pub(crate) grace_ms: Option<i64>,
    pub(crate) name: Option<String>,
}
impl Joined {
    #[must_use]
    pub fn with_grace_period(grace_ms: i64) -> Self {
        Self {
            grace_ms: Some(grace_ms),
            name: None,
        }
    }
    #[must_use]
    pub fn as_named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

/// Serdes + optional name/partitions for an explicit `repartition()`.
#[derive(Debug, Clone)]
pub struct Repartitioned<KS, VS> {
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    pub(crate) name: Option<String>,
    pub(crate) partitions: Option<i32>,
}
impl<KS, VS> Repartitioned<KS, VS> {
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self {
            key_serde,
            value_serde,
            name: None,
            partitions: None,
        }
    }
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    #[must_use]
    pub fn num_partitions(mut self, n: i32) -> Self {
        self.partitions = Some(n);
        self
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    #[test]
    fn grouped_materialized_repartitioned_carry_serdes_and_names() {
        let g = Grouped::with(StringSerde, I64Serde).with_name("g1");
        let m = Materialized::with(StringSerde, I64Serde).as_store("counts");
        let r = Repartitioned::with(StringSerde, I64Serde)
            .with_name("rp")
            .num_partitions(4);
        check!(
            (
                g.name.as_deref(),
                m.store_name.as_deref(),
                m.logging,
                r.name.as_deref(),
                r.partitions,
            ) == (Some("g1"), Some("counts"), true, Some("rp"), Some(4))
        );
    }

    #[test]
    fn materialized_caching_defaults_on_and_toggles() {
        let m = Materialized::with(StringSerde, I64Serde);
        check!(m.caching_enabled()); // default true
        let m = m.with_caching(false);
        check!(!m.caching_enabled());
    }

    #[test]
    fn joined_carries_grace_and_name() {
        let j = Joined::with_grace_period(5_000).as_named("jb");
        check!(
            (j.grace_ms, j.name.as_deref(), Joined::default().grace_ms)
                == (Some(5_000), Some("jb"), None)
        );
    }

    #[test]
    fn materialized_as_versioned_sets_config() {
        let m = Materialized::with(StringSerde, I64Serde).as_versioned("vstore", 600_000);
        let vc = m.versioned.expect("versioned config");
        check!((m.store_name.as_deref(), vc.history_retention_ms) == (Some("vstore"), 600_000));
    }
}
