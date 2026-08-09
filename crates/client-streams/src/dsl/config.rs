//! DSL config objects that mirror the JVM `Grouped`, `Materialized`, and
//! `Repartitioned`.
//!
//! `Consumed` and `Produced` come from `crate::processor::serde`.

use crabka_units::prelude::*;

/// The serdes, and an optional repartition name, for `groupBy` and
/// `groupByKey`.
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

/// Versioned-store settings for a `Materialized` (KIP-889).
///
/// `segment_interval` affects only the JVM eviction granularity, which is not
/// observable here. This type accepts it for API parity. A `None` segment
/// interval uses the JVM default segment heuristic.
#[derive(Debug, Clone, Copy)]
pub struct VersionedConfig {
    pub history_retention: Time,
    pub segment_interval: Option<Time>,
}

/// The store name, the serdes, and the logging flag for a materialized
/// `KTable`.
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
    /// Turn the record cache for this store on or off. This is the JVM
    /// `Materialized.withCachingEnabled` and
    /// `Materialized.withCachingDisabled`. Default: on.
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
    /// `name`, retaining `history_retention` of version history.
    #[must_use]
    pub fn as_versioned(mut self, name: impl Into<String>, history_retention: Time) -> Self {
        self.store_name = Some(name.into());
        self.versioned = Some(VersionedConfig {
            history_retention,
            segment_interval: None,
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

/// The serdes for a windowed `KStream`-`KStream` join, the `StreamJoined`.
///
/// It holds the key serde and the per-side value serdes: `value1` for this side,
/// the left side, and `value2` for the other side, the right side. The two
/// join-window stores are registered with `key_serde` and the matching value
/// serde, which mirrors the JVM `StreamJoined.with`.
#[derive(Debug, Clone, Copy)]
pub struct StreamJoined<KS, V1S, V2S> {
    pub(crate) key: KS,
    pub(crate) left_value: V1S,
    pub(crate) right_value: V2S,
}
impl<KS, V1S, V2S> StreamJoined<KS, V1S, V2S> {
    pub fn with(key_serde: KS, value1_serde: V1S, value2_serde: V2S) -> Self {
        Self {
            key: key_serde,
            left_value: value1_serde,
            right_value: value2_serde,
        }
    }
}

/// Stream-table join config (KIP-923).
///
/// It carries an optional grace period that buffers the stream records, so an
/// out-of-order record still joins the table value as of its own timestamp.
/// `grace` needs the joined table to be versioned, and it must be
/// `< history_retention`. The join DSL asserts that at build time. `name`
/// optionally names the grace buffer store.
#[derive(Debug, Clone, Default)]
pub struct Joined {
    pub(crate) grace: Option<Time>,
    pub(crate) name: Option<String>,
}
impl Joined {
    #[must_use]
    pub fn with_grace_period(grace: Time) -> Self {
        Self {
            grace: Some(grace),
            name: None,
        }
    }
    #[must_use]
    pub fn as_named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

/// The serdes, and an optional name and partition count, for an explicit
/// `repartition()`.
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
        check!(g.name.as_deref() == Some("g1"));
        let m = Materialized::with(StringSerde, I64Serde).as_store("counts");
        check!(m.store_name.as_deref() == Some("counts"));
        check!(m.logging);
        let r = Repartitioned::with(StringSerde, I64Serde)
            .with_name("rp")
            .num_partitions(4);
        check!(r.name.as_deref() == Some("rp"));
        check!(r.partitions == Some(4));
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
        let j = Joined::with_grace_period(secs(5)).as_named("jb");
        check!(j.grace == Some(secs(5)));
        check!(j.name.as_deref() == Some("jb"));
        check!(Joined::default().grace == None);
    }

    #[test]
    fn materialized_as_versioned_sets_config() {
        let m = Materialized::with(StringSerde, I64Serde).as_versioned("vstore", minutes(10));
        check!(m.store_name.as_deref() == Some("vstore"));
        let vc = m.versioned.expect("versioned config");
        check!(vc.history_retention == minutes(10));
    }
}
