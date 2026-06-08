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

/// Store name + serdes + logging flag for a materialized `KTable`.
#[derive(Debug, Clone)]
pub struct Materialized<KS, VS> {
    #[allow(dead_code)]
    pub(crate) key_serde: KS,
    #[allow(dead_code)]
    pub(crate) value_serde: VS,
    pub(crate) store_name: Option<String>,
    pub(crate) logging: bool,
}
impl<KS, VS> Materialized<KS, VS> {
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self {
            key_serde,
            value_serde,
            store_name: None,
            logging: true,
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
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};
    use assert2::check;

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
}
