//! `SlidingWindowedCogroupedStream<K, VOut>`: sliding-windowed KIP-150 cogroup.
//!
//! `CogroupedKStream::windowed_by_sliding(SlidingWindows)` builds it. The
//! terminal `aggregate_explicit` produces a `KTable<Windowed<K>, VOut>` over a
//! shared window store. That store uses the KIP-450 `2 * timeDifferenceMs`
//! retention formula.

use std::{any::Any, cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

use crate::{
    dsl::{
        builder::InternalStreamsBuilder,
        cogrouped::{
            CogroupInput, CogroupKind, CogroupSpec, CogroupedKStream, StoreRegistrarFn,
            lower_cogroup,
        },
        config::Materialized,
        kgrouped::mint_store_name,
        ktable::KTable,
        names,
        sliding_windowed_kgrouped::sliding_suppress_factory,
        windows::{SlidingWindows, TimeWindowedSerde, Windowed},
    },
    processor::serde::Serde,
};

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// `windowedBy(SlidingWindows)`: switch to a sliding-windowed cogroup
    /// (KIP-450).
    #[must_use]
    pub fn windowed_by_sliding(
        self,
        windows: SlidingWindows,
    ) -> SlidingWindowedCogroupedStream<K, VOut> {
        SlidingWindowedCogroupedStream {
            builder: self.builder,
            inputs: self.inputs,
            windows,
            _pd: PhantomData,
        }
    }
}

/// Handle produced by [`CogroupedKStream::windowed_by_sliding`]. The terminal
/// sliding-windowed aggregation consumes it.
pub struct SlidingWindowedCogroupedStream<K, VOut> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    inputs: Vec<CogroupInput<K, VOut>>,
    windows: SlidingWindows,
    _pd: PhantomData<fn() -> (K, VOut)>,
}

impl<K, VOut> SlidingWindowedCogroupedStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// Sliding-windowed terminal aggregation. It produces a
    /// `KTable<Windowed<K>, VOut>`.
    ///
    /// Store retention uses the KIP-450 formula: `size = 2 * timeDifferenceMs`
    /// (matching `sliding_windowed_kgrouped.rs::lower_aggregate`).
    pub fn aggregate_explicit<KS, VS, I>(
        self,
        init: I,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, VOut, TimeWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VOut> + Clone + 'static,
        I: Fn() -> VOut + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        let suppress_factory = sliding_suppress_factory::<K, VOut, KS, VS>(
            materialized.key_serde.clone(),
            materialized.value_serde.clone(),
            self.windows,
        );
        let Materialized {
            key_serde,
            value_serde,
            logging,
            caching,
            ..
        } = materialized;
        let spec = CogroupSpec::<K, VOut> {
            kind: CogroupKind::Sliding(self.windows),
            init: Arc::new(init),
            merger: None,
        };
        let ks = key_serde.clone();
        let vs = value_serde.clone();
        let store_for_reg = store_name.clone();
        // Sliding window retention formula (JVM-exact):
        // timeDifferenceMs + timeDifferenceMs + gracePeriodMs + 86_400_000
        // (a sliding window spans [t - timeDiff, t + timeDiff] so the effective
        // window size for changelog retention is 2 * timeDifferenceMs).
        let size = self.windows.time_difference * 2.0;
        // The true sliding window size for the key end is 1 * timeDiff (the
        // retention basis above is the 2× span).
        let window_size = self.windows.time_difference;
        let grace = self.windows.grace;
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            state.topology.add_window_store::<K, VOut, KS, VS>(
                store_for_reg.clone(),
                ks.clone(),
                vs.clone(),
                (size, window_size, grace),
                procs,
            );
            state.topology.mark_store_caching(&store_for_reg, caching);
        });
        let merge_id = lower_cogroup::<K, VOut, Windowed<K>>(
            &self.builder,
            self.inputs,
            &store_name,
            &spec,
            logging,
            registrar,
        );
        KTable::new(
            Rc::clone(&self.builder),
            merge_id,
            Some(store_name),
            None,
            TimeWindowedSerde::new(key_serde, self.windows.time_difference),
            value_serde,
        )
        .with_window_grace(Some(self.windows.grace))
        .with_suppress_factory(Some(suppress_factory))
    }
}

#[cfg(test)]
mod caching_tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use crate::{
        BufferConfig, I64Serde, Materialized, Produced, SlidingWindows, StringSerde, Suppressed,
        dsl::{StreamsBuilder, windows::TimeWindowedSerde},
        store::backend::StoreBackend,
    };

    #[test]
    fn sliding_windowed_cogroup_marks_store_cached() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(
            100,
        )))
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg"),
        )
        .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(100)), I64Serde),
        );
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
            .unwrap();
        check!(g.cache_owner.contains_key("cg"));
    }

    #[test]
    fn sliding_windowed_cogroup_uncached_when_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(
            100,
        )))
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde)
                .as_store("cg")
                .with_caching(false),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(100)), I64Serde),
        );
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
            .unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
