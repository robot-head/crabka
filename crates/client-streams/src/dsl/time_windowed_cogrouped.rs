//! `TimeWindowedCogroupedStream<K, VOut>`: time-windowed KIP-150 cogroup. Built
//! by `CogroupedKStream::windowed_by(TimeWindows)`. Terminal `aggregate_explicit`
//! produces `KTable<Windowed<K>, VOut>` over a shared window store.

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
        windows::{TimeWindowedSerde, TimeWindows, Windowed},
    },
    processor::serde::Serde,
};

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// `windowedBy(TimeWindows)` → time-windowed cogroup.
    #[must_use]
    pub fn windowed_by(self, windows: TimeWindows) -> TimeWindowedCogroupedStream<K, VOut> {
        TimeWindowedCogroupedStream {
            builder: self.builder,
            inputs: self.inputs,
            windows,
            _pd: PhantomData,
        }
    }
}

/// Handle produced by [`CogroupedKStream::windowed_by`]; terminal
/// time-windowed aggregation consumes it.
pub struct TimeWindowedCogroupedStream<K, VOut> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    inputs: Vec<CogroupInput<K, VOut>>,
    windows: TimeWindows,
    _pd: PhantomData<fn() -> (K, VOut)>,
}

impl<K, VOut> TimeWindowedCogroupedStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// Time-windowed terminal aggregation → `KTable<Windowed<K>, VOut>`.
    ///
    /// Note: the returned table carries no suppress factory, so calling
    /// `.suppress(...)` on it fails at topology-build time. Suppress on windowed
    /// cogroup outputs is a deferred follow-up (emit semantics are emit-on-update
    /// across the cogroup surface, per the KIP-150 slice scope).
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
        let Materialized {
            key_serde,
            value_serde,
            logging,
            caching,
            ..
        } = materialized;
        let spec = CogroupSpec::<K, VOut> {
            kind: CogroupKind::Time(self.windows),
            init: Arc::new(init),
            merger: None,
        };
        let ks = key_serde.clone();
        let vs = value_serde.clone();
        let store_for_reg = store_name.clone();
        let size = self.windows.size_ms;
        let grace = self.windows.grace_ms;
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            state.topology.add_window_store::<K, VOut, KS, VS>(
                store_for_reg.clone(),
                ks.clone(),
                vs.clone(),
                // Tumbling/hopping: retention basis == window size.
                (size, size, grace),
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
            TimeWindowedSerde::new(key_serde, self.windows.size_ms),
            value_serde,
        )
        .with_window_grace(Some(self.windows.grace_ms))
    }
}

#[cfg(test)]
mod caching_tests {
    use assert2::check;

    use crate::{
        I64Serde, Materialized, Produced, StringSerde, TimeWindows,
        dsl::{StreamsBuilder, windows::TimeWindowedSerde},
        store::backend::StoreBackend,
    };

    #[test]
    fn time_windowed_cogroup_marks_store_cached() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by(TimeWindows::of_size(100))
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg"),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 100), I64Serde),
        );
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(g.cache_owner.contains_key("cg"));
    }

    #[test]
    fn time_windowed_cogroup_uncached_when_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by(TimeWindows::of_size(100))
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde)
                .as_store("cg")
                .with_caching(false),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 100), I64Serde),
        );
        let built = b.build("app").unwrap();
        let g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", 1024)).unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
