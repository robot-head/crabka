//! `SessionWindowedCogroupedStream<K, VOut>`: session-windowed KIP-150 cogroup.
//!
//! `CogroupedKStream::windowed_by_session(SessionWindows)` builds it. The
//! terminal `aggregate_explicit` produces a `KTable<Windowed<K>, VOut>` over a
//! shared session store. Unlike the time-windowed and sliding-windowed variants,
//! the session aggregate needs a **merger** that combines two sessions when the
//! runtime merges them.

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
        windows::{SessionWindowedSerde, SessionWindows, Windowed},
    },
    processor::serde::Serde,
};

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// `windowedBy(SessionWindows)`: switch to a session-windowed cogroup
    /// (KIP-150).
    ///
    /// Unlike the time-windowed and sliding-windowed cogroup, the terminal
    /// `aggregate_explicit` needs a `merger`. The merger combines two session
    /// aggregates when the runtime merges sessions after an inactivity gap
    /// expires.
    #[must_use]
    pub fn windowed_by_session(
        self,
        windows: SessionWindows,
    ) -> SessionWindowedCogroupedStream<K, VOut> {
        SessionWindowedCogroupedStream {
            builder: self.builder,
            inputs: self.inputs,
            windows,
            _pd: PhantomData,
        }
    }
}

/// Handle produced by [`CogroupedKStream::windowed_by_session`]. The terminal
/// session-windowed aggregation consumes it.
pub struct SessionWindowedCogroupedStream<K, VOut> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    inputs: Vec<CogroupInput<K, VOut>>,
    windows: SessionWindows,
    _pd: PhantomData<fn() -> (K, VOut)>,
}

impl<K, VOut> SessionWindowedCogroupedStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// Session-windowed terminal aggregation. It produces a
    /// `KTable<Windowed<K>, VOut>`.
    ///
    /// The `merger` combines two session aggregates when the runtime merges
    /// sessions. Session windows need it, because no default merger exists.
    ///
    /// Note: the returned table carries no suppress factory, so a
    /// `.suppress(...)` call on it fails at topology-build time. Suppress on
    /// windowed cogroup outputs is a deferred follow-up. The emit semantics are
    /// emit-on-update across the cogroup surface, which is the KIP-150 slice
    /// scope.
    pub fn aggregate_explicit<KS, VS, I, M>(
        self,
        init: I,
        merger: M,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, VOut, SessionWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VOut> + Clone + 'static,
        I: Fn() -> VOut + Send + Sync + 'static,
        M: Fn(&K, VOut, VOut) -> VOut + Send + Sync + 'static,
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
            kind: CogroupKind::Session(self.windows),
            init: Arc::new(init),
            merger: Some(Arc::new(merger)),
        };
        let ks = key_serde.clone();
        let vs = value_serde.clone();
        let store_for_reg = store_name.clone();
        let gap = self.windows.gap;
        let grace = self.windows.grace;
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            state.topology.add_session_store::<K, VOut, KS, VS>(
                store_for_reg.clone(),
                ks.clone(),
                vs.clone(),
                gap,
                grace,
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
            SessionWindowedSerde::new(key_serde),
            value_serde,
        )
        .with_window_grace(Some(self.windows.grace))
    }
}

#[cfg(test)]
mod caching_tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use crate::{
        I64Serde, Materialized, Produced, SessionWindows, StringSerde,
        dsl::{StreamsBuilder, windows::SessionWindowedSerde},
        store::backend::StoreBackend,
    };

    #[test]
    fn session_windowed_cogroup_marks_store_cached() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(100)))
        .aggregate_explicit(
            || 0i64,
            |_k: &String, a: i64, b: i64| a + b,
            Materialized::with(StringSerde, I64Serde).as_store("cg"),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
        );
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
            .unwrap();
        check!(g.cache_owner.contains_key("cg"));
    }

    #[test]
    fn session_windowed_cogroup_uncached_when_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(100)))
        .aggregate_explicit(
            || 0i64,
            |_k: &String, a: i64, b: i64| a + b,
            Materialized::with(StringSerde, I64Serde)
                .as_store("cg")
                .with_caching(false),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
        );
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
            .unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
