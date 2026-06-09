//! `SessionWindowedCogroupedStream<K, VOut>`: session-windowed KIP-150 cogroup.
//! Built by `CogroupedKStream::windowed_by_session(SessionWindows)`. Terminal
//! `aggregate_explicit` produces `KTable<Windowed<K>, VOut>` over a shared session
//! store. Unlike the time- and sliding-windowed variants, the session aggregate
//! requires a **merger** that combines two sessions when they are merged together.

use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::cogrouped::{
    CogroupInput, CogroupKind, CogroupSpec, CogroupedKStream, StoreRegistrarFn, lower_cogroup,
};
use crate::dsl::config::Materialized;
use crate::dsl::kgrouped::mint_store_name;
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::windows::{SessionWindowedSerde, SessionWindows, Windowed};
use crate::processor::serde::Serde;

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// `windowedBy(SessionWindows)` → session-windowed cogroup (KIP-150).
    ///
    /// Unlike time- and sliding-windowed cogroup, the terminal `aggregate_explicit`
    /// requires a `merger` that combines two session aggregates when sessions are
    /// merged due to inactivity-gap expiry.
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

/// Handle produced by [`CogroupedKStream::windowed_by_session`]; terminal
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
    /// Session-windowed terminal aggregation → `KTable<Windowed<K>, VOut>`.
    ///
    /// The `merger` combines two session aggregates when sessions are merged
    /// (required for session windows — no default merger exists).
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
        let gap = self.windows.gap_ms;
        let grace = self.windows.grace_ms;
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            state.topology.add_session_store::<K, VOut, KS, VS>(
                store_for_reg.clone(),
                ks.clone(),
                vs.clone(),
                gap,
                grace,
                procs,
            );
        });
        let merge_id = lower_cogroup::<K, VOut, Windowed<K>>(
            &self.builder,
            self.inputs,
            store_name.clone(),
            spec,
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
        .with_window_grace(Some(self.windows.grace_ms))
    }
}
