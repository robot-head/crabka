//! `SlidingWindowedCogroupedStream<K, VOut>`: sliding-windowed KIP-150 cogroup.
//! Built by `CogroupedKStream::windowed_by_sliding(SlidingWindows)`. Terminal
//! `aggregate_explicit` produces `KTable<Windowed<K>, VOut>` over a shared window
//! store with the KIP-450 `2 * timeDifferenceMs` retention formula.

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
use crate::dsl::windows::{SlidingWindows, TimeWindowedSerde, Windowed};
use crate::processor::serde::Serde;

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// `windowedBy(SlidingWindows)` → sliding-windowed cogroup (KIP-450).
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

/// Handle produced by [`CogroupedKStream::windowed_by_sliding`]; terminal
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
    /// Sliding-windowed terminal aggregation → `KTable<Windowed<K>, VOut>`.
    ///
    /// Store retention uses the KIP-450 formula: `size = 2 * timeDifferenceMs`
    /// (matching `sliding_windowed_kgrouped.rs::lower_aggregate`).
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
        let size = self.windows.time_difference_ms * 2;
        let grace = self.windows.grace_ms;
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            state.topology.add_window_store::<K, VOut, KS, VS>(
                store_for_reg.clone(),
                ks.clone(),
                vs.clone(),
                size,
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
            TimeWindowedSerde::new(key_serde, self.windows.time_difference_ms),
            value_serde,
        )
        .with_window_grace(Some(self.windows.grace_ms))
    }
}
