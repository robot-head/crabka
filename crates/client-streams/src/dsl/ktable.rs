//! `KTable<K,V>`: a materialized, changelog-backed table view. Produced by a
//! terminal aggregation (`count`/`reduce`/`aggregate`) or by
//! [`StreamsBuilder::table`], and convertible back to a `KStream` via
//! [`KTable::to_stream`].
//!
//! Each op records a logical node + a lowering thunk in the same style as
//! [`crate::dsl::kstream::KStream`]: reconstruct the parent handle from
//! `LowerState`, perform the typed Processor-API call, record the resulting node
//! name. Materialized ops (`map_values`/`filter`) also register a state store.
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kstream::KStream;
use crate::dsl::names;
use crate::dsl::processors::table::{
    KTableFilterProcessor, KTableMapValuesProcessor, KTableToStreamProcessor,
};
use crate::processor::serde::Serde;
use crate::topology::NodeHandle;

/// A changelog-backed table handle. `store_name` is the materialized store this
/// table reads/writes (used to derive changelog topics + reuse the store in
/// downstream materialized ops).
pub struct KTable<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    node: NodeId,
    #[allow(dead_code)]
    store_name: Option<String>,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> KTable<K, V> {
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        store_name: Option<String>,
    ) -> Self {
        Self {
            builder,
            node,
            store_name,
            _pd: PhantomData,
        }
    }
}

impl<K, V> KTable<K, V>
where
    K: Any + Send + Clone,
    V: Any + Send + Clone,
{
    /// `toStream`: view the table's change-stream as a `KStream`, forwarding
    /// every record unchanged. Not key-changing.
    #[must_use]
    pub fn to_stream(&self) -> KStream<K, V> {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_TOSTREAM);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                || KTableToStreamProcessor { _pd: PhantomData },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KStream::new(Rc::clone(&self.builder), id)
    }

    /// `mapValues`: transform each value, materializing the rewritten table into
    /// a new store. Key unchanged.
    pub fn map_values<V2, KS, VS, F>(
        &self,
        f: F,
        materialized: crate::dsl::config::Materialized<KS, VS>,
    ) -> KTable<K, V2>
    where
        V2: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V2> + Clone + 'static,
        F: Fn(&V) -> V2 + Clone + Send + Sync + 'static,
    {
        let store_name = mint_table_store(&self.builder, &materialized, names::TABLE_MAPVALUES);
        let crate::dsl::config::Materialized {
            key_serde,
            value_serde,
            ..
        } = materialized;
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_MAPVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor {
                store_name: Some(store_name.clone()),
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        let store_for_thunk = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state.topology.add_processor::<K, V, K, V2, _, _, _>(
                name.clone(),
                move || KTableMapValuesProcessor {
                    f: f2.clone(),
                    store_name: store_for_proc.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.topology.add_state_store::<K, V2, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                [h.name().to_string()],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, Some(store_name))
    }

    /// `filter`: keep rows matching `predicate`, materializing the filtered view.
    /// Non-matching rows are removed from the store and not forwarded (tombstone
    /// propagation via `Change<V>` is deferred — see the processor module doc).
    #[must_use]
    pub fn filter<KS, VS, P>(
        &self,
        predicate: P,
        materialized: crate::dsl::config::Materialized<KS, VS>,
    ) -> KTable<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        P: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        let store_name = mint_table_store(&self.builder, &materialized, names::TABLE_FILTER);
        let crate::dsl::config::Materialized {
            key_serde,
            value_serde,
            ..
        } = materialized;
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_FILTER);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor {
                store_name: Some(store_name.clone()),
            },
            vec![parent_id],
        );
        let p2 = predicate.clone();
        let store_for_thunk = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || KTableFilterProcessor {
                    predicate: p2.clone(),
                    store_name: store_for_proc.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.topology.add_state_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                [h.name().to_string()],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, Some(store_name))
    }
}

/// Mint a materialized table store name: the `Materialized` name when present,
/// else a fresh counter at the JVM position.
fn mint_table_store<KS, VS>(
    builder: &Rc<RefCell<InternalStreamsBuilder>>,
    materialized: &crate::dsl::config::Materialized<KS, VS>,
    prefix: &str,
) -> String {
    match &materialized.store_name {
        Some(name) => name.clone(),
        None => builder.borrow_mut().new_processor_name(prefix),
    }
}
