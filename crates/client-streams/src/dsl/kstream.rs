//! `KStream<K,V>` handle + its stateless DSL ops.
//!
//! Each op (1) mints a JVM-matching node name, (2) adds a type-erased
//! `StatelessProcessor` node to the logical graph with the right
//! `key_changing_operation` flag, and (3) attaches a **lowering thunk**
//! ([`LowerFn`]) that — when the lowering driver (Task 5) runs it — performs the
//! typed [`Topology::add_processor`] call and records the resulting node name.
//! The thunk captures the op's concrete K/V types and the user closure, so types
//! are statically known *inside* the thunk even though the graph is type-erased.
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::{Grouped, Materialized};
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kgrouped::KGroupedStream;
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::processors::change::Change;
use crate::dsl::processors::stateless;
use crate::dsl::processors::table::KStreamToTableProcessor;
use crate::processor::serde::{Produced, Serde};
use crate::topology::NodeHandle;

pub struct KStream<K, V> {
    #[allow(dead_code)]
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    #[allow(dead_code)]
    pub(crate) node: NodeId,
    /// True when the current key was produced by a key-changing op upstream
    /// (`select_key`/`map`/`flat_map`/`group_by`) that has *not* since been
    /// re-grouped through a repartition. A downstream aggregation reads this to
    /// decide whether it must insert a repartition before the aggregate node.
    /// A source stream starts `false`; value-only ops propagate the parent bit.
    pub(crate) key_changing: bool,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> KStream<K, V> {
    pub(crate) fn new(builder: Rc<RefCell<InternalStreamsBuilder>>, node: NodeId) -> Self {
        Self::new_with_key_changing(builder, node, false)
    }

    pub(crate) fn new_with_key_changing(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        key_changing: bool,
    ) -> Self {
        Self {
            builder,
            node,
            key_changing,
            _pd: std::marker::PhantomData,
        }
    }
}

impl<K, V> KStream<K, V>
where
    K: Any + Send + Clone,
    V: Any + Send + Clone,
{
    /// `mapValues`: transform each value, key unchanged. Not key-changing.
    pub fn map_values<V2, F>(&self, f: F) -> KStream<K, V2>
    where
        V2: Any + Send + Clone,
        F: Fn(&V) -> V2 + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::MAPVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V2, _, _, _>(
                name.clone(),
                move || stateless::MapValuesProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // map_values is value-only → key lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
    }

    /// `filter`: keep records where `predicate(key, value)` is true.
    #[must_use]
    pub fn filter<F>(&self, predicate: F) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        self.filter_inner(predicate, false)
    }

    /// `filterNot`: keep records where `predicate(key, value)` is false.
    #[must_use]
    pub fn filter_not<F>(&self, predicate: F) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        self.filter_inner(predicate, true)
    }

    fn filter_inner<F>(&self, predicate: F, negate: bool) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FILTER);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let p2 = predicate.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::FilterProcessor {
                    predicate: p2.clone(),
                    negate,
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // filter is value-only → key lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
    }

    /// `map`: transform key and value. Key-changing.
    pub fn map<K2, V2, F>(&self, f: F) -> KStream<K2, V2>
    where
        K: Default,
        K2: Any + Send + Clone,
        V2: Any + Send + Clone,
        F: Fn(&K, &V) -> (K2, V2) + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::MAP);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].key_changing_operation = true;
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K2, V2, _, _, _>(
                name.clone(),
                move || stateless::MapProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // map rewrites the key → key-changing lineage.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, true)
    }

    /// `selectKey`: rewrite the key, value unchanged. Key-changing.
    pub fn select_key<K2, F>(&self, f: F) -> KStream<K2, V>
    where
        K: Default,
        K2: Any + Send + Clone,
        F: Fn(&K, &V) -> K2 + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::KEY_SELECT);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].key_changing_operation = true;
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K2, V, _, _, _>(
                name.clone(),
                move || stateless::SelectKeyProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // select_key rewrites the key → key-changing lineage.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, true)
    }

    /// `flatMap`: one record → zero or more `(K2, V2)`. Key-changing.
    pub fn flat_map<K2, V2, IT, F>(&self, f: F) -> KStream<K2, V2>
    where
        K: Default,
        K2: Any + Send + Clone,
        V2: Any + Send + Clone,
        IT: IntoIterator<Item = (K2, V2)> + 'static,
        F: Fn(&K, &V) -> IT + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FLATMAP);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].key_changing_operation = true;
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K2, V2, _, _, _>(
                name.clone(),
                move || stateless::FlatMapProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // flat_map can rewrite the key → key-changing lineage.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, true)
    }

    /// `flatMapValues`: one record → zero or more `V2`, key unchanged.
    pub fn flat_map_values<V2, IT, F>(&self, f: F) -> KStream<K, V2>
    where
        V2: Any + Send + Clone,
        IT: IntoIterator<Item = V2> + 'static,
        F: Fn(&V) -> IT + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FLATMAPVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V2, _, _, _>(
                name.clone(),
                move || stateless::FlatMapValuesProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // flat_map_values is value-only → key lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
    }

    /// `peek`: observe each record, then forward it unchanged.
    #[must_use]
    pub fn peek<F>(&self, f: F) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::PEEK);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::PeekProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // peek is observe-only → key lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
    }

    /// `foreach`: terminal side-effect on each record (consumes the stream).
    pub fn foreach<F>(self, f: F)
    where
        K: Default,
        F: Fn(&K, &V) + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FOREACH);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::ForeachProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
    }

    /// `to`: write the stream to a topic via a sink (consumes the stream).
    pub fn to<KS, VS>(self, topic: impl Into<String>, produced: Produced<KS, VS>)
    where
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let topic: String = topic.into();
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::SINK);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StreamSink {
                topic: topic.clone(),
            },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            state
                .topology
                .add_sink::<K, V, KS, VS, _, _>(name.clone(), topic, [parent], produced);
            // A sink is terminal; record its name so children (none) could find it.
            state.handle_name.insert(id, name.clone());
        }));
    }

    /// `merge`: union this stream with `other` (same K/V). Both feed one node.
    #[must_use]
    pub fn merge(&self, other: &KStream<K, V>) -> KStream<K, V> {
        let left_id = self.node;
        let right_id = other.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::MERGE);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![left_id, right_id],
        );
        g.graph.nodes[id].merge_node = true;
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let left = NodeHandle::<K, V>::from_name(state.handle_name[&left_id].clone());
            let right = NodeHandle::<K, V>::from_name(state.handle_name[&right_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                || stateless::MergeProcessor { _pd: PhantomData },
                [left, right],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // merge keeps keys; conservatively key-changing if either side is.
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing || other.key_changing,
        )
    }

    /// `repartition`: force a repartition through an internal topic.
    ///
    /// Lowers as `sink → add_repartition_topic → source`, the same pattern used
    /// by the implicit repartition inserted before a stateful aggregation. The
    /// repartition topic name is `<app_id>-<name>-repartition`, where `<name>` is
    /// the explicit [`Repartitioned`](crate::dsl::config::Repartitioned) name when
    /// set, otherwise an auto-name minted from the counter.
    ///
    /// **Byte-exactness vs JVM:** the JVM assigns a distinct `KSTREAM-REPARTITION-`
    /// counter for standalone `repartition()` calls. That counter is NOT validated
    /// against a golden fixture in this slice — functional correctness (no panic,
    /// records flow through) is the bar here.
    #[must_use]
    pub fn repartition<KS, VS>(
        &self,
        repartitioned: crate::dsl::config::Repartitioned<KS, VS>,
    ) -> KStream<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        let crate::dsl::config::Repartitioned {
            name: explicit_name,
            partitions,
            key_serde,
            value_serde,
        } = repartitioned;
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        // Mint the base name: explicit name or a fresh counter.
        let base = explicit_name.unwrap_or_else(|| g.new_processor_name(names::SOURCE));
        // Sink + source names used inside the thunk (minted now to advance
        // the counter at the right position relative to later ops).
        let sink_name = g.new_processor_name(names::SINK);
        let source_name = g.new_processor_name(names::SOURCE);
        let topic_base = base.clone();
        let id = g.graph.add(
            source_name.clone(),
            GraphNodeKind::Repartition {
                topic: format!("{topic_base}{}", names::REPARTITION_SUFFIX),
                partitions,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&parent_id].clone();
            let parent = NodeHandle::<K, V>::from_name(parent_name);
            let topic = format!("{}-{topic_base}{}", state.app_id, names::REPARTITION_SUFFIX);
            // sink: write to repartition topic
            state.topology.add_sink::<K, V, KS, VS, _, _>(
                sink_name.clone(),
                topic.clone(),
                [parent],
                crate::processor::serde::Produced::with(key_serde.clone(), value_serde.clone()),
            );
            // mark the topic as internal repartition (loop-back)
            state.topology.add_repartition_topic(topic.clone());
            // source: read from repartition topic
            state.topology.add_source::<K, V, KS, VS>(
                source_name.clone(),
                [topic],
                crate::processor::serde::Consumed::with(key_serde, value_serde),
            );
            state.handle_name.insert(id, source_name.clone());
        }));
        drop(g);
        // An explicit repartition re-groups by key → downstream is no longer
        // key-changing relative to its (now repartitioned) partitioning.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, false)
    }

    /// `split`: begin a branching fan-out. Returns a [`BranchedStream`] builder
    /// from which individual [`branch`](BranchedStream::branch) calls create
    /// filtered child streams. The split itself adds no node to the topology —
    /// each `branch` call creates a [`FilterProcessor`]-backed child wired
    /// directly to this stream's node.
    ///
    /// **Simplification vs JVM:** each branch receives a record when its predicate
    /// matches, not just the first matching branch. For mutually-exclusive
    /// predicates the behaviour is identical to the JVM first-match-wins semantics.
    ///
    /// [`branch`]: BranchedStream::branch
    /// [`FilterProcessor`]: crate::dsl::processors::stateless::FilterProcessor
    #[must_use]
    pub fn split(&self) -> BranchedStream<K, V> {
        // The JVM mints a `KSTREAM-BRANCH-` node at split() time (counter-only:
        // the branch node itself is not wire-visible). Advance the counter here so
        // that downstream auto-names (e.g. from a subsequent aggregation) land at
        // the same indices as the JVM byte-for-byte. Mirrors the FILTER counter
        // mint in `KGroupedStream::record_repartition`.
        let _branch_name = self.builder.borrow_mut().new_processor_name(names::BRANCH);
        BranchedStream {
            builder: Rc::clone(&self.builder),
            parent: self.node,
            key_changing: self.key_changing,
            _pd: std::marker::PhantomData,
        }
    }

    /// `groupByKey`: group by the existing key, preparing for an aggregation.
    ///
    /// Records no graph node — the (optional) repartition + aggregate node are
    /// recorded when a terminal `count`/`reduce`/`aggregate` is called. The
    /// returned [`KGroupedStream`] carries whether the upstream key lineage is
    /// key-changing (→ the aggregation must insert a repartition) and a typed
    /// repartition-lowering thunk built from the `Grouped` serdes.
    pub fn group_by_key<KS, VS>(&self, grouped: Grouped<KS, VS>) -> KGroupedStream<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        KGroupedStream::new(
            Rc::clone(&self.builder),
            self.node,
            self.key_changing,
            grouped.name,
            crate::dsl::kgrouped::repartition_lower::<K, V, KS, VS>(
                grouped.key_serde,
                grouped.value_serde,
            ),
        )
    }

    /// `groupBy`: re-key via `f`, then group by the new key.
    ///
    /// Equivalent to `select_key(f).group_by_key(grouped)`; the key change forces
    /// a repartition before any subsequent aggregation.
    pub fn group_by<K2, KS, VS, F>(&self, f: F, grouped: Grouped<KS, VS>) -> KGroupedStream<K2, V>
    where
        K: Default,
        K2: Any + Send + Clone,
        KS: Serde<K2> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        F: Fn(&K, &V) -> K2 + Clone + Send + Sync + 'static,
    {
        self.select_key(f).group_by_key(grouped)
    }

    /// `toTable`: materialize this stream into a [`KTable`] by writing each record
    /// into a state store and forwarding a `Change<V>` change-stream (prior store
    /// value as `old`). Backed by [`KStreamToTableProcessor`].
    ///
    /// The key is carried through unchanged, so `to_table` never inserts a
    /// repartition (the JVM only repartitions when the upstream key is rewritten
    /// without a re-group). The store name is `Materialized`'s explicit name when
    /// set, else a fresh `KSTREAM-TOTABLE-STATE-STORE-` counter; the store gets the
    /// standard `<app>-<store>-changelog` changelog (or none when
    /// [`Materialized::with_logging(false)`]).
    ///
    /// [`KStreamToTableProcessor`]: crate::dsl::processors::table::KStreamToTableProcessor
    /// [`Materialized::with_logging(false)`]: crate::dsl::config::Materialized::with_logging
    pub fn to_table<KS, VS>(&self, materialized: Materialized<KS, VS>) -> KTable<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        let parent_id = self.node;
        // Mint the store name at the JVM counter position (before the processor
        // name), matching the aggregate store-naming convention.
        let store_name = match &materialized.store_name {
            Some(name) => name.clone(),
            None => self
                .builder
                .borrow_mut()
                .new_processor_name(names::TOTABLE_STORE),
        };
        let Materialized {
            key_serde,
            value_serde,
            logging,
            ..
        } = materialized;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TOTABLE);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.clone(),
                changelog: logging,
            },
            vec![parent_id],
        );
        let store_for_thunk = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            // The stream → table boundary forwards Change<V> (prior store value as old).
            let h = state.topology.add_processor::<K, V, K, Change<V>, _, _, _>(
                name.clone(),
                move || KStreamToTableProcessor {
                    store_name: store_for_proc.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            // Honor `Materialized::with_logging(bool)`, mirroring the aggregate ops:
            // logging=true → changelog topic emitted; logging=false → store usable
            // at runtime but no state_changelog_topics entry in the wire topology.
            if logging {
                state.topology.add_state_store::<K, V, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde.clone(),
                    value_serde.clone(),
                    [h.name().to_string()],
                );
            } else {
                state.topology.add_state_store_no_changelog::<K, V, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde.clone(),
                    value_serde.clone(),
                );
            }
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, Some(store_name), None)
    }
}

// ---------------------------------------------------------------------------
// BranchedStream
// ---------------------------------------------------------------------------

/// Builder returned by [`KStream::split`]. Each [`branch`](Self::branch) call
/// adds a [`FilterProcessor`]-backed child node wired to the parent node and
/// returns a new [`KStream`] carrying only the records for which the predicate
/// returns `true`.
///
/// **Simplification vs JVM first-match-wins:** records are forwarded to ALL
/// branches whose predicate matches. For mutually-exclusive predicates the
/// behaviour is identical.
///
/// Drop `BranchedStream` before calling [`StreamsBuilder::build`] — it holds an
/// `Rc` clone of the shared internal builder and will otherwise cause the
/// `Rc::try_unwrap` inside `build` to fail.
///
/// [`StreamsBuilder::build`]: crate::dsl::builder::StreamsBuilder::build
/// [`FilterProcessor`]: crate::dsl::processors::stateless::FilterProcessor
pub struct BranchedStream<K, V> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) parent: NodeId,
    pub(crate) key_changing: bool,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> BranchedStream<K, V>
where
    K: Any + Send + Clone + Default,
    V: Any + Send + Clone,
{
    /// Add a branch: records for which `predicate(key, value)` returns `true`
    /// are forwarded to the returned [`KStream`]. Uses a `KSTREAM-BRANCHCHILD-`
    /// node backed by a [`FilterProcessor`] (negate = false).
    ///
    /// [`FilterProcessor`]: crate::dsl::processors::stateless::FilterProcessor
    pub fn branch<P>(&self, predicate: P) -> KStream<K, V>
    where
        P: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        let parent_id = self.parent;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::BRANCHCHILD);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let p2 = predicate.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::FilterProcessor {
                    predicate: p2.clone(),
                    negate: false,
                    _pd: std::marker::PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // branch is filter-only → key lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
    }
}

#[cfg(test)]
mod tests {
    use crate::dsl::builder::StreamsBuilder;
    use crate::processor::serde::{Consumed, Produced, StringSerde};
    use assert2::check;

    #[test]
    fn stateless_chain_records_named_nodes() {
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .map_values(|v: &String| v.to_uppercase())
            .filter(|_k: &String, _v: &String| true)
            .to("out", Produced::with(StringSerde, StringSerde));
        let g = b.internal.borrow();
        let names: Vec<&str> = g.graph.nodes.iter().map(|n| n.name.as_str()).collect();
        check!(
            names
                == vec![
                    "KSTREAM-SOURCE-0000000000",
                    "KSTREAM-MAPVALUES-0000000001",
                    "KSTREAM-FILTER-0000000002",
                    "KSTREAM-SINK-0000000003",
                ]
        );
    }

    #[test]
    fn select_key_marks_key_changing() {
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .select_key(|_k: &String, v: &String| v.clone());
        let g = b.internal.borrow();
        check!(g.graph.nodes[1].key_changing_operation);
    }
}
