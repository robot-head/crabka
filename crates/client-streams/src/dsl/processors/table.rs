//! `KTable` `Processor` impls over a #3 `KeyValueStore`.
//!
//! ## Change<old,new> propagation
//!
//! Every `KTable` node forwards a `Record<K, Change<V>>` change-stream, so
//! downstream nodes can detect a tombstone, where `Change::new == None`. The
//! materialized **state stores still hold `V`**, the current value. Only the
//! value forwarded between nodes is wrapped in `Change<V>`, and that keeps the
//! changelog and the wire topology byte-unchanged.
//!
//! - [`KTableSourceProcessor`]: `V` in → `Change<V>` out. It reads the prior
//!   store value as `old`.
//! - [`KTableToStreamProcessor`]: `Change<V>` in → `V` out. It extracts `new`
//!   and drops tombstones, because `toStream` produces a plain `KStream`.
//! - [`KTableMapValuesProcessor`] and [`KTableMapValuesViewProcessor`]:
//!   `Change<V>` in → `Change<V2>` out. They `map` both sides.
//! - [`KTableFilterProcessor`]: `Change<V>` in → `Change<V>` out. It re-applies
//!   the predicate to both sides and **emits tombstones** for rows that stop
//!   matching.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{
    dsl::processors::{change::Change, tuple_forwarder::TupleForwarder},
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
    },
};

/// Variance-neutral marker reused from `stateless.rs`.
type Marker<T> = PhantomData<fn() -> T>;

// ── KTableSourceProcessor ────────────────────────────────────────────────────

/// Materializes incoming records into a `KeyValueStore`, then forwards a
/// `Change<V>`.
///
/// The change carries the prior store value as `old` and the incoming value as
/// `new`. This processor backs a `KTable` created from a source topic.
#[allow(dead_code)]
pub(crate) struct KTableSourceProcessor<K, V> {
    pub store_name: String,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, Change<V>> for KTableSourceProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("KTable source requires a non-null key");
        let rc = ctx.record_context().clone();
        let old = {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("KTable source store not found");
            store.set_record_context(rc);
            let old = store.get(&key).await;
            store.put(key.clone(), r.value.clone()).await;
            old
        };
        self.forwarder
            .maybe_forward(ctx, key, old, r.value, r.timestamp);
    }
}

// ── KStreamToTableProcessor ──────────────────────────────────────────────────

/// Materializes a `KStream` into a `KTable`, backing `KStream::to_table`.
///
/// This processor writes each incoming `V` into the store and forwards a
/// `Change<V>`. The change carries the prior store value as `old` and the
/// incoming value as `new`. It works like [`KTableSourceProcessor`], but its
/// input is a plain `KStream` value and not a source-topic record. It is
/// therefore the boundary where a stream becomes a changelog-backed table.
#[allow(dead_code)]
pub(crate) struct KStreamToTableProcessor<K, V> {
    pub store_name: String,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, Change<V>> for KStreamToTableProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("to_table requires a non-null key");
        // Stash the source record context BEFORE the store borrow so a cached
        // store attaches it to the deduped change it forwards on flush.
        let rc = ctx.record_context().clone();
        let old = {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("to_table store not found");
            store.set_record_context(rc);
            let old = store.get(&key).await;
            store.put(key.clone(), r.value.clone()).await;
            old
        };
        self.forwarder
            .maybe_forward(ctx, key, old, r.value, r.timestamp);
    }
}

// ── KTableToStreamProcessor ──────────────────────────────────────────────────

/// Converts a `KTable` change-stream back to a `KStream`.
///
/// This processor extracts the `new` value of each `Change<V>`. It drops
/// tombstones, where `new == None`, because a `KStream` has no deletion record.
#[allow(dead_code)]
pub(crate) struct KTableToStreamProcessor<K, V> {
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, Change<V>, K, V> for KTableToStreamProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, Change<V>>) {
        if let Some(new) = r.value.new {
            ctx.forward(Record::new(r.key, new, r.timestamp));
        }
    }
}

// ── KTableMapValuesProcessor ─────────────────────────────────────────────────

/// Applies a value-mapping function to both sides of the incoming `Change<V>`.
///
/// This processor reconciles the materialized store with the mapped `new`. It
/// puts the value, or deletes it on a tombstone. It then forwards the mapped
/// `Change<V2>`. The **materialized** `map_values` form,
/// `map_values_materialized`, uses this processor.
#[allow(dead_code)]
pub(crate) struct KTableMapValuesProcessor<K, V, V2, F> {
    pub f: F,
    pub store_name: String,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V, V2)>,
}

#[async_trait]
impl<K, V, V2, F> Processor<K, Change<V>, K, Change<V2>> for KTableMapValuesProcessor<K, V, V2, F>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    V2: std::any::Any + Send + Clone,
    F: Fn(&V) -> V2 + Send + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V2>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<V2>>,
        r: Record<K, Change<V>>,
    ) {
        let key = r.key.expect("KTable map_values requires a non-null key");
        let mapped = r.value.map(|v| (self.f)(v));
        // Stash the source record context BEFORE the store borrow so a cached
        // store attaches it to the deduped change it forwards on flush.
        let rc = ctx.record_context().clone();
        {
            let store = ctx
                .get_state_store::<K, V2>(&self.store_name)
                .expect("KTable map_values store not found");
            store.set_record_context(rc);
            match &mapped.new {
                Some(nv) => {
                    store.put(key.clone(), nv.clone()).await;
                }
                None => {
                    store.delete(&key).await;
                }
            }
        }
        // Preserve the exact mapped Change (both sides mapped, including
        // tombstones): forward it, suppressed when the store is cached (the
        // cache flush forwards the deduped change instead).
        self.forwarder
            .maybe_forward_change(ctx, key, mapped, r.timestamp);
    }
}

// ── KTableMapValuesViewProcessor ─────────────────────────────────────────────

/// Applies a value-mapping function to both sides of the incoming `Change<V>`
/// and forwards the mapped `Change<V2>`.
///
/// This processor **does not materialize** a store. It backs the bare
/// `KTable::map_values` form, which matches the JVM's non-materialized
/// `mapValues` and produces no changelog topic.
#[allow(dead_code)]
pub(crate) struct KTableMapValuesViewProcessor<K, V, V2, F> {
    pub f: F,
    pub _pd: Marker<(K, V, V2)>,
}

#[async_trait]
impl<K, V, V2, F> Processor<K, Change<V>, K, Change<V2>>
    for KTableMapValuesViewProcessor<K, V, V2, F>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    V2: std::any::Any + Send + Clone,
    F: Fn(&V) -> V2 + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<V2>>,
        r: Record<K, Change<V>>,
    ) {
        ctx.forward(Record::new(
            r.key,
            r.value.map(|v| (self.f)(v)),
            r.timestamp,
        ));
    }
}

// ── KTableFilterProcessor ────────────────────────────────────────────────────

/// Re-applies the predicate to both sides of the incoming `Change<V>`.
///
/// This processor reconciles the materialized store with the surviving `new` and
/// forwards a `Change<V>`. A row that matched before and no longer matches
/// forwards a tombstone, where `new == None`, so downstream `KTable` views can
/// delete it.
#[allow(dead_code)]
pub(crate) struct KTableFilterProcessor<K, V, P> {
    pub predicate: P,
    pub store_name: String,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, P> Processor<K, Change<V>, K, Change<V>> for KTableFilterProcessor<K, V, P>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    P: Fn(&K, &V) -> bool + Send + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<V>>,
        r: Record<K, Change<V>>,
    ) {
        let key = r.key.expect("KTable filter requires a non-null key");
        let pred = &self.predicate;
        // A side that doesn't satisfy the predicate is treated as absent.
        let old_p = r.value.old.filter(|v| pred(&key, v));
        let new_p = r.value.new.filter(|v| pred(&key, v));
        // Stash the source record context BEFORE the store borrow so a cached
        // store attaches it to the deduped change it forwards on flush.
        let rc = ctx.record_context().clone();
        {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("KTable filter store not found");
            store.set_record_context(rc);
            match &new_p {
                Some(nv) => {
                    store.put(key.clone(), nv.clone()).await;
                }
                None => {
                    store.delete(&key).await;
                }
            }
        }
        // Forward only when something changed on either side; a row that never
        // matched (old & new both filtered out) produces no change record. The
        // forward is suppressed when the store is cached (the cache flush
        // forwards the deduped change — including tombstones — instead).
        if new_p.is_some() || old_p.is_some() {
            self.forwarder.maybe_forward_change(
                ctx,
                key,
                Change {
                    old: old_p,
                    new: new_p,
                },
                r.timestamp,
            );
        }
    }
}

// ── VersionedKTableSourceProcessor ──────────────────────────────────────────

/// Materializes incoming records into a `VersionedKeyValueStore` at the record's
/// timestamp, then forwards a `Change<V>`.
///
/// The change's `old` is the value that was valid at that timestamp *before*
/// this record. This follows the KIP-914 table semantics. An out-of-order record
/// still emits its local change, and the store keeps the latest pointer.
pub(crate) struct VersionedKTableSourceProcessor<K, V> {
    pub store_name: String,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, Change<V>> for VersionedKTableSourceProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r
            .key
            .expect("versioned KTable source requires a non-null key");
        let ts = r.timestamp;
        let old = {
            let store = ctx
                .get_versioned_store::<K, V>(&self.store_name)
                .expect("versioned KTable source store not found");
            let old = store.get_as_of(&key, ts).await.map(|rec| rec.value);
            store.put(key.clone(), Some(r.value.clone()), ts).await;
            old
        };
        ctx.forward(Record::new(Some(key), Change::update(old, r.value), ts));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;
    use crabka_units::prelude::*;

    use super::*;
    use crate::{
        processor::{
            api::ProcessorContext,
            erased::{Dispatch, ErasedRecord},
            record::RecordContext,
            serde::{I64Serde, StringSerde},
        },
        store::{kv::KeyValueBytesStore, registry::StoreRegistry},
    };

    fn make_stores() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
            "tbl".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "tbl-changelog".into(),
        )));
        stores
    }

    fn make_versioned_stores() -> StoreRegistry {
        use crate::store::versioned::VersionedBytesStore;
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(VersionedBytesStore::<String, i64>::in_memory(
            "vtbl".into(),
            secs(1_000),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "vtbl-changelog".into(),
        )));
        stores
    }

    fn rc() -> RecordContext {
        RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    #[tokio::test]
    async fn ktable_source_materializes_and_forwards() {
        let mut stores = make_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableSourceProcessor::<String, i64> {
            store_name: "tbl".into(),
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };

        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("k".into()), 42i64, 1))
                .await;
        }

        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        // First record for "k": no prior store value → old None, new 42.
        check!(change.old.is_none());
        check!(change.new == Some(42i64));
        check!(
            stores
                .get_kv::<String, i64>("tbl")
                .unwrap()
                .get(&"k".to_string())
                .await
                == Some(42)
        );
    }

    #[tokio::test]
    async fn versioned_ktable_source_out_of_order_changes() {
        // Feed k: (10@100), (20@200), (15@150 out-of-order).
        // Expected Change new/old pairs:
        //   @100: old=None, new=10
        //   @200: old=10,   new=20
        //   @150: old=10,   new=15 (get_as_of(150) before put = v@100)
        // After all three, store latest == 20.
        let mut stores = make_versioned_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = VersionedKTableSourceProcessor::<String, i64> {
            store_name: "vtbl".into(),
            _pd: PhantomData,
        };

        // Record 1: k=10 @ts=100
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("k".into()), 10i64, 100))
                .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(change.old.is_none(), "first record: no prior value");
        check!(change.new == Some(10i64));

        // Record 2: k=20 @ts=200
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("k".into()), 20i64, 200))
                .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(change.old == Some(10i64), "record @200: old was v@100=10");
        check!(change.new == Some(20i64));

        // Record 3: k=15 @ts=150 (out-of-order)
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("k".into()), 15i64, 150))
                .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(
            change.old == Some(10i64),
            "record @150: as_of(150) before put = v@100=10"
        );
        check!(change.new == Some(15i64));

        // Latest (non-versioned get) must still be 20.
        check!(
            stores
                .get_versioned::<String, i64>("vtbl")
                .unwrap()
                .get(&"k".to_string())
                .await
                .map(|r| r.value)
                == Some(20),
            "store latest must be 20 (the @200 record)"
        );
    }

    #[tokio::test]
    async fn ktable_to_stream_extracts_new_and_drops_tombstones() {
        let mut stores = make_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableToStreamProcessor::<String, i64> { _pd: PhantomData };

        // Update record: the `new` value is extracted and forwarded as plain V.
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::update(Some(1), 7i64), 5),
            )
            .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        check!(*rec.value.downcast::<i64>().unwrap() == 7i64);

        // Tombstone record: dropped — a KStream has no deletion record.
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::tombstone(Some(7i64)), 6),
            )
            .await;
        }
        check!(buffer.is_empty(), "tombstone must not reach the KStream");
    }

    #[tokio::test]
    async fn ktable_map_values_rewrites_and_materializes() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, String>::in_memory(
            "mv".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "mv-changelog".into(),
        )));
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableMapValuesProcessor::<String, i64, String, _> {
            f: |v: &i64| v.to_string(),
            store_name: "mv".into(),
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };

        // Use a store with String values since the output type is String.
        let mut stores2 = StoreRegistry::default();
        stores2.insert(Box::new(KeyValueBytesStore::<String, String>::in_memory(
            "mv".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "mv-changelog".into(),
        )));

        // Update: both sides map; the mapped `new` materializes.
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores2,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::update(Some(8i64), 9i64), 0),
            )
            .await;
        }

        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<String>>().unwrap();
        check!(change.old == Some("8".to_string()));
        check!(change.new == Some("9".to_string()));
        check!(
            stores2
                .get_kv::<String, String>("mv")
                .unwrap()
                .get(&"k".to_string())
                .await
                == Some("9".to_string())
        );

        // Tombstone: mapped `new` is None → the store entry is deleted.
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores2,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::tombstone(Some(9i64)), 1),
            )
            .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<String>>().unwrap();
        check!(change.new.is_none());
        check!(
            stores2
                .get_kv::<String, String>("mv")
                .unwrap()
                .get(&"k".to_string())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn ktable_map_values_view_rewrites_without_a_store() {
        // The non-materialized map_values forwards the rewritten value and never
        // touches a store — exercise with an empty StoreRegistry.
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableMapValuesViewProcessor::<String, i64, String, _> {
            f: |v: &i64| v.to_string(),
            _pd: PhantomData,
        };

        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("k".into()), Change::update(Some(8i64), 9i64), 0),
            )
            .await;
        }

        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<String>>().unwrap();
        check!(change.old == Some("8".to_string()));
        check!(change.new == Some("9".to_string()));
        // No store was created or required.
        check!(stores.names().is_empty());
    }

    #[tokio::test]
    // Three inline Dispatch blocks (each now carrying the punctuation fields) push
    // this exhaustive filter test just over the 100-line lint threshold.
    async fn ktable_filter_materializes_matches_and_emits_tombstones() {
        let mut stores = make_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        // Pre-seed the store with a value so we can also test the delete path.
        stores
            .get_kv::<String, i64>("tbl")
            .unwrap()
            .put("b".into(), 99)
            .await;

        let mut proc = KTableFilterProcessor::<String, i64, _> {
            predicate: |_k: &String, v: &i64| *v > 10,
            store_name: "tbl".into(),
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };

        // Matching record (old None → new 42) — stored and forwarded as an update.
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("a".into()), Change::update(None, 42i64), 1),
            )
            .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(change.old.is_none());
        check!(change.new == Some(42i64));
        check!(
            stores
                .get_kv::<String, i64>("tbl")
                .unwrap()
                .get(&"a".to_string())
                .await
                == Some(42)
        );

        // A row that previously matched (old 99) but no longer does (new 5) must
        // delete from the store AND forward a TOMBSTONE so downstream views drop
        // it. old_p survives the predicate (99 > 10); new_p is filtered out.
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("b".into()), Change::update(Some(99i64), 5i64), 2),
            )
            .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(change.old == Some(99i64), "old side survived the predicate");
        check!(change.new.is_none(), "new side filtered out → tombstone");
        check!(
            stores
                .get_kv::<String, i64>("tbl")
                .unwrap()
                .get(&"b".to_string())
                .await
                .is_none()
        );

        // A row that never matched (old & new both filtered out) → no forward.
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(
                &mut ctx,
                Record::new(Some("c".into()), Change::update(Some(3i64), 4i64), 3),
            )
            .await;
        }
        check!(
            buffer.is_empty(),
            "never-matching row must not be forwarded"
        );
    }

    /// A `tbl` store registry, optionally record-cached.
    fn source_registry(cached: bool) -> StoreRegistry {
        let mut stores = make_stores();
        if cached {
            stores.enable_cache(
                "tbl",
                std::sync::Arc::new(std::sync::Mutex::new(
                    crate::store::cache::named::NamedCache::new("tbl".into()),
                )),
            );
        }
        stores
    }

    /// Run `init`, then two same-key `process` calls through the `KTable`
    /// source. Returns how many records reached the downstream buffer.
    async fn source_run_two(stores: &mut StoreRegistry) -> usize {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let mut proc = KTableSourceProcessor::<String, i64> {
            store_name: "tbl".into(),
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };
        for v in 1..3i64 {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            if v == 1 {
                proc.init(&mut ctx).await;
            }
            proc.process(&mut ctx, Record::new(Some("k".into()), v, v))
                .await;
        }
        buffer.len()
    }

    /// Uncached store: the source forwards each record immediately, so two
    /// records give two forwards.
    #[tokio::test]
    async fn uncached_source_forwards_each_record() {
        let mut stores = source_registry(false);
        check!(source_run_two(&mut stores).await == 2);
    }

    /// Cached store: the processor suppresses the immediate forward, because the
    /// cache flush forwards the deduped change. Two records give zero immediate
    /// forwards, and the cached store still holds the dirty entry to flush.
    #[tokio::test]
    async fn cached_source_suppresses_immediate_forward() {
        let mut stores = source_registry(true);
        check!(source_run_two(&mut stores).await == 0);
        check!(stores.kv_is_cached("tbl"));
        let store = stores.get_kv::<String, i64>("tbl").unwrap();
        check!(store.get(&"k".to_string()).await == Some(2));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        stores
            .get_mut("tbl")
            .unwrap()
            .flush_cache_into(&mut buffer, &[0])
            .await;
        check!(buffer.len() == 1);
    }

    // ── KStream.to_table cache suppression ───────────────────────────────────

    /// Run `init`, then two same-key `process` calls through the `to_table`
    /// processor. Returns how many records reached the downstream buffer.
    async fn to_table_run_two(stores: &mut StoreRegistry) -> usize {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let mut proc = KStreamToTableProcessor::<String, i64> {
            store_name: "tbl".into(),
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };
        for v in 1..3i64 {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            if v == 1 {
                proc.init(&mut ctx).await;
            }
            proc.process(&mut ctx, Record::new(Some("k".into()), v, v))
                .await;
        }
        buffer.len()
    }

    /// Uncached: the processor forwards each record immediately, so two records
    /// give two forwards.
    #[tokio::test]
    async fn uncached_to_table_forwards_each_record() {
        let mut stores = source_registry(false);
        check!(to_table_run_two(&mut stores).await == 2);
    }

    /// Cached: the processor suppresses the immediate forward. The cache buffers
    /// the dirty entry, and the flush emits exactly ONE deduped change. The test
    /// also checks read-your-writes: the cached store holds the latest value (2)
    /// before the flush.
    #[tokio::test]
    async fn cached_to_table_suppresses_immediate_forward() {
        let mut stores = source_registry(true);
        check!(to_table_run_two(&mut stores).await == 0);
        check!(stores.kv_is_cached("tbl"));
        let store = stores.get_kv::<String, i64>("tbl").unwrap();
        check!(store.get(&"k".to_string()).await == Some(2));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        stores
            .get_mut("tbl")
            .unwrap()
            .flush_cache_into(&mut buffer, &[0])
            .await;
        check!(buffer.len() == 1);
    }

    // ── KTable.filter cache suppression (Bug B) ──────────────────────────────

    /// Run `init`, then two same-key updates through the filter processor. Both
    /// updates match the predicate, so the store ends with the latest value.
    /// Returns how many records reached the downstream buffer.
    async fn filter_run_two(stores: &mut StoreRegistry) -> usize {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let mut proc = KTableFilterProcessor::<String, i64, _> {
            predicate: |_k: &String, v: &i64| *v > 10,
            store_name: "tbl".into(),
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };
        // Two matching updates for "k": (None→20) then (20→30).
        let changes = [Change::update(None, 20i64), Change::update(Some(20i64), 30)];
        for (i, change) in changes.into_iter().enumerate() {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            if i == 0 {
                proc.init(&mut ctx).await;
            }
            let ts = i64::try_from(i).unwrap();
            proc.process(&mut ctx, Record::new(Some("k".into()), change, ts))
                .await;
        }
        buffer.len()
    }

    /// Uncached filter: the processor forwards each matching update immediately,
    /// so two updates give two forwards.
    #[tokio::test]
    async fn uncached_filter_forwards_each_record() {
        let mut stores = source_registry(false);
        check!(filter_run_two(&mut stores).await == 2);
    }

    /// Cached filter: the processor suppresses the immediate forward. The cache
    /// buffers the dirty entry, and the flush emits exactly ONE deduped change.
    #[tokio::test]
    async fn cached_filter_suppresses_immediate_forward() {
        let mut stores = source_registry(true);
        check!(filter_run_two(&mut stores).await == 0);
        check!(stores.kv_is_cached("tbl"));
        let store = stores.get_kv::<String, i64>("tbl").unwrap();
        check!(store.get(&"k".to_string()).await == Some(30));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        stores
            .get_mut("tbl")
            .unwrap()
            .flush_cache_into(&mut buffer, &[0])
            .await;
        check!(buffer.len() == 1);
    }

    // ── KTable.mapValues cache suppression (Bug B) ───────────────────────────

    /// A `mv` String-valued store registry, optionally record-cached.
    fn mv_registry(cached: bool) -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, String>::in_memory(
            "mv".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "mv-changelog".into(),
        )));
        if cached {
            stores.enable_cache(
                "mv",
                std::sync::Arc::new(std::sync::Mutex::new(
                    crate::store::cache::named::NamedCache::new("mv".into()),
                )),
            );
        }
        stores
    }

    /// Run `init`, then two same-key updates through the `map_values` processor.
    /// Returns how many records reached the downstream buffer.
    async fn map_values_run_two(stores: &mut StoreRegistry) -> usize {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let mut proc = KTableMapValuesProcessor::<String, i64, String, _> {
            f: |v: &i64| v.to_string(),
            store_name: "mv".into(),
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };
        let changes = [Change::update(None, 8i64), Change::update(Some(8i64), 9)];
        for (i, change) in changes.into_iter().enumerate() {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores,
                globals: &globals,
                node_idx: 0,
                schedules: &mut scheds,
                sched_stream_time: i64::MIN,
                sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<String>>::new(&mut dispatch);
            if i == 0 {
                proc.init(&mut ctx).await;
            }
            let ts = i64::try_from(i).unwrap();
            proc.process(&mut ctx, Record::new(Some("k".into()), change, ts))
                .await;
        }
        buffer.len()
    }

    /// Uncached `map_values`: the processor forwards each update immediately, so
    /// two updates give two forwards.
    #[tokio::test]
    async fn uncached_map_values_forwards_each_record() {
        let mut stores = mv_registry(false);
        check!(map_values_run_two(&mut stores).await == 2);
    }

    /// Cached `map_values`: the processor suppresses the immediate forward. The
    /// cache buffers the dirty entry, and the flush emits exactly ONE deduped
    /// change.
    #[tokio::test]
    async fn cached_map_values_suppresses_immediate_forward() {
        let mut stores = mv_registry(true);
        check!(map_values_run_two(&mut stores).await == 0);
        check!(stores.kv_is_cached("mv"));
        let store = stores.get_kv::<String, String>("mv").unwrap();
        check!(store.get(&"k".to_string()).await == Some("9".to_string()));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        stores
            .get_mut("mv")
            .unwrap()
            .flush_cache_into(&mut buffer, &[0])
            .await;
        check!(buffer.len() == 1);
    }
}
