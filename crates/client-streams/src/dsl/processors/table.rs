//! `KTable` `Processor` impls over a #3 `KeyValueStore`.
//!
//! ## KTable.filter tombstone simplification (first slice)
//!
//! Full `KTable` semantics require propagating a `Change<V>` (old, new) so
//! downstream nodes can detect tombstones. In this slice `VOut = V` (no
//! wrapper), so `KTableFilterProcessor` takes the simpler approach:
//!
//! - Matching rows → `store.put(key, value); forward(record)`.
//! - Non-matching rows → `store.delete(&key)` to keep the materialized view
//!   correct; **no forward** (the record is dropped). Full tombstone
//!   propagation via `Change<V>` is deferred to #4c.

use std::marker::PhantomData;

use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker reused from `stateless.rs`.
type Marker<T> = PhantomData<fn() -> T>;

// ── KTableSourceProcessor ────────────────────────────────────────────────────

/// Materializes incoming records into a `KeyValueStore`, then forwards them
/// unchanged as a change-stream. Backs `KTable` created from a source topic.
#[allow(dead_code)]
pub(crate) struct KTableSourceProcessor<K, V> {
    pub store_name: String,
    pub _pd: Marker<(K, V)>,
}

impl<K, V> Processor<K, V, K, V> for KTableSourceProcessor<K, V>
where
    K: std::any::Any + Send + Clone,
    V: std::any::Any + Send + Clone,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        let key = r.key.expect("KTable source requires a non-null key");
        let store = ctx
            .get_state_store::<K, V>(&self.store_name)
            .expect("KTable source store not found");
        store.put(key.clone(), r.value.clone());
        ctx.forward(Record::new(Some(key), r.value, r.timestamp));
    }
}

// ── KTableToStreamProcessor ──────────────────────────────────────────────────

/// Converts a `KTable` change-stream back to a `KStream` by forwarding every
/// record unchanged.
#[allow(dead_code)]
pub(crate) struct KTableToStreamProcessor<K, V> {
    pub _pd: Marker<(K, V)>,
}

impl<K, V> Processor<K, V, K, V> for KTableToStreamProcessor<K, V>
where
    K: std::any::Any + Send + Clone,
    V: std::any::Any + Send + Clone,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        ctx.forward(r);
    }
}

// ── KTableMapValuesProcessor ─────────────────────────────────────────────────

/// Applies a value-mapping function, writes the new value into the store, and
/// forwards the rewritten record.
#[allow(dead_code)]
pub(crate) struct KTableMapValuesProcessor<K, V, V2, F> {
    pub f: F,
    pub store_name: String,
    pub _pd: Marker<(K, V, V2)>,
}

impl<K, V, V2, F> Processor<K, V, K, V2> for KTableMapValuesProcessor<K, V, V2, F>
where
    K: std::any::Any + Send + Clone,
    V: 'static,
    V2: std::any::Any + Send + Clone,
    F: Fn(&V) -> V2 + Send + 'static,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V2>, r: Record<K, V>) {
        let key = r.key.expect("KTable map_values requires a non-null key");
        let new_value = (self.f)(&r.value);
        let store = ctx
            .get_state_store::<K, V2>(&self.store_name)
            .expect("KTable map_values store not found");
        store.put(key.clone(), new_value.clone());
        ctx.forward(Record::new(Some(key), new_value, r.timestamp));
    }
}

// ── KTableFilterProcessor ────────────────────────────────────────────────────

/// Keeps rows matching the predicate in the store and forwards them; drops
/// non-matching rows from the store without forwarding (see module doc).
#[allow(dead_code)]
pub(crate) struct KTableFilterProcessor<K, V, P> {
    pub predicate: P,
    pub store_name: String,
    pub _pd: Marker<(K, V)>,
}

impl<K, V, P> Processor<K, V, K, V> for KTableFilterProcessor<K, V, P>
where
    K: std::any::Any + Send + Clone,
    V: std::any::Any + Send + Clone,
    P: Fn(&K, &V) -> bool + Send + 'static,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        let key = r.key.expect("KTable filter requires a non-null key");
        if (self.predicate)(&key, &r.value) {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("KTable filter store not found");
            store.put(key.clone(), r.value.clone());
            ctx.forward(Record::new(Some(key), r.value, r.timestamp));
        } else {
            // Non-matching: remove from the materialized view, no forward.
            // Full tombstone propagation via Change<V> is deferred to #4c.
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("KTable filter store not found");
            store.delete(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::RecordContext;
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::memory::InMemoryKeyValueStore;
    use crate::store::registry::StoreRegistry;

    fn make_stores() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(InMemoryKeyValueStore::<String, i64>::new(
            "tbl".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "tbl-changelog".into(),
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

    #[test]
    fn ktable_source_materializes_and_forwards() {
        let mut stores = make_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableSourceProcessor::<String, i64> {
            store_name: "tbl".into(),
            _pd: PhantomData,
        };

        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("k".into()), 42i64, 1));
        }

        let (_, rec) = buffer.pop_front().unwrap();
        check!(*rec.value.downcast::<i64>().unwrap() == 42i64);
        check!(
            stores
                .get_kv::<String, i64>("tbl")
                .unwrap()
                .get(&"k".to_string())
                == Some(42)
        );
    }

    #[test]
    fn ktable_to_stream_forwards_unchanged() {
        let mut stores = make_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableToStreamProcessor::<String, i64> { _pd: PhantomData };

        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("k".into()), 7i64, 5));
        }

        let (_, rec) = buffer.pop_front().unwrap();
        check!(*rec.value.downcast::<i64>().unwrap() == 7i64);
    }

    #[test]
    fn ktable_map_values_rewrites_and_materializes() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(InMemoryKeyValueStore::<String, String>::new(
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
            _pd: PhantomData,
        };

        // Use a store with String values since the output type is String.
        let mut stores2 = StoreRegistry::default();
        stores2.insert(Box::new(InMemoryKeyValueStore::<String, String>::new(
            "mv".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "mv-changelog".into(),
        )));

        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores2,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("k".into()), 9i64, 0));
        }

        let (_, rec) = buffer.pop_front().unwrap();
        check!(*rec.value.downcast::<String>().unwrap() == "9");
        check!(
            stores2
                .get_kv::<String, String>("mv")
                .unwrap()
                .get(&"k".to_string())
                == Some("9".to_string())
        );
    }

    #[test]
    fn ktable_filter_matching_materializes_and_forwards() {
        let mut stores = make_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        // Pre-seed the store with a value so we can also test the delete path.
        stores
            .get_kv::<String, i64>("tbl")
            .unwrap()
            .put("b".into(), 99);

        let mut proc = KTableFilterProcessor::<String, i64, _> {
            predicate: |_k: &String, v: &i64| *v > 10,
            store_name: "tbl".into(),
            _pd: PhantomData,
        };

        // Matching record — should be stored and forwarded.
        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("a".into()), 42i64, 1));
        }
        let (_, rec) = buffer.pop_front().unwrap();
        check!(*rec.value.downcast::<i64>().unwrap() == 42i64);
        check!(
            stores
                .get_kv::<String, i64>("tbl")
                .unwrap()
                .get(&"a".to_string())
                == Some(42)
        );

        // Non-matching record — should delete "b" from the store, no forward.
        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("b".into()), 5i64, 2));
        }
        check!(buffer.is_empty(), "non-matching row must not be forwarded");
        check!(
            stores
                .get_kv::<String, i64>("tbl")
                .unwrap()
                .get(&"b".to_string())
                .is_none()
        );
    }
}
