//! `KTable` `Processor` impls over a #3 `KeyValueStore`.
//!
//! ## Change<old,new> propagation
//!
//! Every `KTable` node forwards a `Record<K, Change<V>>` change-stream so
//! downstream nodes can detect tombstones (`Change::new == None`). The
//! materialized **state stores still hold `V`** (the current value); only the
//! inter-node forwarded value is wrapped in `Change<V>`, which keeps the
//! changelog + wire topology byte-unchanged.
//!
//! - [`KTableSourceProcessor`]: `V` in → `Change<V>` out (reads the prior store
//!   value as `old`).
//! - [`KTableToStreamProcessor`]: `Change<V>` in → `V` out (extracts `new`,
//!   dropping tombstones — `toStream` produces a plain `KStream`).
//! - [`KTableMapValuesProcessor`] / [`KTableMapValuesViewProcessor`]:
//!   `Change<V>` in → `Change<V2>` out (`map` both sides).
//! - [`KTableFilterProcessor`]: `Change<V>` in → `Change<V>` out — re-applies
//!   the predicate to both sides and **emits tombstones** for rows that stop
//!   matching.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker reused from `stateless.rs`.
type Marker<T> = PhantomData<fn() -> T>;

// ── KTableSourceProcessor ────────────────────────────────────────────────────

/// Materializes incoming records into a `KeyValueStore`, then forwards a
/// `Change<V>` (prior store value as `old`, incoming value as `new`). Backs
/// `KTable` created from a source topic.
#[allow(dead_code)]
pub(crate) struct KTableSourceProcessor<K, V> {
    pub store_name: String,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, Change<V>> for KTableSourceProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("KTable source requires a non-null key");
        let old = {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("KTable source store not found");
            let old = store.get(&key).await;
            store.put(key.clone(), r.value.clone()).await;
            old
        };
        ctx.forward(Record::new(
            Some(key),
            Change::update(old, r.value),
            r.timestamp,
        ));
    }
}

// ── KStreamToTableProcessor ──────────────────────────────────────────────────

/// Materializes a `KStream` into a `KTable` (`KStream::to_table`): writes each
/// incoming `V` into the store and forwards a `Change<V>` (prior store value as
/// `old`, incoming value as `new`). Like [`KTableSourceProcessor`], but its input
/// is a plain `KStream` value rather than a source-topic record — so it is the
/// boundary where a stream becomes a changelog-backed table.
#[allow(dead_code)]
pub(crate) struct KStreamToTableProcessor<K, V> {
    pub store_name: String,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, Change<V>> for KStreamToTableProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("to_table requires a non-null key");
        let old = {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("to_table store not found");
            let old = store.get(&key).await;
            store.put(key.clone(), r.value.clone()).await;
            old
        };
        ctx.forward(Record::new(
            Some(key),
            Change::update(old, r.value),
            r.timestamp,
        ));
    }
}

// ── KTableToStreamProcessor ──────────────────────────────────────────────────

/// Converts a `KTable` change-stream back to a `KStream` by extracting the
/// `new` value of each `Change<V>`. Tombstones (`new == None`) are dropped — a
/// `KStream` has no notion of a deletion record.
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

/// Applies a value-mapping function to both sides of the incoming `Change<V>`,
/// reconciles the materialized store with the mapped `new` (put / delete on
/// tombstone), and forwards the mapped `Change<V2>`. Used by the
/// **materialized** `map_values` form (`map_values_materialized`).
#[allow(dead_code)]
pub(crate) struct KTableMapValuesProcessor<K, V, V2, F> {
    pub f: F,
    pub store_name: String,
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
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<V2>>,
        r: Record<K, Change<V>>,
    ) {
        let key = r.key.expect("KTable map_values requires a non-null key");
        let mapped = r.value.map(|v| (self.f)(v));
        {
            let store = ctx
                .get_state_store::<K, V2>(&self.store_name)
                .expect("KTable map_values store not found");
            match &mapped.new {
                Some(nv) => {
                    store.put(key.clone(), nv.clone()).await;
                }
                None => {
                    store.delete(&key).await;
                }
            }
        }
        ctx.forward(Record::new(Some(key), mapped, r.timestamp));
    }
}

// ── KTableMapValuesViewProcessor ─────────────────────────────────────────────

/// Applies a value-mapping function to both sides of the incoming `Change<V>`
/// and forwards the mapped `Change<V2>` **without materializing** a store. Backs
/// the bare `KTable::map_values` form (the JVM's non-materialized `mapValues`,
/// which produces no changelog topic).
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

/// Re-applies the predicate to both sides of the incoming `Change<V>`,
/// reconciles the materialized store with the surviving `new`, and forwards a
/// `Change<V>`. A row that previously matched but no longer does forwards a
/// tombstone (`new == None`) so downstream `KTable` views can delete it.
#[allow(dead_code)]
pub(crate) struct KTableFilterProcessor<K, V, P> {
    pub predicate: P,
    pub store_name: String,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, P> Processor<K, Change<V>, K, Change<V>> for KTableFilterProcessor<K, V, P>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    P: Fn(&K, &V) -> bool + Send + 'static,
{
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
        {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("KTable filter store not found");
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
        // matched (old & new both filtered out) produces no change record.
        if new_p.is_some() || old_p.is_some() {
            ctx.forward(Record::new(
                Some(key),
                Change {
                    old: old_p,
                    new: new_p,
                },
                r.timestamp,
            ));
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
    use crate::store::kv::KeyValueBytesStore;
    use crate::store::registry::StoreRegistry;

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
    async fn ktable_to_stream_extracts_new_and_drops_tombstones() {
        let mut stores = make_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableToStreamProcessor::<String, i64> { _pd: PhantomData };

        // Update record: the `new` value is extracted and forwarded as plain V.
        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores2,
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
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores2,
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
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
            _pd: PhantomData,
        };

        // Matching record (old None → new 42) — stored and forwarded as an update.
        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
}
