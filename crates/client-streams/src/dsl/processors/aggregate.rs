//! Aggregation processors over a #3 `KeyValueStore`.
//!
//! - `KStreamAggregateProcessor`: general aggregate; count = `init || 0`, `agg |_,_,a| a+1`;
//!   reduce = first-value seeded init, agg = reducer.

use std::marker::PhantomData;

use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Aggregate records into a typed accumulator stored in a #3 `KeyValueStore`.
///
/// For **count**: `init = || 0i64`, `agg = |_k, _v, acc| acc + 1`.
/// For **reduce**: `init = || first_value` (caller's responsibility to seed on
/// first record), `agg = reducer`.
///
/// Records with a null key are panicked — aggregations require non-null keys
/// (enforced by the repartition step preceding this node in the DSL lowering).
#[allow(dead_code)]
pub(crate) struct KStreamAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub init: I,
    pub agg: A,
    pub _pd: Marker<(K, V, VA)>,
}

impl<K, V, VA, I, A> Processor<K, V, K, VA> for KStreamAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Clone,
    V: 'static,
    VA: std::any::Any + Send + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VA>, r: Record<K, V>) {
        // Aggregations require non-null keys (post-repartition).
        let key = r.key.expect("aggregate requires a non-null key");
        let store = ctx
            .get_state_store::<K, VA>(&self.store_name)
            .expect("aggregate state store not found");
        let old = store.get(&key).unwrap_or_else(|| (self.init)());
        let new = (self.agg)(&key, &r.value, old);
        store.put(key.clone(), new.clone());
        ctx.forward(Record::new(Some(key), new, r.timestamp));
    }
}

/// Reduce records per key in a #3 `KeyValueStore` keyed by `K`, holding `V`.
///
/// The JVM `Reducer` has no separate `init`: the **first** value for a key seeds
/// the accumulator, and later values fold via `reducer(&acc, &value)`. This keeps
/// the public value type `V` (no `Option`/sentinel leaks into the `KTable`); the
/// "first value" check is the store lookup returning `None`.
///
/// Records with a null key are panicked — aggregations require non-null keys
/// (enforced by the repartition step preceding this node in the DSL lowering).
#[allow(dead_code)]
pub(crate) struct KStreamReduceProcessor<K, V, R> {
    pub store_name: String,
    pub reducer: R,
    pub _pd: Marker<(K, V)>,
}

impl<K, V, R> Processor<K, V, K, V> for KStreamReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Clone,
    V: std::any::Any + Send + Clone,
    R: Fn(&V, &V) -> V + Send + 'static,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        let key = r.key.expect("reduce requires a non-null key");
        let store = ctx
            .get_state_store::<K, V>(&self.store_name)
            .expect("reduce state store not found");
        let new = match store.get(&key) {
            // First value for this key seeds the accumulator.
            None => r.value,
            // Later values fold via the reducer.
            Some(acc) => (self.reducer)(&acc, &r.value),
        };
        store.put(key.clone(), new.clone());
        ctx.forward(Record::new(Some(key), new, r.timestamp));
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

    #[test]
    fn count_aggregate_accumulates_via_store() {
        // Build a StoreRegistry with an InMemoryKeyValueStore<String, i64>.
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(InMemoryKeyValueStore::<String, i64>::new(
            "counts".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-counts-changelog".into(),
        )));

        // Plumbing: a single child index so forward() actually enqueues the record.
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        // Build the count processor: init=||0, agg=|_k,_v,a| a+1.
        let mut proc = KStreamAggregateProcessor {
            store_name: "counts".into(),
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };

        // Process record 1: key="a", value="x".
        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 0));
        }

        // After 1st process: forwarded value should be 1.
        let (_, rec1) = buffer
            .pop_front()
            .expect("expected forwarded record after 1st process");
        check!(*rec1.value.downcast::<i64>().unwrap() == 1i64);

        // Process record 2: same key="a", value="x" again.
        {
            let mut dispatch = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 0));
        }

        // After 2nd process: forwarded value should be 2.
        let (_, rec2) = buffer
            .pop_front()
            .expect("expected forwarded record after 2nd process");
        check!(*rec2.value.downcast::<i64>().unwrap() == 2i64);

        // Store should now contain count=2 for key "a".
        let store = stores.get_kv::<String, i64>("counts").unwrap();
        check!(store.get(&"a".to_string()) == Some(2));
    }
}
