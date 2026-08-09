//! Aggregation processors over a #3 `KeyValueStore`.
//!
//! - `KStreamAggregateProcessor` is the general aggregate. For count,
//!   `init` is `|| 0` and `agg` is `|_,_,a| a+1`. For reduce, the first value
//!   seeds `init` and `agg` is the reducer.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{
    dsl::processors::{change::Change, tuple_forwarder::TupleForwarder},
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
    },
};

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Aggregate records into a typed accumulator stored in a #3 `KeyValueStore`.
///
/// For **count**, `init = || 0i64` and `agg = |_k, _v, acc| acc + 1`.
/// For **reduce**, `init = || first_value` and `agg = reducer`. The caller must
/// seed `init` on the first record.
///
/// The processor panics on a record with a null key. Aggregations need non-null
/// keys, and the repartition step before this node in the DSL lowering enforces
/// that.
#[allow(dead_code)]
pub(crate) struct KStreamAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub init: I,
    pub agg: A,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V, VA)>,
}

#[async_trait]
impl<K, V, VA, I, A> Processor<K, V, K, Change<VA>> for KStreamAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VA: std::any::Any + Send + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<VA>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VA>>,
        r: Record<K, V>,
    ) {
        // Aggregations require non-null keys (post-repartition).
        let key = r.key.expect("aggregate requires a non-null key");
        // Stash the source record context BEFORE the store borrow so a cached
        // store attaches it to the deduped change it forwards on flush.
        let rc = ctx.record_context().clone();
        // Hold the store borrow only across the store awaits; drop it before
        // `maybe_forward` (which re-borrows `ctx`). Aggregate math is unchanged:
        // seed the accumulator with `init` when the store has no prior value.
        // The forwarded `Change.old` is the *actual* prior store value (None on
        // the first record for this key), not the init-seeded accumulator.
        let (old, new) = {
            let store = ctx
                .get_state_store::<K, VA>(&self.store_name)
                .expect("aggregate state store not found");
            store.set_record_context(rc);
            let old = store.get(&key).await;
            let seed = old.clone().unwrap_or_else(|| (self.init)());
            let new = (self.agg)(&key, &r.value, seed);
            store.put(key.clone(), new.clone()).await;
            (old, new)
        };
        self.forwarder
            .maybe_forward(ctx, key, old, new, r.timestamp);
    }
}

/// Reduce records per key in a #3 `KeyValueStore` keyed by `K`, holding `V`.
///
/// The JVM `Reducer` has no separate `init`. The **first** value for a key seeds
/// the accumulator, and later values fold in through `reducer(&acc, &value)`.
///
/// This keeps the public value type `V`, so no `Option` and no sentinel leaks
/// into the `KTable`. The check for the first value is the store lookup that
/// returns `None`.
///
/// The processor panics on a record with a null key. Aggregations need non-null
/// keys, and the repartition step before this node in the DSL lowering enforces
/// that.
#[allow(dead_code)]
pub(crate) struct KStreamReduceProcessor<K, V, R> {
    pub store_name: String,
    pub reducer: R,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, R> Processor<K, V, K, Change<V>> for KStreamReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    R: Fn(&V, &V) -> V + Send + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("reduce requires a non-null key");
        let rc = ctx.record_context().clone();
        // Reduce math unchanged: the first value for a key seeds the accumulator;
        // later values fold via the reducer. `old` is the prior store value
        // (None on the first record), forwarded as the `Change.old`. Hold the
        // store borrow only across the awaits, then drop it before forwarding.
        let (old, new) = {
            let store = ctx
                .get_state_store::<K, V>(&self.store_name)
                .expect("reduce state store not found");
            store.set_record_context(rc);
            let old = store.get(&key).await;
            let new = match &old {
                None => r.value,
                Some(acc) => (self.reducer)(acc, &r.value),
            };
            store.put(key.clone(), new.clone()).await;
            (old, new)
        };
        self.forwarder
            .maybe_forward(ctx, key, old, new, r.timestamp);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

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

    #[tokio::test]
    async fn count_aggregate_accumulates_via_store() {
        // Build a StoreRegistry with a KeyValueBytesStore<String, i64>.
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
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
            forwarder: TupleForwarder::default(),
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };

        // Process record 1: key="a", value="x".
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
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 0))
                .await;
        }

        // After 1st process: forwarded Change is (old None → new 1).
        let (_, rec1) = buffer
            .pop_front()
            .expect("expected forwarded record after 1st process");
        let change1 = rec1.value.downcast::<Change<i64>>().unwrap();
        check!(change1.old.is_none());
        check!(change1.new == Some(1i64));

        // Process record 2: same key="a", value="x" again.
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
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 0))
                .await;
        }

        // After 2nd process: forwarded Change is (old 1 → new 2).
        let (_, rec2) = buffer
            .pop_front()
            .expect("expected forwarded record after 2nd process");
        let change2 = rec2.value.downcast::<Change<i64>>().unwrap();
        check!(change2.old == Some(1i64));
        check!(change2.new == Some(2i64));

        // Store should now contain count=2 for key "a".
        let store = stores.get_kv::<String, i64>("counts").unwrap();
        check!(store.get(&"a".to_string()).await == Some(2));
    }

    /// A `counts` store registry, optionally record-cached.
    fn counts_registry(cached: bool) -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
            "counts".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-counts-changelog".into(),
        )));
        if cached {
            stores.enable_cache(
                "counts",
                std::sync::Arc::new(std::sync::Mutex::new(
                    crate::store::cache::named::NamedCache::new("counts".into()),
                )),
            );
        }
        stores
    }

    /// Run `init`, then two same-key `process` calls, through the count
    /// aggregate. The helper returns how many records reached the downstream
    /// buffer.
    async fn run_two(stores: &mut StoreRegistry) -> usize {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        let mut proc = KStreamAggregateProcessor {
            store_name: "counts".into(),
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };
        for ts in 0..2i64 {
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
            if ts == 0 {
                // Resolve the forwarder's cache state from the store on first use.
                proc.init(&mut ctx).await;
            }
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), ts))
                .await;
        }
        buffer.len()
    }

    /// With an uncached store the aggregate forwards each record at once, so two
    /// records give two forwards.
    #[tokio::test]
    async fn uncached_aggregate_forwards_each_record() {
        let mut stores = counts_registry(false);
        check!(run_two(&mut stores).await == 2);
    }

    /// With a cached store the immediate forward is suppressed, because the cache
    /// flush forwards the deduped change later. Two records then give zero
    /// immediate forwards, and the cached store holds the dirty entry until the
    /// flush.
    #[tokio::test]
    async fn cached_aggregate_suppresses_immediate_forward() {
        let mut stores = counts_registry(true);
        check!(run_two(&mut stores).await == 0);
        // The store is cached and read-your-writes shows the staged count=2.
        check!(stores.kv_is_cached("counts"));
        let store = stores.get_kv::<String, i64>("counts").unwrap();
        check!(store.get(&"a".to_string()).await == Some(2));
        // The dirty entry is still buffered in the cache: flushing emits exactly
        // one deduped downstream record (proving suppression deferred, not dropped).
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        stores
            .get_mut("counts")
            .unwrap()
            .flush_cache_into(&mut buffer, &[0])
            .await;
        check!(buffer.len() == 1);
    }
}
