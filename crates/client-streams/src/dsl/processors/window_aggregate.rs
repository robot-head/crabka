//! Windowed aggregation processor: emit-on-every-update (no window closing).
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::windows::{TimeWindows, Window, Windowed};
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Aggregate records into a windowed accumulator stored in a `WindowStore`.
///
/// Emits a `Change<VA>` per window that the record falls into (tumbling:
/// one window; hopping: multiple). This is the **emit-on-update** strategy —
/// the window is never "closed"; every record update is forwarded immediately.
///
/// Records with a null key are panicked (aggregations require non-null keys,
/// enforced by the repartition step preceding this node in the DSL lowering).
#[allow(dead_code)]
pub(crate) struct KStreamWindowAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub windows: TimeWindows,
    pub init: I,
    pub agg: A,
    pub _pd: Marker<(K, V, VA)>,
}

#[async_trait]
impl<K, V, VA, I, A> Processor<K, V, Windowed<K>, Change<VA>>
    for KStreamWindowAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VA: std::any::Any + Send + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("windowed aggregate requires a non-null key");
        let size = self.windows.size_ms;

        for ws in self.windows.windows_for(r.timestamp) {
            // Borrow the store, do the async fetch + put, then drop the borrow
            // before calling ctx.forward (which re-borrows ctx mutably).
            let (old, new, new_ts) = {
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                let prior = store.fetch_single(&key, ws).await;
                // Extract storedTs before consuming `prior` for `old`.
                let stored_ts = prior.as_ref().map_or(i64::MIN, |&(ts, _)| ts);
                let old = prior.map(|(_ts, v)| v);
                let seed = old.clone().unwrap_or_else(|| (self.init)());
                let new = (self.agg)(&key, &r.value, seed);
                let new_ts = std::cmp::max(r.timestamp, stored_ts);
                store.put(key.clone(), ws, new.clone(), new_ts).await;
                (old, new, new_ts)
            };
            ctx.forward(Record::new(
                Some(Windowed {
                    key: key.clone(),
                    window: Window {
                        start: ws,
                        end: ws + size,
                    },
                }),
                Change::update(old, new),
                new_ts,
            ));
        }
    }
}

/// Reduce records into a windowed accumulator stored in a `WindowStore`.
///
/// The windowed analogue of [`KStreamReduceProcessor`]: the **first** value in a
/// window seeds the accumulator (no separate `init`), later values fold via
/// `reducer(&acc, &value)`. Keeps the public value type `V` (no `Option`/sentinel
/// leaks into the `KTable`); the "first value" check is the windowed store lookup
/// returning `None`.
///
/// Emits a `Change<V>` per window that the record falls into (tumbling: one
/// window; hopping: multiple) — the **emit-on-update** strategy.
///
/// Records with a null key are panicked (aggregations require non-null keys,
/// enforced by the repartition step preceding this node in the DSL lowering).
#[allow(dead_code)]
pub(crate) struct KStreamWindowReduceProcessor<K, V, R> {
    pub store_name: String,
    pub windows: TimeWindows,
    pub reducer: R,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, R> Processor<K, V, Windowed<K>, Change<V>> for KStreamWindowReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    R: Fn(&V, &V) -> V + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("windowed reduce requires a non-null key");
        let size = self.windows.size_ms;

        for ws in self.windows.windows_for(r.timestamp) {
            // Borrow the store, do the async fetch + put, then drop the borrow
            // before calling ctx.forward (which re-borrows ctx mutably).
            let (old, new, new_ts) = {
                let store = ctx
                    .get_window_store::<K, V>(&self.store_name)
                    .expect("window store not found");
                let prior = store.fetch_single(&key, ws).await;
                let stored_ts = prior.as_ref().map_or(i64::MIN, |&(ts, _)| ts);
                let old = prior.map(|(_ts, v)| v);
                // First value in this window seeds the accumulator; else fold.
                let new = match &old {
                    None => r.value.clone(),
                    Some(acc) => (self.reducer)(acc, &r.value),
                };
                let new_ts = std::cmp::max(r.timestamp, stored_ts);
                store.put(key.clone(), ws, new.clone(), new_ts).await;
                (old, new, new_ts)
            };
            ctx.forward(Record::new(
                Some(Windowed {
                    key: key.clone(),
                    window: Window {
                        start: ws,
                        end: ws + size,
                    },
                }),
                Change::update(old, new),
                new_ts,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::marker::PhantomData;

    use super::*;
    use crate::dsl::windows::{TimeWindows, Window, Windowed};
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::registry::StoreRegistry;
    use crate::store::window::WindowBytesStore;

    #[tokio::test]
    async fn windowed_count_tumbling_emits_per_window() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
        )));

        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        let mut proc = KStreamWindowAggregateProcessor {
            store_name: "w".into(),
            windows: TimeWindows::of_size(10),
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };

        // record at ts=3 → window [0,10), count 1
        {
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 3))
                .await;
        }

        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        let key = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(key.window, Window { start: 0, end: 10 });
        assert_eq!(change.old, None);
        assert_eq!(change.new, Some(1));

        // record at ts=7 → same window [0,10), count 2
        {
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 7))
                .await;
        }

        let (_, rec2) = buffer.pop_front().unwrap();
        let change2 = rec2.value.downcast::<Change<i64>>().unwrap();
        assert_eq!(change2.old, Some(1));
        assert_eq!(change2.new, Some(2));

        // record at ts=12 → window [10,20), count 1
        {
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
            };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 12))
                .await;
        }

        let (_, rec3) = buffer.pop_front().unwrap();
        assert_eq!(
            rec3.key
                .unwrap()
                .downcast::<Windowed<String>>()
                .unwrap()
                .window,
            Window { start: 10, end: 20 }
        );
        assert_eq!(rec3.value.downcast::<Change<i64>>().unwrap().new, Some(1));
    }
}
