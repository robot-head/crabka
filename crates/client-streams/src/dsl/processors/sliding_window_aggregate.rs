//! Sliding-window aggregation processor (KIP-450): emit-on-update over inclusive,
//! data-defined windows of size `time_difference_ms`. Ports JVM
//! `KStreamSlidingWindowAggregate`.
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::windows::{SlidingWindows, Window, Windowed};
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Aggregate records into sliding windows (KIP-450), emit-on-update.
///
/// A record at time `t` belongs to every window `[ws, ws + W]` that contains
/// it.  Windows are **data-defined** (not epoch-aligned): the left window has
/// `ws = t - W` and the right window has `ws = t + 1` (open on the right, only
/// created if a later record already exists inside `[t+1, t+W]`).  Existing
/// windows whose range overlaps `t` are updated in place.
///
/// Stream-time tracks the maximum observed record timestamp; records older than
/// `stream_time - W - grace_ms` are silently dropped as late arrivals.
#[allow(dead_code)]
pub(crate) struct KStreamSlidingWindowAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub windows: SlidingWindows,
    pub init: I,
    pub agg: A,
    /// Observed max record timestamp (per task instance). Seeds late-record drop.
    pub stream_time: i64,
    pub _pd: Marker<(K, V, VA)>,
}

#[async_trait]
impl<K, V, VA, I, A> Processor<K, V, Windowed<K>, Change<VA>>
    for KStreamSlidingWindowAggregateProcessor<K, V, VA, I, A>
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
        let key = r.key.expect("sliding aggregate requires a non-null key");
        let w = self.windows.time_difference_ms;
        let t = r.timestamp;
        self.stream_time = self.stream_time.max(t);
        let close_time = self.stream_time - w - self.windows.grace_ms;
        if t < close_time {
            return; // too late; window already closed
        }

        // Scan windows that could contain `t` or seed its left window. A
        // predecessor window ending at `t - W` starts at `t - 2W`, so the lower
        // bound is `max(0, t - 2W)`; the `t + 1` upper bound catches an existing
        // right window starting at `t + 1`.
        let scan_from = (t - 2 * w).max(0);
        let found: Vec<(i64, i64, VA)> = {
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.fetch_with_ts(&key, scan_from, t + 1).await
        };

        let mut left_exists = false;
        let mut right_exists = false;
        let mut left_seed: Option<VA> = None;
        let mut updates: Vec<(i64, VA, i64)> = Vec::new(); // (windowStart, newAgg, newTs)

        for (ws, stored_ts, agg) in &found {
            let we = ws + w;
            if we == t {
                // This is exactly the left-window end: [t-W, t].
                left_exists = true;
                let new = (self.agg)(&key, &r.value, agg.clone());
                updates.push((*ws, new, (*stored_ts).max(t)));
            } else if *ws <= t && we > t {
                // Existing window that straddles t: [ws, ws+W] with ws < t < ws+W.
                let new = (self.agg)(&key, &r.value, agg.clone());
                updates.push((*ws, new, (*stored_ts).max(t)));
            } else if *ws == t + 1 {
                // Right-window sentinel: a window starting right after t.
                right_exists = true;
            } else if we < t {
                // Window ends before t — its aggregate can seed the left window.
                left_seed = Some(agg.clone());
            }
        }

        // If no existing left window [t-W, t] was found, create it.
        if !left_exists {
            let ls = (t - w).max(0);
            let seed = left_seed.clone().unwrap_or_else(|| (self.init)());
            let new = (self.agg)(&key, &r.value, seed);
            updates.push((ls, new, t));
        }
        // If a right-window sentinel exists, ensure it is initialised.
        if !right_exists {
            let has_later = found.iter().any(|(ws, _, _)| *ws > t && *ws <= t + w);
            if has_later {
                let new = (self.init)();
                updates.push((t + 1, new, t));
            }
        }

        updates.sort_by_key(|(ws, _, _)| *ws);
        for (ws, new, new_ts) in updates {
            let old = {
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                let prior = store.fetch_single(&key, ws).await.map(|(_ts, v)| v);
                store.put(key.clone(), ws, new.clone(), new_ts).await;
                prior
            };
            ctx.forward(Record::new(
                Some(Windowed {
                    key: key.clone(),
                    window: Window {
                        start: ws,
                        end: ws + w,
                    },
                }),
                Change::update(old, new),
                new_ts,
            ));
        }
    }
}

/// Reduce records into sliding windows (KIP-450).
///
/// Like [`KStreamSlidingWindowAggregateProcessor`] but uses the first value in
/// each window as the accumulator seed (no separate `init` function).
#[allow(dead_code)]
pub(crate) struct KStreamSlidingWindowReduceProcessor<K, V, R> {
    pub store_name: String,
    pub windows: SlidingWindows,
    pub reducer: R,
    pub stream_time: i64,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, R> Processor<K, V, Windowed<K>, Change<V>>
    for KStreamSlidingWindowReduceProcessor<K, V, R>
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
        let key = r.key.expect("sliding reduce requires a non-null key");
        let w = self.windows.time_difference_ms;
        let t = r.timestamp;
        self.stream_time = self.stream_time.max(t);
        let close_time = self.stream_time - w - self.windows.grace_ms;
        if t < close_time {
            return;
        }
        // Scan windows that could contain `t` or seed its left window. A
        // predecessor window ending at `t - W` starts at `t - 2W`, so the lower
        // bound is `max(0, t - 2W)`; the `t + 1` upper bound catches an existing
        // right window starting at `t + 1`.
        let scan_from = (t - 2 * w).max(0);
        let found: Vec<(i64, i64, V)> = {
            let store = ctx
                .get_window_store::<K, V>(&self.store_name)
                .expect("window store not found");
            store.fetch_with_ts(&key, scan_from, t + 1).await
        };
        let mut left_exists = false;
        let mut right_exists = false;
        let mut updates: Vec<(i64, V, i64)> = Vec::new();
        for (ws, stored_ts, agg) in &found {
            let we = ws + w;
            if we == t || (*ws <= t && we > t) {
                if we == t {
                    left_exists = true;
                }
                let new = (self.reducer)(agg, &r.value);
                updates.push((*ws, new, (*stored_ts).max(t)));
            } else if *ws == t + 1 {
                right_exists = true;
            }
        }
        if !left_exists {
            let ls = (t - w).max(0);
            let seed = found
                .iter()
                .filter(|(ws, _, _)| ws + w < t)
                .last()
                .map_or_else(|| r.value.clone(), |(_, _, v)| (self.reducer)(v, &r.value));
            updates.push((ls, seed, t));
        }
        if !right_exists {
            let has_later = found.iter().any(|(ws, _, _)| *ws > t && *ws <= t + w);
            if has_later {
                updates.push((t + 1, r.value.clone(), t));
            }
        }
        updates.sort_by_key(|(ws, _, _)| *ws);
        for (ws, new, new_ts) in updates {
            let old = {
                let store = ctx
                    .get_window_store::<K, V>(&self.store_name)
                    .expect("window store not found");
                let prior = store.fetch_single(&key, ws).await.map(|(_ts, v)| v);
                store.put(key.clone(), ws, new.clone(), new_ts).await;
                prior
            };
            ctx.forward(Record::new(
                Some(Windowed {
                    key: key.clone(),
                    window: Window {
                        start: ws,
                        end: ws + w,
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
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::registry::StoreRegistry;
    use crate::store::window::WindowBytesStore;

    #[allow(clippy::type_complexity)]
    async fn run(
        proc: &mut KStreamSlidingWindowAggregateProcessor<
            String,
            String,
            i64,
            fn() -> i64,
            fn(&String, &String, i64) -> i64,
        >,
        stores: &mut StoreRegistry,
        key: &str,
        ts: i64,
    ) -> Vec<(Window, Option<i64>, Option<i64>)> {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: ts,
        };
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        {
            let mut d = Dispatch {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(key.into()), "x".into(), ts))
                .await;
        }
        buffer
            .into_iter()
            .map(|(_, rec)| {
                let k = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
                let c = rec.value.downcast::<Change<i64>>().unwrap();
                (k.window, c.old, c.new)
            })
            .collect()
    }

    fn store() -> StoreRegistry {
        let mut s = StoreRegistry::default();
        s.insert(Box::new(WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
        )));
        s
    }

    #[allow(clippy::type_complexity)]
    fn count_proc() -> KStreamSlidingWindowAggregateProcessor<
        String,
        String,
        i64,
        fn() -> i64,
        fn(&String, &String, i64) -> i64,
    > {
        KStreamSlidingWindowAggregateProcessor {
            store_name: "w".into(),
            windows: SlidingWindows::of_time_difference_with_no_grace(10),
            init: (|| 0i64) as fn() -> i64,
            agg: (|_k: &String, _v: &String, a: i64| a + 1) as fn(&String, &String, i64) -> i64,
            stream_time: i64::MIN,
            _pd: PhantomData,
        }
    }

    #[tokio::test]
    async fn first_record_creates_left_window() {
        let mut stores = store();
        let mut p = count_proc();
        let out = run(&mut p, &mut stores, "a", 20).await;
        assert!(
            out.contains(&(Window { start: 10, end: 20 }, None, Some(1))),
            "expected left window [10,20]=1, got {out:?}"
        );
    }

    #[tokio::test]
    async fn adjacent_record_seeds_new_left_window() {
        let mut stores = store();
        let mut p = count_proc();
        let _ = run(&mut p, &mut stores, "a", 20).await;
        let out = run(&mut p, &mut stores, "a", 25).await;
        assert!(
            out.contains(&(Window { start: 15, end: 25 }, None, Some(2))),
            "expected left window [15,25]=2, got {out:?}"
        );
    }
}
