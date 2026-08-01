//! Windowed aggregation processors: emit-on-update (default) or emit-on-window-close (KIP-825).
use std::marker::PhantomData;

use async_trait::async_trait;
use crabka_units::prelude::*;

use crate::{
    dsl::{
        processors::{change::Change, tuple_forwarder::TupleForwarder},
        windows::{TimeWindows, Window, Windowed},
    },
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
    },
};

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Aggregate records into a windowed accumulator stored in a `WindowStore`.
///
/// Behavior depends on the [`EmitStrategy`](crate::dsl::emit::EmitStrategy)
/// field: in `on_window_update` (default) it emits a `Change<VA>` per window the
/// record falls into (tumbling: one window; hopping: multiple) immediately; in
/// `on_window_close` (KIP-825) it suppresses those per-update emits and instead
/// forwards each window's final result once stream-time passes its close.
///
/// Records with a null key are panicked (aggregations require non-null keys,
/// enforced by the repartition step preceding this node in the DSL lowering).
#[allow(dead_code)]
pub(crate) struct KStreamWindowAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub windows: TimeWindows,
    pub init: I,
    pub agg: A,
    /// Emit on every update (default) or only on window close (KIP-825).
    pub emit: crate::dsl::emit::EmitStrategy,
    /// Observed max record timestamp (per task instance) — drives window-close.
    pub stream_time: i64,
    /// Highest `window_close_time` already emitted; prevents re-emit.
    pub last_emitted_close: i64,
    /// Forward-suppression seam: when the window store is record-cached the
    /// per-update forward is suppressed (the cache flush forwards the deduped
    /// `Change`). Resolved in `init`. Only used on the emit-on-update path —
    /// emit-final stores are never cached.
    pub forwarder: TupleForwarder,
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
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("windowed aggregate requires a non-null key");
        // The window bounds below are epoch-millisecond instants, so the size
        // and grace extents cross into that coordinate space here.
        let size = self.windows.size.millis_i64();
        self.stream_time = self.stream_time.max(r.timestamp);
        let window_close_time = self.stream_time - self.windows.grace.millis_i64();
        // Stash the source record context BEFORE the store borrow so a cached
        // store attaches it to the deduped change it forwards on flush.
        let rc = ctx.record_context().clone();

        for ws in self.windows.windows_for(r.timestamp) {
            // Emit-final drops updates for windows that already closed.
            if self.emit.is_on_close() && ws + size < self.last_emitted_close {
                continue;
            }
            // Borrow the store, do the async fetch + put, then drop the borrow
            // before calling ctx.forward (which re-borrows ctx mutably).
            let (old, new, new_ts) = {
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                store.set_record_context(rc.clone());
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
            if self.emit.is_on_update() {
                // Suppressed when the store is cached (cache flush forwards the
                // deduped change); forwarded immediately otherwise.
                self.forwarder.maybe_forward_change(
                    ctx,
                    Windowed {
                        key: key.clone(),
                        window: Window {
                            start: ws,
                            end: ws + size,
                        },
                    },
                    Change::update(old, new),
                    new_ts,
                );
            }
        }

        if self.emit.is_on_close() {
            self.emit_closed_windows(ctx, window_close_time).await;
        }
    }
}

impl<K, V, VA, I, A> KStreamWindowAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VA: std::any::Any + Send + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
{
    /// Forward each window whose `end <= window_close_time` and `end >
    /// last_emitted_close` as a final `Change`, ascending by window start, then
    /// advance the watermark.
    async fn emit_closed_windows(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        window_close_time: i64,
    ) {
        // The window bounds below are epoch-millisecond instants, so the size
        // and grace extents cross into that coordinate space here.
        let size = self.windows.size.millis_i64();
        // JVM closes a window strictly: emit once stream-time moves PAST the end
        // (`end < window_close_time`), so a zero-width / boundary window is not
        // finalized at its own stream-time. end = start + size < close ⟺ start <= close-size-1.
        let start_to = window_close_time - size - 1;
        let start_from = self.last_emitted_close.saturating_sub(size);
        let mut due = {
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.fetch_all_in_range(start_from, start_to).await
        };
        due.retain(|(_, ws, _, _)| ws + size >= self.last_emitted_close);
        due.sort_by_key(|(_, ws, _, _)| *ws);
        for (k, ws, ts, v) in due {
            ctx.forward(Record::new(
                Some(Windowed {
                    key: k,
                    window: Window {
                        start: ws,
                        end: ws + size,
                    },
                }),
                Change::update(None, v),
                ts,
            ));
        }
        self.last_emitted_close = window_close_time;
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
/// Like [`KStreamWindowAggregateProcessor`], the
/// [`EmitStrategy`](crate::dsl::emit::EmitStrategy) field selects emit-on-update
/// (default, a `Change<V>` per touched window) or emit-on-window-close (KIP-825,
/// final result only once a window closes).
///
/// Records with a null key are panicked (aggregations require non-null keys,
/// enforced by the repartition step preceding this node in the DSL lowering).
#[allow(dead_code)]
pub(crate) struct KStreamWindowReduceProcessor<K, V, R> {
    pub store_name: String,
    pub windows: TimeWindows,
    pub reducer: R,
    /// Emit on every update (default) or only on window close (KIP-825).
    pub emit: crate::dsl::emit::EmitStrategy,
    /// Observed max record timestamp (per task instance) — drives window-close.
    pub stream_time: i64,
    /// Highest `window_close_time` already emitted; prevents re-emit.
    pub last_emitted_close: i64,
    /// Forward-suppression seam (see [`KStreamWindowAggregateProcessor::forwarder`]).
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, R> Processor<K, V, Windowed<K>, Change<V>> for KStreamWindowReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    R: Fn(&V, &V) -> V + Send + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("windowed reduce requires a non-null key");
        // The window bounds below are epoch-millisecond instants, so the size
        // and grace extents cross into that coordinate space here.
        let size = self.windows.size.millis_i64();
        self.stream_time = self.stream_time.max(r.timestamp);
        let window_close_time = self.stream_time - self.windows.grace.millis_i64();
        // Stash the source record context for cached writes (see aggregate proc).
        let rc = ctx.record_context().clone();

        for ws in self.windows.windows_for(r.timestamp) {
            // Emit-final drops updates for windows that already closed.
            if self.emit.is_on_close() && ws + size < self.last_emitted_close {
                continue;
            }
            // Borrow the store, do the async fetch + put, then drop the borrow
            // before calling ctx.forward (which re-borrows ctx mutably).
            let (old, new, new_ts) = {
                let store = ctx
                    .get_window_store::<K, V>(&self.store_name)
                    .expect("window store not found");
                store.set_record_context(rc.clone());
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
            if self.emit.is_on_update() {
                self.forwarder.maybe_forward_change(
                    ctx,
                    Windowed {
                        key: key.clone(),
                        window: Window {
                            start: ws,
                            end: ws + size,
                        },
                    },
                    Change::update(old, new),
                    new_ts,
                );
            }
        }

        if self.emit.is_on_close() {
            self.emit_closed_windows(ctx, window_close_time).await;
        }
    }
}

impl<K, V, R> KStreamWindowReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    R: Fn(&V, &V) -> V + Send + 'static,
{
    /// Forward each window whose `end <= window_close_time` and `end >
    /// last_emitted_close` as a final `Change`, ascending by window start, then
    /// advance the watermark.
    async fn emit_closed_windows(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        window_close_time: i64,
    ) {
        // The window bounds below are epoch-millisecond instants, so the size
        // and grace extents cross into that coordinate space here.
        let size = self.windows.size.millis_i64();
        // JVM closes a window strictly: emit once stream-time moves PAST the end
        // (`end < window_close_time`), so a zero-width / boundary window is not
        // finalized at its own stream-time. end = start + size < close ⟺ start <= close-size-1.
        let start_to = window_close_time - size - 1;
        let start_from = self.last_emitted_close.saturating_sub(size);
        let mut due = {
            let store = ctx
                .get_window_store::<K, V>(&self.store_name)
                .expect("window store not found");
            store.fetch_all_in_range(start_from, start_to).await
        };
        due.retain(|(_, ws, _, _)| ws + size >= self.last_emitted_close);
        due.sort_by_key(|(_, ws, _, _)| *ws);
        for (k, ws, ts, v) in due {
            ctx.forward(Record::new(
                Some(Windowed {
                    key: k,
                    window: Window {
                        start: ws,
                        end: ws + size,
                    },
                }),
                Change::update(None, v),
                ts,
            ));
        }
        self.last_emitted_close = window_close_time;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, marker::PhantomData};

    use super::*;
    use crate::{
        dsl::windows::{TimeWindows, Window, Windowed},
        processor::{
            api::ProcessorContext,
            erased::{Dispatch, ErasedRecord},
            record::{Record, RecordContext},
            serde::{I64Serde, StringSerde},
        },
        store::{registry::StoreRegistry, window::WindowBytesStore},
    };

    #[tokio::test]
    async fn windowed_count_tumbling_emits_per_window() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
            millis(10),
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
            windows: TimeWindows::of_size(millis(10)),
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            emit: crate::dsl::emit::EmitStrategy::on_window_update(),
            stream_time: i64::MIN,
            last_emitted_close: i64::MIN,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };

        macro_rules! drive {
            ($ts:expr) => {{
                let globals = crate::runtime::global::GlobalStateManager::default();
                let mut scheds = Vec::new();
                let mut d = Dispatch {
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
                let mut ctx =
                    ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
                proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), $ts))
                    .await;
            }};
        }

        // record at ts=3 → window [0,10), count 1
        drive!(3);
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        let key = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(key.window, Window { start: 0, end: 10 });
        assert_eq!(change.old, None);
        assert_eq!(change.new, Some(1));

        // record at ts=7 → same window [0,10), count 2
        drive!(7);
        let (_, rec2) = buffer.pop_front().unwrap();
        let change2 = rec2.value.downcast::<Change<i64>>().unwrap();
        assert_eq!(change2.old, Some(1));
        assert_eq!(change2.new, Some(2));

        // record at ts=12 → window [10,20), count 1
        drive!(12);
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

    #[tokio::test]
    async fn windowed_count_emit_final_emits_only_on_close() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
            millis(10),
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
            windows: TimeWindows::of_size(millis(10)),
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            emit: crate::dsl::emit::EmitStrategy::on_window_close(),
            stream_time: i64::MIN,
            last_emitted_close: i64::MIN,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };

        // helper to drive one record
        macro_rules! drive {
            ($ts:expr) => {{
                let globals = crate::runtime::global::GlobalStateManager::default();
                let mut scheds = Vec::new();
                let mut d = Dispatch {
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
                let mut ctx =
                    ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
                proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), $ts))
                    .await;
            }};
        }

        // ts=3 and ts=7 both in window [0,10) — emit-final does not emit while open.
        drive!(3);
        assert!(buffer.is_empty(), "no emit while window [0,10) is open");
        drive!(7);
        assert!(
            buffer.is_empty(),
            "still no emit while window [0,10) is open"
        );

        // ts=15 → window [10,20) opens, stream_time=15, window [0,10) closes
        // (end 10 <= close_time 15). Exactly one final record forwarded.
        drive!(15);

        assert_eq!(buffer.len(), 1, "exactly one final emit on close");
        let (_, rec) = buffer.pop_front().unwrap();
        let key = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(key.key, "a");
        assert_eq!(key.window, Window { start: 0, end: 10 });
        assert_eq!(rec.value.downcast::<Change<i64>>().unwrap().new, Some(2));
    }

    #[tokio::test]
    async fn windowed_reduce_emit_final_emits_only_on_close() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
            millis(10),
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

        // Reducer sums values; the first value in a window seeds the accumulator.
        let mut proc = KStreamWindowReduceProcessor {
            store_name: "w".into(),
            windows: TimeWindows::of_size(millis(10)),
            reducer: |a: &i64, b: &i64| a + b,
            emit: crate::dsl::emit::EmitStrategy::on_window_close(),
            stream_time: i64::MIN,
            last_emitted_close: i64::MIN,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData::<fn() -> (String, i64)>,
        };

        macro_rules! drive {
            ($v:expr, $ts:expr) => {{
                let globals = crate::runtime::global::GlobalStateManager::default();
                let mut scheds = Vec::new();
                let mut d = Dispatch {
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
                let mut ctx =
                    ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
                proc.process(&mut ctx, Record::new(Some("a".into()), $v, $ts))
                    .await;
            }};
        }

        // 4@ts=3 and 6@ts=7 both in window [0,10) → reduced value 10. Emit-final
        // does not emit while the window is open.
        drive!(4i64, 3);
        assert!(buffer.is_empty(), "no emit while window [0,10) is open");
        drive!(6i64, 7);
        assert!(
            buffer.is_empty(),
            "still no emit while window [0,10) is open"
        );

        // ts=15 → window [10,20) opens, stream_time=15, window [0,10) closes
        // (end 10 <= close_time 15). Exactly one final, carrying the reduced 10.
        drive!(99i64, 15);

        assert_eq!(buffer.len(), 1, "exactly one final emit on close");
        let (_, rec) = buffer.pop_front().unwrap();
        let key = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(key.key, "a");
        assert_eq!(key.window, Window { start: 0, end: 10 });
        let ch = rec.value.downcast::<Change<i64>>().unwrap();
        assert_eq!((ch.old, ch.new), (None, Some(10)));
    }

    // ── Record-cache suppression (sub-task 3d-ii) ─────────────────────────────

    /// A `w` window-store registry, optionally record-cached.
    fn window_registry(cached: bool) -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
            millis(10),
        )));
        if cached {
            stores.enable_cache(
                "w",
                std::sync::Arc::new(std::sync::Mutex::new(
                    crate::store::cache::named::NamedCache::new("w".into()),
                )),
            );
        }
        stores
    }

    /// Run `init` then two same-window+key records (count) through the windowed
    /// aggregate, returning how many records reached the downstream buffer.
    async fn run_two_same_window(stores: &mut StoreRegistry) -> usize {
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
            windows: TimeWindows::of_size(millis(10)),
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            emit: crate::dsl::emit::EmitStrategy::on_window_update(),
            stream_time: i64::MIN,
            last_emitted_close: i64::MIN,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };
        // ts=3 and ts=7 both fall into window [0,10) for key "a".
        for ts in [3i64, 7] {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
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
            if ts == 3 {
                proc.init(&mut ctx).await;
            }
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), ts))
                .await;
        }
        buffer.len()
    }

    /// Uncached → the windowed aggregate forwards each record immediately: two
    /// records into the same window → two forwards.
    #[tokio::test]
    async fn uncached_windowed_aggregate_forwards_each_record() {
        let mut stores = window_registry(false);
        assert_eq!(run_two_same_window(&mut stores).await, 2);
    }

    /// Cached → the immediate forwards are suppressed; the cache flush forwards
    /// exactly ONE deduped `Change` keyed by the correct `Windowed<String>` for
    /// window [0,10) with the final count 2.
    #[tokio::test]
    async fn cached_windowed_aggregate_suppresses_then_flushes_one() {
        let mut stores = window_registry(true);
        assert_eq!(
            run_two_same_window(&mut stores).await,
            0,
            "cached window store must suppress both immediate forwards"
        );
        // Flush the cache: exactly one deduped record, keyed by Windowed [0,10).
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        stores
            .get_mut("w")
            .unwrap()
            .flush_cache_into(&mut buffer, &[0])
            .await;
        assert_eq!(buffer.len(), 1, "flush emits exactly one deduped change");
        let (_, rec) = buffer.pop_front().unwrap();
        let key = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(key.key, "a");
        assert_eq!(key.window, Window { start: 0, end: 10 });
        let ch = rec.value.downcast::<Change<i64>>().unwrap();
        assert_eq!(ch.new, Some(2), "deduped to the latest window count");
    }
}
