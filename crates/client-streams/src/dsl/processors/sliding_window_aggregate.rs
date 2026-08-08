//! Sliding-window aggregation processor (KIP-450).
//!
//! The windows are inclusive and data-defined, with size `time_difference`.
//! This module ports the JVM `KStreamSlidingWindowAggregate`. It supports both
//! the emit-on-update forwarding strategy, which is the default, and the
//! emit-on-close strategy (KIP-825).
//!
//! ## Algorithm overview
//!
//! The JVM has three code paths depending on the record timestamp `t`:
//!
//! - **`processEarly`** (`t < W`): the record is placed into the *combined window*
//!   `[0, W]` that absorbs all early records. Right windows for prior records in
//!   `(t, W]` may also be created.
//! - **`processInOrder`** (`t >= W`): the canonical path.
//!   - Existing windows that straddle `t` are updated in place.
//!   - Left window `[t-W, t]` is found or created, seeded by the nearest prior
//!     window that ends before `t`.
//!   - Right window `[prev+1, prev+1+W]` is created for the most recent prior
//!     record if it does not already exist.
//!
//! **Emission gate (emit-on-update)**: a window is forwarded only when
//! `window_end >= window_close_time`, where
//! `window_close_time = stream_time - grace`. A window that falls entirely
//! before `window_close_time` is updated in the store but is not forwarded,
//! because it is already expired.
//!
//! **Emit-on-close (KIP-825)**: with `EmitStrategy::on_window_close()`, the
//! per-update forwards above are suppressed entirely. Each window is instead
//! forwarded exactly once, as a final `Change` with `old=None`, once
//! stream-time advances past its close (`window_end <= window_close_time`). The
//! store-update logic is identical in both modes. Only the forwarding
//! differs.
use std::marker::PhantomData;

use async_trait::async_trait;
use crabka_units::prelude::*;

use crate::{
    dsl::{
        processors::{change::Change, tuple_forwarder::TupleForwarder},
        windows::{SlidingWindows, Window, Windowed},
    },
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
    },
};

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Aggregate records into sliding windows (KIP-450), emit-on-update.
///
/// A record at time `t` belongs to every window `[ws, ws + W]` that contains
/// it. Windows are **data-defined** and not epoch-aligned. The left window has
/// `ws = t - W`, or `0` when `t < W`. The right window has `ws = t + 1`, and it
/// is created only when a later record already exists inside `[t+1, t+W]`. An
/// existing window whose range overlaps `t` is updated in place.
///
/// Stream-time tracks the maximum observed record timestamp. A record whose own
/// window (`[t, t+W]`) ends before `stream_time - grace` is dropped without a
/// message.
#[allow(dead_code)]
pub(crate) struct KStreamSlidingWindowAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub windows: SlidingWindows,
    pub init: I,
    pub agg: A,
    /// The maximum record timestamp observed by this task instance.
    pub stream_time: i64,
    /// Emit on every update, which is the default, or only on window close
    /// (KIP-825).
    pub emit: crate::dsl::emit::EmitStrategy,
    /// The highest `window_close_time` already emitted. It prevents a
    /// re-emit.
    pub last_emitted_close: i64,
    /// Forward-suppression seam. When the window store is record-cached, the
    /// per-update forwards are suppressed, because the cache flush forwards the
    /// deduped `Change`. `init` resolves this field. Only the emit-on-update
    /// path is wrapped, because an emit-final store is never cached.
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V, VA)>,
}

#[async_trait]
impl<K, V, VA, I, A> Processor<K, V, Windowed<K>, Change<VA>>
    for KStreamSlidingWindowAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + Sync + 'static,
    VA: std::any::Any + Send + Sync + Clone,
    I: Fn() -> VA + Send + Sync + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + Sync + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("sliding aggregate requires a non-null key");
        let w = self.windows.time_difference.millis_i64();
        let t = r.timestamp;
        self.stream_time = self.stream_time.max(t);
        // windowCloseTime = observedStreamTime - gracePeriodMs  (JVM naming)
        let close_time = self.stream_time - self.windows.grace.millis_i64();

        // Drop records whose own window ends before the close time.
        // JVM: `if (windowEnd < windowCloseTime) { return; }`
        // where windowEnd = t + W (the end of the record's left/combined window).
        let record_window_end = t + w;
        if record_window_end < close_time {
            return;
        }

        // Stash the source record context so a cached store stamps it on the
        // staged writes (`write_ctx` clones, not takes — so it persists across
        // every put this record performs).
        {
            let rc = ctx.record_context().clone();
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.set_record_context(rc);
        }

        if t < w {
            self.process_early(ctx, key, r.value, t, w, close_time)
                .await;
        } else {
            self.process_normal(ctx, key, r.value, t, w, close_time)
                .await;
        }

        if self.emit.is_on_close() {
            self.emit_closed_windows(ctx, close_time).await;
        }
    }
}

impl<K, V, VA, I, A> KStreamSlidingWindowAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + Sync + 'static,
    VA: std::any::Any + Send + Sync + Clone,
    I: Fn() -> VA + Send + Sync + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + Sync + 'static,
{
    /// JVM `processEarly`. It handles records with `t < W`.
    ///
    /// The combined window `[0, W]` absorbs all early records. Scan `[0, t+1]`.
    async fn process_early(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        key: K,
        value: V,
        t: i64,
        w: i64,
        close_time: i64,
    ) {
        // Scan [0, t+1] ascending.
        let found: Vec<(i64, i64, VA)> = {
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.fetch_with_ts(&key, 0, t + 1).await
        };

        // combined window has ws=0; right_win_agg = last straddling window in [1, t].
        let mut combined: Option<(i64, VA)> = None; // (stored_max_ts, agg)
        let mut right_win_agg: Option<(i64, VA)> = None; // (stored_max_ts, agg)
        let mut right_win_already_created = false;
        let mut previous_record_ts: Option<i64> = None;
        let mut window_start_times: Vec<i64> = Vec::new();

        // Clone the found list so we can iterate and mutate in separate passes.
        let straddle_updates: Vec<(i64, i64, VA)> = found
            .iter()
            .filter(|(ws, _, _)| *ws > 0 && *ws < t + 1)
            .cloned()
            .collect();

        for (ws, stored_ts, agg) in &found {
            window_start_times.push(*ws);
            if *ws == 0 {
                combined = Some((*stored_ts, agg.clone()));
                if *stored_ts < t {
                    previous_record_ts = Some(*stored_ts);
                }
            } else if *ws == t + 1 {
                right_win_already_created = true;
            } else {
                // ws in (0, t]: right-window region for early path.
                // right_win_agg = last seen (ascending ws → last = highest ws).
                right_win_agg = Some((*stored_ts, agg.clone()));
            }
        }

        // If no straddle found as right_win_agg but combined has ts > t, use it.
        if right_win_agg.is_none()
            && let Some((cts, ref cagg)) = combined
        {
            right_win_agg = (cts > t).then(|| (cts, cagg.clone()));
        }

        // Emit straddling windows (ascending ws order), gated on close_time.
        for (ws, stored_ts, agg) in straddle_updates {
            let we = ws + w;
            let new_agg = (self.agg)(&key, &value, agg);
            let new_ts = stored_ts.max(t);
            if we >= close_time {
                let old_agg = {
                    let store = ctx
                        .get_window_store::<K, VA>(&self.store_name)
                        .expect("window store not found");
                    let prior = store.fetch_single(&key, ws).await.map(|(_, v)| v);
                    store.put(key.clone(), ws, new_agg.clone(), new_ts).await;
                    prior
                };
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window { start: ws, end: we },
                        },
                        Change::update(old_agg, new_agg),
                        new_ts,
                    );
                }
            } else {
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                store.put(key.clone(), ws, new_agg, new_ts).await;
            }
        }

        // createWindows (JVM processEarly order: current right → prev right → combined)

        // Create current record's right window [t+1, t+1+W] if not already present.
        if !right_win_already_created
            && let Some((rts, ragg)) = right_win_agg.filter(|(rts, _)| *rts > t)
        {
            let rws = t + 1;
            let rwe = rws + w;
            if rwe >= close_time {
                {
                    let store = ctx
                        .get_window_store::<K, VA>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), rws, ragg.clone(), rts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: rws,
                                end: rwe,
                            },
                        },
                        Change::update(None, ragg),
                        rts,
                    );
                }
            }
        }

        // Create previous record's right window if needed.
        if let Some(prev_ts) = previous_record_ts {
            let pws = prev_ts + 1;
            let pwe = pws + w;
            if !window_start_times.contains(&pws) && pwe >= t {
                // JVM createPreviousRecordRightWindow: seed=init, update with current record.
                let new_agg = (self.agg)(&key, &value, (self.init)());
                let new_ts = t;
                if pwe >= close_time {
                    {
                        let store = ctx
                            .get_window_store::<K, VA>(&self.store_name)
                            .expect("window store not found");
                        store.put(key.clone(), pws, new_agg.clone(), new_ts).await;
                    }
                    if self.emit.is_on_update() {
                        self.forwarder.maybe_forward_change(
                            ctx,
                            Windowed {
                                key: key.clone(),
                                window: Window {
                                    start: pws,
                                    end: pwe,
                                },
                            },
                            Change::update(None, new_agg),
                            new_ts,
                        );
                    }
                }
            }
        }

        // Update (or create) the combined window [0, W].
        {
            let cws = 0i64;
            let cwe = w;
            if cwe >= close_time {
                let old_agg_opt = combined.as_ref().map(|(_, a)| a.clone());
                let seed = old_agg_opt.clone().unwrap_or_else(|| (self.init)());
                let new_agg = (self.agg)(&key, &value, seed);
                let new_ts = combined.as_ref().map_or(t, |(ts, _)| (*ts).max(t));
                {
                    let store = ctx
                        .get_window_store::<K, VA>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), cws, new_agg.clone(), new_ts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: cws,
                                end: cwe,
                            },
                        },
                        Change::update(old_agg_opt, new_agg),
                        new_ts,
                    );
                }
            }
        }
    }

    /// JVM `processInOrder`. It handles records with `t >= W`.
    ///
    /// Scan `[max(0, t-2W), t+1]` in ascending windowStart order.
    async fn process_normal(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        key: K,
        value: V,
        t: i64,
        w: i64,
        close_time: i64,
    ) {
        let scan_from = (t - 2 * w).max(0);
        let found: Vec<(i64, i64, VA)> = {
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.fetch_with_ts(&key, scan_from, t + 1).await
        };

        // JVM processInOrder variables (ascending iterator = forward scan):
        let mut left_win_agg: Option<VA> = None; // agg of window with we < t
        let mut right_win_agg: Option<(i64, VA)> = None; // first straddle (we > t)
        let mut left_win_already_created = false;
        let mut right_win_already_created = false;
        let mut previous_record_ts: Option<i64> = None;
        let mut window_start_times: Vec<i64> = Vec::new();

        // Collect straddle / left-window updates to emit in order.
        // We need to emit them from the scan, then create-windows after.
        let mut updates_to_emit: Vec<(i64, i64, VA, bool)> = Vec::new(); // (ws, new_ts, new_agg, gated)

        for (ws, stored_ts, agg) in &found {
            let we = ws + w;
            window_start_times.push(*ws);

            if we < t {
                // Before t: seed for new left window.
                left_win_agg = Some(agg.clone());
                previous_record_ts = Some(*stored_ts);
            } else if we == t {
                // This IS the left window [t-W, t].
                left_win_already_created = true;
                if *stored_ts < t {
                    previous_record_ts = Some(*stored_ts);
                }
                let new_agg = (self.agg)(&key, &value, agg.clone());
                let new_ts = (*stored_ts).max(t);
                updates_to_emit.push((*ws, new_ts, new_agg, we >= close_time));
            } else if we > t && *ws <= t {
                // Straddle.
                if right_win_agg.is_none() {
                    right_win_agg = Some((*stored_ts, agg.clone()));
                }
                let new_agg = (self.agg)(&key, &value, agg.clone());
                let new_ts = (*stored_ts).max(t);
                updates_to_emit.push((*ws, new_ts, new_agg, we >= close_time));
            } else if *ws == t + 1 {
                right_win_already_created = true;
            }
        }

        // Emit all straddle/left updates (ascending ws order, from scan).
        for (ws, new_ts, new_agg, emit) in updates_to_emit {
            let we = ws + w;
            if emit {
                let old_agg = {
                    let store = ctx
                        .get_window_store::<K, VA>(&self.store_name)
                        .expect("window store not found");
                    let prior = store.fetch_single(&key, ws).await.map(|(_, v)| v);
                    store.put(key.clone(), ws, new_agg.clone(), new_ts).await;
                    prior
                };
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window { start: ws, end: we },
                        },
                        Change::update(old_agg, new_agg),
                        new_ts,
                    );
                }
            } else {
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                store.put(key.clone(), ws, new_agg, new_ts).await;
            }
        }

        // createWindows (JVM order: prev right → left → current right).

        // 1. Previous record's right window.
        if let Some(prev_ts) = previous_record_ts {
            let pws = prev_ts + 1;
            let pwe = pws + w;
            if !window_start_times.contains(&pws) && pwe >= t {
                let new_agg = (self.agg)(&key, &value, (self.init)());
                let new_ts = t;
                if pwe >= close_time {
                    {
                        let store = ctx
                            .get_window_store::<K, VA>(&self.store_name)
                            .expect("window store not found");
                        store.put(key.clone(), pws, new_agg.clone(), new_ts).await;
                    }
                    if self.emit.is_on_update() {
                        self.forwarder.maybe_forward_change(
                            ctx,
                            Windowed {
                                key: key.clone(),
                                window: Window {
                                    start: pws,
                                    end: pwe,
                                },
                            },
                            Change::update(None, new_agg),
                            new_ts,
                        );
                    }
                }
            }
        }

        // 2. Left window [t-W, t].
        if !left_win_already_created {
            let lws = t - w;
            let lwe = t;
            let seed = if left_window_not_empty(previous_record_ts, t, w) {
                left_win_agg.clone().unwrap_or_else(|| (self.init)())
            } else {
                (self.init)()
            };
            let new_agg = (self.agg)(&key, &value, seed);
            let new_ts = t;
            if lwe >= close_time {
                {
                    let store = ctx
                        .get_window_store::<K, VA>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), lws, new_agg.clone(), new_ts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: lws,
                                end: lwe,
                            },
                        },
                        Change::update(None, new_agg),
                        new_ts,
                    );
                }
            } else {
                // Store but don't emit (expired).
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                store.put(key.clone(), lws, new_agg, new_ts).await;
            }
        }

        // 3. Current record's right window [t+1, t+1+W].
        if !right_win_already_created
            && let Some((rts, ragg)) = right_win_agg.filter(|(rts, _)| *rts > t)
        {
            let rws = t + 1;
            let rwe = rws + w;
            if rwe >= close_time {
                {
                    let store = ctx
                        .get_window_store::<K, VA>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), rws, ragg.clone(), rts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: rws,
                                end: rwe,
                            },
                        },
                        Change::update(None, ragg),
                        rts,
                    );
                }
            }
        }
    }

    /// Forward each window whose `end <= window_close_time` and `end >
    /// last_emitted_close` as a final `Change`, ascending by window start, then
    /// advance the watermark. Sliding windows have `end = start +
    /// time_difference`.
    async fn emit_closed_windows(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        window_close_time: i64,
    ) {
        let w = self.windows.time_difference.millis_i64();
        // Strict close (JVM): emit once stream-time moves PAST the end
        // (`end < window_close_time`). end = start + w < close ⟺ start <= close-w-1.
        let start_to = window_close_time - w - 1;
        // Unlike the time-window processor (which pre-filters already-closed
        // windows out of the store-update loop), the sliding store-update logic
        // runs for every window each record; the `retain` below on
        // `last_emitted_close` is therefore the SOLE re-emit dedup here.
        let start_from = self.last_emitted_close.saturating_sub(w);
        let mut due = {
            let store = ctx
                .get_window_store::<K, VA>(&self.store_name)
                .expect("window store not found");
            store.fetch_all_in_range(start_from, start_to).await
        };
        due.retain(|(_, ws, _, _)| ws + w >= self.last_emitted_close);
        due.sort_by_key(|(_, ws, _, _)| *ws);
        for (k, ws, ts, v) in due {
            ctx.forward(Record::new(
                Some(Windowed {
                    key: k,
                    window: Window {
                        start: ws,
                        end: ws + w,
                    },
                }),
                Change::update(None, v),
                ts,
            ));
        }
        self.last_emitted_close = window_close_time;
    }
}

/// JVM `leftWindowNotEmpty`. It returns true when the previous record falls
/// inside the new left window `[t-W, t]`, that is when `t - W <= prev_ts`.
fn left_window_not_empty(prev_ts: Option<i64>, t: i64, w: i64) -> bool {
    prev_ts.is_some_and(|p| t - w <= p)
}

/// Reduce records into sliding windows (KIP-450).
///
/// Like [`KStreamSlidingWindowAggregateProcessor`], but it uses the first value
/// in each window as the accumulator seed and has no separate `init`
/// function.
#[allow(dead_code)]
pub(crate) struct KStreamSlidingWindowReduceProcessor<K, V, R> {
    pub store_name: String,
    pub windows: SlidingWindows,
    pub reducer: R,
    pub stream_time: i64,
    /// Emit on every update, which is the default, or only on window close
    /// (KIP-825).
    pub emit: crate::dsl::emit::EmitStrategy,
    /// The highest `window_close_time` already emitted. It prevents a re-emit.
    pub last_emitted_close: i64,
    /// Forward-suppression seam. See
    /// [`KStreamSlidingWindowAggregateProcessor::forwarder`].
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, R> Processor<K, V, Windowed<K>, Change<V>>
    for KStreamSlidingWindowReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Sync + Clone,
    R: Fn(&V, &V) -> V + Send + Sync + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("sliding reduce requires a non-null key");
        let w = self.windows.time_difference.millis_i64();
        let t = r.timestamp;
        self.stream_time = self.stream_time.max(t);
        let close_time = self.stream_time - self.windows.grace.millis_i64();

        let record_window_end = t + w;
        if record_window_end < close_time {
            return;
        }

        // Stash the source record context so a cached store stamps it on the
        // staged writes (persists across every put this record performs).
        {
            let rc = ctx.record_context().clone();
            let store = ctx
                .get_window_store::<K, V>(&self.store_name)
                .expect("window store not found");
            store.set_record_context(rc);
        }

        if t < w {
            self.process_early(ctx, key, r.value, t, w, close_time)
                .await;
        } else {
            self.process_normal(ctx, key, r.value, t, w, close_time)
                .await;
        }

        if self.emit.is_on_close() {
            self.emit_closed_windows(ctx, close_time).await;
        }
    }
}

impl<K, V, R> KStreamSlidingWindowReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Sync + Clone,
    R: Fn(&V, &V) -> V + Send + Sync + 'static,
{
    async fn process_early(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        key: K,
        value: V,
        t: i64,
        w: i64,
        close_time: i64,
    ) {
        let found: Vec<(i64, i64, V)> = {
            let store = ctx
                .get_window_store::<K, V>(&self.store_name)
                .expect("window store not found");
            store.fetch_with_ts(&key, 0, t + 1).await
        };

        let mut combined: Option<(i64, V)> = None;
        let mut right_win_agg: Option<(i64, V)> = None;
        let mut right_win_already_created = false;
        let mut previous_record_ts: Option<i64> = None;
        let mut window_start_times: Vec<i64> = Vec::new();

        let straddle_updates: Vec<(i64, i64, V)> = found
            .iter()
            .filter(|(ws, _, _)| *ws > 0 && *ws < t + 1)
            .cloned()
            .collect();

        for (ws, stored_ts, agg) in &found {
            window_start_times.push(*ws);
            if *ws == 0 {
                combined = Some((*stored_ts, agg.clone()));
                if *stored_ts < t {
                    previous_record_ts = Some(*stored_ts);
                }
            } else if *ws == t + 1 {
                right_win_already_created = true;
            } else {
                right_win_agg = Some((*stored_ts, agg.clone()));
            }
        }

        if right_win_agg.is_none()
            && let Some((cts, ref cagg)) = combined
        {
            right_win_agg = (cts > t).then(|| (cts, cagg.clone()));
        }

        // Straddle updates.
        for (ws, stored_ts, agg) in straddle_updates {
            let we = ws + w;
            let new_agg = (self.reducer)(&agg, &value);
            let new_ts = stored_ts.max(t);
            if we >= close_time {
                let old_agg = {
                    let store = ctx
                        .get_window_store::<K, V>(&self.store_name)
                        .expect("window store not found");
                    let prior = store.fetch_single(&key, ws).await.map(|(_, v)| v);
                    store.put(key.clone(), ws, new_agg.clone(), new_ts).await;
                    prior
                };
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window { start: ws, end: we },
                        },
                        Change::update(old_agg, new_agg),
                        new_ts,
                    );
                }
            } else {
                let store = ctx
                    .get_window_store::<K, V>(&self.store_name)
                    .expect("window store not found");
                store.put(key.clone(), ws, new_agg, new_ts).await;
            }
        }

        // createWindows (JVM processEarly order: current right → prev right → combined)

        // Current record's right window.
        if !right_win_already_created
            && let Some((rts, ragg)) = right_win_agg.filter(|(rts, _)| *rts > t)
        {
            let rws = t + 1;
            let rwe = rws + w;
            if rwe >= close_time {
                {
                    let store = ctx
                        .get_window_store::<K, V>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), rws, ragg.clone(), rts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: rws,
                                end: rwe,
                            },
                        },
                        Change::update(None, ragg),
                        rts,
                    );
                }
            }
        }

        // Previous record's right window.
        if let Some(prev_ts) = previous_record_ts {
            let pws = prev_ts + 1;
            let pwe = pws + w;
            if !window_start_times.contains(&pws) && pwe >= t {
                let new_agg = value.clone();
                let new_ts = t;
                if pwe >= close_time {
                    {
                        let store = ctx
                            .get_window_store::<K, V>(&self.store_name)
                            .expect("window store not found");
                        store.put(key.clone(), pws, new_agg.clone(), new_ts).await;
                    }
                    if self.emit.is_on_update() {
                        self.forwarder.maybe_forward_change(
                            ctx,
                            Windowed {
                                key: key.clone(),
                                window: Window {
                                    start: pws,
                                    end: pwe,
                                },
                            },
                            Change::update(None, new_agg),
                            new_ts,
                        );
                    }
                }
            }
        }

        // Combined window [0, W].
        {
            let cws = 0i64;
            let cwe = w;
            if cwe >= close_time {
                let old_agg = combined.as_ref().map(|(_, a)| a.clone());
                let new_agg = if let Some(ref oa) = old_agg {
                    (self.reducer)(oa, &value)
                } else {
                    value.clone()
                };
                let new_ts = combined.as_ref().map_or(t, |(ts, _)| (*ts).max(t));
                {
                    let store = ctx
                        .get_window_store::<K, V>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), cws, new_agg.clone(), new_ts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: cws,
                                end: cwe,
                            },
                        },
                        Change::update(old_agg, new_agg),
                        new_ts,
                    );
                }
            }
        }
    }

    async fn process_normal(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        key: K,
        value: V,
        t: i64,
        w: i64,
        close_time: i64,
    ) {
        let scan_from = (t - 2 * w).max(0);
        let found: Vec<(i64, i64, V)> = {
            let store = ctx
                .get_window_store::<K, V>(&self.store_name)
                .expect("window store not found");
            store.fetch_with_ts(&key, scan_from, t + 1).await
        };

        let mut left_win_agg: Option<V> = None;
        let mut right_win_agg: Option<(i64, V)> = None;
        let mut left_win_already_created = false;
        let mut right_win_already_created = false;
        let mut previous_record_ts: Option<i64> = None;
        let mut window_start_times: Vec<i64> = Vec::new();
        let mut updates_to_emit: Vec<(i64, i64, V, bool)> = Vec::new();

        for (ws, stored_ts, agg) in &found {
            let we = ws + w;
            window_start_times.push(*ws);

            if we < t {
                left_win_agg = Some(agg.clone());
                previous_record_ts = Some(*stored_ts);
            } else if we == t {
                left_win_already_created = true;
                if *stored_ts < t {
                    previous_record_ts = Some(*stored_ts);
                }
                let new_agg = (self.reducer)(agg, &value);
                let new_ts = (*stored_ts).max(t);
                updates_to_emit.push((*ws, new_ts, new_agg, we >= close_time));
            } else if we > t && *ws <= t {
                if right_win_agg.is_none() {
                    right_win_agg = Some((*stored_ts, agg.clone()));
                }
                let new_agg = (self.reducer)(agg, &value);
                let new_ts = (*stored_ts).max(t);
                updates_to_emit.push((*ws, new_ts, new_agg, we >= close_time));
            } else if *ws == t + 1 {
                right_win_already_created = true;
            }
        }

        for (ws, new_ts, new_agg, emit) in updates_to_emit {
            let we = ws + w;
            if emit {
                let old_agg = {
                    let store = ctx
                        .get_window_store::<K, V>(&self.store_name)
                        .expect("window store not found");
                    let prior = store.fetch_single(&key, ws).await.map(|(_, v)| v);
                    store.put(key.clone(), ws, new_agg.clone(), new_ts).await;
                    prior
                };
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window { start: ws, end: we },
                        },
                        Change::update(old_agg, new_agg),
                        new_ts,
                    );
                }
            } else {
                let store = ctx
                    .get_window_store::<K, V>(&self.store_name)
                    .expect("window store not found");
                store.put(key.clone(), ws, new_agg, new_ts).await;
            }
        }

        // 1. Previous record's right window.
        if let Some(prev_ts) = previous_record_ts {
            let pws = prev_ts + 1;
            let pwe = pws + w;
            if !window_start_times.contains(&pws) && pwe >= t {
                let new_agg = value.clone();
                let new_ts = t;
                if pwe >= close_time {
                    {
                        let store = ctx
                            .get_window_store::<K, V>(&self.store_name)
                            .expect("window store not found");
                        store.put(key.clone(), pws, new_agg.clone(), new_ts).await;
                    }
                    if self.emit.is_on_update() {
                        self.forwarder.maybe_forward_change(
                            ctx,
                            Windowed {
                                key: key.clone(),
                                window: Window {
                                    start: pws,
                                    end: pwe,
                                },
                            },
                            Change::update(None, new_agg),
                            new_ts,
                        );
                    }
                }
            }
        }

        // 2. Left window [t-W, t].
        if !left_win_already_created {
            let lws = t - w;
            let lwe = t;
            let new_agg = if left_window_not_empty(previous_record_ts, t, w) {
                // seed from left_win_agg (records before left window boundary are in it).
                (self.reducer)(left_win_agg.as_ref().unwrap_or(&value), &value)
            } else {
                value.clone()
            };
            let new_ts = t;
            if lwe >= close_time {
                {
                    let store = ctx
                        .get_window_store::<K, V>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), lws, new_agg.clone(), new_ts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: lws,
                                end: lwe,
                            },
                        },
                        Change::update(None, new_agg),
                        new_ts,
                    );
                }
            } else {
                let store = ctx
                    .get_window_store::<K, V>(&self.store_name)
                    .expect("window store not found");
                store.put(key.clone(), lws, new_agg, new_ts).await;
            }
        }

        // 3. Current record's right window.
        if !right_win_already_created
            && let Some((rts, ragg)) = right_win_agg.filter(|(rts, _)| *rts > t)
        {
            let rws = t + 1;
            let rwe = rws + w;
            if rwe >= close_time {
                {
                    let store = ctx
                        .get_window_store::<K, V>(&self.store_name)
                        .expect("window store not found");
                    store.put(key.clone(), rws, ragg.clone(), rts).await;
                }
                if self.emit.is_on_update() {
                    self.forwarder.maybe_forward_change(
                        ctx,
                        Windowed {
                            key: key.clone(),
                            window: Window {
                                start: rws,
                                end: rwe,
                            },
                        },
                        Change::update(None, ragg),
                        rts,
                    );
                }
            }
        }
    }

    /// Forward each window whose `end <= window_close_time` and `end >
    /// last_emitted_close` as a final `Change`, ascending by window start, then
    /// advance the watermark. Sliding windows have `end = start +
    /// time_difference`.
    async fn emit_closed_windows(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        window_close_time: i64,
    ) {
        let w = self.windows.time_difference.millis_i64();
        // Strict close (JVM): emit once stream-time moves PAST the end
        // (`end < window_close_time`). end = start + w < close ⟺ start <= close-w-1.
        let start_to = window_close_time - w - 1;
        // Unlike the time-window processor (which pre-filters already-closed
        // windows out of the store-update loop), the sliding store-update logic
        // runs for every window each record; the `retain` below on
        // `last_emitted_close` is therefore the SOLE re-emit dedup here.
        let start_from = self.last_emitted_close.saturating_sub(w);
        let mut due = {
            let store = ctx
                .get_window_store::<K, V>(&self.store_name)
                .expect("window store not found");
            store.fetch_all_in_range(start_from, start_to).await
        };
        due.retain(|(_, ws, _, _)| ws + w >= self.last_emitted_close);
        due.sort_by_key(|(_, ws, _, _)| *ws);
        for (k, ws, ts, v) in due {
            ctx.forward(Record::new(
                Some(Windowed {
                    key: k,
                    window: Window {
                        start: ws,
                        end: ws + w,
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
        processor::{
            erased::{Dispatch, ErasedRecord},
            record::{Record, RecordContext},
            serde::{I64Serde, StringSerde},
        },
        runtime::global::GlobalStateManager,
        store::{registry::StoreRegistry, window::WindowBytesStore},
    };

    type CountProcessor = KStreamSlidingWindowAggregateProcessor<
        String,
        String,
        i64,
        fn() -> i64,
        fn(&String, &String, i64) -> i64,
    >;
    type WindowChanges = Vec<(Window, Option<i64>, Option<i64>)>;

    async fn run(
        proc: &mut CountProcessor,
        stores: &mut StoreRegistry,
        key: &str,
        ts: i64,
    ) -> WindowChanges {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: ts,
        };
        let globals = GlobalStateManager::default();
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
            millis(10),
        )));
        s
    }

    fn count_proc() -> CountProcessor {
        KStreamSlidingWindowAggregateProcessor {
            store_name: "w".into(),
            windows: SlidingWindows::of_time_difference_with_no_grace(millis(10)),
            init: (|| 0i64) as fn() -> i64,
            agg: (|_k: &String, _v: &String, a: i64| a + 1) as fn(&String, &String, i64) -> i64,
            stream_time: i64::MIN,
            emit: crate::dsl::emit::EmitStrategy::on_window_update(),
            last_emitted_close: i64::MIN,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        }
    }

    /// The first record at t=20, which is `>= W=10`, uses `process_normal`. It
    /// creates the left window `[10,20]` with count=1: no prior record means an
    /// empty left window, so init=0 and +1 gives 1.
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

    /// The second record at t=25 creates the left window `[15,25]`. The prior
    /// record at t=20 is in `[15,25]`, because `25-10=15 <= 20`, so
    /// `leftWindowNotEmpty` is true and the seed comes from `[10,20].agg=1`,
    /// which gives count=2.
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

    /// A record at `t=3` with `W=10`, so `t < W`, drives `process_early`. The
    /// combined window `[0, 10]` is created with count=1.
    #[tokio::test]
    async fn early_window_combined_for_t_below_w() {
        let mut stores = store();
        let mut p = count_proc();
        let out = run(&mut p, &mut stores, "a", 3).await;
        // process_early: combined window [0, W=10] is created/updated.
        assert!(
            out.contains(&(Window { start: 0, end: 10 }, None, Some(1))),
            "expected combined window [0,10]=1 for early t=3, got {out:?}"
        );
        // No left window for early records (ws = t-W = -7 < 0, not created).
        assert!(
            !out.iter().any(|(w, _, _)| w.start < 0),
            "unexpected negative-start window in early path: {out:?}"
        );
    }

    /// A second EARLY record (`t=6 < W=10`) after a first early record (`t=3`)
    /// drives the straddle branch and the right-window branch of
    /// `process_early`. The combined window `[0,10]` folds in the new record, so
    /// the count goes from 1 to 2. The previous record's right window
    /// `[t_prev+1, ..]`, which is `[4,14]`, is created, seeded from init, and
    /// then aggregated, which gives count 1.
    #[tokio::test]
    async fn early_path_second_record_updates_combined_and_prev_right_window() {
        let mut stores = store();
        let mut p = count_proc();
        let _ = run(&mut p, &mut stores, "a", 3).await;
        let out = run(&mut p, &mut stores, "a", 6).await;

        // Combined window [0,10] now counts both records.
        assert!(
            out.contains(&(Window { start: 0, end: 10 }, Some(1), Some(2))),
            "expected combined [0,10] old=1 new=2, got {out:?}"
        );
        // Previous record (t=3) right window [4, 14] created with count=1.
        assert!(
            out.contains(&(Window { start: 4, end: 14 }, None, Some(1))),
            "expected previous-record right window [4,14]=1, got {out:?}"
        );
    }

    /// Build a count processor in emit-on-close mode with a large grace. A
    /// freshly-created left window ends exactly at stream-time, so it stays open
    /// until a far-future record forces it closed.
    fn count_proc_close() -> CountProcessor {
        let mut p = count_proc();
        p.windows = SlidingWindows::of_time_difference_and_grace(millis(10), millis(100));
        p.emit = crate::dsl::emit::EmitStrategy::on_window_close();
        p
    }

    /// Emit-on-close (KIP-825): no window is forwarded while it is still open.
    /// Once stream-time advances past a window's close, that window is forwarded
    /// exactly once as a final `Change { old: None, new: Some(count) }`. The set
    /// of forwarded windows must match exactly the store's windows whose `end`
    /// has closed, with no duplicates.
    #[tokio::test]
    async fn sliding_count_emit_final_emits_only_on_close() {
        let mut stores = store();
        let mut p = count_proc_close();

        // Two records in overlapping sliding windows. grace=100 keeps every
        // window open (close_time = stream_time - 100 is far below any window
        // end), so emit-on-close must forward NOTHING here.
        let out1 = run(&mut p, &mut stores, "a", 10).await;
        assert!(
            out1.is_empty(),
            "emit-on-close must not forward on update (ts=10), got {out1:?}"
        );
        let out2 = run(&mut p, &mut stores, "a", 12).await;
        assert!(
            out2.is_empty(),
            "emit-on-close must not forward on update (ts=12), got {out2:?}"
        );

        // Snapshot which windows are present in the store and have closed by the
        // time stream-time jumps to 1000 (close_time = 1000 - grace(100) = 900,
        // so windows with end <= 900 close).
        let w = 10i64;
        let close_time = 900i64;
        let expected: std::collections::HashMap<i64, i64> = {
            let s = stores.get_window::<String, i64>("w").unwrap();
            s.fetch_all_in_range(i64::MIN / 2, close_time - w)
                .await
                .into_iter()
                .filter(|(_, ws, _, _)| ws + w <= close_time)
                .map(|(_, ws, _, v)| (ws, v))
                .collect()
        };
        assert!(
            !expected.is_empty(),
            "test setup: expected some closed windows in the store"
        );

        // Far-future record closes all the earlier windows.
        let out3 = run(&mut p, &mut stores, "a", 1000).await;

        // Collect final emissions for windows that should have closed (end <=
        // close_time). The far-future record also creates a fresh window at
        // start=990 (end=1000) which is itself due, plus possibly others; we only
        // assert about the windows we snapshotted as closed pre-jump.
        let mut emitted: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for (win, old, new) in &out3 {
            assert_eq!(
                *old, None,
                "final emission must have old=None, got {out3:?}"
            );
            assert_eq!(
                win.end,
                win.start + w,
                "sliding window end must be start + W, got {out3:?}"
            );
            if win.end <= close_time {
                let prev = emitted.insert(win.start, new.expect("final has Some value"));
                assert!(
                    prev.is_none(),
                    "window start={} emitted twice as final, got {out3:?}",
                    win.start
                );
            }
        }

        // Every snapshotted closed window must have been emitted with its stored
        // aggregate value.
        for (ws, v) in &expected {
            assert_eq!(
                emitted.get(ws),
                Some(v),
                "window start={ws} expected final value {v}, emitted map {emitted:?}, raw {out3:?}"
            );
        }
        assert!(
            !emitted.is_empty(),
            "expected at least one final emission after close, got {out3:?}"
        );
    }

    // ── Reduce processor unit tests ─────────────────────────────────────────

    /// Helper that builds a `StoreRegistry` holding a
    /// `WindowBytesStore<String, String>` for the reduce processor tests.
    fn str_store() -> StoreRegistry {
        let mut s = StoreRegistry::default();
        s.insert(Box::new(WindowBytesStore::<String, String>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "app-w-changelog".into(),
            millis(10),
        )));
        s
    }

    fn reduce_proc()
    -> KStreamSlidingWindowReduceProcessor<String, String, fn(&String, &String) -> String> {
        KStreamSlidingWindowReduceProcessor {
            store_name: "w".into(),
            windows: SlidingWindows::of_time_difference_with_no_grace(millis(10)),
            reducer: (|a: &String, v: &String| format!("{a}|{v}"))
                as fn(&String, &String) -> String,
            stream_time: i64::MIN,
            emit: crate::dsl::emit::EmitStrategy::on_window_update(),
            last_emitted_close: i64::MIN,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        }
    }

    /// Helper that runs one record through the reduce processor and returns the
    /// raw output as `Vec<(Window, Option<String>, Option<String>)>`.
    async fn run_reduce(
        proc: &mut KStreamSlidingWindowReduceProcessor<
            String,
            String,
            fn(&String, &String) -> String,
        >,
        stores: &mut StoreRegistry,
        key: &str,
        value: &str,
        ts: i64,
    ) -> Vec<(Window, Option<String>, Option<String>)> {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: ts,
        };
        let globals = GlobalStateManager::default();
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<String>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(key.into()), value.into(), ts))
                .await;
        }
        buffer
            .into_iter()
            .map(|(_, rec)| {
                let k = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
                let c = rec.value.downcast::<Change<String>>().unwrap();
                (k.window, c.old, c.new)
            })
            .collect()
    }

    /// The first record at t=20 seeds the left window `[10,20]` with the value
    /// itself, because there is no prior record, so the seed is
    /// `value.clone()`.
    #[tokio::test]
    async fn reduce_first_record_seeds_left_window() {
        let mut stores = str_store();
        let mut p = reduce_proc();
        let out = run_reduce(&mut p, &mut stores, "a", "v", 20).await;
        assert!(
            out.contains(&(Window { start: 10, end: 20 }, None, Some("v".into()))),
            "expected left window [10,20]=\"v\", got {out:?}"
        );
    }

    /// The second record at t=25 reduces the left window `[15,25]`. The prior
    /// record at t=20 falls inside `[15,25]`, because `25-10=15 <= 20`, so the
    /// seed is the existing `[10,20]` aggregate "v" and the fold gives "v|v".
    #[tokio::test]
    async fn reduce_second_record_folds_into_new_left_window() {
        let mut stores = str_store();
        let mut p = reduce_proc();
        let _ = run_reduce(&mut p, &mut stores, "a", "v", 20).await;
        let out = run_reduce(&mut p, &mut stores, "a", "v", 25).await;
        assert!(
            out.contains(&(Window { start: 15, end: 25 }, None, Some("v|v".into()))),
            "expected left window [15,25]=\"v|v\", got {out:?}"
        );
    }

    /// The first reduce record at `t=3`, where `t < W=10`, drives the reduce
    /// `process_early` path. The combined window `[0, W]` is seeded with the
    /// value itself.
    #[tokio::test]
    async fn reduce_early_record_seeds_combined_window() {
        let mut stores = str_store();
        let mut p = reduce_proc();
        let out = run_reduce(&mut p, &mut stores, "a", "v", 3).await;
        assert!(
            out.contains(&(Window { start: 0, end: 10 }, None, Some("v".into()))),
            "expected combined [0,10]=\"v\" for early t=3, got {out:?}"
        );
        assert!(
            !out.iter().any(|(w, _, _)| w.start < 0),
            "no negative-start window in the early path: {out:?}"
        );
    }

    /// A second EARLY reduce record (`t=6`) folds into the combined window
    /// `[0,10]`, so `"v"` becomes `"v|v"`. It also creates the previous record's
    /// right window `[4,14]`, seeded from the current value.
    #[tokio::test]
    async fn reduce_early_path_second_record_updates_combined_and_prev_right_window() {
        let mut stores = str_store();
        let mut p = reduce_proc();
        let _ = run_reduce(&mut p, &mut stores, "a", "v", 3).await;
        let out = run_reduce(&mut p, &mut stores, "a", "v", 6).await;

        assert!(
            out.contains(&(
                Window { start: 0, end: 10 },
                Some("v".into()),
                Some("v|v".into())
            )),
            "expected combined [0,10] old=\"v\" new=\"v|v\", got {out:?}"
        );
        // Previous record (t=3) right window [4,14] seeded with the current value.
        assert!(
            out.contains(&(Window { start: 4, end: 14 }, None, Some("v".into()))),
            "expected previous-record right window [4,14]=\"v\", got {out:?}"
        );
    }

    /// The emit-on-close reduce variant of
    /// `sliding_count_emit_final_emits_only_on_close`.
    ///
    /// The test builds a reduce processor with grace=100, so every window stays
    /// open while the records arrive, and it snapshots the store's closed
    /// windows. A far-future record then closes those windows, and each one is
    /// forwarded exactly once as a final
    /// `Change { old: None, new: Some(reduced) }` that carries the stored
    /// value.
    fn reduce_proc_close()
    -> KStreamSlidingWindowReduceProcessor<String, String, fn(&String, &String) -> String> {
        let mut p = reduce_proc();
        p.windows = SlidingWindows::of_time_difference_and_grace(millis(10), millis(100));
        p.emit = crate::dsl::emit::EmitStrategy::on_window_close();
        p
    }

    #[tokio::test]
    async fn sliding_reduce_emit_final_emits_only_on_close() {
        let mut stores = str_store();
        let mut p = reduce_proc_close();

        // grace=100 keeps every window open (close_time = stream_time - 100 is far
        // below any window end), so emit-on-close must forward NOTHING here.
        let out1 = run_reduce(&mut p, &mut stores, "a", "p", 10).await;
        assert!(
            out1.is_empty(),
            "emit-on-close must not forward on update (ts=10), got {out1:?}"
        );
        let out2 = run_reduce(&mut p, &mut stores, "a", "q", 12).await;
        assert!(
            out2.is_empty(),
            "emit-on-close must not forward on update (ts=12), got {out2:?}"
        );

        // Snapshot the windows that will have closed once stream-time jumps to
        // 1000 (close_time = 1000 - grace(100) = 900; windows with end <= 900).
        let w = 10i64;
        let close_time = 900i64;
        let expected: std::collections::HashMap<i64, String> = {
            let s = stores.get_window::<String, String>("w").unwrap();
            s.fetch_all_in_range(i64::MIN / 2, close_time - w)
                .await
                .into_iter()
                .filter(|(_, ws, _, _)| ws + w <= close_time)
                .map(|(_, ws, _, v)| (ws, v))
                .collect()
        };
        assert!(
            !expected.is_empty(),
            "test setup: expected some closed windows in the store"
        );

        // Far-future record closes all the earlier windows.
        let out3 = run_reduce(&mut p, &mut stores, "a", "r", 1000).await;

        let mut emitted: std::collections::HashMap<i64, String> = std::collections::HashMap::new();
        for (win, old, new) in &out3 {
            assert_eq!(
                *old, None,
                "final emission must have old=None, got {out3:?}"
            );
            assert_eq!(
                win.end,
                win.start + w,
                "sliding window end must be start + W, got {out3:?}"
            );
            if win.end <= close_time {
                let prev = emitted.insert(win.start, new.clone().expect("final has Some value"));
                assert!(
                    prev.is_none(),
                    "window start={} emitted twice as final, got {out3:?}",
                    win.start
                );
            }
        }

        // Every snapshotted closed window must have been emitted with its stored
        // reduced value.
        for (ws, v) in &expected {
            assert_eq!(
                emitted.get(ws),
                Some(v),
                "window start={ws} expected final value {v:?}, emitted map {emitted:?}, raw {out3:?}"
            );
        }
        assert!(
            !emitted.is_empty(),
            "expected at least one final emission after close, got {out3:?}"
        );
    }
}
