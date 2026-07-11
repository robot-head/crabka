//! `KTableSuppressProcessor` — KIP suppression. Buffers `Change` updates and emits
//! each buffered record once stream-time passes `buffer_time(record) + wait_ms`.
//! Unifies `untilWindowCloses` (`buffer_time` = window.end, wait = grace) and
//! `untilTimeLimitElapsed` (`buffer_time` = record ts, wait = the duration) behind a
//! `fn(&K, i64) -> i64` buffer-time pointer.
//!
//! The pending-change buffer is NOT owned by the processor: it lives in a
//! registered, changelog-backed [`SuppressBytesStore`](crate::store::suppress_store)
//! reached per-record via [`ProcessorContext::get_suppress_store`]. The processor is
//! pure control flow over that store.
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{
    dsl::processors::change::Change,
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
    },
    store::suppress_bufval::SuppressRecordCtx,
};

type Marker<T> = PhantomData<fn() -> T>;

pub(crate) struct KTableSuppressProcessor<K, V> {
    pub store_name: String,
    pub observed_stream_time: i64,
    pub wait_ms: i64,
    pub buffer_time: fn(&K, i64) -> i64,
    pub max_records: Option<usize>,
    pub max_bytes: Option<usize>,
    pub emit_early: bool,
    pub _pd: Marker<(K, V)>,
}

impl<K, V> KTableSuppressProcessor<K, V> {
    pub(crate) fn new(
        store_name: String,
        wait_ms: i64,
        buffer_time: fn(&K, i64) -> i64,
        max_records: Option<usize>,
        max_bytes: Option<usize>,
        emit_early: bool,
    ) -> Self {
        Self {
            store_name,
            observed_stream_time: i64::MIN,
            wait_ms,
            buffer_time,
            max_records,
            max_bytes,
            emit_early,
            _pd: PhantomData,
        }
    }
}

/// The buffer is "full" if it exceeds either configured cap (records or bytes).
fn over_caps(
    len: usize,
    byte_size: usize,
    max_records: Option<usize>,
    max_bytes: Option<usize>,
) -> bool {
    max_records.is_some_and(|c| len > c) || max_bytes.is_some_and(|c| byte_size > c)
}

#[async_trait]
impl<K, V> Processor<K, Change<V>, K, Change<V>> for KTableSuppressProcessor<K, V>
where
    // `K: Clone` + `V: Clone` are required by `ProcessorContext::forward`
    // (`KOut: Any + Send + Clone`, `VOut = Change<V>: Any + Send + Clone`); `Sync`
    // is required by `get_suppress_store::<K, V>` (`K: Send + Sync + 'static`).
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<V>>,
        r: Record<K, Change<V>>,
    ) {
        let key = r.key.expect("suppress requires a non-null key");
        self.observed_stream_time = self.observed_stream_time.max(r.timestamp);
        let bt = (self.buffer_time)(&key, r.timestamp);

        // Snapshot the source record context for the changelog VALUE (drop the
        // immutable record_context borrow before the mutable store borrow).
        let rec_ctx = {
            let rc = ctx.record_context();
            SuppressRecordCtx {
                topic: rc.topic.clone(),
                partition: rc.partition,
                offset: rc.offset,
                timestamp: rc.timestamp,
            }
        };

        // Buffer (replace-by-key) the pending change.
        {
            let store = ctx
                .get_suppress_store::<K, V>(&self.store_name)
                .expect("suppress store not found");
            store.put(key, bt, r.value, rec_ctx).await;
        }

        // Emit every entry whose buffer_time has elapsed (stream_time - wait_ms).
        let threshold = self.observed_stream_time - self.wait_ms;
        let due = {
            let store = ctx
                .get_suppress_store::<K, V>(&self.store_name)
                .expect("suppress store not found");
            store.evict_while(threshold).await
        };
        for (k, change, rts) in due {
            ctx.forward(Record::new(Some(k), change, rts));
        }

        // Overflow: maxRecords and/or maxBytes.
        if self.max_records.is_some() || self.max_bytes.is_some() {
            if self.emit_early {
                // emitEarlyWhenFull: evict + emit the oldest until back within caps.
                loop {
                    let evicted = {
                        let store = ctx
                            .get_suppress_store::<K, V>(&self.store_name)
                            .expect("suppress store not found");
                        if over_caps(
                            store.len(),
                            store.byte_size(),
                            self.max_records,
                            self.max_bytes,
                        ) {
                            store.evict_oldest().await
                        } else {
                            None
                        }
                    };
                    match evicted {
                        Some((k, change, rts)) => ctx.forward(Record::new(Some(k), change, rts)),
                        None => break,
                    }
                }
            } else {
                // shutDownWhenFull (strict): fatal if over either cap.
                let store = ctx
                    .get_suppress_store::<K, V>(&self.store_name)
                    .expect("suppress store not found");
                if let Some(cap) = self.max_records {
                    assert!(
                        store.len() <= cap,
                        "suppress buffer exceeded its max capacity of {cap} records (shutDownWhenFull)"
                    );
                }
                if let Some(cap) = self.max_bytes {
                    assert!(
                        store.byte_size() <= cap,
                        "suppress buffer exceeded its max capacity of {cap} bytes (shutDownWhenFull)"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::{
        dsl::windows::{TimeWindowedSerde, Window, Windowed},
        processor::{
            api::ProcessorContext,
            erased::{Dispatch, ErasedRecord},
            record::{Record, RecordContext},
            serde::{I64Serde, StringSerde},
        },
        store::{registry::StoreRegistry, suppress_store::SuppressBytesStore},
    };

    fn windowed(key: &str, start: i64, end: i64) -> Windowed<String> {
        Windowed {
            key: key.into(),
            window: Window { start, end },
        }
    }

    /// Seed a `Windowed<String> -> i64` suppress store named "sup" (size 10 matches
    /// every test window → the serde round-trips `Window{start,end}`).
    fn seed_windowed_store(stores: &mut StoreRegistry) {
        stores.insert(Box::new(
            SuppressBytesStore::<Windowed<String>, i64>::in_memory(
                "sup".into(),
                Box::new(TimeWindowedSerde::new(StringSerde, 10)),
                Box::new(I64Serde),
                "app-sup-changelog".into(),
            ),
        ));
    }

    /// Construct a window-close processor (`buffer_time` = window.end, strict).
    fn window_close_proc(
        grace_ms: i64,
        max_records: Option<usize>,
    ) -> KTableSuppressProcessor<Windowed<String>, i64> {
        KTableSuppressProcessor::new(
            "sup".into(),
            grace_ms,
            |k: &Windowed<String>, _ts| k.window.end,
            max_records,
            None,
            false,
        )
    }

    #[tokio::test]
    async fn buffers_until_window_closes_then_emits_once() {
        let mut stores = StoreRegistry::default();
        seed_windowed_store(&mut stores);
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        let mut proc = window_close_proc(0, None);

        // Two updates for window [0,10): count 1 then 2. ts in [0,10) < window end.
        for (cnt, ts) in [(1i64, 1i64), (2, 3)] {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            let change = if cnt == 1 {
                Change::update(None, 1)
            } else {
                Change::update(Some(1), 2)
            };
            proc.process(
                &mut ctx,
                Record::new(Some(windowed("a", 0, 10)), change, ts),
            )
            .await;
        }
        // Nothing emitted yet (stream_time = 3 < window end 10).
        assert!(buffer.is_empty());

        // A record for window [20,30) advances stream_time to 25 ≥ 10 → [0,10) closes.
        {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some(windowed("a", 20, 30)), Change::update(None, 1), 25),
            )
            .await;
        }
        // Exactly the [0,10) final value (2) emits; [20,30) stays buffered.
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        let k = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(k.window, Window { start: 0, end: 10 });
        assert_eq!(rec.value.downcast::<Change<i64>>().unwrap().new, Some(2));
    }

    #[tokio::test]
    async fn grace_delays_close() {
        let mut stores = StoreRegistry::default();
        seed_windowed_store(&mut stores);
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        let mut proc = window_close_proc(5, None); // grace 5

        {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some(windowed("a", 0, 10)), Change::update(None, 1), 5),
            )
            .await;
        }
        // stream_time 12 → threshold 12-5=7 < window end 10 → NOT closed.
        {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some(windowed("b", 10, 20)), Change::update(None, 1), 12),
            )
            .await;
        }
        assert!(buffer.is_empty());
        // stream_time 16 → threshold 11 >= 10 → [0,10) closes.
        {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some(windowed("c", 20, 30)), Change::update(None, 1), 16),
            )
            .await;
        }
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        assert_eq!(
            rec.key
                .unwrap()
                .downcast::<Windowed<String>>()
                .unwrap()
                .window,
            Window { start: 0, end: 10 }
        );
    }

    #[tokio::test]
    #[should_panic(expected = "max capacity")]
    async fn exceeding_max_records_shuts_down() {
        let mut stores = StoreRegistry::default();
        seed_windowed_store(&mut stores);
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        let mut proc = window_close_proc(0, Some(2)); // cap 2
        // Three distinct keys in the SAME open window [0,10) (ts < 10 → none close).
        for (k, ts) in [("a", 1i64), ("b", 2), ("c", 3)] {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            // the third put brings len() to 3 > cap 2 → panic
            proc.process(
                &mut ctx,
                Record::new(Some(windowed(k, 0, 10)), Change::update(None, 1), ts),
            )
            .await;
        }
    }

    #[tokio::test]
    #[should_panic(expected = "bytes")]
    async fn exceeding_max_bytes_shuts_down() {
        let mut stores = StoreRegistry::default();
        seed_windowed_store(&mut stores);
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        // Each entry's byte_size = key_bytes (TimeWindowedSerde: 1-char key + 8-byte
        // window-start = 9 bytes) + value_bytes (8-byte i64) = 17 bytes. A 20-byte cap
        // holds one entry (17 <= 20) but the second (34 > 20) shuts down.
        let mut proc = KTableSuppressProcessor::<Windowed<String>, i64>::new(
            "sup".into(),
            0,
            |k: &Windowed<String>, _ts| k.window.end,
            None,
            Some(20),
            false,
        );
        for (k, ts) in [("a", 1i64), ("b", 2)] {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some(windowed(k, 0, 10)), Change::update(None, 1), ts),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn at_capacity_does_not_panic_and_closes_normally() {
        let mut stores = StoreRegistry::default();
        seed_windowed_store(&mut stores);
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        let mut proc = window_close_proc(0, Some(2)); // cap 2
        // Two keys in [0,10): len == cap, not over → no panic.
        for (k, ts) in [("a", 1i64), ("b", 2)] {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some(windowed(k, 0, 10)), Change::update(None, 1), ts),
            )
            .await;
        }
        assert!(buffer.is_empty()); // nothing closed yet
        // A record in window [10,20) at ts=15 closes [0,10): the close-eviction runs
        // BEFORE the cap check, so len drops to 1 (the new window) → no panic, and
        // both [0,10) entries emit.
        {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some(windowed("z", 10, 20)), Change::update(None, 1), 15),
            )
            .await;
        }
        assert_eq!(buffer.len(), 2); // a@[0,10] and b@[0,10] emitted
    }

    #[tokio::test]
    async fn until_time_limit_newer_record_resets_timer() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(SuppressBytesStore::<String, i64>::in_memory(
            "sup".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-sup-changelog".into(),
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
        // Rate-limiter, wait 50: buffer_time = record ts (any key).
        let mut proc = KTableSuppressProcessor::<String, i64>::new(
            "sup".into(),
            50,
            |_k: &String, ts| ts,
            None,
            None,
            false,
        );

        // "a"@10 buffers (old timer would fire at 10+50=60). "a"@40 replaces it,
        // re-stamping buffer_time to 40 → new timer 90. "b"@60 advances stream-time
        // to 60 (threshold 10): a@40 is NOT due, proving the old 60 timer was reset.
        for (k, change, ts) in [
            ("a", Change::update(None, 1), 10i64),
            ("a", Change::update(Some(1), 2), 40),
            ("b", Change::update(None, 1), 60),
        ] {
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
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some(k.to_string()), change, ts))
                .await;
        }
        assert!(
            buffer.is_empty(),
            "old timer (60) must not fire after the reset"
        );

        // stream-time 90 (threshold 40): a@40 is now due → emits the NEWER value (2).
        {
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
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut d);
            proc.process(
                &mut ctx,
                Record::new(Some("c".to_string()), Change::update(None, 1), 90),
            )
            .await;
        }
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        assert_eq!(rec.key.unwrap().downcast::<String>().unwrap().as_str(), "a");
        // The newest value (2), not the stale 1 → the reset kept the latest update.
        assert_eq!(rec.value.downcast::<Change<i64>>().unwrap().new, Some(2));
    }
}
