//! Windowed stream-stream join processors (one per side): inner matches + KIP-633
//! left/outer window-close emission. Each puts its record into its own
//! retainDuplicates window store, fetches the OTHER store over the (swapped)
//! window, emits `joiner(this, Some(other))` per match, and — for left/outer —
//! buffers unmatched records in a shared outer KV store and emits the null-padded
//! result once their window closes (stream-time-driven; no wall-clock throttle).
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    dsl::processors::outer_join_store::{
        TimeTracker, outer_key, outer_key_key_bytes, outer_key_side_left, outer_key_ts,
        outer_value_decode, outer_value_left, outer_value_right,
    },
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
        serde::Serde,
    },
};

type Marker<T> = PhantomData<fn() -> T>;

/// One side of a windowed stream-stream join (inner + KIP-633 left/outer).
///
/// The `joiner` is stored in per-side OUTER form `Fn(&VThis, Option<&VOther>) -> VO`:
/// the DSL specializes the user joiner so a match calls `joiner(&this, Some(&other))`
/// and a null result calls `joiner(&this, None)`, with the `(A, B)` arg order kept
/// correct for each side.
///
/// For INNER: `emit_unmatched = false`, `outer_store/tracker/key_serde/value_serde`
/// are `None` — the left/outer block is skipped entirely.
pub(crate) struct KStreamKStreamJoinProcessor<K, VThis, VOther, VO, F> {
    pub own_store: String,
    pub other_store: String,
    pub fetch_before: i64,
    pub fetch_after: i64,
    pub joiner: F,
    pub side_left: bool,
    // ── left/outer (all `None`/`false` for inner) ──────────────────────────────
    pub emit_unmatched: bool,
    pub outer_store: Option<String>,
    pub tracker: Option<Arc<Mutex<TimeTracker>>>,
    pub key_serde: Option<Box<dyn Serde<K>>>,
    pub value_serde: Option<Box<dyn Serde<VThis>>>,
    pub before_ms: i64,
    pub after_ms: i64,
    pub grace_ms: i64,
    pub _pd: Marker<(K, VThis, VOther, VO)>,
}

#[async_trait]
impl<K, VThis, VOther, VO, F> Processor<K, VThis, K, VO>
    for KStreamKStreamJoinProcessor<K, VThis, VOther, VO, F>
where
    K: std::any::Any + Send + Sync + Clone,
    VThis: std::any::Any + Send + Sync + Clone,
    VOther: std::any::Any + Send + Sync + Clone,
    VO: std::any::Any + Send + Clone,
    F: Fn(&VThis, Option<&VOther>) -> VO + Send + 'static,
{
    // inner match + KIP-633 buffer/close-scan in one pass
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, VThis>) {
        let key = r.key.expect("stream-stream join requires a non-null key");
        let t = r.timestamp;

        // 1) put own record into own window store
        {
            let own = ctx
                .get_join_window_store::<K, VThis>(&self.own_store)
                .expect("own join store not found");
            own.put(key.clone(), t, r.value.clone()).await;
        }
        // 2) fetch the OTHER store over [t - fetch_before, t + fetch_after]
        let matches: Vec<(i64, VOther)> = {
            let other = ctx
                .get_join_window_store::<K, VOther>(&self.other_store)
                .expect("other join store not found");
            other
                .fetch(&key, t - self.fetch_before, t + self.fetch_after)
                .await
        };
        let had_match = !matches.is_empty();
        let match_ts: Vec<i64> = matches.iter().map(|(ts, _)| *ts).collect();
        // 3) emit one joined record per match at max(t, t_other)
        for (t_other, v_other) in matches {
            let out = (self.joiner)(&r.value, Some(&v_other));
            ctx.forward(Record::new(
                Some(key.clone()),
                out,
                std::cmp::max(t, t_other),
            ));
        }

        // ── left/outer: buffer unmatched + emit-on-close ──────────────────────
        let Some(os) = self.outer_store.clone() else {
            return;
        };
        let tracker = self
            .tracker
            .clone()
            .expect("left/outer requires a time tracker");
        let key_serde = self
            .key_serde
            .as_ref()
            .expect("left/outer requires a key serde");
        let value_serde = self
            .value_serde
            .as_ref()
            .expect("left/outer requires a value serde");
        let kb = key_serde.serialize(&os, &key);
        tracker.lock().expect("tracker lock").advance(t);

        // 3b) KIP-633: a matched OTHER-side record is no longer "non-joined" —
        //     remove it from the shared outer store so it can't emit a spurious null.
        if !match_ts.is_empty() {
            let osr = ctx
                .get_state_store::<Bytes, Bytes>(&os)
                .expect("outer join store not found");
            for t_other in &match_ts {
                osr.delete(&outer_key(*t_other, !self.side_left, &kb)).await;
            }
        }

        // 4) this record had no match → if its side emits non-joins, eager-emit when
        //    the window has ALREADY closed (a later record advanced stream time past
        //    `t + fetch_after`), else buffer it for the close-scan. (KIP-633: the
        //    buffer-then-close path is what makes windowed left/outer correct — an
        //    `store_empty` eager-emit would short-circuit it on every record, since
        //    eager-emit never fills the store, so the first record's window would
        //    never get a chance to close. We keep the empty-store check only as a
        //    fast forward when nothing is pending AND the window is closed.)
        if self.emit_unmatched && !had_match {
            let st = tracker.lock().expect("tracker lock").stream_time;
            if t + self.fetch_after < st {
                let out = (self.joiner)(&r.value, None);
                ctx.forward(Record::new(Some(key.clone()), out, t));
            } else {
                let raw = value_serde.serialize(&os, &r.value);
                let tagged = if self.side_left {
                    outer_value_left(&raw)
                } else {
                    outer_value_right(&raw)
                };
                let osr = ctx
                    .get_state_store::<Bytes, Bytes>(&os)
                    .expect("outer join store");
                osr.put(outer_key(t, self.side_left, &kb), tagged).await;
            }
        }

        // 5) close-scan: emit this side's buffered records whose window has closed.
        //    DIVERGENCE FROM JVM: the JVM flushes BOTH sides (time-ordered, wall-clock
        //    throttled) on every record; we scan only our OWN side. With both sides
        //    receiving traffic the emitted result set matches, but a record buffered on
        //    a side that then goes silent stays unflushed until that side sees another
        //    record (the JVM punctuator would flush it). Documented in the spec.
        let st = tracker.lock().expect("tracker lock").stream_time;
        let lookback = if self.side_left {
            self.after_ms
        } else {
            self.before_ms
        };
        let zero = Bytes::copy_from_slice(&0i64.to_be_bytes());
        let hi = Bytes::copy_from_slice(&st.saturating_add(1).to_be_bytes());
        // (entry_ts, composite_key_bytes, decoded_value, decoded_key)
        let closed: Vec<(i64, Bytes, VThis, K)> = {
            let osr = ctx
                .get_state_store::<Bytes, Bytes>(&os)
                .expect("outer join store");
            osr.range(&zero, &hi)
                .await
                .into_iter()
                .filter(|(k, _)| outer_key_side_left(k) == self.side_left)
                .filter(|(k, _)| outer_key_ts(k) + lookback + self.grace_ms < st)
                .map(|(k, v)| {
                    let (_is_left, raw) = outer_value_decode(&v);
                    let val = value_serde
                        .deserialize(&os, raw)
                        .expect("outer value deserialize");
                    let ekey = key_serde
                        .deserialize(&os, outer_key_key_bytes(&k))
                        .expect("outer key deserialize");
                    (outer_key_ts(&k), k, val, ekey)
                })
                .collect()
        };
        for (ets, ck, val, ekey) in closed {
            {
                let osr = ctx
                    .get_state_store::<Bytes, Bytes>(&os)
                    .expect("outer join store");
                osr.delete(&ck).await;
            }
            ctx.forward(Record::new(Some(ekey), (self.joiner)(&val, None), ets));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        marker::PhantomData,
        sync::{Arc, Mutex},
    };

    use bytes::Bytes;

    use super::*;
    use crate::{
        processor::{
            api::ProcessorContext,
            erased::{Dispatch, ErasedRecord},
            record::{Record, RecordContext},
            serde::{BytesSerde, StringSerde},
        },
        store::{
            join_window::JoinWindowBytesStore, kv::KeyValueBytesStore, registry::StoreRegistry,
        },
    };

    fn make_stores() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(JoinWindowBytesStore::<String, String>::in_memory(
            "this".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "app-this-changelog".into(),
        )));
        stores.insert(Box::new(JoinWindowBytesStore::<String, String>::in_memory(
            "other".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "app-other-changelog".into(),
        )));
        stores
    }

    /// Inner-only processor: no shared outer store / tracker / serdes.
    fn make_proc() -> KStreamKStreamJoinProcessor<
        String,
        String,
        String,
        String,
        impl Fn(&String, Option<&String>) -> String,
    > {
        KStreamKStreamJoinProcessor {
            own_store: "this".into(),
            other_store: "other".into(),
            fetch_before: 10,
            fetch_after: 10,
            // inner only ever passes `Some`.
            joiner: |a: &String, b: Option<&String>| {
                format!("{a}{}", b.cloned().unwrap_or_default())
            },
            side_left: true,
            emit_unmatched: false,
            outer_store: None,
            tracker: None,
            key_serde: None,
            value_serde: None,
            before_ms: 0,
            after_ms: 0,
            grace_ms: 0,
            _pd: PhantomData::<fn() -> (String, String, String, String)>,
        }
    }

    #[tokio::test]
    async fn this_side_joins_records_in_window() {
        let mut stores = make_stores();

        // Seed "other" (B side) with two records for key "k": at ts 3 and ts 50.
        {
            let s = stores.get_join_window::<String, String>("other").unwrap();
            s.put("k".into(), 3, "b1".into()).await;
            s.put("k".into(), 50, "b2".into()).await;
        }

        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "left".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        let mut proc = make_proc();

        // Process A-record (k, "a") at ts=5 → fetch other [5-10, 5+10]=[-5,15]
        // → matches b1@3 (not b2@50).
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
            let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("k".into()), "a".into(), 5))
                .await;
        }

        // exactly one forward: "ab1" at max(5,3)=5
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        assert_eq!(*rec.value.downcast::<String>().unwrap(), "ab1");
        assert_eq!(rec.timestamp, 5);
    }

    #[tokio::test]
    async fn duplicates_emit_one_per_match() {
        let mut stores = make_stores();

        // Seed "other" with two records at the SAME ts (4) within the window.
        {
            let s = stores.get_join_window::<String, String>("other").unwrap();
            s.put("k".into(), 4, "x".into()).await;
            s.put("k".into(), 4, "y".into()).await;
        }

        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "left".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        let mut proc = make_proc();

        // Process A-record (k, "a") at ts=5 → window [-5,15] → both duplicates at ts=4
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
            let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("k".into()), "a".into(), 5))
                .await;
        }

        // Two forwards: "ax" and "ay", both at max(5,4)=5
        assert_eq!(buffer.len(), 2);

        let (_, rec1) = buffer.pop_front().unwrap();
        assert_eq!(*rec1.value.downcast::<String>().unwrap(), "ax");
        assert_eq!(rec1.timestamp, 5);

        let (_, rec2) = buffer.pop_front().unwrap();
        assert_eq!(*rec2.value.downcast::<String>().unwrap(), "ay");
        assert_eq!(rec2.timestamp, 5);
    }

    /// A left-side processor with no matching right record buffers the record into
    /// the shared outer store (no forward yet). A later record that advances
    /// stream-time past `ts + after` triggers the close-scan, emitting the
    /// null-padded `joiner(a, None)`.
    #[tokio::test]
    async fn left_buffers_then_emits_on_close() {
        let mut stores = make_stores();
        // Shared outer KV store (Bytes→Bytes, identity serdes).
        stores.insert(Box::new(KeyValueBytesStore::<Bytes, Bytes>::in_memory(
            "outer".into(),
            Box::new(BytesSerde),
            Box::new(BytesSerde),
            "app-outer-changelog".into(),
        )));

        let tracker = Arc::new(Mutex::new(TimeTracker::default()));

        let mut proc = KStreamKStreamJoinProcessor {
            own_store: "this".into(),
            other_store: "other".into(),
            fetch_before: 10,
            fetch_after: 10,
            joiner: |a: &String, b: Option<&String>| {
                format!("{a}{}", b.cloned().unwrap_or_default())
            },
            side_left: true,
            emit_unmatched: true,
            outer_store: Some("outer".into()),
            tracker: Some(Arc::clone(&tracker)),
            key_serde: Some(Box::new(StringSerde)),
            value_serde: Some(Box::new(StringSerde)),
            before_ms: 10,
            after_ms: 10,
            grace_ms: 0,
            _pd: PhantomData::<fn() -> (String, String, String, String)>,
        };

        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "left".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        // First A at t=5 with no B match. Its window (5+after=15) is open at
        // stream_time=5 → buffered, no forward yet.
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
            let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("k".into()), "a".into(), 5))
                .await;
        }
        // Nothing emitted yet (buffered, window not closed at stream_time=5).
        assert!(buffer.is_empty(), "expected buffered, got {}", buffer.len());

        // Second A at t=100 advances stream_time past 5+after(10) → close-scan emits
        // the buffered left record (joiner(a, None) = "a") at its own ts=5. The
        // t=100 record itself buffers (window open) and does not emit.
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
            let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("k".into()), "z".into(), 100))
                .await;
        }

        // The close-scan emitted the buffered ts=5 record as a null-padded left
        // result ("a") at ts=5.
        assert_eq!(buffer.len(), 1, "expected 1 close-emit");
        let (_, rec) = buffer.pop_front().unwrap();
        assert_eq!(*rec.value.downcast::<String>().unwrap(), "a");
        assert_eq!(rec.timestamp, 5);
    }
}
