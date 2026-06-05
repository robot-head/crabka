//! Windowed stream-stream join processors (one per side) — INNER path.
//! Each: put own record into its own [`JoinWindowStore`], fetch the OTHER store over
//! the (side-swapped, supplied) window, emit `joiner(this, other)` per match at
//! max(tThis, tOther). The DSL supplies a per-side specialized joiner so the
//! arg order is correct; this processor just calls it. (Phase C extends this
//! struct for left/outer null-result emission.)
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// One side of a windowed stream-stream inner join.
///
/// On each record:
/// 1. Puts `(key, timestamp, value)` into `own_store` (retainDuplicates).
/// 2. Fetches `other_store` over `[t - fetch_before, t + fetch_after]`.
/// 3. For each match emits `joiner(&own_value, &other_value)` at
///    `max(t_own, t_other)`.
///
/// The DSL creates TWO of these (one per stream side) with swapped
/// `fetch_before`/`fetch_after` and a side-specialized `joiner` that
/// places the arguments in the correct `(left, right)` order.
///
/// `side_left` is stored for Phase C (left/outer emit-on-close path).
#[allow(dead_code)] // some fields used by Phase B lowering; side_left reserved for Phase C
pub(crate) struct KStreamKStreamJoinProcessor<K, VThis, VOther, VO, F> {
    pub own_store: String,
    pub other_store: String,
    /// Half-window on the own-side: fetch other in `[t - fetch_before, t + fetch_after]`.
    pub fetch_before: i64,
    /// Half-window on the other-side (swapped by the DSL relative to the named window).
    pub fetch_after: i64,
    /// Per-side specialized joiner: `Fn(&VThis, &VOther) -> VO`.
    pub joiner: F,
    /// Which side this processor drains (reserved for Phase C).
    pub side_left: bool,
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
    F: Fn(&VThis, &VOther) -> VO + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, VThis>) {
        let key = r.key.expect("stream-stream join requires a non-null key");
        let t = r.timestamp;

        // 1) Put own record into own store — borrow dropped before next store access.
        {
            let own = ctx
                .get_join_window_store::<K, VThis>(&self.own_store)
                .expect("own join store not found");
            own.put(key.clone(), t, r.value.clone()).await;
        }

        // 2) Fetch the other store over [t - fetch_before, t + fetch_after].
        //    Collect into a Vec so the borrow is released before we call forward.
        let matches: Vec<(i64, VOther)> = {
            let other = ctx
                .get_join_window_store::<K, VOther>(&self.other_store)
                .expect("other join store not found");
            other
                .fetch(&key, t - self.fetch_before, t + self.fetch_after)
                .await
        };

        // 3) Emit per match at max(t_own, t_other).
        for (t_other, v_other) in matches {
            let out = (self.joiner)(&r.value, &v_other);
            ctx.forward(Record::new(
                Some(key.clone()),
                out,
                std::cmp::max(t, t_other),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::marker::PhantomData;

    use super::*;
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use crate::processor::serde::StringSerde;
    use crate::store::join_window::JoinWindowBytesStore;
    use crate::store::registry::StoreRegistry;

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

    fn make_proc() -> KStreamKStreamJoinProcessor<
        String,
        String,
        String,
        String,
        impl Fn(&String, &String) -> String,
    > {
        KStreamKStreamJoinProcessor {
            own_store: "this".into(),
            other_store: "other".into(),
            fetch_before: 10,
            fetch_after: 10,
            joiner: |a: &String, b: &String| format!("{a}{b}"),
            side_left: true,
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
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: &mut output,
                record_ctx: &rc,
                stores: &mut stores,
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
}
