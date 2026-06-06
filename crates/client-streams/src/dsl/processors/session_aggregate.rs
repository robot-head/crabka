//! Session-window aggregation processors: JVM session-merge, emit-on-update.
//!
//! On each record the processor finds all sessions within the inactivity gap,
//! merges them (and the record) into one `[minStart, maxEnd]` session, removes
//! the merged-away sessions (emitting a tombstone per the now-stale session key),
//! and emits the new merged session. No window closing / suppression.
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::windows::{Window, Windowed};
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// General session aggregation (`count` / `aggregate`). `init` seeds the
/// accumulator, `agg` folds the new record, `merger` combines two session
/// aggregates when sessions merge.
#[allow(dead_code)]
pub(crate) struct KStreamSessionAggregateProcessor<K, V, VA, I, A, M> {
    pub store_name: String,
    pub gap_ms: i64,
    pub init: I,
    pub agg: A,
    pub merger: M,
    pub _pd: Marker<(K, V, VA)>,
}

#[async_trait]
impl<K, V, VA, I, A, M> Processor<K, V, Windowed<K>, Change<VA>>
    for KStreamSessionAggregateProcessor<K, V, VA, I, A, M>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + Sync + 'static,
    VA: std::any::Any + Send + Sync + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
    M: Fn(&K, VA, VA) -> VA + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("session aggregate requires a non-null key");
        let ts = r.timestamp;
        let gap = self.gap_ms;

        // 1. find merge candidates: sessions overlapping [ts-gap, ts+gap].
        let cands: Vec<(i64, i64, VA)> = {
            let store = ctx
                .get_session_store::<K, VA>(&self.store_name)
                .expect("session store not found");
            store.find_sessions(&key, ts - gap, ts + gap).await
        };

        // 2. merge: fold candidate aggregates via the merger, then the record.
        let mut new_start = ts;
        let mut new_end = ts;
        let mut acc = (self.init)();
        for (s, e, v) in &cands {
            acc = (self.merger)(&key, acc, v.clone());
            new_start = new_start.min(*s);
            new_end = new_end.max(*e);
        }
        acc = (self.agg)(&key, &r.value, acc);

        // 3. remove + tombstone each merged-away session (its key is now stale).
        for (s, e, v) in &cands {
            {
                let store = ctx
                    .get_session_store::<K, VA>(&self.store_name)
                    .expect("session store not found");
                store.remove(&key, *s, *e).await;
            }
            ctx.forward(Record::new(
                Some(Windowed {
                    key: key.clone(),
                    window: Window { start: *s, end: *e },
                }),
                Change::tombstone(Some(v.clone())),
                *e,
            ));
        }

        // 4. put + emit the new merged session.
        {
            let store = ctx
                .get_session_store::<K, VA>(&self.store_name)
                .expect("session store not found");
            store
                .put(key.clone(), new_start, new_end, acc.clone())
                .await;
        }
        ctx.forward(Record::new(
            Some(Windowed {
                key: key.clone(),
                window: Window {
                    start: new_start,
                    end: new_end,
                },
            }),
            Change::update(None, acc),
            new_end,
        ));
    }
}

/// Session reduce: keeps the public value type `V` (no `init`/sentinel). The
/// first contribution seeds; folding old sessions + the new record uses
/// `reducer`. The merge structure is identical to the aggregate processor.
#[allow(dead_code)]
pub(crate) struct KStreamSessionReduceProcessor<K, V, R> {
    pub store_name: String,
    pub gap_ms: i64,
    pub reducer: R,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, R> Processor<K, V, Windowed<K>, Change<V>> for KStreamSessionReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Sync + Clone,
    R: Fn(&V, &V) -> V + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("session reduce requires a non-null key");
        let ts = r.timestamp;
        let gap = self.gap_ms;

        let cands: Vec<(i64, i64, V)> = {
            let store = ctx
                .get_session_store::<K, V>(&self.store_name)
                .expect("session store not found");
            store.find_sessions(&key, ts - gap, ts + gap).await
        };

        let mut new_start = ts;
        let mut new_end = ts;
        let mut acc: Option<V> = None;
        for (s, e, v) in &cands {
            acc = Some(match acc {
                None => v.clone(),
                Some(a) => (self.reducer)(&a, v),
            });
            new_start = new_start.min(*s);
            new_end = new_end.max(*e);
        }
        let acc: V = match acc {
            None => r.value.clone(),
            Some(a) => (self.reducer)(&a, &r.value),
        };

        for (s, e, v) in &cands {
            {
                let store = ctx
                    .get_session_store::<K, V>(&self.store_name)
                    .expect("session store not found");
                store.remove(&key, *s, *e).await;
            }
            ctx.forward(Record::new(
                Some(Windowed {
                    key: key.clone(),
                    window: Window { start: *s, end: *e },
                }),
                Change::tombstone(Some(v.clone())),
                *e,
            ));
        }

        {
            let store = ctx
                .get_session_store::<K, V>(&self.store_name)
                .expect("session store not found");
            store
                .put(key.clone(), new_start, new_end, acc.clone())
                .await;
        }
        ctx.forward(Record::new(
            Some(Windowed {
                key: key.clone(),
                window: Window {
                    start: new_start,
                    end: new_end,
                },
            }),
            Change::update(None, acc),
            new_end,
        ));
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
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::registry::StoreRegistry;
    use crate::store::session::SessionBytesStore;

    fn registry() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-s-changelog".into(),
        )));
        stores
    }

    #[tokio::test]
    async fn merge_within_gap_tombstones_then_updates() {
        let mut stores = registry();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };

        let mut proc = KStreamSessionAggregateProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            merger: |_k: &String, a: i64, b: i64| a + b,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };

        // record 1 @ ts=0 → new session [0,0] count 1, no candidates → one update.
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
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 0))
                .await;
        }
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        let ch = rec.value.downcast::<Change<i64>>().unwrap();
        let wk = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(wk.window, Window { start: 0, end: 0 });
        assert_eq!((ch.old, ch.new), (None, Some(1)));

        // record 2 @ ts=30 (within gap 60 of session [0,0]) → merge:
        //   tombstone [0,0], update merged [0,30] count 2.
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
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 30))
                .await;
        }
        assert_eq!(buffer.len(), 2);
        let (_, tomb) = buffer.pop_front().unwrap();
        let tch = tomb.value.downcast::<Change<i64>>().unwrap();
        let tkey = tomb.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(tkey.window, Window { start: 0, end: 0 });
        assert!(tch.is_tombstone());
        let (_, upd) = buffer.pop_front().unwrap();
        let uch = upd.value.downcast::<Change<i64>>().unwrap();
        let ukey = upd.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(ukey.window, Window { start: 0, end: 30 });
        assert_eq!((uch.old, uch.new), (None, Some(2)));
    }

    #[tokio::test]
    async fn beyond_gap_two_independent_sessions() {
        let mut stores = registry();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        let mut proc = KStreamSessionAggregateProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            merger: |_k: &String, a: i64, b: i64| a + b,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };
        for ts in [0i64, 200] {
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
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), ts))
                .await;
        }
        // No merge: two updates, no tombstone (200 is > gap 60 from [0,0]).
        assert_eq!(buffer.len(), 2);
        for (_, rec) in buffer.drain(..) {
            assert!(!rec.value.downcast::<Change<i64>>().unwrap().is_tombstone());
        }
    }

    #[tokio::test]
    async fn three_way_bridge_merge() {
        let mut stores = registry();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        let mut proc = KStreamSessionAggregateProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            merger: |_k: &String, a: i64, b: i64| a + b,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };
        // ts=0 → [0,0]; ts=100 → [100,100] (not within gap of [0,0]); ts=50 bridges
        // both → merged [0,100] count 3 + tombstones for [0,0] and [100,100].
        for ts in [0i64, 100] {
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
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), ts))
                .await;
        }
        buffer.clear(); // discard the two initial updates
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
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 50))
                .await;
        }
        // Deterministic emit order: tombstone candidates in store order (end asc) —
        // [0,0] then [100,100] — then the merged update [0,100] = 3.
        assert_eq!(buffer.len(), 3);
        let (_, t0) = buffer.pop_front().unwrap();
        assert!(t0.value.downcast::<Change<i64>>().unwrap().is_tombstone());
        assert_eq!(
            t0.key
                .unwrap()
                .downcast::<Windowed<String>>()
                .unwrap()
                .window,
            Window { start: 0, end: 0 }
        );
        let (_, t1) = buffer.pop_front().unwrap();
        assert!(t1.value.downcast::<Change<i64>>().unwrap().is_tombstone());
        assert_eq!(
            t1.key
                .unwrap()
                .downcast::<Windowed<String>>()
                .unwrap()
                .window,
            Window {
                start: 100,
                end: 100
            }
        );
        let (_, upd) = buffer.pop_front().unwrap();
        assert_eq!(
            upd.key
                .unwrap()
                .downcast::<Windowed<String>>()
                .unwrap()
                .window,
            Window { start: 0, end: 100 }
        );
        assert_eq!(upd.value.downcast::<Change<i64>>().unwrap().new, Some(3));
    }

    #[tokio::test]
    async fn reduce_first_seeds_then_folds() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(SessionBytesStore::<String, String>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "app-s-changelog".into(),
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
        let mut proc = KStreamSessionReduceProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            reducer: |a: &String, b: &String| format!("{a}{b}"),
            _pd: PhantomData::<fn() -> (String, String)>,
        };
        // "x"@0 seeds [0,0]="x" (one update). "y"@30 merges → tombstone [0,0] then
        // update [0,30]="xy". Drain in order and check the final merged update.
        for (v, ts) in [("x", 0i64), ("y", 30)] {
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
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<String>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), v.into(), ts))
                .await;
        }
        // buffer = [update[0,0]="x", tombstone[0,0], update[0,30]="xy"].
        assert_eq!(buffer.len(), 3);
        let (_, last) = buffer.pop_back().unwrap();
        assert_eq!(
            last.key
                .unwrap()
                .downcast::<Windowed<String>>()
                .unwrap()
                .window,
            Window { start: 0, end: 30 }
        );
        assert_eq!(
            last.value.downcast::<Change<String>>().unwrap().new,
            Some("xy".to_string())
        );
    }
}
