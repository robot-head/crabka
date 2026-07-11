//! `KGroupedTable` processors (`KTable.groupBy` aggregation).
//!
//! - [`KTableRepartitionMapProcessor`]: `Change<V>` in → keyed `Change<VR>` out.
//!   Maps each present side of the change through the user mapper; on a
//!   grouping-key change it forwards a subtract-only record to the old key and
//!   an add-only record to the new key.
//! - [`KTableAggregateProcessor`]: `Change<VR>` in → `Change<T>` out. Subtracts the
//!   old value's contribution then adds the new value's, over a `KeyValueStore`.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{
    dsl::processors::{change::Change, tuple_forwarder::TupleForwarder},
    processor::{
        api::{Processor, ProcessorContext},
        record::Record,
    },
};

type Marker<T> = PhantomData<fn() -> T>;

/// Maps the upstream `Change<V>` to the grouped key/value, splitting a
/// grouping-key change into a subtract-only (old key) and add-only (new key)
/// record so the downstream aggregate nets the change in the right groups.
#[allow(dead_code)]
pub(crate) struct KTableRepartitionMapProcessor<K, V, KR, VR, M> {
    pub mapper: M,
    pub _pd: Marker<(K, V, KR, VR)>,
}

#[async_trait]
impl<K, V, KR, VR, M> Processor<K, Change<V>, KR, Change<VR>>
    for KTableRepartitionMapProcessor<K, V, KR, VR, M>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    KR: std::any::Any + Send + Sync + Clone + PartialEq,
    VR: std::any::Any + Send + Clone,
    M: Fn(&K, &V) -> (KR, VR) + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KR, Change<VR>>,
        r: Record<K, Change<V>>,
    ) {
        let key = r.key.expect("KGroupedTable map requires a non-null key");
        let ts = r.timestamp;
        let old_pair = r.value.old.as_ref().map(|v| (self.mapper)(&key, v));
        let new_pair = r.value.new.as_ref().map(|v| (self.mapper)(&key, v));
        match (old_pair, new_pair) {
            (Some((ko, vo)), Some((kn, vn))) if ko == kn => {
                ctx.forward(Record::new(
                    Some(kn),
                    Change {
                        old: Some(vo),
                        new: Some(vn),
                    },
                    ts,
                ));
            }
            (old_pair, new_pair) => {
                if let Some((ko, vo)) = old_pair {
                    ctx.forward(Record::new(
                        Some(ko),
                        Change {
                            old: Some(vo),
                            new: None,
                        },
                        ts,
                    ));
                }
                if let Some((kn, vn)) = new_pair {
                    ctx.forward(Record::new(
                        Some(kn),
                        Change {
                            old: None,
                            new: Some(vn),
                        },
                        ts,
                    ));
                }
            }
        }
    }
}

/// Subtract-then-add table aggregation over a `KeyValueStore` keyed `KR`,
/// holding the accumulator `T`. `init` seeds an empty group; `subtractor`
/// removes the old value's contribution; `adder` adds the new value's.
#[allow(dead_code)]
pub(crate) struct KTableAggregateProcessor<KR, VR, T, I, Add, Sub> {
    pub store_name: String,
    pub init: I,
    pub adder: Add,
    pub subtractor: Sub,
    pub forwarder: TupleForwarder,
    pub _pd: Marker<(KR, VR, T)>,
}

#[async_trait]
impl<KR, VR, T, I, Add, Sub> Processor<KR, Change<VR>, KR, Change<T>>
    for KTableAggregateProcessor<KR, VR, T, I, Add, Sub>
where
    KR: std::any::Any + Send + Sync + Clone,
    VR: Send + 'static,
    T: std::any::Any + Send + Clone,
    I: Fn() -> T + Send + 'static,
    Add: Fn(&KR, &VR, T) -> T + Send + 'static,
    Sub: Fn(&KR, &VR, T) -> T + Send + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, KR, Change<T>>) {
        self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KR, Change<T>>,
        r: Record<KR, Change<VR>>,
    ) {
        let key = r
            .key
            .expect("KGroupedTable aggregate requires a non-null key");
        let rc = ctx.record_context().clone();
        let (old, new) = {
            let store = ctx
                .get_state_store::<KR, T>(&self.store_name)
                .expect("KGroupedTable aggregate store not found");
            store.set_record_context(rc);
            let prior = store.get(&key).await;
            let mut agg = prior.clone().unwrap_or_else(|| (self.init)());
            if let Some(ov) = &r.value.old {
                agg = (self.subtractor)(&key, ov, agg);
            }
            if let Some(nv) = &r.value.new {
                agg = (self.adder)(&key, nv, agg);
            }
            store.put(key.clone(), agg.clone()).await;
            (prior, agg)
        };
        self.forwarder
            .maybe_forward(ctx, key, old, new, r.timestamp);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, marker::PhantomData};

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

    fn rc() -> RecordContext {
        RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    /// `KTableRepartitionMapProcessor` splits a grouping-key change into a
    /// subtract-only record on the old key and an add-only record on the new key.
    #[tokio::test]
    async fn map_splits_on_key_change() {
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableRepartitionMapProcessor::<String, i64, String, i64, _> {
            mapper: |_k: &String, v: &i64| {
                if v % 2 == 0 {
                    ("even".to_string(), *v)
                } else {
                    ("odd".to_string(), *v)
                }
            },
            _pd: PhantomData,
        };

        // old=4 (even) → new=5 (odd): grouping key changes, must split.
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
            proc.process(
                &mut ctx,
                Record::new(Some("b".into()), Change::update(Some(4i64), 5i64), 0),
            )
            .await;
        }

        let actual = buffer
            .into_iter()
            .map(|(_, rec)| {
                (
                    *rec.key.unwrap().downcast::<String>().unwrap(),
                    *rec.value.downcast::<Change<i64>>().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        check!(
            actual
                == vec![
                    (
                        "even".into(),
                        Change {
                            old: Some(4i64),
                            new: None,
                        },
                    ),
                    (
                        "odd".into(),
                        Change {
                            old: None,
                            new: Some(5i64),
                        },
                    ),
                ],
            "forwarded subtract/add records"
        );
    }

    /// `KTableAggregateProcessor` subtract-then-adds through three operations,
    /// nets to 0 at the end, and the store reflects the final accumulator.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn aggregate_subtracts_then_adds() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
            "agg".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "agg-changelog".into(),
        )));

        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();

        let mut proc = KTableAggregateProcessor::<String, i64, i64, _, _, _> {
            store_name: "agg".into(),
            init: || 0i64,
            adder: |_k: &String, v: &i64, a: i64| a + v,
            subtractor: |_k: &String, v: &i64, a: i64| a - v,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };

        // --- Step 1: key="even", old=None, new=Some(2) → store: 0+2=2 ---
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
            proc.process(
                &mut ctx,
                Record::new(
                    Some("even".into()),
                    Change {
                        old: None,
                        new: Some(2i64),
                    },
                    0,
                ),
            )
            .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(
            *change
                == Change {
                    old: None,
                    new: Some(2i64)
                }
        );

        // --- Step 2: key="even", old=Some(2), new=Some(6) → store: 2-2+6=6 ---
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
            proc.process(
                &mut ctx,
                Record::new(
                    Some("even".into()),
                    Change {
                        old: Some(2i64),
                        new: Some(6i64),
                    },
                    1,
                ),
            )
            .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(
            *change
                == Change {
                    old: Some(2i64),
                    new: Some(6i64)
                }
        );

        // --- Step 3: key="even", old=Some(6), new=None → store: 6-6=0 ---
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
            proc.process(
                &mut ctx,
                Record::new(
                    Some("even".into()),
                    Change {
                        old: Some(6i64),
                        new: None,
                    },
                    2,
                ),
            )
            .await;
        }
        let (_, rec) = buffer.pop_front().unwrap();
        let change = rec.value.downcast::<Change<i64>>().unwrap();
        check!(
            *change
                == Change {
                    old: Some(6i64),
                    new: Some(0i64)
                }
        );

        // Store must reflect the final accumulator = 0.
        check!(
            stores
                .get_kv::<String, i64>("agg")
                .unwrap()
                .get(&"even".to_string())
                .await
                == Some(0)
        );
    }

    /// An `agg` store registry, optionally record-cached.
    fn agg_registry(cached: bool) -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
            "agg".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "agg-changelog".into(),
        )));
        if cached {
            stores.enable_cache(
                "agg",
                std::sync::Arc::new(std::sync::Mutex::new(
                    crate::store::cache::named::NamedCache::new("agg".into()),
                )),
            );
        }
        stores
    }

    /// Run `init` then two same-key `process` calls (adds 2 then 6) through the
    /// table aggregate, returning how many records reached the downstream buffer.
    async fn agg_run_two(stores: &mut StoreRegistry) -> usize {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let mut proc = KTableAggregateProcessor::<String, i64, i64, _, _, _> {
            store_name: "agg".into(),
            init: || 0i64,
            adder: |_k: &String, v: &i64, a: i64| a + v,
            subtractor: |_k: &String, v: &i64, a: i64| a - v,
            forwarder: TupleForwarder::default(),
            _pd: PhantomData,
        };
        for (ts, new) in [(0i64, 2i64), (1i64, 6i64)] {
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
                proc.init(&mut ctx).await;
            }
            proc.process(
                &mut ctx,
                Record::new(
                    Some("even".into()),
                    Change {
                        old: None,
                        new: Some(new),
                    },
                    ts,
                ),
            )
            .await;
        }
        buffer.len()
    }

    /// Uncached store → the aggregate forwards each record immediately (today's
    /// behavior, unchanged): two records → two forwards.
    #[tokio::test]
    async fn uncached_table_aggregate_forwards_each_record() {
        let mut stores = agg_registry(false);
        check!(agg_run_two(&mut stores).await == 2);
    }

    /// Cached store → the immediate forward is suppressed (the cache flush will
    /// forward the deduped change): two records → zero immediate forwards, and the
    /// cached store still holds the dirty entry to flush.
    #[tokio::test]
    async fn cached_table_aggregate_suppresses_immediate_forward() {
        let mut stores = agg_registry(true);
        check!(agg_run_two(&mut stores).await == 0);
        check!(stores.kv_is_cached("agg"));
        // adder adds both contributions (no subtract: old=None each): 0+2+6 = 8.
        let store = stores.get_kv::<String, i64>("agg").unwrap();
        check!(store.get(&"even".to_string()).await == Some(8));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        stores
            .get_mut("agg")
            .unwrap()
            .flush_cache_into(&mut buffer, &[0])
            .await;
        check!(buffer.len() == 1);
    }
}
