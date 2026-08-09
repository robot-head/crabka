//! `KStreamKTableJoinProcessor`, the stream-table join in its inner and left
//! forms.
//!
//! - **Inner** (`emit_on_miss = false`) forwards only when the table has a
//!   matching value for the stream record's key.
//! - **Left** (`emit_on_miss = true`) always forwards. When the table has no
//!   entry, the joiner receives `None` as the table-side value.
//!
//! The joiner signature is `Fn(&V, Option<&VT>) -> VO`, which covers both forms.
//! In an inner join the caller can assume `Some`. In a left join the caller
//! handles `None`.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::processor::{
    api::{Processor, ProcessorContext},
    record::Record,
};

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Stream-table join processor.
///
/// The processor looks up the stream record's key in the named state store,
/// which holds the `KTable`'s materialized view. On a match, or when
/// `emit_on_miss` is true, it calls `joiner(&stream_value, table_value)` and
/// forwards the result.
pub(crate) struct KStreamKTableJoinProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    /// The joiner, `Fn(&V, Option<&VT>) -> VO`.
    /// - An inner join calls it only when `table_value.is_some()`.
    /// - A left join always calls it, and `table_value` is `None` on a miss.
    pub joiner: F,
    /// `false` selects an inner join, which skips on a miss. `true` selects a
    /// left join, which emits on a miss.
    pub emit_on_miss: bool,
    pub _pd: Marker<(K, V, VT, VO)>,
}

#[async_trait]
impl<K, V, VT, VO, F> Processor<K, V, K, VO> for KStreamKTableJoinProcessor<K, V, VT, VO, F>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VT: Send + 'static,
    VO: std::any::Any + Send + Clone,
    F: Fn(&V, Option<&VT>) -> VO + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, V>) {
        let key = r.key.expect("join requires a non-null key");
        // `and_then(|s| s.get(..))` can't `.await`; do the lookup in a `match`,
        // dropping the store borrow before `ctx.forward`.
        let vt = match ctx.get_state_store::<K, VT>(&self.table_store) {
            Some(s) => s.get(&key).await,
            None => None,
        };
        if vt.is_some() || self.emit_on_miss {
            let out = (self.joiner)(&r.value, vt.as_ref());
            ctx.forward(Record::new(Some(key), out, r.timestamp));
        }
    }
}

/// As-of stream-table join processor (KIP-914).
///
/// It matches [`KStreamKTableJoinProcessor`] except for the table lookup, which
/// is a versioned `get_as_of(key, streamRec.ts)`. That call gives the table value
/// valid *as of the stream record's timestamp*. A null as-of result counts as a
/// miss, so an inner join skips it and a left join passes `None`. The processor
/// forwards its output at the stream record's timestamp.
#[allow(dead_code)]
pub(crate) struct KStreamKTableJoinAsOfProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    pub joiner: F,
    pub emit_on_miss: bool,
    pub _pd: Marker<(K, V, VT, VO)>,
}

#[async_trait]
impl<K, V, VT, VO, F> Processor<K, V, K, VO> for KStreamKTableJoinAsOfProcessor<K, V, VT, VO, F>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VT: Send + 'static,
    VO: std::any::Any + Send + Clone,
    F: Fn(&V, Option<&VT>) -> VO + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, V>) {
        let key = r.key.expect("join requires a non-null key");
        let ts = r.timestamp;
        let vt = match ctx.get_versioned_store::<K, VT>(&self.table_store) {
            Some(s) => s.get_as_of(&key, ts).await.map(|rec| rec.value),
            None => None,
        };
        if vt.is_some() || self.emit_on_miss {
            let out = (self.joiner)(&r.value, vt.as_ref());
            ctx.forward(Record::new(Some(key), out, ts));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, marker::PhantomData};

    use assert2::check;
    use crabka_units::prelude::*;

    use super::*;
    use crate::{
        processor::{
            api::ProcessorContext,
            erased::{Dispatch, ErasedRecord},
            record::RecordContext,
            serde::{I64Serde, StringSerde},
        },
        store::{
            api::KeyValueStore,
            kv::KeyValueBytesStore,
            registry::StoreRegistry,
            versioned::{VersionedBytesStore, VersionedKeyValueStore},
        },
    };

    /// Build a store registry with a `KeyValueBytesStore<String, i64>` named
    /// `"t"` pre-seeded with `("a", 10)`.
    async fn make_stores() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        let mut kv = KeyValueBytesStore::<String, i64>::in_memory(
            "t".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-t-changelog".into(),
        );
        kv.put("a".to_string(), 10i64).await;
        stores.insert(Box::new(kv));
        stores
    }

    /// Process a single `(key, value)` record through `proc` and return the
    /// forwarded `i64` output, or `None` when the processor forwarded
    /// nothing.
    async fn run_one(
        proc: &mut KStreamKTableJoinProcessor<
            String,
            i64,
            i64,
            i64,
            impl Fn(&i64, Option<&i64>) -> i64 + Send + 'static,
        >,
        stores: &mut StoreRegistry,
        key: &str,
        value: i64,
    ) -> Option<i64> {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
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
        let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
        proc.process(&mut ctx, Record::new(Some(key.to_string()), value, 0))
            .await;
        buffer
            .pop_front()
            .map(|(_, rec)| *rec.value.downcast::<i64>().unwrap())
    }

    #[tokio::test]
    async fn inner_join_hit_and_miss() {
        let mut stores = make_stores().await;
        let mut proc = KStreamKTableJoinProcessor {
            table_store: "t".into(),
            joiner: |v: &i64, vt: Option<&i64>| v + vt.copied().unwrap_or(0),
            emit_on_miss: false, // inner
            _pd: PhantomData::<fn() -> (String, i64, i64, i64)>,
        };

        // hit: ("a", 1) — store has 10 → 1 + 10 = 11
        let hit = run_one(&mut proc, &mut stores, "a", 1).await;
        check!(hit == Some(11));

        // miss: ("b", 2) — store has no "b" → NOT forwarded
        let miss = run_one(&mut proc, &mut stores, "b", 2).await;
        check!(miss == None);
    }

    #[tokio::test]
    async fn left_join_hit_and_miss() {
        let mut stores = make_stores().await;
        let mut proc = KStreamKTableJoinProcessor {
            table_store: "t".into(),
            joiner: |v: &i64, vt: Option<&i64>| v + vt.copied().unwrap_or(0),
            emit_on_miss: true, // left
            _pd: PhantomData::<fn() -> (String, i64, i64, i64)>,
        };

        // hit: ("a", 1) → 1 + 10 = 11
        let hit = run_one(&mut proc, &mut stores, "a", 1).await;
        check!(hit == Some(11));

        // miss: ("b", 2) — no table entry → joiner gets None → 2 + 0 = 2
        let miss = run_one(&mut proc, &mut stores, "b", 2).await;
        check!(miss == Some(2));
    }

    /// A versioned store "vt" with key "a": value 10 at `valid_from=100` and
    /// value 20 at `valid_from=200`.
    async fn make_versioned_stores() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        let mut v = VersionedBytesStore::<String, i64>::in_memory(
            "vt".into(),
            secs(1_000),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-vt-changelog".into(),
        );
        v.put("a".to_string(), Some(10), 100).await;
        v.put("a".to_string(), Some(20), 200).await;
        stores.insert(Box::new(v));
        stores
    }

    async fn run_one_asof(
        proc: &mut KStreamKTableJoinAsOfProcessor<
            String,
            i64,
            i64,
            i64,
            impl Fn(&i64, Option<&i64>) -> i64 + Send + 'static,
        >,
        stores: &mut StoreRegistry,
        key: &str,
        value: i64,
        ts: i64,
    ) -> Option<i64> {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: ts,
        };
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
        let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
        proc.process(&mut ctx, Record::new(Some(key.to_string()), value, ts))
            .await;
        buffer
            .pop_front()
            .map(|(_, rec)| *rec.value.downcast::<i64>().unwrap())
    }

    #[tokio::test]
    async fn asof_inner_picks_version_at_record_ts() {
        let mut stores = make_versioned_stores().await;
        let mut proc = KStreamKTableJoinAsOfProcessor {
            table_store: "vt".into(),
            joiner: |v: &i64, vt: Option<&i64>| v + vt.copied().unwrap_or(0),
            emit_on_miss: false,
            _pd: PhantomData::<fn() -> (String, i64, i64, i64)>,
        };
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 150).await == Some(11));
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 250).await == Some(21));
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 50).await == None);
    }

    #[tokio::test]
    async fn asof_left_emits_none_on_miss() {
        let mut stores = make_versioned_stores().await;
        let mut proc = KStreamKTableJoinAsOfProcessor {
            table_store: "vt".into(),
            joiner: |v: &i64, vt: Option<&i64>| v + vt.copied().unwrap_or(0),
            emit_on_miss: true,
            _pd: PhantomData::<fn() -> (String, i64, i64, i64)>,
        };
        check!(run_one_asof(&mut proc, &mut stores, "a", 1, 50).await == Some(1));
    }
}
