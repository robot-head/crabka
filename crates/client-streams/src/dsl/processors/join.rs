//! `KStreamKTableJoinProcessor`: stream-table join (inner and left).
//!
//! - **Inner** (`emit_on_miss = false`): only forwards when the table has a
//!   matching value for the stream record's key.
//! - **Left** (`emit_on_miss = true`): always forwards; when the table has no
//!   entry the joiner receives `None` as the table-side value.
//!
//! The joiner signature is `Fn(&V, Option<&VT>) -> VO`, which covers both forms:
//! for inner joins the caller can assume `Some`; for left joins the caller handles
//! `None`.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Stream-table join processor.
///
/// Looks up the stream record's key in the named state store (which holds the
/// `KTable`'s materialized view). If a match is found — or `emit_on_miss` is
/// true — invokes `joiner(&stream_value, table_value)` and forwards the result.
#[allow(dead_code)]
pub(crate) struct KStreamKTableJoinProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    /// Joiner: `Fn(&V, Option<&VT>) -> VO`.
    /// - Inner join: called only when `table_value.is_some()`.
    /// - Left join: called always; `table_value` is `None` on a miss.
    pub joiner: F,
    /// `false` = inner join (skip on miss), `true` = left join (emit on miss).
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::marker::PhantomData;

    use assert2::check;

    use super::*;
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::RecordContext;
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::api::KeyValueStore;
    use crate::store::kv::KeyValueBytesStore;
    use crate::store::registry::StoreRegistry;

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
    /// forwarded `i64` output, or `None` if nothing was forwarded.
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
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: &mut output,
            record_ctx: &rc,
            stores,
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
}
