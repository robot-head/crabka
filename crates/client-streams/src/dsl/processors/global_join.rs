//! Stream-globaltable join: each stream record looks up the (fully-replicated)
//! global store by a key derived from the record, emitting `joiner(streamV,
//! globalV)` keyed by the stream's own key.
//!
//! - **Inner** (`emit_on_miss = false`): only forwards when the global store has a
//!   value for the derived lookup key.
//! - **Left** (`emit_on_miss = true`): always forwards; on a miss the joiner
//!   receives `None` for the global-side value.
//!
//! Unlike the stream-table join, the lookup key is derived from the record via a
//! `key_mapper(&streamKey, &streamValue) -> GK` rather than being the stream key
//! itself — the global table is fully replicated, so any record can look up any
//! key (no copartitioning, no repartition).

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::processor::{
    api::{Processor, ProcessorContext},
    record::Record,
};

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Stream-globaltable join processor.
///
/// For each stream record, computes the lookup key `gk = key_mapper(&k, &v)` and
/// reads the global key-value store. On a hit (or when `emit_on_miss`) it forwards
/// `joiner(&stream_value, global_value)` keyed by the **stream key** with the
/// stream timestamp.
pub(crate) struct KStreamGlobalTableJoinProcessor<K, V, GK, VG, VR, KM, J> {
    pub store_name: String,
    /// Lookup-key mapper: `Fn(&K, &V) -> GK`.
    pub key_mapper: KM,
    /// Joiner: `Fn(&V, Option<&VG>) -> VR`.
    /// - Inner join: called only when the global value is `Some`.
    /// - Left join: called always; `None` on a miss.
    pub joiner: J,
    /// `false` = inner join (skip on miss), `true` = left join (emit on miss).
    pub emit_on_miss: bool,
    pub _pd: Marker<(K, V, GK, VG, VR)>,
}

#[async_trait]
impl<K, V, GK, VG, VR, KM, J> Processor<K, V, K, VR>
    for KStreamGlobalTableJoinProcessor<K, V, GK, VG, VR, KM, J>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    GK: Send + Sync + 'static,
    VG: Send + 'static,
    VR: std::any::Any + Send + Clone,
    KM: Fn(&K, &V) -> GK + Send + 'static,
    J: Fn(&V, Option<&VG>) -> VR + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VR>, r: Record<K, V>) {
        let k = r.key.expect("global join requires a non-null stream key");
        let gk = (self.key_mapper)(&k, &r.value);
        // Read an owned value from the shared global manager: the lookup completes
        // (and drops the manager's lock) before `ctx.forward`, so no borrow/future
        // is held across the forward.
        let looked = ctx.global_get::<GK, VG>(&self.store_name, &gk).await;
        if looked.is_some() || self.emit_on_miss {
            let out = (self.joiner)(&r.value, looked.as_ref());
            ctx.forward(Record::new(Some(k), out, r.timestamp));
        }
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
            serde::{Consumed, StringSerde},
        },
        runtime::global::GlobalStateManager,
        store::{backend::StoreBackend, registry::StoreRegistry},
        topology::{NodeHandle, Topology},
    };

    /// Build a shared `GlobalStateManager` holding a global
    /// `KeyValueBytesStore<String,String>` named `"g-store"` pre-seeded with
    /// `("v", "gv")` (so a `key_mapper` of `|_k, v| v.clone()` finds it for a
    /// record whose value is `"v"`). The store comes from the real
    /// `add_global_store` → `build` path; the seed value is injected via `put`.
    async fn make_globals() -> GlobalStateManager {
        let mut t = Topology::new();
        t.add_global_store::<String, String, _, _>(
            "g-store",
            "g-src",
            "g-topic",
            "g-proc",
            Consumed::with(StringSerde, StringSerde),
        );
        // A topology needs a non-global source/sink to build (global is invisible).
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        let built = t.build("app").unwrap();
        let globals = GlobalStateManager::build(
            built.global_store_factories(),
            built.global_store_topics(),
            &StoreBackend::InMemory,
            "app",
        )
        .await;
        globals
            .put("g-store", "v".to_string(), "gv".to_string())
            .await;
        globals
    }

    /// Drive one `(key, value)` record through `proc` and return the forwarded
    /// `String` output, or `None` if nothing was forwarded.
    async fn run_one(
        proc: &mut KStreamGlobalTableJoinProcessor<
            String,
            String,
            String,
            String,
            String,
            impl Fn(&String, &String) -> String + Send + 'static,
            impl Fn(&String, Option<&String>) -> String + Send + 'static,
        >,
        globals: &GlobalStateManager,
        key: &str,
        value: &str,
    ) -> Option<String> {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let mut stores = StoreRegistry::default();
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
            stores: &mut stores,
            globals,
            node_idx: 0,
            schedules: &mut scheds,
            sched_stream_time: i64::MIN,
            sched_wall_clock: 0,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut dispatch);
        proc.process(
            &mut ctx,
            Record::new(Some(key.to_string()), value.to_string(), 0),
        )
        .await;
        buffer
            .pop_front()
            .map(|(_, rec)| *rec.value.downcast::<String>().unwrap())
    }

    /// Inner join: the lookup key is derived from the record value (NOT the
    /// stream key), the stream key is preserved on the output, and a miss drops
    /// the record.
    #[tokio::test]
    async fn inner_join_hit_uses_derived_key_and_keeps_stream_key() {
        let globals = make_globals().await;
        let mut stores = StoreRegistry::default();
        let mut proc = KStreamGlobalTableJoinProcessor {
            store_name: "g-store".into(),
            // lookup key = the record value, not the stream key
            key_mapper: |_k: &String, v: &String| v.clone(),
            joiner: |v: &String, vg: Option<&String>| {
                format!("{v}+{}", vg.cloned().unwrap_or_else(|| "<none>".into()))
            },
            emit_on_miss: false, // inner
            _pd: PhantomData::<fn() -> (String, String, String, String, String)>,
        };

        // hit: stream ("k", "v") → gk = "v" → store has "gv" → "v+gv", key "k"
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 7,
        };
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
        let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut dispatch);
        proc.process(
            &mut ctx,
            Record::new(Some("k".to_string()), "v".to_string(), 7),
        )
        .await;
        let (_child, rec) = buffer.pop_front().expect("inner hit should forward");
        check!(
            (
                *rec.key.unwrap().downcast::<String>().unwrap(),
                *rec.value.downcast::<String>().unwrap(),
                rec.timestamp,
            ) == ("k".to_string(), "v+gv".to_string(), 7)
        );
    }

    #[tokio::test]
    async fn inner_join_miss_drops() {
        let globals = make_globals().await;
        let mut proc = KStreamGlobalTableJoinProcessor {
            store_name: "g-store".into(),
            key_mapper: |_k: &String, v: &String| v.clone(),
            joiner: |v: &String, vg: Option<&String>| {
                format!("{v}+{}", vg.cloned().unwrap_or_else(|| "<none>".into()))
            },
            emit_on_miss: false, // inner
            _pd: PhantomData::<fn() -> (String, String, String, String, String)>,
        };

        // miss: ("k", "absent") → gk = "absent" → not in store → dropped
        let out = run_one(&mut proc, &globals, "k", "absent").await;
        check!(out == None);
    }

    #[tokio::test]
    async fn left_join_emits_on_miss_with_none() {
        let globals = make_globals().await;
        let mut proc = KStreamGlobalTableJoinProcessor {
            store_name: "g-store".into(),
            key_mapper: |_k: &String, v: &String| v.clone(),
            joiner: |v: &String, vg: Option<&String>| {
                format!("{v}+{}", vg.cloned().unwrap_or_else(|| "<none>".into()))
            },
            emit_on_miss: true, // left
            _pd: PhantomData::<fn() -> (String, String, String, String, String)>,
        };

        // hit still works
        let hit = run_one(&mut proc, &globals, "k", "v").await;
        check!(hit == Some("v+gv".to_string()));

        // miss: joiner receives None → "absent+<none>"
        let miss = run_one(&mut proc, &globals, "k", "absent").await;
        check!(miss == Some("absent+<none>".to_string()));
    }
}
