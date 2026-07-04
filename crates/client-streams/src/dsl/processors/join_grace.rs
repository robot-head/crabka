//! KIP-923 stream–table join grace processor. Buffers each stream record into a
//! `JoinGraceBufferStore`, advances `observed_stream_time`, then drains every
//! buffered record with `bufTs <= streamTime - grace_ms` in ascending `(ts, seq)`
//! order — performing the versioned as-of join (`get_as_of(key, bufTs)`) at drain
//! time and forwarding at `bufTs`. A record already late on arrival drains in the
//! same pass. Inner skips on miss; left passes `None`.
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::processor::{
    api::{Processor, ProcessorContext},
    record::Record,
};

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// Stream–table join grace-flush processor (KIP-923).
///
/// Each incoming stream record is appended to the named [`JoinGraceBufferStore`]
/// rather than joined immediately, and `observed_stream_time` is advanced to the
/// max record timestamp seen. The processor then drains every buffered record
/// whose timestamp is `<= observed_stream_time - grace_ms` — in ascending
/// `(ts, seq)` order — and joins each one against the versioned table *as of its
/// own buffer timestamp*, forwarding at that timestamp. A record that is already
/// past the grace horizon on arrival drains in the same `process` call.
///
/// `emit_on_miss = false` is an inner join (skip on a null as-of result);
/// `emit_on_miss = true` is a left join (the joiner receives `None`).
///
/// [`JoinGraceBufferStore`]: crate::store::join_grace_buffer::JoinGraceBufferStore
pub(crate) struct KStreamKTableJoinGraceProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    pub buffer_store: String,
    pub grace_ms: i64,
    pub joiner: F,
    pub emit_on_miss: bool,
    pub observed_stream_time: i64,
    pub _pd: Marker<(K, V, VT, VO)>,
}

#[async_trait]
impl<K, V, VT, VO, F> Processor<K, V, K, VO> for KStreamKTableJoinGraceProcessor<K, V, VT, VO, F>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Sync + Clone,
    VT: Send + Sync + 'static,
    VO: std::any::Any + Send + Clone,
    F: Fn(&V, Option<&VT>) -> VO + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, V>) {
        let key = r.key.expect("join requires a non-null key");
        self.observed_stream_time = self.observed_stream_time.max(r.timestamp);
        {
            // buffer is always connected by add_join_grace_store
            let buf = ctx
                .get_join_grace_store::<K, V>(&self.buffer_store)
                .expect("join grace buffer store not found");
            buf.put(key, r.value, r.timestamp).await;
        }
        let threshold = self.observed_stream_time - self.grace_ms;
        let due = {
            // buffer is always connected by add_join_grace_store
            let buf = ctx
                .get_join_grace_store::<K, V>(&self.buffer_store)
                .expect("join grace buffer store not found");
            buf.drain_due(threshold).await
        };
        for (k, v, ts) in due {
            let vt = match ctx.get_versioned_store::<K, VT>(&self.table_store) {
                Some(s) => s.get_as_of(&k, ts).await.map(|rec| rec.value),
                None => None,
            };
            if vt.is_some() || self.emit_on_miss {
                let out = (self.joiner)(&v, vt.as_ref());
                ctx.forward(Record::new(Some(k), out, ts));
            }
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
            serde::{I64Serde, StringSerde},
        },
        store::{
            registry::StoreRegistry,
            versioned::{VersionedBytesStore, VersionedKeyValueStore},
        },
    };

    /// Registry holding BOTH stores the grace processor reads:
    /// - versioned table "vt": `(a, 10)@100`, `(a, 20)@200`.
    /// - empty grace buffer "buf".
    async fn make_stores() -> StoreRegistry {
        let mut stores = StoreRegistry::default();

        let mut vt = VersionedBytesStore::<String, i64>::in_memory(
            "vt".into(),
            1_000_000,
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-vt-changelog".into(),
        );
        vt.put("a".to_string(), Some(10), 100).await;
        vt.put("a".to_string(), Some(20), 200).await;
        stores.insert(Box::new(vt));

        let buf = crate::store::join_grace_buffer::JoinGraceBufferStore::<String, i64>::in_memory(
            "buf".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-buf-changelog".into(),
        );
        stores.insert(Box::new(buf));

        stores
    }

    /// Drive one `(key, value)@ts` record through `proc` against the shared
    /// `stores`, returning every forwarded `(value, ts)` from THIS call.
    async fn run_one(
        proc: &mut KStreamKTableJoinGraceProcessor<
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
    ) -> Vec<(i64, i64)> {
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
            .drain(..)
            .map(|(_, rec)| (*rec.value.downcast::<i64>().unwrap(), rec.timestamp))
            .collect()
    }

    #[tokio::test]
    async fn grace_drains_in_order_with_asof() {
        // grace=50. versioned vt: (a,10)@100, (a,20)@200. joiner = v + table.
        let mut stores = make_stores().await;
        let mut proc = KStreamKTableJoinGraceProcessor {
            table_store: "vt".into(),
            buffer_store: "buf".into(),
            grace_ms: 50,
            joiner: |v: &i64, vt: Option<&i64>| v + vt.copied().unwrap_or(0),
            emit_on_miss: false, // inner
            observed_stream_time: i64::MIN,
            _pd: PhantomData::<fn() -> (String, i64, i64, i64)>,
        };

        let mut all: Vec<(i64, i64)> = Vec::new();

        // Record A: (a,1)@150 -> stream_time=150, threshold=100.
        //   A@150: 150 > 100 -> stays buffered. No output.
        all.extend(run_one(&mut proc, &mut stores, "a", 1, 150).await);

        // Record B: (a,1)@260 -> stream_time=260, threshold=210. Drain bufTs<=210:
        //   A@150 due: get_as_of(150)=10 -> 1+10=11 @150.
        //   B@260: 260 > 210 -> stays. Forwards [(11,150)].
        all.extend(run_one(&mut proc, &mut stores, "a", 1, 260).await);

        // Record C: (a,1)@300 -> stream_time=300, threshold=250. Drain bufTs<=250:
        //   B@260: 260 > 250 -> stays. C@300 stays. Forwards [].
        all.extend(run_one(&mut proc, &mut stores, "a", 1, 300).await);

        // Record D: (a,1)@320 -> stream_time=320, threshold=270. Drain bufTs<=270:
        //   B@260 due: get_as_of(260)=20 -> 1+20=21 @260. C@300, D@320 stay.
        //   Forwards [(21,260)].
        all.extend(run_one(&mut proc, &mut stores, "a", 1, 320).await);

        // Cumulative, ordered, as-of: A drained at B's pass (table=10 as of 150),
        // B drained at D's pass (table=20 as of 260).
        check!(all == vec![(11, 150), (21, 260)]);
    }
}
