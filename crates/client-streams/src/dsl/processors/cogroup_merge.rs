//! `KStreamPassThrough<K, V>`: forwards every record unchanged. Used as the
//! KIP-150 cogroup merge node — it fans the per-input aggregate processors
//! (each forwarding `Change<VOut>`) into the single result `KTable` source.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::processor::{
    api::{Processor, ProcessorContext},
    record::Record,
};

/// Variance-neutral marker.
type Marker<T> = PhantomData<fn() -> T>;

#[allow(dead_code)]
pub(crate) struct KStreamPassThrough<K, V> {
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, V> for KStreamPassThrough<K, V>
where
    K: std::any::Any + Send + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        ctx.forward(r);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;
    use crate::processor::{
        erased::{Dispatch, ErasedRecord},
        record::RecordContext,
    };

    #[tokio::test]
    async fn passthrough_forwards_record_unchanged() {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let mut stores = crate::store::registry::StoreRegistry::default();
        let rc = RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 7,
        };
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
        let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
        let mut proc = KStreamPassThrough::<String, i64> { _pd: PhantomData };
        proc.process(&mut ctx, Record::new(Some("a".into()), 42, 7))
            .await;

        let (_, rec) = buffer.pop_front().expect("forwarded record");
        let v = rec.value.downcast::<i64>().unwrap();
        check!(*v == 42);
    }
}
