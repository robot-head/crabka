//! Forward-suppression seam for materializing processors.
//!
//! This seam mirrors the JVM `TimestampedTupleForwarder`. When the backing store
//! is record-cached, the seam suppresses the immediate downstream forward,
//! because the cache flush forwards the deduped `Change`. When the store is not
//! cached, the processor forwards immediately.

use crate::{
    dsl::processors::change::Change,
    processor::{api::ProcessorContext, record::Record},
};

#[derive(Default)]
pub(crate) struct TupleForwarder {
    cached: bool,
}

impl TupleForwarder {
    /// Resolve from the owning store's cache state. Call this in the processor's
    /// `init`.
    pub(crate) fn resolve(cached: bool) -> Self {
        Self { cached }
    }

    /// Forward `Change::update(old,new)` unless the store is cached. When the
    /// store is cached, the cache flush forwards the deduped change.
    pub(crate) fn maybe_forward<K, VA>(
        &self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VA>>,
        key: K,
        old: Option<VA>,
        new: VA,
        ts: i64,
    ) where
        K: std::any::Any + Send + Clone,
        VA: std::any::Any + Send + Clone,
    {
        if self.cached {
            return;
        }
        ctx.forward(Record::new(Some(key), Change::update(old, new), ts));
    }

    /// Forward an already-computed `Change` unless the store is cached. The
    /// change can be a tombstone, that is, `new == None`.
    ///
    /// Processors such as `KTable.filter` and `KTable.mapValues` use this method,
    /// because their change can carry a tombstone. The store has already buffered
    /// the matching put or delete. When the store is cached, the cache flush
    /// forwards the deduped change instead.
    pub(crate) fn maybe_forward_change<K, VA>(
        &self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VA>>,
        key: K,
        change: Change<VA>,
        ts: i64,
    ) where
        K: std::any::Any + Send + Clone,
        VA: std::any::Any + Send + Clone,
    {
        if self.cached {
            return;
        }
        ctx.forward(Record::new(Some(key), change, ts));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;
    use crate::{
        processor::{
            erased::{Dispatch, ErasedRecord},
            record::RecordContext,
        },
        store::registry::StoreRegistry,
    };

    fn rc() -> RecordContext {
        RecordContext {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    /// Run `maybe_forward` once over a one-child dispatch and return how many
    /// records it enqueued.
    fn forwarded_count(forwarder: &TupleForwarder) -> usize {
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
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
        forwarder.maybe_forward(&mut ctx, "k".to_string(), Some(1i64), 2i64, 5);
        buffer.len()
    }

    #[test]
    fn uncached_forwards_one_record() {
        let f = TupleForwarder::resolve(false);
        check!(forwarded_count(&f) == 1);
    }

    #[test]
    fn cached_suppresses_forward() {
        let f = TupleForwarder::resolve(true);
        check!(forwarded_count(&f) == 0);
        // Default == uncached behavior preserved (forwards).
        check!(forwarded_count(&TupleForwarder::default()) == 1);
    }
}
