//! The typed Processor API: `Processor`, `ProcessorSupplier`, and the
//! `ProcessorContext` users call `forward` on.

use std::any::Any;
use std::marker::PhantomData;

use async_trait::async_trait;

use super::erased::{Dispatch, ErasedRecord};
use super::record::{Record, RecordContext};

/// A stateless record processor. One instance is created per task via
/// [`ProcessorSupplier::get`]. Mirrors `org.apache.kafka.streams.processor.api.Processor`.
///
/// ## Lifecycle
///
/// `init` is invoked once before the first record and `close` once at task
/// shutdown — **by the runtime (`KafkaStreams`, sub-project #2b)**. The
/// [`TopologyTestDriver`](crate::TopologyTestDriver) (#2a) does **not** yet
/// invoke `init`/`close`; keep processors usable as-constructed for now.
#[async_trait]
pub trait Processor<KIn: Send, VIn: Send, KOut: Send, VOut: Send>: Send + 'static {
    async fn init(&mut self, _ctx: &mut ProcessorContext<'_, '_, KOut, VOut>) {}
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KOut, VOut>,
        record: Record<KIn, VIn>,
    );
    async fn close(&mut self) {}
}

/// A boxed processor is itself a [`Processor`], delegating to the inner value.
///
/// This is what lets a [`ProcessorSupplier`] closure return `Box<dyn
/// Processor<…>>` when the concrete type is chosen at runtime: the boxed value
/// still satisfies the supplier blanket impl (which only requires the closure's
/// return type to be *some* `Processor`). For the common case, return the
/// concrete processor directly (`|| MyProc`) and skip the box entirely.
#[async_trait]
impl<KIn, VIn, KOut, VOut> Processor<KIn, VIn, KOut, VOut>
    for Box<dyn Processor<KIn, VIn, KOut, VOut>>
where
    KIn: Send + 'static,
    VIn: Send + 'static,
    KOut: Send + 'static,
    VOut: Send + 'static,
{
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, KOut, VOut>) {
        (**self).init(ctx).await;
    }
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KOut, VOut>,
        record: Record<KIn, VIn>,
    ) {
        (**self).process(ctx, record).await;
    }
    async fn close(&mut self) {
        (**self).close().await;
    }
}

/// Factory for [`Processor`] instances (one per task → per-task isolation).
pub trait ProcessorSupplier<KIn, VIn, KOut, VOut>: Send + Sync + 'static {
    fn get(&self) -> Box<dyn Processor<KIn, VIn, KOut, VOut>>;
}

// Blanket impl so a closure `|| MyProc` is a supplier. The closure returns a
// *concrete* `P: Processor`, which we box. Because `P` is concrete, the four KV
// type parameters are inferred from `P`'s single `Processor` impl — callers
// never annotate them. A closure returning `Box<dyn Processor<…>>` also works
// (the boxed value is itself a `Processor`, see the impl above), covering the
// rarer case of picking the concrete processor type at runtime.
impl<F, P, KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> for F
where
    F: Fn() -> P + Send + Sync + 'static,
    KIn: Send,
    VIn: Send,
    KOut: Send,
    VOut: Send,
    P: Processor<KIn, VIn, KOut, VOut>,
{
    fn get(&self) -> Box<dyn Processor<KIn, VIn, KOut, VOut>> {
        Box::new(self())
    }
}

/// Handed to [`Processor::process`]. `forward` boxes the record and queues it
/// for each child node (the driver drains the queue).
///
/// Two lifetimes: `'ctx` is the borrow of the `Dispatch` reference itself;
/// `'d` is the lifetime of the data inside `Dispatch` (buffers, slices, etc.).
/// Keeping them separate avoids lifetime-invariance issues when constructing a
/// `ProcessorContext` from a `&mut Dispatch<'d>` with an independently-scoped
/// outer borrow `'ctx`.
pub struct ProcessorContext<'ctx, 'd, KOut, VOut> {
    dispatch: &'ctx mut Dispatch<'d>,
    _pd: PhantomData<fn(KOut, VOut)>,
}

impl<'ctx, 'd, KOut, VOut> ProcessorContext<'ctx, 'd, KOut, VOut>
where
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    pub(crate) fn new(dispatch: &'ctx mut Dispatch<'d>) -> Self {
        Self {
            dispatch,
            _pd: PhantomData,
        }
    }

    /// Forward a record to all child nodes. The record is cloned per child for
    /// fan-out; the last child receives the original by move (so the common
    /// single-child case performs zero clones). Mirrors the JVM
    /// `ProcessorContext.forward(Record)`, which takes the record by value.
    pub fn forward(&mut self, record: Record<KOut, VOut>) {
        // Copy the child-slice reference out so we can mutably borrow `buffer`.
        let children = self.dispatch.children;
        let Some((&last, rest)) = children.split_last() else {
            return; // no children — drop the record
        };
        for &child in rest {
            let key: Option<Box<dyn Any + Send>> = record
                .key
                .clone()
                .map(|k| Box::new(k) as Box<dyn Any + Send>);
            let value: Box<dyn Any + Send> = Box::new(record.value.clone());
            self.dispatch
                .buffer
                .push_back((child, ErasedRecord::new(key, value, record.timestamp)));
        }
        let ts = record.timestamp;
        let key: Option<Box<dyn Any + Send>> =
            record.key.map(|k| Box::new(k) as Box<dyn Any + Send>);
        let value: Box<dyn Any + Send> = Box::new(record.value);
        self.dispatch
            .buffer
            .push_back((last, ErasedRecord::new(key, value, ts)));
    }

    /// Access a connected state store, typed. `None` if absent or the K/V types
    /// don't match. Fetch it per-record (do not hold across `process` calls).
    pub fn get_state_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::api::KeyValueStore<K2, V2>> {
        self.dispatch.stores.get_kv::<K2, V2>(name)
    }

    /// Access a connected window store, typed. `None` if absent or the K/V types
    /// don't match. Fetch it per-record (do not hold across `process` calls).
    pub fn get_window_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::window::WindowStore<K2, V2>> {
        self.dispatch.stores.get_window::<K2, V2>(name)
    }

    /// Access a connected join-window store (retainDuplicates), typed. `None` if
    /// absent or the K/V types don't match. Fetch it per-record (do not hold
    /// across `process` calls).
    pub fn get_join_window_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::join_window::JoinWindowStore<K2, V2>> {
        self.dispatch.stores.get_join_window::<K2, V2>(name)
    }

    /// Metadata of the source record currently being processed.
    #[must_use]
    pub fn record_context(&self) -> &RecordContext {
        self.dispatch.record_ctx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use assert2::check;
    use std::collections::VecDeque;

    struct Upper;
    #[async_trait]
    impl Processor<String, String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    struct Noop;
    #[async_trait]
    impl Processor<String, String, String, String> for Noop {
        async fn process(
            &mut self,
            _ctx: &mut ProcessorContext<'_, '_, String, String>,
            _r: Record<String, String>,
        ) {
        }
    }

    #[tokio::test]
    async fn forward_pushes_erased_record_to_each_child() {
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 5,
        };
        let children = [3usize, 4usize];
        let mut stores = crate::store::registry::StoreRegistry::default();
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: &mut output,
            record_ctx: &rc,
            stores: &mut stores,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut dispatch);
        Upper
            .process(&mut ctx, Record::new(Some("k".into()), "hi".into(), 5))
            .await;
        check!(buffer.len() == 2);
        let (child, rec) = buffer.pop_front().unwrap();
        check!(child == 3);
        check!(*rec.value.downcast::<String>().unwrap() == "HI");
    }

    #[tokio::test]
    async fn boxed_dyn_processor_delegates_init_process_close() {
        // A `Box<dyn Processor>` is itself a `Processor`, forwarding every method
        // to the inner value. This is the runtime-dispatch path a
        // `ProcessorSupplier` closure takes when it returns `Box<dyn Processor<…>>`
        // instead of a concrete processor.
        let mut boxed: Box<dyn Processor<String, String, String, String>> = Box::new(Upper);
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 5,
        };
        let children = [1usize];
        let mut stores = crate::store::registry::StoreRegistry::default();
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: &mut output,
            record_ctx: &rc,
            stores: &mut stores,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut dispatch);
        boxed.init(&mut ctx).await; // forwards to Upper's default no-op
        boxed
            .process(&mut ctx, Record::new(None, "hi".into(), 5))
            .await; // forwards → uppercases
        boxed.close().await; // forwards to Upper's default no-op
        check!(buffer.len() == 1);
        let (_child, rec) = buffer.pop_front().unwrap();
        check!(*rec.value.downcast::<String>().unwrap() == "HI");
    }

    #[tokio::test]
    async fn default_init_and_close_are_noops_and_forward_with_no_children_drops() {
        let mut p = Noop;
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 9,
        };
        let mut stores = crate::store::registry::StoreRegistry::default();
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &[],
            output: &mut output,
            record_ctx: &rc,
            stores: &mut stores,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, String>::new(&mut dispatch);
        p.init(&mut ctx).await; // default no-op
        check!(ctx.record_context().timestamp == 9);
        ctx.forward(Record::new(None, "x".to_string(), 0)); // no children → dropped, no panic
        check!(buffer.is_empty());
        p.close().await; // default no-op
    }
}
