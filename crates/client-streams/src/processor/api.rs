//! The typed Processor API: `Processor`, `ProcessorSupplier`, and the
//! `ProcessorContext` users call `forward` on.

use std::{any::Any, marker::PhantomData};

use async_trait::async_trait;
use crabka_units::prelude::*;

use super::{
    erased::{Dispatch, ErasedRecord},
    record::{Record, RecordContext},
};

/// A stateless record processor. [`ProcessorSupplier::get`] creates one instance
/// per task. This mirrors
/// `org.apache.kafka.streams.processor.api.Processor`.
///
/// ## Lifecycle
///
/// The runtime calls `init` once before the first record and `close` once at
/// task shutdown. [`TopologyTestDriver`](crate::TopologyTestDriver) calls `init`
/// when it instantiates a topology for tests.
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

/// A boxed processor is itself a [`Processor`] and delegates to the inner value.
///
/// This lets a [`ProcessorSupplier`] closure return `Box<dyn Processor<…>>` when
/// the code chooses the concrete type at runtime. The boxed value still
/// satisfies the supplier blanket impl, which needs only *some* `Processor` as
/// the closure's return type. In the common case, return the concrete processor
/// directly with `|| MyProc` and skip the box.
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

/// Factory for [`Processor`] instances. It makes one instance per task, which
/// gives per-task isolation.
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

/// The context handed to [`Processor::process`]. `forward` boxes the record and
/// queues it for each child node, and the driver drains the queue.
///
/// There are two lifetimes. `'ctx` is the borrow of the `Dispatch` reference
/// itself. `'d` is the lifetime of the data inside `Dispatch`, such as the
/// buffers and the slices. Separate lifetimes avoid lifetime-invariance problems
/// when the code builds a `ProcessorContext` from a `&mut Dispatch<'d>` with an
/// independently-scoped outer borrow `'ctx`.
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

    /// Forward a record to all child nodes.
    ///
    /// The fan-out clones the record once per child. The last child receives the
    /// original by move, so the common single-child case makes no clone. This
    /// mirrors the JVM `ProcessorContext.forward(Record)`, which takes the
    /// record by value.
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

    /// Access a connected state store, typed. It returns `None` when the store
    /// is absent or the K and V types do not match. Fetch it once per record and
    /// do not hold it across `process` calls.
    pub fn get_state_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::api::KeyValueStore<K2, V2>> {
        self.dispatch.stores.get_kv::<K2, V2>(name)
    }

    /// Look up a value in a connected GLOBAL store, which is fully replicated
    /// and shared across tasks.
    ///
    /// The method returns an owned value. No borrow escapes the shared manager's
    /// lock, so the caller need not hold the lookup future across `forward`. It
    /// returns `None` on a miss or a type mismatch. Fetch it once per record and
    /// do not hold it across `process` calls.
    pub async fn global_get<GK: Send + Sync + 'static, VG: Send + 'static>(
        &mut self,
        store: &str,
        key: &GK,
    ) -> Option<VG> {
        self.dispatch.globals.get::<GK, VG>(store, key).await
    }

    /// Access a connected window store, typed. It returns `None` when the store
    /// is absent or the K and V types do not match. Fetch it once per record and
    /// do not hold it across `process` calls.
    pub fn get_window_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::window::WindowStore<K2, V2>> {
        self.dispatch.stores.get_window::<K2, V2>(name)
    }

    /// Access a connected join-window store (retainDuplicates), typed. It
    /// returns `None` when the store is absent or the K and V types do not
    /// match. Fetch it once per record and do not hold it across `process`
    /// calls.
    pub fn get_join_window_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::join_window::JoinWindowStore<K2, V2>> {
        self.dispatch.stores.get_join_window::<K2, V2>(name)
    }

    /// Access a connected session store, typed. It returns `None` when the store
    /// is absent or the K and V types do not match. Fetch it once per record and
    /// do not hold it across `process` calls.
    pub fn get_session_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::session::SessionStore<K2, V2>> {
        self.dispatch.stores.get_session::<K2, V2>(name)
    }

    /// Access a connected versioned store (KIP-889), typed. It returns `None`
    /// when the store is absent or the K and V types do not match. Fetch it once
    /// per record and do not hold it across `process` calls.
    pub fn get_versioned_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::versioned::VersionedKeyValueStore<K2, V2>> {
        self.dispatch.stores.get_versioned::<K2, V2>(name)
    }

    /// Access a connected suppress store, typed. It returns `None` when the
    /// store is absent or the K and V types do not match. Fetch it once per
    /// record and do not hold it across `process` calls.
    ///
    /// This method is `pub(crate)` for two reasons. The returned trait surfaces
    /// the crate-internal `Change<V>`, and the suppress store is a built-in DSL
    /// mechanism and not a user-facing store.
    pub(crate) fn get_suppress_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::suppress_store::SuppressStore<K2, V2>> {
        self.dispatch.stores.get_suppress::<K2, V2>(name)
    }

    /// Access a connected join-grace buffer store (KIP-923), typed. It returns
    /// `None` when the store is absent or the K and V types do not match. Fetch
    /// it once per record and do not hold it across `process` calls.
    ///
    /// This method is `pub(crate)` because the grace buffer is a built-in DSL
    /// mechanism, not a user-facing store. The stream-table join's grace-flush
    /// processor reaches it through the context.
    pub(crate) fn get_join_grace_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::join_grace_buffer::JoinGraceBufferStore<K2, V2>> {
        self.dispatch.stores.get_join_grace::<K2, V2>(name)
    }

    /// Access the connected FK subscription store. It returns `None` when the
    /// store is absent.
    ///
    /// This method is `pub(crate)` because the subscription store is an internal
    /// KIP-213 FK-join mechanism, not a user-facing store.
    pub(crate) fn get_fk_subscription_store(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::fk_subscription::SubscriptionBytesStore> {
        self.dispatch.stores.get_fk_subscription(name)
    }

    /// The metadata of the source record that this context processes now.
    #[must_use]
    pub fn record_context(&self) -> &RecordContext {
        self.dispatch.record_ctx
    }

    /// Whether the named KV state store is record-cached. When it is, this
    /// processor should suppress its immediate forward and let the cache flush
    /// forward the deduped change. The method returns false for an absent store,
    /// a non-KV store, and an uncached store.
    #[must_use]
    pub fn store_is_cached(&self, name: &str) -> bool {
        self.dispatch.stores.kv_is_cached(name)
    }

    /// Schedule a periodic [`Punctuator`]. You can call this from `init` or from
    /// `process`. `interval` must be positive. The method returns a
    /// [`Cancellable`] that stops the punctuator.
    ///
    /// [`Punctuator`]: crate::processor::punctuation::Punctuator
    /// [`Cancellable`]: crate::processor::punctuation::Cancellable
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn schedule<P>(
        &mut self,
        interval: std::time::Duration,
        ty: crate::processor::punctuation::PunctuationType,
        punctuator: P,
    ) -> crate::processor::punctuation::Cancellable
    where
        P: crate::processor::punctuation::Punctuator<KOut, VOut>,
    {
        use crate::processor::punctuation::PunctuationType;
        let interval = interval.as_time();
        assert!(
            interval >= millis(1),
            "schedule interval must be positive (>= 1ms)"
        );
        let base = match ty {
            PunctuationType::StreamTime => self.dispatch.sched_stream_time,
            PunctuationType::WallClockTime => self.dispatch.sched_wall_clock,
        };
        let next_time = base.saturating_add(interval.millis_i64());
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let erased: Box<dyn crate::processor::punctuation::ErasedPunctuator> =
            Box::new(crate::processor::punctuation::TypedPunctuator::<
                KOut,
                VOut,
                P,
            >::new(punctuator));
        self.dispatch
            .schedules
            .push(crate::processor::punctuation::ScheduleEntry {
                node_idx: self.dispatch.node_idx,
                interval,
                ty,
                next_time,
                punctuator: erased,
                cancel: cancel.clone(),
            });
        crate::processor::punctuation::Cancellable::new(cancel)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;
    use crate::processor::{
        erased::{Dispatch, ErasedRecord},
        record::{Record, RecordContext},
    };

    struct Upper;
    crate::impl_processor! {
        impl Upper: (String, String) -> (String, String) {
            async fn process(&mut self, ctx, r) {
                ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
            }
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
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &[],
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
        p.init(&mut ctx).await; // default no-op
        check!(ctx.record_context().timestamp == 9);
        ctx.forward(Record::new(None, "x".to_string(), 0)); // no children → dropped, no panic
        check!(buffer.is_empty());
        p.close().await; // default no-op
    }
}
