//! The typed Processor API: `Processor`, `ProcessorSupplier`, and the
//! `ProcessorContext` users call `forward` on.

use std::{any::Any, marker::PhantomData};

use async_trait::async_trait;

use super::{
    erased::{Dispatch, ErasedRecord},
    record::{Record, RecordContext},
};

/// A stateless record processor. One instance is created per task via
/// [`ProcessorSupplier::get`]. Mirrors `org.apache.kafka.streams.processor.api.Processor`.
///
/// ## Lifecycle
///
/// The runtime invokes `init` once before the first record and `close` once at
/// task shutdown. [`TopologyTestDriver`](crate::TopologyTestDriver) invokes
/// `init` when it instantiates a topology for tests.
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

    /// Look up a value in a connected GLOBAL store (fully-replicated, shared across
    /// tasks). Returns an owned value — no borrow escapes the shared manager's lock,
    /// so the lookup future need not be held across `forward`. `None` on miss /
    /// type mismatch. Fetch it per-record (do not hold across `process` calls).
    pub async fn global_get<GK: Send + Sync + 'static, VG: Send + 'static>(
        &mut self,
        store: &str,
        key: &GK,
    ) -> Option<VG> {
        self.dispatch.globals.get::<GK, VG>(store, key).await
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

    /// Access a connected session store, typed. `None` if absent or the K/V types
    /// don't match. Fetch it per-record (do not hold across `process` calls).
    pub fn get_session_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::session::SessionStore<K2, V2>> {
        self.dispatch.stores.get_session::<K2, V2>(name)
    }

    /// Access a connected versioned store (KIP-889), typed. `None` if absent or
    /// the K/V types don't match. Fetch it per-record (do not hold across
    /// `process` calls).
    pub fn get_versioned_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::versioned::VersionedKeyValueStore<K2, V2>> {
        self.dispatch.stores.get_versioned::<K2, V2>(name)
    }

    /// Access a connected suppress store, typed. `None` if absent or the K/V types
    /// don't match. Fetch it per-record (do not hold across `process` calls).
    ///
    /// `pub(crate)`: the returned trait surfaces `Change<V>` (crate-internal) and
    /// the suppress store is a built-in DSL mechanism, not a user-facing store.
    pub(crate) fn get_suppress_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::suppress_store::SuppressStore<K2, V2>> {
        self.dispatch.stores.get_suppress::<K2, V2>(name)
    }

    /// Access a connected join-grace buffer store (KIP-923), typed. `None` if
    /// absent or the K/V types don't match. Fetch it per-record (do not hold
    /// across `process` calls).
    ///
    /// `pub(crate)`: the grace buffer is a built-in DSL mechanism the stream–table
    /// join's grace-flush processor reaches via the context, not a user-facing
    /// store.
    pub(crate) fn get_join_grace_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::join_grace_buffer::JoinGraceBufferStore<K2, V2>> {
        self.dispatch.stores.get_join_grace::<K2, V2>(name)
    }

    /// Access the connected FK subscription store. `None` if absent.
    ///
    /// `pub(crate)`: the subscription store is an internal KIP-213 FK-join
    /// mechanism, not a user-facing store.
    pub(crate) fn get_fk_subscription_store(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::fk_subscription::SubscriptionBytesStore> {
        self.dispatch.stores.get_fk_subscription(name)
    }

    /// Metadata of the source record currently being processed.
    #[must_use]
    pub fn record_context(&self) -> &RecordContext {
        self.dispatch.record_ctx
    }

    /// Whether the named KV state store is record-cached (so this processor should
    /// suppress its immediate forward and let the cache flush forward the deduped
    /// change). False for absent/non-KV/uncached stores.
    #[must_use]
    pub fn store_is_cached(&self, name: &str) -> bool {
        self.dispatch.stores.kv_is_cached(name)
    }

    /// Schedule a periodic [`Punctuator`]. Callable from `init` or `process`.
    /// `interval` must be positive. Returns a [`Cancellable`] to stop it.
    ///
    /// [`Punctuator`]: crate::processor::punctuation::Punctuator
    /// [`Cancellable`]: crate::processor::punctuation::Cancellable
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
        let interval_ms = i64::try_from(interval.as_millis()).unwrap_or(i64::MAX);
        assert2::assert!(interval_ms >= 1);
        let base = match ty {
            PunctuationType::StreamTime => self.dispatch.sched_stream_time,
            PunctuationType::WallClockTime => self.dispatch.sched_wall_clock,
        };
        let next_time = base.saturating_add(interval_ms);
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
                interval_ms,
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
        let actual = buffer
            .into_iter()
            .map(|(child, rec)| (child, *rec.value.downcast::<String>().unwrap()))
            .collect::<Vec<_>>();
        check!(actual == vec![(3, "HI".to_string()), (4, "HI".to_string())]);
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
        let actual_len = buffer.len();
        let (_child, rec) = buffer.pop_front().unwrap();
        check!((actual_len, *rec.value.downcast::<String>().unwrap()) == (1, "HI".to_string()));
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
