//! Fixed-key Processor API (KIP-820 `processValues`): a processor that may change
//! the value but NOT the key. A thin typed facade over the regular [`Processor`]
//! runtime. An internal adapter bridges a [`FixedKeyProcessor`] into a
//! `Processor<KIn, VIn, KIn, VOut>` (the output key type equals the input key
//! type, so the runtime keeps the partition assignment unchanged).
//!
//! The DSL surface `KStream::process_values` wraps a
//! [`FixedKeyProcessorSupplier`] in that adapter; this module is the
//! typed facade + adapter it builds on.

use std::any::Any;

use async_trait::async_trait;

use crate::processor::{
    api::{Processor, ProcessorContext},
    record::{Record, RecordContext},
};

/// A record whose KEY is immutable. [`with_value`](FixedKeyRecord::with_value)
/// changes the value (possibly the type), keeping the key and timestamp. Unlike
/// [`Record`], the key is **not** optional: a fixed-key processor is only reached
/// for records that carry a key (a null-key record cannot have its key
/// "preserved" — `process_values` requires a non-null key, matching the JVM).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedKeyRecord<K, V> {
    pub key: K,
    pub value: V,
    pub timestamp: i64,
}

impl<K, V> FixedKeyRecord<K, V> {
    /// Replace the value (and possibly its type), preserving key and timestamp.
    #[must_use]
    pub fn with_value<V2>(self, value: V2) -> FixedKeyRecord<K, V2> {
        FixedKeyRecord {
            key: self.key,
            value,
            timestamp: self.timestamp,
        }
    }
}

/// Handed to a [`FixedKeyProcessor::process`]. The only `forward` re-attaches the
/// (unchanged) key, so a fixed-key processor cannot accidentally repartition. All
/// other accessors delegate verbatim to the underlying [`ProcessorContext`].
pub struct FixedKeyProcessorContext<'a, 'ctx, 'd, K, VOut> {
    inner: &'a mut ProcessorContext<'ctx, 'd, K, VOut>,
}

impl<'a, 'ctx, 'd, K, VOut> FixedKeyProcessorContext<'a, 'ctx, 'd, K, VOut>
where
    K: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    // Constructed by FixedKeyAdapter (wired up by KStream::process_values).
    pub(crate) fn new(inner: &'a mut ProcessorContext<'ctx, 'd, K, VOut>) -> Self {
        Self { inner }
    }

    /// Forward a record downstream, re-attaching the (unchanged) key. Mirrors the
    /// JVM `FixedKeyProcessorContext.forward(FixedKeyRecord)`.
    pub fn forward(&mut self, record: FixedKeyRecord<K, VOut>) {
        self.inner.forward(Record::new(
            Some(record.key),
            record.value,
            record.timestamp,
        ));
    }

    /// Access a connected key/value state store, typed. Delegates to
    /// [`ProcessorContext::get_state_store`]. (Window/session-store accessors are
    /// omitted: no DSL path connects those to a `process_values` node yet.)
    pub fn get_state_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::api::KeyValueStore<K2, V2>> {
        self.inner.get_state_store::<K2, V2>(name)
    }

    /// Metadata of the source record currently being processed.
    #[must_use]
    pub fn record_context(&self) -> &RecordContext {
        self.inner.record_context()
    }
}

/// A fixed-key record processor (KIP-820): it may change the value but not the
/// key. The typed analogue of [`Processor`]; one instance is created per task via
/// [`FixedKeyProcessorSupplier::get`].
#[async_trait]
pub trait FixedKeyProcessor<KIn: Send, VIn: Send, VOut: Send>: Send + 'static {
    async fn process(
        &mut self,
        ctx: &mut FixedKeyProcessorContext<'_, '_, '_, KIn, VOut>,
        record: FixedKeyRecord<KIn, VIn>,
    );
}

/// A boxed fixed-key processor is itself a [`FixedKeyProcessor`], delegating to
/// the inner value. Mirrors the `Box<dyn Processor>` blanket impl in
/// [`crate::processor::api`]: it lets a [`FixedKeyProcessorSupplier`] closure
/// return `Box<dyn FixedKeyProcessor<…>>` (chosen at runtime) and still feed a
/// the internal fixed-key adapter, which requires `P: FixedKeyProcessor`.
#[async_trait]
impl<KIn, VIn, VOut> FixedKeyProcessor<KIn, VIn, VOut>
    for Box<dyn FixedKeyProcessor<KIn, VIn, VOut>>
where
    KIn: Send + 'static,
    VIn: Send + 'static,
    VOut: Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut FixedKeyProcessorContext<'_, '_, '_, KIn, VOut>,
        record: FixedKeyRecord<KIn, VIn>,
    ) {
        (**self).process(ctx, record).await;
    }
}

/// Factory for [`FixedKeyProcessor`] instances (one per task → per-task
/// isolation). Mirrors [`ProcessorSupplier`](crate::processor::ProcessorSupplier);
/// a closure `|| MyFixedProc` is a supplier via the blanket impl below.
pub trait FixedKeyProcessorSupplier<KIn, VIn, VOut>: Send + Sync + 'static {
    fn get(&self) -> Box<dyn FixedKeyProcessor<KIn, VIn, VOut>>;
}

impl<F, P, KIn, VIn, VOut> FixedKeyProcessorSupplier<KIn, VIn, VOut> for F
where
    F: Fn() -> P + Send + Sync + 'static,
    KIn: Send,
    VIn: Send,
    VOut: Send,
    P: FixedKeyProcessor<KIn, VIn, VOut>,
{
    fn get(&self) -> Box<dyn FixedKeyProcessor<KIn, VIn, VOut>> {
        Box::new(self())
    }
}

/// Bridges a [`FixedKeyProcessor`] into the regular [`Processor`] runtime with
/// `KOut = KIn` (the key is preserved). Wrapped around a [`FixedKeyProcessorSupplier`]
/// by [`KStream::process_values`](crate::dsl::kstream::KStream::process_values).
pub(crate) struct FixedKeyAdapter<P> {
    pub inner: P,
}

#[async_trait]
impl<P, KIn, VIn, VOut> Processor<KIn, VIn, KIn, VOut> for FixedKeyAdapter<P>
where
    KIn: Any + Send + Clone + 'static,
    VIn: Send + 'static,
    VOut: Any + Send + Clone + 'static,
    P: FixedKeyProcessor<KIn, VIn, VOut>,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KIn, VOut>,
        record: Record<KIn, VIn>,
    ) {
        let key = record.key.expect("process_values requires a non-null key");
        let fkr = FixedKeyRecord {
            key,
            value: record.value,
            timestamp: record.timestamp,
        };
        let mut fk_ctx = FixedKeyProcessorContext::new(ctx);
        self.inner.process(&mut fk_ctx, fkr).await;
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

    #[test]
    fn with_value_keeps_key_and_timestamp() {
        let r = FixedKeyRecord {
            key: "k".to_string(),
            value: 1i32,
            timestamp: 42,
        };
        let r2 = r.with_value("v".to_string());
        check!(r2.key == "k");
        check!(r2.value == "v");
        check!(r2.timestamp == 42);
    }

    /// A fixed-key processor that uppercases the value, preserving the key.
    struct UpperValue;
    crate::impl_fixed_key_processor! {
        impl UpperValue: (String, String) -> String {
            async fn process(&mut self, ctx, r) {
                let v = r.value.clone();
                ctx.forward(r.with_value(v.to_uppercase()));
            }
        }
    }

    #[tokio::test]
    async fn adapter_bridges_fixed_key_processor_preserving_key() {
        // Drive `UpperValue` through `FixedKeyAdapter` over a real
        // `ProcessorContext`, constructed exactly as the `api.rs` tests do.
        let mut adapter = FixedKeyAdapter { inner: UpperValue };
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
        adapter
            .process(&mut ctx, Record::new(Some("k".into()), "hi".into(), 5))
            .await;

        check!(buffer.len() == 1);
        let (child, rec) = buffer.pop_front().unwrap();
        check!(child == 1);
        // The key is preserved (Some("k")) and the value uppercased.
        let key = rec.key.expect("forwarded record must carry the key");
        check!(*key.downcast::<String>().unwrap() == "k");
        check!(*rec.value.downcast::<String>().unwrap() == "HI");
        check!(rec.timestamp == 5);
    }

    #[tokio::test]
    async fn supplier_blanket_impl_boxes_processor() {
        // A closure `|| UpperValue` is a `FixedKeyProcessorSupplier`; `get()`
        // returns a boxed processor that itself impls `FixedKeyProcessor`.
        let supplier = || UpperValue;
        let mut boxed = FixedKeyProcessorSupplier::get(&supplier);

        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 7,
        };
        let children = [2usize];
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
        let mut fk_ctx = FixedKeyProcessorContext::new(&mut ctx);

        // Drive the boxed processor directly to exercise the Box<dyn …> blanket impl.
        boxed
            .process(
                &mut fk_ctx,
                FixedKeyRecord {
                    key: "k".to_string(),
                    value: "lo".to_string(),
                    timestamp: 7,
                },
            )
            .await;

        check!(buffer.len() == 1);
        let (_child, rec) = buffer.pop_front().unwrap();
        check!(*rec.value.downcast::<String>().unwrap() == "LO");
    }
}
