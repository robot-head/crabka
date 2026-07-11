//! Type-erased node adapters: `ProcessorNode`, `SinkNode`, `SourceNode`.
//!
//! Each adapter carries the `TypeId` of the `(K, V)` pairs it consumes and/or
//! produces so graph construction can validate wiring without keeping the
//! concrete type parameters in scope.
//!
//! The three roles:
//! - [`ProcessorNode`] — downcasts `ErasedRecord`, runs the user-supplied
//!   [`Processor`], and any `forward` calls box the output back into
//!   `ErasedRecord` entries in the dispatch buffer.
//! - [`SinkNode`] — downcasts `ErasedRecord` and serializes it to
//!   [`OutputRecord`] bytes (no children).
//! - [`SourceNode`] — deserializes raw bytes into `ErasedRecord`; it is
//!   entered by the graph driver directly via `deserialize`, not as a child
//!   target of another node.

use std::any::{Any, type_name};

use async_trait::async_trait;

use super::{
    api::{Processor, ProcessorContext, ProcessorSupplier},
    erased::{Dispatch, ErasedRecord, OutputRecord, ProcessorError},
    record::Record,
    serde::Serde,
};

// ──────────────────────────────────────────────────────────────────────────────
// ErasedNode trait
// ──────────────────────────────────────────────────────────────────────────────

/// Object-safe trait for a node slot in the execution graph.
///
/// Implemented by [`ProcessorNode`] and [`SinkNode`].  [`SourceNode`] does
/// **not** implement this trait because sources are entered via their own
/// `deserialize` method — they are never the target of a `forward` from a
/// parent node.
#[async_trait]
pub(crate) trait ErasedNode: Send {
    /// Called once before the first record (e.g. to open stores). Default is
    /// a no-op so sink nodes don't need to implement it.
    #[allow(dead_code)]
    async fn init(&mut self, _dispatch: &mut Dispatch<'_>) -> Result<(), ProcessorError> {
        Ok(())
    }

    /// Called once at task shutdown. Default is a no-op.
    async fn close(&mut self) {}

    /// Process one erased record: downcast, run inner logic, push results.
    async fn process(
        &mut self,
        dispatch: &mut Dispatch<'_>,
        record: ErasedRecord,
    ) -> Result<(), ProcessorError>;
}

// ──────────────────────────────────────────────────────────────────────────────
// ProcessorNode
// ──────────────────────────────────────────────────────────────────────────────

/// Wraps a user [`Processor`] and handles type-erasure at both the input
/// (downcast) and output (`ProcessorContext::forward` re-boxes).
pub(crate) struct ProcessorNode<KIn, VIn, KOut, VOut> {
    name: String,
    inner: Box<dyn Processor<KIn, VIn, KOut, VOut>>,
}

impl<KIn, VIn, KOut, VOut> ProcessorNode<KIn, VIn, KOut, VOut>
where
    KIn: Any + Send,
    VIn: Any + Send,
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    pub(crate) fn new(
        name: String,
        supplier: &impl ProcessorSupplier<KIn, VIn, KOut, VOut>,
    ) -> Self {
        Self {
            name,
            inner: supplier.get(),
        }
    }
}

#[async_trait]
impl<KIn, VIn, KOut, VOut> ErasedNode for ProcessorNode<KIn, VIn, KOut, VOut>
where
    KIn: Any + Send,
    VIn: Any + Send,
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    async fn init(&mut self, dispatch: &mut Dispatch<'_>) -> Result<(), ProcessorError> {
        let mut ctx = ProcessorContext::<'_, '_, KOut, VOut>::new(dispatch);
        self.inner.init(&mut ctx).await;
        Ok(())
    }

    async fn close(&mut self) {
        self.inner.close().await;
    }

    async fn process(
        &mut self,
        dispatch: &mut Dispatch<'_>,
        rec: ErasedRecord,
    ) -> Result<(), ProcessorError> {
        // Downcast the value (required).
        let value = *rec
            .value
            .downcast::<VIn>()
            .map_err(|_| ProcessorError::Downcast {
                node: self.name.clone(),
                expected: type_name::<VIn>(),
            })?;

        // Downcast the key (optional — None key is valid).
        let key: Option<KIn> = match rec.key {
            None => None,
            Some(boxed) => {
                let k = *boxed
                    .downcast::<KIn>()
                    .map_err(|_| ProcessorError::Downcast {
                        node: self.name.clone(),
                        expected: type_name::<KIn>(),
                    })?;
                Some(k)
            }
        };

        let record = Record::new(key, value, rec.timestamp);
        let mut ctx = ProcessorContext::<'_, '_, KOut, VOut>::new(dispatch);
        self.inner.process(&mut ctx, record).await;
        Ok(())
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// SinkNode
// ──────────────────────────────────────────────────────────────────────────────

/// Deserializes an [`ErasedRecord`] and pushes the resulting bytes to
/// `Dispatch::output`. This is a terminal node — it has no children.
pub(crate) struct SinkNode<K, V, KS, VS> {
    name: String,
    topic: String,
    key_serde: KS,
    value_serde: VS,
    _pd: std::marker::PhantomData<fn(K, V)>,
}

impl<K, V, KS, VS> SinkNode<K, V, KS, VS>
where
    K: Any + Send,
    V: Any + Send,
    KS: Serde<K>,
    VS: Serde<V>,
{
    pub(crate) fn new(name: String, topic: String, key_serde: KS, value_serde: VS) -> Self {
        Self {
            name,
            topic,
            key_serde,
            value_serde,
            _pd: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<K, V, KS, VS> ErasedNode for SinkNode<K, V, KS, VS>
where
    K: Any + Send,
    V: Any + Send,
    KS: Serde<K> + Send,
    VS: Serde<V> + Send,
{
    async fn process(
        &mut self,
        dispatch: &mut Dispatch<'_>,
        rec: ErasedRecord,
    ) -> Result<(), ProcessorError> {
        // Downcast value.
        let value = *rec
            .value
            .downcast::<V>()
            .map_err(|_| ProcessorError::Downcast {
                node: self.name.clone(),
                expected: type_name::<V>(),
            })?;

        // Downcast key (optional).
        let key: Option<K> = match rec.key {
            None => None,
            Some(boxed) => {
                let k = *boxed
                    .downcast::<K>()
                    .map_err(|_| ProcessorError::Downcast {
                        node: self.name.clone(),
                        expected: type_name::<K>(),
                    })?;
                Some(k)
            }
        };

        let key_bytes = key
            .as_ref()
            .map(|k| self.key_serde.serialize(&self.topic, k));
        let value_bytes = Some(self.value_serde.serialize(&self.topic, &value));

        dispatch.output.push(OutputRecord {
            topic: self.topic.clone(),
            key: key_bytes,
            value: value_bytes,
            timestamp: rec.timestamp,
        });

        Ok(())
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// SourceNode
// ──────────────────────────────────────────────────────────────────────────────

/// Deserializes raw bytes from an input topic into a boxed `ErasedRecord`.
/// The graph driver calls `deserialize` directly — `SourceNode` does **not**
/// implement `ErasedNode` because it is never the target of a `forward`.
pub(crate) struct SourceNode<K, V, KS, VS> {
    name: String,
    key_serde: KS,
    value_serde: VS,
    _pd: std::marker::PhantomData<fn(K, V)>,
}

impl<K, V, KS, VS> SourceNode<K, V, KS, VS>
where
    K: Any + Send + Clone,
    V: Any + Send + Clone,
    KS: Serde<K>,
    VS: Serde<V>,
{
    pub(crate) fn new(name: String, key_serde: KS, value_serde: VS) -> Self {
        Self {
            name,
            key_serde,
            value_serde,
            _pd: std::marker::PhantomData,
        }
    }

    /// Deserialize raw bytes into a type-erased `ErasedRecord`.
    pub(crate) fn deserialize(
        &self,
        key: Option<&[u8]>,
        value: &[u8],
        timestamp: i64,
    ) -> Result<ErasedRecord, ProcessorError> {
        let k: Option<Box<dyn Any + Send>> = match key {
            None => None,
            Some(kb) => {
                let k = self
                    .key_serde
                    // Source deserialize is id-based (the framed schema id selects
                    // the writer schema), so the topic is unused by decoders; pass
                    // the node name as a stable label.
                    .deserialize(&self.name, kb)
                    .map_err(|e| ProcessorError::Serde {
                        node: self.name.clone(),
                        message: e.to_string(),
                    })?;
                Some(Box::new(k) as Box<dyn Any + Send>)
            }
        };

        let v = self
            .value_serde
            .deserialize(&self.name, value)
            .map_err(|e| ProcessorError::Serde {
                node: self.name.clone(),
                message: e.to_string(),
            })?;

        Ok(ErasedRecord::new(
            k,
            Box::new(v) as Box<dyn Any + Send>,
            timestamp,
        ))
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;
    use crate::processor::{
        api::{Processor, ProcessorContext},
        erased::{Dispatch, ErasedRecord},
        record::{Record, RecordContext},
        serde::{I64Serde, StringSerde},
    };

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

    #[allow(clippy::too_many_arguments)]
    fn make_dispatch<'a>(
        buffer: &'a mut VecDeque<(usize, ErasedRecord)>,
        children: &'a [usize],
        output: &'a mut Vec<crate::processor::erased::OutputRecord>,
        rc: &'a RecordContext,
        stores: &'a mut crate::store::registry::StoreRegistry,
        globals: &'a crate::runtime::global::GlobalStateManager,
        schedules: &'a mut Vec<crate::processor::punctuation::ScheduleEntry>,
    ) -> Dispatch<'a> {
        Dispatch {
            buffer,
            children,
            output,
            record_ctx: rc,
            stores,
            globals,
            node_idx: 0,
            schedules,
            sched_stream_time: i64::MIN,
            sched_wall_clock: 0,
        }
    }

    fn default_rc() -> RecordContext {
        RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 1,
        }
    }

    #[tokio::test]
    async fn processor_node_downcasts_runs_forwards() {
        let mut node = ProcessorNode::new("upcase".into(), &(|| Upper));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = default_rc();
        let children = [9usize];
        let mut stores = crate::store::registry::StoreRegistry::default();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut d = make_dispatch(
            &mut buffer,
            &children,
            &mut output,
            &rc,
            &mut stores,
            &globals,
            &mut scheds,
        );
        let rec = ErasedRecord::new(
            Some(Box::new("k".to_string())),
            Box::new("hi".to_string()),
            1,
        );
        node.process(&mut d, rec).await.unwrap();
        let (_c, out) = buffer.pop_front().unwrap();
        check!(*out.value.downcast::<String>().unwrap() == "HI");
    }

    #[tokio::test]
    async fn processor_node_none_key_passes_through() {
        let mut node = ProcessorNode::new("upcase".into(), &(|| Upper));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = default_rc();
        let children = [0usize];
        let mut stores = crate::store::registry::StoreRegistry::default();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut d = make_dispatch(
            &mut buffer,
            &children,
            &mut output,
            &rc,
            &mut stores,
            &globals,
            &mut scheds,
        );
        let rec = ErasedRecord::new(None, Box::new("hi".to_string()), 1);
        node.process(&mut d, rec).await.unwrap();
        let (_c, out) = buffer.pop_front().unwrap();
        check!(
            (out.key.is_none(), *out.value.downcast::<String>().unwrap())
                == (true, "HI".to_string())
        );
    }

    #[tokio::test]
    async fn processor_node_downcast_error() {
        let mut node = ProcessorNode::new("p".into(), &(|| Upper));
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = default_rc();
        let mut stores = crate::store::registry::StoreRegistry::default();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut d = make_dispatch(
            &mut buffer,
            &[],
            &mut output,
            &rc,
            &mut stores,
            &globals,
            &mut scheds,
        );
        // value is i32, not String — must fail
        let bad = ErasedRecord::new(None, Box::new(7i32), 0);
        check!(node.process(&mut d, bad).await.is_err());
    }

    #[tokio::test]
    async fn sink_node_serializes_to_output() {
        let mut node = SinkNode::new("out".into(), "out-topic".into(), StringSerde, StringSerde);
        let mut buffer = VecDeque::new();
        let mut output = Vec::new();
        let rc = default_rc();
        let mut stores = crate::store::registry::StoreRegistry::default();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut d = make_dispatch(
            &mut buffer,
            &[],
            &mut output,
            &rc,
            &mut stores,
            &globals,
            &mut scheds,
        );
        let rec = ErasedRecord::new(
            Some(Box::new("k".to_string())),
            Box::new("V".to_string()),
            1,
        );
        node.process(&mut d, rec).await.unwrap();
        check!(
            (
                output.len(),
                output[0].topic.as_str(),
                output[0].value.as_deref()
            ) == (1, "out-topic", Some(b"V".as_slice()))
        );
    }

    #[tokio::test]
    async fn sink_node_none_key_produces_none_key_bytes() {
        let mut node = SinkNode::new("s".into(), "out-topic".into(), StringSerde, StringSerde);
        let mut buffer = VecDeque::new();
        let mut output = Vec::new();
        let rc = default_rc();
        let mut stores = crate::store::registry::StoreRegistry::default();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut d = make_dispatch(
            &mut buffer,
            &[],
            &mut output,
            &rc,
            &mut stores,
            &globals,
            &mut scheds,
        );
        let rec = ErasedRecord::new(None, Box::new("v".to_string()), 0);
        node.process(&mut d, rec).await.unwrap();
        check!(
            (
                output.len(),
                output[0].key.is_none(),
                output[0].value.as_deref()
            ) == (1, true, Some(b"v".as_slice()))
        );
    }

    #[tokio::test]
    async fn sink_node_downcast_error() {
        let mut node = SinkNode::new("s".into(), "out".into(), StringSerde, StringSerde);
        let mut buffer = VecDeque::new();
        let mut output = Vec::new();
        let rc = default_rc();
        let mut stores = crate::store::registry::StoreRegistry::default();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut d = make_dispatch(
            &mut buffer,
            &[],
            &mut output,
            &rc,
            &mut stores,
            &globals,
            &mut scheds,
        );
        // value is i32, not String — must fail
        let bad = ErasedRecord::new(None, Box::new(7i32), 0);
        check!(node.process(&mut d, bad).await.is_err());
    }

    #[test]
    fn source_node_deserializes() {
        let node = SourceNode::new("src".into(), StringSerde, StringSerde);
        let er = node.deserialize(Some(b"k"), b"v", 3).unwrap();
        check!(*er.value.downcast::<String>().unwrap() == "v");
    }

    #[test]
    fn source_node_deserialize_error_and_none_key() {
        // bad key length for I64Serde → error
        let node = SourceNode::new("src".into(), I64Serde, I64Serde);
        check!(
            node.deserialize(Some(&[0, 1]), &[0, 0, 0, 0, 0, 0, 0, 1], 0)
                .is_err()
        );

        // None key path → key is None in erased record
        let ok = SourceNode::new("s2".into(), StringSerde, StringSerde);
        let er = ok.deserialize(None, b"v", 0).unwrap();
        check!(er.key.is_none());
    }
}
