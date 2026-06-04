//! Type-erased node adapters: `ProcessorNode`, `SinkNode`, `SourceNode`.
//!
//! Each adapter carries the `TypeId` of the `(K, V)` pairs it consumes and/or
//! produces so the graph builder (Task 7) can validate wiring at build time
//! without keeping the concrete type parameters in scope.
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

use std::any::{Any, TypeId, type_name};

use super::api::{Processor, ProcessorContext, ProcessorSupplier};
use super::erased::{Dispatch, ErasedRecord, OutputRecord, ProcessorError};
use super::record::Record;
use super::serde::Serde;

// ──────────────────────────────────────────────────────────────────────────────
// ErasedNode trait
// ──────────────────────────────────────────────────────────────────────────────

/// Object-safe trait for a node slot in the execution graph.
///
/// Implemented by [`ProcessorNode`] and [`SinkNode`].  [`SourceNode`] does
/// **not** implement this trait because sources are entered via their own
/// `deserialize` method — they are never the target of a `forward` from a
/// parent node.
#[allow(dead_code)] // type-query methods used by future graph introspection; process() is used now
pub(crate) trait ErasedNode: Send {
    /// Human-readable name (from the topology builder).
    fn name(&self) -> &str;

    /// Process one erased record: downcast, run inner logic, push results.
    fn process(
        &mut self,
        dispatch: &mut Dispatch<'_>,
        record: ErasedRecord,
    ) -> Result<(), ProcessorError>;

    /// `TypeId` pair `(K, V)` this node **consumes**.
    fn input_kv(&self) -> (TypeId, TypeId);

    /// `TypeId` pair `(K, V)` this node **produces**, or `None` for sinks.
    fn output_kv(&self) -> Option<(TypeId, TypeId)>;

    /// Human-readable type names for the input pair (for error messages).
    fn input_names(&self) -> (&'static str, &'static str);

    /// Human-readable type names for the output pair, or `None` for sinks.
    fn output_names(&self) -> Option<(&'static str, &'static str)>;
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

impl<KIn, VIn, KOut, VOut> ErasedNode for ProcessorNode<KIn, VIn, KOut, VOut>
where
    KIn: Any + Send,
    VIn: Any + Send,
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn process(
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
        self.inner.process(&mut ctx, record);
        Ok(())
    }

    fn input_kv(&self) -> (TypeId, TypeId) {
        (TypeId::of::<KIn>(), TypeId::of::<VIn>())
    }

    fn output_kv(&self) -> Option<(TypeId, TypeId)> {
        Some((TypeId::of::<KOut>(), TypeId::of::<VOut>()))
    }

    fn input_names(&self) -> (&'static str, &'static str) {
        (type_name::<KIn>(), type_name::<VIn>())
    }

    fn output_names(&self) -> Option<(&'static str, &'static str)> {
        Some((type_name::<KOut>(), type_name::<VOut>()))
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

impl<K, V, KS, VS> ErasedNode for SinkNode<K, V, KS, VS>
where
    K: Any + Send,
    V: Any + Send,
    KS: Serde<K> + Send,
    VS: Serde<V> + Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn process(
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

        let key_bytes = key.as_ref().map(|k| self.key_serde.serialize(k));
        let value_bytes = Some(self.value_serde.serialize(&value));

        dispatch.output.push(OutputRecord {
            topic: self.topic.clone(),
            key: key_bytes,
            value: value_bytes,
            timestamp: rec.timestamp,
        });

        Ok(())
    }

    fn input_kv(&self) -> (TypeId, TypeId) {
        (TypeId::of::<K>(), TypeId::of::<V>())
    }

    fn output_kv(&self) -> Option<(TypeId, TypeId)> {
        None // sink — no children
    }

    fn input_names(&self) -> (&'static str, &'static str) {
        (type_name::<K>(), type_name::<V>())
    }

    fn output_names(&self) -> Option<(&'static str, &'static str)> {
        None
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
                    .deserialize(kb)
                    .map_err(|e| ProcessorError::Serde {
                        node: self.name.clone(),
                        message: e.to_string(),
                    })?;
                Some(Box::new(k) as Box<dyn Any + Send>)
            }
        };

        let v = self
            .value_serde
            .deserialize(value)
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

    /// The `TypeId` pair `(K, V)` this source produces.
    #[allow(dead_code, clippy::unused_self)]
    pub(crate) fn output_kv(&self) -> (TypeId, TypeId) {
        (TypeId::of::<K>(), TypeId::of::<V>())
    }

    /// Human-readable names for the output pair.
    #[allow(dead_code, clippy::unused_self)]
    pub(crate) fn output_names(&self) -> (&'static str, &'static str) {
        (type_name::<K>(), type_name::<V>())
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use crate::processor::serde::StringSerde;
    use assert2::check;
    use std::any::TypeId;
    use std::collections::VecDeque;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    #[test]
    fn processor_node_downcasts_runs_forwards() {
        let mut node = ProcessorNode::new(
            "upcase".into(),
            &(|| Box::new(Upper) as Box<dyn Processor<String, String, String, String>>),
        );
        check!(node.input_kv() == (TypeId::of::<String>(), TypeId::of::<String>()));
        check!(node.output_kv() == Some((TypeId::of::<String>(), TypeId::of::<String>())));

        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 1,
        };
        let children = [9usize];
        let mut d = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: &mut output,
            record_ctx: &rc,
        };
        let rec = ErasedRecord::new(
            Some(Box::new("k".to_string())),
            Box::new("hi".to_string()),
            1,
        );
        node.process(&mut d, rec).unwrap();
        let (_c, out) = buffer.pop_front().unwrap();
        check!(*out.value.downcast::<String>().unwrap() == "HI");
    }

    #[test]
    fn sink_node_serializes_to_output() {
        let mut node = SinkNode::new("out".into(), "out-topic".into(), StringSerde, StringSerde);
        let mut buffer = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 1,
        };
        let mut d = Dispatch {
            buffer: &mut buffer,
            children: &[],
            output: &mut output,
            record_ctx: &rc,
        };
        let rec = ErasedRecord::new(
            Some(Box::new("k".to_string())),
            Box::new("V".to_string()),
            1,
        );
        node.process(&mut d, rec).unwrap();
        check!(output.len() == 1);
        check!(output[0].topic == "out-topic");
        check!(output[0].value.as_ref().unwrap().as_ref() == b"V");
    }

    #[test]
    fn source_node_deserializes() {
        let node = SourceNode::new("src".into(), StringSerde, StringSerde);
        check!(node.output_kv() == (TypeId::of::<String>(), TypeId::of::<String>()));
        let er = node.deserialize(Some(b"k"), b"v", 3).unwrap();
        check!(*er.value.downcast::<String>().unwrap() == "v");
    }
}
