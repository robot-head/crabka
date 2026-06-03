//! Type-erased records + the per-dispatch context the graph driver hands to
//! each node. Records flow erased (`Box<dyn Any + Send>`) between nodes; only
//! source/sink boundaries (de)serialize.

use std::any::Any;
use std::collections::VecDeque;

use bytes::Bytes;

use super::record::RecordContext;

/// An error raised while a node processes a record (e.g. an internal downcast
/// mismatch — unreachable in practice because `build()` validates wiring).
#[derive(Debug, thiserror::Error)]
pub enum ProcessorError {
    #[error("type mismatch in node `{node}`: expected {expected}, found a different type")]
    Downcast {
        node: String,
        expected: &'static str,
    },
    #[error("serialization error in sink `{node}`: {message}")]
    Serde { node: String, message: String },
}

/// A record with erased key/value, as it flows between nodes.
#[allow(dead_code)] // fields read in future tasks + tests
pub(crate) struct ErasedRecord {
    pub key: Option<Box<dyn Any + Send>>,
    pub value: Box<dyn Any + Send>,
    pub timestamp: i64,
}

impl ErasedRecord {
    pub fn new(
        key: Option<Box<dyn Any + Send>>,
        value: Box<dyn Any + Send>,
        timestamp: i64,
    ) -> Self {
        Self {
            key,
            value,
            timestamp,
        }
    }
}

/// A record emitted by a sink node, collected by the driver (test driver: into
/// an output queue; runtime: into the producer).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct OutputRecord {
    pub topic: String,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: i64,
}

/// What the driver lends to a node for the duration of one `process` call:
/// the forward buffer (for source/processor children), this node's child
/// indices, the sink output collector, and the source-record context.
#[allow(dead_code)]
pub(crate) struct Dispatch<'a> {
    pub buffer: &'a mut VecDeque<(usize, ErasedRecord)>,
    pub children: &'a [usize],
    pub output: &'a mut Vec<OutputRecord>,
    pub record_ctx: &'a RecordContext,
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn erase_then_downcast_roundtrips_value_and_key() {
        let er = ErasedRecord::new(Some(Box::new(7i32)), Box::new("v".to_string()), 1);
        let key = er.key.unwrap().downcast::<i32>().unwrap();
        let val = er.value.downcast::<String>().unwrap();
        check!(*key == 7);
        check!(*val == "v");
    }
}
