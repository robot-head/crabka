//! Type-erased records + the per-dispatch context the graph driver hands to
//! each node. Records flow erased (`Box<dyn Any + Send>`) between nodes; only
//! source/sink boundaries (de)serialize.

use std::{any::Any, collections::VecDeque};

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
pub(crate) struct OutputRecord {
    pub topic: String,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: i64,
}

/// What the driver lends to a node for the duration of one `process` call:
/// the forward buffer (for source/processor children), this node's child
/// indices, the sink output collector, the source-record context, and the
/// per-task store registry (for `get_state_store`).
pub(crate) struct Dispatch<'a> {
    pub buffer: &'a mut VecDeque<(usize, ErasedRecord)>,
    pub children: &'a [usize],
    pub output: &'a mut Vec<OutputRecord>,
    pub record_ctx: &'a RecordContext,
    pub stores: &'a mut crate::store::registry::StoreRegistry,
    /// The app-wide, fully-replicated global stores (shared across tasks). Read
    /// by stream-globaltable join processors via `ProcessorContext::global_get`.
    pub globals: &'a crate::runtime::global::GlobalStateManager,
    /// The graph node this dispatch is positioned at (so `schedule` tags the
    /// owning node, and a punctuator forwards to this node's children).
    pub node_idx: usize,
    /// Sink for `ProcessorContext::schedule`: newly-registered punctuation schedules.
    pub schedules: &'a mut Vec<crate::processor::punctuation::ScheduleEntry>,
    /// Current stream-time / wall-clock — the BASE `schedule` stamps a new entry's
    /// first-fire time from (`base + interval`).
    pub sched_stream_time: i64,
    pub sched_wall_clock: i64,
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn erase_then_downcast_roundtrips_value_and_key() {
        let er = ErasedRecord::new(Some(Box::new(7i32)), Box::new("v".to_string()), 1);
        let key = er.key.unwrap().downcast::<i32>().unwrap();
        let val = er.value.downcast::<String>().unwrap();
        check!((*key, val.as_str()) == (7, "v"));
    }
}
