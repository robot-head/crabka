//! The typed Processor API: `Processor`, `ProcessorSupplier`, and the
//! `ProcessorContext` users call `forward` on.

use std::any::Any;
use std::marker::PhantomData;

use super::erased::{Dispatch, ErasedRecord};
use super::record::{Record, RecordContext};

/// A stateless record processor. One instance is created per task via
/// [`ProcessorSupplier::get`]. Mirrors `org.apache.kafka.streams.processor.api.Processor`.
pub trait Processor<KIn, VIn, KOut, VOut>: Send + 'static {
    fn init(&mut self, _ctx: &mut ProcessorContext<KOut, VOut>) {}
    fn process(&mut self, ctx: &mut ProcessorContext<KOut, VOut>, record: Record<KIn, VIn>);
    fn close(&mut self) {}
}

/// Factory for [`Processor`] instances (one per task → per-task isolation).
pub trait ProcessorSupplier<KIn, VIn, KOut, VOut>: Send + Sync + 'static {
    fn get(&self) -> Box<dyn Processor<KIn, VIn, KOut, VOut>>;
}

// Blanket impl so a closure `|| Box::new(MyProc)` is a supplier.
impl<F, KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> for F
where
    F: Fn() -> Box<dyn Processor<KIn, VIn, KOut, VOut>> + Send + Sync + 'static,
{
    fn get(&self) -> Box<dyn Processor<KIn, VIn, KOut, VOut>> {
        self()
    }
}

/// Handed to [`Processor::process`]. `forward` boxes the record and queues it
/// for each child node (the driver drains the queue).
pub struct ProcessorContext<'a, KOut, VOut> {
    dispatch: &'a mut Dispatch<'a>,
    _pd: PhantomData<fn(KOut, VOut)>,
}

impl<'a, KOut, VOut> ProcessorContext<'a, KOut, VOut>
where
    KOut: Any + Send + Clone,
    VOut: Any + Send + Clone,
{
    #[allow(dead_code)] // used by future tasks + tests
    pub(crate) fn new(dispatch: &'a mut Dispatch<'a>) -> Self {
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
            let key: Option<Box<dyn Any + Send>> =
                record.key.clone().map(|k| Box::new(k) as Box<dyn Any + Send>);
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
    impl Processor<String, String, String, String> for Upper {
        fn process(
            &mut self,
            ctx: &mut ProcessorContext<String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    #[test]
    fn forward_pushes_erased_record_to_each_child() {
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 5,
        };
        let children = [3usize, 4usize];
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: &mut output,
            record_ctx: &rc,
        };
        let mut ctx = ProcessorContext::<String, String>::new(&mut dispatch);
        Upper.process(&mut ctx, Record::new(Some("k".into()), "hi".into(), 5));
        check!(buffer.len() == 2);
        let (child, rec) = buffer.pop_front().unwrap();
        check!(child == 3);
        check!(*rec.value.downcast::<String>().unwrap() == "HI");
    }
}
