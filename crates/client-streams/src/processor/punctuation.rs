//! Punctuation (`ProcessorContext::schedule`, KIP Processor API): periodic
//! callbacks fired on stream-time or wall-clock boundaries. A `Punctuator` is a
//! trait object erased to the driver exactly like a `Processor` (`TypedPunctuator`
//! rebuilds the typed `ProcessorContext` from the `Dispatch`). Schedules live in
//! the `Graph`, tagged by node index; the driver fires them positioned at that
//! node so a punctuator's `forward` flows downstream. Punctuation is invisible in
//! the wire topology — pure runtime.
use std::{
    any::Any,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use crabka_units::prelude::*;

use crate::processor::{api::ProcessorContext, erased::Dispatch};

/// Which clock drives a punctuation schedule (JVM `PunctuationType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PunctuationType {
    /// Driven by the task's observed max record timestamp.
    StreamTime,
    /// Driven by the system (or mock) wall clock.
    WallClockTime,
}

/// A periodic callback. Implemented on a user struct (like [`Processor`]); shares
/// mutable state with its owning processor via `Arc<Mutex<_>>`.
///
/// [`Processor`]: crate::processor::api::Processor
#[async_trait]
pub trait Punctuator<K: Send, V: Send>: Send + 'static {
    /// Fire at `timestamp` (stream-time: the scheduled time; wall-clock: the
    /// clock's current time). May `forward` via `ctx` and use state stores.
    async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, timestamp: i64);
}

/// Handle returned by `ProcessorContext::schedule`. `cancel()` stops the schedule;
/// the driver drops it on the next punctuate pass.
#[derive(Clone)]
pub struct Cancellable(Arc<AtomicBool>);
impl Cancellable {
    pub(crate) fn new(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }
    /// Stop this schedule from firing again.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Internal: a punctuator erased to the driver's untyped surface (mirrors
/// `ErasedNode`).
#[async_trait]
pub(crate) trait ErasedPunctuator: Send {
    async fn fire(&mut self, dispatch: &mut Dispatch<'_>, timestamp: i64);
}

/// Wraps a typed [`Punctuator`] into an [`ErasedPunctuator`] by rebuilding the
/// typed `ProcessorContext` from the `Dispatch`.
pub(crate) struct TypedPunctuator<K, V, P> {
    inner: P,
    _pd: PhantomData<fn(K, V)>,
}
impl<K, V, P> TypedPunctuator<K, V, P> {
    pub(crate) fn new(inner: P) -> Self {
        Self {
            inner,
            _pd: PhantomData,
        }
    }
}
#[async_trait]
impl<K, V, P> ErasedPunctuator for TypedPunctuator<K, V, P>
where
    K: Any + Send + Clone,
    V: Any + Send + Clone,
    P: Punctuator<K, V>,
{
    async fn fire(&mut self, dispatch: &mut Dispatch<'_>, timestamp: i64) {
        let mut ctx = ProcessorContext::<'_, '_, K, V>::new(dispatch);
        self.inner.punctuate(&mut ctx, timestamp).await;
    }
}

/// One live punctuation schedule, owned by the `Graph`.
pub(crate) struct ScheduleEntry {
    pub node_idx: usize,
    pub interval: Time,
    pub ty: PunctuationType,
    /// The next time to fire — an instant on the evaluating clock's timeline.
    /// Stamped at `schedule()` time as `base + interval` (the clock value when
    /// the schedule is registered).
    pub next_time: i64,
    pub punctuator: Box<dyn ErasedPunctuator>,
    pub cancel: Arc<AtomicBool>,
}
impl ScheduleEntry {
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn cancellable_flips_the_flag() {
        let flag = Arc::new(AtomicBool::new(false));
        let c = Cancellable::new(flag.clone());
        check!(!flag.load(Ordering::SeqCst));
        c.cancel();
        check!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn schedule_entry_reports_cancelled() {
        struct NoOp;
        #[async_trait]
        impl ErasedPunctuator for NoOp {
            async fn fire(&mut self, _d: &mut Dispatch<'_>, _ts: i64) {}
        }
        let flag = Arc::new(AtomicBool::new(false));
        let e = ScheduleEntry {
            node_idx: 0,
            interval: millis(10),
            ty: PunctuationType::StreamTime,
            next_time: 0,
            punctuator: Box::new(NoOp),
            cancel: flag.clone(),
        };
        check!(!e.is_cancelled());
        flag.store(true, Ordering::SeqCst);
        check!(e.is_cancelled());
    }
}
