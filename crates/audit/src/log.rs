//! The `AuditLog` handle (synchronous, non-blocking emit) and the background
//! `AuditWriter` that drains events into a sink.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use crate::event::AuditEvent;
use crate::ocsf::ProductInfo;
use crate::sink::{AuditRecord, AuditSink};

/// Cloneable, cheap handle that broker code calls to record events.
///
/// `emit` is synchronous and never blocks — safe to call from the synchronous
/// `Authorizer::authorize` trait as well as from async request handlers.
#[derive(Debug)]
pub struct AuditLog {
    tx: Option<mpsc::Sender<AuditEvent>>,
    dropped: AtomicU64,
}

impl AuditLog {
    /// Create an enabled log plus the receiver to hand to an [`AuditWriter`].
    #[must_use]
    pub fn new(capacity: usize) -> (Arc<Self>, mpsc::Receiver<AuditEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Arc::new(Self {
                tx: Some(tx),
                dropped: AtomicU64::new(0),
            }),
            rx,
        )
    }

    /// A no-op log used when auditing is disabled.
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            tx: None,
            dropped: AtomicU64::new(0),
        })
    }

    /// Record an event. Non-blocking; on a full queue the event is dropped and
    /// counted (durable spooling is Slice 3 / AU-5).
    pub fn emit(&self, event: AuditEvent) {
        let Some(tx) = &self.tx else { return };
        if tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("audit event dropped (queue full or writer stopped)");
        }
    }

    /// Count of events dropped due to backpressure.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Background task that serializes events and writes them to a sink.
pub struct AuditWriter {
    rx: mpsc::Receiver<AuditEvent>,
    sink: Arc<dyn AuditSink>,
    product: ProductInfo,
}

impl AuditWriter {
    #[must_use]
    pub fn new(
        rx: mpsc::Receiver<AuditEvent>,
        sink: Arc<dyn AuditSink>,
        product: ProductInfo,
    ) -> Self {
        Self { rx, sink, product }
    }

    /// Drain the channel until all senders are dropped.
    pub async fn run(mut self) {
        while let Some(event) = self.rx.recv().await {
            let record = AuditRecord::from_event(&event, &self.product);
            if let Err(e) = self.sink.write(record).await {
                tracing::warn!(error = %e, "audit sink write failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;

    use super::*;
    use crate::event::*;
    use crate::ocsf::ProductInfo;
    use crate::sink::MemorySink;

    fn product() -> ProductInfo {
        ProductInfo {
            vendor_name: "Crabka".into(),
            name: "crabka-broker".into(),
            version: "0".into(),
        }
    }

    fn life(n: i64) -> AuditEvent {
        AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: n,
            time_ms: n,
        }
    }

    #[tokio::test]
    async fn emitted_events_reach_the_sink_in_order() {
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        let writer = AuditWriter::new(rx, sink.clone(), product());
        let handle = tokio::spawn(writer.run());

        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));

        // Dropping the only sender ends the writer loop cleanly.
        drop(log);
        handle.await.unwrap();

        let recs = sink.records();
        check!(recs.len() == 3);
        check!(recs[0].class == AuditEventClass::ApplicationLifecycle);
        // node_id 1,2,3 preserved in order via the OCSF "device.uid" field.
        let v0: serde_json::Value = serde_json::from_slice(&recs[0].value).unwrap();
        check!(v0["device"]["uid"] == "1");
    }

    #[test]
    fn disabled_log_drops_without_panicking() {
        let log = AuditLog::disabled();
        log.emit(life(1)); // no receiver, no panic
        check!(log.dropped() == 0); // disabled path is a silent no-op, not a "drop"
    }

    #[tokio::test]
    async fn full_queue_increments_dropped() {
        let (log, _rx) = AuditLog::new(1); // tiny queue, receiver never drains
        // First may enqueue; subsequent ones overflow.
        for i in 0..10 {
            log.emit(life(i));
        }
        check!(log.dropped() == 9);
    }
}
