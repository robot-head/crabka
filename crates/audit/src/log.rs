//! The `AuditLog` handle (synchronous, non-blocking emit) and the background
//! `AuditWriter` that drains events into a sink.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};

use crate::chain::ChainState;
use crate::checkpoint::Checkpoint;
use crate::event::AuditEvent;
use crate::ocsf::ProductInfo;
use crate::signing::SigningKeyProvider;
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

/// Background task that chains, serializes, and writes audit events to a sink,
/// emitting signed checkpoints on a count/time cadence and at shutdown.
pub struct AuditWriter {
    rx: mpsc::Receiver<AuditEvent>,
    sink: Arc<dyn AuditSink>,
    product: ProductInfo,
    chain: ChainState,
    signer: Option<Arc<dyn SigningKeyProvider>>,
    checkpoint_every_n: u64,
    checkpoint_every: Duration,
    since_checkpoint: u64,
}

impl AuditWriter {
    #[must_use]
    pub fn new(
        rx: mpsc::Receiver<AuditEvent>,
        sink: Arc<dyn AuditSink>,
        product: ProductInfo,
        signer: Option<Arc<dyn SigningKeyProvider>>,
        checkpoint_every_n: u64,
        checkpoint_every: Duration,
    ) -> Self {
        Self {
            rx,
            sink,
            product,
            chain: ChainState::new(),
            signer,
            checkpoint_every_n,
            checkpoint_every,
            since_checkpoint: 0,
        }
    }

    /// Drain the channel until all senders drop; emit a final checkpoint for any
    /// pending tail.
    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval_at(
            Instant::now() + self.checkpoint_every,
            self.checkpoint_every,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(event) => {
                            self.write_chained(&event).await;
                            if self.checkpoint_every_n > 0
                                && self.since_checkpoint >= self.checkpoint_every_n
                            {
                                self.emit_checkpoint().await;
                            }
                        }
                        None => break, // all senders dropped
                    }
                }
                _ = ticker.tick() => {
                    if self.since_checkpoint > 0 {
                        self.emit_checkpoint().await;
                    }
                }
            }
        }
        // Final checkpoint covers any records since the last one.
        if self.since_checkpoint > 0 {
            self.emit_checkpoint().await;
        }
    }

    async fn write_chained(&mut self, event: &AuditEvent) {
        let mut record = AuditRecord::from_event(event, &self.product);
        let (seq, prev) = self.chain.extend(&record.value);
        record.push_chain_headers(seq, &prev);
        if let Err(e) = self.sink.write(record).await {
            tracing::warn!(error = %e, "audit sink write failed");
        }
        self.since_checkpoint += 1;
    }

    async fn emit_checkpoint(&mut self) {
        let Some(signer) = &self.signer else {
            // No key configured: chaining only, no checkpoints.
            self.since_checkpoint = 0;
            return;
        };
        // chain.next_seq() is the seq the NEXT record would get, so the last
        // chained record's seq is next_seq() - 1.
        let seq_high = self.chain.next_seq().saturating_sub(1);
        let head = self.chain.head();
        let cp = Checkpoint::signed(signer.as_ref(), seq_high, &head, now_ms());
        if let Err(e) = self.sink.write(cp.to_record()).await {
            tracing::warn!(error = %e, "audit checkpoint write failed");
        }
        self.since_checkpoint = 0;
    }
}

/// Epoch-millis clock for checkpoint timestamps.
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use assert2::check;

    use super::*;
    use crate::checkpoint::Checkpoint;
    use crate::event::*;
    use crate::ocsf::ProductInfo;
    use crate::signing::FileEd25519Signer;
    use crate::sink::{AuditRecord, MemorySink};

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

    fn header(rec: &AuditRecord, key: &str) -> Option<String> {
        rec.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
    }

    fn test_signer() -> (std::sync::Arc<FileEd25519Signer>, Vec<u8>) {
        use ring::signature::{Ed25519KeyPair, KeyPair};
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let pubkey = kp.public_key().as_ref().to_vec();
        let s = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "k1".into()).unwrap();
        (std::sync::Arc::new(s), pubkey)
    }

    #[tokio::test]
    async fn emitted_events_reach_the_sink_in_order() {
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        let writer = AuditWriter::new(
            rx,
            sink.clone(),
            product(),
            None,
            1_000_000,
            Duration::from_hours(1),
        );
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

    #[tokio::test]
    async fn chained_records_carry_seq_and_prev_hash() {
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        // no signer, huge interval => no checkpoints, just chaining
        let writer = AuditWriter::new(
            rx,
            sink.clone(),
            product(),
            None,
            1_000_000,
            Duration::from_hours(1),
        );
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        drop(log);
        h.await.unwrap();

        let recs = sink.records();
        check!(recs.len() == 2); // no checkpoints (no signer)
        // seq headers present and monotonic from 0
        let seq0 = header(&recs[0], "seq");
        let seq1 = header(&recs[1], "seq");
        check!(seq0 == Some("0".to_string()));
        check!(seq1 == Some("1".to_string()));
        // record 0 prev_hash is genesis (all-zero hex)
        check!(header(&recs[0], "prev_hash") == Some("0".repeat(64)));
        // record 1 prev_hash == chain_hash(genesis, 0, value0)
        let expect = crate::chain::to_hex(&crate::chain::chain_hash(
            &crate::chain::GENESIS_HEAD,
            0,
            &recs[0].value,
        ));
        check!(header(&recs[1], "prev_hash") == Some(expect));
    }

    #[tokio::test]
    async fn checkpoints_emitted_by_count_and_verify_against_recomputed_head() {
        let (signer, pubkey) = test_signer();
        let (log, rx) = AuditLog::new(64);
        let sink = Arc::new(MemorySink::default());
        // checkpoint every 2 records; long interval so only count triggers
        let writer = AuditWriter::new(
            rx,
            sink.clone(),
            product(),
            Some(signer),
            2,
            Duration::from_hours(1),
        );
        let h = tokio::spawn(writer.run());
        for i in 0..4 {
            log.emit(life(i));
        }
        drop(log); // closes channel -> final checkpoint (none pending here: 4 % 2 == 0)
        h.await.unwrap();

        let recs = sink.records();
        // 4 chained + 2 checkpoints (after record 2 and record 4)
        let checkpoints: Vec<_> = recs
            .iter()
            .filter(|r| r.class == AuditEventClass::Checkpoint)
            .collect();
        check!(checkpoints.len() == 2);

        // recompute the chain over the non-checkpoint records and verify each checkpoint
        let mut head = crate::chain::GENESIS_HEAD;
        let mut seq = 0u64;
        for r in &recs {
            if r.class == AuditEventClass::Checkpoint {
                let v: serde_json::Value = serde_json::from_slice(&r.value).unwrap();
                let cp = Checkpoint::from_value(&v).expect("cp");
                check!(cp.verify(&pubkey));
                check!(cp.chain_head == head);
                check!(cp.seq_high == seq - 1);
            } else {
                head = crate::chain::chain_hash(&head, seq, &r.value);
                seq += 1;
            }
        }
    }

    #[tokio::test]
    async fn shutdown_emits_final_checkpoint_for_pending_tail() {
        let (signer, pubkey) = test_signer();
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        // every_n large so only the shutdown path emits
        let writer = AuditWriter::new(
            rx,
            sink.clone(),
            product(),
            Some(signer),
            1_000_000,
            Duration::from_hours(1),
        );
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));
        drop(log);
        h.await.unwrap();

        let recs = sink.records();
        let cps: Vec<_> = recs
            .iter()
            .filter(|r| r.class == AuditEventClass::Checkpoint)
            .collect();
        check!(cps.len() == 1); // single final checkpoint at shutdown
        let v: serde_json::Value = serde_json::from_slice(&cps[0].value).unwrap();
        let cp = Checkpoint::from_value(&v).unwrap();
        check!(cp.verify(&pubkey));
        check!(cp.seq_high == 2); // last chained seq (records 0,1,2)
    }
}
