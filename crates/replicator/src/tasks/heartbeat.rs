//! Heartbeat task: periodically writes MM2 Heartbeat records to the target.

use bytes::Bytes;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_units::{
    fmt::Human as _,
    prelude::{Time, TimeExt as _},
};
use tokio::sync::watch;
use tracing::warn;

use crate::{error::ReplicatorError, mm2::Heartbeat};

/// Parameters for the [`HeartbeatTask`].
pub struct HeartbeatParams {
    /// Bootstrap address of the target cluster.
    pub target_bootstrap: String,
    /// Alias of the source cluster written into each heartbeat record.
    pub source_alias: String,
    /// Alias of the target cluster written into each heartbeat record.
    pub target_alias: String,
    /// How often to emit a heartbeat record.
    pub interval: Time,
    /// Injectable clock: returns the current time in milliseconds since epoch.
    pub now_ms: fn() -> i64,
    /// Injectable async sleeper that paces the heartbeat interval. Production
    /// uses [`qubit_clock::sleep::SystemSleeper`] (real tokio time); tests
    /// inject a [`qubit_clock::sleep::MockSleeper`] so the interval fires on a
    /// mock timeline instead of wall-clock time.
    pub sleeper: std::sync::Arc<dyn qubit_clock::sleep::AsyncSleeper>,
    /// Optional TLS/SASL security for the target cluster.
    pub security: Option<crabka_client_core::security::ClientSecurity>,
}

/// A handle to the background heartbeat task.
///
/// Drop or call [`shutdown`](HeartbeatTask::shutdown) to stop it.
pub struct HeartbeatTask {
    handle: tokio::task::JoinHandle<()>,
    shutdown: watch::Sender<bool>,
}

impl HeartbeatTask {
    /// Ensure the `heartbeats` topic exists, build a producer, and spawn the
    /// background loop.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicatorError::Client`] if topic creation or producer
    /// construction fails.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(source = %p.source_alias, target = %p.target_alias, interval = %p.interval.human()),
        err,
    )]
    pub async fn start(p: HeartbeatParams) -> Result<Self, ReplicatorError> {
        // Ensure the heartbeats topic exists before we start producing.
        crate::admin_util::ensure_topic(
            &p.target_bootstrap,
            Heartbeat::TOPIC,
            1,
            p.security.clone(),
        )
        .await
        .map_err(ReplicatorError::Client)?;

        // Build the producer once — reused for every heartbeat.
        let producer = build_producer(&p.target_bootstrap, p.security)
            .await
            .map_err(|e| ReplicatorError::Client(e.to_string()))?;

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let source = p.source_alias;
        let target = p.target_alias;
        let interval = p.interval;
        let now_ms = p.now_ms;
        let sleeper = p.sleeper;

        let handle = tokio::spawn(async move {
            // Pace the heartbeat cadence through the injected `AsyncSleeper`
            // (production: real tokio time via `SystemSleeper`; tests: a
            // `MockSleeper` on a mock timeline). The interval is a single sleep
            // future re-armed only after it fires — equivalent to
            // `tokio::time::interval` with `MissedTickBehavior::Delay`. Unlike
            // `interval` it has no immediate zeroth tick, so the first heartbeat
            // lands one interval in (which is what we want anyway).
            let mut tick = sleeper.sleep_for_async(interval.to_std());

            loop {
                tokio::select! {
                    () = &mut tick => {
                        let hb = Heartbeat {
                            source: source.clone(),
                            target: target.clone(),
                            timestamp_ms: now_ms(),
                        };

                        let key = Bytes::from(hb.key_bytes());
                        let value = Bytes::from(hb.value_bytes());

                        let rx = producer
                            .send(ProducerRecord {
                                topic: Heartbeat::TOPIC.to_string(),
                                partition: None,
                                key: Some(key),
                                value: Some(value),
                                headers: vec![],
                                timestamp_ms: None,
                            })
                            .await;

                        match rx.await {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => warn!("heartbeat produce error: {e}"),
                            Err(_) => warn!("heartbeat: sender dropped (ack channel closed)"),
                        }

                        if let Err(e) = producer.flush().await {
                            warn!("heartbeat flush error: {e}");
                        }

                        // Re-arm the interval for the next tick.
                        tick = sleeper.sleep_for_async(interval.to_std());
                    }
                    Ok(()) = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        Ok(Self {
            handle,
            shutdown: shutdown_tx,
        })
    }

    /// Signal the background task to stop and wait for it to finish.
    // cargo-mutants: shutdown signal/join not asserted by unit tests
    #[cfg_attr(test, mutants::skip)]
    pub async fn shutdown(self) {
        // Ignore send error: if the receiver is already gone the task is done.
        let _ = self.shutdown.send(true);
        let _ = self.handle.await;
    }
}

/// Build a non-idempotent producer with `acks=All` for heartbeat emission.
async fn build_producer(
    bootstrap: &str,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<Producer, crabka_client_producer::ProducerError> {
    let builder = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(false)
        .acks(Acks::All);
    match security {
        Some(s) => builder.security(s).build().await,
        None => builder.build().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_units::prelude::{millis, secs};
    use qubit_clock::{MockTime, MockWaiterKind};

    use super::*;

    /// Number of heartbeat intervals to fire on the mock timeline.
    const TICKS: usize = 3;
    /// Heartbeat cadence the mock timeline is advanced by, one tick at a time.
    const INTERVAL: Time = millis(100);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn emits_heartbeat() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = broker.listen_addr().to_string();

        // Pace the heartbeat interval off a mock timeline instead of wall-clock
        // time: advancing the timeline — not a real `sleep` — is what fires each
        // tick, so the test is deterministic and doesn't depend on real time.
        let mock = MockTime::unix_epoch();

        let h = HeartbeatTask::start(HeartbeatParams {
            target_bootstrap: target.clone(),
            source_alias: "us-east".into(),
            target_alias: "eu-west".into(),
            interval: INTERVAL,
            now_ms: || 123,
            sleeper: Arc::new(mock.sleeper()),
            security: None,
        })
        .await
        .unwrap();

        // Fire the interval `TICKS` times. Each pass:
        //   1. wait until the task has really parked on its interval sleep
        //      (registered a mock `Sleep` waiter) so the advance can't race
        //      ahead of the sleep's creation and skip a tick;
        //   2. advance the mock timeline by one interval to wake it;
        //   3. wait until that heartbeat is durably written before firing the
        //      next tick.
        // A completed sleep future drops its waiter registration *before* the
        // tick body writes the record, so once the Nth heartbeat is visible the
        // only registered waiter is the freshly re-armed (N+1)th — there is no
        // stale count to trip the next `wait_for_blocked_waiters`.
        for n in 1..=TICKS {
            let timeline = mock.timeline();
            let parked = tokio::task::spawn_blocking(move || {
                timeline.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, secs(5).to_std())
            })
            .await
            .unwrap();
            assert2::assert!(parked);

            mock.advance(INTERVAL.to_std());

            crate::test_util::await_topic_count(&target, Heartbeat::TOPIC, n, secs(10)).await;
        }

        h.shutdown().await;

        // Exactly one heartbeat per advance — no more (no advance fired after
        // the loop) and no fewer (each advance was gated on its write landing).
        let count = crate::test_util::topic_record_count(&target, Heartbeat::TOPIC).await;
        assert2::assert!(count == TICKS);

        let raw = crate::admin_util::read_last_value_for_key(
            &target,
            crate::mm2::Heartbeat::TOPIC,
            b"",
            None,
        )
        .await
        .unwrap();
        assert2::assert!(raw.is_some());
    }
}
