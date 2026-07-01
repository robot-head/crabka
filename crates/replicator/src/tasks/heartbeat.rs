//! Heartbeat task: periodically writes MM2 Heartbeat records to the target.

use std::time::Duration;

use bytes::Bytes;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use tokio::sync::watch;
use tracing::warn;

use crate::error::ReplicatorError;
use crate::mm2::Heartbeat;

/// Parameters for the [`HeartbeatTask`].
pub struct HeartbeatParams {
    /// Bootstrap address of the target cluster.
    pub target_bootstrap: String,
    /// Alias of the source cluster written into each heartbeat record.
    pub source_alias: String,
    /// Alias of the target cluster written into each heartbeat record.
    pub target_alias: String,
    /// How often to emit a heartbeat record.
    pub interval: Duration,
    /// Injectable clock: returns the current time in milliseconds since epoch.
    pub now_ms: fn() -> i64,
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
        fields(source = %p.source_alias, target = %p.target_alias, interval_ms = p.interval.as_millis()),
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

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so we don't emit a
            // heartbeat before the caller has a chance to observe the task.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
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
    use assert2::assert;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn emits_heartbeat() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = broker.listen_addr().to_string();
        let h = HeartbeatTask::start(HeartbeatParams {
            target_bootstrap: target.clone(),
            source_alias: "us-east".into(),
            target_alias: "eu-west".into(),
            interval: std::time::Duration::from_millis(100),
            now_ms: || 123,
            security: None,
        })
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        h.shutdown().await;
        let raw = crate::admin_util::read_last_value_for_key(
            &target,
            crate::mm2::Heartbeat::TOPIC,
            b"",
            None,
        )
        .await
        .unwrap();
        assert!(raw.is_some(), "no heartbeat written");
    }
}
