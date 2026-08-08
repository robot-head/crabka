#![doc = include_str!("../README.md")]

mod health;
mod metrics;

pub mod config;
pub mod kafka;

use std::sync::Arc;

use anyhow::Context as _;
use bytes::Bytes;
pub use config::WorkerConfig;
use crabka_connect::{ConnectorRuntime, RuntimeState};
use crabka_connect_postgres::PostgresWalSource;
pub use kafka::{CHECKPOINT_TOPIC, KafkaCheckpointStore, KafkaSink};
use tokio_util::sync::CancellationToken;

use crate::{kafka::KafkaClientConfig, metrics::WorkerMetrics};

/// Run one Postgres-to-Kafka connector until Ctrl-C and drain it gracefully.
///
/// # Errors
///
/// Returns an error for invalid configuration, startup failures, a connector
/// runtime failure, health-server failure, or signal-listener failure.
pub async fn run(config: WorkerConfig) -> anyhow::Result<()> {
    config.validate().map_err(anyhow::Error::msg)?;
    let security = config.client_security().map_err(anyhow::Error::msg)?;
    let checkpoint_key = config.checkpoint_key();
    let client = KafkaClientConfig {
        bootstrap: config.kafka_bootstrap.clone(),
        security,
        replication_factor: config.replication_factor,
        dispatch_queue_capacity: config.client_dispatch_queue_capacity,
        frame_max_bytes: config.client_frame_max_bytes,
    };
    let metrics = WorkerMetrics::new();
    let shutdown = CancellationToken::new();

    let source = PostgresWalSource::connect(config.postgres_source())
        .await
        .context("connect PostgreSQL source")?;
    let sink =
        KafkaSink::start_with_config(client.clone(), config.topic_prefix.clone(), metrics.clone())
            .await
            .context("connect Kafka sink")?;
    let checkpoints = Arc::new(
        KafkaCheckpointStore::start_with_config(client, checkpoint_key, metrics.clone())
            .await
            .context("connect Kafka checkpoint store")?,
    );
    metrics.set_live(true);
    let mut health_task = health::start(
        config.health_listen,
        metrics.clone(),
        shutdown.child_token(),
    )
    .await?;
    let handle = ConnectorRuntime::<Bytes, Bytes>::new()
        .add_source(source)
        .add_sink(sink)
        .checkpoint_store(checkpoints)
        .max_batch(config.batch_size)
        .commit_interval(config.commit_interval())
        .poll_backoff(config.poll_backoff())
        .run();

    tracing::info!(connector.id = %config.connector_id, "connector worker running");
    let mut terminal_error = None;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        let state = handle.state();
        metrics.set_ready(matches!(
            state,
            RuntimeState::Running | RuntimeState::Paused
        ));
        if matches!(state, RuntimeState::Failed | RuntimeState::Stopped) {
            terminal_error = Some(format!("connector runtime stopped in state {state:?}"));
            break;
        }
        tokio::select! {
            signal = shutdown_signal() => {
                signal.context("listen for shutdown signal")?;
                break;
            }
            _ = tick.tick() => {}
            result = &mut health_task => {
                terminal_error = Some(match result {
                    Ok(Ok(())) => "health server stopped unexpectedly".to_owned(),
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => format!("health server task failed: {error}"),
                });
                break;
            }
        }
    }

    metrics.set_ready(false);
    if let Err(error) = handle.shutdown().await {
        metrics.record_error();
        terminal_error.get_or_insert_with(|| error.to_string());
    }
    metrics.set_live(false);
    shutdown.cancel();
    if !health_task.is_finished() {
        health_task.await.context("join health server task")??;
    }
    if let Some(error) = terminal_error {
        anyhow::bail!(error);
    }
    tracing::info!(connector.id = %config.connector_id, "connector worker stopped");
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
