//! `run` subcommand entry point.
//!
//! Wires together telemetry, the health/metrics server, leader election,
//! and the Kafka controller into a single supervised task tree. Returns
//! when any supervised task finishes (or errors) or a shutdown signal
//! arrives.

use std::sync::Arc;

use kube::Client;
use tokio::sync::Mutex;

use crate::{
    config::OperatorConfig,
    context::Context,
    controller,
    health::{self, HealthState},
    leader_election, telemetry,
};

/// Run the operator. See module docs for the supervision shape.
///
/// # Errors
///
/// Returns an error if Kubernetes client construction fails or if leader
/// election surfaces an unrecoverable API error. Per-task failures inside
/// the `tokio::select!` arms are logged but not propagated, since this is
/// supervisor glue and the e2e test is the contract.
pub async fn run(config: OperatorConfig) -> anyhow::Result<()> {
    telemetry::init_tracing(&config.log_filter);
    let (registry, metrics) = telemetry::new_registry_with_metrics();
    let registry = Arc::new(Mutex::new(registry));
    let health_state = HealthState::new(registry.clone());

    let health_addr = config.health_addr;
    let health_handle = tokio::spawn({
        let state = health_state.clone();
        async move { health::serve(health_addr, state).await }
    });

    let client = Client::try_default().await?;

    leader_election::acquire(
        client.clone(),
        &config.operator_namespace,
        &config.lease_name,
        &config.pod_name,
    )
    .await?;

    let ctx = Context::new(client, config, registry, metrics);
    health_state.mark_ready();

    let kafka_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::kafka::run(ctx).await }
    });
    let pool_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::kafka_node_pool::run(ctx).await }
    });
    let topic_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::topic::run(ctx).await }
    });
    let user_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::user::run(ctx).await }
    });
    let rebalance_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::rebalance::run(ctx).await }
    });
    let grpc_gateway_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::grpc_gateway::run(ctx).await }
    });
    let schema_registry_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::schema_registry::run(ctx).await }
    });

    tokio::select! {
        res = health_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "health server exited with error"),
            Err(e) => tracing::error!(error = %e, "health task panicked"),
        },
        res = kafka_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "Kafka controller exited with error"),
            Err(e) => tracing::error!(error = %e, "Kafka controller task panicked"),
        },
        res = pool_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaNodePool controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaNodePool controller task panicked"),
        },
        res = topic_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaTopic controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaTopic controller task panicked"),
        },
        res = user_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaUser controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaUser controller task panicked"),
        },
        res = rebalance_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaRebalance controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaRebalance controller task panicked"),
        },
        res = grpc_gateway_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaGrpcGateway controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaGrpcGateway controller task panicked"),
        },
        res = schema_registry_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "SchemaRegistry controller exited with error"),
            Err(e) => tracing::error!(error = %e, "SchemaRegistry controller task panicked"),
        },
        () = shutdown_signal() => tracing::info!("shutdown signal received"),
    }
    Ok(())
}

/// Resolve when SIGINT or (on Unix) SIGTERM arrives. SIGTERM is the
/// signal Kubernetes sends on pod shutdown; SIGINT covers `Ctrl+C` for
/// local runs and also works on Windows.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
