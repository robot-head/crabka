//! `run` subcommand entry point.
//!
//! This module connects telemetry, the health and metrics server, leader
//! election, and the Kafka controller into one supervised task tree. It
//! returns when a supervised task finishes, when a supervised task fails,
//! or when a shutdown signal arrives.

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

/// Run the operator. See the module docs for the supervision shape.
///
/// # Errors
///
/// Returns an error if the Kubernetes client cannot be constructed, or if
/// leader election gives an unrecoverable API error. This function logs
/// per-task failures in the `tokio::select!` arms but does not propagate
/// them, because this function is supervisor glue and the e2e test is the
/// contract.
pub async fn run(config: OperatorConfig) -> anyhow::Result<()> {
    config.validate().map_err(anyhow::Error::msg)?;
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
        config.leader_lease_duration,
        config.leader_retry_interval,
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
    let connector_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::connector::run(ctx).await }
    });
    let schema_registry_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::schema_registry::run(ctx).await }
    });
    let gres_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::gres::run(ctx).await }
    });
    let gres_tenant_handle = tokio::spawn({
        let ctx = ctx.clone();
        async move { controller::gres_tenant::run(ctx).await }
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
        res = connector_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "KafkaConnector controller exited with error"),
            Err(e) => tracing::error!(error = %e, "KafkaConnector controller task panicked"),
        },
        res = schema_registry_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "SchemaRegistry controller exited with error"),
            Err(e) => tracing::error!(error = %e, "SchemaRegistry controller task panicked"),
        },
        res = gres_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "Gres controller exited with error"),
            Err(e) => tracing::error!(error = %e, "Gres controller task panicked"),
        },
        res = gres_tenant_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "GresTenant controller exited with error"),
            Err(e) => tracing::error!(error = %e, "GresTenant controller task panicked"),
        },
        () = shutdown_signal() => tracing::info!("shutdown signal received"),
    }
    Ok(())
}

/// Resolve when SIGINT arrives, or when SIGTERM arrives on Unix. Kubernetes
/// sends SIGTERM on pod shutdown. SIGINT covers `Ctrl+C` for local runs and
/// also works on Windows.
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
