//! Instrumented orders-analytics demo. Three roles, all on crabka-broker +
//! the schema registry, emitting metrics/logs/traces/profiles via crabka libs.

#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(unix, feature = "heap-profiling"))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_client_streams::processor::serde::SerdeRole;
use crabka_client_streams::{SchemaSerde, Serde};
use crabka_schema_serde::format::protobuf::ProtobufSerde;
use crabka_schema_serde::{CacheConfig, RegistryClient, SchemaCache, set_default_registry};
use observability_demo_app::{Order, order_at};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Role {
    Produce,
    Stream,
    Consume,
}

#[derive(Debug, Parser)]
#[command(name = "observability-demo-app")]
struct Cli {
    #[arg(long, value_enum)]
    role: Role,
    #[arg(long, env = "CRABKA_DEMO_BOOTSTRAP", default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(
        long,
        env = "CRABKA_DEMO_REGISTRY",
        default_value = "http://127.0.0.1:8081"
    )]
    registry: String,
    #[arg(long, default_value = "orders")]
    input_topic: String,
    #[arg(long, default_value = "order-counts")]
    output_topic: String,
    #[arg(long, env = "CRABKA_DEMO_ORDERS_PER_SEC", default_value_t = 50)]
    orders_per_sec: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    let telemetry = crabka_telemetry::init(
        crabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "demo-app",
            env!("CARGO_PKG_VERSION"),
            "observability-demo-app",
        ),
        "observability_demo_app=info,info",
        "info",
        "observability-demo-app",
    )?;
    crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;

    match cli.role {
        Role::Produce => run_produce(&cli).await?,
        Role::Stream => run_stream(&cli).await?,
        Role::Consume => run_consume(&cli).await?,
    }
    telemetry.shutdown();
    Ok(())
}

#[tracing::instrument(skip(cli))]
async fn run_produce(cli: &Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cache = SchemaCache::new(
        RegistryClient::new(cli.registry.clone()),
        CacheConfig::default(),
    );
    set_default_registry(Arc::clone(&cache));
    let serde: SchemaSerde<Order, ProtobufSerde<Order>> =
        SchemaSerde::new(ProtobufSerde::<Order>::value(&cache));
    // Intern the value subject for the input topic, then resolve ids.
    serde.prepare(&cli.input_topic, SerdeRole::Value);
    cache.prewarm().await?;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(cli.bootstrap.clone())
            .acks(Acks::All)
            .build()
            .await?,
    );

    if cli.orders_per_sec == 0 {
        tracing::warn!("CRABKA_DEMO_ORDERS_PER_SEC=0 — producer paused");
        futures_idle().await;
        return Ok(());
    }
    #[allow(clippy::cast_precision_loss)]
    let per_sec = cli.orders_per_sec as f64;
    let period = Duration::from_secs_f64(1.0 / per_sec);
    let mut tick = tokio::time::interval(period);
    let mut i: u64 = 0;
    loop {
        tick.tick().await;
        let mut order = order_at(i);
        order.ts_ms = i64::try_from(i).unwrap_or(i64::MAX); // monotonic demo clock
        if order.amount.abs() < f64::EPSILON {
            tracing::warn!(order_id = %order.order_id, "anomalous zero-amount order");
        }
        let value = serde.serialize(&cli.input_topic, &order);
        producer
            .send(ProducerRecord {
                topic: cli.input_topic.clone(),
                key: Some(bytes::Bytes::from(order.category.clone().into_bytes())),
                value: Some(value),
                ..Default::default()
            })
            .await
            .await??;
        i += 1;
        if i.is_multiple_of(100) {
            tracing::info!(produced = i, "orders produced");
        }
    }
}

async fn run_stream(cli: &Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = crabka_client_streams::StreamsApp::builder()
        .bootstrap(cli.bootstrap.clone())
        .application_id("orders-analytics")
        .schema_registry(cli.registry.clone())
        .build();
    let topology = app.streams_builder();
    topology
        .stream::<String, Order>([cli.input_topic.as_str()])
        .group_by_key()
        .count("orders-by-category-store")
        .to_stream()
        .to(cli.output_topic.clone());
    tracing::info!("orders-analytics streams app starting");
    let mut streams = app.run(topology).await?;
    // Run until Ctrl-C.
    tokio::signal::ctrl_c().await.ok();
    streams.close().await?;
    Ok(())
}

async fn run_consume(cli: &Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // `Consumer` uses a `bon` builder; `subscribe` is a builder PARAMETER
    // (Vec<String>), and the finisher is `.build().await` (no separate
    // `.subscribe()` call). See crates/client-consumer/src/consumer.rs.
    let mut consumer = crabka_client_consumer::Consumer::builder()
        .bootstrap(cli.bootstrap.clone())
        .group_id("orders-analytics-consumer")
        .subscribe([cli.output_topic.clone()])
        .build()
        .await?;
    loop {
        let records = consumer.poll(Duration::from_millis(500)).await?;
        for record in records {
            tracing::info!(
                topic = %cli.output_topic,
                key = ?record.key,
                "consumed aggregated count"
            );
        }
    }
}

async fn futures_idle() {
    // Park forever (used when production is paused).
    std::future::pending::<()>().await;
}
