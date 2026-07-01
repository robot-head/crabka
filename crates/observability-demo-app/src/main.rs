//! Instrumented orders-analytics demo. Three roles, all on crabka-broker + the
//! schema registry, emitting metrics/logs/traces/profiles via crabka libs.
//!
//! Cross-service traces: the `produce` role injects the current span's W3C trace
//! context (`traceparent`) into each record's Kafka headers; the `consume` role
//! extracts it so its multi-stage processing spans (validate → enrich →
//! `fraud_check` → fulfill) join the producer's trace. Opening one of these in
//! Grafana Tempo shows a single distributed trace spanning demo-produce → the
//! broker's Produce/Fetch server spans → demo-consume. The `stream` role keeps
//! the Kafka-Streams aggregation (`group_by` → count → order-counts) as the
//! streams showcase.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use crabka_client_consumer::{Consumer, ConsumerRecord};
use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_client_streams::processor::serde::SerdeRole;
use crabka_client_streams::{SchemaSerde, Serde};
use crabka_schema_serde::format::protobuf::ProtobufSerde;
use crabka_schema_serde::{CacheConfig, RegistryClient, SchemaCache, set_default_registry};
use observability_demo_app::metrics::{DemoMetrics, metrics_router};
use observability_demo_app::{Order, classify_outcome, is_anomalous, order_at};
use tracing::Instrument as _;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Value serde used by both the producer (serialize) and the traced consumer
/// (deserialize) — the same protobuf `Order` schema resolved via the registry.
type OrderSerde = SchemaSerde<Order, ProtobufSerde<Order>>;

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
async fn main() -> Result<(), BoxError> {
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
    // Business metrics on the shared admin port (:9404) so Alloy scrapes them
    // alongside pprof (crabka_demo_* families).
    let metrics = DemoMetrics::new();
    crabka_telemetry::profiling::serve_admin_from_env_with(
        "0.0.0.0:9404",
        metrics_router(metrics.registry.clone()),
    )
    .await?;

    match cli.role {
        Role::Produce => run_produce(&cli, &metrics).await?,
        Role::Stream => run_stream(&cli).await?,
        Role::Consume => run_consume(&cli, &metrics).await?,
    }
    telemetry.shutdown();
    Ok(())
}

/// Build the protobuf `Order` value serde and warm the registry subject for
/// `topic`. Shared by the producer and the traced consumer.
async fn order_serde(cli: &Cli, topic: &str) -> Result<OrderSerde, BoxError> {
    let cache = SchemaCache::new(
        RegistryClient::new(cli.registry.clone()),
        CacheConfig::default(),
    );
    set_default_registry(Arc::clone(&cache));
    let serde: OrderSerde = SchemaSerde::new(ProtobufSerde::<Order>::value(&cache));
    serde.prepare(topic, SerdeRole::Value);
    cache.prewarm().await?;
    Ok(serde)
}

// NOT `#[tracing::instrument]`: each `produce_order` span below must be a trace
// ROOT (one distributed trace per order) rather than a child of a single
// process-lifetime span.
async fn run_produce(cli: &Cli, metrics: &DemoMetrics) -> Result<(), BoxError> {
    let serde = order_serde(cli, &cli.input_topic).await?;

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
        let value = serde.serialize(&cli.input_topic, &order);

        // A per-order PRODUCER span whose trace context is injected into the
        // record headers below; ~sample_ratio of these become full cross-service
        // traces once the consumer continues them.
        let span = tracing::info_span!(
            "produce_order",
            otel.kind = "producer",
            otel.name = "orders publish",
            messaging.system = "kafka",
            messaging.destination.name = %cli.input_topic,
            messaging.operation = "publish",
            demo.order.id = %order.order_id,
            demo.order.category = %order.category,
            demo.order.region = %order.region,
            demo.order.warehouse = %order.warehouse,
            demo.order.payment_method = %order.payment_method,
            demo.order.customer_tier = %order.customer_tier,
            demo.order.amount = order.amount,
            demo.order.quantity = order.quantity,
        );

        let producer = Arc::clone(&producer);
        let metrics = metrics.clone();
        let topic = cli.input_topic.clone();
        async move {
            if is_anomalous(&order) {
                tracing::warn!(order_id = %order.order_id, "anomalous zero-amount order");
            }
            // Inject the CURRENT (produce_order) span's W3C trace context into
            // the record headers so the consumer can continue this trace, plus a
            // couple of business headers to show custom Kafka headers round-trip
            // through the broker verbatim.
            let mut headers: Vec<Header> = crabka_telemetry::propagation::current_trace_headers()
                .into_iter()
                .map(|(k, v)| Header {
                    key: k,
                    value: Some(Bytes::from(v.into_bytes())),
                })
                .collect();
            headers.push(Header {
                key: "x-demo-region".into(),
                value: Some(Bytes::from(order.region.clone().into_bytes())),
            });
            headers.push(Header {
                key: "x-demo-tier".into(),
                value: Some(Bytes::from(order.customer_tier.clone().into_bytes())),
            });

            let start = Instant::now();
            producer
                .send(ProducerRecord {
                    topic,
                    key: Some(Bytes::from(order.category.clone().into_bytes())),
                    value: Some(value),
                    headers,
                    ..Default::default()
                })
                .await
                .await??;
            metrics.record_produced(
                &order.category,
                &order.region,
                &order.payment_method,
                order.amount,
                start.elapsed().as_secs_f64(),
            );
            Ok::<(), BoxError>(())
        }
        .instrument(span)
        .await?;

        i += 1;
        if i.is_multiple_of(100) {
            tracing::info!(produced = i, "orders produced");
        }
    }
}

async fn run_stream(cli: &Cli) -> Result<(), BoxError> {
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

/// The traced order processor. Consumes the RAW `orders` topic, continues the
/// producer's distributed trace via the `traceparent` header, and runs a
/// multi-stage processing pipeline (validate → enrich → `fraud_check` → fulfill),
/// each stage a child span with a per-stage latency metric.
async fn run_consume(cli: &Cli, metrics: &DemoMetrics) -> Result<(), BoxError> {
    let serde = order_serde(cli, &cli.input_topic).await?;

    let mut consumer = Consumer::builder()
        .bootstrap(cli.bootstrap.clone())
        .group_id("orders-processor")
        .subscribe([cli.input_topic.clone()])
        .build()
        .await?;
    tracing::info!(topic = %cli.input_topic, "order processor starting");
    loop {
        let records = consumer.poll(Duration::from_millis(500)).await?;
        for record in records {
            process_order_record(&serde, &cli.input_topic, metrics, &record).await;
        }
    }
}

/// Build the consumer-side `process_order` span, make it a child of the
/// producer's trace (from the record's `traceparent` header), and run the
/// staged processing under it.
async fn process_order_record(
    serde: &OrderSerde,
    topic: &str,
    metrics: &DemoMetrics,
    record: &ConsumerRecord,
) {
    let span = tracing::info_span!(
        "process_order",
        otel.kind = "consumer",
        otel.name = "orders process",
        messaging.system = "kafka",
        messaging.source.name = %topic,
        messaging.operation = "process",
        messaging.kafka.partition = record.partition,
        messaging.kafka.offset = record.offset,
        demo.order.id = tracing::field::Empty,
        demo.order.category = tracing::field::Empty,
        demo.order.region = tracing::field::Empty,
        demo.order.outcome = tracing::field::Empty,
    );
    // Continue the producer's trace when the record carries one.
    crabka_telemetry::propagation::set_remote_parent(
        &span,
        record
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
    );
    process_order_inner(serde, topic, metrics, record)
        .instrument(span)
        .await;
}

async fn process_order_inner(
    serde: &OrderSerde,
    topic: &str,
    metrics: &DemoMetrics,
    record: &ConsumerRecord,
) {
    let start = Instant::now();
    let Some(value) = record.value.as_deref() else {
        tracing::warn!("order record has no value");
        return;
    };
    let order = match serde.deserialize(topic, value) {
        Ok(order) => order,
        Err(e) => {
            tracing::error!(error = %e, "failed to deserialize order");
            return;
        }
    };

    let span = tracing::Span::current();
    span.record("demo.order.id", order.order_id.as_str());
    span.record("demo.order.category", order.category.as_str());
    span.record("demo.order.region", order.region.as_str());

    // Each stage is a child span with simulated work + a per-stage latency
    // metric, so the trace waterfall shows the processing pipeline.
    stage(metrics, "validate", Duration::from_micros(150)).await;
    stage(metrics, "enrich", Duration::from_micros(400)).await;
    stage(metrics, "fraud_check", Duration::from_micros(200)).await;
    stage(metrics, "fulfill", Duration::from_micros(300)).await;

    let outcome = classify_outcome(&order);
    span.record("demo.order.outcome", outcome);
    metrics.record_processed(
        &order.category,
        &order.region,
        outcome,
        start.elapsed().as_secs_f64(),
    );

    match outcome {
        "anomalous" => {
            tracing::warn!(order_id = %order.order_id, "dropped anomalous zero-amount order");
        }
        "fraud_rejected" => {
            tracing::warn!(
                order_id = %order.order_id,
                amount = order.amount,
                payment_method = %order.payment_method,
                "rejected suspected-fraud order"
            );
        }
        _ => {
            tracing::info!(
                order_id = %order.order_id,
                category = %order.category,
                region = %order.region,
                warehouse = %order.warehouse,
                "fulfilled order"
            );
        }
    }
}

/// One processing stage: a child span named `name` with simulated work and a
/// per-stage latency observation.
async fn stage(metrics: &DemoMetrics, name: &'static str, work: Duration) {
    let span = tracing::info_span!("stage", otel.name = name, demo.stage = name);
    async move {
        let start = Instant::now();
        tokio::time::sleep(work).await;
        metrics.record_stage(name, start.elapsed().as_secs_f64());
    }
    .instrument(span)
    .await;
}

async fn futures_idle() {
    // Park forever (used when production is paused).
    std::future::pending::<()>().await;
}
