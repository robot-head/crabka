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

use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use crabka_client_consumer::{Consumer, ConsumerRecord};
use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_client_streams::{
    ClientDnsTimeout, SchemaSerde, Serde, StreamsCommitInterval, StreamsPollInterval,
    StreamsRebalanceTimeout, processor::serde::SerdeRole,
};
use crabka_schema_serde::{
    CacheConfig, RegistryClient, SchemaCache, format::protobuf::ProtobufSerde, set_default_registry,
};
use observability_demo_app::{
    Order, classify_outcome, is_anomalous,
    metrics::{DemoMetrics, metrics_router},
    order_at,
};
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
    orders_per_sec: u32,
    /// Kafka Streams broker DNS timeout in milliseconds.
    #[arg(long, env = "CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS")]
    streams_broker_dns_timeout_ms: Option<NonZeroU64>,
    /// Client Streams processing poll interval in milliseconds.
    #[arg(long, env = "CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS")]
    streams_poll_interval_ms: Option<NonZeroU64>,
    /// Client Streams commit interval in milliseconds.
    #[arg(long, env = "CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS")]
    streams_commit_interval_ms: Option<NonZeroU64>,
    /// Client Streams rebalance timeout in milliseconds.
    #[arg(long, env = "CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS")]
    streams_rebalance_timeout_ms: Option<NonZeroU64>,
}

fn effective_streams_broker_dns_timeout(cli: &Cli) -> std::io::Result<ClientDnsTimeout> {
    if cli.role != Role::Stream
        && let Some(milliseconds) = cli.streams_broker_dns_timeout_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-broker-dns-timeout-ms ({} ms) is only valid with --role stream",
                milliseconds.get(),
            ),
        ));
    }

    cli.streams_broker_dns_timeout_ms.map_or_else(
        || Ok(ClientDnsTimeout::default()),
        |milliseconds| {
            ClientDnsTimeout::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn effective_streams_runtime_cadence(
    cli: &Cli,
) -> std::io::Result<(StreamsPollInterval, StreamsCommitInterval)> {
    if cli.role != Role::Stream {
        if let Some(milliseconds) = cli.streams_poll_interval_ms {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "--streams-poll-interval-ms ({} ms) is only valid with --role stream",
                    milliseconds.get(),
                ),
            ));
        }
        if let Some(milliseconds) = cli.streams_commit_interval_ms {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "--streams-commit-interval-ms ({} ms) is only valid with --role stream",
                    milliseconds.get(),
                ),
            ));
        }
    }

    let poll = cli.streams_poll_interval_ms.map_or_else(
        || Ok(StreamsPollInterval::default()),
        |milliseconds| {
            StreamsPollInterval::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )?;
    let commit = cli.streams_commit_interval_ms.map_or_else(
        || Ok(StreamsCommitInterval::default()),
        |milliseconds| {
            StreamsCommitInterval::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )?;
    Ok((poll, commit))
}

fn effective_streams_rebalance_timeout(cli: &Cli) -> std::io::Result<StreamsRebalanceTimeout> {
    if cli.role != Role::Stream
        && let Some(milliseconds) = cli.streams_rebalance_timeout_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-rebalance-timeout-ms ({} ms) is only valid with --role stream",
                milliseconds.get(),
            ),
        ));
    }

    cli.streams_rebalance_timeout_ms.map_or_else(
        || Ok(StreamsRebalanceTimeout::default()),
        |milliseconds| {
            StreamsRebalanceTimeout::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let streams_broker_dns_timeout = effective_streams_broker_dns_timeout(&cli)?;
    let (streams_poll_interval, streams_commit_interval) = effective_streams_runtime_cadence(&cli)?;
    let streams_rebalance_timeout = effective_streams_rebalance_timeout(&cli)?;

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
        Role::Stream => {
            run_stream(
                &cli,
                streams_broker_dns_timeout,
                streams_poll_interval,
                streams_commit_interval,
                streams_rebalance_timeout,
            )
            .await?;
        }
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
    let per_sec = f64::from(cli.orders_per_sec);
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

async fn run_stream(
    cli: &Cli,
    broker_dns_timeout: ClientDnsTimeout,
    streams_poll_interval: StreamsPollInterval,
    streams_commit_interval: StreamsCommitInterval,
    streams_rebalance_timeout: StreamsRebalanceTimeout,
) -> Result<(), BoxError> {
    let app = crabka_client_streams::StreamsApp::builder()
        .bootstrap(cli.bootstrap.clone())
        .application_id("orders-analytics")
        .schema_registry(cli.registry.clone())
        .broker_dns_timeout(broker_dns_timeout)
        .poll_interval(streams_poll_interval)
        .commit_interval(streams_commit_interval)
        .rebalance_timeout(streams_rebalance_timeout)
        .build();
    let topology = app.streams_builder();
    topology
        .stream::<String, Order>([cli.input_topic.as_str()])
        .group_by_key()
        .count("orders-by-category-store")
        .to_stream()
        .to(cli.output_topic.clone());
    tracing::info!("orders-analytics streams app starting");
    let streams = app.run(topology).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_broker_dns_timeout_uses_default_and_cli_override() {
        let defaults = Cli {
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            registry: "http://127.0.0.1:8081".to_owned(),
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            orders_per_sec: 50,
            streams_broker_dns_timeout_ms: None,
            streams_poll_interval_ms: None,
            streams_commit_interval_ms: None,
            streams_rebalance_timeout_ms: None,
        };
        assert_eq!(
            effective_streams_broker_dns_timeout(&defaults).expect("typed default"),
            crabka_client_streams::ClientDnsTimeout::default()
        );

        let overridden = Cli {
            streams_broker_dns_timeout_ms: std::num::NonZeroU64::new(37),
            ..defaults
        };
        assert_eq!(
            effective_streams_broker_dns_timeout(&overridden)
                .expect("typed override")
                .milliseconds(),
            37
        );
    }

    #[test]
    fn streams_broker_dns_timeout_rejects_zero_and_non_stream_roles() {
        Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-broker-dns-timeout-ms",
            "0",
        ])
        .expect_err("zero must fail in Clap");

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-broker-dns-timeout-ms",
            "37",
        ])
        .expect("parse before role validation");
        let error = effective_streams_broker_dns_timeout(&produce).expect_err("Stream-only option");
        assert_eq!(
            error.to_string(),
            "--streams-broker-dns-timeout-ms (37 ms) is only valid with --role stream"
        );
    }

    #[test]
    fn streams_runtime_cadence_uses_defaults_and_independent_overrides() {
        let defaults = Cli {
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            registry: "http://127.0.0.1:8081".to_owned(),
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            orders_per_sec: 50,
            streams_broker_dns_timeout_ms: None,
            streams_poll_interval_ms: None,
            streams_commit_interval_ms: None,
            streams_rebalance_timeout_ms: None,
        };
        let (poll, commit) = effective_streams_runtime_cadence(&defaults).expect("typed defaults");
        assert_eq!(poll, crabka_client_streams::StreamsPollInterval::default());
        assert_eq!(
            commit,
            crabka_client_streams::StreamsCommitInterval::default()
        );

        let overridden = Cli {
            streams_poll_interval_ms: std::num::NonZeroU64::new(37),
            streams_commit_interval_ms: std::num::NonZeroU64::new(41),
            ..defaults
        };
        let (poll, commit) =
            effective_streams_runtime_cadence(&overridden).expect("typed overrides");
        assert_eq!(poll.milliseconds(), 37);
        assert_eq!(commit.milliseconds(), 41);
    }

    #[test]
    fn streams_runtime_cadence_rejects_zero_and_non_stream_roles() {
        Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-poll-interval-ms",
            "0",
        ])
        .expect_err("zero poll interval");
        Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-commit-interval-ms",
            "0",
        ])
        .expect_err("zero commit interval");

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-poll-interval-ms",
            "37",
        ])
        .expect("parse before role validation");
        let error = effective_streams_runtime_cadence(&produce).expect_err("Stream-only option");
        assert_eq!(
            error.to_string(),
            "--streams-poll-interval-ms (37 ms) is only valid with --role stream"
        );
    }

    #[test]
    fn streams_rebalance_timeout_uses_default_and_cli_override() {
        let defaults = Cli {
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            registry: "http://127.0.0.1:8081".to_owned(),
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            orders_per_sec: 50,
            streams_broker_dns_timeout_ms: None,
            streams_poll_interval_ms: None,
            streams_commit_interval_ms: None,
            streams_rebalance_timeout_ms: None,
        };
        assert_eq!(
            effective_streams_rebalance_timeout(&defaults).expect("typed default"),
            crabka_client_streams::StreamsRebalanceTimeout::default()
        );

        let overridden = Cli {
            streams_rebalance_timeout_ms: std::num::NonZeroU64::new(45_000),
            ..defaults
        };
        assert_eq!(
            effective_streams_rebalance_timeout(&overridden)
                .expect("typed override")
                .milliseconds(),
            45_000
        );
    }

    #[test]
    fn streams_rebalance_timeout_rejects_zero_overflow_and_non_stream_roles() {
        Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-rebalance-timeout-ms",
            "0",
        ])
        .expect_err("zero must fail in Clap");

        let overflow = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-rebalance-timeout-ms",
            "2147483648",
        ])
        .expect("parse before typed validation");
        let error =
            effective_streams_rebalance_timeout(&overflow).expect_err("i32 overflow must fail");
        assert!(error.to_string().contains("streams rebalance timeout"));

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-rebalance-timeout-ms",
            "45000",
        ])
        .expect("parse before role validation");
        let error = effective_streams_rebalance_timeout(&produce).expect_err("Stream-only option");
        assert_eq!(
            error.to_string(),
            "--streams-rebalance-timeout-ms (45000 ms) is only valid with --role stream"
        );
    }
}
