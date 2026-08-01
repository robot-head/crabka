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
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use crabka_client_consumer::{
    Assignor, AutoOffsetReset, Consumer, ConsumerFetchMaxBytes, ConsumerFetchPartitionMaxBytes,
    ConsumerLeaveGroupTimeout, ConsumerRecord, ConsumerRetryPolicy,
    ConsumerSubscriptionMetadataRefreshInterval, IsolationLevel,
};
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
    FetchMinBytes,
};
use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_client_streams::{
    ClientDnsTimeout, SchemaSerde, Serde, StreamsCommitInterval,
    StreamsInteractiveQueryQueueCapacity, StreamsJoinRetryBackoff, StreamsLeaveHeartbeatTimeout,
    StreamsPollInterval, StreamsRebalanceTimeout, StreamsStateStoreCacheMaxBytes,
    processor::serde::SerdeRole,
};
use crabka_schema_serde::{
    CacheConfig, RegistryClient, SchemaCache, SchemaFetchRetryPolicy,
    format::protobuf::ProtobufSerde, set_default_registry,
};
use crabka_units::{fmt::Human as _, parse, prelude::*};
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
    #[command(flatten)]
    profiling: crabka_telemetry::profiling::ProfilingConfig,
    #[arg(long, env = "CRABKA_DEMO_ROLE", value_enum)]
    role: Role,
    #[arg(long, env = "CRABKA_DEMO_BOOTSTRAP", default_value = "127.0.0.1:9092")]
    bootstrap: String,
    /// Capacity shared by every outbound Kafka client owned by this process.
    #[arg(
        long,
        env = "CRABKA_DEMO_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    /// Maximum frame size shared by every outbound Kafka client.
    #[arg(
        long,
        env = "CRABKA_DEMO_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    #[arg(
        long,
        env = "CRABKA_DEMO_REGISTRY",
        default_value = "http://127.0.0.1:8081"
    )]
    registry: String,
    /// Initial delay before retrying a transient Schema Registry fetch failure.
    #[arg(
        long,
        env = "CRABKA_DEMO_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF",
        value_parser = parse::positive_time
    )]
    schema_fetch_retry_initial_backoff: Option<Time>,
    /// Maximum delay between transient Schema Registry fetch retries.
    #[arg(
        long,
        env = "CRABKA_DEMO_SCHEMA_FETCH_RETRY_MAX_BACKOFF",
        value_parser = parse::positive_time
    )]
    schema_fetch_retry_max_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_DEMO_INPUT_TOPIC", default_value = "orders")]
    input_topic: String,
    #[arg(long, env = "CRABKA_DEMO_OUTPUT_TOPIC", default_value = "order-counts")]
    output_topic: String,
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_APPLICATION_ID",
        default_value = "orders-analytics",
        value_parser = parse_non_empty_string
    )]
    streams_application_id: String,
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_GROUP_ID",
        default_value = "orders-processor",
        value_parser = parse_non_empty_string
    )]
    consumer_group_id: String,
    #[arg(
        long,
        env = "CRABKA_DEMO_ORDERS_PER_SEC",
        default_value = "50Hz",
        value_parser = parse_nonnegative_frequency
    )]
    orders_per_sec: Frequency,
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_POLL_TIMEOUT",
        default_value = "500ms",
        value_parser = parse::positive_time
    )]
    consumer_poll_timeout: Time,
    #[arg(long, env = "CRABKA_DEMO_VALIDATE_WORK", default_value = "150us", value_parser = parse_nonnegative_time)]
    validate_work: Time,
    #[arg(long, env = "CRABKA_DEMO_ENRICH_WORK", default_value = "400us", value_parser = parse_nonnegative_time)]
    enrich_work: Time,
    #[arg(long, env = "CRABKA_DEMO_FRAUD_CHECK_WORK", default_value = "200us", value_parser = parse_nonnegative_time)]
    fraud_check_work: Time,
    #[arg(long, env = "CRABKA_DEMO_FULFILL_WORK", default_value = "300us", value_parser = parse_nonnegative_time)]
    fulfill_work: Time,
    /// Classic Consumer best-effort leave-group timeout.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT",
        value_parser = parse::positive_time
    )]
    consumer_leave_group_timeout: Option<Time>,
    /// Classic Consumer subscribed-topic metadata refresh interval.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL",
        value_parser = parse::positive_time
    )]
    consumer_subscription_metadata_refresh_interval: Option<Time>,
    /// Timeout for each classic Consumer startup attempt.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_STARTUP_ATTEMPT_TIMEOUT",
        value_parser = parse::positive_time
    )]
    consumer_startup_attempt_timeout: Option<Time>,
    /// Wall-clock deadline for classic Consumer startup.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_STARTUP_DEADLINE",
        value_parser = parse::positive_time
    )]
    consumer_startup_deadline: Option<Time>,
    /// Initial classic Consumer startup retry backoff.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_STARTUP_INITIAL_BACKOFF",
        value_parser = parse::positive_time
    )]
    consumer_startup_initial_backoff: Option<Time>,
    /// Maximum classic Consumer startup retry backoff.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_STARTUP_MAX_BACKOFF",
        value_parser = parse::positive_time
    )]
    consumer_startup_max_backoff: Option<Time>,
    /// Timeout for classic Consumer coordinator retry loops.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_COORDINATOR_RETRY_TIMEOUT",
        value_parser = parse::positive_time
    )]
    consumer_coordinator_retry_timeout: Option<Time>,
    /// Initial classic Consumer coordinator retry backoff.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_COORDINATOR_INITIAL_BACKOFF",
        value_parser = parse::positive_time
    )]
    consumer_coordinator_initial_backoff: Option<Time>,
    /// Maximum classic Consumer coordinator retry backoff.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_COORDINATOR_MAX_BACKOFF",
        value_parser = parse::positive_time
    )]
    consumer_coordinator_max_backoff: Option<Time>,
    /// Minimum bytes requested by the classic Consumer fetcher.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_FETCH_MIN",
        value_parser = parse::positive_byte_size
    )]
    consumer_fetch_min: Option<ByteSize>,
    /// Total response-byte budget for one classic Consumer fetch.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_FETCH_MAX",
        value_parser = parse::positive_byte_size
    )]
    consumer_fetch_max: Option<ByteSize>,
    /// Per-partition response-byte budget for one classic Consumer fetch.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_FETCH_PARTITION_MAX",
        value_parser = parse::positive_byte_size
    )]
    consumer_fetch_partition_max: Option<ByteSize>,
    /// Classic Consumer group session timeout.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT",
        value_parser = parse::positive_time
    )]
    consumer_session_timeout: Option<Time>,
    /// Classic Consumer group rebalance timeout.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_REBALANCE_TIMEOUT",
        value_parser = parse::positive_time
    )]
    consumer_rebalance_timeout: Option<Time>,
    /// Classic Consumer group heartbeat interval.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_HEARTBEAT_INTERVAL",
        value_parser = parse::positive_time
    )]
    consumer_heartbeat_interval: Option<Time>,
    /// Classic Consumer request and connection timeout.
    #[arg(
        long,
        env = "CRABKA_DEMO_CONSUMER_REQUEST_TIMEOUT",
        value_parser = parse::positive_time
    )]
    consumer_request_timeout: Option<Time>,
    /// Offset-reset behavior when the classic Consumer has no valid offset.
    #[arg(long, env = "CRABKA_DEMO_CONSUMER_AUTO_OFFSET_RESET")]
    consumer_auto_offset_reset: Option<AutoOffsetReset>,
    /// Transaction visibility for classic Consumer fetches.
    #[arg(long, env = "CRABKA_DEMO_CONSUMER_ISOLATION_LEVEL")]
    consumer_isolation_level: Option<IsolationLevel>,
    /// Partition assignment strategy for the classic Consumer group.
    #[arg(long, env = "CRABKA_DEMO_CONSUMER_ASSIGNOR")]
    consumer_assignor: Option<Assignor>,
    /// Kafka Streams broker DNS timeout.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT",
        value_parser = parse::positive_time
    )]
    streams_broker_dns_timeout: Option<Time>,
    /// Client Streams processing poll interval.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_POLL_INTERVAL",
        value_parser = parse::positive_time
    )]
    streams_poll_interval: Option<Time>,
    /// Client Streams commit interval.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_COMMIT_INTERVAL",
        value_parser = parse::positive_time
    )]
    streams_commit_interval: Option<Time>,
    /// Client Streams rebalance timeout.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT",
        value_parser = parse::positive_time
    )]
    streams_rebalance_timeout: Option<Time>,
    /// Client Streams final leave-heartbeat timeout.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT",
        value_parser = parse::positive_time
    )]
    streams_leave_heartbeat_timeout: Option<Time>,
    /// Client Streams initial join retry backoff.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF",
        value_parser = parse::positive_time
    )]
    streams_join_retry_backoff: Option<Time>,
    /// Capacity shared by the Client Streams interactive-query request queues.
    #[arg(long, env = "CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY")]
    streams_interactive_query_queue_capacity: Option<NonZeroUsize>,
    /// Client Streams state-store record-cache budget; zero disables it.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX",
        value_parser = parse::non_negative_byte_size
    )]
    streams_state_store_cache_max: Option<ByteSize>,
    /// Minimum bytes requested by the Streams fetcher.
    #[arg(
        long,
        env = "CRABKA_DEMO_STREAMS_FETCH_MIN",
        value_parser = parse_fetch_min
    )]
    streams_fetch_min: Option<ByteSize>,
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

fn parse_fetch_min(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    FetchMinBytes::try_from(value).map(FetchMinBytes::size)
}

fn parse_non_empty_string(value: &str) -> Result<String, String> {
    refined_type::rule::NonEmptyString::new(value.to_owned())
        .map(refined_type::Refined::into_value)
        .map_err(|error| error.to_string())
}

fn parse_nonnegative_frequency(value: &str) -> Result<Frequency, String> {
    let value = parse::frequency(value).map_err(|error| error.to_string())?;
    if value < Frequency::ZERO {
        return Err("frequency must not be negative".to_owned());
    }
    Ok(value)
}

fn parse_nonnegative_time(value: &str) -> Result<Time, String> {
    let value = parse::time(value).map_err(|error| error.to_string())?;
    if value < Time::ZERO {
        return Err("time must not be negative".to_owned());
    }
    Ok(value)
}

fn client_resource_policy(cli: &Cli) -> (ConnectionDispatchQueueCapacity, ClientFrameMax) {
    (
        ConnectionDispatchQueueCapacity::new(cli.client_dispatch_queue_capacity)
            .expect("validated demo client dispatch queue capacity"),
        ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated demo client frame maximum"),
    )
}

fn effective_streams_fetch_min(cli: &Cli) -> std::io::Result<FetchMinBytes> {
    if cli.role != Role::Stream
        && let Some(fetch_min) = cli.streams_fetch_min
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-fetch-min ({}) is only valid with --role stream",
                fetch_min.human(),
            ),
        ));
    }

    Ok(cli
        .streams_fetch_min
        .map_or_else(FetchMinBytes::default, |fetch_min| {
            FetchMinBytes::try_from(fetch_min).expect("validated Streams fetch minimum")
        }))
}

fn effective_consumer_leave_group_timeout(cli: &Cli) -> std::io::Result<ConsumerLeaveGroupTimeout> {
    if cli.role != Role::Consume
        && let Some(timeout) = cli.consumer_leave_group_timeout
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--consumer-leave-group-timeout ({}) is only valid with --role consume",
                timeout.human(),
            ),
        ));
    }

    cli.consumer_leave_group_timeout.map_or_else(
        || Ok(ConsumerLeaveGroupTimeout::default()),
        |timeout| {
            ConsumerLeaveGroupTimeout::new(timeout.to_std())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn effective_consumer_subscription_metadata_refresh_interval(
    cli: &Cli,
) -> std::io::Result<ConsumerSubscriptionMetadataRefreshInterval> {
    if cli.role != Role::Consume
        && let Some(interval) = cli.consumer_subscription_metadata_refresh_interval
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--consumer-subscription-metadata-refresh-interval ({}) is only valid with --role consume",
                interval.human(),
            ),
        ));
    }

    cli.consumer_subscription_metadata_refresh_interval
        .map_or_else(
            || Ok(ConsumerSubscriptionMetadataRefreshInterval::default()),
            |interval| {
                ConsumerSubscriptionMetadataRefreshInterval::new(interval.to_std())
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
            },
        )
}

fn effective_consumer_retry_policy(cli: &Cli) -> std::io::Result<ConsumerRetryPolicy> {
    let configured = [
        (
            "--consumer-startup-attempt-timeout",
            cli.consumer_startup_attempt_timeout,
        ),
        ("--consumer-startup-deadline", cli.consumer_startup_deadline),
        (
            "--consumer-startup-initial-backoff",
            cli.consumer_startup_initial_backoff,
        ),
        (
            "--consumer-startup-max-backoff",
            cli.consumer_startup_max_backoff,
        ),
        (
            "--consumer-coordinator-retry-timeout",
            cli.consumer_coordinator_retry_timeout,
        ),
        (
            "--consumer-coordinator-initial-backoff",
            cli.consumer_coordinator_initial_backoff,
        ),
        (
            "--consumer-coordinator-max-backoff",
            cli.consumer_coordinator_max_backoff,
        ),
    ];
    if cli.role != Role::Consume
        && let Some((name, value)) = configured
            .into_iter()
            .find_map(|(name, value)| value.map(|value| (name, value)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{name} ({}) is only valid with --role consume",
                value.human()
            ),
        ));
    }

    let defaults = ConsumerRetryPolicy::default();
    ConsumerRetryPolicy::new(
        cli.consumer_startup_attempt_timeout
            .unwrap_or_else(|| defaults.startup_attempt_timeout()),
        cli.consumer_startup_deadline
            .unwrap_or_else(|| defaults.startup_deadline()),
        cli.consumer_startup_initial_backoff
            .unwrap_or_else(|| defaults.startup_initial_backoff()),
        cli.consumer_startup_max_backoff
            .unwrap_or_else(|| defaults.startup_max_backoff()),
        cli.consumer_coordinator_retry_timeout
            .unwrap_or_else(|| defaults.coordinator_retry_timeout()),
        cli.consumer_coordinator_initial_backoff
            .unwrap_or_else(|| defaults.coordinator_initial_backoff()),
        cli.consumer_coordinator_max_backoff
            .unwrap_or_else(|| defaults.coordinator_max_backoff()),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

fn effective_consumer_fetch_policy(cli: &Cli) -> std::io::Result<(ByteSize, ByteSize, ByteSize)> {
    let configured = [
        ("--consumer-fetch-min", cli.consumer_fetch_min),
        ("--consumer-fetch-max", cli.consumer_fetch_max),
        (
            "--consumer-fetch-partition-max",
            cli.consumer_fetch_partition_max,
        ),
    ];
    if cli.role != Role::Consume
        && let Some((name, value)) = configured
            .into_iter()
            .find_map(|(name, value)| value.map(|value| (name, value)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{name} ({}) is only valid with --role consume",
                value.human()
            ),
        ));
    }

    let min = FetchMinBytes::try_from(cli.consumer_fetch_min.unwrap_or_else(|| bytes(1)))
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
        .size();
    let max =
        ConsumerFetchMaxBytes::try_from(cli.consumer_fetch_max.unwrap_or_else(|| mebibytes(50)))
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
            .size();
    if min.bytes_i32() > max.bytes_i32() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "consumer fetch min must not exceed consumer fetch max",
        ));
    }
    let partition_max = ConsumerFetchPartitionMaxBytes::try_from(
        cli.consumer_fetch_partition_max
            .unwrap_or_else(|| mebibytes(1)),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
    .size();
    Ok((min, max, partition_max))
}

fn effective_consumer_timing(cli: &Cli) -> std::io::Result<(Time, Time, Time, Time)> {
    let configured = [
        ("--consumer-session-timeout", cli.consumer_session_timeout),
        (
            "--consumer-rebalance-timeout",
            cli.consumer_rebalance_timeout,
        ),
        (
            "--consumer-heartbeat-interval",
            cli.consumer_heartbeat_interval,
        ),
        ("--consumer-request-timeout", cli.consumer_request_timeout),
    ];
    if cli.role != Role::Consume
        && let Some((name, value)) = configured
            .into_iter()
            .find_map(|(name, value)| value.map(|value| (name, value)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{name} ({}) is only valid with --role consume",
                value.human()
            ),
        ));
    }
    Ok((
        cli.consumer_session_timeout.unwrap_or_else(|| secs(45)),
        cli.consumer_rebalance_timeout.unwrap_or_else(|| minutes(1)),
        cli.consumer_heartbeat_interval.unwrap_or_else(|| secs(3)),
        cli.consumer_request_timeout.unwrap_or_else(|| secs(30)),
    ))
}

fn effective_consumer_behavior(
    cli: &Cli,
) -> std::io::Result<(AutoOffsetReset, IsolationLevel, Assignor)> {
    let configured = [
        (
            "--consumer-auto-offset-reset",
            cli.consumer_auto_offset_reset.is_some(),
        ),
        (
            "--consumer-isolation-level",
            cli.consumer_isolation_level.is_some(),
        ),
        ("--consumer-assignor", cli.consumer_assignor.is_some()),
    ];
    if cli.role != Role::Consume
        && let Some((name, _)) = configured.into_iter().find(|(_, set)| *set)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{name} is only valid with --role consume"),
        ));
    }
    Ok((
        cli.consumer_auto_offset_reset
            .unwrap_or(AutoOffsetReset::Latest),
        cli.consumer_isolation_level
            .unwrap_or(IsolationLevel::ReadUncommitted),
        cli.consumer_assignor.unwrap_or(Assignor::Range),
    ))
}

fn effective_streams_broker_dns_timeout(cli: &Cli) -> std::io::Result<ClientDnsTimeout> {
    if cli.role != Role::Stream
        && let Some(timeout) = cli.streams_broker_dns_timeout
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-broker-dns-timeout ({}) is only valid with --role stream",
                timeout.human(),
            ),
        ));
    }

    cli.streams_broker_dns_timeout.map_or_else(
        || Ok(ClientDnsTimeout::default()),
        |timeout| {
            ClientDnsTimeout::new(timeout)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn effective_streams_runtime_cadence(
    cli: &Cli,
) -> std::io::Result<(StreamsPollInterval, StreamsCommitInterval)> {
    if cli.role != Role::Stream {
        if let Some(interval) = cli.streams_poll_interval {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "--streams-poll-interval ({}) is only valid with --role stream",
                    interval.human(),
                ),
            ));
        }
        if let Some(interval) = cli.streams_commit_interval {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "--streams-commit-interval ({}) is only valid with --role stream",
                    interval.human(),
                ),
            ));
        }
    }

    let poll = cli.streams_poll_interval.map_or_else(
        || Ok(StreamsPollInterval::default()),
        |interval| {
            StreamsPollInterval::new(interval.to_std())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )?;
    let commit = cli.streams_commit_interval.map_or_else(
        || Ok(StreamsCommitInterval::default()),
        |interval| {
            StreamsCommitInterval::new(interval.to_std())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )?;
    Ok((poll, commit))
}

fn effective_streams_rebalance_timeout(cli: &Cli) -> std::io::Result<StreamsRebalanceTimeout> {
    if cli.role != Role::Stream
        && let Some(timeout) = cli.streams_rebalance_timeout
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-rebalance-timeout ({}) is only valid with --role stream",
                timeout.human(),
            ),
        ));
    }

    cli.streams_rebalance_timeout.map_or_else(
        || Ok(StreamsRebalanceTimeout::default()),
        |timeout| {
            StreamsRebalanceTimeout::new(timeout.to_std())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn effective_streams_leave_heartbeat_timeout(
    cli: &Cli,
) -> std::io::Result<StreamsLeaveHeartbeatTimeout> {
    if cli.role != Role::Stream
        && let Some(timeout) = cli.streams_leave_heartbeat_timeout
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-leave-heartbeat-timeout ({}) is only valid with --role stream",
                timeout.human(),
            ),
        ));
    }

    cli.streams_leave_heartbeat_timeout.map_or_else(
        || Ok(StreamsLeaveHeartbeatTimeout::default()),
        |timeout| {
            StreamsLeaveHeartbeatTimeout::new(timeout.to_std())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn effective_streams_join_retry_backoff(cli: &Cli) -> std::io::Result<StreamsJoinRetryBackoff> {
    if cli.role != Role::Stream
        && let Some(backoff) = cli.streams_join_retry_backoff
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-join-retry-backoff ({}) is only valid with --role stream",
                backoff.human(),
            ),
        ));
    }

    cli.streams_join_retry_backoff.map_or_else(
        || Ok(StreamsJoinRetryBackoff::default()),
        |backoff| {
            StreamsJoinRetryBackoff::new(backoff.to_std())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn effective_streams_interactive_query_queue_capacity(
    cli: &Cli,
) -> std::io::Result<StreamsInteractiveQueryQueueCapacity> {
    if cli.role != Role::Stream
        && let Some(capacity) = cli.streams_interactive_query_queue_capacity
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-interactive-query-queue-capacity ({}) is only valid with --role stream",
                capacity.get(),
            ),
        ));
    }

    cli.streams_interactive_query_queue_capacity.map_or_else(
        || Ok(StreamsInteractiveQueryQueueCapacity::default()),
        |capacity| {
            StreamsInteractiveQueryQueueCapacity::new(capacity.get())
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn effective_streams_state_store_cache_max_bytes(
    cli: &Cli,
) -> std::io::Result<StreamsStateStoreCacheMaxBytes> {
    if cli.role != Role::Stream
        && let Some(size) = cli.streams_state_store_cache_max
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-state-store-cache-max ({}) is only valid with --role stream",
                size.human(),
            ),
        ));
    }

    cli.streams_state_store_cache_max.map_or_else(
        || Ok(StreamsStateStoreCacheMaxBytes::default()),
        |size| {
            StreamsStateStoreCacheMaxBytes::new(size)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}

fn schema_fetch_retry_policy(cli: &Cli) -> std::io::Result<SchemaFetchRetryPolicy> {
    let defaults = SchemaFetchRetryPolicy::default();
    SchemaFetchRetryPolicy::new(
        cli.schema_fetch_retry_initial_backoff
            .unwrap_or_else(|| defaults.initial_backoff()),
        cli.schema_fetch_retry_max_backoff
            .unwrap_or_else(|| defaults.max_backoff()),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(&cli);
    let streams_fetch_min = effective_streams_fetch_min(&cli)?;
    let schema_fetch_retry_policy = schema_fetch_retry_policy(&cli)?;
    let consumer_leave_group_timeout = effective_consumer_leave_group_timeout(&cli)?;
    let consumer_subscription_metadata_refresh_interval =
        effective_consumer_subscription_metadata_refresh_interval(&cli)?;
    let consumer_retry_policy = effective_consumer_retry_policy(&cli)?;
    let (consumer_fetch_min, consumer_fetch_max, consumer_fetch_partition_max) =
        effective_consumer_fetch_policy(&cli)?;
    let (
        consumer_session_timeout,
        consumer_rebalance_timeout,
        consumer_heartbeat_interval,
        consumer_request_timeout,
    ) = effective_consumer_timing(&cli)?;
    let (consumer_auto_offset_reset, consumer_isolation_level, consumer_assignor) =
        effective_consumer_behavior(&cli)?;
    let streams_broker_dns_timeout = effective_streams_broker_dns_timeout(&cli)?;
    let (streams_poll_interval, streams_commit_interval) = effective_streams_runtime_cadence(&cli)?;
    let streams_rebalance_timeout = effective_streams_rebalance_timeout(&cli)?;
    let streams_leave_heartbeat_timeout = effective_streams_leave_heartbeat_timeout(&cli)?;
    let streams_join_retry_backoff = effective_streams_join_retry_backoff(&cli)?;
    let streams_interactive_query_queue_capacity =
        effective_streams_interactive_query_queue_capacity(&cli)?;
    let streams_state_store_cache_max_bytes = effective_streams_state_store_cache_max_bytes(&cli)?;

    let telemetry = crabka_telemetry::init(
        crabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "demo-app",
            env!("CARGO_PKG_VERSION"),
            "observability-demo-app",
        )?,
        "observability_demo_app=info,info",
        "info",
        "observability-demo-app",
    )?;
    // Business metrics on the shared admin port (:9404) so Alloy scrapes them
    // alongside pprof (crabka_demo_* families).
    let metrics = DemoMetrics::new();
    crabka_telemetry::profiling::serve_admin_from_env_with_config(
        "0.0.0.0:9404",
        metrics_router(metrics.registry.clone()),
        cli.profiling.clone(),
    )
    .await?;

    match cli.role {
        Role::Produce => {
            run_produce(
                &cli,
                &metrics,
                schema_fetch_retry_policy,
                client_dispatch_queue_capacity,
                client_frame_max,
            )
            .await?;
        }
        Role::Stream => {
            run_stream(
                &cli,
                schema_fetch_retry_policy,
                streams_broker_dns_timeout,
                streams_poll_interval,
                streams_commit_interval,
                streams_rebalance_timeout,
                streams_leave_heartbeat_timeout,
                streams_join_retry_backoff,
                streams_interactive_query_queue_capacity,
                streams_state_store_cache_max_bytes,
                client_dispatch_queue_capacity,
                client_frame_max,
                streams_fetch_min,
            )
            .await?;
        }
        Role::Consume => {
            Box::pin(run_consume(
                &cli,
                &metrics,
                schema_fetch_retry_policy,
                consumer_leave_group_timeout,
                consumer_subscription_metadata_refresh_interval,
                consumer_retry_policy,
                consumer_fetch_min,
                consumer_fetch_max,
                consumer_fetch_partition_max,
                consumer_session_timeout,
                consumer_rebalance_timeout,
                consumer_heartbeat_interval,
                consumer_request_timeout,
                consumer_auto_offset_reset,
                consumer_isolation_level,
                consumer_assignor,
                client_dispatch_queue_capacity,
                client_frame_max,
            ))
            .await?;
        }
    }
    telemetry.shutdown();
    Ok(())
}

/// Build the protobuf `Order` value serde and warm the registry subject for
/// `topic`. Shared by the producer and the traced consumer.
async fn order_serde(
    cli: &Cli,
    topic: &str,
    fetch_retry_policy: SchemaFetchRetryPolicy,
) -> Result<OrderSerde, BoxError> {
    let cache = SchemaCache::new(
        RegistryClient::new(cli.registry.clone()),
        CacheConfig {
            fetch_retry_policy,
            ..CacheConfig::default()
        },
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
async fn run_produce(
    cli: &Cli,
    metrics: &DemoMetrics,
    schema_fetch_retry_policy: SchemaFetchRetryPolicy,
    client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    client_frame_max: ClientFrameMax,
) -> Result<(), BoxError> {
    let serde = order_serde(cli, &cli.input_topic, schema_fetch_retry_policy).await?;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(cli.bootstrap.clone())
            .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
            .frame_max(client_frame_max.size())
            .acks(Acks::All)
            .build()
            .await?,
    );

    if cli.orders_per_sec == Frequency::ZERO {
        tracing::warn!("CRABKA_DEMO_ORDERS_PER_SEC=0 — producer paused");
        futures_idle().await;
        return Ok(());
    }
    // The reciprocal of an order rate is the inter-order period.
    let period: Time = 1.0 / cli.orders_per_sec;
    let mut tick = tokio::time::interval(period.to_std());
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
                Time::from_std(start.elapsed()),
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

#[allow(clippy::too_many_arguments)]
async fn run_stream(
    cli: &Cli,
    schema_fetch_retry_policy: SchemaFetchRetryPolicy,
    broker_dns_timeout: ClientDnsTimeout,
    streams_poll_interval: StreamsPollInterval,
    streams_commit_interval: StreamsCommitInterval,
    streams_rebalance_timeout: StreamsRebalanceTimeout,
    streams_leave_heartbeat_timeout: StreamsLeaveHeartbeatTimeout,
    streams_join_retry_backoff: StreamsJoinRetryBackoff,
    streams_interactive_query_queue_capacity: StreamsInteractiveQueryQueueCapacity,
    streams_state_store_cache_max_bytes: StreamsStateStoreCacheMaxBytes,
    client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    client_frame_max: ClientFrameMax,
    streams_fetch_min: FetchMinBytes,
) -> Result<(), BoxError> {
    let app = crabka_client_streams::StreamsApp::builder()
        .bootstrap(cli.bootstrap.clone())
        .application_id(cli.streams_application_id.clone())
        .schema_registry(cli.registry.clone())
        .cache_config(CacheConfig {
            fetch_retry_policy: schema_fetch_retry_policy,
            ..CacheConfig::default()
        })
        .broker_dns_timeout(broker_dns_timeout)
        .client_dispatch_queue_capacity(client_dispatch_queue_capacity)
        .client_frame_max(client_frame_max)
        .fetch_min(streams_fetch_min)
        .poll_interval(streams_poll_interval)
        .commit_interval(streams_commit_interval)
        .rebalance_timeout(streams_rebalance_timeout)
        .leave_heartbeat_timeout(streams_leave_heartbeat_timeout)
        .join_retry_backoff(streams_join_retry_backoff)
        .interactive_query_queue_capacity(streams_interactive_query_queue_capacity)
        .cache_max_bytes(streams_state_store_cache_max_bytes.size())
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
#[allow(clippy::too_many_arguments)]
async fn run_consume(
    cli: &Cli,
    metrics: &DemoMetrics,
    schema_fetch_retry_policy: SchemaFetchRetryPolicy,
    consumer_leave_group_timeout: ConsumerLeaveGroupTimeout,
    consumer_subscription_metadata_refresh_interval: ConsumerSubscriptionMetadataRefreshInterval,
    consumer_retry_policy: ConsumerRetryPolicy,
    consumer_fetch_min: ByteSize,
    consumer_fetch_max: ByteSize,
    consumer_fetch_partition_max: ByteSize,
    consumer_session_timeout: Time,
    consumer_rebalance_timeout: Time,
    consumer_heartbeat_interval: Time,
    consumer_request_timeout: Time,
    consumer_auto_offset_reset: AutoOffsetReset,
    consumer_isolation_level: IsolationLevel,
    consumer_assignor: Assignor,
    client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    client_frame_max: ClientFrameMax,
) -> Result<(), BoxError> {
    let serde = order_serde(cli, &cli.input_topic, schema_fetch_retry_policy).await?;

    let mut consumer = Consumer::builder()
        .bootstrap(cli.bootstrap.clone())
        .group_id(cli.consumer_group_id.clone())
        .subscribe([cli.input_topic.clone()])
        .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
        .frame_max(client_frame_max.size())
        .leave_group_timeout(consumer_leave_group_timeout.duration().as_time())
        .subscription_metadata_refresh_interval(
            consumer_subscription_metadata_refresh_interval
                .duration()
                .as_time(),
        )
        .retry_policy(consumer_retry_policy)
        .fetch_min(consumer_fetch_min)
        .fetch_max(consumer_fetch_max)
        .fetch_partition_max(consumer_fetch_partition_max)
        .session_timeout(consumer_session_timeout)
        .rebalance_timeout(consumer_rebalance_timeout)
        .heartbeat_interval(consumer_heartbeat_interval)
        .request_timeout(consumer_request_timeout)
        .auto_offset_reset(consumer_auto_offset_reset)
        .isolation_level(consumer_isolation_level)
        .assignor(consumer_assignor)
        .build()
        .await?;
    tracing::info!(topic = %cli.input_topic, "order processor starting");
    loop {
        let records = consumer.poll(cli.consumer_poll_timeout).await?;
        for record in records {
            process_order_record(cli, &serde, metrics, &record).await;
        }
    }
}

/// Build the consumer-side `process_order` span, make it a child of the
/// producer's trace (from the record's `traceparent` header), and run the
/// staged processing under it.
async fn process_order_record(
    cli: &Cli,
    serde: &OrderSerde,
    metrics: &DemoMetrics,
    record: &ConsumerRecord,
) {
    let span = tracing::info_span!(
        "process_order",
        otel.kind = "consumer",
        otel.name = "orders process",
        messaging.system = "kafka",
        messaging.source.name = %cli.input_topic,
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
    process_order_inner(cli, serde, metrics, record)
        .instrument(span)
        .await;
}

async fn process_order_inner(
    cli: &Cli,
    serde: &OrderSerde,
    metrics: &DemoMetrics,
    record: &ConsumerRecord,
) {
    let start = Instant::now();
    let Some(value) = record.value.as_deref() else {
        tracing::warn!("order record has no value");
        return;
    };
    let order = match serde.deserialize(&cli.input_topic, value) {
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
    stage(metrics, "validate", cli.validate_work.to_std()).await;
    stage(metrics, "enrich", cli.enrich_work.to_std()).await;
    stage(metrics, "fraud_check", cli.fraud_check_work.to_std()).await;
    stage(metrics, "fulfill", cli.fulfill_work.to_std()).await;

    let outcome = classify_outcome(&order);
    span.record("demo.order.outcome", outcome);
    metrics.record_processed(
        &order.category,
        &order.region,
        outcome,
        Time::from_std(start.elapsed()),
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
        metrics.record_stage(name, Time::from_std(start.elapsed()));
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
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn workload_policy_preserves_defaults_and_accepts_units() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "consume"])
            .expect("default CLI");
        assert2::assert!(defaults.orders_per_sec == per_sec(50));
        assert2::assert!(defaults.consumer_poll_timeout == millis(500));
        assert2::assert!(defaults.validate_work == crabka_units::micros(150));
        assert2::assert!(defaults.enrich_work == crabka_units::micros(400));
        assert2::assert!(defaults.fraud_check_work == crabka_units::micros(200));
        assert2::assert!(defaults.fulfill_work == crabka_units::micros(300));

        let custom = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "consume",
            "--orders-per-sec",
            "120/min",
            "--consumer-poll-timeout",
            "2s",
            "--validate-work",
            "0",
            "--enrich-work",
            "1ms",
            "--fraud-check-work",
            "2ms",
            "--fulfill-work",
            "3ms",
        ])
        .expect("custom CLI");
        assert2::assert!(custom.orders_per_sec == per_sec(2));
        assert2::assert!(custom.consumer_poll_timeout == secs(2));
        assert2::assert!(custom.validate_work == Time::ZERO);
        assert2::assert!(custom.enrich_work == millis(1));
        assert2::assert!(custom.fraud_check_work == millis(2));
        assert2::assert!(custom.fulfill_work == millis(3));

        for option in [
            "--orders-per-sec=-1Hz",
            "--consumer-poll-timeout=0",
            "--validate-work=-1ms",
            "--streams-application-id=",
            "--consumer-group-id=",
        ] {
            Cli::try_parse_from(["observability-demo-app", "--role", "consume", option])
                .expect_err(option);
        }
    }

    #[test]
    fn every_process_argument_has_an_environment_binding() {
        let command = Cli::command();
        let missing = command
            .get_arguments()
            .filter(|argument| argument.get_long().is_some() && argument.get_env().is_none())
            .filter_map(|argument| argument.get_long().map(str::to_owned))
            .collect::<Vec<_>>();

        assert2::assert!(
            missing.is_empty(),
            "arguments without env bindings: {missing:?}"
        );
    }

    #[test]
    fn consumer_behavior_uses_defaults_and_independent_overrides() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "consume"])
            .expect("default CLI");
        let (offset_reset, isolation, assignor) =
            effective_consumer_behavior(&defaults).expect("default behavior");
        assert2::assert!(matches!(offset_reset, AutoOffsetReset::Latest));
        assert2::assert!(isolation == IsolationLevel::ReadUncommitted);
        assert2::assert!(assignor == Assignor::Range);

        let custom = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "consume",
            "--consumer-auto-offset-reset",
            "earliest",
            "--consumer-isolation-level",
            "read-committed",
            "--consumer-assignor",
            "cooperative-sticky",
        ])
        .expect("custom CLI");
        let (offset_reset, isolation, assignor) =
            effective_consumer_behavior(&custom).expect("custom behavior");
        assert2::assert!(matches!(offset_reset, AutoOffsetReset::Earliest));
        assert2::assert!(isolation == IsolationLevel::ReadCommitted);
        assert2::assert!(assignor == Assignor::CooperativeSticky);
    }

    #[test]
    fn consumer_timing_uses_defaults_and_independent_overrides() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "consume"])
            .expect("default CLI");
        assert_eq!(
            effective_consumer_timing(&defaults).expect("default timing"),
            (secs(45), minutes(1), secs(3), secs(30))
        );

        let custom = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "consume",
            "--consumer-session-timeout",
            "46s",
            "--consumer-rebalance-timeout",
            "61s",
            "--consumer-heartbeat-interval",
            "4s",
            "--consumer-request-timeout",
            "31s",
        ])
        .expect("custom CLI");
        assert_eq!(
            effective_consumer_timing(&custom).expect("custom timing"),
            (secs(46), secs(61), secs(4), secs(31))
        );
    }

    #[test]
    fn consumer_fetch_policy_uses_defaults_and_validates_overrides() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "consume"])
            .expect("default CLI");
        assert_eq!(
            effective_consumer_fetch_policy(&defaults).expect("default fetch policy"),
            (bytes(1), mebibytes(50), mebibytes(1))
        );

        let custom = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "consume",
            "--consumer-fetch-min",
            "3B",
            "--consumer-fetch-max",
            "32MiB",
            "--consumer-fetch-partition-max",
            "2MiB",
        ])
        .expect("custom CLI");
        assert_eq!(
            effective_consumer_fetch_policy(&custom).expect("custom fetch policy"),
            (bytes(3), mebibytes(32), mebibytes(2))
        );
    }

    #[test]
    fn consumer_retry_policy_uses_defaults_and_validates_overrides() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "consume"])
            .expect("default CLI");
        assert_eq!(
            effective_consumer_retry_policy(&defaults).expect("default retry policy"),
            ConsumerRetryPolicy::default()
        );

        let custom = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "consume",
            "--consumer-startup-attempt-timeout",
            "2s",
            "--consumer-startup-deadline",
            "3s",
            "--consumer-startup-initial-backoff",
            "10ms",
            "--consumer-startup-max-backoff",
            "20ms",
            "--consumer-coordinator-retry-timeout",
            "4s",
            "--consumer-coordinator-initial-backoff",
            "30ms",
            "--consumer-coordinator-max-backoff",
            "40ms",
        ])
        .expect("custom CLI");
        let policy = effective_consumer_retry_policy(&custom).expect("custom retry policy");
        assert_eq!(policy.startup_attempt_timeout(), secs(2));
        assert_eq!(policy.startup_deadline(), secs(3));
        assert_eq!(policy.startup_initial_backoff(), millis(10));
        assert_eq!(policy.startup_max_backoff(), millis(20));
        assert_eq!(policy.coordinator_retry_timeout(), secs(4));
        assert_eq!(policy.coordinator_initial_backoff(), millis(30));
        assert_eq!(policy.coordinator_max_backoff(), millis(40));
    }

    #[test]
    fn schema_fetch_retry_policy_uses_defaults_and_valid_explicit_bounds() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "produce"])
            .expect("default CLI");
        let defaults = schema_fetch_retry_policy(&defaults).expect("default policy");
        assert_eq!(defaults.initial_backoff(), millis(10));
        assert_eq!(defaults.max_backoff(), secs(1));

        let explicit = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "consume",
            "--schema-fetch-retry-initial-backoff",
            "37ms",
            "--schema-fetch-retry-max-backoff",
            "91ms",
        ])
        .expect("explicit CLI");
        let explicit = schema_fetch_retry_policy(&explicit).expect("explicit policy");
        assert_eq!(explicit.initial_backoff(), millis(37));
        assert_eq!(explicit.max_backoff(), millis(91));
    }

    #[test]
    fn schema_fetch_retry_policy_accepts_equal_bounds() {
        let cli = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--schema-fetch-retry-initial-backoff",
            "37ms",
            "--schema-fetch-retry-max-backoff",
            "37ms",
        ])
        .expect("equal CLI bounds");

        let policy = schema_fetch_retry_policy(&cli).expect("equal retry bounds are valid");
        assert_eq!(policy.initial_backoff(), policy.max_backoff());
    }

    #[test]
    fn consumer_subscription_metadata_refresh_uses_default_and_override() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "consume"])
            .expect("default CLI");
        assert_eq!(
            effective_consumer_subscription_metadata_refresh_interval(&defaults)
                .expect("typed default")
                .milliseconds(),
            5_000
        );

        let overridden = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "consume",
            "--consumer-subscription-metadata-refresh-interval",
            "37ms",
        ])
        .expect("override CLI");
        assert_eq!(
            effective_consumer_subscription_metadata_refresh_interval(&overridden)
                .expect("typed override")
                .milliseconds(),
            37
        );
    }

    #[test]
    fn streams_broker_dns_timeout_uses_default_and_cli_override() {
        let defaults = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            client_dispatch_queue_capacity: DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
            client_frame_max: mebibytes(100),
            registry: "http://127.0.0.1:8081".to_owned(),
            schema_fetch_retry_initial_backoff: None,
            schema_fetch_retry_max_backoff: None,
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            streams_application_id: "orders-analytics".to_owned(),
            consumer_group_id: "orders-processor".to_owned(),
            orders_per_sec: per_sec(50),
            consumer_poll_timeout: millis(500),
            validate_work: crabka_units::micros(150),
            enrich_work: crabka_units::micros(400),
            fraud_check_work: crabka_units::micros(200),
            fulfill_work: crabka_units::micros(300),
            consumer_leave_group_timeout: None,
            consumer_subscription_metadata_refresh_interval: None,
            consumer_startup_attempt_timeout: None,
            consumer_startup_deadline: None,
            consumer_startup_initial_backoff: None,
            consumer_startup_max_backoff: None,
            consumer_coordinator_retry_timeout: None,
            consumer_coordinator_initial_backoff: None,
            consumer_coordinator_max_backoff: None,
            consumer_fetch_min: None,
            consumer_fetch_max: None,
            consumer_fetch_partition_max: None,
            consumer_session_timeout: None,
            consumer_rebalance_timeout: None,
            consumer_heartbeat_interval: None,
            consumer_request_timeout: None,
            consumer_auto_offset_reset: None,
            consumer_isolation_level: None,
            consumer_assignor: None,
            streams_broker_dns_timeout: None,
            streams_poll_interval: None,
            streams_commit_interval: None,
            streams_rebalance_timeout: None,
            streams_leave_heartbeat_timeout: None,
            streams_join_retry_backoff: None,
            streams_interactive_query_queue_capacity: None,
            streams_state_store_cache_max: None,
            streams_fetch_min: None,
        };
        assert_eq!(
            effective_streams_broker_dns_timeout(&defaults).expect("typed default"),
            crabka_client_streams::ClientDnsTimeout::default()
        );

        let overridden = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            streams_broker_dns_timeout: Some(millis(37)),
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
            "--streams-broker-dns-timeout",
            "0ms",
        ])
        .expect_err("zero must fail in Clap");

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-broker-dns-timeout",
            "37ms",
        ])
        .expect("parse before role validation");
        let error = effective_streams_broker_dns_timeout(&produce).expect_err("Stream-only option");
        assert_eq!(
            error.to_string(),
            "--streams-broker-dns-timeout (37ms) is only valid with --role stream"
        );
    }

    #[test]
    fn streams_runtime_cadence_uses_defaults_and_independent_overrides() {
        let defaults = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            client_dispatch_queue_capacity: DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
            client_frame_max: mebibytes(100),
            registry: "http://127.0.0.1:8081".to_owned(),
            schema_fetch_retry_initial_backoff: None,
            schema_fetch_retry_max_backoff: None,
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            streams_application_id: "orders-analytics".to_owned(),
            consumer_group_id: "orders-processor".to_owned(),
            orders_per_sec: per_sec(50),
            consumer_poll_timeout: millis(500),
            validate_work: crabka_units::micros(150),
            enrich_work: crabka_units::micros(400),
            fraud_check_work: crabka_units::micros(200),
            fulfill_work: crabka_units::micros(300),
            consumer_leave_group_timeout: None,
            consumer_subscription_metadata_refresh_interval: None,
            consumer_startup_attempt_timeout: None,
            consumer_startup_deadline: None,
            consumer_startup_initial_backoff: None,
            consumer_startup_max_backoff: None,
            consumer_coordinator_retry_timeout: None,
            consumer_coordinator_initial_backoff: None,
            consumer_coordinator_max_backoff: None,
            consumer_fetch_min: None,
            consumer_fetch_max: None,
            consumer_fetch_partition_max: None,
            consumer_session_timeout: None,
            consumer_rebalance_timeout: None,
            consumer_heartbeat_interval: None,
            consumer_request_timeout: None,
            consumer_auto_offset_reset: None,
            consumer_isolation_level: None,
            consumer_assignor: None,
            streams_broker_dns_timeout: None,
            streams_poll_interval: None,
            streams_commit_interval: None,
            streams_rebalance_timeout: None,
            streams_leave_heartbeat_timeout: None,
            streams_join_retry_backoff: None,
            streams_interactive_query_queue_capacity: None,
            streams_state_store_cache_max: None,
            streams_fetch_min: None,
        };
        let (poll, commit) = effective_streams_runtime_cadence(&defaults).expect("typed defaults");
        assert_eq!(poll, crabka_client_streams::StreamsPollInterval::default());
        assert_eq!(
            commit,
            crabka_client_streams::StreamsCommitInterval::default()
        );

        let overridden = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            streams_poll_interval: Some(millis(37)),
            streams_commit_interval: Some(millis(41)),
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
            "--streams-poll-interval",
            "0ms",
        ])
        .expect_err("zero poll interval");
        Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-commit-interval",
            "0ms",
        ])
        .expect_err("zero commit interval");

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-poll-interval",
            "37ms",
        ])
        .expect("parse before role validation");
        let error = effective_streams_runtime_cadence(&produce).expect_err("Stream-only option");
        assert_eq!(
            error.to_string(),
            "--streams-poll-interval (37ms) is only valid with --role stream"
        );
    }

    #[test]
    fn streams_rebalance_timeout_uses_default_and_cli_override() {
        let defaults = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            client_dispatch_queue_capacity: DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
            client_frame_max: mebibytes(100),
            registry: "http://127.0.0.1:8081".to_owned(),
            schema_fetch_retry_initial_backoff: None,
            schema_fetch_retry_max_backoff: None,
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            streams_application_id: "orders-analytics".to_owned(),
            consumer_group_id: "orders-processor".to_owned(),
            orders_per_sec: per_sec(50),
            consumer_poll_timeout: millis(500),
            validate_work: crabka_units::micros(150),
            enrich_work: crabka_units::micros(400),
            fraud_check_work: crabka_units::micros(200),
            fulfill_work: crabka_units::micros(300),
            consumer_leave_group_timeout: None,
            consumer_subscription_metadata_refresh_interval: None,
            consumer_startup_attempt_timeout: None,
            consumer_startup_deadline: None,
            consumer_startup_initial_backoff: None,
            consumer_startup_max_backoff: None,
            consumer_coordinator_retry_timeout: None,
            consumer_coordinator_initial_backoff: None,
            consumer_coordinator_max_backoff: None,
            consumer_fetch_min: None,
            consumer_fetch_max: None,
            consumer_fetch_partition_max: None,
            consumer_session_timeout: None,
            consumer_rebalance_timeout: None,
            consumer_heartbeat_interval: None,
            consumer_request_timeout: None,
            consumer_auto_offset_reset: None,
            consumer_isolation_level: None,
            consumer_assignor: None,
            streams_broker_dns_timeout: None,
            streams_poll_interval: None,
            streams_commit_interval: None,
            streams_rebalance_timeout: None,
            streams_leave_heartbeat_timeout: None,
            streams_join_retry_backoff: None,
            streams_interactive_query_queue_capacity: None,
            streams_state_store_cache_max: None,
            streams_fetch_min: None,
        };
        assert_eq!(
            effective_streams_rebalance_timeout(&defaults).expect("typed default"),
            crabka_client_streams::StreamsRebalanceTimeout::default()
        );

        let overridden = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            streams_rebalance_timeout: Some(secs(45)),
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
            "--streams-rebalance-timeout",
            "0ms",
        ])
        .expect_err("zero must fail in Clap");

        let overflow = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-rebalance-timeout",
            "2147483648ms",
        ])
        .expect("parse before typed validation");
        let error =
            effective_streams_rebalance_timeout(&overflow).expect_err("i32 overflow must fail");
        assert!(error.to_string().contains("streams rebalance timeout"));

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-rebalance-timeout",
            "45s",
        ])
        .expect("parse before role validation");
        let error = effective_streams_rebalance_timeout(&produce).expect_err("Stream-only option");
        assert_eq!(
            error.to_string(),
            "--streams-rebalance-timeout (45s) is only valid with --role stream"
        );
    }

    #[test]
    fn streams_join_retry_backoff_uses_default_and_cli_override() {
        let defaults = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            client_dispatch_queue_capacity: DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
            client_frame_max: mebibytes(100),
            registry: "http://127.0.0.1:8081".to_owned(),
            schema_fetch_retry_initial_backoff: None,
            schema_fetch_retry_max_backoff: None,
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            streams_application_id: "orders-analytics".to_owned(),
            consumer_group_id: "orders-processor".to_owned(),
            orders_per_sec: per_sec(50),
            consumer_poll_timeout: millis(500),
            validate_work: crabka_units::micros(150),
            enrich_work: crabka_units::micros(400),
            fraud_check_work: crabka_units::micros(200),
            fulfill_work: crabka_units::micros(300),
            consumer_leave_group_timeout: None,
            consumer_subscription_metadata_refresh_interval: None,
            consumer_startup_attempt_timeout: None,
            consumer_startup_deadline: None,
            consumer_startup_initial_backoff: None,
            consumer_startup_max_backoff: None,
            consumer_coordinator_retry_timeout: None,
            consumer_coordinator_initial_backoff: None,
            consumer_coordinator_max_backoff: None,
            consumer_fetch_min: None,
            consumer_fetch_max: None,
            consumer_fetch_partition_max: None,
            consumer_session_timeout: None,
            consumer_rebalance_timeout: None,
            consumer_heartbeat_interval: None,
            consumer_request_timeout: None,
            consumer_auto_offset_reset: None,
            consumer_isolation_level: None,
            consumer_assignor: None,
            streams_broker_dns_timeout: None,
            streams_poll_interval: None,
            streams_commit_interval: None,
            streams_rebalance_timeout: None,
            streams_leave_heartbeat_timeout: None,
            streams_join_retry_backoff: None,
            streams_interactive_query_queue_capacity: None,
            streams_state_store_cache_max: None,
            streams_fetch_min: None,
        };
        assert_eq!(
            effective_streams_join_retry_backoff(&defaults).expect("typed default"),
            StreamsJoinRetryBackoff::default()
        );

        let overridden = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            streams_join_retry_backoff: Some(millis(37)),
            ..defaults
        };
        assert_eq!(
            effective_streams_join_retry_backoff(&overridden)
                .expect("typed override")
                .milliseconds(),
            37
        );
    }

    #[test]
    fn streams_interactive_query_queue_capacity_uses_default_and_override() {
        let defaults = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            client_dispatch_queue_capacity: DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
            client_frame_max: mebibytes(100),
            registry: "http://127.0.0.1:8081".to_owned(),
            schema_fetch_retry_initial_backoff: None,
            schema_fetch_retry_max_backoff: None,
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            streams_application_id: "orders-analytics".to_owned(),
            consumer_group_id: "orders-processor".to_owned(),
            orders_per_sec: per_sec(50),
            consumer_poll_timeout: millis(500),
            validate_work: crabka_units::micros(150),
            enrich_work: crabka_units::micros(400),
            fraud_check_work: crabka_units::micros(200),
            fulfill_work: crabka_units::micros(300),
            consumer_leave_group_timeout: None,
            consumer_subscription_metadata_refresh_interval: None,
            consumer_startup_attempt_timeout: None,
            consumer_startup_deadline: None,
            consumer_startup_initial_backoff: None,
            consumer_startup_max_backoff: None,
            consumer_coordinator_retry_timeout: None,
            consumer_coordinator_initial_backoff: None,
            consumer_coordinator_max_backoff: None,
            consumer_fetch_min: None,
            consumer_fetch_max: None,
            consumer_fetch_partition_max: None,
            consumer_session_timeout: None,
            consumer_rebalance_timeout: None,
            consumer_heartbeat_interval: None,
            consumer_request_timeout: None,
            consumer_auto_offset_reset: None,
            consumer_isolation_level: None,
            consumer_assignor: None,
            streams_broker_dns_timeout: None,
            streams_poll_interval: None,
            streams_commit_interval: None,
            streams_rebalance_timeout: None,
            streams_leave_heartbeat_timeout: None,
            streams_join_retry_backoff: None,
            streams_interactive_query_queue_capacity: None,
            streams_state_store_cache_max: None,
            streams_fetch_min: None,
        };
        assert_eq!(
            effective_streams_interactive_query_queue_capacity(&defaults).expect("typed default"),
            StreamsInteractiveQueryQueueCapacity::default()
        );

        let overridden = Cli {
            profiling: crabka_telemetry::profiling::ProfilingConfig::default(),
            streams_interactive_query_queue_capacity: NonZeroUsize::new(37),
            ..defaults
        };
        assert_eq!(
            effective_streams_interactive_query_queue_capacity(&overridden)
                .expect("typed override")
                .capacity(),
            37
        );
    }

    #[test]
    fn client_resource_policy_parses_defaults_overrides_and_invalid_values() {
        let defaults = Cli::try_parse_from(["observability-demo-app", "--role", "stream"])
            .expect("default CLI");
        let (dispatch, frame) = client_resource_policy(&defaults);
        assert_eq!(dispatch.get(), 64);
        assert_eq!(frame.size(), mebibytes(100));
        assert_eq!(
            effective_streams_fetch_min(&defaults)
                .expect("default fetch minimum")
                .bytes(),
            1
        );

        let custom = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
            "--streams-fetch-min",
            "3B",
        ])
        .expect("custom CLI");
        let (dispatch, frame) = client_resource_policy(&custom);
        assert_eq!(dispatch.get(), 7);
        assert_eq!(frame.size(), kibibytes(32));
        assert_eq!(
            effective_streams_fetch_min(&custom)
                .expect("custom fetch minimum")
                .bytes(),
            3
        );

        for option in [
            "--client-dispatch-queue-capacity=0",
            "--client-frame-max=101MiB",
            "--client-frame-max=1.5B",
            "--streams-fetch-min=0B",
        ] {
            Cli::try_parse_from(["observability-demo-app", "--role", "stream", option])
                .expect_err(option);
        }

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-fetch-min",
            "3B",
        ])
        .expect("parse before role validation");
        assert_eq!(
            effective_streams_fetch_min(&produce)
                .expect_err("Streams-only option")
                .to_string(),
            "--streams-fetch-min (3B) is only valid with --role stream"
        );
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        const CHILD: &str = "CRABKA_TEST_DEMO_CLIENT_POLICY_ENV_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let environment = Cli::try_parse_from(["observability-demo-app", "--role", "stream"])
                .expect("environment policy");
            let (dispatch, frame) = client_resource_policy(&environment);
            assert_eq!(dispatch.get(), 7);
            assert_eq!(frame.size(), kibibytes(32));
            assert_eq!(
                effective_streams_fetch_min(&environment)
                    .expect("environment fetch minimum")
                    .bytes(),
                3
            );

            let cli = Cli::try_parse_from([
                "observability-demo-app",
                "--role",
                "stream",
                "--client-dispatch-queue-capacity",
                "9",
                "--client-frame-max",
                "64KiB",
                "--streams-fetch-min",
                "5B",
            ])
            .expect("CLI over environment");
            let (dispatch, frame) = client_resource_policy(&cli);
            assert_eq!(dispatch.get(), 9);
            assert_eq!(frame.size(), kibibytes(64));
            assert_eq!(
                effective_streams_fetch_min(&cli)
                    .expect("environment policy")
                    .bytes(),
                5
            );
            return;
        }

        let status =
            std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "tests::client_resource_policy_reads_environment_and_prefers_cli",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("CRABKA_DEMO_CLIENT_DISPATCH_QUEUE_CAPACITY", "7")
                .env("CRABKA_DEMO_CLIENT_FRAME_MAX", "32KiB")
                .env("CRABKA_DEMO_STREAMS_FETCH_MIN", "3B")
                .status()
                .expect("run isolated environment parser test");
        assert!(status.success());
    }
}
