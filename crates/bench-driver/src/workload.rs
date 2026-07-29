//! Producer + consumer workload runners. Builds N producers and M
//! consumers (members of one group), runs warmup → measurement
//! phases, optionally triggers a failover mid-measurement, and merges
//! per-task histograms into the public `LatencyPercentiles` shape.

use std::{
    fmt,
    future::Future,
    path::PathBuf,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result};
use chrono::Utc;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::security::{ClientSecurity, TlsConnectorConfig};
use crabka_client_producer::{Producer, ProducerError, ProducerRecord, RecordMetadata};
use crabka_security::ListenerProtocol;
use crabka_units::prelude::*;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use hdrhistogram::Histogram;
use refined_type::rule::GreaterU32;
use tokio::{task::JoinSet, time::Instant};
use tracing::{info, warn};

use crate::{
    hist,
    ids::{MessageCount, TimeOffsetMs, WallclockMs},
    numeric::{
        event_rate, nonnegative_i64_to_u64, saturating_u64_to_i64, saturating_u128_to_u64, to_f64,
    },
    payload,
    prom::PromClient,
    rate::Pacer,
    scenario::{
        BrokerSample, Disturbance, LoadMode, ModeTag, Resource, RunOutput, Sample, Scenario, Stack,
        Throughput, Topology,
    },
};

/// Width of one time-series sample bucket. The measurement window is split
/// into configured interval slices; each producer/consumer task tallies
/// per-slice counts + a per-slice latency histogram locally (no shared locks
/// on the hot path), and `run()` merges them into the `samples` series.
type AckResult =
    Result<Result<RecordMetadata, ProducerError>, tokio::sync::oneshot::error::RecvError>;
type AckFuture = Pin<Box<dyn Future<Output = (AckResult, Instant)> + Send>>;

pub const MAX_CLIENT_REQUEST_TIMEOUT: Time = millis(2_147_483_647);
pub const DEFAULT_PRODUCER_REQUEST_TIMEOUT: Time = secs(2);
pub const DEFAULT_CRABKA_CONSUMER_REQUEST_TIMEOUT: Time = secs(5);
pub const DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT: Time = secs(30);
pub const DEFAULT_CONSUMER_BUILD_ATTEMPTS: u32 = 6;
pub const DEFAULT_CONSUMER_BUILD_INITIAL_BACKOFF: Time = millis(100);
pub const DEFAULT_CONSUMER_BUILD_MAX_BACKOFF: Time = secs(2);
pub const DEFAULT_CONSUMER_POLL_TIMEOUT: Time = millis(50);
pub const DEFAULT_CONSUMER_POLL_ERROR_BACKOFF: Time = millis(100);
pub const DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT: Time = secs(10);
pub const DEFAULT_SAMPLE_INTERVAL: Time = secs(2);

/// Return the validated producer request-timeout default.
///
/// # Panics
///
/// Panics if the named default is not protocol-safe.
#[must_use]
pub const fn default_producer_request_timeout() -> Time {
    DEFAULT_PRODUCER_REQUEST_TIMEOUT
}

/// Return the validated consumer request-timeout default for `stack`.
///
/// # Panics
///
/// Panics if the selected named default is not protocol-safe.
#[must_use]
pub const fn default_consumer_request_timeout(stack: Stack) -> Time {
    match stack {
        Stack::Crabka => DEFAULT_CRABKA_CONSUMER_REQUEST_TIMEOUT,
        Stack::Kafka => DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerBuildAttempts(u32);

impl ConsumerBuildAttempts {
    /// Validate a consumer-build attempt count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u32) -> Result<Self, String> {
        GreaterU32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> u32 {
        self.0
    }
}

impl Default for ConsumerBuildAttempts {
    fn default() -> Self {
        Self::new(DEFAULT_CONSUMER_BUILD_ATTEMPTS)
            .expect("default consumer-build attempts are positive")
    }
}

impl fmt::Display for ConsumerBuildAttempts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConsumerBuildAttempts {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

#[must_use]
pub const fn default_consumer_build_initial_backoff() -> Time {
    DEFAULT_CONSUMER_BUILD_INITIAL_BACKOFF
}

#[must_use]
pub const fn default_consumer_build_max_backoff() -> Time {
    DEFAULT_CONSUMER_BUILD_MAX_BACKOFF
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConsumerBuildRetryPolicy {
    attempts: ConsumerBuildAttempts,
    initial_backoff: Time,
    max_backoff: Time,
}

impl ConsumerBuildRetryPolicy {
    /// Validate a complete consumer-build retry policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial backoff exceeds the maximum.
    pub fn new(
        attempts: ConsumerBuildAttempts,
        initial_backoff: Time,
        max_backoff: Time,
    ) -> Result<Self, String> {
        if initial_backoff > max_backoff {
            return Err("consumer-build initial backoff exceeds maximum".to_owned());
        }
        Ok(Self {
            attempts,
            initial_backoff,
            max_backoff,
        })
    }

    #[must_use]
    pub const fn attempts(self) -> u32 {
        self.attempts.into_value()
    }

    #[must_use]
    pub fn initial_backoff(self) -> Time {
        self.initial_backoff
    }

    #[must_use]
    pub fn max_backoff(self) -> Time {
        self.max_backoff
    }
}

impl Default for ConsumerBuildRetryPolicy {
    fn default() -> Self {
        Self::new(
            ConsumerBuildAttempts::default(),
            default_consumer_build_initial_backoff(),
            default_consumer_build_max_backoff(),
        )
        .expect("default consumer-build retry range is ordered")
    }
}

#[must_use]
pub const fn default_consumer_poll_timeout() -> Time {
    DEFAULT_CONSUMER_POLL_TIMEOUT
}

#[must_use]
pub const fn default_consumer_poll_error_backoff() -> Time {
    DEFAULT_CONSUMER_POLL_ERROR_BACKOFF
}

/// The fixed sampling grid, shared (by Copy) with every task so all tasks
/// bucket into the same slices.
#[derive(Clone, Copy)]
struct Grid {
    /// When the measurement window begins (warmup end).
    meas_start: Instant,
    interval_ms: u64,
    /// Number of slices covering the measurement window.
    n: usize,
}

impl Grid {
    /// The grid covering `scenario`'s measurement window, which opens once its
    /// warmup has elapsed from `started_at`. Always at least one slice wide, so
    /// a run shorter than a slice still has somewhere to tally into.
    fn new(started_at: Instant, scenario: &Scenario, interval: Time) -> Self {
        let interval_ms = nonnegative_i64_to_u64(interval.millis_i64());
        let duration_ms = nonnegative_i64_to_u64(scenario.duration.millis_i64());
        Self {
            meas_start: started_at + scenario.warmup.to_std(),
            interval_ms,
            n: usize::try_from(duration_ms.div_ceil(interval_ms).max(1)).unwrap_or(usize::MAX),
        }
    }

    /// Slice index for an event observed at `now`, clamped into `[0, n-1]`.
    fn idx(&self, now: Instant) -> usize {
        let elapsed_ms =
            saturating_u128_to_u64(now.saturating_duration_since(self.meas_start).as_millis());
        usize::try_from(elapsed_ms / self.interval_ms)
            .unwrap_or(usize::MAX)
            .min(self.n.saturating_sub(1))
    }

    /// The width of one slice, as the extent the per-slice rates divide by.
    fn interval(&self) -> Time {
        Time::from_millis(saturating_u64_to_i64(self.interval_ms))
    }
}

/// Parameters wired in from `main` / CLI. Distinct from `Scenario` because
/// the scenario YAML describes *what* to run, while this struct describes
/// *where* it's running.
pub struct DriverConfig {
    pub bootstrap: String,
    pub topic: String,
    pub stack: Stack,
    pub namespace: String,
    pub prometheus_url: Option<String>,
    pub prometheus_request_timeout: Time,
    pub producer_request_timeout: Time,
    pub producer_final_drain_timeout: Time,
    pub sample_interval: Time,
    pub consumer_request_timeout: Time,
    pub consumer_build_retry_policy: ConsumerBuildRetryPolicy,
    /// How long one consumer `poll` waits for records.
    pub consumer_poll_timeout: Time,
    /// How long the consumer loop sleeps after a poll error before retrying.
    pub consumer_poll_error_backoff: Time,
    pub broker_count: u32,
    pub scenario_id: u64,
    /// TLS data-path config. `None` → plaintext (the default benchmark path).
    pub tls: Option<TlsParams>,
}

/// TLS knobs for the producer/consumer data path. When present, both clients
/// dial the broker's TLS listener and the produce/fetch byte stream is
/// encrypted — the path crabka can serve via kTLS sendfile and Strimzi serves
/// via JVM SSL.
#[derive(Debug, Clone)]
pub struct TlsParams {
    /// Mounted CA bundle (`ca.crt`) the client trusts to verify the broker
    /// serving cert. Sourced from the per-stack cluster-CA Secret.
    pub ca_path: PathBuf,
    /// SNI / server-name presented in the TLS `ClientHello`, matched against a
    /// SAN on the broker serving cert. LOAD-BEARING: the bootstrap is
    /// DNS-resolved to a pod IP and dialed by IP, so the SNI is NOT derived
    /// from the bootstrap host — it must be set to a cert-SAN name
    /// (crabka: `demo-broker-headless.<ns>.svc.cluster.local`;
    /// Strimzi: `demo-kafka-bootstrap`).
    pub server_name: String,
    /// Optional mTLS client identity `(cert_pem, key_pem)`. One-way TLS
    /// (`None`) is sufficient for the benchmark; present only if the listener
    /// is configured to require client auth.
    pub client_identity: Option<(PathBuf, PathBuf)>,
}

impl TlsParams {
    /// Build the [`ClientSecurity`] for an `Ssl` listener: one-way TLS by
    /// default (server-authenticated, trust-roots from the mounted CA), or
    /// mutual TLS when [`Self::client_identity`] is set.
    #[must_use]
    pub fn to_security(&self) -> ClientSecurity {
        ClientSecurity {
            protocol: ListenerProtocol::Ssl,
            tls: Some(TlsConnectorConfig {
                trust_roots_pem: Some(self.ca_path.clone()),
                server_name: self.server_name.clone(),
                client_identity: self.client_identity.clone(),
            }),
            sasl: None,
            sasl_host: None,
        }
    }
}

/// Top-level entrypoint called by `main`. Returns the populated
/// `RunOutput`. The caller is responsible for serialising it to disk.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
/// # Panics
/// Panics if synchronized state is poisoned or validated input is missing a field required to produce the output.
pub async fn run(scenario: Scenario, cfg: DriverConfig) -> Result<RunOutput> {
    let wallclock_start = Utc::now().timestamp_millis();
    let t_start = Instant::now();

    // Time-series sampling grid: split the measurement window into fixed
    // slices. Computed up front (meas_start is t_start + warmup) so it can be
    // handed to each task at spawn time.
    let grid = Grid::new(t_start, &scenario, cfg.sample_interval);
    let n_intervals = grid.n;

    let mut notes: Vec<String> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    let (failover_active, skipped) =
        validate_topology(&scenario, &cfg, wallclock_start, &mut notes);
    if let Some(output) = skipped {
        return Ok(output);
    }

    let security = client_security(&cfg);

    let mut prod_set: JoinSet<ProducerOut> = JoinSet::new();
    let stop = Arc::new(AtomicU8::new(STATE_RUN));
    let first_ack_unix_ms = Arc::new(AtomicU64::new(0));

    for i in 0..scenario.producers {
        let s = scenario.clone();
        let bootstrap = cfg.bootstrap.clone();
        let topic = cfg.topic.clone();
        let stop = stop.clone();
        let first_ack = first_ack_unix_ms.clone();
        let sec = security.clone();
        prod_set.spawn(run_producer(ProducerTask {
            idx: i,
            scenario: s,
            bootstrap,
            topic,
            scenario_id: cfg.scenario_id,
            stop,
            first_ack,
            security: sec,
            grid,
            request_timeout: cfg.producer_request_timeout,
            final_drain_timeout: cfg.producer_final_drain_timeout,
        }));
    }

    let mut cons_set: JoinSet<ConsumerOut> = JoinSet::new();
    for i in 0..scenario.consumers {
        let s = scenario.clone();
        let bootstrap = cfg.bootstrap.clone();
        let topic = cfg.topic.clone();
        let stop = stop.clone();
        let sid = cfg.scenario_id;
        let sec = security.clone();
        cons_set.spawn(run_consumer(ConsumerTask {
            idx: i,
            scenario: s,
            bootstrap,
            topic,
            scenario_id: sid,
            stop,
            security: sec,
            grid,
            request_timeout: cfg.consumer_request_timeout,
            build_retry_policy: cfg.consumer_build_retry_policy,
            poll_timeout: cfg.consumer_poll_timeout,
            poll_error_backoff: cfg.consumer_poll_error_backoff,
        }));
    }

    let kill_at_ms = Arc::new(AtomicU64::new(0));
    spawn_failover(
        failover_active,
        &scenario,
        &cfg,
        security.clone(),
        t_start,
        kill_at_ms.clone(),
    );

    let warmup_end = t_start + scenario.warmup.to_std();
    tokio::time::sleep_until(warmup_end).await;
    stop.store(STATE_MEASURING, Ordering::SeqCst);

    let meas_end = warmup_end + scenario.duration.to_std();
    tokio::time::sleep_until(meas_end).await;
    stop.store(STATE_STOP, Ordering::SeqCst);

    // ── Join all tasks ──────────────────────────────────────────────────────
    let mut prod_hist = hist::new();
    let mut prod_msgs = 0u64;
    let mut prod_bytes = 0u64;
    let mut prod_dropped = 0u64;
    let mut earliest_recovery_ms = 0u64;
    let mut max_spike = Time::ZERO;
    let mut prod_iv_msgs = vec![0u64; n_intervals];
    let mut prod_iv_hist: Vec<Histogram<u64>> = (0..n_intervals).map(|_| hist::new()).collect();
    while let Some(j) = prod_set.join_next().await {
        match j {
            Ok(t) => {
                prod_hist.add(&t.latency).ok();
                prod_msgs += t.msgs;
                prod_bytes += t.bytes;
                prod_dropped += t.dropped;
                for iv in 0..n_intervals {
                    if let Some(m) = t.interval_msgs.get(iv) {
                        prod_iv_msgs[iv] += *m;
                    }
                    if let Some(h) = t.interval_hist.get(iv) {
                        prod_iv_hist[iv].add(h).ok();
                    }
                }
                if t.latency_spike_max > max_spike {
                    max_spike = t.latency_spike_max;
                }
                if t.recovery_unix_ms > 0
                    && (earliest_recovery_ms == 0 || t.recovery_unix_ms < earliest_recovery_ms)
                {
                    earliest_recovery_ms = t.recovery_unix_ms;
                }
                if !t.error.is_empty() {
                    errors.push(t.error);
                }
            }
            Err(e) => errors.push(format!("producer-task-panic: {e}")),
        }
    }

    let mut cons_hist = hist::new();
    let mut cons_msgs = 0u64;
    let mut cons_bytes = 0u64;
    let mut cons_iv_msgs = vec![0u64; n_intervals];
    let mut cons_iv_hist: Vec<Histogram<u64>> = (0..n_intervals).map(|_| hist::new()).collect();
    while let Some(j) = cons_set.join_next().await {
        match j {
            Ok(t) => {
                cons_hist.add(&t.latency).ok();
                cons_msgs += t.msgs;
                cons_bytes += t.bytes;
                for iv in 0..n_intervals {
                    if let Some(m) = t.interval_msgs.get(iv) {
                        cons_iv_msgs[iv] += *m;
                    }
                    if let Some(h) = t.interval_hist.get(iv) {
                        cons_iv_hist[iv].add(h).ok();
                    }
                }
                if !t.error.is_empty() {
                    errors.push(t.error);
                }
            }
            Err(e) => errors.push(format!("consumer-task-panic: {e}")),
        }
    }

    let samples = build_samples(
        grid.interval(),
        (&prod_iv_msgs, &prod_iv_hist),
        (&cons_iv_msgs, &cons_iv_hist),
    );

    let wallclock_end = Utc::now().timestamp_millis();
    let (resource, broker_samples) = capture_resources(
        &scenario,
        &cfg,
        (wallclock_start, wallclock_end),
        prod_msgs,
        &mut notes,
    )
    .await;

    let (disturbance, first_ack) = finalize_timing(
        failover_active,
        (
            kill_at_ms.load(Ordering::SeqCst),
            earliest_recovery_ms,
            prod_dropped,
            max_spike,
        ),
        first_ack_unix_ms.load(Ordering::SeqCst),
        wallclock_start,
    );

    Ok(RunOutput {
        scenario: scenario.clone(),
        stack: cfg.stack,
        topology: Topology {
            partitions: scenario.partitions,
            replication_factor: scenario.replication_factor,
            broker_count: cfg.broker_count,
        },
        wallclock_start_unix_ms: WallclockMs(wallclock_start),
        wallclock_end_unix_ms: WallclockMs(wallclock_end),
        throughput: Throughput {
            msgs_produced: MessageCount(prod_msgs),
            msgs_consumed: MessageCount(cons_msgs),
            bytes_in: ByteSize::from_bytes(prod_bytes),
            bytes_out: ByteSize::from_bytes(cons_bytes),
            producer_rate: event_rate(prod_msgs, scenario.duration),
            consumer_rate: event_rate(cons_msgs, scenario.duration),
        },
        producer_latency: hist::percentiles(&prod_hist),
        consumer_e2e_latency: hist::percentiles(&cons_hist),
        resource,
        disturbance,
        startup: None,
        first_ack,
        errors,
        notes,
        samples,
        broker_samples,
    })
}

fn client_security(cfg: &DriverConfig) -> Option<ClientSecurity> {
    let security = cfg.tls.as_ref().map(TlsParams::to_security);
    if let Some(value) = &security {
        info!(
            server_name = value
                .tls
                .as_ref()
                .map_or("", |tls| tls.server_name.as_str()),
            "TLS data path enabled (protocol=Ssl)"
        );
    }
    security
}

fn build_samples(
    interval: Time,
    producer: (&[u64], &[Histogram<u64>]),
    consumer: (&[u64], &[Histogram<u64>]),
) -> Vec<Sample> {
    let interval_ms = nonnegative_i64_to_u64(interval.millis_i64());
    // The histogram counts microseconds; an empty slice measured nothing.
    let percentile = |histogram: &Histogram<u64>, quantile: f64| {
        if histogram.is_empty() {
            Time::ZERO
        } else {
            Time::from_micros(saturating_u64_to_i64(histogram.value_at_quantile(quantile)))
        }
    };
    producer
        .0
        .iter()
        .enumerate()
        .map(|(index, messages)| Sample {
            t_offset_ms: TimeOffsetMs(
                u64::try_from(index)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(interval_ms),
            ),
            producer_rate: event_rate(*messages, interval),
            consumer_rate: event_rate(consumer.0[index], interval),
            producer_p50: percentile(&producer.1[index], 0.50),
            producer_p99: percentile(&producer.1[index], 0.99),
            consumer_e2e_p99: percentile(&consumer.1[index], 0.99),
        })
        .collect()
}

fn finalize_timing(
    failover_active: bool,
    failover: (u64, u64, u64, Time),
    first_ack: u64,
    wallclock_start: i64,
) -> (Option<Disturbance>, Time) {
    let disturbance = failover_active.then_some(Disturbance {
        kill_at_ms: TimeOffsetMs(failover.0),
        recovery_at_ms: TimeOffsetMs(failover.1),
        dropped: MessageCount(failover.2),
        latency_spike_max: failover.3,
    });
    // Both stamps are epoch milliseconds — coordinates — so their difference is
    // the extent from run start to the first ack. No ack at all means no extent.
    let first_ack = if first_ack == 0 {
        Time::ZERO
    } else {
        Time::from_millis(
            i64::try_from(first_ack)
                .unwrap_or(i64::MAX)
                .saturating_sub(wallclock_start)
                .max(0),
        )
    };
    (disturbance, first_ack)
}

fn validate_topology(
    scenario: &Scenario,
    cfg: &DriverConfig,
    wallclock_start: i64,
    notes: &mut Vec<String>,
) -> (bool, Option<RunOutput>) {
    let failover_active =
        scenario.failover.is_some() && scenario.replication_factor >= 3 && cfg.broker_count >= 3;
    if scenario.failover.is_some() && !failover_active {
        notes.push("skipped:failover-needs-rf3".into());
    }
    let topology_mismatch = matches!(scenario.mode_tag, ModeTag::Cluster)
        && scenario.replication_factor >= 3
        && cfg.broker_count < 3;
    if !topology_mismatch {
        return (failover_active, None);
    }
    notes.push(format!(
        "skipped:topology-mismatch (rf={} brokers={})",
        scenario.replication_factor, cfg.broker_count
    ));
    let output = empty_output(
        scenario,
        cfg,
        wallclock_start,
        std::mem::take(notes),
        Vec::new(),
    );
    (failover_active, Some(output))
}

async fn capture_resources(
    scenario: &Scenario,
    cfg: &DriverConfig,
    wallclock: (i64, i64),
    produced: u64,
    notes: &mut Vec<String>,
) -> (Resource, Vec<BrokerSample>) {
    let Some(url) = &cfg.prometheus_url else {
        notes.push("prometheus-url-not-set".into());
        return (Resource::default(), Vec::new());
    };
    let client = match PromClient::new(url, cfg.prometheus_request_timeout) {
        Ok(client) => client,
        Err(error) => {
            notes.push(format!("prometheus-client-failed: {error}"));
            return (Resource::default(), Vec::new());
        }
    };
    let resource = match client
        .capture_resource(
            cfg.stack,
            &cfg.namespace,
            scenario.duration,
            MessageCount(produced),
        )
        .await
    {
        Ok(resource) => resource,
        Err(error) => {
            warn!(%error, "prometheus capture failed");
            notes.push(format!("prometheus-capture-failed: {error}"));
            Resource::default()
        }
    };
    let broker_samples = client
        .capture_resource_series(
            cfg.stack,
            &cfg.namespace,
            to_f64(wallclock.0) / 1000.0,
            to_f64(wallclock.1) / 1000.0,
            15,
        )
        .await
        .unwrap_or_default();
    (resource, broker_samples)
}

fn spawn_failover(
    active: bool,
    scenario: &Scenario,
    cfg: &DriverConfig,
    security: Option<ClientSecurity>,
    started_at: Instant,
    kill_at: Arc<AtomicU64>,
) {
    if !active {
        return;
    }
    let spec = scenario.failover.clone().expect("checked above");
    let stack = cfg.stack;
    let namespace = cfg.namespace.clone();
    let bootstrap = cfg.bootstrap.clone();
    let topic = cfg.topic.clone();
    tokio::spawn(async move {
        tokio::time::sleep_until(started_at + spec.kill_after.to_std()).await;
        kill_at.store(
            nonnegative_i64_to_u64(Utc::now().timestamp_millis()),
            Ordering::SeqCst,
        );
        let client = match crate::failover::try_client().await {
            Ok(client) => client,
            Err(error) => {
                warn!(%error, "failover: in-cluster client unavailable");
                return;
            }
        };
        let leader_id = if spec.target == "partition0_leader" {
            match crate::failover::partition0_leader_from_metadata(&bootstrap, &topic, security)
                .await
            {
                Ok(id) => Some(id),
                Err(error) => {
                    warn!(%error, "failover: leader lookup failed; using first broker pod");
                    None
                }
            }
        } else {
            None
        };
        match crate::failover::kill_broker_pod(&client, stack, &namespace, leader_id).await {
            Ok(name) => info!(pod = %name, "failover: killed broker"),
            Err(error) => warn!(%error, "failover: broker kill failed"),
        }
    });
}

const STATE_RUN: u8 = 0; // warmup phase, record-but-discard
const STATE_MEASURING: u8 = 1;
const STATE_STOP: u8 = 2;

struct ProducerOut {
    latency: Histogram<u64>,
    msgs: u64,
    bytes: u64,
    dropped: u64,
    recovery_unix_ms: u64,
    latency_spike_max: Time,
    error: String,
    /// Per-slice (see [`Grid`]) measurement-window tallies. Empty on a build
    /// failure; `run()` merges by index with bounds checks.
    interval_msgs: Vec<u64>,
    interval_hist: Vec<Histogram<u64>>,
}

struct ConsumerOut {
    latency: Histogram<u64>,
    msgs: u64,
    bytes: u64,
    error: String,
    interval_msgs: Vec<u64>,
    interval_hist: Vec<Histogram<u64>>,
}

fn empty_output(
    scenario: &Scenario,
    cfg: &DriverConfig,
    start: i64,
    notes: Vec<String>,
    errors: Vec<String>,
) -> RunOutput {
    RunOutput {
        scenario: scenario.clone(),
        stack: cfg.stack,
        topology: Topology {
            partitions: scenario.partitions,
            replication_factor: scenario.replication_factor,
            broker_count: cfg.broker_count,
        },
        wallclock_start_unix_ms: WallclockMs(start),
        wallclock_end_unix_ms: WallclockMs(start),
        throughput: Throughput::default(),
        producer_latency: crate::scenario::LatencyPercentiles::default(),
        consumer_e2e_latency: crate::scenario::LatencyPercentiles::default(),
        resource: Resource::default(),
        disturbance: None,
        startup: None,
        first_ack: Time::ZERO,
        errors,
        notes,
        samples: Vec::new(),
        broker_samples: Vec::new(),
    }
}

// ── Producer task ───────────────────────────────────────────────────────────

struct ProducerTask {
    idx: usize,
    scenario: Scenario,
    bootstrap: String,
    topic: String,
    scenario_id: u64,
    stop: Arc<AtomicU8>,
    first_ack: Arc<AtomicU64>,
    security: Option<ClientSecurity>,
    grid: Grid,
    request_timeout: Time,
    final_drain_timeout: Time,
}

async fn run_producer(task: ProducerTask) -> ProducerOut {
    let ProducerTask {
        idx,
        scenario,
        bootstrap,
        topic,
        scenario_id,
        stop,
        first_ack,
        security,
        grid,
        request_timeout,
        final_drain_timeout,
    } = task;
    // Idempotence forces acks=All; turn it off whenever the scenario
    // requested something weaker.
    let enable_idempotence = matches!(scenario.acks, crate::scenario::Acks::All);
    let producer = match Producer::builder()
        .bootstrap(bootstrap.clone())
        .client_id(format!("bench-producer-{idx}"))
        .acks(scenario.acks.into_producer())
        .compression(scenario.compression.into_producer())
        .enable_idempotence(enable_idempotence)
        .linger(scenario.linger.to_std())
        .batch_size(scenario.batch_size.bytes_usize())
        // The producer pipelines one in-flight request PER PARTITION and uses
        // this value as the cross-partition fan-out cap (how many distinct
        // partitions it services concurrently per drain cycle). The default (5)
        // throttles topics with more partitions than that — our scenarios run 6
        // to 100 partitions — so raise it well past the max partition count to
        // keep every partition's pipeline busy.
        .max_in_flight_per_connection(128)
        // A produce to a healthy broker completes in low single-digit ms, so a
        // 2s ceiling should not trip under normal load — but it bounds how long a
        // send to a *killed* leader blocks before the client re-routes, so the
        // measured failover recovery reflects the cluster's leader re-election
        // rather than the 30s default request timeout.
        .request_timeout(request_timeout.to_std())
        // `None` → plaintext (default). `Some` → all produce traffic for this
        // task goes over the broker's TLS listener.
        .maybe_security(security)
        .build()
        .await
        .context("build producer")
    {
        Ok(p) => p,
        Err(e) => {
            return ProducerOut {
                latency: hist::new(),
                msgs: 0,
                bytes: 0,
                dropped: 0,
                recovery_unix_ms: 0,
                latency_spike_max: Time::ZERO,
                error: format!("producer-{idx}-build: {e:#}"),
                interval_msgs: Vec::new(),
                interval_hist: Vec::new(),
            };
        }
    };

    let mut tmpl = payload::template(scenario.msg_size);
    let mut meas_hist = hist::new();
    let mut iv_msgs = vec![0u64; grid.n];
    let mut iv_hist: Vec<Histogram<u64>> = (0..grid.n).map(|_| hist::new()).collect();
    let mut meas_msgs = 0u64;
    let mut meas_bytes = 0u64;
    let mut dropped = 0u64;
    let mut recovery_unix_ms = 0u64;
    let mut latency_spike_max = Time::ZERO;
    let mut kill_observed = false;
    let mut error = String::new();

    let mut pacer = match scenario.mode {
        LoadMode::Saturate => None,
        // The scenario rate is for the whole run, so each task paces at its
        // share of it. `Pacer` floors the result at one message a second.
        LoadMode::FixedRate { rate } => Some(Pacer::new(rate / to_f64(scenario.producers.max(1)))),
    };

    // Pipeline depth: how many records may be in flight (awaiting their ack)
    // before the loop applies back-pressure. Without pipelining the loop sent
    // one record and awaited its ack before the next, capping a task's
    // throughput at 1/RTT regardless of cluster capacity — so the numbers
    // measured the driver, not the cluster. A bounded window lets throughput
    // track the cluster while keeping memory in hand.
    let max_inflight: usize = 512;
    let mut inflight: FuturesUnordered<AckFuture> = FuturesUnordered::new();

    // Record one *settled* send (`Ok` = ack, `Err` = producer error). A macro,
    // not a closure, so it can mutate the per-task accumulators in place.
    macro_rules! handle_ack {
        ($res:expr, $t0:expr) => {
            match $res {
                Ok(_meta) => {
                    let latency = $t0.elapsed().as_time();
                    if stop.load(Ordering::Relaxed) == STATE_MEASURING {
                        hist::record(&mut meas_hist, latency);
                        meas_msgs += 1;
                        meas_bytes += scenario.msg_size.bytes_u64();
                        let iv = grid.idx(Instant::now());
                        iv_msgs[iv] += 1;
                        hist::record(&mut iv_hist[iv], latency);
                        if kill_observed && recovery_unix_ms == 0 {
                            recovery_unix_ms =
                                nonnegative_i64_to_u64(Utc::now().timestamp_millis());
                        }
                        if kill_observed && latency > latency_spike_max {
                            latency_spike_max = latency;
                        }
                    }
                    if first_ack.load(Ordering::Relaxed) == 0 {
                        let now_ms = nonnegative_i64_to_u64(Utc::now().timestamp_millis());
                        let _ = first_ack.compare_exchange(
                            0,
                            now_ms,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        );
                    }
                }
                Err(e) => {
                    if stop.load(Ordering::Relaxed) == STATE_MEASURING {
                        dropped += 1;
                    }
                    kill_observed = true;
                    if dropped == 1 && error.is_empty() {
                        error = format!("producer-{idx}-err: {e}");
                    }
                }
            }
        };
    }
    // The ack channel was dropped before a result (producer gone).
    macro_rules! handle_dropped {
        () => {{
            if stop.load(Ordering::Relaxed) == STATE_MEASURING {
                dropped += 1;
            }
            kill_observed = true;
            if dropped == 1 && error.is_empty() {
                error = format!("producer-{idx}-rx-closed");
            }
        }};
    }
    macro_rules! handle_ack_result {
        ($res:expr, $t0:expr) => {{
            match $res {
                Ok(res) => handle_ack!(res, $t0),
                Err(_) => handle_dropped!(),
            }
        }};
    }

    loop {
        let state = stop.load(Ordering::Relaxed);
        if state == STATE_STOP {
            break;
        }
        // Drain every ack that has already settled — promptly, so the recorded
        // latency is the real send→ack time, not how long a record waited in
        // the in-flight set behind an older stuck send.
        while let Some((res, t0)) = inflight.next().now_or_never().flatten() {
            handle_ack_result!(res, t0);
        }
        // Back-pressure: at capacity, block until any send completes. Waiting
        // for the oldest can make one dead-leader send hide live-partition acks.
        while inflight.len() >= max_inflight {
            if let Some((res, t0)) = inflight.next().await {
                handle_ack_result!(res, t0);
            }
        }
        if let Some(p) = pacer.as_mut() {
            p.await_token().await;
        }
        let value = payload::stamp_into(&mut tmpl, scenario_id);
        let rec = ProducerRecord {
            topic: topic.clone(),
            value: Some(value),
            ..Default::default()
        };
        let t0 = Instant::now();
        let rx = producer.send(rec).await;
        inflight.push(Box::pin(async move { (rx.await, t0) }));
    }

    // Drain anything still outstanding when the measurement window closed, but
    // do not let a stuck producer retry slot keep the whole benchmark job alive
    // forever. Unsettled sends are counted as drops so failover reports capture
    // the stall instead of timing out without a JSON result.
    let drain_until = Instant::now() + final_drain_timeout.to_std();
    while !inflight.is_empty() {
        let now = Instant::now();
        if now >= drain_until {
            let unresolved = inflight.len() as u64;
            dropped += unresolved;
            if error.is_empty() {
                error = format!("producer-{idx}-final-drain-timeout:{unresolved}");
            }
            break;
        }
        match tokio::time::timeout_at(drain_until, inflight.next()).await {
            Ok(Some((res, t0))) => handle_ack_result!(res, t0),
            Ok(None) => break,
            Err(_) => {
                let unresolved = inflight.len() as u64;
                dropped += unresolved;
                if error.is_empty() {
                    error = format!("producer-{idx}-final-drain-timeout:{unresolved}");
                }
                break;
            }
        }
    }

    let _ = producer.flush().await;
    let _ = producer.close().await;

    ProducerOut {
        latency: meas_hist,
        msgs: meas_msgs,
        bytes: meas_bytes,
        dropped,
        recovery_unix_ms,
        latency_spike_max,
        error,
        interval_msgs: iv_msgs,
        interval_hist: iv_hist,
    }
}

// ── Consumer task ───────────────────────────────────────────────────────────

struct ConsumerTask {
    idx: usize,
    scenario: Scenario,
    bootstrap: String,
    topic: String,
    scenario_id: u64,
    stop: Arc<AtomicU8>,
    security: Option<ClientSecurity>,
    grid: Grid,
    request_timeout: Time,
    build_retry_policy: ConsumerBuildRetryPolicy,
    poll_timeout: Time,
    poll_error_backoff: Time,
}

async fn run_consumer(task: ConsumerTask) -> ConsumerOut {
    let ConsumerTask {
        idx,
        scenario,
        bootstrap,
        topic,
        scenario_id,
        stop,
        security,
        grid,
        request_timeout,
        build_retry_policy,
        poll_timeout,
        poll_error_backoff,
    } = task;
    let group_id = format!("crabka-bench-{}", scenario.name);
    let mut consumer = match build_consumer_with_retry(
        idx,
        &bootstrap,
        group_id,
        &topic,
        security,
        request_timeout,
        build_retry_policy,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return ConsumerOut {
                latency: hist::new(),
                msgs: 0,
                bytes: 0,
                error: format!("consumer-{idx}-build: {e:#}"),
                interval_msgs: Vec::new(),
                interval_hist: Vec::new(),
            };
        }
    };

    let mut meas_hist = hist::new();
    let mut iv_msgs = vec![0u64; grid.n];
    let mut iv_hist: Vec<Histogram<u64>> = (0..grid.n).map(|_| hist::new()).collect();
    let mut meas_msgs = 0u64;
    let mut meas_bytes = 0u64;
    let mut error = String::new();

    loop {
        if stop.load(Ordering::Relaxed) == STATE_STOP {
            break;
        }
        match consumer.poll(poll_timeout).await {
            Ok(records) => {
                let now_ns =
                    nonnegative_i64_to_u64(Utc::now().timestamp_nanos_opt().unwrap_or_default());
                let phase = stop.load(Ordering::Relaxed);
                let iv = grid.idx(Instant::now());
                for r in records {
                    if let Some(val) = &r.value {
                        let bytes = val.len() as u64;
                        if let Some(send_nanos) = payload::read_send_nanos(val, scenario_id) {
                            // Both stamps are epoch nanoseconds; their
                            // difference is the end-to-end extent.
                            let latency = Time::from_nanos(saturating_u64_to_i64(
                                now_ns.saturating_sub(send_nanos),
                            ));
                            if phase == STATE_MEASURING {
                                hist::record(&mut meas_hist, latency);
                                meas_msgs += 1;
                                meas_bytes += bytes;
                                iv_msgs[iv] += 1;
                                hist::record(&mut iv_hist[iv], latency);
                            }
                        } else if phase == STATE_MEASURING {
                            // Non-bench record (e.g. left over from a prior
                            // run). Count bytes but not E2E latency.
                            meas_bytes += bytes;
                        }
                    }
                }
            }
            Err(e) => {
                if error.is_empty() {
                    error = format!("consumer-{idx}-poll: {e}");
                }
                tokio::time::sleep(poll_error_backoff.to_std()).await;
            }
        }
    }
    let _ = consumer.close().await;
    ConsumerOut {
        latency: meas_hist,
        msgs: meas_msgs,
        bytes: meas_bytes,
        error,
        interval_msgs: iv_msgs,
        interval_hist: iv_hist,
    }
}

async fn build_consumer_with_retry(
    idx: usize,
    bootstrap: &str,
    group_id: String,
    topic: &str,
    security: Option<ClientSecurity>,
    request_timeout: Time,
    retry_policy: ConsumerBuildRetryPolicy,
) -> Result<Consumer> {
    let backoff = exponential_backoff::Backoff::new(
        retry_policy.attempts(),
        retry_policy.initial_backoff().to_std(),
        Some(retry_policy.max_backoff().to_std()),
    );
    for (attempt_idx, delay) in backoff.into_iter().enumerate() {
        let attempt = attempt_idx + 1;
        let result = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(format!("bench-consumer-{idx}"))
            .group_id(group_id.clone())
            .subscribe(vec![topic.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .request_timeout(request_timeout)
            // `None` → plaintext (default). `Some` → all fetch traffic for this
            // task goes over the broker's TLS listener (kTLS sendfile on crabka).
            .maybe_security(security.clone())
            .build()
            .await
            .context("build consumer");

        match result {
            Ok(consumer) => return Ok(consumer),
            Err(e) => match delay {
                Some(delay) => {
                    warn!(
                        attempt,
                        retry_after_ms = delay.as_millis(),
                        error = %e,
                        "consumer build failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                None => {
                    return Err(
                        e.context(format!("build consumer failed after {attempt} attempts"))
                    );
                }
            },
        }
    }
    unreachable!("exponential_backoff::Backoff yields a terminal attempt");
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::scenario::{Acks, Compression, FailoverSpec, LoadMode, ModeTag};

    fn cfg(broker_count: u32) -> DriverConfig {
        DriverConfig {
            bootstrap: "broker:9092".into(),
            topic: "t".into(),
            stack: Stack::Crabka,
            namespace: "default".into(),
            prometheus_url: None,
            prometheus_request_timeout: crate::prom::DEFAULT_PROMETHEUS_REQUEST_TIMEOUT,
            producer_request_timeout: default_producer_request_timeout(),
            producer_final_drain_timeout: DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT,
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            consumer_request_timeout: default_consumer_request_timeout(Stack::Crabka),
            consumer_build_retry_policy: ConsumerBuildRetryPolicy::default(),
            consumer_poll_timeout: default_consumer_poll_timeout(),
            consumer_poll_error_backoff: default_consumer_poll_error_backoff(),
            broker_count,
            scenario_id: 0,
            tls: None,
        }
    }

    fn scenario(rf: i16) -> Scenario {
        Scenario {
            name: "x".into(),
            mode_tag: ModeTag::Ci,
            msg_size: bytes(100),
            key_size: ByteSize::ZERO,
            partitions: 1,
            replication_factor: rf,
            producers: 1,
            consumers: 1,
            mode: LoadMode::Saturate,
            acks: Acks::Leader,
            compression: Compression::None,
            linger: Time::ZERO,
            batch_size: kibibytes(16),
            duration: secs(1),
            warmup: Time::ZERO,
            failover: None,
        }
    }

    #[test]
    fn client_request_timeout_defaults_preserve_policy() {
        check!(default_producer_request_timeout() == secs(2));
        check!(default_consumer_request_timeout(Stack::Crabka) == secs(5));
        check!(default_consumer_request_timeout(Stack::Kafka) == secs(30));
    }

    #[test]
    fn consumer_build_retry_defaults_preserve_policy() {
        let policy = ConsumerBuildRetryPolicy::default();

        check!(policy.attempts() == 6);
        check!(policy.initial_backoff() == millis(100));
        check!(policy.max_backoff() == secs(2));
    }

    #[test]
    fn consumer_build_retry_accepts_positive_minimum_and_equal_backoffs() {
        let attempts = ConsumerBuildAttempts::new(1).expect("one attempt is valid");
        let policy = ConsumerBuildRetryPolicy::new(attempts, millis(1), millis(1))
            .expect("equal bounds are valid");

        check!(policy.attempts() == 1);
        check!(policy.initial_backoff() == millis(1));
        check!(policy.max_backoff() == millis(1));
    }

    #[test]
    fn consumer_build_retry_rejects_invalid_primitive_values() {
        for invalid in ["0", "not-a-number", "-1", "4294967296"] {
            check!(
                invalid.parse::<ConsumerBuildAttempts>().is_err(),
                "attempts {invalid:?} must be rejected"
            );
        }
    }

    #[test]
    fn consumer_build_retry_rejects_inverted_backoff_range() {
        let attempts = ConsumerBuildAttempts::new(1).expect("valid attempts");
        assert!(ConsumerBuildRetryPolicy::new(attempts, millis(2), millis(1)).is_err());
    }

    #[test]
    fn consumer_poll_timing_defaults_preserve_behavior() {
        check!(default_consumer_poll_timeout() == millis(50));
        check!(default_consumer_poll_error_backoff() == millis(100));
    }

    #[test]
    fn producer_final_drain_timeout_default_preserves_behavior() {
        check!(DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT == secs(10));
    }

    #[test]
    fn sample_interval_default_preserves_behavior() {
        assert_eq!(DEFAULT_SAMPLE_INTERVAL, secs(2));
    }

    // TLS enabled: a CA path + server_name must build a `ClientSecurity` whose
    // protocol is `Ssl`, whose TLS config carries the mounted CA as trust roots
    // and the explicit SNI, and (one-way TLS) no client identity. This is the
    // exact shape passed to `.maybe_security(...)` on both clients.
    #[test]
    fn tls_params_build_ssl_security_with_server_name() {
        let params = TlsParams {
            ca_path: "/etc/bench-ca/ca.crt".into(),
            server_name: "demo-broker-headless.default.svc.cluster.local".into(),
            client_identity: None,
        };
        let sec = params.to_security();
        assert2::assert!(sec.protocol == ListenerProtocol::Ssl);
        assert2::assert!(sec.protocol.requires_tls());
        assert2::assert!(sec.sasl.is_none());
        let tls = sec.tls.expect("Ssl security carries a TLS config");
        assert2::assert!(
            tls.server_name.as_str() == "demo-broker-headless.default.svc.cluster.local"
        );
        assert2::assert!(
            tls.trust_roots_pem == Some(std::path::PathBuf::from("/etc/bench-ca/ca.crt"))
        );
        assert2::assert!(tls.client_identity == None);
    }

    // mTLS variant: a client identity threads through to the TLS config.
    #[test]
    fn tls_params_carry_client_identity_for_mtls() {
        let params = TlsParams {
            ca_path: "/etc/bench-ca/ca.crt".into(),
            server_name: "demo-kafka-bootstrap".into(),
            client_identity: Some(("/c/tls.crt".into(), "/c/tls.key".into())),
        };
        let sec = params.to_security();
        let tls = sec.tls.expect("TLS config present");
        assert2::assert!(
            tls.client_identity
                == Some((
                    std::path::PathBuf::from("/c/tls.crt"),
                    std::path::PathBuf::from("/c/tls.key"),
                ))
        );
    }

    // Disabled: when `DriverConfig.tls` is `None`, the derived security is
    // `None` (plaintext) — the unchanged default benchmark path.
    #[test]
    fn no_tls_params_means_plaintext_security_is_none() {
        let c = cfg(1);
        assert2::assert!(c.tls.is_none());
        let security: Option<ClientSecurity> = c.tls.as_ref().map(TlsParams::to_security);
        assert2::assert!(security.is_none());
    }

    #[test]
    fn empty_output_preserves_inputs() {
        let s = scenario(1);
        let c = cfg(1);
        let out = empty_output(&s, &c, 42, vec!["a-note".into()], vec!["an-error".into()]);
        check!(out.wallclock_start_unix_ms == WallclockMs(42));
        check!(out.wallclock_end_unix_ms == WallclockMs(42));
        check!(out.topology.broker_count == 1);
        check!(out.notes == vec!["a-note".to_owned()]);
        check!(out.errors == vec!["an-error".to_owned()]);
        check!(out.first_ack == Time::ZERO);
        check!(out.disturbance.is_none());
    }

    // The state byte is shared across producer/consumer tasks; verify the
    // three values are pairwise distinct so a flat AtomicU8 can encode them
    // without ambiguity.
    #[test]
    fn state_constants_are_distinct() {
        check!(STATE_RUN != STATE_MEASURING);
        check!(STATE_MEASURING != STATE_STOP);
        check!(STATE_RUN != STATE_STOP);
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_mode_rf3_with_one_broker_is_skipped() {
        let mut s = scenario(3);
        s.mode_tag = ModeTag::Cluster;
        let out = run(s, cfg(1)).await.expect("run returned");
        assert2::assert!(out.throughput.msgs_produced == 0);
        assert2::assert!(out.notes.iter().any(|n| n.contains("topology-mismatch")));
    }

    #[tokio::test(start_paused = true)]
    // The producer/consumer build step makes a real TCP connection to
    // "broker:9092" before checking STATE_STOP.  On Linux the OS returns
    // ECONNREFUSED immediately; on Windows DNS for "broker" triggers
    // LLMNR/NetBIOS probes that take minutes to time out, hanging the test.
    #[cfg_attr(
        windows,
        ignore = "broker DNS hangs on Windows; skip-note logic is OS-independent"
    )]
    async fn failover_request_without_rf3_records_skip_note() {
        // Scenario asks for failover, but RF=1 + 1 broker → driver must
        // record a skip note. A zero duration and warmup mean the
        // producer/consumer build loops exit immediately, so this is
        // safe to run without a live broker.
        let mut s = scenario(1);
        s.failover = Some(FailoverSpec {
            kill_after: secs(1),
            target: "partition0_leader".into(),
        });
        s.warmup = Time::ZERO;
        s.duration = Time::ZERO;
        let out = run(s, cfg(1)).await.expect("run returned");
        check!(
            out.notes
                .iter()
                .any(|n| n.contains("skipped:failover-needs-rf3"))
        );
    }
}
