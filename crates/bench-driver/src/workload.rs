//! Producer + consumer workload runners. Builds N producers and M
//! consumers (members of one group), runs warmup → measurement
//! phases, optionally triggers a failover mid-measurement, and merges
//! per-task histograms into the public `LatencyPercentiles` shape.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use hdrhistogram::Histogram;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{info, warn};

use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::{Producer, ProducerRecord};

use crate::hist;
use crate::payload;
use crate::prom::PromClient;
use crate::rate::Pacer;
use crate::scenario::{
    Disturbance, LoadMode, ModeTag, Resource, RunOutput, Scenario, Stack, Throughput, Topology,
};

/// Parameters wired in from `main` / CLI. Distinct from `Scenario` because
/// the scenario YAML describes *what* to run, while this struct describes
/// *where* it's running.
pub struct DriverConfig {
    pub bootstrap: String,
    pub topic: String,
    pub stack: Stack,
    pub namespace: String,
    pub prometheus_url: Option<String>,
    pub broker_count: u32,
    pub scenario_id: u64,
}

/// Top-level entrypoint called by `main`. Returns the populated
/// `RunOutput`. The caller is responsible for serialising it to disk.
pub async fn run(scenario: Scenario, cfg: DriverConfig) -> Result<RunOutput> {
    let wallclock_start = Utc::now().timestamp_millis();
    let t_start = Instant::now();

    let mut notes: Vec<String> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    // Failover scenarios need RF >= 3; otherwise mark skipped.
    let failover_active =
        scenario.failover.is_some() && scenario.replication_factor >= 3 && cfg.broker_count >= 3;
    if scenario.failover.is_some() && !failover_active {
        notes.push("skipped:failover-needs-rf3".into());
    }

    if matches!(scenario.mode_tag, ModeTag::Cluster)
        && scenario.replication_factor >= 3
        && cfg.broker_count < 3
    {
        notes.push(format!(
            "skipped:topology-mismatch (rf={} brokers={})",
            scenario.replication_factor, cfg.broker_count
        ));
        return Ok(empty_output(
            &scenario,
            &cfg,
            wallclock_start,
            notes,
            errors,
        ));
    }

    // ── Producer workloads ──────────────────────────────────────────────────
    let mut prod_set: JoinSet<ProducerOut> = JoinSet::new();
    let stop = Arc::new(AtomicU8::new(STATE_RUN));
    let first_ack_unix_ms = Arc::new(AtomicU64::new(0));

    for i in 0..scenario.producers {
        let s = scenario.clone();
        let bootstrap = cfg.bootstrap.clone();
        let topic = cfg.topic.clone();
        let stop = stop.clone();
        let first_ack = first_ack_unix_ms.clone();
        let sid = cfg.scenario_id;
        prod_set
            .spawn(async move { run_producer(i, s, bootstrap, topic, sid, stop, first_ack).await });
    }

    // ── Consumer workloads ──────────────────────────────────────────────────
    let mut cons_set: JoinSet<ConsumerOut> = JoinSet::new();
    for i in 0..scenario.consumers {
        let s = scenario.clone();
        let bootstrap = cfg.bootstrap.clone();
        let topic = cfg.topic.clone();
        let stop = stop.clone();
        let sid = cfg.scenario_id;
        cons_set.spawn(async move { run_consumer(i, s, bootstrap, topic, sid, stop).await });
    }

    // ── Failover orchestrator task ──────────────────────────────────────────
    let kill_at_ms = Arc::new(AtomicU64::new(0));
    if failover_active {
        let spec = scenario.failover.clone().expect("checked above");
        let stack = cfg.stack;
        let namespace = cfg.namespace.clone();
        let kill_at = kill_at_ms.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(t_start + Duration::from_secs(spec.kill_at_s)).await;
            let ms = Utc::now().timestamp_millis() as u64;
            kill_at.store(ms, Ordering::SeqCst);
            match crate::failover::try_client().await {
                Ok(client) => {
                    match crate::failover::kill_first_broker(&client, stack, &namespace).await {
                        Ok(name) => info!(pod = %name, "failover: killed broker"),
                        Err(e) => warn!(error = %e, "failover: kill_first_broker failed"),
                    }
                }
                Err(e) => warn!(error = %e, "failover: in-cluster client unavailable"),
            }
        });
    }

    // ── Phase 1: warmup ─────────────────────────────────────────────────────
    let warmup_end = t_start + Duration::from_secs(scenario.warmup_s);
    tokio::time::sleep_until(warmup_end).await;
    stop.store(STATE_MEASURING, Ordering::SeqCst);

    // ── Phase 2: measurement ────────────────────────────────────────────────
    let meas_end = warmup_end + Duration::from_secs(scenario.duration_s);
    tokio::time::sleep_until(meas_end).await;
    stop.store(STATE_STOP, Ordering::SeqCst);

    // ── Join all tasks ──────────────────────────────────────────────────────
    let mut prod_hist = hist::new();
    let mut prod_msgs = 0u64;
    let mut prod_bytes = 0u64;
    let mut prod_dropped = 0u64;
    let mut earliest_recovery_ms = 0u64;
    let mut max_spike_us = 0u64;
    while let Some(j) = prod_set.join_next().await {
        match j {
            Ok(t) => {
                prod_hist.add(&t.latency).ok();
                prod_msgs += t.msgs;
                prod_bytes += t.bytes;
                prod_dropped += t.dropped;
                if t.latency_spike_max_us > max_spike_us {
                    max_spike_us = t.latency_spike_max_us;
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
    while let Some(j) = cons_set.join_next().await {
        match j {
            Ok(t) => {
                cons_hist.add(&t.latency).ok();
                cons_msgs += t.msgs;
                cons_bytes += t.bytes;
                if !t.error.is_empty() {
                    errors.push(t.error);
                }
            }
            Err(e) => errors.push(format!("consumer-task-panic: {e}")),
        }
    }

    let wallclock_end = Utc::now().timestamp_millis();
    let duration_s = scenario.duration_s.max(1) as f64;

    // ── Resource capture from Prometheus ────────────────────────────────────
    let resource = if let Some(url) = &cfg.prometheus_url {
        match PromClient::new(url) {
            Ok(c) => match c
                .capture_resource(cfg.stack, &cfg.namespace, scenario.duration_s, prod_msgs)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "prometheus capture failed");
                    notes.push(format!("prometheus-capture-failed: {e}"));
                    Resource::default()
                }
            },
            Err(e) => {
                notes.push(format!("prometheus-client-failed: {e}"));
                Resource::default()
            }
        }
    } else {
        notes.push("prometheus-url-not-set".into());
        Resource::default()
    };

    let disturbance = if failover_active {
        Some(Disturbance {
            kill_at_ms: kill_at_ms.load(Ordering::SeqCst),
            recovery_at_ms: earliest_recovery_ms,
            dropped: prod_dropped,
            latency_spike_max_ms: max_spike_us as f64 / 1000.0,
        })
    } else {
        None
    };

    let first_ack = first_ack_unix_ms.load(Ordering::SeqCst);
    let first_ack_ms = if first_ack == 0 {
        0
    } else {
        (first_ack as i64 - wallclock_start).max(0) as u64
    };

    Ok(RunOutput {
        scenario: scenario.clone(),
        stack: cfg.stack,
        topology: Topology {
            partitions: scenario.partitions,
            replication_factor: scenario.replication_factor,
            broker_count: cfg.broker_count,
        },
        wallclock_start_unix_ms: wallclock_start,
        wallclock_end_unix_ms: wallclock_end,
        throughput: Throughput {
            msgs_produced: prod_msgs,
            msgs_consumed: cons_msgs,
            mb_in: bytes_to_mb(prod_bytes),
            mb_out: bytes_to_mb(cons_bytes),
            producer_msgs_per_sec: prod_msgs as f64 / duration_s,
            consumer_msgs_per_sec: cons_msgs as f64 / duration_s,
        },
        producer_latency_ms: hist::percentiles(&prod_hist),
        consumer_e2e_latency_ms: hist::percentiles(&cons_hist),
        resource,
        disturbance,
        startup_ms: None,
        first_ack_ms,
        errors,
        notes,
    })
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
    latency_spike_max_us: u64,
    error: String,
}

struct ConsumerOut {
    latency: Histogram<u64>,
    msgs: u64,
    bytes: u64,
    error: String,
}

fn bytes_to_mb(bytes: u64) -> f64 {
    (bytes as f64) / 1_048_576.0
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
        wallclock_start_unix_ms: start,
        wallclock_end_unix_ms: start,
        throughput: Throughput::default(),
        producer_latency_ms: crate::scenario::LatencyPercentiles::default(),
        consumer_e2e_latency_ms: crate::scenario::LatencyPercentiles::default(),
        resource: Resource::default(),
        disturbance: None,
        startup_ms: None,
        first_ack_ms: 0,
        errors,
        notes,
    }
}

// ── Producer task ───────────────────────────────────────────────────────────

async fn run_producer(
    idx: usize,
    scenario: Scenario,
    bootstrap: String,
    topic: String,
    scenario_id: u64,
    stop: Arc<AtomicU8>,
    first_ack: Arc<AtomicU64>,
) -> ProducerOut {
    // Idempotence forces acks=All; turn it off whenever the scenario
    // requested something weaker.
    let enable_idempotence = matches!(scenario.acks, crate::scenario::Acks::All);
    let producer = match Producer::builder()
        .bootstrap(bootstrap.clone())
        .client_id(format!("bench-producer-{idx}"))
        .acks(scenario.acks.into_producer())
        .compression(scenario.compression.into_producer())
        .enable_idempotence(enable_idempotence)
        .linger(Duration::from_millis(scenario.linger_ms))
        .batch_size(scenario.batch_size)
        // A produce to a healthy broker completes in low single-digit ms, so a
        // 10s ceiling never trips under normal load — but it bounds how long a
        // send to a *killed* leader blocks before the client re-routes, so the
        // measured failover recovery reflects the cluster's leader re-election
        // (single-digit seconds) rather than the 30s default request timeout.
        .request_timeout(Duration::from_secs(10))
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
                latency_spike_max_us: 0,
                error: format!("producer-{idx}-build: {e:#}"),
            };
        }
    };

    let mut tmpl = payload::template(scenario.msg_size_bytes);
    let mut meas_hist = hist::new();
    let mut meas_msgs = 0u64;
    let mut meas_bytes = 0u64;
    let mut dropped = 0u64;
    let mut recovery_unix_ms = 0u64;
    let mut latency_spike_max_us = 0u64;
    let mut kill_observed = false;
    let mut error = String::new();

    let mut pacer = match scenario.mode {
        LoadMode::Saturate => None,
        LoadMode::FixedRate { msgs_per_sec } => {
            let per_task = (msgs_per_sec / scenario.producers.max(1) as u64).max(1);
            Some(Pacer::new(per_task))
        }
    };

    // Pipeline depth: how many records may be in flight (awaiting their ack)
    // before the loop drains the oldest. Without this the loop sends one record
    // and awaits its ack before the next, capping a task's throughput at 1/RTT
    // regardless of cluster capacity — so the numbers measured the driver, not
    // the cluster. A bounded window lets throughput track the cluster while
    // keeping memory and back-pressure in hand.
    const MAX_INFLIGHT: usize = 512;
    let mut inflight = std::collections::VecDeque::new();

    // Process one resolved ack. A macro (not a closure) so it can mutate the
    // per-task accumulators in place without borrow-checker gymnastics.
    macro_rules! process_ack {
        ($res:expr, $t0:expr) => {
            match $res {
                Ok(Ok(_meta)) => {
                    let us = $t0.elapsed().as_micros() as u64;
                    let now_state = stop.load(Ordering::Relaxed);
                    if now_state == STATE_MEASURING {
                        hist::record_us(&mut meas_hist, us);
                        meas_msgs += 1;
                        meas_bytes += scenario.msg_size_bytes as u64;
                        if kill_observed && recovery_unix_ms == 0 {
                            recovery_unix_ms = Utc::now().timestamp_millis() as u64;
                        }
                        if kill_observed && us > latency_spike_max_us {
                            latency_spike_max_us = us;
                        }
                    }
                    if first_ack.load(Ordering::Relaxed) == 0 {
                        let now_ms = Utc::now().timestamp_millis() as u64;
                        let _ = first_ack.compare_exchange(
                            0,
                            now_ms,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        );
                    }
                }
                Ok(Err(e)) => {
                    if stop.load(Ordering::Relaxed) == STATE_MEASURING {
                        dropped += 1;
                    }
                    kill_observed = true;
                    if dropped == 1 && error.is_empty() {
                        error = format!("producer-{idx}-first-err: {e}");
                    }
                }
                Err(e) => {
                    if stop.load(Ordering::Relaxed) == STATE_MEASURING {
                        dropped += 1;
                    }
                    kill_observed = true;
                    if dropped == 1 && error.is_empty() {
                        error = format!("producer-{idx}-rx-closed: {e}");
                    }
                }
            }
        };
    }

    loop {
        let state = stop.load(Ordering::Relaxed);
        if state == STATE_STOP {
            break;
        }
        // Drain the oldest acks once the window is full — bounds memory and
        // applies back-pressure at the cluster's completion rate.
        while inflight.len() >= MAX_INFLIGHT {
            let (rx, t0): (_, Instant) = inflight.pop_front().expect("len checked");
            process_ack!(rx.await, t0);
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
        inflight.push_back((rx, t0));
    }

    // Drain any acks still in flight when the measurement window closed.
    while let Some((rx, t0)) = inflight.pop_front() {
        process_ack!(rx.await, t0);
    }

    let _ = producer.flush().await;
    let _ = producer.close().await;

    ProducerOut {
        latency: meas_hist,
        msgs: meas_msgs,
        bytes: meas_bytes,
        dropped,
        recovery_unix_ms,
        latency_spike_max_us,
        error,
    }
}

// ── Consumer task ───────────────────────────────────────────────────────────

async fn run_consumer(
    idx: usize,
    scenario: Scenario,
    bootstrap: String,
    topic: String,
    scenario_id: u64,
    stop: Arc<AtomicU8>,
) -> ConsumerOut {
    let group_id = format!("crabka-bench-{}", scenario.name);
    let mut consumer = match Consumer::builder()
        .bootstrap(bootstrap.clone())
        .client_id(format!("bench-consumer-{idx}"))
        .group_id(group_id)
        .subscribe(vec![topic.clone()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .context("build consumer")
    {
        Ok(c) => c,
        Err(e) => {
            return ConsumerOut {
                latency: hist::new(),
                msgs: 0,
                bytes: 0,
                error: format!("consumer-{idx}-build: {e:#}"),
            };
        }
    };

    let mut meas_hist = hist::new();
    let mut meas_msgs = 0u64;
    let mut meas_bytes = 0u64;
    let mut error = String::new();

    loop {
        if stop.load(Ordering::Relaxed) == STATE_STOP {
            break;
        }
        match consumer.poll(Duration::from_millis(50)).await {
            Ok(records) => {
                let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or_default() as u64;
                let phase = stop.load(Ordering::Relaxed);
                for r in records {
                    if let Some(val) = &r.value {
                        let bytes = val.len() as u64;
                        if let Some(send_nanos) = payload::read_send_nanos(val, scenario_id) {
                            let latency_us = (now_ns.saturating_sub(send_nanos)) / 1000;
                            if phase == STATE_MEASURING {
                                hist::record_us(&mut meas_hist, latency_us);
                                meas_msgs += 1;
                                meas_bytes += bytes;
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
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    let _ = consumer.close().await;
    ConsumerOut {
        latency: meas_hist,
        msgs: meas_msgs,
        bytes: meas_bytes,
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{Acks, Compression, FailoverSpec, LoadMode, ModeTag};
    use assert2::assert;

    fn cfg(broker_count: u32) -> DriverConfig {
        DriverConfig {
            bootstrap: "broker:9092".into(),
            topic: "t".into(),
            stack: Stack::Crabka,
            namespace: "default".into(),
            prometheus_url: None,
            broker_count,
            scenario_id: 0,
        }
    }

    fn scenario(rf: i16) -> Scenario {
        Scenario {
            name: "x".into(),
            mode_tag: ModeTag::Ci,
            msg_size_bytes: 100,
            key_size_bytes: 0,
            partitions: 1,
            replication_factor: rf,
            producers: 1,
            consumers: 1,
            mode: LoadMode::Saturate,
            acks: Acks::Leader,
            compression: Compression::None,
            linger_ms: 0,
            batch_size: 16384,
            duration_s: 1,
            warmup_s: 0,
            failover: None,
        }
    }

    #[test]
    fn bytes_to_mb_is_proper_mebibyte() {
        assert!((bytes_to_mb(1_048_576) - 1.0).abs() < 1e-9);
        assert!(bytes_to_mb(0).abs() < 1e-9);
    }

    #[test]
    fn empty_output_preserves_inputs() {
        let s = scenario(1);
        let c = cfg(1);
        let out = empty_output(&s, &c, 42, vec!["a-note".into()], vec!["an-error".into()]);
        assert!(out.wallclock_start_unix_ms == 42);
        assert!(out.wallclock_end_unix_ms == 42);
        assert!(out.topology.broker_count == 1);
        assert!(out.notes == vec!["a-note"]);
        assert!(out.errors == vec!["an-error"]);
        assert!(out.first_ack_ms == 0);
        assert!(out.disturbance.is_none());
    }

    // The state byte is shared across producer/consumer tasks; verify the
    // three values are pairwise distinct so a flat AtomicU8 can encode them
    // without ambiguity.
    #[test]
    fn state_constants_are_distinct() {
        assert!(STATE_RUN != STATE_MEASURING);
        assert!(STATE_MEASURING != STATE_STOP);
        assert!(STATE_RUN != STATE_STOP);
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_mode_rf3_with_one_broker_is_skipped() {
        let mut s = scenario(3);
        s.mode_tag = ModeTag::Cluster;
        let out = run(s, cfg(1)).await.expect("run returned");
        assert!(out.throughput.msgs_produced == 0);
        assert!(out.notes.iter().any(|n| n.contains("topology-mismatch")));
    }

    #[tokio::test(start_paused = true)]
    async fn failover_request_without_rf3_records_skip_note() {
        // Scenario asks for failover, but RF=1 + 1 broker → driver must
        // record a skip note. duration_s=0/warmup_s=0 means the
        // producer/consumer build loops exit immediately, so this is
        // safe to run without a live broker.
        let mut s = scenario(1);
        s.failover = Some(FailoverSpec {
            kill_at_s: 1,
            target: "partition0_leader".into(),
        });
        s.warmup_s = 0;
        s.duration_s = 0;
        let out = run(s, cfg(1)).await.expect("run returned");
        assert!(
            out.notes
                .iter()
                .any(|n| n.contains("skipped:failover-needs-rf3")),
            "expected failover skip note, got {:?}",
            out.notes
        );
    }
}
