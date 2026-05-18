//! `Scenario` is the on-disk YAML schema the driver reads; `RunOutput` is
//! the on-disk JSON schema the driver writes. The report aggregator reads
//! `RunOutput` documents and emits Markdown.

use serde::{Deserialize, Serialize};

/// Which Kafka stack the scenario is running against. Pure metadata; the
/// driver's client behaviour is identical for both — Crabka's
/// wire-compatible client speaks to either broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stack {
    Crabka,
    Kafka,
}

impl Stack {
    /// Pod-name regex used by `prom.rs` to pick out the right brokers.
    /// Both regexes match the `StatefulSet` names produced by the
    /// respective operators with cluster name `demo`.
    #[must_use]
    pub fn broker_pod_regex(self) -> &'static str {
        match self {
            // Crabka StatefulSet is `<kafka>-<nodepool>` per the operator;
            // the e2e workflow's KafkaNodePool is named `brokers`.
            Stack::Crabka => "^demo-brokers-",
            // Strimzi StatefulSets are `<kafka>-<pool>` with pool typically
            // `kafka` for the broker pool.
            Stack::Kafka => "^demo-kafka-",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModeTag {
    /// Lightweight scenarios suitable for `KinD` in CI.
    Ci,
    /// Heavier scenarios reserved for real Kubernetes clusters.
    Cluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Acks {
    None,
    Leader,
    All,
}

impl Acks {
    pub(crate) fn into_producer(self) -> crabka_client_producer::Acks {
        match self {
            Acks::None => crabka_client_producer::Acks::Zero,
            Acks::Leader => crabka_client_producer::Acks::One,
            Acks::All => crabka_client_producer::Acks::All,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Compression {
    pub(crate) fn into_producer(self) -> crabka_client_producer::Compression {
        match self {
            Compression::None => crabka_client_producer::Compression::None,
            Compression::Gzip => crabka_client_producer::Compression::Gzip,
            Compression::Snappy => crabka_client_producer::Compression::Snappy,
            Compression::Lz4 => crabka_client_producer::Compression::Lz4,
            Compression::Zstd => crabka_client_producer::Compression::Zstd,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LoadMode {
    /// Producers run flat-out and backpressure naturally.
    Saturate,
    /// Producers are paced by a token bucket at exactly this rate.
    FixedRate { msgs_per_sec: u64 },
}

/// Inject a broker kill mid-scenario to measure failover behaviour.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailoverSpec {
    /// Wall-clock offset (seconds from scenario start) at which to delete
    /// the target broker pod.
    pub kill_at_s: u64,
    /// Target: `partition0_leader` picks the broker hosting partition 0's
    /// leader; `any_broker` picks the first matching pod. Only
    /// `partition0_leader` is wired today.
    #[serde(default = "default_failover_target")]
    pub target: String,
}

fn default_failover_target() -> String {
    "partition0_leader".to_string()
}

/// The scenario configuration. Loaded from YAML by the driver and
/// echoed back into `RunOutput.scenario` for the report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    #[serde(default = "default_mode_tag")]
    pub mode_tag: ModeTag,
    #[serde(default = "default_msg_size")]
    pub msg_size_bytes: usize,
    #[serde(default)]
    pub key_size_bytes: usize,
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    #[serde(default = "default_replicas")]
    pub replication_factor: i16,
    #[serde(default = "default_producers")]
    pub producers: usize,
    #[serde(default = "default_consumers")]
    pub consumers: usize,
    pub mode: LoadMode,
    #[serde(default = "default_acks")]
    pub acks: Acks,
    #[serde(default)]
    pub compression: Compression,
    #[serde(default = "default_linger_ms")]
    pub linger_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_duration_s")]
    pub duration_s: u64,
    #[serde(default = "default_warmup_s")]
    pub warmup_s: u64,
    #[serde(default)]
    pub failover: Option<FailoverSpec>,
}

fn default_mode_tag() -> ModeTag {
    ModeTag::Ci
}
fn default_msg_size() -> usize {
    1024
}
fn default_partitions() -> i32 {
    6
}
fn default_replicas() -> i16 {
    1
}
fn default_producers() -> usize {
    1
}
fn default_consumers() -> usize {
    1
}
fn default_acks() -> Acks {
    Acks::Leader
}
fn default_linger_ms() -> u64 {
    5
}
fn default_batch_size() -> usize {
    16 * 1024
}
fn default_duration_s() -> u64 {
    60
}
fn default_warmup_s() -> u64 {
    10
}

// ── Output schema ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub count: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Throughput {
    pub msgs_produced: u64,
    pub msgs_consumed: u64,
    pub mb_in: f64,
    pub mb_out: f64,
    pub producer_msgs_per_sec: f64,
    pub consumer_msgs_per_sec: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resource {
    pub broker_cpu_seconds: f64,
    pub mem_cgroup_working_set_bytes: u64,
    /// Strimzi-only: JVM heap used (sum across broker pods).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jvm_heap_used_bytes: Option<u64>,
    /// Strimzi-only: JVM non-heap used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jvm_nonheap_used_bytes: Option<u64>,
    /// Derived: cgroup working set minus JVM heap + non-heap. Approximates
    /// page-cache footprint on the broker pod. Strimzi-only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka_page_cache_approx_bytes: Option<i64>,
    pub msgs_per_cpu_core: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Disturbance {
    pub kill_at_ms: u64,
    pub recovery_at_ms: u64,
    pub dropped: u64,
    pub latency_spike_max_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub partitions: i32,
    pub replication_factor: i16,
    pub broker_count: u32,
}

/// One run = one scenario × one stack. Written by the driver, read by
/// the report aggregator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunOutput {
    pub scenario: Scenario,
    pub stack: Stack,
    pub topology: Topology,
    pub wallclock_start_unix_ms: i64,
    pub wallclock_end_unix_ms: i64,
    pub throughput: Throughput,
    pub producer_latency_ms: LatencyPercentiles,
    pub consumer_e2e_latency_ms: LatencyPercentiles,
    pub resource: Resource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disturbance: Option<Disturbance>,
    /// Operator+broker startup wall-clock from CR apply → broker Ready
    /// (filled in by `run-scenario.sh` after the driver finishes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup_ms: Option<u64>,
    /// Driver-observed wall-clock from start to first successful
    /// `send().await.await??`.
    pub first_ack_ms: u64,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_yaml_round_trip() {
        let y = r"
name: small-msg-saturate
mode_tag: ci
msg_size_bytes: 100
partitions: 6
replication_factor: 1
producers: 1
consumers: 1
mode:
  kind: saturate
acks: leader
compression: none
linger_ms: 5
batch_size: 16384
duration_s: 60
warmup_s: 10
";
        let s: Scenario = serde_yaml::from_str(y).expect("parse");
        assert_eq!(s.name, "small-msg-saturate");
        assert_eq!(s.partitions, 6);
        assert!(matches!(s.mode, LoadMode::Saturate));
    }

    #[test]
    fn fixed_rate_yaml_parses() {
        let y = r"
name: fixed-rate
mode:
  kind: fixed_rate
  msgs_per_sec: 20000
";
        let s: Scenario = serde_yaml::from_str(y).unwrap();
        assert!(matches!(
            s.mode,
            LoadMode::FixedRate {
                msgs_per_sec: 20000
            }
        ));
    }

    #[test]
    fn run_output_round_trips() {
        let scenario = Scenario {
            name: "x".into(),
            mode_tag: ModeTag::Ci,
            msg_size_bytes: 100,
            key_size_bytes: 0,
            partitions: 1,
            replication_factor: 1,
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
        };
        let out = RunOutput {
            scenario,
            stack: Stack::Crabka,
            topology: Topology {
                partitions: 1,
                replication_factor: 1,
                broker_count: 1,
            },
            wallclock_start_unix_ms: 0,
            wallclock_end_unix_ms: 1000,
            throughput: Throughput::default(),
            producer_latency_ms: LatencyPercentiles::default(),
            consumer_e2e_latency_ms: LatencyPercentiles::default(),
            resource: Resource::default(),
            disturbance: None,
            startup_ms: Some(1234),
            first_ack_ms: 42,
            errors: vec![],
            notes: vec!["test".into()],
        };
        let s = serde_json::to_string(&out).unwrap();
        let back: RunOutput = serde_json::from_str(&s).unwrap();
        assert_eq!(back.scenario.name, "x");
        assert_eq!(back.notes, vec!["test"]);
    }
}
