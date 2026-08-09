//! `Scenario` is the on-disk YAML schema the driver reads. `RunOutput` is
//! the on-disk JSON schema the driver writes. The report aggregator reads
//! `RunOutput` documents and writes Markdown.
//!
//! # Encoding of dimensioned fields
//!
//! Every size, extent, and rate here is a [`crabka_units`] quantity, and each
//! carries an explicit `#[serde(with = ...)]` so the file never holds a bare
//! base-unit float. The scenario an operator writes uses the human form
//! (`msg_size: 1KiB`, `linger: 5ms`, `rate: 20000/s`), which refuses a bare
//! number. A guess about whether `5` is seconds or milliseconds is the mistake
//! the types prevent. The measured output uses the exact integer form,
//! nanoseconds for latencies and bytes for sizes, so the numbers a report
//! compares or plots survive the round trip unrounded.
//!
//! Epoch timestamps (`wallclock_*_unix_ms`, `Disturbance::kill_at_ms`) stay raw
//! integers, because they are coordinates and not magnitudes.
//!
//! [`bounded`] also range-checks every input magnitude as the driver reads it, so
//! a scenario file that asks for something unrunnable fails at load and not at
//! the far end of the driver.

use crabka_units::{prelude::*, serde_units};
use serde::{Deserialize, Serialize};

use crate::ids::{MessageCount, TimeOffsetMs, WallclockMs};

/// `#[serde(with = ...)]` adapters that bound an operator-written magnitude.
///
/// The human forms accept a signed magnitude, so `-1B` reads as minus one byte.
/// The `usize`/`u64` fields they replaced could not be negative at all. Without a
/// check, a negative size deserializes cleanly and then saturates to zero deep
/// inside the driver. `payload::template` would emit header-sized records for a
/// scenario that asks for `-1B`, and the report would name the run after a
/// benchmark it did not perform. A bound on the read path turns that into a load
/// failure that names the field.
///
/// Whether zero is admissible is a per-field question, so each field picks the
/// adapter that matches it. A keyless record (`key_size: 0`) and an unbuffered
/// producer (`linger: 0`) are runnable. A zero-length measurement window or a
/// zero-byte record is not.
mod bounded {
    /// Defines a `#[serde(with = ...)]` module that reads a quantity's human form
    /// and then rejects a magnitude this field cannot run.
    ///
    /// Serialization delegates to the unbounded sibling, because a value that
    /// the read path admitted is in range on the way out by construction.
    macro_rules! bounded_module {
        (
            $(#[$meta:meta])*
            $name:ident, $quantity:ty, $encode:path, $decode:path,
            $admits:expr, $requirement:literal
        ) => {
            $(#[$meta])*
            pub mod $name {
                use crabka_units::{fmt::Human as _, prelude::*};
                use serde::{Deserializer, Serializer, de::Error as _};

                /// Writes the quantity as its human string form.
                ///
                /// # Errors
                ///
                /// Whatever the serializer reports for a string.
                pub fn serialize<S: Serializer>(
                    value: &$quantity,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    $encode(value, serializer)
                }

                /// Reads the quantity from its human string form and bounds it.
                ///
                /// # Errors
                ///
                /// If the value is not a quantity of this dimension carrying an
                /// explicit unit, or its magnitude is one this field cannot run.
                pub fn deserialize<'de, D: Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<$quantity, D::Error> {
                    let value = $decode(deserializer)?;
                    let admits: fn($quantity) -> bool = $admits;
                    if admits(value) {
                        return Ok(value);
                    }
                    Err(D::Error::custom(format!(
                        "must be {}, got {}",
                        $requirement,
                        value.human()
                    )))
                }
            }
        };
    }

    bounded_module!(
        /// A size that must be positive: a record or a producer batch.
        positive_size,
        ByteSize,
        crabka_units::serde_units::human::byte_size::serialize,
        crabka_units::serde_units::human::byte_size::deserialize,
        |value: ByteSize| value > ByteSize::ZERO,
        "a positive size"
    );

    bounded_module!(
        /// A size that may be zero: a key length, where zero means keyless.
        nonnegative_size,
        ByteSize,
        crabka_units::serde_units::human::byte_size::serialize,
        crabka_units::serde_units::human::byte_size::deserialize,
        |value: ByteSize| value >= ByteSize::ZERO,
        "a size of zero or more"
    );

    bounded_module!(
        /// An extent that must be positive: a measurement window.
        positive_time,
        Time,
        crabka_units::serde_units::human::time::serialize,
        crabka_units::serde_units::human::time::deserialize,
        |value: Time| value > Time::ZERO,
        "a positive extent"
    );

    bounded_module!(
        /// An extent that may be zero: a warmup that is skipped, a producer that
        /// does not linger, a kill scheduled at the very start of the run.
        nonnegative_time,
        Time,
        crabka_units::serde_units::human::time::serialize,
        crabka_units::serde_units::human::time::deserialize,
        |value: Time| value >= Time::ZERO,
        "an extent of zero or more"
    );

    bounded_module!(
        /// An event rate that must be positive. A paced producer that asks for
        /// no messages at all never sends one, and no scenario means that.
        positive_rate,
        Frequency,
        crabka_units::serde_units::human::frequency::serialize,
        crabka_units::serde_units::human::frequency::deserialize,
        |value: Frequency| value > Frequency::ZERO,
        "a positive rate"
    );
}

/// Which Kafka stack the scenario runs against. This is metadata only. The
/// driver's client behaviour is the same for both, because Crabka's
/// wire-compatible client speaks to either broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stack {
    Crabka,
    Kafka,
}

impl Stack {
    /// Pod-name regex that `prom.rs` uses to pick out the right brokers.
    /// Both regexes match the `StatefulSet` names that each operator
    /// creates with cluster name `demo`.
    #[must_use]
    pub fn broker_pod_regex(self) -> &'static str {
        match self {
            // Crabka StatefulSets are `<kafka>-<nodepool>` per the operator.
            // `^demo-broker` is a literal prefix common to both the e2e
            // single-pool naming (`demo-brokers-0`, pool `brokers`) and the
            // multi-pool bench topology (`demo-broker-0-0`, pools `broker-0/1/2`)
            // — `failover.rs` uses it via `starts_with`, `prom.rs` as a regex.
            Stack::Crabka => "^demo-broker",
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
    /// Producers are paced by a token bucket at exactly this rate, written as
    /// an event rate (`20000/s`).
    FixedRate {
        #[serde(with = "bounded::positive_rate")]
        rate: Frequency,
    },
}

/// Inject a broker kill mid-scenario to measure failover behaviour.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailoverSpec {
    /// How far into the scenario the driver deletes the target broker pod
    /// (`4s`). Zero kills at the start of warmup, which is extreme but runnable.
    #[serde(with = "bounded::nonnegative_time")]
    pub kill_after: Time,
    /// Which broker to kill. `partition0_leader` picks the broker that hosts
    /// partition 0's leader. `any_broker` picks the first matching pod. Only
    /// `partition0_leader` is wired today.
    #[serde(default = "default_failover_target")]
    pub target: String,
}

fn default_failover_target() -> String {
    "partition0_leader".to_string()
}

/// The scenario configuration. The driver loads it from YAML and
/// echoes it back into `RunOutput.scenario` for the report.
///
/// This struct rejects unknown keys. Almost every field has a default, so
/// without that rejection a stale or misspelled key is not an error. Serde would
/// ignore the key and substitute the default, and the driver would run a
/// *different* benchmark from the one the file describes and label the results
/// with the file's name. A scenario written as `msg_size_bytes: 102400` before
/// sizes carried units would have run at the 1 KiB default and been reported as
/// a 100 KiB benchmark.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    #[serde(default = "default_mode_tag")]
    pub mode_tag: ModeTag,
    /// Record value size on the wire (`1KiB`). A zero-byte record measures
    /// nothing, so this field rejects it and does not floor it at the payload
    /// header.
    #[serde(default = "default_msg_size", with = "bounded::positive_size")]
    pub msg_size: ByteSize,
    /// Record key size (`0` for keyless records).
    #[serde(default, with = "bounded::nonnegative_size")]
    pub key_size: ByteSize,
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
    /// How long the producer holds a partial batch before sending (`5ms`). Zero
    /// means send as soon as a record arrives.
    #[serde(default = "default_linger", with = "bounded::nonnegative_time")]
    pub linger: Time,
    /// Producer batch size (`16KiB`).
    #[serde(default = "default_batch_size", with = "bounded::positive_size")]
    pub batch_size: ByteSize,
    /// Length of the measurement window (`60s`). A zero-length window measures
    /// nothing, so this field rejects it.
    #[serde(default = "default_duration", with = "bounded::positive_time")]
    pub duration: Time,
    /// Length of the discarded warmup window before it (`10s`). Zero skips
    /// warmup and measures from the first record.
    #[serde(default = "default_warmup", with = "bounded::nonnegative_time")]
    pub warmup: Time,
    #[serde(default)]
    pub failover: Option<FailoverSpec>,
}

fn default_mode_tag() -> ModeTag {
    ModeTag::Ci
}
fn default_msg_size() -> ByteSize {
    kibibytes(1)
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
fn default_linger() -> Time {
    millis(5)
}
fn default_batch_size() -> ByteSize {
    kibibytes(16)
}
fn default_duration() -> Time {
    secs(60)
}
fn default_warmup() -> Time {
    secs(10)
}

// ── Output schema ───────────────────────────────────────────────────────────

/// Latency percentiles as measured extents, encoded as whole nanoseconds. The
/// driver records latencies at microsecond resolution, and the report renders
/// them in milliseconds to three decimal places. A millisecond integer would
/// round both away.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub p50: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub p95: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub p99: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub p999: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub max: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub mean: Time,
    pub count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Throughput {
    pub msgs_produced: MessageCount,
    pub msgs_consumed: MessageCount,
    /// Total record bytes produced over the measurement window.
    #[serde(with = "serde_units::numeric::bytes_u64")]
    pub bytes_in: ByteSize,
    /// Total record bytes consumed over the measurement window.
    #[serde(with = "serde_units::numeric::bytes_u64")]
    pub bytes_out: ByteSize,
    #[serde(with = "serde_units::human::frequency")]
    pub producer_rate: Frequency,
    #[serde(with = "serde_units::human::frequency")]
    pub consumer_rate: Frequency,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    /// CPU time burned by the broker pods over the measurement window.
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub broker_cpu: Time,
    #[serde(with = "serde_units::numeric::bytes_u64")]
    pub mem_cgroup_working_set: ByteSize,
    /// JVM heap used, summed across broker pods. Strimzi only.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_units::numeric::option_bytes_u64"
    )]
    pub jvm_heap_used: Option<ByteSize>,
    /// JVM non-heap used. Strimzi only.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_units::numeric::option_bytes_u64"
    )]
    pub jvm_nonheap_used: Option<ByteSize>,
    /// The cgroup working set minus JVM heap and non-heap. This is an
    /// approximation of the page-cache footprint on the broker pod. Strimzi
    /// only.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_units::numeric::option_bytes_i64"
    )]
    pub kafka_page_cache_approx: Option<ByteSize>,
    /// Messages produced per second of broker CPU time.
    #[serde(with = "serde_units::human::frequency")]
    pub msgs_per_cpu_second: Frequency,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Disturbance {
    /// Unix-epoch millisecond at which the driver deleted the broker pod. This
    /// is an instant, so it stays a raw stamp.
    pub kill_at_ms: TimeOffsetMs,
    /// Unix-epoch millisecond of the first ack after the kill.
    pub recovery_at_ms: TimeOffsetMs,
    pub dropped: MessageCount,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub latency_spike_max: Time,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    pub partitions: i32,
    pub replication_factor: i16,
    pub broker_count: u32,
}

/// One time-series sample of client-side throughput and latency over a fixed
/// interval of the measurement window. The default interval is 2s. This lets the
/// report graph values *over the test* and not only end-of-run aggregates. The
/// latency percentiles cover this window only and are not cumulative, so a
/// latency-vs-time curve shows real movement.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Milliseconds since the measurement window started.
    pub t_offset_ms: TimeOffsetMs,
    #[serde(with = "serde_units::human::frequency")]
    pub producer_rate: Frequency,
    #[serde(with = "serde_units::human::frequency")]
    pub consumer_rate: Frequency,
    /// Interval producer-ack latency (this window only).
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub producer_p50: Time,
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub producer_p99: Time,
    /// Interval consumer end-to-end p99 latency (this window only).
    #[serde(with = "serde_units::numeric::nanos_i64")]
    pub consumer_e2e_p99: Time,
}

/// One time-series sample of broker resource usage, scraped from Prometheus
/// as a range query over the run window. The default step is 15s, which is the
/// scrape interval. This covers the full wallclock window, warmup and
/// measurement.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BrokerSample {
    /// Milliseconds since `wallclock_start_unix_ms`.
    pub t_offset_ms: TimeOffsetMs,
    /// Summed CPU usage across broker pods, in cores. A core count is
    /// dimensionless, so it stays a plain number.
    pub cpu_cores: f64,
    /// Summed working-set memory across broker pods.
    #[serde(with = "serde_units::numeric::bytes_u64")]
    pub mem_working_set: ByteSize,
}

/// One run = one scenario × one stack. The driver writes it and the report
/// aggregator reads it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunOutput {
    pub scenario: Scenario,
    pub stack: Stack,
    pub topology: Topology,
    pub wallclock_start_unix_ms: WallclockMs,
    pub wallclock_end_unix_ms: WallclockMs,
    pub throughput: Throughput,
    pub producer_latency: LatencyPercentiles,
    pub consumer_e2e_latency: LatencyPercentiles,
    pub resource: Resource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disturbance: Option<Disturbance>,
    /// Operator and broker startup wall-clock from CR apply to broker Ready, in
    /// whole milliseconds so a shell script can write the field.
    /// `run-scenario.sh` fills it in after the driver finishes.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_units::numeric::option_millis_i64"
    )]
    pub startup: Option<Time>,
    /// Driver-observed wall-clock from start to first successful
    /// `send().await.await??`.
    #[serde(with = "serde_units::numeric::millis_i64")]
    pub first_ack: Time,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub notes: Vec<String>,
    /// Per-interval client throughput and latency over the measurement window.
    /// This is empty for runs made before time-series sampling existed.
    #[serde(default)]
    pub samples: Vec<Sample>,
    /// Per-interval broker CPU and memory over the run window, from a Prometheus
    /// range query.
    #[serde(default)]
    pub broker_samples: Vec<BrokerSample>,
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A scenario with every dimensioned field set to a distinctive value, so a
    /// round trip that drops or rescales one is visible.
    fn scenario() -> Scenario {
        Scenario {
            name: "round-trip".into(),
            mode_tag: ModeTag::Cluster,
            msg_size: kibibytes(4),
            key_size: bytes(16),
            partitions: 12,
            replication_factor: 3,
            producers: 2,
            consumers: 4,
            mode: LoadMode::FixedRate {
                rate: per_sec(20_000),
            },
            acks: Acks::All,
            compression: Compression::Zstd,
            linger: millis(5),
            batch_size: kibibytes(16),
            duration: secs(60),
            warmup: secs(10),
            failover: Some(FailoverSpec {
                kill_after: secs(4),
                target: "partition0_leader".into(),
            }),
        }
    }

    fn run_output() -> RunOutput {
        RunOutput {
            scenario: scenario(),
            stack: Stack::Crabka,
            topology: Topology {
                partitions: 12,
                replication_factor: 3,
                broker_count: 3,
            },
            wallclock_start_unix_ms: WallclockMs(1_700_000_000_000),
            wallclock_end_unix_ms: WallclockMs(1_700_000_060_000),
            throughput: Throughput {
                msgs_produced: MessageCount(600_000),
                msgs_consumed: MessageCount(599_000),
                bytes_in: mebibytes(2400),
                bytes_out: mebibytes(2396),
                producer_rate: per_sec(10_000),
                consumer_rate: per_sec(9_983),
            },
            producer_latency: LatencyPercentiles {
                p50: micros(1500),
                p95: micros(3200),
                p99: micros(4250),
                p999: millis(9),
                max: millis(42),
                mean: micros(1800),
                count: 600_000,
            },
            consumer_e2e_latency: LatencyPercentiles::default(),
            resource: Resource {
                broker_cpu: millis(123_456),
                mem_cgroup_working_set: mebibytes(300),
                jvm_heap_used: Some(mebibytes(1024)),
                jvm_nonheap_used: Some(mebibytes(96)),
                kafka_page_cache_approx: Some(mebibytes(200)),
                msgs_per_cpu_second: per_sec(4_860),
            },
            disturbance: Some(Disturbance {
                kill_at_ms: TimeOffsetMs(1_700_000_004_000),
                recovery_at_ms: TimeOffsetMs(1_700_000_006_000),
                dropped: MessageCount(7),
                latency_spike_max: micros(42_500),
            }),
            startup: Some(millis(1234)),
            first_ack: millis(42),
            errors: vec!["an-error".into()],
            notes: vec!["a-note".into()],
            samples: vec![Sample {
                t_offset_ms: TimeOffsetMs(2000),
                producer_rate: per_sec(10_100),
                consumer_rate: per_sec(9_900),
                producer_p50: micros(1500),
                producer_p99: micros(4200),
                consumer_e2e_p99: millis(7),
            }],
            broker_samples: vec![BrokerSample {
                t_offset_ms: TimeOffsetMs(0),
                cpu_cores: 2.5,
                mem_working_set: mebibytes(300),
            }],
        }
    }

    #[test]
    fn stack_broker_pod_regex_distinguishes_stacks() {
        assert2::assert!(Stack::Crabka.broker_pod_regex() == "^demo-broker");
        assert2::assert!(Stack::Kafka.broker_pod_regex() == "^demo-kafka-");
        // The crabka prefix must match BOTH the single-pool e2e naming and the
        // multi-pool bench naming (used by failover.rs `starts_with`).
        let p = Stack::Crabka.broker_pod_regex().trim_start_matches('^');
        assert2::assert!("demo-brokers-0".starts_with(p));
        assert2::assert!("demo-broker-0-0".starts_with(p));
    }

    #[test]
    fn acks_map_to_producer_enum() {
        use crabka_client_producer::Acks as P;
        for (acks, want) in [
            (Acks::None, P::Zero),
            (Acks::Leader, P::One),
            (Acks::All, P::All),
        ] {
            assert2::assert!(acks.into_producer() == want);
        }
    }

    #[test]
    fn compression_maps_to_producer_enum() {
        use crabka_client_producer::Compression as PC;
        for (compression, want) in [
            (Compression::None, PC::None),
            (Compression::Gzip, PC::Gzip),
            (Compression::Snappy, PC::Snappy),
            (Compression::Lz4, PC::Lz4),
            (Compression::Zstd, PC::Zstd),
        ] {
            assert2::assert!(compression.into_producer() == want);
        }
    }

    #[test]
    fn compression_default_is_none() {
        assert2::assert!(Compression::default() == Compression::None);
    }

    #[test]
    fn scenario_yaml_reads_the_operator_form() {
        let y = r"
name: small-msg-saturate
mode_tag: ci
msg_size: 100B
partitions: 6
replication_factor: 1
producers: 1
consumers: 1
mode:
  kind: saturate
acks: leader
compression: none
linger: 5ms
batch_size: 16KiB
duration: 60s
warmup: 10s
";
        let s: Scenario = serde_yaml::from_str(y).expect("parse");
        check!(s.name.as_str() == "small-msg-saturate");
        check!(s.partitions == 6);
        check!(s.msg_size == bytes(100));
        check!(s.key_size == ByteSize::ZERO);
        check!(s.linger == millis(5));
        check!(s.batch_size == kibibytes(16));
        check!(s.duration == secs(60));
        check!(s.warmup == secs(10));
        check!(matches!(s.mode, LoadMode::Saturate));
    }

    #[test]
    fn scenario_yaml_defaults_every_omitted_field() {
        let y = r"
name: bare
mode:
  kind: saturate
";
        let s: Scenario = serde_yaml::from_str(y).expect("parse");
        check!(s.msg_size == kibibytes(1));
        check!(s.key_size == ByteSize::ZERO);
        check!(s.linger == millis(5));
        check!(s.batch_size == kibibytes(16));
        check!(s.duration == secs(60));
        check!(s.warmup == secs(10));
        check!(s.failover == None);
    }

    #[test]
    fn scenario_yaml_rejects_a_size_without_a_unit() {
        let y = r"
name: unitless
msg_size: 100
mode:
  kind: saturate
";
        let error = serde_yaml::from_str::<Scenario>(y).expect_err("a bare number is not a size");
        check!(error.to_string().contains("missing unit"));
    }

    /// A minimal `saturate` scenario with `extra` appended, so one field's bound
    /// can be exercised against an otherwise-runnable file.
    fn parse_saturate_with(extra: &str) -> Result<Scenario, serde_yaml::Error> {
        serde_yaml::from_str(&format!("name: bounds\nmode:\n  kind: saturate\n{extra}\n"))
    }

    #[test]
    fn scenario_yaml_rejects_unrunnable_magnitudes() {
        // The human forms accept a sign, so every field that used to be an
        // unsigned primitive has to say so itself.
        for (field, requirement) in [
            ("msg_size: -1B", "a positive size"),
            ("msg_size: 0", "a positive size"),
            ("key_size: -1B", "a size of zero or more"),
            ("batch_size: -16KiB", "a positive size"),
            ("batch_size: 0", "a positive size"),
            ("linger: -5ms", "an extent of zero or more"),
            ("duration: -60s", "a positive extent"),
            ("duration: 0", "a positive extent"),
            ("warmup: -10s", "an extent of zero or more"),
            ("failover:\n  kill_after: -4s", "an extent of zero or more"),
        ] {
            let error = parse_saturate_with(field)
                .expect_err("an unrunnable magnitude must fail at load, not at run");
            check!(error.to_string().contains(requirement), "{field:?}");
        }
    }

    #[test]
    fn scenario_yaml_rejects_an_unrunnable_fixed_rate() {
        // Quoted because `LoadMode` is internally tagged: serde buffers the
        // variant's fields before handing them on, and a buffered YAML `0` is an
        // integer rather than the scalar text the human form reads.
        for rate in [r#""-1/s""#, r#""0""#] {
            let yaml = format!("name: bounds\nmode:\n  kind: fixed_rate\n  rate: {rate}\n");
            let error = serde_yaml::from_str::<Scenario>(&yaml)
                .expect_err("a paced producer needs a positive rate");
            check!(error.to_string().contains("a positive rate"), "{rate}");
        }
    }

    #[test]
    fn scenario_yaml_admits_zero_where_zero_is_runnable() {
        let s =
            parse_saturate_with("key_size: 0\nlinger: 0\nwarmup: 0\nfailover:\n  kill_after: 0")
                .expect("keyless records, no linger, no warmup, and an immediate kill all run");
        check!(s.key_size == ByteSize::ZERO);
        check!(s.linger == Time::ZERO);
        check!(s.warmup == Time::ZERO);
        check!(s.failover.map(|failover| failover.kill_after) == Some(Time::ZERO));
    }

    #[test]
    fn fixed_rate_yaml_parses_an_event_rate() {
        let y = r"
name: fixed-rate
mode:
  kind: fixed_rate
  rate: 20000/s
";
        let s: Scenario = serde_yaml::from_str(y).unwrap();
        check!(
            s.mode
                == LoadMode::FixedRate {
                    rate: per_sec(20_000)
                }
        );
    }

    #[test]
    fn failover_yaml_parses_a_kill_offset() {
        let y = r"
name: failover
mode:
  kind: saturate
failover:
  kill_after: 4s
";
        let s: Scenario = serde_yaml::from_str(y).unwrap();
        check!(
            s.failover
                == Some(FailoverSpec {
                    kill_after: secs(4),
                    target: "partition0_leader".into(),
                })
        );
    }

    #[test]
    fn scenario_yaml_round_trips_through_the_human_form() {
        let yaml = serde_yaml::to_string(&scenario()).expect("encode");
        // Operator-facing fields carry their unit rather than a bare float.
        for needle in ["msg_size: 4KiB", "linger: 5ms", "rate: 20000/s"] {
            check!(yaml.contains(needle));
        }
        let back: Scenario = serde_yaml::from_str(&yaml).expect("decode");
        check!(back == scenario());
    }

    #[test]
    fn run_output_json_round_trips() {
        let out = run_output();
        let json = serde_json::to_string(&out).expect("encode");
        let back: RunOutput = serde_json::from_str(&json).expect("decode");
        check!(back == out);
    }

    #[test]
    fn run_output_json_encodes_measurements_as_exact_integers() {
        let json = serde_json::to_value(run_output()).expect("encode");
        check!(json["producer_latency"]["p99"] == serde_json::json!(4_250_000));
        check!(json["throughput"]["bytes_in"] == serde_json::json!(2_516_582_400_u64));
        check!(json["throughput"]["producer_rate"] == serde_json::json!("10000/s"));
        check!(json["resource"]["mem_cgroup_working_set"] == serde_json::json!(314_572_800));
        check!(json["first_ack"] == serde_json::json!(42));
        check!(json["startup"] == serde_json::json!(1234));
    }

    #[test]
    fn absent_optional_resource_fields_decode_as_none() {
        let mut out = run_output();
        out.resource.jvm_heap_used = None;
        out.resource.jvm_nonheap_used = None;
        out.resource.kafka_page_cache_approx = None;
        out.startup = None;
        let json = serde_json::to_string(&out).expect("encode");
        check!(!json.contains("jvm_heap_used"));
        check!(!json.contains("startup"));
        let back: RunOutput = serde_json::from_str(&json).expect("decode");
        check!(back == out);
    }
}
