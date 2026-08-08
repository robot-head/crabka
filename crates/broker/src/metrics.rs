//! Broker-side Prometheus metrics.
//!
//! Mirrors the operator's `telemetry` / `health` pattern: a shared
//! `Registry` is wrapped in `Arc<Mutex<…>>` so hot-path counters can
//! be looked up without holding the registry lock. The
//! [`BrokerMetrics`] struct hands out cheap `Arc<Counter>` / `Arc<Gauge>`
//! handles that handlers and background tasks clone and increment
//! directly.
//!
//! Naming follows Prometheus convention: `crabka_broker_<subject>_<unit>`.
//! Where Kafka has a canonical JMX name, we keep the metric semantics
//! close to it (e.g. `BrokerTopicMetrics:BytesInPerSec` ↔
//! `crabka_broker_topic_bytes_in_total`), but the units convert from
//! per-second gauges to monotonic counters per Prometheus best practice
//! — operators compute rates with `rate()` at scrape time.

use std::sync::Arc;

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

/// Latency buckets (seconds) for the per-API `request_duration_seconds`
/// histogram. Spans ~100µs (idempotent `ApiVersions`) to 10s (a slow
/// controller round-trip or a throttled admin RPC), tuned so the common
/// Produce/Fetch band (0.5ms–50ms) lands on distinct buckets.
const REQUEST_DURATION_BUCKETS: [f64; 12] = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 10.0,
];

/// Shared registry owning every metric the broker emits. Wrapped in
/// `Arc<Mutex<…>>` because `prometheus-client` requires `&mut Registry`
/// to register and we want lazy registration from multiple init paths.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Sentinel label value that folds unbounded inputs (unrecognised
/// `api_key`s, `SaslAuthenticate` without a prior handshake) into one
/// series, keeping label cardinality bounded.
pub(crate) const UNKNOWN_LABEL: &str = "Unknown";

/// Per-topic label set. `EncodeLabelSet` is the prometheus-client
/// derive that produces the `topic="<name>"` label on emitted samples.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicLabel {
    pub topic: String,
}

/// Per-partition label set, paired with the new `partition_*` metric
/// families. Consumed by the rebalancer's metric scraper.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
    pub topic: String,
    pub partition: i32,
}

/// Fleet-complete KIP-932 backlog for one share-group partition.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShareGroupLabel {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}

/// KIP-511 client software fingerprint, attached to the
/// `client_software_versions_total` counter on every accepted v3+
/// `ApiVersions` handshake.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ClientSoftwareLabel {
    pub software_name: String,
    pub software_version: String,
}

/// Per-Kafka-API request fingerprint, paired with the
/// `api_requests` counter family. `api_key` is the
/// `ApiKey::IntoStaticStr`-derived variant name (e.g. `"Produce"`,
/// `"DescribeQuorum"`) so operators see human-readable api-name
/// labels. Cardinality is bounded by `ApiKey::ALL.len()` (~80
/// entries); requests for unknown api keys land under the
/// `"Unknown"` sentinel label.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ApiKeyLabel {
    pub api_key: String,
}

/// SASL mechanism fingerprint, paired with the
/// `{successful,failed}_authentication_total` counter families.
/// `mechanism` is the canonical Kafka wire name from
/// [`crabka_security::SaslMechanism::wire_name`] (`"PLAIN"`,
/// `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, `"OAUTHBEARER"`) when the
/// `SaslAuthenticate` frame arrived in a valid sequence; the
/// `"Unknown"` sentinel covers `ILLEGAL_SASL_STATE` rejects where
/// no prior `SaslHandshake` ran and the mechanism is unset.
/// Cardinality is bounded by `SaslMechanism::*` + 1 — a tight set
/// regardless of client population.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SaslMechanismLabel {
    pub mechanism: String,
}

/// Cheaply-clonable bundle of counter / gauge handles. Construct once
/// in `Broker::start`; hand out clones (each clone is a single
/// `Arc::clone`) to every subsystem that emits.
#[derive(Clone)]
pub struct BrokerMetrics {
    pub registry: SharedRegistry,
    pub topic_bytes_in: Family<TopicLabel, Counter>,
    pub topic_bytes_out: Family<TopicLabel, Counter>,
    /// Cumulative count of records received from producers,
    /// per topic. Sums `RecordBatch.records.len()` for every batch on
    /// the Produce path. Mirrors Kafka's
    /// `BrokerTopicMetrics.MessagesInPerSec`; pairs with
    /// `topic_bytes_in` to surface both volume and message rate.
    /// Legacy (v0/v1) producers don't contribute — `RecordsPayload`
    /// keeps their bytes opaque until the v2 conversion, so we
    /// count there. The accompanying `produce_message_conversions`
    /// counter still tracks how often legacy batches arrive, so
    /// operators can detect under-counting from a legacy fleet.
    pub topic_messages_in: Family<TopicLabel, Counter>,
    pub topic_produce_requests: Family<TopicLabel, Counter>,
    pub topic_fetch_requests: Family<TopicLabel, Counter>,
    /// Per-topic counter of Produce partition responses
    /// that carried a non-zero error code. Mirrors Kafka's
    /// `BrokerTopicMetrics.FailedProduceRequestsPerSec`. Incremented
    /// once per failed partition (matching the JVM's per-row mark),
    /// so a request whose two partitions both fail bumps the topic
    /// counter by 2. Topic-level authorization denials and
    /// unknown-topic responses count, mirroring JVM behavior.
    pub topic_failed_produce_requests: Family<TopicLabel, Counter>,
    /// Per-topic counter of Fetch partition responses
    /// that carried a non-zero error code. Mirrors Kafka's
    /// `BrokerTopicMetrics.FailedFetchRequestsPerSec`. Pairs with
    /// `topic_fetch_requests` to surface error rate.
    pub topic_failed_fetch_requests: Family<TopicLabel, Counter>,
    pub partition_bytes_in: Family<PartitionLabel, Counter>,
    pub partition_bytes_out: Family<PartitionLabel, Counter>,
    /// Cumulative bytes this broker accepted from a partition
    /// leader as a follower (`Fetch(replica_id >= 0)` round-trip). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesInPerSec`. Operators
    /// graph `rate(replication_bytes_in_total[1m])` to spot ISR fall-behind
    /// caused by ingest, not by client read load.
    pub replication_bytes_in: Family<PartitionLabel, Counter>,
    /// Cumulative bytes this broker served *to* a follower
    /// (i.e. the leader-side outbound for inter-broker `Fetch`). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesOutPerSec`. Operators
    /// graph the per-partition rate to attribute leader outbound to
    /// followers vs. consumers (the latter still rolls up to
    /// `partition_bytes_out`).
    pub replication_bytes_out: Family<PartitionLabel, Counter>,
    pub partition_disk_bytes: Family<PartitionLabel, Gauge>,
    /// Records waiting for acquisition in each share-group partition.
    pub share_group_backlog: Family<ShareGroupLabel, Gauge>,
    /// Cumulative handler-thread microseconds spent processing each
    /// (topic, partition). Exported as
    /// `crabka_broker_partition_cpu_micros_total`. Rebalancer takes
    /// `rate(...)` to get micros/sec; dividing by `1_000_000` yields the
    /// per-partition core occupancy. We track microseconds (integer
    /// counter) rather than seconds (float) because `prometheus-client`
    /// counters are `u64`.
    pub partition_cpu_micros: Family<PartitionLabel, Counter>,
    pub partitions_led: Gauge,
    /// Total number of partitions (leader + follower
    /// replicas) this broker hosts. Mirrors Kafka's
    /// `ReplicaManager.PartitionCount`. Sampled in the same per-second
    /// tick as `partitions_led`.
    pub partitions_total: Gauge,
    /// Count of partitions this broker leads whose ISR is
    /// smaller than the assigned replica set — Kafka's
    /// `ReplicaManager.UnderReplicatedPartitions`. Sampled by reading
    /// the current `MetadataImage` and matching partitions where this
    /// broker is the leader. Operators alert on
    /// `under_replicated_partitions > 0` to spot stuck followers
    /// before they fail an unclean election.
    pub under_replicated_partitions: Gauge,
    /// Count of partitions this broker leads whose ISR is
    /// strictly less than the topic's `min.insync.replicas`. Mirrors
    /// Kafka's `ReplicaManager.UnderMinIsrPartitionCount`. Operators
    /// alert on `under_min_isr_partition_count > 0`: partitions in
    /// this state reject `acks=all` produces with
    /// `NOT_ENOUGH_REPLICAS`, so the metric
    /// surfaces "writes are blocked" before clients start retrying.
    pub under_min_isr_partition_count: Gauge,
    /// Count of partitions this broker leads that
    /// currently have no live leader (leader broker dead with no
    /// eligible ISR replacement). Mirrors Kafka's
    /// `ReplicaManager.OfflinePartitionsCount`. Operators alert on
    /// `> 0`: such partitions are wholly unavailable until an ISR
    /// member returns or an unclean election runs.
    pub offline_partitions_count: Gauge,
    pub active_controller: Gauge,
    /// Cumulative count of distinct controller-leader
    /// transitions this broker has observed (any change in the raft
    /// leader, including this broker becoming or ceasing to be
    /// leader). Mirrors Kafka's
    /// `KafkaController.LeaderElectionRateAndTimeMs`. Operators alert
    /// on `rate(controller_leader_changes_total[5m]) > 0` for sustained
    /// periods to spot flapping raft leadership.
    pub controller_leader_changes_total: Counter,
    pub isr_shrinks_total: Counter,
    pub isr_expands_total: Counter,
    /// KIP-227: current count of live incremental-fetch sessions across the
    /// per-broker cache. Sampled periodically from `FetchSessionCache::len()`.
    pub incremental_fetch_sessions: Gauge,
    /// KIP-227: cumulative count of incremental-fetch sessions evicted to
    /// make room for a new allocation. Incremented inside the cache.
    pub incremental_fetch_session_evictions_total: Counter,
    /// KIP-227: sum of `session.partitions.len()` across every live session.
    /// Sampled periodically alongside `incremental_fetch_sessions`.
    pub incremental_fetch_partitions_cached: Gauge,
    /// KIP-511: per-(name, version) counter of accepted v3+ `ApiVersions`
    /// handshakes. Operators graph this to see which client libraries
    /// and versions are connecting.
    pub client_software_versions: Family<ClientSoftwareLabel, Counter>,
    /// Cumulative count of completed `SaslAuthenticate`
    /// frames per mechanism that ended in a successful auth state
    /// transition. Mirrors Kafka's
    /// `kafka.network:type=Selector,name=successful-authentication-total`.
    /// Paired with `failed_authentication` so operators compute the
    /// auth failure ratio per mechanism at scrape time.
    pub successful_authentication: Family<SaslMechanismLabel, Counter>,
    /// Cumulative count of `SaslAuthenticate` frames per
    /// mechanism that returned a non-zero error code. Mirrors
    /// Kafka's `failed-authentication-total`. The `"Unknown"`
    /// mechanism label covers `ILLEGAL_SASL_STATE` rejects where
    /// the connection sent `SaslAuthenticate` without first
    /// completing a `SaslHandshake`; per-mechanism failures land
    /// under the canonical wire name (`PLAIN`, `SCRAM-SHA-256`,
    /// `SCRAM-SHA-512`, `OAUTHBEARER`).
    pub failed_authentication: Family<SaslMechanismLabel, Counter>,
    /// Per-Kafka-API request counter. Bumped once per
    /// dispatched request from the network dispatcher, labelled by
    /// the `ApiKey` variant name (or `"Unknown"` for unrecognised
    /// keys). Mirrors Kafka's `RequestMetrics.RequestsPerSec`; rate(...)
    /// gives operators per-API request throughput across the
    /// broker without needing to slice the dashboard by handler.
    pub api_requests: Family<ApiKeyLabel, Counter>,
    /// Per-Kafka-API counter of requests the dispatcher
    /// answered with the synthetic `UNSUPPORTED_VERSION` response
    /// because no handler matched the `api_key` (or, for unknown
    /// `api_key`s, the dispatcher didn't recognise the key at all).
    /// Operators alert on `rate(unsupported_api_requests_total[5m]) > 0`
    /// to catch clients on `api_key`/version pairs the broker
    /// doesn't speak — frequently the smoking gun for upgrade-skew
    /// or misconfigured clients.
    pub unsupported_api_requests: Family<ApiKeyLabel, Counter>,
    /// Per-Kafka-API request-handling latency in seconds
    /// (`crabka_broker_request_duration_seconds{api}`). Observed in the
    /// dispatch path around the full handler round-trip (decode → handle →
    /// encode) for every dispatched frame, labelled by the `ApiKey`
    /// variant name. Operators graph
    /// `histogram_quantile(0.99, rate(request_duration_seconds_bucket[5m]))`
    /// per api to spot handler tail-latency regressions, and use `_count`
    /// as a request-rate stream that pairs with `api_requests`.
    pub request_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Number of requests currently being handled by this
    /// broker (gauge). Incremented on dispatch entry, decremented on exit
    /// (including the error/close path). A sustained climb signals handler
    /// stalls or a wedged downstream (controller / replication).
    pub in_flight_requests: Gauge,
    /// Number of client connections currently open to this
    /// broker (gauge). Incremented when a connection is accepted and the
    /// per-connection serve loop starts, decremented when that loop exits
    /// (EOF, error, or SASL-session expiry). Mirrors Kafka's
    /// `kafka.network:type=Acceptor` connection-count intent.
    pub active_connections: Gauge,
    /// Per-Kafka-API counter of requests whose handler
    /// returned an error (the dispatcher closed the connection). Labelled
    /// by the `ApiKey` variant name; disjoint from
    /// `unsupported_api_requests` (which counts the synthetic
    /// `UNSUPPORTED_VERSION` arm). Operators alert on
    /// `rate(request_errors_total[5m]) > 0` to catch handler-level faults.
    pub request_errors: Family<ApiKeyLabel, Counter>,
    /// KIP-405: `1` when this broker has finished swapping in
    /// the topic-backed `RemoteLogMetadataManager` and is
    /// answering metadata queries from the durable
    /// `__remote_log_metadata` topic; `0` while still on the
    /// fail-closed `NotReadyRlmm` placeholder (the default until a
    /// configured `[remote_storage.kafka_metadata]` bootstrap completes).
    /// Operators alert on
    /// `min_over_time(tiered_storage_rlmm_topic_backed[5m]) == 0`
    /// against clusters that asked for `metadataManager: Topic` to catch
    /// a stuck bootstrap.
    pub tiered_storage_rlmm_topic_backed: Gauge,
    /// Number of topic-backed RLMM bootstrap attempts; climbs while stuck
    /// retrying, flat once `tiered_storage_rlmm_topic_backed` flips to 1.
    pub tiered_storage_rlmm_bootstrap_attempts: Counter,
    /// Per-topic counter of v0/v1 → v2 record-batch
    /// up-conversions on the Produce path. Mirrors Kafka's
    /// `BrokerTopicMetrics.ProduceMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Produce request whose
    /// `records` field arrived as a legacy `MessageSet`.
    pub produce_message_conversions: Family<TopicLabel, Counter>,
    /// Per-topic counter of v2 → v0/v1 record-batch
    /// down-conversions on the Fetch path. Mirrors Kafka's
    /// `BrokerTopicMetrics.FetchMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Fetch response whose response
    /// payload was down-converted to satisfy a legacy (`Fetch v < 4`)
    /// client.
    pub fetch_message_conversions: Family<TopicLabel, Counter>,
    /// KIP-841: cumulative count of unclean leader
    /// elections this broker, as controller leader, has driven —
    /// i.e. elections that picked an out-of-ISR replica as the new
    /// leader because the topic had
    /// `unclean.leader.election.enable=true` and the ISR was empty
    /// at failover time. Mirrors Kafka's
    /// `ControllerStats.UncleanLeaderElectionsPerSec`. An operator
    /// alert on `rate(unclean_leader_elections_total[5m]) > 0`
    /// flags the data-loss footgun.
    pub unclean_leader_elections_total: Counter,
    /// `FedRAMP` MLA: cumulative audit records successfully written to the
    /// audit topic. Incremented by the audit subsystem on each successful
    /// produce to `__crabka_audit`.
    pub audit_events_total: Counter,
    /// `FedRAMP` MLA: cumulative audit records that failed to write to the
    /// audit topic. Incremented on each produce error; operators alert on
    /// `rate(audit_write_failures_total[5m]) > 0`.
    pub audit_write_failures_total: Counter,
    /// Current count of audit records buffered in the durable spool (gauge).
    pub audit_spool_depth: Gauge,
    /// Current bytes buffered in the durable audit spool (gauge).
    pub audit_spool_bytes: Gauge,
    /// Cumulative audit records diverted to the spool on topic-write failure.
    pub audit_records_spooled_total: Counter,
    /// Cumulative audit records drained from the spool back to the topic.
    pub audit_records_replayed_total: Counter,
    /// Cumulative audit records lost (channel-full or spool-full).
    pub audit_records_dropped_total: Counter,
    /// Cumulative count of completed log-compaction sweeps run by this
    /// broker's cleaner — one increment per `tick_all` pass, whether or
    /// not any partition was eligible. Lets tests (and operators) observe
    /// that the compaction ticker has completed at least one full pass
    /// after a segment was sealed, replacing fixed `sleep`s with a poll on
    /// this counter. Mirrors the intent of Kafka's `LogCleaner` run
    /// accounting.
    pub log_cleaner_runs_total: Counter,
    /// Per-partition cumulative count of compaction passes
    /// (`Partition::compact_log`) this broker's cleaner completed
    /// successfully. Bumped once per eligible (leader &&
    /// `cleanup.policy=compact`) partition per sweep. Pairs with
    /// `log_cleaner_runs_total`: a test that seals a segment then waits for
    /// this counter to advance knows the sealed segment has been through a
    /// compaction pass without guessing a duration.
    pub log_compactions_total: Family<PartitionLabel, Counter>,
}

impl BrokerMetrics {
    fn unregistered(registry: SharedRegistry) -> Self {
        Self {
            registry,
            topic_bytes_in: Family::default(),
            topic_bytes_out: Family::default(),
            topic_messages_in: Family::default(),
            topic_produce_requests: Family::default(),
            topic_fetch_requests: Family::default(),
            topic_failed_produce_requests: Family::default(),
            topic_failed_fetch_requests: Family::default(),
            partition_bytes_in: Family::default(),
            partition_bytes_out: Family::default(),
            replication_bytes_in: Family::default(),
            replication_bytes_out: Family::default(),
            partition_disk_bytes: Family::default(),
            share_group_backlog: Family::default(),
            partition_cpu_micros: Family::default(),
            partitions_led: Gauge::default(),
            partitions_total: Gauge::default(),
            under_replicated_partitions: Gauge::default(),
            under_min_isr_partition_count: Gauge::default(),
            offline_partitions_count: Gauge::default(),
            active_controller: Gauge::default(),
            controller_leader_changes_total: Counter::default(),
            isr_shrinks_total: Counter::default(),
            isr_expands_total: Counter::default(),
            incremental_fetch_sessions: Gauge::default(),
            incremental_fetch_session_evictions_total: Counter::default(),
            incremental_fetch_partitions_cached: Gauge::default(),
            client_software_versions: Family::default(),
            successful_authentication: Family::default(),
            failed_authentication: Family::default(),
            api_requests: Family::default(),
            unsupported_api_requests: Family::default(),
            request_duration_seconds: Family::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS)
            }),
            in_flight_requests: Gauge::default(),
            active_connections: Gauge::default(),
            request_errors: Family::default(),
            tiered_storage_rlmm_topic_backed: Gauge::default(),
            tiered_storage_rlmm_bootstrap_attempts: Counter::default(),
            produce_message_conversions: Family::default(),
            fetch_message_conversions: Family::default(),
            unclean_leader_elections_total: Counter::default(),
            audit_events_total: Counter::default(),
            audit_write_failures_total: Counter::default(),
            audit_spool_depth: Gauge::default(),
            audit_spool_bytes: Gauge::default(),
            audit_records_spooled_total: Counter::default(),
            audit_records_replayed_total: Counter::default(),
            audit_records_dropped_total: Counter::default(),
            log_cleaner_runs_total: Counter::default(),
            log_compactions_total: Family::default(),
        }
    }

    fn register_group_1(&self, registry: &mut Registry) {
        registry.register(
            "topic_bytes_in",
            "Bytes received from producers, per topic (cumulative). \
             Operators compute throughput via rate(...).",
            self.topic_bytes_in.clone(),
        );

        registry.register(
            "topic_bytes_out",
            "Bytes delivered to fetchers, per topic (cumulative).",
            self.topic_bytes_out.clone(),
        );

        registry.register(
            "messages_in",
            "Cumulative count of records received from \
             producers, per topic. Mirrors Kafka's \
             BrokerTopicMetrics.MessagesInPerSec. Legacy v0/v1 \
             produce payloads are not counted (their per-record body \
             stays opaque on the Produce path); the paired \
             produce_message_conversions counter tracks the \
             legacy-arrival rate so operators can detect \
             under-counting.",
            self.topic_messages_in.clone(),
        );

        registry.register(
            "topic_produce_requests",
            "Produce requests handled, per topic (cumulative). One \
             increment per topic per Produce request.",
            self.topic_produce_requests.clone(),
        );

        registry.register(
            "topic_fetch_requests",
            "Fetch requests handled, per topic (cumulative). One \
             increment per topic per Fetch request.",
            self.topic_fetch_requests.clone(),
        );

        registry.register(
            "topic_failed_produce_requests",
            "Cumulative count of Produce partition \
             responses that returned a non-zero error code, per \
             topic. Mirrors Kafka's \
             BrokerTopicMetrics.FailedProduceRequestsPerSec. \
             Operators alert on rate(...) > 0 to catch quota / ACL \
             / NOT_ENOUGH_REPLICAS storms; the ratio against \
             topic_produce_requests yields the per-topic error rate.",
            self.topic_failed_produce_requests.clone(),
        );

        registry.register(
            "topic_failed_fetch_requests",
            "Cumulative count of Fetch partition \
             responses that returned a non-zero error code, per \
             topic. Mirrors Kafka's \
             BrokerTopicMetrics.FailedFetchRequestsPerSec. Pairs \
             with topic_fetch_requests for per-topic error rate.",
            self.topic_failed_fetch_requests.clone(),
        );

        registry.register(
            "partitions_led",
            "Number of partitions for which this broker is currently leader.",
            self.partitions_led.clone(),
        );

        registry.register(
            "partitions_total",
            "Total number of partitions (leader + follower \
             replicas) this broker hosts. Mirrors Kafka's \
             ReplicaManager.PartitionCount.",
            self.partitions_total.clone(),
        );

        registry.register(
            "under_replicated_partitions",
            "Count of partitions this broker leads whose ISR \
             is smaller than the assigned replica set. Mirrors Kafka's \
             ReplicaManager.UnderReplicatedPartitions; alert on > 0 \
             to spot stuck followers before they fail an unclean \
             election.",
            self.under_replicated_partitions.clone(),
        );
    }

    fn register_group_2(&self, registry: &mut Registry) {
        registry.register(
            "under_min_isr_partition_count",
            "Count of partitions this broker leads whose ISR \
             is strictly less than the topic's min.insync.replicas. \
             Mirrors Kafka's ReplicaManager.UnderMinIsrPartitionCount; \
             alert on > 0 — these partitions reject acks=all produces \
             with NOT_ENOUGH_REPLICAS.",
            self.under_min_isr_partition_count.clone(),
        );

        registry.register(
            "offline_partitions_count",
            "Count of partitions this broker leads that have \
             no live leader (leader dead with no eligible ISR \
             replacement). Mirrors Kafka's \
             ReplicaManager.OfflinePartitionsCount; alert on > 0 — \
             these partitions are wholly unavailable until an ISR \
             member returns or an unclean election runs.",
            self.offline_partitions_count.clone(),
        );

        registry.register(
            "active_controller",
            "1 if this broker is the raft (controller) leader, 0 otherwise.",
            self.active_controller.clone(),
        );

        registry.register(
            "controller_leader_changes",
            "Cumulative count of distinct controller-leader \
             transitions this broker has observed (any change in the \
             raft leader, including this broker becoming or ceasing \
             to be leader). Mirrors Kafka's \
             KafkaController.LeaderElectionRateAndTimeMs; alert on a \
             sustained rate() > 0 to spot flapping raft leadership.",
            self.controller_leader_changes_total.clone(),
        );

        registry.register(
            "isr_shrinks",
            "Cumulative count of ISR shrinks proposed by this broker's \
             ISR-maintenance loop.",
            self.isr_shrinks_total.clone(),
        );

        registry.register(
            "isr_expands",
            "Cumulative count of ISR expands proposed by this broker's \
             ISR-maintenance loop.",
            self.isr_expands_total.clone(),
        );

        registry.register(
            "partition_bytes_in",
            "Bytes received from producers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            self.partition_bytes_in.clone(),
        );

        registry.register(
            "partition_bytes_out",
            "Bytes served to consumers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            self.partition_bytes_out.clone(),
        );

        registry.register(
            "replication_bytes_in",
            "Bytes received from the partition leader by this broker as a \
             follower (cumulative). Rate(...) for follower throughput; \
             plotted alongside partition_bytes_in surfaces ingest vs. \
             replication-driven traffic.",
            self.replication_bytes_in.clone(),
        );

        registry.register(
            "replication_bytes_out",
            "Bytes this broker served to followers as the partition leader \
             (cumulative). Rate(...) for leader-out-to-followers throughput; \
             together with partition_bytes_out (consumer reads) it attributes \
             outbound traffic to its source.",
            self.replication_bytes_out.clone(),
        );
    }

    fn register_group_3(&self, registry: &mut Registry) {
        registry.register(
            "partition_disk_bytes",
            "On-disk size of a partition's log directory (gauge). Updated by \
             the broker's periodic disk scanner; suppress if scanner is disabled.",
            self.partition_disk_bytes.clone(),
        );

        registry.register(
            "share_group_backlog",
            "Share-group partition backlog in records, emitted by the group coordinator.",
            self.share_group_backlog.clone(),
        );

        registry.register(
            "partition_cpu_micros",
            "Cumulative handler-thread microseconds spent processing each \
             (topic, partition). Rebalancer-targeted; rate(...) divided by \
             1_000_000 yields core occupancy.",
            self.partition_cpu_micros.clone(),
        );

        registry.register(
            "incremental_fetch_sessions",
            "KIP-227: live incremental-fetch sessions cached by this broker (gauge).",
            self.incremental_fetch_sessions.clone(),
        );

        registry.register(
            "incremental_fetch_session_evictions",
            "KIP-227: cumulative count of incremental-fetch sessions evicted from \
             the cache to make room for a new allocation.",
            self.incremental_fetch_session_evictions_total.clone(),
        );

        registry.register(
            "incremental_fetch_partitions_cached",
            "KIP-227: total (topic, partition) tuples held across every live \
             incremental-fetch session (gauge).",
            self.incremental_fetch_partitions_cached.clone(),
        );

        registry.register(
            "client_software_versions",
            "KIP-511: cumulative count of accepted ApiVersions handshakes, \
             labelled by client software name and version. One increment \
             per successful v3+ ApiVersions call.",
            self.client_software_versions.clone(),
        );

        registry.register(
            "successful_authentication",
            "Cumulative count of SaslAuthenticate frames per \
             mechanism that ended in a successful auth state transition. \
             Mirrors Kafka's \
             kafka.network:type=Selector,name=successful-authentication-total. \
             Labelled by the canonical SASL mechanism wire name \
             (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER). \
             Paired with failed_authentication so rate(...) ratios \
             expose per-mechanism credential-failure rates.",
            self.successful_authentication.clone(),
        );

        registry.register(
            "failed_authentication",
            "Cumulative count of SaslAuthenticate frames per \
             mechanism that returned a non-zero error code. Mirrors \
             Kafka's failed-authentication-total. ILLEGAL_SASL_STATE \
             rejects (SaslAuthenticate without prior SaslHandshake) \
             land under the `Unknown` mechanism label.",
            self.failed_authentication.clone(),
        );

        registry.register(
            "api_requests",
            "Cumulative count of dispatched requests per \
             Kafka API key (variant name from the `ApiKey` enum, e.g. \
             Produce / Fetch / DescribeQuorum). Unknown api keys land \
             under the `Unknown` label. Mirrors Kafka's \
             RequestMetrics.RequestsPerSec; rate(...) yields per-API \
             throughput.",
            self.api_requests.clone(),
        );

        registry.register(
            "unsupported_api_requests",
            "Cumulative count of requests the dispatcher \
             answered with the synthetic UNSUPPORTED_VERSION response \
             because no handler matched the api_key. Labelled with \
             the ApiKey variant name (or `Unknown` for unrecognised \
             keys). Alert on rate(...) > 0 to catch upgrade-skew or \
             misconfigured clients.",
            self.unsupported_api_requests.clone(),
        );
    }

    fn register_group_4(&self, registry: &mut Registry) {
        registry.register(
            "request_duration_seconds",
            "Per-Kafka-API request-handling latency in \
             seconds, observed in the dispatch path around the full \
             handler round-trip (decode → handle → encode). Labelled by \
             the ApiKey variant name. Operators graph \
             histogram_quantile(0.99, rate(..._bucket[5m])) per api to \
             spot tail-latency regressions.",
            self.request_duration_seconds.clone(),
        );

        registry.register(
            "in_flight_requests",
            "Number of requests currently being handled by this broker \
             (gauge). Incremented on dispatch entry, decremented on exit; \
             a sustained climb signals handler stalls.",
            self.in_flight_requests.clone(),
        );

        registry.register(
            "active_connections",
            "Number of client connections currently open to this broker \
             (gauge). Incremented when the per-connection serve loop \
             starts, decremented when it exits (EOF / error / SASL expiry).",
            self.active_connections.clone(),
        );

        registry.register(
            "request_errors",
            "Per-Kafka-API count of requests whose handler \
             returned an error (dispatcher closed the connection). \
             Labelled by the ApiKey variant name; disjoint from \
             unsupported_api_requests. Alert on rate(...) > 0 to catch \
             handler-level faults.",
            self.request_errors.clone(),
        );

        registry.register(
            "tiered_storage_rlmm_topic_backed",
            "KIP-405: 1 when this broker is answering remote-log \
             metadata queries from the durable __remote_log_metadata topic \
             (production RLMM); 0 while still on the fail-closed \
             NotReadyRlmm placeholder. Bumped to 1 by the bootstrap task \
             after a successful SwappableRlmm swap; stays at 0 for \
             clusters that never asked for `metadataManager: Topic`.",
            self.tiered_storage_rlmm_topic_backed.clone(),
        );

        registry.register(
            "tiered_storage_rlmm_bootstrap_attempts",
            "Number of topic-backed RLMM bootstrap attempts; climbs while \
             stuck retrying, flat once tiered_storage_rlmm_topic_backed \
             flips to 1.",
            self.tiered_storage_rlmm_bootstrap_attempts.clone(),
        );

        registry.register(
            "produce_message_conversions",
            "Cumulative count of v0/v1 → v2 record-batch \
             up-conversions on the Produce path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.ProduceMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             producers in the cluster.",
            self.produce_message_conversions.clone(),
        );

        registry.register(
            "fetch_message_conversions",
            "Cumulative count of v2 → v0/v1 record-batch \
             down-conversions on the Fetch path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.FetchMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             consumers in the cluster.",
            self.fetch_message_conversions.clone(),
        );

        registry.register(
            "unclean_leader_elections",
            "KIP-841: cumulative count of unclean leader \
             elections driven by this broker (as controller leader). An \
             unclean election is one where the new leader was picked \
             from outside the ISR because the partition's ISR was empty \
             at failover time and the topic had \
             unclean.leader.election.enable=true. Each such election \
             accepts possible data loss. Mirrors Kafka's \
             ControllerStats.UncleanLeaderElectionsPerSec; an operator \
             alert on rate(unclean_leader_elections_total[5m]) > 0 \
             flags the data-loss footgun.",
            self.unclean_leader_elections_total.clone(),
        );

        registry.register(
            "audit_events_total",
            "Cumulative audit records successfully written to the audit topic",
            self.audit_events_total.clone(),
        );
    }

    fn register_group_5(&self, registry: &mut Registry) {
        registry.register(
            "audit_write_failures_total",
            "Cumulative audit records that failed to write to the audit topic",
            self.audit_write_failures_total.clone(),
        );

        registry.register(
            "audit_spool_depth",
            "Current count of audit records buffered in the durable spool",
            self.audit_spool_depth.clone(),
        );

        registry.register(
            "audit_spool_bytes",
            "Current bytes buffered in the durable audit spool",
            self.audit_spool_bytes.clone(),
        );

        registry.register(
            "audit_records_spooled",
            "Cumulative audit records diverted to the spool on topic-write failure",
            self.audit_records_spooled_total.clone(),
        );

        registry.register(
            "audit_records_replayed",
            "Cumulative audit records drained from the spool back to the topic",
            self.audit_records_replayed_total.clone(),
        );

        registry.register(
            "audit_records_dropped",
            "Cumulative audit records lost (channel-full or spool-full)",
            self.audit_records_dropped_total.clone(),
        );

        registry.register(
            "log_cleaner_runs",
            "Cumulative count of completed log-compaction sweeps run by \
             this broker's cleaner (one per tick_all pass).",
            self.log_cleaner_runs_total.clone(),
        );

        registry.register(
            "log_compactions",
            "Per-partition cumulative count of compaction passes this \
             broker's cleaner completed successfully.",
            self.log_compactions_total.clone(),
        );
    }

    /// Build and register every broker metric.
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn new() -> Self {
        let registry = Arc::new(Mutex::new(Registry::with_prefix("crabka_broker")));
        let metrics = Self::unregistered(registry);
        {
            let mut registry = metrics
                .registry
                .try_lock()
                .expect("fresh metrics registry cannot be locked");
            metrics.register_group_1(&mut registry);
            metrics.register_group_2(&mut registry);
            metrics.register_group_3(&mut registry);
            metrics.register_group_4(&mut registry);
            metrics.register_group_5(&mut registry);
        }
        metrics
    }

    /// KIP-511: bump the per-(name, version) handshake counter.
    /// Caller guarantees both inputs already passed
    /// `handlers::api_versions::is_valid_client_info` so the label
    /// values stay bounded.
    pub fn record_client_software(&self, name: &str, version: &str) {
        let lbl = ClientSoftwareLabel {
            software_name: name.to_string(),
            software_version: version.to_string(),
        };
        self.client_software_versions.get_or_create(&lbl).inc();
    }

    /// Account one completed `SaslAuthenticate` frame on
    /// `mechanism`. `success = true` increments
    /// `successful_authentication_total`; `success = false`
    /// increments `failed_authentication_total`. The mechanism
    /// label is the canonical Kafka wire name; pass `"Unknown"`
    /// for the `ILLEGAL_SASL_STATE` reject (no prior handshake)
    /// to keep cardinality bounded.
    pub fn record_authentication(&self, mechanism: &str, success: bool) {
        let lbl = SaslMechanismLabel {
            mechanism: mechanism.to_string(),
        };
        if success {
            self.successful_authentication.get_or_create(&lbl).inc();
        } else {
            self.failed_authentication.get_or_create(&lbl).inc();
        }
    }

    /// Account one dispatched request for `api_key`. The
    /// label is the human-readable variant name from
    /// `ApiKey::IntoStaticStr`; unknown keys fold under `"Unknown"`.
    pub fn record_api_request(&self, api_key: crate::handlers::ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.api_requests.get_or_create(&lbl).inc();
    }

    /// Account one request the dispatcher rejected with
    /// `UNSUPPORTED_VERSION` because no handler matched `api_key`
    /// (e.g. unknown `api_key`, or known `api_key` with no version
    /// negotiated). Mirrors the labelling of `record_api_request`.
    pub fn record_unsupported_api_request(&self, api_key: crate::handlers::ApiKeyCode) {
        let lbl = ApiKeyLabel {
            api_key: api_key_label_name(api_key).to_string(),
        };
        self.unsupported_api_requests.get_or_create(&lbl).inc();
    }

    /// Observe the wall-clock handling latency for one dispatched
    /// request on the `request_duration_seconds{api}` histogram. `api_key`
    /// is resolved to the same human-readable label as
    /// `record_api_request` (unknown keys fold under `"Unknown"`), so the
    /// two families share a label set. Called from the dispatch path once
    /// per frame with the elapsed seconds of the full handler round-trip.
    pub fn observe_request_duration(&self, api_key: i16, seconds: f64) {
        let name: &'static str = match crabka_protocol::api_key::ApiKey::from_i16(api_key) {
            Some(k) => k.into(),
            None => "Unknown",
        };
        let lbl = ApiKeyLabel {
            api_key: name.to_string(),
        };
        self.request_duration_seconds
            .get_or_create(&lbl)
            .observe(seconds);
    }

    /// Account one request whose handler returned an error (the
    /// dispatcher closed the connection). Labelled like
    /// `record_api_request`; disjoint from the
    /// `unsupported_api_requests` family.
    pub fn record_request_error(&self, api_key: i16) {
        let name: &'static str = match crabka_protocol::api_key::ApiKey::from_i16(api_key) {
            Some(k) => k.into(),
            None => "Unknown",
        };
        let lbl = ApiKeyLabel {
            api_key: name.to_string(),
        };
        self.request_errors.get_or_create(&lbl).inc();
    }

    /// Convenience: record a Produce hit on `topic` with the given
    /// payload size. No-op on the error path — callers shouldn't call
    /// this if the request was rejected.
    pub fn record_produce(&self, topic: &str, bytes: u64) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_produce_requests.get_or_create(&lbl).inc();
        if bytes > 0 {
            self.topic_bytes_in.get_or_create(&lbl).inc_by(bytes);
        }
    }

    /// Account `messages` records received on the Produce
    /// path for `topic`. Mirrors Kafka's
    /// `BrokerTopicMetrics.MessagesInPerSec`. Called once per
    /// `RecordBatch` with the batch's record count. Zero is a
    /// legitimate value (legacy batches whose record count we can't
    /// cheaply derive without a full conversion) and is a no-op.
    pub fn record_produce_messages(&self, topic: &str, messages: u64) {
        if messages == 0 {
            return;
        }
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_messages_in.get_or_create(&lbl).inc_by(messages);
    }

    /// Convenience: record a Fetch hit on `topic` with the bytes
    /// delivered. The `bytes` arg may legitimately be zero (empty
    /// fetch); the request counter still increments.
    pub fn record_fetch(&self, topic: &str, bytes: u64) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_fetch_requests.get_or_create(&lbl).inc();
        if bytes > 0 {
            self.topic_bytes_out.get_or_create(&lbl).inc_by(bytes);
        }
    }

    /// Record a single failed Produce partition response
    /// for `topic`. Callers bump once per partition whose response
    /// carries a non-zero error code — mirrors the JVM's per-row
    /// `failedProduceRequestRate.mark()`.
    pub fn record_failed_produce(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_failed_produce_requests.get_or_create(&lbl).inc();
    }

    /// Record a single failed Fetch partition response
    /// for `topic`. Same per-partition semantics as
    /// `record_failed_produce`.
    pub fn record_failed_fetch(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.topic_failed_fetch_requests.get_or_create(&lbl).inc();
    }

    /// Convenience: account a partition's slice of a Produce request.
    /// Called once per partition by the request handler (alongside the
    /// existing topic-level `record_produce`).
    pub fn record_partition_produce(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_bytes_in.get_or_create(&lbl).inc_by(bytes);
    }

    /// Convenience: account a partition's slice of a Fetch response.
    pub fn record_partition_fetch(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_bytes_out.get_or_create(&lbl).inc_by(bytes);
    }

    /// Account bytes this broker received from the partition
    /// leader as a follower (inter-broker `Fetch` round-trip, follower
    /// side). Called from the replicator after a successful append.
    pub fn record_replication_in(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.replication_bytes_in.get_or_create(&lbl).inc_by(bytes);
    }

    /// Account one v0/v1 → v2 up-conversion on the Produce
    /// path (the partition's `records` field arrived as a legacy
    /// `MessageSet` and was decoded into a v2 `RecordBatch`).
    pub fn record_produce_message_conversion(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.produce_message_conversions.get_or_create(&lbl).inc();
    }

    /// Account one v2 → v0/v1 down-conversion on the Fetch
    /// path (a legacy client's Fetch v < 4 response is being assembled
    /// from a v2 record batch).
    pub fn record_fetch_message_conversion(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.fetch_message_conversions.get_or_create(&lbl).inc();
    }

    /// KIP-841: account one unclean leader election (an
    /// election that picked an out-of-ISR replica because the ISR was
    /// empty and the topic had `unclean.leader.election.enable=true`).
    pub fn record_unclean_leader_election(&self) {
        self.unclean_leader_elections_total.inc();
    }

    /// Account bytes this broker served to a follower as the
    /// partition leader (inter-broker `Fetch` round-trip, leader side).
    /// Called from the `Fetch` handler when `replica_id >= 0`.
    pub fn record_replication_out(&self, topic: &str, partition: i32, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.replication_bytes_out.get_or_create(&lbl).inc_by(bytes);
    }

    /// Convenience: account handler-thread microseconds spent on a
    /// partition. Called from the produce / fetch hot paths around the
    /// per-partition work. No-ops on zero so we don't allocate a label
    /// entry for trivial measurements.
    pub fn record_partition_cpu_micros(&self, topic: &str, partition: i32, micros: u64) {
        if micros == 0 {
            return;
        }
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.partition_cpu_micros.get_or_create(&lbl).inc_by(micros);
    }

    /// Account one completed log-compaction sweep (a full `tick_all`
    /// pass). Called once per cleaner tick, whether or not any partition
    /// was eligible, so a test can observe that a full pass ran after it
    /// sealed a segment.
    pub fn record_cleaner_run(&self) {
        self.log_cleaner_runs_total.inc();
    }

    /// Account one successful per-partition compaction pass
    /// (`Partition::compact_log` returned `Ok`).
    pub fn record_compaction(&self, topic: &str, partition: i32) {
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.log_compactions_total.get_or_create(&lbl).inc();
    }
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve a wire `api_key` to the `ApiKey` variant name used as the
/// metric label, folding unrecognised keys under [`UNKNOWN_LABEL`].
fn api_key_label_name(api_key: crate::handlers::ApiKeyCode) -> &'static str {
    match crabka_protocol::api_key::ApiKey::from_i16(api_key) {
        Some(k) => k.into(),
        None => UNKNOWN_LABEL,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn registry_has_broker_prefix_and_all_metrics() {
        let m = BrokerMetrics::new();
        m.record_produce("topic-a", 100);
        m.record_produce_messages("topic-a", 5);
        m.record_fetch("topic-a", 50);
        m.record_partition_produce("topic-a", 0, 100);
        m.record_partition_fetch("topic-a", 0, 50);
        m.record_partition_cpu_micros("topic-a", 0, 250);
        m.record_replication_in("topic-a", 0, 4096);
        m.record_replication_out("topic-a", 0, 8192);
        m.record_cleaner_run();
        m.record_compaction("topic-a", 0);
        m.record_produce_message_conversion("topic-a");
        m.record_fetch_message_conversion("topic-a");
        m.record_failed_produce("topic-a");
        m.record_failed_fetch("topic-a");
        m.record_authentication("PLAIN", true);
        m.record_authentication("SCRAM-SHA-512", false);
        m.record_authentication("Unknown", false);
        m.record_unclean_leader_election();
        m.record_api_request(0); // Produce
        m.record_api_request(999); // unknown → "Unknown" label
        m.record_unsupported_api_request(999);
        m.observe_request_duration(0, 0.002); // Produce latency sample
        m.observe_request_duration(999, 1.5); // unknown → "Unknown" label
        m.record_request_error(1); // Fetch handler error
        m.in_flight_requests.set(3);
        m.active_connections.set(11);
        m.partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "topic-a".into(),
                partition: 0,
            })
            .set(42);
        m.share_group_backlog
            .get_or_create(&ShareGroupLabel {
                group_id: "workers".into(),
                topic: "topic-a".into(),
                partition: 0,
            })
            .set(9);
        m.partitions_led.set(7);
        m.partitions_total.set(42);
        m.under_replicated_partitions.set(3);
        m.under_min_isr_partition_count.set(2);
        m.offline_partitions_count.set(1);
        m.active_controller.set(1);
        m.controller_leader_changes_total.inc();
        m.isr_shrinks_total.inc();
        m.isr_expands_total.inc_by(2);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        // Spot-check every metric is present and prefixed.
        for needle in [
            "crabka_broker_topic_bytes_in_total",
            "crabka_broker_topic_bytes_out_total",
            "crabka_broker_topic_produce_requests_total",
            "crabka_broker_topic_fetch_requests_total",
            "crabka_broker_partitions_led",
            "crabka_broker_partitions_total",
            "crabka_broker_under_replicated_partitions",
            "crabka_broker_under_min_isr_partition_count",
            "crabka_broker_offline_partitions_count",
            "crabka_broker_active_controller",
            "crabka_broker_controller_leader_changes_total",
            "crabka_broker_isr_shrinks_total",
            "crabka_broker_isr_expands_total",
            "crabka_broker_partition_bytes_in_total",
            "crabka_broker_partition_bytes_out_total",
            "crabka_broker_partition_disk_bytes",
            "crabka_broker_share_group_backlog",
            "crabka_broker_partition_cpu_micros_total",
            "crabka_broker_incremental_fetch_sessions",
            "crabka_broker_incremental_fetch_session_evictions_total",
            "crabka_broker_incremental_fetch_partitions_cached",
            "crabka_broker_replication_bytes_in_total",
            "crabka_broker_replication_bytes_out_total",
            "crabka_broker_tiered_storage_rlmm_topic_backed",
            "crabka_broker_produce_message_conversions_total",
            "crabka_broker_fetch_message_conversions_total",
            "crabka_broker_unclean_leader_elections_total",
            "crabka_broker_log_cleaner_runs_total",
            "crabka_broker_log_compactions_total",
            "crabka_broker_api_requests_total",
            "crabka_broker_unsupported_api_requests_total",
            "crabka_broker_request_duration_seconds_bucket",
            "crabka_broker_request_duration_seconds_sum",
            "crabka_broker_request_duration_seconds_count",
            "crabka_broker_in_flight_requests",
            "crabka_broker_active_connections",
            "crabka_broker_request_errors_total",
            "crabka_broker_messages_in_total",
            "crabka_broker_topic_failed_produce_requests_total",
            "crabka_broker_topic_failed_fetch_requests_total",
            "crabka_broker_successful_authentication_total",
            "crabka_broker_failed_authentication_total",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        // Topic label and values made it through.
        for (needle, what) in [
            ("topic=\"topic-a\"", "topic label"),
            ("100", "bytes_in=100"),
            ("50", "bytes_out=50"),
            ("7", "partitions_led=7"),
        ] {
            assert!(buf.contains(needle), "expected {what} in:\n{buf}");
        }
    }

    #[test]
    fn record_fetch_zero_bytes_still_bumps_request_count() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        // Pre-condition: no entry for the label yet.
        m.record_fetch("t", 0);
        assert!(m.topic_fetch_requests.get_or_create(&lbl).get() == 1);
        assert!(m.topic_bytes_out.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn record_produce_increments_both_counters() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        m.record_produce("t", 1024);
        m.record_produce("t", 2048);
        assert!(m.topic_produce_requests.get_or_create(&lbl).get() == 2);
        assert!(m.topic_bytes_in.get_or_create(&lbl).get() == 3072);
    }

    #[test]
    fn record_produce_messages_sums_across_calls_and_skips_zero() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        // Zero is a no-op (legacy batches; the v2-conversion-time
        // counter tracks those arrivals separately).
        m.record_produce_messages("t", 0);
        // The label entry is intentionally NOT eagerly created on a
        // zero-bump; rate(...) over a never-seen topic should yield
        // 0, not a phantom series.
        m.record_produce_messages("t", 3);
        m.record_produce_messages("t", 7);
        assert!(m.topic_messages_in.get_or_create(&lbl).get() == 10);
    }

    #[test]
    fn record_authentication_splits_success_and_failure_per_mechanism() {
        let m = BrokerMetrics::new();
        let plain = SaslMechanismLabel {
            mechanism: "PLAIN".to_string(),
        };
        let scram = SaslMechanismLabel {
            mechanism: "SCRAM-SHA-256".to_string(),
        };
        let unknown = SaslMechanismLabel {
            mechanism: "Unknown".to_string(),
        };
        m.record_authentication("PLAIN", true);
        m.record_authentication("PLAIN", true);
        m.record_authentication("PLAIN", false);
        m.record_authentication("SCRAM-SHA-256", true);
        m.record_authentication("Unknown", false);
        // PLAIN: 2 successes, 1 failure. SCRAM-SHA-256: 1 success, 0
        // failures (must not lazily allocate a failure entry from the
        // success bump). ILLEGAL_SASL_STATE: 0 successes, 1 failure
        // under the `Unknown` sentinel.
        let cases = [
            ("successful", &m.successful_authentication, &plain, 2),
            ("failed", &m.failed_authentication, &plain, 1),
            ("successful", &m.successful_authentication, &scram, 1),
            ("failed", &m.failed_authentication, &unknown, 1),
            ("successful", &m.successful_authentication, &unknown, 0),
        ];
        for (outcome, family, label, want) in cases {
            // Each read is its own statement: `get_or_create` returns a
            // read guard, and a first-materialization on the same family
            // takes the write lock — holding several guards in one
            // expression self-deadlocks.
            let got = family.get_or_create(label).get();
            assert!(got == want, "{outcome} auth for {:?}", label.mechanism);
        }
    }

    #[test]
    fn record_client_software_accumulates_per_name_version() {
        let m = BrokerMetrics::new();
        let crabka_100 = ClientSoftwareLabel {
            software_name: "crabka".to_string(),
            software_version: "1.0.0".to_string(),
        };
        let crabka_101 = ClientSoftwareLabel {
            software_name: "crabka".to_string(),
            software_version: "1.0.1".to_string(),
        };
        let other = ClientSoftwareLabel {
            software_name: "other-lib".to_string(),
            software_version: "1.0.0".to_string(),
        };

        m.record_client_software("crabka", "1.0.0");
        m.record_client_software("crabka", "1.0.0");
        m.record_client_software("crabka", "1.0.1");
        m.record_client_software("other-lib", "1.0.0");

        for (label, want) in [(&crabka_100, 2), (&crabka_101, 1), (&other, 1)] {
            let got = m.client_software_versions.get_or_create(label).get();
            assert!(got == want, "label {label:?}");
        }
    }

    #[tokio::test]
    async fn record_client_software_renders_labelled_openmetrics_counter() {
        let m = BrokerMetrics::new();

        m.record_client_software("render-lib", "2.0.0");

        let mut body = String::new();
        let registry = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut body, &registry).unwrap();
        assert!(body.contains(
            "crabka_broker_client_software_versions_total{software_name=\"render-lib\",software_version=\"2.0.0\"} 1"
        ));
    }

    #[test]
    fn partition_helpers_increment_the_right_family() {
        let m = BrokerMetrics::new();
        m.record_partition_produce("t", 0, 1024);
        m.record_partition_produce("t", 1, 512);
        m.record_partition_fetch("t", 0, 2048);
        m.record_partition_cpu_micros("t", 0, 500);
        m.partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "t".into(),
                partition: 0,
            })
            .set(1_000_000);

        let lbl_p0 = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        let lbl_p1 = PartitionLabel {
            topic: "t".into(),
            partition: 1,
        };
        let cases = [
            ("bytes_in", &m.partition_bytes_in, &lbl_p0, 1024),
            ("bytes_in", &m.partition_bytes_in, &lbl_p1, 512),
            ("bytes_out", &m.partition_bytes_out, &lbl_p0, 2048),
            ("cpu_micros", &m.partition_cpu_micros, &lbl_p0, 500),
        ];
        for (family_name, family, label, want) in cases {
            // Each read is its own statement: `get_or_create` returns a
            // read guard, and a first-materialization on the same family
            // takes the write lock — holding several guards in one
            // expression self-deadlocks.
            let got = family.get_or_create(label).get();
            assert!(
                got == want,
                "{family_name} for partition {}",
                label.partition
            );
        }
        // `partition_disk_bytes` is a Gauge family (i64), so it stays
        // out of the Counter table above.
        let disk_p0 = m.partition_disk_bytes.get_or_create(&lbl_p0).get();
        assert!(disk_p0 == 1_000_000);
    }

    #[test]
    fn failed_request_counters_track_per_topic_and_per_call() {
        // `record_failed_produce` / `record_failed_fetch`
        // are bumped once per failed partition row. Two calls on
        // `t-good` and one on `t-bad` must land on the right labels
        // and yield independent series.
        let m = BrokerMetrics::new();
        m.record_failed_produce("t-good");
        m.record_failed_produce("t-good");
        m.record_failed_produce("t-bad");
        m.record_failed_fetch("t-good");

        let good = TopicLabel {
            topic: "t-good".into(),
        };
        let bad = TopicLabel {
            topic: "t-bad".into(),
        };
        // t-bad never saw a failed fetch — series is materialized by
        // `get_or_create` at read time but its value is 0, which is
        // what `rate(failed_fetch{topic="t-bad"}[1m])` should compute.
        let cases = [
            ("failed_produce", &m.topic_failed_produce_requests, &good, 2),
            ("failed_produce", &m.topic_failed_produce_requests, &bad, 1),
            ("failed_fetch", &m.topic_failed_fetch_requests, &good, 1),
            ("failed_fetch", &m.topic_failed_fetch_requests, &bad, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.topic);
        }
    }

    #[test]
    fn zero_bytes_no_op_on_partition_helpers() {
        let m = BrokerMetrics::new();
        m.record_partition_produce("t", 0, 0);
        m.record_partition_fetch("t", 0, 0);
        let lbl = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        // Counters still exist (get_or_create creates them) but at 0.
        assert!(m.partition_bytes_in.get_or_create(&lbl).get() == 0);
        assert!(m.partition_bytes_out.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn zero_micros_no_op() {
        let m = BrokerMetrics::new();
        m.record_partition_cpu_micros("t", 0, 0);
        let lbl = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        // Helper short-circuits at 0; the label entry isn't created.
        assert!(m.partition_cpu_micros.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn tiered_storage_rlmm_topic_backed_defaults_zero_and_can_be_set() {
        let m = BrokerMetrics::new();
        // Default for a fresh broker (in-memory placeholder, or no
        // tiered-storage at all) is `0`.
        assert!(m.tiered_storage_rlmm_topic_backed.get() == 0);
        // The bootstrap task bumps it to `1` after a successful
        // SwappableRlmm swap.
        m.tiered_storage_rlmm_topic_backed.set(1);
        assert!(m.tiered_storage_rlmm_topic_backed.get() == 1);
    }

    #[test]
    fn tiered_storage_rlmm_bootstrap_attempts_counts_up() {
        let m = BrokerMetrics::new();
        assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 0);
        m.tiered_storage_rlmm_bootstrap_attempts.inc();
        m.tiered_storage_rlmm_bootstrap_attempts.inc();
        assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 2);
    }

    #[test]
    fn message_conversion_helpers_accumulate_per_topic() {
        let m = BrokerMetrics::new();
        m.record_produce_message_conversion("orders");
        m.record_produce_message_conversion("orders");
        m.record_produce_message_conversion("payments");
        m.record_fetch_message_conversion("orders");
        m.record_fetch_message_conversion("payments");
        m.record_fetch_message_conversion("payments");

        let orders = TopicLabel {
            topic: "orders".into(),
        };
        let payments = TopicLabel {
            topic: "payments".into(),
        };
        let cases = [
            (
                "produce_conversions",
                &m.produce_message_conversions,
                &orders,
                2,
            ),
            (
                "produce_conversions",
                &m.produce_message_conversions,
                &payments,
                1,
            ),
            (
                "fetch_conversions",
                &m.fetch_message_conversions,
                &orders,
                1,
            ),
            (
                "fetch_conversions",
                &m.fetch_message_conversions,
                &payments,
                2,
            ),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.topic);
        }
    }

    #[test]
    fn unsupported_api_requests_counter_is_disjoint_from_api_requests() {
        let m = BrokerMetrics::new();
        // Invariant: `record_unsupported_api_request` bumps
        // only the `unsupported_api_requests` family — operators
        // expect `api_requests` to count *every* dispatched frame and
        // `unsupported_api_requests` to count just the ones that hit
        // the synthetic UNSUPPORTED_VERSION arm.
        m.record_unsupported_api_request(0); // Produce, unsupported
        m.record_unsupported_api_request(999); // truly unknown

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        // `record_unsupported_api_request` does NOT also bump
        // `api_requests`; the dispatcher already did that for the
        // request in question via `record_api_request`.
        let cases = [
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &produce,
                1,
            ),
            (
                "unsupported_api_requests",
                &m.unsupported_api_requests,
                &unknown,
                1,
            ),
            ("api_requests", &m.api_requests, &produce, 0),
            ("api_requests", &m.api_requests, &unknown, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(got == want, "{family_name} for {:?}", label.api_key);
        }
    }

    #[test]
    fn api_requests_label_resolves_known_keys_and_folds_unknown() {
        let m = BrokerMetrics::new();
        // Three known + one unknown api_key. Verify per-label tallies.
        m.record_api_request(0); // Produce
        m.record_api_request(0); // Produce again
        m.record_api_request(1); // Fetch
        m.record_api_request(12_345); // out-of-range → Unknown

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        let unknown = ApiKeyLabel {
            api_key: "Unknown".into(),
        };
        for (label, want) in [(&produce, 2), (&fetch, 1), (&unknown, 1)] {
            let got = m.api_requests.get_or_create(label).get();
            assert!(got == want, "api_key {:?}", label.api_key);
        }
    }

    #[test]
    fn audit_counters_present() {
        let m = BrokerMetrics::new();
        m.audit_events_total.inc();
        m.audit_write_failures_total.inc();
        assert2::check!(m.audit_events_total.get() == 1);
        assert2::check!(m.audit_write_failures_total.get() == 1);
    }

    #[test]
    fn replication_helpers_accumulate_per_partition() {
        let m = BrokerMetrics::new();
        // Two appends from the same leader partition.
        m.record_replication_in("orders", 3, 1_500);
        m.record_replication_in("orders", 3, 2_500);
        // Different partition stays independent.
        m.record_replication_in("orders", 4, 100);
        // Outbound side: bytes this broker served to its followers.
        m.record_replication_out("orders", 3, 4_000);
        m.record_replication_out("orders", 4, 0); // no-op

        let lbl3 = PartitionLabel {
            topic: "orders".into(),
            partition: 3,
        };
        let lbl4 = PartitionLabel {
            topic: "orders".into(),
            partition: 4,
        };
        let cases = [
            ("replication_in", &m.replication_bytes_in, &lbl3, 4_000),
            ("replication_in", &m.replication_bytes_in, &lbl4, 100),
            ("replication_out", &m.replication_bytes_out, &lbl3, 4_000),
            ("replication_out", &m.replication_bytes_out, &lbl4, 0),
        ];
        for (family_name, family, label, want) in cases {
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = family.get_or_create(label).get();
            assert!(
                got == want,
                "{family_name} for partition {}",
                label.partition
            );
        }
    }

    #[tokio::test]
    async fn request_duration_errors_and_gauges_render() {
        let m = BrokerMetrics::new();
        // Two Produce latency samples + one unknown-key sample.
        m.observe_request_duration(0, 0.0008);
        m.observe_request_duration(0, 0.04);
        m.observe_request_duration(12_345, 2.0); // → "Unknown" label
        m.record_request_error(1); // Fetch handler fault
        m.record_request_error(1);
        m.in_flight_requests.inc();
        m.in_flight_requests.inc();
        m.in_flight_requests.dec();
        m.active_connections.set(5);

        let produce = ApiKeyLabel {
            api_key: "Produce".into(),
        };
        let fetch = ApiKeyLabel {
            api_key: "Fetch".into(),
        };
        // Histogram Family exposes sample count via the encoded `_count`;
        // assert the render + the error/gauge values here.
        assert!(m.request_errors.get_or_create(&fetch).get() == 2);
        assert!(m.in_flight_requests.get() == 1);
        assert!(m.active_connections.get() == 5);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        assert!(
            buf.contains("crabka_broker_request_duration_seconds_count{api_key=\"Produce\"} 2"),
            "expected 2 Produce latency samples in:\n{buf}"
        );
        assert!(
            buf.contains("crabka_broker_request_errors_total{api_key=\"Fetch\"} 2"),
            "expected 2 Fetch request errors in:\n{buf}"
        );
        assert!(buf.contains("crabka_broker_in_flight_requests 1"));
        assert!(buf.contains("crabka_broker_active_connections 5"));
        // Unknown api_key folds under the shared "Unknown" label.
        assert!(buf.contains("api_key=\"Unknown\""), "unknown label missing");
        // Keep `produce` referenced to document the intended label.
        let _ = produce;
    }

    #[test]
    fn audit_spool_metrics_present() {
        let m = BrokerMetrics::new();
        m.audit_records_spooled_total.inc();
        m.audit_records_replayed_total.inc();
        m.audit_records_dropped_total.inc();
        m.audit_spool_depth.set(7);
        m.audit_spool_bytes.set(123);
        assert2::check!(m.audit_records_spooled_total.get() == 1);
        assert2::check!(m.audit_spool_depth.get() == 7);
        assert2::check!(m.audit_spool_bytes.get() == 123);
    }
}
