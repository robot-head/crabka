//! Broker-side Prometheus metrics (slice 39).
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

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

/// Shared registry owning every metric the broker emits. Wrapped in
/// `Arc<Mutex<…>>` because `prometheus-client` requires `&mut Registry`
/// to register and we want lazy registration from multiple init paths.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Per-topic label set. `EncodeLabelSet` is the prometheus-client
/// derive that produces the `topic="<name>"` label on emitted samples.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicLabel {
    pub topic: String,
}

/// Per-partition label set, paired with the new `partition_*` metric
/// families. Slice 43e — consumed by the rebalancer's metric scraper.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
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

/// Cheaply-clonable bundle of counter / gauge handles. Construct once
/// in `Broker::start`; hand out clones (each clone is a single
/// `Arc::clone`) to every subsystem that emits.
#[derive(Clone)]
pub struct BrokerMetrics {
    pub registry: SharedRegistry,
    pub topic_bytes_in: Family<TopicLabel, Counter>,
    pub topic_bytes_out: Family<TopicLabel, Counter>,
    pub topic_produce_requests: Family<TopicLabel, Counter>,
    pub topic_fetch_requests: Family<TopicLabel, Counter>,
    pub partition_bytes_in: Family<PartitionLabel, Counter>,
    pub partition_bytes_out: Family<PartitionLabel, Counter>,
    /// Slice 48k: cumulative bytes this broker accepted from a partition
    /// leader as a follower (`Fetch(replica_id >= 0)` round-trip). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesInPerSec`. Operators
    /// graph `rate(replication_bytes_in_total[1m])` to spot ISR fall-behind
    /// caused by ingest, not by client read load.
    pub replication_bytes_in: Family<PartitionLabel, Counter>,
    /// Slice 48k: cumulative bytes this broker served *to* a follower
    /// (i.e. the leader-side outbound for inter-broker `Fetch`). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesOutPerSec`. Operators
    /// graph the per-partition rate to attribute leader outbound to
    /// followers vs. consumers (the latter still rolls up to
    /// `partition_bytes_out`).
    pub replication_bytes_out: Family<PartitionLabel, Counter>,
    pub partition_disk_bytes: Family<PartitionLabel, Gauge>,
    /// Cumulative handler-thread microseconds spent processing each
    /// (topic, partition). Exported as
    /// `crabka_broker_partition_cpu_micros_total`. Rebalancer takes
    /// `rate(...)` to get micros/sec; dividing by `1_000_000` yields the
    /// per-partition core occupancy. We track microseconds (integer
    /// counter) rather than seconds (float) because `prometheus-client`
    /// counters are `u64`.
    pub partition_cpu_micros: Family<PartitionLabel, Counter>,
    pub partitions_led: Gauge,
    /// Slice 8b: total number of partitions (leader + follower
    /// replicas) this broker hosts. Mirrors Kafka's
    /// `ReplicaManager.PartitionCount`. Sampled in the same per-second
    /// tick as `partitions_led`.
    pub partitions_total: Gauge,
    /// Slice 8b: count of partitions this broker leads whose ISR is
    /// smaller than the assigned replica set — Kafka's
    /// `ReplicaManager.UnderReplicatedPartitions`. Sampled by reading
    /// the current `MetadataImage` and matching partitions where this
    /// broker is the leader. Operators alert on
    /// `under_replicated_partitions > 0` to spot stuck followers
    /// before they fail an unclean election.
    pub under_replicated_partitions: Gauge,
    pub active_controller: Gauge,
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
    /// Slice 48l (KIP-405): `1` when this broker has finished swapping in
    /// the topic-backed `RemoteLogMetadataManager` (slice 48f) and is
    /// answering metadata queries from the durable
    /// `__remote_log_metadata` topic; `0` while still on the in-memory
    /// placeholder (the default until a configured
    /// `[remote_storage.kafka_metadata]` bootstrap completes). Operators
    /// alert on `min_over_time(tiered_storage_rlmm_topic_backed[5m]) == 0`
    /// against clusters that asked for `metadataManager: Topic` to catch
    /// a stuck bootstrap.
    pub tiered_storage_rlmm_topic_backed: Gauge,
    /// Slice 12g: per-topic counter of v0/v1 → v2 record-batch
    /// up-conversions on the Produce path. Mirrors Kafka's
    /// `BrokerTopicMetrics.ProduceMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Produce request whose
    /// `records` field arrived as a legacy `MessageSet`.
    pub produce_message_conversions: Family<TopicLabel, Counter>,
    /// Slice 12g: per-topic counter of v2 → v0/v1 record-batch
    /// down-conversions on the Fetch path. Mirrors Kafka's
    /// `BrokerTopicMetrics.FetchMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Fetch response whose response
    /// payload was down-converted to satisfy a legacy (`Fetch v < 4`)
    /// client.
    pub fetch_message_conversions: Family<TopicLabel, Counter>,
    /// Slice 10c (KIP-841): cumulative count of unclean leader
    /// elections this broker, as controller leader, has driven —
    /// i.e. elections that picked an out-of-ISR replica as the new
    /// leader because the topic had
    /// `unclean.leader.election.enable=true` and the ISR was empty
    /// at failover time. Mirrors Kafka's
    /// `ControllerStats.UncleanLeaderElectionsPerSec`. An operator
    /// alert on `rate(unclean_leader_elections_total[5m]) > 0`
    /// flags the data-loss footgun.
    pub unclean_leader_elections_total: Counter,
}

impl BrokerMetrics {
    /// Build a fresh registry, register every metric, and return the
    /// bundle. Idempotent register failures aren't a concern because we
    /// only call this once per broker process.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("crabka_broker");

        let topic_bytes_in: Family<TopicLabel, Counter> = Family::default();
        let topic_bytes_out: Family<TopicLabel, Counter> = Family::default();
        let topic_produce_requests: Family<TopicLabel, Counter> = Family::default();
        let topic_fetch_requests: Family<TopicLabel, Counter> = Family::default();
        let partition_bytes_in: Family<PartitionLabel, Counter> = Family::default();
        let partition_bytes_out: Family<PartitionLabel, Counter> = Family::default();
        let replication_bytes_in: Family<PartitionLabel, Counter> = Family::default();
        let replication_bytes_out: Family<PartitionLabel, Counter> = Family::default();
        let partition_disk_bytes: Family<PartitionLabel, Gauge> = Family::default();
        let partition_cpu_micros: Family<PartitionLabel, Counter> = Family::default();
        let partitions_led = Gauge::default();
        let partitions_total = Gauge::default();
        let under_replicated_partitions = Gauge::default();
        let active_controller = Gauge::default();
        let isr_shrinks_total = Counter::default();
        let isr_expands_total = Counter::default();
        let incremental_fetch_sessions = Gauge::default();
        let incremental_fetch_session_evictions_total = Counter::default();
        let incremental_fetch_partitions_cached = Gauge::default();
        let client_software_versions: Family<ClientSoftwareLabel, Counter> = Family::default();
        let tiered_storage_rlmm_topic_backed = Gauge::default();
        let produce_message_conversions: Family<TopicLabel, Counter> = Family::default();
        let fetch_message_conversions: Family<TopicLabel, Counter> = Family::default();
        let unclean_leader_elections_total = Counter::default();

        registry.register(
            "topic_bytes_in",
            "Bytes received from producers, per topic (cumulative). \
             Operators compute throughput via rate(...).",
            topic_bytes_in.clone(),
        );
        registry.register(
            "topic_bytes_out",
            "Bytes delivered to fetchers, per topic (cumulative).",
            topic_bytes_out.clone(),
        );
        registry.register(
            "topic_produce_requests",
            "Produce requests handled, per topic (cumulative). One \
             increment per topic per Produce request.",
            topic_produce_requests.clone(),
        );
        registry.register(
            "topic_fetch_requests",
            "Fetch requests handled, per topic (cumulative). One \
             increment per topic per Fetch request.",
            topic_fetch_requests.clone(),
        );
        registry.register(
            "partitions_led",
            "Number of partitions for which this broker is currently leader.",
            partitions_led.clone(),
        );
        registry.register(
            "partitions_total",
            "Slice 8b: total number of partitions (leader + follower \
             replicas) this broker hosts. Mirrors Kafka's \
             ReplicaManager.PartitionCount.",
            partitions_total.clone(),
        );
        registry.register(
            "under_replicated_partitions",
            "Slice 8b: count of partitions this broker leads whose ISR \
             is smaller than the assigned replica set. Mirrors Kafka's \
             ReplicaManager.UnderReplicatedPartitions; alert on > 0 \
             to spot stuck followers before they fail an unclean \
             election.",
            under_replicated_partitions.clone(),
        );
        registry.register(
            "active_controller",
            "1 if this broker is the raft (controller) leader, 0 otherwise.",
            active_controller.clone(),
        );
        // Counter names omit the `_total` suffix — `prometheus-client`
        // appends it automatically when encoding (so emitting
        // `isr_shrinks` here renders as `crabka_broker_isr_shrinks_total`
        // on the wire).
        registry.register(
            "isr_shrinks",
            "Cumulative count of ISR shrinks proposed by this broker's \
             ISR-maintenance loop.",
            isr_shrinks_total.clone(),
        );
        registry.register(
            "isr_expands",
            "Cumulative count of ISR expands proposed by this broker's \
             ISR-maintenance loop.",
            isr_expands_total.clone(),
        );
        registry.register(
            "partition_bytes_in",
            "Bytes received from producers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            partition_bytes_in.clone(),
        );
        registry.register(
            "partition_bytes_out",
            "Bytes served to consumers, per partition (cumulative). \
             Rebalancer-targeted; rate(...) for throughput.",
            partition_bytes_out.clone(),
        );
        registry.register(
            "replication_bytes_in",
            "Bytes received from the partition leader by this broker as a \
             follower (cumulative). Rate(...) for follower throughput; \
             plotted alongside partition_bytes_in surfaces ingest vs. \
             replication-driven traffic.",
            replication_bytes_in.clone(),
        );
        registry.register(
            "replication_bytes_out",
            "Bytes this broker served to followers as the partition leader \
             (cumulative). Rate(...) for leader-out-to-followers throughput; \
             together with partition_bytes_out (consumer reads) it attributes \
             outbound traffic to its source.",
            replication_bytes_out.clone(),
        );
        registry.register(
            "partition_disk_bytes",
            "On-disk size of a partition's log directory (gauge). Updated by \
             the broker's periodic disk scanner; suppress if scanner is disabled.",
            partition_disk_bytes.clone(),
        );
        registry.register(
            "partition_cpu_micros",
            "Cumulative handler-thread microseconds spent processing each \
             (topic, partition). Rebalancer-targeted; rate(...) divided by \
             1_000_000 yields core occupancy.",
            partition_cpu_micros.clone(),
        );
        registry.register(
            "incremental_fetch_sessions",
            "KIP-227: live incremental-fetch sessions cached by this broker (gauge).",
            incremental_fetch_sessions.clone(),
        );
        registry.register(
            "incremental_fetch_session_evictions",
            "KIP-227: cumulative count of incremental-fetch sessions evicted from \
             the cache to make room for a new allocation.",
            incremental_fetch_session_evictions_total.clone(),
        );
        registry.register(
            "incremental_fetch_partitions_cached",
            "KIP-227: total (topic, partition) tuples held across every live \
             incremental-fetch session (gauge).",
            incremental_fetch_partitions_cached.clone(),
        );
        registry.register(
            "client_software_versions",
            "KIP-511: cumulative count of accepted ApiVersions handshakes, \
             labelled by client software name and version. One increment \
             per successful v3+ ApiVersions call.",
            client_software_versions.clone(),
        );
        registry.register(
            "tiered_storage_rlmm_topic_backed",
            "Slice 48l (KIP-405): 1 when this broker is answering remote-log \
             metadata queries from the durable __remote_log_metadata topic \
             (slice 48f production RLMM); 0 while still on the in-memory \
             placeholder. Bumped to 1 by the bootstrap task after a \
             successful SwappableRlmm swap; stays at 0 for clusters that \
             never asked for `metadataManager: Topic`.",
            tiered_storage_rlmm_topic_backed.clone(),
        );
        registry.register(
            "produce_message_conversions",
            "Slice 12g: cumulative count of v0/v1 → v2 record-batch \
             up-conversions on the Produce path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.ProduceMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             producers in the cluster.",
            produce_message_conversions.clone(),
        );
        registry.register(
            "fetch_message_conversions",
            "Slice 12g: cumulative count of v2 → v0/v1 record-batch \
             down-conversions on the Fetch path, per topic. Mirrors \
             Kafka's BrokerTopicMetrics.FetchMessageConversionsPerSec; \
             rate(...) lets operators spot the overhead of legacy \
             consumers in the cluster.",
            fetch_message_conversions.clone(),
        );
        registry.register(
            "unclean_leader_elections",
            "Slice 10c (KIP-841): cumulative count of unclean leader \
             elections driven by this broker (as controller leader). An \
             unclean election is one where the new leader was picked \
             from outside the ISR because the partition's ISR was empty \
             at failover time and the topic had \
             unclean.leader.election.enable=true. Each such election \
             accepts possible data loss. Mirrors Kafka's \
             ControllerStats.UncleanLeaderElectionsPerSec; an operator \
             alert on rate(unclean_leader_elections_total[5m]) > 0 \
             flags the data-loss footgun.",
            unclean_leader_elections_total.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            topic_bytes_in,
            topic_bytes_out,
            topic_produce_requests,
            topic_fetch_requests,
            partition_bytes_in,
            partition_bytes_out,
            replication_bytes_in,
            replication_bytes_out,
            partition_disk_bytes,
            partition_cpu_micros,
            partitions_led,
            partitions_total,
            under_replicated_partitions,
            active_controller,
            isr_shrinks_total,
            isr_expands_total,
            incremental_fetch_sessions,
            incremental_fetch_session_evictions_total,
            incremental_fetch_partitions_cached,
            client_software_versions,
            tiered_storage_rlmm_topic_backed,
            produce_message_conversions,
            fetch_message_conversions,
            unclean_leader_elections_total,
        }
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

    /// Slice 48k: account bytes this broker received from the partition
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

    /// Slice 12g: account one v0/v1 → v2 up-conversion on the Produce
    /// path (the partition's `records` field arrived as a legacy
    /// `MessageSet` and was decoded into a v2 `RecordBatch`).
    pub fn record_produce_message_conversion(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.produce_message_conversions.get_or_create(&lbl).inc();
    }

    /// Slice 12g: account one v2 → v0/v1 down-conversion on the Fetch
    /// path (a legacy client's Fetch v < 4 response is being assembled
    /// from a v2 record batch).
    pub fn record_fetch_message_conversion(&self, topic: &str) {
        let lbl = TopicLabel {
            topic: topic.to_string(),
        };
        self.fetch_message_conversions.get_or_create(&lbl).inc();
    }

    /// Slice 10c (KIP-841): account one unclean leader election (an
    /// election that picked an out-of-ISR replica because the ISR was
    /// empty and the topic had `unclean.leader.election.enable=true`).
    pub fn record_unclean_leader_election(&self) {
        self.unclean_leader_elections_total.inc();
    }

    /// Slice 48k: account bytes this broker served to a follower as the
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
}

impl Default for BrokerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registry_has_broker_prefix_and_all_metrics() {
        let m = BrokerMetrics::new();
        m.record_produce("topic-a", 100);
        m.record_fetch("topic-a", 50);
        m.record_partition_produce("topic-a", 0, 100);
        m.record_partition_fetch("topic-a", 0, 50);
        m.record_partition_cpu_micros("topic-a", 0, 250);
        m.record_replication_in("topic-a", 0, 4096);
        m.record_replication_out("topic-a", 0, 8192);
        m.record_produce_message_conversion("topic-a");
        m.record_fetch_message_conversion("topic-a");
        m.record_unclean_leader_election();
        m.partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "topic-a".into(),
                partition: 0,
            })
            .set(42);
        m.partitions_led.set(7);
        m.partitions_total.set(42);
        m.under_replicated_partitions.set(3);
        m.active_controller.set(1);
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
            "crabka_broker_active_controller",
            "crabka_broker_isr_shrinks_total",
            "crabka_broker_isr_expands_total",
            "crabka_broker_partition_bytes_in_total",
            "crabka_broker_partition_bytes_out_total",
            "crabka_broker_partition_disk_bytes",
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
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        assert!(buf.contains("topic=\"topic-a\""), "topic label missing");
        // Values made it through.
        assert!(buf.contains("100"), "bytes_in=100 missing");
        assert!(buf.contains("50"), "bytes_out=50 missing");
        assert!(buf.contains('7'), "partitions_led=7 missing");
    }

    #[test]
    fn record_fetch_zero_bytes_still_bumps_request_count() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        // Pre-condition: no entry for the label yet.
        m.record_fetch("t", 0);
        assert_eq!(m.topic_fetch_requests.get_or_create(&lbl).get(), 1);
        assert_eq!(m.topic_bytes_out.get_or_create(&lbl).get(), 0);
    }

    #[test]
    fn record_produce_increments_both_counters() {
        let m = BrokerMetrics::new();
        let lbl = TopicLabel {
            topic: "t".to_string(),
        };
        m.record_produce("t", 1024);
        m.record_produce("t", 2048);
        assert_eq!(m.topic_produce_requests.get_or_create(&lbl).get(), 2);
        assert_eq!(m.topic_bytes_in.get_or_create(&lbl).get(), 3072);
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
        assert_eq!(m.partition_bytes_in.get_or_create(&lbl_p0).get(), 1024);
        assert_eq!(m.partition_bytes_in.get_or_create(&lbl_p1).get(), 512);
        assert_eq!(m.partition_bytes_out.get_or_create(&lbl_p0).get(), 2048);
        assert_eq!(m.partition_cpu_micros.get_or_create(&lbl_p0).get(), 500);
        assert_eq!(
            m.partition_disk_bytes.get_or_create(&lbl_p0).get(),
            1_000_000
        );
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
        assert_eq!(m.partition_bytes_in.get_or_create(&lbl).get(), 0);
        assert_eq!(m.partition_bytes_out.get_or_create(&lbl).get(), 0);
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
        assert_eq!(m.partition_cpu_micros.get_or_create(&lbl).get(), 0);
    }

    #[test]
    fn tiered_storage_rlmm_topic_backed_defaults_zero_and_can_be_set() {
        let m = BrokerMetrics::new();
        // Default for a fresh broker (in-memory placeholder, or no
        // tiered-storage at all) is `0`.
        assert_eq!(m.tiered_storage_rlmm_topic_backed.get(), 0);
        // The slice-48f bootstrap task bumps it to `1` after a successful
        // SwappableRlmm swap.
        m.tiered_storage_rlmm_topic_backed.set(1);
        assert_eq!(m.tiered_storage_rlmm_topic_backed.get(), 1);
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
        assert_eq!(
            m.produce_message_conversions.get_or_create(&orders).get(),
            2
        );
        assert_eq!(
            m.produce_message_conversions.get_or_create(&payments).get(),
            1
        );
        assert_eq!(m.fetch_message_conversions.get_or_create(&orders).get(), 1);
        assert_eq!(
            m.fetch_message_conversions.get_or_create(&payments).get(),
            2
        );
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
        assert_eq!(m.replication_bytes_in.get_or_create(&lbl3).get(), 4_000);
        assert_eq!(m.replication_bytes_in.get_or_create(&lbl4).get(), 100);
        assert_eq!(m.replication_bytes_out.get_or_create(&lbl3).get(), 4_000);
        assert_eq!(m.replication_bytes_out.get_or_create(&lbl4).get(), 0);
    }
}
