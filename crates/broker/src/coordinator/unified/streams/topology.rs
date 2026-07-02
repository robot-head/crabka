//! KIP-1071 topology resolution, task derivation, copartition validation, and
//! internal-topic management.
//!
//! A streams *topology* is a DAG of subtopologies. Each subtopology consumes
//! some external *source topics* (and/or regex-matched topics), may produce
//! *repartition sink* topics that another subtopology re-consumes as
//! *repartition source* topics, and maintains *state changelog* topics backing
//! its local stores. A *task* is `(subtopology_id, partition)`; the number of
//! tasks for a subtopology equals its partition count, which is derived from
//! the partition counts of the topics it reads.
//!
//! This module is pure-ish: every function except [`ensure_internal_topics`]
//! is synchronous and side-effect-free, taking a [`MetadataImage`] for topic
//! lookups. The coordinator drives the flow: ingest the client's topology into
//! a [`StreamsGroupTopologyValue`] ([`to_stored_topology`]), derive task counts
//! and the external-topic partition snapshot ([`derive_tasks`]), validate it
//! ([`validate_topology`]), materialize the internal topics it needs
//! ([`required_internal_topics`] + [`ensure_internal_topics`]), and report any
//! not-yet-satisfied conditions as a status list ([`validate_topology`] plus
//! the still-missing internal topics).

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crabka_metadata::{
    MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
};
// Alias the request wire module for readability.
use crabka_protocol::owned::streams_group_heartbeat_request as wire;
use crabka_raft::RaftError;
use uuid::Uuid;

use super::persistence::{
    StoredCopartitionGroup, StoredSubtopology, StoredTopicInfo, StreamsGroupPartitionMetadataValue,
    StreamsGroupTopologyValue, StreamsTopicMeta,
};
use crate::{error::BrokerError, metadata_source::MetadataSource};

// ---------------------------------------------------------------------------
// A. Request -> stored topology conversion
// ---------------------------------------------------------------------------

/// Convert a client-supplied [`wire::Topology`] (from a
/// `StreamsGroupHeartbeat` request) into the persisted
/// [`StreamsGroupTopologyValue`] the coordinator stores and reasons over.
///
/// This is a straight field-for-field map; `KeyValue` config pairs collapse to
/// `(String, String)` tuples and the per-subtopology `CopartitionGroup` /
/// `TopicInfo` structs become their `Stored*` analogs.
#[must_use]
pub fn to_stored_topology(t: &wire::Topology) -> StreamsGroupTopologyValue {
    StreamsGroupTopologyValue {
        epoch: t.epoch,
        subtopologies: t.subtopologies.iter().map(to_stored_subtopology).collect(),
    }
}

fn to_stored_subtopology(s: &wire::Subtopology) -> StoredSubtopology {
    StoredSubtopology {
        subtopology_id: s.subtopology_id.clone(),
        source_topics: s.source_topics.clone(),
        source_topic_regex: s.source_topic_regex.clone(),
        repartition_sink_topics: s.repartition_sink_topics.clone(),
        state_changelog_topics: s
            .state_changelog_topics
            .iter()
            .map(to_stored_topic_info)
            .collect(),
        repartition_source_topics: s
            .repartition_source_topics
            .iter()
            .map(to_stored_topic_info)
            .collect(),
        copartition_groups: s
            .copartition_groups
            .iter()
            .map(to_stored_copartition_group)
            .collect(),
    }
}

fn to_stored_topic_info(
    t: &crabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo,
) -> StoredTopicInfo {
    StoredTopicInfo {
        name: t.name.clone(),
        partitions: t.partitions,
        replication_factor: t.replication_factor,
        topic_configs: t
            .topic_configs
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect(),
    }
}

fn to_stored_copartition_group(c: &wire::CopartitionGroup) -> StoredCopartitionGroup {
    StoredCopartitionGroup {
        source_topics: c.source_topics.clone(),
        source_topic_regex: c.source_topic_regex.clone(),
        repartition_source_topics: c.repartition_source_topics.clone(),
    }
}

// ---------------------------------------------------------------------------
// B. Status codes (StreamsGroupHeartbeatResponse.Status)
// ---------------------------------------------------------------------------

/// KIP-1071 heartbeat-response status codes. A non-empty status list keeps the
/// member in a `NotReady` state until the offending condition clears.
// Byte values for the Kafka StreamsGroupHeartbeatResponse.Status enum.
pub mod status {
    pub const STALE_TOPOLOGY: i8 = 0;
    pub const MISSING_SOURCE_TOPICS: i8 = 1;
    pub const INCORRECTLY_PARTITIONED_TOPICS: i8 = 2;
    pub const MISSING_INTERNAL_TOPICS: i8 = 3;
    pub const SHUTDOWN_APPLICATION: i8 = 4;
}

// ---------------------------------------------------------------------------
// C. Task derivation
// ---------------------------------------------------------------------------

/// The result of resolving a topology against the current metadata image:
/// per-subtopology task counts (number of partitions) and the partition-count
/// snapshot of every external source topic that exists.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DerivedTasks {
    /// subtopology id -> number of tasks (partition count). Subtopologies whose
    /// count never resolves are absent from this map.
    pub num_tasks: BTreeMap<String, i32>,
    /// Partition metadata for the external source topics present in the image,
    /// to be persisted as the group's `StreamsGroupPartitionMetadataValue`.
    pub partition_metadata: StreamsGroupPartitionMetadataValue,
}

/// Derive task counts for every subtopology via a bounded fixpoint over the
/// topology DAG.
///
/// Seeding: a subtopology's external source topic present in `image`
/// contributes its `topic_partition_count`; a `repartition_source_topics` or
/// `state_changelog_topics` entry with an explicit `partitions > 0` contributes
/// that. Linkage: a `repartition_sink_topics` name in subtopology A is a
/// `repartition_source_topics` name in subtopology B, so once `num_tasks(A)` is
/// known the repartition topic carries `num_tasks(A)` partitions into B. We
/// iterate `subtopologies.len() + 1` times (or until a pass makes no change) to
/// propagate through chained repartitions. Subtopologies with no resolvable
/// input are left unresolved.
#[must_use]
pub fn derive_tasks(topology: &StreamsGroupTopologyValue, image: &MetadataImage) -> DerivedTasks {
    let mut num_tasks: BTreeMap<String, i32> = BTreeMap::new();

    // Map repartition-sink-topic name -> producing subtopology id, so we can
    // feed the producer's task count into the consumer once it resolves.
    let mut sink_producer: BTreeMap<&str, &str> = BTreeMap::new();
    for sub in &topology.subtopologies {
        for sink in &sub.repartition_sink_topics {
            sink_producer.insert(sink.as_str(), sub.subtopology_id.as_str());
        }
    }

    let max_iters = topology.subtopologies.len() + 1;
    for _ in 0..max_iters {
        let mut changed = false;
        for sub in &topology.subtopologies {
            let mut best: Option<i32> = num_tasks.get(&sub.subtopology_id).copied();

            // External source topics present in the image.
            for src in &sub.source_topics {
                if image.topic(src).is_some() {
                    let pc = image.topic_partition_count(src);
                    if pc > 0 {
                        best = Some(best.map_or(pc, |b| b.max(pc)));
                    }
                }
            }

            // Repartition-source topics: explicit count if given, else the
            // partition count of the upstream subtopology that produces them.
            for rs in &sub.repartition_source_topics {
                if rs.partitions > 0 {
                    best = Some(best.map_or(rs.partitions, |b| b.max(rs.partitions)));
                } else if let Some(&pc) = sink_producer
                    .get(rs.name.as_str())
                    .and_then(|producer| num_tasks.get(*producer))
                {
                    best = Some(best.map_or(pc, |b| b.max(pc)));
                }
            }

            // State-changelog topics with an explicit partition count.
            for cl in &sub.state_changelog_topics {
                if cl.partitions > 0 {
                    best = Some(best.map_or(cl.partitions, |b| b.max(cl.partitions)));
                }
            }

            if let Some(n) = best
                && num_tasks.get(&sub.subtopology_id) != Some(&n)
            {
                num_tasks.insert(sub.subtopology_id.clone(), n);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Snapshot every external source topic that exists in the image. De-dup by
    // name (a topic can be a source of more than one subtopology).
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut topics = Vec::new();
    for sub in &topology.subtopologies {
        for src in &sub.source_topics {
            if seen.contains(src.as_str()) {
                continue;
            }
            if let Some(rec) = image.topic(src) {
                seen.insert(src.as_str());
                topics.push(StreamsTopicMeta {
                    topic_name: src.clone(),
                    topic_id: rec.topic_id,
                    num_partitions: image.topic_partition_count(src),
                });
            }
        }
    }

    DerivedTasks {
        num_tasks,
        partition_metadata: StreamsGroupPartitionMetadataValue { topics },
    }
}

/// Expand per-subtopology task counts into the full universe of tasks: for each
/// subtopology, the partition list `0..num_tasks`.
#[must_use]
pub fn task_set(num_tasks: &BTreeMap<String, i32>) -> BTreeMap<String, Vec<i32>> {
    num_tasks
        .iter()
        .map(|(sub, &n)| (sub.clone(), (0..n.max(0)).collect()))
        .collect()
}

// ---------------------------------------------------------------------------
// D. Validation -> status list
// ---------------------------------------------------------------------------

/// Validate the topology against the metadata image, returning a list of
/// `(status_code, message)` pairs describing every unsatisfied condition. An
/// empty vec means the topology is fully ready (all source topics exist and all
/// copartition groups are consistently partitioned).
#[must_use]
pub fn validate_topology(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Vec<(i8, String)> {
    let mut out: Vec<(i8, String)> = Vec::new();

    for sub in &topology.subtopologies {
        // Missing source topics (exact names only; regex handled below).
        for src in &sub.source_topics {
            if image.topic(src).is_none() {
                out.push((
                    status::MISSING_SOURCE_TOPICS,
                    format!(
                        "subtopology '{}' references missing source topic '{}'",
                        sub.subtopology_id, src
                    ),
                ));
            }
        }

        // Regex source topics (`source_topic_regex`) are not resolved against
        // the metadata image here. They are treated as satisfiable; exact source
        // names still surface MISSING_SOURCE_TOPICS when absent.

        // Copartition groups: every member topic must have the same (known)
        // partition count. Indices map into this subtopology's topic arrays.
        for cg in &sub.copartition_groups {
            let mut counts: Vec<(String, i32)> = Vec::new();
            for &idx in &cg.source_topics {
                if let Some(name) = sub.source_topics.get(idx_to_usize(idx))
                    && image.topic(name).is_some()
                {
                    let pc = image.topic_partition_count(name);
                    if pc > 0 {
                        counts.push((name.clone(), pc));
                    }
                }
            }
            for &idx in &cg.repartition_source_topics {
                if let Some(info) = sub.repartition_source_topics.get(idx_to_usize(idx))
                    && info.partitions > 0
                {
                    counts.push((info.name.clone(), info.partitions));
                }
            }

            // If two resolvable members disagree, flag the group.
            if let Some((_, first)) = counts.first() {
                let first = *first;
                if let Some((bad_name, bad)) = counts.iter().find(|(_, c)| *c != first) {
                    out.push((
                        status::INCORRECTLY_PARTITIONED_TOPICS,
                        format!(
                            "subtopology '{}' copartition group has mismatched partition counts: \
                             expected {first}, but '{bad_name}' has {bad}",
                            sub.subtopology_id
                        ),
                    ));
                }
            }
        }
    }

    out
}

/// Map a copartition-group `i16` index (a non-negative array offset) to a
/// `usize`. Negative indices (never emitted by the client) clamp to `0`.
fn idx_to_usize(idx: i16) -> usize {
    usize::try_from(idx).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// E. Internal-topic specs + creation
// ---------------------------------------------------------------------------

/// A fully-resolved internal topic the coordinator must materialize:
/// repartition or changelog topic with its partition count, replication factor
/// (0 = "use the cluster default"), and config overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalTopicSpec {
    pub name: String,
    pub partitions: i32,
    /// Replication factor as requested by the client; `0` means "fall back to
    /// the cluster default" (chosen at creation time as `min(brokers, 3)`).
    pub replication_factor: i16,
    pub configs: BTreeMap<String, String>,
}

/// Compute the internal topics (repartition + changelog) the topology requires,
/// sized by the owning subtopology's derived task count. Changelog topics get
/// `cleanup.policy=compact`; repartition topics get `cleanup.policy=delete`
/// (each layered on top of any client-supplied configs). De-duplicated by name;
/// the first occurrence wins. Subtopologies whose task count has not resolved
/// contribute no specs (we can't size them yet).
#[must_use]
pub fn required_internal_topics(
    topology: &StreamsGroupTopologyValue,
    num_tasks: &BTreeMap<String, i32>,
) -> Vec<InternalTopicSpec> {
    let mut by_name: BTreeMap<String, InternalTopicSpec> = BTreeMap::new();

    for sub in &topology.subtopologies {
        let Some(&partitions) = num_tasks.get(&sub.subtopology_id) else {
            continue;
        };
        if partitions <= 0 {
            continue;
        }

        for info in &sub.repartition_source_topics {
            add_spec(&mut by_name, info, partitions, "delete");
        }
        for info in &sub.state_changelog_topics {
            add_spec(&mut by_name, info, partitions, "compact");
        }
    }

    by_name.into_values().collect()
}

fn add_spec(
    by_name: &mut BTreeMap<String, InternalTopicSpec>,
    info: &StoredTopicInfo,
    partitions: i32,
    cleanup_policy: &str,
) {
    if by_name.contains_key(&info.name) {
        return;
    }
    let mut configs: BTreeMap<String, String> = info.topic_configs.iter().cloned().collect();
    configs
        .entry("cleanup.policy".to_string())
        .or_insert_with(|| cleanup_policy.to_string());
    by_name.insert(
        info.name.clone(),
        InternalTopicSpec {
            name: info.name.clone(),
            partitions,
            replication_factor: info.replication_factor,
            configs,
        },
    );
}

/// Create any of `specs` not already present in the controller's metadata,
/// mirroring `crate::txn::bootstrap::ensure_topic`: round-robin replica
/// assignment, replication factor `spec.replication_factor` if `> 0` else
/// `min(brokers, 3)`, plus a `V1TopicConfig` record when the spec carries
/// configs. `TopicExists` (a concurrent create) is tolerated.
///
/// Returns the names of topics that are STILL absent from the (freshly
/// re-read) image after the attempt, so the caller can emit
/// `MISSING_INTERNAL_TOPICS` and keep the member `NotReady` until a later
/// heartbeat observes them.
pub async fn ensure_internal_topics(
    controller: &Arc<dyn MetadataSource>,
    specs: &[InternalTopicSpec],
) -> Result<Vec<String>, BrokerError> {
    let image = controller.current_image();

    // Round-robin replica assignment needs the registered broker set.
    let mut brokers: Vec<u64> = image.brokers().map(|b| b.node_id).collect();
    brokers.sort_unstable();

    for spec in specs {
        if image.topic(&spec.name).is_some() {
            continue;
        }
        if spec.partitions <= 0 {
            continue;
        }
        if brokers.is_empty() {
            return Err(BrokerError::Txn(format!(
                "no brokers registered; cannot create internal topic '{}'",
                spec.name
            )));
        }

        let k = brokers.len();
        let rf_usize = if spec.replication_factor > 0 {
            usize::try_from(spec.replication_factor)
                .expect("replication_factor non-negative")
                .min(k)
        } else {
            k.min(3)
        };
        let rf = i16::try_from(rf_usize).expect("rf <= brokers, fits i16");

        let mut records: Vec<MetadataRecord> = Vec::new();
        let topic_id = Uuid::new_v4();
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: spec.name.clone(),
            topic_id,
            partitions: spec.partitions,
            replication_factor: rf,
        }));

        for p in 0..spec.partitions {
            let mut replicas = Vec::with_capacity(rf_usize);
            let base = usize::try_from(p).expect("partition index fits in usize");
            for i in 0..rf_usize {
                replicas.push(brokers[(base + i) % k]);
            }
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: spec.name.clone(),
                partition: p,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }

        if !spec.configs.is_empty() {
            records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: spec.name.clone(),
                overrides: spec.configs.clone(),
            }));
        }

        match controller.submit_change(records).await {
            Ok(()) | Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => {}
            Err(e) => {
                return Err(BrokerError::Txn(format!(
                    "submit_change failed creating internal topic '{}': {e}",
                    spec.name
                )));
            }
        }
    }

    // Re-read the image; report whatever is still absent so the caller stays
    // NotReady until the create propagates.
    let after = controller.current_image();
    let still_missing = specs
        .iter()
        .filter(|s| after.topic(&s.name).is_none())
        .map(|s| s.name.clone())
        .collect();
    Ok(still_missing)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn topic_record(name: &str, id: u8, partitions: i32) -> TopicRecord {
        TopicRecord {
            name: name.to_string(),
            topic_id: Uuid::from_bytes([id; 16]),
            partitions,
            replication_factor: 1,
        }
    }

    /// Build an image with each `(name, id, partitions)` topic present, backed
    /// by `partitions` partition records (so `topic_partition_count` resolves).
    fn image_with(topics: &[(&str, u8, i32)]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for &(name, id, partitions) in topics {
            image.apply(&MetadataRecord::V1Topic(topic_record(name, id, partitions)));
            for p in 0..partitions {
                image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                    topic: name.to_string(),
                    partition: p,
                    leader: 1,
                    replicas: vec![1],
                    isr: vec![1],
                    leader_epoch: 0,
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                }));
            }
        }
        image
    }

    fn sub(id: &str) -> StoredSubtopology {
        StoredSubtopology {
            subtopology_id: id.to_string(),
            source_topics: vec![],
            source_topic_regex: vec![],
            repartition_sink_topics: vec![],
            state_changelog_topics: vec![],
            repartition_source_topics: vec![],
            copartition_groups: vec![],
        }
    }

    #[test]
    fn to_stored_topology_maps_all_fields() {
        use crabka_protocol::owned::common::streams_group_heartbeat_request::{
            key_value::KeyValue, topic_info::TopicInfo,
        };

        let wire_topology = wire::Topology {
            epoch: 9,
            subtopologies: vec![wire::Subtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in-a".into(), "in-b".into()],
                source_topic_regex: vec!["^orders-.*".into()],
                repartition_sink_topics: vec!["rp-1".into()],
                state_changelog_topics: vec![TopicInfo {
                    name: "store-changelog".into(),
                    partitions: 4,
                    replication_factor: 3,
                    topic_configs: vec![KeyValue {
                        key: "cleanup.policy".into(),
                        value: "compact".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                repartition_source_topics: vec![TopicInfo {
                    name: "rp-1".into(),
                    partitions: 4,
                    replication_factor: 3,
                    topic_configs: vec![],
                    ..Default::default()
                }],
                copartition_groups: vec![wire::CopartitionGroup {
                    source_topics: vec![0, 1],
                    source_topic_regex: vec![0],
                    repartition_source_topics: vec![0],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let stored = to_stored_topology(&wire_topology);
        assert!(
            stored
                == StreamsGroupTopologyValue {
                    epoch: 9,
                    subtopologies: vec![StoredSubtopology {
                        subtopology_id: "0".to_string(),
                        source_topics: vec!["in-a".to_string(), "in-b".to_string()],
                        source_topic_regex: vec!["^orders-.*".to_string()],
                        repartition_sink_topics: vec!["rp-1".to_string()],
                        state_changelog_topics: vec![StoredTopicInfo {
                            name: "store-changelog".to_string(),
                            partitions: 4,
                            replication_factor: 3,
                            topic_configs: vec![(
                                "cleanup.policy".to_string(),
                                "compact".to_string()
                            )],
                        }],
                        repartition_source_topics: vec![StoredTopicInfo {
                            name: "rp-1".to_string(),
                            partitions: 4,
                            replication_factor: 3,
                            topic_configs: vec![],
                        }],
                        copartition_groups: vec![StoredCopartitionGroup {
                            source_topics: vec![0, 1],
                            source_topic_regex: vec![0],
                            repartition_source_topics: vec![0],
                        }],
                    }],
                }
        );
    }

    #[test]
    fn derive_tasks_single_external_source() {
        let image = image_with(&[("in-a", 1, 6)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };

        let derived = derive_tasks(&topology, &image);
        assert!(
            derived
                == DerivedTasks {
                    num_tasks: BTreeMap::from([("0".to_string(), 6)]),
                    partition_metadata: StreamsGroupPartitionMetadataValue {
                        topics: vec![StreamsTopicMeta {
                            topic_name: "in-a".to_string(),
                            topic_id: Uuid::from_bytes([1; 16]),
                            num_partitions: 6,
                        }],
                    },
                }
        );
    }

    #[test]
    fn derive_tasks_repartition_chain() {
        // Subtopology 0 reads external "in-a" (3 partitions) and produces
        // repartition sink "rp". Subtopology 1 reads "rp" as a repartition
        // source with no explicit count, so it must inherit num_tasks(0) = 3.
        let image = image_with(&[("in-a", 1, 3)]);

        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        s0.repartition_sink_topics = vec!["rp".into()];

        let mut s1 = sub("1");
        s1.repartition_source_topics = vec![StoredTopicInfo {
            name: "rp".into(),
            partitions: 0, // unknown until upstream resolves
            replication_factor: 0,
            topic_configs: vec![],
        }];

        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0, s1],
        };

        let derived = derive_tasks(&topology, &image);
        // Only the external source topic appears in the partition snapshot.
        assert!(
            derived
                == DerivedTasks {
                    num_tasks: BTreeMap::from([("0".to_string(), 3), ("1".to_string(), 3)]),
                    partition_metadata: StreamsGroupPartitionMetadataValue {
                        topics: vec![StreamsTopicMeta {
                            topic_name: "in-a".to_string(),
                            topic_id: Uuid::from_bytes([1; 16]),
                            num_partitions: 3,
                        }],
                    },
                }
        );
    }

    #[test]
    fn derive_tasks_unresolved_subtopology_absent() {
        // No external source, no resolvable repartition input -> unresolved.
        let image = image_with(&[]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["missing".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let derived = derive_tasks(&topology, &image);
        assert!(!derived.num_tasks.contains_key("0"));
        assert!(derived.partition_metadata.topics.is_empty());
    }

    #[test]
    fn task_set_enumerates_zero_to_n() {
        let mut num_tasks = BTreeMap::new();
        num_tasks.insert("0".to_string(), 3);
        num_tasks.insert("1".to_string(), 0);
        let set = task_set(&num_tasks);
        assert!(set.get("0").unwrap() == &vec![0, 1, 2]);
        assert!(set.get("1").unwrap().is_empty());
    }

    #[test]
    fn validate_topology_ready_is_empty() {
        let image = image_with(&[("in-a", 1, 4)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        assert!(validate_topology(&topology, &image).is_empty());
    }

    #[test]
    fn validate_topology_flags_missing_source() {
        let image = image_with(&[]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let issues = validate_topology(&topology, &image);
        assert!(issues.len() == 1);
        check!(issues[0].0 == status::MISSING_SOURCE_TOPICS);
        check!(issues[0].1.contains("in-a"));
    }

    #[test]
    fn validate_topology_flags_copartition_mismatch() {
        // Two source topics with different partition counts in one copartition
        // group must flag INCORRECTLY_PARTITIONED_TOPICS.
        let image = image_with(&[("in-a", 1, 4), ("in-b", 2, 6)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into(), "in-b".into()];
        s0.copartition_groups = vec![StoredCopartitionGroup {
            source_topics: vec![0, 1],
            source_topic_regex: vec![],
            repartition_source_topics: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let issues = validate_topology(&topology, &image);
        assert!(
            issues
                .iter()
                .any(|(c, _)| *c == status::INCORRECTLY_PARTITIONED_TOPICS)
        );
    }

    #[test]
    fn validate_topology_copartition_match_ok() {
        let image = image_with(&[("in-a", 1, 4), ("in-b", 2, 4)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into(), "in-b".into()];
        s0.copartition_groups = vec![StoredCopartitionGroup {
            source_topics: vec![0, 1],
            source_topic_regex: vec![],
            repartition_source_topics: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        assert!(validate_topology(&topology, &image).is_empty());
    }

    #[test]
    fn required_internal_topics_sizes_and_configs() {
        let mut s0 = sub("0");
        s0.repartition_source_topics = vec![StoredTopicInfo {
            name: "rp".into(),
            partitions: 0,
            replication_factor: 2,
            topic_configs: vec![("segment.ms".into(), "100".into())],
        }];
        s0.state_changelog_topics = vec![StoredTopicInfo {
            name: "cl".into(),
            partitions: 0,
            replication_factor: 3,
            topic_configs: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let mut num_tasks = BTreeMap::new();
        num_tasks.insert("0".to_string(), 5);

        let specs = required_internal_topics(&topology, &num_tasks);
        assert!(specs.len() == 2);

        let rp = specs.iter().find(|s| s.name == "rp").unwrap();
        assert!(
            *rp == InternalTopicSpec {
                name: "rp".to_string(),
                partitions: 5,
                replication_factor: 2,
                configs: BTreeMap::from([
                    ("cleanup.policy".to_string(), "delete".to_string()),
                    ("segment.ms".to_string(), "100".to_string()),
                ]),
            }
        );

        let cl = specs.iter().find(|s| s.name == "cl").unwrap();
        assert!(
            *cl == InternalTopicSpec {
                name: "cl".to_string(),
                partitions: 5,
                replication_factor: 3,
                configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            }
        );
    }

    #[test]
    fn required_internal_topics_skips_unresolved_subtopology() {
        let mut s0 = sub("0");
        s0.repartition_source_topics = vec![StoredTopicInfo {
            name: "rp".into(),
            partitions: 0,
            replication_factor: 1,
            topic_configs: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        // No entry for subtopology "0" -> unresolved -> no specs.
        let specs = required_internal_topics(&topology, &BTreeMap::new());
        assert!(specs.is_empty());
    }
}
