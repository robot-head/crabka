//! `GroupTopics` and `application_id` → the byte-exact `StreamsGroupHeartbeat`
//! wire `Topology`.
//!
//! Every ordering rule here matches the JVM 4.x client.

use crabka_protocol::owned::{
    common::streams_group_heartbeat_request::{key_value::KeyValue, topic_info::TopicInfo},
    streams_group_heartbeat_request::{CopartitionGroup, Subtopology, Topology},
};
use crabka_units::prelude::*;
use serde::Serialize;

use super::grouping::GroupTopics;

/// `replication_factor` the JVM client sends for every internal topic.
///
/// `-1` means "use the broker's `replication.factor` default". This is the
/// KIP-1071 `StreamsGroupHeartbeat` `TopicInfo` convention.
const INTERNAL_TOPIC_DEFAULT_RF: i16 = -1;

/// `segment.bytes` the JVM 4.x client sets on a repartition topic (50 MiB). This
/// value lets the broker delete consumed segments quickly.
const REPARTITION_SEGMENT_SIZE: ByteSize = mebibytes(50);

/// Topic configs the JVM 4.x client attaches to a **repartition** topic, sorted
/// by key. That sort order is the wire array order the fixture pins.
///
/// A repartition topic keeps records only until a consumer reads them. The JVM
/// expresses this as `retention.ms=-1`. That value is the absence of a retention
/// bound and not an extent, so it renders through the `-1` wire sentinel.
fn repartition_topic_configs() -> Vec<KeyValue> {
    topic_configs([
        ("cleanup.policy", "delete".to_string()),
        ("message.timestamp.type", "CreateTime".to_string()),
        (
            "retention.ms",
            crabka_units::convert::wire::opt_time_to_millis_i64(None).to_string(),
        ),
        (
            "segment.bytes",
            REPARTITION_SEGMENT_SIZE.bytes_i64().to_string(),
        ),
    ])
}

/// Topic configs the JVM 4.x client attaches to a key/value-store **changelog**
/// topic, sorted by key.
fn changelog_topic_configs() -> Vec<KeyValue> {
    topic_configs([
        ("cleanup.policy", "compact".to_string()),
        ("message.timestamp.type", "CreateTime".to_string()),
    ])
}

/// Topic configs the JVM 4.x client attaches to a **windowed-store changelog**
/// topic.
///
/// The configs are the `compact,delete` policy and `retention.ms`, which make
/// sure the broker purges expired windows. The keys are in sorted order, the
/// same rule as the repartition configs.
///
/// This function renders `retention` as the raw millisecond integer that Kafka's
/// `retention.ms` takes. The key name and the string value are wire contract.
fn windowed_changelog_topic_configs(retention: Time) -> Vec<KeyValue> {
    vec![
        KeyValue {
            key: "cleanup.policy".into(),
            value: "compact,delete".into(),
            ..Default::default()
        },
        KeyValue {
            key: "message.timestamp.type".into(),
            value: "CreateTime".into(),
            ..Default::default()
        },
        KeyValue {
            key: "retention.ms".into(),
            value: retention.millis_i64().to_string(),
            ..Default::default()
        },
    ]
}

/// Versioned-store changelog topic configs (KIP-889).
///
/// The configs are `cleanup.policy=compact`,
/// `message.timestamp.type=CreateTime`, and `min.compaction.lag.ms`. Together
/// they let recent versions survive un-compacted until a restore reads them. The
/// keys are in sorted order, the same rule as the other changelog configs.
///
/// This function renders `min_compaction_lag` as the raw millisecond integer
/// that Kafka's `min.compaction.lag.ms` takes.
fn versioned_changelog_topic_configs(min_compaction_lag: Time) -> Vec<KeyValue> {
    vec![
        KeyValue {
            key: "cleanup.policy".into(),
            value: "compact".into(),
            ..Default::default()
        },
        KeyValue {
            key: "message.timestamp.type".into(),
            value: "CreateTime".into(),
            ..Default::default()
        },
        KeyValue {
            key: "min.compaction.lag.ms".into(),
            value: min_compaction_lag.millis_i64().to_string(),
            ..Default::default()
        },
    ]
}

/// Topic configs the JVM 4.x client attaches to a **join-window-store changelog**
/// topic.
///
/// The configs are the `delete`-only policy and `retention.ms`. Join window
/// stores use `retainDuplicates=true`, which prohibits compaction.
///
/// This function renders `retention` as the raw millisecond integer that Kafka's
/// `retention.ms` takes.
fn join_window_changelog_topic_configs(retention: Time) -> Vec<KeyValue> {
    vec![
        KeyValue {
            key: "cleanup.policy".into(),
            value: "delete".into(),
            ..Default::default()
        },
        KeyValue {
            key: "message.timestamp.type".into(),
            value: "CreateTime".into(),
            ..Default::default()
        },
        KeyValue {
            key: "retention.ms".into(),
            value: retention.millis_i64().to_string(),
            ..Default::default()
        },
    ]
}

/// Build the `KeyValue` config array from `(key, value)` pairs. The call site
/// gives the pairs in sorted order.
fn topic_configs<const N: usize>(pairs: [(&str, String); N]) -> Vec<KeyValue> {
    pairs
        .into_iter()
        .map(|(key, value)| KeyValue {
            key: key.to_string(),
            value,
            ..Default::default()
        })
        .collect()
}

/// Build the wire `Topology` at epoch 0, with sorted subtopologies and sorted
/// topic arrays.
pub(crate) fn to_wire(groups: &[GroupTopics], application_id: &str) -> Topology {
    let mut subtopologies: Vec<Subtopology> = groups
        .iter()
        .map(|g| subtopology(g, application_id))
        .collect();
    subtopologies.sort_by(|a, b| a.subtopology_id.cmp(&b.subtopology_id));
    Topology {
        epoch: 0,
        subtopologies,
        ..Default::default()
    }
}

fn subtopology(g: &GroupTopics, app: &str) -> Subtopology {
    let mut source_topics = g.source_topics.clone();
    source_topics.sort();
    let mut repartition_sink_topics = g.repartition_sink_topics.clone();
    repartition_sink_topics.sort();

    let mut repartition_source_topics: Vec<TopicInfo> = g
        .repartition_source_topics
        .iter()
        .map(|name| TopicInfo {
            name: name.clone(),
            partitions: 0,
            replication_factor: INTERNAL_TOPIC_DEFAULT_RF,
            topic_configs: repartition_topic_configs(),
            ..Default::default()
        })
        .collect();
    repartition_source_topics.sort_by(|a, b| a.name.cmp(&b.name));

    // The sorted repartition-topic *names*, in the same order as the TopicInfo
    // array above — copartition indices must point into these sorted arrays.
    let repartition_names: Vec<String> = repartition_source_topics
        .iter()
        .map(|t| t.name.clone())
        .collect();
    let copartition_groups = g
        .copartition_groups
        .iter()
        .map(|members| copartition_group(&source_topics, &repartition_names, members))
        .collect();

    let mut state_changelog_topics: Vec<TopicInfo> = g
        .changelog_stores
        .iter()
        .map(|(store, changelog_override, changelog_kind)| TopicInfo {
            // `REUSE_KTABLE_SOURCE_TOPICS`: when the store reuses its source
            // topic as the changelog, the override carries that topic name;
            // otherwise the JVM-default `<app>-<store>-changelog`.
            name: changelog_override
                .clone()
                .unwrap_or_else(|| format!("{app}-{store}-changelog")),
            partitions: 0,
            replication_factor: INTERNAL_TOPIC_DEFAULT_RF,
            topic_configs: match changelog_kind {
                crate::topology::node::ChangelogKind::Kv => changelog_topic_configs(),
                crate::topology::node::ChangelogKind::AggWindow { retention } => {
                    windowed_changelog_topic_configs(*retention)
                }
                crate::topology::node::ChangelogKind::JoinWindow { retention } => {
                    join_window_changelog_topic_configs(*retention)
                }
                crate::topology::node::ChangelogKind::Versioned { min_compaction_lag } => {
                    versioned_changelog_topic_configs(*min_compaction_lag)
                }
            },
            ..Default::default()
        })
        .collect();
    state_changelog_topics.sort_by(|a, b| a.name.cmp(&b.name));

    Subtopology {
        subtopology_id: g.id.clone(),
        source_topics,
        source_topic_regex: Vec::new(),
        state_changelog_topics,
        repartition_sink_topics,
        repartition_source_topics,
        copartition_groups,
        ..Default::default()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Serializable view of the wire `Topology`
// ──────────────────────────────────────────────────────────────────────────────

/// A serde-serializable view of the `StreamsGroupHeartbeat` wire `Topology`.
///
/// The tests use it to assert byte-exact interop against captured JVM fixtures.
///
/// The protocol `Topology` is auto-generated, and it carries
/// `unknown_tagged_fields` that the JVM JSON fixtures omit. This crate therefore
/// projects it onto these flat structs. Their `snake_case` field names carry no
/// `serde(rename_all)` and match the captured fixture shape exactly. Field
/// *order* does not matter, because the tests compare fixtures as
/// `serde_json::Value`, which is a key-sorted map. The topic and subtopology
/// array order is already fixed by
/// [`BuiltTopology::to_wire`](crate::topology::BuiltTopology::to_wire).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WireTopology {
    pub epoch: i32,
    pub subtopologies: Vec<WireSubtopology>,
}

/// One fixture-shaped subtopology in a [`WireTopology`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WireSubtopology {
    pub subtopology_id: String,
    pub source_topics: Vec<String>,
    pub source_topic_regex: Vec<String>,
    pub repartition_sink_topics: Vec<String>,
    pub repartition_source_topics: Vec<WireTopicInfo>,
    pub state_changelog_topics: Vec<WireTopicInfo>,
    pub copartition_groups: Vec<WireCopartitionGroup>,
}

/// An internal-topic descriptor for a repartition source or a state changelog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WireTopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: Vec<WireKeyValue>,
}

/// A topic-config key/value pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WireKeyValue {
    pub key: String,
    pub value: String,
}

/// A copartition group. It holds `int16` indices into the sorted source and
/// repartition arrays.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WireCopartitionGroup {
    pub source_topics: Vec<i16>,
    pub source_topic_regex: Vec<i16>,
    pub repartition_source_topics: Vec<i16>,
}

impl From<&Topology> for WireTopology {
    fn from(t: &Topology) -> Self {
        WireTopology {
            epoch: t.epoch,
            subtopologies: t.subtopologies.iter().map(WireSubtopology::from).collect(),
        }
    }
}

impl From<&Subtopology> for WireSubtopology {
    fn from(s: &Subtopology) -> Self {
        WireSubtopology {
            subtopology_id: s.subtopology_id.clone(),
            source_topics: s.source_topics.clone(),
            source_topic_regex: s.source_topic_regex.clone(),
            repartition_sink_topics: s.repartition_sink_topics.clone(),
            repartition_source_topics: s
                .repartition_source_topics
                .iter()
                .map(WireTopicInfo::from)
                .collect(),
            state_changelog_topics: s
                .state_changelog_topics
                .iter()
                .map(WireTopicInfo::from)
                .collect(),
            copartition_groups: s
                .copartition_groups
                .iter()
                .map(WireCopartitionGroup::from)
                .collect(),
        }
    }
}

impl From<&TopicInfo> for WireTopicInfo {
    fn from(t: &TopicInfo) -> Self {
        WireTopicInfo {
            name: t.name.clone(),
            partitions: t.partitions,
            replication_factor: t.replication_factor,
            topic_configs: t
                .topic_configs
                .iter()
                .map(|kv| WireKeyValue {
                    key: kv.key.clone(),
                    value: kv.value.clone(),
                })
                .collect(),
        }
    }
}

impl From<&CopartitionGroup> for WireCopartitionGroup {
    fn from(c: &CopartitionGroup) -> Self {
        WireCopartitionGroup {
            source_topics: c.source_topics.clone(),
            source_topic_regex: c.source_topic_regex.clone(),
            repartition_source_topics: c.repartition_source_topics.clone(),
        }
    }
}

/// Encode a copartition group as `int16` indices into the sorted `sources` and
/// `repartition` arrays.
///
/// The `subtopology()` builder calls this once per declared copartition group.
/// It passes the same sorted source and repartition arrays that it emits for the
/// wire `source_topics` and `repartition_source_topics` fields.
pub(crate) fn copartition_group(
    sources: &[String],
    repartition: &[String],
    members: &[String],
) -> CopartitionGroup {
    let mut source_topics = Vec::new();
    let mut repartition_source_topics = Vec::new();
    for m in members {
        if let Some(i) = sources.iter().position(|s| s == m) {
            source_topics.push(i16::try_from(i).unwrap_or(i16::MAX));
        } else if let Some(i) = repartition.iter().position(|s| s == m) {
            repartition_source_topics.push(i16::try_from(i).unwrap_or(i16::MAX));
        }
    }
    source_topics.sort_unstable();
    repartition_source_topics.sort_unstable();
    CopartitionGroup {
        source_topics,
        source_topic_regex: Vec::new(),
        repartition_source_topics,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::topology::{grouping::GroupTopics, node::ChangelogKind};

    #[test]
    fn wire_topology_serializes_to_fixture_shape_with_topic_info() {
        use crabka_protocol::owned::common::streams_group_heartbeat_request::key_value::KeyValue;
        // A subtopology whose changelog topic carries a config: exercises the
        // TopicInfo + KeyValue serde projection the stateless fixture omits.
        let proto = Topology {
            epoch: 0,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in".into()],
                state_changelog_topics: vec![TopicInfo {
                    name: "app-store-changelog".into(),
                    partitions: 0,
                    replication_factor: -1,
                    topic_configs: vec![KeyValue {
                        key: "cleanup.policy".into(),
                        value: "compact".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let view = WireTopology::from(&proto);
        let json = serde_json::to_value(&view).unwrap();
        // No `unknown_tagged_fields` leaks into the JSON, and the nested config
        // maps to `{key, value}`.
        check!(
            json["subtopologies"][0]["state_changelog_topics"][0]["name"] == "app-store-changelog"
        );
        check!(json["subtopologies"][0]["state_changelog_topics"][0]["replication_factor"] == -1);
        check!(
            json["subtopologies"][0]["state_changelog_topics"][0]["topic_configs"][0]["key"]
                == "cleanup.policy"
        );
        check!(
            json["subtopologies"][0]
                .get("unknown_tagged_fields")
                .is_none()
        );
        check!(
            json["subtopologies"][0]["state_changelog_topics"][0]
                .get("unknown_tagged_fields")
                .is_none()
        );
    }

    #[test]
    fn wire_copartition_group_projects_indices() {
        let proto = CopartitionGroup {
            source_topics: vec![0, 2],
            source_topic_regex: Vec::new(),
            repartition_source_topics: vec![1],
            ..Default::default()
        };
        let view = WireCopartitionGroup::from(&proto);
        check!(view.source_topics == vec![0i16, 2i16]);
        check!(view.repartition_source_topics == vec![1i16]);
        let json = serde_json::to_value(&view).unwrap();
        check!(json.get("unknown_tagged_fields").is_none());
    }

    #[test]
    fn epoch_is_zero_and_source_topics_sorted() {
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["b".into(), "a".into()],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        check!(topo.epoch == 0);
        check!(topo.subtopologies[0].source_topics == vec!["a".to_string(), "b".to_string()]);
        check!(topo.subtopologies[0].source_topic_regex.is_empty());
    }

    #[test]
    fn subtopologies_sort_by_id_as_string_not_numeric() {
        let groups = vec![
            GroupTopics {
                id: "2".into(),
                source_topics: vec!["x".into()],
                ..Default::default()
            },
            GroupTopics {
                id: "10".into(),
                source_topics: vec!["x".into()],
                ..Default::default()
            },
            GroupTopics {
                id: "1".into(),
                source_topics: vec!["x".into()],
                ..Default::default()
            },
        ];
        let topo = to_wire(&groups, "app");
        let ids: Vec<&str> = topo
            .subtopologies
            .iter()
            .map(|s| s.subtopology_id.as_str())
            .collect();
        check!(ids == vec!["1", "10", "2"]);
    }

    #[test]
    fn changelog_topics_named_zero_partitions_default_rf_and_configs() {
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec![("store".into(), None, ChangelogKind::Kv)],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "my-app");
        let cl = &topo.subtopologies[0].state_changelog_topics;
        check!(cl.len() == 1);
        check!(cl[0].name == "my-app-store-changelog");
        check!(cl[0].partitions == 0);
        // JVM-faithful internal-topic encoding: RF = -1 (broker default) and the
        // sorted KV-store changelog configs.
        check!(cl[0].replication_factor == -1);
        let configs: Vec<(&str, &str)> = cl[0]
            .topic_configs
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
            .collect();
        check!(
            configs
                == vec![
                    ("cleanup.policy", "compact"),
                    ("message.timestamp.type", "CreateTime"),
                ]
        );
    }

    #[test]
    fn changelog_override_uses_source_topic_name_verbatim() {
        // REUSE_KTABLE_SOURCE_TOPICS: the override makes the changelog topic the
        // source topic ("in"), not "my-app-store-changelog". Configs/RF stay the
        // standard KV-store changelog configs.
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec![("store".into(), Some("in".into()), ChangelogKind::Kv)],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "my-app");
        let cl = &topo.subtopologies[0].state_changelog_topics;
        check!(cl.len() == 1);
        check!(cl[0].name == "in");
        check!(cl[0].replication_factor == -1);
        let configs: Vec<(&str, &str)> = cl[0]
            .topic_configs
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
            .collect();
        check!(
            configs
                == vec![
                    ("cleanup.policy", "compact"),
                    ("message.timestamp.type", "CreateTime"),
                ]
        );
    }

    #[test]
    fn repartition_source_topics_carry_default_rf_and_sorted_configs() {
        let groups = vec![GroupTopics {
            id: "1".into(),
            repartition_source_topics: vec!["my-app-store-repartition".into()],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "my-app");
        let rp = &topo.subtopologies[0].repartition_source_topics;
        check!(rp.len() == 1);
        check!(rp[0].name == "my-app-store-repartition");
        check!(rp[0].partitions == 0);
        check!(rp[0].replication_factor == -1);
        let configs: Vec<(&str, &str)> = rp[0]
            .topic_configs
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
            .collect();
        check!(
            configs
                == vec![
                    ("cleanup.policy", "delete"),
                    ("message.timestamp.type", "CreateTime"),
                    ("retention.ms", "-1"),
                    ("segment.bytes", "52428800"),
                ]
        );
    }

    #[test]
    fn copartition_indices_point_into_sorted_arrays() {
        let sources = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let repartition: Vec<String> = vec![];
        let cg = copartition_group(&sources, &repartition, &["c".into(), "a".into()]);
        check!(cg.source_topics == vec![0i16, 2i16]);
        check!(cg.repartition_source_topics.is_empty());
        check!(cg.source_topic_regex.is_empty());
    }

    #[test]
    fn copartition_indices_into_repartition_array() {
        let sources = vec!["a".to_string()];
        let repartition = vec!["rp0".to_string(), "rp1".to_string()];
        let cg = copartition_group(&sources, &repartition, &["rp1".into(), "a".into()]);
        check!(cg.source_topics == vec![0i16]);
        check!(cg.repartition_source_topics == vec![1i16]);
    }

    #[test]
    fn windowed_store_changelog_config_is_compact_delete_with_retention() {
        // size=60s, grace=0 → retention = 60_000 + 0 + 86_400_000 = 86_460_000 ms
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec![(
                "w".into(),
                None,
                ChangelogKind::AggWindow {
                    retention: Time::from_millis(86_460_000),
                },
            )],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        let cl = &topo.subtopologies[0].state_changelog_topics;
        check!(cl.len() == 1);
        check!(cl[0].name == "app-w-changelog");
        check!(cl[0].partitions == 0);
        check!(cl[0].replication_factor == -1);
        let configs: Vec<(&str, &str)> = cl[0]
            .topic_configs
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
            .collect();
        check!(
            configs
                == vec![
                    ("cleanup.policy", "compact,delete"),
                    ("message.timestamp.type", "CreateTime"),
                    ("retention.ms", "86460000"),
                ]
        );
    }

    #[test]
    fn kv_store_changelog_config_unchanged_after_windowed_change() {
        // KV store must still use compact-only config (golden frames must stay byte-identical)
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec![("store".into(), None, ChangelogKind::Kv)],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "my-app");
        let cl = &topo.subtopologies[0].state_changelog_topics;
        check!(cl.len() == 1);
        let configs: Vec<(&str, &str)> = cl[0]
            .topic_configs
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
            .collect();
        check!(
            configs
                == vec![
                    ("cleanup.policy", "compact"),
                    ("message.timestamp.type", "CreateTime"),
                ]
        );
    }

    #[test]
    fn copartition_unknown_member_is_silently_skipped() {
        let sources = vec!["a".to_string()];
        let repartition: Vec<String> = vec![];
        let cg = copartition_group(&sources, &repartition, &["unknown".into()]);
        check!(cg.source_topics.is_empty());
        check!(cg.repartition_source_topics.is_empty());
    }

    #[test]
    fn join_window_changelog_is_delete_only_with_retention() {
        use crate::topology::node::ChangelogKind;
        // before=60s, after=60s, grace=0 → retention = 60_000 + 60_000 + 0 +
        // 86_400_000 = 86_520_000 ms
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec![(
                "j".into(),
                None,
                ChangelogKind::JoinWindow {
                    retention: Time::from_millis(86_520_000),
                },
            )],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        let cl = &topo.subtopologies[0].state_changelog_topics[0];
        check!(cl.name == "app-j-changelog");
        check!(cl.topic_configs[0].key == "cleanup.policy");
        check!(cl.topic_configs[0].value == "delete"); // NOT compact,delete
        check!(cl.topic_configs[1].key == "message.timestamp.type");
        check!(cl.topic_configs[2].key == "retention.ms");
        check!(cl.topic_configs[2].value == "86520000");
    }

    #[test]
    fn versioned_store_changelog_config_is_compact_with_min_compaction_lag() {
        let cfgs = versioned_changelog_topic_configs(Time::from_millis(686_400_000));
        let get = |k: &str| cfgs.iter().find(|c| c.key == k).map(|c| c.value.clone());
        check!(get("cleanup.policy").as_deref() == Some("compact"));
        check!(get("message.timestamp.type").as_deref() == Some("CreateTime"));
        check!(get("min.compaction.lag.ms").as_deref() == Some("686400000"));
    }

    /// Every changelog flavour's rendered `(key, value)` config array, pinned
    /// against the exact strings Kafka's topic configs take.
    ///
    /// The quantity lives in `ChangelogKind`. Only this rendering step turns it
    /// back into a millisecond integer, and a scale slip here would silently
    /// change how long the broker retains a changelog.
    #[test]
    fn changelog_kinds_render_kafka_config_strings_verbatim() {
        let cases = [
            (
                ChangelogKind::Kv,
                vec![
                    ("cleanup.policy", "compact"),
                    ("message.timestamp.type", "CreateTime"),
                ],
            ),
            (
                ChangelogKind::AggWindow { retention: days(7) },
                vec![
                    ("cleanup.policy", "compact,delete"),
                    ("message.timestamp.type", "CreateTime"),
                    ("retention.ms", "604800000"),
                ],
            ),
            (
                ChangelogKind::JoinWindow {
                    retention: minutes(90),
                },
                vec![
                    ("cleanup.policy", "delete"),
                    ("message.timestamp.type", "CreateTime"),
                    ("retention.ms", "5400000"),
                ],
            ),
            (
                ChangelogKind::Versioned {
                    min_compaction_lag: millis(1),
                },
                vec![
                    ("cleanup.policy", "compact"),
                    ("message.timestamp.type", "CreateTime"),
                    ("min.compaction.lag.ms", "1"),
                ],
            ),
        ];

        for (kind, expected) in cases {
            let groups = vec![GroupTopics {
                id: "0".into(),
                source_topics: vec!["in".into()],
                changelog_stores: vec![("s".into(), None, kind)],
                ..Default::default()
            }];
            let topo = to_wire(&groups, "app");
            let rendered: Vec<(&str, &str)> = topo.subtopologies[0].state_changelog_topics[0]
                .topic_configs
                .iter()
                .map(|kv| (kv.key.as_str(), kv.value.as_str()))
                .collect();
            check!(rendered == expected, "{kind:?}");
        }
    }

    /// Repartition topics carry a fixed config array. It includes a
    /// `segment.bytes` that must stay the byte count the JVM client sends, and
    /// not a rounded or rescaled one.
    #[test]
    fn repartition_topic_configs_render_verbatim() {
        let configs = repartition_topic_configs();
        let rendered: Vec<(&str, &str)> = configs
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
            .collect();
        check!(
            rendered
                == vec![
                    ("cleanup.policy", "delete"),
                    ("message.timestamp.type", "CreateTime"),
                    ("retention.ms", "-1"),
                    ("segment.bytes", "52428800"),
                ]
        );
    }

    #[test]
    fn repartition_sink_and_source_topics_included_in_wire() {
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            repartition_sink_topics: vec!["rp".into()],
            repartition_source_topics: vec!["rp".into()],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        let st = &topo.subtopologies[0];
        check!(st.repartition_sink_topics == vec!["rp".to_string()]);
        check!(st.repartition_source_topics.len() == 1);
        check!(st.repartition_source_topics[0].name == "rp");
    }
}
