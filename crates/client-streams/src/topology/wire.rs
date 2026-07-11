//! `GroupTopics` + `application_id` → the byte-exact `StreamsGroupHeartbeat`
//! wire `Topology`. Every ordering rule here matches the JVM 4.x client.

use crabka_protocol::owned::{
    common::streams_group_heartbeat_request::{key_value::KeyValue, topic_info::TopicInfo},
    streams_group_heartbeat_request::{CopartitionGroup, Subtopology, Topology},
};
use serde::Serialize;

use super::grouping::GroupTopics;

/// `replication_factor` the JVM client sends for every internal topic: `-1`
/// means "use the broker's `replication.factor` default" (KIP-1071 / the
/// `StreamsGroupHeartbeat` `TopicInfo` convention).
const INTERNAL_TOPIC_DEFAULT_RF: i16 = -1;

/// Topic configs the JVM 4.x client attaches to a **repartition** topic, sorted
/// by key (the wire array order the fixture pins).
fn repartition_topic_configs() -> Vec<KeyValue> {
    topic_configs([
        ("cleanup.policy", "delete"),
        ("message.timestamp.type", "CreateTime"),
        ("retention.ms", "-1"),
        ("segment.bytes", "52428800"),
    ])
}

/// Topic configs the JVM 4.x client attaches to a key/value-store **changelog**
/// topic, sorted by key.
fn changelog_topic_configs() -> Vec<KeyValue> {
    topic_configs([
        ("cleanup.policy", "compact"),
        ("message.timestamp.type", "CreateTime"),
    ])
}

/// Topic configs the JVM 4.x client attaches to a **windowed-store changelog**
/// topic: `compact,delete` policy + `retention.ms` to ensure expired windows are
/// actually purged. Keys are in sorted order (same rule as repartition configs).
fn windowed_changelog_topic_configs(retention_ms: i64) -> Vec<KeyValue> {
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
            value: retention_ms.to_string(),
            ..Default::default()
        },
    ]
}

/// Versioned-store changelog topic configs (KIP-889): `cleanup.policy=compact` +
/// `message.timestamp.type=CreateTime` + `min.compaction.lag.ms` so recent
/// versions survive (un-compacted) until restore reads them. Keys are in sorted
/// order (same rule as the other changelog configs).
fn versioned_changelog_topic_configs(min_compaction_lag_ms: i64) -> Vec<KeyValue> {
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
            value: min_compaction_lag_ms.to_string(),
            ..Default::default()
        },
    ]
}

/// Topic configs the JVM 4.x client attaches to a **join-window-store changelog**
/// topic: `delete`-only policy + `retention.ms`. Join window stores use
/// `retainDuplicates=true`, which prohibits compaction.
fn join_window_changelog_topic_configs(retention_ms: i64) -> Vec<KeyValue> {
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
            value: retention_ms.to_string(),
            ..Default::default()
        },
    ]
}

/// Build the `KeyValue` config array from `(key, value)` pairs (already in
/// sorted order at the call site).
fn topic_configs<const N: usize>(pairs: [(&str, &str); N]) -> Vec<KeyValue> {
    pairs
        .into_iter()
        .map(|(key, value)| KeyValue {
            key: key.to_string(),
            value: value.to_string(),
            ..Default::default()
        })
        .collect()
}

/// Build the wire `Topology` (epoch 0, sorted subtopologies + topic arrays).
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
                crate::topology::node::ChangelogKind::AggWindow { retention_ms } => {
                    windowed_changelog_topic_configs(*retention_ms)
                }
                crate::topology::node::ChangelogKind::JoinWindow { retention_ms } => {
                    join_window_changelog_topic_configs(*retention_ms)
                }
                crate::topology::node::ChangelogKind::Versioned {
                    min_compaction_lag_ms,
                } => versioned_changelog_topic_configs(*min_compaction_lag_ms),
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

/// A serde-serializable view of the `StreamsGroupHeartbeat` wire `Topology`,
/// used to assert byte-exact interop against captured JVM fixtures.
///
/// The protocol `Topology` is auto-generated (and carries `unknown_tagged_fields`
/// that the JVM JSON fixtures omit), so we project it onto these flat structs
/// whose `serde(rename_all)`-free `snake_case` field names match the captured
/// fixture shape exactly. Field *order* is irrelevant — fixtures are compared as
/// `serde_json::Value` (a key-sorted map), and topic/subtopology array order is
/// already fixed by [`BuiltTopology::to_wire`](crate::topology::BuiltTopology::to_wire).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WireTopology {
    pub epoch: i32,
    pub subtopologies: Vec<WireSubtopology>,
}

/// One subtopology in a [`WireTopology`] (fixture-shaped).
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

/// An internal-topic descriptor (repartition source / state changelog).
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

/// A copartition group: `int16` indices into the sorted source / repartition
/// arrays.
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

/// Encode a copartition group as `int16` indices into the sorted `sources` /
/// `repartition` arrays. The `subtopology()` builder calls this once per declared
/// copartition group, passing the same sorted source/repartition arrays it emits
/// for the wire `source_topics` / `repartition_source_topics` fields.
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
            json == serde_json::json!({
                "epoch": 0,
                "subtopologies": [{
                    "subtopology_id": "0",
                    "source_topics": ["in"],
                    "source_topic_regex": [],
                    "repartition_sink_topics": [],
                    "repartition_source_topics": [],
                    "state_changelog_topics": [{
                        "name": "app-store-changelog",
                        "partitions": 0,
                        "replication_factor": -1,
                        "topic_configs": [{
                            "key": "cleanup.policy",
                            "value": "compact"
                        }]
                    }],
                    "copartition_groups": []
                }]
            })
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
        check!(
            (
                view.source_topics.as_slice(),
                view.repartition_source_topics.as_slice()
            ) == (&[0i16, 2][..], &[1i16][..])
        );
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
        check!(
            (
                topo.epoch,
                topo.subtopologies[0].source_topics.as_slice(),
                topo.subtopologies[0].source_topic_regex.as_slice(),
            ) == (0, &["a".to_string(), "b".to_string()][..], &[][..])
        );
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
        check!(
            (
                cl.len(),
                cl[0].name.as_str(),
                cl[0].partitions,
                cl[0].replication_factor,
            ) == (1, "my-app-store-changelog", 0, -1)
        );
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
        check!((cl.len(), cl[0].name.as_str(), cl[0].replication_factor) == (1, "in", -1));
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
        check!(
            (
                rp.len(),
                rp[0].name.as_str(),
                rp[0].partitions,
                rp[0].replication_factor
            ) == (1, "my-app-store-repartition", 0, -1)
        );
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
        check!(
            (
                cg.source_topics.as_slice(),
                cg.repartition_source_topics.as_slice(),
                cg.source_topic_regex.as_slice(),
            ) == (&[0i16, 2][..], &[][..], &[][..])
        );
    }

    #[test]
    fn copartition_indices_into_repartition_array() {
        let sources = vec!["a".to_string()];
        let repartition = vec!["rp0".to_string(), "rp1".to_string()];
        let cg = copartition_group(&sources, &repartition, &["rp1".into(), "a".into()]);
        check!(
            (
                cg.source_topics.as_slice(),
                cg.repartition_source_topics.as_slice()
            ) == (&[0i16][..], &[1i16][..])
        );
    }

    #[test]
    fn windowed_store_changelog_config_is_compact_delete_with_retention() {
        // size=60_000ms, grace=0ms → retention = 60_000 + 0 + 86_400_000 = 86_460_000
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec![(
                "w".into(),
                None,
                ChangelogKind::AggWindow {
                    retention_ms: 86_460_000,
                },
            )],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        let cl = &topo.subtopologies[0].state_changelog_topics;
        check!(
            (
                cl.len(),
                cl[0].name.as_str(),
                cl[0].partitions,
                cl[0].replication_factor
            ) == (1, "app-w-changelog", 0, -1)
        );
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
        check!(
            (
                cg.source_topics.as_slice(),
                cg.repartition_source_topics.as_slice()
            ) == (&[][..], &[][..])
        );
    }

    #[test]
    fn join_window_changelog_is_delete_only_with_retention() {
        use crate::topology::node::ChangelogKind;
        // before=60_000ms, after=60_000ms, grace=0ms → retention = 60_000 + 60_000 + 0 + 86_400_000 = 86_520_000
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec![(
                "j".into(),
                None,
                ChangelogKind::JoinWindow {
                    retention_ms: 86_520_000,
                },
            )],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        let cl = &topo.subtopologies[0].state_changelog_topics[0];
        assert2::assert!(cl.name.as_str() == "app-j-changelog");
        assert2::assert!(
            cl.topic_configs
                .iter()
                .map(|c| (c.key.as_str(), c.value.as_str()))
                .collect::<Vec<_>>()
                == vec![
                    ("cleanup.policy", "delete"),
                    ("message.timestamp.type", "CreateTime"),
                    ("retention.ms", "86520000"),
                ]
        );
    }

    #[test]
    fn versioned_store_changelog_config_is_compact_with_min_compaction_lag() {
        let cfgs = versioned_changelog_topic_configs(686_400_000);
        assert2::assert!(
            cfgs.iter()
                .map(|cfg| (cfg.key.as_str(), cfg.value.as_str()))
                .collect::<Vec<_>>()
                == vec![
                    ("cleanup.policy", "compact"),
                    ("message.timestamp.type", "CreateTime"),
                    ("min.compaction.lag.ms", "686400000"),
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
        check!(
            (
                st.repartition_sink_topics.as_slice(),
                st.repartition_source_topics.len(),
                st.repartition_source_topics[0].name.as_str(),
            ) == (&["rp".to_string()][..], 1, "rp")
        );
    }
}
