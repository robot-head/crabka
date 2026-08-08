//! A port of `InternalTopologyBuilder.makeNodeGroups`.
//!
//! A union-find pass over the node graph, in insertion order, gives each
//! subtopology its integer index, and the code renders that index as a decimal
//! string. A group with no source topics is dropped but still takes an index, so
//! the ids can be non-contiguous.

use std::collections::HashMap;

use super::node::{ChangelogKind, NodeKind, NodeRegistry};

/// One subtopology's resolved topic sets, keyed by its decimal-string id.
#[derive(Debug, Clone, Default)]
pub(crate) struct GroupTopics {
    pub id: String,
    /// The external source topics. The wire layer sorts them later.
    pub source_topics: Vec<String>,
    /// Internal repartition topics this subtopology reads.
    pub repartition_source_topics: Vec<String>,
    /// Internal repartition topics this subtopology writes.
    pub repartition_sink_topics: Vec<String>,
    /// The stores whose changelog topics back this subtopology, as
    /// `(store_name, changelog_override, changelog_kind)`.
    ///
    /// When the override is `None`, the wire layer derives
    /// `<app>-<store>-changelog`. When it is `Some(topic)`, which the
    /// `REUSE_KTABLE_SOURCE_TOPICS` pass sets, the wire layer uses that topic
    /// name verbatim. `changelog_kind` selects the topic config: `compact`,
    /// `compact,delete`, or `delete`.
    pub changelog_stores: Vec<(String, Option<String>, ChangelogKind)>,
    /// The declared copartition groups whose member topics all read into this
    /// subtopology.
    ///
    /// Each group is a list of member topic names. The wire layer maps them to
    /// `int16` indices into the sorted source and repartition arrays.
    pub copartition_groups: Vec<Vec<String>>,
}

/// A minimal quick-union over `usize` node indices, with a path-compressing
/// find.
struct QuickUnion {
    parent: Vec<usize>,
}

impl QuickUnion {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
        }
    }
    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }
    fn unite(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent[ra] = rb;
        }
    }
}

/// Group the registry's nodes into subtopologies with JVM-matching ids.
pub(crate) fn group_nodes(reg: &NodeRegistry) -> Vec<GroupTopics> {
    let n = reg.nodes.len();
    let mut uf = QuickUnion::new(n);

    for (i, node) in reg.nodes.iter().enumerate() {
        let preds = match &node.kind {
            NodeKind::Processor { predecessors } | NodeKind::Sink { predecessors, .. } => {
                predecessors
            }
            NodeKind::Source { .. } => continue,
        };
        for p in preds {
            if let Some(&j) = reg.index.get(p) {
                uf.unite(i, j);
            }
        }
    }
    for store in &reg.stores {
        let mut iter = store
            .processors
            .iter()
            .filter_map(|p| reg.index.get(p).copied());
        if let Some(first) = iter.next() {
            for other in iter {
                uf.unite(first, other);
            }
        }
    }

    let mut root_to_id: HashMap<usize, usize> = HashMap::new();
    let mut next_id = 0usize;
    let mut groups: HashMap<usize, GroupTopics> = HashMap::new();
    let mut order: Vec<usize> = Vec::new();

    for i in 0..n {
        let root = uf.find(i);
        let id = *root_to_id.entry(root).or_insert_with(|| {
            let id = next_id;
            next_id += 1;
            order.push(id);
            id
        });
        let entry = groups.entry(id).or_insert_with(|| GroupTopics {
            id: id.to_string(),
            ..Default::default()
        });
        match &reg.nodes[i].kind {
            NodeKind::Source { topics } => {
                for t in topics {
                    if reg.global_source_topics.contains(t) {
                        // GlobalKTable source: invisible in the wire. The node still
                        // consumed this group's index above, so downstream
                        // subtopology ids shift; the now-source-less group is dropped
                        // by the final filter.
                        continue;
                    }
                    if reg.repartition_topics.contains(t) {
                        entry.repartition_source_topics.push(t.clone());
                    } else {
                        entry.source_topics.push(t.clone());
                    }
                }
            }
            NodeKind::Sink { topic, .. } => {
                if reg.repartition_topics.contains(topic) {
                    entry.repartition_sink_topics.push(topic.clone());
                }
            }
            NodeKind::Processor { .. } => {}
        }
    }
    for store in &reg.stores {
        if let Some(&first) = store.processors.first().and_then(|p| reg.index.get(p)) {
            let root = uf.find(first);
            if let Some(&id) = root_to_id.get(&root)
                && let Some(g) = groups.get_mut(&id)
            {
                g.changelog_stores.push((
                    store.name.clone(),
                    store.changelog_override.clone(),
                    store.changelog_kind,
                ));
            }
        }
    }

    // Assign each declared copartition group to the subtopology that reads all of
    // its member topics. By construction all members of a group land in one
    // subtopology, so the first group whose source ∪ repartition-source set
    // contains every member owns it.
    for members in &reg.copartition_groups {
        if let Some(g) = groups.values_mut().find(|g| {
            members
                .iter()
                .all(|m| g.source_topics.contains(m) || g.repartition_source_topics.contains(m))
        }) {
            g.copartition_groups.push(members.clone());
        }
    }

    order
        .into_iter()
        .filter_map(|id| groups.remove(&id))
        .filter(|g| !g.source_topics.is_empty() || !g.repartition_source_topics.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::topology::node::{ChangelogKind, NodeRegistry};

    fn ids(groups: &[GroupTopics]) -> Vec<&str> {
        groups.iter().map(|g| g.id.as_str()).collect()
    }

    #[test]
    fn single_source_sink_is_one_subtopology() {
        let mut reg = NodeRegistry::default();
        reg.add_source("src", vec!["in".into()]).unwrap();
        reg.add_sink("snk", "out".into(), vec!["src".into()])
            .unwrap();
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["0"]);
        check!(groups[0].source_topics == vec!["in".to_string()]);
    }

    #[test]
    fn repartition_chain_is_two_subtopologies() {
        let mut reg = NodeRegistry::default();
        reg.repartition_topics.insert("rp".into());
        reg.add_source("src", vec!["in".into()]).unwrap();
        reg.add_sink("rsink", "rp".into(), vec!["src".into()])
            .unwrap();
        reg.add_source("rsrc", vec!["rp".into()]).unwrap();
        reg.add_sink("snk2", "out".into(), vec!["rsrc".into()])
            .unwrap();
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["0", "1"]);
        check!(groups[0].source_topics == vec!["in".to_string()]);
        check!(groups[0].repartition_sink_topics == vec!["rp".to_string()]);
        check!(groups[1].repartition_source_topics == vec!["rp".to_string()]);
    }

    #[test]
    fn shared_state_store_unites_processors_into_one_group() {
        let mut reg = NodeRegistry::default();
        reg.add_source("s1", vec!["a".into()]).unwrap();
        reg.add_processor("p1", vec!["s1".into()]).unwrap();
        reg.add_source("s2", vec!["b".into()]).unwrap();
        reg.add_processor("p2", vec!["s2".into()]).unwrap();
        reg.add_store("store", vec!["p1".into(), "p2".into()], None);
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["0"]);
        let mut srcs = groups[0].source_topics.clone();
        srcs.sort();
        check!(srcs == vec!["a".to_string(), "b".to_string()]);
        check!(groups[0].changelog_stores == vec![("store".to_string(), None, ChangelogKind::Kv)]);
    }

    #[test]
    fn source_less_group_is_dropped_but_consumes_an_index() {
        let mut reg = NodeRegistry::default();
        reg.add_processor("orphan_proc", vec![]).unwrap();
        reg.add_sink("orphan_sink", "x".into(), vec!["orphan_proc".into()])
            .unwrap();
        reg.add_source("src", vec!["in".into()]).unwrap();
        reg.add_sink("snk", "out".into(), vec!["src".into()])
            .unwrap();
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["1"]);
    }

    #[test]
    fn global_source_is_invisible_but_consumes_an_index() {
        // A GlobalKTable's source node, declared FIRST, takes node-group index 0.
        // Its topic is marked global, so it is skipped in the source-bucketing
        // pass — leaving its group source-less. The final filter drops that group
        // (it already consumed index 0), so the normal stream subtopology emits as
        // "1". The global topic appears in NO emitted subtopology.
        let mut reg = NodeRegistry::default();
        // Global source first → index 0.
        reg.add_source("gsrc", vec!["global".into()]).unwrap();
        reg.add_processor("gproc", vec!["gsrc".into()]).unwrap();
        reg.add_global_source("global");
        // Normal stream second → index 1.
        reg.add_source("src", vec!["in".into()]).unwrap();
        reg.add_sink("snk", "out".into(), vec!["src".into()])
            .unwrap();
        let groups = group_nodes(&reg);
        check!(groups.len() == 1);
        check!(ids(&groups) == vec!["1"]);
        check!(groups[0].source_topics == vec!["in".to_string()]);
    }
}
