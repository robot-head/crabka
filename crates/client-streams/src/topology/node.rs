//! Insertion-ordered processor-node graph: the structural input the JVM's
//! `makeNodeGroups` operates on. Order is load-bearing — it determines
//! subtopology indices.

use std::collections::{HashMap, HashSet};

use super::builder::TopologyError;

/// What a node is and which topics/predecessors it touches.
#[derive(Debug, Clone)]
pub(crate) enum NodeKind {
    Source {
        topics: Vec<String>,
    },
    Processor {
        predecessors: Vec<String>,
    },
    Sink {
        topic: String,
        predecessors: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub name: String,
    pub kind: NodeKind,
}

/// A registered state store: its name, the processors it connects (used to
/// union the owning subtopology), and an optional **changelog-topic override**.
///
/// The override is `None` for the default case — the changelog topic name is
/// then derived as `<app_id>-<store_name>-changelog`. The DSL
/// `REUSE_KTABLE_SOURCE_TOPICS` optimizer sets it to the table's *source* topic
/// so the materialized store reuses that topic as its changelog (no separate
/// `app-<store>-changelog` topic is created).
#[derive(Debug, Clone)]
pub(crate) struct StoreEntry {
    pub name: String,
    pub processors: Vec<String>,
    pub changelog_override: Option<String>,
}

/// The full node graph, recorded in insertion order.
#[derive(Debug, Default)]
pub(crate) struct NodeRegistry {
    pub nodes: Vec<Node>,
    pub index: HashMap<String, usize>,
    /// One registered state store, in insertion order.
    pub stores: Vec<StoreEntry>,
    /// Topic names registered as internal repartition topics.
    pub repartition_topics: HashSet<String>,
    /// Declared copartition groups: each a list of member topic names that must
    /// share a partitioning (required for joins). The grouping pass assigns each
    /// group to the subtopology containing its members.
    pub copartition_groups: Vec<Vec<String>>,
}

impl NodeRegistry {
    fn insert(&mut self, node: Node) -> Result<(), TopologyError> {
        if self.index.contains_key(&node.name) {
            return Err(TopologyError::DuplicateNode(node.name));
        }
        self.index.insert(node.name.clone(), self.nodes.len());
        self.nodes.push(node);
        Ok(())
    }

    /// Reject any predecessor name not already present in the registry.
    /// Called at add time to guarantee a DAG by construction — a forward
    /// reference (including a self-reference) makes a cycle possible.
    fn require_predecessors_exist(
        &self,
        node: &str,
        predecessors: &[String],
    ) -> Result<(), TopologyError> {
        for p in predecessors {
            if !self.index.contains_key(p) {
                return Err(TopologyError::UnknownPredecessor {
                    node: node.to_string(),
                    predecessor: p.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn add_source(&mut self, name: &str, topics: Vec<String>) -> Result<(), TopologyError> {
        self.insert(Node {
            name: name.to_string(),
            kind: NodeKind::Source { topics },
        })
    }

    pub fn add_processor(
        &mut self,
        name: &str,
        predecessors: Vec<String>,
    ) -> Result<(), TopologyError> {
        self.require_predecessors_exist(name, &predecessors)?;
        self.insert(Node {
            name: name.to_string(),
            kind: NodeKind::Processor { predecessors },
        })
    }

    pub fn add_sink(
        &mut self,
        name: &str,
        topic: String,
        predecessors: Vec<String>,
    ) -> Result<(), TopologyError> {
        self.require_predecessors_exist(name, &predecessors)?;
        self.insert(Node {
            name: name.to_string(),
            kind: NodeKind::Sink {
                topic,
                predecessors,
            },
        })
    }

    pub fn add_store(
        &mut self,
        name: &str,
        processors: Vec<String>,
        changelog_override: Option<String>,
    ) {
        self.stores.push(StoreEntry {
            name: name.to_string(),
            processors,
            changelog_override,
        });
    }

    /// Declare a copartition group over the given member topic names.
    pub fn add_copartition_group(&mut self, topics: Vec<String>) {
        self.copartition_groups.push(topics);
    }

    /// Validate that every referenced predecessor exists. Call after all nodes
    /// are added, before grouping.
    pub fn validate_predecessors(&self) -> Result<(), TopologyError> {
        for node in &self.nodes {
            let preds = match &node.kind {
                NodeKind::Processor { predecessors } | NodeKind::Sink { predecessors, .. } => {
                    predecessors
                }
                NodeKind::Source { .. } => continue,
            };
            for p in preds {
                if !self.index.contains_key(p) {
                    return Err(TopologyError::UnknownPredecessor {
                        node: node.name.clone(),
                        predecessor: p.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn nodes_preserve_insertion_order() {
        let mut reg = NodeRegistry::default();
        reg.add_source("src", vec!["t".into()]).unwrap();
        reg.add_processor("p", vec!["src".into()]).unwrap();
        reg.add_sink("snk", "out".into(), vec!["p".into()]).unwrap();
        let names: Vec<&str> = reg.nodes.iter().map(|n| n.name.as_str()).collect();
        check!(names == vec!["src", "p", "snk"]);
    }

    #[test]
    fn duplicate_node_is_rejected() {
        let mut reg = NodeRegistry::default();
        reg.add_source("a", vec!["t".into()]).unwrap();
        check!(reg.add_processor("a", vec![]).is_err());
    }
}
