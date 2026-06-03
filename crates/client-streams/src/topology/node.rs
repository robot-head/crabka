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

/// The full node graph, recorded in insertion order.
#[derive(Debug, Default)]
pub(crate) struct NodeRegistry {
    pub nodes: Vec<Node>,
    pub index: HashMap<String, usize>,
    /// `(store_name, connected_processor_names)` in insertion order.
    pub stores: Vec<(String, Vec<String>)>,
    /// Topic names registered as internal repartition topics.
    pub repartition_topics: HashSet<String>,
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
        self.insert(Node {
            name: name.to_string(),
            kind: NodeKind::Sink {
                topic,
                predecessors,
            },
        })
    }

    pub fn add_store(&mut self, name: &str, processors: Vec<String>) {
        self.stores.push((name.to_string(), processors));
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
