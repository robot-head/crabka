//! Topology builder: public Processor-API surface.

use std::collections::BTreeMap;

use crabka_protocol::owned::streams_group_heartbeat_request::Topology as WireTopology;

use super::grouping::group_nodes;
use super::node::NodeRegistry;
use super::wire::to_wire;

/// Error building a topology (bad node graph, invalid configuration, etc.).
#[derive(Debug, Clone, thiserror::Error)]
pub enum TopologyError {
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),
    #[error("node {node} references unknown predecessor {predecessor}")]
    UnknownPredecessor { node: String, predecessor: String },
    #[error("topology has no source nodes")]
    Empty,
}

/// A Processor-API topology under construction. Node insertion order is
/// significant — it determines subtopology indices (JVM-matching).
#[derive(Debug, Default)]
pub struct Topology {
    reg: NodeRegistry,
    error: Option<TopologyError>,
}

impl Topology {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a source node reading the given external topics.
    pub fn add_source<S, I, T>(&mut self, name: S, topics: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let topics = topics.into_iter().map(Into::into).collect();
        let r = self.reg.add_source(&name.into(), topics);
        self.record(r);
        self
    }

    /// Add a processor node with the given predecessor node names.
    pub fn add_processor<S, I, T>(&mut self, name: S, predecessors: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let preds = predecessors.into_iter().map(Into::into).collect();
        let r = self.reg.add_processor(&name.into(), preds);
        self.record(r);
        self
    }

    /// Add a sink node writing to `topic`, fed by the given predecessors.
    pub fn add_sink<S, U, I, T>(&mut self, name: S, topic: U, predecessors: I) -> &mut Self
    where
        S: Into<String>,
        U: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let preds = predecessors.into_iter().map(Into::into).collect();
        let r = self.reg.add_sink(&name.into(), topic.into(), preds);
        self.record(r);
        self
    }

    /// Register a state store connected to the given processors (→ changelog).
    pub fn add_state_store<S, I, T>(&mut self, name: S, processors: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let procs = processors.into_iter().map(Into::into).collect();
        self.reg.add_store(&name.into(), procs);
        self
    }

    /// Register a topic name as an internal repartition topic.
    pub fn add_repartition_topic<S: Into<String>>(&mut self, name: S) -> &mut Self {
        self.reg.repartition_topics.insert(name.into());
        self
    }

    /// Derive subtopologies and the wire topology. `application_id` drives
    /// internal-topic names (`<app>-<store>-changelog`).
    pub fn build<S: Into<String>>(
        &self,
        application_id: S,
    ) -> Result<BuiltTopology, TopologyError> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        self.reg.validate_predecessors()?;
        let groups = group_nodes(&self.reg);
        if groups.is_empty() {
            return Err(TopologyError::Empty);
        }
        let app = application_id.into();
        let wire = to_wire(&groups, &app);
        let mut source_topics: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for g in &groups {
            let mut all = g.source_topics.clone();
            all.extend(g.repartition_source_topics.iter().cloned());
            source_topics.insert(g.id.clone(), all);
        }
        Ok(BuiltTopology {
            wire,
            source_topics,
            application_id: app,
        })
    }

    fn record(&mut self, r: Result<(), TopologyError>) {
        if self.error.is_none()
            && let Err(e) = r
        {
            self.error = Some(e);
        }
    }
}

/// A built topology: the wire `Topology` plus the per-subtopology source-topic
/// map used to resolve task assignments to concrete topic-partitions.
#[derive(Debug, Clone)]
pub struct BuiltTopology {
    wire: WireTopology,
    source_topics: BTreeMap<String, Vec<String>>,
    application_id: String,
}

impl BuiltTopology {
    /// The wire `Topology` to send in the join heartbeat.
    #[must_use]
    pub fn to_wire(&self) -> WireTopology {
        self.wire.clone()
    }

    /// The external + repartition source topics a subtopology's tasks read.
    #[must_use]
    pub fn source_topics_for(&self, subtopology_id: &str) -> &[String] {
        self.source_topics
            .get(subtopology_id)
            .map_or(&[], Vec::as_slice)
    }

    /// The application id (drives internal-topic names).
    #[must_use]
    pub fn application_id(&self) -> &str {
        &self.application_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn build_single_source_sink() {
        let mut topo = Topology::new();
        topo.add_source("src", ["in"]);
        topo.add_sink("snk", "out", ["src"]);
        let built = topo.build("app").unwrap();
        let wire = built.to_wire();
        check!(wire.epoch == 0);
        check!(wire.subtopologies.len() == 1);
        check!(wire.subtopologies[0].subtopology_id == "0");
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        check!(built.source_topics_for("0") == ["in".to_string()]);
    }

    #[test]
    fn unknown_predecessor_is_rejected() {
        let mut topo = Topology::new();
        topo.add_source("src", ["in"]);
        topo.add_sink("snk", "out", ["nope"]);
        check!(topo.build("app").is_err());
    }

    #[test]
    fn empty_topology_is_rejected() {
        let topo = Topology::new();
        check!(topo.build("app").is_err());
    }
}
