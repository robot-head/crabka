//! Logical DSL graph: one `GraphNode` per JVM `GraphNode`, auto-named at build
//! time. The builder optimizes the graph, then lowers it to the Processor-API
//! `Topology`.
use std::any::Any;

pub(crate) type NodeId = usize;

/// A boxed lowering thunk attached at op-call time. The lowering pass calls it to
/// attach the typed processor to the Processor-API builder. The thunk is erased
/// here, because each op has different K and V types.
#[allow(dead_code)]
pub(crate) type LowerFn = Box<dyn FnOnce(&mut LowerState) + Send>;

/// Threaded through lowering: the Processor-API `Topology` under construction,
/// the app id, and the Processor-API node NAME that each logical node lowered to.
#[allow(dead_code)]
pub(crate) struct LowerState {
    pub topology: crate::topology::Topology,
    pub app_id: String,
    pub handle_name: std::collections::HashMap<NodeId, String>,
    /// `REUSE_KTABLE_SOURCE_TOPICS`: node id to the source topic that the node's
    /// store should reuse as its changelog. A `TableSource` thunk reads this map
    /// at lowering time. When an entry is present, the thunk registers its store
    /// with that changelog topic through `add_state_store_with_changelog`, and
    /// not with the default `<app>-<store>-changelog`. The map is empty unless
    /// the optimizer ran.
    pub reuse_changelog: std::collections::HashMap<NodeId, String>,
}

#[allow(dead_code)]
pub(crate) enum GraphNodeKind {
    StreamSource {
        topics: Vec<String>,
    },
    StatelessProcessor {
        repartition_required: bool,
    },
    StreamSink {
        topic: String,
    },
    Repartition {
        topic: String,
        partitions: Option<i32>,
    },
    Aggregate {
        store_name: String,
        changelog: bool,
    },
    TableSource {
        topic: String,
        store_name: String,
        reuse_source_for_changelog: bool,
    },
    TableProcessor {
        store_name: Option<String>,
    },
    /// A `GlobalKTable` source. It is invisible in the wire, with no subtopology
    /// and no changelog. Its lowering thunk calls `Topology::add_global_store`,
    /// which registers a source and an update-processor, and a separate global KV
    /// store factory. The source and update-processor consume a node-group index.
    GlobalSource {
        topic: String,
        store_name: String,
        source_name: String,
        processor_name: String,
    },
}

#[allow(dead_code)]
pub(crate) struct GraphNode {
    pub id: NodeId,
    pub name: String,
    pub kind: GraphNodeKind,
    pub predecessors: Vec<NodeId>,
    pub children: Vec<NodeId>,
    #[allow(dead_code)]
    pub key_changing_operation: bool,
    #[allow(dead_code)]
    pub merge_node: bool,
    /// Typed lowering thunk. It is `None` for a node that lowers structurally.
    #[allow(dead_code)]
    pub lower: Option<LowerFn>,
    /// Erased payload that some passes and the lowering step inspect, such as a
    /// source `Consumed`.
    #[allow(dead_code)]
    pub aux: Option<Box<dyn Any + Send>>,
}

#[derive(Default)]
pub(crate) struct LogicalGraph {
    pub nodes: Vec<GraphNode>,
    /// Optimizer-installed redirects from a redundant node to a keeper node. When
    /// `aliases[&b] == a`, the lowering driver does not run node `b`'s thunk. It
    /// instead points `handle_name[&b]` at whatever `a` lowered to. The
    /// `MERGE_REPARTITION_TOPICS` pass uses this to collapse two repartition
    /// nodes off the same key-changing source onto one shared repartition topic.
    /// The keeper always has the lower id, so id-order lowering visits it first.
    pub aliases: std::collections::HashMap<NodeId, NodeId>,
}

impl LogicalGraph {
    pub fn add(&mut self, name: String, kind: GraphNodeKind, predecessors: Vec<NodeId>) -> NodeId {
        let id = self.nodes.len();
        for &p in &predecessors {
            self.nodes[p].children.push(id);
        }
        self.nodes.push(GraphNode {
            id,
            name,
            kind,
            predecessors,
            children: Vec::new(),
            key_changing_operation: false,
            merge_node: false,
            lower: None,
            aux: None,
        });
        id
    }
}
