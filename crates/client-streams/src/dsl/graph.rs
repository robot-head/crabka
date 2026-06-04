//! Logical DSL graph: a `GraphNode` per JVM `GraphNode`, auto-named at build
//! time, optimized then lowered to the Processor-API `Topology`.
use std::any::Any;

pub(crate) type NodeId = usize;

/// A boxed lowering thunk attached at op-call time; the lowering (Task 5) calls
/// it to attach the typed processor to the Processor-API builder. Erased here
/// because each op has different K/V types.
#[allow(dead_code)]
pub(crate) type LowerFn = Box<dyn FnOnce(&mut LowerState) + Send>;

/// Threaded through lowering: the Processor-API `Topology` under construction +
/// the app id + the Processor-API node NAME each logical node lowered to.
#[allow(dead_code)]
pub(crate) struct LowerState {
    pub topology: crate::topology::Topology,
    pub app_id: String,
    pub handle_name: std::collections::HashMap<NodeId, String>,
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
    /// Typed lowering thunk (None for nodes lowered structurally).
    #[allow(dead_code)]
    pub lower: Option<LowerFn>,
    /// Erased payload some passes/lowering inspect (e.g. source `Consumed`).
    #[allow(dead_code)]
    pub aux: Option<Box<dyn Any + Send>>,
}

#[derive(Default)]
pub(crate) struct LogicalGraph {
    pub nodes: Vec<GraphNode>,
    /// Optimizer-installed redundant→keeper redirects. When `aliases[&b] == a`,
    /// the lowering driver does not run node `b`'s thunk; instead it points
    /// `handle_name[&b]` at whatever `a` lowered to. The `MERGE_REPARTITION_TOPICS`
    /// pass uses this to collapse two repartition nodes (off the same key-changing
    /// source) onto a single shared repartition topic. The keeper always has the
    /// lower id, so id-order lowering visits it first.
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
