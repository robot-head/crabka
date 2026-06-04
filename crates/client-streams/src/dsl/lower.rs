//! Lowering driver: walk the logical DSL graph and run each node's lowering
//! thunk to build the Processor-API [`Topology`].
//!
//! Nodes are visited in id order. Because [`LogicalGraph::add`] assigns ids
//! sequentially and a node's predecessors are passed in by id when it is added,
//! every predecessor has a strictly smaller id than its child — so id order is a
//! valid topological order (parents before children) for the stateless chain and
//! merge. A node whose thunk is `None` (e.g. the repartition stub) is skipped;
//! later tasks attach those thunks and refine optimizer/branch ordering.
use crate::dsl::graph::{LogicalGraph, LowerState};

pub(crate) fn lower(mut graph: LogicalGraph, app_id: &str) -> crate::topology::Topology {
    let mut state = LowerState {
        topology: crate::topology::Topology::new(),
        app_id: app_id.to_string(),
        handle_name: std::collections::HashMap::new(),
    };
    for node in &mut graph.nodes {
        if let Some(thunk) = node.lower.take() {
            thunk(&mut state);
        }
    }
    state.topology
}
