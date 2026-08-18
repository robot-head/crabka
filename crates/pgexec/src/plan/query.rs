//! Query IR shared by planning, EXPLAIN, and execution.
//!
//! These are deliberately data-only contracts.  P0a supplies the binder and
//! node bodies; P0b/P2/P3 can depend on the stable shape without importing the
//! legacy read path.

#![allow(
    dead_code,
    reason = "P0h freezes the crate-internal planner contract before P0a supplies its consumers"
)]

use std::collections::BTreeSet;

use crabka_pgparser::ast::TableExpr;
use crabka_pgtypes::ColumnType;

use crate::{
    bind::BoundExpr,
    error::ExecError,
    join::Relation,
    scope::{ColumnBinding, Scope},
};

/// A range-table entry, numbered from one as PostgreSQL's `varno` is.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RangeTblEntry {
    pub(crate) rti: usize,
    pub(crate) source: TableExpr,
    pub(crate) columns: Vec<ColumnBinding>,
}

/// A bound column reference. `attno` is one-based; zero remains available for
/// whole-row references when P0a introduces them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Var {
    pub(crate) rti: usize,
    pub(crate) attno: usize,
    pub(crate) ty: ColumnType,
}

/// One output expression and the position it occupies in a plan's row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TargetEntry {
    pub(crate) expr: BoundExpr,
    pub(crate) resno: usize,
    pub(crate) resname: String,
}

/// A predicate plus the information that controls where it is legal to run.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RestrictInfo {
    pub(crate) clause: BoundExpr,
    pub(crate) is_pushed_down: bool,
    pub(crate) security_level: u32,
    pub(crate) leakproof: bool,
    pub(crate) required_relids: BTreeSet<usize>,
}

/// A planned statement: a range table and the root of its executable tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedStmt {
    pub(crate) rtable: Vec<RangeTblEntry>,
    pub(crate) plan_tree: Plan,
}

/// Common plan fields. Every node owns the target list and quals it applies.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Plan {
    pub(crate) target_list: Vec<TargetEntry>,
    pub(crate) quals: Vec<RestrictInfo>,
    pub(crate) node: PlanNode,
}

/// The Phase-1 executor nodes. Their bodies land incrementally in P0a.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlanNode {
    Result,
    SeqScan { scanrelid: usize },
    Filter { input: Box<Plan> },
    Aggregate { input: Box<Plan> },
    Sort { input: Box<Plan> },
    Unique { input: Box<Plan> },
    Limit { input: Box<Plan> },
    ProjectSet { input: Box<Plan> },
    WindowAgg { input: Box<Plan> },
    ValuesScan,
    FunctionScan,
    SubqueryScan { input: Box<Plan> },
    CteScan,
    NamedTuplestoreScan,
    TableFunctionScan,
    NestedLoop { outer: Box<Plan>, inner: Box<Plan> },
}

/// Mutable, per-execution node state used for EXPLAIN ANALYZE counters.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanState {
    pub(crate) plan: Plan,
    pub(crate) scope: Scope,
    pub(crate) nloops: u64,
    pub(crate) ntuples: u64,
    pub(crate) rows_removed: u64,
}

impl PlanState {
    pub(crate) fn new(plan: Plan, scope: Scope) -> Self {
        Self {
            plan,
            scope,
            nloops: 0,
            ntuples: 0,
            rows_removed: 0,
        }
    }

    /// Record entering this node once. Executors call it at node boundaries.
    pub(crate) fn begin_loop(&mut self) {
        self.nloops += 1;
    }

    /// Record one emitted row.
    pub(crate) fn emit_row(&mut self) {
        self.ntuples += 1;
    }

    /// Record one row rejected by this node's filter or join condition.
    pub(crate) fn remove_row(&mut self) {
        self.rows_removed += 1;
    }
}

/// Executes one plan node against the statement's evaluation context.
pub(crate) trait Executor {
    fn execute(&mut self, state: &mut PlanState) -> Result<Relation, ExecError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result_plan() -> Plan {
        Plan {
            target_list: Vec::new(),
            quals: Vec::new(),
            node: PlanNode::Result,
        }
    }

    #[test]
    fn plan_state_keeps_per_execution_counters() {
        let mut state = PlanState::new(result_plan(), Scope::empty());
        state.begin_loop();
        state.begin_loop();
        state.emit_row();
        state.emit_row();
        state.remove_row();

        assert_eq!((state.nloops, state.ntuples, state.rows_removed), (2, 2, 1));
        assert!(matches!(state.plan.node, PlanNode::Result));
        assert!(state.scope.columns.is_empty());
    }

    #[test]
    fn planned_statement_keeps_its_range_table_separate_from_the_tree() {
        let statement = PlannedStmt {
            rtable: Vec::new(),
            plan_tree: result_plan(),
        };

        assert!(statement.rtable.is_empty());
        assert!(matches!(statement.plan_tree.node, PlanNode::Result));
    }
}
