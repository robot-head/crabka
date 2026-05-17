//! Proposal + Movement types. Mirrors the proto definitions but owned
//! by the model layer so the optimizer + goals don't depend on
//! generated code.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Movement {
    pub topic: String,
    pub partition: i32,
    pub old_replicas: Vec<i32>,
    pub new_replicas: Vec<i32>,
    pub old_leader: i32,
    pub new_leader: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposalStatus {
    /// The optimizer computed the proposal but it has not been
    /// executed. Slice 43a only ever returns this state — execute
    /// lands in 43b.
    Computed,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProposalSummary {
    pub replica_movements: i32,
    pub leader_movements: i32,
    pub max_replicas_before: i32,
    pub max_replicas_after: i32,
    pub max_leaders_before: i32,
    pub max_leaders_after: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub id: String,
    pub status: ProposalStatus,
    pub created_at_ms: i64,
    pub goals_applied: Vec<String>,
    pub summary: ProposalSummary,
    pub movements: Vec<Movement>,
}
