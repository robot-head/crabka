//! `IQv2` results: per-partition `QueryResult<R>` aggregated into
//! `StateQueryResult<R>`.

use std::collections::BTreeMap;

use super::request::Position;

/// Why a partition's query did not produce a result (mirrors the JVM
/// `FailureReason` subset crabka can produce locally).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// The store kind does not support this query variant.
    UnknownQueryType,
    /// The partition's `Position` did not meet the requested bound.
    NotUpToBound,
    /// The store exists in the topology but not on this partition's task.
    NotPresent,
    /// The partition is standby/restoring and an active-only query was asked.
    NotActive,
    /// The store name is not in the topology.
    DoesNotExist,
    /// Internal failure (e.g. a result/key type mismatch across the boundary).
    StoreException,
}

/// One partition's outcome.
pub enum QueryResult<R> {
    Success {
        result: R,
        position: Position,
    },
    Failure {
        reason: FailureReason,
        message: String,
    },
}

impl<R> QueryResult<R> {
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, QueryResult::Success { .. })
    }
    #[must_use]
    pub fn result(&self) -> Option<&R> {
        match self {
            QueryResult::Success { result, .. } => Some(result),
            QueryResult::Failure { .. } => None,
        }
    }
    #[must_use]
    pub fn into_result(self) -> Option<R> {
        match self {
            QueryResult::Success { result, .. } => Some(result),
            QueryResult::Failure { .. } => None,
        }
    }
    #[must_use]
    pub fn position(&self) -> Option<&Position> {
        match self {
            QueryResult::Success { position, .. } => Some(position),
            QueryResult::Failure { .. } => None,
        }
    }
    #[must_use]
    pub fn failure_reason(&self) -> Option<FailureReason> {
        match self {
            QueryResult::Failure { reason, .. } => Some(*reason),
            QueryResult::Success { .. } => None,
        }
    }
    #[must_use]
    pub fn failure_message(&self) -> Option<&str> {
        match self {
            QueryResult::Failure { message, .. } => Some(message),
            QueryResult::Success { .. } => None,
        }
    }
}

/// All local partitions' outcomes for one query.
pub struct StateQueryResult<R> {
    partition_results: BTreeMap<i32, QueryResult<R>>,
}

impl<R> StateQueryResult<R> {
    #[must_use]
    #[allow(dead_code)] // constructed by the IQv2 dispatch path (later slice)
    pub(crate) fn new(partition_results: BTreeMap<i32, QueryResult<R>>) -> Self {
        Self { partition_results }
    }
    #[must_use]
    pub fn partition_results(&self) -> &BTreeMap<i32, QueryResult<R>> {
        &self.partition_results
    }
    /// The single partition's result, iff exactly one partition responded.
    #[must_use]
    pub fn only_partition_result(&self) -> Option<&QueryResult<R>> {
        if self.partition_results.len() == 1 {
            self.partition_results.values().next()
        } else {
            None
        }
    }
    /// Global-store result — always `None` in slice 3a (out of scope).
    #[must_use]
    pub fn global_result(&self) -> Option<&QueryResult<R>> {
        None
    }
}
