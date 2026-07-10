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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_accessors_expose_result_and_position() {
        let qr: QueryResult<i64> = QueryResult::Success {
            result: 42,
            position: Position::default(),
        };
        assert_eq!(
            (
                qr.is_success(),
                qr.result(),
                qr.position(),
                qr.failure_reason(),
                qr.failure_message(),
            ),
            (true, Some(&42), Some(&Position::default()), None, None)
        );
        assert_eq!(qr.into_result(), Some(42));
    }

    #[test]
    fn failure_accessors_expose_reason_and_message() {
        let qr: QueryResult<i64> = QueryResult::Failure {
            reason: FailureReason::NotPresent,
            message: "store not on this task".to_string(),
        };
        assert_eq!(
            (
                qr.is_success(),
                qr.result(),
                qr.position(),
                qr.failure_reason(),
                qr.failure_message(),
            ),
            (
                false,
                None,
                None,
                Some(FailureReason::NotPresent),
                Some("store not on this task"),
            )
        );
        assert_eq!(qr.into_result(), None);
    }

    #[test]
    fn state_query_result_multi_partition_has_no_only_or_global() {
        let mut map: BTreeMap<i32, QueryResult<Option<i64>>> = BTreeMap::new();
        map.insert(
            0,
            QueryResult::Success {
                result: Some(1),
                position: Position::default(),
            },
        );
        map.insert(
            1,
            QueryResult::Success {
                result: Some(2),
                position: Position::default(),
            },
        );
        let sqr = StateQueryResult::new(map);
        assert_eq!(
            (
                sqr.partition_results().len(),
                sqr.only_partition_result().is_none(),
                sqr.global_result().is_none(),
            ),
            (2, true, true)
        );
    }

    #[test]
    fn state_query_result_single_partition_has_only() {
        let mut map: BTreeMap<i32, QueryResult<Option<i64>>> = BTreeMap::new();
        map.insert(
            3,
            QueryResult::Success {
                result: Some(9),
                position: Position::default(),
            },
        );
        let sqr = StateQueryResult::new(map);
        let only = sqr.only_partition_result().expect("exactly one partition");
        assert_eq!(
            (sqr.partition_results().len(), only.result()),
            (1, Some(&Some(9)))
        );
    }
}
