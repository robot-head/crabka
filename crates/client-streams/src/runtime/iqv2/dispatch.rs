//! `IQv2` supervisor channel message and per-partition result assembly. This is
//! the bridge between the public envelope and the byte-level store hook.

use std::{any::Any, collections::BTreeMap};

use tokio::sync::oneshot;

use super::{
    request::{PartitionSel, Position, PositionBound},
    result::{FailureReason, QueryResult, StateQueryResult},
};
use crate::store::iq::{Iq2Query, StoreKind};

/// One `IQv2` query addressed to the supervisor (sent on the dedicated `iq2`
/// channel; the v1 byte channel is untouched).
pub(crate) struct Iq2Request {
    pub store: String,
    pub kind: StoreKind,
    pub query: Iq2Query,
    pub partitions: PartitionSel,
    pub bound: PositionBound,
    pub require_active: bool,
    pub reply: oneshot::Sender<Iq2Outcome>,
}

/// Type alias for one partition's raw boxed result entry.
type PartitionEntry = (i32, Position, Result<Box<dyn Any + Send>, FailureReason>);

/// Raw per-partition outcomes from the supervisor, before downcast to `R`.
pub(crate) struct Iq2Outcome {
    /// `(partition, position, Ok(boxed R) | Err(failure))` for each responding task.
    pub per_partition: Vec<PartitionEntry>,
}

/// Downcast each partition's boxed result into `R` and build the typed
/// [`StateQueryResult`]. A box that does not downcast to `R` becomes a
/// `StoreException` failure for that partition.
pub(crate) fn assemble<R: 'static>(outcome: Iq2Outcome) -> StateQueryResult<R> {
    let mut map: BTreeMap<i32, QueryResult<R>> = BTreeMap::new();
    for (partition, position, res) in outcome.per_partition {
        let qr = match res {
            Ok(boxed) => match boxed.downcast::<R>() {
                Ok(r) => QueryResult::Success {
                    result: *r,
                    position,
                },
                Err(_) => QueryResult::Failure {
                    reason: FailureReason::StoreException,
                    message: "IQv2 result type mismatch".to_string(),
                },
            },
            Err(reason) => QueryResult::Failure {
                reason,
                message: format!("{reason:?}"),
            },
        };
        map.insert(partition, qr);
    }
    StateQueryResult::new(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::iqv2::{request::Position, result::FailureReason};

    #[test]
    fn assemble_downcasts_success_and_maps_failures() {
        let ok: Box<dyn std::any::Any + Send> = Box::new(Some(7_i64));
        let outcome = Iq2Outcome {
            per_partition: vec![
                (0, Position::default(), Ok(ok)),
                (1, Position::default(), Err(FailureReason::NotUpToBound)),
            ],
        };
        let res = assemble::<Option<i64>>(outcome);
        assert2::assert!(res.partition_results().len() == 2);
        assert2::assert!(res.partition_results()[&0].result() == Some(&Some(7)));
        assert2::assert!(
            res.partition_results()[&1].failure_reason() == Some(FailureReason::NotUpToBound)
        );
    }

    #[test]
    fn assemble_type_mismatch_is_store_exception() {
        let wrong: Box<dyn std::any::Any + Send> = Box::new("not an i64".to_string());
        let outcome = Iq2Outcome {
            per_partition: vec![(0, Position::default(), Ok(wrong))],
        };
        let res = assemble::<Option<i64>>(outcome);
        assert2::assert!(
            res.partition_results()[&0].failure_reason() == Some(FailureReason::StoreException)
        );
    }
}
