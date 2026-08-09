//! Pure decision core of the transaction coordinator's `EndTxn` path.
//!
//! The Phase-3 revalidation primitive lives in `handlers::end_txn` next to the
//! handler that also persists. This module combines it with the completion
//! identity selected during Phase 1 into one decision that the handler and the
//! stateright model can drive. See `decision_model.rs` and the design:
//! `docs/superpowers/specs/2026-06-14-crabka-txn-coordinator-model-design.md`.

use crabka_log::ProducerId;

use super::{
    handlers::end_txn::{ReacquireDecision, validate_complete_reacquire},
    state::{TxnEntry, TxnState},
};
use crate::codes;

/// Phase 1 of `EndTxn`: validates the `Ongoing → Prepare{Commit,Abort}`
/// transition and applies it to `entry`.
///
/// It returns the `(prepare, complete)` states on success, or the Kafka error
/// code to return. The function is pure, and the caller persists `entry`
/// afterwards.
pub(crate) fn decide_phase1_transition(
    entry: &mut TxnEntry,
    committed: bool,
) -> Result<(TxnState, TxnState), i16> {
    let prepare = if committed {
        TxnState::PrepareCommit
    } else {
        TxnState::PrepareAbort
    };
    let complete = if committed {
        TxnState::CompleteCommit
    } else {
        TxnState::CompleteAbort
    };
    if !entry.state.can_transition_to(prepare) {
        return Err(codes::INVALID_TXN_STATE);
    }
    entry.state = prepare;
    Ok((prepare, complete))
}

/// Outcome of [`decide_end_txn_completion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionDecision {
    /// Finalise the transaction: write `next_state`, and return the identity
    /// to the producer. That identity may carry a bumped epoch.
    Proceed {
        next_state: TxnState,
        response_pid: ProducerId,
        response_epoch: i16,
    },
    /// The entry already reached the intended Complete state, from an
    /// idempotent retry or a lost race. Report success and do not finalise it
    /// again.
    AlreadyComplete {
        response_pid: ProducerId,
        response_epoch: i16,
    },
    /// The broker fenced the producer with an epoch bump, or the state
    /// advanced during the marker fan-out. Do NOT finalise, and return this
    /// Kafka error code.
    Reject(i16),
}

/// Phase 3 of `EndTxn`: after marker fan-out, revalidate the entry and decide
/// whether to adopt the completion identity that was persisted during Phase 1.
/// The decision does not allocate or increment an identity.
pub(crate) fn decide_end_txn_completion(
    entry: &TxnEntry,
    expected_pid: ProducerId,
    expected_epoch: i16,
    expected_completion_pid: ProducerId,
    expected_completion_epoch: i16,
    prepare: TxnState,
    complete: TxnState,
) -> CompletionDecision {
    if entry.state == complete
        && entry.producer_id == expected_completion_pid
        && entry.producer_epoch == expected_completion_epoch
    {
        return CompletionDecision::AlreadyComplete {
            response_pid: entry.producer_id,
            response_epoch: entry.producer_epoch,
        };
    }
    match validate_complete_reacquire(entry, expected_pid, expected_epoch, prepare, complete) {
        ReacquireDecision::Proceed => CompletionDecision::Proceed {
            next_state: complete,
            response_pid: expected_completion_pid,
            response_epoch: expected_completion_epoch,
        },
        ReacquireDecision::AlreadyComplete => CompletionDecision::AlreadyComplete {
            response_pid: entry.producer_id,
            response_epoch: entry.producer_epoch,
        },
        ReacquireDecision::Reject(code) => CompletionDecision::Reject(code),
    }
}

#[cfg(test)]
#[path = "decision_model.rs"]
mod decision_model;
