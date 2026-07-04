//! Pure decision core of the transaction coordinator's `EndTxn` path, extracted
//! so the KIP-98/EOS state machine is independently model-checkable. No I/O.
//!
//! The Phase-3 primitives (`validate_complete_reacquire`, `next_producer_identity`)
//! live in `handlers::end_txn` next to the handler that also persists; this module
//! wraps them into a single decision the handler — and the stateright model — can
//! drive. See `decision_model.rs` and the design:
//! `docs/superpowers/specs/2026-06-14-crabka-txn-coordinator-model-design.md`.

use crabka_log::ProducerId;

use super::{
    handlers::end_txn::{ReacquireDecision, next_producer_identity, validate_complete_reacquire},
    state::{TxnEntry, TxnState},
    version::TxnVersion,
};
use crate::{codes, producer_id_manager::ProducerIdManager};

/// Phase 1 of `EndTxn`: validate the `Ongoing → Prepare{Commit,Abort}` transition
/// and apply it to `entry`. Returns `(prepare, complete)` states on success, or
/// the Kafka error code to return. Pure; the caller persists `entry` afterwards.
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
    /// Finalise: write `next_state`, and return the (possibly epoch-bumped)
    /// identity to the producer.
    Proceed {
        next_state: TxnState,
        response_pid: ProducerId,
        response_epoch: i16,
    },
    /// The entry already reached the intended Complete state (idempotent retry /
    /// lost race) — report success without re-finalising.
    AlreadyComplete {
        response_pid: ProducerId,
        response_epoch: i16,
    },
    /// The producer was fenced (epoch bumped) or the state advanced underneath
    /// the marker fan-out — do NOT finalise; return this Kafka error code.
    Reject(i16),
}

/// Phase 3 of `EndTxn`: after the marker fan-out, re-validate the re-acquired
/// `entry` and decide whether to finalise. Pure wrapper over
/// `validate_complete_reacquire` + `next_producer_identity` — the latter bumps
/// the producer epoch at `TV_2` so a zombie holding the old epoch is fenced.
pub(crate) fn decide_end_txn_completion(
    entry: &TxnEntry,
    expected_pid: ProducerId,
    expected_epoch: i16,
    prepare: TxnState,
    complete: TxnState,
    txnv: TxnVersion,
    ids: &ProducerIdManager,
) -> CompletionDecision {
    match validate_complete_reacquire(entry, expected_pid, expected_epoch, prepare, complete) {
        ReacquireDecision::Proceed => {
            let (response_pid, response_epoch) =
                next_producer_identity(txnv, entry.producer_id, entry.producer_epoch, ids);
            CompletionDecision::Proceed {
                next_state: complete,
                response_pid,
                response_epoch,
            }
        }
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
