//! KIP-939 two-phase-commit (2PC) participation — pure decision cores.
//!
//! A 2PC transaction is one whose commit/abort decision is owned by an
//! *external* transaction coordinator (an XA resource manager, Apache Flink's
//! sink, …). Kafka acts as a 2PC participant: once the producer has prepared,
//! the coordinator MUST keep the transaction completable indefinitely and MUST
//! NOT proactively abort it on the transaction timeout — only the external
//! coordinator (via `EndTxn`) or an explicit `InitProducerId` may end it.
//!
//! ## How "this is a 2PC transaction" is encoded
//!
//! Following Apache Kafka exactly, 2PC is NOT a new persisted field. It is
//! encoded in the already-persisted `TransactionTimeoutMs` as the sentinel
//! [`NO_TIMEOUT_MS`] (`i32::MAX`). Kafka's
//! `TransactionMetadata.isDistributedTwoPhaseCommitTxn()` is literally
//! `txnTimeoutMs == Integer.MAX_VALUE`, and `InitProducerId` resolves the
//! stored timeout to `Int.MaxValue` when `enable2Pc` is set. Because the
//! timeout round-trips through `TransactionLogValue`, the property survives
//! coordinator failover and log replay without any schema change.
//!
//! These functions are pure and exhaustively model-checked in
//! [`super::two_pc_model`]; the live coordinator (the idle-transaction reaper in
//! [`super::expiration`] and the `InitProducerId` handler) calls the same
//! functions so the model's guarantees bind production behaviour.

use super::state::TxnState;

/// Sentinel `TransactionTimeoutMs` marking a 2PC transaction: it is never
/// auto-aborted by the coordinator's idle-transaction reaper. Mirrors Apache
/// Kafka's `Integer.MAX_VALUE` 2PC marker (`isDistributedTwoPhaseCommitTxn`).
pub(crate) const NO_TIMEOUT_MS: i32 = i32::MAX;

/// Resolve the `TransactionTimeoutMs` to persist for an `InitProducerId`.
///
/// * `enable_2pc` → [`NO_TIMEOUT_MS`]: the external coordinator owns the
///   commit decision, so the broker never times the transaction out. The
///   client-requested timeout is ignored (it is irrelevant under 2PC, and
///   Kafka's `transaction.max.timeout.ms` cap does not apply).
/// * otherwise → the client-requested timeout clamped to
///   `[min_timeout_ms, max_timeout_ms]`, the classic KIP-98 behaviour.
#[must_use]
pub(crate) fn resolve_txn_timeout(
    enable_2pc: bool,
    requested_ms: i32,
    min_timeout_ms: i32,
    max_timeout_ms: i32,
) -> i32 {
    if enable_2pc {
        NO_TIMEOUT_MS
    } else {
        requested_ms.clamp(min_timeout_ms, max_timeout_ms)
    }
}

/// Is a transaction with this persisted timeout a 2PC (externally-coordinated)
/// transaction? Identified by the [`NO_TIMEOUT_MS`] sentinel, exactly like
/// Kafka's `isDistributedTwoPhaseCommitTxn`.
#[must_use]
pub(crate) fn is_two_phase_commit(txn_timeout_ms: i32) -> bool {
    txn_timeout_ms == NO_TIMEOUT_MS
}

/// THE safety-critical decision: should the idle-transaction reaper abort the
/// transaction in `state` (with persisted `txn_timeout_ms`, having become
/// `Ongoing` at `start_ms`) as of `now_ms`?
///
/// Returns `true` iff ALL of the following hold:
///  - the transaction is [`TxnState::Ongoing`] — only an *open* transaction can
///    time out; `Empty` / `Prepare*` / `Complete*` / `Dead` are never reaped
///    (Prepare\* is a transient commit/abort the coordinator is already driving,
///    and the terminal/idle states are reclaimed by a separate, much longer
///    transactional-id expiry, not this reaper);
///  - it is NOT a 2PC transaction (`!is_two_phase_commit`) — **the KIP-939
///    guarantee**: a prepared 2PC transaction is never unilaterally aborted; and
///  - it has been open at least `txn_timeout_ms`
///    (`now_ms - start_ms >= txn_timeout_ms`).
///
/// Pure and total: clock skew (`now_ms < start_ms`) yields `false` via a
/// saturating subtraction, so a backwards clock can never spuriously abort.
#[must_use]
pub(crate) fn should_abort_idle_txn(
    state: TxnState,
    txn_timeout_ms: i32,
    start_ms: i64,
    now_ms: i64,
) -> bool {
    if state != TxnState::Ongoing {
        return false;
    }
    if is_two_phase_commit(txn_timeout_ms) {
        // KIP-939: a 2PC transaction has no timeout. Skip it unconditionally,
        // BEFORE the elapsed-time arithmetic, so even a far-future `now_ms`
        // (or a future where `i32::MAX` ms has genuinely elapsed) can't reap it.
        return false;
    }
    now_ms.saturating_sub(start_ms) >= i64::from(txn_timeout_ms)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn resolve_timeout_2pc_is_sentinel_regardless_of_request() {
        // Even an out-of-range request (-5) is ignored under 2PC.
        for requested in [30_000, 0, i32::MAX, -5] {
            assert!(
                resolve_txn_timeout(true, requested, 2_000, 8_000) == NO_TIMEOUT_MS,
                "{requested}"
            );
        }
    }

    #[test]
    fn resolve_timeout_non_2pc_clamps_to_configured_bounds() {
        // Below the floor clamps up; above the ceiling clamps down.
        for (requested, want) in [
            (5_000, 5_000),
            (0, 2_000),
            (-1, 2_000),
            (i32::MAX, 8_000),
            (8_001, 8_000),
        ] {
            assert!(
                resolve_txn_timeout(false, requested, 2_000, 8_000) == want,
                "{requested}"
            );
        }
    }

    #[test]
    fn non_2pc_resolution_never_collides_with_the_sentinel() {
        use assert2::check;
        // The clamp ceiling is far below i32::MAX, so a non-2PC transaction can
        // never accidentally look like a 2PC one.
        check!(resolve_txn_timeout(false, i32::MAX, 2_000, 8_000) != NO_TIMEOUT_MS);
        check!(!is_two_phase_commit(resolve_txn_timeout(
            false,
            i32::MAX,
            2_000,
            8_000
        )));
        check!(is_two_phase_commit(resolve_txn_timeout(
            true, 1, 2_000, 8_000
        )));
    }

    #[test]
    fn reaper_aborts_an_expired_ongoing_non_2pc_txn() {
        // Opened at t=0 with a 60s timeout; at t=60s it is reapable.
        assert!(should_abort_idle_txn(TxnState::Ongoing, 60_000, 0, 60_000));
        assert!(should_abort_idle_txn(TxnState::Ongoing, 60_000, 0, 120_000));
    }

    #[test]
    fn reaper_spares_a_not_yet_expired_ongoing_txn() {
        // One ms short of the timeout.
        assert!(!should_abort_idle_txn(TxnState::Ongoing, 60_000, 0, 59_999));
        // Exactly opened "now".
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            60_000,
            1_000,
            1_000
        ));
    }

    #[test]
    fn reaper_never_touches_a_2pc_txn_even_in_the_far_future() {
        // The headline KIP-939 property at the unit level: a 2PC (sentinel
        // timeout) Ongoing transaction is never reaped, no matter how much time
        // has elapsed — including a `now_ms` past `i32::MAX` ms.
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            NO_TIMEOUT_MS,
            0,
            i64::MAX
        ));
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            NO_TIMEOUT_MS,
            0,
            60_000
        ));
    }

    #[test]
    fn reaper_only_acts_on_ongoing() {
        for state in [
            TxnState::Empty,
            TxnState::PrepareCommit,
            TxnState::PrepareAbort,
            TxnState::CompleteCommit,
            TxnState::CompleteAbort,
            TxnState::Dead,
        ] {
            assert!(
                !should_abort_idle_txn(state, 60_000, 0, i64::MAX),
                "{state:?} must never be reaped by the idle-txn timeout"
            );
        }
    }

    #[test]
    fn reaper_is_robust_to_a_backwards_clock() {
        // now < start (clock skew) must not underflow into a spurious abort.
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            60_000,
            100_000,
            0
        ));
    }
}
