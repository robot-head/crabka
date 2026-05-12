//! Client-side transactional state machine. Drives the
//! `init_transactions` / `begin` / `commit` / `abort` / `send_offsets_to_transaction`
//! flow.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TxnState {
    /// `init_transactions` not yet called.
    Uninitialized,
    /// `init_transactions` succeeded; no in-flight txn.
    Ready,
    /// Inside `begin_transaction` ... `commit/abort_transaction`.
    InTransaction,
    /// `commit_transaction` or `abort_transaction` in progress.
    CommittingOrAborting,
    /// Producer is fenced; no further txns possible without re-init.
    Fenced,
}
