//! Client-side transactional state machine. Drives the
//! `init_transactions` / `begin` / `commit` / `abort` / `send_offsets_to_transaction`
//! flow.

use std::sync::Arc;

use crate::error::ProducerError;
use crate::producer::Producer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TxnState {
    /// `init_transactions` not yet called.
    Uninitialized,
    /// `init_transactions` succeeded; no in-flight txn.
    Ready,
    /// Inside `begin_transaction` ... `commit/abort`.
    InTransaction,
    /// `commit`/`abort` in progress.
    CommittingOrAborting,
    /// Producer is fenced; no further txns possible without re-init.
    Fenced,
}

/// An open transaction, borrowing the [`Producer`] that opened it.
///
/// Returned by [`Producer::begin_transaction`]. [`commit`](Self::commit) and
/// [`abort`](Self::abort) each consume `self`, so a transaction can be
/// finished at most once and never silently reused or finished twice.
///
/// Dropping the guard without calling either does nothing — there is no
/// auto-abort on `Drop` (`Producer` has no `Drop` impl today and this
/// preserves that). The producer's transaction state stays `InTransaction`
/// until some guard's `commit`/`abort` runs, or the producer itself is
/// closed/dropped. This is intentionally caller error, not silently "fixed"
/// by the type: do not add a `Drop` impl here without that being a
/// deliberate, separately-reviewed behavior change.
#[must_use = "a transaction must be finished with `commit()` or `abort()`"]
pub struct Transaction<'p> {
    pub(crate) producer: &'p Producer,
}

impl Transaction<'_> {
    /// Commit this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    pub async fn commit(self) -> Result<(), ProducerError> {
        self.producer.end_transaction(true).await
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    pub async fn abort(self) -> Result<(), ProducerError> {
        self.producer.end_transaction(false).await
    }
}

/// Same contract as [`Transaction`], but owns an `Arc<Producer>` instead of
/// borrowing it.
///
/// For callers that must hold the guard across an owned/`'static` boundary a
/// borrow can't survive — e.g. behind a `dyn Trait` object stored in a struct
/// field across many separate async calls. Returned by
/// [`Producer::begin_transaction_owned`]. Mirrors
/// `tokio::sync::Mutex::{lock, lock_owned}` / `MutexGuard`/`OwnedMutexGuard`.
#[must_use = "a transaction must be finished with `commit()` or `abort()`"]
pub struct OwnedTransaction {
    pub(crate) producer: Arc<Producer>,
}

impl OwnedTransaction {
    /// Commit this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    pub async fn commit(self) -> Result<(), ProducerError> {
        self.producer.end_transaction(true).await
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    pub async fn abort(self) -> Result<(), ProducerError> {
        self.producer.end_transaction(false).await
    }
}
