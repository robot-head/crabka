//! Client-side transactional state machine. Drives the
//! `init_transactions` / `begin` / `commit` / `abort` / `send_offsets_to_transaction`
//! flow.

use std::sync::Arc;

use crate::{error::ProducerError, producer::Producer};

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
/// [`abort`](Self::abort) each consume `self` on success, so a transaction
/// cannot be silently reused or finished twice. On failure the guard is
/// handed back via [`EndTransactionError::transaction`] instead of being
/// dropped: Kafka's `EndTxn` contract makes some failures (e.g.
/// `CONCURRENT_TRANSACTIONS`) retryable against the very same broker-side
/// transaction, so the caller can retry `commit()`, or switch to `abort()`,
/// on the returned guard. For non-retryable failures the producer's
/// transaction state has already moved on, and the returned guard's next
/// `commit`/`abort` attempt will itself fail immediately.
///
/// Dropping the guard without calling either does nothing — there is no
/// auto-abort on `Drop` (`Producer` has no `Drop` impl today and this
/// preserves that). The producer's transaction state stays `InTransaction`
/// until some guard's `commit`/`abort` runs, or the producer itself is
/// closed/dropped. This is intentionally caller error, not silently "fixed"
/// by the type: do not add a `Drop` impl here without that being a
/// deliberate, separately-reviewed behavior change.
#[derive(Debug)]
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
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn commit(self) -> Result<(), EndTransactionError<Self>> {
        self.producer
            .end_transaction(true)
            .await
            .map_err(|source| EndTransactionError {
                transaction: self,
                source,
            })
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn abort(self) -> Result<(), EndTransactionError<Self>> {
        self.producer
            .end_transaction(false)
            .await
            .map_err(|source| EndTransactionError {
                transaction: self,
                source,
            })
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
#[derive(Debug)]
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
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn commit(self) -> Result<(), EndTransactionError<Self>> {
        self.producer
            .end_transaction(true)
            .await
            .map_err(|source| EndTransactionError {
                transaction: self,
                source,
            })
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn abort(self) -> Result<(), EndTransactionError<Self>> {
        self.producer
            .end_transaction(false)
            .await
            .map_err(|source| EndTransactionError {
                transaction: self,
                source,
            })
    }
}

/// Error returned by [`Transaction::commit`]/[`abort`](Transaction::abort) or
/// the [`OwnedTransaction`] equivalents, carrying the guard back so a
/// retryable failure (e.g. `CONCURRENT_TRANSACTIONS`) can be retried or
/// aborted on the same underlying transaction instead of being stranded.
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct EndTransactionError<T> {
    /// The guard the `commit`/`abort` call was made on, handed back so the
    /// caller can retry `commit()` or call `abort()` on the same transaction.
    pub transaction: T,
    /// The underlying failure.
    #[source]
    pub source: ProducerError,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicI16, AtomicU16, Ordering},
    };

    use bytes::BytesMut;
    use crabka_client_core::MockBroker;
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request, api_versions_response::ApiVersionsResponse, end_txn_request,
            end_txn_response::EndTxnResponse, find_coordinator_request,
            find_coordinator_response::FindCoordinatorResponse, init_producer_id_request,
            init_producer_id_response::InitProducerIdResponse,
        },
    };

    use crate::{error::ProducerError, producer::Producer};

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    /// Boots a mock broker that also answers as its own transaction
    /// coordinator (`FindCoordinator` resolves back to the mock's own
    /// address), and returns a transactional `Producer` with
    /// `init_transactions` already completed against it. `end_txn_error`
    /// lets each test steer the `EndTxn` response's `error_code` (0 =
    /// success) independently per call, so a test can fail a `commit`/`abort`
    /// and then flip the mock to let a retry on the same guard succeed.
    async fn transactional_producer(end_txn_error: Arc<AtomicI16>) -> (MockBroker, Producer) {
        let port_cell = Arc::new(AtomicU16::new(0));
        let handler_port = port_cell.clone();
        let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse::default()));
            }
            if api_key == find_coordinator_request::API_KEY {
                return Some(encode_v0(&FindCoordinatorResponse {
                    error_code: 0,
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: i32::from(handler_port.load(Ordering::SeqCst)),
                    ..Default::default()
                }));
            }
            if api_key == init_producer_id_request::API_KEY {
                return Some(encode_v0(&InitProducerIdResponse {
                    error_code: 0,
                    producer_id: 7,
                    producer_epoch: 3,
                    ..Default::default()
                }));
            }
            if api_key == end_txn_request::API_KEY {
                return Some(encode_v0(&EndTxnResponse {
                    error_code: end_txn_error.load(Ordering::SeqCst),
                    ..Default::default()
                }));
            }
            None
        })
        .await;
        port_cell.store(mock.addr.port(), Ordering::SeqCst);

        let producer = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .enable_idempotence(false)
            .transactional_id("test-txn")
            .build()
            .await
            .expect("producer connects to the mock");
        producer
            .init_transactions()
            .await
            .expect("init_transactions against the mock coordinator");
        (mock, producer)
    }

    macro_rules! end_txn_retry_test {
        ($name:ident, borrowed, $finish:ident) => {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                let end_txn_error = Arc::new(AtomicI16::new(49));
                let (mock, producer) = transactional_producer(end_txn_error.clone()).await;
                let txn = producer
                    .begin_transaction()
                    .await
                    .expect("begin_transaction");

                let err = txn
                    .$finish()
                    .await
                    .expect_err("broker reported CONCURRENT_TRANSACTIONS");
                assert2::assert!(matches!(err.source, ProducerError::ConcurrentTransactions));

                end_txn_error.store(0, Ordering::SeqCst);
                err.transaction.$finish().await.expect("retry succeeds");
                mock.stop();
            }
        };
        ($name:ident, owned, $finish:ident) => {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                let end_txn_error = Arc::new(AtomicI16::new(49));
                let (mock, producer) = transactional_producer(end_txn_error.clone()).await;
                let producer = Arc::new(producer);
                let txn = producer
                    .clone()
                    .begin_transaction_owned()
                    .await
                    .expect("begin_transaction_owned");

                let err = txn
                    .$finish()
                    .await
                    .expect_err("broker reported CONCURRENT_TRANSACTIONS");
                assert2::assert!(matches!(err.source, ProducerError::ConcurrentTransactions));

                end_txn_error.store(0, Ordering::SeqCst);
                err.transaction.$finish().await.expect("retry succeeds");
                mock.stop();
            }
        };
    }

    // CONCURRENT_TRANSACTIONS (49) proves `commit`/`abort` drive the broker
    // round trip, and the returned guard remains usable once the broker clears
    // the condition.
    end_txn_retry_test!(
        transaction_commit_reports_broker_error_and_retries_on_the_same_guard,
        borrowed,
        commit
    );
    end_txn_retry_test!(
        transaction_abort_reports_broker_error_and_retries_on_the_same_guard,
        borrowed,
        abort
    );
    end_txn_retry_test!(
        owned_transaction_commit_reports_broker_error_and_retries_on_the_same_guard,
        owned,
        commit
    );
    end_txn_retry_test!(
        owned_transaction_abort_reports_broker_error_and_retries_on_the_same_guard,
        owned,
        abort
    );
}
