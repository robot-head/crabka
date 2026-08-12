//! Client-side transactional state machine. It drives the
//! `init_transactions` / `begin` / `commit` / `abort` / `send_offsets_to_transaction`
//! flow.

use std::{fmt, str::FromStr, sync::Arc};

use crate::{error::ProducerError, producer::Producer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TxnState {
    /// The caller has not yet called `init_transactions`.
    Uninitialized,
    /// `init_transactions` succeeded, and no txn is in flight.
    Ready,
    /// Inside `begin_transaction` ... `commit/abort`.
    InTransaction,
    /// `prepare_transaction` has stopped new writes and is flushing records.
    Preparing,
    /// A 2PC transaction has been flushed and awaits its external decision.
    Prepared,
    /// `init_transactions_with_keep_prepared` is in flight.
    Initializing,
    /// A `commit` or an `abort` is in progress.
    CommittingOrAborting,
    /// A guard was dropped or `EndTxn` had an uncertain transport outcome.
    /// `init_transactions` must establish a new epoch before reuse.
    RecoveryRequired,
    /// The producer is fenced. No further txn is possible without a
    /// re-init.
    Fenced,
}

/// Stable identity of a transaction prepared for external two-phase commit.
///
/// Its string form is Kafka-compatible: `"producer_id:producer_epoch"`.
/// The empty string represents no transaction and round-trips through
/// [`Default`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreparedTransactionState {
    producer_id: i64,
    producer_epoch: i16,
}

impl PreparedTransactionState {
    /// Creates a prepared transaction identity.
    ///
    /// # Errors
    ///
    /// Returns [`PreparedTransactionStateParseError`] when either identity
    /// component is negative.
    pub fn new(
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<Self, PreparedTransactionStateParseError> {
        if producer_id < 0 || producer_epoch < 0 {
            return Err(PreparedTransactionStateParseError);
        }
        Ok(Self {
            producer_id,
            producer_epoch,
        })
    }

    /// Producer ID of the prepared transaction.
    #[must_use]
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Producer epoch of the prepared transaction.
    #[must_use]
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }

    /// Whether this token identifies a real transaction.
    #[must_use]
    pub const fn has_transaction(self) -> bool {
        self.producer_id >= 0
    }
}

impl Default for PreparedTransactionState {
    fn default() -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1,
        }
    }
}

impl fmt::Display for PreparedTransactionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.has_transaction() {
            write!(formatter, "{}:{}", self.producer_id, self.producer_epoch)
        } else {
            Ok(())
        }
    }
}

impl FromStr for PreparedTransactionState {
    type Err = PreparedTransactionStateParseError;

    fn from_str(serialized: &str) -> Result<Self, Self::Err> {
        if serialized.is_empty() {
            return Ok(Self::default());
        }
        let (producer_id, producer_epoch) = serialized
            .split_once(':')
            .ok_or(PreparedTransactionStateParseError)?;
        if producer_epoch.contains(':') {
            return Err(PreparedTransactionStateParseError);
        }
        Self::new(
            producer_id
                .parse()
                .map_err(|_| PreparedTransactionStateParseError)?,
            producer_epoch
                .parse()
                .map_err(|_| PreparedTransactionStateParseError)?,
        )
    }
}

/// Error returned when a prepared transaction token is malformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid prepared transaction state; expected producer_id:producer_epoch")]
pub struct PreparedTransactionStateParseError;

/// An open transaction, borrowing the [`Producer`] that opened it.
///
/// [`Producer::begin_transaction`] returns it. [`commit`](Self::commit) and
/// [`abort`](Self::abort) each consume `self` on success, so a transaction
/// cannot be silently reused or finished twice.
///
/// On failure the call hands the guard back through
/// [`EndTransactionError::transaction`] instead of dropping it. Kafka's
/// `EndTxn` contract makes some failures, such as `CONCURRENT_TRANSACTIONS`,
/// retryable against the very same broker-side transaction, so the caller can
/// retry `commit()` on the returned guard, or switch to `abort()`. For
/// non-retryable failures the producer's transaction state has already moved
/// on, and the returned guard's next `commit` or `abort` attempt fails
/// immediately.
///
/// A dropped unresolved guard marks the producer as recovery-required. The
/// producer never guesses whether Kafka committed or aborted the transaction.
/// The caller must call `init_transactions` before the producer can send or
/// begin again.
#[derive(Debug)]
#[must_use = "a transaction must be finished with `commit()` or `abort()`"]
pub struct Transaction<'p> {
    pub(crate) producer: &'p Producer,
    pub(crate) finished: bool,
    pub(crate) guard_generation: u64,
}

impl Transaction<'_> {
    /// Flushes and prepares this transaction for external two-phase commit.
    ///
    /// # Errors
    ///
    /// See [`Producer::prepare_transaction`].
    pub async fn prepare(&self) -> Result<PreparedTransactionState, ProducerError> {
        self.producer.prepare_transaction().await
    }

    /// Commit this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn commit(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(true).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn abort(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(false).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.producer
                .abandon_transaction_guard(self.guard_generation);
        }
    }
}

/// Same contract as [`Transaction`], but owns an `Arc<Producer>` instead of
/// borrowing it.
///
/// Use it when the caller must hold the guard across an owned or `'static`
/// boundary that a borrow cannot survive, for example behind a `dyn Trait`
/// object stored in a struct field across many separate async calls.
/// [`Producer::begin_transaction_owned`] returns it. It mirrors
/// `tokio::sync::Mutex::{lock, lock_owned}` and `MutexGuard`/`OwnedMutexGuard`.
#[derive(Debug)]
#[must_use = "a transaction must be finished with `commit()` or `abort()`"]
pub struct OwnedTransaction {
    pub(crate) producer: Arc<Producer>,
    pub(crate) finished: bool,
    pub(crate) guard_generation: u64,
}

impl OwnedTransaction {
    /// Flushes and prepares this transaction for external two-phase commit.
    ///
    /// # Errors
    ///
    /// See [`Producer::prepare_transaction`].
    pub async fn prepare(&self) -> Result<PreparedTransactionState, ProducerError> {
        self.producer.prepare_transaction().await
    }

    /// Commit this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn commit(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(true).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }

    /// Abort this transaction.
    ///
    /// # Errors
    ///
    /// See [`Producer::begin_transaction`] for the shared error conditions.
    /// On failure, `self` is returned via [`EndTransactionError::transaction`]
    /// so a retryable failure can be retried or aborted.
    pub async fn abort(mut self) -> Result<(), EndTransactionError<Self>> {
        match self.producer.end_transaction(false).await {
            Ok(()) => {
                self.finished = true;
                Ok(())
            }
            Err(source) => Err(EndTransactionError {
                transaction: self,
                source,
            }),
        }
    }
}

impl Drop for OwnedTransaction {
    fn drop(&mut self) {
        if !self.finished {
            self.producer
                .abandon_transaction_guard(self.guard_generation);
        }
    }
}

/// Error returned by [`Transaction::commit`], [`abort`](Transaction::abort),
/// and the [`OwnedTransaction`] equivalents.
///
/// It carries the guard back, so the caller can retry or abort a retryable
/// failure, such as `CONCURRENT_TRANSACTIONS`, on the same underlying
/// transaction instead of stranding it.
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct EndTransactionError<T> {
    /// The guard the `commit` or `abort` call was made on, handed back so the
    /// caller can retry `commit()` or call `abort()` on the same
    /// transaction.
    pub transaction: T,
    /// The underlying failure.
    #[source]
    pub source: ProducerError,
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicI16, AtomicU16, Ordering},
        },
        time::Duration,
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

    use super::{PreparedTransactionState, TxnState};
    use crate::{ProducerRecord, error::ProducerError, producer::Producer};

    #[test]
    fn prepared_transaction_state_has_stable_string_round_trip() {
        let state = PreparedTransactionState::new(42, 7).expect("valid transaction identity");

        assert2::assert!(state.producer_id() == 42);
        assert2::assert!(state.producer_epoch() == 7);
        assert2::assert!(state.has_transaction());
        assert2::assert!(state.to_string() == "42:7");
        assert2::assert!("42:7".parse::<PreparedTransactionState>() == Ok(state));
        assert2::assert!(PreparedTransactionState::default().to_string().is_empty());
        assert2::assert!(
            "".parse::<PreparedTransactionState>() == Ok(PreparedTransactionState::default())
        );
    }

    #[test]
    fn prepared_transaction_state_rejects_malformed_or_negative_identity() {
        for serialized in ["42", "-1:0", "1:-1", "1:2:3", "a:2", "1:b"] {
            assert2::assert!(serialized.parse::<PreparedTransactionState>().is_err());
        }
    }

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    /// Boots a mock broker that also answers as its own transaction
    /// coordinator, so `FindCoordinator` resolves back to the mock's own
    /// address. It returns a transactional `Producer` with `init_transactions`
    /// already completed against that broker.
    ///
    /// `end_txn_error` lets each test steer the `error_code` of the `EndTxn`
    /// response independently per call, where 0 means success. A test can
    /// therefore fail a `commit` or `abort`, and then flip the mock so that a
    /// retry on the same guard succeeds.
    async fn transactional_producer(end_txn_error: Arc<AtomicI16>) -> (MockBroker, Producer) {
        transactional_producer_with_end_txn_timeout(end_txn_error, Arc::new(AtomicBool::new(false)))
            .await
    }

    #[tokio::test]
    async fn init_transactions_retries_with_configured_policy() {
        let port_cell = Arc::new(AtomicU16::new(0));
        let handler_port = Arc::clone(&port_cell);
        let attempts = Arc::new(AtomicU16::new(0));
        let observed = Arc::clone(&attempts);
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
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                return Some(encode_v0(&InitProducerIdResponse {
                    error_code: if attempt == 0 { 14 } else { 0 },
                    producer_id: 7,
                    producer_epoch: 3,
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
            .request_timeout(Duration::from_millis(100))
            .retry_backoff(Duration::from_millis(1))
            .init_retry_timeout(Duration::from_millis(100))
            .init_max_backoff(Duration::from_millis(1))
            .build()
            .await
            .expect("producer connects");

        producer
            .init_transactions()
            .await
            .expect("cold coordinator retry succeeds");
        assert2::assert!(attempts.load(Ordering::SeqCst) == 2);
        mock.stop();
    }

    async fn transactional_producer_with_end_txn_timeout(
        end_txn_error: Arc<AtomicI16>,
        end_txn_silent: Arc<AtomicBool>,
    ) -> (MockBroker, Producer) {
        transactional_producer_configured(end_txn_error, end_txn_silent, false).await
    }

    async fn transactional_producer_configured(
        end_txn_error: Arc<AtomicI16>,
        end_txn_silent: Arc<AtomicBool>,
        two_phase_commit_enabled: bool,
    ) -> (MockBroker, Producer) {
        let port_cell = Arc::new(AtomicU16::new(0));
        let handler_port = port_cell.clone();
        let next_epoch = Arc::new(AtomicI16::new(3));
        let handler_epoch = Arc::clone(&next_epoch);
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
                    producer_epoch: handler_epoch.fetch_add(1, Ordering::SeqCst),
                    ..Default::default()
                }));
            }
            if api_key == end_txn_request::API_KEY {
                if end_txn_silent.load(Ordering::SeqCst) {
                    return None;
                }
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
            .transaction_two_phase_commit_enable(two_phase_commit_enabled)
            .request_timeout(std::time::Duration::from_millis(100))
            .build()
            .await
            .expect("producer connects to the mock");
        producer
            .init_transactions()
            .await
            .expect("init_transactions against the mock coordinator");
        (mock, producer)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepared_guard_can_drop_without_poisoning_the_next_transaction() {
        let end_txn_error = Arc::new(AtomicI16::new(0));
        let (mock, producer) = transactional_producer_configured(
            end_txn_error,
            Arc::new(AtomicBool::new(false)),
            true,
        )
        .await;
        let transaction = producer
            .begin_transaction()
            .await
            .expect("begin transaction");
        let prepared = transaction.prepare().await.expect("prepare transaction");

        assert2::assert!(*producer.txn_state.lock().await == TxnState::Prepared);
        drop(transaction);
        assert2::assert!(!producer.txn_recovery_required.load(Ordering::Acquire));
        producer
            .complete_transaction(prepared)
            .await
            .expect("complete prepared transaction");
        producer
            .begin_transaction()
            .await
            .expect("begin next transaction")
            .abort()
            .await
            .expect("abort next transaction");
        mock.stop();
    }

    macro_rules! end_txn_retry_test {
        ($name:ident, borrowed, $finish:ident) => {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                let end_txn_error = Arc::new(AtomicI16::new(51));
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
                let end_txn_error = Arc::new(AtomicI16::new(51));
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

    // CONCURRENT_TRANSACTIONS (51) proves `commit`/`abort` drive the broker
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uncertain_end_txn_requires_reinitialization_before_reuse() {
        let end_txn_error = Arc::new(AtomicI16::new(0));
        let end_txn_silent = Arc::new(AtomicBool::new(true));
        let (mock, producer) =
            transactional_producer_with_end_txn_timeout(end_txn_error, end_txn_silent.clone())
                .await;

        let error = producer
            .begin_transaction()
            .await
            .expect("begin transaction")
            .commit()
            .await
            .expect_err("silent EndTxn has an uncertain outcome");
        assert!(matches!(error.source, ProducerError::Client(_)));
        drop(error.transaction);

        assert!(matches!(
            producer.begin_transaction().await,
            Err(ProducerError::RecoveryRequired)
        ));
        let acknowledgement = producer.send(ProducerRecord::default()).await;
        assert!(matches!(
            acknowledgement.await.expect("recovery error is delivered"),
            Err(ProducerError::RecoveryRequired)
        ));

        end_txn_silent.store(false, Ordering::SeqCst);
        producer
            .init_transactions()
            .await
            .expect("reinitialization obtains a new epoch");
        assert_eq!(*producer.txn_pid_epoch.lock().await, (7, 4));
        producer
            .begin_transaction()
            .await
            .expect("new epoch permits a transaction")
            .abort()
            .await
            .expect("abort after recovery");
        mock.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_an_open_transaction_requires_explicit_recovery() {
        let end_txn_error = Arc::new(AtomicI16::new(0));
        let (mock, producer) = transactional_producer(end_txn_error).await;
        drop(
            producer
                .begin_transaction()
                .await
                .expect("begin transaction before drop"),
        );

        assert!(matches!(
            producer.begin_transaction().await,
            Err(ProducerError::RecoveryRequired)
        ));
        producer
            .init_transactions()
            .await
            .expect("explicit initialization recovers dropped transaction");
        producer
            .begin_transaction()
            .await
            .expect("begin after recovery")
            .abort()
            .await
            .expect("abort after recovery");
        mock.stop();
    }
}
