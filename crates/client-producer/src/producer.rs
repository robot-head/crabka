//! `Producer` — public type. Builder lives in `builder.rs`. Sender task
//! lives in `sender.rs`.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crabka_client_consumer::ConsumerGroupMetadata;
use crabka_client_core::{Client, security::ClientSecurity};
use crabka_protocol::owned::{
    add_offsets_to_txn_request::AddOffsetsToTxnRequest,
    end_txn_request::EndTxnRequest,
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
    txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    },
};
use dashmap::DashMap;
use tokio::{
    sync::{Mutex, Notify, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    accumulator::{Accumulator, AppendResult},
    compression::Compression,
    error::ProducerError,
    partitioner::UniformStickyPartitioner,
    record::{ProducerRecord, RecordMetadata},
    transactional::{OwnedTransaction, Transaction, TxnState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    Zero,
    One,
    All,
}

impl Acks {
    #[must_use]
    pub fn wire(self) -> i16 {
        match self {
            Acks::Zero => 0,
            Acks::One => 1,
            Acks::All => -1,
        }
    }
}

/// Tri-state lifecycle.
pub(crate) const STATE_ACTIVE: u8 = 0;
pub(crate) const STATE_FENCED: u8 = 1;
pub(crate) const STATE_CLOSED: u8 = 2;

#[derive(Debug, Clone)]
pub(crate) struct TopicMetadata {
    pub num_partitions: i32,
    /// Topic UUID. Needed for Produce v13+, which encodes only the
    /// `topic_id` on the wire. Zero (`Uuid::ZERO`) is a valid sentinel
    /// meaning "not yet known" — the broker falls back to the `name`
    /// field for older wire versions.
    pub topic_id: crabka_protocol::primitives::uuid::Uuid,
}

#[allow(clippy::struct_field_names)] // producer_id / producer_epoch intentionally match struct name
#[allow(clippy::type_complexity)] // accumulators map is inherently complex
pub struct Producer {
    pub(crate) client: Client,
    pub(crate) client_id: String,
    /// TLS/SASL security policy used for the bootstrap connection. Retained
    /// so every secondary connection the producer opens after construction
    /// (transaction-coordinator and group-coordinator dials in the
    /// transactional path) carries the same credentials. Without this, those
    /// connections would be plaintext/unauthenticated and a secured listener
    /// drops them, failing the transactional flow with `Client(Disconnected)`.
    pub(crate) security: Option<ClientSecurity>,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    // The following config knobs are also copied into `SenderConfig` at
    // construction time. They live on `Producer` for diagnostic
    // introspection and to support future reconnect / re-init flows.
    // Suppressing the dead-code warning is honest about
    // their current role.
    #[allow(dead_code)]
    pub(crate) acks: Acks,
    pub(crate) compression: Compression,
    pub(crate) batch_size: usize,
    #[allow(dead_code)]
    pub(crate) linger: Duration,
    #[allow(dead_code)]
    pub(crate) request_timeout: Duration,
    #[allow(dead_code)]
    pub(crate) retries: i32,
    #[allow(dead_code)]
    pub(crate) retry_backoff: Duration,
    #[allow(dead_code)]
    pub(crate) max_in_flight: usize,
    pub(crate) metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    /// Per-`(topic, partition)` leader-id cache used by the sender to route
    /// each Produce to the broker that actually leads the partition. Populated
    /// from `Metadata` alongside the partition count (see `partitions_for`); a
    /// missing entry means "leader unknown → fall back to the bootstrap
    /// connection". A leader id `< 0` is also treated as unknown. Shared with
    /// the sender task via `Arc`.
    pub(crate) partition_leaders: Arc<DashMap<(String, i32), i32>>,
    pub(crate) accumulators: Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>,
    #[allow(dead_code)]
    pub(crate) next_seq: Arc<DashMap<(String, i32), i32>>,
    pub(crate) partitioner: Arc<UniformStickyPartitioner>,
    pub(crate) state: Arc<AtomicU8>,
    pub(crate) wake_tx: tokio::sync::mpsc::Sender<()>,
    pub(crate) flush_notify: Arc<Notify>,
    /// Count of batches the sender has popped from an accumulator but not yet
    /// finished sending (Produce in-flight, awaiting the broker ack). A batch
    /// that has left the accumulator but is still in-flight is invisible to
    /// `all_empty`, so `flush` must also wait for this to reach zero — otherwise
    /// `commit_transaction` can race ahead of the Produce that drives the txn to
    /// `Ongoing` and the coordinator rejects `EndTxn` with `INVALID_TXN_STATE`.
    pub(crate) in_flight: Arc<AtomicUsize>,
    pub(crate) sender_shutdown: CancellationToken,
    pub(crate) sender_handle: Option<JoinHandle<()>>,
    pub(crate) transactional_id: Option<String>,
    pub(crate) transaction_timeout: Duration,
    /// Arc-wrapped so the sender task can share the same state without
    /// additional synchronization structures.
    pub(crate) txn_state: Arc<Mutex<TxnState>>,
    /// Cached connection to the transaction coordinator broker.
    /// Populated by `init_transactions`; reused by begin/commit/abort.
    pub(crate) txn_coord_client: Mutex<Option<Client>>,
    /// Authoritative `(producer_id, producer_epoch)` for the transactional
    /// flow. Set by `init_transactions`; read by the sender when building
    /// transactional `ProduceRequest`s.
    pub(crate) txn_pid_epoch: Arc<Mutex<(i64, i16)>>,
}

impl Producer {
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    #[must_use]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    // ── Transactional API ────────────────────────────────────────────────────

    /// Begin a new transaction, returning a borrowed guard that must be
    /// finished with [`Transaction::commit`] or [`Transaction::abort`].
    ///
    /// Must be called after [`init_transactions`] has completed and before
    /// any transactional [`send`] calls. Transitions the producer from
    /// `Ready` → `InTransaction`.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — producer is not in the
    ///   `Ready` state (e.g. `init_transactions` not yet called, or a
    ///   transaction is already in flight).
    ///
    /// [`init_transactions`]: Self::init_transactions
    /// [`send`]: Self::send
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(transactional_id = self.transactional_id.as_deref()),
        err,
    )]
    pub async fn begin_transaction(&self) -> Result<Transaction<'_>, ProducerError> {
        self.begin_transaction_state().await?;
        Ok(Transaction { producer: self })
    }

    /// Begin a new transaction, returning an owning guard that must be
    /// finished with [`OwnedTransaction::commit`] or
    /// [`OwnedTransaction::abort`].
    ///
    /// Identical semantics to [`begin_transaction`](Self::begin_transaction),
    /// but the returned guard owns an `Arc<Producer>` instead of borrowing
    /// `&self`. Use this when the guard must survive across an owned/`'static`
    /// boundary a borrow can't — e.g. stored behind a `dyn Trait` object.
    /// Mirrors `tokio::sync::Mutex::lock_owned`.
    ///
    /// # Errors
    ///
    /// Same as [`begin_transaction`](Self::begin_transaction).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(transactional_id = self.transactional_id.as_deref()),
        err,
    )]
    pub async fn begin_transaction_owned(
        self: Arc<Self>,
    ) -> Result<OwnedTransaction, ProducerError> {
        self.begin_transaction_state().await?;
        Ok(OwnedTransaction { producer: self })
    }

    async fn begin_transaction_state(&self) -> Result<(), ProducerError> {
        if self.transactional_id.is_none() {
            return Err(ProducerError::NotTransactional);
        }
        let mut state = self.txn_state.lock().await;
        match *state {
            TxnState::Ready => {
                *state = TxnState::InTransaction;
                Ok(())
            }
            _ => Err(ProducerError::InvalidTransactionState(
                "begin_transaction must be called after init_transactions and not while another txn is in flight",
            )),
        }
    }

    /// Finish the current transaction: flushes all in-flight records, then
    /// sends `EndTxn(committed)` to the transaction coordinator. Transitions
    /// the producer from `InTransaction` → `Ready` on success.
    ///
    /// Called by [`Transaction::commit`]/[`Transaction::abort`] and
    /// [`OwnedTransaction::commit`]/[`OwnedTransaction::abort`] — the only
    /// ways to finish a transaction opened via `begin_transaction`/
    /// `begin_transaction_owned`.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — not currently in a transaction.
    /// - [`ProducerError::FencedProducer`] — broker returned `INVALID_PRODUCER_EPOCH (47)`.
    /// - [`ProducerError::ConcurrentTransactions`] — broker returned `CONCURRENT_TRANSACTIONS (49)`; caller may retry.
    /// - [`ProducerError::Server`] — any other broker error code.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(committed, error_code = tracing::field::Empty),
        err,
    )]
    pub(crate) async fn end_transaction(&self, committed: bool) -> Result<(), ProducerError> {
        let tid = self
            .transactional_id
            .clone()
            .ok_or(ProducerError::NotTransactional)?;

        // 1. Flush all in-flight records (block until acks).
        self.flush().await?;

        let mut state = self.txn_state.lock().await;
        if !matches!(*state, TxnState::InTransaction) {
            return Err(ProducerError::InvalidTransactionState(
                "commit/abort_transaction must follow begin_transaction",
            ));
        }
        *state = TxnState::CommittingOrAborting;
        drop(state);

        // 2. Retrieve the cached coordinator connection.
        let coord_guard = self.txn_coord_client.lock().await;
        let coord = coord_guard
            .as_ref()
            .ok_or(ProducerError::InvalidTransactionState(
                "no txn coordinator cached — did init_transactions succeed?",
            ))?
            .clone();
        drop(coord_guard);

        let (pid, epoch) = *self.txn_pid_epoch.lock().await;

        // 3. Send EndTxn to the coordinator.
        let resp = coord
            .send(EndTxnRequest {
                transactional_id: tid,
                producer_id: pid,
                producer_epoch: epoch,
                committed,
                ..Default::default()
            })
            .await?;

        tracing::Span::current().record("error_code", resp.error_code);
        let mut state = self.txn_state.lock().await;
        match resp.error_code {
            0 => {
                // KIP-890 (transaction.version 2): the coordinator bumps the
                // producer epoch on transaction completion and returns the new
                // (producer_id, producer_epoch) in the EndTxn v5 response. Adopt
                // it so the next transaction (and its record batches, which read
                // this shared pair) use the un-fenced epoch. A pre-KIP-890
                // coordinator leaves these at -1, in which case we keep the
                // current pair unchanged.
                if resp.producer_id >= 0 {
                    *self.txn_pid_epoch.lock().await = (resp.producer_id, resp.producer_epoch);
                }
                *state = TxnState::Ready;
                Ok(())
            }
            47 /* INVALID_PRODUCER_EPOCH */ => {
                *state = TxnState::Fenced;
                Err(ProducerError::FencedProducer)
            }
            49 /* CONCURRENT_TRANSACTIONS */ => {
                *state = TxnState::InTransaction; // Caller can retry.
                Err(ProducerError::ConcurrentTransactions)
            }
            other => {
                *state = TxnState::Ready;
                Err(ProducerError::Server(other))
            }
        }
    }

    /// Initialize the transactional producer.
    ///
    /// Must be called before any transactional operations. Discovers the
    /// transaction coordinator via `FindCoordinator`, opens a dedicated
    /// connection to it, and calls `InitProducerId` to obtain a fenced
    /// `(producer_id, producer_epoch)` pair.
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — called while a
    ///   transaction is in flight.
    /// - [`ProducerError::FencedProducer`] — the broker returned
    ///   `INVALID_PRODUCER_EPOCH (47)`.
    /// - [`ProducerError::Server`] — any other broker error code.
    /// - [`ProducerError::Client`] — transport-level failure.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            transactional_id = self.transactional_id.as_deref(),
            coordinator = tracing::field::Empty,
            producer_id = tracing::field::Empty,
            producer_epoch = tracing::field::Empty,
            error_code = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn init_transactions(&self) -> Result<(), ProducerError> {
        let Some(tid) = self.transactional_id.as_deref() else {
            return Err(ProducerError::NotTransactional);
        };

        let mut state = self.txn_state.lock().await;
        if !matches!(
            *state,
            TxnState::Uninitialized | TxnState::Ready | TxnState::Fenced
        ) {
            return Err(ProducerError::InvalidTransactionState(
                "init_transactions called while a transaction is in flight",
            ));
        }

        let coord_addr = self.find_txn_coordinator(tid).await?;
        tracing::Span::current().record("coordinator", coord_addr.as_str());

        let coord = Client::builder()
            .bootstrap(coord_addr)
            .client_id(self.client_id.clone())
            .maybe_security(self.security.clone())
            .build()
            .await?;

        let timeout_ms = i32::try_from(self.transaction_timeout.as_millis()).unwrap_or(60_000);

        let resp = coord
            .send(InitProducerIdRequest {
                transactional_id: Some(tid.to_owned()),
                transaction_timeout_ms: timeout_ms,
                ..Default::default()
            })
            .await?;

        tracing::Span::current().record("error_code", resp.error_code);
        match resp.error_code {
            0 => {
                tracing::Span::current().record("producer_id", resp.producer_id);
                tracing::Span::current().record("producer_epoch", resp.producer_epoch);
                *self.txn_pid_epoch.lock().await = (resp.producer_id, resp.producer_epoch);
                *self.txn_coord_client.lock().await = Some(coord);
                *state = TxnState::Ready;
                Ok(())
            }
            47 /* INVALID_PRODUCER_EPOCH */ => {
                *state = TxnState::Fenced;
                Err(ProducerError::FencedProducer)
            }
            other => Err(ProducerError::Server(other)),
        }
    }

    /// Discover the transaction coordinator for `tid` via `FindCoordinator`.
    ///
    /// Handles both the legacy top-level response (versions 0–3) and the
    /// `coordinators` array introduced in version 4.
    #[tracing::instrument(level = "debug", skip_all, fields(transactional_id = %tid), err)]
    async fn find_txn_coordinator(&self, tid: &str) -> Result<String, ProducerError> {
        self.find_coordinator(tid, 1).await
    }

    async fn find_coordinator(&self, key: &str, key_type: i8) -> Result<String, ProducerError> {
        let resp = self
            .client
            .send(FindCoordinatorRequest {
                // v0-3: the `key` field carries the lookup key
                key: key.to_owned(),
                key_type,
                // v4+: repeated coordinator_keys list
                coordinator_keys: vec![key.to_owned()],
                ..Default::default()
            })
            .await?;

        // v4+ returns a `coordinators` array; prefer it when present.
        if let Some(coord) = resp.coordinators.first() {
            if coord.error_code != 0 {
                return Err(ProducerError::Server(coord.error_code));
            }
            return Ok(format!("{}:{}", coord.host, coord.port));
        }

        // Fallback: legacy top-level host/port (versions 0–3).
        if resp.error_code != 0 {
            return Err(ProducerError::Server(resp.error_code));
        }
        Ok(format!("{}:{}", resp.host, resp.port))
    }

    /// Enroll a consumer group's offsets in the current transaction, fencing
    /// zombie producers via the supplied [`ConsumerGroupMetadata`] (KIP-447).
    ///
    /// Performs two broker round-trips:
    ///
    /// 1. `AddOffsetsToTxn` → transaction coordinator (registers the group
    ///    offset commit as part of the ongoing transaction).
    /// 2. `TxnOffsetCommit` → group coordinator (commits the actual offsets
    ///    transactionally, carrying `group_meta`'s generation/member/instance
    ///    so the coordinator can fence stale producers).
    ///
    /// # Errors
    ///
    /// - [`ProducerError::NotTransactional`] — `transactional_id` was not set.
    /// - [`ProducerError::InvalidTransactionState`] — no cached transaction
    ///   coordinator (call [`init_transactions`] first).
    /// - [`ProducerError::Server`] — any broker error code (checked at the
    ///   `AddOffsetsToTxn` level and per-partition for `TxnOffsetCommit`).
    /// - [`ProducerError::Client`] — transport-level failure.
    ///
    /// [`init_transactions`]: Self::init_transactions
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            transactional_id = self.transactional_id.as_deref(),
            group_id = %group_meta.group_id,
            generation_id = group_meta.generation_id,
            offset_count = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: impl IntoIterator<Item = ((String, i32), i64)>,
        group_meta: &ConsumerGroupMetadata,
    ) -> Result<(), ProducerError> {
        let tid = self
            .transactional_id
            .as_deref()
            .ok_or(ProducerError::NotTransactional)?
            .to_string();
        let offsets_vec: Vec<_> = offsets.into_iter().collect();
        tracing::Span::current().record("offset_count", offsets_vec.len());

        let (pid, epoch) = *self.txn_pid_epoch.lock().await;

        // 1. AddOffsetsToTxn → transaction coordinator.
        let coord_guard = self.txn_coord_client.lock().await;
        let coord = coord_guard
            .as_ref()
            .ok_or(ProducerError::InvalidTransactionState(
                "no txn coordinator cached — did init_transactions succeed?",
            ))?
            .clone();
        drop(coord_guard);

        let r1 = coord
            .send(AddOffsetsToTxnRequest {
                transactional_id: tid.clone(),
                producer_id: pid,
                producer_epoch: epoch,
                group_id: group_meta.group_id.clone(),
                ..Default::default()
            })
            .await?;
        if r1.error_code != 0 {
            return Err(ProducerError::Server(r1.error_code));
        }

        // 2. FindCoordinator(group_id, key_type=0 GROUP) for the group coordinator.
        let group_addr = self.find_group_coordinator(&group_meta.group_id).await?;
        let group_client = Client::builder()
            .bootstrap(group_addr)
            .client_id(self.client_id.clone())
            .maybe_security(self.security.clone())
            .build()
            .await?;

        // 3. TxnOffsetCommit → group coordinator, carrying the consumer group
        //    metadata (generation id / member id / instance id) so the
        //    coordinator can fence zombie producers via the group's own state
        //    rather than requiring one producer per input partition (KIP-447).
        let r2 = group_client
            .send(TxnOffsetCommitRequest {
                transactional_id: tid,
                producer_id: pid,
                producer_epoch: epoch,
                group_id: group_meta.group_id.clone(),
                generation_id: group_meta.generation_id,
                member_id: group_meta.member_id.clone(),
                group_instance_id: group_meta.group_instance_id.clone(),
                topics: build_topics_payload(&offsets_vec),
                ..Default::default()
            })
            .await?;

        // Check per-partition error codes.
        for topic in &r2.topics {
            for p in &topic.partitions {
                if p.error_code != 0 {
                    return Err(ProducerError::Server(p.error_code));
                }
            }
        }
        Ok(())
    }

    /// Discover the group coordinator for `group_id` via `FindCoordinator`
    /// with `key_type = 0` (GROUP).
    ///
    /// Mirrors [`find_txn_coordinator`] but uses `key_type = 0` and looks up
    /// the group coordinator rather than the transaction coordinator.
    ///
    /// [`find_txn_coordinator`]: Self::find_txn_coordinator
    #[tracing::instrument(level = "debug", skip_all, fields(group_id = %group_id), err)]
    async fn find_group_coordinator(&self, group_id: &str) -> Result<String, ProducerError> {
        self.find_coordinator(group_id, 0).await
    }

    // ── Internal lifecycle ───────────────────────────────────────────────────

    pub(crate) fn is_active(&self) -> Result<(), ProducerError> {
        match self.state.load(Ordering::Acquire) {
            STATE_ACTIVE => Ok(()),
            STATE_FENCED => Err(ProducerError::FencedProducer),
            _ => Err(ProducerError::Closed),
        }
    }

    #[allow(dead_code)] // wired by sender on INVALID_PRODUCER_EPOCH; kept for symmetry
    pub(crate) fn fence(&self) {
        self.state
            .compare_exchange(
                STATE_ACTIVE,
                STATE_FENCED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok();
    }

    /// Enqueue a record and return a future that resolves when the broker
    /// acks (or the producer fences / closes).
    ///
    /// Returns a `oneshot::Receiver`. The outer call is `async` because
    /// partition resolution may need to fetch metadata over the wire.
    // The instrument macro wraps the body in an async block whose value is the
    // returned `oneshot::Receiver` — itself awaitable — which trips
    // `async_yields_async`. Returning the un-awaited receiver is intentional
    // here (the caller awaits the broker ack), so allow the lint.
    #[allow(clippy::async_yields_async)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(topic = %record.topic, partition = tracing::field::Empty),
    )]
    pub async fn send(
        &self,
        record: ProducerRecord,
    ) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>> {
        if let Err(e) = self.is_active() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(e));
            return rx;
        }

        let partition = match record.partition {
            Some(p) => {
                // Produce v13 omits the topic name on the wire and carries
                // only `topic_id`, so the metadata cache must hold the topic
                // even when the caller pins the partition itself. The
                // partitioner path populates it via `partition_for`; mirror
                // that here so explicit-partition sends resolve a non-zero
                // `topic_id` instead of failing with UNKNOWN_TOPIC_OR_PARTITION.
                self.partitions_for(&record.topic).await;
                p
            }
            None => {
                self.partition_for(&record.topic, record.key.as_deref())
                    .await
            }
        };
        tracing::Span::current().record("partition", partition);

        let key = (record.topic.clone(), partition);
        let acc = Arc::clone(
            self.accumulators
                .entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(self.batch_size))))
                .value(),
        );

        let timestamp = record.timestamp_ms.unwrap_or_else(current_millis);
        let mut a = acc.lock().await;
        // try_append currently only ever returns `Appended`; if a future
        // change adds `BatchFull` we want a compile error, so match
        // exhaustively rather than `let ... else`.
        let rx = match a.try_append(record.key, record.value, record.headers, timestamp) {
            AppendResult::Appended(rx) => rx,
            AppendResult::BatchFull => {
                // Should not happen with the current implementation; treat
                // as transient and fail the caller rather than panic.
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(Err(ProducerError::BufferFull));
                rx
            }
        };
        let _ = self.wake_tx.try_send(());
        rx
    }

    /// Resolve the destination partition for a record. Hashes the key when
    /// present, otherwise consults the sticky partitioner. Fetches and
    /// caches topic metadata on first reference.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(topic = %topic, keyed = key.is_some()),
    )]
    async fn partition_for(&self, topic: &str, key: Option<&[u8]>) -> i32 {
        let num_partitions = self.partitions_for(topic).await;
        self.partitioner.pick(topic, key, num_partitions)
    }

    /// Return the partition count for `topic`, fetching metadata on cache
    /// miss. Falls back to `1` if the broker reports an error or the
    /// topic is absent — production code can revisit retry
    /// policy here.
    ///
    /// On a cache miss this uses [`Client::refresh_metadata`] rather than a
    /// bare `send(MetadataRequest)`: `refresh_metadata` also teaches the
    /// client's `BrokerPool` each broker's `(id → addr)` mapping, which is what
    /// lets the sender route a Produce to the partition *leader* via
    /// `Client::broker(id)` instead of always hitting the bootstrap connection.
    /// We then record each partition's `leader_id` in `partition_leaders` for
    /// the sender to consult.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(topic = %topic, num_partitions = tracing::field::Empty),
    )]
    async fn partitions_for(&self, topic: &str) -> i32 {
        {
            let m = self.metadata_cache.lock().await;
            if let Some(meta) = m.get(topic) {
                tracing::Span::current().record("num_partitions", meta.num_partitions);
                return meta.num_partitions;
            }
        }
        // Cache miss: refresh. `refresh_metadata` sends a (full-cluster)
        // MetadataRequest, returns the response, AND refreshes the pool's
        // broker address registry so per-leader routing can connect.
        match self.client.refresh_metadata().await {
            Ok(resp) => {
                let topic_meta = resp
                    .topics
                    .iter()
                    .find(|t| t.name.as_deref() == Some(topic));
                // Non-zero per-topic error_code (e.g. UNKNOWN_TOPIC_OR_PARTITION = 3)
                // means the broker didn't fill in the partition list — fall back
                // to a default of 1 so the caller can still attempt the send.
                let (count, topic_id) = match topic_meta {
                    Some(t) if t.error_code == 0 => {
                        let count = i32::try_from(t.partitions.len()).unwrap_or(1).max(1);
                        // Cache the per-partition leader id so the sender can
                        // route each Produce to the partition leader.
                        for part in &t.partitions {
                            self.partition_leaders
                                .insert((topic.to_string(), part.partition_index), part.leader_id);
                        }
                        (count, t.topic_id)
                    }
                    _ => (1, crabka_protocol::primitives::uuid::Uuid::ZERO),
                };
                // NOTE: an unresolved lookup is cached as `{count: 1, topic_id:
                // ZERO}` to avoid a metadata-refresh storm (this runs per record
                // on the produce path). That entry is later corrected in place by
                // `update_leaders_from_metadata` once the topic exists — which is
                // essential: a frozen ZERO `topic_id` makes Produce v≥13 (name
                // dropped on the wire) return an un-correlatable UNKNOWN_TOPIC
                // response that the sender would retry forever.
                let mut m = self.metadata_cache.lock().await;
                m.insert(
                    topic.to_string(),
                    TopicMetadata {
                        num_partitions: count,
                        topic_id,
                    },
                );
                tracing::Span::current().record("num_partitions", count);
                count
            }
            Err(_) => 1,
        }
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(client_id = %self.client_id, transactional_id = self.transactional_id.as_deref()),
        err,
    )]
    pub async fn close(mut self) -> Result<(), ProducerError> {
        self.flush().await?;
        self.state.store(STATE_CLOSED, Ordering::Release);
        self.sender_shutdown.cancel();
        if let Some(h) = self.sender_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    pub async fn flush(&self) -> Result<(), ProducerError> {
        self.is_active()?;
        let _ = self.wake_tx.send(()).await;
        for _ in 0..1000 {
            if self.all_empty().await && self.in_flight.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            let _ =
                tokio::time::timeout(Duration::from_millis(50), self.flush_notify.notified()).await;
        }
        Err(ProducerError::FlushTimeout)
    }

    async fn all_empty(&self) -> bool {
        for entry in self.accumulators.iter() {
            let a = entry.value().lock().await;
            if a.current.as_ref().is_some_and(|b| !b.is_empty()) {
                return false;
            }
            if !a.ready.is_empty() {
                return false;
            }
        }
        true
    }
}

impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("transactional_id", &self.transactional_id)
            .field("compression", &self.compression)
            .finish_non_exhaustive()
    }
}

fn current_millis() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

/// Group `((topic, partition), offset)` pairs by topic name into the nested
/// structure required by [`TxnOffsetCommitRequest`].
fn build_topics_payload(offsets: &[((String, i32), i64)]) -> Vec<TxnOffsetCommitRequestTopic> {
    let mut by_topic: std::collections::HashMap<&str, Vec<TxnOffsetCommitRequestPartition>> =
        std::collections::HashMap::new();
    for ((topic, partition), offset) in offsets {
        by_topic
            .entry(topic.as_str())
            .or_default()
            .push(TxnOffsetCommitRequestPartition {
                partition_index: *partition,
                committed_offset: *offset,
                ..Default::default()
            });
    }
    by_topic
        .into_iter()
        .map(|(name, partitions)| TxnOffsetCommitRequestTopic {
            name: name.to_owned(),
            partitions,
            ..Default::default()
        })
        .collect()
}
