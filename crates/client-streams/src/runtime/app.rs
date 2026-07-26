//! `KafkaStreams` — the managed runtime handle. Owns membership + a `StreamThread`.
//!
//! `start()` builds the broker I/O, joins the streams group (membership owns the
//! heartbeat), and spawns a supervisor that pumps membership events into a
//! `StreamThread` while polling/committing on intervals. `close()` stops the
//! supervisor (flush+commit+leave).

use std::{sync::Arc, time::Duration};

use crabka_client_core::ClientDnsTimeout;
use refined_type::rule::{MinMaxU128, MinMaxUsize};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    error::StreamsClientError,
    membership::{
        DEFAULT_STREAMS_JOIN_RETRY_BACKOFF, DEFAULT_STREAMS_REBALANCE_TIMEOUT, StreamsEvent,
        StreamsJoinRetryBackoff, StreamsMembership, StreamsRebalanceTimeout,
    },
    processor::serde::Serde,
    runtime::{
        eos::{ProcessingGuarantee, TransactionalProducer},
        io::{OffsetStore, RecordFetcher, RecordProducer},
        io_broker,
        iq::IqRequest,
        iq_view::ReadOnlyKeyValueStore,
        iqv2::{Query, StateQuery, StateQueryResult, dispatch::Iq2Request},
        thread::StreamThread,
    },
    store::iq::StoreKind,
    topology::BuiltTopology,
};

/// Default delay between Client Streams processing polls.
pub const DEFAULT_STREAMS_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Default delay between Client Streams commit attempts.
pub const DEFAULT_STREAMS_COMMIT_INTERVAL: Duration = Duration::from_secs(5);
/// Default capacity of each Client Streams interactive-query request queue.
pub const DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY: usize = 64;

/// Tokio-supported capacity shared by the Client Streams interactive-query queues.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsInteractiveQueryQueueCapacity(usize);

impl StreamsInteractiveQueryQueueCapacity {
    /// Validate an interactive-query queue capacity.
    ///
    /// # Errors
    ///
    /// Returns an error unless `value` is within Tokio's supported channel
    /// capacity range.
    pub fn new(value: usize) -> Result<Self, String> {
        MinMaxUsize::<1, { tokio::sync::Semaphore::MAX_PERMITS }>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("streams interactive-query queue capacity: {error}"))
    }

    /// Return the validated capacity.
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.0
    }
}

impl Default for StreamsInteractiveQueryQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY)
            .expect("default streams interactive-query queue capacity is valid")
    }
}

fn interactive_query_queue_capacities(
    capacity: StreamsInteractiveQueryQueueCapacity,
) -> [usize; 2] {
    [capacity.capacity(); 2]
}

fn validate_positive_whole_milliseconds(field: &str, value: Duration) -> Result<u64, String> {
    let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
        .map_err(|error| format!("{field}: {error}"))?
        .into_value();
    let milliseconds = u64::try_from(milliseconds).map_err(|error| format!("{field}: {error}"))?;
    if Duration::from_millis(milliseconds) != value {
        return Err(format!("{field} must be a whole number of milliseconds"));
    }
    Ok(milliseconds)
}

/// Positive, whole-millisecond Client Streams processing poll interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsPollInterval(Duration);

impl StreamsPollInterval {
    /// Validate a processing poll interval.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        validate_positive_whole_milliseconds("streams poll interval", value)?;
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated whole milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the validated interval no longer fits in `u64`.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis()).expect("validated streams poll interval fits u64")
    }
}

impl Default for StreamsPollInterval {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_POLL_INTERVAL).expect("default streams poll interval is valid")
    }
}

/// Positive, whole-millisecond Client Streams commit interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsCommitInterval(Duration);

impl StreamsCommitInterval {
    /// Validate a commit interval.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        validate_positive_whole_milliseconds("streams commit interval", value)?;
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated whole milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the validated interval no longer fits in `u64`.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis()).expect("validated streams commit interval fits u64")
    }
}

impl Default for StreamsCommitInterval {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_COMMIT_INTERVAL)
            .expect("default streams commit interval is valid")
    }
}

fn validate_runtime_configuration(
    poll_interval: Duration,
    commit_interval: Duration,
    rebalance_timeout: Duration,
    join_retry_backoff: Duration,
) -> Result<
    (
        StreamsPollInterval,
        StreamsCommitInterval,
        StreamsRebalanceTimeout,
        StreamsJoinRetryBackoff,
    ),
    StreamsClientError,
> {
    let poll_interval =
        StreamsPollInterval::new(poll_interval).map_err(StreamsClientError::Runtime)?;
    let commit_interval =
        StreamsCommitInterval::new(commit_interval).map_err(StreamsClientError::Runtime)?;
    let rebalance_timeout =
        StreamsRebalanceTimeout::new(rebalance_timeout).map_err(StreamsClientError::Runtime)?;
    let join_retry_backoff =
        StreamsJoinRetryBackoff::new(join_retry_backoff).map_err(StreamsClientError::Runtime)?;
    Ok((
        poll_interval,
        commit_interval,
        rebalance_timeout,
        join_retry_backoff,
    ))
}

/// A managed Kafka Streams runtime: joins a streams group, runs assigned tasks
/// (fetch → process → produce → commit, at-least-once), and reacts to rebalances.
pub struct KafkaStreams {
    member_id: String,
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
    /// Channel to the supervisor for interactive queries. Read by the
    /// `KafkaStreams` IQ accessors.
    iq_tx: mpsc::Sender<IqRequest>,
    /// Channel to the supervisor for `IQv2` queries (separate from the v1 `iq_tx`).
    iq2_tx: mpsc::Sender<Iq2Request>,
}

#[bon::bon]
impl KafkaStreams {
    #[builder(start_fn = builder, finish_fn = build)]
    // one-shot constructor: broker I/O setup,
    // membership join, and the supervisor select-loop (now two IQ channels).
    #[tracing::instrument(
        name = "streams.app.start",
        level = "info",
        skip_all,
        fields(
            application_id = %application_id,
            processing_guarantee = ?processing_guarantee,
            member_id = tracing::field::Empty,
        ),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    #[allow(clippy::similar_names)]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into)] application_id: String,
        topology: BuiltTopology,
        #[builder(default = DEFAULT_STREAMS_POLL_INTERVAL)] poll_interval: Duration,
        #[builder(default = DEFAULT_STREAMS_COMMIT_INTERVAL)] commit_interval: Duration,
        #[builder(default = DEFAULT_STREAMS_REBALANCE_TIMEOUT)] rebalance_timeout: Duration,
        #[builder(default = DEFAULT_STREAMS_JOIN_RETRY_BACKOFF)] join_retry_backoff: Duration,
        #[builder(default)] store_backend: crate::store::backend::StoreBackend,
        #[builder(default)] processing_guarantee: crate::runtime::eos::ProcessingGuarantee,
        /// Deadline for each Kafka broker DNS lookup owned by this process.
        #[builder(default)]
        broker_dns_timeout: ClientDnsTimeout,
        /// Record-cache budget (JVM `statestore.cache.max.bytes`); `0` disables
        /// caching. Threaded onto each task graph at `instantiate`.
        #[builder(default = 10_485_760)]
        cache_max_bytes: i64,
        /// Capacity shared by the v1 and v2 interactive-query request queues.
        #[builder(default)]
        interactive_query_queue_capacity: StreamsInteractiveQueryQueueCapacity,
    ) -> Result<Self, StreamsClientError> {
        let (poll_interval, commit_interval, rebalance_timeout, join_retry_backoff) =
            validate_runtime_configuration(
                poll_interval,
                commit_interval,
                rebalance_timeout,
                join_retry_backoff,
            )?;
        let built = Arc::new(topology);

        // Broker I/O. Under EOS-v2 the producer is transactional: the SAME object
        // is both the task `RecordProducer` (for `send`) and the thread's
        // `TransactionalProducer` (for begin/send_offsets/commit). Under ALO `txn`
        // is `None`.
        let fetcher: Arc<dyn RecordFetcher>;
        let producer: Arc<dyn RecordProducer>;
        let store: Arc<dyn OffsetStore>;
        let txn: Option<Arc<dyn TransactionalProducer>>;
        match processing_guarantee {
            ProcessingGuarantee::AtLeastOnce => {
                let (f, p, s) = io_broker::build(
                    &bootstrap,
                    &application_id,
                    &application_id,
                    broker_dns_timeout,
                )
                .await?;
                fetcher = Arc::new(f);
                producer = p;
                store = s;
                txn = None;
            }
            ProcessingGuarantee::ExactlyOnceV2 => {
                let txn_id = crate::runtime::eos::transactional_id(&application_id, 0);
                let (f, txn_producer, s) = io_broker::build_eos(
                    &bootstrap,
                    &application_id,
                    &application_id,
                    &txn_id,
                    broker_dns_timeout,
                )
                .await?;
                fetcher = Arc::new(f);
                // Two trait-object views of the one transactional producer.
                producer = Arc::clone(&txn_producer) as Arc<dyn RecordProducer>;
                txn = Some(txn_producer as Arc<dyn TransactionalProducer>);
                store = s;
            }
        }

        // Join the streams group (membership owns the heartbeat loop).
        let mut membership = StreamsMembership::builder()
            .bootstrap(bootstrap.clone())
            .group_id(application_id.clone())
            .topology(Arc::clone(&built))
            .broker_dns_timeout(broker_dns_timeout)
            .rebalance_timeout(rebalance_timeout.duration())
            .join_retry_backoff(join_retry_backoff.duration())
            .build()
            .await?;
        let member_id = membership.member_id().to_string();
        tracing::Span::current().record("member_id", tracing::field::display(&member_id));

        // Supervisor: pump membership events into a StreamThread + poll/commit.
        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let topo_for_thread = Arc::clone(&built);
        let fetcher_for_thread = Arc::clone(&fetcher);
        let [iq_capacity, iq2_capacity] =
            interactive_query_queue_capacities(interactive_query_queue_capacity);
        let (iq_tx, mut iq_rx) = mpsc::channel::<IqRequest>(iq_capacity);
        let (iq2_tx, mut iq2_rx) = mpsc::channel::<Iq2Request>(iq2_capacity);
        let is_eos = processing_guarantee == ProcessingGuarantee::ExactlyOnceV2;
        let handle = tokio::spawn(async move {
            let mut thread = StreamThread::new(
                fetcher_for_thread,
                store_backend,
                application_id,
                cache_max_bytes,
            );
            let mut poll = tokio::time::interval(poll_interval.duration());
            let mut commit = tokio::time::interval(commit_interval.duration());
            let tracker = membership.tracker();
            loop {
                tokio::select! {
                    () = sd.cancelled() => {
                        // EOS close aborts any in-flight txn (meta unused); ALO commits.
                        let _ = thread.close_all(None).await;
                        let _ = membership.close().await;
                        break;
                    }
                    ev = membership.next_event() => match ev {
                        Ok(StreamsEvent::Assigned(a)) => {
                            if let Err(e) = thread
                                .apply_assignment(
                                    &a,
                                    &topo_for_thread,
                                    &producer,
                                    &store,
                                    processing_guarantee,
                                    txn.clone(),
                                )
                                .await
                            {
                                tracing::warn!(error = %e, "apply_assignment failed");
                            }
                        }
                        Ok(StreamsEvent::Fenced) => { let _ = thread.close_all(None).await; }
                        Ok(StreamsEvent::NotReady(_)) => {}
                        Err(e) => { tracing::warn!(error = %e, "membership event stream ended"); break; }
                    },
                    _ = poll.tick() => {
                        if let Err(e) = thread.poll_all(&*fetcher, &tracker).await {
                            tracing::warn!(error = %e, "poll_all failed");
                        }
                    }
                    _ = commit.tick() => {
                        // EOS commit folds offsets into the txn — needs the live
                        // streams group metadata; ALO ignores it.
                        let meta = if is_eos {
                            Some(membership.group_metadata().await)
                        } else {
                            None
                        };
                        if let Err(e) = thread.commit_all(meta.as_ref()).await {
                            tracing::warn!(error = %e, "commit_all failed");
                        }
                    }
                    Some(req) = iq_rx.recv() => {
                        thread.serve_iq(req).await;
                    }
                    Some(req) = iq2_rx.recv() => {
                        thread.serve_iq2(req).await;
                    }
                }
            }
        });

        Ok(Self {
            member_id,
            shutdown,
            handle,
            iq_tx,
            iq2_tx,
        })
    }
}

impl KafkaStreams {
    /// The client-generated streams member id.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// A read-only view of the local `KeyValue` state store `name` for
    /// interactive queries. Errors if the store is not assigned here, or it is
    /// a different store kind.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn key_value_store<K, V>(
        &self,
        name: impl Into<String>,
        key_serde: impl Serde<K> + 'static,
        value_serde: impl Serde<V> + 'static,
    ) -> Result<ReadOnlyKeyValueStore<K, V>, StreamsClientError> {
        let view = ReadOnlyKeyValueStore {
            tx: self.iq_tx.clone(),
            store: name.into(),
            key_serde: Box::new(key_serde),
            value_serde: Box::new(value_serde),
        };
        crate::runtime::iq_view::validate(&view.tx, &view.store, StoreKind::KeyValue).await?;
        Ok(view)
    }

    /// A read-only view of the local `Window` state store `name`.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn window_store<K, V>(
        &self,
        name: impl Into<String>,
        key_serde: impl Serde<K> + 'static,
        value_serde: impl Serde<V> + 'static,
    ) -> Result<crate::runtime::iq_view::ReadOnlyWindowStore<K, V>, StreamsClientError> {
        let view = crate::runtime::iq_view::ReadOnlyWindowStore {
            tx: self.iq_tx.clone(),
            store: name.into(),
            key_serde: Box::new(key_serde),
            value_serde: Box::new(value_serde),
        };
        crate::runtime::iq_view::validate(&view.tx, &view.store, StoreKind::Window).await?;
        Ok(view)
    }

    /// A read-only view of the local `Session` state store `name`.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn session_store<K, V>(
        &self,
        name: impl Into<String>,
        key_serde: impl Serde<K> + 'static,
        value_serde: impl Serde<V> + 'static,
    ) -> Result<crate::runtime::iq_view::ReadOnlySessionStore<K, V>, StreamsClientError> {
        let view = crate::runtime::iq_view::ReadOnlySessionStore {
            tx: self.iq_tx.clone(),
            store: name.into(),
            key_serde: Box::new(key_serde),
            value_serde: Box::new(value_serde),
        };
        crate::runtime::iq_view::validate(&view.tx, &view.store, StoreKind::Session).await?;
        Ok(view)
    }

    /// Run an `IQv2` query against locally assigned partitions and return one
    /// `QueryResult` per partition. Serde-free: the store supplies its own
    /// serdes.
    pub async fn query<Q: Query>(&self, req: StateQuery<Q>) -> StateQueryResult<Q::Result> {
        use crate::runtime::iqv2::dispatch::{Iq2Request, assemble};

        let kind = req.query.store_kind();
        let (reply, rx) = tokio::sync::oneshot::channel();
        let iq2 = Iq2Request {
            store: req.store,
            kind,
            query: req.query.lower(),
            partitions: req.partitions,
            bound: req.bound,
            require_active: req.require_active,
            reply,
        };
        if self.iq2_tx.send(iq2).await.is_err() {
            return StateQueryResult::new(std::collections::BTreeMap::new());
        }
        match rx.await {
            Ok(outcome) => assemble::<Q::Result>(outcome),
            Err(_) => StateQueryResult::new(std::collections::BTreeMap::new()),
        }
    }

    /// Stop processing, commit, and leave the group.
    #[tracing::instrument(
        name = "streams.app.close",
        level = "info",
        skip_all,
        fields(member_id = %self.member_id),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn close(self) -> Result<(), StreamsClientError> {
        self.shutdown.cancel();
        let _ = self.handle.await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        KafkaStreams, StreamsCommitInterval, StreamsInteractiveQueryQueueCapacity,
        StreamsPollInterval, interactive_query_queue_capacities, validate_runtime_configuration,
    };
    use crate::{membership::DEFAULT_STREAMS_JOIN_RETRY_BACKOFF, topology::Topology};

    #[test]
    fn interactive_query_queue_capacity_uses_default_and_valid_override() {
        let default = StreamsInteractiveQueryQueueCapacity::default();
        assert_eq!(default.capacity(), 64);

        let capacity =
            StreamsInteractiveQueryQueueCapacity::new(37).expect("positive queue capacity");
        assert_eq!(capacity.capacity(), 37);
    }

    #[test]
    fn interactive_query_queue_capacity_rejects_zero() {
        let error = StreamsInteractiveQueryQueueCapacity::new(0).expect_err("zero queue capacity");
        assert2::assert!(error.contains("streams interactive-query queue capacity"));
    }

    #[test]
    fn interactive_query_queue_capacity_matches_tokio_boundaries() {
        let maximum =
            StreamsInteractiveQueryQueueCapacity::new(tokio::sync::Semaphore::MAX_PERMITS)
                .expect("Tokio maximum queue capacity");
        assert_eq!(maximum.capacity(), tokio::sync::Semaphore::MAX_PERMITS);

        StreamsInteractiveQueryQueueCapacity::new(tokio::sync::Semaphore::MAX_PERMITS + 1)
            .expect_err("capacity above Tokio maximum");
    }

    #[test]
    fn interactive_query_queues_share_the_configured_capacity() {
        let capacity =
            StreamsInteractiveQueryQueueCapacity::new(37).expect("positive queue capacity");
        assert_eq!(interactive_query_queue_capacities(capacity), [37, 37]);
    }

    #[test]
    fn runtime_intervals_use_typed_defaults_and_valid_overrides() {
        let poll = StreamsPollInterval::default();
        let commit = StreamsCommitInterval::default();
        assert2::assert!(poll.milliseconds() == 200);
        assert2::assert!(commit.milliseconds() == 5_000);

        let poll = StreamsPollInterval::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
        let commit = StreamsCommitInterval::new(Duration::from_millis(41))
            .expect("positive whole milliseconds");
        assert2::assert!(poll.duration() == Duration::from_millis(37));
        assert2::assert!(commit.duration() == Duration::from_millis(41));
    }

    #[test]
    fn runtime_intervals_reject_zero_and_fractional_milliseconds() {
        assert2::assert!(StreamsPollInterval::new(Duration::ZERO).is_err());
        assert2::assert!(StreamsCommitInterval::new(Duration::ZERO).is_err());
        assert2::assert!(
            StreamsPollInterval::new(Duration::from_millis(1) + Duration::from_nanos(1)).is_err()
        );
        assert2::assert!(
            StreamsCommitInterval::new(Duration::from_millis(1) + Duration::from_nanos(1)).is_err()
        );
    }

    #[test]
    fn low_level_runtime_validation_names_the_invalid_field() {
        let poll_error = validate_runtime_configuration(
            Duration::ZERO,
            Duration::from_secs(5),
            Duration::from_secs(30),
            DEFAULT_STREAMS_JOIN_RETRY_BACKOFF,
        )
        .expect_err("zero poll interval");
        assert2::assert!(poll_error.to_string().contains("streams poll interval"));

        let commit_error = validate_runtime_configuration(
            Duration::from_millis(200),
            Duration::ZERO,
            Duration::from_secs(30),
            DEFAULT_STREAMS_JOIN_RETRY_BACKOFF,
        )
        .expect_err("zero commit interval");
        assert2::assert!(commit_error.to_string().contains("streams commit interval"));

        let rebalance_error = validate_runtime_configuration(
            Duration::from_millis(200),
            Duration::from_secs(5),
            Duration::from_millis(u64::try_from(i32::MAX).expect("i32 max fits u64") + 1),
            DEFAULT_STREAMS_JOIN_RETRY_BACKOFF,
        )
        .expect_err("rebalance timeout outside Kafka wire range");
        assert2::assert!(
            rebalance_error
                .to_string()
                .contains("streams rebalance timeout")
        );

        let join_retry_error = validate_runtime_configuration(
            Duration::from_millis(200),
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::ZERO,
        )
        .expect_err("zero join retry backoff");
        assert2::assert!(
            join_retry_error
                .to_string()
                .contains("streams join retry backoff")
        );
    }

    #[tokio::test]
    async fn invalid_join_retry_backoff_fails_before_broker_lookup() {
        let mut topology = Topology::new();
        let source = topology.add_source::<String, String>("source", ["input"]);
        topology.add_sink("sink", "output", [&source]);
        let topology = topology.build("join-retry-validation").expect("topology");

        let error = KafkaStreams::builder()
            .bootstrap("invalid.invalid:9092")
            .application_id("join-retry-validation")
            .topology(topology)
            .join_retry_backoff(Duration::ZERO)
            .build()
            .await
            .err()
            .expect("invalid configuration");

        assert2::assert!(error.to_string().contains("streams join retry backoff"));
    }
}
