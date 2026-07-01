//! `KafkaStreams` — the managed runtime handle. Owns membership + a `StreamThread`.
//!
//! `start()` builds the broker I/O, joins the streams group (membership owns the
//! heartbeat), and spawns a supervisor that pumps membership events into a
//! `StreamThread` while polling/committing on intervals. `close()` stops the
//! supervisor (flush+commit+leave).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::StreamsClientError;
use crate::membership::{StreamsEvent, StreamsMembership};
use crate::processor::serde::Serde;
use crate::runtime::eos::{ProcessingGuarantee, TransactionalProducer};
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};
use crate::runtime::io_broker;
use crate::runtime::iq::IqRequest;
use crate::runtime::iq_view::ReadOnlyKeyValueStore;
use crate::runtime::iqv2::dispatch::Iq2Request;
use crate::runtime::iqv2::{Query, StateQuery, StateQueryResult};
use crate::runtime::thread::StreamThread;
use crate::store::iq::StoreKind;
use crate::topology::BuiltTopology;

/// Lifecycle state of a [`KafkaStreams`] runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaStreamsState {
    Created,
    Running,
    Closed,
}

/// A managed Kafka Streams runtime: joins a streams group, runs assigned tasks
/// (fetch → process → produce → commit, at-least-once), and reacts to rebalances.
pub struct KafkaStreams {
    member_id: String,
    shutdown: CancellationToken,
    handle: Option<JoinHandle<()>>,
    state: KafkaStreamsState,
    /// Channel to the supervisor for interactive queries. Read by the
    /// `KafkaStreams` IQ accessors.
    iq_tx: mpsc::Sender<IqRequest>,
    /// Channel to the supervisor for `IQv2` queries (separate from the v1 `iq_tx`).
    iq2_tx: mpsc::Sender<Iq2Request>,
}

#[bon::bon]
impl KafkaStreams {
    #[builder(start_fn = builder, finish_fn = build)]
    #[allow(clippy::too_many_lines)]
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
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into)] application_id: String,
        topology: BuiltTopology,
        #[builder(default = Duration::from_millis(200))] poll_interval: Duration,
        #[builder(default = Duration::from_secs(5))] commit_interval: Duration,
        #[builder(default)] store_backend: crate::store::backend::StoreBackend,
        #[builder(default)] processing_guarantee: crate::runtime::eos::ProcessingGuarantee,
        /// Record-cache budget (JVM `statestore.cache.max.bytes`); `0` disables
        /// caching. Threaded onto each task graph at `instantiate`.
        #[builder(default = 10_485_760)]
        cache_max_bytes: i64,
    ) -> Result<Self, StreamsClientError> {
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
                let (f, p, s) =
                    io_broker::build(&bootstrap, &application_id, &application_id).await?;
                fetcher = Arc::new(f);
                producer = p;
                store = s;
                txn = None;
            }
            ProcessingGuarantee::ExactlyOnceV2 => {
                let txn_id = crate::runtime::eos::transactional_id(&application_id, 0);
                let (f, txn_producer, s) =
                    io_broker::build_eos(&bootstrap, &application_id, &application_id, &txn_id)
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
            .build()
            .await?;
        let member_id = membership.member_id().to_string();
        tracing::Span::current().record("member_id", tracing::field::display(&member_id));

        // Supervisor: pump membership events into a StreamThread + poll/commit.
        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let topo_for_thread = Arc::clone(&built);
        let fetcher_for_thread = Arc::clone(&fetcher);
        let (iq_tx, mut iq_rx) = mpsc::channel::<IqRequest>(64);
        let (iq2_tx, mut iq2_rx) = mpsc::channel::<Iq2Request>(64);
        let is_eos = processing_guarantee == ProcessingGuarantee::ExactlyOnceV2;
        let handle = tokio::spawn(async move {
            let mut thread = StreamThread::new(
                fetcher_for_thread,
                store_backend,
                application_id,
                cache_max_bytes,
            );
            let mut poll = tokio::time::interval(poll_interval);
            let mut commit = tokio::time::interval(commit_interval);
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
            handle: Some(handle),
            state: KafkaStreamsState::Running,
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

    /// Current lifecycle state.
    #[must_use]
    pub fn state(&self) -> KafkaStreamsState {
        self.state
    }

    /// A read-only view of the local `KeyValue` state store `name` for
    /// interactive queries. Errors if the instance is not running, the store is
    /// not assigned here, or it is a different store kind.
    pub async fn key_value_store<K, V>(
        &self,
        name: impl Into<String>,
        key_serde: impl Serde<K> + 'static,
        value_serde: impl Serde<V> + 'static,
    ) -> Result<ReadOnlyKeyValueStore<K, V>, StreamsClientError> {
        if self.state != KafkaStreamsState::Running {
            return Err(StreamsClientError::InteractiveQuery(
                crate::runtime::iq::IqError::NotRunning,
            ));
        }
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
    pub async fn window_store<K, V>(
        &self,
        name: impl Into<String>,
        key_serde: impl Serde<K> + 'static,
        value_serde: impl Serde<V> + 'static,
    ) -> Result<crate::runtime::iq_view::ReadOnlyWindowStore<K, V>, StreamsClientError> {
        if self.state != KafkaStreamsState::Running {
            return Err(StreamsClientError::InteractiveQuery(
                crate::runtime::iq::IqError::NotRunning,
            ));
        }
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
    pub async fn session_store<K, V>(
        &self,
        name: impl Into<String>,
        key_serde: impl Serde<K> + 'static,
        value_serde: impl Serde<V> + 'static,
    ) -> Result<crate::runtime::iq_view::ReadOnlySessionStore<K, V>, StreamsClientError> {
        if self.state != KafkaStreamsState::Running {
            return Err(StreamsClientError::InteractiveQuery(
                crate::runtime::iq::IqError::NotRunning,
            ));
        }
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
    /// serdes. If the instance is not running, the result has no partitions.
    pub async fn query<Q: Query>(&self, req: StateQuery<Q>) -> StateQueryResult<Q::Result> {
        use crate::runtime::iqv2::dispatch::{Iq2Request, assemble};

        if self.state != KafkaStreamsState::Running {
            return StateQueryResult::new(std::collections::BTreeMap::new());
        }
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
    pub async fn close(&mut self) -> Result<(), StreamsClientError> {
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
        self.state = KafkaStreamsState::Closed;
        Ok(())
    }
}
