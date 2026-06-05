//! `KafkaStreams` — the managed runtime handle. Owns membership + a `StreamThread`.
//!
//! `start()` builds the broker I/O, joins the streams group (membership owns the
//! heartbeat), and spawns a supervisor that pumps membership events into a
//! `StreamThread` while polling/committing on intervals. `close()` stops the
//! supervisor (flush+commit+leave).

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::StreamsClientError;
use crate::membership::{StreamsEvent, StreamsMembership};
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};
use crate::runtime::io_broker;
use crate::runtime::thread::StreamThread;
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
}

#[bon::bon]
impl KafkaStreams {
    #[builder(start_fn = builder, finish_fn = build)]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into)] application_id: String,
        topology: BuiltTopology,
        #[builder(default = Duration::from_millis(200))] poll_interval: Duration,
        #[builder(default = Duration::from_secs(5))] commit_interval: Duration,
        #[builder(default)] store_backend: crate::store::backend::StoreBackend,
    ) -> Result<Self, StreamsClientError> {
        let built = Arc::new(topology);

        // Broker I/O (one shared producer).
        let (fetcher, producer, store) =
            io_broker::build(&bootstrap, &application_id, &application_id).await?;
        let fetcher: Arc<dyn RecordFetcher> = Arc::new(fetcher);
        let producer: Arc<dyn RecordProducer> = producer;
        let store: Arc<dyn OffsetStore> = store;

        // Join the streams group (membership owns the heartbeat loop).
        let mut membership = StreamsMembership::builder()
            .bootstrap(bootstrap.clone())
            .group_id(application_id.clone())
            .topology(Arc::clone(&built))
            .build()
            .await?;
        let member_id = membership.member_id().to_string();

        // Supervisor: pump membership events into a StreamThread + poll/commit.
        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let topo_for_thread = Arc::clone(&built);
        let fetcher_for_thread = Arc::clone(&fetcher);
        let handle = tokio::spawn(async move {
            let mut thread = StreamThread::new(fetcher_for_thread, store_backend, application_id);
            let mut poll = tokio::time::interval(poll_interval);
            let mut commit = tokio::time::interval(commit_interval);
            loop {
                tokio::select! {
                    () = sd.cancelled() => {
                        let _ = thread.close_all().await;
                        let _ = membership.close().await;
                        break;
                    }
                    ev = membership.next_event() => match ev {
                        Ok(StreamsEvent::Assigned(a)) => {
                            if let Err(e) = thread.apply_assignment(&a, &topo_for_thread, &producer, &store).await {
                                tracing::warn!(error = %e, "apply_assignment failed");
                            }
                        }
                        Ok(StreamsEvent::Fenced) => { let _ = thread.close_all().await; }
                        Ok(StreamsEvent::NotReady(_)) => {}
                        Err(e) => { tracing::warn!(error = %e, "membership event stream ended"); break; }
                    },
                    _ = poll.tick() => {
                        if let Err(e) = thread.poll_all(&*fetcher).await {
                            tracing::warn!(error = %e, "poll_all failed");
                        }
                    }
                    _ = commit.tick() => {
                        if let Err(e) = thread.commit_all().await {
                            tracing::warn!(error = %e, "commit_all failed");
                        }
                    }
                }
            }
        });

        Ok(Self {
            member_id,
            shutdown,
            handle: Some(handle),
            state: KafkaStreamsState::Running,
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

    /// Stop processing, commit, and leave the group.
    pub async fn close(&mut self) -> Result<(), StreamsClientError> {
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
        self.state = KafkaStreamsState::Closed;
        Ok(())
    }
}
