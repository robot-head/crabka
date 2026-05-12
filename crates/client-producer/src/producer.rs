//! `Producer` — public type. Builder lives in `builder.rs`. Sender task
//! lives in `sender.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;

use crate::accumulator::{Accumulator, AppendResult};
use crate::compression::Compression;
use crate::error::ProducerError;
use crate::partitioner::UniformStickyPartitioner;
use crate::record::{ProducerRecord, RecordMetadata};

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
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    // The following config knobs are also copied into `SenderConfig` at
    // construction time. They live on `Producer` for diagnostic
    // introspection and to support future reconnect / re-init flows
    // (Task 17+). Suppressing the dead-code warning is honest about
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
    pub(crate) accumulators: Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>,
    #[allow(dead_code)]
    pub(crate) next_seq: Arc<DashMap<(String, i32), i32>>,
    pub(crate) partitioner: Arc<UniformStickyPartitioner>,
    pub(crate) state: Arc<AtomicU8>,
    pub(crate) wake_tx: tokio::sync::mpsc::Sender<()>,
    pub(crate) flush_notify: Arc<Notify>,
    pub(crate) sender_shutdown: CancellationToken,
    pub(crate) sender_handle: Option<JoinHandle<()>>,
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
            Some(p) => p,
            None => {
                self.partition_for(&record.topic, record.key.as_deref())
                    .await
            }
        };

        let key = (record.topic.clone(), partition);
        let acc = self
            .accumulators
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(self.batch_size))))
            .value()
            .clone();

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
    async fn partition_for(&self, topic: &str, key: Option<&[u8]>) -> i32 {
        let num_partitions = self.partitions_for(topic).await;
        self.partitioner.pick(topic, key, num_partitions)
    }

    /// Return the partition count for `topic`, fetching metadata on cache
    /// miss. Falls back to `1` if the broker reports an error or the
    /// topic is absent — Task 17 / production code can revisit retry
    /// policy here.
    async fn partitions_for(&self, topic: &str) -> i32 {
        {
            let m = self.metadata_cache.lock().await;
            if let Some(meta) = m.get(topic) {
                return meta.num_partitions;
            }
        }
        // Cache miss: fetch.
        let req = crabka_protocol::owned::metadata_request::MetadataRequest {
            topics: Some(vec![
                crabka_protocol::owned::metadata_request::MetadataRequestTopic {
                    name: Some(topic.to_string()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        match self.client.send(req).await {
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
                        (count, t.topic_id)
                    }
                    _ => (1, crabka_protocol::primitives::uuid::Uuid::ZERO),
                };
                let mut m = self.metadata_cache.lock().await;
                m.insert(
                    topic.to_string(),
                    TopicMetadata {
                        num_partitions: count,
                        topic_id,
                    },
                );
                count
            }
            Err(_) => 1,
        }
    }

    pub async fn close(mut self) -> Result<(), ProducerError> {
        self.flush().await?;
        self.state.store(STATE_CLOSED, Ordering::Release);
        self.sender_shutdown.cancel();
        if let Some(h) = self.sender_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), ProducerError> {
        self.is_active()?;
        let _ = self.wake_tx.send(()).await;
        for _ in 0..1000 {
            if self.all_empty().await {
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
