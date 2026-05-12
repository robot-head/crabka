//! `Producer` — public type. Builder lives in `builder.rs`. Sender task
//! lives in `sender.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;

use crate::accumulator::Accumulator;
use crate::compression::Compression;
use crate::error::ProducerError;
use crate::partitioner::UniformStickyPartitioner;

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
#[allow(dead_code)] // Tasks 15-16 wire these up
pub(crate) const STATE_ACTIVE: u8 = 0;
#[allow(dead_code)] // Tasks 15-16 wire these up
pub(crate) const STATE_FENCED: u8 = 1;
#[allow(dead_code)] // Tasks 15-16 wire these up
pub(crate) const STATE_CLOSED: u8 = 2;

#[allow(dead_code)] // Tasks 15-16 wire these up
#[derive(Debug, Clone)]
pub(crate) struct TopicMetadata {
    pub num_partitions: i32,
    /// Topic UUID. Needed for Produce v13+, which encodes only the
    /// `topic_id` on the wire. Zero (`Uuid::ZERO`) is a valid sentinel
    /// meaning "not yet known" — the broker falls back to the `name`
    /// field for older wire versions.
    pub topic_id: crabka_protocol::primitives::uuid::Uuid,
}

// Tasks 15-16 wire these up
#[allow(dead_code)]
#[allow(clippy::struct_field_names)] // producer_id / producer_epoch intentionally match struct name
#[allow(clippy::type_complexity)] // accumulators map is inherently complex; type alias deferred to Task 15
pub struct Producer {
    pub(crate) client: Client,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) acks: Acks,
    pub(crate) compression: Compression,
    pub(crate) batch_size: usize,
    pub(crate) linger: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) retries: i32,
    pub(crate) retry_backoff: Duration,
    pub(crate) max_in_flight: usize,
    pub(crate) metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    pub(crate) accumulators: Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>,
    pub(crate) next_seq: Arc<DashMap<(String, i32), i32>>,
    pub(crate) partitioner: Arc<UniformStickyPartitioner>,
    pub(crate) state: Arc<AtomicU8>,
    pub(crate) wake_tx: tokio::sync::mpsc::Sender<()>,
    pub(crate) flush_notify: Arc<Notify>,
    pub(crate) sender_shutdown: CancellationToken,
    pub(crate) sender_handle: Option<JoinHandle<()>>,
}

#[allow(dead_code)] // Tasks 15-16 wire these up
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
        Err(ProducerError::Closed)
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
