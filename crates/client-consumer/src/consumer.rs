//! `Consumer` — public lifecycle handle. Built via [`crate::ConsumerBuilder`].
//! Subscribe-only — no `assign()`. Use `crabka-client-core` directly for
//! manual partition consumption.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;

use crate::error::ConsumerError;
use crate::heartbeat::RebalanceNotice;

/// Subscribe-style consumer handle. Construct via [`crate::ConsumerBuilder`].
#[allow(dead_code)] // `session_timeout` / `heartbeat_interval` are kept for
                    // future re-join / re-handshake support (slice 5 MVP
                    // only spawns one heartbeat task at build time).
pub struct Consumer {
    pub(crate) client: Client,
    pub(crate) group_id: String,
    pub(crate) member_id: String,
    pub(crate) generation_id: i32,
    pub(crate) subscribed_topics: Vec<String>,
    /// Current assigned partitions: `(topic, partition_index)`.
    pub(crate) assigned: Arc<Mutex<Vec<(String, i32)>>>,
    /// Next offset to fetch per partition.
    pub(crate) next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub(crate) session_timeout: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) rebalance_rx: Mutex<mpsc::Receiver<RebalanceNotice>>,
    pub(crate) heartbeat_shutdown: CancellationToken,
    pub(crate) heartbeat_handle: Option<JoinHandle<()>>,
}

/// One record returned by `Consumer::poll`.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

impl Consumer {
    /// The consumer's group id.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The member id assigned by the coordinator at join time.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The generation id captured at the most recent successful join.
    #[must_use]
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Topics this consumer subscribed to at build time.
    #[must_use]
    pub fn subscribed_topics(&self) -> &[String] {
        &self.subscribed_topics
    }

    /// Snapshot of currently assigned `(topic, partition)` pairs.
    pub async fn assignment(&self) -> Vec<(String, i32)> {
        self.assigned.lock().await.clone()
    }

    /// Stop the heartbeat task. Returns immediately if already shut down.
    pub async fn close(mut self) -> Result<(), ConsumerError> {
        self.heartbeat_shutdown.cancel();
        if let Some(h) = self.heartbeat_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}
