//! openraft `RaftStateMachine` wrapping a `MetadataImage`. Apply is
//! synchronous + infallible; we swap the `Arc<MetadataImage>` after
//! mutating a fresh clone so readers always observe a consistent view.
//!
//! Snapshots are not implemented; the snapshot methods return a typed
//! "Unsupported" `StorageError` so openraft falls back to plain
//! append-entries replication for lagging followers. The metadata log
//! stays small, so missing snapshots is fine.
//!
//! The inner type is consumed only by `state_machine.rs` and
//! `Controller`, so the lib-crate root sees it as "dead". The
//! module-scoped allow keeps the surface narrow.

#![allow(dead_code)]

use std::io;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    AnyError, Entry, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};
use tokio::sync::{Mutex, watch};
use uuid::Uuid;

use crabka_metadata::MetadataImage;

use crate::types::{AppData, AppDataResponse, Node, NodeId, TypeConfig};

pub(crate) struct CrabkaStateMachine {
    image: watch::Sender<Arc<MetadataImage>>,
    last_applied: Mutex<Option<LogId<NodeId>>>,
    last_membership: Mutex<StoredMembership<NodeId, Node>>,
}

impl CrabkaStateMachine {
    pub(crate) fn new(cluster_id: Uuid) -> Self {
        let initial = Arc::new(MetadataImage::new(cluster_id));
        let (image, _rx) = watch::channel(initial);
        Self {
            image,
            last_applied: Mutex::new(None),
            last_membership: Mutex::new(StoredMembership::default()),
        }
    }

    pub(crate) fn current_image(&self) -> Arc<MetadataImage> {
        self.image.borrow().clone()
    }

    pub(crate) fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image.subscribe()
    }

    /// Apply one committed entry's payload to the image. Infallible —
    /// pre-validation lives in `Controller::submit_change`; if we ever
    /// reach a bad record here, the log is corrupt and the right move
    /// is to crash, not to surface a `StorageError` (which openraft
    /// treats as a fatal storage fault anyway).
    pub(crate) async fn apply_entry(
        &self,
        log_id: LogId<NodeId>,
        data: &AppData,
    ) -> AppDataResponse {
        let current = self.image.borrow().clone();
        let mut next: MetadataImage = (*current).clone();
        let mut rejected: Vec<String> = Vec::new();
        for rec in &data.records {
            // `submit_change` already pre-validates each record against
            // the local image, but with concurrent submitters in a
            // 3-node cluster a follower can pass pre-validation and
            // still race a leader's earlier apply for the same record.
            // The deterministic per-leader apply order is the only
            // authoritative checkpoint, so we re-validate here. Failures
            // are accumulated into `rejected` rather than fatally
            // aborted: openraft requires `apply` to be infallible.
            if let Err(e) = next.validate(rec) {
                rejected.push(e.to_string());
                continue;
            }
            next.apply(rec);
        }
        // Use `send_replace` so the new image is stored even when no
        // `watch::Receiver` has been subscribed yet — `current_image()`
        // reads via `borrow()` on the sender and must always see the
        // latest applied state.
        let _ = self.image.send_replace(Arc::new(next));
        *self.last_applied.lock().await = Some(log_id);
        AppDataResponse {
            applied_index: log_id.index,
            rejected,
        }
    }
}

/// Helper: build a `StorageError` describing a snapshot operation that
/// isn't implemented. We surface `io::ErrorKind::Unsupported` so callers
/// (and logs) can distinguish "deferred feature" from a real I/O fault.
fn snapshot_unsupported(verb: &'static str) -> StorageError<NodeId> {
    let io_err = io::Error::new(
        io::ErrorKind::Unsupported,
        format!("snapshots are not implemented ({verb})"),
    );
    StorageIOError::read_snapshot(None, AnyError::new(&io_err)).into()
}

impl RaftStateMachine<TypeConfig> for Arc<CrabkaStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let last = *self.last_applied.lock().await;
        let membership = self.last_membership.lock().await.clone();
        Ok((last, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppDataResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut out = Vec::new();
        for entry in entries {
            let resp = match &entry.payload {
                EntryPayload::Blank => {
                    *self.last_applied.lock().await = Some(entry.log_id);
                    AppDataResponse {
                        applied_index: entry.log_id.index,
                        rejected: Vec::new(),
                    }
                }
                EntryPayload::Normal(data) => self.apply_entry(entry.log_id, data).await,
                EntryPayload::Membership(m) => {
                    *self.last_applied.lock().await = Some(entry.log_id);
                    *self.last_membership.lock().await =
                        StoredMembership::new(Some(entry.log_id), m.clone());
                    AppDataResponse {
                        applied_index: entry.log_id.index,
                        rejected: Vec::new(),
                    }
                }
            };
            out.push(resp);
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Err(snapshot_unsupported("begin_receiving_snapshot"))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<NodeId, Node>,
        _snapshot: Box<io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        Err(snapshot_unsupported("install_snapshot"))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // No snapshot exists yet; openraft treats `Ok(None)` as "no snapshot
        // available" and falls back to append-entries replication.
        Ok(None)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<CrabkaStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        Err(snapshot_unsupported("build_snapshot"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, TopicRecord};

    #[tokio::test]
    async fn apply_publishes_image_to_watcher() {
        let sm = Arc::new(CrabkaStateMachine::new(Uuid::nil()));
        let mut rx = sm.watch_image();
        let log_id = LogId {
            leader_id: openraft::LeaderId::new(1, 1),
            index: 1,
        };
        let resp = sm
            .apply_entry(
                log_id,
                &AppData {
                    records: vec![MetadataRecord::V1Topic(TopicRecord {
                        name: "t".into(),
                        topic_id: Uuid::new_v4(),
                        partitions: 1,
                        replication_factor: 1,
                    })],
                },
            )
            .await;
        assert_eq!(resp.applied_index, 1);
        rx.changed().await.unwrap();
        assert!(rx.borrow().topic("t").is_some());
    }
}
