//! openraft `RaftStateMachine` wrapping a `MetadataImage`. Apply is
//! synchronous + infallible; we swap the `Arc<MetadataImage>` after
//! mutating a fresh clone so readers always observe a consistent view.
//!
//! Snapshot generation (`build_snapshot`/`get_current_snapshot`),
//! recovery (seeding the image from the newest on-disk checkpoint at
//! construction), and install (`begin_receiving_snapshot`/
//! `install_snapshot`, rebuilding the image from a snapshot streamed over
//! openraft's `InstallSnapshot` RPC for follower catch-up) are all
//! implemented here.

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

use crate::error::RaftError;
use crate::snapshot::{SnapshotId, SnapshotReader, SnapshotWriter};
use crate::types::{AppData, AppDataResponse, Node, NodeId, TypeConfig};

pub(crate) struct CrabkaStateMachine {
    cluster_id: Uuid,
    snapshot_dir: std::path::PathBuf,
    image: watch::Sender<Arc<MetadataImage>>,
    last_applied: Mutex<Option<LogId<NodeId>>>,
    last_membership: Mutex<StoredMembership<NodeId, Node>>,
}

impl CrabkaStateMachine {
    pub(crate) fn new(cluster_id: Uuid, snapshot_dir: std::path::PathBuf) -> Self {
        // Recover from the newest on-disk checkpoint if one exists: rebuild
        // the image from its records and adopt its applied position +
        // membership. openraft reapplies only the log entries *after*
        // `last_applied`, and `purge` deletes the log behind the snapshot,
        // so seeding here is the only way a restarted node recovers state
        // committed before the last checkpoint. A missing/empty snapshot
        // dir yields the fresh empty image.
        let (image_value, last_applied, last_membership) = Self::recover(cluster_id, &snapshot_dir);
        let (image, _rx) = watch::channel(Arc::new(image_value));
        Self {
            cluster_id,
            snapshot_dir,
            image,
            last_applied: Mutex::new(last_applied),
            last_membership: Mutex::new(last_membership),
        }
    }

    fn recover(
        cluster_id: Uuid,
        snapshot_dir: &std::path::Path,
    ) -> (
        MetadataImage,
        Option<LogId<NodeId>>,
        StoredMembership<NodeId, Node>,
    ) {
        match crate::snapshot::load_latest(snapshot_dir) {
            Ok(Some((_, bytes, meta))) => {
                let records = SnapshotReader::read_records(&bytes)
                    .expect("checkpoint records must decode on recovery");
                let image = MetadataImage::from_records(cluster_id, &records);
                (image, meta.last_log_id, meta.last_membership)
            }
            Ok(None) => (
                MetadataImage::new(cluster_id),
                None,
                StoredMembership::default(),
            ),
            Err(e) => panic!("failed to load metadata checkpoint on recovery: {e}"),
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

/// Map a snapshot-build/persist `RaftError` into an openraft write-side
/// snapshot `StorageError`. openraft treats this as fatal storage, which
/// is correct: if we can't write a checkpoint to disk the node can't
/// safely compact its log.
fn io_storage_err(e: &RaftError) -> StorageError<NodeId> {
    StorageIOError::write_snapshot(None, AnyError::new(e)).into()
}

/// Derive the on-disk [`SnapshotId`] from an installed snapshot's meta:
/// `end_offset` is the exclusive offset one past the last contained log
/// index, and `epoch` is the leader term at that index. `None` when the
/// meta carries no `last_log_id` (an empty snapshot needs no checkpoint).
fn snapshot_id_from_meta(meta: &SnapshotMeta<NodeId, Node>) -> Option<SnapshotId> {
    meta.last_log_id.map(|l| SnapshotId {
        end_offset: i64::try_from(l.index).unwrap_or(i64::MAX).saturating_add(1),
        epoch: i32::try_from(l.leader_id.term).unwrap_or(i32::MAX),
    })
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
        Ok(Box::new(io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let records = SnapshotReader::read_records(&bytes).map_err(|e| io_storage_err(&e))?;
        let image = MetadataImage::from_records(self.cluster_id, &records);
        let _ = self.image.send_replace(Arc::new(image));
        *self.last_applied.lock().await = meta.last_log_id;
        *self.last_membership.lock().await = meta.last_membership.clone();
        if let Some(id) = snapshot_id_from_meta(meta) {
            crate::snapshot::persist(&self.snapshot_dir, id, &bytes, meta)
                .map_err(|e| io_storage_err(&e))?;
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let loaded = crate::snapshot::load_latest(&self.snapshot_dir).map_err(|e| io_storage_err(&e))?;
        Ok(loaded.map(|(_, bytes, meta)| Snapshot {
            meta,
            snapshot: Box::new(io::Cursor::new(bytes)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<CrabkaStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let last_applied = *self.last_applied.lock().await;
        let membership = self.last_membership.lock().await.clone();
        let image = self.current_image();

        let end_offset =
            last_applied.map_or(0, |l| i64::try_from(l.index).unwrap_or(i64::MAX).saturating_add(1));
        let epoch = last_applied.map_or(0, |l| i32::try_from(l.leader_id.term).unwrap_or(i32::MAX));
        let id = SnapshotId { end_offset, epoch };

        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(i64::MAX);

        let bytes = SnapshotWriter::serialize(&image, now_ms).map_err(|e| io_storage_err(&e))?;
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: format!("{end_offset}-{epoch}"),
        };
        crate::snapshot::persist(&self.snapshot_dir, id, &bytes, &meta)
            .map_err(|e| io_storage_err(&e))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(io::Cursor::new(bytes.to_vec())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, TopicRecord};

    #[tokio::test]
    async fn apply_publishes_image_to_watcher() {
        let sm = Arc::new(CrabkaStateMachine::new(Uuid::nil(), std::env::temp_dir()));
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

    #[tokio::test]
    async fn build_snapshot_writes_checkpoint_and_meta() {
        let dir = tempfile::TempDir::new().unwrap();
        let sm = Arc::new(CrabkaStateMachine::new(
            Uuid::nil(),
            dir.path().to_path_buf(),
        ));
        let log_id = LogId {
            leader_id: openraft::LeaderId::new(1, 1),
            index: 5,
        };
        sm.apply_entry(
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

        let snap = sm.clone().build_snapshot().await.unwrap();
        assert_eq!(snap.meta.last_log_id, Some(log_id));

        // end_offset = index + 1 = 6, epoch = leader term = 1.
        let checkpoint = dir.path().join("00000000000000000006-0000000001.checkpoint");
        assert!(checkpoint.exists(), "checkpoint file should exist");
        let has_meta = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with(".checkpoint.meta"))
            });
        assert!(has_meta, "a .checkpoint.meta sidecar should exist");
    }

    #[tokio::test]
    async fn get_current_snapshot_loads_latest() {
        let dir = tempfile::TempDir::new().unwrap();
        let sm = Arc::new(CrabkaStateMachine::new(
            Uuid::nil(),
            dir.path().to_path_buf(),
        ));
        assert!(sm.clone().get_current_snapshot().await.unwrap().is_none());

        let log_id = LogId {
            leader_id: openraft::LeaderId::new(1, 1),
            index: 3,
        };
        sm.apply_entry(
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
        sm.clone().build_snapshot().await.unwrap();

        let loaded = sm.clone().get_current_snapshot().await.unwrap();
        let loaded = loaded.expect("snapshot should be present");
        assert_eq!(loaded.meta.last_log_id, Some(log_id));
    }

    #[tokio::test]
    async fn install_snapshot_rebuilds_image() {
        use crabka_metadata::{MetadataRecord, TopicRecord};
        let src_dir = tempfile::TempDir::new().unwrap();
        let src = Arc::new(CrabkaStateMachine::new(
            Uuid::nil(),
            src_dir.path().to_path_buf(),
        ));
        let log_id = LogId {
            leader_id: openraft::LeaderId::new(1, 1),
            index: 4,
        };
        src.apply_entry(
            log_id,
            &AppData {
                records: vec![MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: Uuid::from_u128(1),
                    partitions: 1,
                    replication_factor: 1,
                })],
            },
        )
        .await;
        let snap = src.clone().build_snapshot().await.unwrap();

        let dst_dir = tempfile::TempDir::new().unwrap();
        let dst = Arc::new(CrabkaStateMachine::new(
            Uuid::nil(),
            dst_dir.path().to_path_buf(),
        ));
        let mut dst_mut = dst.clone();
        let buf = dst_mut.begin_receiving_snapshot().await.unwrap();
        let _ = buf;
        let data = Box::new(io::Cursor::new(snap.snapshot.into_inner()));
        dst_mut.install_snapshot(&snap.meta, data).await.unwrap();

        assert!(dst.current_image().topic("t").is_some());
        let (applied, _) = dst_mut.applied_state().await.unwrap();
        assert_eq!(applied, Some(log_id));
    }
}
