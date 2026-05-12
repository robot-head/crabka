//! openraft `RaftLogStorage` backed by `crabka-log`. The log lives at
//! `<log_dir>/@metadata-0/`. Each openraft entry is serialized with
//! bincode and appended as a single Kafka `RecordBatch` whose value
//! payload IS the serialized entry. Future KRaft-wire-compat work will
//! revisit the record layout; today the wrapping is internal only.
//!
//! Slice-7 note: only the smoke tests and (in later tasks) `Controller`
//! reach into this module, so the `dead_code` lint fires for the parts
//! the trait impl alone doesn't consume from the lib crate root. The
//! allow at module scope keeps the surface narrow while letting tests
//! drive the inner helpers.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use bincode::config::standard;
use bytes::Bytes;
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{AnyError, Entry, LogId, StorageError, StorageIOError, Vote};
use tokio::sync::Mutex;

use crabka_log::{Log, LogConfig};
use crabka_protocol::records::{Record, RecordBatch};

use crate::error::RaftError;
use crate::types::{NodeId, TypeConfig};

/// In-memory cache keyed by log index — openraft expects O(1) random
/// reads at the log tip. We populate from disk on startup and keep
/// entries cached until commit (and slightly past).
#[derive(Debug, Default)]
struct EntryCache {
    /// Sorted by index. We never compact in slice 7 (snapshots deferred).
    entries: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: u64,
}

pub(crate) struct RaftLogStore {
    log: Arc<Mutex<Log>>,
    cache: Arc<Mutex<EntryCache>>,
    /// Last `vote` openraft asked us to persist. Held in memory + flushed
    /// to a small adjacent file so it survives restart.
    vote_path: PathBuf,
}

impl RaftLogStore {
    #[allow(clippy::unused_async)]
    pub(crate) async fn open(meta_dir: PathBuf) -> Result<Self, RaftError> {
        std::fs::create_dir_all(&meta_dir).map_err(crabka_log::LogError::Io)?;
        let log_dir = meta_dir.join("@metadata-0");
        std::fs::create_dir_all(&log_dir).map_err(crabka_log::LogError::Io)?;
        let log = Log::open(&log_dir, LogConfig::default())?;
        let vote_path = meta_dir.join("vote.bin");

        // Replay existing log into the cache.
        let mut cache = EntryCache::default();
        let mut offset = log.log_start_offset();
        let log_end = log.log_end_offset();
        while offset < log_end {
            let out = log.read(offset, 1 << 20)?;
            if out.batches.is_empty() {
                break;
            }
            for batch in &out.batches {
                for rec in &batch.records {
                    let Some(value) = rec.value.as_ref() else {
                        continue;
                    };
                    let (entry, _): (Entry<TypeConfig>, _) =
                        bincode::serde::decode_from_slice(value, standard())?;
                    cache.entries.insert(entry.log_id.index, entry);
                }
                offset = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
            }
        }

        Ok(Self {
            log: Arc::new(Mutex::new(log)),
            cache: Arc::new(Mutex::new(cache)),
            vote_path,
        })
    }

    pub(crate) async fn last_log_id(&self) -> Option<LogId<NodeId>> {
        self.cache
            .lock()
            .await
            .entries
            .values()
            .next_back()
            .map(|e| e.log_id)
    }

    pub(crate) async fn read_range<R: RangeBounds<u64>>(&self, range: R) -> Vec<Entry<TypeConfig>> {
        self.cache
            .lock()
            .await
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect()
    }

    pub(crate) async fn append(&self, entries: Vec<Entry<TypeConfig>>) -> Result<(), RaftError> {
        let mut cache = self.cache.lock().await;
        let mut log = self.log.lock().await;
        for entry in entries {
            // Serialize entry into a RecordBatch with a single Record whose
            // value carries the bincode payload. base_offset = entry.log_id.index.
            let payload = bincode::serde::encode_to_vec(&entry, standard())?;
            let mut batch = RecordBatch {
                base_offset: i64::try_from(entry.log_id.index).unwrap_or(i64::MAX),
                last_offset_delta: 0,
                records: vec![Record {
                    offset_delta: 0,
                    value: Some(Bytes::from(payload)),
                    ..Default::default()
                }],
                ..Default::default()
            };
            log.append(&mut batch)?;
            cache.entries.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    pub(crate) async fn truncate(&self, since: u64) -> Result<(), RaftError> {
        let mut cache = self.cache.lock().await;
        let mut log = self.log.lock().await;
        cache.entries.retain(|&k, _| k < since);
        log.truncate_to(i64::try_from(since).unwrap_or(i64::MAX))?;
        Ok(())
    }

    pub(crate) async fn save_vote(&self, vote: &Vote<NodeId>) -> Result<(), RaftError> {
        let bytes = bincode::serde::encode_to_vec(vote, standard())?;
        tokio::fs::write(&self.vote_path, &bytes)
            .await
            .map_err(crabka_log::LogError::Io)?;
        Ok(())
    }

    pub(crate) async fn read_vote(&self) -> Result<Option<Vote<NodeId>>, RaftError> {
        match tokio::fs::read(&self.vote_path).await {
            Ok(bytes) => {
                let (v, _) = bincode::serde::decode_from_slice(&bytes, standard())?;
                Ok(Some(v))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(RaftError::Storage(crabka_log::LogError::Io(e))),
        }
    }

    async fn last_purged(&self) -> u64 {
        self.cache.lock().await.last_purged
    }
}

/// Convert an internal `RaftError` into an openraft `StorageError` flagged
/// as a write-side log I/O failure. The exact subject/verb is opaque to
/// openraft beyond logging.
fn err_write(e: &RaftError) -> StorageError<NodeId> {
    let any = AnyError::new(e);
    StorageIOError::write_logs(any).into()
}

fn err_read(e: &RaftError) -> StorageError<NodeId> {
    let any = AnyError::new(e);
    StorageIOError::read_logs(any).into()
}

impl RaftLogReader<TypeConfig> for Arc<RaftLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        Ok(RaftLogStore::read_range(self, range).await)
    }
}

impl RaftLogStorage<TypeConfig> for Arc<RaftLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_log_id = self.last_log_id().await;
        let last_purged = self.last_purged().await;
        // Slice 7: snapshots deferred. We never purge, so last_purged_log_id
        // tracks only what truncate-from-below would do. Future snapshot
        // work will restore precision.
        let last_purged_log_id = (last_purged > 0).then(|| LogId {
            leader_id: openraft::LeaderId::new(0, 0),
            index: last_purged - 1,
        });
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        RaftLogStore::save_vote(self, vote)
            .await
            .map_err(|e| err_write(&e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        RaftLogStore::read_vote(self)
            .await
            .map_err(|e| err_read(&e))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        RaftLogStore::append(self, entries)
            .await
            .map_err(|e| err_write(&e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        RaftLogStore::truncate(self, log_id.index)
            .await
            .map_err(|e| err_write(&e))
    }

    async fn purge(&mut self, _log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Slice 7: snapshots deferred, so purge is a no-op. Future snapshot
        // work will compact the log behind the snapshot index.
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{Entry, EntryPayload, LeaderId, LogId};
    use tempfile::TempDir;

    #[tokio::test]
    async fn open_empty_returns_no_last_log_id() {
        let dir = TempDir::new().unwrap();
        let store = RaftLogStore::open(dir.path().to_path_buf()).await.unwrap();
        assert!(store.last_log_id().await.is_none());
    }

    #[tokio::test]
    async fn append_then_recover_round_trips() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let store = RaftLogStore::open(dir_path.clone()).await.unwrap();
            let entry: Entry<TypeConfig> = Entry {
                log_id: LogId {
                    leader_id: LeaderId::new(1, 1),
                    index: 1,
                },
                payload: EntryPayload::<TypeConfig>::Blank,
            };
            store.append(vec![entry]).await.unwrap();
        }
        let store2 = RaftLogStore::open(dir_path).await.unwrap();
        assert_eq!(store2.last_log_id().await.unwrap().index, 1);
    }
}
