//! [`S3RemoteStorage`] — an S3-compatible object-store
//! [`RemoteStorageManager`] (KIP-405 production backend).
//!
//! Built on the `object_store` crate, so it works against any `S3-API`
//! endpoint: AWS S3, `MinIO`, and Cloudflare R2. (Google Cloud Storage has a
//! dedicated native backend — see [`from_gcs_config`](S3RemoteStorage::from_gcs_config)
//! in [`crate::gcs`] — which supports keyless GKE Workload Identity instead
//! of the legacy S3-compatibility/HMAC shim.) The trait method bodies are
//! synchronous (mirroring Kafka's
//! blocking `RemoteStorageManager`); the broker drives them from
//! `spawn_blocking`. Internally we block on the async `object_store` calls
//! via the current Tokio runtime handle, which is always present inside a
//! `spawn_blocking` worker spawned by Tokio.
//!
//! ## Object-key layout
//!
//! ```text
//! <prefix?>/<topic_id>_<partition>/<segment_uuid>/log
//! <prefix?>/<topic_id>_<partition>/<segment_uuid>/offset_index
//! <prefix?>/<topic_id>_<partition>/<segment_uuid>/time_index
//! <prefix?>/<topic_id>_<partition>/<segment_uuid>/producer_snapshot   (when present)
//! <prefix?>/<topic_id>_<partition>/<segment_uuid>/leader_epoch
//! <prefix?>/<topic_id>_<partition>/<segment_uuid>/txn_index           (when present)
//! ```
//!
//! Keys mirror [`LocalTieredStorage`](crate::LocalTieredStorage)'s
//! directory layout so the two backends are observationally equivalent. When a
//! remote-storage prefix is configured, the object-store handle applies it via a
//! `PrefixStore`; the segment-key helpers always produce keys relative to that
//! scoped store.

use std::sync::Arc;

use crabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, ObjectOps, ObjectStoreClient,
    ObjectStoreConfig, ObjectStoreError, S3Config, build_object_store,
};
use object_store::{GetRange, ObjectStore, path::Path as ObjectPath, prefix::PrefixStore};
use tracing::instrument;

use crate::{
    error::RemoteStorageError,
    metadata::{CustomMetadata, RemoteLogSegmentMetadata},
    storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
};

/// A [`RemoteStorageManager`] backed by any S3-compatible object store.
///
/// Construct via [`S3RemoteStorage::with_store`] (any `ObjectStore` impl)
/// for in-process tests, or [`S3RemoteStorage::from_s3_config`] for the
/// production path that builds an `AmazonS3` client from credentials,
/// endpoint, and bucket.
pub struct S3RemoteStorage {
    ops: ObjectStoreClient,
    /// Optional key prefix already applied by the wrapped object-store handle.
    /// Kept for diagnostics; segment-key construction stays prefix-relative.
    prefix: Option<String>,
    /// File-size threshold above which uploads switch to S3 multipart.
    multipart_threshold: u64,
    /// Per-part size used by the multipart path.
    multipart_chunk_size: usize,
}

impl std::fmt::Debug for S3RemoteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3RemoteStorage")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3RemoteStorage {
    /// Wrap an arbitrary, unscoped `ObjectStore` (e.g.
    /// `object_store::memory::InMemory` for tests). When `prefix` is non-empty,
    /// this wraps `store` in a `PrefixStore`; pass `None` if the store is
    /// already scoped. Use [`Self::from_s3_config`] for the production S3 path.
    /// Multipart tuning falls back to the [`DEFAULT_MULTIPART_THRESHOLD`] /
    /// [`DEFAULT_MULTIPART_CHUNK_SIZE`] constants; call
    /// [`Self::with_multipart_tuning`] to override in tests.
    #[must_use]
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> Self {
        let prefix = normalize_prefix(prefix);
        let store = prefix_object_store(store, prefix.as_deref());
        Self::with_prefix_applied_store(store, prefix)
    }

    /// Override the multipart threshold + chunk size. Returns `self` for
    /// chaining. Tests use this to force the multipart path on small
    /// fixtures; production typically leaves the defaults alone.
    #[must_use]
    pub fn with_multipart_tuning(mut self, threshold: u64, chunk_size: usize) -> Self {
        self.multipart_threshold = threshold;
        self.multipart_chunk_size = chunk_size;
        self
    }

    /// Build an `AmazonS3` client from `cfg` and wrap it.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if the bucket /
    /// region / endpoint combination is rejected by `object_store`'s
    /// builder.
    pub fn from_s3_config(cfg: &S3Config) -> Result<Self, RemoteStorageError> {
        let store = build_object_store(&ObjectStoreConfig::S3(cfg.clone()))
            .map_err(|e| RemoteStorageError::InvalidArgument(e.to_string()))?;
        Ok(Self::with_prefix_applied_store(store, cfg.prefix.clone())
            .with_multipart_tuning(cfg.multipart_threshold, cfg.multipart_chunk_size))
    }

    pub(super) fn with_prefix_applied_store(
        store: Arc<dyn ObjectStore>,
        prefix: Option<String>,
    ) -> Self {
        Self {
            ops: ObjectStoreClient::new(store),
            prefix: normalize_prefix(prefix),
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
        }
    }

    fn segment_key(metadata: &RemoteLogSegmentMetadata, suffix: &str) -> ObjectPath {
        use std::fmt::Write;
        let id = metadata.remote_log_segment_id();
        let tp = &id.topic_id_partition;
        let mut key = String::new();
        // Infallible — writing into a String.
        let _ = write!(key, "{}_{}/{}/{}", tp.topic_id, tp.partition, id.id, suffix);
        ObjectPath::from(key)
    }

    fn log_key(metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        Self::segment_key(metadata, "log")
    }

    fn index_key(metadata: &RemoteLogSegmentMetadata, index_type: IndexType) -> ObjectPath {
        Self::segment_key(metadata, index_filename(index_type))
    }

    /// Run an async [`ObjectOps`] call to completion on the current Tokio
    /// runtime. Sync trait callers reach this through `spawn_blocking`, inside
    /// which `Handle::current()` is always available. The `block_on` bridge
    /// lives here, never in the substrate.
    fn block_os<T, F>(fut: F) -> Result<T, ObjectStoreError>
    where
        F: std::future::Future<Output = Result<T, ObjectStoreError>>,
    {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ObjectStoreError::Backend(
                "S3RemoteStorage requires an active Tokio runtime; call from spawn_blocking".into(),
            )
        })?;
        tokio::task::block_in_place(|| handle.block_on(fut))
    }
}

fn normalize_prefix(prefix: Option<String>) -> Option<String> {
    prefix.filter(|prefix| !prefix.is_empty())
}

fn prefix_object_store(store: Arc<dyn ObjectStore>, prefix: Option<&str>) -> Arc<dyn ObjectStore> {
    let Some(prefix) = prefix.filter(|prefix| !prefix.is_empty()) else {
        return store;
    };
    Arc::new(PrefixStore::new(store, ObjectPath::from(prefix)))
}

fn index_filename(index_type: IndexType) -> &'static str {
    match index_type {
        IndexType::Offset => "offset_index",
        IndexType::Timestamp => "time_index",
        IndexType::ProducerSnapshot => "producer_snapshot",
        IndexType::LeaderEpoch => "leader_epoch",
        IndexType::Transaction => "txn_index",
    }
}

impl RemoteStorageManager for S3RemoteStorage {
    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_offset = metadata.start_offset(),
            end_offset = metadata.end_offset(),
        ),
        err
    )]
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        let threshold = self.multipart_threshold;
        let chunk_size = self.multipart_chunk_size;
        Self::block_os(self.ops.put_from_path(
            &Self::log_key(metadata),
            &data.log_segment,
            threshold,
            chunk_size,
        ))?;
        Self::block_os(self.ops.put_from_path(
            &Self::index_key(metadata, IndexType::Offset),
            &data.offset_index,
            threshold,
            chunk_size,
        ))?;
        Self::block_os(self.ops.put_from_path(
            &Self::index_key(metadata, IndexType::Timestamp),
            &data.time_index,
            threshold,
            chunk_size,
        ))?;
        if let Some(snap) = &data.producer_snapshot_index {
            Self::block_os(self.ops.put_from_path(
                &Self::index_key(metadata, IndexType::ProducerSnapshot),
                snap,
                threshold,
                chunk_size,
            ))?;
        }
        Self::block_os(self.ops.put(
            &Self::index_key(metadata, IndexType::LeaderEpoch),
            data.leader_epoch_index.clone(),
        ))?;
        if let Some(txn) = &data.transaction_index {
            Self::block_os(self.ops.put_from_path(
                &Self::index_key(metadata, IndexType::Transaction),
                txn,
                threshold,
                chunk_size,
            ))?;
        }
        // The opaque CustomMetadata channel is unused — every object's
        // key is derivable from the segment metadata, so we don't need to
        // echo a separate identifier back.
        Ok(None)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_position,
            end_position = ?end_position,
        ),
        err
    )]
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let key = Self::log_key(metadata);
        let range = match end_position {
            Some(end) => {
                if end < start_position {
                    return Err(RemoteStorageError::InvalidArgument(format!(
                        "end_position {end} < start_position {start_position}"
                    )));
                }
                // GetRange::Bounded is half-open [start, end); the trait
                // contract is inclusive end, so add 1 and saturate.
                GetRange::Bounded(u64::from(start_position)..u64::from(end).saturating_add(1))
            }
            None => GetRange::Offset(u64::from(start_position)),
        };
        match Self::block_os(self.ops.get_range(&key, range)) {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(ObjectStoreError::NotFound(_)) => Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            )),
            Err(other) => Err(other.into()),
        }
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            index_type = ?index_type,
        ),
        err
    )]
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let key = Self::index_key(metadata, index_type);
        match Self::block_os(self.ops.get(&key)) {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(ObjectStoreError::NotFound(_)) => Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            )),
            Err(other) => Err(other.into()),
        }
    }

    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
        ),
        err
    )]
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        for key in [
            Self::log_key(metadata),
            Self::index_key(metadata, IndexType::Offset),
            Self::index_key(metadata, IndexType::Timestamp),
            Self::index_key(metadata, IndexType::ProducerSnapshot),
            Self::index_key(metadata, IndexType::LeaderEpoch),
            Self::index_key(metadata, IndexType::Transaction),
        ] {
            match Self::block_os(self.ops.delete(&key)) {
                // Idempotent: deleting an absent object succeeds.
                Ok(()) | Err(ObjectStoreError::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Write, path::PathBuf};

    use assert2::{assert, check};
    use bytes::Bytes;
    use crabka_ids::LeaderEpoch;
    use object_store::memory::InMemory;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::metadata::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
    };

    fn memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new()) as Arc<dyn ObjectStore>
    }

    fn rsm(prefix: Option<&str>) -> S3RemoteStorage {
        S3RemoteStorage::with_store(memory_store(), prefix.map(str::to_string))
    }

    async fn list_raw_keys(store: Arc<dyn ObjectStore>) -> Vec<String> {
        let client = crabka_object_store::ObjectStoreClient::new(store);
        let mut keys: Vec<String> = crabka_object_store::ObjectOps::list(&client, None)
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.as_ref().to_owned())
            .collect();
        keys.sort();
        keys
    }

    async fn put_raw_object(store: Arc<dyn ObjectStore>, key: &str, bytes: Bytes) {
        let client = crabka_object_store::ObjectStoreClient::new(store);
        crabka_object_store::ObjectOps::put(&client, &ObjectPath::from(key), bytes)
            .await
            .unwrap();
    }

    fn relative_segment_keys(metadata: &RemoteLogSegmentMetadata) -> Vec<String> {
        let mut keys: Vec<String> = [
            S3RemoteStorage::log_key(metadata),
            S3RemoteStorage::index_key(metadata, IndexType::Offset),
            S3RemoteStorage::index_key(metadata, IndexType::Timestamp),
            S3RemoteStorage::index_key(metadata, IndexType::ProducerSnapshot),
            S3RemoteStorage::index_key(metadata, IndexType::LeaderEpoch),
            S3RemoteStorage::index_key(metadata, IndexType::Transaction),
        ]
        .into_iter()
        .map(|key| key.as_ref().to_owned())
        .collect();
        keys.sort();
        keys
    }

    fn prefixed_keys(prefix: &str, relative_keys: &[String]) -> Vec<String> {
        relative_keys
            .iter()
            .map(|key| format!("{prefix}/{key}"))
            .collect()
    }

    #[test]
    fn storage_debug_is_nonempty() {
        // The S3RemoteStorage Debug impl must render something (a `fmt`
        // replaced with `Ok(())` would print nothing).
        let dbg = format!("{:?}", rsm(None));
        assert!(dbg.contains("S3RemoteStorage"));
    }

    fn sample_metadata(id: u128) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
                Uuid::from_u128(id),
            ),
            0,
            99,
            123,
            1,
            456,
            8,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(LeaderEpoch(0), 0)]),
        )
        .unwrap()
    }

    fn write_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> PathBuf {
        let p = dir.join(name);
        std::fs::File::create(&p)
            .unwrap()
            .write_all(contents)
            .unwrap();
        p
    }

    fn sample_data(src: &std::path::Path, with_txn: bool) -> LogSegmentData {
        LogSegmentData {
            log_segment: write_file(src, "00.log", b"0123456789"),
            offset_index: write_file(src, "00.index", b"OFFSET-IDX"),
            time_index: write_file(src, "00.timeindex", b"TIME-IDX"),
            transaction_index: with_txn.then(|| write_file(src, "00.txnindex", b"TXN-IDX")),
            producer_snapshot_index: Some(write_file(src, "00.snapshot", b"SNAP")),
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_then_fetch_full_segment() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(10);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();
            assert!(store.fetch_log_segment(&md, 0, None).unwrap() == b"0123456789");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_partial_byte_ranges() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(10);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), false))
                .unwrap();
            // Inclusive [2, 5] -> "2345".
            assert!(store.fetch_log_segment(&md, 2, Some(5)).unwrap() == b"2345");
            // Open-ended from 7 -> "789".
            assert!(store.fetch_log_segment(&md, 7, None).unwrap() == b"789");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_single_byte_range_start_equals_end() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(10);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), false))
                .unwrap();
            // Inclusive [3, 3] is a valid single-byte range -> "3" (the guard
            // is `end < start_position`, not `<=`/`==`).
            assert!(store.fetch_log_segment(&md, 3, Some(3)).unwrap() == b"3");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_each_index_type() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(11);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();
            for (index_type, want) in [
                (IndexType::Offset, b"OFFSET-IDX".as_ref()),
                (IndexType::Timestamp, b"TIME-IDX".as_ref()),
                (IndexType::ProducerSnapshot, b"SNAP".as_ref()),
                (IndexType::LeaderEpoch, b"EPOCH-BYTES".as_ref()),
                (IndexType::Transaction, b"TXN-IDX".as_ref()),
            ] {
                check!(
                    store.fetch_index(&md, index_type).unwrap() == want,
                    "{index_type:?}"
                );
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_before_copy_is_not_found() {
        let store = rsm(None);
        let md = sample_metadata(404);
        let err = tokio::task::spawn_blocking(move || store.fetch_log_segment(&md, 0, None))
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_optional_txn_index_is_not_found() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(12);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), false))
                .unwrap();
            let err = store.fetch_index(&md, IndexType::Transaction).unwrap_err();
            assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_is_idempotent() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(13);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();
            store.delete_log_segment_data(&md).unwrap();
            store.delete_log_segment_data(&md).unwrap();
            assert!(matches!(
                store.fetch_log_segment(&md, 0, None).unwrap_err(),
                RemoteStorageError::SegmentNotFound(_)
            ));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segments_are_isolated_by_id() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let a = sample_metadata(20);
        let b = sample_metadata(21);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&a, &sample_data(src.path(), false))
                .unwrap();
            store
                .copy_log_segment_data(&b, &sample_data(src.path(), false))
                .unwrap();
            store.delete_log_segment_data(&a).unwrap();
            assert!(store.fetch_log_segment(&b, 0, None).unwrap() == b"0123456789");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefix_is_applied_once_for_fetches() {
        let raw = memory_store();
        let prefixed = prefix_object_store(Arc::clone(&raw), Some("cluster-a"));
        let store = Arc::new(S3RemoteStorage::with_prefix_applied_store(
            prefixed,
            Some("cluster-a".to_string()),
        ));
        let md = sample_metadata(30);
        let log_key = format!("cluster-a/{}", S3RemoteStorage::log_key(&md).as_ref());
        let offset_key = format!(
            "cluster-a/{}",
            S3RemoteStorage::index_key(&md, IndexType::Offset).as_ref()
        );
        put_raw_object(Arc::clone(&raw), &log_key, Bytes::from_static(b"abcdef")).await;
        put_raw_object(
            Arc::clone(&raw),
            &offset_key,
            Bytes::from_static(b"OFFSET-IDX"),
        )
        .await;

        let store_for_fetch = Arc::clone(&store);
        tokio::task::spawn_blocking(move || {
            assert!(store_for_fetch.fetch_log_segment(&md, 1, Some(3)).unwrap() == b"bcd");
            assert!(store_for_fetch.fetch_index(&md, IndexType::Offset).unwrap() == b"OFFSET-IDX");
        })
        .await
        .unwrap();

        let mut expected_keys = vec![log_key, offset_key];
        expected_keys.sort();
        assert!(list_raw_keys(raw).await == expected_keys);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefix_is_applied_once_for_copy_delete_and_list() {
        let raw = memory_store();
        let prefixed = prefix_object_store(Arc::clone(&raw), Some("cluster-a"));
        let store = Arc::new(S3RemoteStorage::with_prefix_applied_store(
            prefixed,
            Some("cluster-a".to_string()),
        ));
        let src = TempDir::new().unwrap();
        let md = sample_metadata(31);
        let expected_keys = prefixed_keys("cluster-a", &relative_segment_keys(&md));
        let store_for_copy = Arc::clone(&store);
        let md_for_copy = md.clone();
        tokio::task::spawn_blocking(move || {
            store_for_copy
                .copy_log_segment_data(&md_for_copy, &sample_data(src.path(), true))
                .unwrap();
            assert!(
                store_for_copy
                    .fetch_log_segment(&md_for_copy, 0, None)
                    .unwrap()
                    == b"0123456789"
            );
            assert!(
                store_for_copy
                    .fetch_index(&md_for_copy, IndexType::Offset)
                    .unwrap()
                    == b"OFFSET-IDX"
            );
        })
        .await
        .unwrap();

        let keys_after_copy = list_raw_keys(Arc::clone(&raw)).await;
        assert!(keys_after_copy == expected_keys);
        assert!(
            !keys_after_copy
                .iter()
                .any(|key| key.starts_with("cluster-a/cluster-a/"))
        );

        let store_for_delete = Arc::clone(&store);
        let md_for_delete = md;
        tokio::task::spawn_blocking(move || {
            store_for_delete
                .delete_log_segment_data(&md_for_delete)
                .unwrap();
        })
        .await
        .unwrap();
        assert!(list_raw_keys(raw).await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_prefix_keeps_relative_keys_for_copy_fetch_delete_and_list() {
        let raw = memory_store();
        let store = Arc::new(S3RemoteStorage::with_store(Arc::clone(&raw), None));
        let src = TempDir::new().unwrap();
        let md = sample_metadata(32);
        let expected_keys = relative_segment_keys(&md);
        let store_for_copy = Arc::clone(&store);
        let md_for_copy = md.clone();
        tokio::task::spawn_blocking(move || {
            store_for_copy
                .copy_log_segment_data(&md_for_copy, &sample_data(src.path(), true))
                .unwrap();
            assert!(
                store_for_copy
                    .fetch_log_segment(&md_for_copy, 0, None)
                    .unwrap()
                    == b"0123456789"
            );
            assert!(
                store_for_copy
                    .fetch_index(&md_for_copy, IndexType::Offset)
                    .unwrap()
                    == b"OFFSET-IDX"
            );
        })
        .await
        .unwrap();

        assert!(list_raw_keys(Arc::clone(&raw)).await == expected_keys);

        let store_for_delete = Arc::clone(&store);
        let md_for_delete = md;
        tokio::task::spawn_blocking(move || {
            store_for_delete
                .delete_log_segment_data(&md_for_delete)
                .unwrap();
        })
        .await
        .unwrap();
        assert!(list_raw_keys(raw).await.is_empty());
    }

    fn write_log_segment(dir: &std::path::Path, len: usize) -> PathBuf {
        let p = dir.join("00.log");
        let mut f = std::fs::File::create(&p).unwrap();
        // Deterministic, position-sensitive bytes so the round-trip
        // assertion catches both reordering bugs and truncation.
        let bytes: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
        f.write_all(&bytes).unwrap();
        p
    }

    /// Files at or above `multipart_threshold` flow through the `ObjectOps`
    /// multipart path. We pick a chunk size that yields multiple
    /// non-trailing parts so the inner loop's tail-flush + finish path is
    /// exercised. The `InMemory` backend implements `put_multipart` /
    /// `complete` end-to-end, so a successful round-trip proves the
    /// multipart wire calls are stitched correctly (per-part offsets and
    /// the final concatenation).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_path_uses_multipart_above_threshold_and_round_trips() {
        // 100 KiB segment, 8 KiB threshold → multipart, 4 KiB chunks
        // → 25 parts (last one full, no tail).
        const SEG_LEN: usize = 100 * 1024;
        const CHUNK: usize = 4 * 1024;
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_multipart_tuning(8 * 1024, CHUNK);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(40);
        let log_path = write_log_segment(src.path(), SEG_LEN);
        let data = LogSegmentData {
            log_segment: log_path,
            offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
            time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: Some(write_file(src.path(), "00.snapshot", b"SNAP")),
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        };
        tokio::task::spawn_blocking(move || {
            store.copy_log_segment_data(&md, &data).unwrap();
            let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
            assert!(fetched.len() == SEG_LEN);
            for (i, b) in fetched.iter().enumerate() {
                assert!(*b == u8::try_from(i % 251).unwrap(), "byte mismatch at {i}");
            }
        })
        .await
        .unwrap();
    }

    /// Multipart path with a tail chunk strictly smaller than
    /// `chunk_size`. `WriteMultipart::finish` is supposed to flush the
    /// partially-filled buffer as the final part; this test asserts that
    /// happens (otherwise the last `tail_len` bytes would be silently
    /// dropped from the uploaded object).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_flushes_partial_tail_chunk() {
        const CHUNK: usize = 4 * 1024;
        const SEG_LEN: usize = 3 * CHUNK + 137; // 3 full parts + tail
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_multipart_tuning(1024, CHUNK);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(41);
        let log_path = write_log_segment(src.path(), SEG_LEN);
        let data = LogSegmentData {
            log_segment: log_path,
            offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
            time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        };
        tokio::task::spawn_blocking(move || {
            store.copy_log_segment_data(&md, &data).unwrap();
            let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
            assert!(fetched.len() == SEG_LEN);
            assert!(
                fetched.last().copied() == Some(u8::try_from((SEG_LEN - 1) % 251).unwrap()),
                "tail byte was dropped"
            );
        })
        .await
        .unwrap();
    }

    /// Files strictly below the threshold MUST still take the single-PUT
    /// path even when multipart tuning is wired up. We exercise that by
    /// raising the threshold above the fixture size; a regression that
    /// inverted the branch would surface as a hang or multipart-specific
    /// error against a backend without multipart support (and would also
    /// be a latency regression in production).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_path_stays_on_single_put_below_threshold() {
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_multipart_tuning(1024 * 1024, 4 * 1024);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(42);
        let log_path = write_log_segment(src.path(), 10); // ten bytes, well under 1 MiB
        let data = LogSegmentData {
            log_segment: log_path,
            offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
            time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        };
        tokio::task::spawn_blocking(move || {
            store.copy_log_segment_data(&md, &data).unwrap();
            let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
            assert!(fetched.len() == 10);
        })
        .await
        .unwrap();
    }
}
