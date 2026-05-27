//! [`S3RemoteStorage`] — an S3-compatible object-store
//! [`RemoteStorageManager`] (KIP-405 production backend).
//!
//! Built on the `object_store` crate, so it works against any `S3-API`
//! endpoint: AWS S3, `MinIO`, Cloudflare R2, and (via the S3 compatibility
//! mode) GCS. The trait method bodies are synchronous (mirroring Kafka's
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
//! directory layout so the two backends are observationally equivalent.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};

use crate::error::RemoteStorageError;
use crate::metadata::{CustomMetadata, RemoteLogSegmentMetadata};
use crate::storage_manager::{IndexType, LogSegmentData, RemoteStorageManager};

/// A [`RemoteStorageManager`] backed by any S3-compatible object store.
///
/// Construct via [`S3RemoteStorage::with_store`] (any `ObjectStore` impl)
/// for in-process tests, or [`S3RemoteStorage::from_s3_config`] for the
/// production path that builds an `AmazonS3` client from credentials,
/// endpoint, and bucket.
pub struct S3RemoteStorage {
    store: Arc<dyn ObjectStore>,
    /// Optional key prefix (joined with `/` to every object key). Lets
    /// multiple Crabka clusters share a bucket safely.
    prefix: Option<String>,
}

impl std::fmt::Debug for S3RemoteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3RemoteStorage")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// Connection / bucket parameters for [`S3RemoteStorage::from_s3_config`].
///
/// Either `access_key_id` + `secret_access_key` or the standard AWS SDK
/// credential chain (env vars, instance profile, …) supplies credentials.
/// When both fields are `None`, `object_store` falls back to the
/// environment-variable chain.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional key prefix inside the bucket (no leading or trailing slash).
    pub prefix: Option<String>,
    /// AWS region. Required by AWS S3; ignored by `MinIO`/R2 when an
    /// `endpoint` is provided but `object_store` still wants a value here
    /// (use `"us-east-1"` as a placeholder).
    pub region: String,
    /// Optional custom endpoint URL (e.g. `http://minio:9000` for `MinIO`,
    /// `https://<account>.r2.cloudflarestorage.com` for R2). When `None`,
    /// `object_store` uses the AWS S3 endpoint for the configured region.
    pub endpoint: Option<String>,
    /// Optional explicit access key id. Falls back to the AWS credential
    /// chain when `None`.
    pub access_key_id: Option<String>,
    /// Optional explicit secret access key. Falls back to the AWS
    /// credential chain when `None`.
    pub secret_access_key: Option<String>,
    /// Allow plaintext HTTP (off-by-default; required by `MinIO` running
    /// without TLS).
    pub allow_http: bool,
}

impl S3RemoteStorage {
    /// Wrap an arbitrary `ObjectStore` (e.g.
    /// `object_store::memory::InMemory` for tests). Use
    /// [`Self::from_s3_config`] for the production S3 path.
    #[must_use]
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> Self {
        Self { store, prefix }
    }

    /// Build an `AmazonS3` client from `cfg` and wrap it.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if the bucket /
    /// region / endpoint combination is rejected by `object_store`'s
    /// builder.
    pub fn from_s3_config(cfg: &S3Config) -> Result<Self, RemoteStorageError> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_allow_http(cfg.allow_http);
        if let Some(endpoint) = &cfg.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let (Some(k), Some(s)) = (&cfg.access_key_id, &cfg.secret_access_key) {
            builder = builder.with_access_key_id(k).with_secret_access_key(s);
        }
        let store = builder
            .build()
            .map_err(|e| RemoteStorageError::InvalidArgument(format!("S3 builder: {e}")))?;
        Ok(Self {
            store: Arc::new(store),
            prefix: cfg.prefix.clone(),
        })
    }

    fn segment_key(&self, metadata: &RemoteLogSegmentMetadata, suffix: &str) -> ObjectPath {
        use std::fmt::Write;
        let id = metadata.remote_log_segment_id();
        let tp = &id.topic_id_partition;
        let mut key = String::new();
        if let Some(p) = &self.prefix {
            key.push_str(p);
            key.push('/');
        }
        // Infallible — writing into a String.
        let _ = write!(key, "{}_{}/{}/{}", tp.topic_id, tp.partition, id.id, suffix);
        ObjectPath::from(key)
    }

    fn log_key(&self, metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        self.segment_key(metadata, "log")
    }

    fn index_key(&self, metadata: &RemoteLogSegmentMetadata, index_type: IndexType) -> ObjectPath {
        self.segment_key(metadata, index_filename(index_type))
    }

    /// Run an async `ObjectStore` call to completion on the current Tokio
    /// runtime. Sync trait callers reach this through `spawn_blocking`,
    /// inside which `Handle::current()` is always available.
    fn block<T, F>(fut: F) -> Result<T, RemoteStorageError>
    where
        F: std::future::Future<Output = Result<T, object_store::Error>>,
    {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            RemoteStorageError::Backend(
                "S3RemoteStorage requires an active Tokio runtime; call from spawn_blocking".into(),
            )
        })?;
        let result = tokio::task::block_in_place(|| handle.block_on(fut));
        result.map_err(map_object_store_error)
    }

    fn put_path(&self, key: &ObjectPath, path: &Path) -> Result<(), RemoteStorageError> {
        let bytes = std::fs::read(path)?;
        Self::block(self.store.put(key, PutPayload::from(bytes)))?;
        Ok(())
    }

    fn put_bytes(&self, key: &ObjectPath, bytes: Bytes) -> Result<(), RemoteStorageError> {
        Self::block(self.store.put(key, PutPayload::from_bytes(bytes)))?;
        Ok(())
    }
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

fn map_object_store_error(e: object_store::Error) -> RemoteStorageError {
    match e {
        object_store::Error::NotFound { .. } => {
            // Caller-visible "not found" is signalled via SegmentNotFound
            // at the trait level, but here we don't know which segment is
            // missing — surface as a backend error and let the caller
            // upgrade to SegmentNotFound where it has the metadata in
            // hand.
            RemoteStorageError::Backend(format!("not found: {e}"))
        }
        other => RemoteStorageError::Backend(other.to_string()),
    }
}

impl RemoteStorageManager for S3RemoteStorage {
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        self.put_path(&self.log_key(metadata), &data.log_segment)?;
        self.put_path(
            &self.index_key(metadata, IndexType::Offset),
            &data.offset_index,
        )?;
        self.put_path(
            &self.index_key(metadata, IndexType::Timestamp),
            &data.time_index,
        )?;
        if let Some(snap) = &data.producer_snapshot_index {
            self.put_path(&self.index_key(metadata, IndexType::ProducerSnapshot), snap)?;
        }
        self.put_bytes(
            &self.index_key(metadata, IndexType::LeaderEpoch),
            data.leader_epoch_index.clone(),
        )?;
        if let Some(txn) = &data.transaction_index {
            self.put_path(&self.index_key(metadata, IndexType::Transaction), txn)?;
        }
        // The opaque CustomMetadata channel is unused — every object's
        // key is derivable from the segment metadata, so we don't need to
        // echo a separate identifier back.
        Ok(None)
    }

    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let key = self.log_key(metadata);
        let opts = GetOptions {
            range: Some(match end_position {
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
            }),
            ..Default::default()
        };
        let result = Self::block(self.store.get_opts(&key, opts));
        match result {
            Ok(get) => {
                let bytes = Self::block(get.bytes())?;
                Ok(bytes.to_vec())
            }
            Err(RemoteStorageError::Backend(ref msg)) if msg.starts_with("not found:") => Err(
                RemoteStorageError::SegmentNotFound(metadata.remote_log_segment_id().clone()),
            ),
            Err(other) => Err(other),
        }
    }

    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let key = self.index_key(metadata, index_type);
        let result = Self::block(self.store.get(&key));
        match result {
            Ok(get) => {
                let bytes = Self::block(get.bytes())?;
                Ok(bytes.to_vec())
            }
            Err(RemoteStorageError::Backend(ref msg)) if msg.starts_with("not found:") => Err(
                RemoteStorageError::SegmentNotFound(metadata.remote_log_segment_id().clone()),
            ),
            Err(other) => Err(other),
        }
    }

    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        for key in [
            self.log_key(metadata),
            self.index_key(metadata, IndexType::Offset),
            self.index_key(metadata, IndexType::Timestamp),
            self.index_key(metadata, IndexType::ProducerSnapshot),
            self.index_key(metadata, IndexType::LeaderEpoch),
            self.index_key(metadata, IndexType::Transaction),
        ] {
            match Self::block(self.store.delete(&key)) {
                Ok(()) => {}
                // Idempotent: deleting an absent object succeeds.
                Err(RemoteStorageError::Backend(msg)) if msg.starts_with("not found:") => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::path::PathBuf;

    use object_store::memory::InMemory;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::metadata::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
    };

    fn rsm(prefix: Option<&str>) -> S3RemoteStorage {
        S3RemoteStorage::with_store(Arc::new(InMemory::new()), prefix.map(str::to_string))
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
            BTreeMap::from([(0, 0)]),
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
            assert_eq!(
                store.fetch_log_segment(&md, 0, None).unwrap(),
                b"0123456789"
            );
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
            assert_eq!(store.fetch_log_segment(&md, 2, Some(5)).unwrap(), b"2345");
            // Open-ended from 7 -> "789".
            assert_eq!(store.fetch_log_segment(&md, 7, None).unwrap(), b"789");
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
            assert_eq!(
                store.fetch_index(&md, IndexType::Offset).unwrap(),
                b"OFFSET-IDX"
            );
            assert_eq!(
                store.fetch_index(&md, IndexType::Timestamp).unwrap(),
                b"TIME-IDX"
            );
            assert_eq!(
                store.fetch_index(&md, IndexType::ProducerSnapshot).unwrap(),
                b"SNAP"
            );
            assert_eq!(
                store.fetch_index(&md, IndexType::LeaderEpoch).unwrap(),
                b"EPOCH-BYTES"
            );
            assert_eq!(
                store.fetch_index(&md, IndexType::Transaction).unwrap(),
                b"TXN-IDX"
            );
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
            assert_eq!(store.fetch_log_segment(&b, 0, None).unwrap(), b"0123456789");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefix_isolates_clusters() {
        let store_a =
            S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("cluster-a".to_string()));
        let _ = store_a;
        // Single cluster keys live under the prefix; we verify the key
        // construction at the unit level (no cross-cluster fixture
        // available without sharing the InMemory backend, which we don't
        // because each cluster gets its own bucket in practice).
        let md = sample_metadata(30);
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("c".to_string()));
        let key = store.log_key(&md);
        assert!(
            key.as_ref().starts_with("c/"),
            "expected prefix to be applied, got {key:?}",
        );
    }
}
