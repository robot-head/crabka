//! Versioned range-map metadata stored through range 0.

use std::sync::Arc;

use crabka_pgexec::{ExecError, Linearizer};
use tokio::sync::Mutex;

use crate::RangeMap;

const RANGE_MAP_METADATA_FORMAT_VERSION: u16 = 1;

/// Monotonically versioned range-map blob applied from range 0.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeMapMetadata {
    /// Metadata envelope version.
    pub format_version: u16,
    /// Monotonic map version. Readers replace their cache only when this increases.
    pub version: u64,
    /// Validated v2 range map descriptor.
    pub map: RangeMap,
}

impl RangeMapMetadata {
    /// Build a parsed metadata envelope.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn new(version: u64, map: RangeMap) -> Result<Self, RangeMapMetadataError> {
        if version == 0 {
            return Err(RangeMapMetadataError::InvalidVersion { version });
        }

        Ok(Self {
            format_version: RANGE_MAP_METADATA_FORMAT_VERSION,
            version,
            map,
        })
    }

    /// Encode the metadata blob written to range 0.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn encode(&self) -> Result<Vec<u8>, RangeMapMetadataError> {
        self.ensure_current_format()?;
        Ok(serde_json::to_vec(self)?)
    }

    /// Decode and validate a metadata blob read from range 0.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn decode(bytes: &[u8]) -> Result<Self, RangeMapMetadataError> {
        let metadata: Self = serde_json::from_slice(bytes)?;
        metadata.ensure_current_format()?;
        if metadata.version == 0 {
            return Err(RangeMapMetadataError::InvalidVersion {
                version: metadata.version,
            });
        }

        Ok(metadata)
    }

    fn ensure_current_format(&self) -> Result<(), RangeMapMetadataError> {
        if self.format_version != RANGE_MAP_METADATA_FORMAT_VERSION {
            return Err(RangeMapMetadataError::UnsupportedFormat {
                format_version: self.format_version,
            });
        }

        Ok(())
    }
}

/// Range-0 append seam used by tests and substrate-backed writers.
pub trait RangeMapCommitter: Send + Sync {
    /// Commit an encoded metadata blob through range 0.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn commit_range_map_metadata(
        &self,
        metadata: &RangeMapMetadata,
    ) -> Result<(), RangeMapMetadataError>;
}

/// Loader seam for range-map readers.
pub trait RangeMapLoader: Send + Sync {
    /// Load the latest metadata visible after a range-0 barrier.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn load_range_map_metadata(&self) -> Result<RangeMapMetadata, RangeMapMetadataError>;
}

/// Writes monotonically versioned range-map metadata through range 0.
pub struct RangeMapMetadataWriter<C> {
    committer: C,
    current_version: u64,
}

impl<C> RangeMapMetadataWriter<C>
where
    C: RangeMapCommitter,
{
    /// Build a writer starting at the supplied current version.
    #[must_use]
    pub const fn new(committer: C, current_version: u64) -> Self {
        Self {
            committer,
            current_version,
        }
    }

    /// Commit a new map with `current_version + 1`.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn commit_next(
        &mut self,
        map: RangeMap,
    ) -> Result<RangeMapMetadata, RangeMapMetadataError> {
        let version = self
            .current_version
            .checked_add(1)
            .ok_or(RangeMapMetadataError::VersionOverflow)?;
        let metadata = RangeMapMetadata::new(version, map)?;
        self.committer.commit_range_map_metadata(&metadata)?;
        self.current_version = version;
        Ok(metadata)
    }
}

/// Cached `(version, map)` value returned to statement execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedRangeMap {
    /// Version observed when the statement acquired its map.
    pub version: u64,
    /// Range map pinned for the statement.
    pub map: RangeMap,
}

impl From<RangeMapMetadata> for LoadedRangeMap {
    fn from(metadata: RangeMapMetadata) -> Self {
        Self {
            version: metadata.version,
            map: metadata.map,
        }
    }
}

/// Barrier-gated range-map reader cache.
pub struct RangeMapMetadataReader<L> {
    loader: L,
    cached: Arc<Mutex<LoadedRangeMap>>,
}

impl<L> RangeMapMetadataReader<L>
where
    L: RangeMapLoader,
{
    /// Build a reader from an initial range-0-applied metadata value.
    #[must_use]
    pub fn new(loader: L, initial: RangeMapMetadata) -> Self {
        Self {
            loader,
            cached: Arc::new(Mutex::new(initial.into())),
        }
    }

    /// Return the map pinned for an already-open snapshot, without refreshing.
    pub async fn map_for_open_snapshot(&self) -> LoadedRangeMap {
        self.cached.lock().await.clone()
    }

    /// Refresh behind a range-0 barrier for a fresh snapshot statement.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn map_for_fresh_snapshot(
        &self,
        barrier: &dyn Linearizer,
    ) -> Result<LoadedRangeMap, RangeMapMetadataError> {
        barrier.ensure_readable().await?;
        let loaded = LoadedRangeMap::from(self.loader.load_range_map_metadata()?);
        let mut cached = self.cached.lock().await;
        if loaded.version > cached.version {
            *cached = loaded;
        }

        Ok(cached.clone())
    }
}

/// Metadata parsing and range-0 commit errors.
#[derive(Debug, thiserror::Error)]
pub enum RangeMapMetadataError {
    /// Metadata JSON was malformed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The metadata envelope version is not supported by this greenfield build.
    #[error("unsupported range map metadata format_version {format_version}")]
    UnsupportedFormat { format_version: u16 },
    /// Metadata versions start at one.
    #[error("range map metadata version must be greater than zero, found {version}")]
    InvalidVersion { version: u64 },
    /// Version increment overflowed.
    #[error("range map metadata version overflow")]
    VersionOverflow,
    /// Range 0 rejected the commit.
    #[error("range-0 metadata commit failed: {0}")]
    Commit(String),
    /// The range-0 read barrier failed.
    #[error("range-0 read barrier failed: {0:?}")]
    Barrier(ExecError),
}

impl From<ExecError> for RangeMapMetadataError {
    fn from(error: ExecError) -> Self {
        Self::Barrier(error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };

    use assert2::assert;
    use crabka_pgexec::Linearizer;

    use super::*;
    use crate::{MapEpoch, RangeId, RangeSpec, TableId, TenantName};

    fn tenant() -> TenantName {
        TenantName::parse("tenant_a").unwrap()
    }

    fn map(epoch: u64, boundary: u64) -> RangeMap {
        RangeMap::new(
            tenant(),
            MapEpoch::new(epoch),
            vec![
                RangeSpec::new(
                    RangeId::COORDINATOR,
                    TableId::ZERO,
                    Some(TableId::new(boundary)),
                ),
                RangeSpec::new(RangeId::new(1), TableId::new(boundary), None),
            ],
        )
        .unwrap()
    }

    #[derive(Clone, Default)]
    struct MemoryRange0 {
        metadata: Arc<StdMutex<Option<RangeMapMetadata>>>,
    }

    impl RangeMapCommitter for MemoryRange0 {
        fn commit_range_map_metadata(
            &self,
            metadata: &RangeMapMetadata,
        ) -> Result<(), RangeMapMetadataError> {
            *self
                .metadata
                .lock()
                .expect("metadata mutex is not poisoned") = Some(metadata.clone());
            Ok(())
        }
    }

    impl RangeMapLoader for MemoryRange0 {
        fn load_range_map_metadata(&self) -> Result<RangeMapMetadata, RangeMapMetadataError> {
            self.metadata
                .lock()
                .expect("metadata mutex is not poisoned")
                .clone()
                .ok_or_else(|| RangeMapMetadataError::Commit("missing metadata".into()))
        }
    }

    struct CountingBarrier {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Linearizer for CountingBarrier {
        async fn ensure_readable(&self) -> Result<(), ExecError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn metadata_encode_decode_roundtrip() {
        let metadata = RangeMapMetadata::new(1, map(1, 10)).unwrap();
        let decoded = RangeMapMetadata::decode(&metadata.encode().unwrap()).unwrap();

        assert!(decoded == metadata);
    }

    #[test]
    fn writer_commits_monotonic_version_through_range0_seam() {
        let range0 = MemoryRange0::default();
        let mut writer = RangeMapMetadataWriter::new(range0.clone(), 0);

        let committed = writer.commit_next(map(1, 10)).unwrap();

        assert!(committed.version == 1);
        assert!(range0.load_range_map_metadata().unwrap() == committed);
    }

    #[tokio::test]
    async fn map_bump_visible_to_barriered_fresh_reader() {
        let range0 = MemoryRange0::default();
        let initial = RangeMapMetadata::new(1, map(1, 10)).unwrap();
        range0.commit_range_map_metadata(&initial).unwrap();
        let reader = RangeMapMetadataReader::new(range0.clone(), initial);
        let barrier = CountingBarrier {
            calls: AtomicUsize::new(0),
        };

        range0
            .commit_range_map_metadata(&RangeMapMetadata::new(2, map(2, 20)).unwrap())
            .unwrap();
        let loaded = reader.map_for_fresh_snapshot(&barrier).await.unwrap();

        assert!(loaded.version == 2);
        assert!(barrier.calls.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn map_bump_invisible_to_mid_snapshot_reader() {
        let range0 = MemoryRange0::default();
        let initial = RangeMapMetadata::new(1, map(1, 10)).unwrap();
        range0.commit_range_map_metadata(&initial).unwrap();
        let reader = RangeMapMetadataReader::new(range0.clone(), initial);

        let pinned = reader.map_for_open_snapshot().await;
        range0
            .commit_range_map_metadata(&RangeMapMetadata::new(2, map(2, 20)).unwrap())
            .unwrap();
        let still_pinned = pinned;

        assert!(still_pinned.version == 1);
        assert!(reader.map_for_open_snapshot().await.version == 1);
    }
}
