use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crabka_units::{ByteSize, mebibytes};
use futures::StreamExt as _;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use refined_type::rule::GreaterUsize;
use tracing::instrument;

use crate::error::{BlockStoreError, Result};

pub const DEFAULT_INDEX_SNAPSHOT_MAX: ByteSize = mebibytes(256);
pub const DEFAULT_INDEX_SNAPSHOT_RETAIN: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexSnapshotRetain(usize);

impl IndexSnapshotRetain {
    /// Validates an index-snapshot retention count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> std::result::Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> usize {
        self.0
    }
}

impl Default for IndexSnapshotRetain {
    fn default() -> Self {
        Self::new(DEFAULT_INDEX_SNAPSHOT_RETAIN)
            .expect("default index snapshot retention is positive")
    }
}

impl fmt::Display for IndexSnapshotRetain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for IndexSnapshotRetain {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

static SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn index_snapshot_prefix_for_key(key: &str) -> String {
    let key = key.trim_matches('/');
    // Use a sibling prefix, not "{key}/snapshots": object stores allow that
    // shape, but filesystem-backed S3 services may already map the legacy key
    // to a directory containing retained physical object parts.
    if let Some(stem) = key.strip_suffix(".json") {
        format!("{stem}/snapshots")
    } else {
        format!("{key}.snapshots")
    }
}

fn next_snapshot_key(key: &str) -> Result<String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            BlockStoreError::InvalidBlock(format!("system clock before epoch: {err}"))
        })?;
    let counter = SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(format!(
        "{}/{:020}-{:016}.json",
        index_snapshot_prefix_for_key(key),
        elapsed.as_nanos(),
        counter
    ))
}

#[instrument(skip_all, fields(key = %key, len = bytes.len()), err)]
pub async fn put_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    bytes: Vec<u8>,
    retain: IndexSnapshotRetain,
) -> Result<String> {
    let snapshot_key = next_snapshot_key(key)?;
    store
        .put(&Path::from(snapshot_key.clone()), PutPayload::from(bytes))
        .await?;
    prune_old_index_snapshots(store, key, retain.into_value()).await?;
    Ok(snapshot_key)
}

pub async fn latest_index_snapshot_path(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Option<Path>> {
    Ok(list_index_snapshot_objects(store, key)
        .await?
        .pop()
        .map(|meta| meta.location))
}

#[instrument(level = "debug", skip_all, fields(key = %key), err)]
pub async fn list_index_snapshot_objects(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Vec<ObjectMeta>> {
    let prefix = Path::from(index_snapshot_prefix_for_key(key));
    let mut stream = store.list(Some(&prefix));
    let mut objects = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        if std::path::Path::new(meta.location.as_ref())
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            objects.push(meta);
        }
    }
    objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
    Ok(objects)
}

#[instrument(level = "debug", skip_all, fields(key = %key, retain), err)]
async fn prune_old_index_snapshots(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    retain: usize,
) -> Result<()> {
    let objects = list_index_snapshot_objects(store, key).await?;
    let stale = objects.len().saturating_sub(retain);
    for meta in objects.into_iter().take(stale) {
        match store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crabka_units::{convert::ByteSizeExt as _, mebibytes};

    use super::{DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain};

    #[test]
    fn index_snapshot_settings_preserve_defaults_and_validate_input() {
        assert_eq!(DEFAULT_INDEX_SNAPSHOT_MAX.bytes_u64(), 256 * 1024 * 1024);
        assert_eq!(IndexSnapshotRetain::default().into_value(), 8);
        assert_eq!(DEFAULT_INDEX_SNAPSHOT_MAX, mebibytes(256));
        assert_eq!(
            "1".parse::<IndexSnapshotRetain>()
                .expect("one retained snapshot is valid")
                .into_value(),
            1
        );

        for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
            assert!(
                invalid.parse::<IndexSnapshotRetain>().is_err(),
                "{invalid:?} should be rejected"
            );
        }
    }
}
