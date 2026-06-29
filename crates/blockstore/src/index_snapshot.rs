use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt as _;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};

use crate::error::{BlockStoreError, Result};

pub const DEFAULT_INDEX_SNAPSHOT_RETAIN: usize = 8;

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

pub async fn put_index_snapshot(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    bytes: Vec<u8>,
) -> Result<String> {
    let snapshot_key = next_snapshot_key(key)?;
    store
        .put(&Path::from(snapshot_key.clone()), PutPayload::from(bytes))
        .await?;
    prune_old_index_snapshots(store, key, DEFAULT_INDEX_SNAPSHOT_RETAIN).await?;
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

pub async fn list_index_snapshot_objects(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Vec<ObjectMeta>> {
    let prefix = Path::from(index_snapshot_prefix_for_key(key));
    let mut stream = store.list(Some(&prefix));
    let mut objects = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        if meta.location.as_ref().ends_with(".json") {
            objects.push(meta);
        }
    }
    objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
    Ok(objects)
}

async fn prune_old_index_snapshots(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    retain: usize,
) -> Result<()> {
    let retain = retain.max(1);
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
