//! Checkpoint object store abstraction and test implementations.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use crabka_object_store::{ObjectOps, ObjectStoreError};
use object_store::path::Path;
use tokio::sync::RwLock;

use crate::error::SubstrateError;

/// Object metadata returned by checkpoint-store listings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointObject {
    /// Object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
}

/// Minimal object-store surface needed by checkpoint runtime code.
#[async_trait::async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Put one immutable object.
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), SubstrateError>;
    /// Get one complete object.
    async fn get(&self, key: &str) -> Result<Vec<u8>, SubstrateError>;
    /// List objects whose key starts with `prefix`, in deterministic key order.
    async fn list(&self, prefix: &str) -> Result<Vec<CheckpointObject>, SubstrateError>;
    /// Delete one object. Missing objects are tolerated by implementations.
    async fn delete(&self, key: &str) -> Result<(), SubstrateError>;
}

/// Deterministic in-memory checkpoint store for tests.
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    objects: RwLock<BTreeMap<String, Vec<u8>>>,
}

impl InMemoryCheckpointStore {
    /// Build a shared in-memory checkpoint store.
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait::async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), SubstrateError> {
        self.objects.write().await.insert(key.to_owned(), bytes);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, SubstrateError> {
        self.objects
            .read()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| SubstrateError::Checkpoint(format!("checkpoint object missing: {key}")))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<CheckpointObject>, SubstrateError> {
        let objects = self.objects.read().await;
        Ok(objects
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, bytes)| CheckpointObject {
                key: key.clone(),
                size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            })
            .collect())
    }

    async fn delete(&self, key: &str) -> Result<(), SubstrateError> {
        self.objects.write().await.remove(key);
        Ok(())
    }
}

/// Adapter from the workspace object-store abstraction.
pub struct ObjectOpsCheckpointStore {
    ops: Arc<dyn ObjectOps>,
}

impl ObjectOpsCheckpointStore {
    /// Wrap an existing [`ObjectOps`] handle.
    #[must_use]
    pub fn new(ops: Arc<dyn ObjectOps>) -> Self {
        Self { ops }
    }
}

#[async_trait::async_trait]
impl CheckpointStore for ObjectOpsCheckpointStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), SubstrateError> {
        self.ops
            .put(&Path::from(key), Bytes::from(bytes))
            .await
            .map_err(|error| map_object_error(&error))
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, SubstrateError> {
        self.ops
            .get(&Path::from(key))
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| map_object_error(&error))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<CheckpointObject>, SubstrateError> {
        let mut listed = self
            .ops
            .list(Some(Path::from(prefix)))
            .await
            .map_err(|error| map_object_error(&error))?
            .into_iter()
            .map(|object| CheckpointObject {
                key: object.location.to_string(),
                size: object.size,
            })
            .collect::<Vec<_>>();
        listed.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(listed)
    }

    async fn delete(&self, key: &str) -> Result<(), SubstrateError> {
        match self.ops.delete(&Path::from(key)).await {
            Ok(()) | Err(ObjectStoreError::NotFound(_)) => Ok(()),
            Err(error) => Err(map_object_error(&error)),
        }
    }
}

fn map_object_error(error: &ObjectStoreError) -> SubstrateError {
    SubstrateError::Checkpoint(format!("object store: {error}"))
}
