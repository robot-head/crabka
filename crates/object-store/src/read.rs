//! Capped buffered reads.
//!
//! The module does a `head()` on an object, rejects the object if it exceeds a
//! byte cap, then does a `get()`. It centralises the OOM guard that runs before
//! the code buffers a whole object, such as an index snapshot, into memory.

use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStoreExt as _, path::Path};

use crate::error::ObjectStoreError;

/// Read a whole object, and reject it if its size exceeds `max_bytes`.
///
/// The function does a `head()` first, so it refuses an oversized object
/// *before* it buffers any bytes. This is the guard against OOM on a corrupt or
/// malicious object.
///
/// # Errors
///
/// - [`ObjectStoreError::TooLarge`] if the object is larger than `max_bytes`.
/// - [`ObjectStoreError::NotFound`] if the object does not exist.
/// - [`ObjectStoreError::Backend`] for any other backend failure.
pub async fn read_capped(
    store: &Arc<dyn object_store::ObjectStore>,
    key: &Path,
    max_bytes: u64,
) -> Result<Bytes, ObjectStoreError> {
    let meta = store.head(key).await?;
    if meta.size > max_bytes {
        return Err(ObjectStoreError::TooLarge {
            key: key.clone(),
            size: meta.size,
            max_bytes,
        });
    }
    Ok(store.get(key).await?.bytes().await?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use object_store::{ObjectStoreExt as _, PutPayload, path::Path};

    use super::*;

    fn store_with(key: &str, bytes: &'static [u8]) -> Arc<dyn object_store::ObjectStore> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let s = store.clone();
        let k = Path::from(key);
        futures::executor::block_on(async move { s.put(&k, PutPayload::from(bytes)).await })
            .unwrap();
        store
    }

    #[tokio::test]
    async fn under_cap_returns_bytes() {
        let store = store_with("k", b"hello");
        let got = read_capped(&store, &Path::from("k"), 1024).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn over_cap_returns_too_large() {
        let store = store_with("k", b"hello world");
        let err = read_capped(&store, &Path::from("k"), 4).await.unwrap_err();
        assert!(matches!(
            err,
            ObjectStoreError::TooLarge {
                size: 11,
                max_bytes: 4,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn missing_object_maps_to_not_found() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let err = read_capped(&store, &Path::from("absent"), 1024)
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound(_)));
    }
}
