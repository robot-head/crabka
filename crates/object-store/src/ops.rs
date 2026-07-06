//! The shared object-store operation surface: an async, mockable [`ObjectOps`]
//! trait and its single concrete implementation [`ObjectStoreClient`] over
//! `object_store`. Consumers route their put/get/delete/multipart calls through
//! this so the operation logic (notably the multipart-threshold branch and the
//! `object_store::Error` -> [`ObjectStoreError`] mapping) lives in one place.

use std::sync::Arc;

use bytes::Bytes;
use object_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStoreExt as _, PutPayload, WriteMultipart, path::Path,
};
use tokio::io::AsyncReadExt as _;

use crate::error::ObjectStoreError;

/// Async object-store operations. `Send + Sync` so it can be shared across tasks.
///
/// Kept dyn-safe and `#[automock]`-able: multipart upload is expressed as
/// [`ObjectOps::put_from_path`] over a filesystem path rather than a generic
/// reader, so the trait mocks cleanly for mutation-testable IO decision logic.
#[cfg_attr(test, mockall::automock)]
#[allow(clippy::ref_option_ref)]
#[async_trait::async_trait]
pub trait ObjectOps: Send + Sync {
    /// Single-PUT an in-memory payload.
    async fn put(&self, key: &Path, bytes: Bytes) -> Result<(), ObjectStoreError>;

    /// Upload a local file, choosing single-PUT below `threshold` bytes and
    /// streaming multipart (in `chunk_size` parts) at or above it.
    async fn put_from_path(
        &self,
        key: &Path,
        src: &std::path::Path,
        threshold: u64,
        chunk_size: usize,
    ) -> Result<(), ObjectStoreError>;

    /// Fetch a whole object.
    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError>;

    /// Fetch a byte range of an object.
    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError>;

    /// Fetch object metadata (size, etag, ...).
    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError>;

    /// List objects under an optional prefix.
    async fn list<'a>(&self, prefix: Option<&'a Path>)
    -> Result<Vec<ObjectMeta>, ObjectStoreError>;

    /// Delete an object.
    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError>;
}

/// The single concrete [`ObjectOps`] implementation, wrapping any
/// `object_store::ObjectStore` handle (e.g. one built by
/// [`build_object_store`](crate::build_object_store), or an
/// `object_store::memory::InMemory` in tests).
#[derive(Clone)]
pub struct ObjectStoreClient {
    inner: Arc<dyn object_store::ObjectStore>,
}

impl ObjectStoreClient {
    /// Wrap an existing object-store handle.
    #[must_use]
    pub fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl ObjectOps for ObjectStoreClient {
    async fn put(&self, key: &Path, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.inner.put(key, PutPayload::from_bytes(bytes)).await?;
        Ok(())
    }

    async fn put_from_path(
        &self,
        key: &Path,
        src: &std::path::Path,
        threshold: u64,
        chunk_size: usize,
    ) -> Result<(), ObjectStoreError> {
        if chunk_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "multipart chunk size must be greater than zero",
            )
            .into());
        }

        let len = tokio::fs::metadata(src).await?.len();
        if len < threshold {
            let bytes = tokio::fs::read(src).await?;
            self.inner.put(key, PutPayload::from(bytes)).await?;
            return Ok(());
        }
        let upload = self.inner.put_multipart(key).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);
        let mut file = tokio::fs::File::open(src).await?;
        let mut buf = vec![0u8; chunk_size];
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            writer.write(&buf[..n]);
        }
        writer.finish().await?;
        Ok(())
    }

    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError> {
        Ok(self.inner.get(key).await?.bytes().await?)
    }

    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError> {
        let opts = GetOptions {
            range: Some(range),
            ..Default::default()
        };
        Ok(self.inner.get_opts(key, opts).await?.bytes().await?)
    }

    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        Ok(self.inner.head(key).await?)
    }

    async fn list<'a>(
        &self,
        prefix: Option<&'a Path>,
    ) -> Result<Vec<ObjectMeta>, ObjectStoreError> {
        use futures::stream::TryStreamExt as _;
        Ok(self.inner.list(prefix).try_collect::<Vec<_>>().await?)
    }

    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use assert2::assert;
    use object_store::{GetRange, path::Path};

    use super::*;

    fn client() -> ObjectStoreClient {
        ObjectStoreClient::new(Arc::new(object_store::memory::InMemory::new()))
    }

    #[tokio::test]
    async fn put_get_round_trips() {
        let c = client();
        let key = Path::from("a/b");
        c.put(&key, bytes::Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = c.get(&key).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn get_range_returns_slice() {
        let c = client();
        let key = Path::from("a/b");
        c.put(&key, bytes::Bytes::from_static(b"hello world"))
            .await
            .unwrap();
        let got = c.get_range(&key, GetRange::Bounded(0..5)).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn get_missing_maps_to_not_found() {
        let c = client();
        let err = c.get(&Path::from("nope")).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn head_and_list_and_delete() {
        let c = client();
        let key = Path::from("p/x");
        c.put(&key, bytes::Bytes::from_static(b"1234"))
            .await
            .unwrap();
        assert!(c.head(&key).await.unwrap().size == 4);
        let listed = c.list(Some(&Path::from("p"))).await.unwrap();
        assert!(listed.iter().any(|m| m.location == key));
        c.delete(&key).await.unwrap();
        assert!(matches!(
            c.get(&key).await.unwrap_err(),
            ObjectStoreError::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn put_from_path_single_put_below_threshold() {
        let c = client();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"tiny").unwrap();
        let key = Path::from("seg/small");
        c.put_from_path(&key, f.path(), 8, 4).await.unwrap();
        assert!(&c.get(&key).await.unwrap()[..] == b"tiny");
    }

    #[tokio::test]
    async fn put_from_path_multipart_above_threshold() {
        let c = client();
        let payload = vec![7u8; 20]; // 20 bytes, threshold 8, chunk 4 -> multipart
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&payload).unwrap();
        let key = Path::from("seg/big");
        c.put_from_path(&key, f.path(), 8, 4).await.unwrap();
        assert!(c.get(&key).await.unwrap()[..] == payload[..]);
    }

    /// A delegating spy that counts which upload path `put_from_path` takes.
    /// Both paths produce identical object bytes, so only call counts can pin
    /// the threshold comparison (`len < threshold`) at its boundary.
    #[derive(Debug)]
    struct CountingStore {
        inner: object_store::memory::InMemory,
        puts: std::sync::atomic::AtomicUsize,
        multiparts: std::sync::atomic::AtomicUsize,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                puts: std::sync::atomic::AtomicUsize::new(0),
                multiparts: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.multiparts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Pins the `len < threshold` boundary: one byte under the threshold must
    /// take the single-PUT path (never multipart).
    #[tokio::test]
    async fn put_from_path_at_threshold_minus_one_takes_single_put() {
        let store = Arc::new(CountingStore::new());
        let c = ObjectStoreClient::new(store.clone());
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&vec![1u8; 7]).unwrap(); // len 7, threshold 8
        c.put_from_path(&Path::from("b/under"), f.path(), 8, 4)
            .await
            .unwrap();
        assert!(store.puts.load(std::sync::atomic::Ordering::SeqCst) == 1);
        assert!(store.multiparts.load(std::sync::atomic::Ordering::SeqCst) == 0);
    }

    /// Pins the boundary's other side: exactly the threshold must take the
    /// multipart path (the comparison is strict `<`, not `<=`).
    #[tokio::test]
    async fn put_from_path_at_exact_threshold_takes_multipart() {
        let store = Arc::new(CountingStore::new());
        let c = ObjectStoreClient::new(store.clone());
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&vec![2u8; 8]).unwrap(); // len 8 == threshold 8
        c.put_from_path(&Path::from("b/at"), f.path(), 8, 4)
            .await
            .unwrap();
        assert!(store.puts.load(std::sync::atomic::Ordering::SeqCst) == 0);
        assert!(store.multiparts.load(std::sync::atomic::Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn put_from_path_rejects_zero_chunk_size() {
        let c = client();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"tiny").unwrap();
        let key = Path::from("seg/bad");

        let err = c.put_from_path(&key, f.path(), 8, 0).await.unwrap_err();

        assert!(matches!(
            err,
            ObjectStoreError::Io(e) if e.kind() == std::io::ErrorKind::InvalidInput
        ));
    }

    #[tokio::test]
    async fn mock_seam_compiles_and_returns() {
        let mut mock = MockObjectOps::new();
        mock.expect_get()
            .returning(|_| Ok(bytes::Bytes::from_static(b"x")));
        let got = mock.get(&Path::from("k")).await.unwrap();
        assert!(&got[..] == b"x");
    }
}
