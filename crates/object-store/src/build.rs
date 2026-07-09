//! Config -> `object_store::ObjectStore` handle construction.

use std::sync::Arc;

use object_store::{ClientOptions, ObjectStore, path::Path, prefix::PrefixStore};

use crate::{
    config::{GcsConfig, ObjectStoreConfig, S3Config},
    error::ObjectStoreError,
};

/// Build an `object_store` handle for `cfg`.
///
/// The builder wiring (credential chains, endpoints, `allow_http`) is identical
/// to the per-crate constructors it replaces.
///
/// # Errors
///
/// Returns [`ObjectStoreError::InvalidConfig`] if the backend builder rejects the
/// bucket / region / endpoint / credential combination.
pub fn build_object_store(
    cfg: &ObjectStoreConfig,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    match cfg {
        ObjectStoreConfig::S3(s3) => build_s3(s3),
        ObjectStoreConfig::Gcs(gcs) => build_gcs(gcs),
        ObjectStoreConfig::Local { root } => {
            let store = object_store::local::LocalFileSystem::new_with_prefix(root)
                .map_err(|e| ObjectStoreError::InvalidConfig(format!("local: {e}")))?;
            Ok(Arc::new(store))
        }
        ObjectStoreConfig::InMemory => Ok(Arc::new(object_store::memory::InMemory::new())),
    }
}

fn build_s3(cfg: &S3Config) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
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
        .map_err(|e| ObjectStoreError::InvalidConfig(format!("S3 builder: {e}")))?;
    Ok(apply_prefix(store, cfg.prefix.as_deref()))
}

fn build_gcs(cfg: &GcsConfig) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    let mut builder =
        object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(&cfg.bucket);
    if let Some(path) = &cfg.service_account_path {
        builder = builder.with_service_account_path(path);
    }
    if let Some(key) = &cfg.service_account_key {
        builder = builder.with_service_account_key(key);
    }
    if let Some(adc) = &cfg.application_credentials_path {
        builder = builder.with_application_credentials(adc);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_base_url(endpoint);
    }
    if cfg.allow_http {
        builder = builder.with_client_options(ClientOptions::new().with_allow_http(true));
    }
    let store = builder
        .build()
        .map_err(|e| ObjectStoreError::InvalidConfig(format!("GCS builder: {e}")))?;
    Ok(apply_prefix(store, cfg.prefix.as_deref()))
}

fn apply_prefix<T>(store: T, prefix: Option<&str>) -> Arc<dyn ObjectStore>
where
    T: ObjectStore + 'static,
{
    let Some(prefix) = prefix.filter(|prefix| !prefix.is_empty()) else {
        return Arc::new(store);
    };
    Arc::new(PrefixStore::new(store, Path::from(prefix)))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use futures::stream::TryStreamExt as _;
    use object_store::ObjectStoreExt;

    use super::*;
    use crate::config::{GcsConfig, ObjectStoreConfig, S3Config};

    async fn list_paths(store: &Arc<dyn ObjectStore>, prefix: Option<&Path>) -> Vec<String> {
        let mut paths: Vec<String> = store
            .list(prefix)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.as_ref().to_owned())
            .collect();
        paths.sort();
        paths
    }

    #[test]
    fn inmemory_builds() {
        assert!(build_object_store(&ObjectStoreConfig::InMemory).is_ok());
    }

    #[tokio::test]
    async fn inmemory_round_trips() {
        let store = build_object_store(&ObjectStoreConfig::InMemory).unwrap();
        let path = object_store::path::Path::from("t/x");
        store
            .put(&path, object_store::PutPayload::from(b"hi".to_vec()))
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert!(&got[..] == b"hi");
    }

    #[tokio::test]
    async fn prefix_wrapper_applies_prefix_once_to_operations_and_lists() {
        let raw = Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;
        let store = apply_prefix(Arc::clone(&raw), Some("cluster-a"));
        let relative_path = Path::from("segments/log");
        let raw_path = Path::from("cluster-a/segments/log");
        let double_prefixed_path = Path::from("cluster-a/cluster-a/segments/log");

        store
            .put(
                &relative_path,
                object_store::PutPayload::from(b"hi".to_vec()),
            )
            .await
            .unwrap();

        let got = raw.get(&raw_path).await.unwrap().bytes().await.unwrap();
        assert!(&got[..] == b"hi");
        assert!(matches!(
            raw.get(&double_prefixed_path).await.unwrap_err(),
            object_store::Error::NotFound { .. }
        ));
        assert!(list_paths(&raw, None).await == vec!["cluster-a/segments/log"]);
        assert!(list_paths(&store, None).await == vec!["segments/log"]);

        store.delete(&relative_path).await.unwrap();
        assert!(list_paths(&raw, None).await.is_empty());
    }

    #[tokio::test]
    async fn absent_prefix_leaves_operations_and_lists_relative() {
        let raw = Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;
        let store = apply_prefix(Arc::clone(&raw), None);
        let relative_path = Path::from("segments/log");
        let prefixed_path = Path::from("cluster-a/segments/log");

        store
            .put(
                &relative_path,
                object_store::PutPayload::from(b"hi".to_vec()),
            )
            .await
            .unwrap();

        let got = raw
            .get(&relative_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(&got[..] == b"hi");
        assert!(matches!(
            raw.get(&prefixed_path).await.unwrap_err(),
            object_store::Error::NotFound { .. }
        ));
        assert!(list_paths(&raw, None).await == vec!["segments/log"]);
        assert!(list_paths(&store, None).await == vec!["segments/log"]);

        store.delete(&relative_path).await.unwrap();
        assert!(list_paths(&raw, None).await.is_empty());
    }

    #[test]
    fn local_builds_against_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ObjectStoreConfig::Local {
            root: dir.path().to_path_buf(),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn s3_builds_with_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://minio:9000".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from crates/remote-storage/src/gcs.rs tests: with every credential
    // field None, the builder selects Workload Identity / ADC and constructs.
    #[test]
    fn gcs_workload_identity_builds() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from gcs.rs tests: a custom endpoint + allow_http builds.
    #[test]
    fn gcs_honors_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            endpoint: Some("http://fake-gcs:4443".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }
}
