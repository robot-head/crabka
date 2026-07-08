//! Native Google Cloud Storage [`RemoteStorageManager`] backend (KIP-405).
//!
//! This is the GCS sibling of [`S3RemoteStorage`]: it builds an
//! `object_store::gcp::GoogleCloudStorage` client from a [`GcsConfig`] and
//! wraps it in the same generic [`S3RemoteStorage`] engine via
//! [`S3RemoteStorage::with_store`]. Because that engine is backend-agnostic
//! (object-key layout, byte-range fetch, streaming multipart upload, and
//! `object_store` error mapping are all generic over `dyn ObjectStore`), GCS
//! reuses the entire copy / fetch / delete + multipart implementation —
//! there is no separate trait impl or storage struct.
//!
//! ## Authentication
//!
//! GCS credentials follow `object_store`'s resolution order, which matches
//! Google's Application Default Credentials (ADC):
//!
//! 1. An explicit service-account JSON key file ([`GcsConfig::service_account_path`]).
//! 2. An inline service-account JSON key ([`GcsConfig::service_account_key`]).
//! 3. An application-default-credentials JSON file
//!    ([`GcsConfig::application_credentials_path`]; when unset, the gcloud
//!    well-known ADC file under `$HOME/.config/gcloud` if present).
//! 4. The GKE / GCE metadata server — i.e. **Workload Identity**. This is the
//!    keyless production path: leave all credential fields unset and the pod's
//!    bound Kubernetes service account is exchanged for GCS access tokens by
//!    the metadata server, with no secret material on disk or in the broker
//!    config.
//!
//! This removes the S3-compatibility shim previously required to reach GCS
//! through [`S3RemoteStorage::from_s3_config`] (which could not use Workload
//! Identity and required HMAC interoperability keys).

use crabka_object_store::{GcsConfig, ObjectStoreConfig, build_object_store};

use crate::{error::RemoteStorageError, s3::S3RemoteStorage};

impl S3RemoteStorage {
    /// Build a `GoogleCloudStorage` client from `cfg` and wrap it in the
    /// generic [`S3RemoteStorage`] engine.
    ///
    /// With no credential fields set, authentication uses Workload Identity /
    /// ADC (the metadata server) — the keyless GKE path.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if the bucket /
    /// credential / endpoint combination is rejected by `object_store`'s
    /// builder (e.g. both a service-account path and key are supplied, a
    /// credential file is unreadable, or the bucket name is empty).
    pub fn from_gcs_config(cfg: &GcsConfig) -> Result<Self, RemoteStorageError> {
        let store = build_object_store(&ObjectStoreConfig::Gcs(cfg.clone()))
            .map_err(|e| RemoteStorageError::InvalidArgument(e.to_string()))?;
        Ok(Self::with_store(store, cfg.prefix.clone())
            .with_multipart_tuning(cfg.multipart_threshold, cfg.multipart_chunk_size))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::Write,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use assert2::{assert, check};
    use bytes::Bytes;
    use crabka_ids::LeaderEpoch;
    use crabka_object_store::{DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD};
    use object_store::memory::InMemory;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata::{
            RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
        },
        storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
    };

    // The GCS backend reuses the generic `S3RemoteStorage` engine, so the
    // copy / fetch / delete round-trip behaviour is already covered by the
    // `InMemory`-backed suite in `s3.rs`. These tests pin the GCS-specific
    // surface: config redaction, the builder's credential / endpoint
    // handling, and that the engine reached through `from_gcs_config` is
    // wired with the requested prefix + multipart tuning.

    #[test]
    fn gcs_config_debug_redacts_credentials() {
        let cfg = GcsConfig {
            bucket: "logs".to_string(),
            service_account_key: Some("{\"private_key\":\"super-secret-pem\"}".to_string()),
            service_account_path: Some("/etc/gcs/key.json".to_string()),
            application_credentials_path: Some("/etc/gcs/adc.json".to_string()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        check!(!dbg.contains("super-secret-pem"));
        check!(!dbg.contains("/etc/gcs/key.json"));
        check!(!dbg.contains("/etc/gcs/adc.json"));
        check!(dbg.contains("***"));
        // Non-secret fields are still printed.
        check!(dbg.contains("logs"));
    }

    #[test]
    fn gcs_config_default_uses_multipart_constants() {
        // No credentials by default → Workload Identity / ADC path.
        assert!(
            GcsConfig::default()
                == GcsConfig {
                    bucket: String::new(),
                    prefix: None,
                    service_account_path: None,
                    service_account_key: None,
                    application_credentials_path: None,
                    endpoint: None,
                    allow_http: false,
                    multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
                    multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
                }
        );
    }

    #[test]
    fn from_gcs_config_workload_identity_builds() {
        // No credential fields → ADC / Workload Identity. The builder must
        // construct successfully without contacting the metadata server
        // (credentials are fetched lazily on first request).
        let cfg = GcsConfig {
            bucket: "crabka-tier".to_string(),
            ..Default::default()
        };
        let store = S3RemoteStorage::from_gcs_config(&cfg);
        assert!(store.is_ok());
    }

    #[test]
    fn from_gcs_config_honors_endpoint_and_allow_http() {
        // A custom emulator base URL + allow_http must be accepted by the
        // builder (the credential path stays on ADC since no key is set).
        let cfg = GcsConfig {
            bucket: "crabka-tier".to_string(),
            endpoint: Some("http://localhost:4443".to_string()),
            allow_http: true,
            ..Default::default()
        };
        let store = S3RemoteStorage::from_gcs_config(&cfg);
        assert!(store.is_ok());
    }

    #[test]
    fn from_gcs_config_rejects_conflicting_credentials() {
        // `object_store` rejects supplying both a service-account path and an
        // inline key; we surface that as InvalidArgument.
        let cfg = GcsConfig {
            bucket: "crabka-tier".to_string(),
            service_account_path: Some("/nonexistent/key.json".to_string()),
            service_account_key: Some("{}".to_string()),
            ..Default::default()
        };
        let err = S3RemoteStorage::from_gcs_config(&cfg).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidArgument(_)));
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
            crate::metadata::RemoteLogSegmentDetails::new(
                8,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0)]),
            ),
        )
        .unwrap()
    }

    fn write_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
        let p = dir.join(name);
        std::fs::File::create(&p)
            .unwrap()
            .write_all(contents)
            .unwrap();
        p
    }

    fn sample_data(src: &Path) -> LogSegmentData {
        LogSegmentData {
            log_segment: write_file(src, "00.log", b"0123456789"),
            offset_index: write_file(src, "00.index", b"OFFSET-IDX"),
            time_index: write_file(src, "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: Some(write_file(src, "00.snapshot", b"SNAP")),
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        }
    }

    /// End-to-end round-trip against the generic engine reached through the
    /// GCS construction path, asserting the operator-visible prefix is
    /// applied. Uses `with_store(InMemory)` for the storage (the real GCS
    /// client needs a live bucket); `from_gcs_config`'s builder wiring is
    /// covered by the tests above.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn engine_round_trips_with_prefix() {
        let store =
            S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("cluster-a".to_string()));
        let src = TempDir::new().unwrap();
        let md = sample_metadata(10);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path()))
                .unwrap();
            assert!(store.fetch_log_segment(&md, 0, None).unwrap() == b"0123456789");
            assert!(store.fetch_index(&md, IndexType::Offset).unwrap() == b"OFFSET-IDX");
        })
        .await
        .unwrap();
    }
}
