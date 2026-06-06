//! Kafka-backed schema store — reads/writes from the `_schemas` compacted topic.

pub mod reader;
pub mod record;
pub mod topic;
pub mod writer;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

use crate::config::RegistryConfig;
use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::store::{Registered, StoreState};

/// Valid `mode` strings for the global / per-subject mode endpoints.
const VALID_MODES: &[&str] = &["READWRITE", "READONLY", "IMPORT"];

/// Facade over the `_schemas`-backed store: owns the writer, the reader's shared
/// store + offset watch, and a write-serialisation gate. Single-node always-primary
/// (slice 1): every mutating request takes the gate, decides on a clone of the
/// store, produces the record, and waits for the reader to apply it
/// (read-your-writes).
pub struct KafkaStore {
    pub store: Arc<RwLock<StoreState>>,
    applied_rx: watch::Receiver<i64>,
    writer: writer::SchemaWriter,
    write_gate: Mutex<()>,
    schemas_topic: String,
}

impl KafkaStore {
    /// Create `_schemas`, start the reader, build the writer.
    ///
    /// NOTE (slice 1): does not block for full initial replay before serving.
    /// Tests start from a fresh (empty) `_schemas`, so there is nothing to
    /// replay. Startup against a pre-existing log could briefly mis-assign ids
    /// until the reader catches up; proper high-watermark catch-up is a later
    /// (HA) slice.
    pub async fn start(
        cfg: &RegistryConfig,
        cancel: CancellationToken,
    ) -> anyhow::Result<Arc<Self>> {
        let topic_id = topic::ensure_schemas_topic(cfg).await?;
        let r = reader::spawn(cfg, topic_id, cancel);
        let writer = writer::SchemaWriter::start(cfg).await?;
        Ok(Arc::new(Self {
            store: r.store,
            applied_rx: r.applied_rx,
            writer,
            write_gate: Mutex::new(()),
            schemas_topic: cfg.schemas_topic.clone(),
        }))
    }

    /// The effective mode for `subject` (subject override else global else
    /// `READWRITE`).
    fn effective_mode(&self, subject: &str) -> String {
        self.store.read().effective_mode(subject).to_string()
    }

    /// `Err(OperationNotPermitted)` if the subject's effective mode is
    /// `READONLY`; `Ok(())` otherwise.
    fn ensure_writable(&self, subject: &str) -> Result<(), SrError> {
        if self.effective_mode(subject) == "READONLY" {
            Err(SrError::OperationNotPermitted(subject.to_string()))
        } else {
            Ok(())
        }
    }

    /// Register a schema. In `IMPORT` mode, persists at the explicit
    /// `import_id`/`import_version` (no id-assignment, no compat check). In
    /// `READONLY` mode, rejected. Otherwise the slice-1/2 path (dedup → compat →
    /// assign → persist → read-your-writes).
    pub async fn register(
        &self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
        import_id: Option<i32>,
        import_version: Option<i32>,
    ) -> Result<Registered, SrError> {
        let _gate = self.write_gate.lock().await;
        let mode = self.effective_mode(subject);
        if mode == "READONLY" {
            return Err(SrError::OperationNotPermitted(subject.to_string()));
        }
        // Normalise before dedup check: `syntax = "proto3"; message ...`
        // needs to deduplicate against the same proto in normalised form.
        let schema = &format::normalized_storage_form(ty, schema)?;
        if mode == "IMPORT" {
            let (Some(id), Some(version)) = (import_id, import_version) else {
                return Err(SrError::InvalidSchema(
                    "IMPORT mode requires explicit id and version".into(),
                ));
            };
            format::parse(ty, schema)?; // 42201 if unparseable
            let (key, value) = record::encode_schema(subject, version, id, ty, schema);
            let offset = self
                .writer
                .produce(key, value)
                .await
                .map_err(|e| SrError::Backend(e.to_string()))?;
            self.await_applied(offset).await;
            return Ok(Registered { id, version });
        }
        if let Some(existing) = self
            .store
            .read()
            .find_under_subject(subject, ty, schema, false)
        {
            return Ok(existing);
        }
        // Slice 2: enforce compatibility against existing versions per the
        // subject's effective level. First version / NONE => no-op. Incompatible
        // => SrError::Incompatible (409); nothing is persisted.
        crate::compat::check_registration(&self.store.read(), subject, ty, schema)?;
        // Genuinely new under this subject: decide id/version on a throwaway
        // clone (the reader is the sole mutator of the live store).
        let reg = {
            let mut probe = self.store.read().clone();
            probe.register(subject, ty, schema)?
        };
        let (key, value) = record::encode_schema(subject, reg.version, reg.id, ty, schema);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(reg)
    }

    /// Persist + apply a global compatibility level (stored, not enforced in
    /// slice 1).
    pub async fn set_global_compat(&self, level: String) -> Result<(), SrError> {
        self.set_compat(None, level).await
    }

    /// Persist + apply a per-subject compatibility level.
    pub async fn set_subject_compat(&self, subject: &str, level: String) -> Result<(), SrError> {
        self.set_compat(Some(subject), level).await
    }

    async fn set_compat(&self, subject: Option<&str>, level: String) -> Result<(), SrError> {
        let _gate = self.write_gate.lock().await;
        let mode = match subject {
            Some(s) => self.store.read().effective_mode(s).to_string(),
            None => self.store.read().global_mode().to_string(),
        };
        if mode == "READONLY" {
            return Err(SrError::OperationNotPermitted(
                subject.unwrap_or("global").to_string(),
            ));
        }
        let (key, value) = record::encode_config(subject, &level);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(())
    }

    /// Soft-delete a version: re-emit its SCHEMA record with `deleted=true`.
    pub async fn soft_delete_version(&self, subject: &str, version: i32) -> Result<i32, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        let (id, ver, ty, schema) = {
            let s = self.store.read();
            if s.versions(subject, true).is_none() {
                return Err(SrError::SubjectNotFound(subject.to_string()));
            }
            s.version(subject, Some(version), true)
                .ok_or(SrError::VersionNotFound)?
        };
        let (key, value) = record::encode_schema_deleted(subject, ver, id, ty, &schema);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(ver)
    }

    /// Permanently delete a version (tombstone). Requires a prior soft delete.
    pub async fn permanent_delete_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<i32, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        {
            let s = self.store.read();
            if s.versions(subject, true).is_none() {
                return Err(SrError::SubjectNotFound(subject.to_string()));
            }
            if s.version(subject, Some(version), true).is_none() {
                return Err(SrError::VersionNotFound);
            }
            if s.version(subject, Some(version), false).is_some() {
                return Err(SrError::VersionNotSoftDeleted(subject.to_string(), version));
            }
        }
        let key = record::encode_tombstone(subject, version);
        let offset = self
            .writer
            .produce_tombstone(key)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(version)
    }

    /// Soft-delete a subject (`DELETE_SUBJECT` marker). Returns the live versions.
    pub async fn soft_delete_subject(&self, subject: &str) -> Result<Vec<i32>, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        let versions = {
            let s = self.store.read();
            match s.versions(subject, false) {
                Some(v) => v,
                // exists but no live versions => already soft-deleted (cp: 40404);
                // truly absent => not found (40401).
                None if s.versions(subject, true).is_some() => {
                    return Err(SrError::SubjectSoftDeleted(subject.to_string()));
                }
                None => return Err(SrError::SubjectNotFound(subject.to_string())),
            }
        };
        let max = versions.iter().copied().max().unwrap_or(0);
        let (key, value) = record::encode_delete_subject(subject, max);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(versions)
    }

    /// Permanently delete a subject (per-version tombstones). Requires a prior
    /// soft delete (no live versions remain).
    pub async fn permanent_delete_subject(&self, subject: &str) -> Result<Vec<i32>, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        let all_versions = {
            let s = self.store.read();
            let all = s
                .versions(subject, true)
                .ok_or_else(|| SrError::SubjectNotFound(subject.to_string()))?;
            if s.versions(subject, false).is_some() {
                return Err(SrError::SubjectNotSoftDeleted(subject.to_string()));
            }
            all
        };
        let mut last_offset = -1;
        for v in &all_versions {
            let key = record::encode_tombstone(subject, *v);
            last_offset = self
                .writer
                .produce_tombstone(key)
                .await
                .map_err(|e| SrError::Backend(e.to_string()))?;
        }
        if last_offset >= 0 {
            self.await_applied(last_offset).await;
        }
        Ok(all_versions)
    }

    /// Set the global mode. `IMPORT` requires the registry to be empty.
    pub async fn set_global_mode(&self, mode: String) -> Result<(), SrError> {
        if !VALID_MODES.contains(&mode.as_str()) {
            return Err(SrError::InvalidMode(mode));
        }
        let _gate = self.write_gate.lock().await;
        if mode == "IMPORT" && !self.store.read().subjects(true).is_empty() {
            return Err(SrError::OperationNotPermitted("registry not empty".into()));
        }
        let (key, value) = record::encode_mode(None, &mode);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(())
    }

    /// Set a per-subject mode. `IMPORT` requires the subject to have no versions.
    pub async fn set_subject_mode(&self, subject: &str, mode: String) -> Result<(), SrError> {
        if !VALID_MODES.contains(&mode.as_str()) {
            return Err(SrError::InvalidMode(mode));
        }
        let _gate = self.write_gate.lock().await;
        if mode == "IMPORT" && self.store.read().versions(subject, true).is_some() {
            return Err(SrError::OperationNotPermitted(subject.to_string()));
        }
        let (key, value) = record::encode_mode(Some(subject), &mode);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(())
    }

    /// Clear a per-subject mode override (MODE tombstone).
    pub async fn clear_subject_mode(&self, subject: &str) -> Result<(), SrError> {
        let _gate = self.write_gate.lock().await;
        let key = record::mode_key(Some(subject));
        let offset = self
            .writer
            .produce_tombstone(key)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(())
    }

    /// Block until the reader has applied the record at `offset`.
    async fn await_applied(&self, offset: i64) {
        let mut rx = self.applied_rx.clone();
        while *rx.borrow() < offset {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    /// Return the name of the backing `_schemas` topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.schemas_topic
    }
}
