//! Kafka-backed schema store — reads/writes from the `_schemas` compacted topic.

pub mod reader;
pub mod record;
pub mod topic;
pub mod writer;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    config::RegistryConfig,
    error::SrError,
    format::{self, SchemaType},
    ids::{SchemaId, SchemaVersion},
    kafkastore::record::SchemaReference,
    store::{Registered, StoreState},
};

/// Valid `mode` strings for the global / per-subject mode endpoints.
const VALID_MODES: &[&str] = &["READWRITE", "READONLY", "IMPORT"];

/// Facade over the `_schemas`-backed store: owns the writer, the reader's shared
/// store + offset watch, and a write-serialisation gate. Only the elected
/// primary writes `_schemas`; secondaries forward mutating requests to it (see
/// `rest::forward`), so this facade trusts that any write reaching it is
/// primary-authorised. Every mutating request takes the gate, decides on a clone
/// of the store, produces the record, and waits for the reader to apply it
/// (read-your-writes).
pub struct KafkaStore {
    pub store: Arc<RwLock<StoreState>>,
    applied_rx: watch::Receiver<i64>,
    writer: writer::SchemaWriter,
    write_gate: Mutex<()>,
    schemas_topic: String,
}

pub struct RegisterSchema<'a> {
    pub subject: &'a str,
    pub ty: SchemaType,
    pub schema: &'a str,
    pub references: &'a [SchemaReference],
    pub message_type: Option<&'a str>,
    pub import_id: Option<SchemaId>,
    pub import_version: Option<SchemaVersion>,
}

impl KafkaStore {
    /// Create `_schemas`, start the reader, build the writer.
    ///
    /// NOTE: does not block for full initial replay before serving. A
    /// freshly-promoted primary whose reader has not yet drained the prior
    /// primary's last records could briefly mis-assign ids/versions until it
    /// catches up — catch-up-before-write on promotion (and fencing the brief
    /// rebalance multi-writer window) is a documented deferred limitation of the
    /// HA support. Tests start from a fresh (empty) `_schemas`, so there is nothing
    /// to replay.
    #[tracing::instrument(
        level = "info",
        name = "kafkastore.start",
        skip_all,
        fields(topic = %cfg.schemas_topic, bootstrap = %cfg.bootstrap),
        err
    )]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn start(
        cfg: &RegistryConfig,
        cancel: CancellationToken,
    ) -> anyhow::Result<Arc<Self>> {
        // SR-to-broker Kafka-client security (SASL/TLS). `None` = plaintext, the
        // pre-security default; threaded identically into every client below.
        let security = cfg.security.client.clone();
        let topic_id = topic::ensure_schemas_topic(cfg, security.clone()).await?;
        let r = reader::spawn(cfg, topic_id, security.clone(), cancel);
        let writer = writer::SchemaWriter::start(cfg, security).await?;
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
    /// `READONLY` mode, rejected. Otherwise the path is dedup → compat →
    /// assign → persist → read-your-writes.
    #[tracing::instrument(
        level = "info",
        name = "kafkastore.register",
        skip_all,
        fields(
            subject = %req.subject,
            schema_type = ?req.ty,
            refs = req.references.len(),
            mode = tracing::field::Empty,
            id = tracing::field::Empty,
            version = tracing::field::Empty,
            dedup = tracing::field::Empty,
        ),
        err
    )]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn register(&self, req: RegisterSchema<'_>) -> Result<Registered, SrError> {
        let _gate = self.write_gate.lock().await;
        let RegisterSchema {
            subject,
            ty,
            schema,
            references,
            message_type,
            import_id,
            import_version,
        } = req;
        let mode = self.effective_mode(subject);
        tracing::Span::current().record("mode", mode.as_str());
        if mode == "READONLY" {
            return Err(SrError::OperationNotPermitted(subject.to_string()));
        }
        // Resolve the candidate's references once (propagating ReferenceNotFound
        // / 42201 if any named version is absent) and reuse the closure for
        // normalisation, dedup, compat, and the parse below.
        let resolved = self.store.read().resolve_closure(references)?;
        // Normalise before dedup check: `syntax = "proto3"; message ...`
        // needs to deduplicate against the same proto in normalised form.
        let schema = &format::normalized_storage_form(ty, schema, &resolved)?;
        if mode == "IMPORT" {
            let (Some(id), Some(version)) = (import_id, import_version) else {
                return Err(SrError::InvalidSchema(
                    "IMPORT mode requires explicit id and version".into(),
                ));
            };
            format::parse(ty, schema, &resolved)?; // 42201 if unparseable
            let (key, value) = record::encode_schema_with_message_type(
                subject,
                version,
                id,
                ty,
                schema,
                references,
                message_type,
            );
            let offset = self
                .writer
                .produce(key, value)
                .await
                .map_err(|e| SrError::Backend(e.to_string()))?;
            self.await_applied(offset).await;
            let span = tracing::Span::current();
            span.record("id", id.0);
            span.record("version", version.0);
            span.record("dedup", false);
            return Ok(Registered { id, version });
        }
        if let Some(existing) = self.store.read().find_under_subject(
            subject,
            ty,
            schema,
            references,
            message_type,
            false,
        ) {
            let span = tracing::Span::current();
            span.record("id", existing.id.0);
            span.record("version", existing.version.0);
            span.record("dedup", true);
            return Ok(existing);
        }
        // Enforce compatibility against existing versions per the subject's
        // effective level. First version / NONE => no-op. Incompatible =>
        // SrError::Incompatible (409); nothing is persisted. Ref-aware: both the
        // candidate's and each existing version's references are resolved.
        crate::compat::check_registration(&self.store.read(), subject, ty, schema, &resolved)?;
        // Genuinely new under this subject: decide id/version on a throwaway
        // clone (the reader is the sole mutator of the live store).
        let reg = {
            let mut probe = self.store.read().clone();
            probe.register(subject, ty, schema, references, message_type)?
        };
        let (key, value) = record::encode_schema_with_message_type(
            subject,
            reg.version,
            reg.id,
            ty,
            schema,
            references,
            message_type,
        );
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        let span = tracing::Span::current();
        span.record("id", reg.id.0);
        span.record("version", reg.version.0);
        span.record("dedup", false);
        Ok(reg)
    }

    /// Persist + apply a global compatibility level (stored, not enforced in
    /// current API surface).
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn set_global_compat(&self, level: String) -> Result<(), SrError> {
        self.set_compat(None, level).await
    }

    /// Persist + apply a per-subject compatibility level.
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn set_subject_compat(&self, subject: &str, level: String) -> Result<(), SrError> {
        self.set_compat(Some(subject), level).await
    }

    /// Remove the per-subject compat override and revert to global. Returns the
    /// deleted level string (e.g. `"BACKWARD"`) or `None` if no per-subject
    /// override was set.
    #[tracing::instrument(level = "info", name = "kafkastore.delete_subject_compat", skip_all, fields(subject = %subject), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn delete_subject_compat(&self, subject: &str) -> Result<Option<String>, SrError> {
        let _gate = self.write_gate.lock().await;
        let current = self
            .store
            .read()
            .subject_compat(subject)
            .map(str::to_string);
        let Some(level) = current else {
            return Ok(None);
        };
        let key = record::config_key(Some(subject));
        let offset = self
            .writer
            .produce_tombstone(key)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(Some(level))
    }

    #[tracing::instrument(level = "info", name = "kafkastore.set_compat", skip_all, fields(subject = subject.unwrap_or("global"), level = %level, mode = tracing::field::Empty), err)]
    async fn set_compat(&self, subject: Option<&str>, level: String) -> Result<(), SrError> {
        let _gate = self.write_gate.lock().await;
        let mode = match subject {
            Some(s) => self.store.read().effective_mode(s).to_string(),
            None => self.store.read().global_mode().to_string(),
        };
        tracing::Span::current().record("mode", mode.as_str());
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
    #[tracing::instrument(level = "info", name = "kafkastore.soft_delete_version", skip_all, fields(subject = %subject, version = version.0), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn soft_delete_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<SchemaVersion, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        let found = {
            let s = self.store.read();
            if s.versions(subject, true).is_none() {
                return Err(SrError::SubjectNotFound(subject.to_string()));
            }
            s.version(subject, Some(version), true)
                .ok_or(SrError::VersionNotFound)?
        };
        // Reference-protection: a live referrer blocks deletion (42206).
        if !self
            .store
            .read()
            .referenced_by(subject, version, false)
            .is_empty()
        {
            return Err(SrError::ReferencedByOthers(format!("{subject}:{version}")));
        }
        let (key, value) = record::encode_schema_deleted_with_message_type(
            subject,
            found.version,
            found.id,
            found.ty,
            &found.schema,
            &found.references,
            found.message_type.as_deref(),
        );
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(found.version)
    }

    /// Permanently delete a version (tombstone). Requires a prior soft delete.
    #[tracing::instrument(level = "info", name = "kafkastore.permanent_delete_version", skip_all, fields(subject = %subject, version), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn permanent_delete_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<SchemaVersion, SrError> {
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
                return Err(SrError::VersionNotSoftDeleted(
                    subject.to_string(),
                    version.0,
                ));
            }
        }
        // Reference-protection: a live referrer blocks deletion (42206).
        if !self
            .store
            .read()
            .referenced_by(subject, version, false)
            .is_empty()
        {
            return Err(SrError::ReferencedByOthers(format!("{subject}:{version}")));
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
    #[tracing::instrument(level = "info", name = "kafkastore.soft_delete_subject", skip_all, fields(subject = %subject), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn soft_delete_subject(&self, subject: &str) -> Result<Vec<SchemaVersion>, SrError> {
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
        // Reference-protection: any live referrer of a live version blocks (42206).
        for v in &versions {
            if !self
                .store
                .read()
                .referenced_by(subject, *v, false)
                .is_empty()
            {
                return Err(SrError::ReferencedByOthers(format!("{subject}:{v}")));
            }
        }
        let max = versions.iter().copied().max().unwrap_or(SchemaVersion(0));
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
    #[tracing::instrument(level = "info", name = "kafkastore.permanent_delete_subject", skip_all, fields(subject = %subject), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn permanent_delete_subject(
        &self,
        subject: &str,
    ) -> Result<Vec<SchemaVersion>, SrError> {
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
        // Reference-protection: any live referrer of any version blocks (42206).
        for v in &all_versions {
            if !self
                .store
                .read()
                .referenced_by(subject, *v, false)
                .is_empty()
            {
                return Err(SrError::ReferencedByOthers(format!("{subject}:{v}")));
            }
        }
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
    #[tracing::instrument(level = "info", name = "kafkastore.set_global_mode", skip_all, fields(mode = %mode), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
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
    #[tracing::instrument(level = "info", name = "kafkastore.set_subject_mode", skip_all, fields(subject = %subject, mode = %mode), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
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
    #[tracing::instrument(level = "info", name = "kafkastore.clear_subject_mode", skip_all, fields(subject = %subject), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
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
