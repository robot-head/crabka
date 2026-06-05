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

    /// Register a schema: idempotent if already present under the subject; else
    /// decide id/version on a clone, persist to `_schemas`, wait for the reader
    /// to apply it, and return. Serialised by the write gate.
    pub async fn register(
        &self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
    ) -> Result<Registered, SrError> {
        let _gate = self.write_gate.lock().await;
        // Normalise before dedup check: `syntax = "proto3"; message ...`
        // needs to deduplicate against the same proto in normalised form.
        let schema = &format::normalized_storage_form(ty, schema)?;
        if let Some(existing) = self.store.read().find_under_subject(subject, ty, schema) {
            return Ok(existing);
        }
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
        let (key, value) = record::encode_config(subject, &level);
        let offset = self
            .writer
            .produce(key, value)
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
