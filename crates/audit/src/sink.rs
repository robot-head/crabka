//! The audit sink abstraction and an in-memory test sink.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::event::{AuditEvent, AuditEventClass, AuditOutcome};
use crate::ocsf::{ProductInfo, to_ocsf};

/// A serialized, sink-ready audit record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub class: AuditEventClass,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl AuditRecord {
    /// Serialize an event to OCSF JSON and attach SIEM-filterable headers.
    #[must_use]
    pub fn from_event(event: &AuditEvent, product: &ProductInfo) -> Self {
        let class = event.class();
        let value = serde_json::to_vec(&to_ocsf(event, product)).unwrap_or_else(|_| b"{}".to_vec());
        let mut headers = vec![(
            "event_class".to_string(),
            class.as_header().as_bytes().to_vec(),
        )];
        if let Some((name, status)) = principal_and_status(event) {
            headers.push(("principal".to_string(), name.into_bytes()));
            headers.push(("status".to_string(), status.as_bytes().to_vec()));
        }
        AuditRecord {
            class,
            value,
            headers,
        }
    }
}

fn principal_and_status(event: &AuditEvent) -> Option<(String, &'static str)> {
    match event {
        AuditEvent::Authentication {
            principal, outcome, ..
        }
        | AuditEvent::AdminOperation {
            principal, outcome, ..
        } => Some((principal.name.clone(), status_str(*outcome))),
        AuditEvent::AuthorizationDenied { principal, .. } => {
            Some((principal.name.clone(), "denied"))
        }
        AuditEvent::Lifecycle { .. } => None,
    }
}

fn status_str(outcome: AuditOutcome) -> &'static str {
    match outcome {
        AuditOutcome::Success => "success",
        AuditOutcome::Failure => "failure",
    }
}

/// Errors a sink may report.
#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    #[error("audit sink: {0}")]
    Sink(String),
    #[error("audit key: {0}")]
    Key(String),
}

/// Destination for serialized audit records.
#[async_trait]
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    async fn write(&self, record: AuditRecord) -> Result<(), AuditError>;
}

/// In-memory sink for tests.
#[derive(Debug, Default)]
pub struct MemorySink {
    records: Mutex<Vec<AuditRecord>>,
}

impl MemorySink {
    #[must_use]
    pub fn records(&self) -> Vec<AuditRecord> {
        self.records
            .lock()
            .expect("audit memory sink poisoned")
            .clone()
    }
}

#[async_trait]
impl AuditSink for MemorySink {
    async fn write(&self, record: AuditRecord) -> Result<(), AuditError> {
        self.records
            .lock()
            .expect("audit memory sink poisoned")
            .push(record);
        Ok(())
    }
}
