//! The audit sink abstraction and an in-memory test sink.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::{
    event::{AuditEvent, AuditEventClass, AuditOutcome},
    ocsf::{ProductInfo, to_ocsf},
};

/// Record-header key carrying the per-broker chain sequence number (decimal ASCII).
pub const HEADER_SEQ: &str = "seq";
/// Record-header key carrying the previous chain head (lowercase hex of 32 bytes).
pub const HEADER_PREV_HASH: &str = "prev_hash";

/// A serialized, sink-ready audit record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRecord {
    pub class: AuditEventClass,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl AuditRecord {
    /// Stamp the hash-chain headers (`seq` decimal, `prev_hash` lowercase hex)
    /// onto this record. Called by the writer, which owns the chain.
    pub fn push_chain_headers(&mut self, seq: u64, prev_head: &[u8; 32]) {
        self.headers
            .push((HEADER_SEQ.to_string(), seq.to_string().into_bytes()));
        self.headers.push((
            HEADER_PREV_HASH.to_string(),
            crate::chain::to_hex(prev_head).into_bytes(),
        ));
    }

    /// Serialize an event to OCSF JSON and attach SIEM-filterable headers.
    #[must_use]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?event.class(), bytes = tracing::field::Empty)
    )]
    pub fn from_event(event: &AuditEvent, product: &ProductInfo) -> Self {
        let class = event.class();
        let value = serde_json::to_vec(&to_ocsf(event, product)).unwrap_or_else(|_| b"{}".to_vec());
        tracing::Span::current().record("bytes", value.len());
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
    #[error("audit spool I/O: {0}")]
    Io(String),
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

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn io_error_displays() {
        let e = AuditError::Io("disk full".to_string());
        check!(format!("{e}").contains("disk full"));
    }

    fn hdr(r: &AuditRecord, k: &str) -> Option<String> {
        r.headers
            .iter()
            .find(|(hk, _)| hk == k)
            .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
    }

    #[test]
    fn from_event_sets_principal_and_status_headers() {
        use crate::{
            event::{AuditEndpoint, AuditEvent, AuditOutcome, AuditPrincipal, LifecycleKind},
            ocsf::ProductInfo,
        };
        let product = ProductInfo {
            vendor_name: "v".into(),
            name: "n".into(),
            version: "0".into(),
        };
        let authn = AuditEvent::Authentication {
            outcome: AuditOutcome::Failure,
            mechanism: "SASL/PLAIN".into(),
            principal: AuditPrincipal {
                name: "alice".into(),
                auth_method: "SaslPlain".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.1".into(),
                port: 1,
            },
            reason: None,
            time_ms: 0,
        };
        let r = AuditRecord::from_event(&authn, &product);
        check!(hdr(&r, "principal").as_deref() == Some("alice"));
        check!(hdr(&r, "status").as_deref() == Some("failure"));
        let admin = AuditEvent::AdminOperation {
            outcome: AuditOutcome::Success,
            principal: AuditPrincipal {
                name: "bob".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.2".into(),
                port: 2,
            },
            operation: "CreateTopics".into(),
            resources: vec![],
            time_ms: 0,
        };
        let r = AuditRecord::from_event(&admin, &product);
        check!(hdr(&r, "principal").as_deref() == Some("bob"));
        check!(hdr(&r, "status").as_deref() == Some("success"));
        let deny = AuditEvent::AuthorizationDenied {
            principal: AuditPrincipal {
                name: "carol".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.3".into(),
                port: 3,
            },
            resource_type: "Topic".into(),
            resource_name: "t".into(),
            operation: "Write".into(),
            time_ms: 0,
        };
        let r = AuditRecord::from_event(&deny, &product);
        check!(hdr(&r, "principal").as_deref() == Some("carol"));
        check!(hdr(&r, "status").as_deref() == Some("denied"));
        let life = AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: 1,
            time_ms: 0,
        };
        let r = AuditRecord::from_event(&life, &product);
        check!(hdr(&r, "principal") == None);
        check!(hdr(&r, "status") == None);
    }
}
