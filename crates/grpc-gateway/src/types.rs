//! Protocol-agnostic record types. Every front-end (gRPC now; webhooks
//! later) converts into `GatewayRecord` and consumes `RecordOutcome`, so
//! the core engines never depend on a wire format.

use bytes::Bytes;

/// One record to produce, independent of transport.
#[derive(Debug, Clone)]
pub struct GatewayRecord {
    pub topic: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, Bytes)>,
    /// Explicit partition override; `None` ⇒ producer's partitioner.
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
    /// Present ⇒ the record is deduplicated by this key (EOS path).
    pub idempotency_key: Option<String>,
}

/// Result of producing one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordOutcome {
    pub partition: i32,
    pub offset: i64,
    /// True ⇒ a prior record with the same `idempotency_key` already
    /// existed; this call did not produce anything new.
    pub deduplicated: bool,
}
