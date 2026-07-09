//! Protocol-agnostic record types. Every front-end (gRPC now; webhooks
//! later) converts into `GatewayRecord` and consumes `RecordOutcome`, so
//! the core engines never depend on a wire format.

use bytes::Bytes;

use crate::{
    codec::{EncodeBody, SchemaSelector},
    ids::{Offset, PartitionIndex},
};

/// One record to produce, independent of transport.
#[derive(Debug, Clone)]
pub struct GatewayRecord {
    pub topic: String,
    pub key: Option<Bytes>,
    /// The raw / already-serialized value. Ignored when `body_structured` is
    /// `Some` (the codec serializes the structured JSON instead).
    pub value: Bytes,
    /// Present ⇒ a structured (JSON) value the codec serializes against the
    /// carried [`SchemaSelector`]. `None` ⇒ the record is raw `value`.
    pub body_structured: Option<(Bytes, SchemaSelector)>,
    pub headers: Vec<(String, Option<Bytes>)>,
    /// Explicit partition override; `None` ⇒ producer's partitioner.
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
    /// Present ⇒ the record is deduplicated by this key (EOS path).
    pub idempotency_key: Option<String>,
}

impl GatewayRecord {
    /// The codec input for this record: `Structured` when a structured body is
    /// present, else `Raw(value)`.
    #[must_use]
    pub fn encode_body(&self) -> EncodeBody {
        match &self.body_structured {
            Some((json, schema)) => EncodeBody::Structured {
                json: json.clone(),
                schema: schema.clone(),
            },
            None => EncodeBody::Raw(self.value.clone()),
        }
    }
}

/// Result of producing one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordOutcome {
    pub partition: PartitionIndex,
    pub offset: Offset,
    /// True ⇒ a prior record with the same `idempotency_key` already
    /// existed; this call did not produce anything new.
    pub deduplicated: bool,
}
