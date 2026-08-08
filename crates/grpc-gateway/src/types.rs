//! Protocol-agnostic record types.
//!
//! Every front-end converts into `GatewayRecord` and consumes `RecordOutcome`,
//! so the core engines never depend on a wire format. gRPC is a front-end now,
//! and webhooks follow later.

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
    /// The raw, already-serialized value. The gateway ignores it when
    /// `body_structured` is `Some`, because the codec then serializes the
    /// structured JSON instead.
    pub value: Bytes,
    /// Present ⇒ a structured JSON value that the codec serializes against the
    /// carried [`SchemaSelector`]. `None` ⇒ the record is the raw `value`.
    pub body_structured: Option<(Bytes, SchemaSelector)>,
    pub headers: Vec<(String, Bytes)>,
    /// Explicit partition override. `None` ⇒ the producer's partitioner.
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
    /// Present ⇒ the EOS path deduplicates the record by this key.
    pub idempotency_key: Option<String>,
}

impl GatewayRecord {
    /// The codec input for this record. It is `Structured` when a structured
    /// body is present, and `Raw(value)` if it is not.
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
    /// True ⇒ an earlier record with the same `idempotency_key` already
    /// existed, and this call produced nothing new.
    pub deduplicated: bool,
}
