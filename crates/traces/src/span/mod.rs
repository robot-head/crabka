//! Internal span model every push door lowers into before WAL encoding.

use serde::{Deserialize, Serialize};

pub mod batch;
pub mod nested_set;

/// OTLP span kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => Self::Internal,
            2 => Self::Server,
            3 => Self::Client,
            4 => Self::Producer,
            5 => Self::Consumer,
            _ => Self::Unspecified,
        }
    }
}

/// OTLP status code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusCode {
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => Self::Ok,
            2 => Self::Error,
            _ => Self::Unset,
        }
    }
}

/// A typed attribute value. Block encoding preserves arrays.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// One attribute key/value pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: AttrValue,
}

/// A span event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub time_unix_nano: i64,
    pub name: String,
    pub attrs: Vec<KeyValue>,
}

/// A linked span reference.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attrs: Vec<KeyValue>,
}

/// One internal span. The WAL carries one record per span.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: SpanKind,
    pub start_ns: i64,
    pub duration_ns: i64,
    pub status: StatusCode,
    pub status_message: String,
    pub resource_attrs: Vec<KeyValue>,
    pub span_attrs: Vec<KeyValue>,
    pub events: Vec<EventRecord>,
    pub links: Vec<LinkRecord>,
    pub instrumentation_scope: String,
    pub instrumentation_version: String,
}

impl Span {
    /// Root spans have no raw semantic parent span id.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn span(parent: Option<[u8; 8]>) -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: parent,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: vec![KeyValue {
                key: "http.status_code".into(),
                value: AttrValue::Int(200),
            }],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "tracer".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[test]
    fn root_detection() {
        assert2::assert!(span(None).is_root());
        assert2::assert!(!span(Some([3; 8])).is_root());
    }

    #[test]
    fn kind_round_trips_i32() {
        for kind in [
            SpanKind::Unspecified,
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ] {
            assert2::assert!(SpanKind::from_i32(kind.as_i32()) == kind);
        }
    }

    #[test]
    fn status_round_trips_i32() {
        for status in [StatusCode::Unset, StatusCode::Ok, StatusCode::Error] {
            assert2::assert!(StatusCode::from_i32(status.as_i32()) == status);
        }
    }
}
