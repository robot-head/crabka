//! W3C Trace Context propagation shared by Crabka's wire-protocol crates.
//!
//! This crate is the single place where a trace context crosses a Crabka
//! process boundary. It covers the three shapes those boundaries take:
//!
//! - **Kafka record headers**: [`current_trace_headers`] injects the
//!   `traceparent` and `tracestate` pair that travels with a record.
//!   [`extract_context`] and [`set_remote_parent`] rebuild that pair.
//! - **Structured RPC payloads**: [`TraceCarrier`] is a serde-serialisable
//!   field inside an existing request envelope, so a node-to-node call needs
//!   no extra frame.
//! - **SQL text**: [`extract_sqlcommenter`] reads the
//!   [sqlcommenter](https://google.github.io/sqlcommenter/) `/*traceparent='…'*/`
//!   comment that OpenTelemetry-instrumented database drivers already append.
//!
//! This crate deliberately depends on nothing but `opentelemetry`, `tracing`,
//! `tracing-opentelemetry`, and `serde`. The OTLP exporter, its configuration,
//! and the process-wide subscriber live in `crabka-telemetry`. That crate is
//! unpublished, and it pulls in a web server, a CLI parser, and a profiler.
//! None of those belong in a crate that a protocol codec links against.
//! `crabka-telemetry` re-exports this crate as
//! `crabka_telemetry::propagation`.
//!
//! This crate validates ingress from an untrusted client. See
//! [`TraceCarrier::from_w3c`] for the exact rules, and for the reason it never
//! keeps the raw client string.

#![forbid(unsafe_code)]

mod carrier;
mod propagation;
mod sqlcommenter;

pub use self::{
    carrier::{TraceCarrier, TraceContextError},
    propagation::{
        TRACEPARENT, TRACESTATE, current_trace_headers, extract_context, set_remote_parent,
    },
    sqlcommenter::{SqlCommenterTrace, extract_sqlcommenter},
};
