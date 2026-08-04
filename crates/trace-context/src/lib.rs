//! W3C Trace Context plumbing shared by Crabka's wire-protocol crates.
//!
//! This crate is the single place where a trace context crosses a Crabka
//! process boundary. It covers the three shapes those boundaries take:
//!
//! - **Kafka record headers** — [`current_trace_headers`] injects, and
//!   [`extract_context`] / [`set_remote_parent`] rebuild, the `traceparent` /
//!   `tracestate` pair carried alongside a record.
//! - **Structured RPC payloads** — [`TraceCarrier`] is a serde-serialisable
//!   field that rides inside an existing request envelope, so a node-to-node
//!   call needs no extra frame.
//! - **SQL text** — [`extract_sqlcommenter`] reads the
//!   [sqlcommenter](https://google.github.io/sqlcommenter/) `/*traceparent='…'*/`
//!   comment that OpenTelemetry-instrumented database drivers already append.
//!
//! It deliberately depends on nothing but `opentelemetry`, `tracing`,
//! `tracing-opentelemetry`, and `serde`. The OTLP exporter, its
//! configuration, and the process-wide subscriber live in `crabka-telemetry`,
//! which is unpublished and pulls in a web server, a CLI parser, and a
//! profiler — none of which belong in a crate that a protocol codec links
//! against. `crabka-telemetry` re-exports this crate as
//! `crabka_telemetry::propagation`.
//!
//! Ingress from an untrusted client is validated: see [`TraceCarrier::from_w3c`]
//! for the exact rules and for why the raw client string is never retained.

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
