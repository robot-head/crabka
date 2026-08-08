# crabka-trace-context

[![crates.io](https://img.shields.io/crates/v/crabka-trace-context.svg)](https://crates.io/crates/crabka-trace-context)

W3C Trace Context propagation helpers shared by Crabka's wire-protocol crates.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

This crate is the single place where a distributed trace crosses a Crabka process boundary. It holds only the propagation primitives. The OTLP exporter, its environment configuration, and the process-wide subscriber are in `crabka-telemetry`, which re-exports this crate as `crabka_telemetry::propagation`. The split lets published crates such as `crabka-pgwire` join a trace. Those crates do not need to link an HTTP server, a CLI parser, and a profiler.

## Features

- **Kafka record headers**. `current_trace_headers` injects the `traceparent` and `tracestate` pair that a record carries. `extract_context` and `set_remote_parent` rebuild that pair.
- **`TraceCarrier`** is a serde-serialisable trace context. It travels inside an existing RPC payload, so a node-to-node call needs no extra frame. Use `apply_to` to make it the parent of a span. Use `link_into` to attach it as an OpenTelemetry link for fan-out and replay paths.
- **`extract_sqlcommenter`** reads the [sqlcommenter](https://google.github.io/sqlcommenter/) `/*traceparent='…'*/` tag that OpenTelemetry-instrumented database drivers append to SQL. It scans genuine comment regions only, so a string literal cannot inject a tag.
- **Ingress validation**. The crate checks a peer-supplied `traceparent` against the W3C format and re-renders it from the parsed span context. A hostile value never reaches a span attribute or a log line.

## Usage

```rust
use crabka_trace_context::{TraceCarrier, extract_sqlcommenter};

let sql = "SELECT 1 /*traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/";
let carrier = extract_sqlcommenter(sql)
    .and_then(|found| TraceCarrier::from_w3c(found.traceparent, found.tracestate).ok())
    .unwrap_or_default();

let span = tracing::info_span!("db.statement");
carrier.apply_to(&span);
```

## Documentation

- [Crabka repository](https://github.com/robot-head/crabka)

## License

Apache-2.0.
