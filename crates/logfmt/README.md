# crabka-logfmt

Structured-JSON `tracing` log formatter shared across Crabka services (broker,
gRPC gateway, operator, schema-registry).

It installs a stdout `fmt` layer. The layer writes each event as one JSON object
on one line, in the shape that Google Cloud Logging (GKE) reads:

```json
{"timestamp":"2026-06-13T05:55:09.951788Z","severity":"INFO","target":"crabka_broker::network::dispatch","message":"connection opened","listener":"PLAIN","sasl":false}
```

- The layer maps `severity` from the `tracing` level (`WARN` → `WARNING`,
  `TRACE` → `DEBUG`), so Cloud Logging sets the entry's `LogSeverity`.
- The layer flattens `message` and all event fields to the top level. The
  message then becomes the entry summary, and the fields stay queryable.
- The layer writes no ANSI colour codes. The default `tracing_subscriber` `fmt`
  layer adds colour even when stdout is not a terminal.

## Usage

```rust
use tracing_subscriber::{prelude::*, EnvFilter};

let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
tracing_subscriber::registry()
    .with(crabka_logfmt::layer(filter, std::io::stdout))
    .init();
```
