# crabka-logfmt

Structured-JSON `tracing` log formatter shared across Crabka services (broker,
gRPC gateway, operator, schema-registry).

It installs a stdout `fmt` layer whose events are emitted as one JSON object per
line, shaped for Google Cloud Logging (GKE):

```json
{"timestamp":"2026-06-13T05:55:09.951788Z","severity":"INFO","target":"crabka_broker::network::dispatch","message":"connection opened","listener":"PLAIN","sasl":false}
```

- `severity` is mapped from the `tracing` level (`WARN` → `WARNING`, `TRACE` →
  `DEBUG`) so Cloud Logging sets the entry's `LogSeverity`.
- `message` and all event fields are flattened to the top level so the message
  becomes the entry summary and fields stay queryable.
- No ANSI colour codes — unlike the default `tracing_subscriber` `fmt` layer,
  which colourises even when stdout is not a terminal.

## Usage

```rust
use tracing_subscriber::{prelude::*, EnvFilter};

let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
tracing_subscriber::registry()
    .with(crabka_logfmt::layer(filter, std::io::stdout))
    .init();
```
