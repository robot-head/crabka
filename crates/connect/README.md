# crabka-connect

The connector-framework SPI for Crabka: the keystone traits every CDC source
and telemetry sink builds on.

- [`Source<K, V>`](src/source.rs) — pull records out of an external system one
  at a time (`poll`), snapshot the read position (`checkpoint`), and restore it
  on restart (`seek`).
- [`Sink<K, V>`](src/sink.rs) — push records into an external system in batches
  (`put`), durably commit them (`flush`), and optionally gate writes behind a
  transaction for exactly-once delivery (`begin` / `commit` / `abort`).
- [`Converter<T>`](src/convert.rs) — bridge a connector's typed payload `T` to
  and from the raw `Bytes` that travel on the Kafka wire. Ships with
  [`ByteIdentity`] (byte-for-byte passthrough) and [`SchemaConverter`]
  (Confluent schema-registry serdes via `crabka-schema-serde`).
- [`ConnectorRuntime`](src/runtime.rs) — the embeddable, single-process driver
  that owns a `Source` + `Sink` pair and pipes records between them. It polls
  into bounded batches (backpressure), brackets each non-empty commit in the
  sink's transactional gate (lazy `begin` → `put` → `commit`), and advances the
  source checkpoint only after the sink commit is durable. Built
  programmatically — `ConnectorRuntime::new().add_source(…).add_sink(…).run()` —
  and driven through a `ConnectorHandle` (`pause` / `resume` / graceful
  `shutdown`). No Connect worker protocol, no REST: the single-binary edge shape.

The transport traits mirror the Streams runtime I/O traits
(`RecordFetcher` / `RecordProducer`) and the remote-storage `RemoteStorageManager`
SPI; the converter layer mirrors Kafka Connect's `Converter`.

## Connector configuration

Connector implementations can declare a ConfigDef-style schema and derive typed
config extraction. This example uses `serde_json` to build raw JSON config, so
consumers using this style should add `serde_json = "1"` as a direct
dependency.

```rust
use crabka_connect::{
    ConfigDef, ConnectorConfig, EnvSecretResolver, SecretString,
};
use serde_json::json;

#[derive(ConnectorConfig)]
struct PostgresSourceConfig {
    #[config(required)]
    database_url: String,
    #[config(secret)]
    password: SecretString,
    #[config(default = "public")]
    schema: String,
}

async fn build() -> crabka_connect::ConfigResult<PostgresSourceConfig> {
    let raw: serde_json::Map<String, serde_json::Value> = serde_json::Map::from_iter([
        ("database_url".to_string(), json!("postgres://localhost/app")),
        (
            "password".to_string(),
            json!({ "from": "env", "name": "POSTGRES_PASSWORD" }),
        ),
    ]);
    let def: ConfigDef = PostgresSourceConfig::config_def();
    let resolved = def.resolve(raw, &EnvSecretResolver).await?;
    let config: PostgresSourceConfig = ConnectorConfig::from_resolved(&resolved)?;
    Ok(config)
}
```

Secret fields resolve through `SecretResolver` implementations and are redacted
by `Debug` and `Display`.
