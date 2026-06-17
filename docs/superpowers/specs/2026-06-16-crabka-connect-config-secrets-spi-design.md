# Crabka Connect Config + Secrets SPI - Design

**Date:** 2026-06-16
**Status:** Implemented
**Workstream:** Connect framework, connector authoring SPI
**Predecessor:** `crabka-connect` embeddable connector runtime + lifecycle

## Goal

Add a ConfigDef-style connector configuration SPI to `crabka-connect` so source
and sink implementations can declare, validate, redact, and materialize typed
configuration from JSON. Secret fields must be explicit, resolved through
pluggable providers, and never exposed through normal formatting or logs.

This is the contract future operator `KafkaConnector` resources populate, but
this slice does not add that CRD or a reconciler. The runtime crate gets the
portable SPI; Kubernetes and Vault integration plug into it later.

## Requirements

- Connector authors can declare required fields, defaults, scalar/list/object
  types, and secret fields.
- Raw configuration is accepted as a JSON object so it maps naturally from
  CRDs, files, and command-line tooling.
- Secret fields are resolved from references such as environment variables,
  Kubernetes Secrets, and Vault paths.
- Secret values are wrapped in a type whose `Debug` and `Display` output is
  redacted.
- Validation errors identify the offending field and distinguish missing,
  unknown, type, invalid default, and secret-resolution failures.
- A proc-macro derive keeps the common connector-author path compact while the
  underlying non-macro SPI remains usable directly.

## Architecture

Add a new `config` module to `crabka-connect` and export its public types from
`lib.rs`.

The module owns these core types:

- `ConfigDef`: a declarative schema for connector configuration.
- `ConfigKey`: one field definition: name, value kind, required/default,
  optional allowed values, description, and `secret` metadata.
- `ConfigKind`: supported logical kinds: string, bool, integer, float, duration
  milliseconds, string list, JSON object, and secret.
- `RawConfig`: an incoming `serde_json::Map<String, serde_json::Value>`.
- `ResolvedConfig`: validated values after defaults and secret references have
  been applied.
- `SecretString`: owned secret bytes/text with redacted formatting.
- `SecretRef`: structured references for `env`, `kubernetesSecret`, and `vault`.
- `SecretResolver`: async trait that resolves a `SecretRef` into a
  `SecretString`.
- `ConnectorConfig`: trait implemented manually or by derive to expose
  `ConfigDef` and construct a typed connector config from `ResolvedConfig`.

The `ConfigDef` API is intentionally small:

```rust
let def = ConfigDef::new("postgres-source")
    .required("database_url", ConfigKind::String)
    .secret("password")
    .default("schema", ConfigKind::String, "public");
```

`ConfigDef::resolve(raw, resolver)` returns `ResolvedConfig`. Connector
constructors can either read fields from `ResolvedConfig` directly or use the
derive-generated `ConnectorConfig::from_resolved`.

## Proc Macro

Add a sibling proc-macro crate, `crates/connect-derive`, and expose it from
`crabka-connect` behind a default `derive` feature:

```rust
pub use crabka_connect_derive::ConnectorConfig;
```

Connector authors write:

```rust
#[derive(ConnectorConfig)]
struct PostgresSourceConfig {
    #[config(required)]
    database_url: String,

    #[config(secret)]
    password: SecretString,

    #[config(default = "public")]
    schema: String,
}
```

The derive emits:

- `impl ConnectorConfig for PostgresSourceConfig`
- a static-style `ConfigDef` builder using field names and attributes
- typed extraction from `ResolvedConfig`
- secret metadata for any field marked `#[config(secret)]`

Supported attributes in this slice:

- `#[config(required)]`
- `#[config(default = "...")]`
- `#[config(secret)]`
- `#[config(name = "...")]` for wire-name overrides

Unsupported shapes fail at compile time with clear diagnostics. The derive only
supports named-field structs. Supported field types are `String`, `bool`, signed
and unsigned integer primitives, floating primitives, `Vec<String>`,
`serde_json::Value`, and `SecretString`.

## Secret References

Secret config fields accept a structured JSON object:

```json
{
  "password": {
    "from": "env",
    "name": "POSTGRES_PASSWORD"
  }
}
```

Reference forms:

- env: `{ "from": "env", "name": "POSTGRES_PASSWORD" }`
- Kubernetes Secret:
  `{ "from": "kubernetesSecret", "name": "pg-auth", "key": "password" }`
- Vault:
  `{ "from": "vault", "path": "secret/data/connect/pg", "key": "password" }`

`crabka-connect` includes an environment-variable resolver because it has no
external service dependency. Kubernetes and Vault resolvers are not implemented
in this slice; their reference variants are part of the portable contract so the
future operator can populate them without changing connector code.

For tests and local development, direct secret literals are accepted only when
the caller enables an explicit `ResolveOptions::allow_literal_secrets` flag.
The default path rejects direct secret strings.

## Data Flow

1. A caller obtains raw connector configuration as JSON.
2. `ConnectorConfig::config_def()` describes the expected fields.
3. `ConfigDef::resolve(raw, resolver)` validates keys, applies defaults, parses
   `SecretRef`s, and calls `SecretResolver` for secret fields.
4. `ConnectorConfig::from_resolved(resolved)` builds the typed config struct.
5. Connector code receives ordinary typed values, except secret fields are
   `SecretString`.

The runtime does not log raw or resolved config. Any diagnostic formatting of
`ResolvedConfig` redacts secret fields by consulting the `ConfigDef` metadata.

## Error Handling

Add config-specific variants to `ConnectError`, or introduce `ConfigError` and
wrap it in `ConnectError::Backend` only at runtime boundaries. The implementation
should prefer a dedicated `ConfigError` in the `config` module because config
validation is useful before a connector runtime exists.

`ConfigError` variants:

- `UnknownKey { key }`
- `MissingRequired { key }`
- `WrongType { key, expected }`
- `InvalidDefault { key, reason }`
- `InvalidSecretRef { key, reason }`
- `SecretResolution { key, source }`
- `UnsupportedFieldType { field, ty }` for derive diagnostics where applicable

Runtime-visible errors must not include resolved secret bytes.

## Testing

Unit tests in `crabka-connect`:

- required field validation
- unknown key rejection
- default application
- type validation for all supported kinds
- secret literal rejection by default
- secret literal acceptance with explicit test/local option
- env secret resolution
- redacted `Debug`/`Display` for `SecretString`
- redacted `Debug` for `ResolvedConfig`
- resolver failure includes field name but not secret value

Macro tests in `crabka-connect-derive`:

- derive emits a correct `ConfigDef`
- derive constructs typed configs with defaults and secrets
- `#[config(name = "...")]` changes the wire name
- unsupported tuple structs and unsupported field types fail to compile

Doctests should include the minimal derived-config example so connector authors
see the expected path from raw JSON to typed config.

## Non-Goals

- No `KafkaConnector` CRD, schema generation, controller, or pod wiring.
- No concrete kube-rs Secret resolver in this slice.
- No concrete Vault client in this slice.
- No broad validation DSL beyond required/default/secret/name/type metadata.
- No automatic logging integration. Safety comes from redacting data structures
  and avoiding raw config logging in runtime paths.

## Acceptance Criteria

- `crabka-connect` exposes a usable direct config SPI.
- `#[derive(ConnectorConfig)]` works for named structs with the supported field
  types and attributes.
- Secret fields are resolved through `SecretResolver` and redacted in formatting.
- Raw direct secret strings are rejected unless explicitly allowed.
- Tests cover validation, resolution, redaction, and derive output.
- Existing connector runtime tests continue to pass.
