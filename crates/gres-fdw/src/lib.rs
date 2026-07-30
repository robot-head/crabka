//! `PostgreSQL` foreign-data wrapper exposing Crabka Kafka topics as SQL tables.
use std::sync::Arc;

use crabka_client_admin::AdminClient;
use crabka_pgcatalog::{Column, ForeignServer, Table, UserMapping};
use crabka_pgexec::{
    ExecError,
    clock::EvalCtx,
    foreign::{ForeignScanner, ImportFilter, ImportedTable, ScanBounds},
};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_schema_serde::{CacheConfig, RegistryClient, SchemaCache};

mod config;
pub mod decode;
mod error;
pub mod provider;
mod scan;
pub mod source;
pub mod types;

pub use config::{ConnProfile, ServerProfile, resolve, resolve_server};
pub use crabka_schema_serde::SchemaFetchRetryPolicy;
pub use decode::{DecodedValue, Wire, decode_value};
pub use error::KafkaFdwError;
pub use source::{FetchPlan, RawRecord, plan_fetch, scan_topic, scan_topic_with_dns_timeout};
pub use types::{
    avro_schema_to_columns, json_schema_to_columns, project, protobuf_message_to_columns,
};

/// The Kafka foreign-data wrapper.
///
/// The optional default bootstrap is process configuration, not catalog state:
/// substrate-mode computes can make their own tenant cluster the default target
/// while standalone/local computes keep PostgreSQL-compatible explicit server
/// configuration.
///
/// Registered with the engine via
/// [`crabka_pgexec::SqlEngine::set_foreign_scanner`].
#[derive(Debug, Default)]
pub struct KafkaFdw {
    default_bootstrap: Option<String>,
    broker_dns_timeout: crabka_client_core::ClientDnsTimeout,
    schema_fetch_retry_policy: SchemaFetchRetryPolicy,
}

impl KafkaFdw {
    /// Construct a scanner with an optional default bootstrap address list.
    #[must_use]
    pub fn with_defaults(default_bootstrap: Option<String>) -> Self {
        Self {
            default_bootstrap,
            broker_dns_timeout: crabka_client_core::ClientDnsTimeout::default(),
            schema_fetch_retry_policy: SchemaFetchRetryPolicy::default(),
        }
    }

    /// Override the broker DNS lookup deadline for this scanner.
    #[must_use]
    pub fn with_broker_dns_timeout(
        mut self,
        timeout: crabka_client_core::ClientDnsTimeout,
    ) -> Self {
        self.broker_dns_timeout = timeout;
        self
    }

    /// Return the broker DNS lookup deadline for this scanner.
    #[must_use]
    pub fn broker_dns_timeout(&self) -> crabka_client_core::ClientDnsTimeout {
        self.broker_dns_timeout
    }

    /// Override the retry range for transient schema fetch failures.
    #[must_use]
    pub fn with_schema_fetch_retry_policy(mut self, policy: SchemaFetchRetryPolicy) -> Self {
        self.schema_fetch_retry_policy = policy;
        self
    }

    /// Return the retry range for transient schema fetch failures.
    #[must_use]
    pub fn schema_fetch_retry_policy(&self) -> SchemaFetchRetryPolicy {
        self.schema_fetch_retry_policy
    }

    /// Return the configured default bootstrap, when this scanner has one.
    #[must_use]
    pub fn default_bootstrap(&self) -> Option<&str> {
        self.default_bootstrap.as_deref()
    }
}

/// Map a [`KafkaFdwError`] onto an [`ExecError`]. Both config and runtime
/// failures surface as `0A000` (`Unsupported`) for now — the closest existing
/// variant; a dedicated foreign-table error class can follow if needed.
fn to_exec_err(err: &KafkaFdwError) -> ExecError {
    ExecError::Unsupported(err.to_string())
}

/// Build a [`SchemaCache`] for one scan from the profile's registry URL.
fn build_cache(
    profile: &ConnProfile,
    fetch_retry_policy: SchemaFetchRetryPolicy,
) -> Arc<SchemaCache> {
    SchemaCache::new(
        RegistryClient::new(profile.registry_url.clone()),
        CacheConfig {
            fetch_retry_policy,
            ..CacheConfig::default()
        },
    )
}

impl ForeignScanner for KafkaFdw {
    fn scan(
        &self,
        table: &Table,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        bounds: &ScanBounds,
        _ctx: &EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        // Ensure the rustcrypto TLS provider is the process default before any
        // crabka-client TLS handshake (idempotent).
        provider::install_default_provider();

        let foreign = table.foreign.as_ref().ok_or_else(|| {
            ExecError::Unsupported(format!("table \"{}\" is not a foreign table", table.name))
        })?;

        let profile = config::resolve(server, mapping, &foreign.options, self.default_bootstrap())
            .map_err(|err| to_exec_err(&err))?;
        let cache = build_cache(&profile, self.schema_fetch_retry_policy);

        // Drive the async fetch + decode on the current multi-thread runtime
        // without blocking its worker pool (`block_in_place` moves this task to
        // a blocking thread).
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raws = source::scan_topic_with_dns_timeout(
                    &profile,
                    &profile.topic,
                    bounds,
                    self.broker_dns_timeout,
                )
                .await
                .map_err(|err| to_exec_err(&err))?;
                scan::assemble_rows(table, &raws, &profile, &cache)
                    .await
                    .map_err(|err| to_exec_err(&err))
            })
        })
    }

    fn import_schema(
        &self,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        filter: &ImportFilter,
    ) -> Result<Vec<ImportedTable>, ExecError> {
        // Idempotent: ensure the rustcrypto TLS provider is installed before any
        // crabka-client TLS handshake.
        provider::install_default_provider();

        // Resolve bootstrap + registry URL from the server only — IMPORT has no
        // per-table OPTIONS (no `topic`); it discovers the topics itself.
        let profile = config::resolve_server(server, mapping, self.default_bootstrap())
            .map_err(|err| to_exec_err(&err))?;
        let registry = RegistryClient::new(profile.registry_url.clone());

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Enumerate every topic via the admin metadata RPC (empty topic
                // list = all topics, per Kafka semantics).
                let mut admin = AdminClient::connect_secured_with_dns_timeout(
                    &profile.bootstrap,
                    profile.security.clone(),
                    self.broker_dns_timeout,
                )
                .await
                .map_err(|e| ExecError::Unsupported(format!("import: admin connect: {e}")))?;
                let meta = admin.metadata(&[]).await.map_err(|e| {
                    ExecError::Unsupported(format!("import: list topics metadata: {e}"))
                })?;

                let mut out: Vec<ImportedTable> = Vec::new();
                for entry in meta.topics {
                    // Skip topics the metadata response flagged as errored, and
                    // Kafka's internal topics (e.g. __consumer_offsets) which are
                    // not user data.
                    if entry.error.is_some() || entry.name.starts_with("__") {
                        continue;
                    }
                    // Apply the LIMIT TO / EXCEPT filter on the topic name.
                    if !filter.retains(&entry.name) {
                        continue;
                    }
                    let (value_columns, wire) = value_columns_for_topic(&registry, &entry.name)
                        .await
                        .map_err(ExecError::Unsupported)?;
                    // Persist the detected `value_format` alongside `topic` so a
                    // later scan decodes the value bytes the SAME way the columns
                    // were derived. Without this the scan would default to `raw`
                    // and produce a 1-column bytea row that mismatches the
                    // schema-derived column count.
                    let options = vec![
                        ("topic".to_string(), entry.name.clone()),
                        ("value_format".to_string(), wire_option(wire).to_string()),
                    ];
                    out.push(ImportedTable {
                        name: entry.name,
                        columns: value_columns,
                        options,
                    });
                }
                // Stable ordering so repeated imports / tests are deterministic.
                out.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(out)
            })
        })
    }
}

/// Derive the value columns + wire format for one topic from its Schema
/// Registry `"<topic>-value"` subject.
///
/// Raw-fallback policy: a topic whose `"<topic>-value"` subject is NOT registered
/// (or whose schema fails to parse / yields no columns) is still importable — it
/// gets a single raw `value bytea` column and [`Wire::Raw`]. This makes EVERY
/// topic queryable (matching the scanner's `Wire::Raw` path, which projects to
/// one bytea column), rather than silently skipping un-schematized topics.
async fn value_columns_for_topic(
    registry: &RegistryClient,
    topic: &str,
) -> Result<(Vec<Column>, Wire), String> {
    let subject = format!("{topic}-value");
    match fetch_value_columns(registry, &subject).await {
        Ok(Some((cols, wire))) if !cols.is_empty() => Ok((cols, wire)),
        Ok(_) => Ok((vec![raw_value_column()], Wire::Raw)),
        Err(err) => Err(format!("import: topic {topic}: {err}")),
    }
}

/// The `value_format` OPTIONS string for a detected [`Wire`].
fn wire_option(wire: Wire) -> &'static str {
    match wire {
        Wire::Raw => "raw",
        Wire::Avro => "avro",
        Wire::Json => "json",
        Wire::Protobuf => "protobuf",
    }
}

/// A single raw `value bytea` column — the import raw-fallback shape.
fn raw_value_column() -> Column {
    Column::new("value", ColumnType::Bytea)
}

/// Fetch the latest schema for `subject` and derive columns + the [`Wire`]
/// format the columns were derived from. Returns `None` when the subject is
/// unregistered, the fetch fails, or the schema is unparseable — the caller
/// then applies the raw-fallback. Detection: explicit Schema Registry Protobuf
/// metadata wins; otherwise a schema that parses as an Avro record yields Avro
/// columns + [`Wire::Avro`]; otherwise it is treated as JSON Schema +
/// [`Wire::Json`].
async fn fetch_value_columns(
    registry: &RegistryClient,
    subject: &str,
) -> Result<Option<(Vec<Column>, Wire)>, String> {
    let Ok(latest) = registry.latest(subject).await else {
        return Ok(None);
    };
    let id = latest.id;
    let fetched_schema = registry
        .schema_by_id(id)
        .await
        .map_err(|e| format!("schema registry fetch id {id}: {e}"))?;
    let schema_text = fetched_schema.schema.as_str();

    if matches!(latest.schema_type.as_deref(), Some("PROTOBUF")) {
        let message_type = fetched_schema
            .message_type
            .as_deref()
            .or(latest.message_type.as_deref());
        let references = registry
            .reference_sources(&fetched_schema.references)
            .await
            .map_err(|e| format!("protobuf references for subject {subject}: {e}"))?;
        let descriptor = decode::build_message_descriptor_with_references(
            schema_text,
            &references,
            message_type,
        )
        .map_err(|e| format!("protobuf descriptor for subject {subject}: {e}"))?;
        let cols = types::protobuf_message_to_columns(&descriptor);
        if !cols.is_empty() {
            return Ok(Some((cols, Wire::Protobuf)));
        }
    }

    // Try Avro first: a Confluent Avro subject's schema text parses as an Avro
    // Schema; `avro_schema_to_columns` returns a non-empty list only for a
    // top-level record.
    if let Ok(avro_schema) = apache_avro::Schema::parse_str(schema_text) {
        let cols = types::avro_schema_to_columns(&avro_schema);
        if !cols.is_empty() {
            return Ok(Some((cols, Wire::Avro)));
        }
    }

    // Fall back to JSON Schema (Confluent JSON subjects store a JSON Schema
    // object with a top-level `properties` map).
    if let Ok(json_schema) = serde_json::from_str::<serde_json::Value>(schema_text) {
        let cols = types::json_schema_to_columns(&json_schema);
        if !cols.is_empty() {
            return Ok(Some((cols, Wire::Json)));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_fetch_retry_policy_reaches_per_scan_cache() {
        let policy = crabka_schema_serde::SchemaFetchRetryPolicy::new(
            crabka_units::millis(37),
            crabka_units::millis(91),
        )
        .unwrap();
        let fdw = KafkaFdw::with_defaults(Some("broker:9092".into()))
            .with_schema_fetch_retry_policy(policy);
        assert_eq!(fdw.schema_fetch_retry_policy(), policy);

        let profile = ConnProfile {
            bootstrap: vec!["broker:9092".to_string()],
            registry_url: "http://registry:8081".to_string(),
            security: None,
            topic: "orders".to_string(),
            value_format: Wire::Raw,
            key_format: Wire::Raw,
        };
        let cache = build_cache(&profile, policy);
        assert_eq!(cache.fetch_retry_policy(), policy);
    }

    #[test]
    fn fdw_carries_typed_broker_dns_timeout() {
        let timeout =
            crabka_client_core::ClientDnsTimeout::new(std::time::Duration::from_millis(37))
                .expect("positive timeout");
        let fdw =
            KafkaFdw::with_defaults(Some("broker:9092".into())).with_broker_dns_timeout(timeout);

        assert_eq!(fdw.default_bootstrap(), Some("broker:9092"));
        assert_eq!(fdw.broker_dns_timeout(), timeout);
    }
}
