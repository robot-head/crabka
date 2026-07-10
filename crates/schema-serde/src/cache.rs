//! Shared, background-refreshed schema cache. Hot-path reads are synchronous;
//! registry I/O happens at pre-warm and on background fetches.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    error::SchemaSerdeError,
    registry::RegistryClient,
    subject::{Role, SchemaKind, SubjectStrategy, TopicNameStrategy},
};

/// A registry writer schema and the `.proto` sources named by its references.
///
/// The root source is stored in [`Self::schema`]. Reference sources are keyed
/// by the exact import name supplied by Schema Registry, never by a filesystem
/// path or URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterSchema {
    pub schema: String,
    pub references: HashMap<String, String>,
}

/// How serialize-side ids are resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterMode {
    /// Register the local schema on pre-warm (Confluent default).
    AutoRegister,
    /// Look up the local schema's id; never register.
    LookupOnly,
    /// Use the latest registered version's id for the subject.
    UseLatest,
}

/// Cache configuration.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub mode: RegisterMode,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            mode: RegisterMode::AutoRegister,
        }
    }
}

/// An interned local schema awaiting pre-warm resolution.
#[derive(Debug, Clone)]
struct Interned {
    subject: String,
    kind: SchemaKind,
    schema: String,
    message_type: Option<String>,
}

#[derive(Default)]
struct Inner {
    /// subject ⇒ resolved id (serialize path).
    subject_id: HashMap<String, u32>,
    /// id ⇒ fully resolved writer schema and reference sources (deserialize path).
    id_writer_schema: HashMap<u32, WriterSchema>,
    /// id ⇒ protobuf message descriptor full name (deserialize path).
    id_message_type: HashMap<u32, String>,
    /// Local schemas to resolve on pre-warm.
    interned: Vec<Interned>,
    /// ids whose fetch is in flight (dedup background fetches).
    fetching: std::collections::HashSet<u32>,
    /// ids known not to exist in the registry or whose schemas are invalid.
    unavailable_schemas: HashMap<u32, String>,
    /// earliest time a transiently failed fetch may be retried.
    retry_after: HashMap<u32, Instant>,
    /// consecutive transient fetch failures, used to increase the next delay.
    retry_attempts: HashMap<u32, u32>,
}

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(10);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Return a capped exponential retry delay with deterministic per-id jitter.
///
/// The deterministic jitter avoids synchronized retries without making tests
/// depend on random timing. The jitter is stable for an id: it is added to the
/// exponential delay while headroom remains, then the delay stays at
/// [`MAX_RETRY_DELAY`]. Each later attempt is at least as long as its
/// predecessor and never exceeds [`MAX_RETRY_DELAY`].
fn retry_delay(id: u32, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(7);
    let multiplier = 1_u32 << exponent;
    let exponential_delay = INITIAL_RETRY_DELAY
        .checked_mul(multiplier)
        .unwrap_or(MAX_RETRY_DELAY)
        .min(MAX_RETRY_DELAY);
    let jitter_percent = id.wrapping_mul(1_103_515_245) % 26;
    let jitter = exponential_delay.mul_f64(f64::from(jitter_percent) / 100.0);
    exponential_delay
        .checked_add(jitter)
        .unwrap_or(MAX_RETRY_DELAY)
        .min(MAX_RETRY_DELAY)
}

/// `Arc`-shared cache wiring serdes to a registry.
pub struct SchemaCache {
    client: RegistryClient,
    config: CacheConfig,
    strategy: Box<dyn SubjectStrategy>,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for SchemaCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaCache")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Process-wide default registry cache, read by the `Default` impls of the
/// format serdes (so a type can declare a default schema serde without
/// threading a cache to the call site — analogous to Confluent serdes reading
/// `schema.registry.url` from config). Set once at application startup.
static DEFAULT_REGISTRY: std::sync::OnceLock<Arc<SchemaCache>> = std::sync::OnceLock::new();

/// Install the process-wide default registry cache (first call wins). Required
/// before constructing any default (`Default::default()`) format serde.
pub fn set_default_registry(cache: Arc<SchemaCache>) {
    let _ = DEFAULT_REGISTRY.set(cache);
}

/// The process-wide default registry cache, if [`set_default_registry`] ran.
#[must_use]
pub fn default_registry() -> Option<Arc<SchemaCache>> {
    DEFAULT_REGISTRY.get().cloned()
}

impl SchemaCache {
    /// Build a cache from a registry client and config, using `TopicNameStrategy`.
    #[must_use]
    pub fn new(client: RegistryClient, config: CacheConfig) -> Arc<Self> {
        Arc::new(Self {
            client,
            config,
            strategy: Box::new(TopicNameStrategy),
            inner: Mutex::new(Inner::default()),
        })
    }

    /// Resolve the subject for `(topic, role)` under the active strategy.
    #[must_use]
    pub fn subject(&self, topic: &str, role: Role) -> String {
        self.strategy.subject(topic, role)
    }

    /// Register a local `(subject, kind, schema)` for pre-warm. Idempotent.
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub fn intern(
        &self,
        subject: &str,
        kind: SchemaKind,
        schema: &str,
        message_type: Option<&str>,
    ) {
        let mut g = self.inner.lock().unwrap();
        if g.interned.iter().any(|i| i.subject == subject) {
            return;
        }
        g.interned.push(Interned {
            subject: subject.to_string(),
            kind,
            schema: schema.to_string(),
            message_type: message_type.map(str::to_string),
        });
    }

    /// Resolve every interned subject's id (register/lookup/latest per mode).
    /// Called once at client/membership start.
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub async fn prewarm(&self) -> Result<(), SchemaSerdeError> {
        let pending: Vec<Interned> = self.inner.lock().unwrap().interned.clone();
        for i in pending {
            let (id, writer_schema, message_type) = match self.config.mode {
                RegisterMode::AutoRegister => {
                    let id = self
                        .client
                        .register(&i.subject, i.kind, &i.schema, i.message_type.as_deref())
                        .await?;
                    (
                        id,
                        WriterSchema {
                            schema: i.schema.clone(),
                            references: HashMap::new(),
                        },
                        i.message_type.clone(),
                    )
                }
                RegisterMode::LookupOnly => {
                    let id = self
                        .client
                        .lookup(&i.subject, i.kind, &i.schema, i.message_type.as_deref())
                        .await?;
                    (
                        id,
                        WriterSchema {
                            schema: i.schema.clone(),
                            references: HashMap::new(),
                        },
                        i.message_type.clone(),
                    )
                }
                RegisterMode::UseLatest => {
                    let latest = self.client.latest(&i.subject).await?;
                    let references = self.client.reference_sources(&latest.references).await?;
                    (
                        latest.id,
                        WriterSchema {
                            schema: latest.schema,
                            references,
                        },
                        latest.message_type,
                    )
                }
            };
            let mut g = self.inner.lock().unwrap();
            g.subject_id.insert(i.subject.clone(), id);
            g.id_writer_schema.insert(id, writer_schema);
            if let Some(message_type) = message_type {
                g.id_message_type.insert(id, message_type);
            }
        }
        Ok(())
    }

    /// Synchronous hot-path read: the id bound to `subject`, or `None` if
    /// pre-warm has not resolved it.
    #[must_use]
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub fn id_for_subject(&self, subject: &str) -> Option<u32> {
        self.inner.lock().unwrap().subject_id.get(subject).copied()
    }

    /// Synchronous hot-path read of a writer schema by id. On a miss, spawns a
    /// background fetch and returns `WriterSchemaPending` (retriable).
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub fn writer_schema(self: &Arc<Self>, id: u32) -> Result<String, SchemaSerdeError> {
        self.writer_schema_with_references(id)
            .map(|writer_schema| writer_schema.schema)
    }

    /// Synchronous hot-path read of a writer schema and all registry-provided
    /// reference sources. A cold read starts one bounded background fetch and
    /// returns [`SchemaSerdeError::WriterSchemaPending`].
    pub fn writer_schema_with_references(
        self: &Arc<Self>,
        id: u32,
    ) -> Result<WriterSchema, SchemaSerdeError> {
        {
            let mut g = self.inner.lock().unwrap();
            if let Some(schema) = g.id_writer_schema.get(&id) {
                return Ok(schema.clone());
            }
            if let Some(reason) = g.unavailable_schemas.get(&id) {
                return Err(SchemaSerdeError::WriterSchemaUnavailable {
                    id,
                    reason: reason.clone(),
                });
            }
            if g.retry_after
                .get(&id)
                .is_some_and(|retry_after| *retry_after > Instant::now())
            {
                return Err(SchemaSerdeError::WriterSchemaPending(id));
            }
            if g.fetching.insert(id) {
                let this = Arc::clone(self);
                tokio::spawn(async move {
                    let fetched = this.resolve_writer_schema(id).await;
                    let mut g = this.inner.lock().unwrap();
                    g.fetching.remove(&id);
                    match fetched {
                        Ok((schema, message_type)) => {
                            if let Some(message_type) = message_type {
                                g.id_message_type.insert(id, message_type);
                            }
                            g.id_writer_schema.insert(id, schema);
                            g.unavailable_schemas.remove(&id);
                            g.retry_after.remove(&id);
                            g.retry_attempts.remove(&id);
                        }
                        Err(error) => {
                            if error.is_transient_registry_failure() {
                                let attempt = *g
                                    .retry_attempts
                                    .entry(id)
                                    .and_modify(|attempt| *attempt = attempt.saturating_add(1))
                                    .or_insert(1);
                                g.retry_after
                                    .insert(id, Instant::now() + retry_delay(id, attempt));
                            } else {
                                g.unavailable_schemas.insert(id, error.to_string());
                                g.retry_after.remove(&id);
                                g.retry_attempts.remove(&id);
                            }
                        }
                    }
                });
            }
        }
        Err(SchemaSerdeError::WriterSchemaPending(id))
    }

    /// Test/seed hook: install an id→schema mapping directly.
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub fn seed_writer_schema(&self, id: u32, schema: impl Into<String>) {
        self.seed_writer_schema_with_references(id, schema, HashMap::new());
    }

    /// Test/seed hook: install an id→root schema mapping and named sources.
    pub fn seed_writer_schema_with_references(
        &self,
        id: u32,
        schema: impl Into<String>,
        references: HashMap<String, String>,
    ) {
        let schema = schema.into();
        let mut g = self.inner.lock().unwrap();
        g.id_writer_schema
            .insert(id, WriterSchema { schema, references });
        g.unavailable_schemas.remove(&id);
        g.retry_after.remove(&id);
        g.retry_attempts.remove(&id);
    }

    /// Synchronous hot-path read of protobuf message metadata for a writer id.
    #[must_use]
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub fn writer_message_type(&self, id: u32) -> Option<String> {
        self.inner.lock().unwrap().id_message_type.get(&id).cloned()
    }

    /// Test/seed hook: install id→protobuf message metadata directly.
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub fn seed_writer_message_type(&self, id: u32, message_type: impl Into<String>) {
        self.inner
            .lock()
            .unwrap()
            .id_message_type
            .insert(id, message_type.into());
    }

    /// Test/seed hook: install a subject→id mapping directly.
    /// # Panics
    /// Panics if a schema previously validated by the registry is missing a definition or dependency required during resolution.
    pub fn seed_subject_id(&self, subject: impl Into<String>, id: u32) {
        self.inner
            .lock()
            .unwrap()
            .subject_id
            .insert(subject.into(), id);
    }

    async fn resolve_writer_schema(
        &self,
        id: u32,
    ) -> Result<(WriterSchema, Option<String>), SchemaSerdeError> {
        let root = self.client.schema_by_id(id).await?;
        let sources = self.client.reference_sources(&root.references).await?;
        Ok((
            WriterSchema {
                schema: root.schema,
                references: sources,
            },
            root.message_type,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::check;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::*;
    use crate::registry::RegistryClient;

    fn cache() -> Arc<SchemaCache> {
        SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default())
    }

    #[test]
    fn intern_is_idempotent_per_subject() {
        let c = cache();
        c.intern("orders-value", SchemaKind::Avro, "a", None);
        c.intern("orders-value", SchemaKind::Avro, "a", None);
        check!(c.inner.lock().unwrap().interned.len() == 1);
    }

    #[tokio::test]
    async fn seeded_reads_are_synchronous() {
        let c = cache();
        c.seed_subject_id("orders-value", 42);
        c.seed_writer_schema(42, "schema-text");
        // Unknown id ⇒ pending (spawns a background fetch; needs a runtime).
        check!(
            (
                c.id_for_subject("orders-value"),
                c.writer_schema(7).is_err(),
                c.writer_schema(42).unwrap(),
            ) == (Some(42), true, "schema-text".to_string())
        );
    }

    #[test]
    fn default_mode_is_auto_register() {
        check!(CacheConfig::default().mode == RegisterMode::AutoRegister);
    }

    #[test]
    fn retry_delay_is_stable_per_id_monotonic_and_capped() {
        let id = 91;
        let first = retry_delay(id, 1);
        let second = retry_delay(id, 2);
        let third = retry_delay(id, 3);
        let before_cap = retry_delay(id, 7);
        let capped = retry_delay(id, 8);
        let far_beyond_cap = retry_delay(id, u32::MAX);

        check!(first <= second);
        check!(second <= third);
        check!(retry_delay(id, 3) == third);
        check!(third <= before_cap);
        check!(before_cap <= capped);
        check!(capped == MAX_RETRY_DELAY);
        check!(far_beyond_cap == capped);
    }

    #[test]
    fn message_type_metadata_is_cached_by_id() {
        let c = cache();
        c.seed_writer_message_type(9, "demo.Order");
        check!(
            (c.writer_message_type(9), c.writer_message_type(10),)
                == (Some("demo.Order".to_string()), None)
        );
    }

    #[tokio::test]
    async fn prewarm_auto_register_resolves_subject_id_and_message_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .and(body_json(serde_json::json!({
                "schema": "syntax = \"proto3\";",
                "schemaType": "PROTOBUF",
                "messageType": "demo.Order"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 50
            })))
            .expect(1)
            .mount(&server)
            .await;

        let c = SchemaCache::new(
            RegistryClient::new(server.uri()),
            CacheConfig {
                mode: RegisterMode::AutoRegister,
            },
        );
        c.intern(
            "orders-value",
            SchemaKind::Protobuf,
            "syntax = \"proto3\";",
            Some("demo.Order"),
        );

        c.prewarm().await.unwrap();

        check!(
            (
                c.id_for_subject("orders-value"),
                c.writer_schema(50).unwrap(),
                c.writer_message_type(50),
            ) == (
                Some(50),
                "syntax = \"proto3\";".to_string(),
                Some("demo.Order".to_string()),
            )
        );
    }

    #[tokio::test]
    async fn prewarm_lookup_only_uses_lookup_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value"))
            .and(body_json(serde_json::json!({
                "schema": r#"{"type":"object"}"#,
                "schemaType": "JSON"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 51,
                "version": 3,
                "schema": r#"{"type":"object"}"#,
                "schemaType": "JSON"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let c = SchemaCache::new(
            RegistryClient::new(server.uri()),
            CacheConfig {
                mode: RegisterMode::LookupOnly,
            },
        );
        c.intern(
            "orders-value",
            SchemaKind::Json,
            r#"{"type":"object"}"#,
            None,
        );

        c.prewarm().await.unwrap();

        check!(
            (
                c.id_for_subject("orders-value"),
                c.writer_schema(51).unwrap()
            ) == (Some(51), r#"{"type":"object"}"#.to_string())
        );
    }

    #[tokio::test]
    async fn prewarm_use_latest_caches_registry_schema_references_and_message_type() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 52,
                "version": 4,
                "schema": "syntax = \"proto3\"; import \"money.proto\";",
                "schemaType": "PROTOBUF",
                "messageType": "demo.Latest",
                "references": [{"name": "money.proto", "subject": "money-value", "version": 1}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/money-value/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 53,
                "version": 1,
                "schema": "syntax = \"proto3\"; package money; message Money {}",
                "schemaType": "PROTOBUF"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let c = SchemaCache::new(
            RegistryClient::new(server.uri()),
            CacheConfig {
                mode: RegisterMode::UseLatest,
            },
        );
        c.intern(
            "orders-value",
            SchemaKind::Protobuf,
            "syntax = \"proto3\";",
            Some("demo.Local"),
        );

        c.prewarm().await.unwrap();

        check!(c.id_for_subject("orders-value") == Some(52));
        let writer_schema = c.writer_schema_with_references(52).unwrap();
        check!(writer_schema.schema == "syntax = \"proto3\"; import \"money.proto\";");
        check!(writer_schema.references.contains_key("money.proto"));
        check!(c.writer_message_type(52).as_deref() == Some("demo.Latest"));
    }

    async fn wait_for_writer_schema(
        c: &Arc<SchemaCache>,
        id: u32,
    ) -> Result<WriterSchema, SchemaSerdeError> {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match c.writer_schema_with_references(id) {
                    Err(SchemaSerdeError::WriterSchemaPending(_)) => {
                        tokio::task::yield_now().await;
                    }
                    result => return result,
                }
            }
        })
        .await
        .expect("writer schema fetch completes within the test deadline")
    }

    #[tokio::test]
    async fn writer_schema_fetch_resolves_registry_reference_sources() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/60"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": "syntax = \"proto3\"; import \"money.proto\";",
                "schemaType": "PROTOBUF",
                "references": [{"name": "money.proto", "subject": "money-value", "version": 1}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/money-value/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 61,
                "version": 1,
                "schema": "syntax = \"proto3\"; package money; message Money { int64 cents = 1; }",
                "schemaType": "PROTOBUF"
            })))
            .mount(&server)
            .await;

        let c = SchemaCache::new(RegistryClient::new(server.uri()), CacheConfig::default());
        check!(matches!(
            c.writer_schema_with_references(60),
            Err(SchemaSerdeError::WriterSchemaPending(60))
        ));
        let writer_schema = wait_for_writer_schema(&c, 60).await.unwrap();

        check!(writer_schema.references.contains_key("money.proto"));
    }

    #[tokio::test]
    async fn writer_schema_fetch_reports_missing_reference_without_remaining_pending() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/62"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": "syntax = \"proto3\"; import \"missing.proto\";",
                "schemaType": "PROTOBUF",
                "references": [{"name": "missing.proto", "subject": "missing-value", "version": 1}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/missing-value/versions/1"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;

        let c = SchemaCache::new(RegistryClient::new(server.uri()), CacheConfig::default());
        let error = wait_for_writer_schema(&c, 62)
            .await
            .expect_err("missing reference fails");

        check!(matches!(
            error,
            SchemaSerdeError::WriterSchemaUnavailable { id: 62, .. }
        ));
    }

    #[tokio::test]
    async fn writer_schema_fetch_terminally_caches_malformed_registry_json() {
        let server = MockServer::start().await;
        let requests = Arc::new(AtomicUsize::new(0));
        let responses = Arc::clone(&requests);
        Mock::given(method("GET"))
            .and(path("/schemas/ids/63"))
            .respond_with(move |_: &wiremock::Request| {
                responses.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_string("{not valid json")
            })
            .mount(&server)
            .await;

        let c = SchemaCache::new(RegistryClient::new(server.uri()), CacheConfig::default());
        let error = wait_for_writer_schema(&c, 63)
            .await
            .expect_err("malformed response is terminal");

        check!(matches!(
            error,
            SchemaSerdeError::WriterSchemaUnavailable { id: 63, .. }
        ));
        check!(matches!(
            c.writer_schema_with_references(63),
            Err(SchemaSerdeError::WriterSchemaUnavailable { id: 63, .. })
        ));
        check!(requests.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn writer_schema_fetch_terminally_caches_not_found_response() {
        let server = MockServer::start().await;
        let requests = Arc::new(AtomicUsize::new(0));
        let responses = Arc::clone(&requests);
        Mock::given(method("GET"))
            .and(path("/schemas/ids/65"))
            .respond_with(move |_: &wiremock::Request| {
                responses.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(404).set_body_string("not found")
            })
            .expect(1)
            .mount(&server)
            .await;

        let c = SchemaCache::new(RegistryClient::new(server.uri()), CacheConfig::default());
        let error = wait_for_writer_schema(&c, 65)
            .await
            .expect_err("not found response is terminal");

        check!(matches!(
            error,
            SchemaSerdeError::WriterSchemaUnavailable { id: 65, .. }
        ));
        check!(matches!(
            c.writer_schema_with_references(65),
            Err(SchemaSerdeError::WriterSchemaUnavailable { id: 65, .. })
        ));
        check!(requests.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn writer_schema_fetch_retries_after_a_throttled_registry_response() {
        let server = MockServer::start().await;
        let requests = Arc::new(AtomicUsize::new(0));
        let responses = Arc::clone(&requests);
        Mock::given(method("GET"))
            .and(path("/schemas/ids/64"))
            .respond_with(move |_: &wiremock::Request| {
                if responses.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(429).set_body_string("too many requests")
                } else {
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"schema": "recovered schema"}))
                }
            })
            .mount(&server)
            .await;

        let c = SchemaCache::new(RegistryClient::new(server.uri()), CacheConfig::default());
        let writer_schema = wait_for_writer_schema(&c, 64).await.unwrap();

        check!(writer_schema.schema == "recovered schema");
        check!(requests.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn writer_schema_fetch_retries_after_a_transient_registry_failure() {
        let server = MockServer::start().await;
        let requests = Arc::new(AtomicUsize::new(0));
        let responses = Arc::clone(&requests);
        Mock::given(method("GET"))
            .and(path("/schemas/ids/63"))
            .respond_with(move |_: &wiremock::Request| {
                if responses.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(503).set_body_string("unavailable")
                } else {
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"schema": "recovered schema"}))
                }
            })
            .mount(&server)
            .await;

        let c = SchemaCache::new(RegistryClient::new(server.uri()), CacheConfig::default());
        let writer_schema = wait_for_writer_schema(&c, 63).await.unwrap();
        check!(writer_schema.schema == "recovered schema");
        check!(requests.load(Ordering::SeqCst) == 2);
        check!(!c.inner.lock().unwrap().retry_attempts.contains_key(&63));
    }
}
