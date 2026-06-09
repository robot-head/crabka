//! Shared, background-refreshed schema cache. Hot-path reads are synchronous;
//! registry I/O happens at pre-warm and on background fetches.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::SchemaSerdeError;
use crate::registry::RegistryClient;
use crate::subject::{Role, SchemaKind, SubjectStrategy, TopicNameStrategy};

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
}

#[derive(Default)]
struct Inner {
    /// subject ⇒ resolved id (serialize path).
    subject_id: HashMap<String, u32>,
    /// id ⇒ writer schema text (deserialize path).
    id_schema: HashMap<u32, String>,
    /// Local schemas to resolve on pre-warm.
    interned: Vec<Interned>,
    /// ids whose fetch is in flight (dedup background fetches).
    fetching: std::collections::HashSet<u32>,
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
    pub fn intern(&self, subject: &str, kind: SchemaKind, schema: &str) {
        let mut g = self.inner.lock().unwrap();
        if g.interned.iter().any(|i| i.subject == subject) {
            return;
        }
        g.interned.push(Interned {
            subject: subject.to_string(),
            kind,
            schema: schema.to_string(),
        });
    }

    /// Resolve every interned subject's id (register/lookup/latest per mode).
    /// Called once at client/membership start.
    pub async fn prewarm(&self) -> Result<(), SchemaSerdeError> {
        let pending: Vec<Interned> = self.inner.lock().unwrap().interned.clone();
        for i in pending {
            let id = match self.config.mode {
                RegisterMode::AutoRegister => {
                    self.client.register(&i.subject, i.kind, &i.schema).await?
                }
                RegisterMode::LookupOnly => {
                    self.client.lookup(&i.subject, i.kind, &i.schema).await?
                }
                RegisterMode::UseLatest => self.client.latest_id(&i.subject).await?,
            };
            let mut g = self.inner.lock().unwrap();
            g.subject_id.insert(i.subject.clone(), id);
            g.id_schema.insert(id, i.schema.clone());
        }
        Ok(())
    }

    /// Synchronous hot-path read: the id bound to `subject`, or `None` if
    /// pre-warm has not resolved it.
    #[must_use]
    pub fn id_for_subject(&self, subject: &str) -> Option<u32> {
        self.inner.lock().unwrap().subject_id.get(subject).copied()
    }

    /// Synchronous hot-path read of a writer schema by id. On a miss, spawns a
    /// background fetch and returns `WriterSchemaPending` (retriable).
    pub fn writer_schema(self: &Arc<Self>, id: u32) -> Result<String, SchemaSerdeError> {
        {
            let mut g = self.inner.lock().unwrap();
            if let Some(s) = g.id_schema.get(&id) {
                return Ok(s.clone());
            }
            if g.fetching.insert(id) {
                let this = Arc::clone(self);
                tokio::spawn(async move {
                    let fetched = this.client.schema_by_id(id).await;
                    let mut g = this.inner.lock().unwrap();
                    g.fetching.remove(&id);
                    if let Ok(schema) = fetched {
                        g.id_schema.insert(id, schema);
                    }
                });
            }
        }
        Err(SchemaSerdeError::WriterSchemaPending(id))
    }

    /// Test/seed hook: install an id→schema mapping directly.
    pub fn seed_writer_schema(&self, id: u32, schema: impl Into<String>) {
        self.inner
            .lock()
            .unwrap()
            .id_schema
            .insert(id, schema.into());
    }

    /// Test/seed hook: install a subject→id mapping directly.
    pub fn seed_subject_id(&self, subject: impl Into<String>, id: u32) {
        self.inner
            .lock()
            .unwrap()
            .subject_id
            .insert(subject.into(), id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::RegistryClient;
    use assert2::check;

    fn cache() -> Arc<SchemaCache> {
        SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default())
    }

    #[test]
    fn intern_is_idempotent_per_subject() {
        let c = cache();
        c.intern("orders-value", SchemaKind::Avro, "a");
        c.intern("orders-value", SchemaKind::Avro, "a");
        check!(c.inner.lock().unwrap().interned.len() == 1);
    }

    #[tokio::test]
    async fn seeded_reads_are_synchronous() {
        let c = cache();
        c.seed_subject_id("orders-value", 42);
        c.seed_writer_schema(42, "schema-text");
        check!(c.id_for_subject("orders-value") == Some(42));
        // Unknown id ⇒ pending (spawns a background fetch; needs a runtime).
        check!(c.writer_schema(7).is_err());
        check!(c.writer_schema(42).unwrap() == "schema-text");
    }

    #[test]
    fn default_mode_is_auto_register() {
        check!(CacheConfig::default().mode == RegisterMode::AutoRegister);
    }
}
