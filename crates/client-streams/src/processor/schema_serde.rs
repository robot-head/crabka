//! Bridges `crabka-schema-serde` typed serdes into the Streams `Serde<T>`
//! boundary, and implements the membership `SchemaPrewarm` hook for
//! `SchemaCache`. The `schema-serde` feature gates this module.

use bytes::Bytes;
use crabka_schema_serde::{
    SchemaCache,
    format::{SchemaDeserializer, SchemaSerializer, SchemaSubject},
};

use crate::{
    error::StreamsClientError,
    membership::SchemaPrewarm,
    processor::serde::{Serde, SerdeAssociate, SerdeError, SerdeRole},
};

/// Wraps a schema-serde serializer and deserializer pair as a Streams
/// `Serde<T>`.
pub struct SchemaSerde<T, S> {
    inner: S,
    _marker: std::marker::PhantomData<fn() -> T>,
}

// Manual `Clone` (not derived) to avoid a spurious `T: Clone` bound;
// `add_source`/`add_sink` require `Serde<_> + Clone`.
impl<T, S: Clone> Clone for SchemaSerde<T, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, S> SchemaSerde<T, S> {
    /// Wrap a schema-serde serde, such as an `AvroSerde<T>`.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

/// `Default` wraps the inner serde's `Default`, such as
/// `AvroSerde<T>::default()`, which reads the process default registry. `T` can
/// therefore declare a default schema serde with `DefaultSerde`, for use with
/// `add_source` and `add_sink`.
impl<T, S: Default> Default for SchemaSerde<T, S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<T: Send + Sync + 'static, S> SerdeAssociate for SchemaSerde<T, S> {
    type Target = T;
}

impl<T, S> Serde<T> for SchemaSerde<T, S>
where
    T: Send + Sync + 'static,
    S: SchemaSerializer<T> + SchemaDeserializer<T> + SchemaSubject,
{
    fn serialize(&self, topic: &str, value: &T) -> Bytes {
        // The Streams sink path is infallible; a missing id means pre-warm was
        // skipped — surface it loudly rather than writing a bad frame.
        self.inner
            .serialize(topic, value)
            .expect("schema serialize failed (did membership prewarm run?)")
    }

    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<T, SerdeError> {
        self.inner
            .deserialize(topic, bytes)
            .map_err(|e| SerdeError(e.to_string()))
    }

    fn prepare(&self, topic: &str, _role: SerdeRole) {
        // The serde was constructed with its key/value role; intern its subject
        // for `topic` so membership pre-warm resolves the id.
        self.inner.register_subject(topic);
    }
}

#[async_trait::async_trait]
impl SchemaPrewarm for SchemaCache {
    async fn prewarm(&self) -> Result<(), StreamsClientError> {
        // Call the inherent SchemaCache::prewarm (not this trait method).
        // Inherent methods take priority in method resolution, so
        // `SchemaCache::prewarm(self)` resolves to the inherent async method
        // returning `Result<(), SchemaSerdeError>`, NOT this trait impl.
        SchemaCache::prewarm(self)
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
}
