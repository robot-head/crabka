//! Bridges `crabka-schema-serde` typed serdes into the Streams `Serde<T>`
//! boundary, and implements the membership `SchemaPrewarm` hook for
//! `SchemaCache`. Gated by the `schema-serde` feature.

use bytes::Bytes;
use crabka_schema_serde::SchemaCache;
use crabka_schema_serde::format::{SchemaDeserializer, SchemaSerializer};

use crate::error::StreamsClientError;
use crate::membership::SchemaPrewarm;
use crate::processor::serde::{Serde, SerdeError};

/// Wraps a schema-serde serializer+deserializer pair as a Streams `Serde<T>`.
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
    /// Wrap a schema-serde serde (e.g. `AvroSerde<T>`).
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, S> Serde<T> for SchemaSerde<T, S>
where
    T: Send + Sync + 'static,
    S: SchemaSerializer<T> + SchemaDeserializer<T>,
{
    fn serialize(&self, value: &T) -> Bytes {
        // The Streams sink path is infallible; a missing id means pre-warm was
        // skipped — surface it loudly rather than writing a bad frame.
        self.inner
            .serialize(value)
            .expect("schema serialize failed (did membership prewarm run?)")
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerdeError> {
        self.inner
            .deserialize(bytes)
            .map_err(|e| SerdeError(e.to_string()))
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
