//! JSON Schema serde. The local type provides its schema via `schemars`;
//! payloads are UTF-8 JSON, optionally validated against the writer schema.

use std::{marker::PhantomData, sync::Arc};

use bytes::Bytes;
use schemars::JsonSchema;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    cache::SchemaCache,
    error::SchemaSerdeError,
    format::{Binding, SchemaDeserializer, SchemaSerializer, SchemaSubject},
    subject::{Role, SchemaKind},
    wire,
};

/// JSON serializer/deserializer for `T: JsonSchema`, bound to a key/value role;
/// the subject is derived from the topic at call time.
pub struct JsonSerde<T> {
    binding: Binding,
    validate: bool,
    _marker: PhantomData<fn() -> T>,
}

// Manual `Clone` (not derived) to avoid a spurious `T: Clone` bound;
// `add_source`/`add_sink` require `Serde<_> + Clone`.
impl<T> Clone for JsonSerde<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            validate: self.validate,
            _marker: PhantomData,
        }
    }
}

impl<T: JsonSchema> JsonSerde<T> {
    fn make(cache: &Arc<SchemaCache>, role: Role, validate: bool) -> Self {
        // schemars 1.x: schema_for! returns schemars::Schema (newtype over serde_json::Value).
        let schema = schemars::schema_for!(T);
        let schema_text = serde_json::to_string(&schema).expect("schemars schema serializes");
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                role,
                kind: SchemaKind::Json,
                schema: schema_text,
                message_type: None,
            },
            validate,
            _marker: PhantomData,
        }
    }

    /// A JSON serde for record **values** (`<topic>-value`). `validate` enables
    /// draft validation of decoded payloads against the writer schema.
    pub fn value(cache: &Arc<SchemaCache>, validate: bool) -> Self {
        Self::make(cache, Role::Value, validate)
    }

    /// A JSON serde for record **keys** (`<topic>-key`).
    pub fn key(cache: &Arc<SchemaCache>, validate: bool) -> Self {
        Self::make(cache, Role::Key, validate)
    }
}

/// A value serde over the process [`default_registry`](crate::default_registry),
/// with payload validation disabled (enable it via [`JsonSerde::value`]).
impl<T: JsonSchema> Default for JsonSerde<T> {
    fn default() -> Self {
        let cache = crate::default_registry()
            .expect("schema-serde: call set_default_registry(cache) before a default JsonSerde");
        Self::value(&cache, false)
    }
}

impl<T: Send + Sync + 'static> SchemaSubject for JsonSerde<T> {
    fn register_subject(&self, topic: &str) {
        self.binding.register(topic);
    }
}

impl<T> SchemaSerializer<T> for JsonSerde<T>
where
    T: Serialize + JsonSchema + Send + Sync + 'static,
{
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id(topic)?;
        let body =
            serde_json::to_vec(value).map_err(|e| SchemaSerdeError::Serialize(e.to_string()))?;
        Ok(wire::encode(id, &body))
    }
}

impl<T> SchemaDeserializer<T> for JsonSerde<T>
where
    T: DeserializeOwned + JsonSchema + Send + Sync + 'static,
{
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
        let (id, body) = wire::decode(bytes)?;
        if self.validate {
            let writer_text = self.binding.cache.writer_schema(id)?;
            let writer: serde_json::Value = serde_json::from_str(&writer_text)
                .map_err(|e| SchemaSerdeError::Schema(e.to_string()))?;
            let instance: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))?;
            // jsonschema: validator_for(&Value) -> Result<Validator, ValidationError<'static>>
            let validator = jsonschema::validator_for(&writer)
                .map_err(|e| SchemaSerdeError::Schema(e.to_string()))?;
            // Validator::validate(&self, instance) -> Result<(), ValidationError<'i>>
            validator.validate(&instance).map_err(|e| {
                SchemaSerdeError::Deserialize(format!("json schema validation: {e}"))
            })?;
        }
        serde_json::from_slice(body).map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        cache::{CacheConfig, SchemaCache},
        registry::RegistryClient,
    };

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
    struct Order {
        id: String,
        total: f64,
    }

    #[test]
    fn round_trips_with_validation() {
        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        let serde = JsonSerde::<Order>::value(&cache, true);
        serde.register_subject("orders");
        let schema_text = serde_json::to_string(&schemars::schema_for!(Order)).unwrap();
        cache.seed_subject_id("orders-value", 5);
        cache.seed_writer_schema(5, schema_text);

        let order = Order {
            id: "o-1".into(),
            total: 3.0,
        };
        let framed = serde.serialize("orders", &order).unwrap();
        check!(framed[0] == 0x00);
        let back: Order = serde.deserialize("orders", &framed).unwrap();
        check!(back == order);
    }
}
