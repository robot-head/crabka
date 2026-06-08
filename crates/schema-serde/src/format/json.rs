//! JSON Schema serde. The local type provides its schema via `schemars`;
//! payloads are UTF-8 JSON, optionally validated against the writer schema.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use schemars::JsonSchema;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::cache::SchemaCache;
use crate::error::SchemaSerdeError;
use crate::format::{Binding, SchemaDeserializer, SchemaSerializer};
use crate::subject::SchemaKind;
use crate::wire;

/// JSON serializer/deserializer for `T: JsonSchema`, bound to a subject.
pub struct JsonSerde<T> {
    binding: Binding,
    validate: bool,
    _marker: PhantomData<fn() -> T>,
}

impl<T: JsonSchema> JsonSerde<T> {
    /// Bind `T`'s `schemars` JSON Schema to `subject` and intern it.
    /// `validate` enables draft validation of decoded payloads.
    pub fn new(cache: &Arc<SchemaCache>, subject: impl Into<String>, validate: bool) -> Self {
        let subject = subject.into();
        // schemars 1.x: schema_for! returns schemars::Schema (newtype over serde_json::Value).
        let schema = schemars::schema_for!(T);
        let schema_text = serde_json::to_string(&schema).expect("schemars schema serializes");
        cache.intern(&subject, SchemaKind::Json, &schema_text);
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                subject,
            },
            validate,
            _marker: PhantomData,
        }
    }
}

impl<T> SchemaSerializer<T> for JsonSerde<T>
where
    T: Serialize + JsonSchema + Send + Sync + 'static,
{
    fn serialize(&self, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id()?;
        let body =
            serde_json::to_vec(value).map_err(|e| SchemaSerdeError::Serialize(e.to_string()))?;
        Ok(wire::encode(id, &body))
    }
}

impl<T> SchemaDeserializer<T> for JsonSerde<T>
where
    T: DeserializeOwned + JsonSchema + Send + Sync + 'static,
{
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
        let (id, body) = wire::decode(bytes)?;
        if self.validate {
            let writer_text = self.binding.cache.writer_schema(id)?;
            let writer: serde_json::Value = serde_json::from_str(&writer_text)
                .map_err(|e| SchemaSerdeError::Schema(e.to_string()))?;
            let instance: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))?;
            // jsonschema 0.26: validator_for(&Value) -> Result<Validator, ValidationError<'static>>
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
    use super::*;
    use crate::cache::{CacheConfig, SchemaCache};
    use crate::registry::RegistryClient;
    use assert2::check;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
    struct Order {
        id: String,
        total: f64,
    }

    #[test]
    fn round_trips_with_validation() {
        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        let serde = JsonSerde::<Order>::new(&cache, "orders-value", true);
        let schema_text = serde_json::to_string(&schemars::schema_for!(Order)).unwrap();
        cache.seed_subject_id("orders-value", 5);
        cache.seed_writer_schema(5, schema_text);

        let order = Order {
            id: "o-1".into(),
            total: 3.0,
        };
        let framed = serde.serialize(&order).unwrap();
        check!(framed[0] == 0x00);
        let back: Order = serde.deserialize(&framed).unwrap();
        check!(back == order);
    }
}
