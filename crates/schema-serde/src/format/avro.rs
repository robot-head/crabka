//! Avro serde over `apache-avro`.
//!
//! The local type gives its schema with the `AvroSchema` derive. Deserialize
//! resolves the writer schema against that schema.

use std::{marker::PhantomData, sync::Arc};

use apache_avro::{
    AvroSchema, from_avro_datum, from_value, schema::Schema, to_avro_datum, to_value,
};
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    cache::SchemaCache,
    error::SchemaSerdeError,
    format::{Binding, SchemaDeserializer, SchemaSerializer, SchemaSubject},
    subject::{Role, SchemaKind},
    wire,
};

/// Avro serializer and deserializer for `T: AvroSchema`, bound to a key/value
/// role.
///
/// The serde takes the subject from the topic at serialize and deserialize
/// time.
pub struct AvroSerde<T> {
    binding: Binding,
    reader_schema: Schema,
    _marker: PhantomData<fn() -> T>,
}

// Manual `Clone` (not derived) to avoid a spurious `T: Clone` bound;
// `add_source`/`add_sink` require `Serde<_> + Clone`.
impl<T> Clone for AvroSerde<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            reader_schema: self.reader_schema.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: AvroSchema> AvroSerde<T> {
    fn make(cache: &Arc<SchemaCache>, role: Role) -> Self {
        let reader_schema = T::get_schema();
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                role,
                kind: SchemaKind::Avro,
                schema: reader_schema.canonical_form(),
                message_type: None,
            },
            reader_schema,
            _marker: PhantomData,
        }
    }

    /// An Avro serde for record **values**: `<topic>-value`.
    pub fn value(cache: &Arc<SchemaCache>) -> Self {
        Self::make(cache, Role::Value)
    }

    /// An Avro serde for record **keys**: `<topic>-key`.
    pub fn key(cache: &Arc<SchemaCache>) -> Self {
        Self::make(cache, Role::Key)
    }
}

/// A value serde over the process [`default_registry`](crate::default_registry).
///
/// It lets `T` declare a default schema serde for `add_source` and `add_sink`.
impl<T: AvroSchema> Default for AvroSerde<T> {
    fn default() -> Self {
        let cache = crate::default_registry()
            .expect("schema-serde: call set_default_registry(cache) before a default AvroSerde");
        Self::value(&cache)
    }
}

impl<T: Send + Sync + 'static> SchemaSubject for AvroSerde<T> {
    fn register_subject(&self, topic: &str) {
        self.binding.register(topic);
    }
}

impl<T> SchemaSerializer<T> for AvroSerde<T>
where
    T: Serialize + AvroSchema + Send + Sync + 'static,
{
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id(topic)?;
        let avro_value = to_value(value).map_err(|e| SchemaSerdeError::Serialize(e.to_string()))?;
        let body = to_avro_datum(&self.reader_schema, avro_value)
            .map_err(|e| SchemaSerdeError::Serialize(e.to_string()))?;
        Ok(wire::encode(id, &body))
    }
}

impl<T> SchemaDeserializer<T> for AvroSerde<T>
where
    T: DeserializeOwned + AvroSchema + Send + Sync + 'static,
{
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
        let (id, body) = wire::decode(bytes)?;
        let writer_text = self.binding.cache.writer_schema(id)?;
        let writer_schema =
            Schema::parse_str(&writer_text).map_err(|e| SchemaSerdeError::Schema(e.to_string()))?;
        let mut cursor = body;
        let value = from_avro_datum(&writer_schema, &mut cursor, Some(&self.reader_schema))
            .map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))?;
        from_value::<T>(&value).map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::AvroSchema;
    use assert2::check;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        cache::{CacheConfig, SchemaCache},
        registry::RegistryClient,
    };

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
    struct Order {
        id: String,
        total: f64,
    }

    #[test]
    fn round_trips_with_seeded_id() {
        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        let serde = AvroSerde::<Order>::value(&cache);
        serde.register_subject("orders");
        cache.seed_subject_id("orders-value", 11);
        cache.seed_writer_schema(11, Order::get_schema().canonical_form());

        let order = Order {
            id: "o-1".into(),
            total: 9.5,
        };
        let framed = serde.serialize("orders", &order).unwrap();
        check!((&framed[..5]) == [0x00, 0x00, 0x00, 0x00, 0x0b]);
        let back: Order = serde.deserialize("orders", &framed).unwrap();
        check!(back == order);
    }
}
