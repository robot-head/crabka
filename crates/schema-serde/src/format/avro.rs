//! Avro serde over `apache-avro`. The local type provides its schema via the
//! `AvroSchema` derive; deserialize resolves the writer schema against it.

use std::marker::PhantomData;
use std::sync::Arc;

use apache_avro::schema::Schema;
use apache_avro::{AvroSchema, from_avro_datum, from_value, to_avro_datum, to_value};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::cache::SchemaCache;
use crate::error::SchemaSerdeError;
use crate::format::{Binding, SchemaDeserializer, SchemaSerializer};
use crate::subject::SchemaKind;
use crate::wire;

/// Avro serializer/deserializer for `T: AvroSchema`, bound to a subject.
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
    /// Bind `T`'s derived schema to `subject` and intern it for pre-warm.
    pub fn new(cache: &Arc<SchemaCache>, subject: impl Into<String>) -> Self {
        let subject = subject.into();
        let reader_schema = T::get_schema();
        cache.intern(&subject, SchemaKind::Avro, &reader_schema.canonical_form());
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                subject,
            },
            reader_schema,
            _marker: PhantomData,
        }
    }
}

impl<T> SchemaSerializer<T> for AvroSerde<T>
where
    T: Serialize + AvroSchema + Send + Sync + 'static,
{
    fn serialize(&self, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id()?;
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
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
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
    use super::*;
    use crate::cache::{CacheConfig, SchemaCache};
    use crate::registry::RegistryClient;
    use apache_avro::AvroSchema;
    use assert2::check;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
    struct Order {
        id: String,
        total: f64,
    }

    #[test]
    fn round_trips_with_seeded_id() {
        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        let serde = AvroSerde::<Order>::new(&cache, "orders-value");
        cache.seed_subject_id("orders-value", 11);
        cache.seed_writer_schema(11, Order::get_schema().canonical_form());

        let order = Order {
            id: "o-1".into(),
            total: 9.5,
        };
        let framed = serde.serialize(&order).unwrap();
        check!(framed[0] == 0x00);
        check!(u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]) == 11);
        let back: Order = serde.deserialize(&framed).unwrap();
        check!(back == order);
    }
}
