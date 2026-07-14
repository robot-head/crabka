//! Confluent-wire decode: strip the 5-byte envelope, fetch the schema from
//! the registry cache, and materialize an Avro Value, JSON Value, or Protobuf
//! `DynamicMessage`.

use std::{collections::HashMap, sync::Arc, time::Duration};

use crabka_schema_serde::{SchemaCache, SchemaSerdeError};
use prost_reflect::prost_types::FileDescriptorSet;

use crate::error::KafkaFdwError;

/// Total time the cold-cache schema fetch is allowed to take before giving up.
const SCHEMA_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// Poll cadence while waiting for the background schema fetch to populate the
/// cache.
const SCHEMA_FETCH_POLL: Duration = Duration::from_millis(20);

/// Resolve a writer schema by id, awaiting the cache's background fetch.
///
/// [`SchemaCache::writer_schema`] is a *synchronous hot-path read*: on a cold
/// miss it spawns a background fetch and immediately returns
/// [`SchemaSerdeError::WriterSchemaPending`]. The FDW decode path runs inside
/// an async scan, so we retry with a bounded backoff until the background
/// fetch populates the cache (or [`SCHEMA_FETCH_TIMEOUT`] elapses). Any other
/// error is returned immediately.
async fn resolve_writer_schema(
    cache: &Arc<SchemaCache>,
    schema_id: u32,
) -> Result<crabka_schema_serde::cache::WriterSchema, KafkaFdwError> {
    let deadline = tokio::time::Instant::now() + SCHEMA_FETCH_TIMEOUT;
    loop {
        match cache.writer_schema_with_references(schema_id) {
            Ok(schema) => return Ok(schema),
            Err(SchemaSerdeError::WriterSchemaPending(_)) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(KafkaFdwError::Other(format!(
                        "schema registry: writer schema for id {schema_id} not fetched within {SCHEMA_FETCH_TIMEOUT:?}"
                    )));
                }
                tokio::time::sleep(SCHEMA_FETCH_POLL).await;
            }
            Err(e) => return Err(KafkaFdwError::Other(format!("schema registry: {e}"))),
        }
    }
}

/// Wire format declared in the foreign table OPTIONS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    /// Pass raw bytes through unchanged (no schema registry).
    Raw,
    /// Confluent Avro binary encoding.
    Avro,
    /// Confluent JSON encoding (framed with a 5-byte header).
    Json,
    /// Confluent Protobuf encoding (framed with a header + message-index varint).
    Protobuf,
}

/// A decoded Kafka message body, ready for column projection.
pub enum DecodedValue {
    /// Schema-decoded Apache Avro value.
    Avro(apache_avro::types::Value),
    /// Schema-decoded JSON value.
    Json(serde_json::Value),
    /// Raw bytes (no schema decoding).
    Raw(Vec<u8>),
    /// Schema-decoded Protobuf dynamic message.
    Protobuf(prost_reflect::DynamicMessage),
}

/// Decode a Kafka message payload according to `fmt`.
///
/// Returns the decoded value alongside the writer [`apache_avro::Schema`] that
/// was used to decode it (Avro only) — the schema is `None` for the JSON,
/// Raw, and Protobuf paths. The scanner threads this schema into
/// [`crate::types::project`] so that decimal `scale` is applied during
/// projection (the parse already happens here, so returning it avoids
/// fetching/parsing the schema a second time).
///
/// * `Wire::Raw`      — wraps the bytes verbatim; no registry access.
/// * `Wire::Avro`     — strips the Confluent 5-byte header, fetches the writer
///   schema from `cache` by id, then decodes the body with
///   `apache_avro::from_avro_datum`.
/// * `Wire::Json`     — strips the header, fetches the schema text (used only
///   for validation today), then deserialises the body as JSON.
/// * `Wire::Protobuf` — strips the Confluent protobuf envelope (magic byte +
///   schema-id + message-index varint), fetches the `FileDescriptorSet` proto
///   from the registry by schema id, builds a `prost_reflect::MessageDescriptor`
///   for the indexed message, and decodes the body via
///   `prost_reflect::DynamicMessage::decode`.
///
/// # Errors
///
/// Returns [`KafkaFdwError`] when the wire envelope, registry schema, or payload cannot be decoded.
pub async fn decode_value(
    cache: &Arc<SchemaCache>,
    fmt: Wire,
    _topic: &str,
    bytes: &[u8],
) -> Result<(DecodedValue, Option<apache_avro::Schema>), KafkaFdwError> {
    match fmt {
        Wire::Raw => Ok((DecodedValue::Raw(bytes.to_vec()), None)),

        Wire::Avro => {
            let (schema_id, body) = crabka_schema_serde::wire::decode(bytes)
                .map_err(|e| KafkaFdwError::Other(format!("avro wire decode: {e}")))?;

            // Fetch (or await) the writer schema by id.
            let schema_text = resolve_writer_schema(cache, schema_id).await?.schema;

            let schema = apache_avro::Schema::parse_str(&schema_text)
                .map_err(|e| KafkaFdwError::Other(format!("avro schema parse: {e}")))?;

            let value = apache_avro::from_avro_datum(&schema, &mut &body[..], None)
                .map_err(|e| KafkaFdwError::Other(format!("avro datum decode: {e}")))?;

            Ok((DecodedValue::Avro(value), Some(schema)))
        }

        Wire::Json => {
            let (_schema_id, body) = crabka_schema_serde::wire::decode(bytes)
                .map_err(|e| KafkaFdwError::Other(format!("json wire decode: {e}")))?;

            let value: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| KafkaFdwError::Other(format!("json decode: {e}")))?;

            Ok((DecodedValue::Json(value), None))
        }

        Wire::Protobuf => {
            // Strip the Confluent protobuf envelope: magic byte + schema-id (4 BE
            // bytes) + message-index zigzag varint(s).
            let (schema_id, message_index, body) =
                crabka_schema_serde::wire::decode_protobuf(bytes)
                    .map_err(|e| KafkaFdwError::Other(format!("protobuf wire decode: {e}")))?;

            // Fetch the schema text (a base64-encoded serialised FileDescriptorSet)
            // from the registry by id.
            let writer_schema = resolve_writer_schema(cache, schema_id).await?;

            let descriptor = build_message_descriptor_for_index_with_references(
                &writer_schema.schema,
                &writer_schema.references,
                &message_index,
                cache.writer_message_type(schema_id).as_deref(),
            )
            .map_err(|e| KafkaFdwError::Other(format!("protobuf descriptor: {e}")))?;

            let msg = prost_reflect::DynamicMessage::decode(descriptor, body)
                .map_err(|e| KafkaFdwError::Other(format!("protobuf decode: {e}")))?;

            Ok((DecodedValue::Protobuf(msg), None))
        }
    }
}

/// Build a [`prost_reflect::MessageDescriptor`] from Confluent Schema Registry
/// Protobuf schema text (`.proto` source).
///
/// When Schema Registry supplies `messageType`, that fully-qualified name is
/// selected exactly. Otherwise the descriptor for the schema's first top-level
/// message is selected, matching Confluent's single-message convention.
///
/// # Errors
///
/// Returns an error when the schema cannot be compiled or its message cannot be resolved.
pub fn build_message_descriptor(
    schema_text: &str,
    message_type: Option<&str>,
) -> Result<prost_reflect::MessageDescriptor, String> {
    build_message_descriptor_with_references(schema_text, &HashMap::new(), message_type)
}

/// Build the descriptor selected by a Confluent Protobuf message-index path.
///
/// The frame's index is authoritative. Registry `messageType` metadata, when
/// present, is constrained to identify that same message rather than overriding
/// the producer-selected path.
///
/// # Errors
///
/// Returns an error for invalid schemas, message indexes, or conflicting message metadata.
pub fn build_message_descriptor_for_index_with_references<S: std::hash::BuildHasher>(
    schema_text: &str,
    references: &HashMap<String, String, S>,
    message_index: &[i32],
    message_type: Option<&str>,
) -> Result<prost_reflect::MessageDescriptor, String> {
    let (file_descriptor_set, root_file_name) = compile_schema_text(schema_text, references)?;
    let selected_message =
        message_name_at_index(&file_descriptor_set, &root_file_name, message_index)?;
    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .map_err(|e| format!("load descriptor set: {e}"))?;
    let descriptor = pool
        .get_message_by_name(&selected_message)
        .ok_or_else(|| format!("message type {selected_message:?} not found in Protobuf schema"))?;

    if let Some(metadata_name) = message_type.map(|name| name.trim_start_matches('.'))
        && metadata_name != descriptor.full_name()
    {
        return Err(format!(
            "protobuf messageType {metadata_name:?} conflicts with frame message-index selecting {:?}",
            descriptor.full_name()
        ));
    }
    Ok(descriptor)
}

/// Build a descriptor from a root source and the exact import-name-to-source
/// mapping returned by Schema Registry. The resolver has no filesystem or
/// network fallback: imports absent from `references` fail at compile time.
///
/// # Errors
///
/// Returns an error when imports, descriptors, or the requested message cannot be resolved.
pub fn build_message_descriptor_with_references<S: std::hash::BuildHasher>(
    schema_text: &str,
    references: &HashMap<String, String, S>,
    message_type: Option<&str>,
) -> Result<prost_reflect::MessageDescriptor, String> {
    let (file_descriptor_set, root_file_name) = compile_schema_text(schema_text, references)?;
    let selected_message = match message_type {
        Some(name) => name.trim_start_matches('.').to_string(),
        None => first_message_name(&file_descriptor_set, &root_file_name)?,
    };
    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .map_err(|e| format!("load descriptor set: {e}"))?;
    pool.get_message_by_name(&selected_message)
        .ok_or_else(|| format!("message type {selected_message:?} not found in Protobuf schema"))
}

fn compile_schema_text<S: std::hash::BuildHasher>(
    schema_text: &str,
    references: &HashMap<String, String, S>,
) -> Result<(FileDescriptorSet, String), String> {
    struct InMemoryResolver {
        sources: HashMap<String, String>,
    }

    impl protox::file::FileResolver for InMemoryResolver {
        fn open_file(&self, name: &str) -> Result<protox::file::File, protox::Error> {
            let source = self
                .sources
                .get(name)
                .ok_or_else(|| protox::Error::file_not_found(name))?;
            protox::file::File::from_source(name, source)
        }
    }

    let mut sources: HashMap<String, String> = references
        .iter()
        .map(|(name, source)| (name.clone(), source.clone()))
        .collect();
    let root_file_name = root_virtual_file_name(references);
    sources.insert(root_file_name.clone(), schema_text.to_string());
    let mut reference_names: Vec<&str> = references.keys().map(String::as_str).collect();
    reference_names.sort_unstable();
    let mut compiler = protox::Compiler::with_file_resolver(InMemoryResolver { sources });
    for reference_name in reference_names {
        compiler
            .open_file(reference_name)
            .map_err(|e| format!("compile registry-provided .proto sources: {e}"))?;
    }
    compiler
        .open_file(&root_file_name)
        .map_err(|e| format!("compile registry-provided .proto sources: {e}"))?;
    Ok((compiler.file_descriptor_set(), root_file_name))
}

fn root_virtual_file_name<S: std::hash::BuildHasher>(
    references: &HashMap<String, String, S>,
) -> String {
    let mut suffix = 0_u32;
    loop {
        let candidate = if suffix == 0 {
            "__crabka_root__.proto".to_string()
        } else {
            format!("__crabka_root_{suffix}__.proto")
        };
        if !references.contains_key(&candidate) {
            return candidate;
        }
        suffix = suffix
            .checked_add(1)
            .expect("root filename suffix overflow");
    }
}

fn message_name_at_index(
    file_descriptor_set: &FileDescriptorSet,
    root_file_name: &str,
    message_index: &[i32],
) -> Result<String, String> {
    let file = file_descriptor_set
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some(root_file_name))
        .ok_or_else(|| {
            format!("Protobuf root file {root_file_name:?} missing from descriptor set")
        })?;
    let (&first_index, nested_indices) = message_index
        .split_first()
        .ok_or_else(|| "Protobuf message-index path is empty".to_string())?;
    let first_index = usize::try_from(first_index)
        .map_err(|_| format!("Protobuf message-index contains negative index {first_index}"))?;
    let mut message = file.message_type.get(first_index).ok_or_else(|| {
        format!("Protobuf message-index {first_index} is outside root message range")
    })?;
    let mut names = vec![
        message
            .name
            .as_deref()
            .ok_or_else(|| "Protobuf message has no name".to_string())?,
    ];
    for &nested_index in nested_indices {
        let nested_index = usize::try_from(nested_index).map_err(|_| {
            format!("Protobuf message-index contains negative index {nested_index}")
        })?;
        message = message.nested_type.get(nested_index).ok_or_else(|| {
            format!("Protobuf message-index {nested_index} is outside nested message range")
        })?;
        names.push(
            message
                .name
                .as_deref()
                .ok_or_else(|| "Protobuf nested message has no name".to_string())?,
        );
    }
    let package = file.package.as_deref().unwrap_or_default();
    if package.is_empty() {
        Ok(names.join("."))
    } else {
        Ok(format!("{package}.{}", names.join(".")))
    }
}

fn first_message_name(
    file_descriptor_set: &FileDescriptorSet,
    root_file_name: &str,
) -> Result<String, String> {
    let file = file_descriptor_set
        .file
        .iter()
        .find(|file| file.name.as_deref() == Some(root_file_name))
        .ok_or_else(|| {
            format!("Protobuf root file {root_file_name:?} missing from descriptor set")
        })?;
    let message = file
        .message_type
        .first()
        .and_then(|message| message.name.as_deref())
        .ok_or_else(|| "Protobuf schema contains no top-level messages".to_string())?;
    let package = file.package.as_deref().unwrap_or_default();
    if package.is_empty() {
        return Ok(message.to_string());
    }
    Ok(format!("{package}.{message}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use prost_reflect::{DynamicMessage, Value, prost::Message as _};

    use super::*;

    const MULTI_MESSAGE_SCHEMA: &str = r#"
        syntax = "proto3";
        package demo;

        message First {
            int64 id = 1;
        }

        message Second {
            string name = 1;
        }
    "#;

    const NESTED_MESSAGE_SCHEMA: &str = r#"
        syntax = "proto3";
        package demo;

        message First { int64 id = 1; }
        message Container {
            message Nested { string value = 1; }
        }
    "#;

    #[test]
    fn descriptor_uses_first_message_when_message_type_is_absent() {
        let descriptor = build_message_descriptor(MULTI_MESSAGE_SCHEMA, None).expect("descriptor");

        assert_eq!(descriptor.full_name(), "demo.First");
    }

    #[test]
    fn descriptor_uses_explicit_message_type_when_present() {
        let descriptor = build_message_descriptor(MULTI_MESSAGE_SCHEMA, Some("demo.Second"))
            .expect("descriptor");

        assert_eq!(descriptor.full_name(), "demo.Second");
    }

    #[test]
    fn descriptor_accepts_leading_dot_message_type() {
        let descriptor = build_message_descriptor(MULTI_MESSAGE_SCHEMA, Some(".demo.Second"))
            .expect("descriptor");

        assert_eq!(descriptor.full_name(), "demo.Second");
    }

    #[test]
    fn frame_message_index_selects_non_first_and_nested_messages() {
        let second = build_message_descriptor_for_index_with_references(
            MULTI_MESSAGE_SCHEMA,
            &HashMap::new(),
            &[1],
            Some("demo.Second"),
        )
        .expect("second descriptor");
        let nested = build_message_descriptor_for_index_with_references(
            NESTED_MESSAGE_SCHEMA,
            &HashMap::new(),
            &[1, 0],
            Some("demo.Container.Nested"),
        )
        .expect("nested descriptor");

        let mut second_message = DynamicMessage::new(second.clone());
        second_message
            .try_set_field_by_name("name", Value::String("second".to_string()))
            .expect("set second name");
        let decoded_second =
            DynamicMessage::decode(second, second_message.encode_to_vec().as_slice())
                .expect("decode second frame body");
        assert_eq!(
            decoded_second
                .get_field_by_name("name")
                .expect("second name")
                .as_str(),
            Some("second")
        );

        let mut nested_message = DynamicMessage::new(nested.clone());
        nested_message
            .try_set_field_by_name("value", Value::String("nested".to_string()))
            .expect("set nested value");
        let decoded_nested =
            DynamicMessage::decode(nested, nested_message.encode_to_vec().as_slice())
                .expect("decode nested frame body");
        assert_eq!(
            decoded_nested
                .get_field_by_name("value")
                .expect("nested value")
                .as_str(),
            Some("nested")
        );
    }

    #[test]
    fn frame_message_index_rejects_negative_out_of_range_and_conflicting_metadata() {
        for (index, message_type, expected) in [
            (vec![-1], None, "negative"),
            (vec![2], None, "outside root"),
            (vec![1], Some("demo.First"), "conflicts"),
        ] {
            let error = build_message_descriptor_for_index_with_references(
                MULTI_MESSAGE_SCHEMA,
                &HashMap::new(),
                &index,
                message_type,
            )
            .expect_err("invalid frame selection");
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn descriptor_resolves_registry_provided_imports() {
        let root = r#"
            syntax = "proto3";
            package demo;
            import "money.proto";
            message Order { money.Money total = 1; }
        "#;
        let references = HashMap::from([(
            "money.proto".to_string(),
            r#"
                syntax = "proto3";
                package money;
                message Money { int64 cents = 1; }
            "#
            .to_string(),
        )]);
        let descriptor = build_message_descriptor_with_references(root, &references, None)
            .expect("descriptor with imported message");
        let money_descriptor = descriptor
            .get_field_by_name("total")
            .expect("total field")
            .kind()
            .as_message()
            .expect("imported message field")
            .clone();
        let mut money = DynamicMessage::new(money_descriptor);
        money
            .try_set_field_by_name("cents", Value::I64(2500))
            .expect("set cents");
        let mut order = DynamicMessage::new(descriptor.clone());
        order
            .try_set_field_by_name("total", Value::Message(money))
            .expect("set total");

        let decoded = DynamicMessage::decode(descriptor, order.encode_to_vec().as_slice())
            .expect("decode imported message");
        assert_eq!(
            decoded
                .get_field_by_name("total")
                .expect("total value")
                .as_message()
                .unwrap()
                .get_field_by_name("cents")
                .expect("cents value")
                .as_i64(),
            Some(2500)
        );
    }

    #[test]
    fn root_virtual_filename_does_not_overwrite_schema_proto_reference() {
        let root = r#"
            syntax = "proto3";
            package demo;
            import "schema.proto";
            message Root { dependency.Dependency value = 1; }
        "#;
        let references = HashMap::from([(
            "schema.proto".to_string(),
            r#"
                syntax = "proto3";
                package dependency;
                message Dependency { string value = 1; }
            "#
            .to_string(),
        )]);

        let descriptor = build_message_descriptor_with_references(root, &references, None)
            .expect("descriptor preserves schema.proto reference");
        assert_eq!(descriptor.full_name(), "demo.Root");
    }
}
