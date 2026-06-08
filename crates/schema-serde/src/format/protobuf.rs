//! Protobuf serde over `prost` + `prost-reflect`. The local message provides
//! its descriptor via `ReflectMessage`; the registered schema is the
//! normalized `.proto` text of its file descriptor.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use prost_reflect::ReflectMessage;

use crate::cache::SchemaCache;
use crate::error::SchemaSerdeError;
use crate::format::{Binding, SchemaDeserializer, SchemaSerializer};
use crate::subject::SchemaKind;
use crate::wire;

/// Protobuf serializer/deserializer for a `prost` message `T: ReflectMessage`.
pub struct ProtobufSerde<T> {
    binding: Binding,
    message_index: Vec<i32>,
    _marker: PhantomData<fn() -> T>,
}

// Manual `Clone` (not derived) to avoid a spurious `T: Clone` bound;
// `add_source`/`add_sink` require `Serde<_> + Clone`.
impl<T> Clone for ProtobufSerde<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            message_index: self.message_index.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: ReflectMessage + Default> ProtobufSerde<T> {
    /// Bind `T`'s descriptor to `subject` and intern its `.proto` schema.
    pub fn new(cache: &Arc<SchemaCache>, subject: impl Into<String>) -> Self {
        let subject = subject.into();
        let descriptor = T::default().descriptor();
        let proto_text = proto_source(&descriptor);
        let message_index = message_index(&descriptor);
        cache.intern(&subject, SchemaKind::Protobuf, &proto_text);
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                subject,
            },
            message_index,
            _marker: PhantomData,
        }
    }
}

impl<T> SchemaSerializer<T> for ProtobufSerde<T>
where
    T: Message + ReflectMessage + Send + Sync + 'static,
{
    fn serialize(&self, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id()?;
        let body = value.encode_to_vec();
        Ok(wire::encode_protobuf(id, &self.message_index, &body))
    }
}

impl<T> SchemaDeserializer<T> for ProtobufSerde<T>
where
    T: Message + ReflectMessage + Default + Send + Sync + 'static,
{
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
        // prost decodes structurally; id/index validated by framing
        let (_id, _idx, body) = wire::decode_protobuf(bytes)?;
        T::decode(body).map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))
    }
}

/// Render the file descriptor of `descriptor`'s parent file to `.proto` text.
fn proto_source(descriptor: &prost_reflect::MessageDescriptor) -> String {
    let file = descriptor.parent_file();
    print::file_to_proto(file.file_descriptor_proto())
}

/// Compute the Confluent message-index path of `descriptor` within its file.
fn message_index(descriptor: &prost_reflect::MessageDescriptor) -> Vec<i32> {
    let file = descriptor.parent_file();
    let target = descriptor.full_name();
    for (i, m) in file.messages().enumerate() {
        if m.full_name() == target {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            return vec![i as i32];
        }
    }
    vec![0]
}

/// Minimal `.proto` text renderer. Kept narrow: the registry stores text for
/// dedup; full normalization parity is a verify-against-cp item.
pub(crate) mod print {
    use std::fmt::Write as _;

    use prost_reflect::prost_types::FileDescriptorProto;

    pub fn file_to_proto(file: &FileDescriptorProto) -> String {
        let mut out = String::new();
        out.push_str("syntax = \"proto3\";\n");
        if let Some(pkg) = file.package.as_deref()
            && !pkg.is_empty()
        {
            let _ = writeln!(out, "package {pkg};");
        }
        for msg in &file.message_type {
            let msg_name = msg.name.as_deref().unwrap_or("");
            let _ = write!(out, "\nmessage {msg_name} {{\n");
            for field in &msg.field {
                let type_str = field
                    .type_name
                    .as_deref()
                    .unwrap_or("")
                    .trim_start_matches('.');
                let field_name = field.name.as_deref().unwrap_or("");
                let field_num = field.number.unwrap_or(0);
                let _ = writeln!(out, "  {type_str} {field_name} = {field_num};");
            }
            out.push_str("}\n");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::print::file_to_proto;
    use assert2::check;
    use prost_reflect::prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};

    #[test]
    fn renders_minimal_proto_text() {
        let file = FileDescriptorProto {
            package: Some("demo".into()),
            message_type: vec![DescriptorProto {
                name: Some("Order".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("id".into()),
                    number: Some(1),
                    type_name: Some(".string".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let text = file_to_proto(&file);
        check!(text.contains("package demo;"));
        check!(text.contains("message Order {"));
        check!(text.contains("id = 1;"));
    }
}
