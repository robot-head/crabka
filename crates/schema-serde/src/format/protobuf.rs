//! Protobuf serde over `prost` + `prost-reflect`. The local message provides
//! its descriptor via `ReflectMessage`; the registered schema is the
//! normalized `.proto` text of its file descriptor.

use std::{marker::PhantomData, sync::Arc};

use bytes::Bytes;
use prost::Message;
use prost_reflect::ReflectMessage;

use crate::{
    cache::SchemaCache,
    error::SchemaSerdeError,
    format::{Binding, SchemaDeserializer, SchemaSerializer, SchemaSubject},
    subject::{Role, SchemaKind},
    wire,
};

/// Protobuf serializer/deserializer for a `prost` message `T: ReflectMessage`,
/// bound to a key/value role; the subject is derived from the topic at call time.
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
    fn make(cache: &Arc<SchemaCache>, role: Role) -> Self {
        let descriptor = T::default().descriptor();
        let proto_text = proto_source(&descriptor);
        let message_index = message_index(&descriptor);
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                role,
                kind: SchemaKind::Protobuf,
                schema: proto_text,
                message_type: Some(descriptor.full_name().to_string()),
            },
            message_index,
            _marker: PhantomData,
        }
    }

    /// A Protobuf serde for record **values** (`<topic>-value`).
    pub fn value(cache: &Arc<SchemaCache>) -> Self {
        Self::make(cache, Role::Value)
    }

    /// A Protobuf serde for record **keys** (`<topic>-key`).
    pub fn key(cache: &Arc<SchemaCache>) -> Self {
        Self::make(cache, Role::Key)
    }
}

/// A value serde over the process [`default_registry`](crate::default_registry).
impl<T: ReflectMessage + Default> Default for ProtobufSerde<T> {
    fn default() -> Self {
        let cache = crate::default_registry().expect(
            "schema-serde: call set_default_registry(cache) before a default ProtobufSerde",
        );
        Self::value(&cache)
    }
}

impl<T: Send + Sync + 'static> SchemaSubject for ProtobufSerde<T> {
    fn register_subject(&self, topic: &str) {
        self.binding.register(topic);
    }
}

impl<T> SchemaSerializer<T> for ProtobufSerde<T>
where
    T: Message + ReflectMessage + Send + Sync + 'static,
{
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id(topic)?;
        let body = value.encode_to_vec();
        Ok(wire::encode_protobuf(id, &self.message_index, &body))
    }
}

impl<T> SchemaDeserializer<T> for ProtobufSerde<T>
where
    T: Message + ReflectMessage + Default + Send + Sync + 'static,
{
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
        // prost decodes structurally; id/index validated by framing and, when
        // registry metadata is available, by the declared protobuf message type.
        let (id, _idx, body) = wire::decode_protobuf(bytes)?;
        if let Some(writer_message_type) = self.binding.cache.writer_message_type(id) {
            let local_message_type = T::default().descriptor().full_name().to_string();
            if writer_message_type != local_message_type {
                return Err(SchemaSerdeError::Deserialize(format!(
                    "protobuf messageType mismatch: writer {writer_message_type}, local {local_message_type}"
                )));
            }
        }
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

    use prost_reflect::prost_types::{
        FieldDescriptorProto, FileDescriptorProto, field_descriptor_proto::Type,
    };

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
                let field_name = field.name.as_deref().unwrap_or("");
                let field_num = field.number.unwrap_or(0);
                let _ = writeln!(out, "  {} {field_name} = {field_num};", field_type(field));
            }
            out.push_str("}\n");
        }
        out
    }

    /// Render a field's `.proto` type token: a message/enum's (leading-dot-stripped)
    /// `type_name`, or the proto3 keyword for a scalar `type`. Scalar fields carry
    /// an empty `type_name` and a populated `type`, so keying off `type_name` alone
    /// would emit no type at all (and produce unparseable `.proto` text).
    fn field_type(field: &FieldDescriptorProto) -> String {
        if let Some(name) = field.type_name.as_deref()
            && !name.is_empty()
        {
            return name.trim_start_matches('.').to_string();
        }
        let scalar = match field.r#type.and_then(|t| Type::try_from(t).ok()) {
            Some(Type::Double) => "double",
            Some(Type::Float) => "float",
            Some(Type::Int64) => "int64",
            Some(Type::Uint64) => "uint64",
            Some(Type::Int32) => "int32",
            Some(Type::Fixed64) => "fixed64",
            Some(Type::Fixed32) => "fixed32",
            Some(Type::Bool) => "bool",
            Some(Type::String) => "string",
            Some(Type::Bytes) => "bytes",
            Some(Type::Uint32) => "uint32",
            Some(Type::Sfixed32) => "sfixed32",
            Some(Type::Sfixed64) => "sfixed64",
            Some(Type::Sint32) => "sint32",
            Some(Type::Sint64) => "sint64",
            _ => "",
        };
        scalar.to_string()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use prost_reflect::prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};

    use super::{ProtobufSerde, print::file_to_proto};
    use crate::format::SchemaDeserializer;

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
        check!(
            (
                text.contains("package demo;"),
                text.contains("message Order {"),
                text.contains("id = 1;"),
            ) == (true, true, true)
        );
    }

    #[test]
    fn renders_scalar_field_types_as_proto3_keywords() {
        // Real prost descriptors set `type` (not `type_name`) for scalars; the
        // rendered `.proto` must name the type or the registry can't parse it.
        // Exercise every proto3 scalar keyword (one field per `field_type` arm),
        // plus the message-typed (`type_name`) branch.
        use prost_reflect::prost_types::field_descriptor_proto::Type;
        let scalars = [
            (Type::Double, "double"),
            (Type::Float, "float"),
            (Type::Int64, "int64"),
            (Type::Uint64, "uint64"),
            (Type::Int32, "int32"),
            (Type::Fixed64, "fixed64"),
            (Type::Fixed32, "fixed32"),
            (Type::Bool, "bool"),
            (Type::String, "string"),
            (Type::Bytes, "bytes"),
            (Type::Uint32, "uint32"),
            (Type::Sfixed32, "sfixed32"),
            (Type::Sfixed64, "sfixed64"),
            (Type::Sint32, "sint32"),
            (Type::Sint64, "sint64"),
        ];
        let mut field = Vec::new();
        for (i, (ty, kw)) in scalars.iter().enumerate() {
            field.push(FieldDescriptorProto {
                name: Some(format!("f_{kw}")),
                number: Some(i32::try_from(i).unwrap() + 1),
                r#type: Some(*ty as i32),
                ..Default::default()
            });
        }
        // Message-typed field: the renderer takes the `type_name` branch and
        // strips the leading dot.
        field.push(FieldDescriptorProto {
            name: Some("nested".into()),
            number: Some(100),
            type_name: Some(".demo.Other".into()),
            ..Default::default()
        });
        let file = FileDescriptorProto {
            package: Some("demo".into()),
            message_type: vec![DescriptorProto {
                name: Some("AllScalars".into()),
                field,
                ..Default::default()
            }],
            ..Default::default()
        };
        let text = file_to_proto(&file);
        for (i, (_, kw)) in scalars.iter().enumerate() {
            check!(text.contains(&format!("{kw} f_{kw} = {};", i + 1)));
        }
        check!(text.contains("demo.Other nested = 100;"));
    }

    #[test]
    fn message_type_metadata_mismatch_rejects_typed_decode() {
        use bytes::{Buf, BufMut};
        use prost::{
            DecodeError, Message,
            encoding::{DecodeContext, WireType},
        };
        use prost_reflect::{
            DescriptorPool, MessageDescriptor, ReflectMessage,
            prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet},
        };

        use crate::{
            cache::{CacheConfig, SchemaCache},
            registry::RegistryClient,
            wire,
        };

        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        struct TestOrder;

        impl Message for TestOrder {
            fn encode_raw(&self, _buf: &mut impl BufMut) {}

            fn merge_field(
                &mut self,
                _tag: u32,
                _wire_type: WireType,
                _buf: &mut impl Buf,
                _ctx: DecodeContext,
            ) -> Result<(), DecodeError> {
                Ok(())
            }

            fn encoded_len(&self) -> usize {
                0
            }

            fn clear(&mut self) {}
        }

        impl ReflectMessage for TestOrder {
            fn descriptor(&self) -> MessageDescriptor {
                static POOL: std::sync::OnceLock<DescriptorPool> = std::sync::OnceLock::new();
                POOL.get_or_init(|| {
                    DescriptorPool::from_file_descriptor_set(FileDescriptorSet {
                        file: vec![FileDescriptorProto {
                            name: Some("demo.proto".into()),
                            package: Some("demo".into()),
                            message_type: vec![DescriptorProto {
                                name: Some("Order".into()),
                                ..Default::default()
                            }],
                            syntax: Some("proto3".into()),
                            ..Default::default()
                        }],
                    })
                    .unwrap()
                })
                .get_message_by_name("demo.Order")
                .unwrap()
            }
        }

        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        cache.seed_subject_id("orders-value", 11);
        cache.seed_writer_message_type(11, "demo.Other");
        let serde = ProtobufSerde::<TestOrder>::value(&cache);
        let frame = wire::encode_protobuf(11, &[0], &[]);

        let err = serde.deserialize("orders", &frame).unwrap_err();
        assert2::assert!(err.to_string().contains("messageType"));
    }
}
