//! Protobuf: parse a single `.proto` source into a `FileDescriptorProto`; dedup
//! key = deterministic prost encoding of the descriptor (source-info cleared so
//! formatting doesn't change the bytes). Confluent-exact canonical form is slice 2+.
//!
//! `normalized_form()` reproduces the pretty-printed text that
//! cp-schema-registry normalises to (verified against the golden fixtures):
//!
//!   `syntax = "proto3";\n\n<messages>\n`
//!
//! where each message is formatted with 2-space indentation.  This is stored
//! in `by_id` so that the REST echo-back matches what cp-schema-registry
//! would return.

use std::fmt::Write as _;

use super::ParsedSchema;
use crate::error::SrError;

use prost_reflect::prost::Message;
use prost_reflect::prost_types::field_descriptor_proto::Label;
use prost_reflect::prost_types::field_descriptor_proto::Type as FieldType;
use prost_reflect::prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};

pub struct ProtobufSchema {
    descriptor: FileDescriptorProto,
    /// Normalised `.proto` text (cp-schema-registry compatible pretty-print).
    normalised: String,
}

/// Return the normalised `.proto` text for a `FileDescriptorProto`, matching
/// the format cp-schema-registry uses when echoing schemas back.
///
/// Format observed in the golden fixtures:
/// ```text
/// syntax = "proto3";
///
/// message User {
///   int32 id = 1;
/// }
/// ```
#[must_use]
pub fn normalize(fdp: &FileDescriptorProto) -> String {
    let mut out = String::new();

    // Emit syntax line; cp-schema-registry always emits `syntax = "proto3";\n`
    // even if the proto2 syntax is used.  For slice-1 we only support proto3.
    let syntax = fdp.syntax.as_deref().unwrap_or("proto3");
    let _ = writeln!(out, "syntax = \"{syntax}\";");

    for msg in &fdp.message_type {
        out.push('\n');
        write_message(&mut out, msg, 0);
    }
    out
}

fn write_message(out: &mut String, msg: &DescriptorProto, depth: usize) {
    let indent = "  ".repeat(depth);
    let name = msg.name.as_deref().unwrap_or("Unknown");
    let _ = writeln!(out, "{indent}message {name} {{");
    for field in &msg.field {
        write_field(out, field, depth + 1);
    }
    for nested in &msg.nested_type {
        write_message(out, nested, depth + 1);
    }
    let _ = writeln!(out, "{indent}}}");
}

fn write_field(out: &mut String, field: &FieldDescriptorProto, depth: usize) {
    let indent = "  ".repeat(depth);
    let ty = proto_type_name(field);
    let name = field.name.as_deref().unwrap_or("unknown");
    let number = field.number.unwrap_or(0);
    // Repeated label prefix (proto3; optional is implicit).
    let label_prefix = match field.label() {
        Label::Repeated => "repeated ",
        Label::Optional | Label::Required => "",
    };
    let _ = writeln!(out, "{indent}{label_prefix}{ty} {name} = {number};");
}

/// Map a `FieldDescriptorProto` to its `.proto` type name.
fn proto_type_name(field: &FieldDescriptorProto) -> String {
    // If type_name is set (enum/message ref), use that (strip leading dot).
    if let Some(ref tn) = field.type_name {
        return tn.trim_start_matches('.').to_string();
    }
    match field.r#type() {
        FieldType::Double => "double",
        FieldType::Float => "float",
        FieldType::Int64 => "int64",
        FieldType::Uint64 => "uint64",
        FieldType::Int32 => "int32",
        FieldType::Fixed64 => "fixed64",
        FieldType::Fixed32 => "fixed32",
        FieldType::Bool => "bool",
        FieldType::String => "string",
        FieldType::Bytes => "bytes",
        FieldType::Uint32 => "uint32",
        FieldType::Sfixed32 => "sfixed32",
        FieldType::Sfixed64 => "sfixed64",
        FieldType::Sint32 => "sint32",
        FieldType::Sint64 => "sint64",
        FieldType::Group | FieldType::Message | FieldType::Enum => "unknown",
    }
    .to_string()
}

pub fn parse(schema: &str) -> Result<ProtobufSchema, SrError> {
    let descriptor = protox_parse::parse("schema.proto", schema)
        .map_err(|e| SrError::InvalidSchema(format!("Protobuf: {e}")))?;
    let normalised = normalize(&descriptor);
    Ok(ProtobufSchema {
        descriptor,
        normalised,
    })
}

impl ProtobufSchema {
    /// Return the normalised `.proto` text (cp-schema-registry compatible).
    #[must_use]
    pub fn normalized_form(&self) -> &str {
        &self.normalised
    }
}

/// Compatibility check. Permissive until slice 2b/2c implement the real rules.
pub fn check(_reader: &str, _writer: &str) -> Result<(), Vec<String>> {
    Ok(())
}

impl ParsedSchema for ProtobufSchema {
    fn canonical_form(&self) -> String {
        // Clone the descriptor, clear source_code_info (formatting/comments)
        // and the file name so neither whitespace nor the synthetic filename
        // affects the dedup key. Then prost-encode to bytes and hex-encode.
        // NOTE: this is a descriptor-bytes key, not Confluent canonical form
        // (which is slice 2+).
        tracing::debug!(
            "protobuf canonical_form: using descriptor-bytes key (not Confluent canonical form; slice 2+)"
        );
        let mut d = self.descriptor.clone();
        d.source_code_info = None;
        d.name = None;
        // hex-encode for a printable, stable string (lowercase, like `{:02x}`)
        hex::encode(d.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ParsedSchema;

    #[test]
    fn check_is_permissive_for_now() {
        assert!(check("anything", "anything else").is_ok());
    }

    const P: &str = "syntax = \"proto3\"; message User { int32 id = 1; }";

    #[test]
    fn parses_and_is_stable() {
        let a = parse(P).unwrap();
        let b = parse("syntax = \"proto3\";\nmessage User {\n  int32 id = 1;\n}\n").unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form());
    }

    #[test]
    fn rejects_invalid_proto() {
        assert!(parse("this is not protobuf").is_err());
    }
}
