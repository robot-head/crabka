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

mod compat;
mod diff;

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

    pub(crate) fn descriptor(&self) -> &FileDescriptorProto {
        &self.descriptor
    }
}

/// Confluent Protobuf compatibility: can a reader using `reader` read data
/// written with `writer`? Computes the structural diff (original = writer,
/// update = reader) and rejects if any difference is backward-incompatible.
pub fn check(reader: &str, writer: &str) -> Result<(), Vec<String>> {
    let reader_d = parse(reader).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_d = parse(writer).map_err(|e| vec![format!("writer: {e}")])?;
    let diffs = diff::compare(writer_d.descriptor(), reader_d.descriptor());
    let incompatible: Vec<&diff::Difference> = diffs
        .iter()
        .filter(|d| !compat::is_backward_compatible(&d.kind))
        .collect();
    if incompatible.is_empty() {
        Ok(())
    } else {
        Err(compat::messages(&incompatible))
    }
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

    fn p(body: &str) -> String {
        format!("syntax = \"proto3\"; message U {{ {body} }}")
    }

    #[test]
    fn field_added_is_backward_compatible() {
        assert!(check(&p("int32 id = 1; int32 x = 2;"), &p("int32 id = 1;")).is_ok());
    }

    #[test]
    fn field_removed_is_backward_compatible() {
        assert!(check(&p("int32 id = 1;"), &p("int32 id = 1; int32 x = 2;")).is_ok());
    }

    #[test]
    fn scalar_change_within_group_ok_across_group_bad() {
        assert!(check(&p("int64 id = 1;"), &p("int32 id = 1;")).is_ok());
        assert!(check(&p("string id = 1;"), &p("int32 id = 1;")).is_err());
    }

    #[test]
    fn label_change_is_incompatible() {
        assert!(check(&p("repeated int32 id = 1;"), &p("int32 id = 1;")).is_err());
    }

    #[test]
    fn kind_change_scalar_to_message_is_incompatible() {
        let w = "syntax = \"proto3\"; message U { int32 id = 1; }";
        let r = "syntax = \"proto3\"; message M {} message U { M id = 1; }";
        assert!(check(r, w).is_err());
    }

    // ── Task 2: oneof rules ───────────────────────────────────────────────────

    #[test]
    fn moving_field_into_oneof_does_not_panic() {
        let w = "syntax = \"proto3\"; message U { int32 a = 1; int32 b = 2; }";
        let r = "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }";
        let _ = check(r, w); // verdict calibrated vs cp in Task 6; must not panic
    }

    #[test]
    fn proto3_optional_is_not_a_oneof_change() {
        let w = "syntax = \"proto3\"; message U { int32 a = 1; }";
        let r = "syntax = \"proto3\"; message U { optional int32 a = 1; }";
        assert!(
            check(r, w).is_ok(),
            "proto3 optional is not a oneof migration"
        );
    }
}
