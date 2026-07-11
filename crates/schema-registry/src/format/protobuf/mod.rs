//! Protobuf: parse a single `.proto` source into a `FileDescriptorProto`; dedup
//! key = deterministic prost encoding of the descriptor (source-info cleared so
//! formatting doesn't change the bytes). The implementation does not attempt
//! Confluent's full canonicalization rules.
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

use prost_reflect::{
    DescriptorPool,
    prost::Message,
    prost_types::{
        DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto,
        FileDescriptorSet, ServiceDescriptorProto,
        field_descriptor_proto::{Label, Type as FieldType},
    },
};

use super::ParsedSchema;
use crate::error::SrError;

pub struct ProtobufSchema {
    descriptor: FileDescriptorProto,
    /// Normalised `.proto` text (cp-schema-registry compatible pretty-print).
    normalised: String,
}

/// Return the normalised `.proto` text for a `FileDescriptorProto`, matching
/// the format cp-schema-registry uses when echoing schemas back.
///
/// Format (verified against cp-schema-registry 7.4.0):
/// ```text
/// syntax = "proto3";
/// package m;
///
/// import "money.proto";
///
/// message Order {
///   m.Money price = 1;
/// }
/// ```
/// The `package` line (when present) follows the `syntax` line with no blank
/// line between them; each `import` and each top-level `message` is then a
/// blank-line-separated block.
#[must_use]
pub fn normalize(fdp: &FileDescriptorProto) -> String {
    let mut out = String::new();

    // Emit syntax line; cp-schema-registry always emits `syntax = "proto3";\n`
    // even if the proto2 syntax is used. This implementation accepts proto3.
    let syntax = fdp.syntax.as_deref().unwrap_or("proto3");
    let _ = writeln!(out, "syntax = \"{syntax}\";");

    // Package declaration (if any) directly under the syntax line — cp emits it
    // with no intervening blank line. Keeping it is required for cross-file
    // type resolution (an imported `m.Money` only links if `package m;` survives).
    if let Some(pkg) = fdp.package.as_deref().filter(|p| !p.is_empty()) {
        let _ = writeln!(out, "package {pkg};");
    }

    // Imports, each a blank-line-separated block. The reference `name` IS the
    // import path, so preserving these is what links resolved references.
    for dep in &fdp.dependency {
        out.push('\n');
        let _ = writeln!(out, "import \"{dep}\";");
    }

    let package = fdp.package.as_deref().unwrap_or("");

    for en in &fdp.enum_type {
        out.push('\n');
        write_enum(&mut out, en, 0);
    }
    for msg in &fdp.message_type {
        out.push('\n');
        write_message(&mut out, msg, 0, package);
    }
    for service in &fdp.service {
        out.push('\n');
        write_service(&mut out, service, package);
    }
    out
}

fn write_message(out: &mut String, msg: &DescriptorProto, depth: usize, package: &str) {
    let indent = "  ".repeat(depth);
    let name = msg.name.as_deref().unwrap_or("Unknown");
    let _ = writeln!(out, "{indent}message {name} {{");
    for en in &msg.enum_type {
        write_enum(out, en, depth + 1);
    }
    for field in &msg.field {
        write_field(out, field, depth + 1, package);
    }
    for nested in &msg.nested_type {
        write_message(out, nested, depth + 1, package);
    }
    let _ = writeln!(out, "{indent}}}");
}

fn write_enum(out: &mut String, en: &EnumDescriptorProto, depth: usize) {
    let indent = "  ".repeat(depth);
    let name = en.name.as_deref().unwrap_or("Unknown");
    let _ = writeln!(out, "{indent}enum {name} {{");
    for value in &en.value {
        let value_name = value.name.as_deref().unwrap_or("UNKNOWN");
        let number = value.number.unwrap_or(0);
        let _ = writeln!(out, "{indent}  {value_name} = {number};");
    }
    let _ = writeln!(out, "{indent}}}");
}

fn write_field(out: &mut String, field: &FieldDescriptorProto, depth: usize, package: &str) {
    let indent = "  ".repeat(depth);
    let ty = proto_type_name(field, package);
    let name = field.name.as_deref().unwrap_or("unknown");
    let number = field.number.unwrap_or(0);
    // Repeated label prefix (proto3; optional is implicit).
    let label_prefix = match field.label() {
        Label::Repeated => "repeated ",
        Label::Optional | Label::Required => "",
    };
    let _ = writeln!(out, "{indent}{label_prefix}{ty} {name} = {number};");
}

fn write_service(out: &mut String, service: &ServiceDescriptorProto, package: &str) {
    let name = service.name.as_deref().unwrap_or("Unknown");
    let _ = writeln!(out, "service {name} {{");
    for method in &service.method {
        let method_name = method.name.as_deref().unwrap_or("Unknown");
        let input_prefix = if method.client_streaming.unwrap_or(false) {
            "stream "
        } else {
            ""
        };
        let output_prefix = if method.server_streaming.unwrap_or(false) {
            "stream "
        } else {
            ""
        };
        let input = proto_ref_name(method.input_type.as_deref().unwrap_or("Unknown"), package);
        let output = proto_ref_name(method.output_type.as_deref().unwrap_or("Unknown"), package);
        let _ = writeln!(
            out,
            "  rpc {method_name} ({input_prefix}{input}) returns ({output_prefix}{output});"
        );
    }
    let _ = writeln!(out, "}}");
}

/// Map a `FieldDescriptorProto` to its `.proto` type name.
fn proto_type_name(field: &FieldDescriptorProto, package: &str) -> String {
    // If type_name is set (enum/message ref), use that (strip leading dot).
    if let Some(ref tn) = field.type_name {
        return proto_ref_name(tn, package);
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

fn proto_ref_name(name: &str, package: &str) -> String {
    if !package.is_empty()
        && let Some(local) = name.strip_prefix(&format!(".{package}."))
    {
        return local.to_string();
    }
    name.trim_start_matches('.').to_string()
}

pub fn parse(schema: &str, refs: &[super::ResolvedReference]) -> Result<ProtobufSchema, SrError> {
    let descriptor = protox_parse::parse("schema.proto", schema)
        .map_err(|e| SrError::InvalidSchema(format!("Protobuf: {e}")))?;
    // Link the candidate + its (protobuf) references so imports resolve and
    // cross-file types validate. The reference `name` IS the import path.
    // Trigger linking whenever the candidate declares imports (so an unresolved
    // import is caught) or references are supplied.
    if !descriptor.dependency.is_empty() || !refs.is_empty() {
        let mut files: Vec<FileDescriptorProto> = Vec::with_capacity(refs.len() + 1);
        for r in refs.iter().filter(|r| r.ty == super::SchemaType::Protobuf) {
            let dep = protox_parse::parse(&r.name, &r.schema).map_err(|e| {
                SrError::InvalidSchema(format!("Protobuf reference {}: {e}", r.name))
            })?;
            files.push(dep);
        }
        files.push(descriptor.clone());
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: files })
            .map_err(|e| SrError::InvalidSchema(format!("Protobuf link: {e}")))?;
    }
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
#[tracing::instrument(level = "debug", name = "protobuf.check", skip_all, fields(reader_refs = reader_refs.len(), writer_refs = writer_refs.len(), diffs = tracing::field::Empty))]
pub fn check(
    reader: &str,
    writer: &str,
    reader_refs: &[super::ResolvedReference],
    writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader_d = parse(reader, reader_refs).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_d = parse(writer, writer_refs).map_err(|e| vec![format!("writer: {e}")])?;
    let diffs = diff::compare(writer_d.descriptor(), reader_d.descriptor());
    tracing::Span::current().record("diffs", diffs.len());
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
        // which this implementation intentionally does not attempt.
        tracing::debug!(
            "protobuf canonical_form: using descriptor-bytes key (not Confluent canonical form)"
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
        let a = parse(P, &[]).unwrap();
        let b = parse(
            "syntax = \"proto3\";\nmessage User {\n  int32 id = 1;\n}\n",
            &[],
        )
        .unwrap();
        assert2::assert!(a.canonical_form() == b.canonical_form());
    }

    #[test]
    fn rejects_invalid_proto() {
        assert2::assert!(parse("this is not protobuf", &[]).is_err());
    }

    fn p(body: &str) -> String {
        format!("syntax = \"proto3\"; message U {{ {body} }}")
    }

    #[test]
    fn compatibility_basic_cases_are_named_and_table_driven() {
        let plain = "syntax = \"proto3\"; message U { int32 a = 1; int32 b = 2; }";
        let oneof = "syntax = \"proto3\"; message U { oneof x { int32 a = 1; int32 b = 2; } }";
        let small = "syntax = \"proto3\"; message U { int32 id = 1; }";
        for (_name, reader, writer, compatible) in [
            (
                "field-added",
                p("int32 id = 1; int32 x = 2;"),
                p("int32 id = 1;"),
                true,
            ),
            (
                "field-removed",
                p("int32 id = 1;"),
                p("int32 id = 1; int32 x = 2;"),
                true,
            ),
            (
                "scalar-same-wire-group",
                p("int64 id = 1;"),
                p("int32 id = 1;"),
                true,
            ),
            (
                "scalar-cross-wire-group",
                p("string id = 1;"),
                p("int32 id = 1;"),
                false,
            ),
            (
                "singular-to-repeated",
                p("repeated int32 id = 1;"),
                p("int32 id = 1;"),
                true,
            ),
            (
                "scalar-to-message",
                "syntax = \"proto3\"; message M {} message U { M id = 1; }".to_string(),
                small.to_string(),
                false,
            ),
            (
                "move-into-oneof",
                oneof.to_string(),
                plain.to_string(),
                false,
            ),
            (
                "move-out-of-oneof",
                plain.to_string(),
                oneof.to_string(),
                true,
            ),
            (
                "proto3-optional",
                "syntax = \"proto3\"; message U { optional int32 a = 1; }".to_string(),
                "syntax = \"proto3\"; message U { int32 a = 1; }".to_string(),
                true,
            ),
        ] {
            assert2::assert!(check(&reader, &writer, &[], &[]).is_ok() == compatible);
        }
    }

    #[test]
    fn compatibility_extended_cases_are_named_and_table_driven() {
        let small = "syntax = \"proto3\"; message U { int32 id = 1; }";
        let big = "syntax = \"proto3\"; message U { int32 id = 1; } message V { int32 a = 1; }";

        for (_name, reader, writer, compatible) in [
            (
                "reserve-number",
                "syntax = \"proto3\"; message U { reserved 2; int32 id = 1; }".to_string(),
                small.to_string(),
                true,
            ),
            (
                "map-cross-wire-group",
                "syntax = \"proto3\"; message U { map<string, string> m = 1; }".to_string(),
                "syntax = \"proto3\"; message U { map<string, int32> m = 1; }".to_string(),
                false,
            ),
            (
                "identical-map",
                "syntax = \"proto3\"; message U { map<string, int32> m = 1; }".to_string(),
                "syntax = \"proto3\"; message U { map<string, int32> m = 1; }".to_string(),
                true,
            ),
            (
                "enum-constant-added",
                "syntax = \"proto3\"; enum E { A = 0; B = 1; } message U { E e = 1; }".to_string(),
                "syntax = \"proto3\"; enum E { A = 0; } message U { E e = 1; }".to_string(),
                true,
            ),
            (
                "nested-field-cross-group",
                "syntax = \"proto3\"; message U { message N { string a = 1; } N n = 1; }"
                    .to_string(),
                "syntax = \"proto3\"; message U { message N { int32 a = 1; } N n = 1; }"
                    .to_string(),
                false,
            ),
            (
                "package-renamed",
                "syntax = \"proto3\"; package b; message U { int32 id = 1; }".to_string(),
                "syntax = \"proto3\"; package a; message U { int32 id = 1; }".to_string(),
                true,
            ),
            (
                "int-to-enum",
                "syntax = \"proto3\"; enum E { A = 0; } message U { E id = 1; }".to_string(),
                small.to_string(),
                true,
            ),
            (
                "reader-message-added",
                big.to_string(),
                small.to_string(),
                true,
            ),
            (
                "reader-message-removed",
                small.to_string(),
                big.to_string(),
                false,
            ),
        ] {
            assert2::assert!(check(&reader, &writer, &[], &[]).is_ok() == compatible);
        }
    }

    // ── reference resolution ─────────────────────────────────────────────────

    #[test]
    fn protobuf_resolves_import_reference() {
        use crate::format::{ResolvedReference, SchemaType};
        let dep = "syntax = \"proto3\"; package m; message Money { int64 cents = 1; }";
        let candidate =
            "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
        // With the import provided as a reference (name = import path), it links.
        for (_name, refs, expected) in [
            (
                "resolved_import",
                vec![ResolvedReference {
                    name: "money.proto".into(),
                    ty: SchemaType::Protobuf,
                    schema: dep.into(),
                }],
                true,
            ),
            ("unresolved_import", vec![], false),
        ] {
            assert2::assert!(parse(candidate, &refs).is_ok() == expected);
        }
    }

    #[test]
    fn normalize_emits_package_and_imports_cp_exact() {
        use crate::format::{ResolvedReference, SchemaType};
        // Packaged schema: `package` follows `syntax` with no blank line, then a
        // blank line precedes the message (verified against cp-schema-registry 7.4.0).
        let money = "syntax = \"proto3\"; package m; message Money { int64 cents = 1; }";
        // Importing schema: blank line, `import`, blank line, message (cp-exact).
        let order =
            "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
        for (_name, schema, refs, expected) in [
            (
                "package",
                money,
                vec![],
                "syntax = \"proto3\";\npackage m;\n\nmessage Money {\n  int64 cents = 1;\n}\n",
            ),
            (
                "import",
                order,
                vec![ResolvedReference {
                    name: "money.proto".into(),
                    ty: SchemaType::Protobuf,
                    schema: money.into(),
                }],
                "syntax = \"proto3\";\n\nimport \"money.proto\";\n\nmessage Order {\n  m.Money price = 1;\n}\n",
            ),
        ] {
            assert2::assert!(parse(schema, &refs).unwrap().normalized_form() == expected);
        }
    }

    #[test]
    fn normalize_preserves_nested_enum_and_message_indentation() {
        use prost_reflect::prost_types::EnumValueDescriptorProto;

        let fdp = FileDescriptorProto {
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Outer".into()),
                enum_type: vec![EnumDescriptorProto {
                    name: Some("Kind".into()),
                    value: vec![
                        EnumValueDescriptorProto {
                            name: Some("KIND_UNSPECIFIED".into()),
                            number: Some(0),
                            ..Default::default()
                        },
                        EnumValueDescriptorProto {
                            name: Some("KIND_READY".into()),
                            number: Some(1),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                nested_type: vec![DescriptorProto {
                    name: Some("Inner".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("id".into()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(FieldType::String as i32),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(
            normalize(&fdp)
                == "syntax = \"proto3\";\n\nmessage Outer {\n  enum Kind {\n    KIND_UNSPECIFIED = 0;\n    KIND_READY = 1;\n  }\n  message Inner {\n    string id = 1;\n  }\n}\n"
        );
    }

    #[test]
    fn proto_ref_name_does_not_treat_empty_package_as_local_prefix() {
        for (_name, reference, package, expected) in [
            ("empty_package", "...Money", "", "Money"),
            ("local_package", ".m.Money", "m", "Money"),
        ] {
            assert2::assert!(proto_ref_name(reference, package) == expected);
        }
    }
}
