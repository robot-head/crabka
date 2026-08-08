//! Schema formats: parse, canonical storage form, and directional
//! compatibility checks.
//!
//! The canonical form is the global-id deduplication key.

pub mod avro;
pub mod json;
pub mod protobuf;

use crate::error::SrError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Avro,
    Protobuf,
    Json,
}

impl SchemaType {
    #[must_use]
    pub fn wire_name(self) -> Option<&'static str> {
        match self {
            Self::Avro => None,
            Self::Protobuf => Some("PROTOBUF"),
            Self::Json => Some("JSON"),
        }
    }

    #[must_use]
    pub fn from_wire(s: Option<&str>) -> Self {
        match s {
            None | Some("" | "AVRO") => Self::Avro,
            Some("PROTOBUF") => Self::Protobuf,
            _ => Self::Json,
        }
    }
}

/// A successfully-parsed schema. `canonical_form()` is a stable string that
/// serves as the global-id dedup key. Two schemas that differ only in
/// formatting collide.
pub trait ParsedSchema {
    fn canonical_form(&self) -> String;
}

/// A referenced schema resolved from the store, ready to feed a format parser.
/// `name` is the format-specific reference label: a Protobuf import path, an
/// Avro type name, or a JSON `$ref` target. `ty` and `schema` are the
/// referenced version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedReference {
    pub name: String,
    pub ty: SchemaType,
    pub schema: String,
}

/// Parse `schema` as `ty` with its resolved references available. Returns a
/// boxed parsed form, or `SrError::InvalidSchema`.
#[tracing::instrument(level = "debug", name = "format.parse", skip_all, fields(schema_type = ?ty, refs = refs.len()), err)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn parse(
    ty: SchemaType,
    schema: &str,
    refs: &[ResolvedReference],
) -> Result<Box<dyn ParsedSchema>, SrError> {
    match ty {
        SchemaType::Avro => avro::parse(schema, refs).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Json => json::parse(schema, refs).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Protobuf => {
            protobuf::parse(schema, refs).map(|p| Box::new(p) as Box<dyn ParsedSchema>)
        }
    }
}

/// Parse `schema` as `ty` and return the normalised storage form.
///
/// For AVRO and JSON, this function returns the raw input, because
/// cp-schema-registry echoes them verbatim. For Protobuf, it returns a
/// pretty-printed canonical text that matches the format cp-schema-registry
/// produces.
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn normalized_storage_form(
    ty: SchemaType,
    schema: &str,
    refs: &[ResolvedReference],
) -> Result<String, SrError> {
    match ty {
        SchemaType::Avro | SchemaType::Json => {
            // Validate (returns Err on bad input) but keep the raw string.
            parse(ty, schema, refs)?;
            Ok(schema.to_string())
        }
        SchemaType::Protobuf => {
            let p = protobuf::parse(schema, refs)?;
            Ok(p.normalized_form().to_string())
        }
    }
}

/// Directional compatibility check. It answers whether a reader that uses
/// `reader` can read data written with `writer`, per format `ty`. It returns
/// `Err(messages)` on incompatibility. `apache-avro` backs the Avro check.
/// The Protobuf and JSON Schema checks are permissive.
#[tracing::instrument(level = "debug", name = "format.check", skip_all, fields(schema_type = ?ty, reader_refs = reader_refs.len(), writer_refs = writer_refs.len()))]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub fn check(
    ty: SchemaType,
    reader: &str,
    writer: &str,
    reader_refs: &[ResolvedReference],
    writer_refs: &[ResolvedReference],
) -> Result<(), Vec<String>> {
    match ty {
        SchemaType::Avro => avro::check(reader, writer, reader_refs, writer_refs),
        SchemaType::Protobuf => protobuf::check(reader, writer, reader_refs, writer_refs),
        SchemaType::Json => json::check(reader, writer, reader_refs, writer_refs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_type_wire_names() {
        for (_name, ty, wire) in [
            ("avro_default", SchemaType::Avro, None),
            ("protobuf", SchemaType::Protobuf, Some("PROTOBUF")),
            ("json", SchemaType::Json, Some("JSON")),
        ] {
            assert2::assert!(ty.wire_name() == wire);
            assert2::assert!(SchemaType::from_wire(wire) == ty);
        }
    }

    #[test]
    fn avro_parses_and_dedups_by_canonical_form() {
        let a = parse(
            SchemaType::Avro,
            r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#,
            &[],
        )
        .unwrap();
        let b = parse(
            SchemaType::Avro,
            "{ \"type\":\"record\", \"name\":\"U\", \"fields\":[ {\"name\":\"id\",\"type\":\"int\"} ] }",
            &[],
        )
        .unwrap();
        let a_form = a.canonical_form();
        let b_form = b.canonical_form();
        let c = parse(
            SchemaType::Avro,
            r#"{"type":"record","name":"V","fields":[]}"#,
            &[],
        )
        .unwrap();
        assert2::assert!(a_form == b_form);
        assert2::assert!(a_form != c.canonical_form());
    }

    #[test]
    fn avro_rejects_invalid() {
        assert2::assert!(parse(SchemaType::Avro, "{not avro}", &[]).is_err());
    }
}
