//! Schema formats: parse + canonical form (the dedup key). Slice 1 does no
//! compatibility checking (that is slice 2); canonical form is needed now for
//! global-id deduplication.

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

/// A successfully-parsed schema. `canonical_form()` is a stable string used as
/// the global-id dedup key; identical schemas (modulo formatting) collide.
pub trait ParsedSchema {
    fn canonical_form(&self) -> String;
}

/// Parse `schema` as `ty`, returning a boxed parsed form or `SrError::InvalidSchema`.
pub fn parse(ty: SchemaType, schema: &str) -> Result<Box<dyn ParsedSchema>, SrError> {
    match ty {
        SchemaType::Avro => avro::parse(schema).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Json => json::parse(schema).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Protobuf => {
            protobuf::parse(schema).map(|p| Box::new(p) as Box<dyn ParsedSchema>)
        }
    }
}

/// Parse `schema` as `ty` and return the normalised storage form.
/// For AVRO and JSON, the raw input is returned (cp-schema-registry echoes
/// them verbatim). For Protobuf, a pretty-printed canonical text is returned
/// matching the format cp-schema-registry produces.
pub fn normalized_storage_form(ty: SchemaType, schema: &str) -> Result<String, SrError> {
    match ty {
        SchemaType::Avro | SchemaType::Json => {
            // Validate (returns Err on bad input) but keep the raw string.
            parse(ty, schema)?;
            Ok(schema.to_string())
        }
        SchemaType::Protobuf => {
            let p = protobuf::parse(schema)?;
            Ok(p.normalized_form().to_string())
        }
    }
}

/// Directional compatibility check: can a reader using `reader` read data
/// written with `writer`, per format `ty`? `Err(messages)` on incompatibility.
/// Avro is real (apache-avro); Protobuf/JSON are permissive until 2b/2c.
pub fn check(ty: SchemaType, reader: &str, writer: &str) -> Result<(), Vec<String>> {
    match ty {
        SchemaType::Avro => avro::check(reader, writer),
        SchemaType::Protobuf => protobuf::check(reader, writer),
        SchemaType::Json => json::check(reader, writer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_type_wire_names() {
        assert_eq!(SchemaType::Avro.wire_name(), None);
        assert_eq!(SchemaType::Protobuf.wire_name(), Some("PROTOBUF"));
        assert_eq!(SchemaType::Json.wire_name(), Some("JSON"));
        assert_eq!(SchemaType::from_wire(None), SchemaType::Avro);
        assert_eq!(
            SchemaType::from_wire(Some("PROTOBUF")),
            SchemaType::Protobuf
        );
    }

    #[test]
    fn avro_parses_and_dedups_by_canonical_form() {
        let a = parse(
            SchemaType::Avro,
            r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#,
        )
        .unwrap();
        let b = parse(
            SchemaType::Avro,
            "{ \"type\":\"record\", \"name\":\"U\", \"fields\":[ {\"name\":\"id\",\"type\":\"int\"} ] }",
        )
        .unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form());
        let c = parse(
            SchemaType::Avro,
            r#"{"type":"record","name":"V","fields":[]}"#,
        )
        .unwrap();
        assert_ne!(a.canonical_form(), c.canonical_form());
    }

    #[test]
    fn avro_rejects_invalid() {
        assert!(parse(SchemaType::Avro, "{not avro}").is_err());
    }
}
