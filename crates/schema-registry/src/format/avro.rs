//! Avro: parse + Parsing Canonical Form via `apache-avro`. Also directional
//! compatibility check via [`apache_avro::schema_compatibility::SchemaCompatibility`].

use apache_avro::schema_compatibility::SchemaCompatibility;

use super::ParsedSchema;
use crate::error::SrError;

pub struct AvroSchema(apache_avro::Schema);

pub fn parse(schema: &str, _refs: &[super::ResolvedReference]) -> Result<AvroSchema, SrError> {
    apache_avro::Schema::parse_str(schema)
        .map(AvroSchema)
        .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")))
}

impl ParsedSchema for AvroSchema {
    fn canonical_form(&self) -> String {
        self.0.canonical_form()
    }
}

/// Directional Avro check: can a reader using `reader` read data written with
/// `writer`? `Ok(())` if compatible, else `Err(messages)`.
pub fn check(
    reader: &str,
    writer: &str,
    _reader_refs: &[super::ResolvedReference],
    _writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader_schema = apache_avro::Schema::parse_str(reader)
        .map_err(|e| vec![format!("reader schema unparseable: {e}")])?;
    let writer_schema = apache_avro::Schema::parse_str(writer)
        .map_err(|e| vec![format!("writer schema unparseable: {e}")])?;
    SchemaCompatibility::can_read(&writer_schema, &reader_schema).map_err(|e| vec![e.to_string()])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avro_check_directions() {
        let old = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;
        let new = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int","default":0}]}"#;
        assert!(
            check(new, old, &[], &[]).is_ok(),
            "new reads old (BACKWARD) ok"
        );
        assert!(check(old, new, &[], &[]).is_ok());
        let new_nodef = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#;
        assert!(
            check(new_nodef, old, &[], &[]).is_err(),
            "new(reader) cannot read old(writer): missing default"
        );
    }
}
