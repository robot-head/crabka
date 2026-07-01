//! Avro: parse + Parsing Canonical Form via `apache-avro`. Also directional
//! compatibility check via [`apache_avro::schema_compatibility::SchemaCompatibility`].

use apache_avro::schema_compatibility::SchemaCompatibility;

use super::ParsedSchema;
use crate::error::SrError;

pub struct AvroSchema(apache_avro::Schema);

pub fn parse(schema: &str, refs: &[super::ResolvedReference]) -> Result<AvroSchema, SrError> {
    if refs.is_empty() {
        return apache_avro::Schema::parse_str(schema)
            .map(AvroSchema)
            .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")));
    }
    // Dependencies first (so their named types are in scope), candidate last.
    let mut sources: Vec<&str> = refs.iter().map(|r| r.schema.as_str()).collect();
    sources.push(schema);
    let parsed = apache_avro::Schema::parse_list(&sources)
        .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")))?;
    // `parse_list` preserves input order; the candidate is the last entry.
    parsed
        .into_iter()
        .next_back()
        .map(AvroSchema)
        .ok_or_else(|| SrError::InvalidSchema("Avro: empty parse_list".into()))
}

impl ParsedSchema for AvroSchema {
    fn canonical_form(&self) -> String {
        self.0.canonical_form()
    }
}

/// Directional Avro check: can a reader using `reader` read data written with
/// `writer`? `Ok(())` if compatible, else `Err(messages)`.
#[tracing::instrument(level = "debug", name = "avro.check", skip_all, fields(reader_refs = reader_refs.len(), writer_refs = writer_refs.len()))]
pub fn check(
    reader: &str,
    writer: &str,
    reader_refs: &[super::ResolvedReference],
    writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader_schema = parse(reader, reader_refs)
        .map_err(|e| vec![format!("reader: {e}")])?
        .0;
    let writer_schema = parse(writer, writer_refs)
        .map_err(|e| vec![format!("writer: {e}")])?
        .0;
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

    #[test]
    fn avro_resolves_named_reference() {
        use crate::format::ResolvedReference;
        let money = r#"{"type":"record","name":"Money","fields":[{"name":"cents","type":"long"}]}"#;
        let candidate =
            r#"{"type":"record","name":"Order","fields":[{"name":"price","type":"Money"}]}"#;
        // Without the reference, the named type "Money" is unresolved → parse error.
        assert!(parse(candidate, &[]).is_err());
        // With it, parse succeeds.
        let refs = vec![ResolvedReference {
            name: "Money".into(),
            ty: crate::format::SchemaType::Avro,
            schema: money.into(),
        }];
        assert!(parse(candidate, &refs).is_ok());
    }
}
