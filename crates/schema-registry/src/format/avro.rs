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
        let new_nodef = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#;
        for (name, reader, writer, compatible) in [
            ("new_reads_old", new, old, true),
            ("old_reads_new", old, new, true),
            ("missing_default", new_nodef, old, false),
        ] {
            assert_eq!(
                check(reader, writer, &[], &[]).is_ok(),
                compatible,
                "case {name}"
            );
        }
    }

    #[test]
    fn avro_resolves_named_reference() {
        use crate::format::ResolvedReference;
        let money = r#"{"type":"record","name":"Money","fields":[{"name":"cents","type":"long"}]}"#;
        let candidate =
            r#"{"type":"record","name":"Order","fields":[{"name":"price","type":"Money"}]}"#;
        let refs = vec![ResolvedReference {
            name: "Money".into(),
            ty: crate::format::SchemaType::Avro,
            schema: money.into(),
        }];
        for (name, refs, valid) in [
            ("unresolved", &[][..], false),
            ("resolved", refs.as_slice(), true),
        ] {
            assert_eq!(parse(candidate, refs).is_ok(), valid, "case {name}");
        }
    }
}
