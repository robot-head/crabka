//! Avro: parse + Parsing Canonical Form via `apache-avro`.

use crate::error::SrError;
use super::ParsedSchema;

pub struct AvroSchema(apache_avro::Schema);

pub fn parse(schema: &str) -> Result<AvroSchema, SrError> {
    apache_avro::Schema::parse_str(schema)
        .map(AvroSchema)
        .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")))
}

impl ParsedSchema for AvroSchema {
    fn canonical_form(&self) -> String {
        self.0.canonical_form()
    }
}
