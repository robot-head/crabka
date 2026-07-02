use thiserror::Error;

use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, MessageType};

#[derive(Debug, Error)]
pub enum ValidateError {
    #[error("{message}: in {context}")]
    Unsupported {
        message: &'static str,
        context: String,
    },
}

/// Field types the generator currently understands. Anything else is a hard error.
/// Verified against the full Kafka 4.2.0 schema corpus (197 files).
/// All field-level primitive types found: bool, bytes, float64, int16, int32, int64,
/// int8, records, string, uint16, uuid.
/// `uint32` and `float32` are listed here for completeness (appear in older/future
/// schemas) but are not present in the 4.2.0 corpus.
const KNOWN_PRIMITIVE_TYPES: &[&str] = &[
    "bool", "int8", "int16", "int32", "int64", "uint16", "uint32", "float64", "string", "bytes",
    "uuid", "records",
];

pub fn validate(specs: &[MessageSpec]) -> Result<(), ValidateError> {
    for spec in specs {
        let ctx = spec.name.clone();
        if matches!(
            spec.message_type,
            MessageType::Request | MessageType::Response
        ) && spec.api_key.is_none()
        {
            return Err(ValidateError::Unsupported {
                message: "request/response missing apiKey",
                context: ctx,
            });
        }
        validate_fields(&spec.fields, spec.flexible_versions, &ctx)?;
        for cs in &spec.common_structs {
            validate_fields(
                &cs.fields,
                spec.flexible_versions,
                &format!("{ctx}.{}", cs.name),
            )?;
        }
    }
    Ok(())
}

fn validate_fields(
    fields: &[FieldSpec],
    flexible: FlexibleVersions,
    ctx: &str,
) -> Result<(), ValidateError> {
    for f in fields {
        let context = format!("{ctx}.{}", f.name);
        let base = base_type(&f.field_type);

        let known = KNOWN_PRIMITIVE_TYPES.contains(&base)
            || base.starts_with("[]")   // arrays (nested [] not stripped by base_type)
            || is_struct_type(base); // struct reference like `MetadataRequestTopic`

        if !known {
            return Err(ValidateError::Unsupported {
                message: "unknown field type",
                context,
            });
        }

        if f.tag.is_some() && !is_some_flexible(flexible) {
            return Err(ValidateError::Unsupported {
                message: "tagged field on non-flexible message",
                context,
            });
        }

        if !f.fields.is_empty() {
            validate_fields(&f.fields, flexible, &context)?; // flexible is Copy
        }
    }
    Ok(())
}

fn is_some_flexible(f: FlexibleVersions) -> bool {
    matches!(f, FlexibleVersions::Range(_))
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_struct_type(t: &str) -> bool {
    // Kafka schema convention: struct types are PascalCase identifiers.
    t.chars().next().is_some_and(char::is_uppercase)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::ir;

    #[test]
    fn vendored_schemas_validate() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("protocol")
            .join("schemas");
        let specs = ir::load_dir(&dir).unwrap();
        // If this test fails, the generator needs an update before we can target
        // this Kafka release — surface the offending schema clearly.
        validate(&specs).unwrap_or_else(|e| panic!("validation failed: {e}"));
    }
}
