//! JSON Schema: parse as JSON + well-formedness; canonical form = recursively
//! key-sorted compact JSON (the dedup key). Compatibility is slice 2.

mod compat;
mod diff;

use super::ParsedSchema;
use crate::error::SrError;

pub struct JsonSchema(serde_json::Value);

impl JsonSchema {
    pub(crate) fn value(&self) -> &serde_json::Value {
        &self.0
    }
}

pub fn parse(schema: &str) -> Result<JsonSchema, SrError> {
    let v: serde_json::Value = serde_json::from_str(schema)
        .map_err(|e| SrError::InvalidSchema(format!("JSON Schema: {e}")))?;
    if !v.is_object() && !v.is_boolean() {
        return Err(SrError::InvalidSchema(
            "JSON Schema must be an object or boolean".into(),
        ));
    }
    Ok(JsonSchema(v))
}

/// Confluent JSON Schema compatibility: can a reader using `reader` read data
/// written with `writer`? Diffs (original = writer, update = reader); rejects if
/// any difference is backward-incompatible.
pub fn check(reader: &str, writer: &str) -> Result<(), Vec<String>> {
    let reader_s = parse(reader).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_s = parse(writer).map_err(|e| vec![format!("writer: {e}")])?;
    let diffs = diff::compare(writer_s.value(), reader_s.value());
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

impl ParsedSchema for JsonSchema {
    fn canonical_form(&self) -> String {
        canonicalize(&self.0)
    }
}

fn canonicalize(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonicalize(&map[*k])
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        serde_json::Value::Array(a) => {
            format!(
                "[{}]",
                a.iter().map(canonicalize).collect::<Vec<_>>().join(",")
            )
        }
        other => serde_json::to_string(other).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ParsedSchema;

    #[test]
    fn parses_object_and_dedups_key_order() {
        let a = parse(
            r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#,
        )
        .unwrap();
        let b = parse(
            r#"{"properties":{"b":{"type":"string"},"a":{"type":"integer"}},"type":"object"}"#,
        )
        .unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form());
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse("not json").is_err());
    }

    #[test]
    fn add_optional_property_open_model_is_compatible() {
        let w = r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#;
        assert!(check(r, w).is_ok());
    }

    #[test]
    fn add_required_property_closed_model_is_incompatible() {
        let w = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"string"}},"required":["b"]}"#;
        assert!(check(r, w).is_err());
    }

    #[test]
    fn type_narrowed_is_incompatible() {
        let w = r#"{"type":["string","null"]}"#;
        let r = r#"{"type":"string"}"#;
        assert!(check(r, w).is_err());
    }

    #[test]
    fn required_added_is_incompatible() {
        let w = r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"]}"#;
        assert!(check(r, w).is_err());
    }
}
