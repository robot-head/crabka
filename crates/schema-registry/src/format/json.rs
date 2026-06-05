//! JSON Schema: parse as JSON + well-formedness; canonical form = recursively
//! key-sorted compact JSON (the dedup key). Compatibility is slice 2.

use crate::error::SrError;
use super::ParsedSchema;

pub struct JsonSchema(serde_json::Value);

pub fn parse(schema: &str) -> Result<JsonSchema, SrError> {
    let v: serde_json::Value =
        serde_json::from_str(schema).map_err(|e| SrError::InvalidSchema(format!("JSON Schema: {e}")))?;
    if !v.is_object() && !v.is_boolean() {
        return Err(SrError::InvalidSchema(
            "JSON Schema must be an object or boolean".into(),
        ));
    }
    Ok(JsonSchema(v))
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
            format!("[{}]", a.iter().map(canonicalize).collect::<Vec<_>>().join(","))
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
        let a = parse(r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#).unwrap();
        let b = parse(r#"{"properties":{"b":{"type":"string"},"a":{"type":"integer"}},"type":"object"}"#).unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form());
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse("not json").is_err());
    }
}
