//! Emit `pub fn default_json() -> serde_json::Value` per message.
//!
//! The emitted function produces a `serde_json::Value` whose shape mirrors
//! what Kafka's `*DataJsonConverter.read(json, version)` expects for the
//! default state of a message — i.e. the JSON the JVM oracle should accept
//! to yield the same bytes as `MessageName::default()` after encoding.

use std::fmt::Write;

use crate::ir::{FieldSpec, MessageSpec};

/// Emit the `default_json()` function body for the given message.
/// The output is plain Rust source intended to be appended to the
/// per-message owned module body.
#[must_use]
pub fn emit_default_json(spec: &MessageSpec) -> String {
    let mut out = String::new();
    writeln!(out).unwrap();
    writeln!(
        out,
        "/// Default JSON payload matching `Self::default()` for JVM oracle differential testing."
    )
    .unwrap();
    writeln!(out, "#[must_use]").unwrap();
    writeln!(out, "pub fn default_json() -> ::serde_json::Value {{").unwrap();
    writeln!(
        out,
        "    ::serde_json::json!({})",
        emit_object(&spec.fields)
    )
    .unwrap();
    writeln!(out, "}}").unwrap();
    out
}

/// Emit a JSON object literal for the given fields, suitable for embedding
/// in the `serde_json::json!({...})` macro.
fn emit_object(fields: &[FieldSpec]) -> String {
    let mut s = String::new();
    s.push('{');
    let mut first = true;
    for f in fields {
        if !first {
            s.push_str(", ");
        }
        first = false;
        // Field names in Kafka JSON are camelCase as written in the schema.
        write!(s, "\"{}\": {}", f.name, json_value_for(f)).unwrap();
    }
    s.push('}');
    s
}

/// Produce a Rust `json!({...})` compatible literal for a field's default value.
///
/// This must mirror `owned_default_expr` in `emit/owned.rs` so that the JSON
/// representation matches what `MessageName::default()` produces when encoded.
fn json_value_for(f: &FieldSpec) -> String {
    let is_array = f.field_type.starts_with("[]");
    let is_nullable = f.nullable_versions.is_some();

    // Handle "null" default (either JSON null or the string "null" in schema).
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");

    if is_nullable && (default_is_null || f.default.is_none()) {
        return "null".into();
        // Non-null default for nullable field falls through to value emission.
    }

    if is_array {
        // Arrays always default to empty (JSON null for nullable arrays that
        // default to null is handled above).
        return "[]".into();
    }

    match &f.default {
        Some(v) => scalar_json_value(&f.field_type, v, f),
        None => type_zero_json(&f.field_type, f),
    }
}

/// Convert a schema `default` annotation to a JSON literal string.
///
/// Kafka schemas encode defaults in two ways:
/// - As JSON strings (e.g. `"default": "-1"`) — need numeric parsing.
/// - As native JSON values (e.g. `"default": false`, `"default": -1`).
fn scalar_json_value(field_type: &str, val: &serde_json::Value, f: &FieldSpec) -> String {
    let base = base_type(field_type);

    match val {
        // String-encoded "null" → already handled by caller, but guard here.
        serde_json::Value::String(s) if s == "null" => "null".into(),

        // String-encoded numbers (the most common Kafka schema pattern).
        serde_json::Value::String(s) if is_numeric_type(base) => {
            // Emit the number as a bare literal (no quotes) so json!({}) treats it as a number.
            s.trim().to_string()
        }

        // String-encoded booleans.
        serde_json::Value::String(s) if base == "bool" => {
            if s == "true" {
                "true".into()
            } else {
                "false".into()
            }
        }

        // Actual string default (e.g. for string-typed fields with a real default).
        serde_json::Value::String(s) if base == "string" => {
            // Produce a JSON string literal with proper escaping.
            format!("{}", serde_json::Value::String(s.clone()))
        }

        // Native JSON number.
        serde_json::Value::Number(n) => n.to_string(),

        // Native JSON bool.
        serde_json::Value::Bool(b) => b.to_string(),

        // Anything else: fall back to type zero.
        _ => type_zero_json(field_type, f),
    }
}

/// The "zero" JSON value for a type (when no schema default is specified).
fn type_zero_json(field_type: &str, f: &FieldSpec) -> String {
    let base = base_type(field_type);
    match base {
        "bool" => "false".into(),
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" => "0".into(),
        "float64" => "0.0".into(),
        "string" | "bytes" | "records" => "\"\"".into(),
        "uuid" => "\"00000000-0000-0000-0000-000000000000\"".into(),
        _ => {
            // Nested struct: recurse into sub-fields if present; otherwise empty object.
            if f.fields.is_empty() {
                // Unknown/common struct type without inline fields — emit empty object.
                // The differential test will catch any mismatch.
                "{}".into()
            } else {
                emit_object(&f.fields)
            }
        }
    }
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_numeric_type(t: &str) -> bool {
    matches!(
        t,
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" | "float64"
    )
}
