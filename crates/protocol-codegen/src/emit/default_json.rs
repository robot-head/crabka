//! Emit `pub fn default_json(version: i16) -> serde_json::Value` per message.
//!
//! The emitted function produces a `serde_json::Value` whose shape mirrors
//! what Kafka's `*DataJsonConverter.read(json, version)` expects for the
//! default state of a message — i.e. the JSON the JVM oracle should accept
//! to yield the same bytes as `MessageName::default()` after encoding.
//!
//! The function is version-aware: it only includes fields that are valid for
//! the requested version, preventing the JVM converter from rejecting unknown
//! fields on older versions.

use std::fmt::Write;

use crate::ir::{FieldSpec, MessageSpec};

/// Emit the `default_json(version: i16)` function body for the given message.
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
    writeln!(out, "/// Only includes fields valid for the given version.").unwrap();
    writeln!(out, "#[must_use]").unwrap();
    writeln!(
        out,
        "pub fn default_json(version: i16) -> ::serde_json::Value {{"
    )
    .unwrap();
    writeln!(out, "    let mut obj = ::serde_json::Map::new();").unwrap();
    emit_fields_stmts(&mut out, &spec.fields, "    ");
    writeln!(out, "    ::serde_json::Value::Object(obj)").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

/// Emit `obj.insert(...)` statements for each field, guarded by version range checks.
fn emit_fields_stmts(out: &mut String, fields: &[FieldSpec], indent: &str) {
    for f in fields {
        let ver_min = f.versions.min;
        let ver_max = f.versions.max;
        let key = json_field_name(&f.name);
        let val_expr = json_value_expr(f);
        if ver_max == i16::MAX {
            // Open-ended: "vN+"
            if ver_min == 0 {
                // Always valid.
                writeln!(
                    out,
                    "{indent}obj.insert(\"{key}\".to_string(), {val_expr});"
                )
                .unwrap();
            } else {
                writeln!(out, "{indent}if version >= {ver_min} {{").unwrap();
                writeln!(
                    out,
                    "{indent}    obj.insert(\"{key}\".to_string(), {val_expr});"
                )
                .unwrap();
                writeln!(out, "{indent}}}").unwrap();
            }
        } else {
            writeln!(
                out,
                "{indent}if version >= {ver_min} && version <= {ver_max} {{"
            )
            .unwrap();
            writeln!(
                out,
                "{indent}    obj.insert(\"{key}\".to_string(), {val_expr});"
            )
            .unwrap();
            writeln!(out, "{indent}}}").unwrap();
        }
    }
}

/// Generate a Rust expression that produces the default `serde_json::Value` for this field.
fn json_value_expr(f: &FieldSpec) -> String {
    let is_array = f.field_type.starts_with("[]");
    let is_nullable = f.nullable_versions.is_some();

    // Check if the default annotation indicates null.
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");

    if is_nullable && (default_is_null || f.default.is_none()) {
        return "::serde_json::Value::Null".into();
    }

    if is_array {
        return "::serde_json::Value::Array(vec![])".into();
    }

    match &f.default {
        Some(v) => scalar_value_expr(&f.field_type, v, f),
        None => type_zero_expr(&f.field_type, f),
    }
}

/// Convert a schema `default` annotation to a Rust expression producing a `serde_json::Value`.
fn scalar_value_expr(field_type: &str, val: &serde_json::Value, f: &FieldSpec) -> String {
    let base = base_type(field_type);

    match val {
        serde_json::Value::String(s) if s == "null" => "::serde_json::Value::Null".into(),

        serde_json::Value::String(s) if is_numeric_type(base) => {
            // Convert hex string defaults (e.g. "0x7fffffff") to decimal.
            let trimmed = s.trim();
            let decimal = if let Some(hex_str) =
                trimmed.strip_prefix("0x").or_else(|| trimmed.strip_prefix("0X"))
            {
                match i64::from_str_radix(hex_str, 16) {
                    Ok(n) => n.to_string(),
                    Err(_) => trimmed.to_string(),
                }
            } else {
                trimmed.to_string()
            };
            // Produce a json! Number value.
            format!("::serde_json::json!({decimal})")
        }

        serde_json::Value::String(s) if base == "bool" => {
            if s == "true" {
                "::serde_json::Value::Bool(true)".into()
            } else {
                "::serde_json::Value::Bool(false)".into()
            }
        }

        serde_json::Value::String(s) if base == "string" => {
            // Escape the string for embedding in Rust source.
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            format!("::serde_json::Value::String(\"{escaped}\".to_string())")
        }

        serde_json::Value::Number(n) => {
            format!("::serde_json::json!({n})")
        }

        serde_json::Value::Bool(b) => {
            format!("::serde_json::Value::Bool({b})")
        }

        _ => type_zero_expr(field_type, f),
    }
}

/// The "zero" `serde_json::Value` expression for a type.
fn type_zero_expr(field_type: &str, f: &FieldSpec) -> String {
    let base = base_type(field_type);
    match base {
        "bool" => "::serde_json::Value::Bool(false)".into(),
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" => {
            "::serde_json::json!(0)".into()
        }
        "float64" => "::serde_json::json!(0.0)".into(),
        "string" | "bytes" | "records" => {
            "::serde_json::Value::String(String::new())".into()
        }
        "uuid" => {
            "::serde_json::Value::String(\"00000000-0000-0000-0000-000000000000\".to_string())"
                .into()
        }
        _ => {
            if f.fields.is_empty() {
                "::serde_json::Value::Object(::serde_json::Map::new())".into()
            } else {
                // Nested struct: build an object with its sub-fields.
                // We emit a block that creates the sub-map at runtime.
                emit_nested_struct_expr(&f.fields)
            }
        }
    }
}

/// Emit a Rust expression that builds a `serde_json::Value::Object` for a nested struct.
fn emit_nested_struct_expr(fields: &[FieldSpec]) -> String {
    let mut s = String::new();
    s.push_str("{ let mut _m = ::serde_json::Map::new(); ");
    for f in fields {
        let key = json_field_name(&f.name);
        let val = json_value_expr(f);
        write!(s, "_m.insert(\"{key}\".to_string(), {val}); ").unwrap();
    }
    s.push_str("::serde_json::Value::Object(_m) }");
    s
}

/// Convert a Kafka schema field name (PascalCase like "TransactionalId") to
/// the JSON key used by the JVM's *DataJsonConverter (camelCase like
/// "transactionalId" — lowercase the first character).
fn json_field_name(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
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
