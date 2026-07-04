//! Emit `pub fn default_json(version: i16) -> serde_json::Value` per message.
//!
//! The emitted function produces a `serde_json::Value` whose shape mirrors
//! what Kafka's `*DataJsonConverter.read(json, version)` expects for the
//! default state of a message — i.e. the JSON the JVM oracle should accept
//! to yield the same bytes as `MessageName::default()` after encoding.
//!
//! The function is version-aware: it only includes fields that are valid for
//! the requested version, and emits null only for versions where the field is
//! actually nullable, preventing the JVM converter from rejecting bad input.

use std::{fmt::Write as FmtWrite, str::FromStr};

use proc_macro2::TokenStream;
use quote::quote;

use crate::ir::{FieldSpec, MessageSpec, VersionRange};

/// Emit the `default_json(version: i16)` function body for the given message.
/// The output is plain Rust source intended to be appended to the
/// per-message owned module body.
#[must_use]
pub fn emit_default_json(spec: &MessageSpec) -> String {
    let field_stmts = fields_stmts_tokens(&spec.fields);

    let tokens = quote! {
        #[doc = " Default JSON payload matching `Self::default()` for JVM oracle differential testing."]
        #[doc = " Only includes fields valid for the given version."]
        #[must_use]
        #[allow(unused_comparisons)]
        pub fn default_json(version: i16) -> ::serde_json::Value {
            let mut obj = ::serde_json::Map::new();
            #field_stmts
            ::serde_json::Value::Object(obj)
        }
    };

    // Validate at generation time — a parse failure is a generator bug.
    let _validate: syn::Item =
        syn::parse2(tokens.clone()).expect("generated default_json fn must be valid Rust");

    tokens.to_string()
}

/// Build a `TokenStream` of `obj.insert(...)` statements for each field,
/// guarded by version range checks.
fn fields_stmts_tokens(fields: &[FieldSpec]) -> TokenStream {
    let stmts: Vec<TokenStream> = fields
        .iter()
        .map(|f| {
            let key = json_field_name(&f.name);
            let field_cond = version_cond(f.versions);
            let val_expr_str = json_value_expr_versioned(f);
            let val_tokens = parse_expr(&val_expr_str);

            match field_cond.as_deref() {
                None => {
                    // Always valid.
                    quote! {
                        obj.insert(#key.to_string(), #val_tokens);
                    }
                }
                Some(cond) => {
                    let cond_tokens = parse_expr(cond);
                    quote! {
                        if #cond_tokens {
                            obj.insert(#key.to_string(), #val_tokens);
                        }
                    }
                }
            }
        })
        .collect();

    quote! { #(#stmts)* }
}

/// Parse an emitter-produced Rust fragment into tokens. The fragment came from
/// a trusted generator, so a lex error is a generator bug, not bad input.
fn parse_expr(s: &str) -> TokenStream {
    TokenStream::from_str(s).expect("leaf generator produced an unlexable fragment")
}

/// Return the condition string for a version range, or None if always valid.
fn version_cond(vr: VersionRange) -> Option<String> {
    if vr.min == 0 && vr.max == i16::MAX {
        None // always valid
    } else if vr.max == i16::MAX {
        Some(format!("version >= {}", vr.min))
    } else if vr.min == 0 {
        Some(format!("version <= {}", vr.max))
    } else {
        Some(format!("version >= {} && version <= {}", vr.min, vr.max))
    }
}

/// Generate a version-aware Rust expression for the default `serde_json::Value`.
///
/// The goal is to match what `MessageName::default()` encodes so that the
/// oracle produces the same bytes as the Rust implementation.
///
/// Key rules:
/// - Nullable non-array fields with no default: Rust `Default` produces `None`
///   (encodes as null), so emit `null` for nullable versions.
/// - Nullable array fields with no default: Rust `Default` produces `None`
///   (encodes as null array, i.e. -1), so emit `null` for nullable versions.
///   For non-nullable versions, emit `[]` (empty array).
/// - Non-nullable array fields: always `[]`.
/// - Fields with explicit null default: emit `null` (version-aware for split
///   nullability ranges).
fn json_value_expr_versioned(f: &FieldSpec) -> String {
    let is_array = f.field_type.starts_with("[]");
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");

    // Fields where Rust Default produces None:
    // - explicit null default
    // - nullable non-array fields with no default (None is the zero)
    // - nullable array fields with no default (None is the zero)
    let rust_default_is_none =
        default_is_null || (f.nullable_versions.is_some() && f.default.is_none());

    if rust_default_is_none {
        if let Some(nv) = f.nullable_versions {
            // Check if nullable for ALL valid versions (trivial case: no branching needed).
            let always_nullable =
                nv.min <= f.versions.min && (nv.max == i16::MAX || nv.max >= f.versions.max);
            if always_nullable {
                return "::serde_json::Value::Null".into();
            }
            // Split: nullable in some versions, not in others.
            // Emit: if <nullable_cond> { Null } else { type_zero }
            let zero = if is_array {
                "::serde_json::Value::Array(vec![])".to_string()
            } else {
                type_zero_expr(&f.field_type, f)
            };
            if let Some(cond) = version_cond(nv) {
                return format!("if {cond} {{ ::serde_json::Value::Null }} else {{ {zero} }}");
            }
            return "::serde_json::Value::Null".into();
        }
        // No nullable_versions but default_is_null — emit type zero.
        if is_array {
            return "::serde_json::Value::Array(vec![])".into();
        }
        return type_zero_expr(&f.field_type, f);
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
            let decimal = if let Some(hex_str) = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"))
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

/// The "zero" `serde_json::Value` expression for a type (when no default specified).
fn type_zero_expr(field_type: &str, f: &FieldSpec) -> String {
    let base = base_type(field_type);
    match base {
        "bool" => "::serde_json::Value::Bool(false)".into(),
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" => {
            "::serde_json::json!(0)".into()
        }
        "float64" => "::serde_json::json!(0.0)".into(),
        "string" | "bytes" | "records" => "::serde_json::Value::String(String::new())".into(),
        "uuid" => {
            // Kafka's *DataJsonConverter encodes Uuid as base64 (22 chars),
            // not the standard hyphen UUID format. The zero UUID in base64 is
            // "AAAAAAAAAAAAAAAAAAAAAA" (16 zero bytes, no padding needed).
            "::serde_json::Value::String(\"AAAAAAAAAAAAAAAAAAAAAA\".to_string())".into()
        }
        _ => {
            if f.fields.is_empty() {
                "::serde_json::Value::Object(::serde_json::Map::new())".into()
            } else {
                emit_nested_struct_expr(&f.fields)
            }
        }
    }
}

/// Emit a Rust expression that builds a `serde_json::Value::Object` for a nested struct.
/// Note: sub-field version guards are NOT applied here (nested struct defaults are
/// version-independent relative to their parent field's guard). If this causes issues
/// for specific complex schemas, fix at the per-field level.
fn emit_nested_struct_expr(fields: &[FieldSpec]) -> String {
    let mut s = String::new();
    s.push_str("{ let mut m = ::serde_json::Map::new(); ");
    for f in fields {
        let key = json_field_name(&f.name);
        let val = json_value_expr_versioned(f);
        write!(s, "m.insert(\"{key}\".to_string(), {val}); ").unwrap();
    }
    s.push_str("::serde_json::Value::Object(m) }");
    s
}

/// Convert a Kafka schema field name (`PascalCase` like `TransactionalId`) to
/// the JSON key used by the JVM's `*DataJsonConverter` (`camelCase` like
/// `transactionalId` — lowercase the first character).
#[must_use]
pub fn json_field_name(name: &str) -> String {
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
