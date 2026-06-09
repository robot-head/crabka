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

use crate::ir::{FieldSpec, MessageSpec, VersionRange};
use proc_macro2::{Literal, TokenStream};
use quote::quote;

/// Emit the `default_json(version: i16)` function body for the given message.
/// The output is plain Rust source intended to be appended to the
/// per-message owned module body.
#[must_use]
pub fn emit_default_json(spec: &MessageSpec) -> String {
    let tokens = emit_default_json_tokens(spec);
    let file = syn::parse2::<syn::File>(tokens).expect("default_json must be valid Rust");
    prettyplease::unparse(&file)
}

#[must_use]
pub fn emit_default_json_tokens(spec: &MessageSpec) -> TokenStream {
    let fields = field_stmts(&spec.fields);
    quote! {
        #[doc = " Default JSON payload matching `Self::default()` for JVM oracle differential testing."]
        #[doc = " Only includes fields valid for the given version."]
        #[must_use]
        #[allow(unused_comparisons)]
        pub fn default_json(version: i16) -> ::serde_json::Value {
            let mut obj = ::serde_json::Map::new();
            #(#fields)*
            ::serde_json::Value::Object(obj)
        }
    }
}

/// Emit `obj.insert(...)` statements for each field, guarded by version range checks.
fn field_stmts(fields: &[FieldSpec]) -> Vec<TokenStream> {
    fields
        .iter()
        .map(|f| {
            let key = json_field_name(&f.name);
            let field_cond = version_cond(f.versions);
            let val_expr = json_value_expr_versioned(f);

            match field_cond {
                None => quote!(obj.insert(#key.to_string(), #val_expr);),
                Some(cond) => quote! {
                    if #cond {
                        obj.insert(#key.to_string(), #val_expr);
                    }
                },
            }
        })
        .collect()
}

/// Return the condition string for a version range, or None if always valid.
fn version_cond(vr: VersionRange) -> Option<TokenStream> {
    if vr.min == 0 && vr.max == i16::MAX {
        None // always valid
    } else if vr.max == i16::MAX {
        let min = Literal::i16_unsuffixed(vr.min);
        Some(quote!(version >= #min))
    } else if vr.min == 0 {
        let max = Literal::i16_unsuffixed(vr.max);
        Some(quote!(version <= #max))
    } else {
        let min = Literal::i16_unsuffixed(vr.min);
        let max = Literal::i16_unsuffixed(vr.max);
        Some(quote!(version >= #min && version <= #max))
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
fn json_value_expr_versioned(f: &FieldSpec) -> TokenStream {
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
                return quote!(::serde_json::Value::Null);
            }
            // Split: nullable in some versions, not in others.
            // Emit: if <nullable_cond> { Null } else { type_zero }
            let zero = if is_array {
                quote!(::serde_json::Value::Array(vec![]))
            } else {
                type_zero_expr(&f.field_type, f)
            };
            return match version_cond(nv) {
                Some(cond) => quote!(if #cond { ::serde_json::Value::Null } else { #zero }),
                None => quote!(::serde_json::Value::Null),
            };
        }
        // No nullable_versions but default_is_null — emit type zero.
        if is_array {
            return quote!(::serde_json::Value::Array(vec![]));
        }
        return type_zero_expr(&f.field_type, f);
    }

    if is_array {
        return quote!(::serde_json::Value::Array(vec![]));
    }

    match &f.default {
        Some(v) => scalar_value_expr(&f.field_type, v, f),
        None => type_zero_expr(&f.field_type, f),
    }
}

/// Convert a schema `default` annotation to a Rust expression producing a `serde_json::Value`.
fn scalar_value_expr(field_type: &str, val: &serde_json::Value, f: &FieldSpec) -> TokenStream {
    let base = base_type(field_type);

    match val {
        serde_json::Value::String(s) if s == "null" => quote!(::serde_json::Value::Null),

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
            let decimal = numeric_literal_tokens(&decimal);
            quote!(::serde_json::json!(#decimal))
        }

        serde_json::Value::String(s) if base == "bool" => {
            if s == "true" {
                quote!(::serde_json::Value::Bool(true))
            } else {
                quote!(::serde_json::Value::Bool(false))
            }
        }

        serde_json::Value::String(s) if base == "string" => {
            quote!(::serde_json::Value::String(#s.to_string()))
        }

        serde_json::Value::Number(n) => {
            let n = json_number_tokens(n);
            quote!(::serde_json::json!(#n))
        }

        serde_json::Value::Bool(b) => {
            quote!(::serde_json::Value::Bool(#b))
        }

        _ => type_zero_expr(field_type, f),
    }
}

fn numeric_literal_tokens(value: &str) -> TokenStream {
    if value.contains('.') || value.contains('e') || value.contains('E') {
        let lit = Literal::f64_unsuffixed(value.parse::<f64>().expect("schema float default"));
        return quote!(#lit);
    }
    if value.starts_with('-') {
        let lit = Literal::i64_unsuffixed(value.parse::<i64>().expect("schema integer default"));
        quote!(#lit)
    } else {
        let lit = Literal::u64_unsuffixed(value.parse::<u64>().expect("schema integer default"));
        quote!(#lit)
    }
}

fn json_number_tokens(n: &serde_json::Number) -> TokenStream {
    if let Some(value) = n.as_i64() {
        let lit = Literal::i64_unsuffixed(value);
        quote!(#lit)
    } else if let Some(value) = n.as_u64() {
        let lit = Literal::u64_unsuffixed(value);
        quote!(#lit)
    } else {
        let lit = Literal::f64_unsuffixed(n.as_f64().expect("JSON float default"));
        quote!(#lit)
    }
}

/// The "zero" `serde_json::Value` expression for a type (when no default specified).
fn type_zero_expr(field_type: &str, f: &FieldSpec) -> TokenStream {
    let base = base_type(field_type);
    match base {
        "bool" => quote!(::serde_json::Value::Bool(false)),
        "int8" | "int16" | "int32" | "int64" | "uint16" | "uint32" => {
            quote!(::serde_json::json!(0))
        }
        "float64" => quote!(::serde_json::json!(0.0)),
        "string" | "bytes" | "records" => quote!(::serde_json::Value::String(String::new())),
        "uuid" => {
            // Kafka's *DataJsonConverter encodes Uuid as base64 (22 chars),
            // not the standard hyphen UUID format. The zero UUID in base64 is
            // "AAAAAAAAAAAAAAAAAAAAAA" (16 zero bytes, no padding needed).
            quote!(::serde_json::Value::String(
                "AAAAAAAAAAAAAAAAAAAAAA".to_string()
            ))
        }
        _ => {
            if f.fields.is_empty() {
                quote!(::serde_json::Value::Object(::serde_json::Map::new()))
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
fn emit_nested_struct_expr(fields: &[FieldSpec]) -> TokenStream {
    let inserts = fields.iter().map(|f| {
        let key = json_field_name(&f.name);
        let val = json_value_expr_versioned(f);
        quote!(m.insert(#key.to_string(), #val);)
    });
    quote!({
        let mut m = ::serde_json::Map::new();
        #(#inserts)*
        ::serde_json::Value::Object(m)
    })
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
