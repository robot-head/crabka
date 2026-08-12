//! Emit Rust source for the owned flavor of a `MessageSpec`.
//!
//! This module handles primitive fields, tagged fields, primitive arrays, and
//! nested struct fields. Nested anonymous structs become sibling types in the
//! same generated file. The module also supports `commonStructs`.

use std::collections::HashMap;

use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote};

use crate::{
    emit::common::format_int_literal,
    ir::{FieldSpec, FlexibleVersions, MessageSpec, MessageType, VersionRange},
    name_conv,
    resolve::{self, Resolution},
};

#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("unsupported (in 1a): {0}")]
    Unsupported(String),
    #[error("resolve error: {0}")]
    Resolve(#[from] resolve::ResolveError),
}

/// Build the `use` items for a common-struct file body.
///
/// This function replicates the import selection the former
/// `emit_common_struct_file` used. It differs from `emit_imports` in one way:
/// it has NO records-import block, because common structs never carry
/// `records` fields.
pub(crate) fn emit_common_imports(fields: &[FieldSpec], flex_min_val: i16) -> TokenStream {
    let types = used_field_types_recursive(fields);
    let has_flex = flex_min_val < i16::MAX;
    let tagged = fields.iter().any(|f| f.tag.is_some());
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);
    let use_nullable_struct = uses_nullable_struct_recursive(fields);

    let mut out = quote!(
        use bytes::{Buf, BufMut};
    );

    {
        let mut gets: Vec<&str> = Vec::new();
        let mut puts: Vec<&str> = Vec::new();
        for (t, g, p) in &[
            ("int8", "get_i8", "put_i8"),
            ("int16", "get_i16", "put_i16"),
            ("uint16", "get_u16", "put_u16"),
            ("int32", "get_i32", "put_i32"),
            ("int64", "get_i64", "put_i64"),
            ("bool", "get_bool", "put_bool"),
            ("float64", "get_f64", "put_f64"),
        ] {
            if uses_fixed_type(&types, t) {
                gets.push(g);
                puts.push(p);
            }
        }
        if use_nullable_struct && !gets.contains(&"get_i8") {
            // get_i8 needed for nullable struct decode prefix even when no int8 fields.
            gets.push("get_i8");
        }
        if !gets.is_empty() {
            let mut combined: Vec<&str> = gets.into_iter().chain(puts).collect();
            combined.sort_unstable();
            combined.dedup();
            let items: Vec<_> = combined.iter().map(|n| format_ident!("{n}")).collect();
            out.extend(quote!(use crate::primitives::fixed::{ #(#items),* };));
        }
    }

    if use_string {
        out.extend(string_import(uses_nullable_string_recursive(fields)));
    }

    if use_bytes {
        out.extend(bytes_import(
            uses_non_nullable_bytes_recursive(fields),
            uses_nullable_bytes_recursive(fields),
        ));
    }

    out.extend(tagged_import(has_flex, tagged));

    out.extend(quote!(
        use crate::{Decode, Encode, ProtocolError, UnknownTaggedFields};
    ));
    out
}

/// The `string_bytes` import list for string fields. The nullable variant also
/// pulls in the nullable-string helpers.
fn string_import(use_nullable_string: bool) -> TokenStream {
    let names: &[&str] = if use_nullable_string {
        &[
            "compact_nullable_string_len",
            "compact_string_len",
            "get_compact_nullable_string_owned",
            "get_compact_string_owned",
            "get_nullable_string_owned",
            "get_string_owned",
            "nullable_string_len",
            "put_compact_nullable_string",
            "put_compact_string",
            "put_nullable_string",
            "put_string",
            "string_len",
        ]
    } else {
        &[
            "compact_string_len",
            "get_compact_string_owned",
            "get_string_owned",
            "put_compact_string",
            "put_string",
            "string_len",
        ]
    };
    let items: Vec<_> = names.iter().map(|n| format_ident!("{n}")).collect();
    quote!(use crate::primitives::string_bytes::{ #(#items),* };)
}

/// The `string_bytes` import list for `bytes` fields.
fn bytes_import(use_non_nullable_bytes: bool, use_nullable_bytes: bool) -> TokenStream {
    let mut items: Vec<&str> = Vec::new();
    if use_non_nullable_bytes {
        items.extend([
            "bytes_len",
            "compact_bytes_len",
            "get_bytes_owned",
            "get_compact_bytes_owned",
            "put_bytes",
            "put_compact_bytes",
        ]);
    }
    if use_nullable_bytes {
        items.extend([
            "compact_nullable_bytes_len",
            "get_compact_nullable_bytes_owned",
            "get_nullable_bytes_owned",
            "nullable_bytes_len",
            "put_compact_nullable_bytes",
            "put_nullable_bytes",
        ]);
    }
    items.sort_unstable();
    let idents: Vec<_> = items.iter().map(|n| format_ident!("{n}")).collect();
    quote!(use crate::primitives::string_bytes::{ #(#idents),* };)
}

/// The `tagged_fields` import line. This function pulls in `encode_to_bytes`
/// only when there are known tagged fields to encode.
fn tagged_import(flex: bool, tagged: bool) -> TokenStream {
    if flex && tagged {
        quote!(
            use crate::tagged_fields::{
                encode_to_bytes, read_tagged_fields, tagged_fields_len, WriteTaggedFields,
            };
        )
    } else if flex {
        quote!(
            use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
        )
    } else {
        quote!()
    }
}

pub(crate) fn flex_min(spec: &MessageSpec) -> i16 {
    match spec.flexible_versions {
        FlexibleVersions::Range(r) => r.min,
        FlexibleVersions::None => i16::MAX,
    }
}

pub(crate) fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

/// Collect the set of primitive schema types that non-tagged fields actually
/// use, so the emitter writes only the imports it needs.
fn used_field_types_recursive(fields: &[FieldSpec]) -> Vec<String> {
    let mut types: Vec<String> = Vec::new();
    for f in fields {
        let base = base_type(&f.field_type).to_string();
        if !types.contains(&base) {
            types.push(base);
        }
        if !f.fields.is_empty() {
            for t in used_field_types_recursive(&f.fields) {
                if !types.contains(&t) {
                    types.push(t);
                }
            }
        }
    }
    types
}

fn has_tagged_fields_recursive(fields: &[FieldSpec]) -> bool {
    fields
        .iter()
        .any(|f| f.tag.is_some() || has_tagged_fields_recursive(&f.fields))
}

/// Returns true if any field, at any depth, is `float64`.
/// `f64` does not implement `Eq`, so structs with `float64` fields must not
/// derive `Eq`.
pub(crate) fn has_float64_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        base == "float64" || has_float64_recursive(&f.fields)
    })
}

fn uses_fixed_type(types: &[String], t: &str) -> bool {
    types.iter().any(|s| s == t)
}

fn uses_string(types: &[String]) -> bool {
    types.iter().any(|t| t == "string")
}

fn uses_bytes(types: &[String]) -> bool {
    types.iter().any(|t| t.as_str() == "bytes")
}

fn uses_nullable_bytes_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "bytes" && f.nullable_versions.is_some();
        here || uses_nullable_bytes_recursive(&f.fields)
    })
}

fn uses_non_nullable_bytes_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "bytes" && needs_non_nullable_codec(f);
        here || uses_non_nullable_bytes_recursive(&f.fields)
    })
}

/// True if a field needs the non-nullable codec for at least some versions.
/// That happens when the field is never nullable, or when its nullable range
/// is narrower than its own version range, so a per-version split emits a
/// non-nullable branch.
fn needs_non_nullable_codec(f: &FieldSpec) -> bool {
    f.nullable_versions.is_none() || nullable_split_cond(f).is_some()
}

fn uses_nullable_records_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "records" && f.nullable_versions.is_some();
        here || uses_nullable_records_recursive(&f.fields)
    })
}

fn uses_non_nullable_records_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "records" && needs_non_nullable_codec(f);
        here || uses_non_nullable_records_recursive(&f.fields)
    })
}

/// Returns true if any field, at any depth, has a string type that is also
/// nullable.
fn uses_nullable_string_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "string" && (f.nullable_versions.is_some() || f.tag.is_some());
        here || uses_nullable_string_recursive(&f.fields)
    })
}

/// Returns true if any field has a non-array struct type with nullableVersions
/// set. Such fields need `get_i8` for the nullable prefix byte in decode.
fn uses_nullable_struct_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let t = &f.field_type;
        let base = base_type(t);
        let here = is_struct_type(base) && !t.starts_with("[]") && f.nullable_versions.is_some();
        here || uses_nullable_struct_recursive(&f.fields)
    })
}

pub(crate) fn has_any_flex(spec: &MessageSpec) -> bool {
    matches!(spec.flexible_versions, FlexibleVersions::Range(_))
}

fn has_any_tagged_in_spec(spec: &MessageSpec) -> bool {
    has_tagged_fields_recursive(&spec.fields)
}

pub(crate) fn emit_imports(spec: &MessageSpec) -> TokenStream {
    let types = used_field_types_recursive(&spec.fields);
    let tagged = has_any_tagged_in_spec(spec);
    let flex = has_any_flex(spec);
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);
    let use_nullable_string = uses_nullable_string_recursive(&spec.fields);
    let use_nullable_bytes = uses_nullable_bytes_recursive(&spec.fields);
    let use_non_nullable_bytes = uses_non_nullable_bytes_recursive(&spec.fields);

    let mut out = quote!(
        use bytes::{Buf, BufMut};
    );

    let use_nullable_struct = uses_nullable_struct_recursive(&spec.fields);

    // Emit only the specific fixed-type imports actually used, to avoid unused-import warnings.
    {
        let mut gets: Vec<&str> = Vec::new();
        let mut puts: Vec<&str> = Vec::new();
        if uses_fixed_type(&types, "int8") {
            gets.push("get_i8");
            puts.push("put_i8");
        } else if use_nullable_struct {
            // get_i8 is needed for nullable struct decode (1-byte signed null prefix).
            gets.push("get_i8");
        }
        if uses_fixed_type(&types, "int16") {
            gets.push("get_i16");
            puts.push("put_i16");
        }
        if uses_fixed_type(&types, "uint16") {
            gets.push("get_u16");
            puts.push("put_u16");
        }
        if uses_fixed_type(&types, "int32") {
            gets.push("get_i32");
            puts.push("put_i32");
        }
        if uses_fixed_type(&types, "int64") {
            gets.push("get_i64");
            puts.push("put_i64");
        }
        if uses_fixed_type(&types, "bool") {
            gets.push("get_bool");
            puts.push("put_bool");
        }
        if uses_fixed_type(&types, "float64") {
            gets.push("get_f64");
            puts.push("put_f64");
        }
        if !gets.is_empty() {
            let combined: Vec<&str> = gets.into_iter().chain(puts).collect();
            let sorted = {
                let mut v = combined;
                v.sort_unstable();
                v
            };
            let items: Vec<_> = sorted.iter().map(|n| format_ident!("{n}")).collect();
            out.extend(quote!(use crate::primitives::fixed::{ #(#items),* };));
        }
    }

    if use_string {
        // Emit only the string helpers that are actually needed for the fields
        // present in this message to avoid unused-import warnings.
        out.extend(string_import(use_nullable_string));
    }

    if use_bytes {
        out.extend(bytes_import(use_non_nullable_bytes, use_nullable_bytes));
    }

    // Records fields: import the byte-primitive helpers used in the generated wrapper code.
    {
        let use_nullable_records = uses_nullable_records_recursive(&spec.fields);
        let use_non_nullable_records = uses_non_nullable_records_recursive(&spec.fields);
        if use_nullable_records || use_non_nullable_records {
            // Note: compact_bytes_len_from_size is used via fully-qualified path in generated code.
            let mut items: Vec<&str> = Vec::new();
            if use_non_nullable_records {
                items.extend([
                    "get_bytes_owned",
                    "get_compact_bytes_owned",
                    "put_bytes",
                    "put_compact_bytes",
                ]);
            }
            if use_nullable_records {
                items.extend([
                    "get_compact_nullable_bytes_owned",
                    "get_nullable_bytes_owned",
                    "put_compact_bytes",
                    "put_compact_nullable_bytes",
                    "put_bytes",
                    "put_nullable_bytes",
                ]);
            }
            items.sort_unstable();
            items.dedup();
            let idents: Vec<_> = items.iter().map(|n| format_ident!("{n}")).collect();
            out.extend(quote!(use crate::primitives::string_bytes::{ #(#idents),* };));
        }
    }

    // Tagged-fields support: encode_to_bytes only when there are known tagged fields to encode.
    out.extend(tagged_import(flex, tagged));

    out.extend(quote!(
        use crate::{Decode, Encode, ProtocolError, UnknownTaggedFields};
    ));
    out
}

pub(crate) fn emit_constants(spec: &MessageSpec) -> TokenStream {
    let min_version = Literal::i16_unsuffixed(spec.valid_versions.min);
    let max_version = Literal::i16_unsuffixed(spec.valid_versions.max);
    let flex_minimum = flex_min(spec);
    let flex = Literal::i16_unsuffixed(flex_minimum);
    let flex_check = match flex_minimum {
        i16::MIN => quote!(true),
        i16::MAX => quote!(version == FLEXIBLE_MIN),
        _ => quote!(version >= FLEXIBLE_MIN),
    };
    // Request/Response schemas have an API key; Header/Data schemas do not.
    let api_key_const = match spec.message_type {
        MessageType::Request | MessageType::Response => {
            let api_key = Literal::i16_unsuffixed(
                spec.api_key
                    .expect("Request/Response must have apiKey in schema"),
            );
            quote!(pub const API_KEY: i16 = #api_key;)
        }
        // No API_KEY const for framing/data types.
        MessageType::Header | MessageType::Data => quote!(),
    };
    quote! {
        #api_key_const
        pub const MIN_VERSION: i16 = #min_version;
        pub const MAX_VERSION: i16 = #max_version;
        pub const FLEXIBLE_MIN: i16 = #flex;

        #[inline]
        #[must_use]
        pub fn is_flexible(version: i16) -> bool { #flex_check }
    }
}

/// Returns a Rust expression for the default value of an owned field. It
/// respects the schema-level `default` attribute, for example `"-1"` for
/// `ControllerId`.
pub(crate) fn owned_default_expr(f: &FieldSpec, res_map: &HashMap<String, Resolution>) -> String {
    let base = base_type(&f.field_type);
    let is_array = f.field_type.starts_with("[]");
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    // Kafka schemas use "null" (string) to mean the default is null for nullable fields.
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");
    if nullable {
        if default_is_null || f.default.is_none() {
            return "None".into();
        }
        if let Some(v) = &f.default {
            return format!("Some({})", scalar_owned_default(base, v));
        }
    }
    if is_array {
        return "Vec::new()".into();
    }
    if f.default.is_none()
        && let Some(resolution) = res_map.get(base)
    {
        return format!("{}::default()", resolution.rust_path);
    }
    match &f.default {
        Some(v) => scalar_owned_default(base, v),
        None => owned_zero(base),
    }
}

fn scalar_owned_default(base_type: &str, val: &serde_json::Value) -> String {
    // Kafka schema defaults are always stored as JSON strings (e.g., "-1", "null", "true").
    // We parse the string value to extract the actual default.
    match (base_type, val) {
        // String-encoded "null" for a nullable field means None — handled by caller.
        (_, serde_json::Value::String(s)) if s == "null" => "None".into(),
        ("string", serde_json::Value::String(s)) => format!("{s:?}.to_string()"),
        ("bool", serde_json::Value::String(s)) if s == "true" => "true".into(),
        ("bool", serde_json::Value::String(_)) => "false".into(),
        ("bool", serde_json::Value::Bool(b)) => b.to_string(),
        ("int8", serde_json::Value::String(s)) => format!("{}i8", s.trim()),
        ("int16", serde_json::Value::String(s)) => format!("{}i16", s.trim()),
        // Format as underscored literal to satisfy clippy::unreadable_literal.
        ("int32", serde_json::Value::String(s)) => format_int_literal(s.trim(), "i32"),
        ("int64", serde_json::Value::String(s)) => format_int_literal(s.trim(), "i64"),
        ("int8", serde_json::Value::Number(n)) => format!("{n}i8"),
        ("int16", serde_json::Value::Number(n)) => format!("{n}i16"),
        ("int32", serde_json::Value::Number(n)) => format_int_literal(&n.to_string(), "i32"),
        ("int64", serde_json::Value::Number(n)) => format_int_literal(&n.to_string(), "i64"),
        _ => owned_zero(base_type),
    }
}

fn owned_zero(base: &str) -> String {
    match base {
        "string" => "String::new()".into(),
        "bytes" => "bytes::Bytes::new()".into(),
        "bool" => "false".into(),
        "int8" => "0i8".into(),
        "int16" => "0i16".into(),
        "int32" => "0i32".into(),
        "int64" => "0i64".into(),
        "uint16" => "0u16".into(),
        "uint32" => "0u32".into(),
        "float64" => "0.0f64".into(),
        "uuid" => "crate::primitives::uuid::Uuid::default()".into(),
        _ => "Default::default()".into(),
    }
}

/// Parse a string schema default as an integer for comparison with zero.
fn parse_string_default_as_i64(s: &str) -> Option<i64> {
    s.trim().parse::<i64>().ok()
}

/// Returns true if any field in `fields` has a non-trivial schema default,
/// which is one that differs from the Rust type's natural Default.
pub(crate) fn needs_manual_default(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        match &f.default {
            None | Some(serde_json::Value::Null) => false, // null/None → None, same as derive
            Some(serde_json::Value::String(s)) if s == "null" => false, // "null" string → None
            Some(serde_json::Value::Bool(false)) if !nullable => false, // false == Default for bool
            Some(serde_json::Value::String(s)) if s == "false" && !nullable => false,
            Some(serde_json::Value::String(s)) if s.is_empty() && !nullable => false,
            Some(serde_json::Value::Number(n)) if n.as_i64() == Some(0) && !nullable => false,
            Some(serde_json::Value::String(s))
                if parse_string_default_as_i64(s) == Some(0) && !nullable =>
            {
                false
            }
            Some(_) => true,
        }
    })
}

/// Returns the resolved Rust path for a struct-typed field, or `None` for primitives.
pub(crate) fn struct_path_for(
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
) -> Option<String> {
    let base = base_type(&f.field_type);
    if is_struct_type(base) {
        res_map.get(base).map(|r| r.rust_path.clone())
    } else {
        None
    }
}

pub(crate) fn is_struct_type(t: &str) -> bool {
    t.chars().next().is_some_and(char::is_uppercase)
}

/// Build the populated-value expression for one owned field. `option` mirrors
/// the field's Rust-type `Option<...>` wrapping, which `emit_struct`
/// computes.
pub(crate) fn owned_populated_value(
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    option: bool,
) -> String {
    let base = base_type(&f.field_type);
    let is_array = f.field_type.starts_with("[]");
    // Records are left at default — building a valid batch here is unnecessary
    // and the dedicated records tests cover that codec.
    if base == "records" {
        return owned_default_expr(f, res_map);
    }
    let elem = owned_populated_scalar(base, f, res_map);
    let inner = if is_array {
        format!("vec![{elem}]")
    } else {
        elem
    };
    if option {
        format!("Some({inner})")
    } else {
        inner
    }
}

fn owned_populated_scalar(
    base: &str,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
) -> String {
    match base {
        "bool" => "true".to_string(),
        "int8" => "1i8".to_string(),
        "int16" => "1i16".to_string(),
        "int32" => "1i32".to_string(),
        "int64" => "1i64".to_string(),
        "uint16" => "1u16".to_string(),
        "uint32" => "1u32".to_string(),
        "float64" => "1.0f64".to_string(),
        "string" => "\"x\".to_string()".to_string(),
        "bytes" => "::bytes::Bytes::from_static(b\"x\")".to_string(),
        "uuid" => "crate::primitives::uuid::Uuid([1u8; 16])".to_string(),
        _ => {
            let path = struct_path_for(f, res_map).expect("struct field must resolve");
            format!("{path}::populated(version)")
        }
    }
}

// --- single-field encode/decode helpers -----------------------------------

/// Returns `true` if this field has the per-field override
/// `"flexibleVersions": "none"`. Such a field must always use the legacy
/// (non-compact) codec, even in flex message versions.
pub(crate) fn field_forces_non_flex(f: &FieldSpec) -> bool {
    matches!(f.flexible_versions, Some(FlexibleVersions::None))
}

/// Wrap the result in `Some` when the emitter uses a non-nullable decode but
/// the field type is `Option<T>`. For an array of structs this wraps the whole
/// block.
pub(crate) fn wrap_non_nullable_for_option(
    _schema_type: &str,
    non_nullable_call: &str,
    _res_map: &HashMap<String, Resolution>,
) -> String {
    // The field is typed as Option<T> (because nullable_versions exists).
    // A non-nullable decode produces T, so we must wrap in Some.
    format!("Some({non_nullable_call})")
}

/// Returns a Rust boolean expression that is `true` when the tagged field
/// equals its schema-specified default. The emitter uses it to suppress
/// tagged-field serialization. JVM Kafka also omits tagged fields that equal
/// their defaults.
pub(crate) fn tagged_is_default_cond(f: &FieldSpec) -> String {
    let field = name_conv::field_name(&f.name);
    let base = base_type(&f.field_type);
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");

    if nullable && (default_is_null || f.default.is_none()) {
        // Default is None; the field is an Option<T>.
        return format!("self.{field}.is_none()");
    }
    if let Some(v) = &f.default {
        // Compare against the explicit schema default.
        let cmp_val = scalar_owned_default(base, v);
        if cmp_val == "None" {
            return format!("self.{field}.is_none()");
        }
        // For Option<T> with a non-null default, compare Some(default) to self.field.
        if nullable {
            return format!("self.{field} == Some({cmp_val})");
        }
        // For Vec/array fields, check empty.
        if f.field_type.starts_with("[]") {
            return format!("self.{field}.is_empty()");
        }
        return format!("self.{field} == {cmp_val}");
    }
    // No schema default; fall back to Rust's Default.
    format!("crate::codegen_helpers::is_default(&self.{field})")
}

// --- primitive encode/decode call generators ------------------------------

/// Encode a field whose Rust type is `Option<T>` but whose wire format is
/// non-nullable, because `nullable_versions.min > field.versions.min`.
/// This function treats `None` as the empty or default value for the
/// underlying type.
pub(crate) fn encode_call_option_as_non_nullable(schema_type: &str, expr: &str) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            // Option<Vec<Struct>> → encode the inner vec (treating None as empty).
            return format!(
                "{{ let v = ({expr}).as_ref().map(Vec::as_slice).unwrap_or(&[]); \
                 crate::primitives::array::put_array_len(buf, v.len(), flex); \
                 for it in v {{ it.encode(buf, version)?; }} }}",
            );
        }
        // Option<Vec<Prim>>
        return format!(
            "{{ let v = ({expr}).as_ref().map(Vec::as_slice).unwrap_or(&[]); \
             crate::primitives::array::put_array_len(buf, v.len(), flex); \
             for it in v {{ {inner}; }} }}",
            inner = encode_call(elem, "it", false),
        );
    }
    // Option<String> → treat None as ""
    match schema_type {
        "string" => format!(
            "if flex {{ let () = put_compact_string(buf, ({expr}).as_deref().unwrap_or(\"\")); }} \
             else {{ let () = put_string(buf, ({expr}).as_deref().unwrap_or(\"\")); }}"
        ),
        "uuid" => format!("crate::primitives::uuid::put_uuid(buf, ({expr}).unwrap_or_default())"),
        // `records` can't go through `unwrap_or_default()` (that would move out of
        // `&self`), so match by reference and encode an empty payload for None.
        "records" => format!(
            "match &{expr} {{ \
                None => {{ let __rb_buf = bytes::BytesMut::new(); if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} }}, \
                Some(__rb) => {{ let mut __rb_buf = bytes::BytesMut::new(); <crate::records::RecordsPayload as crate::Encode>::encode(__rb, &mut __rb_buf, version)?; if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} }} \
            }}"
        ),
        _ => encode_call(schema_type, &format!("({expr}).unwrap_or_default()"), false),
    }
}

/// Compute the `encoded_len` of a field whose Rust type is `Option<T>` but
/// whose wire format is non-nullable.
pub(crate) fn encoded_len_expr_option_as_non_nullable(schema_type: &str, expr: &str) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            return format!(
                "{{ let v = ({expr}).as_ref().map(Vec::as_slice).unwrap_or(&[]); \
                 let prefix = crate::primitives::array::array_len_prefix_len(v.len(), flex); \
                 let body: usize = v.iter().map(|it| it.encoded_len(version)).sum(); \
                 prefix + body }}",
            );
        }
        return format!(
            "{{ let v = ({expr}).as_ref().map(Vec::as_slice).unwrap_or(&[]); \
             let prefix = crate::primitives::array::array_len_prefix_len(v.len(), flex); \
             let body: usize = v.iter().map(|it| {inner}).sum(); \
             prefix + body }}",
            inner = encoded_len_expr(elem, "*it", false),
        );
    }
    match schema_type {
        "string" => format!(
            "if flex {{ compact_string_len(({expr}).as_deref().unwrap_or(\"\")) }} \
             else {{ string_len(({expr}).as_deref().unwrap_or(\"\")) }}"
        ),
        "uuid" => "16".into(),
        "records" => format!(
            "match &{expr} {{ \
                None => if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(0) }} else {{ 4 }}, \
                Some(__rb) => {{ let __rb_len = <crate::records::RecordsPayload as crate::Encode>::encoded_len(__rb, version); if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) }} else {{ 4 + __rb_len }} }} \
            }}"
        ),
        _ => encoded_len_expr(schema_type, &format!("({expr}).unwrap_or_default()"), false),
    }
}

/// Generate an encode call expression with a specific buffer variable name.
/// The emitter uses this for tagged-field closures, where the buffer is `b`
/// and not `buf`.
pub(crate) fn encode_call_with_buf(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    buf_var: &str,
) -> String {
    // Replace all instances of `buf` in the generated expression with `buf_var`.
    let base = encode_call(schema_type, expr, nullable);
    // The expressions use `buf` as the buffer name; substitute with the actual var.
    base.replace("buf", buf_var)
}

/// Generate a decode call expression with a specific buffer variable name.
pub(crate) fn decode_call_with_buf(
    schema_type: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
    buf_var: &str,
    lenient_records: bool,
) -> String {
    let base = decode_call(schema_type, nullable, res_map, lenient_records);
    base.replace("buf", buf_var)
}

// `res_map` is threaded through for array-element recursion even though the
// primitives branch doesn't use it directly.
pub(crate) fn encode_call(schema_type: &str, expr: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem); // same as elem here (already stripped)
        if is_struct_type(elem_base) {
            // Array of structs
            if nullable {
                return format!(
                    "{{ let len = ({expr}).as_ref().map(Vec::len); \
                     crate::primitives::array::put_nullable_array_len(buf, len, flex); \
                     if let Some(v) = &{expr} {{ for it in v {{ it.encode(buf, version)?; }} }} }}",
                );
            }
            return format!(
                "{{ crate::primitives::array::put_array_len(buf, ({expr}).len(), flex); \
                 for it in &{expr} {{ it.encode(buf, version)?; }} }}",
            );
        }
        if nullable {
            return format!(
                "{{ let len = ({expr}).as_ref().map(Vec::len); \
                 crate::primitives::array::put_nullable_array_len(buf, len, flex); \
                 if let Some(v) = &{expr} {{ for it in v {{ {inner}; }} }} }}",
                // `it` is `&T` from iteration; for Copy primitives we dereference with `*it`.
                inner = encode_call(elem, "*it", false),
            );
        }
        return format!(
            "{{ crate::primitives::array::put_array_len(buf, ({expr}).len(), flex); \
             for it in &{expr} {{ {inner}; }} }}",
            inner = encode_call(elem, "*it", false),
        );
    }

    if is_struct_type(schema_type) {
        // Non-array struct. Nullable structs use a 1-byte signed prefix: -1 = null, 1 = non-null.
        // This matches the Kafka Java generator's nullable-struct wire encoding.
        if nullable {
            return format!(
                "match &{expr} {{ \
                 None => {{ buf.put_i8(-1); }}, \
                 Some(v) => {{ buf.put_i8(1); v.encode(buf, version)?; }} \
                 }}"
            );
        }
        return format!("{expr}.encode(buf, version)?");
    }

    match (schema_type, nullable) {
        ("int8", _) => format!("put_i8(buf, {expr})"),
        ("int16", _) => format!("put_i16(buf, {expr})"),
        ("uint16", _) => format!("put_u16(buf, {expr})"),
        ("int32", _) => format!("put_i32(buf, {expr})"),
        ("int64", _) => format!("put_i64(buf, {expr})"),
        ("bool", _) => format!("put_bool(buf, {expr})"),
        ("float64", _) => format!("put_f64(buf, {expr})"),
        ("uuid", _) => format!("crate::primitives::uuid::put_uuid(buf, {expr})"),
        ("string", false) => format!(
            "if flex {{ let () = put_compact_string(buf, &{expr}); }} else {{ let () = put_string(buf, &{expr}); }}"
        ),
        ("string", true) => format!(
            "if flex {{ let () = put_compact_nullable_string(buf, {expr}.as_deref()); }} else {{ let () = put_nullable_string(buf, {expr}.as_deref()); }}"
        ),
        ("bytes", false) => format!(
            "if flex {{ let () = put_compact_bytes(buf, &{expr}); }} else {{ let () = put_bytes(buf, &{expr}); }}"
        ),
        ("bytes", true) => format!(
            "if flex {{ let () = put_compact_nullable_bytes(buf, {expr}.as_deref()); }} else {{ let () = put_nullable_bytes(buf, {expr}.as_deref()); }}"
        ),
        ("records", false) => format!(
            "{{ \
                let mut __rb_buf = bytes::BytesMut::new(); \
                <crate::records::RecordsPayload as crate::Encode>::encode(&{expr}, &mut __rb_buf, version)?; \
                if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} \
            }}"
        ),
        ("records", true) => format!(
            "match &{expr} {{ \
                None => if flex {{ let () = put_compact_nullable_bytes(buf, None); }} else {{ let () = put_nullable_bytes(buf, None); }}, \
                Some(__rb) => {{ \
                    let mut __rb_buf = bytes::BytesMut::new(); \
                    <crate::records::RecordsPayload as crate::Encode>::encode(__rb, &mut __rb_buf, version)?; \
                    if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} \
                }} \
            }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encode_call: {t}\")"),
    }
}

pub(crate) fn encoded_len_expr(schema_type: &str, expr: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            // Array of structs
            if nullable {
                return format!(
                    "{{ let opt: Option<&Vec<_>> = ({expr}).as_ref(); \
                     let prefix = crate::primitives::array::nullable_array_len_prefix_len(opt.map(std::vec::Vec::len), flex); \
                     let body: usize = opt.map_or(0, |v| v.iter().map(|it| it.encoded_len(version)).sum()); \
                     prefix + body }}",
                );
            }
            return format!(
                "{{ let prefix = crate::primitives::array::array_len_prefix_len(({expr}).len(), flex); \
                 let body: usize = ({expr}).iter().map(|it| it.encoded_len(version)).sum(); \
                 prefix + body }}",
            );
        }
        {
            let inner = encoded_len_expr(elem, "*it", false);
            // Use `|_|` when the inner expression is constant (doesn't reference `*it`),
            // to avoid an unused-variable warning.
            let closure_arg = if inner.contains("*it") { "it" } else { "_" };
            if nullable {
                return format!(
                    "{{ let opt: Option<&Vec<_>> = ({expr}).as_ref(); \
                     let prefix = crate::primitives::array::nullable_array_len_prefix_len(opt.map(std::vec::Vec::len), flex); \
                     let body: usize = opt.map_or(0, |v| v.iter().map(|{closure_arg}| {inner}).sum()); \
                     prefix + body }}",
                );
            }
            return format!(
                "{{ let prefix = crate::primitives::array::array_len_prefix_len(({expr}).len(), flex); \
                 let body: usize = ({expr}).iter().map(|{closure_arg}| {inner}).sum(); \
                 prefix + body }}",
            );
        }
    }

    if is_struct_type(schema_type) {
        if nullable {
            // 1 byte for the signed null-marker prefix (–1 or 1) + body when present.
            return format!("1 + {expr}.as_ref().map_or(0, |v| v.encoded_len(version))");
        }
        return format!("{expr}.encoded_len(version)");
    }

    match (schema_type, nullable) {
        ("int8" | "bool", _) => "1".into(),
        ("int16" | "uint16", _) => "2".into(),
        ("int32", _) => "4".into(),
        ("int64" | "float64", _) => "8".into(),
        ("uuid", _) => "16".into(),
        ("string", false) => {
            format!("if flex {{ compact_string_len(&{expr}) }} else {{ string_len(&{expr}) }}")
        }
        ("string", true) => format!(
            "if flex {{ compact_nullable_string_len({expr}.as_deref()) }} else {{ nullable_string_len({expr}.as_deref()) }}"
        ),
        ("bytes", false) => {
            format!("if flex {{ compact_bytes_len(&{expr}) }} else {{ bytes_len(&{expr}) }}")
        }
        ("bytes", true) => format!(
            "if flex {{ compact_nullable_bytes_len({expr}.as_deref()) }} else {{ nullable_bytes_len({expr}.as_deref()) }}"
        ),
        ("records", false) => format!(
            "{{ let __rb_len = <crate::records::RecordsPayload as crate::Encode>::encoded_len(&{expr}, version); \
               if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) }} \
               else {{ 4 + __rb_len }} }}"
        ),
        ("records", true) => format!(
            "match &{expr} {{ \
                None => if flex {{ crate::primitives::varint::uvarint_len(0) }} else {{ 4 }}, \
                Some(__rb) => {{ let __rb_len = <crate::records::RecordsPayload as crate::Encode>::encoded_len(__rb, version); \
                    if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) }} \
                    else {{ 4 + __rb_len }} }} \
            }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encoded_len_expr: {t}\")"),
    }
}

pub(crate) fn decode_call(
    schema_type: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
    lenient_records: bool,
) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            // Array of structs: use the resolved path so common-struct files compile correctly.
            let type_path = res_map
                .get(elem_base)
                .map_or(elem_base, |r| r.rust_path.as_str());
            if nullable {
                return format!(
                    "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                     match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                     for _ in 0..n {{ v.push({type_path}::decode(buf, version)?); }} Some(v) }} }} }}",
                );
            }
            return format!(
                "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
                 let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({type_path}::decode(buf, version)?); }} v }}",
            );
        }
        if nullable {
            return format!(
                "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                 match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({inner}); }} Some(v) }} }} }}",
                inner = decode_call(elem, false, res_map, lenient_records),
            );
        }
        return format!(
            "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
             let mut v = Vec::with_capacity(n); for _ in 0..n {{ v.push({inner}); }} v }}",
            inner = decode_call(elem, false, res_map, lenient_records),
        );
    }

    if is_struct_type(schema_type) {
        let type_path = res_map
            .get(schema_type)
            .map_or(schema_type, |r| r.rust_path.as_str());
        if nullable {
            // Nullable non-array structs use a 1-byte signed prefix: < 0 = null, else non-null.
            // Matches the Kafka Java generator's nullable-struct wire encoding.
            return format!(
                "if get_i8(buf)? < 0 {{ None }} else {{ Some({type_path}::decode(buf, version)?) }}"
            );
        }
        return format!("{type_path}::decode(buf, version)?");
    }

    match (schema_type, nullable) {
        ("int8",   _)     => "get_i8(buf)?".into(),
        ("int16",  _)     => "get_i16(buf)?".into(),
        ("uint16", _)     => "get_u16(buf)?".into(),
        ("int32",  _)     => "get_i32(buf)?".into(),
        ("int64",  _)     => "get_i64(buf)?".into(),
        ("bool",   _)     => "get_bool(buf)?".into(),
        ("float64",_)     => "get_f64(buf)?".into(),
        ("uuid",   _)     => "crate::primitives::uuid::get_uuid(buf)?".into(),
        ("string", false) => "if flex { get_compact_string_owned(buf)? } else { get_string_owned(buf)? }".into(),
        ("string", true)  => "if flex { get_compact_nullable_string_owned(buf)? } else { get_nullable_string_owned(buf)? }".into(),
        ("bytes", false) => "if flex { get_compact_bytes_owned(buf)? } else { get_bytes_owned(buf)? }".into(),
        ("bytes", true)  => "if flex { get_compact_nullable_bytes_owned(buf)? } else { get_nullable_bytes_owned(buf)? }".into(),
        ("records", false) => if lenient_records {
            "{ \
                let __rb_bytes = if flex { get_compact_bytes_owned(buf)? } else { get_bytes_owned(buf)? }; \
                let mut __rb_cur: &[u8] = &__rb_bytes; \
                crate::records::RecordsPayload::decode_lenient(&mut __rb_cur, version)? \
            }".into()
        } else {
            "{ \
                let __rb_bytes = if flex { get_compact_bytes_owned(buf)? } else { get_bytes_owned(buf)? }; \
                let mut __rb_cur: &[u8] = &__rb_bytes; \
                <crate::records::RecordsPayload as crate::Decode>::decode(&mut __rb_cur, version)? \
            }".into()
        },
        ("records", true) => if lenient_records {
            "{ \
                let __rb_opt = if flex { get_compact_nullable_bytes_owned(buf)? } else { get_nullable_bytes_owned(buf)? }; \
                match __rb_opt { \
                    None => None, \
                    Some(__rb_bytes) => { \
                        let mut __rb_cur: &[u8] = &__rb_bytes; \
                        Some(crate::records::RecordsPayload::decode_lenient(&mut __rb_cur, version)?) \
                    } \
                } \
            }".into()
        } else {
            "{ \
                let __rb_opt = if flex { get_compact_nullable_bytes_owned(buf)? } else { get_nullable_bytes_owned(buf)? }; \
                match __rb_opt { \
                    None => None, \
                    Some(__rb_bytes) => { \
                        let mut __rb_cur: &[u8] = &__rb_bytes; \
                        Some(<crate::records::RecordsPayload as crate::Decode>::decode(&mut __rb_cur, version)?) \
                    } \
                } \
            }".into()
        },
        (t, _) => format!("compile_error!(\"unhandled type in decode_call: {t}\")"),
    }
}

// --- helpers --------------------------------------------------------------

pub(crate) fn is_tagged(f: &FieldSpec) -> bool {
    f.tag.is_some()
}
pub(crate) fn is_nullable(f: &FieldSpec) -> bool {
    f.nullable_versions.is_some()
}

/// Version condition under which a field uses its NULLABLE codec.
///
/// A field is nullable only within its `nullableVersions` range. Where that
/// range is narrower than the field's own version range (on either end), the
/// codec must switch between nullable and non-nullable per version. Returns
/// `Some(cond)` for that boundary expression, or `None` when nullability is
/// constant across the whole field range. In that case use `is_nullable(f)`
/// directly.
pub(crate) fn nullable_split_cond(f: &FieldSpec) -> Option<String> {
    let r = f.nullable_versions?;
    let need_lower = r.min > f.versions.min;
    let need_upper = r.max < f.versions.max;
    if !need_lower && !need_upper {
        return None;
    }
    let mut parts = Vec::new();
    if need_lower {
        parts.push(if r.min == i16::MAX {
            "version == i16::MAX".to_string()
        } else {
            format!("version >= {}", r.min)
        });
    }
    if need_upper {
        parts.push(if r.max == i16::MIN {
            "version == i16::MIN".to_string()
        } else {
            format!("version <= {}", r.max)
        });
    }
    Some(parts.join(" && "))
}

pub(crate) fn version_cond(r: VersionRange, version_var: &str) -> String {
    if r.min == i16::MIN && r.max == i16::MAX {
        "true".to_string()
    } else if r.min == r.max {
        format!("{version_var} == {}", r.min)
    } else if r.min == i16::MIN {
        format!("{version_var} <= {}", r.max)
    } else if r.max == i16::MAX {
        format!("{version_var} >= {}", r.min)
    } else {
        format!("({}..={}).contains(&{version_var})", r.min, r.max)
    }
}
