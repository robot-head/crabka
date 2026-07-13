//! Emit Rust source for the borrowed flavor of a `MessageSpec`.
//!
//! Mirrors the structure of `emit/owned.rs`. Strings become `&'a str`,
//! bytes become `&'a [u8]`, the struct carries a `'a` lifetime,
//! `DecodeBorrow<'de>` replaces `Decode<'de>`, and `to_owned()` bridges to
//! the matching owned type.

use std::collections::HashMap;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::{
    emit::common::format_int_literal,
    ir::{FieldSpec, FlexibleVersions, MessageSpec, MessageType, VersionRange},
    name_conv,
    resolve::{Resolution, StructKind},
};

// ── helpers shared with owned ──────────────────────────────────────────────

pub(crate) fn flex_min(spec: &MessageSpec) -> i16 {
    match spec.flexible_versions {
        FlexibleVersions::Range(r) => r.min,
        FlexibleVersions::None => i16::MAX,
    }
}

pub(crate) fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

pub(crate) fn is_struct_type(t: &str) -> bool {
    t.chars().next().is_some_and(char::is_uppercase)
}

/// Returns true if ANY field in the list (recursively) would carry a borrowed
/// lifetime in the generated Rust type — i.e., string, bytes, records, or a
/// nested struct that itself has borrowed fields.
///
/// `res_map` is consulted for common-struct references (`PascalCase` where `f.fields.is_empty()`)
/// to check whether that common struct was generated with `<'a>`.
pub(crate) fn needs_lifetime(
    fields: &[crate::ir::FieldSpec],
    res_map: &HashMap<String, Resolution>,
) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        matches!(base, "string" | "bytes" | "records")  // records borrows via RecordsPayloadBorrowed<'a>
            // Inline nested struct with borrowed fields.
            || (is_struct_type(base) && !f.fields.is_empty() && needs_lifetime(&f.fields, res_map))
            // Common-struct reference: consult the resolution to see if it has '<'a>'.
            || (is_struct_type(base) && f.fields.is_empty()
                && res_map.get(base).is_some_and(|r| r.needs_lifetime))
    })
}

pub(crate) fn is_tagged(f: &FieldSpec) -> bool {
    f.tag.is_some()
}

/// Returns true if the top-level struct needs a `'a` lifetime parameter.
/// Only non-tagged fields contribute borrowed lifetimes; tagged fields that have
/// string/struct content use owned types to avoid escape from the payload closure.
pub(crate) fn spec_needs_lifetime(
    spec: &MessageSpec,
    res_map: &HashMap<String, Resolution>,
) -> bool {
    let non_tagged: Vec<&FieldSpec> = spec.fields.iter().filter(|f| !is_tagged(f)).collect();
    non_tagged.iter().any(|f| {
        let base = base_type(&f.field_type);
        matches!(base, "string" | "bytes" | "records")
            || (is_struct_type(base) && !f.fields.is_empty() && needs_lifetime(&f.fields, res_map))
            // Common-struct reference: consult the resolution to see if it has '<'a>'.
            || (is_struct_type(base) && f.fields.is_empty()
                && res_map.get(base).is_some_and(|r| r.needs_lifetime))
    })
}

/// Returns true if a tagged field's content cannot be zero-copy decoded
/// (because string/bytes data in its payload would escape the closure).
/// In that case the field must use the owned type.
pub(crate) fn tagged_field_needs_owned(
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
) -> bool {
    if !is_tagged(f) {
        return false;
    }
    let base = base_type(&f.field_type);
    // Primitive tagged fields (int*, bool, uuid) are fine as borrowed.
    if !is_struct_type(base) && !matches!(base, "string" | "bytes" | "records") {
        return false;
    }
    // Struct tagged fields: fine only if the nested struct has no borrowed fields.
    if is_struct_type(base) {
        if f.fields.is_empty() {
            // Common-struct reference: needs owned only if the common struct has '<'a>'.
            return res_map.get(base).is_some_and(|r| r.needs_lifetime);
        }
        return needs_lifetime(&f.fields, res_map);
    }
    // String/bytes tagged fields also need owned (lifetime escape).
    true
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
/// constant across the whole field range (use `is_nullable(f)` directly).
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

pub(crate) fn has_any_flex(spec: &MessageSpec) -> bool {
    matches!(spec.flexible_versions, FlexibleVersions::Range(_))
}

fn has_tagged_fields_recursive(fields: &[FieldSpec]) -> bool {
    fields
        .iter()
        .any(|f| f.tag.is_some() || has_tagged_fields_recursive(&f.fields))
}

fn has_any_tagged_in_spec(spec: &MessageSpec) -> bool {
    has_tagged_fields_recursive(&spec.fields)
}

/// Returns true if any tagged field in this spec (at top level) needs to be decoded
/// using the owned `Decode` trait rather than `DecodeBorrow`.
fn has_tagged_fields_needing_owned(
    spec: &MessageSpec,
    res_map: &HashMap<String, Resolution>,
) -> bool {
    spec.fields
        .iter()
        .any(|f| is_tagged(f) && tagged_field_needs_owned(f, res_map))
}

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

fn uses_fixed_type(types: &[String], t: &str) -> bool {
    types.iter().any(|s| s == t)
}

/// Returns true if any field (recursively) is `float64`.
/// Used to suppress the `Eq` derive since `f64` does not implement `Eq`.
pub(crate) fn has_float64_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        base == "float64" || has_float64_recursive(&f.fields)
    })
}

fn uses_string(types: &[String]) -> bool {
    types.iter().any(|t| t == "string")
}

fn uses_bytes(types: &[String]) -> bool {
    types.iter().any(|t| t.as_str() == "bytes")
}

fn uses_nullable_string_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "string" && (f.nullable_versions.is_some() || f.tag.is_some());
        here || uses_nullable_string_recursive(&f.fields)
    })
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

/// True if a field needs the non-nullable codec for at least some versions:
/// either it is never nullable, or its nullable range is narrower than its own
/// version range (so a per-version split emits a non-nullable branch).
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

/// Returns true if any field has a non-array struct type with nullableVersions set.
/// Such fields require `get_i8` for the nullable prefix byte in decode.
fn uses_nullable_struct_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let t = &f.field_type;
        let base = base_type(t);
        let here = is_struct_type(base) && !t.starts_with("[]") && f.nullable_versions.is_some();
        here || uses_nullable_struct_recursive(&f.fields)
    })
}

/// Returns the Rust type path for a struct-typed field in borrowed flavor.
/// Includes `<'a>` only when the nested struct actually has borrowed fields.
/// `type_map::borrowed_type` will use the path verbatim (no automatic `<'a>` addition).
pub(crate) fn struct_path_for(
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
) -> Option<String> {
    let base = base_type(&f.field_type);
    if is_struct_type(base) {
        res_map.get(base).map(|r| {
            // For inline nested structs, check field contents; for common-struct refs, use
            // the pre-computed needs_lifetime flag on the Resolution.
            let needs_lt = if f.fields.is_empty() {
                r.needs_lifetime
            } else {
                needs_lifetime(&f.fields, res_map)
            };
            if needs_lt {
                format!("{}<'a>", r.rust_path)
            } else {
                r.rust_path.clone()
            }
        })
    } else {
        None
    }
}

/// Returns the fully-qualified owned-flavor type path for a struct-typed field,
/// used when a tagged field must store owned data (because borrowed data would escape
/// the `read_tagged_fields` closure).
pub(crate) fn owned_struct_path_for(
    f: &FieldSpec,
    parent_module: &str,
    res_map: &HashMap<String, Resolution>,
) -> Option<String> {
    let base = base_type(&f.field_type);
    if is_struct_type(base) {
        res_map
            .get(base)
            .map(|_| resolved_to_owned_path(base, parent_module, res_map))
    } else {
        None
    }
}

/// Convert a borrowed-flavor resolved `rust_path` to its owned-flavor equivalent.
///
/// Common structs are message-scoped under `common/<message_snake>/<struct_snake>`.
///
/// - Inline nested structs have a bare `rust_path` like `"TypeName"` →
///   `"crate::owned::{parent_module}::TypeName"`.
/// - Common structs from a message-level context have `rust_path` like
///   `"super::common::<msg>::<struct>::TypeName"` →
///   `"crate::owned::common::<msg>::<struct>::TypeName"`.
/// - Common structs from a common-struct-level context (`parent_module` =
///   `"common::<msg>::<struct>"`) have `rust_path` like `"super::<struct>::TypeName"`;
///   the `<msg>` segment is recovered from `parent_module` →
///   `"crate::owned::common::<msg>::<struct>::TypeName"`.
fn resolved_to_owned_path(
    type_name: &str,
    parent_module: &str,
    res_map: &HashMap<String, Resolution>,
) -> String {
    match res_map.get(type_name) {
        Some(r) if r.kind == StructKind::Common => {
            // Determine the owned path from the rust_path stored in the res_map.
            if let Some(without_super) = r.rust_path.strip_prefix("super::common::") {
                // Message-level context:
                // rust_path = "super::common::<msg>::<struct>::TypeName"
                format!("crate::owned::common::{without_super}")
            } else if let Some(sibling) = r.rust_path.strip_prefix("super::") {
                // Common-struct-level context: rust_path = "super::<struct>::TypeName"
                // (a sibling common struct of the same message). Recover the
                // <msg> segment from parent_module = "common::<msg>::<struct>".
                let msg_seg = parent_module
                    .strip_prefix("common::")
                    .and_then(|rest| rest.split("::").next())
                    .unwrap_or(parent_module);
                format!("crate::owned::common::{msg_seg}::{sibling}")
            } else {
                // Fallback: use parent_module
                format!("crate::owned::{parent_module}::{type_name}")
            }
        }
        _ => {
            // Inline nested struct — lives in the parent message's owned module.
            format!("crate::owned::{parent_module}::{type_name}")
        }
    }
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

// ── imports ────────────────────────────────────────────────────────────────

/// Build the fixed-type `use crate::primitives::fixed::{ ... };` import, or an
/// empty stream when no fixed primitives are used. `force_get_i8` pulls in
/// `get_i8` even with no int8 fields (needed for the nullable-struct prefix).
fn fixed_import(types: &[String], force_get_i8: bool) -> TokenStream {
    let mut gets: Vec<&str> = Vec::new();
    let mut puts: Vec<&str> = Vec::new();
    if uses_fixed_type(types, "int8") {
        gets.push("get_i8");
        puts.push("put_i8");
    } else if force_get_i8 {
        // get_i8 is needed for nullable struct decode (1-byte signed null prefix).
        gets.push("get_i8");
    }
    if uses_fixed_type(types, "int16") {
        gets.push("get_i16");
        puts.push("put_i16");
    }
    if uses_fixed_type(types, "uint16") {
        gets.push("get_u16");
        puts.push("put_u16");
    }
    if uses_fixed_type(types, "int32") {
        gets.push("get_i32");
        puts.push("put_i32");
    }
    if uses_fixed_type(types, "int64") {
        gets.push("get_i64");
        puts.push("put_i64");
    }
    if uses_fixed_type(types, "bool") {
        gets.push("get_bool");
        puts.push("put_bool");
    }
    if uses_fixed_type(types, "float64") {
        gets.push("get_f64");
        puts.push("put_f64");
    }
    if gets.is_empty() {
        return quote!();
    }
    let mut combined: Vec<&str> = gets.into_iter().chain(puts).collect();
    combined.sort_unstable();
    let items: Vec<_> = combined.iter().map(|n| format_ident!("{n}")).collect();
    quote!(use crate::primitives::fixed::{ #(#items),* };)
}

/// String-field imports (the `string_bytes` len/put helpers plus the borrowed
/// getters). `nullable` pulls in the nullable variants.
fn string_import(nullable: bool) -> TokenStream {
    if nullable {
        quote! {
            use crate::primitives::string_bytes::{
                compact_nullable_string_len, compact_string_len, nullable_string_len,
                put_compact_nullable_string, put_compact_string, put_nullable_string, put_string,
                string_len,
            };
            use crate::primitives::string_bytes_borrowed::{
                get_compact_nullable_string_borrowed, get_compact_string_borrowed,
                get_nullable_string_borrowed, get_string_borrowed,
            };
        }
    } else {
        quote! {
            use crate::primitives::string_bytes::{
                compact_string_len, put_compact_string, put_string, string_len,
            };
            use crate::primitives::string_bytes_borrowed::{
                get_compact_string_borrowed, get_string_borrowed,
            };
        }
    }
}

/// `bytes`-field imports: the `put_*` helpers plus the borrowed getters.
fn bytes_import(use_non_nullable: bool, use_nullable: bool) -> TokenStream {
    let mut put_items: Vec<&str> = Vec::new();
    let mut get_borrowed_items: Vec<&str> = Vec::new();
    if use_non_nullable {
        put_items.extend(["put_bytes", "put_compact_bytes"]);
        get_borrowed_items.extend(["get_bytes_borrowed", "get_compact_bytes_borrowed"]);
    }
    if use_nullable {
        put_items.extend(["put_compact_nullable_bytes", "put_nullable_bytes"]);
        get_borrowed_items.extend([
            "get_compact_nullable_bytes_borrowed",
            "get_nullable_bytes_borrowed",
        ]);
    }
    put_items.sort_unstable();
    get_borrowed_items.sort_unstable();
    let puts: Vec<_> = put_items.iter().map(|n| format_ident!("{n}")).collect();
    let gets: Vec<_> = get_borrowed_items
        .iter()
        .map(|n| format_ident!("{n}"))
        .collect();
    quote! {
        use crate::primitives::string_bytes::{ #(#puts),* };
        use crate::primitives::string_bytes_borrowed::{ #(#gets),* };
    }
}

/// The `tagged_fields` import line. `encode_to_bytes` is only pulled in when
/// there are known tagged fields to encode.
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

pub(crate) fn emit_imports(
    spec: &MessageSpec,
    res_map: &HashMap<String, Resolution>,
) -> TokenStream {
    let types = used_field_types_recursive(&spec.fields);
    let tagged = has_any_tagged_in_spec(spec);
    let flex = has_any_flex(spec);
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);
    let use_nullable_string = uses_nullable_string_recursive(&spec.fields);
    let use_nullable_bytes = uses_nullable_bytes_recursive(&spec.fields);
    let use_non_nullable_bytes = uses_non_nullable_bytes_recursive(&spec.fields);

    let use_nullable_records = uses_nullable_records_recursive(&spec.fields);
    let use_non_nullable_records = uses_non_nullable_records_recursive(&spec.fields);
    let use_records = use_nullable_records || use_non_nullable_records;
    let use_nullable_struct = uses_nullable_struct_recursive(&spec.fields);

    // `Bytes` is needed for to_owned() on bytes fields.
    // Records fields use `bytes::BytesMut::new()` inline (fully qualified), so no extra import.
    let mut out = if use_bytes {
        quote!(
            use bytes::{Bytes, BufMut};
        )
    } else {
        quote!(
            use bytes::BufMut;
        )
    };

    // Emit only the specific fixed-type imports actually used, to avoid unused-import warnings.
    out.extend(fixed_import(&types, use_nullable_struct));

    if use_string {
        out.extend(string_import(use_nullable_string));
    }

    if use_bytes {
        out.extend(bytes_import(use_non_nullable_bytes, use_nullable_bytes));
    }

    // Records fields: import the byte-primitive helpers used in the generated wrapper code.
    // Note: compact_bytes_len_from_size is used via fully-qualified path in generated code.
    if use_records {
        let mut put_items: Vec<&str> = Vec::new();
        let mut get_borrowed_items: Vec<&str> = Vec::new();
        if use_non_nullable_records {
            put_items.extend(["put_bytes", "put_compact_bytes"]);
            get_borrowed_items.extend(["get_bytes_borrowed", "get_compact_bytes_borrowed"]);
        }
        if use_nullable_records {
            put_items.extend([
                "put_bytes",
                "put_compact_bytes",
                "put_compact_nullable_bytes",
                "put_nullable_bytes",
            ]);
            get_borrowed_items.extend([
                "get_compact_nullable_bytes_borrowed",
                "get_nullable_bytes_borrowed",
            ]);
        }
        put_items.sort_unstable();
        put_items.dedup();
        get_borrowed_items.sort_unstable();
        get_borrowed_items.dedup();
        let puts: Vec<_> = put_items.iter().map(|n| format_ident!("{n}")).collect();
        out.extend(quote!(use crate::primitives::string_bytes::{ #(#puts),* };));
        if !get_borrowed_items.is_empty() {
            let gets: Vec<_> = get_borrowed_items
                .iter()
                .map(|n| format_ident!("{n}"))
                .collect();
            out.extend(quote!(use crate::primitives::string_bytes_borrowed::{ #(#gets),* };));
        }
    }

    out.extend(tagged_import(flex, tagged));

    // `Decode` is needed when any tagged field uses owned decode (to call the trait method).
    if has_tagged_fields_needing_owned(spec, res_map) {
        out.extend(quote!(
            use crate::{Decode, DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields};
        ));
    } else {
        out.extend(quote!(
            use crate::{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields};
        ));
    }
    out
}

/// Build the `use` items for a borrowed common-struct file body.
///
/// Replicates the import selection of the (former) `emit_common_struct_file_borrowed`:
/// it has NO records-import block (common structs never carry `records` fields)
/// and never pulls in the owned `Decode` trait (common structs have no top-level
/// tagged-owned fields).
pub(crate) fn emit_common_imports(fields: &[FieldSpec], flex_min_val: i16) -> TokenStream {
    let types = used_field_types_recursive(fields);
    let has_flex = flex_min_val < i16::MAX;
    let tagged = fields.iter().any(|f| f.tag.is_some());
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);
    let use_nullable_struct = uses_nullable_struct_recursive(fields);

    let mut out = if use_bytes {
        quote!(
            use bytes::{Bytes, BufMut};
        )
    } else {
        quote!(
            use bytes::BufMut;
        )
    };

    out.extend(fixed_import(&types, use_nullable_struct));

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
        use crate::{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields};
    ));
    out
}

// ── constants ──────────────────────────────────────────────────────────────

pub(crate) fn emit_constants(spec: &MessageSpec) -> TokenStream {
    let min_version = proc_macro2::Literal::i16_unsuffixed(spec.valid_versions.min);
    let max_version = proc_macro2::Literal::i16_unsuffixed(spec.valid_versions.max);
    let flex_minimum = flex_min(spec);
    let flex = proc_macro2::Literal::i16_unsuffixed(flex_minimum);
    let flex_check = match flex_minimum {
        i16::MIN => quote!(true),
        i16::MAX => quote!(version == FLEXIBLE_MIN),
        _ => quote!(version >= FLEXIBLE_MIN),
    };
    // Request/Response schemas have an API key; Header/Data schemas do not.
    let api_key_const = match spec.message_type {
        MessageType::Request | MessageType::Response => {
            let api_key = proc_macro2::Literal::i16_unsuffixed(
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

/// Returns a Rust expression for the default value of a borrowed field.
pub(crate) fn borrowed_default_expr(
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
            return format!("Some({})", scalar_borrowed_default(base, v));
        }
    }
    // Arrays always default to empty Vec.
    if is_array {
        return "Vec::new()".into();
    }
    if f.default.is_none()
        && let Some(resolution) = res_map.get(base)
    {
        return format!("{}::default()", resolution.rust_path);
    }
    // Non-nullable scalar
    match &f.default {
        Some(v) => scalar_borrowed_default(base, v),
        None => borrowed_zero(base),
    }
}

fn scalar_borrowed_default(base_type: &str, val: &serde_json::Value) -> String {
    // Kafka schema defaults are always stored as JSON strings (e.g., "-1", "true", "false").
    match (base_type, val) {
        (_, serde_json::Value::String(s)) if s == "null" => "None".into(),
        ("string", serde_json::Value::String(s)) if s.is_empty() => "\"\"".into(),
        ("string", serde_json::Value::String(s)) => format!("\"{s}\""),
        ("bool", serde_json::Value::Bool(b)) => b.to_string(),
        ("bool", serde_json::Value::String(s)) => match s.as_str() {
            "true" => "true".into(),
            _ => "false".into(),
        },
        ("int8", serde_json::Value::String(s)) => format!("{}i8", s.trim()),
        ("int16", serde_json::Value::String(s)) => format!("{}i16", s.trim()),
        // Use underscored literals to satisfy clippy::unreadable_literal for large defaults.
        ("int32", serde_json::Value::String(s)) => format_int_literal(s.trim(), "i32"),
        ("int64", serde_json::Value::String(s)) => format_int_literal(s.trim(), "i64"),
        ("int8", serde_json::Value::Number(n)) => format!("{n}i8"),
        ("int16", serde_json::Value::Number(n)) => format!("{n}i16"),
        ("int32", serde_json::Value::Number(n)) => format_int_literal(&n.to_string(), "i32"),
        ("int64", serde_json::Value::Number(n)) => format_int_literal(&n.to_string(), "i64"),
        _ => borrowed_zero(base_type),
    }
}

fn borrowed_zero(base: &str) -> String {
    match base {
        "string" => "\"\"".into(),
        "bytes" => "&[]".into(),
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

pub(crate) fn to_owned_field_expr(schema_type: &str, expr: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            if nullable {
                return format!(
                    "({expr}).as_ref().map(|v| v.iter().map(|it| it.to_owned()).collect())"
                );
            }
            return format!("({expr}).iter().map(|it| it.to_owned()).collect()");
        }
        // Primitive arrays — Copy types or string slices
        match elem {
            "string" if nullable => {
                return format!(
                    "({expr}).as_ref().map(|v| v.iter().map(|s| s.to_string()).collect())"
                );
            }
            "string" => {
                return format!("({expr}).iter().map(|s| s.to_string()).collect()");
            }
            "bytes" if nullable => {
                return format!(
                    "({expr}).as_ref().map(|v| v.iter().map(|b| Bytes::copy_from_slice(b)).collect())"
                );
            }
            "bytes" => {
                return format!("({expr}).iter().map(|b| Bytes::copy_from_slice(b)).collect()");
            }
            "records" if nullable => {
                return format!(
                    "({expr}).as_ref().map(|v| v.iter().map(|rb| rb.to_owned()).collect())"
                );
            }
            "records" => {
                return format!("({expr}).iter().map(|rb| rb.to_owned()).collect()");
            }
            _ => {
                // Copy types (int*, bool, float64, uuid) — owned Vec<T> is the same
                if nullable {
                    return format!("({expr}).clone()");
                }
                return format!("({expr}).clone()");
            }
        }
    }

    if is_struct_type(schema_type) {
        if nullable {
            return format!("({expr}).as_ref().map(|v| v.to_owned())");
        }
        return format!("({expr}).to_owned()");
    }

    match (schema_type, nullable) {
        ("string", false) => format!("({expr}).to_string()"),
        ("string", true) => format!("({expr}).map(|s| s.to_string())"),
        ("bytes", false) => format!("Bytes::copy_from_slice({expr})"),
        ("bytes", true) => format!("({expr}).map(Bytes::copy_from_slice)"),
        ("records", false) => format!("({expr}).to_owned().expect(\"records to_owned\")"),
        ("records", true) => {
            format!("({expr}).as_ref().map(|rb| rb.to_owned().expect(\"records to_owned\"))")
        }
        _ => {
            // Copy types (int*, bool, float64, uuid) — owned is the same; just copy
            format!("({expr})")
        }
    }
}

/// Returns `true` if this field has `"flexibleVersions": "none"` (per-field override),
/// meaning it must always use the legacy (non-compact) codec even in flex message versions.
pub(crate) fn field_forces_non_flex(f: &FieldSpec) -> bool {
    matches!(f.flexible_versions, Some(FlexibleVersions::None))
}

/// Encode a field whose Rust type is `Option<T>` but wire format is non-nullable.
/// Treats None as empty/default for the underlying type.
pub(crate) fn encode_call_option_as_non_nullable(schema_type: &str, expr: &str) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            return format!(
                "{{ let v = ({expr}).as_ref().map(Vec::as_slice).unwrap_or(&[]); \
                 crate::primitives::array::put_array_len(buf, v.len(), flex); \
                 for it in v {{ it.encode(buf, version)?; }} }}"
            );
        }
        return format!(
            "{{ let v = ({expr}).as_ref().map(Vec::as_slice).unwrap_or(&[]); \
             crate::primitives::array::put_array_len(buf, v.len(), flex); \
             for it in v {{ {inner}; }} }}",
            inner = encode_call(elem, "it", false),
        );
    }
    match schema_type {
        "string" => format!(
            "if flex {{ let () = put_compact_string(buf, ({expr}).unwrap_or(\"\")); }} \
             else {{ let () = put_string(buf, ({expr}).unwrap_or(\"\")); }}"
        ),
        "uuid" => format!("crate::primitives::uuid::put_uuid(buf, ({expr}).unwrap_or_default())"),
        // `records` can't go through `unwrap_or_default()` (that would move out of
        // `&self`), so match by reference and encode an empty payload for None.
        "records" => format!(
            "match &{expr} {{ \
                None => {{ let __rb_buf = bytes::BytesMut::new(); if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} }}, \
                Some(__rb) => {{ let mut __rb_buf = bytes::BytesMut::new(); <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(__rb, &mut __rb_buf, version)?; if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} }} \
            }}"
        ),
        _ => encode_call(schema_type, &format!("({expr}).unwrap_or_default()"), false),
    }
}

/// Compute `encoded_len` for a field whose Rust type is `Option<T>` but wire is non-nullable.
pub(crate) fn encoded_len_expr_option_as_non_nullable(schema_type: &str, expr: &str) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            return format!(
                "{{ let v = ({expr}).as_ref().map(Vec::as_slice).unwrap_or(&[]); \
                 let prefix = crate::primitives::array::array_len_prefix_len(v.len(), flex); \
                 let body: usize = v.iter().map(|it| it.encoded_len(version)).sum(); \
                 prefix + body }}"
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
            "if flex {{ compact_string_len(({expr}).unwrap_or(\"\")) }} \
             else {{ string_len(({expr}).unwrap_or(\"\")) }}"
        ),
        "uuid" => "16".into(),
        "records" => format!(
            "match &{expr} {{ \
                None => if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(0) }} else {{ 4 }}, \
                Some(__rb) => {{ let __rb_len = <crate::records::RecordsPayloadBorrowed as crate::Encode>::encoded_len(__rb, version); if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) }} else {{ 4 + __rb_len }} }} \
            }}"
        ),
        _ => encoded_len_expr(schema_type, &format!("({expr}).unwrap_or_default()"), false),
    }
}

/// Returns a Rust boolean expression that is `true` when the tagged field
/// equals its schema-specified default. Mirrors `owned::tagged_is_default_cond`.
pub(crate) fn tagged_is_default_cond(f: &FieldSpec) -> String {
    let field = name_conv::field_name(&f.name);
    let base = base_type(&f.field_type);
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");

    if nullable && (default_is_null || f.default.is_none()) {
        return format!("self.{field}.is_none()");
    }
    if let Some(v) = &f.default {
        let cmp_val = scalar_borrowed_default(base, v);
        if cmp_val == "None" {
            return format!("self.{field}.is_none()");
        }
        if nullable {
            return format!("self.{field} == Some({cmp_val})");
        }
        if f.field_type.starts_with("[]") {
            return format!("self.{field}.is_empty()");
        }
        return format!("self.{field} == {cmp_val}");
    }
    format!("crate::codegen_helpers::is_default(&self.{field})")
}

/// Like `encoded_len_expr` but uses owned-type conventions (`.as_deref()`).
/// Used for tagged fields stored as `String`/`Bytes` (owned) in borrowed structs.
pub(crate) fn owned_encoded_len_expr(schema_type: &str, expr: &str, nullable: bool) -> String {
    match (schema_type, nullable) {
        ("string", false) => {
            format!("if flex {{ compact_string_len(&{expr}) }} else {{ string_len(&{expr}) }}")
        }
        ("string", true) => format!(
            "if flex {{ compact_nullable_string_len({expr}.as_deref()) }} else {{ nullable_string_len({expr}.as_deref()) }}"
        ),
        ("bytes", false) => format!(
            "if flex {{ crate::primitives::varint::uvarint_len(u32::try_from(({expr}).len() + 1).unwrap()) + ({expr}).len() }} \
             else {{ 4 + ({expr}).len() }}"
        ),
        ("bytes", true) => format!(
            "match {expr}.as_deref() {{ \
             None => if flex {{ 1 }} else {{ 4 }}, \
             Some(b) => if flex {{ crate::primitives::varint::uvarint_len(u32::try_from(b.len() + 1).unwrap()) + b.len() }} \
             else {{ 4 + b.len() }} }}"
        ),
        _ => encoded_len_expr(schema_type, expr, nullable),
    }
}

/// Like `encode_call` but uses owned-type conventions (`.as_deref()`, `&expr`).
/// Used for tagged fields that are stored as `String`/`Bytes` (owned) in the
/// borrowed struct because their data can't borrow from the input buffer.
pub(crate) fn owned_encode_call(schema_type: &str, expr: &str, nullable: bool) -> String {
    match (schema_type, nullable) {
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
        // For non-string/bytes types, fall back to the borrowed encode call.
        _ => encode_call(schema_type, expr, nullable),
    }
}

/// Build the populated-value expression for one borrowed field.
pub(crate) fn borrowed_populated_value(
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    parent_module: &str,
    option: bool,
) -> String {
    let base = base_type(&f.field_type);
    let is_array = f.field_type.starts_with("[]");
    let owned = is_tagged(f) && tagged_field_needs_owned(f, res_map);
    let elem = borrowed_populated_scalar(base, f, res_map, parent_module, owned);
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

fn borrowed_populated_scalar(
    base: &str,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    parent_module: &str,
    owned: bool,
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
        "uuid" => "crate::primitives::uuid::Uuid([1u8; 16])".to_string(),
        "string" if owned => "\"x\".to_string()".to_string(),
        "string" => "\"x\"".to_string(),
        "bytes" if owned => "::bytes::Bytes::from_static(b\"x\")".to_string(),
        "bytes" => "&b\"x\"[..]".to_string(),
        _ if owned => {
            let path = owned_struct_path_for(f, parent_module, res_map)
                .expect("struct field must resolve");
            format!("{path}::populated(version)")
        }
        _ => {
            let path = res_map
                .get(base)
                .map(|r| r.rust_path.clone())
                .expect("struct field must resolve");
            format!("{path}::populated(version)")
        }
    }
}

// ── primitive encode/decode call generators ────────────────────────────────

pub(crate) fn encode_call(schema_type: &str, expr: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
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

    // Borrowed strings/bytes: `expr` is `&str` or `&[u8]` — no extra `&` needed.
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
            "if flex {{ let () = put_compact_string(buf, {expr}); }} else {{ let () = put_string(buf, {expr}); }}"
        ),
        ("string", true) => format!(
            "if flex {{ let () = put_compact_nullable_string(buf, {expr}); }} else {{ let () = put_nullable_string(buf, {expr}); }}"
        ),
        ("bytes", false) => format!(
            "if flex {{ let () = put_compact_bytes(buf, {expr}); }} else {{ let () = put_bytes(buf, {expr}); }}"
        ),
        ("bytes", true) => format!(
            "if flex {{ let () = put_compact_nullable_bytes(buf, {expr}); }} else {{ let () = put_nullable_bytes(buf, {expr}); }}"
        ),
        ("records", false) => format!(
            "{{ \
                let mut __rb_buf = bytes::BytesMut::new(); \
                <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(&{expr}, &mut __rb_buf, version)?; \
                if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} \
            }}"
        ),
        ("records", true) => format!(
            "match &{expr} {{ \
                None => if flex {{ let () = put_compact_nullable_bytes(buf, None); }} else {{ let () = put_nullable_bytes(buf, None); }}, \
                Some(__rb) => {{ \
                    let mut __rb_buf = bytes::BytesMut::new(); \
                    <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(__rb, &mut __rb_buf, version)?; \
                    if flex {{ let () = put_compact_bytes(buf, &__rb_buf); }} else {{ let () = put_bytes(buf, &__rb_buf); }} \
                }} \
            }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encode_call (borrowed): {t}\")"),
    }
}

pub(crate) fn encoded_len_expr(schema_type: &str, expr: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            if nullable {
                return format!(
                    "{{ let opt: Option<&Vec<_>> = ({expr}).as_ref(); \
                     let prefix = crate::primitives::array::nullable_array_len_prefix_len(opt.map(|v| v.len()), flex); \
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
            let closure_arg = if inner.contains("*it") { "it" } else { "_" };
            if nullable {
                return format!(
                    "{{ let opt: Option<&Vec<_>> = ({expr}).as_ref(); \
                     let prefix = crate::primitives::array::nullable_array_len_prefix_len(opt.map(|v| v.len()), flex); \
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

    // Borrowed strings/bytes: `expr` is `&str` or `&[u8]`.
    match (schema_type, nullable) {
        ("int8" | "bool", _) => "1".into(),
        ("int16" | "uint16", _) => "2".into(),
        ("int32", _) => "4".into(),
        ("int64" | "float64", _) => "8".into(),
        ("uuid", _) => "16".into(),
        ("string", false) => {
            format!("if flex {{ compact_string_len({expr}) }} else {{ string_len({expr}) }}")
        }
        ("string", true) => format!(
            "if flex {{ compact_nullable_string_len({expr}) }} else {{ nullable_string_len({expr}) }}"
        ),
        ("bytes", false) => format!(
            "if flex {{ crate::primitives::varint::uvarint_len(u32::try_from(({expr}).len() + 1).unwrap()) + ({expr}).len() }} \
             else {{ 4 + ({expr}).len() }}"
        ),
        ("bytes", true) => format!(
            "match {expr} {{ \
             None => if flex {{ 1 }} else {{ 4 }}, \
             Some(b) => if flex {{ crate::primitives::varint::uvarint_len(u32::try_from(b.len() + 1).unwrap()) + b.len() }} \
             else {{ 4 + b.len() }} }}"
        ),
        ("records", false) => format!(
            "{{ let __rb_len = <crate::records::RecordsPayloadBorrowed as crate::Encode>::encoded_len(&({expr}), version); \
               if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) }} \
               else {{ 4 + __rb_len }} }}"
        ),
        ("records", true) => format!(
            "match &{expr} {{ \
                None => if flex {{ crate::primitives::varint::uvarint_len(0) }} else {{ 4 }}, \
                Some(__rb) => {{ let __rb_len = <crate::records::RecordsPayloadBorrowed as crate::Encode>::encoded_len(__rb, version); \
                    if flex {{ crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) }} \
                    else {{ 4 + __rb_len }} }} \
            }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encoded_len_expr (borrowed): {t}\")"),
    }
}

pub(crate) fn decode_borrow_call(
    schema_type: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            let type_path = res_map
                .get(elem_base)
                .map_or(elem_base, |r| r.rust_path.as_str());
            if nullable {
                return format!(
                    "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                     match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                     for _ in 0..n {{ v.push({type_path}::decode_borrow(buf, version)?); }} Some(v) }} }} }}",
                );
            }
            return format!(
                "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
                 let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({type_path}::decode_borrow(buf, version)?); }} v }}",
            );
        }
        if nullable {
            return format!(
                "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                 match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({inner}); }} Some(v) }} }} }}",
                inner = decode_borrow_call(elem, false, res_map),
            );
        }
        return format!(
            "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
             let mut v = Vec::with_capacity(n); for _ in 0..n {{ v.push({inner}); }} v }}",
            inner = decode_borrow_call(elem, false, res_map),
        );
    }

    if is_struct_type(schema_type) {
        let type_path = res_map
            .get(schema_type)
            .map_or(schema_type, |r| r.rust_path.as_str());
        if nullable {
            // Nullable non-array structs: 1-byte signed prefix < 0 = null, else non-null.
            return format!(
                "if get_i8(buf)? < 0 {{ None }} else {{ Some({type_path}::decode_borrow(buf, version)?) }}"
            );
        }
        return format!("{type_path}::decode_borrow(buf, version)?");
    }

    match (schema_type, nullable) {
        ("int8",    _) => "get_i8(buf)?".into(),
        ("int16",   _) => "get_i16(buf)?".into(),
        ("uint16",  _) => "get_u16(buf)?".into(),
        ("int32",   _) => "get_i32(buf)?".into(),
        ("int64",   _) => "get_i64(buf)?".into(),
        ("bool",    _) => "get_bool(buf)?".into(),
        ("float64", _) => "get_f64(buf)?".into(),
        ("uuid",    _) => "crate::primitives::uuid::get_uuid(buf)?".into(),
        ("string", false) => {
            "if flex { get_compact_string_borrowed(buf)? } else { get_string_borrowed(buf)? }".into()
        }
        ("string", true) => {
            "if flex { get_compact_nullable_string_borrowed(buf)? } else { get_nullable_string_borrowed(buf)? }".into()
        }
        ("bytes", false) => {
            "if flex { get_compact_bytes_borrowed(buf)? } else { get_bytes_borrowed(buf)? }".into()
        }
        ("bytes", true) => {
            "if flex { get_compact_nullable_bytes_borrowed(buf)? } else { get_nullable_bytes_borrowed(buf)? }".into()
        }
        ("records", false) => "{ \
            let __rb_slice = if flex { get_compact_bytes_borrowed(buf)? } else { get_bytes_borrowed(buf)? }; \
            let mut __rb_cur = __rb_slice; \
            <crate::records::RecordsPayloadBorrowed as crate::DecodeBorrow>::decode_borrow(&mut __rb_cur, version)? \
        }".into(),
        ("records", true) => "{ \
            let __rb_opt = if flex { get_compact_nullable_bytes_borrowed(buf)? } else { get_nullable_bytes_borrowed(buf)? }; \
            match __rb_opt { \
                None => None, \
                Some(__rb_slice) => { \
                    let mut __rb_cur = __rb_slice; \
                    Some(<crate::records::RecordsPayloadBorrowed as crate::DecodeBorrow>::decode_borrow(&mut __rb_cur, version)?) \
                } \
            } \
        }".into(),
        (t, _) => format!("compile_error!(\"unhandled type in decode_borrow_call: {t}\")"),
    }
}

/// Generate a decode call that produces an **owned** value (using `Decode`, not `DecodeBorrow`).
/// Used for tagged fields whose content cannot be zero-copy decoded (strings, owned structs).
pub(crate) fn decode_owned_call(
    schema_type: &str,
    nullable: bool,
    parent_module: &str,
    res_map: &HashMap<String, Resolution>,
) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            // Use the resolved rust_path but rewrite it to the owned crate path.
            // For inline nested structs: rust_path = "TypeName" → "crate::owned::{parent_module}::TypeName"
            // For common structs: rust_path = "super::<snake>::TypeName" → "crate::owned::common::<snake>::TypeName"
            let owned_path = resolved_to_owned_path(elem_base, parent_module, res_map);
            if nullable {
                return format!(
                    "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                     match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                     for _ in 0..n {{ v.push({owned_path}::decode(buf, version)?); }} Some(v) }} }} }}"
                );
            }
            return format!(
                "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
                 let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({owned_path}::decode(buf, version)?); }} v }}"
            );
        }
        // Primitive array element
        if nullable {
            return format!(
                "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                 match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({inner}); }} Some(v) }} }} }}",
                inner = decode_owned_call(elem, false, parent_module, res_map),
            );
        }
        return format!(
            "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
             let mut v = Vec::with_capacity(n); for _ in 0..n {{ v.push({inner}); }} v }}",
            inner = decode_owned_call(elem, false, parent_module, res_map),
        );
    }

    if is_struct_type(schema_type) {
        let owned_path = resolved_to_owned_path(schema_type, parent_module, res_map);
        if nullable {
            return format!("Some({owned_path}::decode(buf, version)?)");
        }
        return format!("{owned_path}::decode(buf, version)?");
    }

    // Primitive types: same as borrow decode (no lifetime involved)
    match (schema_type, nullable) {
        ("int8", _) => "get_i8(buf)?".into(),
        ("int16", _) => "get_i16(buf)?".into(),
        ("uint16", _) => "get_u16(buf)?".into(),
        ("int32", _) => "get_i32(buf)?".into(),
        ("int64", _) => "get_i64(buf)?".into(),
        ("bool", _) => "get_bool(buf)?".into(),
        ("float64", _) => "get_f64(buf)?".into(),
        ("uuid", _) => "crate::primitives::uuid::get_uuid(buf)?".into(),
        ("string", false) => {
            "if flex { crate::primitives::string_bytes::get_compact_string_owned(buf)? } \
             else { crate::primitives::string_bytes::get_string_owned(buf)? }"
                .into()
        }
        ("string", true) => {
            "if flex { crate::primitives::string_bytes::get_compact_nullable_string_owned(buf)? } \
             else { crate::primitives::string_bytes::get_nullable_string_owned(buf)? }"
                .into()
        }
        (t, _) => format!("compile_error!(\"unhandled type in decode_owned_call: {t}\")"),
    }
}
