//! Emit Rust source for the owned flavor of a `MessageSpec`.
//!
//! Handles primitive fields, tagged fields, primitive arrays, and nested
//! struct fields. Nested anonymous structs become sibling types in the same
//! generated file. Supports `commonStructs`.

use std::collections::HashMap;
use std::fmt::Write;

use crate::emit::EmittedMessage;
use crate::emit::common::{banner, format_int_literal};
use crate::emit::default_json;
use crate::emit::protocol_request;
use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, MessageType, VersionRange};
use crate::name_conv;
use crate::resolve::{self, Resolution, StructKind};
use crate::type_map;

#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("unsupported (in 1a): {0}")]
    Unsupported(String),
    #[error("resolve error: {0}")]
    Resolve(#[from] resolve::ResolveError),
}

pub fn emit(spec: &MessageSpec, schemas_version: &str) -> Result<EmittedMessage, EmitError> {
    // Build resolution map — validates that all struct references resolve.
    let res_map = resolve::resolve_message(spec)?;

    // Response-message records fields decode leniently (tolerate a truncated
    // trailing batch); request-message records stay strict.
    let lenient_records = matches!(spec.message_type, MessageType::Response);

    let mut primary = banner(schemas_version);
    emit_imports(&mut primary, spec);
    emit_constants(&mut primary, spec);
    emit_struct(&mut primary, spec, &res_map);
    emit_encode_impl(&mut primary, spec, &res_map);
    emit_decode_impl(&mut primary, spec, &res_map, lenient_records);
    emit_populated_impl(
        &mut primary,
        &name_conv::type_name(&spec.name),
        &spec.fields,
        &res_map,
    );

    // Emit sibling types for nested structs (depth-first, post-order so parent
    // types appear before their children's children — order doesn't matter for
    // Rust, but reading top-down is nicer).
    let fm = flex_min(spec);
    emit_nested_structs_for_fields(&mut primary, &spec.fields, fm, &res_map, lenient_records);

    // Emit the default_json() helper for differential testing against the JVM oracle.
    primary.push_str(&default_json::emit_default_json(spec));

    // Emit `impl crate::ProtocolRequest` for Request-typed messages.
    if let Some(impl_block) = protocol_request::emit_protocol_request(spec) {
        primary.push_str(&impl_block);
    }

    // Emit common structs into separate file bodies.
    //
    // `commonStructs` are message-local: each is emitted under a per-message
    // nested module `common/<message_snake>/<struct_snake>`. The `commons` key
    // is the relative path stem `<message_snake>/<struct_snake>`; the caller
    // turns that into the on-disk body path and the wrapper module nesting.
    let message_snake = name_conv::module_name(&spec.name);
    let mut commons: Vec<(String, String)> = Vec::new();
    for cs in &spec.common_structs {
        let cs_flex_min = flex_min(spec); // common structs inherit message flex threshold
        // Build a modified res_map for the common-struct context:
        // Common-struct references use sibling paths `super::<struct_snake>::TypeName`
        // (the body lands in `src/{flavor}/common/<message_snake>/<struct_snake>.rs`,
        // and sibling common structs of the same message share that parent module).
        let common_res_map: HashMap<String, Resolution> = res_map
            .iter()
            .map(|(k, v)| {
                let new_path = if v.kind == StructKind::Common {
                    let snake = name_conv::module_name(k);
                    format!("super::{snake}::{k}")
                } else {
                    v.rust_path.clone()
                };
                (
                    k.clone(),
                    Resolution {
                        kind: v.kind.clone(),
                        rust_path: new_path,
                        needs_lifetime: v.needs_lifetime,
                    },
                )
            })
            .collect();
        let body = emit_common_struct_file(
            &cs.name,
            &cs.fields,
            cs_flex_min,
            &common_res_map,
            schemas_version,
            lenient_records,
        );
        let cs_snake = name_conv::module_name(&cs.name);
        commons.push((format!("{message_snake}/{cs_snake}"), body));
    }

    Ok(EmittedMessage { primary, commons })
}

/// Emit a standalone `.rs` file body for a top-level `commonStruct` entry.
/// The file has the same imports as a primary message file but contains ONLY
/// the struct definition + Encode/Decode impls for that single struct.
#[allow(clippy::too_many_lines)]
fn emit_common_struct_file(
    struct_name: &str,
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &HashMap<String, Resolution>,
    schemas_version: &str,
    lenient_records: bool,
) -> String {
    let types = used_field_types_recursive(fields);
    let has_flex = flex_min_val < i16::MAX;
    let tagged = fields.iter().any(|f| f.tag.is_some());
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);

    let use_nullable_struct = uses_nullable_struct_recursive(fields);
    let mut out = banner(schemas_version);

    writeln!(out, "\nuse bytes::{{Buf, BufMut}};").unwrap();
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
            writeln!(
                out,
                "\nuse crate::primitives::fixed::{{{}}};",
                combined.join(", ")
            )
            .unwrap();
        }
    }
    if use_string {
        if uses_nullable_string_recursive(fields) {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{\n    compact_nullable_string_len, compact_string_len, get_compact_nullable_string_owned,\n    get_compact_string_owned, get_nullable_string_owned, get_string_owned, nullable_string_len,\n    put_compact_nullable_string, put_compact_string, put_nullable_string, put_string,\n    string_len,\n}};"
            ).unwrap();
        } else {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{\n    compact_string_len, get_compact_string_owned, get_string_owned,\n    put_compact_string, put_string, string_len,\n}};"
            ).unwrap();
        }
    }
    if use_bytes {
        let mut items: Vec<&str> = Vec::new();
        if uses_non_nullable_bytes_recursive(fields) {
            items.extend([
                "bytes_len",
                "compact_bytes_len",
                "get_bytes_owned",
                "get_compact_bytes_owned",
                "put_bytes",
                "put_compact_bytes",
            ]);
        }
        if uses_nullable_bytes_recursive(fields) {
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
        writeln!(
            out,
            "use crate::primitives::string_bytes::{{{}}};",
            items.join(", ")
        )
        .unwrap();
    }
    if has_flex && tagged {
        writeln!(out, "use crate::tagged_fields::{{encode_to_bytes, read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    } else if has_flex {
        writeln!(out, "use crate::tagged_fields::{{read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    }
    writeln!(
        out,
        "use crate::{{Decode, Encode, ProtocolError, UnknownTaggedFields}};"
    )
    .unwrap();

    // Reuse the existing nested-struct emitter (struct + Encode + Decode impls).
    emit_nested_struct(
        &mut out,
        struct_name,
        fields,
        flex_min_val,
        res_map,
        lenient_records,
    );

    out
}

/// Walk fields and emit a nested struct for each field that has its own
/// `fields:` list. Recurses depth-first.
fn emit_nested_structs_for_fields(
    out: &mut String,
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &HashMap<String, Resolution>,
    lenient_records: bool,
) {
    for f in fields {
        if !f.fields.is_empty() {
            let struct_name = base_type(&f.field_type);
            emit_nested_struct(
                out,
                struct_name,
                &f.fields,
                flex_min_val,
                res_map,
                lenient_records,
            );
        }
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

/// Collect the set of primitive schema types actually used by non-tagged fields,
/// so we can emit only the imports that are needed.
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

/// Returns true if any field (recursively) is `float64`.
/// `f64` does not implement `Eq`, so structs with `float64` fields must not derive `Eq`.
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

/// Returns true if any field (recursively) has a string type that is also nullable.
fn uses_nullable_string_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "string" && (f.nullable_versions.is_some() || f.tag.is_some());
        here || uses_nullable_string_recursive(&f.fields)
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

pub(crate) fn has_any_flex(spec: &MessageSpec) -> bool {
    matches!(spec.flexible_versions, FlexibleVersions::Range(_))
}

fn has_any_tagged_in_spec(spec: &MessageSpec) -> bool {
    has_tagged_fields_recursive(&spec.fields)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn emit_imports(out: &mut String, spec: &MessageSpec) {
    let types = used_field_types_recursive(&spec.fields);
    let tagged = has_any_tagged_in_spec(spec);
    let flex = has_any_flex(spec);
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);
    let use_nullable_string = uses_nullable_string_recursive(&spec.fields);
    let use_nullable_bytes = uses_nullable_bytes_recursive(&spec.fields);
    let use_non_nullable_bytes = uses_non_nullable_bytes_recursive(&spec.fields);

    writeln!(out, "\nuse bytes::{{Buf, BufMut}};").unwrap();

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
            writeln!(
                out,
                "\nuse crate::primitives::fixed::{{{}}};",
                sorted.join(", ")
            )
            .unwrap();
        }
    }

    if use_string {
        // Emit only the string helpers that are actually needed for the fields
        // present in this message to avoid unused-import warnings.
        if use_nullable_string {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{
    compact_nullable_string_len, compact_string_len, get_compact_nullable_string_owned,
    get_compact_string_owned, get_nullable_string_owned, get_string_owned, nullable_string_len,
    put_compact_nullable_string, put_compact_string, put_nullable_string, put_string,
    string_len,
}};"
            )
            .unwrap();
        } else {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{
    compact_string_len, get_compact_string_owned, get_string_owned,
    put_compact_string, put_string, string_len,
}};"
            )
            .unwrap();
        }
    }

    if use_bytes {
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
        writeln!(
            out,
            "use crate::primitives::string_bytes::{{{}}};",
            items.join(", ")
        )
        .unwrap();
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
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{{}}};",
                items.join(", ")
            )
            .unwrap();
        }
    }

    // Tagged-fields support: encode_to_bytes only when there are known tagged fields to encode.
    if flex && tagged {
        writeln!(out, "use crate::tagged_fields::{{encode_to_bytes, read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    } else if flex {
        writeln!(out, "use crate::tagged_fields::{{read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    }

    writeln!(
        out,
        "use crate::{{Decode, Encode, ProtocolError, UnknownTaggedFields}};"
    )
    .unwrap();
}

pub(crate) fn emit_constants(out: &mut String, spec: &MessageSpec) {
    let min_version = spec.valid_versions.min;
    let max_version = spec.valid_versions.max;
    let flex = flex_min(spec);
    // Request/Response schemas have an API key; Header/Data schemas do not.
    match spec.message_type {
        MessageType::Request | MessageType::Response => {
            let api_key = spec
                .api_key
                .expect("Request/Response must have apiKey in schema");
            writeln!(out, "\npub const API_KEY: i16 = {api_key};").unwrap();
        }
        MessageType::Header | MessageType::Data => {
            // No API_KEY const for framing/data types.
        }
    }
    writeln!(
        out,
        "pub const MIN_VERSION: i16 = {min_version};
pub const MAX_VERSION: i16 = {max_version};
pub const FLEXIBLE_MIN: i16 = {flex};

#[inline]
fn is_flexible(version: i16) -> bool {{ version >= FLEXIBLE_MIN }}"
    )
    .unwrap();
}

/// Returns a Rust expression for the default value of an owned field, respecting the
/// schema-level `default` attribute (e.g. `"-1"` for `ControllerId`).
pub(crate) fn owned_default_expr(f: &FieldSpec) -> String {
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
        _ => "Default::default()".into(),
    }
}

/// Parse a string schema default as an integer for comparison with zero.
fn parse_string_default_as_i64(s: &str) -> Option<i64> {
    s.trim().parse::<i64>().ok()
}

/// Returns true if any field in `fields` has a non-trivial schema default
/// (one that differs from the Rust type's natural Default).
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

fn emit_struct(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    let manual_default = needs_manual_default(&spec.fields);
    let derive_default = if manual_default { "" } else { ", Default" };
    let eq_derive = if has_float64_recursive(&spec.fields) {
        ""
    } else {
        ", Eq"
    };
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq{eq_derive}{derive_default})]
pub struct {type_name} {{"
    )
    .unwrap();

    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f);
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::owned_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    for f in spec.fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::owned_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    writeln!(out, "    pub unknown_tagged_fields: UnknownTaggedFields,").unwrap();
    writeln!(out, "}}").unwrap();

    if manual_default {
        writeln!(
            out,
            "
impl Default for {type_name} {{
    fn default() -> Self {{
        Self {{"
        )
        .unwrap();
        for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
            let field = name_conv::field_name(&f.name);
            let default_expr = owned_default_expr(f);
            writeln!(out, "            {field}: {default_expr},").unwrap();
        }
        for f in spec.fields.iter().filter(|f| is_tagged(f)) {
            let field = name_conv::field_name(&f.name);
            let default_expr = owned_default_expr(f);
            writeln!(out, "            {field}: {default_expr},").unwrap();
        }
        writeln!(
            out,
            "            unknown_tagged_fields: Default::default(),"
        )
        .unwrap();
        writeln!(out, "        }}\n    }}\n}}").unwrap();
    }
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

fn emit_encode_impl(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    let has_flex = has_any_flex(spec);
    let version_err = match spec.message_type {
        MessageType::Request | MessageType::Response => {
            "return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });"
                .to_owned()
        }
        MessageType::Header | MessageType::Data => {
            let name = &spec.name;
            format!("return Err(ProtocolError::SchemaMismatch(\"{name} version out of range\"));")
        }
    };
    writeln!(
        out,
        "
impl Encode for {type_name} {{
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            {version_err}
        }}
        let flex = is_flexible(version);"
    )
    .unwrap();

    encode_struct_body(out, &spec.fields, res_map, "        ", has_flex);

    writeln!(out, "        Ok(())\n    }}").unwrap();

    // encoded_len
    writeln!(
        out,
        "    fn encoded_len(&self, version: i16) -> usize {{
        let flex = is_flexible(version);
        let mut n: usize = 0;"
    )
    .unwrap();

    encoded_len_struct_body(out, &spec.fields, res_map, "        ", has_flex);

    writeln!(out, "        n\n    }}\n}}").unwrap();
}

fn emit_decode_impl(
    out: &mut String,
    spec: &MessageSpec,
    res_map: &HashMap<String, Resolution>,
    lenient_records: bool,
) {
    let type_name = name_conv::type_name(&spec.name);
    let has_flex = has_any_flex(spec);
    let version_err = match spec.message_type {
        MessageType::Request | MessageType::Response => {
            "return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });"
                .to_owned()
        }
        MessageType::Header | MessageType::Data => {
            let name = &spec.name;
            format!("return Err(ProtocolError::SchemaMismatch(\"{name} version out of range\"));")
        }
    };
    writeln!(
        out,
        "
impl<'de> Decode<'de> for {type_name} {{
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            {version_err}
        }}
        let flex = is_flexible(version);
        let mut out = Self::default();"
    )
    .unwrap();

    decode_struct_body(
        out,
        &spec.fields,
        res_map,
        "        ",
        has_flex,
        lenient_records,
    );

    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();
}

// --- shared struct-body generators (used by top-level and nested emitters) ---

/// Emit the body of an Encode impl for the given fields.
/// Assumes `buf`, `version`, `flex` are in scope.
/// `has_flex` controls whether the tagged-fields trailer block is emitted.
fn encode_struct_body(
    out: &mut String,
    fields: &[FieldSpec],
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    has_flex: bool,
) {
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        emit_encode_one(out, f, res_map, indent);
    }

    if has_flex {
        let has_tagged = fields.iter().any(is_tagged);
        let mut_kw = if has_tagged { "mut " } else { "" };
        writeln!(out, "{indent}if flex {{").unwrap();
        writeln!(
            out,
            "{indent}    let {mut_kw}tagged = WriteTaggedFields::new();"
        )
        .unwrap();
        for f in fields.iter().filter(|f| is_tagged(f)) {
            emit_encode_tagged(out, f, res_map, indent);
        }
        writeln!(
            out,
            "{indent}    tagged.write(buf, &self.unknown_tagged_fields);"
        )
        .unwrap();
        writeln!(out, "{indent}}}").unwrap();
    }
}

/// Emit the body of an `encoded_len` impl for the given fields.
/// Assumes `flex` and `n` are in scope.
/// `has_flex` controls whether the tagged-fields length block is emitted.
fn encoded_len_struct_body(
    out: &mut String,
    fields: &[FieldSpec],
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    has_flex: bool,
) {
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        emit_encoded_len_one(out, f, res_map, indent);
    }

    if has_flex {
        let has_tagged = fields.iter().any(is_tagged);
        let pairs_mut = if has_tagged { "mut " } else { "" };
        writeln!(
            out,
            "{indent}if flex {{
{indent}    let {pairs_mut}known_pairs: Vec<(u32, usize)> = Vec::new();"
        )
        .unwrap();
        for f in fields.iter().filter(|f| is_tagged(f)) {
            emit_encoded_len_tagged(out, f, res_map, indent);
        }
        writeln!(
            out,
            "{indent}    n += tagged_fields_len(&known_pairs, &self.unknown_tagged_fields);
{indent}}}"
        )
        .unwrap();
    }
}

/// Emit the body of a Decode impl for the given fields.
/// Assumes `buf`, `version`, `flex`, and `out` are in scope.
/// `has_flex` controls whether the tagged-fields decode block is emitted.
fn decode_struct_body(
    out: &mut String,
    fields: &[FieldSpec],
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    has_flex: bool,
    lenient_records: bool,
) {
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        emit_decode_one(out, f, res_map, indent, lenient_records);
    }

    if has_flex {
        let has_tagged = fields.iter().any(is_tagged);
        writeln!(out, "{indent}if flex {{").unwrap();
        if has_tagged {
            writeln!(
                out,
                "{indent}    // Pre-declare typed slots for known tagged fields."
            )
            .unwrap();
            for f in fields.iter().filter(|f| is_tagged(f)) {
                let field = name_conv::field_name(&f.name);
                writeln!(out, "{indent}    let mut tag_{field} = None;").unwrap();
            }
        }
        let closure_args = if has_tagged {
            "|tag, payload|"
        } else {
            "|_tag, _payload|"
        };
        writeln!(
            out,
            "{indent}    out.unknown_tagged_fields = read_tagged_fields(buf, {closure_args} {{"
        )
        .unwrap();
        if has_tagged {
            writeln!(out, "{indent}        match tag {{").unwrap();
            for f in fields.iter().filter(|f| is_tagged(f)) {
                emit_decode_tagged_arm(out, f, res_map, indent, lenient_records);
            }
            writeln!(
                out,
                "{indent}            _ => Ok(false),
{indent}        }}"
            )
            .unwrap();
        } else {
            writeln!(out, "{indent}        Ok(false)").unwrap();
        }
        writeln!(out, "{indent}    }})?;").unwrap();
        if has_tagged {
            for f in fields.iter().filter(|f| is_tagged(f)) {
                let field = name_conv::field_name(&f.name);
                writeln!(
                    out,
                    "{indent}    if let Some(v) = tag_{field} {{ out.{field} = v; }}"
                )
                .unwrap();
            }
        }
        writeln!(out, "{indent}}}").unwrap();
    }
}

// --- nested struct emitter -----------------------------------------------

fn emit_nested_struct(
    out: &mut String,
    struct_name: &str,
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &HashMap<String, Resolution>,
    lenient_records: bool,
) {
    let manual_default = needs_manual_default(fields);
    let derive_default = if manual_default { "" } else { ", Default" };
    let eq_derive = if has_float64_recursive(fields) {
        ""
    } else {
        ", Eq"
    };
    // Struct definition
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq{eq_derive}{derive_default})]
pub struct {struct_name} {{"
    )
    .unwrap();

    for f in fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f);
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::owned_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    for f in fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::owned_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    writeln!(out, "    pub unknown_tagged_fields: UnknownTaggedFields,").unwrap();
    writeln!(out, "}}").unwrap();

    if manual_default {
        writeln!(
            out,
            "
impl Default for {struct_name} {{
    fn default() -> Self {{
        Self {{"
        )
        .unwrap();
        for f in fields.iter().filter(|f| !is_tagged(f)) {
            let field = name_conv::field_name(&f.name);
            let default_expr = owned_default_expr(f);
            writeln!(out, "            {field}: {default_expr},").unwrap();
        }
        for f in fields.iter().filter(|f| is_tagged(f)) {
            let field = name_conv::field_name(&f.name);
            let default_expr = owned_default_expr(f);
            writeln!(out, "            {field}: {default_expr},").unwrap();
        }
        writeln!(
            out,
            "            unknown_tagged_fields: Default::default(),"
        )
        .unwrap();
        writeln!(out, "        }}\n    }}\n}}").unwrap();
    }

    let has_flex = flex_min_val < i16::MAX;

    // Encode impl — no version-range guard; version flows in from parent
    writeln!(
        out,
        "
impl Encode for {struct_name} {{
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {{
        let flex = version >= {flex_min_val};"
    )
    .unwrap();

    encode_struct_body(out, fields, res_map, "        ", has_flex);

    writeln!(out, "        Ok(())\n    }}").unwrap();

    writeln!(
        out,
        "    fn encoded_len(&self, version: i16) -> usize {{
        let flex = version >= {flex_min_val};
        let mut n: usize = 0;"
    )
    .unwrap();

    encoded_len_struct_body(out, fields, res_map, "        ", has_flex);

    writeln!(out, "        n\n    }}\n}}").unwrap();

    // Decode impl
    writeln!(
        out,
        "
impl<'de> Decode<'de> for {struct_name} {{
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {{
        let flex = version >= {flex_min_val};
        let mut out = Self::default();"
    )
    .unwrap();

    decode_struct_body(out, fields, res_map, "        ", has_flex, lenient_records);

    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();

    emit_populated_impl(out, struct_name, fields, res_map);

    // Recurse into deeper nesting
    emit_nested_structs_for_fields(out, fields, flex_min_val, res_map, lenient_records);
}

/// Emit a test-only `populated(version)` constructor for an owned struct.
///
/// `default()` leaves every collection empty and every scalar zero, so the
/// derived round-trip tests never reach array-element, nested-struct, or
/// tagged-field encode/decode paths. `populated(version)` starts from
/// `Self::default()` and overwrites each field that is valid at `version` with a
/// non-default value (single-element arrays, recursively populated nested
/// structs). Building on top of `default()` guarantees that fields outside their
/// version range keep their exact default value — important for tagged fields,
/// which the encoder emits whenever the message is flexible (independent of the
/// field's own version range): leaving them at default makes the encoder skip
/// them, so the round-trip stays byte-exact. `records` fields are left at default
/// to avoid constructing invalid record batches.
fn emit_populated_impl(
    out: &mut String,
    type_name: &str,
    fields: &[FieldSpec],
    res_map: &HashMap<String, Resolution>,
) {
    writeln!(
        out,
        "
#[cfg(test)]
impl {type_name} {{
    #[must_use]
    pub fn populated(version: i16) -> Self {{
        let mut m = Self::default();"
    )
    .unwrap();
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        emit_owned_populated_field(out, f, res_map, is_nullable(f));
    }
    for f in fields.iter().filter(|f| is_tagged(f)) {
        let option = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        emit_owned_populated_field(out, f, res_map, option);
    }
    writeln!(out, "        m\n    }}\n}}").unwrap();
}

/// Emit one conditional assignment for the populated constructor, guarding the
/// non-default value behind the field's version range so it matches the bytes
/// the encoder produces at that version. `records` fields are skipped (left at
/// default).
fn emit_owned_populated_field(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    option: bool,
) {
    if base_type(&f.field_type) == "records" {
        return;
    }
    let field = name_conv::field_name(&f.name);
    let value = owned_populated_value(f, res_map, option);
    let cond = version_cond(f.versions, "version");
    writeln!(out, "        if {cond} {{ m.{field} = {value}; }}").unwrap();
}

/// Build the populated-value expression for one owned field. `option` mirrors
/// the field's Rust-type `Option<...>` wrapping as computed by `emit_struct`.
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
        return owned_default_expr(f);
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

/// Returns `true` if this field has `"flexibleVersions": "none"` (per-field override),
/// meaning it must always use the legacy (non-compact) codec even in flex message versions.
pub(crate) fn field_forces_non_flex(f: &FieldSpec) -> bool {
    matches!(f.flexible_versions, Some(FlexibleVersions::None))
}

fn emit_encode_one(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    // Per-field `flexibleVersions: "none"` forces legacy encoding for this field.
    let force_non_flex = field_forces_non_flex(f);
    let flex_prefix = if force_non_flex {
        "{ let flex = false; ".to_owned()
    } else {
        String::new()
    };
    let flex_suffix = if force_non_flex { " }" } else { "" };
    // Version-conditional nullability: the field is nullable only within its
    // nullableVersions range. Where that range is narrower than the field's own
    // versions (on either end), switch codec per version.
    if let Some(ncond) = nullable_split_cond(f) {
        let nullable_body = encode_call(&f.field_type, &format!("self.{field}"), true, res_map);
        // For the non-nullable path, the Rust type is still Option<T> so we must unwrap.
        let non_nullable_body =
            encode_call_option_as_non_nullable(&f.field_type, &format!("self.{field}"), res_map);
        writeln!(
            out,
            "{indent}if {cond} {{ {flex_prefix}if {ncond} {{ {nullable_body} }} else {{ {non_nullable_body} }}{flex_suffix} }}"
        ).unwrap();
    } else {
        let body = encode_call(
            &f.field_type,
            &format!("self.{field}"),
            is_nullable(f),
            res_map,
        );
        writeln!(
            out,
            "{indent}if {cond} {{ {flex_prefix}{body}{flex_suffix} }}"
        )
        .unwrap();
    }
}

fn emit_encoded_len_one(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    let force_non_flex = field_forces_non_flex(f);
    let flex_prefix = if force_non_flex {
        "{ let flex = false; ".to_owned()
    } else {
        String::new()
    };
    let flex_suffix = if force_non_flex { " }" } else { "" };
    if let Some(ncond) = nullable_split_cond(f) {
        let nullable_body =
            encoded_len_expr(&f.field_type, &format!("self.{field}"), true, res_map);
        // For the non-nullable path, the Rust type is still Option<T> so unwrap.
        let non_nullable_body = encoded_len_expr_option_as_non_nullable(
            &f.field_type,
            &format!("self.{field}"),
            res_map,
        );
        writeln!(
            out,
            "{indent}if {cond} {{ n += {flex_prefix}if {ncond} {{ {nullable_body} }} else {{ {non_nullable_body} }}{flex_suffix}; }}"
        ).unwrap();
    } else {
        let body = encoded_len_expr(
            &f.field_type,
            &format!("self.{field}"),
            is_nullable(f),
            res_map,
        );
        writeln!(
            out,
            "{indent}if {cond} {{ n += {flex_prefix}{body}{flex_suffix}; }}"
        )
        .unwrap();
    }
}

fn emit_decode_one(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    lenient_records: bool,
) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    // Per-field `flexibleVersions: "none"` forces legacy encoding for this field.
    let force_non_flex = field_forces_non_flex(f);
    let flex_prefix = if force_non_flex {
        "{ let flex = false; ".to_owned()
    } else {
        String::new()
    };
    let flex_suffix = if force_non_flex { " }" } else { "" };
    if let Some(ncond) = nullable_split_cond(f) {
        let nullable_call = decode_call(&f.field_type, true, res_map, lenient_records);
        let non_nullable_call = decode_call(&f.field_type, false, res_map, lenient_records);
        // Non-nullable decode returns a bare value; we must wrap it to match the Option type.
        let non_nullable_wrapped =
            wrap_non_nullable_for_option(&f.field_type, &non_nullable_call, res_map);
        writeln!(
            out,
            "{indent}if {cond} {{ out.{field} = {flex_prefix}if {ncond} {{ {nullable_call} }} else {{ {non_nullable_wrapped} }}{flex_suffix}; }}"
        ).unwrap();
    } else {
        let call = decode_call(&f.field_type, is_nullable(f), res_map, lenient_records);
        writeln!(
            out,
            "{indent}if {cond} {{ out.{field} = {flex_prefix}{call}{flex_suffix}; }}"
        )
        .unwrap();
    }
}

/// When a non-nullable decode is used but the field type is `Option<T>`, wrap the
/// result in `Some`. For array-of-struct this wraps the whole block.
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
/// equals its schema-specified default. This is used to suppress tagged field
/// serialization (JVM Kafka also omits tagged fields that equal their defaults).
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

fn emit_encode_tagged(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    // Generate encode call using `b` (the closure's buffer parameter, not the outer `buf`).
    let encode_body = encode_call_with_buf(
        &f.field_type,
        &format!("self.{field}"),
        nullable,
        res_map,
        "b",
    );
    let is_default_cond = tagged_is_default_cond(f);
    writeln!(
        out,
        "{indent}    if !({is_default_cond}) {{
{indent}        let payload = encode_to_bytes({len_expr}, |b| {{ {encode_body}; Ok(()) }});
{indent}        tagged.add({tag}, payload);
{indent}    }}",
        len_expr = encoded_len_expr(&f.field_type, &format!("self.{field}"), nullable, res_map),
        tag = tag,
    )
    .unwrap();
}

fn emit_encoded_len_tagged(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    let len = encoded_len_expr(&f.field_type, &format!("self.{field}"), nullable, res_map);
    let is_default_cond = tagged_is_default_cond(f);
    writeln!(
        out,
        "{indent}    if !({is_default_cond}) {{
{indent}        known_pairs.push(({tag}, {len}));
{indent}    }}"
    )
    .unwrap();
}

fn emit_decode_tagged_arm(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    lenient_records: bool,
) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    // Bind `b` to the payload slice so generated decode calls use the right buffer.
    let call = decode_call_with_buf(&f.field_type, nullable, res_map, "b", lenient_records);
    writeln!(out, "{indent}        {tag} => {{ tag_{field} = Some({{ let b: &mut &[u8] = payload; {call} }}); Ok(true) }}").unwrap();
}

// --- primitive encode/decode call generators ------------------------------

/// Encode a field whose Rust type is `Option<T>` but the wire format is non-nullable
/// (because `nullable_versions.min > field.versions.min`).
/// Treats `None` as the empty/default value for the underlying type.
pub(crate) fn encode_call_option_as_non_nullable(
    schema_type: &str,
    expr: &str,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
            inner = encode_call(elem, "it", false, res_map),
        );
    }
    // Option<String> → treat None as ""
    match schema_type {
        "string" => format!(
            "if flex {{ put_compact_string(buf, ({expr}).as_deref().unwrap_or(\"\")) }} \
             else {{ put_string(buf, ({expr}).as_deref().unwrap_or(\"\")) }}"
        ),
        "uuid" => format!("crate::primitives::uuid::put_uuid(buf, ({expr}).unwrap_or_default())"),
        // `records` can't go through `unwrap_or_default()` (that would move out of
        // `&self`), so match by reference and encode an empty payload for None.
        "records" => format!(
            "match &{expr} {{ \
                None => {{ let __rb_buf = bytes::BytesMut::new(); if flex {{ put_compact_bytes(buf, &__rb_buf) }} else {{ put_bytes(buf, &__rb_buf) }} }}, \
                Some(__rb) => {{ let mut __rb_buf = bytes::BytesMut::new(); <crate::records::RecordsPayload as crate::Encode>::encode(__rb, &mut __rb_buf, version)?; if flex {{ put_compact_bytes(buf, &__rb_buf) }} else {{ put_bytes(buf, &__rb_buf) }} }} \
            }}"
        ),
        _ => encode_call(
            schema_type,
            &format!("({expr}).unwrap_or_default()"),
            false,
            res_map,
        ),
    }
}

/// Compute the `encoded_len` of a field whose Rust type is `Option<T>` but wire is non-nullable.
pub(crate) fn encoded_len_expr_option_as_non_nullable(
    schema_type: &str,
    expr: &str,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
            inner = encoded_len_expr(elem, "*it", false, res_map),
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
        _ => encoded_len_expr(
            schema_type,
            &format!("({expr}).unwrap_or_default()"),
            false,
            res_map,
        ),
    }
}

/// Generate an encode call expression using a specific buffer variable name.
/// This is used for tagged-field closures where the buffer is `b` not `buf`.
pub(crate) fn encode_call_with_buf(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
    buf_var: &str,
) -> String {
    // Replace all instances of `buf` in the generated expression with `buf_var`.
    let base = encode_call(schema_type, expr, nullable, res_map);
    // The expressions use `buf` as the buffer name; substitute with the actual var.
    base.replace("buf", buf_var)
}

/// Generate a decode call expression using a specific buffer variable name.
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
#[allow(clippy::only_used_in_recursion)]
pub(crate) fn encode_call(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
                inner = encode_call(elem, "*it", false, res_map),
            );
        }
        return format!(
            "{{ crate::primitives::array::put_array_len(buf, ({expr}).len(), flex); \
             for it in &{expr} {{ {inner}; }} }}",
            inner = encode_call(elem, "*it", false, res_map),
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
            "if flex {{ put_compact_string(buf, &{expr}) }} else {{ put_string(buf, &{expr}) }}"
        ),
        ("string", true) => format!(
            "if flex {{ put_compact_nullable_string(buf, {expr}.as_deref()) }} else {{ put_nullable_string(buf, {expr}.as_deref()) }}"
        ),
        ("bytes", false) => format!(
            "if flex {{ put_compact_bytes(buf, &{expr}) }} else {{ put_bytes(buf, &{expr}) }}"
        ),
        ("bytes", true) => format!(
            "if flex {{ put_compact_nullable_bytes(buf, {expr}.as_deref()) }} else {{ put_nullable_bytes(buf, {expr}.as_deref()) }}"
        ),
        ("records", false) => format!(
            "{{ \
                let mut __rb_buf = bytes::BytesMut::new(); \
                <crate::records::RecordsPayload as crate::Encode>::encode(&{expr}, &mut __rb_buf, version)?; \
                if flex {{ put_compact_bytes(buf, &__rb_buf) }} else {{ put_bytes(buf, &__rb_buf) }} \
            }}"
        ),
        ("records", true) => format!(
            "match &{expr} {{ \
                None => if flex {{ put_compact_nullable_bytes(buf, None) }} else {{ put_nullable_bytes(buf, None) }}, \
                Some(__rb) => {{ \
                    let mut __rb_buf = bytes::BytesMut::new(); \
                    <crate::records::RecordsPayload as crate::Encode>::encode(__rb, &mut __rb_buf, version)?; \
                    if flex {{ put_compact_bytes(buf, &__rb_buf) }} else {{ put_bytes(buf, &__rb_buf) }} \
                }} \
            }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encode_call: {t}\")"),
    }
}

#[allow(clippy::only_used_in_recursion)]
pub(crate) fn encoded_len_expr(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            // Array of structs
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
            let inner = encoded_len_expr(elem, "*it", false, res_map);
            // Use `|_|` when the inner expression is constant (doesn't reference `*it`),
            // to avoid an unused-variable warning.
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

#[allow(clippy::only_used_in_recursion)]
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
        parts.push(format!("version >= {}", r.min));
    }
    if need_upper {
        parts.push(format!("version <= {}", r.max));
    }
    Some(parts.join(" && "))
}

pub(crate) fn version_cond(r: VersionRange, version_var: &str) -> String {
    if r.max == i16::MAX {
        format!("{version_var} >= {}", r.min)
    } else {
        format!("{version_var} >= {} && {version_var} <= {}", r.min, r.max)
    }
}
