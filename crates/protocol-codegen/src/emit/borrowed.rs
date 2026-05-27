//! Emit Rust source for the borrowed flavor of a `MessageSpec`.
//!
//! Mirrors the structure of `emit/owned.rs`. Strings become `&'a str`,
//! bytes become `&'a [u8]`, the struct carries a `'a` lifetime,
//! `DecodeBorrow<'de>` replaces `Decode<'de>`, and `to_owned()` bridges to
//! the matching owned type.

use std::collections::HashMap;
use std::fmt::Write;

use crate::emit::EmittedMessage;
use crate::emit::common::{banner, format_int_literal};
use crate::emit::owned::EmitError;
use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, MessageType, VersionRange};
use crate::name_conv;
use crate::resolve::{self, Resolution, StructKind};
use crate::type_map;

pub fn emit(spec: &MessageSpec, schemas_version: &str) -> Result<EmittedMessage, EmitError> {
    // Build resolution map — validates that all struct references resolve.
    let res_map = resolve::resolve_message(spec)?;

    let parent_module = name_conv::module_name(&spec.name);

    let mut primary = banner(schemas_version);
    emit_imports(&mut primary, spec, &res_map);
    emit_constants(&mut primary, spec);
    emit_struct(&mut primary, spec, &res_map, &parent_module);
    emit_to_owned_impl(&mut primary, spec, &res_map);
    emit_encode_impl(&mut primary, spec, &res_map);
    emit_decode_borrow_impl(&mut primary, spec, &res_map, &parent_module);

    let fm = flex_min(spec);
    emit_nested_structs_for_fields(&mut primary, &spec.fields, fm, &res_map, &parent_module);

    // Emit common structs into separate file bodies.
    let mut commons: Vec<(String, String)> = Vec::new();
    for cs in &spec.common_structs {
        let cs_flex_min = fm; // common structs inherit message flex threshold
        // Build a modified res_map for the common-struct context:
        // - Common-struct references use sibling paths `super::<snake>::TypeName`
        //   (since the file is included under `src/{flavor}/common/<snake>.rs`).
        // - Nested struct references remain unchanged (bare type name, same file).
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
        // Use `common::<snake>` as the parent_module so `to_owned()` emits the correct path
        // `crate::owned::common::<snake>::TypeName`.
        let cs_parent_module = format!("common::{}", name_conv::module_name(&cs.name));
        let body = emit_common_struct_file_borrowed(
            &cs.name,
            &cs.fields,
            cs_flex_min,
            &common_res_map,
            &cs_parent_module,
            schemas_version,
        );
        commons.push((cs.name.clone(), body));
    }

    Ok(EmittedMessage { primary, commons })
}

/// Emit a standalone `.rs` file body for a top-level `commonStruct` in the borrowed flavor.
#[allow(clippy::too_many_lines)]
fn emit_common_struct_file_borrowed(
    struct_name: &str,
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &HashMap<String, Resolution>,
    parent_module: &str,
    schemas_version: &str,
) -> String {
    let types = used_field_types_recursive(fields);
    let has_flex = flex_min_val < i16::MAX;
    let tagged = fields.iter().any(|f| f.tag.is_some());
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);

    let mut out = banner(schemas_version);

    if use_bytes {
        writeln!(out, "\nuse bytes::{{Bytes, BufMut}};").unwrap();
    } else {
        writeln!(out, "\nuse bytes::BufMut;").unwrap();
    }

    let use_nullable_struct = uses_nullable_struct_recursive(fields);
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
                "use crate::primitives::string_bytes::{{\n    compact_nullable_string_len, compact_string_len, nullable_string_len,\n    put_compact_nullable_string, put_compact_string, put_nullable_string, put_string,\n    string_len,\n}};\nuse crate::primitives::string_bytes_borrowed::{{\n    get_compact_nullable_string_borrowed, get_compact_string_borrowed,\n    get_nullable_string_borrowed, get_string_borrowed,\n}};"
            ).unwrap();
        } else {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{\n    compact_string_len, put_compact_string, put_string, string_len,\n}};\nuse crate::primitives::string_bytes_borrowed::{{\n    get_compact_string_borrowed, get_string_borrowed,\n}};"
            ).unwrap();
        }
    }
    if use_bytes {
        let use_non_nullable_bytes = uses_non_nullable_bytes_recursive(fields);
        let use_nullable_bytes = uses_nullable_bytes_recursive(fields);
        let mut put_items: Vec<&str> = Vec::new();
        let mut get_borrowed_items: Vec<&str> = Vec::new();
        if use_non_nullable_bytes {
            put_items.extend(["put_bytes", "put_compact_bytes"]);
            get_borrowed_items.extend(["get_bytes_borrowed", "get_compact_bytes_borrowed"]);
        }
        if use_nullable_bytes {
            put_items.extend(["put_compact_nullable_bytes", "put_nullable_bytes"]);
            get_borrowed_items.extend([
                "get_compact_nullable_bytes_borrowed",
                "get_nullable_bytes_borrowed",
            ]);
        }
        put_items.sort_unstable();
        get_borrowed_items.sort_unstable();
        writeln!(
            out,
            "use crate::primitives::string_bytes::{{{}}};\nuse crate::primitives::string_bytes_borrowed::{{{}}};",
            put_items.join(", "),
            get_borrowed_items.join(", "),
        ).unwrap();
    }
    if has_flex && tagged {
        writeln!(out, "use crate::tagged_fields::{{encode_to_bytes, read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    } else if has_flex {
        writeln!(out, "use crate::tagged_fields::{{read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    }
    writeln!(
        out,
        "use crate::{{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields}};"
    )
    .unwrap();

    // Reuse the existing nested-struct emitter.
    emit_nested_struct(
        &mut out,
        struct_name,
        fields,
        flex_min_val,
        res_map,
        parent_module,
    );

    out
}

// ── helpers shared with owned ──────────────────────────────────────────────

fn flex_min(spec: &MessageSpec) -> i16 {
    match spec.flexible_versions {
        FlexibleVersions::Range(r) => r.min,
        FlexibleVersions::None => i16::MAX,
    }
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_struct_type(t: &str) -> bool {
    t.chars().next().is_some_and(char::is_uppercase)
}

/// Returns true if ANY field in the list (recursively) would carry a borrowed
/// lifetime in the generated Rust type — i.e., string, bytes, records, or a
/// nested struct that itself has borrowed fields.
///
/// `res_map` is consulted for common-struct references (`PascalCase` where `f.fields.is_empty()`)
/// to check whether that common struct was generated with `<'a>`.
fn needs_lifetime(fields: &[crate::ir::FieldSpec], res_map: &HashMap<String, Resolution>) -> bool {
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

fn is_tagged(f: &FieldSpec) -> bool {
    f.tag.is_some()
}

/// Returns true if the top-level struct needs a `'a` lifetime parameter.
/// Only non-tagged fields contribute borrowed lifetimes; tagged fields that have
/// string/struct content use owned types to avoid escape from the payload closure.
fn spec_needs_lifetime(spec: &MessageSpec, res_map: &HashMap<String, Resolution>) -> bool {
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
fn tagged_field_needs_owned(f: &FieldSpec, res_map: &HashMap<String, Resolution>) -> bool {
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

fn is_nullable(f: &FieldSpec) -> bool {
    f.nullable_versions.is_some()
}

fn has_any_flex(spec: &MessageSpec) -> bool {
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
fn has_float64_recursive(fields: &[FieldSpec]) -> bool {
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
        let here = base == "bytes" && f.nullable_versions.is_none();
        here || uses_non_nullable_bytes_recursive(&f.fields)
    })
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
        let here = base == "records" && f.nullable_versions.is_none();
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
fn struct_path_for(f: &FieldSpec, res_map: &HashMap<String, Resolution>) -> Option<String> {
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
fn owned_struct_path_for(
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
/// - Inline nested structs have a bare `rust_path` like `"TypeName"` →
///   `"crate::owned::{parent_module}::TypeName"`.
/// - Common structs from a message-level context have `rust_path` like
///   `"super::common::<snake>::TypeName"` → `"crate::owned::common::<snake>::TypeName"`.
/// - Common structs from a common-struct-level context (`parent_module` = `"common::<x>"`)
///   have `rust_path` like `"super::<snake>::TypeName"` → `"crate::owned::common::<snake>::TypeName"`.
fn resolved_to_owned_path(
    type_name: &str,
    parent_module: &str,
    res_map: &HashMap<String, Resolution>,
) -> String {
    match res_map.get(type_name) {
        Some(r) if r.kind == StructKind::Common => {
            // Determine the owned path from the rust_path stored in the res_map.
            if let Some(without_super) = r.rust_path.strip_prefix("super::common::") {
                // Message-level context: rust_path = "super::common::<snake>::TypeName"
                format!("crate::owned::common::{without_super}")
            } else if let Some(without_super) = r.rust_path.strip_prefix("super::") {
                // Common-struct-level context: rust_path = "super::<snake>::TypeName"
                format!("crate::owned::common::{without_super}")
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

fn version_cond(r: VersionRange, version_var: &str) -> String {
    if r.max == i16::MAX {
        format!("{version_var} >= {}", r.min)
    } else {
        format!("{version_var} >= {} && {version_var} <= {}", r.min, r.max)
    }
}

// ── imports ────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
fn emit_imports(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
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
    if use_bytes {
        writeln!(out, "\nuse bytes::{{Bytes, BufMut}};").unwrap();
    } else {
        writeln!(out, "\nuse bytes::BufMut;").unwrap();
    }

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
        if use_nullable_string {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{
    compact_nullable_string_len, compact_string_len, nullable_string_len,
    put_compact_nullable_string, put_compact_string, put_nullable_string, put_string,
    string_len,
}};
use crate::primitives::string_bytes_borrowed::{{
    get_compact_nullable_string_borrowed, get_compact_string_borrowed,
    get_nullable_string_borrowed, get_string_borrowed,
}};"
            )
            .unwrap();
        } else {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{
    compact_string_len, put_compact_string, put_string, string_len,
}};
use crate::primitives::string_bytes_borrowed::{{
    get_compact_string_borrowed, get_string_borrowed,
}};"
            )
            .unwrap();
        }
    }

    if use_bytes {
        let mut put_items: Vec<&str> = Vec::new();
        let mut get_borrowed_items: Vec<&str> = Vec::new();
        if use_non_nullable_bytes {
            put_items.extend(["put_bytes", "put_compact_bytes"]);
            get_borrowed_items.extend(["get_bytes_borrowed", "get_compact_bytes_borrowed"]);
        }
        if use_nullable_bytes {
            put_items.extend(["put_compact_nullable_bytes", "put_nullable_bytes"]);
            get_borrowed_items.extend([
                "get_compact_nullable_bytes_borrowed",
                "get_nullable_bytes_borrowed",
            ]);
        }
        put_items.sort_unstable();
        get_borrowed_items.sort_unstable();
        writeln!(
            out,
            "use crate::primitives::string_bytes::{{{}}};\nuse crate::primitives::string_bytes_borrowed::{{{}}};",
            put_items.join(", "),
            get_borrowed_items.join(", "),
        )
        .unwrap();
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
        if get_borrowed_items.is_empty() {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{{}}};",
                put_items.join(", "),
            )
            .unwrap();
        } else {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{{}}};\nuse crate::primitives::string_bytes_borrowed::{{{}}};",
                put_items.join(", "),
                get_borrowed_items.join(", "),
            )
            .unwrap();
        }
    }

    if flex && tagged {
        writeln!(out, "use crate::tagged_fields::{{encode_to_bytes, read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    } else if flex {
        writeln!(out, "use crate::tagged_fields::{{read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    }

    // `Decode` is needed when any tagged field uses owned decode (to call the trait method).
    let needs_owned_decode = has_tagged_fields_needing_owned(spec, res_map);
    if needs_owned_decode {
        writeln!(
            out,
            "use crate::{{Decode, DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields}};"
        )
        .unwrap();
    } else {
        writeln!(
            out,
            "use crate::{{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields}};"
        )
        .unwrap();
    }
}

// ── constants ──────────────────────────────────────────────────────────────

fn emit_constants(out: &mut String, spec: &MessageSpec) {
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

// ── struct definition ──────────────────────────────────────────────────────

fn emit_struct(
    out: &mut String,
    spec: &MessageSpec,
    res_map: &HashMap<String, Resolution>,
    parent_module: &str,
) {
    let type_name = name_conv::type_name(&spec.name);
    let lt = if spec_needs_lifetime(spec, res_map) {
        "<'a>"
    } else {
        ""
    };
    let eq_derive = if has_float64_recursive(&spec.fields) {
        ""
    } else {
        ", Eq"
    };
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq{eq_derive})]
pub struct {type_name}{lt} {{"
    )
    .unwrap();

    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f);
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::borrowed_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    for f in spec.fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        let rust_type = if tagged_field_needs_owned(f, res_map) {
            // Tagged struct/string fields must store owned data because the payload buffer
            // is ephemeral and strings decoded from it cannot escape the read_tagged_fields closure.
            let owned_path = owned_struct_path_for(f, parent_module, res_map);
            type_map::owned_type(&f.field_type, nullable, owned_path.as_deref())
        } else {
            let struct_path = struct_path_for(f, res_map);
            type_map::borrowed_type(&f.field_type, nullable, struct_path.as_deref())
        };
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    writeln!(out, "    pub unknown_tagged_fields: UnknownTaggedFields,").unwrap();
    writeln!(out, "}}").unwrap();

    // Manual Default impl (required because `'a` lifetime makes derive unusable for &str)
    emit_default_impl(out, spec, res_map, parent_module);
}

fn emit_default_impl(
    out: &mut String,
    spec: &MessageSpec,
    res_map: &HashMap<String, Resolution>,
    _parent_module: &str,
) {
    let type_name = name_conv::type_name(&spec.name);
    let lt = if spec_needs_lifetime(spec, res_map) {
        "<'a>"
    } else {
        ""
    };
    writeln!(
        out,
        "
impl{lt} Default for {type_name}{lt} {{
    fn default() -> Self {{
        Self {{"
    )
    .unwrap();

    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let default_expr = borrowed_default_expr(f, res_map);
        writeln!(out, "            {field}: {default_expr},").unwrap();
    }
    for f in spec.fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        // Tagged fields using owned types still default to Vec::new() or None.
        let default_expr = borrowed_default_expr(f, res_map);
        writeln!(out, "            {field}: {default_expr},").unwrap();
    }
    writeln!(
        out,
        "            unknown_tagged_fields: Default::default(),"
    )
    .unwrap();
    writeln!(out, "        }}\n    }}\n}}").unwrap();
}

/// Returns a Rust expression for the default value of a borrowed field.
fn borrowed_default_expr(f: &FieldSpec, _res_map: &HashMap<String, Resolution>) -> String {
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
        _ => "Default::default()".into(),
    }
}

// ── to_owned() impl ────────────────────────────────────────────────────────

fn emit_to_owned_impl(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    let module_name = name_conv::module_name(&spec.name);
    let lt = if spec_needs_lifetime(spec, res_map) {
        "<'a>"
    } else {
        ""
    };
    let impl_lt = if spec_needs_lifetime(spec, res_map) {
        "<'a>"
    } else {
        ""
    };
    writeln!(
        out,
        "
impl{impl_lt} {type_name}{lt} {{
    pub fn to_owned(&self) -> crate::owned::{module_name}::{type_name} {{
        crate::owned::{module_name}::{type_name} {{"
    )
    .unwrap();

    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let expr = to_owned_field_expr(
            &f.field_type,
            &format!("self.{field}"),
            is_nullable(f),
            res_map,
        );
        writeln!(out, "            {field}: {expr},").unwrap();
    }
    for f in spec.fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        let expr = if tagged_field_needs_owned(f, res_map) {
            // Field is already the owned type — just clone it.
            format!("self.{field}.clone()")
        } else {
            to_owned_field_expr(&f.field_type, &format!("self.{field}"), nullable, res_map)
        };
        writeln!(out, "            {field}: {expr},").unwrap();
    }
    writeln!(
        out,
        "            unknown_tagged_fields: self.unknown_tagged_fields.clone(),
        }}
    }}
}}"
    )
    .unwrap();
}

#[allow(clippy::only_used_in_recursion, unused_variables)]
fn to_owned_field_expr(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
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

// ── encode impl ────────────────────────────────────────────────────────────

fn emit_encode_impl(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    let has_flex = has_any_flex(spec);
    let lt = if spec_needs_lifetime(spec, res_map) {
        "<'a>"
    } else {
        ""
    };
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
impl{lt} Encode for {type_name}{lt} {{
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            {version_err}
        }}
        let flex = is_flexible(version);"
    )
    .unwrap();

    encode_struct_body(out, &spec.fields, res_map, "        ", has_flex);

    writeln!(out, "        Ok(())\n    }}").unwrap();

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

/// Returns `true` if this field has `"flexibleVersions": "none"` (per-field override),
/// meaning it must always use the legacy (non-compact) codec even in flex message versions.
fn field_forces_non_flex(f: &FieldSpec) -> bool {
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
    let nullable_min = f.nullable_versions.map(|r| r.min);
    let needs_version_split = nullable_min.is_some_and(|nmin| nmin > f.versions.min);
    if needs_version_split {
        let nmin = nullable_min.unwrap();
        let nullable_body = encode_call(&f.field_type, &format!("self.{field}"), true, res_map);
        let non_nullable_body =
            encode_call_option_as_non_nullable(&f.field_type, &format!("self.{field}"), res_map);
        writeln!(
            out,
            "{indent}if {cond} {{ {flex_prefix}if version >= {nmin} {{ {nullable_body} }} else {{ {non_nullable_body} }}{flex_suffix} }}"
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
    let nullable_min = f.nullable_versions.map(|r| r.min);
    let needs_version_split = nullable_min.is_some_and(|nmin| nmin > f.versions.min);
    if needs_version_split {
        let nmin = nullable_min.unwrap();
        let nullable_body =
            encoded_len_expr(&f.field_type, &format!("self.{field}"), true, res_map);
        let non_nullable_body = encoded_len_expr_option_as_non_nullable(
            &f.field_type,
            &format!("self.{field}"),
            res_map,
        );
        writeln!(
            out,
            "{indent}if {cond} {{ n += {flex_prefix}if version >= {nmin} {{ {nullable_body} }} else {{ {non_nullable_body} }}{flex_suffix}; }}"
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

/// Encode a field whose Rust type is `Option<T>` but wire format is non-nullable.
/// Treats None as empty/default for the underlying type.
fn encode_call_option_as_non_nullable(
    schema_type: &str,
    expr: &str,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
            inner = encode_call(elem, "it", false, res_map),
        );
    }
    match schema_type {
        "string" => format!(
            "if flex {{ put_compact_string(buf, ({expr}).unwrap_or(\"\")) }} \
             else {{ put_string(buf, ({expr}).unwrap_or(\"\")) }}"
        ),
        "uuid" => format!("crate::primitives::uuid::put_uuid(buf, ({expr}).unwrap_or_default())"),
        _ => encode_call(
            schema_type,
            &format!("({expr}).unwrap_or_default()"),
            false,
            res_map,
        ),
    }
}

/// Compute `encoded_len` for a field whose Rust type is `Option<T>` but wire is non-nullable.
fn encoded_len_expr_option_as_non_nullable(
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
                 prefix + body }}"
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
            "if flex {{ compact_string_len(({expr}).unwrap_or(\"\")) }} \
             else {{ string_len(({expr}).unwrap_or(\"\")) }}"
        ),
        "uuid" => "16".into(),
        _ => encoded_len_expr(
            schema_type,
            &format!("({expr}).unwrap_or_default()"),
            false,
            res_map,
        ),
    }
}

/// Returns a Rust boolean expression that is `true` when the tagged field
/// equals its schema-specified default. Mirrors `owned::tagged_is_default_cond`.
fn tagged_is_default_cond(f: &FieldSpec) -> String {
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
fn owned_encoded_len_expr(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
        _ => encoded_len_expr(schema_type, expr, nullable, res_map),
    }
}

/// Like `encode_call` but uses owned-type conventions (`.as_deref()`, `&expr`).
/// Used for tagged fields that are stored as `String`/`Bytes` (owned) in the
/// borrowed struct because their data can't borrow from the input buffer.
fn owned_encode_call(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
    match (schema_type, nullable) {
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
        // For non-string/bytes types, fall back to the borrowed encode call.
        _ => encode_call(schema_type, expr, nullable, res_map),
    }
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
    // Use `b` (the closure buffer param) not `buf` (the outer message buffer).
    // For tagged fields stored as owned types (strings with Option<String>), the encode
    // call must use `.as_deref()` to convert to the expected &str / Option<&str>.
    let base = base_type(&f.field_type);
    let encode_body = if tagged_field_needs_owned(f, res_map) && matches!(base, "string" | "bytes")
    {
        // Use owned-flavor encode which adds .as_deref() for nullable strings/bytes.
        owned_encode_call(&f.field_type, &format!("self.{field}"), nullable, res_map)
    } else {
        encode_call(&f.field_type, &format!("self.{field}"), nullable, res_map)
    }
    .replace("buf", "b");
    let is_default_cond = tagged_is_default_cond(f);
    // Use owned_encoded_len_expr for tagged string/bytes fields to match the .as_deref() convention.
    let len_expr = if tagged_field_needs_owned(f, res_map) && matches!(base, "string" | "bytes") {
        owned_encoded_len_expr(&f.field_type, &format!("self.{field}"), nullable, res_map)
    } else {
        encoded_len_expr(&f.field_type, &format!("self.{field}"), nullable, res_map)
    };
    writeln!(
        out,
        "{indent}    if !({is_default_cond}) {{
{indent}        let payload = encode_to_bytes({len_expr}, |b| {{ {encode_body}; Ok(()) }});
{indent}        tagged.add({tag}, payload);
{indent}    }}"
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
    // For tagged fields stored as owned strings, the len expression must use .as_deref().
    let base = base_type(&f.field_type);
    let len = if tagged_field_needs_owned(f, res_map) && matches!(base, "string" | "bytes") {
        owned_encoded_len_expr(&f.field_type, &format!("self.{field}"), nullable, res_map)
    } else {
        encoded_len_expr(&f.field_type, &format!("self.{field}"), nullable, res_map)
    };
    let is_default_cond = tagged_is_default_cond(f);
    writeln!(
        out,
        "{indent}    if !({is_default_cond}) {{
{indent}        known_pairs.push(({tag}, {len}));
{indent}    }}"
    )
    .unwrap();
}

// ── decode_borrow impl ────────────────────────────────────────────────────

fn emit_decode_borrow_impl(
    out: &mut String,
    spec: &MessageSpec,
    res_map: &HashMap<String, Resolution>,
    parent_module: &str,
) {
    let type_name = name_conv::type_name(&spec.name);
    let has_flex = has_any_flex(spec);
    // Use 'de as the lifetime param if the struct has borrowed data; otherwise no lifetime needed.
    let lt = if spec_needs_lifetime(spec, res_map) {
        "<'de>"
    } else {
        ""
    };
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
impl<'de> DecodeBorrow<'de> for {type_name}{lt} {{
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            {version_err}
        }}
        let flex = is_flexible(version);
        let mut out = Self::default();"
    )
    .unwrap();

    decode_borrow_struct_body(
        out,
        &spec.fields,
        res_map,
        "        ",
        has_flex,
        parent_module,
    );

    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();
}

fn decode_borrow_struct_body(
    out: &mut String,
    fields: &[FieldSpec],
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    has_flex: bool,
    parent_module: &str,
) {
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        emit_decode_borrow_one(out, f, res_map, indent);
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
                emit_decode_borrow_tagged_arm(out, f, res_map, indent, parent_module);
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

fn emit_decode_borrow_one(
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
    let nullable_min = f.nullable_versions.map(|r| r.min);
    let needs_version_split = nullable_min.is_some_and(|nmin| nmin > f.versions.min);
    if needs_version_split {
        let nmin = nullable_min.unwrap();
        let nullable_call = decode_borrow_call(&f.field_type, true, res_map);
        let non_nullable_call = decode_borrow_call(&f.field_type, false, res_map);
        writeln!(
            out,
            "{indent}if {cond} {{ out.{field} = {flex_prefix}if version >= {nmin} {{ {nullable_call} }} else {{ Some({non_nullable_call}) }}{flex_suffix}; }}"
        ).unwrap();
    } else {
        let call = decode_borrow_call(&f.field_type, is_nullable(f), res_map);
        writeln!(
            out,
            "{indent}if {cond} {{ out.{field} = {flex_prefix}{call}{flex_suffix}; }}"
        )
        .unwrap();
    }
}

fn emit_decode_borrow_tagged_arm(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    parent_module: &str,
) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));

    if tagged_field_needs_owned(f, res_map) {
        // The field's type is stored as owned to avoid borrow-escape from payload.
        // Use the owned Decode impl (which takes B: Buf, not &mut &'de [u8]).
        let call =
            decode_owned_call(&f.field_type, nullable, parent_module, res_map).replace("buf", "b");
        writeln!(out, "{indent}        {tag} => {{ tag_{field} = Some({{ let b: &mut &[u8] = payload; {call} }}); Ok(true) }}").unwrap();
    } else {
        // Primitive or lifetime-free struct: borrow-decode is fine.
        let call = decode_borrow_call(&f.field_type, nullable, res_map).replace("buf", "b");
        writeln!(out, "{indent}        {tag} => {{ tag_{field} = Some({{ let b: &mut &[u8] = payload; {call} }}); Ok(true) }}").unwrap();
    }
}

// ── nested struct emitter ──────────────────────────────────────────────────

fn emit_nested_structs_for_fields(
    out: &mut String,
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &HashMap<String, Resolution>,
    parent_module: &str,
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
                parent_module,
            );
        }
    }
}

#[allow(clippy::too_many_lines)]
fn emit_nested_struct(
    out: &mut String,
    struct_name: &str,
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &HashMap<String, Resolution>,
    parent_module: &str,
) {
    // Only add <'a> lifetime if the struct actually has string/bytes/borrowed-struct fields.
    let has_lifetime = needs_lifetime(fields, res_map);
    let lt = if has_lifetime { "<'a>" } else { "" };
    let lt_de = if has_lifetime { "<'de>" } else { "" };
    let eq_derive = if has_float64_recursive(fields) {
        ""
    } else {
        ", Eq"
    };

    // Struct definition
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq{eq_derive})]
pub struct {struct_name}{lt} {{"
    )
    .unwrap();

    for f in fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f);
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::borrowed_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    for f in fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::borrowed_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    writeln!(out, "    pub unknown_tagged_fields: UnknownTaggedFields,").unwrap();
    writeln!(out, "}}").unwrap();

    // Manual Default impl (only needed when there's a lifetime parameter)
    writeln!(
        out,
        "
impl{lt} Default for {struct_name}{lt} {{
    fn default() -> Self {{
        Self {{"
    )
    .unwrap();
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let default_expr = borrowed_default_expr(f, res_map);
        writeln!(out, "            {field}: {default_expr},").unwrap();
    }
    for f in fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let default_expr = borrowed_default_expr(f, res_map);
        writeln!(out, "            {field}: {default_expr},").unwrap();
    }
    writeln!(
        out,
        "            unknown_tagged_fields: Default::default(),"
    )
    .unwrap();
    writeln!(out, "        }}\n    }}\n}}").unwrap();

    // to_owned() on nested struct — the owned type lives in the parent message's owned module.
    let module_path = format!("crate::owned::{parent_module}");
    writeln!(
        out,
        "
impl{lt} {struct_name}{lt} {{
    pub fn to_owned(&self) -> {module_path}::{struct_name} {{
        {module_path}::{struct_name} {{"
    )
    .unwrap();
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let expr = to_owned_field_expr(
            &f.field_type,
            &format!("self.{field}"),
            is_nullable(f),
            res_map,
        );
        writeln!(out, "            {field}: {expr},").unwrap();
    }
    for f in fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        let expr = to_owned_field_expr(&f.field_type, &format!("self.{field}"), nullable, res_map);
        writeln!(out, "            {field}: {expr},").unwrap();
    }
    writeln!(
        out,
        "            unknown_tagged_fields: self.unknown_tagged_fields.clone(),
        }}
    }}
}}"
    )
    .unwrap();

    let has_flex = flex_min_val < i16::MAX;

    // Encode impl — no version-range guard; version flows in from parent
    writeln!(
        out,
        "
impl{lt} Encode for {struct_name}{lt} {{
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

    // DecodeBorrow impl
    writeln!(
        out,
        "
impl<'de> DecodeBorrow<'de> for {struct_name}{lt_de} {{
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {{
        let flex = version >= {flex_min_val};
        let mut out = Self::default();"
    )
    .unwrap();

    decode_borrow_struct_body(out, fields, res_map, "        ", has_flex, parent_module);

    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();

    // Recurse into deeper nesting
    emit_nested_structs_for_fields(out, fields, flex_min_val, res_map, parent_module);
}

// ── primitive encode/decode call generators ────────────────────────────────

#[allow(clippy::only_used_in_recursion)]
fn encode_call(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
            "if flex {{ put_compact_string(buf, {expr}) }} else {{ put_string(buf, {expr}) }}"
        ),
        ("string", true) => format!(
            "if flex {{ put_compact_nullable_string(buf, {expr}) }} else {{ put_nullable_string(buf, {expr}) }}"
        ),
        ("bytes", false) => format!(
            "if flex {{ put_compact_bytes(buf, {expr}) }} else {{ put_bytes(buf, {expr}) }}"
        ),
        ("bytes", true) => format!(
            "if flex {{ put_compact_nullable_bytes(buf, {expr}) }} else {{ put_nullable_bytes(buf, {expr}) }}"
        ),
        ("records", false) => format!(
            "{{ \
                let mut __rb_buf = bytes::BytesMut::new(); \
                <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(&{expr}, &mut __rb_buf, version)?; \
                if flex {{ put_compact_bytes(buf, &__rb_buf) }} else {{ put_bytes(buf, &__rb_buf) }} \
            }}"
        ),
        ("records", true) => format!(
            "match &{expr} {{ \
                None => if flex {{ put_compact_nullable_bytes(buf, None) }} else {{ put_nullable_bytes(buf, None) }}, \
                Some(__rb) => {{ \
                    let mut __rb_buf = bytes::BytesMut::new(); \
                    <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(__rb, &mut __rb_buf, version)?; \
                    if flex {{ put_compact_bytes(buf, &__rb_buf) }} else {{ put_bytes(buf, &__rb_buf) }} \
                }} \
            }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encode_call (borrowed): {t}\")"),
    }
}

#[allow(clippy::only_used_in_recursion)]
fn encoded_len_expr(
    schema_type: &str,
    expr: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
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
            let inner = encoded_len_expr(elem, "*it", false, res_map);
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

#[allow(clippy::only_used_in_recursion)]
fn decode_borrow_call(
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
#[allow(clippy::only_used_in_recursion)]
fn decode_owned_call(
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
