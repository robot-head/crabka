//! Emit Rust source for the borrowed flavor of a `MessageSpec`.
//!
//! Mirrors the structure of `emit/owned.rs`. Strings become `&'a str`,
//! bytes become `&'a [u8]`, the struct carries a `'a` lifetime,
//! `DecodeBorrow<'de>` replaces `Decode<'de>`, and `to_owned()` bridges to
//! the matching owned type.

use std::collections::HashMap;
use std::fmt::Write;

use crate::emit::common::banner;
use crate::emit::owned::EmitError;
use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, VersionRange};
use crate::name_conv;
use crate::resolve::{self, Resolution, StructKind};
use crate::type_map;

pub fn emit(spec: &MessageSpec, schemas_version: &str) -> Result<String, EmitError> {
    if !spec.common_structs.is_empty() {
        return Err(EmitError::Unsupported(format!(
            "{}: commonStructs not yet supported by borrowed emitter",
            spec.name
        )));
    }

    // Build resolution map — reject any common-struct references.
    let res_map = resolve::resolve_message(spec)?;
    for (name, res) in &res_map {
        if res.kind == StructKind::Common {
            return Err(EmitError::Unsupported(format!(
                "{}: common struct `{name}` not yet supported by borrowed emitter",
                spec.name
            )));
        }
    }

    let mut out = banner(schemas_version);
    emit_imports(&mut out, spec);
    emit_constants(&mut out, spec);
    emit_struct(&mut out, spec, &res_map);
    emit_to_owned_impl(&mut out, spec, &res_map);
    emit_encode_impl(&mut out, spec, &res_map);
    emit_decode_borrow_impl(&mut out, spec, &res_map);

    let fm = flex_min(spec);
    emit_nested_structs_for_fields(&mut out, &spec.fields, fm, &res_map);

    Ok(out)
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

fn is_tagged(f: &FieldSpec) -> bool {
    f.tag.is_some()
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

fn uses_fixed_primitives(types: &[String]) -> bool {
    types.iter().any(|t| {
        matches!(
            t.as_str(),
            "int8" | "int16" | "int32" | "int64" | "bool" | "float64"
        )
    })
}

fn uses_string(types: &[String]) -> bool {
    types.iter().any(|t| t == "string")
}

fn uses_bytes(types: &[String]) -> bool {
    types.iter().any(|t| matches!(t.as_str(), "bytes" | "records"))
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
        let here = matches!(base, "bytes" | "records") && f.nullable_versions.is_some();
        here || uses_nullable_bytes_recursive(&f.fields)
    })
}

fn struct_path_for(f: &FieldSpec, res_map: &HashMap<String, Resolution>) -> Option<String> {
    let base = base_type(&f.field_type);
    if is_struct_type(base) {
        res_map.get(base).map(|r| r.rust_path.clone())
    } else {
        None
    }
}

fn version_cond(r: VersionRange, version_var: &str) -> String {
    if r.max == i16::MAX {
        format!("{version_var} >= {}", r.min)
    } else {
        format!("({version_var} >= {} && {version_var} <= {})", r.min, r.max)
    }
}

// ── imports ────────────────────────────────────────────────────────────────

fn emit_imports(out: &mut String, spec: &MessageSpec) {
    let types = used_field_types_recursive(&spec.fields);
    let tagged = has_any_tagged_in_spec(spec);
    let flex = has_any_flex(spec);
    let use_fixed = uses_fixed_primitives(&types);
    let use_string = uses_string(&types);
    let use_bytes = uses_bytes(&types);
    let use_nullable_string = uses_nullable_string_recursive(&spec.fields);
    let use_nullable_bytes = uses_nullable_bytes_recursive(&spec.fields);

    // `Bytes` is needed for to_owned() on bytes/records fields
    if use_bytes {
        writeln!(out, "\nuse bytes::{{Bytes, BufMut}};").unwrap();
    } else {
        writeln!(out, "\nuse bytes::BufMut;").unwrap();
    }

    if use_fixed {
        writeln!(out, "\nuse crate::primitives::fixed::{{get_bool, get_f64, get_i16, get_i32, get_i64, get_i8, put_bool, put_f64, put_i16, put_i32, put_i64, put_i8}};").unwrap();
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
        if use_nullable_bytes {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{
    put_bytes, put_compact_bytes, put_compact_nullable_bytes, put_nullable_bytes,
}};
use crate::primitives::string_bytes_borrowed::{{
    get_bytes_borrowed, get_compact_bytes_borrowed,
    get_compact_nullable_bytes_borrowed, get_nullable_bytes_borrowed,
}};"
            )
            .unwrap();
        } else {
            writeln!(
                out,
                "use crate::primitives::string_bytes::{{put_bytes, put_compact_bytes}};
use crate::primitives::string_bytes_borrowed::{{get_bytes_borrowed, get_compact_bytes_borrowed}};"
            )
            .unwrap();
        }
    }

    if flex && tagged {
        writeln!(out, "use crate::tagged_fields::{{encode_to_bytes, read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    } else if flex {
        writeln!(out, "use crate::tagged_fields::{{read_tagged_fields, tagged_fields_len, WriteTaggedFields}};").unwrap();
    }

    writeln!(
        out,
        "use crate::{{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields}};"
    )
    .unwrap();
}

// ── constants ──────────────────────────────────────────────────────────────

fn emit_constants(out: &mut String, spec: &MessageSpec) {
    let api_key = spec.api_key.unwrap_or(0);
    let min_version = spec.valid_versions.min;
    let max_version = spec.valid_versions.max;
    let flex = flex_min(spec);
    writeln!(
        out,
        "
pub const API_KEY: i16 = {api_key};
pub const MIN_VERSION: i16 = {min_version};
pub const MAX_VERSION: i16 = {max_version};
pub const FLEXIBLE_MIN: i16 = {flex};

#[inline]
fn is_flexible(version: i16) -> bool {{ version >= FLEXIBLE_MIN }}"
    )
    .unwrap();
}

// ── struct definition ──────────────────────────────────────────────────────

fn emit_struct(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct {type_name}<'a> {{"
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
        let struct_path = struct_path_for(f, res_map);
        let rust_type = type_map::borrowed_type(&f.field_type, nullable, struct_path.as_deref());
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    writeln!(out, "    pub unknown_tagged_fields: UnknownTaggedFields,").unwrap();
    writeln!(out, "}}").unwrap();

    // Manual Default impl (required because `'a` lifetime makes derive unusable for &str)
    emit_default_impl(out, spec, res_map);
}

fn emit_default_impl(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    writeln!(
        out,
        "
impl<'a> Default for {type_name}<'a> {{
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
        let default_expr = borrowed_default_expr(f, res_map);
        writeln!(out, "            {field}: {default_expr},").unwrap();
    }
    writeln!(out, "            unknown_tagged_fields: Default::default(),").unwrap();
    writeln!(out, "        }}\n    }}\n}}").unwrap();
}

/// Returns a Rust expression for the default value of a borrowed field.
fn borrowed_default_expr(f: &FieldSpec, _res_map: &HashMap<String, Resolution>) -> String {
    let base = base_type(&f.field_type);
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    if nullable {
        // Check if the schema-level default is null (or there's no non-null default)
        match &f.default {
            Some(serde_json::Value::Null) | None => return "None".into(),
            Some(v) => {
                // Non-null default with nullable type: use the value
                return format!("Some({})", scalar_borrowed_default(base, v));
            }
        }
    }
    // Non-nullable
    match &f.default {
        Some(v) => scalar_borrowed_default(base, v),
        None => borrowed_zero(base),
    }
}

fn scalar_borrowed_default(base_type: &str, val: &serde_json::Value) -> String {
    match (base_type, val) {
        ("string", serde_json::Value::String(s)) if s.is_empty() => "\"\"".into(),
        ("string", serde_json::Value::String(s)) => format!("\"{s}\""),
        ("bool", serde_json::Value::Bool(b)) => b.to_string(),
        ("int8" | "int16" | "int32" | "int64", serde_json::Value::Number(n)) => {
            n.to_string()
        }
        _ => borrowed_zero(base_type),
    }
}

fn borrowed_zero(base: &str) -> String {
    match base {
        "string" => "\"\"".into(),
        "bytes" | "records" => "&[]".into(),
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
    writeln!(
        out,
        "
impl<'a> {type_name}<'a> {{
    pub fn to_owned(&self) -> crate::owned::{module_name}::{type_name} {{
        crate::owned::{module_name}::{type_name} {{"
    )
    .unwrap();

    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let expr = to_owned_field_expr(&f.field_type, &format!("self.{field}"), is_nullable(f), res_map);
        writeln!(out, "            {field}: {expr},").unwrap();
    }
    for f in spec.fields.iter().filter(|f| is_tagged(f)) {
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
            "bytes" | "records" if nullable => {
                return format!(
                    "({expr}).as_ref().map(|v| v.iter().map(|b| Bytes::copy_from_slice(b)).collect())"
                );
            }
            "bytes" | "records" => {
                return format!("({expr}).iter().map(|b| Bytes::copy_from_slice(b)).collect()");
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
        ("bytes" | "records", false) => format!("Bytes::copy_from_slice({expr})"),
        ("bytes" | "records", true) => format!("({expr}).map(Bytes::copy_from_slice)"),
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
    writeln!(
        out,
        "
impl<'a> Encode for {type_name}<'a> {{
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            return Err(ProtocolError::UnsupportedVersion {{ api_key: API_KEY, version }});
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

fn emit_encode_one(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    let body = encode_call(&f.field_type, &format!("self.{field}"), is_nullable(f), res_map);
    writeln!(out, "{indent}if {cond} {{ {body} }}").unwrap();
}

fn emit_encoded_len_one(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    let body = encoded_len_expr(&f.field_type, &format!("self.{field}"), is_nullable(f), res_map);
    writeln!(out, "{indent}if {cond} {{ n += {body}; }}").unwrap();
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
    writeln!(
        out,
        "{indent}    if !crate::codegen_helpers::is_default(&self.{field}) {{
{indent}        let payload = encode_to_bytes({len_expr}, |b| {{ {encode}; }});
{indent}        tagged.add({tag}, payload);
{indent}    }}",
        len_expr = encoded_len_expr(&f.field_type, &format!("self.{field}"), nullable, res_map),
        encode = encode_call(&f.field_type, &format!("self.{field}"), nullable, res_map),
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
    writeln!(
        out,
        "{indent}    if !crate::codegen_helpers::is_default(&self.{field}) {{
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
) {
    let type_name = name_conv::type_name(&spec.name);
    let has_flex = has_any_flex(spec);
    writeln!(
        out,
        "
impl<'de> DecodeBorrow<'de> for {type_name}<'de> {{
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            return Err(ProtocolError::UnsupportedVersion {{ api_key: API_KEY, version }});
        }}
        let flex = is_flexible(version);
        let mut out = Self::default();"
    )
    .unwrap();

    decode_borrow_struct_body(out, &spec.fields, res_map, "        ", has_flex);

    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();
}

fn decode_borrow_struct_body(
    out: &mut String,
    fields: &[FieldSpec],
    res_map: &HashMap<String, Resolution>,
    indent: &str,
    has_flex: bool,
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
                emit_decode_borrow_tagged_arm(out, f, res_map, indent);
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
    let call = decode_borrow_call(&f.field_type, is_nullable(f), res_map);
    writeln!(out, "{indent}if {cond} {{ out.{field} = {call}; }}").unwrap();
}

fn emit_decode_borrow_tagged_arm(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    let call = decode_borrow_call(&f.field_type, nullable, res_map);
    writeln!(out, "{indent}        {tag} => {{ tag_{field} = Some({{ let b: &mut &[u8] = payload; {call} }}); Ok(true) }}").unwrap();
}

// ── nested struct emitter ──────────────────────────────────────────────────

fn emit_nested_structs_for_fields(
    out: &mut String,
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &HashMap<String, Resolution>,
) {
    for f in fields {
        if !f.fields.is_empty() {
            let struct_name = base_type(&f.field_type);
            emit_nested_struct(out, struct_name, &f.fields, flex_min_val, res_map);
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
) {
    // Struct definition with <'a> lifetime
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct {struct_name}<'a> {{"
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

    // Manual Default impl
    writeln!(
        out,
        "
impl<'a> Default for {struct_name}<'a> {{
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
    writeln!(out, "            unknown_tagged_fields: Default::default(),").unwrap();
    writeln!(out, "        }}\n    }}\n}}").unwrap();

    // to_owned() on nested struct
    let module_path = format!("super::owned::{}", name_conv::module_name(struct_name));
    writeln!(
        out,
        "
impl<'a> {struct_name}<'a> {{
    pub fn to_owned(&self) -> {module_path}::{struct_name} {{
        {module_path}::{struct_name} {{"
    )
    .unwrap();
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let expr = to_owned_field_expr(&f.field_type, &format!("self.{field}"), is_nullable(f), res_map);
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
impl<'a> Encode for {struct_name}<'a> {{
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
impl<'de> DecodeBorrow<'de> for {struct_name}<'de> {{
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {{
        let flex = version >= {flex_min_val};
        let mut out = Self::default();"
    )
    .unwrap();

    decode_borrow_struct_body(out, fields, res_map, "        ", has_flex);

    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();

    // Recurse into deeper nesting
    emit_nested_structs_for_fields(out, fields, flex_min_val, res_map);
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
                inner = encode_call(elem, "it", false, res_map),
            );
        }
        return format!(
            "{{ crate::primitives::array::put_array_len(buf, ({expr}).len(), flex); \
             for it in &{expr} {{ {inner}; }} }}",
            inner = encode_call(elem, "it", false, res_map),
        );
    }

    if is_struct_type(schema_type) {
        if nullable {
            return format!("if let Some(v) = &{expr} {{ v.encode(buf, version)?; }}");
        }
        return format!("{expr}.encode(buf, version)?");
    }

    // Borrowed strings/bytes: `expr` is `&str` or `&[u8]` — no extra `&` needed.
    match (schema_type, nullable) {
        ("int8", _) => format!("put_i8(buf, {expr})"),
        ("int16", _) => format!("put_i16(buf, {expr})"),
        ("int32", _) => format!("put_i32(buf, {expr})"),
        ("int64", _) => format!("put_i64(buf, {expr})"),
        ("bool", _) => format!("put_bool(buf, {expr})"),
        ("float64", _) => format!("put_f64(buf, {expr})"),
        ("string", false) => format!(
            "if flex {{ put_compact_string(buf, {expr}) }} else {{ put_string(buf, {expr}) }}"
        ),
        ("string", true) => format!(
            "if flex {{ put_compact_nullable_string(buf, {expr}) }} else {{ put_nullable_string(buf, {expr}) }}"
        ),
        ("bytes" | "records", false) => format!(
            "if flex {{ put_compact_bytes(buf, {expr}) }} else {{ put_bytes(buf, {expr}) }}"
        ),
        ("bytes" | "records", true) => format!(
            "if flex {{ put_compact_nullable_bytes(buf, {expr}) }} else {{ put_nullable_bytes(buf, {expr}) }}"
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
        if nullable {
            return format!(
                "{{ let opt: Option<&Vec<_>> = ({expr}).as_ref(); \
                 let prefix = crate::primitives::array::nullable_array_len_prefix_len(opt.map(|v| v.len()), flex); \
                 let body: usize = opt.map_or(0, |v| v.iter().map(|it| {inner}).sum()); \
                 prefix + body }}",
                inner = encoded_len_expr(elem, "*it", false, res_map),
            );
        }
        return format!(
            "{{ let prefix = crate::primitives::array::array_len_prefix_len(({expr}).len(), flex); \
             let body: usize = ({expr}).iter().map(|it| {inner}).sum(); \
             prefix + body }}",
            inner = encoded_len_expr(elem, "*it", false, res_map),
        );
    }

    if is_struct_type(schema_type) {
        if nullable {
            return format!("{expr}.as_ref().map_or(0, |v| v.encoded_len(version))");
        }
        return format!("{expr}.encoded_len(version)");
    }

    // Borrowed strings/bytes: `expr` is `&str` or `&[u8]`.
    match (schema_type, nullable) {
        ("int8" | "bool", _) => "1".into(),
        ("int16", _) => "2".into(),
        ("int32", _) => "4".into(),
        ("int64" | "float64", _) => "8".into(),
        ("string", false) => {
            format!("if flex {{ compact_string_len({expr}) }} else {{ string_len({expr}) }}")
        }
        ("string", true) => format!(
            "if flex {{ compact_nullable_string_len({expr}) }} else {{ nullable_string_len({expr}) }}"
        ),
        ("bytes" | "records", false) => format!(
            "if flex {{ crate::primitives::varint::uvarint_len(u32::try_from(({expr}).len() + 1).unwrap()) + ({expr}).len() }} \
             else {{ 4 + ({expr}).len() }}"
        ),
        ("bytes" | "records", true) => format!(
            "match {expr} {{ \
             None => if flex {{ 1 }} else {{ 4 }}, \
             Some(b) => if flex {{ crate::primitives::varint::uvarint_len(u32::try_from(b.len() + 1).unwrap()) + b.len() }} \
             else {{ 4 + b.len() }} }}"
        ),
        (t, _) => format!(
            "compile_error!(\"unhandled type in encoded_len_expr (borrowed): {t}\")"
        ),
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
            if nullable {
                return format!(
                    "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                     match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                     for _ in 0..n {{ v.push({elem_base}::decode_borrow(buf, version)?); }} Some(v) }} }} }}",
                );
            }
            return format!(
                "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
                 let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({elem_base}::decode_borrow(buf, version)?); }} v }}",
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
        if nullable {
            return format!("Some({schema_type}::decode_borrow(buf, version)?)");
        }
        return format!("{schema_type}::decode_borrow(buf, version)?");
    }

    match (schema_type, nullable) {
        ("int8",    _) => "get_i8(buf)?".into(),
        ("int16",   _) => "get_i16(buf)?".into(),
        ("int32",   _) => "get_i32(buf)?".into(),
        ("int64",   _) => "get_i64(buf)?".into(),
        ("bool",    _) => "get_bool(buf)?".into(),
        ("float64", _) => "get_f64(buf)?".into(),
        ("string", false) => {
            "if flex { get_compact_string_borrowed(buf)? } else { get_string_borrowed(buf)? }".into()
        }
        ("string", true) => {
            "if flex { get_compact_nullable_string_borrowed(buf)? } else { get_nullable_string_borrowed(buf)? }".into()
        }
        ("bytes" | "records", false) => {
            "if flex { get_compact_bytes_borrowed(buf)? } else { get_bytes_borrowed(buf)? }".into()
        }
        ("bytes" | "records", true) => {
            "if flex { get_compact_nullable_bytes_borrowed(buf)? } else { get_nullable_bytes_borrowed(buf)? }".into()
        }
        (t, _) => format!("compile_error!(\"unhandled type in decode_borrow_call: {t}\")"),
    }
}
