//! Emit Rust source for the owned flavor of a `MessageSpec`.
//!
//! Handles primitive fields, tagged fields, primitive arrays, and nested
//! struct fields. Nested anonymous structs become sibling types in the same
//! generated file. `commonStructs` support is added in Task 14.

use std::fmt::Write;
use std::collections::HashMap;

use crate::emit::common::banner;
use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, VersionRange};
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

pub fn emit(spec: &MessageSpec, schemas_version: &str) -> Result<String, EmitError> {
    if !spec.common_structs.is_empty() {
        return Err(EmitError::Unsupported(format!(
            "{}: commonStructs not yet supported by owned emitter",
            spec.name
        )));
    }

    // Build resolution map — reject any common-struct references.
    let res_map = resolve::resolve_message(spec)?;
    for (name, res) in &res_map {
        if res.kind == StructKind::Common {
            return Err(EmitError::Unsupported(format!(
                "{}: common struct `{name}` not yet supported by owned emitter",
                spec.name
            )));
        }
    }

    let mut out = banner(schemas_version);
    emit_imports(&mut out, spec);
    emit_constants(&mut out, spec);
    emit_struct(&mut out, spec, &res_map);
    emit_encode_impl(&mut out, spec, &res_map);
    emit_decode_impl(&mut out, spec, &res_map);

    // Emit sibling types for nested structs (depth-first, post-order so parent
    // types appear before their children's children — order doesn't matter for
    // Rust, but reading top-down is nicer).
    let fm = flex_min(spec);
    emit_nested_structs_for_fields(&mut out, &spec.fields, fm, &res_map);

    Ok(out)
}

/// Walk fields and emit a nested struct for each field that has its own
/// `fields:` list. Recurses depth-first.
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

fn flex_min(spec: &MessageSpec) -> i16 {
    match spec.flexible_versions {
        FlexibleVersions::Range(r) => r.min,
        FlexibleVersions::None => i16::MAX,
    }
}

fn base_type(t: &str) -> &str {
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
    fields.iter().any(|f| {
        f.tag.is_some() || has_tagged_fields_recursive(&f.fields)
    })
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

/// Returns true if any field (recursively) has a string type that is also nullable.
fn uses_nullable_string_recursive(fields: &[FieldSpec]) -> bool {
    fields.iter().any(|f| {
        let base = base_type(&f.field_type);
        let here = base == "string" && (f.nullable_versions.is_some() || f.tag.is_some());
        here || uses_nullable_string_recursive(&f.fields)
    })
}

fn has_any_flex(spec: &MessageSpec) -> bool {
    matches!(spec.flexible_versions, FlexibleVersions::Range(_))
}

fn has_any_tagged_in_spec(spec: &MessageSpec) -> bool {
    has_tagged_fields_recursive(&spec.fields)
}

fn emit_imports(out: &mut String, spec: &MessageSpec) {
    let types = used_field_types_recursive(&spec.fields);
    let tagged = has_any_tagged_in_spec(spec);
    let flex = has_any_flex(spec);
    let use_fixed = uses_fixed_primitives(&types);
    let use_string = uses_string(&types);
    let use_nullable_string = uses_nullable_string_recursive(&spec.fields);

    writeln!(out, "\nuse bytes::{{Buf, BufMut}};").unwrap();

    if use_fixed {
        writeln!(out, "\nuse crate::primitives::fixed::{{get_bool, get_f64, get_i16, get_i32, get_i64, get_i8, put_bool, put_f64, put_i16, put_i32, put_i64, put_i8}};").unwrap();
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

fn emit_struct(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
}

/// Returns the resolved Rust path for a struct-typed field, or `None` for primitives.
fn struct_path_for(f: &FieldSpec, res_map: &HashMap<String, Resolution>) -> Option<String> {
    let base = base_type(&f.field_type);
    if is_struct_type(base) {
        res_map.get(base).map(|r| r.rust_path.clone())
    } else {
        None
    }
}

fn is_struct_type(t: &str) -> bool {
    t.chars().next().is_some_and(char::is_uppercase)
}

fn emit_encode_impl(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    let has_flex = has_any_flex(spec);
    writeln!(
        out,
        "
impl Encode for {type_name} {{
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            return Err(ProtocolError::UnsupportedVersion {{ api_key: API_KEY, version }});
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

fn emit_decode_impl(out: &mut String, spec: &MessageSpec, res_map: &HashMap<String, Resolution>) {
    let type_name = name_conv::type_name(&spec.name);
    let has_flex = has_any_flex(spec);
    writeln!(
        out,
        "
impl<'de> Decode<'de> for {type_name} {{
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            return Err(ProtocolError::UnsupportedVersion {{ api_key: API_KEY, version }});
        }}
        let flex = is_flexible(version);
        let mut out = Self::default();"
    )
    .unwrap();

    decode_struct_body(out, &spec.fields, res_map, "        ", has_flex);

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
) {
    for f in fields.iter().filter(|f| !is_tagged(f)) {
        emit_decode_one(out, f, res_map, indent);
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
                emit_decode_tagged_arm(out, f, res_map, indent);
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
) {
    // Struct definition
    writeln!(
        out,
        "
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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

    decode_struct_body(out, fields, res_map, "        ", has_flex);

    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();

    // Recurse into deeper nesting
    emit_nested_structs_for_fields(out, fields, flex_min_val, res_map);
}

// --- single-field encode/decode helpers -----------------------------------

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

fn emit_decode_one(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    let call = decode_call(&f.field_type, is_nullable(f), res_map);
    writeln!(out, "{indent}if {cond} {{ out.{field} = {call}; }}").unwrap();
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

fn emit_decode_tagged_arm(
    out: &mut String,
    f: &FieldSpec,
    res_map: &HashMap<String, Resolution>,
    indent: &str,
) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    let call = decode_call(&f.field_type, nullable, res_map);
    writeln!(out, "{indent}        {tag} => {{ tag_{field} = Some({{ let b: &mut &[u8] = payload; {call} }}); Ok(true) }}").unwrap();
}

// --- primitive encode/decode call generators ------------------------------

// `res_map` is threaded through for array-element recursion even though the
// primitives branch doesn't use it directly.
#[allow(clippy::only_used_in_recursion)]
fn encode_call(
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
        // Non-array struct
        if nullable {
            return format!(
                "if let Some(v) = &{expr} {{ v.encode(buf, version)?; }}"
            );
        }
        return format!("{expr}.encode(buf, version)?");
    }

    match (schema_type, nullable) {
        ("int8", _) => format!("put_i8(buf, {expr})"),
        ("int16", _) => format!("put_i16(buf, {expr})"),
        ("int32", _) => format!("put_i32(buf, {expr})"),
        ("int64", _) => format!("put_i64(buf, {expr})"),
        ("bool", _) => format!("put_bool(buf, {expr})"),
        ("float64", _) => format!("put_f64(buf, {expr})"),
        ("string", false) => format!(
            "if flex {{ put_compact_string(buf, &{expr}) }} else {{ put_string(buf, &{expr}) }}"
        ),
        ("string", true) => format!(
            "if flex {{ put_compact_nullable_string(buf, {expr}.as_deref()) }} else {{ put_nullable_string(buf, {expr}.as_deref()) }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encode_call: {t}\")"),
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
            return format!(
                "{expr}.as_ref().map_or(0, |v| v.encoded_len(version))"
            );
        }
        return format!("{expr}.encoded_len(version)");
    }

    match (schema_type, nullable) {
        ("int8" | "bool", _) => "1".into(),
        ("int16", _) => "2".into(),
        ("int32", _) => "4".into(),
        ("int64" | "float64", _) => "8".into(),
        ("string", false) => {
            format!("if flex {{ compact_string_len(&{expr}) }} else {{ string_len(&{expr}) }}")
        }
        ("string", true) => format!(
            "if flex {{ compact_nullable_string_len({expr}.as_deref()) }} else {{ nullable_string_len({expr}.as_deref()) }}"
        ),
        (t, _) => format!("compile_error!(\"unhandled type in encoded_len_expr: {t}\")"),
    }
}

#[allow(clippy::only_used_in_recursion)]
fn decode_call(
    schema_type: &str,
    nullable: bool,
    res_map: &HashMap<String, Resolution>,
) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = base_type(elem);
        if is_struct_type(elem_base) {
            // Array of structs
            if nullable {
                return format!(
                    "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                     match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                     for _ in 0..n {{ v.push({elem_base}::decode(buf, version)?); }} Some(v) }} }} }}",
                );
            }
            return format!(
                "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
                 let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({elem_base}::decode(buf, version)?); }} v }}",
            );
        }
        if nullable {
            return format!(
                "{{ let opt = crate::primitives::array::get_nullable_array_len(buf, flex)?; \
                 match opt {{ None => None, Some(n) => {{ let mut v = Vec::with_capacity(n); \
                 for _ in 0..n {{ v.push({inner}); }} Some(v) }} }} }}",
                inner = decode_call(elem, false, res_map),
            );
        }
        return format!(
            "{{ let n = crate::primitives::array::get_array_len(buf, flex)?; \
             let mut v = Vec::with_capacity(n); for _ in 0..n {{ v.push({inner}); }} v }}",
            inner = decode_call(elem, false, res_map),
        );
    }

    if is_struct_type(schema_type) {
        if nullable {
            // For nullable non-array structs: a simple decode — schemas rarely use this
            return format!("Some({schema_type}::decode(buf, version)?)");
        }
        return format!("{schema_type}::decode(buf, version)?");
    }

    match (schema_type, nullable) {
        ("int8",   _)     => "get_i8(buf)?".into(),
        ("int16",  _)     => "get_i16(buf)?".into(),
        ("int32",  _)     => "get_i32(buf)?".into(),
        ("int64",  _)     => "get_i64(buf)?".into(),
        ("bool",   _)     => "get_bool(buf)?".into(),
        ("float64",_)     => "get_f64(buf)?".into(),
        ("string", false) => "if flex { get_compact_string_owned(buf)? } else { get_string_owned(buf)? }".into(),
        ("string", true)  => "if flex { get_compact_nullable_string_owned(buf)? } else { get_nullable_string_owned(buf)? }".into(),
        (t, _) => format!("compile_error!(\"unhandled type in decode_call: {t}\")"),
    }
}

// --- helpers --------------------------------------------------------------

fn is_tagged(f: &FieldSpec) -> bool {
    f.tag.is_some()
}
fn is_nullable(f: &FieldSpec) -> bool {
    f.nullable_versions.is_some()
}

fn version_cond(r: VersionRange, version_var: &str) -> String {
    if r.max == i16::MAX {
        format!("{version_var} >= {}", r.min)
    } else {
        format!("({version_var} >= {} && {version_var} <= {})", r.min, r.max)
    }
}

