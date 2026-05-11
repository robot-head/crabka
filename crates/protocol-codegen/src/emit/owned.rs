//! Emit Rust source for the owned flavor of a `MessageSpec`.
//!
//! Today this handles primitive-only message bodies (Request/Response with
//! no arrays and no nested struct fields). Tagged fields are supported and
//! decoded into typed `Option<T>` fields per the schema's `default`.
//! Array, nested struct, and `commonStructs` support is added in later
//! tasks of this plan.

use std::fmt::Write;

use crate::emit::common::banner;
use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, VersionRange};
use crate::name_conv;
use crate::type_map;

#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("unsupported (in 1a): {0}")]
    Unsupported(String),
}

pub fn emit(spec: &MessageSpec, schemas_version: &str) -> Result<String, EmitError> {
    if spec.fields.iter().any(|f| !f.fields.is_empty()) {
        return Err(EmitError::Unsupported(format!(
            "{}: nested structs not yet supported by owned emitter",
            spec.name
        )));
    }
    if !spec.common_structs.is_empty() {
        return Err(EmitError::Unsupported(format!(
            "{}: commonStructs not yet supported by owned emitter",
            spec.name
        )));
    }

    let mut out = banner(schemas_version);
    emit_imports(&mut out);
    emit_constants(&mut out, spec);
    emit_struct(&mut out, spec);
    emit_encode_impl(&mut out, spec);
    emit_decode_impl(&mut out, spec);
    out.push_str(FOOTER_IS_DEFAULT);
    Ok(out)
}

fn flex_min(spec: &MessageSpec) -> i16 {
    match spec.flexible_versions {
        FlexibleVersions::Range(r) => r.min,
        FlexibleVersions::None => i16::MAX,
    }
}

fn emit_imports(out: &mut String) {
    writeln!(out, "
use bytes::{{Buf, BufMut}};

use crate::primitives::fixed::{{get_bool, get_f64, get_i16, get_i32, get_i64, get_i8, put_bool, put_f64, put_i16, put_i32, put_i64, put_i8}};
use crate::primitives::string_bytes::{{
    compact_nullable_string_len, compact_string_len, get_compact_nullable_string_owned,
    get_compact_string_owned, get_nullable_string_owned, get_string_owned, nullable_string_len,
    put_compact_nullable_string, put_compact_string, put_nullable_string, put_string,
    string_len,
}};
use crate::tagged_fields::{{encode_to_bytes, read_tagged_fields, tagged_fields_len, WriteTaggedFields}};
use crate::{{Decode, Encode, ProtocolError, UnknownTaggedFields}};").unwrap();
}

fn emit_constants(out: &mut String, spec: &MessageSpec) {
    let api_key = spec.api_key.unwrap_or(0);
    let min_version = spec.valid_versions.min;
    let max_version = spec.valid_versions.max;
    let flex = flex_min(spec);
    writeln!(out, "
pub const API_KEY: i16 = {api_key};
pub const MIN_VERSION: i16 = {min_version};
pub const MAX_VERSION: i16 = {max_version};
pub const FLEXIBLE_MIN: i16 = {flex};

#[inline]
fn is_flexible(version: i16) -> bool {{ version >= FLEXIBLE_MIN }}").unwrap();
}

fn emit_struct(out: &mut String, spec: &MessageSpec) {
    let type_name = name_conv::type_name(&spec.name);
    writeln!(out, "
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct {type_name} {{").unwrap();

    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        let nullable = is_nullable(f);
        let rust_type = type_map::owned_type(&f.field_type, nullable, None);
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    for f in spec.fields.iter().filter(|f| is_tagged(f)) {
        let field = name_conv::field_name(&f.name);
        // Tagged fields are always wrapped in Option<...> on the typed side
        // when their `default` is null; otherwise the value carries the
        // default and absence on the wire restores it on decode.
        let nullable = is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
        let rust_type = type_map::owned_type(&f.field_type, nullable, None);
        writeln!(out, "    pub {field}: {rust_type},").unwrap();
    }
    writeln!(out, "    pub unknown_tagged_fields: UnknownTaggedFields,").unwrap();
    writeln!(out, "}}").unwrap();
}

fn emit_encode_impl(out: &mut String, spec: &MessageSpec) {
    let type_name = name_conv::type_name(&spec.name);
    writeln!(out, "
impl Encode for {type_name} {{
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            return Err(ProtocolError::UnsupportedVersion {{ api_key: API_KEY, version }});
        }}
        let _flex = is_flexible(version);").unwrap();

    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        emit_encode_one(out, f);
    }

    if has_any_flex(spec) {
        writeln!(out, "        if _flex {{").unwrap();
        writeln!(out, "            let mut tagged = WriteTaggedFields::new();").unwrap();
        for f in spec.fields.iter().filter(|f| is_tagged(f)) {
            emit_encode_tagged(out, f);
        }
        writeln!(out, "            tagged.write(buf, &self.unknown_tagged_fields);").unwrap();
        writeln!(out, "        }}").unwrap();
    }

    writeln!(out, "        Ok(())\n    }}").unwrap();

    // encoded_len
    writeln!(out, "    fn encoded_len(&self, version: i16) -> usize {{
        let _flex = is_flexible(version);
        let mut _n: usize = 0;").unwrap();
    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        emit_encoded_len_one(out, f);
    }
    if has_any_flex(spec) {
        writeln!(out, "        if _flex {{
            let mut _known_pairs: Vec<(u32, usize)> = Vec::new();").unwrap();
        for f in spec.fields.iter().filter(|f| is_tagged(f)) {
            emit_encoded_len_tagged(out, f);
        }
        writeln!(out, "            _n += tagged_fields_len(&_known_pairs, &self.unknown_tagged_fields);
        }}").unwrap();
    }
    writeln!(out, "        _n\n    }}\n}}").unwrap();
}

fn emit_decode_impl(out: &mut String, spec: &MessageSpec) {
    let type_name = name_conv::type_name(&spec.name);
    writeln!(out, "
impl<'de> Decode<'de> for {type_name} {{
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {{
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {{
            return Err(ProtocolError::UnsupportedVersion {{ api_key: API_KEY, version }});
        }}
        let _flex = is_flexible(version);
        let mut out = Self::default();").unwrap();
    for f in spec.fields.iter().filter(|f| !is_tagged(f)) {
        emit_decode_one(out, f);
    }
    if has_any_flex(spec) {
        writeln!(out, "        if _flex {{
            // Pre-declare typed slots for known tagged fields.").unwrap();
        for f in spec.fields.iter().filter(|f| is_tagged(f)) {
            let field = name_conv::field_name(&f.name);
            writeln!(out, "            let mut _tag_{field} = None;").unwrap();
        }
        writeln!(out, "            out.unknown_tagged_fields = read_tagged_fields(buf, |tag, payload| {{
                match tag {{").unwrap();
        for f in spec.fields.iter().filter(|f| is_tagged(f)) {
            emit_decode_tagged_arm(out, f);
        }
        writeln!(out, "                    _ => Ok(false),
                }}
            }})?;").unwrap();
        for f in spec.fields.iter().filter(|f| is_tagged(f)) {
            let field = name_conv::field_name(&f.name);
            writeln!(out, "            if let Some(v) = _tag_{field} {{ out.{field} = v; }}").unwrap();
        }
        writeln!(out, "        }}").unwrap();
    }
    writeln!(out, "        Ok(out)\n    }}\n}}").unwrap();
}

// --- single-field encode/decode helpers -----------------------------------

fn emit_encode_one(out: &mut String, f: &FieldSpec) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    let body = encode_call(&f.field_type, &format!("self.{field}"), is_nullable(f));
    writeln!(out, "        if {cond} {{ {body}; }}").unwrap();
}

fn emit_encoded_len_one(out: &mut String, f: &FieldSpec) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    let body = encoded_len_expr(&f.field_type, &format!("self.{field}"), is_nullable(f));
    writeln!(out, "        if {cond} {{ _n += {body}; }}").unwrap();
}

fn emit_decode_one(out: &mut String, f: &FieldSpec) {
    let field = name_conv::field_name(&f.name);
    let cond = version_cond(f.versions, "version");
    let call = decode_call(&f.field_type, is_nullable(f));
    writeln!(out, "        if {cond} {{ out.{field} = {call}; }}").unwrap();
}

fn emit_encode_tagged(out: &mut String, f: &FieldSpec) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    // Skip emitting if value equals default.
    writeln!(out, "            if !is_default(&self.{field}) {{
                let payload = encode_to_bytes({len_expr}, |b| {{ {encode}; }});
                tagged.add({tag}, payload);
            }}",
        len_expr = encoded_len_expr(&f.field_type, &format!("self.{field}"), is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null))),
        encode = encode_call(&f.field_type, &format!("self.{field}"), is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null))),
        tag = tag,
    ).unwrap();
}

fn emit_encoded_len_tagged(out: &mut String, f: &FieldSpec) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let len = encoded_len_expr(&f.field_type, &format!("self.{field}"), is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null)));
    writeln!(out, "            if !is_default(&self.{field}) {{
                _known_pairs.push(({tag}, {len}));
            }}").unwrap();
}

fn emit_decode_tagged_arm(out: &mut String, f: &FieldSpec) {
    let field = name_conv::field_name(&f.name);
    let tag = f.tag.expect("tagged field must have tag");
    let call = decode_call(&f.field_type, is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null)));
    writeln!(out, "                    {tag} => {{ _tag_{field} = Some({{ let _b: &mut &[u8] = payload; {call} }}); Ok(true) }}").unwrap();
}

// --- primitive encode/decode call generators ------------------------------

fn encode_call(schema_type: &str, expr: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        assert_is_primitive(elem, "encode_call");
        if nullable {
            return format!(
                "{{ let _len = ({expr}).as_ref().map(Vec::len); \
                 crate::primitives::array::put_nullable_array_len(buf, _len, _flex); \
                 if let Some(_v) = &{expr} {{ for _it in _v {{ {inner}; }} }} }}",
                inner = encode_call(elem, "_it", false),
            );
        }
        return format!(
            "{{ crate::primitives::array::put_array_len(buf, ({expr}).len(), _flex); \
             for _it in &{expr} {{ {inner}; }} }}",
            inner = encode_call(elem, "_it", false),
        );
    }
    match (schema_type, nullable) {
        ("int8",   _)     => format!("put_i8(buf, {expr})"),
        ("int16",  _)     => format!("put_i16(buf, {expr})"),
        ("int32",  _)     => format!("put_i32(buf, {expr})"),
        ("int64",  _)     => format!("put_i64(buf, {expr})"),
        ("bool",   _)     => format!("put_bool(buf, {expr})"),
        ("float64",_)     => format!("put_f64(buf, {expr})"),
        ("string", false) => format!("if _flex {{ put_compact_string(buf, &{expr}) }} else {{ put_string(buf, &{expr}) }}"),
        ("string", true)  => format!("if _flex {{ put_compact_nullable_string(buf, {expr}.as_deref()) }} else {{ put_nullable_string(buf, {expr}.as_deref()) }}"),
        (t, _) => format!("compile_error!(\"unhandled type in encode_call: {t}\")"),
    }
}

fn encoded_len_expr(schema_type: &str, expr: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        assert_is_primitive(elem, "encoded_len_expr");
        if nullable {
            return format!(
                "{{ let _opt: Option<&Vec<_>> = ({expr}).as_ref(); \
                 let _prefix = crate::primitives::array::nullable_array_len_prefix_len(_opt.map(|v| v.len()), _flex); \
                 let _body: usize = _opt.map_or(0, |v| v.iter().map(|_it| {inner}).sum()); \
                 _prefix + _body }}",
                inner = encoded_len_expr(elem, "*_it", false),
            );
        }
        return format!(
            "{{ let _prefix = crate::primitives::array::array_len_prefix_len(({expr}).len(), _flex); \
             let _body: usize = ({expr}).iter().map(|_it| {inner}).sum(); \
             _prefix + _body }}",
            inner = encoded_len_expr(elem, "*_it", false),
        );
    }
    match (schema_type, nullable) {
        ("int8",   _)     => "1".into(),
        ("int16",  _)     => "2".into(),
        ("int32",  _)     => "4".into(),
        ("int64",  _)     => "8".into(),
        ("bool",   _)     => "1".into(),
        ("float64",_)     => "8".into(),
        ("string", false) => format!("if _flex {{ compact_string_len(&{expr}) }} else {{ string_len(&{expr}) }}"),
        ("string", true)  => format!("if _flex {{ compact_nullable_string_len({expr}.as_deref()) }} else {{ nullable_string_len({expr}.as_deref()) }}"),
        (t, _) => format!("compile_error!(\"unhandled type in encoded_len_expr: {t}\")"),
    }
}

fn decode_call(schema_type: &str, nullable: bool) -> String {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        assert_is_primitive(elem, "decode_call");
        if nullable {
            return format!(
                "{{ let _opt = crate::primitives::array::get_nullable_array_len(buf, _flex)?; \
                 match _opt {{ None => None, Some(_n) => {{ let mut _v = Vec::with_capacity(_n); \
                 for _ in 0.._n {{ _v.push({inner}); }} Some(_v) }} }} }}",
                inner = decode_call(elem, false),
            );
        }
        return format!(
            "{{ let _n = crate::primitives::array::get_array_len(buf, _flex)?; \
             let mut _v = Vec::with_capacity(_n); for _ in 0.._n {{ _v.push({inner}); }} _v }}",
            inner = decode_call(elem, false),
        );
    }
    match (schema_type, nullable) {
        ("int8",   _)     => "get_i8(buf)?".into(),
        ("int16",  _)     => "get_i16(buf)?".into(),
        ("int32",  _)     => "get_i32(buf)?".into(),
        ("int64",  _)     => "get_i64(buf)?".into(),
        ("bool",   _)     => "get_bool(buf)?".into(),
        ("float64",_)     => "get_f64(buf)?".into(),
        ("string", false) => "if _flex { get_compact_string_owned(buf)? } else { get_string_owned(buf)? }".into(),
        ("string", true)  => "if _flex { get_compact_nullable_string_owned(buf)? } else { get_nullable_string_owned(buf)? }".into(),
        (t, _) => format!("compile_error!(\"unhandled type in decode_call: {t}\")"),
    }
}

// --- helpers --------------------------------------------------------------

/// Panic if `elem` looks like a struct type (PascalCase).  Array-of-struct
/// support is added in Task 7; for now we hard-fail so callers get a clear
/// message rather than a `compile_error!` in generated code.
fn assert_is_primitive(elem: &str, caller: &str) {
    assert!(
        !elem.chars().next().is_some_and(char::is_uppercase),
        "{caller}: array element type `{elem}` is a struct; \
         arrays of structs are not yet supported (Task 7)"
    );
}

fn is_tagged(f: &FieldSpec) -> bool { f.tag.is_some() }
fn is_nullable(f: &FieldSpec) -> bool { f.nullable_versions.is_some() }

fn has_any_flex(spec: &MessageSpec) -> bool {
    matches!(spec.flexible_versions, FlexibleVersions::Range(_))
}

fn version_cond(r: VersionRange, version_var: &str) -> String {
    if r.max == i16::MAX {
        format!("{version_var} >= {}", r.min)
    } else {
        format!("({version_var} >= {} && {version_var} <= {})", r.min, r.max)
    }
}

// is_default is generated into the produced module rather than read from a
// helper crate so the produced files have no extra crate dependency. We
// inject this short helper as part of the imports section. For now keep it
// at the end of every emitted file by extending banner; in Task 8 we move it
// to a shared place.

const FOOTER_IS_DEFAULT: &str = r#"
#[inline]
fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    v == &T::default()
}
"#;
