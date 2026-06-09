use std::collections::HashMap;

use proc_macro2::{Literal, Span, TokenStream};
use quote::{format_ident, quote};
use syn::LitStr;

use crate::emit::common::format_int_literal;
use crate::emit::{borrowed, owned};
use crate::ir::{FieldSpec, VersionRange};
use crate::name_conv;
use crate::resolve::{Resolution, StructKind};

type ResMap = HashMap<String, Resolution>;

pub(crate) fn path_tokens(path: &str) -> TokenStream {
    let (path, lifetime_a) = path
        .strip_suffix("<'a>")
        .map_or((path, false), |path| (path, true));
    let mut out = TokenStream::new();
    if path.starts_with("::") {
        out.extend(quote!(::));
    }
    for (i, segment) in path.trim_start_matches("::").split("::").enumerate() {
        if i > 0 {
            out.extend(quote!(::));
        }
        match segment {
            "crate" => out.extend(quote!(crate)),
            "self" => out.extend(quote!(self)),
            "super" => out.extend(quote!(super)),
            "Self" => out.extend(quote!(Self)),
            other => {
                let ident = format_ident!("{other}");
                out.extend(quote!(#ident));
            }
        }
    }
    if lifetime_a { quote!(#out <'a>) } else { out }
}

pub(crate) fn owned_type_tokens(
    schema_type: &str,
    nullable: bool,
    struct_path: Option<&str>,
) -> TokenStream {
    let inner = inner_owned_type_tokens(schema_type, struct_path);
    if nullable {
        quote!(Option<#inner>)
    } else {
        inner
    }
}

fn inner_owned_type_tokens(schema_type: &str, struct_path: Option<&str>) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let inner = inner_owned_type_tokens(elem, struct_path);
        return quote!(Vec<#inner>);
    }
    match schema_type {
        "bool" => quote!(bool),
        "int8" => quote!(i8),
        "int16" => quote!(i16),
        "int32" => quote!(i32),
        "int64" => quote!(i64),
        "uint16" => quote!(u16),
        "uint32" => quote!(u32),
        "float64" => quote!(f64),
        "string" => quote!(String),
        "bytes" => quote!(::bytes::Bytes),
        "records" => quote!(crate::records::RecordsPayload),
        "uuid" => quote!(crate::primitives::uuid::Uuid),
        other => struct_path
            .map(path_tokens)
            .unwrap_or_else(|| panic!("unmapped owned type: {other}")),
    }
}

pub(crate) fn borrowed_type_tokens(
    schema_type: &str,
    nullable: bool,
    struct_path: Option<&str>,
) -> TokenStream {
    let inner = inner_borrowed_type_tokens(schema_type, struct_path);
    if nullable {
        quote!(Option<#inner>)
    } else {
        inner
    }
}

fn inner_borrowed_type_tokens(schema_type: &str, struct_path: Option<&str>) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let inner = inner_borrowed_type_tokens(elem, struct_path);
        return quote!(Vec<#inner>);
    }
    match schema_type {
        "bool" => quote!(bool),
        "int8" => quote!(i8),
        "int16" => quote!(i16),
        "int32" => quote!(i32),
        "int64" => quote!(i64),
        "uint16" => quote!(u16),
        "uint32" => quote!(u32),
        "float64" => quote!(f64),
        "string" => quote!(&'a str),
        "bytes" => quote!(&'a [u8]),
        "records" => quote!(crate::records::RecordsPayloadBorrowed<'a>),
        "uuid" => quote!(crate::primitives::uuid::Uuid),
        other => struct_path
            .map(path_tokens)
            .unwrap_or_else(|| panic!("unmapped borrowed type: {other}")),
    }
}

pub(crate) fn version_cond_tokens(r: VersionRange) -> TokenStream {
    let min = Literal::i16_unsuffixed(r.min);
    if r.max == i16::MAX {
        quote!(version >= #min)
    } else {
        let max = Literal::i16_unsuffixed(r.max);
        quote!(version >= #min && version <= #max)
    }
}

pub(crate) fn nullable_split_cond_tokens(f: &FieldSpec) -> Option<TokenStream> {
    let r = f.nullable_versions?;
    let need_lower = r.min > f.versions.min;
    let need_upper = r.max < f.versions.max;
    match (need_lower, need_upper) {
        (false, false) => None,
        (true, false) => {
            let min = Literal::i16_unsuffixed(r.min);
            Some(quote!(version >= #min))
        }
        (false, true) => {
            let max = Literal::i16_unsuffixed(r.max);
            Some(quote!(version <= #max))
        }
        (true, true) => {
            let min = Literal::i16_unsuffixed(r.min);
            let max = Literal::i16_unsuffixed(r.max);
            Some(quote!(version >= #min && version <= #max))
        }
    }
}

pub(crate) fn owned_default_tokens(f: &FieldSpec) -> TokenStream {
    let base = owned::base_type(&f.field_type);
    let is_array = f.field_type.starts_with("[]");
    let nullable = owned::is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");
    if nullable {
        if default_is_null || f.default.is_none() {
            return quote!(None);
        }
        if let Some(v) = &f.default {
            let scalar = scalar_owned_default_tokens(base, v);
            return quote!(Some(#scalar));
        }
    }
    if is_array {
        return quote!(Vec::new());
    }
    f.default.as_ref().map_or_else(
        || owned_zero_tokens(base),
        |v| scalar_owned_default_tokens(base, v),
    )
}

fn scalar_owned_default_tokens(base_type: &str, val: &serde_json::Value) -> TokenStream {
    match (base_type, val) {
        (_, serde_json::Value::String(s)) if s == "null" => quote!(None),
        ("string", serde_json::Value::String(s)) => {
            let lit = LitStr::new(s, Span::call_site());
            quote!(#lit.to_string())
        }
        ("bool", serde_json::Value::String(s)) if s == "true" => quote!(true),
        ("bool", serde_json::Value::String(_)) => quote!(false),
        ("bool", serde_json::Value::Bool(b)) => quote!(#b),
        ("int8", serde_json::Value::String(s)) => {
            let n = s.trim().parse::<i8>().expect("int8 default");
            let lit = Literal::i8_suffixed(n);
            quote!(#lit)
        }
        ("int16", serde_json::Value::String(s)) => {
            let n = s.trim().parse::<i16>().expect("int16 default");
            let lit = Literal::i16_suffixed(n);
            quote!(#lit)
        }
        ("int32", serde_json::Value::String(s)) => {
            let n = normalized_i64(s, "i32") as i32;
            let lit = Literal::i32_suffixed(n);
            quote!(#lit)
        }
        ("int64", serde_json::Value::String(s)) => {
            let n = normalized_i64(s, "i64");
            let lit = Literal::i64_suffixed(n);
            quote!(#lit)
        }
        ("int8", serde_json::Value::Number(n)) => {
            let lit = Literal::i8_suffixed(n.as_i64().expect("int8 default") as i8);
            quote!(#lit)
        }
        ("int16", serde_json::Value::Number(n)) => {
            let lit = Literal::i16_suffixed(n.as_i64().expect("int16 default") as i16);
            quote!(#lit)
        }
        ("int32", serde_json::Value::Number(n)) => {
            let lit = Literal::i32_suffixed(n.as_i64().expect("int32 default") as i32);
            quote!(#lit)
        }
        ("int64", serde_json::Value::Number(n)) => {
            let lit = Literal::i64_suffixed(n.as_i64().expect("int64 default"));
            quote!(#lit)
        }
        _ => owned_zero_tokens(base_type),
    }
}

fn owned_zero_tokens(base: &str) -> TokenStream {
    match base {
        "string" => quote!(String::new()),
        "bytes" => quote!(bytes::Bytes::new()),
        "bool" => quote!(false),
        "int8" => quote!(0i8),
        "int16" => quote!(0i16),
        "int32" => quote!(0i32),
        "int64" => quote!(0i64),
        "uint16" => quote!(0u16),
        "uint32" => quote!(0u32),
        "float64" => quote!(0.0f64),
        _ => quote!(Default::default()),
    }
}

pub(crate) fn borrowed_default_tokens(f: &FieldSpec) -> TokenStream {
    let base = borrowed::base_type(&f.field_type);
    let is_array = f.field_type.starts_with("[]");
    let nullable = borrowed::is_nullable(f) || matches!(&f.default, Some(serde_json::Value::Null));
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");
    if nullable {
        if default_is_null || f.default.is_none() {
            return quote!(None);
        }
        if let Some(v) = &f.default {
            let scalar = scalar_borrowed_default_tokens(base, v);
            return quote!(Some(#scalar));
        }
    }
    if is_array {
        return quote!(Vec::new());
    }
    f.default.as_ref().map_or_else(
        || borrowed_zero_tokens(base),
        |v| scalar_borrowed_default_tokens(base, v),
    )
}

fn scalar_borrowed_default_tokens(base_type: &str, val: &serde_json::Value) -> TokenStream {
    match (base_type, val) {
        (_, serde_json::Value::String(s)) if s == "null" => quote!(None),
        ("string", serde_json::Value::String(s)) => {
            let lit = LitStr::new(s, Span::call_site());
            quote!(#lit)
        }
        ("bool", serde_json::Value::Bool(b)) => quote!(#b),
        ("bool", serde_json::Value::String(s)) if s == "true" => quote!(true),
        ("bool", serde_json::Value::String(_)) => quote!(false),
        ("int8", serde_json::Value::String(s)) => {
            let lit = Literal::i8_suffixed(s.trim().parse::<i8>().expect("int8 default"));
            quote!(#lit)
        }
        ("int16", serde_json::Value::String(s)) => {
            let lit = Literal::i16_suffixed(s.trim().parse::<i16>().expect("int16 default"));
            quote!(#lit)
        }
        ("int32", serde_json::Value::String(s)) => {
            let lit = Literal::i32_suffixed(normalized_i64(s, "i32") as i32);
            quote!(#lit)
        }
        ("int64", serde_json::Value::String(s)) => {
            let lit = Literal::i64_suffixed(normalized_i64(s, "i64"));
            quote!(#lit)
        }
        ("int8", serde_json::Value::Number(n)) => {
            let lit = Literal::i8_suffixed(n.as_i64().expect("int8 default") as i8);
            quote!(#lit)
        }
        ("int16", serde_json::Value::Number(n)) => {
            let lit = Literal::i16_suffixed(n.as_i64().expect("int16 default") as i16);
            quote!(#lit)
        }
        ("int32", serde_json::Value::Number(n)) => {
            let lit = Literal::i32_suffixed(n.as_i64().expect("int32 default") as i32);
            quote!(#lit)
        }
        ("int64", serde_json::Value::Number(n)) => {
            let lit = Literal::i64_suffixed(n.as_i64().expect("int64 default"));
            quote!(#lit)
        }
        _ => borrowed_zero_tokens(base_type),
    }
}

fn borrowed_zero_tokens(base: &str) -> TokenStream {
    match base {
        "string" => quote!(""),
        "bytes" => quote!(&[]),
        "bool" => quote!(false),
        "int8" => quote!(0i8),
        "int16" => quote!(0i16),
        "int32" => quote!(0i32),
        "int64" => quote!(0i64),
        "uint16" => quote!(0u16),
        "uint32" => quote!(0u32),
        "float64" => quote!(0.0f64),
        _ => quote!(Default::default()),
    }
}

fn normalized_i64(value: &str, suffix: &str) -> i64 {
    let formatted = format_int_literal(value.trim(), suffix);
    let digits = formatted
        .strip_suffix(suffix)
        .unwrap_or(&formatted)
        .replace('_', "");
    digits.parse::<i64>().expect("integer schema default")
}

pub(crate) fn tagged_is_default_tokens(f: &FieldSpec, borrowed_flavor: bool) -> TokenStream {
    let field = format_ident!("{}", name_conv::field_name(&f.name));
    let base = if borrowed_flavor {
        borrowed::base_type(&f.field_type)
    } else {
        owned::base_type(&f.field_type)
    };
    let nullable = if borrowed_flavor {
        borrowed::is_nullable(f)
    } else {
        owned::is_nullable(f)
    } || matches!(&f.default, Some(serde_json::Value::Null));
    let default_is_null = matches!(&f.default, Some(serde_json::Value::Null))
        || matches!(&f.default, Some(serde_json::Value::String(s)) if s == "null");
    if nullable && (default_is_null || f.default.is_none()) {
        return quote!(self.#field.is_none());
    }
    if let Some(v) = &f.default {
        let cmp = if borrowed_flavor {
            scalar_borrowed_default_tokens(base, v)
        } else {
            scalar_owned_default_tokens(base, v)
        };
        if matches!(v, serde_json::Value::Null)
            || matches!(v, serde_json::Value::String(s) if s == "null")
        {
            return quote!(self.#field.is_none());
        }
        if nullable {
            return quote!(self.#field == Some(#cmp));
        }
        if f.field_type.starts_with("[]") {
            return quote!(self.#field.is_empty());
        }
        return quote!(self.#field == #cmp);
    }
    quote!(crate::codegen_helpers::is_default(&self.#field))
}

pub(crate) fn owned_populated_value_tokens(
    f: &FieldSpec,
    res_map: &ResMap,
    option: bool,
) -> TokenStream {
    let base = owned::base_type(&f.field_type);
    if base == "records" {
        return owned_default_tokens(f);
    }
    let elem = owned_populated_scalar_tokens(base, f, res_map);
    let inner = if f.field_type.starts_with("[]") {
        quote!(vec![#elem])
    } else {
        elem
    };
    if option { quote!(Some(#inner)) } else { inner }
}

fn owned_populated_scalar_tokens(base: &str, f: &FieldSpec, res_map: &ResMap) -> TokenStream {
    match base {
        "bool" => quote!(true),
        "int8" => quote!(1i8),
        "int16" => quote!(1i16),
        "int32" => quote!(1i32),
        "int64" => quote!(1i64),
        "uint16" => quote!(1u16),
        "uint32" => quote!(1u32),
        "float64" => quote!(1.0f64),
        "string" => quote!("x".to_string()),
        "bytes" => quote!(::bytes::Bytes::from_static(b"x")),
        "uuid" => quote!(crate::primitives::uuid::Uuid([1u8; 16])),
        _ => {
            let path = owned::struct_path_for(f, res_map).expect("struct field must resolve");
            let path = path_tokens(&path);
            quote!(#path::populated(version))
        }
    }
}

pub(crate) fn borrowed_populated_value_tokens(
    f: &FieldSpec,
    res_map: &ResMap,
    parent_module: &str,
    option: bool,
) -> TokenStream {
    let base = borrowed::base_type(&f.field_type);
    let owned = borrowed::is_tagged(f) && borrowed::tagged_field_needs_owned(f, res_map);
    let elem = borrowed_populated_scalar_tokens(base, f, res_map, parent_module, owned);
    let inner = if f.field_type.starts_with("[]") {
        quote!(vec![#elem])
    } else {
        elem
    };
    if option { quote!(Some(#inner)) } else { inner }
}

fn borrowed_populated_scalar_tokens(
    base: &str,
    f: &FieldSpec,
    res_map: &ResMap,
    parent_module: &str,
    owned_value: bool,
) -> TokenStream {
    match base {
        "bool" => quote!(true),
        "int8" => quote!(1i8),
        "int16" => quote!(1i16),
        "int32" => quote!(1i32),
        "int64" => quote!(1i64),
        "uint16" => quote!(1u16),
        "uint32" => quote!(1u32),
        "float64" => quote!(1.0f64),
        "uuid" => quote!(crate::primitives::uuid::Uuid([1u8; 16])),
        "string" if owned_value => quote!("x".to_string()),
        "string" => quote!("x"),
        "bytes" if owned_value => quote!(::bytes::Bytes::from_static(b"x")),
        "bytes" => quote!(&b"x"[..]),
        _ if owned_value => {
            let path = borrowed::owned_struct_path_for(f, parent_module, res_map)
                .expect("struct field must resolve");
            let path = path_tokens(&path);
            quote!(#path::populated(version))
        }
        _ => {
            let path = res_map
                .get(base)
                .map(|r| r.rust_path.as_str())
                .expect("struct field must resolve");
            let path = path_tokens(path);
            quote!(#path::populated(version))
        }
    }
}

pub(crate) fn owned_encode_call(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = owned::base_type(elem);
        if owned::is_struct_type(elem_base) {
            return if nullable {
                quote!({
                    let len = (#expr).as_ref().map(Vec::len);
                    crate::primitives::array::put_nullable_array_len(#buf, len, flex);
                    if let Some(v) = &#expr {
                        for it in v {
                            it.encode(#buf, version)?;
                        }
                    }
                })
            } else {
                quote!({
                    crate::primitives::array::put_array_len(#buf, (#expr).len(), flex);
                    for it in &#expr {
                        it.encode(#buf, version)?;
                    }
                })
            };
        }
        let inner = owned_encode_call(elem, quote!(*it), false, res_map, buf);
        return if nullable {
            quote!({
                let len = (#expr).as_ref().map(Vec::len);
                crate::primitives::array::put_nullable_array_len(#buf, len, flex);
                if let Some(v) = &#expr {
                    for it in v {
                        #inner;
                    }
                }
            })
        } else {
            quote!({
                crate::primitives::array::put_array_len(#buf, (#expr).len(), flex);
                for it in &#expr {
                    #inner;
                }
            })
        };
    }

    if owned::is_struct_type(schema_type) {
        return if nullable {
            quote!(match &#expr {
                None => { #buf.put_i8(-1); }
                Some(v) => { #buf.put_i8(1); v.encode(#buf, version)?; }
            })
        } else {
            quote!(#expr.encode(#buf, version)?)
        };
    }

    match (schema_type, nullable) {
        ("int8", _) => quote!(put_i8(#buf, #expr)),
        ("int16", _) => quote!(put_i16(#buf, #expr)),
        ("uint16", _) => quote!(put_u16(#buf, #expr)),
        ("int32", _) => quote!(put_i32(#buf, #expr)),
        ("int64", _) => quote!(put_i64(#buf, #expr)),
        ("bool", _) => quote!(put_bool(#buf, #expr)),
        ("float64", _) => quote!(put_f64(#buf, #expr)),
        ("uuid", _) => quote!(crate::primitives::uuid::put_uuid(#buf, #expr)),
        ("string", false) => {
            quote!(if flex { put_compact_string(#buf, &#expr) } else { put_string(#buf, &#expr) })
        }
        ("string", true) => quote!(
            if flex {
                put_compact_nullable_string(#buf, #expr.as_deref())
            } else {
                put_nullable_string(#buf, #expr.as_deref())
            }
        ),
        ("bytes", false) => {
            quote!(if flex { put_compact_bytes(#buf, &#expr) } else { put_bytes(#buf, &#expr) })
        }
        ("bytes", true) => quote!(
            if flex {
                put_compact_nullable_bytes(#buf, #expr.as_deref())
            } else {
                put_nullable_bytes(#buf, #expr.as_deref())
            }
        ),
        ("records", false) => quote!({
            let mut __rb_buf = bytes::BytesMut::new();
            <crate::records::RecordsPayload as crate::Encode>::encode(&#expr, &mut __rb_buf, version)?;
            if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
        }),
        ("records", true) => quote!(match &#expr {
            None => if flex { put_compact_nullable_bytes(#buf, None) } else { put_nullable_bytes(#buf, None) },
            Some(__rb) => {
                let mut __rb_buf = bytes::BytesMut::new();
                <crate::records::RecordsPayload as crate::Encode>::encode(__rb, &mut __rb_buf, version)?;
                if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
            }
        }),
        (t, _) => {
            let msg = format!("unhandled type in encode_call: {t}");
            quote!(compile_error!(#msg))
        }
    }
}

pub(crate) fn owned_encode_call_option_as_non_nullable(
    schema_type: &str,
    expr: TokenStream,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = owned::base_type(elem);
        if owned::is_struct_type(elem_base) {
            return quote!({
                let v = (#expr).as_ref().map(Vec::as_slice).unwrap_or(&[]);
                crate::primitives::array::put_array_len(#buf, v.len(), flex);
                for it in v {
                    it.encode(#buf, version)?;
                }
            });
        }
        let inner = owned_encode_call(elem, quote!(*it), false, res_map, buf);
        return quote!({
            let v = (#expr).as_ref().map(Vec::as_slice).unwrap_or(&[]);
            crate::primitives::array::put_array_len(#buf, v.len(), flex);
            for it in v {
                #inner;
            }
        });
    }

    match schema_type {
        "string" => quote!(
            if flex {
                put_compact_string(#buf, (#expr).as_deref().unwrap_or(""))
            } else {
                put_string(#buf, (#expr).as_deref().unwrap_or(""))
            }
        ),
        "uuid" => quote!(crate::primitives::uuid::put_uuid(#buf, (#expr).unwrap_or_default())),
        "records" => quote!(match &#expr {
            None => {
                let __rb_buf = bytes::BytesMut::new();
                if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
            }
            Some(__rb) => {
                let mut __rb_buf = bytes::BytesMut::new();
                <crate::records::RecordsPayload as crate::Encode>::encode(__rb, &mut __rb_buf, version)?;
                if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
            }
        }),
        _ => owned_encode_call(
            schema_type,
            quote!((#expr).unwrap_or_default()),
            false,
            res_map,
            buf,
        ),
    }
}

pub(crate) fn borrowed_encode_call(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = borrowed::base_type(elem);
        if borrowed::is_struct_type(elem_base) {
            return if nullable {
                quote!({
                    let len = (#expr).as_ref().map(Vec::len);
                    crate::primitives::array::put_nullable_array_len(#buf, len, flex);
                    if let Some(v) = &#expr {
                        for it in v {
                            it.encode(#buf, version)?;
                        }
                    }
                })
            } else {
                quote!({
                    crate::primitives::array::put_array_len(#buf, (#expr).len(), flex);
                    for it in &#expr {
                        it.encode(#buf, version)?;
                    }
                })
            };
        }
        let inner = borrowed_encode_call(elem, quote!(*it), false, res_map, buf);
        return if nullable {
            quote!({
                let len = (#expr).as_ref().map(Vec::len);
                crate::primitives::array::put_nullable_array_len(#buf, len, flex);
                if let Some(v) = &#expr {
                    for it in v {
                        #inner;
                    }
                }
            })
        } else {
            quote!({
                crate::primitives::array::put_array_len(#buf, (#expr).len(), flex);
                for it in &#expr {
                    #inner;
                }
            })
        };
    }

    if borrowed::is_struct_type(schema_type) {
        return if nullable {
            quote!(match &#expr {
                None => { #buf.put_i8(-1); }
                Some(v) => { #buf.put_i8(1); v.encode(#buf, version)?; }
            })
        } else {
            quote!(#expr.encode(#buf, version)?)
        };
    }

    match (schema_type, nullable) {
        ("int8", _) => quote!(put_i8(#buf, #expr)),
        ("int16", _) => quote!(put_i16(#buf, #expr)),
        ("uint16", _) => quote!(put_u16(#buf, #expr)),
        ("int32", _) => quote!(put_i32(#buf, #expr)),
        ("int64", _) => quote!(put_i64(#buf, #expr)),
        ("bool", _) => quote!(put_bool(#buf, #expr)),
        ("float64", _) => quote!(put_f64(#buf, #expr)),
        ("uuid", _) => quote!(crate::primitives::uuid::put_uuid(#buf, #expr)),
        ("string", false) => {
            quote!(if flex { put_compact_string(#buf, #expr) } else { put_string(#buf, #expr) })
        }
        ("string", true) => quote!(
            if flex { put_compact_nullable_string(#buf, #expr) } else { put_nullable_string(#buf, #expr) }
        ),
        ("bytes", false) => {
            quote!(if flex { put_compact_bytes(#buf, #expr) } else { put_bytes(#buf, #expr) })
        }
        ("bytes", true) => quote!(
            if flex { put_compact_nullable_bytes(#buf, #expr) } else { put_nullable_bytes(#buf, #expr) }
        ),
        ("records", false) => quote!({
            let mut __rb_buf = bytes::BytesMut::new();
            <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(&#expr, &mut __rb_buf, version)?;
            if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
        }),
        ("records", true) => quote!(match &#expr {
            None => if flex { put_compact_nullable_bytes(#buf, None) } else { put_nullable_bytes(#buf, None) },
            Some(__rb) => {
                let mut __rb_buf = bytes::BytesMut::new();
                <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(__rb, &mut __rb_buf, version)?;
                if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
            }
        }),
        (t, _) => {
            let msg = format!("unhandled type in encode_call (borrowed): {t}");
            quote!(compile_error!(#msg))
        }
    }
}

pub(crate) fn borrowed_encode_call_option_as_non_nullable(
    schema_type: &str,
    expr: TokenStream,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = borrowed::base_type(elem);
        if borrowed::is_struct_type(elem_base) {
            return quote!({
                let v = (#expr).as_ref().map(Vec::as_slice).unwrap_or(&[]);
                crate::primitives::array::put_array_len(#buf, v.len(), flex);
                for it in v {
                    it.encode(#buf, version)?;
                }
            });
        }
        let inner = borrowed_encode_call(elem, quote!(*it), false, res_map, buf);
        return quote!({
            let v = (#expr).as_ref().map(Vec::as_slice).unwrap_or(&[]);
            crate::primitives::array::put_array_len(#buf, v.len(), flex);
            for it in v {
                #inner;
            }
        });
    }

    match schema_type {
        "string" => quote!(
            if flex {
                put_compact_string(#buf, (#expr).unwrap_or(""))
            } else {
                put_string(#buf, (#expr).unwrap_or(""))
            }
        ),
        "uuid" => quote!(crate::primitives::uuid::put_uuid(#buf, (#expr).unwrap_or_default())),
        "records" => quote!(match &#expr {
            None => {
                let __rb_buf = bytes::BytesMut::new();
                if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
            }
            Some(__rb) => {
                let mut __rb_buf = bytes::BytesMut::new();
                <crate::records::RecordsPayloadBorrowed as crate::Encode>::encode(__rb, &mut __rb_buf, version)?;
                if flex { put_compact_bytes(#buf, &__rb_buf) } else { put_bytes(#buf, &__rb_buf) }
            }
        }),
        _ => borrowed_encode_call(
            schema_type,
            quote!((#expr).unwrap_or_default()),
            false,
            res_map,
            buf,
        ),
    }
}

pub(crate) fn borrowed_owned_encode_call(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
) -> TokenStream {
    owned_encode_call(schema_type, expr, nullable, res_map, buf)
}

pub(crate) fn owned_encoded_len_expr(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    res_map: &ResMap,
) -> TokenStream {
    encoded_len_expr(schema_type, expr, nullable, res_map, LenFlavor::Owned)
}

pub(crate) fn borrowed_encoded_len_expr(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    res_map: &ResMap,
) -> TokenStream {
    encoded_len_expr(schema_type, expr, nullable, res_map, LenFlavor::Borrowed)
}

pub(crate) fn borrowed_owned_encoded_len_expr(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    res_map: &ResMap,
) -> TokenStream {
    owned_encoded_len_expr(schema_type, expr, nullable, res_map)
}

pub(crate) fn owned_encoded_len_expr_option_as_non_nullable(
    schema_type: &str,
    expr: TokenStream,
    res_map: &ResMap,
) -> TokenStream {
    encoded_len_expr_option_as_non_nullable(schema_type, expr, res_map, LenFlavor::Owned)
}

pub(crate) fn borrowed_encoded_len_expr_option_as_non_nullable(
    schema_type: &str,
    expr: TokenStream,
    res_map: &ResMap,
) -> TokenStream {
    encoded_len_expr_option_as_non_nullable(schema_type, expr, res_map, LenFlavor::Borrowed)
}

#[derive(Clone, Copy)]
enum LenFlavor {
    Owned,
    Borrowed,
}

fn encoded_len_expr(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    res_map: &ResMap,
    flavor: LenFlavor,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = match flavor {
            LenFlavor::Owned => owned::base_type(elem),
            LenFlavor::Borrowed => borrowed::base_type(elem),
        };
        let is_struct = match flavor {
            LenFlavor::Owned => owned::is_struct_type(elem_base),
            LenFlavor::Borrowed => borrowed::is_struct_type(elem_base),
        };
        if is_struct {
            return if nullable {
                quote!({
                    let opt: Option<&Vec<_>> = (#expr).as_ref();
                    let prefix = crate::primitives::array::nullable_array_len_prefix_len(opt.map(|v| v.len()), flex);
                    let body: usize = opt.map_or(0, |v| v.iter().map(|it| it.encoded_len(version)).sum());
                    prefix + body
                })
            } else {
                quote!({
                    let prefix = crate::primitives::array::array_len_prefix_len((#expr).len(), flex);
                    let body: usize = (#expr).iter().map(|it| it.encoded_len(version)).sum();
                    prefix + body
                })
            };
        }
        let inner = encoded_len_expr(elem, quote!(*it), false, res_map, flavor);
        let closure = if fixed_len_primitive(elem) {
            quote!(_)
        } else {
            quote!(it)
        };
        return if nullable {
            quote!({
                let opt: Option<&Vec<_>> = (#expr).as_ref();
                let prefix = crate::primitives::array::nullable_array_len_prefix_len(opt.map(|v| v.len()), flex);
                let body: usize = opt.map_or(0, |v| v.iter().map(|#closure| #inner).sum());
                prefix + body
            })
        } else {
            quote!({
                let prefix = crate::primitives::array::array_len_prefix_len((#expr).len(), flex);
                let body: usize = (#expr).iter().map(|#closure| #inner).sum();
                prefix + body
            })
        };
    }

    let is_struct = match flavor {
        LenFlavor::Owned => owned::is_struct_type(schema_type),
        LenFlavor::Borrowed => borrowed::is_struct_type(schema_type),
    };
    if is_struct {
        return if nullable {
            quote!(1 + #expr.as_ref().map_or(0, |v| v.encoded_len(version)))
        } else {
            quote!(#expr.encoded_len(version))
        };
    }

    match (flavor, schema_type, nullable) {
        (_, "int8" | "bool", _) => quote!(1),
        (_, "int16" | "uint16", _) => quote!(2),
        (_, "int32", _) => quote!(4),
        (_, "int64" | "float64", _) => quote!(8),
        (_, "uuid", _) => quote!(16),
        (LenFlavor::Owned, "string", false) => {
            quote!(if flex { compact_string_len(&#expr) } else { string_len(&#expr) })
        }
        (LenFlavor::Owned, "string", true) => quote!(
            if flex { compact_nullable_string_len(#expr.as_deref()) } else { nullable_string_len(#expr.as_deref()) }
        ),
        (LenFlavor::Owned, "bytes", false) => {
            quote!(if flex { compact_bytes_len(&#expr) } else { bytes_len(&#expr) })
        }
        (LenFlavor::Owned, "bytes", true) => quote!(
            if flex { compact_nullable_bytes_len(#expr.as_deref()) } else { nullable_bytes_len(#expr.as_deref()) }
        ),
        (LenFlavor::Owned, "records", false) => quote!({
            let __rb_len = <crate::records::RecordsPayload as crate::Encode>::encoded_len(&#expr, version);
            if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) } else { 4 + __rb_len }
        }),
        (LenFlavor::Owned, "records", true) => quote!(match &#expr {
            None => if flex { crate::primitives::varint::uvarint_len(0) } else { 4 },
            Some(__rb) => {
                let __rb_len = <crate::records::RecordsPayload as crate::Encode>::encoded_len(__rb, version);
                if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) } else { 4 + __rb_len }
            }
        }),
        (LenFlavor::Borrowed, "string", false) => {
            quote!(if flex { compact_string_len(#expr) } else { string_len(#expr) })
        }
        (LenFlavor::Borrowed, "string", true) => quote!(
            if flex { compact_nullable_string_len(#expr) } else { nullable_string_len(#expr) }
        ),
        (LenFlavor::Borrowed, "bytes", false) => quote!(
            if flex {
                crate::primitives::varint::uvarint_len(u32::try_from((#expr).len() + 1).unwrap()) + (#expr).len()
            } else {
                4 + (#expr).len()
            }
        ),
        (LenFlavor::Borrowed, "bytes", true) => quote!(match #expr {
            None => if flex { 1 } else { 4 },
            Some(b) => if flex {
                crate::primitives::varint::uvarint_len(u32::try_from(b.len() + 1).unwrap()) + b.len()
            } else {
                4 + b.len()
            }
        }),
        (LenFlavor::Borrowed, "records", false) => quote!({
            let __rb_len = <crate::records::RecordsPayloadBorrowed as crate::Encode>::encoded_len(&(#expr), version);
            if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) } else { 4 + __rb_len }
        }),
        (LenFlavor::Borrowed, "records", true) => quote!(match &#expr {
            None => if flex { crate::primitives::varint::uvarint_len(0) } else { 4 },
            Some(__rb) => {
                let __rb_len = <crate::records::RecordsPayloadBorrowed as crate::Encode>::encoded_len(__rb, version);
                if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) } else { 4 + __rb_len }
            }
        }),
        (_, t, _) => {
            let msg = format!("unhandled type in encoded_len_expr: {t}");
            quote!(compile_error!(#msg))
        }
    }
}

fn fixed_len_primitive(schema_type: &str) -> bool {
    matches!(
        schema_type,
        "int8" | "int16" | "uint16" | "int32" | "int64" | "bool" | "float64" | "uuid"
    )
}

fn encoded_len_expr_option_as_non_nullable(
    schema_type: &str,
    expr: TokenStream,
    res_map: &ResMap,
    flavor: LenFlavor,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = match flavor {
            LenFlavor::Owned => owned::base_type(elem),
            LenFlavor::Borrowed => borrowed::base_type(elem),
        };
        let is_struct = match flavor {
            LenFlavor::Owned => owned::is_struct_type(elem_base),
            LenFlavor::Borrowed => borrowed::is_struct_type(elem_base),
        };
        if is_struct {
            return quote!({
                let v = (#expr).as_ref().map(Vec::as_slice).unwrap_or(&[]);
                let prefix = crate::primitives::array::array_len_prefix_len(v.len(), flex);
                let body: usize = v.iter().map(|it| it.encoded_len(version)).sum();
                prefix + body
            });
        }
        let inner = encoded_len_expr(elem, quote!(*it), false, res_map, flavor);
        let closure = if fixed_len_primitive(elem) {
            quote!(_)
        } else {
            quote!(it)
        };
        return quote!({
            let v = (#expr).as_ref().map(Vec::as_slice).unwrap_or(&[]);
            let prefix = crate::primitives::array::array_len_prefix_len(v.len(), flex);
            let body: usize = v.iter().map(|#closure| #inner).sum();
            prefix + body
        });
    }
    match (flavor, schema_type) {
        (LenFlavor::Owned, "string") => quote!(
            if flex {
                compact_string_len((#expr).as_deref().unwrap_or(""))
            } else {
                string_len((#expr).as_deref().unwrap_or(""))
            }
        ),
        (LenFlavor::Borrowed, "string") => quote!(
            if flex {
                compact_string_len((#expr).unwrap_or(""))
            } else {
                string_len((#expr).unwrap_or(""))
            }
        ),
        (_, "uuid") => quote!(16),
        (LenFlavor::Owned, "records") => quote!(match &#expr {
            None => if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(0) } else { 4 },
            Some(__rb) => {
                let __rb_len = <crate::records::RecordsPayload as crate::Encode>::encoded_len(__rb, version);
                if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) } else { 4 + __rb_len }
            }
        }),
        (LenFlavor::Borrowed, "records") => quote!(match &#expr {
            None => if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(0) } else { 4 },
            Some(__rb) => {
                let __rb_len = <crate::records::RecordsPayloadBorrowed as crate::Encode>::encoded_len(__rb, version);
                if flex { crate::primitives::string_bytes::compact_bytes_len_from_size(__rb_len) } else { 4 + __rb_len }
            }
        }),
        _ => encoded_len_expr(
            schema_type,
            quote!((#expr).unwrap_or_default()),
            false,
            res_map,
            flavor,
        ),
    }
}

pub(crate) fn owned_decode_call(
    schema_type: &str,
    nullable: bool,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
    lenient_records: bool,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = owned::base_type(elem);
        if owned::is_struct_type(elem_base) {
            let path = res_map
                .get(elem_base)
                .map_or_else(|| path_tokens(elem_base), |r| path_tokens(&r.rust_path));
            return if nullable {
                quote!({
                    let opt = crate::primitives::array::get_nullable_array_len(#buf, flex)?;
                    match opt {
                        None => None,
                        Some(n) => {
                            let mut v = Vec::with_capacity(n);
                            for _ in 0..n {
                                v.push(#path::decode(#buf, version)?);
                            }
                            Some(v)
                        }
                    }
                })
            } else {
                quote!({
                    let n = crate::primitives::array::get_array_len(#buf, flex)?;
                    let mut v = Vec::with_capacity(n);
                    for _ in 0..n {
                        v.push(#path::decode(#buf, version)?);
                    }
                    v
                })
            };
        }
        let inner = owned_decode_call(elem, false, res_map, buf, lenient_records);
        return if nullable {
            quote!({
                let opt = crate::primitives::array::get_nullable_array_len(#buf, flex)?;
                match opt {
                    None => None,
                    Some(n) => {
                        let mut v = Vec::with_capacity(n);
                        for _ in 0..n {
                            v.push(#inner);
                        }
                        Some(v)
                    }
                }
            })
        } else {
            quote!({
                let n = crate::primitives::array::get_array_len(#buf, flex)?;
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(#inner);
                }
                v
            })
        };
    }

    if owned::is_struct_type(schema_type) {
        let path = res_map
            .get(schema_type)
            .map_or_else(|| path_tokens(schema_type), |r| path_tokens(&r.rust_path));
        return if nullable {
            quote!(if get_i8(#buf)? < 0 { None } else { Some(#path::decode(#buf, version)?) })
        } else {
            quote!(#path::decode(#buf, version)?)
        };
    }

    match (schema_type, nullable) {
        ("int8", _) => quote!(get_i8(#buf)?),
        ("int16", _) => quote!(get_i16(#buf)?),
        ("uint16", _) => quote!(get_u16(#buf)?),
        ("int32", _) => quote!(get_i32(#buf)?),
        ("int64", _) => quote!(get_i64(#buf)?),
        ("bool", _) => quote!(get_bool(#buf)?),
        ("float64", _) => quote!(get_f64(#buf)?),
        ("uuid", _) => quote!(crate::primitives::uuid::get_uuid(#buf)?),
        ("string", false) => {
            quote!(if flex { get_compact_string_owned(#buf)? } else { get_string_owned(#buf)? })
        }
        ("string", true) => {
            quote!(if flex { get_compact_nullable_string_owned(#buf)? } else { get_nullable_string_owned(#buf)? })
        }
        ("bytes", false) => {
            quote!(if flex { get_compact_bytes_owned(#buf)? } else { get_bytes_owned(#buf)? })
        }
        ("bytes", true) => {
            quote!(if flex { get_compact_nullable_bytes_owned(#buf)? } else { get_nullable_bytes_owned(#buf)? })
        }
        ("records", false) if lenient_records => quote!({
            let __rb_bytes = if flex { get_compact_bytes_owned(#buf)? } else { get_bytes_owned(#buf)? };
            let mut __rb_cur: &[u8] = &__rb_bytes;
            crate::records::RecordsPayload::decode_lenient(&mut __rb_cur, version)?
        }),
        ("records", false) => quote!({
            let __rb_bytes = if flex { get_compact_bytes_owned(#buf)? } else { get_bytes_owned(#buf)? };
            let mut __rb_cur: &[u8] = &__rb_bytes;
            <crate::records::RecordsPayload as crate::Decode>::decode(&mut __rb_cur, version)?
        }),
        ("records", true) if lenient_records => quote!({
            let __rb_opt = if flex { get_compact_nullable_bytes_owned(#buf)? } else { get_nullable_bytes_owned(#buf)? };
            match __rb_opt {
                None => None,
                Some(__rb_bytes) => {
                    let mut __rb_cur: &[u8] = &__rb_bytes;
                    Some(crate::records::RecordsPayload::decode_lenient(&mut __rb_cur, version)?)
                }
            }
        }),
        ("records", true) => quote!({
            let __rb_opt = if flex { get_compact_nullable_bytes_owned(#buf)? } else { get_nullable_bytes_owned(#buf)? };
            match __rb_opt {
                None => None,
                Some(__rb_bytes) => {
                    let mut __rb_cur: &[u8] = &__rb_bytes;
                    Some(<crate::records::RecordsPayload as crate::Decode>::decode(&mut __rb_cur, version)?)
                }
            }
        }),
        (t, _) => {
            let msg = format!("unhandled type in decode_call: {t}");
            quote!(compile_error!(#msg))
        }
    }
}

pub(crate) fn wrap_non_nullable_for_option_tokens(non_nullable_call: TokenStream) -> TokenStream {
    quote!(Some(#non_nullable_call))
}

pub(crate) fn borrowed_decode_borrow_call(
    schema_type: &str,
    nullable: bool,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = borrowed::base_type(elem);
        if borrowed::is_struct_type(elem_base) {
            let path = res_map
                .get(elem_base)
                .map_or_else(|| path_tokens(elem_base), |r| path_tokens(&r.rust_path));
            return if nullable {
                quote!({
                    let opt = crate::primitives::array::get_nullable_array_len(#buf, flex)?;
                    match opt {
                        None => None,
                        Some(n) => {
                            let mut v = Vec::with_capacity(n);
                            for _ in 0..n {
                                v.push(#path::decode_borrow(#buf, version)?);
                            }
                            Some(v)
                        }
                    }
                })
            } else {
                quote!({
                    let n = crate::primitives::array::get_array_len(#buf, flex)?;
                    let mut v = Vec::with_capacity(n);
                    for _ in 0..n {
                        v.push(#path::decode_borrow(#buf, version)?);
                    }
                    v
                })
            };
        }
        let inner = borrowed_decode_borrow_call(elem, false, res_map, buf);
        return if nullable {
            quote!({
                let opt = crate::primitives::array::get_nullable_array_len(#buf, flex)?;
                match opt {
                    None => None,
                    Some(n) => {
                        let mut v = Vec::with_capacity(n);
                        for _ in 0..n {
                            v.push(#inner);
                        }
                        Some(v)
                    }
                }
            })
        } else {
            quote!({
                let n = crate::primitives::array::get_array_len(#buf, flex)?;
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(#inner);
                }
                v
            })
        };
    }

    if borrowed::is_struct_type(schema_type) {
        let path = res_map
            .get(schema_type)
            .map_or_else(|| path_tokens(schema_type), |r| path_tokens(&r.rust_path));
        return if nullable {
            quote!(if get_i8(#buf)? < 0 { None } else { Some(#path::decode_borrow(#buf, version)?) })
        } else {
            quote!(#path::decode_borrow(#buf, version)?)
        };
    }

    match (schema_type, nullable) {
        ("int8", _) => quote!(get_i8(#buf)?),
        ("int16", _) => quote!(get_i16(#buf)?),
        ("uint16", _) => quote!(get_u16(#buf)?),
        ("int32", _) => quote!(get_i32(#buf)?),
        ("int64", _) => quote!(get_i64(#buf)?),
        ("bool", _) => quote!(get_bool(#buf)?),
        ("float64", _) => quote!(get_f64(#buf)?),
        ("uuid", _) => quote!(crate::primitives::uuid::get_uuid(#buf)?),
        ("string", false) => {
            quote!(if flex { get_compact_string_borrowed(#buf)? } else { get_string_borrowed(#buf)? })
        }
        ("string", true) => {
            quote!(if flex { get_compact_nullable_string_borrowed(#buf)? } else { get_nullable_string_borrowed(#buf)? })
        }
        ("bytes", false) => {
            quote!(if flex { get_compact_bytes_borrowed(#buf)? } else { get_bytes_borrowed(#buf)? })
        }
        ("bytes", true) => {
            quote!(if flex { get_compact_nullable_bytes_borrowed(#buf)? } else { get_nullable_bytes_borrowed(#buf)? })
        }
        ("records", false) => quote!({
            let __rb_slice = if flex { get_compact_bytes_borrowed(#buf)? } else { get_bytes_borrowed(#buf)? };
            let mut __rb_cur = __rb_slice;
            <crate::records::RecordsPayloadBorrowed as crate::DecodeBorrow>::decode_borrow(&mut __rb_cur, version)?
        }),
        ("records", true) => quote!({
            let __rb_opt = if flex { get_compact_nullable_bytes_borrowed(#buf)? } else { get_nullable_bytes_borrowed(#buf)? };
            match __rb_opt {
                None => None,
                Some(__rb_slice) => {
                    let mut __rb_cur = __rb_slice;
                    Some(<crate::records::RecordsPayloadBorrowed as crate::DecodeBorrow>::decode_borrow(&mut __rb_cur, version)?)
                }
            }
        }),
        (t, _) => {
            let msg = format!("unhandled type in decode_borrow_call: {t}");
            quote!(compile_error!(#msg))
        }
    }
}

pub(crate) fn borrowed_decode_owned_call(
    schema_type: &str,
    nullable: bool,
    parent_module: &str,
    res_map: &ResMap,
    buf: &proc_macro2::Ident,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = borrowed::base_type(elem);
        if borrowed::is_struct_type(elem_base) {
            let path = resolved_to_owned_path_tokens(elem_base, parent_module, res_map);
            return if nullable {
                quote!({
                    let opt = crate::primitives::array::get_nullable_array_len(#buf, flex)?;
                    match opt {
                        None => None,
                        Some(n) => {
                            let mut v = Vec::with_capacity(n);
                            for _ in 0..n {
                                v.push(#path::decode(#buf, version)?);
                            }
                            Some(v)
                        }
                    }
                })
            } else {
                quote!({
                    let n = crate::primitives::array::get_array_len(#buf, flex)?;
                    let mut v = Vec::with_capacity(n);
                    for _ in 0..n {
                        v.push(#path::decode(#buf, version)?);
                    }
                    v
                })
            };
        }
        let inner = borrowed_decode_owned_call(elem, false, parent_module, res_map, buf);
        return if nullable {
            quote!({
                let opt = crate::primitives::array::get_nullable_array_len(#buf, flex)?;
                match opt {
                    None => None,
                    Some(n) => {
                        let mut v = Vec::with_capacity(n);
                        for _ in 0..n {
                            v.push(#inner);
                        }
                        Some(v)
                    }
                }
            })
        } else {
            quote!({
                let n = crate::primitives::array::get_array_len(#buf, flex)?;
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(#inner);
                }
                v
            })
        };
    }

    if borrowed::is_struct_type(schema_type) {
        let path = resolved_to_owned_path_tokens(schema_type, parent_module, res_map);
        return if nullable {
            quote!(Some(#path::decode(#buf, version)?))
        } else {
            quote!(#path::decode(#buf, version)?)
        };
    }

    match (schema_type, nullable) {
        ("int8", _) => quote!(get_i8(#buf)?),
        ("int16", _) => quote!(get_i16(#buf)?),
        ("uint16", _) => quote!(get_u16(#buf)?),
        ("int32", _) => quote!(get_i32(#buf)?),
        ("int64", _) => quote!(get_i64(#buf)?),
        ("bool", _) => quote!(get_bool(#buf)?),
        ("float64", _) => quote!(get_f64(#buf)?),
        ("uuid", _) => quote!(crate::primitives::uuid::get_uuid(#buf)?),
        ("string", false) => quote!(
            if flex { crate::primitives::string_bytes::get_compact_string_owned(#buf)? } else { crate::primitives::string_bytes::get_string_owned(#buf)? }
        ),
        ("string", true) => quote!(
            if flex { crate::primitives::string_bytes::get_compact_nullable_string_owned(#buf)? } else { crate::primitives::string_bytes::get_nullable_string_owned(#buf)? }
        ),
        ("bytes", false) => quote!(
            if flex { crate::primitives::string_bytes::get_compact_bytes_owned(#buf)? } else { crate::primitives::string_bytes::get_bytes_owned(#buf)? }
        ),
        ("bytes", true) => quote!(
            if flex { crate::primitives::string_bytes::get_compact_nullable_bytes_owned(#buf)? } else { crate::primitives::string_bytes::get_nullable_bytes_owned(#buf)? }
        ),
        (t, _) => {
            let msg = format!("unhandled type in decode_owned_call: {t}");
            quote!(compile_error!(#msg))
        }
    }
}

fn resolved_to_owned_path_tokens(
    type_name: &str,
    parent_module: &str,
    res_map: &ResMap,
) -> TokenStream {
    let path = match res_map.get(type_name) {
        Some(r) if r.kind == StructKind::Common => {
            if let Some(without_super) = r.rust_path.strip_prefix("super::common::") {
                format!("crate::owned::common::{without_super}")
            } else if let Some(sibling) = r.rust_path.strip_prefix("super::") {
                let msg_seg = parent_module
                    .strip_prefix("common::")
                    .and_then(|rest| rest.split("::").next())
                    .unwrap_or(parent_module);
                format!("crate::owned::common::{msg_seg}::{sibling}")
            } else {
                format!("crate::owned::{parent_module}::{type_name}")
            }
        }
        _ => format!("crate::owned::{parent_module}::{type_name}"),
    };
    path_tokens(&path)
}

pub(crate) fn borrowed_to_owned_field_tokens(
    schema_type: &str,
    expr: TokenStream,
    nullable: bool,
    _res_map: &ResMap,
) -> TokenStream {
    if let Some(elem) = schema_type.strip_prefix("[]") {
        let elem_base = borrowed::base_type(elem);
        if borrowed::is_struct_type(elem_base) {
            return if nullable {
                quote!((#expr).as_ref().map(|v| v.iter().map(|it| it.to_owned()).collect()))
            } else {
                quote!((#expr).iter().map(|it| it.to_owned()).collect())
            };
        }
        return match (elem, nullable) {
            ("string", true) => {
                quote!((#expr).as_ref().map(|v| v.iter().map(|s| s.to_string()).collect()))
            }
            ("string", false) => quote!((#expr).iter().map(|s| s.to_string()).collect()),
            ("bytes", true) => quote!((#expr).as_ref().map(|v| {
                v.iter().map(|b| Bytes::copy_from_slice(b)).collect()
            })),
            ("bytes", false) => quote!((#expr).iter().map(|b| Bytes::copy_from_slice(b)).collect()),
            ("records", true) => {
                quote!((#expr).as_ref().map(|v| v.iter().map(|rb| rb.to_owned()).collect()))
            }
            ("records", false) => quote!((#expr).iter().map(|rb| rb.to_owned()).collect()),
            _ => quote!((#expr).clone()),
        };
    }

    if borrowed::is_struct_type(schema_type) {
        return if nullable {
            quote!((#expr).as_ref().map(|v| v.to_owned()))
        } else {
            quote!((#expr).to_owned())
        };
    }

    match (schema_type, nullable) {
        ("string", false) => quote!((#expr).to_string()),
        ("string", true) => quote!((#expr).map(|s| s.to_string())),
        ("bytes", false) => quote!(Bytes::copy_from_slice(#expr)),
        ("bytes", true) => quote!((#expr).map(Bytes::copy_from_slice)),
        ("records", false) => quote!((#expr).to_owned().expect("records to_owned")),
        ("records", true) => {
            quote!((#expr).as_ref().map(|rb| rb.to_owned().expect("records to_owned")))
        }
        _ => quote!((#expr)),
    }
}
