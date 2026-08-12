//! A `quote!`-based reimplementation of the `owned` emitter.
//!
//! This module builds the structural scaffolding as a
//! `proc_macro2::TokenStream` instead of a concatenation with `write!`. That
//! scaffolding is the struct definition, `Default`, the `Encode` and `Decode`
//! impls, tagged-field framing, nested structs, and the `populated`
//! constructor. `emit` returns the raw token text, plus a banner and the
//! reused `default_json` and `ProtocolRequest` tails. The rustfmt pass in
//! `crate::fmt`, which the regeneration binary runs, formats it.
//!
//! *Reuse* guarantees wire correctness. The leaf codec expressions
//! (`encode_call`, `decode_call`, `encoded_len_expr`, and their variants), the
//! import and constant blocks, the `default_json` tail, and the common-struct
//! files all come verbatim from `super::owned`, whose bytes the protocol
//! crate's round-trip and JVM-differential tests already cover. This module
//! rebuilds only the *shape* around those leaves, which is where the `write!`
//! templating was densest. `parse_expr` parses each leaf string into tokens.

use std::str::FromStr;

use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote};

use crate::{
    emit::{
        EmittedMessage,
        common::banner,
        default_json,
        owned::{
            self, EmitError, base_type, decode_call, decode_call_with_buf, emit_common_imports,
            encode_call, encode_call_option_as_non_nullable, encode_call_with_buf,
            encoded_len_expr, field_forces_non_flex, flex_min, has_any_flex, has_float64_recursive,
            is_nullable, is_tagged, needs_manual_default, nullable_split_cond, owned_default_expr,
            owned_populated_value, struct_path_for, tagged_is_default_cond, version_cond,
            wrap_non_nullable_for_option,
        },
        protocol_request,
    },
    ir::{FieldSpec, MessageSpec, MessageType},
    name_conv,
    resolve::{self, Resolution, StructKind},
    type_map,
};

type ResMap = std::collections::HashMap<String, Resolution>;

/// Parse an emitter-produced Rust fragment into tokens. The fragment came from
/// a trusted generator, so a lex error is a bug and not bad input.
fn parse_expr(s: &str) -> TokenStream {
    TokenStream::from_str(s).expect("leaf generator produced an unlexable fragment")
}

fn tagged_should_encode(f: &FieldSpec) -> TokenStream {
    if base_type(&f.field_type) == "bool" {
        let field = format_ident!("{}", name_conv::field_name(&f.name));
        return match &f.default {
            Some(serde_json::Value::Bool(true)) => {
                quote!(!self.#field)
            }
            Some(serde_json::Value::String(value)) if value == "true" => {
                quote!(!self.#field)
            }
            Some(serde_json::Value::Bool(false)) => {
                quote!(self.#field)
            }
            Some(serde_json::Value::String(value)) if value == "false" => quote!(self.#field),
            _ => tagged_should_encode_from_default(f),
        };
    }
    tagged_should_encode_from_default(f)
}

fn tagged_should_encode_from_default(f: &FieldSpec) -> TokenStream {
    let default = tagged_is_default_cond(f);
    if let Some(option) = default.strip_suffix(".is_none()") {
        return parse_expr(&format!("{option}.is_some()"));
    }
    if let Some((value, expected)) = default.split_once(" == ") {
        return parse_expr(&format!("{value} != {expected}"));
    }
    if let Some(positive) = default.strip_prefix('!') {
        parse_expr(positive)
    } else {
        let default = parse_expr(&default);
        quote!(!(#default))
    }
}

/// `is_flexible(version)` for top-level messages, and `version >= N` for
/// nested structs whose version flows in from a parent. `N` is the message's
/// flexible threshold.
enum FlexSource {
    TopLevel,
    Nested(i16),
}

impl FlexSource {
    fn tokens(&self) -> TokenStream {
        match self {
            FlexSource::TopLevel => quote!(is_flexible(version)),
            FlexSource::Nested(i16::MIN) => quote!(true),
            FlexSource::Nested(i16::MAX) => quote!(version == i16::MAX),
            FlexSource::Nested(fm) => {
                let n = Literal::i16_unsuffixed(*fm);
                quote!(version >= #n)
            }
        }
    }
}

/// # Errors
/// Returns an error when the schema model is invalid or generated Rust cannot be formatted or written.
pub fn emit(spec: &MessageSpec, schemas_version: &str) -> Result<EmittedMessage, EmitError> {
    // Resolve struct references — also validates that everything resolves.
    let res_map = resolve::resolve_message(spec)?;
    let lenient = matches!(spec.message_type, MessageType::Response);

    // Top-level struct + impls + populated, then sibling structs for nested types.
    let version_err = version_err(spec);
    let mut structural = struct_block(
        &name_conv::type_name(&spec.name),
        &spec.fields,
        &res_map,
        &FlexSource::TopLevel,
        Some(version_err),
        has_any_flex(spec),
        lenient,
    );
    structural.extend(nested_structs(
        &spec.fields,
        flex_min(spec),
        &res_map,
        lenient,
    ));

    // Build imports and constants as token streams, then render them.
    let imports = owned::emit_imports(spec).to_string();
    let constants = owned::emit_constants(spec).to_string();
    let banner = format!(
        "// AUTO-GENERATED by crabka-protocol-codegen against {schemas_version}. Do not edit.\n\n"
    );
    let dj = default_json::emit_default_json(spec);
    let pr = protocol_request::emit_protocol_request(spec).unwrap_or_default();

    let primary = format!("{banner}{imports}{constants}\n{structural}\n{dj}{pr}");

    // Validate at generation time; rustfmt (the caller's secondary step) also
    // re-checks, but a syn error here is more precise.
    syn::parse_str::<syn::File>(&primary).map_err(|e| {
        EmitError::Unsupported(format!(
            "owned_quote produced invalid Rust for {}: {e}",
            spec.name
        ))
    })?;

    let commons = emit_commons(spec, &res_map, schemas_version, lenient);

    Ok(EmittedMessage { primary, commons })
}

/// Emit the standalone `.rs` file bodies for the message's `commonStructs`.
///
/// `commonStructs` are message-local. The emitter writes each one under a
/// per-message nested module `common/<message_snake>/<struct_snake>`. The
/// `commons` key is the relative path stem `<message_snake>/<struct_snake>`.
/// The caller turns that stem into the on-disk body path and the wrapper
/// module nesting.
fn emit_commons(
    spec: &MessageSpec,
    res_map: &ResMap,
    schemas_version: &str,
    lenient: bool,
) -> Vec<(String, String)> {
    let message_snake = name_conv::module_name(&spec.name);
    let fm = flex_min(spec);
    let mut commons: Vec<(String, String)> = Vec::new();
    for cs in &spec.common_structs {
        // Build a modified res_map for the common-struct context: common-struct
        // references use sibling paths `super::<struct_snake>::TypeName` (the body
        // lands in `src/{flavor}/common/<message_snake>/<struct_snake>.rs`, and
        // sibling common structs of the same message share that parent module).
        let common_res_map: ResMap = res_map
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

        let name = name_conv::type_name(&cs.name);
        let struct_tokens = struct_block(
            &name,
            &cs.fields,
            &common_res_map,
            &FlexSource::Nested(fm),
            None,
            fm < i16::MAX,
            lenient,
        );
        let nested = nested_structs(&cs.fields, fm, &common_res_map, lenient);

        let body = format!(
            "{}{}{}{}",
            banner(schemas_version),
            emit_common_imports(&cs.fields, fm),
            struct_tokens,
            nested,
        );

        let cs_snake = name_conv::module_name(&cs.name);
        commons.push((format!("{message_snake}/{cs_snake}"), body));
    }
    commons
}

fn version_err(spec: &MessageSpec) -> TokenStream {
    match spec.message_type {
        MessageType::Request | MessageType::Response => {
            quote!(return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });)
        }
        MessageType::Header | MessageType::Data => {
            let msg = format!("{} version out of range", spec.name);
            quote!(return Err(ProtocolError::SchemaMismatch(#msg));)
        }
    }
}

/// A struct definition, its `Default` when that is non-trivial, the `Encode`
/// and `Decode` impls, and the test `populated` constructor. The top-level
/// message type and every nested struct share this function, and they differ
/// only in flex source and guard.
fn struct_block(
    name: &str,
    fields: &[FieldSpec],
    res_map: &ResMap,
    flex: &FlexSource,
    guard: Option<TokenStream>,
    has_flex: bool,
    lenient: bool,
) -> TokenStream {
    let ty = format_ident!("{name}");
    let eq = if has_float64_recursive(fields) {
        quote!()
    } else {
        quote!(, Eq)
    };
    let manual_default = needs_manual_default(fields);
    let default_derive = if manual_default {
        quote!()
    } else {
        quote!(, Default)
    };

    let field_defs = ordered(fields).map(|f| {
        let name = format_ident!("{}", name_conv::field_name(&f.name));
        let nullable = is_nullable(f) || (is_tagged(f) && default_is_null(f));
        let ty = parse_expr(&type_map::owned_type(
            &f.field_type,
            nullable,
            struct_path_for(f, res_map).as_deref(),
        ));
        quote!(pub #name: #ty,)
    });

    let default_impl = manual_default.then(|| {
        let assigns = ordered(fields).map(|f| {
            let name = format_ident!("{}", name_conv::field_name(&f.name));
            let expr = parse_expr(&owned_default_expr(f, res_map));
            quote!(#name: #expr,)
        });
        quote! {
            impl Default for #ty {
                fn default() -> Self {
                    Self {
                        #(#assigns)*
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }
                }
            }
        }
    });

    let flex_src = flex.tokens();
    let guard = guard.map(|err| {
        quote! {
            if !(MIN_VERSION..=MAX_VERSION).contains(&version) { #err }
        }
    });

    let encode_body = encode_body(fields, has_flex);
    let len_body = len_body(fields, has_flex);
    let decode_body = decode_body(fields, res_map, has_flex, lenient);
    let (codec_helpers, encode_body, decode_body) = if fields.len() >= 8 {
        let (encode_helpers, encode_calls) = split_encode_body(fields, has_flex);
        let (decode_helpers, decode_calls) = split_decode_body(fields, res_map, has_flex, lenient);
        (
            quote!(impl #ty { #encode_helpers #decode_helpers }),
            encode_calls,
            decode_calls,
        )
    } else {
        (quote!(), encode_body, decode_body)
    };
    let encode_flex = flex_binding(&encode_body, &flex_src);
    let len_flex = flex_binding(&len_body, &flex_src);
    let decode_flex = flex_binding(&decode_body, &flex_src);
    let populated = populated_impl(&ty, fields, res_map);

    quote! {
        #[derive(Debug, Clone, PartialEq #eq #default_derive)]
        pub struct #ty {
            #(#field_defs)*
            pub unknown_tagged_fields: UnknownTaggedFields,
        }

        #default_impl

        #codec_helpers

        impl Encode for #ty {
            fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
                #guard
                #encode_flex
                #encode_body
                Ok(())
            }
            fn encoded_len(&self, version: i16) -> usize {
                #len_flex
                let mut n: usize = 0;
                #len_body
                n
            }
        }

        impl Decode<'_> for #ty {
            fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
                #guard
                #decode_flex
                let mut out = Self::default();
                #decode_body
                Ok(out)
            }
        }

        #populated
    }
}

fn flex_binding(body: &TokenStream, flex_source: &TokenStream) -> Option<TokenStream> {
    body.to_string()
        .contains("flex")
        .then(|| quote!(let flex = #flex_source;))
}

/// Fields in emission order: non-tagged first, then tagged. This matches the
/// string emitter, so struct layout and round-trips are identical.
fn ordered(fields: &[FieldSpec]) -> impl Iterator<Item = &FieldSpec> {
    fields
        .iter()
        .filter(|f| !is_tagged(f))
        .chain(fields.iter().filter(|f| is_tagged(f)))
}

fn default_is_null(f: &FieldSpec) -> bool {
    matches!(&f.default, Some(serde_json::Value::Null))
}

// --- encode -----------------------------------------------------------------

fn encode_body(fields: &[FieldSpec], has_flex: bool) -> TokenStream {
    let stmts = fields.iter().filter(|f| !is_tagged(f)).map(encode_one);
    let trailer = has_flex.then(|| {
        let has_tagged = fields.iter().any(is_tagged);
        let mut_kw = if has_tagged { quote!(mut) } else { quote!() };
        let adds = fields.iter().filter(|f| is_tagged(f)).map(encode_tagged);
        quote! {
            if flex {
                let #mut_kw tagged = WriteTaggedFields::new();
                #(#adds)*
                tagged.write(buf, &self.unknown_tagged_fields);
            }
        }
    });
    quote!(#(#stmts)* #trailer)
}

fn split_encode_body(fields: &[FieldSpec], has_flex: bool) -> (TokenStream, TokenStream) {
    let mut helpers = Vec::new();
    let mut calls = Vec::new();
    for (index, field) in fields.iter().filter(|field| !is_tagged(field)).enumerate() {
        let helper = format_ident!("encode_field_{index}");
        let body = encode_one(field);
        let flex = if body.to_string().contains("flex") {
            quote!(flex)
        } else {
            quote!(_flex)
        };
        if body.to_string().contains('?') {
            helpers.push(quote! {
                fn #helper<B: BufMut>(
                    &self,
                    buf: &mut B,
                    version: i16,
                    #flex: bool,
                ) -> Result<(), ProtocolError> {
                    #body
                    Ok(())
                }
            });
            calls.push(quote!(self.#helper(buf, version, flex)?;));
        } else {
            helpers.push(quote! {
                fn #helper<B: BufMut>(&self, buf: &mut B, version: i16, #flex: bool) {
                    #body;
                }
            });
            calls.push(quote!(self.#helper(buf, version, flex);));
        }
    }
    if has_flex {
        let helper = format_ident!("encode_tagged_fields");
        let has_tagged = fields.iter().any(is_tagged);
        let mut_kw = if has_tagged { quote!(mut) } else { quote!() };
        let adds = fields
            .iter()
            .filter(|field| is_tagged(field))
            .map(encode_tagged);
        let body = quote! {
            if flex {
                let #mut_kw tagged = WriteTaggedFields::new();
                #(#adds)*
                tagged.write(buf, &self.unknown_tagged_fields);
            }
        };
        let version = if body.to_string().contains("version") {
            quote!(version)
        } else {
            quote!(_version)
        };
        helpers.push(quote! {
            fn #helper<B: BufMut>(
                &self,
                buf: &mut B,
                #version: i16,
                flex: bool,
            ) {
                #body
            }
        });
        calls.push(quote!(self.#helper(buf, version, flex);));
    }
    (quote!(#(#helpers)*), quote!(#(#calls)*))
}

fn encode_one(f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let cond = parse_expr(&version_cond(f.versions, "version"));
    let expr = format!("self.{field}");
    let inner = if let Some(ncond) = nullable_split_cond(f) {
        let nb = parse_expr(&encode_call(&f.field_type, &expr, true));
        let nnb = parse_expr(&encode_call_option_as_non_nullable(&f.field_type, &expr));
        let nc = parse_expr(&ncond);
        non_flex_wrap(f, quote!(if #nc { #nb } else { #nnb }))
    } else {
        let b = parse_expr(&encode_call(&f.field_type, &expr, is_nullable(f)));
        non_flex_wrap(f, b)
    };
    quote!(if #cond { #inner; })
}

/// `flexibleVersions: "none"` on a field forces the legacy codec. This
/// function shadows `flex` to `false` for that field's body.
fn non_flex_wrap(f: &FieldSpec, body: TokenStream) -> TokenStream {
    if field_forces_non_flex(f) {
        quote!({ let flex = false; #body })
    } else {
        body
    }
}

fn encode_tagged(f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let tag = Literal::u32_unsuffixed(f.tag.expect("tagged field has tag"));
    let nullable = is_nullable(f) || default_is_null(f);
    let expr = format!("self.{field}");
    let body = parse_expr(&encode_call_with_buf(&f.field_type, &expr, nullable, "b"));
    let len = parse_expr(&encoded_len_expr(&f.field_type, &expr, nullable));
    let should_encode = tagged_should_encode(f);
    quote! {
        if #should_encode {
            let payload = encode_to_bytes(#len, |b| { #body; Ok(()) });
            tagged.add(#tag, payload);
        }
    }
}

// --- encoded_len ------------------------------------------------------------

fn len_body(fields: &[FieldSpec], has_flex: bool) -> TokenStream {
    let stmts = fields.iter().filter(|f| !is_tagged(f)).map(len_one);
    let trailer = has_flex.then(|| {
        let has_tagged = fields.iter().any(is_tagged);
        let mut_kw = if has_tagged { quote!(mut) } else { quote!() };
        let pushes = fields.iter().filter(|f| is_tagged(f)).map(len_tagged);
        quote! {
            if flex {
                let #mut_kw known_pairs: Vec<(u32, usize)> = Vec::new();
                #(#pushes)*
                n += tagged_fields_len(&known_pairs, &self.unknown_tagged_fields);
            }
        }
    });
    quote!(#(#stmts)* #trailer)
}

fn len_one(f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let cond = parse_expr(&version_cond(f.versions, "version"));
    let expr = format!("self.{field}");
    let inner = if let Some(ncond) = nullable_split_cond(f) {
        let nb = parse_expr(&encoded_len_expr(&f.field_type, &expr, true));
        let nnb = parse_expr(&owned::encoded_len_expr_option_as_non_nullable(
            &f.field_type,
            &expr,
        ));
        let nc = parse_expr(&ncond);
        non_flex_wrap(f, quote!(if #nc { #nb } else { #nnb }))
    } else {
        let b = parse_expr(&encoded_len_expr(&f.field_type, &expr, is_nullable(f)));
        non_flex_wrap(f, b)
    };
    quote!(if #cond { n += #inner; })
}

fn len_tagged(f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let tag = Literal::u32_unsuffixed(f.tag.expect("tagged field has tag"));
    let nullable = is_nullable(f) || default_is_null(f);
    let len = parse_expr(&encoded_len_expr(
        &f.field_type,
        &format!("self.{field}"),
        nullable,
    ));
    let should_encode = tagged_should_encode(f);
    quote! {
        if #should_encode {
            known_pairs.push((#tag, #len));
        }
    }
}

// --- decode -----------------------------------------------------------------

fn decode_body(
    fields: &[FieldSpec],
    res_map: &ResMap,
    has_flex: bool,
    lenient: bool,
) -> TokenStream {
    let stmts = fields
        .iter()
        .filter(|f| !is_tagged(f))
        .map(|f| decode_one(f, res_map, lenient));
    let trailer = has_flex.then(|| decode_tagged_block(fields, res_map, lenient));
    quote!(#(#stmts)* #trailer)
}

fn split_decode_body(
    fields: &[FieldSpec],
    res_map: &ResMap,
    has_flex: bool,
    lenient: bool,
) -> (TokenStream, TokenStream) {
    let mut helpers = Vec::new();
    let mut calls = Vec::new();
    for (index, field) in fields.iter().filter(|field| !is_tagged(field)).enumerate() {
        let helper = format_ident!("decode_field_{index}");
        let body = decode_one(field, res_map, lenient);
        let flex = if body.to_string().contains("flex") {
            quote!(flex)
        } else {
            quote!(_flex)
        };
        helpers.push(quote! {
            fn #helper<B: Buf>(
                out: &mut Self,
                buf: &mut B,
                version: i16,
                #flex: bool,
            ) -> Result<(), ProtocolError> {
                #body
                Ok(())
            }
        });
        calls.push(quote!(Self::#helper(&mut out, buf, version, flex)?;));
    }
    if has_flex {
        let helper = format_ident!("decode_tagged_fields");
        let body = decode_tagged_block(fields, res_map, lenient);
        let version = if body.to_string().contains("version") {
            quote!(version)
        } else {
            quote!(_version)
        };
        helpers.push(quote! {
            fn #helper<B: Buf>(
                out: &mut Self,
                buf: &mut B,
                #version: i16,
                flex: bool,
            ) -> Result<(), ProtocolError> {
                #body
                Ok(())
            }
        });
        calls.push(quote!(Self::#helper(&mut out, buf, version, flex)?;));
    }
    (quote!(#(#helpers)*), quote!(#(#calls)*))
}

fn decode_one(f: &FieldSpec, res_map: &ResMap, lenient: bool) -> TokenStream {
    let field = format_ident!("{}", name_conv::field_name(&f.name));
    let cond = parse_expr(&version_cond(f.versions, "version"));
    let inner = if let Some(ncond) = nullable_split_cond(f) {
        let nb = parse_expr(&decode_call(&f.field_type, true, res_map, lenient));
        let nnb_call = decode_call(&f.field_type, false, res_map, lenient);
        let nnb = parse_expr(&wrap_non_nullable_for_option(
            &f.field_type,
            &nnb_call,
            res_map,
        ));
        let nc = parse_expr(&ncond);
        non_flex_wrap(f, quote!(if #nc { #nb } else { #nnb }))
    } else {
        let b = parse_expr(&decode_call(
            &f.field_type,
            is_nullable(f),
            res_map,
            lenient,
        ));
        non_flex_wrap(f, b)
    };
    quote!(if #cond { out.#field = #inner; })
}

fn decode_tagged_block(fields: &[FieldSpec], res_map: &ResMap, lenient: bool) -> TokenStream {
    let has_tagged = fields.iter().any(is_tagged);
    if !has_tagged {
        return quote! {
            if flex {
                out.unknown_tagged_fields = read_tagged_fields(buf, |_tag, _payload| { Ok(false) })?;
            }
        };
    }
    let slots = fields.iter().filter(|f| is_tagged(f)).map(|f| {
        let slot = format_ident!("tag_{}", name_conv::field_name(&f.name));
        quote!(let mut #slot = None;)
    });
    let arms = fields.iter().filter(|f| is_tagged(f)).map(|f| {
        let slot = format_ident!("tag_{}", name_conv::field_name(&f.name));
        let tag = Literal::u32_unsuffixed(f.tag.expect("tagged field has tag"));
        let nullable = is_nullable(f) || default_is_null(f);
        let call = parse_expr(&decode_call_with_buf(
            &f.field_type,
            nullable,
            res_map,
            "b",
            lenient,
        ));
        quote!(#tag => { #slot = Some({ let b: &mut &[u8] = payload; #call }); Ok(true) })
    });
    let writebacks = fields.iter().filter(|f| is_tagged(f)).map(|f| {
        let field = format_ident!("{}", name_conv::field_name(&f.name));
        let slot = format_ident!("tag_{}", name_conv::field_name(&f.name));
        quote!(if let Some(v) = #slot { out.#field = v; })
    });
    quote! {
        if flex {
            #(#slots)*
            out.unknown_tagged_fields = read_tagged_fields(buf, |tag, payload| {
                match tag {
                    #(#arms)*
                    _ => Ok(false),
                }
            })?;
            #(#writebacks)*
        }
    }
}

// --- populated + nested -----------------------------------------------------

fn populated_impl(ty: &proc_macro2::Ident, fields: &[FieldSpec], res_map: &ResMap) -> TokenStream {
    let assigns: Vec<_> = ordered(fields)
        .filter(|f| base_type(&f.field_type) != "records")
        .map(|f| {
            let field = format_ident!("{}", name_conv::field_name(&f.name));
            let option = is_nullable(f) || (is_tagged(f) && default_is_null(f));
            let value = parse_expr(&owned_populated_value(f, res_map, option));
            let cond = parse_expr(&version_cond(f.versions, "version"));
            quote!(if #cond { m.#field = #value; })
        })
        .collect();
    if assigns.is_empty() {
        return quote! {
            #[cfg(test)]
            impl #ty {
                #[must_use]
                pub fn populated(_version: i16) -> Self {
                    Self::default()
                }
            }
        };
    }
    let mut_kw = quote!(mut);
    let version = if assigns
        .iter()
        .any(|assign| assign.to_string().contains("version"))
    {
        quote!(version)
    } else {
        quote!(_version)
    };
    quote! {
        #[cfg(test)]
        impl #ty {
            #[must_use]
            pub fn populated(#version: i16) -> Self {
                let #mut_kw m = Self::default();
                #(#assigns)*
                m
            }
        }
    }
}

/// Sibling struct definitions for every field that has its own `fields:` list,
/// depth-first. `flex_min_val` is the message's flexible threshold, and every
/// nested struct inherits it.
fn nested_structs(
    fields: &[FieldSpec],
    flex_min_val: i16,
    res_map: &ResMap,
    lenient: bool,
) -> TokenStream {
    let mut out = TokenStream::new();
    for f in fields.iter().filter(|f| !f.fields.is_empty()) {
        let name = base_type(&f.field_type);
        out.extend(struct_block(
            name,
            &f.fields,
            res_map,
            &FlexSource::Nested(flex_min_val),
            None,
            flex_min_val < i16::MAX,
            lenient,
        ));
        out.extend(nested_structs(&f.fields, flex_min_val, res_map, lenient));
    }
    out
}
