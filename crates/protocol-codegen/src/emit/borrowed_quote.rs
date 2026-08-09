//! A `quote!`-based reimplementation of the `borrowed` emitter.
//!
//! This is the zero-copy, lifetime-carrying flavor with `&'a str`, `&'a [u8]`,
//! and `RecordsPayloadBorrowed<'a>`, plus a `to_owned()` bridge to the owned
//! types and a `DecodeBorrow<'de>` impl.
//!
//! The approach is the same as [`super::owned_quote`]. This module builds the
//! structural scaffolding as tokens: the struct, `Default`, `to_owned`,
//! `Encode`, `DecodeBorrow`, nested structs, and `populated`. It reuses the
//! leaf codec expressions, the import and constant blocks, and the
//! common-struct files verbatim from `super::borrowed`, whose wire bytes are
//! already test-covered. The `borrowed_quote_parity` test asserts
//! token-stream equality with the string emitter for every schema.

use std::str::FromStr;

use proc_macro2::{Literal, TokenStream};
use quote::{format_ident, quote};

use crate::{
    emit::{
        EmittedMessage,
        borrowed::{
            self, base_type, borrowed_default_expr, borrowed_populated_value, decode_borrow_call,
            decode_owned_call, encode_call, encode_call_option_as_non_nullable, encoded_len_expr,
            encoded_len_expr_option_as_non_nullable, field_forces_non_flex, flex_min, has_any_flex,
            has_float64_recursive, is_nullable, is_struct_type, is_tagged, needs_lifetime,
            nullable_split_cond, owned_struct_path_for, spec_needs_lifetime, struct_path_for,
            tagged_field_needs_owned, tagged_is_default_cond, to_owned_field_expr, version_cond,
        },
        common,
        common::banner,
        owned::EmitError,
    },
    ir::{FieldSpec, MessageSpec, MessageType},
    name_conv,
    resolve::{self, Resolution, StructKind},
    type_map,
};

type ResMap = std::collections::HashMap<String, Resolution>;

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
    if let Some(positive) = default.strip_prefix('!') {
        parse_expr(positive)
    } else {
        let default = parse_expr(&default);
        quote!(!(#default))
    }
}

/// Shared context threaded through every struct block in one message file.
struct Ctx<'a> {
    res_map: &'a ResMap,
    /// The owned module the `to_owned()` targets live in (the message's module).
    parent_module: &'a str,
    /// `crate::owned` or `crate::<ns>::owned`.
    owned_root: &'a str,
}

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
pub fn emit(
    spec: &MessageSpec,
    schemas_version: &str,
    namespace: Option<&str>,
) -> Result<EmittedMessage, EmitError> {
    // Resolve struct references — also validates that everything resolves.
    let res_map = resolve::resolve_message(spec)?;
    let parent_module = name_conv::module_name(&spec.name);
    let owned_root = match namespace {
        None => "crate::owned".to_string(),
        Some(ns) => format!("crate::{ns}::owned"),
    };
    let ctx = Ctx {
        res_map: &res_map,
        parent_module: &parent_module,
        owned_root: &owned_root,
    };

    let mut structural = struct_block(
        &ctx,
        &name_conv::type_name(&spec.name),
        &spec.fields,
        spec_needs_lifetime(spec, &res_map),
        &FlexSource::TopLevel,
        Some(version_err(spec)),
        (true, has_any_flex(spec)),
    );
    structural.extend(nested_structs(&ctx, &spec.fields, flex_min(spec)));

    let imports = borrowed::emit_imports(spec, &res_map).to_string();
    let constants = borrowed::emit_constants(spec).to_string();
    let banner = common::banner(schemas_version);

    let primary = format!("{banner}{imports}{constants}\n{structural}\n");
    syn::parse_str::<syn::File>(&primary).map_err(|e| {
        EmitError::Unsupported(format!(
            "borrowed_quote produced invalid Rust for {}: {e}",
            spec.name
        ))
    })?;

    let commons = emit_commons(spec, &res_map, schemas_version, namespace);

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
    namespace: Option<&str>,
) -> Vec<(String, String)> {
    let message_snake = name_conv::module_name(&spec.name);
    let fm = flex_min(spec);
    let owned_root = match namespace {
        None => "crate::owned".to_string(),
        Some(ns) => format!("crate::{ns}::owned"),
    };
    let mut commons: Vec<(String, String)> = Vec::new();
    for cs in &spec.common_structs {
        // Build a modified res_map for the common-struct context:
        // - Common-struct references use sibling paths `super::<struct_snake>::TypeName`
        //   (the body lands under `src/{flavor}/common/<message_snake>/<struct_snake>.rs`,
        //   and sibling common structs of the same message share that parent module).
        // - Nested struct references remain unchanged (bare type name, same file).
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

        let cs_snake = name_conv::module_name(&cs.name);
        // Use `common::<message_snake>::<struct_snake>` as the parent_module so
        // `to_owned()` targets `crate::owned::common::<message_snake>::<struct_snake>::TypeName`.
        let cs_parent_module = format!("common::{message_snake}::{cs_snake}");
        let ctx = Ctx {
            res_map: &common_res_map,
            parent_module: &cs_parent_module,
            owned_root: &owned_root,
        };

        let name = name_conv::type_name(&cs.name);
        let has_lt = needs_lifetime(&cs.fields, &common_res_map);
        let mut struct_tokens = struct_block(
            &ctx,
            &name,
            &cs.fields,
            has_lt,
            &FlexSource::Nested(fm),
            None,
            (false, fm < i16::MAX),
        );
        struct_tokens.extend(nested_structs(&ctx, &cs.fields, fm));

        let body = format!(
            "{}{}{}",
            banner(schemas_version),
            borrowed::emit_common_imports(&cs.fields, fm),
            struct_tokens,
        );

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

/// Struct + `Default` + `to_owned` + `Encode` + `DecodeBorrow` + `populated`.
/// `top_level` toggles the tagged-field-owned typing that only the message
/// type uses. Nested structs always store tagged fields in their borrowed
/// form.
fn struct_block(
    ctx: &Ctx,
    name: &str,
    fields: &[FieldSpec],
    has_lt: bool,
    flex: &FlexSource,
    guard: Option<TokenStream>,
    options: (bool, bool),
) -> TokenStream {
    let (top_level, has_flex) = options;
    let ty = format_ident!("{name}");
    // `<'a>` on the value type / impls, `<'de>` on the Self type in DecodeBorrow.
    let gen_a = if has_lt { quote!(<'a>) } else { quote!() };
    let gen_de = if has_lt { quote!(<'de>) } else { quote!() };
    let impl_a = if has_lt {
        quote!(impl<'a>)
    } else {
        quote!(impl)
    };
    let ty_a = quote!(#ty #gen_a);
    let ty_de = quote!(#ty #gen_de);

    let eq = if has_float64_recursive(fields) {
        quote!()
    } else {
        quote!(, Eq)
    };

    let (field_defs, default_assigns, owned_target, to_owned_assigns) =
        struct_members(ctx, name, fields, top_level);

    let guard =
        guard.map(|err| quote!(if !(MIN_VERSION..=MAX_VERSION).contains(&version) { #err }));
    let flex_src = flex.tokens();
    let encode_body = encode_body(ctx, fields, has_flex);
    let len_body = len_body(ctx, fields, has_flex);
    let decode_body = decode_body(ctx, fields, has_flex);
    let (codec_helpers, encode_body, decode_body) = if fields.len() >= 8 {
        let (encode_helpers, encode_calls) = split_encode_body(ctx, fields, has_flex);
        let (decode_helpers, decode_calls) = split_decode_body(ctx, fields, has_flex, has_lt);
        (
            quote!(#encode_helpers #decode_helpers),
            encode_calls,
            decode_calls,
        )
    } else {
        (quote!(), encode_body, decode_body)
    };
    let encode_flex = flex_binding(&encode_body, &flex_src);
    let len_flex = flex_binding(&len_body, &flex_src);
    let decode_flex = flex_binding(&decode_body, &flex_src);
    let populated_target = if has_lt {
        quote!(impl #ty<'_>)
    } else {
        quote!(impl #ty)
    };
    let populated = populated_impl(ctx, &populated_target, fields);

    quote! {
        #[derive(Debug, Clone, PartialEq #eq)]
        pub struct #ty_a {
            #field_defs
            pub unknown_tagged_fields: UnknownTaggedFields,
        }

        #impl_a Default for #ty_a {
            fn default() -> Self {
                Self {
                    #default_assigns
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
            }
        }

        #impl_a #ty_a {
            /// # Panics
            ///
            /// Panics if a records field contains an invalid encoded record batch.
            pub fn to_owned(&self) -> #owned_target {
                #owned_target {
                    #to_owned_assigns
                    unknown_tagged_fields: self.unknown_tagged_fields.clone(),
                }
            }

            #codec_helpers
        }

        #impl_a Encode for #ty_a {
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

        impl<'de> DecodeBorrow<'de> for #ty_de {
            fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {
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

fn struct_members(
    ctx: &Ctx,
    name: &str,
    fields: &[FieldSpec],
    top_level: bool,
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    let field_defs = ordered(fields).map(|field| {
        let name = format_ident!("{}", name_conv::field_name(&field.name));
        let ty = parse_expr(&field_type(ctx, field, top_level));
        quote!(pub #name: #ty,)
    });
    let default_assigns = ordered(fields).map(|field| {
        let name = format_ident!("{}", name_conv::field_name(&field.name));
        let expr = if is_struct_type(base_type(&field.field_type))
            && !field.field_type.starts_with("[]")
            && field.default.is_none()
        {
            let ty = parse_expr(&field_type(ctx, field, top_level));
            quote!(<#ty>::default())
        } else {
            parse_expr(&borrowed_default_expr(field, ctx.res_map))
        };
        quote!(#name: #expr,)
    });
    let owned_target = parse_expr(&format!(
        "{}::{}::{name}",
        ctx.owned_root, ctx.parent_module
    ));
    let to_owned_assigns = ordered(fields).map(|field| {
        let name = format_ident!("{}", name_conv::field_name(&field.name));
        let expr = parse_expr(&to_owned_expr(ctx, field, top_level));
        quote!(#name: #expr,)
    });
    (
        quote!(#(#field_defs)*),
        quote!(#(#default_assigns)*),
        owned_target,
        quote!(#(#to_owned_assigns)*),
    )
}

fn flex_binding(body: &TokenStream, flex_source: &TokenStream) -> Option<TokenStream> {
    body.to_string()
        .contains("flex")
        .then(|| quote!(let flex = #flex_source;))
}

fn ordered(fields: &[FieldSpec]) -> impl Iterator<Item = &FieldSpec> {
    fields
        .iter()
        .filter(|f| !is_tagged(f))
        .chain(fields.iter().filter(|f| is_tagged(f)))
}

fn default_is_null(f: &FieldSpec) -> bool {
    matches!(&f.default, Some(serde_json::Value::Null))
}

/// Rust struct-field type. The emitter stores top-level tagged fields owned
/// when they cannot borrow from the ephemeral tagged-payload buffer.
fn field_type(ctx: &Ctx, f: &FieldSpec, top_level: bool) -> String {
    if is_tagged(f) {
        let nullable = is_nullable(f) || default_is_null(f);
        if top_level && tagged_field_needs_owned(f, ctx.res_map) {
            let owned_path = owned_struct_path_for(f, ctx.parent_module, ctx.res_map);
            return type_map::owned_type(&f.field_type, nullable, owned_path.as_deref());
        }
        return type_map::borrowed_type(
            &f.field_type,
            nullable,
            struct_path_for(f, ctx.res_map).as_deref(),
        );
    }
    type_map::borrowed_type(
        &f.field_type,
        is_nullable(f),
        struct_path_for(f, ctx.res_map).as_deref(),
    )
}

fn to_owned_expr(ctx: &Ctx, f: &FieldSpec, top_level: bool) -> String {
    let field = name_conv::field_name(&f.name);
    let expr = format!("self.{field}");
    if is_tagged(f) {
        let nullable = is_nullable(f) || default_is_null(f);
        if top_level && tagged_field_needs_owned(f, ctx.res_map) {
            return format!("self.{field}.clone()");
        }
        return to_owned_field_expr(&f.field_type, &expr, nullable);
    }
    to_owned_field_expr(&f.field_type, &expr, is_nullable(f))
}

// --- encode / encoded_len (identical shape to owned, borrowed leaf codecs) ---

fn encode_body(ctx: &Ctx, fields: &[FieldSpec], has_flex: bool) -> TokenStream {
    let stmts = fields.iter().filter(|f| !is_tagged(f)).map(encode_one);
    let trailer = has_flex.then(|| {
        let mut_kw = if fields.iter().any(is_tagged) {
            quote!(mut)
        } else {
            quote!()
        };
        let adds = fields
            .iter()
            .filter(|f| is_tagged(f))
            .map(|f| encode_tagged(ctx, f));
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

fn split_encode_body(
    ctx: &Ctx,
    fields: &[FieldSpec],
    has_flex: bool,
) -> (TokenStream, TokenStream) {
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
                    #body
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
            .map(|field| encode_tagged(ctx, field));
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

fn non_flex_wrap(f: &FieldSpec, body: TokenStream) -> TokenStream {
    if field_forces_non_flex(f) {
        quote!({ let flex = false; #body })
    } else {
        body
    }
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
        non_flex_wrap(
            f,
            parse_expr(&encode_call(&f.field_type, &expr, is_nullable(f))),
        )
    };
    quote!(if #cond { #inner })
}

/// Tagged string and bytes fields stored owned use the owned-flavor codec,
/// which adds `.as_deref()`. Everything else uses the borrowed codec.
fn tagged_owned_codec(ctx: &Ctx, f: &FieldSpec) -> bool {
    tagged_field_needs_owned(f, ctx.res_map)
        && matches!(base_type(&f.field_type), "string" | "bytes")
}

fn encode_tagged(ctx: &Ctx, f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let tag = Literal::u32_unsuffixed(f.tag.expect("tagged field has tag"));
    let nullable = is_nullable(f) || default_is_null(f);
    let expr = format!("self.{field}");
    let body_str = if tagged_owned_codec(ctx, f) {
        borrowed::owned_encode_call(&f.field_type, &expr, nullable)
    } else {
        encode_call(&f.field_type, &expr, nullable)
    };
    let body = parse_expr(&body_str.replace("buf", "b"));
    let len = parse_expr(&tagged_len_str(ctx, f, &expr, nullable));
    let should_encode = tagged_should_encode(f);
    quote! {
        if #should_encode {
            let payload = encode_to_bytes(#len, |b| { #body; Ok(()) });
            tagged.add(#tag, payload);
        }
    }
}

fn tagged_len_str(ctx: &Ctx, f: &FieldSpec, expr: &str, nullable: bool) -> String {
    if tagged_owned_codec(ctx, f) {
        borrowed::owned_encoded_len_expr(&f.field_type, expr, nullable)
    } else {
        encoded_len_expr(&f.field_type, expr, nullable)
    }
}

fn len_body(ctx: &Ctx, fields: &[FieldSpec], has_flex: bool) -> TokenStream {
    let stmts = fields.iter().filter(|f| !is_tagged(f)).map(len_one);
    let trailer = has_flex.then(|| {
        let mut_kw = if fields.iter().any(is_tagged) {
            quote!(mut)
        } else {
            quote!()
        };
        let pushes = fields
            .iter()
            .filter(|f| is_tagged(f))
            .map(|f| len_tagged(ctx, f));
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
        let nnb = parse_expr(&encoded_len_expr_option_as_non_nullable(
            &f.field_type,
            &expr,
        ));
        let nc = parse_expr(&ncond);
        non_flex_wrap(f, quote!(if #nc { #nb } else { #nnb }))
    } else {
        non_flex_wrap(
            f,
            parse_expr(&encoded_len_expr(&f.field_type, &expr, is_nullable(f))),
        )
    };
    quote!(if #cond { n += #inner; })
}

fn len_tagged(ctx: &Ctx, f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let tag = Literal::u32_unsuffixed(f.tag.expect("tagged field has tag"));
    let nullable = is_nullable(f) || default_is_null(f);
    let len = parse_expr(&tagged_len_str(ctx, f, &format!("self.{field}"), nullable));
    let should_encode = tagged_should_encode(f);
    quote! {
        if #should_encode {
            known_pairs.push((#tag, #len));
        }
    }
}

// --- decode_borrow ----------------------------------------------------------

fn decode_body(ctx: &Ctx, fields: &[FieldSpec], has_flex: bool) -> TokenStream {
    let stmts = fields
        .iter()
        .filter(|f| !is_tagged(f))
        .map(|f| decode_one(ctx, f));
    let trailer = has_flex.then(|| decode_tagged_block(ctx, fields));
    quote!(#(#stmts)* #trailer)
}

fn split_decode_body(
    ctx: &Ctx,
    fields: &[FieldSpec],
    has_flex: bool,
    has_lt: bool,
) -> (TokenStream, TokenStream) {
    let mut helpers = Vec::new();
    let mut calls = Vec::new();
    let helper_lifetime = if has_lt { quote!('a) } else { quote!('de) };
    let helper_generics = if has_lt { quote!() } else { quote!(<'de>) };
    for (index, field) in fields.iter().filter(|field| !is_tagged(field)).enumerate() {
        let helper = format_ident!("decode_field_{index}");
        let body = decode_one(ctx, field);
        let flex = if body.to_string().contains("flex") {
            quote!(flex)
        } else {
            quote!(_flex)
        };
        helpers.push(quote! {
            fn #helper #helper_generics(
                out: &mut Self,
                buf: &mut &#helper_lifetime [u8],
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
        let body = decode_tagged_block(ctx, fields);
        helpers.push(quote! {
            fn #helper #helper_generics(
                out: &mut Self,
                buf: &mut &#helper_lifetime [u8],
                version: i16,
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

fn decode_one(ctx: &Ctx, f: &FieldSpec) -> TokenStream {
    let field = format_ident!("{}", name_conv::field_name(&f.name));
    let cond = parse_expr(&version_cond(f.versions, "version"));
    let inner = if let Some(ncond) = nullable_split_cond(f) {
        let nb = parse_expr(&decode_borrow_call(&f.field_type, true, ctx.res_map));
        let nnb = parse_expr(&decode_borrow_call(&f.field_type, false, ctx.res_map));
        let nc = parse_expr(&ncond);
        non_flex_wrap(f, quote!(if #nc { #nb } else { Some(#nnb) }))
    } else {
        non_flex_wrap(
            f,
            parse_expr(&decode_borrow_call(
                &f.field_type,
                is_nullable(f),
                ctx.res_map,
            )),
        )
    };
    quote!(if #cond { out.#field = #inner; })
}

fn decode_tagged_block(ctx: &Ctx, fields: &[FieldSpec]) -> TokenStream {
    if !fields.iter().any(is_tagged) {
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
        let call = if tagged_field_needs_owned(f, ctx.res_map) {
            decode_owned_call(&f.field_type, nullable, ctx.parent_module, ctx.res_map)
                .replace("buf", "b")
        } else {
            decode_borrow_call(&f.field_type, nullable, ctx.res_map).replace("buf", "b")
        };
        let call = parse_expr(&call);
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

fn populated_impl(ctx: &Ctx, impl_target: &TokenStream, fields: &[FieldSpec]) -> TokenStream {
    let assigns: Vec<_> = ordered(fields)
        .filter(|f| base_type(&f.field_type) != "records")
        .map(|f| {
            let field = format_ident!("{}", name_conv::field_name(&f.name));
            let option = is_nullable(f) || (is_tagged(f) && default_is_null(f));
            let value = parse_expr(&borrowed_populated_value(
                f,
                ctx.res_map,
                ctx.parent_module,
                option,
            ));
            let cond = parse_expr(&version_cond(f.versions, "version"));
            quote!(if #cond { m.#field = #value; })
        })
        .collect();
    if assigns.is_empty() {
        return quote! {
            #[cfg(test)]
            #impl_target {
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
        #impl_target {
            #[must_use]
            pub fn populated(#version: i16) -> Self {
                let #mut_kw m = Self::default();
                #(#assigns)*
                m
            }
        }
    }
}

fn nested_structs(ctx: &Ctx, fields: &[FieldSpec], flex_min_val: i16) -> TokenStream {
    let mut out = TokenStream::new();
    for f in fields.iter().filter(|f| !f.fields.is_empty()) {
        let name = base_type(&f.field_type);
        out.extend(struct_block(
            ctx,
            name,
            &f.fields,
            needs_lifetime(&f.fields, ctx.res_map),
            &FlexSource::Nested(flex_min_val),
            None,
            (false, flex_min_val < i16::MAX),
        ));
        out.extend(nested_structs(ctx, &f.fields, flex_min_val));
    }
    out
}
