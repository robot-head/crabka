//! A `quote!`-based reimplementation of the `borrowed` emitter — the
//! zero-copy / lifetime-carrying flavor (`&'a str`, `&'a [u8]`,
//! `RecordsPayloadBorrowed<'a>`), plus a `to_owned()` bridge to the owned types
//! and a `DecodeBorrow<'de>` impl.
//!
//! Same approach as [`super::owned_quote`]: the structural scaffolding (struct,
//! `Default`, `to_owned`, `Encode`, `DecodeBorrow`, nested structs, `populated`)
//! is built as tokens, while the leaf codec expressions, import and constant
//! blocks, and common-struct files are reused verbatim from `super::borrowed`
//! (whose wire bytes are already test-covered). The `borrowed_quote_parity`
//! test asserts token-stream equality with the string emitter for every schema.
#![allow(clippy::doc_markdown, clippy::too_many_lines)]

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
            has_float64_recursive, is_nullable, is_tagged, needs_lifetime, nullable_split_cond,
            owned_struct_path_for, spec_needs_lifetime, struct_path_for, tagged_field_needs_owned,
            tagged_is_default_cond, to_owned_field_expr, version_cond,
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
            FlexSource::Nested(fm) => {
                let n = Literal::i16_unsuffixed(*fm);
                quote!(version >= #n)
            }
        }
    }
}

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
        true,
        has_any_flex(spec),
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
/// `commonStructs` are message-local: each is emitted under a per-message nested
/// module `common/<message_snake>/<struct_snake>`. The `commons` key is the
/// relative path stem `<message_snake>/<struct_snake>`; the caller turns that
/// into the on-disk body path and the wrapper module nesting.
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
            false,
            fm < i16::MAX,
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
/// `top_level` toggles the tagged-field-owned typing that only the message type
/// uses; nested structs always store tagged fields in their borrowed form.
#[allow(clippy::too_many_arguments)]
fn struct_block(
    ctx: &Ctx,
    name: &str,
    fields: &[FieldSpec],
    has_lt: bool,
    flex: &FlexSource,
    guard: Option<TokenStream>,
    top_level: bool,
    has_flex: bool,
) -> TokenStream {
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

    let field_defs = ordered(fields).map(|f| {
        let fname = format_ident!("{}", name_conv::field_name(&f.name));
        let ty = parse_expr(&field_type(ctx, f, top_level));
        quote!(pub #fname: #ty,)
    });

    let default_assigns = ordered(fields).map(|f| {
        let fname = format_ident!("{}", name_conv::field_name(&f.name));
        let expr = parse_expr(&borrowed_default_expr(f, ctx.res_map));
        quote!(#fname: #expr,)
    });

    let owned_target = parse_expr(&format!(
        "{}::{}::{name}",
        ctx.owned_root, ctx.parent_module
    ));
    let to_owned_assigns = ordered(fields).map(|f| {
        let fname = format_ident!("{}", name_conv::field_name(&f.name));
        let expr = parse_expr(&to_owned_expr(ctx, f, top_level));
        quote!(#fname: #expr,)
    });

    let guard =
        guard.map(|err| quote!(if !(MIN_VERSION..=MAX_VERSION).contains(&version) { #err }));
    let flex_src = flex.tokens();
    let encode_body = encode_body(ctx, fields, has_flex);
    let len_body = len_body(ctx, fields, has_flex);
    let decode_body = decode_body(ctx, fields, has_flex);
    let populated = populated_impl(ctx, &impl_a, &ty_a, fields);

    quote! {
        #[derive(Debug, Clone, PartialEq #eq)]
        pub struct #ty_a {
            #(#field_defs)*
            pub unknown_tagged_fields: UnknownTaggedFields,
        }

        #impl_a Default for #ty_a {
            fn default() -> Self {
                Self {
                    #(#default_assigns)*
                    unknown_tagged_fields: Default::default(),
                }
            }
        }

        #impl_a #ty_a {
            pub fn to_owned(&self) -> #owned_target {
                #owned_target {
                    #(#to_owned_assigns)*
                    unknown_tagged_fields: self.unknown_tagged_fields.clone(),
                }
            }
        }

        #impl_a Encode for #ty_a {
            fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
                #guard
                let flex = #flex_src;
                #encode_body
                Ok(())
            }
            fn encoded_len(&self, version: i16) -> usize {
                let flex = #flex_src;
                let mut n: usize = 0;
                #len_body
                n
            }
        }

        impl<'de> DecodeBorrow<'de> for #ty_de {
            fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {
                #guard
                let flex = #flex_src;
                let mut out = Self::default();
                #decode_body
                Ok(out)
            }
        }

        #populated
    }
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

/// Rust struct-field type. Top-level tagged fields that cannot borrow from the
/// ephemeral tagged-payload buffer are stored owned.
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
        return to_owned_field_expr(&f.field_type, &expr, nullable, ctx.res_map);
    }
    to_owned_field_expr(&f.field_type, &expr, is_nullable(f), ctx.res_map)
}

// --- encode / encoded_len (identical shape to owned, borrowed leaf codecs) ---

fn encode_body(ctx: &Ctx, fields: &[FieldSpec], has_flex: bool) -> TokenStream {
    let stmts = fields
        .iter()
        .filter(|f| !is_tagged(f))
        .map(|f| encode_one(ctx, f));
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

fn non_flex_wrap(f: &FieldSpec, body: TokenStream) -> TokenStream {
    if field_forces_non_flex(f) {
        quote!({ let flex = false; #body })
    } else {
        body
    }
}

fn encode_one(ctx: &Ctx, f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let cond = parse_expr(&version_cond(f.versions, "version"));
    let expr = format!("self.{field}");
    let inner = if let Some(ncond) = nullable_split_cond(f) {
        let nb = parse_expr(&encode_call(&f.field_type, &expr, true, ctx.res_map));
        let nnb = parse_expr(&encode_call_option_as_non_nullable(
            &f.field_type,
            &expr,
            ctx.res_map,
        ));
        let nc = parse_expr(&ncond);
        non_flex_wrap(f, quote!(if #nc { #nb } else { #nnb }))
    } else {
        non_flex_wrap(
            f,
            parse_expr(&encode_call(
                &f.field_type,
                &expr,
                is_nullable(f),
                ctx.res_map,
            )),
        )
    };
    quote!(if #cond { #inner })
}

/// Tagged string/bytes fields stored owned use the owned-flavor codec (it adds
/// `.as_deref()`); everything else uses the borrowed codec.
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
        borrowed::owned_encode_call(&f.field_type, &expr, nullable, ctx.res_map)
    } else {
        encode_call(&f.field_type, &expr, nullable, ctx.res_map)
    };
    let body = parse_expr(&body_str.replace("buf", "b"));
    let len = parse_expr(&tagged_len_str(ctx, f, &expr, nullable));
    let is_default = parse_expr(&tagged_is_default_cond(f));
    quote! {
        if !(#is_default) {
            let payload = encode_to_bytes(#len, |b| { #body; Ok(()) });
            tagged.add(#tag, payload);
        }
    }
}

fn tagged_len_str(ctx: &Ctx, f: &FieldSpec, expr: &str, nullable: bool) -> String {
    if tagged_owned_codec(ctx, f) {
        borrowed::owned_encoded_len_expr(&f.field_type, expr, nullable, ctx.res_map)
    } else {
        encoded_len_expr(&f.field_type, expr, nullable, ctx.res_map)
    }
}

fn len_body(ctx: &Ctx, fields: &[FieldSpec], has_flex: bool) -> TokenStream {
    let stmts = fields
        .iter()
        .filter(|f| !is_tagged(f))
        .map(|f| len_one(ctx, f));
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

fn len_one(ctx: &Ctx, f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let cond = parse_expr(&version_cond(f.versions, "version"));
    let expr = format!("self.{field}");
    let inner = if let Some(ncond) = nullable_split_cond(f) {
        let nb = parse_expr(&encoded_len_expr(&f.field_type, &expr, true, ctx.res_map));
        let nnb = parse_expr(&encoded_len_expr_option_as_non_nullable(
            &f.field_type,
            &expr,
            ctx.res_map,
        ));
        let nc = parse_expr(&ncond);
        non_flex_wrap(f, quote!(if #nc { #nb } else { #nnb }))
    } else {
        non_flex_wrap(
            f,
            parse_expr(&encoded_len_expr(
                &f.field_type,
                &expr,
                is_nullable(f),
                ctx.res_map,
            )),
        )
    };
    quote!(if #cond { n += #inner; })
}

fn len_tagged(ctx: &Ctx, f: &FieldSpec) -> TokenStream {
    let field = name_conv::field_name(&f.name);
    let tag = Literal::u32_unsuffixed(f.tag.expect("tagged field has tag"));
    let nullable = is_nullable(f) || default_is_null(f);
    let len = parse_expr(&tagged_len_str(ctx, f, &format!("self.{field}"), nullable));
    let is_default = parse_expr(&tagged_is_default_cond(f));
    quote! {
        if !(#is_default) {
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

fn populated_impl(
    ctx: &Ctx,
    impl_a: &TokenStream,
    ty_a: &TokenStream,
    fields: &[FieldSpec],
) -> TokenStream {
    let assigns = ordered(fields)
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
        });
    quote! {
        #[cfg(test)]
        #impl_a #ty_a {
            #[must_use]
            pub fn populated(version: i16) -> Self {
                let mut m = Self::default();
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
            false,
            flex_min_val < i16::MAX,
        ));
        out.extend(nested_structs(ctx, &f.fields, flex_min_val));
    }
    out
}
