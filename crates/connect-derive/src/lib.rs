use proc_macro::TokenStream;

use quote::quote;
use syn::{
    AngleBracketedGenericArguments, Attribute, Data, DeriveInput, Expr, Fields, GenericArgument,
    PathArguments, Type, parse_macro_input,
};

#[proc_macro_derive(ConnectorConfig, attributes(config))]
pub fn derive_connector_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_connector_config(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_connector_config(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = input.ident;
    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "ConnectorConfig can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "ConnectorConfig can only be derived for structs",
            ));
        }
    };

    let mut def_steps = Vec::new();
    let mut initializers = Vec::new();

    for field in fields {
        let field_ident = field.ident.expect("named fields have identifiers");
        let attrs = FieldAttrs::parse(&field.attrs)?;
        let key = attrs.name.unwrap_or_else(|| field_ident.to_string());
        let key_lit = syn::LitStr::new(&key, field_ident.span());
        let ty = field.ty;

        ensure_supported_type(&ty)?;

        if attrs.secret {
            def_steps.push(quote! {
                def = def.secret(#key_lit);
            });
        } else if let Some(default) = attrs.default {
            def_steps.push(quote! {
                def = def.default(
                    #key_lit,
                    <#ty as ::crabka_connect::FromResolvedValue>::KIND,
                    ::serde_json::json!(#default),
                );
            });
        } else if attrs.required {
            def_steps.push(quote! {
                def = def.required(#key_lit, <#ty as ::crabka_connect::FromResolvedValue>::KIND);
            });
        } else {
            def_steps.push(quote! {
                def = def.optional(#key_lit, <#ty as ::crabka_connect::FromResolvedValue>::KIND);
            });
        }

        initializers.push(quote! {
            #field_ident: <#ty as ::crabka_connect::FromResolvedValue>::from_resolved_value(
                config,
                #key_lit,
            )?
        });
    }

    Ok(quote! {
        impl ::crabka_connect::ConnectorConfig for #ident {
            fn config_def() -> ::crabka_connect::ConfigDef {
                let mut def = ::crabka_connect::ConfigDef::new(stringify!(#ident));
                #(#def_steps)*
                def
            }

            fn from_resolved(
                config: &::crabka_connect::ResolvedConfig,
            ) -> ::crabka_connect::ConfigResult<Self> {
                Ok(Self {
                    #(#initializers,)*
                })
            }
        }
    })
}

#[derive(Default)]
struct FieldAttrs {
    required: bool,
    secret: bool,
    default: Option<Expr>,
    name: Option<String>,
}

impl FieldAttrs {
    fn parse(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut out = Self::default();
        for attr in attrs {
            if !attr.path().is_ident("config") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("required") {
                    out.required = true;
                    return Ok(());
                }
                if meta.path.is_ident("secret") {
                    out.secret = true;
                    return Ok(());
                }
                if meta.path.is_ident("default") {
                    let value = meta.value()?;
                    out.default = Some(value.parse()?);
                    return Ok(());
                }
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    out.name = Some(lit.value());
                    return Ok(());
                }
                Err(meta.error("unsupported config attribute"))
            })?;
        }

        if out.secret && out.default.is_some() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "secret fields cannot declare defaults",
            ));
        }

        Ok(out)
    }
}

fn ensure_supported_type(ty: &Type) -> syn::Result<()> {
    if is_supported_type(ty) {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            ty,
            "unsupported ConnectorConfig field type",
        ))
    }
}

fn is_supported_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };

    if type_path.qself.is_some() {
        return false;
    }

    let Some(segment) = type_path.path.segments.last() else {
        return false;
    };
    let ident = segment.ident.to_string();

    match ident.as_str() {
        "String" | "bool" | "i8" | "i16" | "i32" | "i64" | "isize" | "u8" | "u16" | "u32"
        | "u64" | "usize" | "f32" | "f64" | "Value" | "SecretString" | "Duration" => {
            segment.arguments.is_empty()
        }
        "Vec" => is_single_generic_argument(&segment.arguments, |inner| {
            path_ends_with_ident(inner, "String")
        }),
        "Option" => is_single_generic_argument(&segment.arguments, is_supported_type),
        _ => false,
    }
}

fn is_single_generic_argument(args: &PathArguments, predicate: impl FnOnce(&Type) -> bool) -> bool {
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = args else {
        return false;
    };
    let mut args = args.iter();
    let Some(GenericArgument::Type(inner)) = args.next() else {
        return false;
    };
    args.next().is_none() && predicate(inner)
}

fn path_ends_with_ident(ty: &Type, expected: &str) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == expected && segment.arguments.is_empty())
}
