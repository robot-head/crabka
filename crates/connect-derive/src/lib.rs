use std::collections::BTreeSet;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use quote::quote;
use syn::{
    AngleBracketedGenericArguments, Attribute, Data, DeriveInput, Expr, Fields, GenericArgument,
    Generics, PathArguments, Type, parse_macro_input,
};

#[proc_macro_derive(ConnectorConfig, attributes(config))]
pub fn derive_connector_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_connector_config(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_connector_config(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let DeriveInput {
        attrs,
        ident,
        generics,
        data,
        ..
    } = input;
    reject_generics(&generics)?;

    let crate_path = resolve_crabka_connect_path(&attrs)?;
    let fields = match data {
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
    let mut seen_keys = BTreeSet::new();

    for field in fields {
        let field_ident = field.ident.expect("named fields have identifiers");
        let attrs = FieldAttrs::parse(&field.attrs)?;
        let ty = field.ty;

        let type_info = analyze_type(&ty)?;
        validate_field_attrs(&attrs, &ty)?;
        let key = attrs.name.unwrap_or_else(|| field_ident.to_string());
        if !seen_keys.insert(key.clone()) {
            return Err(syn::Error::new_spanned(
                &field_ident,
                format!("duplicate connector config key `{key}`"),
            ));
        }
        let key_lit = syn::LitStr::new(&key, field_ident.span());

        if attrs.secret {
            if attrs.required || !type_info.is_option {
                def_steps.push(quote! {
                    def = def.secret(#key_lit);
                });
            } else {
                def_steps.push(quote! {
                    def = def.optional(
                        #key_lit,
                        <#ty as #crate_path::FromResolvedValue>::KIND,
                    );
                });
            }
        } else if let Some(default) = attrs.default {
            def_steps.push(quote! {
                def = def.default(
                    #key_lit,
                    <#ty as #crate_path::FromResolvedValue>::KIND,
                    #crate_path::__serde_json::json!(#default),
                );
            });
        } else if attrs.required || !type_info.is_option {
            def_steps.push(quote! {
                def = def.required(#key_lit, <#ty as #crate_path::FromResolvedValue>::KIND);
            });
        } else {
            def_steps.push(quote! {
                def = def.optional(#key_lit, <#ty as #crate_path::FromResolvedValue>::KIND);
            });
        }

        initializers.push(quote! {
            #field_ident: <#ty as #crate_path::FromResolvedValue>::from_resolved_value(
                config,
                #key_lit,
            )?
        });
    }

    Ok(quote! {
        impl #crate_path::ConnectorConfig for #ident {
            fn config_def() -> #crate_path::ConfigDef {
                let mut def = #crate_path::ConfigDef::new(stringify!(#ident));
                #(#def_steps)*
                def
            }

            fn from_resolved(
                config: &#crate_path::ResolvedConfig,
            ) -> #crate_path::ConfigResult<Self> {
                Ok(Self {
                    #(#initializers,)*
                })
            }
        }
    })
}

fn reject_generics(generics: &Generics) -> syn::Result<()> {
    if generics.params.is_empty() && generics.where_clause.is_none() {
        return Ok(());
    }

    Err(syn::Error::new_spanned(
        generics,
        "ConnectorConfig does not support generic structs",
    ))
}

fn resolve_crabka_connect_path(attrs: &[Attribute]) -> syn::Result<proc_macro2::TokenStream> {
    let mut override_path = None;
    for attr in attrs {
        if !attr.path().is_ident("config") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                let path: syn::Path = lit.parse()?;
                override_path = Some(quote!(#path));
                Ok(())
            } else {
                Err(meta.error("unsupported container config attribute"))
            }
        })?;
    }

    if let Some(path) = override_path {
        return Ok(path);
    }

    match crate_name("crabka-connect") {
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            Ok(quote!(::#ident))
        }
        Ok(FoundCrate::Itself) | Err(_) => Ok(quote!(::crabka_connect)),
    }
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

            if out.secret && out.default.is_some() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "secret fields cannot declare defaults",
                ));
            }
        }

        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct TypeInfo {
    is_option: bool,
    is_secret: bool,
}

fn analyze_type(ty: &Type) -> syn::Result<TypeInfo> {
    type_info(ty)
        .ok_or_else(|| syn::Error::new_spanned(ty, "unsupported ConnectorConfig field type"))
}

fn validate_field_attrs(attrs: &FieldAttrs, ty: &Type) -> syn::Result<()> {
    let type_info = analyze_type(ty)?;
    if attrs.secret && !type_info.is_secret {
        return Err(syn::Error::new_spanned(
            ty,
            "#[config(secret)] fields must have type SecretString or Option<SecretString>",
        ));
    }
    if type_info.is_secret && !attrs.secret {
        return Err(syn::Error::new_spanned(
            ty,
            "SecretString fields must be marked #[config(secret)]",
        ));
    }
    Ok(())
}

fn type_info(ty: &Type) -> Option<TypeInfo> {
    let Type::Path(type_path) = ty else {
        return None;
    };

    if type_path.qself.is_some() {
        return None;
    }

    let segment = type_path.path.segments.last()?;

    if segment.ident == "Option" {
        let inner = single_generic_argument(&segment.arguments)?;
        let inner = type_info(inner)?;
        return Some(TypeInfo {
            is_option: true,
            is_secret: inner.is_secret,
        });
    }

    let ident = &segment.ident;
    let is_supported_scalar = ident == "String"
        || ident == "bool"
        || ident == "i8"
        || ident == "i16"
        || ident == "i32"
        || ident == "i64"
        || ident == "isize"
        || ident == "u8"
        || ident == "u16"
        || ident == "u32"
        || ident == "u64"
        || ident == "usize"
        || ident == "f32"
        || ident == "f64"
        || ident == "Value"
        || ident == "SecretString"
        || ident == "Duration";
    if is_supported_scalar && segment.arguments.is_empty() {
        return Some(TypeInfo {
            is_option: false,
            is_secret: segment.ident == "SecretString",
        });
    }

    if segment.ident == "Vec"
        && single_generic_argument(&segment.arguments)
            .is_some_and(|inner| path_ends_with_ident(inner, "String"))
    {
        return Some(TypeInfo {
            is_option: false,
            is_secret: false,
        });
    }

    None
}

fn single_generic_argument(args: &PathArguments) -> Option<&Type> {
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = args else {
        return None;
    };
    let mut args = args.iter();
    let Some(GenericArgument::Type(inner)) = args.next() else {
        return None;
    };
    if args.next().is_none() {
        Some(inner)
    } else {
        None
    }
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

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn expanded_config_def(input: &str) -> String {
        let input: DeriveInput = syn::parse_str(input).unwrap();
        expand_connector_config(input)
            .unwrap()
            .to_string()
            .split_whitespace()
            .collect()
    }

    fn parse_type(input: &str) -> Type {
        syn::parse_str(input).unwrap()
    }

    #[test]
    fn generated_config_def_cases_match_expected_entries() {
        let cases = [
            (
                "secret fields",
                r"
                struct Demo {
                    #[config(secret)]
                    password: SecretString,
                    #[config(secret)]
                    maybe_password: Option<SecretString>,
                    #[config(required, secret)]
                    required_maybe_password: Option<SecretString>,
                }
                ",
                [
                    "def=def.secret(\"password\");",
                    "def=def.optional(\"maybe_password\",",
                    "def=def.secret(\"required_maybe_password\");",
                ],
            ),
            (
                "plain fields",
                r"
                struct Demo {
                    database_url: String,
                    note: Option<String>,
                    #[config(required)]
                    required_note: Option<String>,
                }
                ",
                [
                    "def=def.required(\"database_url\",",
                    "def=def.optional(\"note\",",
                    "def=def.required(\"required_note\",",
                ],
            ),
        ];

        for (name, input, expected) in cases {
            let expanded = expanded_config_def(input);
            let actual: Vec<_> = expected
                .iter()
                .copied()
                .filter(|needle| expanded.contains(needle))
                .collect();
            assert_eq!(actual, expected, "generated config case {name}");
        }
    }

    #[test]
    fn type_info_recognizes_options_secrets_and_duration_paths() {
        let optional_secret = analyze_type(&parse_type("Option<SecretString>")).unwrap();
        assert!(optional_secret.is_option);
        assert!(optional_secret.is_secret);

        let string_vec = analyze_type(&parse_type("Vec<String>")).unwrap();
        check!((string_vec.is_option, string_vec.is_secret) == (false, false));
        check!(analyze_type(&parse_type("Vec<u8>")).is_err());

        for scalar in [
            "String", "bool", "i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64",
            "usize", "f32", "f64", "Value",
        ] {
            let info = analyze_type(&parse_type(scalar)).unwrap();
            assert!(!info.is_option, "case {scalar}");
            assert!(!info.is_secret, "case {scalar}");
        }

        for (name, ty) in [
            ("standard_duration_path", "std::time::Duration"),
            ("absolute_duration_path", "::std::time::Duration"),
        ] {
            let info = analyze_type(&parse_type(ty)).unwrap();
            assert!(!info.is_option, "case {name}");
            assert!(!info.is_secret, "case {name}");
        }
    }

    #[test]
    fn secret_field_validation_requires_secret_attr_and_secret_type() {
        let secret_ty = parse_type("SecretString");
        let string_ty = parse_type("String");

        let explicit_secret = FieldAttrs {
            secret: true,
            ..FieldAttrs::default()
        };
        validate_field_attrs(&explicit_secret, &secret_ty).unwrap();

        let secret_attr_on_string = FieldAttrs {
            secret: true,
            ..FieldAttrs::default()
        };
        for (name, attrs, ty) in [
            (
                "secret_type_without_attribute",
                FieldAttrs::default(),
                &secret_ty,
            ),
            (
                "secret_attribute_on_string",
                secret_attr_on_string,
                &string_ty,
            ),
        ] {
            assert!(validate_field_attrs(&attrs, ty).is_err(), "case {name}");
        }
    }
}
