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
        let ty = field.ty;

        let type_info = analyze_type(&ty)?;
        validate_field_attrs(&attrs, &ty)?;
        let key = attrs.name.unwrap_or_else(|| field_ident.to_string());
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
                        <#ty as ::crabka_connect::FromResolvedValue>::KIND,
                    );
                });
            }
        } else if let Some(default) = attrs.default {
            def_steps.push(quote! {
                def = def.default(
                    #key_lit,
                    <#ty as ::crabka_connect::FromResolvedValue>::KIND,
                    ::crabka_connect::__serde_json::json!(#default),
                );
            });
        } else if attrs.required || !type_info.is_option {
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
    use super::*;

    fn parse_type(input: &str) -> Type {
        syn::parse_str(input).unwrap()
    }

    #[test]
    fn type_info_recognizes_options_secrets_and_duration_paths() {
        let optional_secret = analyze_type(&parse_type("Option<SecretString>")).unwrap();
        assert!(optional_secret.is_option);
        assert!(optional_secret.is_secret);

        let std_duration = analyze_type(&parse_type("std::time::Duration")).unwrap();
        assert!(!std_duration.is_option);
        assert!(!std_duration.is_secret);

        let absolute_duration = analyze_type(&parse_type("::std::time::Duration")).unwrap();
        assert!(!absolute_duration.is_option);
        assert!(!absolute_duration.is_secret);
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

        let secret_without_attr = FieldAttrs::default();
        assert!(validate_field_attrs(&secret_without_attr, &secret_ty).is_err());

        let secret_attr_on_string = FieldAttrs {
            secret: true,
            ..FieldAttrs::default()
        };
        assert!(validate_field_attrs(&secret_attr_on_string, &string_ty).is_err());
    }
}
