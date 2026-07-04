//! Generate the `include!` wrapper module bodies under
//! `crates/protocol/src/{owned,borrowed}/<snake>.rs`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::{
    emit::common::banner,
    ir::{MessageSpec, MessageType},
    name_conv,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Owned,
    Borrowed,
}

impl Flavor {
    #[must_use]
    pub fn dir(self) -> &'static str {
        match self {
            Flavor::Owned => "owned",
            Flavor::Borrowed => "borrowed",
        }
    }
}

/// The clippy-suppress header that all generated wrapper files receive.
///
/// Only lints with **no** machine-applicable fix are listed — intentional
/// narrowing casts, the `#[must_use]` / `Default` style suggestions, and the
/// always-true version comparisons the schema version ranges produce (e.g.
/// `version >= 0`). Everything else (`semicolon_if_nothing_returned`,
/// `manual_range_contains`, `redundant_closure_for_method_calls`, `unused_*`, …)
/// is auto-corrected by the `cargo clippy --fix` pass in `tools/regenerate.sh`,
/// so it does not need an allow here.
#[must_use]
pub fn allow_header() -> &'static str {
    concat!(
        "#![allow(\n",
        "    clippy::absurd_extreme_comparisons,\n",
        "    clippy::cast_possible_truncation,\n",
        "    clippy::cast_possible_wrap,\n",
        "    clippy::cast_sign_loss,\n",
        "    clippy::default_trait_access,\n",
        "    clippy::must_use_candidate,\n",
        "    clippy::new_without_default,\n",
        "    clippy::nonminimal_bool,\n",
        "    clippy::too_many_lines,\n",
        "    clippy::unnecessary_wraps,\n",
        "    clippy::unreadable_literal,\n",
        "    unused_mut,\n",
        "    unused_variables\n",
        ")]",
    )
}

/// Emit a wrapper body for one message + flavor.
#[must_use]
pub fn emit(
    spec: &MessageSpec,
    flavor: Flavor,
    schemas_version: &str,
    namespace: Option<&str>,
) -> String {
    let type_name = name_conv::type_name(&spec.name);
    let suffix = match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    };

    // 1. Banner string.
    let mut out = banner(schemas_version);

    // 2. Two // comment lines explaining the clippy suppression (not tokens).
    out.push_str(
        "// Clippy lints that fire on generated code patterns are suppressed here so\n\
         // that regenerating the file does not require manual allow annotations.\n",
    );

    // 3. allow_header() — already a string constant.
    out.push_str(allow_header());
    out.push_str("\n\n");

    // 4. include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/{path}.rs"));
    let path_prefix = match namespace {
        None => String::new(),
        Some(ns) => format!("{ns}/"),
    };
    let path = format!("/generated/{path_prefix}{type_name}.{suffix}.rs");
    let include_tokens: TokenStream = quote! {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), #path));
    };
    let _validate_include: syn::Stmt =
        syn::parse2(include_tokens.clone()).expect("include! must be valid Rust");
    out.push_str(&include_tokens.to_string());
    out.push('\n');

    // 5. #[cfg(test)] mod tests { ... } built with quote!.
    let tests_tokens = match flavor {
        Flavor::Owned => owned_tests_tokens(&type_name),
        Flavor::Borrowed => borrowed_tests_tokens(&type_name),
    };
    out.push_str(&tests_tokens.to_string());

    out
}

/// Build `#[cfg(test)] mod tests { ... }` for the Owned flavor as a `TokenStream`.
fn owned_tests_tokens(type_name: &str) -> TokenStream {
    let ty = format_ident!("{type_name}");
    let tokens = quote! {
        #[cfg(test)]
        mod tests {
            use assert2::assert;
            use super::*;
            use crate::{Decode, Encode};
            use bytes::BytesMut;

            fn roundtrip(msg: &#ty, v: i16) {
                let mut buf = BytesMut::new();
                msg.encode(&mut buf, v).unwrap();
                assert!(msg.encoded_len(v) == buf.len());
                let bytes = buf.freeze();
                let mut cur = &bytes[..];
                let decoded = #ty::decode(&mut cur, v).unwrap();
                assert!(cur.is_empty());
                let mut reencoded = BytesMut::new();
                decoded.encode(&mut reencoded, v).unwrap();
                assert!(&reencoded[..] == &bytes[..]);
                let _ = default_json(v);
            }

            #[test]
            fn default_roundtrips_all_versions() {
                for v in MIN_VERSION..=MAX_VERSION {
                    roundtrip(&#ty::default(), v);
                }
            }

            #[test]
            fn populated_roundtrips_all_versions() {
                for v in MIN_VERSION..=MAX_VERSION {
                    roundtrip(&#ty::populated(v), v);
                }
            }
        }
    };
    let _validate: syn::ItemMod =
        syn::parse2(tokens.clone()).expect("generated owned tests must be valid Rust");
    tokens
}

/// Build `#[cfg(test)] mod tests { ... }` for the Borrowed flavor as a `TokenStream`.
fn borrowed_tests_tokens(type_name: &str) -> TokenStream {
    let ty = format_ident!("{type_name}");
    let tokens = quote! {
        #[cfg(test)]
        mod tests {
            use assert2::assert;
            use super::*;
            use crate::{DecodeBorrow, Encode};
            use bytes::BytesMut;

            fn check(msg_bytes: &bytes::Bytes, v: i16) {
                let mut cur: &[u8] = msg_bytes;
                let decoded = #ty::decode_borrow(&mut cur, v).unwrap();
                assert!(cur.is_empty());
                assert!(decoded.encoded_len(v) == msg_bytes.len());
                let mut reencoded = BytesMut::new();
                decoded.encode(&mut reencoded, v).unwrap();
                assert!(&reencoded[..] == &msg_bytes[..]);
                let owned = decoded.to_owned();
                let mut owned_buf = BytesMut::new();
                owned.encode(&mut owned_buf, v).unwrap();
                assert!(&owned_buf[..] == &msg_bytes[..]);
            }

            #[test]
            fn default_roundtrips_all_versions() {
                for v in MIN_VERSION..=MAX_VERSION {
                    let msg = #ty::default();
                    let mut buf = BytesMut::new();
                    msg.encode(&mut buf, v).unwrap();
                    check(&buf.freeze(), v);
                }
            }

            #[test]
            fn populated_roundtrips_all_versions() {
                for v in MIN_VERSION..=MAX_VERSION {
                    let msg = #ty::populated(v);
                    let mut buf = BytesMut::new();
                    msg.encode(&mut buf, v).unwrap();
                    check(&buf.freeze(), v);
                }
            }
        }
    };
    let _validate: syn::ItemMod =
        syn::parse2(tokens.clone()).expect("generated borrowed tests must be valid Rust");
    tokens
}

/// True if this spec should get a wrapper.
#[must_use]
pub fn should_emit_wrapper(spec: &MessageSpec) -> bool {
    !spec.valid_versions.is_empty()
        && matches!(
            spec.message_type,
            MessageType::Request | MessageType::Response | MessageType::Header | MessageType::Data
        )
}
