//! Generate the `include!` wrapper module bodies under
//! `crates/protocol/src/{owned,borrowed}/<snake>.rs`.

use crate::emit::common::banner;
use crate::ir::{MessageSpec, MessageType};
use crate::name_conv;
use quote::{format_ident, quote};

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

pub fn allow_attrs() -> proc_macro2::TokenStream {
    quote! {
        #![allow(
            clippy::absurd_extreme_comparisons,
            clippy::cast_possible_truncation,
            clippy::cast_possible_wrap,
            clippy::cast_sign_loss,
            clippy::default_trait_access,
            clippy::must_use_candidate,
            clippy::new_without_default,
            clippy::nonminimal_bool,
            clippy::too_many_lines,
            clippy::unnecessary_wraps,
            clippy::unreadable_literal,
            unused_mut,
            unused_variables
        )]
    }
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
    let type_ident = format_ident!("{type_name}");
    let suffix = match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    };
    let path_prefix = match namespace {
        None => String::new(),
        Some(ns) => format!("{ns}/"),
    };
    let include_path = format!("/generated/{path_prefix}{type_name}.{suffix}.rs");
    let allow_attrs = allow_attrs();
    // Inline round-trip tests gated on the flavor.
    let tests = match flavor {
        Flavor::Owned => owned_tests(&type_ident),
        Flavor::Borrowed => borrowed_tests(&type_ident),
    };
    let tokens = quote! {
        #allow_attrs
        include!(concat!(env!("CARGO_MANIFEST_DIR"), #include_path));
        #tests
    };
    format!("{}{}", banner(schemas_version), tokens)
}

fn owned_tests(type_name: &proc_macro2::Ident) -> proc_macro2::TokenStream {
    quote! {
        #[cfg(test)]
        mod tests {
            use assert2::assert;
            use super::*;
            use crate::{Decode, Encode};
            use bytes::BytesMut;

            fn roundtrip(msg: &#type_name, v: i16) {
                let mut buf = BytesMut::new();
                msg.encode(&mut buf, v).unwrap();
                assert!(msg.encoded_len(v) == buf.len());
                let bytes = buf.freeze();
                let mut cur = &bytes[..];
                let decoded = #type_name::decode(&mut cur, v).unwrap();
                assert!(cur.is_empty());
                let mut reencoded = BytesMut::new();
                decoded.encode(&mut reencoded, v).unwrap();
                assert!(&reencoded[..] == &bytes[..]);
                let _ = default_json(v);
            }

            #[test]
            fn default_roundtrips_all_versions() {
                for v in MIN_VERSION..=MAX_VERSION {
                    roundtrip(&#type_name::default(), v);
                }
            }

            #[test]
            fn populated_roundtrips_all_versions() {
                for v in MIN_VERSION..=MAX_VERSION {
                    roundtrip(&#type_name::populated(v), v);
                }
            }
        }
    }
}

fn borrowed_tests(type_name: &proc_macro2::Ident) -> proc_macro2::TokenStream {
    quote! {
        #[cfg(test)]
        mod tests {
            use assert2::assert;
            use super::*;
            use crate::{DecodeBorrow, Encode};
            use bytes::BytesMut;

            fn check(msg_bytes: &bytes::Bytes, v: i16) {
                let mut cur: &[u8] = msg_bytes;
                let decoded = #type_name::decode_borrow(&mut cur, v).unwrap();
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
                    let msg = #type_name::default();
                    let mut buf = BytesMut::new();
                    msg.encode(&mut buf, v).unwrap();
                    check(&buf.freeze(), v);
                }
            }

            #[test]
            fn populated_roundtrips_all_versions() {
                for v in MIN_VERSION..=MAX_VERSION {
                    let msg = #type_name::populated(v);
                    let mut buf = BytesMut::new();
                    msg.encode(&mut buf, v).unwrap();
                    check(&buf.freeze(), v);
                }
            }
        }
    }
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
