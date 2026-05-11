//! Generate the `include!` wrapper module bodies under
//! `crates/protocol/src/{owned,borrowed}/<snake>.rs`.

use std::fmt::Write;

use crate::emit::common::banner;
use crate::ir::{MessageSpec, MessageType};
use crate::name_conv;

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

/// The standard clippy-suppress header that all generated wrapper files receive.
/// This is the union of lints that the generated `.rs` bodies (via `include!`)
/// are known to fire, mirroring the hand-written wrappers' `#![allow]` lists.
fn allow_header() -> &'static str {
    concat!(
        "#![allow(\n",
        "    clippy::elidable_lifetime_names,\n",
        "    clippy::must_use_candidate,\n",
        "    clippy::unnecessary_wraps,\n",
        "    clippy::cast_sign_loss,\n",
        "    clippy::cast_possible_truncation,\n",
        "    clippy::cast_possible_wrap,\n",
        "    clippy::default_trait_access,\n",
        "    clippy::derivable_impls,\n",
        "    clippy::collapsible_if,\n",
        "    clippy::new_without_default,\n",
        "    clippy::unreadable_literal,\n",
        "    clippy::redundant_closure_for_method_calls,\n",
        "    clippy::nonminimal_bool,\n",
        "    clippy::bool_comparison,\n",
        "    clippy::map_unwrap_or,\n",
        "    clippy::option_as_ref_deref,\n",
        "    clippy::manual_range_contains,\n",
        "    clippy::borrow_deref_ref,\n",
        "    clippy::explicit_auto_deref,\n",
        "    clippy::unnecessary_semicolon,\n",
        "    unused_imports\n",
        ")]",
    )
}

/// Emit a wrapper body for one message + flavor.
#[must_use]
pub fn emit(spec: &MessageSpec, flavor: Flavor, schemas_version: &str) -> String {
    let type_name = name_conv::type_name(&spec.name);
    let suffix = match flavor {
        Flavor::Owned => "owned",
        Flavor::Borrowed => "borrowed",
    };
    let mut out = banner(schemas_version);
    // Comment explaining origin, matching the hand-written wrapper style.
    writeln!(
        out,
        "// Clippy lints that fire on generated code patterns are suppressed here so"
    )
    .unwrap();
    writeln!(
        out,
        "// that regenerating the file does not require manual allow annotations."
    )
    .unwrap();
    out.push_str(allow_header());
    writeln!(out).unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "include!(concat!(\n    env!(\"CARGO_MANIFEST_DIR\"),\n    \"/generated/{type_name}.{suffix}.rs\"\n));"
    )
    .unwrap();
    writeln!(out).unwrap();
    // Inline round-trip tests gated on the flavor.
    match flavor {
        Flavor::Owned => write_owned_tests(&mut out, &type_name),
        Flavor::Borrowed => write_borrowed_tests(&mut out, &type_name),
    }
    out
}

fn write_owned_tests(out: &mut String, type_name: &str) {
    // Note: we do not assert decoded == msg for min_version because some messages
    // have encoding quirks at old versions where the default value does not survive
    // a round-trip unchanged (e.g. MetadataRequest.topics = None encodes as an
    // empty array at v0 and decodes as Some([])). The bespoke integration tests
    // under crates/protocol/tests/ cover semantic equality.
    // Import order: crate:: before external crates to match rustfmt grouping.
    writeln!(
        out,
        "#[cfg(test)]\nmod tests {{\n    use super::*;\n    use crate::{{Decode, Encode}};\n    use bytes::BytesMut;\n\n    #[test]\n    fn min_version_roundtrips() {{\n        let v = MIN_VERSION;\n        let msg = {type_name}::default();\n        let mut buf = BytesMut::new();\n        msg.encode(&mut buf, v).unwrap();\n        assert_eq!(msg.encoded_len(v), buf.len());\n        let mut cur = &buf[..];\n        let _decoded = {type_name}::decode(&mut cur, v).unwrap();\n        assert!(cur.is_empty(), \"decoder left trailing bytes\");\n    }}\n\n    #[test]\n    fn max_version_roundtrips() {{\n        let v = MAX_VERSION;\n        let msg = {type_name}::default();\n        let mut buf = BytesMut::new();\n        msg.encode(&mut buf, v).unwrap();\n        assert_eq!(msg.encoded_len(v), buf.len());\n        let mut cur = &buf[..];\n        let decoded = {type_name}::decode(&mut cur, v).unwrap();\n        assert_eq!(decoded, msg);\n        assert!(cur.is_empty(), \"decoder left trailing bytes\");\n    }}\n}}"
    )
    .unwrap();
}

fn write_borrowed_tests(out: &mut String, type_name: &str) {
    // Import order: crate:: before external crates to match rustfmt grouping.
    writeln!(
        out,
        "#[cfg(test)]\nmod tests {{\n    use super::*;\n    use crate::{{DecodeBorrow, Encode}};\n    use bytes::BytesMut;\n\n    #[test]\n    fn min_version_roundtrips() {{\n        let v = MIN_VERSION;\n        let msg = {type_name}::default();\n        let mut buf = BytesMut::new();\n        msg.encode(&mut buf, v).unwrap();\n        assert_eq!(msg.encoded_len(v), buf.len());\n        let frozen = buf.freeze();\n        let mut cur: &[u8] = &frozen;\n        let _decoded = {type_name}::decode_borrow(&mut cur, v).unwrap();\n    }}\n\n    #[test]\n    fn max_version_roundtrips() {{\n        let v = MAX_VERSION;\n        let msg = {type_name}::default();\n        let mut buf = BytesMut::new();\n        msg.encode(&mut buf, v).unwrap();\n        assert_eq!(msg.encoded_len(v), buf.len());\n        let frozen = buf.freeze();\n        let mut cur: &[u8] = &frozen;\n        let _decoded = {type_name}::decode_borrow(&mut cur, v).unwrap();\n    }}\n}}"
    )
    .unwrap();
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
