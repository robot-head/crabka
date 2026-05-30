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
#[must_use]
pub fn allow_header() -> &'static str {
    concat!(
        "#![allow(\n",
        "    clippy::absurd_extreme_comparisons,\n",
        "    clippy::double_comparisons,\n",
        "    clippy::elidable_lifetime_names,\n",
        "    clippy::manual_string_new,\n",
        "    clippy::must_use_candidate,\n",
        "    clippy::unnecessary_wraps,\n",
        "    clippy::cast_sign_loss,\n",
        "    clippy::cast_possible_truncation,\n",
        "    clippy::cast_possible_wrap,\n",
        "    clippy::default_trait_access,\n",
        "    clippy::derivable_impls,\n",
        "    clippy::collapsible_if,\n",
        "    clippy::too_many_lines,\n",
        "    clippy::field_reassign_with_default,\n",
        "    unused_mut,\n",
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
        "    clippy::semicolon_if_nothing_returned,\n",
        "    unused_imports,\n",
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
    let path_prefix = match namespace {
        None => String::new(),
        Some(ns) => format!("{ns}/"),
    };
    writeln!(
        out,
        "include!(concat!(\n    env!(\"CARGO_MANIFEST_DIR\"),\n    \"/generated/{path_prefix}{type_name}.{suffix}.rs\"\n));"
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
    // Each message is round-tripped across EVERY supported version, in two
    // shapes: `default()` (empty collections / zero scalars) and `populated()`
    // (one element per array, recursively populated nested structs, non-default
    // scalars). The populated form drives the array-element, nested-struct, and
    // tagged-field encode/decode paths that an empty default never reaches.
    //
    // Rather than assert structural equality (which has version-dependent quirks,
    // e.g. `None` vs `Some([])` for nullable arrays at old versions), the helper
    // re-encodes the decoded value and asserts byte-equality with the original
    // encoding — a stronger, quirk-free invariant. Bespoke integration tests
    // under crates/protocol/tests/ cover semantic equality.
    writeln!(
        out,
        "#[cfg(test)]
mod tests {{
    use super::*;
    use crate::{{Decode, Encode}};
    use bytes::BytesMut;

    fn roundtrip(msg: &{type_name}, v: i16) {{
        let mut buf = BytesMut::new();
        msg.encode(&mut buf, v).unwrap();
        assert_eq!(msg.encoded_len(v), buf.len());
        let bytes = buf.freeze();
        let mut cur = &bytes[..];
        let decoded = {type_name}::decode(&mut cur, v).unwrap();
        assert!(cur.is_empty());
        let mut reencoded = BytesMut::new();
        decoded.encode(&mut reencoded, v).unwrap();
        assert_eq!(&reencoded[..], &bytes[..]);
        // Exercise the JVM-oracle default-JSON builder for this version.
        let _ = default_json(v);
    }}

    #[test]
    fn default_roundtrips_all_versions() {{
        for v in MIN_VERSION..=MAX_VERSION {{
            roundtrip(&{type_name}::default(), v);
        }}
    }}

    #[test]
    fn populated_roundtrips_all_versions() {{
        for v in MIN_VERSION..=MAX_VERSION {{
            roundtrip(&{type_name}::populated(v), v);
        }}
    }}
}}"
    )
    .unwrap();
}

fn write_borrowed_tests(out: &mut String, type_name: &str) {
    // See `write_owned_tests` for rationale. The borrowed flavor is exercised by
    // encoding a populated/default value, decoding it zero-copy, then re-encoding
    // the decoded borrow and asserting byte-equality.
    writeln!(
        out,
        "#[cfg(test)]
mod tests {{
    use super::*;
    use crate::{{DecodeBorrow, Encode}};
    use bytes::BytesMut;

    fn check(msg_bytes: &bytes::Bytes, v: i16) {{
        let mut cur: &[u8] = msg_bytes;
        let decoded = {type_name}::decode_borrow(&mut cur, v).unwrap();
        assert!(cur.is_empty());
        assert_eq!(decoded.encoded_len(v), msg_bytes.len());
        let mut reencoded = BytesMut::new();
        decoded.encode(&mut reencoded, v).unwrap();
        assert_eq!(&reencoded[..], &msg_bytes[..]);
        // Exercise the zero-copy -> owned conversion, then confirm the owned
        // value still encodes to the same bytes.
        let owned = decoded.to_owned();
        let mut owned_buf = BytesMut::new();
        owned.encode(&mut owned_buf, v).unwrap();
        assert_eq!(&owned_buf[..], &msg_bytes[..]);
    }}

    #[test]
    fn default_roundtrips_all_versions() {{
        for v in MIN_VERSION..=MAX_VERSION {{
            let msg = {type_name}::default();
            let mut buf = BytesMut::new();
            msg.encode(&mut buf, v).unwrap();
            check(&buf.freeze(), v);
        }}
    }}

    #[test]
    fn populated_roundtrips_all_versions() {{
        for v in MIN_VERSION..=MAX_VERSION {{
            let msg = {type_name}::populated(v);
            let mut buf = BytesMut::new();
            msg.encode(&mut buf, v).unwrap();
            check(&buf.freeze(), v);
        }}
    }}
}}"
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
