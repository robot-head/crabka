//! Generate the `mod.rs` files for `crates/protocol/src/{owned,borrowed}/`.

use std::fmt::Write;

use crate::emit::common::banner;
use crate::emit::wrappers::Flavor;
use crate::ir::MessageSpec;
use crate::name_conv;

/// Emit a `mod.rs` that declares one `pub mod` per active spec, sorted
/// alphabetically by snake-case module name.
///
/// Pass only the specs whose wrapper files actually exist (i.e., the set
/// covering all active schemas).
/// If `has_common` is true, a `pub mod common;` entry is also emitted.
#[must_use]
pub fn emit(
    specs: &[&MessageSpec],
    flavor: Flavor,
    schemas_version: &str,
    has_common: bool,
) -> String {
    let flavor_comment = match flavor {
        Flavor::Owned => "Owned (heap-allocated) message types.",
        Flavor::Borrowed => "Borrowed-flavor generated message types.",
    };
    let mut out = banner(schemas_version);
    writeln!(out, "//! {flavor_comment}").unwrap();
    writeln!(out).unwrap();
    // Collect snake-case names, sort, deduplicate.
    let mut entries: Vec<String> = specs
        .iter()
        .filter(|s| !s.valid_versions.is_empty())
        .map(|s| name_conv::module_name(&s.name))
        .collect();
    // Include `common` in the sorted list if this flavor has a common submodule.
    if has_common {
        entries.push("common".to_string());
    }
    entries.sort();
    entries.dedup();
    for snake in &entries {
        writeln!(out, "pub mod {snake};").unwrap();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{MessageSpec, MessageType, VersionRange};

    fn make_spec(name: &str, min: i16, max: i16, ty: MessageType) -> MessageSpec {
        MessageSpec {
            name: name.to_string(),
            message_type: ty,
            api_key: None,
            valid_versions: VersionRange { min, max },
            flexible_versions: crate::ir::FlexibleVersions::None,
            fields: vec![],
            common_structs: vec![],
        }
    }

    #[test]
    fn owned_mod_rs_sorted_alphabetically() {
        let specs = [
            make_spec("ProduceRequest", 0, 10, MessageType::Request),
            make_spec("ApiVersionsRequest", 0, 3, MessageType::Request),
            make_spec("MetadataRequest", 0, 13, MessageType::Request),
        ];
        let refs: Vec<&MessageSpec> = specs.iter().collect();
        let out = emit(&refs, Flavor::Owned, "test-sha", false);
        // Must contain all three, in alphabetical order.
        let api_pos = out.find("pub mod api_versions_request").unwrap();
        let meta_pos = out.find("pub mod metadata_request").unwrap();
        let prod_pos = out.find("pub mod produce_request").unwrap();
        assert!(api_pos < meta_pos, "api before metadata");
        assert!(meta_pos < prod_pos, "metadata before produce");
    }

    #[test]
    fn skips_specs_with_empty_valid_versions() {
        let specs = [
            make_spec("ActiveRequest", 0, 5, MessageType::Request),
            make_spec("RemovedRequest", i16::MAX, i16::MIN, MessageType::Request),
        ];
        let refs: Vec<&MessageSpec> = specs.iter().collect();
        let out = emit(&refs, Flavor::Owned, "test-sha", false);
        assert!(out.contains("pub mod active_request"));
        assert!(!out.contains("pub mod removed_request"));
    }

    #[test]
    fn borrowed_flavor_has_correct_doc_comment() {
        let refs: Vec<&MessageSpec> = vec![];
        let out = emit(&refs, Flavor::Borrowed, "sha123", false);
        assert!(out.contains("//! Borrowed-flavor generated message types."));
    }
}
