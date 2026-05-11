//! Generate the `mod.rs` files for `crates/protocol/src/{owned,borrowed}/`.

use std::fmt::Write;

use crate::emit::common::banner;
use crate::emit::wrappers::Flavor;
use crate::ir::MessageSpec;
use crate::name_conv;

/// Emit a `mod.rs` that declares one `pub mod` per active spec, sorted
/// alphabetically by snake-case module name.
///
/// Pass only the specs whose wrapper files actually exist (i.e., the curated
/// set in Task 2; expanded to all active schemas in Task 3).
#[must_use]
pub fn emit(specs: &[&MessageSpec], flavor: Flavor, schemas_version: &str) -> String {
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
    entries.sort();
    entries.dedup();
    for snake in &entries {
        writeln!(out, "pub mod {snake};").unwrap();
    }
    out
}
