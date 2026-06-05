//! Backward-compatibility classification for JSON Schema differences. SEED from
//! Confluent's behavior; the cp golden matrix (`compat_conformance`) is the
//! authority and re-tunes this table.

use super::diff::{Difference, Kind};

#[must_use]
pub fn is_backward_compatible(kind: &Kind) -> bool {
    match kind {
        Kind::TypeNarrowed
        | Kind::TypeChanged
        | Kind::PropertyRemovedFromClosedContentModel
        | Kind::RequiredAttributeAdded
        | Kind::AdditionalPropertiesRemoved => false,

        Kind::TypeExtended
        | Kind::PropertyAddedToOpenContentModel
        | Kind::PropertyRemovedFromOpenContentModel
        | Kind::PropertyAddedToClosedContentModel
        | Kind::RequiredAttributeRemoved
        | Kind::AdditionalPropertiesAdded => true,
    }
}

#[must_use]
pub fn messages(diffs: &[&Difference]) -> Vec<String> {
    diffs
        .iter()
        .map(|d| format!("{:?} at {}", d.kind, d.path))
        .collect()
}
