//! Backward-compatibility classification for JSON Schema differences. SEED from
//! Confluent's behavior; the cp golden matrix (`compat_conformance`) is the
//! authority and re-tunes this table.

use super::diff::{Difference, Kind};

#[must_use]
#[allow(clippy::match_same_arms)]
pub fn is_backward_compatible(kind: &Kind) -> bool {
    match kind {
        // --- Type ---
        Kind::TypeNarrowed => false,
        Kind::TypeChanged => false,
        Kind::TypeExtended => true,

        // --- Properties ---
        Kind::PropertyRemovedFromClosedContentModel => false,
        Kind::PropertyAddedToOpenContentModel => true,
        Kind::PropertyRemovedFromOpenContentModel => true,
        Kind::PropertyAddedToClosedContentModel => true,

        // --- Required ---
        Kind::RequiredAttributeAdded => false,
        Kind::RequiredAttributeRemoved => true,

        // --- AdditionalProperties ---
        Kind::AdditionalPropertiesRemoved => false,
        Kind::AdditionalPropertiesAdded => true,

        // --- Enum / const ---
        // Narrowed = fewer allowed values = breaking for readers expecting old values
        Kind::EnumArrayNarrowed => false,
        // Extended = more allowed values = backward compatible
        Kind::EnumArrayExtended => true,
        // Changed = neither subset → breaking
        Kind::EnumArrayChanged => false,

        // --- Numeric: maximum (max decreased = tighter = breaking) ---
        Kind::MaximumAdded => false,  // new constraint added = tighter
        Kind::MaximumRemoved => true, // constraint removed = looser
        Kind::MaximumDecreased => false, // tighter
        Kind::MaximumIncreased => true, // looser

        // --- Numeric: minimum (min increased = tighter = breaking) ---
        Kind::MinimumAdded => false,
        Kind::MinimumRemoved => true,
        Kind::MinimumDecreased => true,  // looser
        Kind::MinimumIncreased => false, // tighter

        // --- ExclusiveMaximum ---
        Kind::ExclusiveMaximumAdded => false,
        Kind::ExclusiveMaximumRemoved => true,
        Kind::ExclusiveMaximumDecreased => false,
        Kind::ExclusiveMaximumIncreased => true,

        // --- ExclusiveMinimum ---
        Kind::ExclusiveMinimumAdded => false,
        Kind::ExclusiveMinimumRemoved => true,
        Kind::ExclusiveMinimumDecreased => true,
        Kind::ExclusiveMinimumIncreased => false,

        // --- MultipleOf: added/changed = tighter ---
        Kind::MultipleOfAdded => false,
        Kind::MultipleOfRemoved => true,
        Kind::MultipleOfChanged => false,

        // --- String: maxLength ---
        Kind::MaxLengthAdded => false,
        Kind::MaxLengthRemoved => true,
        Kind::MaxLengthDecreased => false,
        Kind::MaxLengthIncreased => true,

        // --- String: minLength ---
        Kind::MinLengthAdded => false,
        Kind::MinLengthRemoved => true,
        Kind::MinLengthDecreased => true,
        Kind::MinLengthIncreased => false,

        // --- String: pattern ---
        Kind::PatternAdded => false,
        Kind::PatternRemoved => true,
        Kind::PatternChanged => false,

        // --- Array: maxItems ---
        Kind::MaxItemsAdded => false,
        Kind::MaxItemsRemoved => true,
        Kind::MaxItemsDecreased => false,
        Kind::MaxItemsIncreased => true,

        // --- Array: minItems ---
        Kind::MinItemsAdded => false,
        Kind::MinItemsRemoved => true,
        Kind::MinItemsDecreased => true,
        Kind::MinItemsIncreased => false,

        // --- Array: additionalItems ---
        Kind::AdditionalItemsRemoved => false,
        Kind::AdditionalItemsAdded => true,

        // --- Object size: maxProperties ---
        Kind::MaxPropertiesAdded => false,
        Kind::MaxPropertiesRemoved => true,
        Kind::MaxPropertiesDecreased => false,
        Kind::MaxPropertiesIncreased => true,

        // --- Object size: minProperties ---
        Kind::MinPropertiesAdded => false,
        Kind::MinPropertiesRemoved => true,
        Kind::MinPropertiesDecreased => true,
        Kind::MinPropertiesIncreased => false,

        // --- Combinators ---
        Kind::CombinedTypeChanged => false,
        Kind::ProductTypeExtended => true,
        Kind::ProductTypeNarrowed => false,
        Kind::SumTypeExtended => true,
        Kind::SumTypeNarrowed => false,
        Kind::NotTypeExtended => true,
        Kind::NotTypeNarrowed => false,
        Kind::CombinedTypeSubschemasChanged => false,

        // --- $ref / dependencies / conditionals ---
        Kind::DependencyAdded => false,
        Kind::DependencyRemoved => true,
        Kind::ConditionalChanged => false,
    }
}

#[must_use]
pub fn messages(diffs: &[&Difference]) -> Vec<String> {
    diffs
        .iter()
        .map(|d| format!("{:?} at {}", d.kind, d.path))
        .collect()
}
