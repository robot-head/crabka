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
        // cp is authority: adding a property to an open content model is
        // backward-INcompatible (the reader gains a property the writer's data
        // lacks); removing one is compatible. (add_prop_open BACKWARD=false /
        // remove_prop_open FORWARD=false in the cp golden matrix.)
        Kind::PropertyAddedToOpenContentModel => false,
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
        // cp is authority: cp's json.diff treats both adding and removing a
        // dependency/dependentRequired as compatible in either direction
        // (dependency_added BACKWARD=FORWARD=true in the cp golden matrix).
        Kind::DependencyAdded => true,
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

#[cfg(test)]
mod tests {
    use super::is_backward_compatible;
    use crate::format::json::diff::Kind;

    /// Exercise every `Kind` match arm so compat.rs is fully covered.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn every_kind_is_classified() {
        let all = [
            // --- Type ---
            Kind::TypeNarrowed,
            Kind::TypeExtended,
            Kind::TypeChanged,
            // --- Properties ---
            Kind::PropertyAddedToOpenContentModel,
            Kind::PropertyRemovedFromOpenContentModel,
            Kind::PropertyAddedToClosedContentModel,
            Kind::PropertyRemovedFromClosedContentModel,
            // --- Required ---
            Kind::RequiredAttributeAdded,
            Kind::RequiredAttributeRemoved,
            // --- AdditionalProperties ---
            Kind::AdditionalPropertiesRemoved,
            Kind::AdditionalPropertiesAdded,
            // --- Enum / const ---
            Kind::EnumArrayNarrowed,
            Kind::EnumArrayExtended,
            Kind::EnumArrayChanged,
            // --- Numeric: maximum ---
            Kind::MaximumAdded,
            Kind::MaximumRemoved,
            Kind::MaximumDecreased,
            Kind::MaximumIncreased,
            // --- Numeric: minimum ---
            Kind::MinimumAdded,
            Kind::MinimumRemoved,
            Kind::MinimumDecreased,
            Kind::MinimumIncreased,
            // --- ExclusiveMaximum ---
            Kind::ExclusiveMaximumAdded,
            Kind::ExclusiveMaximumRemoved,
            Kind::ExclusiveMaximumDecreased,
            Kind::ExclusiveMaximumIncreased,
            // --- ExclusiveMinimum ---
            Kind::ExclusiveMinimumAdded,
            Kind::ExclusiveMinimumRemoved,
            Kind::ExclusiveMinimumDecreased,
            Kind::ExclusiveMinimumIncreased,
            // --- MultipleOf ---
            Kind::MultipleOfAdded,
            Kind::MultipleOfRemoved,
            Kind::MultipleOfChanged,
            // --- String: maxLength ---
            Kind::MaxLengthAdded,
            Kind::MaxLengthRemoved,
            Kind::MaxLengthDecreased,
            Kind::MaxLengthIncreased,
            // --- String: minLength ---
            Kind::MinLengthAdded,
            Kind::MinLengthRemoved,
            Kind::MinLengthDecreased,
            Kind::MinLengthIncreased,
            // --- String: pattern ---
            Kind::PatternAdded,
            Kind::PatternRemoved,
            Kind::PatternChanged,
            // --- Array: maxItems ---
            Kind::MaxItemsAdded,
            Kind::MaxItemsRemoved,
            Kind::MaxItemsDecreased,
            Kind::MaxItemsIncreased,
            // --- Array: minItems ---
            Kind::MinItemsAdded,
            Kind::MinItemsRemoved,
            Kind::MinItemsDecreased,
            Kind::MinItemsIncreased,
            // --- Array: additionalItems ---
            Kind::AdditionalItemsRemoved,
            Kind::AdditionalItemsAdded,
            // --- Object size: maxProperties ---
            Kind::MaxPropertiesAdded,
            Kind::MaxPropertiesRemoved,
            Kind::MaxPropertiesDecreased,
            Kind::MaxPropertiesIncreased,
            // --- Object size: minProperties ---
            Kind::MinPropertiesAdded,
            Kind::MinPropertiesRemoved,
            Kind::MinPropertiesDecreased,
            Kind::MinPropertiesIncreased,
            // --- Combinators ---
            Kind::CombinedTypeChanged,
            Kind::ProductTypeExtended,
            Kind::ProductTypeNarrowed,
            Kind::SumTypeExtended,
            Kind::SumTypeNarrowed,
            Kind::NotTypeExtended,
            Kind::NotTypeNarrowed,
            Kind::CombinedTypeSubschemasChanged,
            // --- $ref / dependencies / conditionals ---
            Kind::DependencyAdded,
            Kind::DependencyRemoved,
            Kind::ConditionalChanged,
        ];
        // Every call hits its match arm (coverage goal).
        for k in &all {
            let _ = is_backward_compatible(k);
        }
        // Spot-check a representative selection of known verdicts.
        assert!(is_backward_compatible(&Kind::TypeExtended));
        assert!(!is_backward_compatible(&Kind::TypeNarrowed));
        assert!(!is_backward_compatible(&Kind::TypeChanged));
        assert!(!is_backward_compatible(
            &Kind::PropertyAddedToOpenContentModel
        ));
        assert!(is_backward_compatible(
            &Kind::PropertyRemovedFromOpenContentModel
        ));
        assert!(is_backward_compatible(
            &Kind::PropertyAddedToClosedContentModel
        ));
        assert!(!is_backward_compatible(
            &Kind::PropertyRemovedFromClosedContentModel
        ));
        assert!(!is_backward_compatible(&Kind::RequiredAttributeAdded));
        assert!(is_backward_compatible(&Kind::RequiredAttributeRemoved));
        assert!(!is_backward_compatible(&Kind::AdditionalPropertiesRemoved));
        assert!(is_backward_compatible(&Kind::AdditionalPropertiesAdded));
        assert!(!is_backward_compatible(&Kind::EnumArrayNarrowed));
        assert!(is_backward_compatible(&Kind::EnumArrayExtended));
        assert!(!is_backward_compatible(&Kind::EnumArrayChanged));
        assert!(!is_backward_compatible(&Kind::MaximumAdded));
        assert!(is_backward_compatible(&Kind::MaximumRemoved));
        assert!(!is_backward_compatible(&Kind::MaximumDecreased));
        assert!(is_backward_compatible(&Kind::MaximumIncreased));
        assert!(!is_backward_compatible(&Kind::MinimumAdded));
        assert!(is_backward_compatible(&Kind::MinimumRemoved));
        assert!(is_backward_compatible(&Kind::MinimumDecreased));
        assert!(!is_backward_compatible(&Kind::MinimumIncreased));
        assert!(!is_backward_compatible(&Kind::MultipleOfAdded));
        assert!(is_backward_compatible(&Kind::MultipleOfRemoved));
        assert!(!is_backward_compatible(&Kind::MultipleOfChanged));
        assert!(!is_backward_compatible(&Kind::MaxLengthAdded));
        assert!(is_backward_compatible(&Kind::MaxLengthRemoved));
        assert!(!is_backward_compatible(&Kind::PatternAdded));
        assert!(is_backward_compatible(&Kind::PatternRemoved));
        assert!(!is_backward_compatible(&Kind::PatternChanged));
        assert!(!is_backward_compatible(&Kind::MaxItemsAdded));
        assert!(is_backward_compatible(&Kind::MaxItemsRemoved));
        assert!(!is_backward_compatible(&Kind::MinItemsAdded));
        assert!(is_backward_compatible(&Kind::MinItemsRemoved));
        assert!(!is_backward_compatible(&Kind::AdditionalItemsRemoved));
        assert!(is_backward_compatible(&Kind::AdditionalItemsAdded));
        assert!(!is_backward_compatible(&Kind::MaxPropertiesAdded));
        assert!(is_backward_compatible(&Kind::MaxPropertiesRemoved));
        assert!(!is_backward_compatible(&Kind::MinPropertiesAdded));
        assert!(is_backward_compatible(&Kind::MinPropertiesRemoved));
        assert!(!is_backward_compatible(&Kind::CombinedTypeChanged));
        assert!(is_backward_compatible(&Kind::ProductTypeExtended));
        assert!(!is_backward_compatible(&Kind::ProductTypeNarrowed));
        assert!(is_backward_compatible(&Kind::SumTypeExtended));
        assert!(!is_backward_compatible(&Kind::SumTypeNarrowed));
        assert!(is_backward_compatible(&Kind::NotTypeExtended));
        assert!(!is_backward_compatible(&Kind::NotTypeNarrowed));
        assert!(!is_backward_compatible(
            &Kind::CombinedTypeSubschemasChanged
        ));
        assert!(is_backward_compatible(&Kind::DependencyAdded));
        assert!(is_backward_compatible(&Kind::DependencyRemoved));
        assert!(!is_backward_compatible(&Kind::ConditionalChanged));
    }
}
