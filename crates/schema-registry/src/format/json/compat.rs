//! Backward-compatibility classification for JSON Schema differences. SEED from
//! Confluent's behavior; the cp golden matrix (`compat_conformance`) is the
//! authority and re-tunes this table.

use super::diff::{Difference, Kind};

#[must_use]
pub fn is_backward_compatible(kind: &Kind) -> bool {
    matches!(
        kind,
        // --- Type ---
        | Kind::TypeExtended

        // --- Properties ---
        // cp is authority: adding a property to an open content model is
        // backward-INcompatible (the reader gains a property the writer's data
        // lacks); removing one is compatible. (add_prop_open BACKWARD=false /
        // remove_prop_open FORWARD=false in the cp golden matrix.)
        | Kind::PropertyRemovedFromOpenContentModel
        | Kind::PropertyAddedToClosedContentModel

        // --- Required ---
        | Kind::RequiredAttributeRemoved

        // --- AdditionalProperties ---
        | Kind::AdditionalPropertiesAdded

        // --- Enum / const ---
        // Narrowed = fewer allowed values = breaking for readers expecting old values
        // Extended = more allowed values = backward compatible
        | Kind::EnumArrayExtended
        // Changed = neither subset → breaking

        // --- Numeric: maximum (max decreased = tighter = breaking) ---
        | Kind::MaximumRemoved
        | Kind::MaximumIncreased

        // --- Numeric: minimum (min increased = tighter = breaking) ---
        | Kind::MinimumRemoved
        | Kind::MinimumDecreased

        // --- ExclusiveMaximum ---
        | Kind::ExclusiveMaximumRemoved
        | Kind::ExclusiveMaximumIncreased

        // --- ExclusiveMinimum ---
        | Kind::ExclusiveMinimumRemoved
        | Kind::ExclusiveMinimumDecreased

        // --- MultipleOf: added/changed = tighter ---
        | Kind::MultipleOfRemoved

        // --- String: maxLength ---
        | Kind::MaxLengthRemoved
        | Kind::MaxLengthIncreased

        // --- String: minLength ---
        | Kind::MinLengthRemoved
        | Kind::MinLengthDecreased

        // --- String: pattern ---
        | Kind::PatternRemoved

        // --- Array: maxItems ---
        | Kind::MaxItemsRemoved
        | Kind::MaxItemsIncreased

        // --- Array: minItems ---
        | Kind::MinItemsRemoved
        | Kind::MinItemsDecreased

        // --- Array: additionalItems ---
        | Kind::AdditionalItemsAdded

        // --- Object size: maxProperties ---
        | Kind::MaxPropertiesRemoved
        | Kind::MaxPropertiesIncreased

        // --- Object size: minProperties ---
        | Kind::MinPropertiesRemoved
        | Kind::MinPropertiesDecreased

        // --- Combinators ---
        | Kind::ProductTypeExtended
        | Kind::SumTypeExtended
        | Kind::NotTypeExtended

        // --- $ref / dependencies / conditionals ---
        // cp is authority: cp's json.diff treats both adding and removing a
        // dependency/dependentRequired as compatible in either direction
        // (dependency_added BACKWARD=FORWARD=true in the cp golden matrix).
        | Kind::DependencyAdded
        | Kind::DependencyRemoved
    )
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
    }

    #[test]
    fn representative_verdicts_match() {
        for (kind, want) in [
            (Kind::TypeExtended, true),
            (Kind::TypeNarrowed, false),
            (Kind::TypeChanged, false),
            (Kind::PropertyAddedToOpenContentModel, false),
            (Kind::PropertyRemovedFromOpenContentModel, true),
            (Kind::PropertyAddedToClosedContentModel, true),
            (Kind::PropertyRemovedFromClosedContentModel, false),
            (Kind::RequiredAttributeAdded, false),
            (Kind::RequiredAttributeRemoved, true),
            (Kind::AdditionalPropertiesRemoved, false),
            (Kind::AdditionalPropertiesAdded, true),
            (Kind::EnumArrayNarrowed, false),
            (Kind::EnumArrayExtended, true),
            (Kind::EnumArrayChanged, false),
            (Kind::MaximumAdded, false),
            (Kind::MaximumRemoved, true),
            (Kind::MaximumDecreased, false),
            (Kind::MaximumIncreased, true),
            (Kind::MinimumAdded, false),
            (Kind::MinimumRemoved, true),
            (Kind::MinimumDecreased, true),
            (Kind::MinimumIncreased, false),
            (Kind::MultipleOfAdded, false),
            (Kind::MultipleOfRemoved, true),
            (Kind::MultipleOfChanged, false),
            (Kind::MaxLengthAdded, false),
            (Kind::MaxLengthRemoved, true),
            (Kind::PatternAdded, false),
            (Kind::PatternRemoved, true),
            (Kind::PatternChanged, false),
            (Kind::MaxItemsAdded, false),
            (Kind::MaxItemsRemoved, true),
            (Kind::MinItemsAdded, false),
            (Kind::MinItemsRemoved, true),
            (Kind::AdditionalItemsRemoved, false),
            (Kind::AdditionalItemsAdded, true),
            (Kind::MaxPropertiesAdded, false),
            (Kind::MaxPropertiesRemoved, true),
            (Kind::MinPropertiesAdded, false),
            (Kind::MinPropertiesRemoved, true),
            (Kind::CombinedTypeChanged, false),
            (Kind::ProductTypeExtended, true),
            (Kind::ProductTypeNarrowed, false),
            (Kind::SumTypeExtended, true),
            (Kind::SumTypeNarrowed, false),
            (Kind::NotTypeExtended, true),
            (Kind::NotTypeNarrowed, false),
            (Kind::CombinedTypeSubschemasChanged, false),
            (Kind::DependencyAdded, true),
            (Kind::DependencyRemoved, true),
            (Kind::ConditionalChanged, false),
        ] {
            assert2::assert!(is_backward_compatible(&kind) == want);
        }
    }
}
