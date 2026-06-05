//! Backward-compatibility classification for Protobuf differences. SEED values
//! from Confluent's behavior; the cp golden matrix (`compat_conformance`) is the
//! authority and re-tunes this table.

use super::diff::{Difference, Kind};

/// Is this difference backward-compatible (reader can still read writer's data)?
#[must_use]
pub fn is_backward_compatible(kind: &Kind) -> bool {
    match kind {
        Kind::FieldAdded
        | Kind::FieldRemoved
        | Kind::MessageAdded
        | Kind::MessageRemoved
        // Oneof rules (Task 2) — seed values, calibrated vs cp in Task 6.
        | Kind::OneofFieldMovedIn
        | Kind::OneofAdded => true,
        Kind::FieldScalarKindChanged { compatible_group } => *compatible_group,
        Kind::FieldKindChanged
        | Kind::FieldNamedTypeChanged
        | Kind::FieldLabelChanged
        | Kind::OneofFieldMovedOut
        | Kind::OneofRemoved => false,
    }
}

#[must_use]
pub fn messages(diffs: &[&Difference]) -> Vec<String> {
    diffs
        .iter()
        .map(|d| format!("{:?} at {}", d.kind, d.path))
        .collect()
}
