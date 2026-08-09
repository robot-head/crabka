//! Backward-compatibility classification for Protobuf differences. CALIBRATED
//! against the golden cp-schema-registry matrix, which is `compat_conformance` →
//! `engine_matches_cp_protobuf_verdicts`, 88 cases from real cp 7.4.0. cp is the
//! authority, and every verdict below is the one cp emits.
//!
//! Direction note: `diff::compare(original, update)` is called with
//! `original = writer` for BACKWARD and `original = reader` for FORWARD, because
//! the engine swaps the pair per direction. An asymmetric rule is therefore
//! encoded by a different classification of the two mirror `Kind`s. For example,
//! a new message is BACKWARD-ok but FORWARD-broken, so `MessageAdded` is true
//! and `MessageRemoved` is false. For BACKWARD a new reader message appears as
//! `MessageAdded`. For FORWARD the same schema pair diffs the other way and
//! appears as `MessageRemoved`.

use super::diff::{Difference, Kind};

/// Is this difference backward-compatible (reader can still read writer's data)?
#[must_use]
pub fn is_backward_compatible(kind: &Kind) -> bool {
    match kind {
        Kind::FieldAdded
        | Kind::FieldRemoved
        // A message present only on the reader side is fine for a reader (cp:
        // message_added is BACKWARD-compatible). The FORWARD case diffs to
        // `MessageRemoved`, which is incompatible below.
        | Kind::MessageAdded
        // A field moved OUT of a oneof: cp says BACKWARD-compatible (FORWARD diffs
        // to `OneofFieldMovedIn`, incompatible below).
        | Kind::OneofFieldMovedOut
        // Adding or removing a oneof *declaration* is itself compatible: the wire
        // encoding of the member fields is unchanged. The only incompatible oneof
        // change is grouping ≥2 formerly-independent fields together, which the
        // diff reports as `OneofFieldMovedIn` (see `diff::compare_field`).
        | Kind::OneofAdded
        | Kind::OneofRemoved
        | Kind::ReservedNumberAdded
        | Kind::ReservedNameAdded
        | Kind::EnumConstAdded
        | Kind::EnumConstRemoved
        | Kind::EnumAdded
        | Kind::EnumRemoved
        // Changing the proto package is compatible in cp (the package is not part
        // of the wire encoding).
        | Kind::PackageChanged
        // singular ↔ repeated of the same type is compatible in cp (a reader can
        // decode a single value as a length-1 repeated and vice versa).
        | Kind::FieldLabelChanged => true,
        Kind::FieldScalarKindChanged { compatible_group } => *compatible_group,
        Kind::FieldKindChanged
        | Kind::FieldNamedTypeChanged
        // A field moved INTO a oneof (grouping ≥2 formerly-independent fields) is
        // BACKWARD-incompatible in cp (the mirror of `OneofFieldMovedOut`).
        | Kind::OneofFieldMovedIn
        // A message removed from the reader is BACKWARD-incompatible (mirror of
        // `MessageAdded`).
        | Kind::MessageRemoved => false,
    }
}

#[must_use]
pub fn messages(diffs: &[&Difference]) -> Vec<String> {
    diffs
        .iter()
        .map(|d| format!("{:?} at {}", d.kind, d.path))
        .collect()
}
