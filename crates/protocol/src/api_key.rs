// Clippy lints that fire on generated code patterns are suppressed here so
// that regenerating the file does not require manual allow annotations.
#![allow(
    clippy::elidable_lifetime_names,
    clippy::must_use_candidate,
    clippy::unnecessary_wraps,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::default_trait_access,
    clippy::derivable_impls,
    clippy::collapsible_if,
    clippy::new_without_default,
    clippy::unreadable_literal,
    clippy::redundant_closure_for_method_calls,
    clippy::nonminimal_bool,
    clippy::bool_comparison,
    clippy::map_unwrap_or,
    clippy::option_as_ref_deref,
    clippy::manual_range_contains
)]

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/api_key.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_keys_unique() {
        let mut seen = std::collections::HashSet::new();
        for k in ApiKey::ALL {
            assert!(seen.insert(*k as i16), "duplicate: {k:?}");
        }
    }

    #[test]
    fn from_i16_round_trip() {
        for k in ApiKey::ALL {
            assert_eq!(ApiKey::from_i16(*k as i16), Some(*k));
        }
        assert_eq!(ApiKey::from_i16(-1), None);
        assert_eq!(ApiKey::from_i16(9999), None);
    }
}
