include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/api_key.rs"));

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn all_keys_unique() {
        let mut seen = std::collections::HashSet::new();
        for k in ApiKey::ALL {
            assert2::assert!(seen.insert(*k as i16));
        }
    }

    #[test]
    fn from_i16_round_trip() {
        for k in ApiKey::ALL {
            assert2::assert!(ApiKey::from_i16(*k as i16) == Some(*k));
        }
        for (_case, raw) in [("negative", -1), ("unknown positive", 9999)] {
            assert2::assert!(ApiKey::from_i16(raw) == None);
        }
    }
}
