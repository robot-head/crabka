use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 64-bit fingerprint of a label set. Stable across process runs.
pub type SeriesFingerprint = u64;

/// An ordered set of `name -> value` labels identifying a series.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Labels(BTreeMap<String, String>);

impl Labels {
    #[must_use]
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Build a label set from an iterator of `(name, value)` pairs.
    #[must_use]
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut labels = Self::new();
        for (name, value) in pairs {
            labels.insert(name, value);
        }
        labels
    }

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.0.iter()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// FNV-1a 64-bit hash over canonical `name=value\n` entries. `BTreeMap`
    /// keeps names sorted, so the hash is independent of insertion order.
    #[must_use]
    pub fn fingerprint(&self) -> SeriesFingerprint {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET;
        for (name, value) in &self.0 {
            hash_bytes(&mut hash, name.as_bytes(), PRIME);
            hash_bytes(&mut hash, b"=", PRIME);
            hash_bytes(&mut hash, value.as_bytes(), PRIME);
            hash_bytes(&mut hash, b"\n", PRIME);
        }
        hash
    }
}

impl FromIterator<(String, String)> for Labels {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

fn hash_bytes(hash: &mut u64, bytes: &[u8], prime: u64) {
    for &b in bytes {
        *hash ^= u64::from(b);
        *hash = hash.wrapping_mul(prime);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn fingerprint_is_order_independent() {
        let mut a = Labels::new();
        a.insert("app", "api");
        a.insert("env", "prod");
        let mut b = Labels::new();
        b.insert("env", "prod");
        b.insert("app", "api");
        assert!(a.fingerprint() == b.fingerprint());
    }

    #[test]
    fn fingerprint_distinguishes_values() {
        let mut a = Labels::new();
        a.insert("app", "api");
        let mut b = Labels::new();
        b.insert("app", "web");
        assert!(a.fingerprint() != b.fingerprint());
    }

    #[test]
    fn fingerprint_matches_reference_fnv1a() {
        // Pin the exact FNV-1a 64-bit hash so swapping the `^=` in `hash_bytes`
        // for `|=` (which would change every byte mix) makes this fail.
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut a = Labels::new();
        a.insert("app", "api");

        // Reference: hash each canonical `name=value\n` byte with FNV-1a.
        let mut want = OFFSET;
        for &b in b"app=api\n" {
            want ^= u64::from(b);
            want = want.wrapping_mul(PRIME);
        }
        assert!(a.fingerprint() == want);

        // The `|=` variant produces a different digest for the same input.
        let mut or_variant = OFFSET;
        for &b in b"app=api\n" {
            or_variant |= u64::from(b);
            or_variant = or_variant.wrapping_mul(PRIME);
        }
        assert!(want != or_variant);
    }

    #[test]
    fn get_and_iter_round_trip() {
        let mut l = Labels::new();
        l.insert("app", "api");
        assert!(l.get("app") == Some("api"));
        assert!(l.get("missing") == None);
        assert!(l.len() == 1);
    }
}
