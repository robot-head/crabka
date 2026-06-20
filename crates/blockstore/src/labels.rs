//! Series labels and stable fingerprints.

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

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

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

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn fingerprint(&self) -> SeriesFingerprint {
        let mut hash = FNV_OFFSET;
        for (name, value) in &self.0 {
            fnv_update(&mut hash, name.as_bytes());
            fnv_update(&mut hash, b"\xff");
            fnv_update(&mut hash, value.as_bytes());
            fnv_update(&mut hash, b"\x00");
        }
        hash
    }
}

impl FromIterator<(String, String)> for Labels {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

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
    fn get_and_iter_round_trip() {
        let mut labels = Labels::new();
        labels.insert("app", "api");
        assert!(labels.get("app") == Some("api"));
        assert!(labels.get("missing") == None);
        assert!(labels.len() == 1);
        let pairs: Vec<_> = labels.iter().collect();
        assert!(pairs == vec![("app", "api")]);
    }
}
