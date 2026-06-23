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

    /// FNV-1a 64-bit hash over canonical length-prefixed `name`/`value` entries.
    ///
    /// Each name and value is preceded by its byte length (`u64` little-endian)
    /// so the encoding is injective: a name or value containing `=` or a newline
    /// cannot be re-parsed across the field boundary, which a bare `name=value\n`
    /// separator encoding would allow (e.g. `a=b\nc` vs `a` with value `b\nc`).
    /// Profile labels are user-controlled, so this collision is reachable; the
    /// length prefix closes it. `BTreeMap` keeps names sorted, so the hash is
    /// independent of insertion order. Greenfield, so no persisted fingerprints
    /// depend on the old encoding.
    #[must_use]
    pub fn fingerprint(&self) -> SeriesFingerprint {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET;
        for (name, value) in &self.0 {
            for byte in (name.len() as u64)
                .to_le_bytes()
                .iter()
                .copied()
                .chain(name.as_bytes().iter().copied())
                .chain((value.len() as u64).to_le_bytes().iter().copied())
                .chain(value.as_bytes().iter().copied())
            {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(PRIME);
            }
        }
        hash
    }
}

impl FromIterator<(String, String)> for Labels {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
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
    fn fingerprint_is_injective_across_delimiter_ambiguity() {
        // A bare `name=value\n` encoding flattens both of these label sets to the
        // same byte string `a=b=c\n`, colliding two distinct series. The length
        // prefix makes the encoding injective, so the fingerprints must differ.
        let mut a = Labels::new();
        a.insert("a", "b=c");
        let mut b = Labels::new();
        b.insert("a=b", "c");
        assert!(a.fingerprint() != b.fingerprint());

        // The same ambiguity via an embedded newline (reachable through
        // user-controlled profile label values).
        let mut c = Labels::new();
        c.insert("x", "y");
        c.insert("z", "");
        let mut d = Labels::new();
        d.insert("x", "y\nz=");
        assert!(c.fingerprint() != d.fingerprint());
    }

    #[test]
    fn get_and_iter_round_trip() {
        let mut l = Labels::new();
        assert!(l.is_empty());
        l.insert("app", "api");
        assert!(l.get("app") == Some("api"));
        assert!(l.get("missing") == None);
        assert!(l.len() == 1);
        let pairs = l
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert!(pairs == vec![("app", "api")]);
    }
}
