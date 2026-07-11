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

    #[test]
    fn fingerprint_is_order_independent() {
        let mut a = Labels::new();
        a.insert("app", "api");
        a.insert("env", "prod");
        let mut b = Labels::new();
        b.insert("env", "prod");
        b.insert("app", "api");
        assert2::assert!(a.fingerprint() == b.fingerprint());
    }

    #[test]
    fn fingerprint_distinguishes_values() {
        let mut a = Labels::new();
        a.insert("app", "api");
        let mut b = Labels::new();
        b.insert("app", "web");
        assert2::assert!(a.fingerprint() != b.fingerprint());
    }

    #[test]
    fn fingerprint_is_injective_across_delimiter_ambiguity() {
        for (_name, left, right) in [
            (
                "embedded equals sign",
                Labels::from_pairs([("a", "b=c")]),
                Labels::from_pairs([("a=b", "c")]),
            ),
            (
                "embedded newline",
                Labels::from_pairs([("x", "y"), ("z", "")]),
                Labels::from_pairs([("x", "y\nz=")]),
            ),
        ] {
            assert2::assert!(left.fingerprint() != right.fingerprint());
        }
    }

    #[test]
    fn get_and_iter_round_trip() {
        let mut l = Labels::new();
        assert2::assert!(&l == &Labels::new());
        l.insert("app", "api");
        assert2::assert!(&l == &Labels::from_pairs([("app", "api")]));
        assert2::assert!(l.get("app") == Some("api"));
        assert2::assert!(l.get("missing") == None);
        l.insert("env", "prod");
        let pairs = l
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert2::assert!(&l == &Labels::from_pairs([("app", "api"), ("env", "prod")]));
        assert2::assert!(pairs == vec![("app", "api"), ("env", "prod")]);
    }

    #[test]
    fn from_iterator_preserves_pairs() {
        let labels = vec![
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]
        .into_iter()
        .collect::<Labels>();

        let pairs = labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert2::assert!(pairs == vec![("app", "api"), ("env", "prod")]);
    }
}
