//! Label matchers.

use serde::{Deserialize, Serialize};

use crate::labels::SeriesFingerprint;

/// Synthetic label naming the active query shard (`N_of_M`).
///
/// Crabka's sharding is an internal scheme over the FNV [`SeriesFingerprint`]
/// (see [`QueryShardSelector::matches`]); it is self-consistent but not
/// byte-compatible with Mimir's stable label-hash sharding, so this label is
/// internal-only and must not cross the Mimir-facing wire boundary.
pub const QUERY_SHARD_LABEL: &str = "__query_shard__";

/// Matcher operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchOp {
    /// `name="value"`
    Eq,
    /// `name!="value"`
    Neq,
    /// `name=~"regex"`
    Re,
    /// `name!~"regex"`
    Nre,
}

/// A single label matcher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

impl LabelMatcher {
    #[must_use]
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }
}

/// Parsed `N_of_M` Mimir query shard selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryShardSelector {
    pub index: usize,
    pub total: usize,
}

impl QueryShardSelector {
    /// Whether series fingerprint `fp` falls in this shard.
    ///
    /// This shards on the crate's internal FNV [`SeriesFingerprint`] (a 0-based
    /// remap of Mimir's 1-based `N_of_M`). It is self-consistent within Crabka
    /// but is **not** byte-compatible with Mimir's stable label-hash sharding,
    /// which hashes the label set with a different algorithm. Consequently
    /// `__query_shard__` is an internal-only sharding scheme: it must never be
    /// exposed to, nor accepted from, a real Mimir client, since the shard
    /// boundaries would not agree across the two systems.
    #[must_use]
    pub fn matches(self, fp: SeriesFingerprint) -> bool {
        fp % self.total as u64 == (self.index - 1) as u64
    }
}

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn parse_query_shard_selector(value: &str) -> Result<QueryShardSelector, String> {
    let Some((index, total)) = value.split_once("_of_") else {
        return Err(format!("invalid query shard selector `{value}`"));
    };
    let index = index
        .parse::<usize>()
        .map_err(|_| format!("invalid query shard selector `{value}`"))?;
    let total = total
        .parse::<usize>()
        .map_err(|_| format!("invalid query shard selector `{value}`"))?;
    if index == 0 || total == 0 || index > total {
        return Err(format!("invalid query shard selector `{value}`"));
    }
    Ok(QueryShardSelector { index, total })
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_query_shard_selector_accepts_inclusive_upper_bound() {
        let selector = parse_query_shard_selector("1_of_1").unwrap();

        assert2::assert!(selector == QueryShardSelector { index: 1, total: 1 });
        assert2::assert!(selector.matches(42));
    }

    #[test]
    fn parse_query_shard_selector_rejects_zero_and_out_of_range_bounds() {
        for (_name, value) in [
            ("zero index", "0_of_1"),
            ("zero total", "1_of_0"),
            ("index exceeds total", "2_of_1"),
            ("larger index exceeds total", "3_of_2"),
            ("malformed selector", "not-a-shard"),
        ] {
            assert2::assert!(parse_query_shard_selector(value).is_err());
        }
    }
}
