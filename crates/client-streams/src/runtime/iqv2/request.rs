//! `IQv2` request envelope: `Position`/`PositionBound` (KIP-796 bounded
//! staleness), partition selection, and the finalized `StateQuery<Q>` that
//! `KafkaStreams::query` consumes.

use std::collections::{BTreeMap, BTreeSet};

use super::query::Query;

/// Source-topic consumed offsets folded into a store: topic → partition →
/// offset (the next offset to read, i.e. one past the last consumed record).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Position(pub BTreeMap<String, BTreeMap<i32, i64>>);

impl Position {
    /// Offset recorded for one topic-partition, if any.
    #[must_use]
    pub fn offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.0.get(topic).and_then(|m| m.get(&partition)).copied()
    }

    /// True if `self` meets or exceeds every `(topic, partition)` offset in
    /// `bound`. A bound naming a partition `self` has never advanced fails.
    #[must_use]
    pub(crate) fn dominates(&self, bound: &Position) -> bool {
        bound.0.iter().all(|(topic, parts)| {
            parts
                .iter()
                .all(|(p, off)| self.offset(topic, *p).is_some_and(|cur| cur >= *off))
        })
    }
}

/// Freshness bound for a query (KIP-796). `At` requires each partition's
/// `Position` to dominate the given one, else that partition fails fast with
/// `NotUpToBound` — the query never blocks.
#[derive(Debug, Clone, Default)]
pub enum PositionBound {
    #[default]
    Unbounded,
    At(Position),
}

/// Which local partitions to query.
#[derive(Debug, Clone, Default)]
pub(crate) enum PartitionSel {
    #[default]
    All,
    Set(BTreeSet<i32>),
}

/// A finalized `IQv2` request: built via
/// `StateQueryRequest::in_store(name).with_query(q)`.
pub struct StateQuery<Q: Query> {
    pub(crate) store: String,
    pub(crate) query: Q,
    pub(crate) partitions: PartitionSel,
    pub(crate) bound: PositionBound,
    pub(crate) require_active: bool,
}

impl<Q: Query> StateQuery<Q> {
    /// Restrict to a specific set of local partitions (default: all).
    #[must_use]
    pub fn with_partitions(mut self, set: BTreeSet<i32>) -> Self {
        self.partitions = PartitionSel::Set(set);
        self
    }

    /// Query all locally assigned partitions (the default).
    #[must_use]
    pub fn with_all_partitions(mut self) -> Self {
        self.partitions = PartitionSel::All;
        self
    }

    /// Require each queried partition to meet a freshness bound.
    #[must_use]
    pub fn with_position_bound(mut self, bound: PositionBound) -> Self {
        self.bound = bound;
        self
    }

    /// Only serve from active (not standby/restoring) tasks.
    #[must_use]
    pub fn require_active(mut self) -> Self {
        self.require_active = true;
        self
    }
}

/// Entry point namespace: `StateQueryRequest::in_store("s").with_query(q)`.
pub struct StateQueryRequest;

impl StateQueryRequest {
    /// Begin a request against state store `name`.
    #[must_use]
    pub fn in_store(name: impl Into<String>) -> StateQueryRequestBuilder {
        StateQueryRequestBuilder { store: name.into() }
    }
}

/// Half-built request awaiting `.with_query(q)`.
pub struct StateQueryRequestBuilder {
    store: String,
}

impl StateQueryRequestBuilder {
    /// Attach the query, finalizing the request.
    #[must_use]
    pub fn with_query<Q: Query>(self, query: Q) -> StateQuery<Q> {
        StateQuery {
            store: self.store,
            query,
            partitions: PartitionSel::All,
            bound: PositionBound::Unbounded,
            require_active: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(entries: &[(&str, i32, i64)]) -> Position {
        let mut m: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        for (t, p, o) in entries {
            m.entry((*t).to_string()).or_default().insert(*p, *o);
        }
        Position(m)
    }

    #[test]
    fn dominates_requires_all_bound_partitions_met() {
        let cur = pos(&[("in", 0, 10), ("in", 1, 5)]);
        for (name, bound, want) in [
            ("exact bound", pos(&[("in", 0, 10)]), true),
            ("ahead of bound", pos(&[("in", 0, 9), ("in", 1, 5)]), true),
            ("behind bound", pos(&[("in", 0, 11)]), false),
            ("unknown topic partition", pos(&[("other", 0, 1)]), false),
        ] {
            assert_eq!(cur.dominates(&bound), want, "case {name}");
        }
    }

    #[test]
    fn position_offset_reads_present_and_absent_tps() {
        let p = pos(&[("in", 0, 10), ("in", 1, 5)]);
        for (name, topic, partition, expected) in [
            ("first present partition", "in", 0, Some(10)),
            ("second present partition", "in", 1, Some(5)),
            ("absent partition", "in", 2, None),
            ("absent topic", "other", 0, None),
        ] {
            assert_eq!(p.offset(topic, partition), expected, "case {name}");
        }
    }

    #[test]
    fn builder_defaults_and_store_name() {
        use crate::runtime::iqv2::query::KeyQuery;

        let q = StateQueryRequest::in_store("s")
            .with_query(KeyQuery::<String, i64>::with_key("k".into()));
        assert_eq!(q.store.as_str(), "s");
        assert_eq!(matches!(q.partitions, PartitionSel::All), true);
        assert_eq!(matches!(q.bound, PositionBound::Unbounded), true);
        assert_eq!(q.require_active, false);
    }

    #[test]
    fn builder_chain_sets_each_field() {
        use crate::runtime::iqv2::query::KeyQuery;

        let set: BTreeSet<i32> = [0, 2].into_iter().collect();
        let q = StateQueryRequest::in_store("s")
            .with_query(KeyQuery::<String, i64>::with_key("k".into()))
            .with_partitions(set.clone());
        match &q.partitions {
            PartitionSel::Set(s) => assert_eq!(s, &set),
            PartitionSel::All => panic!("expected explicit partition set"),
        }

        // with_all_partitions resets back to All.
        let q = q.with_all_partitions();
        assert!(matches!(q.partitions, PartitionSel::All));

        // with_position_bound stores the bound.
        let bound_pos = pos(&[("in", 0, 7)]);
        let q = q.with_position_bound(PositionBound::At(bound_pos.clone()));
        match &q.bound {
            PositionBound::At(p) => assert_eq!(p, &bound_pos),
            PositionBound::Unbounded => panic!("expected At bound"),
        }

        // require_active flips the flag.
        let q = q.require_active();
        assert!(q.require_active);
    }
}
