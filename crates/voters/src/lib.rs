//! KIP-853 voter set value types: a voter is (id, directory-id, endpoints, kraft.version range).
//!
//! This is a pure value-type leaf crate — no IO, no async, no crypto — so it
//! compiles for `wasm32-unknown-unknown`. `crabka-metadata` re-exports it as
//! its `voters` module, and the deterministic consensus core
//! (`crabka-kraft-core`) embeds a [`VoterSet`] in its quorum state.

#![doc(html_root_url = "https://docs.rs/crabka-voters/0.3.8")]

use std::collections::BTreeMap;

/// A broker/controller node id — the canonical [`crabka_ids::NodeId`] newtype
/// (a `u64` internally; Kafka's `int32` on the wire). Re-exported here so the
/// consensus stack keeps naming it `crabka_voters::NodeId`.
pub use crabka_ids::NodeId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single listener endpoint advertised by a voter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoterEndpoint {
    pub name: String,
    pub host: String,
    pub port: u16,
}

/// Supported kraft.version range for a voter (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KRaftVersionRange {
    pub min: u16,
    pub max: u16,
}

impl Default for KRaftVersionRange {
    fn default() -> Self {
        Self { min: 0, max: 1 }
    }
}

/// One voter's full identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Voter {
    pub id: NodeId,
    pub directory_id: Uuid,
    pub endpoints: Vec<VoterEndpoint>,
    pub kraft_version: KRaftVersionRange,
}

/// The authoritative voter set (ordered by node id).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoterSet {
    voters: BTreeMap<NodeId, Voter>,
}

impl VoterSet {
    #[must_use]
    pub fn from_voters(voters: impl IntoIterator<Item = Voter>) -> Self {
        Self {
            voters: voters.into_iter().map(|v| (v.id, v)).collect(),
        }
    }

    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains_key(&id)
    }

    #[must_use]
    pub fn get(&self, id: NodeId) -> Option<&Voter> {
        self.voters.get(&id)
    }

    #[must_use]
    pub fn ids(&self) -> std::collections::BTreeSet<NodeId> {
        self.voters.keys().copied().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.voters.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.voters.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Voter> {
        self.voters.values()
    }

    /// Return a copy with `voter` added or replaced.
    #[must_use]
    pub fn with_voter(&self, voter: Voter) -> Self {
        let mut next = self.clone();
        next.voters.insert(voter.id, voter);
        next
    }

    /// Return a copy with `id` removed.
    #[must_use]
    pub fn without_voter(&self, id: NodeId) -> Self {
        let mut next = self.clone();
        next.voters.remove(&id);
        next
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn sample(id: NodeId) -> Voter {
        Voter {
            id,
            directory_id: Uuid::from_u128(u128::from(id.0)),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9093,
            }],
            kraft_version: KRaftVersionRange::default(),
        }
    }

    #[test]
    fn add_remove_are_immutable_copies() {
        let base = VoterSet::from_voters([sample(NodeId(1))]);
        let added = base.with_voter(sample(NodeId(2)));
        assert!(base.contains(NodeId(1)) && !base.contains(NodeId(2)));
        assert!(added.contains(NodeId(1)) && added.contains(NodeId(2)));
        let removed = added.without_voter(NodeId(1));
        assert!(!removed.contains(NodeId(1)) && removed.contains(NodeId(2)));
    }

    #[test]
    fn ids_are_sorted() {
        let set = VoterSet::from_voters([sample(NodeId(3)), sample(NodeId(1)), sample(NodeId(2))]);
        assert!(set.ids().into_iter().collect::<Vec<_>>() == vec![NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[test]
    fn accessors_reflect_contents() {
        let set = VoterSet::from_voters([sample(NodeId(1)), sample(NodeId(2))]);
        check!(set.len() == 2);
        check!(!set.is_empty());
        check!(set.get(NodeId(1)) == Some(&sample(NodeId(1))));
        check!(set.get(NodeId(99)).is_none());
        check!(set.iter().count() == 2);

        let empty = VoterSet::default();
        check!(empty.len() == 0);
        check!(empty.is_empty());
        check!(empty.iter().count() == 0);
    }
}
