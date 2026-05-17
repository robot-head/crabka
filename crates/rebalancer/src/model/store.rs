//! In-memory ring buffer of recent `Proposal`s, UUID-keyed.
//!
//! Slice 43a only persists proposals for the lifetime of the
//! `crabka-rebalancer` process. Restart drops them. Slice 43b adds
//! on-disk persistence.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::proposal::Proposal;

pub struct ProposalStore {
    /// Most recent insertion at the back, oldest at the front. Bounded
    /// by `capacity`.
    inner: Mutex<VecDeque<Proposal>>,
    capacity: usize,
}

impl ProposalStore {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity: capacity.max(1),
        }
    }

    /// Append a proposal; drop the oldest if capacity is exceeded.
    pub fn insert(&self, p: Proposal) {
        let mut q = self.inner.lock().expect("ProposalStore mutex poisoned");
        if q.len() == self.capacity {
            q.pop_front();
        }
        q.push_back(p);
    }

    /// Fetch one proposal by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<Proposal> {
        let q = self.inner.lock().expect("ProposalStore mutex poisoned");
        q.iter().find(|p| p.id == id).cloned()
    }

    /// Return up to `limit` proposals, most recent first. `limit == 0`
    /// uses the store's capacity as the default.
    #[must_use]
    pub fn list(&self, limit: usize) -> Vec<Proposal> {
        let q = self.inner.lock().expect("ProposalStore mutex poisoned");
        let n = if limit == 0 {
            self.capacity
        } else {
            limit.min(self.capacity)
        };
        q.iter().rev().take(n).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::proposal::{Proposal, ProposalStatus, ProposalSummary};

    fn p(id: &str) -> Proposal {
        Proposal {
            id: id.into(),
            status: ProposalStatus::Computed,
            created_at_ms: 0,
            goals_applied: vec![],
            summary: ProposalSummary::default(),
            movements: vec![],
        }
    }

    #[test]
    fn get_returns_inserted_proposal() {
        let s = ProposalStore::new(4);
        s.insert(p("a"));
        assert!(s.get("a").is_some());
        assert!(s.get("ghost").is_none());
    }

    #[test]
    fn ring_buffer_drops_oldest_at_capacity() {
        let s = ProposalStore::new(2);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        assert!(s.get("a").is_none(), "a should have been evicted");
        assert!(s.get("b").is_some());
        assert!(s.get("c").is_some());
    }

    #[test]
    fn list_returns_most_recent_first_within_limit() {
        let s = ProposalStore::new(10);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        let listed: Vec<String> = s.list(2).into_iter().map(|p| p.id).collect();
        assert_eq!(listed, vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn list_limit_zero_uses_capacity_default() {
        let s = ProposalStore::new(2);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c")); // evicts "a"
        let listed: Vec<String> = s.list(0).into_iter().map(|p| p.id).collect();
        assert_eq!(listed, vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn capacity_zero_clamped_to_one() {
        let s = ProposalStore::new(0);
        s.insert(p("a"));
        s.insert(p("b"));
        assert!(s.get("a").is_none());
        assert!(s.get("b").is_some());
    }
}
