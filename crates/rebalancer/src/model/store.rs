//! Ring buffer of recent `Proposal`s, UUID-keyed, with atomic on-disk
//! persistence. Persists to `{data_dir}/proposals.json` so
//! proposals survive a rebalancer restart.

use std::{
    collections::VecDeque,
    fs, io,
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::proposal::Proposal;

const FILE_VERSION: u32 = 1;
const DEFAULT_FILENAME: &str = "proposals.json";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema version {found} not supported (expected {expected})")]
    UnsupportedVersion { found: u32, expected: u32 },
}

#[derive(Debug, Serialize, Deserialize)]
struct OnDisk {
    version: u32,
    capacity: usize,
    proposals: Vec<Proposal>,
}

#[derive(Debug)]
pub struct ProposalStore {
    inner: Mutex<VecDeque<Proposal>>,
    capacity: usize,
    /// Where to persist. `None` = in-memory only (tests or ephemeral runs).
    path: Option<PathBuf>,
}

impl ProposalStore {
    /// New in-memory-only store. Used in unit tests where persistence
    /// isn't under test.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            capacity: capacity.max(1),
            path: None,
        }
    }

    /// Open or create a persisted store at `{data_dir}/proposals.json`.
    /// If the file is missing, returns an empty store and will create
    /// the file on first write.
    pub fn open(data_dir: &Path, capacity: usize) -> Result<Self, StoreError> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(DEFAULT_FILENAME);
        let inner = match fs::read(&path) {
            Ok(bytes) => {
                let parsed: OnDisk = serde_json::from_slice(&bytes)?;
                if parsed.version != FILE_VERSION {
                    return Err(StoreError::UnsupportedVersion {
                        found: parsed.version,
                        expected: FILE_VERSION,
                    });
                }
                VecDeque::from(parsed.proposals)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => VecDeque::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            inner: Mutex::new(inner),
            capacity: capacity.max(1),
            path: Some(path),
        })
    }

    pub fn insert(&self, p: Proposal) {
        {
            let mut q = self.inner.lock().expect("ProposalStore mutex poisoned");
            if q.len() == self.capacity {
                q.pop_front();
            }
            q.push_back(p);
        }
        self.persist_if_durable();
    }

    /// Apply `f` to the proposal with `id`. Returns the post-mutation
    /// clone, or `None` if no such id. Persists.
    pub fn mutate<F: FnOnce(&mut Proposal)>(&self, id: &str, f: F) -> Option<Proposal> {
        let updated = {
            let mut q = self.inner.lock().expect("ProposalStore mutex poisoned");
            let p = q.iter_mut().find(|p| p.id == id)?;
            f(p);
            p.clone()
        };
        self.persist_if_durable();
        Some(updated)
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<Proposal> {
        let q = self.inner.lock().expect("ProposalStore mutex poisoned");
        q.iter().find(|p| p.id == id).cloned()
    }

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

    fn persist_if_durable(&self) {
        let Some(ref path) = self.path else {
            return;
        };
        let snapshot: Vec<Proposal> = {
            let q = self.inner.lock().expect("ProposalStore mutex poisoned");
            q.iter().cloned().collect()
        };
        let on_disk = OnDisk {
            version: FILE_VERSION,
            capacity: self.capacity,
            proposals: snapshot,
        };
        match write_atomic(path, &on_disk) {
            Ok(()) => debug!(?path, "proposals.json persisted"),
            Err(e) => {
                warn!(?path, error = %e, "proposals.json persist failed; in-memory state ahead of disk");
            }
        }
    }
}

fn write_atomic(path: &Path, on_disk: &OnDisk) -> Result<(), StoreError> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(on_disk)?;
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
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
            started_at_ms: 0,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle_bytes_per_sec: 0,
        }
    }

    #[test]
    fn get_returns_inserted_proposal() {
        let s = ProposalStore::new(4);
        s.insert(p("a"));
        assert2::assert!((s.get("a").is_some(), s.get("ghost").is_none()) == (true, true));
    }

    #[test]
    fn ring_buffer_drops_oldest_at_capacity() {
        let s = ProposalStore::new(2);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        for (id, want_present) in [("a", false), ("b", true), ("c", true)] {
            assert2::assert!(s.get(id).is_some() == want_present);
        }
    }

    #[test]
    fn list_returns_most_recent_first_within_limit() {
        let s = ProposalStore::new(10);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        let listed: Vec<String> = s.list(2).into_iter().map(|p| p.id).collect();
        assert2::assert!(listed == vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn list_limit_zero_uses_capacity_default() {
        let s = ProposalStore::new(2);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        let listed: Vec<String> = s.list(0).into_iter().map(|p| p.id).collect();
        assert2::assert!(listed == vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn capacity_zero_clamped_to_one() {
        let s = ProposalStore::new(0);
        s.insert(p("a"));
        s.insert(p("b"));
        assert2::assert!((s.get("a").is_none(), s.get("b").is_some()) == (true, true));
    }

    #[test]
    fn mutate_updates_status_and_persists() {
        let s = ProposalStore::new(4);
        s.insert(p("a"));
        let updated = s
            .mutate("a", |pp| {
                pp.status = ProposalStatus::Executing;
                pp.started_at_ms = 42;
            })
            .expect("mutated");
        let want = Proposal {
            id: "a".into(),
            status: ProposalStatus::Executing,
            created_at_ms: 0,
            goals_applied: vec![],
            summary: ProposalSummary::default(),
            movements: vec![],
            started_at_ms: 42,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle_bytes_per_sec: 0,
        };
        assert2::assert!(updated == want);
        assert2::assert!(s.get("a") == Some(want));
    }

    #[test]
    fn mutate_returns_none_for_unknown_id() {
        let s = ProposalStore::new(4);
        assert2::assert!(s.mutate("ghost", |_| {}).is_none());
    }

    #[test]
    fn open_creates_empty_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let s = ProposalStore::open(dir.path(), 4).unwrap();
        assert2::assert!(s.list(0).is_empty());
    }

    #[test]
    fn open_propagates_non_not_found_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join(DEFAULT_FILENAME)).unwrap();

        let err = ProposalStore::open(dir.path(), 4).unwrap_err();

        assert2::assert!(matches!(err, StoreError::Io(_)));
    }

    #[test]
    fn persist_round_trips_via_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = ProposalStore::open(dir.path(), 4).unwrap();
            s.insert(p("a"));
            s.insert(p("b"));
            s.mutate("a", |pp| pp.status = ProposalStatus::Executing);
        }
        let s2 = ProposalStore::open(dir.path(), 4).unwrap();
        assert2::assert!(
            (s2.get("a").map(|p| p.status), s2.get("b").is_some())
                == (Some(ProposalStatus::Executing), true)
        );
    }

    #[test]
    fn open_rejects_unsupported_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DEFAULT_FILENAME);
        let bogus = r#"{"version":999,"capacity":4,"proposals":[]}"#;
        fs::write(&path, bogus).unwrap();
        let err = ProposalStore::open(dir.path(), 4).unwrap_err();
        assert2::assert!(matches!(
            err,
            StoreError::UnsupportedVersion {
                found: 999,
                expected: 1
            }
        ));
    }
}
