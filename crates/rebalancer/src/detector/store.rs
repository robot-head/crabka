//! Ring buffer of `Anomaly` records with atomic on-disk persistence at
//! `{data_dir}/anomalies.json`. Mirrors `model::store::ProposalStore`.

use std::{
    collections::VecDeque,
    fs, io,
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use uuid::Uuid;

use super::anomaly::{Anomaly, AnomalyKey, AnomalyKind, AnomalySeverity};

const FILE_VERSION: u32 = 1;
const DEFAULT_FILENAME: &str = "anomalies.json";

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
    items: Vec<Anomaly>,
}

#[derive(Debug)]
pub struct AnomalyStore {
    inner: Mutex<VecDeque<Anomaly>>,
    capacity: usize,
    path: Option<PathBuf>,
}

impl AnomalyStore {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            capacity: capacity.max(1),
            path: None,
        }
    }

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
                VecDeque::from(parsed.items)
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

    /// Returns `(id, is_new)`. `is_new = true` means a fresh record was
    /// inserted, so the caller's tick loop may fire auto-trigger; `false`
    /// means an existing open record was refreshed in place (dedup).
    pub fn upsert_open(
        &self,
        kind: AnomalyKind,
        key: AnomalyKey,
        severity: AnomalySeverity,
        details: String,
        now_ms: i64,
    ) -> (String, bool) {
        let result = {
            let mut q = self.inner.lock().expect("AnomalyStore mutex poisoned");
            if let Some(existing) = q.iter_mut().rev().find(|a| is_open_match(a, kind, &key)) {
                existing.last_seen_at_ms = now_ms;
                existing.severity = severity;
                existing.details = details;
                (existing.id.clone(), false)
            } else {
                let id = Uuid::new_v4().to_string();
                if q.len() == self.capacity {
                    q.pop_front();
                }
                q.push_back(Anomaly {
                    id: id.clone(),
                    kind,
                    key,
                    severity,
                    detected_at_ms: now_ms,
                    last_seen_at_ms: now_ms,
                    resolved_at_ms: None,
                    triggered_proposal_id: None,
                    mute_until_ms: None,
                    details,
                });
                (id, true)
            }
        };
        self.persist_if_durable();
        result
    }

    pub fn mark_resolved(&self, kind: AnomalyKind, key: &AnomalyKey, now_ms: i64) -> bool {
        let flipped = {
            let mut q = self.inner.lock().expect("AnomalyStore mutex poisoned");
            if let Some(a) = q.iter_mut().rev().find(|a| is_open_match(a, kind, key)) {
                a.resolved_at_ms = Some(now_ms);
                true
            } else {
                false
            }
        };
        if flipped {
            self.persist_if_durable();
        }
        flipped
    }

    pub fn set_triggered_proposal(&self, id: &str, proposal_id: String, mute_until_ms: i64) {
        let updated = {
            let mut q = self.inner.lock().expect("AnomalyStore mutex poisoned");
            if let Some(a) = q.iter_mut().find(|a| a.id == id) {
                a.triggered_proposal_id = Some(proposal_id);
                a.mute_until_ms = Some(mute_until_ms);
                true
            } else {
                false
            }
        };
        if updated {
            self.persist_if_durable();
        }
    }

    #[must_use]
    pub fn find_open(&self, kind: AnomalyKind, key: &AnomalyKey) -> Option<Anomaly> {
        let q = self.inner.lock().expect("AnomalyStore mutex poisoned");
        q.iter()
            .rev()
            .find(|a| is_open_match(a, kind, key))
            .cloned()
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<Anomaly> {
        let q = self.inner.lock().expect("AnomalyStore mutex poisoned");
        q.iter().find(|a| a.id == id).cloned()
    }

    #[must_use]
    pub fn list(&self, limit: usize, include_resolved: bool) -> Vec<Anomaly> {
        let q = self.inner.lock().expect("AnomalyStore mutex poisoned");
        let n = if limit == 0 {
            self.capacity
        } else {
            limit.min(self.capacity)
        };
        q.iter()
            .rev()
            .filter(|a| include_resolved || a.resolved_at_ms.is_none())
            .take(n)
            .cloned()
            .collect()
    }

    fn persist_if_durable(&self) {
        let Some(ref path) = self.path else {
            return;
        };
        let snapshot: Vec<Anomaly> = {
            let q = self.inner.lock().expect("AnomalyStore mutex poisoned");
            q.iter().cloned().collect()
        };
        let on_disk = OnDisk {
            version: FILE_VERSION,
            capacity: self.capacity,
            items: snapshot,
        };
        match write_atomic(path, &on_disk) {
            Ok(()) => debug!(?path, "anomalies.json persisted"),
            Err(e) => {
                warn!(?path, error = %e, "anomalies.json persist failed; in-memory state ahead of disk");
            }
        }
    }
}

fn is_open_match(a: &Anomaly, kind: AnomalyKind, key: &AnomalyKey) -> bool {
    a.kind == kind && &a.key == key && a.resolved_at_ms.is_none()
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
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn upsert_creates_new_when_absent() {
        let s = AnomalyStore::new(4);
        let (id, is_new) = s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(1),
            AnomalySeverity::Critical,
            "down".into(),
            100,
        );
        check!(is_new);
        check!(!id.is_empty());
        check!(s.list(0, false).len() == 1);
    }

    #[test]
    fn upsert_updates_existing_open() {
        let s = AnomalyStore::new(4);
        let (id1, new1) = s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(1),
            AnomalySeverity::Warning,
            "transient".into(),
            100,
        );
        let (id2, new2) = s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(1),
            AnomalySeverity::Critical,
            "still down".into(),
            250,
        );
        check!(new1);
        check!(!new2);
        check!(id1 == id2);
        let all = s.list(0, false);
        assert!(
            all == vec![Anomaly {
                id: id1,
                kind: AnomalyKind::BrokerDeath,
                key: AnomalyKey::Broker(1),
                severity: AnomalySeverity::Critical,
                detected_at_ms: 100,
                last_seen_at_ms: 250,
                resolved_at_ms: None,
                triggered_proposal_id: None,
                mute_until_ms: None,
                details: "still down".into(),
            }]
        );
    }

    #[test]
    fn mark_resolved_transitions() {
        let s = AnomalyStore::new(4);
        s.upsert_open(
            AnomalyKind::DiskPressure,
            AnomalyKey::Broker(3),
            AnomalySeverity::Warning,
            "85%".into(),
            10,
        );
        check!(s.mark_resolved(AnomalyKind::DiskPressure, &AnomalyKey::Broker(3), 20));
        let all = s.list(0, true);
        check!(all[0].resolved_at_ms == Some(20));
        check!(!s.mark_resolved(AnomalyKind::DiskPressure, &AnomalyKey::Broker(3), 30));
    }

    #[test]
    fn ring_buffer_evicts_oldest_when_full() {
        let s = AnomalyStore::new(2);
        s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(1),
            AnomalySeverity::Critical,
            "1".into(),
            1,
        );
        s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(2),
            AnomalySeverity::Critical,
            "2".into(),
            2,
        );
        s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(3),
            AnomalySeverity::Critical,
            "3".into(),
            3,
        );
        let listed = s.list(0, true);
        assert!(listed.len() == 2);
        let keys: Vec<_> = listed.into_iter().map(|a| a.key).collect();
        assert!(keys == vec![AnomalyKey::Broker(3), AnomalyKey::Broker(2)]);
    }

    #[test]
    fn persist_round_trips_via_open() {
        let dir = tempfile::tempdir().unwrap();
        let (id_a, id_b) = {
            let s = AnomalyStore::open(dir.path(), 4).unwrap();
            let (a, _) = s.upsert_open(
                AnomalyKind::BrokerDeath,
                AnomalyKey::Broker(1),
                AnomalySeverity::Critical,
                "a".into(),
                10,
            );
            let (b, _) = s.upsert_open(
                AnomalyKind::SlowBroker,
                AnomalyKey::Broker(2),
                AnomalySeverity::Warning,
                "b".into(),
                20,
            );
            (a, b)
        };
        let s2 = AnomalyStore::open(dir.path(), 4).unwrap();
        let got_a = s2.get(&id_a).expect("a persisted");
        let got_b = s2.get(&id_b).expect("b persisted");
        assert!(
            got_a
                == Anomaly {
                    id: id_a,
                    kind: AnomalyKind::BrokerDeath,
                    key: AnomalyKey::Broker(1),
                    severity: AnomalySeverity::Critical,
                    detected_at_ms: 10,
                    last_seen_at_ms: 10,
                    resolved_at_ms: None,
                    triggered_proposal_id: None,
                    mute_until_ms: None,
                    details: "a".into(),
                }
        );
        assert!(
            got_b
                == Anomaly {
                    id: id_b,
                    kind: AnomalyKind::SlowBroker,
                    key: AnomalyKey::Broker(2),
                    severity: AnomalySeverity::Warning,
                    detected_at_ms: 20,
                    last_seen_at_ms: 20,
                    resolved_at_ms: None,
                    triggered_proposal_id: None,
                    mute_until_ms: None,
                    details: "b".into(),
                }
        );
    }

    #[test]
    fn open_rejects_unsupported_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DEFAULT_FILENAME);
        let bogus = r#"{"version":99,"capacity":4,"items":[]}"#;
        fs::write(&path, bogus).unwrap();
        let err = AnomalyStore::open(dir.path(), 4).unwrap_err();
        assert!(matches!(
            err,
            StoreError::UnsupportedVersion {
                found: 99,
                expected: 1
            }
        ));
    }

    #[test]
    fn open_propagates_non_not_found_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(DEFAULT_FILENAME)).unwrap();
        let err = AnomalyStore::open(dir.path(), 4).unwrap_err();
        assert!(matches!(err, StoreError::Io(_)), "got {err:?}");
    }

    #[test]
    fn set_triggered_proposal_updates_only_matching_id() {
        let s = AnomalyStore::new(4);
        let (id1, _) = s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(1),
            AnomalySeverity::Critical,
            "down".into(),
            1,
        );
        let (id2, _) = s.upsert_open(
            AnomalyKind::DiskPressure,
            AnomalyKey::Broker(2),
            AnomalySeverity::Warning,
            "hot".into(),
            2,
        );

        s.set_triggered_proposal(&id1, "proposal-a".into(), 1234);

        let a1 = s.get(&id1).expect("updated anomaly");
        let a2 = s.get(&id2).expect("other anomaly");
        assert!(
            a1 == Anomaly {
                id: id1,
                kind: AnomalyKind::BrokerDeath,
                key: AnomalyKey::Broker(1),
                severity: AnomalySeverity::Critical,
                detected_at_ms: 1,
                last_seen_at_ms: 1,
                resolved_at_ms: None,
                triggered_proposal_id: Some("proposal-a".into()),
                mute_until_ms: Some(1234),
                details: "down".into(),
            }
        );
        assert!(
            a2 == Anomaly {
                id: id2,
                kind: AnomalyKind::DiskPressure,
                key: AnomalyKey::Broker(2),
                severity: AnomalySeverity::Warning,
                detected_at_ms: 2,
                last_seen_at_ms: 2,
                resolved_at_ms: None,
                triggered_proposal_id: None,
                mute_until_ms: None,
                details: "hot".into(),
            }
        );
    }

    #[test]
    fn find_open_skips_resolved() {
        let s = AnomalyStore::new(4);
        s.upsert_open(
            AnomalyKind::SlowBroker,
            AnomalyKey::Broker(5),
            AnomalySeverity::Warning,
            "slow".into(),
            1,
        );
        s.mark_resolved(AnomalyKind::SlowBroker, &AnomalyKey::Broker(5), 2);
        assert!(
            s.find_open(AnomalyKind::SlowBroker, &AnomalyKey::Broker(5))
                .is_none()
        );
    }

    #[test]
    fn find_open_requires_matching_kind_and_key() {
        let s = AnomalyStore::new(4);
        let (death_id, _) = s.upsert_open(
            AnomalyKind::BrokerDeath,
            AnomalyKey::Broker(5),
            AnomalySeverity::Critical,
            "down".into(),
            1,
        );
        s.upsert_open(
            AnomalyKind::DiskPressure,
            AnomalyKey::Broker(5),
            AnomalySeverity::Warning,
            "disk".into(),
            2,
        );

        let found = s
            .find_open(AnomalyKind::BrokerDeath, &AnomalyKey::Broker(5))
            .expect("broker-death anomaly");
        assert!(
            found
                == Anomaly {
                    id: death_id,
                    kind: AnomalyKind::BrokerDeath,
                    key: AnomalyKey::Broker(5),
                    severity: AnomalySeverity::Critical,
                    detected_at_ms: 1,
                    last_seen_at_ms: 1,
                    resolved_at_ms: None,
                    triggered_proposal_id: None,
                    mute_until_ms: None,
                    details: "down".into(),
                }
        );
        check!(
            s.find_open(
                AnomalyKind::UnderReplicatedPartitions,
                &AnomalyKey::Broker(5)
            )
            .is_none()
        );
        check!(
            s.find_open(AnomalyKind::BrokerDeath, &AnomalyKey::Broker(6))
                .is_none()
        );
    }
}
