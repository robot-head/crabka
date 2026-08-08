//! HA deduplication for Prometheus replica pairs.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use crabka_units::prelude::*;

use crate::wire::DecodedSeries;

/// The compacted HA-tracker topic: `(tenant, cluster) -> elected __replica__`.
pub const HA_TRACKER_TOPIC: &str = "__crabka_metrics_ha";
/// Default elected-replica lease timeout before another replica may take over.
pub const DEFAULT_HA_FAILOVER_TIMEOUT: Time = secs(30);

/// In-memory elected replica view. The distributor rebuilds it from the
/// compacted HA-tracker topic, and extends it with an in-process first-seen
/// election for unseen pairs.
#[derive(Debug, Default)]
pub struct HaTracker {
    elected: Mutex<HashMap<(String, String), HaElectionRecord>>,
}

/// A persisted HA election record for the compacted HA-tracker topic.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HaElectionRecord {
    pub tenant: String,
    pub cluster: String,
    pub replica: String,
    pub lease_timestamp_ms: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum HaElectionRecordError {
    #[error("HA election record encode failed: {0}")]
    Encode(String),

    #[error("HA election record decode failed: {0}")]
    Decode(String),
}

impl HaElectionRecord {
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn encode(&self) -> Result<Vec<u8>, HaElectionRecordError> {
        serde_json::to_vec(self).map_err(|error| HaElectionRecordError::Encode(error.to_string()))
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, HaElectionRecordError> {
        serde_json::from_slice(bytes)
            .map_err(|error| HaElectionRecordError::Decode(error.to_string()))
    }
}

impl HaTracker {
    #[must_use]
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn elected_replica(&self, tenant: &str, cluster: &str) -> Option<String> {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .get(&(tenant.to_string(), cluster.to_string()))
            .map(|record| record.replica.clone())
    }

    #[must_use]
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn election_record(&self, tenant: &str, cluster: &str) -> Option<HaElectionRecord> {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .get(&(tenant.to_string(), cluster.to_string()))
            .cloned()
    }

    pub fn set_elected(
        &self,
        tenant: impl Into<String>,
        cluster: impl Into<String>,
        replica: impl Into<String>,
    ) {
        self.set_elected_at(tenant, cluster, replica, now_ms());
    }

    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn set_elected_at(
        &self,
        tenant: impl Into<String>,
        cluster: impl Into<String>,
        replica: impl Into<String>,
        lease_timestamp_ms: i64,
    ) {
        let tenant = tenant.into();
        let cluster = cluster.into();
        let replica = replica.into();
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .insert(
                (tenant.clone(), cluster.clone()),
                HaElectionRecord {
                    tenant,
                    cluster,
                    replica,
                    lease_timestamp_ms,
                },
            );
    }

    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn persist_elected(&self, record: &HaElectionRecord) {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .insert(
                (record.tenant.clone(), record.cluster.clone()),
                record.clone(),
            );
    }

    /// Decides and commits the HA election for `series` atomically, with the
    /// current wall clock and the default failover timeout. See
    /// [`Self::elect`].
    pub fn elect_now(&self, tenant: &str, series: &[DecodedSeries]) -> HaElection {
        self.elect(tenant, series, now_ms(), DEFAULT_HA_FAILOVER_TIMEOUT)
    }

    /// Decides and commits the HA election atomically, with the current wall
    /// clock and the supplied failover timeout.
    pub fn elect_now_with_timeout(
        &self,
        tenant: &str,
        series: &[DecodedSeries],
        failover_timeout: Time,
    ) -> HaElection {
        self.elect(tenant, series, now_ms(), failover_timeout)
    }

    /// Decides the HA election for `series` atomically. When the decision is
    /// `Elect` or `Update`, it commits the in-memory winner under the same
    /// lock. This closes the elect TOCTOU, because a second racing replica that
    /// locks afterwards sees the committed winner and is dropped. The DURABLE
    /// Kafka persist stays with the caller and can proceed asynchronously after
    /// this function returns.
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn elect(
        &self,
        tenant: &str,
        series: &[DecodedSeries],
        lease_timestamp_ms: i64,
        failover_timeout: Time,
    ) -> HaElection {
        let mut elected = self.elected.lock().expect("HaTracker mutex poisoned");
        let decision = decide_election(
            &elected,
            tenant,
            series,
            lease_timestamp_ms,
            failover_timeout,
        );
        if let HaElection::Elect(record) | HaElection::Update(record) = &decision {
            elected.insert(
                (record.tenant.clone(), record.cluster.clone()),
                record.clone(),
            );
        }
        decision
    }
}

/// Whether a decoded ingest request should append to the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaDecision {
    Accept,
    Drop,
}

/// The HA election action required for a decoded ingest request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaElection {
    Accept,
    Drop,
    Elect(HaElectionRecord),
    Update(HaElectionRecord),
}

/// Inspects the `cluster` and `__replica__` labels of the first series. A
/// missing `__replica__` means HA is off for the request, and the distributor
/// accepts the request. Otherwise only the elected replica may write.
#[must_use]
pub fn ha_election(tracker: &HaTracker, tenant: &str, series: &[DecodedSeries]) -> HaElection {
    ha_election_at(tracker, tenant, series, now_ms())
}

/// Timestamped HA election helper for deterministic tests and injectable clocks.
#[must_use]
pub fn ha_election_at(
    tracker: &HaTracker,
    tenant: &str,
    series: &[DecodedSeries],
    lease_timestamp_ms: i64,
) -> HaElection {
    ha_election_at_with_timeout(
        tracker,
        tenant,
        series,
        lease_timestamp_ms,
        DEFAULT_HA_FAILOVER_TIMEOUT,
    )
}

/// Timestamped HA election helper with an explicit failover timeout.
#[must_use]
/// # Panics
/// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
pub fn ha_election_at_with_timeout(
    tracker: &HaTracker,
    tenant: &str,
    series: &[DecodedSeries],
    lease_timestamp_ms: i64,
    failover_timeout: Time,
) -> HaElection {
    let elected = tracker.elected.lock().expect("HaTracker mutex poisoned");
    decide_election(
        &elected,
        tenant,
        series,
        lease_timestamp_ms,
        failover_timeout,
    )
}

/// Pure HA election decision against an elected view that is already locked. A
/// caller that holds the tracker lock can decide and commit atomically. A
/// lock-free caller goes through [`ha_election_at_with_timeout`].
fn decide_election(
    elected: &HashMap<(String, String), HaElectionRecord>,
    tenant: &str,
    series: &[DecodedSeries],
    lease_timestamp_ms: i64,
    failover_timeout: Time,
) -> HaElection {
    let Some(first) = series.first() else {
        return HaElection::Accept;
    };
    let Some(replica) = first.labels.get("__replica__") else {
        return HaElection::Accept;
    };
    let cluster = first.labels.get("cluster").unwrap_or("");

    match elected.get(&(tenant.to_string(), cluster.to_string())) {
        Some(elected) if elected.replica == replica => HaElection::Update(HaElectionRecord {
            tenant: tenant.to_string(),
            cluster: cluster.to_string(),
            replica: replica.to_string(),
            lease_timestamp_ms,
        }),
        Some(elected)
            if failover_timeout >= Time::ZERO
                && Time::from_millis(
                    lease_timestamp_ms.saturating_sub(elected.lease_timestamp_ms),
                ) > failover_timeout =>
        {
            HaElection::Elect(HaElectionRecord {
                tenant: tenant.to_string(),
                cluster: cluster.to_string(),
                replica: replica.to_string(),
                lease_timestamp_ms,
            })
        }
        Some(_) => HaElection::Drop,
        None => HaElection::Elect(HaElectionRecord {
            tenant: tenant.to_string(),
            cluster: cluster.to_string(),
            replica: replica.to_string(),
            lease_timestamp_ms,
        }),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Synchronous compatibility helper for tests and direct callers.
#[must_use]
pub fn ha_decision(tracker: &HaTracker, tenant: &str, series: &[DecodedSeries]) -> HaDecision {
    match ha_election(tracker, tenant, series) {
        HaElection::Accept => HaDecision::Accept,
        HaElection::Drop => HaDecision::Drop,
        HaElection::Elect(record) | HaElection::Update(record) => {
            tracker.persist_elected(&record);
            HaDecision::Accept
        }
    }
}

/// Removes the HA coordination label from the series before the WAL append.
pub fn strip_replica_label(series: &mut [DecodedSeries]) {
    for series in series {
        series.labels = series
            .labels
            .iter()
            .filter(|(name, _)| name.as_str() != "__replica__")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_blockstore::Labels;

    use super::*;
    use crate::wire::DecodedSample;

    fn series_with(cluster: &str, replica: &str) -> DecodedSeries {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("cluster", cluster);
        labels.insert("__replica__", replica);
        DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(1, 1.0)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }
    }

    #[test]
    fn elected_replica_accepts() {
        let tracker = HaTracker::default();
        tracker.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r1")];

        assert!(ha_decision(&tracker, "tenant", &series) == HaDecision::Accept);
    }

    #[test]
    fn non_elected_replica_drops() {
        let tracker = HaTracker::default();
        tracker.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r2")];

        assert!(ha_decision(&tracker, "tenant", &series) == HaDecision::Drop);
    }

    #[test]
    fn first_seen_replica_elected_second_dropped() {
        let tracker = HaTracker::default();
        let r1 = [series_with("c1", "r1")];
        let r2 = [series_with("c1", "r2")];

        check!(ha_decision(&tracker, "tenant", &r1) == HaDecision::Accept);
        check!(ha_decision(&tracker, "tenant", &r2) == HaDecision::Drop);
        check!(tracker.elected_replica("tenant", "c1") == Some("r1".to_string()));
    }

    #[test]
    fn elected_replica_updates_lease_timestamp() {
        let tracker = HaTracker::default();
        tracker.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r1")];

        assert!(
            ha_election_at(&tracker, "tenant", &series, 42_000)
                == HaElection::Update(HaElectionRecord {
                    tenant: "tenant".to_string(),
                    cluster: "c1".to_string(),
                    replica: "r1".to_string(),
                    lease_timestamp_ms: 42_000,
                })
        );
    }

    #[test]
    fn stale_elected_replica_can_fail_over() {
        let tracker = HaTracker::default();
        tracker.persist_elected(&HaElectionRecord {
            tenant: "tenant".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 10_000,
        });
        let replacement = [series_with("c1", "r2")];

        assert!(
            ha_election_at_with_timeout(&tracker, "tenant", &replacement, 45_001, secs(30))
                == HaElection::Elect(HaElectionRecord {
                    tenant: "tenant".to_string(),
                    cluster: "c1".to_string(),
                    replica: "r2".to_string(),
                    lease_timestamp_ms: 45_001,
                })
        );
    }

    #[test]
    fn negative_failover_timeout_disables_takeover() {
        // A negative extent is the "never fail over" sentinel: however stale the
        // lease, the incumbent keeps it and the challenger is dropped.
        let tracker = HaTracker::default();
        tracker.persist_elected(&HaElectionRecord {
            tenant: "tenant".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 10_000,
        });
        let replacement = [series_with("c1", "r2")];

        assert!(
            ha_election_at_with_timeout(
                &tracker,
                "tenant",
                &replacement,
                i64::MAX,
                Time::from_millis(-1),
            ) == HaElection::Drop
        );
    }

    #[test]
    fn configured_failover_timeout_controls_takeover() {
        let tracker = || {
            let tracker = HaTracker::default();
            tracker.persist_elected(&HaElectionRecord {
                tenant: "tenant".to_owned(),
                cluster: "c1".to_owned(),
                replica: "r1".to_owned(),
                lease_timestamp_ms: 1_000,
            });
            tracker
        };
        let replacement = [series_with("c1", "r2")];

        check!(
            tracker().elect("tenant", &replacement, i64::MAX, Time::from_millis(-1_000),)
                == HaElection::Drop
        );
        check!(matches!(
            tracker().elect("tenant", &replacement, 1_001, Time::ZERO),
            HaElection::Elect(_)
        ));
        check!(
            tracker().elect("tenant", &replacement, 2_000, millis(999))
                == HaElection::Elect(HaElectionRecord {
                    tenant: "tenant".to_owned(),
                    cluster: "c1".to_owned(),
                    replica: "r2".to_owned(),
                    lease_timestamp_ms: 2_000,
                })
        );
    }

    #[test]
    fn no_replica_label_means_ha_disabled() {
        let tracker = HaTracker::default();
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        let series = [DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(1, 1.0)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }];

        assert!(ha_decision(&tracker, "tenant", &series) == HaDecision::Accept);
    }

    #[test]
    fn strip_removes_replica_label() {
        let mut series = vec![series_with("c1", "r1")];

        strip_replica_label(&mut series);

        assert!(series[0].labels.get("__replica__") == None);
        assert!(series[0].labels.get("cluster") == Some("c1"));
    }

    #[test]
    fn elect_commits_in_memory_winner_atomically() {
        let tracker = HaTracker::default();
        let r1 = [series_with("c1", "r1")];
        let r2 = [series_with("c1", "r2")];

        assert!(matches!(
            tracker.elect("tenant", &r1, 1_000, DEFAULT_HA_FAILOVER_TIMEOUT),
            HaElection::Elect(_)
        ));
        // The first elect already committed the winner under the lock, so a
        // competing replica observes it and is dropped without a separate
        // persist step.
        assert!(
            tracker.elect("tenant", &r2, 1_001, DEFAULT_HA_FAILOVER_TIMEOUT) == HaElection::Drop
        );
        assert!(tracker.elected_replica("tenant", "c1") == Some("r1".to_string()));
    }

    #[test]
    fn concurrent_first_seen_elections_elect_exactly_one() {
        use std::{
            sync::{Arc, Barrier},
            thread,
        };

        let tracker = Arc::new(HaTracker::default());
        let barrier = Arc::new(Barrier::new(2));

        let handles = ["ra", "rb"].map(|replica| {
            let tracker = Arc::clone(&tracker);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let series = [series_with("c1", replica)];
                barrier.wait();
                tracker.elect("tenant", &series, 1_000, DEFAULT_HA_FAILOVER_TIMEOUT)
            })
        });

        let elects = handles
            .into_iter()
            .map(|handle| handle.join().expect("election thread panicked"))
            .filter(|decision| matches!(decision, HaElection::Elect(_)))
            .count();

        assert!(elects == 1);
    }
}
