//! Parses the `--metrics-scrape-targets` CLI value:
//! `id:host:port,id:host:port,...` into a list of `ScrapeTarget`s.
//! Empty input is fine (scraper disabled). Malformed entries return
//! a typed error rather than panicking.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrapeTarget {
    pub broker_id: i32,
    pub addr: String, // host:port; resolved at scrape time
}

#[derive(Debug, thiserror::Error)]
pub enum TargetParseError {
    #[error("malformed entry `{0}` (expected `id:host:port`)")]
    Malformed(String),
    #[error("invalid broker id in `{0}`")]
    BadId(String),
}

/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub fn parse_targets(spec: &str) -> Result<Vec<ScrapeTarget>, TargetParseError> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(Vec::new());
    }
    spec.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let (id_str, addr) = entry
                .split_once(':')
                .ok_or_else(|| TargetParseError::Malformed(entry.to_string()))?;
            // After splitting on the first `:`, `addr` is `host:port`.
            if !addr.contains(':') {
                return Err(TargetParseError::Malformed(entry.to_string()));
            }
            let broker_id: i32 = id_str
                .parse()
                .map_err(|_| TargetParseError::BadId(entry.to_string()))?;
            Ok(ScrapeTarget {
                broker_id,
                addr: addr.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn empty_input_returns_empty_vec() {
        for (_name, input) in [("empty", ""), ("whitespace", "   ")] {
            assert2::assert!(parse_targets(input).unwrap().is_empty());
        }
    }

    #[test]
    fn well_formed_entries_parse() {
        let out = parse_targets("1:broker1:9100,2:broker2:9100,3:broker3:9100").unwrap();
        assert2::assert!(
            out == vec![
                ScrapeTarget {
                    broker_id: 1,
                    addr: "broker1:9100".into(),
                },
                ScrapeTarget {
                    broker_id: 2,
                    addr: "broker2:9100".into(),
                },
                ScrapeTarget {
                    broker_id: 3,
                    addr: "broker3:9100".into(),
                },
            ]
        );
    }

    #[test]
    fn malformed_entry_errors() {
        let err = parse_targets("nope").unwrap_err();
        assert2::assert!(matches!(err, TargetParseError::Malformed(_)));
        let err = parse_targets("1:host_without_port").unwrap_err();
        assert2::assert!(matches!(err, TargetParseError::Malformed(_)));
        let err = parse_targets("abc:host:9100").unwrap_err();
        assert2::assert!(matches!(err, TargetParseError::BadId(_)));
    }
}

use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use tracing::warn;

use crate::model::ClusterState;

/// Where the scraper finds its targets each tick.
///
/// `Static` uses an explicit `id:host:port` list
/// from `--metrics-scrape-targets`). `Discovered` reads from the
/// ingester's `ClusterState` snapshot and synthesizes targets at
/// `host:metrics_port` for every broker in the snapshot.
pub enum TargetSource {
    Static(Vec<ScrapeTarget>),
    Discovered {
        snapshot: Arc<ArcSwap<Option<ClusterState>>>,
        metrics_port: u16,
    },
}

impl TargetSource {
    /// Materialize the current set of scrape targets.
    ///
    /// Called by the scraper's main loop each tick. Cheap: the `Static`
    /// arm clones a small `Vec`; the `Discovered` arm reads the snapshot
    /// guard and emits one `ScrapeTarget` per broker (skipping brokers
    /// with empty `host`).
    #[must_use]
    pub fn current(&self) -> Vec<ScrapeTarget> {
        match self {
            Self::Static(targets) => targets.clone(),
            Self::Discovered {
                snapshot,
                metrics_port,
            } => {
                let guard = snapshot.load();
                let state: &Option<ClusterState> = &guard;
                let Some(state) = state.as_ref() else {
                    return Vec::new();
                };
                let mut out = Vec::with_capacity(state.brokers.len());
                for b in &state.brokers {
                    if b.host.is_empty() {
                        if should_warn_empty_host(b.id) {
                            warn!(
                                broker_id = b.id,
                                "broker advertises empty host in metadata; skipping in scrape discovery"
                            );
                        }
                        continue;
                    }
                    out.push(ScrapeTarget {
                        broker_id: b.id,
                        addr: format!("{}:{}", b.host, metrics_port),
                    });
                }
                out
            }
        }
    }
}

/// One-time WARN per `broker_id` when the broker advertises an empty host.
fn should_warn_empty_host(broker_id: i32) -> bool {
    static SEEN: OnceLock<std::sync::Mutex<std::collections::HashSet<i32>>> = OnceLock::new();
    let mut seen = SEEN
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
        .lock()
        .expect("empty-host seen-set");
    seen.insert(broker_id)
}

#[cfg(test)]
mod target_source_tests {

    use super::*;
    use crate::model::{BrokerView, ClusterState, InFlightReassignment, PartitionView};

    fn cluster_state_with(brokers: Vec<BrokerView>) -> ClusterState {
        ClusterState {
            cluster_id: Some("test-cluster".into()),
            snapshot_at_ms: 0,
            brokers,
            partitions: Vec::<PartitionView>::new(),
            in_flight_reassignments: Vec::<InFlightReassignment>::new(),
        }
    }

    #[test]
    fn static_source_returns_underlying_list() {
        let targets = vec![
            ScrapeTarget {
                broker_id: 1,
                addr: "h1:9404".into(),
            },
            ScrapeTarget {
                broker_id: 2,
                addr: "h2:9404".into(),
            },
        ];
        let src = TargetSource::Static(targets.clone());
        assert2::assert!(src.current() == targets);
    }

    #[test]
    fn discovered_source_with_no_snapshot_returns_empty() {
        let snapshot: Arc<ArcSwap<Option<ClusterState>>> = Arc::new(ArcSwap::from_pointee(None));
        let src = TargetSource::Discovered {
            snapshot,
            metrics_port: 9404,
        };
        assert2::assert!(src.current().is_empty());
    }

    #[test]
    fn discovered_source_emits_one_target_per_broker() {
        let state = cluster_state_with(vec![
            BrokerView {
                id: 1,
                host: "broker1".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 2,
                host: "broker2".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 3,
                host: "broker3".into(),
                port: 9092,
                rack: None,
            },
        ]);
        let snapshot = Arc::new(ArcSwap::from_pointee(Some(state)));
        let src = TargetSource::Discovered {
            snapshot,
            metrics_port: 9404,
        };
        let mut out = src.current();
        out.sort_by_key(|t| t.broker_id);
        assert2::assert!(
            out == vec![
                ScrapeTarget {
                    broker_id: 1,
                    addr: "broker1:9404".into(),
                },
                ScrapeTarget {
                    broker_id: 2,
                    addr: "broker2:9404".into(),
                },
                ScrapeTarget {
                    broker_id: 3,
                    addr: "broker3:9404".into(),
                },
            ]
        );
    }

    #[test]
    fn discovered_source_skips_brokers_with_empty_host() {
        let state = cluster_state_with(vec![
            BrokerView {
                id: 1,
                host: "broker1".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 2,
                host: String::new(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 3,
                host: "broker3".into(),
                port: 9092,
                rack: None,
            },
        ]);
        let snapshot = Arc::new(ArcSwap::from_pointee(Some(state)));
        let src = TargetSource::Discovered {
            snapshot,
            metrics_port: 9404,
        };
        let mut out = src.current();
        out.sort_by_key(|t| t.broker_id);
        assert2::assert!(out.len() == 2);
        assert2::assert!(out.iter().map(|t| t.broker_id).collect::<Vec<_>>() == vec![1, 3]);
    }

    #[test]
    fn empty_host_warning_is_emitted_once_per_broker_id() {
        for (_case, broker_id, want) in [
            ("first warning for broker", 1_000_002, true),
            ("duplicate warning suppressed", 1_000_002, false),
            ("first warning for another broker", 1_000_003, true),
        ] {
            assert2::assert!(should_warn_empty_host(broker_id) == want);
        }
    }

    #[test]
    fn discovered_source_reflects_snapshot_updates() {
        let snapshot: Arc<ArcSwap<Option<ClusterState>>> = Arc::new(ArcSwap::from_pointee(None));
        let src = TargetSource::Discovered {
            snapshot: snapshot.clone(),
            metrics_port: 9404,
        };
        assert2::assert!(src.current().is_empty());

        // Now publish a snapshot.
        let state = cluster_state_with(vec![BrokerView {
            id: 7,
            host: "newbie".into(),
            port: 9092,
            rack: None,
        }]);
        snapshot.store(Arc::new(Some(state)));

        let out = src.current();
        assert2::assert!(
            out == vec![ScrapeTarget {
                broker_id: 7,
                addr: "newbie:9404".into()
            }]
        );
    }
}
