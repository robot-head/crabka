//! Per-partition metric scraper. The binary entry spawns it when
//! `--metrics-scrape-targets` is non-empty.

pub mod parse;
pub mod targets;
pub mod window;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crabka_units::{fmt::Human as _, prelude::*};
pub use parse::{MetricKind, ParsedSample};
pub use targets::{ScrapeTarget, TargetParseError, TargetSource, parse_targets};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
pub use window::{UsageStore, Window, WindowConfig};

/// Edge-triggered log level for a scrape outcome.
///
/// The scraper polls each target every interval, and a log line for every
/// failure of a permanently dead target floods the log. The scraper therefore
/// emits a `Warn` on the transition from "ok or unknown" to "failed", a
/// `Recovered` at info level on the transition from "failed" to "ok", and a
/// quiet `Debug` for steady states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrapeLogLevel {
    /// First failure, where the previous state was Ok or unknown. Emit at
    /// WARN.
    Warn,
    /// Recovery from a previously failed state. Emit at INFO.
    Recovered,
    /// Steady state, either ok to ok or fail to fail. Emit at DEBUG.
    Debug,
}

/// Decide how loudly to log a scrape outcome, given the previous outcome.
///
/// `prev` is `None` on the first observation, `Some(true)` if the last scrape
/// succeeded, and `Some(false)` if it failed. `current` is the outcome of this
/// scrape: `true` for ok and `false` for failed.
#[must_use]
pub fn classify(prev: Option<bool>, current: bool) -> ScrapeLogLevel {
    match (prev, current) {
        // First-ever failure, or transition ok → fail: warn.
        (None | Some(true), false) => ScrapeLogLevel::Warn,
        // Recovery: previously failed, now ok.
        (Some(false), true) => ScrapeLogLevel::Recovered,
        // Steady state: ok→ok (incl. first-time success) or fail→fail.
        (None | Some(true), true) | (Some(false), false) => ScrapeLogLevel::Debug,
    }
}

pub struct Scraper {
    source: TargetSource,
    interval: Time,
    store: Arc<UsageStore>,
    http: reqwest::Client,
    shutdown: CancellationToken,
    /// Per-broker last-scrape outcome for edge-triggered logging. Each tick
    /// prunes the map: it drops brokers that disappeared from
    /// `source.current()` on the next iteration.
    last_ok: HashMap<i32, bool>,
}

impl Scraper {
    #[must_use]
    /// # Panics
    /// Panics if an internal lock is poisoned or validated cluster state is missing an assignment required by the plan.
    pub fn new(
        source: TargetSource,
        interval: Time,
        store: Arc<UsageStore>,
        shutdown: CancellationToken,
    ) -> Self {
        Self::new_with_http_timeout(
            source,
            interval,
            store,
            shutdown,
            crate::config::RebalancerRuntimePolicy::default().scraper_http_timeout,
        )
    }

    #[must_use]
    /// # Panics
    /// Panics if the validated HTTP client configuration cannot be built.
    pub fn new_with_http_timeout(
        source: TargetSource,
        interval: Time,
        store: Arc<UsageStore>,
        shutdown: CancellationToken,
        http_timeout: Time,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(http_timeout.to_std())
            .build()
            .expect("reqwest client");
        Self {
            source,
            interval,
            store,
            http,
            shutdown,
            last_ok: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        info!(interval = %self.interval.human(), "scraper started");
        let mut ticker = tokio::time::interval(self.interval.to_std());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = self.shutdown.cancelled() => {
                    info!("scraper shutting down");
                    return;
                }
            }
            self.tick_once().await;
        }
    }

    async fn tick_once(&mut self) {
        let now_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0);
        // Refresh targets each tick — `TargetSource::Discovered` may have
        // gained or lost brokers since the last iteration.
        let targets = self.source.current();
        // Prune stale `last_ok` entries: any broker_id no longer in the
        // current target list dropped out of the snapshot or was removed
        // from the static config.
        {
            use std::collections::HashSet;
            let current_ids: HashSet<i32> = targets.iter().map(|t| t.broker_id).collect();
            self.last_ok.retain(|id, _| current_ids.contains(id));
        }
        for target in &targets {
            let url = format!("http://{}/metrics", target.addr);
            let (ok, outcome) = match self.http.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => match resp.text().await {
                    Ok(body) => {
                        let samples = parse::parse(&body);
                        let count = samples.len();
                        self.store.insert(target.broker_id, samples, now_ms);
                        (true, Outcome::Ok { count })
                    }
                    Err(e) => (false, Outcome::BodyReadFailed(e.to_string())),
                },
                Ok(resp) => (false, Outcome::NonSuccess(resp.status().to_string())),
                Err(e) => (false, Outcome::TransportFailure(e.to_string())),
            };
            let prev = self.last_ok.insert(target.broker_id, ok);
            Self::log_outcome(target.broker_id, &url, prev, ok, &outcome);
        }
    }

    fn log_outcome(
        broker_id: i32,
        url: &str,
        prev: Option<bool>,
        current: bool,
        outcome: &Outcome,
    ) {
        let level = classify(prev, current);
        match (level, outcome) {
            // Steady-state success — debug as before.
            (ScrapeLogLevel::Debug, Outcome::Ok { count }) => {
                debug!(broker_id, url = %url, count = count, "scrape ok");
            }
            // Recovery from previously-failed state.
            (ScrapeLogLevel::Recovered, Outcome::Ok { count }) => {
                info!(broker_id, url = %url, count = count, "scraper recovered");
            }
            // Edge-triggered failure: first failure (or ok→fail transition).
            (ScrapeLogLevel::Warn, Outcome::BodyReadFailed(e)) => {
                warn!(broker_id, url = %url, error = %e, "scrape body read failed");
            }
            (ScrapeLogLevel::Warn, Outcome::NonSuccess(status)) => {
                warn!(broker_id, url = %url, status = %status, "scrape returned non-success");
            }
            (ScrapeLogLevel::Warn, Outcome::TransportFailure(e)) => {
                warn!(broker_id, url = %url, error = %e, "scrape transport failure");
            }
            // Steady-state failure: keep noise out of WARN, demote to DEBUG.
            (ScrapeLogLevel::Debug, Outcome::BodyReadFailed(e)) => {
                debug!(broker_id, url = %url, error = %e, "scrape body read failed (still failing)");
            }
            (ScrapeLogLevel::Debug, Outcome::NonSuccess(status)) => {
                debug!(broker_id, url = %url, status = %status, "scrape returned non-success (still failing)");
            }
            (ScrapeLogLevel::Debug, Outcome::TransportFailure(e)) => {
                debug!(broker_id, url = %url, error = %e, "scrape transport failure (still failing)");
            }
            // Unreachable combinations (Recovered+failure, Warn+success).
            (ScrapeLogLevel::Recovered, _) | (ScrapeLogLevel::Warn, Outcome::Ok { .. }) => {
                debug!(broker_id, url = %url, "scrape outcome (unexpected level/outcome combo)");
            }
        }
    }
}

/// Outcome of a single scrape attempt, captured before the log level is
/// decided.
enum Outcome {
    Ok { count: usize },
    BodyReadFailed(String),
    NonSuccess(String),
    TransportFailure(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crabka_units::prelude::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    async fn one_response_server(status: &str, body: &str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let status = status.to_string();
        let body = body.to_string();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = [0_u8; 1024];
            let _ = stream.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });
        addr.to_string()
    }

    #[test]
    fn classify_transitions_have_expected_log_levels() {
        let cases = [
            ("first failure", None, false, ScrapeLogLevel::Warn),
            ("ok to failure", Some(true), false, ScrapeLogLevel::Warn),
            (
                "repeated failure",
                Some(false),
                false,
                ScrapeLogLevel::Debug,
            ),
            ("recovery", Some(false), true, ScrapeLogLevel::Recovered),
            // No prior failure means a first success is not a recovery.
            ("first success", None, true, ScrapeLogLevel::Debug),
            ("repeated success", Some(true), true, ScrapeLogLevel::Debug),
        ];
        for (_name, previous, current, expected) in cases {
            assert2::assert!(classify(previous, current) == expected);
        }
    }

    #[test]
    fn edge_triggered_sequence_three_failures_then_recovery() {
        // Simulate three failures followed by a recovery: only the first
        // failure should be warn-level, the next two debug, then recovery
        // info.
        let mut prev: Option<bool> = None;
        let mut levels = Vec::new();
        for current in [false, false, false, true] {
            levels.push(classify(prev, current));
            prev = Some(current);
        }
        assert2::assert!(
            levels
                == vec![
                    ScrapeLogLevel::Warn,
                    ScrapeLogLevel::Debug,
                    ScrapeLogLevel::Debug,
                    ScrapeLogLevel::Recovered,
                ]
        );
    }

    #[tokio::test]
    async fn tick_once_prunes_last_ok_for_brokers_no_longer_in_source() {
        use arc_swap::ArcSwap;

        use crate::model::{BrokerView, ClusterState};

        let snapshot: Arc<ArcSwap<Option<ClusterState>>> =
            Arc::new(ArcSwap::from_pointee(Some(ClusterState {
                cluster_id: None,
                snapshot_at_ms: 0,
                brokers: vec![
                    BrokerView {
                        id: 1,
                        host: "127.0.0.1".into(),
                        port: 1,
                        rack: None,
                    },
                    BrokerView {
                        id: 2,
                        host: "127.0.0.1".into(),
                        port: 1,
                        rack: None,
                    },
                ],
                partitions: vec![],
                in_flight_reassignments: vec![],
            })));

        let store = Arc::new(UsageStore::new(WindowConfig {
            scrape_interval: secs(1),
            retention: minutes(1),
        }));
        let mut scraper = Scraper::new(
            TargetSource::Discovered {
                snapshot: snapshot.clone(),
                metrics_port: 1, // bogus port — scrapes will fail but that's fine
            },
            millis(50),
            store,
            CancellationToken::new(),
        );

        // First tick: scrape both brokers (they'll fail; we don't care).
        scraper.tick_once().await;
        assert2::assert!(
            scraper.last_ok.keys().copied().collect::<BTreeSet<_>>() == BTreeSet::from([1, 2])
        );

        // Snapshot loses broker 2.
        snapshot.store(Arc::new(Some(ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![BrokerView {
                id: 1,
                host: "127.0.0.1".into(),
                port: 1,
                rack: None,
            }],
            partitions: vec![],
            in_flight_reassignments: vec![],
        })));

        scraper.tick_once().await;
        // last_ok should now only contain broker 1.
        assert2::assert!(
            scraper.last_ok.keys().copied().collect::<BTreeSet<_>>() == BTreeSet::from([1])
        );
    }

    #[tokio::test]
    async fn tick_once_inserts_samples_only_on_success_status() {
        let metric_body = "crabka_broker_partition_disk_bytes{topic=\"t\",partition=\"0\"} 42\n";

        let failed_addr = one_response_server("500 Internal Server Error", metric_body).await;
        let failed_store = Arc::new(UsageStore::new(WindowConfig {
            scrape_interval: secs(1),
            retention: minutes(1),
        }));
        let mut failed_scraper = Scraper::new(
            TargetSource::Static(vec![ScrapeTarget {
                broker_id: 1,
                addr: failed_addr,
            }]),
            millis(50),
            failed_store.clone(),
            CancellationToken::new(),
        );
        failed_scraper.tick_once().await;
        assert2::assert!(failed_scraper.last_ok.get(&1) == Some(&false));
        assert2::assert!(
            failed_store
                .disk_bytes_avg(1, "t", 0, Window::FiveMin, crate::goals::now_ms())
                .is_none()
        );

        let ok_addr = one_response_server("200 OK", metric_body).await;
        let ok_store = Arc::new(UsageStore::new(WindowConfig {
            scrape_interval: secs(1),
            retention: minutes(1),
        }));
        let mut ok_scraper = Scraper::new(
            TargetSource::Static(vec![ScrapeTarget {
                broker_id: 1,
                addr: ok_addr,
            }]),
            millis(50),
            ok_store.clone(),
            CancellationToken::new(),
        );
        ok_scraper.tick_once().await;
        assert2::assert!(ok_scraper.last_ok.get(&1) == Some(&true));
        assert2::assert!(
            ok_store
                .disk_bytes_avg(1, "t", 0, Window::FiveMin, crate::goals::now_ms())
                .is_some_and(|v| v == bytes(42))
        );
    }

    #[tokio::test]
    async fn run_waits_until_shutdown() {
        let shutdown = CancellationToken::new();
        let scraper = Scraper::new(
            TargetSource::Static(vec![]),
            minutes(1),
            Arc::new(UsageStore::default()),
            shutdown.clone(),
        );

        let handle = tokio::spawn(scraper.run());
        tokio::time::sleep(millis(10).to_std()).await;
        assert2::assert!(!handle.is_finished());
        shutdown.cancel();
        tokio::time::timeout(secs(1).to_std(), handle)
            .await
            .expect("scraper should stop after cancellation")
            .expect("scraper task should join");
    }
}
