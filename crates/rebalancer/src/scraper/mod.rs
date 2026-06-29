//! Per-partition metric scraper. Spawned from the binary entry when
//! `--metrics-scrape-targets` is non-empty.

pub mod parse;
pub mod targets;
pub mod window;

pub use parse::{MetricKind, ParsedSample};
pub use targets::{ScrapeTarget, TargetParseError, TargetSource, parse_targets};
pub use window::{UsageStore, Window, WindowConfig};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Edge-triggered log level for a scrape outcome.
///
/// The scraper polls each target every interval; logging every failure of a
/// permanently-dead target floods the log. Instead we emit a `Warn` on the
/// transition from "ok or unknown" → "failed", a `Recovered` (info) on the
/// transition from "failed" → "ok", and a quiet `Debug` for steady states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrapeLogLevel {
    /// First-time failure (previous state was Ok or unknown). Emit at WARN.
    Warn,
    /// Recovery from a previously-failed state. Emit at INFO.
    Recovered,
    /// Steady state (ok→ok or fail→fail). Emit at DEBUG.
    Debug,
}

/// Decide how loudly to log a scrape outcome, given the previous outcome.
///
/// `prev` is `None` on first observation, `Some(true)` if the last scrape
/// succeeded, `Some(false)` if it failed. `current` is the outcome of this
/// scrape (`true` = ok, `false` = failed).
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
    interval: Duration,
    store: Arc<UsageStore>,
    http: reqwest::Client,
    shutdown: CancellationToken,
    /// Per-broker last-scrape outcome for edge-triggered logging.
    /// Pruned each tick: brokers that disappear from `source.current()`
    /// are dropped on the next iteration.
    last_ok: HashMap<i32, bool>,
}

impl Scraper {
    #[must_use]
    pub fn new(
        source: TargetSource,
        interval: Duration,
        store: Arc<UsageStore>,
        shutdown: CancellationToken,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
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
        info!(interval_secs = self.interval.as_secs(), "scraper started");
        let mut ticker = tokio::time::interval(self.interval);
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

/// Outcome of a single scrape attempt, captured before we decide log level.
enum Outcome {
    Ok { count: usize },
    BodyReadFailed(String),
    NonSuccess(String),
    TransportFailure(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    fn first_failure_warns() {
        assert!(classify(None, false) == ScrapeLogLevel::Warn);
    }

    #[test]
    fn ok_to_fail_warns() {
        assert!(classify(Some(true), false) == ScrapeLogLevel::Warn);
    }

    #[test]
    fn repeated_failure_is_quiet() {
        assert!(classify(Some(false), false) == ScrapeLogLevel::Debug);
    }

    #[test]
    fn recovery_is_info() {
        assert!(classify(Some(false), true) == ScrapeLogLevel::Recovered);
    }

    #[test]
    fn first_success_is_quiet() {
        // No prior failure → no "recovered" log on first scrape.
        assert!(classify(None, true) == ScrapeLogLevel::Debug);
    }

    #[test]
    fn ok_to_ok_is_quiet() {
        assert!(classify(Some(true), true) == ScrapeLogLevel::Debug);
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
        assert!(
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
        use crate::model::{BrokerView, ClusterState};
        use arc_swap::ArcSwap;

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
            scrape_interval: Duration::from_secs(1),
            retention: Duration::from_mins(1),
        }));
        let mut scraper = Scraper::new(
            TargetSource::Discovered {
                snapshot: snapshot.clone(),
                metrics_port: 1, // bogus port — scrapes will fail but that's fine
            },
            Duration::from_millis(50),
            store,
            CancellationToken::new(),
        );

        // First tick: scrape both brokers (they'll fail; we don't care).
        scraper.tick_once().await;
        assert!(scraper.last_ok.len() == 2);
        assert!(scraper.last_ok.contains_key(&1));
        assert!(scraper.last_ok.contains_key(&2));

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
        assert!(scraper.last_ok.len() == 1);
        assert!(scraper.last_ok.contains_key(&1));
        assert!(!scraper.last_ok.contains_key(&2));
    }

    #[tokio::test]
    async fn tick_once_inserts_samples_only_on_success_status() {
        let metric_body = "crabka_broker_partition_disk_bytes{topic=\"t\",partition=\"0\"} 42\n";

        let failed_addr = one_response_server("500 Internal Server Error", metric_body).await;
        let failed_store = Arc::new(UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(1),
            retention: Duration::from_mins(1),
        }));
        let mut failed_scraper = Scraper::new(
            TargetSource::Static(vec![ScrapeTarget {
                broker_id: 1,
                addr: failed_addr,
            }]),
            Duration::from_millis(50),
            failed_store.clone(),
            CancellationToken::new(),
        );
        failed_scraper.tick_once().await;
        assert!(failed_scraper.last_ok.get(&1) == Some(&false));
        assert!(
            failed_store
                .disk_bytes_avg(1, "t", 0, Window::FiveMin, crate::goals::now_ms())
                .is_none()
        );

        let ok_addr = one_response_server("200 OK", metric_body).await;
        let ok_store = Arc::new(UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(1),
            retention: Duration::from_mins(1),
        }));
        let mut ok_scraper = Scraper::new(
            TargetSource::Static(vec![ScrapeTarget {
                broker_id: 1,
                addr: ok_addr,
            }]),
            Duration::from_millis(50),
            ok_store.clone(),
            CancellationToken::new(),
        );
        ok_scraper.tick_once().await;
        assert!(ok_scraper.last_ok.get(&1) == Some(&true));
        assert!(
            ok_store
                .disk_bytes_avg(1, "t", 0, Window::FiveMin, crate::goals::now_ms())
                .is_some_and(|v| (v - 42.0).abs() < 1e-9)
        );
    }

    #[tokio::test]
    async fn run_waits_until_shutdown() {
        let shutdown = CancellationToken::new();
        let scraper = Scraper::new(
            TargetSource::Static(vec![]),
            Duration::from_secs(60),
            Arc::new(UsageStore::default()),
            shutdown.clone(),
        );

        let handle = tokio::spawn(scraper.run());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle.is_finished());
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("scraper should stop after cancellation")
            .expect("scraper task should join");
    }
}
