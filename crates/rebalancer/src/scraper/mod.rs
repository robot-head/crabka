//! Per-partition metric scraper. Spawned from the binary entry when
//! `--metrics-scrape-targets` is non-empty.

pub mod parse;
pub mod targets;
pub mod window;

pub use parse::{MetricKind, ParsedSample};
pub use targets::{ScrapeTarget, TargetParseError, parse_targets};
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
    targets: Vec<ScrapeTarget>,
    interval: Duration,
    store: Arc<UsageStore>,
    http: reqwest::Client,
    shutdown: CancellationToken,
    /// Per-broker last-scrape outcome for edge-triggered logging.
    /// `Some(true)` = last scrape ok, `Some(false)` = last scrape failed,
    /// absent = never scraped successfully or unsuccessfully yet.
    last_ok: HashMap<i32, bool>,
}

impl Scraper {
    #[must_use]
    pub fn new(
        targets: Vec<ScrapeTarget>,
        interval: Duration,
        store: Arc<UsageStore>,
        shutdown: CancellationToken,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client");
        Self {
            targets,
            interval,
            store,
            http,
            shutdown,
            last_ok: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        info!(
            target_count = self.targets.len(),
            interval_secs = self.interval.as_secs(),
            "scraper started"
        );
        let mut ticker = tokio::time::interval(self.interval);
        // Don't skip the first tick — pull metrics immediately on startup.
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
        // Clone the targets out so we can mutate `self.last_ok` inside the
        // loop without holding a borrow on `self.targets`.
        let targets = self.targets.clone();
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

    #[test]
    fn first_failure_warns() {
        assert_eq!(classify(None, false), ScrapeLogLevel::Warn);
    }

    #[test]
    fn ok_to_fail_warns() {
        assert_eq!(classify(Some(true), false), ScrapeLogLevel::Warn);
    }

    #[test]
    fn repeated_failure_is_quiet() {
        assert_eq!(classify(Some(false), false), ScrapeLogLevel::Debug);
    }

    #[test]
    fn recovery_is_info() {
        assert_eq!(classify(Some(false), true), ScrapeLogLevel::Recovered);
    }

    #[test]
    fn first_success_is_quiet() {
        // No prior failure → no "recovered" log on first scrape.
        assert_eq!(classify(None, true), ScrapeLogLevel::Debug);
    }

    #[test]
    fn ok_to_ok_is_quiet() {
        assert_eq!(classify(Some(true), true), ScrapeLogLevel::Debug);
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
        assert_eq!(
            levels,
            vec![
                ScrapeLogLevel::Warn,
                ScrapeLogLevel::Debug,
                ScrapeLogLevel::Debug,
                ScrapeLogLevel::Recovered,
            ]
        );
    }
}
