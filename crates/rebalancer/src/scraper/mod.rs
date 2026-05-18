//! Per-partition metric scraper. Spawned from the binary entry when
//! `--metrics-scrape-targets` is non-empty.

pub mod parse;
pub mod targets;
pub mod window;

pub use parse::{MetricKind, ParsedSample};
pub use targets::{ScrapeTarget, TargetParseError, parse_targets};
pub use window::{UsageStore, Window, WindowConfig};

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

pub struct Scraper {
    targets: Vec<ScrapeTarget>,
    interval: Duration,
    store: Arc<UsageStore>,
    http: reqwest::Client,
    shutdown: CancellationToken,
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
        }
    }

    pub async fn run(self) {
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

    async fn tick_once(&self) {
        let now_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0);
        for target in &self.targets {
            let url = format!("http://{}/metrics", target.addr);
            match self.http.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => match resp.text().await {
                    Ok(body) => {
                        let samples = parse::parse(&body);
                        debug!(broker_id = target.broker_id, url = %url, count = samples.len(), "scrape ok");
                        self.store.insert(target.broker_id, samples, now_ms);
                    }
                    Err(e) => {
                        warn!(broker_id = target.broker_id, url = %url, error = %e, "scrape body read failed");
                    }
                },
                Ok(resp) => {
                    warn!(broker_id = target.broker_id, url = %url, status = %resp.status(), "scrape returned non-success");
                }
                Err(e) => {
                    warn!(broker_id = target.broker_id, url = %url, error = %e, "scrape transport failure");
                }
            }
        }
    }
}
