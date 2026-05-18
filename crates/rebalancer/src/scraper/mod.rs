//! Per-partition metric scraper. Spawned from the binary entry when
//! `--metrics-scrape-targets` is non-empty. The full `Scraper` task
//! is added in T8; this module currently exposes only the pure-logic
//! pieces (parser, target list, ring-buffer store).

pub mod parse;
pub mod targets;
pub mod window;

pub use parse::{MetricKind, ParsedSample};
pub use targets::{parse_targets, ScrapeTarget, TargetParseError};
pub use window::{UsageStore, Window, WindowConfig};
