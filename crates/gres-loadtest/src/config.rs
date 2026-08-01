//! Validated process policy for the Gres load-test harness.

use std::str::FromStr;

use crabka_units::prelude::*;
use refined_type::rule::{GreaterEqualUsize, GreaterUsize};

/// A positive process-owned count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveUsize(usize);

impl PositiveUsize {
    /// Validate a positive count.
    ///
    /// # Errors
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated primitive value.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl FromStr for PositiveUsize {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// A non-negative process-owned count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonNegativeUsize(usize);

impl NonNegativeUsize {
    /// Validate a non-negative count.
    ///
    /// # Errors
    /// Returns an error when the refinement cannot represent `value`.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterEqualUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated primitive value.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl FromStr for NonNegativeUsize {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Operational policy shared by internal and external harness runs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LoadtestRuntimePolicy {
    pub launch_timeout: Time,
    pub kill_timeout: Time,
    pub log_drain_timeout: Time,
    pub broker_poll_interval: Time,
    pub topic_create_timeout: Time,
    pub log_tail_lines: PositiveUsize,
    pub max_serialization_retries: NonNegativeUsize,
    pub operation_timeout: Time,
    pub connect_timeout: Time,
    pub startup_deadline: Time,
    pub startup_retry_delay: Time,
    pub shutdown_grace: Time,
    pub reconnect_backoff_min: Time,
    pub reconnect_backoff_max: Time,
    pub histogram_min: Time,
    pub histogram_max: Time,
    pub min_pacing_wait: Time,
    pub read_slice_rows: PositiveUsize,
    pub seed_batch_rows: PositiveUsize,
    pub proxy_min_burst: ByteSize,
    pub proxy_burst_window: Time,
    pub proxy_delay_queue_depth: PositiveUsize,
    pub sample_interval: Time,
    pub fault_window: Time,
    pub timeline_row_cap: PositiveUsize,
    pub deviation_threshold: Ratio,
    pub min_flap_period: Time,
    pub compare_hlc_max_offset: Time,
}

impl LoadtestRuntimePolicy {
    /// Validate coupled policy relationships.
    ///
    /// # Errors
    /// Returns an error when a scalar or relationship is invalid.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("launch timeout", self.launch_timeout),
            ("kill timeout", self.kill_timeout),
            ("log drain timeout", self.log_drain_timeout),
            ("broker poll interval", self.broker_poll_interval),
            ("topic create timeout", self.topic_create_timeout),
            ("operation timeout", self.operation_timeout),
            ("connect timeout", self.connect_timeout),
            ("startup deadline", self.startup_deadline),
            ("startup retry delay", self.startup_retry_delay),
            ("shutdown grace", self.shutdown_grace),
            ("minimum reconnect backoff", self.reconnect_backoff_min),
            ("maximum reconnect backoff", self.reconnect_backoff_max),
            ("histogram minimum", self.histogram_min),
            ("histogram maximum", self.histogram_max),
            ("minimum pacing wait", self.min_pacing_wait),
            ("proxy burst window", self.proxy_burst_window),
            ("sample interval", self.sample_interval),
            ("fault window", self.fault_window),
            ("minimum flap period", self.min_flap_period),
            ("compare HLC maximum offset", self.compare_hlc_max_offset),
        ] {
            let duration = std::time::Duration::try_from_secs_f64(value.secs_f64())
                .map_err(|error| format!("{name}: {error}"))?;
            if duration.is_zero() {
                return Err(format!("{name} must be positive"));
            }
        }
        if self.reconnect_backoff_min > self.reconnect_backoff_max {
            return Err("minimum reconnect backoff must not exceed maximum".to_owned());
        }
        if self.histogram_min > self.histogram_max {
            return Err("histogram minimum must not exceed maximum".to_owned());
        }
        if self.startup_retry_delay > self.startup_deadline {
            return Err("startup retry delay must not exceed startup deadline".to_owned());
        }
        if self.proxy_min_burst.bytes_f64() <= 0.0 {
            return Err("proxy minimum burst must be positive".to_owned());
        }
        let threshold = self.deviation_threshold.as_f64();
        if !threshold.is_finite() || !(0.0..=1.0).contains(&threshold) {
            return Err("deviation threshold must be between zero and one".to_owned());
        }
        u32::try_from(self.max_serialization_retries.get())
            .map_err(|_| "serialization retries must fit u32".to_owned())?;
        i64::try_from(self.read_slice_rows.get())
            .map_err(|_| "read slice rows must fit i64".to_owned())?;
        u32::try_from(self.seed_batch_rows.get())
            .map_err(|_| "seed batch rows must fit u32".to_owned())?;
        Ok(())
    }
}

impl Default for LoadtestRuntimePolicy {
    fn default() -> Self {
        Self {
            launch_timeout: minutes(2),
            kill_timeout: secs(10),
            log_drain_timeout: secs(5),
            broker_poll_interval: millis(100),
            topic_create_timeout: secs(30),
            log_tail_lines: PositiveUsize(40),
            max_serialization_retries: NonNegativeUsize(5),
            operation_timeout: secs(30),
            connect_timeout: secs(5),
            startup_deadline: secs(30),
            startup_retry_delay: millis(250),
            shutdown_grace: secs(5),
            reconnect_backoff_min: millis(100),
            reconnect_backoff_max: secs(2),
            histogram_min: micros(1),
            histogram_max: secs(60),
            min_pacing_wait: micros(500),
            read_slice_rows: PositiveUsize(1_024),
            seed_batch_rows: PositiveUsize(500),
            proxy_min_burst: kibibytes(64),
            proxy_burst_window: millis(100),
            proxy_delay_queue_depth: PositiveUsize(256),
            sample_interval: secs(1),
            fault_window: secs(5),
            timeline_row_cap: PositiveUsize(60),
            deviation_threshold: percent(30),
            min_flap_period: secs(1),
            compare_hlc_max_offset: millis(250),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_existing_values() {
        let policy = LoadtestRuntimePolicy::default();
        assert2::assert!(policy.validate().is_ok());
        assert2::assert!(policy.launch_timeout == minutes(2));
        assert2::assert!(policy.read_slice_rows.get() == 1_024);
        assert2::assert!(policy.proxy_min_burst == kibibytes(64));
        assert2::assert!(policy.deviation_threshold == percent(30));
    }

    #[test]
    fn rejects_invalid_relationships() {
        for policy in [
            LoadtestRuntimePolicy {
                reconnect_backoff_min: secs(3),
                ..Default::default()
            },
            LoadtestRuntimePolicy {
                histogram_min: secs(61),
                ..Default::default()
            },
            LoadtestRuntimePolicy {
                deviation_threshold: percent(101),
                ..Default::default()
            },
        ] {
            assert2::assert!(policy.validate().is_err());
        }
        assert2::assert!(PositiveUsize::new(0).is_err());
    }
}
