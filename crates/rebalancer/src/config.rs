//! Validated process policy for the standalone rebalancer.

use std::str::FromStr;

use crabka_units::{
    ByteSize, Ratio, Time,
    convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
    mebibytes, millis, minutes, percent, secs,
};
use refined_type::rule::GreaterUsize;

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
            .parse::<usize>()
            .map_err(|error| error.to_string())
            .and_then(Self::new)
    }
}

/// Remaining deployment-owned runtime and state-topic policy.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RebalancerRuntimePolicy {
    pub recovery_load_poll_interval: Time,
    pub executor_drain_timeout: Time,
    pub ingester_join_timeout: Time,
    pub scraper_http_timeout: Time,
    pub cancel_drain_timeout: Time,
    pub cancel_drain_poll_interval: Time,
    pub detector_history_capacity: PositiveUsize,
    pub state_topic_create_timeout: Time,
    pub state_loader_poll_interval: Time,
    pub state_loader_quiet_polls: PositiveUsize,
    pub state_fetch_max: ByteSize,
    pub state_produce_retry_attempts: PositiveUsize,
    pub state_produce_retry_backoff: Time,
    pub state_produce_timeout: Time,
    pub state_topic_min_cleanable_dirty_ratio: Ratio,
    pub state_topic_segment_interval: Time,
}

impl RebalancerRuntimePolicy {
    /// Validate scalar protocol bounds and coupled timing values.
    ///
    /// # Errors
    /// Returns an error when a value cannot safely reach its runtime owner.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            (
                "recovery load poll interval",
                self.recovery_load_poll_interval,
            ),
            ("executor drain timeout", self.executor_drain_timeout),
            ("ingester join timeout", self.ingester_join_timeout),
            ("scraper HTTP timeout", self.scraper_http_timeout),
            ("cancellation drain timeout", self.cancel_drain_timeout),
            (
                "cancellation poll interval",
                self.cancel_drain_poll_interval,
            ),
            (
                "state loader poll interval",
                self.state_loader_poll_interval,
            ),
            (
                "state produce retry backoff",
                self.state_produce_retry_backoff,
            ),
        ] {
            validate_positive_time(name, value)?;
        }
        for (name, value) in [
            (
                "state-topic creation timeout",
                self.state_topic_create_timeout,
            ),
            ("state produce timeout", self.state_produce_timeout),
            (
                "state-topic segment interval",
                self.state_topic_segment_interval,
            ),
        ] {
            validate_protocol_millis(name, value)?;
        }
        validate_fetch_max(self.state_fetch_max)?;
        let ratio = self.state_topic_min_cleanable_dirty_ratio.as_f64();
        if !ratio.is_finite() || ratio <= 0.0 || ratio >= 1.0 {
            return Err(
                "state-topic minimum cleanable dirty ratio must be between zero and one".to_owned(),
            );
        }
        if self.cancel_drain_poll_interval >= self.cancel_drain_timeout {
            return Err("cancellation poll interval must be below drain timeout".to_owned());
        }
        Ok(())
    }
}

impl Default for RebalancerRuntimePolicy {
    fn default() -> Self {
        Self {
            recovery_load_poll_interval: millis(100),
            executor_drain_timeout: secs(10),
            ingester_join_timeout: secs(5),
            scraper_http_timeout: secs(5),
            cancel_drain_timeout: secs(5),
            cancel_drain_poll_interval: millis(25),
            detector_history_capacity: PositiveUsize(10),
            state_topic_create_timeout: secs(10),
            state_loader_poll_interval: millis(100),
            state_loader_quiet_polls: PositiveUsize(5),
            state_fetch_max: mebibytes(1),
            state_produce_retry_attempts: PositiveUsize(50),
            state_produce_retry_backoff: millis(200),
            state_produce_timeout: secs(10),
            state_topic_min_cleanable_dirty_ratio: percent(1),
            state_topic_segment_interval: minutes(1),
        }
    }
}

fn validate_positive_time(name: &str, value: Time) -> Result<(), String> {
    std::time::Duration::try_from_secs_f64(value.secs_f64())
        .map_err(|error| format!("{name}: {error}"))
        .and_then(|duration| {
            if duration.is_zero() {
                Err(format!("{name} must be positive"))
            } else {
                Ok(())
            }
        })
}

fn validate_protocol_millis(name: &str, value: Time) -> Result<(), String> {
    let millis = value.millis_i64();
    if value.secs_f64().is_finite()
        && millis > 0
        && millis <= i64::from(i32::MAX)
        && Time::from_millis(millis) == value
    {
        Ok(())
    } else {
        Err(format!(
            "{name} must be a positive whole number of milliseconds within 1..=i32::MAX"
        ))
    }
}

fn validate_fetch_max(value: ByteSize) -> Result<(), String> {
    let bytes = value.bytes_f64();
    if bytes.is_finite() && bytes > 0.0 && bytes.fract() == 0.0 && bytes <= f64::from(i32::MAX) {
        Ok(())
    } else {
        Err(
            "state fetch maximum must be a positive whole number of bytes within 1..=i32::MAX"
                .to_owned(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_existing_runtime_values() {
        let policy = RebalancerRuntimePolicy::default();
        assert2::assert!(policy.validate().is_ok());
        assert2::assert!(policy.recovery_load_poll_interval == millis(100));
        assert2::assert!(policy.executor_drain_timeout == secs(10));
        assert2::assert!(policy.state_loader_quiet_polls.get() == 5);
        assert2::assert!(policy.state_fetch_max == mebibytes(1));
        assert2::assert!(policy.state_produce_retry_attempts.get() == 50);
        assert2::assert!(policy.state_topic_min_cleanable_dirty_ratio == percent(1));
    }

    #[test]
    fn rejects_invalid_runtime_relationships_and_protocol_values() {
        for policy in [
            RebalancerRuntimePolicy {
                cancel_drain_poll_interval: secs(5),
                ..Default::default()
            },
            RebalancerRuntimePolicy {
                state_fetch_max: ByteSize::from_bytes(u64::from(i32::MAX.unsigned_abs()) + 1),
                ..Default::default()
            },
            RebalancerRuntimePolicy {
                state_produce_timeout: Time::from_micros(1_500),
                ..Default::default()
            },
            RebalancerRuntimePolicy {
                state_topic_min_cleanable_dirty_ratio: Ratio::ZERO,
                ..Default::default()
            },
        ] {
            assert2::assert!(policy.validate().is_err());
        }
        assert2::assert!(PositiveUsize::new(0).is_err());
    }
}
