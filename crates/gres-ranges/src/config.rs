//! Validated deployment policy for distributed range execution.

use std::str::FromStr;

use crabka_units::{
    ByteSize, Time, kibibytes, mebibytes, millis, minutes,
    prelude::{ByteSizeExt as _, TimeExt as _},
    secs,
};
use refined_type::rule::{GreaterU32, GreaterU64, GreaterUsize};

macro_rules! positive_newtype {
    ($name:ident, $primitive:ty, $rule:ident) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $name($primitive);

        impl $name {
            /// Validate a positive value.
            ///
            /// # Errors
            /// Returns an error when `value` is zero.
            pub fn new(value: $primitive) -> Result<Self, String> {
                $rule::<0>::new(value)
                    .map(|value| Self(value.into_value()))
                    .map_err(|error| error.to_string())
            }

            /// Return the validated primitive value.
            #[must_use]
            pub const fn get(self) -> $primitive {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value
                    .parse::<$primitive>()
                    .map_err(|error| error.to_string())
                    .and_then(Self::new)
            }
        }
    };
}

positive_newtype!(PositiveUsize, usize, GreaterUsize);
positive_newtype!(PositiveU32, u32, GreaterU32);
positive_newtype!(PositiveU64, u64, GreaterU64);

/// Process-owned limits and pacing for distributed ranges.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RangeRuntimePolicy {
    pub join: crabka_pgexec::scanner::JoinPolicy,
    pub rpc_frame_max: ByteSize,
    pub rpc_request_timeout: Time,
    pub rpc_server_idle_timeout: Time,
    pub rpc_pool_idle_ttl: Time,
    pub rpc_pool_max_idle_per_endpoint: PositiveUsize,
    pub remote_session_idle: Time,
    pub remote_session_max: PositiveUsize,
    pub range0_wait_timeout: Time,
    pub range0_barrier_reply_budget: Time,
    pub cross_range_lock_wait_cap: Time,
    pub durable_inspect_max_records: PositiveU32,
    pub durable_inspect_max_size: ByteSize,
    pub decision_release_lag_retries: PositiveU32,
    pub decision_release_retry_backoff: Time,
    pub tso_heartbeat_interval: Time,
    pub logical_min_persist_interval: Time,
    pub logical_base_persist_stride: PositiveU64,
    pub logical_max_persist_stride: PositiveU64,
    pub hlc_horizon_headroom: Time,
}

impl RangeRuntimePolicy {
    /// Validate relationships between independently parsed policy values.
    ///
    /// # Errors
    ///
    /// Returns an error when a value is not positive or is not finite.
    ///
    /// Returns an error when coupled limits are inconsistent.
    pub fn validate(&self) -> Result<(), String> {
        self.join.validate().map_err(|error| error.to_string())?;
        for (name, value) in [
            ("RPC request timeout", self.rpc_request_timeout),
            ("RPC server idle timeout", self.rpc_server_idle_timeout),
            ("RPC pool idle TTL", self.rpc_pool_idle_ttl),
            ("remote session idle", self.remote_session_idle),
            ("range-0 wait timeout", self.range0_wait_timeout),
            (
                "range-0 barrier reply budget",
                self.range0_barrier_reply_budget,
            ),
            ("cross-range lock wait cap", self.cross_range_lock_wait_cap),
            (
                "decision release retry backoff",
                self.decision_release_retry_backoff,
            ),
            ("TSO heartbeat interval", self.tso_heartbeat_interval),
            (
                "logical persistence interval",
                self.logical_min_persist_interval,
            ),
            ("HLC horizon headroom", self.hlc_horizon_headroom),
        ] {
            if !value.secs_f64().is_finite() || value <= Time::ZERO {
                return Err(format!("{name} must be positive and finite"));
            }
        }
        if !self.rpc_frame_max.bytes_f64().is_finite() || self.rpc_frame_max <= kibibytes(4) {
            return Err("RPC frame maximum must exceed the fixed 4KiB envelope".to_owned());
        }
        if !self.durable_inspect_max_size.bytes_f64().is_finite()
            || self.durable_inspect_max_size <= ByteSize::ZERO
            || self.durable_inspect_max_size.bytes_u64() > u64::from(u32::MAX)
        {
            return Err("durable inspection maximum size must be positive and finite".to_owned());
        }
        if self.rpc_pool_idle_ttl >= self.rpc_server_idle_timeout {
            return Err("RPC pool idle TTL must be below server idle timeout".to_owned());
        }
        if self.range0_barrier_reply_budget >= self.rpc_request_timeout
            || self.cross_range_lock_wait_cap >= self.rpc_request_timeout
        {
            return Err(
                "range reply and lock budgets must be below RPC request timeout".to_owned(),
            );
        }
        if self.logical_base_persist_stride.get() > self.logical_max_persist_stride.get() {
            return Err("logical base persistence stride exceeds maximum".to_owned());
        }
        Ok(())
    }
}

impl Default for RangeRuntimePolicy {
    fn default() -> Self {
        Self {
            join: crabka_pgexec::scanner::JoinPolicy::default(),
            rpc_frame_max: mebibytes(1),
            rpc_request_timeout: secs(5),
            rpc_server_idle_timeout: minutes(1),
            rpc_pool_idle_ttl: secs(5),
            rpc_pool_max_idle_per_endpoint: PositiveUsize(32),
            remote_session_idle: minutes(1),
            remote_session_max: PositiveUsize(1024),
            range0_wait_timeout: secs(10),
            range0_barrier_reply_budget: secs(4),
            cross_range_lock_wait_cap: secs(2),
            durable_inspect_max_records: PositiveU32(4096),
            durable_inspect_max_size: kibibytes(128),
            decision_release_lag_retries: PositiveU32(10),
            decision_release_retry_backoff: millis(200),
            tso_heartbeat_interval: millis(10),
            logical_min_persist_interval: millis(100),
            logical_base_persist_stride: PositiveU64(1024),
            logical_max_persist_stride: PositiveU64(1 << 24),
            hlc_horizon_headroom: millis(128),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_existing_runtime_values() {
        let policy = RangeRuntimePolicy::default();
        assert2::assert!(policy.validate().is_ok());
        assert2::assert!(policy.rpc_frame_max == mebibytes(1));
        assert2::assert!(policy.rpc_request_timeout == secs(5));
        assert2::assert!(policy.rpc_pool_max_idle_per_endpoint.get() == 32);
        assert2::assert!(policy.remote_session_max.get() == 1024);
        assert2::assert!(policy.durable_inspect_max_records.get() == 4096);
        assert2::assert!(policy.logical_base_persist_stride.get() == 1024);
        assert2::assert!(policy.logical_max_persist_stride.get() == 1 << 24);
    }

    #[test]
    fn rejects_invalid_relationships() {
        for policy in [
            RangeRuntimePolicy {
                rpc_pool_idle_ttl: minutes(1),
                ..RangeRuntimePolicy::default()
            },
            RangeRuntimePolicy {
                range0_barrier_reply_budget: secs(5),
                ..RangeRuntimePolicy::default()
            },
            RangeRuntimePolicy {
                logical_base_persist_stride: PositiveU64::new(9).unwrap(),
                logical_max_persist_stride: PositiveU64::new(8).unwrap(),
                ..RangeRuntimePolicy::default()
            },
        ] {
            assert2::assert!(policy.validate().is_err());
        }
    }

    #[test]
    fn refined_counts_reject_zero() {
        assert2::assert!(PositiveUsize::new(0).is_err());
        assert2::assert!(PositiveU32::new(0).is_err());
        assert2::assert!(PositiveU64::new(0).is_err());
    }
}
