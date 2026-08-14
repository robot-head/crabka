//! KIP-13 + KIP-124 + KIP-257 client quotas.

use crabka_metadata::{EntityKey, MetadataImage};
use crabka_units::{
    ByteRate, Time,
    convert::{ByteRateExt as _, TimeExt},
};
use num_traits::cast::{NumCast, ToPrimitive as _};

mod buckets;
mod controller_mutation;
mod lookup;
mod producer;
mod request;

pub use buckets::QuotaBuckets;
pub(crate) use controller_mutation::apply_controller_mutation_quota_mode;
pub use controller_mutation::consume_controller_mutation_quota;
pub use lookup::{lookup_ip_quota, lookup_ip_quota_with_key, lookup_quota, lookup_quota_with_key};
pub use producer::consume_producer_quota;
pub use request::consume_request_quota;

mod refresh;
pub use refresh::{ImageWatcher, run};

#[derive(Clone, Copy)]
struct QuotaConsumption<'a> {
    image: &'a MetadataImage,
    buckets: &'a QuotaBuckets,
    principal: &'a str,
    client_id: &'a str,
    quota_key: &'a str,
    amount: u64,
}

fn consume_configured_quota(
    request: QuotaConsumption<'_>,
    bucket_entity_key: impl FnOnce(&mut EntityKey),
    initial_rate: impl FnOnce(f64) -> Option<u64>,
    delay_for_overage: impl FnOnce(u64, f64, u64) -> Time,
    maximum_delay: Time,
) -> Time {
    if request.amount == 0 {
        return <Time as TimeExt>::ZERO;
    }
    let Some((mut entity_key, rate)) = lookup::lookup_quota_with_key(
        request.image,
        request.principal,
        request.client_id,
        request.quota_key,
    ) else {
        return <Time as TimeExt>::ZERO;
    };
    if !rate.is_finite() || rate <= 0.0 {
        return <Time as TimeExt>::ZERO;
    }
    let Some(initial_rate) = initial_rate(rate) else {
        return <Time as TimeExt>::ZERO;
    };
    bucket_entity_key(&mut entity_key);
    let bucket = request
        .buckets
        .get_or_create(request.quota_key, &entity_key, initial_rate);
    let granted = bucket.try_consume(request.amount);
    if granted >= request.amount {
        return <Time as TimeExt>::ZERO;
    }
    delay_for_overage(request.amount - granted, rate, initial_rate).min(maximum_delay)
}

/// A quota delay as Kafka's `throttle_time_ms` wire field.
///
/// The conversion truncates toward zero and does not round to the nearest
/// value. It reports a 1.6 ms delay as `1`. A client reads `throttle_time_ms`
/// back and sleeps on it, so the byte on the wire must not change because the
/// code carries the delay as a [`Time`]. A delay beyond `i32::MAX`
/// milliseconds saturates.
#[must_use]
pub(crate) fn throttle_time_ms(delay: Time) -> i32 {
    i32::try_from(delay.millis_i64_trunc()).unwrap_or(i32::MAX)
}

/// A raw quota rate as the [`TokenBucket`](crate::throttle::TokenBucket)'s
/// [`ByteRate`].
///
/// The bucket is byte-dimensioned, but Kafka drives `request_percentage` and
/// `controller_mutation_rate` through the same token arithmetic, and those are
/// not byte throughputs. Their raw magnitudes therefore cross into the
/// bucket's dimension here, in one place, instead of at each call site.
pub(crate) fn bucket_rate(raw: u64) -> ByteRate {
    ByteRate::from_bytes_per_sec(i64::try_from(raw).unwrap_or(i64::MAX))
}

/// A configured rate as a whole token count, truncated toward zero.
///
/// Negative and non-finite rates are not throughputs, so they collapse to `0`,
/// the bucket's "no limit configured" sentinel. Anything past `u64::MAX`
/// saturates.
pub(crate) fn positive_f64_to_u64(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    value.trunc().to_u64().unwrap_or(u64::MAX)
}

/// A token count widened for the overage-over-rate division.
///
/// The conversion is exact below 2^53, which covers every quota magnitude
/// Kafka can express. `NumCast` never fails for `u64` into `f64`. The fallback
/// keeps the quota path total instead of panicking on a value that cannot
/// occur.
pub(crate) fn u64_to_f64(value: u64) -> f64 {
    NumCast::from(value).unwrap_or(f64::INFINITY)
}

#[cfg(test)]
mod test_support {
    use crabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};

    pub(super) fn image_with_quota(
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) -> MetadataImage {
        image_with_quotas(vec![quota_record(entity, key, value)])
    }

    pub(super) fn image_with_quotas(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for record in records {
            image.apply(&MetadataRecord::V1ClientQuota(record));
        }
        image
    }

    pub(super) fn quota_record(
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) -> ClientQuotaRecord {
        ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|(entity_type, entity_name)| QuotaEntity {
                    entity_type: entity_type.into(),
                    entity_name: entity_name.map(Into::into),
                })
                .collect(),
            config_key: key.into(),
            config_value: Some(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use assert2::{assert, check};
    use crabka_units::secs;

    use super::{test_support::image_with_quota, *};

    /// `throttle_time_ms` truncates: a sub-millisecond delay reports `0`, and
    /// a 1.6 ms delay reports `1`, not `2`.
    #[test]
    fn throttle_time_ms_truncates_toward_zero() {
        let cases = [
            (crabka_units::micros(0), 0),
            (crabka_units::micros(400), 0),
            (crabka_units::micros(999), 0),
            (crabka_units::millis(1), 1),
            (crabka_units::micros(1_600), 1),
            (crabka_units::micros(1_999), 1),
            (secs(1), 1_000),
        ];
        for (delay, want) in cases {
            check!(
                throttle_time_ms(delay) == want,
                "{delay:?} should report {want}ms"
            );
        }
    }

    #[test]
    fn throttle_time_ms_saturates_past_i32_milliseconds() {
        assert!(throttle_time_ms(crabka_units::days(36_500)) == i32::MAX);
    }

    #[test]
    fn quota_rate_conversion_is_checked_and_saturating() {
        assert!(positive_f64_to_u64(-1.0) == 0);
        assert!(positive_f64_to_u64(f64::NAN) == 0);
        assert!(positive_f64_to_u64(10.9) == 10);
        assert!(positive_f64_to_u64(f64::MAX) == u64::MAX);
        assert!(u64_to_f64(u64::MAX).is_finite());
    }

    /// The producer path once carried its own copy of this widening. The two
    /// agreed on every input, and this test pins that they still would. It
    /// compares bit patterns instead of using `==`, so the comparison is
    /// exact.
    #[test]
    fn widening_agrees_with_the_former_producer_copy() {
        for value in [0_u64, 1, 1024, 1 << 52, (1_u64 << 53) - 1, u64::MAX] {
            let former: f64 = value.to_string().parse().unwrap_or(f64::INFINITY);
            check!(u64_to_f64(value).to_bits() == former.to_bits());
        }
    }

    /// Truncating a configured rate agrees with the producer path's former
    /// floor-and-parse, and also on the sub-one rates that it rejects.
    #[test]
    fn rate_truncation_agrees_with_the_former_producer_copy() {
        for rate in [1.0_f64, 1.9, 1024.0, 9.007_199_254_740_99e15, f64::MAX] {
            let former: Option<u64> = rate.floor().to_string().parse().ok();
            check!(rate.floor().to_u64() == former);
        }
    }

    #[test]
    fn consume_configured_quota_returns_zero_without_mutating_bucket_for_zero_amount() {
        let image = image_with_quota(vec![("user", Some("alice"))], "request_percentage", 100.0);
        let buckets = QuotaBuckets::new();
        let bucket_entity_key_called = Arc::new(AtomicBool::new(false));
        let initial_rate_called = Arc::new(AtomicBool::new(false));
        let delay_for_overage_called = Arc::new(AtomicBool::new(false));

        let delay = consume_configured_quota(
            QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: "",
                quota_key: "request_percentage",
                amount: 0,
            },
            {
                let called = Arc::clone(&bucket_entity_key_called);
                move |_| called.store(true, Ordering::Relaxed)
            },
            {
                let called = Arc::clone(&initial_rate_called);
                move |_| {
                    called.store(true, Ordering::Relaxed);
                    Some(100)
                }
            },
            {
                let called = Arc::clone(&delay_for_overage_called);
                move |_, _, _| {
                    called.store(true, Ordering::Relaxed);
                    secs(1)
                }
            },
            secs(1),
        );

        check!(delay == <Time as TimeExt>::ZERO);
        check!(buckets.is_empty());
        check!(!bucket_entity_key_called.load(Ordering::Relaxed));
        check!(!initial_rate_called.load(Ordering::Relaxed));
        assert!(!delay_for_overage_called.load(Ordering::Relaxed));
    }

    #[test]
    fn consume_configured_quota_ignores_non_positive_rates() {
        for rate in [-1.0, 0.0] {
            let image = image_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", rate);
            let buckets = QuotaBuckets::new();
            let initial_rate_called = Arc::new(AtomicBool::new(false));

            let delay = consume_configured_quota(
                QuotaConsumption {
                    image: &image,
                    buckets: &buckets,
                    principal: "alice",
                    client_id: "",
                    quota_key: "producer_byte_rate",
                    amount: 1,
                },
                |_| {},
                {
                    let called = Arc::clone(&initial_rate_called);
                    move |_| {
                        called.store(true, Ordering::Relaxed);
                        Some(1)
                    }
                },
                |_, _, _| secs(1),
                secs(1),
            );

            check!(delay == <Time as TimeExt>::ZERO);
            check!(buckets.is_empty());
            assert!(!initial_rate_called.load(Ordering::Relaxed));
        }
    }

    #[test]
    fn consume_configured_quota_skips_unrepresentable_initial_rate() {
        let image = image_with_quota(
            vec![("user", Some("alice"))],
            "controller_mutation_rate",
            0.5,
        );
        let buckets = QuotaBuckets::new();

        let delay = consume_configured_quota(
            QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: "",
                quota_key: "controller_mutation_rate",
                amount: 1,
            },
            |_| {},
            |_| None,
            |_, _, _| secs(1),
            secs(1),
        );

        check!(delay == <Time as TimeExt>::ZERO);
        assert!(buckets.is_empty());
    }

    #[test]
    fn consume_configured_quota_caps_overage_delay() {
        let image = image_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 1.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_configured_quota(
            QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: "",
                quota_key: "producer_byte_rate",
                amount: 10,
            },
            |entity_key| entity_key.push(("qos-tier".into(), Some("bulk".into()))),
            |_| Some(1),
            |overage, rate, initial_rate| {
                check!(overage == 9);
                check!((rate - 1.0).abs() < f64::EPSILON);
                check!(initial_rate == 1);
                secs(10)
            },
            secs(1),
        );

        check!(delay == secs(1));
        assert!(buckets.len() == 1);
    }
}
