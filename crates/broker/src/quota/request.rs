//! KIP-124 `request_percentage` helper.
//!
//! The request quota throttles on the server-side time a request takes, as a
//! percentage of one request-handler thread. The per-connection dispatch loop
//! calls this helper for most APIs. The `Produce` and `Fetch` handlers call it
//! inline, so that the broker can combine the request throttle with the
//! byte-rate data throttle, with `max`, into one `throttle_time_ms` and one
//! channel mute (KIP-219).

use crabka_metadata::MetadataImage;
use crabka_units::{Time, convert::TimeExt};

use super::{
    QuotaConsumption, buckets::QuotaBuckets, consume_configured_quota, positive_f64_to_u64,
};

/// Consumes `elapsed_micros` of request-handler time from the
/// `request_percentage` bucket for `(principal, client_id)`.
///
/// It returns the throttle delay to apply before the broker sends the
/// response. The delay is a zero extent when no quota is configured, when the
/// rate is not positive, or when there was no overage. `maximum_delay` caps
/// the returned delay.
///
/// `request_percentage` is a percentage of one thread-second, so `100.0` gives
/// a budget of 1 000 000 µs per second. The bucket therefore meters in
/// microseconds, the same unit as `elapsed_micros`.
#[must_use]
pub fn consume_request_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    elapsed_micros: u64,
    maximum_delay: Time,
) -> Time {
    consume_configured_quota(
        QuotaConsumption {
            image,
            buckets,
            principal,
            client_id,
            quota_key: "request_percentage",
            amount: elapsed_micros,
        },
        |_| {},
        |rate_pct| {
            let rate_micros_per_sec = positive_f64_to_u64(rate_pct * 10_000.0);
            (rate_micros_per_sec != 0).then_some(rate_micros_per_sec)
        },
        |overage_micros, _, rate_micros_per_sec| {
            Time::from_micros(
                i64::try_from(overage_micros.saturating_mul(1_000_000) / rate_micros_per_sec)
                    .unwrap_or(i64::MAX),
            )
        },
        maximum_delay,
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::{millis, secs};

    use super::*;
    use crate::quota::test_support::image_with_quota as quota_image;

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        quota_image(entity, "request_percentage", rate)
    }

    #[test]
    fn zero_elapsed_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::new();
        assert!(
            consume_request_quota(&img, &buckets, "alice", "", 0, secs(1))
                == <Time as TimeExt>::ZERO
        );
    }

    #[test]
    fn no_quota_returns_zero_delay() {
        let img = MetadataImage::new(uuid::Uuid::nil());
        let buckets = QuotaBuckets::new();
        assert!(
            consume_request_quota(&img, &buckets, "alice", "", 5_000, secs(1))
                == <Time as TimeExt>::ZERO
        );
    }

    #[test]
    fn under_budget_returns_zero_delay() {
        // rate=100% ⇒ 1_000_000 µs/sec budget; 5_000 µs is well under one
        // second of capacity → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::new();
        assert!(
            consume_request_quota(&img, &buckets, "alice", "", 5_000, secs(1))
                == <Time as TimeExt>::ZERO
        );
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=0.001% ⇒ 10 µs/sec budget; 1_000_000 µs of work is a colossal
        // overage → multi-day delay → capped at 1s.
        let img = img_with_quota(vec![("user", Some("alice"))], 0.001);
        let buckets = QuotaBuckets::new();
        let delay = consume_request_quota(&img, &buckets, "alice", "", 1_000_000, secs(1));
        assert!(delay == secs(1));
    }

    #[test]
    fn overage_uses_configured_maximum_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 0.001);
        let buckets = QuotaBuckets::new();

        let delay = consume_request_quota(&img, &buckets, "alice", "", 1_000_000, millis(25));

        assert!(delay == millis(25));
    }

    #[test]
    fn overage_returns_scaled_uncapped_delay() {
        // rate=100% gives a 1_000_000 us/sec budget. The bucket starts with
        // one second of burst, so 1_500_000 us leaves a 500_000 us overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_request_quota(&img, &buckets, "alice", "", 1_500_000, secs(1));

        assert!(delay == millis(500));
    }
}
