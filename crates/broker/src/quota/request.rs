//! KIP-124 `request_percentage` helper. The request quota throttles on the
//! server-side time spent handling a request (a percentage of one
//! request-handler thread). Called from the per-connection dispatch loop for
//! most APIs, and inline from the `Produce`/`Fetch` handlers so the request
//! throttle can be combined (`max`) with the data (byte-rate) throttle into a
//! single `throttle_time_ms` and a single channel mute (KIP-219).

use std::time::Duration;

use crabka_metadata::MetadataImage;

use super::{QuotaConsumption, buckets::QuotaBuckets, consume_configured_quota};

/// Consume `elapsed_micros` of request-handler time from the
/// `request_percentage` bucket for `(principal, client_id)`. Returns the
/// throttle delay to apply before sending the response. `Duration::ZERO`
/// if no quota is configured, the rate is non-positive, or there was no
/// overage. Capped at 1 second.
///
/// `request_percentage` is expressed as a percentage of one thread-second:
/// `100.0` ⇒ a 1 000 000 µs/sec budget. The bucket therefore meters in
/// microseconds, the same unit as `elapsed_micros`.
#[must_use]
pub fn consume_request_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    elapsed_micros: u64,
) -> Duration {
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
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let rate_micros_per_sec = (rate_pct * 10_000.0) as u64;
            (rate_micros_per_sec != 0).then_some(rate_micros_per_sec)
        },
        |overage_micros, _, rate_micros_per_sec| {
            Duration::from_micros(overage_micros.saturating_mul(1_000_000) / rate_micros_per_sec)
        },
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::quota::test_support::image_with_quota as quota_image;

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        quota_image(entity, "request_percentage", rate)
    }

    #[test]
    fn zero_elapsed_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::new();
        assert!(consume_request_quota(&img, &buckets, "alice", "", 0) == Duration::ZERO);
    }

    #[test]
    fn no_quota_returns_zero_delay() {
        let img = MetadataImage::new(uuid::Uuid::nil());
        let buckets = QuotaBuckets::new();
        assert!(consume_request_quota(&img, &buckets, "alice", "", 5_000) == Duration::ZERO);
    }

    #[test]
    fn under_budget_returns_zero_delay() {
        // rate=100% ⇒ 1_000_000 µs/sec budget; 5_000 µs is well under one
        // second of capacity → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::new();
        assert!(consume_request_quota(&img, &buckets, "alice", "", 5_000) == Duration::ZERO);
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=0.001% ⇒ 10 µs/sec budget; 1_000_000 µs of work is a colossal
        // overage → multi-day delay → capped at 1s.
        let img = img_with_quota(vec![("user", Some("alice"))], 0.001);
        let buckets = QuotaBuckets::new();
        let delay = consume_request_quota(&img, &buckets, "alice", "", 1_000_000);
        assert!(delay == Duration::from_secs(1));
    }

    #[test]
    fn overage_returns_scaled_uncapped_delay() {
        // rate=100% gives a 1_000_000 us/sec budget. The bucket starts with
        // one second of burst, so 1_500_000 us leaves a 500_000 us overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_request_quota(&img, &buckets, "alice", "", 1_500_000);

        assert!(delay == Duration::from_millis(500));
    }
}
