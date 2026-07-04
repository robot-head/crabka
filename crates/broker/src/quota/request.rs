//! KIP-124 `request_percentage` helper. The request quota throttles on the
//! server-side time spent handling a request (a percentage of one
//! request-handler thread). Called from the per-connection dispatch loop for
//! most APIs, and inline from the `Produce`/`Fetch` handlers so the request
//! throttle can be combined (`max`) with the data (byte-rate) throttle into a
//! single `throttle_time_ms` and a single channel mute (KIP-219).

use std::time::Duration;

use crabka_metadata::MetadataImage;

use super::{buckets::QuotaBuckets, lookup::lookup_quota_with_key};

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
    if elapsed_micros == 0 {
        return Duration::ZERO;
    }
    let Some((entity_key, rate_pct)) =
        lookup_quota_with_key(image, principal, client_id, "request_percentage")
    else {
        return Duration::ZERO;
    };
    if rate_pct <= 0.0 {
        return Duration::ZERO;
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let rate_micros_per_sec = (rate_pct * 10_000.0) as u64;
    if rate_micros_per_sec == 0 {
        return Duration::ZERO;
    }
    let bucket = buckets.get_or_create("request_percentage", &entity_key, rate_micros_per_sec);
    let granted = bucket.try_consume(elapsed_micros);
    if granted >= elapsed_micros {
        return Duration::ZERO;
    }
    let overage_micros = elapsed_micros - granted;
    let delay_micros = overage_micros.saturating_mul(1_000_000) / rate_micros_per_sec;
    Duration::from_micros(delay_micros).min(Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};

    use super::*;

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|(t, n)| QuotaEntity {
                    entity_type: t.into(),
                    entity_name: n.map(Into::into),
                })
                .collect(),
            config_key: "request_percentage".into(),
            config_value: Some(rate),
        }));
        img
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
}
