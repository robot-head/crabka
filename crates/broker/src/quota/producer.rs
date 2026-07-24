//! Producer-byte quota enforcement with Crabka QoS-tier bucket partitioning.

use std::time::Duration;

use crabka_metadata::MetadataImage;

use super::{QuotaConsumption, buckets::QuotaBuckets, consume_configured_quota};

#[must_use]
pub fn consume_producer_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    qos_tier: &str,
    bytes: u64,
    maximum_delay: Duration,
) -> Duration {
    consume_configured_quota(
        QuotaConsumption {
            image,
            buckets,
            principal,
            client_id,
            quota_key: "producer_byte_rate",
            amount: bytes,
        },
        |entity_key| entity_key.push(("qos-tier".into(), Some(qos_tier.into()))),
        quota_rate_to_bucket_rate,
        |overage, rate, _| {
            let overage = u64_to_f64(overage);
            Duration::from_secs_f64(overage / rate)
        },
        maximum_delay,
    )
}

fn quota_rate_to_bucket_rate(rate: f64) -> Option<u64> {
    if !rate.is_finite() || rate < 1.0 {
        return None;
    }

    rate.floor().to_string().parse().ok()
}

fn u64_to_f64(value: u64) -> f64 {
    value.to_string().parse().unwrap_or(f64::INFINITY)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
    use crabka_metadata::MetadataImage;

    use super::consume_producer_quota;
    use crate::quota::{QuotaBuckets, test_support::image_with_quota as quota_image};

    fn img_with_quota(entity: Vec<(&str, Option<&str>)>, rate: f64) -> MetadataImage {
        quota_image(entity, "producer_byte_rate", rate)
    }

    #[test]
    fn producer_quota_buckets_are_separate_by_qos_tier() {
        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            128.0,
        );
        let buckets = QuotaBuckets::new();

        let gold = consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "app",
            "gold",
            1024,
            Duration::from_secs(1),
        );
        let bulk = consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "app",
            "bulk",
            64,
            Duration::from_secs(1),
        );

        check!(gold > Duration::ZERO);
        check!(bulk == Duration::ZERO);
        check!(buckets.len() == 2);
    }

    #[test]
    fn producer_quota_uses_client_id_entity_precedence_per_tier() {
        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            128.0,
        );
        let buckets = QuotaBuckets::new();

        let matching = consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "app",
            "default",
            4096,
            Duration::from_secs(1),
        );
        let other_client = consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "other",
            "default",
            4096,
            Duration::from_secs(1),
        );

        assert!(matching > Duration::ZERO);
        assert!(other_client == Duration::ZERO);
    }

    #[test]
    fn producer_quota_delay_reflects_exact_overage_at_configured_rate() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1_000.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "app",
            "default",
            1_250,
            Duration::from_secs(1),
        );

        assert!(delay == Duration::from_millis(250));
    }

    #[test]
    fn producer_quota_uses_configured_maximum_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "app",
            "default",
            100,
            Duration::from_millis(25),
        );

        assert!(delay == Duration::from_millis(25));
    }
}
