//! Producer-byte quota enforcement with Crabka QoS-tier bucket partitioning.

use std::time::Duration;

use crabka_metadata::MetadataImage;

use super::buckets::QuotaBuckets;
use super::lookup::lookup_quota_with_key;

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
#[must_use]
pub fn consume_producer_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    qos_tier: &str,
    bytes: u64,
) -> Duration {
    if bytes == 0 {
        return Duration::ZERO;
    }
    let Some((mut entity_key, rate)) =
        lookup_quota_with_key(image, principal, client_id, "producer_byte_rate")
    else {
        return Duration::ZERO;
    };
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    entity_key.push(("qos-tier".into(), Some(qos_tier.into())));
    let bucket = buckets.get_or_create("producer_byte_rate", &entity_key, rate as u64);
    let granted = bucket.try_consume(bytes);
    if granted >= bytes {
        return Duration::ZERO;
    }
    let overage = bytes - granted;
    let delay_secs = overage as f64 / rate;
    Duration::from_micros((delay_secs * 1_000_000.0) as u64).min(Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use crabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};

    use crate::quota::QuotaBuckets;

    use super::consume_producer_quota;

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
            config_key: "producer_byte_rate".into(),
            config_value: Some(rate),
        }));
        img
    }

    #[test]
    fn producer_quota_buckets_are_separate_by_qos_tier() {
        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            128.0,
        );
        let buckets = QuotaBuckets::new();

        let gold = consume_producer_quota(&img, &buckets, "alice", "app", "gold", 1024);
        let bulk = consume_producer_quota(&img, &buckets, "alice", "app", "bulk", 64);

        use assert2::check;
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

        let matching = consume_producer_quota(&img, &buckets, "alice", "app", "default", 4096);
        let other_client =
            consume_producer_quota(&img, &buckets, "alice", "other", "default", 4096);

        assert!(matching > Duration::ZERO);
        assert!(other_client == Duration::ZERO);
    }

    #[test]
    fn producer_quota_delay_reflects_exact_overage_at_configured_rate() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1_000.0);
        let buckets = QuotaBuckets::new();

        let delay = consume_producer_quota(&img, &buckets, "alice", "app", "default", 1_250);

        assert!(delay == Duration::from_millis(250));
    }
}
