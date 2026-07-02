//! KIP-599 `controller_mutation_rate` helper. Called from `CreateTopics`,
//! `CreatePartitions`, `DeleteTopics` handlers after response assembly.

use std::time::Duration;

use crabka_metadata::MetadataImage;

use super::{buckets::QuotaBuckets, lookup::lookup_quota_with_key};

/// Consume `mutations` from the `controller_mutation_rate` bucket for
/// `(principal, client_id)`. Returns the throttle delay to apply
/// before sending the response. `Duration::ZERO` if no quota
/// configured, no overage, or `mutations == 0`. Capped at 1 second.
#[must_use]
pub fn consume_controller_mutation_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: &str,
    mutations: u64,
) -> Duration {
    if mutations == 0 {
        return Duration::ZERO;
    }
    let Some((entity_key, rate)) =
        lookup_quota_with_key(image, principal, client_id, "controller_mutation_rate")
    else {
        return Duration::ZERO;
    };
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let initial_rate = rate as u64;
    let bucket = buckets.get_or_create("controller_mutation_rate", &entity_key, initial_rate);
    let granted = bucket.try_consume(mutations);
    if granted >= mutations {
        return Duration::ZERO;
    }
    let overage = mutations - granted;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let delay_micros = ((overage as f64 / rate) * 1_000_000.0) as u64;
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
            config_key: "controller_mutation_rate".into(),
            config_value: Some(rate),
        }));
        img
    }

    #[test]
    fn zero_mutations_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 0);
        assert!(delay == Duration::ZERO);
    }

    #[test]
    fn under_rate_returns_zero_delay() {
        // rate=10/sec, burst capacity=10 (one second of capacity).
        // 5 mutations consumed → bucket has 5 left → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 5);
        assert!(delay == Duration::ZERO);
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=1/sec, burst=1; 100 mutations → overage 99 → delay 99s
        // → capped at 1s.
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", "", 100);
        assert!(delay == Duration::from_secs(1));
    }
}
