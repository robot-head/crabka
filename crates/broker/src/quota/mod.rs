//! KIP-13 + KIP-124 + KIP-257 client quotas.

use std::time::Duration;

use crabka_metadata::{EntityKey, MetadataImage};

mod buckets;
mod controller_mutation;
mod lookup;
mod producer;
mod request;

pub use buckets::QuotaBuckets;
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
    delay_for_overage: impl FnOnce(u64, f64, u64) -> Duration,
) -> Duration {
    if request.amount == 0 {
        return Duration::ZERO;
    }
    let Some((mut entity_key, rate)) = lookup::lookup_quota_with_key(
        request.image,
        request.principal,
        request.client_id,
        request.quota_key,
    ) else {
        return Duration::ZERO;
    };
    if rate <= 0.0 {
        return Duration::ZERO;
    }
    let Some(initial_rate) = initial_rate(rate) else {
        return Duration::ZERO;
    };
    bucket_entity_key(&mut entity_key);
    let bucket = request
        .buckets
        .get_or_create(request.quota_key, &entity_key, initial_rate);
    let granted = bucket.try_consume(request.amount);
    if granted >= request.amount {
        return Duration::ZERO;
    }
    delay_for_overage(request.amount - granted, rate, initial_rate).min(Duration::from_secs(1))
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

    use assert2::check;

    use super::{test_support::image_with_quota, *};

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
                    Duration::from_secs(1)
                }
            },
        );

        check!(delay == Duration::ZERO);
        check!(buckets.is_empty());
        check!(!bucket_entity_key_called.load(Ordering::Relaxed));
        check!(!initial_rate_called.load(Ordering::Relaxed));
        assert2::assert!(!delay_for_overage_called.load(Ordering::Relaxed));
    }

    #[test]
    fn consume_configured_quota_ignores_non_positive_rates() {
        for (case, rate) in [("negative", -1.0), ("zero", 0.0)] {
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
                |_, _, _| Duration::from_secs(1),
            );

            check!(delay == Duration::ZERO, "case {case}");
            check!(buckets.is_empty(), "case {case}");
            assert2::assert!(!initial_rate_called.load(Ordering::Relaxed));
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
            |_, _, _| Duration::from_secs(1),
        );

        check!(delay == Duration::ZERO);
        assert2::assert!(buckets.is_empty());
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
                Duration::from_secs(10)
            },
        );

        check!(delay == Duration::from_secs(1));
        assert2::assert!(buckets.len() == 1);
    }
}
