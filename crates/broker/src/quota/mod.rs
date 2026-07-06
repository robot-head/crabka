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
