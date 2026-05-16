//! Background task that subscribes to `MetadataImage` changes and
//! pushes new quota rates to the `QuotaBuckets` cache.
//!
//! Mirrors slice 15b's `throttle::refresh` shape.

use std::sync::Arc;

use async_trait::async_trait;
use crabka_metadata::MetadataImage;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::buckets::QuotaBuckets;

#[async_trait]
pub trait ImageWatcher: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
}

pub async fn run(
    controller: Arc<dyn ImageWatcher>,
    buckets: Arc<QuotaBuckets>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    refresh_buckets(&controller.current_image(), &buckets);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                info!("quota refresh task shutting down");
                return;
            }
            r = watcher.changed() => {
                if r.is_err() {
                    info!("quota refresh: image channel closed");
                    return;
                }
            }
        }
        refresh_buckets(&controller.current_image(), &buckets);
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // quota rates are non-negative
fn refresh_buckets(image: &MetadataImage, buckets: &QuotaBuckets) {
    for ((quota_key, entity_key), bucket) in buckets.iter() {
        let new_rate: u64 = image
            .client_quotas()
            .get(&entity_key)
            .and_then(|m| m.get(&quota_key))
            .copied()
            .map_or(0, |v| v.max(0.0) as u64);
        if bucket.rate() != new_rate {
            debug!(
                quota_key,
                ?entity_key,
                new_rate,
                "quota refresh: rate update"
            );
            bucket.set_rate(new_rate);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity};

    fn img_with_quota(
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|(t, n)| QuotaEntity {
                    entity_type: t.into(),
                    entity_name: n.map(Into::into),
                })
                .collect(),
            config_key: key.into(),
            config_value: Some(value),
        }));
        Arc::new(img)
    }

    #[test]
    fn refresh_updates_existing_bucket_rate() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, 0);
        assert_eq!(b.rate(), 0);

        let img = img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 2048.0);
        refresh_buckets(&img, &buckets);
        assert_eq!(b.rate(), 2048);
    }

    #[test]
    fn refresh_zeroes_bucket_when_quota_removed_from_image() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, 1024);
        assert_eq!(b.rate(), 1024);

        let empty = Arc::new(MetadataImage::new(uuid::Uuid::nil()));
        refresh_buckets(&empty, &buckets);
        assert_eq!(b.rate(), 0);
    }
}
