//! Background task that subscribes to `MetadataImage` changes and
//! pushes new quota rates to the `QuotaBuckets` cache.
//!
//! Mirrors the `throttle::refresh` shape.

use std::sync::Arc;

use async_trait::async_trait;
use crabka_metadata::{EntityKey, MetadataImage};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::{buckets::QuotaBuckets, positive_f64_to_u64};

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

fn refresh_buckets(image: &MetadataImage, buckets: &QuotaBuckets) {
    for ((quota_key, entity_key), bucket) in buckets.iter() {
        let persisted_key = persisted_quota_entity_key(&entity_key);
        let new_rate: u64 = image
            .client_quotas()
            .get(&persisted_key)
            .and_then(|m| m.get(&quota_key))
            .copied()
            .map_or(0, positive_f64_to_u64);
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

fn persisted_quota_entity_key(entity_key: &EntityKey) -> EntityKey {
    entity_key
        .iter()
        .filter(|(entity_type, _)| entity_type != "qos-tier")
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_metadata::EntityKey;

    use super::*;
    use crate::quota::test_support::image_with_quota as quota_image;

    fn img_with_quota(
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) -> Arc<MetadataImage> {
        Arc::new(quota_image(entity, key, value))
    }

    #[test]
    fn refresh_updates_existing_bucket_rate() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, 0);
        assert!(b.rate() == 0);

        let img = img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 2048.0);
        refresh_buckets(&img, &buckets);
        assert!(b.rate() == 2048);
    }

    #[test]
    fn refresh_zeroes_bucket_when_quota_removed_from_image() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, 1024);
        assert!(b.rate() == 1024);

        let empty = Arc::new(MetadataImage::new(uuid::Uuid::nil()));
        refresh_buckets(&empty, &buckets);
        assert!(b.rate() == 0);
    }

    #[test]
    fn refresh_updates_qos_tier_bucket_from_base_quota_entity() {
        let buckets = Arc::new(QuotaBuckets::new());
        let tiered_key: EntityKey = vec![
            ("client-id".into(), Some("app".into())),
            ("user".into(), Some("alice".into())),
            ("qos-tier".into(), Some("gold".into())),
        ];
        let b = buckets.get_or_create("producer_byte_rate", &tiered_key, 128);
        assert!(b.rate() == 128);

        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            "producer_byte_rate",
            2048.0,
        );
        refresh_buckets(&img, &buckets);
        assert!(b.rate() == 2048);
    }
}
