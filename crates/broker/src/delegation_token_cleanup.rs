//! KIP-48: background sweep that tombstones expired
//! delegation tokens.
//!
//! Spawned from [`Broker::start`] only when
//! `delegation_token_secret_key` is set — without a master key the four
//! delegation-token RPCs return `DELEGATION_TOKEN_AUTH_DISABLED` and
//! the image never has any tokens to expire.
//!
//! Every broker runs the sweep. Raft serializes the resulting
//! tombstones, so a duplicate tombstone for the same `token_id` is a
//! no-op on the apply path (the entry is already gone from
//! `delegation_tokens`). This matches Kafka's "every broker sweeps,
//! idempotent" pattern from KIP-48 §6.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use crabka_metadata::{DeleteDelegationTokenRecord, MetadataImage, MetadataRecord};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Minimal controller surface used by the sweep. The real
/// [`crabka_raft::ControllerHandle`] is adapted in [`crate::broker`];
/// tests inject a mock.
#[async_trait]
pub(crate) trait DelegationTokenController: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

/// Spawned task entry point. Returns when `shutdown` is cancelled.
pub(crate) async fn run(
    controller: Arc<dyn DelegationTokenController>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => sweep(&*controller).await,
            () = shutdown.cancelled() => {
                info!("delegation-token cleanup shutting down");
                return;
            }
        }
    }
}

pub(crate) async fn sweep(controller: &dyn DelegationTokenController) {
    let now = crate::time_util::now_ms();
    let expired: Vec<String> = controller
        .current_image()
        .all_delegation_tokens()
        .filter(|t| t.expiry_timestamp_ms <= now)
        .map(|t| t.token_id.clone())
        .collect();
    if expired.is_empty() {
        return;
    }
    let records: Vec<MetadataRecord> = expired
        .iter()
        .map(|id| {
            MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
                token_id: id.clone(),
            })
        })
        .collect();
    let count = expired.len();
    if let Err(e) = controller.submit_change(records).await {
        warn!(
            error = %e,
            count,
            "failed to submit delegation-token tombstone batch"
        );
    } else {
        for id in &expired {
            debug!(token_id = %id, "delegation token expired and tombstoned");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use std::sync::Mutex;

    use crabka_metadata::{DelegationTokenRecord, MetadataImage, MetadataRecord};
    use crabka_security::KafkaPrincipal;
    use uuid::Uuid;

    struct MockController {
        image: Mutex<Arc<MetadataImage>>,
        submitted: Mutex<Vec<MetadataRecord>>,
    }

    #[async_trait]
    impl DelegationTokenController for MockController {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.lock().unwrap().clone()
        }
        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
            // Apply the records to the local image so the test observes
            // the post-tombstone state via `current_image()`.
            let mut img: MetadataImage = (**self.image.lock().unwrap()).clone();
            for r in &records {
                img.apply(r);
            }
            *self.image.lock().unwrap() = Arc::new(img);
            self.submitted.lock().unwrap().extend(records);
            Ok(())
        }
    }

    fn principal(t: &str, n: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: t.into(),
            name: n.into(),
        }
    }

    fn dt_record(token_id: &str, expiry_ms: i64) -> MetadataRecord {
        MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: token_id.into(),
            owner: principal("User", "alice"),
            hmac: vec![0xAB; 32],
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: expiry_ms,
            max_timestamp_ms: i64::MAX,
            renewers: vec![],
        })
    }

    #[tokio::test]
    async fn sweep_emits_tombstones_for_expired_tokens_only() {
        let mut img = MetadataImage::new(Uuid::nil());
        // `now_ms()` returns wall-clock; pick expiries far in the past
        // (expired) and far in the future (fresh) so the test is
        // independent of when it runs.
        img.apply(&dt_record("expired-1", 1_000));
        img.apply(&dt_record("expired-2", 2_000));
        img.apply(&dt_record("fresh", i64::MAX));

        let mock = Arc::new(MockController {
            image: Mutex::new(Arc::new(img)),
            submitted: Mutex::new(Vec::new()),
        });

        sweep(&*mock).await;

        // Two tombstones submitted, one per expired token (order in the
        // batch is HashMap-iteration order, so check by id-set).
        let submitted = mock.submitted.lock().unwrap();
        assert!(submitted.len() == 2);
        let mut tombstone_ids: Vec<String> = submitted
            .iter()
            .map(|r| match r {
                MetadataRecord::V1DeleteDelegationToken(d) => d.token_id.clone(),
                other => panic!("unexpected record type: {other:?}"),
            })
            .collect();
        tombstone_ids.sort();
        assert!(tombstone_ids == vec!["expired-1".to_string(), "expired-2".to_string()]);
        drop(submitted);

        // Post-sweep image keeps only the fresh token.
        let after = mock.current_image();
        assert!(after.all_delegation_tokens().count() == 1);
        let cases = [("fresh", true), ("expired-1", false), ("expired-2", false)];
        for (token_id, expect_present) in cases {
            assert!(
                after.delegation_token_by_id(token_id).is_some() == expect_present,
                "case: {token_id}"
            );
        }
    }

    #[tokio::test]
    async fn sweep_with_no_expired_tokens_is_a_no_op() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&dt_record("fresh", i64::MAX));

        let mock = Arc::new(MockController {
            image: Mutex::new(Arc::new(img)),
            submitted: Mutex::new(Vec::new()),
        });

        sweep(&*mock).await;

        assert!(mock.submitted.lock().unwrap().is_empty());
        assert!(mock.current_image().all_delegation_tokens().count() == 1);
    }

    #[tokio::test]
    async fn run_sweeps_expired_tokens_and_waits_for_shutdown() {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&dt_record("expired", 1_000));

        let mock = Arc::new(MockController {
            image: Mutex::new(Arc::new(img)),
            submitted: Mutex::new(Vec::new()),
        });
        let shutdown = CancellationToken::new();
        let mut task = tokio::spawn(run(mock.clone(), Duration::from_hours(1), shutdown.clone()));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !mock.submitted.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cleanup run loop should sweep on its first tick");

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut task)
                .await
                .is_err(),
            "cleanup run loop exited before shutdown"
        );

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cleanup task should observe shutdown")
            .expect("cleanup task should not panic");
    }
}
