//! Idempotent topic-create for the rebalancer's state topic. Run once
//! at startup; existing topic is left alone.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, AdminError, CreateTopicOutcome, CreateTopicSpec};
use crabka_units::{
    Time,
    convert::{RatioExt as _, TimeExt as _},
};
use tracing::warn;

use crate::{config::RebalancerRuntimePolicy, state_topic::error::StateTopicError};

/// Kafka error code for "replication factor exceeds available brokers".
const INVALID_REPLICATION_FACTOR: i16 = 38;

#[async_trait::async_trait]
pub trait TopicAdminClient: Send {
    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout: Time,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError>;
}

#[async_trait::async_trait]
impl TopicAdminClient for AdminClient {
    // cargo-mutants: live admin-client adapter; FakeAdmin tests cover callers.
    #[cfg_attr(test, mutants::skip)]
    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout: Time,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        AdminClient::create_topics(self, specs, timeout).await
    }
}

/// Create the state topic if missing, with the compaction configs the
/// loader expects. `replication_factor` is the requested value; the
/// broker may downgrade it (or reject) based on the live broker count.
///
/// If the broker rejects the requested replication factor as invalid
/// (`INVALID_REPLICATION_FACTOR`, code 38 — returned when RF > broker count),
/// the call retries with RF=1 so single-broker development clusters work
/// without any extra configuration.
///
/// Idempotent: if the topic already exists with any config, this is a
/// no-op (the existing topic's configs are NOT updated; that's a
/// separate operator reconciliation path).
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn ensure_topic<A: TopicAdminClient + ?Sized>(
    admin: &mut A,
    name: &str,
    replication_factor: i16,
) -> Result<(), StateTopicError> {
    ensure_topic_with_policy(
        admin,
        name,
        replication_factor,
        &RebalancerRuntimePolicy::default(),
    )
    .await
}

/// Create the state topic using explicit runtime policy.
///
/// # Errors
/// Returns an error when topic creation fails.
pub async fn ensure_topic_with_policy<A: TopicAdminClient + ?Sized>(
    admin: &mut A,
    name: &str,
    replication_factor: i16,
    policy: &RebalancerRuntimePolicy,
) -> Result<(), StateTopicError> {
    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());
    configs.insert(
        "min.cleanable.dirty.ratio".to_string(),
        policy
            .state_topic_min_cleanable_dirty_ratio
            .as_f64()
            .to_string(),
    );
    configs.insert(
        "segment.ms".to_string(),
        policy.state_topic_segment_interval.millis_i64().to_string(),
    );

    let effective_rf = try_create_topic(
        admin,
        name,
        replication_factor,
        &configs,
        policy.state_topic_create_timeout,
    )
    .await?;
    if effective_rf != replication_factor {
        warn!(
            topic = %name,
            requested_rf = replication_factor,
            effective_rf,
            "replication factor downgraded; cluster has fewer brokers than requested"
        );
    }
    Ok(())
}

/// Inner helper: attempt to create with `rf`; if the broker returns
/// `INVALID_REPLICATION_FACTOR` and `rf > 1`, retry with `rf = 1`.
/// Returns the replication factor that succeeded.
async fn try_create_topic<A: TopicAdminClient + ?Sized>(
    admin: &mut A,
    name: &str,
    rf: i16,
    configs: &BTreeMap<String, String>,
    timeout: Time,
) -> Result<i16, StateTopicError> {
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions: 1,
        replicas: i32::from(rf),
        configs: configs.clone(),
    };
    let outcomes = admin.create_topics(&[spec], timeout).await?;
    for o in outcomes {
        // 36 = TOPIC_ALREADY_EXISTS  → idempotent, treat as success.
        // 38 = INVALID_REPLICATION_FACTOR → retry with rf=1 if rf > 1.
        // 0 / None → success.
        match o.error.as_ref().map(|e| e.code) {
            None | Some(0 | 36) => {}
            Some(INVALID_REPLICATION_FACTOR) if rf > 1 => {
                return Box::pin(try_create_topic(admin, name, 1, configs, timeout)).await;
            }
            Some(code) => return Err(StateTopicError::ProduceErrorCode { code }),
        }
    }
    Ok(rf)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;
    use crabka_client_admin::{AdminError, CreateTopicOutcome, KafkaError};
    use crabka_units::secs;

    use super::*;

    #[derive(Default)]
    struct FakeAdmin {
        outcomes: VecDeque<Result<Vec<CreateTopicOutcome>, AdminError>>,
        calls: Vec<(Vec<CreateTopicSpec>, Time)>,
    }

    #[async_trait::async_trait]
    impl TopicAdminClient for FakeAdmin {
        async fn create_topics(
            &mut self,
            specs: &[CreateTopicSpec],
            timeout: Time,
        ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
            self.calls.push((specs.to_vec(), timeout));
            self.outcomes.pop_front().expect("fake outcome")
        }
    }

    fn ok(name: &str) -> CreateTopicOutcome {
        CreateTopicOutcome {
            name: name.into(),
            topic_id: None,
            error: None,
        }
    }

    fn error(name: &str, code: i16) -> CreateTopicOutcome {
        CreateTopicOutcome {
            name: name.into(),
            topic_id: None,
            error: Some(KafkaError {
                code,
                name: "TEST_ERROR",
                message: None,
            }),
        }
    }

    #[tokio::test]
    async fn ensure_topic_submits_compacted_single_partition_topic_spec() {
        let mut admin = FakeAdmin {
            outcomes: VecDeque::from([Ok(vec![ok("__crabka_state")])]),
            ..Default::default()
        };

        ensure_topic(&mut admin, "__crabka_state", 3).await.unwrap();

        let expected_configs = BTreeMap::from([
            ("cleanup.policy".to_string(), "compact".to_string()),
            ("min.cleanable.dirty.ratio".to_string(), "0.01".to_string()),
            ("segment.ms".to_string(), "60000".to_string()),
        ]);
        assert2::assert!(
            admin.calls.first().map(|(specs, timeout)| {
                (
                    *timeout,
                    specs.first().map(|spec| {
                        (
                            spec.name.as_str(),
                            spec.partitions,
                            spec.replicas,
                            &spec.configs,
                        )
                    }),
                )
            }) == Some((secs(10), Some(("__crabka_state", 1, 3, &expected_configs))))
        );
    }

    #[tokio::test]
    async fn ensure_topic_applies_custom_runtime_policy() {
        let mut admin = FakeAdmin {
            outcomes: VecDeque::from([Ok(vec![ok("__crabka_state")])]),
            ..Default::default()
        };
        let policy = RebalancerRuntimePolicy {
            state_topic_create_timeout: crabka_units::millis(37),
            state_topic_min_cleanable_dirty_ratio: crabka_units::percent(2),
            state_topic_segment_interval: crabka_units::secs(90),
            ..Default::default()
        };

        ensure_topic_with_policy(&mut admin, "__crabka_state", 3, &policy)
            .await
            .unwrap();

        let (specs, timeout) = &admin.calls[0];
        assert2::assert!(*timeout == crabka_units::millis(37));
        assert2::assert!(specs[0].configs["min.cleanable.dirty.ratio"] == "0.02");
        assert2::assert!(specs[0].configs["segment.ms"] == "90000");
    }

    #[tokio::test]
    async fn ensure_topic_propagates_non_retryable_create_error() {
        let mut admin = FakeAdmin {
            outcomes: VecDeque::from([Ok(vec![error("__crabka_state", 42)])]),
            ..Default::default()
        };

        let err = ensure_topic(&mut admin, "__crabka_state", 3)
            .await
            .unwrap_err();

        assert2::assert!(matches!(
            err,
            StateTopicError::ProduceErrorCode { code: 42 }
        ));
    }

    #[tokio::test]
    async fn try_create_topic_returns_requested_replication_factor_for_existing_topic() {
        let mut admin = FakeAdmin {
            outcomes: VecDeque::from([Ok(vec![error("__crabka_state", 36)])]),
            ..Default::default()
        };
        let configs = BTreeMap::new();

        let effective = try_create_topic(&mut admin, "__crabka_state", 3, &configs, secs(10))
            .await
            .unwrap();

        assert2::assert!(effective == 3);
    }

    #[tokio::test]
    async fn invalid_requested_rf_retries_once_with_single_replica() {
        let mut admin = FakeAdmin {
            outcomes: VecDeque::from([
                Ok(vec![error("__crabka_state", INVALID_REPLICATION_FACTOR)]),
                Ok(vec![ok("__crabka_state")]),
            ]),
            ..Default::default()
        };
        let configs = BTreeMap::new();

        let effective = try_create_topic(&mut admin, "__crabka_state", 3, &configs, secs(10))
            .await
            .unwrap();

        check!(
            (
                effective,
                admin
                    .calls
                    .iter()
                    .filter_map(|(specs, _)| specs.first().map(|spec| spec.replicas))
                    .collect::<Vec<_>>(),
            ) == (1, vec![3, 1])
        );
    }

    #[tokio::test]
    async fn invalid_single_replica_rf_is_not_retried() {
        let mut admin = FakeAdmin {
            outcomes: VecDeque::from([
                Ok(vec![error("__crabka_state", INVALID_REPLICATION_FACTOR)]),
                Ok(vec![ok("__crabka_state")]),
            ]),
            ..Default::default()
        };
        let configs = BTreeMap::new();

        let err = try_create_topic(&mut admin, "__crabka_state", 1, &configs, secs(10))
            .await
            .unwrap_err();

        assert2::assert!(matches!(
            err,
            StateTopicError::ProduceErrorCode { code: 38 }
        ));
        assert2::assert!(admin.calls.len() == 1);
    }
}
