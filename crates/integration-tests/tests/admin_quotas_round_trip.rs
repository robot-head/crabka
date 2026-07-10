//! Integration test: drive `DescribeClientQuotas` (`api_key` 48)
//! and `AlterClientQuotas` (`api_key` 49) against a live in-process
//! broker and assert end-to-end wire behavior.
//!
//! The pipeline exercised matches the operator's `KafkaUser` reconcile
//! quota path: read the current per-user state, diff, write the
//! resulting `(set, remove)` ops, read back.

use assert2::{assert, check};
use crabka_client_admin::{AdminClient, QuotaOp, diff_user_quotas};

#[path = "../../broker/tests/support/mod.rs"]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_quotas_set_change_remove() {
    support::init_tracing();
    let proc = support::start().await;
    let bootstrap = proc.broker.listen_addr().to_string();

    let mut admin = AdminClient::connect(&[bootstrap]).await.unwrap();

    let user = "alice";

    // 1. No quotas at startup.
    let initial = admin.describe_user_quotas(user).await.unwrap();
    assert!(
        initial.is_empty(),
        "broker should report no quotas: {initial:?}"
    );

    // 2. Set producer + request-percentage. `validate_only=false`
    // commits to the metadata log.
    let outcome = admin
        .alter_user_quotas(
            user,
            &[
                QuotaOp::Set {
                    key: "producer_byte_rate".into(),
                    value: 1_048_576.0,
                },
                QuotaOp::Set {
                    key: "request_percentage".into(),
                    value: 25.0,
                },
            ],
            false,
        )
        .await
        .unwrap();
    assert!(outcome.is_none(), "alter must succeed: {outcome:?}");

    let after_set = admin.describe_user_quotas(user).await.unwrap();
    assert_eq!(after_set.len(), 2);
    check!((after_set["producer_byte_rate"] - 1_048_576.0).abs() < f64::EPSILON);
    check!((after_set["request_percentage"] - 25.0).abs() < f64::EPSILON);

    // 3. `diff_user_quotas` with the same desired-state map → no ops.
    let same = after_set.clone();
    let ops = diff_user_quotas(&after_set, &same);
    assert!(ops.is_empty(), "no-change diff should be empty: {ops:?}");

    // 4. Change producer rate, drop request_percentage. The diff
    // produces one Set + one Remove; apply and read back.
    let mut desired = std::collections::BTreeMap::new();
    desired.insert("producer_byte_rate".into(), 2_097_152.0);
    let ops = diff_user_quotas(&after_set, &desired);
    assert_eq!(ops.len(), 2, "set + remove: {ops:?}");
    let outcome = admin.alter_user_quotas(user, &ops, false).await.unwrap();
    assert!(outcome.is_none(), "alter must succeed: {outcome:?}");

    let after_drift = admin.describe_user_quotas(user).await.unwrap();
    assert_eq!(after_drift.len(), 1);
    assert!((after_drift["producer_byte_rate"] - 2_097_152.0).abs() < f64::EPSILON);

    // 5. Remove the remaining key. Read-back is empty.
    let outcome = admin
        .alter_user_quotas(
            user,
            &[QuotaOp::Remove {
                key: "producer_byte_rate".into(),
            }],
            false,
        )
        .await
        .unwrap();
    assert!(outcome.is_none());
    let final_state = admin.describe_user_quotas(user).await.unwrap();
    assert!(
        final_state.is_empty(),
        "broker should report no quotas after remove: {final_state:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validate_only_does_not_persist() {
    support::init_tracing();
    let proc = support::start().await;
    let bootstrap = proc.broker.listen_addr().to_string();
    let mut admin = AdminClient::connect(&[bootstrap]).await.unwrap();

    let user = "bob";

    let outcome = admin
        .alter_user_quotas(
            user,
            &[QuotaOp::Set {
                key: "producer_byte_rate".into(),
                value: 1.0,
            }],
            true, // validate_only
        )
        .await
        .unwrap();
    assert!(outcome.is_none(), "validate must pass: {outcome:?}");

    let after = admin.describe_user_quotas(user).await.unwrap();
    assert!(
        after.is_empty(),
        "validate_only must not persist: {after:?}",
    );
}
