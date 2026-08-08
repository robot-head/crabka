//! Integration test for the client-quota admin RPCs.
//!
//! The test drives `DescribeClientQuotas` (`api_key` 48) and
//! `AlterClientQuotas` (`api_key` 49) against a live in-process broker. It then
//! asserts the end-to-end wire behavior.
//!
//! The pipeline matches the quota path of the operator's `KafkaUser` reconcile:
//! read the current per-user state, diff it, write the resulting
//! `(set, remove)` ops, then read the state back.

use assert2::check;
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
    assert2::assert!(initial.is_empty());

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
    assert2::assert!(outcome.is_none());

    let after_set = admin.describe_user_quotas(user).await.unwrap();
    assert2::assert!(after_set.len() == 2);
    check!((after_set["producer_byte_rate"] - 1_048_576.0).abs() < f64::EPSILON);
    check!((after_set["request_percentage"] - 25.0).abs() < f64::EPSILON);

    // 3. `diff_user_quotas` with the same desired-state map → no ops.
    let same = after_set.clone();
    let ops = diff_user_quotas(&after_set, &same);
    assert2::assert!(ops.is_empty());

    // 4. Change producer rate, drop request_percentage. The diff
    // produces one Set + one Remove; apply and read back.
    let mut desired = std::collections::BTreeMap::new();
    desired.insert("producer_byte_rate".into(), 2_097_152.0);
    let ops = diff_user_quotas(&after_set, &desired);
    assert2::assert!(ops.len() == 2);
    let outcome = admin.alter_user_quotas(user, &ops, false).await.unwrap();
    assert2::assert!(outcome.is_none());

    let after_drift = admin.describe_user_quotas(user).await.unwrap();
    assert2::assert!(after_drift.len() == 1);
    assert2::assert!((after_drift["producer_byte_rate"] - 2_097_152.0).abs() < f64::EPSILON);

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
    assert2::assert!(outcome.is_none());
    let final_state = admin.describe_user_quotas(user).await.unwrap();
    assert2::assert!(final_state.is_empty());
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
    assert2::assert!(outcome.is_none());

    let after = admin.describe_user_quotas(user).await.unwrap();
    assert2::assert!(after.is_empty());
}
