//! Shared admin/IO helpers used by replicator engine tasks.
//!
//! All functions return `Result<_, String>` and map client errors via
//! `.map_err(|e| e.to_string())` so callers stay wire-error-agnostic.

use std::collections::BTreeMap;

use bytes::Bytes;
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::security::ClientSecurity;

use crate::config::{ClientResourcePolicy, ReplicationFactor, ReplicatorRuntimePolicy};

/// Kafka error code: the topic already exists.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Ensure `topic` exists with the given parameters, treating an
/// already-exists response as success.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(topic = %topic, partitions, bootstrap = %bootstrap),
    err,
)]
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn ensure_topic(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    security: Option<ClientSecurity>,
) -> Result<(), String> {
    ensure_topic_with_policy(
        bootstrap,
        topic,
        partitions,
        security,
        ClientResourcePolicy::default(),
    )
    .await
}

/// Ensure a topic using the deployment's client resource policy.
///
/// # Errors
///
/// Returns an error when configuration is invalid, protocol encoding fails,
/// the broker rejects the request, or transport I/O fails.
pub async fn ensure_topic_with_policy(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
) -> Result<(), String> {
    let runtime_policy = ReplicatorRuntimePolicy::default();
    ensure_topic_with_runtime_policy(
        bootstrap,
        topic,
        partitions,
        security,
        client_resource_policy,
        &runtime_policy,
        runtime_policy.data_topic_replication_factor,
    )
    .await
}

/// Ensure a topic with an explicit replication factor.
///
/// # Errors
///
/// Returns an error when the admin client cannot create or inspect the topic.
pub async fn ensure_topic_with_replication_factor(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    security: Option<ClientSecurity>,
    replication_factor: ReplicationFactor,
) -> Result<(), String> {
    let runtime_policy = ReplicatorRuntimePolicy {
        data_topic_replication_factor: replication_factor,
        ..ReplicatorRuntimePolicy::default()
    };
    ensure_topic_with_runtime_policy(
        bootstrap,
        topic,
        partitions,
        security,
        ClientResourcePolicy::default(),
        &runtime_policy,
        replication_factor,
    )
    .await
}

/// Ensure a topic using explicit process and replication policy.
pub(crate) async fn ensure_topic_with_runtime_policy(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
    runtime_policy: &ReplicatorRuntimePolicy,
    replication_factor: ReplicationFactor,
) -> Result<(), String> {
    let mut admin = AdminClient::connect_with_options(
        &[bootstrap.to_string()],
        admin_options(security, client_resource_policy, runtime_policy)?,
    )
    .await
    .map_err(|e| e.to_string())?;

    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.to_string(),
                partitions,
                replicas: i32::from(replication_factor.get()),
                configs: BTreeMap::new(),
            }],
            runtime_policy.topic_create_timeout,
        )
        .await
        .map_err(|e| e.to_string())?;

    for outcome in &outcomes {
        if let Some(ref err) = outcome.error
            && err.code != TOPIC_ALREADY_EXISTS
        {
            return Err(format!(
                "create_topic {topic}: error code {} ({}): {:?}",
                err.code, err.name, err.message
            ));
        }
    }

    Ok(())
}

/// Ensure a compacted topic exists with one partition and the default replica count.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(topic = %topic, bootstrap = %bootstrap),
    err,
)]
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn ensure_compacted_topic(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
) -> Result<(), String> {
    ensure_compacted_topic_with_policy(bootstrap, topic, security, ClientResourcePolicy::default())
        .await
}

/// Ensure a compacted topic using the deployment's client resource policy.
///
/// # Errors
///
/// Returns an error when configuration is invalid, protocol encoding fails,
/// the broker rejects the request, or transport I/O fails.
pub async fn ensure_compacted_topic_with_policy(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
) -> Result<(), String> {
    let runtime_policy = ReplicatorRuntimePolicy::default();
    ensure_compacted_topic_with_runtime_policy(
        bootstrap,
        topic,
        security,
        client_resource_policy,
        &runtime_policy,
    )
    .await
}

/// Ensure a compacted topic with an explicit replication factor.
///
/// # Errors
///
/// Returns an error when the admin client cannot create or inspect the topic.
pub async fn ensure_compacted_topic_with_replication_factor(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
    replication_factor: ReplicationFactor,
) -> Result<(), String> {
    let runtime_policy = ReplicatorRuntimePolicy {
        internal_topic_replication_factor: replication_factor,
        ..ReplicatorRuntimePolicy::default()
    };
    ensure_compacted_topic_with_runtime_policy(
        bootstrap,
        topic,
        security,
        ClientResourcePolicy::default(),
        &runtime_policy,
    )
    .await
}

pub(crate) async fn ensure_compacted_topic_with_runtime_policy(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
    runtime_policy: &ReplicatorRuntimePolicy,
) -> Result<(), String> {
    let mut admin = AdminClient::connect_with_options(
        &[bootstrap.to_string()],
        admin_options(security, client_resource_policy, runtime_policy)?,
    )
    .await
    .map_err(|e| e.to_string())?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());

    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.to_string(),
                partitions: 1,
                replicas: i32::from(runtime_policy.internal_topic_replication_factor.get()),
                configs,
            }],
            runtime_policy.topic_create_timeout,
        )
        .await
        .map_err(|e| e.to_string())?;

    for outcome in &outcomes {
        if let Some(ref err) = outcome.error
            && err.code != TOPIC_ALREADY_EXISTS
        {
            return Err(format!(
                "ensure_compacted_topic {topic}: error code {} ({}): {:?}",
                err.code, err.name, err.message
            ));
        }
    }

    Ok(())
}

fn admin_options(
    security: Option<ClientSecurity>,
    policy: ClientResourcePolicy,
    runtime_policy: &ReplicatorRuntimePolicy,
) -> Result<crabka_client_core::ConnectionOptions, String> {
    Ok(crabka_client_core::ConnectionOptions {
        dns_timeout: crabka_client_core::ClientDnsTimeout::new(runtime_policy.client_dns_timeout)?,
        connect_timeout: runtime_policy.client_connect_timeout,
        request_timeout: runtime_policy.client_request_timeout,
        client_id: "crabka-operator".to_owned(),
        dispatch_queue_capacity: policy.dispatch_queue_capacity,
        frame_max: policy.frame_max,
        security: security.map(Box::new),
    })
}

/// Build a drain consumer for the given topic.  Security is threaded through
/// conditionally — `bon` wraps bare `T` setters in `Some`, so passing `None`
/// means simply omitting the `.security()` call.
async fn build_drain_consumer(
    bootstrap: &str,
    group_id: String,
    topic: &str,
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
) -> Result<Consumer, crabka_client_consumer::ConsumerError> {
    if let Some(sec) = security {
        Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(group_id)
            .client_id("crabka-replicator-util")
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .subscribe(vec![topic.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .security(sec)
            .build()
            .await
    } else {
        Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(group_id)
            .client_id("crabka-replicator-util")
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .subscribe(vec![topic.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
    }
}

/// Drain all records from `topic` from the earliest offset, returning
/// `(key, value)` pairs in order.
///
/// Uses the runtime policy's consecutive-empty threshold as the drain sentinel.
/// Poll errors for a not-yet-existing topic are silently treated as empty.
pub type RawRecord = (Option<Bytes>, Option<Bytes>);

#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(topic = %topic, drained = tracing::field::Empty),
    err,
)]
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn read_all(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
) -> Result<Vec<RawRecord>, String> {
    read_all_with_policy(bootstrap, topic, security, ClientResourcePolicy::default()).await
}

/// Drain a topic using the deployment's client resource policy.
///
/// # Errors
///
/// Returns an error when configuration is invalid, protocol encoding fails,
/// the broker rejects the request, or transport I/O fails.
pub async fn read_all_with_policy(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
) -> Result<Vec<RawRecord>, String> {
    let runtime_policy = ReplicatorRuntimePolicy::default();
    read_all_with_runtime_policy(
        bootstrap,
        topic,
        security,
        client_resource_policy,
        &runtime_policy,
    )
    .await
}

pub(crate) async fn read_all_with_runtime_policy(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
    runtime_policy: &ReplicatorRuntimePolicy,
) -> Result<Vec<RawRecord>, String> {
    let group_id = format!("crabka-replicator-reader-{topic}");

    let mut consumer =
        match build_drain_consumer(bootstrap, group_id, topic, security, client_resource_policy)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                let msg = e.to_string();
                if is_unknown_topic_error(&msg) {
                    return Ok(Vec::new());
                }
                return Err(msg);
            }
        };

    let mut records = Vec::new();
    let mut consecutive_empty = 0usize;

    loop {
        match consumer
            .poll(runtime_policy.internal_drain_poll_timeout)
            .await
        {
            Ok(batch) => {
                if batch.is_empty() {
                    consecutive_empty += 1;
                    if consecutive_empty >= runtime_policy.internal_drain_empty_polls.get() {
                        break;
                    }
                } else {
                    consecutive_empty = 0;
                    for r in batch {
                        records.push((r.key, r.value));
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if is_unknown_topic_error(&msg) {
                    break;
                }
                let _ = consumer.close().await;
                return Err(msg);
            }
        }
    }

    let _ = consumer.close().await;
    tracing::Span::current().record("drained", records.len());
    Ok(records)
}

/// Return the value bytes of the last record whose key equals `key`.
///
/// If `key` is empty, returns the last record overall regardless of key.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn read_last_value_for_key(
    bootstrap: &str,
    topic: &str,
    key: &[u8],
    security: Option<ClientSecurity>,
) -> Result<Option<Vec<u8>>, String> {
    read_last_value_for_key_with_policy(
        bootstrap,
        topic,
        key,
        security,
        ClientResourcePolicy::default(),
    )
    .await
}

/// Read a key using the deployment's client resource policy.
///
/// # Errors
///
/// Returns an error when configuration is invalid, protocol encoding fails,
/// the broker rejects the request, or transport I/O fails.
pub async fn read_last_value_for_key_with_policy(
    bootstrap: &str,
    topic: &str,
    key: &[u8],
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
) -> Result<Option<Vec<u8>>, String> {
    let runtime_policy = ReplicatorRuntimePolicy::default();
    read_last_value_for_key_with_runtime_policy(
        bootstrap,
        topic,
        key,
        security,
        client_resource_policy,
        &runtime_policy,
    )
    .await
}

pub(crate) async fn read_last_value_for_key_with_runtime_policy(
    bootstrap: &str,
    topic: &str,
    key: &[u8],
    security: Option<ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
    runtime_policy: &ReplicatorRuntimePolicy,
) -> Result<Option<Vec<u8>>, String> {
    let all = read_all_with_runtime_policy(
        bootstrap,
        topic,
        security,
        client_resource_policy,
        runtime_policy,
    )
    .await?;

    let matched = if key.is_empty() {
        all.into_iter().last()
    } else {
        all.into_iter()
            .filter(|(k, _)| k.as_deref() == Some(key))
            .last()
    };

    Ok(matched.and_then(|(_, v)| v.map(|b| b.to_vec())))
}

/// Returns `true` if the error message indicates the topic doesn't exist.
fn is_unknown_topic_error(msg: &str) -> bool {
    msg.contains("UNKNOWN_TOPIC_OR_PARTITION")
        || msg.contains("unknown topic")
        || msg.contains("UnknownTopicOrPartition")
        || msg.contains("NotSubscribed")
}

#[cfg(test)]
mod tests {
    use crabka_units::prelude::{TimeExt as _, secs};

    #[test]
    fn create_topics_timeout_reaches_the_wire_as_int32_millis() {
        // Kafka's `CreateTopics.timeoutMs` is an int32 count of milliseconds, so
        // the configured extent must cross the seam as 10000 — a seconds-valued
        // 10 would have the broker give up almost immediately.
        let policy = crate::config::ReplicatorRuntimePolicy::default();
        assert2::assert!(policy.topic_create_timeout.millis_i32() == 10_000);
    }

    #[test]
    fn drain_poll_timeout_is_half_a_second() {
        let policy = crate::config::ReplicatorRuntimePolicy::default();
        assert2::assert!(policy.internal_drain_poll_timeout == crabka_units::millis(500));
    }

    #[test]
    fn unknown_topic_error_matches_each_substring() {
        // Each positive exercises exactly one of the OR'd substrings, so the
        // single-substring match must hold on its own (kills `||`→`&&`, which
        // would require all four, and the `-> true`/`-> false` constants).
        for (msg, want) in [
            ("x UNKNOWN_TOPIC_OR_PARTITION y", true),
            ("unknown topic foo", true),
            ("UnknownTopicOrPartition", true),
            ("err: NotSubscribed", true),
            // A message with none of the substrings must not match (kills `-> true`).
            ("connection refused", false),
        ] {
            assert2::assert!(super::is_unknown_topic_error(msg) == want);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ensure_produce_read_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let b = broker.listen_addr().to_string();

        crate::test_util::create_topic(&b, "t", 1).await;
        crate::test_util::produce(&b, "t", b"k1", b"v1").await;
        crate::test_util::produce(&b, "t", b"k1", b"v2").await;

        assert2::assert!(crate::test_util::topic_record_count(&b, "t").await == 2);

        let last = super::read_last_value_for_key(&b, "t", b"k1", None)
            .await
            .unwrap();
        assert2::assert!(last.as_deref() == Some(b"v2".as_slice()));

        super::ensure_compacted_topic(&b, "state", None)
            .await
            .unwrap();

        crate::test_util::await_topic_count(&b, "t", 2, secs(5)).await;
    }
}
