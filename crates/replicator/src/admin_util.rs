//! Shared admin/IO helpers used by replicator engine tasks.
//!
//! All functions return `Result<_, String>` and map client errors via
//! `.map_err(|e| e.to_string())` so callers stay wire-error-agnostic.

use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::security::ClientSecurity;

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
pub async fn ensure_topic(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    security: Option<ClientSecurity>,
) -> Result<(), String> {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_string()], security)
        .await
        .map_err(|e| e.to_string())?;

    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.to_string(),
                partitions,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
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

/// Ensure a compacted topic exists with 1 partition and 1 replica.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(topic = %topic, bootstrap = %bootstrap),
    err,
)]
pub async fn ensure_compacted_topic(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
) -> Result<(), String> {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_string()], security)
        .await
        .map_err(|e| e.to_string())?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());

    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.to_string(),
                partitions: 1,
                replicas: 1,
                configs,
            }],
            10_000,
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

/// Build a drain consumer for the given topic.  Security is threaded through
/// conditionally — `bon` wraps bare `T` setters in `Some`, so passing `None`
/// means simply omitting the `.security()` call.
async fn build_drain_consumer(
    bootstrap: &str,
    group_id: String,
    topic: &str,
    security: Option<ClientSecurity>,
) -> Result<Consumer, crabka_client_consumer::ConsumerError> {
    if let Some(sec) = security {
        Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(group_id)
            .client_id("crabka-replicator-util")
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
            .subscribe(vec![topic.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
    }
}

/// Drain all records from `topic` from the earliest offset, returning
/// `(key, value)` pairs in order.
///
/// Uses N=3 consecutive empty polls (500 ms each) as the drain sentinel.
/// Poll errors for a not-yet-existing topic are silently treated as empty.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(topic = %topic, drained = tracing::field::Empty),
    err,
)]
#[allow(clippy::type_complexity)]
pub async fn read_all(
    bootstrap: &str,
    topic: &str,
    security: Option<ClientSecurity>,
) -> Result<Vec<(Option<Bytes>, Option<Bytes>)>, String> {
    const MAX_EMPTY: usize = 3;

    let group_id = format!("crabka-replicator-reader-{topic}");

    let mut consumer = match build_drain_consumer(bootstrap, group_id, topic, security).await {
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
        match consumer.poll(Duration::from_millis(500)).await {
            Ok(batch) => {
                if batch.is_empty() {
                    consecutive_empty += 1;
                    if consecutive_empty >= MAX_EMPTY {
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
pub async fn read_last_value_for_key(
    bootstrap: &str,
    topic: &str,
    key: &[u8],
    security: Option<ClientSecurity>,
) -> Result<Option<Vec<u8>>, String> {
    let all = read_all(bootstrap, topic, security).await?;

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

        crate::test_util::await_topic_count(&b, "t", 2, std::time::Duration::from_secs(5)).await;
    }
}
