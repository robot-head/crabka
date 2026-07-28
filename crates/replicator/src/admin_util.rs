//! Shared admin/IO helpers used by replicator engine tasks.
//!
//! All functions return `Result<_, String>` and map client errors via
//! `.map_err(|e| e.to_string())` so callers stay wire-error-agnostic.

use std::collections::BTreeMap;

use bytes::Bytes;
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::security::ClientSecurity;
use crabka_units::prelude::{Time, millis, secs};

/// Kafka error code: the topic already exists.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// How long the broker may take to complete a `CreateTopics` request before it
/// answers with a timeout error. Kafka carries this as an `int32` millisecond
/// field, so it converts at the `create_topics` call.
const CREATE_TOPICS_TIMEOUT: Time = secs(10);

/// Per-call fetch wait used while draining an internal topic.
const DRAIN_POLL_TIMEOUT: Time = millis(500);

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
            CREATE_TOPICS_TIMEOUT,
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
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
            CREATE_TOPICS_TIMEOUT,
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
/// Uses N=3 consecutive empty polls ([`DRAIN_POLL_TIMEOUT`] each) as the drain sentinel.
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
        match consumer.poll(DRAIN_POLL_TIMEOUT).await {
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
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
    use crabka_units::prelude::{TimeExt as _, secs};

    #[test]
    fn create_topics_timeout_reaches_the_wire_as_int32_millis() {
        // Kafka's `CreateTopics.timeoutMs` is an int32 count of milliseconds, so
        // the configured extent must cross the seam as 10000 — a seconds-valued
        // 10 would have the broker give up almost immediately.
        assert2::assert!(super::CREATE_TOPICS_TIMEOUT.millis_i32() == 10_000);
    }

    #[test]
    fn drain_poll_timeout_is_half_a_second() {
        assert2::assert!(super::DRAIN_POLL_TIMEOUT == crabka_units::millis(500));
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
