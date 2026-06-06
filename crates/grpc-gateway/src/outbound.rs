//! Outbound webhook delivery: one task per subscription. Batch-at-a-time so the
//! commit boundary == the delivered boundary (the consumer commits the whole
//! polled position, so we deliver the whole batch before committing). Per
//! partition: deliver in offset order, retry with exponential backoff + jitter,
//! dead-letter on exhaustion. At-least-once; receivers dedup on X-Crabka-Event-Id.
//!
//! Forced design (the consumer API has no commit-specific-offset, no
//! pause/resume): the polled batch is the commit + backpressure unit. A crash
//! mid-batch re-delivers the whole batch (never commits undelivered records);
//! the receiver dedups. Head-of-line: a failing record blocks ONLY its own
//! partition (partitions deliver concurrently) until 2xx or DLQ.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64STD;
use bytes::Bytes;
use crabka_client_consumer::{Assignor, AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};
use crabka_client_producer::{Header, Producer, ProducerRecord};
use jsonpath_rust::parser::model::JpQuery;
use jsonpath_rust::query::js_path_process;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::error::GatewayError;
use crate::outbound_config::CompiledSubscription;

/// Run a subscription's delivery loop until `shutdown` fires.
///
/// Joins the consumer group `__crabka_grpc_wh_{name}` on the subscription's
/// source topics, then loops batch-at-a-time: poll → group by `(topic,
/// partition)` in offset order → deliver each partition's records sequentially
/// (partitions concurrently) → `commit_sync` only once the WHOLE batch is
/// delivered-or-dead-lettered. Closes the consumer on exit so the coordinator
/// task + group member don't leak.
///
/// # Errors
///
/// Returns [`GatewayError`] if the consumer cannot be built or the HTTP client
/// cannot be constructed, or a poll error terminates the loop. A failed
/// `commit_sync` is logged (not fatal): the uncommitted batch re-delivers,
/// which is fine under at-least-once.
pub async fn run_subscription(
    sub: CompiledSubscription,
    bootstrap: String,
    client_id: String,
    producer: Arc<Producer>,
    shutdown: CancellationToken,
) -> Result<(), GatewayError> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(sub.request_timeout_ms))
        .build()
        .map_err(|e| GatewayError::Other(format!("build outbound http client: {e}")))?;

    let group = format!("__crabka_grpc_wh_{}", sub.name);
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group)
        .subscribe(sub.source_topics.clone())
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .assignor(Assignor::CooperativeSticky)
        .build()
        .await?;

    let mut poll_err: Option<GatewayError> = None;
    loop {
        let batch = tokio::select! {
            () = shutdown.cancelled() => break,
            b = consumer.poll(Duration::from_millis(500)) => match b {
                Ok(b) => b,
                Err(e) => {
                    poll_err = Some(e.into());
                    break;
                }
            },
        };
        if batch.is_empty() {
            continue;
        }

        deliver_batch(&http, &sub, &producer, batch).await;

        // The whole batch is delivered or dead-lettered ⇒ advance the committed
        // position. On failure we log and continue: the uncommitted position
        // re-delivers (at-least-once; receivers dedup on X-Crabka-Event-Id).
        if let Err(e) = consumer.commit_sync().await {
            tracing::warn!(
                subscription = %sub.name,
                error = %e,
                "outbound commit failed; batch will redeliver",
            );
        }
    }

    let _ = consumer.close().await;
    match poll_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Deliver one polled batch: group by `(topic, partition)`, sort each partition
/// by ascending offset, then deliver partitions CONCURRENTLY while records
/// WITHIN a partition go SEQUENTIALLY (offset order is the ordering guarantee).
async fn deliver_batch(
    http: &reqwest::Client,
    sub: &CompiledSubscription,
    producer: &Producer,
    batch: Vec<ConsumerRecord>,
) {
    let mut by_part: std::collections::BTreeMap<(String, i32), Vec<ConsumerRecord>> =
        std::collections::BTreeMap::new();
    for r in batch {
        by_part
            .entry((r.topic.clone(), r.partition))
            .or_default()
            .push(r);
    }
    for recs in by_part.values_mut() {
        recs.sort_by_key(|r| r.offset);
    }

    // Bind references before the closure so each per-partition future borrows
    // (rather than moves) the shared client/sub/producer. `join_all(..).await`
    // completes before the next loop iteration, so the borrows are sound.
    let http = &http;
    let sub = &sub;
    let producer = &producer;
    let futures = by_part.into_values().map(|recs| async move {
        for rec in recs {
            deliver_one(http, sub, producer, &rec).await;
        }
    });
    futures_util::future::join_all(futures).await;
}

/// Deliver one record: filter → render → sign → POST with exponential-backoff
/// retries → dead-letter on exhaustion. Returns once the record is delivered
/// (2xx), skipped by the filter, or dead-lettered/dropped after `max_attempts`
/// — i.e. once it may be committed as part of the batch.
async fn deliver_one(
    http: &reqwest::Client,
    sub: &CompiledSubscription,
    producer: &Producer,
    rec: &ConsumerRecord,
) {
    // 1. Filter: a non-matching record is skipped (counts as delivered so the
    //    batch still advances). A non-JSON body never matches a JSON filter.
    if let Some(q) = &sub.filter
        && !passes_filter(q, rec)
    {
        return;
    }

    // 2. Render the signed JSON envelope. event_id = topic-partition-offset is
    //    the receiver's dedup key (X-Crabka-Event-Id).
    let event_id = format!("{}-{}-{}", rec.topic, rec.partition, rec.offset);
    let body = render_envelope(&event_id, rec);
    let ts = now_unix_ms();
    let sig = sub
        .signing_secret
        .as_ref()
        .map(|s| crate::webhook_config::sign_hmac_hex(s, &body));

    // 3. POST with exponential backoff + jitter up to max_attempts.
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut req = http
            .post(&sub.target_url)
            .header("X-Crabka-Event-Id", &event_id)
            .header("X-Crabka-Timestamp", ts.to_string())
            .header("content-type", "application/json")
            .body(body.clone());
        if let Some(sig) = &sig {
            req = req.header("X-Crabka-Signature", sig);
        }
        for (k, v) in &sub.headers {
            req = req.header(k, v);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => return, // delivered
            Ok(resp) => tracing::debug!(
                subscription = %sub.name,
                event = %event_id,
                status = %resp.status(),
                attempt,
                "outbound non-2xx",
            ),
            Err(e) => tracing::debug!(
                subscription = %sub.name,
                event = %event_id,
                error = %e,
                attempt,
                "outbound request failed",
            ),
        }

        if attempt >= sub.max_attempts {
            dead_letter(producer, sub, rec, &event_id).await;
            return;
        }
        tokio::time::sleep(backoff_with_jitter(
            attempt,
            sub.base_backoff_ms,
            sub.max_backoff_ms,
        ))
        .await;
    }
}

/// Render the delivery envelope as serialized JSON bytes. The value is embedded
/// as raw JSON when the record value parses as JSON, otherwise wrapped as
/// `{"_base64": "..."}`; the key is base64. The envelope omits record headers
/// (`ConsumerRecord` exposes none).
fn render_envelope(event_id: &str, rec: &ConsumerRecord) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "event_id": event_id,
        "topic": rec.topic,
        "partition": rec.partition,
        "offset": rec.offset,
        "timestamp_ms": rec.timestamp,
        "key": rec.key.as_ref().map(|k| b64(k)),
        "value": value_field(rec),
    }))
    .unwrap_or_default()
}

/// The `value` field of the envelope: raw JSON if the record value is valid
/// JSON, else a `{"_base64": "..."}` wrapper; `Null` for an empty value.
fn value_field(rec: &ConsumerRecord) -> Value {
    match &rec.value {
        None => Value::Null,
        Some(v) => {
            serde_json::from_slice::<Value>(v).unwrap_or_else(|_| json!({ "_base64": b64(v) }))
        }
    }
}

/// Whether `rec`'s JSON body matches the filter. Mirrors the broker's
/// `evaluate_custom_claim_check`: a non-JSON body never matches; the `JSONPath`
/// must yield a non-empty result with no element being `null` or `false`.
fn passes_filter(q: &JpQuery, rec: &ConsumerRecord) -> bool {
    let Some(bytes) = &rec.value else {
        return false;
    };
    let Ok(v) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    let Ok(refs) = js_path_process(q, &v) else {
        return false;
    };
    if refs.is_empty() {
        return false;
    }
    for r in refs {
        match r.val() {
            Value::Null | Value::Bool(false) => return false,
            _ => {}
        }
    }
    true
}

/// Full-ish jitter exponential backoff (no `rand` dep): the deterministic
/// component is `min(base * 2^(attempt-1), max) / 2`, plus a jitter in
/// `0..=half` seeded from the current sub-second nanos. `attempt` is 1-based.
fn backoff_with_jitter(attempt: u32, base_ms: u64, max_ms: u64) -> Duration {
    let exp = base_ms
        .saturating_mul(2u64.saturating_pow(attempt - 1))
        .min(max_ms);
    let half = exp / 2;
    let jitter = nanos() % (half + 1);
    Duration::from_millis(half + jitter)
}

/// Dead-letter an exhausted record. When a DLQ topic is configured, produce the
/// original key/value with `x-crabka-dlq-source` (the event id) +
/// `x-crabka-dlq-reason` headers; otherwise log and drop (retries already
/// satisfied at-least-once — the batch still advances either way).
async fn dead_letter(
    producer: &Producer,
    sub: &CompiledSubscription,
    rec: &ConsumerRecord,
    event_id: &str,
) {
    let Some(dlq) = &sub.dead_letter_topic else {
        tracing::warn!(
            target: "gateway::audit",
            subscription = %sub.name,
            event = %event_id,
            "outbound delivery exhausted; no dead_letter_topic configured, dropping",
        );
        return;
    };

    let prec = ProducerRecord {
        topic: dlq.clone(),
        partition: None,
        key: rec.key.clone(),
        value: rec.value.clone(),
        headers: vec![
            Header {
                key: "x-crabka-dlq-source".into(),
                value: Some(Bytes::from(event_id.as_bytes().to_vec())),
            },
            Header {
                key: "x-crabka-dlq-reason".into(),
                value: Some(Bytes::from_static(b"delivery exhausted")),
            },
        ],
        timestamp_ms: None,
    };

    match producer.send(prec).await.await {
        Ok(Ok(_)) => tracing::warn!(
            target: "gateway::audit",
            subscription = %sub.name,
            event = %event_id,
            dlq = %dlq,
            "outbound delivery exhausted; dead-lettered",
        ),
        Ok(Err(e)) => tracing::warn!(
            subscription = %sub.name,
            event = %event_id,
            dlq = %dlq,
            error = %e,
            "outbound dead-letter produce failed",
        ),
        Err(_) => tracing::warn!(
            subscription = %sub.name,
            event = %event_id,
            dlq = %dlq,
            "outbound dead-letter produce canceled",
        ),
    }
}

/// Current Unix time in milliseconds (0 on a pre-epoch clock).
fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Pseudo-random jitter source: the current sub-second nanos (no `rand` dep).
fn nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()))
}

/// Standard-base64 encode (the envelope key/value encoding).
fn b64(bytes: &[u8]) -> String {
    B64STD.encode(bytes)
}

// ---------------------------------------------------------------------------
// Unit tests (the engine is not wired into the binary until Task 3).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use jsonpath_rust::parser::parse_json_path;

    use super::*;

    fn rec_with_value(value: Option<&[u8]>) -> ConsumerRecord {
        ConsumerRecord {
            topic: "events".into(),
            partition: 3,
            offset: 42,
            leader_epoch: 0,
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"k1")),
            value: value.map(|v| Bytes::from(v.to_vec())),
        }
    }

    #[test]
    fn envelope_embeds_raw_json_value() {
        let rec = rec_with_value(Some(br#"{"type":"order","n":7}"#));
        let body = render_envelope("events-3-42", &rec);
        let v: Value = serde_json::from_slice(&body).expect("envelope is JSON");
        assert_eq!(v["event_id"], "events-3-42");
        assert_eq!(v["topic"], "events");
        assert_eq!(v["partition"], 3);
        assert_eq!(v["offset"], 42);
        assert_eq!(v["timestamp_ms"], 1_700_000_000_000_i64);
        // Raw JSON is embedded as an object, not a base64 string.
        assert_eq!(v["value"]["type"], "order");
        assert_eq!(v["value"]["n"], 7);
        // Key is base64-encoded.
        assert_eq!(v["key"], B64STD.encode(b"k1"));
    }

    #[test]
    fn envelope_wraps_non_json_value_as_base64() {
        let rec = rec_with_value(Some(&[0xff, 0x00, 0x10]));
        let body = render_envelope("events-3-42", &rec);
        let v: Value = serde_json::from_slice(&body).expect("envelope is JSON");
        assert_eq!(v["value"]["_base64"], B64STD.encode([0xff, 0x00, 0x10]));
    }

    #[test]
    fn envelope_null_value_when_empty() {
        let rec = rec_with_value(None);
        let body = render_envelope("events-3-42", &rec);
        let v: Value = serde_json::from_slice(&body).expect("envelope is JSON");
        assert_eq!(v["value"], Value::Null);
    }

    #[test]
    fn filter_matches_truthy_path() {
        let q = parse_json_path("$.deliver").expect("compile");
        let rec = rec_with_value(Some(br#"{"deliver":true}"#));
        assert!(passes_filter(&q, &rec));
    }

    #[test]
    fn filter_rejects_false_path() {
        let q = parse_json_path("$.deliver").expect("compile");
        let rec = rec_with_value(Some(br#"{"deliver":false}"#));
        assert!(!passes_filter(&q, &rec));
    }

    #[test]
    fn filter_rejects_missing_path() {
        let q = parse_json_path("$.deliver").expect("compile");
        let rec = rec_with_value(Some(br#"{"other":1}"#));
        assert!(!passes_filter(&q, &rec));
    }

    #[test]
    fn filter_rejects_non_json_body() {
        let q = parse_json_path("$.deliver").expect("compile");
        let rec = rec_with_value(Some(b"not json at all"));
        assert!(!passes_filter(&q, &rec));
    }

    #[test]
    fn filter_rejects_empty_value() {
        let q = parse_json_path("$.deliver").expect("compile");
        let rec = rec_with_value(None);
        assert!(!passes_filter(&q, &rec));
    }

    #[test]
    fn backoff_grows_and_caps_at_max() {
        // attempt 1: exp = min(100, 1000) = 100, range [50, 100].
        let d1 = backoff_with_jitter(1, 100, 1000);
        assert!(d1.as_millis() >= 50 && d1.as_millis() <= 100, "{d1:?}");
        // attempt 10 saturates the cap: exp = min(100 * 2^9, 1000) = 1000,
        // range [500, 1000]; must not panic on the large shift.
        let d10 = backoff_with_jitter(10, 100, 1000);
        assert!(d10.as_millis() >= 500 && d10.as_millis() <= 1000, "{d10:?}");
    }

    #[test]
    fn backoff_does_not_overflow_on_high_attempt() {
        // 2^(u32::MAX - 1) overflows a u64 shift; saturating_pow must clamp.
        let d = backoff_with_jitter(u32::MAX, 500, 30_000);
        assert!(d.as_millis() >= 15_000 && d.as_millis() <= 30_000, "{d:?}");
    }
}
