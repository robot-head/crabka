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

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD as B64STD};
use bytes::Bytes;
use crabka_client_consumer::{Assignor, AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};
use crabka_client_producer::{Header, Producer, ProducerRecord};
use jsonpath_rust::{parser::model::JpQuery, query::js_path_process};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    codec::RecordCodec, error::GatewayError, metrics::metrics,
    outbound_config::CompiledSubscription,
};

/// RAII guard that decrements the active-subscriptions gauge exactly once on
/// drop, regardless of how `run_subscription` exits (normal shutdown, poll
/// error, or early return).
struct ActiveSubscriptionGuard;

impl Drop for ActiveSubscriptionGuard {
    fn drop(&mut self) {
        metrics().dec_active_subscriptions();
    }
}

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
    consumer_policy: (
        Option<crabka_client_core::security::ClientSecurity>,
        Duration,
    ),
    codec: Arc<dyn RecordCodec>,
) -> Result<(), GatewayError> {
    let (security, poll_timeout) = consumer_policy;
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
        .maybe_security(security)
        .build()
        .await?;

    metrics().inc_active_subscriptions();
    let _guard = ActiveSubscriptionGuard;

    let mut poll_err: Option<GatewayError> = None;
    loop {
        let batch = tokio::select! {
            () = shutdown.cancelled() => break,
            b = consumer.poll(poll_timeout) => match b {
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

        deliver_batch(&http, &sub, &producer, codec.as_ref(), batch).await;

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
    codec: &dyn RecordCodec,
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
    let codec = &codec;
    let futures = by_part.into_values().map(|recs| async move {
        for rec in recs {
            deliver_one(http, sub, producer, *codec, &rec).await;
        }
    });
    futures_util::future::join_all(futures).await;
}

/// Deliver one record: filter → render → sign → POST with exponential-backoff
/// retries → dead-letter on exhaustion. Returns once the record is delivered
/// (2xx), skipped by the filter, or dead-lettered/dropped after `max_attempts`
/// — i.e. once it may be committed as part of the batch.
#[tracing::instrument(skip_all)]
async fn deliver_one(
    http: &reqwest::Client,
    sub: &CompiledSubscription,
    producer: &Producer,
    codec: &dyn RecordCodec,
    rec: &ConsumerRecord,
) {
    // 1. Filter: a non-matching record is skipped (counts as delivered so the
    //    batch still advances). A non-JSON body never matches a JSON filter.
    if let Some(q) = &sub.filter
        && !passes_filter(q, rec)
    {
        return;
    }

    // 2. Build the delivery body. event_id = topic-partition-offset is the
    //    receiver's dedup key (X-Crabka-Event-Id). When `decode_to_json` is set,
    //    the (Confluent-framed) record value is decoded and its JSON view is
    //    delivered verbatim; otherwise — and whenever decode yields no JSON or
    //    errors — the standard signed JSON envelope is delivered (raw path,
    //    inert under `RawCodec`).
    let event_id = format!("{}-{}-{}", rec.topic, rec.partition, rec.offset);
    let body = decoded_body(codec, sub, rec)
        .await
        .unwrap_or_else(|| render_envelope(&event_id, rec));
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
            Ok(resp) if resp.status().is_success() => {
                metrics().record_webhook_out("delivered");
                return; // delivered
            }
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
        metrics().record_webhook_retry();
        tokio::time::sleep(backoff_with_jitter(
            attempt,
            sub.base_backoff_ms,
            sub.max_backoff_ms,
        ))
        .await;
    }
}

/// The decoded-to-JSON delivery body, or `None` to fall back to the envelope.
///
/// Returns `None` (envelope path) when `decode_to_json` is off, the record
/// value is empty, the codec yields no JSON view (e.g. `RawCodec`, or a record
/// that wasn't Confluent-framed), or decode errors. A decode error is logged
/// and treated as non-fatal — the record is still delivered (as the envelope),
/// preserving at-least-once.
async fn decoded_body(
    codec: &dyn RecordCodec,
    sub: &CompiledSubscription,
    rec: &ConsumerRecord,
) -> Option<Vec<u8>> {
    if !sub.decode_to_json {
        return None;
    }
    let value = rec.value.clone()?;
    match codec.decode(&rec.topic, value).await {
        Ok(decoded) => decoded.json.map(|j| j.to_vec()),
        Err(e) => {
            tracing::debug!(
                subscription = %sub.name,
                topic = %rec.topic,
                partition = rec.partition,
                offset = rec.offset,
                error = %e,
                "outbound decode_to_json failed; delivering raw envelope",
            );
            None
        }
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
        match r.val {
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
        metrics().record_webhook_out("dropped");
        return;
    };

    metrics().record_dead_letter();
    metrics().record_webhook_out("dead_letter");

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
// Unit tests for the outbound engine.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use jsonpath_rust::parser::parse_json_path;

    use super::*;
    use crate::codec::{CodecError, Decoded, EncodeBody, RawCodec};

    /// A codec stub whose `decode` returns a fixed [`Decoded`], or a
    /// `CodecError::Registry` when constructed with `None` — exercising the
    /// `decode_to_json` delivery path (and its decode-error fallback) without a
    /// registry. `encode` is unused on the outbound path. (`CodecError` is not
    /// `Clone`, so the error case is represented as `None` and synthesized.)
    struct StubCodec(Option<Decoded>);

    #[async_trait::async_trait]
    impl RecordCodec for StubCodec {
        async fn encode(&self, _topic: &str, _body: EncodeBody) -> Result<Bytes, CodecError> {
            unreachable!("outbound path never encodes")
        }
        async fn decode(&self, _topic: &str, _value: Bytes) -> Result<Decoded, CodecError> {
            match &self.0 {
                Some(d) => Ok(d.clone()),
                None => Err(CodecError::Registry("stub decode error".into())),
            }
        }
    }

    /// A `Decoded` carrying a JSON view (the de-framed structured payload).
    fn decoded_with_json(json: &[u8]) -> Decoded {
        Decoded {
            value: Bytes::from(json.to_vec()),
            schema: None,
            json: Some(Bytes::from(json.to_vec())),
        }
    }

    fn sub_with_decode(decode_to_json: bool) -> CompiledSubscription {
        CompiledSubscription {
            name: "dec".into(),
            source_topics: vec!["events".into()],
            target_url: "https://hooks.example.com/x".into(),
            signing_secret: None,
            dead_letter_topic: None,
            max_attempts: 1,
            base_backoff_ms: 1,
            max_backoff_ms: 1,
            request_timeout_ms: 1,
            filter: None,
            headers: vec![],
            decode_to_json,
        }
    }

    fn rec_with_value(value: Option<&[u8]>) -> ConsumerRecord {
        ConsumerRecord {
            topic: "events".into(),
            partition: 3,
            offset: 42,
            leader_epoch: 0,
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"k1")),
            value: value.map(|v| Bytes::from(v.to_vec())),
            headers: vec![],
        }
    }

    #[test]
    fn envelope_renders_each_value_shape() {
        for (_name, value, expected_value) in [
            (
                "raw-json",
                Some(br#"{"type":"order","n":7}"#.as_slice()),
                serde_json::json!({"type": "order", "n": 7}),
            ),
            (
                "binary-base64",
                Some([0xff, 0x00, 0x10].as_slice()),
                serde_json::json!({"_base64": B64STD.encode([0xff, 0x00, 0x10])}),
            ),
            ("empty-null", None, Value::Null),
        ] {
            let rec = rec_with_value(value);
            let body = render_envelope("events-3-42", &rec);
            let actual: Value = serde_json::from_slice(&body).expect("envelope is JSON");
            let expected = serde_json::json!({
                "event_id": "events-3-42",
                "topic": "events",
                "partition": 3,
                "offset": 42,
                "timestamp_ms": 1_700_000_000_000_i64,
                "key": B64STD.encode(b"k1"),
                "value": expected_value,
            });
            assert2::assert!(actual == expected);
        }
    }

    #[test]
    fn filter_cases() {
        let q = parse_json_path("$.deliver").expect("compile");
        for (_name, value, expected) in [
            ("truthy_path", Some(br#"{"deliver":true}"#.as_slice()), true),
            (
                "false_path",
                Some(br#"{"deliver":false}"#.as_slice()),
                false,
            ),
            ("missing_path", Some(br#"{"other":1}"#.as_slice()), false),
            ("non_json_body", Some(b"not json at all".as_slice()), false),
            ("empty_value", None, false),
        ] {
            let rec = rec_with_value(value);
            assert2::assert!(passes_filter(&q, &rec) == expected);
        }
    }

    #[test]
    fn backoff_grows_and_caps_at_max() {
        // attempt 1: exp = min(100, 1000) = 100, range [50, 100].
        let d1 = backoff_with_jitter(1, 100, 1000);
        assert2::assert!(d1.as_millis() >= 50 && d1.as_millis() <= 100);
        // attempt 10 saturates the cap: exp = min(100 * 2^9, 1000) = 1000,
        // range [500, 1000]; must not panic on the large shift.
        let d10 = backoff_with_jitter(10, 100, 1000);
        assert2::assert!(d10.as_millis() >= 500 && d10.as_millis() <= 1000);
    }

    #[test]
    fn backoff_does_not_overflow_on_high_attempt() {
        // 2^(u32::MAX - 1) overflows a u64 shift; saturating_pow must clamp.
        let d = backoff_with_jitter(u32::MAX, 500, 30_000);
        assert2::assert!(d.as_millis() >= 15_000 && d.as_millis() <= 30_000);
    }

    // -----------------------------------------------------------------------
    // decoded_body: decode_to_json delivery path
    // -----------------------------------------------------------------------

    /// Every decode-to-JSON outcome is checked as one named table: successful
    /// JSON delivery, and each envelope fallback reason.
    #[tokio::test]
    async fn decode_to_json_delivery_cases() {
        type TestCase1<'a> = (
            &'a str,
            Box<dyn RecordCodec>,
            bool,
            Option<&'a [u8]>,
            Option<Vec<u8>>,
        );
        let json = br#"{"decoded":true,"n":7}"#;
        let cases: [TestCase1<'_>; 5] = [
            (
                "decoded JSON",
                Box::new(StubCodec(Some(decoded_with_json(json)))),
                true,
                Some(b"\x00\x00\x00\x00\x01raw-framed-bytes"),
                Some(json.to_vec()),
            ),
            (
                "decode disabled",
                Box::new(StubCodec(Some(decoded_with_json(b"{}")))),
                false,
                Some(br#"{"n":1}"#),
                None,
            ),
            (
                "raw codec",
                Box::new(RawCodec),
                true,
                Some(br#"{"n":1}"#),
                None,
            ),
            (
                "decode error",
                Box::new(StubCodec(None)),
                true,
                Some(br#"{"n":1}"#),
                None,
            ),
            (
                "empty value",
                Box::new(StubCodec(Some(decoded_with_json(b"{}")))),
                true,
                None,
                None,
            ),
        ];

        for (_name, codec, decode_to_json, value, expected) in cases {
            let sub = sub_with_decode(decode_to_json);
            let rec = rec_with_value(value);
            let actual = decoded_body(codec.as_ref(), &sub, &rec).await;
            assert2::assert!(actual == expected);
        }
    }
}
