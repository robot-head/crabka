//! Outbound webhook delivery, with one task per subscription.
//!
//! Delivery runs one batch at a time, so the commit boundary equals the
//! delivered boundary. The consumer commits the whole polled position, so the
//! gateway delivers the whole batch before it commits. Within one partition,
//! the gateway delivers in offset order, retries with exponential backoff plus
//! jitter, and dead-letters on exhaustion. Delivery is at-least-once, so
//! receivers dedup on X-Crabka-Event-Id.
//!
//! The consumer API has no commit-specific-offset and no pause/resume, so the
//! polled batch must be the commit and backpressure unit. A crash mid-batch
//! re-delivers the whole batch, because the gateway never commits undelivered
//! records, and the receiver dedups. For head-of-line blocking, a failing
//! record blocks ONLY its own partition until a 2xx or the DLQ. Partitions
//! deliver concurrently.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD as B64STD};
use bytes::Bytes;
use crabka_client_consumer::{Assignor, AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};
use crabka_client_producer::{Header, Producer, ProducerRecord};
use crabka_units::prelude::*;
use jsonpath_rust::{parser::model::JpQuery, query::js_path_process};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    ce_translate,
    codec::RecordCodec,
    error::GatewayError,
    metrics::metrics,
    outbound_config::{CompiledSubscription, OutboundContentMode},
};

const CLOUDEVENTS_JSON_CONTENT_TYPE: &str = "application/cloudevents+json; charset=UTF-8";

/// RAII guard that decrements the active-subscriptions gauge exactly once on
/// drop. It does this however `run_subscription` exits: on a normal shutdown,
/// on a poll error, or on an early return.
struct ActiveSubscriptionGuard;

impl Drop for ActiveSubscriptionGuard {
    fn drop(&mut self) {
        metrics().dec_active_subscriptions();
    }
}

/// Run a subscription's delivery loop until `shutdown` fires.
///
/// The task joins the consumer group `__crabka_grpc_wh_{name}` on the
/// subscription's source topics. It then loops one batch at a time: poll, group
/// by `(topic, partition)` in offset order, deliver each partition's records in
/// sequence with the partitions concurrent, and `commit_sync` only once the
/// WHOLE batch is delivered or dead-lettered. The task closes the consumer on
/// exit so the coordinator task and the group member do not leak.
///
/// # Errors
///
/// Returns [`GatewayError`] if the consumer cannot be built, if the HTTP client
/// cannot be constructed, or if a poll error stops the loop. A failed
/// `commit_sync` is not fatal; the task logs it. The uncommitted batch
/// re-delivers, which is correct under at-least-once.
pub async fn run_subscription(
    sub: CompiledSubscription,
    bootstrap: String,
    client_id: String,
    producer: Arc<Producer>,
    shutdown: CancellationToken,
    consumer_policy: (Option<crabka_client_core::security::ClientSecurity>, Time),
    codec: Arc<dyn RecordCodec>,
) -> Result<(), GatewayError> {
    run_subscription_with_policy(
        sub,
        bootstrap,
        client_id,
        producer,
        shutdown,
        (
            consumer_policy.0,
            consumer_policy.1,
            crate::config::GatewayRuntimeConfig::default(),
        ),
        codec,
    )
    .await
}

/// Run a subscription with the deployment's client resource policy.
/// # Errors
/// Returns an error when consumer or delivery setup fails.
pub async fn run_subscription_with_policy(
    sub: CompiledSubscription,
    bootstrap: String,
    client_id: String,
    producer: Arc<Producer>,
    shutdown: CancellationToken,
    consumer_policy: (
        Option<crabka_client_core::security::ClientSecurity>,
        Time,
        crate::config::GatewayRuntimeConfig,
    ),
    codec: Arc<dyn RecordCodec>,
) -> Result<(), GatewayError> {
    let (security, poll_timeout, policy) = consumer_policy;
    let http = reqwest::Client::builder()
        .timeout(sub.request_timeout.to_std())
        .build()
        .map_err(|e| GatewayError::Other(format!("build outbound http client: {e}")))?;

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .dispatch_queue_capacity(policy.client_dispatch_queue_capacity.get())
        .frame_max(policy.client_frame_max.size())
        .group_id(sub.group_id.clone())
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

/// Deliver one polled batch.
///
/// The function groups by `(topic, partition)` and sorts each partition by
/// ascending offset. It then delivers partitions CONCURRENTLY, while records
/// WITHIN a partition go SEQUENTIALLY. Offset order is the ordering guarantee.
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

/// Deliver one record.
///
/// The steps are: filter, render, sign, POST with exponential-backoff retries,
/// then dead-letter on exhaustion. The function returns once the record is
/// delivered with a 2xx, skipped by the filter, or dead-lettered or dropped
/// after `max_attempts`. At that point the batch can commit the record.
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

    // 2. Build the mode-specific body and binding headers. Invalid
    //    CloudEvents records never reach the HTTP target; they follow the same
    //    DLQ/drop policy as exhausted requests.
    let event_id = format!("{}-{}-{}", rec.topic, rec.partition, rec.offset);
    let delivery = match render_delivery(codec, sub, &event_id, rec).await {
        Ok(delivery) => delivery,
        Err(error) => {
            tracing::warn!(
                subscription = %sub.name,
                event = %event_id,
                error = %error,
                "outbound CloudEvents record is invalid; dropping or dead-lettering",
            );
            dead_letter(producer, sub, rec, &event_id).await;
            return;
        }
    };
    let ts = now_unix_ms();
    let sig = sub
        .signing_secret
        .as_ref()
        .map(|s| crate::webhook_config::sign_hmac_hex(s, &delivery.body));

    // 3. POST with exponential backoff + jitter up to max_attempts.
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let mut req = http
            .post(&sub.target_url)
            .header("X-Crabka-Event-Id", &event_id)
            .header("X-Crabka-Timestamp", ts.to_string())
            .body(delivery.body.clone());
        for (name, value) in &delivery.headers {
            req = req.header(name, value);
        }
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
        tokio::time::sleep(
            backoff_with_jitter(attempt, sub.base_backoff, sub.max_backoff).to_std(),
        )
        .await;
    }
}

struct RenderedDelivery {
    body: Vec<u8>,
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

async fn render_delivery(
    codec: &dyn RecordCodec,
    sub: &CompiledSubscription,
    event_id: &str,
    rec: &ConsumerRecord,
) -> Result<RenderedDelivery, ce_translate::CeError> {
    match sub.content_mode {
        OutboundContentMode::Envelope => Ok(RenderedDelivery {
            body: decoded_body(codec, sub, rec)
                .await
                .unwrap_or_else(|| render_envelope(event_id, rec)),
            headers: vec![content_type_header("application/json")],
        }),
        OutboundContentMode::CloudEventsBinary => render_cloudevents_binary(rec),
        OutboundContentMode::CloudEventsStructured => render_cloudevents_structured(rec),
    }
}

fn render_cloudevents_binary(
    rec: &ConsumerRecord,
) -> Result<RenderedDelivery, ce_translate::CeError> {
    let (headers, body) = if is_structured_at_rest(rec)? {
        let event = parse_structured_record(rec)?;
        let (headers, body) = ce_translate::binary_from_structured(&event)?;
        (
            headers
                .into_iter()
                .map(|(key, value)| (key, Some(value)))
                .collect(),
            body.to_vec(),
        )
    } else {
        let headers = record_headers(rec);
        validate_record_cloudevent(&headers)?;
        (headers, rec.value.clone().unwrap_or_default().to_vec())
    };

    Ok(RenderedDelivery {
        body,
        headers: ce_translate::kafka_headers_to_http(&headers)?,
    })
}

fn render_cloudevents_structured(
    rec: &ConsumerRecord,
) -> Result<RenderedDelivery, ce_translate::CeError> {
    let body = if is_structured_at_rest(rec)? {
        let event = parse_structured_record(rec)?;
        ce_translate::validate_structured_json(&event)?;
        rec.value.clone().unwrap_or_default().to_vec()
    } else {
        let headers = record_headers(rec);
        validate_record_cloudevent(&headers)?;
        ce_translate::structured_from_binary(&headers, rec.value.as_deref().unwrap_or_default())?
    };

    Ok(RenderedDelivery {
        body,
        headers: vec![content_type_header(CLOUDEVENTS_JSON_CONTENT_TYPE)],
    })
}

fn record_headers(rec: &ConsumerRecord) -> Vec<(String, Option<Bytes>)> {
    rec.headers
        .iter()
        .map(|header| (header.key.clone(), header.value.clone()))
        .collect()
}

fn validate_record_cloudevent(
    headers: &[(String, Option<Bytes>)],
) -> Result<(), ce_translate::CeError> {
    let present = headers
        .iter()
        .filter_map(|(key, value)| Some((key.clone(), value.clone()?)))
        .collect::<Vec<_>>();
    ce_translate::validate_binary_required(&present)
}

fn is_structured_at_rest(rec: &ConsumerRecord) -> Result<bool, ce_translate::CeError> {
    for header in &rec.headers {
        if header.key != "content-type" {
            continue;
        }
        let Some(value) = header.value.as_deref() else {
            continue;
        };
        let value = std::str::from_utf8(value)
            .map_err(|_| ce_translate::CeError::NonUtf8Attribute(header.key.clone()))?;
        if ce_translate::is_structured_media_type(value) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_structured_record(rec: &ConsumerRecord) -> Result<Value, ce_translate::CeError> {
    serde_json::from_slice(rec.value.as_deref().unwrap_or_default())
        .map_err(|_| ce_translate::CeError::MalformedJson)
}

fn content_type_header(
    value: &'static str,
) -> (reqwest::header::HeaderName, reqwest::header::HeaderValue) {
    (
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static(value),
    )
}

/// The decoded-to-JSON delivery body, or `None` to fall back to the envelope.
///
/// Returns `None` (envelope path) when `decode_to_json` is off, the record
/// value is empty, the codec yields no JSON view (e.g. `RawCodec`, or a record
/// that wasn't Confluent-framed), or decode errors. A decode error is not
/// fatal. The gateway logs it and still delivers the record as the envelope,
/// which keeps at-least-once.
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

/// Render the delivery envelope as serialized JSON bytes.
///
/// The envelope embeds the value as raw JSON when the record value parses as
/// JSON, and otherwise wraps it as `{"_base64": "..."}`. The key is base64.
/// Record headers are an ordered, duplicate-preserving array; each value is
/// base64 or `null`.
fn render_envelope(event_id: &str, rec: &ConsumerRecord) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "event_id": event_id,
        "topic": rec.topic,
        "partition": rec.partition,
        "offset": rec.offset,
        "timestamp_ms": rec.timestamp,
        "key": rec.key.as_ref().map(|k| b64(k)),
        "value": value_field(rec),
        "headers": rec.headers.iter().map(|header| json!({
            "key": header.key,
            "value": header.value.as_ref().map(|value| b64(value)),
        })).collect::<Vec<_>>(),
    }))
    .unwrap_or_default()
}

/// The `value` field of the envelope. It is raw JSON if the record value is
/// valid JSON, a `{"_base64": "..."}` wrapper if it is not, and `Null` for an
/// empty value.
fn value_field(rec: &ConsumerRecord) -> Value {
    match &rec.value {
        None => Value::Null,
        Some(v) => {
            serde_json::from_slice::<Value>(v).unwrap_or_else(|_| json!({ "_base64": b64(v) }))
        }
    }
}

/// Whether `rec`'s JSON body matches the filter.
///
/// This function matches the broker's `evaluate_custom_claim_check`. A non-JSON
/// body never matches. The `JSONPath` must give a non-empty result, and no
/// element in that result can be `null` or `false`.
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

/// Full-ish jitter exponential backoff, with no `rand` dependency.
///
/// The deterministic component is `min(base * 2^(attempt-1), max) / 2`, plus a
/// jitter in `0..=half`. `attempt` is 1-based.
fn backoff_with_jitter(attempt: u32, base: Time, max: Time) -> Time {
    // `saturating_pow` clamps the doubling so a large `attempt` cannot overflow
    // before the cap applies.
    let doubled = base * f64::from(2u32.saturating_pow(attempt - 1));
    let capped = if doubled > max { max } else { doubled };
    let half = capped * 0.5;
    half + half * jitter().as_f64()
}

/// Pseudo-random jitter in `0..1`, with no `rand` dependency. It is the current
/// sub-second nanos as a fraction of one second.
fn jitter() -> Ratio {
    fraction(Time::from_nanos(i64::from(subsec_nanos())).secs_f64())
}

/// Dead-letter an exhausted record.
///
/// When a DLQ topic is configured, produce the original key and value with the
/// `x-crabka-dlq-source` header, which holds the event id, and the
/// `x-crabka-dlq-reason` header. Otherwise log the record and drop it. The
/// retries already satisfied at-least-once, and the batch advances either way.
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

/// The wall clock's current sub-second nanos, which is the jitter seed.
fn subsec_nanos() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos())
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
    /// `CodecError::Registry` when the caller constructs it with `None`.
    ///
    /// The stub exercises the `decode_to_json` delivery path and its
    /// decode-error fallback without a registry. The outbound path never calls
    /// `encode`. `CodecError` is not `Clone`, so the stub holds the error case
    /// as `None` and builds the error on demand.
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

    /// A `Decoded` that carries a JSON view, the de-framed structured payload.
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
            group_id: "__crabka_grpc_wh_dec".into(),
            source_topics: vec!["events".into()],
            target_url: "https://hooks.example.com/x".into(),
            signing_secret: None,
            dead_letter_topic: None,
            max_attempts: 1,
            base_backoff: millis(1),
            max_backoff: millis(1),
            request_timeout: millis(1),
            filter: None,
            headers: vec![],
            content_mode: OutboundContentMode::Envelope,
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

    fn binary_cloudevent_record(value: &[u8]) -> ConsumerRecord {
        let mut record = rec_with_value(Some(value));
        record.headers = vec![
            crabka_client_consumer::Header {
                key: "ce_id".into(),
                value: Some(Bytes::from_static(b"event-1")),
            },
            crabka_client_consumer::Header {
                key: "ce_source".into(),
                value: Some(Bytes::from_static(b"/tests")),
            },
            crabka_client_consumer::Header {
                key: "ce_type".into(),
                value: Some(Bytes::from_static(b"example.created")),
            },
            crabka_client_consumer::Header {
                key: "ce_specversion".into(),
                value: Some(Bytes::from_static(b"1.0")),
            },
            crabka_client_consumer::Header {
                key: "content-type".into(),
                value: Some(Bytes::from_static(b"application/json")),
            },
        ];
        record
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
                "headers": [],
            });
            assert2::assert!(actual == expected);
        }
    }

    #[test]
    fn envelope_preserves_order_duplicates_and_null_header_values() {
        let mut rec = rec_with_value(Some(br#"{"n":1}"#));
        rec.headers = vec![
            crabka_client_consumer::Header {
                key: "ce-type".into(),
                value: Some(Bytes::from_static(b"order")),
            },
            crabka_client_consumer::Header {
                key: "duplicate".into(),
                value: Some(Bytes::from_static(b"first")),
            },
            crabka_client_consumer::Header {
                key: "duplicate".into(),
                value: Some(Bytes::from_static(b"last")),
            },
            crabka_client_consumer::Header {
                key: "null-value".into(),
                value: None,
            },
        ];

        let actual: Value = serde_json::from_slice(&render_envelope("events-3-42", &rec))
            .expect("envelope is JSON");

        assert2::assert!(
            actual["headers"]
                == serde_json::json!([
                    {"key": "ce-type", "value": B64STD.encode(b"order")},
                    {"key": "duplicate", "value": B64STD.encode(b"first")},
                    {"key": "duplicate", "value": B64STD.encode(b"last")},
                    {"key": "null-value", "value": null},
                ])
        );
    }

    #[tokio::test]
    async fn cloudevents_binary_delivery_has_raw_body_and_binding_headers() {
        let codec = RawCodec;
        let mut sub = sub_with_decode(false);
        sub.content_mode = OutboundContentMode::CloudEventsBinary;
        let record = binary_cloudevent_record(br#"{"n":1}"#);

        let delivery = render_delivery(&codec, &sub, "events-3-42", &record)
            .await
            .expect("binary delivery renders");

        assert2::assert!(delivery.body == br#"{"n":1}"#);
        assert2::assert!(
            delivery.headers.iter().any(|(name, value)| {
                name.as_str() == "ce-id" && value.as_bytes() == b"event-1"
            })
        );
        assert2::assert!(delivery.headers.iter().any(|(name, value)| {
            name.as_str() == "content-type" && value.as_bytes() == b"application/json"
        }));
    }

    #[tokio::test]
    async fn cloudevents_structured_delivery_uses_charset_media_type() {
        let codec = RawCodec;
        let mut sub = sub_with_decode(false);
        sub.content_mode = OutboundContentMode::CloudEventsStructured;
        let record = binary_cloudevent_record(br#"{"n":1}"#);

        let delivery = render_delivery(&codec, &sub, "events-3-42", &record)
            .await
            .expect("structured delivery renders");
        let event: Value = serde_json::from_slice(&delivery.body).expect("CloudEvent JSON");

        assert2::assert!(event["id"] == "event-1");
        assert2::assert!(event["data"]["n"] == 1);
        assert2::assert!(delivery.headers.iter().any(|(name, value)| {
            name.as_str() == "content-type"
                && value.as_bytes() == CLOUDEVENTS_JSON_CONTENT_TYPE.as_bytes()
        }));
    }

    #[tokio::test]
    async fn structured_at_rest_is_verbatim_or_converted_for_requested_mode() {
        let codec = RawCodec;
        let body = br#"{ "specversion":"1.0", "id":"event-1", "source":"/tests", "type":"example.created", "datacontenttype":"application/json", "data":{"n":7} }"#;
        let mut record = rec_with_value(Some(body));
        record.headers = vec![crabka_client_consumer::Header {
            key: "content-type".into(),
            value: Some(Bytes::from_static(
                b"application/cloudevents+json; charset=UTF-8",
            )),
        }];

        let mut sub = sub_with_decode(false);
        sub.content_mode = OutboundContentMode::CloudEventsStructured;
        let structured = render_delivery(&codec, &sub, "events-3-42", &record)
            .await
            .expect("structured delivery renders");
        assert2::assert!(structured.body == body);

        sub.content_mode = OutboundContentMode::CloudEventsBinary;
        let binary = render_delivery(&codec, &sub, "events-3-42", &record)
            .await
            .expect("binary delivery renders");
        assert2::assert!(binary.body == br#"{"n":7}"#);
        assert2::assert!(
            binary.headers.iter().any(|(name, value)| {
                name.as_str() == "ce-id" && value.as_bytes() == b"event-1"
            })
        );
    }

    #[tokio::test]
    async fn cloudevents_delivery_rejects_missing_required_attributes() {
        let codec = RawCodec;
        let mut sub = sub_with_decode(false);
        sub.content_mode = OutboundContentMode::CloudEventsBinary;
        let record = rec_with_value(Some(b"data"));

        let result = render_delivery(&codec, &sub, "events-3-42", &record).await;

        assert2::assert!(matches!(
            result,
            Err(ce_translate::CeError::MissingAttribute("id"))
        ));
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
        // attempt 1: exp = min(100ms, 1s) = 100ms, range [50ms, 100ms].
        let first = backoff_with_jitter(1, millis(100), secs(1));
        assert2::assert!(first >= millis(50) && first <= millis(100));
        // attempt 10 saturates the cap: exp = min(100ms * 2^9, 1s) = 1s,
        // range [500ms, 1s]; must not panic on the large doubling.
        let tenth = backoff_with_jitter(10, millis(100), secs(1));
        assert2::assert!(tenth >= millis(500) && tenth <= secs(1));
    }

    #[test]
    fn backoff_does_not_overflow_on_high_attempt() {
        // 2^(u32::MAX - 1) overflows an integer shift; saturating_pow clamps it
        // and the cap then applies.
        let backoff = backoff_with_jitter(u32::MAX, millis(500), secs(30));
        assert2::assert!(backoff >= secs(15) && backoff <= secs(30));
    }

    // -----------------------------------------------------------------------
    // decoded_body: decode_to_json delivery path
    // -----------------------------------------------------------------------

    /// One named table checks every decode-to-JSON outcome: successful JSON
    /// delivery, and each envelope fallback reason.
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
