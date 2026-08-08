//! Gateway Prometheus metrics.
//!
//! A process-global `GatewayMetrics` initializes lazily, so any code path can
//! record without a threaded handle. The `/metrics` route renders it.
//!
//! Names follow the Prometheus convention `crabka_gateway_<subject>_<unit>`.
//! `Registry::with_prefix("crabka_gateway")` registers the prefix.
//! `prometheus-client` appends `_total` to `Counter` metrics automatically.

use std::sync::OnceLock;

use crabka_units::prelude::*;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

/// Latency histogram bucket boundaries, from 1 ms to 5 s. They are the Go
/// Prometheus defaults, adapted for sub-second Kafka produce round-trips. The
/// produce-path histogram and the per-method request histograms share them.
const LATENCY_BUCKETS: [Time; 11] = [
    millis(1),
    millis(5),
    millis(10),
    millis(25),
    millis(50),
    millis(100),
    millis(250),
    millis(500),
    secs(1),
    millis(2_500),
    secs(5),
];

/// The bucket boundaries as the seconds `prometheus-client` samples in.
fn latency_buckets() -> impl Iterator<Item = f64> {
    LATENCY_BUCKETS.into_iter().map(TimeExt::secs_f64)
}

/// Label for send/webhook result (`"ok"`, `"error"`, `"unauthorized"`, …).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ResultLabel {
    pub result: String,
}

/// Label for forward outcome (`"ok"`, `"unavailable"`, `"unauthorized"`,
/// `"forward_error"`).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OutcomeLabel {
    pub outcome: String,
}

/// Label for transaction kind (`"commit"`, `"abort"`).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct KindLabel {
    pub kind: String,
}

/// Label for the gateway request method / entry point (`"send"`,
/// `"webhook_in"`, `"produce_http"`, …).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MethodLabel {
    pub method: String,
}

/// Process-global Prometheus metrics bundle for the gRPC gateway.
///
/// Construct it once with [`GatewayMetrics::new`], or with the global
/// [`metrics()`] accessor. Every handle is cheap to clone, because the
/// counters, gauges, and histograms are Arc-backed.
pub struct GatewayMetrics {
    /// The registry that owns all metric descriptors. Exposed so the
    /// `/metrics` render handler can call
    /// `prometheus_client::encoding::text::encode`.
    pub registry: Registry,

    // -- 11 metric handles (§10) ------------------------------------------
    sends_total: Family<ResultLabel, Counter>,
    produce_latency_seconds: Histogram,
    dedup_hits_total: Counter,
    forward_total: Family<OutcomeLabel, Counter>,
    txn_total: Family<KindLabel, Counter>,
    active_subscriptions: Gauge,
    owned_partitions: Gauge,
    webhook_in_total: Family<ResultLabel, Counter>,
    webhook_out_total: Family<ResultLabel, Counter>,
    webhook_retries_total: Counter,
    dead_letter_total: Counter,
    // -- request-level RED signals ----------------------------------------
    /// End-to-end handler latency per request method (histogram family).
    request_duration_seconds: Family<MethodLabel, Histogram>,
    /// Requests currently being served across all entry points (gauge).
    inflight_requests: Gauge,
}

impl GatewayMetrics {
    /// Build a fresh registry with the prefix `crabka_gateway`, register all 11
    /// metrics, and return the bundle. The global [`metrics()`] accessor
    /// normally calls this exactly once.
    #[must_use]
    // flat registration of every metric family
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("crabka_gateway");

        let sends_total = Family::<ResultLabel, Counter>::default();
        registry.register(
            "sends",
            "Cumulative count of produce-path sends, labelled by result \
             (ok, deduplicated, unauthorized, error).",
            sends_total.clone(),
        );

        let produce_latency_seconds = Histogram::new(latency_buckets());
        registry.register(
            "produce_latency_seconds",
            "End-to-end produce latency in seconds (histogram), from \
             handler entry to broker ack.",
            produce_latency_seconds.clone(),
        );

        let dedup_hits_total = Counter::default();
        registry.register(
            "dedup_hits",
            "Cumulative count of produce requests short-circuited by the \
             idempotent-deduplication cache.",
            dedup_hits_total.clone(),
        );

        let forward_total = Family::<OutcomeLabel, Counter>::default();
        registry.register(
            "forward",
            "Cumulative count of cross-cluster forward attempts, labelled \
             by outcome (ok, unavailable, unauthorized, forward_error).",
            forward_total.clone(),
        );

        let txn_total = Family::<KindLabel, Counter>::default();
        registry.register(
            "txn",
            "Cumulative count of dedup-store transaction completions, \
             labelled by kind (commit, abort).",
            txn_total.clone(),
        );

        let active_subscriptions: Gauge = Gauge::default();
        registry.register(
            "active_subscriptions",
            "Current number of live Subscribe streams (gauge).",
            active_subscriptions.clone(),
        );

        let owned_partitions: Gauge = Gauge::default();
        registry.register(
            "owned_partitions",
            "Current number of topic-partitions owned by the dedup store \
             on this gateway instance (gauge).",
            owned_partitions.clone(),
        );

        let webhook_in_total = Family::<ResultLabel, Counter>::default();
        registry.register(
            "webhook_in",
            "Cumulative count of inbound webhook (HTTP → Kafka) requests, \
             labelled by result (ok, unauthenticated, too_large, not_found, \
             unauthorized, bad_request, error).",
            webhook_in_total.clone(),
        );

        let webhook_out_total = Family::<ResultLabel, Counter>::default();
        registry.register(
            "webhook_out",
            "Cumulative count of outbound webhook (Kafka → HTTP) delivery \
             attempts, labelled by result (delivered, dead_letter, dropped).",
            webhook_out_total.clone(),
        );

        let webhook_retries_total = Counter::default();
        registry.register(
            "webhook_retries",
            "Cumulative count of outbound webhook delivery retries \
             (each backoff retry increments this counter).",
            webhook_retries_total.clone(),
        );

        let dead_letter_total = Counter::default();
        registry.register(
            "dead_letter",
            "Cumulative count of outbound webhook messages routed to the \
             dead-letter topic after all retry attempts were exhausted.",
            dead_letter_total.clone(),
        );

        // A Histogram family: `prometheus-client` needs a constructor closure
        // because `Histogram` is not `Default` (each series is seeded with the
        // same buckets used for the produce-latency histogram).
        let request_duration_seconds =
            Family::<MethodLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(latency_buckets())
            });
        registry.register(
            "request_duration_seconds",
            "End-to-end gateway handler latency in seconds (histogram), \
             labelled by request method (send, webhook_in, produce_http).",
            request_duration_seconds.clone(),
        );

        let inflight_requests: Gauge = Gauge::default();
        registry.register(
            "inflight_requests",
            "Number of gateway requests currently being served (gauge).",
            inflight_requests.clone(),
        );

        Self {
            registry,
            sends_total,
            produce_latency_seconds,
            dedup_hits_total,
            forward_total,
            txn_total,
            active_subscriptions,
            owned_partitions,
            webhook_in_total,
            webhook_out_total,
            webhook_retries_total,
            dead_letter_total,
            request_duration_seconds,
            inflight_requests,
        }
    }

    // -- Recorder helpers ---------------------------------------------------
    // One-liner call sites; the label allocation happens here so callers only
    // pass a plain `&str`.

    /// Record a produce-path send with the given `result` label
    /// (`"ok"`, `"deduplicated"`, `"unauthorized"`, `"error"`).
    pub fn record_send(&self, result: &str) {
        self.sends_total
            .get_or_create(&ResultLabel {
                result: result.into(),
            })
            .inc();
    }

    /// Record an end-to-end produce latency observation. Prometheus samples
    /// are floats, so this method converts the extent to seconds.
    pub fn observe_produce_latency(&self, latency: Time) {
        self.produce_latency_seconds.observe(latency.secs_f64());
    }

    /// Bump the deduplication-cache hit counter.
    pub fn record_dedup_hit(&self) {
        self.dedup_hits_total.inc();
    }

    /// Record a forward attempt with the given `outcome` label
    /// (`"ok"`, `"unavailable"`, `"unauthorized"`, `"forward_error"`).
    pub fn record_forward(&self, outcome: &str) {
        self.forward_total
            .get_or_create(&OutcomeLabel {
                outcome: outcome.into(),
            })
            .inc();
    }

    /// Record a dedup-store transaction completion with the given `kind`
    /// (`"commit"` or `"abort"`).
    pub fn record_txn(&self, kind: &str) {
        self.txn_total
            .get_or_create(&KindLabel { kind: kind.into() })
            .inc();
    }

    /// Set the owned-partitions gauge to `n`.
    pub fn set_owned_partitions(&self, n: i64) {
        self.owned_partitions.set(n);
    }

    /// Increment the active-subscriptions gauge. Call this at the start of a
    /// Subscribe stream.
    pub fn inc_active_subscriptions(&self) {
        self.active_subscriptions.inc();
    }

    /// Decrement the active-subscriptions gauge. Call this at the end of a
    /// Subscribe stream, on all exit paths.
    pub fn dec_active_subscriptions(&self) {
        self.active_subscriptions.dec();
    }

    /// Record an inbound webhook request result.
    pub fn record_webhook_in(&self, result: &str) {
        self.webhook_in_total
            .get_or_create(&ResultLabel {
                result: result.into(),
            })
            .inc();
    }

    /// Record an outbound webhook delivery result.
    pub fn record_webhook_out(&self, result: &str) {
        self.webhook_out_total
            .get_or_create(&ResultLabel {
                result: result.into(),
            })
            .inc();
    }

    /// Bump the outbound webhook retry counter once per backoff retry.
    pub fn record_webhook_retry(&self) {
        self.webhook_retries_total.inc();
    }

    /// Bump the dead-letter counter once per message sent to the DLQ.
    pub fn record_dead_letter(&self) {
        self.dead_letter_total.inc();
    }

    /// Observe an end-to-end handler latency for `method` (`"send"`,
    /// `"webhook_in"`, `"produce_http"`).
    pub fn observe_request_duration(&self, method: &str, latency: Time) {
        self.request_duration_seconds
            .get_or_create(&MethodLabel {
                method: method.into(),
            })
            .observe(latency.secs_f64());
    }

    /// Increment the in-flight-requests gauge. Call this at handler entry.
    pub fn inc_inflight(&self) {
        self.inflight_requests.inc();
    }

    /// Decrement the in-flight-requests gauge. Call this at handler exit, on
    /// all paths.
    pub fn dec_inflight(&self) {
        self.inflight_requests.dec();
    }

    /// Begin an in-flight request for `method`.
    ///
    /// This method bumps the in-flight gauge and starts a latency timer. On
    /// drop, the returned [`RequestGuard`] decrements the gauge and observes
    /// the elapsed latency into `request_duration_seconds{method}`. The guard
    /// covers every early-return path of a handler with many exits, such as the
    /// webhook guards, so no path needs its own `dec` or `observe` call.
    #[must_use]
    pub fn begin_request(&'static self, method: &'static str) -> RequestGuard {
        self.inc_inflight();
        RequestGuard {
            metrics: self,
            method,
            start: std::time::Instant::now(),
        }
    }
}

/// RAII guard returned by [`GatewayMetrics::begin_request`]. On drop, it
/// decrements the in-flight gauge and records the handler latency.
pub struct RequestGuard {
    metrics: &'static GatewayMetrics,
    method: &'static str,
    start: std::time::Instant,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.metrics
            .observe_request_duration(self.method, self.start.elapsed().as_time());
        self.metrics.dec_inflight();
    }
}

impl Default for GatewayMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Process-global accessor
// ---------------------------------------------------------------------------

static METRICS: OnceLock<GatewayMetrics> = OnceLock::new();

/// Return the process-global [`GatewayMetrics`] instance. The first call
/// initializes it. It is safe to call before the binary initializes anything
/// else.
#[must_use]
pub fn metrics() -> &'static GatewayMetrics {
    METRICS.get_or_init(GatewayMetrics::new)
}

// ---------------------------------------------------------------------------
// /metrics router
// ---------------------------------------------------------------------------

/// Build an [`axum::Router`] that serves the `OpenMetrics` text encoding of the
/// global gateway registry at `GET /metrics`.
pub fn router() -> axum::Router {
    axum::Router::new().route("/metrics", axum::routing::get(render))
}

async fn render() -> impl axum::response::IntoResponse {
    use axum::response::IntoResponse as _;
    let mut buf = String::new();
    match prometheus_client::encoding::text::encode(&mut buf, &metrics().registry) {
        Ok(()) => (
            axum::http::StatusCode::OK,
            [(
                "content-type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            buf,
        )
            .into_response(),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode: {e}"),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A fresh `GatewayMetrics`, not the global one, so each test stays isolated.
    fn fresh() -> GatewayMetrics {
        GatewayMetrics::new()
    }

    fn encode(m: &GatewayMetrics) -> String {
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &m.registry).unwrap();
        buf
    }

    #[test]
    fn registry_prefix_and_sends_total_present() {
        // Use the global accessor (as the spec requires) for the primary check,
        // then encode a fresh instance to keep the assertion deterministic.
        metrics().record_send("ok"); // exercises the global
        let m = fresh();
        m.record_send("ok");
        let buf = encode(&m);
        // prometheus-client appends _total for Counter; prefix = crabka_gateway.
        assert2::assert!(buf.contains("crabka_gateway_sends_total"));
    }

    #[test]
    fn all_metrics_appear_in_encoded_output() {
        let m = fresh();

        m.record_send("ok");
        m.observe_produce_latency(millis(42));
        m.record_dedup_hit();
        m.record_forward("ok");
        m.record_txn("commit");
        m.inc_active_subscriptions();
        m.set_owned_partitions(3);
        m.record_webhook_in("ok");
        m.record_webhook_out("delivered");
        m.record_webhook_retry();
        m.record_dead_letter();
        m.observe_request_duration("send", millis(10));
        m.inc_inflight();

        let buf = encode(&m);

        let expected = [
            "crabka_gateway_sends_total",
            "crabka_gateway_produce_latency_seconds",
            "crabka_gateway_dedup_hits_total",
            "crabka_gateway_forward_total",
            "crabka_gateway_txn_total",
            "crabka_gateway_active_subscriptions",
            "crabka_gateway_owned_partitions",
            "crabka_gateway_webhook_in_total",
            "crabka_gateway_webhook_out_total",
            "crabka_gateway_webhook_retries_total",
            "crabka_gateway_dead_letter_total",
            "crabka_gateway_request_duration_seconds_bucket",
            "crabka_gateway_request_duration_seconds_count",
            "crabka_gateway_inflight_requests",
        ];
        let missing: Vec<_> = expected
            .into_iter()
            .filter(|needle| !buf.contains(needle))
            .collect();
        assert2::assert!(missing == Vec::<&str>::new());
    }

    #[test]
    fn request_duration_labelled_per_method_and_inflight_moves() {
        let m = fresh();
        m.observe_request_duration("send", millis(10));
        m.observe_request_duration("send", millis(500));
        m.observe_request_duration("webhook_in", millis(20));

        let buf = encode(&m);
        assert2::assert!(buf.contains("method=\"send\""));
        assert2::assert!(buf.contains("method=\"webhook_in\""));

        m.inc_inflight();
        m.inc_inflight();
        assert2::assert!(m.inflight_requests.get() == 2);
        m.dec_inflight();
        assert2::assert!(m.inflight_requests.get() == 1);
    }

    #[test]
    fn send_counter_increments_per_result_label() {
        let m = fresh();
        m.record_send("ok");
        m.record_send("ok");
        m.record_send("error");

        let lbl_ok = ResultLabel {
            result: "ok".into(),
        };
        let lbl_err = ResultLabel {
            result: "error".into(),
        };
        assert2::assert!(m.sends_total.get_or_create(&lbl_ok).get() == 2);
        assert2::assert!(m.sends_total.get_or_create(&lbl_err).get() == 1);
    }

    #[test]
    fn gauge_inc_dec_set_work() {
        let m = fresh();
        m.inc_active_subscriptions();
        m.inc_active_subscriptions();
        assert2::assert!(m.active_subscriptions.get() == 2);
        m.dec_active_subscriptions();
        assert2::assert!(m.active_subscriptions.get() == 1);

        m.set_owned_partitions(7);
        assert2::assert!(m.owned_partitions.get() == 7);
    }

    #[test]
    fn histogram_observe_appears_in_output() {
        let m = fresh();
        m.observe_produce_latency(millis(10));
        m.observe_produce_latency(millis(500));
        let buf = encode(&m);
        assert2::assert!(buf.contains("crabka_gateway_produce_latency_seconds_bucket"));
        assert2::assert!(buf.contains("crabka_gateway_produce_latency_seconds_count"));
    }

    #[test]
    fn forward_and_txn_labels_are_independent() {
        let m = fresh();
        m.record_forward("ok");
        m.record_forward("ok");
        m.record_forward("unavailable");
        m.record_txn("commit");
        m.record_txn("abort");

        let ok = OutcomeLabel {
            outcome: "ok".into(),
        };
        let unavail = OutcomeLabel {
            outcome: "unavailable".into(),
        };
        assert2::assert!(m.forward_total.get_or_create(&ok).get() == 2);
        assert2::assert!(m.forward_total.get_or_create(&unavail).get() == 1);

        let commit = KindLabel {
            kind: "commit".into(),
        };
        let abort = KindLabel {
            kind: "abort".into(),
        };
        assert2::assert!(m.txn_total.get_or_create(&commit).get() == 1);
        assert2::assert!(m.txn_total.get_or_create(&abort).get() == 1);
    }

    #[test]
    fn webhook_counters_accumulate_independently() {
        let m = fresh();
        m.record_webhook_in("ok");
        m.record_webhook_in("error");
        m.record_webhook_out("delivered");
        m.record_webhook_out("dead_letter");
        m.record_webhook_retry();
        m.record_webhook_retry();
        m.record_dead_letter();

        let ok = ResultLabel {
            result: "ok".into(),
        };
        let err = ResultLabel {
            result: "error".into(),
        };
        let delivered = ResultLabel {
            result: "delivered".into(),
        };
        let dl = ResultLabel {
            result: "dead_letter".into(),
        };
        check!(
            (
                m.webhook_in_total.get_or_create(&ok).get(),
                m.webhook_in_total.get_or_create(&err).get(),
                m.webhook_out_total.get_or_create(&delivered).get(),
                m.webhook_out_total.get_or_create(&dl).get(),
                m.webhook_retries_total.get(),
                m.dead_letter_total.get(),
            ) == (1, 1, 1, 1, 2, 1)
        );
    }
}
