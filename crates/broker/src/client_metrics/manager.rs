//! Per-broker KIP-714 client-metrics state: instance registry, subscription
//! matching, stable subscription-id computation, and push throttling. All
//! state is in-memory (KIP-714 is per-broker — a client pins telemetry to
//! one broker, so no raft replication is needed).

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use crabka_metadata::MetadataImage;
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use uuid::Uuid;

use super::config::{self, ALL_METRICS};

/// Connection-derived attributes used for subscription matching.
#[derive(Debug, Clone)]
pub(crate) struct ClientAttributes {
    pub client_instance_id: Uuid,
    pub client_id: String,
    pub software_name: String,
    pub software_version: String,
    pub source_address: String,
    pub source_port: u16,
}

/// The metric prefixes + push interval a client should use, after unioning
/// every matched subscription.
#[derive(Debug, Clone)]
pub(crate) struct ComputedSubscription {
    pub metrics: Vec<String>,
    pub push_interval_ms: i32,
}

#[derive(Debug)]
struct ClientInstance {
    subscription_id: i32,
    push_interval: Duration,
    metrics: Vec<String>,
    last_get: Instant,
    last_push: Option<Instant>,
    terminating: bool,
    last_error: i16,
}

pub(crate) struct SubscriptionAssignment {
    pub subscription_id: i32,
    pub push_interval_ms: i32,
    pub metrics: Vec<String>,
}

pub(crate) enum PushDecision {
    Accept {
        #[allow(dead_code)] // carried for future per-prefix filtering
        metrics: Vec<String>,
    },
    Reject {
        error_code: i16,
        throttle_ms: i32,
    },
}

pub(crate) struct ClientMetricsManager {
    instances: Mutex<HashMap<Uuid, ClientInstance>>,
    default_interval: Time,
    telemetry_max: ByteSize,
}

/// Compression codecs the broker advertises, in Kafka's fixed order:
/// ZSTD(4), LZ4(3), GZIP(1), SNAPPY(2). NONE is intentionally not advertised.
pub(crate) const ACCEPTED_COMPRESSION_TYPES: [i8; 4] = [4, 3, 1, 2];

impl ClientMetricsManager {
    pub(crate) fn new(telemetry_max: ByteSize, default_interval: Time) -> Self {
        Self {
            instances: Mutex::new(HashMap::new()),
            default_interval,
            telemetry_max,
        }
    }

    /// The KIP-714 `PushTelemetry` size ceiling in the `int32` byte form the
    /// wire response carries.
    pub(crate) fn telemetry_max_bytes(&self) -> i32 {
        self.telemetry_max.bytes_i32()
    }

    pub(crate) fn assign(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
    ) -> SubscriptionAssignment {
        // `push_interval_ms` is both a wire field and a byte-exact input to the
        // subscription-id hash, so the interval crosses into milliseconds here.
        let computed = compute_subscription(image, attrs, self.default_interval.millis_i32());
        let sub_id = subscription_id(&computed, attrs.client_instance_id);
        let now = Instant::now();
        let mut guard = self
            .instances
            .lock()
            .expect("client-metrics mutex poisoned");
        // push_interval_ms is validated in [100, 3_600_000] — always positive.
        let push_interval = Duration::from_millis(
            u64::try_from(computed.push_interval_ms).expect("validated positive push interval"),
        );
        let inst = guard
            .entry(attrs.client_instance_id)
            .or_insert(ClientInstance {
                subscription_id: sub_id,
                push_interval,
                metrics: computed.metrics.clone(),
                last_get: now,
                last_push: None,
                terminating: false,
                last_error: crate::codes::NONE,
            });
        inst.subscription_id = sub_id;
        inst.push_interval = push_interval;
        inst.metrics.clone_from(&computed.metrics);
        inst.last_get = now;
        inst.last_error = crate::codes::NONE;
        SubscriptionAssignment {
            subscription_id: sub_id,
            push_interval_ms: computed.push_interval_ms,
            metrics: computed.metrics,
        }
    }

    pub(crate) fn authorize_push(
        &self,
        client_instance_id: Uuid,
        subscription_id_in: i32,
        terminating: bool,
        compression_supported: bool,
        payload_len: usize,
    ) -> PushDecision {
        let now = Instant::now();
        let mut guard = self
            .instances
            .lock()
            .expect("client-metrics mutex poisoned");

        // 1. Unknown instance → INVALID_REQUEST.
        let Some(inst) = guard.get_mut(&client_instance_id) else {
            return PushDecision::Reject {
                error_code: crate::codes::INVALID_REQUEST,
                throttle_ms: 0,
            };
        };

        // 2. Instance already terminating → INVALID_REQUEST.
        if inst.terminating {
            return PushDecision::Reject {
                error_code: crate::codes::INVALID_REQUEST,
                throttle_ms: 0,
            };
        }

        // 3. Subscription-id mismatch → UNKNOWN_SUBSCRIPTION_ID.
        if subscription_id_in != inst.subscription_id {
            inst.last_error = crate::codes::UNKNOWN_SUBSCRIPTION_ID;
            return PushDecision::Reject {
                error_code: crate::codes::UNKNOWN_SUBSCRIPTION_ID,
                throttle_ms: 0,
            };
        }

        // 4. Throttle check (Kafka order: throttle before codec/size).
        let interval_elapsed = inst
            .last_push
            .is_none_or(|lp| now.duration_since(lp) >= inst.push_interval);
        let first_after_get = inst.last_push.is_none_or(|lp| inst.last_get > lp);
        if !terminating && !interval_elapsed && !first_after_get {
            inst.last_error = crate::codes::THROTTLING_QUOTA_EXCEEDED;
            let throttle_ms = i32::try_from(inst.push_interval.as_millis()).unwrap_or(i32::MAX);
            return PushDecision::Reject {
                error_code: crate::codes::THROTTLING_QUOTA_EXCEEDED,
                throttle_ms,
            };
        }

        // 5. Unsupported compression codec → UNSUPPORTED_COMPRESSION_TYPE.
        //    Do NOT update last_push on this path.
        if !compression_supported {
            return PushDecision::Reject {
                error_code: crate::codes::UNSUPPORTED_COMPRESSION_TYPE,
                throttle_ms: 0,
            };
        }

        // 6. Payload oversize → TELEMETRY_TOO_LARGE.
        //    Do NOT update last_push on this path.
        let max_payload_len = self.telemetry_max.bytes_usize();
        if payload_len > max_payload_len {
            return PushDecision::Reject {
                error_code: crate::codes::TELEMETRY_TOO_LARGE,
                throttle_ms: 0,
            };
        }

        // 7. Success: update state and return accepted metrics.
        inst.last_push = Some(now);
        inst.last_error = crate::codes::NONE;
        let metrics = inst.metrics.clone();
        if terminating {
            inst.terminating = true;
        }
        PushDecision::Accept { metrics }
    }

    /// Drop instances idle beyond `max(interval * factor, floor)`.
    pub(crate) fn evict_stale(&self, factor: u32, floor: Duration) {
        let now = Instant::now();
        let mut guard = self
            .instances
            .lock()
            .expect("client-metrics mutex poisoned");
        guard.retain(|_, inst| {
            if inst.terminating {
                return false;
            }
            let ttl = (inst.push_interval * factor).max(floor);
            let last = inst.last_push.unwrap_or(inst.last_get);
            now.duration_since(last) < ttl
        });
    }
}

pub(crate) fn compute_subscription(
    image: &MetadataImage,
    attrs: &ClientAttributes,
    default_interval_ms: i32,
) -> ComputedSubscription {
    let mut matched_metrics: Vec<String> = Vec::new();
    let mut min_interval: Option<i32> = None;
    let mut any_star = false;

    for (_name, configs) in image.client_metrics_subscriptions() {
        let rules = match configs.get(config::KEY_MATCH) {
            Some(v) => match config::parse_match_rules(v) {
                Ok(r) => r,
                Err(_) => continue,
            },
            None => Vec::new(),
        };
        if !rules.iter().all(|r| selector_matches(r, attrs)) {
            continue;
        }
        let metrics = configs
            .get(config::KEY_METRICS)
            .map_or_else(Vec::new, |v| config::parse_metrics(v));
        if metrics.is_empty() {
            continue;
        }
        if metrics.iter().any(|m| m == ALL_METRICS) {
            any_star = true;
        }
        for m in metrics {
            if !matched_metrics.contains(&m) {
                matched_metrics.push(m);
            }
        }
        let interval = config::effective_interval_ms(configs, default_interval_ms);
        min_interval = Some(min_interval.map_or(interval, |cur| cur.min(interval)));
    }

    let metrics = if any_star {
        vec![ALL_METRICS.to_string()]
    } else {
        matched_metrics
    };
    ComputedSubscription {
        metrics,
        push_interval_ms: min_interval.unwrap_or(default_interval_ms),
    }
}

fn selector_matches(rule: &config::MatchRule, attrs: &ClientAttributes) -> bool {
    use config::MatchSelector::{
        Id, InstanceId, SoftwareName, SoftwareVersion, SourceAddress, SourcePort,
    };
    let target: std::borrow::Cow<'_, str> = match rule.selector {
        InstanceId => attrs.client_instance_id.to_string().into(),
        Id => (&attrs.client_id).into(),
        SoftwareName => (&attrs.software_name).into(),
        SoftwareVersion => (&attrs.software_version).into(),
        SourceAddress => (&attrs.source_address).into(),
        SourcePort => attrs.source_port.to_string().into(),
    };
    rule.pattern
        .find(&target)
        .is_some_and(|m| m.start() == 0 && m.end() == target.len())
}

/// Stable, change-sensitive subscription id. CRC32C over a canonical
/// (sorted) rendering of the metric set + push interval, XOR-ed with the
/// instance-id hash. Self-consistent across re-fetch; not byte-identical to
/// the JVM broker (which is not required).
pub(crate) fn subscription_id(sub: &ComputedSubscription, client_instance_id: Uuid) -> i32 {
    let mut sorted = sub.metrics.clone();
    sorted.sort();
    let rendered = format!("[{}]{}", sorted.join(", "), sub.push_interval_ms);
    // CRC32C output is u32; reinterpreting as i32 is intentional — the value
    // is used only for equality checks, not arithmetic.
    let crc = crc32c::crc32c(rendered.as_bytes()).cast_signed();
    crc ^ uuid_hashcode(client_instance_id)
}

/// Reproduces `java.util.UUID.hashCode()` for parity of shape.
fn uuid_hashcode(id: Uuid) -> i32 {
    let bytes = id.as_bytes();
    let msb = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let lsb = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let hilo = msb ^ lsb;
    let hilo_bytes = hilo.to_be_bytes();
    let high = i32::from_be_bytes(hilo_bytes[..4].try_into().expect("four-byte high half"));
    let low = i32::from_be_bytes(hilo_bytes[4..].try_into().expect("four-byte low half"));
    high ^ low
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::{assert, check};
    use crabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord};
    use uuid::Uuid;

    use super::*;

    fn img_with(name: &str, kvs: &[(&str, &str)]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        let mut cfgs = BTreeMap::new();
        for (k, v) in kvs {
            cfgs.insert((*k).to_string(), (*v).to_string());
        }
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: name.into(),
                configs: cfgs,
            },
        ));
        img
    }

    fn attrs() -> ClientAttributes {
        ClientAttributes {
            client_instance_id: Uuid::from_u128(1),
            client_id: "svc-1".into(),
            software_name: "apache-kafka-java".into(),
            software_version: "3.9.0".into(),
            source_address: "10.0.0.5".into(),
            source_port: 5556,
        }
    }

    #[test]
    fn no_subscription_means_no_metrics() {
        let img = MetadataImage::new(Uuid::nil());
        let m = compute_subscription(&img, &attrs(), 12_345);
        assert!(m.metrics.is_empty());
        check!(m.push_interval_ms == 12_345);
    }

    #[test]
    fn match_all_empty_match_applies() {
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let m = compute_subscription(&img, &attrs(), 300_000);
        check!(m.metrics == vec!["*".to_string()]);
        check!(m.push_interval_ms == 60_000);
    }

    #[test]
    fn selector_filters_clients() {
        let img = img_with(
            "java-only",
            &[
                ("metrics", "a."),
                ("match", "client_software_name=apache-kafka-java"),
            ],
        );
        let m = compute_subscription(&img, &attrs(), 300_000);
        check!(m.metrics == vec!["a.".to_string()]);

        let img2 = img_with(
            "py-only",
            &[
                ("metrics", "a."),
                ("match", "client_software_name=kafka-python"),
            ],
        );
        let m2 = compute_subscription(&img2, &attrs(), 300_000);
        assert!(
            m2.metrics.is_empty(),
            "java client must not match python selector"
        );
    }

    #[test]
    fn min_interval_and_metric_union_across_subs() {
        let mut img = img_with("s1", &[("metrics", "a."), ("interval.ms", "60000")]);
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "s2".into(),
                configs: {
                    let mut c = BTreeMap::new();
                    c.insert("metrics".into(), "b.".into());
                    c.insert("interval.ms".into(), "30000".into());
                    c
                },
            },
        ));
        let m = compute_subscription(&img, &attrs(), 300_000);
        let mut got = m.metrics.clone();
        got.sort();
        check!(got == vec!["a.".to_string(), "b.".to_string()]);
        check!(m.push_interval_ms == 30_000);
    }

    #[test]
    fn star_collapses_union() {
        let mut img = img_with("s1", &[("metrics", "a.")]);
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "s2".into(),
                configs: {
                    let mut c = BTreeMap::new();
                    c.insert("metrics".into(), "*".into());
                    c
                },
            },
        ));
        let m = compute_subscription(&img, &attrs(), 300_000);
        check!(m.metrics == vec!["*".to_string()]);
    }

    #[test]
    fn subscription_id_stable_and_change_sensitive() {
        let a = attrs();
        let s1 = ComputedSubscription {
            metrics: vec!["a.".into(), "b.".into()],
            push_interval_ms: 60_000,
        };
        let id1 = subscription_id(&s1, a.client_instance_id);
        let s1b = ComputedSubscription {
            metrics: vec!["b.".into(), "a.".into()],
            push_interval_ms: 60_000,
        };
        check!(id1 == subscription_id(&s1b, a.client_instance_id));
        let s2 = ComputedSubscription {
            metrics: vec!["a.".into(), "b.".into()],
            push_interval_ms: 30_000,
        };
        check!(id1 != subscription_id(&s2, a.client_instance_id));
        let s3 = ComputedSubscription {
            metrics: vec!["a.".into()],
            push_interval_ms: 60_000,
        };
        check!(id1 != subscription_id(&s3, a.client_instance_id));
    }

    #[test]
    fn push_throttle_ladder() {
        let m = ClientMetricsManager::new(crabka_units::kibibytes(1), crabka_units::minutes(5));
        let id = Uuid::from_u128(7);
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let attrs = ClientAttributes {
            client_instance_id: id,
            client_id: "c".into(),
            software_name: "n".into(),
            software_version: "v".into(),
            source_address: "1.2.3.4".into(),
            source_port: 1,
        };
        let assigned = m.assign(&img, &attrs);
        // First push after assign is allowed (compression_supported=true).
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, true, 10),
            PushDecision::Accept { .. }
        ));
        // Immediate second push (interval not elapsed, no new get) is throttled —
        // even if the payload would also be oversized; throttle wins per Kafka order.
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, true, 10),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::THROTTLING_QUOTA_EXCEEDED
        ));
        // Ordering assertion: oversized payload that is ALSO interval-not-elapsed
        // must return THROTTLING_QUOTA_EXCEEDED (throttle before size in ladder).
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id, false, true, 2048),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::THROTTLING_QUOTA_EXCEEDED
        ));
        // Wrong subscription id → UNKNOWN_SUBSCRIPTION_ID.
        assert!(matches!(
            m.authorize_push(id, assigned.subscription_id ^ 0x5555, false, true, 10),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::UNKNOWN_SUBSCRIPTION_ID
        ));
        // Unknown instance → INVALID_REQUEST.
        assert!(matches!(
            m.authorize_push(Uuid::from_u128(999), 0, false, true, 10),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::INVALID_REQUEST
        ));
        // Re-assign to get a fresh get-timestamp (acts as a new allowance).
        let assigned2 = m.assign(&img, &attrs);
        // Oversized payload with fresh get → TELEMETRY_TOO_LARGE.
        assert!(matches!(
            m.authorize_push(id, assigned2.subscription_id, false, true, 2048),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::TELEMETRY_TOO_LARGE
        ));
        // Unsupported compression with fresh get + small payload → UNSUPPORTED_COMPRESSION_TYPE.
        let assigned3 = m.assign(&img, &attrs);
        assert!(matches!(
            m.authorize_push(id, assigned3.subscription_id, false, false, 10),
            PushDecision::Reject { error_code, .. } if error_code == crate::codes::UNSUPPORTED_COMPRESSION_TYPE
        ));
    }
}
