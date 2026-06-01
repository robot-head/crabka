# KIP-714 Client Metrics Push — Design

**Date:** 2026-05-30
**Status:** Approved (pending spec review)
**KIPs:** [KIP-714](https://cwiki.apache.org/confluence/display/KAFKA/KIP-714:+Client+metrics+and+observability), [KIP-1000](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1000:+List+Client+Metrics+Configuration+Resources) (listing)

## 1. Problem & goal

KIP-714 lets Kafka clients push their own metrics to a broker, which the broker
forwards to an observability backend. The broker advertises two APIs:

- **`GetTelemetrySubscriptions` (key 71)** — a client asks "what metrics, if any,
  do you want from me, and how often?". The broker assigns the client a stable
  instance id and returns the matched subscription.
- **`PushTelemetry` (key 72)** — the client periodically ships an OTLP
  `MetricsData` protobuf payload; the broker ingests it.

Operators define *subscriptions* (which clients, which metric prefixes, how
often) as dynamic cluster configs on a `CLIENT_METRICS` config resource,
managed via `kafka-client-metrics.sh` / `kafka-configs.sh` → standard
`IncrementalAlterConfigs` / `DescribeConfigs` / `ListConfigResources` RPCs.

**Today Crabka stubs both APIs as deliberate no-ops** (`get_telemetry_subscriptions.rs`
returns an empty subscription; `push_telemetry.rs` drops the payload). This
design replaces those stubs with a full implementation: config-driven
subscriptions persisted through the existing metadata path, a per-broker client
instance registry, OTLP decode, and a dual sink (Prometheus re-export + OTLP
forward).

### Compatibility target

**Strimzi is not a constraint.** Strimzi has no CRD or `Kafka` CR field for
KIP-714 subscriptions, and its similarly-named "ClientMetricsReporter" is an
unrelated Prometheus-scrape plugin. Operators on Strimzi manage subscriptions
out-of-band via the Apache CLI / `AdminClient`. **The compatibility surface that
matters is therefore the Apache Kafka admin tooling** (`kafka-client-metrics.sh`,
`kafka-configs.sh --entity-type client-metrics`) and the JVM client handshake.
All wire shapes, config-resource semantics, and error codes below are matched to
Apache Kafka `trunk` (4.x line).

### Non-goals (YAGNI)

- Cross-broker telemetry state sharing — KIP-714 is per-broker by design (a
  client pins all telemetry to one broker). Instance state stays in-memory.
- A pluggable `ClientTelemetry`/`metric.reporters` loader. Crabka ships a single
  built-in receiver (the two sinks). It is always present, so APIs 71/72 are
  always advertised (matching Kafka's "receiver configured → advertise" rule
  trivially).
- Client-side metric *emission* (Crabka is a broker, not a client).
- Historical metric storage/query beyond the live `/metrics` scrape snapshot and
  the OTLP forward.

## 2. Architecture overview

```
operator → IncrementalAlterConfigs(CLIENT_METRICS=16) ─→ V1ClientMetricsConfig record ─→ raft ─→ MetadataImage
                                                                                                    │
client → GetTelemetrySubscriptions(71) → handler → ClientMetricsManager ──reads subs from──────────┘
                                                          │ match client attrs, compute subscription_id
                                                          │ register/refresh ClientInstance
client → PushTelemetry(72) ── OTLP bytes ──→ handler → ClientMetricsManager
                                                          ├─ validate (sub id / throttle / size / codec)
                                                          ├─ decompress + decode OTLP MetricsData
                                                          ├─ Prometheus sink: feed dynamic Collector (/metrics)
                                                          └─ OTLP sink: forward ExportMetricsServiceRequest
```

New module `crates/broker/src/client_metrics/` houses a per-broker
**`ClientMetricsManager`** stored on `Broker`. Subscriptions live in the
replicated `MetadataImage`; live client-instance state and the sinks are
broker-local.

## 3. Subscription config storage (extend existing config mechanism)

Subscriptions are dynamic configs on `CLIENT_METRICS(16)` resources, persisted
exactly like topic/broker configs.

### 3.1 Metadata record

`crates/metadata/src/records.rs`:

```rust
pub struct ClientMetricsConfigRecord {
    pub name: String,                              // subscription name (resource name)
    pub configs: BTreeMap<String, String>,         // full override set; empty ⇒ delete
}
// new MetadataRecord variant:
//   V1ClientMetricsConfig(ClientMetricsConfigRecord)
```

Replace-on-empty semantics, mirroring `TopicConfigRecord`
(`image.rs` apply: empty map → `remove`, else `insert`).

### 3.2 Metadata image

`crates/metadata/src/image.rs`:

- field `client_metrics_configs: HashMap<String, BTreeMap<String, String>>`
- accessor `client_metrics_config(&self, name: &str) -> Option<&BTreeMap<String,String>>`
- accessor `client_metrics_subscriptions(&self) -> impl Iterator<Item=(&String, &BTreeMap<String,String>)>`
- `apply` arm for `V1ClientMetricsConfig`
- `to_records` snapshot emission (one record per subscription)

### 3.3 Config keys & validation

New module `crates/broker/src/client_metrics/config.rs` (mirrors the role of
`config_keys` for topics). The only three keys Kafka recognizes:

| Key | Type | Default | Bounds / rules |
|---|---|---|---|
| `metrics` | list (CSV) | `[]` (no metrics) | metric-name prefixes; the single element `"*"` = all metrics |
| `interval.ms` | int | `300000` | **100 ≤ v ≤ 3_600_000** |
| `match` | list | `[]` (match all) | each entry `key=regex`; `key` ∈ the six selectors below; regex must compile |

Allowed `match` selector keys (exact strings):
`client_instance_id`, `client_id`, `client_software_name`,
`client_software_version`, `client_source_address`, `client_source_port`.

Unknown keys, out-of-range `interval.ms`, bad regex, or a `match` entry with an
unknown selector → `INVALID_CONFIG` with a descriptive message.

### 3.4 IncrementalAlterConfigs (key 44) — CLIENT_METRICS branch

`crates/broker/src/handlers/incremental_alter_configs.rs`:

- New branch for `resource_type == 16`.
- **Authz:** `ALTER_CONFIGS` on `Cluster("kafka-cluster")` (same gate as BROKER).
- `resource_name` = subscription name (non-empty; empty → `INVALID_REQUEST`).
- Merge ops into the current override map:
  - `OP_SET (0)`: validate via §3.3, insert.
  - `OP_DELETE (1)`: remove the key. **`interval.ms` delete reverts to default
    300000** at read time, not a stored null (KAFKA-18984) — i.e. deletion just
    drops the override and the effective value falls back to the default.
  - `OP_APPEND/SUBTRACT (2/3)`: `INVALID_CONFIG` (unsupported, matching Kafka).
- `validate_only` short-circuits before submit.
- Persist `MetadataRecord::V1ClientMetricsConfig` via `controller.submit_change`
  (NotLeader → `NOT_CONTROLLER`).

### 3.5 DescribeConfigs (key 32) — CLIENT_METRICS branch

`crates/broker/src/handlers/describe_configs.rs`:

- New branch for `resource_type == 16`.
- **Authz:** `DESCRIBE_CONFIGS` on `Cluster`.
- Returns **all three keys including defaults + synonyms** (KAFKA-17516): an
  unset `interval.ms` is reported as `300000`. Set values use config-source
  **byte `7` = `CLIENT_METRICS_CONFIG`**; defaulted values use the
  default-config source. (Without defaults/synonyms, `kafka-configs.sh
  --describe --all` shows blanks.)
- Respect the request's `configuration_keys` filter.

### 3.6 ListConfigResources (key 74)

`crates/broker/src/handlers/list_config_resources.rs`:

- Replace the current empty CLIENT_METRICS stub: enumerate configured
  subscription names from `image.client_metrics_subscriptions()`, emit a
  `ConfigResource{ resource_type: 16, resource_name: <name> }` per subscription.
  (Drives `kafka-client-metrics.sh --list`.)

## 4. ClientMetricsManager (per-broker)

`crates/broker/src/client_metrics/mod.rs`. Held as `Arc<ClientMetricsManager>`
on `Broker`. Owns:

- `instances: Mutex<HashMap<Uuid, ClientInstance>>` — live client-instance state.
- the Prometheus `ClientMetricsCollector` handle (§6.1).
- the OTLP forwarder handle (§6.2).
- a clock + config (`telemetry_max_bytes`, eviction policy).

Reads subscriptions from `broker.metadata.current_image()` on each
`GetTelemetrySubscriptions` (no caching needed — image swaps are cheap `Arc`s).

### 4.1 ClientInstance

```rust
struct ClientInstance {
    client_instance_id: Uuid,
    // connection attributes captured at GetTelemetrySubscriptions time:
    client_id: String,
    client_software_name: String,
    client_software_version: String,
    source_address: IpAddr,
    source_port: u16,
    // negotiated subscription state:
    subscription_id: i32,
    push_interval_ms: i32,
    subscribed_metrics: Vec<String>,   // prefixes, or ["*"]
    // throttling / lifecycle:
    last_get_timestamp: Instant,
    last_push_timestamp: Option<Instant>,
    terminating: bool,
}
```

Eviction: a background sweep (or lazy-on-access) drops instances idle beyond
`max(push_interval_ms * EVICTION_FACTOR, MIN_EVICTION)`. Terminated instances are
dropped immediately after their final push.

### 4.2 Subscription matching

For a connecting client, evaluate every configured subscription:

1. A subscription matches if **every** `match` selector regex fully matches the
   corresponding client attribute (empty `match` ⇒ matches all clients).
2. Union the `metrics` prefix lists of all matched subscriptions. If any matched
   subscription contains `"*"`, collapse the whole set to the single `["*"]`.
3. `push_interval_ms` = **min** `interval.ms` across matched subscriptions;
   `300000` if none match.
4. `requested_metrics` returned to the client = the computed prefix set
   (`[]` if no subscription matched → client does not push; `["*"]` for all).

### 4.3 subscription_id (exact Kafka algorithm)

```
metrics_bytes = utf8( set_to_string(subscribed_metrics) + decimal(push_interval_ms) )
crc           = Crc32C(metrics_bytes)                      // CRC32C, not CRC32
subscription_id = (crc as i32) XOR uuid_hashcode(client_instance_id)
```

- `set_to_string` must reproduce Java `Set<String>.toString()` ordering/format
  used by Kafka so the value is internally consistent across a re-fetch (the
  client compares it to detect subscription changes). Exact byte-equality with
  the JVM broker is **not** required (the client only checks self-consistency),
  but the algorithm shape is preserved.
- `uuid_hashcode` reproduces `java.util.UUID.hashCode()` (xor of the two longs,
  folded). Documented inline.

### 4.4 GetTelemetrySubscriptions response

| Field | Value |
|---|---|
| `client_instance_id` | fresh v4 UUID if request id == nil, else `nil` (echo-on-assign only) |
| `subscription_id` | §4.3 |
| `accepted_compression_types` | **hardcoded `[4, 3, 1, 2]`** = ZSTD, LZ4, GZIP, SNAPPY (NONE not advertised) |
| `push_interval_ms` | §4.2 |
| `telemetry_max_bytes` | broker config `telemetry.max.bytes`, default `1048576` |
| `delta_temporality` | **hardcoded `true`** |
| `requested_metrics` | §4.2 |
| `error_code` | `NONE` (or throttle, §4.6) |

A fresh instance is registered (or the existing one refreshed) and
`last_get_timestamp` set. The client id assignment never errors on an unknown
incoming UUID — an unrecognized non-nil id is simply adopted.

### 4.5 Connection-attribute plumbing

The two handlers currently take `(&Broker, version, correlation_id, req_bytes)`.
They will be extended to receive a context carrying `client_id`, peer
`SocketAddr`, and the negotiated `client_software_name`/`client_software_version`
(already captured per-connection for the `client_software_versions` Prometheus
metric — thread the same values through the dispatch loop). A small
`TelemetryContext<'a>` struct is added rather than overloading the ACL
`RequestContext` (telemetry RPCs are unauthenticated — see §7).

### 4.6 Throttling state machine

Matches Kafka's `ClientMetricsInstance` logic:

- **GetTelemetrySubscriptions** is throttled if it arrives within the interval of
  the previous get, **unless** the last response to this instance was
  `UNKNOWN_SUBSCRIPTION_ID` or `UNSUPPORTED_COMPRESSION_TYPE` (the client is
  expected to immediately re-fetch in those cases). Throttled → `throttle_time_ms`
  set, `THROTTLING_QUOTA_EXCEEDED`.
- **PushTelemetry** is accepted if either: (a) `last_get_timestamp >
  last_push_timestamp` (the first push after a fresh subscription is always
  allowed — this is how Kafka tolerates the client's 0.5×–1.5× jitter, with no
  broker-side jitter of its own), or (b) `now - last_push_timestamp >=
  push_interval_ms`. Otherwise → `THROTTLING_QUOTA_EXCEEDED`.

## 5. PushTelemetry (key 72) handler

`crates/broker/src/handlers/push_telemetry.rs`, validation ladder (first failure
wins; each returns the response with that `error_code` and `throttle_time_ms`):

1. **Instance unknown** (id not in registry, e.g. evicted/expired) →
   `INVALID_REQUEST`. (Kafka has no dedicated code; unknown instance falls
   through to the catch-all. The client re-fetches subscriptions.)
2. **Already terminated** (a request after a prior `terminating=true`) →
   `INVALID_REQUEST`.
3. **`subscription_id` != the instance's current id** → `UNKNOWN_SUBSCRIPTION_ID`.
4. **Throttle** per §4.6 (skipped when `terminating=true` — a terminating push
   bypasses the interval once) → `THROTTLING_QUOTA_EXCEEDED`.
5. **`compression_type` not in {none, gzip, snappy, lz4, zstd}** →
   `UNSUPPORTED_COMPRESSION_TYPE`.
6. **`metrics.len() > telemetry_max_bytes`** → `TELEMETRY_TOO_LARGE`.

On success:

1. Decompress `metrics` per `compression_type` (reuse the existing broker
   compression codecs used for produce/fetch records).
2. Decode the OTLP `MetricsData` protobuf (§6).
3. Fan out to both sinks (§6.1, §6.2). Sink errors are logged and counted, never
   surfaced to the client (a push that decoded fine is acked `NONE`).
4. Update `last_push_timestamp`; if `terminating`, mark + evict the instance.
5. Respond `error_code = NONE`.

### New error constants (`crates/broker/src/codes.rs`)

```rust
pub const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;  // add if absent
pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;     // add if absent
pub const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
pub const TELEMETRY_TOO_LARGE: i16 = 118;
```

(Values reconfirmed against `org.apache.kafka.common.protocol.Errors`.)

## 6. OTLP decode + dual sink

### Dependencies

Add the `opentelemetry-proto` crate (version-aligned with the existing
`opentelemetry 0.32` stack) with its metrics + prost-message features. This
provides pre-generated prost types for `MetricsData` /
`ExportMetricsServiceRequest` — **no `.proto` vendoring or `prost-build`**. `prost`
0.14 is already a workspace dependency.

`crates/broker/src/client_metrics/otlp.rs`: `decode_metrics(&[u8]) ->
Result<ResourceMetrics-set, DecodeError>`.

### 6.1 Prometheus sink — dynamic `Collector`

`crates/broker/src/client_metrics/prometheus_sink.rs`. Client metric *names* are
dynamic and supplied by the client, which doesn't fit `prometheus-client`'s
statically-registered `Family` model. Solution: a custom **`Collector`**
(implementing `prometheus_client::collector::Collector`) registered once into the
existing `SharedRegistry`. It holds an `Arc<Mutex<Snapshot>>` of the most recent
decoded data points (keyed by metric name + attribute set), with per-entry
staleness expiry. The push handler updates the snapshot; the collector renders
the live snapshot at scrape time as `crabka_client_*` series labeled with
`client_instance_id` / `client_id` plus the OTLP datapoint attributes. OTLP
Sum/Gauge → counter/gauge; Histogram → a summary-style rendering (buckets). This
keeps `/metrics` a single endpoint.

### 6.2 OTLP forward sink

`crates/broker/src/client_metrics/otlp_sink.rs`. Wrap the decoded
`ResourceMetrics` into an `ExportMetricsServiceRequest`, inject
`client_instance_id` and the connection principal as resource attributes, and
send to the OTLP endpoint already used for traces (`CRABKA_OTLP_ENDPOINT`,
reusing the configured gRPC vs HTTP/protobuf transport). Implemented as a
bounded async queue + worker so a slow collector never blocks the request path;
overflow is dropped + counted. No-op when OTLP is not configured.

Both sinks are independently disable-able; when neither is configured the
receiver still validates/acks (matching "ingest succeeds, nothing re-exported").

## 7. Authorization

- **`GetTelemetrySubscriptions` / `PushTelemetry`: no ACL gate** (Kafka serves
  these unauthenticated). They still run after the connection's normal auth
  handshake but require no specific permission.
- **`IncrementalAlterConfigs` for CLIENT_METRICS:** `ALTER_CONFIGS` on `CLUSTER`,
  controller-forwarded (like topic/broker configs).
- **`DescribeConfigs` / `ListConfigResources` for CLIENT_METRICS:**
  `DESCRIBE_CONFIGS` on `CLUSTER`.

## 8. Broker config

- `telemetry.max.bytes` (default `1048576`) — wired as a static/dynamic broker
  config and returned in `GetTelemetrySubscriptionsResponse.telemetry_max_bytes`.
- Sink targets reuse existing config: the OTLP endpoint env vars already parsed
  by `telemetry.rs`; the Prometheus sink uses the existing `/metrics` registry.

## 9. Testing strategy

**Unit**
- `match`-rule evaluation: each selector, multi-selector AND, empty=match-all,
  bad regex rejected.
- metrics-prefix union + `"*"` collapse.
- `subscription_id` determinism and change-detection (same inputs → same id;
  changed metrics/interval → different id), CRC32C + uuid-hashcode correctness.
- config-key validation: `interval.ms` bounds, unknown key, `match` selector
  whitelist, APPEND/SUBTRACT rejection, `interval.ms` delete → default.
- PushTelemetry error ladder: each rung in isolation + ordering.
- throttle state machine: first-push-after-get allowed; too-soon throttled;
  terminating bypass once then INVALID_REQUEST.
- OTLP→Prometheus translation (Sum/Gauge/Histogram) and staleness eviction.

**Integration**
- Config round-trip: `IncrementalAlterConfigs` → `DescribeConfigs` (incl.
  defaults/synonyms + source byte 7) → `ListConfigResources` shows the name.
- Full handshake: nil id → assigned id → matched subscription → push → scrape
  `/metrics` shows `crabka_client_*` series.
- Error paths over the wire: unknown sub id, too-large, throttle, bad codec.
- Byte-exactness of both response shapes vs the schema, and a behavioral check
  against the latest cp-kafka image for `DescribeConfigs`/`ListConfigResources`
  CLIENT_METRICS output (per CLAUDE.md: verify undocumented details empirically).

## 10. Touched files (for plan batching)

Non-overlapping clusters suitable for parallel implementation:

- **Metadata layer:** `crates/metadata/src/records.rs`, `image.rs`.
- **Wire codes:** `crates/broker/src/codes.rs`.
- **Config handlers:** `incremental_alter_configs.rs`, `describe_configs.rs`,
  `list_config_resources.rs` (+ new `client_metrics/config.rs`).
- **Manager + sinks (new module):** `crates/broker/src/client_metrics/{mod,otlp,
  prometheus_sink,otlp_sink}.rs`.
- **Handlers + plumbing:** `get_telemetry_subscriptions.rs`,
  `push_telemetry.rs`, dispatch context threading, `Broker` wiring.
- **Deps:** workspace + broker `Cargo.toml` (`opentelemetry-proto`).

(The metadata, codes, and config-handler clusters touch disjoint files and can
run concurrently; the manager/sinks land before the handler rewrite that depends
on them.)
