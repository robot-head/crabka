# Slice 16b: IP quotas + connection_creation_rate (KIP-612) — Design

**Status:** Approved 2026-05-15.

**Goal:** Add the `ip` entity type to `AlterClientQuotas`/`DescribeClientQuotas` and implement KIP-612 `connection_creation_rate` — enforced at TCP accept by delaying the per-connection spawn when the source IP's bucket is exhausted. IPv4-only (slice-13 ACL parity).

**Out of scope:**
- IPv6 entity names (slice 13 ACLs are IPv4 too)
- Connection rejection (vs delay) on sustained overage
- KIP-599 `controller_mutation_rate` — slice 16c
- Slice 16's known follow-ups (client_id through HandlerTable, DescribeUserScramCredentials)

---

## 1. Scope

### In

- `ip` entity type recognized by `AlterClientQuotas` (api_key 49) and `DescribeClientQuotas` (api_key 48)
- IPv4 entity names only; `entity_name=None` accepted as the IP default
- `connection_creation_rate` KIP-612 quota type, in connections-per-second (`f64`)
- Two-priority IP lookup (specific → default) via `lookup_ip_quota(image, peer_ipv4, quota_key)`
- Enforcement at TCP accept in `broker.rs::accept` loop:
  - Consume 1 token from the `connection_creation_rate` bucket for the peer's IPv4
  - On overage: compute `delay = 1 / rate` seconds, capped at 1 second; `tokio::time::sleep(delay).await` before spawning the per-connection handler
  - Connection is never rejected — only delayed (KIP-612 semantic)
- Per-IP buckets reuse `QuotaBuckets` cache from slice 16; no new cache type
- Refresh task picks up `(ip)` quota changes via the existing image-watcher
- JVM acceptance: `kafka-configs --entity-type ips --entity-name 127.0.0.1 --add-config 'connection_creation_rate=2.0'` round-trip

### Not in

- IPv6 entity names
- Byte-rate quotas on `ip` entity — accepted by the validator but not enforced (matches Kafka)
- Connection rejection / firewall-style hard cutoff
- Bucket TTL / eviction (inherits slice 16's unbounded-cache limitation)
- KIP-599 controller_mutation_rate (slice 16c)

---

## 2. Storage & IP lookup

### Reuse existing storage

No new metadata record. `ClientQuotaRecord` (slice 16 T1) already carries arbitrary `(entity, config_key, config_value)` tuples; `entity_type="ip"` + `config_key="connection_creation_rate"` slot in cleanly.

`MetadataImage::client_quotas` (slice 16 T1) already stores any canonicalized entity tuple. Specific: `EntityKey = vec![("ip", Some("127.0.0.1"))]`. Default: `vec![("ip", None)]`.

### `lookup_ip_quota`

KIP-612's IP lookup is independent of the user/client-id 8-priority. Just two priority levels (specific → default). New function in `crates/broker/src/quota/lookup.rs`:

```rust
#[must_use]
pub fn lookup_ip_quota(
    image: &MetadataImage,
    peer_ip: &std::net::Ipv4Addr,
    quota_key: &str,
) -> Option<f64> {
    lookup_ip_quota_with_key(image, peer_ip, quota_key).map(|(_, v)| v)
}

#[must_use]
pub fn lookup_ip_quota_with_key(
    image: &MetadataImage,
    peer_ip: &std::net::Ipv4Addr,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let candidates: [EntityKey; 2] = [
        vec![("ip".into(), Some(peer_ip.to_string()))],
        vec![("ip".into(), None)],
    ];
    for key in candidates {
        if let Some(configs) = image.client_quotas().get(&key) {
            if let Some(&v) = configs.get(quota_key) {
                return Some((key, v));
            }
        }
    }
    None
}
```

The user/client-id `lookup_quota` and the new `lookup_ip_quota` don't share candidates — the former checks `("user", *)` and `("client-id", *)`, the latter only `("ip", *)`. Document in both with a comment.

### Validator extension in `process_one_entry`

`crates/broker/src/handlers/alter_client_quotas.rs::process_one_entry` (slice 16 T5):

```rust
const SUPPORTED_ENTITY_TYPES: &[&str] = &["user", "client-id", "ip"];
const KNOWN_QUOTA_KEYS: &[&str] = &[
    "producer_byte_rate",
    "consumer_byte_rate",
    "request_percentage",
    "connection_creation_rate",   // KIP-612 — only enforced when on ip entity
];
```

When `entity_type == "ip"` and `entity_name == Some(s)`, validate `s.parse::<Ipv4Addr>()`; reject with `INVALID_REQUEST` on parse failure. `entity_name == None` (default IP) accepted.

**No entity/key cross-validation** — `connection_creation_rate` on a `(user)` entity is accepted but never enforced. Symmetric: byte rates on `(ip)` are accepted but never enforced. Matches Kafka's permissive validator.

### `DescribeClientQuotas` filter extension

`entity_matches_filter` (slice 16 T6) is generic over entity_type strings. `ip`-filtered describe requests work without changes.

---

## 3. TCP accept enforcement

### Hook in `broker.rs::accept` loop

Insert between `listener.accept()` returning `(stream, peer)` and the per-connection `tokio::spawn`. Around `broker.rs:1206`:

```rust
match accept {
    Ok((stream, peer)) => {
        // KIP-612 connection_creation_rate enforcement (slice 16b).
        // IPv4 only — slice 13 ACL parity. IPv6 peers skip the quota check.
        if let std::net::IpAddr::V4(peer_ipv4) = peer.ip() {
            let image = listener_controller.current_image();
            if let Some((entity_key, rate)) = crate::quota::lookup_ip_quota_with_key(
                &image, &peer_ipv4, "connection_creation_rate",
            ) {
                if rate > 0.0 {
                    let bucket = listener_quota_buckets.get_or_create(
                        "connection_creation_rate",
                        &entity_key,
                        rate.max(1.0) as u64,
                    );
                    let granted = bucket.try_consume(1);
                    if granted == 0 {
                        let delay_secs = 1.0 / rate;
                        let delay = std::time::Duration::from_micros(
                            (delay_secs * 1_000_000.0) as u64,
                        ).min(std::time::Duration::from_secs(1));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        // ... existing per-connection spawn ...
    }
    Err(e) => { /* existing error path */ }
}
```

### Per-listener capture of Arcs

The accept loop runs inside a per-listener `tokio::spawn` from `Broker::start`. Clone `quota_buckets` + `controller` into the spawn closure before the loop:

```rust
let listener_quota_buckets = quota_buckets.clone();
let listener_controller = controller.clone();
tokio::spawn(async move {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accept = listener.accept() => { /* hook + spawn */ }
        }
    }
});
```

### Two semantic subtleties

1. **Rate stored as `f64`, bucket internally `u64`.** Floor fractional rates via `rate.max(1.0) as u64` to avoid the "0 tokens/sec = always blocked" footgun. Sub-1.0 rates aren't a real production use case. Documented in STATUS.

2. **Per-listener vs broker-wide bucket.** Bucket key is `(quota_key, entity_key)` where `entity_key = [("ip", Some(peer_ipv4))]`. A single IP connecting to multiple listeners (PLAINTEXT + SSL + SASL_PLAINTEXT) shares one bucket — broker-wide quota, matches Kafka.

### Refresh task — no changes

Slice 16's `quota::refresh::run` already iterates every `(quota_key, entity_key)` in `QuotaBuckets`. When a `(ip)` connection_creation_rate changes via `AlterClientQuotas`, the next image apply updates the bucket. Zero new code.

### Bucket lifetime

Lazy allocation on first connection from each IP; sticks for the broker's lifetime. Slice 16 explicitly declined TTL/eviction. Real-world clusters that see millions of distinct client IPs would need a strategy; deferred.

---

## 4. Testing

### Unit tests (~6 total)

**`crates/broker/src/quota/lookup.rs` — `lookup_ip_quota` (4 tests):**
- `ip_specific_match` — `(ip=127.0.0.1) connection_creation_rate=1.0` matches lookup for 127.0.0.1.
- `ip_default_fallback` — `(ip=<default>) connection_creation_rate=2.0` matches for any IP when no specific.
- `ip_specific_wins_over_default` — both configured; specific wins.
- `no_match_returns_none` — nothing configured; returns None.

**`crates/broker/src/handlers/alter_client_quotas.rs` — validator (2 tests):**
- `ip_entity_with_valid_ipv4_accepted` — entity_name="10.0.0.1" + connection_creation_rate=1.0 succeeds.
- `ip_entity_with_invalid_address_rejected` — entity_name="not-an-ip" returns `INVALID_REQUEST`.

### Broker integration tests (`crates/broker/tests/ip_quotas.rs`, 3 tests)

1. **`ip_quota_alter_then_describe_round_trip`** — single-broker SASL/PLAIN; alter `(ip=127.0.0.1) connection_creation_rate=2.0` via wire; describe with `ip any-name` filter; assert returned value matches.

2. **`connection_creation_rate_throttles_accept`** — single-broker PLAINTEXT; set `(ip=127.0.0.1) connection_creation_rate=1.0` via wire; open 5 connections in rapid succession; measure wall time. Expected ~4 seconds total (1-second burst, then ~1s per remaining connection, capped at 1s per slot). Tolerance: ≥3 seconds proves the throttle fires.

3. **`unthrottled_ip_unaffected`** — same setup, no quota configured; open 5 connections; assert wall time ≤ 500ms.

### JVM acceptance (1 new test in `jvm_acceptance.rs`)

`jvm_kafka_configs_alter_ip_quota_end_to_end` — `#[ignore]`-tagged, WSL:

1. 3-broker SASL/PLAINTEXT cluster (reuse slice 14 helper with extra users).
2. `kafka-configs --alter --entity-type ips --entity-name 127.0.0.1 --add-config 'connection_creation_rate=2.0'` → exit 0 (or stdout check per slice 16 T13 workaround).
3. `kafka-configs --describe --entity-type ips --entity-name 127.0.0.1` → stdout contains `connection_creation_rate=2.0`.
4. `kafka-configs --alter --entity-type ips --entity-name 127.0.0.1 --delete-config 'connection_creation_rate'` → exit 0.
5. Confirm quota cleared from image (poll).

**Wall-time enforcement not in the JVM test** — `kafka-console-producer` is a single long-lived connection; doesn't exercise `connection_creation_rate`. The Rust integration test 2 proves enforcement; the JVM test proves the wire round-trip.

### Slice 16 interaction note

The 8-priority `lookup_quota` (slice 16 T2) and the 2-priority `lookup_ip_quota` (slice 16b) are disjoint — `lookup_quota` checks `("user", *)` and `("client-id", *)` candidates; `lookup_ip_quota` only checks `("ip", *)`. Add code comments in both.
