# QoS-Tiered Producer Quotas — Design

**Status:** Approved 2026-06-17.

**Goal:** Add per-topic `qos.tier` routing so Produce byte-rate quota enforcement uses separate runtime buckets per `(user, client-id, qos_tier)`.

**Scope:**
- Accept a topic config key named `qos.tier`.
- Resolve missing `qos.tier` to the literal tier `default`.
- Split a Produce request's record bytes by resolved topic tier.
- Consume `producer_byte_rate` quota independently for each tier.
- Preserve Kafka entity precedence for selecting the configured quota rate.
- Preserve the existing KIP-219 response behavior: one response-level `throttle_time_ms`, one channel mute, and `max(data_delay, request_delay)`.

**Out of scope:**
- New wire protocol fields.
- New persisted metadata record types.
- Tier-specific quota configuration keys; tiers partition runtime buckets while reusing the selected Kafka quota rate.
- Tiered request-percentage accounting. Request quota remains per `(user, client-id)` because it is not topic-byte traffic.

## Architecture

Topic configs already live in `MetadataImage::topic_config`, so `qos.tier` can use the existing topic-config persistence and propagation path. `crates/broker/src/config_keys.rs` will whitelist the key, validate it, document it, and provide a resolver that returns `default` when unset or malformed.

The quota subsystem already selects Kafka quota entities via `lookup_quota_with_key` and caches `TokenBucket`s in `QuotaBuckets`. The change is to extend the bucket identity used for producer-byte enforcement by adding the QoS tier to the runtime entity key after the quota entity has been selected. The underlying quota value still comes from the existing `producer_byte_rate` client quota.

The Produce handler already measures payload bytes before consuming `topic_data`. It will replace the single total-byte sum with a per-tier byte map, then consume each tier bucket and use the maximum tier delay as the data delay. Mixed-tier Produce requests are accepted and charged to multiple independent buckets.

## Error Handling

`qos.tier` validation rejects empty values and values outside a conservative ASCII identifier set. That keeps bucket labels stable and avoids accidental whitespace or punctuation creating hard-to-debug buckets.

If a corrupt metadata image somehow contains an invalid tier, enforcement resolves it to `default`, matching the existing permissive handling used by other produce-time topic config reads.

## Testing

Unit tests cover:
- `qos.tier` validation acceptance and rejection.
- `resolve_qos_tier` default fallback.
- Producer quota bucket separation by tier.
- Mixed-tier byte accounting uses the max delay across independent buckets.

Integration coverage can reuse the existing SASL Produce quota helpers if needed, but unit coverage is sufficient for the core bucket identity and accounting behavior because this change is local to config validation, quota helper logic, and Produce byte aggregation.
