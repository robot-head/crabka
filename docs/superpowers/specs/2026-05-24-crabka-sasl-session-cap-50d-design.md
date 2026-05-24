# Slice 50d — Operator + Broker: SASL session-lifetime cap (KIP-368 ceiling)

Status: Draft
Date: 2026-05-24
Umbrella: `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`
Builds on: slice 49e (broker KIP-368 SASL re-auth) — `docs/superpowers/specs/2026-05-24-crabka-broker-sasl-reauth-49e-design.md`

## Goal

Surface Strimzi's `maxSecondsWithoutReauthentication` field on
`KafkaListenerAuthenticationOAuth` and bundle the prerequisite broker
config that lets the field do something. After this slice, an operator
can clamp OAUTHBEARER SASL sessions tighter than the token's natural
`exp`.

## Why this slice bundles both layers

The umbrella roadmap pitched slice 50d as a pure operator addition that
"threads down to broker TOML." But slice 49e shipped re-auth with
`session_lifetime_ms = max(exp - now, 0)` and no server-side cap —
there is no broker TOML key for the operator to thread into. So 50d
adds both ends in one PR rather than ship a CRD field that does
nothing.

## Semantic divergence from Strimzi (acknowledged)

Strimzi's `maxSecondsWithoutReauthentication`:
- **unset** → no re-auth (session lasts forever).
- **set to N** → enable re-auth, session = `min(token.exp, N)`.

Crabka's 50d:
- **unset** → use token `exp` (49e default; re-auth always on).
- **set to N** → use `min(token.exp - now, N)` (clamp tighter).

Crabka takes the "bounded by default" posture: OAUTHBEARER tokens have
an `exp`; bounded sessions are a security floor, not opt-in. The
operator field is shape-only Strimzi parity, not semantic parity.
Greenfield-acceptable: there are no users with existing
`maxSecondsWithoutReauthentication=null` configurations expecting
unbounded sessions.

## Scope

### In scope

- **Broker:** new optional `[oauthbearer].max_session_lifetime_seconds:
  u32` TOML key. When set, `handle_authenticate_oauthbearer` and its
  `Reauthenticating` arm clamp:
  `session_lifetime_ms = min(token_exp_ms - now_ms,
  cap_seconds * 1000)`. When unset, behavior is unchanged from 49e
  (session = token `exp`).
- **Broker:** `Authenticated.expires_at_ms` stores the CLAMPED value
  (not the raw token `exp`), so the dispatch loop's `select!` timer
  fires at the clamped time. Without this, the response would tell the
  client a shorter lifetime than the broker would actually enforce.
- **Operator:** new CRD field
  `maxSecondsWithoutReauthentication: Option<u32>` on
  `ListenerAuthenticationOAuth`. Strimzi-shape camelCase; minimum
  validation `minimum: 1`.
- **Operator reconciler:** thread the value through the existing
  canonical-config divergence machinery (so two OAuth listeners with
  divergent values get `ConflictingOAuthListenerConfig`). Render as
  `max_session_lifetime_seconds = N` in the broker TOML when set.
- **Tests:** broker unit + integration (cap below/above/equal to
  token exp; timer fires at cap; regression for unset cap). Operator
  CRD round-trip + schema regression + render + divergence + reconcile
  integration. E2E: extend the existing `kind-oauth` job's Kafka CR
  with the field at a safe value (300s, well above the producer Job
  runtime).

### Out of scope

- **Mechanism-agnostic `connections.max.reauth.ms`** (would force
  re-auth on PLAIN/SCRAM too). Not in the OAUTHBEARER parity umbrella.
- **Per-listener divergent caps.** Operator still enforces one
  canonical OAuth config per broker (slice 49b/50 stance). Different
  listeners with different caps = `ConflictingOAuthListenerConfig`.
- **Client-side scheduler.** librdkafka and Crabka's own Kafka client
  already handle the `session_lifetime_ms` field per KIP-368; the cap
  just makes that value smaller. Broker-only change on the broker
  side.
- **New e2e workflow job.** The existing `kind-oauth` job is extended
  with the field; no new `kind-oauth-cap` variant.
- **Server-side minimum check** ("cap too short, reject auth"). The
  CRD's `minimum: 1` is the only minimum gate; values lower than ~60
  seconds may be operationally annoying but are user policy.

## Wire / config / CRD shapes

### Broker TOML (new key under `[oauthbearer]`)

```toml
[oauthbearer]
# existing keys ...
max_session_lifetime_seconds = 300   # optional; absent = no cap
```

`FileOAuthBearerConfig` gains:

```rust
pub struct FileOAuthBearerConfig {
    // existing fields ...
    pub max_session_lifetime_seconds: Option<u32>,
}
```

`BrokerConfig` gains:

```rust
pub oauthbearer_max_session_lifetime_seconds: Option<u32>,
```

### Broker handler clamp

In both `handle_authenticate_oauthbearer`'s `Negotiating` success arm
and its `Reauthenticating` success arm:

```rust
let raw_session_ms = outcome.expires_at_ms.map_or(0, |e| (e - now_ms).max(0));
let session_ms = match max_session_lifetime_seconds {
    Some(cap) => raw_session_ms.min(i64::from(cap) * 1000),
    None => raw_session_ms,
};
let effective_expires_at_ms = now_ms + session_ms;
*auth = ConnectionAuth::Authenticated {
    principal: outcome.principal,
    mechanism: mech,
    expires_at_ms: Some(effective_expires_at_ms),
};
SaslAuthenticateResponse {
    error_code: 0,
    auth_bytes: bytes::Bytes::new(),
    session_lifetime_ms: session_ms,
    ..Default::default()
}
```

The cap is threaded as a new parameter to `handle_authenticate_oauthbearer`
(alongside `validator: &OAuthBearerValidator, now_ms: i64`). The call
site in `dispatch.rs` reads from `broker.config.oauthbearer_max_session_lifetime_seconds`.

### Operator CRD (`crates/operator/src/crd/listener.rs`)

```rust
pub struct ListenerAuthenticationOAuth {
    // existing fields (validIssuerUri, jwksEndpointUri,
    // introspectionEndpointUri, accessTokenIsJwt, clientId,
    // clientSecret, introspectionHttpTimeoutSeconds, ...) ...

    /// Maximum SASL session lifetime (seconds) before the broker
    /// forces re-authentication via KIP-368. Acts as a ceiling on
    /// top of the token's `exp` — the effective session is
    /// `min(token_exp - now, maxSecondsWithoutReauthentication)`.
    /// When unset (the default), sessions last until the token's
    /// natural `exp`.
    ///
    /// Strimzi-shape field. Minimum value 1 second (CRD-validated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_seconds_without_reauthentication: Option<u32>,
}
```

Hand-rolled schema fragment (alphabetical position):

```rust
"maxSecondsWithoutReauthentication": {
    "type": "integer",
    "format": "int32",
    "minimum": 1
},
```

## Architecture

### Component map

```
Operator                              Broker
─────────                             ─────
KafkaListenerAuthenticationOAuth      [oauthbearer]
  .maxSecondsWithoutReauthentication    max_session_lifetime_seconds
            │                                       │
            ▼                                       ▼
controller/listeners.rs               file_config.rs (apply_to)
  oauth_canonical extends                       │
  render_broker_toml emits                      ▼
            │                         BrokerConfig
            ▼                           .oauthbearer_max_session_lifetime_seconds
broker TOML over the wire                       │
            │                                   ▼
            └────────────────────►   network/auth.rs
                                       handle_authenticate_oauthbearer
                                         (and reauth arm)
                                         clamps session_ms
                                         stores effective expires_at_ms
                                                  │
                                                  ▼
                                       network/dispatch.rs
                                         select! timer fires
                                         at effective_expires_at_ms
```

### Why `Authenticated.expires_at_ms` must store the CLAMPED value

49e's dispatch loop computes the timer deadline from
`Authenticated.expires_at_ms`. If 50d stored the raw token `exp` there,
the timer would fire at the token's natural expiry — but the response
already told the client a shorter `session_lifetime_ms`. Three failure
modes:

1. Connection stays open past the client's expected expiry. Bad for ops
   visibility.
2. Client re-auths at what it thinks is its deadline; broker accepts
   because the timer hasn't fired yet. The cap is then meaningless.
3. Test assertions on timer-fire timing become non-deterministic.

Storing the clamped value (`now + session_ms`) ensures the timer fires
at the time the client was told.

### Operator divergence enforcement

The operator already rejects two OAuth listeners with divergent
`validIssuerUri` (or any canonical field) as
`ConflictingOAuthListenerConfig`. `maxSecondsWithoutReauthentication`
joins the canonical-config list. Two listeners with caps 300 and 600 →
reject at reconcile-time; the broker only sees one value.

### Greenfield: no `#[serde(default)]` magic on the broker config

`FileOAuthBearerConfig.max_session_lifetime_seconds: Option<u32>` is
already optional via the type. Serde's default for `Option<u32>` is
`None`, which is exactly the "no cap" sentinel. No `#[serde(default)]`
attribute needed.

## Testing

### Broker unit tests (`crates/broker/src/network/auth.rs::tests`)

- `handle_authenticate_oauthbearer_clamps_session_lifetime_when_cap_set_below_exp`
  — cap = 30, token exp = now+60s → `session_lifetime_ms = 30_000`,
  `expires_at_ms` stored = `now + 30_000`.
- `handle_authenticate_oauthbearer_no_clamp_when_cap_unset` —
  regression for 49e behavior.
- `handle_authenticate_oauthbearer_no_clamp_when_cap_above_exp` — cap
  = 600, token exp = now+60s → `session_lifetime_ms ≈ 60_000`.

### Broker integration test (`crates/broker/tests/auth_handlers.rs`)

- `oauthbearer_session_capped_by_broker_max_session_lifetime_seconds`
  — wires the cap into the test BrokerConfig, uses `tokio::time::pause`,
  asserts (a) response `session_lifetime_ms` carries the cap value
  (not the token's natural exp), and (b) the dispatch timer fires at
  cap time (advance by `cap + 1s`, expect EOF).

### Operator CRD tests (`crates/operator/src/crd/listener.rs::tests`)

- `oauth_round_trip_with_max_seconds_without_reauthentication` —
  serialize/deserialize round-trip with the field set.
- `oauth_round_trip_without_max_seconds_without_reauthentication_omits_field`
  — Option-None → key absent in YAML.
- Schema regression: existing `crd_oauth_schema_emits_expected_properties`
  asserts the new key is present with `minimum: 1`.

### Operator reconciler tests (`crates/operator/src/controller/listeners.rs::tests`)

- `render_broker_toml_emits_max_session_lifetime_seconds_when_set`.
- `render_broker_toml_omits_max_session_lifetime_seconds_when_unset`.
- Divergence walk: existing
  `validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field`
  extended to perturb `max_seconds_without_reauthentication` and
  expect `ConflictingOAuthListenerConfig`.

### Operator integration tests (`crates/operator/tests/reconcile_listener_oauth.rs`)

- `oauth_listener_with_max_seconds_without_reauthentication_renders_broker_toml_key`
  — apply a Kafka CR with the field set; assert the rendered broker
  TOML carries `max_session_lifetime_seconds = N`.
- `two_oauth_listeners_with_divergent_max_seconds_without_reauthentication_rejected_with_conflicting_oauth_config`
  — end-to-end divergence.

### E2E extension (`.github/workflows/operator-e2e.yml`)

Extend the existing `kind-oauth` job's Kafka CR YAML with
`maxSecondsWithoutReauthentication: 300`. 300 seconds is well above
any reasonable producer Job runtime, so the existing produce-and-
consume assertion still passes; the broker's startup log will record
the cap value, which we can grep in the diagnostics step.

No new e2e job. No `kind-oauth-introspection` variant change.

### No JVM differential test

JVM admin tools (`kafka-topics`, `kafka-acls`, etc.) don't read OAuth
listener config and don't care about session-lifetime caps.

## File touch list

- `crates/broker/src/file_config.rs` — new field on
  `FileOAuthBearerConfig` + threading through `apply_to`.
- `crates/broker/src/network/auth.rs` — handler clamp in both
  Negotiating and Reauthenticating arms; new function parameter.
- `crates/broker/src/network/dispatch.rs` — call site updates to pass
  the cap from `BrokerConfig`.
- `crates/broker/tests/auth_handlers.rs` — integration test for the
  cap (response field + timer fire timing).
- `crates/operator/src/crd/listener.rs` — new CRD field +
  hand-rolled schema + round-trip tests + struct-literal fixture
  sweep (a handful of existing test sites need the new default).
- `crates/operator/src/controller/listeners.rs` — `oauth_canonical`
  extension + `render_broker_toml` emit + divergence walk update +
  render unit tests.
- `crates/operator/tests/reconcile_listener_oauth.rs` — integration
  tests.
- `crates/operator/sample/oauth-listener.yaml` — commented-out
  example showing the field.
- `deploy/crds/crabka.io_kafkas.yaml` — regenerated CRD.
- `.github/workflows/operator-e2e.yml` — `kind-oauth` job CR YAML
  extension.
- `STATUS.md` — slice 50d entry.

## Decomposition for the plan

Six tasks across three batches:

| Batch | Task | Files (disjoint within batch) |
|---|---|---|
| 1 | T1 — Broker config + handler clamp + tests | `file_config.rs`, `auth.rs`, `dispatch.rs`, `tests/auth_handlers.rs` |
| 2 | T2 — Operator CRD field + schema + round-trip tests | `crd/listener.rs` |
| 2 | T3 — Operator reconciler + TOML render + divergence + unit tests | `controller/listeners.rs` |
| 3 | T4 — Operator integration tests + sample + CRD regen | `tests/reconcile_listener_oauth.rs`, `sample/oauth-listener.yaml`, `deploy/crds/crabka.io_kafkas.yaml` |
| 3 | T5 — E2E extension on `kind-oauth` job | `.github/workflows/operator-e2e.yml` |
| 4 | T6 — STATUS.md + final gate | `STATUS.md` |

Dependency chain: T1 → (T2 ‖ T3) → (T4 ‖ T5) → T6. T1 alone first
because T3 needs to know the broker TOML key name to render it.
Batches 2 and 3 are file-disjoint and run in parallel per CLAUDE.md.

## What this design does not commit to

- The exact `BrokerConfig` field name (`oauthbearer_max_session_lifetime_seconds`
  is the working name; minor renames at implementation time are fine).
- Whether the broker logs the cap value at startup, and at what level
  (info vs debug). Match existing `[oauthbearer]` startup-log style at
  plan time.
- Whether the operator's render output puts `max_session_lifetime_seconds`
  before or after `jwks_refresh_seconds` in the TOML. Whatever
  alphabetical order the existing renderer uses.

## After 50d lands

Per the umbrella roadmap, the next pair is **49f + 50e** —
PLAIN-with-OAuth-token. Optional: only worth shipping if a real user
reports a client they can't migrate to OAUTHBEARER. After that,
**49g + 50f** — claim enrichments + remaining Strimzi fields.
