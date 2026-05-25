# Slice 49i — Operator + Broker: OAUTHBEARER JWKS refresher policies

Status: Draft
Date: 2026-05-24
Umbrella: `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`
Builds on: slice 49b (JWKS validator + refresher), slices 49g/49h (sibling long-tail clusters)
Follows: nothing — this is the LAST OAUTHBEARER umbrella slice.

## Goal

Final cluster of the OAUTHBEARER umbrella. Three Strimzi-shape JWKS
operational tuning fields on the listener OAuth CRD + broker:

- `jwksMinRefreshPauseSeconds` — rate-limits on-demand JWKS refresh
  triggered by tokens with unknown `kid`.
- `jwksExpirySeconds` — hard cache expiry; validators reject tokens
  when the cached JWKS is older than this (fails closed when IdP is
  unreachable).
- `jwksIgnoreKeyUse` — toggle that disables the default `use=sig`
  filter on JWKS keys (for IdPs that mis-tag their signing keys).

After 49i lands, the OAUTHBEARER umbrella reaches Strimzi field parity
(modulo the explicitly-skipped slice 49f, PLAIN-with-OAuth-token).

## Why three orthogonal behaviors in one slice

Per the 49g brainstorming session's decomposition, the long-tail
splits into three feature-sliced bundles (validation = 49g, claims =
49h, JWKS = 49i). All three are operationally independent:

- `jwksMinRefreshPauseSeconds` is a refresher-loop rate-limit on a
  new on-demand-refresh code path.
- `jwksExpirySeconds` is a validator-side fail-closed check on a
  new last-successful-fetch timestamp.
- `jwksIgnoreKeyUse` is a single-line filter toggle in the JWKS
  parser.

Bundling them keeps the OAUTHBEARER umbrella PR count down without
mixing concerns within a single PR.

## Architecture choice (Approach A: fire-and-forget signaling)

On-demand JWKS refresh is the only non-trivial architectural addition.
The validator stays sync (preserves the existing JWT-validate API);
when it encounters an unknown `kid`, it fires an mpsc signal to the
refresher and rejects the current token. The refresher reads the
signal in a `tokio::select!` alongside the periodic-refresh tick,
subject to a rate-limit pause.

Rejected alternative (Approach B): make `validate()` async and await
an immediate refresh + retry. Would let the FIRST failing token
succeed (no client retry), but breaks the existing sync JWT validator
API and holds the SASL handshake connection open for the IdP RTT.

Rejected alternative (Approach C): skip on-demand refresh entirely,
make `jwksMinRefreshPauseSeconds` a no-op. Breaks Strimzi semantic
parity for fast-key-rotation deployments.

## Scope

### In scope

**Broker JWKS refresher** (slice-49b module — path TBD at plan time):

- New `last_successful_fetch: Arc<AtomicI64>` (epoch ms). Updated
  after each successful fetch; readable by validators for the
  expiry check.
- New `last_on_demand_refresh: Arc<AtomicI64>` (epoch ms). Updated
  after each on-demand fetch; used for the rate-limit gate.
- New `signal_rx: mpsc::Receiver<()>`. Validator-side `signal_tx` is
  cloned into the validator's `JwksHandle`.
- New `min_on_demand_pause: Duration`. Rate-limit window.
- New `ignore_key_use: bool`. Filter toggle for JWKS parsing.
- Loop becomes `tokio::select!` over periodic-tick + `signal_rx.recv()`.
  On-demand refresh fires only when `now - last_on_demand_refresh >=
  min_on_demand_pause`; otherwise the signal is dropped silently.
- `parse_jwks` filters keys: drop `use=enc` keys unless
  `ignore_key_use: true`. `use` absent → always kept (existing
  behavior).

**Broker `SignedJwsValidator`** (the only JWKS-consuming validator):

- New `expiry_ms: Option<i64>` field.
- At top of `validate()`, after the JWS structural parse + alg check:
  if `expiry_ms` is set AND `now_ms - last_successful_fetch > expiry_ms`,
  reject with `InvalidToken`. Cache is stale; fail closed.
- On `keys.verify()` failure (unknown kid or bad signature): fire
  `keys.signal_refresh()` (non-blocking `try_send`). Reject the
  current token. Next successful refresh allows next attempt.

**Unsecured-JWS + Introspection validators**: UNCHANGED. Neither
consults JWKS.

**Broker `JwksHandle`** (validator-side accessor): gain
`last_successful_fetch_ms()` + `signal_refresh()` methods.

**Broker `FileOAuthBearerConfig`**: 3 new `Option<>` fields
(`jwks_min_refresh_pause_seconds: Option<u32>`,
`jwks_expiry_seconds: Option<u32>`,
`jwks_ignore_key_use: Option<bool>`). `apply_to` threads them to the
refresher constructor + the validator's `expiry_ms` field.

**Operator CRD `ListenerAuthenticationOAuth`**: 3 new fields,
Strimzi-shape camelCase, hand-rolled schema entries:

- `jwksMinRefreshPauseSeconds: integer, minimum: 0`
- `jwksExpirySeconds: integer, minimum: 1`
- `jwksIgnoreKeyUse: boolean`

**Operator reconciler:**

- `render_broker_toml` emits 3 new keys when set (plain double-quoted
  numbers/bool; no escape concerns).
- **NEW cross-mode validation**:
  `ListenerOauthJwksFieldsRejectedInIntrospectionMode(String)` fires
  when any of the 3 fields is set on an `accessTokenIsJwt: false`
  listener. Introspection mode doesn't use JWKS; setting these
  fields is a configuration error worth surfacing at apply time
  (rather than silently ignoring as Strimzi does).
- Cross-listener divergence walk gets 3 new perturbations.

**E2E** (`.github/workflows/operator-e2e.yml`):

- `kind-oauth` job's Kafka CR YAML adds the 3 fields.
- `kind-oauth-introspection` job's CR is NOT touched (cross-mode
  validator would reject).

### Out of scope

- **Approach B** (synchronous-await refresh on validator) — rejected
  per architectural decision.
- **Approach C** (skip on-demand refresh) — rejected; would break
  Strimzi semantic parity.
- **Per-kid on-demand refresh batching** beyond the global min-pause
  rate limit. If 1000 tokens with the same bad kid arrive in 1
  second, the refresher does ONE refresh (rate-limit absorbs the
  rest). Good enough.
- **Per-listener JWKS refreshers** — broker still has one global
  `[oauthbearer]` block. Operator rejects two OAuth listeners with
  divergent config as `ConflictingOAuthListenerConfig` (existing
  50c behavior).
- **JWKS configuration validation at reconcile time** — operator
  just renders the values; broker validates at startup.
- **No follow-up OAUTHBEARER umbrella slices.** After 49i:
  Strimzi field parity complete. Slices 51+ from the operator
  roadmap consume the scaffolding (Principal.groups, customClaimCheck)
  for authorizer plugins.

## Wire / config / CRD shapes

### Broker TOML (new keys under `[oauthbearer]`)

```toml
[oauthbearer]
# existing slice-49b/49d/49g/50d keys ...
jwks_min_refresh_pause_seconds = 1
jwks_expiry_seconds = 3600
jwks_ignore_key_use = false
```

### Operator CRD

```yaml
authentication:
  type: oauth
  validIssuerUri: https://...
  jwksEndpointUri: https://.../jwks
  jwksRefreshSeconds: 300         # slice 49b — periodic cadence
  jwksMinRefreshPauseSeconds: 1   # 49i — on-demand refresh rate-limit
  jwksExpirySeconds: 3600         # 49i — hard cache expiry
  jwksIgnoreKeyUse: false         # 49i — JWKS key-filter toggle
```

## Architecture

### Refresher loop changes

Pre-49i (slice 49b):

```rust
loop {
    tokio::time::sleep(refresh_interval).await;
    self.refresh_and_swap().await;
}
```

Post-49i:

```rust
loop {
    tokio::select! {
        _ = tokio::time::sleep(refresh_interval) => {
            // Periodic refresh — unchanged 49b behavior.
            self.refresh_and_swap().await;
        }
        _ = signal_rx.recv() => {
            let now_ms = now_ms();
            let last = last_on_demand_refresh.load(Ordering::Relaxed);
            if now_ms - last >= min_on_demand_pause.as_millis() as i64 {
                last_on_demand_refresh.store(now_ms, Ordering::Relaxed);
                self.refresh_and_swap().await;
            } else {
                tracing::debug!(
                    "JWKS on-demand refresh rate-limited (last={last}, min_pause_ms={})",
                    min_on_demand_pause.as_millis()
                );
            }
        }
    }
}
```

### `refresh_and_swap` (49b method, extended)

```rust
async fn refresh_and_swap(&self) {
    match self.fetch_jwks().await {
        Ok(jwks_json) => {
            let keys = parse_jwks(&jwks_json, self.ignore_key_use);
            self.keys.store(Arc::new(keys));
            self.last_successful_fetch
                .store(now_ms(), Ordering::Relaxed);
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "JWKS refresh failed; keeping previous keyset"
            );
            // last_successful_fetch unchanged — cache ages toward expiry.
        }
    }
}
```

### `parse_jwks` filter (49b method, extended)

```rust
fn parse_jwks(json: &str, ignore_key_use: bool) -> JwksKeyset {
    let mut keyset = JwksKeyset::default();
    for raw_key in raw_keys_iter(json) {
        if !ignore_key_use {
            // Strimzi default: drop encryption-only keys.
            let use_field = raw_key.get("use").and_then(Value::as_str);
            if use_field == Some("enc") {
                continue;
            }
            // use=sig or use absent → keep.
        }
        // existing 49b parse: kid, alg, n/e or x/y, push to keyset.
    }
    keyset
}
```

### `JwksHandle` (validator accessor) extensions

```rust
impl JwksHandle {
    /// Slice 49i: epoch-ms timestamp of last successful JWKS fetch.
    /// 0 if no fetch has succeeded yet (initial state).
    pub fn last_successful_fetch_ms(&self) -> i64 { ... }

    /// Slice 49i: fire-and-forget signal to the refresher that an
    /// on-demand refresh is requested (e.g., unknown-kid token).
    /// Subject to rate-limiting by `jwksMinRefreshPauseSeconds`.
    pub fn signal_refresh(&self) { ... }
}
```

### `SignedJwsValidator::validate()` integration

Two new checks at well-defined points:

```rust
pub fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
    // existing: structural parse, alg check, typ check (49g) ...

    // Slice 49i: hard cache-expiry check. If the cached JWKS is older
    // than `expiry_ms`, reject regardless of token. Fails closed when
    // the IdP has been unreachable for too long.
    if let Some(expiry_ms) = self.expiry_ms {
        let last_fetch = self.keys.last_successful_fetch_ms();
        if last_fetch > 0 && (now_ms - last_fetch) > expiry_ms {
            tracing::debug!(
                last_fetch_ms = last_fetch,
                now_ms = now_ms,
                expiry_ms = expiry_ms,
                "JWKS cache expired; rejecting token until next successful refresh"
            );
            return Err(AuthError::InvalidToken);
        }
    }

    // existing: signature verification via keys.verify(...).
    match self.keys.load().verify(kid, alg, signing_input.as_bytes(), &sig) {
        Ok(()) => { /* claim checks, name fallback (49h), groups (49h) ... */ }
        Err(AuthError::InvalidToken) => {
            // Slice 49i: trigger on-demand refresh. Fire-and-forget;
            // the refresher rate-limits via jwks_min_refresh_pause.
            // The current token is rejected; next successful refresh
            // allows the next attempt to succeed.
            self.keys.signal_refresh();
            return Err(AuthError::InvalidToken);
        }
        Err(other) => return Err(other),
    }

    // existing: claim checks → AuthOutcome ...
}
```

**On the verify() failure mode**: we signal for ANY verify failure
(unknown kid OR present-but-bad-signature). Distinguishing them
would require a new error variant. The simpler "signal on any
verify failure" rate-limits the extra refresh round-trip via
`jwks_min_refresh_pause` — bounded cost.

### `FileOAuthBearerConfig.apply_to` threading

The 3 new fields plumb into the signed-validator branch only
(unsecured + introspection ignore them):

```rust
(Some(_), None) => {
    // signed validator branch
    let (signal_tx, signal_rx) = mpsc::channel::<()>(1);
    let last_successful_fetch = Arc::new(AtomicI64::new(0));
    let last_on_demand_refresh = Arc::new(AtomicI64::new(0));

    let refresher = JwksRefresher::new(
        jwks_uri,
        refresh_interval,
        // ... existing args ...
        // Slice 49i:
        last_successful_fetch.clone(),
        last_on_demand_refresh.clone(),
        signal_rx,
        Duration::from_secs(u64::from(
            oauth.jwks_min_refresh_pause_seconds.unwrap_or(1)
        )),
        oauth.jwks_ignore_key_use.unwrap_or(false),
    );

    let handle = JwksHandle::new(
        refresher.keys_arc(),
        last_successful_fetch,
        signal_tx,
    );

    let mut v = SignedJwsValidator::new(handle, /* existing args */);
    // Slice 49g + 49h fields (unchanged):
    v.custom_claim_check = custom_claim_check_compiled.clone();
    v.valid_token_type.clone_from(&oauth.valid_token_type);
    v.fallback_user_name_claim.clone_from(&oauth.fallback_user_name_claim);
    // ... etc.
    // Slice 49i:
    v.expiry_ms = oauth.jwks_expiry_seconds.map(|s| i64::from(s) * 1000);

    cfg.oauthbearer_validator =
        crabka_security::OAuthBearerValidator::Signed(v);
    // ... refresher spawned by Broker::start as before
}
```

The Strimzi default for `jwks_min_refresh_pause_seconds` (1) is
applied via `.unwrap_or(1)` at apply time — keeps the broker config
clean when the operator omits the field.

### Cross-mode validator

New `ValidationError` variant:

```rust
ListenerOauthJwksFieldsRejectedInIntrospectionMode(String),
```

Fires in `validate_listeners` when `accessTokenIsJwt: false` AND any of
the 3 fields is set:

```rust
} else {
    // existing introspection-mode rejections ...

    // Slice 49i: JWKS fields are JWT-mode-only.
    let mut jwks_fields_set = Vec::new();
    if cfg.jwks_min_refresh_pause_seconds.is_some() {
        jwks_fields_set.push("jwksMinRefreshPauseSeconds");
    }
    if cfg.jwks_expiry_seconds.is_some() {
        jwks_fields_set.push("jwksExpirySeconds");
    }
    if cfg.jwks_ignore_key_use.is_some() {
        jwks_fields_set.push("jwksIgnoreKeyUse");
    }
    if !jwks_fields_set.is_empty() {
        return Err(ValidationError::ListenerOauthJwksFieldsRejectedInIntrospectionMode(
            format!(
                "listener '{}': accessTokenIsJwt=false forbids JWKS-only fields ({})",
                l.name, jwks_fields_set.join(", ")
            ),
        ));
    }
}
```

## Testing

### Broker unit tests (`crates/security/src/oauthbearer.rs::tests`)

- `signed_validate_rejects_when_jwks_cache_expired` — set
  `last_successful_fetch_ms` to `now - expiry_ms - 1`; rejected.
- `signed_validate_accepts_when_jwks_cache_within_expiry` —
  regression for the happy path.
- `signed_validate_accepts_when_expiry_unset_regardless_of_cache_age`
  — regression for the no-config path.
- `signed_validate_signals_refresh_on_unknown_kid` — instrument a
  test refresher with mpsc receiver; assert signal arrives.
- `parse_jwks_filters_use_enc_by_default`.
- `parse_jwks_keeps_use_enc_when_ignore_key_use_true`.
- `parse_jwks_keeps_keys_with_absent_use_field_regardless`.

### Refresher unit tests

- `refresher_signal_triggers_on_demand_refresh_when_pause_elapsed`
  (`tokio::time::pause()` + `advance()`).
- `refresher_signal_dropped_when_within_min_pause_window`.
- `refresher_periodic_refresh_unaffected_by_on_demand_pause`.
- `refresher_successful_refresh_updates_last_successful_fetch_timestamp`.
- `refresher_failed_refresh_does_not_advance_last_successful_fetch`
  (cache ages toward expiry).

### Operator unit tests (`crates/operator/src/crd/listener.rs::tests`)

- 3 round-trip tests (one per new field): with-set + omits-when-unset
  (6 total).
- Schema regression test extension.

### Operator reconciler tests

- 3 render-emit tests.
- `validate_listeners_rejects_jwks_fields_in_introspection_mode`
  (table-driven across the 3 fields).
- Cross-listener divergence walk: 3 new perturbations.

### Operator integration tests (`tests/reconcile_listener_oauth.rs`)

- `oauth_listener_with_jwks_policies_renders_broker_toml_keys`.
- `oauth_listener_jwks_fields_in_introspection_mode_rejected_with_listeners_valid_false`.

### E2E

`kind-oauth` job's CR YAML adds:

```yaml
jwksMinRefreshPauseSeconds: 1
jwksExpirySeconds: 3600   # 1 hour — long enough not to interfere
jwksIgnoreKeyUse: false   # default; explicit to exercise the wire
```

`kind-oauth-introspection` job's CR is NOT touched (would be
rejected). T5 prompt must explicitly state this.

**Plan-time TBD**: verify Keycloak's JWKS endpoint returns keys
with `use=sig` (it does by default). If anything in the existing
realm bootstrap exposes encryption keys, the e2e would need an
adjustment.

### No JVM differential

JVM admin tools don't read OAuth listener config.

## File touch list

- `crates/security/src/oauthbearer.rs` — `SignedJwsValidator.expiry_ms`
  + `validate()` body changes + tests.
- `crates/broker/src/oauthbearer/jwks_refresher.rs` (or wherever the
  slice-49b refresher lives — TBD at plan time) — `select!` loop
  + on-demand refresh + `last_successful_fetch` tracking +
  `parse_jwks` filter.
- `crates/broker/src/file_config.rs` — 3 new fields + `apply_to`
  threading.
- `crates/broker/src/config.rs` — BrokerConfig fields if needed
  (likely not — validator carries the state directly).
- `crates/operator/src/crd/listener.rs` — 3 new fields + schema +
  own-file fixture sweep + round-trip tests.
- `crates/operator/src/controller/listeners.rs` — render + cross-mode
  validation + divergence walk + own-file + sibling-file fixture
  sweep + unit tests.
- `crates/operator/src/controller/kafka.rs` +
  `kafka_node_pool.rs` — fixture sweep.
- `crates/operator/tests/reconcile_listener_oauth.rs` +
  `reconcile_oauth_introspection.rs` + `reconcile_oauth_trust.rs` —
  fixture sweep + 2 new integration tests.
- `crates/operator/sample/oauth-listener.yaml` — commented-out
  examples for the 3 new fields.
- `deploy/crds/crabka.io_kafkas.yaml` — regenerated CRD.
- `.github/workflows/operator-e2e.yml` — `kind-oauth` job only
  (introspection job NOT touched).
- `STATUS.md` — slice 49i entry + OAUTHBEARER umbrella completion
  note.

## Decomposition for the plan

Six tasks across four batches (mirrors slice 49g/49h pattern):

| Batch | Task | Files |
|---|---|---|
| 1 | T1 — Broker: JWKS refresher rework + validator expiry check + signal-on-unknown-kid + parse-filter + FileOAuthBearerConfig + tests | `crates/security/src/oauthbearer.rs`, `crates/broker/src/oauthbearer/jwks_refresher.rs` (path TBD), `crates/broker/src/file_config.rs` |
| 2 | T2 — Operator CRD: 3 new fields + schema + own-file fixture sweep + round-trip tests | `crates/operator/src/crd/listener.rs` |
| 2 | T3 — Operator reconciler: render + cross-mode validation + divergence walk + sibling-file fixture sweep + unit tests | `crates/operator/src/controller/listeners.rs`, `controller/kafka.rs`, `controller/kafka_node_pool.rs` |
| 3 | T4 — Operator integration tests + sample + CRD regen + sibling-test fixture sweep | `crates/operator/tests/reconcile_*.rs`, `sample/oauth-listener.yaml`, `deploy/crds/*` |
| 3 | T5 — kind-oauth e2e CR YAML extension (introspection NOT touched per cross-mode rejection) | `.github/workflows/operator-e2e.yml` |
| 4 | T6 — STATUS.md + final gate + OAUTHBEARER umbrella completion note | `STATUS.md` |

**Dependency chain**: T1 → T2 → T3 → (T4 ‖ T5) → T6. Same shape as
slice 49g/49h.

## Holistic-review lessons carried forward

From slice 49g: introspection-job CR YAML must be carefully checked
when adding JWT-mode-only fields. T5 prompt must EXPLICITLY state
"do NOT touch the introspection job" — the cross-mode validator will
reject the operator config at apply time AND the introspection
listener has no use for JWKS-tuning anyway.

From slice 49h: per-task explore counts (fixture sweep estimates) are
often imprecise. Plan numbers should be estimates; implementer
verifies via grep.

## Plan-time TBDs

- Exact path of the slice-49b JWKS refresher module.
- Whether `JwksHandle` needs new public methods for
  `last_successful_fetch_ms()` + `signal_refresh()` (probably yes —
  add via T1).
- mpsc channel capacity (probably `1` — signals coalesce since
  `try_send` drops on full).
- Whether to tracing-log the rate-limit drop (useful for ops; minor).
- Whether to verify Keycloak's JWKS endpoint emits `use=sig`
  (probably yes by default).

## After 49i lands

**OAUTHBEARER umbrella complete.** Strimzi field parity reached
(modulo skipped 49f PLAIN-with-OAuth-token).

Per the operator roadmap, the next-up work is slices 51+:

- 51 — delegation tokens (Kafka admin client compat).
- 52 — GSSAPI / Kerberos (only-if-user-demand).
- 53/54 — OPA / Keycloak authorizer plugins (will consume the
  scaffolding from 49g `customClaimCheck`, 49h `Principal.groups`,
  49d introspection metadata).

These are out of the OAUTHBEARER scope; brainstorm separately when
ready.
