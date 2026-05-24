# Slice 49e — Broker: KIP-368 SASL re-authentication

Status: Draft
Date: 2026-05-24
Umbrella: `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`
Pairs with: slice 50d (operator surface — `maxSecondsWithoutReauthentication` on listener OAuth config)

## Goal

Ship KIP-368 SASL re-authentication on the broker for OAUTHBEARER
connections: report each session's expiry to the client via
`SaslAuthenticateResponse.session_lifetime_ms`, accept in-band
re-authentication (a fresh `SaslHandshake`/`SaslAuthenticate` pair on
an already-authenticated connection), and close the connection cleanly
when the timer fires without a successful re-auth.

## Why this slice exists

Today, an OAUTHBEARER token that expires keeps its connection alive
indefinitely — the broker validates `exp` at auth time and never
revisits it. Slices 49b (JWKS / signed JWT) and 49d (RFC 7662
introspection) both check `exp` once and discard the value. That's a
security regression compared to Apache Kafka, which implements KIP-368
to bound a SASL session by token lifetime and to close the connection
when the token expires.

This slice fixes that for OAUTHBEARER. Mechanism-agnostic re-auth (a
broker-global `connections.max.reauth.ms` covering PLAIN/SCRAM/GSSAPI)
is explicitly out of scope — see the deferrals at the end.

## Scope

### In scope

- `SaslAuthenticateResponse v1+`: populate `session_lifetime_ms` for
  OAUTHBEARER connections only. PLAIN/SCRAM/no-auth keep emitting 0
  (= "no re-auth required" per KIP-368 wire spec).
- Per-connection shutdown timer that fires at session expiry and
  closes the TCP connection cleanly.
- In-band re-authentication: a client sends a fresh `SaslHandshake` +
  `SaslAuthenticate` pair on an already-authenticated OAUTHBEARER
  connection to refresh; server validates same-mechanism +
  same-principal-name, updates session state, resets the timer.
- All three OAuth validators (unsecured JWS, signed JWKS, RFC 7662
  introspection) surface the token's `exp` to the auth path so the
  handler can compute `session_lifetime_ms`.

### Out of scope (deferred to other slices or never)

- **Mechanism-agnostic `connections.max.reauth.ms`** — would force
  re-auth on PLAIN/SCRAM/GSSAPI too. Not in the OAUTHBEARER parity
  umbrella; can ship later if a user reports a real ask.
- **Operator-side `maxSecondsWithoutReauthentication` CRD field** —
  slice 50d.
- **Server-side cap on `session_lifetime_ms`** (e.g.
  `oauthbearer.max.session.lifetime.ms`) — defense-in-depth knob;
  not required for parity, can ship later.
- **Server-side minimum check** ("token too-short-lived, reject auth
  outright") — Kafka doesn't do this either; the client is responsible
  for not connecting with a 5-second token.
- **Client-side re-auth scheduler** in Crabka's Kafka client crate —
  separate roadmap; this slice is broker-only. The broker reports
  `session_lifetime_ms`; Java/librdkafka clients schedule their own
  proactive re-auth. Crabka's client gets parity later.

## Wire format

No new fields. `session_lifetime_ms: i64` already exists in the
codegen'd v1+ response:

```
crates/protocol/generated/SaslAuthenticateResponse.borrowed.rs:33
```

The field has been encoded as 0 since slice 49 shipped. This slice
flips the value to `max(token_exp_ms - now_ms, 0)` for OAUTHBEARER
auth outcomes; other mechanisms keep emitting 0.

Wire field order (per Apache Kafka KIP-368, already matched in the
codegen): `error_code`, `error_message`, `auth_bytes`,
`session_lifetime_ms`, tagged fields (v2+).

## Architecture

### New type: `AuthOutcome`

In `crates/security/src/oauthbearer.rs`:

```rust
pub struct AuthOutcome {
    pub principal: Principal,
    /// Token expiry in epoch ms. `None` = no expiry / no re-auth.
    /// For OAUTHBEARER this is always `Some` (validators reject tokens
    /// without `exp`); the Option keeps the type usable for future
    /// non-OAuth paths that may not carry an explicit expiry.
    pub expires_at_ms: Option<u64>,
}
```

The validator trait `OAuthBearerValidator::validate` flips from
`-> Result<Principal, AuthError>` to
`-> Result<AuthOutcome, AuthError>`. Each of the three concrete
validators (unsecured JWS, signed JWKS, RFC 7662 introspection)
already parses + checks `exp` during validation — they just stop
discarding the value:

- `UnsecuredJwsValidator` — `exp` extracted at the existing line ~159
  check.
- `SignedJwsValidator` — same pattern; JWT decode already extracts
  `exp`.
- `IntrospectionValidator` — `exp` comes from the introspection
  response (or the merged userinfo if introspection has none, though
  per RFC 7662 introspection's `exp` is canonical).

Greenfield rename, no deprecation shim. The change is mechanical per
validator.

### State machine extension: `ConnectionAuth`

In `crates/broker/src/network/auth.rs`. Current variants:
`Anonymous`, `Negotiating { mechanism, exchange }`, `Authenticated
{ principal }`.

Changes:

1. `Authenticated` carries the session state needed for the timer
   and for re-auth validation:

   ```rust
   Authenticated {
       principal: Principal,
       mechanism: SaslMechanism,
       expires_at_ms: Option<u64>,  // None = no re-auth (PLAIN/SCRAM)
   }
   ```

2. New `Reauthenticating` variant for in-band re-auth in progress:

   ```rust
   Reauthenticating {
       previous: AuthenticatedSnapshot,    // (principal, mechanism, expires_at_ms)
       exchange: NegotiatingExchange,      // same shape as Negotiating's
   }
   ```

   `previous` carries the still-current authenticated state so the
   post-validate equality check has something to compare against and
   a failed re-auth's error message can reference the current
   principal.

3. New helper `is_reauth_request(api_key: ApiKey) -> bool` returns
   true for `SaslHandshake` and `SaslAuthenticate` only.

4. `is_pre_auth_allowed(api_key)` is extended to gate `Reauthenticating`
   identically to `Negotiating`: only `SaslAuthenticate` accepted.

### Handler changes

**`SaslHandshake` handler** (in `auth.rs`):

- Currently rejects when `ConnectionAuth != Anonymous`.
- New: also accepts when `ConnectionAuth == Authenticated
  { mechanism: M, .. }`. The request mechanism must equal `M`;
  otherwise respond with `Errors::ILLEGAL_SASL_STATE` (Kafka's
  reference behavior) and close.
- On a valid in-band handshake: snapshot the current `Authenticated`
  into `previous`, transition to `Reauthenticating { previous,
  exchange: Negotiating(M) }`. Response shape unchanged.

**`SaslAuthenticate` handler** (in `auth.rs`):

- During `Reauthenticating`: run the same validator instance as the
  initial auth (same mechanism, so same configured validator).
- On validator success → `AuthOutcome { principal: P2, expires_at_ms:
  Some(exp2) }`:
  - If `P2.name != previous.principal.name`: respond with
    `Errors::SASL_AUTHENTICATION_FAILED` (message: "re-authentication
    may not change the principal"), then close.
  - Else: transition to `Authenticated { principal: P2, mechanism: M,
    expires_at_ms: Some(exp2) }`. Response carries `session_lifetime_ms
    = max(exp2 - now_ms, 0)`.
- On validator failure: respond with `Errors::SASL_AUTHENTICATION_FAILED`
  (validator's error message), then close.

### Dispatch loop: per-connection `select!` timer

`serve_connection_stream` in `crates/broker/src/network/dispatch.rs`
becomes (sketch):

```rust
let mut conn_auth = ConnectionAuth::Anonymous;
loop {
    let expiry: Option<tokio::time::Instant> = conn_auth
        .session_expires_at_ms()
        .map(|exp_ms| /* epoch-ms → Instant via the broker's clock */);

    tokio::select! {
        biased;  // prefer request handling over timer when both are ready
        next = framed.next() => {
            // existing request-handling path; SASL handlers mutate
            // conn_auth, the next iteration picks up new expiry
            match next { ... }
        }
        _ = sleep_until_some(expiry) => {
            // Timer fired before client re-authed. Close cleanly.
            tracing::info!(
                principal = ?conn_auth.principal_name(),
                "session expired; closing connection"
            );
            break;
        }
    }
}
```

`sleep_until_some(opt)` returns the underlying `sleep_until` when
`Some`, or `std::future::pending()` when `None`. This disarms the
timer arm for non-OAuth (no-expiry) connections.

`biased` ensures that if a request and the timer are both ready in
the same poll, the request wins — letting the last in-flight request
before expiry complete normally per KIP-368 spirit.

**Clock injection.** The validators already take `now_ms: u64`
(slice-49b/49d pattern). The dispatch loop converts epoch-ms expiry
into a tokio `Instant` either via the existing broker clock helper
(plan step 1 will grep for one) or via
`Instant::now() + (exp_ms - now_ms).milliseconds()`. Tests use
`tokio::time::pause/advance` to control.

### Reauth: same-mechanism, same-principal

Per KIP-368:

- Same mechanism: enforced by the `SaslHandshake` handler before the
  validator runs.
- Same principal name: enforced by the `SaslAuthenticate` handler
  after the validator returns.
- Either mismatch → error response + close.

No fallback to "keep the old session" — a failed re-auth is fatal to
the connection. Same as Kafka.

### Connection close on timer fire

When the `sleep_until_some` arm wins:

- No response is sent. The client is responsible for re-auth before
  expiry; if it didn't, the broker just closes the socket. (KIP-368
  doesn't define a "session-expired" error frame — the close itself
  is the signal.)
- The dispatch loop breaks out and the surrounding
  `serve_connection_stream` returns normally; Tokio's drop closes the
  TCP socket.
- A tracing log line records the principal name and the expiry event
  for ops visibility.

## Testing

### Unit tests — validator level (`crates/security/src/oauthbearer.rs`)

- `validate_returns_exp_from_jwt_claim` (unsecured + signed JWS).
- `validate_returns_exp_from_introspection_response`.

### State-machine unit tests (`crates/broker/src/network/auth.rs`)

- `reauth_handshake_from_authenticated_state_allowed_same_mechanism`.
- `reauth_handshake_rejected_when_mechanism_changes` → `ILLEGAL_SASL_STATE`.
- `reauth_authenticate_rejected_when_principal_changes` →
  `SASL_AUTHENTICATION_FAILED`.
- `pre_auth_gate_rejects_non_sasl_requests_during_reauthenticating`.

### Integration tests (`crates/broker/tests/auth_handlers.rs`)

Extends the existing `drive_sasl_plain_session()`-style helper for
OAUTHBEARER:

- `oauthbearer_session_lifetime_ms_set_from_token_exp` — happy path;
  assert response field carries `exp - now`.
- `oauthbearer_session_expires_closes_connection` — token
  `exp = now + 100ms`; `tokio::time::pause` + `advance(101ms)`;
  next read on the connection returns EOF.
- `oauthbearer_in_band_reauth_with_fresh_token_resets_timer` —
  handshake with token-A (`exp=now+5s`); advance 4s; re-handshake
  with token-B (`exp=now+10s`); advance another 4s (8s total, past
  token-A's `exp`); assert connection still open and a Metadata RPC
  succeeds.
- `oauthbearer_in_band_reauth_with_different_principal_closes` —
  new token's `sub` claim differs from the original; assert error
  response then EOF.
- `oauthbearer_in_band_reauth_with_different_mechanism_closes` —
  initial OAUTHBEARER, re-auth tries SCRAM-SHA-512.
- `plain_listener_session_lifetime_ms_is_zero_and_no_timer` —
  confirms non-OAuth listener unchanged.

### Clock control

`tokio::time::pause()` + `tokio::time::advance(Duration)` in tests to
drive the timer deterministically. The plan task will mirror whatever
clock pattern the existing `auth_handlers.rs` integration tests use.

### Not in scope for this slice

- JVM differential test — the JVM admin tools don't exercise SASL
  re-auth.
- Real client end-to-end (librdkafka against the broker, long-running
  produce that crosses a token-expiry boundary) — deferred to slice
  50d's e2e job since the operator already has Keycloak in kind.

## File touch list

- `crates/security/src/oauthbearer.rs` — `AuthOutcome`; validator
  return shape; per-validator `exp` surfacing.
- `crates/broker/src/network/auth.rs` — `ConnectionAuth` extension
  (`Authenticated` carries session state; new `Reauthenticating`
  variant); handler changes for in-band re-auth; pre-auth gate
  update.
- `crates/broker/src/network/dispatch.rs` — per-connection `select!`
  with `sleep_until_some`.
- `crates/broker/tests/auth_handlers.rs` — integration tests for the
  KIP-368 scenarios.
- `STATUS.md` — slice 49e entry.

No CRD changes. No operator changes. No e2e workflow changes. No new
broker config keys.

## Decomposition for the plan

The plan task will split into roughly 5 work items:

1. Validator refactor (`oauthbearer.rs`) — self-contained.
2. State machine + handler changes (`auth.rs`) — combined into one
   task since both touch the same file (CLAUDE.md file-disjoint rule).
3. Dispatch loop timer (`dispatch.rs`).
4. Integration tests (`auth_handlers.rs`).
5. STATUS.md + final gate.

Dependency chain: 1 → 2 → 3 → 4 → 5. Mostly sequential since each
task's input depends on the previous task's output (validator returns
flow into handler, handler updates flow into dispatch loop state,
dispatch loop is what the integration tests exercise).

## What this design does not commit to

- Specific tokio-time API details (`sleep_until` vs `sleep` + offset)
  — pick at plan time based on existing broker patterns.
- Whether `AuthenticatedSnapshot` is a new named struct or a tuple
  inside `Reauthenticating` — refactor decision at implementation
  time.
- The exact tracing-log shape — match existing broker conventions.

These are local implementation choices that don't affect the wire
format, the auth semantics, or the test contract.
