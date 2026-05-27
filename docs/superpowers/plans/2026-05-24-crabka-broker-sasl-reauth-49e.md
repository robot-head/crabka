# Slice 49e — Broker KIP-368 SASL re-authentication Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** ## Slice 49e — Broker: SASL re-authentication (KIP-368) (2026-05-24)

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Mechanism-agnostic connections.max.reauth.ms broker config (would gate PLAIN/SCRAM too)
- Operator-side maxSecondsWithoutReauthentication CRD field — closed by slice 50d
- Server-side cap on session_lifetime_ms (oauthbearer.max.session.lifetime.ms defense-in-depth knob) — closed by slice 50d
- Server-side minimum check (token too-short-lived, reject auth)
- Client-side re-auth scheduler in Crabka's Kafka client crate (broker-only this slice)

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship KIP-368 SASL re-authentication on the broker for OAUTHBEARER connections: surface token `exp` from the three OAuth validators, populate `SaslAuthenticateResponse.session_lifetime_ms`, add a per-connection `tokio::select!` timer that closes the connection at session expiry unless the client sends a fresh `SaslHandshake`/`SaslAuthenticate` pair (in-band re-auth — same mechanism, same principal enforced).

**Architecture:** Five sequential tasks. T1 introduces `AuthOutcome` and flips the OAuth validator returns. T2 extends `ConnectionAuth` (carries session state; adds `Reauthenticating`), updates the gate, and updates the `SaslHandshake` + `SaslAuthenticate` handlers. T3 wraps the dispatch read loop in a `tokio::select!` with `sleep_until_some(deadline)`. T4 adds the 6 integration scenarios using a new `drive_sasl_oauthbearer_session` helper + `tokio::time::pause`. T5 ships STATUS + final gate.

**Tech Stack:** Rust, tokio (async, `time::pause`, `time::sleep_until`, `select!`), tokio-util Framed codec, JWT (base64url + JSON), existing slice-49/49b/49d validators.

**Spec:** `docs/superpowers/specs/2026-05-24-crabka-broker-sasl-reauth-49e-design.md` (commit `e07845d`).

**Worktree:** `/Users/mattstone/git/crabka/.worktrees/slice-49e-sasl-reauth` on branch `slice-49e-sasl-reauth`. Verify with `git branch --show-current`. Commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`.

---

## File structure

| File | Responsibility | Touched by |
|---|---|---|
| `crates/security/src/oauthbearer.rs` | `AuthOutcome` type; OAuth validator returns | T1 |
| `crates/security/src/lib.rs` | Re-export `AuthOutcome` | T1 (small) |
| `crates/broker/src/network/auth.rs` | `ConnectionAuth` shape; `is_pre_auth_allowed` / new gate; `SaslHandshake` + `SaslAuthenticate` handlers | T2 |
| `crates/broker/src/network/dispatch.rs` | Per-connection `select!` timer; pass `expires_at_ms` from validator outcome to `Authenticated` state | T3 |
| `crates/broker/tests/auth_handlers.rs` | 6 KIP-368 integration scenarios + `drive_sasl_oauthbearer_session` helper | T4 |
| `STATUS.md` | Slice 49e entry | T5 |

No CRD, no operator, no e2e workflow changes.

**Spec note vs plan:** The spec says `expires_at_ms: Option<u64>`. Plan uses `Option<i64>` instead, to match the existing `now_ms: i64` parameter type on all validators and avoid casting in `exp_ms - now_ms`. Semantically equivalent; consistency wins.

---

## Batches

### Batch 1 (sequential — T1 alone)

#### Task T1: AuthOutcome + validator refactor

**Files:**
- Modify: `crates/security/src/oauthbearer.rs`
- Modify: `crates/security/src/lib.rs` (re-export)

**Context:** Currently, `OAuthBearerValidator::validate(...)` returns `Result<Principal, AuthError>` — the validator extracts and checks `exp` but discards the value. We need to surface `exp` so the SASL handler can compute `session_lifetime_ms`. Each of the three concrete validators (Unsecured, Signed, Introspection) already reads `exp`; the change is mechanical.

- [ ] **Step 1: Add the `AuthOutcome` struct + a failing test for the unsecured validator**

Edit `crates/security/src/oauthbearer.rs`. Find the existing `Principal` import / use area near the top, then add this struct definition (near the top of the file, after imports):

```rust
/// Outcome of an OAUTHBEARER validation: the authenticated principal plus the
/// token's expiry. The expiry is what slice 49e populates as
/// `SaslAuthenticateResponse.session_lifetime_ms` and what the dispatch loop
/// uses to schedule per-connection re-auth deadlines (KIP-368).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthOutcome {
    pub principal: crate::Principal,
    /// Token expiry as Unix epoch milliseconds. `None` means "no expiry / no
    /// re-auth required" — reserved for future non-OAuth paths. For
    /// OAUTHBEARER this is always `Some` (validators reject tokens without
    /// `exp`).
    pub expires_at_ms: Option<i64>,
}
```

Then in the existing `#[cfg(test)] mod tests` block (find an existing OAUTHBEARER unsecured-validator test and place the new one beside it), add:

```rust
#[test]
fn unsecured_validate_surfaces_exp_in_auth_outcome() {
    // exp = 2000 sec = 2_000_000 ms; now = 1_000_000 ms.
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_for_tests(&serde_json::json!({
        "sub": "alice",
        "exp": exp_secs,
    }));
    let v = UnsecuredJwsValidator::for_tests();
    let outcome = v.validate(&token, now_ms).expect("token valid");
    assert_eq!(outcome.principal.name, "alice");
    assert_eq!(outcome.expires_at_ms, Some(exp_secs * 1000));
}
```

If `make_unsecured_jws_for_tests` and `UnsecuredJwsValidator::for_tests()` helpers don't exist, the existing test module almost certainly has equivalent fixture builders (grep for `fn make_` and `for_tests` in this file). Use whichever helpers the existing tests use; adapt the test body to match.

- [ ] **Step 2: Run the test — verify it fails**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49e-sasl-reauth
cargo test -p crabka-security oauthbearer::tests::unsecured_validate_surfaces_exp_in_auth_outcome 2>&1 | tail -20
```

Expected: compile error (`.expires_at_ms` doesn't exist on `Principal`; `AuthOutcome` not imported; or `validate` returns `Principal`, not `AuthOutcome`). The test won't even compile until step 3.

- [ ] **Step 3: Refactor `UnsecuredJwsValidator::validate` to return `AuthOutcome`**

Replace the existing `UnsecuredJwsValidator::validate` body (~lines 136–187) with the version that returns `AuthOutcome`. The `exp_ms` value is already computed at the existing line ~159; just thread it through:

```rust
pub fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
    // JWS compact serialization: header.payload.signature. For `alg:none`
    // the signature segment is empty.
    let mut segs = token.split('.');
    let header_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
    let payload_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
    let sig = segs.next().ok_or(AuthError::InvalidToken)?;
    if segs.next().is_some() {
        return Err(AuthError::InvalidToken);
    }
    if !sig.is_empty() {
        return Err(AuthError::InvalidToken);
    }

    let header: Value = decode_json_segment(header_b64)?;
    if header.get("alg").and_then(Value::as_str) != Some("none") {
        return Err(AuthError::InvalidToken);
    }

    let claims: Value = decode_json_segment(payload_b64)?;

    let exp_ms = numeric_date_ms(&claims, "exp").ok_or(AuthError::InvalidToken)?;
    if exp_ms + self.allowable_clock_skew_ms <= now_ms {
        return Err(AuthError::InvalidToken);
    }
    if let Some(iat_ms) = numeric_date_ms(&claims, "iat")
        && iat_ms - self.allowable_clock_skew_ms > now_ms
    {
        return Err(AuthError::InvalidToken);
    }

    if let Some(required) = &self.required_scope
        && !self.scope_contains(&claims, required)
    {
        return Err(AuthError::InvalidToken);
    }

    let name = claims
        .get(&self.principal_claim_name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or(AuthError::InvalidToken)?
        .to_string();

    Ok(AuthOutcome {
        principal: Principal {
            name,
            auth_method: AuthMethod::SaslOAuthBearer,
        },
        expires_at_ms: Some(exp_ms),
    })
}
```

- [ ] **Step 4: Refactor `SignedJwsValidator::validate` + `check_claims`**

Both delegate. Change `check_claims` (lines ~346–394) to return `Result<AuthOutcome, AuthError>`:

```rust
fn check_claims(&self, claims: &Value, now_ms: i64) -> Result<AuthOutcome, AuthError> {
    let exp_ms = numeric_date_ms(claims, "exp").ok_or(AuthError::InvalidToken)?;
    if exp_ms + self.allowable_clock_skew_ms <= now_ms {
        return Err(AuthError::InvalidToken);
    }
    if let Some(iat_ms) = numeric_date_ms(claims, "iat")
        && iat_ms - self.allowable_clock_skew_ms > now_ms
    {
        return Err(AuthError::InvalidToken);
    }
    if let Some(nbf_ms) = numeric_date_ms(claims, "nbf")
        && nbf_ms - self.allowable_clock_skew_ms > now_ms
    {
        return Err(AuthError::InvalidToken);
    }

    if let Some(expected) = &self.valid_issuer
        && claims.get("iss").and_then(Value::as_str) != Some(expected.as_str())
    {
        return Err(AuthError::InvalidToken);
    }

    if let Some(expected) = &self.expected_audience
        && !audience_contains(claims, expected)
    {
        return Err(AuthError::InvalidToken);
    }

    if let Some(required) = &self.required_scope
        && !scope_claim_contains(claims, &self.scope_claim_name, required)
    {
        return Err(AuthError::InvalidToken);
    }

    let name = claims
        .get(&self.principal_claim_name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or(AuthError::InvalidToken)?
        .to_string();

    Ok(AuthOutcome {
        principal: Principal {
            name,
            auth_method: AuthMethod::SaslOAuthBearer,
        },
        expires_at_ms: Some(exp_ms),
    })
}
```

Change `SignedJwsValidator::validate` signature (line 311) — only the return type changes:

```rust
pub fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
```

(The body's last line `self.check_claims(&claims, now_ms)` already returns whatever `check_claims` returns; no further body change needed.)

- [ ] **Step 5: Refactor `IntrospectionValidator::validate`**

Edit lines ~508–541. The introspection response's `exp` is in seconds (RFC 7662). Need to extract it after `check_temporal_claims` succeeds and convert to ms:

```rust
pub async fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
    let mut claims = self
        .client
        .introspect(token)
        .await
        .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?;
    if claims.get("active").and_then(Value::as_bool) != Some(true) {
        return Err(AuthError::InvalidToken);
    }
    check_temporal_claims(&claims, now_ms, self.allowable_clock_skew_ms)?;
    let exp_ms = numeric_date_ms(&claims, "exp").ok_or(AuthError::InvalidToken)?;
    if self.call_userinfo
        && let Some(ui) = self
            .client
            .userinfo(token)
            .await
            .map_err(|e| AuthError::IntrospectionTransport(e.to_string()))?
    {
        merge_userinfo_over_introspection(&mut claims, ui);
    }
    check_required_scope(
        &claims,
        &self.scope_claim_name,
        self.required_scope.as_deref(),
    )?;
    let name = claims
        .get(&self.principal_claim_name)
        .and_then(Value::as_str)
        .ok_or(AuthError::InvalidToken)?
        .to_string();
    Ok(AuthOutcome {
        principal: Principal {
            name,
            auth_method: AuthMethod::SaslOAuthBearer,
        },
        expires_at_ms: Some(exp_ms),
    })
}
```

If `numeric_date_ms` is not in scope (it's used in oauthbearer.rs higher up but might not be imported at this position), add the import or full path.

**RFC 7662 note:** Introspection's `exp` is the authoritative session expiry. The userinfo endpoint typically doesn't carry `exp`; if it did, `merge_userinfo_over_introspection` would NOT override the introspection `exp` (per the existing function's merge precedence — verify by reading the function, but the principle is introspection wins for temporal claims). Capture `exp_ms` BEFORE the userinfo merge to make this explicit and avoid any merge-order surprise.

- [ ] **Step 6: Refactor the dispatch enum `OAuthBearerValidator::validate`**

Edit lines ~401–415. The enum just dispatches; signature change:

```rust
impl OAuthBearerValidator {
    pub async fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
        match self {
            Self::Unsecured(v) => v.validate(token, now_ms),
            Self::Signed(v) => v.validate(token, now_ms),
            Self::Introspection(v) => v.validate(token, now_ms).await,
        }
    }
}
```

- [ ] **Step 7: Re-export `AuthOutcome` from the crate root**

Edit `crates/security/src/lib.rs`. Find the existing `pub use ... oauthbearer::*` (or whichever line re-exports OAUTHBEARER types). Add `AuthOutcome` to whatever's re-exported:

```bash
grep -n "AuthOutcome\|OAuthBearerValidator\|oauthbearer::" crates/security/src/lib.rs | head
```

If `OAuthBearerValidator` is already re-exported, add `AuthOutcome` alongside it in the same `pub use` line.

- [ ] **Step 8: Add the equivalent failing tests for the signed + introspection validators**

In the same test module, beside step 1's test:

```rust
#[test]
fn signed_validate_surfaces_exp_in_auth_outcome() {
    // Use whichever fixture builder the existing signed-jws tests use to
    // construct a JWT with a known `exp` and a JWKS keypair the validator
    // can verify. Pattern: grep `fn signed_validate_` in the existing test
    // module for the canonical builder.
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_signed_jws_for_tests(&serde_json::json!({
        "sub": "alice",
        "exp": exp_secs,
        "iss": "https://test.example",
    }));
    let v = signed_validator_for_tests();
    let outcome = v.validate(&token, now_ms).expect("token valid");
    assert_eq!(outcome.principal.name, "alice");
    assert_eq!(outcome.expires_at_ms, Some(exp_secs * 1000));
}

#[tokio::test]
async fn introspection_validate_surfaces_exp_from_introspection_response() {
    // Use the fake introspection client pattern from slice 49d's tests.
    // Grep `IntrospectionValidator::new` or `IntrospectionClient` test
    // fixtures.
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let fake_client = FakeIntrospectionClient::with_response(serde_json::json!({
        "active": true,
        "sub": "alice",
        "exp": exp_secs,
        "scope": "kafka.write",
    }));
    let v = IntrospectionValidator::new_for_tests(fake_client);
    let outcome = v.validate("opaque-token", now_ms).await.expect("token valid");
    assert_eq!(outcome.principal.name, "alice");
    assert_eq!(outcome.expires_at_ms, Some(exp_secs * 1000));
}
```

**Adapt test helper names** by grepping the existing slice-49b/49d tests in the same file for actual fixture-builder names. Implementer judgment: match what's there.

- [ ] **Step 9: Update all existing tests in this file that call `.validate(...)` and expect `Principal`**

The existing tests in oauthbearer.rs were written against the `-> Principal` return. They'll fail to compile after the signature flip. Find them:

```bash
grep -n "\.validate(" crates/security/src/oauthbearer.rs
```

For each call site that does `let p = v.validate(...).expect(...)` or `assert_eq!(v.validate(...).unwrap(), Principal { ... })`, update to:

```rust
let outcome = v.validate(...).expect("token valid");
assert_eq!(outcome.principal, Principal { ... });
// (Keep any existing principal-comparison; just route through .principal.)
```

For tests asserting an error (`assert_eq!(v.validate(...).unwrap_err(), AuthError::InvalidToken)`), no change — only the success path changes.

- [ ] **Step 10: Update all other callers of `.validate(...)` in the workspace**

```bash
grep -rn "OAuthBearerValidator\|UnsecuredJwsValidator\|SignedJwsValidator\|IntrospectionValidator" crates/ --include="*.rs" | grep -v "^Binary" | grep -v "/src/oauthbearer.rs:"
```

The likely call site is `crates/broker/src/network/auth.rs` (in `handle_authenticate_oauthbearer`, which the explorer's report shows calls `validate_bearer` which in turn calls the validator). Trace and update:

- Find `validate_bearer` (probably in `auth.rs`).
- Its return type changes from `Result<Principal, AuthError>` to `Result<AuthOutcome, AuthError>`.
- Its single caller (`handle_authenticate_oauthbearer`) destructures the outcome:

  ```rust
  match validate_bearer(&req.auth_bytes, validator, now_ms).await {
      Ok(outcome) => {
          *auth = ConnectionAuth::Authenticated { principal: outcome.principal };
          SaslAuthenticateResponse {
              error_code: 0,
              error_message: None,
              auth_bytes: bytes::Bytes::new(),
              session_lifetime_ms: outcome
                  .expires_at_ms
                  .map_or(0, |e| (e - now_ms).max(0)),
              ..Default::default()
          }
      }
      Err(e) => { /* unchanged */ }
  }
  ```

  **Note:** This is a temporary state — T2 will rewire `Authenticated` to also carry `expires_at_ms` and `mechanism`. For T1, just plumb the value into `session_lifetime_ms` so the wire field starts populating; the connection state will get updated in T2.

- [ ] **Step 11: Run the workspace build**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49e-sasl-reauth
cargo build --workspace 2>&1 | tail -20
```

Expected: clean. If there are stragglers (a test in `crates/broker/tests/` or elsewhere that calls the validator), fix them with the same pattern.

- [ ] **Step 12: Run the new + all existing oauthbearer tests**

```bash
cargo test -p crabka-security oauthbearer 2>&1 | tail -10
cargo test -p crabka-broker auth 2>&1 | tail -10
```

Expected: all pass. The 3 new tests pass; existing tests still pass after the small per-test edits in step 9.

- [ ] **Step 13: fmt + clippy check on the touched files**

```bash
cargo fmt -p crabka-security -p crabka-broker -- --check
cargo clippy -p crabka-security -p crabka-broker --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 14: Commit**

```bash
git add crates/security/src/oauthbearer.rs crates/security/src/lib.rs crates/broker/src/network/auth.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T1: AuthOutcome + OAuth validator surface token expiry

Introduces `AuthOutcome { principal, expires_at_ms: Option<i64> }` in
crabka-security and flips OAuthBearerValidator::validate (and the three
concrete validators — unsecured, signed, RFC 7662 introspection) to
return AuthOutcome instead of bare Principal. Each validator already
extracted `exp` during temporal-claim checks; this just stops
discarding the value.

The broker's handle_authenticate_oauthbearer wires the outcome's
expires_at_ms into SaslAuthenticateResponse.session_lifetime_ms
(was always 0). The Authenticated connection state still only carries
the principal — T2 extends it to carry session state for the
per-connection re-auth timer.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 2 (sequential — T2 alone)

#### Task T2: ConnectionAuth + Reauthenticating + handler updates

**Files:**
- Modify: `crates/broker/src/network/auth.rs`

**Context:** T1 surfaced expiry into the response but didn't change connection state. T2 extends `ConnectionAuth::Authenticated` to carry the mechanism + expires_at_ms, adds a `Reauthenticating` variant for in-band re-auth, updates the pre-auth gate, and updates `SaslHandshake` + `SaslAuthenticate` handlers to accept the in-band re-auth flow with same-mechanism + same-principal enforcement.

- [ ] **Step 1: Write failing unit test for the extended `Authenticated` shape**

In `crates/broker/src/network/auth.rs`, in the `#[cfg(test)] mod tests` block (find an existing test of ConnectionAuth — or if no module exists, add one at the bottom of the file with `#[cfg(test)] mod tests { use super::*; ... }`):

```rust
#[test]
fn authenticated_state_carries_mechanism_and_expires_at_ms() {
    let auth = ConnectionAuth::Authenticated {
        principal: Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslOAuthBearer,
        },
        mechanism: SaslMechanism::OAuthBearer,
        expires_at_ms: Some(2_000_000),
    };
    match auth {
        ConnectionAuth::Authenticated { principal, mechanism, expires_at_ms } => {
            assert_eq!(principal.name, "alice");
            assert_eq!(mechanism, SaslMechanism::OAuthBearer);
            assert_eq!(expires_at_ms, Some(2_000_000));
        }
        _ => panic!("expected Authenticated"),
    }
}
```

- [ ] **Step 2: Run test — verify it fails to compile**

```bash
cargo test -p crabka-broker --lib network::auth::tests::authenticated_state_carries_mechanism_and_expires_at_ms 2>&1 | tail
```

Expected: compile error — `Authenticated` doesn't have `mechanism` or `expires_at_ms` fields.

- [ ] **Step 3: Extend the `Authenticated` variant**

Replace the existing `ConnectionAuth` enum (lines 31–43) with:

```rust
#[derive(Debug)]
pub enum ConnectionAuth {
    /// PLAINTEXT / SSL listener, or pre-handshake on a SASL listener.
    Anonymous,
    /// `SaslHandshake` received; awaiting (possibly multiple) `SaslAuthenticate`.
    Negotiating {
        mechanism: SaslMechanism,
        exchange: SaslExchange,
    },
    Authenticated {
        principal: Principal,
        /// SASL mechanism this connection authenticated with. Used by KIP-368
        /// in-band re-auth (slice 49e) to reject a fresh SaslHandshake that
        /// switches mechanisms mid-connection. For mTLS / anonymous
        /// connections (no SASL), this is `SaslMechanism::Plain` as a
        /// don't-care default (the in-band reauth path is unreachable since
        /// the listener doesn't accept SaslHandshake at all).
        mechanism: SaslMechanism,
        /// Session expiry as Unix epoch ms. `None` = no expiry / no re-auth
        /// timer (PLAIN/SCRAM/mTLS/anonymous). `Some` = OAUTHBEARER token's
        /// `exp`; the dispatch loop closes the connection when this elapses
        /// (slice 49e).
        expires_at_ms: Option<i64>,
    },
    /// In-band re-authentication in progress: a `SaslHandshake` from a
    /// previously `Authenticated` OAuth connection. Holds the previous
    /// session snapshot so the post-validate equality check (same principal
    /// name, same mechanism) has something to compare against, and so a
    /// failed re-auth's error message can reference the still-current
    /// principal. (Slice 49e; KIP-368.)
    Reauthenticating {
        previous: AuthenticatedSnapshot,
        exchange: SaslExchange,
    },
}

/// Snapshot of an `Authenticated` connection at the moment a re-auth
/// `SaslHandshake` arrives. Used by the `SaslAuthenticate` handler during
/// re-auth to enforce same-mechanism + same-principal-name semantics
/// (KIP-368).
#[derive(Debug, Clone)]
pub struct AuthenticatedSnapshot {
    pub principal: Principal,
    pub mechanism: SaslMechanism,
    pub expires_at_ms: Option<i64>,
}
```

- [ ] **Step 4: Update all existing call sites that construct `Authenticated` (without the new fields)**

```bash
grep -rn "ConnectionAuth::Authenticated" crates/ --include="*.rs"
```

For each construction site, add the two new fields. Examples:

- In `dispatch.rs` (mTLS init): `ConnectionAuth::Authenticated { principal, mechanism: SaslMechanism::Plain, expires_at_ms: None }` (the mechanism is a don't-care; pick Plain as the inert default — see the doc comment in the enum).
- In `dispatch.rs` (PLAINTEXT anonymous): same `{ mechanism: SaslMechanism::Plain, expires_at_ms: None }`.
- In `auth.rs` (PLAIN/SCRAM success): `{ principal, mechanism: <actual mechanism>, expires_at_ms: None }`.
- In `auth.rs` (OAUTHBEARER success from T1): `{ principal, mechanism: SaslMechanism::OAuthBearer, expires_at_ms: outcome.expires_at_ms }`.

For destructuring sites that do `match auth { Authenticated { principal } => ... }`, use `Authenticated { principal, .. }` to ignore the new fields.

- [ ] **Step 5: Run test — verify it now passes**

```bash
cargo test -p crabka-broker --lib network::auth::tests::authenticated_state_carries_mechanism_and_expires_at_ms 2>&1 | tail
```

Expected: PASS.

- [ ] **Step 6: Failing test for in-band re-auth handshake (same mechanism)**

Add to the same test module:

```rust
#[test]
fn handshake_from_authenticated_with_same_mechanism_transitions_to_reauthenticating() {
    let mut auth = ConnectionAuth::Authenticated {
        principal: Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslOAuthBearer,
        },
        mechanism: SaslMechanism::OAuthBearer,
        expires_at_ms: Some(2_000_000),
    };
    let req = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let resp = handle_handshake(&req, &mut auth, &[SaslMechanism::OAuthBearer]);
    assert_eq!(resp.error_code, 0);
    assert!(matches!(auth, ConnectionAuth::Reauthenticating {
        previous: AuthenticatedSnapshot { mechanism: SaslMechanism::OAuthBearer, .. },
        ..
    }));
}

#[test]
fn handshake_from_authenticated_with_different_mechanism_rejected_with_illegal_sasl_state() {
    let mut auth = ConnectionAuth::Authenticated {
        principal: Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslOAuthBearer,
        },
        mechanism: SaslMechanism::OAuthBearer,
        expires_at_ms: Some(2_000_000),
    };
    let req = SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-512".to_string(),
        ..Default::default()
    };
    let resp = handle_handshake(
        &req,
        &mut auth,
        &[SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512],
    );
    // ILLEGAL_SASL_STATE = 34 per Apache Kafka protocol.
    assert_eq!(resp.error_code, 34);
    // The state stays Authenticated (not transitioned).
    assert!(matches!(auth, ConnectionAuth::Authenticated { .. }));
}
```

- [ ] **Step 7: Run tests — verify they fail**

```bash
cargo test -p crabka-broker --lib network::auth::tests::handshake_from_authenticated 2>&1 | tail
```

Expected: FAIL. The existing `handle_handshake` rejects when called on non-`Anonymous`.

- [ ] **Step 8: Update `handle_handshake` to accept Authenticated → Reauthenticating**

Edit `crates/broker/src/network/auth.rs`, `handle_handshake` function (lines ~110–155). Add a branch at the top that handles the `Authenticated` case BEFORE the existing match on `requested`:

```rust
pub fn handle_handshake(
    req: &SaslHandshakeRequest,
    auth: &mut ConnectionAuth,
    enabled: &[SaslMechanism],
) -> SaslHandshakeResponse {
    let enabled_names: Vec<String> = enabled.iter().map(|m| m.wire_name().to_string()).collect();
    let requested = SaslMechanism::from_wire(&req.mechanism);

    // Slice 49e: in-band re-auth on an already-authenticated OAUTHBEARER
    // connection. Per KIP-368, only the same mechanism is allowed; a
    // mismatch is ILLEGAL_SASL_STATE.
    if let ConnectionAuth::Authenticated { mechanism: current, .. } = auth {
        let current = *current;
        match requested {
            Some(m) if m == current => {
                // OK: snapshot the previous Authenticated and transition.
                let prev = std::mem::replace(auth, ConnectionAuth::Anonymous);
                let ConnectionAuth::Authenticated { principal, mechanism, expires_at_ms } = prev
                else {
                    unreachable!("matched Authenticated above");
                };
                let exchange = exchange_for_mechanism(m);
                *auth = ConnectionAuth::Reauthenticating {
                    previous: AuthenticatedSnapshot { principal, mechanism, expires_at_ms },
                    exchange,
                };
                return SaslHandshakeResponse {
                    error_code: 0,
                    mechanisms: enabled_names,
                    ..Default::default()
                };
            }
            _ => {
                // Mechanism switch attempted — reject without transition.
                return SaslHandshakeResponse {
                    // ILLEGAL_SASL_STATE per Apache Kafka protocol.
                    error_code: 34,
                    mechanisms: enabled_names,
                    ..Default::default()
                };
            }
        }
    }

    // Existing path for Anonymous: initial handshake.
    match requested {
        Some(m) if enabled.contains(&m) => {
            let exchange = exchange_for_mechanism(m);
            *auth = ConnectionAuth::Negotiating { mechanism: m, exchange };
            SaslHandshakeResponse {
                error_code: 0,
                mechanisms: enabled_names,
                ..Default::default()
            }
        }
        // ... existing rejection branches unchanged ...
    }
}

/// Build the per-mechanism `SaslExchange` initial state. Extracted from
/// `handle_handshake` so both the initial-auth path and the re-auth path
/// can construct it identically.
fn exchange_for_mechanism(m: SaslMechanism) -> SaslExchange {
    match m {
        SaslMechanism::Plain => SaslExchange::Plain,
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => SaslExchange::ScramPending,
        SaslMechanism::OAuthBearer => SaslExchange::OAuthBearer,
    }
}
```

**Implementer judgment:** the existing `handle_handshake` body has the same per-mechanism switch inline; lift it to the new `exchange_for_mechanism` helper rather than duplicating.

- [ ] **Step 9: Run handshake tests — verify they pass**

```bash
cargo test -p crabka-broker --lib network::auth::tests::handshake_from_authenticated 2>&1 | tail
```

Expected: PASS.

- [ ] **Step 10: Failing test for re-auth `SaslAuthenticate` success path (same principal)**

```rust
#[tokio::test]
async fn authenticate_during_reauth_same_principal_transitions_back_to_authenticated() {
    let mut auth = ConnectionAuth::Reauthenticating {
        previous: AuthenticatedSnapshot {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: AuthMethod::SaslOAuthBearer,
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
        },
        exchange: SaslExchange::OAuthBearer,
    };
    let validator = make_test_oauthbearer_validator_for_alice_with_exp(3_000_000);
    let token_bytes = make_test_oauthbearer_client_first(/* alice token with exp=3000s */);
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(token_bytes),
        ..Default::default()
    };
    let resp = handle_authenticate_oauthbearer(&req, &mut auth, &validator, 1_500_000).await;
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.session_lifetime_ms, 3_000_000 - 1_500_000);
    assert!(matches!(
        auth,
        ConnectionAuth::Authenticated {
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(3_000_000),
            ..
        }
    ));
}

#[tokio::test]
async fn authenticate_during_reauth_different_principal_rejected_with_sasl_auth_failed() {
    let mut auth = ConnectionAuth::Reauthenticating {
        previous: AuthenticatedSnapshot {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: AuthMethod::SaslOAuthBearer,
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
        },
        exchange: SaslExchange::OAuthBearer,
    };
    // Token belongs to "bob", not "alice".
    let validator = make_test_oauthbearer_validator_for_user_with_exp("bob", 3_000_000);
    let token_bytes = make_test_oauthbearer_client_first(/* bob token */);
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(token_bytes),
        ..Default::default()
    };
    let resp = handle_authenticate_oauthbearer(&req, &mut auth, &validator, 1_500_000).await;
    // SASL_AUTHENTICATION_FAILED = 58 per Apache Kafka protocol.
    assert_eq!(resp.error_code, 58);
    assert!(resp.error_message.as_deref().unwrap_or("").contains("principal"));
}
```

**Test fixture builders** (`make_test_oauthbearer_validator_for_alice_with_exp`, `make_test_oauthbearer_client_first`) — implementer judgment: match the pattern of existing OAUTHBEARER unit tests in this file (grep for `handle_authenticate_oauthbearer` in tests).

- [ ] **Step 11: Run tests — verify they fail**

```bash
cargo test -p crabka-broker --lib network::auth::tests::authenticate_during_reauth 2>&1 | tail
```

Expected: FAIL — `handle_authenticate_oauthbearer` doesn't recognize `Reauthenticating`.

- [ ] **Step 12: Update `handle_authenticate_oauthbearer` to handle Reauthenticating**

Edit lines ~304–353. Add a `Reauthenticating` arm to the top-level match:

```rust
pub async fn handle_authenticate_oauthbearer(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &crabka_security::OAuthBearerValidator,
    now_ms: i64,
) -> SaslAuthenticateResponse {
    match auth {
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearer,
            mechanism,
        } => {
            let mech = *mechanism;
            // ... existing logic from before, but using AuthOutcome from T1 ...
            match validate_bearer(&req.auth_bytes, validator, now_ms).await {
                Ok(outcome) => {
                    let session_ms = outcome
                        .expires_at_ms
                        .map_or(0, |e| (e - now_ms).max(0));
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: mech,
                        expires_at_ms: outcome.expires_at_ms,
                    };
                    SaslAuthenticateResponse {
                        error_code: 0,
                        error_message: None,
                        auth_bytes: bytes::Bytes::new(),
                        session_lifetime_ms: session_ms,
                        ..Default::default()
                    }
                }
                Err(e) => {
                    // ... existing error handling unchanged ...
                }
            }
        }
        ConnectionAuth::Reauthenticating {
            previous,
            exchange: SaslExchange::OAuthBearer,
        } => {
            let prev_mech = previous.mechanism;
            let prev_name = previous.principal.name.clone();
            match validate_bearer(&req.auth_bytes, validator, now_ms).await {
                Ok(outcome) => {
                    if outcome.principal.name != prev_name {
                        // Principal switch — reject and let dispatch close.
                        return SaslAuthenticateResponse {
                            error_code: 58, // SASL_AUTHENTICATION_FAILED
                            error_message: Some(
                                "re-authentication may not change the principal".to_string(),
                            ),
                            auth_bytes: bytes::Bytes::new(),
                            session_lifetime_ms: 0,
                            ..Default::default()
                        };
                    }
                    let session_ms = outcome
                        .expires_at_ms
                        .map_or(0, |e| (e - now_ms).max(0));
                    *auth = ConnectionAuth::Authenticated {
                        principal: outcome.principal,
                        mechanism: prev_mech,
                        expires_at_ms: outcome.expires_at_ms,
                    };
                    SaslAuthenticateResponse {
                        error_code: 0,
                        error_message: None,
                        auth_bytes: bytes::Bytes::new(),
                        session_lifetime_ms: session_ms,
                        ..Default::default()
                    }
                }
                Err(_e) => SaslAuthenticateResponse {
                    error_code: 58,
                    error_message: Some("re-authentication failed".to_string()),
                    auth_bytes: bytes::Bytes::new(),
                    session_lifetime_ms: 0,
                    ..Default::default()
                },
            }
        }
        // ... existing fallthrough for non-OAuthBearer SaslExchange ...
    }
}
```

The dispatch loop (T3) will detect that `error_code != 0` from a SASL handler and close the connection — same pattern as the existing OAUTHBEARER failure path.

- [ ] **Step 13: Update the pre-auth gate to handle Reauthenticating**

Find the dispatch site that calls `is_pre_auth_allowed`. It's currently a free function `pub fn is_pre_auth_allowed(api_key: i16) -> bool` at line 89–92. Replace with a method on `ConnectionAuth` that knows the state:

```rust
impl ConnectionAuth {
    /// Whether `api_key` may be served given the current auth state.
    /// - `Anonymous` / `Negotiating`: allow the pre-auth allowlist
    ///   (ApiVersions=18, SaslHandshake=17, SaslAuthenticate=36).
    /// - `Reauthenticating`: allow only `SaslAuthenticate=36`. Any other
    ///   request during in-band re-auth is a protocol violation and the
    ///   dispatch layer closes the connection (KIP-368).
    /// - `Authenticated`: allow everything.
    #[must_use]
    pub fn allows_request(&self, api_key: i16) -> bool {
        match self {
            Self::Anonymous | Self::Negotiating { .. } => is_pre_auth_allowed(api_key),
            Self::Reauthenticating { .. } => api_key == 36,
            Self::Authenticated { .. } => true,
        }
    }
}
```

Keep `is_pre_auth_allowed` as the helper. Add a unit test:

```rust
#[test]
fn allows_request_during_reauthenticating_only_sasl_authenticate() {
    let auth = ConnectionAuth::Reauthenticating {
        previous: AuthenticatedSnapshot {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: AuthMethod::SaslOAuthBearer,
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
        },
        exchange: SaslExchange::OAuthBearer,
    };
    assert!(auth.allows_request(36)); // SaslAuthenticate
    assert!(!auth.allows_request(17)); // SaslHandshake
    assert!(!auth.allows_request(18)); // ApiVersions
    assert!(!auth.allows_request(3));  // Metadata
}
```

The dispatch layer (currently uses `is_pre_auth_allowed` somewhere) will be updated in T3 to call `auth.allows_request(api_key)` instead.

- [ ] **Step 14: Run full auth test suite**

```bash
cargo test -p crabka-broker --lib network::auth 2>&1 | tail -15
```

Expected: all pass. Includes the 4 new tests (handshake same-mech, handshake diff-mech, authenticate same-principal, allows_request during reauth) + existing tests still green.

- [ ] **Step 15: fmt + clippy**

```bash
cargo fmt -p crabka-broker -- --check
cargo clippy -p crabka-broker --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 16: Commit**

```bash
git add crates/broker/src/network/auth.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T2: ConnectionAuth carries session state; Reauthenticating + handler updates

Extends Authenticated to carry { mechanism, expires_at_ms } so the
dispatch loop can schedule the re-auth deadline (T3) and the in-band
re-auth handler can enforce same-mechanism. New Reauthenticating
variant + AuthenticatedSnapshot carry the previous session's principal
across the in-band handshake → authenticate round-trip so the
SaslAuthenticate handler can reject principal switches with
SASL_AUTHENTICATION_FAILED.

handle_handshake now accepts an in-band re-auth handshake from
Authenticated → Reauthenticating; mechanism mismatch returns
ILLEGAL_SASL_STATE (34). handle_authenticate_oauthbearer handles
the Reauthenticating arm, validates same-principal, and transitions
back to Authenticated with the new expiry.

New ConnectionAuth::allows_request method replaces direct
is_pre_auth_allowed calls at the gate: during Reauthenticating, only
SaslAuthenticate (api_key=36) is allowed. T3 wires this into the
dispatch layer.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 3 (sequential — T3 alone)

#### Task T3: Dispatch loop `select!` + per-connection timer

**Files:**
- Modify: `crates/broker/src/network/dispatch.rs`

**Context:** The connection state now carries `expires_at_ms`, but the dispatch loop still just reads frames forever. T3 wraps the read in a `tokio::select!` that races against `tokio::time::sleep_until(deadline)`. When the deadline fires before the client re-auths, the loop breaks and the TCP socket closes. Also: switch the pre-auth gate from the free function `is_pre_auth_allowed` to the method `auth.allows_request(api_key)` (so Reauthenticating gates properly).

- [ ] **Step 1: Add a `sleep_until_some` helper**

Edit `crates/broker/src/network/dispatch.rs`. Near the top of the file (after imports, before `serve_connection_stream`), add:

```rust
/// Returns a future that resolves at `deadline` if `Some`, or never resolves
/// if `None`. Used in `tokio::select!` to disarm the timer arm for non-OAuth
/// connections (which have no session expiry).
async fn sleep_until_some(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}
```

- [ ] **Step 2: Add a helper to convert epoch-ms → tokio Instant**

In the same file, near the other helpers:

```rust
/// Convert an "expires-at as Unix epoch ms" into a `tokio::time::Instant`
/// suitable for `sleep_until`. Computes the delta against the current wall
/// clock and adds to `Instant::now()`; tests using `tokio::time::pause` can
/// then advance the tokio clock to fire the deadline deterministically.
fn instant_at_epoch_ms(epoch_ms: i64) -> tokio::time::Instant {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_i64, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let delta_ms = (epoch_ms - now_ms).max(0);
    tokio::time::Instant::now() + std::time::Duration::from_millis(delta_ms as u64)
}
```

- [ ] **Step 3: Refactor the per-connection read loop into a `select!`**

Find the existing loop in `serve_connection_stream` (lines ~168–185 per the explorer's report):

```rust
while let Some(frame) = framed.next().await {
    let frame = match frame { ... };
    // per-request handling ...
}
```

Replace with:

```rust
loop {
    // Compute the re-auth deadline for OAUTHBEARER connections. PLAIN/SCRAM/
    // anonymous return None and the timer arm is effectively disabled.
    let deadline: Option<tokio::time::Instant> = match &auth {
        crate::network::auth::ConnectionAuth::Authenticated {
            expires_at_ms: Some(exp_ms),
            ..
        } => Some(instant_at_epoch_ms(*exp_ms)),
        // During Reauthenticating, keep the previous deadline so a slow
        // re-auth attempt can't extend the session by sitting in the
        // Reauthenticating state past the original expiry.
        crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => previous
            .expires_at_ms
            .map(instant_at_epoch_ms),
        _ => None,
    };

    let frame_result = tokio::select! {
        biased;
        next = framed.next() => next,
        _ = sleep_until_some(deadline) => {
            tracing::info!(
                principal = ?auth_principal_name(&auth),
                "SASL session expired, closing connection (KIP-368)"
            );
            break;
        }
    };

    let frame = match frame_result {
        Some(Ok(b)) => b,
        Some(Err(e)) => {
            tracing::warn!(error = %e, "frame decode error, closing");
            break;
        }
        None => break, // EOF
    };

    // ... rest of per-request handling unchanged (the existing span /
    // dispatch / response-write code; just lift its body verbatim from
    // the old `while let Some(frame) = ...` block) ...
}
```

`biased` ensures that if both `framed.next()` and the timer are ready in the same poll, the request wins — letting the last in-flight request before expiry complete normally per KIP-368 spirit.

- [ ] **Step 4: Add `auth_principal_name` helper (or inline equivalent)**

For the tracing log line:

```rust
fn auth_principal_name(auth: &crate::network::auth::ConnectionAuth) -> Option<&str> {
    match auth {
        crate::network::auth::ConnectionAuth::Authenticated { principal, .. } => {
            Some(principal.name.as_str())
        }
        crate::network::auth::ConnectionAuth::Reauthenticating { previous, .. } => {
            Some(previous.principal.name.as_str())
        }
        _ => None,
    }
}
```

- [ ] **Step 5: Switch the pre-auth gate to `auth.allows_request(api_key)`**

Find the existing site in `dispatch.rs` (or `auth.rs`) where `is_pre_auth_allowed(api_key)` is called against the dispatch loop's auth state:

```bash
grep -n "is_pre_auth_allowed\|allows_request" crates/broker/src/network/
```

Replace the call with `auth.allows_request(api_key)`. The condition logic stays the same shape:

```rust
// Before:
if !matches!(auth, ConnectionAuth::Authenticated { .. }) && !is_pre_auth_allowed(api_key) {
    // reject with ILLEGAL_SASL_STATE
}

// After:
if !auth.allows_request(api_key) {
    // reject with ILLEGAL_SASL_STATE
}
```

`allows_request` already encodes the Authenticated-allows-everything branch, so the outer `!matches!(Authenticated)` check goes away.

- [ ] **Step 6: Update `handle_authenticate_oauthbearer` call site to pass through `now_ms` consistently**

The dispatch site at line ~1124 already computes `now_ms` and calls `handle_authenticate_oauthbearer`. Verify no change needed (T2's handler already returns the right state). If the inline `now_ms` computation can share with the `instant_at_epoch_ms` clock, leave them separate for now — clock unification is out of scope for this slice.

- [ ] **Step 7: Build the workspace**

```bash
cargo build --workspace 2>&1 | tail
```

Expected: clean.

- [ ] **Step 8: Run all broker auth + dispatch tests**

```bash
cargo test -p crabka-broker --lib network 2>&1 | tail -15
cargo test -p crabka-broker --test auth_handlers 2>&1 | tail -10
```

Expected: all pass. No new tests in this task (T4 has them); just confirm the refactor doesn't break existing tests.

- [ ] **Step 9: fmt + clippy**

```bash
cargo fmt -p crabka-broker -- --check
cargo clippy -p crabka-broker --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add crates/broker/src/network/dispatch.rs crates/broker/src/network/auth.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T3: dispatch loop select! with per-connection re-auth deadline

Wraps the per-connection read in a tokio::select! that races
framed.next() against sleep_until(deadline), where deadline is derived
from ConnectionAuth::Authenticated.expires_at_ms (slice 49e / KIP-368).
PLAIN/SCRAM/anonymous connections return None and the timer arm is
effectively disabled via std::future::pending().

`biased;` ensures the read arm wins ties so the last in-flight request
before expiry completes normally. During Reauthenticating, the deadline
stays pinned to the previous expires_at_ms so a slow re-auth can't
extend the session by sitting in the in-progress state past the
original expiry.

Switches the pre-auth gate from `is_pre_auth_allowed(api_key)` to
`auth.allows_request(api_key)`, picking up T2's per-state gate logic
(Reauthenticating allows only SaslAuthenticate=36).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 4 (sequential — T4 alone)

#### Task T4: Integration tests for KIP-368 scenarios

**Files:**
- Modify: `crates/broker/tests/auth_handlers.rs`

**Context:** T1/T2/T3 changed wire field, state machine, and dispatch loop. T4 proves the end-to-end behavior with 6 integration scenarios using a new `drive_sasl_oauthbearer_session` helper and `tokio::time::pause` for deterministic timer control.

- [ ] **Step 1: Add the `drive_sasl_oauthbearer_session` helper**

Edit `crates/broker/tests/auth_handlers.rs`. After the existing `drive_sasl_plain_session` (around line 466), add a parallel helper for OAUTHBEARER:

```rust
/// Drive a SASL_PLAINTEXT OAUTHBEARER handshake to completion on `stream`.
/// Returns the open stream + the `session_lifetime_ms` from the
/// `SaslAuthenticateResponse` so tests can assert on the timer and continue
/// using the connection (for the in-band re-auth scenarios).
///
/// `bearer_token` is the JWS string (for unsecured tests, an `alg:none` JWT
/// with the desired `exp` claim). The function frames the RFC 7628 client-
/// first message wrapping the token.
async fn drive_sasl_oauthbearer_session_open(
    addr: SocketAddr,
    bearer_token: &str,
) -> Result<(TcpStream, i64), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp;
    let _ = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // ── 2. SaslHandshake v1 (mechanism = OAUTHBEARER)
    let sh_req = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req
        .encode(&mut sh_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake error_code={}",
            sh_resp.error_code
        )));
    }

    // ── 3. SaslAuthenticate v1, RFC 7628 client-first wrapping the bearer.
    //    Frame:  n,a=,\x01auth=Bearer <token>\x01\x01
    let client_first = format!("n,a=,\x01auth=Bearer {}\x01\x01", bearer_token);
    let sa_req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_first.into_bytes()),
        ..Default::default()
    };
    let mut sa_body = BytesMut::new();
    sa_req
        .encode(&mut sa_body, 1)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let sa_resp_bytes = round_trip(&mut stream, 36, 1, 3, false, &sa_body).await?;
    let mut cur: &[u8] = &sa_resp_bytes;
    let sa_resp = SaslAuthenticateResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if sa_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate error_code={} message={:?}",
            sa_resp.error_code, sa_resp.error_message
        )));
    }

    Ok((stream, sa_resp.session_lifetime_ms))
}
```

Adapt the framing details (round_trip signature, correlation_id allocation) to whatever `drive_sasl_plain_session` actually uses — the implementer should literally copy the PLAIN helper and swap mechanism + auth_bytes.

- [ ] **Step 2: Add a fixture builder for unsecured-JWS broker config + token**

Add helpers (or extend `BrokerConfig::for_tests`) at the bottom of the test file:

```rust
/// Build a BrokerConfig wired for SASL_PLAINTEXT + OAUTHBEARER with an
/// unsecured-JWS validator configured for `sub`-claim principals.
fn oauthbearer_broker_config_for_tests(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
    cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Unsecured(
        crabka_security::UnsecuredJwsValidator::new_for_tests("sub", /* skew_ms */ 0),
    );
    cfg
}

/// Build an unsecured-JWS bearer token with `sub` = `user` and `exp` (seconds
/// since epoch) = `exp_secs`.
fn make_unsecured_bearer_token(user: &str, exp_secs: i64) -> String {
    let header = serde_json::json!({ "alg": "none", "typ": "JWT" });
    let payload = serde_json::json!({ "sub": user, "exp": exp_secs });
    let b = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    format!(
        "{}.{}.",
        b.encode(serde_json::to_vec(&header).unwrap()),
        b.encode(serde_json::to_vec(&payload).unwrap()),
    )
}
```

`UnsecuredJwsValidator::new_for_tests` — implementer judgment: if such a helper exists, reuse; otherwise construct manually with the public constructor + adjust skew.

- [ ] **Step 3: Test #1 — `oauthbearer_session_lifetime_ms_set_from_token_exp`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_lifetime_ms_set_from_token_exp() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = oauthbearer_broker_config_for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // exp = now + 600s.
    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let exp_secs = now_s + 600;
    let token = make_unsecured_bearer_token("alice", exp_secs);

    let (stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");
    drop(stream);

    // Should be ≈600_000 ms; allow generous wall-clock slop.
    assert!(session_lifetime_ms > 590_000 && session_lifetime_ms < 605_000,
        "session_lifetime_ms = {session_lifetime_ms}, expected ~600_000");

    handle.shutdown().await;
}
```

- [ ] **Step 4: Test #2 — `oauthbearer_session_expires_closes_connection`**

```rust
#[tokio::test(flavor = "multi_thread", start_paused = true)]
async fn oauthbearer_session_expires_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = oauthbearer_broker_config_for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let exp_secs = now_s + 60; // 60-second token.
    let token = make_unsecured_bearer_token("alice", exp_secs);

    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Advance tokio clock past expiry. Wall clock (SystemTime) does NOT
    // advance under start_paused, but the dispatch loop's deadline was
    // computed from tokio::Instant::now() + (exp_ms - sys_now_ms), so
    // advancing tokio's clock past that delta fires the sleep_until.
    tokio::time::advance(std::time::Duration::from_secs(61)).await;

    // After expiry the broker should close. Read should EOF.
    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.read(&mut buf),
    )
    .await
    .expect("read should not hang")
    .expect("read should not error");
    assert_eq!(n, 0, "expected EOF after session expiry, got {n} bytes");

    handle.shutdown().await;
}
```

- [ ] **Step 5: Test #3 — `oauthbearer_in_band_reauth_with_fresh_token_resets_timer`**

```rust
#[tokio::test(flavor = "multi_thread", start_paused = true)]
async fn oauthbearer_in_band_reauth_with_fresh_token_resets_timer() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = oauthbearer_broker_config_for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let token_a = make_unsecured_bearer_token("alice", now_s + 60);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_a)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // Advance to 30s. Token A is still valid (exp = now+60s).
    tokio::time::advance(std::time::Duration::from_secs(30)).await;

    // Re-auth in-band with a fresh token (exp = now+120s).
    let token_b = make_unsecured_bearer_token("alice", now_s + 120);
    drive_inband_reauth(&mut stream, &token_b)
        .await
        .expect("in-band re-auth must succeed");

    // Advance to 35s past original token-A expiry (65s total).
    tokio::time::advance(std::time::Duration::from_secs(35)).await;

    // Connection must still be open — issue a Metadata RPC.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 0).unwrap();
    let md_resp_bytes = round_trip(&mut stream, 3, 0, 99, false, &md_body)
        .await
        .expect("Metadata RPC must succeed past original token expiry");
    let mut cur: &[u8] = &md_resp_bytes;
    let _ = MetadataResponse::decode(&mut cur, 0)
        .expect("Metadata decode must succeed");

    handle.shutdown().await;
}

/// Drive a SASL_HANDSHAKE + SASL_AUTHENTICATE pair on an already-
/// authenticated `stream`, swapping the bearer token. Used for KIP-368
/// in-band re-authentication scenarios.
async fn drive_inband_reauth(
    stream: &mut TcpStream,
    new_token: &str,
) -> Result<(), io::Error> {
    let sh_req = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req.encode(&mut sh_body, 1).unwrap();
    let sh_resp_bytes = round_trip(stream, 17, 1, 100, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1).unwrap();
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslHandshake error_code={}", sh_resp.error_code
        )));
    }

    let client_first = format!("n,a=,\x01auth=Bearer {}\x01\x01", new_token);
    let sa_req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_first.into_bytes()),
        ..Default::default()
    };
    let mut sa_body = BytesMut::new();
    sa_req.encode(&mut sa_body, 1).unwrap();
    let sa_resp_bytes = round_trip(stream, 36, 1, 101, false, &sa_body).await?;
    let mut cur: &[u8] = &sa_resp_bytes;
    let sa_resp = SaslAuthenticateResponse::decode(&mut cur, 1).unwrap();
    if sa_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslAuthenticate error_code={} message={:?}",
            sa_resp.error_code, sa_resp.error_message
        )));
    }
    Ok(())
}
```

- [ ] **Step 6: Test #4 — `oauthbearer_in_band_reauth_with_different_principal_closes`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_principal_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = oauthbearer_broker_config_for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let token_alice = make_unsecured_bearer_token("alice", now_s + 300);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_alice)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // Attempt re-auth with a token belonging to "bob".
    let token_bob = make_unsecured_bearer_token("bob", now_s + 300);
    let result = drive_inband_reauth(&mut stream, &token_bob).await;
    // Should error with SASL_AUTHENTICATION_FAILED in the response.
    let err = result.expect_err("re-auth with different principal must fail");
    assert!(err.to_string().contains("58"), "expected SASL_AUTHENTICATION_FAILED (58); got {err}");

    // Connection closes after the error response.
    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.read(&mut buf),
    )
    .await
    .expect("read should not hang")
    .expect("read should not error");
    assert_eq!(n, 0, "expected EOF after failed re-auth");

    handle.shutdown().await;
}
```

- [ ] **Step 7: Test #5 — `oauthbearer_in_band_reauth_with_different_mechanism_closes`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_mechanism_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = oauthbearer_broker_config_for_tests(log_dir.path().to_path_buf());
    // Enable both mechanisms so the broker WOULD accept SCRAM if it weren't
    // for the re-auth same-mechanism rule.
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512];
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let now_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let token = make_unsecured_bearer_token("alice", now_s + 300);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // Send a SaslHandshake with SCRAM-SHA-512 — should be rejected
    // with ILLEGAL_SASL_STATE (34).
    let sh_req = SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-512".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req.encode(&mut sh_body, 1).unwrap();
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 200, false, &sh_body)
        .await
        .expect("frame round-trip");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1).unwrap();
    assert_eq!(sh_resp.error_code, 34, "expected ILLEGAL_SASL_STATE");

    handle.shutdown().await;
}
```

- [ ] **Step 8: Test #6 — `plain_listener_session_lifetime_ms_is_zero_and_no_timer`**

```rust
#[tokio::test(flavor = "multi_thread", start_paused = true)]
async fn plain_listener_session_lifetime_ms_is_zero_and_no_timer() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials.insert("alice".into(), "wonderland".into());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // PLAIN handshake using existing helper (returns nothing if successful).
    drive_sasl_plain_session(addr, "alice", b"wonderland")
        .await
        .expect("PLAIN handshake must succeed");

    // Connection from drive_sasl_plain_session is dropped; that's fine —
    // the assertion we care about is that the broker accepted PLAIN without
    // setting up a timer. Re-open and assert the response field directly.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    // ... full PLAIN frame round-trip, capturing the SaslAuthenticateResponse ...
    // session_lifetime_ms must be 0.
    // (See drive_sasl_plain_session for the framing; for this test, inline
    // the round-trips and capture the decoded response.)
    //
    // Then advance tokio clock by a long duration and verify the connection
    // is still open (a Metadata RPC succeeds).
    tokio::time::advance(std::time::Duration::from_secs(3600)).await;

    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 0).unwrap();
    let md_resp_bytes = round_trip(&mut stream, 3, 0, 5, false, &md_body)
        .await
        .expect("Metadata RPC on PLAIN connection must succeed an hour after auth");
    let mut cur: &[u8] = &md_resp_bytes;
    let _ = MetadataResponse::decode(&mut cur, 0).unwrap();

    handle.shutdown().await;
}
```

**Implementer judgment:** the test body has a "inline the round-trips" comment — the implementer fills in the same 3-step pattern from `drive_sasl_plain_session` to capture the response and assert `session_lifetime_ms == 0`. Alternatively, extend `drive_sasl_plain_session` to return the response struct.

- [ ] **Step 9: Run all 6 tests**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49e-sasl-reauth
cargo test -p crabka-broker --test auth_handlers oauthbearer 2>&1 | tail -15
cargo test -p crabka-broker --test auth_handlers plain_listener_session 2>&1 | tail -10
```

Expected: all 6 pass.

- [ ] **Step 10: Run full broker test suite to confirm no regressions**

```bash
cargo test -p crabka-broker 2>&1 | tail
```

Expected: all pass. Known pre-existing flake: `auto_rebalance_restores_preferred_leader` in `elect_leaders.rs` can time out under parallel load. If it fires, re-run in isolation (it's documented in slice 50c notes).

- [ ] **Step 11: fmt + clippy**

```bash
cargo fmt -p crabka-broker -- --check
cargo clippy -p crabka-broker --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 12: Commit**

```bash
git add crates/broker/tests/auth_handlers.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T4: integration tests for KIP-368 OAUTHBEARER re-authentication

Six end-to-end scenarios using a new drive_sasl_oauthbearer_session_open
helper + tokio::time::pause/advance for deterministic timer control:

- session_lifetime_ms populated from the token's `exp` claim
- timer fires past expiry: broker closes the TCP connection (EOF)
- in-band re-auth with a fresh token resets the timer (Metadata
  RPC succeeds past the original token's `exp`)
- in-band re-auth with a different principal name returns
  SASL_AUTHENTICATION_FAILED (58) and the connection closes
- in-band re-auth attempting to switch SASL mechanism is rejected
  with ILLEGAL_SASL_STATE (34)
- PLAIN-listener regression check: session_lifetime_ms = 0 and the
  connection stays open an hour past auth (no timer scheduled)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 5 (sequential — T5 alone)

#### Task T5: STATUS.md entry + final gate

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Read slice 49d's STATUS entry for tone**

```bash
grep -n "^## Slice 49d " STATUS.md
# Then read that section (use the line numbers from grep) — ~50-70 lines.
```

- [ ] **Step 2: Append a `## Slice 49e — Broker: SASL re-authentication (KIP-368) (2026-05-24)` section**

Add at the end of STATUS.md. Roughly 45-55 lines, mirroring 49d's structure:

- **Opener (2-3 sentences):** Bounds an OAUTHBEARER SASL session by the token's `exp`. Server populates `SaslAuthenticateResponse.session_lifetime_ms`; per-connection `tokio::select!` timer closes the connection at expiry; in-band re-auth (fresh `SaslHandshake`/`SaslAuthenticate` on an already-authenticated connection) refreshes the session without dropping the TCP stream.
- **Validator surface (`crates/security/src/oauthbearer.rs`):** New `AuthOutcome { principal, expires_at_ms }`. `OAuthBearerValidator::validate` returns `AuthOutcome` instead of bare `Principal`. All three concrete validators (unsecured JWS, signed JWKS, RFC 7662 introspection) surface the token's `exp` (each already extracted it during temporal-claim checks — this just stops discarding the value).
- **Connection state (`crates/broker/src/network/auth.rs`):** `ConnectionAuth::Authenticated` extended with `{ mechanism: SaslMechanism, expires_at_ms: Option<i64> }`. New `Reauthenticating { previous: AuthenticatedSnapshot, exchange: SaslExchange }` variant. New `ConnectionAuth::allows_request(api_key)` method gates the dispatch loop's pre-auth allowlist per state — during `Reauthenticating`, only `SaslAuthenticate=36` is accepted.
- **Handler updates:** `handle_handshake` now accepts an in-band handshake from `Authenticated`. Same-mechanism enforced — mismatch returns `ILLEGAL_SASL_STATE (34)`. `handle_authenticate_oauthbearer` handles the `Reauthenticating` arm: same-principal-name enforced — mismatch returns `SASL_AUTHENTICATION_FAILED (58)` with message "re-authentication may not change the principal".
- **Dispatch loop (`crates/broker/src/network/dispatch.rs`):** Per-connection read becomes `tokio::select! { biased; next = framed.next() => ..., _ = sleep_until_some(deadline) => break }`. `deadline` is derived from `Authenticated.expires_at_ms` (or `Reauthenticating.previous.expires_at_ms` so a slow re-auth can't extend the session). `biased` makes the read arm win ties so the last in-flight request before expiry completes. Non-OAuth connections return `None` from the deadline derivation and the timer arm is disarmed via `std::future::pending()`.
- **Tests:** 3 new unit tests in `crates/security/src/oauthbearer.rs::tests` (each validator surfaces `exp`); 4 new unit tests in `crates/broker/src/network/auth.rs::tests` (`Authenticated` shape; in-band handshake same-mech + diff-mech; in-band authenticate same-principal; `allows_request` during `Reauthenticating`). 6 new integration tests in `crates/broker/tests/auth_handlers.rs` (session lifetime, timer fire, in-band re-auth happy path, in-band re-auth principal-switch reject, in-band re-auth mechanism-switch reject, PLAIN regression).
- **Reference doc:** `[docs/superpowers/specs/2026-05-24-crabka-broker-sasl-reauth-49e-design.md]`
- **Out of scope:**
  - Mechanism-agnostic `connections.max.reauth.ms` broker config (would gate PLAIN/SCRAM too); not in OAUTHBEARER parity umbrella.
  - Operator-side `maxSecondsWithoutReauthentication` CRD field — slice 50d.
  - Server-side cap on `session_lifetime_ms` (`oauthbearer.max.session.lifetime.ms` defense-in-depth knob).
  - Server-side minimum check ("token too-short-lived, reject auth").
  - Client-side re-auth scheduler in Crabka's Kafka client crate; broker-only this slice.

- [ ] **Step 3: Final gate**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49e-sasl-reauth
cargo fmt --check 2>&1 | tail -3
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
cargo test --workspace 2>&1 | tail -20
```

Expected: all green. Known pre-existing flake: `auto_rebalance_restores_preferred_leader` in `elect_leaders.rs`. If it fires, re-run in isolation:

```bash
cargo test -p crabka-broker --test elect_leaders auto_rebalance_restores_preferred_leader 2>&1 | tail
```

If it still fires, document as pre-existing — not a T5 blocker.

This slice does NOT touch CRDs, so the CRD drift gate is unaffected; no need to run `tools/regen-crds.sh`.

- [ ] **Step 4: Commit**

```bash
git add STATUS.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
Slice 49e: STATUS.md entry + final gate

Documents the broker-side KIP-368 SASL re-authentication surface:
AuthOutcome on the OAuth validators, ConnectionAuth.Authenticated
carrying { mechanism, expires_at_ms }, Reauthenticating variant for
in-band re-auth, dispatch-loop select! timer that closes the connection
at session expiry. fmt + clippy + workspace tests all green.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Notes

- **Dependency chain:** T1 → T2 → T3 → T4 → T5. Five batches, five tasks, all sequential. Each task's input depends on the previous task's public surface (validator return shape → connection state shape → dispatch loop hooks → integration test fixtures).
- **No parallel batches in this slice.** All work converges on `auth.rs` / `dispatch.rs` / `oauthbearer.rs`, and each task's edits depend on its predecessor's type signatures. Slice 50c's 4-way parallel batches are not replicable here.
- **Greenfield compliance (CLAUDE.md):** Validator signature flip is a straight rename — no deprecation alias, no `#[serde(default)]` shim. `ConnectionAuth::Authenticated` gains required fields with no defaults; all construction sites are updated atomically in T2.
- **Test fixtures naming:** Several plan steps reference helper names like `UnsecuredJwsValidator::new_for_tests` or `make_unsecured_jws_for_tests` that the implementer should adapt to whatever exists in the existing oauthbearer.rs test module. Grep before inventing.
- **Clock injection:** No `BrokerClock` abstraction exists today (per the spec's deferral). T3 introduces `instant_at_epoch_ms` as a local helper in `dispatch.rs`; a workspace-wide `BrokerClock` is out of scope for this slice and can be lifted later if more callers need clock control.
- **JVM differential:** Not added — JVM admin tools don't exercise SASL re-auth. The 6 integration tests + e2e (deferred to slice 50d) cover the wire + the timer.
- **After 49e lands:** the umbrella's next pair is `50d` (operator surfaces `maxSecondsWithoutReauthentication` on the listener OAuth config). Then `49f + 50e` (PLAIN-with-OAuth-token, optional only if a user reports it). Then `49g + 50f` (claim enrichments + remaining Strimzi fields).
