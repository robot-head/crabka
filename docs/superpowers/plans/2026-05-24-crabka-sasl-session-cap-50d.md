# Slice 50d — SASL session-lifetime cap Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** ## Slice 50d — Operator + Broker: SASL session-lifetime cap (KIP-368 ceiling) (2026-05-24)

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Mechanism-agnostic connections.max.reauth.ms (would force re-auth on PLAIN/SCRAM)
- Per-listener divergent caps (still rejected as ConflictingOAuthListenerConfig)
- Client-side re-auth scheduler in Crabka's Kafka client crate (broker-only this slice)
- New e2e workflow job
- Semantic divergence from Strimzi (acknowledged): Strimzi's unset = no re-auth; Crabka 50d: unset = session = token exp

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bundle a server-side cap (`[oauthbearer].max_session_lifetime_seconds`) on top of slice 49e + surface Strimzi's `maxSecondsWithoutReauthentication` field on the listener OAuth CRD, so operators can clamp OAUTHBEARER sessions tighter than the token's natural `exp`.

**Architecture:** Six sequential tasks. T1 wires the broker (config key, handler clamp in both initial-auth and re-auth arms, `Authenticated.expires_at_ms` stores the CLAMPED value so the timer fires when the client was told). T2 + T3 each touch one operator file (CRD type, then reconciler render + divergence) — file-disjoint by design, but dispatched sequentially because T3's verify needs T2's struct field present. T4 + T5 run in parallel (operator integration tests + sample + CRD regen ‖ e2e job extension). T6 ships STATUS + final gate.

**Tech Stack:** Rust, tokio, serde (TOML for broker, YAML for operator), kube-rs, schemars (hand-rolled JSON Schema), existing slice 49b/49d/49e validators + state machine.

**Spec:** `docs/superpowers/specs/2026-05-24-crabka-sasl-session-cap-50d-design.md` (commit `6bc69ca`).

**Worktree:** `/Users/mattstone/git/crabka/.worktrees/slice-50d-sasl-session-cap` on branch `slice-50d-sasl-session-cap`. Verify with `git branch --show-current`. Commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`.

---

## File structure

| File | Responsibility | Touched by |
|---|---|---|
| `crates/broker/src/file_config.rs` | New `max_session_lifetime_seconds` field on `FileOAuthBearerConfig` + threading into `BrokerConfig` | T1 |
| `crates/broker/src/config.rs` | New `oauthbearer_max_session_lifetime_seconds: Option<u32>` field on `BrokerConfig` | T1 |
| `crates/broker/src/network/auth.rs` | `handle_authenticate_oauthbearer` clamps `session_lifetime_ms` and stores clamped `expires_at_ms` in both Negotiating and Reauthenticating arms | T1 |
| `crates/broker/src/network/dispatch.rs` | Pass the cap value from `broker.config` into the handler call site | T1 |
| `crates/broker/tests/auth_handlers.rs` | New integration test for the cap (response field + timer fire timing) | T1 |
| `crates/operator/src/crd/listener.rs` | New `max_seconds_without_reauthentication` field + hand-rolled schema entry + round-trip tests + fixture-site sweep in this file's tests | T2 |
| `crates/operator/src/controller/listeners.rs` | TOML render line + divergence walk perturbation entry + new unit tests + fixture sweep in this file's tests | T3 |
| `crates/operator/tests/reconcile_listener_oauth.rs` | Integration tests for the new field + fixture-helper updates | T4 |
| `crates/operator/sample/oauth-listener.yaml` | Commented-out `# maxSecondsWithoutReauthentication: 300` example | T4 |
| `deploy/crds/crabka.io_kafkas.yaml` | Regenerated CRD picks up the new property | T4 (via `tools/regen-crds.sh`) |
| `.github/workflows/operator-e2e.yml` | `kind-oauth` job's Kafka CR YAML extended with `maxSecondsWithoutReauthentication: 300` | T5 |
| `STATUS.md` | Slice 50d entry | T6 |

---

## Batches

### Batch 1 — T1 (broker, alone)

#### Task T1: Broker config + handler clamp + integration test

**Files:**
- Modify: `crates/broker/src/file_config.rs`
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/network/auth.rs`
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/tests/auth_handlers.rs`

- [ ] **Step 1: Write the failing integration test for the cap**

Edit `crates/broker/tests/auth_handlers.rs`. After the existing `oauthbearer_session_lifetime_ms_set_from_token_exp` test (around line 1321), add:

```rust
#[tokio::test(flavor = "current_thread", start_paused = false)]
async fn oauthbearer_session_capped_by_broker_max_session_lifetime_seconds() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker_with_cap(
        log_dir.path(),
        oauthbearer_zero_skew_validator(),
        Some(30), // 30s cap
    )
    .await;
    let addr = handle.listen_addr();

    // Token exp = now + 600s. Cap = 30s. Expected session = 30_000 ms.
    let exp_secs = now_unix_secs() + 600;
    let token = unsecured_jws("alice", exp_secs);

    let (mut stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Cap should clamp the response.
    assert!(
        (29_000..31_000).contains(&session_lifetime_ms),
        "session_lifetime_ms = {session_lifetime_ms}, expected ~30_000 (capped)"
    );

    // Now pause and advance past cap; broker should close.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(31)).await;

    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    assert_eq!(n, 0, "expected EOF after cap-bounded session expiry, got {n} bytes");

    handle.shutdown().await;
}
```

- [ ] **Step 2: Add the `start_oauthbearer_broker_with_cap` helper**

In the same file, add a new helper near the existing `start_oauthbearer_broker` (around line 865):

```rust
async fn start_oauthbearer_broker_with_cap(
    log_dir: &std::path::Path,
    validator: crabka_security::OAuthBearerValidator,
    max_session_lifetime_seconds: Option<u32>,
) -> crabka_broker::BrokerHandle {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
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
    cfg.oauthbearer_validator = validator;
    cfg.oauthbearer_max_session_lifetime_seconds = max_session_lifetime_seconds;
    Broker::start(cfg).await.expect("broker must start")
}
```

- [ ] **Step 3: Run the failing test — verify it fails to compile**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-50d-sasl-session-cap
cargo test -p crabka-broker --test auth_handlers oauthbearer_session_capped_by_broker_max_session_lifetime_seconds 2>&1 | tail
```

Expected: compile error — `BrokerConfig` has no `oauthbearer_max_session_lifetime_seconds` field.

- [ ] **Step 4: Add the field to `BrokerConfig`**

Edit `crates/broker/src/config.rs`. Find the existing `oauthbearer_idp_tls_trust` field (around line 176) and add after it:

```rust
/// Slice 50d: optional ceiling on OAUTHBEARER session lifetime, in
/// seconds. When set, the broker reports
/// `session_lifetime_ms = min(token_exp_ms - now_ms, cap * 1000)`
/// and the dispatch-loop re-auth timer fires at the clamped time.
/// When unset, sessions last until the token's natural `exp`
/// (slice 49e default).
pub oauthbearer_max_session_lifetime_seconds: Option<u32>,
```

Then find `impl Default for BrokerConfig` (or whichever constructor sets defaults — likely `for_tests` and `default`) and add the default initializer:

```rust
oauthbearer_max_session_lifetime_seconds: None,
```

Add this initializer wherever other `oauthbearer_*` fields are initialized. Grep first:

```bash
grep -n "oauthbearer_idp_tls_trust" crates/broker/src/config.rs
```

Add the new field on the line below each match.

- [ ] **Step 5: Add the field to `FileOAuthBearerConfig`**

Edit `crates/broker/src/file_config.rs`. Find the `introspection_http_timeout_ms` field (last existing field, around line 135) and add after it:

```rust
/// Slice 50d: optional ceiling on OAUTHBEARER session lifetime, in
/// seconds. When set, the broker clamps `session_lifetime_ms` to
/// `min(token_exp_ms - now_ms, cap * 1000)`. When unset, sessions
/// last until the token's natural `exp`.
#[serde(default)]
pub max_session_lifetime_seconds: Option<u32>,
```

- [ ] **Step 6: Thread the field through `apply_to`**

In the same file, find the `apply_to` block that handles oauthbearer (around line 258). Add this line near the top of the `if let Some(oauth) = self.oauthbearer { ... }` block, alongside the existing `cfg.oauthbearer_idp_tls_trust.clone_from(&oauth.idp_tls_trust);`:

```rust
cfg.oauthbearer_max_session_lifetime_seconds = oauth.max_session_lifetime_seconds;
```

- [ ] **Step 7: Run build — verify the BrokerConfig field is recognized**

```bash
cargo build -p crabka-broker 2>&1 | tail
```

Expected: clean build (test still fails, but the BrokerConfig field error is gone).

- [ ] **Step 8: Add failing handler-clamp unit tests**

Edit `crates/broker/src/network/auth.rs`. In the `#[cfg(test)] mod tests` block (search for the existing `handle_authenticate_oauthbearer_*` tests), add:

```rust
#[tokio::test]
async fn handle_authenticate_oauthbearer_clamps_session_lifetime_when_cap_set_below_exp() {
    let mut auth = ConnectionAuth::Negotiating {
        mechanism: SaslMechanism::OAuthBearer,
        exchange: SaslExchange::OAuthBearer,
    };
    let validator = OAuthBearerValidator::Unsecured(UnsecuredJwsValidator {
        allowable_clock_skew_ms: 0,
        ..Default::default()
    });
    let now_ms = 1_000_000_i64;
    let exp_ms = now_ms + 60_000; // token good for 60s
    let token = unsecured_token("alice", exp_ms / 1000);
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(oauthbearer_client_response(&token).into_bytes()),
        ..Default::default()
    };

    let resp = handle_authenticate_oauthbearer(
        &req,
        &mut auth,
        &validator,
        now_ms,
        Some(30), // 30s cap, less than token's 60s exp
    )
    .await;

    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.session_lifetime_ms, 30_000);
    match auth {
        ConnectionAuth::Authenticated { expires_at_ms, .. } => {
            assert_eq!(
                expires_at_ms,
                Some(now_ms + 30_000),
                "expires_at_ms must reflect the clamped value (not raw token exp)"
            );
        }
        _ => panic!("expected Authenticated"),
    }
}

#[tokio::test]
async fn handle_authenticate_oauthbearer_no_clamp_when_cap_unset() {
    let mut auth = ConnectionAuth::Negotiating {
        mechanism: SaslMechanism::OAuthBearer,
        exchange: SaslExchange::OAuthBearer,
    };
    let validator = OAuthBearerValidator::Unsecured(UnsecuredJwsValidator {
        allowable_clock_skew_ms: 0,
        ..Default::default()
    });
    let now_ms = 1_000_000_i64;
    let exp_ms = now_ms + 60_000;
    let token = unsecured_token("alice", exp_ms / 1000);
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(oauthbearer_client_response(&token).into_bytes()),
        ..Default::default()
    };

    let resp =
        handle_authenticate_oauthbearer(&req, &mut auth, &validator, now_ms, None).await;

    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.session_lifetime_ms, 60_000);
    match auth {
        ConnectionAuth::Authenticated { expires_at_ms, .. } => {
            assert_eq!(expires_at_ms, Some(exp_ms), "unset cap = raw token exp");
        }
        _ => panic!("expected Authenticated"),
    }
}

#[tokio::test]
async fn handle_authenticate_oauthbearer_no_clamp_when_cap_above_exp() {
    let mut auth = ConnectionAuth::Negotiating {
        mechanism: SaslMechanism::OAuthBearer,
        exchange: SaslExchange::OAuthBearer,
    };
    let validator = OAuthBearerValidator::Unsecured(UnsecuredJwsValidator {
        allowable_clock_skew_ms: 0,
        ..Default::default()
    });
    let now_ms = 1_000_000_i64;
    let exp_ms = now_ms + 60_000; // 60s
    let token = unsecured_token("alice", exp_ms / 1000);
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(oauthbearer_client_response(&token).into_bytes()),
        ..Default::default()
    };

    let resp = handle_authenticate_oauthbearer(
        &req,
        &mut auth,
        &validator,
        now_ms,
        Some(600), // 600s cap, well above 60s token
    )
    .await;

    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.session_lifetime_ms, 60_000, "cap above exp = no effect");
}
```

`unsecured_token` and `oauthbearer_client_response` are existing helpers in this test module (per slice 49e's plan). If they live under different names, grep:

```bash
grep -n "fn unsecured_token\|fn oauthbearer_client_response" crates/broker/src/network/auth.rs
```

- [ ] **Step 9: Run the unit tests — verify they fail to compile**

```bash
cargo test -p crabka-broker --lib network::auth::tests::handle_authenticate_oauthbearer_clamps 2>&1 | tail
```

Expected: compile error — `handle_authenticate_oauthbearer` takes 4 args, not 5.

- [ ] **Step 10: Update `handle_authenticate_oauthbearer` to accept the cap + clamp in both arms**

Edit `crates/broker/src/network/auth.rs`. Modify the function signature (around line 420):

```rust
pub async fn handle_authenticate_oauthbearer(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &crabka_security::OAuthBearerValidator,
    now_ms: i64,
    max_session_lifetime_seconds: Option<u32>,
) -> SaslAuthenticateResponse {
```

In the **Negotiating-success arm** (around line 433), replace the existing block:

```rust
Ok(outcome) => {
    let session_lifetime_ms =
        outcome.expires_at_ms.map_or(0, |e| (e - now_ms).max(0));
    *auth = ConnectionAuth::Authenticated {
        principal: outcome.principal,
        mechanism: mech,
        expires_at_ms: outcome.expires_at_ms,
    };
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms,
        ..Default::default()
    }
}
```

with:

```rust
Ok(outcome) => {
    let raw_session_ms = outcome.expires_at_ms.map_or(0, |e| (e - now_ms).max(0));
    let session_lifetime_ms = match max_session_lifetime_seconds {
        Some(cap) => raw_session_ms.min(i64::from(cap) * 1000),
        None => raw_session_ms,
    };
    // Store the CLAMPED expires_at_ms so the dispatch-loop timer
    // fires at the time the client was told. Otherwise the
    // session_lifetime_ms in the response would be a lie.
    let effective_expires_at_ms = Some(now_ms + session_lifetime_ms);
    *auth = ConnectionAuth::Authenticated {
        principal: outcome.principal,
        mechanism: mech,
        expires_at_ms: effective_expires_at_ms,
    };
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms,
        ..Default::default()
    }
}
```

In the **Reauthenticating-success arm** (around line 499), make the equivalent change:

```rust
let raw_session_ms = outcome.expires_at_ms.map_or(0, |e| (e - now_ms).max(0));
let session_lifetime_ms = match max_session_lifetime_seconds {
    Some(cap) => raw_session_ms.min(i64::from(cap) * 1000),
    None => raw_session_ms,
};
let effective_expires_at_ms = Some(now_ms + session_lifetime_ms);
*auth = ConnectionAuth::Authenticated {
    principal: outcome.principal,
    mechanism: prev_mech,
    expires_at_ms: effective_expires_at_ms,
};
SaslAuthenticateResponse {
    error_code: 0,
    error_message: None,
    auth_bytes: bytes::Bytes::new(),
    session_lifetime_ms,
    ..Default::default()
}
```

- [ ] **Step 11: Update the dispatch call site**

Edit `crates/broker/src/network/dispatch.rs`. Find the call to `handle_authenticate_oauthbearer` (search for the function name; from the explore it's the site that builds `now_ms` via `SystemTime::now()`). Add the new argument at the end of the call:

```rust
let resp = crate::network::auth::handle_authenticate_oauthbearer(
    &req,
    auth,
    &broker.config.oauthbearer_validator,
    now_ms,
    broker.config.oauthbearer_max_session_lifetime_seconds,
)
.await;
```

- [ ] **Step 12: Run all the broker tests**

```bash
cargo build -p crabka-broker 2>&1 | tail
cargo test -p crabka-broker --lib network::auth 2>&1 | tail
cargo test -p crabka-broker --test auth_handlers oauthbearer 2>&1 | tail
```

Expected: all green. The 3 new unit tests + 1 new integration test pass; existing tests still pass.

- [ ] **Step 13: fmt + clippy**

```bash
cargo fmt -p crabka-broker -- --check
cargo clippy -p crabka-broker --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 14: Commit**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-50d-sasl-session-cap
git add crates/broker/src/file_config.rs crates/broker/src/config.rs crates/broker/src/network/auth.rs crates/broker/src/network/dispatch.rs crates/broker/tests/auth_handlers.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T1: broker [oauthbearer].max_session_lifetime_seconds clamp

Adds an optional server-side ceiling on OAUTHBEARER session lifetime.
When set, handle_authenticate_oauthbearer clamps both the response
session_lifetime_ms and the stored Authenticated.expires_at_ms to
min(token_exp_ms - now_ms, cap * 1000). The dispatch-loop timer then
fires at the clamped time (not raw token exp) so the response value
the client sees is the value the broker actually enforces. Both the
initial-auth and Reauthenticating arms clamp identically.

Unset cap = unchanged from 49e behavior (session = token exp).

3 new unit tests cover cap below exp, cap unset, cap above exp.
1 new integration test wires the cap end-to-end with
tokio::time::pause + advance and asserts both the response field
and the timer-fire timing.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 2 — T2 then T3 (sequential within batch)

**Note:** T2 and T3 touch different files (`crd/listener.rs` vs `controller/listeners.rs`) — file-disjoint by the CLAUDE.md rule. BUT T3's verify step needs T2's struct field present (it adds `..base.clone()` style usages that reference the new field). Dispatch T2 first, wait for its commit, then dispatch T3. Within the same batch, but sequential dispatch order.

#### Task T2: Operator CRD field + schema + round-trip tests

**Files:**
- Modify: `crates/operator/src/crd/listener.rs`

- [ ] **Step 1: Write the failing round-trip test**

Edit `crates/operator/src/crd/listener.rs`. In the `#[cfg(test)] mod tests` block (search for existing `oauth_round_trip_*` tests), add:

```rust
#[test]
fn oauth_round_trip_with_max_seconds_without_reauthentication() {
    let yaml = r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
maxSecondsWithoutReauthentication: 300
"#;
    let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
    let oauth = match &parsed {
        ListenerAuthentication::OAuth(c) => c,
        _ => panic!("expected oauth variant"),
    };
    assert_eq!(oauth.max_seconds_without_reauthentication, Some(300));
}

#[test]
fn oauth_round_trip_without_max_seconds_without_reauthentication_omits_field() {
    let cfg = ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://issuer.example/".into(),
        jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: true,
        introspection_endpoint_uri: None,
        user_info_endpoint_uri: None,
        client_id: None,
        client_secret: None,
        introspection_http_timeout_seconds: None,
        max_seconds_without_reauthentication: None,
    };
    let auth = ListenerAuthentication::OAuth(cfg);
    let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
    assert!(
        !yaml.contains("maxSecondsWithoutReauthentication"),
        "None field must be omitted from YAML; got:\n{yaml}"
    );
}
```

- [ ] **Step 2: Run the tests — verify they fail to compile**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-50d-sasl-session-cap
cargo test -p crabka-operator --lib crd::listener::tests::oauth_round_trip_with_max_seconds 2>&1 | tail
```

Expected: compile error — `ListenerAuthenticationOAuth` has no `max_seconds_without_reauthentication` field.

- [ ] **Step 3: Add the field to the struct**

In the same file, find `ListenerAuthenticationOAuth` (around line 154). After the existing `introspection_http_timeout_seconds` field (the last field, around line 226), add:

```rust
/// Slice 50d: maximum SASL session lifetime (seconds) before the
/// broker forces re-authentication via KIP-368. Acts as a ceiling on
/// top of the token's `exp` — the effective session is
/// `min(token_exp - now, maxSecondsWithoutReauthentication)`. When
/// unset (the default), sessions last until the token's natural
/// `exp` (slice 49e behavior). Strimzi-shape field;
/// CRD-validated `minimum: 1`.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub max_seconds_without_reauthentication: Option<u32>,
```

- [ ] **Step 4: Add the field to the hand-rolled schema**

Find `listener_authentication_schema` (around line 287). Add the new property entry to the `properties` map. Place it alphabetically between `introspectionHttpTimeoutSeconds` (line 325) and `jwksRefreshSeconds`:

```rust
"introspectionHttpTimeoutSeconds": { "type": "integer", "minimum": 1 },
"maxSecondsWithoutReauthentication": { "type": "integer", "format": "int32", "minimum": 1 },
```

Wait — `jwksRefreshSeconds` is already listed earlier in the schema (line 305 per the explore), and the schema isn't fully alphabetical (e.g., `enableOauthBearer` comes before `tlsTrustedCertificates`). Match the existing order: place `maxSecondsWithoutReauthentication` AFTER `introspectionHttpTimeoutSeconds` (which is the last property in the existing schema).

- [ ] **Step 5: Sweep existing struct-literal sites in this file's tests**

Find ALL existing `ListenerAuthenticationOAuth { ... }` literals in this file:

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/crd/listener.rs
```

For each match site (likely a handful in the tests module), add `max_seconds_without_reauthentication: None,` as the last field initializer. Don't miss any — each one will fail to compile after the struct change.

If a fixture builder helper exists (e.g., a `fn oauth_full_config_for_tests()` somewhere in the test module), update its construction site once and all callers pick up the change.

- [ ] **Step 6: Update the schema-regression test (if present)**

Search for a test that asserts on the schema's properties:

```bash
grep -n "crd_oauth_schema_emits_expected_properties\|listener_authentication_schema\|maxClockSkewSeconds.*schema" crates/operator/src/crd/listener.rs
```

If a schema-regression test exists, extend its expected-properties list to include `"maxSecondsWithoutReauthentication"`. If it doesn't exist, skip this step.

- [ ] **Step 7: Run tests — verify they pass**

```bash
cargo test -p crabka-operator --lib crd::listener 2>&1 | tail
```

Expected: all pass. The 2 new round-trip tests + all existing tests green.

- [ ] **Step 8: fmt + clippy**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean. (If clippy warns about a large enum variant on `ListenerAuthentication::OAuth` again — that was `#[allow]`'d in slice 50c; verify the `#[allow]` is still in place.)

- [ ] **Step 9: Commit**

```bash
git add crates/operator/src/crd/listener.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T2: operator CRD — maxSecondsWithoutReauthentication on listener OAuth

Adds Option<u32> field on ListenerAuthenticationOAuth, Strimzi-shape
camelCase via serde, snake_case in Rust. Hand-rolled schema entry
with minimum: 1 (CRD-side validation). Two new round-trip tests
cover the field set + omitted from YAML when None. Existing
struct-literal fixtures in this file's test module swept to include
the new None default.

T3 follows up with the broker-TOML render in controller/listeners.rs
and extends the cross-listener divergence walk to include this field.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T3: Operator reconciler render + divergence + unit tests

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs`

**Prerequisite:** T2 must be committed before T3 starts (so the new struct field exists).

- [ ] **Step 1: Sweep this file's fixture sites**

Find all `ListenerAuthenticationOAuth { ... }` struct literals in `crates/operator/src/controller/listeners.rs`:

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/listeners.rs
```

For each, add `max_seconds_without_reauthentication: None,` at the end of the field list. The biggest one is the `base` in the divergence test (around line 1503 per the explore) — that already has 15 explicit fields after slice 50c.

- [ ] **Step 2: Write the failing render test**

In the test module, after existing `render_broker_toml_*` tests (search for one), add:

```rust
#[test]
fn render_broker_toml_emits_max_session_lifetime_seconds_when_set() {
    let oauth = ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://iss.example/".into(),
        jwks_endpoint_uri: Some("https://iss.example/jwks".into()),
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: true,
        introspection_endpoint_uri: None,
        user_info_endpoint_uri: None,
        client_id: None,
        client_secret: None,
        introspection_http_timeout_seconds: None,
        max_seconds_without_reauthentication: Some(300),
    };
    let listeners = vec![Listener {
        name: "oauth".into(),
        port: 9096,
        type_: ListenerType::Internal,
        tls: false,
        authentication: Some(ListenerAuthentication::OAuth(oauth)),
        configuration: None,
        network_policy_peers: None,
    }];
    let toml = render_broker_toml(&listeners, /* other args matching existing call sites */);
    assert!(
        toml.contains("max_session_lifetime_seconds = 300"),
        "expected TOML to contain max_session_lifetime_seconds = 300; got:\n{toml}"
    );
}

#[test]
fn render_broker_toml_omits_max_session_lifetime_seconds_when_unset() {
    let oauth = ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://iss.example/".into(),
        jwks_endpoint_uri: Some("https://iss.example/jwks".into()),
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: true,
        introspection_endpoint_uri: None,
        user_info_endpoint_uri: None,
        client_id: None,
        client_secret: None,
        introspection_http_timeout_seconds: None,
        max_seconds_without_reauthentication: None,
    };
    let listeners = vec![Listener {
        name: "oauth".into(),
        port: 9096,
        type_: ListenerType::Internal,
        tls: false,
        authentication: Some(ListenerAuthentication::OAuth(oauth)),
        configuration: None,
        network_policy_peers: None,
    }];
    let toml = render_broker_toml(&listeners, /* other args */);
    assert!(
        !toml.contains("max_session_lifetime_seconds"),
        "TOML must omit max_session_lifetime_seconds when unset; got:\n{toml}"
    );
}
```

`render_broker_toml` is a function in this file; grep for its actual signature to match the call site:

```bash
grep -n "fn render_broker_toml" crates/operator/src/controller/listeners.rs
```

If the function takes more parameters than shown above, mirror them from another existing `render_broker_toml_*` test.

- [ ] **Step 3: Run the tests — verify they fail**

```bash
cargo test -p crabka-operator --lib controller::listeners::tests::render_broker_toml_emits_max 2>&1 | tail
```

Expected: FAIL — the rendered TOML doesn't include `max_session_lifetime_seconds`.

- [ ] **Step 4: Add the render line to `render_broker_toml`**

Edit `render_broker_toml` (around line 2550). Find the `[oauthbearer]` block — specifically, the last `writeln!` in that block (currently the `idp_tls_trust` emission around line 2599). After that block and BEFORE the trailing `out.push('\n');` at line 2603, add:

```rust
if let Some(s) = oauth_cfg.max_seconds_without_reauthentication {
    let _ = writeln!(out, "max_session_lifetime_seconds = {s}");
}
```

This places the new key at the END of the `[oauthbearer]` block, matching the existing pattern of placing newer keys after older ones.

- [ ] **Step 5: Run render tests — verify pass**

```bash
cargo test -p crabka-operator --lib controller::listeners::tests::render_broker_toml_emits_max 2>&1 | tail
cargo test -p crabka-operator --lib controller::listeners::tests::render_broker_toml_omits_max 2>&1 | tail
```

Expected: both pass.

- [ ] **Step 6: Add the perturbation entry to the divergence walk**

Find `validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field` (around line 1495). At the end of its `perturbations` vec (after the `access_token_is_jwt` entry around line 1562), add:

```rust
(
    "max_seconds_without_reauthentication",
    crate::crd::ListenerAuthenticationOAuth {
        max_seconds_without_reauthentication: Some(600),
        ..base.clone()
    },
),
```

`base` already includes `max_seconds_without_reauthentication: None` from Step 1's sweep, so the perturbation has a clear delta.

- [ ] **Step 7: Run the divergence walk test**

```bash
cargo test -p crabka-operator --lib controller::listeners::tests::validate_listeners_rejects_two_oauth_listeners_with_divergent_config 2>&1 | tail
```

Expected: pass with one more perturbation case asserted.

- [ ] **Step 8: Run all listeners tests + fmt + clippy**

```bash
cargo test -p crabka-operator --lib controller::listeners 2>&1 | tail
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail
```

Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add crates/operator/src/controller/listeners.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T3: operator reconciler — render max_session_lifetime_seconds; divergence walk

Adds the broker-TOML render line for `max_session_lifetime_seconds`
under [oauthbearer] when the listener's
maxSecondsWithoutReauthentication is set. Omits the key when unset
(broker defaults to no cap = 49e behavior).

Extends the cross-listener divergence walk to perturb the new field
(two OAuth listeners with different values get
ConflictingOAuthListenerConfig). Existing fixture sites in this
file's test module swept to include the new None default.

The oauth_canonical helper requires no change — its PartialEq-based
comparison automatically picks up the new field.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 3 — T4 ‖ T5 (truly parallel, file-disjoint)

#### Task T4: Operator integration tests + sample + CRD regen

**Files:**
- Modify: `crates/operator/tests/reconcile_listener_oauth.rs`
- Modify: `crates/operator/sample/oauth-listener.yaml`
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (regenerated)

- [ ] **Step 1: Sweep fixture-builder helpers in the integration test file**

Find `oauth_cfg_minimal()` (around line 67 per the explore) and other fixture builders in `crates/operator/tests/reconcile_listener_oauth.rs`. Add `max_seconds_without_reauthentication: None,` to each builder's struct literal.

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/tests/reconcile_listener_oauth.rs
```

For each match site, add the field initializer at the end.

- [ ] **Step 2: Write the failing integration test**

In `crates/operator/tests/reconcile_listener_oauth.rs`, add:

```rust
#[tokio::test]
async fn oauth_listener_with_max_seconds_without_reauthentication_renders_broker_toml_key() {
    let mut cfg = oauth_cfg_minimal();
    cfg.max_seconds_without_reauthentication = Some(300);
    let listeners = vec![oauth_listener("oauth", 9096, true, cfg)];

    let toml = crabka_operator::controller::listeners::render_broker_toml(
        &listeners,
        /* match the args used by existing integration tests in this file */
    );
    assert!(
        toml.contains("max_session_lifetime_seconds = 300"),
        "expected rendered broker TOML to include max_session_lifetime_seconds = 300;\nfull TOML:\n{toml}"
    );
}
```

Mirror the `render_broker_toml` call shape from existing tests in this file.

- [ ] **Step 3: Write the failing divergence integration test**

```rust
#[tokio::test]
async fn two_oauth_listeners_with_divergent_max_seconds_without_reauthentication_rejected_with_conflicting_oauth_config() {
    let mut cfg_a = oauth_cfg_minimal();
    cfg_a.max_seconds_without_reauthentication = Some(300);
    let mut cfg_b = oauth_cfg_minimal();
    cfg_b.max_seconds_without_reauthentication = Some(600);
    let listeners = vec![
        oauth_listener("oauth-a", 9096, true, cfg_a),
        oauth_listener("oauth-b", 9097, true, cfg_b),
    ];

    let result = crabka_operator::controller::listeners::validate_listeners(&listeners);
    assert!(
        matches!(
            result,
            Err(crabka_operator::controller::listeners::ValidationError::ConflictingOAuthListenerConfig(_))
        ),
        "expected ConflictingOAuthListenerConfig; got {result:?}"
    );
}
```

Adjust the import path (`crabka_operator::controller::listeners::*`) to match what the existing tests use — likely a `use crabka_operator::controller::listeners::{render_broker_toml, validate_listeners, ValidationError};` block at the top of the file.

- [ ] **Step 4: Run the integration tests — verify pass**

```bash
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_with_max_seconds 2>&1 | tail
cargo test -p crabka-operator --test reconcile_listener_oauth two_oauth_listeners_with_divergent_max 2>&1 | tail
```

Expected: both pass (T3's render + divergence walk has already landed).

- [ ] **Step 5: Update the sample manifest**

Edit `crates/operator/sample/oauth-listener.yaml`. Inside the existing JWT-mode `oauth` listener block (around lines 17–34 per the explore), add a commented-out line under the `authentication:` block, before the existing `tlsTrustedCertificates`:

```yaml
        customClaimCheck:
          scope: kafka.write
        # Optional: cap SASL session lifetime tighter than the token's natural `exp`
        # (KIP-368 re-auth ceiling, slice 50d). Forces clients to refresh more
        # often than their token would otherwise require. Unset = session lasts
        # until token exp.
        # maxSecondsWithoutReauthentication: 300
        tlsTrustedCertificates:
```

Match indentation: 8 spaces for keys, 10 for comment-prefixed nested values.

- [ ] **Step 6: Verify sample YAML still parses**

```bash
cat crates/operator/sample/oauth-listener.yaml | python3 -c "import sys, yaml; docs = list(yaml.safe_load_all(sys.stdin)); print(f'{len(docs)} docs: {[d.get(\"kind\") for d in docs]}')"
```

Expected: `3 docs: ['Kafka', 'KafkaNodePool', 'KafkaUser']`.

- [ ] **Step 7: Regenerate CRDs**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-50d-sasl-session-cap
bash tools/regen-crds.sh 2>&1 | tail -10
git diff --stat deploy/crds/
```

Expected: ONLY `deploy/crds/crabka.io_kafkas.yaml` changed. The diff should add one new property under `spec.listeners[].authentication`:

```yaml
maxSecondsWithoutReauthentication:
  format: int32
  minimum: 1
  type: integer
```

If the diff shows other CRDs changing or shows unexpected properties, investigate before committing.

- [ ] **Step 8: Run the full operator test suite**

```bash
cargo test -p crabka-operator 2>&1 | tail
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --tests -- -D warnings 2>&1 | tail
```

Expected: all green.

- [ ] **Step 9: Commit**

```bash
git add crates/operator/tests/reconcile_listener_oauth.rs crates/operator/sample/oauth-listener.yaml deploy/crds/crabka.io_kafkas.yaml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T4: operator integration tests + sample + CRD regen for slice 50d

Two integration tests in tests/reconcile_listener_oauth.rs:
- render: maxSecondsWithoutReauthentication threads through to the
  broker TOML as max_session_lifetime_seconds.
- divergence: two OAuth listeners with different cap values get
  ConflictingOAuthListenerConfig.

Existing fixture builders in this test file swept to include the
new None default.

Sample manifest gets a commented-out maxSecondsWithoutReauthentication
line in the JWT-mode oauth listener block.

CRDs regenerated; only deploy/crds/crabka.io_kafkas.yaml changed
(one new property with minimum: 1, format: int32, type: integer).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T5: E2E job extension on `kind-oauth`

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

- [ ] **Step 1: Read the existing `kind-oauth` job's Kafka CR YAML heredoc**

```bash
grep -n "^  kind-oauth:\|maxSecondsWithoutReauthentication\|authentication:" .github/workflows/operator-e2e.yml | head -20
```

Find the `kind-oauth` job's `kubectl apply -f -` block (around lines 1960+ per the slice 50c context). Locate the listener's `authentication:` block — it's an OAuth JWT-mode config similar to the sample manifest.

- [ ] **Step 2: Add `maxSecondsWithoutReauthentication: 300` to the OAuth listener**

Inside the heredoc's `authentication:` block (for the OAuth listener — there may be multiple listeners in the Kafka CR), add the new line. Indentation matches existing sibling keys.

Example (the exact line numbers and surrounding context will differ; match what's there):

```yaml
              authentication:
                type: oauth
                validIssuerUri: https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka
                jwksEndpointUri: https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka/protocol/openid-connect/certs
                validAudience: kafka-client
                userNameClaim: preferred_username
                customClaimCheck: { scope: kafka.write }
                maxSecondsWithoutReauthentication: 300
                tlsTrustedCertificates:
                  - secretName: keycloak-ca
                    certificate: tls.crt
```

**Why 300?** Well above any reasonable producer-Job runtime (existing producer Jobs complete in <60s), so the cap doesn't break the existing produce-and-consume assertion. Yet small enough that it's clearly a non-default value being exercised.

- [ ] **Step 3: Extend the diagnostics step (optional but useful)**

In the same job's `if: failure()` diagnostics block (or wherever the broker logs are captured), add a grep for the cap value to make the cap-being-applied visible in CI logs:

```bash
kubectl logs -n default $(kubectl get pods -n default -l app.kubernetes.io/name=crabka -o jsonpath='{.items[0].metadata.name}') | grep -i 'max_session_lifetime\|session expired' || true
```

This is a nice-to-have for debugging; skip if the existing diagnostic block already captures the broker logs in full.

- [ ] **Step 4: Verify the YAML parses**

```bash
python3 -c "
import yaml
w = yaml.safe_load(open('.github/workflows/operator-e2e.yml'))
print('jobs:', list(w['jobs'].keys()))
"
```

Expected: list includes `kind-oauth`. No parse errors.

- [ ] **Step 5: Verify with actionlint (if available)**

```bash
which actionlint && actionlint .github/workflows/operator-e2e.yml 2>&1 | head -20 || echo "actionlint not installed; skip"
```

Pre-existing warnings (per slice 50c notes) are fine; no NEW warnings should appear from this small CR YAML change.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/operator-e2e.yml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T5: kind-oauth e2e — exercise maxSecondsWithoutReauthentication

Adds maxSecondsWithoutReauthentication: 300 to the kind-oauth job's
Kafka CR YAML, so the e2e exercises slice 50d end-to-end (operator
threads the field through to broker TOML; broker emits the clamped
session_lifetime_ms; client sees the value via librdkafka). 300s is
well above any producer-Job runtime so the existing produce-and-
consume assertion still passes.

No new job. No semantic change to existing assertions.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 4 — T6 (alone)

#### Task T6: STATUS.md + final gate

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Read slice 49e's STATUS entry for tone**

```bash
grep -n "^## Slice 49e " STATUS.md
# Then read that section.
```

- [ ] **Step 2: Append the slice 50d entry**

Append to `STATUS.md`:

```markdown
## Slice 50d — Operator + Broker: SASL session-lifetime cap (KIP-368 ceiling) (2026-05-24)

Bundles a server-side cap on top of slice 49e + surfaces Strimzi's
`maxSecondsWithoutReauthentication` field on
`KafkaListenerAuthenticationOAuth`. Operators can now clamp
OAUTHBEARER sessions tighter than the token's natural `exp`.

- **Broker (`crates/broker/src/file_config.rs` + `config.rs`):** new
  optional `[oauthbearer].max_session_lifetime_seconds: u32` TOML key,
  threaded into `BrokerConfig.oauthbearer_max_session_lifetime_seconds`.
  When unset, behavior is unchanged from 49e (session = token `exp`).
- **Broker handler (`crates/broker/src/network/auth.rs`):** both the
  Negotiating-success and Reauthenticating-success arms of
  `handle_authenticate_oauthbearer` clamp:
  `session_lifetime_ms = min(token_exp_ms - now_ms, cap * 1000)`. The
  CLAMPED value is what's stored on `Authenticated.expires_at_ms`, so
  the dispatch loop's KIP-368 timer fires at the time the client was
  told (not the raw token exp).
- **Operator CRD (`crates/operator/src/crd/listener.rs`):** new
  `maxSecondsWithoutReauthentication: Option<u32>` field on
  `ListenerAuthenticationOAuth`, Strimzi-shape camelCase. Hand-rolled
  schema entry with `minimum: 1`.
- **Operator reconciler (`crates/operator/src/controller/listeners.rs`):**
  `render_broker_toml` emits `max_session_lifetime_seconds = N` under
  `[oauthbearer]` when set. The existing cross-listener divergence
  walk picks up the new field via `oauth_canonical`'s PartialEq
  comparison; the per-field perturbation list explicitly covers it.
- **Semantic divergence from Strimzi (acknowledged):** Strimzi's
  unset = no re-auth (session = ∞), set = enable re-auth with cap.
  Crabka 50d: unset = session = token exp (49e default), set =
  clamp tighter. Strimzi parity is shape-only; greenfield-OK because
  there are no users with existing unbounded expectations.
- **Tests:** 3 new broker unit tests (cap below/unset/above exp) + 1
  new broker integration test (response field + timer fire timing).
  2 new operator CRD round-trip tests + extended schema regression.
  2 new operator reconciler unit tests (render set/unset) +
  extended cross-listener divergence walk. 2 new operator
  integration tests (render-through + divergence).
- **E2E:** existing `kind-oauth` job's Kafka CR YAML extended with
  `maxSecondsWithoutReauthentication: 300`. No new job.
- **Reference doc:** `[docs/superpowers/specs/2026-05-24-crabka-sasl-session-cap-50d-design.md]`.
- **Out of scope:** mechanism-agnostic `connections.max.reauth.ms`
  (would force re-auth on PLAIN/SCRAM); per-listener divergent caps
  (still rejected as `ConflictingOAuthListenerConfig`); client-side
  re-auth scheduler in Crabka's Kafka client crate (broker-only this
  slice); new e2e workflow job.
```

- [ ] **Step 3: Final gate**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-50d-sasl-session-cap
cargo fmt --check 2>&1 | tail -3
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -30
cargo test --workspace 2>&1 | tail -30
bash tools/regen-crds.sh && git diff --exit-code -- deploy/crds/ ; echo "exit: $?"
```

All four must be green. Known pre-existing flake:
`auto_rebalance_restores_preferred_leader` in
`crates/broker/tests/elect_leaders.rs`. If it fires, re-run in
isolation:

```bash
cargo test -p crabka-broker --test elect_leaders auto_rebalance_restores_preferred_leader 2>&1 | tail
```

If still flaky after isolation, document as pre-existing — not a T6 blocker.

- [ ] **Step 4: Commit**

```bash
git add STATUS.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
Slice 50d: STATUS.md entry + final gate

Documents the new operator + broker session-cap surface:
[oauthbearer].max_session_lifetime_seconds on the broker,
maxSecondsWithoutReauthentication on the operator listener OAuth
CRD, additive-cap semantic divergence from Strimzi acknowledged.
fmt + clippy + workspace tests + CRD drift gate all green.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Notes

- **Dependency chain:** T1 → T2 → T3 → (T4 ‖ T5) → T6. Six tasks, four batches. T2 and T3 are file-disjoint by design but dispatched sequentially because T3's verify needs T2's struct field present; T4 and T5 are truly parallel.
- **CLAUDE.md greenfield:** No `#[serde(default)]` magic beyond what `Option<>` already provides. No "supports old config without the key" branches. The struct change cascades to all fixture sites; T2 and T3 each sweep their own file's fixtures.
- **Semantic divergence from Strimzi (deliberate):** documented in the spec + STATUS. Crabka's 49e baseline is "always-bounded by token exp"; 50d's field is an additive ceiling, not the on/off switch Strimzi uses. Greenfield posture wins.
- **`oauth_canonical` requires no code change:** it returns `cfg.clone()` modulo `enable_oauth_bearer = true`. Adding a new field to the struct automatically participates in the canonical comparison via `PartialEq`. Only the divergence walk's perturbation list needs the explicit entry (T3 Step 6).
- **`Authenticated.expires_at_ms` semantics (per spec):** must reflect the CLAMPED value, not the raw token exp. Without this, the dispatch-loop timer would fire at the token's natural expiry and the broker would tolerate the connection past the value reported to the client. Three failure modes documented in the spec; T1 Step 10's "Store the CLAMPED expires_at_ms" comment captures the WHY.
- **After 50d lands:** umbrella's next pair is **49f + 50e** (PLAIN-with-OAuth-token, optional and only-if-user-demand). Then **49g + 50f** (claim enrichments + remaining Strimzi fields — the long tail).
