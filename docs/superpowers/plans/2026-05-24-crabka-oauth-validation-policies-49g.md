# Slice 49g — OAUTHBEARER validation policies Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** ## Slice 49g — Operator + Broker: OAUTHBEARER validation policies (customClaimCheck JsonPath + validTokenType) (2026-05-24)

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Slice 49h (claims mapping — groupsClaim, groupsClaimDelimiter, fallbackUserNameClaim, fallbackUserNamePrefix) — closed by slice 49h
- Slice 49i (JWKS refresher policies — jwksMinRefreshPauseSeconds, jwksExpirySeconds, jwksIgnoreKeyUse) — closed by slice 49i
- Slice 49f (PLAIN-with-OAuth-token) — skipped indefinitely
- Semantic divergence from Strimzi (acknowledged): Crabka uses jsonpath-rust 1.0 (RFC 9535) — NOT the Jayway dialect Strimzi inherits; operators porting Strimzi expressions must rewrite filter syntax

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Strimzi's full `customClaimCheck` (JsonPath expression via `jsonpath-rust`) and `validTokenType` (JWT `typ` header check) on the broker + operator surfaces. Replaces slice 50's typed `customClaimCheck: { scope, scope_claim }` stub with the full Strimzi string-expression shape.

**Architecture:** Six tasks across four batches (mirrors slice 50d). T1 wires the broker: new `jsonpath-rust` dep, validator integration in all three validators (Unsecured/Signed/Introspection), JWT `typ` check (JWT-mode only), and DELETES the slice-50 `required_scope` + `scope_claim_name` stub fields. T2 + T3 each touch one operator file (CRD type shape change + reconciler render). T4 + T5 are file-disjoint parallel work (operator integration tests + sample + CRD regen ‖ kind-oauth e2e YAML rewrite). T6 ships STATUS + final gate. The slice is more breaking than 50d: the typed `OAuthCustomClaimCheck` struct disappears entirely, with ~30 sweep sites across operator + tests.

**Tech Stack:** Rust, `jsonpath-rust` crate (Jayway-flavored JsonPath, besok/jsonpath-rust), serde (TOML/YAML), kube-rs, schemars (hand-rolled JSON Schema), existing slice 49b/49d/49e/50d validators.

**Spec:** `docs/superpowers/specs/2026-05-24-crabka-oauth-validation-policies-49g-design.md` (commit `8a2a93d`).

**Worktree:** `/Users/mattstone/git/crabka/.worktrees/slice-49g-oauth-validation-policies` on branch `slice-49g-oauth-validation-policies`. Verify with `git branch --show-current`. Commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`.

---

## File structure

| File | Responsibility | Touched by |
|---|---|---|
| `crates/security/Cargo.toml` | Add `jsonpath-rust` dep | T1 |
| `crates/security/src/oauthbearer.rs` | Validator structs gain `custom_claim_check: Option<JsonPathInst>` + `valid_token_type: Option<String>` (JWT-mode only); per-validator integration + unit tests; DELETE `required_scope`, `scope_claim_name`, `scope_contains()`, `scope_claim_contains()`, `check_required_scope()` | T1 |
| `crates/broker/src/file_config.rs` | New `custom_claim_check` + `valid_token_type` fields on `FileOAuthBearerConfig` + threading into `BrokerConfig`; DELETE old `required_scope` / `scope_claim_name` fields | T1 |
| `crates/broker/src/config.rs` | New `oauthbearer_custom_claim_check_expression: Option<String>` + `oauthbearer_valid_token_type: Option<String>` on `BrokerConfig`; the expression is compiled once in the validator constructor | T1 |
| `crates/operator/src/crd/listener.rs` | Replace `custom_claim_check: Option<OAuthCustomClaimCheck>` → `Option<String>`; add `valid_token_type: Option<String>`; DELETE `OAuthCustomClaimCheck` struct; update schema; sweep struct-literal sites in this file's tests | T2 |
| `crates/operator/src/controller/listeners.rs` | Cross-mode validation for `validTokenType` (JWT-mode only); rewrite render code for `custom_claim_check` + add `valid_token_type` emission; DELETE `ListenerOauthCustomClaimCheckScopeEmpty` ValidationError variant + the OLD render code; update divergence walk; sweep struct-literal sites in this file's tests | T3 |
| `crates/operator/src/controller/kafka.rs` + `kafka_node_pool.rs` | Fixture sweep (4 + 3 struct-literal sites) — atomic with T3's struct change | T3 |
| `crates/operator/tests/reconcile_listener_oauth.rs` + `reconcile_oauth_introspection.rs` + `reconcile_oauth_trust.rs` | Sweep fixtures (5 + 2 + 1 struct sites + 3 `OAuthCustomClaimCheck` refs); add 3 new integration tests | T4 |
| `crates/operator/sample/oauth-listener.yaml` | Rewrite `customClaimCheck:` block to the JsonPath string form; add commented `validTokenType:` hint | T4 |
| `deploy/crds/crabka.io_kafkas.yaml` | Regenerated CRD picks up the new shape | T4 (via `tools/regen-crds.sh`) |
| `.github/workflows/operator-e2e.yml` | `kind-oauth` job's Kafka CR YAML rewrite (replace existing `customClaimCheck: { scope: kafka.write }` block) | T5 |
| `STATUS.md` | Slice 49g entry | T6 |

---

## Batches

### Batch 1 — T1 (broker, alone)

#### Task T1: Broker dep + validator integration + delete slice-50 scope stub

**Files:**
- Modify: `crates/security/Cargo.toml`
- Modify: `crates/security/src/oauthbearer.rs`
- Modify: `crates/broker/src/file_config.rs`
- Modify: `crates/broker/src/config.rs`

- [ ] **Step 1: Add `jsonpath-rust` dependency**

Edit `crates/security/Cargo.toml`. In the `[dependencies]` block, add:

```toml
jsonpath-rust = "1.0"
```

Verify the actual current version with `cargo search jsonpath-rust 2>&1 | head -3`. Use the latest stable major (likely `1.x`). License: confirm it's MIT or Apache-2.0 with `cargo metadata --format-version 1 | jq '.packages[] | select(.name=="jsonpath-rust") | .license'` after `cargo build`.

- [ ] **Step 2: Write failing unit test for `customClaimCheck` JsonPath integration (unsecured validator)**

Edit `crates/security/src/oauthbearer.rs`. In the existing `#[cfg(test)] mod` for unsecured-validator tests, add:

```rust
#[test]
fn unsecured_validate_rejects_when_custom_claim_check_fails() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws(&serde_json::json!({
        "sub": "alice",
        "exp": exp_secs,
        "scope": "kafka.read",
    }));
    let mut v = UnsecuredJwsValidator::default();
    v.custom_claim_check = Some(
        jsonpath_rust::JsonPathInst::from_str("$[?(@.scope == 'kafka.admin')]")
            .expect("expression compiles"),
    );
    let result = v.validate(&token, now_ms);
    assert_eq!(result.unwrap_err(), AuthError::InvalidToken);
}

#[test]
fn unsecured_validate_accepts_when_custom_claim_check_passes() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws(&serde_json::json!({
        "sub": "alice",
        "exp": exp_secs,
        "scope": "kafka.admin",
    }));
    let mut v = UnsecuredJwsValidator::default();
    v.custom_claim_check = Some(
        jsonpath_rust::JsonPathInst::from_str("$[?(@.scope == 'kafka.admin')]")
            .expect("expression compiles"),
    );
    let outcome = v.validate(&token, now_ms).expect("valid token");
    assert_eq!(outcome.principal.name, "alice");
}

#[test]
fn unsecured_validate_rejects_when_valid_token_type_mismatch() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    // Token with typ=OPAQUE in the header.
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "OPAQUE"}),
        &serde_json::json!({"sub": "alice", "exp": exp_secs}),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.valid_token_type = Some("JWT".into());
    let result = v.validate(&token, now_ms);
    assert_eq!(result.unwrap_err(), AuthError::InvalidToken);
}

#[test]
fn unsecured_validate_accepts_when_valid_token_type_match() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({"sub": "alice", "exp": exp_secs}),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.valid_token_type = Some("JWT".into());
    assert!(v.validate(&token, now_ms).is_ok());
}

#[test]
fn unsecured_validate_accepts_when_valid_token_type_unset_regardless_of_header() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "OPAQUE"}),
        &serde_json::json!({"sub": "alice", "exp": exp_secs}),
    );
    let v = UnsecuredJwsValidator::default();
    // No valid_token_type set → header `typ` ignored.
    assert!(v.validate(&token, now_ms).is_ok());
}
```

`make_unsecured_jws` and `make_unsecured_jws_with_header` are likely-existing test helpers. Grep first:

```bash
grep -n "fn make_unsecured\|fn unsecured_jws\|fn jws_for_tests" crates/security/src/oauthbearer.rs
```

If only `make_unsecured_jws` exists (with a hardcoded header `{"alg":"none","typ":"JWT"}` or no typ), you'll need to either:
- Add a sibling `make_unsecured_jws_with_header(header, payload)` helper that takes a custom header.
- Or use an inline base64-encoded JWT for the `validTokenType` tests.

Mirror the existing helper's pattern.

- [ ] **Step 3: Run the test — verify it fails to compile**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49g-oauth-validation-policies
cargo test -p crabka-security oauthbearer::tests::unsecured_validate_rejects_when_custom_claim_check_fails 2>&1 | tail -20
```

Expected: compile error — `UnsecuredJwsValidator` has no `custom_claim_check` or `valid_token_type` field; `jsonpath_rust::JsonPathInst` not in scope without `use jsonpath_rust::JsonPathInst;` at top of module.

- [ ] **Step 4: Add `jsonpath-rust` use + extend `UnsecuredJwsValidator` struct**

At the top of `crates/security/src/oauthbearer.rs`:

```rust
use jsonpath_rust::JsonPathInst;
```

Modify the `UnsecuredJwsValidator` struct (lines 116–128). DELETE the `required_scope` and `scope_claim_name` fields. ADD `custom_claim_check` and `valid_token_type`:

```rust
#[derive(Debug, Clone, Default)]
pub struct UnsecuredJwsValidator {
    /// Claim whose string value becomes the principal name. Default `sub`.
    pub principal_claim_name: String,
    /// Tolerance, in milliseconds, applied to the `exp` / `iat` temporal
    /// checks to absorb clock drift between the client and broker.
    pub allowable_clock_skew_ms: i64,
    /// Slice 49g: precompiled JsonPath expression evaluated against the
    /// token's claim set. Token is rejected when the expression yields
    /// empty/null/false. Compile once at validator construction.
    pub custom_claim_check: Option<JsonPathInst>,
    /// Slice 49g: when set, the JWT `typ` header field must equal this
    /// string. Ignored when unset.
    pub valid_token_type: Option<String>,
}
```

If `Default` is implemented manually (the explore showed `pub struct` without `derive(Default)`), check whether it's derived or hand-rolled. If derived, the new `Option` fields default to `None`. If hand-rolled, update the impl.

- [ ] **Step 5: Update `UnsecuredJwsValidator::validate()` body**

Replace the body (lines 150–204). Two changes:
- Add JWT `typ` header check after the existing `alg:none` check.
- Replace the `required_scope` block with the JsonPath eval block.

```rust
pub fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
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
    // Slice 49g: optional JWT `typ` check (JWT-mode validator only).
    if let Some(expected_typ) = &self.valid_token_type
        && header.get("typ").and_then(Value::as_str) != Some(expected_typ.as_str())
    {
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

    // Slice 49g: optional JsonPath custom_claim_check.
    if let Some(path) = &self.custom_claim_check
        && !evaluate_custom_claim_check(path, &claims)
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

- [ ] **Step 6: Add the `evaluate_custom_claim_check` helper + DELETE the slice-50 scope helpers**

Add this private helper near the existing claim-helper functions in the same file:

```rust
/// Evaluate a precompiled JsonPath expression against the token claims.
/// Returns true when the result is truthy (non-empty/non-null/non-false);
/// false otherwise. Matches Strimzi's "expression yields truthy" semantics.
fn evaluate_custom_claim_check(path: &JsonPathInst, claims: &Value) -> bool {
    let result = path.find_slice(claims);
    // `find_slice` returns Vec<&Value>. Empty = no match = rejection.
    // Single result of `false` or `null` should also count as rejection.
    if result.is_empty() {
        return false;
    }
    for r in result {
        let v: &Value = r.deref();
        match v {
            Value::Null => return false,
            Value::Bool(false) => return false,
            _ => {}
        }
    }
    true
}
```

Note: `JsonPathInst::find_slice()` is the API in `jsonpath-rust` 0.5+ and 1.0+. Verify the exact API name in the version chosen via:

```bash
cargo doc -p jsonpath-rust --no-deps --open 2>&1 | head -3 || \
  grep -rn "find_slice\|JsonPathInst" $(find ~/.cargo/registry/src -name "jsonpath-rust-*" -type d | head -1) | head -10
```

If the actual method is named differently (e.g., `find()`, `query()`, `apply()`), adapt the helper accordingly. The contract: take a `&JsonPathInst` + `&Value`, return Vec of references (or similar collection) — empty means no match.

DELETE these obsolete functions/methods (slice-50 scope-check residue):

```bash
grep -n "fn scope_contains\|fn scope_claim_contains\|fn check_required_scope" crates/security/src/oauthbearer.rs
```

Remove all three function definitions. Anything that called them (the slice-50-era code in validator bodies) is being rewritten in this task anyway.

- [ ] **Step 7: Run unsecured tests — verify they pass**

```bash
cargo test -p crabka-security oauthbearer::tests::unsecured_validate 2>&1 | tail -15
```

Expected: all 5 new unsecured tests pass. Existing unsecured tests should also still pass (they no longer reference `required_scope` — but you may need to delete obsolete tests like `unsecured_validate_rejects_when_required_scope_missing` if they exist). Grep:

```bash
grep -n "required_scope\|scope_contains" crates/security/src/oauthbearer.rs
```

Delete any remaining references in tests. If a test was specifically about scope validation, rewrite it to use the JsonPath equivalent (`@.scope == 'X'`).

- [ ] **Step 8: Extend `SignedJwsValidator` struct — same pattern**

Replace lines 281–297. DELETE `required_scope` and `scope_claim_name`. ADD `custom_claim_check` and `valid_token_type`:

```rust
#[derive(Debug, Clone, Default)]
pub struct SignedJwsValidator {
    pub principal_claim_name: String,
    pub allowable_clock_skew_ms: i64,
    pub valid_issuer: Option<String>,
    pub expected_audience: Option<String>,
    /// Slice 49g: precompiled JsonPath custom_claim_check. See
    /// `UnsecuredJwsValidator` for semantics.
    pub custom_claim_check: Option<JsonPathInst>,
    /// Slice 49g: JWT `typ` header check.
    pub valid_token_type: Option<String>,
    keys: JwksHandle,
}
```

`keys` field stays last (private). `default` on `JwksHandle` — verify the existing `Default` impl is intact; if not derived, hand-update.

- [ ] **Step 9: Update `SignedJwsValidator::validate()` for `typ` check**

Add the JWT typ check after the existing `alg` check (around line 339). Insert:

```rust
if alg != "RS256" && alg != "ES256" {
    return Err(AuthError::InvalidToken);
}
// Slice 49g: optional JWT `typ` check (JWT-mode validator only).
if let Some(expected_typ) = &self.valid_token_type
    && header.get("typ").and_then(Value::as_str) != Some(expected_typ.as_str())
{
    return Err(AuthError::InvalidToken);
}
let kid = header.get("kid").and_then(Value::as_str);
```

- [ ] **Step 10: Update `SignedJwsValidator::check_claims()` body**

Replace the scope-check block (in lines 363–414) with the JsonPath eval. Delete:

```rust
if let Some(required) = &self.required_scope
    && !scope_claim_contains(claims, &self.scope_claim_name, required)
{
    return Err(AuthError::InvalidToken);
}
```

Add:

```rust
if let Some(path) = &self.custom_claim_check
    && !evaluate_custom_claim_check(path, claims)
{
    return Err(AuthError::InvalidToken);
}
```

- [ ] **Step 11: Add Signed-validator unit tests (5 mirror tests)**

Mirror the 5 unsecured tests in Step 2, but for `SignedJwsValidator`. The existing signed-validator test helpers (likely `signed_validator_for_tests()`, `make_signed_jws()` per slice 49b's tests) — grep:

```bash
grep -n "fn signed_validator_for_tests\|fn make_signed_jws\|fn signed_jws" crates/security/src/oauthbearer.rs
```

5 tests: `signed_validate_rejects_when_custom_claim_check_fails`, `signed_validate_accepts_when_custom_claim_check_passes`, `signed_validate_rejects_when_valid_token_type_mismatch`, `signed_validate_accepts_when_valid_token_type_match`, `signed_validate_accepts_when_valid_token_type_unset_regardless_of_header`.

Each follows the same body shape as the unsecured equivalent, with `make_signed_jws_with_header(...)` (or however the existing helper exposes a custom header).

- [ ] **Step 12: Run signed-validator tests — verify pass**

```bash
cargo test -p crabka-security oauthbearer::tests::signed_validate 2>&1 | tail -15
```

Expected: all 5 new signed tests pass + existing signed tests still pass.

- [ ] **Step 13: Extend `IntrospectionValidator` struct — same pattern (no typ check)**

Replace lines 499–517. DELETE `required_scope` and `scope_claim_name`. ADD `custom_claim_check` (no `valid_token_type` — introspection has no JWT header to validate):

```rust
#[derive(Debug, Clone)]
pub struct IntrospectionValidator {
    pub client: Arc<dyn IntrospectionClient>,
    pub principal_claim_name: String,
    /// Slice 49g: precompiled JsonPath custom_claim_check.
    pub custom_claim_check: Option<JsonPathInst>,
    pub call_userinfo: bool,
    pub allowable_clock_skew_ms: i64,
}
```

- [ ] **Step 14: Update `IntrospectionValidator::validate()` body**

Replace the scope-check block (the `check_required_scope(...)` call around line 567) with the JsonPath eval:

```rust
// Slice 49g: optional JsonPath custom_claim_check (replaces slice-50
// scope check). Evaluated against the merged claims (introspection +
// userinfo).
if let Some(path) = &self.custom_claim_check
    && !evaluate_custom_claim_check(path, &claims)
{
    return Err(AuthError::InvalidToken);
}
```

- [ ] **Step 15: Add Introspection-validator unit tests (2 tests)**

```rust
#[tokio::test]
async fn introspection_validate_rejects_when_custom_claim_check_fails() {
    let fake_client = FakeIntrospectionClient::with_response(serde_json::json!({
        "active": true,
        "sub": "alice",
        "exp": 2_000,
        "scope": "kafka.read",
    }));
    let mut v = IntrospectionValidator::new_for_tests(Arc::new(fake_client));
    v.custom_claim_check = Some(
        JsonPathInst::from_str("$[?(@.scope == 'kafka.admin')]")
            .expect("expression compiles"),
    );
    let result = v.validate("opaque-token", 1_000_000).await;
    assert_eq!(result.unwrap_err(), AuthError::InvalidToken);
}

#[tokio::test]
async fn introspection_validate_does_not_check_valid_token_type() {
    // Introspection responses have no JWT header → typ check is N/A.
    // The struct doesn't even expose a valid_token_type field; this
    // is a regression test that validation passes regardless of any
    // hypothetical typ in the response (introspection responses don't
    // typically carry `typ`).
    let fake_client = FakeIntrospectionClient::with_response(serde_json::json!({
        "active": true,
        "sub": "alice",
        "exp": 2_000,
    }));
    let v = IntrospectionValidator::new_for_tests(Arc::new(fake_client));
    let outcome = v.validate("opaque-token", 1_000_000).await.expect("valid");
    assert_eq!(outcome.principal.name, "alice");
}
```

Adapt `FakeIntrospectionClient` / `IntrospectionValidator::new_for_tests` to whatever the existing introspection tests use (per slice 50d).

- [ ] **Step 16: Add compile-error unit test**

```rust
#[test]
fn custom_claim_check_compile_error_at_validator_construction() {
    // Operators paste a malformed expression. We catch it at compile
    // time (validator construction), not per-token validation.
    let result = JsonPathInst::from_str("@.unterminated");
    assert!(result.is_err(), "malformed expression must fail to parse");
}
```

This is intentionally trivial — confirms our reliance on `jsonpath-rust`'s parse-time validation.

- [ ] **Step 17: Run all oauthbearer tests**

```bash
cargo test -p crabka-security oauthbearer 2>&1 | tail -20
```

Expected: 5 + 5 + 2 + 1 = 13 new tests pass. All existing tests still pass (with any necessary rewrites for ones that referenced `required_scope`).

- [ ] **Step 18: Update `FileOAuthBearerConfig`**

Edit `crates/broker/src/file_config.rs` (lines 57–142). DELETE the `required_scope` and `scope_claim_name` fields. ADD `custom_claim_check` and `valid_token_type`:

```rust
// Delete these existing fields:
//     #[serde(default)]
//     pub scope_claim_name: Option<String>,
//     #[serde(default)]
//     pub required_scope: Option<String>,

// Add after the `principal_claim_name` field:
    /// Slice 49g: optional JsonPath expression (Jayway-flavored, via
    /// jsonpath-rust) evaluated against the token claim set. Token is
    /// rejected when the expression yields empty/null/false. Compiled
    /// once at broker startup; malformed expressions panic with a
    /// descriptive error.
    #[serde(default)]
    pub custom_claim_check: Option<String>,

    /// Slice 49g: optional JWT `typ` header check. When set, JWT-mode
    /// validators (unsecured + signed JWS) require the JWT header's
    /// `typ` field to equal this string. Introspection-mode skips
    /// (no JWT header). Ignored when unset.
    #[serde(default)]
    pub valid_token_type: Option<String>,
```

- [ ] **Step 19: Update `apply_to` threading**

In the same file, find the `apply_to` block (around line 258 per slice 50d). Replace the slice-50 `required_scope` / `scope_claim_name` threading (if present — they may have been threaded into the validator's fields directly) with the new fields:

```rust
// Inside `if let Some(oauth) = self.oauthbearer { ... }`:
//
// In the JWT-mode branch (jwks_endpoint_uri set OR no introspection):
let custom_claim_check_compiled = oauth
    .custom_claim_check
    .as_deref()
    .map(|expr| {
        jsonpath_rust::JsonPathInst::from_str(expr).unwrap_or_else(|e| {
            panic!("[oauthbearer]: invalid custom_claim_check JsonPath expression: {e}")
        })
    });

// For the Unsecured branch:
v.custom_claim_check = custom_claim_check_compiled.clone();
v.valid_token_type = oauth.valid_token_type.clone();

// For the Signed branch:
v.custom_claim_check = custom_claim_check_compiled.clone();
v.valid_token_type = oauth.valid_token_type.clone();

// For the Introspection branch:
v.custom_claim_check = custom_claim_check_compiled;
// NO valid_token_type — introspection skips.
```

Adapt to the actual `apply_to` block's structure (the validator is built differently per mode). The key thing: compile the expression ONCE and clone it into the validator (cheap clone — `JsonPathInst` is small).

If the `JsonPathInst` doesn't implement `Clone`, instead recompile per-validator (panic still per-mode), or build a single `Arc<JsonPathInst>` and clone the Arc.

- [ ] **Step 20: Delete old `BrokerConfig` field threading**

In `apply_to`, find any lines that assigned `required_scope` or `scope_claim_name` to validator fields. Delete them.

DELETE the `OAuthBearerValidator` enum branches' references to `required_scope` / `scope_claim_name` if present. The new validator structs no longer have these fields, so the assignments don't compile.

- [ ] **Step 21: `BrokerConfig` — no new fields needed**

The plan: the validator stores the COMPILED `JsonPathInst` directly. `BrokerConfig` doesn't need to carry the string form — `FileOAuthBearerConfig` reads it, `apply_to` compiles + injects into the validator, done.

So `crates/broker/src/config.rs` requires NO changes in this task. (The plan-document's earlier "new oauthbearer_custom_claim_check_expression / oauthbearer_valid_token_type fields on BrokerConfig" was a half-step; the cleaner approach is direct validator injection.)

If for any reason the cleaner approach doesn't work (e.g., the validator's `apply_to` is async-late and can't compile), fall back to:
- Add `pub oauthbearer_custom_claim_check_compiled: Option<JsonPathInst>` and `pub oauthbearer_valid_token_type: Option<String>` on BrokerConfig.
- `apply_to` populates them.
- The validator-construction code (presumably elsewhere in the broker startup) reads them.

Implementer judgment per actual code shape.

- [ ] **Step 22: Update + run the workspace build**

```bash
cargo build --workspace 2>&1 | tail -20
```

Expected: clean build for `crabka-security` and `crabka-broker`. Failures in `crabka-operator` are EXPECTED — T2/T3 sweep those.

- [ ] **Step 23: fmt + clippy on broker side**

```bash
cargo fmt -p crabka-security -p crabka-broker -- --check
cargo clippy -p crabka-security -p crabka-broker --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 24: Commit**

```bash
git add crates/security/Cargo.toml crates/security/src/oauthbearer.rs crates/broker/src/file_config.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T1: broker — custom_claim_check JsonPath + valid_token_type checks

Adds jsonpath-rust dependency and wires customClaimCheck (JsonPath
expression evaluated against token claims, rejected when empty/null/
false) + validTokenType (JWT typ header check, JWT-mode only) into
all three OAuth validators. The expression compiles once at broker
startup; malformed expressions panic with a descriptive error.

Deletes the slice-50 scope-check stub: required_scope +
scope_claim_name fields on the validators, plus scope_contains()
and scope_claim_contains() and check_required_scope() helpers.
Operators rewrite `customClaimCheck: { scope: X }` to the JsonPath
equivalent `customClaimCheck: "@.scope == 'X'"`. Greenfield: no
compat shim.

12 new tests + 1 trivial compile-error test. JWT-mode validators
(Unsecured + Signed) check both fields; Introspection validator
checks only custom_claim_check (no JWT header to validate typ on).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 2 — T2 then T3 (sequential within batch; file-disjoint by design)

**Dispatch order:** T2 first; wait for its commit; then dispatch T3. T2 changes the struct shape that T3's fixtures depend on.

#### Task T2: Operator CRD shape change + own-file fixture sweep

**Files:**
- Modify: `crates/operator/src/crd/listener.rs`

- [ ] **Step 1: Write the failing round-trip test**

In `crates/operator/src/crd/listener.rs` test module:

```rust
#[test]
fn oauth_round_trip_with_custom_claim_check_string() {
    let yaml = r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
customClaimCheck: "@.scope == 'kafka.write'"
validTokenType: JWT
"#;
    let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
    let ListenerAuthentication::OAuth(oauth) = &parsed else {
        panic!("expected oauth variant");
    };
    assert_eq!(
        oauth.custom_claim_check.as_deref(),
        Some("@.scope == 'kafka.write'")
    );
    assert_eq!(oauth.valid_token_type.as_deref(), Some("JWT"));
}

#[test]
fn oauth_round_trip_without_custom_claim_check_and_valid_token_type_omits_both() {
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
        valid_token_type: None,
    };
    let auth = ListenerAuthentication::OAuth(cfg);
    let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
    assert!(
        !yaml.contains("customClaimCheck"),
        "None field must be omitted; got:\n{yaml}"
    );
    assert!(
        !yaml.contains("validTokenType"),
        "None field must be omitted; got:\n{yaml}"
    );
}

#[test]
fn oauth_old_custom_claim_check_object_shape_no_longer_parses() {
    // The slice-50 object shape `{ scope: ... }` is gone.
    let yaml = r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
customClaimCheck:
  scope: kafka.write
"#;
    let result: Result<ListenerAuthentication, _> = serde_yaml::from_str(yaml);
    assert!(
        result.is_err(),
        "old object shape must be rejected; got Ok"
    );
}
```

- [ ] **Step 2: Run tests — verify failure**

```bash
cargo test -p crabka-operator --lib crd::listener::auth_tests::oauth_round_trip_with_custom_claim_check_string 2>&1 | tail
```

Expected: compile error — `custom_claim_check` field type is `Option<OAuthCustomClaimCheck>` not `Option<String>`; `valid_token_type` field doesn't exist.

- [ ] **Step 3: Change `ListenerAuthenticationOAuth` shape**

In `crates/operator/src/crd/listener.rs`, modify the struct (lines 154–240). Replace the existing `custom_claim_check` field (around line 173):

```rust
// Was:
//     /// Optional required-scope check; see `OAuthCustomClaimCheck`.
//     #[serde(default, skip_serializing_if = "Option::is_none")]
//     pub custom_claim_check: Option<OAuthCustomClaimCheck>,
//
// Now:
    /// Slice 49g (replaces slice 50's typed stub): JsonPath expression
    /// (Jayway-flavored via jsonpath-rust) evaluated against the
    /// token's claim set. Token is rejected when the expression yields
    /// empty/null/false. Examples:
    /// `"@.scope == 'kafka.write'"`,
    /// `"@.roles contains 'admin' || @.groups in ['kafka-ops']"`.
    /// CRD-validated `minLength: 1` when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_claim_check: Option<String>,
```

After the existing `max_seconds_without_reauthentication` field (last field, slice 50d), add:

```rust
    /// Slice 49g: when set, the JWT `typ` header must equal this
    /// string. JWT-mode only — rejected with
    /// `ListenersValid=False reason=ListenerOauthValidTokenTypeRejectedInIntrospectionMode`
    /// when set on an `accessTokenIsJwt: false` listener (no JWT
    /// header in introspection responses). CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_token_type: Option<String>,
```

- [ ] **Step 4: Delete `OAuthCustomClaimCheck` struct entirely**

Find it (around line 256–266):

```bash
grep -n "OAuthCustomClaimCheck" crates/operator/src/crd/listener.rs
```

Delete the struct definition AND any `pub use OAuthCustomClaimCheck` re-exports in `crates/operator/src/crd/mod.rs` (or wherever the `crd` module re-exports). Grep:

```bash
grep -rn "OAuthCustomClaimCheck" crates/operator/src/
```

Every reference in `crates/operator/src/` must be removed. Some references in `controller/listeners.rs` will be touched by T3 — leave them for T3 to clean up.

- [ ] **Step 5: Update the hand-rolled schema**

In the same file, find `listener_authentication_schema` (around line 296). Replace the `customClaimCheck` block (lines 309–316):

```rust
// Was:
//             "customClaimCheck": {
//                 "type": "object",
//                 "required": ["scope"],
//                 "properties": {
//                     "scope": { "type": "string", "minLength": 1 },
//                     "scopeClaim": { "type": "string" },
//                 },
//             },
//
// Now:
            "customClaimCheck": { "type": "string", "minLength": 1 },
            "validTokenType": { "type": "string", "minLength": 1 },
```

Place `validTokenType` alphabetically — it slots between `tlsTrustedCertificates` and `userNameClaim` per the existing alphabetical order (verify by reading the surrounding entries).

- [ ] **Step 6: Sweep struct-literal sites in this file's tests**

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/crd/listener.rs
grep -n "OAuthCustomClaimCheck {" crates/operator/src/crd/listener.rs
```

For each `ListenerAuthenticationOAuth { ... }` literal, add `valid_token_type: None,` as the last field. Per the explore there are 16+ sites.

For each `OAuthCustomClaimCheck { scope: ..., scope_claim: ... }` literal IN THIS FILE: if it's used to set `custom_claim_check: Some(OAuthCustomClaimCheck { ... })`, rewrite the whole assignment to `custom_claim_check: Some("@.scope == 'kafka.write'".into())` (or the JsonPath equivalent of whatever the original scope was). Delete the `OAuthCustomClaimCheck` instance.

Also: any test that asserted on `OAuthCustomClaimCheck.scope` field needs rewriting. The new field is `custom_claim_check: Option<String>` — assertions check the string content.

The schema-regression test (search for `oauth_listener_authentication_schema_smoke` or similar) needs its expected-properties list updated: remove the OLD `customClaimCheck` object-shape assertion, add `customClaimCheck` string-shape + `validTokenType` string-shape.

- [ ] **Step 7: Run tests — verify pass**

```bash
cargo test -p crabka-operator --lib crd::listener 2>&1 | tail -15
```

Expected: 3 new round-trip tests pass + all existing tests still pass.

- [ ] **Step 8: fmt + clippy (scoped)**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail
```

Expected build-only failures in `controller/listeners.rs` (E0063 from missing `valid_token_type: None` fixtures, OR `OAuthCustomClaimCheck` references). Those are T3's job. fmt/clippy on the changed file should be clean.

- [ ] **Step 9: Commit**

```bash
git add crates/operator/src/crd/listener.rs crates/operator/src/crd/mod.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T2: operator CRD — replace customClaimCheck with string + add validTokenType

Changes `ListenerAuthenticationOAuth.custom_claim_check` from
`Option<OAuthCustomClaimCheck>` (slice 50's typed `{ scope, scope_claim }`
stub) to `Option<String>` (the raw JsonPath expression). Adds
`valid_token_type: Option<String>` field for Strimzi parity.

Deletes the `OAuthCustomClaimCheck` struct + its re-exports. Schema
entry rewritten: customClaimCheck now `string minLength:1`,
validTokenType added similarly.

3 new round-trip tests: with-fields, without-fields (omits from
YAML), and old-object-shape-no-longer-parses regression. Existing
struct-literal fixtures in this file's tests swept with the new
`valid_token_type: None` default and the rewritten string-form
`custom_claim_check`.

T3 follows up with the reconciler render + cross-mode validation +
divergence walk + sibling-file fixture sweeps.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T3: Operator reconciler + divergence + sibling-file sweep

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs`
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

**Prerequisite:** T2 must be committed first.

- [ ] **Step 1: Sweep struct-literal sites in `controller/listeners.rs`**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49g-oauth-validation-policies
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/listeners.rs
grep -n "OAuthCustomClaimCheck" crates/operator/src/controller/listeners.rs
```

For each `ListenerAuthenticationOAuth { ... }` literal: add `valid_token_type: None,` and rewrite `custom_claim_check` if it used the OAuthCustomClaimCheck shape.

The biggest site is the `base` fixture in the divergence walk test (around line 1507). After the sweep it looks like:

```rust
let base = crate::crd::ListenerAuthenticationOAuth {
    valid_issuer_uri: "https://issuer.example.com/".into(),
    jwks_endpoint_uri: Some("https://issuer.example.com/jwks".into()),
    valid_audience: Some("kafka".into()),
    user_name_claim: Some("preferred_username".into()),
    custom_claim_check: Some("@.scope == 'kafka.write'".into()),
    jwks_refresh_seconds: Some(300),
    max_clock_skew_seconds: Some(30),
    enable_oauth_bearer: true,
    tls_trusted_certificates: vec![],
    access_token_is_jwt: true,
    introspection_endpoint_uri: None,
    user_info_endpoint_uri: None,
    client_id: None,
    client_secret: None,
    introspection_http_timeout_seconds: None,
    max_seconds_without_reauthentication: None,
    valid_token_type: None,
};
```

The OLD divergence-walk perturbation for `custom_claim_check` (around line 1560–1565) uses the OAuthCustomClaimCheck shape:

```rust
// Was:
//     (
//         "custom_claim_check",
//         crate::crd::ListenerAuthenticationOAuth {
//             custom_claim_check: Some(crate::crd::OAuthCustomClaimCheck {
//                 scope: "kafka.read".into(),
//                 scope_claim: Some("scope".into()),
//             }),
//             ..base.clone()
//         },
//     ),
//
// Becomes:
    (
        "custom_claim_check",
        crate::crd::ListenerAuthenticationOAuth {
            custom_claim_check: Some("@.scope == 'kafka.read'".into()),
            ..base.clone()
        },
    ),
```

- [ ] **Step 2: Sweep `controller/kafka.rs` and `controller/kafka_node_pool.rs`**

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/kafka.rs
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/kafka_node_pool.rs
```

4 + 3 = 7 sites. Each gets `valid_token_type: None,` added as the last field. If any used `OAuthCustomClaimCheck`, rewrite to the JsonPath string form.

- [ ] **Step 3: Run cargo build — verify the sweep unbroke the workspace**

```bash
cargo build -p crabka-operator 2>&1 | tail
```

Expected: build succeeds. Any remaining E0063 errors mean a sweep site was missed; grep and fix.

- [ ] **Step 4: Update `render_broker_toml` — DELETE old render + ADD new render**

In `crates/operator/src/controller/listeners.rs`, find the `[oauthbearer]` block render (lines 2561–2637). DELETE the slice-50 render lines (the two `if let Some(ccc) = ...` blocks around line 2613–2620):

```rust
// DELETE these lines:
        if let Some(ccc) = &oauth_cfg.custom_claim_check
            && let Some(sc) = &ccc.scope_claim
        {
            let _ = writeln!(out, "scope_claim_name = \"{sc}\"");
        }
        if let Some(ccc) = &oauth_cfg.custom_claim_check {
            let _ = writeln!(out, "required_scope = \"{}\"", ccc.scope);
        }
```

Replace with:

```rust
        // Slice 49g: customClaimCheck (JsonPath expression) emission.
        if let Some(expr) = &oauth_cfg.custom_claim_check {
            // Use single-quoted TOML literal string to avoid escape
            // processing — JsonPath expressions can contain `\` and `"`.
            // Reject expressions containing single quotes here would be
            // an over-step; document in the YAML doc that single-quote
            // wrapping for string literals inside the expression must
            // use double-quote-then-escape if needed.
            let _ = writeln!(out, "custom_claim_check = '{expr}'");
        }
        // Slice 49g: validTokenType (JWT typ header check) emission.
        if let Some(typ) = &oauth_cfg.valid_token_type {
            let _ = writeln!(out, "valid_token_type = \"{typ}\"");
        }
```

**Single-quote vs double-quote** for the TOML string literal: JsonPath expressions commonly contain single quotes (e.g., `@.scope == 'kafka.write'`). TOML's double-quoted strings require escaping `"` and process `\` escapes; single-quoted (literal) strings don't escape-process AND can contain double quotes. BUT single-quoted strings can't contain single quotes (no escape).

The safest emission is multi-line literal `'''...'''` which can contain both quote types. Adjust the render to:

```rust
        if let Some(expr) = &oauth_cfg.custom_claim_check {
            // TOML multi-line literal string — no escape processing,
            // can contain both `'` and `"`.
            let _ = writeln!(out, "custom_claim_check = '''{expr}'''");
        }
```

If `expr` itself contains `'''` (very unlikely in a JsonPath), the render breaks. Document the limitation as an out-of-scope edge case OR validate the expression doesn't contain `'''` at reconcile time.

For initial implementation, the multi-line literal is the most-forgiving choice.

- [ ] **Step 5: Add the cross-mode validation rule**

In the same file, find `validate_listeners` (around line 202). The existing cross-mode block (lines 231–290) checks introspection-mode forbids JWT-mode fields and vice versa. After the existing JWT-mode block (the `if cfg.access_token_is_jwt { ... }` branch), the introspection-mode branch (`else { ... }`) currently checks 4 things. Add a 5th:

```rust
} else {
    // existing introspection-mode forbids/requires rules ...
    // Add this:
    if cfg.valid_token_type.is_some() {
        return Err(ValidationError::ListenerOauthValidTokenTypeRejectedInIntrospectionMode(
            format!(
                "listener '{}': accessTokenIsJwt=false forbids validTokenType (no JWT header in introspection responses)",
                l.name
            ),
        ));
    }
}
```

- [ ] **Step 6: Add the new `ValidationError` variant + DELETE old variant**

In `crates/operator/src/controller/listeners.rs`, find the `ValidationError` enum (around line 53). DELETE the obsolete `ListenerOauthCustomClaimCheckScopeEmpty(String)` variant (slice 50). Find any callers:

```bash
grep -n "ListenerOauthCustomClaimCheckScopeEmpty" crates/operator/src/
```

If any callers exist (probably in `validate_listeners` itself), delete them — they're slice-50 residue that's no longer relevant (the new `customClaimCheck: String` can't have an "empty scope" check; CRD's `minLength: 1` already rejects empty strings).

ADD the new variant:

```rust
    /// Slice 49g: `validTokenType` set on an `accessTokenIsJwt: false`
    /// listener. Introspection-mode validation has no JWT header to
    /// check `typ` against; the field is rejected. The `String` carries
    /// the listener name + a human-readable description.
    ListenerOauthValidTokenTypeRejectedInIntrospectionMode(String),
```

Also: check the dispatch/reason-mapping. The reason string for `Ready=False reason=...` (or `ListenersValid=False reason=...`) is conventionally the variant name. Find where ValidationError gets mapped to a reason string:

```bash
grep -n "ValidationError::" crates/operator/src/controller/listeners.rs | head -20
grep -n "fn reason\|impl.*ValidationError" crates/operator/src/controller/listeners.rs
```

If there's a centralized mapping table, add the new variant there. If reasons are inline per-call-site (per slice 50c's pattern), add the mapping at the call site (likely `controller/kafka.rs` per slice 50c).

```bash
grep -n "ValidationError" crates/operator/src/controller/kafka.rs
```

- [ ] **Step 7: Failing unit test for `render_broker_toml` emission**

Add to the test module in `controller/listeners.rs`:

```rust
#[test]
fn render_broker_toml_emits_custom_claim_check_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.custom_claim_check = Some("@.scope == 'kafka.write'".into());
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args matching existing tests */);
    assert!(
        toml.contains("custom_claim_check = '''@.scope == 'kafka.write''''"),
        "expected custom_claim_check render; got:\n{toml}"
    );
}

#[test]
fn render_broker_toml_emits_valid_token_type_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.valid_token_type = Some("JWT".into());
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(
        toml.contains("valid_token_type = \"JWT\""),
        "expected valid_token_type render; got:\n{toml}"
    );
}

#[test]
fn render_broker_toml_omits_custom_claim_check_when_unset() {
    let oauth = oauth_full_cfg(); // None default
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(
        !toml.contains("custom_claim_check"),
        "TOML must omit custom_claim_check when None; got:\n{toml}"
    );
}
```

Adapt `oauth_full_cfg()` and `oauth_listener_for_render()` to existing helper names. The `''''` escaping in the assertion is because the multi-line literal closer is `'''` and TOML interprets adjacent `'` as ending the string — but a single embedded `'` inside `'''...'''` is fine. Adjust based on actual `render_broker_toml` output.

- [ ] **Step 8: Failing unit test for cross-mode validation**

```rust
#[test]
fn validate_listeners_rejects_valid_token_type_in_introspection_mode() {
    let cfg = crate::crd::ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://iss.example/".into(),
        jwks_endpoint_uri: None,
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: false, // introspection mode
        introspection_endpoint_uri: Some("https://iss.example/introspect".into()),
        user_info_endpoint_uri: None,
        client_id: Some("kafka-broker".into()),
        client_secret: Some(crate::crd::OauthClientSecretRef {
            secret_name: "creds".into(),
            key: "client-secret".into(),
        }),
        introspection_http_timeout_seconds: None,
        max_seconds_without_reauthentication: None,
        valid_token_type: Some("JWT".into()), // The violation
    };
    let listeners = vec![
        crate::crd::Listener {
            name: "oauth".into(),
            port: 9096,
            type_: crate::crd::ListenerType::Internal,
            tls: false,
            authentication: Some(crate::crd::ListenerAuthentication::OAuth(cfg)),
            configuration: None,
            network_policy_peers: None,
        },
    ];
    let result = validate_listeners(&listeners);
    assert!(matches!(
        result,
        Err(ValidationError::ListenerOauthValidTokenTypeRejectedInIntrospectionMode(_))
    ));
}
```

- [ ] **Step 9: Run failing tests — verify they fail**

```bash
cargo test -p crabka-operator --lib controller::listeners::tests::render_broker_toml_emits_custom_claim_check 2>&1 | tail
cargo test -p crabka-operator --lib controller::listeners::tests::validate_listeners_rejects_valid_token_type 2>&1 | tail
```

Expected: the validate test FAILS first (the validation rule doesn't exist yet). The render tests FAIL until step 4 lands (DELETE old + ADD new render).

- [ ] **Step 10: Run tests — verify all listeners tests pass**

```bash
cargo test -p crabka-operator --lib controller::listeners 2>&1 | tail -15
```

Expected: all pass. New tests pass; existing tests still green after the sweep.

- [ ] **Step 11: Add divergence-walk perturbation for `valid_token_type`**

In the same test (`validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field`), at the end of the `perturbations` vec, add:

```rust
        (
            "valid_token_type",
            crate::crd::ListenerAuthenticationOAuth {
                valid_token_type: Some("JWT".into()),
                ..base.clone()
            },
        ),
```

- [ ] **Step 12: Run the divergence walk test**

```bash
cargo test -p crabka-operator --lib controller::listeners::tests::validate_listeners_rejects_two_oauth_listeners_with_divergent_config 2>&1 | tail
```

Expected: pass with one more perturbation case asserted (10 total now: 9 from previous slices + `valid_token_type`).

- [ ] **Step 13: fmt + clippy**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean. (T4 will sweep integration tests; clippy `--tests` may flag E0063 there — that's T4's responsibility.)

- [ ] **Step 14: Commit**

```bash
git add crates/operator/src/controller/listeners.rs crates/operator/src/controller/kafka.rs crates/operator/src/controller/kafka_node_pool.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T3: operator reconciler — render JsonPath + validTokenType; cross-mode + divergence

Replaces the slice-50 render of `required_scope` / `scope_claim_name`
under [oauthbearer] with the new emissions:
  custom_claim_check = '''<expr>'''  (TOML multi-line literal, no escapes)
  valid_token_type   = "<value>"

Adds cross-mode validation: `validTokenType` rejected with
ListenerOauthValidTokenTypeRejectedInIntrospectionMode when
accessTokenIsJwt=false (introspection responses have no JWT header).

Extends the cross-listener divergence walk with a valid_token_type
perturbation; rewrites the existing custom_claim_check perturbation
to use the new string shape.

Deletes the obsolete ListenerOauthCustomClaimCheckScopeEmpty
ValidationError variant (slice 50 residue — CRD minLength:1
already rejects empty customClaimCheck strings).

Sweep: 7 sibling-file fixture sites in controller/kafka.rs and
controller/kafka_node_pool.rs picked up the new valid_token_type:
None default for atomic compile.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 3 — T4 ‖ T5 (truly parallel, file-disjoint)

#### Task T4: Operator integration tests + sample + CRD regen

**Files:**
- Modify: `crates/operator/tests/reconcile_listener_oauth.rs`
- Modify: `crates/operator/tests/reconcile_oauth_introspection.rs`
- Modify: `crates/operator/tests/reconcile_oauth_trust.rs`
- Modify: `crates/operator/sample/oauth-listener.yaml`
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (regenerated)

- [ ] **Step 1: Sweep fixtures in all 3 integration test files**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49g-oauth-validation-policies
grep -n "ListenerAuthenticationOAuth {" crates/operator/tests/reconcile_listener_oauth.rs crates/operator/tests/reconcile_oauth_introspection.rs crates/operator/tests/reconcile_oauth_trust.rs
grep -n "OAuthCustomClaimCheck" crates/operator/tests/reconcile_listener_oauth.rs crates/operator/tests/reconcile_oauth_introspection.rs crates/operator/tests/reconcile_oauth_trust.rs
```

For each `ListenerAuthenticationOAuth { ... }` literal: add `valid_token_type: None,` as last field.

For each `OAuthCustomClaimCheck` reference (3 sites in `reconcile_listener_oauth.rs`): the slice-50 stub is gone. If the test used `OAuthCustomClaimCheck { scope: 'X', scope_claim: Some('scope') }`, rewrite the assignment to `custom_claim_check: Some("@.scope == 'X'".into())` (using the appropriate scope value the test originally checked).

Specifically `oauth_cfg_full` in `tests/reconcile_listener_oauth.rs` (line 90+) replaces:

```rust
        custom_claim_check: Some(OAuthCustomClaimCheck {
            scope: "kafka.write".into(),
            scope_claim: Some("scope".into()),
        }),
```

with:

```rust
        custom_claim_check: Some("@.scope == 'kafka.write'".into()),
```

Also: remove the `use crabka_operator::crd::OAuthCustomClaimCheck` (or similar) import at the top of the file.

- [ ] **Step 2: Add 3 new integration tests**

In `crates/operator/tests/reconcile_listener_oauth.rs`, mirror the existing test pattern (full `reconcile` + `extract_broker0_toml`):

```rust
#[tokio::test]
async fn oauth_listener_with_custom_claim_check_expression_renders_broker_toml_key() {
    let mut cfg = oauth_cfg_minimal();
    cfg.custom_claim_check = Some("@.scope == 'kafka.write'".into());
    // ... reconcile + extract broker TOML pattern (see test 1 in this file)
    let toml = /* extract broker TOML */;
    assert!(
        toml.contains("custom_claim_check = "),
        "expected custom_claim_check to render; got:\n{toml}"
    );
    assert!(
        toml.contains("@.scope == 'kafka.write'"),
        "expected the expression body; got:\n{toml}"
    );
}

#[tokio::test]
async fn oauth_listener_with_valid_token_type_renders_broker_toml_key() {
    let mut cfg = oauth_cfg_minimal();
    cfg.valid_token_type = Some("JWT".into());
    // ... reconcile + extract broker TOML
    let toml = /* extract */;
    assert!(
        toml.contains("valid_token_type = \"JWT\""),
        "expected valid_token_type render; got:\n{toml}"
    );
}

#[tokio::test]
async fn oauth_listener_valid_token_type_in_introspection_mode_rejected_with_listeners_valid_false() {
    // Build a Kafka CR with an introspection-mode OAuth listener that
    // also sets validTokenType. Reconcile must reject with
    // ListenersValid=False reason=ListenerOauthValidTokenTypeRejectedInIntrospectionMode.
    let mut cfg = oauth_cfg_minimal();
    cfg.access_token_is_jwt = false;
    cfg.jwks_endpoint_uri = None;
    cfg.introspection_endpoint_uri = Some("https://iss.example/introspect".into());
    cfg.client_id = Some("kafka-broker".into());
    cfg.client_secret = Some(crabka_operator::crd::OauthClientSecretRef {
        secret_name: "creds".into(),
        key: "client-secret".into(),
    });
    cfg.valid_token_type = Some("JWT".into());
    // ... reconcile, expect a non-success status with the right reason
    // Use the same pattern as test 17 (slice 50d's divergence-rejection
    // integration test) — assert via the rules_for_failure_path pattern.
}
```

Pattern-match the existing tests for the exact `reconcile` call shape + assertion helpers.

- [ ] **Step 3: Run new integration tests — verify pass**

```bash
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_with_custom_claim_check 2>&1 | tail
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_with_valid_token_type 2>&1 | tail
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_valid_token_type_in_introspection 2>&1 | tail
```

Expected: all 3 pass.

- [ ] **Step 4: Run full operator test suite**

```bash
cargo test -p crabka-operator 2>&1 | tail -15
```

Expected: all green.

- [ ] **Step 5: Update the sample manifest**

Edit `crates/operator/sample/oauth-listener.yaml`. Replace the existing `customClaimCheck:` block (lines 27–28):

```yaml
# Was:
#         customClaimCheck:
#           scope: kafka.write
#
# Now:
        customClaimCheck: "@.scope == 'kafka.write'"
        # Optional: enforce JWT `typ` header (slice 49g).
        # JWT-mode only; rejected on introspection-mode listeners.
        # validTokenType: JWT
```

Maintain 8-space indentation for keys (matches sibling `validIssuerUri:`, etc.).

Verify YAML still parses:

```bash
cat crates/operator/sample/oauth-listener.yaml | python3 -c "import sys, yaml; docs = list(yaml.safe_load_all(sys.stdin)); print(f'{len(docs)} docs: {[d.get(\"kind\") for d in docs]}')"
```

Expected: `3 docs: ['Kafka', 'KafkaNodePool', 'KafkaUser']`.

- [ ] **Step 6: Regenerate CRDs**

```bash
bash tools/regen-crds.sh 2>&1 | tail -10
git diff --stat deploy/crds/
git diff deploy/crds/crabka.io_kafkas.yaml | head -40
```

Expected: ONLY `deploy/crds/crabka.io_kafkas.yaml` changed. The diff should:
- DELETE the existing `customClaimCheck` object-shape entry (with `scope` + `scopeClaim` sub-properties).
- ADD a new `customClaimCheck: { type: string, minLength: 1 }` entry.
- ADD a new `validTokenType: { type: string, minLength: 1 }` entry.

- [ ] **Step 7: fmt + clippy**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add crates/operator/tests/reconcile_listener_oauth.rs crates/operator/tests/reconcile_oauth_introspection.rs crates/operator/tests/reconcile_oauth_trust.rs crates/operator/sample/oauth-listener.yaml deploy/crds/crabka.io_kafkas.yaml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T4: operator integration tests + sample + CRD regen for slice 49g

Sweeps ~8 fixture sites across 3 integration test files: adds
valid_token_type: None default; rewrites OAuthCustomClaimCheck
constructions to the new custom_claim_check: Option<String> shape.

3 new integration tests:
- custom_claim_check expression renders to broker TOML
- valid_token_type renders to broker TOML
- validTokenType on introspection-mode listener rejected as
  ListenerOauthValidTokenTypeRejectedInIntrospectionMode

Sample manifest: customClaimCheck rewritten to JsonPath string form
with a commented-out validTokenType hint.

CRD regenerated: customClaimCheck flips from object-shape to string;
validTokenType added as a sibling string field.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T5: kind-oauth e2e CR YAML rewrite

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

**Race awareness:** T4 is running in parallel on this branch. T4 touches `crates/operator/tests/*`, `sample/`, `deploy/crds/` — all disjoint from your file. Before commit: `git pull --rebase 2>&1` (likely no-op) + `git status`. If `git commit` hits index.lock, sleep 2s and retry.

- [ ] **Step 1: Read the existing customClaimCheck block**

```bash
sed -n '2250,2265p' .github/workflows/operator-e2e.yml
```

Confirm the existing shape:

```yaml
        customClaimCheck:
          scope: kafka.write
```

- [ ] **Step 2: Rewrite to the JsonPath string shape + add validTokenType**

Edit `.github/workflows/operator-e2e.yml`. Replace the existing `customClaimCheck` block with:

```yaml
                customClaimCheck: "@.scope == 'kafka.write'"
                validTokenType: JWT
```

The indentation MUST match the sibling keys (`validIssuerUri:`, `customClaimCheck:`, `tlsTrustedCertificates:`). Per the explore that's 16 spaces of leading whitespace.

- [ ] **Step 3: Verify YAML parses + actionlint clean**

```bash
python3 -c "
import yaml
w = yaml.safe_load(open('.github/workflows/operator-e2e.yml'))
jobs = list(w['jobs'].keys())
print('jobs:', jobs)
assert 'kind-oauth' in jobs
"
```

Expected: no parse errors, kind-oauth in the list.

```bash
which actionlint && actionlint .github/workflows/operator-e2e.yml 2>&1 | head -20 || echo "actionlint not installed; skip"
```

Pre-existing warnings (from prior slices) are fine; NO new warnings.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/operator-e2e.yml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T5: kind-oauth e2e — JsonPath customClaimCheck + validTokenType

Replaces the slice-50 customClaimCheck object shape ({scope:...}) with
the new Strimzi-shape JsonPath string ("@.scope == 'kafka.write'") and
adds validTokenType: JWT to exercise the new JWT typ header check.

Keycloak's default tokens carry typ=JWT, so the typ check passes for
producers using the existing realm bootstrap. Producer Jobs unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 4 — T6 (alone)

#### Task T6: STATUS.md + final gate

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Read slice 50d's STATUS entry for tone**

```bash
grep -n "^## Slice 50d " STATUS.md
# Then read that section.
```

- [ ] **Step 2: Append the slice 49g entry**

Append to `STATUS.md`:

```markdown
## Slice 49g — Operator + Broker: OAUTHBEARER validation policies (customClaimCheck JsonPath + validTokenType) (2026-05-24)

First of the three "long-tail" clusters closing out the OAUTHBEARER
umbrella's Strimzi field parity. Replaces slice 50's typed
`customClaimCheck: { scope, scope_claim }` stub with the full Strimzi
string-expression shape (JsonPath via jsonpath-rust); adds
`validTokenType` (JWT `typ` header check, JWT-mode only).

- **Broker (`crates/security/`, `crates/broker/`):** new
  `jsonpath-rust` runtime dependency in `crabka-security`. New
  `[oauthbearer].custom_claim_check: String` TOML key (Jayway-
  flavored JsonPath, compiled at broker startup). New
  `[oauthbearer].valid_token_type: String` TOML key (JWT-mode
  validators check; introspection skips). All three validators
  (`UnsecuredJwsValidator`, `SignedJwsValidator`,
  `IntrospectionValidator`) carry `Option<JsonPathInst>` for the
  pre-compiled expression. JWT-mode validators additionally carry
  `Option<String>` for `valid_token_type`.
- **Slice-50 stub removed:** `required_scope` / `scope_claim_name`
  fields on validators + the `scope_contains` / `scope_claim_contains`
  / `check_required_scope` helpers deleted. Operators rewrite
  `customClaimCheck: { scope: 'X' }` to
  `customClaimCheck: "@.scope == 'X'"`. Greenfield: no compat shim.
- **Operator CRD (`crates/operator/src/crd/listener.rs`):**
  `custom_claim_check: Option<OAuthCustomClaimCheck>` (typed struct,
  slice 50) → `custom_claim_check: Option<String>`. `OAuthCustomClaimCheck`
  struct + its schema entry + re-exports deleted. New
  `valid_token_type: Option<String>` field. Hand-rolled schema entries:
  `customClaimCheck` flips to string `minLength: 1`; `validTokenType`
  added similarly.
- **Operator reconciler (`crates/operator/src/controller/listeners.rs`):**
  `render_broker_toml` emits `custom_claim_check = '''<expr>'''` (TOML
  multi-line literal, no escape processing) and `valid_token_type = "<v>"`.
  New cross-mode validation:
  `ListenerOauthValidTokenTypeRejectedInIntrospectionMode` fires when
  `validTokenType` is set on an `accessTokenIsJwt: false` listener.
  Existing `ListenerOauthCustomClaimCheckScopeEmpty` ValidationError
  variant deleted (slice 50 residue — CRD `minLength: 1` already
  rejects empty strings). Cross-listener divergence walk extended with
  a `valid_token_type` perturbation; the existing `custom_claim_check`
  perturbation rewritten to the new string shape.
- **Scope expansion (CLAUDE.md greenfield rule):** T2/T3/T4 swept
  ~30 fixture sites across operator code/tests (notably 11 in
  `controller/listeners.rs`, 4 in `controller/kafka.rs`, 3 in
  `controller/kafka_node_pool.rs`, 8 across the three integration
  test files) so the struct extension + deletion compiled atomically.
- **E2E (`.github/workflows/operator-e2e.yml`):** existing `kind-oauth`
  job's Kafka CR YAML rewrote `customClaimCheck` to the JsonPath shape
  and added `validTokenType: JWT`. Same producer Jobs (Keycloak emits
  `typ: JWT` by default).
- **Tests:** ~17 new (13 broker unit across the three validators +
  3 operator CRD round-trip + 2 reconciler unit + extended divergence
  walk + 3 operator integration). Workspace fmt + clippy `-D warnings`
  + tests + CRD drift gate all green.
- **Reference doc:** `[docs/superpowers/specs/2026-05-24-crabka-oauth-validation-policies-49g-design.md]`
- **Semantic divergence from Strimzi (acknowledged):** Crabka uses
  jsonpath-rust (Jayway-flavored, github.com/besok/jsonpath-rust).
  Common expressions parse identically; edge cases (Jayway-specific
  operators like `=~` regex or nested filter `?(@.x > 0 && @.y < 10)`)
  may differ. YAML field shape matches Strimzi exactly.
- **Out of scope:** slice 49h (claims mapping — `groupsClaim`,
  `groupsClaimDelimiter`, `fallbackUserNameClaim`, `fallbackUserNamePrefix`);
  slice 49i (JWKS refresher policies — `jwksMinRefreshPauseSeconds`,
  `jwksExpirySeconds`, `jwksIgnoreKeyUse`); slice 49f
  (PLAIN-with-OAuth-token, skipped indefinitely).
```

- [ ] **Step 3: Final gate**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49g-oauth-validation-policies
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

Not a T6 blocker if it's still flaky after isolation.

- [ ] **Step 4: Commit**

```bash
git add STATUS.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
Slice 49g: STATUS.md entry + final gate

Documents the new operator + broker OAUTHBEARER validation surface:
customClaimCheck flips from typed struct to JsonPath string;
validTokenType added for JWT typ header validation. Slice-50
scope-check stub deleted. fmt + clippy + workspace tests + CRD
drift gate all green.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Notes

- **Dependency chain:** T1 → T2 → T3 → (T4 ‖ T5) → T6. Six tasks, four batches. T2 and T3 are file-disjoint by design but dispatched sequentially because T3's fixture sweep depends on T2's struct field changes. T4 and T5 are truly parallel.
- **CLAUDE.md greenfield:** The slice-50 typed `OAuthCustomClaimCheck` struct is DELETED, not deprecated. The `required_scope` / `scope_claim_name` broker fields are DELETED. ~30 fixture sweep sites across operator + tests get the new defaults atomically.
- **Breaking change footprint** is larger than slice 50d (which only ADDED a field). This slice REPLACES a field shape. The slice-50 sample manifest, e2e job, and all in-tree fixtures rewrite in one coordinated commit chain. No out-of-tree users yet (greenfield project).
- **JsonPath crate verification:** the plan assumes `jsonpath-rust = "1.0"` and a `JsonPathInst::from_str` + `find_slice` API. T1 step 1 + step 6 instruct the implementer to verify the actual current version and API names before committing. If `jsonpath-rust 1.x` exposes a different API surface (e.g., `query()` instead of `find_slice()`), adapt the helper in step 6.
- **TOML literal-string escaping for customClaimCheck:** T3 step 4 uses TOML multi-line literal (`'''...'''`) for the rendered key, which avoids both `\` escape processing AND single-quote/double-quote conflicts. Edge case: an expression containing `'''` itself would break the render — document as out-of-scope (extremely unlikely in real JsonPath).
- **After 49g lands:** umbrella reaches 5/7 cluster-equivalents. **49h** (claims mapping — Principal-touching) and **49i** (JWKS refresher policies — refresher-loop-touching) follow independently. After both ship, the OAUTHBEARER umbrella reaches Strimzi field parity (modulo skipped 49f).
