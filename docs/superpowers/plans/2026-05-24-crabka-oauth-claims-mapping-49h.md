# Slice 49h — OAUTHBEARER claims mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Strimzi's `fallbackUserNameClaim` + `fallbackUserNamePrefix` (principal-name fallback chain) and `groupsClaim` + `groupsClaimDelimiter` (groups extraction via JsonPath) on the broker + operator surfaces. Adds `Principal.groups: Vec<String>` field, populated by OAuth validators; no broker-side authorizer reads it yet (scaffolding for slice 53/54).

**Architecture:** Six tasks across four batches (mirrors slice 49g's shape). T1 is the largest: extends `Principal`, sweeps ~57 workspace-wide `Principal { ... }` literal sites, updates all 3 OAuth validators with the 4 new fields + name-fallback + groups extraction, adds the `extract_groups` helper, wires `FileOAuthBearerConfig`. T2 + T3 each touch one operator file (CRD + reconciler). T4 + T5 are file-disjoint parallel work (integration tests + sample + CRD regen ‖ kind-oauth + kind-oauth-introspection e2e CR rewrites + Keycloak realm bootstrap extension). T6 ships STATUS + final gate.

**Tech Stack:** Rust, jsonpath-rust (already in workspace deps from slice 49g — reused for groupsClaim), serde (TOML/YAML), kube-rs, schemars, existing slice 49b/49d/49g/50d validator surfaces.

**Spec:** `docs/superpowers/specs/2026-05-24-crabka-oauth-claims-mapping-49h-design.md` (commit `b8e2789`).

**Worktree:** `/Users/mattstone/git/crabka/.worktrees/slice-49h-oauth-claims-mapping` on branch `slice-49h-oauth-claims-mapping`. Verify with `git branch --show-current`. Commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`.

---

## File structure

| File | Responsibility | Touched by |
|---|---|---|
| `crates/security/src/principal.rs` | New `Principal.groups: Vec<String>` field | T1 |
| `crates/security/src/oauthbearer.rs` | 4 new fields on each validator struct; name-fallback + groups extraction in `validate()` bodies; new `extract_groups` helper; unit tests | T1 |
| `crates/broker/src/file_config.rs` | 4 new `FileOAuthBearerConfig` fields + `apply_to` threading (groups_claim compiled at startup like custom_claim_check) | T1 |
| `crates/security/src/plain.rs` + `scram/server.rs` + tests | `Principal { ... }` sweep: add `groups: vec![]` to each construction site | T1 |
| `crates/broker/src/network/auth.rs` + `dispatch.rs` + `authorizer.rs` + tests | `Principal { ... }` sweep across all PLAIN/SCRAM/mTLS/OAuth construction sites | T1 |
| `crates/operator/src/crd/listener.rs` | 4 new `ListenerAuthenticationOAuth` fields + hand-rolled schema + own-file fixture sweep + round-trip tests | T2 |
| `crates/operator/src/controller/listeners.rs` | render_broker_toml emits 4 new keys; divergence walk extension; own-file fixture sweep + unit tests | T3 |
| `crates/operator/src/controller/kafka.rs` + `kafka_node_pool.rs` | Fixture sweep (4+3 sites) | T3 |
| `crates/operator/tests/reconcile_*.rs` | Fixture sweep (5+2+1 sites) + 2 new integration tests | T4 |
| `crates/operator/sample/oauth-listener.yaml` | Commented-out hint lines for the 4 new fields | T4 |
| `deploy/crds/crabka.io_kafkas.yaml` | Regenerated CRD (4 new properties) | T4 |
| `.github/workflows/operator-e2e.yml` | `kind-oauth` AND `kind-oauth-introspection` CR YAMLs add `groupsClaim`; Keycloak realm bootstrap extended with a role + service-account mapping for both jobs | T5 |
| `STATUS.md` | Slice 49h entry | T6 |

---

## Batches

### Batch 1 — T1 (broker, alone, large)

#### Task T1: Principal extension + validator changes + Principal cascade sweep + extract_groups helper

**Files:**
- Modify: `crates/security/src/principal.rs`
- Modify: `crates/security/src/oauthbearer.rs`
- Modify: `crates/broker/src/file_config.rs`
- Sweep: `crates/security/src/plain.rs` + `crates/security/src/scram/server.rs` + tests
- Sweep: `crates/broker/src/network/auth.rs` + `crates/broker/src/network/dispatch.rs` + `crates/broker/src/authorizer.rs` + tests

**Context:** T1 is the largest task in this slice. Three things happen in this commit:
1. `Principal` gains a `groups: Vec<String>` field. Cascades to ~57 construction sites across the workspace.
2. All 3 OAuth validators (Unsecured/Signed/Introspection) gain 4 new fields and execute name-fallback + groups extraction logic.
3. `FileOAuthBearerConfig` gains 4 new fields and threads them through `apply_to`.

- [ ] **Step 1: Add `groups: Vec<String>` field to `Principal`**

Edit `crates/security/src/principal.rs`. Replace the existing `Principal` struct (lines 43-47):

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub auth_method: AuthMethod,
    /// Slice 49h: OAuth-derived group memberships from the listener's
    /// `groupsClaim`. Empty vec for non-OAuth principals (PLAIN/SCRAM/
    /// mTLS/anonymous) and for OAuth principals whose listener has no
    /// `groupsClaim` configured. No broker-side authorizer reads this
    /// yet (slice 53/54 will); populated as scaffolding + for
    /// observability.
    pub groups: Vec<String>,
}
```

- [ ] **Step 2: Run cargo build — verify the cascade is broken**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49h-oauth-claims-mapping
cargo build --workspace 2>&1 | tail -30
```

Expected: many E0063 errors across `crates/broker/`, `crates/security/`, possibly `crates/operator/` and `crates/client-core/`. Note the count + which files are affected.

- [ ] **Step 3: Sweep all `Principal { ... }` literal construction sites — add `groups: vec![]`**

Find every literal construction:

```bash
grep -rn "Principal {" crates/ --include="*.rs" | grep -v "^.*://" | head -80
```

The grep matches BOTH construction sites (`Principal { name, auth_method }`) and destructuring patterns (`Principal { name, .. }` in match arms). Construction sites must be updated; destructure patterns with `..` are fine.

For each construction site, add `groups: vec![]` as the last field. Examples per file (the explore counted these — your actual counts may differ slightly):

- `crates/security/src/plain.rs:22` — PLAIN authenticator construction site.
- `crates/security/src/scram/server.rs:168` — SCRAM authenticator success.
- `crates/security/src/oauthbearer.rs:209, 426, 589` — OAuth validators (T1 will rewrite these in Step 8/10/12 anyway; add `groups: vec![]` as a placeholder, then it gets overwritten with the real extraction logic).
- `crates/broker/src/authorizer.rs:165, 735, 739` — authorizer construction sites (likely tests).
- `crates/broker/src/network/auth.rs:802, 820, 844, 872, 904, 952, 985, 1013, ...` — PLAIN/SCRAM/OAUTHBEARER auth-success paths in handlers + tests.
- `crates/broker/src/network/dispatch.rs:154, 167, ...` — mTLS + anonymous PLAINTEXT init + ~30 test fixtures.

For destructures (the literal pattern, not constructor):

```rust
// Was:
match auth { Principal { name, auth_method } => ... }
// Becomes:
match auth { Principal { name, auth_method, .. } => ... }
```

Or destructure `groups` explicitly if the code needs it (unlikely outside OAuth validators in this slice).

After each file's sweep, run `cargo build -p <crate>` to confirm that crate compiles before moving to the next.

- [ ] **Step 4: Confirm workspace builds**

```bash
cargo build --workspace 2>&1 | tail
```

Expected: clean. If E0063 errors remain, find the missed sites and fix.

- [ ] **Step 5: Write failing fallback-claim unit test (unsecured validator)**

Edit `crates/security/src/oauthbearer.rs`. In the existing test module for unsecured-validator tests, add:

```rust
#[test]
fn unsecured_validate_uses_primary_principal_claim_when_present() {
    // Regression: primary claim present → use primary, no prefix.
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({"sub": "alice", "exp": exp_secs}),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.fallback_user_name_claim = Some("client_id".into());
    v.fallback_user_name_prefix = Some("service-account-".into());
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.name, "alice"); // primary, no prefix
}

#[test]
fn unsecured_validate_falls_back_to_alt_claim_when_primary_absent() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({"client_id": "svc1", "exp": exp_secs}),
        // No `sub` — primary lookup fails.
    );
    let mut v = UnsecuredJwsValidator::default();
    v.fallback_user_name_claim = Some("client_id".into());
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.name, "svc1"); // fallback, no prefix
}

#[test]
fn unsecured_validate_applies_fallback_prefix_only_on_fallback() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({"client_id": "svc1", "exp": exp_secs}),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.fallback_user_name_claim = Some("client_id".into());
    v.fallback_user_name_prefix = Some("service-account-".into());
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.name, "service-account-svc1");
}

#[test]
fn unsecured_validate_rejects_when_neither_primary_nor_fallback_present() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({"exp": exp_secs}),
        // Neither sub nor client_id.
    );
    let mut v = UnsecuredJwsValidator::default();
    v.fallback_user_name_claim = Some("client_id".into());
    assert_eq!(v.validate(&token, now_ms), Err(AuthError::InvalidToken));
}
```

- [ ] **Step 6: Run unit tests — verify they fail to compile**

```bash
cargo test -p crabka-security oauthbearer::tests::unsecured_validate_uses_primary_principal_claim_when_present 2>&1 | tail
```

Expected: compile error — `UnsecuredJwsValidator` has no `fallback_user_name_claim` field.

- [ ] **Step 7: Extend `UnsecuredJwsValidator` struct + add the 4 new fields**

Replace the struct (lines 118-132):

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct UnsecuredJwsValidator {
    pub principal_claim_name: String,
    pub allowable_clock_skew_ms: i64,
    pub custom_claim_check: Option<JpQuery>,
    pub valid_token_type: Option<String>,
    /// Slice 49h: alternate claim name to read the principal name from
    /// when `principal_claim_name` is absent or empty. Strimzi's
    /// "service-account fallback" — `sub` typically holds a UUID,
    /// `client_id` is the readable name.
    pub fallback_user_name_claim: Option<String>,
    /// Slice 49h: prepended to the resolved principal name ONLY when
    /// the fallback claim fires. Strimzi convention: "service-account-".
    pub fallback_user_name_prefix: Option<String>,
    /// Slice 49h: precompiled JsonPath expression extracting group
    /// memberships from the token claims. Compile-once-at-startup.
    pub groups_claim: Option<JpQuery>,
    /// Slice 49h: when `groups_claim` resolves to a string (not an
    /// array), split on this delimiter. Common: "," or " ".
    pub groups_claim_delimiter: Option<String>,
}
```

`Default::default()` will set the new fields to `None` (and `vec![]` semantics implied by `Option`).

If `Default` is hand-rolled rather than derived, grep + update:
```bash
grep -n "impl Default for UnsecuredJwsValidator" crates/security/src/oauthbearer.rs
```

- [ ] **Step 8: Update `UnsecuredJwsValidator::validate()` body — name fallback + groups extraction**

Replace the existing principal-name extraction block (lines 201-206) and the `Ok(AuthOutcome { ... })` (lines 208-214) with:

```rust
        // Slice 49h: primary → fallback → reject. Prefix applied only
        // when fallback fires.
        let (raw_name, used_fallback) = match claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            Some(n) => (n.to_string(), false),
            None => {
                let fallback_claim = self.fallback_user_name_claim
                    .as_deref()
                    .ok_or(AuthError::InvalidToken)?;
                let raw = claims
                    .get(fallback_claim)
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or(AuthError::InvalidToken)?;
                (raw.to_string(), true)
            }
        };
        let name = if used_fallback {
            match &self.fallback_user_name_prefix {
                Some(prefix) => format!("{prefix}{raw_name}"),
                None => raw_name,
            }
        } else {
            raw_name
        };

        // Slice 49h: groups extraction.
        let groups = match &self.groups_claim {
            Some(path) => extract_groups(path, &claims, self.groups_claim_delimiter.as_deref()),
            None => Vec::new(),
        };

        Ok(AuthOutcome {
            principal: Principal {
                name,
                auth_method: AuthMethod::SaslOAuthBearer,
                groups,
            },
            expires_at_ms: Some(exp_ms),
        })
```

- [ ] **Step 9: Add the `extract_groups` helper**

Add this private helper in `crates/security/src/oauthbearer.rs` near `evaluate_custom_claim_check` (around line 222):

```rust
/// Slice 49h: extract group memberships from token claims using a
/// precompiled JsonPath. Each result element is interpreted per its
/// JSON type:
/// - `String`: if `delimiter` is set, split + trim + drop empty;
///   otherwise the whole string becomes one group.
/// - `Array`: each string element becomes a group.
/// - `Number` / `Object` / `Null`: ignored (no error).
///
/// Returns `vec![]` for empty matches (no groups extracted is not an
/// error — the token may legitimately have no groups).
fn extract_groups(path: &JpQuery, claims: &Value, delimiter: Option<&str>) -> Vec<String> {
    let Ok(refs) = js_path_process(path, claims) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for r in refs {
        match r.val() {
            Value::String(s) => match delimiter {
                Some(d) => out.extend(
                    s.split(d)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(String::from),
                ),
                None => out.push(s.clone()),
            },
            Value::Array(items) => {
                out.extend(items.iter().filter_map(Value::as_str).map(String::from));
            }
            _ => {} // ignore numbers, objects, nulls
        }
    }
    out
}
```

`r.val()` is the same accessor pattern `evaluate_custom_claim_check` uses post-49g. If the actual API is `r.deref()` or something else, mirror that.

- [ ] **Step 10: Add groups + fallback unit tests for the unsecured validator**

In the same test module, add:

```rust
#[test]
fn unsecured_validate_extracts_groups_from_array_claim() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({
            "sub": "alice",
            "exp": exp_secs,
            "groups": ["admin", "ops"],
        }),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.groups_claim = Some(
        jsonpath_rust::parser::parse_json_path("$.groups").expect("compiles"),
    );
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.groups, vec!["admin".to_string(), "ops".to_string()]);
}

#[test]
fn unsecured_validate_extracts_groups_from_delimited_string() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({
            "sub": "alice",
            "exp": exp_secs,
            "groups": "admin,ops, kafka",
        }),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.groups_claim = Some(
        jsonpath_rust::parser::parse_json_path("$.groups").expect("compiles"),
    );
    v.groups_claim_delimiter = Some(",".into());
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(
        outcome.principal.groups,
        vec!["admin".to_string(), "ops".to_string(), "kafka".to_string()]
    );
}

#[test]
fn unsecured_validate_extracts_groups_from_nested_claim_via_jsonpath() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({
            "sub": "alice",
            "exp": exp_secs,
            "realm_access": { "roles": ["admin", "ops"] },
        }),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.groups_claim = Some(
        jsonpath_rust::parser::parse_json_path("$.realm_access.roles[*]").expect("compiles"),
    );
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(
        outcome.principal.groups,
        vec!["admin".to_string(), "ops".to_string()]
    );
}

#[test]
fn unsecured_validate_returns_empty_groups_when_claim_unset() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({
            "sub": "alice",
            "exp": exp_secs,
            "groups": ["admin"],
        }),
    );
    let v = UnsecuredJwsValidator::default(); // no groups_claim
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.groups, Vec::<String>::new());
}

#[test]
fn unsecured_validate_returns_empty_groups_when_claim_resolves_to_empty() {
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let token = make_unsecured_jws_with_header(
        &serde_json::json!({"alg": "none", "typ": "JWT"}),
        &serde_json::json!({
            "sub": "alice",
            "exp": exp_secs,
        }),
    );
    let mut v = UnsecuredJwsValidator::default();
    v.groups_claim = Some(
        jsonpath_rust::parser::parse_json_path("$.nonexistent").expect("compiles"),
    );
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.groups, Vec::<String>::new());
}
```

- [ ] **Step 11: Run unsecured tests — verify pass**

```bash
cargo test -p crabka-security oauthbearer::tests::unsecured_validate 2>&1 | tail -15
```

Expected: all 9 new unsecured tests pass + existing pass.

- [ ] **Step 12: Repeat for `SignedJwsValidator` — extend struct + body**

Replace the `SignedJwsValidator` struct (lines 292-301) — add the same 4 fields:

```rust
#[derive(Debug, Clone)]
pub struct SignedJwsValidator {
    pub principal_claim_name: String,
    pub allowable_clock_skew_ms: i64,
    pub valid_issuer: Option<String>,
    pub expected_audience: Option<String>,
    pub custom_claim_check: Option<JpQuery>,
    pub valid_token_type: Option<String>,
    /// Slice 49h: alternate principal claim. See UnsecuredJwsValidator.
    pub fallback_user_name_claim: Option<String>,
    pub fallback_user_name_prefix: Option<String>,
    pub groups_claim: Option<JpQuery>,
    pub groups_claim_delimiter: Option<String>,
    keys: JwksHandle,
}
```

In `check_claims()` (lines 380-432), replace the principal-name extraction + `Ok(AuthOutcome { ... })` (lines 417-431) with the same fallback + groups logic from Step 8 (just substituting `claims` for `&claims` per the function signature):

```rust
        // Slice 49h: primary → fallback → reject. Prefix on fallback only.
        let (raw_name, used_fallback) = match claims
            .get(&self.principal_claim_name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            Some(n) => (n.to_string(), false),
            None => {
                let fallback_claim = self.fallback_user_name_claim
                    .as_deref()
                    .ok_or(AuthError::InvalidToken)?;
                let raw = claims
                    .get(fallback_claim)
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or(AuthError::InvalidToken)?;
                (raw.to_string(), true)
            }
        };
        let name = if used_fallback {
            match &self.fallback_user_name_prefix {
                Some(prefix) => format!("{prefix}{raw_name}"),
                None => raw_name,
            }
        } else {
            raw_name
        };

        let groups = match &self.groups_claim {
            Some(path) => extract_groups(path, claims, self.groups_claim_delimiter.as_deref()),
            None => Vec::new(),
        };

        Ok(AuthOutcome {
            principal: Principal {
                name,
                auth_method: AuthMethod::SaslOAuthBearer,
                groups,
            },
            expires_at_ms: Some(exp_ms),
        })
```

- [ ] **Step 13: Add 1 signed-validator parity test**

```rust
#[test]
fn signed_validate_falls_back_to_alt_claim_when_primary_absent() {
    // Mirror the unsecured fallback test using the signed validator.
    // Use the same JWKS test fixture pattern as existing signed_validate_* tests.
    let exp_secs: i64 = 2_000;
    let now_ms: i64 = 1_000_000;
    let (token, jwks) = mint_rs256_with_header(
        "k1",
        &serde_json::json!({"alg": "RS256", "typ": "JWT", "kid": "k1"}),
        &serde_json::json!({
            "client_id": "svc1",
            "exp": exp_secs,
            "iss": "https://test.example",
        }),
        // No `sub` claim.
    );
    let mut v = signed_validator_for_tests_with_jwks(jwks);
    v.fallback_user_name_claim = Some("client_id".into());
    v.fallback_user_name_prefix = Some("service-account-".into());
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.name, "service-account-svc1");
}
```

Adapt `signed_validator_for_tests_with_jwks` to whatever the existing slice-49b signed-validator tests use. If a `_with_jwks` variant doesn't exist, mirror the existing pattern (probably `signed_validator_for_tests()` returns a validator with default config; just mutate it).

- [ ] **Step 14: Repeat for `IntrospectionValidator` — extend struct + body**

Replace the struct (lines 518-525):

```rust
#[derive(Debug, Clone)]
pub struct IntrospectionValidator {
    pub client: Arc<dyn IntrospectionClient>,
    pub principal_claim_name: String,
    pub custom_claim_check: Option<JpQuery>,
    pub call_userinfo: bool,
    pub allowable_clock_skew_ms: i64,
    /// Slice 49h: alternate principal claim. See UnsecuredJwsValidator.
    pub fallback_user_name_claim: Option<String>,
    pub fallback_user_name_prefix: Option<String>,
    pub groups_claim: Option<JpQuery>,
    pub groups_claim_delimiter: Option<String>,
}
```

In `validate()` (lines 548-587), replace the principal-name extraction + `Ok(AuthOutcome { ... })` (lines 575-587) with the same fallback + groups logic.

- [ ] **Step 15: Add 1 introspection-validator parity test**

```rust
#[tokio::test]
async fn introspection_validate_extracts_groups_from_introspection_response() {
    let fake_client = FakeIntrospectionClient::with_response(serde_json::json!({
        "active": true,
        "sub": "alice",
        "exp": 2_000,
        "groups": ["admin", "ops"],
    }));
    let mut v = IntrospectionValidator::new_for_tests(Arc::new(fake_client));
    v.groups_claim = Some(
        jsonpath_rust::parser::parse_json_path("$.groups").expect("compiles"),
    );
    let outcome = v.validate("opaque-token", 1_000_000).await.expect("valid");
    assert_eq!(
        outcome.principal.groups,
        vec!["admin".to_string(), "ops".to_string()]
    );
}
```

Adapt `FakeIntrospectionClient::with_response` and `IntrospectionValidator::new_for_tests` to whatever exists in the file post-49g.

- [ ] **Step 16: Run all oauthbearer tests**

```bash
cargo test -p crabka-security oauthbearer 2>&1 | tail -20
```

Expected: ~11 new tests pass (9 unsecured + 1 signed + 1 introspection). All existing pass.

- [ ] **Step 17: Update `FileOAuthBearerConfig`**

Edit `crates/broker/src/file_config.rs`. In the struct (lines 58-149), AFTER the existing `max_session_lifetime_seconds` field (last field), ADD:

```rust
    /// Slice 49h: alternate claim name for principal-name fallback.
    #[serde(default)]
    pub fallback_user_name_claim: Option<String>,
    /// Slice 49h: prepended on fallback only.
    #[serde(default)]
    pub fallback_user_name_prefix: Option<String>,
    /// Slice 49h: JsonPath expression (RFC 9535) extracting groups.
    /// Compiled once at broker startup; malformed expression panics
    /// with descriptive error.
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// Slice 49h: when groups_claim resolves to a string, split on
    /// this delimiter.
    #[serde(default)]
    pub groups_claim_delimiter: Option<String>,
```

- [ ] **Step 18: Update `apply_to` threading**

In the `apply_to` block for oauthbearer (around line 272), AFTER the existing `custom_claim_check_compiled` block (line 282-293), ADD:

```rust
            // Slice 49h: compile groups_claim JsonPath at load time.
            let groups_claim_compiled = oauth
                .groups_claim
                .as_deref()
                .map(|expr| {
                    jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
                        panic!(
                            "[oauthbearer]: invalid groups_claim JsonPath expression {expr:?}: {e}"
                        )
                    })
                });
```

For EACH of the 3 validator-branch assignments (signed, introspection, unsecured), add 4 new field assignments. Example for the signed branch (around line 319-322):

```rust
                    // Slice 49g: JsonPath custom_claim_check + JWT typ check.
                    v.custom_claim_check
                        .clone_from(&custom_claim_check_compiled);
                    v.valid_token_type.clone_from(&oauth.valid_token_type);
                    // Slice 49h: claims mapping.
                    v.fallback_user_name_claim
                        .clone_from(&oauth.fallback_user_name_claim);
                    v.fallback_user_name_prefix
                        .clone_from(&oauth.fallback_user_name_prefix);
                    v.groups_claim.clone_from(&groups_claim_compiled);
                    v.groups_claim_delimiter
                        .clone_from(&oauth.groups_claim_delimiter);
```

The introspection branch (around line 377-379) is a struct-literal `IntrospectionValidator { ... }` rather than mutating an existing `v` — adapt the assignments:

```rust
                        custom_claim_check: custom_claim_check_compiled.clone(),
                        // Slice 49h:
                        fallback_user_name_claim: oauth.fallback_user_name_claim.clone(),
                        fallback_user_name_prefix: oauth.fallback_user_name_prefix.clone(),
                        groups_claim: groups_claim_compiled.clone(),
                        groups_claim_delimiter: oauth.groups_claim_delimiter.clone(),
```

The unsecured branch (around line 395-397) is similar to signed.

Be careful: `custom_claim_check_compiled` was being moved (not cloned) in the unsecured branch per slice 49g. After T1, `groups_claim_compiled` would also be moved, but the assignment order matters. If both compiled values need to live in the SAME validator, they can both move (each into different fields). If one needs `.clone()`, the other can move. Pick whichever matches the existing pattern; the implementer's call.

- [ ] **Step 19: Run workspace build + broker tests**

```bash
cargo build --workspace 2>&1 | tail
cargo test -p crabka-security oauthbearer 2>&1 | tail
cargo test -p crabka-broker --lib 2>&1 | tail
```

Expected: `crates/security` + `crates/broker` clean. `crates/operator` will likely still build clean too (Principal cascade should have caught everything). If anything fails, fix.

- [ ] **Step 20: fmt + clippy**

```bash
cargo fmt -p crabka-security -p crabka-broker -- --check
cargo clippy -p crabka-security -p crabka-broker --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 21: Commit**

```bash
git add crates/security/src/principal.rs \
        crates/security/src/oauthbearer.rs \
        crates/security/src/plain.rs \
        crates/security/src/scram/server.rs \
        crates/broker/src/file_config.rs \
        crates/broker/src/network/auth.rs \
        crates/broker/src/network/dispatch.rs \
        crates/broker/src/authorizer.rs
# Plus any test files that needed Principal { ... } sweeps.

git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T1: broker — Principal.groups + OAuth claims mapping (fallback + groups extraction)

Adds Principal.groups: Vec<String> field. Populated by OAuth
validators when groupsClaim is configured; empty everywhere else
(PLAIN/SCRAM/mTLS/anonymous). No broker-side authorizer reads this
yet — scaffolding for slices 53/54.

All three OAuth validators (UnsecuredJwsValidator,
SignedJwsValidator, IntrospectionValidator) gain four new fields:
fallback_user_name_claim + fallback_user_name_prefix (principal-name
chain), groups_claim (Option<JpQuery> compiled at broker startup)
+ groups_claim_delimiter (string-claim split).

Validator validate() bodies now:
1. Try primary principal_claim_name; fall back to
   fallback_user_name_claim; reject if both absent/empty. Prefix
   prepended only when fallback fires (matches Strimzi behavior).
2. Run groups_claim JsonPath against claims (post-userinfo-merge for
   introspection); split strings on groups_claim_delimiter when set.

New private extract_groups helper. FileOAuthBearerConfig gains the
four new fields; apply_to compiles groups_claim once at startup with
panic-on-malformed (matches slice 49g's custom_claim_check pattern).

~11 new validator tests (9 unsecured covering primary/fallback/prefix
× groups-array/string/nested/empty + 1 signed parity + 1 introspection
parity). Workspace Principal { ... } cascade swept across ~57 sites
in crates/security + crates/broker. Greenfield: no compat shim.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 2 — T2 then T3 (sequential within batch)

**Dispatch order:** T2 first; wait for its commit; then dispatch T3. T2 changes the struct shape that T3's fixture sweep depends on.

#### Task T2: Operator CRD — 4 new fields + own-file sweep + round-trip tests

**Files:**
- Modify: `crates/operator/src/crd/listener.rs`

- [ ] **Step 1: Write 2 failing round-trip tests**

Edit `crates/operator/src/crd/listener.rs`. In the test module, after existing 49g round-trip tests, add:

```rust
#[test]
fn oauth_round_trip_with_claims_mapping_fields() {
    let yaml = r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
fallbackUserNameClaim: client_id
fallbackUserNamePrefix: "service-account-"
groupsClaim: "$.realm_access.roles[*]"
groupsClaimDelimiter: ","
"#;
    let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
    let ListenerAuthentication::OAuth(oauth) = &parsed else {
        panic!("expected oauth variant");
    };
    assert_eq!(oauth.fallback_user_name_claim.as_deref(), Some("client_id"));
    assert_eq!(oauth.fallback_user_name_prefix.as_deref(), Some("service-account-"));
    assert_eq!(oauth.groups_claim.as_deref(), Some("$.realm_access.roles[*]"));
    assert_eq!(oauth.groups_claim_delimiter.as_deref(), Some(","));
}

#[test]
fn oauth_round_trip_without_claims_mapping_fields_omits_them() {
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
        fallback_user_name_claim: None,
        fallback_user_name_prefix: None,
        groups_claim: None,
        groups_claim_delimiter: None,
    };
    let auth = ListenerAuthentication::OAuth(cfg);
    let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
    for key in ["fallbackUserNameClaim", "fallbackUserNamePrefix", "groupsClaim", "groupsClaimDelimiter"] {
        assert!(!yaml.contains(key), "{key} must be omitted; got:\n{yaml}");
    }
}
```

- [ ] **Step 2: Run tests — verify failure**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49h-oauth-claims-mapping
cargo test -p crabka-operator --lib crd::listener::auth_tests::oauth_round_trip_with_claims_mapping 2>&1 | tail
```

Expected: compile error — `ListenerAuthenticationOAuth` has no `fallback_user_name_claim` field.

- [ ] **Step 3: Add the 4 new fields to `ListenerAuthenticationOAuth`**

In `crates/operator/src/crd/listener.rs`, AFTER the existing `valid_token_type` field (last field post-49g, around line 191), ADD:

```rust
    /// Slice 49h: alternate claim name for principal-name fallback when
    /// `userNameClaim` (default `sub`) is absent/empty on the token.
    /// Strimzi convention: `client_id` for Keycloak service-account
    /// tokens whose `sub` is a UUID. Flat claim name, NOT JsonPath.
    /// CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_user_name_claim: Option<String>,

    /// Slice 49h: prepended to the resolved principal name ONLY when
    /// the fallback claim fires (primary present → no prefix). Strimzi
    /// convention: `"service-account-"` to namespace
    /// fallback-derived principals so ACLs can distinguish service
    /// accounts from human users. CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_user_name_prefix: Option<String>,

    /// Slice 49h: JsonPath expression (RFC 9535 via jsonpath-rust)
    /// extracting group memberships from token claims. Examples:
    /// `"$.groups"` (top-level array), `"$.realm_access.roles[*]"`
    /// (Keycloak realm-roles shape). When the path resolves to an
    /// array, each string element is a group; when it resolves to a
    /// string and `groupsClaimDelimiter` is set, the string is split.
    /// Result attached to the Kafka principal but not yet consumed by
    /// any broker-side authorizer (slice 53/54 will use it).
    /// CRD-validated `minLength: 1`.
    ///
    /// Note: Strimzi uses Jayway JsonPath (`$[?(@.x == 'y')]`); Crabka
    /// uses RFC 9535 (`$[?@.x == 'y']` — no parens) per the slice 49g
    /// choice of `jsonpath-rust`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim: Option<String>,

    /// Slice 49h: delimiter to split `groupsClaim` results when the
    /// claim resolves to a string (e.g., `","` or `" "`). Ignored
    /// when the claim is an array. CRD-validated `minLength: 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups_claim_delimiter: Option<String>,
```

- [ ] **Step 4: Add 4 entries to the hand-rolled schema**

Find `listener_authentication_schema` and add to the `properties` map (alphabetical-ish; match existing positioning):

```rust
            "fallbackUserNameClaim":  { "type": "string", "minLength": 1 },
            "fallbackUserNamePrefix": { "type": "string", "minLength": 1 },
            "groupsClaim":            { "type": "string", "minLength": 1 },
            "groupsClaimDelimiter":   { "type": "string", "minLength": 1 },
```

- [ ] **Step 5: Sweep `ListenerAuthenticationOAuth { ... }` struct-literal sites in this file's tests**

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/crd/listener.rs
```

Per the explore: 11 sites. For each, add 4 new field defaults at the end:

```rust
        fallback_user_name_claim: None,
        fallback_user_name_prefix: None,
        groups_claim: None,
        groups_claim_delimiter: None,
```

- [ ] **Step 6: Update the schema-regression test**

Find the schema regression test (probably `oauth_listener_authentication_schema_smoke` or similar). Extend the expected-properties list with the 4 new keys.

- [ ] **Step 7: Run tests — verify pass**

```bash
cargo test -p crabka-operator --lib crd::listener 2>&1 | tail -15
```

Expected: 2 new round-trip tests pass + all existing pass + schema regression test still pass with 4 new properties.

- [ ] **Step 8: fmt + clippy on the changed file**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clippy errors in `controller/listeners.rs` (T3 territory — E0063 missing fields). These are NOT T2 concerns; they'll be fixed in T3.

- [ ] **Step 9: Commit**

```bash
git add crates/operator/src/crd/listener.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T2: operator CRD — 4 new claims-mapping fields

Adds fallbackUserNameClaim + fallbackUserNamePrefix (principal-name
chain) and groupsClaim + groupsClaimDelimiter (groups extraction)
to ListenerAuthenticationOAuth. All Option<String>, all CRD-validated
minLength:1. Hand-rolled schema entries added.

2 new round-trip tests (with-fields-set, without-fields-omits). 11
struct-literal fixture sites in this file's tests swept with the new
None defaults. Schema-regression test extended with the 4 new
property keys.

T3 follows up with reconciler render + divergence walk + sibling-file
sweeps.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T3: Operator reconciler — render + divergence + sibling-file sweep

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs`
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

**Prerequisite:** T2 must be committed first.

- [ ] **Step 1: Sweep `controller/listeners.rs` fixtures**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49h-oauth-claims-mapping
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/listeners.rs
```

Per the explore: ~10 sites (test fixtures + divergence walk base). For each, add the 4 new `None` defaults.

The biggest is the `base` fixture in the divergence-walk test (around line 1507). After sweep:

```rust
let base = crate::crd::ListenerAuthenticationOAuth {
    // existing 19 fields ...
    valid_token_type: None,
    fallback_user_name_claim: None,
    fallback_user_name_prefix: None,
    groups_claim: None,
    groups_claim_delimiter: None,
};
```

- [ ] **Step 2: Sweep sibling-file fixtures (kafka.rs + kafka_node_pool.rs)**

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/kafka.rs
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/kafka_node_pool.rs
```

Per the explore: 4 + 3 = 7 sites total. Add the 4 new `None` defaults at each.

- [ ] **Step 3: Run cargo build — verify the operator builds**

```bash
cargo build -p crabka-operator 2>&1 | tail
```

Expected: clean. Integration tests (`tests/reconcile_*.rs`) likely still fail E0063 — that's T4 territory.

- [ ] **Step 4: Add 4 render-emit lines to `render_broker_toml`**

In `crates/operator/src/controller/listeners.rs::render_broker_toml`, find the `[oauthbearer]` block. After the existing slice 49g `valid_token_type` emission (around line 2677), ADD:

```rust
        // Slice 49h: claims mapping.
        if let Some(c) = &oauth_cfg.fallback_user_name_claim {
            let _ = writeln!(out, "fallback_user_name_claim = \"{c}\"");
        }
        if let Some(p) = &oauth_cfg.fallback_user_name_prefix {
            let _ = writeln!(out, "fallback_user_name_prefix = \"{p}\"");
        }
        if let Some(expr) = &oauth_cfg.groups_claim {
            // TOML multi-line literal — no escape processing, allows
            // embedded `'` and `"` in the JsonPath. Same convention
            // as 49g's custom_claim_check.
            let _ = writeln!(out, "groups_claim = '''{expr}'''");
        }
        if let Some(d) = &oauth_cfg.groups_claim_delimiter {
            let _ = writeln!(out, "groups_claim_delimiter = \"{d}\"");
        }
```

- [ ] **Step 5: Add 4 perturbation entries to the divergence walk**

Find `validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field` (around line 1495). After the existing `valid_token_type` perturbation (post-49g, around line 1601-1607), ADD:

```rust
        (
            "fallback_user_name_claim",
            crate::crd::ListenerAuthenticationOAuth {
                fallback_user_name_claim: Some("client_id".into()),
                ..base.clone()
            },
        ),
        (
            "fallback_user_name_prefix",
            crate::crd::ListenerAuthenticationOAuth {
                fallback_user_name_prefix: Some("svc-".into()),
                ..base.clone()
            },
        ),
        (
            "groups_claim",
            crate::crd::ListenerAuthenticationOAuth {
                groups_claim: Some("$.groups".into()),
                ..base.clone()
            },
        ),
        (
            "groups_claim_delimiter",
            crate::crd::ListenerAuthenticationOAuth {
                groups_claim_delimiter: Some(",".into()),
                ..base.clone()
            },
        ),
```

- [ ] **Step 6: Add 4 new render unit tests**

In the test module:

```rust
#[test]
fn render_broker_toml_emits_fallback_user_name_claim_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.fallback_user_name_claim = Some("client_id".into());
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(
        toml.contains("fallback_user_name_claim = \"client_id\""),
        "expected fallback_user_name_claim render; got:\n{toml}"
    );
}

#[test]
fn render_broker_toml_emits_fallback_user_name_prefix_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.fallback_user_name_prefix = Some("service-account-".into());
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(toml.contains("fallback_user_name_prefix = \"service-account-\""));
}

#[test]
fn render_broker_toml_emits_groups_claim_with_jsonpath_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.groups_claim = Some("$.realm_access.roles[*]".into());
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(
        toml.contains("groups_claim = '''$.realm_access.roles[*]'''"),
        "expected groups_claim render (TOML multi-line literal); got:\n{toml}"
    );
}

#[test]
fn render_broker_toml_emits_groups_claim_delimiter_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.groups_claim_delimiter = Some(",".into());
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(toml.contains("groups_claim_delimiter = \",\""));
}
```

Adapt `oauth_full_cfg()` + `oauth_listener_for_render()` to actual helper names per the file's existing tests (search the file for `fn oauth_full_cfg` or `fn oauth_minimal_cfg`).

- [ ] **Step 7: Run listeners tests — verify pass**

```bash
cargo test -p crabka-operator --lib controller::listeners 2>&1 | tail -15
```

Expected: all green. Includes the 4 new render tests + extended divergence walk (now 15 perturbations).

- [ ] **Step 8: fmt + clippy**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean for `controller/*` files. Integration tests in `tests/reconcile_*.rs` will still fail E0063 — T4 territory.

- [ ] **Step 9: Commit**

```bash
git add crates/operator/src/controller/listeners.rs \
        crates/operator/src/controller/kafka.rs \
        crates/operator/src/controller/kafka_node_pool.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T3: operator reconciler — render claims-mapping keys + divergence

render_broker_toml emits 4 new keys under [oauthbearer] when set:
fallback_user_name_claim, fallback_user_name_prefix (plain
double-quoted strings), groups_claim (TOML multi-line literal
'''...''' so embedded ' and " in JsonPath don't need escaping),
groups_claim_delimiter (plain string).

Cross-listener divergence walk extended with 4 new perturbations
(one per field). 4 new render unit tests cover the Some/None render
behavior.

Sweep: 10 fixture sites in controller/listeners.rs + 7 sibling-file
sites (controller/kafka.rs:4 + controller/kafka_node_pool.rs:3) got
the 4 new None defaults. T2's struct change cascades here per the
49g pattern.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 3 — T4 ‖ T5 (file-disjoint parallel)

#### Task T4: Integration tests + sample + CRD regen

**Files:**
- Modify: `crates/operator/tests/reconcile_listener_oauth.rs`
- Modify: `crates/operator/tests/reconcile_oauth_introspection.rs`
- Modify: `crates/operator/tests/reconcile_oauth_trust.rs`
- Modify: `crates/operator/sample/oauth-listener.yaml`
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (regenerated)

**Race awareness:** T5 is running in parallel; touches only `.github/workflows/operator-e2e.yml`. File-disjoint.

- [ ] **Step 1: Sweep integration-test fixtures**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49h-oauth-claims-mapping
grep -n "ListenerAuthenticationOAuth {" crates/operator/tests/reconcile_listener_oauth.rs \
    crates/operator/tests/reconcile_oauth_introspection.rs \
    crates/operator/tests/reconcile_oauth_trust.rs
```

Per the explore: 5 + 2 + 1 = 8 sites. Add the 4 new `None` defaults to each.

The big ones are `oauth_cfg_minimal()` and `oauth_cfg_full()` in `reconcile_listener_oauth.rs`.

- [ ] **Step 2: Add 2 new integration tests in `reconcile_listener_oauth.rs`**

After the existing 49g integration tests, add:

```rust
#[tokio::test]
async fn oauth_listener_with_fallback_user_name_claim_renders_broker_toml_key() {
    let mut cfg = oauth_cfg_minimal();
    cfg.fallback_user_name_claim = Some("client_id".into());
    cfg.fallback_user_name_prefix = Some("service-account-".into());
    // ... build Kafka CR, reconcile, extract broker0 TOML
    let toml = /* extract — mirror existing test pattern */;
    assert!(toml.contains("fallback_user_name_claim = \"client_id\""));
    assert!(toml.contains("fallback_user_name_prefix = \"service-account-\""));
}

#[tokio::test]
async fn oauth_listener_with_groups_claim_renders_broker_toml_key() {
    let mut cfg = oauth_cfg_minimal();
    cfg.groups_claim = Some("$.realm_access.roles[*]".into());
    cfg.groups_claim_delimiter = Some(",".into());
    // ... reconcile + extract
    assert!(toml.contains("groups_claim = '''$.realm_access.roles[*]'''"));
    assert!(toml.contains("groups_claim_delimiter = \",\""));
}
```

Mirror the EXACT reconcile + extract_broker0_toml pattern from existing tests in this file (look for a similar slice-50d/49g test like `oauth_listener_with_max_seconds_without_reauthentication_renders_broker_toml_key`).

- [ ] **Step 3: Run integration tests — verify pass**

```bash
cargo build -p crabka-operator --tests 2>&1 | tail
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_with_fallback 2>&1 | tail
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_with_groups_claim 2>&1 | tail
```

Expected: both new tests pass + existing pass.

- [ ] **Step 4: Update the sample manifest**

Edit `crates/operator/sample/oauth-listener.yaml`. After the existing `validTokenType: JWT` block (post-49g, around line 28), ADD:

```yaml
        # Slice 49h: fallback principal claim — used when userNameClaim
        # is absent or empty on the token. Strimzi convention: pair
        # with fallbackUserNamePrefix to namespace service-account
        # principals so ACLs can distinguish them.
        # fallbackUserNameClaim: client_id
        # fallbackUserNamePrefix: "service-account-"
        #
        # Slice 49h: groups extraction — JsonPath expression (RFC 9535)
        # against the token's claim set. Attached to the Kafka principal
        # but not yet consumed by any broker-side authorizer (slice
        # 53/54 will use it).
        # groupsClaim: "$.realm_access.roles[*]"
        # groupsClaimDelimiter: ","
```

Indentation: 8 spaces for keys.

Verify YAML parses to 3 docs:

```bash
cat crates/operator/sample/oauth-listener.yaml | python3 -c "import sys, yaml; docs = list(yaml.safe_load_all(sys.stdin)); print(f'{len(docs)} docs: {[d.get(\"kind\") for d in docs]}')"
```

Expected: `3 docs: ['Kafka', 'KafkaNodePool', 'KafkaUser']`.

- [ ] **Step 5: Regenerate CRDs**

```bash
bash tools/regen-crds.sh 2>&1 | tail
git diff --stat deploy/crds/
git diff deploy/crds/crabka.io_kafkas.yaml | grep -B 2 -A 4 "fallbackUserNameClaim\|fallbackUserNamePrefix\|groupsClaim\|groupsClaimDelimiter" | head -30
```

Expected: ONLY `deploy/crds/crabka.io_kafkas.yaml` changed. Diff adds 4 new property entries (~12 lines total). Each entry should be:

```yaml
<key>:
  minLength: 1
  type: string
```

- [ ] **Step 6: Run full operator test suite + fmt + clippy**

```bash
cargo test -p crabka-operator 2>&1 | tail -15
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --tests -- -D warnings 2>&1 | tail
```

Expected: all green. CRD drift gate:

```bash
bash tools/regen-crds.sh && git diff --exit-code -- deploy/crds/ ; echo "exit: $?"
```

Expected: exit 0.

- [ ] **Step 7: Commit**

```bash
git add crates/operator/tests/reconcile_listener_oauth.rs \
        crates/operator/tests/reconcile_oauth_introspection.rs \
        crates/operator/tests/reconcile_oauth_trust.rs \
        crates/operator/sample/oauth-listener.yaml \
        deploy/crds/crabka.io_kafkas.yaml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T4: operator integration tests + sample + CRD regen for slice 49h

Sweeps 8 fixture sites across 3 integration test files (5 in
reconcile_listener_oauth, 2 in reconcile_oauth_introspection, 1 in
reconcile_oauth_trust): adds the 4 new None defaults.

2 new integration tests:
- oauth_listener_with_fallback_user_name_claim_renders_broker_toml_key
- oauth_listener_with_groups_claim_renders_broker_toml_key

Sample manifest: commented-out examples for all 4 new fields.

CRDs regenerated; only deploy/crds/crabka.io_kafkas.yaml changed
(4 new properties, all { type: string, minLength: 1 }).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T5: kind-oauth + kind-oauth-introspection e2e + Keycloak realm bootstrap extension

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

**Race awareness:** T4 is running in parallel. T4 touches `crates/operator/tests/*`, `sample/`, `deploy/crds/` — disjoint from your file.

**Mission:** Two coordinated changes per the spec's "T5 must touch BOTH" lesson from slice 49g's holistic review:

1. **Extend the Keycloak realm bootstrap** to create a realm role + assign it to the `kafka-client` service account in BOTH the `kind-oauth` and `kind-oauth-introspection` jobs. Without this, tokens have empty `realm_access.roles` and the JsonPath extraction returns nothing.
2. **Add `groupsClaim: "$.realm_access.roles[*]"`** to BOTH Kafka CR YAMLs (in `kind-oauth` AND `kind-oauth-introspection` jobs).

- [ ] **Step 1: Read the existing kafka-client client creation in both jobs**

```bash
sed -n '2700,2780p' .github/workflows/operator-e2e.yml | head -90
```

Find the `$KCADM create clients -r kafka -s clientId=kafka-client ...` lines in both job sections. Note the surrounding context (what role-assignment commands exist already, if any).

- [ ] **Step 2: Extend the realm bootstrap to add a role + assign it**

For BOTH the `kind-oauth` job's kafka-client setup AND the `kind-oauth-introspection` job's kafka-client setup, AFTER the existing `$KCADM create clients ...` block, ADD:

```bash
          # Slice 49h: create a realm role + assign to the kafka-client
          # service account so realm_access.roles is populated. The
          # broker's groupsClaim = "$.realm_access.roles[*]" extracts
          # this into Principal.groups.
          $KCADM create roles -r kafka -s name=kafka-cluster-admin
          KC_SVC_USER_ID=$($KCADM get clients -r kafka -q clientId=kafka-client \
            --fields id --format csv --noquotes | tail -1)
          KC_SVC_USER_USERNAME=service-account-kafka-client
          $KCADM add-roles -r kafka --uusername $KC_SVC_USER_USERNAME --rolename kafka-cluster-admin
```

Adapt the kcadm variable names + role name conventions to whatever the existing bootstrap uses. The intent: a realm role exists AND it's mapped to the kafka-client's service-account user.

Run this in BOTH jobs (kind-oauth and kind-oauth-introspection). Different jobs may have slightly different bootstrap scripts; mirror each.

- [ ] **Step 3: Add `groupsClaim` to both Kafka CR YAMLs**

For the `kind-oauth` job's Kafka CR YAML (around line 2253 per the explore), after the existing `validTokenType: JWT`, ADD:

```yaml
                  groupsClaim: "$.realm_access.roles[*]"
```

(Indentation: 18 spaces, matching siblings.)

For the `kind-oauth-introspection` job's Kafka CR YAML, find the analogous `authentication:` block (post-49g — `customClaimCheck` already converted to string form). After the existing `customClaimCheck` line, ADD:

```yaml
                  groupsClaim: "$.realm_access.roles[*]"
```

Note: `validTokenType` is JWT-mode only and is NOT added to the introspection job (operator cross-mode validator would reject).

- [ ] **Step 4: Verify YAML parses + actionlint**

```bash
python3 -c "
import yaml
w = yaml.safe_load(open('.github/workflows/operator-e2e.yml'))
print('jobs:', list(w['jobs'].keys()))
"
```

Expected: no parse errors. Both `kind-oauth` and `kind-oauth-introspection` in the jobs list.

```bash
which actionlint && actionlint .github/workflows/operator-e2e.yml 2>&1 | head -20 || echo "actionlint not installed"
```

Pre-existing warnings (from prior slices) are fine; NO new warnings.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/operator-e2e.yml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T5: kind-oauth + kind-oauth-introspection e2e — groupsClaim end-to-end

Both Keycloak realm bootstraps gain a kafka-cluster-admin realm role
assigned to the kafka-client service account, so realm_access.roles
is populated on tokens.

Both Kafka CR YAMLs add groupsClaim: "$.realm_access.roles[*]"
(introspection job omits validTokenType — cross-mode validator
rejects it on accessTokenIsJwt:false). Producer Jobs unchanged.

After this slice merges + the e2e runs, broker logs will show
Principal.groups populated with ["kafka-cluster-admin"] for
authenticated producers — proves the JsonPath extraction path
end-to-end even before a broker-side consumer (authorizer) exists.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 4 — T6 (alone)

#### Task T6: STATUS.md + final gate

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Append the slice 49h entry**

Read slice 49g's STATUS section first (`grep -n "^## Slice 49g " STATUS.md` then read the ~70 lines below). Append to STATUS.md:

```markdown
## Slice 49h — Operator + Broker: OAUTHBEARER claims mapping (fallback principal chain + groups extraction) (2026-05-24)

Second of three "long-tail" Strimzi-parity clusters closing the
OAUTHBEARER umbrella (49g shipped validation policies; 49i will ship
JWKS refresher policies). Adds 4 Strimzi-shape fields on the listener
OAuth CRD + broker validators.

- **Broker (`crates/security/`, `crates/broker/`):** new
  `Principal.groups: Vec<String>` field — populated by OAuth
  validators when `groupsClaim` is configured; empty for non-OAuth
  principals. **No broker-side authorizer reads `groups` yet** —
  scaffolding for slice 53/54.
  Four new `[oauthbearer]` TOML keys: `fallback_user_name_claim`,
  `fallback_user_name_prefix`, `groups_claim` (RFC 9535 JsonPath via
  jsonpath-rust, compiled at broker startup), `groups_claim_delimiter`.
- **Validator logic:** All three OAuth validators (`UnsecuredJwsValidator`,
  `SignedJwsValidator`, `IntrospectionValidator`) execute new
  principal-name resolution + groups extraction:
  - **Name fallback chain**: primary `principal_claim_name` → fallback
    `fallback_user_name_claim` → reject. Prefix
    `fallback_user_name_prefix` applied only when fallback fires (Strimzi
    behavior).
  - **Groups extraction** (`extract_groups` helper): JsonPath result
    interpreted per element type — string + delimiter → split+trim+
    drop-empty; array → string elements only; number/object/null
    ignored; empty match → empty groups (not an error).
- **Operator CRD (`crates/operator/src/crd/listener.rs`):** 4 new
  `Option<String>` fields on `ListenerAuthenticationOAuth`,
  Strimzi-shape camelCase, all hand-rolled schema entries `minLength: 1`.
- **Operator reconciler (`crates/operator/src/controller/listeners.rs`):**
  `render_broker_toml` emits the 4 new keys when set; `groups_claim`
  uses TOML multi-line literal `'''...'''` per slice 49g's JsonPath
  pattern. Existing cross-listener divergence walk extended with 4
  new perturbations.
- **Principal cascade (CLAUDE.md greenfield rule):** T1 swept ~57
  `Principal { ... }` literal sites across `crates/security/` and
  `crates/broker/` (PLAIN/SCRAM/mTLS/OAuth construction + dispatch
  init + tests) to add `groups: vec![]` defaults.
- **E2E (`.github/workflows/operator-e2e.yml`):** both `kind-oauth`
  (JWT mode) and `kind-oauth-introspection` (introspection mode)
  Kafka CRs add `groupsClaim: "$.realm_access.roles[*]"`. Both
  jobs' Keycloak realm bootstraps gain a `kafka-cluster-admin`
  realm role mapped to the `kafka-client` service account so
  `realm_access.roles` is populated on tokens. `validTokenType` is
  JWT-mode only and is NOT added to the introspection job.
- **`fallbackUserNameClaim` not exercised in e2e** — would require
  producers to send tokens without `sub`. Unit-tested only (4
  unsecured + 1 signed parity tests cover the primary/fallback/
  prefix matrix).
- **Tests:** ~14 new (11 broker unit covering the matrix for
  Unsecured + 1 Signed parity + 1 Introspection parity + extract_groups
  helper-level + 2 CRD round-trip + 4 reconciler unit + extended
  divergence walk (now 15 perturbations) + 2 operator integration).
  Workspace fmt + clippy `-D warnings` + tests + CRD drift gate all
  green.
- **Reference doc:** `[docs/superpowers/specs/2026-05-24-crabka-oauth-claims-mapping-49h-design.md]`
- **Semantic divergence from Strimzi:** `groupsClaim` is RFC 9535
  JsonPath (inherited from 49g's jsonpath-rust choice), not Strimzi's
  Jayway flavor. Operators porting Strimzi configs rewrite filter
  predicates accordingly.
- **Out of scope:** slice 49i (JWKS refresher policies — last of the
  long-tail clusters); slice 49f (PLAIN-with-OAuth-token, skipped
  indefinitely); broker-side groups consumer (slice 53/54 operator
  authorizer plugins).
```

- [ ] **Step 2: Final gate**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49h-oauth-claims-mapping
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

- [ ] **Step 3: Commit**

```bash
git add STATUS.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
Slice 49h: STATUS.md entry + final gate

Documents the new operator + broker claims-mapping surface:
fallbackUserNameClaim + fallbackUserNamePrefix (principal-name chain),
groupsClaim + groupsClaimDelimiter (groups extraction via JsonPath).
Principal struct gains groups: Vec<String> field; no broker-side
authorizer consumer yet (slice 53/54). fmt + clippy + workspace
tests + CRD drift gate all green.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Notes

- **Dependency chain:** T1 → T2 → T3 → (T4 ‖ T5) → T6. Six tasks, four batches. T2 + T3 file-disjoint by design but dispatched sequentially because T3's fixture sweep depends on T2's struct field present. T4 + T5 truly parallel.
- **T1 is the largest task** in this slice (~57 Principal cascade sites). Allocate enough time for the implementer; consider splitting into "Step 1-4: Principal cascade alone" → review → "Step 5+: validator changes" if it overwhelms a single subagent. The plan dispatches it as one task per the slice-49g/50d pattern; the implementer's call to chunk steps within the task.
- **T5 lesson from slice 49g's holistic review:** Touch BOTH `kind-oauth` AND `kind-oauth-introspection` Kafka CR YAMLs + extend Keycloak realm bootstrap in BOTH jobs (the holistic reviewer for 49g caught this gap when 49g's T5 only touched `kind-oauth`; T5's slice-49h prompt explicitly covers both jobs).
- **CLAUDE.md greenfield:** No back-compat shims. The Principal struct grows a field atomically; all 57 construction sites updated in one commit. No `#[serde(default)]` magic beyond what `Option<>` already provides.
- **JsonPath crate is already in workspace deps** (slice 49g's polish). T1 doesn't need to add it — it's available via `jsonpath-rust.workspace = true` already in `crates/security/Cargo.toml` and `crates/broker/Cargo.toml`. The `parse_json_path` + `js_path_process` API is reused as-is.
- **The `Principal.groups` field is dead-code at the broker level** until slice 53/54 ships. STATUS calls this out. Operators see the data on Principal via tracing/observability but no authorization decision is made on it yet.
- **After 49h lands:** **49i** (JWKS refresher policies — `jwksMinRefreshPauseSeconds`, `jwksExpirySeconds`, `jwksIgnoreKeyUse`) is next and LAST in the OAUTHBEARER umbrella. After 49i lands, the umbrella reaches Strimzi field parity (modulo skipped 49f PLAIN-with-OAuth-token). Beyond that, the operator roadmap has slices 51+ (delegation tokens, GSSAPI/Kerberos, OPA/Keycloak authorizers).
