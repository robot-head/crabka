# Slice 49g — Operator + Broker: OAUTHBEARER validation policies (`customClaimCheck` + `validTokenType`)

Status: Draft
Date: 2026-05-24
Umbrella: `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`
Builds on: slices 49b (signed JWS validator), 49d (RFC 7662 introspection), 50 (initial CRD shape with `customClaimCheck` stub)
Followups: slice 49h (claims mapping), slice 49i (JWKS refresher policies)

## Goal

Ship Strimzi's full `customClaimCheck` (JsonPath expression evaluated
against token claims) and `validTokenType` (JWT `typ` header check) on
the broker + operator surfaces. Replaces slice 50's typed
`customClaimCheck: { scope, scope_claim }` stub with the full Strimzi
string-expression shape.

## Why this slice

After slice 50d, the OAUTHBEARER umbrella has shipped wire, JWKS,
introspection, re-auth, and session-cap support. The remaining
"long-tail" claim-validation features are the most-asked surfaces in
real-world OAuth deployments — particularly `customClaimCheck`, which
operators routinely use to require multiple scopes, audience matches,
nested claim values, or boolean combinations beyond what
`validAudience` alone supports.

This slice is the first of three "long-tail" bundles. The other two
(claims mapping = slice 49h; JWKS refresher policies = slice 49i)
follow independently. Slice 49f (PLAIN-with-OAuth-token) is
explicitly skipped per the brainstorming session — Crabka has no
users yet, so the "only-if-user-demand" gate fires negative.

## Semantic divergence from Strimzi (acknowledged)

Strimzi's `customClaimCheck` uses Jayway JsonPath (a Java library
with its own syntax flavor). Crabka uses the `jsonpath-rust` crate
(Boris Zhguchev's, github.com/besok/jsonpath-rust), which aims for
Jayway compatibility but isn't byte-exact. Concretely:

- Both support `@.claimName`, `==`, `!=`, `&&`, `||`, `!`.
- Both support `in [...]` set-membership and `contains` string-match.
- Edge cases (e.g., Jayway's `=~` regex operator, or nested filter
  expressions like `?(@.x > 0 && @.y < 10)`) may parse but evaluate
  differently.

The YAML shape (`customClaimCheck: "<expression string>"`) matches
Strimzi exactly. Operators porting a Strimzi config will rewrite
expressions one-for-one for the common cases; obscure Jayway
features may need adaptation. Documented as "Crabka uses
jsonpath-rust; see <crate-link> for full syntax."

## Scope

### In scope

**Broker (`crates/security/`, `crates/broker/`):**

- New `jsonpath-rust` runtime dependency in `crabka-security`.
- New `[oauthbearer].custom_claim_check: Option<String>` TOML key.
  Holds a JsonPath expression evaluated against the token's claim
  set. Token is rejected when the expression yields empty/false.
- New `[oauthbearer].valid_token_type: Option<String>` TOML key.
  When set, the JWT `typ` header must equal this string. JWT-mode
  validators (unsecured + signed JWS) check; introspection skips
  (no JWT header).
- Both validators (`UnsecuredJwsValidator`, `SignedJwsValidator`)
  + `IntrospectionValidator` gain `Option<JsonPathInst>` field for
  the precompiled expression. JWT-mode validators additionally gain
  `Option<String>` for the `typ` check.
- Compile-once-at-construction: malformed expressions error at
  validator construction, not per-token validation.
- Replace slice 50's `required_scope` + `scope_claim_name` fields
  on the validators with the JsonPath mechanism. Operators rewrite
  `customClaimCheck: { scope: 'X' }` to `customClaimCheck: "@.scope
  == 'X'"`. Greenfield: no compat shim.

**Operator (`crates/operator/`):**

- Replace `ListenerAuthenticationOAuth.custom_claim_check:
  Option<OAuthCustomClaimCheck>` (typed struct, slice 50) with
  `Option<String>` (the raw expression). Delete the
  `OAuthCustomClaimCheck` type entirely.
- Add `valid_token_type: Option<String>` field on
  `ListenerAuthenticationOAuth`. CRD-validated `minLength: 1` when
  set.
- Hand-rolled schema entry updated: `customClaimCheck` shape flips
  from object-with-properties to bare string. New
  `validTokenType: { type: string, minLength: 1 }` entry.
- Cross-mode validation: `valid_token_type` rejected with a new
  `ValidationError` variant when `accessTokenIsJwt: false`
  (introspection mode has no JWT header). Surfaces as
  `ListenersValid=False reason=...`.
- Reconciler render: both fields emit `custom_claim_check` and
  `valid_token_type` broker TOML keys when set. The OLD render code
  for `required_scope` + `scope_claim_name` is deleted.
- Cross-listener divergence walk: rewrite the existing
  `custom_claim_check` perturbation entry to the new string shape;
  add a perturbation for `valid_token_type`.

**E2E (`.github/workflows/operator-e2e.yml`):**

- `kind-oauth` job's Kafka CR YAML: replace the existing
  `customClaimCheck: { scope: kafka.write }` with
  `customClaimCheck: "@.scope == 'kafka.write'"`, and add
  `validTokenType: "JWT"`. Same producer Jobs (no client-side
  change; Keycloak emits `typ: JWT` by default).

### Out of scope

- **Slice 49h (claims mapping)** — `groupsClaim`,
  `groupsClaimDelimiter`, `fallbackUserNameClaim`,
  `fallbackUserNamePrefix`. Touches the `Principal` struct.
- **Slice 49i (JWKS refresher policies)** —
  `jwksMinRefreshPauseSeconds`, `jwksExpirySeconds`,
  `jwksIgnoreKeyUse`. Touches the slice-49b refresher loop.
- **Slice 49f (PLAIN-with-OAuth-token)** — skipped indefinitely per
  brainstorming.
- **Compile-cache for JsonPath expressions** — the validator's
  `Option<JsonPathInst>` is set once at construction; no per-token
  compile.
- **Strimzi byte-exact JsonPath semantics** — see "Semantic
  divergence" above; jsonpath-rust is close-enough-not-exact.
- **Validation that the expression is "well-formed Jayway"** — we
  only validate that jsonpath-rust accepts it. Operators using
  Jayway-only features get a parse error at broker startup, not at
  reconcile time.

## Wire / config / CRD shapes

### Broker TOML

```toml
[oauthbearer]
# existing keys ...
custom_claim_check = "@.scope == 'kafka.write' || @.roles contains 'admin'"
valid_token_type = "JWT"     # optional, JWT-mode validators only
```

### Operator CRD

```yaml
authentication:
  type: oauth
  validIssuerUri: https://...
  jwksEndpointUri: https://.../jwks
  customClaimCheck: "@.scope == 'kafka.write'"
  validTokenType: JWT          # optional, JWT-mode only
```

Reconciler emits the broker TOML keys above. Both fields are
optional; absent = no check.

## Architecture

### Component map

```
Operator                              Broker
─────────                             ─────
KafkaListenerAuthenticationOAuth      [oauthbearer]
  .customClaimCheck (string)            custom_claim_check (string)
  .validTokenType   (string)            valid_token_type   (string)
            │                                       │
            ▼                                       ▼
controller/listeners.rs               file_config.rs (apply_to)
  oauth_canonical extends                       │
  validate_listeners checks                     ▼
    cross-mode for validTokenType     BrokerConfig
  render_broker_toml emits              .oauthbearer_custom_claim_check
            │                           .oauthbearer_valid_token_type
            ▼                                   │
broker TOML over the wire                       ▼
            │                         OAuthBearerValidator
            └────────────────────►     compile_custom_claim_check (once)
                                       compile + store as Option<JsonPathInst>
                                                  │
                                                  ▼
                                       per-token validate():
                                         existing temporal/iss/aud checks
                                         + jsonpath evaluate -> bool
                                         + (JWT-mode only) typ header check
                                         on fail → AuthError::InvalidToken
```

### JsonPath integration

`jsonpath-rust` crate API surface (verify at plan time):

- `JsonPathInst::from_str(expr: &str) -> Result<Self, ParseError>` —
  compiles an expression.
- `JsonPathInst::find(value: &Value) -> Value` (or similar) — runs
  the expression against a JSON value, returns a result Value.

Evaluation contract for `customClaimCheck`:

- Run the JsonPath against the token claims (`serde_json::Value`).
- Token is REJECTED if the result is:
  - JSON `null`, JSON `false`, empty array, empty object.
- Token is ACCEPTED otherwise (non-empty result, true, non-zero
  number, non-empty string, etc.).

This matches Strimzi's "expression yields truthy" semantics.

### Validator construction

The OAuth validators are constructed at broker startup from
`BrokerConfig` (built from the parsed TOML). The `compile_custom_claim_check`
helper runs at THAT moment. If the expression is malformed,
broker startup fails with a clear panic + reason. The operator's
CRD-level validation only checks `minLength: 1`; the operator does
NOT itself parse the expression (greenfield: keep operator
dependency-free of broker-specific libraries).

### `valid_token_type` validation

JWT-mode validators (`UnsecuredJwsValidator`, `SignedJwsValidator`)
parse the JWT header alongside the payload. When `valid_token_type`
is `Some`, the validator extracts `header.typ` and compares with
string equality. Mismatch → `AuthError::InvalidToken`.

The introspection validator skips this check entirely (introspection
responses don't include a JWT header). The cross-mode validator in
the operator ensures that operators can't accidentally configure
`validTokenType` on an introspection-mode listener (rejected at
reconcile time).

### Slice-50 stub removal

Slice 50 shipped a `customClaimCheck: { scope, scope_claim }` typed
struct + corresponding `required_scope` / `scope_claim_name` fields
on the unsecured/signed validators. These are deleted:

- Operator: `OAuthCustomClaimCheck` type + its schema entry + its
  render code + its tests get rewritten or removed.
- Broker: `UnsecuredJwsValidator::required_scope`,
  `UnsecuredJwsValidator::scope_claim_name`, equivalents on
  `SignedJwsValidator`, and the `scope_contains()` /
  `scope_claim_contains()` helpers are deleted. Existing
  scope-related broker tests rewritten to use the JsonPath form.
- Sample manifest: existing `customClaimCheck: { scope: kafka.write }`
  block rewritten to `customClaimCheck: "@.scope == 'kafka.write'"`.
- E2E: `kind-oauth` job's Kafka CR YAML rewritten similarly.

This is a coordinated rewrite across ~10-15 sites. Each task picks
up its own files.

## Testing

### Broker unit tests (`crates/security/src/oauthbearer.rs::tests`)

- `unsecured_validate_rejects_when_custom_claim_check_fails` —
  expression doesn't match the claims; `AuthError::InvalidToken`.
- `unsecured_validate_accepts_when_custom_claim_check_passes` —
  expression matches; ok.
- `unsecured_validate_rejects_when_valid_token_type_mismatch` —
  `validTokenType: "JWT"`, token header `"typ": "OPAQUE"` →
  rejected.
- `unsecured_validate_accepts_when_valid_token_type_match` — ok.
- `unsecured_validate_accepts_when_valid_token_type_unset_regardless_of_header`
  — regression for the no-config path.
- `signed_validate_*` — same 5 patterns for `SignedJwsValidator`.
- `introspection_validate_rejects_when_custom_claim_check_fails`.
- `introspection_validate_does_not_check_valid_token_type` — even
  if set on BrokerConfig, the introspection validator skips it.
- `custom_claim_check_compile_error_at_validator_construction` —
  malformed JsonPath (e.g., `@.unterminated`) → construction errors;
  validator not built.

### Operator unit tests (`crates/operator/src/crd/listener.rs::tests`)

- `oauth_round_trip_with_custom_claim_check_string` — new shape.
- `oauth_round_trip_with_valid_token_type`.
- Schema regression extension: `customClaimCheck` is now a string
  (not object); `validTokenType` is a new property.
- `oauth_old_custom_claim_check_object_shape_no_longer_parses` —
  the slice-50 struct shape now fails to parse. Confirms the
  breaking change.

### Operator reconciler tests (`crates/operator/src/controller/listeners.rs::tests`)

- `render_broker_toml_emits_custom_claim_check_when_set`.
- `render_broker_toml_emits_valid_token_type_when_set`.
- `validate_listeners_rejects_valid_token_type_in_introspection_mode`
  — new cross-mode check.
- Divergence walk: rewrite the existing `custom_claim_check`
  perturbation entry to the new string shape; add a perturbation
  for `valid_token_type`.

### Operator integration tests (`crates/operator/tests/reconcile_listener_oauth.rs`)

- `oauth_listener_with_custom_claim_check_expression_renders_broker_toml_key`.
- `oauth_listener_with_valid_token_type_renders_broker_toml_key`.
- `oauth_listener_valid_token_type_in_introspection_mode_rejected_with_listeners_valid_false`
  — end-to-end cross-mode rejection.

### E2E

`kind-oauth` job's Kafka CR YAML rewrite. Same producer Jobs (no
client-side change). Verify Keycloak emits `typ: JWT` in tokens by
default (it does, per Keycloak release notes — but the
implementer should sanity-check at plan time, since changing the
producer to fake a different `typ` is a real maintenance burden if
needed).

### No JVM differential test

JVM admin tools don't read listener OAuth config.

## File touch list

- `crates/security/Cargo.toml` — new `jsonpath-rust` dep.
- `crates/security/src/oauthbearer.rs` — validator integration +
  unit tests + delete `required_scope`/`scope_claim_name`/related
  helpers.
- `crates/broker/src/file_config.rs` — new `FileOAuthBearerConfig`
  fields + `apply_to` threading.
- `crates/broker/src/config.rs` — new `BrokerConfig` fields.
- `crates/operator/src/crd/listener.rs` — replace
  `custom_claim_check` shape + add `valid_token_type` + delete
  `OAuthCustomClaimCheck` + sweep fixtures in this file's tests.
- `crates/operator/src/controller/listeners.rs` — render update +
  cross-mode validation + divergence walk + unit tests + sweep
  fixtures in this file's tests.
- `crates/operator/src/controller/kafka.rs` +
  `crates/operator/src/controller/kafka_node_pool.rs` — fixture
  sweep (expected per slice 50d's experience — these files have
  test fixtures that won't compile after the struct change).
- `crates/operator/tests/reconcile_listener_oauth.rs` +
  `tests/reconcile_oauth_introspection.rs` +
  `tests/reconcile_oauth_trust.rs` — fixture sweep + 3 new
  integration tests.
- `crates/operator/sample/oauth-listener.yaml` — rewrite
  `customClaimCheck` block.
- `deploy/crds/crabka.io_kafkas.yaml` — regenerated CRD.
- `.github/workflows/operator-e2e.yml` — `kind-oauth` job CR YAML.
- `STATUS.md` — slice 49g entry.

## Decomposition for the plan

Six tasks across four batches (mirrors slice 50d's shape):

| Batch | Task | Files |
|---|---|---|
| 1 | T1 — Broker dep + validator integration + unit tests + delete slice-50 stub | `crates/security/*`, `crates/broker/src/file_config.rs`, `crates/broker/src/config.rs` |
| 2 | T2 — Operator CRD shape change + add validTokenType + own-file fixture sweep | `crates/operator/src/crd/listener.rs` |
| 2 | T3 — Operator reconciler: render + cross-mode validation + divergence walk + own-file fixture sweep + sibling-file (kafka.rs / kafka_node_pool.rs) sweep | `crates/operator/src/controller/listeners.rs`, `controller/kafka.rs`, `controller/kafka_node_pool.rs` |
| 3 | T4 — Operator integration tests + sample + CRD regen | `crates/operator/tests/reconcile_*.rs`, `crates/operator/sample/*`, `deploy/crds/*` |
| 3 | T5 — kind-oauth e2e CR YAML rewrite | `.github/workflows/operator-e2e.yml` |
| 4 | T6 — STATUS.md + final gate | `STATUS.md` |

Dependency chain: T1 → T2 → T3 → (T4 ‖ T5) → T6. Same pattern as
slice 50d:

- T2 + T3 file-disjoint by design but dispatched sequentially
  because T3's verify needs T2's struct field changes present.
- T4 + T5 truly parallel (different files, no struct dependency).
- T3 owns the sweep of `kafka.rs` / `kafka_node_pool.rs` per
  slice 50d's pattern (T2 sweeps `crd/listener.rs`, T3 sweeps the
  rest of `controller/`).

## Plan-time TBDs (deferred to implementer judgment)

- Exact `jsonpath-rust` crate version (`^1` or current stable;
  implementer verifies license + active maintenance before adding
  the dep).
- Exact name of the new `ValidationError` variant for the
  introspection-mode `validTokenType` reject (match existing slice
  50c convention — entity-prefix + problem-suffix).
- Whether to emit a broker startup log line confirming the
  `custom_claim_check` expression compiled successfully (match
  existing `[oauthbearer]` startup-log style).
- Whether the JsonPath compile failure at broker startup panics or
  returns a clean error (existing `[oauthbearer]` config errors
  panic with a descriptive message — match that pattern).

## Breaking change footprint

This slice is more breaking than 50d (which only added an optional
field). 49g RENAMES the existing `customClaimCheck` shape:

- Old: `customClaimCheck: { scope: kafka.write }` or
  `customClaimCheck: { scope: kafka.write, scopeClaim: scope }`.
- New: `customClaimCheck: "@.scope == 'kafka.write'"`.

Tracking sites that get rewritten (per task ownership):

- T1: broker validators' `required_scope`/`scope_claim_name` fields
  + helpers deleted; ~5 broker tests touched in `oauthbearer.rs`.
- T2: operator CRD `OAuthCustomClaimCheck` struct deleted; schema
  entry rewritten; ~9 sweep sites in `crd/listener.rs` tests.
- T3: operator reconciler render code deleted/rewritten; ~10+
  sweep sites in `controller/*` tests; divergence-walk perturbation
  entry rewritten.
- T4: ~3 sweep sites in `tests/reconcile_*.rs`; sample manifest
  rewritten.
- T5: `kind-oauth` job CR YAML rewritten.

All in-tree migrations are mechanical. No CLAUDE.md-incompatible
shims; the slice-50 typed struct just disappears.

## After 49g lands

The OAUTHBEARER umbrella is at 5/7 cluster-equivalents shipped
(49g leaves 49h + 49i to follow). After all three "long-tail"
clusters ship, the umbrella reaches Strimzi field parity (modulo
the explicitly-skipped 49f PLAIN-with-OAuth-token).

Natural sequencing for the remaining clusters:

- **49h** (claims mapping): `groupsClaim` + `groupsClaimDelimiter`,
  `fallbackUserNameClaim` + `fallbackUserNamePrefix`. Touches
  Principal — useful one-time scaffolding for slice 53/54
  authorizer work.
- **49i** (JWKS refresher policies): `jwksMinRefreshPauseSeconds`,
  `jwksExpirySeconds`, `jwksIgnoreKeyUse`. Smallest cluster;
  defensive operational tuning.
