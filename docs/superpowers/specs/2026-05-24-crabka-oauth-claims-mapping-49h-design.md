# Slice 49h — Operator + Broker: OAUTHBEARER claims mapping (fallback principal chain + groups extraction)

Status: Draft
Date: 2026-05-24
Umbrella: `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`
Builds on: slices 49b/49d (JWT/introspection validators), 49g (jsonpath-rust dep + customClaimCheck)
Followups: slice 49i (JWKS refresher policies — last of the long-tail clusters)

## Goal

Second of three "long-tail" Strimzi-parity clusters closing the
OAUTHBEARER umbrella. Adds four Strimzi-shape fields on the listener
OAuth CRD + broker validators:

- `fallbackUserNameClaim` + `fallbackUserNamePrefix` — alternate
  principal-claim chain for tokens missing the primary claim (Keycloak
  service-account convention).
- `groupsClaim` + `groupsClaimDelimiter` — extract group memberships
  from token claims via JsonPath (RFC 9535 via jsonpath-rust); attach
  to the `Principal` struct.

Slice 49f (PLAIN-with-OAuth-token) was explicitly skipped per the
49g brainstorming session. Slice 49i (JWKS refresher policies)
follows this slice.

## Why bundle both sub-clusters

Per the 49g brainstorming session, the user chose to ship the
long-tail as three feature-sliced pairs (validation → claims → JWKS).
49h = the claims pair. The fallback chain (2 fields) and groups
extraction (2 fields) both touch the validators' claim-resolution
logic; shipping them together keeps the validator changes coherent.

## No broker-side consumer of groups yet (acknowledged)

The `Principal.groups: Vec<String>` field is populated by OAuth
validators but has no broker-side authorizer reading it today. Slice
53/54 in the operator roadmap (OPA / Keycloak authorizer plugins)
will consume groups for ACL decisions. Until then, groups are stored
on Principal for:

- Future authorizer integration (the load-bearing motivation).
- Observability — operators can `tracing::debug!` Principal contents
  to see what groups OAuth tokens carry.

Greenfield Crabka can ship the scaffolding now (cheap struct
extension + small cascade). Documented as "no consumer yet" in
STATUS and the field's doc-comment.

## Strimzi semantic parity

| Field | Strimzi behavior | Crabka 49h behavior |
|---|---|---|
| `fallbackUserNameClaim` | Flat claim name. Used when primary `userNameClaim` is absent/empty. | Same. |
| `fallbackUserNamePrefix` | Prepended to resolved name ONLY when fallback fires. Strimzi convention: `"service-account-"` to namespace service-account principals. | Same. |
| `groupsClaim` | JsonPath expression (Jayway in Strimzi). | RFC 9535 JsonPath via jsonpath-rust (same syntax divergence as 49g — operators rewrite predicates if they had Jayway nested filters). |
| `groupsClaimDelimiter` | Split delimited string into groups when claim is string-typed. Ignored when claim is array. | Same. |

`fallbackUserNamePrefix` without `fallbackUserNameClaim` is accepted
silently (no-op) per Strimzi behavior. The operator does not reject
this combination; the field is just unused.

## Scope

### In scope

**Broker (`crates/security/`, `crates/broker/`):**

- New `Principal.groups: Vec<String>` field. Defaults to empty
  everywhere; populated by OAuth validators when `groups_claim` is
  set.
- Four new `[oauthbearer]` TOML keys:
  - `fallback_user_name_claim: Option<String>`
  - `fallback_user_name_prefix: Option<String>`
  - `groups_claim: Option<String>` (the JsonPath expression as
    string; compiled at broker startup like 49g's
    `custom_claim_check`)
  - `groups_claim_delimiter: Option<String>`
- All three validators (`UnsecuredJwsValidator`, `SignedJwsValidator`,
  `IntrospectionValidator`) carry the four new fields (with
  `groups_claim: Option<JpQuery>` precompiled at construction).
- Validator `validate()` body changes:
  1. **Principal-name resolution** — primary claim, then fallback,
     then prefix (only on fallback path), then reject if both
     absent/empty.
  2. **Groups extraction** — after name resolution, run `groups_claim`
     JsonPath against claims; populate `Principal.groups`.
- New `extract_groups(path, claims, delimiter)` helper in
  `crates/security/src/oauthbearer.rs`.

**Operator (`crates/operator/src/crd/listener.rs`):**

- Four new `Option<String>` fields on `ListenerAuthenticationOAuth`:
  `fallback_user_name_claim`, `fallback_user_name_prefix`,
  `groups_claim`, `groups_claim_delimiter`.
- Hand-rolled schema entries: all four `{ type: string, minLength: 1 }`.

**Operator reconciler (`crates/operator/src/controller/listeners.rs`):**

- `render_broker_toml` emits the four new TOML keys when set.
  `groups_claim` uses TOML multi-line literal (`'''...'''`) per slice
  49g's pattern for JsonPath. Others use double-quoted strings.
- Cross-listener divergence walk extended with four new perturbations.
- NO new cross-mode validation rules: all four fields work in both
  JWT and introspection modes (introspection responses are JSON
  objects too — JsonPath evaluates identically; fallback claim lookup
  is mode-agnostic).

**E2E (`.github/workflows/operator-e2e.yml`):**

- Both `kind-oauth` (JWT mode) and `kind-oauth-introspection`
  (introspection mode) Kafka CRs add `groupsClaim:
  "$.realm_access.roles[*]"` (Keycloak emits `realm_access.roles` by
  default; verify at plan time).
- `fallbackUserNameClaim` is harder to e2e (existing producer Jobs
  use tokens with both `sub` and `client_id`; the fallback path
  requires a token WITHOUT `sub`). Skip from e2e; unit-tested only.

### Out of scope

- **Broker-side groups consumer.** Authorizer integration is slice
  53/54 in the operator roadmap. This slice ships the scaffolding.
- **JsonPath for `fallbackUserNameClaim`.** Strimzi treats it as a
  flat claim name. Match.
- **Fallback chains of arbitrary length.** Strimzi supports exactly
  ONE fallback claim, not a sequence. Match.
- **Reading groups in mTLS / PLAIN / SCRAM principals.** Those auth
  paths don't have token claims; their `Principal.groups` is always
  empty.
- **Slice 49i** (JWKS refresher policies) — follows.
- **Slice 49f** (PLAIN-with-OAuth-token) — skipped indefinitely.

## Wire / config / CRD shapes

### Broker TOML (new keys under `[oauthbearer]`)

```toml
[oauthbearer]
# existing keys (slice 49b/49d/49g/50d) ...
fallback_user_name_claim = "client_id"
fallback_user_name_prefix = "service-account-"
groups_claim = '''$.realm_access.roles[*]'''
groups_claim_delimiter = ","   # only when groups_claim resolves to a string
```

### Operator CRD

```yaml
authentication:
  type: oauth
  validIssuerUri: https://...
  jwksEndpointUri: https://.../jwks
  userNameClaim: preferred_username
  fallbackUserNameClaim: client_id           # new in 49h
  fallbackUserNamePrefix: "service-account-" # new in 49h
  groupsClaim: "$.realm_access.roles[*]"     # new in 49h
  groupsClaimDelimiter: ","                  # new in 49h
```

### Principal struct

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub auth_method: AuthMethod,
    /// Slice 49h: OAuth-derived group memberships. Empty vec for
    /// non-OAuth principals and for OAuth without `groupsClaim`.
    pub groups: Vec<String>,
}
```

## Architecture

### Data flow trace

Operator deploys Kafka CR with `groupsClaim: "$.realm_access.roles[*]"`
and `userNameClaim: preferred_username` and `fallbackUserNameClaim:
client_id` and `fallbackUserNamePrefix: "service-account-"`. Client
connects with bearer token that has:

- `preferred_username: null` (absent)
- `client_id: "svc1"`
- `realm_access: { roles: ["admin", "ops"] }`

Trace:

1. CRD validation accepts all 4 fields.
2. Reconciler renders the 4 TOML keys.
3. Broker `FileOAuthBearerConfig::apply_to` compiles
   `$.realm_access.roles[*]` to `JpQuery`, injects into validator
   alongside the simple-string fields.
4. Token arrives. Validator:
   - Header + claims temporal checks pass.
   - `valid_token_type` / `custom_claim_check` checks (49g) pass.
   - Principal-name resolution:
     - Primary `preferred_username` → absent/empty.
     - Fallback `client_id` → `"svc1"`.
     - Prefix `service-account-` applied (fallback fired).
     - Final name: `"service-account-svc1"`.
   - Groups extraction:
     - JsonPath `$.realm_access.roles[*]` → `["admin", "ops"]`.
     - `groups_claim_delimiter` is None → array elements used as-is.
     - Final groups: `vec!["admin".into(), "ops".into()]`.
   - Construct `AuthOutcome { principal: Principal { name:
     "service-account-svc1", auth_method: SaslOAuthBearer, groups:
     vec!["admin", "ops"] }, expires_at_ms: Some(...) }`.

### Principal-name resolution (the new logic)

Replaces the existing single-claim lookup in each validator's
`validate()`:

```rust
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
```

Three observable behaviors:

- Primary present → use primary, no prefix.
- Primary absent, fallback present → use fallback, with prefix if set.
- Primary AND fallback absent → reject (no change from existing
  rejection — just the error path now considers both).

### Groups extraction (the new helper)

```rust
fn extract_groups(
    path: &JpQuery,
    claims: &Value,
    delimiter: Option<&str>,
) -> Vec<String> {
    let results = jsonpath_rust::query::js_path_process(path, claims)
        .unwrap_or_default();
    let mut out = Vec::new();
    for r in results {
        let v: &Value = r.deref();
        match v {
            Value::String(s) => match delimiter {
                Some(d) => out.extend(
                    s.split(d)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(String::from),
                ),
                None => out.push(s.clone()),
            },
            Value::Array(items) => out.extend(
                items.iter().filter_map(Value::as_str).map(String::from),
            ),
            _ => {} // ignore numbers, objects, nulls
        }
    }
    out
}
```

Semantics:

- Result is an array → each string element is a group.
- Result is a delimited string + delimiter set → split + trim + drop
  empty.
- Result is a non-delimited string → the whole string is one group.
- Result is a number / object / null → ignored (no error).
- JsonPath returns multiple matches → results concatenated.
- JsonPath returns empty (no matches) → `vec![]` (no error).

### Principal struct cascade

`Principal { name, auth_method }` literal sites get `groups: vec![]`
added. Expected sites (per slice-50d / 49g sweep size estimates):

- `crates/security/`: ~3 sites (test fixtures).
- `crates/broker/`: ~15-20 sites (PLAIN/SCRAM/mTLS/anonymous
  construction + dispatch.rs + tests).
- `crates/operator/`: probably 0 (operator doesn't construct
  `Principal` directly; that's broker-side).
- `crates/client-core/`: possibly a few (if Principal is exposed in
  client-facing types).

T1 owns the cascade sweep. The plan's file-touch list will identify
sites after a grep.

### Validator construction (apply_to)

In `crates/broker/src/file_config.rs::FileOAuthBearerConfig::apply_to`,
extend the per-validator-branch initialization. For each validator
(unsecured / signed / introspection):

```rust
v.fallback_user_name_claim = oauth.fallback_user_name_claim.clone();
v.fallback_user_name_prefix = oauth.fallback_user_name_prefix.clone();
v.groups_claim = oauth.groups_claim.as_deref().map(|expr| {
    parse_json_path(expr).unwrap_or_else(|e| {
        panic!("[oauthbearer]: invalid groups_claim JsonPath expression {expr:?}: {e}")
    })
});
v.groups_claim_delimiter = oauth.groups_claim_delimiter.clone();
```

The `groups_claim` panic-on-malformed pattern matches slice 49g's
`custom_claim_check`.

## Testing

### Broker unit tests (`crates/security/src/oauthbearer.rs::tests`)

Aim for ~10-12 new validator tests (not full 5×3 matrix — some
behavior is identical across validators and can be unsecured-only).

- `unsecured_validate_uses_primary_principal_claim_when_present`
  (regression).
- `unsecured_validate_falls_back_to_alt_claim_when_primary_absent`.
- `unsecured_validate_applies_fallback_prefix_only_on_fallback`.
- `unsecured_validate_rejects_when_neither_primary_nor_fallback_present`.
- `unsecured_validate_extracts_groups_from_array_claim`.
- `unsecured_validate_extracts_groups_from_delimited_string`.
- `unsecured_validate_extracts_groups_from_nested_claim_via_jsonpath`.
- `unsecured_validate_returns_empty_groups_when_claim_unset`.
- `unsecured_validate_returns_empty_groups_when_claim_resolves_to_empty`.
- `signed_validate_falls_back_to_alt_claim_when_primary_absent`
  (one signed-validator parity test).
- `introspection_validate_extracts_groups_from_introspection_response`
  (one introspection parity test).

### `extract_groups` table-driven helper test

- Array of strings → all elements.
- String + delimiter → split + trim + filter.
- String without delimiter → single group.
- Number / object / null → ignored.
- Empty match → empty vec.

### Operator unit tests (`crates/operator/src/crd/listener.rs::tests`)

- 4 round-trip tests (one per new field): with-field-set + omits-when-unset (8 tests total).
- Schema regression test extension with 4 new property keys.

### Operator reconciler tests (`crates/operator/src/controller/listeners.rs::tests`)

- 4 render-emit tests (one per field).
- Existing divergence walk gets 4 new perturbation entries.

### Operator integration tests (`crates/operator/tests/reconcile_listener_oauth.rs`)

- `oauth_listener_with_fallback_user_name_claim_renders_broker_toml_key`.
- `oauth_listener_with_groups_claim_renders_broker_toml_key`.

### E2E

`kind-oauth` AND `kind-oauth-introspection` Kafka CRs get:

```yaml
groupsClaim: "$.realm_access.roles[*]"
```

Producer Jobs still use the same tokens; Keycloak's default
`realm_access.roles` shape satisfies the path. No client-side change.
If Keycloak's default realm doesn't populate roles for the
`kafka-client`, add a Keycloak role-mapper in the realm bootstrap —
verify at plan time.

`fallbackUserNameClaim` not exercised in e2e (would require a
token-shape change to omit `sub`). Unit tests cover it.

### No JVM differential

JVM admin tools don't read listener OAuth config.

## File touch list

- `crates/security/src/principal.rs` — `Principal.groups` field.
- `crates/security/src/oauthbearer.rs` — validator field extensions +
  `validate()` body changes + `extract_groups` helper + unit tests.
- `crates/security/` — fixture-sweep sites for `Principal { ... }`
  literals (broker-side test helpers).
- `crates/broker/src/file_config.rs` — `FileOAuthBearerConfig` field
  extensions + `apply_to` threading.
- `crates/broker/src/config.rs` — BrokerConfig fields if needed
  (likely not — same pattern as 49g where validator carries the
  compiled state directly).
- `crates/broker/` — fixture-sweep sites for `Principal { ... }`
  literals in PLAIN/SCRAM/mTLS construction + dispatch.rs + tests.
- `crates/operator/src/crd/listener.rs` — 4 new fields + schema +
  own-file fixture sweep + round-trip tests.
- `crates/operator/src/controller/listeners.rs` — render + divergence
  walk + own-file fixture sweep + reconciler unit tests.
- `crates/operator/src/controller/kafka.rs` +
  `crates/operator/src/controller/kafka_node_pool.rs` — fixture
  sweep (per slice 49g/50d's pattern).
- `crates/operator/tests/reconcile_listener_oauth.rs` +
  `reconcile_oauth_introspection.rs` + `reconcile_oauth_trust.rs` —
  fixture sweep + 2 new integration tests.
- `crates/operator/sample/oauth-listener.yaml` — add commented-out
  examples of the 4 new fields.
- `deploy/crds/crabka.io_kafkas.yaml` — regenerated CRD picks up the
  4 new properties.
- `.github/workflows/operator-e2e.yml` — `kind-oauth` AND
  `kind-oauth-introspection` Kafka CRs add `groupsClaim`.
- `STATUS.md` — slice 49h entry.

## Decomposition for the plan

Six tasks across four batches (mirrors slice 49g):

| Batch | Task | Files (file-disjoint within batch) |
|---|---|---|
| 1 | T1 — Broker: Principal extension + validator changes + extract_groups + unit tests | `crates/security/*`, `crates/broker/src/file_config.rs`, broker `Principal { ... }` sweep sites |
| 2 | T2 — Operator CRD: 4 new fields + schema + round-trip tests + own-file fixture sweep | `crates/operator/src/crd/listener.rs` |
| 2 | T3 — Operator reconciler: render + divergence walk + own-file + sibling-file fixture sweep + unit tests | `crates/operator/src/controller/listeners.rs`, `controller/kafka.rs`, `controller/kafka_node_pool.rs` |
| 3 | T4 — Operator integration tests + sample + CRD regen + sibling-test fixture sweep | `crates/operator/tests/reconcile_*.rs`, `sample/oauth-listener.yaml`, `deploy/crds/*` |
| 3 | T5 — kind-oauth AND kind-oauth-introspection e2e CR YAML extension | `.github/workflows/operator-e2e.yml` |
| 4 | T6 — STATUS.md + final gate | `STATUS.md` |

**Dependency chain**: T1 → T2 → T3 → (T4 ‖ T5) → T6. Same pattern as
slice 49g:

- T2 + T3 file-disjoint by design but dispatched sequentially (T3's
  fixture sweep needs T2's struct field present).
- T4 + T5 truly parallel.

**Holistic-review lesson from 49g**: T5 must touch BOTH `kind-oauth`
AND `kind-oauth-introspection` Kafka CR YAMLs — adding `groupsClaim`
to only one would leave the other partially configured. Both modes
have JSON claims; both can extract groups.

## Plan-time TBDs

- Exact site count for the `Principal { ... }` cascade — grep at
  plan time, expect ~15-25 sites across `crates/security/` and
  `crates/broker/`.
- Whether Keycloak's default realm populates `realm_access.roles`
  for the `kafka-client` (it should for any client with realm-role
  assignments; verify the existing realm bootstrap in the e2e
  workflow).
- Whether to emit a `tracing::debug!` log line when groups are
  extracted (operator visibility; trivial to add — implementer
  judgment).
- Whether to add a no-op-prefix-without-claim CRD validation rule
  (Strimzi accepts silently; we match — but implementer may
  surface as a warning in reconcile output).

## Breaking change footprint

SMALLER than slice 49g (which deleted a typed struct). This slice
only ADDS fields. The cascade is mechanical:

- `Principal { ... }` literals get `groups: vec![]` — ~15-25 sites
  across security + broker crates. T1 owns.
- `ListenerAuthenticationOAuth { ... }` literals get 4 new `None`
  defaults — ~21 sites per slice 49g's experience. T2/T3/T4 own.

No public API removals. No CRD-shape breakages (only field
additions).

## After 49h lands

**49i** (JWKS refresher policies — `jwksMinRefreshPauseSeconds`,
`jwksExpirySeconds`, `jwksIgnoreKeyUse`) is next and last. Touches
the slice-49b JWKS refresher loop. Smallest cluster of the three.

After 49i, the OAUTHBEARER umbrella is at Strimzi field parity
(modulo skipped 49f PLAIN-with-OAuth-token).
