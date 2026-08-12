# Slice 49: SASL/OAUTHBEARER (KIP-255 / RFC 7628) — Design

**Status:** Approved 2026-05-23.

**Goal:** Add `OAUTHBEARER` as a fourth broker-side SASL mechanism alongside
the existing `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`. A client presents
a bearer token in an RFC 7628 SASL exchange; the broker validates it and
derives the connection principal from a token claim. This is the Crabka-core
half of Phase 9 — it unblocks operator slice 50 (`KafkaUser` OAuth + listener
OAuth config).

The concrete validator implemented here is Kafka's **unsecured JWS** validator
(`alg: none`), which is the built-in, no-external-dependency path used by the
JVM `OAuthBearerLoginModule` / `OAuthBearerUnsecuredValidatorCallbackHandler`
for development and testing. Production signed-token validation against a JWKS
endpoint (RS256/ES256 signature verification, issuer/audience checks, key
rotation) is deferred to slice 49b.

---

## 1. Scope

### In

- `SaslMechanism::OAuthBearer` (wire name `OAUTHBEARER`); `AuthMethod::SaslOAuthBearer`.
- `crates/security/src/oauthbearer.rs` — pure logic:
  - `parse_client_initial_response(bytes) -> Result<ClientInitialResponse, AuthError>`
    — RFC 7628 GS2 + kvpair parse extracting the bearer token (+ optional authzid).
  - `UnsecuredJwsValidator` — validates an unsecured JWS (`alg:none`), checking
    the expiration (`exp`, required), not-before/issued-at (`iat`, optional),
    optional required scope, and extracting the principal from a configurable
    claim (default `sub`). Returns a `Principal { auth_method: SaslOAuthBearer }`.
  - `invalid_token_json()` — the `{"status":"invalid_token"}` server error body.
- Broker SASL plumbing (`network/auth.rs`):
  - `SaslExchange::OAuthBearer` (post-handshake) + `SaslExchange::OAuthBearerFailed`
    (error JSON sent, awaiting the client's RFC 7628 `\x01` "dummy" final message).
  - `handle_authenticate_oauthbearer` — the success / failure / dummy state machine.
- `network/dispatch.rs` — route `OAUTHBEARER` in the `SaslAuthenticate` match.
- `BrokerConfig.oauthbearer_validator: UnsecuredJwsValidator` (default unsecured,
  principal claim `sub`, 30s clock skew). Consulted only when `OAUTHBEARER` is
  in `enabled_sasl_mechanisms` (the handshake won't advertise it otherwise).
- TOML `[oauthbearer]` section in `FileConfig` (principal claim name, allowable
  clock-skew ms, optional required scope). `enabled_mechanisms = ["OAUTHBEARER"]`
  already parses via the existing `from_wire`.
- Unit tests (parser, validator, handler) + one broker integration test +
  one `#[ignore]` JVM acceptance test.

### Out (deferred)

| Concern | Slice |
|---|---|
| Signed JWT validation via JWKS (RS256/ES256, issuer/audience, key rotation) | 49b |
| Token re-authentication / `session_lifetime_ms` expiry (KIP-368) | 49b |
| OAUTHBEARER for inter-broker / controller-listener outbound | ✅ shipped; token-file credentials are re-read on each connection |
| `KafkaUser` OAuth + `Kafka.spec` listener OAuth config | 50 (operator) |

### Semantics for slice 49

- **Single round on success.** Valid token → empty server `auth_bytes`,
  `error_code = 0`, connection `Authenticated`. Matches Kafka's
  `OAuthBearerSaslServer`.
- **Two rounds on failure.** Invalid token → `error_code = 0` carrying the
  `{"status":"invalid_token"}` JSON in `auth_bytes` (connection stays open);
  the JVM client replies with a single `\x01`; the broker then returns
  `error_code = 58` (`SASL_AUTHENTICATION_FAILED`) and closes. This is the
  exact RFC 7628 / Kafka failure handshake — the existing dispatcher rule
  `close = error_code != 0` already produces the right close timing.
- **Principal = token claim.** The configured principal claim (default `sub`).
  A non-empty client authzid must equal that principal or auth fails.
- **Unsecured only.** `alg` must be `none` and the JWS signature segment empty;
  any signed token is rejected this slice (49b adds JWKS).

---

## 2. RFC 7628 wire format

Client initial response (what the JVM `OAuthBearerSaslClient` sends):

```
n,,^Aauth=Bearer <token>^A^A                 (^A = 0x01, authzid empty)
```

`<token>` for the unsecured path is a JWS compact serialization
`base64url(header).base64url(claims).` with an empty signature segment:

```
header  = {"alg":"none"}
claims  = {"sub":"admin","iat":1716...,"exp":1716...}
```

Parse: split off the GS2 header at the first `\x01`; the remainder is
`\x01`-separated kvpairs terminated by an empty pair; find `auth=Bearer …`.

---

## 3. Validation (`UnsecuredJwsValidator`)

```rust
pub struct UnsecuredJwsValidator {
    pub principal_claim_name: String,    // default "sub"
    pub scope_claim_name: String,        // default "scope"
    pub required_scope: Option<String>,  // default None
    pub allowable_clock_skew_ms: i64,    // default 30_000
}
```

`validate(token, now_ms) -> Result<Principal, AuthError>`:

1. Split into exactly 3 dot segments; signature segment must be empty.
2. base64url-decode header + claims; `header.alg == "none"`.
3. `exp` required: reject if `exp_ms + skew <= now_ms` (expired).
4. `iat` optional: reject if `iat_ms - skew > now_ms` (issued in the future).
5. `required_scope`, if set, must appear in the token scope (string or array).
6. principal = claims[`principal_claim_name`] as a non-empty string.

`exp`/`iat` are JWT NumericDate (seconds, possibly fractional) → ms.

---

## 4. Testing

- **Unit (`oauthbearer.rs`):** parse happy path + malformed (no auth kvpair,
  bad GS2); validator accepts a fresh token, rejects expired / future-iat /
  signed (`alg:RS256`) / missing-principal / missing-exp / wrong-scope; authzid
  mismatch fails.
- **Unit (`network/auth.rs`):** handshake advertises OAUTHBEARER; authenticate
  success → `Authenticated`; failure → error JSON, `error_code 0`, state
  `OAuthBearerFailed`; dummy round → `error_code 58`.
- **Broker wire integration (`crates/broker/tests/auth_handlers.rs`):** two raw
  TCP cases reusing the existing `round_trip` helper (no JVM, no Docker) — a
  full `ApiVersions` → `SaslHandshake` → `SaslAuthenticate` → Metadata happy
  path, and the expired-token two-round `invalid_token` → 58 failure handshake.
- **JVM acceptance (`jvm_acceptance.rs`, `#[ignore]`):**
  `jvm_sasl_oauthbearer_produce_consume` — `SASL_PLAINTEXT` + `OAUTHBEARER`
  broker; `kafka-console-producer` / `-consumer` with
  `OAuthBearerLoginModule unsecuredLoginStringClaim_sub="admin"` +
  `OAuthBearerUnsecuredLoginCallbackHandler`; produce 10, consume 10.

---

## 5. File structure

```
crates/security/
├── Cargo.toml                 # MODIFIED — add serde_json
└── src/
    ├── lib.rs                 # MODIFIED — mod oauthbearer; re-exports
    ├── mechanism.rs           # MODIFIED — OAuthBearer variant
    ├── principal.rs           # MODIFIED — SaslOAuthBearer + from_sasl
    └── oauthbearer.rs         # NEW — parser + UnsecuredJwsValidator
crates/broker/src/
├── config.rs                  # MODIFIED — oauthbearer_validator field
├── file_config.rs             # MODIFIED — [oauthbearer] TOML section
├── network/auth.rs            # MODIFIED — exchange variants + handler
├── network/dispatch.rs        # MODIFIED — route OAUTHBEARER
├── network/client.rs          # MODIFIED — shared outbound RFC 7628 client
├── raft_handshake.rs          # MODIFIED — controller-listener validation
└── handlers/describe_user_scram_credentials.rs  # MODIFIED — non-SCRAM arm
crates/broker/tests/
├── auth_handlers.rs           # MODIFIED — two wire-level OAUTHBEARER cases
└── jvm_acceptance.rs          # MODIFIED — OAUTHBEARER produce/consume
```

No protocol regeneration: `SaslHandshake` / `SaslAuthenticate` types already
cover the wire envelope.
