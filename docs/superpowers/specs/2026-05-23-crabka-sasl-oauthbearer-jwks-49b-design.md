# Slice 49b: SASL/OAUTHBEARER — JWKS / signed-JWT validation — Design

**Status:** Approved 2026-05-23.

**Goal:** Promote SASL/OAUTHBEARER from the dev-only *unsecured JWS* validator
(slice 49) to production-grade **signed-JWT validation against a JWKS
endpoint**. A client presents a real OAuth 2.0 access token (a JWS signed by
the identity provider); the broker verifies the signature using the IdP's
published JSON Web Key Set, checks the standard JWT claims (`exp`, `iat`,
`nbf`, `iss`, `aud`), and derives the connection principal from a configured
claim. This is the second half of the slice-49 core work and unblocks the
production path for operator slice 50 (`KafkaUser` OAuth + listener OAuth
config).

Slice 49 left the RFC 7628 wire plumbing (handshake, client-initial-response
parse, single-round success / two-round failure handshake, principal
extraction, authzid match) mechanism-agnostic. This slice swaps the *validator*
behind that plumbing; the wire state machine is reused unchanged.

---

## 1. Scope

### In

- **Signature verification (`crates/security`, pure logic, no I/O):**
  - `RS256` (RSASSA-PKCS1-v1_5 + SHA-256) and `ES256` (ECDSA P-256 + SHA-256)
    via `ring` (already a dependency; the codebase hand-rolls crypto on `ring`
    rather than pulling a JWT crate).
  - `Jwks` — an RFC 7517 key set parsed from JSON. Holds RSA (`kty:RSA`, `n`,
    `e`) and EC P-256 (`kty:EC`, `crv:P-256`, `x`, `y`) public keys, indexed by
    `kid`. Verification selects the key by the token header `kid` (or the sole
    key when the header omits `kid`).
  - `JwksHandle` — a cheaply-clonable, atomically-swappable holder
    (`Arc<ArcSwap<Jwks>>`, mirroring slice 33's `DynamicServerConfig`) so the
    broker's background refresher can rotate keys with no lock and no restart.
- **`SignedJwsValidator` (`crates/security`):** config (principal claim, scope
  claim, required scope, clock skew, expected issuer, expected audience) + a
  `JwksHandle`. `validate(token, now_ms)`:
  1. Split 3 JWS segments; signature segment must be **non-empty**.
  2. Header `alg` ∈ {`RS256`, `ES256`}; read optional `kid`.
  3. Verify the signature over `header_b64 "." payload_b64` against the current
     key set.
  4. `exp` required (reject if `exp + skew <= now`); `iat`/`nbf` optional
     (reject future `iat`/`nbf` beyond skew).
  5. `iss` must equal the configured issuer (when configured).
  6. `aud` (string or array) must contain the configured audience (when
     configured).
  7. Optional required scope (string or array), same rule as the unsecured
     validator.
  8. Principal = the configured claim (default `sub`), a non-empty string.
- **`OAuthBearerValidator` enum** = `Unsecured(UnsecuredJwsValidator)` |
  `Signed(SignedJwsValidator)`. The broker holds one of these; the
  `SaslAuthenticate` handler is rewritten against the enum. `Default` =
  `Unsecured` (slice-49 behavior preserved).
- **Broker JWKS refresher (`crates/broker/src/oauth_jwks.rs`, I/O):** a
  background task that GETs the JWKS endpoint (`reqwest`, rustls TLS), parses
  it, and `store`s it into the shared `JwksHandle` on a configurable interval.
  First fetch fires immediately on spawn (a `tokio::interval` ticks at t=0);
  fetch failures log a warning and the task keeps retrying — a transient IdP
  outage does not crash the broker.
- **Config:** `BrokerConfig.oauthbearer_validator` becomes
  `OAuthBearerValidator`; new `oauthbearer_jwks_endpoint: Option<String>` and
  `oauthbearer_jwks_refresh_interval: Duration` (default 5 min). The
  `[oauthbearer]` TOML section gains `jwks_endpoint_uri`, `valid_issuer_uri`,
  `expected_audience`, `jwks_refresh_interval_ms`. Setting `jwks_endpoint_uri`
  selects the `Signed` validator; otherwise the unsecured validator is built as
  before. `Broker::start` spawns the refresher iff a JWKS endpoint is set.

### Out (deferred)

| Concern | Slice |
|---|---|
| KIP-368 re-authentication / `session_lifetime_ms` connection expiry | 49c |
| Opaque-token validation via an introspection endpoint (RFC 7662) | future |
| Custom CA trust / mTLS to the JWKS endpoint (`tlsTrustedCertificates`) | future (webpki/Mozilla roots only this slice) |
| OAUTHBEARER on inter-broker / controller listeners | ✅ shipped later; outbound token files are re-read per connection and controller listeners use the broker OAuth validator |
| `KafkaUser` OAuth + `Kafka.spec` listener OAuth config | 50 (operator) |
| Additional algs (RS384/512, ES384/512, PS256) | future (RS256 + ES256 cover the overwhelming majority of IdPs) |

---

## 2. Why `ring`, not a JWT crate

The codebase hand-rolls SCRAM, PLAIN, mTLS, and the unsecured-JWS path on
`ring` 0.17 + `base64` + `serde_json`, all already dependencies of
`crates/security`. Signed-JWS verification needs only:

- RS256: `ring::signature::RsaPublicKeyComponents { n, e }.verify(
  &RSA_PKCS1_2048_8192_SHA256, signing_input, sig)` — feed the base64url-decoded
  modulus/exponent straight from the JWK.
- ES256: build the SEC1 uncompressed point `0x04 ‖ x ‖ y` (each coord
  left-padded to 32 bytes) and
  `UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, point).verify(...)` — the
  JWS ES256 signature is the fixed 64-byte `r ‖ s` that `_FIXED` expects.

No new crate, no second crypto backend, and `now_ms` stays injectable for
deterministic temporal tests (a JWT crate's built-in `exp` check reads the
system clock and can't be injected — it would break the established test
seam).

---

## 3. Pure-logic / I/O split

`crates/security` stays I/O-free: it parses a JWKS *string* and verifies a
token against an in-memory `Jwks`. All network fetching lives in
`crates/broker/src/oauth_jwks.rs`. The `JwksHandle` is the seam — the
refresher `store`s, the validator `load`s. This keeps the entire validation
path unit-testable without a socket (build a `Jwks` in-memory, mint a token,
validate) and confines the only network dependency to one thin broker module.

---

## 4. Testing

- **`crates/security` unit (`jwks.rs`):** parse a mixed RSA+EC key set; reject
  malformed JSON / unsupported `kty`. Verify a valid RS256 token (minted at
  runtime from a static embedded RSA-2048 PKCS#8 key) and a valid ES256 token
  (key generated at runtime via `ring`). Reject: tampered signature, unknown
  `kid`, ambiguous missing `kid` with >1 key, EC signed by the wrong key.
  *(The RSA modulus/exponent for the test JWK are split from the key pair's
  PKCS#1 public DER by a ~20-line test-only helper — production code never
  touches private keys; it reads `n`/`e` from the IdP's JWKS JSON.)*
- **`crates/security` unit (`oauthbearer.rs`):** `SignedJwsValidator` accepts a
  fresh signed token; rejects expired / future-`nbf` / `alg:none` / wrong
  issuer / wrong audience / missing principal; honors string + array `aud` and
  scope; **key rotation** — a token verifies, the handle is `store`d with a new
  key set, the same token now fails and a token under the new key passes. The
  claim-policy checks are exercised directly with constructed claims (no
  signature) for exhaustive, fast coverage.
- **Broker wire integration (`crates/broker/tests/auth_handlers.rs`):** full
  `ApiVersions` → `SaslHandshake(OAUTHBEARER)` → `SaslAuthenticate` → `Metadata`
  happy path against a `Signed` validator whose `JwksHandle` is pre-populated
  in-memory (no network), and a wrong-issuer two-round `invalid_token` → 58
  failure handshake.
- **Broker refresher integration (`oauth_jwks.rs` test):** serve a JWKS over a
  local `axum` HTTP server, point a `JwksRefresher` at it, assert the handle is
  populated and a token signed by the served key validates; assert a fetch
  against a dead endpoint leaves the handle empty without panicking.

No JVM acceptance test: the JVM `OAuthBearerUnsecuredLoginCallbackHandler`
mints only unsecured tokens, and a signed-token JVM test would require standing
up a real OAuth server in CI. The slice-49 unsecured JVM acceptance test still
covers the wire handshake end to end; this slice's signature path is covered by
the Rust integration tests above.

---

## 5. File structure

```
crates/security/
├── Cargo.toml                 # unchanged (ring/base64/serde_json/arc-swap present)
└── src/
    ├── lib.rs                 # MODIFIED — mod jwks; export Jwks/JwksHandle/
    │                          #            SignedJwsValidator/OAuthBearerValidator
    ├── jwks.rs                # NEW — Jwks parse + RS256/ES256 verify + JwksHandle
    └── oauthbearer.rs         # MODIFIED — SignedJwsValidator + OAuthBearerValidator
crates/broker/
├── Cargo.toml                 # MODIFIED — add reqwest (rustls-tls)
└── src/
    ├── lib.rs                 # MODIFIED — mod oauth_jwks
    ├── oauth_jwks.rs          # NEW — JwksRefresher fetch loop + fetch_jwks
    ├── config.rs              # MODIFIED — validator enum + jwks endpoint/interval
    ├── file_config.rs         # MODIFIED — [oauthbearer] jwks fields → Signed
    ├── broker.rs              # MODIFIED — spawn refresher when JWKS configured
    └── network/
        ├── auth.rs            # MODIFIED — handler takes &OAuthBearerValidator
        └── dispatch.rs        # unchanged call site (type change only)
crates/broker/tests/
└── auth_handlers.rs           # MODIFIED — signed-token wire cases
```

No protocol regeneration: `SaslHandshake` / `SaslAuthenticate` already cover
the wire envelope, and KIP-368 `session_lifetime_ms` stays 0 (49c).
