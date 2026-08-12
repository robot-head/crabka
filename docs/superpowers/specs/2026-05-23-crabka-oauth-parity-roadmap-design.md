# Crabka OAUTHBEARER full-parity roadmap

Status: Draft (umbrella)
Date: 2026-05-23
Supersedes: nothing
Superseded by: nothing

## Why this exists

Slices 49 and 49b shipped the foundation of SASL/OAUTHBEARER on the broker:
RFC 7628 handshake, unsecured JWS validator, and JWKS / signed-JWT
validation. The operator-side roadmap (`docs/superpowers/specs/2026-05-15-crabka-operator-roadmap-design.md`)
lists exactly one follow-up operator slice — **slice 50: `KafkaUser` OAuth +
listener OAuth config** — paired with the existing core work.

Apache Kafka's OAUTHBEARER surface and Strimzi's
`KafkaListenerAuthenticationOAuth` are much wider than what 49b implements.
A single "slice 50" cannot ship full parity; the field set in
`KafkaListenerAuthenticationOAuth` alone covers six distinct broker
subsystems that 49b explicitly deferred. This document is the umbrella
that defines those subsystems, breaks them into individually-deliverable
sub-slices, and pairs each broker slice with its operator slice — the same
rhythm already used for slices 22/21, 29/30, 33/34, 45/46.

Each sub-slice ships as its own PR, with its own design document (linked
below as filenames that don't yet exist) and its own implementation plan.
This document is the index — it doesn't itself contain the per-slice
designs.

## Subsystem inventory

Strimzi's `KafkaListenerAuthenticationOAuth` (≈28 fields) maps onto six
subsystems on the broker side:

| Subsystem | Strimzi fields | What the broker has to add | Status |
|-----------|----------------|----------------------------|--------|
| **A. Signed JWT (JWKS)** | `validIssuerUri`, `validAudience` / `clientAudience`, `jwksEndpointUri`, `jwksRefreshSeconds`, `userNameClaim`, `customClaimCheck` (scope), `enableOauthBearer`, `maxSecondsWithoutReauthentication` (no — that's KIP-368) | RS256/ES256 verify, JWKS refresher, claim mapper | ✅ shipped in 49b |
| **B. Custom TLS trust to IdP** | `tlsTrustedCertificates` | rustls config that augments webpki roots with a user-supplied CA bundle for outbound HTTPS to JWKS / introspection / userinfo endpoints | ❌ deferred from 49b |
| **C. Opaque-token introspection (RFC 7662)** | `introspectionEndpointUri`, `userInfoEndpointUri`, `clientId`, `clientSecret`, `accessTokenIsJwt`, `checkAccessTokenType` | Outbound HTTP POST to introspection endpoint with client-credentials basic auth; userinfo follow-up; per-call validation; `accessTokenIsJwt=false` selects this path | ❌ deferred from 49b |
| **D. KIP-368 re-authentication** | `maxSecondsWithoutReauthentication` | Per-connection session expiry; `SaslAuthenticate` returns `session_lifetime_ms`; connection closes when the timer fires unless the client re-handshakes | ❌ deferred from 49b |
| **E. PLAIN-with-OAuth-token** | `enablePlain`, `tokenEndpointUri` | PLAIN authenticator path that validates the password as a bearer token (call token endpoint with client-credentials to mint one, then verify) — for clients that can't speak OAUTHBEARER. | ❌ not in any roadmap slice yet |
| **F. Claim enrichments** | `groupsClaim`, `groupsClaimDelimiter`, `fallbackUserNameClaim`, `fallbackUserNamePrefix`, `validTokenType`, `customClaimCheck` (JsonPath), `jwksMinRefreshPauseSeconds`, `jwksExpirySeconds`, `jwksIgnoreKeyUse` | Groups extractor that surfaces into a future principal-builder, fallback claim chain, `typ` header validation, expression-language claim check, JWKS pause/expiry policies, `use=sig` filter toggle | ❌ deferred from 49b |

Two Strimzi fields are out of scope at the broker level entirely and
will not have parity slices: `enableMetrics`
(redundant — Crabka's metrics exporter covers SASL counters generically),
and the deprecated dual `clientAudience`/`validAudience` pair (Crabka exposes
just `validAudience`).

## Sub-slice plan

Pair each broker sub-slice with its operator counterpart immediately
following — same rhythm as 22/21, 29/30, 33/34, 45/46. This gets a
working feature into operator hands every two slices instead of forcing
users to wait for a long broker phase.

| Slice | Layer | Subsystem | Title | Notes |
|------:|-------|-----------|-------|-------|
| 49    | broker | A wire | OAUTHBEARER (KIP-255 / RFC 7628) | ✅ shipped |
| 49b   | broker | A validator | JWKS / signed-JWT validation | ✅ shipped |
| **50** | **operator** | **A surface** | **Listener OAuth + `KafkaUser` tls-external** | **First PR of this umbrella; this slice's own design + plan document the operator surface.** |
| 49c   | broker | B | Custom TLS trust to IdP | Reusable for any future outbound HTTPS in the broker (e.g. introspection in 49d). Small slice — new `[oauthbearer].jwks_tls_trust` config + a rustls `ClientConfig` builder that uses the caller-supplied PEM bundle as the *exclusive* trust store (Strimzi-parity replace semantic, not additive). When unset, refresher keeps the slice-49b webpki-roots default. |
| 50b   | operator | B | Listener `tlsTrustedCertificates` | Surface 49c. CRD field is a list of `{secretName, certificate}` (Strimzi shape). Reconciler mounts the Secret into the broker pod and writes the file path into the broker TOML. |
| 49d   | broker | C | Opaque-token introspection | RFC 7662 introspection client (HTTP POST with Basic Auth client credentials), principal/claim derivation from the introspection JSON, OIDC userinfo enrichment when `userinfo_endpoint_uri` is configured. Reuses 49c for the HTTPS trust config — and renames `jwks_tls_trust` → `idp_tls_trust` (shared across JWKS/introspection/userinfo, one trust bundle per IdP). Validator's `validate` becomes `async fn` to accommodate the per-token HTTP round trip; existing sync paths wrap in `async {}`. |
| 50c   | operator | C | Introspection CRD fields | `introspectionEndpointUri`, `userInfoEndpointUri`, `clientId`, `clientSecret` (Secret ref), `accessTokenIsJwt`, `checkAccessTokenType`. |
| 49e   | broker | D | KIP-368 re-authentication | Per-connection `session_lifetime_ms`; new field on `SaslAuthenticateResponse v1+`; connection-shutdown timer. Touches the auth state machine — non-trivial. |
| 50d   | operator | D | `maxSecondsWithoutReauthentication` | One CRD field on the listener OAuth config; threads down to broker TOML. |
| 49f   | broker | E | PLAIN-with-OAuth-token | New PLAIN authenticator path: treat the password as either a literal bearer token or as client-credentials material for the token endpoint. Optional slice — only if user demand emerges (gates clients that can't do OAUTHBEARER). |
| 50e   | operator | E | `enablePlain` + `tokenEndpointUri` | Two CRD fields; enabling them flips PLAIN on for that listener. |
| 49g   | broker | F | Claim enrichments + JWKS policy | Groups claim extractor, fallback claim chain, `typ` validation, JsonPath-ish `customClaimCheck` evaluator (small expr lang — needs its own micro-design), `jwksMinRefreshPauseSeconds` + `jwksExpirySeconds` policy in the refresher, `jwksIgnoreKeyUse` toggle. Largest of the six; may itself split into 49g + 49h at plan time. |
| 50f   | operator | F | Remaining Strimzi fields | `groupsClaim`, `groupsClaimDelimiter`, `fallbackUserNameClaim`, `fallbackUserNamePrefix`, `validTokenType`, `customClaimCheck` (full Strimzi shape), `jwksMinRefreshPauseSeconds`, `jwksExpirySeconds`, `jwksIgnoreKeyUse`. |

**Eleven follow-up slices** (six broker + five operator) over the existing
49/49b. After 50f lands, `KafkaListenerAuthenticationOAuth` is at Strimzi
field parity, and the operator's OAuth surface is complete.

## What is explicitly not in this roadmap

- **`KafkaUser.authentication: oauth` variant.** Strimzi's actual enum is
  `tls` / `tls-external` / `scram-sha-512`; OAuth users are represented as
  `tls-external` (ACLs + quotas bound to `User:<metadata.name>`, no
  credentials generated). Slice 50 adds `tls-external` to Crabka's
  `KafkaUser` enum and that's the OAuth user model going forward. No
  future slice will introduce a separate `oauth` user type.
- **Inter-broker OAuth.** `inter_broker_credentials.type = "oauth-bearer"`
  points at a bearer-token file. The outbound client re-reads it for each new
  inter-broker or controller-listener connection, and the controller listener
  validates it through the same broker-global OAuth validator as data-plane
  clients.
- **Delegation tokens.** Slice 51 in the operator roadmap. Independent of
  this umbrella; runs on its own track.
- **GSSAPI / Kerberos.** Slice 52, optional, only-if-user-demand. Outside
  this umbrella.
- **OPA / Keycloak authorizer plugins.** Slices 53 / 54 in the operator
  roadmap. They consume the OAuth identity but are not OAuth themselves.
- **Per-listener `[oauthbearer]` broker config.** Slice 49b made
  `[oauthbearer]` broker-global on the rationale that a single broker
  normally fronts one identity provider. If real users land with two
  listeners pointed at two IdPs, that becomes its own broker slice
  (likely 49h); the operator slice 50 design covers this case by
  rejecting it at reconcile with `Ready=False
  reason=ConflictingOAuthConfig` until then.

## Sequencing notes

- 50 unlocks production OAuth users on a single listener. Most clusters
  with one IdP need nothing else.
- 50b is the highest-priority follow-up: real IdPs run on certs from
  private CAs more often than not, and 49b's webpki-only trust store will
  fail those out of the box.
- 50c (introspection) is the second-most-requested feature — opaque
  tokens are common in non-OIDC OAuth deployments.
- 50d (KIP-368 re-auth) is a hardening slice: without it, a compromised
  token grants access for its full lifetime regardless of operator
  policy. Important but not a blocker for a working OAuth deployment.
- 50e is optional and only worth shipping if a real user reports a
  client they can't migrate to OAUTHBEARER.
- 50f is the long tail — useful for Strimzi-migration parity, but
  individually low-value.

A natural batching that keeps each PR small is: ship 50 alone, ship
49c+50b as a paired pair, then 49d+50c, then 49e+50d, then (if needed)
49f+50e, finally 49g+50f. No bundled mega-slices.

## What this document does not commit to

This is the **plan for the plan** — it sets up the sub-slice boundaries
and pairs them with operator follow-ups. Each sub-slice (49c, 50b, 49d,
…) gets its own per-slice design document at the time it's planned, with
all the wire-level, file-level, and test-level detail. The contents of
those future designs are not legislated here — only the scope envelope
and the pairing.
