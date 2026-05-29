# Crabka — SASL/GSSAPI (Kerberos) Design

**Date:** 2026-05-28
**Status:** Approved design, pre-implementation-plan

## Goal

Add SASL/GSSAPI (Kerberos) authentication to Crabka with **functional parity to Apache Kafka**: real Kafka clients authenticate against a real KDC using a service keytab, and Crabka brokers authenticate to each other over GSSAPI. Parity is proven by interoperating with stock cp-kafka tooling against a containerized KDC.

This covers both directions:
- **Accept** — external Kafka clients authenticate to the broker (server-side `accept_security_context`).
- **Initiate** — the broker authenticates to peer brokers for inter-broker traffic (client-side `initialize_security_context` using its own keytab).

## Scope decisions

| Decision | Choice | Rationale |
|---|---|---|
| Fidelity | Kafka parity, verified against a containerized KDC + cp-kafka tooling | North star is interop, not just protocol negotiation. |
| Kerberos library | `sspi-rs` (pure Rust, no system krb5) | No FFI / system-package dependency. Server-accept + keytab maturity is the key risk — gated by a spike. |
| Security layer (QOP) | **Auth-only** | Matches stock Kafka exactly: Kafka's `SaslServerAuthenticator` negotiates `auth` only and delegates encryption to the SASL_SSL listener. Full integrity/confidentiality would *exceed* Kafka and risk diverging from stock clients that offer only `auth`. No data-plane GSS wrapping. |
| Principal mapping | Full `auth_to_local` rule DSL | Matches Kafka's `sasl.kerberos.principal.to.local.rules` / `KerberosName`; required for enterprise configs. |
| Inter-broker | Both accept + initiate | Supports `sasl.mechanism.inter.broker.protocol=GSSAPI`. |

## Existing architecture (integration points)

Crabka's SASL is **enum-dispatch**, not trait-based:

- `SaslMechanism` enum (`crates/security/src/mechanism.rs`) — PLAIN, SCRAM-SHA-256/512, OAUTHBEARER today.
- Per-connection `ConnectionAuth` state (`crates/broker/src/network/auth.rs`) with a `SaslExchange` enum holding per-mechanism state machines. SCRAM (`ScramServerExchange::step()`) is the closest precedent for a multi-round mechanism.
- Server frame loop: `Framed<S, LengthDelimitedCodec>` in `crates/broker/src/network/dispatch.rs:175`; SASL frames handled by `try_handle_sasl_frame` before normal request dispatch.
- `Principal { name, auth_method, groups }` (`crates/security/src/principal.rs`); every mechanism maps to ACL principal `User:<name>`.
- Outbound SASL **client** already exists: `run_outbound_sasl` in `crates/broker/src/network/client.rs:161` does PLAIN/SCRAM over manual SaslHandshake/SaslAuthenticate framing. This is where the initiate path slots in.
- TLS via `rustls` / `tokio-rustls`; listeners support `SASL_SSL`.
- Config: `BrokerConfig` (`crates/broker/src/config.rs`) carries `enabled_sasl_mechanisms`, per-listener `sasl_mechanisms`, `oauthbearer_validator`, `inter_broker_credentials`.

No Kerberos crate is present today — `sspi-rs` is a new dependency.

## Approach

**Thin GSS boundary, spike-first.** Introduce a minimal internal abstraction in `crates/security/src/gssapi/` — a `GssAcceptor` (server) / `GssInitiator` (client) pair — backed by `sspi-rs`. The rest of the integration (state machines, config, `auth_to_local`, dispatch wiring, tests) sits on top of this boundary so that the one real risk — sspi-rs's server-side capability — is contained at a single seam.

Rejected alternatives:
- **Direct sspi-rs calls inline** — couples `auth.rs`/`client.rs` to sspi-rs quirks; hard to absorb spike findings or pivot.
- **Roll our own krb5 on sspi-rs crypto primitives** — effectively reimplementing a Kerberos stack; unjustified.

## Components

### Module layout

New `crates/security/src/gssapi/`, mirroring the SCRAM module:

- `mod.rs` — `SaslMechanism::Gssapi` wiring, `GssapiConfig`, the `GssAcceptor`/`GssInitiator` trait boundary, and the sspi-rs-backed implementations.
- `server.rs` — `GssapiServerExchange` accept state machine (parallel to `ScramServerExchange`).
- `client.rs` — `GssapiClientExchange` initiate state machine (for inter-broker).
- `name.rs` — `auth_to_local` rule DSL: parser + applier (Kafka `KerberosName` equivalent).
- `keytab.rs` — MIT keytab reader. **Only built if the spike shows sspi-rs cannot ingest a keytab directly**; otherwise omitted.

### Enum / principal wiring

- `SaslMechanism::Gssapi`, wire name `"GSSAPI"` (`mechanism.rs`).
- `AuthMethod::SaslGssapi` (`principal.rs`). Authenticated principal name = `auth_to_local(client_krb_principal)`; ACL principal `User:<shortname>`.
- `SaslExchange::Gssapi(Box<GssapiServerExchange>)` (`auth.rs`), plus `handle_authenticate_gssapi` and a dispatch arm in `dispatch.rs` (the mechanism match near line 1486).
- `enabled_sasl_mechanisms` / per-listener `sasl_mechanisms` advertise `GSSAPI` once the enum grows — no extra advertisement plumbing.

### The GSS boundary

```rust
// crates/security/src/gssapi/mod.rs (sketch)

/// Server side: drive GSS context establishment from client tokens.
trait GssAcceptor {
    /// Feed a client token; return either a token to send back (context still
    /// in progress) or completion carrying the established context handle.
    fn accept(&mut self, client_token: &[u8]) -> Result<AcceptStep, GssError>;
    /// After establishment: GSS-wrap / GSS-unwrap the RFC 4752 layer-negotiation
    /// messages (auth-only, conf_req=false).
    fn wrap(&self, plaintext: &[u8]) -> Result<Vec<u8>, GssError>;
    fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, GssError>;
    /// The authenticated source principal, e.g. "alice@REALM" or "alice/host@REALM".
    fn src_principal(&self) -> Result<String, GssError>;
}

enum AcceptStep {
    Continue(Vec<u8>),       // send this token, expect another client token
    Established(Vec<u8>),    // optional final token (e.g. AP-REP), context ready
}

/// Client side: produce tokens to send, consume server tokens.
trait GssInitiator {
    fn step(&mut self, server_token: Option<&[u8]>) -> Result<InitStep, GssError>;
    fn wrap(&self, plaintext: &[u8]) -> Result<Vec<u8>, GssError>;
    fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, GssError>;
}
```

The sspi-rs-backed impls construct a `sspi::Kerberos` package, load the broker's service credentials from the keytab, and call `accept_security_context` / `initialize_security_context`.

## Data flow

### Server accept (state machine)

Carried over repeated `SaslAuthenticate` request/response pairs (token in `auth_bytes`, token out in `auth_bytes`):

1. **`AcceptingContext`** — feed each client token to `GssAcceptor::accept`; return its output token; loop until `Established`.
2. **`OfferingSecurityLayer`** — context established. GSS-wrap the RFC 4752 offer: 1-byte security-layer bitmask `0x01` (auth-only) + 3-byte max receive size; send as `auth_bytes`.
3. **`AwaitingLayerChoice`** — client returns a GSS-wrapped message (1-byte selected layer + 3-byte max size + optional authzid). Unwrap; assert the selected layer is `auth`; read the source principal via `src_principal()`.
4. **`Complete`** — apply `auth_to_local` → `Principal`; transition `ConnectionAuth::Authenticated { mechanism: Gssapi, .. }`.

Because QOP is auth-only, there is **no data-plane wrapping** after step 4 — the existing `Framed<S, LengthDelimitedCodec>` loop handles plain Kafka frames unchanged.

**Validation item (not guesswork):** the exact mapping of GSS tokens to `SaslAuthenticate` round trips — how many exchanges, and whether a final context token (AP-REP) shares a response with the security-layer offer — will be pinned **empirically against cp-kafka and a real GSSAPI client**, per the project's "match Kafka empirically when undocumented" rule. The state machine is structured to drive the GSS loop generically so the precise choreography is a wiring detail, not a redesign.

### Inter-broker initiate

Extend `run_outbound_sasl` (`client.rs`). `InterBrokerCredentials` becomes an enum (greenfield — no compat shim):

```rust
enum InterBrokerCredentials {
    Plain { username: String, password: String },
    Scram { mechanism: SaslMechanism, username: String, password: String },
    Gssapi { keytab_path: PathBuf, client_principal: String, service_name: String },
}
```

The new `Gssapi` arm runs `GssapiClientExchange` over the existing manual SaslHandshake/SaslAuthenticate framing: `initialize_security_context` loop (AS-REQ/TGS-REQ to the KDC via sspi-rs, producing the AP-REQ), then the security-layer reply. Returns the authenticated stream for normal Kafka RPCs.

### Configuration

Add `gssapi: Option<GssapiConfig>` to `BrokerConfig` (parallel to `oauthbearer_validator`):

```rust
struct GssapiConfig {
    keytab_path: PathBuf,                 // broker service keytab
    service_name: String,                 // Kafka sasl.kerberos.service.name, default "kafka"
    principal_to_local_rules: Vec<Rule>,  // parsed auth_to_local DSL
    realm: Option<String>,
    kdc: Option<String>,                  // for the initiate path; else discovered via krb5.conf
}
```

### auth_to_local DSL (`name.rs`)

Implements Kafka's rule grammar:
- `RULE:[n:string](regex)s/pattern/replacement/[g]` chains
- `/L` lowercasing modifier
- `DEFAULT` (first component, realm stripped)

Applied left-to-right; first matching rule wins. Default behavior with no rules: strip realm, take the first principal component (`alice/host@REALM` → `alice`).

## Error handling

- Unknown/disabled mechanism, or `GSSAPI` requested on a listener that doesn't advertise it → existing handshake rejection path (`UNSUPPORTED_SASL_MECHANISM`).
- GSS context failure (bad ticket, clock skew, wrong service principal, decrypt failure) → `SASL_AUTHENTICATION_FAILED` with the GSS minor status logged; connection closed, matching Kafka.
- Client selects a security layer other than `auth` → reject (we only offer auth).
- No matching `auth_to_local` rule → authentication failure (matches Kafka's `NoMatchingRule`).
- Missing/unreadable keytab or initiate-path KDC unreachable → startup/connect error surfaced clearly; inter-broker connect fails closed.

## Testing

- **Unit:** `auth_to_local` rule parsing + application using Kafka's own `KerberosName` test vectors; security-layer offer/choice byte encoding; keytab parsing (if `keytab.rs` is built).
- **Integration (parity proof):** Dockerized MIT KDC + Crabka broker; a stock GSSAPI client (cp-kafka `kafka-console-producer`/`consumer` configured for GSSAPI) authenticates and produces/consumes end-to-end. Plus a two-Crabka-broker test exercising inter-broker initiate ↔ accept.
- **Cross-check:** observe a stock cp-kafka broker's GSSAPI exchange to confirm byte/choreography exactness where the wire behavior is otherwise undocumented.

## Risks

1. **sspi-rs server-side capability (highest).** Server `accept_security_context` + keytab ingestion is undocumented for sspi-rs. **Step 1 of implementation is a focused spike** proving: (a) load the service key from a keytab, (b) accept an AP-REQ and extract the client principal, (c) initiate as a client using a keytab. If a capability is missing, fall back to a local MIT-keytab parser feeding sspi-rs (`keytab.rs`), or revisit the library — contained behind the `GssAcceptor`/`GssInitiator` boundary, no churn elsewhere.
2. **Exact SaslAuthenticate choreography** — resolved empirically against cp-kafka (see Data flow validation item).

## Implementation order (for the plan)

1. **Spike** — sspi-rs accept/initiate + keytab, behind throwaway test harness. Gate.
2. GSS boundary + sspi-rs impls; `mechanism.rs`/`principal.rs` enum additions; config struct.
3. `auth_to_local` DSL (`name.rs`) + unit tests.
4. Server accept state machine + `auth.rs`/`dispatch.rs` wiring.
5. Inter-broker initiate (`InterBrokerCredentials` enum + `run_outbound_sasl` arm).
6. Integration tests (Dockerized KDC, cp-kafka client, two-broker inter-broker), cp-kafka cross-check.
