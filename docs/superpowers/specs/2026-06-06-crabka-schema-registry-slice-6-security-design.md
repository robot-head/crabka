# Crabka Schema Registry — Slice 6 (Security) Design

**Status:** approved (brainstorm) — ready for implementation plan.
**Stacks on:** slice 5 (HA, PR #414). Branch `claude/schema-registry-slice-6`.

## Goal

Add security to the standalone `crabka-schema-registry` REST service in one slice: **authentication** (HTTP Basic + Bearer/OAuth + mTLS), **authorization** (per-subject Kafka Topic ACLs), **server-side TLS** (HTTPS), and **SR↔broker client security** (SASL/TLS). Reuse Crabka's existing security crates wholesale, mirroring the grpc-gateway's P5 pattern — only HTTP Basic is new code.

## Non-negotiables

- **Reuse, don't reinvent.** The grpc-gateway (P5, `crates/grpc-gateway/src/authz/`, `serve.rs`) is the reference. Reused: `crabka-authz` (authorizer + ACL cache), `crabka-security` (`Principal`, `OAuthBearerValidator`, `TlsConfig`, `extract_principal_from_cert`), `crabka-metadata` (`ResourceType`/`AclOperation`), `client-core` (`ClientSecurity`/`SaslCredentials`).
- **cp-fidelity** is limited to **HTTP Basic** behavior (OSS cp-schema-registry security = HTTPS + `BASIC` + Kafka-client security; fine-grained authz is Confluent-commercial, no OSS oracle). Per-subject authz is Crabka-specific, validated by our own broker-backed tests.
- **Greenfield / Kafka-byte-exactness** per CLAUDE.md. No back-compat shims.
- **Backwards-compatible default:** with no security configured, the service behaves exactly as today (open, anonymous, HTTP) — security is opt-in via config.

## Reused APIs (grounded)

- `crabka_authz::{Authorizer, SimpleAclAuthorizer, AclSource, AuthorizationRequest<'a>, AuthorizationResult}`; `crabka_authz::cache::AclCache` (snapshot from broker `DescribeAcls`, implements `AclSource`).
- `crabka_security::{Principal, AuthMethod, TlsConfig, ClientAuthMode, extract_principal_from_cert, OAuthBearerValidator (Unsecured|Signed|Introspection), AuthOutcome}`.
- `crabka_metadata::{ResourceType (Topic|Group|Cluster|TransactionalId), AclOperation (Read|Write|Create|Delete|Alter|Describe|…), PatternType, PermissionType}`.
- `crabka_client_core::{ClientSecurity, SaslCredentials, TlsConnectorConfig}`; `crabka_security::ListenerProtocol`.
- Gateway templates: `crates/grpc-gateway/src/authz/auth_layer.rs` (Bearer middleware → `Principal` in extensions), `serve.rs` (TLS accept loop + mTLS principal), `authz/mod.rs` (AclCache refresh task + `authorize` gating).

## Architecture — middleware stack

The router is wrapped by three layers; **execution order** is auth → authz → forward → handler (achieved by `router(state).layer(forward).layer(authz).layer(auth)` — axum runs the last-added layer first):

```
request → [auth_layer]  resolve Principal (mTLS|Bearer|Basic|Anonymous) → extensions
        → [authz_layer] map (method,path)→(ResourceType,name,AclOperation); authorize; 403 on Deny
                        (SKIP if request is a trusted forward: X-Forwarded-For-Registry present)
        → [forward_layer] (slice 5) read=local, mutating-on-secondary=proxy to primary
        → handler
```

### Unit 1 — Authentication (`src/auth/`, new module)

`auth_layer` (axum `from_fn_with_state`) resolves a `crabka_security::Principal` and inserts it into request extensions, in precedence order:

1. **mTLS** — if the TLS accept loop (Unit 3) already inserted an mTLS `Principal` into extensions (from the verified peer cert), use it as the highest-precedence source.
2. **Bearer** — `Authorization: Bearer <jwt>` → reused `OAuthBearerValidator::validate` → `Principal` (auth_method `SaslOAuthBearer`). Invalid token → `401`.
3. **Basic** — `Authorization: Basic <base64(user:pass)>` → **new** `BasicAuthStore::verify(user, pass)` → `Principal { name: user, auth_method: SaslPlain }`. Bad credentials → `401`.
4. **Anonymous** — none of the above → `Principal { name: "ANONYMOUS", auth_method: Anonymous }`.

`401` responses carry `WWW-Authenticate: Basic realm="<realm>"` (cp-exact) when Basic is enabled. `require_auth = true` turns a resolved Anonymous principal into a `401` (deny unauthenticated); `require_auth = false` lets Anonymous through to authz. A present-but-invalid credential is **always** `401` regardless of `require_auth`.

**`BasicAuthStore`** (the only new auth primitive): an in-memory `HashMap<String, CredHash>` loaded from config — either an inline `user:bcrypt` map or an htpasswd-style file path. Verify with a constant-time compare. The default credential format is **plaintext** (cp's `PropertyFileLoginModule` stores `user=password,role` plaintext — this is the cp-parity path); a configured value shaped like a bcrypt hash (`$2…`) is instead bcrypt-verified (Crabka enhancement, `bcrypt` crate). Pure + unit-tested.

### Unit 2 — Authorization (`src/authz.rs`, new)

`SchemaRegistryAuthz { authorizer: Arc<dyn Authorizer>, acls: watch::Receiver<Arc<AclCache>>, super_users: HashSet<String>, enabled: bool }`.

- A background task (gateway pattern) periodically `DescribeAcls` via `crabka-client-admin` and publishes a fresh `AclCache` over a `watch` channel.
- `authz_layer` maps the request to a permission and calls `authorizer.authorize(&*acls, &AuthorizationRequest { principal, host, resource_type, resource_name, operation })`; `Deny` → `403`. Super-users and `enabled = false` short-circuit to allow.
- **Forward-trust:** if the request carries `forward::FORWARD_HEADER` (a write already authorized at its ingress node), `authz_layer` **skips** authorization — the primary trusts inter-node forwards (documented trust boundary; secure the inter-node link in deployment).

**(method, path) → (ResourceType, name, AclOperation) map** (the pure core, unit-tested):

| Route | Resource | Op |
|---|---|---|
| `GET /` | — (health, unauthenticated) | — |
| `GET /schemas/types` | Cluster `kafka-cluster` | Describe |
| `GET /schemas`, `/schemas/ids/{id}`, `/schemas/ids/{id}/versions` | Cluster `kafka-cluster` | Read |
| `GET /subjects` | Cluster `kafka-cluster` | Describe |
| `POST /subjects/{s}` (lookup) | Topic `{s}` | Read |
| `DELETE /subjects/{s}` | Topic `{s}` | Delete |
| `GET /subjects/{s}/versions` | Topic `{s}` | Read |
| `POST /subjects/{s}/versions` (register) | Topic `{s}` | Write |
| `GET /subjects/{s}/versions/{v}` (+ `/schema`, `/referencedby`) | Topic `{s}` | Read |
| `DELETE /subjects/{s}/versions/{v}` | Topic `{s}` | Delete |
| `GET /config`, `/mode` | Cluster `kafka-cluster` | Describe |
| `PUT /config`, `/mode` | Cluster `kafka-cluster` | Alter |
| `GET /config/{s}`, `/mode/{s}` | Topic `{s}` | Describe |
| `PUT /config/{s}`, `/mode/{s}`, `DELETE /mode/{s}` | Topic `{s}` | Alter |
| `POST /compatibility/subjects/{s}/versions/{v}` | Topic `{s}` | Read |

`AclOperation` implication (Read→Describe etc.) is handled inside `SimpleAclAuthorizer` — we request the most specific op. Subject parsing reuses axum's matched-path/`Path` extraction inside the middleware (a pure `fn authz_target(method, path) -> Option<(ResourceType, String, AclOperation)>` so it is unit-tested independent of axum).

### Unit 3 — Server TLS (`bin/schema-registry.rs` + `rest/serve.rs`, new)

When `tls` config is present, serve HTTPS: build `Arc<rustls::ServerConfig>` via `crabka_security::TlsConfig::build_server_config()`, accept with `tokio-rustls` (gateway `serve.rs` accept-loop pattern). On `ClientAuthMode::Optional|Required`, run `extract_principal_from_cert` on the verified peer cert and insert the resulting `Principal { auth_method: MTls }` into request extensions (gateway `serve.rs` `peer_principal` pattern); Unit 1 consumes it as its highest-precedence source. Without `tls`, serve plain HTTP as today.

### Unit 4 — Client→broker security (`config.rs` + `kafkastore`)

`KafkaStore::start` threads a `client_core::ClientSecurity` (from config) into every `Client::builder()` it constructs (producer, reader, the election + admin clients). Pure passthrough — supports `PLAINTEXT|SASL_PLAINTEXT|SSL|SASL_SSL` with `SaslCredentials::{Plain,Scram,Gssapi}` + `TlsConnectorConfig`. No new logic; just wire the config through.

### Unit 5 — Config (`config.rs` + CLI/env)

`RegistryConfig` gains a `security: SecurityConfig` (all sub-fields `Option`, default = today's open behavior):

```
SecurityConfig {
  require_auth: bool,                      // anonymous → 401 when true
  realm: String,                           // WWW-Authenticate realm
  basic: Option<BasicAuthConfig>,          // file path or inline user→hash map
  bearer: Option<BearerAuthConfig>,        // reuse broker OAuth config (issuer/JWKS/principal-claim/…)
  tls: Option<crabka_security::TlsConfig>, // server cert/key/CA + client_auth
  authz: Option<AuthzConfig>,              // enable + super_users + acl_refresh_interval
  client: crabka_client_core::ClientSecurity, // SR↔broker SASL/TLS (default PLAINTEXT)
}
```

CLI/env flags for each (clap), mirroring the broker/gateway flag names where they exist.

## Interaction with slice-5 forwarding

- Authz is enforced **at the ingress node** for both reads and writes (every node refreshes its own `AclCache`). A write hitting a secondary is authorized **before** `forward_layer` proxies it to the primary.
- The primary **trusts** forwarded writes: `authz_layer` skips authorization when `FORWARD_HEADER` is present. This is the same documented trust boundary as slice-5's brief split-brain window and matches cp SR's secondary→primary forwarding. Secure the inter-node link in deployment (network policy / inter-node mTLS).
- `forward::proxy` already re-attaches `FORWARD_HEADER`; no credential forwarding is needed. (It does NOT forward the client `Authorization` header — authz already happened at ingress.)

## cp-fidelity

OSS `cp-schema-registry` security = HTTPS + `BASIC` auth + Kafka-client security. The **only** cp oracle is HTTP Basic behavior:

- `tests/capture_auth_fixtures.rs` (`#[ignore]`, Docker): boot `cp-schema-registry:7.4.0` with `SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC` + a JAAS/htpasswd file; capture, for (a) no credentials, (b) wrong credentials, (c) right credentials: the HTTP status (`401`/`200`), the `WWW-Authenticate` header, and the error body → `tests/fixtures/auth/basic.json`. Pin our middleware to match the `401` + `WWW-Authenticate: Basic realm="…"` + body byte-shape.
- Per-subject authz (Topic ACLs), Bearer, mTLS, TLS, and client-security have **no cp oracle** — validated by broker-backed integration tests + the proven gateway pattern.

## Testing

- **Unit:** `BasicAuthStore::verify`; the auth precedence + `401`/`WWW-Authenticate` logic (`decide`-style pure fn over headers + config); `authz_target(method, path)` mapping table; the authz allow/deny/super-user/forward-skip branches; config parsing.
- **Integration (in-process, `tests/security.rs`):** boot an in-process broker (with seeded ACLs via `client-admin` `CreateAcls`) + the SR with auth+authz enabled. Assert: `401` (no creds, `require_auth`), `401` (bad creds), `403` (authn'd but no ACL), `200` (authorized register/read/delete); reads authorized on any node; a write to a secondary is authorized-at-ingress then forwarded and lands; an unauthorized write to a secondary is `403` and never forwarded. TLS: an HTTPS round-trip with a self-signed cert; mTLS principal extraction.
- **cp Docker capture:** the Basic-auth fixture above.
- Existing slices 1–5 tests stay green (security is opt-in; the default `router`/`KafkaStore::start` path is unchanged when no `SecurityConfig` is set).

## Out of scope / deferred

- Confluent-commercial RBAC (role bindings, MDS), SR ACL management *via the REST API* (`/acls`), and resource patterns beyond Literal/Prefixed.
- A dedicated `ResourceType::Subject` (we reuse `Topic` per the approved decision).
- Forwarding the original client identity to the primary for re-authorization (we authorize at ingress + trust the inter-node link).
- Audit logging of authz decisions (the gateway has it; defer unless trivial to reuse).
- OAuth token *acquisition* by the SR client to the broker (SASL OAUTHBEARER outbound) — the client supports PLAIN/SCRAM/GSSAPI now; OAUTHBEARER-outbound is deferred.

## File structure

```
crates/schema-registry/
  Cargo.toml                 # + crabka-authz, crabka-security, crabka-metadata, crabka-client-admin, base64, (bcrypt?)
  src/
    config.rs                # + SecurityConfig and sub-structs
    auth/mod.rs              # auth_layer + AuthState; Principal resolution + 401 logic
    auth/basic.rs            # BasicAuthStore (new credential primitive)
    authz.rs                 # SchemaRegistryAuthz + authz_layer + authz_target() + AclCache refresh task
    rest/mod.rs              # router_with_security(state, SecurityLayers) wrapping forward+authz+auth
    rest/serve.rs            # TLS accept loop (HTTPS) + mTLS principal plumbing
    kafkastore/mod.rs        # thread ClientSecurity into Client::builder() calls
    bin/schema-registry.rs   # CLI/env for SecurityConfig; build layers; serve HTTP or HTTPS
  tests/
    security.rs              # in-process broker + ACLs; 401/403/200; forward-authz; TLS/mTLS
    capture_auth_fixtures.rs # #[ignore] Docker: cp BASIC auth oracle
    fixtures/auth/basic.json
```
