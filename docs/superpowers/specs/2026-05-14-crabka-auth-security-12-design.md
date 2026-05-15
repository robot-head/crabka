# Slice 12: Auth & security (TLS + SASL/PLAIN + SASL/SCRAM-SHA-512) — Design Spec

## Goal

Make a Crabka broker safe to expose on a network. JVM clients must be
able to connect over TLS, authenticate with SASL/PLAIN or
SASL/SCRAM-SHA-512, and have inter-broker traffic (replication, raft,
controller heartbeats) authenticated end-to-end.

Out of scope: ACLs, delegation tokens, OAUTHBEARER, GSSAPI, mTLS
client-auth enforcement, SCRAM-SHA-256 (mechanism plumbing is generic,
but only SHA-512 ships), credential rotation choreography, quotas.

## Background

Slices 1–11 ship a broker that JVM clients can produce, consume,
replicate, and operate against, all over plaintext TCP. The roadmap's
item 11 ("Auth/security") is the next gate. The slice-11 spec
explicitly deferred ACLs and any auth surface; this spec picks that up.

We target the canonical Kafka KRaft-era auth shape so existing operator
tooling and client configs work unchanged:

- Listener protocol multiplexing (PLAINTEXT / SSL / SASL_PLAINTEXT /
  SASL_SSL) per `listeners` config.
- TLS via `rustls` (pure-Rust, tokio-native).
- SASL handshake via the standard `SaslHandshake (17)` /
  `SaslAuthenticate (36)` request pair.
- Credentials stored in the metadata image via a new
  `V1ScramCredential` record, provisioned through
  `AlterUserScramCredentials (51)` per KIP-554.
- Bootstrap via a `crabka format --add-scram` CLI subcommand
  (analogue of `kafka-storage.sh format --add-scram`) AND via static
  inter-broker credentials in `BrokerConfig`.

## Architecture

### New and modified crates

| Crate | Change |
|-------|--------|
| `crabka-security` (new) | Pure-logic SCRAM + PLAIN verifiers, credential hashing, listener+TLS config types. No I/O. Shared by broker and CLI. |
| `crabka-cli` (new) | `crabka format --add-scram` bootstrap tool. |
| `crabka-metadata` | New `V1ScramCredential` + `V1DeleteScramCredential` records; image entry; `scram_credential()` accessor. |
| `crabka-broker` | Listener registry; per-listener accept loops; TLS termination; SASL handshake handlers; per-connection auth state machine; `AlterUserScramCredentials` handler; inter-broker client wrapper. |

`crabka-security` exists as its own crate so the format CLI can produce
a `ScramCredential` using the exact code the broker validates against,
without dragging the broker crate into the CLI.

### Listener model

`BrokerConfig` gains:

```rust
pub struct ListenerSpec {
    pub name: String,                      // e.g. "EXTERNAL", "INTERNAL"
    pub bind_addr: SocketAddr,
    pub advertised: String,                // host:port returned in Metadata
    pub protocol: ListenerProtocol,        // PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
}

pub struct BrokerConfig {
    // ... existing fields ...
    pub listeners: Vec<ListenerSpec>,
    pub inter_broker_listener_name: String,
    pub inter_broker_credentials: Option<InterBrokerCredentials>,
    pub plain_credentials: HashMap<String, String>,  // user -> password, static
    pub super_user_name: Option<String>,             // principal granted admin rights
    pub tls_config: Option<TlsConfig>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
}
```

The broker spawns one `accept_loop` per `ListenerSpec` at startup.
For SSL / SASL_SSL listeners the accept loop terminates TLS via
`tokio_rustls::TlsAcceptor` before yielding the stream to
`serve_connection`. `serve_connection` is generic over
`AsyncRead + AsyncWrite + Unpin + Send`, so the rest of the dispatch
code is unchanged.

### Per-connection auth state

```rust
enum ConnectionAuth {
    Anonymous,                                  // PLAINTEXT/SSL listeners
    Negotiating { mechanism: SaslMechanism, exchange: SaslExchangeState },
    Authenticated { principal: Principal },
}
```

The dispatch loop holds `ConnectionAuth` next to its existing
per-connection state. The request gate consults it:

- Pre-auth allowlist on SASL_* listeners: `ApiVersions (18)`,
  `SaslHandshake (17)`, `SaslAuthenticate (36)`. Anything else returns
  `ILLEGAL_SASL_STATE (34)` and closes the connection.
- PLAINTEXT / SSL listeners stay anonymous and unrestricted.

### Outbound (inter-broker) traffic

A new `crate::network::client::InterBrokerClient` resolves the target
broker's endpoint matching `BrokerConfig.inter_broker_listener_name`
from the metadata image, runs TLS if the listener protocol requires
it, then runs the client-side SASL handshake using
`crabka-security::ScramClientExchange` or the PLAIN client.

The replicator, raft transport, and controller-heartbeat clients all
dial through `InterBrokerClient`. Existing per-RPC logic is unchanged
once the stream is authed.

### Metadata wire change

`Metadata` response v9+ carries a per-listener-name `endpoints` array
on each broker. The current code populates a single `host`/`port` from
`advertised_listener`. We extend `BrokerRegistrationRecord` to carry a
`endpoints: Vec<BrokerEndpoint>` field. `MetadataResponse` projects all
endpoints; the legacy single-endpoint fields are derived from the
inter-broker endpoint for compatibility with older client API versions
that don't read the array.

## Components

### `crabka-security`

```rust
pub enum ListenerProtocol { Plaintext, Ssl, SaslPlaintext, SaslSsl }
pub enum SaslMechanism { Plain, ScramSha512 }

pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    pub trust_roots_path: Option<PathBuf>,     // for inter-broker outbound
}
impl TlsConfig {
    pub fn build_server_config(&self) -> Result<rustls::ServerConfig, _>;
    pub fn build_client_config(&self) -> Result<rustls::ClientConfig, _>;
}

pub struct ScramCredential {
    pub mechanism: SaslMechanism,
    pub salt: [u8; 16],
    pub stored_key: Vec<u8>,        // length depends on mechanism's hash output
    pub server_key: Vec<u8>,
    pub iterations: u32,
}
pub fn hash_scram_password(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) -> ScramCredential;

pub fn verify_plain(
    creds: &HashMap<String, String>,    // from BrokerConfig
    user: &str,
    password: &[u8],
) -> Result<Principal, AuthError>;       // constant-time compare on hit; AuthError::UnknownUser on miss
                                         // (returned as the same wire error as BadPassword)

pub struct ScramServerExchange { /* internal state machine */ }
impl ScramServerExchange {
    pub fn new(credential: ScramCredential) -> Self;
    pub fn step(&mut self, client_bytes: &[u8]) -> StepResult;
}
pub enum StepResult { Continue(Vec<u8>), Done(Principal, Vec<u8>), Failed }

pub struct ScramClientExchange { /* mirror for outbound */ }

pub struct Principal { pub name: String, pub mechanism: SaslMechanism }

pub enum AuthError {
    UnknownUser,
    BadPassword,
    BadProof,
    MalformedMessage,
    UnsupportedMechanism,
}
```

Constant-time comparison is used for SCRAM proof verification.
`AuthError` is never surfaced verbatim to the wire — it always maps to
the single error code `SASL_AUTHENTICATION_FAILED (58)` on the
response, with the detail logged at `debug`. This prevents
username-enumeration via response timing/content.

### `crabka-metadata`

```rust
pub enum MetadataRecord {
    // ... existing variants ...
    V1ScramCredential(ScramCredentialRecord),
    V1DeleteScramCredential(DeleteScramCredentialRecord),
}

pub struct ScramCredentialRecord {
    pub user: String,
    pub mechanism: SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}
pub struct DeleteScramCredentialRecord {
    pub user: String,
    pub mechanism: SaslMechanism,
}
```

`MetadataImage` keeps a `HashMap<(String, SaslMechanism),
ScramCredential>`. Last-write-wins. `scram_credential(user,
mechanism) -> Option<&ScramCredential>` is the accessor.

### `crabka-broker`

New modules:

- `src/network/auth.rs` — `ConnectionAuth` state, `SaslHandshake` and
  `SaslAuthenticate` handlers, pre-auth gate.
- `src/network/client.rs` — `InterBrokerClient`, outbound TLS + SASL
  driver.
- `src/handlers/alter_user_scram_credentials.rs` — api_key 51, KIP-554.

Existing handlers register the three new api_keys (17, 36, 51) in
`api_versions.rs::supported_apis()` and `network/dispatch.rs`'s
`handler_body_flexible` table.

`BrokerConfig` keeps a backwards-compatible default: when `listeners`
is empty, the existing `listen_addr` + `advertised_listener` fields
synthesize a single `PLAINTEXT` listener named `PLAINTEXT`. All
current tests stay green without rewrite.

### `crabka-cli`

A new minimal crate with one binary, `crabka`, and one subcommand:

```
crabka format --log-dir <DIR> --cluster-id <UUID> \
  [--add-scram SCRAM-SHA-512=[name=<U>,password=<P>,iterations=<N>]]
```

Refuses to overwrite an already-formatted log directory. Computes the
PBKDF2 hash via `crabka-security`, writes a `V1ScramCredential` record
into a fresh raft log at the same offset the broker would use for its
initial cluster-id record.

## Data flow

### Inbound client handshake (SASL_SSL listener)

1. Accept TCP; `TlsAcceptor::accept` runs the TLS handshake. On
   error, drop connection and log at `debug`.
2. The plaintext-over-TLS stream is handed to `serve_connection` with
   `listener_protocol = SaslSsl`.
3. Loop reads framed Kafka requests. The pre-auth gate:
   - `ApiVersions (18)` → respond normally; clients send it first.
   - `SaslHandshake (17, mechanism)`:
     - mechanism in `enabled_sasl_mechanisms` → respond with the
       enabled list and transition state to
       `Negotiating { mechanism, exchange: ... }`.
     - else → `UNSUPPORTED_SASL_MECHANISM (33)`; connection stays
       open per Kafka behavior so the client may retry handshake.
   - `SaslAuthenticate (36, bytes)` while `Negotiating(mech, exch)`:
     - `PLAIN`: decode `\0user\0password`. Look up
       `BrokerConfig.plain_credentials[user]` (constant-time compare).
       Success → `Authenticated { principal }`. Fail →
       `SASL_AUTHENTICATION_FAILED (58)`, send response, close.
       PLAIN credentials are static config (analogous to Kafka's JAAS
       `user_X=...` entries); they never live in the metadata image.
     - `SCRAM-SHA-512`: `exch.step(bytes)`. First call returns
       `Continue(server_first_message)`; state stays
       `Negotiating(...)`. Second call returns
       `Done(principal, server_final_message)` →
       `Authenticated { principal }`. Any `Failed` →
       `SASL_AUTHENTICATION_FAILED (58)` + close.
   - Anything else → `ILLEGAL_SASL_STATE (34)` + close.
4. Once `Authenticated`, the existing handler dispatch runs unchanged.
   The principal is available via the per-connection context (used
   only for `AlterUserScramCredentials` super-user check in slice 12).

### Outbound (replicator → leader)

1. `InterBrokerClient::connect(target_node_id)` reads
   `MetadataImage.broker(target).endpoint(inter_broker_listener_name)`.
2. TCP connect. If protocol is `SSL` or `SASL_SSL`, wrap in
   `tokio_rustls::TlsConnector` with configured trust roots.
3. If protocol is SASL_*, send `ApiVersions`, then
   `SaslHandshake(mech)`, then drive the `SaslAuthenticate` exchange
   using `ScramClientExchange` (or PLAIN). Credentials come from
   `BrokerConfig.inter_broker_credentials`.
4. Return the authed stream to the caller. Existing fetch / raft RPC
   code reads and writes against it unchanged.

### `AlterUserScramCredentials` (api_key 51)

1. Request: `Upsertions: [{ name, mechanism, iterations, salt,
   salted_password }]`, `Deletions: [{ name, mechanism }]`. The
   client (`kafka-configs --alter --entity-type users`) computes the
   PBKDF2 hash and salt locally; the broker stores them as given.
2. Authorization: `principal.name == BrokerConfig.super_user_name`,
   else per-user `CLUSTER_AUTHORIZATION_FAILED (31)`. (Stand-in for
   the real ACL system; explicitly commented.)
3. Validation:
   - `iterations >= 4096` else `UNACCEPTABLE_CREDENTIAL (74)`.
   - `salt` non-empty else `UNACCEPTABLE_CREDENTIAL (74)`.
   - `salted_password` length matches mechanism hash size else
     `UNACCEPTABLE_CREDENTIAL (74)`.
   - Same `(user, mechanism)` appears twice in the same request →
     `DUPLICATE_RESOURCE (84)`.
   - Deletion target doesn't exist → `RESOURCE_NOT_FOUND (66)`
     (per-user, doesn't fail the request).
4. For each valid upsertion, submit `V1ScramCredential` to controller.
   For each deletion, submit `V1DeleteScramCredential`. Wait for
   commit; respond.

### Format-time bootstrap

1. Parse `--add-scram SCRAM-SHA-512=[name=admin,password=hunter2,iterations=4096]`.
2. Generate 16-byte random salt, run PBKDF2-HMAC-SHA-512, build
   `ScramCredentialRecord`.
3. Append a `V1ScramCredential` record to a fresh raft log directory
   alongside the existing `V1ClusterId` bootstrap record using the
   raft bootstrap path the broker already exercises on first start.

## Error handling

**Authentication errors** (per-connection, fatal):

- `UNSUPPORTED_SASL_MECHANISM (33)` — mechanism not enabled. Response
  carries the enabled list; connection stays open.
- `ILLEGAL_SASL_STATE (34)` — non-handshake request before auth
  completes on a SASL listener. Response sent, connection closed.
- `SASL_AUTHENTICATION_FAILED (58)` — bad PLAIN credentials, SCRAM
  proof mismatch, unknown user, expired credential. Response sent,
  connection closed. Same error shape for "user unknown" vs "wrong
  password" so timing and content don't leak which side failed.

**TLS errors** (accept-loop, fatal-to-connection):

- Handshake failure → log at `debug` (noisy from health-checkers),
  drop connection. Never propagates to broker-level shutdown.
- Cert load failure at startup → `BrokerStartError::Tls(...)`. Broker
  refuses to start.

**Config errors** (startup, fatal-to-broker):

- Listener bind collision (two listeners share `bind_addr`).
- `inter_broker_listener_name` not present in `listeners` list.
- SASL listener declared without `enabled_sasl_mechanisms` populated.
- `inter_broker_credentials.mechanism` not enabled on the named
  listener.
- Cert / key path unreadable.

**Provisioning errors** (`AlterUserScramCredentials`):

- `RESOURCE_NOT_FOUND (66)` — per-user on a delete miss; doesn't fail
  the request.
- `UNACCEPTABLE_CREDENTIAL (74)` — iterations < 4096, empty salt,
  wrong-length salted password.
- `DUPLICATE_RESOURCE (84)` — same `(user, mechanism)` twice in one
  request.
- `CLUSTER_AUTHORIZATION_FAILED (31)` — principal is not the
  super-user. (Stand-in until real ACLs land.)

**Outbound auth failures** (inter-broker):

- `InterBrokerClient::connect` returns an error; existing
  reconnect-with-backoff path in replicator / raft handles it. The
  broker does not crash on transient inter-broker auth failure
  (rolling credential rotation must not bring the broker down).
  Logged at `warn`.

**Bootstrap CLI errors:**

- `--add-scram` with `iterations < 4096` → exit code 2.
- Target log dir not empty → exit code 3 (refuse to overwrite).

## Testing

### Unit tests

`crabka-security`:

- PBKDF2 vectors from RFC 7677 / RFC 5802.
- `ScramServerExchange` round-trips with `ScramClientExchange`
  in-process for SCRAM-SHA-512.
- Constant-time proof comparison (loose timing assertion).
- `verify_plain`: correct passes, wrong password fails, unknown user
  fails with same wire-error mapping. (Internally `UnknownUser` vs
  `BadPassword` is distinct for logging; both map to the wire's
  `SASL_AUTHENTICATION_FAILED`.)
- TLS config builder: valid cert+key loads; mismatched key errors;
  bad PEM errors.

`crabka-metadata`:

- `V1ScramCredential` round-trip via `serde_wincode`.
- `V1DeleteScramCredential` apply removes from image.
- `scram_credential()` accessor returns last-write-wins.

`crabka-broker`:

- `auth.rs` state machine: pre-auth allowlist (ApiVersions /
  SaslHandshake / SaslAuthenticate pass, anything else → 34);
  illegal transitions rejected.
- `AlterUserScramCredentials` handler: upsert writes record, delete
  emits delete record, duplicate detected, sub-min iterations
  rejected, non-super-user gets 31.
- `BrokerConfig` validation: bind collision, missing inter-broker
  listener, missing creds.

### Integration tests (`crates/broker/tests/auth_handlers.rs`, no Docker)

- Broker with one `SASL_PLAINTEXT` listener + a provisioned SCRAM
  user (direct controller write). Hand-rolled Rust client speaks the
  SASL handshake, then runs `Produce`. Produce succeeds; principal
  logged.
- Wrong password → connection closed after 58.
- `AlterUserScramCredentials` via authed super-user creates user U2;
  a second client authenticates as U2.
- `ApiVersions` reachable pre-auth on SASL listener; `Metadata` is
  not (gets 34).
- TLS-only listener (`SSL`, no SASL): hand-rolled rustls client
  connects, produces, no SASL handshake.

### JVM acceptance tests (`crates/broker/tests/jvm_acceptance.rs`, Docker)

- `jvm_sasl_plain_produce_consume` — broker with SASL_PLAINTEXT
  listener and super-user creds. `kafka-console-producer` with
  `security.protocol=SASL_PLAINTEXT,sasl.mechanism=PLAIN,
  sasl.jaas.config=...` produces 10 records;
  `kafka-console-consumer` reads them back.
- `jvm_sasl_scram_sha512_produce_consume` — same but
  `sasl.mechanism=SCRAM-SHA-512`. User provisioned via
  `kafka-configs --alter --entity-type users --entity-name alice
  --add-config 'SCRAM-SHA-512=[password=foo]'` (JVM translates this
  to `AlterUserScramCredentials`).
- `jvm_tls_handshake_succeeds` — broker with SSL listener
  (self-signed cert), JVM client with
  `security.protocol=SSL,ssl.truststore.location=...` runs
  `kafka-broker-api-versions`.
- `jvm_sasl_ssl_full_stack` — SASL_SSL listener, JVM client with
  SASL_SSL + SCRAM-SHA-512.
- `jvm_inter_broker_replication_authed` — two-broker cluster,
  inter-broker SASL_PLAINTEXT/SCRAM. Topic with rf=2; produce, kill
  leader, follower takes over; reads intact.

### Negative-path JVM

- `jvm_wrong_password_rejected` — SASL_PLAIN with bad password from
  `kafka-console-producer`; assert producer errors with
  `SaslAuthenticationException`.

### Regression guards

- Existing PLAINTEXT-listener tests stay green: empty `listeners`
  config synthesizes a single PLAINTEXT listener matching current
  behavior.
- Default JVM image is cp-kafka:6.1.1 where it works; the SCRAM
  provisioning test may need cp-kafka:7.5.0 (verified during plan).

### Out of scope for tests

- Delegation tokens.
- ACL enforcement (only the placeholder super-user check on
  `AlterUserScramCredentials`).
- TLS client-auth / mTLS (configured-but-untested in slice 12).
- SCRAM credential rotation under load.
- Crypto fuzzing (relies on upstream `ring`, `rustls`, `pbkdf2`).

## Wire-protocol additions

| api_key | Name | Versions targeted |
|---------|------|-------------------|
| 17 | SaslHandshake | v1 |
| 36 | SaslAuthenticate | v2 (flexible) |
| 51 | AlterUserScramCredentials | v0 (flexible) |

`Metadata` response: populate the per-listener `endpoints` array on
each broker (v9+), which the codec already supports.

## Out of scope

- ACLs (real authorizer interface, ACL records, AccessControlEntry
  RPCs). The slice-12 super-user check is a stand-in.
- Delegation tokens.
- OAUTHBEARER and GSSAPI mechanisms.
- SCRAM-SHA-256.
- mTLS client-auth enforcement.
- Quotas / throttling.
- TLS hostname verification configurability beyond defaults.
- `kafka-storage.sh format`'s other subcommands; only `--add-scram`
  is in scope.
