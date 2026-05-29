# Crabka tiered storage 48r — TLS/SASL on the internal metadata client (design)

**Date:** 2026-05-29
**Status:** Slice design. Sequenced after 48o (both touch `kafka_log.rs`).
Closes a 48f follow-up. Part of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Let the topic-based RLMM connect to the broker over a secured listener
(TLS and/or SASL) instead of plaintext loopback, so tiered storage is
usable on a cluster where the only reachable listeners require
authentication. Achieve it by giving the **public client-core clients** a
real security surface (reusable beyond tiered storage), then pointing the
RLMM at the inter-broker listener with the broker's existing inter-broker
credentials.

## The gap

The RLMM's producer/consumer connect plaintext, with no security config:

```rust
// kafka_log.rs:113
Producer::builder().bootstrap(cfg.bootstrap.clone()) ...   // plaintext
// kafka_log.rs:325
Consumer::builder().bootstrap(bootstrap) ...               // plaintext
```

The 48f design records this: *"The TBRLMM connects via plaintext loopback
to its own broker. TLS / SASL on the internal client is a follow-up."*

Crabka **already has** all the outbound auth machinery — just not on the
public clients:

- `InterBrokerClient` (`crates/broker/src/network/client.rs:62`) does TLS +
  outbound SASL (`run_outbound_sasl`, PLAIN/SCRAM/GSSAPI) and hands a
  ready stream to `Connection::from_stream` (`connection.rs:95`).
- The broker config carries `InterBrokerCredentials`
  (`crates/broker/src/config.rs:43`) and a per-listener `TlsConfig` /
  `sasl_mechanisms` (`ListenerSpec`, `config.rs:26`).
- The public `Client` builder (`crates/client-core/src/client.rs:25`) has
  **no** TLS/SASL options — public clients are plaintext-only today.

## Approach (security on the public clients)

### 1. Extract the outbound SASL handshake into a shared crate

`run_outbound_sasl` / `run_scram_client` / `run_gssapi_client` /
`send_sasl_handshake` / `send_plain_authenticate` currently live in
`crates/broker/src/network/client.rs`. Move the transport-agnostic parts
into `crates/security` (or `crates/client-core`) as a reusable
`outbound_sasl(stream, creds, server_name)` operating on any
`AsyncRead + AsyncWrite`. `InterBrokerClient` then calls the shared
implementation (no behavior change), and the public clients call the same
one.

### 2. Security surface on client-core

Add an optional security config to the `Client` builder and a connect path
that performs TLS then SASL before `Connection::from_stream`:

```rust
pub struct ClientSecurity {
    pub protocol: ListenerProtocol,         // Plaintext | Ssl | SaslPlaintext | SaslSsl
    pub tls: Option<TlsConnectorConfig>,    // CA / SNI / client cert
    pub sasl: Option<SaslCredentials>,      // PLAIN | SCRAM | GSSAPI
}

Client::builder()
    .bootstrap(addr)
    .security(security)   // new; default None = plaintext (unchanged)
    .build()
```

`Producer`, `Consumer`, and `Admin` builders gain a pass-through
`.security(...)`. When `None`, the connect path is exactly today's
plaintext — no behavior change for existing callers.

Reuse the broker's TLS connector building and the
`InterBrokerCredentials` shape (or a thin client-side mirror) so config
types stay aligned across the inter-broker and public-client paths.

### 3. Point the RLMM at the secured listener

`KafkaRlmmConfig` / the bootstrap (`bootstrap_topic_rlmm`,
`crates/broker/src/broker.rs:2101`) gains the security config for the
metadata client. The broker supplies:

- bootstrap = the **inter-broker listener** advertised address (instead of
  plaintext loopback);
- credentials = `config.inter_broker_credentials`;
- TLS = the broker's inter-broker TLS connector.

`kafka_log.rs` threads `ClientSecurity` into the `Producer`/`Consumer`
(48o: client-core fetch loops) builders. When the broker runs fully
plaintext, the config is `None` and loopback behavior is unchanged.

## Files

- `crates/security` (or `crates/client-core`) — extracted shared outbound
  SASL handshake.
- `crates/broker/src/network/client.rs` — call the shared handshake (no
  behavior change).
- `crates/client-core/src/{client.rs, connection.rs}` — `ClientSecurity`,
  TLS+SASL connect path.
- `crates/client-producer`, `crates/client-consumer`,
  `crates/client-admin` — `.security(...)` pass-through.
- `crates/remote-storage-topic/src/kafka_log.rs` — accept + apply
  `ClientSecurity`.
- `crates/broker/src/broker.rs` — bootstrap supplies inter-broker
  listener + creds + TLS to the RLMM; `KafkaRlmmConfig` security field.

## Testing

- client-core unit/integration: connect to a `SASL_PLAINTEXT` listener and
  to a `SASL_SSL` listener; reuse the SASL test harness from the
  GSSAPI/SASL work (`701bea7`). PLAIN and SCRAM at minimum; GSSAPI if the
  existing harness supports it.
- Regression: plaintext clients (no `.security`) behave exactly as before.
- RLMM loopback over `SASL_PLAINTEXT`: bootstrap the topic-based RLMM
  against a SASL-required listener and round-trip a copy→metadata→read
  cycle (extends `tiered_storage_topic_rlmm.rs`).

## Non-goals

- No new authentication mechanisms — reuse the existing PLAIN/SCRAM/GSSAPI.
- No mutual-auth/ACL changes on the metadata topic beyond connecting
  authenticated as the inter-broker principal.

## Dependencies & sequencing

Sequenced after 48o (both touch `kafka_log.rs`; 48o reworks the consumer,
48r adds security to the reworked client). Independent of 48p/48q in
concept but shares `kafka_log.rs`/`manager.rs`, so run sequentially within
the topic-RLMM chain.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-client-core -p crabka-broker -p crabka-remote-storage-topic`
- `cargo test --workspace` (no regressions)
- Inter-broker SASL/TLS semantics unchanged; no CRD drift.
