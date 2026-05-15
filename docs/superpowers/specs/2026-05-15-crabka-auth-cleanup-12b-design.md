# Slice 12b: Auth cleanup — Design Spec

## Goal

Finish the two deferred items from slice 12:

1. **Raft-over-SASL**: the controller listener terminates TLS + SASL using
   the same code path as the data plane, and the `InterBrokerDialer`
   slot on `ControllerConfig` (added but not wired in slice 12) is
   filled in so outbound raft RPC authenticates against peers.
2. **Bootstrap-file consumption**: `Broker::start` reads
   `<log_dir>/bootstrap.records.bin` produced by
   `crabka format --add-scram` and injects those records into the
   initial raft bootstrap, so SCRAM users provisioned at format time
   are usable on first start without any `AlterUserScramCredentials`
   call.

Out of scope: mTLS, ACLs proper, SCRAM-SHA-256, controller-quorum
listener mapping beyond a single protocol.

## Background

Slice 12 shipped TLS + SASL across the broker's data plane,
inter-broker replication, and controller heartbeat. Two pieces were
deferred:

- The `OutboundDialer` trait abstraction landed in `crabka-raft`
  alongside an `InterBrokerDialer` adapter in `crabka-broker`, but
  `ControllerConfig::dialer` is injected as `None` in slice 12 — so
  raft RPC still uses raw `TcpStream::connect` on outbound and the
  controller's listener accepts raw `TcpStream` on inbound. The
  controller listener never terminates SASL/TLS today.
- `crabka format --add-scram` writes `bootstrap.records.bin` (and a
  `bootstrap.json` operator-manifest), but `Broker::start` doesn't
  read either. The working operator-bootstrap channel in slice 12 is
  the static super-user PLAIN credentials in `BrokerConfig`.

This slice closes both. No new wire api_keys, no new metadata records.
The scope is exclusively wiring + config + tests.

## Architecture

### Crates touched

| Crate | Change |
|-------|--------|
| `crabka-broker` | `controller_listener_protocol` on `BrokerConfig`; new `raft_handshake.rs` module; bootstrap-records loader; `Broker::start` wires both. |
| `crabka-raft` | `RaftListenerHandshake` trait + `ControllerConfig::handshake` slot. No behavior change when slot is `None`. |
| `crabka-cli` | No code change — its slice-12 output is now consumed for the first time. |

### Controller listener protocol

`BrokerConfig` gains a single field:

```rust
pub controller_listener_protocol: ListenerProtocol,   // default Plaintext
```

Reuses the existing `tls_config` and `enabled_sasl_mechanisms` /
`inter_broker_credentials` config. No second TLS keystore or second
SASL credential pool.

### Raft listener accept

The controller's existing accept loop (in `crates/raft/src/network.rs`)
is generalized to call an optional handshake hook before yielding the
stream to the raft per-connection handler:

```rust
#[async_trait::async_trait]
pub trait RaftListenerHandshake: Send + Sync {
    async fn upgrade(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<Box<dyn DuplexStream>, RaftHandshakeError>;
}

// In ControllerConfig (next to existing `dialer`):
pub handshake: Option<Arc<dyn RaftListenerHandshake>>,
```

When `handshake.is_none()` the listener feeds `TcpStream` directly to
the raft handler (legacy path; every existing test keeps passing).

The per-connection handler is generalized over
`AsyncRead + AsyncWrite + Unpin + Send + 'static` to accept either a
raw `TcpStream` or a wrapped one. Mirror image of what slice 12 did to
`serve_connection_stream` on the data plane.

### Broker-side handshake adapter

A new `crates/broker/src/raft_handshake.rs` holds the broker's impl:

```rust
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub protocol: ListenerProtocol,
    pub controller_image: Arc<...>, // for SCRAM credential lookup
    pub super_user_name: Option<String>,
}

impl RaftListenerHandshake for BrokerRaftHandshake {
    async fn upgrade(&self, stream) -> Result<Box<dyn DuplexStream>, _> {
        let stream: Box<dyn DuplexStream> =
            if self.protocol.requires_tls() {
                Box::new(self.tls_acceptor.as_ref().unwrap()
                    .accept(stream).await?)
            } else {
                Box::new(stream)
            };
        if self.protocol.requires_sasl() {
            // Drive SaslHandshake + SaslAuthenticate inline, reusing
            // network::auth::handle_handshake + handle_authenticate_plain
            // + handle_authenticate_scram. Mirror image of
            // InterBrokerClient::run_outbound_sasl.
            run_inbound_sasl(&mut *stream, ...).await?;
        }
        Ok(stream)
    }
}
```

`run_inbound_sasl` is ~60 lines of frame-the-Kafka-response code; the
underlying state-machine logic comes verbatim from
`crates/broker/src/network/auth.rs`.

`Broker::start` constructs `BrokerRaftHandshake` only when
`controller_listener_protocol != Plaintext`. The `Arc<dyn>` is then
passed to `ControllerConfig::handshake`.

### Outbound dialer wiring

`Broker::start` also flips the existing slice-12 `InterBrokerDialer`
adapter from "constructed but unused" to "constructed and assigned":

```rust
controller_config.dialer = Some(Arc::new(InterBrokerDialer::new(
    inter_broker_client.clone(),
    controller_listener_protocol,
    inter_broker_server_name.clone(),
)));
```

The dialer's `dial` impl is unchanged — slice 12 already implemented
it; we just stop passing `None`.

### Bootstrap-records loader

`Broker::start`'s bootstrap branch is the only consumer:

```rust
if matches!(config.bootstrap_mode, BootstrapMode::Bootstrap)
   && raft_log_dir_is_empty(&config.log_dir)?
{
    let extra = load_bootstrap_records(&config.log_dir)?;
    // existing: write V1ClusterId
    // new:      chain `extra` into the same initial submit_change batch
}
```

`load_bootstrap_records`:

```rust
fn load_bootstrap_records(
    log_dir: &Path,
) -> Result<Vec<MetadataRecord>, BrokerError> {
    let path = log_dir.join("bootstrap.records.bin");
    if !path.exists() {
        return Ok(vec![]);
    }
    let bytes = std::fs::read(&path)
        .map_err(|e| BrokerError::BootstrapFile { path: path.clone(), source: e.into() })?;
    let mut out = Vec::new();
    let mut cur = &bytes[..];
    while !cur.is_empty() {
        if cur.len() < 4 {
            return Err(BrokerError::BootstrapFile {
                path: path.clone(),
                source: anyhow::anyhow!("truncated length prefix").into(),
            });
        }
        let len = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
        cur = &cur[4..];
        if cur.len() < len {
            return Err(...);  // truncated body
        }
        let rec: MetadataRecord =
            <SerdeCompat<MetadataRecord>>::deserialize(&cur[..len])
                .map_err(|e| BrokerError::BootstrapFile { path: path.clone(), source: e.into() })?;
        out.push(rec);
        cur = &cur[len..];
    }
    Ok(out)
}
```

Matches the framing `crabka-cli`'s format subcommand wrote in slice 12.

The records are appended to the initial `submit_change` batch *after*
the `V1ClusterId` record. openraft applies them in order; the metadata
image has the SCRAM credentials before the broker accepts its first
client connection.

`bootstrap.json` (the operator-readable manifest) is ignored.
`bootstrap.records.bin` is left on disk after bootstrap — never
re-consulted on subsequent starts; the raft log is now the source of
truth.

## Components

### `crabka-broker`

- `config.rs`:
  - `pub controller_listener_protocol: ListenerProtocol` field (default `Plaintext`).
  - `validate()` extends with:
    - `controller_listener_protocol.requires_tls() && tls_config.is_none()` → `Err(BrokerError::Tls("controller listener requires TLS but tls_config is None"))`.
    - `controller_listener_protocol.requires_sasl() && enabled_sasl_mechanisms.is_empty()` → `Err(BrokerError::SaslListenerNoMechanisms { name: "controller".into() })`.
- `raft_handshake.rs` (new, ~120 lines including frame helpers):
  - `BrokerRaftHandshake` struct + `RaftListenerHandshake` impl.
  - `run_inbound_sasl` private helper.
  - `RaftHandshakeError` enum (Io / Tls / Sasl / Protocol).
- `bootstrap.rs` (new, ~80 lines):
  - `load_bootstrap_records(log_dir)`.
  - `raft_log_dir_is_empty(log_dir)` helper.
  - `BrokerError::BootstrapFile { path, source }` variant.
- `broker.rs`:
  - In `Broker::start`, after building `inter_broker_client`:
    - Build `BrokerRaftHandshake` if controller protocol != Plaintext; pass to `controller_config.handshake`.
    - Build `InterBrokerDialer` and assign to `controller_config.dialer`.
    - Call `load_bootstrap_records` on the bootstrap branch; chain into the initial `submit_change`.

### `crabka-raft`

- `config.rs`:
  - `pub handshake: Option<Arc<dyn RaftListenerHandshake>>` on `ControllerConfig`.
- `network.rs`:
  - The accept-loop body becomes generic over a per-connection stream type. The wrapper that owns the `TcpListener` calls `handshake.upgrade(stream)` (if `Some`) before invoking the generic handler.
  - The per-connection handler signature changes from `fn(TcpStream, ...)` to `fn<S>(S, ...) where S: AsyncRead + AsyncWrite + Unpin + Send + 'static`.
- `lib.rs`:
  - `pub use network::{RaftListenerHandshake, RaftHandshakeError};` (exposed so the broker can implement the trait).
- Pre-existing PLAINTEXT-only multi-broker tests must keep passing. The `handshake = None` default preserves behavior.

### `crabka-cli`

No code change. Slice 12's `format --add-scram` already produces the
file shape the new loader expects.

## Data flow

### Outbound raft RPC

1. openraft asks the network factory for a client to a peer node.
2. Factory calls `cfg.dialer.dial(target)` → `InterBrokerDialer::dial`.
3. `InterBrokerDialer` resolves host/port from the metadata image and calls `InterBrokerClient::connect(host, port, controller_listener_protocol, server_name)`.
4. `InterBrokerClient` runs TCP connect → TLS wrap (if needed) → SASL handshake (if needed) → returns `Box<dyn DuplexStream>`.
5. openraft sends raft frames over the auth'd stream.

### Inbound raft RPC

1. Controller listener accepts a TCP stream on `controller_listen_addr`.
2. If `controller_config.handshake.is_some()`:
   - `BrokerRaftHandshake::upgrade(stream)`:
     - TLS terminate if `requires_tls()`.
     - Run inbound SASL state machine if `requires_sasl()`, using the same handler bodies as the data plane.
     - Return `Box<dyn DuplexStream>` once `Authenticated`.
3. If `handshake.is_none()`: pass the raw `TcpStream` directly.
4. Raft per-connection handler reads RPC frames over the (possibly-wrapped) stream.

### Bootstrap consumption

1. `Broker::start` enters the bootstrap branch only on `BootstrapMode::Bootstrap` AND empty raft log dir.
2. Calls `load_bootstrap_records(&config.log_dir)`:
   - File absent → returns `Ok(vec![])`. Legacy path.
   - File present, decodes cleanly → returns `Ok(records)`.
   - File present, IO/decode failure → returns `Err(BrokerError::BootstrapFile { ... })`. Broker refuses to start.
3. Initial submit chains `[V1ClusterId, ...records]` into one `submit_change`.
4. openraft commits in order; metadata image picks up SCRAM credentials via the normal apply path.
5. `bootstrap.records.bin` is left on disk but never re-read.

## Error handling

### Raft listener (inbound)

- **TLS handshake failure** — drop connection, `debug!` log. Never propagates to broker shutdown.
- **SASL handshake failure (33 / 58)** — emit typed Kafka response, then close. Mirror of the data plane.
- **Pre-auth gate violation** — close. Real peers via `InterBrokerDialer` always reach `Authenticated` before the first raft frame.
- **Post-auth raft framing error** — existing openraft behavior, unchanged.

### Raft dialer (outbound)

- **TLS / SASL failure** — return `InterBrokerError`. openraft's retry-with-backoff absorbs the transient case. Logged `warn` per attempt.
- **Persistent auth failure** — broker keeps retrying. We do NOT fence a peer on persistent raft-auth failure; that would let a credential-rotation window split-brain the cluster. Operator surfaces the misconfiguration via log inspection.

### Bootstrap-file load

- **File absent** — `Ok(vec![])`. No log entry.
- **IO error / permission denied** — `BrokerError::BootstrapFile { path, source }`. Broker refuses to start. `error!` log.
- **Truncated length prefix or body** — same outcome.
- **Decode failure (unknown variant, corrupt bytes)** — same outcome.
- **Unknown `MetadataRecord` variant** (older broker reading newer CLI output) — `serde_wincode` decode fails; broker refuses to start. Operator response: upgrade broker.

### Config validation (startup, fatal-to-broker)

- `controller_listener_protocol.requires_tls() && tls_config.is_none()` → `Tls(...)`.
- `controller_listener_protocol.requires_sasl() && enabled_sasl_mechanisms.is_empty()` → `SaslListenerNoMechanisms { name: "controller" }`.

No new variants — both reuse slice 12's error types.

## Testing

### Unit tests

`crabka-broker`:

- `raft_handshake::tests::plaintext_passthrough` — `BrokerRaftHandshake { protocol: Plaintext, .. }` returns the raw stream wrapped. In-process `tokio::io::duplex()` pair; no TCP.
- `raft_handshake::tests::sasl_plain_rejects_bad_password` — drive handshake in-process, send bad PLAIN creds, expect connection close.
- `bootstrap::tests::load_records_returns_empty_when_absent`.
- `bootstrap::tests::load_records_decodes_v1_scram_credential` — write one record, round-trip.
- `bootstrap::tests::load_records_refuses_truncated_frame`.
- `bootstrap::tests::load_records_refuses_undecodable_record`.

### Broker integration tests (no Docker)

- `crates/broker/tests/raft_sasl.rs` (new):
  - `controller_listener_sasl_plaintext_two_broker_quorum` — two brokers, both `controller_listener_protocol = SaslPlaintext`, matching credentials; assert `broker_count() == 2` converges.
  - `controller_listener_sasl_plaintext_rejects_mismatched_creds` — mismatched passwords; assert no convergence (`broker_count() == 1` after timeout).
  - `controller_listener_plaintext_legacy_path_unchanged` — explicit smoke for the default path.
- `crates/broker/tests/bootstrap_consumption.rs` (new):
  - `bootstrap_records_provisions_scram_user` — run `crabka format --add-scram` (as library call or shell-out), start broker pointed at the produced log_dir, auth as the provisioned user via SCRAM.
  - `corrupt_bootstrap_refuses_start` — garbage in `bootstrap.records.bin`; expect `Err(BootstrapFile)`.
  - `bootstrap_absent_legacy_path` — no file; legacy path unchanged.

### JVM acceptance tests (Docker)

- `crates/broker/tests/jvm_acceptance.rs`:
  - `jvm_inter_broker_sasl_ssl_raft_replication` (new) — two brokers, controller + data plane both SASL_SSL. Provision alice via `kafka-configs --alter --entity-type users`. Create rf=2 topic. Produce 50 records via JVM SASL_SSL client. Assert both brokers reach offset 50. Supersedes slice 12 T23's simplified inter-broker test.
  - Slice 12 T23's `jvm_inter_broker_replication_authed` stays — documents the legacy plaintext-controller path still works.

- Gated `#[cfg(not(target_os = "windows"))]` per existing convention.

### Regression guards

- All existing 3-broker raft tests use default `controller_listener_protocol = Plaintext` and pass without edits.
- Slice 12's `jvm_inter_broker_replication_authed` keeps plaintext-controller setup; proves the listeners are independently configurable.

### Out of scope for tests

- mTLS on the controller listener.
- Mixed mechanisms (PLAIN on data, SCRAM on controller).
- SCRAM credential rotation under live raft traffic.

## Wire-protocol additions

None.

## Out of scope

- ACLs (slice 12's super-user-name stand-in remains).
- Delegation tokens, OAUTHBEARER, GSSAPI, SCRAM-SHA-256.
- mTLS client-auth enforcement.
- Quotas / throttling.
- Per-listener controller-quorum mapping (single protocol per cluster).
