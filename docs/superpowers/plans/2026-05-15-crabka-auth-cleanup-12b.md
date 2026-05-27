# Slice 12b: Auth cleanup — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** `## Slice 12b — auth cleanup (2026-05-15)`

**Incomplete / deferred steps (out-of-scope follow-ups):**

- mTLS (closed by later mTLS slice)
- ACLs (closed by slice 13)
- Per-listener controller-quorum protocol mapping
- SCRAM rotation under live raft traffic

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish slice 12's two deferred items — raft transport over SASL/TLS, and `Broker::start` consumption of `crabka format --add-scram` bootstrap records.

**Architecture:** A new `RaftListenerHandshake` trait in `crabka-raft` lets the broker inject a SASL/TLS terminator on the controller listener's accept path; `crabka-broker` provides the impl that reuses slice 12's `network::auth` state machines. Slice 12's `InterBrokerDialer` adapter is moved from "constructed-but-unused" to wired into `ControllerConfig::dialer`. `Broker::start` reads `bootstrap.records.bin` on first start and submits its records after the broker becomes raft leader.

**Tech Stack:** Rust 1.95.0; reuses slice 12's `rustls 0.23` + `tokio-rustls 0.26` + `network::auth` + `InterBrokerClient`. No new dependencies.

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-auth-cleanup-12b-design.md`](../specs/2026-05-15-crabka-auth-cleanup-12b-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Implementation runs on `feature/auth-cleanup-12b` (already created off `main`; the spec is already committed).

---

## File structure

```
crates/raft/src/
├── config.rs          # MODIFIED — `handshake` field on ControllerConfig
├── handshake.rs       # NEW       — RaftListenerHandshake trait + DuplexStream re-export
├── lib.rs             # MODIFIED — re-export RaftListenerHandshake
├── controller.rs      # MODIFIED — thread handshake into server::run
└── server.rs          # MODIFIED — handle_conn generic over stream; call handshake.upgrade()

crates/broker/src/
├── config.rs          # MODIFIED — controller_listener_protocol field + validate()
├── error.rs           # MODIFIED — BootstrapFile variant
├── raft_handshake.rs  # NEW       — BrokerRaftHandshake impl (inbound TLS+SASL)
├── bootstrap.rs       # NEW       — load_bootstrap_records helper
├── lib.rs             # MODIFIED — mod raft_handshake; mod bootstrap
└── broker.rs          # MODIFIED — wire handshake + dialer; load bootstrap records

crates/broker/tests/
├── raft_sasl.rs               # NEW — 3 inbound-controller-auth tests
├── bootstrap_consumption.rs   # NEW — 3 format CLI -> broker tests
└── jvm_acceptance.rs          # MODIFIED — 1 new JVM test
```

The plan is structured in **5 batches**. Each batch ends in one commit. Batches build sequentially.

---

## Batch 1 — `crabka-raft` extensions

### Task 1: `RaftListenerHandshake` trait + module skeleton

**Files:**
- Create: `crates/raft/src/handshake.rs`
- Modify: `crates/raft/src/lib.rs`
- Modify: `crates/raft/Cargo.toml` (if `async-trait` / `tokio` features are missing — they shouldn't be)

- [ ] **Step 1: Write `crates/raft/src/handshake.rs`**

```rust
//! Pluggable inbound handshake for the controller listener.
//!
//! Slice 12b. Lets the broker terminate TLS + SASL on every accepted
//! controller-listener connection before raft frames start flowing. The
//! trait abstraction keeps `crabka-raft` free of any dependency on
//! `crabka-broker` / `crabka-security`.

use std::pin::Pin;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// Type-erased duplex stream returned by [`RaftListenerHandshake::upgrade`].
/// The raft connection handler is generic over `AsyncRead + AsyncWrite +
/// Unpin + Send + 'static`, so a `Box<dyn DuplexStream>` plugs in directly.
pub trait DuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> DuplexStream for T {}

#[derive(Debug, Error)]
pub enum RaftHandshakeError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Per-connection handshake hook. Implementations consume the raw
/// `TcpStream` and return either an authenticated `Box<dyn DuplexStream>`
/// (for raft frames to ride on) or a `RaftHandshakeError` (the listener
/// drops the connection at debug level).
pub trait RaftListenerHandshake: Send + Sync {
    fn upgrade<'a>(
        &'a self,
        stream: TcpStream,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Box<dyn DuplexStream>, RaftHandshakeError>>
                + Send
                + 'a,
        >,
    >;
}
```

(Using `Pin<Box<dyn Future>>` rather than `#[async_trait]` to avoid pulling `async-trait` if it isn't already in the raft crate's deps. If it is, swap for `#[async_trait]` for ergonomics — both compile.)

- [ ] **Step 2: Re-export from `crates/raft/src/lib.rs`**

Find the existing `pub use` block and add:

```rust
pub mod handshake;
pub use handshake::{DuplexStream, RaftHandshakeError, RaftListenerHandshake};
```

- [ ] **Step 3: Verify build**

Run: `cargo build -p crabka-raft`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/handshake.rs crates/raft/src/lib.rs
git commit -m "feat(raft): RaftListenerHandshake trait scaffold

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: `ControllerConfig::handshake` slot + thread into server::run

**Files:**
- Modify: `crates/raft/src/config.rs`
- Modify: `crates/raft/src/controller.rs`
- Modify: `crates/raft/src/server.rs`

- [ ] **Step 1: Add the field to `ControllerConfig`**

Open `crates/raft/src/config.rs`. Locate `pub dialer: Option<Arc<dyn OutboundDialer>>,` and add a sibling field below it:

```rust
    /// Optional inbound handshake hook. `None` means: feed every
    /// accepted TcpStream directly into the raft handler (legacy
    /// PLAINTEXT path). The broker injects a TLS + SASL terminator
    /// when `controller_listener_protocol != Plaintext`.
    pub handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
```

Update the manual `Debug` impl for `ControllerConfig` to include:

```rust
            .field("handshake", &self.handshake.as_ref().map(|_| "..."))
```

And any constructor / `Default` site must initialise `handshake: None`. Find via:

```bash
rg "ControllerConfig \{" crates/raft/
rg "ControllerConfig::default\|ControllerConfig\s*\{" crates/raft/
```

Add `handshake: None` to every literal.

- [ ] **Step 2: Thread the handshake through `Controller::start`**

In `crates/raft/src/controller.rs` around line 472 where the listener is bound + `server::run` is spawned:

```rust
        let listener = tokio::net::TcpListener::bind(config.controller_listen_addr)
            .await
            .map_err(RaftError::from)?;
        // Existing line:
        // let listener_task = tokio::spawn(server::run(listener, raft.clone(), shutdown.clone()));
        // NEW:
        let listener_task = tokio::spawn(crate::server::run(
            listener,
            raft.clone(),
            shutdown.clone(),
            config.handshake.clone(),
        ));
```

- [ ] **Step 3: Extend `server::run` signature**

In `crates/raft/src/server.rs`, change `run`:

```rust
pub(crate) async fn run(
    listener: TcpListener,
    raft: Arc<Raft>,
    shutdown: CancellationToken,
    handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
) {
    match listener.local_addr() {
        Ok(addr) => info!(%addr, "controller listener started"),
        Err(e) => info!(error = %e, "controller listener started (addr unknown)"),
    }
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let raft = raft.clone();
                        let shutdown_c = shutdown.clone();
                        let handshake_c = handshake.clone();
                        tokio::spawn(async move {
                            let upgraded = match handshake_c {
                                Some(h) => match h.upgrade(stream).await {
                                    Ok(boxed) => boxed,
                                    Err(e) => {
                                        tracing::debug!(%peer, error = %e, "raft handshake failed");
                                        return;
                                    }
                                },
                                None => Box::new(stream) as Box<dyn crate::DuplexStream>,
                            };
                            if let Err(e) = handle_conn(upgraded, raft, shutdown_c).await {
                                error!(%peer, error = %e, "controller connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "controller listener accept failed");
                    }
                }
            }
        }
    }
}
```

(Note `handle_conn`'s signature still takes `TcpStream` — Task 3 generalises it.)

- [ ] **Step 4: Run existing tests to confirm legacy path still works** (after Task 3 lands handle_conn will compile)

Skip — Task 2 alone won't compile because `Box<dyn DuplexStream>` isn't compatible with `handle_conn(TcpStream)`. Task 3 finishes the generalisation. Combine 2+3 into one commit if needed; the plan keeps them separate for diff clarity, but you can also let Task 2 introduce a temporary `unimplemented!()` or leave server.rs broken until Task 3.

**Preferred:** combine Task 2 and Task 3 into a single commit and only run tests at the end of Task 3. Skip the cargo check at the end of Task 2.

- [ ] **Step 5: Commit (deferred to Task 3)**

---

### Task 3: Generalise `handle_conn` over the stream type

**Files:**
- Modify: `crates/raft/src/server.rs`

- [ ] **Step 1: Make `handle_conn` generic**

```rust
async fn handle_conn<S>(
    mut stream: S,
    raft: Arc<Raft>,
    shutdown: CancellationToken,
) -> Result<(), RaftError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            res = read_one_request(&mut stream) => {
                // ... unchanged body
            }
        }
    }
}
```

Then update `read_one_request`, `write_response`, `write_response_no_tagged_fields`, and any other internal helpers in `server.rs` to be generic over `AsyncRead + AsyncWrite` rather than `TcpStream`. Likely already abstract via `AsyncReadExt` / `AsyncWriteExt` trait method calls — only the type parameter at the boundary changes.

- [ ] **Step 2: Drop unused `TcpStream` imports**

Remove `tokio::net::TcpStream` from `server.rs` imports if no longer referenced. Keep `TcpListener` (still used by `run`).

- [ ] **Step 3: Confirm build**

Run: `cargo build -p crabka-raft`
Expected: clean.

- [ ] **Step 4: Confirm existing tests pass**

```bash
cargo test -p crabka-raft
```

Expected: PASS. The PLAINTEXT path through `Box<TcpStream> as Box<dyn DuplexStream>` is byte-identical to the old path.

```bash
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests integration unit
```

Expected: PASS (slice 12 multi-broker tests in `auth_handlers.rs::two_broker_sasl` should still converge).

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/config.rs crates/raft/src/controller.rs crates/raft/src/server.rs
git commit -m "feat(raft): handshake hook + generic stream type on handle_conn

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — Broker-side handshake adapter

### Task 4: `BrokerConfig.controller_listener_protocol` + validation

**Files:**
- Modify: `crates/broker/src/config.rs`

- [ ] **Step 1: Add the field**

Locate `pub listeners: Vec<ListenerSpec>` in the `BrokerConfig` struct (added in slice 12 T7). Add:

```rust
    /// Protocol terminator for the controller listener. Default
    /// `Plaintext` preserves the legacy raw-TCP raft transport.
    /// Set to `SaslPlaintext` / `Ssl` / `SaslSsl` to require auth
    /// on inbound raft RPCs (and outbound, when paired with
    /// `inter_broker_credentials`).
    pub controller_listener_protocol: crabka_security::ListenerProtocol,
```

- [ ] **Step 2: Default to Plaintext**

In `Default` and `for_tests` initialisers:

```rust
controller_listener_protocol: crabka_security::ListenerProtocol::Plaintext,
```

- [ ] **Step 3: Extend `validate()`**

Find the existing `pub fn validate(&self) -> Result<(), BrokerError>` (slice 12 T8). Add to its body:

```rust
        let cp = self.controller_listener_protocol;
        if cp.requires_tls() && self.tls_config.is_none() {
            return Err(BrokerError::Tls(
                "controller_listener_protocol requires TLS but tls_config is None".into(),
            ));
        }
        if cp.requires_sasl() && self.enabled_sasl_mechanisms.is_empty() {
            return Err(BrokerError::SaslListenerNoMechanisms {
                name: "controller".into(),
            });
        }
```

- [ ] **Step 4: Add failing tests in the existing `#[cfg(test)] mod tests` block**

```rust
    #[test]
    fn rejects_controller_tls_without_config() {
        let mut c = BrokerConfig::default();
        c.controller_listener_protocol = ListenerProtocol::Ssl;
        c.tls_config = None;
        assert!(matches!(c.validate(), Err(BrokerError::Tls(_))));
    }

    #[test]
    fn rejects_controller_sasl_without_mechanisms() {
        let mut c = BrokerConfig::default();
        c.controller_listener_protocol = ListenerProtocol::SaslPlaintext;
        c.enabled_sasl_mechanisms = vec![];
        assert!(matches!(
            c.validate(),
            Err(BrokerError::SaslListenerNoMechanisms { .. })
        ));
    }

    #[test]
    fn legacy_default_still_passes() {
        BrokerConfig::default().validate().expect("legacy default validates");
    }
```

- [ ] **Step 5: Run tests**

```bash
cargo test -p crabka-broker --lib config
```

Expected: PASS (3 new tests).

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/config.rs
git commit -m "feat(broker): controller_listener_protocol on BrokerConfig

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 5: `BrokerRaftHandshake` (inbound TLS + SASL)

**Files:**
- Create: `crates/broker/src/raft_handshake.rs`
- Modify: `crates/broker/src/lib.rs`

This task ports the inbound SASL frame-driving code from
`crates/broker/src/network/dispatch.rs` (slice 12 T12-T14) into a new
module that implements `RaftListenerHandshake`. The state-machine logic
is shared with `network::auth`, so we call those handlers directly.

- [ ] **Step 1: Write `crates/broker/src/raft_handshake.rs`**

```rust
//! Inbound TLS + SASL handshake for the controller listener.
//!
//! Slice 12b. Mirror image of `network::client::InterBrokerClient`'s
//! outbound auth flow. Reuses `network::auth::handle_handshake` +
//! `handle_authenticate_*` state machines so the controller listener
//! and data plane share one source of truth.

#![allow(dead_code)] // exercised via the runtime path; tests use the broker.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
use crabka_protocol::{Decode, Encode};
use crabka_raft::{DuplexStream, RaftHandshakeError, RaftListenerHandshake};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use crate::controller::ControllerHandleArc; // see step 3 for the alias
use crate::network::auth::{
    handle_authenticate_plain, handle_authenticate_scram, handle_handshake, is_pre_auth_allowed,
    ConnectionAuth,
};

/// Per-broker handshake adapter. Constructed in `Broker::start` and
/// passed into `ControllerConfig::handshake`.
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub protocol: ListenerProtocol,
    pub controller: ControllerHandleArc,
}

impl BrokerRaftHandshake {
    fn pre_auth_state(&self) -> ConnectionAuth {
        ConnectionAuth::Anonymous
    }
}

impl RaftListenerHandshake for BrokerRaftHandshake {
    fn upgrade<'a>(
        &'a self,
        stream: TcpStream,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Box<dyn DuplexStream>, RaftHandshakeError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let stream: Box<dyn DuplexStream> = if self.protocol.requires_tls() {
                let acceptor = self.tls_acceptor.clone().ok_or_else(|| {
                    RaftHandshakeError::Tls("tls_config required for TLS controller listener".into())
                })?;
                let tls = acceptor
                    .accept(stream)
                    .await
                    .map_err(|e| RaftHandshakeError::Tls(e.to_string()))?;
                Box::new(tls)
            } else {
                Box::new(stream)
            };
            if self.protocol.requires_sasl() {
                let mut stream = stream;
                run_inbound_sasl(&mut *stream, self).await?;
                Ok(stream)
            } else {
                Ok(stream)
            }
        })
    }
}

async fn run_inbound_sasl(
    stream: &mut dyn DuplexStream,
    cfg: &BrokerRaftHandshake,
) -> Result<(), RaftHandshakeError> {
    let mut auth = cfg.pre_auth_state();
    loop {
        let (api_key, api_version, corr_id, body) = read_kafka_request(stream).await?;
        if !is_pre_auth_allowed(api_key) && !auth.is_authenticated() {
            return Err(RaftHandshakeError::Sasl(format!(
                "pre-auth request api_key={api_key} rejected"
            )));
        }
        match api_key {
            // ApiVersions — minimal response so peers that send it first
            // (typical pattern) can proceed.
            18 => {
                let resp_bytes = build_api_versions_response(corr_id, &cfg.enabled_sasl_mechanisms)?;
                stream.write_all(&resp_bytes).await?;
            }
            17 => {
                let req = SaslHandshakeRequest::decode(&mut body.as_slice(), api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let resp =
                    handle_handshake(&req, &mut auth, &cfg.enabled_sasl_mechanisms);
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
                if resp.error_code != 0 {
                    return Err(RaftHandshakeError::Sasl(format!(
                        "handshake error_code={}",
                        resp.error_code
                    )));
                }
            }
            36 => {
                let req = SaslAuthenticateRequest::decode(&mut body.as_slice(), api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let mech = match &auth {
                    ConnectionAuth::Negotiating { mechanism, .. } => *mechanism,
                    _ => {
                        return Err(RaftHandshakeError::Sasl(
                            "authenticate before handshake".into(),
                        ));
                    }
                };
                let resp = match mech {
                    SaslMechanism::Plain => handle_authenticate_plain(
                        &req,
                        &mut auth,
                        &cfg.plain_credentials,
                    ),
                    SaslMechanism::ScramSha512 => handle_authenticate_scram(
                        &req,
                        &mut auth,
                        &cfg.controller,
                    ),
                };
                write_response(stream, api_key, api_version, corr_id, &resp).await?;
                if resp.error_code != 0 {
                    return Err(RaftHandshakeError::Sasl(format!(
                        "authenticate error_code={}",
                        resp.error_code
                    )));
                }
                if auth.is_authenticated() {
                    return Ok(());
                }
                // SCRAM second round — continue the loop and read the
                // next SaslAuthenticate frame.
            }
            other => {
                return Err(RaftHandshakeError::Protocol(format!(
                    "unexpected api_key={other} during handshake"
                )));
            }
        }
    }
}

// Frame helpers: read length-prefixed Kafka request, decode header v1/v2,
// return body bytes. Same shape as the helpers in
// `crates/broker/src/network/client.rs::run_outbound_sasl` (slice 12 T16),
// but inverted: server-side decode rather than client-side.

async fn read_kafka_request(
    stream: &mut dyn DuplexStream,
) -> Result<(i16, i16, i32, Vec<u8>), RaftHandshakeError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_be_bytes(size_buf) as usize;
    let mut frame = vec![0u8; size];
    stream.read_exact(&mut frame).await?;
    // RequestHeader v1: api_key i16, api_version i16, corr_id i32,
    //                   client_id_len i16, client_id bytes.
    if frame.len() < 10 {
        return Err(RaftHandshakeError::Protocol("short header".into()));
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let corr_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let client_id_len = i16::from_be_bytes([frame[8], frame[9]]);
    let mut cursor = 10;
    if client_id_len >= 0 {
        cursor += client_id_len as usize;
    }
    // For flexible-header api_keys (36 v2+), skip the tagged-fields byte.
    if api_key == 36 && api_version >= 2 {
        if frame.len() <= cursor {
            return Err(RaftHandshakeError::Protocol("missing tag byte".into()));
        }
        cursor += 1;
    }
    let body = frame[cursor..].to_vec();
    Ok((api_key, api_version, corr_id, body))
}

async fn write_response<R: Encode>(
    stream: &mut dyn DuplexStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    resp: &R,
) -> Result<(), RaftHandshakeError> {
    let flexible = is_response_header_flexible(api_key, api_version);
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    let mut frame = Vec::with_capacity(8 + body.len());
    frame.extend_from_slice(&corr_id.to_be_bytes());
    if flexible {
        frame.push(0);
    }
    frame.extend_from_slice(&body);
    let mut out = Vec::with_capacity(4 + frame.len());
    out.extend_from_slice(&(frame.len() as u32).to_be_bytes());
    out.extend_from_slice(&frame);
    stream.write_all(&out).await?;
    Ok(())
}

fn is_response_header_flexible(api_key: i16, api_version: i16) -> bool {
    match (api_key, api_version) {
        (17, _) => false,                  // SaslHandshake non-flexible
        (36, v) => v >= 2,                  // SaslAuthenticate flexible from v2
        (18, _) => false,                  // ApiVersions response is always v0 header
        _ => false,
    }
}

fn build_api_versions_response(
    corr_id: i32,
    enabled: &[SaslMechanism],
) -> Result<Vec<u8>, RaftHandshakeError> {
    // Tiny stub: only api_keys 17 / 36 / 18 advertised, matching the
    // pre-auth allowlist. Real peers (InterBrokerClient) skip ApiVersions,
    // so this exists only to be tolerant.
    let _ = enabled;
    let body: Vec<u8> = {
        // error_code=0, num_api_keys=3, [api_key, min, max] * 3, throttle=0
        // Non-flexible v0: i16 + i32 array + i32 throttle.
        let mut v = Vec::new();
        v.extend_from_slice(&0i16.to_be_bytes()); // error_code
        v.extend_from_slice(&3i32.to_be_bytes()); // array len
        for k in [17i16, 36, 18] {
            v.extend_from_slice(&k.to_be_bytes());
            v.extend_from_slice(&0i16.to_be_bytes()); // min
            v.extend_from_slice(&2i16.to_be_bytes()); // max
        }
        v.extend_from_slice(&0i32.to_be_bytes()); // throttle
        v
    };
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&corr_id.to_be_bytes());
    frame.extend_from_slice(&body);
    let mut out = Vec::with_capacity(4 + frame.len());
    out.extend_from_slice(&(frame.len() as u32).to_be_bytes());
    out.extend_from_slice(&frame);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_security::{Principal, SaslMechanism};
    use tokio::io::{duplex, AsyncReadExt as _, AsyncWriteExt as _};

    fn handshake_with_plain(creds: &[(&str, &str)]) -> BrokerRaftHandshake {
        let mut map = HashMap::new();
        for (u, p) in creds {
            map.insert((*u).into(), (*p).into());
        }
        BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials: map,
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            protocol: ListenerProtocol::SaslPlaintext,
            controller: dummy_controller(),
        }
    }

    fn dummy_controller() -> ControllerHandleArc {
        // The PLAIN tests don't touch the controller. Reuse the broker's
        // existing test helper that constructs a single-node controller
        // shell. If `dummy_controller` doesn't exist as a helper, build one
        // here via `Controller::start(...).await` against a tempdir.
        unimplemented!("see step 3 — build a tempdir-backed controller for tests")
    }

    // Note: full state-machine exercise lives in tests/raft_sasl.rs (T9)
    // since it requires a running raft. This unit test layer only proves
    // the trait wiring + that PLAINTEXT short-circuits cleanly.

    #[tokio::test]
    async fn plaintext_passthrough_short_circuits() {
        let cfg = BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials: HashMap::new(),
            enabled_sasl_mechanisms: vec![],
            protocol: ListenerProtocol::Plaintext,
            controller: dummy_controller(),
        };
        // `upgrade(TcpStream)` requires a real TCP socket; we can't easily
        // exercise the trait-object method without one. So just verify the
        // Plaintext branch directly via the internal logic:
        assert!(!cfg.protocol.requires_tls());
        assert!(!cfg.protocol.requires_sasl());
    }
}
```

NOTE: the `dummy_controller()` helper is awkward in pure-unit tests. The richer behavioural tests live in Task 9 (`tests/raft_sasl.rs`) where a real two-broker raft cluster is spun up. For the unit tests in this file, keep coverage narrow: just confirm `protocol.requires_*` predicates short-circuit correctly. Remove the `handshake_with_plain` helper if it isn't needed by the final tests.

- [ ] **Step 2: Register the module**

In `crates/broker/src/lib.rs`:

```rust
pub mod raft_handshake;
```

- [ ] **Step 3: Define the `ControllerHandleArc` alias**

In `crates/broker/src/broker.rs` (or wherever the existing `Controller` is built), confirm the type — most likely `Arc<crabka_raft::ControllerHandle>`. Add a `pub(crate) type ControllerHandleArc = Arc<crabka_raft::ControllerHandle>;` alias in `crates/broker/src/controller.rs` (or `broker.rs`), or just inline the full path in `raft_handshake.rs`.

- [ ] **Step 4: Confirm build**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: clean. (Some `dead_code` warnings on the `tests` module's helpers are fine while T9 hasn't wired up the rich tests.)

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/raft_handshake.rs crates/broker/src/lib.rs
git commit -m "feat(broker): BrokerRaftHandshake inbound TLS+SASL adapter

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 6: Wire `BrokerRaftHandshake` + `InterBrokerDialer` into `Broker::start`

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Construct the handshake + assign to `ControllerConfig`**

Locate where `ControllerConfig` is built in `Broker::start`. Find the existing
`dialer: None,` (or whatever the slice-12 wiring left). Replace the block:

```rust
        let inter_broker_dialer = Arc::new(crate::network::client::InterBrokerDialer::new(
            inter_broker_client.clone(),
            config.controller_listener_protocol,
            inter_broker_server_name.clone(),
        ));

        let controller_handshake: Option<Arc<dyn crabka_raft::RaftListenerHandshake>> =
            if config.controller_listener_protocol == crabka_security::ListenerProtocol::Plaintext {
                None
            } else {
                Some(Arc::new(crate::raft_handshake::BrokerRaftHandshake {
                    tls_acceptor: tls_acceptor.clone(),
                    plain_credentials: config.plain_credentials.clone(),
                    enabled_sasl_mechanisms: config.enabled_sasl_mechanisms.clone(),
                    protocol: config.controller_listener_protocol,
                    controller: controller.clone(),
                }))
            };

        let controller_config = crabka_raft::ControllerConfig {
            // ... existing fields ...
            dialer: Some(inter_broker_dialer.clone() as Arc<dyn crabka_raft::OutboundDialer>),
            handshake: controller_handshake,
        };
```

**Catch**: the controller is constructed AFTER `ControllerConfig`, so `controller.clone()` for the handshake isn't available yet. Resolve by either:

- (a) Construct an `Arc<OnceCell<ControllerHandle>>` first, pass it to the handshake, and fill it after the controller starts. Pattern matches the heartbeat client (slice 12).
- (b) Re-order: build the controller, THEN build the handshake, THEN call `controller.set_handshake(...)` if such a setter exists. If not, the handshake must be passed at controller-start time, so option (a) is the natural fit.

Use approach (a): the handshake holds an `Arc<tokio::sync::OnceCell<Arc<ControllerHandle>>>` and looks up the controller lazily on each `upgrade` call. This is necessary because raft connections start arriving before / during controller construction.

If the controller is already ready before the listener is spawned (verify by reading `Controller::start` in `crates/raft/src/controller.rs`), the simpler synchronous pass works. Pick whichever matches the actual lifecycle.

- [ ] **Step 2: Choose the lifecycle approach and update `BrokerRaftHandshake`**

If you went with `OnceCell`:

```rust
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub protocol: ListenerProtocol,
    pub controller: Arc<tokio::sync::OnceCell<Arc<crabka_raft::ControllerHandle>>>,
}
```

…and in the SCRAM handler call site, `cfg.controller.get().await` (or similar).

- [ ] **Step 3: Run all broker tests**

```bash
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests integration unit auth_handlers
```

Expected: PASS. The default `controller_listener_protocol = Plaintext` means `controller_handshake = None`, so every existing test continues exercising the legacy path. The dialer is non-`None`, but its `dial` impl with `Plaintext` listener is byte-identical to a raw `TcpStream::connect` (verified in slice 12).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/broker.rs crates/broker/src/raft_handshake.rs
git commit -m "feat(broker): wire BrokerRaftHandshake + InterBrokerDialer into start

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Bootstrap-records loader

### Task 7: `load_bootstrap_records` helper + unit tests

**Files:**
- Create: `crates/broker/src/bootstrap.rs`
- Modify: `crates/broker/src/lib.rs`
- Modify: `crates/broker/src/error.rs`

- [ ] **Step 1: Add the `BootstrapFile` error variant**

In `crates/broker/src/error.rs`, append a variant to `BrokerError`:

```rust
    #[error("bootstrap file {path:?}: {source}")]
    BootstrapFile {
        path: std::path::PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
```

If a sibling `crates/broker/src/codes.rs::from_broker_error` exhaustively matches `BrokerError`, add a `BrokerError::BootstrapFile { .. } => UNKNOWN_SERVER_ERROR` arm (broker won't reach the codec path, but the match still needs the variant).

- [ ] **Step 2: Write `crates/broker/src/bootstrap.rs`**

```rust
//! Slice 12b. Read `bootstrap.records.bin` (produced by
//! `crabka format --add-scram`) on broker first start.
//!
//! File framing (matches `crates/cli/src/format.rs`):
//!   [u32_le length][serde_wincode-encoded MetadataRecord]
//! Repeated until EOF.

use std::path::Path;

use crabka_metadata::MetadataRecord;
use serde_wincode::SerdeCompat;
use wincode::Deserialize;

use crate::error::BrokerError;

pub fn load_bootstrap_records(log_dir: &Path) -> Result<Vec<MetadataRecord>, BrokerError> {
    let path = log_dir.join("bootstrap.records.bin");
    if !path.exists() {
        return Ok(vec![]);
    }
    let bytes = std::fs::read(&path).map_err(|e| BrokerError::BootstrapFile {
        path: path.clone(),
        source: Box::new(e),
    })?;
    let mut out = Vec::new();
    let mut cur = &bytes[..];
    while !cur.is_empty() {
        if cur.len() < 4 {
            return Err(BrokerError::BootstrapFile {
                path: path.clone(),
                source: "truncated length prefix".into(),
            });
        }
        let len = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
        cur = &cur[4..];
        if cur.len() < len {
            return Err(BrokerError::BootstrapFile {
                path: path.clone(),
                source: "truncated record body".into(),
            });
        }
        let rec = <SerdeCompat<MetadataRecord>>::deserialize(&cur[..len])
            .map_err(|e| BrokerError::BootstrapFile {
                path: path.clone(),
                source: Box::new(std::io::Error::other(format!("decode: {e}"))),
            })?;
        out.push(rec);
        cur = &cur[len..];
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::records::ScramCredentialRecord;
    use crabka_security::SaslMechanism;
    use serde_wincode::SerdeCompat;
    use std::io::Write;
    use wincode::Serialize;

    fn write_frame(out: &mut Vec<u8>, rec: &MetadataRecord) {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(rec).unwrap();
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }

    #[test]
    fn returns_empty_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn decodes_v1_scram_credential() {
        let dir = tempfile::tempdir().unwrap();
        let rec = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        });
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &rec);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert_eq!(got.len(), 1);
        match &got[0] {
            MetadataRecord::V1ScramCredential(r) => assert_eq!(r.user, "alice"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn refuses_truncated_length_prefix() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bootstrap.records.bin"), [0u8, 0u8, 0u8]).unwrap();
        let err = load_bootstrap_records(dir.path()).unwrap_err();
        assert!(matches!(err, BrokerError::BootstrapFile { .. }));
    }

    #[test]
    fn refuses_truncated_record_body() {
        let dir = tempfile::tempdir().unwrap();
        // Length prefix says 100 bytes follow; only write 4.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 4]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }

    #[test]
    fn refuses_undecodable_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = Vec::new();
        // Length prefix=8, body=random bytes that aren't valid bincode for MetadataRecord.
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&[0xFFu8; 8]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }
}
```

- [ ] **Step 3: Register the module**

In `crates/broker/src/lib.rs`:

```rust
pub mod bootstrap;
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-broker --lib bootstrap
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/bootstrap.rs crates/broker/src/lib.rs crates/broker/src/error.rs
git commit -m "feat(broker): load_bootstrap_records reader + BrokerError::BootstrapFile

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 8: Submit bootstrap records on first start

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Locate the bootstrap branch**

In `Broker::start`, find where the broker enters `BootstrapMode::Bootstrap` handling. The controller is spawned with `bootstrap_mode: config.bootstrap_mode`, which raft uses to drive `initialize`. After raft initialize completes (broker becomes leader), submit the bootstrap records.

- [ ] **Step 2: Load + submit the records**

After the spot where `controller_leader_id()` first returns `Some(self_id)` (the broker has become raft leader on a fresh bootstrap), add:

```rust
        if matches!(config.bootstrap_mode, crabka_broker::BootstrapMode::Bootstrap) {
            let records = crate::bootstrap::load_bootstrap_records(&config.log_dir)?;
            if !records.is_empty() {
                tracing::info!(count = records.len(), "submitting bootstrap records");
                if let Err(e) = controller.submit_change(records).await {
                    return Err(BrokerError::Replication(format!(
                        "bootstrap submit failed: {e}"
                    )));
                }
            }
        }
```

Concrete placement: if `Broker::start` doesn't already block on leader-election, add a short loop that polls `controller.watch_leader().borrow()` until it sees `Some(self_id)` (with a timeout — the existing slice-7 broker startup likely already has a similar wait). The exact integration point is implementation-specific; the load + submit logic above is the load-bearing part.

- [ ] **Step 3: Confirm existing tests pass**

```bash
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests integration unit auth_handlers
```

Expected: PASS. No legacy path uses `bootstrap.records.bin`, so all existing tests see `records.is_empty()` and skip the submit.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "feat(broker): submit bootstrap.records.bin on first start

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Integration tests

### Task 9: `tests/raft_sasl.rs` — 3 inbound-controller-auth tests

**Files:**
- Create: `crates/broker/tests/raft_sasl.rs`

This task uses the existing two-broker test scaffolding from
slice 12 T17 (`crates/broker/tests/auth_handlers.rs::two_broker_sasl`).
Copy the helper that starts two brokers, then vary the listener config.

- [ ] **Step 1: Write the test file**

```rust
//! Slice 12b. Inbound raft listener auth tests.
//!
//! These exercise the controller listener under `SaslPlaintext` and prove
//! both inbound (broker A accepts auth'd raft frames from broker B) and
//! outbound (`InterBrokerDialer` dials with SASL credentials) paths
//! work together. Gated `#[cfg(not(target_os = "windows"))]` per the
//! existing multi-broker test convention.

#![cfg(not(target_os = "windows"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
use crabka_broker::{Broker, BrokerConfig};
use crabka_security::{ListenerProtocol, SaslMechanism};

// `start_two_brokers_with_controller_protocol` is a local helper that
// mirrors slice 12's `start_two_sasl_brokers` (auth_handlers.rs) but
// adds a `controller_listener_protocol` parameter so each test picks the
// transport for the controller independently from the data plane.
async fn start_two_brokers_with_controller_protocol(
    ctrl: ListenerProtocol,
    plain_user: &str,
    plain_pass: &str,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let dir1 = tempfile::tempdir().expect("tempdir1");
    let dir2 = tempfile::tempdir().expect("tempdir2");
    let creds = {
        let mut m = HashMap::new();
        m.insert(plain_user.into(), plain_pass.into());
        m
    };
    let make = |node_id: u64, broker_id: i32, listen: &str, ctrl_listen: &str, dir: &std::path::Path| -> BrokerConfig {
        let mut c = BrokerConfig::for_tests(broker_id, dir.to_path_buf());
        c.listeners = vec![ListenerSpec {
            name: "SASL_PLAINTEXT".into(),
            bind_addr: listen.parse().unwrap(),
            advertised: listen.into(),
            protocol: ListenerProtocol::SaslPlaintext,
        }];
        c.inter_broker_listener_name = "SASL_PLAINTEXT".into();
        c.controller_listener_protocol = ctrl;
        c.controller_listen_addr = ctrl_listen.parse().unwrap();
        c.controller_quorum_voters = vec![
            (1, "127.0.0.1:9091".parse().unwrap()),
            (2, "127.0.0.1:9092".parse().unwrap()),
        ];
        c.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        c.plain_credentials = creds.clone();
        c.inter_broker_credentials = Some(InterBrokerCredentials {
            mechanism: SaslMechanism::Plain,
            username: plain_user.into(),
            password: plain_pass.into(),
        });
        c.node_id = node_id;
        c.bootstrap_mode = if node_id == 1 {
            crabka_broker::BootstrapMode::Bootstrap
        } else {
            crabka_broker::BootstrapMode::Join
        };
        c
    };
    let b1 = Broker::start(make(1, 1, "127.0.0.1:0", "127.0.0.1:9091", dir1.path()))
        .await
        .expect("start broker 1");
    let b2 = Broker::start(make(2, 2, "127.0.0.1:0", "127.0.0.1:9092", dir2.path()))
        .await
        .expect("start broker 2");
    (b1, b2, dir1, dir2)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_two_broker_quorum() {
    let (b1, b2, _d1, _d2) =
        start_two_brokers_with_controller_protocol(ListenerProtocol::SaslPlaintext, "broker", "secret")
            .await;
    // Wait until both brokers see two registered peers in the metadata
    // image. Bounded wait — fail the test if convergence takes too long.
    let converge = async {
        loop {
            if b1.broker_count().await == 2 && b2.broker_count().await == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(15), converge)
        .await
        .expect("brokers converge on 2-broker quorum within 15s");
    b1.shutdown().await;
    b2.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_rejects_mismatched_creds() {
    // Start broker A with username=alice; broker B with username=bob.
    // Neither has the other's password, so inbound raft auth fails on
    // both sides. Expect they never converge.
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let mut c1 = BrokerConfig::for_tests(1, dir1.path().to_path_buf());
    let mut c2 = BrokerConfig::for_tests(2, dir2.path().to_path_buf());
    c1.controller_listener_protocol = ListenerProtocol::SaslPlaintext;
    c2.controller_listener_protocol = ListenerProtocol::SaslPlaintext;
    c1.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    c2.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    c1.plain_credentials.insert("alice".into(), "wonderland".into());
    c2.plain_credentials.insert("bob".into(), "burgers".into());
    c1.inter_broker_credentials = Some(InterBrokerCredentials {
        mechanism: SaslMechanism::Plain,
        username: "alice".into(),
        password: "wonderland".into(),
    });
    c2.inter_broker_credentials = Some(InterBrokerCredentials {
        mechanism: SaslMechanism::Plain,
        username: "bob".into(),
        password: "burgers".into(),
    });
    c1.node_id = 1;
    c2.node_id = 2;
    c1.bootstrap_mode = crabka_broker::BootstrapMode::Bootstrap;
    c2.bootstrap_mode = crabka_broker::BootstrapMode::Join;
    c1.controller_listen_addr = "127.0.0.1:9094".parse().unwrap();
    c2.controller_listen_addr = "127.0.0.1:9095".parse().unwrap();
    c1.controller_quorum_voters = vec![
        (1, "127.0.0.1:9094".parse().unwrap()),
        (2, "127.0.0.1:9095".parse().unwrap()),
    ];
    c2.controller_quorum_voters = c1.controller_quorum_voters.clone();
    let b1 = Broker::start(c1).await.expect("start b1");
    let b2 = Broker::start(c2).await.expect("start b2");
    // Wait 3 seconds and assert NO convergence.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        b1.broker_count().await < 2,
        "mismatched creds must not converge"
    );
    b1.shutdown().await;
    b2.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_plaintext_legacy_path_unchanged() {
    // Default `controller_listener_protocol = Plaintext` — no
    // handshake injected. Two brokers converge as in slice 7.
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let mut c1 = BrokerConfig::for_tests(1, dir1.path().to_path_buf());
    let mut c2 = BrokerConfig::for_tests(2, dir2.path().to_path_buf());
    c1.controller_listener_protocol = ListenerProtocol::Plaintext;
    c2.controller_listener_protocol = ListenerProtocol::Plaintext;
    c1.node_id = 1;
    c2.node_id = 2;
    c1.bootstrap_mode = crabka_broker::BootstrapMode::Bootstrap;
    c2.bootstrap_mode = crabka_broker::BootstrapMode::Join;
    c1.controller_listen_addr = "127.0.0.1:9096".parse().unwrap();
    c2.controller_listen_addr = "127.0.0.1:9097".parse().unwrap();
    c1.controller_quorum_voters = vec![
        (1, "127.0.0.1:9096".parse().unwrap()),
        (2, "127.0.0.1:9097".parse().unwrap()),
    ];
    c2.controller_quorum_voters = c1.controller_quorum_voters.clone();
    let b1 = Broker::start(c1).await.expect("start b1");
    let b2 = Broker::start(c2).await.expect("start b2");
    let converge = async {
        loop {
            if b1.broker_count().await == 2 && b2.broker_count().await == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), converge)
        .await
        .expect("legacy plaintext path still converges");
    b1.shutdown().await;
    b2.shutdown().await;
}
```

- [ ] **Step 2: Run tests in WSL or on Linux**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test raft_sasl -- --nocapture --test-threads=1"
```

Expected: 3 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/raft_sasl.rs
git commit -m "test(broker): raft_sasl two-broker controller-listener tests

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 10: `tests/bootstrap_consumption.rs` — 3 format CLI → broker tests

**Files:**
- Create: `crates/broker/tests/bootstrap_consumption.rs`

- [ ] **Step 1: Write the test file**

```rust
//! Slice 12b. `crabka format --add-scram` -> broker bootstrap consumption.
//!
//! Each test produces a log_dir via the format CLI (or by writing the
//! `bootstrap.records.bin` file directly) and starts a broker pointed
//! at that dir. We then verify the seeded SCRAM credential is usable
//! via SASL/SCRAM auth.

#![cfg(not(target_os = "windows"))]

use std::process::Command;

use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerConfig};
use crabka_security::{ListenerProtocol, SaslMechanism};

fn run_crabka_format(log_dir: &std::path::Path, add_scram: &str) {
    let bin = env!("CARGO_BIN_EXE_crabka");
    let out = Command::new(bin)
        .args([
            "format",
            "--log-dir",
            log_dir.to_str().unwrap(),
            "--add-scram",
            add_scram,
        ])
        .output()
        .expect("spawn crabka format");
    assert!(
        out.status.success(),
        "crabka format failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_records_provisions_scram_user() {
    let dir = tempfile::tempdir().unwrap();
    run_crabka_format(
        dir.path(),
        "SCRAM-SHA-512=[name=alice,password=wonderland,iterations=4096]",
    );

    let mut cfg = BrokerConfig::for_tests(1, dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".into();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];
    cfg.bootstrap_mode = crabka_broker::BootstrapMode::Bootstrap;

    let handle = Broker::start(cfg).await.expect("start");
    let addr = handle.listen_addr();

    // Reuse the slice-12 SCRAM-client helper from auth_handlers.rs.
    // For test brevity, inline the call here — copy `drive_sasl_scram_session`
    // if it isn't pub-accessible from a sibling integration test.
    let result = drive_sasl_scram_session(addr, "alice", "wonderland").await;
    assert!(
        result.is_ok(),
        "alice/wonderland should authenticate via SCRAM: {result:?}"
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_bootstrap_refuses_start() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("bootstrap.records.bin"),
        b"this is not a length-prefixed metadata record",
    )
    .unwrap();
    let cfg = BrokerConfig::for_tests(1, dir.path().to_path_buf());
    // BootstrapMode defaults to Bootstrap for `for_tests`; if not, set it.
    let result = Broker::start(cfg).await;
    assert!(
        matches!(result, Err(crabka_broker::BrokerError::BootstrapFile { .. })),
        "expected BootstrapFile error, got {result:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_absent_legacy_path() {
    // No bootstrap.records.bin written. Existing fresh-bootstrap behavior
    // unchanged — single-broker cluster comes up.
    let dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(1, dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("start");
    handle.shutdown().await;
}

// Copy of slice 12 T14's `drive_sasl_scram_session` since cargo integration
// tests can't share helpers across files without a `helpers` module.
async fn drive_sasl_scram_session(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
) -> std::io::Result<()> {
    // The body is the same as in `tests/auth_handlers.rs`. To keep this
    // plan focused, inline it from there: ApiVersions -> SaslHandshake
    // (SCRAM-SHA-512) -> SaslAuthenticate (client-first) -> SaslAuthenticate
    // (client-final) -> verify Authenticated.
    // Refer to `auth_handlers.rs` lines tagged `// drive_sasl_scram_session`
    // and copy verbatim.
    let _ = (addr, user, pass);
    unimplemented!("inline drive_sasl_scram_session from auth_handlers.rs")
}
```

NOTE: replace the `unimplemented!()` by copying the body of
`drive_sasl_scram_session` from `tests/auth_handlers.rs` (slice 12 T14).
Or factor it out into a `tests/common/mod.rs` helper consumed by both
integration test files. The latter is cleaner; pick that if Rust's
integration-test conventions allow it (`tests/common/mod.rs` is the
canonical pattern for shared helpers).

- [ ] **Step 2: Run tests**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test bootstrap_consumption -- --nocapture --test-threads=1"
```

Expected: 3 PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/bootstrap_consumption.rs crates/broker/tests/common/mod.rs
git commit -m "test(broker): bootstrap_consumption end-to-end format CLI -> SCRAM auth

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 11: JVM `jvm_inter_broker_sasl_ssl_raft_replication`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

Two brokers; both controller listeners AND data-plane listeners are
`SASL_SSL`. JVM client produces rf=2; both broker logs must reach
offset N. This is the production-shape end-to-end demo.

- [ ] **Step 1: Write the test**

Append to `jvm_acceptance.rs`:

```rust
/// Slice 12b: two-broker SASL_SSL cluster with controller_listener_protocol =
/// SaslSsl. Provisions a SCRAM user, produces rf=2 via JVM client, asserts
/// both brokers replicate the records. Supersedes slice 12 T23's simplified
/// inter-broker test which only proved metadata convergence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_inter_broker_sasl_ssl_raft_replication() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";
    const TOPIC: &str = "crabka-sasl-ssl-raft-rf2";

    let (b1, b2, _d1, _d2) = start_two_sasl_ssl_brokers_with_controller_protocol(
        ListenerProtocol::SaslSsl,
        ADMIN,
        ADMIN_PASS,
    )
    .await;
    let truststore = prepare_jks_truststore();
    nc_check_connectivity();

    // Provision alice via admin/PLAIN over SASL_SSL on cp-kafka:7.5.0.
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[
            &admin_props.mount_str(),
            &format!("{}:/truststore.jks:ro", truststore.display()),
        ],
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            ALICE,
            "--add-config",
            &format!("SCRAM-SHA-512=[password={ALICE_PASS}]"),
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );

    // Create topic rf=2 and produce 50 records as alice.
    // ... mirror jvm_sasl_ssl_full_stack pattern from slice 12 T23 ...

    // Assert both brokers reach offset 50 on partition 0.
    // ... use existing test helpers like `partition_log_end_offset_for_test` ...
}

async fn start_two_sasl_ssl_brokers_with_controller_protocol(
    ctrl: ListenerProtocol,
    admin: &str,
    admin_pass: &str,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    // Build on `start_sasl_ssl_broker` from slice 12 T23. Two brokers,
    // both with SASL_SSL data plane + ctrl controller-listener-protocol.
    // tls_config points at crates/security/tests/fixtures/dev_{cert,key}.pem.
    // ... full body ...
    unimplemented!("compose from slice 12 T23 helpers")
}
```

Replace `unimplemented!()` with the actual two-broker setup, modelled on
`start_two_sasl_brokers` from slice 12 T23 plus the SASL_SSL flags from
`start_sasl_ssl_broker`. The produce + replication-check pattern is
covered by `auth_handlers.rs::two_broker_sasl_plaintext_replication`
(slice 12 T17) — same shape, just routing over SASL_SSL.

- [ ] **Step 2: Run via WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance jvm_inter_broker_sasl_ssl_raft_replication -- --ignored --nocapture --test-threads=1"
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): SASL_SSL controller + data plane + raft replication

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 5 — Final acceptance sweep

### Task 12: Sweep + docs + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Run the full local test matrix**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
```

Expected: all clean.

- [ ] **Step 2: Run WSL JVM acceptance**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1"
```

Expected: all green. Watch for the WSL `/etc/hosts host.docker.internal` setup if some tests time out — same setup from slice 12.

- [ ] **Step 3: Update `README.md`**

Append a slice-12b bullet under "Slices delivered":

```markdown
- **Slice 12b** — auth cleanup: controller listener terminates TLS +
  SASL via a `RaftListenerHandshake` trait shared with the data plane;
  `InterBrokerDialer` wired into `ControllerConfig::dialer` so raft RPC
  authenticates outbound; `Broker::start` consumes
  `crabka format --add-scram` bootstrap records on first start.
```

- [ ] **Step 4: Append a Slice 12b section to `STATUS.md`**

```markdown
## Slice 12b — auth cleanup (2026-05-15)

- `BrokerConfig.controller_listener_protocol` (default Plaintext).
  Controller listener terminates TLS + SASL when set to SSL /
  SASL_PLAINTEXT / SASL_SSL.
- `crabka-raft` gains a `RaftListenerHandshake` trait + optional
  `ControllerConfig::handshake` slot. `crabka-broker` provides
  `BrokerRaftHandshake` reusing slice 12's `network::auth` state
  machines for inbound SASL.
- `InterBrokerDialer` (added in slice 12 but injected as `None`) is
  now always wired into `ControllerConfig::dialer`. For the legacy
  PLAINTEXT path it is byte-identical to raw `TcpStream::connect`.
- `Broker::start` consumes `<log_dir>/bootstrap.records.bin` on
  first start. Records are submitted via `controller.submit_change`
  after raft initialize. Corrupt files refuse-to-start with
  `BrokerError::BootstrapFile`.
- 3 new no-Docker integration tests (raft_sasl) + 3 bootstrap
  consumption tests + 1 new JVM acceptance test.
- Out of scope: mTLS, ACLs, per-listener controller-quorum protocol
  mapping, raft over SCRAM rotation under load.
```

- [ ] **Step 5: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "docs(slice-12b): README + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

- [ ] **Step 6: Push + open PR**

```bash
git push -u origin feature/auth-cleanup-12b
gh pr create --base main --head feature/auth-cleanup-12b \
  --title "Slice 12b: Auth cleanup (raft-over-SASL + bootstrap consumption)" \
  --body "$(cat <<'EOF'
## Summary

Finishes slice 12's two deferred items:

- **Raft-over-SASL** — controller listener now terminates TLS + SASL via a new `RaftListenerHandshake` trait (in `crabka-raft`) and a `BrokerRaftHandshake` impl (in `crabka-broker`) that reuses slice 12's `network::auth` state machines. The `InterBrokerDialer` adapter from slice 12 is wired into `ControllerConfig::dialer` for outbound raft RPC.
- **Bootstrap consumption** — `Broker::start` reads `<log_dir>/bootstrap.records.bin` produced by `crabka format --add-scram` and submits the records after raft initialize. The static super-user PLAIN path keeps working as a fallback.

`BrokerConfig.controller_listener_protocol` defaults to `Plaintext`; every existing test keeps passing without edits.

## Verified

- 3 no-Docker integration tests (`tests/raft_sasl.rs`): two-broker SASL_PLAINTEXT quorum converges; mismatched creds don't converge; legacy plaintext path unchanged.
- 3 no-Docker integration tests (`tests/bootstrap_consumption.rs`): format CLI -> SCRAM auth works; corrupt bootstrap refuses to start; absent bootstrap leaves legacy path unchanged.
- 1 new JVM acceptance test (`jvm_inter_broker_sasl_ssl_raft_replication`): two-broker SASL_SSL cluster (controller + data plane) replicates rf=2 produces from a JVM client.
- Workspace `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test` all green.

## Out of scope

mTLS, ACLs, per-listener controller-quorum mapping, SCRAM rotation under live raft traffic.

## Plan / spec

- Spec: `docs/superpowers/specs/2026-05-15-crabka-auth-cleanup-12b-design.md`
- Plan: `docs/superpowers/plans/2026-05-15-crabka-auth-cleanup-12b.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Confirm CI passes**

Watch for: cargo-deny on any new dep (none expected); broker-jvm-acceptance running the new SASL_SSL test on Linux (no WSL networking quirks on CI).

---

## Notes for the executing agent

1. **Branch:** all work is on `feature/auth-cleanup-12b`. Do NOT push to main.
2. **No new dependencies.** Every external crate this slice needs (`rustls`, `tokio-rustls`, `pbkdf2`, `hmac`, `sha2`, `subtle`, `base64`, `rustls-pki-types`) already landed in slice 12.
3. **Backward compatibility load-bearing piece:** `BrokerConfig::controller_listener_protocol = Plaintext` (default) MUST produce `ControllerConfig::handshake = None`, which MUST be byte-identical to the slice-12 raft path. Every pre-12b multi-broker test is the regression suite — keep them green.
4. **The `RaftListenerHandshake` trait avoids `crabka-broker → crabka-raft` circular dep.** Don't shortcut by adding a direct dep.
5. **`unimplemented!()` is a plan failure.** Each one must become a real body before commit. T5/T10/T11 have helper-stub `unimplemented!()`s that flag exactly the copy work needed.
6. **JVM tests on Linux CI:** if you write any tempfile and mount it into a container, chmod 0644 OR use `--user 0:0` + chmod inside the container (slice 12 lessons).
7. **Lifecycle subtlety:** the `BrokerRaftHandshake` may need access to the `ControllerHandle` for SCRAM credential lookup, but the controller is constructed after the handshake config is built. Use `Arc<tokio::sync::OnceCell<...>>` if the synchronous lifecycle doesn't work. Task 6 step 2 documents this.
