# Tiered Storage 48r — TLS/SASL on the internal metadata client — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the public client-core clients a TLS/SASL security surface, then point the topic-based RLMM at the broker's inter-broker listener using the broker's existing inter-broker credentials, so tiered storage works on auth-required clusters.

**Architecture:** Extract the transport-agnostic outbound SASL handshake out of `crates/broker/src/network/client.rs` into `crates/client-core` (which already depends on `crabka-protocol` for the SASL message types and tokio's `io-util`) as a reusable `outbound_sasl(stream, creds, server_name)` over any `AsyncRead + AsyncWrite + Unpin + Send`. A new `ClientSecurity` value in client-core wraps `ListenerProtocol` + an optional TLS connector config + optional SASL credentials; the `Client` builder grows a `.security(Option<ClientSecurity>)` knob whose connect path runs TLS then `outbound_sasl` before `Connection::from_stream`. `Producer`, `Consumer`, and `AdminClient` pass the security through; `kafka_log.rs` threads it into the producer/consumer/admin builds; and the broker's `bootstrap_topic_rlmm` supplies the inter-broker listener address, `config.inter_broker_credentials`, and the broker's TLS client config. When security is `None` (the default), every path is byte-for-byte today's plaintext behavior.

**Tech Stack:** Rust, tokio, rustls, crabka workspace crates (security, client-core, broker, remote-storage-topic)

---

## File structure

```
crates/client-core/
  Cargo.toml                         # + crabka-security, rustls, tokio-rustls deps
  src/
    lib.rs                           # pub mod security; pub use; pub mod sasl
    sasl.rs                          # NEW: outbound_sasl + helpers (moved from broker)
    security.rs                      # NEW: ClientSecurity, TlsConnectorConfig, SaslCredentials
    client.rs                        # + .security(...) builder arg, ConnectionOptions wiring
    connection.rs                    # + Connection::connect_secured(addr, opts, security)
crates/broker/
  src/network/client.rs              # run_outbound_sasl etc. deleted; call crabka_client_core::outbound_sasl
  src/broker.rs                      # bootstrap_topic_rlmm supplies inter-broker addr+creds+TLS
  src/config.rs                      # KafkaRlmmConfig gains `security: Option<ClientSecurity>`
  tests/tiered_storage_topic_rlmm.rs # + SASL_PLAINTEXT loopback round-trip test
crates/client-producer/
  src/builder.rs                     # + .security(...) pass-through
crates/client-consumer/
  src/consumer.rs                    # + .security(...) pass-through (both Client::builder calls)
crates/client-admin/
  src/lib.rs                         # + AdminClient::connect_secured
crates/remote-storage-topic/
  src/kafka_log.rs                   # KafkaMetadataLogConfig gains `security`; thread into all clients
```

---

### Task 1: Extract the outbound SASL handshake into client-core

**Files:**
- `crates/client-core/Cargo.toml`
- `crates/client-core/src/sasl.rs` (new)
- `crates/client-core/src/lib.rs`
- `crates/broker/src/network/client.rs`

The transport-agnostic outbound SASL state machine (`run_outbound_sasl`, `send_sasl_handshake`, `send_plain_authenticate`, `run_scram_client`, `run_gssapi_client`, `send_sasl_authenticate`, `round_trip`) currently lives in the broker. It depends only on `crabka-protocol` (SASL message types) and `crabka-security` (`SaslMechanism`, `ScramClientExchange`, `gssapi`) — both of which client-core can take as deps with **no dependency cycle** (`crabka-security` does not depend on `crabka-client-core`). Move it into client-core verbatim, generalize the credential parameter to a client-core-owned type, and have the broker call the shared function.

The credentials enum used by `run_outbound_sasl` is `crate::config::InterBrokerCredentials` in the broker. We introduce a client-core-owned `SaslCredentials` (Task 2) with the same three variants and the same `mechanism()` method, and `outbound_sasl` takes `&SaslCredentials`. To keep this task self-contained, define a *minimal* `SaslCredentials` here in `sasl.rs` first; Task 2 re-homes it into `security.rs` and adds the surrounding `ClientSecurity`. (They do not conflict — Task 2 only moves the type within client-core and never touches the broker.)

- [ ] Add deps to `crates/client-core/Cargo.toml` under `[dependencies]`:
  ```toml
  crabka-security = { version = "0.1", path = "../security" }
  rustls = { workspace = true }
  rustls-pki-types = { workspace = true }
  tokio-rustls = { workspace = true }
  ```
- [ ] Run `cargo build -p crabka-client-core` and expect PASS (deps resolve; no code uses them yet).
- [ ] Write a failing test: create `crates/client-core/src/sasl.rs` ending with a `#[cfg(test)]` module that drives `outbound_sasl` PLAIN against an in-process fake broker (one `tokio::io::duplex` half feeds a tiny SASL server that replies `SaslHandshakeResponse{error_code:0}` then `SaslAuthenticateResponse{error_code:0}`):
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use bytes::{Buf, BufMut, BytesMut};
      use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
      use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
      use crabka_protocol::Encode;
      use tokio::io::{AsyncReadExt, AsyncWriteExt};

      // Minimal server: read one request frame, reply with a v0-header
      // (corr_id only) response carrying `body`.
      async fn reply_frame<S>(stream: &mut S, body: &[u8])
      where
          S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
      {
          let req_len = stream.read_u32().await.unwrap();
          let mut req = vec![0u8; req_len as usize];
          stream.read_exact(&mut req).await.unwrap();
          // corr_id is at request header bytes [4..8] (api_key,version,corr_id).
          let corr_id = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
          let mut frame = BytesMut::new();
          frame.put_i32(corr_id);
          frame.put_slice(body);
          stream.write_u32(frame.len() as u32).await.unwrap();
          stream.write_all(&frame).await.unwrap();
          stream.flush().await.unwrap();
      }

      #[tokio::test]
      async fn outbound_plain_completes() {
          let (mut client, mut server) = tokio::io::duplex(8192);
          let server_task = tokio::spawn(async move {
              // 1. SaslHandshake v1 → error_code 0 + empty mechanisms.
              let mut hs = BytesMut::new();
              SaslHandshakeResponse { error_code: 0, ..Default::default() }
                  .encode(&mut hs, 1)
                  .unwrap();
              reply_frame(&mut server, &hs).await;
              // 2. SaslAuthenticate v2 → error_code 0.
              let mut au = BytesMut::new();
              SaslAuthenticateResponse { error_code: 0, ..Default::default() }
                  .encode(&mut au, 2)
                  .unwrap();
              reply_frame(&mut server, &au).await;
          });
          let creds = SaslCredentials::Plain {
              username: "u".into(),
              password: "p".into(),
          };
          outbound_sasl(&mut client, &creds, "localhost")
              .await
              .expect("PLAIN outbound handshake completes");
          server_task.await.unwrap();
      }
  }
  ```
- [ ] Run `cargo test -p crabka-client-core sasl::tests::outbound_plain_completes` and expect FAIL (`outbound_sasl` / `SaslCredentials` undefined).
- [ ] Implement `crates/client-core/src/sasl.rs`: move the broker's SASL functions verbatim, with these edits:
  - Drop `use crate::config::InterBrokerCredentials;`; define the credential type locally:
    ```rust
    use std::path::{Path, PathBuf};

    use bytes::{Buf, BufMut, BytesMut};
    use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
    use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
    use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
    use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
    use crabka_protocol::{Decode, Encode};
    use crabka_security::{SaslMechanism, ScramClientExchange};
    use thiserror::Error;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    const API_KEY_SASL_HANDSHAKE: i16 = 17;
    const API_KEY_SASL_AUTHENTICATE: i16 = 36;
    const OUTBOUND_CLIENT_ID: &str = "crabka-client";
    const GSSAPI_MAX_RECV_SIZE: u32 = 0x1_0000;

    /// Outbound SASL credentials. Mirrors the broker's
    /// `InterBrokerCredentials`; one variant per supported mechanism.
    #[derive(Debug, Clone)]
    pub enum SaslCredentials {
        Plain { username: String, password: String },
        Scram { mechanism: SaslMechanism, username: String, password: String },
        Gssapi {
            keytab_path: PathBuf,
            client_principal: String,
            service_name: String,
            kdc_url: String,
        },
    }

    impl SaslCredentials {
        #[must_use]
        pub fn mechanism(&self) -> SaslMechanism {
            match self {
                Self::Plain { .. } => SaslMechanism::Plain,
                Self::Scram { mechanism, .. } => *mechanism,
                Self::Gssapi { .. } => SaslMechanism::Gssapi,
            }
        }
    }

    #[derive(Debug, Error)]
    pub enum OutboundSaslError {
        #[error("io: {0}")]
        Io(#[from] std::io::Error),
        #[error("sasl: {0}")]
        Sasl(String),
        #[error("codec: {0}")]
        Codec(String),
    }
    ```
  - Rename `run_outbound_sasl` → public `outbound_sasl`, returning `Result<(), OutboundSaslError>`, taking `creds: &SaslCredentials`. Keep the body identical (match arms over `SaslCredentials::{Plain,Scram,Gssapi}`).
  - In every helper, replace `InterBrokerError::{Sasl,Codec}` with `OutboundSaslError::{Sasl,Codec}` and `InterBrokerError::Io` (auto `#[from]`). The `gssapi` helper keeps using `crabka_security::gssapi::{client::*, provider::SspiInitiator}`.
  - Keep `round_trip`, `send_sasl_handshake`, `send_plain_authenticate`, `run_scram_client`, `run_gssapi_client`, `send_sasl_authenticate` private. Keep the `OUTBOUND_CLIENT_ID` reporter-id string (now `"crabka-client"`).
- [ ] Add to `crates/client-core/src/lib.rs`:
  ```rust
  pub mod sasl;
  pub use sasl::{OutboundSaslError, SaslCredentials, outbound_sasl};
  ```
- [ ] Run `cargo test -p crabka-client-core sasl::tests::outbound_plain_completes` and expect PASS.
- [ ] Rewrite `crates/broker/src/network/client.rs` to call the shared fn (no behavior change):
  - Delete `run_outbound_sasl`, `send_sasl_handshake`, `send_plain_authenticate`, `run_scram_client`, `run_gssapi_client`, `send_sasl_authenticate`, `round_trip`, the `API_KEY_*`, `OUTBOUND_CLIENT_ID`, `GSSAPI_MAX_RECV_SIZE` consts, and the now-unused imports (`Buf`, `BufMut`, `BytesMut`, the SASL `owned::*` types, `Decode`, `Encode`, `ScramClientExchange`, `AsyncReadExt`, `AsyncWriteExt`, `std::path::Path`).
  - Add a `pub(crate)` mapping fn from `InterBrokerCredentials` to `crabka_client_core::SaslCredentials`; given both enums are identical, implement it in `client.rs` (Task 6 reuses it for the RLMM bootstrap, so make it `pub(crate)` now):
    ```rust
    pub(crate) fn to_client_creds(c: &InterBrokerCredentials) -> crabka_client_core::SaslCredentials {
        match c {
            InterBrokerCredentials::Plain { username, password } =>
                crabka_client_core::SaslCredentials::Plain {
                    username: username.clone(), password: password.clone() },
            InterBrokerCredentials::Scram { mechanism, username, password } =>
                crabka_client_core::SaslCredentials::Scram {
                    mechanism: *mechanism, username: username.clone(),
                    password: password.clone() },
            InterBrokerCredentials::Gssapi { keytab_path, client_principal, service_name, kdc_url } =>
                crabka_client_core::SaslCredentials::Gssapi {
                    keytab_path: keytab_path.clone(),
                    client_principal: client_principal.clone(),
                    service_name: service_name.clone(),
                    kdc_url: kdc_url.clone() },
        }
    }
    ```
  - In both `connect` and `connect_as_connection`, replace the
    `run_outbound_sasl(&mut *stream, &creds, server_name).await?;` call with:
    ```rust
    crabka_client_core::outbound_sasl(&mut *stream, &to_client_creds(&creds), server_name)
        .await
        .map_err(|e| InterBrokerError::Sasl(e.to_string()))?;
    ```
  - Leave `InterBrokerError`, `DuplexStream`, `InterBrokerClient`, `InterBrokerDialer`, the TLS-connect blocks, and the SNI handling unchanged.
- [ ] Run `cargo test -p crabka-broker --test raft_sasl` and expect PASS (inter-broker SASL semantics unchanged).
- [ ] Run `cargo fmt -p crabka-client-core -p crabka-broker`.
- [ ] Commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "refactor(client-core): extract outbound SASL handshake from broker"`

---

### Task 2: Define `ClientSecurity` in client-core

**Files:**
- `crates/client-core/src/security.rs` (new)
- `crates/client-core/src/sasl.rs`
- `crates/client-core/src/lib.rs`

Re-home `SaslCredentials` into a `security` module alongside a `TlsConnectorConfig` (a thin client-side mirror of the broker's `TlsConfig::build_client_config` trust-roots path) and the top-level `ClientSecurity`. This task only moves/extends types *within* client-core — it does not touch `client.rs`, `connection.rs`, or the broker, so it does not conflict with later builder-wiring tasks if those are deferred, but here they run sequentially.

- [ ] Write a failing test in a new `crates/client-core/src/security.rs`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crabka_security::ListenerProtocol;

      #[test]
      fn plaintext_security_has_no_tls_or_sasl() {
          let s = ClientSecurity {
              protocol: ListenerProtocol::Plaintext,
              tls: None,
              sasl: None,
          };
          assert!(!s.protocol.requires_tls());
          assert!(!s.protocol.requires_sasl());
      }

      #[test]
      fn sasl_plaintext_carries_creds() {
          let s = ClientSecurity {
              protocol: ListenerProtocol::SaslPlaintext,
              tls: None,
              sasl: Some(SaslCredentials::Plain {
                  username: "u".into(),
                  password: "p".into(),
              }),
          };
          assert!(s.protocol.requires_sasl());
          assert!(matches!(s.sasl, Some(SaslCredentials::Plain { .. })));
      }

      #[test]
      fn tls_connector_config_builds_client_config() {
          // Empty trust roots → webpki defaults disabled; we only assert it builds.
          let _ = rustls::crypto::ring::default_provider().install_default();
          let cfg = TlsConnectorConfig { trust_roots_pem: None, server_name: "broker".into() };
          cfg.build().expect("client config builds with empty roots");
      }
  }
  ```
- [ ] Run `cargo test -p crabka-client-core security::tests` and expect FAIL (`security` module / `ClientSecurity` / `TlsConnectorConfig` undefined).
- [ ] Implement `crates/client-core/src/security.rs`:
  ```rust
  //! Client-side TLS/SASL security surface for [`crate::Client`].
  //!
  //! Mirrors the broker's inter-broker credential + TLS shapes so the
  //! public clients and the inter-broker dialer negotiate the same way.

  use std::path::PathBuf;
  use std::sync::Arc;

  use crabka_security::ListenerProtocol;
  use rustls_pki_types::pem::PemObject;
  use tokio_rustls::TlsConnector;

  pub use crate::sasl::SaslCredentials;

  /// Client-side TLS trust + SNI. Mirrors the trust-roots half of the
  /// broker's [`crabka_security::TlsConfig::build_client_config`].
  #[derive(Debug, Clone)]
  pub struct TlsConnectorConfig {
      /// PEM file of CA certs the client trusts to verify the broker's
      /// server cert. `None` → empty root store (handshake fails unless
      /// the server cert chains to a webpki default, which we do not
      /// install — mirrors the broker's strict `build_client_config`).
      pub trust_roots_pem: Option<PathBuf>,
      /// SNI / server-name used for the TLS handshake and as the
      /// canonical hostname for any GSSAPI SPN.
      pub server_name: String,
  }

  impl TlsConnectorConfig {
      /// Build a `rustls::ClientConfig` (no client cert; trust-roots only).
      ///
      /// # Errors
      /// Returns a string error if a trust-roots PEM is configured but
      /// fails to load or add to the root store.
      pub fn build(&self) -> Result<Arc<rustls::ClientConfig>, String> {
          let mut roots = rustls::RootCertStore::empty();
          if let Some(path) = &self.trust_roots_pem {
              for cert in rustls::pki_types::CertificateDer::pem_file_iter(path)
                  .map_err(|e| format!("trust roots load {}: {e}", path.display()))?
              {
                  let cert = cert.map_err(|e| format!("trust roots parse: {e}"))?;
                  roots.add(cert).map_err(|e| format!("trust roots add: {e}"))?;
              }
          }
          let cfg = rustls::ClientConfig::builder()
              .with_root_certificates(roots)
              .with_no_client_auth();
          Ok(Arc::new(cfg))
      }

      /// Build a ready `TlsConnector`.
      ///
      /// # Errors
      /// Propagates [`Self::build`] failures.
      pub fn connector(&self) -> Result<TlsConnector, String> {
          Ok(TlsConnector::from(self.build()?))
      }
  }

  /// Full client security policy: which listener protocol to speak, plus
  /// the TLS and SASL material it implies. `None` fields are required to
  /// match `protocol` (a `SaslSsl` policy needs both `tls` and `sasl`).
  #[derive(Debug, Clone)]
  pub struct ClientSecurity {
      pub protocol: ListenerProtocol,
      pub tls: Option<TlsConnectorConfig>,
      pub sasl: Option<SaslCredentials>,
  }
  ```
- [ ] Move the `SaslCredentials` enum + its `mechanism()` impl OUT of `sasl.rs` is **not** done — keep `SaslCredentials` defined in `sasl.rs` and merely `pub use crate::sasl::SaslCredentials;` from `security.rs` (avoids churning Task 1's tests). Confirm `sasl.rs` still declares `pub enum SaslCredentials`.
- [ ] Add to `crates/client-core/src/lib.rs`:
  ```rust
  pub mod security;
  pub use security::{ClientSecurity, TlsConnectorConfig};
  ```
  (`SaslCredentials` is already re-exported by Task 1.)
- [ ] Run `cargo test -p crabka-client-core security::tests` and expect PASS.
- [ ] Run `cargo fmt -p crabka-client-core`.
- [ ] Commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(client-core): ClientSecurity TLS/SASL config surface"`

---

### Task 3: `.security(...)` connect path on the Client builder

**Files:**
- `crates/client-core/src/connection.rs`
- `crates/client-core/src/client.rs`
- `crates/client-core/src/pool.rs`

Add a `Connection::connect_secured` that dials TCP, optionally wraps in TLS, optionally runs `outbound_sasl`, then defers to `from_stream`. Wire an `Option<ClientSecurity>` through `ConnectionOptions` so the existing `BrokerPool` (which builds connections from `ConnectionOptions` alone) inherits the policy without an API change, and add `.security(...)` to the `Client` builder. Default `None` = today's plaintext `Connection::connect`.

- [ ] Write a failing test at the end of `crates/client-core/src/connection.rs`:
  ```rust
  #[cfg(test)]
  mod secured_tests {
      use super::*;
      use crate::security::{ClientSecurity, SaslCredentials};
      use crabka_security::ListenerProtocol;

      // A SASL_PLAINTEXT connect drives the handshake then ApiVersions.
      // The fake broker answers SaslHandshake(0), SaslAuthenticate(0),
      // then a minimal ApiVersionsResponse v0 so from_stream succeeds.
      #[tokio::test]
      async fn connect_secured_runs_sasl_then_api_versions() {
          use bytes::{BufMut, BytesMut};
          use crabka_protocol::Encode;
          use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
          use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
          use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
          use tokio::io::{AsyncReadExt, AsyncWriteExt};

          let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
          let addr = listener.local_addr().unwrap();
          let server = tokio::spawn(async move {
              let (mut s, _) = listener.accept().await.unwrap();
              for body in [
                  {
                      let mut b = BytesMut::new();
                      SaslHandshakeResponse { error_code: 0, ..Default::default() }
                          .encode(&mut b, 1).unwrap();
                      b
                  },
                  {
                      let mut b = BytesMut::new();
                      SaslAuthenticateResponse { error_code: 0, ..Default::default() }
                          .encode(&mut b, 2).unwrap();
                      b
                  },
                  {
                      let mut b = BytesMut::new();
                      ApiVersionsResponse::default().encode(&mut b, 0).unwrap();
                      b
                  },
              ] {
                  let req_len = s.read_u32().await.unwrap();
                  let mut req = vec![0u8; req_len as usize];
                  s.read_exact(&mut req).await.unwrap();
                  let corr = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
                  let mut frame = BytesMut::new();
                  frame.put_i32(corr);
                  frame.put_slice(&body);
                  s.write_u32(frame.len() as u32).await.unwrap();
                  s.write_all(&frame).await.unwrap();
                  s.flush().await.unwrap();
              }
          });
          let security = ClientSecurity {
              protocol: ListenerProtocol::SaslPlaintext,
              tls: None,
              sasl: Some(SaslCredentials::Plain { username: "u".into(), password: "p".into() }),
          };
          let conn = Connection::connect_secured(addr, ConnectionOptions::default(), &security)
              .await
              .expect("secured connect completes");
          conn.close();
          server.await.unwrap();
      }
  }
  ```
- [ ] Run `cargo test -p crabka-client-core connection::secured_tests` and expect FAIL (`connect_secured` undefined).
- [ ] In `crates/client-core/src/connection.rs`, add a security field to `ConnectionOptions`:
  ```rust
  #[derive(Debug, Clone)]
  pub struct ConnectionOptions {
      pub client_id: String,
      pub connect_timeout: Duration,
      pub request_timeout: Duration,
      /// Client-side TLS/SASL policy. `None` = plaintext (default).
      pub security: Option<crate::security::ClientSecurity>,
  }
  ```
  and set `security: None` in `impl Default for ConnectionOptions`.
- [ ] Add `Connection::connect_secured` (and route `connect` through it):
  ```rust
  impl Connection {
      /// Connect to `addr`, applying `security` (TLS then SASL) before the
      /// API-versions bootstrap. `Plaintext` is identical to [`connect`].
      pub async fn connect_secured(
          addr: SocketAddr,
          options: ConnectionOptions,
          security: &crate::security::ClientSecurity,
      ) -> Result<Self, ClientError> {
          use tokio::io::{AsyncRead, AsyncWrite};

          let tcp = tokio::time::timeout(options.connect_timeout, TcpStream::connect(addr))
              .await
              .map_err(|_| ClientError::Timeout(options.connect_timeout))?
              .map_err(|source| ClientError::Connect { addr, source })?;
          tcp.set_nodelay(true).ok();

          // 1. TLS (if the protocol demands it).
          let mut stream: Box<dyn ClientDuplex> = if security.protocol.requires_tls() {
              let tls = security.tls.as_ref().ok_or_else(|| {
                  ClientError::Io(std::io::Error::other("TLS protocol without tls config"))
              })?;
              let connector = tls
                  .connector()
                  .map_err(|e| ClientError::Io(std::io::Error::other(e)))?;
              let sni = tokio_rustls::rustls::pki_types::ServerName::try_from(
                  tls.server_name.clone(),
              )
              .map_err(|e| ClientError::Io(std::io::Error::other(format!("invalid SNI: {e}"))))?;
              let s = connector
                  .connect(sni, tcp)
                  .await
                  .map_err(|e| ClientError::Io(std::io::Error::other(e.to_string())))?;
              Box::new(s)
          } else {
              Box::new(tcp)
          };

          // 2. SASL (if the protocol demands it).
          if security.protocol.requires_sasl() {
              let creds = security.sasl.as_ref().ok_or_else(|| {
                  ClientError::Io(std::io::Error::other("SASL protocol without credentials"))
              })?;
              let server_name = security
                  .tls
                  .as_ref()
                  .map_or("localhost", |t| t.server_name.as_str());
              crate::sasl::outbound_sasl(&mut *stream, creds, server_name)
                  .await
                  .map_err(|e| ClientError::Io(std::io::Error::other(e.to_string())))?;
          }

          // Bound to silence the unused-import lint when neither arm runs.
          let _assert_duplex: &(dyn AsyncRead + AsyncWrite + Send + Unpin) = &*stream;
          Self::from_stream(stream, options).await
      }
  }
  ```
  (Remove the `_assert_duplex` line if clippy is happy without it; it is only a guard against unused `AsyncRead`/`AsyncWrite` imports — prefer deleting the `use` and the guard together if unneeded.)
- [ ] Update `Connection::connect` to delegate when no security is set, preserving the exact plaintext path: leave `connect` as-is (it already builds a plaintext `TcpStream` and calls `from_stream`). No change needed unless `pool.rs` chooses to call `connect_secured` unconditionally (next step).
- [ ] In `crates/client-core/src/pool.rs`, find where the pool opens a connection from `ConnectionOptions` and branch on `options.security`:
  ```rust
  let conn = match &options.security {
      Some(sec) => Connection::connect_secured(addr, options.clone(), sec).await?,
      None => Connection::connect(addr, options.clone()).await?,
  };
  ```
  (Locate the existing `Connection::connect(addr, ...)` call sites in `pool.rs` and replace each; if there is exactly one constructor helper, edit that.)
- [ ] Add `.security(...)` to the `Client` builder in `crates/client-core/src/client.rs`:
  ```rust
  #[builder(start_fn = builder, finish_fn = build)]
  pub async fn start(
      #[builder(into)] bootstrap: String,
      #[builder(into, default = "crabka".to_string())] client_id: String,
      #[builder(default = std::time::Duration::from_secs(30))]
      connect_timeout: std::time::Duration,
      #[builder(default = std::time::Duration::from_secs(30))]
      request_timeout: std::time::Duration,
      security: Option<crate::security::ClientSecurity>,
  ) -> Result<Self, ClientError> {
      let options = ConnectionOptions {
          client_id,
          connect_timeout,
          request_timeout,
          security,
      };
      Self::start_with_options(bootstrap, options).await
  }
  ```
- [ ] Fix every other `ConnectionOptions { ... }` literal that no longer compiles (it now needs `security`). Search the workspace: `crates/client-admin/src/lib.rs` builds `ConnectionOptions` directly (`connect` + `reconnect`) — add `security: None` to both literals (Task 4 makes admin configurable). Also check `crates/broker` for any direct `ConnectionOptions { .. }` construction and add `security: None`.
- [ ] Run `cargo test -p crabka-client-core connection::secured_tests` and expect PASS.
- [ ] Run `cargo build --workspace` and expect PASS (all `ConnectionOptions` literals fixed).
- [ ] Run `cargo fmt --all`.
- [ ] Commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(client-core): .security(...) TLS/SASL connect path on Client"`

---

### Task 4: `.security(...)` pass-through on Producer, Consumer, and Admin

**Files:**
- `crates/client-producer/src/builder.rs`
- `crates/client-consumer/src/consumer.rs`
- `crates/client-admin/src/lib.rs`

Thread an `Option<ClientSecurity>` from each public builder into the underlying `Client::builder().security(...)` (producer/consumer) and into `ConnectionOptions.security` (admin).

- [ ] Write a failing test in `crates/client-producer/src/builder.rs` (`#[cfg(test)]`) that asserts the builder *accepts* a `.security(...)` arg by constructing it against an unreachable bootstrap and matching on a connect error (proves the arg compiles + is plumbed, without needing a live broker):
  ```rust
  #[cfg(test)]
  mod security_arg_tests {
      use super::*;
      use crabka_client_core::security::{ClientSecurity, SaslCredentials};
      use crabka_security::ListenerProtocol;

      #[tokio::test]
      async fn producer_builder_accepts_security() {
          let security = ClientSecurity {
              protocol: ListenerProtocol::SaslPlaintext,
              tls: None,
              sasl: Some(SaslCredentials::Plain { username: "u".into(), password: "p".into() }),
          };
          // 127.0.0.1:1 is unroutable for a listener; build must fail at
          // connect, proving the security arg is threaded (not a type error).
          let res = Producer::builder()
              .bootstrap("127.0.0.1:1")
              .enable_idempotence(false)
              .security(security)
              .build()
              .await;
          assert!(res.is_err(), "connect to closed port must fail");
      }
  }
  ```
- [ ] Run `cargo test -p crabka-client-producer security_arg_tests` and expect FAIL (`.security` not a builder method).
- [ ] In `crates/client-producer/src/builder.rs`, add the arg and thread it:
  ```rust
  security: Option<crabka_client_core::security::ClientSecurity>,
  ...
  let client = Client::builder()
      .bootstrap(bootstrap)
      .client_id(client_id.clone())
      .request_timeout(request_timeout)
      .maybe_security(security)
      .build()
      .await?;
  ```
  (bon generates `maybe_<field>` for `Option<T>` builder args; if the generated name differs, use `.security(security)` taking the `Option` directly — verify against the generated builder.)
- [ ] Run `cargo test -p crabka-client-producer security_arg_tests` and expect PASS.
- [ ] Write a failing test in `crates/client-consumer/src/consumer.rs` (`#[cfg(test)]`) mirroring the producer one (build against `127.0.0.1:1` with a `group_id` + `subscribe`, expect `Err`). Run `cargo test -p crabka-client-consumer` for the new test name and expect FAIL.
- [ ] In `crates/client-consumer/src/consumer.rs`, add `security: Option<crabka_client_core::security::ClientSecurity>` to the builder args and thread it into **both** `Client::builder()` calls — the primary `client` (line ~100) and the `coordinator_client` (line ~287). Clone the value for the second use (`.maybe_security(security.clone())`).
- [ ] Run the consumer test and expect PASS.
- [ ] Write a failing test in `crates/client-admin/src/lib.rs` (`#[cfg(test)]`) asserting `AdminClient::connect_secured(&["127.0.0.1:1".into()], Some(sec))` returns `Err`. Run `cargo test -p crabka-client-admin` for it and expect FAIL.
- [ ] In `crates/client-admin/src/lib.rs`, add a secured constructor and route the existing one through it:
  ```rust
  /// Connect, applying optional client security. `None` = plaintext
  /// (identical to [`AdminClient::connect`]).
  pub async fn connect_secured(
      bootstrap_addrs: &[String],
      security: Option<crabka_client_core::security::ClientSecurity>,
  ) -> Result<Self, AdminError> {
      let opts = ConnectionOptions {
          connect_timeout: Duration::from_secs(5),
          request_timeout: Duration::from_secs(30),
          client_id: "crabka-operator".to_string(),
          security,
      };
      for host_port in bootstrap_addrs {
          match Self::connect_one(host_port, opts.clone()).await {
              Ok(conn) => return Ok(Self { conn }),
              Err(e) => tracing::debug!(target: "crabka_client_admin", addr = %host_port, error = %e, "bootstrap connect failed"),
          }
      }
      Err(AdminError::Connect { tried: bootstrap_addrs.len() })
  }

  pub async fn connect(bootstrap_addrs: &[String]) -> Result<Self, AdminError> {
      Self::connect_secured(bootstrap_addrs, None).await
  }
  ```
  Update `connect_one` to branch on `opts.security` (mirroring `pool.rs`): when `Some`, resolve the addr and call `Connection::connect_secured(addr, opts, sec)`, else `Connection::connect(addr, opts)`. Update `reconnect` the same way (it rebuilds `ConnectionOptions` — carry `self`'s security forward by storing the `Option<ClientSecurity>` on `AdminClient`, or re-thread via a stored field). Add a `security: Option<ClientSecurity>` field to `AdminClient` set in `connect_secured`, read in `reconnect`.
- [ ] Run `cargo test -p crabka-client-admin` for the new test and expect PASS.
- [ ] Run `cargo fmt --all`.
- [ ] Commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(clients): .security(...) pass-through on Producer/Consumer/Admin"`

---

### Task 5: Thread `ClientSecurity` through `kafka_log.rs`

**Files:**
- `crates/remote-storage-topic/src/kafka_log.rs`

`KafkaMetadataLogConfig` gains an `Option<ClientSecurity>`; `start`, `ensure_topic`, and `consumer_pump` apply it to the producer, raw client, admin client, and consumer respectively. `subscribe()` clones the security into the spawned `consumer_pump`.

- [ ] Write a failing test in the `#[cfg(test)]` module of `crates/remote-storage-topic/src/kafka_log.rs`:
  ```rust
  #[test]
  fn config_carries_security() {
      use crabka_client_core::security::{ClientSecurity, SaslCredentials};
      use crabka_security::ListenerProtocol;
      let cfg = KafkaMetadataLogConfig {
          bootstrap: "127.0.0.1:9092".into(),
          topic: METADATA_TOPIC.into(),
          num_partitions: 1,
          replication: 1,
          client_id: "x".into(),
          security: Some(ClientSecurity {
              protocol: ListenerProtocol::SaslPlaintext,
              tls: None,
              sasl: Some(SaslCredentials::Plain { username: "u".into(), password: "p".into() }),
          }),
      };
      assert!(cfg.security.is_some());
  }
  ```
- [ ] Run `cargo test -p crabka-remote-storage-topic kafka_log::tests::config_carries_security` and expect FAIL (no `security` field; the existing `config_defaults_match_kafka` test will also fail to compile once the field is added — update it too).
- [ ] In `KafkaMetadataLogConfig`, add:
  ```rust
  /// Client TLS/SASL security applied to the producer, consumer, raw
  /// client, and admin client. `None` = plaintext loopback (default).
  pub security: Option<crabka_client_core::security::ClientSecurity>,
  ```
  In `KafkaMetadataLogConfig::new`, set `security: None`. The `#[derive(Debug, Clone)]` stays (`ClientSecurity` is `Debug + Clone`).
- [ ] In `KafkaMetadataEventLog`, store `security: Option<ClientSecurity>` (clone of `cfg.security`) so `subscribe()` can hand it to `consumer_pump`. Add the field to the struct and set it in `start`.
- [ ] In `start`, thread security into the producer and raw client builds:
  ```rust
  let producer = Producer::builder()
      .bootstrap(cfg.bootstrap.clone())
      .client_id(format!("{}-producer", cfg.client_id))
      .acks(Acks::All)
      .enable_idempotence(true)
      .maybe_security(cfg.security.clone())
      .build()
      .await
      .map_err(|e| MetadataLogError::Other(format!("producer build failed: {e}")))?;

  let client = Client::builder()
      .bootstrap(cfg.bootstrap.clone())
      .client_id(format!("{}-client", cfg.client_id))
      .maybe_security(cfg.security.clone())
      .build()
      .await
      .map_err(|e| MetadataLogError::Other(format!("client build failed: {e}")))?;
  ```
- [ ] In `ensure_topic`, switch to the secured admin connect:
  ```rust
  let mut admin = AdminClient::connect_secured(
      std::slice::from_ref(&cfg.bootstrap),
      cfg.security.clone(),
  )
  .await
  .map_err(|e| MetadataLogError::Other(format!("admin connect failed: {e}")))?;
  ```
- [ ] In `subscribe`, capture `let security = self.security.clone();` and pass it into the `consumer_pump(...)` call. Update `consumer_pump`'s signature to take `security: Option<ClientSecurity>` and thread it into the `Consumer::builder()` call with `.maybe_security(security)`.
- [ ] Update the existing `config_defaults_match_kafka` test (and any other `KafkaMetadataLogConfig { .. }` literal in this crate) to set/expect `security: None`.
- [ ] Run `cargo test -p crabka-remote-storage-topic kafka_log` and expect PASS.
- [ ] Run `cargo fmt -p crabka-remote-storage-topic`.
- [ ] Commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(rlmm): thread ClientSecurity through KafkaMetadataEventLog"`

---

### Task 6: `bootstrap_topic_rlmm` supplies inter-broker addr + creds + TLS

**Files:**
- `crates/broker/src/config.rs`
- `crates/broker/src/broker.rs`

`KafkaRlmmConfig` gains `security: Option<ClientSecurity>`. The broker's RLMM bootstrap sets `bootstrap` to the inter-broker listener's advertised address and `security` to a `ClientSecurity` derived from the inter-broker listener protocol, `config.inter_broker_credentials`, and the broker's TLS client config. Fully-plaintext brokers leave it `None` and loopback is unchanged.

- [ ] Write a failing test in `crates/broker/src/config.rs` `#[cfg(test)] mod tests`:
  ```rust
  #[test]
  fn kafka_rlmm_config_carries_optional_security() {
      let c = KafkaRlmmConfig {
          bootstrap: "127.0.0.1:9092".into(),
          num_partitions: 1,
          replication: 1,
          security: None,
      };
      assert!(c.security.is_none());
  }
  ```
- [ ] Run `cargo test -p crabka-broker config::tests::kafka_rlmm_config_carries_optional_security` and expect FAIL (no `security` field).
- [ ] In `crates/broker/src/config.rs`, add to `KafkaRlmmConfig`:
  ```rust
  /// Client TLS/SASL security for the metadata client. `None` =
  /// plaintext loopback (single-broker / fully-plaintext clusters).
  pub security: Option<crabka_client_core::security::ClientSecurity>,
  ```
  Note: `KafkaRlmmConfig` derives `PartialEq, Eq` — `ClientSecurity` does NOT derive those (it holds rustls-adjacent types). **Drop `PartialEq, Eq` from `KafkaRlmmConfig`'s derive** (greenfield; no caller compares it — verified: `grep -rn "KafkaRlmmConfig" crates/ | grep -v "src/config.rs"` shows no `==`). Keep `Debug, Clone`.
  Ensure `crates/broker/Cargo.toml` already has `crabka-client-core` (it does, line 33).
- [ ] Fix the other `KafkaRlmmConfig { .. }` literal at `crates/broker/src/file_config.rs:827` (the `[remote_storage.kafka_metadata]` TOML path): add `security: None,` (the broker overrides it at runtime from the inter-broker listener in `bootstrap_topic_rlmm`, so the TOML path always supplies `None`).
- [ ] Run the config test and expect PASS.
- [ ] In `crates/broker/src/broker.rs`, build the inter-broker `ClientSecurity` and feed it to the kickoff. The kickoff is constructed at ~line 1833 (`kafka_swap_kickoff`); the inter-broker listener protocol is computed at ~line 1393 (`inter_listener_proto`) but that is *after* line 1833 — so compute the security inline at the kickoff site instead, from `config.effective_listeners()`:
  ```rust
  let kafka_swap_kickoff: Option<KafkaSwapKickoff> = config
      .remote_log_metadata_kafka
      .as_ref()
      .map(|cfg| {
          // Resolve the inter-broker listener: its advertised address is
          // the RLMM bootstrap; its protocol + the broker's credentials +
          // TLS client config form the metadata-client security policy.
          let listeners = config.effective_listeners();
          let inter = listeners
              .iter()
              .find(|l| l.name == config.inter_broker_listener_name);
          let proto = inter.map_or(
              crabka_security::ListenerProtocol::Plaintext,
              |l| l.protocol,
          );
          // Plaintext inter-broker → no security (loopback unchanged).
          let security = if proto.requires_tls() || proto.requires_sasl() {
              let tls = if proto.requires_tls() {
                  config.tls_config.as_ref().map(|t| {
                      crabka_client_core::security::TlsConnectorConfig {
                          trust_roots_pem: t.trust_roots_path.clone(),
                          // SNI = the advertised host of the inter-broker listener.
                          server_name: inter
                              .map(|l| l.advertised.rsplit_once(':').map_or(
                                  l.advertised.clone(), |(h, _)| h.to_string()))
                              .unwrap_or_else(|| "localhost".to_string()),
                      }
                  })
              } else {
                  None
              };
              let sasl = config
                  .inter_broker_credentials
                  .as_ref()
                  .map(to_client_creds_from_inter_broker);
              Some(crabka_client_core::security::ClientSecurity { protocol: proto, tls, sasl })
          } else {
              None
          };
          // Bootstrap = inter-broker listener advertised addr when secured;
          // otherwise the operator-supplied (loopback) bootstrap is kept.
          let bootstrap = if security.is_some() {
              inter.map_or(cfg.bootstrap.clone(), |l| l.advertised.clone())
          } else {
              cfg.bootstrap.clone()
          };
          KafkaSwapKickoff {
              cfg: crate::config::KafkaRlmmConfig {
                  bootstrap,
                  num_partitions: cfg.num_partitions,
                  replication: cfg.replication,
                  security,
              },
              broker_id: config.broker_id,
          }
      });
  ```
  Add a free helper in `broker.rs` (or reuse Task 1's `to_client_creds` — if that one lives in `network/client.rs` and is private, add a `pub(crate)` version there and import it; cleanest is a `pub(crate) fn to_client_creds(&InterBrokerCredentials) -> SaslCredentials` in `network/client.rs` exported for reuse):
  ```rust
  fn to_client_creds_from_inter_broker(
      c: &crate::config::InterBrokerCredentials,
  ) -> crabka_client_core::security::SaslCredentials {
      crate::network::client::to_client_creds(c)
  }
  ```
  (Promote Task 1's `to_client_creds` to `pub(crate)` in `network/client.rs` so both the dialer and this bootstrap share one mapping.)
- [ ] In `bootstrap_topic_rlmm`, copy the security from the kickoff config into `KafkaMetadataLogConfig`:
  ```rust
  let log_cfg = crabka_remote_storage_topic::KafkaMetadataLogConfig {
      bootstrap: cfg.cfg.bootstrap,
      topic: crabka_remote_storage_topic::METADATA_TOPIC.to_string(),
      num_partitions: cfg.cfg.num_partitions,
      replication: cfg.cfg.replication,
      client_id: format!("crabka-rlmm-broker-{}", cfg.broker_id),
      security: cfg.cfg.security,
  };
  ```
- [ ] Fix the existing `KafkaRlmmConfig { .. }` literal in `crates/broker/tests/tiered_storage_topic_rlmm.rs:68` (the plaintext `start_broker_with_topic_rlmm` helper): add `security: None,`.
- [ ] Run `cargo build -p crabka-broker --tests` and expect PASS (all `KafkaRlmmConfig` literals updated).
- [ ] Run `cargo test -p crabka-broker --test tiered_storage_topic_rlmm` and expect PASS (the existing plaintext loopback tests are unaffected — inter-broker listener is `PLAINTEXT` so `security` is `None` and bootstrap stays the operator value).
- [ ] Run `cargo fmt -p crabka-broker`.
- [ ] Commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(broker): RLMM metadata client uses inter-broker listener + creds + TLS"`

---

### Task 7: Integration — RLMM loopback over SASL_PLAINTEXT

**Files:**
- `crates/broker/tests/tiered_storage_topic_rlmm.rs`

Extend the existing topic-RLMM integration test with a single-broker config whose inter-broker listener is `SASL_PLAINTEXT` (PLAIN). The RLMM must bootstrap against the SASL-required listener (authenticating as the inter-broker PLAIN principal) and round-trip a copy→metadata→read cycle, proving the secured metadata client works end-to-end.

- [ ] Add a failing test to `crates/broker/tests/tiered_storage_topic_rlmm.rs`. Reuse the file's existing helpers where possible; add a SASL variant of `start_broker_with_topic_rlmm`:
  ```rust
  /// Boot a single broker whose *only* (and inter-broker) listener is
  /// SASL_PLAINTEXT/PLAIN, with the topic-backed RLMM pointed at it. The
  /// RLMM authenticates as the inter-broker PLAIN principal.
  async fn start_sasl_broker_with_topic_rlmm() -> (BrokerHandle, TempDir, TempDir) {
      use crabka_broker::config::{InterBrokerCredentials, ListenerSpec};
      use crabka_security::{ListenerProtocol, SaslMechanism};

      support::init_tracing();
      let (client_addrs, controller_addrs) = support::bind_and_drop_ports(1).await;
      let listen = client_addrs[0];
      let log_dir = TempDir::new().expect("log tempdir");
      let remote_dir = TempDir::new().expect("remote tempdir");

      let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
      cfg.listen_addr = listen;
      cfg.advertised_listener = listen.to_string();
      cfg.controller_listen_addr = controller_addrs[0];
      cfg.controller_quorum_voters = vec![(1, controller_addrs[0])];
      cfg.listeners = vec![ListenerSpec {
          name: "SASL_PLAINTEXT".to_string(),
          bind_addr: listen,
          advertised: format!("127.0.0.1:{}", listen.port()),
          protocol: ListenerProtocol::SaslPlaintext,
          tls_config: None,
          sasl_mechanisms: None,
      }];
      cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
      cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
      cfg.plain_credentials
          .insert("rlmm".to_string(), "rlmm-secret".to_string());
      cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
          username: "rlmm".to_string(),
          password: "rlmm-secret".to_string(),
      });
      cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
          dir: remote_dir.path().to_path_buf(),
      });
      cfg.remote_log_manager_interval = Duration::from_secs(1);
      cfg.remote_log_metadata_kafka = Some(KafkaRlmmConfig {
          bootstrap: format!("127.0.0.1:{}", listen.port()),
          num_partitions: 1,
          replication: 1,
          security: None, // broker overrides from the inter-broker listener
      });

      let broker = Broker::start(cfg).await.expect("broker start");
      (broker, log_dir, remote_dir)
  }
  ```
  Then add the round-trip test (model it on `topic_rlmm_copy_then_fetch_round_trip`, but: the *test's own* `build_client` must also authenticate — produce/fetch the data topic over SASL_PLAINTEXT by passing a `ClientSecurity` to `Client::builder().security(...)`):
  ```rust
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  #[allow(clippy::too_many_lines)]
  async fn topic_rlmm_sasl_loopback_copy_then_fetch_round_trip() {
      use crabka_client_core::security::{ClientSecurity, SaslCredentials};
      use crabka_security::ListenerProtocol;

      const TOPIC: &str = "tiered-topic-rlmm-sasl-itest";
      let (broker, _log_dir, remote_dir) = start_sasl_broker_with_topic_rlmm().await;
      await_activation(&broker).await;

      let security = ClientSecurity {
          protocol: ListenerProtocol::SaslPlaintext,
          tls: None,
          sasl: Some(SaslCredentials::Plain {
              username: "rlmm".into(),
              password: "rlmm-secret".into(),
          }),
      };
      let client = Client::builder()
          .bootstrap(broker.listen_addr().to_string())
          .client_id("tiered-topic-rlmm-sasl-test")
          .security(security.clone())
          .build()
          .await
          .expect("authed client build");

      // ... reuse the create-topic / produce / await-tier / fetch body from
      // topic_rlmm_copy_then_fetch_round_trip verbatim, but every Producer /
      // Client / Consumer the test spins up takes `.security(security.clone())`.
      // Assert the records read back at offset 0 (same assertions as the
      // plaintext variant).
  }
  ```
  Copy the create→produce→await-tier→fetch body from `topic_rlmm_copy_then_fetch_round_trip` (lines ~120–319), adding `.security(security.clone())` to any client/producer/consumer the test constructs.
- [ ] Run `cargo test -p crabka-broker --test tiered_storage_topic_rlmm topic_rlmm_sasl_loopback_copy_then_fetch_round_trip` and expect FAIL (until Tasks 1–6 are in; if running after those tasks land it should pass — if it fails, debug per `systematic-debugging`).
- [ ] Confirm the plaintext tests in the same file still pass: `cargo test -p crabka-broker --test tiered_storage_topic_rlmm`.
- [ ] Run `cargo fmt -p crabka-broker`.
- [ ] Commit: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "test(broker): RLMM topic loopback over SASL_PLAINTEXT round-trip"`

---

### Task 8: Final verification

**Files:** none (verification only)

- [ ] Run `cargo fmt --all --check` and expect clean exit.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings` and expect clean exit. Fix any lint (likely candidates: unused imports left in `network/client.rs` after the SASL extraction; `clippy::missing_errors_doc` on new public fns; an unused `_assert_duplex` guard in `connect_secured`).
- [ ] Run `cargo test -p crabka-client-core -p crabka-broker -p crabka-remote-storage-topic` and expect PASS (the design doc's targeted gate).
- [ ] Run `cargo test --workspace` and expect PASS (no regressions across the tree).
- [ ] If all four gates are green, the slice is complete. Use superpowers:finishing-a-development-branch to decide on merge / PR.
