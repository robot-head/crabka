# Slice 49c: Broker — Custom TLS trust to IdP for JWKS — Implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (matches the project's CLAUDE.md mandate to execute in parallel batches). Steps use the project's compact-batch style — each T is one focused PR-worth of work, file-conflict-disjoint within a batch.

**Design:** `docs/superpowers/specs/2026-05-23-crabka-broker-jwks-tls-trust-49c-design.md`
**Umbrella:** `docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`

**Goal:** Let ops point the broker's JWKS refresher at an IdP whose TLS cert isn't signed by a public webpki root, by supplying a PEM bundle that *replaces* webpki as the exclusive trust store for the JWKS HTTPS connection (Strimzi-shaped `tlsTrustedCertificates` semantic).

**Architecture:** Three-step sequential dependency chain. `crates/security` gets a small new module `jwks_trust.rs` that builds a `rustls::ClientConfig` from a PEM path (single-purpose helper, reusable for slice 49d's introspection client). `crates/broker/src/file_config.rs` + `config.rs` get an optional `oauthbearer_jwks_tls_trust: Option<PathBuf>` field. `JwksRefresher::run` reads the field; when set, calls the helper and passes the resulting `ClientConfig` to `reqwest::Client::builder().use_preconfigured_tls(...)`; when unset, current webpki-roots default is preserved. No CRD changes, no operator changes, no wire-protocol changes — broker-only.

**Tech stack:** Same as slice 49b — rustls 0.23+, reqwest 0.13 with `rustls` feature, axum + tokio-rustls for the HTTPS test fixture (add `axum-server` as a `[dev-dependencies]` if not already present).

---

## Batches

### Batch 1 (parallel — disjoint crates/files; no compile-time dep between T1 and T2)

- **T1 — `crates/security` helper.** New `crates/security/src/jwks_trust.rs`. New module declaration + re-exports in `crates/security/src/lib.rs`.

  Content of `jwks_trust.rs`:
  ```rust
  //! Build a rustls `ClientConfig` whose trust roots are exclusively the
  //! certificates in a user-supplied PEM bundle. Slice 49c: backs the
  //! broker's outbound HTTPS to the JWKS endpoint when the operator
  //! configures a private IdP CA (Strimzi-shaped tlsTrustedCertificates
  //! "replace" semantic — webpki-roots are not consulted when this is
  //! used).
  
  use std::path::{Path, PathBuf};
  use std::sync::Arc;
  
  use rustls::pki_types::CertificateDer;
  use rustls::pki_types::pem::PemObject;
  use thiserror::Error;
  
  #[derive(Debug, Error)]
  pub enum JwksTrustError {
      #[error("read {0}: {1}")]
      Io(PathBuf, std::io::Error),
      #[error("no certificates in {0}")]
      Empty(PathBuf),
      #[error("rustls add cert: {0}")]
      Rustls(#[from] rustls::Error),
  }
  
  /// Read a PEM bundle of one or more CA certificates and produce a
  /// `rustls::ClientConfig` that trusts exactly those certificates. The
  /// returned config has no client auth (the broker does not present a
  /// client cert when fetching the JWKS endpoint).
  pub fn build_client_config_from_pem(
      path: &Path,
  ) -> Result<Arc<rustls::ClientConfig>, JwksTrustError> {
      let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
          .map_err(|e| JwksTrustError::Io(path.into(), std::io::Error::other(e.to_string())))?
          .collect::<Result<Vec<_>, _>>()
          .map_err(|e| JwksTrustError::Io(path.into(), std::io::Error::other(e.to_string())))?;
      if certs.is_empty() {
          return Err(JwksTrustError::Empty(path.into()));
      }
      let mut roots = rustls::RootCertStore::empty();
      for cert in certs {
          roots.add(cert)?;
      }
      Ok(Arc::new(
          rustls::ClientConfig::builder()
              .with_root_certificates(roots)
              .with_no_client_auth(),
      ))
  }
  ```

  Re-exports to add to `crates/security/src/lib.rs` (find where other `pub use` lines live; add):
  ```rust
  mod jwks_trust;
  pub use jwks_trust::{build_client_config_from_pem, JwksTrustError};
  ```

  Unit tests in the same file under `#[cfg(test)] mod tests`:
  - `build_client_config_from_pem_loads_single_cert` — write `crates/security/tests/fixtures/dev_cert.pem` (an existing test fixture) to a tempdir under a fresh filename via `std::fs::copy`, call helper, assert `Ok(_)`. The dev_cert.pem is a self-signed cert; the helper accepts any CA-shaped PEM.
  - `build_client_config_from_pem_loads_concatenated_chain` — write a tempfile that concatenates `dev_cert.pem` with itself (two `-----BEGIN CERTIFICATE-----` blocks); call helper; assert `Ok(_)`. (Asserting `roots.len() == 2` requires reaching inside the rustls types — `RootCertStore::roots` is a public `Vec` in rustls 0.23, so `cfg.crypto_provider()` and similar are not needed. If the assertion shape proves awkward, just assert `is_ok()` — the parsing path is already covered by other tests.)
  - `build_client_config_from_pem_rejects_missing_file` — call helper with `/nonexistent/path.pem`; assert `matches!(err, JwksTrustError::Io(_, _))`.
  - `build_client_config_from_pem_rejects_empty_pem` — write a tempfile with just `"# no certs here\n"`; assert `matches!(err, JwksTrustError::Empty(_))` (or `Io` — adjust assertion based on what `CertificateDer::pem_file_iter` actually returns for a file with zero certs; experimental verification needed and that's fine).
  - `build_client_config_from_pem_rejects_non_pem_garbage` — write a tempfile of arbitrary bytes (`[0u8; 64]`); assert `err.is_err()` (don't pin the variant).

  Make sure the existing `crates/security/src/tls.rs::tests::install_provider()` pattern is reused inside this module's tests too — rustls 0.23 needs a CryptoProvider installed for `ClientConfig::builder()`. Copy or wrap the pattern.

  Verify:
  ```bash
  cargo test -p crabka-security --lib jwks_trust:: 2>&1 | tail
  cargo fmt -p crabka-security -- --check
  ```

  Commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`:
  ```
  T1: crates/security — build_client_config_from_pem helper
  
  Loads a PEM bundle of CA certificates and produces a rustls
  ClientConfig that trusts exclusively those certificates. Used by the
  broker's JWKS refresher (slice 49c) and the future opaque-token
  introspection client (slice 49d) for outbound HTTPS to identity
  providers whose certs aren't signed by a public webpki root.
  
  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

- **T2 — Broker config field.** `crates/broker/src/file_config.rs` + `crates/broker/src/config.rs`.

  In `crates/broker/src/file_config.rs`, extend `FileOAuthBearerConfig`:
  ```rust
  #[derive(Debug, Clone, Default, Deserialize, PartialEq)]
  pub struct FileOAuthBearerConfig {
      // ...existing fields unchanged...
  
      /// Slice 49c: PEM file containing the CA certificate(s) used to
      /// verify the JWKS endpoint's TLS certificate when
      /// `jwks_endpoint_uri` is an `https://` URL. When set, these are
      /// the *only* trust roots used for the JWKS fetch (replaces the
      /// default webpki-roots — Strimzi-shaped). When unset, the
      /// refresher uses reqwest's default rustls webpki-roots. Inert if
      /// `jwks_endpoint_uri` is unset.
      #[serde(default)]
      pub jwks_tls_trust: Option<std::path::PathBuf>,
  }
  ```

  In `FileOAuthBearerConfig::apply_to` (look at how `jwks_endpoint_uri` is threaded — same shape), add:
  ```rust
  cfg.oauthbearer_jwks_tls_trust = oauth.jwks_tls_trust;
  ```

  In `crates/broker/src/config.rs`, add to `BrokerConfig`:
  ```rust
  /// Slice 49c: optional PEM path used as the exclusive trust store for
  /// outbound HTTPS to the JWKS endpoint. `None` → reqwest's default
  /// webpki-roots (slice 49b behavior). Threaded into JwksRefresher at
  /// spawn time.
  pub oauthbearer_jwks_tls_trust: Option<std::path::PathBuf>,
  ```

  And in the `Default for BrokerConfig` impl (or wherever the default values live — likely `BrokerConfig::default()`), add:
  ```rust
  oauthbearer_jwks_tls_trust: None,
  ```

  New unit test in `crates/broker/src/file_config.rs` (under the existing `#[cfg(test)] mod tests`):
  ```rust
  #[test]
  fn apply_to_oauthbearer_threads_jwks_tls_trust_to_broker_config() {
      let toml = r#"
  [oauthbearer]
  jwks_endpoint_uri = "https://idp.example/certs"
  jwks_tls_trust = "/etc/crabka/oauth/idp-ca.pem"
  "#;
      let file: FileConfig = toml::from_str(toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);
      assert_eq!(
          cfg.oauthbearer_jwks_tls_trust.as_deref(),
          Some(std::path::Path::new("/etc/crabka/oauth/idp-ca.pem"))
      );
  }
  ```

  Also: add a paired negative test if the existing file has paired tests for `jwks_endpoint_uri` (it does — `apply_to_oauthbearer_jwks_selects_signed_validator` is one example). Mirror that style:
  ```rust
  #[test]
  fn apply_to_oauthbearer_without_jwks_tls_trust_leaves_field_none() {
      let toml = r#"
  [oauthbearer]
  jwks_endpoint_uri = "https://idp.example/certs"
  "#;
      let file: FileConfig = toml::from_str(toml).unwrap();
      let mut cfg = crate::config::BrokerConfig::default();
      file.apply_to(&mut cfg);
      assert!(cfg.oauthbearer_jwks_tls_trust.is_none());
  }
  ```

  Verify:
  ```bash
  cargo build -p crabka-broker 2>&1 | tail
  # Some compile errors in oauth_jwks.rs and broker.rs are EXPECTED until T3 lands —
  # specifically the JwksRefresher literal will be missing the new tls_trust field
  # AS LONG AS T3 adds it to the struct. If you avoid changing the struct in T2 (and
  # we do — the struct lives in oauth_jwks.rs which is T3's file), the workspace
  # should still build cleanly. Verify it does.
  cargo test -p crabka-broker --lib file_config:: 2>&1 | tail
  cargo fmt -p crabka-broker -- --check
  ```

  Commit:
  ```
  T2: crates/broker — oauthbearer_jwks_tls_trust config field
  
  Adds the [oauthbearer].jwks_tls_trust TOML key and the corresponding
  BrokerConfig field. T3 wires it into JwksRefresher; this commit only
  threads the value end-to-end through file-config + runtime-config.
  
  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 2 (sequential — depends on T1 + T2)

- **T3 — Wire JwksRefresher + thread into Broker::start.** `crates/broker/src/oauth_jwks.rs` and `crates/broker/src/broker.rs`.

  In `crates/broker/src/oauth_jwks.rs`:

  1. Add `use std::path::PathBuf;` if not already imported.

  2. Add field to `JwksRefresher`:
     ```rust
     pub(crate) struct JwksRefresher {
         pub endpoint: String,
         pub handle: JwksHandle,
         pub interval: Duration,
         pub shutdown: CancellationToken,
         /// Slice 49c: optional PEM path; when `Some`, the rustls
         /// ClientConfig used by reqwest is built from this file and
         /// replaces the default webpki-roots trust store. When `None`,
         /// reqwest's webpki-roots default applies (slice 49b behavior).
         pub tls_trust: Option<PathBuf>,
     }
     ```

  3. Update `run` to build the reqwest client with the preconfigured TLS when `tls_trust` is `Some`:
     ```rust
     pub(crate) async fn run(self) {
         let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
         if let Some(path) = &self.tls_trust {
             match crabka_security::build_client_config_from_pem(path) {
                 Ok(cfg) => {
                     // reqwest's use_preconfigured_tls takes the rustls
                     // ClientConfig by value; clone the inner config
                     // (cheap — it's a small struct of Arc fields).
                     builder = builder.use_preconfigured_tls((*cfg).clone());
                 }
                 Err(e) => {
                     tracing::error!(
                         error = %e,
                         path = %path.display(),
                         "failed to load OAUTHBEARER JWKS TLS trust bundle; refresher will not start",
                     );
                     return;
                 }
             }
         }
         let client = match builder.build() {
             Ok(c) => c,
             Err(e) => {
                 tracing::error!(error = %e, "failed to build JWKS HTTP client; OAUTHBEARER signed tokens will not validate");
                 return;
             }
         };
         // ...existing tick-loop body unchanged from here down...
     }
     ```

     **Note on the existing match.** The current code has `let client = match reqwest::Client::builder().timeout(Duration::from_secs(10)).build() { Ok(c) => c, ... }`. Restructure into the `builder = …; let client = match builder.build() {...}` shape so the optional TLS-trust step can mutate `builder` between construction and `build()`. Keep all the existing error handling (the `tracing::error!` + early-return for build failure) intact.

  4. Update the existing tests in this file to construct `JwksRefresher` with `tls_trust: None`. There's one such construction site: `refresher_populates_handle_then_stops_on_shutdown` at the bottom of the file. Add `tls_trust: None,` to its literal.

  5. Add two new HTTPS integration tests at the bottom of the file (after the existing tests):

     The HTTPS test harness needs a self-signed server cert + a CA bundle and a way to bind an HTTPS-enabled axum server. Two practical approaches; pick what the broker dev-deps already support:

     **Approach A (preferred if axum-server isn't already a dev-dep):** use `tokio-rustls::TlsAcceptor` to wrap a `tokio::net::TcpListener` and serve directly:
     ```rust
     async fn serve_jwks_https(
         body: &'static str,
     ) -> (SocketAddr, CancellationToken, PathBuf, PathBuf) {
         // Returns (server_addr, shutdown_token, server_cert_pem_path,
         // ca_cert_pem_path). Server presents server_cert; ca_cert is the
         // root the client trusts (here, the same self-signed cert).
         // ... see implementation sketch below ...
     }
     ```

     The fixture should write the dev cert + key to a tempdir, build a rustls::ServerConfig from them, accept connections, terminate TLS, and serve `body` at `/jwks` over the TLS-wrapped socket. The PEM written out doubles as the CA bundle in the trust-store test below.

     Use the existing dev cert fixtures: `crates/security/tests/fixtures/dev_cert.pem` and `dev_key.pem`. They're self-signed (CN=crabka-dev) so the cert is its own CA — the client can verify the connection by trusting the same cert.

     **Important:** axum's higher-level `axum::serve(listener, app)` does not support TLS termination directly — that's why we go via `tokio-rustls`. Sketch:
     ```rust
     use std::sync::Arc;
     use tokio_rustls::TlsAcceptor;
     use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

     async fn serve_jwks_https(body: &'static str) -> (SocketAddr, CancellationToken, PathBuf) {
         let dir = tempfile::tempdir().unwrap();
         let cert_path = dir.path().join("cert.pem");
         let key_path = dir.path().join("key.pem");
         std::fs::write(&cert_path, include_str!("../../security/tests/fixtures/dev_cert.pem")).unwrap();
         std::fs::write(&key_path, include_str!("../../security/tests/fixtures/dev_key.pem")).unwrap();

         let _ = rustls::crypto::ring::default_provider().install_default();
         let certs: Vec<CertificateDer<'static>> =
             CertificateDer::pem_file_iter(&cert_path).unwrap().collect::<Result<_, _>>().unwrap();
         let key = PrivateKeyDer::from_pem_file(&key_path).unwrap();
         let server_cfg = Arc::new(
             rustls::ServerConfig::builder()
                 .with_no_client_auth()
                 .with_single_cert(certs, key)
                 .unwrap(),
         );
         let acceptor = TlsAcceptor::from(server_cfg);

         let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
         let addr = listener.local_addr().unwrap();
         let shutdown = CancellationToken::new();

         let srv_shutdown = shutdown.clone();
         // Leak the dir for the test's lifetime so the PEM file remains
         // valid until shutdown fires.
         let cert_path_for_caller = cert_path.clone();
         let _dir = Box::leak(Box::new(dir));

         tokio::spawn(async move {
             loop {
                 tokio::select! {
                     _ = srv_shutdown.cancelled() => break,
                     Ok((sock, _peer)) = listener.accept() => {
                         let acceptor = acceptor.clone();
                         tokio::spawn(async move {
                             let Ok(tls) = acceptor.accept(sock).await else { return };
                             // Trivial HTTP/1.1 reply with a fixed body. This is a
                             // test fixture — no need for hyper.
                             let mut tls = tls;
                             let req = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ";
                             let _ = tokio::io::AsyncWriteExt::write_all(&mut tls, req).await;
                             let len = body.len().to_string();
                             let _ = tokio::io::AsyncWriteExt::write_all(&mut tls, len.as_bytes()).await;
                             let _ = tokio::io::AsyncWriteExt::write_all(&mut tls, b"\r\n\r\n").await;
                             let _ = tokio::io::AsyncWriteExt::write_all(&mut tls, body.as_bytes()).await;
                         });
                     }
                 }
             }
         });

         (addr, shutdown, cert_path_for_caller)
     }
     ```

     **Important — the HTTP reply parsing path.** The above writes a raw HTTP/1.1 response. reqwest needs the request to be made over HTTPS with SNI; the server cert's CN is `crabka-dev`, which means the request URL must use that hostname (not `127.0.0.1` IP). reqwest's `connect_to` is the standard escape valve — but a simpler test approach: use rustls's `dangerous_configuration` to disable hostname verification, OR generate the test cert with a SAN that includes `127.0.0.1`. The dev cert fixture's SANs aren't documented; if it lacks `127.0.0.1`, the test client will fail with `InvalidDnsName` / `NotValidForName`.

     **Escalation cue for the implementer:** if the dev_cert.pem doesn't include `127.0.0.1` as a SAN, **generate a fresh test cert on the fly using `rcgen`** — add `rcgen` as a `[dev-dependencies]` of `crabka-broker` (it's already in the workspace deps because `crates/security/src/ca.rs` uses it; the broker dev deps need to add `rcgen.workspace = true` under `[dev-dependencies]`). The fixture function then becomes:
     ```rust
     let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
     let key = rcgen::KeyPair::generate().unwrap();
     let cert = params.self_signed(&key).unwrap();
     let cert_pem = cert.pem();
     let key_pem = key.serialize_pem();
     // write both to tempdir, build ServerConfig from them
     ```
     This is the cleanest path. Prefer it over including a static fixture, because static fixtures rot and the SAN list is brittle.

  6. The two new tests:
     ```rust
     #[tokio::test]
     async fn refresher_fetches_jwks_over_https_with_custom_trust() {
         let (addr, srv_shutdown, ca_path) = serve_jwks_https(JWKS_BODY).await;
         let handle = JwksHandle::default();
         let shutdown = CancellationToken::new();
         let refresher = JwksRefresher {
             endpoint: format!("https://127.0.0.1:{}/jwks", addr.port()),
             handle: handle.clone(),
             interval: Duration::from_millis(50),
             shutdown: shutdown.clone(),
             tls_trust: Some(ca_path),
         };
         let task = tokio::spawn(refresher.run());
         for _ in 0..100 {
             if !handle.load().is_empty() { break; }
             tokio::time::sleep(Duration::from_millis(20)).await;
         }
         assert_eq!(handle.load().len(), 1);
         shutdown.cancel();
         task.await.unwrap();
         srv_shutdown.cancel();
     }

     #[tokio::test]
     async fn refresher_https_fetch_fails_when_custom_trust_doesnt_match_server_cert() {
         // Server uses cert A; trust bundle is cert B (an unrelated
         // self-signed cert). Handle stays empty because every refresh
         // fails verification.
         let (addr, srv_shutdown, _server_ca_path) = serve_jwks_https(JWKS_BODY).await;
         // Build an UNRELATED self-signed cert to use as the bogus
         // trust bundle.
         let dir = tempfile::tempdir().unwrap();
         let params = rcgen::CertificateParams::new(vec!["unrelated.example".to_string()]).unwrap();
         let key = rcgen::KeyPair::generate().unwrap();
         let cert = params.self_signed(&key).unwrap();
         let bogus_ca = dir.path().join("bogus-ca.pem");
         std::fs::write(&bogus_ca, cert.pem()).unwrap();

         let handle = JwksHandle::default();
         let shutdown = CancellationToken::new();
         let refresher = JwksRefresher {
             endpoint: format!("https://127.0.0.1:{}/jwks", addr.port()),
             handle: handle.clone(),
             interval: Duration::from_millis(50),
             shutdown: shutdown.clone(),
             tls_trust: Some(bogus_ca),
         };
         let task = tokio::spawn(refresher.run());
         tokio::time::sleep(Duration::from_millis(300)).await; // multiple ticks
         assert!(handle.load().is_empty(), "fetch should fail verification and leave handle empty");
         shutdown.cancel();
         task.await.unwrap();
         srv_shutdown.cancel();
     }
     ```

  7. Update `Cargo.toml` for `crabka-broker` — add `rcgen` to `[dev-dependencies]`:
     ```toml
     rcgen.workspace = true
     ```
     (Verify it's a workspace dep first via `grep '^rcgen' Cargo.toml` at the workspace root. If it's not in the workspace, add it: `rcgen = "0.13"` or whatever version `crates/security` already uses.)

  In `crates/broker/src/broker.rs` (around line 1238, the `JwksRefresher { ... }` literal):

  Add the new field:
  ```rust
  let refresher = crate::oauth_jwks::JwksRefresher {
      endpoint,
      handle,
      interval: config.oauthbearer_jwks_refresh_interval,
      shutdown: supervisor_shutdown.child_token(),
      tls_trust: config.oauthbearer_jwks_tls_trust.clone(),
  };
  ```

  Verify:
  ```bash
  cargo build -p crabka-broker 2>&1 | tail
  # Should be clean.
  cargo test -p crabka-broker --lib oauth_jwks:: 2>&1 | tail
  cargo test -p crabka-broker --lib file_config:: 2>&1 | tail
  cargo fmt -p crabka-broker -- --check
  cargo clippy -p crabka-broker --tests -- -D warnings 2>&1 | tail
  ```

  Commit:
  ```
  T3: crates/broker — wire jwks_tls_trust into JwksRefresher
  
  Adds tls_trust field on JwksRefresher; when Some, builds the reqwest
  client with use_preconfigured_tls(rustls::ClientConfig) via the new
  crates/security::build_client_config_from_pem helper. Threads the
  field into Broker::start. Two new tokio-rustls HTTPS integration
  tests cover happy-path verification and a deliberately mismatched
  trust bundle (handle stays empty, refresher keeps logging warnings
  on each tick).
  
  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

### Batch 3 (sequential — STATUS + final gate)

- **T4 — STATUS.md entry + final gate.**

  Append a `## Slice 49c — Broker: Custom TLS trust to IdP for JWKS (2026-05-23)` section at the end of `STATUS.md`. Match the slice-49b entry's tone and structure (~30-40 lines is right for a slice this small). Cover:
  - 2-3 sentence opener: what it ships, why (50b unblock, kind+Keycloak HTTPS).
  - `crates/security` bullet: new `build_client_config_from_pem` helper.
  - `crates/broker` bullet: new `[oauthbearer].jwks_tls_trust` TOML key, `BrokerConfig.oauthbearer_jwks_tls_trust` runtime field, JwksRefresher wired with `use_preconfigured_tls`.
  - Test count: 5 new security unit + 2 new broker integration + 2 new broker file_config unit.
  - Reference doc link.
  - Out-of-scope reminder: 50b (operator surface), 49d (introspection — will reuse the helper), hot reload, multiple paths, cert pinning.

  Final gate:
  ```bash
  cargo fmt --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  ```

  All three must be green. If clippy fires on the new code, decide per lint: rename/restructure for substantive lints; targeted `#[allow(clippy::...)]` with a one-line rationale only for intentional patterns the lint can't infer.

  Commit:
  ```
  Slice 49c: STATUS.md entry + final gate
  
  Documents the new oauthbearer JWKS TLS trust knob. fmt + clippy +
  workspace tests all green.
  
  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

## Notes

- The dependency chain is T1 (helper) → T2 (config field, independent of T1 at compile time) → T3 (uses both) → T4 (STATUS + gate). T1 and T2 are file-disjoint and run in parallel as Batch 1.
- The HTTPS test fixture (T3) is the trickiest part of the slice. The `rcgen`-on-the-fly approach avoids the brittle "what SANs does dev_cert.pem have" question and is the recommended path. If the implementer hits an unexpected obstacle with `tokio-rustls` + a raw HTTP/1.1 hand-roll, fall back to using `axum-server` (a `[dev-dependencies]` add) — but `tokio-rustls` is already a workspace dep so the hand-roll has fewer moving parts.
- Greenfield: no compat shims. The new field defaults to `None`; existing deployments are unchanged.
- File ownership stays disjoint across the slice. `tls.rs` is for inter-broker mTLS; `jwks_trust.rs` is for outbound HTTPS-to-IdP. Don't merge them.
- After slice 49c lands, the slice-50 Keycloak kind e2e (T8 of slice 50) becomes upgradable from HTTP-only to HTTPS by (a) installing Keycloak with `tls.enabled=true` and `tls.autoGenerated=true`, (b) pulling the auto-generated CA out of the Bitnami Secret, (c) setting the operator's listener `jwksEndpointUri: https://...` plus (when 50b lands) `tlsTrustedCertificates: <secret-ref>`. That upgrade is 50b's job, not 49c's.
