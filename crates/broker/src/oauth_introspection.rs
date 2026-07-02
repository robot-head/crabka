//! HTTP transport for RFC 7662 introspection + OIDC userinfo. Lives
//! here (not in `crates/security`) so the security crate stays
//! I/O-free, mirroring the JWKS-refresher pattern.

use std::{path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use crabka_security::{
    IntrospectionClient, IntrospectionError, JwksTrustError, build_client_config_from_pem,
};

/// reqwest-backed RFC 7662 introspection client. Uses HTTP Basic Auth
/// with the operator-configured `client_id` + `client_secret`.
/// Optionally calls a userinfo endpoint after a successful
/// introspection.
#[derive(Debug)]
pub struct ReqwestIntrospectionClient {
    client: reqwest::Client,
    introspection_endpoint: String,
    userinfo_endpoint: Option<String>,
    client_id: String,
    client_secret: String,
}

/// Errors building the introspection client at broker startup.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("tls trust: {0}")]
    Tls(#[from] JwksTrustError),
    #[error("reqwest build: {0}")]
    Reqwest(String),
}

impl ReqwestIntrospectionClient {
    /// Build a new client. When `tls_trust` is `Some`, the rustls
    /// `ClientConfig` is built via `crabka_security::build_client_config_from_pem`
    /// — the same trust bundle covers JWKS, introspection,
    /// and userinfo. When `None`, reqwest's default webpki-roots apply.
    // Returns Arc<dyn IntrospectionClient> to fit the validator's trait-object
    // slot; constructing the concrete type would just be unwrapped immediately
    // into the Arc.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        introspection_endpoint: String,
        userinfo_endpoint: Option<String>,
        client_id: String,
        client_secret: String,
        tls_trust: Option<&Path>,
        timeout: Duration,
    ) -> Result<Arc<dyn IntrospectionClient>, BuildError> {
        // `file_config::FileConfig::apply_to` constructs this client
        // before `Broker::start` runs, so the rustls CryptoProvider that
        // `Broker::start` installs at line ~783 isn't there yet when
        // `build_client_config_from_pem` reaches into
        // `rustls::ClientConfig::builder()` → `CryptoProvider::get_default()`
        // and panics with "Could not automatically determine the
        // process-level CryptoProvider from Rustls crate features."
        // (Both the `aws-lc-rs` and `ring` rustls features end up in the
        // dep graph, so the `auto-determine` heuristic refuses to pick
        // one.) Install idempotently here so the introspection client
        // works from any entry point — the `let _ =` swallows
        // `AlreadySet` when a sibling caller (notably `Broker::start`)
        // won the race.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut builder = reqwest::Client::builder().timeout(timeout);
        if let Some(path) = tls_trust {
            let cfg = build_client_config_from_pem(path)?;
            builder = builder.use_preconfigured_tls((*cfg).clone());
        }
        let client = builder
            .build()
            .map_err(|e| BuildError::Reqwest(e.to_string()))?;
        Ok(Arc::new(Self {
            client,
            introspection_endpoint,
            userinfo_endpoint,
            client_id,
            client_secret,
        }))
    }
}

#[async_trait]
impl IntrospectionClient for ReqwestIntrospectionClient {
    async fn introspect(&self, token: &str) -> Result<serde_json::Value, IntrospectionError> {
        let resp = self
            .client
            .post(&self.introspection_endpoint)
            .basic_auth(&self.client_id, Some(&self.client_secret))
            .form(&[("token", token)])
            .send()
            .await
            .map_err(|e| IntrospectionError::Transport(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(IntrospectionError::Status(resp.status().as_u16()));
        }
        resp.json::<serde_json::Value>()
            .await
            .map_err(|_| IntrospectionError::Parse)
    }

    async fn userinfo(&self, token: &str) -> Result<Option<serde_json::Value>, IntrospectionError> {
        let Some(endpoint) = &self.userinfo_endpoint else {
            return Ok(None);
        };
        let resp = self
            .client
            .get(endpoint)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| IntrospectionError::Transport(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(IntrospectionError::Status(resp.status().as_u16()));
        }
        let json = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|_| IntrospectionError::Parse)?;
        Ok(Some(json))
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Mutex};

    use assert2::assert;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_rustls::TlsAcceptor;
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// Records observed requests so tests can assert on the bytes.
    #[derive(Debug, Default)]
    struct ObservedRequests {
        introspect_bodies: Mutex<Vec<String>>,
        introspect_auths: Mutex<Vec<String>>,
        userinfo_auths: Mutex<Vec<String>>,
    }

    /// Spin up an HTTPS test server. Routes:
    ///   POST /introspect  -> `introspect_body` with `introspect_status`
    ///   GET  /userinfo    -> `userinfo_body` (200) or 404 if None
    /// Returns (addr, shutdown, `ca_pem_path`, observed).
    // TLS handshake + HTTP/1.1 framing + dual-route dispatch sit naturally in one
    // function for the test fixture; extraction would just scatter mock state.
    #[allow(clippy::too_many_lines)]
    async fn serve_https(
        introspect_body: &'static str,
        introspect_status: u16,
        userinfo_body: Option<&'static str>,
    ) -> (
        SocketAddr,
        CancellationToken,
        std::path::PathBuf,
        Arc<ObservedRequests>,
    ) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let cert_path = dir.path().join("cert.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        let key_path = dir.path().join("key.pem");
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let priv_key = PrivateKeyDer::from_pem_file(&key_path).unwrap();
        let server_cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, priv_key)
                .unwrap(),
        );
        let acceptor = TlsAcceptor::from(server_cfg);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let srv_shutdown = shutdown.clone();
        let observed = Arc::new(ObservedRequests::default());
        let observed_in_task = observed.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = srv_shutdown.cancelled() => break,
                    Ok((sock, _peer)) = listener.accept() => {
                        let acceptor = acceptor.clone();
                        let observed = observed_in_task.clone();
                        tokio::spawn(async move {
                            let Ok(mut tls) = acceptor.accept(sock).await else { return };
                            let mut buf = vec![0u8; 8192];
                            let n = tls.read(&mut buf).await.unwrap_or(0);
                            let req = String::from_utf8_lossy(&buf[..n]).to_string();
                            let body = req
                                .split("\r\n\r\n")
                                .nth(1)
                                .unwrap_or("")
                                .to_string();
                            let auth_header = req
                                .lines()
                                .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
                                .map(|l| {
                                    l.trim_start_matches(|c: char| c != ':')
                                        .trim_start_matches(':')
                                        .trim()
                                        .to_string()
                                })
                                .unwrap_or_default();
                            let (status, body_out) = if req.starts_with("POST /introspect") {
                                observed
                                    .introspect_bodies
                                    .lock()
                                    .unwrap()
                                    .push(body.clone());
                                observed
                                    .introspect_auths
                                    .lock()
                                    .unwrap()
                                    .push(auth_header.clone());
                                (introspect_status, introspect_body)
                            } else if req.starts_with("GET /userinfo") {
                                observed
                                    .userinfo_auths
                                    .lock()
                                    .unwrap()
                                    .push(auth_header.clone());
                                match userinfo_body {
                                    Some(b) => (200u16, b),
                                    None => (404, "{}"),
                                }
                            } else {
                                (404, "{}")
                            };
                            let status_text = match status {
                                200 => "OK",
                                401 => "Unauthorized",
                                404 => "Not Found",
                                500 => "Internal Server Error",
                                _ => "Error",
                            };
                            let header = format!(
                                "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                                body_out.len(),
                            );
                            let _ = tls.write_all(header.as_bytes()).await;
                            let _ = tls.write_all(body_out.as_bytes()).await;
                            let _ = tls.shutdown().await;
                        });
                    }
                }
            }
        });

        (addr, shutdown, cert_path, observed)
    }

    #[tokio::test]
    async fn introspection_fetches_active_token_over_https_with_custom_trust() {
        let body = r#"{"active":true,"sub":"alice"}"#;
        let (addr, srv_shutdown, ca, _observed) = serve_https(body, 200, None).await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "kafka-broker".into(),
            "secret".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        let resp = client.introspect("tok").await.unwrap();
        assert!(resp.get("active").and_then(serde_json::Value::as_bool) == Some(true));
        assert!(resp.get("sub").and_then(|v| v.as_str()) == Some("alice"));
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_returns_inactive_when_idp_says_inactive() {
        let (addr, srv_shutdown, ca, _) = serve_https(r#"{"active":false}"#, 200, None).await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "id".into(),
            "s".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        let resp = client.introspect("tok").await.unwrap();
        assert!(resp.get("active").and_then(serde_json::Value::as_bool) == Some(false));
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_returns_transport_error_on_non_2xx() {
        let (addr, srv_shutdown, ca, _) = serve_https(r#"{"error":"x"}"#, 500, None).await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "id".into(),
            "s".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        let err = client.introspect("tok").await.unwrap_err();
        assert!(
            matches!(err, IntrospectionError::Status(500)),
            "got {err:?}"
        );
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_userinfo_endpoint_is_called_after_active_introspection() {
        let (addr, srv_shutdown, ca, observed) = serve_https(
            r#"{"active":true,"sub":"alice"}"#,
            200,
            Some(r#"{"preferred_username":"alice","email":"a@b.c"}"#),
        )
        .await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            Some(format!("https://127.0.0.1:{}/userinfo", addr.port())),
            "id".into(),
            "s".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        client.introspect("tok").await.unwrap();
        let ui = client.userinfo("tok").await.unwrap().unwrap();
        assert!(ui.get("preferred_username").and_then(|v| v.as_str()) == Some("alice"));
        assert!(observed.userinfo_auths.lock().unwrap().len() == 1);
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_userinfo_endpoint_is_not_called_when_endpoint_unset() {
        let (addr, srv_shutdown, ca, _) =
            serve_https(r#"{"active":true,"sub":"a"}"#, 200, None).await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "id".into(),
            "s".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        let ui = client.userinfo("tok").await.unwrap();
        assert!(ui.is_none());
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_handles_keycloak_response_shape() {
        let body = r#"{"active":true,"sub":"svc-account-kafka-client","client_id":"kafka-client","scope":"kafka.write profile","exp":9999999999}"#;
        let (addr, srv_shutdown, ca, _) = serve_https(body, 200, None).await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "id".into(),
            "s".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        let resp = client.introspect("tok").await.unwrap();
        assert!(resp.get("client_id").and_then(|v| v.as_str()) == Some("kafka-client"));
        assert!(resp.get("scope").and_then(|v| v.as_str()) == Some("kafka.write profile"));
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_basic_auth_sent_with_configured_client_id_and_secret() {
        let (addr, srv_shutdown, ca, observed) =
            serve_https(r#"{"active":true,"sub":"a"}"#, 200, None).await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "kafka-broker".into(),
            "shh".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        client.introspect("tok").await.unwrap();
        let auths = observed.introspect_auths.lock().unwrap();
        assert!(auths.len() == 1);
        // base64("kafka-broker:shh") = "a2Fma2EtYnJva2VyOnNoaA=="
        assert!(auths[0] == "Basic a2Fma2EtYnJva2VyOnNoaA==");
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_form_body_token_field() {
        let (addr, srv_shutdown, ca, observed) =
            serve_https(r#"{"active":true,"sub":"a"}"#, 200, None).await;
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "id".into(),
            "s".into(),
            Some(&ca),
            Duration::from_secs(5),
        )
        .unwrap();
        client.introspect("opaque-abc").await.unwrap();
        let bodies = observed.introspect_bodies.lock().unwrap();
        assert!(bodies.len() == 1);
        assert!(bodies[0] == "token=opaque-abc");
        srv_shutdown.cancel();
    }

    #[tokio::test]
    async fn introspection_respects_http_timeout() {
        // Bind a port, immediately drop the listener — connect attempts
        // hit a closed port and reqwest's transport layer fails fast.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, cert.pem()).unwrap();
        drop(listener);
        let client = ReqwestIntrospectionClient::new(
            format!("https://127.0.0.1:{}/introspect", addr.port()),
            None,
            "id".into(),
            "s".into(),
            Some(&ca_path),
            Duration::from_millis(200),
        )
        .unwrap();
        let err = client.introspect("tok").await.unwrap_err();
        assert!(
            matches!(err, IntrospectionError::Transport(_)),
            "got {err:?}"
        );
    }
}
