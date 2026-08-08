//! One-shot Linux kTLS support probe (Increment F).
//!
//! `ktls::config_ktls_server` consumes the post-handshake `TlsStream` **by
//! value**. If it fails mid-stream, the connection cannot fall back to
//! userspace TLS, because the stream is already moved or partly consumed. The
//! broker does not take that risk per connection. It probes kTLS support
//! **once at startup** and stores the result in `Broker::ktls_enabled`. The
//! per-connection path tries the kTLS transition only when the probe
//! succeeded. If the probe failed, it serves the exact userspace rustls path.
//!
//! The probe is authoritative. It runs a real loopback TLS 1.3 handshake with
//! a throwaway self-signed cert and then drives `config_ktls_server`. This
//! exercises the exact kernel path the data plane uses: `TCP_ULP="tls"` plus
//! the `crypto_info` install for the negotiated AEAD. Anything that would make
//! a production kTLS connection fail makes the probe return `false` here. That
//! includes a kernel below 4.13, an absent `tls` module, and an unmappable
//! cipher suite.
//!
//! On non-Linux targets the whole kTLS feature is `#[cfg]`-compiled out, and
//! the probe is a constant `false`.

/// Probe whether this host supports Linux kTLS TX. Returns `true` only when a
/// full loopback TLS handshake and then `ktls::config_ktls_server` succeed.
/// That means the kernel `tls` module is present and the socket's
/// `crypto_info` accepts the negotiated cipher suite.
#[cfg(target_os = "linux")]
pub(crate) async fn probe_ktls_support() -> bool {
    #[cfg(test)]
    if let Some(result) = take_test_probe_result() {
        return ktls_probe_result_to_bool(result);
    }

    ktls_probe_result_to_bool(try_probe_ktls().await)
}

/// On non-Linux targets kTLS does not exist. The probe is a constant `false`,
/// and TLS listeners always serve the userspace rustls path.
#[cfg(not(target_os = "linux"))]
pub(crate) fn probe_ktls_support() -> std::future::Ready<bool> {
    std::future::ready(false)
}

#[cfg(target_os = "linux")]
async fn try_probe_ktls() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::Arc;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    // 1. Throwaway self-signed cert (ECDSA P-256) with a `localhost` SAN so the
    //    loopback client can verify the server name. Never persisted.
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let params = rcgen::CertificateParams::new(vec!["localhost".to_string()])?;
    let cert = params.self_signed(&key)?;
    let cert_der = rustls::pki_types::CertificateDer::from(cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der())
        .map_err(|e| format!("probe key der: {e}"))?;

    // 2. Server config WITH secret extraction (the prerequisite for kTLS), and
    //    a client config that trusts the throwaway cert. Restrict to TLS 1.3 so
    //    the probe is deterministic across kernels.
    let mut server_cfg =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)?;
    server_cfg.enable_secret_extraction = true;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der)?;
    let client_cfg =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));

    // 3. Loopback TCP pair.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // Client task: connect, complete the TLS handshake, write one byte so the
    // server side has data flowing, then idle until the server drops us.
    let client = tokio::spawn(async move {
        let tcp = TcpStream::connect(addr).await?;
        let server_name = rustls::pki_types::ServerName::try_from("localhost")?;
        let mut tls = connector.connect(server_name, tcp).await?;
        tls.write_all(b"x").await?;
        tls.flush().await?;
        // Keep the connection open until the server finishes the probe.
        let mut buf = [0u8; 1];
        let _ = tls.read(&mut buf).await;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });

    // 4. Server side: accept inside a `CorkStream` (the only way `ktls` can
    //    cleanly drain the rustls buffer), complete the handshake, then run the
    //    real `config_ktls_server`. Success here means production kTLS works.
    let (tcp, _) = listener.accept().await?;
    let tls = acceptor.accept(ktls::CorkStream::new(tcp)).await?;
    // This is the exact call the data plane makes; it installs `TCP_ULP="tls"`
    // and the AEAD `crypto_info` into the kernel. If it returns Ok, kTLS is
    // usable on this host.
    let ktls_stream = ktls::config_ktls_server(tls).await?;

    // Drop the kTLS stream (closes the probe socket) and tear the client down.
    drop(ktls_stream);
    client.abort();
    Ok(())
}

#[cfg(target_os = "linux")]
fn ktls_probe_result_to_bool(result: Result<(), Box<dyn std::error::Error + Send + Sync>>) -> bool {
    match result {
        Ok(()) => true,
        Err(e) => {
            tracing::debug!(error = %e, "kTLS startup probe failed; falling back to userspace TLS");
            false
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
static TEST_PROBE_RESULT: std::sync::Mutex<Option<TestProbeResult>> = std::sync::Mutex::new(None);

#[cfg(all(test, target_os = "linux"))]
enum TestProbeResult {
    Success,
    Failure,
}

#[cfg(all(test, target_os = "linux"))]
fn set_test_probe_result(result: TestProbeResult) {
    *TEST_PROBE_RESULT.lock().expect("test probe mutex") = Some(result);
}

#[cfg(all(test, target_os = "linux"))]
fn take_test_probe_result() -> Option<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    TEST_PROBE_RESULT
        .lock()
        .expect("test probe mutex")
        .take()
        .map(|result| match result {
            TestProbeResult::Success => Ok(()),
            TestProbeResult::Failure => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "kTLS unavailable",
            ))
                as Box<dyn std::error::Error + Send + Sync>),
        })
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[cfg(target_os = "linux")]
    async fn linux_probe_maps_injected_results() {
        super::set_test_probe_result(super::TestProbeResult::Failure);
        assert!(!super::probe_ktls_support().await);

        super::set_test_probe_result(super::TestProbeResult::Success);
        assert!(super::probe_ktls_support().await);
    }
}
