//! `sspi`-rs-backed implementations of [`GssAcceptor`] and [`GssInitiator`].
//!
//! KEYTAB-CLIENT-AUTH DECISION: sspi 0.21's client AS-exchange derives the
//! client's long-term key from a *password + salt* (`generate_key_from_password`
//! in `kerberos/client/generators.rs`); its `Credentials` enum only carries
//! `AuthIdentity{username,password}` or `SmartCard`. There is NO entry point to
//! inject a raw keytab key for the *initiator*. The raw-key path
//! (`Secret::new(..)`) exists only on `ServerProperties` (the acceptor). So
//! [`SspiInitiator`] is PASSWORD-based here. CONCERN: the inter-broker initiate
//! path (a later task) needs keytab-based client auth — an unresolved sspi gap.
//! The acceptor, by contrast, uses the keytab key as designed.

use std::sync::Mutex;

use sspi::kerberos::ServerProperties;
use sspi::{
    AuthIdentity, BufferType, ClientRequestFlags, CredentialUse, Credentials, CredentialsBuffers,
    DataRepresentation, EncryptionFlags, Kerberos, KerberosConfig, Secret, SecurityBuffer,
    SecurityBufferRef, SecurityStatus, ServerRequestFlags, Sspi, SspiImpl, Username,
};

use super::keytab::{ENCTYPE_AES256_CTS_HMAC_SHA1_96, load_service_key};
use super::{AcceptStep, GssAcceptor, GssError, GssInitiator, InitStep};

/// Default KDC URL used when `SSPI_KDC_URL` is unset. The accept path does not
/// hit the network, but `KerberosConfig::new` requires a URL string.
const DEFAULT_KDC_URL: &str = "tcp://localhost:88";

/// Max clock skew tolerated when validating an incoming AP-REQ.
// `Duration::from_mins` is still unstable; spell the skew out in seconds.
#[allow(clippy::duration_suboptimal_units)]
const MAX_TIME_SKEW: std::time::Duration = std::time::Duration::from_secs(300);

fn kdc_url_from_env() -> String {
    std::env::var("SSPI_KDC_URL").unwrap_or_else(|_| DEFAULT_KDC_URL.to_string())
}

/// Map an sspi context error to [`GssError::Context`].
fn ctx_err(e: impl std::fmt::Display) -> GssError {
    GssError::Context(e.to_string())
}

/// Map an sspi wrap/unwrap error to [`GssError::Wrap`].
fn wrap_err(e: impl std::fmt::Display) -> GssError {
    GssError::Wrap(e.to_string())
}

/// GSS wrap (`encrypt_message`) with confidentiality disabled, per RFC 4752.
/// Returns `token || data`.
fn gss_wrap(ctx: &mut Kerberos, plaintext: &[u8]) -> Result<Vec<u8>, GssError> {
    let trailer_len = ctx
        .query_context_sizes()
        .map_err(wrap_err)?
        .security_trailer as usize;
    let mut token = vec![0u8; trailer_len];
    let mut data = plaintext.to_vec();
    let mut wrap_buf = [
        SecurityBufferRef::token_buf(token.as_mut_slice()),
        SecurityBufferRef::data_buf(data.as_mut_slice()),
    ];
    ctx.encrypt_message(EncryptionFlags::WRAP_NO_ENCRYPT, &mut wrap_buf)
        .map_err(wrap_err)?;
    let mut out = wrap_buf[0].data().to_vec();
    out.extend_from_slice(wrap_buf[1].data());
    Ok(out)
}

/// GSS unwrap (`decrypt_message`). Input is the `token || data` stream.
fn gss_unwrap(ctx: &mut Kerberos, token_bytes: &[u8]) -> Result<Vec<u8>, GssError> {
    let mut stream = token_bytes.to_vec();
    let mut unwrap_buf = [
        SecurityBufferRef::stream_buf(stream.as_mut_slice()),
        SecurityBufferRef::data_buf(&mut []),
    ];
    ctx.decrypt_message(&mut unwrap_buf).map_err(wrap_err)?;
    Ok(unwrap_buf[1].data().to_vec())
}

/// Treat a status as "context established" (vs. needing another round trip).
fn is_established(status: SecurityStatus) -> bool {
    matches!(
        status,
        SecurityStatus::Ok | SecurityStatus::CompleteNeeded | SecurityStatus::CompleteAndContinue
    )
}

/// Server-side GSSAPI acceptor backed by sspi + a keytab-extracted service key.
pub struct SspiAcceptor {
    server: Mutex<Kerberos>,
}

impl SspiAcceptor {
    /// Build an acceptor from a keytab file and the SPN's first component.
    ///
    /// Reads `keytab_path`, extracts the highest-kvno aes256 key for
    /// `service_name`, reconstructs the SPN components, and builds the sspi
    /// server context.
    ///
    /// # Errors
    ///
    /// Returns [`GssError::Keytab`] if the keytab cannot be read or has no
    /// matching key, or [`GssError::Context`] if sspi rejects the server config.
    pub fn new(keytab_path: &str, service_name: &str) -> Result<Self, GssError> {
        let bytes = std::fs::read(keytab_path)
            .map_err(|e| GssError::Keytab(format!("reading {keytab_path}: {e}")))?;
        let entry = load_service_key(&bytes, service_name, ENCTYPE_AES256_CTS_HMAC_SHA1_96)
            .map_err(|e| GssError::Keytab(e.to_string()))?;

        // sname = SPN components WITHOUT realm, e.g. ["kafka", "localhost"].
        let sname: Vec<&str> = entry.components.iter().map(String::as_str).collect();
        let server_properties =
            ServerProperties::new(&sname, None, MAX_TIME_SKEW, Some(Secret::new(entry.key)))
                .map_err(ctx_err)?;

        let server = Kerberos::new_server_from_config(
            KerberosConfig::new(&kdc_url_from_env(), "crabka-broker".to_string()),
            server_properties,
        )
        .map_err(ctx_err)?;

        Ok(Self {
            server: Mutex::new(server),
        })
    }
}

impl GssAcceptor for SspiAcceptor {
    fn accept(&mut self, client_token: &[u8]) -> Result<AcceptStep, GssError> {
        let server = self.server.get_mut().expect("acceptor mutex poisoned");

        let mut input = vec![SecurityBuffer::new(
            client_token.to_vec(),
            BufferType::Token,
        )];
        let mut output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
        let mut cred_handle: Option<CredentialsBuffers> = None;

        let builder = server
            .accept_security_context()
            .with_credentials_handle(&mut cred_handle)
            .with_context_requirements(ServerRequestFlags::empty())
            .with_target_data_representation(DataRepresentation::Native)
            .with_input(&mut input)
            .with_output(&mut output);
        let result = server
            .accept_security_context_impl(builder)
            .map_err(ctx_err)?
            .resolve_to_result()
            .map_err(ctx_err)?;

        let out_token = output[0].buffer.clone();
        if is_established(result.status) {
            Ok(AcceptStep::Established(
                (!out_token.is_empty()).then_some(out_token),
            ))
        } else {
            Ok(AcceptStep::Continue(out_token))
        }
    }

    fn wrap(&self, plaintext: &[u8], _confidential: bool) -> Result<Vec<u8>, GssError> {
        let mut server = self.server.lock().expect("acceptor mutex poisoned");
        gss_wrap(&mut server, plaintext)
    }

    fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, GssError> {
        let mut server = self.server.lock().expect("acceptor mutex poisoned");
        gss_unwrap(&mut server, token)
    }

    fn src_principal(&self) -> Result<String, GssError> {
        let mut server = self.server.lock().expect("acceptor mutex poisoned");
        let names = server
            .query_context_names()
            .map_err(|_| GssError::NoSrcPrincipal)?;
        Ok(names.username.inner().to_string())
    }
}

/// Client-side GSSAPI initiator backed by sspi.
///
/// PASSWORD-based (see the module-level decision comment): sspi 0.21 cannot
/// initiate from a keytab key. The client context and credentials handle must
/// persist across [`GssInitiator::step`] calls, so they live in this struct.
pub struct SspiInitiator {
    client: Mutex<Kerberos>,
    cred_handle: Mutex<<Kerberos as SspiImpl>::CredentialsHandle>,
    target_spn: String,
}

impl SspiInitiator {
    /// Build a password-based initiator.
    ///
    /// `client_principal` is e.g. `"alice@CRABKA.TEST"`, `target_spn` is the
    /// service SPN without realm, e.g. `"kafka/localhost"`, and `kdc_url` is the
    /// KDC endpoint (e.g. `"tcp://localhost:88"`).
    ///
    /// # Errors
    ///
    /// Returns [`GssError::Context`] if sspi rejects the principal, credentials,
    /// or client configuration.
    pub fn new(
        client_principal: &str,
        password: &str,
        target_spn: &str,
        kdc_url: &str,
    ) -> Result<Self, GssError> {
        let mut client = Kerberos::new_client_from_config(KerberosConfig::new(
            kdc_url,
            "crabka-broker".to_string(),
        ))
        .map_err(ctx_err)?;

        let identity = AuthIdentity {
            username: Username::parse(client_principal).map_err(ctx_err)?,
            password: password.to_string().into(),
        };
        let creds: Credentials = identity.into();
        let cred_handle = client
            .acquire_credentials_handle()
            .with_credential_use(CredentialUse::Outbound)
            .with_auth_data(&creds)
            .execute(&mut client)
            .map_err(ctx_err)?
            .credentials_handle;

        Ok(Self {
            client: Mutex::new(client),
            cred_handle: Mutex::new(cred_handle),
            target_spn: target_spn.to_string(),
        })
    }
}

impl GssInitiator for SspiInitiator {
    fn step(&mut self, server_token: Option<&[u8]>) -> Result<InitStep, GssError> {
        let client = self.client.get_mut().expect("initiator mutex poisoned");
        let cred_handle = self
            .cred_handle
            .get_mut()
            .expect("initiator mutex poisoned");

        let mut input = vec![SecurityBuffer::new(
            server_token.map(<[u8]>::to_vec).unwrap_or_default(),
            BufferType::Token,
        )];
        let mut output = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];

        let mut builder = client
            .initialize_security_context()
            .with_credentials_handle(cred_handle)
            .with_context_requirements(ClientRequestFlags::MUTUAL_AUTH)
            .with_target_data_representation(DataRepresentation::Native)
            .with_target_name(&self.target_spn)
            .with_input(&mut input)
            .with_output(&mut output);
        let result = client
            .initialize_security_context_impl(&mut builder)
            .map_err(ctx_err)?
            .resolve_with_default_network_client()
            .map_err(ctx_err)?;

        let out_token = output[0].buffer.clone();
        if is_established(result.status) {
            Ok(InitStep::Established(
                (!out_token.is_empty()).then_some(out_token),
            ))
        } else {
            Ok(InitStep::Continue(out_token))
        }
    }

    fn wrap(&self, plaintext: &[u8], _confidential: bool) -> Result<Vec<u8>, GssError> {
        let mut client = self.client.lock().expect("initiator mutex poisoned");
        gss_wrap(&mut client, plaintext)
    }

    fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, GssError> {
        let mut client = self.client.lock().expect("initiator mutex poisoned");
        gss_unwrap(&mut client, token)
    }
}
