//! `ScramClientExchange`, the RFC 5802 SCRAM client state machine.
//!
//! It supports SCRAM-SHA-256 and SCRAM-SHA-512. The mechanism is fixed at
//! construction.

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, KeyInit, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

use crate::{AuthError, SaslMechanism};

/// RFC 5802 SCRAM client-side handshake, initial phase.
#[derive(Debug)]
pub struct ScramClientExchange {
    username: String,
    password: Vec<u8>,
    mechanism: SaslMechanism,
}

/// Post-client-first phase: awaiting the server-first message.
#[derive(Debug)]
pub struct AwaitingServerFirst {
    username: String,
    password: Vec<u8>,
    mechanism: SaslMechanism,
    client_first_bare: String,
    client_nonce: String,
}

/// Post-client-final phase: awaiting the server-final message.
#[derive(Debug)]
pub struct AwaitingServerFinal {
    mechanism: SaslMechanism,
    username: String,
    auth_message: String,
    server_key: Vec<u8>,
}

impl ScramClientExchange {
    #[must_use]
    /// # Panics
    /// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
    pub fn new(username: String, password: Vec<u8>, mechanism: SaslMechanism) -> Self {
        assert2::assert!(mechanism.is_scram());
        Self {
            username,
            password,
            mechanism,
        }
    }

    // SCRAM client-first. skip_all keeps the stored `password` out of span
    // fields; only the non-sensitive mechanism + username are recorded.
    //
    // `AwaitingServerFirst` is intentionally not re-exported: callers thread
    // it through via type inference (`let (bytes, exch) = x.client_first()?`)
    // without ever naming the phase type, so the typestate chain can't be
    // driven out of order.
    #[allow(private_interfaces)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(mechanism = %self.mechanism.wire_name(), principal = %self.username),
        err
    )]
    /// # Errors
    /// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
    pub fn client_first(self) -> Result<(Vec<u8>, AwaitingServerFirst), AuthError> {
        let mut nonce_bytes = [0u8; 18];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| AuthError::MalformedMessage)?;
        let client_nonce = B64.encode(nonce_bytes);
        let bare = format!("n={},r={}", self.username, client_nonce);
        let msg = format!("n,,{bare}");
        let next = AwaitingServerFirst {
            username: self.username,
            password: self.password,
            mechanism: self.mechanism,
            client_first_bare: bare,
            client_nonce,
        };
        Ok((msg.into_bytes(), next))
    }
}

impl AwaitingServerFirst {
    // SCRAM client processing of server-first. skip_all keeps the stored
    // `password` and the raw `server_bytes` out of span fields.
    //
    // `AwaitingServerFinal` is intentionally not re-exported; see
    // `ScramClientExchange::client_first`.
    #[allow(private_interfaces)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(mechanism = %self.mechanism.wire_name(), principal = %self.username),
        err
    )]
    pub fn step(self, server_bytes: &[u8]) -> Result<(Vec<u8>, AwaitingServerFinal), AuthError> {
        let s = std::str::from_utf8(server_bytes).map_err(|_| AuthError::MalformedMessage)?;
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for attr in s.split(',') {
            if let Some(v) = attr.strip_prefix("r=") {
                nonce = Some(v.to_string());
            } else if let Some(v) = attr.strip_prefix("s=") {
                salt = Some(B64.decode(v).map_err(|_| AuthError::MalformedMessage)?);
            } else if let Some(v) = attr.strip_prefix("i=") {
                iterations = Some(v.parse::<u32>().map_err(|_| AuthError::MalformedMessage)?);
            }
        }
        let (Some(combined_nonce), Some(salt), Some(iters)) = (nonce, salt, iterations) else {
            return Err(AuthError::MalformedMessage);
        };
        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err(AuthError::BadProof);
        }

        let channel_binding = B64.encode(b"n,,");
        let client_final_no_proof = format!("c={channel_binding},r={combined_nonce}");
        let auth_message = format!("{},{s},{client_final_no_proof}", self.client_first_bare);

        let (proof, server_key) = match self.mechanism {
            SaslMechanism::ScramSha512 => {
                compute_proof_sha512(&self.password, &salt, iters, auth_message.as_bytes())?
            }
            SaslMechanism::ScramSha256 => {
                compute_proof_sha256(&self.password, &salt, iters, auth_message.as_bytes())?
            }
            SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
                return Err(AuthError::MalformedMessage);
            }
        };

        let client_final = format!("{client_final_no_proof},p={}", B64.encode(&proof));
        let next = AwaitingServerFinal {
            mechanism: self.mechanism,
            username: self.username,
            auth_message,
            server_key,
        };
        Ok((client_final.into_bytes(), next))
    }
}

impl AwaitingServerFinal {
    // SCRAM client verification of server-final (server signature). skip_all
    // keeps the raw `server_bytes` out of span fields. Terminal: no further
    // exchange state to hold.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(mechanism = %self.mechanism.wire_name(), principal = %self.username),
        err
    )]
    pub fn verify_server_final(self, server_bytes: &[u8]) -> Result<(), AuthError> {
        let s = std::str::from_utf8(server_bytes).map_err(|_| AuthError::MalformedMessage)?;
        let v_b64 = s.strip_prefix("v=").ok_or(AuthError::MalformedMessage)?;
        let v = B64.decode(v_b64).map_err(|_| AuthError::MalformedMessage)?;
        let expected: Vec<u8> = match self.mechanism {
            SaslMechanism::ScramSha512 => {
                let mut mac = <Hmac<Sha512>>::new_from_slice(&self.server_key)
                    .map_err(|_| AuthError::MalformedMessage)?;
                mac.update(self.auth_message.as_bytes());
                mac.finalize().into_bytes().to_vec()
            }
            SaslMechanism::ScramSha256 => {
                let mut mac = <Hmac<Sha256>>::new_from_slice(&self.server_key)
                    .map_err(|_| AuthError::MalformedMessage)?;
                mac.update(self.auth_message.as_bytes());
                mac.finalize().into_bytes().to_vec()
            }
            SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
                return Err(AuthError::MalformedMessage);
            }
        };
        if expected.ct_eq(&v).unwrap_u8() != 1 {
            return Err(AuthError::BadProof);
        }
        Ok(())
    }
}

fn compute_proof_sha512(
    password: &[u8],
    salt: &[u8],
    iters: u32,
    auth_message: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), AuthError> {
    let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, salt, iters);
    let mut client_key_mac =
        <Hmac<Sha512>>::new_from_slice(&salted).map_err(|_| AuthError::MalformedMessage)?;
    client_key_mac.update(b"Client Key");
    let client_key = client_key_mac.finalize().into_bytes();
    let stored_key = Sha512::digest(client_key);
    let mut server_key_mac =
        <Hmac<Sha512>>::new_from_slice(&salted).map_err(|_| AuthError::MalformedMessage)?;
    server_key_mac.update(b"Server Key");
    let server_key = server_key_mac.finalize().into_bytes().to_vec();

    let mut client_sig_mac =
        <Hmac<Sha512>>::new_from_slice(&stored_key).map_err(|_| AuthError::MalformedMessage)?;
    client_sig_mac.update(auth_message);
    let client_signature = client_sig_mac.finalize().into_bytes();
    let proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    Ok((proof, server_key))
}

fn compute_proof_sha256(
    password: &[u8],
    salt: &[u8],
    iters: u32,
    auth_message: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), AuthError> {
    let salted: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(password, salt, iters);
    let mut client_key_mac =
        <Hmac<Sha256>>::new_from_slice(&salted).map_err(|_| AuthError::MalformedMessage)?;
    client_key_mac.update(b"Client Key");
    let client_key = client_key_mac.finalize().into_bytes();
    let stored_key = Sha256::digest(client_key);
    let mut server_key_mac =
        <Hmac<Sha256>>::new_from_slice(&salted).map_err(|_| AuthError::MalformedMessage)?;
    server_key_mac.update(b"Server Key");
    let server_key = server_key_mac.finalize().into_bytes().to_vec();

    let mut client_sig_mac =
        <Hmac<Sha256>>::new_from_slice(&stored_key).map_err(|_| AuthError::MalformedMessage)?;
    client_sig_mac.update(auth_message);
    let client_signature = client_sig_mac.finalize().into_bytes();
    let proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    Ok((proof, server_key))
}
