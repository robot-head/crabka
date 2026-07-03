//! `ScramServerExchange` — RFC 5802 SCRAM server state machine.
//! Supports SCRAM-SHA-256 and SCRAM-SHA-512; the mechanism comes from
//! the credential being authenticated against.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, KeyInit, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

use super::{ScramCredential, scram_hash_len};
use crate::{AuthError, AuthMethod, Principal, SaslMechanism};

struct AwaitingClientFirst {
    username: String,
    credential: ScramCredential,
    principal_override: Option<Principal>,
}

struct AwaitingClientFinal {
    username: String,
    credential: ScramCredential,
    principal_override: Option<Principal>,
    client_first_bare: String,
    server_first: String,
}

/// RFC 5802 SCRAM server-side handshake, one type per negotiation phase.
///
/// The variant payload types are intentionally not exported: the phase is
/// driven entirely through `step`/`StepResult`, so callers never need to
/// name `AwaitingClientFirst`/`AwaitingClientFinal` directly.
#[allow(private_interfaces)]
pub enum ScramServerExchange {
    AwaitingClientFirst(AwaitingClientFirst),
    AwaitingClientFinal(AwaitingClientFinal),
}

impl std::fmt::Debug for ScramServerExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AwaitingClientFirst(_) => f
                .debug_tuple("ScramServerExchange::AwaitingClientFirst")
                .finish(),
            Self::AwaitingClientFinal(_) => f
                .debug_tuple("ScramServerExchange::AwaitingClientFinal")
                .finish(),
        }
    }
}

#[derive(Debug)]
#[must_use]
pub enum StepResult {
    Continue(Vec<u8>, ScramServerExchange),
    Done(Principal, Vec<u8>),
    Failed(AuthError),
}

impl ScramServerExchange {
    #[must_use]
    pub fn new(username: String, credential: ScramCredential) -> Self {
        Self::AwaitingClientFirst(AwaitingClientFirst {
            username,
            credential,
            principal_override: None,
        })
    }

    /// KIP-48: variant of [`Self::new`] that stamps a
    /// principal to be returned by the `Done` arm in place of one
    /// synthesized from `username`. Used by the delegation-token SCRAM
    /// fallback to surface the token's owner (e.g. `User:alice`) when
    /// the client authenticated with the token's UUID `token_id` as the
    /// SCRAM username.
    #[must_use]
    pub fn new_with_principal(
        username: String,
        credential: ScramCredential,
        override_principal: Principal,
    ) -> Self {
        Self::AwaitingClientFirst(AwaitingClientFirst {
            username,
            credential,
            principal_override: Some(override_principal),
        })
    }

    /// Feed one client message and advance the negotiation.
    pub fn step(self, client_bytes: &[u8]) -> StepResult {
        match self {
            Self::AwaitingClientFirst(s) => s.step(client_bytes),
            Self::AwaitingClientFinal(s) => s.step(client_bytes),
        }
    }
}

impl AwaitingClientFirst {
    // Per-step SCRAM server driver. skip_all keeps the raw `client_bytes`
    // (client-first carrying nonce) out of span fields; only the
    // non-sensitive mechanism + username are recorded. Returns a
    // `StepResult` enum (not a Result), so no `err`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            mechanism = %self.credential.mechanism.wire_name(),
            principal = %self.username,
        )
    )]
    fn step(self, client_bytes: &[u8]) -> StepResult {
        let Ok(s) = std::str::from_utf8(client_bytes) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        // GS2 header "n,," then bare client-first
        let Some(bare) = s.strip_prefix("n,,") else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        let mut user = None;
        let mut nonce = None;
        for attr in bare.split(',') {
            if let Some(v) = attr.strip_prefix("n=") {
                user = Some(v.to_string());
            } else if let Some(v) = attr.strip_prefix("r=") {
                nonce = Some(v.to_string());
            }
        }
        let (Some(u), Some(c_nonce)) = (user, nonce) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        if u != self.username {
            return StepResult::Failed(AuthError::UnknownUser);
        }
        let mut server_nonce_bytes = [0u8; 18];
        SystemRandom::new()
            .fill(&mut server_nonce_bytes)
            .expect("rng");
        let server_nonce = B64.encode(server_nonce_bytes);
        let combined_nonce = format!("{c_nonce}{server_nonce}");
        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            B64.encode(&self.credential.salt),
            self.credential.iterations,
        );
        let response = server_first.clone().into_bytes();
        let next = AwaitingClientFinal {
            username: self.username,
            credential: self.credential,
            principal_override: self.principal_override,
            client_first_bare: bare.to_string(),
            server_first,
        };
        StepResult::Continue(response, ScramServerExchange::AwaitingClientFinal(next))
    }
}

impl AwaitingClientFinal {
    // Per-step SCRAM server driver. skip_all keeps the raw `client_bytes`
    // (client-final carrying proof) out of span fields; only the
    // non-sensitive mechanism + username are recorded. Terminal: returns
    // `Done`/`Failed` only. Returns a `StepResult` enum (not a Result), so
    // no `err`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            mechanism = %self.credential.mechanism.wire_name(),
            principal = %self.username,
        )
    )]
    fn step(self, client_bytes: &[u8]) -> StepResult {
        let client_first_bare = &self.client_first_bare;
        let server_first = &self.server_first;
        let Ok(s) = std::str::from_utf8(client_bytes) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        let mut channel_binding = None;
        let mut nonce = None;
        let mut proof_b64 = None;
        for attr in s.split(',') {
            if let Some(v) = attr.strip_prefix("c=") {
                channel_binding = Some(v);
            } else if let Some(v) = attr.strip_prefix("r=") {
                nonce = Some(v);
            } else if let Some(v) = attr.strip_prefix("p=") {
                proof_b64 = Some(v);
            }
        }
        let (Some(cb), Some(nonce), Some(proof_b64)) = (channel_binding, nonce, proof_b64) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };

        // RFC 5802 §5.1: the client-final `r=` (combined nonce) must
        // equal the nonce the server issued in server-first
        // (client-first-nonce + server-nonce). server-first is
        // `r={combined_nonce},s=...,i=...`, so the expected value is the
        // `r=` attribute up to the first comma.
        let expected_nonce = server_first
            .strip_prefix("r=")
            .and_then(|rest| rest.split(',').next())
            .unwrap_or_default();
        if nonce != expected_nonce {
            return StepResult::Failed(AuthError::MalformedMessage);
        }

        // RFC 5802 §5.1: with no channel binding, the GS2 header is
        // `n,,` and `c=` must equal its base64 encoding (`"biws"`).
        if cb != B64.encode(b"n,,") {
            return StepResult::Failed(AuthError::MalformedMessage);
        }

        let expected_proof_len = scram_hash_len(self.credential.mechanism);
        let proof = match B64.decode(proof_b64) {
            Ok(b) if b.len() == expected_proof_len => b,
            _ => return StepResult::Failed(AuthError::MalformedMessage),
        };

        // client-final-without-proof = everything before ",p="
        let Some(cf_no_proof_end) = s.rfind(",p=") else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        let client_final_no_proof = &s[..cf_no_proof_end];

        let auth_message = format!("{client_first_bare},{server_first},{client_final_no_proof}");

        let (computed_stored, server_signature) = match self.credential.mechanism {
            SaslMechanism::ScramSha512 => verify_and_sign_sha512(
                &self.credential.stored_key,
                &self.credential.server_key,
                &proof,
                auth_message.as_bytes(),
            ),
            SaslMechanism::ScramSha256 => verify_and_sign_sha256(
                &self.credential.stored_key,
                &self.credential.server_key,
                &proof,
                auth_message.as_bytes(),
            ),
            SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
                return StepResult::Failed(AuthError::MalformedMessage);
            }
        };

        if computed_stored
            .ct_eq(self.credential.stored_key.as_slice())
            .unwrap_u8()
            != 1
        {
            return StepResult::Failed(AuthError::BadProof);
        }
        let server_final = format!("v={}", B64.encode(&server_signature));
        // KIP-48: prefer the override principal when set
        // (delegation-token SCRAM fallback path). Otherwise build the
        // standard `User:<scram-username>` principal from the live
        // exchange state.
        let principal = self
            .principal_override
            .clone()
            .unwrap_or_else(|| Principal {
                name: self.username.clone(),
                auth_method: AuthMethod::from_sasl(self.credential.mechanism),
                groups: vec![],
            });
        StepResult::Done(principal, server_final.into_bytes())
    }
}

fn verify_and_sign_sha512(
    stored_key: &[u8],
    server_key: &[u8],
    proof: &[u8],
    auth_message: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let mut mac = <Hmac<Sha512>>::new_from_slice(stored_key).expect("hmac");
    mac.update(auth_message);
    let client_signature = mac.finalize().into_bytes();
    let client_key: Vec<u8> = client_signature
        .iter()
        .zip(proof.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let computed_stored = Sha512::digest(&client_key).to_vec();
    let mut server_mac = <Hmac<Sha512>>::new_from_slice(server_key).expect("hmac");
    server_mac.update(auth_message);
    let server_signature = server_mac.finalize().into_bytes().to_vec();
    (computed_stored, server_signature)
}

fn verify_and_sign_sha256(
    stored_key: &[u8],
    server_key: &[u8],
    proof: &[u8],
    auth_message: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let mut mac = <Hmac<Sha256>>::new_from_slice(stored_key).expect("hmac");
    mac.update(auth_message);
    let client_signature = mac.finalize().into_bytes();
    let client_key: Vec<u8> = client_signature
        .iter()
        .zip(proof.iter())
        .map(|(a, b)| a ^ b)
        .collect();
    let computed_stored = Sha256::digest(&client_key).to_vec();
    let mut server_mac = <Hmac<Sha256>>::new_from_slice(server_key).expect("hmac");
    server_mac.update(auth_message);
    let server_signature = server_mac.finalize().into_bytes().to_vec();
    (computed_stored, server_signature)
}
