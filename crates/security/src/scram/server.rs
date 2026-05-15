//! `ScramServerExchange` — RFC 5802 SCRAM-SHA-512 server state machine.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha512};
use subtle::ConstantTimeEq;

use super::ScramCredential;
use crate::{AuthError, Principal, SaslMechanism};

#[derive(Debug)]
enum State {
    AwaitingClientFirst,
    AwaitingClientFinal {
        client_first_bare: String,
        server_first: String,
    },
    Finished,
}

#[derive(Debug)]
pub struct ScramServerExchange {
    username: String,
    credential: ScramCredential,
    state: State,
}

#[derive(Debug)]
pub enum StepResult {
    Continue(Vec<u8>),
    Done(Principal, Vec<u8>),
    Failed(AuthError),
}

impl ScramServerExchange {
    #[must_use]
    pub fn new(username: String, credential: ScramCredential) -> Self {
        Self {
            username,
            credential,
            state: State::AwaitingClientFirst,
        }
    }

    pub fn step(&mut self, client_bytes: &[u8]) -> StepResult {
        match std::mem::replace(&mut self.state, State::Finished) {
            State::AwaitingClientFirst => self.step_first(client_bytes),
            State::AwaitingClientFinal {
                client_first_bare,
                server_first,
            } => self.step_final(client_bytes, &client_first_bare, &server_first),
            State::Finished => StepResult::Failed(AuthError::MalformedMessage),
        }
    }

    fn step_first(&mut self, client_bytes: &[u8]) -> StepResult {
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
        self.state = State::AwaitingClientFinal {
            client_first_bare: bare.to_string(),
            server_first,
        };
        StepResult::Continue(response)
    }

    fn step_final(
        &mut self,
        client_bytes: &[u8],
        client_first_bare: &str,
        server_first: &str,
    ) -> StepResult {
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
        let (Some(_cb), Some(_nonce), Some(proof_b64)) = (channel_binding, nonce, proof_b64) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        let proof = match B64.decode(proof_b64) {
            Ok(b) if b.len() == 64 => b,
            _ => return StepResult::Failed(AuthError::MalformedMessage),
        };

        // client-final-without-proof = everything before ",p="
        let Some(cf_no_proof_end) = s.rfind(",p=") else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        let client_final_no_proof = &s[..cf_no_proof_end];

        let auth_message = format!("{client_first_bare},{server_first},{client_final_no_proof}");

        // client_signature = HMAC(stored_key, auth_message)
        let Ok(mut mac) = <Hmac<Sha512>>::new_from_slice(&self.credential.stored_key) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        mac.update(auth_message.as_bytes());
        let client_signature = mac.finalize().into_bytes();

        // client_key = client_signature XOR proof
        let client_key: Vec<u8> = client_signature
            .iter()
            .zip(proof.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        // stored_key = H(client_key)
        let computed_stored = Sha512::digest(&client_key);
        if computed_stored
            .ct_eq(self.credential.stored_key.as_slice())
            .unwrap_u8()
            != 1
        {
            return StepResult::Failed(AuthError::BadProof);
        }
        // server_signature = HMAC(server_key, auth_message)
        let mut server_mac =
            <Hmac<Sha512>>::new_from_slice(&self.credential.server_key).expect("hmac");
        server_mac.update(auth_message.as_bytes());
        let server_signature = server_mac.finalize().into_bytes();
        let server_final = format!("v={}", B64.encode(server_signature));
        StepResult::Done(
            Principal {
                name: self.username.clone(),
                mechanism: SaslMechanism::ScramSha512,
            },
            server_final.into_bytes(),
        )
    }
}
