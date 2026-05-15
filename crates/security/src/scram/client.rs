//! `ScramClientExchange` — RFC 5802 SCRAM-SHA-512 client state machine.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, KeyInit, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha512};
use subtle::ConstantTimeEq;

use crate::AuthError;

#[derive(Debug)]
enum State {
    Initial,
    AwaitingServerFirst {
        client_first_bare: String,
        client_nonce: String,
    },
    AwaitingServerFinal {
        auth_message: String,
        server_key: Vec<u8>,
    },
    Finished,
}

#[derive(Debug)]
pub struct ScramClientExchange {
    username: String,
    password: Vec<u8>,
    state: State,
}

impl ScramClientExchange {
    #[must_use]
    pub fn new(username: String, password: Vec<u8>) -> Self {
        Self {
            username,
            password,
            state: State::Initial,
        }
    }

    pub fn client_first(&mut self) -> Result<Vec<u8>, AuthError> {
        if !matches!(self.state, State::Initial) {
            return Err(AuthError::MalformedMessage);
        }
        let mut nonce_bytes = [0u8; 18];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| AuthError::MalformedMessage)?;
        let client_nonce = B64.encode(nonce_bytes);
        let bare = format!("n={},r={}", self.username, client_nonce);
        let msg = format!("n,,{bare}");
        self.state = State::AwaitingServerFirst {
            client_first_bare: bare,
            client_nonce,
        };
        Ok(msg.into_bytes())
    }

    pub fn step(&mut self, server_bytes: &[u8]) -> Result<Vec<u8>, AuthError> {
        let State::AwaitingServerFirst {
            client_first_bare,
            client_nonce,
        } = std::mem::replace(&mut self.state, State::Finished)
        else {
            return Err(AuthError::MalformedMessage);
        };
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
        if !combined_nonce.starts_with(&client_nonce) {
            return Err(AuthError::BadProof);
        }

        let salted: [u8; 64] =
            pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(&self.password, &salt, iters);
        let mut client_key_mac =
            <Hmac<Sha512>>::new_from_slice(&salted).map_err(|_| AuthError::MalformedMessage)?;
        client_key_mac.update(b"Client Key");
        let client_key = client_key_mac.finalize().into_bytes();
        let stored_key = Sha512::digest(client_key);
        let mut server_key_mac =
            <Hmac<Sha512>>::new_from_slice(&salted).map_err(|_| AuthError::MalformedMessage)?;
        server_key_mac.update(b"Server Key");
        let server_key = server_key_mac.finalize().into_bytes().to_vec();

        let channel_binding = B64.encode(b"n,,");
        let client_final_no_proof = format!("c={channel_binding},r={combined_nonce}");
        let auth_message = format!("{client_first_bare},{s},{client_final_no_proof}");
        let mut client_sig_mac =
            <Hmac<Sha512>>::new_from_slice(&stored_key).map_err(|_| AuthError::MalformedMessage)?;
        client_sig_mac.update(auth_message.as_bytes());
        let client_signature = client_sig_mac.finalize().into_bytes();
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let client_final = format!("{client_final_no_proof},p={}", B64.encode(&proof));
        self.state = State::AwaitingServerFinal {
            auth_message,
            server_key,
        };
        Ok(client_final.into_bytes())
    }

    pub fn verify_server_final(&mut self, server_bytes: &[u8]) -> Result<(), AuthError> {
        let State::AwaitingServerFinal {
            auth_message,
            server_key,
        } = std::mem::replace(&mut self.state, State::Finished)
        else {
            return Err(AuthError::MalformedMessage);
        };
        let s = std::str::from_utf8(server_bytes).map_err(|_| AuthError::MalformedMessage)?;
        let v_b64 = s.strip_prefix("v=").ok_or(AuthError::MalformedMessage)?;
        let v = B64.decode(v_b64).map_err(|_| AuthError::MalformedMessage)?;
        let mut mac =
            <Hmac<Sha512>>::new_from_slice(&server_key).map_err(|_| AuthError::MalformedMessage)?;
        mac.update(auth_message.as_bytes());
        let expected = mac.finalize().into_bytes();
        if expected.ct_eq(&v).unwrap_u8() != 1 {
            return Err(AuthError::BadProof);
        }
        Ok(())
    }
}
