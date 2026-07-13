//! Server-side SCRAM-SHA-256 (RFC 5802/7677), on `RustCrypto` primitives.

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::{PgError, sqlstate};

type HmacSha256 = Hmac<Sha256>;

pub const DEFAULT_ITERATIONS: u32 = 4096; // PostgreSQL's default

/// Salt length used for both real and mock verifiers. Mock salts MUST match
/// the real salt length so the server-first `s=` field is the same size for
/// known and unknown users (no username-enumeration oracle via salt length).
pub const SALT_LEN: usize = 16;

/// Precomputed SCRAM-SHA-256 verifier — stores no plaintext password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ScramVerifier {
    /// Build a verifier from precomputed SCRAM fields.
    ///
    /// # Errors
    ///
    /// Returns an error when the salt is empty or the iteration count is zero.
    pub fn from_parts(
        salt: Vec<u8>,
        iterations: u32,
        stored_key: [u8; 32],
        server_key: [u8; 32],
    ) -> Result<Self, PgError> {
        if iterations == 0 {
            return Err(PgError::fatal(
                sqlstate::INVALID_AUTHORIZATION_SPECIFICATION,
                "SCRAM: verifier iterations must be greater than zero",
            ));
        }

        Ok(Self {
            salt,
            iterations,
            stored_key,
            server_key,
        })
    }

    #[must_use]
    /// Derive a verifier from a plaintext password.
    ///
    /// # Panics
    ///
    /// Panics only if the SHA-256 HMAC implementation returns a digest with an
    /// unexpected length.
    pub fn from_password(password: &str, salt: Vec<u8>, iterations: u32) -> Self {
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(&client_key).into();
        let server_key: [u8; 32] = hmac(&salted, b"Server Key")
            .try_into()
            .expect("hmac-sha256 is 32 bytes");
        Self {
            salt,
            iterations,
            stored_key,
            server_key,
        }
    }

    /// Deterministic fake verifier for an unknown user (mock authentication).
    #[must_use]
    /// Derive a deterministic mock verifier for an unknown user.
    ///
    /// # Panics
    ///
    /// Panics only if the SHA-256 HMAC implementation returns a digest with an
    /// unexpected length.
    pub fn mock(server_secret: &[u8; 32], user: &str) -> Self {
        let salt = hmac(server_secret, format!("mock-salt:{user}").as_bytes())[..SALT_LEN].to_vec();
        let stored_key: [u8; 32] = hmac(server_secret, format!("mock-stored:{user}").as_bytes())
            .try_into()
            .expect("32 bytes");
        let server_key: [u8; 32] = hmac(server_secret, format!("mock-server:{user}").as_bytes())
            .try_into()
            .expect("32 bytes");
        Self {
            salt,
            iterations: DEFAULT_ITERATIONS,
            stored_key,
            server_key,
        }
    }
}

enum State {
    Initial,
    SentServerFirst {
        client_first_bare: String,
        server_first: String,
        full_nonce: String,
        gs2_header: &'static str,
    },
    Done,
}

pub struct ScramServer {
    verifier: ScramVerifier,
    server_nonce: String,
    state: State,
}

impl ScramServer {
    #[must_use]
    pub fn from_verifier(verifier: ScramVerifier, server_nonce: String) -> Self {
        Self {
            verifier,
            server_nonce,
            state: State::Initial,
        }
    }

    /// Deterministic constructor for tests.
    #[must_use]
    pub fn new_with(password: &str, salt: Vec<u8>, iterations: u32, server_nonce: String) -> Self {
        Self::from_verifier(
            ScramVerifier::from_password(password, salt, iterations),
            server_nonce,
        )
    }

    /// Process a SCRAM client-first message.
    ///
    /// # Errors
    ///
    /// Returns a protocol error for invalid state, encoding, channel-binding
    /// headers, or missing attributes.
    pub fn handle_client_first(&mut self, msg: &[u8]) -> Result<Vec<u8>, PgError> {
        if !matches!(self.state, State::Initial) {
            return Err(PgError::protocol("SCRAM: unexpected client-first message"));
        }
        let msg = std::str::from_utf8(msg)
            .map_err(|_| PgError::protocol("SCRAM: client-first is not UTF-8"))?;

        // gs2 header: we never advertise SCRAM-SHA-256-PLUS, so requiring
        // channel binding ("p=...") is a protocol violation.
        let (gs2_header, bare) = if let Some(rest) = msg.strip_prefix("n,,") {
            ("n,,", rest)
        } else if let Some(rest) = msg.strip_prefix("y,,") {
            ("y,,", rest)
        } else {
            return Err(PgError::protocol(
                "SCRAM: unsupported gs2 header (channel binding not offered)",
            ));
        };

        let client_nonce = attr(bare, 'r')?;
        let full_nonce = format!("{client_nonce}{}", self.server_nonce);
        let server_first = format!(
            "r={full_nonce},s={},i={}",
            B64.encode(&self.verifier.salt),
            self.verifier.iterations
        );

        self.state = State::SentServerFirst {
            client_first_bare: bare.to_string(),
            server_first: server_first.clone(),
            full_nonce,
            gs2_header,
        };
        Ok(server_first.into_bytes())
    }

    /// Process and authenticate a SCRAM client-final message.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid state or encoding, malformed attributes,
    /// mismatched nonces or channel binding, or failed authentication.
    ///
    /// # Panics
    ///
    /// Panics only if the SHA-256 HMAC implementation returns a digest with an
    /// unexpected length.
    pub fn handle_client_final(&mut self, msg: &[u8]) -> Result<Vec<u8>, PgError> {
        let State::SentServerFirst {
            client_first_bare,
            server_first,
            full_nonce,
            gs2_header,
        } = std::mem::replace(&mut self.state, State::Done)
        else {
            return Err(PgError::protocol("SCRAM: unexpected client-final message"));
        };
        let msg = std::str::from_utf8(msg)
            .map_err(|_| PgError::protocol("SCRAM: client-final is not UTF-8"))?;

        let channel = attr(msg, 'c')?;
        if channel != B64.encode(gs2_header) {
            return Err(PgError::protocol("SCRAM: channel binding data mismatch"));
        }
        if attr(msg, 'r')? != full_nonce {
            return Err(PgError::protocol("SCRAM: nonce mismatch"));
        }
        let proof = B64
            .decode(attr(msg, 'p')?)
            .map_err(|_| PgError::protocol("SCRAM: proof is not valid base64"))?;
        let without_proof = msg
            .rsplit_once(",p=")
            .map(|(head, _)| head)
            .ok_or_else(|| PgError::protocol("SCRAM: missing proof"))?;

        let stored_key = self.verifier.stored_key;
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
        let client_signature: [u8; 32] = hmac(&stored_key, auth_message.as_bytes())
            .try_into()
            .expect("hmac-sha256 is 32 bytes");

        let proof: [u8; 32] = proof.try_into().map_err(|_| {
            PgError::fatal(sqlstate::INVALID_PASSWORD, "password authentication failed")
        })?;
        let recovered_key: [u8; 32] =
            std::array::from_fn(|index| proof[index] ^ client_signature[index]);
        let recovered_stored_key: [u8; 32] = Sha256::digest(recovered_key).into();
        let ok: bool = recovered_stored_key.ct_eq(&stored_key).into();
        if !ok {
            return Err(PgError::fatal(
                sqlstate::INVALID_PASSWORD,
                "password authentication failed",
            ));
        }

        let server_signature = hmac(&self.verifier.server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_signature)).into_bytes())
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Extracts the value of a comma-separated `x=value` attribute.
fn attr(msg: &str, name: char) -> Result<&str, PgError> {
    msg.split(',')
        .find_map(|part| part.strip_prefix(name).and_then(|p| p.strip_prefix('=')))
        .ok_or_else(|| PgError::protocol(format!("SCRAM: missing attribute '{name}'")))
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose::STANDARD as B64;

    use super::*;

    #[test]
    fn verifier_from_password_then_verify_roundtrip() {
        let salt = vec![1u8; 16];
        let v = ScramVerifier::from_password("pencil", salt.clone(), 4096);
        assert_eq!(v.salt, salt);
        assert_eq!(v.iterations, 4096);
        let mut server = ScramServer::from_verifier(v.clone(), "SNONCE".into());
        let server_first = server
            .handle_client_first(b"n,,n=user,r=CNONCE")
            .expect("client-first");
        let final_msg = client_final_for(&v, "pencil", "CNONCE", &server_first);
        let server_final = server
            .handle_client_final(final_msg.as_bytes())
            .expect("verify");
        assert!(server_final.starts_with(b"v="));
    }

    #[test]
    fn verifier_from_parts_then_verify_roundtrip() {
        let expected = ScramVerifier::from_password("pencil", vec![1u8; 16], 4096);
        let verifier = ScramVerifier::from_parts(
            expected.salt.clone(),
            expected.iterations,
            expected.stored_key,
            expected.server_key,
        )
        .expect("valid verifier parts");

        let mut server = ScramServer::from_verifier(verifier, "SNONCE".into());
        let server_first = server
            .handle_client_first(b"n,,n=user,r=CNONCE")
            .expect("client-first");
        let final_msg = client_final_for(&expected, "pencil", "CNONCE", &server_first);
        let server_final = server
            .handle_client_final(final_msg.as_bytes())
            .expect("verify");

        assert!(server_final.starts_with(b"v="));
    }

    #[test]
    fn verifier_from_parts_rejects_zero_iterations() {
        let err = ScramVerifier::from_parts(vec![1u8; 16], 0, [2u8; 32], [3u8; 32])
            .expect_err("zero iterations must be rejected");

        assert_eq!(
            err.code,
            crate::error::sqlstate::INVALID_AUTHORIZATION_SPECIFICATION
        );
    }

    #[test]
    fn verifier_from_parts_rejects_wrong_material_or_password() {
        let expected = ScramVerifier::from_password("pencil", vec![2u8; 16], 4096);
        let mut wrong_stored_key = expected.stored_key;
        wrong_stored_key[0] ^= 1;

        let cases = [
            (
                "wrong stored key",
                ScramVerifier::from_parts(
                    expected.salt.clone(),
                    expected.iterations,
                    wrong_stored_key,
                    expected.server_key,
                )
                .expect("valid verifier parts"),
                "pencil",
            ),
            (
                "wrong password",
                ScramVerifier::from_parts(
                    expected.salt.clone(),
                    expected.iterations,
                    expected.stored_key,
                    expected.server_key,
                )
                .expect("valid verifier parts"),
                "WRONG",
            ),
        ];

        for (name, verifier, password) in cases {
            let mut server = ScramServer::from_verifier(verifier, "SNONCE".into());
            let server_first = server
                .handle_client_first(b"n,,n=user,r=CNONCE")
                .expect("client-first");
            let final_msg = client_final_for(&expected, password, "CNONCE", &server_first);
            let err = server
                .handle_client_final(final_msg.as_bytes())
                .expect_err(name);

            assert_eq!(err.code, crate::error::sqlstate::INVALID_PASSWORD, "{name}");
        }
    }

    #[test]
    fn verifier_rejects_wrong_password() {
        let v = ScramVerifier::from_password("pencil", vec![2u8; 16], 4096);
        let mut server = ScramServer::from_verifier(v.clone(), "SNONCE".into());
        let server_first = server
            .handle_client_first(b"n,,n=user,r=CNONCE")
            .expect("cf");
        let final_msg = client_final_for(&v, "WRONG", "CNONCE", &server_first);
        let err = server
            .handle_client_final(final_msg.as_bytes())
            .expect_err("reject");
        assert_eq!(err.code, crate::error::sqlstate::INVALID_PASSWORD);
    }

    fn client_final_for(
        v: &ScramVerifier,
        password: &str,
        cnonce: &str,
        server_first: &[u8],
    ) -> String {
        use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::{Digest, Sha256};
        let sf = std::str::from_utf8(server_first).expect("utf8");
        let full_nonce = sf.split(',').find_map(|p| p.strip_prefix("r=")).expect("r");
        let client_first_bare = format!("n=user,r={cnonce}");
        let without_proof = format!("c=biws,r={full_nonce}");
        let auth_message = format!("{client_first_bare},{sf},{without_proof}");
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &v.salt, v.iterations, &mut salted);
        let mut m = Hmac::<Sha256>::new_from_slice(&salted).expect("hmac");
        m.update(b"Client Key");
        let client_key = m.finalize().into_bytes();
        let stored_key = Sha256::digest(client_key);
        let mut ms = Hmac::<Sha256>::new_from_slice(&stored_key).expect("hmac");
        ms.update(auth_message.as_bytes());
        let client_sig = ms.finalize().into_bytes();
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_sig.iter())
            .map(|(k, s)| k ^ s)
            .collect();
        format!("{without_proof},p={}", B64.encode(proof))
    }

    /// The exact exchange from RFC 7677 §3 (user "user", password "pencil").
    #[test]
    fn rfc_7677_test_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with(
            "pencil",
            salt,
            4096,
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into(),
        );

        let server_first = server
            .handle_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO")
            .expect("client-first ok");
        assert_eq!(
            server_first,
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".to_vec()
        );

        let server_final = server
            .handle_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .expect("proof verifies");
        assert_eq!(
            server_final,
            b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".to_vec()
        );
    }

    #[test]
    fn wrong_password_proof_is_rejected() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with(
            "not-pencil",
            salt,
            4096,
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into(),
        );
        server
            .handle_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO")
            .expect("client-first ok");
        let err = server
            .handle_client_final(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::INVALID_PASSWORD);
    }

    #[test]
    fn channel_binding_gs2_header_y_is_accepted() {
        // tokio-postgres over plaintext sends "y,," when the server doesn't
        // advertise -PLUS; c= must then be base64("y,,") = "eSws".
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let mut server = ScramServer::new_with("pw", salt, 4096, "SNONCE".into());
        let first = server
            .handle_client_first(b"y,,n=user,r=CNONCE")
            .expect("ok");
        assert!(first.starts_with(b"r=CNONCESNONCE,"));
    }

    #[test]
    fn requested_channel_binding_without_plus_is_rejected() {
        let mut server = ScramServer::new_with("pw", vec![0; 16], 4096, "S".into());
        let err = server
            .handle_client_first(b"p=tls-server-end-point,,n=user,r=CNONCE")
            .expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }
}
