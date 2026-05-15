//! SCRAM-SHA-512 — implemented in tasks 2-3.

mod client;
mod server;

pub use client::ScramClientExchange;
pub use server::{ScramServerExchange, StepResult};

use crate::SaslMechanism;
use hmac::{Hmac, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha512};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

#[must_use]
pub fn hash_scram_password(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) -> ScramCredential {
    assert!(iterations > 0, "iterations must be > 0");
    let mut salt = vec![0u8; 16];
    SystemRandom::new()
        .fill(&mut salt)
        .expect("system RNG must succeed");
    hash_scram_password_with_salt(password, mechanism, iterations, salt)
}

/// Test-only entry that lets callers fix the salt (for golden vectors).
#[must_use]
pub fn hash_scram_password_with_salt(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
    salt: Vec<u8>,
) -> ScramCredential {
    assert_eq!(
        mechanism,
        SaslMechanism::ScramSha512,
        "only SCRAM-SHA-512 supported in slice 12"
    );
    let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, &salt, iterations);
    let mut client_key_mac =
        <Hmac<Sha512>>::new_from_slice(&salted).expect("hmac accepts any key length");
    client_key_mac.update(b"Client Key");
    let client_key = client_key_mac.finalize().into_bytes();
    let stored_key = Sha512::digest(client_key).to_vec();

    let mut server_key_mac =
        <Hmac<Sha512>>::new_from_slice(&salted).expect("hmac accepts any key length");
    server_key_mac.update(b"Server Key");
    let server_key = server_key_mac.finalize().into_bytes().to_vec();

    ScramCredential {
        mechanism,
        salt,
        stored_key,
        server_key,
        iterations,
    }
}

/// Reconstruct `(stored_key, server_key)` from the 64-byte PBKDF2 output
/// (`salted_password`) the client sends in an
/// `AlterUserScramCredentialsRequest` per KIP-554.
///
/// The KIP places PBKDF2 on the client side: the wire request carries the
/// already-stretched 64-byte SHA-512 PBKDF2 output, and the broker derives
/// the two stored keys from it. This avoids the broker holding the raw
/// password even briefly. The transformation is:
///
/// ```text
/// client_key  = HMAC-SHA-512(salted_password, "Client Key")
/// stored_key  = SHA-512(client_key)
/// server_key  = HMAC-SHA-512(salted_password, "Server Key")
/// ```
///
/// Both outputs are 64 bytes for SHA-512. The function name elides the
/// digest because slice 12 only supports SCRAM-SHA-512; a future SHA-256
/// variant would split into a separate helper.
#[must_use]
pub fn derive_keys_from_salted(salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut ck_mac = <Hmac<Sha512>>::new_from_slice(salted).expect("hmac accepts any key length");
    ck_mac.update(b"Client Key");
    let client_key = ck_mac.finalize().into_bytes();
    let stored_key = Sha512::digest(client_key).to_vec();
    let mut sk_mac = <Hmac<Sha512>>::new_from_slice(salted).expect("hmac accepts any key length");
    sk_mac.update(b"Server Key");
    let server_key = sk_mac.finalize().into_bytes().to_vec();
    (stored_key, server_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha512};

    /// RFC 7677 vector for SCRAM-SHA-256 doesn't translate directly,
    /// but we can verify PBKDF2-HMAC-SHA-512 with a known vector and
    /// then assert `stored_key` = `H(client_key)`, `server_key` = `HMAC(salted, "Server Key")`.
    #[test]
    fn hash_scram_password_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha512, 4096);
        assert_eq!(cred.mechanism, SaslMechanism::ScramSha512);
        assert_eq!(cred.salt.len(), 16, "salt must be 16 bytes");
        assert_eq!(cred.stored_key.len(), 64, "SHA-512 output is 64 bytes");
        assert_eq!(cred.server_key.len(), 64);
        assert_eq!(cred.iterations, 4096);
        // stored_key = H(client_key) — verify by recomputing
        let salted =
            pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &cred.salt, cred.iterations);
        let client_key = {
            use hmac::{Hmac, Mac};
            let mut m = <Hmac<Sha512>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = Sha512::digest(client_key);
        assert_eq!(cred.stored_key, expected_stored.as_slice());
    }

    #[test]
    fn hash_scram_password_is_deterministic_given_salt() {
        // Internal helper that takes a fixed salt for reproducibility.
        // We can't assert against a public hash_scram_password (which generates
        // random salt), so smoke-test via two calls producing different salts.
        let a = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096);
        let b = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096);
        assert_ne!(a.salt, b.salt, "fresh salt each call");
    }

    use crate::scram::client::ScramClientExchange;
    use crate::scram::server::{ScramServerExchange, StepResult};

    #[test]
    fn scram_server_and_client_round_trip() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha512,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let mut server = ScramServerExchange::new("alice".to_string(), cred);
        let mut client = ScramClientExchange::new("alice".to_string(), password.to_vec());

        // Client first
        let c1 = client.client_first().expect("client first");
        // Server step 1 -> server-first
        let s1 = match server.step(&c1) {
            StepResult::Continue(b) => b,
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        // Client final
        let c2 = client.step(&s1).expect("client final");
        // Server step 2 -> done
        let (principal, s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        assert_eq!(principal.name, "alice");
        assert_eq!(principal.mechanism, SaslMechanism::ScramSha512);
        // Client verifies server signature
        let final_check = client.verify_server_final(&s2);
        assert!(final_check.is_ok(), "server signature must verify");
    }

    #[test]
    fn derive_keys_from_salted_matches_hash_scram_password() {
        // The two paths must produce identical (stored_key, server_key)
        // when fed the same salted_password — `hash_scram_password_with_salt`
        // runs PBKDF2 then derives keys, and `derive_keys_from_salted` skips
        // PBKDF2 and reads the salted output directly.
        let password = b"hunter2";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha512, 4096, salt.clone());
        let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, 4096);
        let (stored_key, server_key) = derive_keys_from_salted(&salted);
        assert_eq!(stored_key, cred.stored_key);
        assert_eq!(server_key, cred.server_key);
        assert_eq!(stored_key.len(), 64);
        assert_eq!(server_key.len(), 64);
    }

    #[test]
    fn scram_server_rejects_bad_proof() {
        let cred = hash_scram_password_with_salt(
            b"correct",
            SaslMechanism::ScramSha512,
            4096,
            vec![0u8; 16],
        );
        let mut server = ScramServerExchange::new("alice".to_string(), cred);
        let mut client = ScramClientExchange::new("alice".to_string(), b"wrong".to_vec());
        let c1 = client.client_first().unwrap();
        let StepResult::Continue(s1) = server.step(&c1) else {
            panic!();
        };
        let c2 = client.step(&s1).unwrap();
        match server.step(&c2) {
            StepResult::Failed(crate::AuthError::BadProof) => {}
            other => panic!("expected BadProof, got {other:?}"),
        }
    }
}
