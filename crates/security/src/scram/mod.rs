//! SCRAM (RFC 5802) — supports SHA-256 and SHA-512.

mod client;
mod server;

pub use client::ScramClientExchange;
pub use server::{ScramServerExchange, StepResult};

use crate::SaslMechanism;
use hmac::{Hmac, KeyInit, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha256, Sha512};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

/// Output byte length of the underlying hash function for a given
/// SCRAM mechanism: 32 for SHA-256, 64 for SHA-512. Panics on a
/// non-SCRAM mechanism.
#[must_use]
pub fn scram_hash_len(mechanism: SaslMechanism) -> usize {
    match mechanism {
        SaslMechanism::ScramSha256 => 32,
        SaslMechanism::ScramSha512 => 64,
        SaslMechanism::Plain | SaslMechanism::OAuthBearer => {
            panic!("scram_hash_len called with non-SCRAM mechanism {mechanism:?}")
        }
    }
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
    let (stored_key, server_key) = match mechanism {
        SaslMechanism::ScramSha512 => {
            let salted: [u8; 64] =
                pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, &salt, iterations);
            derive_keys_sha512(&salted)
        }
        SaslMechanism::ScramSha256 => {
            let salted: [u8; 32] =
                pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(password, &salt, iterations);
            derive_keys_sha256(&salted)
        }
        SaslMechanism::Plain | SaslMechanism::OAuthBearer => {
            panic!("hash_scram_password called with non-SCRAM mechanism {mechanism:?}");
        }
    };
    ScramCredential {
        mechanism,
        salt,
        stored_key,
        server_key,
        iterations,
    }
}

/// Compute the PBKDF2 output ("salted password" in KIP-554 / RFC 5802
/// language) for a given SCRAM mechanism. Output length matches
/// [`scram_hash_len`]: 32 bytes for SHA-256, 64 bytes for SHA-512.
///
/// Used by `crabka-client-admin` to populate
/// `AlterUserScramCredentialsRequest.upsertions[].salted_password`
/// without leaking the broker-side `derive_keys_from_salted` machinery
/// or the raw password into the operator. Panics on a non-SCRAM
/// mechanism.
#[must_use]
pub fn pbkdf2_salted(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
    salt: &[u8],
) -> Vec<u8> {
    assert!(iterations > 0, "iterations must be > 0");
    match mechanism {
        SaslMechanism::ScramSha512 => {
            let arr: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, salt, iterations);
            arr.to_vec()
        }
        SaslMechanism::ScramSha256 => {
            let arr: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(password, salt, iterations);
            arr.to_vec()
        }
        SaslMechanism::Plain | SaslMechanism::OAuthBearer => {
            panic!("pbkdf2_salted called with non-SCRAM mechanism {mechanism:?}");
        }
    }
}

/// Reconstruct `(stored_key, server_key)` from the salted-password
/// output the client sends in an `AlterUserScramCredentialsRequest`
/// per KIP-554.
///
/// The KIP places PBKDF2 on the client side: the wire request carries
/// the already-stretched PBKDF2 output (32 bytes for SHA-256, 64 bytes
/// for SHA-512), and the broker derives the two stored keys from it.
/// This avoids the broker holding the raw password even briefly. The
/// transformation, for each hash H:
///
/// ```text
/// client_key  = HMAC-H(salted_password, "Client Key")
/// stored_key  = H(client_key)
/// server_key  = HMAC-H(salted_password, "Server Key")
/// ```
///
/// The mechanism argument selects which `H` to use. Panics on a
/// non-SCRAM mechanism.
#[must_use]
pub fn derive_keys_from_salted(mechanism: SaslMechanism, salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    match mechanism {
        SaslMechanism::ScramSha512 => derive_keys_sha512(salted),
        SaslMechanism::ScramSha256 => derive_keys_sha256(salted),
        SaslMechanism::Plain | SaslMechanism::OAuthBearer => {
            panic!("derive_keys_from_salted called with non-SCRAM mechanism {mechanism:?}");
        }
    }
}

fn derive_keys_sha512(salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut ck_mac = <Hmac<Sha512>>::new_from_slice(salted).expect("hmac accepts any key length");
    ck_mac.update(b"Client Key");
    let client_key = ck_mac.finalize().into_bytes();
    let stored_key = Sha512::digest(client_key).to_vec();
    let mut sk_mac = <Hmac<Sha512>>::new_from_slice(salted).expect("hmac accepts any key length");
    sk_mac.update(b"Server Key");
    let server_key = sk_mac.finalize().into_bytes().to_vec();
    (stored_key, server_key)
}

fn derive_keys_sha256(salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut ck_mac = <Hmac<Sha256>>::new_from_slice(salted).expect("hmac accepts any key length");
    ck_mac.update(b"Client Key");
    let client_key = ck_mac.finalize().into_bytes();
    let stored_key = Sha256::digest(client_key).to_vec();
    let mut sk_mac = <Hmac<Sha256>>::new_from_slice(salted).expect("hmac accepts any key length");
    sk_mac.update(b"Server Key");
    let server_key = sk_mac.finalize().into_bytes().to_vec();
    (stored_key, server_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha512};

    /// SHA-512 PBKDF2 vector + verify `stored_key = H(client_key)`.
    #[test]
    fn hash_scram_password_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha512, 4096);
        assert_eq!(cred.mechanism, SaslMechanism::ScramSha512);
        assert_eq!(cred.salt.len(), 16, "salt must be 16 bytes");
        assert_eq!(cred.stored_key.len(), 64, "SHA-512 output is 64 bytes");
        assert_eq!(cred.server_key.len(), 64);
        assert_eq!(cred.iterations, 4096);
        let salted =
            pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &cred.salt, cred.iterations);
        let client_key = {
            use hmac::{Hmac, KeyInit, Mac};
            let mut m = <Hmac<Sha512>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = Sha512::digest(client_key);
        assert_eq!(cred.stored_key, expected_stored.as_slice());
    }

    /// SHA-256 analog of the SHA-512 vector. Verifies output lengths
    /// (32 bytes) and the same `stored_key = H(client_key)`
    /// invariant.
    #[test]
    fn hash_scram_password_sha256_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha256, 4096);
        assert_eq!(cred.mechanism, SaslMechanism::ScramSha256);
        assert_eq!(cred.salt.len(), 16);
        assert_eq!(cred.stored_key.len(), 32, "SHA-256 output is 32 bytes");
        assert_eq!(cred.server_key.len(), 32);
        let salted =
            pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &cred.salt, cred.iterations);
        let client_key = {
            use hmac::{Hmac, KeyInit, Mac};
            let mut m = <Hmac<sha2::Sha256>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = sha2::Sha256::digest(client_key);
        assert_eq!(cred.stored_key, expected_stored.as_slice());
    }

    #[test]
    fn hash_scram_password_is_deterministic_given_salt() {
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
        let mut client = ScramClientExchange::new(
            "alice".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha512,
        );

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
        assert_eq!(principal.auth_method, crate::AuthMethod::SaslScramSha512);
        // Client verifies server signature
        let final_check = client.verify_server_final(&s2);
        assert!(final_check.is_ok(), "server signature must verify");
    }

    /// Mirror of the SHA-512 round-trip with SHA-256 — proves the
    /// generalized state machines produce a matching client/server
    /// pair on the smaller-hash path.
    #[test]
    fn scram_server_and_client_round_trip_sha256() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let mut server = ScramServerExchange::new("alice".to_string(), cred);
        let mut client = ScramClientExchange::new(
            "alice".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let c1 = client.client_first().expect("client first");
        let s1 = match server.step(&c1) {
            StepResult::Continue(b) => b,
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let c2 = client.step(&s1).expect("client final");
        let (principal, s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        assert_eq!(principal.name, "alice");
        assert_eq!(principal.auth_method, crate::AuthMethod::SaslScramSha256);
        let final_check = client.verify_server_final(&s2);
        assert!(final_check.is_ok(), "server signature must verify");
    }

    #[test]
    fn pbkdf2_salted_matches_hash_scram_password_intermediate_sha512() {
        // `pbkdf2_salted` exposes the PBKDF2 intermediate so the
        // operator can produce the KIP-554 wire `salted_password`. It
        // must equal the value `hash_scram_password_with_salt` feeds
        // into `derive_keys_from_salted` internally.
        let password = b"pencil";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha512, 4096, salt.clone());
        let salted = pbkdf2_salted(password, SaslMechanism::ScramSha512, 4096, &salt);
        assert_eq!(salted.len(), 64);
        // Re-derive keys from the helper output → must match the
        // credential the slow path computed.
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha512, &salted);
        assert_eq!(stored_key, cred.stored_key);
        assert_eq!(server_key, cred.server_key);
    }

    #[test]
    fn pbkdf2_salted_matches_hash_scram_password_intermediate_sha256() {
        let password = b"pencil";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha256, 4096, salt.clone());
        let salted = pbkdf2_salted(password, SaslMechanism::ScramSha256, 4096, &salt);
        assert_eq!(salted.len(), 32);
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha256, &salted);
        assert_eq!(stored_key, cred.stored_key);
        assert_eq!(server_key, cred.server_key);
    }

    #[test]
    fn derive_keys_from_salted_matches_hash_scram_password_sha512() {
        let password = b"hunter2";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha512, 4096, salt.clone());
        let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, 4096);
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha512, &salted);
        assert_eq!(stored_key, cred.stored_key);
        assert_eq!(server_key, cred.server_key);
        assert_eq!(stored_key.len(), 64);
        assert_eq!(server_key.len(), 64);
    }

    #[test]
    fn derive_keys_from_salted_matches_hash_scram_password_sha256() {
        let password = b"hunter2";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha256, 4096, salt.clone());
        let salted: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &salt, 4096);
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha256, &salted);
        assert_eq!(stored_key, cred.stored_key);
        assert_eq!(server_key, cred.server_key);
        assert_eq!(stored_key.len(), 32);
        assert_eq!(server_key.len(), 32);
    }

    /// Slice 51 (KIP-48): `new_with_principal` stamps an override
    /// principal that wins on the `Done` arm — used by the
    /// delegation-token SCRAM fallback so a client authenticating
    /// with a `tokenId` as the SCRAM username surfaces as the token's
    /// owner (e.g. `User:alice`), not as `User:<token-uuid>`.
    #[test]
    fn scram_server_with_principal_override_yields_override_on_done() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let override_principal = crate::Principal {
            name: "alice".to_string(),
            auth_method: crate::AuthMethod::SaslScramSha256,
            groups: vec![],
        };
        // SCRAM username (the wire "n=..." attribute) is "tok-uuid";
        // the override principal is "alice" (the token's owner).
        let mut server = ScramServerExchange::new_with_principal(
            "tok-uuid".to_string(),
            cred,
            override_principal.clone(),
        );
        let mut client = ScramClientExchange::new(
            "tok-uuid".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let c1 = client.client_first().expect("client first");
        let s1 = match server.step(&c1) {
            StepResult::Continue(b) => b,
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let c2 = client.step(&s1).expect("client final");
        let (principal, _s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        // Override wins: principal is the token owner, NOT "tok-uuid".
        assert_eq!(principal, override_principal);
        assert_eq!(principal.name, "alice");
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
        let mut client = ScramClientExchange::new(
            "alice".to_string(),
            b"wrong".to_vec(),
            SaslMechanism::ScramSha512,
        );
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
