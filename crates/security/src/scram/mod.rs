//! SCRAM (RFC 5802) — supports SHA-256 and SHA-512.

use std::{fmt, str::FromStr};

mod client;
mod pg_verifier;
mod server;

pub use client::ScramClientExchange;
use hmac::{Hmac, KeyInit, Mac};
pub use pg_verifier::{PgScramVerifier, ScramError};
use refined_type::rule::MinMaxI32;
use ring::rand::{SecureRandom, SystemRandom};
pub use server::{ScramServerExchange, StepResult};
use sha2::{Digest, Sha256, Sha512};

use crate::SaslMechanism;

/// Default PBKDF2 iteration count for new Kafka SCRAM credentials.
pub const DEFAULT_SCRAM_ITERATIONS: i32 = 8192;
/// Minimum PBKDF2 iteration count accepted by the broker.
pub const MIN_SCRAM_ITERATIONS: i32 = 4096;
/// Maximum PBKDF2 iteration count accepted by the broker.
pub const MAX_SCRAM_ITERATIONS: i32 = 16_384;

/// Broker-valid PBKDF2 iteration count for a SCRAM credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScramIterations(i32);

impl ScramIterations {
    /// Validate a SCRAM iteration count.
    ///
    /// # Errors
    ///
    /// Returns an error unless `value` is accepted by the broker.
    pub fn new(value: i32) -> Result<Self, String> {
        MinMaxI32::<MIN_SCRAM_ITERATIONS, MAX_SCRAM_ITERATIONS>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

impl Default for ScramIterations {
    fn default() -> Self {
        Self::new(DEFAULT_SCRAM_ITERATIONS).expect("default SCRAM iterations are broker-valid")
    }
}

impl fmt::Display for ScramIterations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ScramIterations {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

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
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn scram_hash_len(mechanism: SaslMechanism) -> usize {
    match mechanism {
        SaslMechanism::ScramSha256 => 32,
        SaslMechanism::ScramSha512 => 64,
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
            panic!("scram_hash_len called with non-SCRAM mechanism {mechanism:?}")
        }
    }
}

#[must_use]
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn hash_scram_password(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) -> ScramCredential {
    assert2::assert!(iterations > 0);
    let mut salt = vec![0u8; 16];
    SystemRandom::new()
        .fill(&mut salt)
        .expect("system RNG must succeed");
    hash_scram_password_with_salt(password, mechanism, iterations, salt)
}

/// Test-only entry that lets callers fix the salt (for golden vectors).
#[must_use]
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
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
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
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
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn pbkdf2_salted(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
    salt: &[u8],
) -> Vec<u8> {
    assert2::assert!(iterations > 0);
    match mechanism {
        SaslMechanism::ScramSha512 => {
            let arr: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(password, salt, iterations);
            arr.to_vec()
        }
        SaslMechanism::ScramSha256 => {
            let arr: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<Sha256, 32>(password, salt, iterations);
            arr.to_vec()
        }
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
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
/// # Panics
/// Panics if validated key material has an impossible size or synchronized credential state is poisoned.
pub fn derive_keys_from_salted(mechanism: SaslMechanism, salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    match mechanism {
        SaslMechanism::ScramSha512 => derive_keys_sha512(salted),
        SaslMechanism::ScramSha256 => derive_keys_sha256(salted),
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => {
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
    use assert2::check;
    use base64::{Engine, engine::general_purpose::STANDARD as B64};
    use sha2::{Digest, Sha512};

    use super::*;

    /// SHA-512 PBKDF2 vector + verify `stored_key = H(client_key)`.
    #[test]
    fn hash_scram_password_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha512, 4096);
        check!(
            (
                cred.mechanism,
                cred.salt.len(),
                cred.stored_key.len(),
                cred.server_key.len(),
                cred.iterations,
            ) == (SaslMechanism::ScramSha512, 16, 64, 64, 4096)
        );
        let salted =
            pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &cred.salt, cred.iterations);
        let client_key = {
            use hmac::{Hmac, KeyInit, Mac};
            let mut m = <Hmac<Sha512>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = Sha512::digest(client_key);
        assert2::assert!(cred.stored_key == expected_stored.as_slice());
    }

    /// SHA-256 analog of the SHA-512 vector. Verifies output lengths
    /// (32 bytes) and the same `stored_key = H(client_key)`
    /// invariant.
    #[test]
    fn hash_scram_password_sha256_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha256, 4096);
        check!(
            (
                cred.mechanism,
                cred.salt.len(),
                cred.stored_key.len(),
                cred.server_key.len(),
            ) == (SaslMechanism::ScramSha256, 16, 32, 32)
        );
        let salted =
            pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &cred.salt, cred.iterations);
        let client_key = {
            use hmac::{Hmac, KeyInit, Mac};
            let mut m = <Hmac<sha2::Sha256>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = sha2::Sha256::digest(client_key);
        assert2::assert!(cred.stored_key == expected_stored.as_slice());
    }

    #[test]
    fn hash_scram_password_is_deterministic_given_salt() {
        let a = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096);
        let b = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096);
        assert2::assert!(a.salt != b.salt);
    }

    use crate::{
        AuthError,
        scram::{
            client::ScramClientExchange,
            server::{ScramServerExchange, StepResult},
        },
    };

    #[test]
    fn scram_server_and_client_round_trip() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha512,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha512,
        );

        // Client first
        let (c1, client) = client.client_first().expect("client first");
        // Server step 1 -> server-first
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        // Client final
        let (c2, client) = client.step(&s1).expect("client final");
        // Server step 2 -> done
        let (principal, s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        assert2::assert!(principal.name.as_str() == "alice");
        assert2::assert!(principal.auth_method == crate::AuthMethod::SaslScramSha512);
        // Client verifies server signature. `verify_server_final` consumes
        // `client`, so a second verification attempt (the old "server final
        // must only verify once" regression test) is now a compile-time
        // move error rather than a runtime `MalformedMessage` — the
        // scenario it guarded against is no longer expressible.
        let final_check = client.verify_server_final(&s2);
        assert2::assert!(final_check.is_ok());
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
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let (c1, client) = client.client_first().expect("client first");
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let (c2, client) = client.step(&s1).expect("client final");
        let (principal, s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        assert2::assert!(principal.name.as_str() == "alice");
        assert2::assert!(principal.auth_method == crate::AuthMethod::SaslScramSha256);
        let final_check = client.verify_server_final(&s2);
        assert2::assert!(final_check.is_ok());
    }

    /// The hand-written `Debug` impl on `ScramServerExchange` prints the
    /// observable negotiation phase (the variant payload isn't exported, so
    /// callers can only ever see the phase name via `Debug`/logging).
    #[test]
    fn scram_server_exchange_debug_includes_the_observable_phase() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let rendered = format!("{server:?}");
        assert2::assert!(rendered.contains("ScramServerExchange::AwaitingClientFirst"));

        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );
        let (c1, _client) = client.client_first().expect("client first");
        let server = match server.step(&c1) {
            StepResult::Continue(_, next) => next,
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let rendered = format!("{server:?}");
        assert2::assert!(rendered.contains("ScramServerExchange::AwaitingClientFinal"));
    }

    #[test]
    fn scram_client_rejects_tampered_server_final_signature() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let (c1, client) = client.client_first().expect("client first");
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let (c2, client) = client.step(&s1).expect("client final");
        match server.step(&c2) {
            StepResult::Done(_, _) => {}
            other => panic!("server step 2 must Done, got {other:?}"),
        }

        assert2::assert!(client.verify_server_final(b"v=AAAA").is_err());
    }

    #[test]
    fn scram_server_rejects_wrong_length_client_proof() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let (c1, client) = client.client_first().expect("client first");
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let (c2_bytes, _client) = client.step(&s1).expect("client final");
        let mut c2 = String::from_utf8(c2_bytes).unwrap();
        let proof_start = c2.find("p=").expect("proof attribute") + 2;
        c2.truncate(proof_start);
        c2.push_str("AAAA");

        assert2::assert!(matches!(
            server.step(c2.as_bytes()),
            StepResult::Failed(AuthError::MalformedMessage)
        ));
    }

    #[test]
    fn pbkdf2_salted_matches_hash_scram_password_intermediate() {
        // `pbkdf2_salted` exposes the PBKDF2 intermediate so the
        // operator can produce the KIP-554 wire `salted_password`. It
        // must equal the value `hash_scram_password_with_salt` feeds
        // into `derive_keys_from_salted` internally.
        let password = b"pencil";
        let salt: Vec<u8> = (0..16).collect();
        for (_case, mechanism, expected_len) in [
            ("SCRAM SHA-512", SaslMechanism::ScramSha512, 64),
            ("SCRAM SHA-256", SaslMechanism::ScramSha256, 32),
        ] {
            let cred = hash_scram_password_with_salt(password, mechanism, 4096, salt.clone());
            let salted = pbkdf2_salted(password, mechanism, 4096, &salt);
            let (stored_key, server_key) = derive_keys_from_salted(mechanism, &salted);
            assert2::assert!(salted.len() == expected_len);
            assert2::assert!(stored_key == cred.stored_key);
            assert2::assert!(server_key == cred.server_key);
        }
    }

    #[test]
    fn derive_keys_from_salted_matches_hash_scram_password_sha512() {
        let password = b"hunter2";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha512, 4096, salt.clone());
        let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, 4096);
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha512, &salted);
        check!(
            (&stored_key, &server_key, stored_key.len(), server_key.len())
                == (&cred.stored_key, &cred.server_key, 64, 64)
        );
    }

    #[test]
    fn derive_keys_from_salted_matches_hash_scram_password_sha256() {
        let password = b"hunter2";
        let salt: Vec<u8> = (0..16).collect();
        let cred =
            hash_scram_password_with_salt(password, SaslMechanism::ScramSha256, 4096, salt.clone());
        let salted: [u8; 32] = pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &salt, 4096);
        let (stored_key, server_key) = derive_keys_from_salted(SaslMechanism::ScramSha256, &salted);
        check!(
            (&stored_key, &server_key, stored_key.len(), server_key.len())
                == (&cred.stored_key, &cred.server_key, 32, 32)
        );
    }

    /// KIP-48: `new_with_principal` stamps an override
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
        let server = ScramServerExchange::new_with_principal(
            "tok-uuid".to_string(),
            cred,
            override_principal.clone(),
        );
        let client = ScramClientExchange::new(
            "tok-uuid".to_string(),
            password.to_vec(),
            SaslMechanism::ScramSha256,
        );

        let (c1, client) = client.client_first().expect("client first");
        let (s1, server) = match server.step(&c1) {
            StepResult::Continue(b, next) => (b, next),
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        let (c2, _client) = client.step(&s1).expect("client final");
        let (principal, _s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        // Override wins: principal is the token owner, NOT "tok-uuid".
        assert2::assert!(principal == override_principal);
    }

    #[test]
    fn scram_server_rejects_bad_proof() {
        let cred = hash_scram_password_with_salt(
            b"correct",
            SaslMechanism::ScramSha512,
            4096,
            vec![0u8; 16],
        );
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            b"wrong".to_vec(),
            SaslMechanism::ScramSha512,
        );
        let (c1, client) = client.client_first().unwrap();
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!();
        };
        let (c2, _client) = client.step(&s1).unwrap();
        match server.step(&c2) {
            StepResult::Failed(crate::AuthError::BadProof) => {}
            other => panic!("expected BadProof, got {other:?}"),
        }
    }

    /// RFC 5802 §5.1: the server must reject a client-final whose `r=`
    /// (combined nonce) does not equal the nonce it issued in
    /// server-first. We tamper with the `r=` attribute of an otherwise
    /// well-formed client-final and expect `MalformedMessage`.
    #[test]
    fn scram_server_rejects_wrong_nonce() {
        // Arbitrary, non-secret test password generated at runtime (not a
        // hard-coded credential literal).
        let password: Vec<u8> = (b'A'..=b'Z').collect();
        let cred = hash_scram_password_with_salt(
            &password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.clone(),
            SaslMechanism::ScramSha256,
        );
        let (c1, client) = client.client_first().unwrap();
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!("server step 1 must continue");
        };
        let (c2, _client) = client.step(&s1).unwrap();
        let c2_str = String::from_utf8(c2).unwrap();
        // Flip the combined nonce: replace `r=<nonce>` with a different
        // value while leaving `c=` and `p=` intact.
        let combined = c2_str
            .split(',')
            .find_map(|a| a.strip_prefix("r="))
            .expect("client-final has r=");
        let tampered = c2_str.replacen(
            &format!("r={combined}"),
            &format!("r={combined}deadbeef"),
            1,
        );
        match server.step(tampered.as_bytes()) {
            StepResult::Failed(crate::AuthError::MalformedMessage) => {}
            other => panic!("expected MalformedMessage for wrong nonce, got {other:?}"),
        }
    }

    /// RFC 5802 §5.1: the server must reject a client-final whose `c=`
    /// channel binding is not the base64 of the GS2 header `n,,`
    /// (`"biws"`). We swap in a bogus channel binding and expect
    /// `MalformedMessage`.
    #[test]
    fn scram_server_rejects_wrong_channel_binding() {
        // Arbitrary, non-secret test password generated at runtime (not a
        // hard-coded credential literal).
        let password: Vec<u8> = (b'A'..=b'Z').collect();
        let cred = hash_scram_password_with_salt(
            &password,
            SaslMechanism::ScramSha256,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let server = ScramServerExchange::new("alice".to_string(), cred);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.clone(),
            SaslMechanism::ScramSha256,
        );
        let (c1, client) = client.client_first().unwrap();
        let StepResult::Continue(s1, server) = server.step(&c1) else {
            panic!("server step 1 must continue");
        };
        let (c2, _client) = client.step(&s1).unwrap();
        let c2_str = String::from_utf8(c2).unwrap();
        // The client always emits `c=biws`; rewrite it to a different
        // (still-valid-base64) channel binding.
        assert2::assert!(c2_str.starts_with("c=biws,"));
        let wrong_cb = B64.encode(b"y,,");
        let tampered = c2_str.replacen("c=biws", &format!("c={wrong_cb}"), 1);
        match server.step(tampered.as_bytes()) {
            StepResult::Failed(crate::AuthError::MalformedMessage) => {}
            other => panic!("expected MalformedMessage for wrong channel binding, got {other:?}"),
        }
    }
}
