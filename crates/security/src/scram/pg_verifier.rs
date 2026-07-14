//! `PostgreSQL` `pg_authid` SCRAM-SHA-256 verifier codec.

use std::{fmt, str::FromStr};

use base64::{Engine, engine::general_purpose::STANDARD as B64};
use thiserror::Error;

use super::{ScramCredential, hash_scram_password, hash_scram_password_with_salt};
use crate::SaslMechanism;

const POSTGRES_SCRAM_SHA256_PREFIX: &str = "SCRAM-SHA-256";
const SHA256_KEY_LEN: usize = 32;

/// A `PostgreSQL` `pg_authid.rolpassword` SCRAM-SHA-256 verifier.
///
/// The textual form is
/// `SCRAM-SHA-256$<iterations>:<salt_b64>$<stored_key_b64>:<server_key_b64>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgScramVerifier {
    /// PBKDF2 iteration count.
    pub iterations: u32,
    /// SCRAM salt bytes.
    pub salt: Vec<u8>,
    /// SHA-256 stored key bytes.
    pub stored_key: Vec<u8>,
    /// SHA-256 server key bytes.
    pub server_key: Vec<u8>,
}

/// Errors returned by the `PostgreSQL` SCRAM verifier codec.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ScramError {
    /// The verifier did not match `PostgreSQL`'s SCRAM-SHA-256 shape.
    #[error("malformed PostgreSQL SCRAM verifier")]
    MalformedVerifier,
    /// The verifier names anything other than SCRAM-SHA-256.
    #[error("unsupported SCRAM verifier mechanism")]
    UnsupportedMechanism,
    /// The verifier has a zero iteration count.
    #[error("SCRAM iteration count must be greater than zero")]
    InvalidIterations,
    /// The verifier's salt decoded to an empty byte string.
    #[error("SCRAM salt must not be empty")]
    EmptySalt,
    /// One of the verifier's base64 fields is not valid standard base64.
    #[error("invalid base64 in SCRAM verifier")]
    InvalidBase64,
    /// One of the verifier's SHA-256 keys decoded to the wrong length.
    #[error("invalid {field} length: expected {expected} bytes, got {actual}")]
    InvalidKeyLength {
        /// Field with the wrong length.
        field: &'static str,
        /// Required byte length.
        expected: usize,
        /// Actual decoded byte length.
        actual: usize,
    },
}

impl PgScramVerifier {
    /// Generate a fresh `PostgreSQL` SCRAM-SHA-256 verifier for `password`.
    ///
    /// # Errors
    ///
    /// Returns [`ScramError::InvalidIterations`] when `iterations` is zero.
    pub fn generate(password: &str, iterations: u32) -> Result<Self, ScramError> {
        if iterations == 0 {
            return Err(ScramError::InvalidIterations);
        }

        let credential =
            hash_scram_password(password.as_bytes(), SaslMechanism::ScramSha256, iterations);
        Ok(Self::from_sha256_credential(credential))
    }

    /// Generate a deterministic `PostgreSQL` SCRAM-SHA-256 verifier with `salt`.
    ///
    /// # Errors
    ///
    /// Returns an error when `iterations` is zero or `salt` is empty.
    pub fn generate_with_salt(
        password: &str,
        iterations: u32,
        salt: Vec<u8>,
    ) -> Result<Self, ScramError> {
        if iterations == 0 {
            return Err(ScramError::InvalidIterations);
        }
        if salt.is_empty() {
            return Err(ScramError::EmptySalt);
        }

        let credential = hash_scram_password_with_salt(
            password.as_bytes(),
            SaslMechanism::ScramSha256,
            iterations,
            salt,
        );
        Ok(Self::from_sha256_credential(credential))
    }

    /// Parse a `PostgreSQL` `pg_authid.rolpassword` SCRAM-SHA-256 verifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the verifier is malformed, uses an unsupported
    /// mechanism, or contains invalid iteration, salt, or key fields.
    pub fn parse(verifier: &str) -> Result<Self, ScramError> {
        verifier.parse()
    }

    fn parse_fields(verifier: &str) -> Result<Self, ScramError> {
        let (prefix, rest) = verifier
            .split_once('$')
            .ok_or(ScramError::MalformedVerifier)?;
        if prefix != POSTGRES_SCRAM_SHA256_PREFIX {
            return Err(ScramError::UnsupportedMechanism);
        }

        let (iterations_and_salt, keys) =
            rest.split_once('$').ok_or(ScramError::MalformedVerifier)?;
        if keys.contains('$') {
            return Err(ScramError::MalformedVerifier);
        }

        let (iterations, salt) = parse_iterations_and_salt(iterations_and_salt)?;
        let (stored_key, server_key) = parse_key_pair(keys)?;

        Ok(Self {
            iterations,
            salt,
            stored_key,
            server_key,
        })
    }

    fn from_sha256_credential(credential: ScramCredential) -> Self {
        debug_assert_eq!(credential.mechanism, SaslMechanism::ScramSha256);
        Self {
            iterations: credential.iterations,
            salt: credential.salt,
            stored_key: credential.stored_key,
            server_key: credential.server_key,
        }
    }
}

impl fmt::Display for PgScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{POSTGRES_SCRAM_SHA256_PREFIX}${}:{}${}:{}",
            self.iterations,
            B64.encode(&self.salt),
            B64.encode(&self.stored_key),
            B64.encode(&self.server_key)
        )
    }
}

impl FromStr for PgScramVerifier {
    type Err = ScramError;

    fn from_str(verifier: &str) -> Result<Self, Self::Err> {
        Self::parse_fields(verifier)
    }
}

impl From<PgScramVerifier> for ScramCredential {
    fn from(verifier: PgScramVerifier) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha256,
            salt: verifier.salt,
            stored_key: verifier.stored_key,
            server_key: verifier.server_key,
            iterations: verifier.iterations,
        }
    }
}

impl From<&PgScramVerifier> for ScramCredential {
    fn from(verifier: &PgScramVerifier) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha256,
            salt: verifier.salt.clone(),
            stored_key: verifier.stored_key.clone(),
            server_key: verifier.server_key.clone(),
            iterations: verifier.iterations,
        }
    }
}

fn parse_iterations_and_salt(input: &str) -> Result<(u32, Vec<u8>), ScramError> {
    let (iterations, salt_b64) = input.split_once(':').ok_or(ScramError::MalformedVerifier)?;
    if salt_b64.contains(':') {
        return Err(ScramError::MalformedVerifier);
    }

    let iterations = iterations
        .parse::<u32>()
        .map_err(|_| ScramError::MalformedVerifier)?;
    if iterations == 0 {
        return Err(ScramError::InvalidIterations);
    }

    let salt = B64
        .decode(salt_b64)
        .map_err(|_| ScramError::InvalidBase64)?;
    if salt.is_empty() {
        return Err(ScramError::EmptySalt);
    }

    Ok((iterations, salt))
}

fn parse_key_pair(input: &str) -> Result<(Vec<u8>, Vec<u8>), ScramError> {
    let (stored_key_b64, server_key_b64) =
        input.split_once(':').ok_or(ScramError::MalformedVerifier)?;
    if server_key_b64.contains(':') {
        return Err(ScramError::MalformedVerifier);
    }

    let stored_key = B64
        .decode(stored_key_b64)
        .map_err(|_| ScramError::InvalidBase64)?;
    let server_key = B64
        .decode(server_key_b64)
        .map_err(|_| ScramError::InvalidBase64)?;
    require_key_len("stored_key", &stored_key)?;
    require_key_len("server_key", &server_key)?;

    Ok((stored_key, server_key))
}

fn require_key_len(field: &'static str, key: &[u8]) -> Result<(), ScramError> {
    if key.len() == SHA256_KEY_LEN {
        return Ok(());
    }

    Err(ScramError::InvalidKeyLength {
        field,
        expected: SHA256_KEY_LEN,
        actual: key.len(),
    })
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::scram::{ScramClientExchange, ScramServerExchange, StepResult};

    const FIXED_SALT: &[u8] = b"0123456789abcdef";

    fn fixture_password() -> String {
        std::process::id().to_string()
    }

    fn pg_scram_golden_password() -> String {
        std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/pg_scram_password.txt"
        ))
        .expect("PostgreSQL SCRAM password fixture")
        .trim_end()
        .to_owned()
    }

    #[test]
    fn parse_display_roundtrip_emits_canonical_verifier() {
        let verifier =
            PgScramVerifier::generate_with_salt(&fixture_password(), 4096, FIXED_SALT.to_vec())
                .expect("fixed salt verifier");

        let rendered = verifier.to_string();
        let parsed = PgScramVerifier::parse(&rendered).expect("rendered verifier parses");

        assert!(parsed == verifier);
        assert!(parsed.to_string() == rendered);
    }

    #[test]
    fn deterministic_generation_with_fixed_salt_matches_known_sha256_material() {
        let verifier = PgScramVerifier::generate_with_salt(
            &pg_scram_golden_password(),
            4096,
            (0_u8..16).collect::<Vec<_>>(),
        )
        .expect("fixed salt verifier");

        check!(verifier.iterations == 4096);
        check!(verifier.salt == (0_u8..16).collect::<Vec<_>>());
        check!(B64.encode(&verifier.stored_key) == "ijmdkBF6VaUuRpEzxe8P8NyZKzuHGkoluJBVZ5DF5+Q=");
        check!(B64.encode(&verifier.server_key) == "naMMD2q/yKfDcTgfNhpZqsbkzoRQwOPfT/TgHEPWP7w=");
        assert!(
            verifier.to_string()
                == "SCRAM-SHA-256$4096:AAECAwQFBgcICQoLDA0ODw==$ijmdkBF6VaUuRpEzxe8P8NyZKzuHGkoluJBVZ5DF5+Q=:naMMD2q/yKfDcTgfNhpZqsbkzoRQwOPfT/TgHEPWP7w="
        );
    }

    #[test]
    fn rejects_malformed_verifier_table() {
        let wrong_stored_key = B64.encode([0_u8; SHA256_KEY_LEN - 1]);
        let wrong_server_key = B64.encode([0_u8; SHA256_KEY_LEN + 1]);
        let valid_server_key = B64.encode([1_u8; SHA256_KEY_LEN]);

        for (input, expected) in [
            ("", ScramError::MalformedVerifier),
            (
                "SCRAM-SHA-512$4096:c2FsdA==$a:b",
                ScramError::UnsupportedMechanism,
            ),
            ("md5deadbeef", ScramError::MalformedVerifier),
            ("SCRAM-SHA-256", ScramError::MalformedVerifier),
            ("SCRAM-SHA-256$4096:c2FsdA==", ScramError::MalformedVerifier),
            (
                "SCRAM-SHA-256$4096:c2FsdA==$AAAA:AAAA$extra",
                ScramError::MalformedVerifier,
            ),
            (
                "SCRAM-SHA-256$4096:c2FsdA==:extra$AAAA:AAAA",
                ScramError::MalformedVerifier,
            ),
            (
                "SCRAM-SHA-256$4096:c2FsdA==$AAAA:AAAA:extra",
                ScramError::MalformedVerifier,
            ),
            (
                "SCRAM-SHA-256$not-a-number:c2FsdA==$AAAA:AAAA",
                ScramError::MalformedVerifier,
            ),
            (
                "SCRAM-SHA-256$0:c2FsdA==$AAAA:AAAA",
                ScramError::InvalidIterations,
            ),
            ("SCRAM-SHA-256$4096:*$AAAA:AAAA", ScramError::InvalidBase64),
            ("SCRAM-SHA-256$4096:$AAAA:AAAA", ScramError::EmptySalt),
            (
                "SCRAM-SHA-256$4096:c2FsdA==$*:AAAA",
                ScramError::InvalidBase64,
            ),
            (
                "SCRAM-SHA-256$4096:c2FsdA==$AAAA:*",
                ScramError::InvalidBase64,
            ),
            (
                &format!("SCRAM-SHA-256$4096:c2FsdA==${wrong_stored_key}:{valid_server_key}"),
                ScramError::InvalidKeyLength {
                    field: "stored_key",
                    expected: SHA256_KEY_LEN,
                    actual: SHA256_KEY_LEN - 1,
                },
            ),
            (
                &format!("SCRAM-SHA-256$4096:c2FsdA==${valid_server_key}:{wrong_server_key}"),
                ScramError::InvalidKeyLength {
                    field: "server_key",
                    expected: SHA256_KEY_LEN,
                    actual: SHA256_KEY_LEN + 1,
                },
            ),
        ] {
            assert!(
                PgScramVerifier::parse(input) == Err(expected),
                "case {input}"
            );
        }
    }

    #[test]
    fn parses_postgresql_format_fixture() {
        // PostgreSQL `pg_authid.rolpassword` shape for SCRAM-SHA-256 with
        // password "hunter2", 4096 iterations, and a fixed 16-byte salt.
        let fixture = "SCRAM-SHA-256$4096:AAECAwQFBgcICQoLDA0ODw==$ijmdkBF6VaUuRpEzxe8P8NyZKzuHGkoluJBVZ5DF5+Q=:naMMD2q/yKfDcTgfNhpZqsbkzoRQwOPfT/TgHEPWP7w=";

        let verifier = PgScramVerifier::parse(fixture).expect("PostgreSQL verifier parses");

        check!(verifier.iterations == 4096);
        check!(verifier.salt == (0_u8..16).collect::<Vec<_>>());
        check!(verifier.stored_key.len() == SHA256_KEY_LEN);
        check!(verifier.server_key.len() == SHA256_KEY_LEN);
        assert!(verifier.to_string() == fixture);
    }

    #[test]
    fn generated_verifier_material_authenticates_scram_sha256_exchange() {
        let password = fixture_password();
        let verifier = PgScramVerifier::generate(&password, 4096).expect("fresh verifier");
        let credential = ScramCredential::from(&verifier);
        let server = ScramServerExchange::new("alice".to_string(), credential);
        let client = ScramClientExchange::new(
            "alice".to_string(),
            password.into_bytes(),
            SaslMechanism::ScramSha256,
        );

        let (client_first, client) = client.client_first().expect("client first");
        let (server_first, server) = match server.step(&client_first) {
            StepResult::Continue(bytes, exchange) => (bytes, exchange),
            other => panic!("server first must continue, got {other:?}"),
        };
        let (client_final, client) = client.step(&server_first).expect("client final");
        let (principal, server_final) = match server.step(&client_final) {
            StepResult::Done(principal, bytes) => (principal, bytes),
            other => panic!("server final must finish, got {other:?}"),
        };

        check!(principal.name == "alice");
        check!(principal.auth_method == crate::AuthMethod::SaslScramSha256);
        assert!(client.verify_server_final(&server_final).is_ok());
    }
}
