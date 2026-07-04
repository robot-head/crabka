//! Ed25519 checkpoint signing for the audit hash-chain.
//!
//! `checkpoint_signing_bytes` defines the canonical signed payload; both the
//! writer (signing) and the verifier (verifying) call it — never reimplement.

use std::path::Path;

use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};

use crate::{
    ids::{EpochMs, Seq},
    sink::AuditError,
};

/// Domain-separation prefix for checkpoint signatures (versioned).
pub const CHECKPOINT_DOMAIN: &[u8] = b"crabka-audit-ckpt-v1\0";

/// Source of the broker's audit signing key. A file-backed Ed25519 impl ships
/// in Slice 2; a KMS/HSM backend can implement this trait later without
/// touching the chain logic.
pub trait SigningKeyProvider: Send + Sync + std::fmt::Debug {
    /// Stable identifier for the key, recorded on every checkpoint so chains
    /// span key-rotation epochs verifiably.
    fn key_id(&self) -> &str;
    /// Ed25519 signature over `msg`.
    fn sign(&self, msg: &[u8]) -> Vec<u8>;
    /// Raw 32-byte Ed25519 public key.
    fn public_key(&self) -> Vec<u8>;
}

/// File-backed Ed25519 signer (PKCS#8 v2 DER key).
pub struct FileEd25519Signer {
    key_id: String,
    key_pair: Ed25519KeyPair,
    public_key: Vec<u8>,
}

impl std::fmt::Debug for FileEd25519Signer {
    // cargo-mutants: Debug formatting is not behaviorally tested.
    #[cfg_attr(test, mutants::skip)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileEd25519Signer")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl FileEd25519Signer {
    /// Load a PKCS#8 Ed25519 key from `path`.
    #[tracing::instrument(level = "debug", skip_all, fields(key_id = %key_id), err)]
    pub fn from_pkcs8_file(path: impl AsRef<Path>, key_id: String) -> Result<Self, AuditError> {
        let der = std::fs::read(path.as_ref())
            .map_err(|e| AuditError::Key(format!("read key file: {e}")))?;
        Self::from_pkcs8_bytes(&der, key_id)
    }

    /// Load a PKCS#8 Ed25519 key from DER bytes.
    #[tracing::instrument(level = "debug", skip_all, fields(key_id = %key_id, bytes = der.len()), err)]
    pub fn from_pkcs8_bytes(der: &[u8], key_id: String) -> Result<Self, AuditError> {
        let key_pair = Ed25519KeyPair::from_pkcs8(der)
            .map_err(|_| AuditError::Key("invalid PKCS#8 Ed25519 key".to_string()))?;
        let public_key = key_pair.public_key().as_ref().to_vec();
        Ok(Self {
            key_id,
            key_pair,
            public_key,
        })
    }
}

impl SigningKeyProvider for FileEd25519Signer {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    fn sign(&self, msg: &[u8]) -> Vec<u8> {
        self.key_pair.sign(msg).as_ref().to_vec()
    }

    fn public_key(&self) -> Vec<u8> {
        self.public_key.clone()
    }
}

/// Canonical checkpoint signed payload:
/// `DOMAIN ‖ key_id_len(u16 BE) ‖ key_id ‖ seq_high(u64 BE) ‖ head(32) ‖ time_ms(i64 BE)`.
#[must_use]
pub fn checkpoint_signing_bytes(
    key_id: &str,
    seq_high: Seq,
    head: &[u8; 32],
    time_ms: EpochMs,
) -> Vec<u8> {
    let kid = key_id.as_bytes();
    #[allow(clippy::cast_possible_truncation)]
    let kid_len = kid.len().min(usize::from(u16::MAX)) as u16;
    let mut v = Vec::with_capacity(CHECKPOINT_DOMAIN.len() + 2 + kid.len() + 8 + 32 + 8);
    v.extend_from_slice(CHECKPOINT_DOMAIN);
    v.extend_from_slice(&kid_len.to_be_bytes());
    v.extend_from_slice(kid);
    v.extend_from_slice(&seq_high.0.to_be_bytes());
    v.extend_from_slice(head);
    v.extend_from_slice(&time_ms.0.to_be_bytes());
    v
}

/// Verify an Ed25519 signature. Returns `false` on any error.
#[must_use]
pub fn verify_signature(public_key: &[u8], msg: &[u8], sig: &[u8]) -> bool {
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(msg, sig)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };

    use super::*;

    fn fresh_signer(key_id: &str) -> (FileEd25519Signer, Vec<u8>) {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate");
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse");
        let pubkey = kp.public_key().as_ref().to_vec();
        let signer = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), key_id.to_string())
            .expect("signer");
        (signer, pubkey)
    }

    #[test]
    fn sign_then_verify_roundtrips() {
        let (signer, pubkey) = fresh_signer("k1");
        check!(signer.key_id() == "k1");
        check!(signer.public_key() == pubkey);
        let msg = b"the quick brown fox";
        let sig = signer.sign(msg);
        check!(verify_signature(&pubkey, msg, &sig));
        // tampered message fails
        check!(!verify_signature(&pubkey, b"the quick brown FOX", &sig));
        // wrong key fails
        let (_other, other_pub) = fresh_signer("k2");
        check!(!verify_signature(&other_pub, msg, &sig));
    }

    #[test]
    fn checkpoint_bytes_are_canonical_and_field_sensitive() {
        let head = [9u8; 32];
        let base = checkpoint_signing_bytes("k1", Seq(42), &head, EpochMs(1_700_000_000_000));
        check!(base == checkpoint_signing_bytes("k1", Seq(42), &head, EpochMs(1_700_000_000_000)));
        check!(base.starts_with(CHECKPOINT_DOMAIN));
        // any field change changes the bytes
        check!(checkpoint_signing_bytes("k2", Seq(42), &head, EpochMs(1_700_000_000_000)) != base);
        check!(checkpoint_signing_bytes("k1", Seq(43), &head, EpochMs(1_700_000_000_000)) != base);
        check!(
            checkpoint_signing_bytes("k1", Seq(42), &[8u8; 32], EpochMs(1_700_000_000_000)) != base
        );
        check!(checkpoint_signing_bytes("k1", Seq(42), &head, EpochMs(1)) != base);
    }

    #[test]
    fn bad_pkcs8_is_a_key_error() {
        let err = FileEd25519Signer::from_pkcs8_bytes(b"not-a-key", "k".into());
        check!(matches!(err, Err(crate::sink::AuditError::Key(_))));
    }
}
