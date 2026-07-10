//! KIP-48 delegation token primitives — HMAC and secret-key
//! wrapper that keeps the bytes out of Debug.

use bytes::Bytes;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

#[derive(Clone, PartialEq, Eq)]
pub struct SecretBytes(Bytes);

impl SecretBytes {
    #[must_use]
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretBytes(<{} bytes redacted>)", self.0.len())
    }
}

#[must_use]
pub fn compute_token_hmac(secret_key: &[u8], token_id: &str) -> Vec<u8> {
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(secret_key).expect("HMAC-SHA-256 accepts any key length");
    mac.update(token_id.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn secret_bytes_accessors() {
        let s = SecretBytes::new(Bytes::from_static(b"abc"));
        check!(
            (
                s.as_bytes(),
                s.len(),
                s.is_empty(),
                SecretBytes::new(Bytes::new()).is_empty(),
            ) == (b"abc".as_slice(), 3, false, true)
        );
    }

    #[test]
    fn hmac_is_deterministic_for_same_inputs() {
        let h1 = compute_token_hmac(b"k", "tok-1");
        let h2 = compute_token_hmac(b"k", "tok-1");
        assert2::assert!(&h1 == &h2);
        assert2::assert!(h1.len() == 32);
    }

    #[test]
    fn hmac_diverges_on_key_change() {
        let h1 = compute_token_hmac(b"k1", "tok-1");
        let h2 = compute_token_hmac(b"k2", "tok-1");
        assert2::assert!(h1 != h2);
    }

    #[test]
    fn secret_bytes_debug_does_not_leak_bytes() {
        let s = SecretBytes::new(b"super-secret-master-key".to_vec());
        let d = format!("{s:?}");
        assert2::assert!(d.contains("redacted"));
        assert2::assert!(!d.contains("super-secret"));
    }
}
