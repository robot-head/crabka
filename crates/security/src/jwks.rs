//! JSON Web Key Set (RFC 7517) parsing + JWS signature verification.
//!
//! Pure logic, no I/O: this module parses a JWKS *string* and verifies a
//! token's signature against an in-memory [`Jwks`]. The broker fetches the key
//! set over the network and feeds it in via a [`JwksHandle`]. Supported key
//! types / algorithms:
//!
//! - `RS256` — RSASSA-PKCS1-v1_5 + SHA-256 over an `RSA` key (`n`, `e`).
//! - `ES256` — ECDSA P-256 + SHA-256 over an `EC` key (`crv:P-256`, `x`, `y`).
//!
//! These cover the overwhelming majority of OAuth 2.0 identity providers.
//! Verification is delegated to `ring` — the same crypto backend the rest of
//! `crates/security` uses — rather than pulling in a JWT crate, which keeps the
//! dependency surface small and the temporal checks (`exp` / `iat` / `nbf`)
//! injectable for deterministic tests.

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64URL};
use ring::signature;
use serde_json::Value;

use crate::AuthError;

/// A single supported public key from a JWKS, keyed externally by `kid`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum JwkKey {
    /// RSA public key: big-endian modulus + exponent, as decoded from the JWK
    /// `n` / `e` members. Fed verbatim to `ring`'s `RsaPublicKeyComponents`.
    Rsa { n: Vec<u8>, e: Vec<u8> },
    /// EC P-256 public key: the SEC1 uncompressed point `0x04 ‖ x ‖ y`, ready
    /// for `ring`'s `UnparsedPublicKey`.
    EcP256 { point: Vec<u8> },
}

/// A parsed JWK key set. Lookups are by `kid`; keys without a `kid` are stored
/// under the empty string and used when a token header omits `kid` and the set
/// holds exactly one key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Jwks {
    keys: HashMap<String, JwkKey>,
}

impl Jwks {
    /// An empty key set — nothing validates until the broker's refresher
    /// populates it.
    #[must_use]
    // cargo-mutants: trivial constructor.
    #[cfg_attr(test, mutants::skip)]
    pub fn empty() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Whether the key set has no usable keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Number of usable keys (skips unsupported `kty` / `crv`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Parse an RFC 7517 JWKS document. Keys with an unsupported `kty` /
    /// `crv` / `alg` are skipped (not an error — identity providers publish
    /// encryption keys and other algs alongside the signing keys we care
    /// about). A document that is not valid JSON or lacks a `keys` array is an
    /// error.
    ///
    /// When `ignore_key_use` is `false` (the default), keys with
    /// `use=enc` are filtered out (encryption-only keys are unsuitable for
    /// signature verification). When `true`, all keys are kept regardless of
    /// `use`. Strimzi exposes the same toggle; some identity providers serve
    /// signing keys with `use:"enc"` by mistake, and operators occasionally
    /// need to override.
    ///
    /// # Errors
    ///
    /// [`AuthError::MalformedMessage`] when the document is not valid JSON or
    /// has no `keys` array.
    // JWKS key-set load/reload. skip_all keeps the raw JWKS document `s` out of
    // span fields (it is not secret, but is large); only the non-sensitive
    // `ignore_key_use` toggle is recorded. `err` surfaces a parse failure.
    #[tracing::instrument(level = "info", skip_all, fields(ignore_key_use = ignore_key_use), err)]
    pub fn from_json(s: &str, ignore_key_use: bool) -> Result<Self, AuthError> {
        let doc: Value = serde_json::from_str(s).map_err(|_| AuthError::MalformedMessage)?;
        let arr = doc
            .get("keys")
            .and_then(Value::as_array)
            .ok_or(AuthError::MalformedMessage)?;
        let mut keys = HashMap::new();
        for jwk in arr {
            let Some((kid, key)) = parse_one_jwk(jwk, ignore_key_use) else {
                continue;
            };
            keys.insert(kid, key);
        }
        Ok(Self { keys })
    }

    /// Whether this key set holds an entry for `kid`. Convenience for callers
    /// that want to assert on individual keys without exposing the internal
    /// map type.
    #[must_use]
    pub fn contains_kid(&self, kid: &str) -> bool {
        self.keys.contains_key(kid)
    }

    /// Verify a JWS `signature` over `signing_input` (the ASCII
    /// `header_b64 "." payload_b64`) using the key selected by `kid` / `alg`.
    ///
    /// `kid` is the token header `kid` (when present). When `None`, the set
    /// must hold exactly one key. `alg` must be `RS256` or `ES256` and must
    /// match the selected key's type.
    ///
    /// # Errors
    ///
    /// [`AuthError::InvalidToken`] for an unknown / ambiguous key, an
    /// alg/key-type mismatch, or a bad signature.
    // Per-token JWS signature verification. skip_all keeps `signing_input` +
    // `signature` bytes out of span fields; only the non-sensitive `alg` and
    // `kid` (a key identifier, not a secret) are recorded. `err` surfaces a
    // verification failure (Debug).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(alg = %alg, kid = kid.unwrap_or("<none>")),
        err
    )]
    pub fn verify(
        &self,
        kid: Option<&str>,
        alg: &str,
        signing_input: &[u8],
        signature: &[u8],
    ) -> Result<(), AuthError> {
        let key = self.select_key(kid)?;
        match (alg, key) {
            ("RS256", JwkKey::Rsa { n, e }) => {
                let pk = ring::signature::RsaPublicKeyComponents { n, e };
                pk.verify(
                    &signature::RSA_PKCS1_2048_8192_SHA256,
                    signing_input,
                    signature,
                )
                .map_err(|_| AuthError::InvalidToken)
            }
            ("ES256", JwkKey::EcP256 { point }) => {
                let pk =
                    signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, point);
                pk.verify(signing_input, signature)
                    .map_err(|_| AuthError::InvalidToken)
            }
            // alg present but no key of the matching type, or unsupported alg.
            _ => Err(AuthError::InvalidToken),
        }
    }

    /// Pick the key for `kid`. With no `kid`, succeed only when the set holds a
    /// single key (otherwise selection is ambiguous).
    fn select_key(&self, kid: Option<&str>) -> Result<&JwkKey, AuthError> {
        match kid {
            Some(kid) => self.keys.get(kid).ok_or(AuthError::InvalidToken),
            None => {
                if self.keys.len() == 1 {
                    self.keys.values().next().ok_or(AuthError::InvalidToken)
                } else {
                    Err(AuthError::InvalidToken)
                }
            }
        }
    }
}

/// Parse a single JWK object into a `(kid, key)` pair, or `None` for an
/// unsupported / malformed key (skipped, not fatal). `use` is honored when
/// present: a key explicitly marked for encryption (`use: "enc"`) is skipped
/// unless `ignore_key_use` is true.
fn parse_one_jwk(jwk: &Value, ignore_key_use: bool) -> Option<(String, JwkKey)> {
    if !ignore_key_use && jwk.get("use").and_then(Value::as_str) == Some("enc") {
        return None;
    }
    let kid = jwk
        .get("kid")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match jwk.get("kty").and_then(Value::as_str)? {
        "RSA" => {
            let n = b64url_field(jwk, "n")?;
            let e = b64url_field(jwk, "e")?;
            Some((kid, JwkKey::Rsa { n, e }))
        }
        "EC" => {
            if jwk.get("crv").and_then(Value::as_str)? != "P-256" {
                return None;
            }
            // P-256 coordinates are 32 bytes; JWK base64url may drop leading
            // zero bytes, so left-pad before assembling the SEC1 point.
            let x = left_pad_32(&b64url_field(jwk, "x")?)?;
            let y = left_pad_32(&b64url_field(jwk, "y")?)?;
            let mut point = Vec::with_capacity(65);
            point.push(0x04);
            point.extend_from_slice(&x);
            point.extend_from_slice(&y);
            Some((kid, JwkKey::EcP256 { point }))
        }
        _ => None,
    }
}

/// base64url-decode a string-valued JWK member.
fn b64url_field(jwk: &Value, key: &str) -> Option<Vec<u8>> {
    let s = jwk.get(key).and_then(Value::as_str)?;
    B64URL.decode(s).ok()
}

/// Left-pad a big-endian coordinate to exactly 32 bytes. Returns `None` if the
/// input is already longer than 32 bytes (malformed P-256 coordinate).
fn left_pad_32(bytes: &[u8]) -> Option<[u8; 32]> {
    if bytes.len() > 32 {
        return None;
    }
    let mut out = [0u8; 32];
    out[32 - bytes.len()..].copy_from_slice(bytes);
    Some(out)
}

/// A cheaply-clonable, atomically-swappable holder for the live [`Jwks`].
///
/// Mirrors `DynamicServerConfig`: the broker's background JWKS
/// refresher [`store`](JwksHandle::store)s a freshly-fetched key set while
/// validators [`load`](JwksHandle::load) the current one with no lock. Cloning
/// a handle shares the same underlying cell.
///
/// Handles paired with a refresher additionally carry a shared
/// `last_successful_fetch_ms` counter (for hard cache-expiry checks in the
/// signed validator) and a `signal_tx` mpsc sender (for fire-and-forget
/// on-demand refresh requests when validators encounter unknown-kid /
/// bad-signature tokens). Default-constructed handles carry no signal sender
/// — `signal_refresh()` is a silent no-op on those.
#[derive(Debug, Clone)]
pub struct JwksHandle {
    keys: Arc<ArcSwap<Jwks>>,
    /// Epoch ms of last successful refresh. Validators
    /// check this against `expiry_ms` to fail closed on stale cache.
    /// `0` sentinel = never successfully fetched (initial state).
    last_successful_fetch_ms: Arc<std::sync::atomic::AtomicI64>,
    /// Fire-and-forget signal sender to the refresher.
    /// Validator calls `signal_refresh()` on verify failure (unknown
    /// kid or bad signature). `None` when the validator isn't paired
    /// with a refresher (e.g., default-constructed `JwksHandle` in
    /// non-signed validators or pre-`apply_to` state).
    signal_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl JwksHandle {
    /// Wrap an initial key set (often [`Jwks::empty`] at startup) with NO
    /// refresher coordination. Used by default-constructed handles and tests
    /// that don't need the signal channel. `signal_refresh()` on these is a
    /// silent no-op.
    #[must_use]
    pub fn new(jwks: Jwks) -> Self {
        Self {
            keys: Arc::new(ArcSwap::from_pointee(jwks)),
            last_successful_fetch_ms: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            signal_tx: None,
        }
    }

    /// Wrap an initial key set WITH the shared timestamp counter
    /// and signal sender pre-wired. The refresher constructs its own
    /// `(signal_tx, signal_rx)` pair and passes `signal_tx` here; the
    /// refresher holds `signal_rx` and a clone of the shared
    /// `Arc<AtomicI64>` for timestamp updates.
    #[must_use]
    pub fn new_with_refresher_handles(
        jwks: Jwks,
        last_successful_fetch_ms: Arc<std::sync::atomic::AtomicI64>,
        signal_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Self {
        Self {
            keys: Arc::new(ArcSwap::from_pointee(jwks)),
            last_successful_fetch_ms,
            signal_tx: Some(signal_tx),
        }
    }

    /// Atomically replace the key set — called by the refresher after a
    /// successful fetch. Lock-free; concurrent `load`s see either the old or
    /// the new set, never a torn one.
    pub fn store(&self, jwks: Jwks) {
        self.keys.store(Arc::new(jwks));
    }

    /// Load the current key set. Cheap (an `Arc` clone).
    #[must_use]
    pub fn load(&self) -> Arc<Jwks> {
        self.keys.load_full()
    }

    /// Epoch-ms timestamp of last successful JWKS fetch. `0` if no
    /// fetch has succeeded yet (initial state). Validators compare against
    /// `now_ms - expiry_ms` to enforce hard cache expiry.
    #[must_use]
    pub fn last_successful_fetch_ms(&self) -> i64 {
        self.last_successful_fetch_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Fire-and-forget signal to the refresher that an on-demand
    /// refresh is requested (e.g., unknown-kid token). Non-blocking — drops
    /// silently if the channel is full (signals coalesce; one is enough).
    /// No-op when `signal_tx` is `None` (default-constructed handles).
    pub fn signal_refresh(&self) {
        if let Some(tx) = &self.signal_tx {
            let _ = tx.try_send(());
        }
    }
}

impl Default for JwksHandle {
    fn default() -> Self {
        Self::new(Jwks::empty())
    }
}

// Token-minting helpers the sibling `oauthbearer` tests reuse. Declared before
// the test module so clippy's `items_after_test_module` stays quiet.
#[cfg(test)]
pub(crate) use tests::{mint_es256, mint_rs256, mint_rs256_with_header};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use ring::{
        rand::SystemRandom,
        signature::{EcdsaKeyPair, KeyPair, RsaKeyPair},
    };

    use super::*;

    /// A static RSA-2048 PKCS#8 key (generated with `openssl genpkey`). `ring`
    /// cannot generate RSA keys, so tests mint RS256 tokens from this fixed
    /// key. Production never sees a private key — it reads `n`/`e` from JWKS.
    const RSA_PKCS8_B64: &str = "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC1Ekoc++7sSsH55QXBCq/aj71helk6ZCTkzYxfLRZXbox0FcV7vOkLNodetJLY7nAUekZLltQ7Q6FJ42geqGV+vgttF63Ue9OP24mPmn/OiFqVYhBaJDRI5BMBLCqZbUfpNBDh7ZOCczwlX8Z5FQS0QJBA4F26H9AKzFRvofwHFk1wxqiGdgwDyClgi+eDnhEGGhBEHuTl1edvTRif88rLDfPHKG1TRqKC6LMXCZQdNy7lrDEGPKHqfW4mb2mq7Vj6h2Jjv+1SpsSxdqX8Tsua4/LrAKvFIXfoZAnjzhACbhXqf1DdSdInZ0i1adY8JpgJQ+WtJ0i9aIOnnmDYwgMvAgMBAAECggEAHqBqUr62Kdd3Odpn/7/cAL7hTHSSVRMNPnoZ7RtGNSGothXcolJQpKnjebxXPkQORxhrfWuUmDWXOVUyjkTzbd2dNyWTLGaJYULD4LtENN3RXIUKuQR4p3+US1V6Gxtl12cMF/rEQYNWQAgUHPTWJ9rny2Fn2Qx6dukauwsOAvCU47fL873sm06SYgPJsLm7MKVeifl8dDudgpURxeC9z37cm9kjjE6n6aiBTNAuBEkMaAbcfgJ0RZfzaMo7IpsOeyOwp932JDlKROpQWKA+lz08YzhkU81qHJYOS/js2F0jxzFz31D9IN+OLu7vRCANFLJl/qnin1JEgVPh7gxSKQKBgQDfrQEsutvH1746ytfE+4jUXyv7Fuaz9MML8uaJbC4hMFdCJuMuLBY07bDE23+4byuWY7JHrgsLRaZ+qpNGWs3LH2x6xsHiK8Ivpuy8TVUJ6hgkPK1cr8yUJxaDcyV8tJAZ+mFmyyWx7wUdlgJFCa2MQF1HnrlBKZvSLWV4CjctZQKBgQDPPR2wLwyk6JlyapsVnCpNBGcXqbJxPh1TM7uPqlODxTzegUK+TMJDZ840u2aBNXf2D5WIJMl+/ohYefOOqK9z2OJUGObnJMgGusH04rdbBoDCdBwfwjiluU7vxbuQKBu8JNXzeb7HJhmgxtXWdJuFYcYbmGu8leFvmUxZTPRfAwKBgQCm6Gpf/m/SiGMjbAnmq+xGzV38V/J/hr2lRPRSx68EhRYX/vy3j55ikJu/yitcbViROIPoiS8kkizTiGWtskSuthw04ev74btd46n0OaCjbVPmdoDHEUgPpbtfC6WFkReWyweztRPD2yBuG2pGKhqe9cilkQOcZHgqNkXpdXYHIQKBgQCO0BQkdNfm0O/l3DdRdhPkjVMqCGSTC3YT/0OS5pK07PhccYF4ONdqsh91UWt7QUiRBf5LGubMoEV/i1LfjbmTQPP/dkWxJjS+Bndg9dfbX6jd2DwFWsfE1OXj8ESoPCuYxV23cr+Y59WjaUK1jhgam9106N3d0P/Q8zidFZ4V1wKBgQDFvIqMLnpaInWhb7kP+X6o0tPQSg+6odMWPnjhwnpSIiUjPUTZV4ijc/d1tPsUemFQxDe+ZreQXDMVGcAVldFnoEMyL8iAtMAHtsSmq2E80RNZfc6nUgy5esQ9rJeX2pH9aZCVvKv6iVTeUtAxS+ltjmEG9BSEI2WQI1WDzPbKiA==";

    fn rsa_pkcs8() -> Vec<u8> {
        use base64::engine::general_purpose::STANDARD;
        STANDARD.decode(RSA_PKCS8_B64).unwrap()
    }

    /// Split the two big-endian INTEGERs (modulus, publicExponent) out of a
    /// PKCS#1 `RSAPublicKey` DER. Test-only — production reads `n`/`e` straight
    /// from JWKS JSON.
    fn split_pkcs1_public(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
        // SEQUENCE { INTEGER n, INTEGER e }
        let mut p = 0usize;
        assert!(der[p] == 0x30, "expected SEQUENCE");
        p += 1;
        let (_seq_len, adv) = read_der_len(&der[p..]);
        p += adv;
        let n = read_der_integer(der, &mut p);
        let e = read_der_integer(der, &mut p);
        (n, e)
    }

    fn read_der_len(b: &[u8]) -> (usize, usize) {
        if b[0] & 0x80 == 0 {
            (b[0] as usize, 1)
        } else {
            let nbytes = (b[0] & 0x7f) as usize;
            let mut len = 0usize;
            for i in 0..nbytes {
                len = (len << 8) | b[1 + i] as usize;
            }
            (len, 1 + nbytes)
        }
    }

    fn read_der_integer(der: &[u8], p: &mut usize) -> Vec<u8> {
        assert!(der[*p] == 0x02, "expected INTEGER");
        *p += 1;
        let (len, adv) = read_der_len(&der[*p..]);
        *p += adv;
        let mut bytes = der[*p..*p + len].to_vec();
        *p += len;
        // Strip the leading zero a DER INTEGER prepends to keep the high bit
        // clear; JWK `n`/`e` are unsigned big-endian.
        if bytes.first() == Some(&0) {
            bytes.remove(0);
        }
        bytes
    }

    fn b64(b: &[u8]) -> String {
        B64URL.encode(b)
    }

    /// Mint an RS256 token signed by the static RSA key, returning
    /// `(token, jwks_json)` where the JWKS advertises the matching public key
    /// under `kid`.
    fn rs256(kid: &str, claims: &str) -> (String, String) {
        let der = rsa_pkcs8();
        let kp = RsaKeyPair::from_pkcs8(&der).unwrap();
        let header = format!("{{\"alg\":\"RS256\",\"kid\":\"{kid}\"}}");
        let signing_input = format!("{}.{}", b64(header.as_bytes()), b64(claims.as_bytes()));
        let mut sig = vec![0u8; kp.public().modulus_len()];
        kp.sign(
            &signature::RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            signing_input.as_bytes(),
            &mut sig,
        )
        .unwrap();
        let token = format!("{signing_input}.{}", b64(&sig));

        let pkcs1 = kp.public().as_ref();
        let (n, e) = split_pkcs1_public(pkcs1);
        let jwks = format!(
            "{{\"keys\":[{{\"kty\":\"RSA\",\"kid\":\"{kid}\",\"n\":\"{}\",\"e\":\"{}\"}}]}}",
            b64(&n),
            b64(&e),
        );
        (token, jwks)
    }

    /// Generate a fresh ES256 key pair and return `(key_pair, jwks_json)`.
    fn es256_key(kid: &str) -> (EcdsaKeyPair, String) {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&signature::ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .unwrap();
        let kp = EcdsaKeyPair::from_pkcs8(
            &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            pkcs8.as_ref(),
            &rng,
        )
        .unwrap();
        let point = kp.public_key().as_ref(); // 0x04 || x || y
        let x = &point[1..33];
        let y = &point[33..65];
        let jwks = format!(
            "{{\"keys\":[{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"{kid}\",\"x\":\"{}\",\"y\":\"{}\"}}]}}",
            b64(x),
            b64(y),
        );
        (kp, jwks)
    }

    fn es256_token(kp: &EcdsaKeyPair, kid: &str, claims: &str) -> String {
        let header = format!("{{\"alg\":\"ES256\",\"kid\":\"{kid}\"}}");
        let signing_input = format!("{}.{}", b64(header.as_bytes()), b64(claims.as_bytes()));
        let sig = kp
            .sign(&SystemRandom::new(), signing_input.as_bytes())
            .unwrap();
        format!("{signing_input}.{}", b64(sig.as_ref()))
    }

    /// Pull `(kid, alg, signing_input, sig)` out of a compact JWS for the
    /// verify-level tests.
    fn parts(token: &str) -> (Option<String>, String, Vec<u8>, Vec<u8>) {
        let segs: Vec<&str> = token.split('.').collect();
        let header: Value = serde_json::from_slice(&B64URL.decode(segs[0]).unwrap()).unwrap();
        let kid = header
            .get("kid")
            .and_then(Value::as_str)
            .map(str::to_string);
        let alg = header
            .get("alg")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        let signing_input = format!("{}.{}", segs[0], segs[1]).into_bytes();
        let sig = B64URL.decode(segs[2]).unwrap();
        (kid, alg, signing_input, sig)
    }

    #[test]
    fn empty_jwks_has_no_keys() {
        let jwks = Jwks::empty();
        check!(jwks.is_empty());
        check!(jwks.len() == 0);
        check!(!jwks.contains_kid("k1"));
    }

    #[test]
    fn jwks_with_key_is_not_empty() {
        let (_token, jwks_json) = rs256("k1", "{\"sub\":\"alice\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_json, false).expect("parse jwks");
        check!(!jwks.is_empty());
        check!(jwks.len() == 1);
        check!(jwks.contains_kid("k1"));
    }

    #[test]
    fn left_pad_32_accepts_short_and_exact_coordinates_only() {
        assert!(left_pad_32(&[0xAA; 33]).is_none());

        let short = left_pad_32(&[0x01, 0x02]).expect("short coordinate padded");
        assert!(short[..30] == [0u8; 30]);
        assert!(short[30..] == [0x01, 0x02]);

        let exact = [0x7Fu8; 32];
        assert!(left_pad_32(&exact).expect("exact coordinate") == exact);
    }

    #[test]
    fn parses_mixed_rsa_and_ec_set() {
        let (_kp, ec_jwks) = es256_key("ec1");
        let (_t, rsa_jwks) = rs256("rsa1", "{\"sub\":\"a\",\"exp\":9999999999}");
        // Merge the two single-key documents into one set.
        let ec: Value = serde_json::from_str(&ec_jwks).unwrap();
        let rsa: Value = serde_json::from_str(&rsa_jwks).unwrap();
        let mut keys = ec["keys"].as_array().unwrap().clone();
        keys.extend(rsa["keys"].as_array().unwrap().clone());
        let merged = serde_json::json!({ "keys": keys }).to_string();
        let jwks = Jwks::from_json(&merged, false).unwrap();
        assert!(jwks.len() == 2);
    }

    #[test]
    fn skips_unsupported_and_enc_keys() {
        let jwks = Jwks::from_json(
            r#"{"keys":[
                {"kty":"oct","k":"AAAA","kid":"sym"},
                {"kty":"EC","crv":"P-384","kid":"big"},
                {"kty":"RSA","use":"enc","kid":"enc1","n":"AQAB","e":"AQAB"}
            ]}"#,
            false,
        )
        .unwrap();
        assert!(jwks.is_empty());
    }

    #[test]
    fn rejects_non_json_and_missing_keys_array() {
        assert!(Jwks::from_json("not json", false) == Err(AuthError::MalformedMessage));
        assert!(Jwks::from_json("{}", false) == Err(AuthError::MalformedMessage));
    }

    #[test]
    fn verifies_valid_rs256() {
        let (token, jwks_json) = rs256("rsa1", "{\"sub\":\"admin\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_json, false).unwrap();
        let (kid, alg, si, sig) = parts(&token);
        assert!(jwks.verify(kid.as_deref(), &alg, &si, &sig).is_ok());
    }

    #[test]
    fn verifies_valid_es256() {
        let (kp, jwks_json) = es256_key("ec1");
        let token = es256_token(&kp, "ec1", "{\"sub\":\"admin\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_json, false).unwrap();
        let (kid, alg, si, sig) = parts(&token);
        assert!(jwks.verify(kid.as_deref(), &alg, &si, &sig).is_ok());
    }

    #[test]
    fn rejects_tampered_rs256_signature() {
        let (token, jwks_json) = rs256("rsa1", "{\"sub\":\"admin\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_json, false).unwrap();
        let (kid, alg, si, mut sig) = parts(&token);
        sig[0] ^= 0xff;
        assert!(jwks.verify(kid.as_deref(), &alg, &si, &sig) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn rejects_unknown_kid() {
        let (token, jwks_json) = rs256("rsa1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_json, false).unwrap();
        let (_kid, alg, si, sig) = parts(&token);
        assert!(jwks.verify(Some("other"), &alg, &si, &sig) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn rejects_missing_kid_when_set_has_multiple_keys() {
        let (kp, ec_jwks) = es256_key("ec1");
        let (_t, rsa_jwks) = rs256("rsa1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let ec: Value = serde_json::from_str(&ec_jwks).unwrap();
        let rsa: Value = serde_json::from_str(&rsa_jwks).unwrap();
        let mut keys = ec["keys"].as_array().unwrap().clone();
        keys.extend(rsa["keys"].as_array().unwrap().clone());
        let merged = serde_json::json!({ "keys": keys }).to_string();
        let jwks = Jwks::from_json(&merged, false).unwrap();
        // Token minted without a kid in the header.
        let header = b64(b"{\"alg\":\"ES256\"}");
        let payload = b64(b"{\"sub\":\"a\",\"exp\":9999999999}");
        let signing_input = format!("{header}.{payload}");
        let sig = kp
            .sign(&SystemRandom::new(), signing_input.as_bytes())
            .unwrap();
        assert!(
            jwks.verify(None, "ES256", signing_input.as_bytes(), sig.as_ref())
                == Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn accepts_missing_kid_when_set_has_single_key() {
        let (token, jwks_json) =
            mint_rs256_with_header("{\"alg\":\"RS256\"}", "{\"sub\":\"a\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_json, false).unwrap();
        let (kid, alg, si, sig) = parts(&token);
        assert!(kid.is_none());
        jwks.verify(kid.as_deref(), &alg, &si, &sig)
            .expect("single-key JWKS should verify a token without kid");
    }

    #[test]
    fn rejects_es256_signed_by_wrong_key() {
        let (kp_a, _jwks_a) = es256_key("ec1");
        let (_kp_b, jwks_b) = es256_key("ec1"); // same kid, different key
        let token = es256_token(&kp_a, "ec1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_b, false).unwrap();
        let (kid, alg, si, sig) = parts(&token);
        assert!(jwks.verify(kid.as_deref(), &alg, &si, &sig) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn rejects_alg_key_type_mismatch() {
        // RSA key set, but ask it to verify with ES256.
        let (token, jwks_json) = rs256("rsa1", "{\"sub\":\"a\",\"exp\":9999999999}");
        let jwks = Jwks::from_json(&jwks_json, false).unwrap();
        let (kid, _alg, si, sig) = parts(&token);
        assert!(jwks.verify(kid.as_deref(), "ES256", &si, &sig) == Err(AuthError::InvalidToken));
    }

    #[test]
    fn handle_store_and_load_round_trips() {
        let h = JwksHandle::default();
        assert!(h.load().is_empty());
        let (_t, jwks_json) = rs256("rsa1", "{\"sub\":\"a\",\"exp\":9999999999}");
        h.store(Jwks::from_json(&jwks_json, false).unwrap());
        assert!(h.load().len() == 1);
    }

    // ---- ignore_key_use filter + handle helpers ------------------

    #[test]
    fn parse_jwks_filters_use_enc_by_default() {
        let json = r#"{
            "keys": [
                {"kty":"RSA","kid":"sig-key","use":"sig","n":"AQAB","e":"AQAB"},
                {"kty":"RSA","kid":"enc-key","use":"enc","n":"AQAB","e":"AQAB"}
            ]
        }"#;
        let jwks = Jwks::from_json(json, false).expect("parses");
        assert!(jwks.contains_kid("sig-key"));
        assert!(!jwks.contains_kid("enc-key"));
    }

    #[test]
    fn parse_jwks_keeps_use_enc_when_ignore_key_use_true() {
        let json = r#"{
            "keys": [
                {"kty":"RSA","kid":"sig-key","use":"sig","n":"AQAB","e":"AQAB"},
                {"kty":"RSA","kid":"enc-key","use":"enc","n":"AQAB","e":"AQAB"}
            ]
        }"#;
        let jwks = Jwks::from_json(json, true).expect("parses");
        assert!(jwks.contains_kid("sig-key"));
        assert!(jwks.contains_kid("enc-key"));
    }

    #[test]
    fn parse_jwks_keeps_keys_with_absent_use_field_regardless() {
        let json = r#"{
            "keys": [
                {"kty":"RSA","kid":"no-use","n":"AQAB","e":"AQAB"}
            ]
        }"#;
        assert!(Jwks::from_json(json, false).unwrap().contains_kid("no-use"));
        assert!(Jwks::from_json(json, true).unwrap().contains_kid("no-use"));
    }

    #[test]
    fn default_handle_signal_refresh_is_silent_noop() {
        // A handle constructed without refresher wiring must not panic on
        // signal_refresh(); validators may share the same code path whether
        // or not a refresher is paired.
        let h = JwksHandle::default();
        h.signal_refresh();
        assert!(h.last_successful_fetch_ms() == 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paired_handle_signal_refresh_delivers_to_receiver() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let ts = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let h = JwksHandle::new_with_refresher_handles(Jwks::empty(), ts, tx);
        h.signal_refresh();
        assert!(rx.try_recv().is_ok(), "signal should land in receiver");
    }

    // Expose minting helpers to the sibling `oauthbearer` tests.
    pub(crate) fn mint_rs256(kid: &str, claims: &str) -> (String, String) {
        rs256(kid, claims)
    }

    /// Mint an RS256 token with a fully-controlled header JSON (so the
    /// caller can drive `typ` / `kid` / extra header fields). Returns
    /// `(token, jwks_json)` like the other minters.
    pub(crate) fn mint_rs256_with_header(header_json: &str, claims: &str) -> (String, String) {
        let der = rsa_pkcs8();
        let kp = RsaKeyPair::from_pkcs8(&der).unwrap();
        let signing_input = format!("{}.{}", b64(header_json.as_bytes()), b64(claims.as_bytes()));
        let mut sig = vec![0u8; kp.public().modulus_len()];
        kp.sign(
            &signature::RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            signing_input.as_bytes(),
            &mut sig,
        )
        .unwrap();
        let token = format!("{signing_input}.{}", b64(&sig));

        // Echo the JWKS used by `rs256` so the validator can verify the
        // signature. Default kid="k1" so tests don't have to thread it.
        let pkcs1 = kp.public().as_ref();
        let (n, e) = split_pkcs1_public(pkcs1);
        let jwks = format!(
            "{{\"keys\":[{{\"kty\":\"RSA\",\"kid\":\"k1\",\"n\":\"{}\",\"e\":\"{}\"}}]}}",
            b64(&n),
            b64(&e),
        );
        (token, jwks)
    }

    /// Mint an ES256 token under a *fresh* key, returning `(token, jwks_json)`.
    /// Each call generates a new key pair, so two calls yield independent keys
    /// — useful for key-rotation tests where RS256's fixed key can't differ.
    pub(crate) fn mint_es256(kid: &str, claims: &str) -> (String, String) {
        let (kp, jwks) = es256_key(kid);
        let token = es256_token(&kp, kid, claims);
        (token, jwks)
    }
}
