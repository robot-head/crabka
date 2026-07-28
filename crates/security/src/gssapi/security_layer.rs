use crabka_units::{ByteSize, convert::ByteSizeExt as _};

/// RFC 4752 security-layer bitmask. We only support auth-only (matches Kafka).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecurityLayer(pub u8);

impl SecurityLayer {
    pub const AUTH: SecurityLayer = SecurityLayer(0x01);
    pub const INTEGRITY: SecurityLayer = SecurityLayer(0x02);
    pub const CONFIDENTIALITY: SecurityLayer = SecurityLayer(0x04);
}

#[derive(Debug, thiserror::Error)]
pub enum LayerError {
    #[error("security-layer message too short")]
    Short,
    #[error("client selected unsupported security layer {0:#04x} (only auth offered)")]
    Unsupported(u8),
    #[error("authzid is not valid UTF-8")]
    Authzid,
}

/// The RFC 4752 `max recv size` field as its three big-endian wire bytes.
///
/// The field is 24 bits wide, so the top byte of the `u32` is dropped —
/// unchanged from the original encoding, which took a `u32` and sliced
/// `[1..4]`. A cap wider than `u32` saturates rather than wrapping; every
/// configured cap is orders of magnitude below that.
fn max_recv_wire_bytes(max_recv: ByteSize) -> [u8; 3] {
    let raw = u32::try_from(max_recv.bytes_u64()).unwrap_or(u32::MAX);
    let [_, b1, b2, b3] = raw.to_be_bytes();
    [b1, b2, b3]
}

/// Server offer: 1-byte supported-layer bitmask + 3-byte big-endian max recv size.
#[must_use]
pub fn encode_offer(layers: SecurityLayer, max_recv: ByteSize) -> Vec<u8> {
    let [b1, b2, b3] = max_recv_wire_bytes(max_recv);
    vec![layers.0, b1, b2, b3]
}

/// Client choice reply: the selected layer, the client's max recv size, and an
/// optional authzid.
#[must_use]
pub fn encode_choice(
    selected: SecurityLayer,
    max_recv: ByteSize,
    authzid: Option<&str>,
) -> Vec<u8> {
    let [b1, b2, b3] = max_recv_wire_bytes(max_recv);
    let mut reply = vec![selected.0, b1, b2, b3];
    if let Some(z) = authzid {
        reply.extend_from_slice(z.as_bytes());
    }
    reply
}

/// Client choice parsed from the unwrapped response.
#[derive(Debug, PartialEq, Eq)]
pub struct LayerChoice {
    pub selected: SecurityLayer,
    pub max_size: u32,
    pub authzid: Option<String>,
}

/// Decode the client's choice. Rejects any selected layer other than auth.
/// # Errors
/// Returns an error when credentials or key material are invalid, cryptographic verification fails, or the TLS, SASL, or Kerberos exchange is rejected.
pub fn decode_choice(bytes: &[u8]) -> Result<LayerChoice, LayerError> {
    if bytes.len() < 4 {
        return Err(LayerError::Short);
    }
    let selected = SecurityLayer(bytes[0]);
    if selected != SecurityLayer::AUTH {
        return Err(LayerError::Unsupported(bytes[0]));
    }
    let max_size = u32::from_be_bytes([0, bytes[1], bytes[2], bytes[3]]);
    let authzid = if bytes.len() > 4 {
        Some(
            std::str::from_utf8(&bytes[4..])
                .map_err(|_| LayerError::Authzid)?
                .to_string(),
        )
    } else {
        None
    };
    Ok(LayerChoice {
        selected,
        max_size,
        authzid,
    })
}

/// Client side: read the server's offered-layer bitmask (first byte).
///
/// # Errors
/// Returns [`LayerError::Short`] if the offer is empty.
pub fn decode_offer_layers(bytes: &[u8]) -> Result<SecurityLayer, LayerError> {
    if bytes.is_empty() {
        return Err(LayerError::Short);
    }
    Ok(SecurityLayer(bytes[0]))
}

#[cfg(test)]
mod tests {

    use crabka_units::kibibytes;

    use super::*;

    #[test]
    fn encode_offer_auth_only() {
        // bitmask 0x01 (auth), max recv size 0x10000 (65536)
        let bytes = encode_offer(SecurityLayer::AUTH, kibibytes(64));
        assert2::assert!(bytes == vec![0x01, 0x01, 0x00, 0x00]);
    }

    /// The 24-bit `max recv size` field is what the peer reads, so a
    /// [`ByteSize`] must land on exactly the bytes the raw `u32` encoding
    /// produced: the big-endian value with its top octet dropped.
    #[test]
    fn max_recv_size_encodes_to_the_same_three_wire_bytes_as_the_raw_u32() {
        for raw in [
            0u32,
            1,
            0xFF,
            0x1000,
            0x1_0000,
            0x0010_0000,
            0x00FF_FFFF,
            // Above 24 bits the top octet is dropped, exactly as
            // `u32::to_be_bytes()[1..4]` always did.
            0x0100_0000,
            u32::MAX,
        ] {
            let expected = {
                let [_, b1, b2, b3] = raw.to_be_bytes();
                [b1, b2, b3]
            };
            assert2::check!(
                max_recv_wire_bytes(ByteSize::from_bytes(u64::from(raw))) == expected,
                "raw = {raw:#x}"
            );
        }
    }

    /// A cap beyond `u32` saturates instead of wrapping, so an absurd
    /// configuration cannot silently advertise a tiny buffer.
    #[test]
    fn max_recv_size_beyond_u32_saturates() {
        let huge = ByteSize::from_bytes(u64::from(u32::MAX) + 1_000);
        assert2::check!(max_recv_wire_bytes(huge) == [0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn encode_choice_appends_authzid_after_the_size_field() {
        assert2::check!(
            encode_choice(SecurityLayer::AUTH, kibibytes(64), None) == vec![0x01, 0x01, 0x00, 0x00]
        );
        assert2::check!(
            encode_choice(SecurityLayer::AUTH, kibibytes(64), Some("alice"))
                == vec![0x01, 0x01, 0x00, 0x00, b'a', b'l', b'i', b'c', b'e']
        );
    }

    #[test]
    fn decode_client_choice_auth_no_authzid() {
        // selected 0x01, max size 0x1000, no authzid
        let bytes = [0x01u8, 0x00, 0x10, 0x00];
        let choice = decode_choice(&bytes).unwrap();
        assert2::assert!(
            choice
                == LayerChoice {
                    selected: SecurityLayer::AUTH,
                    max_size: 0x1000,
                    authzid: None,
                }
        );
    }

    #[test]
    fn decode_client_choice_with_authzid() {
        let mut bytes = vec![0x01u8, 0x00, 0x10, 0x00];
        bytes.extend_from_slice(b"alice");
        let choice = decode_choice(&bytes).unwrap();
        assert2::assert!(choice.authzid.as_deref() == Some("alice"));
    }

    #[test]
    fn decode_rejects_non_auth_layer() {
        // client picked integrity (0x02) which we never offered
        let bytes = [0x02u8, 0x00, 0x10, 0x00];
        assert2::assert!(decode_choice(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_short_message() {
        assert2::assert!(decode_choice(&[0x01u8, 0x00]).is_err());
    }
}
