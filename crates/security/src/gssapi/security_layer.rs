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

/// Server offer: 1-byte supported-layer bitmask + 3-byte big-endian max recv size.
#[must_use]
pub fn encode_offer(layers: SecurityLayer, max_recv_size: u32) -> Vec<u8> {
    let s = max_recv_size.to_be_bytes(); // [b0,b1,b2,b3]
    vec![layers.0, s[1], s[2], s[3]]
}

/// Client choice parsed from the unwrapped response.
#[derive(Debug)]
pub struct LayerChoice {
    pub selected: SecurityLayer,
    pub max_size: u32,
    pub authzid: Option<String>,
}

/// Decode the client's choice. Rejects any selected layer other than auth.
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
    use super::*;
    use assert2::assert;

    #[test]
    fn encode_offer_auth_only() {
        // bitmask 0x01 (auth), max recv size 0x10000 (65536)
        let bytes = encode_offer(SecurityLayer::AUTH, 0x1_0000);
        assert!(bytes == vec![0x01, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn decode_client_choice_auth_no_authzid() {
        // selected 0x01, max size 0x1000, no authzid
        let bytes = [0x01u8, 0x00, 0x10, 0x00];
        let choice = decode_choice(&bytes).unwrap();
        assert!(choice.selected == SecurityLayer::AUTH);
        assert!(choice.max_size == 0x1000);
        assert!(choice.authzid == None);
    }

    #[test]
    fn decode_client_choice_with_authzid() {
        let mut bytes = vec![0x01u8, 0x00, 0x10, 0x00];
        bytes.extend_from_slice(b"alice");
        let choice = decode_choice(&bytes).unwrap();
        assert!(choice.authzid.as_deref() == Some("alice"));
    }

    #[test]
    fn decode_rejects_non_auth_layer() {
        // client picked integrity (0x02) which we never offered
        let bytes = [0x02u8, 0x00, 0x10, 0x00];
        assert!(decode_choice(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_short_message() {
        assert!(decode_choice(&[0x01u8, 0x00]).is_err());
    }
}
