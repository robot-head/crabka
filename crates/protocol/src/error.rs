use thiserror::Error;

/// Errors that can occur during wire-protocol encoding or decoding.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Decode reached end of buffer before the expected number of bytes.
    #[error("unexpected end of buffer: needed {needed} more bytes")]
    UnexpectedEof { needed: usize },

    /// Decoded a value that the schema says is impossible (e.g. negative array length).
    #[error("invalid value: {0}")]
    InvalidValue(&'static str),

    /// Decoded UTF-8 bytes that are not valid UTF-8.
    #[error("invalid UTF-8 in string field")]
    InvalidUtf8(#[source] std::str::Utf8Error),

    /// Decoded a varint that exceeds the maximum legal length.
    #[error("varint exceeds {max} bytes")]
    VarintTooLong { max: usize },

    /// Encountered an unknown API version for a known API key.
    #[error("unsupported API version {version} for api key {api_key}")]
    UnsupportedVersion { api_key: i16, version: i16 },

    /// Schema version requested is not within the message's supported range.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_useful() {
        let e = ProtocolError::UnexpectedEof { needed: 4 };
        assert_eq!(e.to_string(), "unexpected end of buffer: needed 4 more bytes");
    }
}
