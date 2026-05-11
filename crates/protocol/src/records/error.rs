//! Errors specific to record-batch decoding/encoding.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RecordsError {
    #[error("buffer too short for batch header (need {needed} more bytes)")]
    HeaderTooShort { needed: usize },

    #[error("batch magic byte {found} unsupported (only v2 supported)")]
    UnsupportedMagic { found: i8 },

    #[error("CRC mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    CrcMismatch { expected: u32, computed: u32 },

    #[error("batch body truncated (need {needed} more bytes)")]
    BodyTooShort { needed: usize },

    #[error("record parse failed: {0}")]
    RecordParse(String),

    #[error("compression: {0}")]
    Compression(#[from] crabka_compression::CompressionError),

    #[error("zerocopy reinterpretation failed")]
    ZerocopyFailure,
}

impl From<RecordsError> for crate::ProtocolError {
    fn from(e: RecordsError) -> Self {
        crate::ProtocolError::InvalidValue(match e {
            RecordsError::HeaderTooShort { .. } => "records: header too short",
            RecordsError::UnsupportedMagic { .. } => "records: unsupported magic",
            RecordsError::CrcMismatch { .. } => "records: CRC mismatch",
            RecordsError::BodyTooShort { .. } => "records: body truncated",
            RecordsError::RecordParse(_) => "records: record parse failed",
            RecordsError::Compression(_) => "records: compression error",
            RecordsError::ZerocopyFailure => "records: zerocopy failure",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages() {
        let cases: &[(RecordsError, &str)] = &[
            (
                RecordsError::HeaderTooShort { needed: 4 },
                "buffer too short for batch header",
            ),
            (
                RecordsError::UnsupportedMagic { found: 1 },
                "batch magic byte 1 unsupported",
            ),
            (
                RecordsError::CrcMismatch {
                    expected: 0xDEAD_BEEF,
                    computed: 0x1234_5678,
                },
                "CRC mismatch: expected 0xdeadbeef",
            ),
            (
                RecordsError::BodyTooShort { needed: 17 },
                "batch body truncated",
            ),
            (
                RecordsError::RecordParse("bad varint".into()),
                "record parse failed",
            ),
            (
                RecordsError::ZerocopyFailure,
                "zerocopy reinterpretation failed",
            ),
        ];
        for (err, contains) in cases {
            assert!(
                err.to_string().contains(contains),
                "{err} did not contain {contains:?}",
            );
        }
    }

    #[test]
    fn into_protocol_error_is_invalid_value() {
        let e: crate::ProtocolError = RecordsError::UnsupportedMagic { found: 0 }.into();
        assert!(matches!(e, crate::ProtocolError::InvalidValue(_)));
    }
}
