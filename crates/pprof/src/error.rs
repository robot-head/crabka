//! Profiles engine error type.

/// Errors across profiles decode, planning, execution, storage, and symbolization.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("decode: {0}")]
    Decode(String),
    #[error("plan: {0}")]
    Plan(String),
    #[error("exec: {0}")]
    Exec(String),
    #[error("store: {0}")]
    Store(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("symbolize: {0}")]
    Symbolize(String),
}

impl From<prost::DecodeError> for ProfileError {
    fn from(error: prost::DecodeError) -> Self {
        Self::Decode(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use prost::Message;

    use super::*;

    #[test]
    fn error_display_includes_message() {
        let error = ProfileError::Decode("bad pprof".to_string());
        assert!(format!("{error}").contains("bad pprof"));
    }

    #[test]
    fn prost_decode_maps_to_decode_error() {
        let error = crate::proto::Profile::decode(&[0xff][..])
            .unwrap_err()
            .into();
        assert!(matches!(error, ProfileError::Decode(_)));
    }
}
