//! Wire-format isolation for state-topic records. Today this is
//! `serde_json::to_vec` over `InFlightFile`; swapping to bincode
//! or protobuf is a one-function change behind these helpers.

use bytes::Bytes;

use crate::executor::state::InFlightFile;
use crate::state_topic::error::StateTopicError;

pub(crate) fn encode(f: &InFlightFile) -> Result<Bytes, StateTopicError> {
    let v = serde_json::to_vec(f)?;
    Ok(Bytes::from(v))
}

pub(crate) fn decode(bytes: &[u8]) -> Result<InFlightFile, StateTopicError> {
    let f: InFlightFile = serde_json::from_slice(bytes)?;
    Ok(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::state::Phase;
    use assert2::assert;

    #[test]
    fn round_trip_preserves_all_fields() {
        let f = InFlightFile::new("p-abc".into(), Phase::Wait, 1234, 50_000_000);
        let bytes = encode(&f).unwrap();
        let back = decode(&bytes).unwrap();
        assert!(back == f);
    }

    #[test]
    fn decode_rejects_malformed_json() {
        let err = decode(b"{not json").unwrap_err();
        assert!(matches!(err, StateTopicError::MalformedJson(_)));
    }
}
