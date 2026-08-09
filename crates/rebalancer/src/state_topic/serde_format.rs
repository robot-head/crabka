//! Wire-format isolation for state-topic records. Today the format is
//! `serde_json::to_vec` over `InFlightFile`. A move to bincode or protobuf is
//! a one-function change behind these helpers.

use bytes::Bytes;

use crate::{executor::state::InFlightFile, state_topic::error::StateTopicError};

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
    use crabka_units::mebibytes_per_sec;

    use super::*;
    use crate::executor::state::Phase;

    #[test]
    fn round_trip_preserves_all_fields() {
        let f = InFlightFile::new("p-abc".into(), Phase::Wait, 1234, mebibytes_per_sec(48));
        let bytes = encode(&f).unwrap();
        let back = decode(&bytes).unwrap();
        assert2::assert!(back == f);
    }

    #[test]
    fn decode_rejects_malformed_json() {
        let err = decode(b"{not json").unwrap_err();
        assert2::assert!(matches!(err, StateTopicError::MalformedJson(_)));
    }
}
