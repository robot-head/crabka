//! Wire-format isolation for state-topic records. Today this is
//! `serde_json::to_vec` over `InFlightFile`; swapping to bincode
//! or protobuf is a one-function change behind these helpers.

use bytes::Bytes;

use crate::executor::state::InFlightFile;
use crate::state_topic::error::StateTopicError;

#[allow(dead_code)]
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

    #[test]
    fn round_trip_preserves_all_fields() {
        let f = InFlightFile::new("p-abc".into(), Phase::Wait, 1234, 50_000_000);
        let bytes = encode(&f).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.proposal_id, f.proposal_id);
        assert_eq!(back.phase, f.phase);
        assert_eq!(back.started_at_ms, f.started_at_ms);
        assert_eq!(back.throttle_bytes_per_sec, f.throttle_bytes_per_sec);
        assert_eq!(back.version, f.version);
    }

    #[test]
    fn decode_rejects_malformed_json() {
        let err = decode(b"{not json").unwrap_err();
        assert!(matches!(err, StateTopicError::MalformedJson(_)));
    }
}
