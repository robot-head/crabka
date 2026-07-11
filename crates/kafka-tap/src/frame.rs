//! Kafka wire framing + request/response correlation. No schema knowledge.

use std::collections::HashMap;

use crabka_ids::{ApiKey, ApiVersion};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RequestPrefix {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: i32,
}

/// Parse the fixed request-header prefix common to every request type:
/// `api_key: i16, api_version: i16, correlation_id: i32`.
#[must_use]
pub fn parse_request_prefix(body: &[u8]) -> Option<RequestPrefix> {
    if body.len() < 8 {
        return None;
    }
    Some(RequestPrefix {
        api_key: ApiKey(i16::from_be_bytes([body[0], body[1]])),
        api_version: ApiVersion(i16::from_be_bytes([body[2], body[3]])),
        correlation_id: i32::from_be_bytes([body[4], body[5], body[6], body[7]]),
    })
}

/// Every response body begins with `correlation_id: i32`, before any tagged
/// header — true for flexible and non-flexible responses alike.
#[must_use]
pub fn read_correlation_id(body: &[u8]) -> Option<i32> {
    if body.len() < 4 {
        return None;
    }
    Some(i32::from_be_bytes([body[0], body[1], body[2], body[3]]))
}

/// Per-connection map from correlation id to the (api_key, api_version) of the
/// request that bore it, so responses can be classified.
#[derive(Default)]
pub struct Pending {
    map: HashMap<i32, (ApiKey, ApiVersion)>,
}

impl Pending {
    pub fn record(&mut self, correlation_id: i32, api_key: ApiKey, api_version: ApiVersion) {
        self.map.insert(correlation_id, (api_key, api_version));
    }
    #[must_use]
    pub fn take(&mut self, correlation_id: i32) -> Option<(ApiKey, ApiVersion)> {
        self.map.remove(&correlation_id)
    }
}

/// One captured frame, emitted by the relay to the recorder spool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedFrame {
    pub api_key: ApiKey,
    pub version: ApiVersion,
    pub is_request: bool,
    /// Full frame body, excluding the 4-byte length prefix (header + message).
    pub body: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_frame(api_key: i16, api_version: i16, corr: i32, payload: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&api_key.to_be_bytes());
        b.extend_from_slice(&api_version.to_be_bytes());
        b.extend_from_slice(&corr.to_be_bytes());
        b.extend_from_slice(payload);
        b
    }

    #[test]
    fn parses_request_header_prefix() {
        let body = req_frame(18, 3, 7, &[0xaa, 0xbb]);
        let p = parse_request_prefix(&body).unwrap();
        assert2::assert!(
            p == RequestPrefix {
                api_key: ApiKey(18),
                api_version: ApiVersion(3),
                correlation_id: 7
            }
        );
    }

    #[test]
    fn correlates_response_by_id() {
        let mut pending = Pending::default();
        let body = req_frame(1, 11, 42, &[]);
        let p = parse_request_prefix(&body).unwrap();
        pending.record(p.correlation_id, p.api_key, p.api_version);
        let mut resp = Vec::new();
        resp.extend_from_slice(&42i32.to_be_bytes());
        resp.extend_from_slice(&[0x01]);
        let got = pending.take(read_correlation_id(&resp).unwrap()).unwrap();
        assert2::assert!(got == (ApiKey(1), ApiVersion(11)));
    }
}
