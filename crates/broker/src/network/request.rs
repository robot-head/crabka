//! Borrowed Kafka request-header parsing for the broker dispatch loop.
#![allow(dead_code)]

use bytes::Buf;
use crabka_protocol::primitives::string_bytes_borrowed::get_nullable_string_borrowed;

use crate::{
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ParsedRequest<'a> {
    pub api_key: ApiKeyCode,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub body: &'a [u8],
    pub body_flexible: bool,
    pub client_id: Option<&'a str>,
}

pub(crate) fn parse_request<F>(
    frame: &[u8],
    flexible_for: F,
) -> Result<ParsedRequest<'_>, BrokerError>
where
    F: Fn(ApiKeyCode, ApiVersion) -> bool,
{
    if frame.len() < 8 {
        return Err(protocol_invalid("request frame < 8 bytes"));
    }

    let mut cur = frame;
    let api_key = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();
    let body_flexible = flexible_for(api_key, api_version);

    let client_id = get_nullable_string_borrowed(&mut cur)?;

    if body_flexible {
        crabka_protocol::tagged_fields::read_tagged_fields(&mut cur, |_tag, _payload| Ok(false))?;
    }

    Ok(ParsedRequest {
        api_key,
        api_version,
        correlation_id,
        body: cur,
        body_flexible,
        client_id,
    })
}

pub(crate) fn peek_api_key(frame: &[u8]) -> Result<ApiKeyCode, BrokerError> {
    if frame.len() < 2 {
        return Err(protocol_invalid("request frame < 2 bytes"));
    }
    Ok(i16::from_be_bytes([frame[0], frame[1]]))
}

pub(crate) fn peek_client_id(frame: &[u8]) -> Option<&str> {
    if frame.len() < 10 {
        return None;
    }
    let cid_len = i16::from_be_bytes([frame[8], frame[9]]);
    if cid_len <= 0 {
        return None;
    }
    let n = usize::try_from(cid_len).ok()?;
    let start = 10_usize;
    let end = start.checked_add(n)?;
    if frame.len() < end {
        return None;
    }
    std::str::from_utf8(&frame[start..end]).ok()
}

fn protocol_invalid(message: &'static str) -> BrokerError {
    BrokerError::Protocol(crabka_protocol::ProtocolError::InvalidValue(message))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::{BufMut, BytesMut};

    use super::*;

    fn request_frame(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&[u8]>,
        tagged: Option<&[u8]>,
        body: &[u8],
    ) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_i16(api_key);
        buf.put_i16(api_version);
        buf.put_i32(correlation_id);
        match client_id {
            Some(id) => {
                buf.put_i16(i16::try_from(id.len()).expect("client id length"));
                buf.put_slice(id);
            }
            None => buf.put_i16(-1),
        }
        if let Some(tagged) = tagged {
            buf.put_slice(tagged);
        }
        buf.put_slice(body);
        buf
    }

    #[test]
    fn parse_request_non_flexible_header() {
        let frame = request_frame(3, 8, 42, Some(b"client-a"), None, b"body");

        let parsed = parse_request(&frame, |_, _| false).expect("parse request");

        check!(parsed.api_key == 3);
        check!(parsed.api_version == 8);
        check!(parsed.correlation_id == 42);
        check!(parsed.client_id == Some("client-a"));
        check!(!parsed.body_flexible);
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_flexible_header_consumes_tagged_fields_byte() {
        let frame = request_frame(18, 3, 7, Some(b"client-a"), Some(&[0]), b"body");

        let parsed =
            parse_request(&frame, |key, version| key == 18 && version >= 3).expect("parse request");

        check!(parsed.api_key == 18);
        check!(parsed.api_version == 3);
        check!(parsed.correlation_id == 7);
        check!(parsed.client_id == Some("client-a"));
        check!(parsed.body_flexible);
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_flexible_header_skips_non_empty_tagged_fields() {
        let frame = request_frame(
            18,
            3,
            7,
            Some(b"client-a"),
            Some(&[1, 1, 3, b't', b'a', b'g']),
            b"body",
        );

        let parsed =
            parse_request(&frame, |key, version| key == 18 && version >= 3).expect("parse request");

        check!(parsed.client_id == Some("client-a"));
        check!(parsed.body_flexible);
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_preserves_empty_client_id() {
        let frame = request_frame(3, 8, 42, Some(b""), None, b"body");

        let parsed = parse_request(&frame, |_, _| false).expect("parse request");

        check!(parsed.client_id == Some(""));
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_rejects_invalid_utf8_client_id() {
        let frame = request_frame(3, 8, 42, Some(&[0xff, 0xfe]), None, b"body");

        let err = parse_request(&frame, |_, _| false).expect_err("invalid utf8 client id");

        assert!(matches!(
            err,
            BrokerError::Protocol(crabka_protocol::ProtocolError::InvalidUtf8(_))
        ));
    }

    #[test]
    fn parse_request_rejects_truncated_headers() {
        let mut missing_client_id_len = BytesMut::new();
        missing_client_id_len.put_i16(3);
        missing_client_id_len.put_i16(8);
        missing_client_id_len.put_i32(42);

        let truncated_client_id = request_frame(3, 8, 42, Some(b"client"), None, b"");
        let flexible_without_tag = request_frame(18, 3, 42, Some(b"client"), None, b"");

        let cases = [
            ("missing fixed header", vec![0_u8; 7]),
            ("missing client id length", missing_client_id_len.to_vec()),
            (
                "truncated client id",
                truncated_client_id[..truncated_client_id.len() - 1].to_vec(),
            ),
            (
                "flexible missing tagged byte",
                flexible_without_tag.to_vec(),
            ),
        ];

        for (case, frame) in cases {
            assert!(
                parse_request(&frame, |key, version| key == 18 && version >= 3).is_err(),
                "{case}"
            );
        }
    }

    #[test]
    fn peek_helpers_match_existing_dispatch_behavior() {
        let present = request_frame(3, 8, 42, Some(b"client-a"), None, b"body");
        let null = request_frame(3, 8, 42, None, None, b"body");
        let empty = request_frame(3, 8, 42, Some(b""), None, b"body");
        let invalid = request_frame(3, 8, 42, Some(&[0xff, 0xfe]), None, b"body");

        assert!(peek_api_key(&present).expect("api key") == 3);
        assert!(peek_client_id(&present) == Some("client-a"));
        assert!(peek_client_id(&null).is_none());
        assert!(peek_client_id(&empty).is_none());
        assert!(peek_client_id(&invalid).is_none());
    }
}
