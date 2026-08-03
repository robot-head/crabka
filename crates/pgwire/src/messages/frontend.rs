//! Decoding of frontend (client → server) messages.
//!
//! All decode functions return `Ok(None)` when the buffer does not yet hold a
//! complete message, and never panic on malformed input.

use bytes::{Buf, Bytes, BytesMut};

use crate::error::PgError;

pub const PROTOCOL_3_0: i32 = 0x0003_0000; // 196608
pub const SSL_REQUEST_CODE: i32 = 80_877_103;
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;

const SPECIAL_REQUEST_LEN: usize = 8;
const CANCEL_REQUEST_LEN: usize = 16;

/// Matches real Postgres startup packet length cap (`MAX_STARTUP_PACKET_LENGTH`).
pub const MAX_STARTUP_PACKET_LEN: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    Startup { params: Vec<(String, String)> },
    SslRequest,
    GssEncRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
}

/// Decode one startup packet from `buf` when enough bytes are available.
///
/// # Errors
///
/// Returns a protocol error for malformed lengths, unsupported protocol
/// versions, or invalid startup fields.
///
/// # Panics
///
/// Panics only on a platform where a positive protocol `i32` length cannot be
/// represented as `usize`.
pub fn decode_startup(buf: &mut BytesMut) -> Result<Option<StartupPacket>, PgError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if len < 8 {
        return Err(PgError::protocol(format!(
            "invalid startup packet length: {len}"
        )));
    }
    let len = usize::try_from(len).expect("positive startup packet length fits in usize");
    if len > MAX_STARTUP_PACKET_LEN {
        return Err(PgError::protocol(format!(
            "invalid startup packet length: {len}"
        )));
    }
    if buf.len() < len {
        return Ok(None);
    }
    let mut body = buf.split_to(len).freeze();
    body.advance(4); // length field
    let code = get_i32(&mut body)?;
    match code {
        SSL_REQUEST_CODE => {
            require_startup_len(len, SPECIAL_REQUEST_LEN, "SSLRequest")?;
            Ok(Some(StartupPacket::SslRequest))
        }
        GSSENC_REQUEST_CODE => {
            require_startup_len(len, SPECIAL_REQUEST_LEN, "GSSENCRequest")?;
            Ok(Some(StartupPacket::GssEncRequest))
        }
        CANCEL_REQUEST_CODE => {
            require_startup_len(len, CANCEL_REQUEST_LEN, "CancelRequest")?;
            Ok(Some(StartupPacket::CancelRequest {
                process_id: get_i32(&mut body)?,
                secret_key: get_i32(&mut body)?,
            }))
        }
        PROTOCOL_3_0 => {
            // Bounded by MAX_STARTUP_PACKET_LEN, like real PostgreSQL — no per-pair cap needed.
            let mut params = Vec::new();
            loop {
                let key = get_cstr(&mut body)?;
                if key.is_empty() {
                    break;
                }
                let value = get_cstr(&mut body)?;
                params.push((key, value));
            }
            Ok(Some(StartupPacket::Startup { params }))
        }
        other => Err(PgError::protocol(format!(
            "unsupported frontend protocol {}.{}; server supports 3.0",
            other.cast_unsigned() >> 16,
            other.cast_unsigned() & 0xffff,
        ))),
    }
}

fn require_startup_len(actual: usize, expected: usize, packet_name: &str) -> Result<(), PgError> {
    if actual == expected {
        return Ok(());
    }

    Err(PgError::protocol(format!(
        "invalid {packet_name} length: expected {expected}, got {actual}"
    )))
}

/// Cap matching Postgres `PQ_LARGE_MESSAGE_LIMIT` order of magnitude.
pub const MAX_MESSAGE_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    Query {
        sql: String,
    },
    /// 'p' carries password / `SASLInitialResponse` / `SASLResponse` depending on
    /// auth state; the session layer interprets the raw body.
    Password(Bytes),
    Parse {
        name: String,
        sql: String,
        param_types: Vec<u32>,
    },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Bytes>>,
        result_formats: Vec<i16>,
    },
    /// `kind` is NOT validated here ('S'/'P' expected); the session maps invalid kinds to 08P01.
    Describe {
        kind: u8,
        name: String,
    },
    Execute {
        portal: String,
        max_rows: i32,
    },
    /// `kind` is NOT validated here ('S'/'P' expected); the session maps invalid kinds to 08P01.
    Close {
        kind: u8,
        name: String,
    },
    Sync,
    Flush,
    CopyData(Bytes),
    CopyDone,
    CopyFail(String),
    /// Legacy fastpath function call. The wire layer rejects these after
    /// consuming the complete frame so the connection stays synchronized.
    FunctionCall,
    Terminate,
}

/// Decode one regular frontend message from `buf` when complete.
///
/// # Errors
///
/// Returns a protocol error for malformed lengths, tags, fields, or payloads.
///
/// # Panics
///
/// Panics only on a platform where a positive protocol `i32` length cannot be
/// represented as `usize`.
pub fn decode_message(buf: &mut BytesMut) -> Result<Option<FrontendMessage>, PgError> {
    decode_message_with_max_len(buf, MAX_MESSAGE_LEN)
}

/// Decode one regular frontend message with an explicit length cap.
///
/// # Errors
/// Returns a protocol error for malformed lengths, tags, fields, payloads, or
/// messages exceeding `max_message_len`.
///
/// # Panics
/// Panics only on a platform where a positive protocol `i32` length cannot be
/// represented as `usize`.
pub fn decode_message_with_max_len(
    buf: &mut BytesMut,
    max_message_len: usize,
) -> Result<Option<FrontendMessage>, PgError> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let tag = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len < 4 {
        return Err(PgError::protocol(format!(
            "invalid message length {len} for tag {}",
            tag as char
        )));
    }
    let len = usize::try_from(len).expect("positive message length fits in usize");
    if len > max_message_len {
        return Err(PgError::protocol(format!(
            "invalid message length {len} for tag {}",
            tag as char
        )));
    }
    let total = 1 + len;
    if buf.len() < total {
        return Ok(None);
    }
    let mut body = buf.split_to(total).freeze();
    body.advance(5); // tag + length

    let msg = match tag {
        b'Q' => FrontendMessage::Query {
            sql: get_cstr(&mut body)?,
        },
        b'X' => FrontendMessage::Terminate,
        b'S' => FrontendMessage::Sync,
        b'H' => FrontendMessage::Flush,
        b'd' => {
            let payload = body.clone();
            body.advance(body.len());
            FrontendMessage::CopyData(payload)
        }
        b'c' => FrontendMessage::CopyDone,
        b'f' => FrontendMessage::CopyFail(get_cstr(&mut body)?),
        b'F' => {
            body.advance(body.len());
            FrontendMessage::FunctionCall
        }
        b'p' => {
            let payload = body.clone();
            body.advance(body.len());
            FrontendMessage::Password(payload)
        }
        b'P' => {
            let name = get_cstr(&mut body)?;
            let sql = get_cstr(&mut body)?;
            let n = get_i16(&mut body)?;
            let n =
                usize::try_from(n).map_err(|_| PgError::protocol("negative count in message"))?;
            let mut param_types = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                param_types.push(get_i32(&mut body)?.cast_unsigned());
            }
            FrontendMessage::Parse {
                name,
                sql,
                param_types,
            }
        }
        b'B' => {
            let portal = get_cstr(&mut body)?;
            let statement = get_cstr(&mut body)?;
            let param_formats = decode_i16_vec(&mut body)?;
            let nparams = get_i16(&mut body)?;
            let nparams = usize::try_from(nparams)
                .map_err(|_| PgError::protocol("negative count in message"))?;
            let mut params = Vec::with_capacity(nparams.min(1024));
            for _ in 0..nparams {
                let len = get_i32(&mut body)?;
                if len < 0 {
                    params.push(None);
                } else {
                    params.push(Some(get_bytes(
                        &mut body,
                        usize::try_from(len).expect("non-negative parameter length fits in usize"),
                    )?));
                }
            }
            let result_formats = decode_i16_vec(&mut body)?;
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            }
        }
        b'D' => FrontendMessage::Describe {
            kind: get_u8(&mut body)?,
            name: get_cstr(&mut body)?,
        },
        b'E' => FrontendMessage::Execute {
            portal: get_cstr(&mut body)?,
            max_rows: get_i32(&mut body)?,
        },
        b'C' => FrontendMessage::Close {
            kind: get_u8(&mut body)?,
            name: get_cstr(&mut body)?,
        },
        other => {
            return Err(PgError::protocol(format!(
                "unknown frontend message tag {:?}",
                other as char
            )));
        }
    };
    if !body.is_empty() {
        return Err(PgError::protocol(format!(
            "invalid message format: {} trailing bytes after '{}' message",
            body.len(),
            tag as char
        )));
    }
    Ok(Some(msg))
}

fn decode_i16_vec(body: &mut Bytes) -> Result<Vec<i16>, PgError> {
    let n = get_i16(body)?;
    let n = usize::try_from(n).map_err(|_| PgError::protocol("negative count in message"))?;
    let mut out = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        out.push(get_i16(body)?);
    }
    Ok(out)
}

// ---- checked readers (Bytes::get_* panic on underflow; never call those) ----

pub(crate) fn get_i32(buf: &mut Bytes) -> Result<i32, PgError> {
    if buf.len() < 4 {
        return Err(PgError::protocol("message truncated reading i32"));
    }
    Ok(buf.get_i32())
}

pub(crate) fn get_i16(buf: &mut Bytes) -> Result<i16, PgError> {
    if buf.len() < 2 {
        return Err(PgError::protocol("message truncated reading i16"));
    }
    Ok(buf.get_i16())
}

pub(crate) fn get_u8(buf: &mut Bytes) -> Result<u8, PgError> {
    if buf.is_empty() {
        return Err(PgError::protocol("message truncated reading byte"));
    }
    Ok(buf.get_u8())
}

pub(crate) fn get_bytes(buf: &mut Bytes, n: usize) -> Result<Bytes, PgError> {
    if buf.len() < n {
        return Err(PgError::protocol("message truncated reading bytes"));
    }
    Ok(buf.split_to(n))
}

pub(crate) fn get_cstr(buf: &mut Bytes) -> Result<String, PgError> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| PgError::protocol("unterminated string"))?;
    let raw = buf.split_to(pos);
    buf.advance(1); // NUL
    String::from_utf8(raw.to_vec()).map_err(|_| PgError::protocol("string is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use super::*;

    fn frame_len(len: usize) -> i32 {
        i32::try_from(len).expect("test frame length fits in i32") + 4
    }

    fn tagged(tag: u8, body: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(tag);
        buf.put_i32(frame_len(body.len()));
        buf.put_slice(body);
        buf
    }

    #[test]
    fn decodes_query() {
        let mut buf = tagged(b'Q', b"SELECT 1\0");
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            msg,
            FrontendMessage::Query {
                sql: "SELECT 1".into()
            }
        );
    }

    #[test]
    fn decodes_terminate_sync_flush() {
        for (tag, want) in [
            (b'X', FrontendMessage::Terminate),
            (b'S', FrontendMessage::Sync),
            (b'H', FrontendMessage::Flush),
        ] {
            let mut buf = tagged(tag, b"");
            assert_eq!(
                decode_message(&mut buf).expect("ok").expect("complete"),
                want
            );
        }
    }

    #[test]
    fn decodes_parse_with_param_types() {
        let mut body = BytesMut::new();
        body.put_slice(b"stmt1\0SELECT $1\0");
        body.put_i16(1);
        body.put_i32(23); // int4 oid
        let mut buf = tagged(b'P', &body);
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            msg,
            FrontendMessage::Parse {
                name: "stmt1".into(),
                sql: "SELECT $1".into(),
                param_types: vec![23],
            }
        );
    }

    #[test]
    fn decodes_bind() {
        let mut body = BytesMut::new();
        body.put_slice(b"portal1\0stmt1\0");
        body.put_i16(1); // one param format code
        body.put_i16(0); // text
        body.put_i16(2); // two params
        body.put_i32(2);
        body.put_slice(b"42");
        body.put_i32(-1); // NULL param
        body.put_i16(1); // one result format code
        body.put_i16(1); // binary
        let mut buf = tagged(b'B', &body);
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            msg,
            FrontendMessage::Bind {
                portal: "portal1".into(),
                statement: "stmt1".into(),
                param_formats: vec![0],
                params: vec![Some(Bytes::from_static(b"42")), None],
                result_formats: vec![1],
            }
        );
    }

    #[test]
    fn decodes_describe_execute_close() {
        let mut buf = tagged(b'D', b"Sstmt1\0");
        assert_eq!(
            decode_message(&mut buf).expect("ok").expect("complete"),
            FrontendMessage::Describe {
                kind: b'S',
                name: "stmt1".into()
            }
        );

        let mut body = BytesMut::new();
        body.put_slice(b"portal1\0");
        body.put_i32(0);
        let mut buf = tagged(b'E', &body);
        assert_eq!(
            decode_message(&mut buf).expect("ok").expect("complete"),
            FrontendMessage::Execute {
                portal: "portal1".into(),
                max_rows: 0
            }
        );

        let mut buf = tagged(b'C', b"P\0");
        assert_eq!(
            decode_message(&mut buf).expect("ok").expect("complete"),
            FrontendMessage::Close {
                kind: b'P',
                name: String::new()
            }
        );
    }

    #[test]
    fn decodes_password_message_raw() {
        let mut buf = tagged(b'p', b"SCRAM-SHA-256\0\0\0\0\x05hello");
        let msg = decode_message(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            msg,
            FrontendMessage::Password(Bytes::from_static(b"SCRAM-SHA-256\0\0\0\0\x05hello"))
        );
    }

    #[test]
    fn partial_message_returns_none_and_keeps_buffer() {
        let full = tagged(b'Q', b"SELECT 1\0");
        let mut partial = BytesMut::from(&full[..4]);
        assert_eq!(decode_message(&mut partial).expect("ok"), None);
        assert_eq!(partial.len(), 4, "no bytes consumed");
    }

    #[test]
    fn configured_message_limit_is_enforced() {
        let mut buf = tagged(b'Q', b"SELECT 1\0");
        assert!(decode_message_with_max_len(&mut buf, 4).is_err());
    }

    #[test]
    fn unknown_tag_is_error() {
        let mut buf = tagged(b'?', b"");
        assert!(decode_message(&mut buf).is_err());
    }

    #[test]
    fn oversized_length_is_error_not_panic() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32(i32::MAX);
        assert!(decode_message(&mut buf).is_err());
    }

    fn startup_bytes(params: &[(&str, &str)]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(PROTOCOL_3_0);
        for (k, v) in params {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0); // terminator
        let mut buf = BytesMut::new();
        buf.put_i32(frame_len(body.len()));
        buf.put_slice(&body);
        buf
    }

    #[test]
    fn decodes_startup_with_params() {
        let mut buf = startup_bytes(&[("user", "crab"), ("database", "crab")]);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            pkt,
            StartupPacket::Startup {
                params: vec![
                    ("user".into(), "crab".into()),
                    ("database".into(), "crab".into()),
                ]
            }
        );
        assert!(buf.is_empty(), "packet bytes fully consumed");
    }

    #[test]
    fn incomplete_packet_returns_none() {
        let full = startup_bytes(&[("user", "crab")]);
        let mut partial = BytesMut::from(&full[..full.len() - 3]);
        assert_eq!(decode_startup(&mut partial).expect("ok"), None);
    }

    #[test]
    fn decodes_ssl_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(SSL_REQUEST_CODE);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(pkt, StartupPacket::SslRequest);
    }

    #[test]
    fn rejects_ssl_request_with_trailing_bytes() {
        let mut buf = BytesMut::new();
        buf.put_i32(12);
        buf.put_i32(SSL_REQUEST_CODE);
        buf.put_i32(0);
        let err = decode_startup(&mut buf).expect_err("SSLRequest length is fixed");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[test]
    fn rejects_gss_request_with_trailing_bytes() {
        let mut buf = BytesMut::new();
        buf.put_i32(12);
        buf.put_i32(GSSENC_REQUEST_CODE);
        buf.put_i32(0);
        let err = decode_startup(&mut buf).expect_err("GSSENCRequest length is fixed");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[test]
    fn decodes_cancel_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(CANCEL_REQUEST_CODE);
        buf.put_i32(4242);
        buf.put_i32(-12345);
        let pkt = decode_startup(&mut buf).expect("ok").expect("complete");
        assert_eq!(
            pkt,
            StartupPacket::CancelRequest {
                process_id: 4242,
                secret_key: -12345
            }
        );
    }

    #[test]
    fn rejects_cancel_request_with_trailing_bytes() {
        let mut buf = BytesMut::new();
        buf.put_i32(20);
        buf.put_i32(CANCEL_REQUEST_CODE);
        buf.put_i32(4242);
        buf.put_i32(-12345);
        buf.put_i32(0);
        let err = decode_startup(&mut buf).expect_err("CancelRequest length is fixed");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[test]
    fn unknown_protocol_version_is_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(9);
        buf.put_i32(0x0002_0000); // protocol 2.0
        buf.put_u8(0);
        let err = decode_startup(&mut buf).expect_err("must reject");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[test]
    fn trailing_bytes_after_message_are_rejected() {
        let mut buf = tagged(b'Q', b"SELECT 1\0junk");
        let err = decode_message(&mut buf).expect_err("trailing bytes must be rejected");
        assert_eq!(err.code, crate::error::sqlstate::PROTOCOL_VIOLATION);
    }

    #[test]
    fn absurd_length_is_error_not_panic() {
        let mut buf = BytesMut::new();
        buf.put_i32(i32::MAX);
        buf.put_i32(PROTOCOL_3_0);
        assert!(decode_startup(&mut buf).is_err());
    }

    #[test]
    fn length_field_present_body_absent_returns_none() {
        let full = startup_bytes(&[("user", "crab")]);
        // Keep exactly 4 bytes: length field only.
        let mut partial = BytesMut::from(&full[..4]);
        assert_eq!(decode_startup(&mut partial).expect("ok"), None);
        // Buffer must be untouched (no split_to happened).
        assert_eq!(partial.len(), 4);
    }
}

#[cfg(test)]
mod proptests {
    use bytes::BytesMut;
    use proptest::prelude::*;

    use super::*;

    fn frame_len(len: usize) -> i32 {
        i32::try_from(len).expect("test frame length fits in i32") + 4
    }

    proptest! {
        #[test]
        fn decode_message_never_panics(data: Vec<u8>) {
            let mut buf = BytesMut::from(&data[..]);
            let _ = decode_message(&mut buf);
        }

        #[test]
        fn decode_startup_never_panics(data: Vec<u8>) {
            let mut buf = BytesMut::from(&data[..]);
            let _ = decode_startup(&mut buf);
        }
    }

    /// Builds a structurally valid Bind message body — the deepest parse path.
    fn valid_bind_frame(nparams: u16, param_len: u16) -> Vec<u8> {
        use bytes::BufMut;
        let mut body = bytes::BytesMut::new();
        body.put_slice(b"portal\0stmt\0");
        body.put_i16(1);
        body.put_i16(0);
        body.put_i16(nparams.cast_signed());
        for _ in 0..nparams {
            body.put_i32(i32::from(param_len));
            body.put_slice(&vec![0x42u8; usize::from(param_len)]);
        }
        body.put_i16(1);
        body.put_i16(1);
        let mut frame = Vec::with_capacity(body.len() + 5);
        frame.push(b'B');
        frame.extend_from_slice(&frame_len(body.len()).to_be_bytes());
        frame.extend_from_slice(&body);
        frame
    }

    proptest! {
        #[test]
        fn mutated_valid_frames_never_panic(
            nparams in 0u16..64,
            param_len in 0u16..64,
            mutate_at in 0usize..256,
            mutate_to: u8,
        ) {
            let mut frame = valid_bind_frame(nparams, param_len);
            let idx = mutate_at % frame.len();
            frame[idx] = mutate_to;
            let mut buf = BytesMut::from(&frame[..]);
            let _ = decode_message(&mut buf);
        }

        #[test]
        fn valid_bind_frames_roundtrip(nparams in 0u16..16, param_len in 0u16..32) {
            let frame = valid_bind_frame(nparams, param_len);
            let mut buf = BytesMut::from(&frame[..]);
            let msg = decode_message(&mut buf).expect("valid frame decodes").expect("complete");
            let bind_len = if let FrontendMessage::Bind { ref params, .. } = msg { Some(params.len()) } else { None };
            prop_assert_eq!(bind_len, Some(usize::from(nparams)));
        }
    }
}
