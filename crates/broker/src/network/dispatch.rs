//! Per-connection request loop. Reads a frame, parses the request
//! header, looks up the handler, awaits the response, encodes the
//! response header in front of the handler's bytes, and writes the
//! result back to the client.
//!
//! Header rules (verified against Apache Kafka 4.x):
//! - Request header is v2 when the body is flexible (KIP-482), v1 otherwise.
//!   Note: `client_id` is `NULLABLE_STRING` (i16 length) in BOTH header
//!   versions — see `RequestHeader.json` schema (`flexibleVersions: none`
//!   on the field).
//! - Response header is v1 (i.e. a trailing tagged-fields byte) iff the
//!   *body* is flexible — EXCEPT for `ApiVersions` (`api_key=18`), whose
//!   response header is always v0.

#![allow(dead_code)] // accept loop wires this up in Phase D (Task 11).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::network::codec::{self, MAX_FRAME_BYTES};

const API_VERSIONS_KEY: i16 = 18;

/// Run the connection's read/dispatch/write loop until the peer disconnects.
pub async fn serve_connection(broker: std::sync::Arc<Broker>, stream: TcpStream) {
    let peer = stream
        .peer_addr()
        .map_or_else(|_| "<unknown>".to_string(), |a| a.to_string());
    let mut framed: Framed<TcpStream, _> = codec::frame(stream);
    tracing::info!(%peer, "connection opened");

    while let Some(frame) = framed.next().await {
        let frame = match frame {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%peer, error = %e, "frame decode error, closing");
                break;
            }
        };
        let response_bytes = match dispatch_one(&broker, &frame).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%peer, error = %e, "dispatch error, closing connection");
                break;
            }
        };
        if let Err(e) = framed.send(response_bytes).await {
            tracing::warn!(%peer, error = %e, "framed.send error, closing");
            break;
        }
    }
    tracing::info!(%peer, "connection closed");
}

/// Decode one request from the framed bytes, call the handler, build a
/// response with the right `ResponseHeader` version, return the bytes
/// ready for `framed.send` (which prepends the i32 length).
///
/// Errors here close the connection — they're protocol violations.
async fn dispatch_one(broker: &Broker, frame: &[u8]) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    let body_flexible = handler_body_flexible(api_key, api_version);
    tracing::info!(
        api_key,
        api_version,
        correlation_id,
        body_flexible,
        body_len = body.len(),
        "dispatching request"
    );

    let handler = broker
        .handlers()
        .get(api_key)
        .ok_or(BrokerError::UnsupportedApi {
            api_key,
            version: api_version,
        });

    let resp_body: Bytes = if let Ok(h) = handler {
        h(broker, api_version, correlation_id, body).await?
    } else {
        tracing::warn!(api_key, api_version, "unsupported api, returning error");
        // Build a synthetic UNSUPPORTED_VERSION response: just a 2-byte
        // error code + an empty body. Most Kafka responses begin with
        // `error_code: i16` at offset 0; clients that don't expect
        // this for some api_keys will close anyway.
        let mut buf = BytesMut::with_capacity(2);
        buf.put_i16(codes::UNSUPPORTED_VERSION);
        buf.freeze()
    };

    let out = encode_response(api_key, correlation_id, body_flexible, &resp_body);
    tracing::info!(
        api_key,
        api_version,
        correlation_id,
        resp_len = out.len(),
        "response built"
    );
    Ok(out)
}

/// Parse `RequestHeader` and return `(api_key, version, corr_id, &body)`.
fn parse_request_header(frame: &[u8]) -> Result<(i16, i16, i32, &[u8]), BrokerError> {
    if frame.len() < 8 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("request frame < 8 bytes"),
        ));
    }
    let mut cur = frame;
    let api_key = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();

    let body_flexible = handler_body_flexible(api_key, api_version);
    let header_v2 = body_flexible;

    // client_id: NULLABLE_STRING (i16 length) in BOTH header versions.
    if cur.remaining() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("request frame: missing client_id length"),
        ));
    }
    let cid_len = cur.get_i16();
    if cid_len > 0 {
        let n = usize::try_from(cid_len).expect("non-negative i16 fits usize");
        if cur.remaining() < n {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue(
                    "request frame: client_id length > available",
                ),
            ));
        }
        cur.advance(n);
    }
    if header_v2 {
        if cur.remaining() < 1 {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue(
                    "request frame: missing header tagged-fields byte",
                ),
            ));
        }
        // For the MVP we don't surface unknown header-level tagged fields.
        // Consume one UVARINT = 0 (empty). If non-zero, log + ignore.
        let tagged = cur.get_u8();
        if tagged != 0 {
            tracing::debug!(
                api_key,
                api_version,
                "non-empty header tagged fields ignored"
            );
        }
    }
    Ok((api_key, api_version, correlation_id, cur))
}

/// Returns whether the request *body* (and therefore the response body)
/// is flexible for this `(api_key, version)`. Mirrors
/// `crabka_protocol::owned::*::FLEXIBLE_MIN`.
///
/// For the handful of APIs the MVP supports, this is a small static table;
/// keep it next to the handler registry so adding a new handler updates one
/// place.
fn handler_body_flexible(api_key: i16, version: i16) -> bool {
    use crabka_protocol::owned;
    match api_key {
        0 => version >= owned::produce_request::FLEXIBLE_MIN,
        1 => version >= owned::fetch_request::FLEXIBLE_MIN,
        2 => version >= owned::list_offsets_request::FLEXIBLE_MIN,
        3 => version >= owned::metadata_request::FLEXIBLE_MIN,
        8 => version >= owned::offset_commit_request::FLEXIBLE_MIN,
        9 => version >= owned::offset_fetch_request::FLEXIBLE_MIN,
        10 => version >= owned::find_coordinator_request::FLEXIBLE_MIN,
        11 => version >= owned::join_group_request::FLEXIBLE_MIN,
        12 => version >= owned::heartbeat_request::FLEXIBLE_MIN,
        13 => version >= owned::leave_group_request::FLEXIBLE_MIN,
        14 => version >= owned::sync_group_request::FLEXIBLE_MIN,
        18 => version >= owned::api_versions_request::FLEXIBLE_MIN,
        19 => version >= owned::create_topics_request::FLEXIBLE_MIN,
        20 => version >= owned::delete_topics_request::FLEXIBLE_MIN,
        22 => version >= owned::init_producer_id_request::FLEXIBLE_MIN,
        23 => version >= owned::offset_for_leader_epoch_request::FLEXIBLE_MIN,
        24 => version >= owned::add_partitions_to_txn_request::FLEXIBLE_MIN,
        25 => version >= owned::add_offsets_to_txn_request::FLEXIBLE_MIN,
        26 => version >= owned::end_txn_request::FLEXIBLE_MIN,
        27 => version >= owned::write_txn_markers_request::FLEXIBLE_MIN,
        28 => version >= owned::txn_offset_commit_request::FLEXIBLE_MIN,
        32 => version >= owned::describe_configs_request::FLEXIBLE_MIN,
        _ => false,
    }
}

/// Prepend the response header (`corr_id` + optional tagged-fields byte)
/// in front of the handler's body bytes.
fn encode_response(api_key: i16, correlation_id: i32, body_flexible: bool, body: &[u8]) -> Bytes {
    let header_v1 = body_flexible && api_key != API_VERSIONS_KEY;
    let header_len = if header_v1 { 5 } else { 4 };
    debug_assert!(body.len() < MAX_FRAME_BYTES);
    let mut buf = BytesMut::with_capacity(header_len + body.len());
    buf.put_i32(correlation_id);
    if header_v1 {
        buf.put_u8(0); // empty tagged fields
    }
    buf.put_slice(body);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_header_v1_no_flexible() {
        // api_key=3, version=8 (non-flexible), corr_id=42, client_id="hi"
        let mut buf = BytesMut::new();
        buf.put_i16(3);
        buf.put_i16(8);
        buf.put_i32(42);
        buf.put_i16(2);
        buf.put_slice(b"hi");
        let (k, v, c, body) = parse_request_header(&buf).unwrap();
        assert_eq!((k, v, c, body.len()), (3, 8, 42, 0));
    }

    #[test]
    fn parse_header_v2_with_tagged_byte() {
        // api_key=18 (ApiVersions), version=3 (flexible), corr_id=1, client_id="x"
        let mut buf = BytesMut::new();
        buf.put_i16(18);
        buf.put_i16(3);
        buf.put_i32(1);
        buf.put_i16(1);
        buf.put_slice(b"x");
        buf.put_u8(0); // tagged-fields byte
        let (k, v, c, body) = parse_request_header(&buf).unwrap();
        assert_eq!((k, v, c, body.len()), (18, 3, 1, 0));
    }

    #[test]
    fn encode_response_apiversions_uses_v0_header() {
        // ApiVersions response is always header v0 (no tagged byte) even
        // for flexible body versions.
        let body = [0u8, 0u8]; // error_code=0
        let out = encode_response(API_VERSIONS_KEY, 7, true, &body);
        // 4 byte corr_id + body, no tagged byte.
        assert_eq!(out.len(), 4 + body.len());
    }

    #[test]
    fn encode_response_other_flexible_inserts_tagged_byte() {
        let body = [0u8, 0u8];
        let out = encode_response(3, 7, true, &body);
        assert_eq!(out.len(), 5 + body.len());
        assert_eq!(out[4], 0); // tagged byte
    }
}
