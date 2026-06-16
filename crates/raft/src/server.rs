//! Accept loop for the controller TCP listener. Receives inbound KIP-595 RPCs
//! (Fetch=1, Vote=52, BeginQuorumEpoch=53, EndQuorumEpoch=54) plus the
//! Crabka-private observer/forward RPCs and feeds them into the local
//! [`KraftController`] engine.
//!
//! Wire shape matches `crabka_client_core::Connection::raw_request`:
//!
//! - Request: `len(i32) | RequestHeader v2 (flexible) | body`
//! - Response: `len(i32) | correlation_id(i32) | tagged_fields(0u8) | body`
//!
//! `RequestHeader` v2 = `api_key(i16) api_version(i16) correlation_id(i32)
//! client_id(NULLABLE_STRING) tagged_fields(varint=0)`. We parse and discard
//! everything but `api_key`/`correlation_id` (the body is decoded by the
//! engine's transport codec / the Crabka-private wire types).

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::RaftError;
use crate::kraft::KraftController;
use crate::kraft::transport::{Inbound, api_key};
use crate::wire::{
    API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE, CrabkaMetadataFetchRequest,
    CrabkaMetadataFetchResponse, CrabkaSubmitChangeRequest, CrabkaSubmitChangeResponse,
};

/// Kafka's `ApiVersions` API key. The controller TCP listener answers this
/// because `crabka_client_core::Connection::connect` performs an `ApiVersions`
/// handshake before any other request.
const API_KEY_API_VERSIONS: i16 = 18;

/// `DescribeCluster` (KIP-919) — served on the controller listener so an
/// `AdminClient` bootstrapped with `--bootstrap-controller` can discover the
/// quorum's controller (or broker) endpoints directly from the leader.
const API_KEY_DESCRIBE_CLUSTER: i16 = 60;

pub(crate) async fn run(
    listener: TcpListener,
    engine: KraftController,
    shutdown: CancellationToken,
    handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
) {
    match listener.local_addr() {
        Ok(addr) => info!(%addr, "controller listener started"),
        Err(e) => info!(error = %e, "controller listener started (addr unknown)"),
    }
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let engine = engine.clone();
                        let shutdown = shutdown.clone();
                        let handshake = handshake.clone();
                        tokio::spawn(async move {
                            let boxed: Box<dyn crate::DuplexStream> = if let Some(hs) = handshake {
                                match hs.upgrade(stream).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::debug!(%peer, error = %e, "handshake failed");
                                        return;
                                    }
                                }
                            } else {
                                Box::new(stream) as Box<dyn crate::DuplexStream>
                            };
                            if let Err(e) = handle_conn(boxed, engine, shutdown).await {
                                error!(%peer, error = %e, "controller connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "controller listener accept failed");
                    }
                }
            }
        }
    }
}

async fn handle_conn<S>(
    mut stream: S,
    engine: KraftController,
    shutdown: CancellationToken,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            res = read_one_request(&mut stream) => {
                let (api_key_n, api_version, correlation_id, body) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        // Treat peer EOF as a clean shutdown of this conn.
                        if is_eof(&e) {
                            return Ok(());
                        }
                        return Err(e);
                    }
                };
                // ApiVersions (18) is the bootstrap handshake performed by
                // `Connection::connect`. It arrives at v0 with a header v1 (no
                // tagged-fields byte) and expects a ResponseHeader v0 reply (also
                // no tagged-fields byte) — the documented Kafka asymmetry. We
                // serialize it separately rather than poisoning the generic codec.
                if api_key_n == API_KEY_API_VERSIONS {
                    // ApiVersionsResponse always uses a v0 ResponseHeader (no
                    // tagged-fields byte), but the BODY shape depends on the
                    // request version: v0..=2 are non-flexible (i32 array), v3+
                    // are flexible (compact array). Crabka's own client asks at
                    // v0; the JVM controller asks at v4.
                    let resp = api_versions_response_body(api_version);
                    write_response_no_tagged_fields(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                // DescribeCluster (60, KIP-919) is served here rather than in
                // `dispatch` because it needs the request version (for the
                // flexible body codec) and the controller's metadata image. The
                // flexible v1 ResponseHeader is supplied by `write_response`.
                if api_key_n == API_KEY_DESCRIBE_CLUSTER {
                    let resp = describe_cluster_response_body(api_version, &body, &engine).await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                let resp = dispatch(api_key_n, body, &engine).await?;
                write_response(&mut stream, correlation_id, resp).await?;
            }
        }
    }
}

fn is_eof(e: &RaftError) -> bool {
    matches!(e,
        RaftError::Storage(crabka_log::LogError::Io(io))
            if io.kind() == std::io::ErrorKind::UnexpectedEof
    )
}

fn io_err(e: std::io::Error) -> RaftError {
    RaftError::Storage(crabka_log::LogError::Io(e))
}

fn truncated(needed: usize) -> RaftError {
    RaftError::Protocol(crabka_protocol::ProtocolError::UnexpectedEof { needed })
}

async fn read_one_request<S>(stream: &mut S) -> Result<(i16, i16, i32, Bytes), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(io_err)?;
    let raw_len = i32::from_be_bytes(len_buf);
    let len = usize::try_from(raw_len.max(0)).unwrap_or(0);
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.map_err(io_err)?;

    // RequestHeader v2 (flexible): api_key(i16), api_version(i16),
    // correlation_id(i32), client_id(NULLABLE_STRING), tagged_fields(varint=0).
    let mut cur: &[u8] = &frame;
    let fixed = 2 + 2 + 4;
    if cur.remaining() < fixed {
        return Err(truncated(fixed - cur.remaining()));
    }
    let api_key_n = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();

    // Skip client_id: NULLABLE_STRING (i16 length + bytes; -1 = null).
    if cur.remaining() < 2 {
        return Err(truncated(2 - cur.remaining()));
    }
    let cs_len = cur.get_i16();
    if cs_len > 0 {
        let n = usize::try_from(cs_len).unwrap_or(0);
        if cur.remaining() < n {
            return Err(truncated(n - cur.remaining()));
        }
        cur.advance(n);
    }
    // tagged_fields: single varint zero.
    if cur.has_remaining() && cur[0] == 0 {
        cur.advance(1);
    }

    Ok((
        api_key_n,
        api_version,
        correlation_id,
        Bytes::copy_from_slice(cur),
    ))
}

async fn write_response<S>(
    stream: &mut S,
    correlation_id: i32,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(4 + 1 + body.len());
    frame.put_i32(correlation_id);
    frame.put_u8(0); // empty tagged_fields (ResponseHeader v1)
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(io_err)?;
    stream.write_all(&frame).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    Ok(())
}

/// Write a response without the leading tagged-fields byte. Used only by the
/// `ApiVersions` v0 path, which decodes a `ResponseHeader v0`.
async fn write_response_no_tagged_fields<S>(
    stream: &mut S,
    correlation_id: i32,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(correlation_id);
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(io_err)?;
    stream.write_all(&frame).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    Ok(())
}

/// `ApiVersionsResponse` advertising the controller-listener APIs.
///
/// A real `apache/kafka:4.0.0` controller dials peers with `ApiVersions v4` over a
/// flexible (v2) request header, then consults the returned table to decide
/// which version of `Vote`/`Fetch`/etc. to send. An EMPTY `api_keys` list made
/// the JVM treat every raft RPC as `UNSUPPORTED_VERSION` and refuse to send
/// `Vote` on the wire. Advertising the KIP-595 APIs at the versions Crabka's
/// engine speaks lets compatible peers proceed to real `Vote`/`Fetch`.
///
/// Body is the flexible (v3+) `ApiVersionsResponse` shape: `error_code(i16)`,
/// `api_keys` compact-array of `{api_key(i16), min(i16), max(i16), tagged(0)}`,
/// `throttle_time_ms(i32)`, response-level `tagged(0)`. Per the documented Kafka
/// asymmetry, the *response header* stays v0 (no leading tagged-fields byte) —
/// so this is written via [`write_response_no_tagged_fields`].
fn api_versions_response_body(req_version: i16) -> Bytes {
    use crabka_protocol::Encode;
    use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
    // (api_key, min_version, max_version) — versions Crabka's transport codec
    // emits (Vote v2, Begin/End QuorumEpoch v1, Fetch v17, FetchSnapshot v1) plus
    // ApiVersions itself.
    const KEYS: &[(i16, i16, i16)] = &[
        (1, 0, 17), // Fetch
        (18, 0, 4), // ApiVersions
        (52, 0, 2), // Vote
        (53, 0, 1), // BeginQuorumEpoch
        (54, 0, 1), // EndQuorumEpoch
        (59, 0, 1), // FetchSnapshot
        (60, 0, 2), // DescribeCluster (KIP-919)
    ];
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: KEYS
            .iter()
            .map(|&(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
        throttle_time_ms: 0,
        ..Default::default()
    };
    // JVM dials at v4 (flexible); Crabka's own client at v0 (non-flexible). The
    // codec emits the correct body shape per version: req v<=2 → non-flexible
    // v0-shaped body, req v>=3 → flexible (compact) body. The v0 ApiVersions
    // response HEADER asymmetry lives in the framing (`write_response_no_tagged_fields`),
    // not here.
    let body_version = req_version.clamp(0, 4);
    let mut buf = BytesMut::new();
    let _ = resp.encode(&mut buf, body_version);
    buf.freeze()
}

/// Route an inbound RPC body to the engine and produce the response body.
///
/// The KIP-595 engine RPCs (1/52/53/54) go through [`KraftController::deliver`],
/// which decodes the body, runs the core, and replies on a oneshot with the
/// encoded response body. The Crabka-private 1003/1004 keep their bespoke
/// request/response wire types.
async fn dispatch(
    api_key_n: i16,
    body: Bytes,
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    match api_key_n {
        api_key::FETCH => {
            deliver_inbound(engine, |reply| Inbound::Fetch { req: body, reply }).await
        }
        api_key::VOTE => deliver_inbound(engine, |reply| Inbound::Vote { req: body, reply }).await,
        api_key::BEGIN_QUORUM_EPOCH => {
            deliver_inbound(engine, |reply| Inbound::BeginQuorumEpoch {
                req: body,
                reply,
            })
            .await
        }
        api_key::END_QUORUM_EPOCH => {
            deliver_inbound(engine, |reply| Inbound::EndQuorumEpoch { req: body, reply }).await
        }
        api_key::FETCH_SNAPSHOT => {
            deliver_inbound(engine, |reply| Inbound::FetchSnapshot { req: body, reply }).await
        }
        API_KEY_SUBMIT_CHANGE => dispatch_submit_change(&body, engine).await,
        API_KEY_METADATA_FETCH => dispatch_metadata_fetch(&body, engine).await,
        _ => Err(RaftError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("unknown controller api key"),
        )),
    }
}

/// Deliver an [`Inbound`] to the engine and await the encoded response body.
async fn deliver_inbound<F>(engine: &KraftController, make: F) -> Result<Bytes, RaftError>
where
    F: FnOnce(oneshot::Sender<Bytes>) -> Inbound,
{
    let (reply, rx) = oneshot::channel();
    engine.deliver(make(reply)).await?;
    rx.await.map_err(|_| RaftError::Shutdown)
}

/// Handle a follower-forwarded `submit_change` (1003). The forwarder wrapped a
/// wincode-encoded `Vec<MetadataRecord>`; we submit it to the local engine
/// (presumably the leader) and translate the result into the `error_code` enum:
/// `0` applied, `1` not leader (with `leader_hint`), `2` metadata-rejected.
async fn dispatch_submit_change(body: &[u8], engine: &KraftController) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = CrabkaSubmitChangeRequest::decode_v0(&mut cur)?;
    let records: Vec<crabka_metadata::MetadataRecord> = match <serde_wincode::SerdeCompat<
        Vec<crabka_metadata::MetadataRecord>,
    > as wincode::Deserialize>::deserialize(
        &req.records
    ) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "submit-change body decode failed");
            let resp = CrabkaSubmitChangeResponse {
                error_code: 2,
                leader_hint: -1,
            };
            let mut out = Vec::with_capacity(16);
            resp.encode_v0(&mut out);
            return Ok(Bytes::from(out));
        }
    };
    let resp = match engine.submit_change(records).await {
        Ok(()) => CrabkaSubmitChangeResponse {
            error_code: 0,
            leader_hint: -1,
        },
        Err(RaftError::Metadata(_)) => CrabkaSubmitChangeResponse {
            error_code: 2,
            leader_hint: -1,
        },
        Err(RaftError::NotLeader { current_leader }) => CrabkaSubmitChangeResponse {
            error_code: 1,
            leader_hint: current_leader
                .and_then(|l| i64::try_from(l).ok())
                .unwrap_or(-1),
        },
        Err(e) => {
            tracing::warn!(error = ?e, "submit-change failed");
            CrabkaSubmitChangeResponse {
                error_code: 3,
                leader_hint: -1,
            }
        }
    };
    let mut out = Vec::with_capacity(16);
    resp.encode_v0(&mut out);
    Ok(Bytes::from(out))
}

/// Serve a committed `__cluster_metadata` slice to a broker-only observer (1004)
/// from the engine's `KraftLog`.
async fn dispatch_metadata_fetch(
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = CrabkaMetadataFetchRequest::decode_v0(&mut cur)?;
    let fetch_offset = req.fetch_offset.max(0);
    let max_bytes = usize::try_from(req.max_bytes.max(0)).unwrap_or(0);
    let slice = engine.metadata_fetch(fetch_offset, max_bytes).await?;
    let leader_hint: i64 = engine
        .quorum_state()
        .await
        .ok()
        .and_then(|qs| qs.leader_id)
        .and_then(|l| i64::try_from(l).ok())
        .unwrap_or(-1);

    let resp = CrabkaMetadataFetchResponse {
        error_code: 0,
        leader_hint,
        log_start_offset: slice.log_start_offset,
        high_watermark: slice.high_watermark,
        records: slice.records,
    };
    let mut out = Vec::new();
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

/// Serve `DescribeCluster` (60, KIP-919) on the controller listener from the
/// controller's metadata image. `endpoint_type=2` (CONTROLLERS) projects the
/// voter set so a `--bootstrap-controller` `AdminClient` can discover the
/// quorum; otherwise the registered brokers are returned. The controller
/// listener carries no principal/ACL context (it is the inter-node trust
/// boundary, like `metadata_fetch`), so there is no auth gate.
async fn describe_cluster_response_body(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use crabka_protocol::Decode;
    use crabka_protocol::owned::describe_cluster_request::DescribeClusterRequest;

    let mut cur = body;
    let req = DescribeClusterRequest::decode(&mut cur, version)?;
    let image = engine.current_image();

    // Controller endpoints: each voter's CONTROLLER-named listener, falling back
    // to its first advertised endpoint.
    let voters: Vec<(i32, String, i32)> = image
        .voters()
        .iter()
        .map(|v| {
            let ep = v
                .endpoints
                .iter()
                .find(|e| e.name.eq_ignore_ascii_case("CONTROLLER"))
                .or_else(|| v.endpoints.first());
            (
                i32::try_from(v.id).unwrap_or(-1),
                ep.map(|e| e.host.clone()).unwrap_or_default(),
                ep.map_or(-1, |e| i32::from(e.port)),
            )
        })
        .collect();
    // Broker endpoints: each registered broker's inter-broker host/port.
    let brokers: Vec<(i32, String, i32, Option<String>)> = image
        .brokers()
        .map(|b| {
            (
                i32::try_from(b.node_id).unwrap_or(-1),
                b.host.clone(),
                i32::from(b.port),
                b.rack.clone(),
            )
        })
        .collect();

    let controller_id: i32 = engine
        .quorum_state()
        .await
        .ok()
        .and_then(|qs| qs.leader_id)
        .and_then(|l| i32::try_from(l).ok())
        .unwrap_or(-1);

    Ok(build_describe_cluster_body(
        version,
        req.endpoint_type,
        &voters,
        &brokers,
        &image.cluster_id().to_string(),
        controller_id,
    )?)
}

/// Encode a `DescribeClusterResponse` body for `version` from already-projected
/// node tuples. Pure (no engine), so the projection-and-encode is unit-testable.
fn build_describe_cluster_body(
    version: i16,
    endpoint_type: i8,
    voters: &[(i32, String, i32)],
    brokers: &[(i32, String, i32, Option<String>)],
    cluster_id: &str,
    controller_id: i32,
) -> Result<Bytes, crabka_protocol::ProtocolError> {
    use crabka_protocol::Encode;
    use crabka_protocol::owned::describe_cluster_response::{
        DescribeClusterBroker, DescribeClusterResponse,
    };

    const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
    let entries: Vec<DescribeClusterBroker> = if endpoint_type == ENDPOINT_TYPE_CONTROLLERS {
        voters
            .iter()
            .map(|(id, host, port)| DescribeClusterBroker {
                broker_id: *id,
                host: host.clone(),
                port: *port,
                rack: None,
                ..Default::default()
            })
            .collect()
    } else {
        brokers
            .iter()
            .map(|(id, host, port, rack)| DescribeClusterBroker {
                broker_id: *id,
                host: host.clone(),
                port: *port,
                rack: rack.clone(),
                ..Default::default()
            })
            .collect()
    };

    let resp = DescribeClusterResponse {
        error_code: 0,
        error_message: None,
        endpoint_type,
        cluster_id: cluster_id.to_string(),
        controller_id,
        brokers: entries,
        // No ACL context on the controller listener → "not present" sentinel.
        cluster_authorized_operations: i32::MIN,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    #[test]
    fn api_versions_body_advertises_kip595_set_both_shapes() {
        use crabka_protocol::Decode;
        use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
        for req_v in [0i16, 4i16] {
            let body = super::api_versions_response_body(req_v);
            let v = req_v.clamp(0, 4);
            let mut cur = &body[..];
            let resp = ApiVersionsResponse::decode(&mut cur, v).expect("decode body");
            assert!(cur.is_empty(), "no trailing bytes (req_v={req_v})");
            assert!(resp.error_code == 0);
            let keys: std::collections::BTreeSet<i16> =
                resp.api_keys.iter().map(|k| k.api_key).collect();
            for want in [1i16, 18, 52, 53, 54, 59] {
                assert!(
                    keys.contains(&want),
                    "missing api_key {want} at req_v={req_v}"
                );
            }
            let vote = resp.api_keys.iter().find(|k| k.api_key == 52).unwrap();
            assert!(vote.min_version == 0 && vote.max_version == 2);
        }
    }

    #[test]
    fn describe_cluster_body_projects_controllers_and_brokers() {
        use crabka_protocol::Decode;
        use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
        use crabka_protocol::owned::describe_cluster_response::DescribeClusterResponse;

        // DescribeCluster (60) is advertised so clients negotiate it (KIP-919).
        let av = super::api_versions_response_body(4);
        let mut cur = &av[..];
        let avr = ApiVersionsResponse::decode(&mut cur, 4).unwrap();
        assert!(avr.api_keys.iter().any(|k| k.api_key == 60));

        let voters = vec![
            (1i32, "c1".to_string(), 9093i32),
            (2, "c2".to_string(), 9093),
        ];
        let brokers = vec![(
            10i32,
            "b10".to_string(),
            9092i32,
            Some("rack-a".to_string()),
        )];

        for version in [1i16, 2] {
            // endpoint_type = CONTROLLERS (2) → voter projection.
            let body =
                super::build_describe_cluster_body(version, 2, &voters, &brokers, "clusterX", 1)
                    .unwrap();
            let mut cur = &body[..];
            let resp = DescribeClusterResponse::decode(&mut cur, version).unwrap();
            assert!(cur.is_empty(), "no trailing bytes (v={version})");
            assert!(resp.endpoint_type == 2);
            assert!(resp.cluster_id == "clusterX");
            assert!(resp.controller_id == 1);
            assert!(resp.brokers.len() == 2);
            assert!(
                resp.brokers[0].broker_id == 1
                    && resp.brokers[0].host == "c1"
                    && resp.brokers[0].port == 9093
            );

            // endpoint_type = BROKERS (1) → broker projection (rack preserved).
            let body =
                super::build_describe_cluster_body(version, 1, &voters, &brokers, "clusterX", 1)
                    .unwrap();
            let mut cur = &body[..];
            let resp = DescribeClusterResponse::decode(&mut cur, version).unwrap();
            assert!(resp.endpoint_type == 1);
            assert!(resp.brokers.len() == 1);
            assert!(resp.brokers[0].broker_id == 10 && resp.brokers[0].host == "b10");
            assert!(resp.brokers[0].rack.as_deref() == Some("rack-a"));
        }
    }
}
