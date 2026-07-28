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
use crabka_ids::{ApiKey, ApiVersion};
use crabka_units::prelude::{ByteSize, ByteSizeExt as _};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    error::RaftError,
    kraft::{
        KraftController,
        transport::{Inbound, api_key},
    },
    wire::{
        API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE, CrabkaMetadataFetchRequest,
        CrabkaMetadataFetchResponse, CrabkaSubmitChangeRequest, CrabkaSubmitChangeResponse,
    },
};

/// Kafka request-header `correlation_id`, echoed back in the response header.
type CorrelationId = i32;

/// Kafka's `ApiVersions` API key. The controller TCP listener answers this
/// because `crabka_client_core::Connection::connect` performs an `ApiVersions`
/// handshake before any other request.
const API_KEY_API_VERSIONS: i16 = 18;

/// Highest `ApiVersions` request version this listener speaks: the advertised
/// max in the `api_keys` table and the clamp applied to the response body codec
/// (JVM controllers dial at v4; Crabka's own client at v0).
const API_VERSIONS_MAX_VERSION: i16 = 4;

/// `DescribeCluster` (KIP-919) — served on the controller listener so an
/// `AdminClient` bootstrapped with `--bootstrap-controller` can discover the
/// quorum's controller (or broker) endpoints directly from the leader.
const API_KEY_DESCRIBE_CLUSTER: i16 = 60;

/// `CrabkaSubmitChangeResponse::error_code`: the change was applied.
const SUBMIT_CHANGE_APPLIED: i16 = 0;
/// `CrabkaSubmitChangeResponse::error_code`: this node is not the leader;
/// consult `leader_hint`.
const SUBMIT_CHANGE_NOT_LEADER: i16 = 1;
/// `CrabkaSubmitChangeResponse::error_code`: metadata validation rejected the
/// records (also returned when the wincode body fails to decode).
const SUBMIT_CHANGE_REJECTED: i16 = 2;
/// `CrabkaSubmitChangeResponse::error_code`: any other engine failure.
const SUBMIT_CHANGE_FAILED: i16 = 3;

/// `leader_hint` sentinel meaning "the current leader is unknown".
const LEADER_HINT_UNKNOWN: i64 = -1;

pub(crate) async fn run(
    listener: TcpListener,
    engine: KraftController,
    shutdown: CancellationToken,
    handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
    shard_router: Option<Arc<dyn crate::RaftShardRouter>>,
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
                        let shard_router = shard_router.clone();
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
                            if let Err(e) = handle_conn(boxed, engine, shutdown, shard_router).await {
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
    shard_router: Option<Arc<dyn crate::RaftShardRouter>>,
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
                    // v0; the JVM controller asks at v4. The generated codec
                    // speaks the raw `int16`, so unwrap the version here.
                    let resp = api_versions_response_body(api_version.get());
                    write_response_no_tagged_fields(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                // DescribeCluster (60, KIP-919) is served here rather than in
                // `dispatch` because it needs the request version (for the
                // flexible body codec) and the controller's metadata image. The
                // flexible v1 ResponseHeader is supplied by `write_response`.
                if api_key_n == API_KEY_DESCRIBE_CLUSTER {
                    let resp =
                        describe_cluster_response_body(api_version.get(), &body, &engine).await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                let resp = dispatch_with_router(api_key_n, body, &engine, shard_router.as_deref()).await?;
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

fn require_remaining(available: usize, required: usize) -> Result<(), RaftError> {
    match required.checked_sub(available) {
        Some(0) | None => Ok(()),
        Some(needed) => Err(truncated(needed)),
    }
}

async fn read_one_request<S>(
    stream: &mut S,
) -> Result<(ApiKey, ApiVersion, CorrelationId, Bytes), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    const REQUEST_HEADER_FIXED_LEN: usize = 8;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(io_err)?;
    let raw_len = i32::from_be_bytes(len_buf);
    let len = usize::try_from(raw_len.max(0)).unwrap_or(0);
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.map_err(io_err)?;

    // RequestHeader v2 (flexible): api_key(i16), api_version(i16),
    // correlation_id(i32), client_id(NULLABLE_STRING), tagged_fields(varint=0).
    // The two adjacent header `int16`s are wrapped into distinct newtypes here so
    // the transpose-prone pair can't be swapped by callers.
    let mut cur: &[u8] = &frame;
    require_remaining(cur.remaining(), REQUEST_HEADER_FIXED_LEN)?;
    let api_key_n = ApiKey(cur.get_i16());
    let api_version = ApiVersion(cur.get_i16());
    let correlation_id = cur.get_i32();

    // Skip client_id: NULLABLE_STRING (i16 length + bytes; -1 = null).
    require_remaining(cur.remaining(), 2)?;
    let cs_len = cur.get_i16();
    if let Ok(n @ 1..) = usize::try_from(cs_len) {
        require_remaining(cur.remaining(), n)?;
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
    correlation_id: CorrelationId,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_response_frame(stream, correlation_id, body, true).await
}

/// Write a response without the leading tagged-fields byte. Used only by the
/// `ApiVersions` v0 path, which decodes a `ResponseHeader v0`.
async fn write_response_no_tagged_fields<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_response_frame(stream, correlation_id, body, false).await
}

async fn write_response_frame<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
    include_tagged_fields: bool,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(4 + usize::from(include_tagged_fields) + body.len());
    frame.put_i32(correlation_id);
    if include_tagged_fields {
        frame.put_u8(0); // empty tagged_fields (ResponseHeader v1)
    }
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
/// A real `mirror.gcr.io/apache/kafka:4.0.0` controller dials peers with `ApiVersions v4` over a
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
    use crabka_protocol::{
        Encode,
        owned::api_versions_response::{ApiVersion as ApiVersionEntry, ApiVersionsResponse},
    };
    // (api_key, max_version) — min_version/error_code/throttle_time_ms are all
    // protocol defaults of 0, so leave them implicit. Each max_version is the
    // highest version the engine's codec speaks for that API.
    const KEYS: &[(i16, i16)] = &[
        (api_key::FETCH, 17),
        (API_KEY_API_VERSIONS, API_VERSIONS_MAX_VERSION),
        (api_key::VOTE, 2),
        (api_key::BEGIN_QUORUM_EPOCH, 1),
        (api_key::END_QUORUM_EPOCH, 1),
        (api_key::FETCH_SNAPSHOT, 1),
        (API_KEY_DESCRIBE_CLUSTER, 2),
    ];
    let resp = ApiVersionsResponse {
        api_keys: KEYS
            .iter()
            .map(|&(api_key, max_version)| ApiVersionEntry {
                api_key,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    // JVM dials at v4 (flexible); Crabka's own client at v0 (non-flexible). The
    // codec emits the correct body shape per version: req v<=2 → non-flexible
    // v0-shaped body, req v>=3 → flexible (compact) body. The v0 ApiVersions
    // response HEADER asymmetry lives in the framing (`write_response_no_tagged_fields`),
    // not here.
    let body_version = req_version.clamp(0, API_VERSIONS_MAX_VERSION);
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
#[cfg(test)]
#[tracing::instrument(level = "debug", skip_all, fields(node = engine.node_id().0, api_key = api_key_n.get()), err)]
async fn dispatch(
    api_key_n: ApiKey,
    body: Bytes,
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    dispatch_with_router(api_key_n, body, engine, None).await
}

async fn dispatch_with_router(
    api_key_n: ApiKey,
    body: Bytes,
    engine: &KraftController,
    shard_router: Option<&dyn crate::RaftShardRouter>,
) -> Result<Bytes, RaftError> {
    if let Some(router) = shard_router
        && let Some(resp) = router.route(api_key_n.get(), body.clone()).await?
    {
        return Ok(resp);
    }
    match api_key_n {
        ApiKey(api_key::FETCH) => {
            deliver_inbound(engine, |reply| Inbound::Fetch { req: body, reply }).await
        }
        ApiKey(api_key::VOTE) => {
            deliver_inbound(engine, |reply| Inbound::Vote { req: body, reply }).await
        }
        ApiKey(api_key::BEGIN_QUORUM_EPOCH) => {
            deliver_inbound(engine, |reply| Inbound::BeginQuorumEpoch {
                req: body,
                reply,
            })
            .await
        }
        ApiKey(api_key::END_QUORUM_EPOCH) => {
            deliver_inbound(engine, |reply| Inbound::EndQuorumEpoch { req: body, reply }).await
        }
        ApiKey(api_key::FETCH_SNAPSHOT) => {
            deliver_inbound(engine, |reply| Inbound::FetchSnapshot { req: body, reply }).await
        }
        ApiKey(API_KEY_SUBMIT_CHANGE) => dispatch_submit_change(&body, engine).await,
        ApiKey(API_KEY_METADATA_FETCH) => dispatch_metadata_fetch(&body, engine).await,
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
                error_code: SUBMIT_CHANGE_REJECTED,
                leader_hint: LEADER_HINT_UNKNOWN,
                result: Bytes::new(),
            };
            let mut out = Vec::with_capacity(16);
            resp.encode_v0(&mut out)?;
            return Ok(Bytes::from(out));
        }
    };
    let resp = match engine.submit_change(records).await {
        Ok(result) => CrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_APPLIED,
            leader_hint: LEADER_HINT_UNKNOWN,
            result: Bytes::from(
                <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Serialize>::serialize(&result)?,
            ),
        },
        Err(RaftError::Metadata(_)) => CrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_REJECTED,
            leader_hint: LEADER_HINT_UNKNOWN,
            result: Bytes::new(),
        },
        Err(RaftError::NotLeader { current_leader }) => CrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_NOT_LEADER,
            leader_hint: current_leader
                .and_then(|l| i64::try_from(l.0).ok())
                .unwrap_or(LEADER_HINT_UNKNOWN),
            result: Bytes::new(),
        },
        Err(e) => {
            tracing::warn!(error = ?e, "submit-change failed");
            CrabkaSubmitChangeResponse {
                error_code: SUBMIT_CHANGE_FAILED,
                leader_hint: LEADER_HINT_UNKNOWN,
                result: Bytes::new(),
            }
        }
    };
    let mut out = Vec::with_capacity(16);
    resp.encode_v0(&mut out)?;
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
    // The decoded `int32` enters the domain here; the codec itself stays raw so
    // the request stays byte-exact. A negative budget clamps to zero, as before.
    let max_size = ByteSize::from_bytes_i64(i64::from(req.max_bytes.max(0)));
    let slice = engine.metadata_fetch(fetch_offset, max_size).await?;
    let leader_hint: i64 = engine
        .quorum_state()
        .await
        .ok()
        .and_then(|qs| qs.leader_id)
        .and_then(|l| i64::try_from(l.0).ok())
        .unwrap_or(LEADER_HINT_UNKNOWN);

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
// The broker-id `i32::try_from(node_id).unwrap_or(-1)` overflow fallback is
// unreachable: the metadata layer rejects registering a `node_id` exceeding
// `i32::MAX` (BrokerRegistrationRecord encode validation), so the `-1` sentinel
// is dead defensive code that no input can reach. The reachable voter/broker
// projection is covered by the sibling tests.
#[cfg_attr(test, mutants::skip)]
async fn describe_cluster_response_body(
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    use crabka_protocol::{Decode, owned::describe_cluster_request::DescribeClusterRequest};

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
                i32::try_from(v.id.0).unwrap_or(-1),
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
                i32::try_from(b.node_id.0).unwrap_or(-1),
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
        .and_then(|l| i32::try_from(l.0).ok())
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
    use crabka_protocol::{
        Encode,
        owned::describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
    };

    const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
    let entries: Vec<DescribeClusterBroker> = if endpoint_type == ENDPOINT_TYPE_CONTROLLERS {
        voters
            .iter()
            .map(|(id, host, port)| DescribeClusterBroker {
                broker_id: *id,
                host: host.clone(),
                port: *port,
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

    // Only the non-default fields are set; error_code (0), error_message (None),
    // throttle_time_ms (0), and cluster_authorized_operations (i32::MIN — "not
    // present"; the controller listener has no ACL context) fall through to
    // `Default`. Specifying them explicitly would just be equivalent-mutant noise.
    let resp = DescribeClusterResponse {
        endpoint_type,
        cluster_id: cluster_id.to_string(),
        controller_id,
        brokers: entries,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::{BufMut, Bytes};
    use crabka_metadata::{MetadataRecord, NodeId, TopicRecord};
    use crabka_protocol::Decode;
    use crabka_units::prelude::{Time, TimeExt as _, millis, secs};
    use tokio::io::AsyncWriteExt;
    use uuid::Uuid;

    use super::*;

    /// Election timeout for the in-test engines: short, so a single voter wins
    /// immediately.
    const TEST_ELECTION_TIMEOUT: Time = millis(50);

    /// How long a test waits for a leader to appear.
    const TEST_LEADER_DEADLINE: Time = secs(5);

    fn length_prefixed(frame: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(frame.len() + 4);
        out.extend_from_slice(&(u32::try_from(frame.len()).unwrap()).to_be_bytes());
        out.extend_from_slice(frame);
        out
    }

    fn request_frame(
        api_key: ApiKey,
        api_version: ApiVersion,
        correlation_id: i32,
        client_id: &str,
        body: &[u8],
    ) -> Vec<u8> {
        let mut frame = bytes::BytesMut::new();
        frame.put_i16(api_key.get());
        frame.put_i16(api_version.get());
        frame.put_i32(correlation_id);
        frame.put_i16(i16::try_from(client_id.len()).unwrap());
        frame.put_slice(client_id.as_bytes());
        frame.put_u8(0);
        frame.put_slice(body);
        length_prefixed(&frame)
    }

    fn raw_request_frame(
        api_key: ApiKey,
        api_version: ApiVersion,
        correlation_id: i32,
        client_id_len: i16,
        client_id_bytes: &[u8],
        tagged_or_body: &[u8],
    ) -> Vec<u8> {
        let mut frame = bytes::BytesMut::new();
        frame.put_i16(api_key.get());
        frame.put_i16(api_version.get());
        frame.put_i32(correlation_id);
        frame.put_i16(client_id_len);
        frame.put_slice(client_id_bytes);
        frame.put_slice(tagged_or_body);
        length_prefixed(&frame)
    }

    fn voter(id: u64, endpoints: Vec<crabka_metadata::VoterEndpoint>) -> crabka_metadata::Voter {
        crabka_metadata::Voter {
            id: NodeId(id),
            directory_id: Uuid::from_u128(u128::from(id)),
            endpoints,
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        }
    }

    fn controller_endpoint(host: &str, port: u16) -> crabka_metadata::VoterEndpoint {
        crabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.into(),
            port,
        }
    }

    fn test_engine_with_voters(
        me: u64,
        voters: impl IntoIterator<Item = crabka_metadata::Voter>,
    ) -> (KraftController, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let ctrl = KraftController::open(
            dir.path().to_path_buf(),
            NodeId(me),
            Uuid::nil(),
            crabka_metadata::VoterSet::from_voters(voters),
            TEST_ELECTION_TIMEOUT,
            std::sync::Arc::new(crate::kraft::NullPeerSender),
            0,
        )
        .expect("open engine");
        (ctrl, dir)
    }

    fn single_voter_engine() -> (KraftController, tempfile::TempDir) {
        test_engine_with_voters(
            1,
            [voter(1, vec![controller_endpoint("controller-1", 9093)])],
        )
    }

    async fn wait_for_leader(engine: &KraftController) {
        let mut rx = engine.watch_leader();
        tokio::time::timeout(TEST_LEADER_DEADLINE.to_std(), rx.wait_for(Option::is_some))
            .await
            .expect("leader elected")
            .expect("leader channel open");
    }

    fn topic_record(name: &str) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })
    }

    fn submit_change_body(records: &[MetadataRecord]) -> Bytes {
        let records = records.to_vec();
        let records =
            <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
                &records,
            )
            .expect("wincode");
        let req = CrabkaSubmitChangeRequest {
            records: Bytes::from(records),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).expect("submit request");
        Bytes::from(out)
    }

    fn metadata_fetch_body(fetch_offset: i64, max_bytes: i32) -> Bytes {
        let req = CrabkaMetadataFetchRequest {
            fetch_offset,
            max_bytes,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out);
        Bytes::from(out)
    }

    fn describe_cluster_body(version: i16, endpoint_type: i8) -> Bytes {
        use crabka_protocol::{Encode, owned::describe_cluster_request::DescribeClusterRequest};

        let req = DescribeClusterRequest {
            endpoint_type,
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        req.encode(&mut out, version).expect("describe request");
        out.freeze()
    }

    fn decode_submit_change_response(body: &[u8]) -> CrabkaSubmitChangeResponse {
        let mut cur = body;
        CrabkaSubmitChangeResponse::decode_v0(&mut cur).expect("submit response")
    }

    fn decode_metadata_fetch_response(body: &[u8]) -> CrabkaMetadataFetchResponse {
        let mut cur = body;
        CrabkaMetadataFetchResponse::decode_v0(&mut cur).expect("metadata fetch response")
    }

    #[test]
    fn is_eof_only_matches_unexpected_eof_io_errors() {
        let io_error = |kind| {
            super::RaftError::Storage(crabka_log::LogError::Io(std::io::Error::new(kind, "io")))
        };
        let cases = [
            (
                "unexpected EOF",
                io_error(std::io::ErrorKind::UnexpectedEof),
                true,
            ),
            (
                "broken pipe",
                io_error(std::io::ErrorKind::BrokenPipe),
                false,
            ),
            (
                "protocol error",
                super::RaftError::Protocol(crabka_protocol::ProtocolError::InvalidValue("not io")),
                false,
            ),
        ];
        for (_case, err, want) in cases {
            assert2::assert!(super::is_eof(&err) == want);
        }
    }

    #[tokio::test]
    async fn read_one_request_decodes_header_variants() {
        let cases = [
            (
                "flexible header with client id and body",
                request_frame(ApiKey(52), ApiVersion(2), 123, "raft-client", b"payload"),
                b"payload".as_slice(),
            ),
            (
                "null client id with no body",
                raw_request_frame(ApiKey(52), ApiVersion(2), 123, -1, &[], &[]),
                b"".as_slice(),
            ),
        ];
        for (case, frame, want_body) in cases {
            let (mut client, mut server) = tokio::io::duplex(128);
            let writer = tokio::spawn(async move {
                client.write_all(&frame).await.unwrap();
            });

            let (api_key, api_version, correlation_id, body) =
                super::read_one_request(&mut server).await.expect("decode");

            check!(
                (api_key, api_version, correlation_id, body.as_ref())
                    == (ApiKey(52), ApiVersion(2), 123, want_body),
                "case: {case}"
            );
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_one_request_reports_header_shortfalls() {
        let partial_fixed = {
            let mut f = bytes::BytesMut::new();
            f.put_i16(52);
            f.put_i16(2);
            f.put_i32(123);
            f
        };
        let mut partial_client_id_len = partial_fixed.clone();
        partial_client_id_len.put_u8(0x80);
        let cases = [
            // Frame ends inside the 8-byte fixed header.
            ("short fixed header", length_prefixed(&[0, 52, 0, 2]), 4),
            // Fixed header complete, client-id length missing entirely.
            (
                "missing client id length",
                length_prefixed(&partial_fixed),
                2,
            ),
            // Only one byte of the 2-byte client-id length present.
            (
                "partial client id length",
                length_prefixed(&partial_client_id_len),
                1,
            ),
            // Client-id length declares 4 bytes; only 1 present.
            (
                "client id bytes shortfall",
                raw_request_frame(ApiKey(52), ApiVersion(2), 123, 4, b"x", &[]),
                3,
            ),
        ];
        for (_case, frame, needed) in cases {
            let (mut client, mut server) = tokio::io::duplex(128);
            let writer = tokio::spawn(async move {
                client.write_all(&frame).await.unwrap();
            });

            let err = super::read_one_request(&mut server)
                .await
                .expect_err("short frame");

            assert2::assert!(matches!(
                err,
                super::RaftError::Protocol(
                    crabka_protocol::ProtocolError::UnexpectedEof { needed: n }
                ) if n == needed
            ));
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_one_request_keeps_nonzero_tagged_byte_as_body() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let frame = raw_request_frame(
            ApiKey(52),
            ApiVersion(2),
            123,
            0,
            &[],
            &[1, b'p', b'a', b'y'],
        );
        let writer = tokio::spawn(async move {
            client.write_all(&frame).await.unwrap();
        });

        let (_, _, _, body) = super::read_one_request(&mut server).await.expect("decode");

        assert2::assert!(body.as_ref() == &[1, b'p', b'a', b'y']);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_routes_kip595_peer_apis_to_engine() {
        use crate::kraft::transport::wire::{PeerRequest, PeerResponse};

        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let vote = PeerRequest::Vote {
            voter_id: NodeId(1),
            candidate_epoch: 1,
            candidate: NodeId(2),
            last_epoch: 0,
            last_offset: 0,
            pre_vote: false,
        }
        .encode();
        let vote_resp = super::dispatch(ApiKey(api_key::VOTE), vote, &engine)
            .await
            .expect("vote dispatch");
        assert2::assert!(PeerResponse::decode_vote(&vote_resp).is_some());

        let fetch = PeerRequest::Fetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 0,
        }
        .encode();
        let fetch_resp = super::dispatch(ApiKey(api_key::FETCH), fetch, &engine)
            .await
            .expect("fetch dispatch");
        assert2::assert!(PeerResponse::decode_fetch(&fetch_resp).is_some());

        let snapshot = PeerRequest::FetchSnapshot {
            from: NodeId(2),
            snapshot_id: (10, 1),
            position: 0,
            max_bytes: 32,
        }
        .encode();
        let snapshot_resp = super::dispatch(ApiKey(api_key::FETCH_SNAPSHOT), snapshot, &engine)
            .await
            .expect("snapshot dispatch");
        assert2::assert!(matches!(
            PeerResponse::decode_fetch_snapshot(&snapshot_resp),
            Some(PeerResponse::FetchSnapshot { error_code: 98, .. })
        ));

        let begin = PeerRequest::BeginQuorumEpoch {
            leader_id: NodeId(1),
            leader_epoch: 1,
        }
        .encode();
        let begin_resp = super::dispatch(ApiKey(api_key::BEGIN_QUORUM_EPOCH), begin, &engine)
            .await
            .expect("begin dispatch");
        assert2::assert!(!begin_resp.is_empty());

        let end = PeerRequest::EndQuorumEpoch {
            leader_id: NodeId(1),
            leader_epoch: 1,
        }
        .encode();
        let end_resp = super::dispatch(ApiKey(api_key::END_QUORUM_EPOCH), end, &engine)
            .await
            .expect("end dispatch");
        assert2::assert!(!end_resp.is_empty());
    }

    #[tokio::test]
    async fn dispatch_submit_change_encodes_success_and_decode_errors() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let ok_body = super::dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            submit_change_body(&[topic_record("submit-ok")]),
            &engine,
        )
        .await
        .expect("submit dispatch");
        let ok = decode_submit_change_response(&ok_body);
        assert2::assert!(ok.error_code == 0);
        assert2::assert!(ok.leader_hint == -1);

        let bad_req = CrabkaSubmitChangeRequest {
            records: Bytes::from_static(b"not-wincode"),
        };
        let mut bad_body = Vec::new();
        bad_req.encode_v0(&mut bad_body).unwrap();
        let err_body = super::dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            Bytes::from(bad_body),
            &engine,
        )
        .await
        .expect("decode failure dispatch");
        let err = decode_submit_change_response(&err_body);
        assert2::assert!(err.error_code == 2);
        assert2::assert!(err.leader_hint == -1);
    }

    #[tokio::test]
    async fn dispatch_submit_change_encodes_metadata_rejection() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let topic = topic_record("duplicate");
        let first = super::dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            submit_change_body(std::slice::from_ref(&topic)),
            &engine,
        )
        .await
        .expect("first submit");
        assert2::assert!(decode_submit_change_response(&first).error_code == 0);

        let duplicate = super::dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            submit_change_body(&[topic]),
            &engine,
        )
        .await
        .expect("duplicate submit");
        let duplicate = decode_submit_change_response(&duplicate);
        assert2::assert!(duplicate.error_code == 2);
        assert2::assert!(duplicate.leader_hint == -1);
    }

    #[tokio::test]
    async fn dispatch_metadata_fetch_clamps_negative_request_and_reports_unknown_leader() {
        let (engine, _dir) = test_engine_with_voters(1, std::iter::empty());

        let body = super::dispatch(
            ApiKey(API_KEY_METADATA_FETCH),
            metadata_fetch_body(-5, -1),
            &engine,
        )
        .await
        .expect("metadata fetch dispatch");

        let resp = decode_metadata_fetch_response(&body);
        check!(
            (
                resp.error_code,
                resp.leader_hint,
                resp.high_watermark,
                resp.records.is_empty(),
            ) == (0, -1, 0, true)
        );
    }

    #[tokio::test]
    async fn dispatch_metadata_fetch_returns_committed_records_and_leader_hint() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;
        engine
            .submit_change(vec![topic_record("metadata-fetch")])
            .await
            .expect("submit");

        let body = super::dispatch(
            ApiKey(API_KEY_METADATA_FETCH),
            metadata_fetch_body(0, 1_048_576),
            &engine,
        )
        .await
        .expect("metadata fetch dispatch");

        let resp = decode_metadata_fetch_response(&body);
        check!(
            (
                resp.error_code,
                resp.leader_hint,
                resp.high_watermark >= 1,
                resp.records.is_empty(),
            ) == (0, 1, true, false)
        );
    }

    #[tokio::test]
    async fn describe_cluster_response_body_projects_controller_fallbacks() {
        use crabka_protocol::owned::describe_cluster_response::DescribeClusterResponse;

        let (engine, _dir) = test_engine_with_voters(1, [voter(u64::MAX, Vec::new())]);
        let body = super::describe_cluster_response_body(1, &describe_cluster_body(1, 2), &engine)
            .await
            .expect("describe cluster");

        let mut cur = &body[..];
        let resp = DescribeClusterResponse::decode(&mut cur, 1).expect("describe response");
        check!(cur.is_empty());
        check!(
            (
                resp.controller_id,
                resp.brokers
                    .iter()
                    .map(|broker| (broker.broker_id, broker.host.as_str(), broker.port))
                    .collect::<Vec<_>>(),
            ) == (-1, vec![(-1, "", -1)])
        );
    }

    #[test]
    fn api_versions_body_advertises_kip595_set_both_shapes() {
        use crabka_protocol::{Decode, owned::api_versions_response::ApiVersionsResponse};
        for req_v in [0i16, 4i16] {
            let body = super::api_versions_response_body(req_v);
            let v = req_v.clamp(0, 4);
            let mut cur = &body[..];
            let resp = ApiVersionsResponse::decode(&mut cur, v).expect("decode body");
            assert2::assert!(cur.is_empty());
            assert2::assert!(resp.error_code == 0);
            let keys: std::collections::BTreeSet<i16> =
                resp.api_keys.iter().map(|k| k.api_key).collect();
            for want in [1i16, 18, 52, 53, 54, 59] {
                assert2::assert!(keys.contains(&want));
            }
            let vote = resp.api_keys.iter().find(|k| k.api_key == 52).unwrap();
            assert2::assert!(vote.min_version == 0 && vote.max_version == 2);
        }
    }

    #[test]
    fn describe_cluster_body_projects_controllers_and_brokers() {
        use crabka_protocol::{
            Decode,
            owned::{
                api_versions_response::ApiVersionsResponse,
                describe_cluster_response::DescribeClusterResponse,
            },
        };

        // DescribeCluster (60) is advertised so clients negotiate it (KIP-919).
        let av = super::api_versions_response_body(4);
        let mut cur = &av[..];
        let avr = ApiVersionsResponse::decode(&mut cur, 4).unwrap();
        assert2::assert!(avr.api_keys.iter().any(|k| k.api_key == 60));

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
            assert2::assert!(cur.is_empty());
            check!(
                (
                    resp.endpoint_type,
                    resp.cluster_id.as_str(),
                    resp.controller_id,
                    resp.brokers
                        .iter()
                        .map(|broker| (broker.broker_id, broker.host.as_str(), broker.port))
                        .collect::<Vec<_>>(),
                ) == (2, "clusterX", 1, vec![(1, "c1", 9093), (2, "c2", 9093)])
            );

            // endpoint_type = BROKERS (1) → broker projection (rack preserved).
            let body =
                super::build_describe_cluster_body(version, 1, &voters, &brokers, "clusterX", 1)
                    .unwrap();
            let mut cur = &body[..];
            let resp = DescribeClusterResponse::decode(&mut cur, version).unwrap();
            check!(
                (
                    resp.endpoint_type,
                    resp.brokers
                        .iter()
                        .map(|broker| (
                            broker.broker_id,
                            broker.host.as_str(),
                            broker.port,
                            broker.rack.as_deref(),
                        ))
                        .collect::<Vec<_>>(),
                ) == (1, vec![(10, "b10", 9092, Some("rack-a"))])
            );
        }
    }
}
