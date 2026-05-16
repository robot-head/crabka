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

use std::net::SocketAddr;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::network::codec::{self, MAX_FRAME_BYTES};

const API_VERSIONS_KEY: i16 = 18;

/// Per-listener entrypoint. Branches between TLS termination (when the
/// listener's protocol requires TLS) and the plaintext path. Both paths
/// converge on [`serve_connection_stream`] for the per-connection request
/// loop.
pub async fn serve_connection_on_listener(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
) {
    // Capture the peer address from the underlying TCP socket before we
    // hand the stream off to the TLS layer / framing loop. Slice-13 ACL
    // handlers need this for host-based ACL matching. If `peer_addr`
    // fails (rare — socket closed mid-accept), fall back to the
    // unspecified address; ACL matchers treat it as a non-matching host.
    let peer = stream.peer_addr().unwrap_or_else(|e| {
        tracing::debug!(error = %e, "peer_addr() failed, using 0.0.0.0:0");
        SocketAddr::from(([0u8, 0, 0, 0], 0))
    });
    if spec.protocol.requires_tls() {
        let Some(acceptor) = broker.tls_acceptor.clone() else {
            tracing::error!(
                listener = %spec.name,
                "TLS listener configured but broker has no TlsAcceptor"
            );
            return;
        };
        match acceptor.accept(stream).await {
            Ok(tls_stream) => serve_connection_stream(broker, tls_stream, spec, peer).await,
            Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
        }
    } else {
        serve_connection_plaintext(broker, stream, spec, peer).await;
    }
}

/// Plaintext entry point: keeps the legacy `TcpStream`-typed signature
/// for call sites (and lets us record the peer's TCP address before we
/// hand the stream to the generic loop).
async fn serve_connection_plaintext(
    broker: std::sync::Arc<Broker>,
    stream: TcpStream,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
) {
    serve_connection_stream(broker, stream, spec, peer).await;
}

/// Generic per-connection request loop. `S` is the post-handshake byte
/// stream — `TcpStream` for plaintext listeners, `tokio_rustls::server::TlsStream<TcpStream>`
/// for TLS listeners. `spec` carries the listener's protocol so the loop
/// can initialise `ConnectionAuth` correctly and gate pre-auth requests on
/// SASL listeners (Slice 12, T12).
#[allow(clippy::too_many_lines)] // each api_key intercept arm adds ~15 lines; T8/T9 will extract them.
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<Broker>,
    stream: S,
    spec: crate::config::ListenerSpec,
    peer: SocketAddr,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut framed: Framed<S, _> = Framed::new(stream, codec::codec());
    let is_sasl_listener = spec.protocol.requires_sasl();
    // Per-connection auth state. Mutated by the SASL handlers in T13/T14;
    // T12 only uses it to gate non-allowlisted api_keys before auth completes.
    #[allow(unused_mut)] // T13/T14 mutate `auth` via SaslAuthenticate handlers.
    let mut auth = if is_sasl_listener {
        crate::network::auth::ConnectionAuth::Anonymous
    } else {
        // PLAINTEXT / SSL: implicit anonymous, treated as authenticated for
        // gating purposes so the pre-auth allowlist is a no-op.
        crate::network::auth::ConnectionAuth::Authenticated {
            principal: crabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                mechanism: crabka_security::SaslMechanism::Plain,
            },
        }
    };
    tracing::info!(listener = %spec.name, sasl = is_sasl_listener, "connection opened");

    while let Some(frame) = framed.next().await {
        let frame = match frame {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "frame decode error, closing");
                break;
            }
        };
        // Pre-auth gate: on SASL listeners, before the connection is
        // authenticated, only api_keys on the allowlist (17/36/18) are
        // permitted. Anything else gets ILLEGAL_SASL_STATE (34).
        //
        // Response-shape note: every api_key has a different response body,
        // so producing a typed `error_code = 34` frame from this generic
        // dispatch layer would require a switch over every api_key. T13
        // sends a *typed* SaslAuthenticate(36) response with error_code=58
        // on credential failure (its specific shape is known there). For
        // the generic pre-auth gate we close the TCP connection without
        // sending a body — JVM clients surface this to the caller as an
        // auth failure (closed connection during SASL), and this matches
        // the conservative behaviour we want for unauthenticated peers.
        if is_sasl_listener && !auth.is_authenticated() {
            match peek_api_key(&frame) {
                Ok(api_key) if !crate::network::auth::is_pre_auth_allowed(api_key) => {
                    tracing::info!(
                        api_key,
                        listener = %spec.name,
                        "pre-auth request blocked (ILLEGAL_SASL_STATE), closing connection"
                    );
                    let _ = codes::ILLEGAL_SASL_STATE; // referenced for docs/grep
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "frame too small to peek api_key, closing");
                    break;
                }
            }
        }
        // SASL frames (api_key 17 / 36) mutate the per-connection auth state,
        // which lives in this loop. They run *before* the regular handler
        // table because handlers receive only `&Broker` and have no way to
        // touch `auth`. Returning `Some(SaslFrameOutcome)` short-circuits
        // the normal dispatch_one() path for that frame.
        if let Some(outcome) = try_handle_sasl_frame(&broker, &frame, &mut auth) {
            let SaslFrameOutcome {
                response_bytes,
                close_after,
            } = match outcome {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, "SASL dispatch error, closing connection");
                    break;
                }
            };
            if let Err(e) = framed.send(response_bytes).await {
                tracing::warn!(error = %e, "framed.send error during SASL, closing");
                break;
            }
            if close_after {
                tracing::info!("closing connection after failed SaslAuthenticate");
                break;
            }
            continue;
        }
        // AlterUserScramCredentials (51) needs the connection's authenticated
        // principal and the peer `SocketAddr` so it can enforce the Cluster
        // Alter ACL gate (slice-13 T19, replacing the slice-12 super-user-name
        // equality check). The handler table signature passes only `&Broker`,
        // so this case is intercepted inline like the SASL frames are.
        // Returning `Some` short-circuits the normal `dispatch_one()` path
        // for this frame.
        if peek_api_key(&frame).ok() == Some(51) {
            match handle_alter_user_scram_credentials_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AUSCR, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AUSCR dispatch error, closing connection");
                    break;
                }
            }
        }
        // Produce (0, slice-13 T10) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic in the request for `Write` and
        // emit TOPIC_AUTHORIZATION_FAILED on the per-partition rows of
        // any topic that comes back `Deny`. The `&Broker`-only handler
        // table signature can't carry that context, so this api_key
        // intercepts inline.
        if peek_api_key(&frame).ok() == Some(0) {
            match handle_produce_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during Produce, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Produce dispatch error, closing connection");
                    break;
                }
            }
        }
        // Fetch (1, slice-13 T11) needs both the authenticated principal
        // AND the peer's `SocketAddr` so the handler can batch-authorize
        // every topic in the request for `Read` and emit
        // TOPIC_AUTHORIZATION_FAILED on the per-partition rows of any
        // topic that comes back `Deny`. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts
        // inline.
        if peek_api_key(&frame).ok() == Some(1) {
            match handle_fetch_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during Fetch, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Fetch dispatch error, closing connection");
                    break;
                }
            }
        }
        // Metadata (3, slice-13 T12) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every candidate topic for `Describe`.
        // Asymmetric behaviour: named-topic Deny surfaces
        // TOPIC_AUTHORIZATION_FAILED on the topic row; fetch-all Deny
        // silently omits the topic from the response. The
        // `&Broker`-only handler table signature can't carry the
        // principal+peer context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(3) {
            match handle_metadata_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during Metadata, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Metadata dispatch error, closing connection");
                    break;
                }
            }
        }
        // CreateTopics (19, slice-13 T13) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Create` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on every topic row on Deny. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(19) {
            match handle_create_topics_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during CreateTopics, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CreateTopics dispatch error, closing connection");
                    break;
                }
            }
        }
        // DeleteTopics (20, slice-13 T13) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic for `Delete` and emit
        // TOPIC_AUTHORIZATION_FAILED on denied topic rows. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(20) {
            match handle_delete_topics_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteTopics, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteTopics dispatch error, closing connection");
                    break;
                }
            }
        }
        // AlterConfigs (33, slice-13 T14) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `AlterConfigs` per resource: Topic resources check
        // against `ResourceType::Topic(resource_name)` and emit
        // TOPIC_AUTHORIZATION_FAILED on Deny; Broker resources check
        // against `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on Deny.
        if peek_api_key(&frame).ok() == Some(33) {
            match handle_alter_configs_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AlterConfigs, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AlterConfigs dispatch error, closing connection");
                    break;
                }
            }
        }
        // IncrementalAlterConfigs (44, slice-13 T14) — same shape as
        // AlterConfigs: needs both the authenticated principal and the peer
        // `SocketAddr` for per-resource ACL enforcement.
        if peek_api_key(&frame).ok() == Some(44) {
            match handle_incremental_alter_configs_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(
                            error = %e,
                            "framed.send error during IncrementalAlterConfigs, closing"
                        );
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "IncrementalAlterConfigs dispatch error, closing connection"
                    );
                    break;
                }
            }
        }
        // DeleteRecords (21, slice-13 T15) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic in the request for `Delete` and emit
        // TOPIC_AUTHORIZATION_FAILED on the per-partition rows of any
        // topic that comes back `Deny`. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts
        // inline.
        if peek_api_key(&frame).ok() == Some(21) {
            match handle_delete_records_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteRecords, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteRecords dispatch error, closing connection");
                    break;
                }
            }
        }
        // CreatePartitions (37, slice-13 T15) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // batch-authorize every topic in the request for `Alter` and emit
        // TOPIC_AUTHORIZATION_FAILED on the topic row of any topic that
        // comes back `Deny`. The `&Broker`-only handler table signature
        // can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(37) {
            match handle_create_partitions_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during CreatePartitions, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CreatePartitions dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeGroups (15, slice-13 T16) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Describe` per group and emit
        // GROUP_AUTHORIZATION_FAILED on denied group rows. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(15) {
            match handle_describe_groups_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeGroups, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeGroups dispatch error, closing connection");
                    break;
                }
            }
        }
        // ListGroups (16, slice-13 T16) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // silently filter out groups denied `Describe`. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(16) {
            match handle_list_groups_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ListGroups, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ListGroups dispatch error, closing connection");
                    break;
                }
            }
        }
        // DeleteGroups (42, slice-13 T16) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Delete` per group and emit
        // GROUP_AUTHORIZATION_FAILED on denied group rows. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(42) {
            match handle_delete_groups_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteGroups, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteGroups dispatch error, closing connection");
                    break;
                }
            }
        }
        // JoinGroup (11, slice-13 T17) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Read` on `Group(group_id)` and emit
        // GROUP_AUTHORIZATION_FAILED on the whole response on Deny. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(11) {
            match handle_join_group_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during JoinGroup, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "JoinGroup dispatch error, closing connection");
                    break;
                }
            }
        }
        // OffsetCommit (8, slice-13 T18) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Read` on `Group(group_id)` (whole-response deny =
        // GROUP_AUTHORIZATION_FAILED) and then per-topic `Read` (per-partition
        // deny = TOPIC_AUTHORIZATION_FAILED). The `&Broker`-only handler
        // table signature can't carry that context, so this api_key
        // intercepts inline.
        if peek_api_key(&frame).ok() == Some(8) {
            match handle_offset_commit_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during OffsetCommit, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "OffsetCommit dispatch error, closing connection");
                    break;
                }
            }
        }
        // OffsetFetch (9, slice-13 T18) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Describe` on `Group(group_id)` (whole-response deny =
        // GROUP_AUTHORIZATION_FAILED) and then per-topic `Read` (per-topic
        // deny = TOPIC_AUTHORIZATION_FAILED). The `topics: None` fetch-all
        // sentinel also applies the per-topic check across committed-offsets
        // topics. The `&Broker`-only handler table signature can't carry that
        // context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(9) {
            match handle_offset_fetch_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during OffsetFetch, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "OffsetFetch dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeCluster (60, slice-13 T19) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Describe` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on the whole response on Deny. The
        // `&Broker`-only handler table signature can't carry that context,
        // so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(60) {
            match handle_describe_cluster_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeCluster, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeCluster dispatch error, closing connection");
                    break;
                }
            }
        }
        // DescribeAcls (29, slice-13 T7) needs both the authenticated
        // principal AND the peer's `SocketAddr` for host-based ACL
        // matching; neither is reachable from the `&Broker`-only handler
        // table signature, so it intercepts inline.
        if peek_api_key(&frame).ok() == Some(29) {
            match handle_describe_acls_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DescribeAcls, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeAcls dispatch error, closing connection");
                    break;
                }
            }
        }
        // CreateAcls (30, slice-13 T8) — same shape as DescribeAcls: needs
        // both the authenticated principal and the peer `SocketAddr` for
        // host-based ACL matching on the `Alter` cluster gate.
        if peek_api_key(&frame).ok() == Some(30) {
            match handle_create_acls_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during CreateAcls, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "CreateAcls dispatch error, closing connection");
                    break;
                }
            }
        }
        // DeleteAcls (31, slice-13 T9) — same shape as CreateAcls: needs
        // both the authenticated principal and the peer `SocketAddr` for
        // host-based ACL matching on the `Alter` cluster gate.
        if peek_api_key(&frame).ok() == Some(31) {
            match handle_delete_acls_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during DeleteAcls, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DeleteAcls dispatch error, closing connection");
                    break;
                }
            }
        }
        // ElectLeaders (43, slice-14 T5) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Alter` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED on every per-partition row on Deny.
        // The `&Broker`-only handler table signature can't carry that
        // context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(43) {
            match handle_elect_leaders_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ElectLeaders, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ElectLeaders dispatch error, closing connection");
                    break;
                }
            }
        }
        // AlterPartitionReassignments (45, slice-15 T7) needs both the
        // authenticated principal AND the peer's `SocketAddr` so the handler
        // can authorize `Alter` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(45) {
            match handle_alter_partition_reassignments_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AlterPartitionReassignments, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AlterPartitionReassignments dispatch error, closing connection");
                    break;
                }
            }
        }
        // ListPartitionReassignments (46, slice-15 T7) needs both the
        // authenticated principal AND the peer's `SocketAddr` so the handler
        // can authorize `Describe` on `Cluster("kafka-cluster")` and emit
        // CLUSTER_AUTHORIZATION_FAILED. The `&Broker`-only handler table
        // signature can't carry that context, so this api_key intercepts inline.
        if peek_api_key(&frame).ok() == Some(46) {
            match handle_list_partition_reassignments_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during ListPartitionReassignments, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "ListPartitionReassignments dispatch error, closing connection");
                    break;
                }
            }
        }
        // InitProducerId (22, slice-13 T20) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Write` on `TransactionalId` (transactional path) or
        // `IdempotentWrite` on `Cluster` (idempotent-only path). On Deny
        // the handler returns a whole-response error_code = 53 or 31.
        if peek_api_key(&frame).ok() == Some(22) {
            match handle_init_producer_id_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during InitProducerId, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "InitProducerId dispatch error, closing connection");
                    break;
                }
            }
        }
        // AddPartitionsToTxn (24, slice-13 T20) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Write` on `TransactionalId` (whole-txn deny =
        // TRANSACTIONAL_ID_AUTHORIZATION_FAILED) and per-topic `Write` on
        // `Topic` (per-row deny = TOPIC_AUTHORIZATION_FAILED).
        if peek_api_key(&frame).ok() == Some(24) {
            match handle_add_partitions_to_txn_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during AddPartitionsToTxn, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "AddPartitionsToTxn dispatch error, closing connection");
                    break;
                }
            }
        }
        // EndTxn (26, slice-13 T20) needs both the authenticated principal
        // AND the peer's `SocketAddr` so the handler can authorize
        // `Write` on `TransactionalId`. On Deny → whole-response
        // TRANSACTIONAL_ID_AUTHORIZATION_FAILED.
        if peek_api_key(&frame).ok() == Some(26) {
            match handle_end_txn_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during EndTxn, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "EndTxn dispatch error, closing connection");
                    break;
                }
            }
        }
        // TxnOffsetCommit (28, slice-13 T20) needs both the authenticated
        // principal AND the peer's `SocketAddr` so the handler can
        // authorize `Write` on `TransactionalId` + `Read` on `Group` +
        // per-topic `Read` on `Topic`.
        if peek_api_key(&frame).ok() == Some(28) {
            match handle_txn_offset_commit_frame(&broker, &frame, &auth, &peer).await {
                Ok(bytes) => {
                    if let Err(e) = framed.send(bytes).await {
                        tracing::warn!(error = %e, "framed.send error during TxnOffsetCommit, closing");
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "TxnOffsetCommit dispatch error, closing connection");
                    break;
                }
            }
        }
        let response_bytes = match dispatch_one(&broker, &frame).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "dispatch error, closing connection");
                break;
            }
        };
        if let Err(e) = framed.send(response_bytes).await {
            tracing::warn!(error = %e, "framed.send error, closing");
            break;
        }
    }
    tracing::info!("connection closed");
}

/// Outcome of intercepting a SASL frame: the bytes to write back to the
/// peer and whether the dispatcher should close the connection after the
/// send completes (used for `SaslAuthenticate` failures + illegal state).
struct SaslFrameOutcome {
    response_bytes: Bytes,
    close_after: bool,
}

/// If `frame` is a `SaslHandshake` (17) or `SaslAuthenticate` (36) request,
/// handle it inline (mutating `auth`) and return a [`SaslFrameOutcome`].
/// Returns `None` for every other `api_key` — the caller falls through to the
/// regular handler-table dispatch in [`dispatch_one`].
///
/// Errors here close the connection (protocol violations, e.g. an
/// undecodable header).
fn try_handle_sasl_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &mut crate::network::auth::ConnectionAuth,
) -> Option<Result<SaslFrameOutcome, BrokerError>> {
    let api_key = peek_api_key(frame).ok()?;
    if api_key != 17 && api_key != 36 {
        return None;
    }
    Some(handle_sasl_frame(broker, frame, auth, api_key))
}

fn handle_sasl_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &mut crate::network::auth::ConnectionAuth,
    api_key: i16,
) -> Result<SaslFrameOutcome, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (parsed_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(parsed_key, api_key);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let (resp_body, close_after) = match api_key {
        17 => {
            let mut cur: &[u8] = body;
            let req = crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest::decode(
                &mut cur,
                api_version,
            )?;
            let resp = crate::network::auth::handle_handshake(
                &req,
                auth,
                &broker.config.enabled_sasl_mechanisms,
            );
            let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
            resp.encode(&mut buf, api_version)?;
            (buf.freeze(), false)
        }
        36 => {
            let mut cur: &[u8] = body;
            let req =
                crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest::decode(
                    &mut cur,
                    api_version,
                )?;
            // Must be in `Negotiating` state (i.e. SaslHandshake was the
            // previous frame). Otherwise return ILLEGAL_SASL_STATE (34) and
            // close.
            let mech_opt = match auth {
                crate::network::auth::ConnectionAuth::Negotiating { mechanism, .. } => {
                    Some(*mechanism)
                }
                _ => None,
            };
            let resp = if let Some(mech) = mech_opt {
                match mech {
                    crabka_security::SaslMechanism::Plain => {
                        crate::network::auth::handle_authenticate_plain(
                            &req,
                            auth,
                            &broker.config.plain_credentials,
                        )
                    }
                    crabka_security::SaslMechanism::ScramSha512 => {
                        crate::network::auth::handle_authenticate_scram(
                            &req,
                            auth,
                            &broker.controller,
                        )
                    }
                }
            } else {
                crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
                    error_code: codes::ILLEGAL_SASL_STATE,
                    error_message: Some("SaslAuthenticate without prior SaslHandshake".into()),
                    auth_bytes: Bytes::new(),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }
            };
            let close = resp.error_code != 0;
            let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
            resp.encode(&mut buf, api_version)?;
            (buf.freeze(), close)
        }
        _ => unreachable!("filtered by caller to 17 / 36 only"),
    };

    let response_bytes = encode_response(api_key, correlation_id, body_flexible, &resp_body);
    Ok(SaslFrameOutcome {
        response_bytes,
        close_after,
    })
}

/// Decode + dispatch an `AlterUserScramCredentials` (`api_key` 51) frame.
/// Pulls the authenticated principal off the per-connection `auth` state and
/// the peer `SocketAddr` from the accept-time capture so the handler can
/// enforce the Cluster Alter ACL gate (slice-13 T19). On a SASL listener the
/// pre-auth allowlist already rejects this frame; on PLAINTEXT/SSL listeners
/// the connection is implicitly `Authenticated { ANONYMOUS / Plain }` (see
/// the loop init), so `principal()` always returns `Some` here.
async fn handle_alter_user_scram_credentials_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::{Decode, Encode};

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 51);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req =
        crabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
            &mut cur, api_version,
        )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp =
        crate::handlers::alter_user_scram_credentials::handle(broker, req, &principal, peer).await;
    let mut buf = BytesMut::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut buf, api_version)?;
    let resp_body = buf.freeze();
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeCluster` (`api_key` 60) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` on `Cluster("kafka-cluster")` (slice-13 T19).
/// On Deny the whole response receives `CLUSTER_AUTHORIZATION_FAILED`.
async fn handle_describe_cluster_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 60);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::describe_cluster::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `Produce` (`api_key` 0) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Write` (slice-13 T10).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_produce_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 0);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::produce::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `Fetch` (`api_key` 1) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Read` (slice-13 T11).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_fetch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 1);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body =
        crate::handlers::fetch::handle(broker, api_version, correlation_id, body, &principal, peer)
            .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `Metadata` (`api_key` 3) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every candidate topic for `Describe` (slice-13 T12).
/// Named-topic Deny surfaces `TOPIC_AUTHORIZATION_FAILED`; fetch-all Deny
/// silently omits the topic. On PLAINTEXT/SSL listeners the connection
/// is implicitly `Authenticated { ANONYMOUS / Plain }` (see the loop
/// init), so `principal()` always returns `Some` here; the
/// `unwrap_or_else` fallback covers the defensive SASL pre-auth case.
async fn handle_metadata_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 3);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::metadata::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `CreateTopics` (`api_key` 19) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Create` on `Cluster("kafka-cluster")` (slice-13 T13).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_create_topics_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 19);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::create_topics::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteTopics` (`api_key` 20) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic for `Delete` (slice-13 T13).
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_delete_topics_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 20);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::delete_topics::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeAcls` (`api_key` 29) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` on `Cluster` with host-based ACL matching.
/// On PLAINTEXT/SSL listeners the connection is implicitly
/// `Authenticated { ANONYMOUS / Plain }` (see the loop init), so
/// `principal()` always returns `Some` here; the `unwrap_or_else`
/// fallback covers the defensive SASL pre-auth case.
async fn handle_describe_acls_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 29);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::describe_acls_request::DescribeAclsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body =
        crate::handlers::describe_acls::handle(broker, req, &principal, peer, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `CreateAcls` (`api_key` 30) frame. Mirrors
/// [`handle_describe_acls_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from
/// the accept-time capture so the handler can authorize `Alter` on
/// `Cluster` with host-based ACL matching.
async fn handle_create_acls_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 30);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::create_acls_request::CreateAclsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body =
        crate::handlers::create_acls::handle(broker, req, &principal, peer, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteAcls` (`api_key` 31) frame. Mirrors
/// [`handle_create_acls_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from
/// the accept-time capture so the handler can authorize `Alter` on
/// `Cluster` with host-based ACL matching.
async fn handle_delete_acls_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 31);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::delete_acls_request::DeleteAclsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body =
        crate::handlers::delete_acls::handle(broker, req, &principal, peer, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `ElectLeaders` (`api_key` 43) frame. Mirrors
/// [`handle_delete_acls_frame`] — pulls the authenticated principal off the
/// per-connection `auth` state and the peer `SocketAddr` from the
/// accept-time capture so the handler can authorize `Alter` on
/// `Cluster("kafka-cluster")` with host-based ACL matching.
async fn handle_elect_leaders_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 43);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::elect_leaders_request::ElectLeadersRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body =
        crate::handlers::elect_leaders::handle(broker, req, &principal, peer, api_version).await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AlterPartitionReassignments` (`api_key` 45) frame.
/// Mirrors [`handle_elect_leaders_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from the
/// accept-time capture so the handler can authorize `Alter` on
/// `Cluster("kafka-cluster")` with host-based ACL matching.
async fn handle_alter_partition_reassignments_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 45);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::alter_partition_reassignments::handle(
        broker,
        req,
        &principal,
        peer,
        api_version,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `ListPartitionReassignments` (`api_key` 46) frame.
/// Mirrors [`handle_elect_leaders_frame`] — pulls the authenticated principal
/// off the per-connection `auth` state and the peer `SocketAddr` from the
/// accept-time capture so the handler can authorize `Describe` on
/// `Cluster("kafka-cluster")` with host-based ACL matching.
async fn handle_list_partition_reassignments_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    use crabka_protocol::Decode;

    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 46);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let mut cur: &[u8] = body;
    let req = crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest::decode(
        &mut cur,
        api_version,
    )?;

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::list_partition_reassignments::handle(
        broker,
        req,
        &principal,
        peer,
        api_version,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AlterConfigs` (`api_key` 33) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `AlterConfigs` per resource (slice-13 T14).
/// Topic resources → `TOPIC_AUTHORIZATION_FAILED` on Deny.
/// Broker resources → `CLUSTER_AUTHORIZATION_FAILED` on Deny.
async fn handle_alter_configs_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 33);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::alter_configs::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `IncrementalAlterConfigs` (`api_key` 44) frame.
/// Pulls the authenticated principal off the per-connection `auth` state
/// and the peer `SocketAddr` from the accept-time capture so the handler
/// can authorize `AlterConfigs` per resource (slice-13 T14).
/// Topic resources → `TOPIC_AUTHORIZATION_FAILED` on Deny.
/// Broker resources → `CLUSTER_AUTHORIZATION_FAILED` on Deny.
async fn handle_incremental_alter_configs_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 44);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::incremental_alter_configs::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteRecords` (`api_key` 21) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Delete` (slice-13 T15).
/// Topics that come back `Deny` have `TOPIC_AUTHORIZATION_FAILED` set on
/// every partition row.
async fn handle_delete_records_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 21);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::delete_records::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `CreatePartitions` (`api_key` 37) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// batch-authorize every topic in the request for `Alter` (slice-13 T15).
/// Topics that come back `Deny` receive `TOPIC_AUTHORIZATION_FAILED` on
/// that topic row.
async fn handle_create_partitions_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 37);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::create_partitions::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DescribeGroups` (`api_key` 15) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` per group (slice-13 T16). Denied groups receive
/// `GROUP_AUTHORIZATION_FAILED` on their per-group entry.
async fn handle_describe_groups_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 15);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::describe_groups::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `ListGroups` (`api_key` 16) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// silently filter out groups denied `Describe` (slice-13 T16). Denied
/// groups are omitted from the response without an error code.
async fn handle_list_groups_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 16);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::list_groups::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `DeleteGroups` (`api_key` 42) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Delete` per group (slice-13 T16). Denied groups receive
/// `GROUP_AUTHORIZATION_FAILED` on their per-group entry.
async fn handle_delete_groups_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 42);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::delete_groups::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `JoinGroup` (`api_key` 11) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Read` on `Group(group_id)` (slice-13 T17). On Deny the
/// whole response receives `GROUP_AUTHORIZATION_FAILED`.
async fn handle_join_group_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 11);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::join_group::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `OffsetCommit` (`api_key` 8) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Read` on `Group(group_id)` (whole-response deny =
/// `GROUP_AUTHORIZATION_FAILED`) and per-topic `Read` (per-partition deny =
/// `TOPIC_AUTHORIZATION_FAILED`) (slice-13 T18).
async fn handle_offset_commit_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 8);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::offset_commit::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `OffsetFetch` (`api_key` 9) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Describe` on `Group(group_id)` (whole-response deny =
/// `GROUP_AUTHORIZATION_FAILED`) and per-topic `Read` (per-topic deny =
/// `TOPIC_AUTHORIZATION_FAILED`). The `topics: None` fetch-all sentinel runs
/// the per-topic check across discovered committed-offsets topics (slice-13 T18).
async fn handle_offset_fetch_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 9);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::offset_fetch::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `InitProducerId` (`api_key` 22) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId(transactional_id)` (transactional
/// path) or `IdempotentWrite` on `Cluster("kafka-cluster")` (idempotent-only
/// path) (slice-13 T20).
async fn handle_init_producer_id_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 22);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::init_producer_id::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `AddPartitionsToTxn` (`api_key` 24) frame. Pulls
/// the authenticated principal off the per-connection `auth` state and
/// the peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId` AND per-topic `Write` on
/// `Topic` (slice-13 T20).
async fn handle_add_partitions_to_txn_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 24);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::txn::handlers::add_partitions_to_txn::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch an `EndTxn` (`api_key` 26) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId` (slice-13 T20).
async fn handle_end_txn_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 26);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::txn::handlers::end_txn::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Decode + dispatch a `TxnOffsetCommit` (`api_key` 28) frame. Pulls the
/// authenticated principal off the per-connection `auth` state and the
/// peer `SocketAddr` from the accept-time capture so the handler can
/// authorize `Write` on `TransactionalId` + `Read` on `Group` +
/// per-topic `Read` on `Topic` (slice-13 T20).
async fn handle_txn_offset_commit_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 28);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::txn::handlers::txn_offset_commit::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(
        api_key,
        correlation_id,
        body_flexible,
        &resp_body,
    ))
}

/// Read just the `api_key` (first 2 bytes) of a request frame without
/// otherwise consuming or validating it. Used by the pre-auth gate so we
/// can decide whether to even dispatch the frame.
fn peek_api_key(frame: &[u8]) -> Result<i16, BrokerError> {
    if frame.len() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue(
                "request frame: too short to peek api_key",
            ),
        ));
    }
    Ok(i16::from_be_bytes([frame[0], frame[1]]))
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
        15 => version >= owned::describe_groups_request::FLEXIBLE_MIN,
        16 => version >= owned::list_groups_request::FLEXIBLE_MIN,
        // SaslHandshake (17) is permanently non-flexible (its
        // `FLEXIBLE_MIN` is `i16::MAX` in the upstream schema); covered
        // by the `_ => false` arm below.
        18 => version >= owned::api_versions_request::FLEXIBLE_MIN,
        19 => version >= owned::create_topics_request::FLEXIBLE_MIN,
        20 => version >= owned::delete_topics_request::FLEXIBLE_MIN,
        21 => version >= owned::delete_records_request::FLEXIBLE_MIN,
        22 => version >= owned::init_producer_id_request::FLEXIBLE_MIN,
        23 => version >= owned::offset_for_leader_epoch_request::FLEXIBLE_MIN,
        24 => version >= owned::add_partitions_to_txn_request::FLEXIBLE_MIN,
        25 => version >= owned::add_offsets_to_txn_request::FLEXIBLE_MIN,
        26 => version >= owned::end_txn_request::FLEXIBLE_MIN,
        27 => version >= owned::write_txn_markers_request::FLEXIBLE_MIN,
        28 => version >= owned::txn_offset_commit_request::FLEXIBLE_MIN,
        29 => version >= owned::describe_acls_request::FLEXIBLE_MIN,
        30 => version >= owned::create_acls_request::FLEXIBLE_MIN,
        31 => version >= owned::delete_acls_request::FLEXIBLE_MIN,
        32 => version >= owned::describe_configs_request::FLEXIBLE_MIN,
        33 => version >= owned::alter_configs_request::FLEXIBLE_MIN,
        36 => version >= owned::sasl_authenticate_request::FLEXIBLE_MIN,
        37 => version >= owned::create_partitions_request::FLEXIBLE_MIN,
        42 => version >= owned::delete_groups_request::FLEXIBLE_MIN,
        43 => version >= owned::elect_leaders_request::FLEXIBLE_MIN,
        44 => version >= owned::incremental_alter_configs_request::FLEXIBLE_MIN,
        45 => version >= owned::alter_partition_reassignments_request::FLEXIBLE_MIN,
        46 => version >= owned::list_partition_reassignments_request::FLEXIBLE_MIN,
        // AlterUserScramCredentials (KIP-554, slice 12 T15) is flexible from v0.
        51 => version >= owned::alter_user_scram_credentials_request::FLEXIBLE_MIN,
        56 => version >= owned::alter_partition_request::FLEXIBLE_MIN,
        60 => version >= owned::describe_cluster_request::FLEXIBLE_MIN,
        63 => version >= owned::broker_heartbeat_request::FLEXIBLE_MIN,
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
    fn peek_api_key_reads_first_two_bytes_big_endian() {
        // api_key=18, version=3, corr_id=1 — only first 2 bytes are inspected.
        let mut buf = BytesMut::new();
        buf.put_i16(18);
        buf.put_i16(3);
        buf.put_i32(1);
        assert_eq!(peek_api_key(&buf).unwrap(), 18);
    }

    #[test]
    fn peek_api_key_rejects_short_frame() {
        let buf = [0u8; 1];
        assert!(peek_api_key(&buf).is_err());
    }

    #[test]
    fn encode_response_other_flexible_inserts_tagged_byte() {
        let body = [0u8, 0u8];
        let out = encode_response(3, 7, true, &body);
        assert_eq!(out.len(), 5 + body.len());
        assert_eq!(out[4], 0); // tagged byte
    }
}
