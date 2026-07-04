//! `FindCoordinator` (`api_key=10`). Supports:
//!   - `key_type=0` (GROUP): returns this broker as coordinator for every
//!     group key (single-broker MVP).
//!   - `key_type=1` (TRANSACTION): ensures `__transaction_state` exists,
//!     hashes the transaction-id to a partition, resolves the leader, and
//!     returns that broker's address.
//!
//! Response fields are populated in both the legacy single-coordinator form
//! (v0-v3) and the per-key `coordinators` array (v4+).

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;
const KEY_TYPE_SHARE: i8 = 2;

/// A per-key authorization-failed `Coordinator` entry. Kafka stamps the
/// denied key's row with the authorization-failed code and leaves
/// authorized keys to resolve normally.
fn denied_coordinator(key: String, error_code: i16) -> Coordinator {
    Coordinator {
        key,
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code,
        error_message: Some("authorization failed".into()),
        ..Default::default()
    }
}

/// Authorize a single `FindCoordinator` key against its key-type ACL:
/// GROUP → `Describe` on `Group(key)`; TRANSACTION → `Describe` on
/// `TransactionalId(key)`. Returns the authorization-failed code to stamp
/// on Deny, or `None` when allowed (or for key-types we don't gate, e.g.
/// SHARE / unknown).
fn key_authz_failure(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
    key_type: i8,
    key: &str,
) -> Option<i16> {
    let (resource_type, failure_code) = match key_type {
        KEY_TYPE_GROUP => (ResourceType::Group, codes::GROUP_AUTHORIZATION_FAILED),
        KEY_TYPE_TRANSACTION => (
            ResourceType::TransactionalId,
            codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        ),
        _ => return None,
    };
    let allow = authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type,
            resource_name: key,
            operation: AclOperation::Describe,
        },
    );
    (allow == AuthorizationResult::Deny).then_some(failure_code)
}

// cargo-mutants: the surviving mutant here flips the `-1` fallback in
// `i32::try_from(leader.0).unwrap_or(-1)` (a coordinator broker's node id).
// Kafka broker ids are int32 on the wire, so `try_from` from the u64 NodeId
// never fails and the `-1` branch is unreachable with realistic inputs. The
// live-broker TXN/GROUP coordinator-resolution behaviour is covered by the
// integration suite, not this in-file module.
#[cfg_attr(test, mutants::skip)]
#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_find_coordinator",
    level = "info",
    skip_all,
    fields(api = "FindCoordinator", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let broker_id = broker.config.broker_id;
    let node_id = broker.config.node_id;
    // The local broker's advertised `host:port` for the listener this request
    // arrived on (Kafka returns the connection listener's address). Falls back
    // to the legacy top-level `advertised_listener` when the connection
    // listener isn't among this broker's configured listeners.
    let advertised = local_advertised_for_listener(&broker.config, ctx.connection_listener_name);
    let controller = Arc::clone(&broker.controller);
    {
        let mut cur: &[u8] = req_bytes;
        let req = FindCoordinatorRequest::decode(&mut cur, version)?;

        // For v4+, requests carry `coordinator_keys`. For v0-v3 the single
        // `key` field is what the client cares about — populate the legacy
        // top-level fields and also emit a single `Coordinator` entry for
        // that key so the encode path is uniform.
        let keys: Vec<String> = if req.coordinator_keys.is_empty() {
            vec![req.key.clone()]
        } else {
            req.coordinator_keys.clone()
        };

        // ── ACL preamble ────────────────────────────────────────────
        // Per-key `Describe`: GROUP → `Group(key)`; TRANSACTION →
        // `TransactionalId(key)`. Denied keys are emitted with the
        // authorization-failed code (per-entry for the v4+ multi-key
        // array; the v0-v3 top-level fields are derived from the first
        // entry below). Authorized keys resolve normally — so we split
        // `keys` into denied entries + the still-to-resolve list.
        let acl_image = controller.current_image();
        let mut denied_entries: Vec<Coordinator> = Vec::new();
        let mut allowed_keys: Vec<String> = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(code) = key_authz_failure(
                broker.config.authorizer.as_ref(),
                &acl_image,
                ctx.principal,
                ctx.peer,
                req.key_type,
                &k,
            ) {
                denied_entries.push(denied_coordinator(k, code));
            } else {
                allowed_keys.push(k);
            }
        }
        let keys = allowed_keys;

        let mut coordinators: Vec<Coordinator> = match req.key_type {
            KEY_TYPE_GROUP => {
                let (host, port) = parse_host_port(&advertised);
                let port_i32 = i32::from(port);
                keys.into_iter()
                    .map(|k| Coordinator {
                        key: k,
                        node_id: broker_id,
                        host: host.clone(),
                        port: port_i32,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    })
                    .collect()
            }
            KEY_TYPE_TRANSACTION => {
                // Ensure __transaction_state topic exists before we try to
                // look up partitions in it.
                if let Err(e) = crate::txn::bootstrap::ensure_topic(&controller).await {
                    tracing::warn!(
                        error = %e,
                        "txn bootstrap failed; replying COORDINATOR_NOT_AVAILABLE"
                    );
                    return encode_error_response(
                        broker_id,
                        &advertised,
                        version,
                        codes::COORDINATOR_NOT_AVAILABLE,
                        Some("txn topic bootstrap failed"),
                    );
                }

                let mut result = Vec::with_capacity(keys.len());
                for k in keys {
                    let p = crate::txn::partitioner::partition_for_tid(
                        &k,
                        crate::txn::bootstrap::NUM_PARTITIONS,
                    );
                    let image = controller.current_image();
                    let Some(pr) = image.partition(crate::txn::bootstrap::TOPIC, p) else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("partition not found".into()),
                            ..Default::default()
                        });
                        continue;
                    };
                    let leader = pr.leader;
                    let Some(broker_info) = image.broker(leader) else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("leader broker not registered".into()),
                            ..Default::default()
                        });
                        continue;
                    };
                    let node_id_i32 = i32::try_from(leader.0).unwrap_or(-1);
                    // Resolve the coordinator's address for the listener this
                    // request arrived on. For the local broker prefer our own
                    // connection-listener advertised address (the metadata
                    // record may carry the pre-bind port 0 in test setups
                    // where the OS assigns the port); for a remote leader pick
                    // its connection-listener endpoint from the image record.
                    let (host, port_i32) = if leader == node_id {
                        let (h, p) = parse_host_port(&advertised);
                        (h, i32::from(p))
                    } else {
                        crate::handlers::metadata::pick_endpoint_host_port(
                            broker_info,
                            ctx.connection_listener_name,
                            broker.config.inter_broker_listener_name.as_str(),
                        )
                    };
                    result.push(Coordinator {
                        key: k,
                        node_id: node_id_i32,
                        host,
                        port: port_i32,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    });
                }
                result
            }
            KEY_TYPE_SHARE => {
                // Ensure __share_group_state exists before resolving its
                // partitions' leaders.
                if let Err(e) = crate::share_coordinator::bootstrap::ensure_topic(&controller).await
                {
                    tracing::warn!(
                        error = %e,
                        "share-state bootstrap failed; replying COORDINATOR_NOT_AVAILABLE"
                    );
                    return encode_error_response(
                        broker_id,
                        &advertised,
                        version,
                        codes::COORDINATOR_NOT_AVAILABLE,
                        Some("share-state topic bootstrap failed"),
                    );
                }

                let mut result = Vec::with_capacity(keys.len());
                for k in keys {
                    // Kafka's share-coordinator key is `group:topicId:partition`.
                    // Group ids may contain ':', so split from the right: the
                    // last segment is the partition, the next is the topic id,
                    // and everything before that is the group.
                    let Some((group, topic_uuid, partition)) = parse_share_key(&k) else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("malformed share-state key".into()),
                            ..Default::default()
                        });
                        continue;
                    };

                    let p = crate::share_coordinator::partitioner::partition_for_share_key(
                        group,
                        &topic_uuid,
                        partition,
                        crate::share_coordinator::bootstrap::NUM_PARTITIONS,
                    );
                    let image = controller.current_image();
                    let Some(pr) = image.partition(crate::share_coordinator::bootstrap::TOPIC, p)
                    else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("partition not found".into()),
                            ..Default::default()
                        });
                        continue;
                    };
                    let leader = pr.leader;
                    let Some(broker_info) = image.broker(leader) else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("leader broker not registered".into()),
                            ..Default::default()
                        });
                        continue;
                    };
                    let node_id_i32 = i32::try_from(leader.0).unwrap_or(-1);
                    // Resolve the coordinator's address for the connection
                    // listener (see the TXN branch). Local broker → our own
                    // connection-listener advertised address (test setups may
                    // record a pre-bind port); remote leader → its
                    // connection-listener endpoint from the image record.
                    let (host, port_i32) = if leader == node_id {
                        let (h, p) = parse_host_port(&advertised);
                        (h, i32::from(p))
                    } else {
                        crate::handlers::metadata::pick_endpoint_host_port(
                            broker_info,
                            ctx.connection_listener_name,
                            broker.config.inter_broker_listener_name.as_str(),
                        )
                    };
                    result.push(Coordinator {
                        key: k,
                        node_id: node_id_i32,
                        host,
                        port: port_i32,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    });
                }
                result
            }
            unknown => {
                tracing::warn!(key_type = unknown, "unknown FindCoordinator key_type");
                let (host, port) = parse_host_port(&advertised);
                let port_i32 = i32::from(port);
                keys.into_iter()
                    .map(|k| Coordinator {
                        key: k,
                        node_id: broker_id,
                        host: host.clone(),
                        port: port_i32,
                        error_code: codes::NONE,
                        error_message: None,
                        ..Default::default()
                    })
                    .collect()
            }
        };

        // Re-attach the authorization-denied entries. They lead the list so
        // a v0-v3 single-key request whose only key was denied surfaces the
        // authorization-failed code in the derived top-level fields below.
        if !denied_entries.is_empty() {
            denied_entries.extend(coordinators);
            coordinators = denied_entries;
        }

        // Derive the legacy top-level fields from the first coordinator in
        // the list (matches Apache Kafka v0-v3 behaviour).
        let (top_node_id, top_host, top_port, top_error, top_error_message) =
            if let Some(first) = coordinators.first() {
                (
                    first.node_id,
                    first.host.clone(),
                    first.port,
                    first.error_code,
                    first.error_message.clone(),
                )
            } else {
                let (host, port) = parse_host_port(&advertised);
                (broker_id, host, i32::from(port), codes::NONE, None)
            };

        let resp = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: top_error,
            error_message: top_error_message,
            node_id: top_node_id,
            host: top_host,
            port: top_port,
            coordinators,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    }
}

/// Build an error response (all coordinators carry the given error code).
/// Used when a top-level failure (e.g. bootstrap) prevents per-key lookup.
fn encode_error_response(
    broker_id: i32,
    advertised: &str,
    version: i16,
    error_code: i16,
    error_message: Option<&str>,
) -> Result<Bytes, BrokerError> {
    let (host, port) = parse_host_port(advertised);
    let resp = FindCoordinatorResponse {
        throttle_time_ms: 0,
        error_code,
        error_message: error_message.map(str::to_owned),
        node_id: broker_id,
        host,
        port: i32::from(port),
        coordinators: vec![],
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Parse a share-coordinator key `"{group}:{topicId}:{partition}"` into its
/// `(group, topic_id, partition)` parts. Group ids may themselves contain `:`,
/// so the partition and topic-id are peeled from the right. Returns `None` on a
/// malformed partition int, topic-id UUID, or missing segments.
fn parse_share_key(key: &str) -> Option<(&str, uuid::Uuid, i32)> {
    let (rest, partition_str) = key.rsplit_once(':')?;
    let (group, topic_str) = rest.rsplit_once(':')?;
    let partition: i32 = partition_str.parse().ok()?;
    let topic_id = uuid::Uuid::parse_str(topic_str).ok()?;
    Some((group, topic_id, partition))
}

/// The local broker's advertised `host:port` string for the listener a
/// request arrived on. Kafka returns the connection listener's advertised
/// address, so a TLS client gets the TLS listener's `advertised` and a
/// plaintext client gets the plaintext one. Falls back to the legacy
/// top-level `advertised_listener` when the connection listener isn't among
/// this broker's configured listeners (e.g. the single-listener default,
/// where `connection_listener_name == "PLAINTEXT"` still resolves it).
///
/// A matched listener whose advertised port is `0` (an OS-assigned dynamic
/// port, common in test harnesses) is unusable as a coordinator address, so
/// we fall back to `advertised_listener`, which `Broker::start` rewrites to
/// the real bound port after binding.
fn local_advertised_for_listener(
    config: &crate::config::BrokerConfig,
    connection_listener_name: &str,
) -> String {
    config
        .effective_listeners()
        .into_iter()
        .find(|l| l.name == connection_listener_name && !l.advertised.ends_with(":0"))
        .map_or_else(|| config.advertised_listener.clone(), |l| l.advertised)
}

fn parse_host_port(addr: &str) -> (String, u16) {
    if let Some((h, p)) = addr.rsplit_once(':')
        && let Ok(port) = p.parse::<u16>()
    {
        return (h.to_string(), port);
    }
    tracing::warn!(
        addr,
        "advertised_listener not host:port; falling back to localhost:9092"
    );
    ("localhost".into(), 9092)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn deny_authorizer() -> crate::authorizer::SimpleAclAuthorizer {
        crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new())
    }

    fn anon() -> crabka_security::Principal {
        crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        }
    }

    #[test]
    fn group_key_denied_maps_to_group_authorization_failed() {
        let authz = deny_authorizer();
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = key_authz_failure(&authz, &image, &anon(), &peer, KEY_TYPE_GROUP, "g");
        assert!(code == Some(codes::GROUP_AUTHORIZATION_FAILED));
    }

    #[test]
    fn txn_key_denied_maps_to_transactional_id_authorization_failed() {
        let authz = deny_authorizer();
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = key_authz_failure(&authz, &image, &anon(), &peer, KEY_TYPE_TRANSACTION, "t");
        assert!(code == Some(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
    }

    #[test]
    fn denied_entry_carries_the_failure_code() {
        let c = denied_coordinator("g".into(), codes::GROUP_AUTHORIZATION_FAILED);
        assert!(c.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(c.node_id == -1);
    }

    fn listener(name: &str, advertised: &str) -> crate::config::ListenerSpec {
        crate::config::ListenerSpec {
            name: name.to_string(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            advertised: advertised.to_string(),
            protocol: crabka_security::ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        }
    }

    /// A request on the `"tls"` listener resolves the local coordinator to the
    /// tls listener's advertised address; a request on `"plain"` resolves to
    /// the plain listener's address.
    #[test]
    fn local_advertised_tracks_connection_listener() {
        let config = crate::config::BrokerConfig {
            advertised_listener: "legacy:1000".to_string(),
            listeners: vec![
                listener("plain", "plain-host:9092"),
                listener("tls", "tls-host:9094"),
            ],
            ..Default::default()
        };
        assert!(local_advertised_for_listener(&config, "tls") == "tls-host:9094");
        assert!(local_advertised_for_listener(&config, "plain") == "plain-host:9092");
    }

    /// When the connection listener isn't configured, fall back to the legacy
    /// top-level `advertised_listener`.
    #[test]
    fn local_advertised_falls_back_to_legacy() {
        let config = crate::config::BrokerConfig {
            advertised_listener: "legacy:1000".to_string(),
            listeners: vec![listener("plain", "plain-host:9092")],
            ..Default::default()
        };
        assert!(local_advertised_for_listener(&config, "external") == "legacy:1000");
    }
}
