//! Kafka broker/controller lifecycle RPCs served on the controller listener.

use std::collections::{BTreeMap, HashSet};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use crabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, ControllerRegistrationRecord, MetadataRecord, NodeId,
};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        broker_heartbeat_request::{self, BrokerHeartbeatRequest},
        broker_heartbeat_response::BrokerHeartbeatResponse,
        broker_registration_request::{self, BrokerRegistrationRequest},
        broker_registration_response::BrokerRegistrationResponse,
        controller_registration_request::{self, ControllerRegistrationRequest},
        controller_registration_response::ControllerRegistrationResponse,
    },
};
use crabka_security::ListenerProtocol;

use crate::{RaftError, kraft::KraftController};

const SUCCESS: i16 = 0;
const UNKNOWN_SERVER_ERROR: i16 = -1;
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const UNSUPPORTED_VERSION: i16 = 35;
const NOT_CONTROLLER: i16 = 41;
const STALE_BROKER_EPOCH: i16 = 77;
const DUPLICATE_BROKER_REGISTRATION: i16 = 101;
const BROKER_ID_NOT_REGISTERED: i16 = 102;
const INCONSISTENT_CLUSTER_ID: i16 = 104;
const UNKNOWN_CONTROLLER_ID: i16 = 116;
const INVALID_REGISTRATION: i16 = 119;

pub(super) const SUPPORTED_APIS: [(i16, i16); 3] = [
    (
        broker_registration_request::API_KEY,
        broker_registration_request::MAX_VERSION,
    ),
    (
        broker_heartbeat_request::API_KEY,
        broker_heartbeat_request::MAX_VERSION,
    ),
    (
        controller_registration_request::API_KEY,
        controller_registration_request::MAX_VERSION,
    ),
];

pub(super) fn is_controller_api(api_key: i16) -> bool {
    SUPPORTED_APIS.iter().any(|&(key, _)| key == api_key)
}

pub(super) async fn dispatch(
    api_key: i16,
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    match api_key {
        broker_registration_request::API_KEY => {
            broker_registration(version, body, engine, authorized).await
        }
        broker_heartbeat_request::API_KEY => broker_heartbeat(version, body, engine, authorized),
        controller_registration_request::API_KEY => {
            controller_registration(version, body, engine, authorized).await
        }
        _ => Err(RaftError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("unknown controller lifecycle API"),
        )),
    }
}

async fn broker_registration(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = BrokerRegistrationRequest::decode(&mut body, version)?;
    if !authorized {
        return broker_registration_response(version, CLUSTER_AUTHORIZATION_FAILED, -1);
    }
    if !is_leader(engine) {
        return broker_registration_response(version, NOT_CONTROLLER, -1);
    }
    let image = engine.current_image();
    let node_id = match u64::try_from(request.broker_id) {
        Ok(id) => NodeId(id),
        Err(_) => return broker_registration_response(version, INVALID_REGISTRATION, -1),
    };
    if !cluster_id_matches(&request.cluster_id, image.cluster_id()) {
        return broker_registration_response(version, INCONSISTENT_CLUSTER_ID, -1);
    }
    if request.is_migrating_zk_broker {
        return broker_registration_response(version, BROKER_ID_NOT_REGISTERED, -1);
    }
    let endpoints = match decode_broker_listeners(&request.listeners) {
        Ok(endpoints) => endpoints,
        Err(code) => return broker_registration_response(version, code, -1),
    };
    if !features_support_finalized(&request, &image) {
        return broker_registration_response(version, UNSUPPORTED_VERSION, -1);
    }
    let incarnation_id = uuid::Uuid::from_bytes(request.incarnation_id.0);
    if let Some(existing) = image.broker(node_id) {
        let result = if existing.incarnation_id == incarnation_id {
            (SUCCESS, existing.broker_epoch)
        } else {
            (DUPLICATE_BROKER_REGISTRATION, -1)
        };
        return broker_registration_response(version, result.0, result.1);
    }

    let first = &endpoints[0];
    let features = request
        .features
        .into_iter()
        .map(|feature| {
            (
                feature.name,
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect();
    let log_dirs = request
        .log_dirs
        .iter()
        .map(|directory| uuid::Uuid::from_bytes(directory.0))
        .collect();
    let record = BrokerRegistrationRecord {
        node_id,
        broker_epoch: 0,
        incarnation_id,
        host: first.host.clone(),
        port: first.port,
        rack: request.rack,
        endpoints,
        log_dirs,
        features,
    };
    if let Err(error) = engine
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(record)])
        .await
    {
        return broker_registration_response(version, raft_error_code(&error), -1);
    }
    let epoch = engine.current_image().broker_epoch(node_id).unwrap_or(-1);
    let error = if epoch < 0 {
        UNKNOWN_SERVER_ERROR
    } else {
        SUCCESS
    };
    broker_registration_response(version, error, epoch)
}

fn broker_heartbeat(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = BrokerHeartbeatRequest::decode(&mut body, version)?;
    let mut response = BrokerHeartbeatResponse::default();
    response.error_code = if !authorized {
        CLUSTER_AUTHORIZATION_FAILED
    } else if !is_leader(engine) {
        NOT_CONTROLLER
    } else {
        validate_heartbeat(&request, engine, &mut response)
    };
    encode(&response, version)
}

fn validate_heartbeat(
    request: &BrokerHeartbeatRequest,
    engine: &KraftController,
    response: &mut BrokerHeartbeatResponse,
) -> i16 {
    let Ok(node) = u64::try_from(request.broker_id).map(NodeId) else {
        return BROKER_ID_NOT_REGISTERED;
    };
    let image = engine.current_image();
    let Some(registration) = image.broker(node) else {
        return BROKER_ID_NOT_REGISTERED;
    };
    if registration.broker_epoch != request.broker_epoch {
        return STALE_BROKER_EPOCH;
    }
    response.is_caught_up = request.current_metadata_offset >= registration.broker_epoch;
    response.is_fenced = request.want_fence || !response.is_caught_up;
    response.should_shut_down = request.want_shut_down;
    SUCCESS
}

async fn controller_registration(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = ControllerRegistrationRequest::decode(&mut body, version)?;
    if !authorized {
        return controller_registration_response(
            version,
            CLUSTER_AUTHORIZATION_FAILED,
            Some("cluster action denied".into()),
        );
    }
    if !is_leader(engine) {
        return controller_registration_response(version, NOT_CONTROLLER, None);
    }
    let node_id = match u64::try_from(request.controller_id) {
        Ok(id) => NodeId(id),
        Err(_) => {
            return controller_registration_response(
                version,
                INVALID_REGISTRATION,
                Some("controller id must be non-negative".into()),
            );
        }
    };
    if !engine.quorum_snapshot().voters.contains(node_id) {
        return controller_registration_response(
            version,
            UNKNOWN_CONTROLLER_ID,
            Some(format!(
                "controller {} is not a quorum voter",
                request.controller_id
            )),
        );
    }
    let endpoints = match decode_controller_listeners(&request.listeners) {
        Ok(endpoints) => endpoints,
        Err(message) => {
            return controller_registration_response(version, INVALID_REGISTRATION, Some(message));
        }
    };
    let features = match decode_controller_features(&request) {
        Ok(features) => features,
        Err(message) => {
            return controller_registration_response(version, INVALID_REGISTRATION, Some(message));
        }
    };
    let record = ControllerRegistrationRecord {
        node_id,
        incarnation_id: uuid::Uuid::from_bytes(request.incarnation_id.0),
        zk_migration_ready: request.zk_migration_ready,
        endpoints,
        features,
    };
    if engine.current_image().controller(node_id) == Some(&record) {
        return controller_registration_response(version, SUCCESS, None);
    }
    let result = engine
        .submit_change(vec![MetadataRecord::V1ControllerRegistration(record)])
        .await;
    match result {
        Ok(_) => controller_registration_response(version, SUCCESS, None),
        Err(error) => controller_registration_response(
            version,
            raft_error_code(&error),
            Some(error.to_string()),
        ),
    }
}

fn decode_controller_features(
    request: &ControllerRegistrationRequest,
) -> Result<BTreeMap<String, (i16, i16)>, String> {
    let features: BTreeMap<_, _> = request
        .features
        .iter()
        .map(|feature| {
            (
                feature.name.clone(),
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect();
    if features
        .iter()
        .any(|(name, (min, max))| name.is_empty() || min > max)
    {
        return Err("invalid controller feature range".into());
    }
    Ok(features)
}

fn cluster_id_matches(request: &str, cluster_id: uuid::Uuid) -> bool {
    request == cluster_id.to_string() || request == URL_SAFE_NO_PAD.encode(cluster_id.as_bytes())
}

fn decode_broker_listeners(
    listeners: &[broker_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, i16> {
    decode_listeners(listeners.iter().map(|listener| {
        (
            listener.name.as_str(),
            listener.host.as_str(),
            listener.port,
            listener.security_protocol,
        )
    }))
    .map_err(|_| INVALID_REGISTRATION)
}

fn decode_controller_listeners(
    listeners: &[controller_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, String> {
    decode_listeners(listeners.iter().map(|listener| {
        (
            listener.name.as_str(),
            listener.host.as_str(),
            listener.port,
            listener.security_protocol,
        )
    }))
}

fn decode_listeners<'a>(
    listeners: impl Iterator<Item = (&'a str, &'a str, u16, i16)>,
) -> Result<Vec<BrokerEndpoint>, String> {
    let mut names = HashSet::new();
    let endpoints = listeners
        .map(|(name, host, port, protocol)| {
            if name.is_empty() || host.is_empty() || port == 0 || !names.insert(name.to_owned()) {
                return Err("invalid or duplicate registration listener".into());
            }
            let protocol = protocol_from_wire(protocol)
                .ok_or_else(|| "unknown listener security protocol".to_owned())?;
            Ok(BrokerEndpoint {
                name: name.to_owned(),
                host: host.to_owned(),
                port,
                protocol,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if endpoints.is_empty() {
        return Err("registration has no listeners".into());
    }
    Ok(endpoints)
}

fn protocol_from_wire(protocol: i16) -> Option<ListenerProtocol> {
    match protocol {
        0 => Some(ListenerProtocol::Plaintext),
        1 => Some(ListenerProtocol::Ssl),
        2 => Some(ListenerProtocol::SaslPlaintext),
        3 => Some(ListenerProtocol::SaslSsl),
        _ => None,
    }
}

fn features_support_finalized(
    request: &BrokerRegistrationRequest,
    image: &crabka_metadata::MetadataImage,
) -> bool {
    image.finalized_features().iter().all(|(name, level)| {
        request.features.iter().any(|feature| {
            feature.name == *name
                && feature.min_supported_version <= *level
                && *level <= feature.max_supported_version
        })
    })
}

fn is_leader(engine: &KraftController) -> bool {
    engine.watch_leader().borrow().as_ref() == Some(&engine.node_id())
}

fn raft_error_code(error: &RaftError) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => NOT_CONTROLLER,
        RaftError::Metadata(_) | RaftError::ChangeRejected(_) => INVALID_REGISTRATION,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

fn broker_registration_response(
    version: i16,
    error_code: i16,
    broker_epoch: i64,
) -> Result<Bytes, RaftError> {
    encode(
        &BrokerRegistrationResponse {
            error_code,
            broker_epoch,
            ..Default::default()
        },
        version,
    )
}

fn controller_registration_response(
    version: i16,
    error_code: i16,
    error_message: Option<String>,
) -> Result<Bytes, RaftError> {
    encode(
        &ControllerRegistrationResponse {
            error_code,
            error_message,
            ..Default::default()
        },
        version,
    )
}

fn encode(response: &impl Encode, version: i16) -> Result<Bytes, RaftError> {
    let mut bytes = BytesMut::new();
    response.encode(&mut bytes, version)?;
    Ok(bytes.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_kafka_and_uuid_cluster_ids() {
        let cluster_id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert2::assert!(cluster_id_matches(&cluster_id.to_string(), cluster_id));
        assert2::assert!(cluster_id_matches(
            &URL_SAFE_NO_PAD.encode(cluster_id.as_bytes()),
            cluster_id
        ));
    }

    #[test]
    fn lifecycle_api_table_matches_generated_schemas() {
        assert2::assert!(SUPPORTED_APIS == [(62, 4), (63, 2), (70, 0)]);
    }
}
