//! `BrokerRegistration` (`api_key=62`). KIP-631/KIP-903 broker registration.

use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use crabka_metadata::{
    AclOperation, BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord, NodeId, ResourceType,
};
use crabka_protocol::{
    Decode,
    owned::{
        broker_registration_request::{BrokerRegistrationRequest, Listener},
        broker_registration_response::BrokerRegistrationResponse,
    },
};
use crabka_raft::RaftError;
use crabka_security::ListenerProtocol;

use crate::{broker::Broker, codes, error::BrokerError, handlers::RequestContext};

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = BrokerRegistrationRequest::decode(&mut cur, version)?;
    let image = broker.controller.current_image();

    if crate::handlers::acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        ResourceType::Cluster,
        crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
        AclOperation::ClusterAction,
    ) {
        return response(version, codes::CLUSTER_AUTHORIZATION_FAILED, -1);
    }
    if broker.controller.watch_leader().borrow().as_ref() != Some(&broker.config.node_id) {
        return response(version, codes::NOT_CONTROLLER, -1);
    }

    let node_id = match u64::try_from(req.broker_id) {
        Ok(id) => NodeId(id),
        Err(_) => return response(version, codes::INVALID_REGISTRATION, -1),
    };
    if !cluster_id_matches(&req.cluster_id, image.cluster_id()) {
        return response(version, codes::INCONSISTENT_CLUSTER_ID, -1);
    }
    if req.is_migrating_zk_broker {
        return response(version, codes::BROKER_ID_NOT_REGISTERED, -1);
    }
    let endpoints = match decode_listeners(&req.listeners) {
        Ok(endpoints) => endpoints,
        Err(code) => return response(version, code, -1),
    };
    if !features_support_finalized(&req, &image) {
        return response(version, codes::UNSUPPORTED_VERSION, -1);
    }

    let incarnation_id = uuid::Uuid::from_bytes(req.incarnation_id.0);
    if let Some(existing) = image.broker(node_id) {
        if existing.incarnation_id == incarnation_id {
            // Retried registration from the same process. Kafka preserves its
            // epoch; returning it makes the operation idempotent.
            return response(version, 0, existing.broker_epoch);
        }
        if broker.liveness.is_alive(node_id.0).await {
            return response(version, codes::DUPLICATE_BROKER_REGISTRATION, -1);
        }
    }

    let first = &endpoints[0];
    let features = req
        .features
        .into_iter()
        .map(|feature| {
            (
                feature.name,
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect();
    let log_dirs = req
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
        rack: req.rack,
        endpoints,
        log_dirs,
        features,
    };
    if let Err(error) = broker
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(record)])
        .await
    {
        return response(version, raft_error_code(&error), -1);
    }

    let epoch = broker
        .controller
        .current_image()
        .broker(node_id)
        .map_or(-1, |registration| registration.broker_epoch);
    response(
        version,
        if epoch < 0 {
            codes::UNKNOWN_SERVER_ERROR
        } else {
            0
        },
        epoch,
    )
}

fn cluster_id_matches(request: &str, cluster_id: uuid::Uuid) -> bool {
    request == cluster_id.to_string() || request == URL_SAFE_NO_PAD.encode(cluster_id.as_bytes())
}

fn decode_listeners(listeners: &[Listener]) -> Result<Vec<BrokerEndpoint>, i16> {
    if listeners.is_empty() {
        return Err(codes::INVALID_REGISTRATION);
    }
    let mut names = HashSet::with_capacity(listeners.len());
    listeners
        .iter()
        .map(|listener| {
            if listener.name.is_empty()
                || listener.host.is_empty()
                || listener.port == 0
                || !names.insert(listener.name.clone())
            {
                return Err(codes::INVALID_REGISTRATION);
            }
            Ok(BrokerEndpoint {
                name: listener.name.clone(),
                host: listener.host.clone(),
                port: listener.port,
                protocol: protocol_from_wire(listener.security_protocol)
                    .ok_or(codes::INVALID_REGISTRATION)?,
            })
        })
        .collect()
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
    req: &BrokerRegistrationRequest,
    image: &crabka_metadata::MetadataImage,
) -> bool {
    image.finalized_features().iter().all(|(name, level)| {
        req.features.iter().any(|feature| {
            feature.name == *name
                && feature.min_supported_version <= *level
                && *level <= feature.max_supported_version
        })
    })
}

fn raft_error_code(error: &RaftError) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => codes::NOT_CONTROLLER,
        RaftError::Metadata(_) => codes::INVALID_REGISTRATION,
        _ => codes::UNKNOWN_SERVER_ERROR,
    }
}

fn response(version: i16, error_code: i16, broker_epoch: i64) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(
        &BrokerRegistrationResponse {
            error_code,
            broker_epoch,
            ..Default::default()
        },
        version,
    )
}

#[cfg(test)]
mod tests {
    use crabka_protocol::owned::broker_registration_request::Feature;

    use super::*;

    #[test]
    fn accepts_uuid_and_kafka_base64_cluster_ids() {
        let id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert2::assert!(cluster_id_matches(&id.to_string(), id));
        assert2::assert!(cluster_id_matches(
            &URL_SAFE_NO_PAD.encode(id.as_bytes()),
            id
        ));
        assert2::assert!(!cluster_id_matches("different", id));
    }

    #[test]
    fn listener_validation_rejects_duplicates_and_unknown_protocol() {
        let valid = Listener {
            name: "PLAINTEXT".into(),
            host: "broker".into(),
            port: 9092,
            security_protocol: 0,
            ..Default::default()
        };
        assert2::assert!(decode_listeners(std::slice::from_ref(&valid)).is_ok());
        assert2::assert!(decode_listeners(&[valid.clone(), valid.clone()]).is_err());
        assert2::assert!(
            decode_listeners(&[Listener {
                security_protocol: 99,
                ..valid
            }])
            .is_err()
        );
    }

    #[test]
    fn finalized_features_must_fit_request_ranges() {
        let mut image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(
            crabka_metadata::FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 25,
            },
        ));
        let mut req = BrokerRegistrationRequest {
            features: vec![Feature {
                name: "metadata.version".into(),
                min_supported_version: 7,
                max_supported_version: 25,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert2::assert!(features_support_finalized(&req, &image));
        req.features[0].max_supported_version = 24;
        assert2::assert!(!features_support_finalized(&req, &image));
    }
}
