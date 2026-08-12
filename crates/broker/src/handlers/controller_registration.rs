//! `ControllerRegistration` (`api_key=70`). KIP-919 controller registration.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use crabka_metadata::{
    AclOperation, BrokerEndpoint, ControllerRegistrationRecord, MetadataRecord, NodeId,
    ResourceType,
};
use crabka_protocol::{
    Decode,
    owned::{
        controller_registration_request::ControllerRegistrationRequest,
        controller_registration_response::ControllerRegistrationResponse,
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
    let req = ControllerRegistrationRequest::decode(&mut cur, version)?;
    let image = broker.controller.current_image();
    if crate::handlers::acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        ResourceType::Cluster,
        crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
        AclOperation::ClusterAction,
    ) {
        return response(
            version,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("cluster action denied".into()),
        );
    }
    if broker.controller.watch_leader().borrow().as_ref() != Some(&broker.config.node_id) {
        return response(version, codes::NOT_CONTROLLER, None);
    }

    let node_id = match u64::try_from(req.controller_id) {
        Ok(id) => NodeId(id),
        Err(_) => {
            return response(
                version,
                codes::INVALID_REGISTRATION,
                Some("controller id must be non-negative".into()),
            );
        }
    };
    if !broker.controller.quorum_state().voters.contains(&node_id) {
        return response(
            version,
            codes::UNKNOWN_CONTROLLER_ID,
            Some(format!(
                "controller {} is not a quorum voter",
                req.controller_id
            )),
        );
    }

    let endpoints = match decode_listeners(&req.listeners) {
        Ok(endpoints) => endpoints,
        Err(message) => return response(version, codes::INVALID_REGISTRATION, Some(message)),
    };
    let features = req
        .features
        .into_iter()
        .map(|feature| {
            (
                feature.name,
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if features
        .iter()
        .any(|(name, (min, max))| name.is_empty() || min > max)
    {
        return response(
            version,
            codes::INVALID_REGISTRATION,
            Some("invalid controller feature range".into()),
        );
    }

    let record = ControllerRegistrationRecord {
        node_id,
        incarnation_id: uuid::Uuid::from_bytes(req.incarnation_id.0),
        zk_migration_ready: req.zk_migration_ready,
        endpoints,
        features,
    };
    if image.controller(node_id) == Some(&record) {
        return response(version, 0, None);
    }
    match broker
        .controller
        .submit_change(vec![MetadataRecord::V1ControllerRegistration(record)])
        .await
    {
        Ok(_) => response(version, 0, None),
        Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
            response(version, codes::NOT_CONTROLLER, None)
        }
        Err(RaftError::Metadata(error)) => response(
            version,
            codes::INVALID_REGISTRATION,
            Some(error.to_string()),
        ),
        Err(error) => response(
            version,
            codes::UNKNOWN_SERVER_ERROR,
            Some(error.to_string()),
        ),
    }
}

fn decode_listeners(
    listeners: &[crabka_protocol::owned::controller_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, String> {
    if listeners.is_empty() {
        return Err("controller registration has no listeners".into());
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
                return Err("invalid or duplicate controller listener".into());
            }
            let protocol = match listener.security_protocol {
                0 => ListenerProtocol::Plaintext,
                1 => ListenerProtocol::Ssl,
                2 => ListenerProtocol::SaslPlaintext,
                3 => ListenerProtocol::SaslSsl,
                _ => return Err("unknown controller listener security protocol".into()),
            };
            Ok(BrokerEndpoint {
                name: listener.name.clone(),
                host: listener.host.clone(),
                port: listener.port,
                protocol,
            })
        })
        .collect()
}

fn response(
    version: i16,
    error_code: i16,
    error_message: Option<String>,
) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(
        &ControllerRegistrationResponse {
            error_code,
            error_message,
            ..Default::default()
        },
        version,
    )
}

#[cfg(test)]
mod tests {
    use crabka_protocol::owned::controller_registration_request::Listener;

    use super::*;

    #[test]
    fn controller_listener_validation_is_strict() {
        let listener = Listener {
            name: "CONTROLLER".into(),
            host: "controller-1".into(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        };
        assert2::assert!(decode_listeners(std::slice::from_ref(&listener)).is_ok());
        assert2::assert!(decode_listeners(&[listener.clone(), listener]).is_err());
    }
}
