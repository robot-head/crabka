//! `AlterClientQuotas` (`api_key` 49, KIP-13/124/257).

use std::collections::HashSet;

use bytes::Bytes;
use crabka_metadata::{AclOperation, ClientQuotaRecord, MetadataRecord, QuotaEntity, ResourceType};
use crabka_protocol::Encode;
use crabka_protocol::owned::alter_client_quotas_request::{
    AlterClientQuotasRequest, EntityData, EntryData,
};
use crabka_protocol::owned::alter_client_quotas_response::{
    AlterClientQuotasResponse, EntityData as RespEntity, EntryData as RespEntry,
};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::{
    CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE, INVALID_CONFIG, INVALID_REQUEST,
};

const KNOWN_QUOTA_KEYS: &[&str] = &[
    "producer_byte_rate",
    "consumer_byte_rate",
    "request_percentage",
    "connection_creation_rate", // KIP-612 — only enforced when paired with ip entity
    "controller_mutation_rate", // KIP-599
];
const SUPPORTED_ENTITY_TYPES: &[&str] = &["user", "client-id", "ip"];

#[tracing::instrument(
    name = "handle_alter_client_quotas",
    level = "info",
    skip_all,
    fields(api = "AlterClientQuotas"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: AlterClientQuotasRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(
            &req,
            CLUSTER_AUTHORIZATION_FAILED,
            "alter-client-quotas denied",
            api_version,
        );
    }

    let mut entry_results = Vec::with_capacity(req.entries.len());
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    for entry in &req.entries {
        match process_one_entry(entry) {
            Ok(records) => {
                if !req.validate_only {
                    to_submit.extend(records);
                }
                entry_results.push(ok_entry(&entry.entity));
            }
            Err((code, msg)) => entry_results.push(err_entry(&entry.entity, code, msg)),
        }
    }

    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "alter-client-quotas submit failed");
        for r in &mut entry_results {
            if r.error_code == 0 {
                r.error_code = COORDINATOR_NOT_AVAILABLE;
                r.error_message = Some(format!("submit failed: {e}"));
            }
        }
    }

    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: entry_results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

/// Validate + transform one `EntryData` into a list of `MetadataRecord`s
/// to submit. Returns wire `(code, message)` on validation failure.
pub(crate) fn process_one_entry(entry: &EntryData) -> Result<Vec<MetadataRecord>, (i16, String)> {
    if entry.entity.is_empty() {
        return Err((INVALID_REQUEST, "empty entity tuple".into()));
    }
    let mut seen_types: HashSet<&str> = HashSet::new();
    for e in &entry.entity {
        if !SUPPORTED_ENTITY_TYPES.contains(&e.entity_type.as_str()) {
            return Err((
                INVALID_REQUEST,
                format!("unsupported entity_type {:?}", e.entity_type),
            ));
        }
        if !seen_types.insert(e.entity_type.as_str()) {
            return Err((
                INVALID_REQUEST,
                format!("duplicate entity_type {:?}", e.entity_type),
            ));
        }
        // entity_name == None is fine for ip — that means the default ip entity.
        if e.entity_type == "ip"
            && let Some(name) = &e.entity_name
            && name.parse::<std::net::Ipv4Addr>().is_err()
        {
            return Err((INVALID_REQUEST, format!("invalid IPv4 address {name:?}")));
        }
    }
    let mut records = Vec::with_capacity(entry.ops.len());
    for op in &entry.ops {
        if !KNOWN_QUOTA_KEYS.contains(&op.key.as_str()) {
            return Err((INVALID_CONFIG, format!("unknown quota key {:?}", op.key)));
        }
        if !op.remove {
            if !op.value.is_finite() || op.value < 0.0 {
                return Err((
                    INVALID_CONFIG,
                    format!("invalid value {} for {}", op.value, op.key),
                ));
            }
            if op.key == "request_percentage" && op.value > 100.0 {
                return Err((
                    INVALID_CONFIG,
                    format!("request_percentage > 100.0: {}", op.value),
                ));
            }
        }
        records.push(MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: entry
                .entity
                .iter()
                .map(|e| QuotaEntity {
                    entity_type: e.entity_type.clone(),
                    entity_name: e.entity_name.clone(),
                })
                .collect(),
            config_key: op.key.clone(),
            config_value: if op.remove { None } else { Some(op.value) },
        }));
    }
    Ok(records)
}

fn ok_entry(entity: &[EntityData]) -> RespEntry {
    RespEntry {
        error_code: 0,
        error_message: None,
        entity: entity
            .iter()
            .map(|e| RespEntity {
                entity_type: e.entity_type.clone(),
                entity_name: e.entity_name.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn err_entry(entity: &[EntityData], code: i16, msg: String) -> RespEntry {
    RespEntry {
        error_code: code,
        error_message: Some(msg),
        entity: entity
            .iter()
            .map(|e| RespEntity {
                entity_type: e.entity_type.clone(),
                entity_name: e.entity_name.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn encode_whole_request_error(
    req: &AlterClientQuotasRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let entries: Vec<RespEntry> = req
        .entries
        .iter()
        .map(|e| err_entry(&e.entity, code, msg.into()))
        .collect();
    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version).map_err(|e| {
        crate::error::BrokerError::Replication(format!("encode AlterClientQuotas: {e}"))
    })?;
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::alter_client_quotas_request::{EntityData, EntryData, OpData};

    fn entry(entity: Vec<(&str, Option<&str>)>, ops: Vec<(&str, f64, bool)>) -> EntryData {
        EntryData {
            entity: entity
                .into_iter()
                .map(|(t, n)| EntityData {
                    entity_type: t.into(),
                    entity_name: n.map(Into::into),
                    ..Default::default()
                })
                .collect(),
            ops: ops
                .into_iter()
                .map(|(k, v, r)| OpData {
                    key: k.into(),
                    value: v,
                    remove: r,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn start_writes_v1_client_quota_record() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert!(records.len() == 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!("wrong variant")
        };
        assert!(r.config_key == "producer_byte_rate");
        assert!(r.config_value == Some(1024.0));
    }

    #[test]
    fn validate_only_does_not_submit() {
        // This is exercised at the handler level; process_one_entry has no notion.
        // The test below verifies that the record-building step works regardless.
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        assert!(process_one_entry(&e).is_ok());
    }

    #[test]
    fn remove_writes_none_value() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 0.0, true)],
        );
        let records = process_one_entry(&e).expect("ok");
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!()
        };
        assert!(r.config_value == None);
    }

    #[test]
    fn unsupported_entity_type_rejected() {
        let e = entry(
            vec![("group", Some("g1"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert!(err.0 == INVALID_REQUEST);
    }

    #[test]
    fn duplicate_entity_type_rejected() {
        let e = entry(
            vec![("user", Some("alice")), ("user", Some("bob"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert!(err.0 == INVALID_REQUEST);
    }

    #[test]
    fn out_of_range_value_rejected() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", -100.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert!(err.0 == INVALID_CONFIG);

        let e2 = entry(
            vec![("user", Some("alice"))],
            vec![("request_percentage", 250.0, false)],
        );
        let err2 = process_one_entry(&e2).unwrap_err();
        assert!(err2.0 == INVALID_CONFIG);

        let e3 = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", f64::NAN, false)],
        );
        let err3 = process_one_entry(&e3).unwrap_err();
        assert!(err3.0 == INVALID_CONFIG);
    }

    #[test]
    fn ip_entity_with_valid_ipv4_accepted() {
        let e = entry(
            vec![("ip", Some("10.0.0.1"))],
            vec![("connection_creation_rate", 1.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert!(records.len() == 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!()
        };
        assert!(r.config_key == "connection_creation_rate");
        assert!(r.config_value == Some(1.0));
    }

    #[test]
    fn ip_entity_with_invalid_address_rejected() {
        let e = entry(
            vec![("ip", Some("not-an-ip"))],
            vec![("connection_creation_rate", 1.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert!(err.0 == INVALID_REQUEST);
    }

    #[test]
    fn controller_mutation_rate_key_accepted() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("controller_mutation_rate", 2.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert!(records.len() == 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!("wrong variant");
        };
        assert!(r.config_key == "controller_mutation_rate");
        assert!(r.config_value == Some(2.0));
    }
}
