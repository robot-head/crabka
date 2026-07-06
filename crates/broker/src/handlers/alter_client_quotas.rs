//! `AlterClientQuotas` (`api_key` 49, KIP-13/124/257).

use std::collections::HashSet;

use bytes::Bytes;
use crabka_metadata::{AclOperation, ClientQuotaRecord, MetadataRecord, QuotaEntity, ResourceType};
use crabka_protocol::{
    Encode, UnknownTaggedFields,
    owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData},
        alter_client_quotas_response::{
            AlterClientQuotasResponse, EntityData as RespEntity, EntryData as RespEntry,
        },
    },
};

use super::acl_wire::CLUSTER_RESOURCE_NAME;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::{
        CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE, INVALID_CONFIG, INVALID_REQUEST,
        NONE,
    },
};

/// Quota key: produce-side bandwidth cap in bytes/sec (KIP-13).
const PRODUCER_BYTE_RATE_KEY: &str = "producer_byte_rate";
/// Quota key: fetch-side bandwidth cap in bytes/sec (KIP-13).
const CONSUMER_BYTE_RATE_KEY: &str = "consumer_byte_rate";
/// Quota key: request-handler time cap as a percentage of one thread (KIP-124).
const REQUEST_PERCENTAGE_KEY: &str = "request_percentage";
/// Quota key: per-IP connection creation rate (KIP-612).
const CONNECTION_CREATION_RATE_KEY: &str = "connection_creation_rate";
/// Quota key: controller mutation rate for topic/partition creation and deletion (KIP-599).
const CONTROLLER_MUTATION_RATE_KEY: &str = "controller_mutation_rate";
/// Upper bound for `request_percentage` — a percentage of one request-handler thread.
const REQUEST_PERCENTAGE_MAX: f64 = 100.0;

/// Quota keys Crabka accepts in `AlterClientQuotas` ops.
const KNOWN_QUOTA_KEYS: &[&str] = &[
    PRODUCER_BYTE_RATE_KEY,
    CONSUMER_BYTE_RATE_KEY,
    REQUEST_PERCENTAGE_KEY,
    CONNECTION_CREATION_RATE_KEY, // KIP-612 — only enforced when paired with ip entity
    CONTROLLER_MUTATION_RATE_KEY, // KIP-599
];

/// Quota entity type: authenticated user principal (KIP-257).
const ENTITY_TYPE_USER: &str = "user";
/// Quota entity type: client id (KIP-257).
const ENTITY_TYPE_CLIENT_ID: &str = "client-id";
/// Quota entity type: client source IP address (KIP-612).
const ENTITY_TYPE_IP: &str = "ip";

/// Quota entity types Crabka accepts in `AlterClientQuotas` entries.
const SUPPORTED_ENTITY_TYPES: &[&str] = &[ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT_ID, ENTITY_TYPE_IP];

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
            resource_name: CLUSTER_RESOURCE_NAME,
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
        apply_submit_error(&mut entry_results, e);
    }

    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: entry_results,
        unknown_tagged_fields: UnknownTaggedFields::default(),
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
        if e.entity_type == ENTITY_TYPE_IP
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
            if op.key == REQUEST_PERCENTAGE_KEY && op.value > REQUEST_PERCENTAGE_MAX {
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
        error_code: NONE,
        error_message: None,
        entity: entity
            .iter()
            .map(|e| RespEntity {
                entity_type: e.entity_type.clone(),
                entity_name: e.entity_name.clone(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            })
            .collect(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
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
                unknown_tagged_fields: UnknownTaggedFields::default(),
            })
            .collect(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn apply_submit_error(entry_results: &mut [RespEntry], error: impl std::fmt::Display) {
    let message = format!("submit failed: {error}");
    for r in entry_results {
        if r.error_code == NONE {
            r.error_code = COORDINATOR_NOT_AVAILABLE;
            r.error_message = Some(message.clone());
        }
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
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode AlterClientQuotas")
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use crabka_protocol::owned::alter_client_quotas_request::{EntityData, EntryData, OpData};
    use crabka_security::{AuthMethod, Principal};

    use super::*;
    use crate::{broker::BrokerHandle, test_support::DenyAll};

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

    fn request(entries: Vec<EntryData>, validate_only: bool) -> AlterClientQuotasRequest {
        AlterClientQuotasRequest {
            entries,
            validate_only,
            ..Default::default()
        }
    }

    crate::test_support::response_helpers!(AlterClientQuotasResponse, client_id = "admin-client");

    use crate::test_support::start_broker_with_authorizer as start_broker;

    fn quota_value(handle: &BrokerHandle, user: &str, quota_key: &str) -> Option<f64> {
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some(user.into()))];
        handle
            .controller_image_for_test()
            .client_quotas()
            .get(&key)
            .and_then(|configs| configs.get(quota_key).copied())
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
    fn inclusive_boundary_values_are_accepted() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![
                ("producer_byte_rate", 0.0, false),
                ("request_percentage", 100.0, false),
            ],
        );

        let records = process_one_entry(&e).expect("boundary values are valid");
        let alice_entity = vec![QuotaEntity {
            entity_type: "user".into(),
            entity_name: Some("alice".into()),
        }];
        let expected = vec![
            MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: alice_entity.clone(),
                config_key: "producer_byte_rate".into(),
                config_value: Some(0.0),
            }),
            MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: alice_entity,
                config_key: "request_percentage".into(),
                config_value: Some(100.0),
            }),
        ];
        assert!(records == expected);
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
        let cases = [
            ("producer_byte_rate", -100.0),   // negative
            ("request_percentage", 250.0),    // > 100.0 cap
            ("producer_byte_rate", f64::NAN), // non-finite
        ];
        for (quota_key, value) in cases {
            let e = entry(
                vec![("user", Some("alice"))],
                vec![(quota_key, value, false)],
            );
            let err = process_one_entry(&e).unwrap_err();
            assert!(err.0 == INVALID_CONFIG, "key {quota_key}, value {value}");
        }
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

    #[test]
    fn entry_helpers_preserve_wire_fields() {
        let entity = [EntityData {
            entity_type: "user".into(),
            entity_name: Some("alice".into()),
            ..Default::default()
        }];

        let ok = ok_entry(&entity);
        let expected_ok = RespEntry {
            error_code: 0,
            error_message: None,
            entity: vec![RespEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(ok == expected_ok);

        let err = err_entry(&entity, INVALID_CONFIG, "bad quota".into());
        let expected_err = RespEntry {
            error_code: INVALID_CONFIG,
            error_message: Some("bad quota".into()),
            entity: vec![RespEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(err == expected_err);
    }

    #[test]
    fn submit_error_only_stamps_successful_entries() {
        let mut results = vec![
            ok_entry(&[EntityData {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                ..Default::default()
            }]),
            err_entry(
                &[EntityData {
                    entity_type: "user".into(),
                    entity_name: Some("bob".into()),
                    ..Default::default()
                }],
                INVALID_REQUEST,
                "invalid bob quota".into(),
            ),
        ];

        apply_submit_error(&mut results, "raft unavailable");

        let expected = vec![
            RespEntry {
                error_code: COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: raft unavailable".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            RespEntry {
                error_code: INVALID_REQUEST,
                error_message: Some("invalid bob quota".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("bob".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(results == expected);
    }

    #[test]
    fn whole_request_error_encodes_all_entries() {
        let version = 1;
        let req = request(
            vec![
                entry(vec![("user", Some("alice"))], vec![]),
                entry(vec![("client-id", Some("app"))], vec![]),
            ],
            false,
        );

        let bytes =
            encode_whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied", version)
                .expect("encode");
        let resp = decode_response(&bytes, version);

        let expected = AlterClientQuotasResponse {
            throttle_time_ms: 0,
            entries: vec![
                RespEntry {
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("denied".into()),
                    entity: vec![RespEntity {
                        entity_type: "user".into(),
                        entity_name: Some("alice".into()),
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                RespEntry {
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("denied".into()),
                    entity: vec![RespEntity {
                        entity_type: "client-id".into(),
                        entity_name: Some("app".into()),
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn encode_response_writes_decodable_body() {
        let version = 1;
        let resp = AlterClientQuotasResponse {
            throttle_time_ms: 123,
            entries: vec![err_entry(
                &[EntityData {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    ..Default::default()
                }],
                INVALID_REQUEST,
                "bad request".into(),
            )],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };

        let bytes = encode_response(&resp, version).expect("encode");
        let decoded = decode_response(&bytes, version);

        let expected = AlterClientQuotasResponse {
            throttle_time_ms: 123,
            entries: vec![RespEntry {
                error_code: INVALID_REQUEST,
                error_message: Some("bad request".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(decoded == expected);
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_for_each_entry() {
        let version = 1;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = request(
            vec![entry(
                vec![("user", Some("alice"))],
                vec![("producer_byte_rate", 1024.0, false)],
            )],
            false,
        );

        let resp = handle(&broker, req, &ctx, version).await.expect("handle");
        let resp = decode_response(&resp, version);

        let expected = AlterClientQuotasResponse {
            throttle_time_ms: 0,
            entries: vec![RespEntry {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("alter-client-quotas denied".into()),
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(quota_value(&broker_handle, "alice", "producer_byte_rate") == None);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_entry_results_and_submits_valid_changes() {
        let version = 1;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = request(
            vec![
                entry(
                    vec![("user", Some("alice"))],
                    vec![("producer_byte_rate", 1024.0, false)],
                ),
                entry(
                    vec![("user", Some("bob"))],
                    vec![("unknown_quota_key", 1.0, false)],
                ),
            ],
            false,
        );

        let resp = handle(&broker, req, &ctx, version).await.expect("handle");
        let resp = decode_response(&resp, version);

        let expected = AlterClientQuotasResponse {
            throttle_time_ms: 0,
            entries: vec![
                RespEntry {
                    error_code: 0,
                    error_message: None,
                    entity: vec![RespEntity {
                        entity_type: "user".into(),
                        entity_name: Some("alice".into()),
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                RespEntry {
                    error_code: INVALID_CONFIG,
                    error_message: Some("unknown quota key \"unknown_quota_key\"".into()),
                    entity: vec![RespEntity {
                        entity_type: "user".into(),
                        entity_name: Some("bob".into()),
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        for (user, quota_key, want) in [
            ("alice", "producer_byte_rate", Some(1024.0)),
            ("bob", "unknown_quota_key", None),
        ] {
            assert!(
                quota_value(&broker_handle, user, quota_key) == want,
                "user {user}"
            );
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_validate_only_reports_success_without_submitting() {
        let version = 1;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = request(
            vec![entry(
                vec![("user", Some("carol"))],
                vec![("producer_byte_rate", 2048.0, false)],
            )],
            true,
        );

        let resp = handle(&broker, req, &ctx, version).await.expect("handle");
        let resp = decode_response(&resp, version);

        let expected = AlterClientQuotasResponse {
            throttle_time_ms: 0,
            entries: vec![RespEntry {
                error_code: 0,
                error_message: None,
                entity: vec![RespEntity {
                    entity_type: "user".into(),
                    entity_name: Some("carol".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(quota_value(&broker_handle, "carol", "producer_byte_rate") == None);
        broker_handle.shutdown().await;
    }
}
