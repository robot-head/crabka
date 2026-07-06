//! Client-quota admin RPCs.
//!
//! Two admin operations the `KafkaUser` reconciler drives:
//! `DescribeClientQuotas` (`api_key` 48) reads the current set of
//! quota keys → values for a single (user) entity;
//! `AlterClientQuotas` (`api_key` 49) upserts and/or removes those keys.
//!
//! Only the per-user shape is exposed (entity `[("user", Some(name))]`).
//! Per-`client-id`, per-`ip`, and tuple entities (e.g. `(user, client-id)`)
//! are reserved for later operator surfaces.

use std::collections::BTreeMap;

use crabka_protocol::owned::{
    alter_client_quotas_request::{
        AlterClientQuotasRequest, EntityData as AlterEntity, EntryData as AlterEntry,
        OpData as AlterOp,
    },
    describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
};

use crate::{AdminClient, AdminError, KafkaError, kafka_error_if, kafka_error_name};

/// Wire `match_type` constant from KIP-546 / `DescribeClientQuotasRequest.json`.
const MATCH_TYPE_EXACT: i8 = 0;

/// One mutation against a (user) quota entity. The reconciler computes
/// these by diffing the spec against the current broker state.
#[derive(Debug, Clone, PartialEq)]
pub enum QuotaOp {
    /// Upsert `key` → `value`. `value` must be finite and non-negative;
    /// for `request_percentage` the broker also requires `value <= 100`.
    Set { key: String, value: f64 },
    /// Tombstone `key` for this entity. Matches Kafka's `remove=true`
    /// `OpData` flag.
    Remove { key: String },
}

/// Snapshot of the broker's quota state for a single user. Empty map ==
/// no per-user quotas configured.
pub type UserQuotaConfig = BTreeMap<String, f64>;

impl AdminClient {
    /// Read the broker's current client-quota config for the named user.
    /// Filters strictly on the single-component entity
    /// `[("user", Some(username))]`; broker entries whose entity also
    /// carries a `client-id` axis do not match (matches Kafka admin-tool
    /// strict-component semantics).
    pub async fn describe_user_quotas(
        &mut self,
        username: &str,
    ) -> Result<UserQuotaConfig, AdminError> {
        let req = DescribeClientQuotasRequest {
            components: vec![ComponentData {
                entity_type: "user".into(),
                match_type: MATCH_TYPE_EXACT,
                match_: Some(username.into()),
                ..Default::default()
            }],
            strict: true,
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;
        if resp.error_code != 0 {
            return Err(AdminError::Broker {
                api: "DescribeClientQuotas",
                code: resp.error_code,
                name: kafka_error_name(resp.error_code),
                message: resp.error_message,
            });
        }
        let mut out = UserQuotaConfig::new();
        // `strict: true` plus a one-component filter means the broker
        // returns at most one entry — but be tolerant of broker bugs.
        for entry in resp.entries.unwrap_or_default() {
            for v in entry.values {
                out.insert(v.key, v.value);
            }
        }
        Ok(out)
    }

    /// Apply `ops` against the (user) entity. Returns the per-entry
    /// `KafkaError` surfaced by the broker, or `None` on success.
    ///
    /// `validate_only` mirrors the wire flag — when `true` the broker
    /// runs validation but writes no metadata record.
    pub async fn alter_user_quotas(
        &mut self,
        username: &str,
        ops: &[QuotaOp],
        validate_only: bool,
    ) -> Result<Option<KafkaError>, AdminError> {
        if ops.is_empty() {
            return Ok(None);
        }
        let req = AlterClientQuotasRequest {
            entries: vec![AlterEntry {
                entity: vec![AlterEntity {
                    entity_type: "user".into(),
                    entity_name: Some(username.into()),
                    ..Default::default()
                }],
                ops: ops.iter().map(op_to_wire).collect(),
                ..Default::default()
            }],
            validate_only,
            ..Default::default()
        };
        let resp = self.conn.send(req).await?;
        // We pass one entry → expect one result. Defensive on length.
        let entry = resp.entries.into_iter().next();
        let Some(entry) = entry else {
            return Ok(None);
        };
        Ok(kafka_error_if(entry.error_code, entry.error_message))
    }
}

fn op_to_wire(op: &QuotaOp) -> AlterOp {
    match op {
        QuotaOp::Set { key, value } => AlterOp {
            key: key.clone(),
            value: *value,
            ..Default::default()
        },
        QuotaOp::Remove { key } => AlterOp {
            key: key.clone(),
            remove: true,
            ..Default::default()
        },
    }
}

/// Pure: diff the desired key-set against the current key-set, producing
/// the minimal `(set, remove)` op stream. Floats compare bit-equal so a
/// no-op `Set` with the same value is not re-issued.
#[must_use]
pub fn diff_user_quotas(current: &UserQuotaConfig, desired: &UserQuotaConfig) -> Vec<QuotaOp> {
    let mut ops = Vec::new();
    for (k, v) in desired {
        match current.get(k) {
            Some(cur) if cur.to_bits() == v.to_bits() => {}
            _ => ops.push(QuotaOp::Set {
                key: k.clone(),
                value: *v,
            }),
        }
    }
    for k in current.keys() {
        if !desired.contains_key(k) {
            ops.push(QuotaOp::Remove { key: k.clone() });
        }
    }
    ops
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use bytes::{Buf, BytesMut};
    use crabka_client_core::MockBroker;
    use crabka_protocol::{
        Decode, Encode, UnknownTaggedFields,
        owned::{
            alter_client_quotas_request,
            alter_client_quotas_response::{AlterClientQuotasResponse, EntityData, EntryData},
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            describe_client_quotas_request,
            describe_client_quotas_response::{
                DescribeClientQuotasResponse, EntityData as DescribeEntityData,
                EntryData as DescribeEntryData, ValueData as DescribeValueData,
            },
        },
    };

    use super::*;

    fn encode_v0(resp: &impl Encode) -> Vec<u8> {
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn api_versions_response(api_key: i16, version: i16) -> Vec<u8> {
        encode_v0(&ApiVersionsResponse {
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 0,
                    ..Default::default()
                },
                ApiVersion {
                    api_key,
                    min_version: version,
                    max_version: version,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
    }

    fn request_body_after_header(mut body: &[u8], flexible_header: bool) -> &[u8] {
        let client_id_len = body.get_i16();
        assert!(client_id_len >= 0);
        body.advance(usize::try_from(client_id_len).expect("client id length is non-negative"));
        if flexible_header {
            assert!(body.get_u8() == 0);
        }
        body
    }

    #[test]
    fn diff_no_change_returns_empty() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1_048_576.0);
        let d = c.clone();
        assert!(diff_user_quotas(&c, &d).is_empty());
    }

    #[test]
    fn diff_set_added_keys() {
        let c = UserQuotaConfig::new();
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 1_048_576.0);
        d.insert("request_percentage".into(), 25.0);
        let ops = diff_user_quotas(&c, &d);
        // `desired` is a BTreeMap, so `Set` ops come out in key order.
        assert!(
            ops == vec![
                QuotaOp::Set {
                    key: "producer_byte_rate".to_string(),
                    value: 1_048_576.0,
                },
                QuotaOp::Set {
                    key: "request_percentage".to_string(),
                    value: 25.0,
                },
            ]
        );
    }

    #[test]
    fn diff_remove_dropped_keys() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1.0);
        c.insert("consumer_byte_rate".into(), 2.0);
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 1.0);
        let ops = diff_user_quotas(&c, &d);
        assert!(
            ops == vec![QuotaOp::Remove {
                key: "consumer_byte_rate".into()
            }]
        );
    }

    #[test]
    fn diff_value_change_is_a_set() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1.0);
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 2.0);
        let ops = diff_user_quotas(&c, &d);
        assert!(
            ops == vec![QuotaOp::Set {
                key: "producer_byte_rate".into(),
                value: 2.0,
            }]
        );
    }

    #[test]
    fn diff_mixed_add_change_remove() {
        let mut c = UserQuotaConfig::new();
        c.insert("producer_byte_rate".into(), 1.0);
        c.insert("consumer_byte_rate".into(), 2.0);
        let mut d = UserQuotaConfig::new();
        d.insert("producer_byte_rate".into(), 5.0); // change
        d.insert("request_percentage".into(), 25.0); // add
        // consumer_byte_rate dropped
        let ops = diff_user_quotas(&c, &d);
        // Sets come first (in `desired` key order), then removes (in
        // `current` key order) — both maps are BTreeMaps.
        assert!(
            ops == vec![
                QuotaOp::Set {
                    key: "producer_byte_rate".to_string(),
                    value: 5.0,
                },
                QuotaOp::Set {
                    key: "request_percentage".to_string(),
                    value: 25.0,
                },
                QuotaOp::Remove {
                    key: "consumer_byte_rate".to_string(),
                },
            ]
        );
    }

    #[test]
    fn op_to_wire_set() {
        let op = QuotaOp::Set {
            key: "producer_byte_rate".into(),
            value: 1.0,
        };
        let w = op_to_wire(&op);
        assert!(
            w == AlterOp {
                key: "producer_byte_rate".to_string(),
                value: 1.0,
                remove: false,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[test]
    fn op_to_wire_remove_sends_zero_value_and_flag() {
        let op = QuotaOp::Remove {
            key: "producer_byte_rate".into(),
        };
        let w = op_to_wire(&op);
        assert!(
            w == AlterOp {
                key: "producer_byte_rate".to_string(),
                value: 0.0,
                remove: true,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_user_quotas_sends_strict_user_component() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(
                    describe_client_quotas_request::API_KEY,
                    0,
                ));
            }
            if api_key == describe_client_quotas_request::API_KEY {
                let mut body = request_body_after_header(
                    body,
                    version >= describe_client_quotas_request::FLEXIBLE_MIN,
                );
                let request = DescribeClientQuotasRequest::decode(&mut body, version)
                    .expect("describe quotas request decodes");
                assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_v0(&DescribeClientQuotasResponse {
                    entries: Some(vec![DescribeEntryData {
                        entity: vec![DescribeEntityData {
                            entity_type: "user".into(),
                            entity_name: Some("alice".into()),
                            ..Default::default()
                        }],
                        values: vec![DescribeValueData {
                            key: "producer_byte_rate".into(),
                            value: 1024.0,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }]),
                    ..Default::default()
                }));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");

        let quotas = admin
            .describe_user_quotas("alice")
            .await
            .expect("describe quotas response maps");

        assert!(quotas.get("producer_byte_rate") == Some(&1024.0));
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("describe quotas request was captured");
        assert!(
            request
                == DescribeClientQuotasRequest {
                    components: vec![ComponentData {
                        entity_type: "user".into(),
                        match_type: MATCH_TYPE_EXACT,
                        match_: Some("alice".into()),
                        ..Default::default()
                    }],
                    strict: true,
                    ..Default::default()
                }
        );
        mock.stop();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_user_quotas_surfaces_broker_entry_error() {
        let seen_request = Arc::new(Mutex::new(None));
        let captured_request = Arc::clone(&seen_request);
        let mock = MockBroker::start(move |api_key, version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_response(
                    alter_client_quotas_request::API_KEY,
                    0,
                ));
            }
            if api_key == alter_client_quotas_request::API_KEY {
                let mut body = request_body_after_header(
                    body,
                    version >= alter_client_quotas_request::FLEXIBLE_MIN,
                );
                let request = AlterClientQuotasRequest::decode(&mut body, version)
                    .expect("alter quotas request decodes");
                assert!(body.is_empty());
                *captured_request.lock().expect("request capture lock") = Some(request);
                return Some(encode_v0(&AlterClientQuotasResponse {
                    entries: vec![EntryData {
                        error_code: 40,
                        error_message: Some("invalid quota".into()),
                        entity: vec![EntityData {
                            entity_type: "user".into(),
                            entity_name: Some("alice".into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }));
            }
            None
        })
        .await;
        let mut admin = AdminClient::connect(&[mock.addr.to_string()])
            .await
            .expect("admin connects to mock broker");

        let error = admin
            .alter_user_quotas(
                "alice",
                &[
                    QuotaOp::Set {
                        key: "producer_byte_rate".into(),
                        value: 1024.0,
                    },
                    QuotaOp::Remove {
                        key: "consumer_byte_rate".into(),
                    },
                ],
                true,
            )
            .await
            .expect("alter quotas maps broker entry")
            .expect("non-zero broker error is returned");

        assert!(error.code == 40);
        assert!(error.message == Some("invalid quota".into()));
        let request = seen_request
            .lock()
            .expect("request capture lock")
            .take()
            .expect("alter quotas request was captured");
        assert!(
            request
                == AlterClientQuotasRequest {
                    entries: vec![AlterEntry {
                        entity: vec![AlterEntity {
                            entity_type: "user".into(),
                            entity_name: Some("alice".into()),
                            ..Default::default()
                        }],
                        ops: vec![
                            AlterOp {
                                key: "producer_byte_rate".into(),
                                value: 1024.0,
                                ..Default::default()
                            },
                            AlterOp {
                                key: "consumer_byte_rate".into(),
                                remove: true,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    validate_only: true,
                    ..Default::default()
                }
        );
        mock.stop();
    }
}
