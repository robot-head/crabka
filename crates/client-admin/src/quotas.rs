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
            remove: false,
            ..Default::default()
        },
        QuotaOp::Remove { key } => AlterOp {
            key: key.clone(),
            // Wire requires a value field even on remove; broker ignores it.
            value: 0.0,
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
    use assert2::assert;
    use crabka_protocol::UnknownTaggedFields;

    use super::*;

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
}
