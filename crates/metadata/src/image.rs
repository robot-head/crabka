//! Immutable snapshot of the cluster's metadata state. Mutated only by
//! [`MetadataImage::apply`] (called from the Raft state machine), and
//! read everywhere else via shared references / `Arc` clones.

use std::collections::{BTreeMap, HashMap};

use uuid::Uuid;

use crabka_security::{KafkaPrincipal, SaslMechanism, ScramCredential};

use crate::acl::{AclEntry, PatternType, ResourceType};
use crate::error::MetadataError;
use crate::records::{
    BrokerRegistrationRecord, DelegationTokenRecord, MetadataRecord, NodeId, PartitionRecord,
    TopicRecord,
};

pub type EntityKey = Vec<(String, Option<String>)>;

/// Slice 51 (KIP-48): In-memory image type for a single delegation
/// token. Mirrors [`DelegationTokenRecord`] minus any tombstone
/// concerns — tombstones are handled as removals on the apply path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationToken {
    pub token_id: String,
    pub owner: KafkaPrincipal,
    pub hmac: Vec<u8>,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub renewers: Vec<KafkaPrincipal>,
}

impl DelegationToken {
    #[must_use]
    pub fn from_record(rec: &DelegationTokenRecord) -> Self {
        Self {
            token_id: rec.token_id.clone(),
            owner: rec.owner.clone(),
            hmac: rec.hmac.clone(),
            issue_timestamp_ms: rec.issue_timestamp_ms,
            expiry_timestamp_ms: rec.expiry_timestamp_ms,
            max_timestamp_ms: rec.max_timestamp_ms,
            renewers: rec.renewers.clone(),
        }
    }
}

#[must_use]
pub fn canonicalize_entity(mut tuple: Vec<(String, Option<String>)>) -> EntityKey {
    tuple.sort_by(|a, b| a.0.cmp(&b.0));
    tuple
}

#[derive(Debug, Clone, Default)]
pub struct MetadataImage {
    cluster_id: Uuid,
    topics: HashMap<String, TopicRecord>,
    partitions: HashMap<(String, i32), PartitionRecord>,
    brokers: HashMap<NodeId, BrokerRegistrationRecord>,
    topic_configs: HashMap<String, BTreeMap<String, String>>,
    broker_configs: HashMap<NodeId, BTreeMap<String, String>>,
    scram_credentials: HashMap<(String, SaslMechanism), ScramCredential>,
    acls_literal: HashMap<(ResourceType, String), Vec<AclEntry>>,
    acls_prefixed: HashMap<ResourceType, Vec<AclEntry>>,
    client_quotas: HashMap<EntityKey, BTreeMap<String, f64>>,
    delegation_tokens: HashMap<String, DelegationToken>,
}

/// Selects which KIP-73 throttle rate config key to read.
#[derive(Debug, Clone, Copy)]
pub enum ThrottleKind {
    Leader,
    Follower,
}

impl MetadataImage {
    #[must_use]
    pub fn new(cluster_id: Uuid) -> Self {
        Self {
            cluster_id,
            topics: HashMap::new(),
            partitions: HashMap::new(),
            brokers: HashMap::new(),
            topic_configs: HashMap::new(),
            broker_configs: HashMap::new(),
            scram_credentials: HashMap::new(),
            acls_literal: HashMap::new(),
            acls_prefixed: HashMap::new(),
            client_quotas: HashMap::new(),
            delegation_tokens: HashMap::new(),
        }
    }

    #[must_use]
    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    pub fn topics(&self) -> impl Iterator<Item = &TopicRecord> {
        self.topics.values()
    }

    #[must_use]
    pub fn topic(&self, name: &str) -> Option<&TopicRecord> {
        self.topics.get(name)
    }

    #[must_use]
    pub fn partition(&self, topic: &str, idx: i32) -> Option<&PartitionRecord> {
        self.partitions.get(&(topic.to_string(), idx))
    }

    pub fn partitions_of(&self, topic: &str) -> impl Iterator<Item = &PartitionRecord> {
        self.partitions
            .iter()
            .filter(move |((t, _), _)| t == topic)
            .map(|(_, v)| v)
    }

    /// All partitions where a reassignment is currently in flight
    /// (`adding_replicas` or `removing_replicas` non-empty).
    pub fn reassignments_in_flight(&self) -> impl Iterator<Item = &PartitionRecord> + '_ {
        self.topics()
            .flat_map(move |t| self.partitions_of(&t.name))
            .filter(|p| !p.adding_replicas.is_empty() || !p.removing_replicas.is_empty())
    }

    /// Currently-effective config overrides for `topic`, or `None` if no
    /// `V1TopicConfig` record has been applied for this topic since the last
    /// `V1DeleteTopic` (or since image creation).
    #[must_use]
    pub fn topic_config(&self, topic: &str) -> Option<&BTreeMap<String, String>> {
        self.topic_configs.get(topic)
    }

    /// Per-broker config overrides for `node_id`, or `None` if no
    /// `V1BrokerConfig` record has been applied for this broker.
    #[must_use]
    pub fn broker_config(&self, node_id: NodeId) -> Option<&BTreeMap<String, String>> {
        self.broker_configs.get(&node_id)
    }

    /// Returns the throttle rate in bytes/sec for `node_id` and `kind`.
    /// Returns `None` if the config key is absent, unparseable, or is `-1`
    /// (Kafka convention for "disabled / unlimited").
    #[must_use]
    pub fn broker_throttle_rate(&self, node_id: NodeId, kind: ThrottleKind) -> Option<u64> {
        let key = match kind {
            ThrottleKind::Leader => "leader.replication.throttled.rate",
            ThrottleKind::Follower => "follower.replication.throttled.rate",
        };
        let raw = self.broker_config(node_id)?.get(key)?;
        let v: i64 = raw.parse().ok()?;
        #[allow(clippy::cast_sign_loss)]
        if v < 0 { None } else { Some(v as u64) }
    }

    #[must_use]
    pub fn client_quotas(&self) -> &HashMap<EntityKey, BTreeMap<String, f64>> {
        &self.client_quotas
    }

    #[must_use]
    pub fn scram_credential(
        &self,
        user: &str,
        mechanism: SaslMechanism,
    ) -> Option<&ScramCredential> {
        self.scram_credentials.get(&(user.to_string(), mechanism))
    }

    /// All distinct users with at least one SCRAM credential. Order is
    /// unspecified.
    #[must_use]
    pub fn scram_credentials_users(&self) -> Vec<String> {
        self.scram_credentials
            .keys()
            .map(|(u, _)| u.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// All `(mechanism, iterations)` pairs for `user`. Empty if user has
    /// no SCRAM credentials. Order is unspecified.
    #[must_use]
    pub fn scram_credentials_for_user(&self, user: &str) -> Vec<(SaslMechanism, u32)> {
        self.scram_credentials
            .iter()
            .filter(|((u, _), _)| u == user)
            .map(|((_, mech), cred)| (*mech, cred.iterations))
            .collect()
    }

    #[must_use]
    pub fn broker(&self, node_id: NodeId) -> Option<&BrokerRegistrationRecord> {
        self.brokers.get(&node_id)
    }

    pub fn brokers(&self) -> impl Iterator<Item = &BrokerRegistrationRecord> {
        self.brokers.values()
    }

    /// Iterate every ACL that could possibly match `(rt, rn)`:
    /// - all literal entries at `(rt, rn)`
    /// - all prefixed entries whose `resource_name` is a prefix of `rn`
    pub fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        rn: &'a str,
    ) -> impl Iterator<Item = &'a AclEntry> + 'a {
        let literal_iter = self
            .acls_literal
            .get(&(rt, rn.to_string()))
            .into_iter()
            .flatten();
        let prefixed_iter = self
            .acls_prefixed
            .get(&rt)
            .into_iter()
            .flatten()
            .filter(move |e| rn.starts_with(&e.resource_name));
        literal_iter.chain(prefixed_iter)
    }

    /// All ACL entries (literal + prefixed across all resource types).
    /// Used by `DescribeAcls`.
    pub fn all_acls(&self) -> impl Iterator<Item = &AclEntry> {
        self.acls_literal
            .values()
            .flatten()
            .chain(self.acls_prefixed.values().flatten())
    }

    /// Slice 51 (KIP-48): lookup a delegation token by its `token_id`.
    #[must_use]
    pub fn delegation_token_by_id(&self, token_id: &str) -> Option<&DelegationToken> {
        self.delegation_tokens.get(token_id)
    }

    /// Slice 51 (KIP-48): all tokens owned by `owner` (exact match on
    /// the owning [`KafkaPrincipal`]). Order is unspecified.
    #[must_use]
    pub fn delegation_tokens_by_owner(&self, owner: &KafkaPrincipal) -> Vec<&DelegationToken> {
        self.delegation_tokens
            .values()
            .filter(|t| &t.owner == owner)
            .collect()
    }

    /// Slice 51 (KIP-48): tokens that `principal` is allowed to see via
    /// `DescribeDelegationToken` without `DescribeToken` permission —
    /// either as the owner or as a listed renewer. Order is
    /// unspecified.
    #[must_use]
    pub fn delegation_tokens_visible_to(
        &self,
        principal: &KafkaPrincipal,
    ) -> Vec<&DelegationToken> {
        self.delegation_tokens
            .values()
            .filter(|t| &t.owner == principal || t.renewers.iter().any(|r| r == principal))
            .collect()
    }

    /// Slice 51 (KIP-48): every delegation token currently in the
    /// image. Used by `DescribeDelegationToken` for callers with
    /// `DescribeToken` permission on the cluster.
    pub fn all_delegation_tokens(&self) -> impl Iterator<Item = &DelegationToken> {
        self.delegation_tokens.values()
    }

    /// Slice 51 (KIP-48): lookup a delegation token by its HMAC bytes.
    /// `RenewDelegationToken` / `ExpireDelegationToken` identify a token
    /// by HMAC on the wire (not by `token_id`), and the upcoming SCRAM
    /// delegation-token fallback (slice 51 T8) needs the same lookup at
    /// the auth path. Implementation is a linear scan over the small
    /// (per-broker, in-memory) token map — clarity over an explicit
    /// `HMAC→token_id` index until cardinality justifies it.
    #[must_use]
    pub fn delegation_token_by_hmac(&self, hmac: &[u8]) -> Option<&DelegationToken> {
        self.delegation_tokens.values().find(|t| t.hmac == hmac)
    }

    /// Apply one record. Returns the previous record (for `V1Topic` /
    /// `V1BrokerRegistration`) so the caller can observe overwrite cases.
    /// Infallible — pre-validation against the current image happens
    /// in the controller before submitting to Raft. Apply must never
    /// fail on a committed entry.
    #[allow(clippy::too_many_lines)] // exhaustive match over MetadataRecord
    pub fn apply(&mut self, rec: &MetadataRecord) {
        match rec {
            MetadataRecord::V1Topic(t) => {
                self.topics.insert(t.name.clone(), t.clone());
            }
            MetadataRecord::V1Partition(p) => {
                self.partitions
                    .insert((p.topic.clone(), p.partition), p.clone());
            }
            MetadataRecord::V1BrokerRegistration(b) => {
                self.brokers.insert(b.node_id, b.clone());
            }
            MetadataRecord::V1DeleteTopic(d) => {
                self.topics.remove(&d.name);
                self.partitions.retain(|(t, _), _| t != &d.name);
                self.topic_configs.remove(&d.name);
            }
            MetadataRecord::V1TopicConfig(c) => {
                if c.overrides.is_empty() {
                    self.topic_configs.remove(&c.topic);
                } else {
                    self.topic_configs
                        .insert(c.topic.clone(), c.overrides.clone());
                }
            }
            MetadataRecord::V1ScramCredential(r) => {
                self.scram_credentials.insert(
                    (r.user.clone(), r.mechanism),
                    ScramCredential {
                        mechanism: r.mechanism,
                        salt: r.salt.clone(),
                        stored_key: r.stored_key.clone(),
                        server_key: r.server_key.clone(),
                        iterations: r.iterations,
                    },
                );
            }
            MetadataRecord::V1DeleteScramCredential(r) => {
                self.scram_credentials
                    .remove(&(r.user.clone(), r.mechanism));
            }
            MetadataRecord::V1AccessControlEntry(entry) => {
                let bucket = match entry.pattern_type {
                    PatternType::Literal => self
                        .acls_literal
                        .entry((entry.resource_type, entry.resource_name.clone()))
                        .or_default(),
                    PatternType::Prefixed => {
                        self.acls_prefixed.entry(entry.resource_type).or_default()
                    }
                };
                // Last-write-wins on full-tuple equality.
                bucket.retain(|e| e != entry);
                bucket.push(entry.clone());
            }
            MetadataRecord::V1DeleteAccessControlEntry(filter) => {
                self.acls_literal.retain(|_, v| {
                    v.retain(|e| !filter.matches(e));
                    !v.is_empty()
                });
                self.acls_prefixed.retain(|_, v| {
                    v.retain(|e| !filter.matches(e));
                    !v.is_empty()
                });
            }
            MetadataRecord::V1BrokerConfig(rec) => {
                let entry = self.broker_configs.entry(rec.node_id).or_default();
                match &rec.config_value {
                    Some(v) => {
                        entry.insert(rec.config_name.clone(), v.clone());
                    }
                    None => {
                        entry.remove(&rec.config_name);
                    }
                }
            }
            MetadataRecord::V1ClientQuota(rec) => {
                let key = canonicalize_entity(
                    rec.entity
                        .iter()
                        .map(|e| (e.entity_type.clone(), e.entity_name.clone()))
                        .collect(),
                );
                let configs = self.client_quotas.entry(key).or_default();
                match rec.config_value {
                    Some(v) => {
                        configs.insert(rec.config_key.clone(), v);
                    }
                    None => {
                        configs.remove(&rec.config_key);
                    }
                }
            }
            // Slice 51 (KIP-48): replacement semantics — same
            // `token_id` overwrites the prior entry (used by Create
            // and Renew). Tombstone removes by `token_id`.
            MetadataRecord::V1DelegationToken(rec) => {
                self.delegation_tokens
                    .insert(rec.token_id.clone(), DelegationToken::from_record(rec));
            }
            MetadataRecord::V1DeleteDelegationToken(rec) => {
                self.delegation_tokens.remove(&rec.token_id);
            }
            MetadataRecord::V1UnregisterBroker(rec) => {
                // Idempotent: applying against an unknown `node_id` is
                // a no-op.
                self.brokers.remove(&rec.node_id);
            }
        }
    }

    /// Synchronous pre-validation: returns `Ok` if the record would be a
    /// no-conflict apply, otherwise the appropriate error. Used by
    /// `Controller::submit_change` before forwarding to openraft.
    pub fn validate(&self, rec: &MetadataRecord) -> Result<(), MetadataError> {
        match rec {
            MetadataRecord::V1Topic(t) => {
                if let Some(existing) = self.topics.get(&t.name) {
                    // Updating an existing topic is allowed only if it's a
                    // strict partition-count expansion that preserves
                    // identity: same topic_id, same replication_factor,
                    // partitions strictly growing. CreatePartitions emits
                    // exactly this. Identical re-submits stay rejected so
                    // CreateTopics' idempotency contract still holds.
                    if existing.topic_id != t.topic_id
                        || existing.replication_factor != t.replication_factor
                        || t.partitions <= existing.partitions
                    {
                        return Err(MetadataError::TopicExists(t.name.clone()));
                    }
                    return Ok(());
                }
                if t.partitions <= 0 {
                    return Err(MetadataError::InvalidRecord("partitions must be > 0"));
                }
                Ok(())
            }
            MetadataRecord::V1Partition(p) => {
                if !self.topics.contains_key(&p.topic) {
                    return Err(MetadataError::UnknownTopic(p.topic.clone()));
                }
                Ok(())
            }
            MetadataRecord::V1DeleteTopic(d) => {
                if !self.topics.contains_key(&d.name) {
                    return Err(MetadataError::UnknownTopic(d.name.clone()));
                }
                Ok(())
            }
            MetadataRecord::V1TopicConfig(c) => {
                if !self.topics.contains_key(&c.topic) {
                    return Err(MetadataError::UnknownTopic(c.topic.clone()));
                }
                Ok(())
            }
            MetadataRecord::V1BrokerRegistration(_)
            | MetadataRecord::V1ScramCredential(_)
            | MetadataRecord::V1DeleteScramCredential(_)
            | MetadataRecord::V1AccessControlEntry(_)
            | MetadataRecord::V1DeleteAccessControlEntry(_)
            | MetadataRecord::V1BrokerConfig(_)
            | MetadataRecord::V1ClientQuota(_)
            // Slice 51 (KIP-48): delegation-token records have no
            // topic-store concerns and admission is gated by the
            // handler-side checks (KIP-48 §protocol errors), so the
            // image-level validate is unconditional Ok.
            | MetadataRecord::V1DelegationToken(_)
            | MetadataRecord::V1DeleteDelegationToken(_)
            // UnregisterBroker (KIP-185 / api_key 64). The handler-side
            // existence check + Cluster:Alter ACL gate provide all the
            // pre-validation we need; image-level apply is idempotent.
            | MetadataRecord::V1UnregisterBroker(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AclEntryFilter, AclOperation, PermissionType};
    use crate::records::{
        BrokerConfigRecord, ClientQuotaRecord, DeleteDelegationTokenRecord,
        DeleteScramCredentialRecord, DeleteTopicRecord, QuotaEntity, ScramCredentialRecord,
    };

    fn img() -> MetadataImage {
        MetadataImage::new(Uuid::nil())
    }

    fn topic(name: &str, partitions: i32) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions,
            replication_factor: 1,
        })
    }

    #[test]
    fn apply_topic_inserts() {
        let mut m = img();
        m.apply(&topic("t", 3));
        assert!(m.topic("t").is_some());
    }

    #[test]
    fn apply_delete_clears_partitions() {
        let mut m = img();
        m.apply(&topic("t", 2));
        m.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
        m.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 1,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
        assert_eq!(m.partitions_of("t").count(), 2);
        m.apply(&MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "t".into(),
        }));
        assert!(m.topic("t").is_none());
        assert_eq!(m.partitions_of("t").count(), 0);
    }

    #[test]
    fn validate_topic_exists_rejected() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let err = m.validate(&topic("t", 1)).unwrap_err();
        assert!(matches!(err, MetadataError::TopicExists(_)));
    }

    #[test]
    fn validate_topic_partition_count_increase_allowed() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let existing = m.topic("t").unwrap().clone();
        let updated = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: existing.topic_id,
            partitions: 3,
            replication_factor: existing.replication_factor,
        });
        assert!(m.validate(&updated).is_ok());
    }

    #[test]
    fn validate_topic_partition_count_decrease_rejected() {
        let mut m = img();
        m.apply(&topic("t", 3));
        let existing = m.topic("t").unwrap().clone();
        let updated = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: existing.topic_id,
            partitions: 1,
            replication_factor: existing.replication_factor,
        });
        let err = m.validate(&updated).unwrap_err();
        assert!(matches!(err, MetadataError::TopicExists(_)));
    }

    #[test]
    fn validate_delete_unknown_topic_rejected() {
        let m = img();
        let err = m
            .validate(&MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "ghost".into(),
            }))
            .unwrap_err();
        assert!(matches!(err, MetadataError::UnknownTopic(_)));
    }

    #[test]
    fn validate_partition_for_unknown_topic_rejected() {
        let m = img();
        let p = MetadataRecord::V1Partition(PartitionRecord {
            topic: "ghost".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
        });
        let err = m.validate(&p).unwrap_err();
        assert!(matches!(err, MetadataError::UnknownTopic(_)));
    }

    #[test]
    fn broker_registration_is_idempotent() {
        let mut m = img();
        let b = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 1,
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
        });
        m.apply(&b);
        m.apply(&b);
        assert_eq!(m.brokers().count(), 1);
    }

    #[test]
    fn apply_topic_config_inserts() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides: overrides.clone(),
            },
        ));
        assert_eq!(m.topic_config("t"), Some(&overrides));
    }

    #[test]
    fn apply_topic_config_replaces_previous() {
        let mut m = img();
        m.apply(&topic("t", 1));

        let mut first = std::collections::BTreeMap::new();
        first.insert("retention.ms".to_string(), "60000".to_string());
        first.insert("segment.bytes".to_string(), "1024".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides: first,
            },
        ));

        let mut second = std::collections::BTreeMap::new();
        second.insert("retention.ms".to_string(), "120000".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides: second.clone(),
            },
        ));

        // segment.bytes is GONE — last-write-wins is authoritative.
        assert_eq!(m.topic_config("t"), Some(&second));
    }

    #[test]
    fn delete_topic_clears_configs() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides,
            },
        ));
        m.apply(&MetadataRecord::V1DeleteTopic(
            crate::records::DeleteTopicRecord { name: "t".into() },
        ));
        assert!(m.topic_config("t").is_none());
    }

    #[test]
    fn validate_topic_config_for_unknown_topic_rejected() {
        let m = img();
        let r = MetadataRecord::V1TopicConfig(crate::records::TopicConfigRecord {
            topic: "ghost".into(),
            overrides: std::collections::BTreeMap::new(),
        });
        let err = m.validate(&r).unwrap_err();
        assert!(matches!(err, MetadataError::UnknownTopic(_)));
    }

    #[test]
    fn apply_scram_credential_stores() {
        let mut m = img();
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        }));
        let got = m.scram_credential("alice", crabka_security::SaslMechanism::ScramSha512);
        assert!(got.is_some());
        assert_eq!(got.unwrap().iterations, 4096);
    }

    #[test]
    fn apply_scram_credential_last_write_wins() {
        let mut m = img();
        let mech = crabka_security::SaslMechanism::ScramSha512;
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: mech,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        }));
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: mech,
            salt: vec![9; 16],
            stored_key: vec![9; 64],
            server_key: vec![9; 64],
            iterations: 8192,
        }));
        let got = m.scram_credential("alice", mech).unwrap();
        assert_eq!(got.iterations, 8192);
        assert_eq!(got.salt, vec![9; 16]);
    }

    #[test]
    fn delete_scram_credential_removes() {
        let mut m = img();
        let mech = crabka_security::SaslMechanism::ScramSha512;
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: mech,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        }));
        m.apply(&MetadataRecord::V1DeleteScramCredential(
            DeleteScramCredentialRecord {
                user: "alice".into(),
                mechanism: mech,
            },
        ));
        assert!(m.scram_credential("alice", mech).is_none());
    }

    fn topic_read_for_alice() -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "foo".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }
    }

    fn topic_prefixed_team() -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "team-".into(),
            pattern_type: PatternType::Prefixed,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }
    }

    #[test]
    fn apply_v1_access_control_entry_literal_stores_in_literal_map() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_read_for_alice()));
        let mut hits: Vec<_> = m.matching_acls(ResourceType::Topic, "foo").collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits.pop().unwrap().resource_name, "foo");
    }

    #[test]
    fn apply_v1_access_control_entry_prefixed_stores_in_prefixed_vec() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_prefixed_team()));
        let hits: Vec<_> = m.matching_acls(ResourceType::Topic, "team-foo").collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].resource_name, "team-");
        // Non-matching resource: empty.
        let none: Vec<_> = m.matching_acls(ResourceType::Topic, "other").collect();
        assert!(none.is_empty());
    }

    #[test]
    fn matching_acls_combines_literal_and_prefixed() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_read_for_alice()));
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_prefixed_team()));
        let hits_foo: Vec<_> = m.matching_acls(ResourceType::Topic, "foo").collect();
        let hits_team: Vec<_> = m.matching_acls(ResourceType::Topic, "team-x").collect();
        assert_eq!(hits_foo.len(), 1);
        assert_eq!(hits_team.len(), 1);
    }

    #[test]
    fn apply_v1_delete_access_control_entry_removes_matching() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_read_for_alice()));
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_prefixed_team()));
        let filter = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            pattern_type: Some(PatternType::Literal),
            ..AclEntryFilter::default()
        };
        m.apply(&MetadataRecord::V1DeleteAccessControlEntry(filter));
        let hits_foo: Vec<_> = m.matching_acls(ResourceType::Topic, "foo").collect();
        let hits_team: Vec<_> = m.matching_acls(ResourceType::Topic, "team-x").collect();
        assert_eq!(hits_foo.len(), 0); // literal removed
        assert_eq!(hits_team.len(), 1); // prefixed survives
    }

    #[test]
    fn apply_v1_delete_access_control_entry_no_match_is_noop() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_read_for_alice()));
        let filter = AclEntryFilter {
            resource_type: Some(ResourceType::Group),
            ..AclEntryFilter::default()
        };
        m.apply(&MetadataRecord::V1DeleteAccessControlEntry(filter));
        let hits: Vec<_> = m.matching_acls(ResourceType::Topic, "foo").collect();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn all_acls_returns_every_entry() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_read_for_alice()));
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_prefixed_team()));
        assert_eq!(m.all_acls().count(), 2);
    }

    #[test]
    fn reassignments_in_flight_excludes_idle_partitions() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
        assert_eq!(img.reassignments_in_flight().count(), 0);
    }

    #[test]
    fn reassignments_in_flight_returns_partitions_with_adding() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3, 4],
            isr: vec![1, 2, 3],
            leader_epoch: 0,
            adding_replicas: vec![4],
            removing_replicas: vec![],
        }));
        let rows: Vec<_> = img.reassignments_in_flight().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].adding_replicas, vec![4]);
    }

    #[test]
    fn reassignments_in_flight_returns_partitions_with_removing() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![3],
        }));
        let rows: Vec<_> = img.reassignments_in_flight().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].removing_replicas, vec![3]);
    }

    #[test]
    fn reassignments_in_flight_covers_multiple_topics() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for name in ["foo", "bar"] {
            img.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: uuid::Uuid::nil(),
                partitions: 1,
                replication_factor: 3,
            }));
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: 1,
                replicas: vec![1, 2, 3, 4],
                isr: vec![1, 2, 3],
                leader_epoch: 0,
                adding_replicas: vec![4],
                removing_replicas: vec![],
            }));
        }
        assert_eq!(img.reassignments_in_flight().count(), 2);
    }

    #[test]
    fn broker_config_set_inserts_into_image() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        let bc = img.broker_config(1).expect("broker config");
        assert_eq!(
            bc.get("leader.replication.throttled.rate"),
            Some(&"2048".to_string())
        );
    }

    #[test]
    fn broker_config_delete_removes_from_image() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: None,
        }));
        let bc = img.broker_config(1).expect("broker_configs entry retained");
        assert!(bc.get("leader.replication.throttled.rate").is_none());
    }

    #[test]
    fn broker_throttle_rate_parses_positive_value() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        assert_eq!(
            img.broker_throttle_rate(1, ThrottleKind::Leader),
            Some(2048)
        );
    }

    #[test]
    fn broker_throttle_rate_returns_none_for_negative_one() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("-1".into()),
        }));
        assert!(img.broker_throttle_rate(1, ThrottleKind::Leader).is_none());
    }

    #[test]
    fn client_quota_apply_inserts_canonicalized() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        // Input order: (user, client-id) — should canonicalize to (client-id, user).
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app1".into()),
                },
            ],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let key: EntityKey = vec![
            ("client-id".into(), Some("app1".into())),
            ("user".into(), Some("alice".into())),
        ];
        let configs = img
            .client_quotas()
            .get(&key)
            .expect("entry under canonical key");
        assert_eq!(configs.get("producer_byte_rate"), Some(&1024.0));
    }

    #[test]
    fn client_quota_apply_delete_removes_key() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
            }],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
            }],
            config_key: "producer_byte_rate".into(),
            config_value: None,
        }));
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let configs = img.client_quotas().get(&key).expect("entry retained");
        assert!(configs.get("producer_byte_rate").is_none());
    }

    #[test]
    fn client_quota_default_entity_uses_none_name() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".into(),
                entity_name: None,
            }],
            config_key: "producer_byte_rate".into(),
            config_value: Some(512.0),
        }));
        let key: EntityKey = vec![("user".into(), None)];
        assert!(img.client_quotas().contains_key(&key));
    }

    #[test]
    fn canonicalize_sorts_alphabetically_by_entity_type() {
        let input = vec![
            ("user".to_string(), Some("alice".to_string())),
            ("client-id".to_string(), Some("app1".to_string())),
        ];
        let canon = canonicalize_entity(input);
        assert_eq!(canon[0].0, "client-id");
        assert_eq!(canon[1].0, "user");
    }

    #[test]
    fn scram_credentials_users_returns_distinct_users() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1, 2, 3],
            stored_key: vec![4, 5, 6],
            server_key: vec![7, 8, 9],
            iterations: 4096,
        }));
        img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "bob".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1, 2, 3],
            stored_key: vec![4, 5, 6],
            server_key: vec![7, 8, 9],
            iterations: 4096,
        }));
        let mut users = img.scram_credentials_users();
        users.sort();
        assert_eq!(users, vec!["alice".to_string(), "bob".to_string()]);
    }

    fn principal(pt: &str, name: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: pt.into(),
            name: name.into(),
        }
    }

    fn dt_record(
        token_id: &str,
        owner: KafkaPrincipal,
        expiry_timestamp_ms: i64,
        renewers: Vec<KafkaPrincipal>,
    ) -> MetadataRecord {
        MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: token_id.into(),
            owner,
            hmac: vec![0x42; 32],
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms,
            max_timestamp_ms: 10_000,
            renewers,
        })
    }

    #[test]
    fn apply_delegation_token_insert_and_replace() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let alice = principal("User", "alice");

        img.apply(&dt_record("tok-1", alice.clone(), 5_000, vec![]));
        let got = img.delegation_token_by_id("tok-1").expect("token present");
        assert_eq!(got.expiry_timestamp_ms, 5_000);
        assert_eq!(got.owner, alice);

        // Same token_id, different expiry — replace, not duplicate.
        img.apply(&dt_record("tok-1", alice.clone(), 7_500, vec![]));
        let got = img.delegation_token_by_id("tok-1").expect("token present");
        assert_eq!(got.expiry_timestamp_ms, 7_500);
        assert_eq!(img.all_delegation_tokens().count(), 1);
    }

    #[test]
    fn apply_delete_delegation_token_removes_from_image() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let alice = principal("User", "alice");

        img.apply(&dt_record("tok-1", alice, 5_000, vec![]));
        assert!(img.delegation_token_by_id("tok-1").is_some());

        img.apply(&MetadataRecord::V1DeleteDelegationToken(
            DeleteDelegationTokenRecord {
                token_id: "tok-1".into(),
            },
        ));
        assert!(img.delegation_token_by_id("tok-1").is_none());
        assert_eq!(img.all_delegation_tokens().count(), 0);
    }

    #[test]
    fn delegation_token_by_hmac_finds_token_by_hmac_bytes() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let alice = principal("User", "alice");
        let bob = principal("User", "bob");

        let hmac_a = vec![0xAA; 32];
        let hmac_b = vec![0xBB; 32];
        img.apply(&MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: "tok-a".into(),
            owner: alice,
            hmac: hmac_a.clone(),
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 5_000,
            max_timestamp_ms: 10_000,
            renewers: vec![],
        }));
        img.apply(&MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: "tok-b".into(),
            owner: bob,
            hmac: hmac_b.clone(),
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 5_000,
            max_timestamp_ms: 10_000,
            renewers: vec![],
        }));

        let found_a = img
            .delegation_token_by_hmac(&hmac_a)
            .expect("hmac_a present");
        assert_eq!(found_a.token_id, "tok-a");
        let found_b = img
            .delegation_token_by_hmac(&hmac_b)
            .expect("hmac_b present");
        assert_eq!(found_b.token_id, "tok-b");
        assert!(img.delegation_token_by_hmac(&[0xCC; 32]).is_none());
    }

    #[test]
    fn delegation_tokens_by_owner_filters_correctly() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let alice = principal("User", "alice");
        let bob = principal("User", "bob");

        img.apply(&dt_record("a-1", alice.clone(), 5_000, vec![]));
        img.apply(&dt_record("a-2", alice.clone(), 6_000, vec![bob.clone()]));
        img.apply(&dt_record("b-1", bob.clone(), 7_000, vec![]));

        let alice_tokens = img.delegation_tokens_by_owner(&alice);
        assert_eq!(alice_tokens.len(), 2);
        assert!(alice_tokens.iter().all(|t| t.owner == alice));

        let bob_tokens = img.delegation_tokens_by_owner(&bob);
        assert_eq!(bob_tokens.len(), 1);
        assert_eq!(bob_tokens[0].token_id, "b-1");

        // visible_to: bob owns b-1 and is renewer on a-2 → 2 tokens.
        let bob_visible = img.delegation_tokens_visible_to(&bob);
        assert_eq!(bob_visible.len(), 2);
        let mut ids: Vec<&str> = bob_visible.iter().map(|t| t.token_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["a-2", "b-1"]);
    }

    #[test]
    fn scram_credentials_for_user_returns_pairs() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1, 2, 3],
            stored_key: vec![4, 5, 6],
            server_key: vec![7, 8, 9],
            iterations: 8192,
        }));
        let pairs = img.scram_credentials_for_user("alice");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, SaslMechanism::ScramSha512);
        assert_eq!(pairs[0].1, 8192);
        assert!(img.scram_credentials_for_user("ghost").is_empty());
    }
}
