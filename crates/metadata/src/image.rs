//! Immutable snapshot of the cluster's metadata state. Mutated only by
//! [`MetadataImage::apply`] (called from the Raft state machine), and
//! read everywhere else via shared references / `Arc` clones.

use std::collections::{BTreeMap, HashMap};

use uuid::Uuid;

use crabka_security::{KafkaPrincipal, SaslMechanism, ScramCredential};

use crate::acl::{AclEntry, PatternType, ResourceType};
use crate::error::MetadataError;
use crate::records::{
    BrokerConfigRecord, BrokerRegistrationRecord, ClientMetricsConfigRecord, ClientQuotaRecord,
    DelegationTokenRecord, FeatureLevelRecord, FeaturesEpochRecord, KRaftVersionRecord,
    MetadataRecord, NodeId, PartitionRecord, QuotaEntity, ScramCredentialRecord, TopicConfigRecord,
    TopicRecord, VotersRecord,
};

pub type EntityKey = Vec<(String, Option<String>)>;

/// In-memory image type for a single delegation
/// token (KIP-48). Mirrors [`DelegationTokenRecord`] minus any tombstone
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

/// Static discriminant name for a [`MetadataRecord`], used as tracing span
/// context (the record itself is skipped to keep spans cheap and PII-free).
fn record_variant(rec: &MetadataRecord) -> &'static str {
    match rec {
        MetadataRecord::V1Topic(_) => "V1Topic",
        MetadataRecord::V1Partition(_) => "V1Partition",
        MetadataRecord::V1BrokerRegistration(_) => "V1BrokerRegistration",
        MetadataRecord::V1DeleteTopic(_) => "V1DeleteTopic",
        MetadataRecord::V1TopicConfig(_) => "V1TopicConfig",
        MetadataRecord::V1ScramCredential(_) => "V1ScramCredential",
        MetadataRecord::V1DeleteScramCredential(_) => "V1DeleteScramCredential",
        MetadataRecord::V1AccessControlEntry(_) => "V1AccessControlEntry",
        MetadataRecord::V1DeleteAccessControlEntry(_) => "V1DeleteAccessControlEntry",
        MetadataRecord::V1BrokerConfig(_) => "V1BrokerConfig",
        MetadataRecord::V1ClientQuota(_) => "V1ClientQuota",
        MetadataRecord::V1DelegationToken(_) => "V1DelegationToken",
        MetadataRecord::V1DeleteDelegationToken(_) => "V1DeleteDelegationToken",
        MetadataRecord::V1UnregisterBroker(_) => "V1UnregisterBroker",
        MetadataRecord::V1KRaftVersion(_) => "V1KRaftVersion",
        MetadataRecord::V1Voters(_) => "V1Voters",
        MetadataRecord::V1FeatureLevel(_) => "V1FeatureLevel",
        MetadataRecord::V1ClientMetricsConfig(_) => "V1ClientMetricsConfig",
        MetadataRecord::V1FeaturesEpoch(_) => "V1FeaturesEpoch",
        MetadataRecord::V1PartitionDirAssignment(_) => "V1PartitionDirAssignment",
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetadataImage {
    cluster_id: Uuid,
    topics: HashMap<String, TopicRecord>,
    /// KIP-516 reverse index: topic UUID -> topic name. Maintained in
    /// `apply()` alongside `topics`; rebuilt on snapshot replay because
    /// every record (including snapshot installs) flows through `apply()`.
    topic_ids: HashMap<Uuid, String>,
    partitions: HashMap<(String, i32), PartitionRecord>,
    brokers: HashMap<NodeId, BrokerRegistrationRecord>,
    topic_configs: HashMap<String, BTreeMap<String, String>>,
    broker_configs: HashMap<NodeId, BTreeMap<String, String>>,
    client_metrics_configs: HashMap<String, BTreeMap<String, String>>,
    scram_credentials: HashMap<(String, SaslMechanism), ScramCredential>,
    acls_literal: HashMap<(ResourceType, String), Vec<AclEntry>>,
    acls_prefixed: HashMap<ResourceType, Vec<AclEntry>>,
    client_quotas: HashMap<EntityKey, BTreeMap<String, f64>>,
    delegation_tokens: HashMap<String, DelegationToken>,
    kraft_version: u16,
    voters: crate::voters::VoterSet,
    feature_levels: BTreeMap<String, i16>,
    /// KIP-584 finalized-features epoch. `-1` until the first
    /// `V1FeatureLevel` record applies, then monotonically increasing
    /// (one bump per applied record). Deterministic across replicas
    /// because records apply in committed-log order on every node.
    features_epoch: i64,
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
            topic_ids: HashMap::new(),
            partitions: HashMap::new(),
            brokers: HashMap::new(),
            topic_configs: HashMap::new(),
            broker_configs: HashMap::new(),
            client_metrics_configs: HashMap::new(),
            scram_credentials: HashMap::new(),
            acls_literal: HashMap::new(),
            acls_prefixed: HashMap::new(),
            client_quotas: HashMap::new(),
            delegation_tokens: HashMap::new(),
            kraft_version: 0,
            voters: crate::voters::VoterSet::default(),
            feature_levels: BTreeMap::new(),
            features_epoch: -1,
        }
    }

    #[must_use]
    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    pub fn topics(&self) -> impl Iterator<Item = &TopicRecord> {
        self.topics.values()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(topic = %name))]
    #[must_use]
    pub fn topic(&self, name: &str) -> Option<&TopicRecord> {
        self.topics.get(name)
    }

    /// KIP-516: resolve a topic by its UUID. O(1) via the `topic_ids` index.
    #[tracing::instrument(level = "debug", skip_all, fields(topic_id = %id))]
    #[must_use]
    pub fn topic_by_id(&self, id: &Uuid) -> Option<&TopicRecord> {
        let name = self.topic_ids.get(id)?;
        self.topics.get(name)
    }

    /// KIP-516: resolve a topic name by its UUID.
    #[must_use]
    pub fn topic_name_by_id(&self, id: &Uuid) -> Option<&str> {
        self.topic_ids.get(id).map(String::as_str)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(topic = %topic, partition = idx))]
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

    /// The live partition count for `topic`, derived from the partitions
    /// map rather than the stored `TopicRecord.partitions` field. This is
    /// the authoritative count for the KIP-631 round-trip (the KIP-631
    /// `TopicRecord` carries no partition count) and for the `validate`
    /// partition-count-grew check. Returns 0 for an unknown topic or one
    /// with no partition records yet applied.
    #[must_use]
    pub fn topic_partition_count(&self, topic: &str) -> i32 {
        i32::try_from(self.partitions_of(topic).count()).unwrap_or(i32::MAX)
    }

    /// Single-pass iterator over every partition in the image, yielding
    /// the flat `(topic_name, partition_index)` key alongside the record.
    /// O(P) in total partition count — the cluster-wide maintenance loops
    /// (failover, rebalance, reassignment, metrics) use this instead of
    /// `topics().flat_map(partitions_of)`, which is O(topics × P).
    pub fn all_partitions(&self) -> impl Iterator<Item = (&(String, i32), &PartitionRecord)> {
        self.partitions.iter()
    }

    /// All partitions where a reassignment is currently in flight
    /// (`adding_replicas` or `removing_replicas` non-empty).
    pub fn reassignments_in_flight(&self) -> impl Iterator<Item = &PartitionRecord> + '_ {
        self.all_partitions()
            .map(|(_, p)| p)
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

    /// Override map for a single KIP-714 client-metrics subscription.
    #[must_use]
    pub fn client_metrics_config(&self, name: &str) -> Option<&BTreeMap<String, String>> {
        self.client_metrics_configs.get(name)
    }

    /// All configured client-metrics subscriptions, `(name, overrides)`.
    pub fn client_metrics_subscriptions(
        &self,
    ) -> impl Iterator<Item = (&String, &BTreeMap<String, String>)> {
        self.client_metrics_configs.iter()
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

    /// KIP-903: the broker epoch (registration commit offset) for `node_id`,
    /// or `None` if the broker is not registered in this image.
    #[must_use]
    pub fn broker_epoch(&self, node_id: NodeId) -> Option<i64> {
        self.brokers.get(&node_id).map(|b| b.broker_epoch)
    }

    pub fn brokers(&self) -> impl Iterator<Item = &BrokerRegistrationRecord> {
        self.brokers.values()
    }

    #[must_use]
    pub fn kraft_version(&self) -> u16 {
        self.kraft_version
    }

    #[must_use]
    pub fn voters(&self) -> &crate::voters::VoterSet {
        &self.voters
    }

    /// Iterate every ACL that could possibly match `(rt, rn)`:
    /// - all literal entries at `(rt, rn)`
    /// - all literal entries at `(rt, "*")` — the `WILDCARD_RESOURCE`, which
    ///   matches every resource of that type (see Kafka's `AclAuthorizer` /
    ///   `StandardAuthorizer`; this is what `kafka-acls --topic '*'` produces)
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
        // Literal "*" is the WILDCARD_RESOURCE and matches every resource of
        // this type. Skip it when `rn` is already "*" to avoid duplicating the
        // entries already produced by `literal_iter`.
        let wildcard_iter = (rn != "*")
            .then(|| self.acls_literal.get(&(rt, "*".to_string())))
            .into_iter()
            .flatten()
            .flatten();
        let prefixed_iter = self
            .acls_prefixed
            .get(&rt)
            .into_iter()
            .flatten()
            .filter(move |e| rn.starts_with(&e.resource_name));
        literal_iter.chain(wildcard_iter).chain(prefixed_iter)
    }

    /// All ACL entries (literal + prefixed across all resource types).
    /// Used by `DescribeAcls`.
    pub fn all_acls(&self) -> impl Iterator<Item = &AclEntry> {
        self.acls_literal
            .values()
            .flatten()
            .chain(self.acls_prefixed.values().flatten())
    }

    /// Look up a delegation token by its `token_id` (KIP-48).
    #[must_use]
    pub fn delegation_token_by_id(&self, token_id: &str) -> Option<&DelegationToken> {
        self.delegation_tokens.get(token_id)
    }

    /// All tokens owned by `owner` (KIP-48; exact match on
    /// the owning [`KafkaPrincipal`]). Order is unspecified.
    #[must_use]
    pub fn delegation_tokens_by_owner(&self, owner: &KafkaPrincipal) -> Vec<&DelegationToken> {
        self.delegation_tokens
            .values()
            .filter(|t| &t.owner == owner)
            .collect()
    }

    /// Tokens that `principal` is allowed to see via
    /// `DescribeDelegationToken` without `DescribeToken` permission —
    /// either as the owner or as a listed renewer (KIP-48). Order is
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

    /// Every delegation token currently in the
    /// image (KIP-48). Used by `DescribeDelegationToken` for callers with
    /// `DescribeToken` permission on the cluster.
    pub fn all_delegation_tokens(&self) -> impl Iterator<Item = &DelegationToken> {
        self.delegation_tokens.values()
    }

    /// KIP-48: lookup a delegation token by its HMAC bytes.
    /// `RenewDelegationToken` / `ExpireDelegationToken` identify a token
    /// by HMAC on the wire (not by `token_id`), and the SCRAM
    /// delegation-token fallback needs the same lookup at
    /// the auth path. Implementation is a linear scan over the small
    /// (per-broker, in-memory) token map — clarity over an explicit
    /// `HMAC→token_id` index until cardinality justifies it.
    #[must_use]
    pub fn delegation_token_by_hmac(&self, hmac: &[u8]) -> Option<&DelegationToken> {
        self.delegation_tokens.values().find(|t| t.hmac == hmac)
    }

    /// KIP-584: finalized feature levels, keyed by feature name. Empty
    /// until an `UpdateFeatures` call lands a `V1FeatureLevel` record.
    #[must_use]
    pub fn finalized_features(&self) -> &BTreeMap<String, i16> {
        &self.feature_levels
    }

    /// KIP-584 finalized-features epoch. `-1` ("unknown") until the first
    /// feature is finalized.
    #[must_use]
    pub fn finalized_features_epoch(&self) -> i64 {
        self.features_epoch
    }

    /// The finalized `metadata.version` level, or `None` if no
    /// `V1FeatureLevel` for `metadata.version` has been applied
    /// (a pre-bootstrap / legacy image — `MetadataVersion.UNKNOWN`).
    #[must_use]
    pub fn finalized_metadata_version(&self) -> Option<i16> {
        self.feature_levels
            .get(crate::metadata_version::METADATA_VERSION_FEATURE)
            .copied()
    }

    /// KIP-584: the finalized level for an arbitrary feature, or `None` if it
    /// has not been finalized. Generic counterpart to
    /// [`Self::finalized_metadata_version`].
    #[must_use]
    pub fn finalized_feature(&self, name: &str) -> Option<i16> {
        self.feature_levels.get(name).copied()
    }

    /// The minimum `metadata.version` level the live image requires: the
    /// floor a downgrade must not drop below. Rises with feature-gated
    /// state present in the image (`KRaft` SCRAM creds, delegation tokens).
    /// Baseline is `METADATA_VERSION_MIN`.
    #[must_use]
    pub fn min_required_metadata_version(&self) -> i16 {
        use crate::metadata_version::{
            DELEGATION_TOKEN_MIN_LEVEL, METADATA_VERSION_MIN, SCRAM_MIN_LEVEL,
        };
        let mut floor = METADATA_VERSION_MIN;
        if !self.scram_credentials.is_empty() {
            floor = floor.max(SCRAM_MIN_LEVEL);
        }
        if !self.delegation_tokens.is_empty() {
            floor = floor.max(DELEGATION_TOKEN_MIN_LEVEL);
        }
        floor
    }

    /// Apply one record. Returns the previous record (for `V1Topic` /
    /// `V1BrokerRegistration`) so the caller can observe overwrite cases.
    /// Infallible — pre-validation against the current image happens
    /// in the controller before submitting to Raft. Apply must never
    /// fail on a committed entry.
    #[tracing::instrument(level = "info", skip_all, fields(record = record_variant(rec)))]
    #[allow(clippy::too_many_lines)] // exhaustive match over MetadataRecord
    pub fn apply(&mut self, rec: &MetadataRecord) {
        match rec {
            MetadataRecord::V1Topic(t) => {
                // If a topic with this name already exists under a different
                // id, drop the stale id entry before re-indexing.
                if let Some(prev) = self.topics.get(&t.name)
                    && prev.topic_id != t.topic_id
                {
                    self.topic_ids.remove(&prev.topic_id);
                }
                self.topic_ids.insert(t.topic_id, t.name.clone());
                // The denormalized `partitions` count is a derived cache of the
                // partitions map (KIP-631 `TopicRecord` carries no partition
                // count), maintained incrementally as `V1Partition` records
                // apply. A brand-new topic starts at 0 and the partition records
                // that follow it in log order restore the real count; a
                // re-registration of an existing topic keeps its partitions in
                // the map, so preserve the running count rather than trusting
                // the (KIP-631-zeroed) record field.
                let mut rec = t.clone();
                rec.partitions = self.topics.get(&t.name).map_or(0, |prev| prev.partitions);
                self.topics.insert(t.name.clone(), rec);
            }
            MetadataRecord::V1Partition(p) => {
                // Maintain the owning topic's denormalized partition count / RF
                // incrementally. Re-scanning the whole partitions map on every
                // record (the previous approach) made log/snapshot replay
                // O(P²); bumping the cached count only when a *new* partition
                // key lands keeps apply O(1) while leaving the count a true
                // derived view of the partitions map.
                let is_new = self
                    .partitions
                    .insert((p.topic.clone(), p.partition), p.clone())
                    .is_none();
                if let Some(t) = self.topics.get_mut(&p.topic) {
                    if is_new {
                        t.partitions = t.partitions.saturating_add(1);
                    }
                    t.replication_factor =
                        i16::try_from(p.replicas.len()).unwrap_or(t.replication_factor);
                }
            }
            MetadataRecord::V1BrokerRegistration(b) => {
                self.brokers.insert(b.node_id, b.clone());
            }
            MetadataRecord::V1DeleteTopic(d) => {
                if let Some(prev) = self.topics.get(&d.name) {
                    self.topic_ids.remove(&prev.topic_id);
                }
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
            // Replacement semantics (KIP-48) — same
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
            MetadataRecord::V1KRaftVersion(r) => {
                self.kraft_version = r.kraft_version;
            }
            MetadataRecord::V1Voters(r) => {
                self.voters = r.voters.clone();
            }
            MetadataRecord::V1FeatureLevel(rec) => {
                if rec.level == 0 {
                    self.feature_levels.remove(&rec.name);
                } else {
                    self.feature_levels.insert(rec.name.clone(), rec.level);
                }
                // Monotonic epoch: -1 -> 0 on the first record, then +1.
                self.features_epoch = self.features_epoch.saturating_add(1).max(0);
            }
            MetadataRecord::V1ClientMetricsConfig(c) => {
                if c.configs.is_empty() {
                    self.client_metrics_configs.remove(&c.name);
                } else {
                    self.client_metrics_configs
                        .insert(c.name.clone(), c.configs.clone());
                }
            }
            // Snapshot-only: restore the epoch verbatim rather than bumping.
            // Emitted last by `to_records` so it overrides the value the
            // preceding `V1FeatureLevel` replays accumulated. Never reaches
            // the live log (not a controller-submittable change).
            MetadataRecord::V1FeaturesEpoch(rec) => {
                self.features_epoch = rec.epoch;
            }
            // KIP-858 directory-assignment delta: merge ONLY the reporting
            // replica's `directories` slot. Never touches leader/isr/replicas/
            // adding/removing, so a concurrent reassignment or ISR change is
            // preserved regardless of commit order.
            MetadataRecord::V1PartitionDirAssignment(r) => {
                if let Some(pr) = self.partitions.get_mut(&(r.topic.clone(), r.partition))
                    && let Some(slot) = pr.replicas.iter().position(|n| *n == r.replica)
                {
                    if pr.directories.len() < pr.replicas.len() {
                        pr.directories.resize(pr.replicas.len(), uuid::Uuid::nil());
                    }
                    pr.directories[slot] = r.directory;
                }
            }
        }
    }

    /// Serialize the image into the minimal sequence of [`MetadataRecord`]s
    /// whose in-order [`apply`](Self::apply) reproduces this image exactly.
    /// This is the inverse of `apply` over a sequence of non-tombstone
    /// records: each stored entry maps to the record that would create it.
    ///
    /// Tombstone / removal records (`V1DeleteTopic`,
    /// `V1DeleteScramCredential`, `V1DeleteAccessControlEntry`,
    /// `V1DeleteDelegationToken`, `V1UnregisterBroker`) are intentionally
    /// never emitted: a snapshot captures resulting *state*, not deletion
    /// history. `V1BrokerConfig` deletes likewise vanish — only the
    /// surviving key/value pairs are emitted as `Some(value)` sets.
    ///
    /// Records are emitted in dependency order so that a fresh image
    /// `apply`ing them never sees a dangling reference: brokers,
    /// broker-configs, topics, partitions, topic-configs, SCRAM creds,
    /// ACLs, client quotas, delegation tokens.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(topics = self.topics.len(), partitions = self.partitions.len())
    )]
    #[must_use]
    pub fn to_records(&self) -> Vec<MetadataRecord> {
        let mut out = Vec::new();

        // Brokers before their configs.
        for b in self.brokers.values() {
            out.push(MetadataRecord::V1BrokerRegistration(b.clone()));
        }
        for (node_id, configs) in &self.broker_configs {
            for (config_name, config_value) in configs {
                out.push(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id: *node_id,
                    config_name: config_name.clone(),
                    config_value: Some(config_value.clone()),
                }));
            }
        }

        // Topics before partitions and topic-configs (apply-side validate
        // and the snapshot's own DeleteTopic-clears-configs invariant both
        // assume the topic exists first).
        for t in self.topics.values() {
            out.push(MetadataRecord::V1Topic(t.clone()));
        }
        for p in self.partitions.values() {
            out.push(MetadataRecord::V1Partition(p.clone()));
        }
        for (topic, overrides) in &self.topic_configs {
            out.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: topic.clone(),
                overrides: overrides.clone(),
            }));
        }

        // SCRAM credentials.
        for ((user, mechanism), cred) in &self.scram_credentials {
            out.push(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
                user: user.clone(),
                mechanism: *mechanism,
                salt: cred.salt.clone(),
                stored_key: cred.stored_key.clone(),
                server_key: cred.server_key.clone(),
                iterations: cred.iterations,
            }));
        }

        // ACLs (literal + prefixed). Each stored entry already carries its
        // own pattern_type, so re-applying routes it back to the right
        // bucket.
        for entry in self.all_acls() {
            out.push(MetadataRecord::V1AccessControlEntry(entry.clone()));
        }

        // Client quotas. The key is the canonicalized entity tuple; rebuild
        // a QuotaEntity list from it and emit one record per config pair.
        // apply re-canonicalizes, so the round-trip is stable.
        for (entity_key, configs) in &self.client_quotas {
            let entity: Vec<QuotaEntity> = entity_key
                .iter()
                .map(|(entity_type, entity_name)| QuotaEntity {
                    entity_type: entity_type.clone(),
                    entity_name: entity_name.clone(),
                })
                .collect();
            for (config_key, config_value) in configs {
                out.push(MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                    entity: entity.clone(),
                    config_key: config_key.clone(),
                    config_value: Some(*config_value),
                }));
            }
        }

        // Delegation tokens.
        for tok in self.delegation_tokens.values() {
            out.push(MetadataRecord::V1DelegationToken(DelegationTokenRecord {
                token_id: tok.token_id.clone(),
                owner: tok.owner.clone(),
                hmac: tok.hmac.clone(),
                issue_timestamp_ms: tok.issue_timestamp_ms,
                expiry_timestamp_ms: tok.expiry_timestamp_ms,
                max_timestamp_ms: tok.max_timestamp_ms,
                renewers: tok.renewers.clone(),
            }));
        }

        // Client-metrics subscriptions (KIP-714).
        for (name, configs) in &self.client_metrics_configs {
            out.push(MetadataRecord::V1ClientMetricsConfig(
                ClientMetricsConfigRecord {
                    name: name.clone(),
                    configs: configs.clone(),
                },
            ));
        }

        // KIP-584 finalized features: one record per live feature, in the
        // BTreeMap's deterministic key order. (level == 0 is the delete
        // sentinel and is never stored, so every emitted level is >= 1 and
        // re-applies as a set.) The trailing V1FeaturesEpoch then pins the
        // epoch verbatim — the preceding feature records each bump it, but the
        // bumped value reflects the live feature count, not the original apply
        // history, so it must be overwritten. Emitted whenever any feature was
        // ever finalized (epoch left -1 only on a pristine image), so an image
        // that finalized then removed every feature still restores its epoch.
        for (name, level) in &self.feature_levels {
            out.push(MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.clone(),
                level: *level,
            }));
        }
        if self.features_epoch >= 0 {
            out.push(MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord {
                epoch: self.features_epoch,
            }));
        }

        // KIP-853 controller-quorum state. Both MUST be emitted or a snapshot
        // recovery / learner install silently drops them: the voter set goes
        // empty (breaking `DescribeQuorum` and KIP-853 auto-join) and the
        // cluster `kraft.version` reverts to 0. The cluster `kraft_version`
        // in particular has no other persistence path — unlike the openraft
        // membership (restored from `SnapshotMeta.last_membership`), it is not
        // carried anywhere outside the metadata records, so the image is its
        // only source of truth. Independent of every other record, so order
        // here is irrelevant. The reconfig coordinator commits the openraft
        // membership change and the authoritative `V1Voters` record under one
        // lock, so the emitted voter set mirrors `last_membership` for the
        // same committed prefix (any transient one-reconfig skew self-heals as
        // the trailing entry replays from the retained log — the same
        // eventual lockstep that holds in live operation).
        if self.kraft_version != 0 {
            out.push(MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
                kraft_version: self.kraft_version,
            }));
        }
        if !self.voters.is_empty() {
            out.push(MetadataRecord::V1Voters(VotersRecord {
                voters: self.voters.clone(),
            }));
        }

        out
    }

    /// Reconstruct an image from a `cluster_id` and a record sequence
    /// (typically [`Self::to_records`] output read back from a snapshot):
    /// `new` an empty image and `apply` each record in order.
    #[tracing::instrument(level = "info", skip_all, fields(records = records.len()))]
    #[must_use]
    pub fn from_records(cluster_id: Uuid, records: &[MetadataRecord]) -> Self {
        let mut image = Self::new(cluster_id);
        for rec in records {
            image.apply(rec);
        }
        image
    }

    /// Synchronous pre-validation: returns `Ok` if the record would be a
    /// no-conflict apply, otherwise the appropriate error. Used by
    /// `Controller::submit_change` before forwarding to openraft.
    #[tracing::instrument(level = "debug", skip_all, fields(record = record_variant(rec)), err)]
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
                    // Partition count derives from the partitions map (the
                    // KIP-631 round-trip does not carry the stored
                    // `TopicRecord.partitions`), so the expansion check reads
                    // the live count rather than `existing.partitions`.
                    if existing.topic_id != t.topic_id
                        || existing.replication_factor != t.replication_factor
                        || t.partitions <= self.topic_partition_count(&t.name)
                    {
                        return Err(MetadataError::TopicExists(t.name.clone()));
                    }
                    return Ok(());
                }
                // A new topic is valid regardless of the `TopicRecord.partitions`
                // count: KIP-631 framing does not carry that field, so a
                // freshly-decoded `V1Topic` round-trips back as partitions=0 with
                // the real count arriving via the following `V1Partition` records.
                // Partition-count validity (`> 0`) is enforced at the CreateTopics
                // request boundary, not on the metadata-record apply path.
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
            // Delegation-token records have no
            // topic-store concerns and admission is gated by the
            // handler-side checks (KIP-48 §protocol errors), so the
            // image-level validate is unconditional Ok.
            | MetadataRecord::V1DelegationToken(_)
            | MetadataRecord::V1DeleteDelegationToken(_)
            // UnregisterBroker (KIP-919 / api_key 64). The handler-side
            // existence check + Cluster:Alter ACL gate provide all the
            // pre-validation we need; image-level apply is idempotent.
            | MetadataRecord::V1UnregisterBroker(_)
            // KIP-853: voter-set / kraft.version records are validated by
            // the reconfiguration coordinator before submission; the
            // image-level apply is an unconditional replacement.
            | MetadataRecord::V1KRaftVersion(_)
            | MetadataRecord::V1Voters(_)
            // KIP-584: feature-level admission is fully gated by the
            // UpdateFeatures handler (supported-range + downgrade checks);
            // image-level apply is an idempotent map upsert.
            | MetadataRecord::V1FeatureLevel(_)
            // KIP-714: client-metrics subscription config. The
            // IncrementalAlterConfigs handler validates the subscription
            // name and config keys before submission; image-level apply
            // is an idempotent map upsert (or removal on empty map).
            | MetadataRecord::V1ClientMetricsConfig(_)
            // Snapshot-only epoch carrier: only ever produced by `to_records`
            // and replayed on snapshot install, never submitted as a change.
            // Validated permissively for match-exhaustiveness.
            | MetadataRecord::V1FeaturesEpoch(_)
            // KIP-858 directory-assignment delta: a merge into one replica's
            // `directories` slot. The handler already resolved topic/partition/
            // replica against the image; apply is an idempotent slot upsert
            // (a no-op if the partition or replica is unknown).
            | MetadataRecord::V1PartitionDirAssignment(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `record_variant` maps each enum variant to its exact discriminant
    /// name; asserting concrete mappings kills whole-fn replacements (e.g.
    /// returning `""` or a constant).
    #[test]
    fn record_variant_returns_exact_discriminant_names() {
        let topic = MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 2,
        });
        assert!(record_variant(&topic) == "V1Topic");

        let feature = MetadataRecord::V1FeatureLevel(crate::records::FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        });
        assert!(record_variant(&feature) == "V1FeatureLevel");
    }

    use crate::acl::{AclEntryFilter, AclOperation, PermissionType};
    use crate::records::{
        BrokerConfigRecord, ClientQuotaRecord, DeleteDelegationTokenRecord,
        DeleteScramCredentialRecord, DeleteTopicRecord, FeatureLevelRecord, QuotaEntity,
        ScramCredentialRecord,
    };
    use assert2::assert;

    fn img() -> MetadataImage {
        MetadataImage::new(Uuid::nil())
    }

    #[test]
    fn fresh_image_has_no_features_and_unknown_epoch() {
        let m = img();
        assert!(m.finalized_features().is_empty());
        assert!(m.finalized_features_epoch() == -1);
    }

    #[test]
    fn finalized_feature_reads_arbitrary_name() {
        let mut m = MetadataImage::new(uuid::Uuid::nil());
        assert!(m.finalized_feature("transaction.version").is_none());
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "transaction.version".into(),
            level: 2,
        }));
        assert!(m.finalized_feature("transaction.version") == Some(2));
    }

    #[test]
    fn apply_feature_level_sets_level_and_bumps_epoch() {
        let mut m = img();
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        }));
        assert!(m.finalized_features().get("metadata.version") == Some(&1));
        assert!(m.finalized_features_epoch() == 0);

        // A second apply bumps the epoch again (monotonic).
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        }));
        assert!(m.finalized_features_epoch() == 1);
    }

    #[test]
    fn apply_feature_level_zero_deletes_entry() {
        let mut m = img();
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 1,
        }));
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 0,
        }));
        assert!(m.finalized_features().get("metadata.version").is_none());
        // Epoch still advanced — it is monotonic, not a count of live features.
        assert!(m.finalized_features_epoch() == 1);
    }

    #[test]
    fn to_records_from_records_round_trips() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 2,
        }));
        assert!(MetadataImage::from_records(cid, &image.to_records()) == image);
    }

    /// Exercises every stored variant the image can hold (the 9
    /// state-producing records), plus tombstones whose effects must NOT
    /// leak into the snapshot. The round-trip must reproduce the image
    /// exactly.
    #[test]
    #[allow(clippy::too_many_lines)] // exhaustive fixture over every stored variant
    fn to_records_round_trips_all_variants() {
        use crabka_security::{KafkaPrincipal, SaslMechanism};

        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);

        // Brokers (one survives, one is unregistered → must not reappear).
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: 1,
                broker_epoch: 0,
                incarnation_id: Uuid::nil(),
                host: "h1".into(),
                port: 9092,
                rack: Some("r1".into()),
                endpoints: vec![crate::records::BrokerEndpoint {
                    name: "EXTERNAL".into(),
                    host: "ext".into(),
                    port: 9093,
                    protocol: crabka_security::ListenerProtocol::SaslSsl,
                }],
            },
        ));
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: 2,
                broker_epoch: 0,
                incarnation_id: Uuid::nil(),
                host: "h2".into(),
                port: 9092,
                rack: None,
                endpoints: vec![],
            },
        ));
        image.apply(&MetadataRecord::V1UnregisterBroker(
            crate::records::UnregisterBrokerRecord { node_id: 2 },
        ));

        // Broker configs: one set survives, one deleted-back-to-empty.
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("2048".into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "follower.replication.throttled.rate".into(),
            config_value: Some("4096".into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 1,
            config_name: "follower.replication.throttled.rate".into(),
            config_value: None,
        }));

        // Topics + partitions (one topic deleted → topic, partitions and
        // its config must vanish).
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 2,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "doomed".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "orders".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 4,
            adding_replicas: vec![2],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "orders".into(),
            partition: 1,
            leader: 2,
            replicas: vec![2, 1],
            isr: vec![2],
            leader_epoch: 7,
            adding_replicas: vec![],
            removing_replicas: vec![1],
            directories: vec![],
            partition_epoch: 0,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "doomed".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));

        // Topic config.
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        overrides.insert("segment.bytes".to_string(), "1048576".to_string());
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides,
        }));

        // Delete the doomed topic (clears its partition + config too).
        image.apply(&MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        }));

        // SCRAM creds (one survives, one deleted).
        image.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 8192,
        }));
        image.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "bob".into(),
            mechanism: SaslMechanism::ScramSha256,
            salt: vec![4; 16],
            stored_key: vec![5; 32],
            server_key: vec![6; 32],
            iterations: 4096,
        }));
        image.apply(&MetadataRecord::V1DeleteScramCredential(
            DeleteScramCredentialRecord {
                user: "bob".into(),
                mechanism: SaslMechanism::ScramSha256,
            },
        ));

        // ACLs: literal + prefixed survive; a deleted one must not.
        let literal = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        let prefixed = AclEntry {
            resource_type: ResourceType::Group,
            resource_name: "cg-".into(),
            pattern_type: PatternType::Prefixed,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        let doomed_acl = AclEntry {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".into(),
            pattern_type: PatternType::Literal,
            principal: "User:eve".into(),
            host: "*".into(),
            operation: AclOperation::Alter,
            permission_type: PermissionType::Deny,
        };
        image.apply(&MetadataRecord::V1AccessControlEntry(literal));
        image.apply(&MetadataRecord::V1AccessControlEntry(prefixed));
        image.apply(&MetadataRecord::V1AccessControlEntry(doomed_acl));
        image.apply(&MetadataRecord::V1DeleteAccessControlEntry(
            AclEntryFilter {
                resource_type: Some(ResourceType::Cluster),
                ..AclEntryFilter::default()
            },
        ));

        // Client quotas.
        image.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
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
        image.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".into(),
                entity_name: None,
            }],
            config_key: "consumer_byte_rate".into(),
            config_value: Some(2048.0),
        }));

        // Delegation tokens (one survives, one deleted).
        let alice = KafkaPrincipal {
            principal_type: "User".into(),
            name: "alice".into(),
        };
        let bob = KafkaPrincipal {
            principal_type: "User".into(),
            name: "bob".into(),
        };
        image.apply(&MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: "tok-1".into(),
            owner: alice,
            hmac: vec![0x42; 32],
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 5_000,
            max_timestamp_ms: 10_000,
            renewers: vec![bob.clone()],
        }));
        image.apply(&MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: "tok-2".into(),
            owner: bob,
            hmac: vec![0x43; 32],
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 5_000,
            max_timestamp_ms: 10_000,
            renewers: vec![],
        }));
        image.apply(&MetadataRecord::V1DeleteDelegationToken(
            DeleteDelegationTokenRecord {
                token_id: "tok-2".into(),
            },
        ));

        // KIP-584 finalized features: two live features at a non-trivial epoch
        // (the metadata.version re-finalize bumps the epoch past the live
        // count), so the trailing V1FeaturesEpoch carrier is exercised too.
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 24,
        }));
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        }));
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".into(),
            level: 1,
        }));
        assert!(image.finalized_features_epoch() == 2);

        let rebuilt = MetadataImage::from_records(cid, &image.to_records());
        assert!(rebuilt == image);
    }

    /// Finalized features and their epoch must survive a `to_records` /
    /// `from_records` round-trip exactly. The epoch is carried verbatim by a
    /// trailing snapshot-only record, NOT recomputed by apply-bumping — so an
    /// image whose epoch (history of `UpdateFeatures` applies) exceeds its live
    /// feature count still reproduces exactly. Regression guard for the bug
    /// where `to_records` emitted no feature records at all and the snapshot
    /// path silently dropped every finalized feature.
    #[test]
    fn to_records_round_trips_features_and_epoch() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        // Four applies → epoch climbs -1 → 0 → 1 → 2 → 3, while only two
        // features remain live (metadata.version re-finalized once).
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 24,
        }));
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        }));
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".into(),
            level: 1,
        }));
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        }));
        assert!(image.finalized_features_epoch() == 3);

        let rebuilt = MetadataImage::from_records(cid, &image.to_records());
        assert!(rebuilt == image);
        // Belt-and-suspenders beyond derived PartialEq: the two fields the
        // snapshot path used to lose.
        assert!(rebuilt.finalized_features().get("metadata.version") == Some(&25));
        assert!(rebuilt.finalized_features().get("group.version") == Some(&1));
        assert!(rebuilt.finalized_features_epoch() == 3);
    }

    /// KIP-853 voter set + finalized cluster `kraft.version` must survive
    /// `to_records` / `from_records`. Before the fix `to_records` dropped
    /// both, so a snapshot recovery / learner install rebuilt the image with
    /// an EMPTY voter set and `kraft_version = 0` — breaking `DescribeQuorum`
    /// and KIP-853 auto-join. The cluster `kraft_version` has no other
    /// persistence source (it is not part of openraft's membership), so it
    /// can only round-trip through a `V1KRaftVersion` record.
    #[test]
    fn to_records_preserves_voters_and_kraft_version() {
        use crate::records::{KRaftVersionRecord, VotersRecord};
        use crate::voters::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};

        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        image.apply(&MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
            kraft_version: 1,
        }));
        let voters = VoterSet::from_voters([
            Voter {
                id: 1,
                directory_id: Uuid::from_u128(1),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "127.0.0.1".into(),
                    port: 9093,
                }],
                kraft_version: KRaftVersionRange::default(),
            },
            Voter {
                id: 2,
                directory_id: Uuid::from_u128(2),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "127.0.0.1".into(),
                    port: 9094,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            },
        ]);
        image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: voters.clone(),
        }));

        let rebuilt = MetadataImage::from_records(cid, &image.to_records());

        assert_eq!(rebuilt.kraft_version(), 1);
        assert_eq!(rebuilt.voters(), &voters);
        assert_eq!(rebuilt, image);
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
    fn apply_dir_assignment_merges_one_slot_without_clobbering_reassignment() {
        // Regression guard for the KIP-858 dir-assignment clobber: a directory
        // report applied as a delta must set only the reporting replica's slot
        // and leave a concurrent reassignment's `adding_replicas` intact.
        let mut m = img();
        m.apply(&topic("t", 1));
        m.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 0,
            adding_replicas: vec![3],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil(), uuid::Uuid::nil()],
            partition_epoch: 0,
        }));
        let dir = uuid::Uuid::from_u128(0xABCD);
        m.apply(&MetadataRecord::V1PartitionDirAssignment(
            crate::records::PartitionDirAssignmentRecord {
                topic: "t".into(),
                partition: 0,
                replica: 2,
                directory: dir,
            },
        ));
        let pr = m.partition("t", 0).unwrap();
        // Slot 1 (replica 2) set; slots 0 and 2 untouched.
        assert!(pr.directories == vec![uuid::Uuid::nil(), dir, uuid::Uuid::nil()]);
        // The delta did NOT clobber the in-flight reassignment.
        assert!(pr.adding_replicas == vec![3]);
        assert!(pr.replicas == vec![1, 2, 3]);
        assert!(pr.isr == vec![1, 2]);
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
            directories: vec![],
            partition_epoch: 0,
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
            directories: vec![],
            partition_epoch: 0,
        }));
        assert!(m.partitions_of("t").count() == 2);
        m.apply(&MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "t".into(),
        }));
        assert!(m.topic("t").is_none());
        assert!(m.partitions_of("t").count() == 0);
    }

    #[test]
    fn validate_topic_exists_rejected() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let err = m.validate(&topic("t", 1)).unwrap_err();
        assert!(matches!(err, MetadataError::TopicExists(_)));
    }

    /// Apply `count` partition records (indices `0..count`) for `topic` so
    /// the image's derived partition count reflects a realistic topic. The
    /// `validate` partition-count check reads this derived count, not the
    /// stored `TopicRecord.partitions`.
    fn apply_partitions(m: &mut MetadataImage, topic: &str, count: i32) {
        for p in 0..count {
            m.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.into(),
                partition: p,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
    }

    #[test]
    fn validate_topic_partition_count_increase_allowed() {
        let mut m = img();
        m.apply(&topic("t", 1));
        apply_partitions(&mut m, "t", 1);
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
        apply_partitions(&mut m, "t", 3);
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
            directories: vec![],
            partition_epoch: 0,
        });
        let err = m.validate(&p).unwrap_err();
        assert!(matches!(err, MetadataError::UnknownTopic(_)));
    }

    #[test]
    fn broker_registration_is_idempotent() {
        let mut m = img();
        let b = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 1,
            broker_epoch: 0,
            incarnation_id: Uuid::nil(),
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
        });
        m.apply(&b);
        m.apply(&b);
        assert!(m.brokers().count() == 1);
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
        assert!(m.topic_config("t") == Some(&overrides));
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
        assert!(m.topic_config("t") == Some(&second));
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
        assert!(got.unwrap().iterations == 4096);
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
        assert!(got.iterations == 8192);
        assert!(got.salt == vec![9; 16]);
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
        assert!(hits.len() == 1);
        assert!(hits.pop().unwrap().resource_name == "foo");
    }

    #[test]
    fn apply_v1_access_control_entry_replaces_identical_entry() {
        let mut m = img();
        let acl = topic_read_for_alice();
        m.apply(&MetadataRecord::V1AccessControlEntry(acl.clone()));
        m.apply(&MetadataRecord::V1AccessControlEntry(acl));

        let hits: Vec<_> = m.matching_acls(ResourceType::Topic, "foo").collect();
        assert!(hits.len() == 1);
    }

    #[test]
    fn apply_v1_access_control_entry_prefixed_stores_in_prefixed_vec() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_prefixed_team()));
        let hits: Vec<_> = m.matching_acls(ResourceType::Topic, "team-foo").collect();
        assert!(hits.len() == 1);
        assert!(hits[0].resource_name == "team-");
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
        assert!(hits_foo.len() == 1);
        assert!(hits_team.len() == 1);
    }

    fn topic_wildcard_allow() -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "*".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }
    }

    fn topic_wildcard_deny() -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "*".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Deny,
        }
    }

    #[test]
    fn matching_acls_literal_wildcard_allow_matches_arbitrary_topic() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_wildcard_allow()));
        // An arbitrarily-named topic the grant never mentions by name.
        let hits: Vec<_> = m
            .matching_acls(ResourceType::Topic, "some-random-topic")
            .collect();
        assert!(hits.len() == 1);
        assert!(hits[0].resource_name == "*");
        assert!(hits[0].permission_type == PermissionType::Allow);
        // Requesting "*" itself must not double-count the wildcard entry.
        let star: Vec<_> = m.matching_acls(ResourceType::Topic, "*").collect();
        assert!(star.len() == 1);
    }

    #[test]
    fn matching_acls_literal_wildcard_deny_matches_arbitrary_topic() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_wildcard_deny()));
        let hits: Vec<_> = m
            .matching_acls(ResourceType::Topic, "another-random-topic")
            .collect();
        assert!(hits.len() == 1);
        assert!(hits[0].resource_name == "*");
        assert!(hits[0].permission_type == PermissionType::Deny);
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
        assert!(hits_foo.len() == 0); // literal removed
        assert!(hits_team.len() == 1); // prefixed survives
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
        assert!(hits.len() == 1);
    }

    #[test]
    fn all_acls_returns_every_entry() {
        let mut m = img();
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_read_for_alice()));
        m.apply(&MetadataRecord::V1AccessControlEntry(topic_prefixed_team()));
        assert!(m.all_acls().count() == 2);
    }

    #[test]
    fn all_partitions_count_matches_map_size() {
        let mut m = img();
        m.apply(&topic("t", 3));
        for p in 0..3 {
            m.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: p,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
        m.apply(&topic("u", 1));
        m.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "u".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        // 3 partitions for "t" + 1 for "u" = 4, one walk, no per-topic filter.
        assert!(m.all_partitions().count() == 4);
        assert!(
            m.all_partitions().count()
                == m.partitions_of("t").count() + m.partitions_of("u").count()
        );
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
            directories: vec![],
            partition_epoch: 0,
        }));
        assert!(img.reassignments_in_flight().count() == 0);
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
            directories: vec![],
            partition_epoch: 0,
        }));
        let rows: Vec<_> = img.reassignments_in_flight().collect();
        assert!(rows.len() == 1);
        assert!(rows[0].adding_replicas == vec![4]);
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
            directories: vec![],
            partition_epoch: 0,
        }));
        let rows: Vec<_> = img.reassignments_in_flight().collect();
        assert!(rows.len() == 1);
        assert!(rows[0].removing_replicas == vec![3]);
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
                directories: vec![],
                partition_epoch: 0,
            }));
        }
        assert!(img.reassignments_in_flight().count() == 2);
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
        assert!(bc.get("leader.replication.throttled.rate") == Some(&"2048".to_string()));
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
        assert!(img.broker_throttle_rate(1, ThrottleKind::Leader) == Some(2048));
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
        assert!(configs.get("producer_byte_rate") == Some(&1024.0));
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
        assert!(canon[0].0 == "client-id");
        assert!(canon[1].0 == "user");
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
        assert!(users == vec!["alice".to_string(), "bob".to_string()]);
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
        assert!(got.expiry_timestamp_ms == 5_000);
        assert!(got.owner == alice);

        // Same token_id, different expiry — replace, not duplicate.
        img.apply(&dt_record("tok-1", alice.clone(), 7_500, vec![]));
        let got = img.delegation_token_by_id("tok-1").expect("token present");
        assert!(got.expiry_timestamp_ms == 7_500);
        assert!(img.all_delegation_tokens().count() == 1);
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
        assert!(img.all_delegation_tokens().count() == 0);
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
        assert!(found_a.token_id == "tok-a");
        let found_b = img
            .delegation_token_by_hmac(&hmac_b)
            .expect("hmac_b present");
        assert!(found_b.token_id == "tok-b");
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
        assert!(alice_tokens.len() == 2);
        assert!(alice_tokens.iter().all(|t| t.owner == alice));

        let bob_tokens = img.delegation_tokens_by_owner(&bob);
        assert!(bob_tokens.len() == 1);
        assert!(bob_tokens[0].token_id == "b-1");

        // visible_to: bob owns b-1 and is renewer on a-2 → 2 tokens.
        let bob_visible = img.delegation_tokens_visible_to(&bob);
        assert!(bob_visible.len() == 2);
        let mut ids: Vec<&str> = bob_visible.iter().map(|t| t.token_id.as_str()).collect();
        ids.sort_unstable();
        assert!(ids == vec!["a-2", "b-1"]);
    }

    #[test]
    fn applies_voters_and_version() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1KRaftVersion(
            crate::records::KRaftVersionRecord { kraft_version: 1 },
        ));
        image.apply(&MetadataRecord::V1Voters(crate::records::VotersRecord {
            voters: crate::voters::VoterSet::from_voters([crate::voters::Voter {
                id: 1,
                directory_id: uuid::Uuid::nil(),
                endpoints: vec![],
                kraft_version: crate::voters::KRaftVersionRange::default(),
            }]),
        }));
        assert!(image.kraft_version() == 1);
        assert!(image.voters().contains(1));
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
        assert!(pairs.len() == 1);
        assert!(pairs[0].0 == SaslMechanism::ScramSha512);
        assert!(pairs[0].1 == 8192);
        assert!(img.scram_credentials_for_user("ghost").is_empty());
    }

    #[test]
    fn finalized_metadata_version_reads_feature_map() {
        use crate::records::FeatureLevelRecord;
        let mut m = img();
        assert!(m.finalized_metadata_version() == None);
        m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 19,
        }));
        assert!(m.finalized_metadata_version() == Some(19));
    }

    #[test]
    fn min_required_metadata_version_baseline_is_min() {
        use crate::metadata_version::METADATA_VERSION_MIN;
        let m = img();
        assert!(m.min_required_metadata_version() == METADATA_VERSION_MIN);
    }

    #[test]
    fn client_metrics_config_apply_and_clear() {
        use crate::records::ClientMetricsConfigRecord;
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let mut cfgs = std::collections::BTreeMap::new();
        cfgs.insert("interval.ms".to_string(), "60000".to_string());
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "sub-a".into(),
                configs: cfgs,
            },
        ));
        assert_eq!(
            img.client_metrics_config("sub-a")
                .and_then(|m| m.get("interval.ms"))
                .map(String::as_str),
            Some("60000")
        );
        assert_eq!(img.client_metrics_subscriptions().count(), 1);
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "sub-a".into(),
                configs: std::collections::BTreeMap::new(),
            },
        ));
        assert!(img.client_metrics_config("sub-a").is_none());
        assert_eq!(img.client_metrics_subscriptions().count(), 0);
    }

    #[test]
    fn min_required_metadata_version_rises_with_scram_and_tokens() {
        use crate::metadata_version::{DELEGATION_TOKEN_MIN_LEVEL, SCRAM_MIN_LEVEL};
        use crabka_security::{KafkaPrincipal, SaslMechanism};
        let mut m = img();
        m.apply(&MetadataRecord::V1ScramCredential(
            crate::records::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                salt: vec![1; 16],
                stored_key: vec![2; 64],
                server_key: vec![3; 64],
                iterations: 4096,
            },
        ));
        assert!(m.min_required_metadata_version() == SCRAM_MIN_LEVEL);
        m.apply(&MetadataRecord::V1DelegationToken(
            crate::records::DelegationTokenRecord {
                token_id: "t1".into(),
                owner: KafkaPrincipal {
                    principal_type: "User".into(),
                    name: "alice".into(),
                },
                hmac: vec![0x42; 32],
                issue_timestamp_ms: 1,
                expiry_timestamp_ms: 5,
                max_timestamp_ms: 10,
                renewers: vec![],
            },
        ));
        assert!(m.min_required_metadata_version() == DELEGATION_TOKEN_MIN_LEVEL);
    }

    #[test]
    fn topic_by_id_resolves_and_drops_on_delete() {
        use crate::records::{MetadataRecord, TopicRecord};
        let mut img = MetadataImage::new(Uuid::nil());
        let id = Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: id,
            partitions: 1,
            replication_factor: 1,
        }));

        // Resolves by id to the same record `topic(name)` returns.
        assert!(img.topic_by_id(&id).map(|t| t.name.as_str()) == Some("orders"));
        assert!(img.topic_name_by_id(&id) == Some("orders"));

        // After delete the id no longer resolves and the name index is gone.
        img.apply(&MetadataRecord::V1DeleteTopic(
            crate::records::DeleteTopicRecord {
                name: "orders".into(),
            },
        ));
        assert!(img.topic_by_id(&id).is_none());
        assert!(img.topic_name_by_id(&id).is_none());
    }

    #[test]
    fn broker_epoch_reads_back_registered_epoch() {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: 5,
                broker_epoch: 99,
                incarnation_id: Uuid::nil(),
                host: "h".into(),
                port: 9092,
                rack: None,
                endpoints: vec![],
            },
        ));
        assert!(image.broker_epoch(5) == Some(99));
        assert!(image.broker_epoch(404) == None);
    }

    // --- mutation-coverage tests --------------------------------------------

    #[test]
    fn fresh_image_accessors() {
        let id = Uuid::from_u128(0xABCD);
        let image = MetadataImage::new(id);
        assert!(image.cluster_id() == id); // not Uuid::default()
        assert!(image.kraft_version() == 0); // fresh image has kraft_version 0
    }

    #[test]
    fn broker_lookup_returns_registered_record() {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: 7,
                broker_epoch: 1,
                incarnation_id: Uuid::nil(),
                host: "h".into(),
                port: 9092,
                rack: None,
                endpoints: vec![],
            },
        ));
        let b = image.broker(7).expect("registered broker resolvable");
        assert!(b.node_id == 7);
        assert!(image.broker(404).is_none());
    }

    #[test]
    fn reapplying_topic_with_new_id_drops_stale_id_index() {
        let mut image = MetadataImage::new(Uuid::nil());
        let old_id = Uuid::from_u128(1);
        let new_id = Uuid::from_u128(2);
        let topic = |id| {
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: id,
                partitions: 1,
                replication_factor: 1,
            })
        };
        image.apply(&topic(old_id));
        assert!(image.topic_by_id(&old_id).is_some());
        // Re-apply the same NAME with a different id: the stale id index entry
        // must be dropped (the `prev.topic_id != t.topic_id` guard).
        image.apply(&topic(new_id));
        assert!(image.topic_by_id(&new_id).is_some());
        assert!(image.topic_by_id(&old_id).is_none());
    }

    #[test]
    fn broker_throttle_rate_allows_zero() {
        // A configured rate of 0 is a real (zero) value, distinct from absent
        // or -1 (disabled). The cap check is `v < 0` (not `<=`), so 0 -> Some(0).
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: 3,
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some("0".into()),
        }));
        assert!(image.broker_throttle_rate(3, ThrottleKind::Leader) == Some(0));
    }

    #[test]
    fn dir_assignment_resizes_directories_to_replica_count() {
        // KIP-858: a dir-assignment delta for a partition whose `directories`
        // vec is shorter than `replicas` must grow it (to `replicas.len()`,
        // nil-padded) before writing the reporting replica's slot. Without the
        // resize the slot write would index out of bounds.
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 10,
            replicas: vec![10, 20],
            isr: vec![10, 20],
            directories: vec![], // not yet reported
            ..Default::default()
        }));
        let dir = Uuid::from_u128(0xD15);
        image.apply(&MetadataRecord::V1PartitionDirAssignment(
            crate::records::PartitionDirAssignmentRecord {
                topic: "t".into(),
                partition: 0,
                replica: 10,
                directory: dir,
            },
        ));
        let pr = image.partition("t", 0).expect("partition present");
        assert!(pr.directories.len() == 2);
        assert!(pr.directories[0] == dir); // replica 10 is slot 0
        assert!(pr.directories[1] == Uuid::nil()); // replica 20 still unassigned
    }

    #[test]
    fn dir_assignment_updates_existing_full_length_directories() {
        let mut image = MetadataImage::new(Uuid::nil());
        let old = Uuid::from_u128(0xAAA);
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 10,
            replicas: vec![10, 20],
            isr: vec![10, 20],
            directories: vec![old, Uuid::nil()],
            ..Default::default()
        }));
        let dir = Uuid::from_u128(0xD15);
        image.apply(&MetadataRecord::V1PartitionDirAssignment(
            crate::records::PartitionDirAssignmentRecord {
                topic: "t".into(),
                partition: 0,
                replica: 20,
                directory: dir,
            },
        ));

        let pr = image.partition("t", 0).expect("partition present");
        assert!(pr.directories == vec![old, dir]);
    }
}
