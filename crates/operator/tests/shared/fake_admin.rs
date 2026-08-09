//! In-memory `AdminClientLike` for reconcile tests.
//!
//! This fake records every call in an internal log. It serves canned
//! responses from a `HashMap<topic_name, TopicState>` that the test fills
//! in first. It follows enough of the JVM-broker semantics for the
//! `KafkaTopic` reconcile to exercise its branches without a live TCP
//! connection: the happy path, a partition change, an immutable field, a
//! config diff, and a delete.

#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Mutex as StdMutex,
};

use crabka_client_admin::{
    AclEntry, AclEntryFilter, AdminClientLike, AdminError, AlterConfigsOutcome, CreateAclOutcome,
    CreatePartitionsOp, CreatePartitionsOutcome, CreateTopicOutcome, CreateTopicSpec,
    DeleteAclFilterOutcome, DeleteRecordsOp, DeleteRecordsOutcome, DeleteTopicOutcome,
    IncrementalAlterOp, KafkaError, QuotaOp, ScramDeletion, ScramUpsertion, ScramUserOutcome,
    TopicConfigOverrides, TopicMetadata, TopicMetadataEntry, UserQuotaConfig,
};
use crabka_client_core::ClientError;
use crabka_metadata::DelegationToken;
use crabka_security::KafkaPrincipal;
use crabka_units::{Time, convert::TimeExt as _, days};

/// The error to inject into one RPC.
///
/// `Broker` appears as a per-outcome error, which is how Kafka reports a
/// per-topic error. `Transport` appears as `AdminError::Transport(_)`.
/// That is the variant on which the T3 fix in the reconcile evicts the
/// cached admin client. `BrokerToplevel` appears as
/// `AdminError::Broker { .. }`. That is the variant that
/// `describe_configs` returns when any result carries a non-zero error
/// code.
#[derive(Debug, Clone)]
pub enum InjectableError {
    Broker {
        code: i16,
        name: &'static str,
        message: Option<String>,
    },
    BrokerToplevel {
        api: &'static str,
        code: i16,
        name: &'static str,
        message: Option<String>,
    },
    Transport,
}

#[derive(Debug, Default)]
pub struct InjectedErrors {
    pub create_topics: Option<InjectableError>,
    pub delete_topics: Option<InjectableError>,
    pub create_partitions: Option<InjectableError>,
    pub describe_configs: Option<InjectableError>,
    pub incremental_alter_configs: Option<InjectableError>,
    pub metadata: Option<InjectableError>,
}

/// One recorded admin call.
///
/// The tests assert against the captured sequence to confirm which RPCs
/// ran, and in what order.
#[derive(Debug, Clone)]
pub enum RecordedCall {
    Metadata(Vec<String>),
    CreateTopics(Vec<CreateTopicSpec>),
    DeleteTopics(Vec<String>),
    DeleteRecords(Vec<DeleteRecordsOp>),
    CreatePartitions(Vec<CreatePartitionsOp>),
    DescribeConfigs(Vec<String>),
    IncrementalAlterConfigs(Vec<IncrementalAlterOp>),
    AlterUserScramCredentials {
        upsertions: Vec<ScramUpsertion>,
        deletions: Vec<ScramDeletion>,
    },
    DescribeAcls(AclEntryFilter),
    CreateAcls(Vec<AclEntry>),
    DeleteAcls(Vec<AclEntryFilter>),
    DescribeUserQuotas(String),
    AlterUserQuotas {
        username: String,
        ops: Vec<QuotaOp>,
        validate_only: bool,
    },
    // ── delegation-token RPCs ─────────────────────────────────────────
    CreateDelegationToken {
        owner_principal_name: String,
        renewers: Vec<String>,
        max_lifetime: Option<Time>,
    },
    RenewDelegationToken {
        hmac: Vec<u8>,
    },
    ExpireDelegationToken {
        hmac: Vec<u8>,
    },
    DescribeDelegationTokensOwnedBy {
        owner_principal: String,
    },
}

/// Per-topic state that the fake holds.
///
/// It follows `TopicMetadataEntry` and the dynamic-topic config
/// overrides.
#[derive(Debug, Clone, Default)]
pub struct TopicState {
    pub partitions: i32,
    pub replicas: i32,
    pub topic_id: Option<uuid::Uuid>,
    pub config_overrides: BTreeMap<String, String>,
}

/// The test fake.
///
/// `recorded_calls` and `topics` use `std::sync::Mutex` and not
/// `tokio::sync::Mutex`. The code touches both only while the `async`
/// methods of the fake hold the outer per-cluster `tokio::sync::Mutex`
/// lock. There is therefore no contention, and no await crosses these
/// mutations.
#[derive(Default)]
pub struct FakeAdminClient {
    pub recorded_calls: StdMutex<Vec<RecordedCall>>,
    pub create_topic_timeouts: StdMutex<Vec<Time>>,
    pub delete_topic_timeouts: StdMutex<Vec<Time>>,
    pub topics: StdMutex<HashMap<String, TopicState>>,
    pub injected: StdMutex<InjectedErrors>,
    /// In-memory ACL store, keyed on the full tuple. Reconcile tests fill
    /// it in first when they verify convergence. The trait implementations
    /// below diff against the live set.
    pub acls: StdMutex<BTreeSet<AclEntry>>,
    /// The SCRAM users that the fake upserted one time or more. The
    /// reconcile happy path reads only the recorded-call log. This set
    /// lets a future deletion-path test check the eviction.
    pub scram_users: StdMutex<BTreeSet<String>>,
    /// In-memory client-quota store, keyed by username. Reconcile tests
    /// fill it in when they verify convergence.
    pub user_quotas: StdMutex<BTreeMap<String, UserQuotaConfig>>,
    /// In-memory delegation-token store.
    ///
    /// The fake follows enough of the KIP-48 semantics of the broker for
    /// the Describe, decide, and Create, Renew, or Expire loop of the
    /// reconciler to exercise its branches.
    /// `create_delegation_token_as_owner` mints a new token with a
    /// sequential id, `expiry_timestamp_ms = now + 7d`, and
    /// `max_timestamp_ms = now + 30d`. `renew_delegation_token` extends
    /// `expiry_timestamp_ms` to `min(now + 7d, max_timestamp_ms)`.
    /// `expire_delegation_token` removes the matching entry.
    pub delegation_tokens: StdMutex<Vec<DelegationToken>>,
    /// Monotonic id counter for minted tokens.
    pub next_token_id: StdMutex<u64>,
    /// Makes a successful `DeleteTopics` acknowledgement leave the topic
    /// metadata visible. This models the asynchronous topic deletion of
    /// Kafka.
    pub retain_topics_after_delete_ack: StdMutex<bool>,
}

impl FakeAdminClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_topic(&self, name: &str, state: TopicState) {
        self.topics.lock().unwrap().insert(name.into(), state);
    }

    pub fn retain_topics_after_delete_ack(&self) {
        *self.retain_topics_after_delete_ack.lock().unwrap() = true;
    }

    pub fn remove_topic(&self, name: &str) {
        self.topics.lock().unwrap().remove(name);
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.recorded_calls.lock().unwrap().clone()
    }

    pub fn create_topic_timeouts(&self) -> Vec<Time> {
        self.create_topic_timeouts.lock().unwrap().clone()
    }

    pub fn delete_topic_timeouts(&self) -> Vec<Time> {
        self.delete_topic_timeouts.lock().unwrap().clone()
    }

    pub fn inject_create_topics_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().create_topics = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    pub fn inject_create_partitions_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().create_partitions = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    pub fn inject_incremental_alter_configs_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().incremental_alter_configs = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    pub fn inject_delete_topics_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().delete_topics = Some(InjectableError::Broker {
            code,
            name,
            message,
        });
    }

    /// Injects a top-level `AdminError::Broker { .. }` for
    /// `describe_configs`.
    ///
    /// This matches the path that the real `describe_configs` returns when
    /// any per-resource result carries a non-zero error code.
    pub fn inject_describe_configs_broker_error(
        &self,
        code: i16,
        name: &'static str,
        message: Option<String>,
    ) {
        self.injected.lock().unwrap().describe_configs = Some(InjectableError::BrokerToplevel {
            api: "DescribeConfigs",
            code,
            name,
            message,
        });
    }

    /// Injects an `AdminError::Transport(_)` on the named RPC.
    ///
    /// The reconcile loop evicts the cached admin client on this variant.
    /// That is the T3-fix path.
    pub fn inject_metadata_transport_error(&self) {
        self.injected.lock().unwrap().metadata = Some(InjectableError::Transport);
    }
}

fn transport_error() -> AdminError {
    AdminError::Transport(ClientError::Disconnected)
}

#[async_trait::async_trait]
impl AdminClientLike for FakeAdminClient {
    async fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::Metadata(
                topics.iter().map(|s| (*s).to_string()).collect(),
            ));
        if let Some(inj) = self.injected.lock().unwrap().metadata.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::BrokerToplevel {
                    api,
                    code,
                    name,
                    message,
                } => {
                    return Err(AdminError::Broker {
                        api,
                        code,
                        name,
                        message,
                    });
                }
                InjectableError::Broker { .. } => {
                    // Metadata in the real client doesn't fail per-topic via
                    // top-level error; tests don't use this path today.
                }
            }
        }
        let stored = self.topics.lock().unwrap().clone();
        let entries: Vec<TopicMetadataEntry> = topics
            .iter()
            .map(|t| match stored.get(*t) {
                Some(s) => TopicMetadataEntry {
                    name: (*t).to_string(),
                    topic_id: s.topic_id,
                    partition_count: s.partitions,
                    replication_factor: s.replicas,
                    error: None,
                },
                None => TopicMetadataEntry {
                    name: (*t).to_string(),
                    topic_id: None,
                    partition_count: 0,
                    replication_factor: 0,
                    error: Some(KafkaError {
                        code: 3,
                        name: "UNKNOWN_TOPIC_OR_PARTITION",
                        message: None,
                    }),
                },
            })
            .collect();
        Ok(TopicMetadata {
            controller_id: 0,
            topics: entries,
        })
    }

    async fn create_topics(
        &mut self,
        specs: &[CreateTopicSpec],
        timeout: Time,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        self.create_topic_timeouts.lock().unwrap().push(timeout);
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreateTopics(specs.to_vec()));
        if let Some(inj) = self.injected.lock().unwrap().create_topics.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(specs
                        .iter()
                        .map(|s| CreateTopicOutcome {
                            name: s.name.clone(),
                            topic_id: None,
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {
                    // Not used for per-outcome RPCs; ignore.
                }
            }
        }
        let mut store = self.topics.lock().unwrap();
        let outcomes = specs
            .iter()
            .map(|s| {
                let id = uuid::Uuid::new_v4();
                store.insert(
                    s.name.clone(),
                    TopicState {
                        partitions: s.partitions,
                        replicas: s.replicas,
                        topic_id: Some(id),
                        config_overrides: s.configs.clone(),
                    },
                );
                CreateTopicOutcome {
                    name: s.name.clone(),
                    topic_id: Some(id),
                    error: None,
                }
            })
            .collect();
        Ok(outcomes)
    }

    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout: Time,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        self.delete_topic_timeouts.lock().unwrap().push(timeout);
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DeleteTopics(
                names.iter().map(|s| (*s).to_string()).collect(),
            ));
        if let Some(inj) = self.injected.lock().unwrap().delete_topics.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(names
                        .iter()
                        .map(|n| DeleteTopicOutcome {
                            name: (*n).to_string(),
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {}
            }
        }
        let mut store = self.topics.lock().unwrap();
        let retain_topics_after_delete_ack = *self.retain_topics_after_delete_ack.lock().unwrap();
        let outcomes = names
            .iter()
            .map(|n| {
                if !retain_topics_after_delete_ack {
                    store.remove(*n);
                }
                DeleteTopicOutcome {
                    name: (*n).to_string(),
                    error: None,
                }
            })
            .collect();
        Ok(outcomes)
    }

    async fn create_partitions(
        &mut self,
        ops: &[CreatePartitionsOp],
        _timeout: Time,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreatePartitions(ops.to_vec()));
        if let Some(inj) = self.injected.lock().unwrap().create_partitions.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Ok(ops
                        .iter()
                        .map(|op| CreatePartitionsOutcome {
                            name: op.name.clone(),
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {}
            }
        }
        let mut store = self.topics.lock().unwrap();
        let outcomes = ops
            .iter()
            .map(|op| {
                if let Some(s) = store.get_mut(&op.name) {
                    s.partitions = op.new_total_count;
                }
                CreatePartitionsOutcome {
                    name: op.name.clone(),
                    error: None,
                }
            })
            .collect();
        Ok(outcomes)
    }

    async fn delete_records(
        &mut self,
        ops: &[DeleteRecordsOp],
        _timeout: Time,
    ) -> Result<Vec<DeleteRecordsOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DeleteRecords(ops.to_vec()));
        Ok(ops
            .iter()
            .map(|op| DeleteRecordsOutcome {
                topic: op.topic.clone(),
                partition: op.partition,
                error_code: 0,
                low_watermark: op.offset,
            })
            .collect())
    }

    async fn describe_configs(
        &mut self,
        topics: &[&str],
    ) -> Result<Vec<TopicConfigOverrides>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeConfigs(
                topics.iter().map(|s| (*s).to_string()).collect(),
            ));
        if let Some(inj) = self.injected.lock().unwrap().describe_configs.clone() {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::BrokerToplevel {
                    api,
                    code,
                    name,
                    message,
                } => {
                    return Err(AdminError::Broker {
                        api,
                        code,
                        name,
                        message,
                    });
                }
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    return Err(AdminError::Broker {
                        api: "DescribeConfigs",
                        code,
                        name,
                        message,
                    });
                }
            }
        }
        let store = self.topics.lock().unwrap();
        Ok(topics
            .iter()
            .map(|t| {
                let overrides = store
                    .get(*t)
                    .map(|s| s.config_overrides.clone())
                    .unwrap_or_default();
                TopicConfigOverrides {
                    topic: (*t).to_string(),
                    overrides,
                }
            })
            .collect())
    }

    async fn incremental_alter_configs(
        &mut self,
        ops: &[IncrementalAlterOp],
    ) -> Result<Vec<AlterConfigsOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::IncrementalAlterConfigs(ops.to_vec()));
        if let Some(inj) = self
            .injected
            .lock()
            .unwrap()
            .incremental_alter_configs
            .clone()
        {
            match inj {
                InjectableError::Transport => return Err(transport_error()),
                InjectableError::Broker {
                    code,
                    name,
                    message,
                } => {
                    let mut topics_touched: BTreeSet<String> = BTreeSet::new();
                    for op in ops {
                        match op {
                            IncrementalAlterOp::Set { topic, .. }
                            | IncrementalAlterOp::Delete { topic, .. } => {
                                topics_touched.insert(topic.clone());
                            }
                        }
                    }
                    return Ok(topics_touched
                        .into_iter()
                        .map(|topic| AlterConfigsOutcome {
                            topic,
                            error: Some(KafkaError {
                                code,
                                name,
                                message: message.clone(),
                            }),
                        })
                        .collect());
                }
                InjectableError::BrokerToplevel { .. } => {}
            }
        }
        let mut store = self.topics.lock().unwrap();
        let mut topics_touched: BTreeSet<String> = BTreeSet::new();
        for op in ops {
            match op {
                IncrementalAlterOp::Set { topic, key, value } => {
                    topics_touched.insert(topic.clone());
                    if let Some(s) = store.get_mut(topic) {
                        s.config_overrides.insert(key.clone(), value.clone());
                    }
                }
                IncrementalAlterOp::Delete { topic, key } => {
                    topics_touched.insert(topic.clone());
                    if let Some(s) = store.get_mut(topic) {
                        s.config_overrides.remove(key);
                    }
                }
            }
        }
        Ok(topics_touched
            .into_iter()
            .map(|topic| AlterConfigsOutcome { topic, error: None })
            .collect())
    }

    async fn alter_user_scram_credentials_sha512(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::AlterUserScramCredentials {
                upsertions: upsertions.to_vec(),
                deletions: deletions.to_vec(),
            });
        let mut users = self.scram_users.lock().unwrap();
        let mut out = Vec::with_capacity(upsertions.len() + deletions.len());
        for u in upsertions {
            users.insert(u.username.clone());
            out.push(ScramUserOutcome {
                username: u.username.clone(),
                error: None,
            });
        }
        for d in deletions {
            users.remove(&d.username);
            out.push(ScramUserOutcome {
                username: d.username.clone(),
                error: None,
            });
        }
        Ok(out)
    }

    async fn alter_user_scram_credentials_sha256(
        &mut self,
        upsertions: &[ScramUpsertion],
        deletions: &[ScramDeletion],
    ) -> Result<Vec<ScramUserOutcome>, AdminError> {
        // Same in-memory model as SHA-512; the fake doesn't care about
        // the mechanism wire byte, only the recorded-call trace.
        self.alter_user_scram_credentials_sha512(upsertions, deletions)
            .await
    }

    async fn describe_acls(
        &mut self,
        filter: &AclEntryFilter,
    ) -> Result<Vec<AclEntry>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeAcls(filter.clone()));
        let store = self.acls.lock().unwrap();
        Ok(store
            .iter()
            .filter(|e| matches_filter(filter, e))
            .cloned()
            .collect())
    }

    async fn create_acls(
        &mut self,
        creations: &[AclEntry],
    ) -> Result<Vec<CreateAclOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreateAcls(creations.to_vec()));
        let mut store = self.acls.lock().unwrap();
        let mut out = Vec::with_capacity(creations.len());
        for e in creations {
            store.insert(e.clone());
            out.push(CreateAclOutcome { error: None });
        }
        Ok(out)
    }

    async fn delete_acls(
        &mut self,
        filters: &[AclEntryFilter],
    ) -> Result<Vec<DeleteAclFilterOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DeleteAcls(filters.to_vec()));
        let mut store = self.acls.lock().unwrap();
        let mut out = Vec::with_capacity(filters.len());
        for f in filters {
            let matched: Vec<AclEntry> = store
                .iter()
                .filter(|e| matches_filter(f, e))
                .cloned()
                .collect();
            for e in &matched {
                store.remove(e);
            }
            out.push(DeleteAclFilterOutcome {
                error: None,
                matched,
            });
        }
        Ok(out)
    }

    async fn describe_user_quotas(
        &mut self,
        username: &str,
    ) -> Result<UserQuotaConfig, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeUserQuotas(username.into()));
        let store = self.user_quotas.lock().unwrap();
        Ok(store.get(username).cloned().unwrap_or_default())
    }

    async fn alter_user_quotas(
        &mut self,
        username: &str,
        ops: &[QuotaOp],
        validate_only: bool,
    ) -> Result<Option<KafkaError>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::AlterUserQuotas {
                username: username.into(),
                ops: ops.to_vec(),
                validate_only,
            });
        if validate_only {
            return Ok(None);
        }
        let mut store = self.user_quotas.lock().unwrap();
        let entry = store.entry(username.into()).or_default();
        for op in ops {
            match op {
                QuotaOp::Set { key, value } => {
                    entry.insert(key.clone(), *value);
                }
                QuotaOp::Remove { key } => {
                    entry.remove(key);
                }
            }
        }
        if entry.is_empty() {
            store.remove(username);
        }
        Ok(None)
    }

    // ── delegation-token RPCs ─────────────────────────────────────────
    //
    // In-memory KIP-48 model. Tokens carry:
    //   - `token_id`  — sequential `tok-<n>` strings
    //   - `hmac`      — 32 zero bytes plus the id as a discriminator,
    //                   so `expire(hmac)` / `renew(hmac)` can find them.
    //   - `expiry_timestamp_ms` — `now + 7d` on create.
    //   - `max_timestamp_ms`    — `now + 30d` on create.
    //
    // Renew advances `expiry_timestamp_ms` to `min(now + 7d, max)`. The
    // operator's `reconcile_renews_when_within_threshold` test depends
    // on this — it uses a `renew_before_expiry_ms` of exactly 7d so the
    // decision always lands on Renew, and asserts that the post-renew
    // expiry only ever increases (or holds at `max`).
    async fn create_delegation_token_as_owner(
        &mut self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime: Option<Time>,
    ) -> Result<DelegationToken, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreateDelegationToken {
                owner_principal_name: owner_principal_name.into(),
                renewers: renewers.to_vec(),
                max_lifetime,
            });
        let now_ms = chrono::Utc::now().timestamp_millis();
        // The broker caps an absent or over-long lifetime at its 7-day
        // `delegation.token.max.lifetime.ms` default.
        let broker_ceiling = days(7);
        let lifetime_ms = max_lifetime
            .filter(|lifetime| *lifetime > Time::ZERO)
            .unwrap_or(broker_ceiling)
            .min(broker_ceiling)
            .millis_i64();
        let max_ts = now_ms + 30 * 24 * 60 * 60 * 1_000;
        let id = {
            let mut next = self.next_token_id.lock().unwrap();
            let i = *next;
            *next += 1;
            i
        };
        let token_id = format!("tok-{id}");
        // Discriminating HMAC: 32 bytes where the trailing 8 carry the
        // id LE-encoded so `renew`/`expire` can match the right token.
        let mut hmac = vec![0u8; 32];
        hmac[24..].copy_from_slice(&id.to_le_bytes());

        let owner: KafkaPrincipal = format!("User:{owner_principal_name}")
            .parse()
            .map_err(AdminError::Protocol)?;
        let parsed_renewers: Vec<KafkaPrincipal> =
            renewers.iter().filter_map(|s| s.parse().ok()).collect();
        let token = DelegationToken {
            token_id,
            owner,
            hmac,
            issue_timestamp_ms: now_ms,
            expiry_timestamp_ms: now_ms + lifetime_ms,
            max_timestamp_ms: max_ts,
            renewers: parsed_renewers,
        };
        self.delegation_tokens.lock().unwrap().push(token.clone());
        Ok(token)
    }

    async fn renew_delegation_token(&mut self, hmac: &[u8]) -> Result<DelegationToken, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::RenewDelegationToken {
                hmac: hmac.to_vec(),
            });
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut store = self.delegation_tokens.lock().unwrap();
        let pos = store
            .iter()
            .position(|t| t.hmac == hmac)
            .ok_or_else(|| AdminError::Protocol("renew: hmac not found".into()))?;
        let max = store[pos].max_timestamp_ms;
        let new_expiry = (now_ms + 7 * 24 * 60 * 60 * 1_000).min(max);
        // Renew never moves expiry backwards.
        if new_expiry > store[pos].expiry_timestamp_ms {
            store[pos].expiry_timestamp_ms = new_expiry;
        }
        Ok(store[pos].clone())
    }

    async fn expire_delegation_token(&mut self, hmac: &[u8]) -> Result<(), AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::ExpireDelegationToken {
                hmac: hmac.to_vec(),
            });
        let mut store = self.delegation_tokens.lock().unwrap();
        store.retain(|t| t.hmac != hmac);
        Ok(())
    }

    async fn describe_delegation_tokens_owned_by(
        &mut self,
        owner_principal: &str,
    ) -> Result<Vec<DelegationToken>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DescribeDelegationTokensOwnedBy {
                owner_principal: owner_principal.into(),
            });
        let want: KafkaPrincipal = owner_principal.parse().map_err(AdminError::Protocol)?;
        let store = self.delegation_tokens.lock().unwrap();
        Ok(store.iter().filter(|t| t.owner == want).cloned().collect())
    }
}

/// Returns true when every populated axis of `filter` matches `entry`.
///
/// It follows the semantics of `AclEntryFilter::matches` in the broker.
fn matches_filter(f: &AclEntryFilter, e: &AclEntry) -> bool {
    f.resource_type.is_none_or(|rt| rt == e.resource_type)
        && f.resource_name
            .as_ref()
            .is_none_or(|n| n == &e.resource_name)
        && f.pattern_type.is_none_or(|pt| pt == e.pattern_type)
        && f.principal.as_ref().is_none_or(|p| p == &e.principal)
        && f.host.as_ref().is_none_or(|h| h == &e.host)
        && f.operation.is_none_or(|op| op == e.operation)
        && f.permission_type.is_none_or(|p| p == e.permission_type)
}
