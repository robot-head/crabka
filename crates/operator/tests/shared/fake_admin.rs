//! In-memory `AdminClientLike` for reconcile tests.
//!
//! Records every call against an internal log and serves canned
//! responses from a `HashMap<topic_name, TopicState>` the test
//! pre-populates. Mirrors enough of the JVM-broker semantics for the
//! `KafkaTopic` reconcile to exercise its happy / partition-change /
//! immutable / config-diff / delete branches without a live TCP
//! connection.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex as StdMutex;

use crabka_client_admin::{
    AdminClientLike, AdminError, AlterConfigsOutcome, CreatePartitionsOp, CreatePartitionsOutcome,
    CreateTopicOutcome, CreateTopicSpec, DeleteTopicOutcome, IncrementalAlterOp, KafkaError,
    TopicConfigOverrides, TopicMetadata, TopicMetadataEntry,
};

/// A single recorded admin call. Tests assert against the captured
/// sequence to verify which RPCs were issued (and in what order).
#[derive(Debug, Clone)]
pub enum RecordedCall {
    Metadata(Vec<String>),
    CreateTopics(Vec<CreateTopicSpec>),
    DeleteTopics(Vec<String>),
    CreatePartitions(Vec<CreatePartitionsOp>),
    DescribeConfigs(Vec<String>),
    IncrementalAlterConfigs(Vec<IncrementalAlterOp>),
}

/// Per-topic state held by the fake. Mirrors `TopicMetadataEntry` +
/// dynamic-topic config overrides.
#[derive(Debug, Clone, Default)]
pub struct TopicState {
    pub partitions: i32,
    pub replicas: i32,
    pub topic_id: Option<uuid::Uuid>,
    pub config_overrides: BTreeMap<String, String>,
}

/// Test fake. `recorded_calls` and `topics` use `std::sync::Mutex`
/// (rather than `tokio::sync::Mutex`) because both are accessed only
/// while the fake's `async` methods hold the outer per-cluster
/// `tokio::sync::Mutex` lock — there's no contention or await across
/// these mutations.
#[derive(Default)]
pub struct FakeAdminClient {
    pub recorded_calls: StdMutex<Vec<RecordedCall>>,
    pub topics: StdMutex<HashMap<String, TopicState>>,
}

impl FakeAdminClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_topic(&self, name: &str, state: TopicState) {
        self.topics.lock().unwrap().insert(name.into(), state);
    }

    pub fn calls(&self) -> Vec<RecordedCall> {
        self.recorded_calls.lock().unwrap().clone()
    }
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
        _timeout_ms: i32,
    ) -> Result<Vec<CreateTopicOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreateTopics(specs.to_vec()));
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
        _timeout_ms: i32,
    ) -> Result<Vec<DeleteTopicOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::DeleteTopics(
                names.iter().map(|s| (*s).to_string()).collect(),
            ));
        let mut store = self.topics.lock().unwrap();
        let outcomes = names
            .iter()
            .map(|n| {
                store.remove(*n);
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
        _timeout_ms: i32,
    ) -> Result<Vec<CreatePartitionsOutcome>, AdminError> {
        self.recorded_calls
            .lock()
            .unwrap()
            .push(RecordedCall::CreatePartitions(ops.to_vec()));
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
}
