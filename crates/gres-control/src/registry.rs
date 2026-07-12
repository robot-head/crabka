//! Registry store abstractions and Kafka-backed registry client.

use std::{
    collections::BTreeMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex as StdMutex, RwLock},
    time::Duration,
};

use bytes::Bytes;
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::{Connection, ConnectionOptions, fetch_partition_with_isolation_progress};
use crabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord, Transaction};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use tokio::sync::{Mutex, watch};

use crate::{
    ControlError,
    record::{
        FinalCheckpoint, RangeLayoutMerge, RangeLayoutMutation, RangeLayoutSplit, RegistryKey,
        TENANT_REGISTRY_TOPIC, TenantName, TenantRecord, decode_registry_record,
        encode_registry_record, encode_tenant_config_record, tenant_config_topic,
        tenant_registry_key,
    },
};

const TOPIC_ALREADY_EXISTS: i16 = 36;
const FETCH_MAX_WAIT_MS: i32 = 500;
const FETCH_PARTITION_MAX_BYTES: i32 = 1 << 20;
const READ_COMMITTED: i8 = 1;
const REGISTRY_TRANSACTIONAL_ID: &str = "__gres_tenants.writer";

/// Pure tenant-registry store seam for operator and CLI code.
pub trait TenantRegistryStore {
    /// Upsert one whole tenant snapshot.
    fn upsert(&mut self, record: TenantRecord) -> Result<(), ControlError>;
    /// Create a tenant when absent or replace its whole snapshot only when the
    /// observed version still matches. `None` is the create-only precondition.
    fn replace_if_version(
        &mut self,
        record: TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<TenantRecord, ControlError> {
        let current = self.get(&record.name);
        let record = canonical_replacement(current.as_ref(), record);
        record.ensure_valid()?;
        validate_replacement_version(record.record_version, expected_record_version)?;
        if current.as_ref() == Some(&record) {
            return Ok(record);
        }
        match (current, expected_record_version) {
            (None, None) => {
                self.upsert(record.clone())?;
                Ok(record)
            }
            (Some(current), Some(expected)) if current.record_version == expected => {
                self.upsert(record.clone())?;
                Ok(record)
            }
            (Some(current), Some(expected)) => Err(ControlError::RegistryVersionConflict {
                tenant: current.name,
                expected,
                actual: current.record_version,
            }),
            (Some(current), None) => Err(ControlError::RegistryVersionConflict {
                tenant: current.name,
                expected: 0,
                actual: current.record_version,
            }),
            (None, Some(expected)) => Err(ControlError::RegistryVersionConflict {
                tenant: record.name,
                expected,
                actual: 0,
            }),
        }
    }
    /// Tombstone one tenant by name. Idempotent when the tenant is already absent.
    fn delete(&mut self, tenant: &TenantName) -> Result<(), ControlError>;
    /// Return one tenant by name.
    fn get(&self, tenant: &TenantName) -> Option<TenantRecord>;
    /// Return all tenants ordered by name.
    fn list(&self) -> Vec<TenantRecord>;
    /// Apply a versioned split/merge mutation without overwriting concurrent changes.
    fn mutate_range_layout_if_version(
        &mut self,
        tenant: &TenantName,
        expected_record_version: u64,
        mutation: RangeLayoutMutation,
    ) -> Result<Option<TenantRecord>, ControlError>;
    /// Split one tenant range layout when the tenant is still at `expected_record_version`.
    fn split_range_layout_if_version(
        &mut self,
        tenant: &TenantName,
        expected_record_version: u64,
        split: RangeLayoutSplit,
    ) -> Result<Option<TenantRecord>, ControlError> {
        self.mutate_range_layout_if_version(
            tenant,
            expected_record_version,
            RangeLayoutMutation::Split(split),
        )
    }
    /// Merge two adjacent tenant ranges when the tenant is still at `expected_record_version`.
    fn merge_range_layout_if_version(
        &mut self,
        tenant: &TenantName,
        expected_record_version: u64,
        merge: RangeLayoutMerge,
    ) -> Result<Option<TenantRecord>, ControlError> {
        self.mutate_range_layout_if_version(
            tenant,
            expected_record_version,
            RangeLayoutMutation::Merge(merge),
        )
    }
}

/// In-memory implementation of [`TenantRegistryStore`] used by tests and future fakes.
#[derive(Debug, Default)]
pub struct InMemoryRegistryStore {
    tenants: Arc<StdMutex<BTreeMap<String, TenantRecord>>>,
}

impl Clone for InMemoryRegistryStore {
    fn clone(&self) -> Self {
        let tenants = self
            .tenants
            .lock()
            .expect("in-memory registry lock poisoned")
            .clone();
        Self {
            tenants: Arc::new(StdMutex::new(tenants)),
        }
    }
}

impl InMemoryRegistryStore {
    /// Build an empty in-memory registry store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl TenantRegistryStore for InMemoryRegistryStore {
    fn upsert(&mut self, record: TenantRecord) -> Result<(), ControlError> {
        record.ensure_valid()?;
        let name = record.name.as_str().to_string();
        let mut tenants = self
            .tenants
            .lock()
            .expect("in-memory registry lock poisoned");
        match tenants.get(&name) {
            Some(current) if current.record_version >= record.record_version => Ok(()),
            _ => {
                let record = merge_monotonic_generation(tenants.get(&name), record);
                tenants.insert(name, record);
                Ok(())
            }
        }
    }

    fn replace_if_version(
        &mut self,
        record: TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<TenantRecord, ControlError> {
        let name = record.name.as_str().to_string();
        let mut tenants = self
            .tenants
            .lock()
            .expect("in-memory registry lock poisoned");
        let current = tenants.get(&name);
        let record = canonical_replacement(current, record);
        record.ensure_valid()?;
        validate_replacement_version(record.record_version, expected_record_version)?;
        if current == Some(&record) {
            return Ok(record);
        }
        match (current, expected_record_version) {
            (None, None) => {
                tenants.insert(name, record.clone());
                Ok(record)
            }
            (Some(current), Some(expected)) if current.record_version == expected => {
                tenants.insert(name, record.clone());
                Ok(record)
            }
            (Some(current), Some(expected)) => Err(ControlError::RegistryVersionConflict {
                tenant: current.name.clone(),
                expected,
                actual: current.record_version,
            }),
            (Some(current), None) => Err(ControlError::RegistryVersionConflict {
                tenant: current.name.clone(),
                expected: 0,
                actual: current.record_version,
            }),
            (None, Some(expected)) => Err(ControlError::RegistryVersionConflict {
                tenant: record.name,
                expected,
                actual: 0,
            }),
        }
    }

    fn delete(&mut self, tenant: &TenantName) -> Result<(), ControlError> {
        self.tenants
            .lock()
            .expect("in-memory registry lock poisoned")
            .remove(tenant.as_str());
        Ok(())
    }

    fn get(&self, tenant: &TenantName) -> Option<TenantRecord> {
        self.tenants
            .lock()
            .expect("in-memory registry lock poisoned")
            .get(tenant.as_str())
            .cloned()
    }

    fn list(&self) -> Vec<TenantRecord> {
        self.tenants
            .lock()
            .expect("in-memory registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn mutate_range_layout_if_version(
        &mut self,
        tenant: &TenantName,
        expected_record_version: u64,
        mutation: RangeLayoutMutation,
    ) -> Result<Option<TenantRecord>, ControlError> {
        let mut tenants = self
            .tenants
            .lock()
            .expect("in-memory registry lock poisoned");
        let Some(current) = tenants.get(tenant.as_str()).cloned() else {
            return Ok(None);
        };
        let next = mutate_layout_if_version(current, expected_record_version, mutation)?;
        if tenants.get(tenant.as_str()) == Some(&next) {
            return Ok(Some(next));
        }

        tenants.insert(tenant.as_str().to_string(), next.clone());
        Ok(Some(next))
    }
}

/// Fold raw compacted-topic records into the latest tenant image.
#[must_use]
pub fn fold(
    records: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
) -> BTreeMap<String, TenantRecord> {
    let mut tenants = BTreeMap::new();
    for (key, value) in records {
        apply_raw_record(&mut tenants, &key, value.as_deref());
    }
    tenants
}

/// Kafka-backed registry facade over `__gres_tenants`.
pub struct Registry {
    bootstrap: String,
    producer: Producer,
    tenants: Arc<RwLock<BTreeMap<String, TenantRecord>>>,
    applied_rx: watch::Receiver<i64>,
    applied_tx: watch::Sender<i64>,
    write_gate: Mutex<()>,
    reader_started: bool,
}

impl Registry {
    /// Connect producer-side registry resources. Call [`Self::ensure_topic`] before writes.
    pub async fn connect(bootstrap: &str) -> Result<Self, ControlError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id("crabka-gres-control-writer")
            .enable_idempotence(true)
            .acks(Acks::All)
            .transactional_id(REGISTRY_TRANSACTIONAL_ID)
            .build()
            .await?;
        let (applied_tx, applied_rx) = watch::channel(-1_i64);
        Ok(Self {
            bootstrap: bootstrap.to_string(),
            producer,
            tenants: Arc::new(RwLock::new(BTreeMap::new())),
            applied_rx,
            applied_tx,
            write_gate: Mutex::new(()),
            reader_started: false,
        })
    }

    /// Ensure `__gres_tenants` exists as a compacted, one-partition topic.
    pub async fn ensure_topic(&mut self, replicas: i32) -> Result<(), ControlError> {
        let topic_id = ensure_registry_topic(&self.bootstrap, replicas).await?;
        if self.reader_started {
            return Ok(());
        }
        spawn_reader(
            self.bootstrap.clone(),
            topic_id,
            Arc::clone(&self.tenants),
            self.applied_tx.clone(),
        );
        self.reader_started = true;
        Ok(())
    }

    /// Create a tenant snapshot, or accept an exact idempotent retry.
    ///
    /// Replacing an existing snapshot through this unversioned API is rejected:
    /// callers must use a semantic mutation so the writer can derive the next
    /// version from its fenced, read-committed image.
    pub async fn upsert(&mut self, record: &TenantRecord) -> Result<(), ControlError> {
        record.ensure_valid()?;
        let record = record.clone();
        let tenant = record.name.clone();
        self.mutate_after_fencing(&tenant, move |current| match current {
            None => Ok(Some(record)),
            Some(current) if current == record => Ok(None),
            Some(current) => Err(ControlError::RegistryVersionConflict {
                tenant: current.name,
                expected: record.record_version,
                actual: current.record_version,
            }),
        })
        .await
        .map(|_| ())
    }

    /// Create a tenant when absent or replace its snapshot only when the
    /// caller's observed version still matches the fenced, read-committed
    /// image. Exact retries are no-ops.
    pub async fn replace_if_version(
        &mut self,
        record: &TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<TenantRecord, ControlError> {
        let record = record.clone();
        let tenant = record.name.clone();
        let stored_record = self
            .mutate_after_fencing(&tenant, move |current| {
                let record = canonical_replacement(current.as_ref(), record);
                record.ensure_valid()?;
                validate_replacement_version(record.record_version, expected_record_version)?;
                if current.as_ref() == Some(&record) {
                    return Ok(None);
                }
                match (current, expected_record_version) {
                    (None, None) => Ok(Some(record)),
                    (Some(current), Some(expected)) if current.record_version == expected => {
                        Ok(Some(record))
                    }
                    (Some(current), Some(expected)) => Err(ControlError::RegistryVersionConflict {
                        tenant: current.name,
                        expected,
                        actual: current.record_version,
                    }),
                    (Some(current), None) => Err(ControlError::RegistryVersionConflict {
                        tenant: current.name,
                        expected: 0,
                        actual: current.record_version,
                    }),
                    (None, Some(expected)) => Err(ControlError::RegistryVersionConflict {
                        tenant: record.name,
                        expected,
                        actual: 0,
                    }),
                }
            })
            .await?;
        stored_record.ok_or(ControlError::TopicMissing(
            TENANT_REGISTRY_TOPIC.to_string(),
        ))
    }

    /// Request resume for a suspended tenant. No-op unless the current state is `Suspended`.
    pub async fn request_resume(&mut self, tenant: &str) -> Result<(), ControlError> {
        self.transform_tenant(tenant, TenantRecord::request_resume)
            .await
    }

    /// Mark a tenant active and publish the endpoint activators should dial.
    pub async fn mark_active(
        &mut self,
        tenant: &str,
        endpoint: impl Into<String>,
    ) -> Result<(), ControlError> {
        let endpoint = endpoint.into();
        self.transform_tenant(tenant, |record| record.mark_active(endpoint))
            .await
    }

    /// Mark a tenant suspended after its final checkpoint is durable.
    pub async fn mark_suspended(&mut self, tenant: &str) -> Result<(), ControlError> {
        self.transform_tenant(tenant, TenantRecord::mark_suspended)
            .await
    }

    /// Mark a tenant suspended after recording the durable final checkpoint that permits parking.
    pub async fn mark_suspended_after_checkpoint(
        &mut self,
        tenant: &str,
        checkpoint: FinalCheckpoint,
    ) -> Result<(), ControlError> {
        self.transform_tenant(tenant, |record| {
            record.mark_suspended_after_checkpoint(checkpoint)
        })
        .await
    }

    /// Monotonically advance the WAL generation for a tenant.
    pub async fn bump_wal_generation(
        &mut self,
        tenant: &str,
        generation: u64,
    ) -> Result<(), ControlError> {
        self.transform_tenant(tenant, |record| record.with_wal_generation(generation))
            .await
    }

    /// Apply a versioned range-layout mutation through the fenced registry writer.
    pub async fn mutate_range_layout_if_version(
        &mut self,
        tenant: &str,
        expected_record_version: u64,
        mutation: RangeLayoutMutation,
    ) -> Result<(), ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.mutate_after_fencing(&tenant, move |current| {
            let Some(current) = current else {
                return Ok(None);
            };
            let next =
                mutate_layout_if_version(current.clone(), expected_record_version, mutation)?;
            Ok((next != current).then_some(next))
        })
        .await
        .map(|_| ())
    }

    pub async fn split_range_layout_if_version(
        &mut self,
        tenant: &str,
        expected_record_version: u64,
        split: RangeLayoutSplit,
    ) -> Result<(), ControlError> {
        self.mutate_range_layout_if_version(
            tenant,
            expected_record_version,
            RangeLayoutMutation::Split(split),
        )
        .await
    }

    pub async fn merge_range_layout_if_version(
        &mut self,
        tenant: &str,
        expected_record_version: u64,
        merge: RangeLayoutMerge,
    ) -> Result<(), ControlError> {
        self.mutate_range_layout_if_version(
            tenant,
            expected_record_version,
            RangeLayoutMutation::Merge(merge),
        )
        .await
    }

    /// Produce the per-tenant runtime snapshot consumed by substrate computes.
    pub async fn upsert_tenant_config(
        &self,
        record: &TenantRecord,
        replicas: i32,
    ) -> Result<(), ControlError> {
        let topic = tenant_config_topic(&record.name);
        ensure_compacted_single_partition_topic(&self.bootstrap, &topic, replicas).await?;
        let value = encode_tenant_config_record(record)?;
        let rx = self
            .producer
            .send(ProducerRecord {
                topic,
                partition: Some(0),
                value: Some(Bytes::from(value)),
                ..Default::default()
            })
            .await;
        rx.await.map_err(|_| ControlError::ProducerAckDropped)??;
        Ok(())
    }

    /// Produce a tombstone for one tenant and wait until it is locally applied.
    pub async fn delete(&mut self, tenant: &str) -> Result<(), ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.delete_after_fencing(&tenant).await
    }

    /// Return the locally applied image for one tenant.
    pub async fn get(&mut self, tenant: &str) -> Result<Option<TenantRecord>, ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.refresh().await?;
        Ok(read_tenants(&self.tenants).get(tenant.as_str()).cloned())
    }

    /// Return all locally applied tenants ordered by tenant name.
    pub async fn list(&mut self) -> Result<Vec<TenantRecord>, ControlError> {
        self.refresh().await?;
        Ok(read_tenants(&self.tenants).values().cloned().collect())
    }

    /// Watch the last offset applied by the registry reader.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<i64> {
        self.applied_rx.clone()
    }

    async fn produce(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<i64, ControlError> {
        let rx = self
            .producer
            .send(ProducerRecord {
                topic: TENANT_REGISTRY_TOPIC.to_string(),
                partition: Some(0),
                key: Some(Bytes::from(key)),
                value: value.map(Bytes::from),
                ..Default::default()
            })
            .await;
        let metadata = rx.await.map_err(|_| ControlError::ProducerAckDropped)??;
        Ok(metadata.offset)
    }

    async fn await_applied(&self, offset: i64) {
        let mut applied_rx = self.applied_rx.clone();
        while *applied_rx.borrow_and_update() < offset {
            if applied_rx.changed().await.is_err() {
                return;
            }
        }
    }

    async fn transform_tenant(
        &mut self,
        tenant: &str,
        transform: impl FnOnce(TenantRecord) -> Result<TenantRecord, ControlError>,
    ) -> Result<(), ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.mutate_after_fencing(&tenant, |current| {
            let Some(current) = current else {
                return Ok(None);
            };
            let next = transform(current.clone())?;
            Ok((next != current).then_some(next))
        })
        .await
        .map(|_| ())
    }

    /// Fence every other registry writer, then read, transform, and commit one
    /// snapshot.  The shared transactional id makes the producer epoch a
    /// cross-process write lease; a compacted-topic version alone is not CAS.
    async fn mutate_after_fencing(
        &mut self,
        tenant: &TenantName,
        transform: impl FnOnce(Option<TenantRecord>) -> Result<Option<TenantRecord>, ControlError>,
    ) -> Result<Option<TenantRecord>, ControlError> {
        let _gate = self.write_gate.lock().await;
        self.initialize_transactional_writer().await?;
        self.refresh().await?;
        let current = read_tenants(&self.tenants).get(tenant.as_str()).cloned();
        let Some(next) = transform(current.clone())? else {
            return Ok(current);
        };
        next.ensure_valid()?;
        let transaction = self.begin_registry_transaction().await?;
        let (key, value) = encode_registry_record(&next)?;
        let offset = match self.produce(key, Some(value)).await {
            Ok(offset) => offset,
            Err(error) => {
                abort_registry_transaction(transaction).await?;
                return Err(error);
            }
        };
        commit_registry_transaction(transaction).await?;
        self.await_applied(offset).await;
        Ok(Some(next))
    }

    async fn delete_after_fencing(&mut self, tenant: &TenantName) -> Result<(), ControlError> {
        let _gate = self.write_gate.lock().await;
        self.initialize_transactional_writer().await?;
        self.refresh().await?;
        if !read_tenants(&self.tenants).contains_key(tenant.as_str()) {
            return Ok(());
        }
        let transaction = self.begin_registry_transaction().await?;
        let offset = match self.produce(tenant_registry_key(tenant)?, None).await {
            Ok(offset) => offset,
            Err(error) => {
                abort_registry_transaction(transaction).await?;
                return Err(error);
            }
        };
        commit_registry_transaction(transaction).await?;
        self.await_applied(offset).await;
        Ok(())
    }

    async fn refresh(&self) -> Result<(), ControlError> {
        let bootstrap_addrs = split_bootstrap(&self.bootstrap);
        let mut admin = AdminClient::connect(&bootstrap_addrs).await?;
        let metadata = admin.metadata(&[TENANT_REGISTRY_TOPIC]).await?;
        let entry = metadata
            .topics
            .into_iter()
            .find(|topic| topic.name == TENANT_REGISTRY_TOPIC)
            .ok_or_else(|| ControlError::TopicMissing(TENANT_REGISTRY_TOPIC.to_string()))?;
        let Some(addr) = resolve_bootstrap_addr(&self.bootstrap) else {
            return Err(ControlError::TopicMissing(
                TENANT_REGISTRY_TOPIC.to_string(),
            ));
        };
        let opts = ConnectionOptions {
            client_id: "crabka-gres-control-refresh".to_string(),
            ..Default::default()
        };
        let conn = Connection::connect_with_options(addr, opts).await?;
        let topic_id = entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid);
        let mut next_offset = 0_i64;
        loop {
            let result = fetch_partition_with_isolation_progress(
                &conn,
                TENANT_REGISTRY_TOPIC,
                topic_id,
                0,
                next_offset,
                FETCH_MAX_WAIT_MS,
                FETCH_PARTITION_MAX_BYTES,
                READ_COMMITTED,
            )
            .await?;
            let Some(progress) = result.next_offset else {
                conn.close();
                return Ok(());
            };
            for record in result.records {
                if record.offset < next_offset {
                    continue;
                }
                write_tenants(&self.tenants, |state| {
                    apply_raw_record(
                        state,
                        record.key.as_deref().unwrap_or_default(),
                        record.value.as_deref(),
                    );
                });
                let _ = self.applied_tx.send(record.offset);
            }
            next_offset = progress;
        }
    }

    /// Establish the transactional writer lease before every registry mutation.
    /// A successful initialization publishes the coordinator-issued epoch before
    /// the caller can begin and produce a mutation.
    async fn initialize_transactional_writer(&self) -> Result<(), ControlError> {
        self.producer.init_transactions().await?;
        Ok(())
    }

    /// Begin only after initialization. A recovery marker can be installed by a
    /// concurrently dropped transaction guard between initialization and begin;
    /// reinitialize once in that case rather than producing with an uncertain
    /// epoch.
    async fn begin_registry_transaction(&self) -> Result<Transaction<'_>, ControlError> {
        match self.producer.begin_transaction().await {
            Ok(transaction) => Ok(transaction),
            Err(error) if requires_writer_reinitialization(&error) => {
                self.initialize_transactional_writer().await?;
                Ok(self.producer.begin_transaction().await?)
            }
            Err(error) => Err(ControlError::Producer(error)),
        }
    }
}

/// Finish a registry transaction without discarding the guard while Kafka says
/// the same transaction can be retried. Terminal and transport errors are
/// surfaced unchanged after the producer has moved to its terminal/ready state;
/// the next mutation reinitializes the transactional producer.
async fn commit_registry_transaction(mut transaction: Transaction<'_>) -> Result<(), ControlError> {
    loop {
        match transaction.commit().await {
            Ok(()) => return Ok(()),
            Err(error) if end_transaction_is_retryable(&error.source) => {
                transaction = error.transaction;
            }
            Err(error) => return Err(ControlError::Producer(error.source)),
        }
    }
}

/// Abort after a produce failure. Retryable `EndTxn` errors retain their guard;
/// terminal errors remain visible to the caller rather than leaving a producer
/// silently wedged in `InTransaction`.
async fn abort_registry_transaction(mut transaction: Transaction<'_>) -> Result<(), ControlError> {
    loop {
        match transaction.abort().await {
            Ok(()) => return Ok(()),
            Err(error) if end_transaction_is_retryable(&error.source) => {
                transaction = error.transaction;
            }
            Err(error) => return Err(ControlError::Producer(error.source)),
        }
    }
}

fn end_transaction_is_retryable(error: &ProducerError) -> bool {
    matches!(error, ProducerError::ConcurrentTransactions)
}

fn requires_writer_reinitialization(error: &ProducerError) -> bool {
    matches!(error, ProducerError::RecoveryRequired)
}

fn validate_replacement_version(
    record_version: u64,
    expected_record_version: Option<u64>,
) -> Result<(), ControlError> {
    let required_version = match expected_record_version {
        None => 1,
        Some(version) => version.checked_add(1).ok_or_else(|| {
            ControlError::invalid_field("record_version", "must not overflow when replaced")
        })?,
    };
    if record_version == required_version {
        return Ok(());
    }
    Err(ControlError::invalid_field(
        "record_version",
        "must advance exactly once from the expected version",
    ))
}

fn apply_raw_record(
    tenants: &mut BTreeMap<String, TenantRecord>,
    key_bytes: &[u8],
    value_bytes: Option<&[u8]>,
) {
    let Ok(key) = RegistryKey::decode(key_bytes) else {
        return;
    };
    let name = key.name.as_str().to_string();
    let Some(value_bytes) = value_bytes else {
        tenants.remove(&name);
        return;
    };
    let Ok(record) = decode_registry_record(value_bytes) else {
        return;
    };
    if record.name != key.name {
        return;
    }
    match tenants.get(&name) {
        Some(current) if current.record_version >= record.record_version => {}
        _ => {
            let record = merge_monotonic_generation(tenants.get(&name), record);
            tenants.insert(name, record);
        }
    }
}

fn merge_monotonic_generation(
    current: Option<&TenantRecord>,
    mut incoming: TenantRecord,
) -> TenantRecord {
    let Some(current) = current else {
        return incoming;
    };
    incoming.wal_generation = incoming.wal_generation.max(current.wal_generation);
    for incoming_range in &mut incoming.ranges {
        if let Some(current_range) = current
            .ranges
            .iter()
            .find(|range| range.range_id == incoming_range.range_id)
        {
            incoming_range.wal_generation = incoming_range
                .wal_generation
                .max(current_range.wal_generation);
        }
    }
    incoming
}

fn canonical_replacement(current: Option<&TenantRecord>, record: TenantRecord) -> TenantRecord {
    merge_monotonic_generation(current, record)
}

fn mutate_layout_if_version(
    current: TenantRecord,
    expected_record_version: u64,
    mutation: RangeLayoutMutation,
) -> Result<TenantRecord, ControlError> {
    if current.record_version == expected_record_version {
        return current.mutate_range_layout(mutation);
    }

    if is_layout_mutation_already_applied(&current, &mutation) {
        return Ok(current);
    }

    Err(ControlError::RegistryVersionConflict {
        tenant: current.name,
        expected: expected_record_version,
        actual: current.record_version,
    })
}

fn is_layout_mutation_already_applied(
    current: &TenantRecord,
    mutation: &RangeLayoutMutation,
) -> bool {
    match mutation {
        RangeLayoutMutation::Split(split) => is_split_already_applied(current, split),
        RangeLayoutMutation::Merge(merge) => is_merge_already_applied(current, merge),
    }
}

fn is_split_already_applied(current: &TenantRecord, split: &RangeLayoutSplit) -> bool {
    current.ranges.windows(2).any(|pair| {
        pair[0].range_id == split.left.range_id
            && pair[0].end_key == split.left.end_key
            && pair[0].endpoint == split.left.endpoint
            && pair[0].wal_generation >= split.left.wal_generation
            && pair[1].range_id == split.right.range_id
            && pair[1].end_key == split.right.end_key
            && pair[1].endpoint == split.right.endpoint
            && pair[1].wal_generation >= split.right.wal_generation
    })
}

fn is_merge_already_applied(current: &TenantRecord, merge: &RangeLayoutMerge) -> bool {
    if current
        .ranges
        .iter()
        .any(|range| range.range_id == merge.right_range_id)
    {
        return false;
    }
    let Some(left) = current
        .ranges
        .iter()
        .find(|range| range.range_id == merge.left_range_id)
    else {
        return false;
    };

    left.endpoint == merge.merged_endpoint && left.wal_generation >= merge.merged_wal_generation
}

async fn ensure_registry_topic(bootstrap: &str, replicas: i32) -> Result<WireUuid, ControlError> {
    ensure_compacted_single_partition_topic(bootstrap, TENANT_REGISTRY_TOPIC, replicas).await
}

async fn ensure_compacted_single_partition_topic(
    bootstrap: &str,
    topic: &str,
    replicas: i32,
) -> Result<WireUuid, ControlError> {
    let bootstrap_addrs = split_bootstrap(bootstrap);
    let mut admin = AdminClient::connect(&bootstrap_addrs).await?;
    let spec = CreateTopicSpec {
        name: topic.to_string(),
        partitions: 1,
        replicas,
        configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
    };
    let outcomes = admin.create_topics(&[spec], 15_000).await?;
    if let Some(outcome) = outcomes.into_iter().next() {
        match outcome.error {
            None => {
                if let Some(id) = outcome.topic_id {
                    return Ok(to_wire_uuid(id));
                }
            }
            Some(error) if error.code == TOPIC_ALREADY_EXISTS => {}
            Some(error) => {
                return Err(ControlError::TopicCreateFailed {
                    topic: outcome.name,
                    name: error.name,
                    code: error.code,
                });
            }
        }
    }
    let metadata = admin.metadata(&[topic]).await?;
    let entry = metadata
        .topics
        .into_iter()
        .find(|entry| entry.name == topic)
        .ok_or_else(|| ControlError::TopicMissing(topic.to_string()))?;
    Ok(entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid))
}

fn spawn_reader(
    bootstrap: String,
    topic_id: WireUuid,
    tenants: Arc<RwLock<BTreeMap<String, TenantRecord>>>,
    applied_tx: watch::Sender<i64>,
) {
    tokio::spawn(async move {
        let mut next_offset = 0_i64;
        loop {
            let Some(addr) = resolve_bootstrap_addr(&bootstrap) else {
                tracing::error!(%bootstrap, "gres control registry reader: bad bootstrap address");
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };
            let opts = ConnectionOptions {
                client_id: "crabka-gres-control-reader".to_string(),
                ..Default::default()
            };
            let conn = match Connection::connect_with_options(addr, opts).await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::warn!(%error, "gres control registry reader: connect failed");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
            };
            loop {
                match fetch_partition_with_isolation_progress(
                    &conn,
                    TENANT_REGISTRY_TOPIC,
                    topic_id,
                    0,
                    next_offset,
                    FETCH_MAX_WAIT_MS,
                    FETCH_PARTITION_MAX_BYTES,
                    READ_COMMITTED,
                )
                .await
                {
                    Ok(result) => {
                        let Some(progress) = result.next_offset else {
                            continue;
                        };
                        for record in result.records {
                            if record.offset < next_offset {
                                continue;
                            }
                            write_tenants(&tenants, |state| {
                                apply_raw_record(
                                    state,
                                    record.key.as_deref().unwrap_or_default(),
                                    record.value.as_deref(),
                                );
                            });
                            let _ = applied_tx.send(record.offset);
                        }
                        next_offset = progress;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "gres control registry reader: fetch failed");
                        conn.close();
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        break;
                    }
                }
            }
        }
    });
}

fn split_bootstrap(bootstrap: &str) -> Vec<String> {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

fn resolve_bootstrap_addr(bootstrap: &str) -> Option<SocketAddr> {
    bootstrap
        .split(',')
        .filter_map(|entry| entry.trim().to_socket_addrs().ok())
        .find_map(|mut addrs| addrs.next())
}

fn to_wire_uuid(id: uuid::Uuid) -> WireUuid {
    WireUuid(*id.as_bytes())
}

fn read_tenants(
    tenants: &RwLock<BTreeMap<String, TenantRecord>>,
) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, TenantRecord>> {
    tenants
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_tenants(
    tenants: &RwLock<BTreeMap<String, TenantRecord>>,
    apply: impl FnOnce(&mut BTreeMap<String, TenantRecord>),
) {
    let mut guard = tenants
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    apply(&mut guard);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_broker::{Broker, BrokerConfig};
    use proptest::prelude::*;
    use tempfile::TempDir;

    use super::*;
    use crate::record::{
        RangeBoundary, SqlUser, TenantId, TenantState, decode_tenant_config_record,
    };

    fn tenant_name(name: &str) -> TenantName {
        TenantName::try_from(name).unwrap()
    }

    fn record(name: &str, version: u64, state: TenantState) -> TenantRecord {
        TenantRecord::new(
            version,
            TenantId::try_from(name).unwrap(),
            tenant_name(name),
            state,
            SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            3,
        )
        .unwrap()
    }

    fn ranged_record(name: &str, version: u64) -> TenantRecord {
        record(name, version - 1, TenantState::Active)
            .with_range_layout(vec![crate::record::RangeLayoutEntry {
                range_id: 0,
                end_key: None,
                endpoint: "tenant-a-r0.gres.svc:7432".to_string(),
                wal_generation: 7,
                lifecycle: Default::default(),
                retirement: None,
            }])
            .unwrap()
    }

    fn split_layout(split_key: RangeBoundary) -> RangeLayoutSplit {
        RangeLayoutSplit {
            source_range_id: 0,
            predecessor_generation: 7,
            left: crate::record::RangeLayoutEntry {
                range_id: 1,
                end_key: Some(split_key),
                endpoint: "tenant-a-r1.gres.svc:7432".to_string(),
                wal_generation: 8,
                lifecycle: Default::default(),
                retirement: None,
            },
            right: crate::record::RangeLayoutEntry {
                range_id: 2,
                end_key: None,
                endpoint: "tenant-a-r2.gres.svc:7432".to_string(),
                wal_generation: 8,
                lifecycle: Default::default(),
                retirement: None,
            },
        }
    }

    fn encoded(record: &TenantRecord) -> (Vec<u8>, Option<Vec<u8>>) {
        let (key, value) = encode_registry_record(record).unwrap();
        (key, Some(value))
    }

    fn tombstone(name: &str) -> (Vec<u8>, Option<Vec<u8>>) {
        (tenant_registry_key(&tenant_name(name)).unwrap(), None)
    }

    #[test]
    fn fold_applies_create_update_suspend_and_tombstone_orderings() {
        let create = record("tenant-a", 1, TenantState::Active);
        let suspend = record("tenant-a", 2, TenantState::Suspended);

        let folded = fold(vec![encoded(&create), encoded(&suspend)].into_iter());
        assert!(folded["tenant-a"].state == TenantState::Suspended);

        let folded = fold(vec![encoded(&create), tombstone("tenant-a")].into_iter());
        assert!(folded.is_empty());
    }

    #[test]
    fn fold_rejects_divergent_equal_version_snapshots() {
        let active_v2 = record("tenant-a", 2, TenantState::Active);
        let suspended_v1 = record("tenant-a", 1, TenantState::Suspended);
        let suspended_v2 = record("tenant-a", 2, TenantState::Suspended);

        let folded = fold(vec![encoded(&active_v2), encoded(&suspended_v1)].into_iter());
        assert!(folded["tenant-a"].state == TenantState::Active);

        let folded = fold(vec![encoded(&active_v2), encoded(&suspended_v2)].into_iter());
        assert!(folded["tenant-a"].state == TenantState::Active);
    }

    #[test]
    fn fold_resolves_lifecycle_sequence_by_distinct_record_versions() {
        let suspended = record("tenant-a", 2, TenantState::Suspended);
        let requested = record("tenant-a", 3, TenantState::ResumeRequested);
        let active = record("tenant-a", 4, TenantState::Active);

        let folded =
            fold(vec![encoded(&active), encoded(&suspended), encoded(&requested)].into_iter());

        assert!(folded["tenant-a"].state == TenantState::Active);
    }

    #[test]
    fn fold_collapses_duplicate_resume_requests() {
        let first = record("tenant-a", 3, TenantState::ResumeRequested);
        let duplicate = record("tenant-a", 3, TenantState::ResumeRequested);

        let folded = fold(vec![encoded(&first), encoded(&duplicate)].into_iter());

        assert!(folded["tenant-a"].record_version == 3);
        assert!(folded["tenant-a"].state == TenantState::ResumeRequested);
    }

    #[test]
    fn fold_preserves_tombstone_over_lifecycle_state() {
        let requested = record("tenant-a", 3, TenantState::ResumeRequested);

        let folded = fold(vec![encoded(&requested), tombstone("tenant-a")].into_iter());

        assert!(folded.is_empty());
    }

    #[test]
    fn fold_never_decreases_wal_generation() {
        let mut older_generation = record("tenant-a", 4, TenantState::Active);
        older_generation.wal_generation = 7;
        let mut newer_version = record("tenant-a", 5, TenantState::Active);
        newer_version.wal_generation = 2;

        let folded = fold(vec![encoded(&older_generation), encoded(&newer_version)].into_iter());

        assert!(folded["tenant-a"].record_version == 5);
        assert!(folded["tenant-a"].wal_generation == 7);
    }

    #[test]
    fn fold_never_decreases_per_range_wal_generation() {
        let mut older_generation = record("tenant-a", 4, TenantState::Active);
        older_generation.ranges = vec![crate::record::RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "tenant-a-r1.gres.svc:7432".to_string(),
            wal_generation: 9,
            lifecycle: Default::default(),
            retirement: None,
        }];
        let mut newer_version = record("tenant-a", 5, TenantState::Active);
        newer_version.ranges = vec![crate::record::RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "tenant-a-r1-new.gres.svc:7432".to_string(),
            wal_generation: 3,
            lifecycle: Default::default(),
            retirement: None,
        }];

        let folded = fold(vec![encoded(&older_generation), encoded(&newer_version)].into_iter());

        assert!(folded["tenant-a"].record_version == 5);
        assert!(folded["tenant-a"].ranges[0].endpoint == "tenant-a-r1-new.gres.svc:7432");
        assert!(folded["tenant-a"].ranges[0].wal_generation == 9);
    }

    #[test]
    fn in_memory_store_upsert_delete_and_tombstone_are_idempotent() {
        let mut store = InMemoryRegistryStore::new();
        let active = record("tenant-a", 1, TenantState::Active);
        let stale = record("tenant-a", 2, TenantState::Suspended);
        let name = tenant_name("tenant-a");

        store.upsert(active.clone()).unwrap();
        store.upsert(active.clone()).unwrap();
        assert!(store.list() == vec![active.clone()]);

        store.upsert(stale.clone()).unwrap();
        assert!(store.get(&name).unwrap() == stale);

        store.delete(&name).unwrap();
        store.delete(&name).unwrap();
        assert!(store.get(&name).is_none());
    }

    #[test]
    fn in_memory_store_replaces_only_at_the_observed_version() {
        let mut store = InMemoryRegistryStore::new();
        let original = record("tenant-a", 1, TenantState::Active);
        let replacement = record("tenant-a", 2, TenantState::Suspended);

        store.replace_if_version(original.clone(), None).unwrap();
        store
            .replace_if_version(replacement.clone(), Some(1))
            .unwrap();
        store
            .replace_if_version(replacement.clone(), Some(1))
            .expect("exact replacement retry is idempotent");

        let error = store
            .replace_if_version(record("tenant-a", 2, TenantState::Active), Some(1))
            .expect_err("stale replacement is rejected");
        assert!(matches!(
            error,
            ControlError::RegistryVersionConflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));
        assert!(store.get(&tenant_name("tenant-a")) == Some(replacement));
    }

    #[test]
    fn replacement_canonicalizes_generations_before_idempotence_checks() {
        let mut store = InMemoryRegistryStore::new();
        let name = tenant_name("tenant-a");
        let mut current = ranged_record("tenant-a", 4);
        current.wal_generation = 7;
        let mut replacement = ranged_record("tenant-a", 5);
        replacement.wal_generation = 1;
        replacement.ranges[0].wal_generation = 2;

        store.upsert(current).unwrap();
        let stored = store
            .replace_if_version(replacement.clone(), Some(4))
            .expect("replacement is canonicalized before it is stored");
        store
            .replace_if_version(replacement, Some(4))
            .expect("the same lower-generation retry is idempotent");

        let canonical = store.get(&name).expect("replacement stored");
        let config = decode_tenant_config_record(
            &encode_tenant_config_record(&stored).expect("encode canonical config"),
        )
        .expect("decode canonical config");
        assert!(canonical == stored);
        assert!(config == canonical);
        assert!(canonical.wal_generation == 7);
        assert!(canonical.ranges[0].wal_generation == 7);
    }

    #[test]
    fn versioned_replacement_requires_the_immediate_successor_version() {
        assert!(validate_replacement_version(1, None).is_ok());
        assert!(validate_replacement_version(2, Some(1)).is_ok());
        assert!(validate_replacement_version(1, Some(1)).is_err());
        assert!(validate_replacement_version(3, Some(1)).is_err());
        assert!(validate_replacement_version(1, Some(u64::MAX)).is_err());
    }

    #[test]
    fn registry_writer_reinitializes_only_for_uncertain_transaction_outcomes() {
        assert!(requires_writer_reinitialization(
            &ProducerError::RecoveryRequired
        ));
        assert!(!requires_writer_reinitialization(
            &ProducerError::ConcurrentTransactions
        ));
        assert!(!requires_writer_reinitialization(
            &ProducerError::FencedProducer
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_writer_recovers_a_dropped_transaction_before_mutating() {
        let directory = TempDir::new().expect("broker tempdir");
        let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        let mut registry = Registry::connect(&bootstrap)
            .await
            .expect("registry connect");
        registry.ensure_topic(1).await.expect("registry topic");

        registry
            .producer
            .init_transactions()
            .await
            .expect("writer initialization");
        drop(
            registry
                .producer
                .begin_transaction()
                .await
                .expect("writer transaction"),
        );

        let tenant = record("tenant-a", 1, TenantState::Active);
        registry
            .upsert(&tenant)
            .await
            .expect("writer reinitializes before mutation");
        assert!(registry.get("tenant-a").await.unwrap() == Some(tenant));
        broker.shutdown().await;
    }

    #[test]
    fn in_memory_store_versioned_split_is_idempotent() {
        let mut store = InMemoryRegistryStore::new();
        let name = tenant_name("tenant-a");
        store.upsert(ranged_record("tenant-a", 4)).unwrap();

        store
            .split_range_layout_if_version(&name, 4, split_layout(RangeBoundary::table_start(100)))
            .unwrap();
        let split = store.get(&name).unwrap();
        assert!(split.record_version == 5);
        assert!(split.ranges.len() == 2);
        assert!(split.ranges[0].wal_generation == 8);
        assert!(split.ranges[1].wal_generation == 8);
    }

    #[test]
    fn in_memory_versioned_split_rejects_stale_version_and_preserves_layout() {
        let mut store = InMemoryRegistryStore::new();
        let name = tenant_name("tenant-a");
        store.upsert(ranged_record("tenant-a", 4)).unwrap();

        let result = store.split_range_layout_if_version(
            &name,
            3,
            split_layout(RangeBoundary::table_start(100)),
        );

        assert!(matches!(
            result,
            Err(ControlError::RegistryVersionConflict {
                expected: 3,
                actual: 4,
                ..
            })
        ));
        assert!(store.get(&name).unwrap() == ranged_record("tenant-a", 4));
    }

    #[test]
    fn in_memory_versioned_split_mutates_layout_and_retries_idempotently() {
        let mut store = InMemoryRegistryStore::new();
        let name = tenant_name("tenant-a");
        store.upsert(ranged_record("tenant-a", 4)).unwrap();
        let split = split_layout(RangeBoundary::table_start(100));

        let first = store
            .split_range_layout_if_version(&name, 4, split.clone())
            .unwrap()
            .unwrap();
        let retry = store
            .split_range_layout_if_version(&name, 4, split)
            .unwrap()
            .unwrap();

        assert!(first == retry);
        assert!(first.record_version == 5);
        assert!(first.ranges.len() == 2);
        assert!(first.ranges[0].end_key == Some(RangeBoundary::table_start(100)));
    }

    #[test]
    fn in_memory_versioned_merge_mutates_layout_and_retries_idempotently() {
        let mut store = InMemoryRegistryStore::new();
        let name = tenant_name("tenant-a");
        let split = ranged_record("tenant-a", 4)
            .split_range_layout(split_layout(RangeBoundary::table_start(100)))
            .unwrap();
        store.upsert(split).unwrap();
        let merge = RangeLayoutMerge {
            left_range_id: 1,
            right_range_id: 2,
            merged_endpoint: "tenant-a-r1-merged.gres.svc:7432".to_string(),
            merged_wal_generation: 9,
        };

        let first = store
            .merge_range_layout_if_version(&name, 5, merge.clone())
            .unwrap()
            .unwrap();
        let retry = store
            .merge_range_layout_if_version(&name, 5, merge)
            .unwrap()
            .unwrap();

        assert!(first == retry);
        assert!(first.record_version == 6);
        assert!(first.ranges.len() == 1);
        assert!(first.ranges[0].range_id == 1);
        assert!(first.ranges[0].endpoint == "tenant-a-r1-merged.gres.svc:7432");
        assert!(first.ranges[0].wal_generation == 9);
    }

    #[test]
    fn split_layout_has_exactly_one_owner_per_table() {
        let mut store = InMemoryRegistryStore::new();
        let name = tenant_name("tenant-a");
        store.upsert(ranged_record("tenant-a", 4)).unwrap();
        store
            .split_range_layout_if_version(&name, 4, split_layout(RangeBoundary::new(100, 25)))
            .unwrap();
        let split = store.get(&name).unwrap();

        assert!(owners(&split, 99, u64::MAX) == vec![1]);
        assert!(owners(&split, 100, 0) == vec![1]);
        assert!(owners(&split, 100, 25) == vec![2]);
    }

    fn owners(record: &TenantRecord, table: u64, rowid: u64) -> Vec<u32> {
        let mut previous_end = crate::record::RangeBoundary::table_start(0);
        let key = crate::record::RangeBoundary::new(table, rowid);
        record
            .ranges
            .iter()
            .filter_map(|range| {
                let owns = key >= previous_end && range.end_key.is_none_or(|end| key < end);
                if let Some(end) = range.end_key {
                    previous_end = end;
                }
                owns.then_some(range.range_id)
            })
            .collect()
    }

    proptest! {
        #[test]
        fn fold_selects_highest_version_for_any_replay_order(versions in proptest::collection::vec(1_u64..=8, 1..16)) {
            let expected = versions.iter().copied().max().unwrap();
            let records = versions
                .into_iter()
                .map(|version| encoded(&record("tenant-a", version, TenantState::Active)))
                .collect::<Vec<_>>();
            let folded = fold(records.into_iter());
            prop_assert_eq!(folded["tenant-a"].record_version, expected);
        }
    }
}
