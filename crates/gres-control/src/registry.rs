//! Registry store abstractions and Kafka-backed registry client.

#[cfg(test)]
use std::time::Duration;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, RwLock},
};

use bytes::Bytes;
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::{
    ClientDnsTimeout, ClientFrameMax, Connection, ConnectionDispatchQueueCapacity,
    ConnectionOptions, DEFAULT_FETCH_RESPONSE_MAX, FetchMinBytes, IsolatedFetch,
    fetch_partition_with_isolation_progress,
};
use crabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord, Transaction};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
#[cfg(test)]
use crabka_units::{bytes, millis};
use refined_type::rule::{GreaterU64, MinMaxI32};
use tokio::sync::{Mutex, watch};

use crate::{
    ControlError,
    record::{
        FinalCheckpoint, RangeLayoutMerge, RangeLayoutMutation, RangeLayoutSplit, RegistryKey,
        SplitOperationRecord, TENANT_REGISTRY_TOPIC, TenantName, TenantRecord,
        decode_registry_record, encode_registry_record, encode_tenant_config_record,
        tenant_config_topic, tenant_registry_key,
    },
};

const TOPIC_ALREADY_EXISTS: i16 = 36;
const READ_COMMITTED: i8 = 1;
const REGISTRY_TRANSACTIONAL_ID: &str = "__gres_tenants.writer";

/// A Kafka replication factor representable on the protocol wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryReplicationFactor(i32);

impl RegistryReplicationFactor {
    /// Validates a replication factor.
    ///
    /// # Errors
    ///
    /// Returns an error unless `value` is in `1..=32767`.
    pub fn new(value: i32) -> Result<Self, String> {
        MinMaxI32::<1, 32_767>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Returns the validated value.
    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

impl FromStr for RegistryReplicationFactor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// A positive value representable as a protocol `i32`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveI32(i32);

impl PositiveI32 {
    /// Validates a positive protocol value.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not positive.
    pub fn new(value: i32) -> Result<Self, String> {
        MinMaxI32::<1, { i32::MAX }>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Returns the validated value.
    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

impl FromStr for PositiveI32 {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// A positive millisecond count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveMillis(u64);

impl PositiveMillis {
    /// Validates a positive millisecond count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u64) -> Result<Self, String> {
        GreaterU64::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Returns the validated value.
    #[must_use]
    pub const fn into_value(self) -> u64 {
        self.0
    }
}

impl FromStr for PositiveMillis {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Shared creation and reader policy for the Gres tenant registry topic.
///
/// This type is not `Eq`, because the timeout and size fields are `f64`-backed
/// quantities.
#[derive(Clone, Debug, PartialEq)]
pub struct RegistryPolicy {
    replication_factor: i32,
    topic_create_timeout: Time,
    reader_retry_backoff: Time,
    fetch_max_wait: Time,
    fetch_partition_max: ByteSize,
    producer_dns_timeout: ClientDnsTimeout,
    reader_admin_dns_timeout: ClientDnsTimeout,
    dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    frame_max: ClientFrameMax,
    reader_fetch_min: FetchMinBytes,
}

impl RegistryPolicy {
    /// Validates and constructs a registry policy.
    ///
    /// # Errors
    ///
    /// Returns an error when any value is outside its supported range.
    pub fn new(
        replication_factor: i32,
        topic_create_timeout: Time,
        reader_retry_backoff: Time,
        fetch_max_wait: Time,
        fetch_partition_max: ByteSize,
    ) -> Result<Self, String> {
        Ok(Self {
            replication_factor: RegistryReplicationFactor::new(replication_factor)?.into_value(),
            topic_create_timeout: whole_millis_i32("topic_create_timeout", topic_create_timeout)?,
            reader_retry_backoff: whole_millis_i64("reader_retry_backoff", reader_retry_backoff)?,
            fetch_max_wait: whole_millis_i32("fetch_max_wait", fetch_max_wait)?,
            fetch_partition_max: whole_bytes_i32("fetch_partition_max", fetch_partition_max)?,
            producer_dns_timeout: ClientDnsTimeout::default(),
            reader_admin_dns_timeout: ClientDnsTimeout::default(),
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
            reader_fetch_min: FetchMinBytes::default(),
        })
    }

    /// Registry topic replication factor.
    #[must_use]
    pub const fn replication_factor(&self) -> i32 {
        self.replication_factor
    }

    /// Kafka topic-creation timeout.
    #[must_use]
    pub const fn topic_create_timeout(&self) -> Time {
        self.topic_create_timeout
    }

    /// Delay after a registry reader failure.
    #[must_use]
    pub const fn reader_retry_backoff(&self) -> Time {
        self.reader_retry_backoff
    }

    /// Maximum time a registry fetch waits for data.
    #[must_use]
    pub const fn fetch_max_wait(&self) -> Time {
        self.fetch_max_wait
    }

    /// Maximum bytes fetched from the registry partition.
    #[must_use]
    pub const fn fetch_partition_max(&self) -> ByteSize {
        self.fetch_partition_max
    }

    /// DNS lookup deadline used by the registry producer.
    #[must_use]
    pub const fn producer_dns_timeout(&self) -> ClientDnsTimeout {
        self.producer_dns_timeout
    }

    /// Validates and replaces the registry producer DNS lookup deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the timeout is not a positive whole millisecond.
    #[must_use = "the validated policy must be used"]
    pub fn with_producer_dns_timeout(mut self, timeout: Time) -> Result<Self, String> {
        self.producer_dns_timeout =
            ClientDnsTimeout::new(whole_millis_i64("producer_dns_timeout", timeout)?)?;
        Ok(self)
    }

    /// DNS lookup deadline used by registry reader and admin paths.
    #[must_use]
    pub const fn reader_admin_dns_timeout(&self) -> ClientDnsTimeout {
        self.reader_admin_dns_timeout
    }

    /// Validates and replaces the registry reader/admin DNS lookup deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the timeout is not a positive whole millisecond.
    #[must_use = "the validated policy must be used"]
    pub fn with_reader_admin_dns_timeout(mut self, timeout: Time) -> Result<Self, String> {
        self.reader_admin_dns_timeout =
            ClientDnsTimeout::new(whole_millis_i64("reader_admin_dns_timeout", timeout)?)?;
        Ok(self)
    }

    /// Overrides the registry client connection and reader fetch policy.
    #[must_use]
    pub fn with_client_resource_policy(
        mut self,
        dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
        frame_max: ClientFrameMax,
        reader_fetch_min: FetchMinBytes,
    ) -> Self {
        self.dispatch_queue_capacity = dispatch_queue_capacity;
        self.frame_max = frame_max;
        self.reader_fetch_min = reader_fetch_min;
        self
    }

    #[must_use]
    pub const fn dispatch_queue_capacity(&self) -> ConnectionDispatchQueueCapacity {
        self.dispatch_queue_capacity
    }

    #[must_use]
    pub const fn frame_max(&self) -> ClientFrameMax {
        self.frame_max
    }

    #[must_use]
    pub const fn reader_fetch_min(&self) -> FetchMinBytes {
        self.reader_fetch_min
    }
}

impl Default for RegistryPolicy {
    fn default() -> Self {
        Self::new(
            1,
            crabka_units::secs(15),
            crabka_units::millis(250),
            crabka_units::millis(500),
            crabka_units::mebibytes(1),
        )
        .expect("default registry policy is valid")
    }
}

fn whole_millis_i64(name: &str, value: Time) -> Result<Time, String> {
    let millis = value.millis_i64();
    if value.secs_f64().is_finite() && millis > 0 && Time::from_millis(millis) == value {
        Ok(value)
    } else {
        Err(format!(
            "{name} must be a positive whole number of milliseconds"
        ))
    }
}

fn whole_bytes_i32(name: &str, value: ByteSize) -> Result<ByteSize, String> {
    let bytes = value.bytes_f64();
    if bytes.is_finite() && bytes > 0.0 && bytes.fract() == 0.0 && bytes <= f64::from(i32::MAX) {
        Ok(value)
    } else {
        Err(format!(
            "{name} must be a positive whole number of bytes within the i32 range"
        ))
    }
}

fn whole_millis_i32(name: &str, value: Time) -> Result<Time, String> {
    let value = whole_millis_i64(name, value)?;
    PositiveI32::new(
        value
            .millis_i64()
            .try_into()
            .map_err(|error: std::num::TryFromIntError| format!("{name}: {error}"))?,
    )?;
    Ok(value)
}

/// Pure tenant-registry store seam for operator and CLI code.
pub trait TenantRegistryStore {
    /// Upserts one whole tenant snapshot.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn upsert(&mut self, record: TenantRecord) -> Result<(), ControlError>;
    /// Creates a tenant when it is absent, or replaces its whole snapshot only
    /// when the observed version still matches. `None` is the create-only
    /// precondition.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
    /// Tombstones one tenant by name. This is idempotent when the tenant is
    /// already absent.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn delete(&mut self, tenant: &TenantName) -> Result<(), ControlError>;
    /// Returns one tenant by name.
    fn get(&self, tenant: &TenantName) -> Option<TenantRecord>;
    /// Returns all tenants ordered by name.
    fn list(&self) -> Vec<TenantRecord>;
    /// Applies a versioned split/merge mutation without overwriting concurrent changes.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn mutate_range_layout_if_version(
        &mut self,
        tenant: &TenantName,
        expected_record_version: u64,
        mutation: RangeLayoutMutation,
    ) -> Result<Option<TenantRecord>, ControlError>;
    /// Splits one tenant range layout when the tenant is still at `expected_record_version`.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn split_range_layout_if_version(
        &mut self,
        tenant: &TenantName,
        expected_record_version: u64,
        split: RangeLayoutSplit,
    ) -> Result<Option<TenantRecord>, ControlError> {
        self.mutate_range_layout_if_version(
            tenant,
            expected_record_version,
            RangeLayoutMutation::Split(Box::new(split)),
        )
    }
    /// Merges two adjacent tenant ranges when the tenant is still at `expected_record_version`.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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

    /// Idempotently creates a durable split-operation initiation record.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn begin_split_operation(
        &mut self,
        _operation: SplitOperationRecord,
    ) -> Result<SplitOperationRecord, ControlError> {
        Err(ControlError::UnsupportedRegistryMutation {
            mutation: "begin_split_operation",
            reason: "backend does not implement split-operation journaling",
        })
    }

    /// Loads one split operation by tenant and operation id.
    fn load_split_operation(
        &self,
        _tenant: &TenantName,
        _operation_id: &str,
    ) -> Option<SplitOperationRecord> {
        None
    }

    /// Lists one tenant's split operations in operation-id order.
    fn list_split_operations(&self, _tenant: &TenantName) -> Vec<SplitOperationRecord> {
        Vec::new()
    }

    /// Persists one exact monotone split-operation revision with CAS semantics.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn compare_and_swap_split_operation(
        &mut self,
        _expected_revision: Option<u64>,
        _operation: SplitOperationRecord,
    ) -> Result<SplitOperationRecord, ControlError> {
        Err(ControlError::UnsupportedRegistryMutation {
            mutation: "compare_and_swap_split_operation",
            reason: "backend does not implement split-operation journaling",
        })
    }
}

/// In-memory implementation of [`TenantRegistryStore`] for tests and future
/// fakes.
#[derive(Debug, Default)]
pub struct InMemoryRegistryStore {
    tenants: Arc<StdMutex<BTreeMap<String, TenantRecord>>>,
    split_operations: Arc<StdMutex<BTreeMap<(String, String), SplitOperationRecord>>>,
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
            split_operations: Arc::new(StdMutex::new(
                self.split_operations
                    .lock()
                    .expect("in-memory split operation lock poisoned")
                    .clone(),
            )),
        }
    }
}

impl InMemoryRegistryStore {
    /// Builds an empty in-memory registry store.
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

    fn begin_split_operation(
        &mut self,
        operation: SplitOperationRecord,
    ) -> Result<SplitOperationRecord, ControlError> {
        operation.ensure_valid()?;
        if operation.revision != 0
            || operation.phase != crate::record::SplitOperationPhase::Initiated
            || operation.attempts != 0
            || !operation.errors.is_empty()
        {
            return Err(split_operation_conflict(
                &operation,
                "begin requires a revision-zero initiated record",
            ));
        }
        let key = split_operation_map_key(&operation.tenant, &operation.operation_id);
        let mut operations = self
            .split_operations
            .lock()
            .expect("in-memory split operation lock poisoned");
        match operations.get(&key) {
            Some(current)
                if current.tenant == operation.tenant
                    && current.operation_id == operation.operation_id
                    && current.mutation == operation.mutation
                    && current.plan == operation.plan =>
            {
                Ok(current.clone())
            }
            Some(_) => Err(split_operation_conflict(
                &operation,
                "operation id already names a different split intent",
            )),
            None => {
                if operations.values().any(|current| {
                    current.tenant == operation.tenant
                        && !matches!(
                            current.phase,
                            crate::record::SplitOperationPhase::Completed
                                | crate::record::SplitOperationPhase::Failed
                        )
                }) {
                    return Err(split_operation_conflict(
                        &operation,
                        "tenant already has an active range mutation",
                    ));
                }
                operations.insert(key, operation.clone());
                Ok(operation)
            }
        }
    }

    fn load_split_operation(
        &self,
        tenant: &TenantName,
        operation_id: &str,
    ) -> Option<SplitOperationRecord> {
        self.split_operations
            .lock()
            .expect("in-memory split operation lock poisoned")
            .get(&split_operation_map_key(tenant, operation_id))
            .cloned()
    }

    fn list_split_operations(&self, tenant: &TenantName) -> Vec<SplitOperationRecord> {
        self.split_operations
            .lock()
            .expect("in-memory split operation lock poisoned")
            .range((tenant.as_str().to_string(), String::new())..)
            .take_while(|((name, _), _)| name == tenant.as_str())
            .map(|(_, operation)| operation.clone())
            .collect()
    }

    fn compare_and_swap_split_operation(
        &mut self,
        expected_revision: Option<u64>,
        operation: SplitOperationRecord,
    ) -> Result<SplitOperationRecord, ControlError> {
        let key = split_operation_map_key(&operation.tenant, &operation.operation_id);
        let mut operations = self
            .split_operations
            .lock()
            .expect("in-memory split operation lock poisoned");
        let current = operations
            .get(&key)
            .ok_or_else(|| split_operation_conflict(&operation, "operation does not exist"))?;
        if current == &operation {
            return Ok(current.clone());
        }
        if expected_revision != Some(current.revision) {
            return Err(split_operation_conflict(
                &operation,
                "expected revision differs from durable revision",
            ));
        }
        operation.ensure_monotone_extension(current)?;
        operations.insert(key, operation.clone());
        Ok(operation)
    }
}

fn split_operation_map_key(tenant: &TenantName, operation_id: &str) -> (String, String) {
    (tenant.as_str().to_string(), operation_id.to_string())
}

fn split_operation_conflict(
    operation: &SplitOperationRecord,
    reason: impl Into<String>,
) -> ControlError {
    ControlError::SplitOperationConflict {
        tenant: operation.tenant.clone(),
        operation_id: operation.operation_id.clone(),
        reason: reason.into(),
    }
}

/// Folds raw compacted-topic records into the latest tenant image.
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
    policy: RegistryPolicy,
    producer: Producer,
    tenants: Arc<RwLock<BTreeMap<String, TenantRecord>>>,
    split_operations: Arc<RwLock<BTreeMap<(String, String), SplitOperationRecord>>>,
    applied_rx: watch::Receiver<i64>,
    applied_tx: watch::Sender<i64>,
    write_gate: Mutex<()>,
    /// Background reader task that keeps `tenants` and `split_operations`
    /// fresh. It aborts when the registry drops, so a short-lived holder such
    /// as CLI provisioning or a test harness does not leak a poll loop that
    /// retries against a broker that may already be gone.
    reader: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Registry {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
    }
}

impl Registry {
    /// Connects producer-side registry resources. Call [`Self::ensure_topic`] before writes.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn connect(bootstrap: &str) -> Result<Self, ControlError> {
        Self::connect_with_policy(bootstrap, RegistryPolicy::default()).await
    }

    /// Connects registry resources using an explicit shared topic policy.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn connect_with_policy(
        bootstrap: &str,
        policy: RegistryPolicy,
    ) -> Result<Self, ControlError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id("crabka-gres-control-writer")
            .dns_timeout(policy.producer_dns_timeout().time())
            .dispatch_queue_capacity(policy.dispatch_queue_capacity.get())
            .frame_max(policy.frame_max.size())
            .enable_idempotence(true)
            .acks(Acks::All)
            .transactional_id(REGISTRY_TRANSACTIONAL_ID)
            .build()
            .await?;
        let (applied_tx, applied_rx) = watch::channel(-1_i64);
        Ok(Self {
            bootstrap: bootstrap.to_string(),
            policy,
            producer,
            tenants: Arc::new(RwLock::new(BTreeMap::new())),
            split_operations: Arc::new(RwLock::new(BTreeMap::new())),
            applied_rx,
            applied_tx,
            write_gate: Mutex::new(()),
            reader: None,
        })
    }

    /// Returns the effective shared registry policy.
    #[must_use]
    pub const fn policy(&self) -> &RegistryPolicy {
        &self.policy
    }

    /// Makes sure `__gres_tenants` exists as a compacted, one-partition topic.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn ensure_topic(&mut self) -> Result<(), ControlError> {
        let topic_id = ensure_registry_topic(&self.bootstrap, &self.policy).await?;
        if self.reader.is_some() {
            return Ok(());
        }
        self.reader = Some(spawn_reader(
            self.bootstrap.clone(),
            topic_id,
            Arc::clone(&self.tenants),
            Arc::clone(&self.split_operations),
            self.applied_tx.clone(),
            self.policy.clone(),
        ));
        Ok(())
    }

    /// Creates a tenant snapshot, or accepts an exact idempotent retry.
    ///
    /// This unversioned API rejects a replacement of an existing snapshot.
    /// Callers must use a semantic mutation, so the writer can derive the next
    /// version from its fenced, read-committed image.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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

    /// Creates a tenant when it is absent, or replaces its snapshot only when
    /// the caller's observed version still matches the fenced, read-committed
    /// image. An exact retry does nothing.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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

    /// Requests resume for a suspended tenant. This does nothing unless the
    /// current state is `Suspended`.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn request_resume(&mut self, tenant: &str) -> Result<(), ControlError> {
        self.transform_tenant(tenant, TenantRecord::request_resume)
            .await
    }

    /// Marks a tenant active and publishes the endpoint that activators dial.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn mark_active(
        &mut self,
        tenant: &str,
        endpoint: impl Into<String>,
    ) -> Result<(), ControlError> {
        let endpoint = endpoint.into();
        self.transform_tenant(tenant, |record| record.mark_active(endpoint))
            .await
    }

    /// Marks a tenant suspended after its final checkpoint is durable.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn mark_suspended(&mut self, tenant: &str) -> Result<(), ControlError> {
        self.transform_tenant(tenant, TenantRecord::mark_suspended)
            .await
    }

    /// Marks a tenant suspended after it records the durable final checkpoint
    /// that permits parking.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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

    /// Monotonically advances the WAL generation for a tenant.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn bump_wal_generation(
        &mut self,
        tenant: &str,
        generation: u64,
    ) -> Result<(), ControlError> {
        self.transform_tenant(tenant, |record| record.with_wal_generation(generation))
            .await
    }

    /// Applies a versioned range-layout mutation through the fenced registry writer.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn split_range_layout_if_version(
        &mut self,
        tenant: &str,
        expected_record_version: u64,
        split: RangeLayoutSplit,
    ) -> Result<(), ControlError> {
        self.mutate_range_layout_if_version(
            tenant,
            expected_record_version,
            RangeLayoutMutation::Split(Box::new(split)),
        )
        .await
    }

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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

    /// Produces the per-tenant runtime snapshot consumed by substrate computes.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn upsert_tenant_config(
        &self,
        record: &TenantRecord,
        replicas: i32,
    ) -> Result<(), ControlError> {
        let topic = tenant_config_topic(&record.name);
        ensure_compacted_single_partition_topic(&self.bootstrap, &topic, replicas, &self.policy)
            .await?;
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

    /// Produces a tombstone for one tenant and wait until it is locally applied.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn delete(&mut self, tenant: &str) -> Result<(), ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.delete_after_fencing(&tenant).await
    }

    /// Returns the locally applied image for one tenant.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn get(&mut self, tenant: &str) -> Result<Option<TenantRecord>, ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.refresh().await?;
        Ok(read_tenants(&self.tenants).get(tenant.as_str()).cloned())
    }

    /// Returns all locally applied tenants ordered by tenant name.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn list(&mut self) -> Result<Vec<TenantRecord>, ControlError> {
        self.refresh().await?;
        Ok(read_tenants(&self.tenants).values().cloned().collect())
    }

    /// Idempotently persists one immutable split-operation intent.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn begin_split_operation(
        &mut self,
        operation: &SplitOperationRecord,
    ) -> Result<SplitOperationRecord, ControlError> {
        operation.ensure_valid()?;
        if operation.revision != 0
            || operation.phase != crate::record::SplitOperationPhase::Initiated
            || operation.attempts != 0
            || !operation.errors.is_empty()
        {
            return Err(split_operation_conflict(
                operation,
                "begin requires a revision-zero initiated record",
            ));
        }
        let operation = operation.clone();
        self.mutate_split_operation_after_fencing(
            &operation.tenant.clone(),
            &operation.operation_id.clone(),
            move |current| match current {
                Some(current)
                    if current.tenant == operation.tenant
                        && current.operation_id == operation.operation_id
                        && current.mutation == operation.mutation
                        && current.plan == operation.plan =>
                {
                    Ok(None)
                }
                Some(_) => Err(split_operation_conflict(
                    &operation,
                    "operation id already names a different split intent",
                )),
                None => Ok(Some(operation)),
            },
        )
        .await
    }

    /// Loads one durable split operation after refreshing the committed registry image.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn load_split_operation(
        &mut self,
        tenant: &str,
        operation_id: &str,
    ) -> Result<Option<SplitOperationRecord>, ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.refresh().await?;
        Ok(read_split_operations(&self.split_operations)
            .get(&split_operation_map_key(&tenant, operation_id))
            .cloned())
    }

    /// Lists one tenant's durable split operations in operation-id order.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn list_split_operations(
        &mut self,
        tenant: &str,
    ) -> Result<Vec<SplitOperationRecord>, ControlError> {
        let tenant = TenantName::try_from(tenant)?;
        self.refresh().await?;
        Ok(read_split_operations(&self.split_operations)
            .range((tenant.as_str().to_string(), String::new())..)
            .take_while(|((name, _), _)| name == tenant.as_str())
            .map(|(_, operation)| operation.clone())
            .collect())
    }

    /// Persists one exact monotone split-operation revision with fenced CAS semantics.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn compare_and_swap_split_operation(
        &mut self,
        expected_revision: Option<u64>,
        operation: &SplitOperationRecord,
    ) -> Result<SplitOperationRecord, ControlError> {
        let operation = operation.clone();
        self.mutate_split_operation_after_fencing(
            &operation.tenant.clone(),
            &operation.operation_id.clone(),
            move |current| {
                let current = current.ok_or_else(|| {
                    split_operation_conflict(&operation, "operation does not exist")
                })?;
                if current == operation {
                    return Ok(None);
                }
                if expected_revision != Some(current.revision) {
                    return Err(split_operation_conflict(
                        &operation,
                        "expected revision differs from durable revision",
                    ));
                }
                operation.ensure_monotone_extension(&current)?;
                Ok(Some(operation))
            },
        )
        .await
    }

    /// Watches the last offset applied by the registry reader.
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

    /// Fences every other registry writer, then reads, transforms, and commits
    /// one snapshot. The shared transactional id makes the producer epoch a
    /// cross-process write lease. A compacted-topic version alone is not CAS.
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

    async fn mutate_split_operation_after_fencing(
        &mut self,
        tenant: &TenantName,
        operation_id: &str,
        transform: impl FnOnce(
            Option<SplitOperationRecord>,
        ) -> Result<Option<SplitOperationRecord>, ControlError>,
    ) -> Result<SplitOperationRecord, ControlError> {
        let _gate = self.write_gate.lock().await;
        self.initialize_transactional_writer().await?;
        self.refresh().await?;
        let key = split_operation_map_key(tenant, operation_id);
        let current = read_split_operations(&self.split_operations)
            .get(&key)
            .cloned();
        if current.is_none()
            && read_split_operations(&self.split_operations)
                .values()
                .any(|active| {
                    active.tenant == *tenant
                        && !matches!(
                            active.phase,
                            crate::record::SplitOperationPhase::Completed
                                | crate::record::SplitOperationPhase::Failed
                        )
                })
        {
            return Err(ControlError::SplitOperationConflict {
                tenant: tenant.clone(),
                operation_id: operation_id.to_string(),
                reason: "tenant already has an active range mutation".into(),
            });
        }
        let Some(next) = transform(current.clone())? else {
            return current.ok_or_else(|| ControlError::SplitOperationConflict {
                tenant: tenant.clone(),
                operation_id: operation_id.to_string(),
                reason: "idempotent operation is absent".to_string(),
            });
        };
        next.ensure_valid()?;
        let transaction = self.begin_registry_transaction().await?;
        let offset = match self
            .produce(
                encode_split_operation_key(&next.tenant, &next.operation_id)?,
                Some(serde_json::to_vec(&next)?),
            )
            .await
        {
            Ok(offset) => offset,
            Err(error) => {
                abort_registry_transaction(transaction).await?;
                return Err(error);
            }
        };
        commit_registry_transaction(transaction).await?;
        self.await_applied(offset).await;
        Ok(next)
    }

    async fn refresh(&self) -> Result<(), ControlError> {
        let bootstrap_addrs = split_bootstrap(&self.bootstrap);
        let mut admin = AdminClient::connect_with_options(
            &bootstrap_addrs,
            registry_admin_options(&self.policy),
        )
        .await?;
        let metadata = admin.metadata(&[TENANT_REGISTRY_TOPIC]).await?;
        let entry = metadata
            .topics
            .into_iter()
            .find(|topic| topic.name == TENANT_REGISTRY_TOPIC)
            .ok_or_else(|| ControlError::TopicMissing(TENANT_REGISTRY_TOPIC.to_string()))?;
        let Some(addr) =
            resolve_bootstrap_addr(&self.bootstrap, self.policy.reader_admin_dns_timeout()).await
        else {
            return Err(ControlError::TopicMissing(
                TENANT_REGISTRY_TOPIC.to_string(),
            ));
        };
        let opts = ConnectionOptions {
            dns_timeout: self.policy.reader_admin_dns_timeout(),
            client_id: "crabka-gres-control-refresh".to_string(),
            dispatch_queue_capacity: self.policy.dispatch_queue_capacity,
            frame_max: self.policy.frame_max,
            ..Default::default()
        };
        let conn = Connection::connect_with_options(addr, opts).await?;
        let topic_id = entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid);
        let mut next_offset = 0_i64;
        loop {
            let result = fetch_partition_with_isolation_progress(
                &conn,
                registry_fetch(next_offset, topic_id, &self.policy),
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
                write_split_operations(&self.split_operations, |state| {
                    apply_raw_split_operation(
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

    /// Establishes the transactional writer lease before every registry
    /// mutation. A successful initialization publishes the coordinator-issued
    /// epoch before the caller can begin and produce a mutation.
    async fn initialize_transactional_writer(&self) -> Result<(), ControlError> {
        self.producer.init_transactions().await?;
        Ok(())
    }

    /// Begins only after initialization. A transaction guard that drops
    /// concurrently can install a recovery marker between initialization and
    /// begin. In that case this function reinitializes once and does not
    /// produce with an uncertain epoch.
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

/// Finishes a registry transaction and keeps the guard while Kafka says the same
/// transaction can be retried. Terminal and transport errors reach the caller
/// unchanged after the producer has moved to its terminal or ready state. The
/// next mutation reinitializes the transactional producer.
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

/// Aborts after a produce failure. A retryable `EndTxn` error keeps its guard.
/// A terminal error stays visible to the caller and does not leave a producer
/// wedged in `InTransaction`.
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

const SPLIT_OPERATION_KEY_PREFIX: &[u8] = b"\0gres-split-operation\0";

fn encode_split_operation_key(
    tenant: &TenantName,
    operation_id: &str,
) -> Result<Vec<u8>, ControlError> {
    if operation_id.is_empty() {
        return Err(ControlError::invalid_field(
            "split_operation.operation_id",
            "must not be empty",
        ));
    }
    let mut key = SPLIT_OPERATION_KEY_PREFIX.to_vec();
    key.extend(serde_json::to_vec(&(tenant.as_str(), operation_id))?);
    Ok(key)
}

fn decode_split_operation_key(bytes: &[u8]) -> Option<(TenantName, String)> {
    let suffix = bytes.strip_prefix(SPLIT_OPERATION_KEY_PREFIX)?;
    let (tenant, operation_id): (String, String) = serde_json::from_slice(suffix).ok()?;
    if operation_id.is_empty() {
        return None;
    }
    Some((TenantName::try_from(tenant.as_str()).ok()?, operation_id))
}

fn apply_raw_split_operation(
    operations: &mut BTreeMap<(String, String), SplitOperationRecord>,
    key_bytes: &[u8],
    value_bytes: Option<&[u8]>,
) {
    let Some((tenant, operation_id)) = decode_split_operation_key(key_bytes) else {
        return;
    };
    let key = split_operation_map_key(&tenant, &operation_id);
    let Some(value_bytes) = value_bytes else {
        operations.remove(&key);
        return;
    };
    let Ok(operation) = serde_json::from_slice::<SplitOperationRecord>(value_bytes) else {
        return;
    };
    if operation.tenant != tenant
        || operation.operation_id != operation_id
        || operation.ensure_valid().is_err()
    {
        return;
    }
    match operations.get(&key) {
        Some(current) if current.revision > operation.revision => {}
        Some(current) if current.revision == operation.revision && current != &operation => {}
        Some(current) if operation.ensure_monotone_extension(current).is_err() => {}
        _ => {
            operations.insert(key, operation);
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
    let source_absent_or_reused = split.source_range_id == split.left.range_id
        || !current
            .ranges
            .iter()
            .any(|range| range.range_id == split.source_range_id);
    source_absent_or_reused
        && current
            .ranges
            .windows(2)
            .any(|pair| pair[0] == split.left && pair[1] == split.right)
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

async fn ensure_registry_topic(
    bootstrap: &str,
    policy: &RegistryPolicy,
) -> Result<WireUuid, ControlError> {
    let entry = ensure_compacted_single_partition_topic(
        bootstrap,
        TENANT_REGISTRY_TOPIC,
        policy.replication_factor,
        policy,
    )
    .await?;
    validate_registry_replication(entry.replication_factor, policy.replication_factor)?;
    Ok(entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid))
}

fn validate_registry_replication(observed: i32, configured: i32) -> Result<(), ControlError> {
    if observed != 0 && observed != configured {
        return Err(ControlError::invalid_field(
            "registry replication factor",
            format!("configured {configured}, but existing topic has {observed}"),
        ));
    }
    Ok(())
}

async fn ensure_compacted_single_partition_topic(
    bootstrap: &str,
    topic: &str,
    replicas: i32,
    policy: &RegistryPolicy,
) -> Result<crabka_client_admin::TopicMetadataEntry, ControlError> {
    let bootstrap_addrs = split_bootstrap(bootstrap);
    let mut admin =
        AdminClient::connect_with_options(&bootstrap_addrs, registry_admin_options(policy)).await?;
    let (spec, timeout) = compacted_topic_request(topic, replicas, policy);
    let outcomes = admin.create_topics(&[spec], timeout).await?;
    if let Some(outcome) = outcomes.into_iter().next() {
        match outcome.error {
            None => {}
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
    metadata
        .topics
        .into_iter()
        .find(|entry| entry.name == topic)
        .ok_or_else(|| ControlError::TopicMissing(topic.to_string()))
}

fn compacted_topic_request(
    topic: &str,
    replicas: i32,
    policy: &RegistryPolicy,
) -> (CreateTopicSpec, Time) {
    (
        CreateTopicSpec {
            name: topic.to_string(),
            partitions: 1,
            replicas,
            configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
        },
        policy.topic_create_timeout,
    )
}

fn registry_fetch(
    fetch_offset: i64,
    topic_id: WireUuid,
    policy: &RegistryPolicy,
) -> IsolatedFetch<'static> {
    IsolatedFetch {
        topic: TENANT_REGISTRY_TOPIC,
        topic_id,
        partition: 0,
        fetch_offset,
        max_wait: policy.fetch_max_wait,
        max: DEFAULT_FETCH_RESPONSE_MAX,
        partition_max: policy.fetch_partition_max,
        fetch_min: policy.reader_fetch_min,
        isolation_level: READ_COMMITTED,
    }
}

#[derive(Clone, Copy)]
enum ReaderFailure {
    ResolveBootstrap,
    Connect,
    Fetch,
}

const fn reader_retry_delay(policy: &RegistryPolicy, _failure: ReaderFailure) -> Time {
    policy.reader_retry_backoff
}

fn spawn_reader(
    bootstrap: String,
    topic_id: WireUuid,
    tenants: Arc<RwLock<BTreeMap<String, TenantRecord>>>,
    split_operations: Arc<RwLock<BTreeMap<(String, String), SplitOperationRecord>>>,
    applied_tx: watch::Sender<i64>,
    policy: RegistryPolicy,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut next_offset = 0_i64;
        loop {
            let Some(addr) =
                resolve_bootstrap_addr(&bootstrap, policy.reader_admin_dns_timeout()).await
            else {
                tracing::error!(%bootstrap, "gres control registry reader: bad bootstrap address");
                tokio::time::sleep(
                    reader_retry_delay(&policy, ReaderFailure::ResolveBootstrap).to_std(),
                )
                .await;
                continue;
            };
            let opts = ConnectionOptions {
                dns_timeout: policy.reader_admin_dns_timeout(),
                client_id: "crabka-gres-control-reader".to_string(),
                dispatch_queue_capacity: policy.dispatch_queue_capacity,
                frame_max: policy.frame_max,
                ..Default::default()
            };
            let conn = match Connection::connect_with_options(addr, opts).await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::warn!(%error, "gres control registry reader: connect failed");
                    tokio::time::sleep(
                        reader_retry_delay(&policy, ReaderFailure::Connect).to_std(),
                    )
                    .await;
                    continue;
                }
            };
            loop {
                match fetch_partition_with_isolation_progress(
                    &conn,
                    registry_fetch(next_offset, topic_id, &policy),
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
                            write_split_operations(&split_operations, |state| {
                                apply_raw_split_operation(
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
                        tokio::time::sleep(
                            reader_retry_delay(&policy, ReaderFailure::Fetch).to_std(),
                        )
                        .await;
                        break;
                    }
                }
            }
        }
    })
}

fn split_bootstrap(bootstrap: &str) -> Vec<String> {
    bootstrap
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

fn registry_admin_options(policy: &RegistryPolicy) -> ConnectionOptions {
    ConnectionOptions {
        dns_timeout: policy.reader_admin_dns_timeout(),
        connect_timeout: crabka_units::secs(5),
        request_timeout: crabka_units::secs(30),
        client_id: "crabka-operator".to_string(),
        dispatch_queue_capacity: policy.dispatch_queue_capacity,
        frame_max: policy.frame_max,
        security: None,
    }
}

async fn resolve_bootstrap_addr(
    bootstrap: &str,
    dns_timeout: ClientDnsTimeout,
) -> Option<SocketAddr> {
    for entry in bootstrap
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Ok(Ok(mut addrs)) =
            tokio::time::timeout(dns_timeout.time().to_std(), tokio::net::lookup_host(entry)).await
        else {
            continue;
        };
        if let Some(addr) = addrs.next() {
            return Some(addr);
        }
    }
    None
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

fn read_split_operations(
    operations: &RwLock<BTreeMap<(String, String), SplitOperationRecord>>,
) -> std::sync::RwLockReadGuard<'_, BTreeMap<(String, String), SplitOperationRecord>> {
    operations
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_split_operations(
    operations: &RwLock<BTreeMap<(String, String), SplitOperationRecord>>,
    apply: impl FnOnce(&mut BTreeMap<(String, String), SplitOperationRecord>),
) {
    let mut guard = operations
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
        RangeBoundary, RangeLifecycle, SplitOperationPhase, SqlUser, TenantId, TenantState,
        decode_tenant_config_record,
    };

    #[test]
    fn registry_policy_defaults_and_validated_scalars_are_exact() {
        let policy = RegistryPolicy::default();

        assert!(policy.replication_factor == 1);
        assert!(policy.topic_create_timeout == crabka_units::secs(15));
        assert!(policy.reader_retry_backoff == crabka_units::millis(250));
        assert!(policy.fetch_max_wait == crabka_units::millis(500));
        assert!(policy.fetch_partition_max == crabka_units::mebibytes(1));
        assert!("1".parse::<RegistryReplicationFactor>().is_ok());
        assert!("32767".parse::<RegistryReplicationFactor>().is_ok());
        assert!("0".parse::<RegistryReplicationFactor>().is_err());
        assert!("32768".parse::<RegistryReplicationFactor>().is_err());
        assert!("0".parse::<PositiveI32>().is_err());
        assert!("2147483648".parse::<PositiveI32>().is_err());
        assert!("0".parse::<PositiveMillis>().is_err());
        assert!(
            RegistryPolicy::new(
                0,
                millis(15_000),
                millis(250),
                millis(500),
                crabka_units::mebibytes(1),
            )
            .is_err()
        );
        assert!(
            RegistryPolicy::new(
                32_768,
                millis(15_000),
                millis(250),
                millis(500),
                crabka_units::mebibytes(1),
            )
            .is_err()
        );
        assert!(
            RegistryPolicy::new(
                1,
                millis(0),
                millis(250),
                millis(500),
                crabka_units::mebibytes(1),
            )
            .is_err()
        );
        assert!(
            RegistryPolicy::new(
                1,
                millis(15_000),
                millis(0),
                millis(500),
                crabka_units::mebibytes(1),
            )
            .is_err()
        );
        assert!(
            RegistryPolicy::new(
                1,
                millis(15_000),
                millis(250),
                millis(0),
                crabka_units::mebibytes(1),
            )
            .is_err()
        );
        assert!(
            RegistryPolicy::new(1, millis(15_000), millis(250), millis(500), bytes(0)).is_err()
        );
        assert!(
            RegistryPolicy::new(
                1,
                millis(15_000),
                millis(250),
                millis(500),
                ByteSize::from_bytes_f64(1.5),
            )
            .is_err()
        );
        assert!(
            RegistryPolicy::new(
                1,
                millis(15_000),
                millis(250),
                millis(500),
                ByteSize::from_bytes_f64(f64::from(i32::MAX) + 1.0),
            )
            .is_err()
        );
    }

    #[test]
    fn registry_policy_dns_timeout_defaults_and_replaces_exactly() {
        let defaults = RegistryPolicy::default();
        assert!(defaults.producer_dns_timeout() == crabka_client_core::ClientDnsTimeout::default());

        let policy = defaults
            .with_producer_dns_timeout(millis(37))
            .expect("valid DNS timeout");
        assert!(policy.producer_dns_timeout().milliseconds() == 37);
        assert!(
            RegistryPolicy::default()
                .with_producer_dns_timeout(millis(0))
                .is_err()
        );
    }

    #[test]
    fn registry_client_resource_policy_defaults_and_replaces_exactly() {
        let dispatch = ConnectionDispatchQueueCapacity::new(7).unwrap();
        let frame_max = ClientFrameMax::try_from(crabka_units::kibibytes(32)).unwrap();
        let fetch_min = FetchMinBytes::try_from(bytes(9)).unwrap();
        let policy =
            RegistryPolicy::default().with_client_resource_policy(dispatch, frame_max, fetch_min);

        assert!(policy.dispatch_queue_capacity == dispatch);
        assert!(policy.frame_max == frame_max);
        assert!(policy.reader_fetch_min == fetch_min);
        let admin = registry_admin_options(&policy);
        assert!(admin.dispatch_queue_capacity == dispatch);
        assert!(admin.frame_max == frame_max);
        assert!(registry_fetch(0, WireUuid::ZERO, &policy).fetch_min == fetch_min);
    }

    #[test]
    fn registry_reader_admin_dns_defaults_and_replaces_exactly() {
        let defaults = RegistryPolicy::default();
        assert!(
            defaults.reader_admin_dns_timeout() == crabka_client_core::ClientDnsTimeout::default()
        );

        let policy = defaults
            .with_reader_admin_dns_timeout(millis(37))
            .expect("valid DNS timeout");
        assert!(policy.reader_admin_dns_timeout().milliseconds() == 37);
        assert!(
            RegistryPolicy::default()
                .with_reader_admin_dns_timeout(millis(0))
                .is_err()
        );
    }

    #[tokio::test]
    async fn registry_bootstrap_resolver_accepts_typed_deadline() {
        let timeout = ClientDnsTimeout::new(millis(37)).expect("positive timeout");
        let addr = resolve_bootstrap_addr("127.0.0.1:9092", timeout)
            .await
            .expect("literal address");
        assert!(addr.port() == 9092);
    }

    #[test]
    fn checkpoint_scalars_enforce_runtime_boundaries() {
        use crate::{CheckpointPartBytes, PositiveUsize};

        assert!("8".parse::<CheckpointPartBytes>().is_ok());
        assert!("7".parse::<CheckpointPartBytes>().is_err());
        assert!("1".parse::<PositiveUsize>().is_ok());
        assert!("0".parse::<PositiveUsize>().is_err());
    }

    #[test]
    fn registry_policy_reaches_topic_and_fetch_requests() {
        let policy =
            RegistryPolicy::new(7, millis(12_345), millis(678), millis(901), bytes(234_567))
                .unwrap();

        let (registry_spec, timeout) =
            compacted_topic_request(TENANT_REGISTRY_TOPIC, policy.replication_factor(), &policy);
        let (tenant_spec, tenant_timeout) = compacted_topic_request("tenant-config", 3, &policy);
        assert!(registry_spec.replicas == 7);
        assert!(tenant_spec.replicas == 3);
        assert!(timeout == crabka_units::millis(12_345));
        assert!(tenant_timeout == crabka_units::millis(12_345));
        let fetch = registry_fetch(42, WireUuid::ZERO, &policy);
        assert!(fetch.max_wait == crabka_units::millis(901));
        assert!(fetch.partition_max == crabka_units::bytes(234_567));
        assert!(policy.reader_retry_backoff == crabka_units::millis(678));
    }

    #[test]
    fn registry_policy_reaches_every_reader_failure_backoff() {
        let policy = RegistryPolicy::new(
            1,
            millis(15_000),
            millis(678),
            millis(500),
            crabka_units::mebibytes(1),
        )
        .unwrap();

        for failure in [
            ReaderFailure::ResolveBootstrap,
            ReaderFailure::Connect,
            ReaderFailure::Fetch,
        ] {
            assert!(reader_retry_delay(&policy, failure) == crabka_units::millis(678));
        }
    }

    #[test]
    fn registry_topic_rejects_immutable_replication_mismatch() {
        assert!(validate_registry_replication(0, 2).is_ok());
        assert!(validate_registry_replication(2, 2).is_ok());
        let error = validate_registry_replication(1, 2)
            .expect_err("immutable registry replication mismatch");
        assert!(matches!(
            error,
            ControlError::InvalidField {
                field: "registry replication factor",
                ..
            }
        ));
    }

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
                lifecycle: RangeLifecycle::default(),
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
                lifecycle: RangeLifecycle::default(),
                retirement: None,
            },
            right: crate::record::RangeLayoutEntry {
                range_id: 2,
                end_key: None,
                endpoint: "tenant-a-r2.gres.svc:7432".to_string(),
                wal_generation: 8,
                lifecycle: RangeLifecycle::default(),
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
            lifecycle: RangeLifecycle::default(),
            retirement: None,
        }];
        let mut newer_version = record("tenant-a", 5, TenantState::Active);
        newer_version.ranges = vec![crate::record::RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "tenant-a-r1-new.gres.svc:7432".to_string(),
            wal_generation: 3,
            lifecycle: RangeLifecycle::default(),
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
        let policy =
            RegistryPolicy::new(1, millis(12_345), millis(678), millis(901), bytes(234_567))
                .unwrap();
        let mut registry = Registry::connect_with_policy(&bootstrap, policy.clone())
            .await
            .expect("registry connect");
        assert!(registry.policy() == &policy);
        registry.ensure_topic().await.expect("registry topic");

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_the_registry_aborts_its_reader_task() {
        let directory = TempDir::new().expect("broker tempdir");
        let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        let mut registry = Registry::connect(&bootstrap)
            .await
            .expect("registry connect");
        registry.ensure_topic().await.expect("registry topic");

        let reader = registry
            .reader
            .as_ref()
            .expect("ensure_topic starts the reader")
            .abort_handle();
        assert!(!reader.is_finished());

        drop(registry);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !reader.is_finished() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "reader task still alive 5s after the registry was dropped"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        broker.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kafka_split_operation_journal_survives_reconnect_and_retries_idempotently() {
        let directory = TempDir::new().expect("broker tempdir");
        let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        let operation = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-1",
            split_layout(RangeBoundary::table_start(100)),
        )
        .expect("operation");

        let mut first = Registry::connect(&bootstrap)
            .await
            .expect("registry connect");
        first.ensure_topic().await.expect("registry topic");
        first
            .begin_split_operation(&operation)
            .await
            .expect("begin operation");
        first
            .begin_split_operation(&operation)
            .await
            .expect("idempotent begin");
        drop(first);

        let mut reopened = Registry::connect(&bootstrap)
            .await
            .expect("reconnect registry");
        reopened
            .ensure_topic()
            .await
            .expect("existing registry topic");
        let loaded = reopened
            .load_split_operation("tenant-a", "split-1")
            .await
            .expect("load operation")
            .expect("operation survives reconnect");
        assert!(loaded == operation);
        let running = loaded
            .advance(SplitOperationPhase::Running, 1, None)
            .expect("running");
        reopened
            .compare_and_swap_split_operation(Some(0), &running)
            .await
            .expect("update");
        reopened
            .compare_and_swap_split_operation(Some(0), &running)
            .await
            .expect("idempotent crash retry");
        assert!(reopened.list_split_operations("tenant-a").await.unwrap() == vec![running]);

        let conflicting = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-1",
            split_layout(RangeBoundary::table_start(200)),
        )
        .expect("conflicting operation");
        assert!(matches!(
            reopened.begin_split_operation(&conflicting).await,
            Err(ControlError::SplitOperationConflict { .. })
        ));
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
    fn split_retry_rejects_conflicting_generation_or_lineage() {
        let mut store = InMemoryRegistryStore::new();
        let name = tenant_name("tenant-a");
        store.upsert(ranged_record("tenant-a", 4)).unwrap();
        let split = split_layout(RangeBoundary::table_start(100));
        store
            .split_range_layout_if_version(&name, 4, split.clone())
            .unwrap();
        let mut conflicting = split;
        conflicting.right.wal_generation += 1;

        let error = store
            .split_range_layout_if_version(&name, 4, conflicting)
            .expect_err("conflicting retry must not be accepted as applied");

        assert!(matches!(
            error,
            ControlError::RegistryVersionConflict { .. }
        ));
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
    fn split_operation_begin_is_idempotent_and_rejects_conflicting_intent() {
        let mut store = InMemoryRegistryStore::new();
        let operation = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-1",
            split_layout(RangeBoundary::table_start(100)),
        )
        .expect("valid operation");

        let first = store
            .begin_split_operation(operation.clone())
            .expect("begin operation");
        let retry = store
            .begin_split_operation(operation.clone())
            .expect("idempotent retry");
        assert!(first == retry);
        assert!(store.load_split_operation(&tenant_name("tenant-a"), "split-1") == Some(operation));

        let conflicting = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-1",
            split_layout(RangeBoundary::table_start(200)),
        )
        .expect("valid conflicting operation");
        assert!(matches!(
            store.begin_split_operation(conflicting),
            Err(ControlError::SplitOperationConflict { .. })
        ));
    }

    #[test]
    fn tenant_active_mutation_slot_rejects_preemption_until_terminal() {
        let mut store = InMemoryRegistryStore::new();
        let first = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-z",
            split_layout(RangeBoundary::table_start(100)),
        )
        .unwrap();
        let earlier = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-a",
            split_layout(RangeBoundary::table_start(200)),
        )
        .unwrap();
        store.begin_split_operation(first.clone()).unwrap();
        assert!(matches!(
            store.begin_split_operation(earlier.clone()),
            Err(ControlError::SplitOperationConflict { .. })
        ));
        let running = first
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        store
            .compare_and_swap_split_operation(Some(0), running.clone())
            .unwrap();
        let completed = running
            .advance(SplitOperationPhase::Completed, 1, None)
            .unwrap();
        store
            .compare_and_swap_split_operation(Some(1), completed)
            .unwrap();
        assert_eq!(
            store.begin_split_operation(earlier.clone()).unwrap(),
            earlier
        );
    }

    #[test]
    fn split_operation_cas_enforces_monotone_progress_and_crash_retry() {
        let mut store = InMemoryRegistryStore::new();
        let operation = store
            .begin_split_operation(
                SplitOperationRecord::new(
                    tenant_name("tenant-a"),
                    "split-1",
                    split_layout(RangeBoundary::table_start(100)),
                )
                .expect("valid operation"),
            )
            .expect("begin operation");
        let running = operation
            .advance(SplitOperationPhase::Running, 1, None)
            .expect("running update");
        let running = store
            .compare_and_swap_split_operation(Some(0), running.clone())
            .expect("first attempt");
        let retry = store
            .compare_and_swap_split_operation(Some(0), running)
            .expect("crash retry is idempotent");
        assert!(retry.revision == 1);

        let failed = retry
            .advance(
                SplitOperationPhase::Failed,
                1,
                Some("compute died after checkpoint".to_string()),
            )
            .expect("record failure");
        let failed = store
            .compare_and_swap_split_operation(Some(1), failed)
            .expect("persist failure");
        let mut completed = failed
            .advance(SplitOperationPhase::Running, 2, None)
            .expect("restart failed attempt");
        completed = store
            .compare_and_swap_split_operation(Some(2), completed)
            .expect("persist retry");
        for phase in [
            SplitOperationPhase::Checkpointed,
            SplitOperationPhase::Paused,
            SplitOperationPhase::Restored,
            SplitOperationPhase::Activated,
            SplitOperationPhase::LayoutPublished,
            SplitOperationPhase::Retiring,
            SplitOperationPhase::Resuming,
            SplitOperationPhase::Completed,
        ] {
            let prior_revision = completed.revision;
            completed = completed.advance(phase, 2, None).expect("advance retry");
            completed = store
                .compare_and_swap_split_operation(Some(prior_revision), completed)
                .expect("persist retry phase");
        }

        let completed_revision = completed.revision;
        let regression = SplitOperationRecord {
            revision: completed_revision + 1,
            phase: SplitOperationPhase::Running,
            ..completed
        };
        assert!(matches!(
            store.compare_and_swap_split_operation(Some(completed_revision), regression),
            Err(ControlError::SplitOperationConflict { .. })
        ));
    }

    #[test]
    fn split_operation_list_is_tenant_scoped_and_ordered() {
        let mut store = InMemoryRegistryStore::new();
        for (tenant, operation_id) in [
            ("tenant-a", "split-2"),
            ("tenant-b", "split-0"),
            ("tenant-a", "split-1"),
        ] {
            let begun = store
                .begin_split_operation(
                    SplitOperationRecord::new(
                        tenant_name(tenant),
                        operation_id,
                        split_layout(RangeBoundary::table_start(100)),
                    )
                    .expect("valid operation"),
                )
                .expect("begin operation");
            if tenant == "tenant-a" && operation_id == "split-2" {
                let running = begun
                    .advance(SplitOperationPhase::Running, 1, None)
                    .unwrap();
                store
                    .compare_and_swap_split_operation(Some(0), running.clone())
                    .unwrap();
                let completed = running
                    .advance(SplitOperationPhase::Completed, 1, None)
                    .unwrap();
                store
                    .compare_and_swap_split_operation(Some(1), completed)
                    .unwrap();
            }
        }

        let operations = store.list_split_operations(&tenant_name("tenant-a"));
        assert!(
            operations
                .iter()
                .map(|operation| operation.operation_id.as_str())
                .collect::<Vec<_>>()
                == ["split-1", "split-2"]
        );
    }

    #[test]
    fn split_operation_record_rejects_impossible_attempt_and_error_shapes() {
        let mut invalid_split = split_layout(RangeBoundary::table_start(100));
        invalid_split.right.range_id = invalid_split.left.range_id;
        assert!(
            SplitOperationRecord::new(tenant_name("tenant-a"), "invalid-split", invalid_split)
                .is_err()
        );
        let operation = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-1",
            split_layout(RangeBoundary::table_start(100)),
        )
        .expect("operation");

        assert!(
            operation
                .advance(SplitOperationPhase::Running, 0, None)
                .is_err()
        );
        assert!(
            operation
                .advance(SplitOperationPhase::Failed, 1, None)
                .is_err()
        );
        let mut malformed = operation;
        malformed
            .errors
            .push("failure before any attempt".to_string());
        assert!(malformed.ensure_valid().is_err());

        let mut completed = SplitOperationRecord::new(
            tenant_name("tenant-a"),
            "split-2",
            split_layout(RangeBoundary::table_start(100)),
        )
        .unwrap();
        for phase in [
            SplitOperationPhase::Running,
            SplitOperationPhase::Checkpointed,
            SplitOperationPhase::Paused,
            SplitOperationPhase::Restored,
            SplitOperationPhase::Activated,
            SplitOperationPhase::LayoutPublished,
            SplitOperationPhase::Retiring,
            SplitOperationPhase::Resuming,
            SplitOperationPhase::Completed,
        ] {
            completed = completed.advance(phase, 1, None).unwrap();
        }
        assert!(
            completed
                .advance(SplitOperationPhase::Completed, 2, None)
                .is_err()
        );
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
