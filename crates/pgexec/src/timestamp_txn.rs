//! Timestamp-transaction primitives for the G-9 sharded-table path.

use std::{num::NonZeroU64, sync::Mutex};

use thiserror::Error;

use crate::{ExecError, commit::Committer};

/// Errors raised when constructing or ordering timestamp-transaction values.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum TimestampTxnError {
    /// Timestamp zero is reserved so missing timestamps cannot masquerade as real
    /// transaction times.
    #[error("timestamp transaction values must be greater than zero")]
    ZeroTimestamp,
    /// A commit timestamp must sort strictly after the transaction's start
    /// timestamp.
    #[error("commit timestamp {commit_ts} must be greater than start timestamp {start_ts}")]
    CommitNotAfterStart { start_ts: u64, commit_ts: u64 },
    /// The allocator reached `u64::MAX` and cannot hand out another non-zero
    /// timestamp.
    #[error("timestamp transaction allocator exhausted the u64 timestamp space")]
    TimestampExhausted,
    /// A prewrite encountered another unresolved intent or a newer committed
    /// version for the same row.
    #[error("timestamp transaction write conflict on table {table_id} row {rowid}")]
    WriteConflict { table_id: u32, rowid: u64 },
    /// The requested transaction has no intent for the row being resolved.
    #[error("timestamp transaction intent {start_ts} is missing on table {table_id} row {rowid}")]
    MissingIntent {
        table_id: u32,
        rowid: u64,
        start_ts: u64,
    },
    /// A participant request does not match the durable range-0 descriptor.
    #[error("timestamp transaction primary identity is fenced")]
    IdentityFenced,
    /// The primary descriptor is already terminal and cannot accept a prewrite.
    #[error("timestamp transaction primary is already terminal")]
    PrimaryAlreadyDecided,
    /// A prepared participant must have at least one durable physical operation.
    #[error("timestamp participant acknowledgement requires durable operations")]
    EmptyOperations,
}

/// Errors raised by a timestamp oracle implementation.
#[derive(Debug, Error)]
pub enum TimestampOracleError {
    /// The oracle returned a timestamp that cannot satisfy the transaction
    /// timestamp invariants.
    #[error(transparent)]
    Timestamp(#[from] TimestampTxnError),
    /// The oracle is unavailable or explicitly fenced.
    #[error("timestamp oracle unavailable: {0}")]
    Unavailable(String),
}

/// Narrow timestamp grant seam used by sharded timestamp DML.
#[async_trait::async_trait]
pub trait TimestampOracle: Send + Sync {
    /// Allocate the single read timestamp used by one SQL statement.
    async fn allocate_read_timestamp(&self) -> Result<ReadTimestamp, TimestampOracleError>;

    /// Allocate a start timestamp for a timestamp transaction.
    async fn allocate_transaction_id(&self)
    -> Result<TimestampTransactionId, TimestampOracleError>;

    /// Allocate a commit timestamp strictly after `start_ts`.
    async fn allocate_commit_after(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<CommitTimestamp, TimestampOracleError>;

    /// Allocate a read timestamp strictly after durable timestamp state already
    /// present on this range.
    async fn allocate_read_timestamp_after(
        &self,
        durable_horizon: u64,
    ) -> Result<ReadTimestamp, TimestampOracleError> {
        let timestamp = self.allocate_read_timestamp().await?;
        if timestamp.get() > durable_horizon {
            return Ok(timestamp);
        }
        Err(TimestampOracleError::Unavailable(
            "timestamp oracle granted a read before the durable timestamp horizon".into(),
        ))
    }

    /// Allocate a transaction id strictly after durable timestamp state already
    /// present on this range.
    async fn allocate_transaction_id_after(
        &self,
        durable_horizon: u64,
    ) -> Result<TimestampTransactionId, TimestampOracleError> {
        let timestamp = self.allocate_transaction_id().await?;
        if timestamp.get() > durable_horizon {
            return Ok(timestamp);
        }
        Err(TimestampOracleError::Unavailable(
            "timestamp oracle granted a transaction before the durable timestamp horizon".into(),
        ))
    }

    /// Allocate a commit timestamp strictly after both the transaction and every
    /// durable timestamp already present on this range.
    async fn allocate_commit_after_durable(
        &self,
        start_ts: TimestampTransactionId,
        durable_horizon: u64,
    ) -> Result<CommitTimestamp, TimestampOracleError> {
        let timestamp = self.allocate_commit_after(start_ts).await?;
        if timestamp.get() > durable_horizon {
            return Ok(timestamp);
        }
        Err(TimestampOracleError::Unavailable(
            "timestamp oracle granted a commit before the durable timestamp horizon".into(),
        ))
    }
}

/// Default in-process timestamp oracle preserving single-engine behavior.
#[derive(Debug, Default)]
pub struct LocalTimestampOracle {
    allocator: Mutex<MonotonicTimestampAllocator>,
}

impl LocalTimestampOracle {
    /// Build a local oracle from an existing monotonic allocator.
    #[must_use]
    pub fn new(allocator: MonotonicTimestampAllocator) -> Self {
        Self {
            allocator: Mutex::new(allocator),
        }
    }
}

#[async_trait::async_trait]
impl TimestampOracle for LocalTimestampOracle {
    async fn allocate_read_timestamp(&self) -> Result<ReadTimestamp, TimestampOracleError> {
        let mut allocator = self
            .allocator
            .lock()
            .map_err(|_| TimestampOracleError::Unavailable("local oracle lock poisoned".into()))?;
        allocator
            .allocate_read_timestamp()
            .map_err(TimestampOracleError::from)
    }

    async fn allocate_transaction_id(
        &self,
    ) -> Result<TimestampTransactionId, TimestampOracleError> {
        let mut allocator = self
            .allocator
            .lock()
            .map_err(|_| TimestampOracleError::Unavailable("local oracle lock poisoned".into()))?;
        allocator
            .allocate_transaction_id()
            .map_err(TimestampOracleError::from)
    }

    async fn allocate_commit_after(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<CommitTimestamp, TimestampOracleError> {
        let mut allocator = self
            .allocator
            .lock()
            .map_err(|_| TimestampOracleError::Unavailable("local oracle lock poisoned".into()))?;
        allocator
            .allocate_commit_after(start_ts)
            .map_err(TimestampOracleError::from)
    }

    async fn allocate_read_timestamp_after(
        &self,
        durable_horizon: u64,
    ) -> Result<ReadTimestamp, TimestampOracleError> {
        let mut allocator = self
            .allocator
            .lock()
            .map_err(|_| TimestampOracleError::Unavailable("local oracle lock poisoned".into()))?;
        allocator.advance_past(durable_horizon)?;
        allocator
            .allocate_read_timestamp()
            .map_err(TimestampOracleError::from)
    }

    async fn allocate_transaction_id_after(
        &self,
        durable_horizon: u64,
    ) -> Result<TimestampTransactionId, TimestampOracleError> {
        let mut allocator = self
            .allocator
            .lock()
            .map_err(|_| TimestampOracleError::Unavailable("local oracle lock poisoned".into()))?;
        allocator.advance_past(durable_horizon)?;
        allocator
            .allocate_transaction_id()
            .map_err(TimestampOracleError::from)
    }

    async fn allocate_commit_after_durable(
        &self,
        start_ts: TimestampTransactionId,
        durable_horizon: u64,
    ) -> Result<CommitTimestamp, TimestampOracleError> {
        let mut allocator = self
            .allocator
            .lock()
            .map_err(|_| TimestampOracleError::Unavailable("local oracle lock poisoned".into()))?;
        allocator.advance_past(durable_horizon.max(start_ts.get()))?;
        allocator
            .allocate_commit_after(start_ts)
            .map_err(TimestampOracleError::from)
    }
}

/// The start timestamp that names one timestamp transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampTransactionId(NonZeroU64);

impl TimestampTransactionId {
    /// Parse a durable/wire timestamp into the typed transaction id space.
    pub fn new(raw: u64) -> Result<Self, TimestampTxnError> {
        let Some(value) = NonZeroU64::new(raw) else {
            return Err(TimestampTxnError::ZeroTimestamp);
        };
        Ok(Self(value))
    }

    /// Return the raw durable/wire timestamp value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for TimestampTransactionId {
    type Error = TimestampTxnError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TimestampTransactionId> for u64 {
    fn from(value: TimestampTransactionId) -> Self {
        value.get()
    }
}

/// A timestamp chosen as the atomic commit order of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitTimestamp(NonZeroU64);

impl CommitTimestamp {
    /// Parse a durable/wire timestamp into the typed commit timestamp space.
    pub fn new(raw: u64) -> Result<Self, TimestampTxnError> {
        let Some(value) = NonZeroU64::new(raw) else {
            return Err(TimestampTxnError::ZeroTimestamp);
        };
        Ok(Self(value))
    }

    /// Parse and prove that `raw` can commit the transaction named by `start_ts`.
    pub fn after_start(
        start_ts: TimestampTransactionId,
        raw: u64,
    ) -> Result<Self, TimestampTxnError> {
        let commit_ts = Self::new(raw)?;
        if commit_ts.get() <= start_ts.get() {
            return Err(TimestampTxnError::CommitNotAfterStart {
                start_ts: start_ts.get(),
                commit_ts: commit_ts.get(),
            });
        }
        Ok(commit_ts)
    }

    /// Return the raw durable/wire timestamp value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for CommitTimestamp {
    type Error = TimestampTxnError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CommitTimestamp> for u64 {
    fn from(value: CommitTimestamp) -> Self {
        value.get()
    }
}

/// The timestamp a read uses to decide whether committed versions are visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadTimestamp(NonZeroU64);

impl ReadTimestamp {
    /// A read timestamp after every finite commit timestamp.
    pub const MAX: Self = Self(NonZeroU64::MAX);

    /// Parse a durable/wire timestamp into the typed read timestamp space.
    pub fn new(raw: u64) -> Result<Self, TimestampTxnError> {
        let Some(value) = NonZeroU64::new(raw) else {
            return Err(TimestampTxnError::ZeroTimestamp);
        };
        Ok(Self(value))
    }

    /// Return the raw durable/wire timestamp value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for ReadTimestamp {
    type Error = TimestampTxnError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ReadTimestamp> for u64 {
    fn from(value: ReadTimestamp) -> Self {
        value.get()
    }
}

/// Monotone local timestamp allocator used by pgexec tests and future TSO client
/// adapters. It does not pretend to be a distributed oracle; callers must persist
/// and fence distributed grants outside this primitive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonotonicTimestampAllocator {
    next: NonZeroU64,
}

impl Default for MonotonicTimestampAllocator {
    fn default() -> Self {
        Self::starting_at(1).expect("one is a valid timestamp")
    }
}

impl MonotonicTimestampAllocator {
    /// Create an allocator whose next grant is `first`.
    pub fn starting_at(first: u64) -> Result<Self, TimestampTxnError> {
        let Some(next) = NonZeroU64::new(first) else {
            return Err(TimestampTxnError::ZeroTimestamp);
        };
        Ok(Self { next })
    }

    /// Return the next raw timestamp without allocating it.
    #[must_use]
    pub fn next_timestamp(&self) -> u64 {
        self.next.get()
    }

    /// Allocate a start timestamp for a timestamp transaction.
    pub fn allocate_transaction_id(&mut self) -> Result<TimestampTransactionId, TimestampTxnError> {
        let timestamp = self.allocate_raw()?;
        TimestampTransactionId::new(timestamp)
    }

    /// Allocate a read timestamp.
    pub fn allocate_read_timestamp(&mut self) -> Result<ReadTimestamp, TimestampTxnError> {
        let timestamp = self.allocate_raw()?;
        ReadTimestamp::new(timestamp)
    }

    /// Allocate a commit timestamp that is strictly greater than `start_ts`.
    pub fn allocate_commit_after(
        &mut self,
        start_ts: TimestampTransactionId,
    ) -> Result<CommitTimestamp, TimestampTxnError> {
        let required = start_ts
            .get()
            .checked_add(1)
            .ok_or(TimestampTxnError::TimestampExhausted)?;
        if self.next.get() < required {
            self.next = NonZeroU64::new(required).expect("required is greater than zero");
        }
        let timestamp = self.allocate_raw()?;
        CommitTimestamp::after_start(start_ts, timestamp)
    }

    /// Advance the next grant beyond a durable timestamp without regressing an
    /// allocator that has already issued a newer grant.
    pub fn advance_past(&mut self, durable_horizon: u64) -> Result<(), TimestampTxnError> {
        if self.next.get() > durable_horizon {
            return Ok(());
        }
        let next = durable_horizon
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(TimestampTxnError::TimestampExhausted)?;
        self.next = next;
        Ok(())
    }

    fn allocate_raw(&mut self) -> Result<u64, TimestampTxnError> {
        let timestamp = self.next.get();
        let next = timestamp
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(TimestampTxnError::TimestampExhausted)?;
        self.next = next;
        Ok(timestamp)
    }
}

/// The terminal or non-terminal state associated with a timestamp transaction's
/// written version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampTxnDecision {
    /// The transaction wrote an intent but has no terminal primary decision yet.
    Pending,
    /// The transaction was aborted and its intent is never visible.
    Aborted,
    /// The transaction committed at this timestamp.
    Committed(CommitTimestamp),
    /// The transaction deleted the row at this timestamp.
    Deleted(CommitTimestamp),
}

/// Visibility metadata for a row/index version written by a timestamp
/// transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampVersionState {
    start_ts: TimestampTransactionId,
    decision: TimestampTxnDecision,
}

/// One row mutation staged by a timestamp transaction prewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampWrite {
    /// Catalog table id.
    pub table_id: u32,
    /// MVCC row id.
    pub rowid: u64,
    /// New row payload.
    pub row: Vec<crabka_pgtypes::Datum>,
    /// Whether this write is a delete tombstone rather than an inserted/updated row.
    pub delete: bool,
    /// Global-index entries that must be maintained in the same timestamp txn.
    pub global_index_intents: Vec<GlobalIndexIntent>,
}

/// One global-secondary-index maintenance intent coupled to a base row write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalIndexIntent {
    /// Catalog index id.
    pub index_id: u32,
    /// Indexed column values in index-definition order.
    pub indexed_values: Vec<crabka_pgtypes::Datum>,
    /// Base table id this index entry points back to.
    pub base_table_id: u32,
    /// Base MVCC row id this index entry points back to.
    pub base_rowid: u64,
    /// Whether the catalog declares this as a unique index.
    pub unique: bool,
    /// Whether this intent removes this index entry instead of adding it.
    pub delete: bool,
}

/// One visible global secondary-index pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VisibleGlobalIndexEntry {
    /// Base table id this index entry points back to.
    pub base_table_id: u32,
    /// Base MVCC row id this index entry points back to.
    pub base_rowid: u64,
}

/// The primary range's durable timestamp-transaction decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimaryTxnDecision {
    /// Primary record has not been written yet.
    Pending,
    /// Primary record chose abort.
    Aborted,
    /// Primary record chose commit at this timestamp.
    Committed(CommitTimestamp),
}

/// Durable range-0 record for a distributed timestamp transaction.  The record
/// is deliberately separate from participant intents: range 0 is the only
/// authority that can make the transaction logically visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampTxnDescriptor {
    /// TSO start timestamp naming this transaction.
    pub start_ts: TimestampTransactionId,
    /// Globally unique xid allocated by range 0 for this attempt.
    pub global_xid: u64,
    /// Monotone descriptor generation. Range 0 serializes transitions and rejects
    /// attempts made from an older observed generation.
    pub generation: u64,
    /// Complete, sorted participant set captured before any prewrite.
    pub participants: Vec<u32>,
    /// Participants that have durably acknowledged prewrite.
    pub prepared: Vec<u32>,
    /// Physical operations durably acknowledged by participants. Recovery uses these
    /// to distinguish committed delete tombstones from ordinary puts.
    pub operations: Vec<TimestampTxnOperation>,
    /// Range-0 terminal decision.
    pub decision: PrimaryTxnDecision,
}

/// One participant-local row intent that a durable descriptor must physically settle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimestampTxnOperation {
    /// Range that owns the local intent.
    pub range_id: u32,
    /// Catalog table containing the row intent.
    pub table_id: u32,
    /// MVCC row identifier.
    pub rowid: u64,
    /// Whether commit resolves the intent to a delete tombstone.
    pub delete: bool,
}

/// The immutable primary identity attached to every distributed participant
/// intent.  `global_xid` fences an old coordinator; `primary_range` tells a
/// remote resolver where the authoritative decision lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampTxnIdentity {
    /// The transaction start timestamp.
    pub start_ts: TimestampTransactionId,
    /// Write-once range-0 global xid.
    pub global_xid: u64,
    /// Range holding the primary descriptor (currently range 0).
    pub primary_range: u32,
}

impl TimestampTxnDescriptor {
    /// Create a begun descriptor.  The caller must provide a canonical,
    /// duplicate-free participant set.
    #[must_use]
    pub fn begun(
        start_ts: TimestampTransactionId,
        global_xid: u64,
        participants: Vec<u32>,
    ) -> Self {
        Self {
            start_ts,
            global_xid,
            generation: 0,
            participants,
            prepared: Vec::new(),
            operations: Vec::new(),
            decision: PrimaryTxnDecision::Pending,
        }
    }

    /// Record a participant acknowledgement and the exact local operations it prewrote.
    pub fn acknowledge_operations(
        &mut self,
        participant: u32,
        operations: &[TimestampTxnOperation],
    ) -> Result<(), TimestampTxnError> {
        if operations.is_empty() {
            return Err(TimestampTxnError::EmptyOperations);
        }
        if self.decision != PrimaryTxnDecision::Pending {
            return Err(TimestampTxnError::MissingIntent {
                table_id: participant,
                rowid: 0,
                start_ts: self.start_ts.get(),
            });
        }
        if !self.participants.contains(&participant) {
            return Err(TimestampTxnError::MissingIntent {
                table_id: participant,
                rowid: 0,
                start_ts: self.start_ts.get(),
            });
        }
        if !self.prepared.contains(&participant) {
            self.prepared.push(participant);
            self.prepared.sort_unstable();
            self.generation = self
                .generation
                .checked_add(1)
                .ok_or(TimestampTxnError::TimestampExhausted)?;
        }
        for operation in operations {
            if operation.range_id != participant {
                return Err(TimestampTxnError::MissingIntent {
                    table_id: operation.table_id,
                    rowid: operation.rowid,
                    start_ts: self.start_ts.get(),
                });
            }
            if !self.operations.contains(operation) {
                self.operations.push(*operation);
                self.operations.sort_unstable();
                self.generation = self
                    .generation
                    .checked_add(1)
                    .ok_or(TimestampTxnError::TimestampExhausted)?;
            }
        }
        Ok(())
    }

    /// Advance the descriptor to a terminal decision exactly once.
    pub fn decide(&mut self, decision: PrimaryTxnDecision) -> Result<(), TimestampTxnError> {
        if self.decision != PrimaryTxnDecision::Pending {
            if self.decision == decision {
                return Ok(());
            }
            return Err(TimestampTxnError::MissingIntent {
                table_id: 0,
                rowid: 0,
                start_ts: self.start_ts.get(),
            });
        }
        if let PrimaryTxnDecision::Committed(commit_ts) = decision
            && commit_ts.get() <= self.start_ts.get()
        {
            return Err(TimestampTxnError::CommitNotAfterStart {
                start_ts: self.start_ts.get(),
                commit_ts: commit_ts.get(),
            });
        }
        self.decision = decision;
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(TimestampTxnError::TimestampExhausted)?;
        Ok(())
    }

    /// Return whether every participant has durably prewritten its intents.
    #[must_use]
    pub fn all_prepared(&self) -> bool {
        self.participants
            .iter()
            .all(|range| self.prepared.contains(range))
    }
}

/// Return the range-0 descriptor key for a timestamp transaction.
#[must_use]
pub fn timestamp_txn_descriptor_key(start_ts: TimestampTransactionId) -> Vec<u8> {
    let mut key = b"\0\0\0\0meta/ts_txn/".to_vec();
    key.extend_from_slice(&start_ts.get().to_be_bytes());
    key
}

/// Encode a descriptor for one atomic range-0 commit batch.
#[must_use]
pub fn timestamp_txn_descriptor_op(descriptor: &TimestampTxnDescriptor) -> crabka_pgkv::WriteOp {
    crabka_pgkv::WriteOp::Put {
        key: timestamp_txn_descriptor_key(descriptor.start_ts),
        value: encode_timestamp_txn_descriptor(descriptor),
    }
}

/// Encode a descriptor as an atomic conditional state-machine transition.
#[must_use]
pub fn timestamp_txn_descriptor_cas_op(
    descriptor: &TimestampTxnDescriptor,
    expected: Option<&TimestampTxnDescriptor>,
) -> crabka_pgkv::WriteOp {
    crabka_pgkv::WriteOp::ConditionalPut {
        key: timestamp_txn_descriptor_key(descriptor.start_ts),
        expected: expected.map(encode_timestamp_txn_descriptor),
        value: encode_timestamp_txn_descriptor(descriptor),
    }
}

fn encode_timestamp_txn_descriptor(descriptor: &TimestampTxnDescriptor) -> Vec<u8> {
    let mut value = Vec::with_capacity(
        37 + (descriptor.participants.len() + descriptor.prepared.len()) * 4
            + descriptor.operations.len() * 17,
    );
    value.extend_from_slice(&descriptor.global_xid.to_be_bytes());
    value.extend_from_slice(&descriptor.generation.to_be_bytes());
    value.extend_from_slice(
        &(u32::try_from(descriptor.participants.len()).expect("participant count fits u32"))
            .to_be_bytes(),
    );
    for range in &descriptor.participants {
        value.extend_from_slice(&range.to_be_bytes());
    }
    value.extend_from_slice(
        &(u32::try_from(descriptor.prepared.len()).expect("prepared count fits u32")).to_be_bytes(),
    );
    for range in &descriptor.prepared {
        value.extend_from_slice(&range.to_be_bytes());
    }
    value.extend_from_slice(
        &(u32::try_from(descriptor.operations.len()).expect("operation count fits u32"))
            .to_be_bytes(),
    );
    for operation in &descriptor.operations {
        value.extend_from_slice(&operation.range_id.to_be_bytes());
        value.extend_from_slice(&operation.table_id.to_be_bytes());
        value.extend_from_slice(&operation.rowid.to_be_bytes());
        value.push(u8::from(operation.delete));
    }
    value.extend_from_slice(&descriptor.decision.encode());
    value
}

/// Read a range-0 timestamp descriptor, if the transaction was distributed.
pub fn read_timestamp_txn_descriptor(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
) -> Result<Option<TimestampTxnDescriptor>, crabka_pgkv::KvError> {
    let Some(value) = kv.get(&timestamp_txn_descriptor_key(start_ts))? else {
        return Ok(None);
    };
    let Some((global_xid, rest)) = take_u64(&value) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction descriptor".into(),
        ));
    };
    let Some((generation, rest)) = take_u64(rest) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction descriptor generation".into(),
        ));
    };
    let Some((participant_count, rest)) = take_u32(rest) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction participants".into(),
        ));
    };
    let participant_bytes = usize::try_from(participant_count)
        .expect("u32 fits usize")
        .checked_mul(4)
        .ok_or_else(|| {
            crabka_pgkv::KvError::CorruptRow("timestamp transaction participant overflow".into())
        })?;
    let Some((participant_bytes, rest)) = rest.split_at_checked(participant_bytes) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction participants".into(),
        ));
    };
    let participants = participant_bytes
        .chunks_exact(4)
        .map(|raw| u32::from_be_bytes(raw.try_into().expect("4 bytes")))
        .collect();
    let Some((prepared_count, rest)) = take_u32(rest) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction prepared set".into(),
        ));
    };
    let prepared_bytes = usize::try_from(prepared_count)
        .expect("u32 fits usize")
        .checked_mul(4)
        .ok_or_else(|| {
            crabka_pgkv::KvError::CorruptRow("timestamp transaction prepared overflow".into())
        })?;
    let Some((prepared_bytes, rest)) = rest.split_at_checked(prepared_bytes) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction prepared set".into(),
        ));
    };
    let prepared = prepared_bytes
        .chunks_exact(4)
        .map(|raw| u32::from_be_bytes(raw.try_into().expect("4 bytes")))
        .collect();
    let Some((operation_count, rest)) = take_u32(rest) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction operation count".into(),
        ));
    };
    let operation_bytes = usize::try_from(operation_count)
        .expect("u32 fits usize")
        .checked_mul(17)
        .ok_or_else(|| {
            crabka_pgkv::KvError::CorruptRow("timestamp transaction operation overflow".into())
        })?;
    let Some((operation_bytes, decision)) = rest.split_at_checked(operation_bytes) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp transaction operations".into(),
        ));
    };
    let operations = operation_bytes
        .chunks_exact(17)
        .map(|raw| {
            Ok(TimestampTxnOperation {
                range_id: u32::from_be_bytes(raw[0..4].try_into().expect("4 bytes")),
                table_id: u32::from_be_bytes(raw[4..8].try_into().expect("4 bytes")),
                rowid: u64::from_be_bytes(raw[8..16].try_into().expect("8 bytes")),
                delete: match raw[16] {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(crabka_pgkv::KvError::CorruptRow(
                            "bad timestamp transaction delete operation".into(),
                        ));
                    }
                },
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let decision = PrimaryTxnDecision::decode(decision)?;
    if let PrimaryTxnDecision::Committed(commit_ts) = decision
        && commit_ts.get() <= start_ts.get()
    {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "timestamp transaction commit timestamp does not follow start timestamp".into(),
        ));
    }
    Ok(Some(TimestampTxnDescriptor {
        start_ts,
        global_xid,
        generation,
        participants,
        prepared,
        operations,
        decision,
    }))
}

/// Enumerate all durable range-0 timestamp descriptors. Malformed keys and values
/// are corruption: recovery must stop rather than guess a transaction decision.
pub fn timestamp_txn_descriptors(
    kv: &dyn crabka_pgkv::Kv,
) -> Result<Vec<TimestampTxnDescriptor>, crabka_pgkv::KvError> {
    const PREFIX: &[u8] = b"\0\0\0\0meta/ts_txn/";
    kv.scan_prefix(PREFIX)?
        .into_iter()
        .map(|(key, _)| {
            let Some(raw) = key.strip_prefix(PREFIX) else {
                return Err(crabka_pgkv::KvError::CorruptRow(
                    "timestamp transaction descriptor has invalid key prefix".into(),
                ));
            };
            let raw: [u8; 8] = raw.try_into().map_err(|_| {
                crabka_pgkv::KvError::CorruptRow(
                    "timestamp transaction descriptor has invalid key length".into(),
                )
            })?;
            let start_ts = TimestampTransactionId::new(u64::from_be_bytes(raw)).map_err(|_| {
                crabka_pgkv::KvError::CorruptRow(
                    "timestamp transaction descriptor has zero start timestamp".into(),
                )
            })?;
            read_timestamp_txn_descriptor(kv, start_ts)?.ok_or_else(|| {
                crabka_pgkv::KvError::CorruptRow(
                    "timestamp transaction descriptor disappeared during recovery".into(),
                )
            })
        })
        .collect()
}

/// Return the largest timestamp represented by durable timestamp tuple versions
/// and range-0 transaction descriptors on this store.
pub fn durable_timestamp_horizon(kv: &dyn crabka_pgkv::Kv) -> Result<u64, crabka_pgkv::KvError> {
    let start = crabka_pgkv::key::table_prefix(crabka_pgkv::key::SYSTEM_TABLE_ID + 1);
    let end = [0xFF_u8; 5];
    let mut horizon = 0;
    for (_key, bytes) in kv.scan_range(&start, &end)? {
        let Ok(version) = crabka_pgmvcc::version::decode_ts_tuple(&bytes) else {
            continue;
        };
        horizon = horizon.max(version.start_ts);
        match version.state {
            crabka_pgmvcc::version::TsVersionState::Committed { commit_ts }
            | crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts } => {
                horizon = horizon.max(commit_ts);
            }
            crabka_pgmvcc::version::TsVersionState::Intent
            | crabka_pgmvcc::version::TsVersionState::Aborted => {}
        }
    }
    for descriptor in timestamp_txn_descriptors(kv)? {
        horizon = horizon.max(descriptor.start_ts.get());
        if let PrimaryTxnDecision::Committed(commit_ts) = descriptor.decision {
            horizon = horizon.max(commit_ts.get());
        }
    }
    Ok(horizon)
}

/// Return the greatest durable timestamp known to either the local range or the
/// range-0 catalog/descriptor store.
pub fn durable_timestamp_horizon_with_catalog(
    local_kv: &dyn crabka_pgkv::Kv,
    catalog_kv: &dyn crabka_pgkv::Kv,
) -> Result<u64, crabka_pgkv::KvError> {
    Ok(durable_timestamp_horizon(local_kv)?.max(durable_timestamp_horizon(catalog_kv)?))
}

/// Build idempotent physical-abort operations for every timestamp intent on this
/// range. It deliberately discovers intents from durable MVCC state so crash
/// recovery does not depend on a coordinator's volatile write list.
pub fn abort_timestamp_intent_ops(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
) -> Result<Vec<crabka_pgkv::WriteOp>, crabka_pgkv::KvError> {
    let start = crabka_pgkv::key::table_prefix(crabka_pgkv::key::SYSTEM_TABLE_ID + 1);
    let end = [0xFF_u8; 5];
    let mut ops: Vec<_> = kv
        .scan_range(&start, &end)?
        .into_iter()
        .filter_map(|(key, bytes)| {
            let version = crabka_pgmvcc::version::decode_ts_tuple(&bytes).ok()?;
            if version.start_ts != start_ts.get()
                || version.state != crabka_pgmvcc::version::TsVersionState::Intent
            {
                return None;
            }
            Some(crabka_pgkv::WriteOp::Put {
                key,
                value: crabka_pgmvcc::version::encode_ts_tuple(
                    start_ts.get(),
                    crabka_pgmvcc::version::TsVersionState::Aborted,
                    &version.row,
                ),
            })
        })
        .collect();
    let expected_reservation = start_ts.get().to_be_bytes();
    ops.extend(
        kv.scan_prefix(b"\0\0\0\0meta/ts_prewrite/")?
            .into_iter()
            .filter(|(_, value)| value.as_slice() == expected_reservation)
            .map(|(key, _)| crabka_pgkv::WriteOp::Delete { key }),
    );
    ops.extend(
        kv.scan_prefix(b"\0\0\0\0meta/ts_intent/")?
            .into_iter()
            .filter(|(key, _)| key.ends_with(&expected_reservation))
            .map(|(key, _)| crabka_pgkv::WriteOp::Delete { key }),
    );
    ops.extend(
        kv.scan_prefix(b"\0\0\0\0index/ts_intent/")?
            .into_iter()
            .filter(|(_, value)| {
                take_u64(value)
                    .is_some_and(|(intent_start_ts, _)| intent_start_ts == start_ts.get())
            })
            .map(|(key, _)| crabka_pgkv::WriteOp::Delete { key }),
    );
    Ok(ops)
}

impl PrimaryTxnDecision {
    fn encode(self) -> Vec<u8> {
        match self {
            Self::Pending => vec![0],
            Self::Aborted => vec![1],
            Self::Committed(commit_ts) => {
                let mut out = Vec::with_capacity(9);
                out.push(2);
                out.extend_from_slice(&commit_ts.get().to_be_bytes());
                out
            }
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, crabka_pgkv::KvError> {
        let Some((&tag, rest)) = bytes.split_first() else {
            return Err(crabka_pgkv::KvError::CorruptRow(
                "empty timestamp primary decision".into(),
            ));
        };
        match (tag, rest) {
            (0, []) => Ok(Self::Pending),
            (1, []) => Ok(Self::Aborted),
            (2, raw) if raw.len() == 8 => {
                let commit_ts = u64::from_be_bytes(raw.try_into().expect("8 bytes"));
                CommitTimestamp::new(commit_ts)
                    .map(Self::Committed)
                    .map_err(|_| crabka_pgkv::KvError::CorruptRow("bad commit timestamp".into()))
            }
            _ => Err(crabka_pgkv::KvError::CorruptRow(
                "bad timestamp primary decision".into(),
            )),
        }
    }
}

/// Minimal participant seam for G-9 timestamp transactions.
#[derive(Clone)]
pub struct TimestampTxnParticipant {
    kv: std::sync::Arc<dyn crabka_pgkv::Kv>,
    primary_kv: std::sync::Arc<dyn crabka_pgkv::Kv>,
    committer: std::sync::Arc<dyn Committer>,
    range_id: u32,
}

impl std::fmt::Debug for TimestampTxnParticipant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampTxnParticipant")
            .finish_non_exhaustive()
    }
}

impl TimestampTxnParticipant {
    /// Build a participant over the local range store and durable committer.
    #[must_use]
    pub fn new(
        kv: std::sync::Arc<dyn crabka_pgkv::Kv>,
        primary_kv: std::sync::Arc<dyn crabka_pgkv::Kv>,
        committer: std::sync::Arc<dyn Committer>,
        range_id: u32,
    ) -> Self {
        Self {
            kv,
            primary_kv,
            committer,
            range_id,
        }
    }

    /// Prewrite durable intents after first-committer-wins conflict checks.
    pub async fn prewrite(
        &self,
        start_ts: TimestampTransactionId,
        writes: &[TimestampWrite],
    ) -> Result<(), ExecError> {
        let ops = prewrite_ops(self.kv.as_ref(), start_ts, writes).map_err(map_ts_error)?;
        self.committer.commit(ops).await?;
        verify_prewrite_reservations(self.kv.as_ref(), start_ts, writes).map_err(map_ts_error)
    }

    /// Prewrite intents for a transaction whose authoritative decision is the
    /// range-0 descriptor. The descriptor already contains the complete
    /// participant set, so no per-intent sidecar is needed for recovery.
    pub async fn prewrite_with_primary(
        &self,
        identity: TimestampTxnIdentity,
        writes: &[TimestampWrite],
    ) -> Result<(), ExecError> {
        validate_primary_identity(self.primary_kv.as_ref(), identity, self.range_id)
            .map_err(map_ts_error)?;
        let ops = prewrite_with_identity_ops(self.kv.as_ref(), identity, self.range_id, writes)
            .map_err(map_ts_error)?;
        self.committer.commit(ops).await?;
        verify_local_prewrite(self.kv.as_ref(), identity, self.range_id, writes)
            .map_err(map_ts_error)?;
        if let Err(error) =
            validate_primary_identity(self.primary_kv.as_ref(), identity, self.range_id)
        {
            self.abort_unacknowledged_prewrites(identity, writes)
                .await?;
            return Err(map_ts_error(error));
        }
        Ok(())
    }

    /// Write the primary decision and resolve local intents to committed versions.
    pub async fn commit(
        &self,
        start_ts: TimestampTransactionId,
        commit_ts: CommitTimestamp,
        writes: &[TimestampWrite],
    ) -> Result<(), ExecError> {
        self.commit_with_ops(start_ts, commit_ts, writes, Vec::new())
            .await
    }

    /// Write the primary decision, resolve local intents, and fold extra durable
    /// operations into the same atomic commit batch.
    pub async fn commit_with_ops(
        &self,
        start_ts: TimestampTransactionId,
        commit_ts: CommitTimestamp,
        writes: &[TimestampWrite],
        extra_ops: Vec<crabka_pgkv::WriteOp>,
    ) -> Result<(), ExecError> {
        let mut ops = vec![primary_decision_op(
            start_ts,
            PrimaryTxnDecision::Committed(commit_ts),
        )];
        ops.extend(resolve_commit_ops(
            self.kv.as_ref(),
            start_ts,
            commit_ts,
            writes,
        )?);
        ops.extend(extra_ops);
        self.committer.commit(ops).await
    }

    /// Abort the primary decision and resolve local intents to aborted versions.
    pub async fn abort(
        &self,
        start_ts: TimestampTransactionId,
        writes: &[TimestampWrite],
    ) -> Result<(), ExecError> {
        let mut ops = vec![primary_decision_op(start_ts, PrimaryTxnDecision::Aborted)];
        ops.extend(
            resolve_ops(
                self.kv.as_ref(),
                start_ts,
                TimestampTxnDecision::Aborted,
                writes,
            )
            .map_err(map_ts_error)?,
        );
        ops.extend(delete_global_index_intent_ops(start_ts, writes));
        self.committer.commit(ops).await
    }

    /// Idempotently apply a local decision without a distributed primary.
    pub async fn resolve(
        &self,
        start_ts: TimestampTransactionId,
        decision: TimestampTxnDecision,
        writes: &[TimestampWrite],
    ) -> Result<(), ExecError> {
        let mut ops = resolve_ops_idempotent_legacy(self.kv.as_ref(), start_ts, decision, writes)
            .map_err(map_ts_error)?;
        if decision == TimestampTxnDecision::Aborted {
            ops.extend(delete_global_index_intent_ops(start_ts, writes));
        }
        self.committer.commit(ops).await
    }

    /// Idempotently apply a decision already made by the range-0 primary after
    /// proving the durable primary identity and intent fence.
    pub async fn resolve_with_primary(
        &self,
        identity: TimestampTxnIdentity,
        decision: TimestampTxnDecision,
        writes: &[TimestampWrite],
    ) -> Result<(), ExecError> {
        validate_primary_identity_for_resolution(self.primary_kv.as_ref(), identity, decision)
            .map_err(map_ts_error)?;
        let mut ops =
            resolve_ops_idempotent(self.kv.as_ref(), identity, self.range_id, decision, writes)
                .map_err(map_ts_error)?;
        if decision == TimestampTxnDecision::Aborted {
            ops.extend(delete_global_index_intent_ops(identity.start_ts, writes));
        }
        self.committer.commit(ops).await
    }

    /// Idempotently settle durable operations after recovering a terminal range-0
    /// descriptor. Unlike normal resolution, restart recovery has no volatile
    /// `TimestampWrite` list to reconstruct global-index keys from.
    pub async fn resolve_operations_with_primary(
        &self,
        identity: TimestampTxnIdentity,
        decision: TimestampTxnDecision,
        operations: &[TimestampTxnOperation],
    ) -> Result<(), ExecError> {
        validate_primary_identity_for_resolution(self.primary_kv.as_ref(), identity, decision)
            .map_err(map_ts_error)?;
        let mut ops = Vec::with_capacity(operations.len());
        for operation in operations {
            if operation.range_id != self.range_id {
                return Err(map_ts_error(TimestampTxnError::IdentityFenced));
            }
            let write = TimestampWrite {
                table_id: operation.table_id,
                rowid: operation.rowid,
                row: Vec::new(),
                delete: operation.delete,
                global_index_intents: Vec::new(),
            };
            let row_decision = match decision {
                TimestampTxnDecision::Committed(commit_ts) if operation.delete => {
                    TimestampTxnDecision::Deleted(commit_ts)
                }
                other => other,
            };
            ops.extend(
                resolve_ops_idempotent(
                    self.kv.as_ref(),
                    identity,
                    self.range_id,
                    row_decision,
                    std::slice::from_ref(&write),
                )
                .map_err(map_ts_error)?,
            );
        }
        ops.extend(resolve_recovered_global_index_intents(
            self.kv.as_ref(),
            identity.start_ts,
            decision,
        )?);
        if ops.is_empty() {
            return Ok(());
        }
        self.committer.commit(ops).await
    }

    async fn abort_unacknowledged_prewrites(
        &self,
        identity: TimestampTxnIdentity,
        writes: &[TimestampWrite],
    ) -> Result<(), ExecError> {
        let mut ops = resolve_ops(
            self.kv.as_ref(),
            identity.start_ts,
            TimestampTxnDecision::Aborted,
            writes,
        )
        .map_err(map_ts_error)?;
        ops.extend(writes.iter().map(|write| crabka_pgkv::WriteOp::Delete {
            key: timestamp_intent_identity_key(write, identity.start_ts),
        }));
        ops.extend(delete_global_index_intent_ops(identity.start_ts, writes));
        self.committer.commit(ops).await
    }

    /// Read the durable primary decision for resolver RPC handlers.
    pub fn primary_decision(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<PrimaryTxnDecision, ExecError> {
        read_primary_decision(self.kv.as_ref(), start_ts).map_err(Into::into)
    }
}

fn resolve_ops_idempotent_legacy(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    decision: TimestampTxnDecision,
    writes: &[TimestampWrite],
) -> Result<Vec<crabka_pgkv::WriteOp>, TimestampTxnError> {
    let mut ops = Vec::with_capacity(writes.len());
    for write in writes {
        ops.extend(resolve_ops_idempotent_for_write(
            kv, start_ts, decision, write,
        )?);
    }
    Ok(ops)
}

fn resolve_ops_idempotent(
    kv: &dyn crabka_pgkv::Kv,
    identity: TimestampTxnIdentity,
    range_id: u32,
    decision: TimestampTxnDecision,
    writes: &[TimestampWrite],
) -> Result<Vec<crabka_pgkv::WriteOp>, TimestampTxnError> {
    let mut ops = Vec::with_capacity(writes.len());
    for write in writes {
        let stored_identity = kv
            .get(&timestamp_intent_identity_key(write, identity.start_ts))
            .map_err(|_| TimestampTxnError::IdentityFenced)?;
        let expected_identity = encode_timestamp_intent_identity(identity, range_id);
        if stored_identity.as_deref() == Some(expected_identity.as_slice()) {
            ops.extend(resolve_ops_idempotent_for_write(
                kv,
                identity.start_ts,
                decision,
                write,
            )?);
            if decision == TimestampTxnDecision::Aborted {
                ops.push(crabka_pgkv::WriteOp::Delete {
                    key: timestamp_intent_identity_key(write, identity.start_ts),
                });
            }
            continue;
        }
        if stored_identity.is_none()
            && write_is_resolved_to(kv, identity.start_ts, decision, write)?
        {
            continue;
        }
        return Err(TimestampTxnError::IdentityFenced);
    }
    Ok(ops)
}

fn write_is_resolved_to(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    decision: TimestampTxnDecision,
    write: &TimestampWrite,
) -> Result<bool, TimestampTxnError> {
    let key = crabka_pgmvcc::version::version_key_ts(write.table_id, write.rowid, start_ts.get());
    let Some(bytes) = kv
        .get(&key)
        .map_err(|_| TimestampTxnError::IdentityFenced)?
    else {
        return Ok(false);
    };
    let version = crabka_pgmvcc::version::decode_ts_tuple(&bytes)
        .map_err(|_| TimestampTxnError::IdentityFenced)?;
    let expected_state = match decision {
        TimestampTxnDecision::Pending => crabka_pgmvcc::version::TsVersionState::Intent,
        TimestampTxnDecision::Aborted => crabka_pgmvcc::version::TsVersionState::Aborted,
        TimestampTxnDecision::Committed(commit_ts) => {
            crabka_pgmvcc::version::TsVersionState::Committed {
                commit_ts: commit_ts.get(),
            }
        }
        TimestampTxnDecision::Deleted(commit_ts) => {
            crabka_pgmvcc::version::TsVersionState::Deleted {
                commit_ts: commit_ts.get(),
            }
        }
    };
    Ok(version.start_ts == start_ts.get() && version.state == expected_state)
}

fn resolve_ops_idempotent_for_write(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    decision: TimestampTxnDecision,
    write: &TimestampWrite,
) -> Result<Vec<crabka_pgkv::WriteOp>, TimestampTxnError> {
    let key = crabka_pgmvcc::version::version_key_ts(write.table_id, write.rowid, start_ts.get());
    let Some(bytes) = kv.get(&key).map_err(|_| TimestampTxnError::MissingIntent {
        table_id: write.table_id,
        rowid: write.rowid,
        start_ts: start_ts.get(),
    })?
    else {
        return Ok(Vec::new());
    };
    let version = crabka_pgmvcc::version::decode_ts_tuple(&bytes).map_err(|_| {
        TimestampTxnError::MissingIntent {
            table_id: write.table_id,
            rowid: write.rowid,
            start_ts: start_ts.get(),
        }
    })?;
    let requested_state = match decision {
        TimestampTxnDecision::Aborted => crabka_pgmvcc::version::TsVersionState::Aborted,
        TimestampTxnDecision::Committed(commit_ts) => {
            crabka_pgmvcc::version::TsVersionState::Committed {
                commit_ts: commit_ts.get(),
            }
        }
        TimestampTxnDecision::Deleted(commit_ts) => {
            crabka_pgmvcc::version::TsVersionState::Deleted {
                commit_ts: commit_ts.get(),
            }
        }
        TimestampTxnDecision::Pending => crabka_pgmvcc::version::TsVersionState::Intent,
    };
    if version.state == requested_state {
        return Ok(Vec::new());
    }
    if version.state != crabka_pgmvcc::version::TsVersionState::Intent {
        return Err(TimestampTxnError::MissingIntent {
            table_id: write.table_id,
            rowid: write.rowid,
            start_ts: start_ts.get(),
        });
    }
    Ok(vec![
        crabka_pgkv::WriteOp::Put {
            key,
            value: crabka_pgmvcc::version::encode_ts_tuple(
                start_ts.get(),
                requested_state,
                &version.row,
            ),
        },
        crabka_pgkv::WriteOp::Delete {
            key: timestamp_prewrite_reservation_key(write),
        },
    ])
}

fn prewrite_with_identity_ops(
    kv: &dyn crabka_pgkv::Kv,
    identity: TimestampTxnIdentity,
    range_id: u32,
    writes: &[TimestampWrite],
) -> Result<Vec<crabka_pgkv::WriteOp>, TimestampTxnError> {
    let mut ops = prewrite_ops(kv, identity.start_ts, writes)?;
    for write in writes {
        ops.push(crabka_pgkv::WriteOp::ConditionalPut {
            key: timestamp_intent_identity_key(write, identity.start_ts),
            expected: None,
            value: encode_timestamp_intent_identity(identity, range_id),
        });
    }
    Ok(ops)
}

fn verify_local_prewrite(
    kv: &dyn crabka_pgkv::Kv,
    identity: TimestampTxnIdentity,
    range_id: u32,
    writes: &[TimestampWrite],
) -> Result<(), TimestampTxnError> {
    let expected_identity = encode_timestamp_intent_identity(identity, range_id);
    for write in writes {
        let stored_identity = kv
            .get(&timestamp_intent_identity_key(write, identity.start_ts))
            .map_err(|_| TimestampTxnError::WriteConflict {
                table_id: write.table_id,
                rowid: write.rowid,
            })?;
        if stored_identity.as_deref() != Some(expected_identity.as_slice()) {
            return Err(TimestampTxnError::WriteConflict {
                table_id: write.table_id,
                rowid: write.rowid,
            });
        }
    }
    Ok(())
}

fn verify_prewrite_reservations(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    writes: &[TimestampWrite],
) -> Result<(), TimestampTxnError> {
    let expected_reservation = start_ts.get().to_be_bytes();
    for write in writes {
        let reservation = kv
            .get(&timestamp_prewrite_reservation_key(write))
            .map_err(|_| TimestampTxnError::WriteConflict {
                table_id: write.table_id,
                rowid: write.rowid,
            })?;
        if reservation.as_deref() != Some(expected_reservation.as_slice()) {
            return Err(TimestampTxnError::WriteConflict {
                table_id: write.table_id,
                rowid: write.rowid,
            });
        }
    }
    Ok(())
}

fn validate_primary_identity(
    primary_kv: &dyn crabka_pgkv::Kv,
    identity: TimestampTxnIdentity,
    participant_range: u32,
) -> Result<(), TimestampTxnError> {
    if identity.primary_range != 0 {
        return Err(TimestampTxnError::IdentityFenced);
    }
    let descriptor = read_timestamp_txn_descriptor(primary_kv, identity.start_ts)
        .map_err(|_| TimestampTxnError::IdentityFenced)?
        .ok_or(TimestampTxnError::IdentityFenced)?;
    if descriptor.global_xid != identity.global_xid {
        return Err(TimestampTxnError::IdentityFenced);
    }
    if participant_range != u32::MAX && !descriptor.participants.contains(&participant_range) {
        return Err(TimestampTxnError::IdentityFenced);
    }
    if descriptor.decision != PrimaryTxnDecision::Pending {
        return Err(TimestampTxnError::PrimaryAlreadyDecided);
    }
    Ok(())
}

fn validate_primary_identity_for_resolution(
    primary_kv: &dyn crabka_pgkv::Kv,
    identity: TimestampTxnIdentity,
    decision: TimestampTxnDecision,
) -> Result<(), TimestampTxnError> {
    validate_primary_identity(primary_kv, identity, u32::MAX).or_else(|error| match error {
        TimestampTxnError::PrimaryAlreadyDecided => Ok(()),
        other => Err(other),
    })?;
    let descriptor = read_timestamp_txn_descriptor(primary_kv, identity.start_ts)
        .map_err(|_| TimestampTxnError::IdentityFenced)?
        .ok_or(TimestampTxnError::IdentityFenced)?;
    let actual = match descriptor.decision {
        PrimaryTxnDecision::Pending => return Err(TimestampTxnError::PrimaryAlreadyDecided),
        PrimaryTxnDecision::Aborted => TimestampTxnDecision::Aborted,
        PrimaryTxnDecision::Committed(commit_ts) => TimestampTxnDecision::Committed(commit_ts),
    };
    if actual != decision {
        return Err(TimestampTxnError::IdentityFenced);
    }
    Ok(())
}

fn timestamp_intent_identity_key(
    write: &TimestampWrite,
    start_ts: TimestampTransactionId,
) -> Vec<u8> {
    let mut key = b"\0\0\0\0meta/ts_intent/".to_vec();
    key.extend_from_slice(&write.table_id.to_be_bytes());
    key.extend_from_slice(&write.rowid.to_be_bytes());
    key.extend_from_slice(&start_ts.get().to_be_bytes());
    key
}

fn timestamp_prewrite_reservation_key(write: &TimestampWrite) -> Vec<u8> {
    timestamp_prewrite_reservation_key_for(write.table_id, write.rowid)
}

fn timestamp_prewrite_reservation_key_for(table_id: u32, rowid: u64) -> Vec<u8> {
    let mut key = b"\0\0\0\0meta/ts_prewrite/".to_vec();
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&rowid.to_be_bytes());
    key
}

fn encode_timestamp_intent_identity(identity: TimestampTxnIdentity, range_id: u32) -> Vec<u8> {
    let mut value = Vec::with_capacity(24);
    value.extend_from_slice(&identity.start_ts.get().to_be_bytes());
    value.extend_from_slice(&identity.global_xid.to_be_bytes());
    value.extend_from_slice(&identity.primary_range.to_be_bytes());
    value.extend_from_slice(&range_id.to_be_bytes());
    value
}

/// Prove that a local version belongs to the exact committed distributed
/// transaction recorded by the range-0 descriptor. Missing or malformed intent
/// identity is deliberately not treated as a legacy version.
pub(crate) fn local_intent_matches_descriptor(
    kv: &dyn crabka_pgkv::Kv,
    descriptor: &TimestampTxnDescriptor,
    table_id: u32,
    rowid: u64,
) -> Result<bool, crabka_pgkv::KvError> {
    let write = TimestampWrite {
        table_id,
        rowid,
        row: Vec::new(),
        delete: false,
        global_index_intents: Vec::new(),
    };
    let Some(bytes) = kv.get(&timestamp_intent_identity_key(&write, descriptor.start_ts))? else {
        return Ok(false);
    };
    let Some((start_ts, rest)) = take_u64(&bytes) else {
        return Ok(false);
    };
    let Some((global_xid, rest)) = take_u64(rest) else {
        return Ok(false);
    };
    let Some((primary_range, rest)) = take_u32(rest) else {
        return Ok(false);
    };
    let Some((range_id, [])) = take_u32(rest) else {
        return Ok(false);
    };
    if start_ts != descriptor.start_ts.get()
        || global_xid != descriptor.global_xid
        || primary_range != 0
        || !descriptor.prepared.contains(&range_id)
        || !descriptor.operations.iter().any(|operation| {
            operation.range_id == range_id
                && operation.table_id == table_id
                && operation.rowid == rowid
        })
    {
        return Ok(false);
    }
    Ok(true)
}

/// Build prewrite intent operations, failing before any write if the row has a
/// conflicting intent or a version committed after `start_ts`.
pub fn prewrite_ops(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    writes: &[TimestampWrite],
) -> Result<Vec<crabka_pgkv::WriteOp>, TimestampTxnError> {
    let index_intent_count = writes
        .iter()
        .map(|write| write.global_index_intents.len())
        .sum::<usize>();
    let mut ops = Vec::with_capacity((writes.len() * 2) + index_intent_count);
    for write in writes {
        ensure_prewrite_can_win(kv, start_ts, write)?;
        ops.push(crabka_pgkv::WriteOp::ConditionalPut {
            key: timestamp_prewrite_reservation_key(write),
            expected: None,
            value: start_ts.get().to_be_bytes().to_vec(),
        });
        ops.push(crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_ts(
                write.table_id,
                write.rowid,
                start_ts.get(),
            ),
            value: crabka_pgmvcc::version::encode_ts_tuple(
                start_ts.get(),
                crabka_pgmvcc::version::TsVersionState::Intent,
                &write.row,
            ),
        });
        for intent in &write.global_index_intents {
            ops.push(crabka_pgkv::WriteOp::Put {
                key: global_index_intent_key(start_ts, intent),
                value: global_index_intent_value(start_ts, intent),
            });
        }
    }
    Ok(ops)
}

/// Check whether a set of base writes carries exactly the supplied global-index
/// maintenance intents.
#[must_use]
pub fn base_index_intents_match(writes: &[TimestampWrite]) -> bool {
    writes.iter().all(|write| {
        write.global_index_intents.iter().all(|intent| {
            intent.base_table_id == write.table_id && intent.base_rowid == write.rowid
        })
    })
}

/// Build operations that resolve existing intents locally.
pub fn resolve_ops(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    decision: TimestampTxnDecision,
    writes: &[TimestampWrite],
) -> Result<Vec<crabka_pgkv::WriteOp>, TimestampTxnError> {
    let mut ops = Vec::with_capacity(writes.len());
    for write in writes {
        let key =
            crabka_pgmvcc::version::version_key_ts(write.table_id, write.rowid, start_ts.get());
        let Some(bytes) = kv.get(&key).map_err(|_| TimestampTxnError::MissingIntent {
            table_id: write.table_id,
            rowid: write.rowid,
            start_ts: start_ts.get(),
        })?
        else {
            return Err(TimestampTxnError::MissingIntent {
                table_id: write.table_id,
                rowid: write.rowid,
                start_ts: start_ts.get(),
            });
        };
        let version = crabka_pgmvcc::version::decode_ts_tuple(&bytes).map_err(|_| {
            TimestampTxnError::MissingIntent {
                table_id: write.table_id,
                rowid: write.rowid,
                start_ts: start_ts.get(),
            }
        })?;
        if version.start_ts != start_ts.get()
            || version.state != crabka_pgmvcc::version::TsVersionState::Intent
        {
            return Err(TimestampTxnError::MissingIntent {
                table_id: write.table_id,
                rowid: write.rowid,
                start_ts: start_ts.get(),
            });
        }
        let state = match decision {
            TimestampTxnDecision::Pending => crabka_pgmvcc::version::TsVersionState::Intent,
            TimestampTxnDecision::Aborted => crabka_pgmvcc::version::TsVersionState::Aborted,
            TimestampTxnDecision::Committed(commit_ts) => {
                crabka_pgmvcc::version::TsVersionState::Committed {
                    commit_ts: commit_ts.get(),
                }
            }
            TimestampTxnDecision::Deleted(commit_ts) => {
                crabka_pgmvcc::version::TsVersionState::Deleted {
                    commit_ts: commit_ts.get(),
                }
            }
        };
        ops.push(crabka_pgkv::WriteOp::Put {
            key,
            value: crabka_pgmvcc::version::encode_ts_tuple(start_ts.get(), state, &version.row),
        });
        ops.push(crabka_pgkv::WriteOp::Delete {
            key: timestamp_prewrite_reservation_key(write),
        });
    }
    Ok(ops)
}

fn resolve_commit_ops(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    commit_ts: CommitTimestamp,
    writes: &[TimestampWrite],
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    let index_intent_count = writes
        .iter()
        .map(|write| write.global_index_intents.len())
        .sum::<usize>();
    let mut ops = Vec::with_capacity(writes.len() + (index_intent_count * 2));
    for write in writes {
        let decision = if write.delete {
            TimestampTxnDecision::Deleted(commit_ts)
        } else {
            TimestampTxnDecision::Committed(commit_ts)
        };
        let write_ops = resolve_ops(kv, start_ts, decision, std::slice::from_ref(write))
            .map_err(map_ts_error)?;
        ops.extend(write_ops);
        ops.extend(resolve_global_index_intent_ops(start_ts, commit_ts, write));
    }
    Ok(ops)
}

fn resolve_global_index_intent_ops(
    start_ts: TimestampTransactionId,
    commit_ts: CommitTimestamp,
    write: &TimestampWrite,
) -> Vec<crabka_pgkv::WriteOp> {
    write
        .global_index_intents
        .iter()
        .flat_map(|intent| {
            [
                crabka_pgkv::WriteOp::Put {
                    key: global_index_entry_key(commit_ts, intent),
                    value: global_index_entry_value(commit_ts, intent),
                },
                crabka_pgkv::WriteOp::Delete {
                    key: global_index_intent_key(start_ts, intent),
                },
            ]
        })
        .collect()
}

fn delete_global_index_intent_ops(
    start_ts: TimestampTransactionId,
    writes: &[TimestampWrite],
) -> Vec<crabka_pgkv::WriteOp> {
    writes
        .iter()
        .flat_map(|write| &write.global_index_intents)
        .map(|intent| crabka_pgkv::WriteOp::Delete {
            key: global_index_intent_key(start_ts, intent),
        })
        .collect()
}

fn resolve_recovered_global_index_intents(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    decision: TimestampTxnDecision,
) -> Result<Vec<crabka_pgkv::WriteOp>, ExecError> {
    const INTENT_PREFIX: &[u8] = b"\0\0\0\0index/ts_intent/";
    const ENTRY_PREFIX: &[u8] = b"\0\0\0\0index/ts_entry/";
    let mut ops = Vec::new();
    for (intent_key, intent_value) in kv.scan_prefix(INTENT_PREFIX)? {
        let Some((intent_start_ts, rest)) = take_u64(&intent_value) else {
            return Err(ExecError::Unsupported(
                "malformed timestamp global-index intent during recovery".into(),
            ));
        };
        if intent_start_ts != start_ts.get() {
            continue;
        }
        match decision {
            TimestampTxnDecision::Aborted => {
                ops.push(crabka_pgkv::WriteOp::Delete { key: intent_key });
            }
            TimestampTxnDecision::Committed(commit_ts)
            | TimestampTxnDecision::Deleted(commit_ts) => {
                let Some(key_tail) = intent_key.strip_prefix(INTENT_PREFIX) else {
                    return Err(ExecError::Unsupported(
                        "malformed timestamp global-index intent key during recovery".into(),
                    ));
                };
                let Some((index_id, key_tail)) = key_tail.split_at_checked(4) else {
                    return Err(ExecError::Unsupported(
                        "short timestamp global-index intent key during recovery".into(),
                    ));
                };
                let Some((_start_ts, index_values_and_base)) = key_tail.split_at_checked(8) else {
                    return Err(ExecError::Unsupported(
                        "short timestamp global-index intent key during recovery".into(),
                    ));
                };
                if rest.len() != 14 {
                    return Err(ExecError::Unsupported(
                        "short timestamp global-index intent value during recovery".into(),
                    ));
                }
                let unique = rest[12];
                let delete = rest[13];
                if unique > 1 || delete > 1 {
                    return Err(ExecError::Unsupported(
                        "malformed timestamp global-index intent value during recovery".into(),
                    ));
                }
                let mut entry_key = Vec::with_capacity(
                    ENTRY_PREFIX.len() + index_id.len() + index_values_and_base.len() + 8,
                );
                entry_key.extend_from_slice(ENTRY_PREFIX);
                entry_key.extend_from_slice(index_id);
                entry_key.extend_from_slice(index_values_and_base);
                entry_key.extend_from_slice(&commit_ts.get().to_be_bytes());
                let mut entry_value = Vec::with_capacity(21);
                entry_value.push(delete);
                entry_value.extend_from_slice(&commit_ts.get().to_be_bytes());
                entry_value.extend_from_slice(&rest[0..12]);
                ops.push(crabka_pgkv::WriteOp::Put {
                    key: entry_key,
                    value: entry_value,
                });
                ops.push(crabka_pgkv::WriteOp::Delete { key: intent_key });
            }
            TimestampTxnDecision::Pending => {
                return Err(ExecError::Unsupported(
                    "cannot recover pending timestamp global-index intent".into(),
                ));
            }
        }
    }
    Ok(ops)
}

/// Read the newest visible timestamp version for one row at `read_ts`, excluding
/// unresolved or aborted intents and honoring delete tombstones.
pub fn read_visible_ts_row(
    kv: &dyn crabka_pgkv::Kv,
    table_id: u32,
    rowid: u64,
    read_ts: ReadTimestamp,
) -> Result<Option<Vec<crabka_pgtypes::Datum>>, ExecError> {
    let prefix = crabka_pgkv::key::row_key(table_id, rowid);
    let mut visible: Option<(u64, Option<Vec<crabka_pgtypes::Datum>>)> = None;
    for (_key, value) in kv.scan_prefix(&prefix)? {
        let version = crabka_pgmvcc::version::decode_ts_tuple(&value)?;
        let candidate = match version.state {
            crabka_pgmvcc::version::TsVersionState::Committed { commit_ts }
                if commit_ts <= read_ts.get() =>
            {
                Some((commit_ts, Some(version.row)))
            }
            crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts }
                if commit_ts <= read_ts.get() =>
            {
                Some((commit_ts, None))
            }
            _ => None,
        };
        let Some((commit_ts, row)) = candidate else {
            continue;
        };
        if visible
            .as_ref()
            .is_none_or(|(current_ts, _)| commit_ts > *current_ts)
        {
            visible = Some((commit_ts, row));
        }
    }
    Ok(visible.and_then(|(_commit_ts, row)| row))
}

/// Read visible non-unique global-index entries for an exact index key at
/// `read_ts`.
pub fn read_visible_global_index_entries(
    kv: &dyn crabka_pgkv::Kv,
    index_id: u32,
    indexed_values: &[crabka_pgtypes::Datum],
    read_ts: ReadTimestamp,
) -> Result<Vec<VisibleGlobalIndexEntry>, ExecError> {
    let prefix = global_index_entry_prefix(index_id, indexed_values);
    let mut latest_by_base =
        std::collections::BTreeMap::<VisibleGlobalIndexEntry, (u64, bool)>::new();
    for (_key, value) in kv.scan_prefix(&prefix)? {
        let Some(entry) = decode_global_index_entry_value(&value)? else {
            continue;
        };
        if entry.commit_ts > read_ts.get() {
            continue;
        }
        let visible_entry = VisibleGlobalIndexEntry {
            base_table_id: entry.base_table_id,
            base_rowid: entry.base_rowid,
        };
        let latest = latest_by_base
            .entry(visible_entry)
            .or_insert((entry.commit_ts, entry.delete));
        if entry.commit_ts >= latest.0 {
            *latest = (entry.commit_ts, entry.delete);
        }
    }
    Ok(latest_by_base
        .into_iter()
        .filter_map(|(entry, (_commit_ts, delete))| (!delete).then_some(entry))
        .collect())
}

/// Return the primary-decision write op for `start_ts`.
#[must_use]
pub fn primary_decision_op(
    start_ts: TimestampTransactionId,
    decision: PrimaryTxnDecision,
) -> crabka_pgkv::WriteOp {
    crabka_pgkv::WriteOp::Put {
        key: primary_decision_key(start_ts),
        value: decision.encode(),
    }
}

/// Read the primary decision, returning pending when no record exists yet.
pub fn read_primary_decision(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
) -> Result<PrimaryTxnDecision, crabka_pgkv::KvError> {
    let Some(bytes) = kv.get(&primary_decision_key(start_ts))? else {
        return Ok(PrimaryTxnDecision::Pending);
    };
    PrimaryTxnDecision::decode(&bytes)
}

fn primary_decision_key(start_ts: TimestampTransactionId) -> Vec<u8> {
    let mut key = b"\0\0\0\0meta/ts_primary/".to_vec();
    key.extend_from_slice(&start_ts.get().to_be_bytes());
    key
}

fn global_index_intent_key(
    start_ts: TimestampTransactionId,
    intent: &GlobalIndexIntent,
) -> Vec<u8> {
    let mut key = b"\0\0\0\0index/ts_intent/".to_vec();
    key.extend_from_slice(&intent.index_id.to_be_bytes());
    key.extend_from_slice(&start_ts.get().to_be_bytes());
    for value in &intent.indexed_values {
        append_datum_key_part(&mut key, value);
    }
    key.extend_from_slice(&intent.base_table_id.to_be_bytes());
    key.extend_from_slice(&intent.base_rowid.to_be_bytes());
    key
}

fn global_index_intent_value(
    start_ts: TimestampTransactionId,
    intent: &GlobalIndexIntent,
) -> Vec<u8> {
    let mut value = Vec::with_capacity(21);
    value.extend_from_slice(&start_ts.get().to_be_bytes());
    value.extend_from_slice(&intent.base_table_id.to_be_bytes());
    value.extend_from_slice(&intent.base_rowid.to_be_bytes());
    value.push(u8::from(intent.unique));
    value.push(u8::from(intent.delete));
    value
}

fn global_index_entry_prefix(index_id: u32, indexed_values: &[crabka_pgtypes::Datum]) -> Vec<u8> {
    let mut key = b"\0\0\0\0index/ts_entry/".to_vec();
    key.extend_from_slice(&index_id.to_be_bytes());
    for value in indexed_values {
        append_datum_key_part(&mut key, value);
    }
    key
}

fn global_index_entry_key(commit_ts: CommitTimestamp, intent: &GlobalIndexIntent) -> Vec<u8> {
    let mut key = global_index_entry_prefix(intent.index_id, &intent.indexed_values);
    key.extend_from_slice(&intent.base_table_id.to_be_bytes());
    key.extend_from_slice(&intent.base_rowid.to_be_bytes());
    key.extend_from_slice(&commit_ts.get().to_be_bytes());
    key
}

fn global_index_entry_value(commit_ts: CommitTimestamp, intent: &GlobalIndexIntent) -> Vec<u8> {
    let mut value = Vec::with_capacity(21);
    value.push(u8::from(intent.delete));
    value.extend_from_slice(&commit_ts.get().to_be_bytes());
    value.extend_from_slice(&intent.base_table_id.to_be_bytes());
    value.extend_from_slice(&intent.base_rowid.to_be_bytes());
    value
}

struct GlobalIndexEntryValue {
    commit_ts: u64,
    base_table_id: u32,
    base_rowid: u64,
    delete: bool,
}

fn decode_global_index_entry_value(
    value: &[u8],
) -> Result<Option<GlobalIndexEntryValue>, crabka_pgkv::KvError> {
    let [delete, rest @ ..] = value else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "empty timestamp global index entry".into(),
        ));
    };
    let delete = match delete {
        0 => false,
        1 => true,
        _ => {
            return Err(crabka_pgkv::KvError::CorruptRow(
                "bad timestamp global index entry state".into(),
            ));
        }
    };
    let Some((commit_ts, rest)) = take_u64(rest) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp global index entry commit timestamp".into(),
        ));
    };
    if commit_ts == 0 {
        return Ok(None);
    }
    let Some((base_table_id, rest)) = take_u32(rest) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "short timestamp global index entry table id".into(),
        ));
    };
    let Some((base_rowid, [])) = take_u64(rest) else {
        return Err(crabka_pgkv::KvError::CorruptRow(
            "bad timestamp global index entry row id".into(),
        ));
    };
    Ok(Some(GlobalIndexEntryValue {
        commit_ts,
        base_table_id,
        base_rowid,
        delete,
    }))
}

fn take_u32(bytes: &[u8]) -> Option<(u32, &[u8])> {
    let (head, tail) = bytes.split_at_checked(4)?;
    Some((u32::from_be_bytes(head.try_into().expect("4 bytes")), tail))
}

fn take_u64(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let (head, tail) = bytes.split_at_checked(8)?;
    Some((u64::from_be_bytes(head.try_into().expect("8 bytes")), tail))
}

fn append_datum_key_part(out: &mut Vec<u8>, datum: &crabka_pgtypes::Datum) {
    match datum {
        crabka_pgtypes::Datum::Null => out.push(0),
        value => {
            out.push(1);
            let binary = crabka_pgtypes::encoding::encode_binary(value);
            out.extend_from_slice(
                &u32::try_from(binary.len())
                    .expect("index datum length must fit in u32")
                    .to_be_bytes(),
            );
            out.extend_from_slice(&binary);
        }
    }
}

fn ensure_prewrite_can_win(
    kv: &dyn crabka_pgkv::Kv,
    start_ts: TimestampTransactionId,
    write: &TimestampWrite,
) -> Result<(), TimestampTxnError> {
    let prefix = crabka_pgkv::key::row_key(write.table_id, write.rowid);
    let versions = kv
        .scan_prefix(&prefix)
        .map_err(|_| TimestampTxnError::WriteConflict {
            table_id: write.table_id,
            rowid: write.rowid,
        })?;
    for (_key, value) in versions {
        let Ok(version) = crabka_pgmvcc::version::decode_ts_tuple(&value) else {
            continue;
        };
        match version.state {
            crabka_pgmvcc::version::TsVersionState::Intent
                if version.start_ts != start_ts.get() =>
            {
                return Err(TimestampTxnError::WriteConflict {
                    table_id: write.table_id,
                    rowid: write.rowid,
                });
            }
            crabka_pgmvcc::version::TsVersionState::Committed { commit_ts }
            | crabka_pgmvcc::version::TsVersionState::Deleted { commit_ts }
                if commit_ts > start_ts.get() =>
            {
                return Err(TimestampTxnError::WriteConflict {
                    table_id: write.table_id,
                    rowid: write.rowid,
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn map_ts_error(error: TimestampTxnError) -> ExecError {
    match error {
        TimestampTxnError::WriteConflict { .. } => ExecError::SerializationFailure,
        other => ExecError::Unsupported(other.to_string()),
    }
}

impl TimestampVersionState {
    /// Build an unresolved intent.
    #[must_use]
    pub fn intent(start_ts: TimestampTransactionId) -> Self {
        Self {
            start_ts,
            decision: TimestampTxnDecision::Pending,
        }
    }

    /// Build an aborted version state.
    #[must_use]
    pub fn aborted(start_ts: TimestampTransactionId) -> Self {
        Self {
            start_ts,
            decision: TimestampTxnDecision::Aborted,
        }
    }

    /// Build a committed version state after proving commit order.
    pub fn committed(
        start_ts: TimestampTransactionId,
        commit_ts: CommitTimestamp,
    ) -> Result<Self, TimestampTxnError> {
        if commit_ts.get() <= start_ts.get() {
            return Err(TimestampTxnError::CommitNotAfterStart {
                start_ts: start_ts.get(),
                commit_ts: commit_ts.get(),
            });
        }
        Ok(Self {
            start_ts,
            decision: TimestampTxnDecision::Committed(commit_ts),
        })
    }

    /// Resolve this intent to a committed version.
    pub fn commit(self, commit_ts: CommitTimestamp) -> Result<Self, TimestampTxnError> {
        Self::committed(self.start_ts, commit_ts)
    }

    /// Resolve this intent to an aborted version.
    #[must_use]
    pub fn abort(self) -> Self {
        Self::aborted(self.start_ts)
    }

    /// Return the transaction start timestamp.
    #[must_use]
    pub fn start_ts(self) -> TimestampTransactionId {
        self.start_ts
    }

    /// Return the transaction's decision state.
    #[must_use]
    pub fn decision(self) -> TimestampTxnDecision {
        self.decision
    }

    /// Timestamp visibility: only committed versions with `commit_ts <= read_ts`
    /// are visible. Pending intents and aborted versions are excluded.
    #[must_use]
    pub fn is_visible_at(self, read_ts: ReadTimestamp) -> bool {
        match self.decision {
            TimestampTxnDecision::Committed(commit_ts) => commit_ts.get() <= read_ts.get(),
            TimestampTxnDecision::Pending
            | TimestampTxnDecision::Aborted
            | TimestampTxnDecision::Deleted(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_pgkv::Kv;

    use super::*;

    #[tokio::test]
    async fn bare_prewrite_reports_a_memkv_reservation_collision() {
        let kv: Arc<dyn crabka_pgkv::Kv> = Arc::new(crabka_pgkv::MemKv::new());
        let participant = TimestampTxnParticipant::new(
            Arc::clone(&kv),
            Arc::clone(&kv),
            Arc::new(crate::commit::LocalCommitter {
                kv: Arc::clone(&kv),
            }),
            0,
        );
        let write = TimestampWrite {
            table_id: 7,
            rowid: 9,
            row: vec![crabka_pgtypes::Datum::Int4(1)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        let first_start = TimestampTransactionId::new(10).expect("first timestamp");
        participant
            .prewrite(first_start, std::slice::from_ref(&write))
            .await
            .expect("first reservation");
        kv.delete(&crabka_pgmvcc::version::version_key_ts(
            write.table_id,
            write.rowid,
            first_start.get(),
        ))
        .expect("remove first intent but retain reservation");

        let second_start = TimestampTransactionId::new(11).expect("second timestamp");
        let error = participant
            .prewrite(second_start, std::slice::from_ref(&write))
            .await
            .expect_err("reservation collision must not report success");

        assert!(matches!(error, ExecError::SerializationFailure));
        assert!(
            kv.get(&crabka_pgmvcc::version::version_key_ts(
                write.table_id,
                write.rowid,
                second_start.get(),
            ))
            .expect("read second intent")
            .is_none(),
            "the rejected conditional batch must not create an intent"
        );
    }

    #[test]
    fn descriptor_rejects_empty_participant_operations_without_preparing() {
        let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
        let mut descriptor = TimestampTxnDescriptor::begun(start_ts, 9, vec![1]);

        assert_eq!(
            descriptor.acknowledge_operations(1, &[]),
            Err(TimestampTxnError::EmptyOperations)
        );
        assert!(descriptor.prepared.is_empty());
        assert!(descriptor.operations.is_empty());
        assert_eq!(descriptor.generation, 0);
    }

    #[test]
    fn allocator_grants_monotone_transaction_and_commit_timestamps() {
        let mut allocator = MonotonicTimestampAllocator::default();

        let first = allocator.allocate_transaction_id().expect("first start");
        let second = allocator.allocate_transaction_id().expect("second start");
        let first_commit = allocator
            .allocate_commit_after(first)
            .expect("first commit");
        let second_commit = allocator
            .allocate_commit_after(second)
            .expect("second commit");

        assert_eq!(first.get(), 1);
        assert_eq!(second.get(), 2);
        assert!(first < second);
        assert!(first < TimestampTransactionId::new(first_commit.get()).expect("non-zero"));
        assert!(first_commit < second_commit);
    }

    #[test]
    fn commit_allocator_skips_forward_to_stay_after_start_timestamp() {
        let start = TimestampTransactionId::new(10).expect("start");
        let mut allocator = MonotonicTimestampAllocator::starting_at(3).expect("allocator");

        let commit = allocator.allocate_commit_after(start).expect("commit");

        assert_eq!(commit.get(), 11);
        assert_eq!(allocator.next_timestamp(), 12);
    }

    #[test]
    fn committed_versions_are_visible_at_and_after_commit_timestamp_only() {
        let start = TimestampTransactionId::new(5).expect("start");
        let commit = CommitTimestamp::after_start(start, 8).expect("commit");
        let version = TimestampVersionState::committed(start, commit).expect("version");

        assert!(!version.is_visible_at(ReadTimestamp::new(7).expect("before")));
        assert!(version.is_visible_at(ReadTimestamp::new(8).expect("at boundary")));
        assert!(version.is_visible_at(ReadTimestamp::new(9).expect("after")));
    }

    #[test]
    fn pending_and_aborted_versions_are_never_visible() {
        let start = TimestampTransactionId::new(5).expect("start");
        let pending = TimestampVersionState::intent(start);
        let aborted = pending.abort();
        let read = ReadTimestamp::new(100).expect("read");

        assert!(!pending.is_visible_at(read));
        assert!(!aborted.is_visible_at(read));
    }

    #[test]
    fn prewrite_commit_and_abort_control_read_ts_visibility() {
        let kv = std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let start = TimestampTransactionId::new(5).expect("start");
        let commit = CommitTimestamp::after_start(start, 8).expect("commit");
        let write = TimestampWrite {
            table_id: 11,
            rowid: 42,
            row: vec![crabka_pgtypes::Datum::Int4(7)],
            delete: false,
            global_index_intents: Vec::new(),
        };

        kv.write_batch(
            &prewrite_ops(kv.as_ref(), start, std::slice::from_ref(&write)).expect("prewrite"),
        )
        .expect("write intent");
        assert_eq!(
            read_visible_ts_row(kv.as_ref(), 11, 42, ReadTimestamp::new(100).expect("read"))
                .expect("read pending"),
            None
        );

        kv.write_batch(
            &resolve_ops(
                kv.as_ref(),
                start,
                TimestampTxnDecision::Committed(commit),
                std::slice::from_ref(&write),
            )
            .expect("commit ops"),
        )
        .expect("commit write");
        assert_eq!(
            read_visible_ts_row(kv.as_ref(), 11, 42, ReadTimestamp::new(7).expect("before"))
                .expect("read before"),
            None
        );
        assert_eq!(
            read_visible_ts_row(kv.as_ref(), 11, 42, ReadTimestamp::new(8).expect("at"))
                .expect("read at"),
            Some(vec![crabka_pgtypes::Datum::Int4(7)])
        );

        let abort_start = TimestampTransactionId::new(9).expect("abort start");
        let abort_write = TimestampWrite {
            row: vec![crabka_pgtypes::Datum::Int4(9)],
            ..write
        };
        kv.write_batch(
            &prewrite_ops(kv.as_ref(), abort_start, std::slice::from_ref(&abort_write))
                .expect("prewrite 2"),
        )
        .expect("write intent 2");
        kv.write_batch(
            &resolve_ops(
                kv.as_ref(),
                abort_start,
                TimestampTxnDecision::Aborted,
                &[abort_write],
            )
            .expect("abort ops"),
        )
        .expect("abort write");
        assert_eq!(
            read_visible_ts_row(kv.as_ref(), 11, 42, ReadTimestamp::new(100).expect("after"))
                .expect("read after abort"),
            Some(vec![crabka_pgtypes::Datum::Int4(7)])
        );
    }

    #[test]
    fn prewrite_conflict_excludes_in_doubt_intent() {
        let kv = crabka_pgkv::MemKv::new();
        let first = TimestampTransactionId::new(5).expect("first");
        let second = TimestampTransactionId::new(6).expect("second");
        let write = TimestampWrite {
            table_id: 11,
            rowid: 42,
            row: vec![crabka_pgtypes::Datum::Int4(7)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        kv.write_batch(
            &prewrite_ops(&kv, first, std::slice::from_ref(&write)).expect("first prewrite"),
        )
        .expect("write first");

        assert!(matches!(
            prewrite_ops(&kv, second, &[write]),
            Err(TimestampTxnError::WriteConflict {
                table_id: 11,
                rowid: 42,
            })
        ));
    }

    #[test]
    fn prewrite_carries_global_index_intents_with_base_write() {
        let kv = crabka_pgkv::MemKv::new();
        let start = TimestampTransactionId::new(5).expect("start");
        let write = TimestampWrite {
            table_id: 11,
            rowid: 42,
            row: vec![
                crabka_pgtypes::Datum::Int4(7),
                crabka_pgtypes::Datum::Text("a".into()),
            ],
            delete: false,
            global_index_intents: vec![GlobalIndexIntent {
                index_id: 3,
                indexed_values: vec![crabka_pgtypes::Datum::Text("a".into())],
                base_table_id: 11,
                base_rowid: 42,
                unique: true,
                delete: false,
            }],
        };

        let ops = prewrite_ops(&kv, start, std::slice::from_ref(&write)).expect("prewrite");

        assert_eq!(ops.len(), 3);
        assert!(base_index_intents_match(&[write]));
    }

    #[tokio::test]
    async fn abort_deletes_prewritten_non_unique_global_index_intents() {
        let kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter {
            kv: std::sync::Arc::clone(&kv),
        });
        let participant = TimestampTxnParticipant::new(
            std::sync::Arc::clone(&kv),
            std::sync::Arc::clone(&kv),
            committer,
            0,
        );

        let insert_start = TimestampTransactionId::new(5).expect("insert start");
        let insert_write = global_index_test_write(42, "alpha", false, false);
        participant
            .prewrite(insert_start, std::slice::from_ref(&insert_write))
            .await
            .expect("prewrite insert intent");
        assert_eq!(global_index_intent_count(kv.as_ref()), 1);

        participant
            .abort(insert_start, std::slice::from_ref(&insert_write))
            .await
            .expect("abort insert intent");

        assert_eq!(global_index_intent_count(kv.as_ref()), 0);
        assert_visible_global_index_entries(kv.as_ref(), "alpha", &[]);
    }

    #[tokio::test]
    async fn aborted_resolution_deletes_prewritten_global_index_intents() {
        let kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter {
            kv: std::sync::Arc::clone(&kv),
        });
        let participant = TimestampTxnParticipant::new(
            std::sync::Arc::clone(&kv),
            std::sync::Arc::clone(&kv),
            committer,
            0,
        );
        let start = TimestampTransactionId::new(5).expect("start");
        let write = global_index_test_write(42, "alpha", false, false);

        participant
            .prewrite(start, std::slice::from_ref(&write))
            .await
            .expect("prewrite intent");
        assert_eq!(global_index_intent_count(kv.as_ref()), 1);

        participant
            .resolve(
                start,
                TimestampTxnDecision::Aborted,
                std::slice::from_ref(&write),
            )
            .await
            .expect("resolve abort");
        participant
            .resolve(
                start,
                TimestampTxnDecision::Aborted,
                std::slice::from_ref(&write),
            )
            .await
            .expect("repeat resolve abort");

        assert_eq!(global_index_intent_count(kv.as_ref()), 0);
        assert_visible_global_index_entries(kv.as_ref(), "alpha", &[]);
    }

    #[tokio::test]
    async fn abort_keeps_existing_global_index_entries_for_update_and_delete_intents() {
        let kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter {
            kv: std::sync::Arc::clone(&kv),
        });
        let participant = TimestampTxnParticipant::new(
            std::sync::Arc::clone(&kv),
            std::sync::Arc::clone(&kv),
            committer,
            0,
        );

        let committed_start = TimestampTransactionId::new(5).expect("committed start");
        let committed_at = CommitTimestamp::after_start(committed_start, 8).expect("commit ts");
        let committed_write = global_index_test_write(42, "alpha", false, false);
        participant
            .prewrite(committed_start, std::slice::from_ref(&committed_write))
            .await
            .expect("prewrite committed row");
        participant
            .commit(
                committed_start,
                committed_at,
                std::slice::from_ref(&committed_write),
            )
            .await
            .expect("commit existing row");

        let update_start = TimestampTransactionId::new(10).expect("update start");
        let update_write = TimestampWrite {
            table_id: 11,
            rowid: 42,
            row: vec![crabka_pgtypes::Datum::Text("beta".into())],
            delete: false,
            global_index_intents: vec![
                global_index_test_intent(42, "alpha", true),
                global_index_test_intent(42, "beta", false),
            ],
        };
        participant
            .prewrite(update_start, std::slice::from_ref(&update_write))
            .await
            .expect("prewrite update intents");
        assert_eq!(global_index_intent_count(kv.as_ref()), 2);

        participant
            .abort(update_start, std::slice::from_ref(&update_write))
            .await
            .expect("abort update intents");

        assert_eq!(global_index_intent_count(kv.as_ref()), 0);
        assert_visible_global_index_entries(kv.as_ref(), "alpha", &[42]);
        assert_visible_global_index_entries(kv.as_ref(), "beta", &[]);

        let delete_start = TimestampTransactionId::new(12).expect("delete start");
        let delete_write = global_index_test_write(42, "alpha", true, true);
        participant
            .prewrite(delete_start, std::slice::from_ref(&delete_write))
            .await
            .expect("prewrite delete intent");
        assert_eq!(global_index_intent_count(kv.as_ref()), 1);

        participant
            .abort(delete_start, std::slice::from_ref(&delete_write))
            .await
            .expect("abort delete intent");

        assert_eq!(global_index_intent_count(kv.as_ref()), 0);
        assert_visible_global_index_entries(kv.as_ref(), "alpha", &[42]);
    }

    fn global_index_test_write(
        rowid: u64,
        indexed_value: &str,
        delete: bool,
        intent_delete: bool,
    ) -> TimestampWrite {
        TimestampWrite {
            table_id: 11,
            rowid,
            row: vec![crabka_pgtypes::Datum::Text(indexed_value.into())],
            delete,
            global_index_intents: vec![global_index_test_intent(
                rowid,
                indexed_value,
                intent_delete,
            )],
        }
    }

    fn global_index_test_intent(
        base_rowid: u64,
        indexed_value: &str,
        delete: bool,
    ) -> GlobalIndexIntent {
        GlobalIndexIntent {
            index_id: 3,
            indexed_values: vec![crabka_pgtypes::Datum::Text(indexed_value.into())],
            base_table_id: 11,
            base_rowid,
            unique: false,
            delete,
        }
    }

    fn global_index_intent_count(kv: &dyn crabka_pgkv::Kv) -> usize {
        kv.scan_prefix(b"\0\0\0\0index/ts_intent/")
            .expect("scan global index intents")
            .len()
    }

    fn assert_visible_global_index_entries(
        kv: &dyn crabka_pgkv::Kv,
        indexed_value: &str,
        expected_rowids: &[u64],
    ) {
        let entries = read_visible_global_index_entries(
            kv,
            3,
            &[crabka_pgtypes::Datum::Text(indexed_value.into())],
            ReadTimestamp::MAX,
        )
        .expect("read visible global index entries");
        let rowids = entries
            .into_iter()
            .map(|entry| entry.base_rowid)
            .collect::<Vec<_>>();
        assert_eq!(rowids, expected_rowids);
    }

    #[test]
    fn divergence_detector_rejects_index_intent_for_different_base_row() {
        let write = TimestampWrite {
            table_id: 11,
            rowid: 42,
            row: vec![crabka_pgtypes::Datum::Int4(7)],
            delete: false,
            global_index_intents: vec![GlobalIndexIntent {
                index_id: 3,
                indexed_values: vec![crabka_pgtypes::Datum::Int4(7)],
                base_table_id: 11,
                base_rowid: 99,
                unique: false,
                delete: false,
            }],
        };

        assert!(!base_index_intents_match(&[write]));
    }

    #[test]
    fn primary_decision_defaults_pending_then_roundtrips_terminal() {
        let kv = crabka_pgkv::MemKv::new();
        let start = TimestampTransactionId::new(5).expect("start");
        let commit = CommitTimestamp::after_start(start, 8).expect("commit");

        assert_eq!(
            read_primary_decision(&kv, start).expect("missing primary"),
            PrimaryTxnDecision::Pending
        );
        kv.write_batch(&[primary_decision_op(
            start,
            PrimaryTxnDecision::Committed(commit),
        )])
        .expect("write primary");
        assert_eq!(
            read_primary_decision(&kv, start).expect("committed primary"),
            PrimaryTxnDecision::Committed(commit)
        );
    }

    #[test]
    fn commit_timestamps_must_sort_after_transaction_start() {
        let start = TimestampTransactionId::new(5).expect("start");

        assert_eq!(
            CommitTimestamp::after_start(start, 5),
            Err(TimestampTxnError::CommitNotAfterStart {
                start_ts: 5,
                commit_ts: 5,
            })
        );
        assert_eq!(
            TimestampVersionState::committed(start, CommitTimestamp::new(4).expect("commit")),
            Err(TimestampTxnError::CommitNotAfterStart {
                start_ts: 5,
                commit_ts: 4,
            })
        );
    }

    #[test]
    fn zero_is_not_a_valid_timestamp_transaction_value() {
        assert_eq!(
            TimestampTransactionId::new(0),
            Err(TimestampTxnError::ZeroTimestamp)
        );
        assert_eq!(
            CommitTimestamp::new(0),
            Err(TimestampTxnError::ZeroTimestamp)
        );
        assert_eq!(ReadTimestamp::new(0), Err(TimestampTxnError::ZeroTimestamp));
        assert_eq!(
            MonotonicTimestampAllocator::starting_at(0),
            Err(TimestampTxnError::ZeroTimestamp)
        );
    }
}
