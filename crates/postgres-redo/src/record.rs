//! Typed redo input records for the narrow redo seam.

use bytes::Bytes;
use crabka_page_store::{PAGE_SIZE, PageKey, SlruPageKey};
use crabka_postgres_wal::Lsn;
use thiserror::Error;

/// Identifies storage addressed by the redo seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RedoKey {
    /// A relation heap/index/fork page stored by `crabka-page-store` today.
    Relation(PageKey),
    /// Reserved address for PG-4b SLRU materialization.
    Slru(SlruKey),
    /// Reserved address for PG-4b relation metadata materialization.
    RelMeta(RedoRelMetaKey),
}

impl From<PageKey> for RedoKey {
    fn from(key: PageKey) -> Self {
        Self::Relation(key)
    }
}

/// Identifies one SLRU page targeted by future PG-4b redo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlruKey {
    /// SLRU family.
    pub kind: SlruKind,
    /// Segment number.
    pub segment_number: u32,
    /// Block number within the segment.
    pub block_number: u32,
}

impl From<SlruPageKey> for SlruKey {
    fn from(key: SlruPageKey) -> Self {
        Self {
            kind: match key.kind {
                crabka_page_store::SlruKind::Clog => SlruKind::Clog,
                crabka_page_store::SlruKind::MultiXactOffset => SlruKind::MultiXactOffset,
                crabka_page_store::SlruKind::MultiXactMember => SlruKind::MultiXactMember,
                crabka_page_store::SlruKind::CommitTs => SlruKind::CommitTs,
            },
            segment_number: key.segment_number,
            block_number: key.block_number,
        }
    }
}

/// SLRU families called out by the PG-4b plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SlruKind {
    /// `pg_xact`/CLOG status pages.
    Clog,
    /// Multixact offset pages.
    MultiXactOffset,
    /// Multixact member pages.
    MultiXactMember,
    /// Commit timestamp pages.
    CommitTs,
}

/// Identifies relation metadata targeted by future PG-4b redo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RedoRelMetaKey {
    /// Metadata address with PostgreSQL-compatible scope.
    pub scope: RelMetaScope,
}

impl RedoRelMetaKey {
    /// Builds a key for one `PostgreSQL` relation-map file.
    #[must_use]
    pub const fn relmap(db_oid: u32, spc_oid: u32) -> Self {
        Self {
            scope: RelMetaScope::RelMap(RelMapKey { db_oid, spc_oid }),
        }
    }
}

/// `PostgreSQL`-compatible relation metadata address spaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RelMetaScope {
    /// A relation-map file scoped by database and tablespace OIDs.
    RelMap(RelMapKey),
}

/// Identifies a `PostgreSQL` relation-map file update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelMapKey {
    /// Database OID, or zero for the shared relation map.
    pub db_oid: u32,
    /// Tablespace OID carried by `PostgreSQL` WAL for database-local relation maps.
    pub spc_oid: u32,
}

/// Long-tail redo families decoded by the scaffold but not applied yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedRedoFamily {
    /// Transaction records outside commit/abort status folding.
    Transaction,
    /// Heap rmgr records whose tuple-level payloads are not byte-exactly modeled yet.
    Heap,
    /// Heap2 rmgr records outside the decoded page-visible slice.
    Heap2,
    /// Btree rmgr records whose item/split payloads are not byte-exactly modeled yet.
    Btree,
    /// Sequence rmgr records that are not carried as exact full-page payloads.
    Sequence,
    /// SLRU-backed status storage.
    Slru(SlruKind),
    /// Relation metadata/storage-manager state.
    RelMeta,
    /// Secondary index rmgr outside the bounded btree slice.
    Index(IndexRedoFamily),
}

/// A decoded rmgr family whose opcode is outside the bounded PG-4b slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedoOpcodeFamily {
    /// SLRU-backed status storage.
    Slru(SlruKind),
    /// Relation metadata/storage-manager state.
    RelMeta,
}

/// Index-family redo families outside the bounded btree slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRedoFamily {
    /// Hash indexes.
    Hash,
    /// GIN indexes.
    Gin,
    /// `GiST` indexes.
    Gist,
    /// SP-GiST indexes.
    SpGist,
    /// BRIN indexes.
    Brin,
}

/// A typed redo input record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedoRecord {
    /// Storage key targeted by this record.
    pub key: RedoKey,
    /// End LSN of the record.
    pub end_lsn: Lsn,
    /// Narrow action implemented by this scaffold.
    pub action: RedoAction,
}

impl RedoRecord {
    /// Builds a relation-page initialization record.
    #[must_use]
    pub fn initialize_page(key: PageKey, end_lsn: Lsn, image: Bytes) -> Self {
        Self {
            key: RedoKey::Relation(key),
            end_lsn,
            action: RedoAction::InitializePage { image },
        }
    }

    /// Builds a relation full-page image record.
    #[must_use]
    pub fn full_page_image(key: PageKey, end_lsn: Lsn, image: Bytes) -> Self {
        Self {
            key: RedoKey::Relation(key),
            end_lsn,
            action: RedoAction::FullPageImage { image },
        }
    }

    /// Builds a relation byte-range patch record.
    #[must_use]
    pub fn byte_range_patch(key: PageKey, end_lsn: Lsn, offset: usize, bytes: Bytes) -> Self {
        Self {
            key: RedoKey::Relation(key),
            end_lsn,
            action: RedoAction::ByteRangePatch(ByteRangePatch { offset, bytes }),
        }
    }
}

/// Redo action variants implemented by this scaffold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedoAction {
    /// Initialize the page from a full image; no base page is required.
    InitializePage {
        /// Full page image bytes.
        image: Bytes,
    },
    /// Replace the page with a full-page image; no base page is required.
    FullPageImage {
        /// Full page image bytes.
        image: Bytes,
    },
    /// Apply a byte-range patch to an existing base page.
    ByteRangePatch(ByteRangePatch),
}

/// A byte-range patch action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRangePatch {
    /// Start offset within the page.
    pub offset: usize,
    /// Bytes to copy into the page.
    pub bytes: Bytes,
}

impl ByteRangePatch {
    /// Returns whether this patch fits in one `PostgreSQL` page.
    #[must_use]
    pub fn fits_in_page(&self) -> bool {
        patch_end(self.offset, self.bytes.len()).is_some_and(|end| end <= PAGE_SIZE)
    }
}

/// Errors returned while applying redo records.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RedoError {
    /// The record LSN is not newer than the page LSN.
    #[error(
        "stale redo record for {key:?}: page LSN {page_lsn} is at or after record LSN {record_lsn}"
    )]
    StaleRecord {
        /// Target storage key.
        key: RedoKey,
        /// Existing page LSN.
        page_lsn: Lsn,
        /// Record end LSN.
        record_lsn: Lsn,
    },
    /// A patch did not fit within the fixed page size.
    #[error(
        "byte patch for {key:?} is out of bounds: offset {offset}, len {len}, page size {page_size}"
    )]
    OutOfBoundsPatch {
        /// Target storage key.
        key: RedoKey,
        /// Patch offset.
        offset: usize,
        /// Patch length.
        len: usize,
        /// Page size.
        page_size: usize,
    },
    /// The base page key and record key differ.
    #[error("redo key mismatch: page {page_key:?}, record {record_key:?}")]
    KeyMismatch {
        /// Key carried by the base page.
        page_key: RedoKey,
        /// Key carried by the record.
        record_key: RedoKey,
    },
    /// A delta record needs a base page but none was supplied.
    #[error("missing base page for redo record {key:?} at {record_lsn}")]
    MissingBasePage {
        /// Target storage key.
        key: RedoKey,
        /// Record end LSN.
        record_lsn: Lsn,
    },
    /// A full image had the wrong byte length.
    #[error("page image for {key:?} must be exactly {expected} bytes, got {actual}")]
    WrongPageSize {
        /// Target storage key.
        key: RedoKey,
        /// Required size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },
    /// Non-relation storage is reserved for PG-4b and is refused loudly for now.
    #[error("unsupported redo storage key {key:?} at {record_lsn}")]
    UnsupportedStorageKey {
        /// Unsupported key.
        key: RedoKey,
        /// Record end LSN.
        record_lsn: Lsn,
    },
    /// The native PG17 rmgr slice does not implement this `(rmid, info)` arm.
    #[error("unsupported PostgreSQL redo rmgr {rmid} info {info:#04x} at {lsn}")]
    UnsupportedRmgr {
        /// Resource manager id.
        rmid: u8,
        /// Raw record info byte.
        info: u8,
        /// Record start LSN.
        lsn: Lsn,
    },
    /// A known PG-4b long-tail family was decoded but is not applied yet.
    #[error("unsupported PostgreSQL redo family {family:?} info {info:#04x} at {lsn}")]
    UnsupportedRedoFamily {
        /// Decoded family.
        family: UnsupportedRedoFamily,
        /// Raw record info byte.
        info: u8,
        /// Record start LSN.
        lsn: Lsn,
    },
    /// A known PG-4b rmgr family carried an opcode outside the bounded slice.
    #[error("unsupported PostgreSQL redo opcode {opcode:#04x} for {family:?} at {lsn}")]
    UnsupportedRedoOpcode {
        /// Decoded family.
        family: RedoOpcodeFamily,
        /// Resource-manager-specific opcode after masking generic info flags.
        opcode: u8,
        /// Record start LSN.
        lsn: Lsn,
    },
    /// A decoded record is malformed for the bounded native redo slice.
    #[error("bad PostgreSQL redo record at {lsn}: {context}")]
    BadRecord {
        /// Record start LSN.
        lsn: Lsn,
        /// Static explanation of the violated boundary condition.
        context: &'static str,
    },
}

pub(crate) fn patch_end(offset: usize, len: usize) -> Option<usize> {
    offset.checked_add(len)
}
