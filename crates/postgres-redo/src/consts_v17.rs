//! `PostgreSQL` 17 redo constants for the bounded rmgr slice.

/// `PostgreSQL` page size in bytes.
pub const BLCKSZ: usize = 8 * 1024;

/// WAL record info flag mask reserved by the generic xlog record header.
pub const XLR_INFO_MASK: u8 = 0x0F;
/// XACT operation bits after generic WAL flags are removed; excludes xact info flags.
pub const XLOG_XACT_OPMASK: u8 = 0x70;
/// XACT record flag indicating an `xl_xact_xinfo` word follows the timestamp payload.
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

/// XLOG rmgr id.
pub const RM_XLOG_ID: u8 = 0;
/// Transaction commit/abort rmgr id.
pub const RM_XACT_ID: u8 = 1;
/// HEAP2 rmgr id.
pub const RM_HEAP2_ID: u8 = 9;
/// CLOG/`pg_xact` rmgr id.
pub const RM_CLOG_ID: u8 = 3;
/// Multixact rmgr id.
pub const RM_MULTIXACT_ID: u8 = 6;
/// Relation map metadata rmgr id.
pub const RM_RELMAP_ID: u8 = 7;
/// HEAP rmgr id.
pub const RM_HEAP_ID: u8 = 10;
/// BTREE rmgr id.
pub const RM_BTREE_ID: u8 = 11;
/// HASH rmgr id.
pub const RM_HASH_ID: u8 = 12;
/// GIN rmgr id, used by tests to prove loud refusal.
pub const RM_GIN_ID: u8 = 13;
/// `GiST` rmgr id.
pub const RM_GIST_ID: u8 = 14;
/// SEQ rmgr id.
pub const RM_SEQ_ID: u8 = 15;
/// SP-GiST rmgr id.
pub const RM_SPGIST_ID: u8 = 16;
/// BRIN rmgr id.
pub const RM_BRIN_ID: u8 = 17;
/// Commit timestamp SLRU rmgr id.
pub const RM_COMMIT_TS_ID: u8 = 18;

/// XLOG full-page image record.
pub const XLOG_FPI: u8 = 0xB0;
/// XLOG full-page image-for-hint record.
pub const XLOG_FPI_FOR_HINT: u8 = 0xC0;

/// HEAP operation bits.
pub const XLOG_HEAP_OPMASK: u8 = 0x70;
/// HEAP insert operation; supported here only when block redo initializes the page.
pub const XLOG_HEAP_INSERT: u8 = 0x00;
/// HEAP record flag indicating redo initializes the target page.
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

/// HEAP2 visible operation. This bounded slice sets `PD_ALL_VISIBLE` on the heap page.
pub const XLOG_HEAP2_VISIBLE: u8 = 0x10;

/// BTREE operation bits.
pub const XLOG_BTREE_OPMASK: u8 = 0xF0;
/// BTREE leaf insert operation; delta payloads are refused until byte-exact index redo exists.
pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x00;

/// SEQ log operation.
pub const XLOG_SEQ_LOG: u8 = 0x00;
