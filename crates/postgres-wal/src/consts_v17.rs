//! `PostgreSQL` 17 WAL format constants used by the sans-IO decoder.

/// `PostgreSQL` 17 WAL page magic, stored in native little-endian order in the
/// committed fixtures.
pub const XLOG_PAGE_MAGIC: u16 = 0xD116;

/// `PostgreSQL` WAL page size in bytes.
pub const XLOG_BLCKSZ: usize = 8 * 1024;

/// Page flag: the first record fragment continues a record from the previous
/// page.
pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;

/// Page flag: the page uses the long header present at the start of a segment.
pub const XLP_LONG_HEADER: u16 = 0x0002;

/// Size in bytes of `XLogPageHeaderData`.
pub const SIZE_OF_SHORT_PHD: usize = 24;

/// Size in bytes of `XLogLongPageHeaderData`.
pub const SIZE_OF_LONG_PHD: usize = 40;
