//! Sans-IO `PostgreSQL` WAL decoding primitives.

pub mod consts_v17;
mod framing;
mod lsn;
mod record;
mod shard;

pub use self::{
    framing::{
        PageHeader, StandardPageHeader, WalDecodeError, WalStreamDecoder, parse_page_header,
        parse_page_headers,
    },
    lsn::{Lsn, LsnHalf, ParseLsnError, XLOG_BLCKSZ},
    record::{
        BlockFlags, BlockImage, BlockRef, DecodedRecord, RelFileLocator, XLOG_RECORD_HEADER_SIZE,
        XLogRecord, XLogRecordHeader, validate_record_crc,
    },
    shard::{PageKey, RelTag, Sharded, shard_record},
};
