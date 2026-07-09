//! Bounded PG17 sequence redo decoding.

use crabka_page_store::PAGE_SIZE;
use crabka_postgres_wal::{BlockRef, DecodedRecord};

use crate::{RedoError, UnsupportedRedoFamily, consts_v17, engine::BlockRedoAction};

pub(crate) fn decode_seq_action(
    record: &DecodedRecord,
    block: &BlockRef,
) -> Result<BlockRedoAction, RedoError> {
    let opcode = record.header.info & !consts_v17::XLR_INFO_MASK;
    if opcode != consts_v17::XLOG_SEQ_LOG {
        return Err(unsupported_sequence(record));
    }

    if block.data.len() != PAGE_SIZE {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "sequence log record does not carry an exact full-page payload",
        });
    }

    Ok(BlockRedoAction::ReplaceWithDecodedPageData)
}

fn unsupported_sequence(record: &DecodedRecord) -> RedoError {
    RedoError::UnsupportedRedoFamily {
        family: UnsupportedRedoFamily::Sequence,
        info: record.header.info,
        lsn: record.start_lsn,
    }
}
