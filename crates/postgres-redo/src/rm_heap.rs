//! Bounded PG17 heap redo decoding.

use crabka_postgres_wal::{BlockRef, DecodedRecord};

use crate::{RedoError, UnsupportedRedoFamily, consts_v17, engine::BlockRedoAction};

pub(crate) fn decode_heap_action(
    record: &DecodedRecord,
    block: &BlockRef,
) -> Result<BlockRedoAction, RedoError> {
    let info = record.header.info;
    if info & consts_v17::XLOG_HEAP_INIT_PAGE != 0 {
        if !block.data.is_empty() {
            return Err(unsupported_heap(record));
        }

        return Ok(BlockRedoAction::Initialize);
    }

    let opcode = info & consts_v17::XLOG_HEAP_OPMASK;
    if opcode == consts_v17::XLOG_HEAP_INSERT {
        return Err(unsupported_heap(record));
    }

    Err(unsupported_heap(record))
}

pub(crate) fn decode_heap2_action(record: &DecodedRecord) -> Result<BlockRedoAction, RedoError> {
    let opcode = record.header.info & !consts_v17::XLR_INFO_MASK;
    if opcode == consts_v17::XLOG_HEAP2_VISIBLE {
        return Ok(BlockRedoAction::SetHeapAllVisible);
    }

    Err(unsupported_heap2(record))
}

fn unsupported_heap(record: &DecodedRecord) -> RedoError {
    RedoError::UnsupportedRedoFamily {
        family: UnsupportedRedoFamily::Heap,
        info: record.header.info,
        lsn: record.start_lsn,
    }
}

fn unsupported_heap2(record: &DecodedRecord) -> RedoError {
    RedoError::UnsupportedRedoFamily {
        family: UnsupportedRedoFamily::Heap2,
        info: record.header.info,
        lsn: record.start_lsn,
    }
}
