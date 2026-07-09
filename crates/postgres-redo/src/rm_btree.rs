//! Bounded PG17 btree redo decoding.

use crabka_postgres_wal::{BlockRef, DecodedRecord};

use crate::{RedoError, UnsupportedRedoFamily, engine::BlockRedoAction};

pub(crate) fn decode_btree_action(
    record: &DecodedRecord,
    _block: &BlockRef,
) -> Result<BlockRedoAction, RedoError> {
    Err(RedoError::UnsupportedRedoFamily {
        family: UnsupportedRedoFamily::Btree,
        info: record.header.info,
        lsn: record.start_lsn,
    })
}
