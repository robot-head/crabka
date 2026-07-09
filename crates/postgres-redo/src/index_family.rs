//! Shared PG-4b index-family redo rejection helpers.

use crabka_postgres_wal::DecodedRecord;

use crate::{
    IndexRedoFamily, RedoError, UnsupportedRedoFamily, consts_v17, engine::BlockRedoAction,
};

pub(crate) fn accept_index_page_image(
    record: &DecodedRecord,
    expected_family: IndexRedoFamily,
) -> Result<(), RedoError> {
    let decoded_family = index_family(record.header.rmid, record)?;
    if decoded_family != expected_family {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "index rmgr did not match index-family page-image path",
        });
    }

    Ok(())
}

pub(crate) fn reject_delta_index_record(
    record: &DecodedRecord,
    expected_family: IndexRedoFamily,
) -> Result<BlockRedoAction, RedoError> {
    let decoded_family = index_family(record.header.rmid, record)?;
    if decoded_family != expected_family {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "index rmgr did not match index-family delta path",
        });
    }

    Err(RedoError::UnsupportedRedoFamily {
        family: UnsupportedRedoFamily::Index(expected_family),
        info: record.header.info,
        lsn: record.start_lsn,
    })
}

fn index_family(rmid: u8, record: &DecodedRecord) -> Result<IndexRedoFamily, RedoError> {
    match rmid {
        consts_v17::RM_HASH_ID => Ok(IndexRedoFamily::Hash),
        consts_v17::RM_GIN_ID => Ok(IndexRedoFamily::Gin),
        consts_v17::RM_GIST_ID => Ok(IndexRedoFamily::Gist),
        consts_v17::RM_SPGIST_ID => Ok(IndexRedoFamily::SpGist),
        consts_v17::RM_BRIN_ID => Ok(IndexRedoFamily::Brin),
        rmid => Err(RedoError::UnsupportedRmgr {
            rmid,
            info: record.header.info,
            lsn: record.start_lsn,
        }),
    }
}
