//! PG-4b hash-index redo gate.

use crabka_postgres_wal::DecodedRecord;

use crate::{IndexRedoFamily, RedoError, engine::BlockRedoAction, index_family};

pub(crate) fn accept_hash_page_image(record: &DecodedRecord) -> Result<(), RedoError> {
    index_family::accept_index_page_image(record, IndexRedoFamily::Hash)
}

pub(crate) fn decode_hash_action(record: &DecodedRecord) -> Result<BlockRedoAction, RedoError> {
    index_family::reject_delta_index_record(record, IndexRedoFamily::Hash)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_postgres_wal::{DecodedRecord, Lsn, XLogRecordHeader};

    use super::*;
    use crate::{UnsupportedRedoFamily, consts_v17};

    #[test]
    fn hash_page_images_are_explicitly_accepted() {
        assert!(accept_hash_page_image(&record(consts_v17::RM_HASH_ID)) == Ok(()));
    }

    #[test]
    fn hash_delta_records_fail_with_hash_family() {
        assert!(
            decode_hash_action(&record(consts_v17::RM_HASH_ID))
                == Err(RedoError::UnsupportedRedoFamily {
                    family: UnsupportedRedoFamily::Index(IndexRedoFamily::Hash),
                    info: 0x10,
                    lsn: Lsn(30),
                })
        );
    }

    #[test]
    fn hash_gate_rejects_wrong_index_family() {
        assert!(let Err(RedoError::BadRecord { context, .. }) = accept_hash_page_image(&record(consts_v17::RM_GIN_ID)));
        assert!(context == "index rmgr did not match index-family page-image path");
    }

    fn record(rmid: u8) -> DecodedRecord {
        DecodedRecord {
            start_lsn: Lsn(30),
            total_len: 24,
            header: XLogRecordHeader {
                total_len: 24,
                xid: 0,
                prev_lsn: Lsn(0),
                info: 0x10,
                rmid,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: Box::new([]),
            origin: None,
            toplevel_xid: None,
        }
    }
}
