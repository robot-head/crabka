//! PG-4b BRIN-index redo gate.

use crabka_postgres_wal::DecodedRecord;

use crate::{IndexRedoFamily, RedoError, engine::BlockRedoAction, index_family};

pub(crate) fn accept_brin_page_image(record: &DecodedRecord) -> Result<(), RedoError> {
    index_family::accept_index_page_image(record, IndexRedoFamily::Brin)
}

pub(crate) fn decode_brin_action(record: &DecodedRecord) -> Result<BlockRedoAction, RedoError> {
    index_family::reject_delta_index_record(record, IndexRedoFamily::Brin)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_postgres_wal::{DecodedRecord, Lsn, XLogRecordHeader};

    use super::*;
    use crate::{UnsupportedRedoFamily, consts_v17};

    #[test]
    fn brin_page_images_are_explicitly_accepted() {
        assert!(accept_brin_page_image(&record(consts_v17::RM_BRIN_ID)) == Ok(()));
    }

    #[test]
    fn brin_delta_records_fail_with_brin_family() {
        assert!(
            decode_brin_action(&record(consts_v17::RM_BRIN_ID))
                == Err(RedoError::UnsupportedRedoFamily {
                    family: UnsupportedRedoFamily::Index(IndexRedoFamily::Brin),
                    info: 0x10,
                    lsn: Lsn(30),
                })
        );
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
