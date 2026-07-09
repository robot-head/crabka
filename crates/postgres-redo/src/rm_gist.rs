//! PG-4b GiST-index redo gate.

use crabka_postgres_wal::DecodedRecord;

use crate::{IndexRedoFamily, RedoError, engine::BlockRedoAction, index_family};

pub(crate) fn accept_gist_page_image(record: &DecodedRecord) -> Result<(), RedoError> {
    index_family::accept_index_page_image(record, IndexRedoFamily::Gist)
}

pub(crate) fn decode_gist_action(record: &DecodedRecord) -> Result<BlockRedoAction, RedoError> {
    index_family::reject_delta_index_record(record, IndexRedoFamily::Gist)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_postgres_wal::{DecodedRecord, Lsn, XLogRecordHeader};

    use super::*;
    use crate::{UnsupportedRedoFamily, consts_v17};

    #[test]
    fn gist_page_images_are_explicitly_accepted() {
        assert!(accept_gist_page_image(&record(consts_v17::RM_GIST_ID)) == Ok(()));
    }

    #[test]
    fn gist_delta_records_fail_with_gist_family() {
        assert!(
            decode_gist_action(&record(consts_v17::RM_GIST_ID))
                == Err(RedoError::UnsupportedRedoFamily {
                    family: UnsupportedRedoFamily::Index(IndexRedoFamily::Gist),
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
