//! PG-4b SLRU redo decoding.

use crabka_postgres_wal::DecodedRecord;

use crate::{
    ClogTransactionStatus, RedoError, RedoOpcodeFamily, SlruKind, SlruUpdate, SlruUpdateAction,
    UnsupportedRedoFamily, consts_v17, meta::slru_key_for_page,
};

const XLOG_XACT_COMMIT: u8 = 0x00;
const XLOG_XACT_ABORT: u8 = 0x20;
const XACT_XINFO_HAS_DBINFO: u32 = 0x01;
const XACT_XINFO_HAS_SUBXACTS: u32 = 0x02;
const CLOG_ZERO_PAGE: u8 = 0x00;
const CLOG_TRUNCATE: u8 = 0x10;
const MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
const MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
const MULTIXACT_TRUNCATE: u8 = 0x30;
const COMMIT_TS_ZERO_PAGE: u8 = 0x00;
const COMMIT_TS_TRUNCATE: u8 = 0x10;
const CLOG_TRUNCATE_RECORD_LEN: usize = 16;
const COMMIT_TS_TRUNCATE_RECORD_LEN: usize = 12;
const MULTIXACT_TRUNCATE_RECORD_LEN: usize = 20;
const POSTGRES_PAGE_SIZE: u32 = 8192;
const MULTIXACT_OFFSETS_PER_PAGE: u32 = POSTGRES_PAGE_SIZE / 4;
const MULTIXACT_MEMBERS_PER_PAGE: u32 = (POSTGRES_PAGE_SIZE / 20) * 4;

pub(crate) fn decode_slru_action(record: &DecodedRecord) -> Result<SlruUpdate, RedoError> {
    if record.header.rmid == consts_v17::RM_XACT_ID {
        return decode_xact_status_action(record);
    }

    let family = slru_family(record.header.rmid)?;
    let opcode = record.header.info & !consts_v17::XLR_INFO_MASK;
    let parsed_action = parse_slru_action(family, opcode, record)?;
    let key = slru_key_for_page(parsed_action.key_kind, parsed_action.key_page_number);

    Ok(SlruUpdate {
        key,
        end_lsn: record_end_lsn(record),
        action: parsed_action.action,
    })
}

fn decode_xact_status_action(record: &DecodedRecord) -> Result<SlruUpdate, RedoError> {
    let opcode = record.header.info & consts_v17::XLOG_XACT_OPMASK;
    let status = match opcode {
        XLOG_XACT_COMMIT => ClogTransactionStatus::Committed,
        XLOG_XACT_ABORT => ClogTransactionStatus::Aborted,
        _ => {
            return Err(RedoError::UnsupportedRedoFamily {
                family: UnsupportedRedoFamily::Transaction,
                info: record.header.info,
                lsn: record.start_lsn,
            });
        }
    };

    let has_xinfo = record.header.info & consts_v17::XLOG_XACT_HAS_INFO != 0;
    let xids = parse_xact_status_xids(record, has_xinfo)?;
    let Some(&first_xid) = xids.first() else {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "xact status record has no transaction ids",
        });
    };

    Ok(SlruUpdate {
        key: crate::meta::slru_key_for_page(SlruKind::Clog, clog_page_number_for_xid(first_xid)),
        end_lsn: record_end_lsn(record),
        action: SlruUpdateAction::SetTransactionStatus { status, xids },
    })
}

fn parse_xact_status_xids(
    record: &DecodedRecord,
    has_xinfo: bool,
) -> Result<Box<[u32]>, RedoError> {
    const XACT_TIME_LEN: usize = 8;
    const XINFO_LEN: usize = 4;
    const DBINFO_LEN: usize = 8;
    const SUBXACT_COUNT_LEN: usize = 4;

    if record.header.xid == 0 {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "xact status record header xid is invalid",
        });
    }

    let data = record.main_data.as_ref();
    reject_short_xact_time(data, record.start_lsn)?;

    let mut xids = Vec::with_capacity(1);
    xids.push(record.header.xid);
    if !has_xinfo {
        return Ok(xids.into_boxed_slice());
    }

    let xinfo_offset = XACT_TIME_LEN;
    let xinfo = read_u32_at(data, xinfo_offset, record.start_lsn, "xact status xinfo")?;
    let mut cursor = XACT_TIME_LEN + XINFO_LEN;
    if xinfo & XACT_XINFO_HAS_DBINFO != 0 {
        cursor = checked_advance(
            cursor,
            DBINFO_LEN,
            data.len(),
            record.start_lsn,
            "xact dbinfo",
        )?;
    }

    if xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
        let count = read_i32_at(data, cursor, record.start_lsn, "xact subxact count")?;
        let count = usize::try_from(count).map_err(|_| RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "xact subxact count is negative",
        })?;
        cursor = checked_advance(
            cursor,
            SUBXACT_COUNT_LEN,
            data.len(),
            record.start_lsn,
            "xact subxacts",
        )?;
        let bytes_len = count.checked_mul(4).ok_or(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "xact subxact array is too large",
        })?;
        checked_advance(
            cursor,
            bytes_len,
            data.len(),
            record.start_lsn,
            "xact subxacts",
        )?;
        xids.reserve(count);
        for index in 0..count {
            let offset = cursor + index * 4;
            xids.push(read_u32_at(
                data,
                offset,
                record.start_lsn,
                "xact subxacts",
            )?);
        }
    }

    Ok(xids.into_boxed_slice())
}

fn reject_short_xact_time(data: &[u8], lsn: crabka_postgres_wal::Lsn) -> Result<(), RedoError> {
    if data.len() < 8 {
        return Err(RedoError::BadRecord {
            lsn,
            context: "xact status timestamp",
        });
    }

    Ok(())
}

fn slru_family(rmid: u8) -> Result<SlruKind, RedoError> {
    match rmid {
        consts_v17::RM_CLOG_ID => Ok(SlruKind::Clog),
        consts_v17::RM_MULTIXACT_ID => Ok(SlruKind::MultiXactMember),
        consts_v17::RM_COMMIT_TS_ID => Ok(SlruKind::CommitTs),
        rmid => Err(RedoError::UnsupportedRmgr {
            rmid,
            info: 0,
            lsn: crabka_postgres_wal::Lsn(0),
        }),
    }
}

fn slru_update_family(
    family: SlruKind,
    opcode: u8,
    record: &DecodedRecord,
) -> Result<SlruKind, RedoError> {
    match (family, opcode) {
        (SlruKind::MultiXactMember, MULTIXACT_ZERO_OFF_PAGE) => Ok(SlruKind::MultiXactOffset),
        (SlruKind::MultiXactMember, MULTIXACT_ZERO_MEM_PAGE | MULTIXACT_TRUNCATE) => {
            Ok(SlruKind::MultiXactMember)
        }
        (SlruKind::Clog, CLOG_ZERO_PAGE | CLOG_TRUNCATE)
        | (SlruKind::CommitTs, COMMIT_TS_ZERO_PAGE | COMMIT_TS_TRUNCATE) => Ok(family),
        _ => Err(RedoError::UnsupportedRedoOpcode {
            family: RedoOpcodeFamily::Slru(family),
            opcode,
            lsn: record.start_lsn,
        }),
    }
}

fn parse_slru_action(
    family: SlruKind,
    opcode: u8,
    record: &DecodedRecord,
) -> Result<ParsedSlruAction, RedoError> {
    match (family, opcode) {
        (SlruKind::Clog, CLOG_ZERO_PAGE)
        | (SlruKind::MultiXactMember, MULTIXACT_ZERO_OFF_PAGE | MULTIXACT_ZERO_MEM_PAGE)
        | (SlruKind::CommitTs, COMMIT_TS_ZERO_PAGE) => {
            let page_number = read_u32(
                record.main_data.as_ref(),
                record.start_lsn,
                "SLRU page number",
            )?;
            Ok(ParsedSlruAction {
                key_kind: slru_update_family(family, opcode, record)?,
                key_page_number: page_number,
                action: SlruUpdateAction::ZeroPage,
            })
        }
        (SlruKind::Clog, CLOG_TRUNCATE) => {
            let cutoff_page =
                read_truncate_page(record, CLOG_TRUNCATE_RECORD_LEN, "CLOG truncate record")?;
            Ok(ParsedSlruAction {
                key_kind: SlruKind::Clog,
                key_page_number: cutoff_page,
                action: SlruUpdateAction::TruncateBefore { cutoff_page },
            })
        }
        (SlruKind::CommitTs, COMMIT_TS_TRUNCATE) => {
            let cutoff_page = read_truncate_page(
                record,
                COMMIT_TS_TRUNCATE_RECORD_LEN,
                "CommitTs truncate record",
            )?;
            Ok(ParsedSlruAction {
                key_kind: SlruKind::CommitTs,
                key_page_number: cutoff_page,
                action: SlruUpdateAction::TruncateBefore { cutoff_page },
            })
        }
        (SlruKind::MultiXactMember, MULTIXACT_TRUNCATE) => {
            let (offset_cutoff_page, member_cutoff_page) = read_multixact_truncate(record)?;
            Ok(ParsedSlruAction {
                key_kind: SlruKind::MultiXactMember,
                key_page_number: member_cutoff_page,
                action: SlruUpdateAction::TruncateMultiXact {
                    offset_cutoff_page,
                    member_cutoff_page,
                },
            })
        }
        _ => Err(RedoError::UnsupportedRedoOpcode {
            family: RedoOpcodeFamily::Slru(family),
            opcode,
            lsn: record.start_lsn,
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSlruAction {
    key_kind: SlruKind,
    key_page_number: u32,
    action: SlruUpdateAction,
}

fn read_truncate_page(
    record: &DecodedRecord,
    expected_len: usize,
    context: &'static str,
) -> Result<u32, RedoError> {
    reject_truncate_shape(record, expected_len, context)?;
    read_i64_page(record.main_data.as_ref(), record.start_lsn, context)
}

fn read_multixact_truncate(record: &DecodedRecord) -> Result<(u32, u32), RedoError> {
    reject_truncate_shape(
        record,
        MULTIXACT_TRUNCATE_RECORD_LEN,
        "MultiXact truncate record",
    )?;
    let end_trunc_off = read_u32_at(
        record.main_data.as_ref(),
        8,
        record.start_lsn,
        "MultiXact truncate record",
    )?;
    let end_trunc_memb = read_u32_at(
        record.main_data.as_ref(),
        16,
        record.start_lsn,
        "MultiXact truncate record",
    )?;

    Ok((
        end_trunc_off / MULTIXACT_OFFSETS_PER_PAGE,
        end_trunc_memb / MULTIXACT_MEMBERS_PER_PAGE,
    ))
}

fn reject_truncate_shape(
    record: &DecodedRecord,
    expected_len: usize,
    context: &'static str,
) -> Result<(), RedoError> {
    if !record.blocks.is_empty() {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context,
        });
    }

    if record.main_data.len() != expected_len {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context,
        });
    }

    Ok(())
}

fn read_i64_page(
    bytes: &[u8],
    lsn: crabka_postgres_wal::Lsn,
    context: &'static str,
) -> Result<u32, RedoError> {
    let Some(raw) = bytes.get(0..8) else {
        return Err(RedoError::BadRecord { lsn, context });
    };
    let page_number = i64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]);
    u32::try_from(page_number).map_err(|_| RedoError::BadRecord { lsn, context })
}

fn read_u32(
    bytes: &[u8],
    lsn: crabka_postgres_wal::Lsn,
    context: &'static str,
) -> Result<u32, RedoError> {
    let Some(raw) = bytes.get(0..4) else {
        return Err(RedoError::BadRecord { lsn, context });
    };

    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_u32_at(
    bytes: &[u8],
    offset: usize,
    lsn: crabka_postgres_wal::Lsn,
    context: &'static str,
) -> Result<u32, RedoError> {
    let Some(raw) = bytes.get(offset..offset + 4) else {
        return Err(RedoError::BadRecord { lsn, context });
    };

    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_i32_at(
    bytes: &[u8],
    offset: usize,
    lsn: crabka_postgres_wal::Lsn,
    context: &'static str,
) -> Result<i32, RedoError> {
    let Some(raw) = bytes.get(offset..offset + 4) else {
        return Err(RedoError::BadRecord { lsn, context });
    };

    Ok(i32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn checked_advance(
    cursor: usize,
    len: usize,
    data_len: usize,
    lsn: crabka_postgres_wal::Lsn,
    context: &'static str,
) -> Result<usize, RedoError> {
    let Some(end) = cursor.checked_add(len) else {
        return Err(RedoError::BadRecord { lsn, context });
    };

    if end > data_len {
        return Err(RedoError::BadRecord { lsn, context });
    }

    Ok(end)
}

const fn clog_page_number_for_xid(xid: u32) -> u32 {
    const CLOG_XACTS_PER_PAGE: u32 = 8192 * 4;

    xid / CLOG_XACTS_PER_PAGE
}

fn record_end_lsn(record: &DecodedRecord) -> crabka_postgres_wal::Lsn {
    crabka_postgres_wal::Lsn(record.start_lsn.value() + u64::from(record.total_len))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_postgres_wal::{DecodedRecord, Lsn, XLogRecordHeader};

    use super::*;
    use crate::{MetadataState, MetadataUpdate};

    #[test]
    fn clog_zero_page_decodes_to_typed_update() {
        let record = DecodedRecord {
            start_lsn: Lsn(10),
            total_len: 24,
            header: XLogRecordHeader {
                total_len: 24,
                xid: 0,
                prev_lsn: Lsn(0),
                info: CLOG_ZERO_PAGE,
                rmid: consts_v17::RM_CLOG_ID,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: Box::new(33_u32.to_le_bytes()),
            origin: None,
            toplevel_xid: None,
        };

        let action = decode_slru_action(&record);

        assert!(
            action
                == Ok(SlruUpdate {
                    key: crate::SlruKey {
                        kind: SlruKind::Clog,
                        segment_number: 1,
                        block_number: 1,
                    },
                    end_lsn: Lsn(34),
                    action: SlruUpdateAction::ZeroPage,
                })
        );
    }

    #[test]
    fn unsupported_slru_opcode_fails_loudly() {
        let record = DecodedRecord {
            start_lsn: Lsn(10),
            total_len: 24,
            header: XLogRecordHeader {
                total_len: 24,
                xid: 0,
                prev_lsn: Lsn(0),
                info: 0x70,
                rmid: consts_v17::RM_CLOG_ID,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: Box::new(0_u32.to_le_bytes()),
            origin: None,
            toplevel_xid: None,
        };

        let action = decode_slru_action(&record);

        assert!(
            action
                == Err(RedoError::UnsupportedRedoOpcode {
                    family: RedoOpcodeFamily::Slru(SlruKind::Clog),
                    opcode: 0x70,
                    lsn: Lsn(10),
                })
        );
    }

    #[test]
    fn xact_simple_commit_decodes_header_xid_to_clog_status_update() {
        let record = xact_record(XLOG_XACT_COMMIT, 100, xact_simple_main_data());

        let action = decode_slru_action(&record);

        assert!(
            action
                == Ok(SlruUpdate {
                    key: crate::SlruKey {
                        kind: SlruKind::Clog,
                        segment_number: 0,
                        block_number: 0,
                    },
                    end_lsn: Lsn(34),
                    action: SlruUpdateAction::SetTransactionStatus {
                        status: ClogTransactionStatus::Committed,
                        xids: Box::from([100]),
                    },
                })
        );
    }

    #[test]
    fn xact_simple_abort_decodes_header_xid_to_clog_status_update() {
        let record = xact_record(XLOG_XACT_ABORT, 7, xact_simple_main_data());

        let action = decode_slru_action(&record);

        assert!(
            action
                == Ok(SlruUpdate {
                    key: crate::SlruKey {
                        kind: SlruKind::Clog,
                        segment_number: 0,
                        block_number: 0,
                    },
                    end_lsn: Lsn(34),
                    action: SlruUpdateAction::SetTransactionStatus {
                        status: ClogTransactionStatus::Aborted,
                        xids: Box::from([7]),
                    },
                })
        );
    }

    #[test]
    fn xact_commit_with_info_decodes_main_and_subxids_to_clog_status_update() {
        let record = xact_record(
            XLOG_XACT_COMMIT | consts_v17::XLOG_XACT_HAS_INFO,
            100,
            xact_main_data(
                XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SUBXACTS,
                &[101, 32_768],
            ),
        );

        let action = decode_slru_action(&record);

        assert!(
            action
                == Ok(SlruUpdate {
                    key: crate::SlruKey {
                        kind: SlruKind::Clog,
                        segment_number: 0,
                        block_number: 0,
                    },
                    end_lsn: Lsn(34),
                    action: SlruUpdateAction::SetTransactionStatus {
                        status: ClogTransactionStatus::Committed,
                        xids: Box::from([100, 101, 32_768]),
                    },
                })
        );
    }

    #[test]
    fn xact_abort_with_info_decodes_subxids_to_aborted_clog_status_update() {
        let record = xact_record(
            XLOG_XACT_ABORT | consts_v17::XLOG_XACT_HAS_INFO,
            7,
            xact_main_data(XACT_XINFO_HAS_SUBXACTS, &[8, 9]),
        );

        let action = decode_slru_action(&record);

        assert!(
            action
                == Ok(SlruUpdate {
                    key: crate::SlruKey {
                        kind: SlruKind::Clog,
                        segment_number: 0,
                        block_number: 0,
                    },
                    end_lsn: Lsn(34),
                    action: SlruUpdateAction::SetTransactionStatus {
                        status: ClogTransactionStatus::Aborted,
                        xids: Box::from([7, 8, 9]),
                    },
                })
        );
    }

    #[test]
    fn malformed_xact_has_info_without_xinfo_fails_loudly() {
        let record = xact_record(
            XLOG_XACT_COMMIT | consts_v17::XLOG_XACT_HAS_INFO,
            100,
            xact_simple_main_data(),
        );

        let action = decode_slru_action(&record);

        assert!(
            action
                == Err(RedoError::BadRecord {
                    lsn: Lsn(10),
                    context: "xact status xinfo",
                })
        );
    }

    #[test]
    fn malformed_xact_has_info_truncated_subxid_array_fails_loudly() {
        let mut main_data = xact_main_data(XACT_XINFO_HAS_SUBXACTS, &[8, 9]);
        main_data.pop();
        let record = xact_record(
            XLOG_XACT_ABORT | consts_v17::XLOG_XACT_HAS_INFO,
            7,
            main_data,
        );

        let action = decode_slru_action(&record);

        assert!(
            action
                == Err(RedoError::BadRecord {
                    lsn: Lsn(10),
                    context: "xact subxacts",
                })
        );
    }

    #[test]
    fn xact_opcode_with_has_info_is_masked_before_status_matching() {
        let record = xact_record(
            XLOG_XACT_COMMIT | consts_v17::XLOG_XACT_HAS_INFO,
            100,
            xact_main_data(0, &[]),
        );

        let action = decode_slru_action(&record);

        assert!(
            action
                == Ok(SlruUpdate {
                    key: crate::SlruKey {
                        kind: SlruKind::Clog,
                        segment_number: 0,
                        block_number: 0,
                    },
                    end_lsn: Lsn(34),
                    action: SlruUpdateAction::SetTransactionStatus {
                        status: ClogTransactionStatus::Committed,
                        xids: Box::from([100]),
                    },
                })
        );
    }

    #[test]
    fn unsupported_xact_opcode_fails_loudly() {
        let record = xact_record(
            0x50 | consts_v17::XLOG_XACT_HAS_INFO,
            7,
            xact_main_data(0, &[]),
        );

        let action = decode_slru_action(&record);

        assert!(
            action
                == Err(RedoError::UnsupportedRedoFamily {
                    family: UnsupportedRedoFamily::Transaction,
                    info: 0x50 | consts_v17::XLOG_XACT_HAS_INFO,
                    lsn: Lsn(10),
                })
        );
    }

    #[test]
    fn malformed_clog_truncate_fails_without_applying() {
        let key = slru_key_for_page(SlruKind::Clog, 1);
        let state = state_with_zero_page(key);
        let record = slru_record(consts_v17::RM_CLOG_ID, CLOG_TRUNCATE, &[2, 0, 0, 0]);

        let decoded = decode_slru_action(&record);

        assert!(
            decoded
                == Err(RedoError::BadRecord {
                    lsn: Lsn(10),
                    context: "CLOG truncate record",
                })
        );
        assert!(state.slru_page(key).is_some());
    }

    #[test]
    fn malformed_multixact_truncate_fails_without_applying() {
        let offset_key = slru_key_for_page(SlruKind::MultiXactOffset, 1);
        let member_key = slru_key_for_page(SlruKind::MultiXactMember, 1);
        let state = state_with_zero_pages([offset_key, member_key]);
        let record = slru_record(
            consts_v17::RM_MULTIXACT_ID,
            MULTIXACT_TRUNCATE,
            &[2, 0, 0, 0],
        );

        let decoded = decode_slru_action(&record);

        assert!(
            decoded
                == Err(RedoError::BadRecord {
                    lsn: Lsn(10),
                    context: "MultiXact truncate record",
                })
        );
        assert!(state.slru_page(offset_key).is_some());
        assert!(state.slru_page(member_key).is_some());
    }

    #[test]
    fn malformed_commit_ts_truncate_fails_without_applying() {
        let key = slru_key_for_page(SlruKind::CommitTs, 1);
        let state = state_with_zero_page(key);
        let record = slru_record(
            consts_v17::RM_COMMIT_TS_ID,
            COMMIT_TS_TRUNCATE,
            &[2, 0, 0, 0],
        );

        let decoded = decode_slru_action(&record);

        assert!(
            decoded
                == Err(RedoError::BadRecord {
                    lsn: Lsn(10),
                    context: "CommitTs truncate record",
                })
        );
        assert!(state.slru_page(key).is_some());
    }

    #[test]
    fn valid_clog_truncate_decodes_and_hides_only_clog() {
        let clog_before = slru_key_for_page(SlruKind::Clog, 1);
        let clog_after = slru_key_for_page(SlruKind::Clog, 4);
        let commit_ts_before = slru_key_for_page(SlruKind::CommitTs, 1);
        let mut state = state_with_zero_pages([clog_before, clog_after, commit_ts_before]);
        let record = slru_record(
            consts_v17::RM_CLOG_ID,
            CLOG_TRUNCATE,
            &clog_truncate_record(3),
        );

        let decoded = decode_slru_action(&record);
        assert!(let Ok(update) = decoded);
        let applied = state.apply_update(MetadataUpdate::Slru(update));

        assert!(applied == Ok(()));
        assert!(state.slru_page(clog_before).is_none());
        assert!(state.slru_page(clog_after).is_some());
        assert!(state.slru_page(commit_ts_before).is_some());
    }

    #[test]
    fn valid_multixact_truncate_decodes_and_hides_offsets_and_members() {
        let offset_before = slru_key_for_page(SlruKind::MultiXactOffset, 1);
        let offset_after = slru_key_for_page(SlruKind::MultiXactOffset, 4);
        let member_before = slru_key_for_page(SlruKind::MultiXactMember, 2);
        let member_after = slru_key_for_page(SlruKind::MultiXactMember, 5);
        let clog_before = slru_key_for_page(SlruKind::Clog, 1);
        let mut state = state_with_zero_pages([
            offset_before,
            offset_after,
            member_before,
            member_after,
            clog_before,
        ]);
        let record = slru_record(
            consts_v17::RM_MULTIXACT_ID,
            MULTIXACT_TRUNCATE,
            &multixact_truncate_record(
                3 * MULTIXACT_OFFSETS_PER_PAGE,
                4 * MULTIXACT_MEMBERS_PER_PAGE,
            ),
        );

        let decoded = decode_slru_action(&record);
        assert!(let Ok(update) = decoded);
        let applied = state.apply_update(MetadataUpdate::Slru(update));

        assert!(applied == Ok(()));
        assert!(state.slru_page(offset_before).is_none());
        assert!(state.slru_page(offset_after).is_some());
        assert!(state.slru_page(member_before).is_none());
        assert!(state.slru_page(member_after).is_some());
        assert!(state.slru_page(clog_before).is_some());
    }

    #[test]
    fn valid_commit_ts_truncate_decodes_and_hides_only_commit_ts() {
        let commit_ts_before = slru_key_for_page(SlruKind::CommitTs, 1);
        let commit_ts_after = slru_key_for_page(SlruKind::CommitTs, 4);
        let clog_before = slru_key_for_page(SlruKind::Clog, 1);
        let mut state = state_with_zero_pages([commit_ts_before, commit_ts_after, clog_before]);
        let record = slru_record(
            consts_v17::RM_COMMIT_TS_ID,
            COMMIT_TS_TRUNCATE,
            &commit_ts_truncate_record(3),
        );

        let decoded = decode_slru_action(&record);
        assert!(let Ok(update) = decoded);
        let applied = state.apply_update(MetadataUpdate::Slru(update));

        assert!(applied == Ok(()));
        assert!(state.slru_page(commit_ts_before).is_none());
        assert!(state.slru_page(commit_ts_after).is_some());
        assert!(state.slru_page(clog_before).is_some());
    }

    fn slru_record(rmid: u8, info: u8, main_data: &[u8]) -> DecodedRecord {
        DecodedRecord {
            start_lsn: Lsn(10),
            total_len: 24,
            header: XLogRecordHeader {
                total_len: 24,
                xid: 0,
                prev_lsn: Lsn(0),
                info,
                rmid,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: main_data.into(),
            origin: None,
            toplevel_xid: None,
        }
    }

    fn xact_record(info: u8, xid: u32, main_data: Vec<u8>) -> DecodedRecord {
        DecodedRecord {
            start_lsn: Lsn(10),
            total_len: 24,
            header: XLogRecordHeader {
                total_len: 24,
                xid,
                prev_lsn: Lsn(0),
                info,
                rmid: consts_v17::RM_XACT_ID,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: main_data.into_boxed_slice(),
            origin: None,
            toplevel_xid: None,
        }
    }

    fn xact_main_data(xinfo: u32, subxids: &[u32]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&123_i64.to_le_bytes());
        bytes.extend_from_slice(&xinfo.to_le_bytes());
        if xinfo & XACT_XINFO_HAS_DBINFO != 0 {
            bytes.extend_from_slice(&5_u32.to_le_bytes());
            bytes.extend_from_slice(&1663_u32.to_le_bytes());
        }
        if xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
            let count = i32::try_from(subxids.len()).expect("test subxid count fits i32");
            bytes.extend_from_slice(&count.to_le_bytes());
            for &subxid in subxids {
                bytes.extend_from_slice(&subxid.to_le_bytes());
            }
        }
        bytes
    }

    fn xact_simple_main_data() -> Vec<u8> {
        123_i64.to_le_bytes().to_vec()
    }

    fn clog_truncate_record(cutoff_page: u32) -> [u8; CLOG_TRUNCATE_RECORD_LEN] {
        let mut bytes = [0_u8; CLOG_TRUNCATE_RECORD_LEN];
        bytes[0..8].copy_from_slice(&i64::from(cutoff_page).to_le_bytes());
        bytes[8..12].copy_from_slice(&11_u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&12_u32.to_le_bytes());
        bytes
    }

    fn commit_ts_truncate_record(cutoff_page: u32) -> [u8; COMMIT_TS_TRUNCATE_RECORD_LEN] {
        let mut bytes = [0_u8; COMMIT_TS_TRUNCATE_RECORD_LEN];
        bytes[0..8].copy_from_slice(&i64::from(cutoff_page).to_le_bytes());
        bytes[8..12].copy_from_slice(&11_u32.to_le_bytes());
        bytes
    }

    fn multixact_truncate_record(
        end_offset: u32,
        end_member: u32,
    ) -> [u8; MULTIXACT_TRUNCATE_RECORD_LEN] {
        let mut bytes = [0_u8; MULTIXACT_TRUNCATE_RECORD_LEN];
        bytes[0..4].copy_from_slice(&12_u32.to_le_bytes());
        bytes[4..8].copy_from_slice(&11_u32.to_le_bytes());
        bytes[8..12].copy_from_slice(&end_offset.to_le_bytes());
        bytes[12..16].copy_from_slice(&22_u32.to_le_bytes());
        bytes[16..20].copy_from_slice(&end_member.to_le_bytes());
        bytes
    }

    fn state_with_zero_page(key: crate::SlruKey) -> MetadataState {
        state_with_zero_pages([key])
    }

    fn state_with_zero_pages<const N: usize>(keys: [crate::SlruKey; N]) -> MetadataState {
        let mut state = MetadataState::default();
        for key in keys {
            state
                .apply_update(MetadataUpdate::Slru(SlruUpdate {
                    key,
                    end_lsn: Lsn(5),
                    action: SlruUpdateAction::ZeroPage,
                }))
                .expect("test fixture uses fresh metadata keys");
        }
        state
    }
}
