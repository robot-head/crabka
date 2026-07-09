//! PG-4b relation metadata redo decoding.

use bytes::Bytes;
use crabka_postgres_wal::DecodedRecord;

use crate::{RedoError, RedoOpcodeFamily, RedoRelMetaKey, RelMetaUpdate, consts_v17};

const XLOG_RELMAP_UPDATE: u8 = 0x00;
const RELMAP_HEADER_LEN: usize = 12;

pub(crate) fn decode_relmeta_action(record: &DecodedRecord) -> Result<RelMetaUpdate, RedoError> {
    let opcode = record.header.info & !consts_v17::XLR_INFO_MASK;
    if opcode != XLOG_RELMAP_UPDATE {
        return Err(RedoError::UnsupportedRedoOpcode {
            family: RedoOpcodeFamily::RelMeta,
            opcode,
            lsn: record.start_lsn,
        });
    }

    let data = record.main_data.as_ref();
    if data.len() < RELMAP_HEADER_LEN {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "relmap update header",
        });
    }

    let db_oid = read_u32(data, 0);
    let spc_oid = read_u32(data, 4);
    let nbytes = read_i32(data, 8);
    if nbytes < 0 {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "relmap update payload length is negative",
        });
    }

    let payload_len = usize::try_from(nbytes).map_err(|_| RedoError::BadRecord {
        lsn: record.start_lsn,
        context: "relmap update payload length is too large",
    })?;
    let payload = &data[RELMAP_HEADER_LEN..];
    if payload.len() != payload_len {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "relmap update payload length mismatch",
        });
    }

    Ok(RelMetaUpdate {
        key: RedoRelMetaKey::relmap(db_oid, spc_oid),
        end_lsn: record_end_lsn(record),
        bytes: Bytes::copy_from_slice(payload),
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn record_end_lsn(record: &DecodedRecord) -> crabka_postgres_wal::Lsn {
    crabka_postgres_wal::Lsn(record.start_lsn.value() + u64::from(record.total_len))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_postgres_wal::{DecodedRecord, Lsn, XLogRecordHeader};

    use super::*;
    use crate::consts_v17;

    #[test]
    fn relmap_update_decodes_to_typed_update() {
        let mut main_data = Vec::new();
        main_data.extend_from_slice(&5_u32.to_le_bytes());
        main_data.extend_from_slice(&1663_u32.to_le_bytes());
        main_data.extend_from_slice(&12_i32.to_le_bytes());
        main_data.extend_from_slice(b"relmap-bytes");

        let record = DecodedRecord {
            start_lsn: Lsn(20),
            total_len: 52,
            header: XLogRecordHeader {
                total_len: 52,
                xid: 0,
                prev_lsn: Lsn(0),
                info: XLOG_RELMAP_UPDATE,
                rmid: consts_v17::RM_RELMAP_ID,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: main_data.into_boxed_slice(),
            origin: None,
            toplevel_xid: None,
        };

        let action = decode_relmeta_action(&record);

        assert!(let Ok(update) = action);
        assert!(update.key == RedoRelMetaKey::relmap(5, 1663));
        assert!(update.end_lsn == Lsn(72));
        assert!(update.bytes.as_ref() == b"relmap-bytes");
    }

    #[test]
    fn relmap_update_keeps_payload_start_aligned_after_pg_header() {
        let payload = b"\x01\x02\x03\x04relmap-file";
        let mut main_data = Vec::new();
        main_data.extend_from_slice(&0_u32.to_le_bytes());
        main_data.extend_from_slice(&0_u32.to_le_bytes());
        let payload_len = i32::try_from(payload.len()).expect("test payload fits in relmap length");
        main_data.extend_from_slice(&payload_len.to_le_bytes());
        main_data.extend_from_slice(payload);

        let record = relmap_record(main_data);

        let action = decode_relmeta_action(&record);

        assert!(let Ok(update) = action);
        assert!(update.key == RedoRelMetaKey::relmap(0, 0));
        assert!(update.bytes.as_ref() == payload);
    }

    #[test]
    fn relmap_update_rejects_payload_length_mismatch() {
        let mut main_data = Vec::new();
        main_data.extend_from_slice(&5_u32.to_le_bytes());
        main_data.extend_from_slice(&1663_u32.to_le_bytes());
        main_data.extend_from_slice(&4_i32.to_le_bytes());
        main_data.extend_from_slice(b"abc");

        let record = relmap_record(main_data);

        let action = decode_relmeta_action(&record);

        assert!(
            action
                == Err(RedoError::BadRecord {
                    lsn: Lsn(20),
                    context: "relmap update payload length mismatch",
                })
        );
    }

    #[test]
    fn relmap_update_rejects_negative_payload_length() {
        let mut main_data = Vec::new();
        main_data.extend_from_slice(&5_u32.to_le_bytes());
        main_data.extend_from_slice(&1663_u32.to_le_bytes());
        main_data.extend_from_slice(&(-1_i32).to_le_bytes());

        let record = relmap_record(main_data);

        let action = decode_relmeta_action(&record);

        assert!(
            action
                == Err(RedoError::BadRecord {
                    lsn: Lsn(20),
                    context: "relmap update payload length is negative",
                })
        );
    }

    #[test]
    fn unsupported_relmeta_opcode_fails_loudly() {
        let record = DecodedRecord {
            start_lsn: Lsn(20),
            total_len: 24,
            header: XLogRecordHeader {
                total_len: 24,
                xid: 0,
                prev_lsn: Lsn(0),
                info: 0x30,
                rmid: consts_v17::RM_RELMAP_ID,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: Box::new([]),
            origin: None,
            toplevel_xid: None,
        };

        let action = decode_relmeta_action(&record);

        assert!(
            action
                == Err(RedoError::UnsupportedRedoOpcode {
                    family: RedoOpcodeFamily::RelMeta,
                    opcode: 0x30,
                    lsn: Lsn(20),
                })
        );
    }

    fn relmap_record(main_data: Vec<u8>) -> DecodedRecord {
        DecodedRecord {
            start_lsn: Lsn(20),
            total_len: 52,
            header: XLogRecordHeader {
                total_len: 52,
                xid: 0,
                prev_lsn: Lsn(0),
                info: XLOG_RELMAP_UPDATE,
                rmid: consts_v17::RM_RELMAP_ID,
                crc: 0,
            },
            blocks: Vec::new(),
            main_data: main_data.into_boxed_slice(),
            origin: None,
            toplevel_xid: None,
        }
    }
}
