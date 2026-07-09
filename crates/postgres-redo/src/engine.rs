//! Pure redo engine for the narrow page-service seam.

use bytes::Bytes;
use crabka_page_store::{PAGE_SIZE, PageKey};
use crabka_postgres_wal::{BlockRef, DecodedRecord, Lsn};

use crate::{
    MetadataState, MetadataUpdate, PageImage, PageImageError, RedoAction, RedoError, RedoKey,
    RedoRecord, consts_v17, page::bytes_with_page_lsn, record::patch_end,
};

/// Stateless redo engine.
#[derive(Debug, Default, Clone, Copy)]
pub struct RedoEngine;

impl RedoEngine {
    /// Applies records in caller-provided order and returns the resulting page.
    pub fn apply(
        self,
        base: Option<PageImage>,
        records: &[RedoRecord],
    ) -> Result<PageImage, RedoError> {
        records
            .iter()
            .try_fold(base, apply_next_record)?
            .ok_or_else(|| missing_base_for_empty_chain(records))
    }
}

/// Applies records with the default stateless engine.
pub fn apply_redo_records(
    base: Option<PageImage>,
    records: &[RedoRecord],
) -> Result<PageImage, RedoError> {
    RedoEngine.apply(base, records)
}

/// Applies one decoded PG17 WAL record block from the bounded native rmgr slice.
pub fn apply_decoded_record_block(
    base: Option<PageImage>,
    record: &DecodedRecord,
    block_index: usize,
) -> Result<PageImage, RedoError> {
    let block = record.blocks.get(block_index).ok_or(RedoError::BadRecord {
        lsn: record.start_lsn,
        context: "block index is not present in decoded record",
    })?;
    let key = block_key(block);

    if let Some(image) = block.image.as_ref() {
        if !image.apply {
            return Err(RedoError::BadRecord {
                lsn: record.start_lsn,
                context: "non-apply block image is unsupported",
            });
        }

        reject_unsupported_image_rmgr(record)?;
        let image_bytes = bytes_with_page_lsn(image.as_ref().to_vec(), record_end_lsn(record))
            .map_err(|err| wrong_page_size(RedoKey::Relation(key), err))?;
        return replace_pg_image(base, key, record_end_lsn(record), image_bytes);
    }

    let action = decode_block_action(record, block)?;
    apply_block_action(base, key, record, block, action)
}

/// Decodes one non-relation metadata update from the bounded PG-4b rmgr slice.
pub fn decode_metadata_update(record: &DecodedRecord) -> Result<MetadataUpdate, RedoError> {
    match record.header.rmid {
        consts_v17::RM_XACT_ID
        | consts_v17::RM_CLOG_ID
        | consts_v17::RM_MULTIXACT_ID
        | consts_v17::RM_COMMIT_TS_ID => {
            crate::rm_slru::decode_slru_action(record).map(MetadataUpdate::Slru)
        }
        consts_v17::RM_RELMAP_ID => {
            crate::rm_relmeta::decode_relmeta_action(record).map(MetadataUpdate::RelMeta)
        }
        consts_v17::RM_HASH_ID => crate::rm_hash::decode_hash_action(record)
            .map(|action| unsupported_index_metadata_action(record, action)),
        consts_v17::RM_GIN_ID => crate::rm_gin::decode_gin_action(record)
            .map(|action| unsupported_index_metadata_action(record, action)),
        consts_v17::RM_GIST_ID => crate::rm_gist::decode_gist_action(record)
            .map(|action| unsupported_index_metadata_action(record, action)),
        consts_v17::RM_SPGIST_ID => crate::rm_spgist::decode_spgist_action(record)
            .map(|action| unsupported_index_metadata_action(record, action)),
        consts_v17::RM_BRIN_ID => crate::rm_brin::decode_brin_action(record)
            .map(|action| unsupported_index_metadata_action(record, action)),
        rmid => Err(RedoError::UnsupportedRmgr {
            rmid,
            info: record.header.info,
            lsn: record.start_lsn,
        }),
    }
}

/// Applies one decoded PG-4b metadata update to deterministic metadata state.
pub fn apply_decoded_metadata_update(
    state: &mut MetadataState,
    record: &DecodedRecord,
) -> Result<(), RedoError> {
    state.apply_update(decode_metadata_update(record)?)
}

fn reject_unsupported_image_rmgr(record: &DecodedRecord) -> Result<(), RedoError> {
    match record.header.rmid {
        consts_v17::RM_XLOG_ID
        | consts_v17::RM_HEAP_ID
        | consts_v17::RM_HEAP2_ID
        | consts_v17::RM_BTREE_ID
        | consts_v17::RM_SEQ_ID => Ok(()),
        consts_v17::RM_CLOG_ID
        | consts_v17::RM_MULTIXACT_ID
        | consts_v17::RM_COMMIT_TS_ID
        | consts_v17::RM_RELMAP_ID => Err(unsupported_metadata_block_record(
            decode_metadata_update(record)?,
        )),
        consts_v17::RM_HASH_ID => crate::rm_hash::accept_hash_page_image(record),
        consts_v17::RM_GIN_ID => crate::rm_gin::accept_gin_page_image(record),
        consts_v17::RM_GIST_ID => crate::rm_gist::accept_gist_page_image(record),
        consts_v17::RM_SPGIST_ID => crate::rm_spgist::accept_spgist_page_image(record),
        consts_v17::RM_BRIN_ID => crate::rm_brin::accept_brin_page_image(record),
        rmid => Err(RedoError::UnsupportedRmgr {
            rmid,
            info: record.header.info,
            lsn: record.start_lsn,
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockRedoAction {
    Initialize,
    ReplaceWithDecodedPageData,
    SetHeapAllVisible,
}

fn decode_block_action(
    record: &DecodedRecord,
    block: &BlockRef,
) -> Result<BlockRedoAction, RedoError> {
    match record.header.rmid {
        consts_v17::RM_HEAP_ID => crate::rm_heap::decode_heap_action(record, block),
        consts_v17::RM_HEAP2_ID => crate::rm_heap::decode_heap2_action(record),
        consts_v17::RM_BTREE_ID => crate::rm_btree::decode_btree_action(record, block),
        consts_v17::RM_SEQ_ID => crate::rm_seq::decode_seq_action(record, block),
        consts_v17::RM_XLOG_ID => decode_xlog_action(record),
        consts_v17::RM_CLOG_ID
        | consts_v17::RM_MULTIXACT_ID
        | consts_v17::RM_COMMIT_TS_ID
        | consts_v17::RM_RELMAP_ID => Err(unsupported_metadata_block_record(
            decode_metadata_update(record)?,
        )),
        consts_v17::RM_HASH_ID => crate::rm_hash::decode_hash_action(record),
        consts_v17::RM_GIN_ID => crate::rm_gin::decode_gin_action(record),
        consts_v17::RM_GIST_ID => crate::rm_gist::decode_gist_action(record),
        consts_v17::RM_SPGIST_ID => crate::rm_spgist::decode_spgist_action(record),
        consts_v17::RM_BRIN_ID => crate::rm_brin::decode_brin_action(record),
        rmid => Err(RedoError::UnsupportedRmgr {
            rmid,
            info: record.header.info,
            lsn: record.start_lsn,
        }),
    }
}

fn unsupported_metadata_block_record(update: MetadataUpdate) -> RedoError {
    match update {
        MetadataUpdate::Slru(update) => RedoError::UnsupportedStorageKey {
            key: RedoKey::Slru(update.key),
            record_lsn: update.end_lsn,
        },
        MetadataUpdate::RelMeta(update) => RedoError::UnsupportedStorageKey {
            key: RedoKey::RelMeta(update.key),
            record_lsn: update.end_lsn,
        },
    }
}

fn unsupported_index_metadata_action(
    record: &DecodedRecord,
    _action: BlockRedoAction,
) -> MetadataUpdate {
    unreachable!(
        "index-family block actions are relation-page actions, not metadata updates: rmid {}, info {:#04x}",
        record.header.rmid, record.header.info
    )
}

fn decode_xlog_action(record: &DecodedRecord) -> Result<BlockRedoAction, RedoError> {
    let opcode = record.header.info & !consts_v17::XLR_INFO_MASK;
    if opcode == consts_v17::XLOG_FPI || opcode == consts_v17::XLOG_FPI_FOR_HINT {
        return Err(RedoError::BadRecord {
            lsn: record.start_lsn,
            context: "xlog FPI record block did not carry a page image",
        });
    }

    Err(RedoError::UnsupportedRmgr {
        rmid: record.header.rmid,
        info: record.header.info,
        lsn: record.start_lsn,
    })
}

fn apply_block_action(
    base: Option<PageImage>,
    key: PageKey,
    record: &DecodedRecord,
    block: &BlockRef,
    action: BlockRedoAction,
) -> Result<PageImage, RedoError> {
    let end_lsn = record_end_lsn(record);
    match action {
        BlockRedoAction::Initialize => {
            let bytes = bytes_with_page_lsn(vec![0_u8; PAGE_SIZE], end_lsn)
                .map_err(|err| wrong_page_size(RedoKey::Relation(key), err))?;
            replace_pg_image(base, key, end_lsn, bytes)
        }
        BlockRedoAction::ReplaceWithDecodedPageData => {
            let bytes = bytes_with_page_lsn(block.data.to_vec(), end_lsn)
                .map_err(|err| wrong_page_size(RedoKey::Relation(key), err))?;
            replace_pg_image(base, key, end_lsn, bytes)
        }
        BlockRedoAction::SetHeapAllVisible => {
            let Some(page) = base else {
                return Err(RedoError::MissingBasePage {
                    key: RedoKey::Relation(key),
                    record_lsn: end_lsn,
                });
            };
            let page = checked_page_for_pg_record(page, key, end_lsn)?;
            let mut bytes = page.bytes().as_ref().to_vec();
            set_heap_all_visible(&mut bytes, record.start_lsn)?;
            let bytes = bytes_with_page_lsn(bytes, end_lsn)
                .map_err(|err| wrong_page_size(RedoKey::Relation(key), err))?;
            PageImage::new(key, end_lsn, bytes)
                .map_err(|err| wrong_page_size(RedoKey::Relation(key), err))
        }
    }
}

fn set_heap_all_visible(bytes: &mut [u8], lsn: Lsn) -> Result<(), RedoError> {
    const PD_FLAGS_OFFSET: usize = 10;
    const PD_ALL_VISIBLE: u16 = 0x0004;

    let Some(raw_flags) = bytes.get_mut(PD_FLAGS_OFFSET..PD_FLAGS_OFFSET + 2) else {
        return Err(RedoError::BadRecord {
            lsn,
            context: "heap page header is too short to set PD_ALL_VISIBLE",
        });
    };
    let flags = u16::from_le_bytes([raw_flags[0], raw_flags[1]]) | PD_ALL_VISIBLE;
    raw_flags.copy_from_slice(&flags.to_le_bytes());
    Ok(())
}

fn replace_pg_image(
    base: Option<PageImage>,
    key: PageKey,
    end_lsn: Lsn,
    image: Bytes,
) -> Result<PageImage, RedoError> {
    if let Some(page) = base {
        let page = checked_page_for_pg_record(page, key, end_lsn)?;
        return page
            .replace(end_lsn, image)
            .map_err(|err| wrong_page_size(RedoKey::Relation(key), err));
    }

    PageImage::new(key, end_lsn, image).map_err(|err| wrong_page_size(RedoKey::Relation(key), err))
}

fn checked_page_for_pg_record(
    page: PageImage,
    key: PageKey,
    end_lsn: Lsn,
) -> Result<PageImage, RedoError> {
    if page.key() != key {
        return Err(RedoError::KeyMismatch {
            page_key: RedoKey::Relation(page.key()),
            record_key: RedoKey::Relation(key),
        });
    }

    if page.lsn() >= end_lsn {
        return Err(RedoError::StaleRecord {
            key: RedoKey::Relation(key),
            page_lsn: page.lsn(),
            record_lsn: end_lsn,
        });
    }

    Ok(page)
}

fn block_key(block: &BlockRef) -> PageKey {
    PageKey::new(
        block.rel.spc_oid,
        block.rel.db_oid,
        block.rel.rel_number,
        block.fork,
        block.blkno,
    )
}

fn record_end_lsn(record: &DecodedRecord) -> Lsn {
    Lsn(record.start_lsn.value() + u64::from(record.total_len))
}

fn apply_next_record(
    page: Option<PageImage>,
    record: &RedoRecord,
) -> Result<Option<PageImage>, RedoError> {
    let key = relation_key(record)?;

    match &record.action {
        RedoAction::InitializePage { image } | RedoAction::FullPageImage { image } => {
            let next_page = replace_with_image(page, key, record, image.clone())?;
            Ok(Some(next_page))
        }
        RedoAction::ByteRangePatch(patch) => {
            let Some(page) = page else {
                return Err(RedoError::MissingBasePage {
                    key: record.key,
                    record_lsn: record.end_lsn,
                });
            };
            let page = checked_existing_page(page, record)?;
            let offset = checked_patch_offset(record, patch.offset, patch.bytes.len())?;
            Ok(Some(page.patch_unchecked(
                record.end_lsn,
                offset,
                patch.bytes.as_ref(),
            )))
        }
    }
}

fn relation_key(record: &RedoRecord) -> Result<PageKey, RedoError> {
    let RedoKey::Relation(key) = record.key else {
        return Err(RedoError::UnsupportedStorageKey {
            key: record.key,
            record_lsn: record.end_lsn,
        });
    };

    Ok(key)
}

fn replace_with_image(
    page: Option<PageImage>,
    key: PageKey,
    record: &RedoRecord,
    image: Bytes,
) -> Result<PageImage, RedoError> {
    if let Some(page) = page {
        let page = checked_existing_page(page, record)?;
        return page
            .replace(record.end_lsn, image)
            .map_err(|err| wrong_page_size(record.key, err));
    }

    PageImage::new(key, record.end_lsn, image).map_err(|err| wrong_page_size(record.key, err))
}

fn checked_existing_page(page: PageImage, record: &RedoRecord) -> Result<PageImage, RedoError> {
    let page_key = RedoKey::Relation(page.key());
    if page_key != record.key {
        return Err(RedoError::KeyMismatch {
            page_key,
            record_key: record.key,
        });
    }

    if page.lsn() >= record.end_lsn {
        return Err(RedoError::StaleRecord {
            key: record.key,
            page_lsn: page.lsn(),
            record_lsn: record.end_lsn,
        });
    }

    Ok(page)
}

fn checked_patch_offset(
    record: &RedoRecord,
    offset: usize,
    len: usize,
) -> Result<usize, RedoError> {
    let Some(end) = patch_end(offset, len) else {
        return Err(out_of_bounds_patch(record, offset, len));
    };

    if end > PAGE_SIZE {
        return Err(out_of_bounds_patch(record, offset, len));
    }

    Ok(offset)
}

fn out_of_bounds_patch(record: &RedoRecord, offset: usize, len: usize) -> RedoError {
    RedoError::OutOfBoundsPatch {
        key: record.key,
        offset,
        len,
        page_size: PAGE_SIZE,
    }
}

fn wrong_page_size(key: RedoKey, err: PageImageError) -> RedoError {
    match err {
        PageImageError::WrongSize { expected, actual } => RedoError::WrongPageSize {
            key,
            expected,
            actual,
        },
    }
}

fn missing_base_for_empty_chain(records: &[RedoRecord]) -> RedoError {
    let key = records.first().map_or_else(
        || RedoKey::Relation(PageKey::new(0, 0, 0, 0, 0)),
        |record| record.key,
    );
    RedoError::MissingBasePage {
        key,
        record_lsn: records
            .first()
            .map_or(crabka_postgres_wal::Lsn(0), |record| record.end_lsn),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use crabka_page_store::PAGE_SIZE;
    use crabka_postgres_wal::Lsn;

    use super::*;
    use crate::{SlruKey, SlruKind, deterministic_page_hash};

    fn key(block_number: u32) -> PageKey {
        PageKey::new(1663, 5, 16_384, 0, block_number)
    }

    fn image(fill: u8) -> Bytes {
        Bytes::from(vec![fill; PAGE_SIZE])
    }

    #[test]
    fn initialize_page_image_without_base() {
        let record = RedoRecord::initialize_page(key(0), Lsn(10), image(7));

        let page = apply_redo_records(None, &[record]);

        assert!(let Ok(page) = page);
        assert!(page.key() == key(0));
        assert!(page.lsn() == Lsn(10));
        assert!(page.bytes().as_ref() == vec![7; PAGE_SIZE].as_slice());
    }

    #[test]
    fn full_page_image_replaces_existing_bytes() {
        let base = PageImage::zeroed(key(0), Lsn(5));
        let record = RedoRecord::full_page_image(key(0), Lsn(10), image(9));

        let page = apply_redo_records(Some(base), &[record]);

        assert!(let Ok(page) = page);
        assert!(page.lsn() == Lsn(10));
        assert!(page.bytes().as_ref() == vec![9; PAGE_SIZE].as_slice());
    }

    #[test]
    fn byte_patch_changes_only_the_target_range() {
        let base = PageImage::zeroed(key(0), Lsn(5));
        let record = RedoRecord::byte_range_patch(key(0), Lsn(10), 4, Bytes::from_static(b"abc"));

        let page = apply_redo_records(Some(base), &[record]);

        assert!(let Ok(page) = page);
        assert!(&page.bytes().as_ref()[0..4] == [0, 0, 0, 0]);
        assert!(&page.bytes().as_ref()[4..7] == b"abc");
        assert!(page.bytes().as_ref()[7] == 0);
    }

    #[test]
    fn stale_lsn_is_rejected() {
        let base = PageImage::zeroed(key(0), Lsn(10));
        let record = RedoRecord::byte_range_patch(key(0), Lsn(10), 0, Bytes::from_static(b"a"));

        let page = apply_redo_records(Some(base), &[record]);

        assert!(
            page == Err(RedoError::StaleRecord {
                key: RedoKey::Relation(key(0)),
                page_lsn: Lsn(10),
                record_lsn: Lsn(10),
            })
        );
    }

    #[test]
    fn byte_patch_without_base_is_rejected() {
        let record = RedoRecord::byte_range_patch(key(0), Lsn(10), 0, Bytes::from_static(b"a"));

        let page = apply_redo_records(None, &[record]);

        assert!(
            page == Err(RedoError::MissingBasePage {
                key: RedoKey::Relation(key(0)),
                record_lsn: Lsn(10),
            })
        );
    }

    #[test]
    fn byte_patch_past_page_end_is_rejected() {
        let base = PageImage::zeroed(key(0), Lsn(5));
        let record =
            RedoRecord::byte_range_patch(key(0), Lsn(10), PAGE_SIZE - 1, Bytes::from_static(b"ab"));

        let page = apply_redo_records(Some(base), &[record]);

        assert!(
            page == Err(RedoError::OutOfBoundsPatch {
                key: RedoKey::Relation(key(0)),
                offset: PAGE_SIZE - 1,
                len: 2,
                page_size: PAGE_SIZE,
            })
        );
    }

    #[test]
    fn key_mismatch_is_rejected() {
        let base = PageImage::zeroed(key(0), Lsn(5));
        let record = RedoRecord::full_page_image(key(1), Lsn(10), image(1));

        let page = apply_redo_records(Some(base), &[record]);

        assert!(
            page == Err(RedoError::KeyMismatch {
                page_key: RedoKey::Relation(key(0)),
                record_key: RedoKey::Relation(key(1)),
            })
        );
    }

    #[test]
    fn page_hash_is_deterministic_after_redo() {
        let base = PageImage::zeroed(key(0), Lsn(5));
        let record =
            RedoRecord::byte_range_patch(key(0), Lsn(10), 8, Bytes::from_static(b"hash-me"));

        let first = apply_redo_records(Some(base.clone()), std::slice::from_ref(&record));
        let second = apply_redo_records(Some(base), &[record]);

        assert!(let (Ok(first), Ok(second)) = (first, second));
        assert!(first.bytes() == second.bytes());
        assert!(deterministic_page_hash(&first) == deterministic_page_hash(&second));
    }

    #[test]
    fn slru_placeholder_refuses_loudly() {
        let record = RedoRecord {
            key: RedoKey::Slru(SlruKey {
                kind: SlruKind::Clog,
                segment_number: 0,
                block_number: 0,
            }),
            end_lsn: Lsn(10),
            action: RedoAction::InitializePage { image: image(0) },
        };

        let page = apply_redo_records(None, &[record]);

        assert!(let Err(RedoError::UnsupportedStorageKey { key: RedoKey::Slru(_), record_lsn }) = page);
        assert!(record_lsn == Lsn(10));
    }
}
