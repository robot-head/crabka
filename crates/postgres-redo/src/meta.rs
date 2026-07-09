//! Typed PG-4b metadata and SLRU redo state.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use crabka_page_store::PAGE_SIZE;
use crabka_postgres_wal::Lsn;

use crate::{RedoError, RedoKey, RedoRelMetaKey, SlruKey, SlruKind};

/// Deterministic in-memory state for non-relation redo families.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataState {
    slru_pages: BTreeMap<SlruKey, VersionedBytes>,
    slru_truncations: BTreeMap<SlruKind, SlruTruncation>,
    relmeta_values: BTreeMap<RedoRelMetaKey, VersionedBytes>,
}

impl MetadataState {
    /// Applies one parsed metadata update.
    pub fn apply_update(&mut self, update: MetadataUpdate) -> Result<(), RedoError> {
        match update {
            MetadataUpdate::Slru(slru_update) => self.apply_slru_update(slru_update),
            MetadataUpdate::RelMeta(relmeta_update) => self.apply_relmeta_update(relmeta_update),
        }
    }

    /// Returns a materialized SLRU page, when one has been created.
    #[must_use]
    pub fn slru_page(&self, key: SlruKey) -> Option<&Bytes> {
        if self.is_slru_page_truncated(key) {
            return None;
        }

        self.slru_pages.get(&key).map(|page| &page.bytes)
    }

    /// Returns a materialized relation metadata value, when one has been stored.
    #[must_use]
    pub fn relmeta_value(&self, key: RedoRelMetaKey) -> Option<&Bytes> {
        self.relmeta_values.get(&key).map(|value| &value.bytes)
    }

    fn apply_slru_update(&mut self, update: SlruUpdate) -> Result<(), RedoError> {
        match update.action {
            SlruUpdateAction::ZeroPage => {
                self.reject_stale_slru_update(&update)?;
                self.slru_pages.insert(
                    update.key,
                    VersionedBytes {
                        lsn: update.end_lsn,
                        bytes: Bytes::from(vec![0_u8; PAGE_SIZE]),
                    },
                );
                Ok(())
            }
            SlruUpdateAction::SetTransactionStatus { status, xids } => {
                self.apply_clog_status_update(update.key, update.end_lsn, status, &xids)
            }
            SlruUpdateAction::TruncateBefore { cutoff_page } => {
                for &kind in slru_truncate_kinds(update.key.kind) {
                    self.advance_slru_truncation(kind, cutoff_page, update.end_lsn)?;
                }
                Ok(())
            }
            SlruUpdateAction::TruncateMultiXact {
                offset_cutoff_page,
                member_cutoff_page,
            } => {
                self.advance_slru_truncation(
                    SlruKind::MultiXactOffset,
                    offset_cutoff_page,
                    update.end_lsn,
                )?;
                self.advance_slru_truncation(
                    SlruKind::MultiXactMember,
                    member_cutoff_page,
                    update.end_lsn,
                )?;
                Ok(())
            }
        }
    }

    fn apply_clog_status_update(
        &mut self,
        update_key: SlruKey,
        end_lsn: Lsn,
        status: ClogTransactionStatus,
        xids: &[u32],
    ) -> Result<(), RedoError> {
        if update_key.kind != SlruKind::Clog {
            return Err(RedoError::BadRecord {
                lsn: end_lsn,
                context: "transaction status updates must target CLOG",
            });
        }

        let touched_keys = xids
            .iter()
            .copied()
            .map(clog_key_for_xid)
            .collect::<BTreeSet<_>>();
        for key in touched_keys {
            self.reject_stale_slru_update(&SlruUpdate {
                key,
                end_lsn,
                action: SlruUpdateAction::ZeroPage,
            })?;
        }

        for &xid in xids {
            let key = clog_key_for_xid(xid);
            let page = self
                .slru_pages
                .entry(key)
                .or_insert_with(|| VersionedBytes {
                    lsn: end_lsn,
                    bytes: Bytes::from(vec![0_u8; PAGE_SIZE]),
                });
            let mut bytes = page.bytes.to_vec();
            set_clog_status_bits(&mut bytes, xid, status);
            page.lsn = end_lsn;
            page.bytes = Bytes::from(bytes);
        }

        Ok(())
    }

    fn reject_stale_slru_update(&self, update: &SlruUpdate) -> Result<(), RedoError> {
        reject_stale_metadata_lsn(
            RedoKey::Slru(update.key),
            update.end_lsn,
            self.slru_pages.get(&update.key),
        )?;

        let Some(truncation) = self.slru_truncations.get(&update.key.kind) else {
            return Ok(());
        };

        if slru_page_number(update.key) >= truncation.cutoff_page {
            return Ok(());
        }

        Err(RedoError::StaleRecord {
            key: RedoKey::Slru(update.key),
            page_lsn: truncation.lsn,
            record_lsn: update.end_lsn,
        })
    }

    fn advance_slru_truncation(
        &mut self,
        kind: SlruKind,
        cutoff_page: u32,
        lsn: Lsn,
    ) -> Result<(), RedoError> {
        if let Some(existing) = self
            .slru_truncations
            .get(&kind)
            .filter(|existing| existing.lsn >= lsn)
        {
            return Err(RedoError::StaleRecord {
                key: RedoKey::Slru(slru_key_for_page(kind, cutoff_page)),
                page_lsn: existing.lsn,
                record_lsn: lsn,
            });
        }

        self.slru_pages
            .retain(|key, _| key.kind != kind || slru_page_number(*key) >= cutoff_page);
        self.slru_truncations
            .insert(kind, SlruTruncation { lsn, cutoff_page });
        Ok(())
    }

    fn is_slru_page_truncated(&self, key: SlruKey) -> bool {
        self.slru_truncations
            .get(&key.kind)
            .is_some_and(|truncation| slru_page_number(key) < truncation.cutoff_page)
    }

    fn apply_relmeta_update(&mut self, update: RelMetaUpdate) -> Result<(), RedoError> {
        reject_stale_metadata_lsn(
            RedoKey::RelMeta(update.key),
            update.end_lsn,
            self.relmeta_values.get(&update.key),
        )?;
        self.relmeta_values.insert(
            update.key,
            VersionedBytes {
                lsn: update.end_lsn,
                bytes: update.bytes,
            },
        );
        Ok(())
    }
}

/// A parsed metadata update from a known PG-4b rmgr operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataUpdate {
    /// SLRU state update.
    Slru(SlruUpdate),
    /// Relation metadata update.
    RelMeta(RelMetaUpdate),
}

/// A parsed SLRU update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlruUpdate {
    /// Target SLRU key.
    pub key: SlruKey,
    /// Record end LSN.
    pub end_lsn: Lsn,
    /// Deterministic operation to apply.
    pub action: SlruUpdateAction,
}

/// Supported deterministic SLRU operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlruUpdateAction {
    /// Materialize the page as all zeroes.
    ZeroPage,
    /// Set `pg_xact` two-bit transaction statuses for one transaction record.
    SetTransactionStatus {
        /// Status to write for every listed transaction id.
        status: ClogTransactionStatus,
        /// Top-level and subtransaction ids whose CLOG bits are updated.
        xids: Box<[u32]>,
    },
    /// Remove materialized pages before the cutoff page number.
    TruncateBefore {
        /// First page retained after truncation.
        cutoff_page: u32,
    },
    /// Remove `MultiXact` offset and member pages before their family-specific cutoffs.
    TruncateMultiXact {
        /// First offset page retained after truncation.
        offset_cutoff_page: u32,
        /// First member page retained after truncation.
        member_cutoff_page: u32,
    },
}

/// Two-bit transaction status values stored in `pg_xact`/CLOG pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClogTransactionStatus {
    /// Transaction committed.
    Committed,
    /// Transaction aborted.
    Aborted,
    /// Subtransaction committed but not yet parent-committed.
    SubCommitted,
}

impl ClogTransactionStatus {
    const fn bits(self) -> u8 {
        match self {
            Self::Committed => 0b01,
            Self::Aborted => 0b10,
            Self::SubCommitted => 0b11,
        }
    }
}

/// A parsed relation metadata update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelMetaUpdate {
    /// Target relation metadata key.
    pub key: RedoRelMetaKey,
    /// Record end LSN.
    pub end_lsn: Lsn,
    /// Exact metadata bytes carried by the WAL record.
    pub bytes: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionedBytes {
    lsn: Lsn,
    bytes: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlruTruncation {
    lsn: Lsn,
    cutoff_page: u32,
}

fn slru_truncate_kinds(kind: SlruKind) -> &'static [SlruKind] {
    match kind {
        SlruKind::MultiXactMember | SlruKind::MultiXactOffset => {
            &[SlruKind::MultiXactOffset, SlruKind::MultiXactMember]
        }
        SlruKind::Clog => &[SlruKind::Clog],
        SlruKind::CommitTs => &[SlruKind::CommitTs],
    }
}

fn reject_stale_metadata_lsn(
    key: RedoKey,
    record_lsn: Lsn,
    existing: Option<&VersionedBytes>,
) -> Result<(), RedoError> {
    let Some(existing) = existing else {
        return Ok(());
    };

    if existing.lsn >= record_lsn {
        return Err(RedoError::StaleRecord {
            key,
            page_lsn: existing.lsn,
            record_lsn,
        });
    }

    Ok(())
}

/// Builds an SLRU key from a logical SLRU page number.
#[must_use]
pub(crate) const fn slru_key_for_page(kind: crate::SlruKind, page_number: u32) -> SlruKey {
    const SLRU_PAGES_PER_SEGMENT: u32 = 32;

    SlruKey {
        kind,
        segment_number: page_number / SLRU_PAGES_PER_SEGMENT,
        block_number: page_number % SLRU_PAGES_PER_SEGMENT,
    }
}

const fn slru_page_number(key: SlruKey) -> u32 {
    const SLRU_PAGES_PER_SEGMENT: u32 = 32;

    key.segment_number * SLRU_PAGES_PER_SEGMENT + key.block_number
}

const fn clog_page_number_for_xid(xid: u32) -> u32 {
    const CLOG_XACTS_PER_PAGE: u32 = 8192 * 4;

    xid / CLOG_XACTS_PER_PAGE
}

const fn clog_key_for_xid(xid: u32) -> SlruKey {
    slru_key_for_page(SlruKind::Clog, clog_page_number_for_xid(xid))
}

fn set_clog_status_bits(bytes: &mut [u8], xid: u32, status: ClogTransactionStatus) {
    const CLOG_XACTS_PER_BYTE: u32 = 4;
    const CLOG_BYTES_PER_PAGE: u32 = 8192;

    let byte_index = usize::try_from((xid / CLOG_XACTS_PER_BYTE) % CLOG_BYTES_PER_PAGE)
        .expect("CLOG byte index is always within one page");
    let bit_shift = (xid % CLOG_XACTS_PER_BYTE) * 2;
    let mask = !(0b11_u8 << bit_shift);
    bytes[byte_index] = (bytes[byte_index] & mask) | (status.bits() << bit_shift);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    #[test]
    fn zero_page_materializes_deterministic_slru_bytes() {
        let key = slru_key_for_page(SlruKind::Clog, 7);
        let mut state = MetadataState::default();

        let applied = state.apply_update(MetadataUpdate::Slru(SlruUpdate {
            key,
            end_lsn: Lsn(10),
            action: SlruUpdateAction::ZeroPage,
        }));

        assert!(applied == Ok(()));
        assert!(let Some(page) = state.slru_page(key));
        assert!(page.len() == PAGE_SIZE);
        assert!(page.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn truncate_removes_only_older_pages_in_same_slru_family() {
        let clog_before = slru_key_for_page(SlruKind::Clog, 1);
        let clog_after = slru_key_for_page(SlruKind::Clog, 4);
        let commit_ts_before = slru_key_for_page(SlruKind::CommitTs, 1);
        let mut state = MetadataState::default();
        for key in [clog_before, clog_after, commit_ts_before] {
            state
                .apply_update(MetadataUpdate::Slru(SlruUpdate {
                    key,
                    end_lsn: Lsn(10),
                    action: SlruUpdateAction::ZeroPage,
                }))
                .expect("test fixture uses fresh metadata keys");
        }

        let applied = state.apply_update(MetadataUpdate::Slru(SlruUpdate {
            key: slru_key_for_page(SlruKind::Clog, 3),
            end_lsn: Lsn(20),
            action: SlruUpdateAction::TruncateBefore { cutoff_page: 3 },
        }));

        assert!(applied == Ok(()));
        assert!(state.slru_page(clog_before).is_none());
        assert!(state.slru_page(clog_after).is_some());
        assert!(state.slru_page(commit_ts_before).is_some());
    }

    #[test]
    fn multixact_truncate_hides_offsets_and_members_only() {
        let offset_before = slru_key_for_page(SlruKind::MultiXactOffset, 1);
        let member_before = slru_key_for_page(SlruKind::MultiXactMember, 1);
        let offset_after = slru_key_for_page(SlruKind::MultiXactOffset, 4);
        let member_after = slru_key_for_page(SlruKind::MultiXactMember, 4);
        let clog_before = slru_key_for_page(SlruKind::Clog, 1);
        let commit_ts_before = slru_key_for_page(SlruKind::CommitTs, 1);
        let mut state = MetadataState::default();
        for key in [
            offset_before,
            member_before,
            offset_after,
            member_after,
            clog_before,
            commit_ts_before,
        ] {
            state
                .apply_update(MetadataUpdate::Slru(SlruUpdate {
                    key,
                    end_lsn: Lsn(10),
                    action: SlruUpdateAction::ZeroPage,
                }))
                .expect("test fixture uses fresh metadata keys");
        }

        let applied = state.apply_update(MetadataUpdate::Slru(SlruUpdate {
            key: slru_key_for_page(SlruKind::MultiXactMember, 3),
            end_lsn: Lsn(20),
            action: SlruUpdateAction::TruncateBefore { cutoff_page: 3 },
        }));

        assert!(applied == Ok(()));
        assert!(state.slru_page(offset_before).is_none());
        assert!(state.slru_page(member_before).is_none());
        assert!(state.slru_page(offset_after).is_some());
        assert!(state.slru_page(member_after).is_some());
        assert!(state.slru_page(clog_before).is_some());
        assert!(state.slru_page(commit_ts_before).is_some());
    }

    #[test]
    fn commit_ts_truncate_hides_commit_ts_only() {
        let commit_ts_before = slru_key_for_page(SlruKind::CommitTs, 1);
        let commit_ts_after = slru_key_for_page(SlruKind::CommitTs, 4);
        let clog_before = slru_key_for_page(SlruKind::Clog, 1);
        let multixact_offset_before = slru_key_for_page(SlruKind::MultiXactOffset, 1);
        let multixact_member_before = slru_key_for_page(SlruKind::MultiXactMember, 1);
        let mut state = MetadataState::default();
        for key in [
            commit_ts_before,
            commit_ts_after,
            clog_before,
            multixact_offset_before,
            multixact_member_before,
        ] {
            state
                .apply_update(MetadataUpdate::Slru(SlruUpdate {
                    key,
                    end_lsn: Lsn(10),
                    action: SlruUpdateAction::ZeroPage,
                }))
                .expect("test fixture uses fresh metadata keys");
        }

        let applied = state.apply_update(MetadataUpdate::Slru(SlruUpdate {
            key: slru_key_for_page(SlruKind::CommitTs, 3),
            end_lsn: Lsn(20),
            action: SlruUpdateAction::TruncateBefore { cutoff_page: 3 },
        }));

        assert!(applied == Ok(()));
        assert!(state.slru_page(commit_ts_before).is_none());
        assert!(state.slru_page(commit_ts_after).is_some());
        assert!(state.slru_page(clog_before).is_some());
        assert!(state.slru_page(multixact_offset_before).is_some());
        assert!(state.slru_page(multixact_member_before).is_some());
    }

    #[test]
    fn relmeta_update_stores_exact_payload() {
        let key = RedoRelMetaKey::relmap(5, 1663);
        let mut state = MetadataState::default();

        let applied = state.apply_update(MetadataUpdate::RelMeta(RelMetaUpdate {
            key,
            end_lsn: Lsn(10),
            bytes: Bytes::from_static(b"payload"),
        }));

        assert!(applied == Ok(()));
        assert!(let Some(bytes) = state.relmeta_value(key));
        assert!(bytes.as_ref() == b"payload");
    }

    #[test]
    fn clog_status_update_writes_expected_two_bit_slots() {
        let mut expected = vec![0_u8; PAGE_SIZE];
        expected[0] = 0b0000_0001;
        expected[1] = 0b0001_0000;
        let key = slru_key_for_page(SlruKind::Clog, 0);
        let mut state = MetadataState::default();

        let applied = state.apply_update(MetadataUpdate::Slru(SlruUpdate {
            key,
            end_lsn: Lsn(10),
            action: SlruUpdateAction::SetTransactionStatus {
                status: ClogTransactionStatus::Committed,
                xids: Box::from([0, 6]),
            },
        }));

        assert!(applied == Ok(()));
        assert!(let Some(page) = state.slru_page(key));
        assert!(page.as_ref() == expected.as_slice());
    }

    #[test]
    fn clog_status_update_materializes_every_touched_page() {
        let first_key = slru_key_for_page(SlruKind::Clog, 0);
        let second_key = slru_key_for_page(SlruKind::Clog, 1);
        let mut state = MetadataState::default();

        let applied = state.apply_update(MetadataUpdate::Slru(SlruUpdate {
            key: first_key,
            end_lsn: Lsn(10),
            action: SlruUpdateAction::SetTransactionStatus {
                status: ClogTransactionStatus::Aborted,
                xids: Box::from([1, 32_768]),
            },
        }));

        assert!(applied == Ok(()));
        assert!(let Some(first_page) = state.slru_page(first_key));
        assert!(let Some(second_page) = state.slru_page(second_key));
        assert!(first_page.as_ref()[0] == 0b0000_1000);
        assert!(second_page.as_ref()[0] == 0b0000_0010);
    }
}
