//! Page-shard routing for decoded WAL records.

use std::sync::Arc;

use crate::{DecodedRecord, Lsn};

/// Relation identity used for page-shard routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelTag {
    /// Tablespace OID.
    pub spc_oid: u32,
    /// Database OID.
    pub db_oid: u32,
    /// Relation filenode number.
    pub rel_number: u32,
    /// Fork number encoded in the block reference.
    pub fork: u8,
}

/// Key for a single relation fork block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageKey(pub RelTag, pub u32);

/// A decoded WAL record routed to the page shard(s) it touches or the metadata lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sharded {
    /// A record that references a concrete relation fork block.
    Page {
        /// Relation/block key for the shard.
        key: PageKey,
        /// Record start LSN.
        lsn: Lsn,
        /// Index of the routed block reference inside [`DecodedRecord::blocks`].
        blk_idx: usize,
        /// Shared decoded record.
        rec: Arc<DecodedRecord>,
    },
    /// A record with no block references, routed by resource manager.
    Meta {
        /// Resource manager identifier.
        rmid: u8,
        /// Record start LSN.
        lsn: Lsn,
        /// Shared decoded record.
        rec: Arc<DecodedRecord>,
    },
}

/// Routes a decoded WAL record to relation/block shards.
pub fn shard_record(rec: Arc<DecodedRecord>) -> impl Iterator<Item = Sharded> {
    if rec.blocks.is_empty() {
        return vec![meta_shard(rec)].into_iter();
    }

    rec.blocks
        .iter()
        .enumerate()
        .map(|(blk_idx, block)| {
            let rel = block.rel;
            Sharded::Page {
                key: PageKey(
                    RelTag {
                        spc_oid: rel.spc_oid,
                        db_oid: rel.db_oid,
                        rel_number: rel.rel_number,
                        fork: block.fork,
                    },
                    block.blkno,
                ),
                lsn: rec.start_lsn,
                blk_idx,
                rec: Arc::clone(&rec),
            }
        })
        .collect::<Vec<_>>()
        .into_iter()
}

fn meta_shard(rec: Arc<DecodedRecord>) -> Sharded {
    Sharded::Meta {
        rmid: rec.header.rmid,
        lsn: rec.start_lsn,
        rec,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{BlockFlags, BlockRef, RelFileLocator, XLogRecordHeader};

    #[test]
    fn relation_blocks_route_to_page_keys_sharing_one_record() {
        let rec = Arc::new(decoded_record(vec![
            block_ref(0, 1, 7, rel(1663, 5, 123)),
            block_ref(1, 2, 8, rel(1663, 5, 456)),
        ]));

        let routed = shard_record(Arc::clone(&rec)).collect::<Vec<_>>();

        assert!(routed.len() == 2);
        assert!(let Sharded::Page {
            key: first_key,
            lsn: first_lsn,
            blk_idx: 0,
            rec: first_rec,
        } = &routed[0]);
        assert!(let Sharded::Page {
            key: second_key,
            lsn: second_lsn,
            blk_idx: 1,
            rec: second_rec,
        } = &routed[1]);
        assert!(*first_key == PageKey(rel_tag(1663, 5, 123, 1), 7));
        assert!(*second_key == PageKey(rel_tag(1663, 5, 456, 2), 8));
        assert!(first_key != second_key);
        assert!(*first_lsn == rec.start_lsn);
        assert!(*second_lsn == rec.start_lsn);
        assert!(Arc::ptr_eq(first_rec, &rec));
        assert!(Arc::ptr_eq(second_rec, &rec));
    }

    #[test]
    fn main_data_only_record_routes_to_meta() {
        let mut rec = decoded_record(Vec::new());
        rec.header.rmid = 42;
        rec.main_data = vec![1, 2, 3].into_boxed_slice();
        let rec = Arc::new(rec);

        let routed = shard_record(Arc::clone(&rec)).collect::<Vec<_>>();

        assert!(let [Sharded::Meta { rmid, lsn, rec: routed_rec }] = routed.as_slice());
        assert!(*rmid == 42);
        assert!(*lsn == rec.start_lsn);
        assert!(Arc::ptr_eq(routed_rec, &rec));
    }

    fn decoded_record(blocks: Vec<BlockRef>) -> DecodedRecord {
        let total_len = u32::try_from(crate::XLOG_RECORD_HEADER_SIZE)
            .expect("WAL record header size fits in u32");
        DecodedRecord {
            start_lsn: Lsn(0x0100_0028),
            total_len,
            header: XLogRecordHeader {
                total_len,
                xid: 0,
                prev_lsn: Lsn(0),
                info: 0,
                rmid: 0,
                crc: 0,
            },
            blocks,
            main_data: Box::default(),
            origin: None,
            toplevel_xid: None,
        }
    }

    fn block_ref(id: u8, fork: u8, blkno: u32, rel: RelFileLocator) -> BlockRef {
        BlockRef {
            id,
            fork,
            flags: BlockFlags { raw: fork },
            rel,
            blkno,
            image: None,
            data: Box::default(),
        }
    }

    const fn rel(spc_oid: u32, db_oid: u32, rel_number: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid,
            db_oid,
            rel_number,
        }
    }

    const fn rel_tag(spc_oid: u32, db_oid: u32, rel_number: u32, fork: u8) -> RelTag {
        RelTag {
            spc_oid,
            db_oid,
            rel_number,
            fork,
        }
    }
}
