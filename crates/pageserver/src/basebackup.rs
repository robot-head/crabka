//! Deterministic basebackup payload generation for the in-memory pageserver.

use bytes::{BufMut as _, Bytes, BytesMut};
use crabka_page_store::{PageKey, RelMetaKey, SlruPageKey};
use crabka_postgres_wal::Lsn;

use crate::{BasebackupRelMetaMetadata, BasebackupSlruPageMetadata, TimelineKey};

const MAGIC: &[u8] = b"CRABKA_BASEBACKUP_V1\n";

/// Parsed data used to assemble a deterministic basebackup payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasebackupPayloadInput {
    /// Timeline included in the backup.
    pub timeline: TimelineKey,
    /// Snapshot LSN.
    pub lsn: Lsn,
    /// Visible relation pages reconstructed through redo.
    pub pages: Vec<BasebackupPage>,
    /// Visible relation metadata.
    pub relmeta: Vec<BasebackupRelMetaMetadata>,
    /// Visible SLRU pages.
    pub slru_pages: Vec<BasebackupSlruPageMetadata>,
}

/// One reconstructed relation page in a basebackup payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasebackupPage {
    /// Page-store relation key.
    pub key: PageKey,
    /// Exact page image visible at the backup LSN.
    pub page: Bytes,
}

/// Builds a deterministic protobuf-friendly payload.
#[must_use]
pub fn encode_basebackup_payload(mut input: BasebackupPayloadInput) -> Bytes {
    input.pages.sort_by_key(|page| page.key);
    input.relmeta.sort_by_key(|metadata| metadata.key);
    input.slru_pages.sort_by_key(|metadata| metadata.key);

    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(MAGIC);
    put_string(&mut bytes, &input.timeline.branch_id.to_string());
    put_string(&mut bytes, &input.timeline.path.tenant_id.to_string());
    put_string(&mut bytes, &input.timeline.path.timeline_id.to_string());
    bytes.put_u64_le(input.lsn.value());

    bytes.put_u32_le(u32::try_from(input.pages.len()).unwrap_or(u32::MAX));
    for page in input.pages {
        put_page_key(&mut bytes, page.key);
        put_bytes(&mut bytes, &page.page);
    }

    bytes.put_u32_le(u32::try_from(input.relmeta.len()).unwrap_or(u32::MAX));
    for metadata in input.relmeta {
        put_relmeta_key(&mut bytes, metadata.key);
        put_bytes(&mut bytes, &metadata.metadata);
    }

    bytes.put_u32_le(u32::try_from(input.slru_pages.len()).unwrap_or(u32::MAX));
    for metadata in input.slru_pages {
        put_slru_key(&mut bytes, metadata.key);
        put_bytes(&mut bytes, &metadata.page);
    }

    bytes.freeze()
}

fn put_string(bytes: &mut BytesMut, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn put_bytes(bytes: &mut BytesMut, value: &[u8]) {
    bytes.put_u32_le(u32::try_from(value.len()).unwrap_or(u32::MAX));
    bytes.extend_from_slice(value);
}

fn put_page_key(bytes: &mut BytesMut, key: PageKey) {
    put_rel_tag(bytes, key.rel);
    bytes.put_u32_le(key.block_number.0);
}

fn put_relmeta_key(bytes: &mut BytesMut, key: RelMetaKey) {
    put_rel_tag(bytes, key.rel);
    bytes.put_u8(relmeta_kind_tag(key.kind));
}

fn put_rel_tag(bytes: &mut BytesMut, rel: crabka_page_store::RelTag) {
    bytes.put_u32_le(rel.spc_node);
    bytes.put_u32_le(rel.db_node);
    bytes.put_u32_le(rel.rel_node);
    bytes.put_u8(rel.fork_number.0);
}

fn put_slru_key(bytes: &mut BytesMut, key: SlruPageKey) {
    bytes.put_u8(slru_kind_tag(key.kind));
    bytes.put_u32_le(key.segment_number);
    bytes.put_u32_le(key.block_number);
}

const fn relmeta_kind_tag(kind: crabka_page_store::RelMetaKind) -> u8 {
    match kind {
        crabka_page_store::RelMetaKind::Size => 1,
        crabka_page_store::RelMetaKind::RelMap => 2,
        crabka_page_store::RelMetaKind::StorageManager => 3,
    }
}

const fn slru_kind_tag(kind: crabka_page_store::SlruKind) -> u8 {
    match kind {
        crabka_page_store::SlruKind::Clog => 1,
        crabka_page_store::SlruKind::MultiXactOffset => 2,
        crabka_page_store::SlruKind::MultiXactMember => 3,
        crabka_page_store::SlruKind::CommitTs => 4,
    }
}
