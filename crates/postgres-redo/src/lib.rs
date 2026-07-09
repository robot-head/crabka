//! Pure, deterministic `PostgreSQL` redo scaffold.
//!
//! This crate intentionally implements only a narrow seam: initialize a page,
//! replace a page with a full-page image, and apply a checked byte-range patch.
//! Unsupported `PostgreSQL` redo records are refused as typed family-specific
//! errors, not silently interpreted.

pub mod consts_v17;
mod engine;
mod index_family;
mod meta;
mod page;
mod record;
mod rm_brin;
mod rm_btree;
mod rm_gin;
mod rm_gist;
mod rm_hash;
mod rm_heap;
mod rm_relmeta;
mod rm_seq;
mod rm_slru;
mod rm_spgist;

pub use engine::{
    RedoEngine, apply_decoded_metadata_update, apply_decoded_record_block, apply_redo_records,
    decode_metadata_update,
};
pub use meta::{
    ClogTransactionStatus, MetadataState, MetadataUpdate, RelMetaUpdate, SlruUpdate,
    SlruUpdateAction,
};
pub use page::{PageImage, PageImageError, deterministic_page_hash};
pub use record::{
    ByteRangePatch, IndexRedoFamily, RedoAction, RedoError, RedoKey, RedoOpcodeFamily, RedoRecord,
    RedoRelMetaKey, RelMapKey, RelMetaScope, SlruKey, SlruKind, UnsupportedRedoFamily,
};
