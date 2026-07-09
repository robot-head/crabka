//! Timeline seeding seams for exact page images.

use bytes::Bytes;
use crabka_page_store::{PAGE_SIZE, PageKey};
use crabka_postgres_wal::Lsn;

use crate::{InMemoryTimelineStore, PageServiceError, TimelineKey};

/// One page image discovered by a seeding boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedPageImage {
    /// Relation page key.
    pub key: PageKey,
    /// Exact 8 KiB image.
    pub page: Bytes,
}

/// Seeds already parsed page images at one LSN.
pub async fn seed_page_images(
    store: &mut InMemoryTimelineStore,
    timeline: &TimelineKey,
    lsn: Lsn,
    pages: impl IntoIterator<Item = SeedPageImage>,
) -> Result<(), PageServiceError> {
    store.create_timeline(timeline);
    for image in pages {
        store
            .put_image(timeline, image.key, lsn, image.page)
            .await?;
    }
    Ok(())
}

/// Splits a relation file image into fixed-size page seed entries.
#[must_use]
pub fn relation_file_seed_pages(key_prefix: PageKey, file: &[u8]) -> Vec<SeedPageImage> {
    file.chunks_exact(PAGE_SIZE)
        .enumerate()
        .map(|(block_number, page)| SeedPageImage {
            key: PageKey::new(
                key_prefix.rel.spc_node,
                key_prefix.rel.db_node,
                key_prefix.rel.rel_node,
                key_prefix.rel.fork_number.0,
                u32::try_from(block_number).unwrap_or(u32::MAX),
            ),
            page: Bytes::copy_from_slice(page),
        })
        .collect()
}
