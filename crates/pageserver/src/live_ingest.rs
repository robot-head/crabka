//! Live-ingest seam over parsed page WAL records.

use bytes::Bytes;
use crabka_page_store::PageKey;
use crabka_postgres_wal::Lsn;

use crate::{InMemoryTimelineStore, PageServiceError, TimelineKey};

/// One page-scoped WAL record accepted by the current live-ingest seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveIngestRecord {
    /// Page key targeted by the record.
    pub key: PageKey,
    /// Record LSN.
    pub lsn: Lsn,
    /// Whether the record initializes the page without an ancestor image.
    pub will_init: bool,
    /// Opaque redo bytes understood by the configured redo codec.
    pub record: Bytes,
}

/// Applies a contiguous batch of parsed live WAL records into a timeline.
pub async fn ingest_live_records(
    store: &mut InMemoryTimelineStore,
    timeline: &TimelineKey,
    records: impl IntoIterator<Item = LiveIngestRecord>,
) -> Result<Lsn, PageServiceError> {
    let mut last_lsn = Lsn(0);
    for record in records {
        store
            .put_wal(
                timeline,
                record.key,
                record.lsn,
                record.will_init,
                record.record,
            )
            .await?;
        last_lsn = last_lsn.max(record.lsn);
    }
    Ok(last_lsn)
}
