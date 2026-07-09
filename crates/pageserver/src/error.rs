//! Error taxonomy for page reconstruction.

use crabka_page_store::{
    LayerMapError, PAGE_SIZE, PageKey, RelMetaKey, RelTag, SlruPageKey, TimelinePath,
};
use crabka_postgres_redo::{PageImageError, RedoError};
use crabka_postgres_wal::Lsn;
use thiserror::Error;

use crate::TimelineKey;

/// Errors returned by the page-service entrypoints.
#[derive(Debug, Error)]
pub enum PageServiceError {
    /// The requested timeline has not been created in this store.
    #[error("timeline {timeline} does not exist")]
    TimelineNotFound {
        /// Missing timeline.
        timeline: TimelineKey,
    },
    /// The requested page cannot be reconstructed from retained layer history.
    #[error("page {key} is missing on timeline {timeline} at {lsn}")]
    PageNotFound {
        /// Timeline that was queried.
        timeline: TimelineKey,
        /// Page key that was queried.
        key: PageKey,
        /// Target LSN.
        lsn: Lsn,
    },
    /// A caller attempted to ingest an image with the wrong byte length.
    #[error("page image for {key} must be exactly {expected} bytes, got {actual}")]
    WrongImageSize {
        /// Page key associated with the image.
        key: PageKey,
        /// Required size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },
    /// A caller attempted to ingest an SLRU page with the wrong byte length.
    #[error("SLRU page image for {key} must be exactly {expected} bytes, got {actual}")]
    WrongSlruPageSize {
        /// SLRU page key associated with the image.
        key: SlruPageKey,
        /// Required size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },
    /// Relation-size metadata is missing from the timeline metadata store.
    #[error("relation size for {rel:?} on timeline {timeline} at {lsn} is missing")]
    RelationSizeMissing {
        /// Timeline that was queried.
        timeline: TimelineKey,
        /// Relation fork that was queried.
        rel: RelTag,
        /// Target LSN.
        lsn: Lsn,
    },
    /// SLRU page metadata is missing from the timeline metadata store.
    #[error("SLRU page {key} on timeline {timeline} at {lsn} is missing")]
    SlruPageMissing {
        /// Timeline that was queried.
        timeline: TimelineKey,
        /// SLRU page that was queried.
        key: SlruPageKey,
        /// Target LSN.
        lsn: Lsn,
    },
    /// Relation metadata is missing from the timeline metadata store.
    #[error("relation metadata {key} on timeline {timeline} at {lsn} is missing")]
    RelMetaMissing {
        /// Timeline that was queried.
        timeline: TimelineKey,
        /// Relation metadata key that was queried.
        key: RelMetaKey,
        /// Target LSN.
        lsn: Lsn,
    },
    /// A branch source timeline is missing.
    #[error("branch source timeline {timeline} does not exist")]
    BranchSourceNotFound {
        /// Missing source timeline.
        timeline: TimelineKey,
    },
    /// A branch point is newer than the source timeline's durable head.
    #[error("branch point {branch_lsn} is beyond source timeline {timeline} head {head_lsn}")]
    BranchLsnBeyondHead {
        /// Source timeline.
        timeline: TimelineKey,
        /// Requested branch point.
        branch_lsn: Lsn,
        /// Source timeline high watermark.
        head_lsn: Lsn,
    },
    /// A write attempted to cross a branch ancestry boundary.
    #[error("write at {lsn} violates branch boundary {branch_lsn} for timeline {timeline}")]
    BranchBoundaryViolation {
        /// Timeline being written.
        timeline: TimelineKey,
        /// Write LSN.
        lsn: Lsn,
        /// Branch point owned by the ancestor.
        branch_lsn: Lsn,
    },
    /// A timeline with descendants cannot be deleted.
    #[error("timeline {timeline} has descendants and cannot be deleted")]
    TimelineHasDescendants {
        /// Timeline targeted by deletion.
        timeline: TimelineKey,
    },
    /// The page-store layer map or object store failed.
    #[error(transparent)]
    PageStore(#[from] LayerMapError),
    /// Redo reconstruction failed after page-store produced a delta chain.
    #[error(transparent)]
    Redo(#[from] RedoReconstructionError),
}

impl PageServiceError {
    pub(crate) fn wrong_image_size(key: PageKey, actual: usize) -> Self {
        Self::WrongImageSize {
            key,
            expected: PAGE_SIZE,
            actual,
        }
    }
}

impl PageServiceError {
    pub(crate) fn from_layer_map_error(timeline: &TimelineKey, err: LayerMapError) -> Self {
        match err {
            LayerMapError::HistoryTrimmed { key, lsn } => Self::PageNotFound {
                timeline: timeline.clone(),
                key,
                lsn,
            },
            err => Self::PageStore(err),
        }
    }
}

/// Errors returned while decoding opaque page-store WAL bytes into typed redo records.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RedoDecodeError {
    /// The synthetic redo tag is not implemented by this bounded slice.
    #[error("unsupported synthetic redo tag {tag} for page {key} at {lsn}")]
    UnsupportedTag {
        /// Page targeted by the delta bytes.
        key: PageKey,
        /// LSN associated with the delta bytes.
        lsn: Lsn,
        /// Rejected tag byte.
        tag: u8,
    },
    /// The encoded record ended before a required field was present.
    #[error("truncated synthetic redo record for page {key} at {lsn}: {field}")]
    Truncated {
        /// Page targeted by the delta bytes.
        key: PageKey,
        /// LSN associated with the delta bytes.
        lsn: Lsn,
        /// Field that could not be read.
        field: &'static str,
    },
    /// The record carried bytes after the expected payload.
    #[error("synthetic redo record for page {key} at {lsn} has {extra} trailing bytes")]
    TrailingBytes {
        /// Page targeted by the delta bytes.
        key: PageKey,
        /// LSN associated with the delta bytes.
        lsn: Lsn,
        /// Number of unused trailing bytes.
        extra: usize,
    },
    /// A numeric field does not fit on this platform.
    #[error("synthetic redo record for page {key} at {lsn} field {field} is too large: {value}")]
    FieldTooLarge {
        /// Page targeted by the delta bytes.
        key: PageKey,
        /// LSN associated with the delta bytes.
        lsn: Lsn,
        /// Field that did not fit.
        field: &'static str,
        /// Rejected value.
        value: u64,
    },
}

/// Errors returned by a redo implementation while materializing a page.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RedoReconstructionError {
    /// Page-store supplied a base image with the wrong size.
    #[error("invalid base page image for {key} at {lsn}: {source}")]
    InvalidBaseImage {
        /// Page key being reconstructed.
        key: PageKey,
        /// Base image LSN.
        lsn: Lsn,
        /// Size validation error.
        #[source]
        source: PageImageError,
    },
    /// A delta byte string could not be parsed into a typed redo record.
    #[error(transparent)]
    Decode(#[from] RedoDecodeError),
    /// The typed redo engine refused the record chain.
    #[error(transparent)]
    Redo(#[from] RedoError),
    /// Page-store produced no base and no WAL records for a page.
    #[error("empty reconstruction chain for {key} at {lsn}")]
    EmptyChain {
        /// Page key being reconstructed.
        key: PageKey,
        /// Target LSN.
        lsn: Lsn,
    },
}

pub(crate) fn wrong_timeline(expected: &TimelinePath, actual: &TimelinePath) -> LayerMapError {
    LayerMapError::WrongTimeline {
        expected: expected.prefix(),
        actual: actual.prefix(),
    }
}
