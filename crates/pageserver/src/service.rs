//! Typed page-service entrypoints used by the RPC boundary.

use bytes::Bytes;
use crabka_page_store::{PageKey, RelMetaKey, RelTag, SlruPageKey};
use crabka_postgres_wal::Lsn;
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::{
    BasebackupPage, BasebackupPayloadInput, InMemoryTimelineStore, PageRedo, PageServiceError,
    TimelineKey, encode_basebackup_payload,
};

/// Typed request for reconstructing one page from a branch timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPageRequest {
    /// Branch/timeline namespace to query.
    pub timeline: TimelineKey,
    /// `PostgreSQL` relation page key.
    pub key: PageKey,
    /// Target LSN for reconstruction.
    pub lsn: Lsn,
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_page_store::{RelMetaKind, SlruKind, TenantId, TimelineId, TimelinePath};

    use super::*;
    use crate::{PostgresRedo, SyntheticRedoCodec};

    fn timeline() -> TimelineKey {
        TimelineKey::new(
            crate::BranchId::parse("main").expect("test branch id is valid"),
            TimelinePath::new(
                TenantId::parse("11111111111111111111111111111111")
                    .expect("test tenant id is valid"),
                TimelineId::parse("22222222222222222222222222222222")
                    .expect("test timeline id is valid"),
            ),
        )
    }

    fn service() -> PageService<PostgresRedo<SyntheticRedoCodec>> {
        let key = timeline();
        let mut store = InMemoryTimelineStore::new();
        store.create_timeline(&key);
        PageService::new(store, PostgresRedo::new(SyntheticRedoCodec))
    }

    #[tokio::test]
    async fn slru_lookup_returns_typed_missing_error() {
        let timeline = timeline();
        let request = SlruPageRequest {
            timeline: timeline.clone(),
            key: SlruPageKey::new(SlruKind::Clog, 0, 1),
            lsn: Lsn(10),
        };

        let response = service().slru_page(request).await;

        assert!(let Err(PageServiceError::SlruPageMissing { timeline: found, key, lsn }) = response);
        assert!(found == timeline);
        assert!(key == SlruPageKey::new(SlruKind::Clog, 0, 1));
        assert!(lsn == Lsn(10));
    }

    #[tokio::test]
    async fn relmeta_lookup_returns_typed_missing_error() {
        let timeline = timeline();
        let key = RelMetaKey::new(RelTag::new(1663, 5, 16_384, 0), RelMetaKind::Size);
        let request = RelMetaRequest {
            timeline: timeline.clone(),
            key,
            lsn: Lsn(20),
        };

        let response = service().relmeta(request).await;

        assert!(let Err(PageServiceError::RelMetaMissing { timeline: found, key: found_key, lsn }) = response);
        assert!(found == timeline);
        assert!(found_key == key);
        assert!(lsn == Lsn(20));
    }
}

/// Typed response containing one reconstructed page image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPageResponse {
    /// Exact `PAGE_SIZE` page bytes reconstructed at the requested LSN.
    pub page: Bytes,
}

/// Typed request for relation-size metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationSizeRequest {
    /// Branch/timeline namespace to query.
    pub timeline: TimelineKey,
    /// `PostgreSQL` relation fork whose size is requested.
    pub rel: RelTag,
    /// Target LSN for size visibility.
    pub lsn: Lsn,
}

/// Typed response for relation-size metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationSizeResponse {
    /// Number of blocks visible in the requested relation fork.
    pub blocks: u32,
}

/// Typed request for an SLRU page lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlruPageRequest {
    /// Branch/timeline namespace to query.
    pub timeline: TimelineKey,
    /// SLRU page key.
    pub key: SlruPageKey,
    /// Target LSN for reconstruction.
    pub lsn: Lsn,
}

/// Typed response containing one SLRU page image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlruPageResponse {
    /// Exact SLRU page bytes reconstructed at the requested LSN.
    pub page: Bytes,
}

/// Typed request for relation metadata lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelMetaRequest {
    /// Branch/timeline namespace to query.
    pub timeline: TimelineKey,
    /// Relation metadata key.
    pub key: RelMetaKey,
    /// Target LSN for metadata visibility.
    pub lsn: Lsn,
}

/// Typed response containing relation metadata bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelMetaResponse {
    /// Opaque relation metadata payload.
    pub metadata: Bytes,
}

/// Typed request for basebackup metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasebackupMetadataRequest {
    /// Branch/timeline namespace to query.
    pub timeline: TimelineKey,
    /// LSN at which the basebackup metadata snapshot is requested.
    pub lsn: Lsn,
}

/// Typed response for basebackup metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasebackupMetadataResponse {
    /// Timeline snapshot used for the metadata response.
    pub timeline: TimelineKey,
    /// LSN used for the metadata response.
    pub lsn: Lsn,
    /// Relation metadata visible at `lsn`, ordered by [`RelMetaKey`].
    pub relmeta: Vec<BasebackupRelMetaMetadata>,
    /// SLRU pages visible at `lsn`, ordered by [`SlruPageKey`].
    pub slru_pages: Vec<BasebackupSlruPageMetadata>,
    /// Deterministic payload containing reconstructed pages and metadata.
    pub tar: Bytes,
}

/// One relation metadata item included in basebackup metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasebackupRelMetaMetadata {
    /// Relation metadata key.
    pub key: RelMetaKey,
    /// Metadata payload visible at the requested LSN.
    pub metadata: Bytes,
}

/// One SLRU page included in basebackup metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasebackupSlruPageMetadata {
    /// SLRU page key.
    pub key: SlruPageKey,
    /// Exact SLRU page bytes visible at the requested LSN.
    pub page: Bytes,
}

/// Pure page service over an in-memory timeline store and redo implementation.
pub struct PageService<R> {
    store: RwLock<InMemoryTimelineStore>,
    redo: R,
}

impl<R> PageService<R> {
    /// Builds a service with an existing in-memory store.
    #[must_use]
    pub const fn new(store: InMemoryTimelineStore, redo: R) -> Self {
        Self {
            store: RwLock::const_new(store),
            redo,
        }
    }

    /// Returns a mutable guard for tests and bootstrap code.
    pub async fn store_mut(&self) -> RwLockWriteGuard<'_, InMemoryTimelineStore> {
        self.store.write().await
    }

    /// Creates a branch/timeline namespace for page and metadata operations.
    pub async fn create_timeline(&self, timeline: &TimelineKey) -> CreateTimelineResponse {
        let created = self.store.write().await.create_timeline(timeline);
        CreateTimelineResponse {
            timeline: timeline.clone(),
            created,
        }
    }

    /// Creates a branch namespace backed by page-store timeline ancestry metadata.
    pub async fn create_branch(
        &self,
        request: CreateBranchRequest,
    ) -> Result<CreateBranchResponse, PageServiceError> {
        let created = self.store.write().await.create_branch(
            &request.timeline,
            &request.source_timeline,
            request.branch_lsn,
        )?;
        Ok(CreateBranchResponse {
            timeline: request.timeline,
            created,
            source_timeline: request.source_timeline,
            branch_lsn: request.branch_lsn,
        })
    }

    /// Lists branch/timeline namespaces known to this service.
    pub async fn list_timelines(&self) -> ListTimelinesResponse {
        let timelines = self.store.read().await.list_timelines();
        ListTimelinesResponse { timelines }
    }

    /// Deletes a branch/timeline namespace from the in-memory backend.
    pub async fn delete_timeline(
        &self,
        timeline: &TimelineKey,
    ) -> Result<DeleteTimelineResponse, PageServiceError> {
        let deleted = self.store.write().await.delete_timeline(timeline)?;
        Ok(DeleteTimelineResponse {
            timeline: timeline.clone(),
            deleted,
        })
    }

    /// Seeds one exact page image into a branch/timeline namespace.
    pub async fn put_page_image(
        &self,
        request: PutPageImageRequest,
    ) -> Result<PutPageImageResponse, PageServiceError> {
        self.store
            .write()
            .await
            .put_image(&request.timeline, request.key, request.lsn, request.page)
            .await?;
        Ok(PutPageImageResponse {
            timeline: request.timeline,
            key: request.key,
            lsn: request.lsn,
        })
    }

    /// Ingests one WAL delta record into a branch/timeline namespace.
    pub async fn put_wal_record(
        &self,
        request: PutWalRecordRequest,
    ) -> Result<PutWalRecordResponse, PageServiceError> {
        self.store
            .write()
            .await
            .put_wal(
                &request.timeline,
                request.key,
                request.lsn,
                request.will_init,
                request.record,
            )
            .await?;
        Ok(PutWalRecordResponse {
            timeline: request.timeline,
            key: request.key,
            lsn: request.lsn,
            will_init: request.will_init,
        })
    }
}

/// Typed response for branch/timeline creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTimelineResponse {
    /// Created or existing branch/timeline namespace.
    pub timeline: TimelineKey,
    /// Whether this request inserted a new namespace.
    pub created: bool,
}

/// Typed request for creating a branch/timeline from an existing source timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBranchRequest {
    /// Branch/timeline namespace to create.
    pub timeline: TimelineKey,
    /// Source timeline the branch is logically derived from.
    pub source_timeline: TimelineKey,
    /// Branch point requested by the caller.
    pub branch_lsn: Lsn,
}

/// Typed response for branch/timeline creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBranchResponse {
    /// Created or existing branch/timeline namespace.
    pub timeline: TimelineKey,
    /// Whether this request inserted a new namespace.
    pub created: bool,
    /// Source timeline the branch is logically derived from.
    pub source_timeline: TimelineKey,
    /// Branch point requested by the caller.
    pub branch_lsn: Lsn,
}

/// Typed response for listing branch/timeline namespaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTimelinesResponse {
    /// Timelines known to this service.
    pub timelines: Vec<TimelineKey>,
}

/// Typed response for deleting a branch/timeline namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTimelineResponse {
    /// Branch/timeline namespace targeted by the request.
    pub timeline: TimelineKey,
    /// Whether an existing namespace was removed.
    pub deleted: bool,
}

/// Typed request for seeding one exact page image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutPageImageRequest {
    /// Branch/timeline namespace to mutate.
    pub timeline: TimelineKey,
    /// Page key to write.
    pub key: PageKey,
    /// LSN at which the image becomes visible.
    pub lsn: Lsn,
    /// Exact `PAGE_SIZE` page bytes.
    pub page: Bytes,
}

/// Typed response for seeding one exact page image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutPageImageResponse {
    /// Branch/timeline namespace that was mutated.
    pub timeline: TimelineKey,
    /// Page key that was written.
    pub key: PageKey,
    /// LSN at which the image becomes visible.
    pub lsn: Lsn,
}

/// Typed request for ingesting one live WAL delta record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutWalRecordRequest {
    /// Branch/timeline namespace to mutate.
    pub timeline: TimelineKey,
    /// Page key to write.
    pub key: PageKey,
    /// LSN at which the delta record becomes visible.
    pub lsn: Lsn,
    /// Whether this WAL record initializes the page image.
    pub will_init: bool,
    /// Opaque redo record bytes understood by the configured redo codec.
    pub record: Bytes,
}

/// Typed response for ingesting one live WAL delta record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutWalRecordResponse {
    /// Branch/timeline namespace that was mutated.
    pub timeline: TimelineKey,
    /// Page key that was written.
    pub key: PageKey,
    /// LSN at which the delta record becomes visible.
    pub lsn: Lsn,
    /// Whether this WAL record initializes the page image.
    pub will_init: bool,
}

impl<R> PageService<R>
where
    R: PageRedo,
{
    /// Reconstructs one page from a typed service request.
    pub async fn get_page(
        &self,
        request: GetPageRequest,
    ) -> Result<GetPageResponse, PageServiceError> {
        let data = {
            let store = self.store.read().await;
            store
                .get_reconstruct_data(&request.timeline, request.key, request.lsn)
                .await?
        };
        let page = self.redo.reconstruct_page(request.key, data, request.lsn)?;
        Ok(GetPageResponse { page })
    }

    /// Returns exact relation-size metadata from the timeline metadata store.
    pub async fn relation_size(
        &self,
        request: RelationSizeRequest,
    ) -> Result<RelationSizeResponse, PageServiceError> {
        let store = self.store.read().await;
        let blocks = store.relation_size(&request.timeline, request.rel, request.lsn)?;
        Ok(RelationSizeResponse { blocks })
    }

    /// Returns an exact SLRU page from the timeline metadata store.
    pub async fn slru_page(
        &self,
        request: SlruPageRequest,
    ) -> Result<SlruPageResponse, PageServiceError> {
        let store = self.store.read().await;
        let page = store.slru_page(&request.timeline, request.key, request.lsn)?;
        Ok(SlruPageResponse { page })
    }

    /// Returns exact relation metadata from the timeline metadata store.
    pub async fn relmeta(
        &self,
        request: RelMetaRequest,
    ) -> Result<RelMetaResponse, PageServiceError> {
        let store = self.store.read().await;
        let metadata = store.relmeta(&request.timeline, request.key, request.lsn)?;
        Ok(RelMetaResponse { metadata })
    }

    /// Returns basebackup metadata once timeline metadata manifests are modeled.
    pub async fn basebackup_metadata(
        &self,
        request: BasebackupMetadataRequest,
    ) -> Result<BasebackupMetadataResponse, PageServiceError> {
        let (snapshot, page_keys) = {
            let store = self.store.read().await;
            (
                store.basebackup_metadata(&request.timeline, request.lsn)?,
                store.visible_page_keys(&request.timeline, request.lsn)?,
            )
        };
        let mut pages = Vec::with_capacity(page_keys.len());
        for key in page_keys {
            let page = self
                .get_page(GetPageRequest {
                    timeline: request.timeline.clone(),
                    key,
                    lsn: request.lsn,
                })
                .await?
                .page;
            pages.push(BasebackupPage { key, page });
        }
        let relmeta: Vec<BasebackupRelMetaMetadata> = snapshot
            .relmeta
            .into_iter()
            .map(|(key, metadata)| BasebackupRelMetaMetadata { key, metadata })
            .collect();
        let slru_pages: Vec<BasebackupSlruPageMetadata> = snapshot
            .slru_pages
            .into_iter()
            .map(|(key, page)| BasebackupSlruPageMetadata { key, page })
            .collect();
        let tar = encode_basebackup_payload(BasebackupPayloadInput {
            timeline: request.timeline.clone(),
            lsn: request.lsn,
            pages,
            relmeta: relmeta.clone(),
            slru_pages: slru_pages.clone(),
        });

        Ok(BasebackupMetadataResponse {
            timeline: request.timeline,
            lsn: request.lsn,
            relmeta,
            slru_pages,
            tar,
        })
    }
}
