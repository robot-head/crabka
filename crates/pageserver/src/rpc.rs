//! HTTP/JSON RPC boundary for the pageserver service.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use bytes::Bytes;
use crabka_page_store::PageKey;
use crabka_postgres_wal::Lsn;
use serde::{Deserialize, Serialize};

use crate::{
    BasebackupMetadataRequest, BasebackupMetadataResponse, GetPageRequest, GetPageResponse,
    PageRedo, PageService, PageServiceError, PutPageImageRequest, PutPageImageResponse,
    PutWalRecordRequest, PutWalRecordResponse, TimelineKey,
};

/// Wire representation of a branch/timeline namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcTimeline {
    /// Logical branch id.
    pub branch_id: String,
    /// Page-store tenant id.
    pub tenant_id: String,
    /// Page-store timeline id.
    pub timeline_id: String,
}

impl RpcTimeline {
    fn parse(self) -> Result<TimelineKey, PageServerRpcError> {
        TimelineKey::parse(self.branch_id, self.tenant_id, self.timeline_id)
            .map_err(|source| PageServerRpcError::bad_request(source.to_string()))
    }
}

impl From<&TimelineKey> for RpcTimeline {
    fn from(timeline: &TimelineKey) -> Self {
        Self {
            branch_id: timeline.branch_id.to_string(),
            tenant_id: timeline.path.tenant_id.to_string(),
            timeline_id: timeline.path.timeline_id.to_string(),
        }
    }
}

/// RPC request for reconstructing one page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPageRpcRequest {
    /// Branch/timeline namespace to query.
    pub timeline: RpcTimeline,
    /// `PostgreSQL` relation page key.
    pub key: PageKey,
    /// Target LSN as a raw `u64`.
    pub lsn: u64,
}

/// RPC request for basebackup metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasebackupMetadataRpcRequest {
    /// Branch/timeline namespace to query.
    pub timeline: RpcTimeline,
    /// Target LSN as a raw `u64`.
    pub lsn: u64,
}

/// RPC request for branch/timeline creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTimelineRpcRequest {
    /// Branch/timeline namespace to create.
    pub timeline: RpcTimeline,
}

/// RPC request for seeding one exact page image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutPageImageRpcRequest {
    /// Branch/timeline namespace to mutate.
    pub timeline: RpcTimeline,
    /// Page key to write.
    pub key: PageKey,
    /// LSN at which the image becomes visible.
    pub lsn: u64,
    /// Exact `PAGE_SIZE` page bytes.
    pub page: Vec<u8>,
}

/// RPC request for ingesting one live WAL delta record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutWalRecordRpcRequest {
    /// Branch/timeline namespace to mutate.
    pub timeline: RpcTimeline,
    /// Page key to write.
    pub key: PageKey,
    /// LSN at which the delta record becomes visible.
    pub lsn: u64,
    /// Whether this WAL record initializes the page image.
    pub will_init: bool,
    /// Opaque redo record bytes understood by the configured redo codec.
    pub record: Vec<u8>,
}

/// RPC response for reconstructing one page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPageRpcResponse {
    /// Reconstructed page bytes.
    pub page: Vec<u8>,
}

/// RPC response for basebackup metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasebackupMetadataRpcResponse {
    /// Branch/timeline namespace used for the snapshot.
    pub timeline: RpcTimeline,
    /// Target LSN as a raw `u64`.
    pub lsn: u64,
    /// Relation metadata visible at the requested LSN.
    pub relmeta: Vec<BasebackupRelMetaRpcMetadata>,
    /// SLRU pages visible at the requested LSN.
    pub slru_pages: Vec<BasebackupSlruPageRpcMetadata>,
}

/// One relation metadata item included in an RPC basebackup response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasebackupRelMetaRpcMetadata {
    /// Relation metadata key.
    pub key: crabka_page_store::RelMetaKey,
    /// Metadata payload visible at the requested LSN.
    pub metadata: Vec<u8>,
}

/// One SLRU page included in an RPC basebackup response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasebackupSlruPageRpcMetadata {
    /// SLRU page key.
    pub key: crabka_page_store::SlruPageKey,
    /// Exact SLRU page bytes visible at the requested LSN.
    pub page: Vec<u8>,
}

/// RPC response for branch/timeline creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTimelineRpcResponse {
    /// Created or existing branch/timeline namespace.
    pub timeline: RpcTimeline,
    /// Whether this request inserted a new namespace.
    pub created: bool,
}

/// RPC response for seeding one exact page image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutPageImageRpcResponse {
    /// Branch/timeline namespace that was mutated.
    pub timeline: RpcTimeline,
    /// Page key that was written.
    pub key: PageKey,
    /// LSN at which the image becomes visible.
    pub lsn: u64,
}

/// RPC response for ingesting one live WAL delta record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutWalRecordRpcResponse {
    /// Branch/timeline namespace that was mutated.
    pub timeline: RpcTimeline,
    /// Page key that was written.
    pub key: PageKey,
    /// LSN at which the delta record becomes visible.
    pub lsn: u64,
    /// Whether this WAL record initializes the page image.
    pub will_init: bool,
}

/// Stable JSON error body returned by RPC handlers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageServerErrorBody {
    /// Stable error category.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

/// Builds the externally callable pageserver RPC surface.
pub struct PageServerRpcRouter;

impl PageServerRpcRouter {
    /// Returns an `axum` router with page, basebackup, branch/timeline, and seed-ingest endpoints.
    pub fn router<R>(service: Arc<PageService<R>>) -> Router
    where
        R: PageRedo + Send + Sync + 'static,
    {
        Router::new()
            .route("/v1/pageserver/page:get", post(get_page::<R>))
            .route(
                "/v1/pageserver/basebackup/metadata:get",
                post(basebackup_metadata::<R>),
            )
            .route("/v1/pageserver/timeline:create", post(create_timeline::<R>))
            .route("/v1/pageserver/page:put-image", post(put_page_image::<R>))
            .route("/v1/pageserver/page:put-wal", post(put_wal_record::<R>))
            .with_state(service)
    }
}

/// Typed client for any pageserver RPC transport implementation.
pub trait PageServerRpcClient {
    /// Reconstructs one relation page through the RPC boundary.
    fn get_page(
        &self,
        request: GetPageRpcRequest,
    ) -> impl Future<Output = Result<GetPageRpcResponse, PageServerRpcError>> + Send;

    /// Returns basebackup metadata through the RPC boundary.
    fn basebackup_metadata(
        &self,
        request: BasebackupMetadataRpcRequest,
    ) -> impl Future<Output = Result<BasebackupMetadataRpcResponse, PageServerRpcError>> + Send;

    /// Creates a branch/timeline namespace through the RPC boundary.
    fn create_timeline(
        &self,
        request: CreateTimelineRpcRequest,
    ) -> impl Future<Output = Result<CreateTimelineRpcResponse, PageServerRpcError>> + Send;

    /// Seeds one page image through the RPC boundary.
    fn put_page_image(
        &self,
        request: PutPageImageRpcRequest,
    ) -> impl Future<Output = Result<PutPageImageRpcResponse, PageServerRpcError>> + Send;

    /// Ingests one WAL delta record through the RPC boundary.
    fn put_wal_record(
        &self,
        request: PutWalRecordRpcRequest,
    ) -> impl Future<Output = Result<PutWalRecordRpcResponse, PageServerRpcError>> + Send;
}

impl<R> PageServerRpcClient for Arc<PageService<R>>
where
    R: PageRedo + Send + Sync,
{
    async fn get_page(
        &self,
        request: GetPageRpcRequest,
    ) -> Result<GetPageRpcResponse, PageServerRpcError> {
        let response = self
            .as_ref()
            .get_page(GetPageRequest {
                timeline: request.timeline.parse()?,
                key: request.key,
                lsn: Lsn(request.lsn),
            })
            .await?;
        Ok(response.into())
    }

    async fn basebackup_metadata(
        &self,
        request: BasebackupMetadataRpcRequest,
    ) -> Result<BasebackupMetadataRpcResponse, PageServerRpcError> {
        let response = self
            .as_ref()
            .basebackup_metadata(BasebackupMetadataRequest {
                timeline: request.timeline.parse()?,
                lsn: Lsn(request.lsn),
            })
            .await?;
        Ok(response.into())
    }

    async fn create_timeline(
        &self,
        request: CreateTimelineRpcRequest,
    ) -> Result<CreateTimelineRpcResponse, PageServerRpcError> {
        let timeline = request.timeline.parse()?;
        Ok(self.as_ref().create_timeline(&timeline).await.into())
    }

    async fn put_page_image(
        &self,
        request: PutPageImageRpcRequest,
    ) -> Result<PutPageImageRpcResponse, PageServerRpcError> {
        let response = self
            .as_ref()
            .put_page_image(PutPageImageRequest {
                timeline: request.timeline.parse()?,
                key: request.key,
                lsn: Lsn(request.lsn),
                page: Bytes::from(request.page),
            })
            .await?;
        Ok(response.into())
    }

    async fn put_wal_record(
        &self,
        request: PutWalRecordRpcRequest,
    ) -> Result<PutWalRecordRpcResponse, PageServerRpcError> {
        let response = self
            .as_ref()
            .put_wal_record(PutWalRecordRequest {
                timeline: request.timeline.parse()?,
                key: request.key,
                lsn: Lsn(request.lsn),
                will_init: request.will_init,
                record: Bytes::from(request.record),
            })
            .await?;
        Ok(response.into())
    }
}

/// Error mapped at the RPC boundary.
#[derive(Debug, thiserror::Error)]
pub enum PageServerRpcError {
    /// Request failed boundary parsing.
    #[error("bad request: {message}")]
    BadRequest { message: String },
    /// Request reached the page service and failed there.
    #[error(transparent)]
    Service(#[from] PageServiceError),
}

impl PageServerRpcError {
    fn bad_request(message: String) -> Self {
        Self::BadRequest { message }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest { .. }
            | Self::Service(
                PageServiceError::WrongImageSize { .. }
                | PageServiceError::WrongSlruPageSize { .. }
                | PageServiceError::BranchLsnBeyondHead { .. }
                | PageServiceError::BranchBoundaryViolation { .. }
                | PageServiceError::TimelineHasDescendants { .. },
            ) => StatusCode::BAD_REQUEST,
            Self::Service(
                PageServiceError::TimelineNotFound { .. }
                | PageServiceError::PageNotFound { .. }
                | PageServiceError::RelationSizeMissing { .. }
                | PageServiceError::SlruPageMissing { .. }
                | PageServiceError::RelMetaMissing { .. }
                | PageServiceError::BranchSourceNotFound { .. },
            ) => StatusCode::NOT_FOUND,
            Self::Service(PageServiceError::PageStore(_) | PageServiceError::Redo(_)) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::BadRequest { .. } => "bad_request",
            Self::Service(PageServiceError::TimelineNotFound { .. }) => "timeline_not_found",
            Self::Service(PageServiceError::PageNotFound { .. }) => "page_not_found",
            Self::Service(PageServiceError::WrongImageSize { .. }) => "wrong_image_size",
            Self::Service(PageServiceError::WrongSlruPageSize { .. }) => "wrong_slru_page_size",
            Self::Service(PageServiceError::RelationSizeMissing { .. }) => "relation_size_missing",
            Self::Service(PageServiceError::SlruPageMissing { .. }) => "slru_page_missing",
            Self::Service(PageServiceError::RelMetaMissing { .. }) => "relmeta_missing",
            Self::Service(PageServiceError::BranchSourceNotFound { .. }) => {
                "branch_source_not_found"
            }
            Self::Service(PageServiceError::BranchLsnBeyondHead { .. }) => "branch_lsn_beyond_head",
            Self::Service(PageServiceError::BranchBoundaryViolation { .. }) => {
                "branch_boundary_violation"
            }
            Self::Service(PageServiceError::TimelineHasDescendants { .. }) => {
                "timeline_has_descendants"
            }
            Self::Service(PageServiceError::PageStore(_)) => "page_store_error",
            Self::Service(PageServiceError::Redo(_)) => "redo_error",
        }
    }
}

impl IntoResponse for PageServerRpcError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = PageServerErrorBody {
            code: self.code().to_owned(),
            message: self.to_string(),
        };
        (status, Json(body)).into_response()
    }
}

async fn get_page<R>(
    State(service): State<Arc<PageService<R>>>,
    Json(request): Json<GetPageRpcRequest>,
) -> Result<Json<GetPageRpcResponse>, PageServerRpcError>
where
    R: PageRedo + Send + Sync,
{
    Ok(Json(service.get_page(request).await?))
}

async fn basebackup_metadata<R>(
    State(service): State<Arc<PageService<R>>>,
    Json(request): Json<BasebackupMetadataRpcRequest>,
) -> Result<Json<BasebackupMetadataRpcResponse>, PageServerRpcError>
where
    R: PageRedo + Send + Sync,
{
    Ok(Json(service.basebackup_metadata(request).await?))
}

async fn create_timeline<R>(
    State(service): State<Arc<PageService<R>>>,
    Json(request): Json<CreateTimelineRpcRequest>,
) -> Result<Json<CreateTimelineRpcResponse>, PageServerRpcError>
where
    R: PageRedo + Send + Sync,
{
    Ok(Json(service.create_timeline(request).await?))
}

async fn put_page_image<R>(
    State(service): State<Arc<PageService<R>>>,
    Json(request): Json<PutPageImageRpcRequest>,
) -> Result<Json<PutPageImageRpcResponse>, PageServerRpcError>
where
    R: PageRedo + Send + Sync,
{
    Ok(Json(service.put_page_image(request).await?))
}

async fn put_wal_record<R>(
    State(service): State<Arc<PageService<R>>>,
    Json(request): Json<PutWalRecordRpcRequest>,
) -> Result<Json<PutWalRecordRpcResponse>, PageServerRpcError>
where
    R: PageRedo + Send + Sync,
{
    Ok(Json(service.put_wal_record(request).await?))
}

impl From<GetPageResponse> for GetPageRpcResponse {
    fn from(response: GetPageResponse) -> Self {
        Self {
            page: response.page.to_vec(),
        }
    }
}

impl From<BasebackupMetadataResponse> for BasebackupMetadataRpcResponse {
    fn from(response: BasebackupMetadataResponse) -> Self {
        Self {
            timeline: RpcTimeline::from(&response.timeline),
            lsn: response.lsn.value(),
            relmeta: response
                .relmeta
                .into_iter()
                .map(|metadata| BasebackupRelMetaRpcMetadata {
                    key: metadata.key,
                    metadata: metadata.metadata.to_vec(),
                })
                .collect(),
            slru_pages: response
                .slru_pages
                .into_iter()
                .map(|metadata| BasebackupSlruPageRpcMetadata {
                    key: metadata.key,
                    page: metadata.page.to_vec(),
                })
                .collect(),
        }
    }
}

impl From<crate::CreateTimelineResponse> for CreateTimelineRpcResponse {
    fn from(response: crate::CreateTimelineResponse) -> Self {
        Self {
            timeline: RpcTimeline::from(&response.timeline),
            created: response.created,
        }
    }
}

impl From<PutPageImageResponse> for PutPageImageRpcResponse {
    fn from(response: PutPageImageResponse) -> Self {
        Self {
            timeline: RpcTimeline::from(&response.timeline),
            key: response.key,
            lsn: response.lsn.value(),
        }
    }
}

impl From<PutWalRecordResponse> for PutWalRecordRpcResponse {
    fn from(response: PutWalRecordResponse) -> Self {
        Self {
            timeline: RpcTimeline::from(&response.timeline),
            key: response.key,
            lsn: response.lsn.value(),
            will_init: response.will_init,
        }
    }
}
